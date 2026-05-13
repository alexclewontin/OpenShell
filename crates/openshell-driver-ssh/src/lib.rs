// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! SSH compute driver for OpenShell.
//!
//! Manages sandbox lifecycle on a remote host via SSH. The driver:
//! 1. SCPs the `openshell-sandbox` supervisor binary to the remote host
//! 2. Starts the supervisor process over SSH
//! 3. The supervisor connects *outbound* to the gateway via `ConnectSupervisor`
//! 4. All subsequent operations (connect, exec, file sync) use the supervisor relay
//!
//! SSH is only needed during bootstrap and for stop/delete operations.

#![allow(clippy::result_large_err)]

use futures::Stream;
use openshell_core::proto::compute::v1::{
    CreateSandboxRequest, CreateSandboxResponse, DeleteSandboxRequest, DeleteSandboxResponse,
    DriverCondition, DriverSandbox, DriverSandboxStatus, GetCapabilitiesRequest,
    GetCapabilitiesResponse, GetSandboxRequest, GetSandboxResponse, ListSandboxesRequest,
    ListSandboxesResponse, StopSandboxRequest, StopSandboxResponse, ValidateSandboxCreateRequest,
    ValidateSandboxCreateResponse, WatchSandboxesEvent, WatchSandboxesRequest,
    WatchSandboxesSandboxEvent, compute_driver_server::ComputeDriver, watch_sandboxes_event,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::info;

#[cfg(test)]
mod tests;

const WATCH_BUFFER: usize = 128;
const WATCH_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Remote path where the supervisor binary is deployed.
const REMOTE_SUPERVISOR_PATH: &str = "/opt/openshell/bin/openshell-sandbox";

/// Default SSH port.
const DEFAULT_SSH_PORT: u16 = 22;

/// Queried by the SSH driver to decide when a sandbox's supervisor
/// relay is live. Implementations return `true` once a sandbox has an
/// active `ConnectSupervisor` session registered.
pub trait SupervisorReadiness: Send + Sync + 'static {
    fn is_supervisor_connected(&self, sandbox_id: &str) -> bool;
}

/// Configuration for the SSH compute driver.
#[derive(Debug, Clone)]
pub struct SshComputeConfig {
    /// Remote host to SSH into.
    pub host: String,

    /// SSH port on the remote host.
    pub port: u16,

    /// SSH username (typically "root").
    pub user: String,

    /// Path to the SSH private key for authentication.
    pub identity_file: PathBuf,

    /// Local path to the pre-built `openshell-sandbox` supervisor binary.
    pub supervisor_bin: PathBuf,

    /// Gateway gRPC endpoint the supervisor connects back to.
    pub grpc_endpoint: String,

    /// SSH socket path for the sandbox SSH server.
    pub ssh_socket_path: String,

    /// Log level for the supervisor.
    pub log_level: String,
}

impl Default for SshComputeConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: DEFAULT_SSH_PORT,
            user: "root".to_string(),
            identity_file: PathBuf::new(),
            supervisor_bin: PathBuf::new(),
            grpc_endpoint: String::new(),
            ssh_socket_path: String::new(),
            log_level: "info".to_string(),
        }
    }
}

/// Internal state for a managed SSH sandbox.
#[derive(Debug, Clone)]
struct SshSandboxState {
    sandbox: DriverSandbox,
    /// PID of the supervisor process on the remote host, if known.
    remote_pid: Option<u32>,
    /// Whether the supervisor is running on the remote host.
    running: bool,
}

type WatchStream =
    Pin<Box<dyn Stream<Item = Result<WatchSandboxesEvent, Status>> + Send + 'static>>;

/// SSH compute driver implementation.
#[derive(Clone)]
pub struct SshComputeDriver {
    config: SshComputeConfig,
    /// In-memory state of all managed sandboxes.
    sandboxes: Arc<Mutex<HashMap<String, SshSandboxState>>>,
    events: broadcast::Sender<WatchSandboxesEvent>,
    supervisor_readiness: Arc<dyn SupervisorReadiness>,
}

impl std::fmt::Debug for SshComputeDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SshComputeDriver")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl SshComputeDriver {
    pub async fn new(
        config: SshComputeConfig,
        supervisor_readiness: Arc<dyn SupervisorReadiness>,
    ) -> Result<Self, String> {
        if config.host.is_empty() {
            return Err("SSH host is required (--ssh-host or OPENSHELL_SSH_HOST)".to_string());
        }
        if config.identity_file.as_os_str().is_empty() {
            return Err(
                "SSH identity file is required (--ssh-key or OPENSHELL_SSH_KEY)".to_string(),
            );
        }
        if !config.identity_file.exists() {
            return Err(format!(
                "SSH identity file not found: {}",
                config.identity_file.display()
            ));
        }
        if config.supervisor_bin.as_os_str().is_empty() {
            return Err(
                "SSH supervisor binary path is required (--ssh-supervisor-bin or OPENSHELL_SSH_SUPERVISOR_BIN)".to_string(),
            );
        }
        if !config.supervisor_bin.exists() {
            return Err(format!(
                "SSH supervisor binary not found: {}",
                config.supervisor_bin.display()
            ));
        }

        let driver = Self {
            config,
            sandboxes: Arc::new(Mutex::new(HashMap::new())),
            events: broadcast::channel(WATCH_BUFFER).0,
            supervisor_readiness,
        };

        let poll_driver = driver.clone();
        tokio::spawn(async move {
            poll_driver.poll_loop().await;
        });

        Ok(driver)
    }

    fn capabilities(&self) -> GetCapabilitiesResponse {
        GetCapabilitiesResponse {
            driver_name: "ssh".to_string(),
            driver_version: env!("CARGO_PKG_VERSION").to_string(),
            // SSH driver doesn't use container images.
            default_image: String::new(),
            supports_gpu: false,
            gpu_count: 0,
        }
    }

    /// Execute a command on the remote host via SSH, returning stdout.
    async fn ssh_exec(&self, command: &str) -> Result<String, Status> {
        let key_path = self.config.identity_file.display().to_string();
        let user_host = format!("{}@{}", self.config.user, self.config.host);

        let output = tokio::process::Command::new("ssh")
            .args([
                "-o", "StrictHostKeyChecking=accept-new",
                "-o", "BatchMode=yes",
                "-o", "ConnectTimeout=10",
                "-p", &self.config.port.to_string(),
                "-i", &key_path,
                &user_host,
                command,
            ])
            .output()
            .await
            .map_err(|err| Status::internal(format!("SSH exec failed: {err}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Status::internal(format!(
                "SSH command failed (exit {}): {stderr}",
                output.status.code().unwrap_or(-1)
            )));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// SCP a local file to the remote host.
    async fn scp_to_remote(&self, local_path: &str, remote_path: &str) -> Result<(), Status> {
        let key_path = self.config.identity_file.display().to_string();
        let remote_target = format!(
            "{}@{}:{}",
            self.config.user, self.config.host, remote_path
        );

        let output = tokio::process::Command::new("scp")
            .args([
                "-o", "StrictHostKeyChecking=accept-new",
                "-o", "BatchMode=yes",
                "-o", "ConnectTimeout=10",
                "-P", &self.config.port.to_string(),
                "-i", &key_path,
                local_path,
                &remote_target,
            ])
            .output()
            .await
            .map_err(|err| Status::internal(format!("SCP failed: {err}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Status::internal(format!(
                "SCP failed (exit {}): {stderr}",
                output.status.code().unwrap_or(-1)
            )));
        }

        Ok(())
    }

    /// Deploy the supervisor binary and start it on the remote host.
    async fn deploy_and_start_supervisor(
        &self,
        sandbox: &DriverSandbox,
    ) -> Result<u32, Status> {
        // Create remote directories
        self.ssh_exec(&format!(
            "mkdir -p /opt/openshell/bin /sandbox && \
             id sandbox >/dev/null 2>&1 || useradd -r -m sandbox"
        ))
        .await?;

        // SCP the supervisor binary
        self.scp_to_remote(
            &self.config.supervisor_bin.display().to_string(),
            REMOTE_SUPERVISOR_PATH,
        )
        .await?;

        // Make it executable
        self.ssh_exec(&format!("chmod +x {REMOTE_SUPERVISOR_PATH}")).await?;

        // Build environment variables for the supervisor
        let env_vars = self.build_supervisor_env(sandbox);
        let env_string = env_vars
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(" ");

        // Start the supervisor in the background, capture its PID
        let pid_output = self
            .ssh_exec(&format!(
                "nohup env {env_string} {REMOTE_SUPERVISOR_PATH} \
                 > /tmp/openshell-supervisor.log 2>&1 & echo $!"
            ))
            .await?;

        let pid: u32 = pid_output.trim().parse().map_err(|_| {
            Status::internal(format!(
                "failed to parse supervisor PID from output: {pid_output:?}"
            ))
        })?;

        info!(
            sandbox_id = %sandbox.id,
            sandbox_name = %sandbox.name,
            remote_host = %self.config.host,
            pid,
            "Started supervisor on remote host"
        );

        Ok(pid)
    }

    /// Build environment variables the supervisor needs.
    fn build_supervisor_env(&self, sandbox: &DriverSandbox) -> Vec<(String, String)> {
        let mut env = vec![
            ("HOME".to_string(), "/root".to_string()),
            ("TERM".to_string(), "xterm".to_string()),
            (
                "OPENSHELL_LOG_LEVEL".to_string(),
                self.config.log_level.clone(),
            ),
            (
                "OPENSHELL_ENDPOINT".to_string(),
                self.config.grpc_endpoint.clone(),
            ),
            ("OPENSHELL_SANDBOX_ID".to_string(), sandbox.id.clone()),
            ("OPENSHELL_SANDBOX".to_string(), sandbox.name.clone()),
            (
                "OPENSHELL_SSH_SOCKET_PATH".to_string(),
                self.config.ssh_socket_path.clone(),
            ),
            (
                "OPENSHELL_SANDBOX_COMMAND".to_string(),
                "sleep infinity".to_string(),
            ),
        ];

        // Merge environment from the sandbox spec
        if let Some(spec) = sandbox.spec.as_ref() {
            if let Some(template) = spec.template.as_ref() {
                for (k, v) in &template.environment {
                    env.push((k.clone(), v.clone()));
                }
            }
            for (k, v) in &spec.environment {
                env.push((k.clone(), v.clone()));
            }
        }

        env
    }

    /// Check if a remote process is still running.
    async fn is_process_alive(&self, pid: u32) -> bool {
        self.ssh_exec(&format!("kill -0 {pid} 2>/dev/null && echo alive"))
            .await
            .is_ok_and(|output| output.trim() == "alive")
    }

    /// Kill the supervisor process on the remote host.
    async fn kill_supervisor(&self, pid: u32) -> Result<(), Status> {
        // SIGTERM first, then SIGKILL after timeout
        let _ = self
            .ssh_exec(&format!(
                "kill {pid} 2>/dev/null; \
                 sleep 2; \
                 kill -0 {pid} 2>/dev/null && kill -9 {pid} 2>/dev/null; \
                 true"
            ))
            .await;
        Ok(())
    }

    /// Build a `DriverSandbox` snapshot from internal state.
    fn build_snapshot(state: &SshSandboxState, supervisor_ready: bool) -> DriverSandbox {
        let mut conditions = vec![DriverCondition {
            r#type: "Ready".to_string(),
            status: if supervisor_ready {
                "True".to_string()
            } else if state.running {
                "False".to_string()
            } else {
                "False".to_string()
            },
            reason: if supervisor_ready {
                "SupervisorConnected".to_string()
            } else if state.running {
                "DependenciesNotReady".to_string()
            } else {
                "NotRunning".to_string()
            },
            message: if supervisor_ready {
                "Supervisor relay is connected".to_string()
            } else if state.running {
                "Waiting for supervisor to connect".to_string()
            } else {
                "Supervisor process is not running".to_string()
            },
            last_transition_time: String::new(),
        }];

        if state.running {
            conditions.push(DriverCondition {
                r#type: "Scheduled".to_string(),
                status: "True".to_string(),
                reason: "Running".to_string(),
                message: format!(
                    "Process running (PID {})",
                    state.remote_pid.unwrap_or(0)
                ),
                last_transition_time: String::new(),
            });
        }

        DriverSandbox {
            id: state.sandbox.id.clone(),
            name: state.sandbox.name.clone(),
            namespace: state.sandbox.namespace.clone(),
            spec: state.sandbox.spec.clone(),
            status: Some(DriverSandboxStatus {
                sandbox_name: state.sandbox.name.clone(),
                instance_id: state
                    .remote_pid
                    .map_or_else(String::new, |pid| pid.to_string()),
                agent_fd: String::new(),
                sandbox_fd: String::new(),
                conditions,
                deleting: false,
            }),
        }
    }

    /// Background poll loop that checks remote process liveness.
    async fn poll_loop(self) {
        loop {
            tokio::time::sleep(WATCH_POLL_INTERVAL).await;

            let sandbox_states: Vec<(String, u32)> = {
                let sandboxes = self.sandboxes.lock().await;
                sandboxes
                    .iter()
                    .filter_map(|(id, state)| {
                        state.remote_pid.map(|pid| (id.clone(), pid))
                    })
                    .collect()
            };

            for (sandbox_id, pid) in sandbox_states {
                let alive = self.is_process_alive(pid).await;
                let supervisor_connected =
                    self.supervisor_readiness.is_supervisor_connected(&sandbox_id);

                let mut sandboxes = self.sandboxes.lock().await;
                if let Some(state) = sandboxes.get_mut(&sandbox_id) {
                    let was_running = state.running;
                    state.running = alive;

                    if was_running != alive || supervisor_connected {
                        let snapshot =
                            Self::build_snapshot(state, supervisor_connected);
                        let _ = self.events.send(WatchSandboxesEvent {
                            payload: Some(watch_sandboxes_event::Payload::Sandbox(
                                WatchSandboxesSandboxEvent {
                                    sandbox: Some(snapshot),
                                },
                            )),
                        });
                    }
                }
            }
        }
    }
}

#[tonic::async_trait]
impl ComputeDriver for SshComputeDriver {
    type WatchSandboxesStream = WatchStream;

    async fn get_capabilities(
        &self,
        _request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<GetCapabilitiesResponse>, Status> {
        Ok(Response::new(self.capabilities()))
    }

    async fn validate_sandbox_create(
        &self,
        request: Request<ValidateSandboxCreateRequest>,
    ) -> Result<Response<ValidateSandboxCreateResponse>, Status> {
        let sandbox = request
            .into_inner()
            .sandbox
            .ok_or_else(|| Status::invalid_argument("sandbox is required"))?;

        // Validate we can reach the remote host
        self.ssh_exec("echo ok")
            .await
            .map_err(|_| Status::failed_precondition(format!(
                "cannot reach SSH host {}:{} as {}",
                self.config.host, self.config.port, self.config.user
            )))?;

        // Check kernel supports Landlock (best-effort)
        let _ = self
            .ssh_exec("cat /sys/kernel/security/lsm 2>/dev/null")
            .await;

        // Check for required tools
        self.ssh_exec("which ip nsenter >/dev/null 2>&1")
            .await
            .map_err(|_| Status::failed_precondition(
                "remote host missing required tools: ip, nsenter (install iproute2)"
            ))?;

        // v1: only one sandbox per SSH target
        let sandboxes = self.sandboxes.lock().await;
        if !sandboxes.is_empty() {
            let existing = sandboxes.keys().next().unwrap();
            if !sandboxes.contains_key(&sandbox.id) {
                return Err(Status::failed_precondition(format!(
                    "SSH driver supports one sandbox per host; sandbox '{existing}' already exists on {}",
                    self.config.host
                )));
            }
        }

        Ok(Response::new(ValidateSandboxCreateResponse {}))
    }

    async fn get_sandbox(
        &self,
        request: Request<GetSandboxRequest>,
    ) -> Result<Response<GetSandboxResponse>, Status> {
        let request = request.into_inner();
        require_sandbox_identifier(&request.sandbox_id, &request.sandbox_name)?;

        let sandboxes = self.sandboxes.lock().await;
        let state = find_sandbox(&sandboxes, &request.sandbox_id, &request.sandbox_name)
            .ok_or_else(|| Status::not_found("sandbox not found"))?;

        let supervisor_connected = self
            .supervisor_readiness
            .is_supervisor_connected(&state.sandbox.id);
        let snapshot = Self::build_snapshot(state, supervisor_connected);

        Ok(Response::new(GetSandboxResponse {
            sandbox: Some(snapshot),
        }))
    }

    async fn list_sandboxes(
        &self,
        _request: Request<ListSandboxesRequest>,
    ) -> Result<Response<ListSandboxesResponse>, Status> {
        let sandboxes = self.sandboxes.lock().await;
        let mut snapshots: Vec<DriverSandbox> = sandboxes
            .values()
            .map(|state| {
                let connected = self
                    .supervisor_readiness
                    .is_supervisor_connected(&state.sandbox.id);
                Self::build_snapshot(state, connected)
            })
            .collect();
        snapshots.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(Response::new(ListSandboxesResponse {
            sandboxes: snapshots,
        }))
    }

    async fn create_sandbox(
        &self,
        request: Request<CreateSandboxRequest>,
    ) -> Result<Response<CreateSandboxResponse>, Status> {
        let sandbox = request
            .into_inner()
            .sandbox
            .ok_or_else(|| Status::invalid_argument("sandbox is required"))?;

        {
            let sandboxes = self.sandboxes.lock().await;
            if sandboxes.contains_key(&sandbox.id) {
                return Err(Status::already_exists("sandbox already exists"));
            }
        }

        let pid = self.deploy_and_start_supervisor(&sandbox).await?;

        let state = SshSandboxState {
            sandbox: sandbox.clone(),
            remote_pid: Some(pid),
            running: true,
        };

        {
            let mut sandboxes = self.sandboxes.lock().await;
            sandboxes.insert(sandbox.id.clone(), state.clone());
        }

        // Emit initial watch event
        let snapshot = Self::build_snapshot(&state, false);
        let _ = self.events.send(WatchSandboxesEvent {
            payload: Some(watch_sandboxes_event::Payload::Sandbox(
                WatchSandboxesSandboxEvent {
                    sandbox: Some(snapshot),
                },
            )),
        });

        Ok(Response::new(CreateSandboxResponse {}))
    }

    async fn stop_sandbox(
        &self,
        request: Request<StopSandboxRequest>,
    ) -> Result<Response<StopSandboxResponse>, Status> {
        let request = request.into_inner();
        require_sandbox_identifier(&request.sandbox_id, &request.sandbox_name)?;

        let pid = {
            let sandboxes = self.sandboxes.lock().await;
            let state =
                find_sandbox(&sandboxes, &request.sandbox_id, &request.sandbox_name)
                    .ok_or_else(|| Status::not_found("sandbox not found"))?;
            state.remote_pid
        };

        if let Some(pid) = pid {
            self.kill_supervisor(pid).await?;
        }

        {
            let mut sandboxes = self.sandboxes.lock().await;
            if let Some(state) = find_sandbox_mut(
                &mut sandboxes,
                &request.sandbox_id,
                &request.sandbox_name,
            ) {
                state.running = false;
            }
        }

        Ok(Response::new(StopSandboxResponse {}))
    }

    async fn delete_sandbox(
        &self,
        request: Request<DeleteSandboxRequest>,
    ) -> Result<Response<DeleteSandboxResponse>, Status> {
        let request = request.into_inner();
        require_sandbox_identifier(&request.sandbox_id, &request.sandbox_name)?;

        let removed = {
            let mut sandboxes = self.sandboxes.lock().await;
            let key = find_sandbox_key(
                &sandboxes,
                &request.sandbox_id,
                &request.sandbox_name,
            );
            match key {
                Some(key) => sandboxes.remove(&key),
                None => None,
            }
        };

        let Some(state) = removed else {
            return Ok(Response::new(DeleteSandboxResponse { deleted: false }));
        };

        // Kill the remote process if running
        if let Some(pid) = state.remote_pid {
            let _ = self.kill_supervisor(pid).await;
        }

        // Clean up remote artifacts
        let _ = self
            .ssh_exec(&format!(
                "rm -f {REMOTE_SUPERVISOR_PATH} /tmp/openshell-supervisor.log"
            ))
            .await;

        info!(
            sandbox_id = %state.sandbox.id,
            sandbox_name = %state.sandbox.name,
            "Deleted SSH sandbox and cleaned up remote host"
        );

        // Emit deletion event
        if !state.sandbox.id.is_empty() {
            let _ = self.events.send(WatchSandboxesEvent {
                payload: Some(watch_sandboxes_event::Payload::Deleted(
                    openshell_core::proto::compute::v1::WatchSandboxesDeletedEvent {
                        sandbox_id: state.sandbox.id,
                    },
                )),
            });
        }

        Ok(Response::new(DeleteSandboxResponse { deleted: true }))
    }

    async fn watch_sandboxes(
        &self,
        _request: Request<WatchSandboxesRequest>,
    ) -> Result<Response<Self::WatchSandboxesStream>, Status> {
        let mut rx = self.events.subscribe();

        // Send initial snapshot
        let initial: Vec<DriverSandbox> = {
            let sandboxes = self.sandboxes.lock().await;
            sandboxes
                .values()
                .map(|state| {
                    let connected = self
                        .supervisor_readiness
                        .is_supervisor_connected(&state.sandbox.id);
                    Self::build_snapshot(state, connected)
                })
                .collect()
        };

        let (tx, out_rx) = mpsc::channel(WATCH_BUFFER);
        tokio::spawn(async move {
            for sandbox in initial {
                if tx
                    .send(Ok(WatchSandboxesEvent {
                        payload: Some(watch_sandboxes_event::Payload::Sandbox(
                            WatchSandboxesSandboxEvent {
                                sandbox: Some(sandbox),
                            },
                        )),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }

            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if tx.send(Ok(event)).await.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(out_rx))))
    }
}

fn require_sandbox_identifier(sandbox_id: &str, sandbox_name: &str) -> Result<(), Status> {
    if sandbox_id.is_empty() && sandbox_name.is_empty() {
        return Err(Status::invalid_argument(
            "sandbox_id or sandbox_name is required",
        ));
    }
    Ok(())
}

fn find_sandbox<'a>(
    sandboxes: &'a HashMap<String, SshSandboxState>,
    sandbox_id: &str,
    sandbox_name: &str,
) -> Option<&'a SshSandboxState> {
    if !sandbox_id.is_empty() {
        return sandboxes.get(sandbox_id);
    }
    sandboxes
        .values()
        .find(|state| state.sandbox.name == sandbox_name)
}

fn find_sandbox_mut<'a>(
    sandboxes: &'a mut HashMap<String, SshSandboxState>,
    sandbox_id: &str,
    sandbox_name: &str,
) -> Option<&'a mut SshSandboxState> {
    if !sandbox_id.is_empty() {
        return sandboxes.get_mut(sandbox_id);
    }
    sandboxes
        .values_mut()
        .find(|state| state.sandbox.name == sandbox_name)
}

fn find_sandbox_key(
    sandboxes: &HashMap<String, SshSandboxState>,
    sandbox_id: &str,
    sandbox_name: &str,
) -> Option<String> {
    if !sandbox_id.is_empty() {
        return sandboxes.contains_key(sandbox_id).then(|| sandbox_id.to_string());
    }
    sandboxes
        .iter()
        .find(|(_, state)| state.sandbox.name == sandbox_name)
        .map(|(key, _)| key.clone())
}
