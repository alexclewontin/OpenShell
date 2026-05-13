// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Bare-metal local compute driver for OpenShell.
//!
//! Spawns sandbox supervisors as local child processes on the same host,
//! without requiring Docker, Podman, Kubernetes, or any container runtime.
//! The supervisor applies Landlock, seccomp, and netns isolation directly.
//!
//! Flow:
//! 1. Spawn `openshell-sandbox` as a child process with appropriate env vars
//! 2. The supervisor connects *outbound* to the gateway via `ConnectSupervisor`
//! 3. All subsequent operations (connect, exec, file sync) use the supervisor relay
//!
//! This is the bare-metal analog of the SSH driver, without SSH transport.

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
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::info;

#[cfg(test)]
mod tests;

const WATCH_BUFFER: usize = 128;
const WATCH_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Queried by the local driver to decide when a sandbox's supervisor
/// relay is live. Implementations return `true` once a sandbox has an
/// active `ConnectSupervisor` session registered.
pub trait SupervisorReadiness: Send + Sync + 'static {
    fn is_supervisor_connected(&self, sandbox_id: &str) -> bool;
}

/// Configuration for the local compute driver.
#[derive(Debug, Clone)]
pub struct LocalComputeConfig {
    /// Path to the pre-built `openshell-sandbox` supervisor binary.
    pub supervisor_bin: PathBuf,

    /// Gateway gRPC endpoint the supervisor connects back to.
    pub grpc_endpoint: String,

    /// SSH socket path for the sandbox SSH server.
    pub ssh_socket_path: String,

    /// Log level for the supervisor.
    pub log_level: String,
}

impl Default for LocalComputeConfig {
    fn default() -> Self {
        Self {
            supervisor_bin: PathBuf::new(),
            grpc_endpoint: String::new(),
            ssh_socket_path: String::new(),
            log_level: "info".to_string(),
        }
    }
}

/// Internal state for a managed local sandbox.
#[derive(Debug, Clone)]
struct LocalSandboxState {
    sandbox: DriverSandbox,
    running: bool,
}

type WatchStream =
    Pin<Box<dyn Stream<Item = Result<WatchSandboxesEvent, Status>> + Send + 'static>>;

/// Local bare-metal compute driver implementation.
#[derive(Clone)]
pub struct LocalComputeDriver {
    config: LocalComputeConfig,
    sandboxes: Arc<Mutex<HashMap<String, LocalSandboxState>>>,
    children: Arc<Mutex<HashMap<String, Child>>>,
    events: broadcast::Sender<WatchSandboxesEvent>,
    supervisor_readiness: Arc<dyn SupervisorReadiness>,
}

impl std::fmt::Debug for LocalComputeDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalComputeDriver")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl LocalComputeDriver {
    pub async fn new(
        config: LocalComputeConfig,
        supervisor_readiness: Arc<dyn SupervisorReadiness>,
    ) -> Result<Self, String> {
        if config.supervisor_bin.as_os_str().is_empty() {
            return Err(
                "supervisor binary path is required (--local-supervisor-bin or OPENSHELL_LOCAL_SUPERVISOR_BIN)".to_string(),
            );
        }
        if config.grpc_endpoint.is_empty() {
            return Err(
                "gRPC endpoint is required (--grpc-endpoint or OPENSHELL_GRPC_ENDPOINT)".to_string(),
            );
        }
        if !config.supervisor_bin.exists() {
            return Err(format!(
                "supervisor binary not found: {}",
                config.supervisor_bin.display()
            ));
        }

        let driver = Self {
            config,
            sandboxes: Arc::new(Mutex::new(HashMap::new())),
            children: Arc::new(Mutex::new(HashMap::new())),
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
            driver_name: "local".to_string(),
            driver_version: env!("CARGO_PKG_VERSION").to_string(),
            default_image: String::new(),
            supports_gpu: false,
            gpu_count: 0,
        }
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

    /// Spawn the supervisor as a local child process.
    async fn spawn_supervisor(
        &self,
        sandbox: &DriverSandbox,
    ) -> Result<Child, Status> {
        let env_vars = self.build_supervisor_env(sandbox);

        let mut cmd = Command::new(&self.config.supervisor_bin);
        for (key, value) in &env_vars {
            cmd.env(key, value);
        }
        cmd.stdout(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::null());
        cmd.kill_on_drop(false);

        let child = cmd
            .spawn()
            .map_err(|err| Status::internal(format!("failed to spawn supervisor: {err}")))?;

        let pid = child.id().unwrap_or(0);

        info!(
            sandbox_id = %sandbox.id,
            sandbox_name = %sandbox.name,
            pid,
            "Started supervisor as local child process"
        );

        Ok(child)
    }

    /// Build a `DriverSandbox` snapshot from internal state.
    fn build_snapshot(state: &LocalSandboxState, supervisor_ready: bool) -> DriverSandbox {
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
                message: "Process running".to_string(),
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
                instance_id: String::new(),
                agent_fd: String::new(),
                sandbox_fd: String::new(),
                conditions,
                deleting: false,
            }),
        }
    }

    /// Background poll loop that checks child process liveness.
    async fn poll_loop(self) {
        loop {
            tokio::time::sleep(WATCH_POLL_INTERVAL).await;

            let sandbox_ids: Vec<String> = {
                let sandboxes = self.sandboxes.lock().await;
                sandboxes.keys().cloned().collect()
            };

            let mut events_to_send = Vec::new();

            for sandbox_id in &sandbox_ids {
                let mut still_alive = false;

                {
                    let mut children = self.children.lock().await;
                    if let Some(child) = children.get_mut(sandbox_id) {
                        match child.try_wait() {
                            Ok(Some(_status)) => {
                                // Child exited — mark it done
                                info!(
                                    sandbox_id = %sandbox_id,
                                    exit_status = ?_status,
                                    "Supervisor process exited"
                                );
                                children.remove(sandbox_id);
                            }
                            Ok(None) => {
                                // Still running
                                still_alive = true;
                            }
                            Err(e) => {
                                info!(
                                    sandbox_id = %sandbox_id,
                                    error = %e,
                                    "Failed to check supervisor process"
                                );
                            }
                        }
                    }
                }

                let supervisor_connected = self
                    .supervisor_readiness
                    .is_supervisor_connected(sandbox_id);

                let mut sandboxes = self.sandboxes.lock().await;
                if let Some(state) = sandboxes.get_mut(sandbox_id) {
                    let was_running = state.running;
                    state.running = still_alive;

                    if was_running != still_alive || supervisor_connected {
                        let snapshot = Self::build_snapshot(state, supervisor_connected);
                        events_to_send.push(WatchSandboxesEvent {
                            payload: Some(watch_sandboxes_event::Payload::Sandbox(
                                WatchSandboxesSandboxEvent {
                                    sandbox: Some(snapshot),
                                },
                            )),
                        });
                    }
                }
            }

            for event in events_to_send {
                let _ = self.events.send(event);
            }
        }
    }
}

#[tonic::async_trait]
impl ComputeDriver for LocalComputeDriver {
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

        if !self.config.supervisor_bin.exists() {
            return Err(Status::failed_precondition(format!(
                "supervisor binary not found: {}",
                self.config.supervisor_bin.display()
            )));
        }

        // Check kernel supports Landlock (best-effort)
        let _ = std::fs::read_to_string("/sys/kernel/security/lsm");

        // v1: only one sandbox per local host
        let sandboxes = self.sandboxes.lock().await;
        if !sandboxes.is_empty() {
            let existing = sandboxes.keys().next().unwrap();
            if !sandboxes.contains_key(&sandbox.id) {
                return Err(Status::failed_precondition(format!(
                    "local driver supports one sandbox per host; sandbox '{existing}' already exists on this host"
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

        let child = self.spawn_supervisor(&sandbox).await?;

        let state = LocalSandboxState {
            sandbox: sandbox.clone(),
            running: true,
        };

        {
            let mut sandboxes = self.sandboxes.lock().await;
            sandboxes.insert(sandbox.id.clone(), state.clone());
        }

        {
            let mut children = self.children.lock().await;
            children.insert(sandbox.id.clone(), child);
        }

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

        let sandbox_id = {
            let sandboxes = self.sandboxes.lock().await;
            let key = find_sandbox_key(
                &sandboxes,
                &request.sandbox_id,
                &request.sandbox_name,
            );
            key
        };

        if let Some(ref id) = sandbox_id {
            let mut children = self.children.lock().await;
            if let Some(mut child) = children.remove(id) {
                let _ = child.start_kill();
            }
        }

        if let Some(ref id) = sandbox_id {
            let mut sandboxes = self.sandboxes.lock().await;
            if let Some(state) = sandboxes.get_mut(id) {
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

        let sandbox_id = {
            let sandboxes = self.sandboxes.lock().await;
            find_sandbox_key(&sandboxes, &request.sandbox_id, &request.sandbox_name)
        };

        let removed = {
            let mut sandboxes = self.sandboxes.lock().await;
            sandbox_id.as_ref().and_then(|id| sandboxes.remove(id))
        };

        let Some(state) = removed else {
            return Ok(Response::new(DeleteSandboxResponse { deleted: false }));
        };

        // Kill the child process if running
        if let Some(ref id) = sandbox_id {
            let mut children = self.children.lock().await;
            if let Some(mut child) = children.remove(id) {
                let _ = child.start_kill();
            }
        }

        info!(
            sandbox_id = %state.sandbox.id,
            sandbox_name = %state.sandbox.name,
            "Deleted local sandbox"
        );

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
    sandboxes: &'a HashMap<String, LocalSandboxState>,
    sandbox_id: &str,
    sandbox_name: &str,
) -> Option<&'a LocalSandboxState> {
    if !sandbox_id.is_empty() {
        return sandboxes.get(sandbox_id);
    }
    sandboxes
        .values()
        .find(|state| state.sandbox.name == sandbox_name)
}

fn find_sandbox_key(
    sandboxes: &HashMap<String, LocalSandboxState>,
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