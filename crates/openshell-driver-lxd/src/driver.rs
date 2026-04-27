// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! LXD compute driver.

use crate::client::{LxdApiError, LxdClient};
use crate::config::LxdComputeConfig;
use crate::instance::{self, CONFIG_MANAGED, CONFIG_SANDBOX_ID};
use crate::watcher::{self, WatchStream, driver_sandbox_from_instance};
use openshell_core::ComputeDriverError;
use openshell_core::proto::compute::v1::{DriverSandbox, GetCapabilitiesResponse};
use tracing::{info, warn};

impl From<LxdApiError> for ComputeDriverError {
    fn from(value: LxdApiError) -> Self {
        match value {
            LxdApiError::Conflict(_) => Self::AlreadyExists,
            LxdApiError::NotFound(msg) => Self::Message(format!("not found: {msg}")),
            other => Self::Message(other.to_string()),
        }
    }
}

/// LXD compute driver managing sandbox instances via the LXD REST API.
#[derive(Clone)]
pub struct LxdComputeDriver {
    client: LxdClient,
    config: LxdComputeConfig,
}

impl std::fmt::Debug for LxdComputeDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LxdComputeDriver")
            .field("socket_path", &self.config.socket_path)
            .field("project", &self.config.project)
            .field("default_image", &self.config.default_image)
            .field("network_name", &self.config.network_name)
            .finish()
    }
}

/// Construct and validate an instance name from a sandbox name.
fn validated_instance_name(sandbox_name: &str) -> Result<String, ComputeDriverError> {
    let name = instance::instance_name(sandbox_name);
    crate::client::validate_name(&name)
        .map_err(|e| ComputeDriverError::Precondition(e.to_string()))?;
    Ok(name)
}

impl LxdComputeDriver {
    /// Create a new driver, verifying the LXD socket is reachable.
    pub async fn new(mut config: LxdComputeConfig) -> Result<Self, LxdApiError> {
        if !config.socket_path.exists() {
            warn!(
                path = %config.socket_path.display(),
                "LXD socket not found; is the LXD daemon running? \
                 Set OPENSHELL_LXD_SOCKET to override."
            );
        }

        let client = LxdClient::new(config.socket_path.clone(), config.project.clone());

        // Verify connectivity.
        client.ping().await?;

        // Log server info.
        match client.server_info().await {
            Ok(info) => {
                let api_version = info
                    .get("api_version")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                info!(
                    api_version = %api_version,
                    project = %config.project,
                    "Connected to LXD"
                );
            }
            Err(e) => {
                return Err(LxdApiError::Connection(format!(
                    "failed to query LXD server info: {e}"
                )));
            }
        }

        // Ensure the LXD project exists.
        client.ensure_project().await?;
        info!(project = %config.project, "LXD project ready");

        // Auto-detect the gRPC callback endpoint.
        if config.grpc_endpoint.is_empty() {
            // In LXD, the host is typically reachable via the bridge
            // gateway address. We try to get it from the network config.
            config.grpc_endpoint = format!("http://10.0.0.1:{}", config.gateway_port);
            info!(
                grpc_endpoint = %config.grpc_endpoint,
                "Auto-detected gRPC endpoint (override with OPENSHELL_GRPC_ENDPOINT)"
            );
        }

        Ok(Self { client, config })
    }

    /// Report driver capabilities.
    pub async fn capabilities(&self) -> Result<GetCapabilitiesResponse, ComputeDriverError> {
        let supports_gpu = self.has_gpu_capacity();
        Ok(GetCapabilitiesResponse {
            driver_name: "lxd".to_string(),
            driver_version: openshell_core::VERSION.to_string(),
            default_image: self.config.default_image.clone(),
            supports_gpu,
        })
    }

    #[must_use]
    pub fn default_image(&self) -> &str {
        &self.config.default_image
    }

    /// Check whether GPU devices are available.
    fn has_gpu_capacity(&self) -> bool {
        std::path::Path::new("/dev/nvidia0").exists()
    }

    /// Validate a sandbox before creation.
    pub async fn validate_sandbox_create(
        &self,
        sandbox: &DriverSandbox,
    ) -> Result<(), ComputeDriverError> {
        let gpu_requested = sandbox.spec.as_ref().is_some_and(|s| s.gpu);
        if gpu_requested && !self.has_gpu_capacity() {
            return Err(ComputeDriverError::Precondition(
                "GPU sandbox requested, but no NVIDIA GPU devices are available.".to_string(),
            ));
        }
        Ok(())
    }

    /// Create a sandbox instance.
    pub async fn create_sandbox(&self, sandbox: &DriverSandbox) -> Result<(), ComputeDriverError> {
        if sandbox.name.is_empty() {
            return Err(ComputeDriverError::Precondition(
                "sandbox name is required".into(),
            ));
        }
        if sandbox.id.is_empty() {
            return Err(ComputeDriverError::Precondition(
                "sandbox id is required".into(),
            ));
        }

        let name = validated_instance_name(&sandbox.name)?;

        info!(
            sandbox_id = %sandbox.id,
            sandbox_name = %sandbox.name,
            instance = %name,
            "Creating sandbox instance"
        );

        let image = instance::resolve_image(sandbox, &self.config);
        if image.is_empty() {
            return Err(ComputeDriverError::Precondition(
                "no sandbox image configured: set --sandbox-image on the server \
                 or provide an image in the sandbox template"
                    .to_string(),
            ));
        }

        // 1. Create the LXD instance.
        let spec = instance::build_instance_spec(sandbox, &self.config);
        match self.client.create_instance(&spec).await {
            Ok(()) => {}
            Err(LxdApiError::Conflict(_)) => {
                return Err(ComputeDriverError::AlreadyExists);
            }
            Err(e) => {
                return Err(ComputeDriverError::from(e));
            }
        }

        // 2. Start the instance.
        if let Err(e) = self.client.start_instance(&name).await {
            warn!(
                sandbox_name = %sandbox.name,
                error = %e,
                "Failed to start instance; cleaning up"
            );
            let _ = self.client.delete_instance(&name).await;
            return Err(ComputeDriverError::from(e));
        }

        // 3. Push the supervisor binary into the running instance.
        //    In the LXD driver, we push the binary and a startup script
        //    via the files API, then exec the supervisor.
        let env = self.build_supervisor_env(sandbox, image);
        let startup_script = build_startup_script(&env);

        if let Err(e) = self
            .client
            .push_file(
                &name,
                "/opt/openshell/bin/start.sh",
                startup_script.as_bytes(),
                "0755",
            )
            .await
        {
            warn!(
                sandbox_name = %sandbox.name,
                error = %e,
                "Failed to push startup script; cleaning up"
            );
            let _ = self.client.force_stop_instance(&name).await;
            let _ = self.client.delete_instance(&name).await;
            return Err(ComputeDriverError::from(e));
        }

        // 4. Execute the startup script inside the instance.
        if let Err(e) = self
            .client
            .exec_instance(&name, &["/bin/sh", "/opt/openshell/bin/start.sh"])
            .await
        {
            warn!(
                sandbox_name = %sandbox.name,
                error = %e,
                "Failed to exec startup script; cleaning up"
            );
            let _ = self.client.force_stop_instance(&name).await;
            let _ = self.client.delete_instance(&name).await;
            return Err(ComputeDriverError::from(e));
        }

        info!(
            sandbox_id = %sandbox.id,
            sandbox_name = %sandbox.name,
            "Sandbox instance started"
        );

        Ok(())
    }

    /// Build environment variables for the supervisor startup script.
    fn build_supervisor_env(&self, sandbox: &DriverSandbox, image: &str) -> Vec<(String, String)> {
        let mut env = vec![
            ("OPENSHELL_SANDBOX".into(), sandbox.name.clone()),
            ("OPENSHELL_SANDBOX_ID".into(), sandbox.id.clone()),
            (
                "OPENSHELL_ENDPOINT".into(),
                self.config.grpc_endpoint.clone(),
            ),
            (
                "OPENSHELL_SSH_SOCKET_PATH".into(),
                self.config.sandbox_ssh_socket_path.clone(),
            ),
            (
                "OPENSHELL_SSH_LISTEN_ADDR".into(),
                self.config.ssh_listen_addr.clone(),
            ),
            (
                "OPENSHELL_SSH_HANDSHAKE_SECRET".into(),
                self.config.ssh_handshake_secret.clone(),
            ),
            (
                "OPENSHELL_SSH_HANDSHAKE_SKEW_SECS".into(),
                self.config.ssh_handshake_skew_secs.to_string(),
            ),
            ("OPENSHELL_CONTAINER_IMAGE".into(), image.to_string()),
            ("OPENSHELL_SANDBOX_COMMAND".into(), "sleep infinity".into()),
        ];

        // Add user-supplied env from spec/template.
        if let Some(spec) = sandbox.spec.as_ref() {
            if !spec.log_level.is_empty() {
                env.push(("OPENSHELL_LOG_LEVEL".into(), spec.log_level.clone()));
            }
            for (k, v) in &spec.environment {
                env.push((k.clone(), v.clone()));
            }
            if let Some(template) = &spec.template {
                for (k, v) in &template.environment {
                    env.push((k.clone(), v.clone()));
                }
            }
        }

        env
    }

    /// Stop a sandbox instance without deleting it.
    pub async fn stop_sandbox(&self, sandbox_name: &str) -> Result<(), ComputeDriverError> {
        let name = validated_instance_name(sandbox_name)?;
        info!(sandbox_name = %sandbox_name, instance = %name, "Stopping sandbox instance");

        self.client
            .stop_instance(&name, self.config.stop_timeout_secs)
            .await
            .map_err(ComputeDriverError::from)
    }

    /// Delete a sandbox instance.
    pub async fn delete_sandbox(
        &self,
        sandbox_id: &str,
        sandbox_name: &str,
    ) -> Result<bool, ComputeDriverError> {
        if sandbox_id.is_empty() {
            return Err(ComputeDriverError::Precondition(
                "sandbox id is required".into(),
            ));
        }
        let name = validated_instance_name(sandbox_name)?;
        info!(
            sandbox_id = %sandbox_id,
            sandbox_name = %sandbox_name,
            instance = %name,
            "Deleting sandbox instance"
        );

        // Verify the instance belongs to this sandbox.
        match self.client.get_instance(&name).await {
            Ok(inst) => {
                if let Some(label_id) = inst.config.get(CONFIG_SANDBOX_ID) {
                    if label_id != sandbox_id {
                        warn!(
                            sandbox_id = %sandbox_id,
                            sandbox_name = %sandbox_name,
                            instance = %name,
                            label_sandbox_id = %label_id,
                            "Instance config sandbox ID did not match delete request"
                        );
                    }
                }
            }
            Err(LxdApiError::NotFound(_)) => return Ok(false),
            Err(e) => return Err(ComputeDriverError::from(e)),
        }

        // Stop (best-effort).
        let _ = self.client.force_stop_instance(&name).await;

        // Delete.
        match self.client.delete_instance(&name).await {
            Ok(()) => Ok(true),
            Err(LxdApiError::NotFound(_)) => Ok(false),
            Err(e) => Err(ComputeDriverError::from(e)),
        }
    }

    /// Check whether a sandbox instance exists.
    pub async fn sandbox_exists(&self, sandbox_name: &str) -> Result<bool, ComputeDriverError> {
        let name = instance::instance_name(sandbox_name);
        match self.client.get_instance(&name).await {
            Ok(_) => Ok(true),
            Err(LxdApiError::NotFound(_)) => Ok(false),
            Err(e) => Err(ComputeDriverError::from(e)),
        }
    }

    /// Fetch a single sandbox by name.
    pub async fn get_sandbox(
        &self,
        sandbox_name: &str,
    ) -> Result<Option<DriverSandbox>, ComputeDriverError> {
        let name = instance::instance_name(sandbox_name);
        match self.client.get_instance(&name).await {
            Ok(inst) => Ok(driver_sandbox_from_instance(&inst)),
            Err(LxdApiError::NotFound(_)) => Ok(None),
            Err(e) => Err(ComputeDriverError::from(e)),
        }
    }

    /// List all managed sandboxes.
    pub async fn list_sandboxes(&self) -> Result<Vec<DriverSandbox>, ComputeDriverError> {
        let instances = self
            .client
            .list_instances()
            .await
            .map_err(ComputeDriverError::from)?;

        let mut sandboxes: Vec<DriverSandbox> = instances
            .iter()
            .filter(|inst| inst.config.get(CONFIG_MANAGED).is_some_and(|v| v == "true"))
            .filter_map(|inst| driver_sandbox_from_instance(inst))
            .collect();

        sandboxes.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));
        Ok(sandboxes)
    }

    /// Start watching all managed sandbox instances.
    pub async fn watch_sandboxes(&self) -> Result<WatchStream, ComputeDriverError> {
        watcher::start_watch(self.client.clone())
            .await
            .map_err(ComputeDriverError::from)
    }
}

/// Build a shell script that sets environment variables and launches
/// the supervisor binary.
fn build_startup_script(env: &[(String, String)]) -> String {
    let mut script =
        String::from("#!/bin/sh\nset -e\nmkdir -p /opt/openshell/bin /run/openshell /sandbox\n");

    for (k, v) in env {
        // Shell-escape single quotes in values.
        let escaped = v.replace('\'', "'\\''");
        script.push_str(&format!("export {k}='{escaped}'\n"));
    }

    script.push_str("exec /opt/openshell/bin/openshell-sandbox\n");
    script
}

#[cfg(test)]
impl LxdComputeDriver {
    pub(crate) fn for_tests(config: LxdComputeConfig) -> Self {
        let client = LxdClient::new(config.socket_path.clone(), config.project.clone());
        Self { client, config }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LxdComputeConfig;

    #[test]
    fn lxd_driver_error_from_conflict() {
        let err = ComputeDriverError::from(LxdApiError::Conflict("exists".into()));
        assert!(matches!(err, ComputeDriverError::AlreadyExists));
    }

    #[test]
    fn lxd_driver_error_from_not_found() {
        let err = ComputeDriverError::from(LxdApiError::NotFound("gone".into()));
        assert!(matches!(err, ComputeDriverError::Message(_)));
    }

    #[test]
    fn grpc_endpoint_auto_detected_from_gateway_port() {
        let config = LxdComputeConfig {
            gateway_port: 8081,
            ..LxdComputeConfig::default()
        };
        let mut cfg = config;
        if cfg.grpc_endpoint.is_empty() {
            cfg.grpc_endpoint = format!("http://10.0.0.1:{}", cfg.gateway_port);
        }
        assert_eq!(cfg.grpc_endpoint, "http://10.0.0.1:8081");
    }

    #[test]
    fn explicit_grpc_endpoint_takes_precedence() {
        let config = LxdComputeConfig {
            grpc_endpoint: "https://gateway.internal:9000".to_string(),
            gateway_port: 8081,
            ..LxdComputeConfig::default()
        };
        let mut cfg = config;
        if cfg.grpc_endpoint.is_empty() {
            cfg.grpc_endpoint = format!("http://10.0.0.1:{}", cfg.gateway_port);
        }
        assert_eq!(cfg.grpc_endpoint, "https://gateway.internal:9000");
    }

    #[test]
    fn startup_script_escapes_single_quotes() {
        let env = vec![("KEY".to_string(), "value with 'quotes'".to_string())];
        let script = build_startup_script(&env);
        assert!(script.contains("export KEY='value with '\\''quotes'\\'''"));
    }

    #[test]
    fn startup_script_contains_exec() {
        let env = vec![("TEST".to_string(), "val".to_string())];
        let script = build_startup_script(&env);
        assert!(script.contains("exec /opt/openshell/bin/openshell-sandbox"));
    }
}
