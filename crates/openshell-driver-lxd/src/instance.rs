// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Instance spec construction for the LXD driver.

use crate::config::LxdComputeConfig;
use openshell_core::proto::compute::v1::DriverSandbox;
use serde_json::Value;
use std::collections::BTreeMap;

/// Config key for the sandbox ID.
pub const CONFIG_SANDBOX_ID: &str = "user.openshell.sandbox-id";
/// Config key for the sandbox name.
pub const CONFIG_SANDBOX_NAME: &str = "user.openshell.sandbox-name";
/// Config key marking managed instances.
pub const CONFIG_MANAGED: &str = "user.openshell.managed";

/// Instance name prefix to avoid collisions with user instances.
const INSTANCE_PREFIX: &str = "openshell-sandbox-";

/// Build a LXD instance name from the sandbox name.
#[must_use]
pub fn instance_name(sandbox_name: &str) -> String {
    format!("{INSTANCE_PREFIX}{sandbox_name}")
}

/// Extract the sandbox name from a LXD instance name, if it has the prefix.
#[must_use]
#[allow(dead_code)]
pub fn sandbox_name_from_instance(name: &str) -> Option<&str> {
    name.strip_prefix(INSTANCE_PREFIX)
}

/// Truncate an instance name to 12 characters (standard short form for IDs).
#[must_use]
pub(crate) fn short_id(id: &str) -> String {
    id.chars().take(12).collect()
}

/// Resolve the image for a sandbox, using the template image
/// if provided, otherwise the driver's default image.
#[must_use]
pub fn resolve_image<'a>(sandbox: &'a DriverSandbox, config: &'a LxdComputeConfig) -> &'a str {
    let spec = sandbox.spec.as_ref();
    let template = spec.and_then(|s| s.template.as_ref());
    template
        .map(|t| t.image.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(&config.default_image)
}

/// Merge environment variables from user spec/template with required driver vars.
fn build_env(
    sandbox: &DriverSandbox,
    config: &LxdComputeConfig,
    image: &str,
) -> BTreeMap<String, String> {
    let spec = sandbox.spec.as_ref();
    let template = spec.and_then(|s| s.template.as_ref());

    let mut env: BTreeMap<String, String> = BTreeMap::new();

    // 1. User-supplied environment (lowest priority).
    if let Some(s) = spec {
        if !s.log_level.is_empty() {
            env.insert("OPENSHELL_LOG_LEVEL".into(), s.log_level.clone());
        }
        for (k, v) in &s.environment {
            env.insert(k.clone(), v.clone());
        }
    }
    if let Some(t) = template {
        for (k, v) in &t.environment {
            env.insert(k.clone(), v.clone());
        }
    }

    // 2. Required driver vars (highest priority -- always overwrite).
    env.insert("OPENSHELL_SANDBOX".into(), sandbox.name.clone());
    env.insert("OPENSHELL_SANDBOX_ID".into(), sandbox.id.clone());
    env.insert("OPENSHELL_ENDPOINT".into(), config.grpc_endpoint.clone());
    env.insert(
        "OPENSHELL_SSH_SOCKET_PATH".into(),
        config.sandbox_ssh_socket_path.clone(),
    );
    env.insert(
        "OPENSHELL_SSH_LISTEN_ADDR".into(),
        config.ssh_listen_addr.clone(),
    );
    env.insert(
        "OPENSHELL_SSH_HANDSHAKE_SECRET".into(),
        config.ssh_handshake_secret.clone(),
    );
    env.insert(
        "OPENSHELL_SSH_HANDSHAKE_SKEW_SECS".into(),
        config.ssh_handshake_skew_secs.to_string(),
    );
    env.insert("OPENSHELL_CONTAINER_IMAGE".into(), image.to_string());
    env.insert("OPENSHELL_SANDBOX_COMMAND".into(), "sleep infinity".into());

    env
}

/// Build the LXD instance creation JSON spec.
#[must_use]
pub fn build_instance_spec(sandbox: &DriverSandbox, config: &LxdComputeConfig) -> Value {
    let image = resolve_image(sandbox, config);
    let name = instance_name(&sandbox.name);

    let env = build_env(sandbox, config, image);

    // LXD instance config — user.* keys are custom metadata,
    // environment variables are set via cloud-init or exec.
    let mut instance_config: BTreeMap<String, Value> = BTreeMap::new();
    instance_config.insert(CONFIG_SANDBOX_ID.into(), Value::String(sandbox.id.clone()));
    instance_config.insert(
        CONFIG_SANDBOX_NAME.into(),
        Value::String(sandbox.name.clone()),
    );
    instance_config.insert(CONFIG_MANAGED.into(), Value::String("true".into()));

    // Set resource limits.
    let resources = sandbox
        .spec
        .as_ref()
        .and_then(|s| s.template.as_ref())
        .and_then(|t| t.resources.as_ref());

    let cpu_limit = resources
        .filter(|r| !r.cpu_limit.is_empty())
        .map(|r| r.cpu_limit.clone())
        .unwrap_or_else(|| "2".to_string());

    let memory_limit = resources
        .filter(|r| !r.memory_limit.is_empty())
        .map(|r| r.memory_limit.clone())
        .unwrap_or_else(|| "4GiB".to_string());

    instance_config.insert("limits.cpu".into(), Value::String(cpu_limit));
    instance_config.insert("limits.memory".into(), Value::String(memory_limit));

    // Security settings — the supervisor needs elevated privileges.
    instance_config.insert("security.nesting".into(), Value::String("true".into()));
    instance_config.insert("security.privileged".into(), Value::String("false".into()));

    // Store environment as user.env.* for later retrieval.
    for (k, v) in &env {
        instance_config.insert(format!("user.env.{k}"), Value::String(v.clone()));
    }

    // Devices: root disk + network + optional GPU.
    let mut devices: BTreeMap<String, Value> = BTreeMap::new();
    devices.insert(
        "root".into(),
        serde_json::json!({
            "type": "disk",
            "path": "/",
            "pool": "default",
        }),
    );
    devices.insert(
        "eth0".into(),
        serde_json::json!({
            "type": "nic",
            "network": config.network_name,
            "name": "eth0",
        }),
    );

    // Workspace disk.
    devices.insert(
        "workspace".into(),
        serde_json::json!({
            "type": "disk",
            "source": "",
            "path": "/sandbox",
        }),
    );

    // GPU passthrough if requested.
    if sandbox.spec.as_ref().is_some_and(|s| s.gpu) {
        devices.insert(
            "gpu".into(),
            serde_json::json!({
                "type": "gpu",
            }),
        );
    }

    serde_json::json!({
        "name": name,
        "source": {
            "type": "image",
            "alias": image,
        },
        "config": instance_config,
        "devices": devices,
        "type": "container",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> LxdComputeConfig {
        LxdComputeConfig {
            default_image: "ubuntu:22.04".to_string(),
            network_name: "openshell".to_string(),
            grpc_endpoint: "http://10.0.0.1:8080".to_string(),
            ssh_listen_addr: "0.0.0.0:2222".to_string(),
            ssh_handshake_secret: "test-secret".to_string(),
            ..LxdComputeConfig::default()
        }
    }

    fn test_sandbox(id: &str, name: &str) -> DriverSandbox {
        DriverSandbox {
            id: id.to_string(),
            name: name.to_string(),
            namespace: String::new(),
            spec: None,
            status: None,
        }
    }

    #[test]
    fn instance_name_is_prefixed() {
        assert_eq!(instance_name("my-sandbox"), "openshell-sandbox-my-sandbox");
    }

    #[test]
    fn sandbox_name_from_instance_strips_prefix() {
        assert_eq!(
            sandbox_name_from_instance("openshell-sandbox-my-sandbox"),
            Some("my-sandbox")
        );
        assert_eq!(sandbox_name_from_instance("other-instance"), None);
    }

    #[test]
    fn short_id_truncates() {
        assert_eq!(short_id("abc123def456789"), "abc123def456");
        assert_eq!(short_id("short"), "short");
    }

    #[test]
    fn build_spec_includes_managed_config() {
        let sandbox = test_sandbox("test-id", "test-name");
        let config = test_config();
        let spec = build_instance_spec(&sandbox, &config);

        assert_eq!(spec["name"], "openshell-sandbox-test-name");
        assert_eq!(spec["config"][CONFIG_SANDBOX_ID], "test-id");
        assert_eq!(spec["config"][CONFIG_SANDBOX_NAME], "test-name");
        assert_eq!(spec["config"][CONFIG_MANAGED], "true");
    }

    #[test]
    fn build_spec_includes_environment() {
        let sandbox = test_sandbox("test-id", "test-name");
        let config = test_config();
        let spec = build_instance_spec(&sandbox, &config);

        assert_eq!(spec["config"]["user.env.OPENSHELL_SANDBOX"], "test-name");
        assert_eq!(
            spec["config"]["user.env.OPENSHELL_ENDPOINT"],
            "http://10.0.0.1:8080"
        );
    }

    #[test]
    fn build_spec_includes_network_device() {
        let sandbox = test_sandbox("test-id", "test-name");
        let config = test_config();
        let spec = build_instance_spec(&sandbox, &config);

        assert_eq!(spec["devices"]["eth0"]["type"], "nic");
        assert_eq!(spec["devices"]["eth0"]["network"], "openshell");
    }
}
