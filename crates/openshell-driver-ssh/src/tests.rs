// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;

struct MockSupervisorReadiness {
    connected: std::sync::Mutex<std::collections::HashSet<String>>,
}

impl MockSupervisorReadiness {
    fn new() -> Self {
        Self {
            connected: std::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }

    #[allow(dead_code)]
    fn set_connected(&self, sandbox_id: &str) {
        self.connected
            .lock()
            .unwrap()
            .insert(sandbox_id.to_string());
    }
}

impl SupervisorReadiness for MockSupervisorReadiness {
    fn is_supervisor_connected(&self, sandbox_id: &str) -> bool {
        self.connected.lock().unwrap().contains(sandbox_id)
    }
}

fn test_config() -> SshComputeConfig {
    SshComputeConfig {
        host: "10.0.0.42".to_string(),
        port: 22,
        user: "root".to_string(),
        identity_file: PathBuf::from("/tmp/test-key"),
        supervisor_bin: PathBuf::from("/tmp/openshell-sandbox"),
        grpc_endpoint: "http://gateway:8080".to_string(),
        ssh_socket_path: "/run/openshell/ssh.sock".to_string(),
        log_level: "info".to_string(),
    }
}

fn test_sandbox() -> DriverSandbox {
    DriverSandbox {
        id: "sb-1234".to_string(),
        name: "test-sandbox".to_string(),
        namespace: "default".to_string(),
        spec: Some(openshell_core::proto::compute::v1::DriverSandboxSpec {
            log_level: "info".to_string(),
            environment: HashMap::new(),
            template: Some(openshell_core::proto::compute::v1::DriverSandboxTemplate {
                image: String::new(),
                agent_socket_path: String::new(),
                labels: HashMap::new(),
                environment: HashMap::new(),
                resources: None,
                platform_config: None,
            }),
            gpu: false,
            gpu_device: String::new(),
        }),
        status: None,
    }
}

#[test]
fn test_config_validation_empty_host() {
    let mut config = test_config();
    config.host = String::new();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let result = rt.block_on(SshComputeDriver::new(
        config,
        Arc::new(MockSupervisorReadiness::new()),
    ));
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("SSH host is required"));
}

#[test]
fn test_config_validation_missing_key() {
    let mut config = test_config();
    config.identity_file = PathBuf::from("/nonexistent/key");
    let rt = tokio::runtime::Runtime::new().unwrap();
    let result = rt.block_on(SshComputeDriver::new(
        config,
        Arc::new(MockSupervisorReadiness::new()),
    ));
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not found"));
}

#[test]
fn test_build_supervisor_env() {
    let config = test_config();
    // Build env without needing a real SSH connection
    let sandbox = test_sandbox();
    let driver = SshComputeDriver {
        config,
        sandboxes: Arc::new(Mutex::new(HashMap::new())),
        events: broadcast::channel(1).0,
        supervisor_readiness: Arc::new(MockSupervisorReadiness::new()),
    };

    let env = driver.build_supervisor_env(&sandbox);
    let env_map: HashMap<_, _> = env.into_iter().collect();

    assert_eq!(env_map.get("OPENSHELL_SANDBOX_ID").unwrap(), "sb-1234");
    assert_eq!(env_map.get("OPENSHELL_SANDBOX").unwrap(), "test-sandbox");
    assert_eq!(
        env_map.get("OPENSHELL_ENDPOINT").unwrap(),
        "http://gateway:8080"
    );
    assert_eq!(env_map.get("OPENSHELL_LOG_LEVEL").unwrap(), "info");
    assert_eq!(
        env_map.get("OPENSHELL_SANDBOX_COMMAND").unwrap(),
        "sleep infinity"
    );
}

#[test]
fn test_build_snapshot_not_running() {
    let state = SshSandboxState {
        sandbox: test_sandbox(),
        remote_pid: None,
        running: false,
    };

    let snapshot = SshComputeDriver::build_snapshot(&state, false);
    assert_eq!(snapshot.id, "sb-1234");
    assert_eq!(snapshot.name, "test-sandbox");

    let status = snapshot.status.unwrap();
    let ready = status
        .conditions
        .iter()
        .find(|c| c.r#type == "Ready")
        .unwrap();
    assert_eq!(ready.status, "False");
    assert_eq!(ready.reason, "NotRunning");
}

#[test]
fn test_build_snapshot_running_not_connected() {
    let state = SshSandboxState {
        sandbox: test_sandbox(),
        remote_pid: Some(1234),
        running: true,
    };

    let snapshot = SshComputeDriver::build_snapshot(&state, false);
    let status = snapshot.status.unwrap();

    let ready = status
        .conditions
        .iter()
        .find(|c| c.r#type == "Ready")
        .unwrap();
    assert_eq!(ready.status, "False");
    assert_eq!(ready.reason, "DependenciesNotReady");

    let scheduled = status
        .conditions
        .iter()
        .find(|c| c.r#type == "Scheduled")
        .unwrap();
    assert_eq!(scheduled.status, "True");
    assert_eq!(status.instance_id, "1234");
}

#[test]
fn test_build_snapshot_running_and_connected() {
    let state = SshSandboxState {
        sandbox: test_sandbox(),
        remote_pid: Some(5678),
        running: true,
    };

    let snapshot = SshComputeDriver::build_snapshot(&state, true);
    let status = snapshot.status.unwrap();

    let ready = status
        .conditions
        .iter()
        .find(|c| c.r#type == "Ready")
        .unwrap();
    assert_eq!(ready.status, "True");
    assert_eq!(ready.reason, "SupervisorConnected");
}

#[test]
fn test_find_sandbox_by_id() {
    let sandbox = test_sandbox();
    let state = SshSandboxState {
        sandbox: sandbox.clone(),
        remote_pid: Some(123),
        running: true,
    };
    let mut map = HashMap::new();
    map.insert("sb-1234".to_string(), state);

    assert!(find_sandbox(&map, "sb-1234", "").is_some());
    assert!(find_sandbox(&map, "sb-9999", "").is_none());
}

#[test]
fn test_find_sandbox_by_name() {
    let sandbox = test_sandbox();
    let state = SshSandboxState {
        sandbox: sandbox.clone(),
        remote_pid: Some(123),
        running: true,
    };
    let mut map = HashMap::new();
    map.insert("sb-1234".to_string(), state);

    assert!(find_sandbox(&map, "", "test-sandbox").is_some());
    assert!(find_sandbox(&map, "", "nonexistent").is_none());
}

#[test]
fn test_require_sandbox_identifier() {
    assert!(require_sandbox_identifier("id", "").is_ok());
    assert!(require_sandbox_identifier("", "name").is_ok());
    assert!(require_sandbox_identifier("id", "name").is_ok());
    assert!(require_sandbox_identifier("", "").is_err());
}

#[test]
fn test_capabilities() {
    let driver = SshComputeDriver {
        config: test_config(),
        sandboxes: Arc::new(Mutex::new(HashMap::new())),
        events: broadcast::channel(1).0,
        supervisor_readiness: Arc::new(MockSupervisorReadiness::new()),
    };

    let caps = driver.capabilities();
    assert_eq!(caps.driver_name, "ssh");
    assert!(caps.default_image.is_empty());
    assert!(!caps.supports_gpu);
}
