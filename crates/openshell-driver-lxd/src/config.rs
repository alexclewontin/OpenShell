// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use openshell_core::config::{
    DEFAULT_NETWORK_NAME, DEFAULT_SSH_HANDSHAKE_SKEW_SECS, DEFAULT_SSH_PORT,
    DEFAULT_STOP_TIMEOUT_SECS,
};
use std::path::PathBuf;

#[derive(Clone)]
pub struct LxdComputeConfig {
    /// Path to the LXD API Unix socket.
    /// Default: `/var/snap/lxd/common/lxd/unix.socket` (snap) or
    /// `/var/lib/lxd/unix.socket` (deb).
    pub socket_path: PathBuf,
    /// LXD project name. Instances are created in this project for
    /// isolation from other workloads. Created automatically if it
    /// does not exist.
    pub project: String,
    /// Default image alias or fingerprint for sandboxes.
    pub default_image: String,
    /// Gateway gRPC endpoint the sandbox connects back to.
    ///
    /// When empty, the driver auto-detects the endpoint using
    /// `gateway_port` and the host bridge address.
    pub grpc_endpoint: String,
    /// Port the gateway server is actually listening on.
    pub gateway_port: u16,
    /// Unix socket path the in-container supervisor bridges relay traffic to.
    pub sandbox_ssh_socket_path: String,
    /// Name of the LXD network (bridge) to attach instances to.
    pub network_name: String,
    /// SSH listen address passed to the sandbox binary.
    pub ssh_listen_addr: String,
    /// SSH port inside the instance.
    pub ssh_port: u16,
    /// Shared secret for the NSSH1 SSH handshake.
    pub ssh_handshake_secret: String,
    /// Maximum clock skew in seconds for SSH handshake timestamps.
    pub ssh_handshake_skew_secs: u64,
    /// Instance stop timeout in seconds.
    pub stop_timeout_secs: u32,
}

impl LxdComputeConfig {
    /// Resolve the default socket path.
    ///
    /// Checks for the snap LXD socket first, then falls back to the
    /// deb-installed path.
    #[must_use]
    pub fn default_socket_path() -> PathBuf {
        let snap_path = PathBuf::from("/var/snap/lxd/common/lxd/unix.socket");
        if snap_path.exists() {
            return snap_path;
        }
        PathBuf::from("/var/lib/lxd/unix.socket")
    }
}

impl Default for LxdComputeConfig {
    fn default() -> Self {
        Self {
            socket_path: Self::default_socket_path(),
            project: "openshell".to_string(),
            default_image: String::new(),
            grpc_endpoint: String::new(),
            gateway_port: openshell_core::config::DEFAULT_SERVER_PORT,
            sandbox_ssh_socket_path: "/run/openshell/ssh.sock".to_string(),
            network_name: DEFAULT_NETWORK_NAME.to_string(),
            ssh_listen_addr: String::new(),
            ssh_port: DEFAULT_SSH_PORT,
            ssh_handshake_secret: String::new(),
            ssh_handshake_skew_secs: DEFAULT_SSH_HANDSHAKE_SKEW_SECS,
            stop_timeout_secs: DEFAULT_STOP_TIMEOUT_SECS,
        }
    }
}

impl std::fmt::Debug for LxdComputeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LxdComputeConfig")
            .field("socket_path", &self.socket_path)
            .field("project", &self.project)
            .field("default_image", &self.default_image)
            .field("grpc_endpoint", &self.grpc_endpoint)
            .field("gateway_port", &self.gateway_port)
            .field("sandbox_ssh_socket_path", &self.sandbox_ssh_socket_path)
            .field("network_name", &self.network_name)
            .field("ssh_listen_addr", &self.ssh_listen_addr)
            .field("ssh_port", &self.ssh_port)
            .field("ssh_handshake_secret", &"[REDACTED]")
            .field("ssh_handshake_skew_secs", &self.ssh_handshake_skew_secs)
            .field("stop_timeout_secs", &self.stop_timeout_secs)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_expected_ports() {
        let config = LxdComputeConfig::default();
        assert_eq!(config.ssh_port, DEFAULT_SSH_PORT);
        assert_eq!(
            config.gateway_port,
            openshell_core::config::DEFAULT_SERVER_PORT
        );
        assert_eq!(config.stop_timeout_secs, DEFAULT_STOP_TIMEOUT_SECS);
    }
}
