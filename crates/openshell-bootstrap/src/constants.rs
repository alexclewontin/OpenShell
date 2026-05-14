// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/// K8s secret holding the server's TLS certificate and private key.
pub const SERVER_TLS_SECRET_NAME: &str = "openshell-server-tls";
/// K8s secret holding the CA certificate used to verify client certificates.
pub const SERVER_CLIENT_CA_SECRET_NAME: &str = "openshell-server-client-ca";
/// K8s secret holding the client TLS certificate, key, and CA cert (shared by CLI and sandboxes).
pub const CLIENT_TLS_SECRET_NAME: &str = "openshell-client-tls";
/// K8s secret holding the SSH handshake HMAC secret (shared by gateway and sandbox pods).
pub const SSH_HANDSHAKE_SECRET_NAME: &str = "openshell-ssh-handshake";
/// `NodePort` used by the gateway `StatefulSet`. Must match the Helm chart's service definition.
pub const GATEWAY_NODE_PORT: u16 = 30051;
