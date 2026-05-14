// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::constants::{CLIENT_TLS_SECRET_NAME, SERVER_TLS_SECRET_NAME};
use crate::pki::PkiBundle;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Secret;
use kube::{
    Api, Client,
    api::{Patch, PatchParams},
};
use miette::Result;

pub async fn try_load_pki(client: Client, namespace: &str) -> Result<Option<PkiBundle>> {
    let secrets: Api<Secret> = Api::namespaced(client, namespace);

    let Ok(client_tls) = secrets.get(CLIENT_TLS_SECRET_NAME).await else {
        return Ok(None);
    };
    let ca_cert = client_tls
        .data
        .as_ref()
        .and_then(|d| d.get("ca.crt"))
        .map(|b| String::from_utf8_lossy(&b.0).to_string());
    let client_cert = client_tls
        .data
        .as_ref()
        .and_then(|d| d.get("tls.crt"))
        .map(|b| String::from_utf8_lossy(&b.0).to_string());
    let client_key = client_tls
        .data
        .as_ref()
        .and_then(|d| d.get("tls.key"))
        .map(|b| String::from_utf8_lossy(&b.0).to_string());

    let Ok(server_tls) = secrets.get(SERVER_TLS_SECRET_NAME).await else {
        return Ok(None);
    };
    let server_cert = server_tls
        .data
        .as_ref()
        .and_then(|d| d.get("tls.crt"))
        .map(|b| String::from_utf8_lossy(&b.0).to_string());
    let server_key = server_tls
        .data
        .as_ref()
        .and_then(|d| d.get("tls.key"))
        .map(|b| String::from_utf8_lossy(&b.0).to_string());

    match (ca_cert, client_cert, client_key, server_cert, server_key) {
        (Some(ca), Some(cc), Some(ck), Some(sc), Some(sk)) => {
            Ok(Some(PkiBundle {
                ca_cert_pem: ca,
                ca_key_pem: String::new(), // Not needed for existing
                server_cert_pem: sc,
                server_key_pem: sk,
                client_cert_pem: cc,
                client_key_pem: ck,
            }))
        }
        _ => Ok(None),
    }
}

pub async fn adopt_legacy_resources(client: Client, namespace: &str) -> Result<()> {
    let patch = serde_json::json!({
        "metadata": {
            "labels": {
                "app.kubernetes.io/managed-by": "Helm"
            },
            "annotations": {
                "meta.helm.sh/release-name": "gateway",
                "meta.helm.sh/release-namespace": namespace
            }
        }
    });
    let params = PatchParams::default();

    macro_rules! adopt {
        ($api:expr, $name:expr) => {
            if let Err(e) = $api.patch($name, &params, &Patch::Merge(&patch)).await {
                match e {
                    kube::Error::Api(ae) if ae.code == 404 => {}
                    _ => tracing::warn!("Failed to adopt {}: {}", $name, e),
                }
            }
        };
    }

    let sts: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
    let svc: Api<k8s_openapi::api::core::v1::Service> = Api::namespaced(client.clone(), namespace);
    let sa: Api<k8s_openapi::api::core::v1::ServiceAccount> =
        Api::namespaced(client.clone(), namespace);
    let role: Api<k8s_openapi::api::rbac::v1::Role> = Api::namespaced(client.clone(), namespace);
    let rb: Api<k8s_openapi::api::rbac::v1::RoleBinding> =
        Api::namespaced(client.clone(), namespace);
    let np: Api<k8s_openapi::api::networking::v1::NetworkPolicy> =
        Api::namespaced(client.clone(), namespace);
    let cr: Api<k8s_openapi::api::rbac::v1::ClusterRole> = Api::all(client.clone());
    let crb: Api<k8s_openapi::api::rbac::v1::ClusterRoleBinding> = Api::all(client.clone());

    adopt!(sts, "gateway-openshell");
    adopt!(svc, "gateway-openshell");
    adopt!(sa, "gateway-openshell");
    adopt!(role, "gateway-openshell-sandbox");
    adopt!(rb, "gateway-openshell-sandbox");
    adopt!(np, "gateway-openshell-sandbox-ssh");
    adopt!(cr, "gateway-openshell-sandbox-runtimeclass");
    adopt!(crb, "gateway-openshell-sandbox-runtimeclass");

    Ok(())
}
