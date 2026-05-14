// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::{Namespace, Secret};
use kube::{
    Api, Client,
    api::{Patch, PatchParams, PostParams},
    core::ObjectMeta,
};
use miette::{Context, IntoDiagnostic, Result};
use std::collections::BTreeMap;

use crate::constants::{
    CLIENT_TLS_SECRET_NAME, GATEWAY_NODE_PORT, SERVER_CLIENT_CA_SECRET_NAME,
    SERVER_TLS_SECRET_NAME, SSH_HANDSHAKE_SECRET_NAME,
};
use crate::pki;
use std::io::Read;

pub async fn init_external_cluster(
    gateway_name: &str,
    namespace: &str,
    registry: &str,
    registry_username: Option<&str>,
    registry_token: Option<&str>,
    registry_authfile: Option<&str>,
) -> Result<()> {
    let client = Client::try_default()
        .await
        .into_diagnostic()
        .context("failed to create Kubernetes client")?;

    // 1. Create namespace
    ensure_namespace(client.clone(), namespace).await?;

    // 2. Reconcile PKI
    let bundle = if let Some(existing_bundle) =
        crate::reconcile::try_load_pki(client.clone(), namespace).await?
    {
        tracing::info!("Using existing PKI certificates from cluster");
        existing_bundle
    } else {
        tracing::info!("Generating new PKI certificates");
        let new_bundle = pki::generate_pki(&[]).context("failed to generate PKI")?;

        // 3. Create all TLS secrets in Kubernetes
        ensure_client_tls_secret(client.clone(), namespace, &new_bundle).await?;
        ensure_server_tls_secret(client.clone(), namespace, &new_bundle).await?;
        ensure_server_client_ca_secret(client.clone(), namespace, &new_bundle).await?;

        // Helm will automatically roll out the StatefulSet if secrets change (if the chart has checksum annotations)
        // or we can rely on standard Helm behavior. For now, no explicit restart is needed.

        new_bundle
    };

    // 4. Create SSH handshake secret in Kubernetes
    ensure_ssh_handshake_secret(client.clone(), namespace).await?;

    // 5. Deploy agent-sandbox CRD and controller
    let sandbox_manifest = include_str!("../../../deploy/kube/manifests/agent-sandbox.yaml");
    tracing::info!("Applying agent-sandbox manifests");
    crate::apply::apply_yaml(client.clone(), sandbox_manifest, namespace).await?;

    // 6. Adopt legacy Server-Side Applied resources so Helm can manage them
    tracing::info!("Adopting legacy resources for Helm");
    crate::reconcile::adopt_legacy_resources(client.clone(), namespace).await?;

    // 7. Deploy Gateway via Helm
    tracing::info!("Deploying gateway via Helm");

    // Helm binary path. Override with OPENSHELL_HELM_BIN for non-PATH installations.
    let helm_bin = std::env::var("OPENSHELL_HELM_BIN").unwrap_or_else(|_| "helm".to_string());

    // Helm chart path. Override with OPENSHELL_HELM_CHART_PATH.
    let chart_path = std::env::var("OPENSHELL_HELM_CHART_PATH")
        .unwrap_or_else(|_| "deploy/helm/openshell".to_string());

    // Bundled images directory. Set by installers to surface pre-built image archives.
    // When absent, images are pulled directly from the configured registry.
    let images_dir = std::env::var_os("OPENSHELL_IMAGES_DIR");
    let (gateway_tar, supervisor_tar, core_rock) = images_dir.as_ref().map_or_else(
        || (String::new(), String::new(), String::new()),
        |dir| {
            let dir = std::path::Path::new(dir);
            (
                dir.join("gateway.tar").to_string_lossy().into_owned(),
                dir.join("supervisor.tar").to_string_lossy().into_owned(),
                dir.join("openshell-core.rock")
                    .to_string_lossy()
                    .into_owned(),
            )
        },
    );

    let (has_bundled_images, is_core_rock) = if images_dir.is_none() {
        (false, false)
    } else if std::path::Path::new(&core_rock).exists() {
        (true, true)
    } else if std::path::Path::new(&gateway_tar).exists()
        && std::path::Path::new(&supervisor_tar).exists()
    {
        (true, false)
    } else {
        (false, false)
    };

    let target_registry = registry;

    if has_bundled_images {
        tracing::info!(
            "Found bundled images in OPENSHELL_IMAGES_DIR. Attempting to push to registry at {}...",
            target_registry
        );

        let skopeo_tmpdir = std::env::temp_dir();

        let (authfile_path, _tmp_dir_guard) = if let Some(path) = registry_authfile {
            (std::path::PathBuf::from(path), None)
        } else {
            // Validate and resolve credentials
            let effective_username = match (registry_username, registry_token) {
                (Some(u), Some(_)) => Some(u),
                (None, Some(_)) => Some("__token__"),
                (Some(_), None) => {
                    return Err(miette::miette!(
                        "A registry token/password must be provided when a username is specified."
                    ));
                }
                (None, None) => None,
            };

            if let Some(token) = registry_token {
                // Create tempdir for secure skopeo execution
                let tmp_dir = tempfile::tempdir()
                    .into_diagnostic()
                    .context("Failed to create temporary directory for secure auth")?;
                let authfile = tmp_dir.path().join("auth.json");

                let u = effective_username.unwrap();
                let mut login_cmd = std::process::Command::new("skopeo");
                login_cmd.env("CONTAINERS_REGISTRIES_CONF", "/dev/null");
                login_cmd
                    .arg("login")
                    .arg("--authfile")
                    .arg(authfile.as_os_str())
                    .arg("--username")
                    .arg(u)
                    .arg("--password-stdin")
                    .arg(target_registry);

                // Pipe token to stdin
                login_cmd.stdin(std::process::Stdio::piped());
                login_cmd.stdout(std::process::Stdio::null());
                login_cmd.stderr(std::process::Stdio::piped());

                let mut child = login_cmd
                    .spawn()
                    .into_diagnostic()
                    .context("Failed to spawn skopeo login")?;
                if let Some(mut stdin) = child.stdin.take() {
                    use std::io::Write;
                    stdin.write_all(token.as_bytes()).into_diagnostic()?;
                }

                let mut stderr_output = String::new();
                if let Some(mut stderr) = child.stderr.take() {
                    use std::io::Read;
                    let _ = stderr.read_to_string(&mut stderr_output);
                }

                let status = child
                    .wait()
                    .into_diagnostic()
                    .context("Failed to wait on skopeo login")?;
                if !status.success() {
                    return Err(miette::miette!(
                        "Failed to authenticate with registry using skopeo login. Error:\n{}",
                        stderr_output.trim()
                    ));
                }
                (authfile, Some(tmp_dir))
            } else {
                (std::path::PathBuf::new(), None)
            }
        };

        let has_authfile = registry_authfile.is_some() || registry_token.is_some();

        let authfile_arg = if has_authfile {
            Some(authfile_path.as_path())
        } else {
            None
        };

        let push_success = if is_core_rock {
            let target_image = format!("{target_registry}/openshell-core:local");
            let ok = skopeo_copy(
                &core_rock,
                &target_image,
                &skopeo_tmpdir,
                authfile_arg,
                "openshell-core",
            );
            if ok {
                tracing::info!("Successfully pushed unified openshell-core to local registry.");
            }
            ok
        } else {
            let target_gateway = format!("{target_registry}/openshell-gateway:dev");
            let target_supervisor = format!("{target_registry}/openshell-supervisor:dev");
            let ok1 = skopeo_copy(
                &gateway_tar,
                &target_gateway,
                &skopeo_tmpdir,
                authfile_arg,
                "gateway",
            );
            let ok2 = skopeo_copy(
                &supervisor_tar,
                &target_supervisor,
                &skopeo_tmpdir,
                authfile_arg,
                "supervisor",
            );
            let ok = ok1 && ok2;
            if ok {
                tracing::info!("Successfully pushed gateway and supervisor to registry.");
            }
            ok
        };

        if !push_success {
            tracing::warn!("Failed to push bundled images to the registry.");
            tracing::warn!(
                "For local development, please run \"sudo snap install registry\" and use \"localhost:5000\", or point to your preferred registry with --registry."
            );

            // We exit early because without the local images available in the registry,
            // the Helm deployment will fail to pull them and the pods will hang in ImagePullBackOff.
            return Err(miette::miette!(
                "Local registry is not available or unreachable at {}.",
                target_registry
            ));
        }
    }

    let mut helm_cmd = std::process::Command::new(helm_bin);
    helm_cmd
        .arg("upgrade")
        .arg("--install")
        .arg("gateway")
        .arg(&chart_path)
        .arg("--namespace")
        .arg(namespace)
        .arg("--wait")
        .arg("--timeout")
        .arg("2m");

    if has_bundled_images {
        if is_core_rock {
            helm_cmd
                .arg("--set")
                .arg(format!("image.repository={target_registry}/openshell-core"))
                .arg("--set")
                .arg("image.tag=local")
                .arg("--set")
                .arg("image.pullPolicy=Always")
                .arg("--set")
                .arg(format!(
                    "supervisorImage={target_registry}/openshell-core:local"
                ))
                .arg("--set")
                .arg("supervisorImagePullPolicy=Always");
        } else {
            helm_cmd
                .arg("--set")
                .arg(format!(
                    "image.repository={target_registry}/openshell-gateway"
                ))
                .arg("--set")
                .arg("image.tag=dev")
                .arg("--set")
                .arg("image.pullPolicy=Always")
                .arg("--set")
                .arg(format!(
                    "supervisorImage={target_registry}/openshell-supervisor:dev"
                ))
                .arg("--set")
                .arg("supervisorImagePullPolicy=Always");
        }
    }

    let status = helm_cmd
        .status()
        .into_diagnostic()
        .context("Failed to execute helm upgrade")?;

    if !status.success() {
        return Err(miette::miette!(
            "Helm upgrade failed with status: {}",
            status
        ));
    }

    // 8. Save client TLS materials locally for CLI to connect
    let home_dir =
        dirs::home_dir().ok_or_else(|| miette::miette!("Could not find home directory"))?;
    let mtls_dir = home_dir
        .join(".config")
        .join("openshell")
        .join("gateways")
        .join(gateway_name)
        .join("mtls");
    std::fs::create_dir_all(&mtls_dir).into_diagnostic()?;

    std::fs::write(mtls_dir.join("tls.crt"), &bundle.client_cert_pem).into_diagnostic()?;
    std::fs::write(mtls_dir.join("tls.key"), &bundle.client_key_pem).into_diagnostic()?;
    std::fs::write(mtls_dir.join("ca.crt"), &bundle.ca_cert_pem).into_diagnostic()?;

    // 9. Save gateway metadata and set active gateway
    let metadata = crate::metadata::GatewayMetadata {
        name: gateway_name.to_string(),
        gateway_endpoint: format!("https://127.0.0.1:{GATEWAY_NODE_PORT}"),
        is_remote: false,
        gateway_port: GATEWAY_NODE_PORT,
        remote_host: None,
        resolved_host: None,
        auth_mode: Some("mtls".to_string()),
        edge_team_domain: None,
        edge_auth_url: None,
        oidc_issuer: None,
        oidc_client_id: None,
        oidc_audience: None,
        oidc_scopes: None,
        vm_driver_state_dir: None,
    };
    crate::metadata::store_gateway_metadata(gateway_name, &metadata)?;
    crate::metadata::save_active_gateway(gateway_name)?;

    Ok(())
}

/// Run `skopeo copy oci-archive:<src> docker://<dst>` and return whether it succeeded.
/// Logs a warning on failure; the caller decides whether failure is fatal.
// TODO(k8s-review): --dest-tls-verify=false is unconditional. This is intentional
// for the snap/localhost registry workflow but silently disables TLS verification
// for external registries. Should be restricted to loopback hosts or gated on an
// explicit --insecure flag.
fn skopeo_copy(
    src: &str,
    dst: &str,
    tmpdir: &std::path::Path,
    authfile: Option<&std::path::Path>,
    label: &str,
) -> bool {
    let mut cmd = std::process::Command::new("skopeo");
    cmd.env("CONTAINERS_REGISTRIES_CONF", "/dev/null");
    cmd.arg("--insecure-policy")
        .arg("copy")
        .arg("--dest-tls-verify=false");
    if let Some(path) = authfile {
        cmd.arg("--authfile").arg(path);
    }
    cmd.arg("--tmpdir").arg(tmpdir);
    cmd.arg(format!("oci-archive:{src}"))
        .arg(format!("docker://{dst}"));

    match cmd.output() {
        Ok(out) if out.status.success() => true,
        Ok(out) => {
            tracing::warn!(
                "Failed to push {} image. Skopeo output:\n{}",
                label,
                String::from_utf8_lossy(&out.stderr).trim()
            );
            false
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::warn!("'skopeo' command not found. Please install skopeo to push images.");
            false
        }
        Err(e) => {
            tracing::warn!("Failed to execute skopeo ({}): {}", label, e);
            false
        }
    }
}

async fn ensure_namespace(client: Client, namespace: &str) -> Result<()> {
    let namespaces: Api<Namespace> = Api::all(client);

    let ns = Namespace {
        metadata: ObjectMeta {
            name: Some(namespace.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    let params = PatchParams::apply("openshell-bootstrap").force();
    namespaces
        .patch(namespace, &params, &Patch::Apply(&ns))
        .await
        .into_diagnostic()
        .context("failed to apply namespace")?;

    Ok(())
}

async fn ensure_client_tls_secret(
    client: Client,
    namespace: &str,
    bundle: &pki::PkiBundle,
) -> Result<()> {
    let secrets: Api<Secret> = Api::namespaced(client, namespace);

    let mut data = BTreeMap::new();
    data.insert(
        "tls.crt".to_string(),
        ByteString(bundle.client_cert_pem.as_bytes().to_vec()),
    );
    data.insert(
        "tls.key".to_string(),
        ByteString(bundle.client_key_pem.as_bytes().to_vec()),
    );
    data.insert(
        "ca.crt".to_string(),
        ByteString(bundle.ca_cert_pem.as_bytes().to_vec()),
    );

    let secret = Secret {
        metadata: ObjectMeta {
            name: Some(CLIENT_TLS_SECRET_NAME.to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        type_: Some("Opaque".to_string()),
        data: Some(data),
        ..Default::default()
    };

    let params = PatchParams::apply("openshell-bootstrap").force();
    secrets
        .patch(CLIENT_TLS_SECRET_NAME, &params, &Patch::Apply(&secret))
        .await
        .into_diagnostic()
        .context("failed to apply client TLS secret")?;

    Ok(())
}

async fn ensure_ssh_handshake_secret(client: Client, namespace: &str) -> Result<()> {
    let secrets: Api<Secret> = Api::namespaced(client, namespace);

    // TODO(k8s-review): This get-then-create has a TOCTOU race under concurrent
    // cluster init runs (a 409 from the create will surface as an error). Unlike
    // the TLS helpers, force-overwriting is wrong here: the secret is shared with
    // running sandbox pods and replacing it breaks SSH tunnel auth until pods
    // restart. Treating 409 as success is probably the right fix, but needs
    // review from someone familiar with the K8s secret lifecycle and the
    // re-init story before changing.
    if secrets.get(SSH_HANDSHAKE_SECRET_NAME).await.is_ok() {
        return Ok(());
    }

    let mut buf = [0u8; 32];
    std::fs::File::open("/dev/urandom")
        .into_diagnostic()
        .context("failed to open /dev/urandom")?
        .read_exact(&mut buf)
        .into_diagnostic()
        .context("failed to read from /dev/urandom")?;

    let mut secret_val = String::with_capacity(64);
    for b in buf {
        use std::fmt::Write;
        write!(&mut secret_val, "{b:02x}").unwrap();
    }
    let mut data = BTreeMap::new();
    data.insert(
        "secret".to_string(),
        ByteString(secret_val.as_bytes().to_vec()),
    );

    let secret = Secret {
        metadata: ObjectMeta {
            name: Some(SSH_HANDSHAKE_SECRET_NAME.to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        type_: Some("Opaque".to_string()),
        data: Some(data),
        ..Default::default()
    };

    secrets
        .create(&PostParams::default(), &secret)
        .await
        .into_diagnostic()
        .context("failed to create ssh handshake secret")?;

    Ok(())
}

async fn ensure_server_tls_secret(
    client: Client,
    namespace: &str,
    bundle: &pki::PkiBundle,
) -> Result<()> {
    let secrets: Api<Secret> = Api::namespaced(client, namespace);
    let mut data = BTreeMap::new();
    data.insert(
        "tls.crt".to_string(),
        ByteString(bundle.server_cert_pem.as_bytes().to_vec()),
    );
    data.insert(
        "tls.key".to_string(),
        ByteString(bundle.server_key_pem.as_bytes().to_vec()),
    );

    let secret = Secret {
        metadata: ObjectMeta {
            name: Some(SERVER_TLS_SECRET_NAME.to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        type_: Some("kubernetes.io/tls".to_string()),
        data: Some(data),
        ..Default::default()
    };

    let params = PatchParams::apply("openshell-bootstrap").force();
    secrets
        .patch(SERVER_TLS_SECRET_NAME, &params, &Patch::Apply(&secret))
        .await
        .into_diagnostic()
        .context("failed to apply server TLS secret")?;

    Ok(())
}

async fn ensure_server_client_ca_secret(
    client: Client,
    namespace: &str,
    bundle: &pki::PkiBundle,
) -> Result<()> {
    let secrets: Api<Secret> = Api::namespaced(client, namespace);
    let mut data = BTreeMap::new();
    data.insert(
        "ca.crt".to_string(),
        ByteString(bundle.ca_cert_pem.as_bytes().to_vec()),
    );

    let secret = Secret {
        metadata: ObjectMeta {
            name: Some(SERVER_CLIENT_CA_SECRET_NAME.to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        type_: Some("Opaque".to_string()),
        data: Some(data),
        ..Default::default()
    };

    let params = PatchParams::apply("openshell-bootstrap").force();
    secrets
        .patch(
            SERVER_CLIENT_CA_SECRET_NAME,
            &params,
            &Patch::Apply(&secret),
        )
        .await
        .into_diagnostic()
        .context("failed to apply server client CA secret")?;

    Ok(())
}
