// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use kube::{
    Client,
    api::{Api, DynamicObject, Patch, PatchParams},
    discovery::{Discovery, Scope},
};
use miette::{Context, IntoDiagnostic, Result, miette};
use serde::Deserialize;

pub async fn apply_yaml(client: Client, yaml: &str, namespace: &str) -> Result<()> {
    // Collect documents synchronously: serde_yml::Deserializer is not Send and
    // cannot be held across await points.
    let documents: Vec<serde_yml::Value> = serde_yml::Deserializer::from_str(yaml)
        .map(|doc| serde_yml::Value::deserialize(doc).into_diagnostic())
        .collect::<Result<_>>()?;

    let mut attempt = 0;
    let discovery = loop {
        match Discovery::new(client.clone()).run().await {
            Ok(d) => break d,
            Err(e) => {
                attempt += 1;
                if attempt >= 10 {
                    return Err(e).into_diagnostic();
                }
                tracing::warn!(attempt, error = %e, "API discovery failed, retrying");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    };

    for value in documents {
        if value.is_null() {
            continue;
        }

        let obj: DynamicObject = serde_yml::from_value(value).into_diagnostic()?;
        let types = obj
            .types
            .as_ref()
            .ok_or_else(|| miette!("YAML object is missing type metadata"))?;
        let gvk = kube::api::GroupVersionKind::try_from(types).into_diagnostic()?;
        let (ar, caps) = discovery
            .resolve_gvk(&gvk)
            .ok_or_else(|| miette!("unknown GVK: {}/{}", gvk.group, gvk.kind))?;

        let target_namespace = obj.metadata.namespace.as_deref().unwrap_or(namespace);
        let api: Api<DynamicObject> = if caps.scope == Scope::Namespaced {
            Api::namespaced_with(client.clone(), target_namespace, &ar)
        } else {
            Api::all_with(client.clone(), &ar)
        };

        let name = obj
            .metadata
            .name
            .clone()
            .ok_or_else(|| miette!("YAML object of kind '{}' is missing a name", types.kind))?;
        let params = PatchParams::apply("openshell-bootstrap").force();
        tracing::info!(
            "Applying {}/{} (namespace: {})",
            types.kind,
            name,
            target_namespace
        );
        api.patch(&name, &params, &Patch::Apply(&obj))
            .await
            .into_diagnostic()
            .context(format!("Failed to apply {} {}", types.kind, name))?;
    }
    Ok(())
}
