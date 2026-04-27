// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Maps LXD instance events to the compute-driver watch protocol.

use crate::client::{Instance, LxdApiError, LxdClient, LxdEvent};
use crate::instance::{CONFIG_MANAGED, CONFIG_SANDBOX_ID, CONFIG_SANDBOX_NAME, short_id};
use futures::Stream;
use openshell_core::ComputeDriverError;
use openshell_core::proto::compute::v1::{
    DriverCondition, DriverSandbox, DriverSandboxStatus, WatchSandboxesDeletedEvent,
    WatchSandboxesEvent, WatchSandboxesSandboxEvent, watch_sandboxes_event,
};
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info, warn};

// Condition reason constants.
const CONDITION_RUNNING: &str = "InstanceRunning";
const CONDITION_STARTING: &str = "InstanceStarting";
const CONDITION_STOPPED: &str = "InstanceStopped";

pub type WatchStream =
    Pin<Box<dyn Stream<Item = Result<WatchSandboxesEvent, ComputeDriverError>> + Send>>;

/// Build a `WatchSandboxesEvent` carrying a sandbox snapshot.
fn sandbox_event(sandbox: DriverSandbox) -> WatchSandboxesEvent {
    WatchSandboxesEvent {
        payload: Some(watch_sandboxes_event::Payload::Sandbox(
            WatchSandboxesSandboxEvent {
                sandbox: Some(sandbox),
            },
        )),
    }
}

/// Build a `WatchSandboxesEvent` for a deleted sandbox.
fn deleted_event(sandbox_id: String) -> WatchSandboxesEvent {
    WatchSandboxesEvent {
        payload: Some(watch_sandboxes_event::Payload::Deleted(
            WatchSandboxesDeletedEvent { sandbox_id },
        )),
    }
}

/// Start a watch stream that emits current state and live events.
///
/// The stream first emits a snapshot of all currently-managed instances
/// (initial state sync), then delivers live lifecycle events.
///
/// Callers are responsible for reconnecting when the stream terminates.
pub async fn start_watch(client: LxdClient) -> Result<WatchStream, LxdApiError> {
    let (tx, rx) = mpsc::channel::<Result<WatchSandboxesEvent, ComputeDriverError>>(256);

    // 1. Subscribe to events first so we don't miss any during the list.
    let mut event_rx = client.events_stream().await?;

    // 2. List existing instances for initial state sync.
    let existing = client.list_instances().await?;

    for inst in &existing {
        if inst.config.get(CONFIG_MANAGED).is_some_and(|v| v == "true") {
            if let Some(sandbox) = driver_sandbox_from_instance(inst) {
                if tx.send(Ok(sandbox_event(sandbox))).await.is_err() {
                    return Err(LxdApiError::Connection(
                        "watch receiver dropped during initial sync".into(),
                    ));
                }
            }
        }
    }

    // 3. Stream live events.
    let watch_client = client.clone();
    tokio::spawn(async move {
        while let Some(result) = event_rx.recv().await {
            match result {
                Ok(event) => {
                    if let Some(we) = map_lxd_event(&event, &watch_client).await {
                        if tx.send(Ok(we)).await.is_err() {
                            return;
                        }
                    }
                }
                Err(e) => {
                    if tx
                        .send(Err(ComputeDriverError::Message(e.to_string())))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
        warn!("LXD event stream ended unexpectedly; watch_loop will reconnect");
        let _ = tx
            .send(Err(ComputeDriverError::Message(
                "LXD event stream ended unexpectedly".to_string(),
            )))
            .await;
    });

    Ok(Box::pin(ReceiverStream::new(rx)))
}

/// Map a LXD lifecycle event to an optional watch event.
async fn map_lxd_event(event: &LxdEvent, client: &LxdClient) -> Option<WatchSandboxesEvent> {
    // Only process lifecycle events.
    if event.event_type != "lifecycle" {
        return None;
    }

    let metadata = event.metadata.as_ref()?;
    let action = &metadata.action;

    // LXD lifecycle actions look like "instance-started", "instance-deleted", etc.
    // Extract the instance name from the source URL (e.g., "/1.0/instances/name").
    let inst_name = metadata.source.rsplit('/').next().unwrap_or_default();

    if inst_name.is_empty() {
        return None;
    }

    // Only handle our managed instances.
    if !inst_name.starts_with("openshell-sandbox-") {
        return None;
    }

    match action.as_str() {
        "instance-deleted" => {
            // For deletion, we need to figure out the sandbox ID.
            // Try to get it from context or use the instance name.
            let sandbox_id = metadata
                .context
                .get("sandbox-id")
                .cloned()
                .unwrap_or_else(|| inst_name.to_string());
            Some(deleted_event(sandbox_id))
        }
        "instance-created" | "instance-started" | "instance-stopped" | "instance-shutdown"
        | "instance-restarted" => {
            // Fetch current instance state.
            match client.get_instance(inst_name).await {
                Ok(inst) => driver_sandbox_from_instance(&inst).map(sandbox_event),
                Err(LxdApiError::NotFound(_)) => {
                    info!(
                        instance = %inst_name,
                        action = %action,
                        "Instance already removed when inspecting after event"
                    );
                    None
                }
                Err(e) => {
                    warn!(
                        instance = %inst_name,
                        error = %e,
                        "Failed to inspect instance after event"
                    );
                    None
                }
            }
        }
        _ => {
            debug!(action = %action, "Ignoring unhandled LXD lifecycle event");
            None
        }
    }
}

/// Construct a `DriverSandbox` from common fields.
fn build_driver_sandbox(
    sandbox_id: String,
    sandbox_name: String,
    instance_name: String,
    instance_id: String,
    condition: DriverCondition,
    deleting: bool,
) -> DriverSandbox {
    DriverSandbox {
        id: sandbox_id,
        name: sandbox_name,
        namespace: String::new(),
        spec: None,
        status: Some(DriverSandboxStatus {
            sandbox_name: instance_name,
            instance_id,
            agent_fd: String::new(),
            sandbox_fd: String::new(),
            conditions: vec![condition],
            deleting,
        }),
    }
}

/// Build a `DriverSandbox` from a LXD instance.
pub fn driver_sandbox_from_instance(inst: &Instance) -> Option<DriverSandbox> {
    let sandbox_id = inst.config.get(CONFIG_SANDBOX_ID)?.clone();
    let sandbox_name = inst
        .config
        .get(CONFIG_SANDBOX_NAME)
        .cloned()
        .unwrap_or_default();

    let condition = condition_from_status(&inst.status, inst.status_code);
    let deleting = inst.status.to_lowercase() == "deleting";

    Some(build_driver_sandbox(
        sandbox_id,
        sandbox_name,
        inst.name.clone(),
        short_id(&inst.name),
        condition,
        deleting,
    ))
}

/// Derive a `DriverCondition` from LXD instance status.
fn condition_from_status(status: &str, status_code: i32) -> DriverCondition {
    let (status_val, reason, message) = match status.to_lowercase().as_str() {
        "running" => ("True", CONDITION_RUNNING, String::new()),
        "starting" => ("False", CONDITION_STARTING, String::new()),
        "stopped" => (
            "False",
            CONDITION_STOPPED,
            format!("Instance stopped (code {status_code})"),
        ),
        "frozen" => ("False", "InstanceFrozen", "Instance is frozen".to_string()),
        "error" => (
            "False",
            "InstanceError",
            format!("Instance in error state (code {status_code})"),
        ),
        other => (
            "Unknown",
            "Unknown",
            format!("Unknown instance status: {other}"),
        ),
    };

    DriverCondition {
        r#type: "Ready".to_string(),
        status: status_val.to_string(),
        reason: reason.to_string(),
        message,
        last_transition_time: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn test_instance(name: &str, status: &str, sandbox_id: &str) -> Instance {
        let mut config = HashMap::new();
        config.insert(CONFIG_SANDBOX_ID.to_string(), sandbox_id.to_string());
        config.insert(CONFIG_SANDBOX_NAME.to_string(), name.to_string());
        config.insert(CONFIG_MANAGED.to_string(), "true".to_string());

        Instance {
            name: format!("openshell-sandbox-{name}"),
            status: status.to_string(),
            status_code: if status == "Running" { 103 } else { 102 },
            config,
            instance_type: "container".to_string(),
            architecture: "x86_64".to_string(),
            created_at: String::new(),
            last_used_at: String::new(),
        }
    }

    #[test]
    fn condition_running_instance() {
        let cond = condition_from_status("Running", 103);
        assert_eq!(cond.r#type, "Ready");
        assert_eq!(cond.status, "True");
        assert_eq!(cond.reason, "InstanceRunning");
    }

    #[test]
    fn condition_stopped_instance() {
        let cond = condition_from_status("Stopped", 102);
        assert_eq!(cond.status, "False");
        assert_eq!(cond.reason, "InstanceStopped");
    }

    #[test]
    fn driver_sandbox_from_running_instance() {
        let inst = test_instance("my-sandbox", "Running", "test-id-123");
        let sandbox = driver_sandbox_from_instance(&inst).unwrap();
        assert_eq!(sandbox.id, "test-id-123");
        assert_eq!(sandbox.name, "my-sandbox");
        assert_eq!(
            sandbox.status.as_ref().unwrap().conditions[0].status,
            "True"
        );
    }

    #[test]
    fn driver_sandbox_skips_unmanaged() {
        let inst = Instance {
            name: "other-instance".to_string(),
            status: "Running".to_string(),
            status_code: 103,
            config: HashMap::new(),
            instance_type: "container".to_string(),
            architecture: "x86_64".to_string(),
            created_at: String::new(),
            last_used_at: String::new(),
        };
        assert!(driver_sandbox_from_instance(&inst).is_none());
    }
}
