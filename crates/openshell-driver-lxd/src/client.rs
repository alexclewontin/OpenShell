// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Thin async HTTP client for the LXD REST API over a Unix socket.

use http_body_util::{BodyExt, Full};
use hyper::Request;
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tracing::debug;

/// LXD API version prefix.
const API_VERSION: &str = "1.0";

/// Timeout for individual LXD API calls.
const API_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum allowed size for the event stream line buffer (1 MB).
const MAX_EVENT_BUFFER: usize = 1_048_576;

#[derive(Debug, thiserror::Error)]
pub enum LxdApiError {
    #[error("LXD API not found (404): {0}")]
    NotFound(String),
    #[error("LXD API conflict (409): {0}")]
    Conflict(String),
    #[error("LXD API error ({status}): {message}")]
    Api { status: u16, message: String },
    #[error("connection error: {0}")]
    Connection(String),
    #[error("timeout after {0:?}")]
    Timeout(Duration),
    #[error("JSON error: {0}")]
    Json(String),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("LXD async operation failed: {0}")]
    OperationFailed(String),
}

/// Maximum resource name length.
const MAX_NAME_LEN: usize = 63;

/// Validate that a resource name is safe for URL path interpolation.
///
/// Valid LXD instance names start with a letter or digit and contain only
/// alphanumerics and hyphens. Names longer than [`MAX_NAME_LEN`] are rejected.
pub(crate) fn validate_name(name: &str) -> Result<(), LxdApiError> {
    if name.is_empty() {
        return Err(LxdApiError::InvalidInput(
            "name must not be empty".to_string(),
        ));
    }
    if name.len() > MAX_NAME_LEN {
        return Err(LxdApiError::InvalidInput(format!(
            "name exceeds maximum length of {MAX_NAME_LEN} characters (got {})",
            name.len()
        )));
    }
    let bytes = name.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() {
        return Err(LxdApiError::InvalidInput(format!(
            "name must start with an alphanumeric character: {name:?}"
        )));
    }
    if !bytes
        .iter()
        .all(|&b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err(LxdApiError::InvalidInput(format!(
            "name contains invalid characters (only alphanumerics and hyphens allowed): {name:?}"
        )));
    }
    Ok(())
}

/// An instance state returned by the LXD API.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[allow(dead_code)]
pub struct InstanceState {
    pub status: String,
    pub status_code: i32,
    #[serde(default)]
    pub processes: i64,
    #[serde(default)]
    pub network: Option<HashMap<String, NetworkState>>,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[allow(dead_code)]
pub struct NetworkState {
    #[serde(default)]
    pub addresses: Vec<NetworkAddress>,
    #[serde(default)]
    pub state: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[allow(dead_code)]
pub struct NetworkAddress {
    pub family: String,
    pub address: String,
    pub scope: String,
}

/// Instance metadata returned by GET /1.0/instances/<name>.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[allow(dead_code)]
pub struct Instance {
    pub name: String,
    #[serde(default)]
    pub status: String,
    pub status_code: i32,
    #[serde(default)]
    pub config: HashMap<String, String>,
    #[serde(rename = "type")]
    #[serde(default)]
    pub instance_type: String,
    #[serde(default)]
    pub architecture: String,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub last_used_at: String,
}

/// A LXD lifecycle event from the events stream.
#[derive(Debug, Clone, serde::Deserialize)]
#[allow(dead_code)]
pub struct LxdEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(default)]
    pub metadata: Option<EventMetadata>,
    #[serde(default)]
    pub timestamp: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct EventMetadata {
    #[serde(default)]
    pub action: String,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub requestor: Option<EventRequestor>,
    #[serde(default)]
    pub context: HashMap<String, String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[allow(dead_code)]
pub struct EventRequestor {
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub protocol: String,
}

// ── Client ───────────────────────────────────────────────────────────────

/// Async LXD REST API client communicating over a Unix socket.
#[derive(Debug, Clone)]
pub struct LxdClient {
    socket_path: PathBuf,
    /// LXD project to scope all instance operations to.
    project: String,
}

impl LxdClient {
    /// Create a new client targeting the given socket path and project.
    #[must_use]
    pub fn new(socket_path: PathBuf, project: String) -> Self {
        Self {
            socket_path,
            project,
        }
    }

    /// Return a query-string suffix that scopes requests to the configured
    /// project. Returns `"?project=<name>"` when using a non-default
    /// project, or an empty string for the default project.
    fn project_query(&self) -> String {
        if self.project.is_empty() || self.project == "default" {
            String::new()
        } else {
            format!("?project={}", url_encode(&self.project))
        }
    }

    /// Like [`project_query`] but for paths that already have query
    /// parameters.  Returns `"&project=<name>"` or an empty string.
    fn project_query_extra(&self) -> String {
        if self.project.is_empty() || self.project == "default" {
            String::new()
        } else {
            format!("&project={}", url_encode(&self.project))
        }
    }

    /// Open a new HTTP/1.1 connection to the LXD socket.
    async fn connect(
        &self,
    ) -> Result<hyper::client::conn::http1::SendRequest<Full<Bytes>>, LxdApiError> {
        let stream = UnixStream::connect(&self.socket_path)
            .await
            .map_err(|e| LxdApiError::Connection(format!("{}: {e}", self.socket_path.display())))?;

        let (sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|e| LxdApiError::Connection(e.to_string()))?;

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                debug!(error = %e, "LXD API connection closed");
            }
        });

        Ok(sender)
    }

    // ── Request infrastructure ───────────────────────────────────────────

    /// Build an HTTP request from components.
    fn build_request(
        method: hyper::Method,
        path: &str,
        body: Full<Bytes>,
        content_type: Option<&str>,
    ) -> Request<Full<Bytes>> {
        let mut builder = Request::builder()
            .method(method)
            .uri(format!("http://localhost{path}"))
            .header("Host", "localhost");
        if let Some(ct) = content_type {
            builder = builder.header("Content-Type", ct);
        }
        builder.body(body).expect("valid request")
    }

    /// Send a pre-built HTTP request and return status + body bytes.
    async fn send_request(
        &self,
        req: Request<Full<Bytes>>,
        timeout: Duration,
    ) -> Result<(hyper::StatusCode, Bytes), LxdApiError> {
        let mut sender = self.connect().await?;
        let response = tokio::time::timeout(timeout, sender.send_request(req))
            .await
            .map_err(|_| LxdApiError::Timeout(timeout))?
            .map_err(|e| LxdApiError::Connection(e.to_string()))?;
        let status = response.status();
        let bytes = tokio::time::timeout(timeout, response.into_body().collect())
            .await
            .map_err(|_| LxdApiError::Timeout(timeout))?
            .map_err(|e| LxdApiError::Connection(e.to_string()))?
            .to_bytes();
        Ok((status, bytes))
    }

    /// Perform an HTTP request and return status + body bytes.
    async fn request(
        &self,
        method: hyper::Method,
        path: &str,
        body: Option<&Value>,
        timeout: Duration,
    ) -> Result<(hyper::StatusCode, Bytes), LxdApiError> {
        let (full_body, content_type) = match body {
            Some(json) => {
                let payload =
                    serde_json::to_vec(json).map_err(|e| LxdApiError::Json(e.to_string()))?;
                (Full::new(Bytes::from(payload)), Some("application/json"))
            }
            None => (Full::new(Bytes::new()), None),
        };
        let req = Self::build_request(
            method,
            &format!("/{API_VERSION}{path}"),
            full_body,
            content_type,
        );
        self.send_request(req, timeout).await
    }

    /// Perform a request and deserialize the LXD response metadata.
    ///
    /// LXD wraps responses in `{"type":"sync","metadata":{...}}` or
    /// `{"type":"async","operation":"...","metadata":{...}}`.
    async fn request_json<T: DeserializeOwned + Default>(
        &self,
        method: hyper::Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<T, LxdApiError> {
        let (status, bytes) = self.request(method, path, body, API_TIMEOUT).await?;
        if status.is_success() {
            let wrapper: LxdResponse<T> = serde_json::from_slice(&bytes).map_err(|e| {
                LxdApiError::Json(format!("{e}: {}", String::from_utf8_lossy(&bytes)))
            })?;
            match wrapper.error_code {
                Some(code) if code >= 400 => Err(error_from_lxd(
                    code as u16,
                    &wrapper.error.unwrap_or_default(),
                )),
                _ => wrapper.metadata.ok_or_else(|| {
                    LxdApiError::Json("response missing metadata field".to_string())
                }),
            }
        } else {
            Err(error_from_response(status.as_u16(), &bytes))
        }
    }

    /// Perform a request that returns an async operation, and wait for it.
    async fn request_and_wait(
        &self,
        method: hyper::Method,
        path: &str,
        body: Option<&Value>,
        timeout: Duration,
    ) -> Result<(), LxdApiError> {
        let (status, bytes) = self.request(method, path, body, timeout).await?;
        if !status.is_success() {
            return Err(error_from_response(status.as_u16(), &bytes));
        }

        let wrapper: LxdResponse<Value> = serde_json::from_slice(&bytes)
            .map_err(|e| LxdApiError::Json(format!("{e}: {}", String::from_utf8_lossy(&bytes))))?;

        // Check for error in the response wrapper itself.
        if let Some(code) = wrapper.error_code {
            if code >= 400 {
                return Err(error_from_lxd(
                    code as u16,
                    &wrapper.error.unwrap_or_default(),
                ));
            }
        }

        // Sync operations complete immediately.
        if wrapper.response_type == "sync" {
            return Ok(());
        }

        // Async operations: wait on the operation URL.
        if let Some(operation) = &wrapper.operation {
            self.wait_for_operation(operation, timeout).await?;
        }

        Ok(())
    }

    /// Wait for an async LXD operation to complete.
    async fn wait_for_operation(
        &self,
        operation_url: &str,
        timeout: Duration,
    ) -> Result<(), LxdApiError> {
        // operation_url is like /1.0/operations/<uuid>
        // We call /1.0/operations/<uuid>/wait?timeout=<secs>
        let timeout_secs = timeout.as_secs().max(30);
        let wait_path = format!("{operation_url}/wait?timeout={timeout_secs}");
        let (status, bytes) = self
            .request(
                hyper::Method::GET,
                &wait_path,
                None,
                timeout + Duration::from_secs(5),
            )
            .await?;

        if !status.is_success() {
            return Err(error_from_response(status.as_u16(), &bytes));
        }

        let wrapper: LxdResponse<OperationStatus> = serde_json::from_slice(&bytes)
            .map_err(|e| LxdApiError::Json(format!("{e}: {}", String::from_utf8_lossy(&bytes))))?;

        // The wait_path is already a full path including the API version,
        // but we call it via request() which prepends /1.0 — fix by using
        // the raw path. Actually, operation_url already starts with /1.0,
        // so we need to strip it for request() or use send_request directly.
        // Let's check the operation status in the response.
        if let Some(meta) = &wrapper.metadata {
            if meta.status_code >= 400 {
                return Err(LxdApiError::OperationFailed(format!(
                    "operation {}: {} (code {})",
                    meta.description.as_deref().unwrap_or("unknown"),
                    meta.err.as_deref().unwrap_or("failed"),
                    meta.status_code,
                )));
            }
        }

        Ok(())
    }

    // ── Instance operations ──────────────────────────────────────────────

    /// Create an instance from a JSON spec.
    pub async fn create_instance(&self, spec: &Value) -> Result<(), LxdApiError> {
        let pq = self.project_query();
        self.request_and_wait(
            hyper::Method::POST,
            &format!("/instances{pq}"),
            Some(spec),
            Duration::from_secs(300),
        )
        .await
    }

    /// Start an instance by name.
    pub async fn start_instance(&self, name: &str) -> Result<(), LxdApiError> {
        validate_name(name)?;
        let pq = self.project_query();
        let body = serde_json::json!({"action": "start"});
        self.request_and_wait(
            hyper::Method::PUT,
            &format!("/instances/{name}/state{pq}"),
            Some(&body),
            API_TIMEOUT,
        )
        .await
    }

    /// Stop an instance with a timeout.
    pub async fn stop_instance(&self, name: &str, timeout_secs: u32) -> Result<(), LxdApiError> {
        validate_name(name)?;
        let pq = self.project_query();
        let body = serde_json::json!({
            "action": "stop",
            "timeout": timeout_secs,
            "force": false,
        });
        let http_timeout = Duration::from_secs(u64::from(timeout_secs) + 10);
        self.request_and_wait(
            hyper::Method::PUT,
            &format!("/instances/{name}/state{pq}"),
            Some(&body),
            http_timeout,
        )
        .await
    }

    /// Force-stop an instance.
    pub async fn force_stop_instance(&self, name: &str) -> Result<(), LxdApiError> {
        validate_name(name)?;
        let pq = self.project_query();
        let body = serde_json::json!({
            "action": "stop",
            "force": true,
        });
        self.request_and_wait(
            hyper::Method::PUT,
            &format!("/instances/{name}/state{pq}"),
            Some(&body),
            API_TIMEOUT,
        )
        .await
    }

    /// Delete an instance.
    pub async fn delete_instance(&self, name: &str) -> Result<(), LxdApiError> {
        validate_name(name)?;
        let pq = self.project_query();
        self.request_and_wait(
            hyper::Method::DELETE,
            &format!("/instances/{name}{pq}"),
            None,
            API_TIMEOUT,
        )
        .await
    }

    /// Get instance metadata.
    pub async fn get_instance(&self, name: &str) -> Result<Instance, LxdApiError> {
        validate_name(name)?;
        let pq = self.project_query();
        self.request_json(hyper::Method::GET, &format!("/instances/{name}{pq}"), None)
            .await
    }

    /// Get instance state (running status, network, etc.).
    #[allow(dead_code)]
    pub async fn get_instance_state(&self, name: &str) -> Result<InstanceState, LxdApiError> {
        validate_name(name)?;
        let pq = self.project_query();
        self.request_json(
            hyper::Method::GET,
            &format!("/instances/{name}/state{pq}"),
            None,
        )
        .await
    }

    /// List all instances, optionally filtered by a config key prefix.
    pub async fn list_instances(&self) -> Result<Vec<Instance>, LxdApiError> {
        let pq = self.project_query_extra();
        self.request_json(
            hyper::Method::GET,
            &format!("/instances?recursion=1{pq}"),
            None,
        )
        .await
    }

    /// Execute a command inside an instance.
    ///
    /// Used to run health checks and set up the sandbox environment.
    pub async fn exec_instance(&self, name: &str, command: &[&str]) -> Result<(), LxdApiError> {
        validate_name(name)?;
        let pq = self.project_query();
        let body = serde_json::json!({
            "command": command,
            "wait-for-websocket": false,
            "record-output": false,
        });
        self.request_and_wait(
            hyper::Method::POST,
            &format!("/instances/{name}/exec{pq}"),
            Some(&body),
            API_TIMEOUT,
        )
        .await
    }

    /// Push a file into an instance.
    pub async fn push_file(
        &self,
        name: &str,
        path: &str,
        content: &[u8],
        mode: &str,
    ) -> Result<(), LxdApiError> {
        validate_name(name)?;
        let encoded_path = url_encode(path);
        let pqe = self.project_query_extra();
        let req_path = format!("/{API_VERSION}/instances/{name}/files?path={encoded_path}{pqe}");
        let req = Self::build_request(
            hyper::Method::POST,
            &req_path,
            Full::new(Bytes::copy_from_slice(content)),
            Some("application/octet-stream"),
        );
        // Add LXD-specific headers for file metadata.
        let req = {
            let (mut parts, body) = req.into_parts();
            parts
                .headers
                .insert("X-LXD-type", "file".parse().expect("valid header value"));
            parts
                .headers
                .insert("X-LXD-mode", mode.parse().expect("valid header value"));
            Request::from_parts(parts, body)
        };
        let (status, bytes) = self.send_request(req, API_TIMEOUT).await?;
        if status.is_success() {
            Ok(())
        } else {
            Err(error_from_response(status.as_u16(), &bytes))
        }
    }

    // ── System operations ────────────────────────────────────────────────

    /// Check connectivity by hitting the root API endpoint.
    pub async fn ping(&self) -> Result<(), LxdApiError> {
        let req = Self::build_request(
            hyper::Method::GET,
            &format!("/{API_VERSION}"),
            Full::new(Bytes::new()),
            None,
        );
        let (status, _) = self.send_request(req, API_TIMEOUT).await?;
        if status.is_success() {
            Ok(())
        } else {
            Err(LxdApiError::Api {
                status: status.as_u16(),
                message: "ping failed".to_string(),
            })
        }
    }

    /// Get server info.
    pub async fn server_info(&self) -> Result<Value, LxdApiError> {
        self.request_json(hyper::Method::GET, "", None).await
    }

    // ── Project operations ───────────────────────────────────────────────

    /// Ensure the configured LXD project exists, creating it if necessary.
    ///
    /// The project is created with separate `instances` and `profiles`
    /// features so sandbox instances are isolated from other workloads,
    /// while sharing images and storage pools with the default project.
    pub async fn ensure_project(&self) -> Result<(), LxdApiError> {
        if self.project.is_empty() || self.project == "default" {
            return Ok(());
        }

        // Check whether the project already exists.
        match self
            .request_json::<Value>(
                hyper::Method::GET,
                &format!("/projects/{}", url_encode(&self.project)),
                None,
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(LxdApiError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }

        // Create the project with instance isolation.
        let spec = serde_json::json!({
            "name": self.project,
            "description": "OpenShell sandbox instances",
            "config": {
                "features.images": "false",
                "features.profiles": "true",
                "features.storage.volumes": "false",
                "features.networks": "false",
            },
        });

        match self
            .request_and_wait(hyper::Method::POST, "/projects", Some(&spec), API_TIMEOUT)
            .await
        {
            Ok(()) => Ok(()),
            // Race: another process created the project concurrently.
            Err(LxdApiError::Conflict(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    // ── Event streaming ──────────────────────────────────────────────────

    /// Start streaming lifecycle events.
    ///
    /// Events are sent to the returned receiver. The background task runs
    /// until the receiver is dropped.
    pub async fn events_stream(
        &self,
    ) -> Result<mpsc::Receiver<Result<LxdEvent, LxdApiError>>, LxdApiError> {
        let pqe = self.project_query_extra();
        let path = format!("http://localhost/{API_VERSION}/events?type=lifecycle{pqe}");

        let mut sender = self.connect().await?;

        let req = Request::builder()
            .method(hyper::Method::GET)
            .uri(&path)
            .header("Host", "localhost")
            .body(Full::new(Bytes::new()))
            .map_err(|e| LxdApiError::Connection(e.to_string()))?;

        let response = tokio::time::timeout(API_TIMEOUT, sender.send_request(req))
            .await
            .map_err(|_| LxdApiError::Timeout(API_TIMEOUT))?
            .map_err(|e| LxdApiError::Connection(e.to_string()))?;

        if !response.status().is_success() {
            return Err(LxdApiError::Api {
                status: response.status().as_u16(),
                message: "events stream request failed".to_string(),
            });
        }

        let (tx, rx) = mpsc::channel(256);
        let body = response.into_body();

        tokio::spawn(async move {
            let mut buffer = Vec::new();
            let mut body = body;

            loop {
                use hyper::body::Body;

                let frame =
                    match std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
                        Some(Ok(frame)) => frame,
                        Some(Err(e)) => {
                            let _ = tx.send(Err(LxdApiError::Connection(e.to_string()))).await;
                            break;
                        }
                        None => break,
                    };

                if let Some(data) = frame.data_ref() {
                    buffer.extend_from_slice(data);
                }

                if buffer.len() > MAX_EVENT_BUFFER {
                    tracing::error!("event stream buffer exceeded maximum size, disconnecting");
                    let _ = tx
                        .send(Err(LxdApiError::Connection(
                            "event buffer exceeded 1 MB limit".to_string(),
                        )))
                        .await;
                    break;
                }

                // Parse complete newline-delimited JSON lines.
                while let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
                    let line: Vec<u8> = buffer.drain(..=pos).collect();
                    let trimmed = line.strip_suffix(&[b'\n']).unwrap_or(&line);
                    if trimmed.is_empty() {
                        continue;
                    }
                    let event = serde_json::from_slice::<LxdEvent>(trimmed).map_err(|e| {
                        LxdApiError::Json(format!("{e}: {}", String::from_utf8_lossy(trimmed)))
                    });
                    if tx.send(event).await.is_err() {
                        return;
                    }
                }
            }
        });

        Ok(rx)
    }
}

// ── Response wrappers ────────────────────────────────────────────────────

/// Generic LXD API response wrapper.
#[derive(Debug, serde::Deserialize)]
struct LxdResponse<T> {
    #[serde(rename = "type")]
    response_type: String,
    #[serde(default)]
    metadata: Option<T>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    error_code: Option<i32>,
    #[serde(default)]
    operation: Option<String>,
}

/// Async operation status returned by /1.0/operations/<uuid>/wait.
#[derive(Debug, Default, serde::Deserialize)]
#[allow(dead_code)]
struct OperationStatus {
    #[serde(default)]
    status: String,
    status_code: i32,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    err: Option<String>,
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn error_from_response(status: u16, bytes: &Bytes) -> LxdApiError {
    let message = serde_json::from_slice::<Value>(bytes)
        .ok()
        .and_then(|v| v.get("error").and_then(Value::as_str).map(String::from))
        .unwrap_or_else(|| String::from_utf8_lossy(bytes).to_string());

    match status {
        404 => LxdApiError::NotFound(message),
        409 => LxdApiError::Conflict(message),
        _ => LxdApiError::Api { status, message },
    }
}

fn error_from_lxd(code: u16, message: &str) -> LxdApiError {
    match code {
        404 => LxdApiError::NotFound(message.to_string()),
        409 => LxdApiError::Conflict(message.to_string()),
        _ => LxdApiError::Api {
            status: code,
            message: message.to_string(),
        },
    }
}

/// Minimal percent-encoding for query parameter values.
fn url_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                String::from(b as char)
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_encode_encodes_special_characters() {
        assert_eq!(url_encode("hello world"), "hello%20world");
        assert_eq!(url_encode("a=b&c=d"), "a%3Db%26c%3Dd");
        assert_eq!(url_encode("safe-_.~chars"), "safe-_.~chars");
    }

    #[test]
    fn validate_name_accepts_valid_names() {
        assert!(validate_name("my-instance").is_ok());
        assert!(validate_name("a").is_ok());
        assert!(validate_name("instance123").is_ok());
        assert!(validate_name("test-sandbox-abc").is_ok());
    }

    #[test]
    fn validate_name_rejects_invalid_names() {
        assert!(validate_name("").is_err());
        assert!(validate_name("-leading").is_err());
        assert!(validate_name("has/slash").is_err());
        assert!(validate_name("has_underscore").is_err()); // LXD doesn't allow underscores
        assert!(validate_name("has space").is_err());
        assert!(validate_name("has.dot").is_err()); // LXD doesn't allow dots
    }

    #[test]
    fn validate_name_rejects_names_exceeding_max_length() {
        let long_name = format!("a{}", "b".repeat(MAX_NAME_LEN));
        assert!(long_name.len() > MAX_NAME_LEN);
        assert!(validate_name(&long_name).is_err());

        let exact_name = "a".repeat(MAX_NAME_LEN);
        assert!(validate_name(&exact_name).is_ok());
    }
}
