// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use futures::{Stream, StreamExt};
use openshell_core::proto::compute::v1::{
    CreateSandboxRequest, CreateSandboxResponse, DeleteSandboxRequest, DeleteSandboxResponse,
    GetCapabilitiesRequest, GetCapabilitiesResponse, GetSandboxRequest, GetSandboxResponse,
    ListSandboxesRequest, ListSandboxesResponse, StopSandboxRequest, StopSandboxResponse,
    ValidateSandboxCreateRequest, ValidateSandboxCreateResponse, WatchSandboxesEvent,
    WatchSandboxesRequest, compute_driver_server::ComputeDriver,
};
use std::pin::Pin;
use tonic::{Request, Response, Status};

use crate::LxdComputeDriver;
use openshell_core::ComputeDriverError;

#[derive(Debug, Clone)]
pub struct ComputeDriverService {
    driver: LxdComputeDriver,
}

impl ComputeDriverService {
    #[must_use]
    pub fn new(driver: LxdComputeDriver) -> Self {
        Self { driver }
    }
}

#[tonic::async_trait]
impl ComputeDriver for ComputeDriverService {
    async fn get_capabilities(
        &self,
        _request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<GetCapabilitiesResponse>, Status> {
        self.driver
            .capabilities()
            .await
            .map(Response::new)
            .map_err(status_from_driver_error)
    }

    async fn validate_sandbox_create(
        &self,
        request: Request<ValidateSandboxCreateRequest>,
    ) -> Result<Response<ValidateSandboxCreateResponse>, Status> {
        let sandbox = request
            .into_inner()
            .sandbox
            .ok_or_else(|| Status::invalid_argument("sandbox is required"))?;
        self.driver
            .validate_sandbox_create(&sandbox)
            .await
            .map_err(status_from_driver_error)?;
        Ok(Response::new(ValidateSandboxCreateResponse {}))
    }

    async fn get_sandbox(
        &self,
        request: Request<GetSandboxRequest>,
    ) -> Result<Response<GetSandboxResponse>, Status> {
        let request = request.into_inner();
        if request.sandbox_name.is_empty() {
            return Err(Status::invalid_argument("sandbox_name is required"));
        }

        let sandbox = self
            .driver
            .get_sandbox(&request.sandbox_name)
            .await
            .map_err(status_from_driver_error)?
            .ok_or_else(|| Status::not_found("sandbox not found"))?;

        if !request.sandbox_id.is_empty() && request.sandbox_id != sandbox.id {
            return Err(Status::failed_precondition(
                "sandbox_id did not match the fetched sandbox",
            ));
        }

        Ok(Response::new(GetSandboxResponse {
            sandbox: Some(sandbox),
        }))
    }

    async fn list_sandboxes(
        &self,
        _request: Request<ListSandboxesRequest>,
    ) -> Result<Response<ListSandboxesResponse>, Status> {
        let sandboxes = self
            .driver
            .list_sandboxes()
            .await
            .map_err(status_from_driver_error)?;
        Ok(Response::new(ListSandboxesResponse { sandboxes }))
    }

    async fn create_sandbox(
        &self,
        request: Request<CreateSandboxRequest>,
    ) -> Result<Response<CreateSandboxResponse>, Status> {
        let sandbox = request
            .into_inner()
            .sandbox
            .ok_or_else(|| Status::invalid_argument("sandbox is required"))?;
        self.driver
            .create_sandbox(&sandbox)
            .await
            .map_err(status_from_driver_error)?;
        Ok(Response::new(CreateSandboxResponse {}))
    }

    async fn stop_sandbox(
        &self,
        request: Request<StopSandboxRequest>,
    ) -> Result<Response<StopSandboxResponse>, Status> {
        let request = request.into_inner();
        if request.sandbox_name.is_empty() {
            return Err(Status::invalid_argument("sandbox_name is required"));
        }
        self.driver
            .stop_sandbox(&request.sandbox_name)
            .await
            .map_err(status_from_driver_error)?;
        Ok(Response::new(StopSandboxResponse {}))
    }

    async fn delete_sandbox(
        &self,
        request: Request<DeleteSandboxRequest>,
    ) -> Result<Response<DeleteSandboxResponse>, Status> {
        let request = request.into_inner();
        if request.sandbox_id.is_empty() {
            return Err(Status::invalid_argument("sandbox_id is required"));
        }
        if request.sandbox_name.is_empty() {
            return Err(Status::invalid_argument("sandbox_name is required"));
        }
        let deleted = self
            .driver
            .delete_sandbox(&request.sandbox_id, &request.sandbox_name)
            .await
            .map_err(status_from_driver_error)?;
        Ok(Response::new(DeleteSandboxResponse { deleted }))
    }

    type WatchSandboxesStream =
        Pin<Box<dyn Stream<Item = Result<WatchSandboxesEvent, Status>> + Send + 'static>>;

    async fn watch_sandboxes(
        &self,
        _request: Request<WatchSandboxesRequest>,
    ) -> Result<Response<Self::WatchSandboxesStream>, Status> {
        let stream = self
            .driver
            .watch_sandboxes()
            .await
            .map_err(status_from_driver_error)?;
        let stream = stream.map(|item| item.map_err(|err| Status::internal(err.to_string())));
        Ok(Response::new(Box::pin(stream)))
    }
}

fn status_from_driver_error(err: ComputeDriverError) -> Status {
    match err {
        ComputeDriverError::AlreadyExists => Status::already_exists("sandbox already exists"),
        ComputeDriverError::Precondition(message) => Status::failed_precondition(message),
        ComputeDriverError::Message(message) => Status::internal(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn precondition_driver_errors_map_to_failed_precondition_status() {
        let status = status_from_driver_error(ComputeDriverError::Precondition(
            "sandbox instance is not running".to_string(),
        ));
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), "sandbox instance is not running");
    }

    #[test]
    fn already_exists_driver_errors_map_to_already_exists_status() {
        let status = status_from_driver_error(ComputeDriverError::AlreadyExists);
        assert_eq!(status.code(), tonic::Code::AlreadyExists);
    }

    #[test]
    fn message_driver_errors_map_to_internal_status() {
        let status =
            status_from_driver_error(ComputeDriverError::Message("something broke".to_string()));
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "something broke");
    }
}
