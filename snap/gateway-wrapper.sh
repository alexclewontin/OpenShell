#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Wrapper for openshell-gateway that reads snap config to select the compute driver.
#
# Usage:
#   snap set openshell driver=docker   # (default) use Docker driver
#   snap set openshell driver=local    # use native local driver (no Docker needed)
#   snap set openshell driver=auto     # auto-detect (prefer docker, fall back to local)

set -euo pipefail

SNAP="${SNAP:-/snap/openshell/current}"
SNAP_DATA="${SNAP_DATA:-/var/snap/openshell/current}"
SNAP_COMMON="${SNAP_COMMON:-/var/snap/openshell/common}"

# Read driver from snap config; default to "docker" for backwards compatibility
DRIVER="$(snapctl get driver 2>/dev/null || echo "docker")"
if [ -z "$DRIVER" ]; then
    DRIVER="docker"
fi

# Base environment (shared across all drivers)
export OPENSHELL_BIND_ADDRESS="${OPENSHELL_BIND_ADDRESS:-127.0.0.1}"
export OPENSHELL_SERVER_PORT="${OPENSHELL_SERVER_PORT:-17670}"
export OPENSHELL_DB_URL="${OPENSHELL_DB_URL:-sqlite:${SNAP_COMMON}/gateway.db?mode=rwc}"
export OPENSHELL_GRPC_ENDPOINT="${OPENSHELL_GRPC_ENDPOINT:-http://host.openshell.internal:17670}"
export OPENSHELL_DISABLE_TLS="${OPENSHELL_DISABLE_TLS:-true}"
export OPENSHELL_SANDBOX_SSH_PORT="${OPENSHELL_SANDBOX_SSH_PORT:-2222}"
export OPENSHELL_SSH_GATEWAY_HOST="${OPENSHELL_SSH_GATEWAY_HOST:-127.0.0.1}"
export OPENSHELL_SSH_GATEWAY_PORT="${OPENSHELL_SSH_GATEWAY_PORT:-8080}"
export XDG_DATA_HOME="${XDG_DATA_HOME:-${SNAP_COMMON}}"
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-${SNAP_COMMON}}"

case "$DRIVER" in
    local|native)
        export OPENSHELL_DRIVERS="local"
        export OPENSHELL_LOCAL_SUPERVISOR_BIN="${SNAP}/bin/openshell-sandbox"
        # Local driver does not use container images
        unset OPENSHELL_SANDBOX_IMAGE
        unset OPENSHELL_SANDBOX_IMAGE_PULL_POLICY
        unset OPENSHELL_DOCKER_SUPERVISOR_BIN
        unset OPENSHELL_DOCKER_NETWORK_NAME
        ;;
    auto)
        # Auto-detect: use docker if available, otherwise fall back to local
        if command -v docker &>/dev/null && docker info &>/dev/null 2>&1; then
            export OPENSHELL_DRIVERS="docker"
            export OPENSHELL_DOCKER_SUPERVISOR_BIN="${SNAP}/bin/openshell-sandbox"
            export OPENSHELL_DOCKER_NETWORK_NAME="openshell-snap"
            export OPENSHELL_SANDBOX_IMAGE="${OPENSHELL_SANDBOX_IMAGE:-ghcr.io/nvidia/openshell-community/sandboxes/base:latest}"
            export OPENSHELL_SANDBOX_IMAGE_PULL_POLICY="${OPENSHELL_SANDBOX_IMAGE_PULL_POLICY:-IfNotPresent}"
        else
            export OPENSHELL_DRIVERS="local"
            export OPENSHELL_LOCAL_SUPERVISOR_BIN="${SNAP}/bin/openshell-sandbox"
            unset OPENSHELL_SANDBOX_IMAGE
            unset OPENSHELL_SANDBOX_IMAGE_PULL_POLICY
            unset OPENSHELL_DOCKER_SUPERVISOR_BIN
            unset OPENSHELL_DOCKER_NETWORK_NAME
        fi
        ;;
    docker|*)
        export OPENSHELL_DRIVERS="docker"
        export OPENSHELL_DOCKER_SUPERVISOR_BIN="${SNAP}/bin/openshell-sandbox"
        export OPENSHELL_DOCKER_NETWORK_NAME="openshell-snap"
        export OPENSHELL_SANDBOX_IMAGE="${OPENSHELL_SANDBOX_IMAGE:-ghcr.io/nvidia/openshell-community/sandboxes/base:latest}"
        export OPENSHELL_SANDBOX_IMAGE_PULL_POLICY="${OPENSHELL_SANDBOX_IMAGE_PULL_POLICY:-IfNotPresent}"
        unset OPENSHELL_LOCAL_SUPERVISOR_BIN
        ;;
esac

exec "${SNAP}/bin/openshell-gateway" "$@"
