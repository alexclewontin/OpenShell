#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Build the VM rootfs using Rockcraft and umoci.
# Replaces the legacy docker-based build-rootfs.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

TARGET_DIR="${PROJECT_ROOT}/target/vm-runtime-compressed"
ROOTFS_TARBALL="${TARGET_DIR}/rootfs.tar.zst"

echo "==> Building OpenShell VM Rootfs with Rockcraft..."
cd "${PROJECT_ROOT}"

if ! command -v rockcraft >/dev/null 2>&1; then
    echo "ERROR: rockcraft is required. Install it with: sudo snap install rockcraft --classic"
    exit 1
fi

if ! command -v umoci >/dev/null 2>&1; then
    echo "ERROR: umoci is required to extract the OCI image. Install it with: sudo apt install umoci"
    exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "ERROR: jq is required to parse the OCI manifest. Install it with: sudo apt install jq"
    exit 1
fi

if ! command -v zstd >/dev/null 2>&1; then
    echo "ERROR: zstd is required. Install it with: sudo apt install zstd"
    exit 1
fi

if ! command -v fakeroot >/dev/null 2>&1; then
    echo "ERROR: fakeroot not installed. Install with: sudo apt install fakeroot"
    exit 1
fi

# Clean up any previous rock build artifacts
rm -f openshell-vm-rootfs_*.rock

echo "==> Preparing Rockcraft configuration..."
# Rockcraft runs in an LXD container and ONLY mounts the directory containing rockcraft.yaml.
# We temporarily swap our specific rockcraft config into the root, and use a trap to
# ensure it is cleanly removed afterward so we don't pollute the root directory.
if [ -f "rockcraft.yaml" ]; then
    echo "ERROR: rockcraft.yaml already exists in the project root. Please remove or rename it."
    exit 1
fi

restore_rockcraft() {
    rm -f rockcraft.yaml
}
trap restore_rockcraft EXIT

cp rockcraft-vm-rootfs.yaml rockcraft.yaml

if [[ -n "${1:-}" ]]; then
  if [[ "$1" == "clean" ]]; then
      echo "Cleaning build environment..."
      rockcraft clean
  fi
fi
# Build the rock
rockcraft pack

ROCK_FILE=$(ls openshell-vm-rootfs_*.rock | head -n 1)
if [ -z "$ROCK_FILE" ]; then
    echo "ERROR: Failed to find built rock file"
    exit 1
fi

echo "==> Extracting OCI image into rootfs..."
OCI_DIR=$(mktemp -d)
BUNDLE_DIR=$(mktemp -d)

# A .rock file is actually an OCI Archive (a tarball containing an OCI layout).
# umoci expects an unpacked OCI layout directory, so we tar -xf it first.
tar -xf "${ROCK_FILE}" -C "${OCI_DIR}"

# Rockcraft sets the OCI reference name (tag) to the version, not 'latest'.
# We extract the tag dynamically from index.json using jq.
TAG=$(jq -r '.manifests[0].annotations["org.opencontainers.image.ref.name"]' "${OCI_DIR}/index.json")

# umoci unpack extracts an OCI image into an OCI runtime bundle (which contains a 'rootfs' dir).
# --rootless allows extracting without sudo, mapping UID/GID to the current user.
umoci unpack --rootless --image "${OCI_DIR}:${TAG}" "${BUNDLE_DIR}"

echo "==> Compressing rootfs into ${ROOTFS_TARBALL}..."
mkdir -p "${TARGET_DIR}"

# Create the final tarball directly from the extracted rootfs directory.
# We use fakeroot so that the resulting tarball stores files as root:root (UID 0),
# which is what the VM guest kernel expects when it mounts the virtio-fs.
fakeroot tar -C "${BUNDLE_DIR}/rootfs" -cf - . | zstd -f -19 -T0 -o "${ROOTFS_TARBALL}"

echo "==> Cleaning up..."
rm -rf "${BUNDLE_DIR}"
rm -rf "${OCI_DIR}"
rm -f "${ROCK_FILE}"

echo "==> VM Rootfs ready at: ${ROOTFS_TARBALL}"
echo "    Size: $(du -h "${ROOTFS_TARBALL}" | cut -f1)"
exit 0
