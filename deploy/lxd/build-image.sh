#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Build the OpenShell sandbox LXD image using distrobuilder.
#
# Usage:
#   ./deploy/lxd/build-image.sh [--import [ALIAS]]
#
# Options:
#   --import [ALIAS]   Import the built image into the local LXD daemon.
#                      Default alias: openshell-sandbox
#
# Prerequisites:
#   - distrobuilder (sudo snap install distrobuilder --classic)
#   - Pre-built openshell-sandbox binary at target/release/openshell-sandbox
#     (or set OPENSHELL_SANDBOX_BIN to override)
#
# Output:
#   deploy/lxd/output/incus.tar.xz      - metadata tarball
#   deploy/lxd/output/rootfs.squashfs    - rootfs squashfs image

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUTPUT_DIR="${OPENSHELL_LXD_IMAGE_OUTPUT:-$SCRIPT_DIR/output}"
DEFINITION="$SCRIPT_DIR/openshell-sandbox.yaml"
SANDBOX_BIN="${OPENSHELL_SANDBOX_BIN:-$ROOT_DIR/target/release/openshell-sandbox}"
ARCH="${OPENSHELL_LXD_IMAGE_ARCH:-$(uname -m)}"
IMPORT_ALIAS=""

# Parse arguments.
while [[ $# -gt 0 ]]; do
    case "$1" in
        --import)
            IMPORT_ALIAS="${2:-openshell-sandbox}"
            if [[ "${2:-}" != "" && "${2:-}" != --* ]]; then
                shift
            fi
            ;;
        *)
            echo "Unknown argument: $1" >&2
            exit 1
            ;;
    esac
    shift
done

# Validate prerequisites.
if ! command -v distrobuilder &>/dev/null; then
    echo "error: distrobuilder not found. Install with: sudo snap install distrobuilder --classic" >&2
    exit 1
fi

if [[ ! -f "$SANDBOX_BIN" ]]; then
    echo "error: supervisor binary not found at $SANDBOX_BIN" >&2
    echo "  Build it first:  cargo build --release -p openshell-sandbox" >&2
    echo "  Or set OPENSHELL_SANDBOX_BIN to the binary path." >&2
    exit 1
fi

if [[ ! -x "$SANDBOX_BIN" ]]; then
    echo "error: $SANDBOX_BIN is not executable" >&2
    exit 1
fi

# Prepare output directory.
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Create an overlay directory with the supervisor binary so distrobuilder
# injects it into the rootfs.
OVERLAY_DIR="$(mktemp -d)"
trap 'rm -rf "$OVERLAY_DIR"' EXIT
mkdir -p "$OVERLAY_DIR/opt/openshell/bin"
cp "$SANDBOX_BIN" "$OVERLAY_DIR/opt/openshell/bin/openshell-sandbox"
chmod 755 "$OVERLAY_DIR/opt/openshell/bin/openshell-sandbox"

echo "Building LXD image..."
echo "  Definition:  $DEFINITION"
echo "  Binary:      $SANDBOX_BIN"
echo "  Arch:        $ARCH"
echo "  Output:      $OUTPUT_DIR"

# distrobuilder requires root for debootstrap.
sudo distrobuilder build-incus "$DEFINITION" "$OUTPUT_DIR" \
    -o "image.architecture=$ARCH" \
    --import-into-incus=""

# The overlay with the supervisor binary needs to be injected manually.
# distrobuilder's --overlay flag does this for us.
# Rebuild with overlay:
sudo distrobuilder build-incus "$DEFINITION" "$OUTPUT_DIR" \
    -o "image.architecture=$ARCH" \
    --overlay "$OVERLAY_DIR"

# Fix output ownership (distrobuilder runs as root).
sudo chown -R "$(id -u):$(id -g)" "$OUTPUT_DIR"

echo ""
echo "LXD image built successfully:"
ls -lh "$OUTPUT_DIR"

# Import if requested.
if [[ -n "$IMPORT_ALIAS" ]]; then
    if ! command -v lxc &>/dev/null; then
        echo "error: lxc CLI not found, cannot import" >&2
        exit 1
    fi

    echo ""
    echo "Importing into local LXD as '$IMPORT_ALIAS'..."

    # Delete existing image with same alias if present.
    if lxc image info "$IMPORT_ALIAS" &>/dev/null; then
        lxc image delete "$IMPORT_ALIAS"
    fi

    lxc image import "$OUTPUT_DIR/incus.tar.xz" "$OUTPUT_DIR/rootfs.squashfs" \
        --alias "$IMPORT_ALIAS"

    echo "Imported: $IMPORT_ALIAS"
    lxc image info "$IMPORT_ALIAS" | head -10
fi
