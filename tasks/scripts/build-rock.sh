#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

echo "==> Preparing Rockcraft configuration for core..."
if [ -f "rockcraft.yaml" ]; then
    echo "ERROR: rockcraft.yaml already exists in the project root. Please remove or rename it."
    exit 1
fi

restore_rockcraft() {
    rm -f rockcraft.yaml
}
trap restore_rockcraft EXIT

cp rockcraft-openshell-core.yaml rockcraft.yaml

echo "==> Building OpenShell Core ROCK (Gateway + Supervisor)..."
rm -f openshell-core_*_*.rock
if [[ -n "${1:-}" ]]; then
  if [[ "$1" == "clean" ]]; then
      echo "Cleaning build environment..."
      rockcraft clean
  fi
fi
rockcraft pack
rm -f rockcraft.yaml
trap - EXIT

echo "==> Exporting ROCK to snap/local-images..."
mkdir -p snap/local-images
cp openshell-core*.rock snap/local-images/openshell-core.rock

echo "==> Done building OpenShell Core ROCK."
