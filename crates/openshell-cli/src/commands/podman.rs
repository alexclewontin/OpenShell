// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use miette::{IntoDiagnostic, Result, WrapErr, miette};
use owo_colors::OwoColorize;
use std::env;
use std::path::PathBuf;

use openshell_driver_podman::client::PodmanClient;

/// Initialize Podman to serve as a compute driver for `OpenShell`.
pub async fn init() -> Result<()> {
    println!("{}", "Initializing Podman for OpenShell...".bold());

    let socket_path = env::var("OPENSHELL_PODMAN_SOCKET").unwrap_or_else(|_| {
        env::var("XDG_RUNTIME_DIR").map_or_else(
            |_| "/run/podman/podman.sock".to_string(),
            |xdg| format!("{xdg}/podman/podman.sock"),
        )
    });

    if !socket_path.starts_with("tcp://") && !std::path::Path::new(&socket_path).exists() {
        println!("\n{}", "Podman socket not found!".red().bold());
        println!("Expected socket at: {socket_path}");
        println!("\nEnsure Podman is running with socket activation enabled:");
        println!("  systemctl --user start podman.socket");
        return Err(miette!("Podman socket not found at {}", socket_path));
    }

    println!("✓ Found Podman socket at: {socket_path}");

    let client = PodmanClient::new(PathBuf::from(&socket_path));
    client
        .ping()
        .await
        .into_diagnostic()
        .wrap_err("Failed to connect to Podman daemon")?;
    println!("✓ Successfully connected to Podman daemon");

    println!("\n{}", "Podman initialization complete!".green().bold());
    println!("To configure OpenShell to use the Podman compute driver, start the gateway with:");
    println!(
        "  OPENSHELL_PODMAN_SOCKET=\"{socket_path}\" openshell gateway start --compute-driver podman"
    );

    Ok(())
}
