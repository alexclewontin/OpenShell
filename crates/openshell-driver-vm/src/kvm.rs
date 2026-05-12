// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/// Check that `/dev/kvm` is accessible and return an architecture-specific
/// diagnostic if not.
#[cfg(target_os = "linux")]
pub(crate) fn check_kvm_access() -> Result<(), String> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/kvm")
        .map(|_| ())
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::PermissionDenied
                || e.kind() == std::io::ErrorKind::NotFound
            {
                diagnose_missing_kvm()
            } else {
                format!("failed to access /dev/kvm: {e}")
            }
        })
}

fn diagnose_missing_kvm() -> String {
    let arch = std::env::consts::ARCH;
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    diagnose_missing_kvm_with_cpuinfo(arch, &cpuinfo)
}

fn diagnose_missing_kvm_with_cpuinfo(arch: &str, cpuinfo: &str) -> String {
    match arch {
        "x86_64" | "x86" => {
            if !cpuinfo.contains("vmx") && !cpuinfo.contains("svm") {
                return "Your machine does not have the 'vmx' (Intel) or 'svm' (AMD) CPU flag. If you are running in a VM, you have not enabled nested virtualization.".to_string();
            }
        }
        "aarch64" => {
            return "Your machine does not support hardware virtualization. Ensure your CPU booted in EL2 (Hypervisor mode) and that virtualization is enabled in your firmware.".to_string();
        }
        "riscv64" => {
            let has_h_ext = cpuinfo
                .lines()
                .filter(|line| line.starts_with("isa"))
                .any(|line| line.contains("_h") || line.contains(" h "));

            if !has_h_ext {
                return "Your machine does not support hardware virtualization. Missing the 'h' (Hypervisor) extension in the RISC-V ISA string.".to_string();
            }
        }
        "s390x" => {
            if !cpuinfo.contains("sie") {
                return "Your machine does not support hardware virtualization. Missing the 'sie' (System z Virtualization) feature.".to_string();
            }
        }
        "powerpc" | "ppc64" | "ppc64le" => {
            let is_powernv = cpuinfo
                .lines()
                .any(|line| line.starts_with("platform") && line.contains("PowerNV"));
            if !is_powernv {
                return "Your machine does not support hardware virtualization. Your platform may not provide KVM hypervisor capabilities (requires PowerNV or a compatible bare-metal environment).".to_string();
            }
        }
        _ => {
            return format!(
                "Hardware virtualization requirements are not explicitly checked for architecture '{arch}'.",
            );
        }
    }

    "Hardware virtualization is supported, but /dev/kvm is missing. Ensure the KVM kernel module is loaded ('modprobe kvm') or, if running as a snap, connect the kvm interface:\n  sudo snap connect openshell:kvm".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diagnose_missing_kvm_x86_64_missing() {
        let cpuinfo = "flags: fpu vme de pse tsc";
        let err = diagnose_missing_kvm_with_cpuinfo("x86_64", cpuinfo);
        assert!(err.contains("vmx"));
    }

    #[test]
    fn test_diagnose_missing_kvm_x86_64_present() {
        let cpuinfo = "flags: fpu vme de pse tsc vmx svm";
        let err = diagnose_missing_kvm_with_cpuinfo("x86_64", cpuinfo);
        assert!(err.contains("Hardware virtualization is supported"));
    }

    #[test]
    fn test_diagnose_missing_kvm_powerpc_missing() {
        let cpuinfo = "platform: PowerVM";
        let err = diagnose_missing_kvm_with_cpuinfo("powerpc", cpuinfo);
        assert!(err.contains("requires PowerNV"));
    }

    #[test]
    fn test_diagnose_missing_kvm_powerpc_present() {
        let cpuinfo = "platform: PowerNV";
        let err = diagnose_missing_kvm_with_cpuinfo("powerpc", cpuinfo);
        assert!(err.contains("Hardware virtualization is supported"));
    }

    #[test]
    fn test_diagnose_missing_kvm_riscv64_present() {
        let cpuinfo = "isa: rv64imafdc_h_zicbom";
        let err = diagnose_missing_kvm_with_cpuinfo("riscv64", cpuinfo);
        assert!(err.contains("Hardware virtualization is supported"));
    }

    #[test]
    fn test_diagnose_missing_kvm_riscv64_missing() {
        let cpuinfo = "isa: rv64imafdc_zicbom";
        let err = diagnose_missing_kvm_with_cpuinfo("riscv64", cpuinfo);
        assert!(err.contains("Missing the 'h' (Hypervisor) extension"));
    }

    #[test]
    fn test_diagnose_missing_kvm_s390x_present() {
        let cpuinfo = "features : sie";
        let err = diagnose_missing_kvm_with_cpuinfo("s390x", cpuinfo);
        assert!(err.contains("Hardware virtualization is supported"));
    }

    #[test]
    fn test_diagnose_missing_kvm_s390x_missing() {
        let cpuinfo = "features : lpar";
        let err = diagnose_missing_kvm_with_cpuinfo("s390x", cpuinfo);
        assert!(err.contains("Missing the 'sie'"));
    }
}
