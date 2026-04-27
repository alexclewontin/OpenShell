# openshell-driver-lxd

In-process [`ComputeDriver`](../../proto/compute_driver.proto) backend that manages sandbox instances via the [LXD](https://canonical.com/lxd) REST API over a Unix socket.

## How it works

The driver communicates with the LXD daemon over its Unix socket (`/var/snap/lxd/common/lxd/unix.socket` by default). Each sandbox becomes an LXD container running the `openshell-sandbox` supervisor. All instances are scoped to a dedicated LXD project (default: `openshell`) for isolation from other workloads.

```
openshell-server
  └─ LxdComputeDriver (in-process)
       └─ LXD REST API (Unix socket)
            └─ LXD project "openshell"
                 ├─ openshell-sandbox-<id-1>  (container)
                 ├─ openshell-sandbox-<id-2>  (container)
                 └─ ...
```

## Configuration

The driver is selected by setting `--drivers lxd` (or `OPENSHELL_DRIVERS=lxd`).

| Flag / Env var            | Default       | Purpose                                                              |
| ------------------------- | ------------- | -------------------------------------------------------------------- |
| `OPENSHELL_LXD_SOCKET`    | auto-detected | Path to the LXD Unix socket. Checks snap then deb paths.             |
| `OPENSHELL_LXD_PROJECT`   | `openshell`   | LXD project for sandbox instances. Created automatically on startup. |
| `OPENSHELL_NETWORK_NAME`  | `openshell`   | LXD network (bridge) to attach instances to.                         |
| `OPENSHELL_SANDBOX_IMAGE` | —             | Default LXD image alias or fingerprint for sandbox containers.       |
| `OPENSHELL_STOP_TIMEOUT`  | `10`          | Instance stop timeout in seconds.                                    |

## Building the sandbox image

LXD uses its own image format (not OCI). The sandbox image must contain the `openshell-sandbox` supervisor binary at `/opt/openshell/bin/openshell-sandbox`. A [distrobuilder](https://github.com/lxc/distrobuilder) definition is provided at `deploy/lxd/openshell-sandbox.yaml`.

**Quick start (requires `distrobuilder` and LXD):**

```shell
# Build and import in one step:
mise run lxd:image

# Or build a portable tarball without importing:
mise run lxd:image:export
```

**Manual build:**

```shell
# 1. Build the supervisor binary
cargo build --release -p openshell-sandbox

# 2. Build the LXD image
sudo distrobuilder build-incus deploy/lxd/openshell-sandbox.yaml deploy/lxd/output/ \
    -o image.architecture=$(uname -m)

# 3. Import into LXD
lxc image import deploy/lxd/output/incus.tar.xz deploy/lxd/output/rootfs.squashfs \
    --alias openshell-sandbox
```

**Transfer to a remote host:**

```shell
# On the build machine:
mise run lxd:image:export
scp deploy/lxd/output/incus.tar.xz deploy/lxd/output/rootfs.squashfs remote-host:

# On the remote host:
lxc image import incus.tar.xz rootfs.squashfs --alias openshell-sandbox
```

Install distrobuilder: `sudo snap install distrobuilder --classic`

## Socket auto-detection

The driver probes these paths in order and uses the first one that exists:

1. `/var/snap/lxd/common/lxd/unix.socket` (LXD snap)
2. `/var/lib/lxd/unix.socket` (LXD deb)

Override with `OPENSHELL_LXD_SOCKET` when using a non-standard path.
