# Ziggurat

Distributed research compute mesh. Single Go binary — drop it on any machine, it finds the cluster (or starts one), advertises capabilities, and begins accepting work.

Tasks are arbitrary commands: any script, binary, or pipeline that runs on the worker's OS. Ziggurat manages workspaces, fetches inputs from content-addressed storage, executes the command, captures output, and uploads results. No SDK required.

Runs on **Linux** and **Windows**.

## Quick Start

```bash
# Build
go build -o ziggurat ./cmd/ziggurat

# Start a node — works immediately with no config
./ziggurat start

# In another terminal: submit a task
./ziggurat run -- echo "hello world"

# Check status
./ziggurat status

# Wait for a task to complete
./ziggurat wait <task-id>
```

To generate a config file with LAN defaults (optional):

```bash
./ziggurat init          # writes ~/.ziggurat/ziggurat.yaml
```

## Multi-Node Cluster

```bash
# On the first machine
./ziggurat start

# On additional machines — join via seed address
./ziggurat start --join 10.0.0.5:7102
```

Nodes discover each other via gossip (memberlist). By default every node runs as `hybrid` (coordinator + worker). Use `--role` to run dedicated roles:

```bash
# Dedicated coordinator (schedules tasks, doesn't execute them)
./ziggurat start --role coordinator

# Dedicated worker (executes tasks, doesn't schedule)
./ziggurat start --role worker --join 10.0.0.5:7102
```

## Configuration

Ziggurat works out of the box with no config file. To customize, run `ziggurat init` to generate a commented template at `~/.ziggurat/ziggurat.yaml`, then edit as needed.

Config file search order (first found wins):
1. `--config <path>` flag
2. `./ziggurat.yaml` (current directory)
3. `~/.ziggurat/ziggurat.yaml` (data directory)
4. Built-in defaults (if no file exists)

```yaml
node:
  name: gpu-workstation
  role: hybrid            # hybrid (default), coordinator, worker
  tags: [gpu, cuda, python3]
  data_dir: ~/.ziggurat
  capabilities:
    custom.key: value

network:
  bind: 0.0.0.0
  http_port: 7100     # REST API
  grpc_port: 7101     # inter-node transport
  gossip_port: 7102   # memberlist gossip

cluster:
  name: default
  seeds: [10.0.0.5:7102]

storage:
  replication_factor: 2
  capacity: 53687091200  # 50 GB
  gc_grace_period: 1h
  erasure:
    enabled: true
    data_shards: 4
    parity_shards: 2

compute:
  concurrency: 4           # max parallel tasks (0 = NumCPU)
  task_timeout: 5m
  max_output_size: 1073741824  # 1 GB
  cancel_grace: 10s
  max_retained_workspaces: 20
  env_max_age: 168h        # 7 days
  env_max_count: 50

resilience:
  task_retries: 2
  dead_letter: true
  max_queue_depth: 0       # 0 = unlimited

security:
  tls:
    enabled: false         # mTLS for inter-node gRPC
  join_token: ""           # shared secret for cluster join
  api_token: ""            # bearer token for HTTP API

log:
  format: text             # text | json
  level: info              # debug | info | warn | error
```

Connection resolution for client commands: `--addr` flag > `ZIGGURAT_ADDR` env > config `client.addr` > `127.0.0.1:7100`.

## Capabilities & Scheduling

Each node advertises **capabilities** — auto-detected facts plus operator-declared values — and tasks are routed only to nodes that satisfy their requirements. Matching happens both at the coordinator (candidate selection) and at the receiving worker, so a task never runs on an ineligible node.

**Auto-detected** (no config needed):

| Capability | Example | Source |
|------------|---------|--------|
| `os`, `arch`, `cpu.cores` | `linux`, `amd64`, `24` | runtime |
| `mem.total`, `disk.avail`, `storage.class` | bytes; `ssd` | OS probes |
| `gpu.count`, `gpu.model`, `gpu.vram`, `gpu.cuda`, `gpu.driver` | `1`, `NVIDIA GeForce RTX 4090 D`, … | `nvidia-smi` / `nvcc` |
| `compute.concurrency` | `16` | task slots |
| `python.version`, `node.version`, `go.version`, `java.version`, `ruby.version`, `rust.version` | `3.12.1`, `20.11.0`, … | runtime `--version` probes |

Operator-declared `node.tags` and `node.capabilities` are merged in and **override** auto-detected values for the same key.

**Three ways to require capabilities when submitting a task:**

```bash
# Tags (presence flags) — node must carry every required tag
ziggurat run --require gpu --require python3 -- python train.py

# Capability constraints (expressions: == != >= <= > <; version- and byte-size-aware)
ziggurat run --constraint "python.version >= 3.10" -- python script.py
ziggurat run --constraint "gpu.vram >= 16GB" -- ./infer

# Resources (admission + live capacity) — routes to nodes with enough free CPU/mem/GPU
ziggurat run --gpus 1 --memory 8GB -- ./gpu-job
```

A GPU job (`--gpus N`) is placed only on nodes with enough free GPUs, which then reserve devices and expose them via `CUDA_VISIBLE_DEVICES`. A task whose requirements no node satisfies stays queued until an eligible node appears.

> Note: capability requirements are **explicit** — Ziggurat does not infer them from the command (a `.py` argument does not auto-require Python). Declare what a task needs with `--require` / `--constraint` / `--gpus`.

## Commands

### Cluster Lifecycle
| Command | Description |
|---------|-------------|
| `ziggurat init` | Generate default config at `~/.ziggurat/ziggurat.yaml` |
| `ziggurat start` | Start a node (hybrid role) |
| `ziggurat start --role worker --join <addr>` | Start a dedicated worker and join cluster |
| `ziggurat drain` | Stop accepting work, finish in-flight tasks |
| `ziggurat resume` | Resume task dequeuing after drain |
| `ziggurat status` | Cluster dashboard |
| `ziggurat nodes` | List cluster members |
| `ziggurat version` | Binary version |
| `ziggurat token generate` | Generate a cluster join token |

### Tasks
| Command | Description |
|---------|-------------|
| `ziggurat run -- <cmd>` | Submit a task |
| `ziggurat run --image <ref> -- <cmd>` | Submit a task in an OCI container |
| `ziggurat run --wait -- <cmd>` | Submit and wait for result |
| `ziggurat tasks` | List tasks (filterable: `--status running`) |
| `ziggurat task <id>` | Task detail + stdout/stderr |
| `ziggurat logs <id>` | Stream live stdout/stderr (SSE) |
| `ziggurat cancel <id>` | Cancel a task |
| `ziggurat wait <id>` | Block until task completes |
| `ziggurat batch --from <file>` | Submit batch from JSON/YAML |
| `ziggurat dead-letter` | List dead-lettered tasks |
| `ziggurat run --env <name> --env-setup <cmd>` | Submit with persistent environment |

### Pipelines (Task DAGs)
| Command | Description |
|---------|-------------|
| `ziggurat pipeline submit <file>` | Submit a pipeline definition |
| `ziggurat pipeline status <id>` | Pipeline status + stage details |
| `ziggurat pipeline cancel <id>` | Cancel all stages |
| `ziggurat pipeline retry <id>` | Retry from first failed stage |

### Environments
| Command | Description |
|---------|-------------|
| `ziggurat env list` | List persistent environments on this node |
| `ziggurat env prune` | Remove stale environments |

### Storage
| Command | Description |
|---------|-------------|
| `ziggurat put <key> <file>` | Upload object |
| `ziggurat get <key> [dest]` | Download object |
| `ziggurat ls [prefix]` | List objects |
| `ziggurat rm <key>` | Delete object |
| `ziggurat pin <key>` | Pin object (prevent GC) |
| `ziggurat unpin <key>` | Unpin object (allow GC) |
| `ziggurat mount <path>` | FUSE-mount the store at a directory |

### Diagnostics
| Command | Description |
|---------|-------------|
| `ziggurat top` | Live cluster dashboard (nodes, tasks, load) |
| `ziggurat top --once` | Single snapshot, no refresh |
| `ziggurat benchmark` | CPU, memory, disk I/O, GPU detection, and peer latency benchmarks |
| `ziggurat benchmark --skip-network` | Local benchmarks only |

All commands support `--json` for machine-parseable output.

## Architecture

```
ziggurat/
  cmd/ziggurat/        # main entry point
  internal/
    api/               # HTTP REST API (chi router)
    benchmark/         # Local + network benchmarks, GPU detection
    cluster/           # Gossip membership (memberlist)
    cmd/               # CLI commands (cobra)
    config/            # YAML config loading + defaults
    coord/             # Coordinator: task queue, scheduling, pipelines
    metrics/           # Prometheus metric definitions
    model/             # Shared types (Task, Node, Pipeline, etc.)
    node/              # Node lifecycle, capability detection
    scheduler/         # Locality + load-balanced scoring
    store/             # Content-addressed storage (BLAKE3, BoltDB)
    transport/         # gRPC inter-node transport (protobuf)
    worker/            # Process execution engine
  proto/               # Protobuf definitions
  docs/                # Spec and UX design docs
```

### Key Design Points

- **Content-addressed storage**: BLAKE3 hashing, integrity verified on read (verifyingReader), deduplication via refcounting
- **Deterministic tar**: Sorted entries, normalized metadata (uid/gid 0, epoch mtime, fixed mode) — identical content always produces identical hashes across platforms
- **Pull-based local scheduling**: Workers poll the coordinator via Dequeue; natural load balancing without push complexity
- **Push-based cross-node dispatch**: Coordinator dispatches tasks to remote workers via gRPC, collects results, and replicates output back to the origin node
- **Persistent environments**: Fingerprint-based reuse of task environments (venvs, node_modules, etc.) — same deps = same env across tasks, rebuilt only when fingerprint changes
- **Pipeline DAGs**: Kahn's algorithm cycle detection, `$stage.output` reference resolution, transitive failure cancellation
- **Platform-split process management**: `process_unix.go` (SIGTERM/SIGKILL via process groups) and `process_windows.go` (CREATE_NEW_PROCESS_GROUP + TerminateProcess)

### Fault tolerance & delivery semantics

When a node leaves the cluster, the coordinator re-queues the tasks it was running (`RUNNING`/`SCHEDULED`) so they can be retried on another capable node; its load tracking and stale shard placements are cleaned up and a repair pass restores replication. Graceful shutdowns broadcast a clean departure; crashes are caught by the gossip failure detector.

**Task execution is at-least-once, not exactly-once.** Re-queuing assumes a departed node has stopped working. On a true crash this holds. But during a **network partition** — the node is alive and still executing but unreachable from the coordinator — the task may be re-dispatched and run a second time elsewhere. Design your task commands to be **idempotent** (or tolerant of duplicate runs); writing outputs to content-addressed storage, which is keyed by content hash, naturally deduplicates. Exactly-once execution would require execution leases / fencing tokens and is not currently implemented.

## Cross-Platform Notes

Ziggurat runs on Linux and Windows. Platform-specific code is isolated via build tags:

| Concern | Linux | Windows |
|---------|-------|---------|
| Process groups | `Setpgid` + `SIGTERM`/`SIGKILL` | `CREATE_NEW_PROCESS_GROUP` + `TerminateProcess` |
| Graceful shutdown signals | `SIGINT` + `SIGTERM` | `SIGINT` only |
| Memory detection | `syscall.Sysinfo` | `GlobalMemoryStatusEx` |
| Disk space detection | `syscall.Statfs` | `GetDiskFreeSpaceExW` |

File permissions (`0o755`/`0o644`) are specified but only enforced on Linux; Windows ignores them. Tar archives use forward-slash paths regardless of OS for cross-platform determinism.

### Windows gotchas for LAN use

- **Commands must be real executables.** Task commands are executed directly, not through a shell. On Windows, shell built-ins like `echo`, `dir`, `set`, and `type` are *not* programs and will fail with "executable file not found". Wrap them: `ziggurat run -- cmd /c echo hello` (or invoke an interpreter, e.g. `python script.py`).
- **Firewall.** Zero-config LAN discovery needs inbound traffic allowed for the gossip port (TCP+UDP `7102`), the gRPC port (`7101`), the HTTP API (`7100`), and mDNS (UDP `5353`). On first run, allow `ziggurat.exe` through Windows Defender Firewall on **private** networks, or peers won't discover/reach each other.
- **Advertise address.** If the machine has Docker or a WSL2 bridge on a `172.16–31.x.x` subnet, the node may auto-select that address — unreachable from other machines. Ziggurat warns when it detects this range; set `network.advertise` to the machine's real LAN IP in `~/.ziggurat/ziggurat.yaml`.
- **Persistent environments.** Python venvs on Windows place executables in `Scripts\` (vs `bin/` on Unix); both are added to `PATH` automatically.

### Running inside WSL2

By default WSL2 uses **NAT networking**: the Linux instance sits on a private subnet (e.g. `192.168.239.x` / `172.x`) behind the Windows host, *not* on your LAN. A node started inside WSL2 can reach the mesh **outbound** (so `--join` and even task execution appear to work), but it advertises its private WSL address, which **other physical machines cannot route to**. The node ends up only half-connected: reachable from its own host, invisible to the rest of the LAN. mDNS auto-discovery also won't cross the NAT boundary.

Note that the private subnet is often a `192.168.x` range indistinguishable from a real LAN by address alone, so Ziggurat cannot reliably warn about it — the symptom is simply that peers on other machines never see the WSL node.

To make a WSL2 node a real LAN member, choose one:

1. **Mirrored networking (recommended, Windows 11 22H2+).** Give WSL the host's LAN identity. In `C:\Users\<you>\.wslconfig`:
   ```ini
   [wsl2]
   networkingMode=mirrored
   ```
   Then `wsl --shutdown` and restart. The WSL node now joins and is discovered like a native host.
2. **Run the node natively on Windows** instead of inside WSL — simplest, and the intended deployment.
3. **Manual port-forward (advanced).** Keep NAT, set `network.advertise` to the Windows host's LAN IP, and forward the gossip/gRPC/HTTP ports from the host into WSL with `netsh interface portproxy`. Fiddly and easy to get wrong; prefer options 1–2.

## Monitoring

Prometheus metrics are served at `/metrics` on the HTTP port (default 7100):

- `ziggurat_tasks_submitted_total` — counter
- `ziggurat_tasks_completed_total{status}` — counter by terminal status
- `ziggurat_task_duration_seconds` — histogram (1s to ~4.5h buckets)
- `ziggurat_task_queue_depth` — gauge
- `ziggurat_workers_active` — gauge
- `ziggurat_store_objects_total` — gauge
- `ziggurat_store_bytes_total` — gauge
- `ziggurat_nodes_total{role}` — gauge by role (hybrid/coordinator/worker/all)

## Implementation Status

### Phase 0a: Single Node -- Complete
Task execution, storage, API, CLI.

### Phase 0b: Cluster -- Complete
Gossip discovery, replication, distributed scheduling, gRPC transport, failure detection + task requeue.

### Phase 1.5: LAN Productivity -- Complete
Streaming I/O, storage repair loop, dead letter queue, batch submission, Prometheus metrics, pipelines, cross-node dispatch with output replication.

### Phase 1: Production -- Complete
All planned features implemented: shard rebalancing on join, drain with shard migration, resource-aware scheduling (memory/CPU/GPU admission), work stealing from overloaded workers, mDNS auto-discovery (`_ziggurat._tcp.local`), remote cancel propagation via gRPC, schema versioning for BoltDB, integration test harness, task log streaming (SSE), container execution (OCI via Podman/Docker), mTLS + join tokens + API bearer auth.

### Phase 2: Advanced -- Planned
Coordinator failover (Raft), speculative execution, cross-cluster federation, Python client, encryption at rest, cgroup resource limits.

## Building

```bash
go build -o ziggurat ./cmd/ziggurat

# With version info
go build -ldflags "-X main.version=0.1.0 -X main.commit=$(git rev-parse --short HEAD)" -o ziggurat ./cmd/ziggurat
```

Or use the Makefile (injects version/commit automatically):

```bash
make build              # build for the host OS (ziggurat, or ziggurat.exe on Windows)
make install            # build and copy to ~/.local/bin (%USERPROFILE%\.local\bin on Windows)
make windows            # cross-compile ziggurat.exe for Windows amd64
```

The Makefile works both under a POSIX shell (Linux, macOS, Git Bash/MSYS2) and under
`cmd.exe`/PowerShell on Windows. FUSE `mount` is excluded from Windows builds.

Requires Go 1.24+. No CGo dependencies.

## Testing

```bash
go test ./...
```
