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
  seeds: [10.0.0.5:7102]

storage:
  replication_factor: 2
  capacity: 53687091200  # 50 GB
  gc_grace_period: 1h

compute:
  concurrency: 4           # max parallel tasks (0 = NumCPU)
  task_timeout: 5m
  max_output_size: 1073741824  # 1 GB
  cancel_grace: 10s
  max_retained_workspaces: 20

resilience:
  task_retries: 2
  dead_letter: true
```

Connection resolution for client commands: `--addr` flag > `ZIGGURAT_ADDR` env > config `client.addr` > `127.0.0.1:7100`.

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

### Tasks
| Command | Description |
|---------|-------------|
| `ziggurat run -- <cmd>` | Submit a task |
| `ziggurat run --wait -- <cmd>` | Submit and wait for result |
| `ziggurat tasks` | List tasks (filterable: `--status running`) |
| `ziggurat task <id>` | Task detail + stdout/stderr |
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

## Cross-Platform Notes

Ziggurat runs on Linux and Windows. Platform-specific code is isolated via build tags:

| Concern | Linux | Windows |
|---------|-------|---------|
| Process groups | `Setpgid` + `SIGTERM`/`SIGKILL` | `CREATE_NEW_PROCESS_GROUP` + `TerminateProcess` |
| Graceful shutdown signals | `SIGINT` + `SIGTERM` | `SIGINT` only |
| Memory detection | `syscall.Sysinfo` | `GlobalMemoryStatusEx` |
| Disk space detection | `syscall.Statfs` | `GetDiskFreeSpaceExW` |

File permissions (`0o755`/`0o644`) are specified but only enforced on Linux; Windows ignores them. Tar archives use forward-slash paths regardless of OS for cross-platform determinism.

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

### Phase 1: Production -- Partial
Remaining: erasure coding, shard rebalancing, drain with migration, container execution (OCI), mTLS + join tokens, resource-aware scheduling, work stealing, mDNS auto-discovery, remote cancel propagation.

### Phase 2: Advanced -- Planned
Coordinator failover (Raft), speculative execution, cross-cluster federation, Python client, encryption at rest, cgroup resource limits, live task streaming.

## Building

```bash
go build -o ziggurat ./cmd/ziggurat

# With version info
go build -ldflags "-X main.version=0.1.0 -X main.commit=$(git rev-parse --short HEAD)" -o ziggurat ./cmd/ziggurat
```

Requires Go 1.24+. No CGo dependencies.

## Testing

```bash
go test ./...
```
