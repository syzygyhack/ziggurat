# Ziggurat User Guide

## Installation

Build from source (requires Go 1.24+):

```bash
go build -o ziggurat ./cmd/ziggurat
```

Or with version metadata:

```bash
make build
```

Put the binary anywhere on your PATH. There are no other dependencies.

## Deployment

The simplest deployment is: copy the binary, run it.

```bash
ziggurat start
```

No config file is needed. The node creates `~/.ziggurat/` for storage and
metadata, binds the default ports, and is immediately ready to accept tasks.

### Optional: Generate a Config

To customize settings before first start:

```bash
ziggurat init
```

This writes a commented `~/.ziggurat/ziggurat.yaml` with LAN defaults. Edit
it (ports, tags, seeds, role, etc.) then start the node. If a config already
exists there, init refuses to overwrite it.

The config search order is:
1. `--config <path>` (explicit flag)
2. `./ziggurat.yaml` (current directory)
3. `~/.ziggurat/ziggurat.yaml` (data directory)
4. Built-in defaults

## Starting a Node

```bash
ziggurat start
```

This starts a single hybrid node (coordinator + worker) on the default ports:
- **7100** HTTP API
- **7101** gRPC inter-node transport
- **7102** gossip

### Joining a Cluster

On additional machines, point to any existing node's gossip address:

```bash
ziggurat start --join 10.0.0.5:7102
```

Nodes discover each other via gossip -- you only need one seed address. New
members propagate automatically.

### Node Roles

Every node defaults to `hybrid` (both schedules and executes tasks). For larger
clusters, separate the roles:

```bash
# Coordinator only -- schedules tasks, doesn't run them
ziggurat start --role coordinator

# Worker only -- executes tasks, doesn't schedule
ziggurat start --role worker --join 10.0.0.5:7102
```

Roles can also be set in `ziggurat.yaml`:

```yaml
node:
  role: worker
```

## Running Tasks

### Basic Execution

```bash
# Fire and forget -- prints the task ID
ziggurat run -- echo "hello world"

# Wait for completion and print stdout
ziggurat run --wait -- python3 train.py --epochs 10
```

The `--` separates ziggurat flags from the task command. Everything after `--`
is the command that runs on the worker.

### Task Options

```bash
ziggurat run \
  --priority 10 \
  --timeout 30m \
  --retries 3 \
  --require gpu \
  --constraint "gpu.vram >= 16GB" \
  --memory 8GB \
  --cpus 4 \
  --wait \
  -- python3 train.py
```

| Flag | Purpose |
|------|---------|
| `--wait, -w` | Block until task completes, print stdout/stderr |
| `--priority N` | Higher = dequeued sooner |
| `--timeout 5m` | Per-attempt timeout |
| `--retries N` | Retry on failure (up to N times) |
| `--require tag` | Only run on workers with this tag (repeatable) |
| `--constraint expr` | Capability constraint (repeatable) |
| `--memory 4GB` | Memory requirement |
| `--cpus 4` | CPU cores requirement |
| `--input name=key` | Named store reference, fetched into workspace |
| `--artifact key` | Store key extracted into workspace root |
| `--param key=val` | Key-value parameter passed to the task |
| `--image ref` | Run the task in an OCI container (routed to nodes advertising a container runtime) |
| `--keep-workspace` | Don't clean up workspace on failure |
| `--max-output 1GB` | Output size limit |
| `--gpus N` | GPU devices required |
| `--affinity id` | Prefer a specific node for scheduling |
| `--env name` | Persistent environment name (reused across tasks) |
| `--env-setup cmd` | Shell command to initialize the environment |
| `--env-fingerprint file` | File whose content triggers env rebuild (repeatable) |

### Checking Task Status

```bash
# Dashboard -- running, queued, completed, failed counts
ziggurat status

# List all tasks
ziggurat tasks

# Filter by status
ziggurat tasks --status running

# Full detail on a single task
ziggurat task <id>

# Block until a task finishes
ziggurat wait <id>

# Block with a timeout
ziggurat wait --timeout 5m <id>

# Cancel a task
ziggurat cancel <id>
```

Task IDs are UUIDs. Short prefixes work in most commands (e.g. `ziggurat task a3f8`).

### Dead Letter Queue

Tasks that exhaust all retries land in the dead letter queue:

```bash
ziggurat dead-letter
```

## Batch Submission

Submit multiple tasks at once from a YAML file:

```yaml
# batch.yaml — fields mirror `ziggurat run` and the task API
- command: [python3, train.py, --seed, "1"]
  requires: [gpu]
  resources: {gpus: 1, cpu_cores: 4}     # memory is in bytes if set
- command: [python3, train.py, --seed, "2"]
  constraints: ["python.version >= 3.10"]
  environment: {name: ml-env}            # reuse a persistent environment
  config: {priority: 10}
- command: [run]
  image: docker.io/library/python:3.12   # OCI container execution
```

```bash
ziggurat batch --from batch.yaml
```

Supported per-task fields match the API: `command`, `env`, `input_refs`,
`artifacts`, `params`, `requires`, `constraints`, `resources`, `environment`,
`image`, and `config`.

All tasks are submitted together. If any task fails validation, previously
submitted tasks in the batch are cancelled.

## Pipelines

Pipelines define multi-stage DAGs where stages can depend on each other:

```yaml
# pipeline.yaml
name: train-and-eval
stages:
  - id: preprocess
    command: [python3, prep.py]
  - id: train
    command: [python3, train.py, --data, "$preprocess.output"]
    depends_on: [preprocess]
    requires: [gpu]
  - id: eval
    command: [python3, eval.py, --model, "$train.output"]
    depends_on: [train]
```

```bash
# Submit
ziggurat pipeline submit pipeline.yaml

# Check progress
ziggurat pipeline status <id>

# Retry from first failed stage
ziggurat pipeline retry <id>

# Cancel all stages
ziggurat pipeline cancel <id>
```

Stage outputs are referenced with `$stage_id.output`. Stages run as soon as all
their dependencies complete. If a stage fails, downstream stages are cancelled.

## Storage

Ziggurat has built-in content-addressed storage. Objects are deduplicated and
integrity-verified via BLAKE3 hashing.

```bash
# Upload a file
ziggurat put datasets/mnist data/mnist.tar.gz

# Upload a directory (auto-archived as deterministic tar)
ziggurat put datasets/imagenet ./imagenet/

# Download
ziggurat get datasets/mnist ./mnist.tar.gz

# Download and extract a tar archive
ziggurat get datasets/imagenet ./output --extract

# List objects
ziggurat ls
ziggurat ls datasets/

# Delete
ziggurat rm datasets/mnist
```

Tasks can reference stored objects via `--input` and `--artifact` flags. Inputs
are named references available in the workspace; artifacts are extracted into
the workspace root.

## Cluster Management

### Status Dashboard

```bash
ziggurat status
```

Shows health, node count, task counts (running/queued/completed/failed/cancelled/dead-letter),
store usage, and a table of active tasks.

### Node Listing

```bash
# All nodes
ziggurat nodes

# Detail for one node
ziggurat node <id>
```

The list view shows node ID, name, HTTP address, role, and tags. The detail view
(`ziggurat node <id>`) also shows gRPC and gossip addresses, capabilities, status,
running/queued task counts, and uptime.

### Drain and Resume

To gracefully take a node out of service:

```bash
# Stop dequeuing new tasks (in-flight tasks finish normally)
ziggurat drain

# Resume accepting tasks
ziggurat resume
```

Drain does not stop the node -- it only pauses task dequeuing. Submissions
still succeed (tasks queue but won't execute on the drained node). Use
`resume` to return to normal operation.

## Persistent Environments

Tasks that need a pre-built environment (virtualenv, node_modules, etc.) can use
persistent environments. These survive across tasks and are rebuilt only when their
fingerprint changes.

```bash
# Create/reuse a named environment, initialized by a setup command
ziggurat run \
  --env my-venv \
  --env-setup "python3 -m venv . && ./bin/pip install -r requirements.txt" \
  --env-fingerprint requirements.txt \
  -- ./bin/python train.py
```

| Flag | Purpose |
|------|---------|
| `--env name` | Name for the environment directory (reused across tasks) |
| `--env-setup cmd` | Shell command run inside the env dir on first use or when stale |
| `--env-fingerprint file` | File to hash for staleness detection (repeatable) |

Rules:
- `--env-setup` requires `--env` or `--env-fingerprint` to be meaningful.
- If fingerprint files change, the setup command re-runs automatically.
- If no fingerprint files are specified, setup runs only once (on initial creation).
- Missing fingerprint files are a fatal error (not silently skipped).

The environment directory is passed to the task as `$ZIGGURAT_ENV`. The setup
command also receives `$ZIGGURAT_WORKSPACE` and `$ZIGGURAT_INPUT`.

### Managing Environments

```bash
# List environments on this node
ziggurat env list

# Remove stale environments (default: unused for 7 days)
ziggurat env prune
ziggurat env prune --max-age 48h
```

## Live Dashboard

```bash
ziggurat top
```

Displays a live-refreshing view of cluster status, node load, and active tasks.
Refreshes every 2 seconds by default.

```bash
# Custom refresh interval
ziggurat top --interval 5s

# Single snapshot (useful for scripts)
ziggurat top --once

# Machine-parseable output
ziggurat top --json
```

Press Ctrl+C to quit the live view.

## Benchmarking

```bash
ziggurat benchmark
```

Runs local CPU (BLAKE3 throughput), memory, and disk I/O benchmarks. Detects
GPUs via `nvidia-smi` if available. If a running node is reachable, also
measures HTTP round-trip latency to all cluster peers.

```bash
# Skip network probes (local benchmarks only)
ziggurat benchmark --skip-network

# JSON output
ziggurat benchmark --json
```

## Interactive Shell

```bash
ziggurat shell
```

Opens a REPL connected to the cluster. Supports `ls`, `put`, `get`, `rm`,
`run`, `tasks`, `status`, `nodes`, and `top` commands. Type `help` for usage,
`exit` or Ctrl+D to quit.

## FUSE Mount (Linux)

```bash
ziggurat mount /mnt/zig
```

Mounts the object store as a filesystem. Objects appear as files using their
namespace keys; subdirectories are derived from key prefixes. Supports read
and write.

```bash
ls /mnt/zig/
cat /mnt/zig/datasets/train.csv
cp results.tar /mnt/zig/outputs/results.tar
```

Press Ctrl+C or run `fusermount -u /mnt/zig` to unmount.

## Configuration

Run `ziggurat init` to generate a config, or place a `ziggurat.yaml` in your
working directory or `~/.ziggurat/`. Override any location with `--config <path>`.

All fields are optional. Here is a complete reference:

```yaml
node:
  name: gpu-workstation           # human-readable node name
  role: hybrid                    # hybrid | coordinator | worker
  tags: [gpu, cuda, python3]      # worker tags for --require matching
  data_dir: ~/.ziggurat           # storage and metadata directory
  capabilities:                   # key-value pairs for --constraint matching
    gpu.vram: 24GB

network:
  bind: 0.0.0.0                   # listen address
  http_port: 7100                 # REST API port
  grpc_port: 7101                 # inter-node transport port
  gossip_port: 7102               # memberlist gossip port

client:
  addr: 10.0.0.5:7100            # default server for CLI commands

cluster:
  seeds: [10.0.0.5:7102]         # initial gossip peers

storage:
  replication_factor: 2           # copies across cluster
  capacity: 53687091200           # 50 GB limit (bytes)
  gc_grace_period: 1h             # delay before garbage collection

compute:
  concurrency: 4                  # max parallel tasks (0 = NumCPU)
  task_timeout: 5m                # default per-task timeout
  max_output_size: 1073741824     # 1 GB max output capture
  cancel_grace: 10s               # SIGTERM-to-SIGKILL grace period
  max_retained_workspaces: 20     # task workspaces kept for debugging

resilience:
  task_retries: 2                 # default retry count
  dead_letter: true               # enable dead letter queue

metrics:
  enabled: true                   # expose /metrics endpoint
```

### Client Address Resolution

Client commands (everything except `start`) resolve the server address in this order:

1. `--addr` flag
2. `ZIGGURAT_ADDR` environment variable
3. `client.addr` in config file
4. `127.0.0.1:7100` (default)

## JSON Output

Every command supports `--json` for scripting:

```bash
# Parse with jq
ziggurat tasks --json | jq '.[].id'

# Get task status programmatically
ziggurat task abc123 --json | jq '.status'
```

## Monitoring

Prometheus metrics are served at `http://<node>:7100/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `ziggurat_tasks_submitted_total` | counter | Total tasks submitted |
| `ziggurat_tasks_completed_total` | counter | Tasks by terminal status |
| `ziggurat_task_duration_seconds` | histogram | Execution time distribution |
| `ziggurat_task_queue_depth` | gauge | Current queue length |
| `ziggurat_workers_active` | gauge | Workers currently executing |
| `ziggurat_store_objects_total` | gauge | Objects in storage |
| `ziggurat_store_bytes_total` | gauge | Storage bytes used |
| `ziggurat_nodes_total` | gauge | Cluster node count by role |

## Typical Workflows

### Single-Machine Research

```bash
ziggurat start
ziggurat put data/corpus ./corpus/
ziggurat run --wait --artifact data/corpus -- python3 train.py
ziggurat task <id>    # check results
```

### Multi-Machine Sweep

```bash
# Machine A (coordinator)
ziggurat start --role coordinator

# Machines B, C, D (GPU workers)
ziggurat start --role worker --join A:7102

# From any machine, submit a parameter sweep
for lr in 0.001 0.01 0.1; do
  ziggurat run --require gpu --param lr=$lr -- python3 train.py --lr $lr
done

ziggurat status    # watch progress
```

### CI/CD Pipeline

```yaml
# ci-pipeline.yaml
name: build-test-deploy
stages:
  - id: build
    command: [make, build]
  - id: test
    command: [make, test]
    depends_on: [build]
  - id: deploy
    command: [make, deploy]
    depends_on: [test]
```

```bash
ziggurat pipeline submit ci-pipeline.yaml
ziggurat pipeline status <id>
```
