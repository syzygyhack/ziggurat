# Ziggurat: Distributed Research Compute Mesh

## Problem

Research workloads (numerical evaluation, RG running, symbolic algebra, data validation) are expensive. Running them on the same machine used for development degrades both. What's needed: offload compute to available LAN devices (spare machines, NUCs, old laptops, cloud VMs) while the primary machine stays free for dev work.

Existing solutions either require heavyweight infrastructure (Kubernetes, Spark, Ray), external dependencies (Celery needs Redis, Temporal needs a server), or are language-specific (Dask/Ray are Python-centric). None provide a single binary you drop on N machines that auto-discovers peers, accepts tasks, manages its own storage, and handles distribution/failover transparently.

## What Ziggurat Is

A single Go binary that forms a compute mesh with integrated distributed storage. Drop it on any machine, it finds the cluster (or starts one), advertises its capabilities, and begins accepting work. One HTTP endpoint to submit tasks and retrieve results.

Tasks are arbitrary commands -- any script, binary, or pipeline that runs on the worker's OS. Ziggurat manages the workspace, fetches inputs from distributed storage, executes the command, captures output, and uploads results. No SDK required. No recompilation for new task types. Write a Python script, upload it to storage, submit a task that runs it.

**Not**: a container orchestrator, a message broker, a distributed filesystem, or a database. It's a research compute cluster that happens to store the data its tasks need.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        ZIGGURAT CLUSTER                          │
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │   Node A     │◄──►│   Node B     │◄──►│   Node C     │       │
│  │  COORD+WORK  │    │   WORKER    │    │   WORKER    │         │
│  │              │    │              │    │              │         │
│  │ ┌──────────┐ │    │ ┌──────────┐ │    │ ┌──────────┐ │       │
│  │ │ Compute  │ │    │ │ Compute  │ │    │ │ Compute  │ │       │
│  │ │ Engine   │ │    │ │ Engine   │ │    │ │ Engine   │ │       │
│  │ └──────────┘ │    │ └──────────┘ │    │ └──────────┘ │       │
│  │ ┌──────────┐ │    │ ┌──────────┐ │    │ ┌──────────┐ │       │
│  │ │ Storage  │ │    │ │ Storage  │ │    │ │ Storage  │ │       │
│  │ │ Engine   │ │    │ │ Engine   │ │    │ │ Engine   │ │       │
│  │ └──────────┘ │    │ └──────────┘ │    │ └──────────┘ │       │
│  │              │    │              │    │              │         │
│  │ REST API ◄───┼────┼── client submits here            │       │
│  │ gRPC mesh    │    │ gRPC mesh    │    │ gRPC mesh    │       │
│  └──────────┘    └─────────────┘    └─────────────┘         │
│       ▲                                                         │
│       │ mDNS / seed list / gossip                               │
│       ▼                                                         │
│  ┌─────────────┐                                                │
│  │   Node D     │  (joins automatically via mDNS)               │
│  │   WORKER    │                                                │
│  └─────────────┘                                                │
└─────────────────────────────────────────────────────────────────┘
```

### Node Roles

Every node runs the same binary. Role is determined by `--role` flag or config:

| Role | Description | Flag |
|------|-------------|------|
| **hybrid** | Both coordinator and worker (default) | `--role hybrid` or omit |
| **coordinator** | Accepts client requests, schedules tasks, manages metadata | `--role coordinator` |
| **worker** | Executes tasks, stores data shards | `--role worker` |
| **client** | Submits tasks, retrieves results, no compute/storage | CLI commands (no `start`) |

A cluster has exactly one active coordinator (with optional standby). All other nodes are workers. The coordinator is also a worker by default (hybrid mode) unless `--role coordinator` is set.

---

## Storage Layer

### Design Rationale

Tasks need data. Shipping large payloads in every task submission is wasteful. A shared storage layer lets tasks reference data by key, with the scheduler placing tasks near their data.

The storage layer is **not a general-purpose filesystem**. It stores:
- **Input objects**: Datasets, coefficient tables, experimental databases (uploaded once, read many times)
- **Output objects**: Task results, intermediate computations, checkpoints
- **Ephemeral objects**: Scratch data that tasks produce and consume within a pipeline

### Content-Addressed Object Store

All objects are stored by content hash (BLAKE3). This gives:
- **Deduplication**: Same data stored once regardless of how many tasks reference it
- **Integrity**: Every read is verified against the hash
- **Immutability**: Objects never change. New versions = new hashes
- **Cache-friendly**: Any node with a copy can serve the object

```
Object Key:    blake3:<hex>     (content-addressed)
Namespace Key: ns/path/name     (human-friendly alias → content hash)
```

Clients interact via namespace keys. The storage layer resolves to content hashes internally.

```
PUT /store/datasets/planck-2018  →  stores object, returns blake3:abc123...
GET /store/datasets/planck-2018  →  resolves to blake3:abc123..., returns data
```

### Object Tiering

Objects are stored differently based on size:

| Tier | Size | Strategy | Rationale |
|------|------|----------|-----------|
| **Small** | < 1 MB | Full replication (N copies) | Fast, simple, overhead is negligible |
| **Medium** | 1 MB - 64 MB | Full replication (N copies) | Still manageable, fast reads |
| **Large** | > 64 MB | Erasure coding (RS) | Space-efficient, acceptable read latency |

Tier thresholds and replication factor are configurable:

```yaml
storage:
  data_dir: /var/ziggurat/data    # where shards live on disk
  replication_factor: 2          # for small/medium objects
  erasure:
    data_shards: 4               # k: minimum shards needed to reconstruct
    parity_shards: 2             # m: redundancy shards
    # Total shards: k+m = 6, storage overhead: 1.5x
    # Tolerates loss of any 2 nodes without data loss
  tier_thresholds:
    medium: 1MB
    large: 64MB
  capacity: 0                    # 0 = use all available, or explicit limit
```

### Erasure Coding

For large objects, Reed-Solomon erasure coding (via `klauspost/reedsolomon`) splits an object into `k` data shards and `m` parity shards, distributed across `k+m` distinct nodes. The `Put` path automatically erasure-codes objects that exceed the `large` tier threshold when `erasure.enabled: true`.

```
Original object (256 MB)
    │
    ▼
┌─────────────────────────────────┐
│  RS Encoder (k=4, m=2)         │
│  256 MB ÷ 4 = 64 MB per shard  │
└─────────────────────────────────┘
    │
    ├── Shard 0 (64 MB) → Node A
    ├── Shard 1 (64 MB) → Node B
    ├── Shard 2 (64 MB) → Node C
    ├── Shard 3 (64 MB) → Node D
    ├── Parity 0 (64 MB) → Node E
    └── Parity 1 (64 MB) → Node F

Any 4 of 6 shards → full reconstruction
Lose any 2 nodes → data survives
Storage: 384 MB for 256 MB object (1.5x overhead)
vs replication: 512 MB (2x) or 768 MB (3x)
```

When fewer nodes exist than `k+m`, the system falls back to replication. Erasure coding activates only when the cluster has enough nodes to distribute shards.

### Metadata

Object metadata is lightweight and replicated to all nodes. See [Data Structures](#core-types) for the authoritative `ObjectMeta` definition (`ObjectMeta`, `ShardPlacement`, `Tier`, `StorageStrategy`).

Metadata is small (< 1 KB per object) and kept in memory on every node with periodic persistence to disk. On a cluster with 100K objects, metadata totals ~100 MB -- trivially fits in RAM.

**Scaling envelope**: This design targets clusters of up to ~50 nodes and ~1M objects. Full metadata replication means every `PUT` broadcasts to all nodes, and every node join triggers a full metadata sync. Within this envelope, the overhead is negligible. Beyond it, metadata sharding would be required (not planned).

### Garbage Collection

Objects are reference-counted. References come from:
- Namespace aliases
- Task inputs/outputs (while task is active)
- Explicit pins (user marks object as persistent)

When refcount hits zero, the object enters a grace period (configurable, default 1 hour). If no new references appear, shards are deleted.

```
Object lifecycle:
  upload → refcount=1 (namespace ref)
  task references it → refcount=2
  task completes → refcount=1
  namespace delete → refcount=0 → grace period → GC
```

### Namespace Key Semantics

Namespace keys are mutable aliases that point to immutable content hashes.

**Overwrite**: `PUT` to an existing namespace key overwrites the alias. The old content hash loses one reference (namespace ref decremented). The new content hash gains one. If the old hash's refcount hits zero, GC rules apply. This is the only operation -- no versioning, no append.

**Concurrent writes**: Last-write-wins. Two concurrent `PUT`s to the same key result in one winner; the other's content still exists in the store (content-addressed) but has no namespace alias. For pipelines and automated workflows, use unique keys (e.g., include task ID or timestamp in the namespace path) to avoid races.

**Delete**: Removes the namespace alias and decrements the content hash's refcount. The content is not immediately deleted -- GC grace period applies.

### Directory Storage Model

Directories are stored as deterministic tar archives. When uploading a directory:

1. Walk the directory tree, sorted lexicographically by path
2. Create a tar archive with normalized metadata (mode 0644/0755, uid/gid 0, mtime set to Unix epoch)
3. BLAKE3-hash the archive → content address
4. Store and replicate the archive as a single object

Deterministic tar rules ensure the same directory contents always produce the same hash, preserving deduplication.

When downloading, `ziggurat get` returns the raw tar by default. The `--extract` flag (or `Accept: application/x-tar+extract` header) extracts to a destination directory. Task input fetching always extracts automatically.

Artifacts follow the same model: a directory artifact is a tar in storage, extracted into the workspace root before execution.

### Consistent Hash Ring

Shard placement uses a consistent hash ring (`cluster.HashRing`) to deterministically map content hashes to nodes. Each physical node gets 128 virtual nodes on the ring for even distribution. When a node joins or leaves, only ~1/N of keys remap.

```
GetNode(contentHash) → primary node for that object
GetNodes(contentHash, k+m) → ordered set of nodes for erasure-coded shard distribution
```

The ring is derived from the gossip membership list and automatically updates on node join/leave events.

### Storage Classes

Nodes advertise their storage hardware via the `storage.class` capability:

| Class | Capability Value | Typical Use |
|-------|-----------------|-------------|
| NVMe  | `storage.class: nvme` | Hot data, small frequently-accessed objects |
| SSD   | `storage.class: ssd`  | Default, general-purpose |
| HDD   | `storage.class: hdd`  | Cold data, large erasure-coded objects |
| S3    | `storage.class: s3`   | External cloud storage |

The placement policy (`store.SelectNodes`) routes objects to appropriate storage classes based on tier:

- **Small** (< 1 MB): prefer NVMe > SSD > HDD
- **Medium** (1-64 MB): prefer SSD > NVMe > HDD
- **Large** (> 64 MB): prefer HDD > SSD > NVMe (erasure coding handles resilience)

### FUSE Mount

`ziggurat mount <dir>` mounts the cluster's object store as a local filesystem via FUSE:

```
/mnt/zig/                     ← mount point
├── datasets/
│   ├── training.csv          ← namespace key "datasets/training.csv"
│   └── validation.csv
├── models/
│   └── checkpoint.pt
└── outputs/
```

- **Read**: `open()` → resolve namespace key → `GET` from store API
- **Write**: buffer to temp file → `PUT` to store API on `close()`
- **Directory listing**: derived from namespace key prefixes
- **Delete**: `unlink()` → `DELETE` on store API

### Interactive Shell

`ziggurat shell` opens a REPL connected to the cluster:

```
$ ziggurat shell
Ziggurat interactive shell. Type 'help' for commands, 'exit' to quit.
zig> ls datasets/
training.csv    validation.csv
zig> put results.csv outputs/results.csv
stored: outputs/results.csv (blake3:a1b2c3d4)
zig> run python3 process.py
submitted: task-abc1
zig> status
Status: healthy  Nodes: 3  Tasks: 1 running, 0 queued, 42 completed
zig> exit
```

Commands: `ls`, `put`, `get`, `rm`, `run`, `tasks`, `status`, `nodes`, `help`, `exit`.

### Degraded Replication

When the cluster has fewer nodes than `replication_factor`:

- **PUT succeeds** with degraded replication (writes to all available nodes)
- Object metadata records actual vs. desired replica count
- A repair queue entry is created; when new nodes join, under-replicated objects are re-replicated to meet the target
- The `store status` command and health endpoint report under-replicated object counts

This means a single-node cluster works -- objects are stored locally with replication_factor=1 regardless of config. As nodes join, the background repair process brings objects up to the configured level.

### Data Locality Scheduling

The coordinator knows where every object shard lives (via metadata). When scheduling a task:

1. Identify input objects for the task
2. Score each worker: `locality_score = (local_shards / total_input_shards)`
3. Weight with load: `score = locality_score * (1 - load_factor)`
4. Assign to highest-scoring worker

This means tasks run where their data already is, minimizing network transfer.

**Priority and locality are independent axes.** Priority determines which task is dequeued next (highest priority first, FIFO within same priority). Locality determines which worker is selected for that task (highest locality score wins). A high-priority task with no local data still schedules before a low-priority task with perfect locality -- it just runs on whatever eligible worker has the best score for its specific inputs.

---

## Compute Layer

### Execution Model: Process Execution

Ziggurat does not compile task logic into its binary. Instead, it executes **arbitrary commands** in isolated workspaces. The worker doesn't understand what the task does -- it sets up the environment, runs the command, and captures the result. This is how every real HPC system works (SLURM, PBS, SGE).

Any language, any tool, any script. If it runs on the worker's OS, Ziggurat can execute it.

### Task Contract

The contract between Ziggurat and a task is pure environment:

| Variable | Description |
|----------|-------------|
| `ZIGGURAT_WORKSPACE` | Root of temporary workspace for this task |
| `ZIGGURAT_INPUT` | Directory containing fetched input objects |
| `ZIGGURAT_OUTPUT` | Directory where task writes its results |
| `ZIGGURAT_TASK_ID` | Task UUID |
| `ZIGGURAT_ATTEMPT` | Current attempt number (0-indexed) |
| `ZIGGURAT_PARAM_<KEY>` | Each task parameter as an env var (uppercased) |
| `ZIGGURAT_ENV` | Persistent environment directory (only if `environment` is set) |
| `ZIGGURAT_ENV_NAME` | Environment name (only if `environment` is set) |
| `VIRTUAL_ENV` | Same as `ZIGGURAT_ENV` (Python venv compatibility) |
| `CUDA_VISIBLE_DEVICES` | Allocated GPU device indices (only if `--gpus` > 0) |

**The rules**:
- Read data from `$ZIGGURAT_INPUT/<name>/` (populated from storage before execution)
- Write results to `$ZIGGURAT_OUTPUT/` (uploaded to storage after execution)
- Parameters arrive as `$ZIGGURAT_PARAM_*` env vars
- Exit 0 = success. Non-zero = failure.
- Stdout/stderr are captured and attached to the task result.

**Environment variable protection**: The `Env` field on a task allows injecting additional env vars. However, `ZIGGURAT_*` variables set by the worker are authoritative and MUST NOT be overridden by user-supplied `Env` entries. The worker silently drops any `Env` key prefixed with `ZIGGURAT_`. System-critical vars (`PATH`, `HOME`, `USER`, `SHELL`) are inherited from the worker's environment and cannot be replaced via `Env` -- they can only be extended (e.g., prepending to `PATH` is done via artifacts, not env override).

No SDK, no library, no framework required. A shell script works:

```bash
#!/bin/bash
# rg_evolve.sh -- runs RG evolution
python3 rg_evolve.py \
    --input "$ZIGGURAT_INPUT/coefficients" \
    --scale "$ZIGGURAT_PARAM_SCALE" \
    --output "$ZIGGURAT_OUTPUT/predictions.json"
```

### Worker Execution Flow

```
Coordinator assigns task to Worker B:

  1. Create workspace:
     /tmp/ziggurat/<task-id>/
     /tmp/ziggurat/<task-id>/input/
     /tmp/ziggurat/<task-id>/output/

  2. Fetch inputs from storage into workspace:
     storage:datasets/fhuft-v3  →  /tmp/ziggurat/<task-id>/input/coefficients/
     storage:scripts/rg_evolve  →  /tmp/ziggurat/<task-id>/rg_evolve.py

  3. Set environment:
     ZIGGURAT_WORKSPACE=/tmp/ziggurat/<task-id>
     ZIGGURAT_INPUT=/tmp/ziggurat/<task-id>/input
     ZIGGURAT_OUTPUT=/tmp/ziggurat/<task-id>/output
     ZIGGURAT_TASK_ID=<task-id>
     ZIGGURAT_ATTEMPT=0
     ZIGGURAT_PARAM_SCALE=mZ
     ZIGGURAT_PARAM_OBSERVABLES=sin2tw,mW

  4. Execute command:
     ["python3", "rg_evolve.py"]
     working directory = workspace root
     stdout/stderr captured

  5. On exit 0:
     Upload contents of output/ to storage → set task.OutputRef
     Capture stdout/stderr as task.Stdout/task.Stderr

  6. On non-zero exit:
     Capture stderr as error
     Retry or dead-letter based on config

  7. Clean up workspace (configurable: immediate or keep-on-failure)
```

### Script and Code Distribution

The task's command needs to exist on the worker. Three mechanisms, composable:

**Artifacts (primary mechanism)**: Objects fetched from storage into the workspace root before execution. The task is self-contained -- code, data, and command all come from storage.

```yaml
command: ["python3", "rg_evolve.py"]
artifacts:                             # fetched into workspace root
  - "scripts/rg_evolve.py"            # single file
  - "scripts/lib/"                     # directory (recursive)
inputs:
  coefficients: "datasets/fhuft-v3"   # fetched into $ZIGGURAT_INPUT/coefficients/
```

Upload once with `ziggurat put scripts/rg_evolve.py ./rg_evolve.py`, reference forever. Update by uploading a new version.

**Absolute paths**: For tools already installed on the worker (system Python, compiled binaries in `/usr/local/bin`, etc.), just reference them directly:

```yaml
command: ["/usr/local/bin/class", "--ini", "params.ini"]
```

**Container images (optional)**: For tasks needing hermetic environments. Worker must have a container runtime (Podman preferred -- rootless, daemonless).

```yaml
command: ["python3", "rg_evolve.py"]
image: "ghcr.io/syzygy/research-env:latest"  # OCI image
```

When `image` is set, the workspace is bind-mounted into the container. Same env vars, same contract, different execution environment.

### Persistent Environments

Tasks often need dependencies (Python packages, npm modules, compiled libraries) that are expensive to install on every run. Ziggurat provides **persistent environments** -- named directories that survive workspace cleanup and are reused across tasks.

Ziggurat does not understand Python, pip, venv, conda, npm, or any specific runtime. It provides a named persistent directory and runs a setup command when that directory is new or stale. What goes in the directory is the task's business.

#### Task Environment Config

```go
type TaskEnvironment struct {
    Name        string   // explicit env name; empty = derive from fingerprint
    Setup       []string // command to run when env needs (re)creation
    Fingerprint []string // input/artifact filenames whose content hash determines staleness
}
```

#### Resolution Rules

1. `Name` set → use it directly (e.g. `research-rg`)
2. `Name` empty, `Fingerprint` set → BLAKE3 hash of fingerprint file contents, first 16 hex chars
3. Neither → task ID (ephemeral per-task env, no reuse)

Same fingerprint = same env name = automatic reuse across tasks. Different fingerprint = different env = isolated.

#### Worker Behavior

1. If `task.Environment` is nil → no env management (current behavior)
2. Resolve env name per rules above
3. Env directory: `<data_dir>/envs/<name>/`
4. Compute fingerprint hash from listed files (searched in workspace root, then `input/`)
5. Compare against stored `.ziggurat-fingerprint` in the env directory
6. Match → skip setup, reuse. Mismatch or missing → acquire file lock, run `Setup` command with `cwd=env_dir`, write fingerprint
7. Set environment variables:
   - `ZIGGURAT_ENV=<env_dir>` — the persistent directory
   - `ZIGGURAT_ENV_NAME=<name>`
   - `VIRTUAL_ENV=<env_dir>` — Python venv compatibility
   - Prepend `<env_dir>/bin` to `PATH`
8. Run main command as normal

#### Concurrency

File lock (`<env_dir>/.ziggurat-lock`) during setup. First task to need the env acquires lock, runs setup, writes fingerprint, releases lock. Concurrent tasks block on the lock, then share the built env. Stale locks (>10 minutes) are broken automatically.

#### GC

```yaml
compute:
  env_max_age: 168h    # 7 days — prune unused envs
  env_max_count: 50    # max persistent envs before FIFO eviction
```

Last-used timestamp is tracked per env. `ziggurat env list` shows persistent envs on a node. `ziggurat env prune` removes stale envs.

#### Cross-Node Behavior

Each worker builds its own env from `Setup`. Venvs contain platform-specific binaries and are not portable between machines. The fingerprint ensures the same deps produce the same env *name* on every node, but env *contents* are built locally per-worker.

#### CLI

```bash
# Named env with fingerprint-based rebuild
ziggurat run python3 rg_evolve.py \
  --env research-rg \
  --env-setup "python3 -m venv \$ZIGGURAT_ENV && \$ZIGGURAT_ENV/bin/pip install -r \$ZIGGURAT_INPUT/requirements/requirements.txt" \
  --env-fingerprint requirements.txt \
  --input requirements=deps/requirements.txt \
  --artifact scripts/rg_evolve.py

# Content-addressed (no name — same requirements = same env)
ziggurat run python3 rg_evolve.py \
  --env-setup "python3 -m venv \$ZIGGURAT_ENV && \$ZIGGURAT_ENV/bin/pip install -r requirements.txt" \
  --env-fingerprint requirements.txt

# JSON API
{
  "command": ["python3", "rg_evolve.py"],
  "environment": {
    "name": "research-rg",
    "setup": ["sh", "-c", "python3 -m venv $ZIGGURAT_ENV && $ZIGGURAT_ENV/bin/pip install -r $ZIGGURAT_INPUT/requirements/requirements.txt"],
    "fingerprint": ["requirements.txt"]
  }
}
```

Because `<env_dir>/bin` is prepended to PATH, `python3` inside the task resolves to the venv's Python automatically. No `source activate` needed.

### Capability Matching

Workers declare what they have. Tasks declare what they need. Coordinator only schedules to capable workers.

Capabilities come from three sources, merged at startup:

1. **Auto-detected** -- the node probes the local system at startup
2. **Configured** -- the `node.capabilities` map in `ziggurat.yaml`
3. **Tags** -- the `node.tags` list (simple presence flags, as before)

Configured values override auto-detected ones for the same key. Tags are additive.

#### Auto-Detected Capabilities

Every node probes the following at startup (no configuration needed):

| Key | Type | Example | Source |
|-----|------|---------|--------|
| `os` | string | `linux`, `darwin`, `windows` | `runtime.GOOS` |
| `arch` | string | `amd64`, `arm64` | `runtime.GOARCH` |
| `cpu.cores` | int | `16` | logical CPUs, capped by cgroup CPU quota |
| `mem.total` | int (bytes) | `34359738368` | physical RAM, capped by cgroup memory limit |
| `disk.avail` | int (bytes) | `107374182400` | Available space on `data_dir` volume |
| `storage.class` | string | `ssd`, `nvme`, `hdd` | block-device probe (seek-penalty on Windows) |
| `hostname` | string | `lab-gpu-03` | `os.Hostname()` |
| `compute.concurrency` | int | `16` | task slots (`compute.concurrency` or CPU count) |
| `container.runtime` | string | `podman`, `docker` | `PATH` lookup (omitted if none) |
| `ziggurat.version` | string | `0.3.0` | build version (tagged builds only) |

Language runtimes are probed via `--version` (omitted if absent):

| Key | Example | Source |
|-----|---------|--------|
| `python.version` | `3.12.1` | `python3`/`python --version` |
| `node.version`, `go.version`, `java.version`, `ruby.version`, `rust.version` | `20.11.0`, … | each runtime's version command |

GPU detection (NVIDIA full; AMD `rocm-smi`, Intel `xpu-smi` best-effort):

| Key | Type | Example | Source |
|-----|------|---------|--------|
| `gpu.count` | int | `2` | nvidia-smi / rocm-smi / xpu-smi |
| `gpu.model` | string | `NVIDIA A100` | model(s), comma-separated if heterogeneous |
| `gpu.vendor` | string | `nvidia` | vendor(s) detected |
| `gpu.vram` | int (bytes) | `85899345920` | total VRAM summed across all GPUs |
| `gpu.vram.max` | int (bytes) | `42949672960` | largest single-device VRAM (use for per-GPU sizing) |
| `gpu.<i>.model`, `gpu.<i>.vram` | per-device | — | per-GPU model and VRAM |
| `gpu.cuda` | string | `12.4` | CUDA toolkit version |
| `gpu.driver` | string | `550.54.15` | Driver version |

Operators can add arbitrary probes via `node.capability_probes` (e.g. a `torch.version` command) for facts not covered above.

Auto-detection is best-effort. If a probe fails (e.g., no GPU, no nvidia-smi), the key is omitted. Missing keys never cause errors -- they simply mean the capability isn't advertised.

**Refresh**: `disk.avail` is re-probed periodically (every GC cycle) since it changes as objects are stored and collected. All other auto-detected values are static for the lifetime of the process.

#### Configured Capabilities

For capabilities that can't be auto-detected (installed software, site-specific labels):

```yaml
# Worker config (ziggurat.yaml)
node:
  tags:                          # Simple presence flags (backward compatible)
    - python3
    - numpy
    - julia
    - podman

  capabilities:                  # Typed key-value pairs
    python.version: "3.12"
    julia.version: "1.10"
    cuda.version: "12.4"         # Override auto-detected if needed
    site: "building-7"
    rack: "R14"
```

Tags and capabilities are complementary:
- **Tags** answer "does this worker have X?" -- boolean presence check
- **Capabilities** answer "what is this worker's X?" -- typed value queries

A tag `python3` means the worker has Python 3. A capability `python.version: "3.12"` says exactly which version.

#### Task Constraints

Tasks declare requirements using two mechanisms:

**`requires`** (unchanged): flat list of tags. All must be present.

```yaml
requires: [python3, numpy]        # Worker must have both tags
```

**`constraints`**: list of expressions matching against capabilities. Each expression is `<key> <op> <value>`. All constraints must be satisfied.

```yaml
constraints:
  - "gpu.count >= 1"              # At least one GPU
  - "gpu.vram >= 16GB"            # At least 16 GB VRAM
  - "mem.total >= 32GB"           # At least 32 GB RAM
  - "os == linux"                 # Linux only
  - "python.version >= 3.11"     # Python 3.11+
  - "site == building-7"          # Site affinity
```

Operators:

| Op | Semantics | Applies to |
|----|-----------|------------|
| `==` | Exact match | string, int |
| `!=` | Not equal | string, int |
| `>=` | Greater or equal | int, semver string |
| `<=` | Less or equal | int, semver string |
| `>` | Greater than | int, semver string |
| `<` | Less than | int, semver string |

**Type coercion rules:**
- Integer values support byte suffixes: `16GB`, `512MB`, `1TB` (parsed same as CLI `--memory` flag)
- Version strings use semver-style comparison: `"3.12" >= "3.11"` is true, `"3.9" >= "3.11"` is false. Comparison is segment-by-segment (`major.minor.patch`), missing segments treated as 0.
- All other strings use lexicographic comparison for ordering operators

**Missing key**: if a constraint references a capability key the worker doesn't have, the constraint fails. This is correct behavior -- if a task needs `gpu.count >= 1` and the worker has no GPU capabilities, it shouldn't be scheduled there.

#### Capability Merging and Precedence

At startup, capabilities are assembled in this order:

```
1. Auto-detect system capabilities  →  base map
2. Apply node.capabilities from YAML →  overrides auto-detected keys
3. Add node.tags as boolean flags    →  tags["python3"] = true, etc.
```

Tags are stored separately from capabilities (they remain a `[]string` in the Node struct). The scheduler checks `requires` against tags and `constraints` against capabilities independently. Both must pass.

#### The Full Picture

```yaml
# ziggurat.yaml on a GPU workstation
node:
  name: gpu-workstation
  tags:
    - python3
    - numpy
    - cuda
  capabilities:
    python.version: "3.12"
    cuda.version: "12.4"
    site: "lab-east"
```

Auto-detection adds: `os=linux`, `arch=amd64`, `cpu.cores=32`, `mem.total=137438953472`, `disk.avail=500000000000`, `gpu.count=4`, `gpu.model=NVIDIA A100`, `gpu.vram=343597383680`, `gpu.driver=550.54.15`.

```bash
# Submit a task that needs a beefy GPU node
ziggurat run python3 train_model.py \
  --require python3 --require cuda \
  --constraint "gpu.vram >= 80GB" \
  --constraint "mem.total >= 64GB" \
  --constraint "python.version >= 3.11" \
  --artifact scripts/train_model.py \
  --wait
```

The coordinator checks: does the worker have tags `python3` AND `cuda`? Does `gpu.vram >= 80GB`? Does `mem.total >= 64GB`? Does `python.version >= 3.11`? All yes -- schedule it.

If no worker satisfies the requirements, the task stays queued with a clear diagnostic. No silent misscheduling.

### Task Structure

```go
type Task struct {
    ID          string               // UUID, assigned by coordinator
    Command     []string             // e.g. ["python3", "rg_evolve.py"]
    Env         map[string]string    // additional env vars (ZIGGURAT_* protected, see contract)
    InputRefs   map[string]string    // name → content hash (resolved at submission from namespace keys)
    Artifacts   []string             // content hashes (resolved at submission from namespace keys)
    Params      map[string]string    // injected as ZIGGURAT_PARAM_<KEY> env vars
    Requires    []string             // required worker tags (all must be present)
    Constraints []string             // capability constraint expressions (all must pass)
    Resources   ResourceReq          // optional resource requests for admission
    Image       string               // OCI image ref (empty = bare exec)
    Environment *TaskEnvironment     // persistent env config (optional)
    Config      TaskConfig

    // Set by coordinator / worker
    Status    TaskStatus
    Attempt   int
    Worker    string               // node ID executing this task
    OutputRef string               // storage key of output directory contents
    Stdout    string               // captured stdout (truncated if large)
    Stderr    string               // captured stderr (truncated if large)
    ExitCode  int
    Error     string               // system error (not task stderr)
    Metrics   TaskMetrics
    CreatedAt time.Time
}

type ResourceReq struct {
    Memory    int64         // bytes, 0 = no requirement (best-effort)
    CPUCores  int           // logical cores, 0 = no requirement
    GPUs      int           // GPU devices, 0 = no requirement; sets CUDA_VISIBLE_DEVICES
}

type TaskConfig struct {
    Priority      int           // higher = sooner (default 0)
    Timeout       time.Duration // per-attempt timeout (default 5m)
    MaxRetries    int           // retry on failure (default 2)
    MaxOutputSize int64         // bytes, 0 = cluster default (default 1 GB)
    Affinity      string        // prefer node with this tag
    KeepWorkspace bool          // don't clean up workspace on failure (debugging)
}
```

**InputRefs and Artifacts are resolved at submission time.** When the coordinator accepts a task, it resolves all namespace keys in `InputRefs` and `Artifacts` to their current content hashes and stores the hashes on the task. This means:
- Tasks are immune to subsequent namespace key reassignment or deletion
- The exact data referenced at submission time is what the task receives
- Content hashes are immutable, so fetch at execution time is guaranteed to succeed (barring GC, which won't collect referenced objects)

**Resource admission.** When `Resources` is set, the scheduler only assigns the task to workers with sufficient available capacity (`MemPercent` and running task count checked against declared resources). If no worker can satisfy the request, the task stays queued. Resources are best-effort reservations, not hard limits -- the OS enforces actual limits. Phase 2 adds cgroup enforcement.

**Output size limits.** After task completion, the worker checks the total size of `$ZIGGURAT_OUTPUT` before uploading. If it exceeds `MaxOutputSize`, the task is marked FAILED with error `output size exceeded (actual: X, limit: Y)`. The cluster-wide default is set via `compute.max_output_size` (default 1 GB).

### Task Lifecycle

```
submit → QUEUED → SCHEDULED → RUNNING → COMPLETED
                     │            │
                     │            ├── FAILED (retries exhausted → dead letter)
                     │            ├── RETRY → QUEUED (attempt < max)
                     │            └── CANCELLING → CANCELLED
                     │
                     └── CANCELLED (cancelled while queued)
```

Transitions are journaled on the coordinator. On coordinator restart, incomplete tasks are re-queued.

### Task Cancellation

Cancelling a task follows a deterministic sequence:

| Task State | Cancel Action |
|------------|---------------|
| **QUEUED** | Immediately transition to CANCELLED. No process to stop. |
| **SCHEDULED** | Prevent dispatch. Transition to CANCELLED. |
| **RUNNING** | Enter CANCELLING state (see below). |
| **COMPLETED/FAILED/CANCELLED** | No-op. Return current state. |

**CANCELLING a running task:**

1. Coordinator sends cancel signal to the worker executing the task
2. Worker sends `SIGTERM` to the task process (and process group)
3. Grace period begins (default 10s, configurable via `compute.cancel_grace`)
4. If the process exits within the grace period, capture exit code and output
5. If the process does not exit, worker sends `SIGKILL`
6. Workspace is cleaned up per normal rules (`KeepWorkspace` still honored)
7. Task transitions to CANCELLED with `Error: "cancelled by user"` and actual exit code

Tasks that need to handle cancellation gracefully (e.g., checkpoint on shutdown) SHOULD trap `SIGTERM`. This is part of the task contract: Ziggurat sends `SIGTERM` first, `SIGKILL` after grace period.

### Pipelines (Task DAGs)

Research workloads are often multi-stage:

```
[Fetch data] → [Compute predictions] → [Validate against experiment] → [Generate report]
```

Ziggurat supports this via pipelines -- ordered groups of tasks with dependency edges and output forwarding through storage.

```go
type Pipeline struct {
    ID     string
    Name   string
    Stages []Stage
    Status PipelineStatus
}

type Stage struct {
    ID          string
    Command     []string
    Artifacts   []string
    InputRefs   map[string]string   // can use "$<stage_id>.output" syntax
    Params      map[string]string
    Requires    []string
    Constraints []string             // capability constraint expressions
    Image       string
    DependsOn   []string            // stage IDs
    Config      TaskConfig
    TaskID      string              // resolved task ID once scheduled
    Status      TaskStatus
}
```

Stage N writes output to `$ZIGGURAT_OUTPUT`. The coordinator uploads it to storage. Stage N+1 references it via `$<stage_id>.output`, which the coordinator resolves to the actual storage key before scheduling.

```json
{
  "name": "electroweak-fit",
  "stages": [
    {
      "id": "fetch",
      "command": ["python3", "fetch_pdg.py"],
      "artifacts": ["scripts/fetch_pdg.py"],
      "params": {"dataset": "electroweak-2024"}
    },
    {
      "id": "predict",
      "command": ["python3", "rg_evolve.py"],
      "artifacts": ["scripts/rg_evolve.py", "scripts/lib/"],
      "depends_on": ["fetch"],
      "input_refs": {"data": "$fetch.output"},
      "params": {"scale": "mZ", "observables": "sin2tw,mW,mt"}
    },
    {
      "id": "validate",
      "command": ["python3", "chi2_fit.py"],
      "artifacts": ["scripts/chi2_fit.py"],
      "depends_on": ["predict"],
      "input_refs": {
        "predictions": "$predict.output",
        "experimental": "$fetch.output"
      }
    }
  ]
}
```

### Pipeline Failure Policy

When a stage fails (retries exhausted):

1. The stage is marked FAILED
2. All downstream stages (stages that transitively depend on the failed stage) are marked CANCELLED
3. Stages with no dependency on the failed stage continue executing (independent branches)
4. The pipeline status becomes FAILED once all running stages complete or are cancelled

**Pipeline retry**: `ziggurat pipeline retry <id>` re-submits the pipeline starting from the first failed stage. Completed stages are skipped -- their outputs are already in storage. This enables fix-and-resume workflows without re-running expensive upstream computation.

**Pipeline cancel**: `ziggurat pipeline cancel <id>` cancels all QUEUED/RUNNING stages using the task cancellation protocol described above.

### Work Stealing

The coordinator tracks per-worker load via `WorkerLoad` and identifies overloaded workers (`OverloadedWorkers()` — queue depth > 2× median). The dispatch loop periodically calls `stealWork()`, which reassigns queued (not running) tasks from overloaded workers back to the global queue for redistribution to idle workers.

Work stealing respects data locality — tasks are only moved when the load imbalance is severe enough to justify remote execution.

---

## Discovery and Membership

### LAN Discovery (mDNS)

On startup, a node announces itself via mDNS service `_ziggurat._tcp.local`. Other nodes discover it automatically.

```
Node A starts → announces _ziggurat._tcp.local
Node B starts → discovers Node A via mDNS → joins cluster
Node C starts → discovers Node A or B → joins cluster
```

Zero configuration for LAN. Works on any network that allows multicast.

### Seed-Based Join (Current)

Nodes join an existing cluster by specifying any member's gossip address:

```bash
ziggurat start --join 10.0.0.5:7102
```

Or via config:

```yaml
cluster:
  seeds: [10.0.0.5:7102]
```

Seed nodes are used for initial contact only. After joining, gossip handles membership.

### WAN / Cross-Network

For nodes outside the LAN broadcast domain, use multiple seeds:

```yaml
cluster:
  seeds:
    - 10.0.0.5:7102       # office server
    - vpn.example.com:7102 # cloud VM
```

### Gossip Protocol (SWIM)

After initial discovery (mDNS or seeds), membership is maintained via SWIM:

- **Heartbeat**: Periodic pings between random node pairs
- **Suspicion**: Missed heartbeats trigger indirect probes via third-party nodes
- **Failure**: Confirmed failures after suspicion timeout → node removed from membership
- **Protocol period**: Configurable (default 1s for LAN, 5s for WAN)

Using hashicorp/memberlist, which implements SWIM with extensions.

---

## Configuration

### Cluster Config (`ziggurat.yaml`)

```yaml
# Node identity
node:
  name: ""               # auto-generated from hostname if empty
  tags:                   # presence flags for tag-based scheduling
    - gpu
    - high-memory
  capabilities:            # typed key-value pairs (override auto-detected values)
    python.version: "3.12"
    cuda.version: "12.4"
    # Auto-detected (no config needed): os, arch, cpu.cores, mem.total,
    # disk.avail, hostname, gpu.count, gpu.model, gpu.vram, gpu.driver
  data_dir: ~/.ziggurat    # storage, metadata, journals

# Network
network:
  bind: 0.0.0.0
  http_port: 7100         # HTTP API (client-facing)
  grpc_port: 7101         # gRPC (inter-node transport)
  gossip_port: 7102       # memberlist gossip (SWIM protocol)
  advertise: ""           # auto-detected, override for NAT/VPN

# Client connection (for CLI commands and library usage)
client:
  addr: ""               # coordinator address (flag: --addr, env: ZIGGURAT_ADDR)
                         # empty = auto-discover via mDNS

# Cluster formation
cluster:
  discovery: auto         # auto | mdns | seeds | static
  seeds: []               # explicit seed addresses for WAN
  name: default           # cluster name (nodes only join matching clusters)

# Storage
storage:
  data_dir: ""            # defaults to <node.data_dir>/store
  capacity: 0             # 0 = use all available space
  replication_factor: 2
  erasure:
    enabled: true
    data_shards: 4
    parity_shards: 2
  tier_thresholds:
    medium: 1MB
    large: 64MB
  gc_grace_period: 1h

# Compute
compute:
  concurrency: 0          # 0 = auto (GOMAXPROCS)
  memory_limit: 0         # 0 = no limit
  task_timeout: 5m        # default per-task timeout
  max_output_size: 1GB    # default per-task output limit
  cancel_grace: 10s       # SIGTERM → SIGKILL grace period
  workspace_dir: ""       # defaults to <os.TempDir()>/ziggurat
  max_retained_workspaces: 20  # FIFO eviction for --keep-workspace

# Fault tolerance
resilience:
  mode: balanced          # fast | balanced | resilient
  heartbeat_interval: 1s
  suspicion_timeout: 5s
  task_retries: 2
  dead_letter: true       # store failed tasks for inspection

# Observability
metrics:
  enabled: true           # served at /metrics on main port
```

### Resilience Modes (Presets)

| Mode | Replication | Retries | Heartbeat | Behaviour |
|------|-------------|---------|-----------|-----------|
| **fast** | 1 (none) | 0 | 5s | Max throughput, tasks lost on node failure |
| **balanced** | 2 | 2 | 1s | Good default. Survives single node loss |
| **resilient** | 3 | 3 | 500ms | Max safety. Survives 2 concurrent failures |

Individual settings override mode presets.

---

## CLI Interface

Commands are annotated with their implementation phase. Phase 0a commands are
available now; later phases are planned.

```bash
# ── Cluster Lifecycle ──────────────────────────────

ziggurat start                       # Start coordinator+worker (hybrid)     [0a]
ziggurat start --role coordinator    # Coordinator only, no compute          [0a]
ziggurat start --role worker --join 10.0.0.5:7102  # Worker only             [0a]
ziggurat start --join auto           # Join via mDNS                         [0b]
ziggurat start --join 10.0.0.5:7102  # Join via explicit address             [0a]

ziggurat stop                        # Graceful shutdown                     [0b]
ziggurat drain                       # Stop accepting work, finish current   [0b]

# ── Cluster Status ─────────────────────────────────

ziggurat status                      # Cluster overview                      [0a]
ziggurat nodes                       # List nodes with load, storage, tags   [0b]

# ── Task Submission ────────────────────────────────

ziggurat run <command...> [flags]    # Submit a task (exec model)            [0a]
  --input <name>=<store-key>        # Named input references
  --artifact <store-key>            # Fetch into workspace root
  --param <key>=<value>             # Injected as ZIGGURAT_PARAM_<KEY>
  --require <tag>                   # Required worker tag
  --constraint <expr>               # Capability constraint
  --image <ref>                     # OCI image (optional)
  --priority <n>                    # Priority (default 0)
  --timeout <duration>              # Per-attempt timeout
  --retries <n>                     # Max retry count
  --memory <size>                   # Memory requirement
  --cpus <n>                        # CPU cores requirement
  --gpus <n>                        # GPU devices required (sets CUDA_VISIBLE_DEVICES)
  --max-output <size>               # Output size limit
  --wait                            # Block until complete
  --keep-workspace                  # Don't clean up on failure
  --env <name>                     # Persistent environment name
  --env-setup <cmd>                # Setup command (run in shell)
  --env-fingerprint <file>         # File whose content determines env staleness

# Examples:
ziggurat run python3 rg_evolve.py \
  --artifact scripts/rg_evolve.py \
  --input coefficients=datasets/fhuft-v3 \
  --param scale=mZ --param observables=sin2tw,mW \
  --require python3 --require numpy \
  --wait

ziggurat run ./compute_cross_section --input data=nuclear/endf-b8 \
  --artifact bin/compute_cross_section \
  --timeout 30m

ziggurat batch --from tasks.yaml     # Submit batch from YAML file           [1]

ziggurat pipeline submit <file>      # Submit a pipeline definition          [1]
ziggurat pipeline status <id>        # Pipeline status + stage results       [1]
ziggurat pipeline cancel <id>        # Cancel pipeline                       [1]
ziggurat pipeline retry <id>         # Retry from first failed stage         [1]

# ── Task Management ────────────────────────────────

ziggurat tasks                       # List tasks (filterable by status)     [0a]
ziggurat task <id>                   # Task detail + result + stdout/stderr  [0a]
ziggurat cancel <id>                 # Cancel a queued/running task          [0a]
ziggurat wait <id>                   # Block until task completes            [0a]
ziggurat retry <id>                  # Re-submit a failed task               [0b]
ziggurat dead-letter                 # List dead-lettered tasks              [1]
ziggurat logs <id>                   # Stream stdout/stderr from task        [1]

# ── Storage ────────────────────────────────────────

ziggurat put <namespace-key> <file>  # Upload object (file or directory)     [0a]
ziggurat get <namespace-key> [dest]  # Download object (default: stdout)     [0a]
ziggurat ls [prefix]                 # List objects                          [0a]
ziggurat rm <namespace-key>          # Delete object                         [0a]
ziggurat pin <namespace-key>         # Prevent GC                            [0b]
ziggurat unpin <namespace-key>       # Allow GC                              [0b]
ziggurat store status                # Storage utilization                   [0b]
ziggurat store rebalance             # Trigger shard rebalancing             [1]

# ── Environments ──────────────────────────────────

ziggurat env list                    # List persistent envs on this node     [1.5]
ziggurat env prune                   # Remove stale envs                     [1.5]
  --max-age <duration>              # Override configured env_max_age

# ── Interactive ────────────────────────────────────

ziggurat shell                       # Interactive REPL (ls/put/get/rm/run)  [2]
ziggurat mount <mountpoint>          # FUSE mount of cluster storage         [2]
  # Ctrl+C or fusermount -u to unmount

# ── Diagnostics ────────────────────────────────────

ziggurat health                      # Health check (JSON)                   [0a]
ziggurat metrics                     # Key metrics summary                   [1]
ziggurat top                         # Live dashboard (nodes, tasks, load)   [1.5]
  --interval, -n <dur>              # Refresh interval (default 2s)
  --once                            # Print once and exit
ziggurat benchmark                   # CPU, memory, disk, GPU, peer latency  [1.5]
  --skip-network                    # Skip peer latency probes
ziggurat version                     # Binary version + build info           [0a]
```

### Client API (HTTP)

```
# Tasks                                                            Phase
POST   /api/v1/tasks                Submit task                     [0a]
GET    /api/v1/tasks                List tasks                      [0a]
GET    /api/v1/tasks/:id            Get task detail                 [0a]
DELETE /api/v1/tasks/:id            Cancel task                     [0a]
POST   /api/v1/tasks/:id/wait       Long-poll until complete        [0a]
GET    /api/v1/tasks/:id/logs       Stream stdout/stderr (SSE)      [1]

# Pipelines
POST   /api/v1/pipelines            Submit pipeline                 [1]
GET    /api/v1/pipelines/:id        Pipeline status + stages        [1]
POST   /api/v1/pipelines/:id/retry  Retry from failed stage         [1]
DELETE /api/v1/pipelines/:id        Cancel pipeline                 [1]

# Storage
PUT    /api/v1/store/*key           Upload object                   [0a]
GET    /api/v1/store/*key           Download object                 [0a]
DELETE /api/v1/store/*key           Delete object                   [0a]
GET    /api/v1/store/*              List objects (?prefix=...)       [0a]
POST   /api/v1/store/*key/pin       Pin object                      [0b]
DELETE /api/v1/store/*key/pin       Unpin object                    [0b]

# Cluster
GET    /api/v1/cluster              Cluster status                  [0a]
GET    /api/v1/cluster/nodes        Node list with load + tags      [0b]
GET    /api/v1/health               Health check                    [0a]
GET    /metrics                     Prometheus metrics               [1]
```

---

## Client Libraries

### Go (importable package)

```go
client, _ := ziggurat.Connect("localhost:7100")

// Upload script and data
client.Store.PutFile(ctx, "scripts/rg_evolve.py", "./rg_evolve.py")
client.Store.PutFile(ctx, "datasets/planck-2018", "./planck_2018.tar.gz")

// Submit task -- arbitrary command with storage references
task, _ := client.Run(ctx, &ziggurat.TaskOpts{
    Command:   []string{"python3", "rg_evolve.py"},
    Artifacts: []string{"scripts/rg_evolve.py"},
    Inputs: map[string]string{
        "coefficients": "datasets/planck-2018",
    },
    Params: map[string]string{
        "scale":       "mZ",
        "observables": "sin2tw,mW",
    },
    Requires: []string{"python3", "numpy"},
    Timeout:  10 * time.Minute,
})

// Wait for result
result, _ := client.Wait(ctx, task.ID)

// Retrieve output from storage
data, _ := client.Store.Get(ctx, result.OutputRef)

// Or inspect stdout/stderr
fmt.Println(result.Stdout)
fmt.Println("Exit code:", result.ExitCode)
```

### Python (HTTP wrapper)

```python
from ziggurat import Client

client = Client("http://localhost:7100")

# Upload
client.store.put("scripts/rg_evolve.py", open("rg_evolve.py", "rb"))
client.store.put("datasets/planck-2018", open("planck_2018.tar.gz", "rb"))

# Submit
task = client.run(
    command=["python3", "rg_evolve.py"],
    artifacts=["scripts/rg_evolve.py"],
    inputs={"coefficients": "datasets/planck-2018"},
    params={"scale": "mZ", "observables": "sin2tw,mW"},
    requires=["python3", "numpy"],
    timeout=600,
)

# Wait
result = client.wait(task.id)
output = client.store.get(result.output_ref)
print(result.stdout)
```

---

## Data Structures

### Core Types

```go
// ── Node ──────────────────────────────────────────

type Node struct {
    ID           string              // UUID
    Name         string              // human-friendly (hostname)
    Address      string              // ip:port
    Role         Role                // coordinator | worker | hybrid
    Tags         []string            // presence flags (python3, cuda, numpy, etc.)
    Capabilities map[string]string   // typed key-value pairs (auto-detected + configured)
    Load         LoadInfo
    Storage      StorageInfo
    JoinedAt     time.Time
    LastSeen     time.Time
}

type LoadInfo struct {
    CPUPercent    float64
    MemPercent    float64
    TasksRunning  int
    TasksQueued   int
    Concurrency   int     // max concurrent tasks
}

type StorageInfo struct {
    Capacity    int64  // bytes
    Used        int64
    Objects     int
    Shards      int
}

// ── Task ──────────────────────────────────────────
// See Compute Layer section for full Task, TaskConfig, ResourceReq,
// Pipeline, and Stage struct definitions. Those are authoritative.

type TaskStatus int
const (
    TaskQueued     TaskStatus = iota
    TaskScheduled
    TaskRunning
    TaskCompleted
    TaskFailed
    TaskCancelling
    TaskCancelled
    TaskDeadLetter  // retries exhausted, held for inspection
)

type TaskMetrics struct {
    QueuedAt    time.Time     // when task entered queue
    StartedAt   time.Time     // when execution began
    CompletedAt time.Time     // when execution ended (success or failure)
    WallTime    time.Duration // CompletedAt - StartedAt
    OutputBytes int64         // total bytes written to $ZIGGURAT_OUTPUT
}

type PipelineStatus string
const (
    PipelineRunning   PipelineStatus = "running"
    PipelineCompleted PipelineStatus = "completed"
    PipelineFailed    PipelineStatus = "failed"
    PipelineCancelled PipelineStatus = "cancelled"
)

// ── Storage ───────────────────────────────────────

type ObjectMeta struct {
    Hash           [32]byte             // BLAKE3
    Size           int64
    Tier           Tier                 // small | medium | large
    Strategy       StorageStrategy      // replicated | erasure_coded
    Shards         []ShardPlacement
    Erasure        *ErasureParams       // set when Strategy == ErasureCoded
    Namespace      string               // human-friendly key
    Pinned         bool
    RefCount       int32
    Tags           map[string]string
    CreatedAt      time.Time
    UnreferencedAt time.Time            // when refcount last reached 0; starts GC grace period
}

type ErasureParams struct {
    DataShards   int       // k: minimum shards to reconstruct
    ParityShards int       // m: redundancy shards
    OriginalSize int64     // original object size before splitting
    ShardSize    int64     // per-shard size
    ShardHashes  []string  // hex-encoded BLAKE3 per shard (len = k+m)
    ShardNodes   []string  // nodeID per shard index; set by origin during distribution
}

type ShardPlacement struct {
    Index  int      // shard index (0 for replicated copies)
    NodeID string   // which node holds this shard
    Verified time.Time // last integrity check
}

type Tier int
const (
    TierSmall  Tier = iota  // < 1 MB: full replication
    TierMedium              // 1-64 MB: full replication
    TierLarge               // > 64 MB: erasure coding
)

type StorageStrategy int
const (
    Replicated   StorageStrategy = iota
    ErasureCoded
)

type StorageClass string
const (
    StorageClassNVMe StorageClass = "nvme"
    StorageClassSSD  StorageClass = "ssd"
    StorageClassHDD  StorageClass = "hdd"
    StorageClassS3   StorageClass = "s3"
)
```

---

## Failure Modes and Recovery

| Failure | Detection | Recovery |
|---------|-----------|----------|
| **Worker dies mid-task** | Heartbeat timeout | Coordinator re-queues task to another worker |
| **Worker dies with storage shards** | Heartbeat timeout | Coordinator triggers re-replication from surviving copies/parity |
| **Coordinator dies (Phase 0/1)** | Workers detect via SWIM | **Cluster stops accepting new work.** Running tasks complete but results cannot be recorded. Storage reads from local shards continue. No new scheduling, no new storage writes. On coordinator restart, task journal is replayed and incomplete tasks re-queued. Data stored on workers is not lost. |
| **Coordinator dies (Phase 2)** | SWIM + Raft heartbeat | Standby promotes via Raft. Brief scheduling pause during leader election (~seconds). |
| **Network partition** | Split membership views | Side with coordinator continues. Orphaned workers pause task acceptance and reconnect when healed. Running tasks on orphaned workers complete but cannot report results -- coordinator re-queues them. |
| **Task process crashes** | Non-zero exit code | Task marked failed, retried if attempts remain |
| **Task timeout** | Per-task timer | SIGTERM → grace period → SIGKILL. Retried or dead-lettered |
| **Task cancelled** | Client request | SIGTERM → grace period → SIGKILL. Task marked CANCELLED |
| **Storage corruption** | BLAKE3 verification on read | Re-fetch shard from replica/reconstruct from parity |
| **Disk full** | Capacity monitoring | Node stops accepting new shards, coordinator routes elsewhere |
| **Slow worker** | Speculative execution (Phase 2) | Duplicate task on faster worker, first result wins |

### Storage Repair

Background process on coordinator periodically:
1. Checks every object's shard placement against desired replication/erasure level
2. If any shards are under-replicated (node left cluster), triggers repair
3. Repair reads surviving shards, reconstructs missing ones, places on healthy nodes
4. Integrity scan: randomly sample objects, verify BLAKE3 hashes

---

## Observability

### Prometheus Metrics

```
# Compute
ziggurat_tasks_submitted_total
ziggurat_tasks_completed_total{status}
ziggurat_task_duration_seconds             # histogram
ziggurat_task_queue_depth{node}
ziggurat_workers_active{node}

# Storage
ziggurat_store_objects_total
ziggurat_store_bytes_total
ziggurat_store_shards_total{tier, strategy}
ziggurat_store_reads_total{tier}
ziggurat_store_writes_total{tier}
ziggurat_store_repair_total{reason}

# Cluster
ziggurat_nodes_total{role}
ziggurat_heartbeat_rtt_seconds{node}     # histogram
ziggurat_network_bytes_total{direction}
```

### Health Endpoint

```json
GET /api/v1/health

{
  "status": "healthy",
  "nodes": 4,
  "nodes_healthy": 4,
  "tasks_running": 12,
  "tasks_queued": 3,
  "storage_used_bytes": 1073741824,
  "storage_capacity_bytes": 10737418240,
  "objects_under_replicated": 0,
  "uptime_seconds": 86400
}
```

---

## Security Model (Phase 1+)

MVP assumes trusted LAN. Phase 1 adds:

- **mTLS**: All inter-node gRPC uses mutual TLS. Coordinator acts as CA.
- **Join tokens**: New nodes must present a token to join the cluster.
- **API auth**: HTTP API requires bearer token (static secret or JWT).
- **Encryption at rest**: Storage shards encrypted with node-local key.

```bash
# Generate cluster token
ziggurat token generate > cluster.token

# Node joins with token
ziggurat start --join auto --token $(cat cluster.token)
```

---

## Technology Choices

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Language | Go 1.23+ | Single binary, excellent networking, goroutines, slog |
| Inter-node RPC | gRPC (google.golang.org/grpc) | Efficient, streaming, bidirectional |
| Discovery | hashicorp/mdns | Zero-config LAN, proven |
| Membership | hashicorp/memberlist (SWIM) | Battle-tested, LAN + WAN |
| Erasure coding | klauspost/reedsolomon | Pure Go, fast, well-maintained |
| Hashing | zeebo/blake3 | Fast, cryptographic, SIMD-accelerated |
| HTTP | net/http + chi | Consistent with Syzygy stack |
| Serialization | protobuf (gRPC native) | Efficient, typed |
| Metrics | prometheus/client_golang | Standard |
| Config | YAML (gopkg.in/yaml.v3) | Consistent with Syzygy tools |
| CLI | cobra | Consistent with Cardinal, Anza, Guillotine |
| Metadata DB | go.etcd.io/bbolt | Embedded, crash-safe, zero dependencies |
| Logging | log/slog (stdlib) | Structured, leveled, zero dependencies |
| UUID | github.com/google/uuid | Consistent with Cardinal |

---

## Syzygy Integration

### Research Plane

Ziggurat is the compute backend for the Research Plane. Research scripts and binaries live in Ziggurat storage; tasks reference them as artifacts:

```
Prediction Engine:
  ziggurat run python3 rg_evolve.py --artifact scripts/rg_evolve.py \
    --input coefficients=datasets/fhuft-v3 --param scale=mZ
  ziggurat run ./class --artifact bin/class --input ini=configs/fhuft-ede.ini

Validation Framework:
  ziggurat run python3 chi2_fit.py --artifact scripts/chi2_fit.py \
    --input predictions=results/ew-predictions --input data=datasets/pdg-2024

IG Toolkit:
  ziggurat run python3 geodesic_opt.py --artifact scripts/geodesic_opt.py \
    --input manifold=datasets/fisher-manifold --require cuda
```

The Prediction Engine can submit entire pipeline definitions for multi-stage workloads (fetch data → compute → validate → report) as a single pipeline YAML.

### Development Plane

Optional integration for parallelizable dev tasks:

```
Guillotine: Distribute benchmark runs across machines
  ziggurat run guillotine run --provider anthropic --suite essentials \
    --artifact bin/guillotine

Cardinal: Parallel Avril sessions on different nodes (future)
```

### Runtime Stack

Halcyon applications can offload expensive compute to Ziggurat:

```go
func handleSimulation(w http.ResponseWriter, r *http.Request) {
    ref, _ := zigguratClient.Store.Put(ctx, "sim/input/"+reqID, inputData)
    task, _ := zigguratClient.Run(ctx, &ziggurat.TaskOpts{
        Command:   []string{"python3", "simulate.py"},
        Artifacts: []string{"scripts/simulate.py"},
        Inputs:    map[string]string{"data": ref},
    })
    halcyon.OK(w, map[string]string{"task_id": task.ID})
}
```

---

## On-Disk Persistence

All persistent state lives under `node.data_dir` (default `~/.ziggurat`):

```
~/.ziggurat/
├── ziggurat.yaml              # config (if not specified via --config)
├── node.id                   # stable node UUID (generated on first start)
├── metadata/
│   ├── objects.db            # object + namespace metadata (BoltDB, buckets: objects, namespaces)
│   └── tasks.db              # task state snapshots (BoltDB, bucket: tasks)
├── store/
│   └── <hash-prefix>/<hash>  # object shards on disk (2-char prefix for directory fan-out)
├── envs/                     # persistent task environments (venvs, node_modules, etc.)
│   └── <name>/               # one directory per environment (named or fingerprinted)
└── workspaces/               # retained workspaces (--keep-workspace)
```

**BoltDB** (go.etcd.io/bbolt) for all metadata: single-file, embedded, crash-safe B+ tree. No external process.

- **objects.db** contains two buckets: `objects` (content hash → ObjectMeta JSON) and `namespaces` (namespace key → content hash). Both live in one file because BoltDB transactions are per-database, and namespace resolution always accompanies object lookups.
- **tasks.db** contains the `tasks` bucket (task ID → Task JSON snapshot). Every state transition writes the full task snapshot. On coordinator restart, all tasks are loaded and in-progress tasks (queued, scheduled, running, cancelling) are re-enqueued.

**Why BoltDB snapshots, not a WAL**: BoltDB provides crash-safe transactional writes — each `Save()` is an atomic B+ tree update backed by `fdatasync`. This gives the same durability guarantee as a WAL with simpler code and no checkpoint/truncation logic. The tradeoff is write amplification (full task JSON per transition vs. delta), which is acceptable at Phase 0a task volumes. A WAL may be introduced in Phase 0b if write throughput becomes a bottleneck under distributed scheduling.

**Schema versioning**: The metadata DB includes a `schema_version` key (see `dbutil.CheckSchema`). On startup, the node checks the version and runs migrations if needed. Unknown future versions cause the node to refuse to start (preventing corruption from downgrade).

**Object shards** are stored as flat files named by content hash, grouped into directories by the first two hex characters of the hash (e.g., `store/a3/a3f2b1...`). This limits directory entries to ~256 directories × N files, keeping filesystem operations fast.

---

## Scaling Envelope

Ziggurat is designed for small-to-medium research clusters, not datacenter-scale infrastructure.

| Dimension | Target | Notes |
|-----------|--------|-------|
| Nodes | 2-50 | Full metadata replication to all nodes |
| Objects | up to ~1M | Metadata fits in RAM (~1 KB/object) |
| Object size | bytes to ~10 GB | Streaming I/O for large objects |
| Concurrent tasks | hundreds | Bounded by worker count × concurrency |
| Task throughput | ~100/s sustained | Coordinator scheduling + journaling bottleneck |

Beyond this envelope, the architecture would need metadata sharding, multiple coordinators, and hierarchical scheduling. That's a different tool.

---

## Implementation Phases

### Phase 0a: Single Node

Ziggurat running on one machine — useful standalone before any cluster features. Validates the exec engine, storage, and API in isolation.

- [x] Go module scaffolding, CLI skeleton (cobra)
- [x] Node startup, config loading (YAML), node ID persistence
- [x] Storage: content-addressed object store (BLAKE3, local disk, BoltDB metadata)
- [x] Storage: namespace keys → content hash resolution
- [x] Storage: directory upload/download as deterministic tar
- [x] Storage: basic GC (refcount + grace period)
- [x] Worker: process execution engine (workspace setup, env injection, subprocess, capture)
- [x] Worker: task cancellation (SIGTERM → grace → SIGKILL)
- [x] Worker: output size limit enforcement
- [x] Worker: env var protection (ZIGGURAT_* non-overridable)
- [x] Node: auto-detect capabilities (os, arch, cpu, mem, disk; GPU if present)
- [x] Node: merge auto-detected + configured capabilities at startup
- [x] Coordinator: task queue, priority ordering, tag-matching scheduling
- [x] Coordinator: constraint expression evaluation against node capabilities
- [x] Coordinator: task state persistence (BoltDB snapshots + replay on restart)
- [x] Coordinator: InputRef/Artifact resolution to content hashes at submission
- [x] HTTP API: run, poll, wait, cancel, store put/get/delete/list
- [x] CLI: start, run, tasks, task, cancel, status, put, get, ls, rm
- [x] Stdout/stderr capture and retrieval

### Phase 0b: Cluster

Multi-node mesh. Builds on 0a by adding discovery, gossip, replication, and distributed scheduling.

- [x] Memberlist gossip (node discovery, metadata broadcast, join/leave)
- [x] Node registry (track live nodes, capabilities, tags via gossip events)
- [x] CLI: start --join, nodes, node, drain
- [x] API: /nodes, /nodes/{id}, /drain endpoints
- [x] Coordinator drain mode (pause dequeue, in-flight tasks complete)
- [x] gRPC inter-node transport (DispatchTask, TaskResult, PullShard/PushShard streaming)
- [x] Storage: full replication for all objects via Replicator.AfterPut + gRPC PushShard
- [x] Storage: metadata replication across nodes (shard placements tracked in ObjectMeta, NodesForHash for locality)
- [x] Storage: degraded replication when nodes < replication_factor (under-replicated queue + Repair)
- [x] Distributed scheduling: locality scoring + load balancing (scheduler.Score/Select with ObjectLocator + NodeLoad interfaces)
- [x] Heartbeat failure detection + task re-queue + storage repair (gossip NotifyLeave → Registry.OnLeave → RequeueByWorker + RemoveNodePlacements + TriggerRepair)

### Phase 1: Production

- [x] Erasure coding (Reed-Solomon) for large objects
- [x] Storage repair (background re-replication + integrity scanning)
- [x] Shard rebalancing on node join/leave
- [x] Drain with shard migration
- [x] Work stealing from overloaded workers
- [x] Pipelines (task DAGs with output forwarding + retry from failed stage)
- [x] Prometheus metrics (served at /metrics on main port)
- [x] Batch submission
- [x] Dead letter queue
- [x] Resource-aware scheduling (memory/CPU/GPU admission)
- [x] Streaming I/O for large objects (io.Reader/io.Writer, not []byte)
- [x] mDNS auto-discovery (`_ziggurat._tcp.local`)
- [x] Remote cancel propagation via gRPC
- [x] Schema versioning for BoltDB
- [x] Integration test harness
- [x] Task log streaming (SSE)
- [x] Persistent environments (fingerprint-based reuse)
- [ ] Container execution (OCI image support via Podman)
- [ ] mTLS + join tokens

### Phase 2: Advanced

- [ ] Coordinator failover (Raft via hashicorp/raft)
- [ ] Speculative execution for slow tasks
- [ ] Cross-cluster federation (connect multiple Ziggurat clusters)
- [ ] Python client library (pip installable)
- [ ] Encryption at rest
- [ ] Resource limits enforcement (cgroups: CPU, memory, disk I/O per task)

---

## Design Decisions (Resolved)

1. **Port separation**: HTTP API on 7100 (client-facing), gRPC on 7101 (inter-node). cmux was considered but has unresolved bugs with graceful shutdown, TLS, and HTTP/2 negotiation. Two ports costs one extra firewall rule but gives independent lifecycle management, native TLS support, and cleaner debugging. Phase 0a only needs the HTTP port.

2. **Large object streaming**: `io.Reader`/`io.Writer` interfaces throughout the storage layer — mandatory from Phase 1. Phase 0 may buffer small/medium objects in memory but MUST NOT assume objects fit in RAM in the API contract. Task outputs are uploaded as deterministic tar archives (see Directory Storage Model).

3. **Coordinator placement**: Dev machine is coordinator by default (first node). Any node can be designated coordinator via `--role coordinator`. No automatic promotion until Phase 2 (Raft).

4. **Storage consistency**: Strong consistency for both replicated and erasure-coded objects — `PUT` returns only after all replicas/shards are distributed. For EC objects, all `k+m` shards are pushed to ring-determined nodes synchronously before the call returns.

5. **Output format**: Deterministic tar archive. Stored as a single content-addressed object. Consumers extract what they need. Avoids metadata explosion and transactional multi-object uploads.

6. **Workspace retention**: Per-task flag (`--keep-workspace`) plus cluster-wide limit (`compute.max_retained_workspaces`, default 20). FIFO eviction when limit reached. Only failed/cancelled task workspaces are retained; successful tasks always clean up.

7. **Name**: Ziggurat. Ships as `ziggurat`.
