# Ziggurat CLI UX Design

## Design Principles

Drawn from the Anza control-plane pattern, adapted for a networked compute mesh:

1. **Status is the front door.** `ziggurat status` is the first thing you run. It answers "what's happening in my cluster?" in one screen.
2. **Summary first, drill to detail.** Every list command shows a scannable overview. Pass an ID or name to get the full picture.
3. **`--json` everywhere.** Every command that produces output supports `--json` for scripting and piping.
4. **`--node` for targeting.** Like Anza's `--project`, any command that can be scoped to a single node accepts `--node <name-or-id>`.
5. **Human output is opinionated.** Columns are aligned, status is color-coded (when TTY), output fits 80 columns. JSON output is the escape hatch for custom formatting.
6. **One binary, two roles.** `ziggurat start` runs a node. Every other command is a client that talks to the cluster over HTTP. No separate "admin" binary.

---

## Command Map

```
ziggurat
 |
 |-- Cluster Lifecycle
 |   init                   Generate default config (~/.ziggurat/ziggurat.yaml)
 |   start                  Start this node (server mode)
 |   stop                   Graceful shutdown                    [PLANNED]
 |   drain                  Stop accepting work, finish in-flight tasks
 |   resume                 Resume task dequeuing after drain
 |
 |-- Cluster Status
 |   status                 Cluster dashboard (the hero command)
 |   nodes                  List nodes with health, load, capabilities
 |   node <id>              Detail view for a single node
 |   top                    Live-updating cluster view (--once for snapshot)
 |
 |-- Task Submission
 |   run <cmd...>           Submit a task
 |   batch --from <file>    Submit batch from YAML
 |
 |-- Task Management
 |   tasks                  List tasks (filterable)
 |   task <id>              Task detail + stdout/stderr
 |   cancel <id>            Cancel a task
 |   wait <id>              Block until task completes
 |   dead-letter            List dead-lettered tasks (retries exhausted)
 |   retry <id>             Re-submit a failed task              [PLANNED]
 |   logs <id>              Stream stdout/stderr (SSE)
 |
 |-- Pipeline Management
 |   pipeline submit <file> Submit a pipeline definition
 |   pipeline status <id>   Pipeline status + stage results
 |   pipeline cancel <id>   Cancel all stages
 |   pipeline retry <id>    Retry from first failed stage
 |
 |-- Environments
 |   env list               List persistent environments on this node
 |   env prune              Remove stale environments
 |
 |-- Storage
 |   put <key> <file>       Upload object
 |   get <key> [dest]       Download object
 |   ls [prefix]            List objects
 |   rm <key>               Delete object
 |   pin <key>              Prevent GC                           [PLANNED]
 |   unpin <key>            Allow GC                             [PLANNED]
 |   store status           Storage utilization across cluster   [PLANNED]
 |
 |-- Interactive
 |   shell                  Interactive REPL for cluster operations
 |   mount <path>           Mount store as FUSE filesystem (Linux)
 |
 |-- Diagnostics
 |   health                 Cluster health check (API only: /api/v1/health)
 |   version                Binary version + build info
 |   benchmark              CPU, memory, disk, GPU, and peer latency benchmarks
```

---

## Hero Command: `ziggurat status`

The most important command. One screen, full picture. Modeled after `anza status`:
grouped by significance, most important information first.

### Cluster Overview (no arguments)

```
$ ziggurat status

Cluster: default    Status: healthy    Uptime: 3d 14h
Nodes: 4/4 healthy  Tasks: 7 running, 3 queued  Store: 42.1 GB / 200 GB

Nodes:
  NAME             STATUS    TASKS   CPU    MEM    STORE      TAGS
  gpu-workstation  healthy   3/8     62%    41%    18.2 GB    gpu, cuda, python3
  lab-nuc-01       healthy   2/4     88%    73%    8.7 GB     python3, numpy
  lab-nuc-02       healthy   2/4     45%    38%    8.4 GB     python3, numpy
  cloud-vm-1       healthy   0/2     3%     12%    6.8 GB     python3

Active Tasks:
  ID          STATUS     NODE             COMMAND                   WALL
  a3f2b1c8    running    gpu-workstation  python3 train_model.py    14m32s
  d7e4f012    running    gpu-workstation  python3 rg_evolve.py      8m11s
  ...5 more running

Queue: 3 tasks waiting (highest priority: 10, oldest: 2m ago)
```

### Single-Node Detail (`ziggurat status <node>`)

```
$ ziggurat status gpu-workstation

gpu-workstation
  id:           c8a3f2b1-d7e4-4f01-b234-567890abcdef
  address:      10.0.0.5:7100
  status:       healthy
  uptime:       3d 14h
  joined:       2026-04-21 09:12:03

  load:
    cpu:        62%
    memory:     41% (55.2 GB / 134.2 GB)
    tasks:      3/8 running

  storage:
    used:       18.2 GB / 50 GB
    objects:    1,247
    shards:     3,891

  capabilities:
    os:             linux
    arch:           amd64
    cpu.cores:      32
    mem.total:      137438953472
    gpu.count:      4
    gpu.model:      NVIDIA A100
    gpu.vram:       343597383680
    gpu.cuda:       12.4
    gpu.driver:     550.54.15
    python.version: 3.12

  tags: gpu, cuda, python3, numpy

  running tasks:
    a3f2b1c8  python3 train_model.py   14m32s  priority=10
    d7e4f012  python3 rg_evolve.py      8m11s  priority=5
    f0123456  ./compute_xs              2m44s  priority=0
```

### JSON Output

```
$ ziggurat status --json
{
  "cluster": "default",
  "status": "healthy",
  "uptime_seconds": 309600,
  "nodes": [ ... ],
  "tasks_running": 7,
  "tasks_queued": 3,
  "storage_used_bytes": 45208989696,
  "storage_capacity": 214748364800
}
```

---

## Node Commands

### `ziggurat nodes`

Tabular overview. Default sort: by task load (busiest first).

```
$ ziggurat nodes

NAME             STATUS    ROLE      TASKS   CPU    MEM    STORE      JOINED       TAGS
gpu-workstation  healthy   hybrid    3/8     62%    41%    18.2 GB    3d ago       gpu, cuda, python3, numpy
lab-nuc-01       healthy   hybrid    2/4     88%    73%    8.7 GB     3d ago       python3, numpy
lab-nuc-02       healthy   hybrid    2/4     45%    38%    8.4 GB     3d ago       python3, numpy
cloud-vm-1       healthy   hybrid    0/2     3%     12%    6.8 GB     1d ago       python3
```

Filterable:

```
ziggurat nodes --tag gpu              # Only nodes with gpu tag             [PLANNED]
ziggurat nodes --status healthy       # Only healthy nodes                  [PLANNED]
ziggurat nodes --idle                 # Only nodes with 0 running tasks     [PLANNED]
ziggurat nodes --capable "gpu.vram >= 16GB"  # Nodes matching a constraint  [PLANNED]
```

### `ziggurat node <name-or-id>`

Alias for `ziggurat status <name-or-id>`. Same detail view.

### `ziggurat drain <name-or-id>`

```
$ ziggurat drain lab-nuc-01
draining lab-nuc-01: 2 tasks in-flight, waiting...
lab-nuc-01: drained (0 tasks remaining)

$ ziggurat drain lab-nuc-01 --force
draining lab-nuc-01: cancelling 2 running tasks...
lab-nuc-01: drained
```

---

## Task Commands

### `ziggurat tasks`

Default: most recent first. Truncated to terminal height.

```
$ ziggurat tasks

ID          STATUS      NODE        COMMAND               WALL       EXIT
a3f2b1c8    running     gpu-works   python3 ...           14m32s     0
d7e4f012    running     gpu-works   python3 ...            8m11s     0
f0123456    running     lab-nuc-0   ./compute_xs           2m44s     0
b2c3d4e5    queued      --          python3 ...            --        0
e5f67890    completed   lab-nuc-0   python3 ...            4m18s     0
12345678    failed      lab-nuc-0   ./broken_script        0s        1
```

Filterable:

```
ziggurat tasks --status running         # Only running
ziggurat tasks --status queued          # Only queued
ziggurat tasks --status failed          # Only failed
ziggurat tasks --node gpu-workstation   # Only tasks on this node             [PLANNED]
ziggurat tasks --all                    # Include completed/failed/cancelled  [PLANNED]
ziggurat tasks --limit 50              # Override default limit              [PLANNED]
```

### `ziggurat task <id>`

Full detail. ID prefix matching (unambiguous shortest prefix works).

```
$ ziggurat task a3f2

a3f2b1c8-d7e4-4f01-b234-567890abcdef
  status:     running
  command:    python3 train_model.py
  node:       gpu-workstation
  attempt:    1/3
  priority:   10
  created:    2026-04-24 14:22:01
  started:    2026-04-24 14:22:03
  wall:       14m32s

  requires:   python3, cuda
  constraints:
    gpu.vram >= 80GB
    mem.total >= 64GB

  inputs:
    coefficients: blake3:a3f2b1c8d7e4...

  artifacts:
    scripts/train_model.py: blake3:e5f67890b2c3...

  params:
    SCALE=mZ
    OBSERVABLES=sin2tw,mW
```

After completion:

```
$ ziggurat task e5f6

e5f67890-b2c3-d4e5-f678-901234567890
  status:     completed
  command:    python3 chi2_fit.py
  node:       lab-nuc-02
  attempt:    1/3
  priority:   0
  created:    2026-04-24 14:00:01
  started:    2026-04-24 14:00:02
  completed:  2026-04-24 14:04:20
  wall:       4m18s
  exit:       0
  output_ref: results/chi2-fit-20260424

  stdout (last 5 lines):
    chi2/dof = 1.02
    p-value  = 0.412
    sin2tw   = 0.23122 +/- 0.00003
    mW       = 80.3692 +/- 0.0048
    fit converged in 847 iterations
```

### `ziggurat run`

Unchanged from spec. The key addition is response format:

```
$ ziggurat run python3 rg_evolve.py --require python3 --wait
a3f2b1c8

$ ziggurat run python3 rg_evolve.py --require python3 --wait
chi2/dof = 1.02
p-value  = 0.412
...

$ ziggurat run python3 rg_evolve.py --require python3 --wait --json
{
  "id": "a3f2b1c8-...",
  "status": "completed",
  "exit_code": 0,
  "stdout": "chi2/dof = 1.02\n...",
  "wall_time": "4m18s",
  "output_ref": "results/chi2-fit-20260424"
}
```

Without `--wait`, prints just the task ID (scriptable).

### `ziggurat cancel <id>`

```
$ ziggurat cancel a3f2
a3f2b1c8: cancelled

$ ziggurat cancel a3f2         # already cancelled
a3f2b1c8: already cancelled (not cancelled)

$ ziggurat cancel d7e4         # still running, graceful shutdown initiated
d7e4f012: cancelling (waiting for process to exit)
```

### `ziggurat wait <id>`

Blocks until terminal. Prints stdout/stderr when the task completes. Returns
exit code 0 on success or propagates the task's exit code on failure.

```
$ ziggurat wait a3f2
chi2/dof = 1.02
p-value  = 0.412

$ ziggurat wait a3f2 --timeout 5m
(timed out waiting for task after 5m0s)

$ ziggurat wait a3f2 --json
{ "id": "a3f2b1c8-...", "status": "completed", ... }
```

### `ziggurat retry <id>`

Re-submits a failed task with identical parameters. Returns new task ID.

```
$ ziggurat retry 12345678
re-submitted as b7c8d9e0
```

---

## Storage Commands

### `ziggurat ls`

```
$ ziggurat ls

KEY                         SIZE       CREATED      REFS   PINNED
datasets/fhuft-v3           1.2 GB     3d ago       2      yes
datasets/pdg-2024           847 MB     3d ago       1      no
scripts/rg_evolve.py        4.2 KB     3d ago       0      no
scripts/chi2_fit.py         3.1 KB     2d ago       0      no
results/chi2-fit-20260424   12.8 MB    22m ago      0      no
(5 objects, 2.1 GB total)

$ ziggurat ls datasets/
KEY                         SIZE       CREATED      REFS   PINNED
datasets/fhuft-v3           1.2 GB     3d ago       2      yes
datasets/pdg-2024           847 MB     3d ago       1      no
(2 objects, 2.0 GB total)
```

### `ziggurat store status`

Cluster-wide storage health. Like `anza projects` but for storage.

```
$ ziggurat store status

Objects:    1,247 (42.1 GB)
Replicated: 1,240 healthy, 7 under-replicated
Pinned:     3 objects (2.8 GB)

Per-Node:
  NODE             OBJECTS   SHARDS   USED       CAPACITY   HEALTH
  gpu-workstation  1,247     3,891    18.2 GB    50 GB      healthy
  lab-nuc-01       1,102     3,412    8.7 GB     20 GB      healthy
  lab-nuc-02       1,098     3,398    8.4 GB     20 GB      healthy
  cloud-vm-1       847       2,104    6.8 GB     10 GB      healthy
```

---

## Connection Resolution

The client needs to find the cluster. Resolution order (first wins):

```
1. --addr flag              ziggurat status --addr 10.0.0.5:7100
2. ZIGGURAT_ADDR env var    export ZIGGURAT_ADDR=10.0.0.5:7100
3. Config file client.addr  client: { addr: "10.0.0.5:7100" }
4. Default                  127.0.0.1:7100 (local node)
```

For multi-node clusters, the `--addr` points at any node (typically the coordinator).
Nodes on the same LAN also discover each other automatically via mDNS (`_ziggurat._tcp.local`).

---

## Output Conventions

### Human Output

- **Columns**: fixed-width, aligned with spaces (no tabs). Fits 80-column terminal.
- **IDs**: truncated to 8 hex chars in tables. Full ID in detail views.
- **Durations**: human-friendly (`14m32s`, `3d ago`, `1h`). Never raw seconds in human mode.
- **Sizes**: human-friendly (`42.1 GB`, `4.2 KB`). Never raw bytes in human mode.
- **Empty states**: always a hint, never blank silence.

```
$ ziggurat tasks
no tasks found

$ ziggurat nodes
no nodes registered -- is the cluster running?

$ ziggurat tasks --status running
no running tasks (3 queued)
```

### JSON Output

- **`--json` on any command** produces machine-parseable JSON to stdout.
- Human diagnostics (warnings, hints) go to stderr, never mixed into JSON stdout.
- Arrays are never null; empty = `[]`.
- Timestamps are RFC 3339.
- Durations are strings (`"4m18s"`) matching Go's `time.Duration.String()`.
- Byte sizes are integers (raw bytes), not human-formatted.

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Command error (bad args, missing resource, server error) |
| 2 | Connection refused (cluster unreachable) |
| 3 | Task failed (used by `ziggurat run --wait` when exit != 0) |

`ziggurat run --wait` propagates the task's exit code when possible (exit code < 126).
Exit codes 126+ are reserved for Ziggurat's own errors to avoid collision.

---

## ID Prefix Matching

Any command that takes a task or node ID accepts unambiguous prefix shortcuts.
Minimum prefix length is 4 characters. Shorter prefixes return "task not found".

```
$ ziggurat task a3f2b1c8-d7e4-4f01-b234-567890abcdef   # full ID
$ ziggurat task a3f2b1c8                                 # 8-char prefix
$ ziggurat task a3f2                                     # 4-char prefix (if unique)
$ ziggurat task a3f2                                     # ambiguous → error
error: ambiguous task ID prefix "a3f2"

$ ziggurat task a3                                       # too short → not found
error: task not found: a3
```

Node commands accept name or ID:

```
$ ziggurat node gpu-workstation       # by name
$ ziggurat node c8a3f2b1              # by ID prefix
$ ziggurat status gpu-workstation     # same detail view
```

---

## Anza Integration

Ziggurat nodes are projects in the Anza sense -- each has a `data_dir` with state.
But the primary integration path is **Anza reading Ziggurat's health endpoint**,
not scanning files.

Future: `anza` could learn a `ziggurat` scanner that calls `GET /api/v1/health`
on a configured Ziggurat cluster and surfaces cluster health in the cross-project
status view:

```
$ anza status

Active:
  virc         cardinal executing  12/18 tasks      "Build chat system"
  ziggurat     cluster healthy     4 nodes, 7 tasks  3 queued

Idle:
  phobos, runelite
```

This is out of scope for Ziggurat itself but documents the integration seam.

---

## Phase Mapping

| Command | Phase 0a | Phase 0b | Phase 1 |
|---------|----------|----------|---------|
| `start` | local only | `--join`, mDNS | mTLS, tokens |
| `stop` | local | local | local |
| `drain` | -- | compute-only | + shard migration |
| `status` | single-node summary | cluster dashboard | + replication health |
| `nodes` | -- (single node) | node list | + health history |
| `node <id>` | -- | detail view | + capability history |
| `run` | full | + node targeting | + resource admission |
| `tasks` | full | + `--node` filter | + `--pipeline` filter |
| `task <id>` | full | + node info | + shard placement |
| `cancel` | full | full | full |
| `wait` | full | full | full |
| `retry` | -- | full | full |
| `logs` | -- | -- | SSE streaming ✓ |
| `env *` | -- | full | full |
| `put/get/ls/rm` | full | + replication | + erasure coding |
| `pin/unpin` | full | full | full |
| `store status` | local stats | cluster-wide | + repair queue |
| `pipeline *` | -- | full | full |
| `batch` | -- | full | full |
| `top` | -- | full | + color/sparklines |
| `benchmark` | full | full | full |
| `shell` | -- | full | full |
| `mount` | -- | full | full |
| `health` | full | full | full |
| `version` | full | full | full |

Phase 0a (complete): single-node. Task execution, storage, API, CLI.

Phase 0b (complete): multi-node. Gossip discovery, replication, distributed scheduling,
gRPC transport, failure detection + task requeue. `status` is the cluster dashboard.
`nodes` lists all cluster members. `drain` works for compute.

Phase 1.5 (complete): LAN productivity. Streaming I/O, storage repair loop, dead letter
queue, batch submission, Prometheus metrics, pipelines, cross-node dispatch.

Phase 1 (partial): full production UX. Implemented: streaming logs (SSE), work stealing,
mDNS auto-discovery, resource-aware scheduling, shard rebalancing, drain with shard
migration, remote cancel propagation, schema versioning, persistent environments.
Remaining: container execution (OCI), mTLS + join tokens.
