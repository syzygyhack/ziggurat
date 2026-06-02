#!/usr/bin/env bash
# mesh-test.sh — exercise a running Ziggurat mesh: distribute an assortment of
# tasks across nodes, verify results, and test storage + monitoring live.
#
# Usage:
#   COORD=http://<coordinator-ip>:7100 ./scripts/mesh-test.sh
#   ./scripts/mesh-test.sh http://192.168.1.10:7100 [http://192.168.1.11:7100]
#
# Args/env:
#   COORD   coordinator API base URL (required)
#   COORD2  a second node's API base URL (optional; tests cross-node storage)
#   TOKEN   API bearer token (optional; if api_token is configured)
#
# Requires: bash, curl, python3. No ziggurat binary needed on this machine.
set -uo pipefail

COORD="${COORD:-${1:-}}"
COORD2="${COORD2:-${2:-}}"
TOKEN="${TOKEN:-}"
if [ -z "$COORD" ]; then
  echo "ERROR: set COORD=http://<host>:7100 (coordinator API base URL)" >&2
  exit 2
fi
AUTH=(); [ -n "$TOKEN" ] && AUTH=(-H "Authorization: Bearer $TOKEN")

PASS=0; FAIL=0
ok()   { echo "  PASS: $*"; PASS=$((PASS+1)); }
bad()  { echo "  FAIL: $*"; FAIL=$((FAIL+1)); }
hr()   { echo "------------------------------------------------------------"; }

api()  { curl -s --max-time 15 "${AUTH[@]}" "$@"; }
jq_py(){ python3 -c "import json,sys; d=json.load(sys.stdin); print($1)" 2>/dev/null; }

# submit_task <json> -> prints task id
submit_task() {
  api -X POST "$COORD/api/v1/tasks" -H 'Content-Type: application/json' -d "$1" \
    | jq_py 'd["id"]'
}

hr; echo "PHASE 0 — Preflight: cluster health & capabilities"; hr
HEALTH=$(api "$COORD/api/v1/health" -o /dev/null -w '%{http_code}')
[ "$HEALTH" = "200" ] && ok "coordinator healthy (HTTP 200)" || { bad "coordinator unreachable (got $HEALTH)"; exit 1; }

CLUSTER=$(api "$COORD/api/v1/cluster")
NODES=$(echo "$CLUSTER" | jq_py 'd["nodes"]')
HEALTHY=$(echo "$CLUSTER" | jq_py 'd["nodes_healthy"]')
echo "  cluster: $NODES nodes, $HEALTHY healthy"
[ "${NODES:-0}" -ge 2 ] && ok "at least 2 nodes present" || bad "expected >= 2 nodes, got ${NODES:-0}"

# Capture node id->hostname and the worker OS (assume homogeneous OS for commands).
NODE_JSON=$(api "$COORD/api/v1/nodes")
OS=$(echo "$NODE_JSON" | python3 -c '
import json,sys
ns=json.load(sys.stdin)
print(ns[0]["capabilities"].get("os","linux") if ns else "linux")')
echo "  worker OS: $OS"
echo "$NODE_JSON" | python3 -c '
import json,sys
for n in json.load(sys.stdin):
    c=n["capabilities"]
    print("    node {} host={} os={} cores={} gpu={}".format(
        n["id"][:8], c.get("hostname"), c.get("os"), c.get("cpu.cores"), c.get("gpu.count","-")))'

# Collect node IDs (for affinity-pinned tasks to force a split).
mapfile -t NODE_IDS < <(echo "$NODE_JSON" | python3 -c '
import json,sys
[print(n["id"]) for n in json.load(sys.stdin)]')

# OS-aware command builders (emit a JSON array for the "command" field).
cmd_json() { python3 -c 'import json,sys; print(json.dumps(sys.argv[1:]))' "$@"; }
build_echo()   { if [ "$OS" = windows ]; then cmd_json cmd /c echo "$1"; else cmd_json echo "$1"; fi; }
build_host()   { cmd_json hostname; }
build_readin() { if [ "$OS" = windows ]; then cmd_json cmd /c type "%ZIGGURAT_INPUT%\\data"; else cmd_json sh -c 'cat "$ZIGGURAT_INPUT/data"'; fi; }
build_writeo() { if [ "$OS" = windows ]; then cmd_json cmd /c "echo mesh-output-ok> %ZIGGURAT_OUTPUT%\\r.txt"; else cmd_json sh -c 'echo mesh-output-ok > "$ZIGGURAT_OUTPUT/r.txt"'; fi; }
build_sleep()  { if [ "$OS" = windows ]; then cmd_json cmd /c "ping -n $(($1+1)) 127.0.0.1 >NUL"; else cmd_json sleep "$1"; fi; }
build_fail()   { if [ "$OS" = windows ]; then cmd_json cmd /c "exit 3"; else cmd_json sh -c 'exit 3'; fi; }

hr; echo "PHASE 1 — Storage round-trip (content-addressed put/get)"; hr
BLOB="ziggurat-storage-test-$(date +%s)-$$-payload-payload-payload"
PUTRESP=$(api -X PUT "$COORD/api/v1/store/test/blob" --data-binary "$BLOB")
HASH=$(echo "$PUTRESP" | jq_py 'd["hash"]')
[ -n "$HASH" ] && ok "PUT stored blob, hash=${HASH:0:16}…" || bad "PUT failed: $PUTRESP"
GOT=$(api "$COORD/api/v1/store/test/blob")
[ "$GOT" = "$BLOB" ] && ok "GET returned identical bytes (integrity verified)" || bad "GET mismatch"
if [ -n "$COORD2" ]; then
  sleep 3  # allow async replication
  # Namespace keys are node-local by design; replicated CONTENT is addressed by
  # hash (this is how a remote worker fetches a task's resolved inputs).
  GOT2=$(api "$COORD2/api/v1/store/@hash/$HASH")
  [ "$GOT2" = "$BLOB" ] && ok "content replicated & readable from second node (by hash)" || bad "second node missing replicated content"
  KEY2=$(api "$COORD2/api/v1/store/test/blob" -o /dev/null -w '%{http_code}')
  [ "$KEY2" = "404" ] && ok "namespace keys are node-local as designed (key 404 on peer; content via @hash)" \
    || echo "  note: peer returned $KEY2 for the namespace key"
fi

hr; echo "PHASE 2 — Submit an assortment of tasks"; hr
declare -A TID   # logical name -> task id
declare -A TEXP  # logical name -> expected (status:exitcode)

# Per-node affinity tasks to FORCE a split across the mesh.
i=0
for nid in "${NODE_IDS[@]}"; do
  cmd=$(build_host)
  body=$(python3 -c 'import json,sys; print(json.dumps({"command":json.loads(sys.argv[1]),"config":{"affinity":sys.argv[2]}}))' "$cmd" "$nid")
  id=$(submit_task "$body")
  TID[pin$i]=$id; TEXP[pin$i]="completed:0"
  echo "  pinned hostname task -> node ${nid:0:8} : ${id:0:8}"
  i=$((i+1))
done

# A burst of unpinned tasks (observe natural load balancing).
for n in 1 2 3 4 5 6; do
  id=$(submit_task "{\"command\":$(build_host)}")
  TID[burst$n]=$id; TEXP[burst$n]="completed:0"
done
echo "  submitted 6 unpinned hostname tasks"

# Input-consuming task: reads the stored blob and echoes it (tests store fetch).
INBODY=$(python3 -c 'import json,sys; print(json.dumps({"command":json.loads(sys.argv[1]),"input_refs":{"data":"test/blob"}}))' "$(build_readin)")
TID[input]=$(submit_task "$INBODY"); TEXP[input]="completed:0"
echo "  input-consuming task -> ${TID[input]:0:8}"

# Output-producing task: writes to \$ZIGGURAT_OUTPUT (tests store upload).
TID[output]=$(submit_task "{\"command\":$(build_writeo)}"); TEXP[output]="completed:0"
echo "  output-producing task -> ${TID[output]:0:8}"

# Resource-constrained task.
TID[res]=$(submit_task "{\"command\":$(build_echo 'resourced'),\"resources\":{\"cpu_cores\":1}}"); TEXP[res]="completed:0"
echo "  resource-constrained task -> ${TID[res]:0:8}"

# Deliberate failure (exit 3) -> verifies error capture. With dead_letter
# enabled (default), a retry-exhausted task ends in dead_letter; otherwise failed.
TID[fail]=$(submit_task "{\"command\":$(build_fail),\"config\":{\"max_retries\":0}}"); TEXP[fail]="failed:3"
echo "  failing task (exit 3) -> ${TID[fail]:0:8}"

# Long-running tasks to keep the mesh busy during monitoring.
for n in 1 2; do
  TID[long$n]=$(submit_task "{\"command\":$(build_sleep 12)}"); TEXP[long$n]="completed:0"
done
echo "  2 long (~12s) tasks -> monitoring window"

hr; echo "PHASE 3 — Live monitoring while tasks run"; hr
metric() { api "$COORD/metrics" | grep -E "^$1 " | awk '{print $2}' | head -1; }
for t in 0 3 6 9 12 15; do
  sleep 3
  cs=$(api "$COORD/api/v1/cluster")
  running=$(echo "$cs" | jq_py 'd["tasks_running"]')
  queued=$(echo "$cs" | jq_py 'd["tasks_queued"]')
  done_=$(echo "$cs" | jq_py 'd["tasks_completed"]')
  sub=$(metric ziggurat_tasks_submitted_total)
  active=$(metric ziggurat_workers_active)
  qd=$(metric ziggurat_task_queue_depth)
  printf "  t+%2ss  cluster[run=%s queue=%s done=%s]  metrics[submitted=%s active=%s qdepth=%s]\n" \
    "$t" "$running" "$queued" "$done_" "${sub%.*}" "${active%.*}" "${qd%.*}"
done

hr; echo "PHASE 4 — Collect results & verify"; hr
# Wait for all tasks to reach a terminal state.
declare -A WORKER STATUS EXIT STDOUT
deadline=$((SECONDS+60))
pending=("${!TID[@]}")
while [ ${#pending[@]} -gt 0 ] && [ $SECONDS -lt $deadline ]; do
  still=()
  for name in "${pending[@]}"; do
    t=$(api "$COORD/api/v1/tasks/${TID[$name]}")
    st=$(echo "$t" | jq_py 'd["status"]')
    case "$st" in
      completed|failed|cancelled|dead_letter)
        STATUS[$name]=$st
        EXIT[$name]=$(echo "$t" | jq_py 'd["exit_code"]')
        WORKER[$name]=$(echo "$t" | jq_py 'd.get("worker","")')
        STDOUT[$name]=$(echo "$t" | jq_py 'repr(d.get("stdout",""))')
        ;;
      *) still+=("$name");;
    esac
  done
  pending=("${still[@]}")
  [ ${#pending[@]} -gt 0 ] && sleep 2
done
[ ${#pending[@]} -eq 0 ] && ok "all ${#TID[@]} tasks reached terminal state" || bad "tasks still pending: ${pending[*]}"

# Verify each task's outcome matches expectation.
for name in "${!TEXP[@]}"; do
  want="${TEXP[$name]}"; got="${STATUS[$name]:-?}:${EXIT[$name]:-?}"
  if [ "$got" = "$want" ]; then
    ok "$name -> $got"
  elif [ "$name" = "fail" ] && [ "${EXIT[$name]:-}" = "3" ] && \
       { [ "${STATUS[$name]:-}" = "failed" ] || [ "${STATUS[$name]:-}" = "dead_letter" ]; }; then
    ok "$name -> ${STATUS[$name]}:3 (failed/dead-lettered as expected)"
  else
    bad "$name -> got $got, want $want"
  fi
done

# Verify the work was actually SPLIT across distinct workers.
SPLIT=$(printf '%s\n' "${WORKER[@]}" | grep -v '^$' | sort -u | wc -l)
echo "  distinct workers used: $SPLIT"
[ "$SPLIT" -ge 2 ] && ok "tasks distributed across >= 2 nodes" || bad "all tasks ran on a single node"

# Verify storage input fetch: the input task's stdout must equal the blob.
echo "  input task stdout = ${STDOUT[input]}"
echo "${STDOUT[input]}" | grep -qF "$BLOB" && ok "input-consuming task read the stored blob correctly" \
  || bad "input task did not return the stored blob"

# Verify task output landed in the store (retrievable).
OUT=$(api "$COORD/api/v1/store/output/${TID[output]}" -o /dev/null -w '%{http_code}')
[ "$OUT" = "200" ] && ok "output task artifact retrievable from store (output/<taskid>)" \
  || bad "output artifact not in store (HTTP $OUT)"

hr; echo "SUMMARY: $PASS passed, $FAIL failed"; hr
[ "$FAIL" -eq 0 ]
