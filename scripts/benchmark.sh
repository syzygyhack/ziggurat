#!/usr/bin/env bash
# benchmark.sh — submit a fixed set of fixed-duration tasks to ONE coordinator
# and measure wall-clock completion time. Run with 1 node, then again with the
# remote connected, to compare. Uses deterministic sleep tasks so the only
# variable is available worker-slot parallelism (not CPU-speed differences).
#
# Usage: COORD=http://<ip>:7100 [N=120] [D=3] ./scripts/benchmark.sh
set -uo pipefail
COORD="${COORD:-${1:-}}"; N="${N:-120}"; D="${D:-3}"; TOKEN="${TOKEN:-}"
[ -z "$COORD" ] && { echo "set COORD=http://<ip>:7100" >&2; exit 2; }
AUTH=(); [ -n "$TOKEN" ] && AUTH=(-H "Authorization: Bearer $TOKEN")
api() { curl -s --max-time 20 "${AUTH[@]}" "$@"; }
jget(){ python3 -c "import json,sys;d=json.load(sys.stdin);print($1)" 2>/dev/null; }

CLUSTER=$(api "$COORD/api/v1/cluster")
NODES=$(echo "$CLUSTER" | jget 'd["nodes"]')
DONE0=$(echo "$CLUSTER" | jget 'd["tasks_completed"]')
NODE_JSON=$(api "$COORD/api/v1/nodes")
OS=$(echo "$NODE_JSON" | python3 -c 'import json,sys;n=json.load(sys.stdin);print(n[0]["capabilities"].get("os","linux") if n else "linux")')
# Total worker slots across all healthy nodes (sum of compute.concurrency).
SLOTS=$(echo "$NODE_JSON" | python3 -c '
import json,sys
print(sum(int(n["capabilities"].get("compute.concurrency","0") or 0) for n in json.load(sys.stdin)))')

# Build an OS-appropriate ~D-second no-op task command (JSON array).
if [ "$OS" = windows ]; then
  CMD=$(python3 -c 'import json,sys;print(json.dumps(["cmd","/c","ping -n %d 127.0.0.1 >NUL"%(int(sys.argv[1])+1)]))' "$D")
else
  CMD=$(python3 -c 'import json,sys;print(json.dumps(["sleep",sys.argv[1]]))' "$D")
fi
# Build the batch body: N copies of the task.
BODY=$(python3 -c 'import json,sys;cmd=json.loads(sys.argv[1]);n=int(sys.argv[2]);print(json.dumps([{"command":cmd}]*n))' "$CMD" "$N")

echo "Benchmark: $N tasks x ~${D}s, submitted to $COORD"
echo "  cluster: $NODES node(s), $SLOTS total worker slots"
ideal=$(python3 -c 'import math;print(math.ceil('"$N"'/max(1,'"$SLOTS"'))*'"$D"')')
echo "  ideal makespan at $SLOTS slots: ~${ideal}s (ceil($N/$SLOTS) waves x ${D}s)"
echo "  submitting..."

t0=$(date +%s.%N)
SUBMIT=$(api -X POST "$COORD/api/v1/tasks/batch" -H 'Content-Type: application/json' -d "$BODY")
ACCEPTED=$(echo "$SUBMIT" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(len(d) if isinstance(d,list) else 0)')
[ "$ACCEPTED" = "$N" ] || { echo "  submit failed (accepted=$ACCEPTED): $(echo "$SUBMIT" | head -c 200)"; exit 1; }

# Poll the cluster completed-counter until our N tasks finish.
while :; do
  cs=$(api "$COORD/api/v1/cluster")
  done_=$(echo "$cs" | jget 'd["tasks_completed"]')
  run=$(echo "$cs" | jget 'd["tasks_running"]')
  delta=$(( ${done_:-0} - ${DONE0:-0} ))
  printf "\r  completed %d/%d  running=%s   " "$delta" "$N" "${run:-?}"
  [ "$delta" -ge "$N" ] && break
  sleep 0.5
done
t1=$(date +%s.%N)
echo
wall=$(python3 -c 'print("%.1f"%('"$t1"'-'"$t0"'))')
thru=$(python3 -c 'print("%.1f"%('"$N"'/('"$t1"'-'"$t0"')))')
eff=$(python3 -c 'print("%.0f%%"%(100*'"$ideal"'/max(0.1,'"$t1"'-'"$t0"')))')
echo "------------------------------------------------------------"
echo "RESULT: $N tasks finished in ${wall}s  (throughput ${thru} tasks/s)"
echo "  nodes=$NODES slots=$SLOTS  ideal≈${ideal}s  efficiency≈${eff}"
echo "------------------------------------------------------------"
