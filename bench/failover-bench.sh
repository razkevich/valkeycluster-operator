#!/usr/bin/env bash
# Failover-latency benchmark: measure the write-availability window when a shard's
# primary is killed, as a function of cluster-node-timeout. This is the evidence
# behind the "HA settings vs. performance tradeoff" discussion: a lower node-timeout
# fails over faster (shorter window) but is more prone to false positives under jitter.
#
# A cluster-aware probe writes a shard-0 key every ~50ms from a surviving pod; we kill
# shard 0's current primary and measure ms from the first failed write to the first
# recovered write. Repeated for several node-timeout values. Writes bench/FAILOVER-RESULTS.md.
#
# Usage: bench/failover-bench.sh [valkeycluster-name] [namespace]
#   Prereq: the named ValkeyCluster is Ready with >=3 shards and >=1 replica per shard.
set -euo pipefail

CR="${1:-valkeycluster-sample}"
NS="${2:-default}"
CLIENT="${CR}-shard-2-0" # a surviving pod that runs the probe
OUT="$(dirname "$0")/FAILOVER-RESULTS.md"
TIMEOUTS="${TIMEOUTS:-15000 5000}"
# How the primary is failed:
#   kill (default) — force-delete the pod (SIGKILL): a *clean crash*. The port closes, so
#                    peers detect connection-refused almost immediately; the window is
#                    dominated by election (~constant) and barely tracks the timeout.
#   freeze         — `DEBUG SLEEP` on the primary: a *soft* failure (stall / GC pause).
#                    It stops answering heartbeats, so peers must wait cluster-node-timeout
#                    to declare it dead — the window then scales with the timeout.
#                    (SIGSTOP can't be used: the kernel blocks it for PID 1, which is valkey.)
FAILURE="${FAILURE:-kill}"

phase() { kubectl get valkeycluster "$CR" -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null; }
shard0Primary() { kubectl get valkeycluster "$CR" -n "$NS" -o jsonpath='{.status.shards[0].primaryPod}'; }

# Safety + fail-fast preconditions: this bench kills a primary pod, so refuse any
# non-Kind context, and verify the cluster and probe pod exist before doing anything.
ctx=$(kubectl config current-context)
case "$ctx" in
  kind-*) ;;
  *) echo "refusing to run: kube context '$ctx' is not a kind-* context (this bench deletes a pod)" >&2; exit 1 ;;
esac
if [ -z "$(phase)" ]; then
  echo "ValkeyCluster '$CR' not found in namespace '$NS' (context: $ctx)." >&2
  echo "Deploy it first (make kind-deploy IMG=valkeycluster-operator:dev && kubectl apply -f config/samples/cache_v1alpha1_valkeycluster.yaml)," >&2
  echo "or pass the right name:  $(basename "$0") <name> [namespace]" >&2
  exit 1
fi
if ! kubectl get pod -n "$NS" "$CLIENT" >/dev/null 2>&1; then
  echo "probe client pod '$CLIENT' not found — this bench needs >=3 shards (shard-2 must exist)." >&2
  exit 1
fi

# Pick a probe key whose slot is owned by shard 0 (slots 0-5460).
probe=""
for i in $(seq 1 500); do
  s=$(kubectl exec -n "$NS" "$CLIENT" -- valkey-cli -p 6379 cluster keyslot "p$i" 2>/dev/null || echo 99999)
  if [ "$s" -ge 0 ] 2>/dev/null && [ "$s" -le 5460 ] 2>/dev/null; then probe="p$i"; break; fi
done
[ -n "$probe" ] || { echo "could not find a shard-0 probe key" >&2; exit 1; }
echo "probe key = $probe (shard 0), failure mode = $FAILURE"

# fail_victim injects the failure. A killed pod is recreated by its StatefulSet; a
# DEBUG SLEEP'd node wakes (and demotes to replica) on its own — neither needs cleanup.
fail_victim() {
  if [ "$FAILURE" = freeze ]; then
    # block the primary's event loop past the node-timeout; DEBUG SLEEP holds its
    # connection, so background it.
    local secs=$(( NT/1000 + 12 ))
    kubectl exec -n "$NS" "$victim" -- sh -c "valkey-cli -p 6379 debug sleep $secs" >/dev/null 2>&1 &
  else
    kubectl delete pod -n "$NS" "$victim" --grace-period=0 --force >/dev/null 2>&1
  fi
}

rows=""
for NT in $TIMEOUTS; do
  echo "=== cluster-node-timeout = ${NT}ms ==="
  # apply the timeout live to every node
  for p in $(kubectl get pods -n "$NS" -l app.kubernetes.io/instance="$CR" -o jsonpath='{.items[*].metadata.name}'); do
    kubectl exec -n "$NS" "$p" -- valkey-cli -p 6379 config set cluster-node-timeout "$NT" >/dev/null 2>&1 || true
  done

  victim=$(shard0Primary)
  echo "victim (shard-0 primary) = $victim"

  # start a ~45s cluster-aware probe loop inside the surviving client pod
  kubectl exec -n "$NS" "$CLIENT" -- sh -c '
    end=$(( $(date +%s) + 45 ))
    while [ $(date +%s) -lt $end ]; do
      t=$(date +%s%3N)
      out=$(timeout 1 valkey-cli -c -p 6379 set '"$probe"' v 2>&1); if [ "$out" = OK ]; then echo "$t ok"; else echo "$t fail"; fi
      sleep 0.05
    done > /tmp/probe.log 2>&1
  ' &
  loop_pid=$!

  sleep 5 # baseline window
  # Take kill_ts from the pod's clock (same domain as the probe timestamps). macOS
  # BSD `date` doesn't support %N, so a host-side ms timestamp would be garbage.
  kill_ts=$(kubectl exec -n "$NS" "$CLIENT" -- date +%s%3N)
  fail_victim
  wait "$loop_pid"

  log=$(kubectl exec -n "$NS" "$CLIENT" -- cat /tmp/probe.log 2>/dev/null || echo "")
  window=$(printf '%s\n' "$log" | awk -v k="$kill_ts" '
    $1>=k && $2=="fail" && ff==0 {ff=$1}
    $1>=k && $2=="ok"   && ff>0 && lo==0 {lo=$1}
    END { if (ff>0 && lo>0) print lo-ff; else print 0 }')
  errors=$(printf '%s\n' "$log" | awk -v k="$kill_ts" '$1>=k && $2=="fail"{c++} END{print c+0}')
  echo "  failover window ≈ ${window}ms, failed writes = ${errors}"
  rows="${rows}| ${NT} | ${window} | ${errors} |\n"

  echo "  waiting for cluster to recover before next round..."
  until [ "$(phase)" = "Ready" ]; do sleep 5; done
  sleep 10
done

fmode_desc="clean crash — force-delete the primary pod (\`SIGKILL\`), so the port closes \
and peers detect the failure via connection-refused almost immediately"
[ "$FAILURE" = freeze ] && fmode_desc="soft failure — \`DEBUG SLEEP\` blocks the primary's \
event loop past the node-timeout, so peers stop receiving heartbeats and must time it out"

{
  echo "# Failover latency vs. cluster-node-timeout"
  echo
  echo "## What this measures"
  echo
  echo "The write-availability window when a shard's primary fails: how long clients cannot write"
  echo "to that shard's keys, as a function of \`cluster-node-timeout\`. This is the evidence behind"
  echo "the HA tradeoff — a lower timeout fails over faster but is more prone to false positives."
  echo
  echo "## How it was tested"
  echo
  echo "- A cluster-aware probe runs from a **surviving pod** (\`$CLIENT\`): every ~50ms it issues a"
  echo "  \`SET\` of a key owned by **shard 0** and records \`ok\` only when the reply is literally \`OK\`"
  echo "  (a \`CLUSTERDOWN\`/redirect error counts as a failed write, not a success)."
  echo "- Each round sets \`cluster-node-timeout\` live on every node, waits a baseline, then fails"
  echo "  **shard 0's current primary**. Failure mode for this run: **$FAILURE** — $fmode_desc."
  echo "- **Window** = ms from the first failed write to the first recovered write (≈ failure"
  echo "  detection + replica election + promotion). The cluster is allowed to return to \`Ready\`"
  echo "  between rounds. Probe writes are wrapped in a 1s \`timeout\` so a frozen node fails fast."
  echo
  echo "## Results"
  echo
  echo "| cluster-node-timeout (ms) | failover window (ms) | failed writes during window |"
  echo "|---|---|---|"
  printf "$rows"
  echo
  echo "**Reading it:** under a **soft** failure the window tracks \`cluster-node-timeout\` (you wait"
  echo "the timeout to detect the dead node) — the tradeoff: a lower timeout recovers faster but is"
  echo "likelier to misfire under transient latency or GC pauses. Under a **clean crash** (\`kill\`"
  echo "mode) the window is near-constant and dominated by election, because connection-refused is"
  echo "detected almost instantly regardless of the timeout."
} > "$OUT"

echo "Wrote $OUT"
cat "$OUT"
