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

CR="${1:-fl}"
NS="${2:-default}"
CLIENT="${CR}-shard-2-0" # a surviving pod that runs the probe
OUT="$(dirname "$0")/FAILOVER-RESULTS.md"
TIMEOUTS="${TIMEOUTS:-15000 5000}"

phase() { kubectl get valkeycluster "$CR" -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null; }
shard0Primary() { kubectl get valkeycluster "$CR" -n "$NS" -o jsonpath='{.status.shards[0].primaryPod}'; }

# Pick a probe key whose slot is owned by shard 0 (slots 0-5460).
probe=""
for i in $(seq 1 500); do
  s=$(kubectl exec -n "$NS" "$CLIENT" -- valkey-cli -p 6379 cluster keyslot "p$i" 2>/dev/null || echo 99999)
  if [ "$s" -ge 0 ] 2>/dev/null && [ "$s" -le 5460 ] 2>/dev/null; then probe="p$i"; break; fi
done
[ -n "$probe" ] || { echo "could not find a shard-0 probe key" >&2; exit 1; }
echo "probe key = $probe (shard 0)"

rows=""
for NT in $TIMEOUTS; do
  echo "=== cluster-node-timeout = ${NT}ms ==="
  # apply the timeout live to every node
  for p in $(kubectl get pods -n "$NS" -l app.kubernetes.io/instance="$CR" -o jsonpath='{.items[*].metadata.name}'); do
    kubectl exec -n "$NS" "$p" -- valkey-cli -p 6379 config set cluster-node-timeout "$NT" >/dev/null 2>&1 || true
  done

  victim=$(shard0Primary)
  echo "victim (shard-0 primary) = $victim"

  # start a ~35s cluster-aware probe loop inside the surviving client pod
  kubectl exec -n "$NS" "$CLIENT" -- sh -c '
    end=$(( $(date +%s) + 35 ))
    while [ $(date +%s) -lt $end ]; do
      t=$(date +%s%3N)
      if valkey-cli -c -p 6379 set '"$probe"' v >/dev/null 2>&1; then echo "$t ok"; else echo "$t fail"; fi
      sleep 0.05
    done > /tmp/probe.log 2>&1
  ' &
  loop_pid=$!

  sleep 5 # baseline window
  kill_ts=$(date +%s%3N)
  kubectl delete pod -n "$NS" "$victim" --wait=false >/dev/null 2>&1
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

{
  echo "# Failover latency vs. cluster-node-timeout"
  echo
  echo "A cluster-aware probe writes a shard-0 key every ~50ms from a surviving pod while"
  echo "shard 0's primary is killed. **Window** = ms from the first failed write to the first"
  echo "recovered write (≈ failure detection + replica election + promotion)."
  echo
  echo "| cluster-node-timeout (ms) | failover window (ms) | failed writes during window |"
  echo "|---|---|---|"
  printf "$rows"
  echo
  echo "**Tradeoff:** a lower \`cluster-node-timeout\` shortens the unavailability window (faster"
  echo "failover) but makes the cluster more likely to trigger *false-positive* failovers under"
  echo "transient network latency or GC pauses. The default (5000ms) balances the two."
} > "$OUT"

echo "Wrote $OUT"
cat "$OUT"
