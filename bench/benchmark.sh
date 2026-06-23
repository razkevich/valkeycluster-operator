#!/usr/bin/env bash
# Benchmark a running ValkeyCluster to demonstrate the clustering/HA trade-offs.
# Runs valkey-benchmark and WAIT-based durability probes from inside a cluster pod
# (so it speaks the cluster protocol and follows MOVED), and writes a markdown
# results table to bench/RESULTS.md.
#
# Usage: bench/benchmark.sh [valkeycluster-name] [namespace]
#   Prereq: the named ValkeyCluster is Ready. To compare shard counts, run once,
#   `kubectl patch ... shards`, wait for Ready, and run again.
set -euo pipefail

CR="${1:-valkeycluster-sample}"
NS="${2:-default}"
POD="${CR}-shard-0-0"
N="${REQUESTS:-100000}"
C="${CLIENTS:-50}"
OUT="$(dirname "$0")/RESULTS.md"

kx() { kubectl exec -n "$NS" "$POD" -- "$@"; }

shards=$(kubectl get valkeycluster "$CR" -n "$NS" -o jsonpath='{.spec.shards}')
replicas=$(kubectl get valkeycluster "$CR" -n "$NS" -o jsonpath='{.spec.replicasPerShard}')

echo "Benchmarking $CR (shards=$shards replicasPerShard=$replicas), $N requests / $C clients"

# 1) Throughput in cluster mode (SET + GET)
set_qps=$(kx valkey-benchmark -p 6379 --cluster -t set -n "$N" -c "$C" -q 2>/dev/null | sed -n 's/.*: \([0-9.]*\) requests per second.*/\1/p' | head -1)
get_qps=$(kx valkey-benchmark -p 6379 --cluster -t get -n "$N" -c "$C" -q 2>/dev/null | sed -n 's/.*: \([0-9.]*\) requests per second.*/\1/p' | head -1)

# 2) Durability vs latency: plain SET vs SET+WAIT 1 (replica-acked)
plain_ms=$(kx sh -c 'start=$(date +%s%N); for i in $(seq 1 2000); do valkey-cli -c -p 6379 set d:$i x >/dev/null; done; echo $(( ($(date +%s%N)-start)/1000000 ))')
wait_ms=$(kx sh -c 'start=$(date +%s%N); for i in $(seq 1 2000); do valkey-cli -c -p 6379 set w:$i x >/dev/null; valkey-cli -c -p 6379 wait 1 100 >/dev/null; done; echo $(( ($(date +%s%N)-start)/1000000 ))')

{
  echo "# Benchmark results"
  echo
  echo "Topology: **shards=$shards, replicasPerShard=$replicas**, $N requests, $C clients."
  echo
  echo "## Throughput (cluster mode)"
  echo
  echo "| Operation | Requests/sec |"
  echo "|-----------|--------------|"
  echo "| SET | ${set_qps:-n/a} |"
  echo "| GET | ${get_qps:-n/a} |"
  echo
  echo "## Durability vs. latency (2000 sequential writes)"
  echo
  echo "| Mode | Total ms | Trade-off |"
  echo "|------|----------|-----------|"
  echo "| plain SET (async replication) | ${plain_ms} | fastest, may lose the last acked writes on failover |"
  echo "| SET + WAIT 1 100 (replica-acked) | ${wait_ms} | slower, shrinks the data-loss window |"
  echo
  echo "_Re-run after \`kubectl patch valkeycluster $CR --type merge -p '{\"spec\":{\"shards\":N}}'\` to compare shard counts (sharding write scale-out)._"
} > "$OUT"

echo "Wrote $OUT"
cat "$OUT"
