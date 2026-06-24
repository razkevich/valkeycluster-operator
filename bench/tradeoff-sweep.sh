#!/usr/bin/env bash
# bench/tradeoff-sweep.sh — sweep the durability/HA levers under high TPS and show
# that looser guarantees buy throughput. For each profile it patches the
# ValkeyCluster, waits for the config-hash rolling restart to actually land (gating on
# the live CONFIG GET, not just phase=Ready), then runs a pipelined valkey-benchmark
# and records SET/GET throughput. Within the loosest profile it also contrasts
# WAIT 0 vs WAIT 1 (replica-acked) sequential writes.
#
# It writes bench/TRADEOFF-RESULTS.md with a PASS/FAIL verdict column for the expected
# ordering (none >= everysec >= always; WAIT1 slower than WAIT0). Report-only: it
# prints the verdict but never fails the run. The original spec is restored on exit.
#
# Usage: bench/tradeoff-sweep.sh [valkeycluster-name] [namespace]
#   Prereq: the named ValkeyCluster is Ready on a Kind cluster, replicasPerShard >= 1
#           (the WAIT contrast needs a replica to ack).
#   Tunables: REQUESTS (default 1000000), CLIENTS (50), PIPELINE (16).
set -euo pipefail

CR="${1:-valkeycluster-sample}"
NS="${2:-default}"
POD="${CR}-shard-0-0"
N="${REQUESTS:-1000000}"
C="${CLIENTS:-50}"
P="${PIPELINE:-16}"
OUT="$(dirname "$0")/TRADEOFF-RESULTS.md"

kc() { kubectl -n "$NS" "$@"; }
kx() { kubectl exec -n "$NS" "$POD" -- "$@"; }

# Safety: this script mutates the cluster's config; refuse to run anywhere but Kind.
ctx=$(kubectl config current-context)
case "$ctx" in
  kind-*) ;;
  *) echo "refusing to run: kube context '$ctx' is not a kind-* context" >&2; exit 1 ;;
esac

replicas=$(kc get valkeycluster "$CR" -o jsonpath='{.spec.replicasPerShard}')

# Capture the original spec so we can restore it on exit (null -> CRD default).
orig_mode=$(kc get valkeycluster "$CR" -o jsonpath='{.spec.persistence.mode}')
orig_fsync=$(kc get valkeycluster "$CR" -o jsonpath='{.spec.persistence.appendFsync}')
orig_io=$(kc get valkeycluster "$CR" -o jsonpath='{.spec.performance.ioThreads}')
jval() { [ -z "$1" ] && echo null || echo "\"$1\""; }
ival() { [ -z "$1" ] && echo null || echo "$1"; }
restore() {
  echo "==> restoring original persistence/performance"
  kc patch valkeycluster "$CR" --type merge -p \
    "{\"spec\":{\"persistence\":{\"mode\":$(jval "$orig_mode"),\"appendFsync\":$(jval "$orig_fsync")},\"performance\":{\"ioThreads\":$(ival "$orig_io")}}}" >/dev/null 2>&1 || true
}
trap restore EXIT

# val extracts the value line from `valkey-cli config get <param>` (name, then value).
cfg() { kx valkey-cli -p 6379 config get "$1" 2>/dev/null | tail -1 | tr -d '\r'; }

# wait_applied blocks until the rolling restart lands and the live config matches the
# profile: every shard StatefulSet finished rolling, phase is Ready, and the seed pod
# reports the expected appendonly / appendfsync.
wait_applied() { # mode fsync
  local mode="$1" fsync="$2" want_aof="yes"
  [ "$mode" = "None" ] && want_aof="no"
  sleep 5 # let the operator update the StatefulSet template before we watch the roll
  local sts
  for sts in $(kc get sts -l app.kubernetes.io/instance="$CR" -o jsonpath='{.items[*].metadata.name}'); do
    kc rollout status "sts/$sts" --timeout=180s >/dev/null
  done
  local i phase aof cur_fsync
  for i in $(seq 1 60); do
    phase=$(kc get valkeycluster "$CR" -o jsonpath='{.status.phase}')
    aof=$(cfg appendonly)
    cur_fsync=$(cfg appendfsync)
    if [ "$phase" = "Ready" ] && [ "$aof" = "$want_aof" ] &&
       { [ "$want_aof" = "no" ] || [ "$cur_fsync" = "$fsync" ]; }; then
      return 0
    fi
    sleep 3
  done
  echo "WARN: profile (mode=$mode fsync=$fsync) not fully applied (phase=$phase appendonly=$aof appendfsync=$cur_fsync)" >&2
}

apply_profile() { # mode fsync io
  echo "==> applying profile: mode=$1 appendFsync=$2 ioThreads=$3"
  kc patch valkeycluster "$CR" --type merge -p \
    "{\"spec\":{\"persistence\":{\"mode\":\"$1\",\"appendFsync\":\"$2\"},\"performance\":{\"ioThreads\":$3}}}" >/dev/null
  wait_applied "$1" "$2"
}

qps() { # op
  kx valkey-benchmark -p 6379 --cluster -t "$1" -n "$N" -c "$C" -P "$P" -q 2>/dev/null |
    sed -n 's/.*: \([0-9.]*\) requests per second.*/\1/p' | head -1
}

# Lean sweep: three persistence profiles at io-threads 4 (loosest -> safest).
names=("None (loosest)"        "AOF everysec"  "AOF always (safest)")
modes=("None"                  "AOF"           "AOF")
fsyncs=("everysec"             "everysec"      "always")
io=4

setq=(); getq=()
wait0_ms=""; wait1_ms=""
for idx in "${!names[@]}"; do
  apply_profile "${modes[$idx]}" "${fsyncs[$idx]}" "$io"
  echo "    benchmarking SET/GET ($N req, $C clients, pipeline $P)"
  setq+=("$(qps set)")
  getq+=("$(qps get)")

  # WAIT 0 vs WAIT 1 contrast, run once on the loosest profile.
  if [ "$idx" -eq 0 ] && [ "${replicas:-0}" -ge 1 ]; then
    echo "    measuring WAIT 0 vs WAIT 1 (2000 sequential writes)"
    wait0_ms=$(kx sh -c 'start=$(date +%s%N); for i in $(seq 1 2000); do valkey-cli -c -p 6379 set t0:$i x >/dev/null; done; echo $(( ($(date +%s%N)-start)/1000000 ))')
    wait1_ms=$(kx sh -c 'start=$(date +%s%N); for i in $(seq 1 2000); do valkey-cli -c -p 6379 set t1:$i x >/dev/null; valkey-cli -c -p 6379 wait 1 200 >/dev/null; done; echo $(( ($(date +%s%N)-start)/1000000 ))')
  fi
done

# Verdicts (5% tolerance to absorb measurement noise). Report-only: never fails.
ge() { awk -v a="${1:-0}" -v b="${2:-0}" 'BEGIN{exit !(a+0 >= (b+0)*0.95)}'; }
gt() { awk -v a="${1:-0}" -v b="${2:-0}" 'BEGIN{exit !(a+0 > b+0)}'; }
v_none_ev="n/a"; v_ev_al="n/a"; v_wait="n/a"
[ -n "${setq[0]:-}" ] && [ -n "${setq[1]:-}" ] && { ge "${setq[0]}" "${setq[1]}" && v_none_ev=PASS || v_none_ev=FAIL; }
[ -n "${setq[1]:-}" ] && [ -n "${setq[2]:-}" ] && { ge "${setq[1]}" "${setq[2]}" && v_ev_al=PASS || v_ev_al=FAIL; }
[ -n "$wait0_ms" ] && [ -n "$wait1_ms" ] && { gt "$wait1_ms" "$wait0_ms" && v_wait=PASS || v_wait=FAIL; }

{
  echo "# Lever tradeoff sweep"
  echo
  echo "## What this measures"
  echo
  echo "That looser durability guarantees buy throughput: SET/GET ops/sec across three persistence"
  echo "profiles (None → AOF everysec → AOF always), plus the per-write cost of \`WAIT\` (replica-acked)."
  echo
  echo "## How it was tested"
  echo
  echo "- ValkeyCluster **$CR** (replicasPerShard=$replicas), io-threads $io across all profiles."
  echo "- Each profile is applied by **patching the CR** and waiting for the config-hash rolling"
  echo "  restart to land (gated on the live \`CONFIG GET appendonly/appendfsync\`, not just phase)."
  echo "- Throughput: \`valkey-benchmark --cluster -t set/get -n $N -c $C -P $P\`, run inside a pod."
  echo "- WAIT contrast: 2000 sequential \`SET\`s, plain vs. each followed by \`WAIT 1 200\`, on the"
  echo "  loosest profile. The original spec is restored afterward."
  echo
  echo "## SET/GET throughput by durability profile"
  echo
  echo "| Profile | persistence | appendfsync | SET req/s | GET req/s |"
  echo "|---------|-------------|-------------|-----------|-----------|"
  for idx in "${!names[@]}"; do
    echo "| ${names[$idx]} | ${modes[$idx]} | ${fsyncs[$idx]} | ${setq[$idx]:-n/a} | ${getq[$idx]:-n/a} |"
  done
  echo
  echo "**Expected:** SET throughput falls as durability tightens (None ≥ everysec ≥ always)."
  echo
  echo "| Check | Verdict |"
  echo "|-------|---------|"
  echo "| None ≥ AOF everysec | $v_none_ev |"
  echo "| AOF everysec ≥ AOF always | $v_ev_al |"
  echo
  echo "## Per-write durability cost (WAIT, 2000 sequential writes, loosest profile)"
  echo
  echo "| Mode | Total ms | Trade-off |"
  echo "|------|----------|-----------|"
  echo "| plain SET (async replication) | ${wait0_ms:-n/a} | fastest; may lose the last acked writes on failover |"
  echo "| SET + WAIT 1 200 (replica-acked) | ${wait1_ms:-n/a} | slower; shrinks the data-loss window |"
  echo
  echo "**Expected:** WAIT 1 is slower than plain SET — verdict: $v_wait"
  echo
  echo "_Verdicts use a 5% tolerance and are report-only. Tunables: REQUESTS, CLIENTS, PIPELINE._"
} > "$OUT"

echo
echo "Wrote $OUT"
cat "$OUT"
