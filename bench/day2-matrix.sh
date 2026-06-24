#!/usr/bin/env bash
#
# day2-matrix.sh — manual day-2 smoke test against an already-running ValkeyCluster.
#
# Drives the full topology matrix (provision 3 -> scale-out 5 -> scale-in 3) and
# verifies reads AND writes at every pivot, including LIVE writes mid-reshard, to
# prove the cluster keeps serving and preserves data through each operation.
#
# This is the quick, human-readable harness used during development. The asserted,
# CI-grade version lives in the std-testing e2e suite (test/e2e/valkeycluster_test.go,
# `make test-e2e`); use this when you want to watch a real cluster move in real
# time against an existing operator install.
#
# Writes are verify-and-retry: a SET is retried on transient CLUSTERDOWN (the brief
# formation/resharding window under requireFullCoverage), so the matrix measures
# durability rather than racing cluster warm-up — exactly what a real client does.
#
# Usage:
#   bench/day2-matrix.sh
#   NAME=try CTX=kind-valkeycluster-dev bench/day2-matrix.sh
#
# Env (with defaults):
#   NAME  ValkeyCluster name + StatefulSet prefix   (default: try)
#   CTX   kubectl context                           (default: kind-valkeycluster-dev)
#   CR    optional manifest to apply for the base    (default: generated inline from NAME)
#
# This script manages its own throwaway cluster named $NAME (clean start -> provision
# -> scale-out -> scale-in), so it won't disturb other ValkeyClusters. By default it
# generates the base manifest inline; set CR=path/to.yaml to apply your own.
set -uo pipefail

NAME="${NAME:-try}"
CTX="${CTX:-kind-valkeycluster-dev}"
CR="${CR:-}"
SEED="${NAME}-shard-0-0"

ns() { kubectl config use-context "$CTX" >/dev/null 2>&1; }

# apply_base applies the 3-shard base: a CR file if one was provided, else a manifest
# generated inline from $NAME (so there's no dependency on a checked-in file).
apply_base() {
  ns
  if [ -n "$CR" ] && [ -f "$CR" ]; then
    kubectl apply -f "$CR" >/dev/null 2>&1
    return
  fi
  kubectl apply -f - >/dev/null 2>&1 <<EOF
apiVersion: cache.razkevich.dev/v1alpha1
kind: ValkeyCluster
metadata:
  name: ${NAME}
  namespace: default
spec:
  shards: 3
  replicasPerShard: 2
  image: valkey/valkey:8
  storage:
    size: 512Mi
EOF
}

# write_range LO HI — set key:i=val:i for i in [LO,HI] via a cluster-mode client,
# verifying each SET and retrying on transient CLUSTERDOWN.
write_range() {
  ns
  kubectl exec "$SEED" -- sh -c "
    fail=0
    for i in \$(seq $1 $2); do
      ok=0
      for try in 1 2 3 4 5 6 7 8 9 10; do
        out=\$(valkey-cli -c -p 6379 set \"key:\$i\" \"val:\$i\" 2>&1)
        if [ \"\$out\" = OK ]; then ok=1; break; fi
        sleep 0.5
      done
      [ \$ok = 1 ] || { fail=\$((fail+1)); echo \"   !! set key:\$i failed: \$out\"; }
    done
    echo \"wrote $1-$2 (failed=\$fail)\"
  "
}

# read_check HI — read key:1..HI, report hits/misses.
read_check() {
  ns
  kubectl exec "$SEED" -- sh -c "ok=0; miss=0; for i in \$(seq 1 $1); do v=\$(valkey-cli -c -p 6379 get \"key:\$i\"); if [ \"\$v\" = \"val:\$i\" ]; then ok=\$((ok+1)); else miss=\$((miss+1)); fi; done; echo \"READ 1-$1 => ok=\$ok miss=\$miss\""
}

snap() {
  ns
  local sts pods phase masters
  sts=$(kubectl get sts --no-headers 2>/dev/null | grep "$NAME-shard" | sed "s/$NAME-shard-//;s/ .*//" | tr '\n' ',')
  pods=$(kubectl get pods --no-headers 2>/dev/null | grep -c "$NAME-shard")
  phase=$(kubectl get valkeycluster "$NAME" -o jsonpath='{.status.phase}/{.status.readyShards}' 2>/dev/null)
  masters=$(kubectl exec "$SEED" -- valkey-cli -p 6379 cluster nodes 2>/dev/null | awk '$3 ~ /master/ && NF>8 {c++} END{print c+0}')
  echo "   snap: sts=[$sts] pods=$pods masters_with_slots=$masters phase=$phase"
}

# wait_ready DESIRED EXPECT_PODS TIMEOUT_TICKS — poll until Ready/DESIRED with
# DESIRED slot-owning masters and EXPECT_PODS ready pods (12s ticks).
wait_ready() {
  local d=$1 ep=$2 ticks=$3 n pods phase masters
  for ((n=1;n<=ticks;n++)); do
    ns
    pods=$(kubectl get pods --no-headers 2>/dev/null | grep "$NAME-shard" | grep -c "1/1")
    phase=$(kubectl get valkeycluster "$NAME" -o jsonpath='{.status.phase}/{.status.readyShards}' 2>/dev/null)
    masters=$(kubectl exec "$SEED" -- valkey-cli -p 6379 cluster nodes 2>/dev/null | awk '$3 ~ /master/ && NF>8 {c++} END{print c+0}')
    echo "   ...[$n] ready_pods=$pods masters=$masters phase=$phase"
    if [ "$phase" = "Ready/$d" ] && [ "$masters" = "$d" ] && [ "$pods" = "$ep" ]; then echo "   => CONVERGED desired=$d"; return 0; fi
    sleep 12
  done
  echo "   => TIMEOUT waiting for Ready/$d"; return 1
}

patch_shards() { ns; kubectl patch valkeycluster "$NAME" --type=merge -p "{\"spec\":{\"shards\":$1}}" >/dev/null 2>&1; }

echo "############ CLEAN START (context=$CTX, name=$NAME) ############"
ns
kubectl delete valkeycluster "$NAME" --ignore-not-found --wait=true >/dev/null 2>&1
for ((n=1;n<=20;n++)); do [ "$(kubectl get pods --no-headers 2>/dev/null | grep -c "$NAME-shard")" = "0" ] && break; sleep 6; done
echo "cleaned. pods=$(kubectl get pods --no-headers 2>/dev/null | grep -c "$NAME-shard")"

echo "############ PIVOT 0: PROVISION 3 ############"
apply_base
wait_ready 3 9 14 || exit 1
write_range 1 100
read_check 100
snap

echo "############ PIVOT 1: SCALE OUT 3 -> 5 ############"
patch_shards 5
sleep 20
echo "   -- live WRITE during reshard (keys 101-150) --"
write_range 101 150
echo "   -- live READ during reshard --"
read_check 150
wait_ready 5 15 16 || exit 1
echo "   -- post scale-out verify --"
read_check 150
write_range 151 160
read_check 160
snap

echo "############ PIVOT 2: SCALE IN 5 -> 3 ############"
patch_shards 3
sleep 20
echo "   -- live WRITE during scale-in (keys 161-200) --"
write_range 161 200
echo "   -- live READ during scale-in --"
read_check 200
wait_ready 3 9 18 || exit 1
echo "   -- post scale-in verify (ALL 200 keys) --"
read_check 200
write_range 201 210
read_check 210
snap
ns
echo "   -- cluster health --"
kubectl exec "$SEED" -- valkey-cli -p 6379 cluster info 2>/dev/null | grep -E "cluster_state|cluster_slots_ok|cluster_known_nodes"

echo "############ MATRIX COMPLETE ############"
