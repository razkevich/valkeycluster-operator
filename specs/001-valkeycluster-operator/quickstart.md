# Quickstart / Validation Guide: ValkeyCluster Operator

Runnable scenarios that prove the feature end-to-end on a local **kind** cluster. Commands assume
the repo root and the `valkeycluster-dev` kind cluster (`make kind-create`).

## Prerequisites

```bash
make kind-create                 # 1 control-plane + 2 workers (hack/kind-cluster.yaml)
make kind-deploy IMG=valkeycluster-operator:dev   # build, load, install CRD, deploy operator
kubectl -n valkeycluster-system get deploy         # operator Running
```

## Scenario 1 — Provision (US1)

```bash
kubectl apply -f config/samples/cache_v1alpha1_valkeycluster.yaml   # shards:3 replicasPerShard:1
kubectl wait --for=jsonpath='{.status.phase}'=Ready valkeycluster/demo --timeout=300s
kubectl get valkeycluster demo            # SHARDS=3 REPLICAS=1 PHASE=Ready READY=3
```
**Use the cluster** (cluster-aware client, in-cluster):
```bash
kubectl run vk -it --rm --image=valkey/valkey:8 --restart=Never -- \
  valkey-cli -c -h demo-shard-0-0.demo-nodes set foo bar     # -c follows MOVED across shards
# then GET foo from a different node and confirm the value
```
**Expected**: writes spanning shards succeed; `valkey-cli --cluster check` reports 16384 slots covered.

## Scenario 2 — Failover (US2)

```bash
# find the primary of shard 0, delete its pod
kubectl delete pod demo-shard-0-0
# within ~30s a replica is promoted; data intact; status reflects new primary
kubectl get valkeycluster demo -o jsonpath='{.status.shards[0].primaryPod}'
```
**Expected**: affected slots resume serving without operator action; previously written keys readable.

## Scenario 3 — Data-preserving resharding (US3)

```bash
# write a known keyset, then grow shards 3 -> 5
kubectl patch valkeycluster demo --type merge -p '{"spec":{"shards":5}}'
kubectl wait --for=jsonpath='{.status.phase}'=Ready valkeycluster/demo --timeout=600s
# verify every previously written key is still readable and slots now span 5 shards
```
**Expected**: `phase` passes through `Resharding`; all prior keys readable; 5 shards cover the keyspace.

## Scenario 4 — Replica scaling

```bash
kubectl patch valkeycluster demo --type merge -p '{"spec":{"replicasPerShard":2}}'
# each shard gains a replica; no slot redistribution
```

## Scenario 5 — Teardown

```bash
kubectl delete valkeycluster demo
kubectl get statefulset,svc,cm,pvc -l app.kubernetes.io/instance=demo   # all gone
```

## Benchmark (trade-off evidence)

```bash
bench/benchmark.sh demo          # sweeps shard counts; emits a markdown throughput/latency table
```

## Automated equivalents

- Unit: `go test ./internal/...`
- Controller/envtest: `make test`
- E2E (kind): `make test-e2e`
