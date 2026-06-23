# Phase 1 Data Model: ValkeyCluster

API group/version: `cache.razkevich.dev/v1alpha1`. Kind: `ValkeyCluster` (namespaced).

## Spec

| Field | Type | Default | Validation | Notes |
|-------|------|---------|------------|-------|
| `shards` | int32 | 3 | `== 1 \|\| >= 3` (reject 2); `<= 16` | number of data partitions / primaries |
| `replicasPerShard` | int32 | 1 | `>= 0`, `<= 5` | HA copies per shard |
| `image` | string | `valkey/valkey:8` | non-empty | Valkey image (must include `valkey-cli`) |
| `storage.size` | quantity | `1Gi` | required; **immutable** after create | per-pod PVC size |
| `storage.storageClassName` | string | `""` (cluster default) | optional | |
| `resources` | corev1.ResourceRequirements | `{}` | optional | passed to the valkey container; a memory limit drives `maxmemory` at ~70% |
| `persistence.mode` | enum | `AOF` | `AOF\|RDB\|AOFAndRDB\|None` | drives `appendonly` + `save` |
| `persistence.appendFsync` | enum | `everysec` | `always\|everysec\|no` | `appendfsync` (AOF only) |
| `performance.ioThreads` | int32 | 1 | `1..128` | `io-threads` (Valkey-8 network parallelism) |
| `performance.maxmemoryPolicy` | enum | `noeviction` | `noeviction\|allkeys-lru\|allkeys-lfu\|allkeys-random\|volatile-lru\|volatile-lfu\|volatile-random\|volatile-ttl` | `maxmemory-policy` |
| `haPolicy.minReplicasToWrite` | int32 | 0 | `>= 0` | `min-replicas-to-write` |
| `haPolicy.requireFullCoverage` | bool | true | — | `cluster-require-full-coverage` |
| `haPolicy.clusterNodeTimeoutMillis` | int32 | 5000 | `>= 1000` | `cluster-node-timeout` |

Validation expressed via kubebuilder markers + CEL (`x-kubernetes-validations`) for the
`shards != 2` rule and `storage.size` immutability (`self == oldSelf`).

## Status

| Field | Type | Notes |
|-------|------|-------|
| `phase` | enum | `Pending\|Provisioning\|Forming\|Ready\|Resharding\|ScalingReplicas\|Degraded\|Failed` |
| `observedGeneration` | int64 | generation the status reflects |
| `readyShards` | int32 | shards with a reachable primary + assigned slots |
| `shards[]` | ShardStatus | per-shard observed state |
| `conditions[]` | metav1.Condition | `Available`, `Progressing`, `Degraded` (`listType=map` on `type`) |

`ShardStatus`: `index int32`, `primaryPod string`, `primaryNodeID string`, `slots string`
(e.g. `0-5460`), `readyReplicas int32`, `nodeIDs []string`.

Printer columns: `SHARDS` (`.spec.shards`), `REPLICAS` (`.spec.replicasPerShard`),
`PHASE` (`.status.phase`), `READY` (`.status.readyShards`), `AGE`.

## Derived / internal domain types (`internal/cluster`)

- `NodeInfo`: `ID, Addr, Host, Flags([]string), MasterID, Slots([]SlotRange), Connected bool`
  — parsed from `CLUSTER NODES`.
- `ClusterState`: `Formed bool, SlotsCovered bool, Nodes []NodeInfo` — from `CLUSTER INFO`+`NODES`.
- `DesiredTopology`: `Shards int, ReplicasPerShard int` (from spec).
- `Action`: enum of reconcile actions (MeetNode, AddSlots, Replicate, RebalanceIn, RebalanceOut,
  ForgetNode, FixSlots, FailoverPrimary) produced by `topology.Diff(desired, observed)`.

## Invariants

1. Union of all shard primary slot ranges == `[0,16383]` when `phase == Ready`.
2. Each shard has exactly one primary and `replicasPerShard` replicas when `Ready`.
3. `observedGeneration == metadata.generation` whenever `phase ∈ {Ready, Degraded}`.
4. Every created resource (StatefulSet, Service, ConfigMap, PVC) has an owner reference to the CR.
5. A primary is never deleted before its role is handed off to a replica (scale-in).
