# Contract: ValkeyCluster custom resource (user-facing)

The user's API. Apply with `kubectl apply -f`. See [data-model.md](../data-model.md) for full field
table and validation.

## Minimal

```yaml
apiVersion: cache.razkevich.dev/v1alpha1
kind: ValkeyCluster
metadata:
  name: demo
spec:
  shards: 3
  replicasPerShard: 1
```

## Full

```yaml
apiVersion: cache.razkevich.dev/v1alpha1
kind: ValkeyCluster
metadata:
  name: demo
spec:
  shards: 3                 # 1 (HA-only) or >=3 (sharded); 2 is rejected
  replicasPerShard: 1
  image: valkey/valkey:8
  storage:
    size: 1Gi               # immutable after create
    storageClassName: ""    # "" = default StorageClass
  resources:
    requests: { cpu: 100m, memory: 128Mi }
  haPolicy:
    minReplicasToWrite: 0       # durability vs write-availability
    requireFullCoverage: true   # availability vs correctness
    appendFsync: everysec       # always | everysec | no
    clusterNodeTimeoutMillis: 5000
```

## Behavioral contract

- **Create** → operator provisions `shards × (1+replicasPerShard)` nodes, forms one cluster covering
  100% of slots, attaches replicas; `status.phase` → `Ready`.
- **Edit `shards`** → `Resharding`; slots+keys redistributed, data preserved; back to `Ready`.
- **Edit `replicasPerShard`** → `ScalingReplicas`; replicas added/removed; no slot redistribution.
- **Edit `storage.size`** → rejected by admission.
- **Delete** → all owned resources (StatefulSets, Services, ConfigMap, PVCs) garbage-collected.
- **Observe** → `kubectl get valkeyclusters` (columns) and `kubectl describe` (conditions + per-shard).

## Status example

```yaml
status:
  phase: Ready
  readyShards: 3
  shards:
    - index: 0
      primaryPod: demo-shard-0-0
      slots: "0-5460"
      readyReplicas: 1
  conditions:
    - type: Available
      status: "True"
```
