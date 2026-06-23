# Day-2 Operations

How to operate a `ValkeyCluster` after it is running. All examples assume the operator is
installed (`make deploy` / `make kind-deploy`) and you have a cluster named `demo`.

## Observe state (the monitoring surface is `kubectl`)

```bash
kubectl get valkeyclusters
# NAME   SHARDS   REPLICAS   PHASE   READY   AGE
# demo   3        1          Ready   3       5m

kubectl describe valkeycluster demo        # conditions + per-shard primary/slots/replicas
kubectl get valkeycluster demo -o jsonpath='{.status.shards}' | jq
```

`phase` is derived from the live cluster every reconcile:

| Phase | Meaning |
|-------|---------|
| `Provisioning` | StatefulSets created, waiting for pods to become ready |
| `Forming` | bootstrapping the cluster (meet + slot assignment + replicas) |
| `Resharding` | a topology change is migrating slots |
| `ScalingReplicas` | adding/removing HA copies |
| `Ready` | all shards serving, 100% of the keyspace covered |
| `Degraded` | a shard has no reachable primary, or coverage is incomplete |

Conditions: `Available` (true when fully serving), `Progressing`, `Degraded`.

## Connect a client (in-cluster, cluster-aware)

Clients must be **cluster-aware** (follow `MOVED`/`ASK` redirects). Seed them with the headless
service DNS; any pod works as an entry point.

```bash
kubectl run vk -it --rm --image=valkey/valkey:8 --restart=Never -- \
  valkey-cli -c -h demo-shard-0-0.demo-nodes set foo bar     # -c = cluster mode
```

**Retry on `CLUSTERDOWN`.** With the default `requireFullCoverage: true`, a node rejects *all*
commands while any slot is uncovered from its view — a brief window during initial **formation**
and mid-**resharding**, before coverage is gossiped cluster-wide. This is expected; a production
client should retry with backoff (most cluster clients do by default). A non-retrying script that
writes the instant `phase` first reads `Ready` can see those first writes bounce.

## Scale replicas (HA copies per shard)

```bash
kubectl patch valkeycluster demo --type merge -p '{"spec":{"replicasPerShard":2}}'
```
Each shard gains/loses a replica. No keyspace redistribution. Watch `status.shards[*].readyReplicas`.

## Reshard (change the number of shards) — data preserving

```bash
kubectl patch valkeycluster demo --type merge -p '{"spec":{"shards":5}}'   # grow
kubectl patch valkeycluster demo --type merge -p '{"spec":{"shards":3}}'   # shrink
```
- `shards` must be `1` or `≥3` (2 is rejected — no failover quorum).
- The operator migrates hash slots (and their keys) to the new layout; **previously written keys
  are preserved**. A brief unavailability of affected slots during migration is expected.
- Watch it pass through `Resharding` back to `Ready`:
  ```bash
  kubectl get valkeycluster demo -w
  ```

## Observe / drive failover

Failover is automatic (Valkey cluster gossip promotes a replica). To exercise it:

```bash
kubectl delete pod demo-shard-0-0          # kill a primary
kubectl get valkeycluster demo -o jsonpath='{.status.shards[0].primaryPod}'
```
Promotion is gated by `cluster-node-timeout`: a replica is promoted only after the primary is
unreachable for longer than that window. A quick `kubectl delete pod` usually brings the **same**
node back (its identity persists in `nodes.conf` on the PVC) before the timeout elapses, so it
**resumes as primary** with no failover — no needless data movement. A genuine longer outage
(node failure) promotes a replica; the old primary then rejoins as a **replica**. To force a real
failover for testing, keep the primary down past the timeout (e.g. `kubectl exec demo-shard-0-0 --
valkey-cli debug sleep <seconds>` longer than `clusterNodeTimeoutMillis`).

Inspect roles directly:
```bash
kubectl exec demo-shard-0-0 -- valkey-cli --cluster check 127.0.0.1:6379
```

## Tune the HA / consistency policy

Edit `spec.haPolicy` / `spec.persistence` / `spec.performance` and re-apply; the operator re-renders
`valkey.conf` and rolls the pods. Each field, its underlying Valkey directive, and its trade-off are
documented in [Settings for Performance and High Availability](./settings.md).

## Teardown

```bash
kubectl delete valkeycluster demo
```
The finalizer reclaims the PVCs; owner references garbage-collect the StatefulSets, Service, and
ConfigMap.

## Troubleshooting

```bash
kubectl -n valkeycluster-system logs deploy/valkeycluster-controller-manager   # operator logs
kubectl exec demo-shard-0-0 -- valkey-cli --cluster check 127.0.0.1:6379       # slot coverage
kubectl exec demo-shard-0-0 -- valkey-cli cluster nodes                        # raw membership
```
- **Stuck in `Provisioning`**: pods not Ready — check `kubectl get pods -l app.kubernetes.io/instance=demo` and pod events (image pull, scheduling, PVC binding).
- **`Degraded`**: a shard lost its only node (`replicasPerShard: 0`) or a migration was interrupted; the operator runs `RepairSlots` (open-slot finalization) and retries automatically.
