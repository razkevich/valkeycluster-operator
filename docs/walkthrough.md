# Architecture

The ValkeyCluster operator turns one custom resource into a running, sharded, highly-available Valkey
cluster and keeps the live cluster matching it as you change it. This page is **how it's built**. The
cluster model it relies on (slots, quorum, async replication) is in
[Performance & HA](settings.md); the CR you declare is in the [Overview](index.md).

## What the operator creates

For a `ValkeyCluster`, the operator owns these objects (owner-referenced, so deleting the CR
garbage-collects them):

```mermaid
flowchart LR
  CR["ValkeyCluster CR<br/>shards x replicasPerShard"] -->|desired state| OP["operator"]
  OP -->|owns| CM["ConfigMap<br/>valkey.conf"]
  OP -->|owns| SVC["Headless Service<br/>stable per-pod DNS"]
  OP -->|owns| STS["StatefulSet per shard<br/>shard-0 ... shard-N"]
  STS --> POD["Valkey pods (cluster mode)<br/>PVC per pod"]
  OP -.->|"CLUSTER RPCs + native slot-mover"| POD
```

- **One StatefulSet per shard** (`demo-shard-0`, …), each `1 + replicasPerShard` pods — so topology
  operations map to single objects: adding a shard is adding a StatefulSet.
- **One headless Service** — stable per-pod DNS (client port 6379, cluster-bus 16379).
- **A ConfigMap** — the rendered `valkey.conf`; a content-hash on the pod template rolls the pods when
  the config changes (the only way startup-only settings like `io-threads` take effect).
- **A PVC per pod** — holds `nodes.conf`, so a restarted pod rejoins as the same cluster node.

Two identity rules make it stable on Kubernetes:

- Each pod **announces its stable hostname**, so gossip and `MOVED`/`ASK` redirects survive pod-IP
  changes.
- A shard's pods get **pod anti-affinity** so a replica never shares a node with its primary.

## How the operator talks to Valkey

- **`go-redis`** for inspection and one-shot topology RPCs: `CLUSTER INFO`, `NODES`, `MEET`,
  `ADDSLOTS`, `REPLICATE`, `FORGET`, `FAILOVER`, `MYID`.
- **A native Go slot-mover** (`ClusterAdmin.MoveSlots`) for all slot migration — scale-out and
  scale-in — plus `RepairSlots` to finalize any slot left mid-migration. It's deterministic where
  `valkey-cli --cluster reshard` is not (the CLI refuses on uneven/interrupted distributions).

Two rules that only matter once you run it on Kubernetes:

- `CLUSTER MEET` takes an **IP**, not a hostname — so the operator resolves FQDN→IP for `MEET`, even
  though the node announces its hostname for everything else.
- A shard's primary is found via `CLUSTER MYID` + gossiped slot ownership — the operator **never
  assumes pod ordinal 0**, since after a failover any pod can be the primary.

## The reconcile loop

Reconciliation is **level-triggered** (handed the object, it reads full state and converges — no
events to miss) and **idempotent**. Each pass:

```
1. Ensure infra:   ConfigMap, headless Service, one StatefulSet per shard
2. Readiness gate: not all pods Ready → phase=Provisioning, requeue (don't touch the cluster)
3. Observe:        read live ClusterState via go-redis
4. Decide:         topology.Decide(desired, observed) → one action
5. Act:            Form | Repair | ScaleOut | ScaleIn | ScaleReplicas | (steady) reconcile membership
6. Status:         re-observe and publish phase / conditions / per-shard detail
```

The decision is a pure, I/O-free, unit-tested function:

```go
func Decide(desired Desired, observed Observed) Plan {
    if !observed.Formed         { return Plan{Kind: Form} }
    if !observed.SlotsCovered   { return Plan{Kind: Repair} }   // stability gate
    if desired.Shards > observed.PrimaryCount { return Plan{ScaleOutShards, ...} }
    if desired.Shards < observed.PrimaryCount { return Plan{ScaleInShards, ...} }
    if replicaCountsDiffer(...)               { return Plan{ScaleReplicas, ...} }
    return Plan{Kind: None}
}
```

A "shard" is a **primary that owns slots** — a momentarily-empty master is never miscounted as an
extra shard. How the loop stays correct under crashes, races, and partial failures is in
[Operator Best Practices & Edge Cases](reconcile-edge-cases.md).

## Resharding: move slots, not keys

A key's slot is fixed — `slot = CRC16(key) mod 16384` — and clients route purely by slot → owner, so
you can't relocate data by moving keys: you transfer the **slot**, and its keys follow. Resharding
rewrites the slot → primary map to rebalance when shards are added or removed. It's data-preserving
(keys requested mid-move are redirected to the new owner), and the native mover runs in bounded,
resumable batches — a crash mid-reshard is repaired and resumed on the next reconcile.

## The testability seam

One interface decouples the reconciler from a live cluster:

```go
type ClusterAdmin interface {
    State / Meet / AddSlots / Replicate / Forget / Failover / MyID ...
    MoveSlots / RepairSlots ...
}
```

Production uses a `go-redis` + pod-exec implementation; tests use an in-memory **fake**, so the
reconciler's "given this state, take these actions" logic runs in `envtest` with no live Valkey. Real
Valkey behavior is verified separately in kind e2e — see [Testing & Verification](manual-verification.md).

## Code map

Four layers — the contract, the brain, the hands, and the glue:

```
cmd/main.go              wires the pieces, starts the manager
api/v1alpha1/            CRD types — the contract (spec/status, validation)
internal/
  topology/   (pure)     Decide() — the brain; one action from desired-vs-observed (100% tested)
  slots/      (pure)     hash-slot distribution math
  cluster/               ClusterAdmin: go-redis RPCs + pod-exec + an in-memory fake — the hands
  resources/             StatefulSet / Service / ConfigMap builders
  controller/            the reconcile loop, split by concern:
                           reconcile.go  observe → decide → act
                           scaling.go    scale-out / scale-in + teardown
                           membership.go replica attach, primary discovery, forget
                           status.go     truthful status
```

If you read three: `api/v1alpha1/valkeycluster_types.go` (the API), `internal/topology/topology.go`
(the decision), `internal/cluster/goredis.go` (how it talks to the cluster).

## Built with

- **Go** + **controller-runtime** (Kubebuilder v4 scaffold) — the operator framework.
- **go-redis/v9** — Valkey-wire-compatible `CLUSTER` client for inspection and topology RPCs.
- **valkey-cli** via the Kubernetes **pod-exec** API — for the few cluster operations that are simplest run in-pod.
- **envtest** (a real API server, no kubelet) for controller tests; **kind** + **Ginkgo/Gomega** for e2e.

