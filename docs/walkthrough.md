# How the ValkeyCluster Operator Works

*A Kubernetes operator that turns a one-line desired state — "3 shards, 1 replica each" — into a self-healing, resharding, highly-available Valkey cluster.*

---

## The problem in one sentence

A user writes a few lines of YAML —

```yaml
apiVersion: cache.razkevich.dev/v1alpha1
kind: ValkeyCluster
metadata: { name: demo }
spec:
  shards: 3
  replicasPerShard: 1
```

— `kubectl apply`s it, and gets a real, sharded, highly-available Valkey cluster. They can then *change* that YAML (grow shards, add replicas, tune durability) and the cluster reshapes itself **without losing data**. That's what the operator does: it encodes the human runbook as software.

This document walks through how it works and the decisions that shape it.

---

## Part 1 — Why a cluster operator is genuinely hard

Valkey (the open-source Redis fork) Cluster has a specific model, and most of the difficulty is the collision between that model and how Kubernetes treats pods.

**Sharding by hash slots.** The keyspace is split into a fixed **16384 hash slots**; `slot = CRC16(key) mod 16384`. Each *primary* owns a contiguous range of slots; together they cover all 16384. "Sharding" is just *which primary owns which slots*. "Resharding" is *moving slot ownership (and the keys in those slots) between primaries* — which is the operationally hard part.

**Replication and failover.** Each primary has N async *replicas* for HA. Nodes gossip over a separate "cluster bus" port (16379). If a primary stops answering for `cluster-node-timeout`, a **majority of primaries** must agree it's dead before a replica promotes itself — which is exactly why you need **≥3 primaries** (with 2, there's no majority when one dies). Replication is **asynchronous**, so the system is AP-leaning: an acknowledged write can be lost if a primary dies before its replica sees it. That's not a bug, it's a tunable tradeoff.

**Why Kubernetes makes it hard.** Everything above assumes nodes with stable identities. Pods don't have that:

- Pods need stable storage and DNS → **StatefulSet** + **headless Service**.
- **Pod IPs change on restart**, but a Valkey node *announces an address* to the cluster and to clients. Announcing the ephemeral pod IP fragments the cluster on the first restart. The fix (Redis 7+/Valkey): announce a stable **hostname** (`cluster-announce-hostname` + `cluster-preferred-endpoint-type hostname`).
- **Scaling ≠ adding pods.** Adding a shard means resharding slots onto it; removing one means draining its slots *first*. Naively resizing a StatefulSet corrupts coverage and loses data.
- **Two failover layers** interact: Valkey fails over to a replica in seconds, *independently* of Kubernetes rescheduling the dead pod. When the pod comes back, it must rejoin as a replica of the new primary.

The operator makes all of that converge automatically.

---

## Part 2 — The shape of the solution

### One CRD, three axes of tuning

The `ValkeyCluster` custom resource is the entire API. Topology is two numbers; everything else is optional tuning grouped into three axes — **persistence**, **HA**, and **performance** — each a real Valkey dial with a real tradeoff:

```yaml
spec:
  shards: 3                 # 1 (HA-only) or >=3 (failover quorum)
  replicasPerShard: 1
  image: valkey/valkey:8
  storage: { size: 1Gi }
  resources: { limits: { memory: 512Mi } }   # drives maxmemory @ ~70%
  persistence:
    mode: AOFAndRDB          # AOF | RDB | AOFAndRDB | None
    appendFsync: everysec    # always | everysec | no
  performance:
    ioThreads: 2             # Valkey-8 network parallelism
    maxmemoryPolicy: noeviction   # datastore vs. cache (lru/lfu)
  haPolicy:
    minReplicasToWrite: 1    # refuse writes if no replica is in sync
    requireFullCoverage: true
    clusterNodeTimeoutMillis: 5000
```

The `status` subresource reports the truth, derived from the live cluster every reconcile: a `phase` (`Provisioning → Forming → Ready / Resharding / Degraded`), per-shard primary + slot range + ready replicas, and standard conditions. `kubectl get valkeycluster` is the monitoring surface — no extra metrics stack required.

### The big architecture decision: one StatefulSet per shard

The operator uses **one StatefulSet per shard** (rather than one StatefulSet for the whole cluster). This makes the topology *legible*: adding a shard is adding a StatefulSet, scaling replicas is changing one StatefulSet's size, and resharding has a clean unit to operate on. Each shard gets pods `demo-shard-0-0`, `demo-shard-0-1`, … behind a single headless Service, with anti-affinity so a shard's replicas don't land on the same node as its primary (otherwise one node failure takes out the whole shard — HA in name only).

Crucially, the operator **never assumes the primary is ordinal 0**. After a failover, any pod can be the primary, so roles are always read live from `CLUSTER NODES` / `CLUSTER MYID`.

### How the operator talks to Valkey

The operator uses a hybrid of three mechanisms, each matched to what it does best:

- **`go-redis`** for inspection and one-shot topology commands: `CLUSTER INFO`, `NODES`, `MEET`, `ADDSLOTS`, `REPLICATE`, `FORGET`, `FAILOVER`, `MYID`.
- **`valkey-cli --cluster reshard`** (pod-exec) for **scale-out** slot/key migration — a *targeted* reshard to specific new-primary node IDs.
- **A native Go slot-mover** (`ClusterAdmin.MoveSlots`) for **scale-in** drain: `SETSLOT IMPORTING/MIGRATING` → `MIGRATE … REPLACE` (by IP) → masters-only `SETSLOT NODE`, in bounded batches. `MIGRATE` is idempotent with `REPLACE`, so the drain is safe to retry.
- **`ClusterAdmin.RepairSlots`** for open-slot finalization — closing slots left in an intermediate `IMPORTING`/`MIGRATING` state.

The native scale-in mover gives the shrink path determinism: it sidesteps `valkey-cli`'s drain pre-check refusals, `BUSYKEY` errors, and timeouts that can wedge a shrink mid-flight, since it issues `MIGRATE … REPLACE` by IP and finalizes ownership only on masters.

---

## Part 3 — The reconcile loop

The heart of the operator. Kubernetes is **level-triggered**: `Reconcile` isn't told *what changed* — it's told *"reconcile this object,"* looks at the current full state, and converges. So missed events can't break it; there are no events, only state. And it is **idempotent** — safe to run a hundred times.

The loop, simplified:

```
1. Ensure infra:     ConfigMap (rendered valkey.conf), headless Service, one StatefulSet per shard
2. Readiness gate:   if not all shard pods Ready → phase=Provisioning, requeue (don't touch the cluster yet)
3. Observe:          read live ClusterState via go-redis (CLUSTER INFO + NODES)
4. Decide:           topology.Decide(desired, observed) → one action
5. Act:              Form | Repair(fix) | ScaleOutShards | ScaleInShards | ScaleReplicas | (steady) reconcile membership
6. Status:           re-observe, derive phase/conditions/per-shard from the live cluster
```

The decision logic is a **pure function** — no I/O, fully unit-tested:

```go
func Decide(desired Desired, observed Observed) Plan {
    if !observed.Formed         { return Plan{Kind: Form} }
    if !observed.SlotsCovered   { return Plan{Kind: Repair} }              // stability gate
    if desired.Shards > observed.PrimaryCount { return Plan{ScaleOutShards, ...} }
    if desired.Shards < observed.PrimaryCount { return Plan{ScaleInShards, ...} }
    if replicaCountsDiffer(...)               { return Plan{ScaleReplicas, ...} }
    return Plan{Kind: None}
}
```

A subtle but important detail: a "shard" is counted as a **primary that owns slots**, not just any master. A replica pod that briefly appears as an empty master must *not* be mistaken for an extra shard.

### The testability seam

The thing that lets most of this be tested without a live Valkey is one interface:

```go
type ClusterAdmin interface {
    State(ctx, seed) (ClusterState, error)
    Meet / AddSlots / Replicate / Forget / Failover / MyID ...
    Reshard / MoveSlots / RepairSlots ...   // scale-out reshard (valkey-cli),
                                            // scale-in drain + open-slot finalize (native Go)
}
```

Production uses a `go-redis` + pod-exec implementation; tests use an in-memory **fake**. So the reconciler's "given this cluster state, issue these actions in this order" logic runs in `envtest` (a real API server, no kubelet) against the fake — fast and deterministic. Real Valkey behavior is proven separately in kind e2e.

---

## Part 4 — Day-two operations

**Provision.** Apply the CR → operator creates the StatefulSets → waits for pods Ready → forms the cluster: `MEET` all nodes, split 16384 slots across the shard primaries, `REPLICATE` the replicas onto their primary. Status goes `Ready`.

**Failover (automatic).** A primary that is unreachable beyond `cluster-node-timeout` is replaced: Valkey's own gossip promotes one of its replicas within seconds. The operator doesn't implement the algorithm — it keeps the workloads alive and reflects the new roles, then ensures the returned pod rejoins as a replica of the new primary. A *quick* pod delete usually brings the same node back before the timeout window elapses; because node identity persists in `nodes.conf` on the PVC, it resumes as primary with no failover at all. A primary loss promotes a replica with no data loss; this is covered by the kind e2e suite.

**Data-preserving resharding.** Changing `shards: 3 → 5` joins the new shard primaries, then runs a **targeted** `valkey-cli --cluster reshard` to move each new primary its fair share of slots — and the keys in them — off the existing primaries, then attaches the new replicas. Shrinking (`5 → 3`) drains the departing shards' slots onto survivors *first* with the native slot-mover, then forgets the nodes and deletes their StatefulSet + PVCs (teardown is driven by which StatefulSets exist). Both 3→5 and 5→3 preserve all keys.

**Replica scaling.** Changing `replicasPerShard` resizes each shard's StatefulSet and attaches/forgets replicas, with no keyspace movement.

**Self-healing.** Deleting a StatefulSet out of band triggers the operator to recreate it and re-form. Interrupting the operator mid-reshard leaves open slots; on restart it detects them, runs `RepairSlots`, and converges. No manual repair, no data loss.

---

## Part 5 — Valkey-on-Kubernetes details that matter

These three details are where running a clustering operator differs from reading about one. Each is a genuine Valkey-on-Kubernetes gotcha that the operator handles by design:

1. **`CLUSTER MEET` rejects hostnames.** `MEET` needs an *IP*, not a DNS name — even though nodes should advertise their stable hostname. The operator resolves the pod FQDN → IP for the `MEET` call while each node still announces its stable hostname (`cluster-announce-hostname`) for gossip and client redirects, so identity survives restarts. "Announce hostname, meet by IP" is the distinction that keeps forming reliable.

2. **A node's announced hostname hasn't gossiped right after MEET.** Peers still see a freshly-met node by IP, so looking up a shard primary by its announced hostname is unreliable in that window. The operator discovers each shard's primary by dialing pods directly (`CLUSTER MYID`) and matching against the reliably-gossiped slot ownership — robust to both gossip lag and to failover, where the primary isn't ordinal 0.

3. **Scale-out uses a targeted reshard, not `rebalance --use-empty-masters`.** `rebalance --use-empty-masters` hands slots to *every* empty master — including replica pods that are momentarily empty masters before being attached, which would turn them into spurious shards. The operator instead reshards to specific new-primary node IDs, so momentarily-empty replica pods are never handed slots.

---

## Part 6 — The clustering / HA tradeoffs (with measurements)

The operator exposes the consistency and performance settings as the three axes above, and the benchmarks quantify them:

- **Sharding scales writes; replication buys availability.** More shards = more aggregate write throughput + capacity; more replicas = HA and read-scaling, but no extra write throughput (every replica holds the full shard). The benchmark shows throughput rising with shard count.

- **`cluster-node-timeout`: failover speed vs. false positives.** A sweep (kill a primary, measure the write-availability window): **15000ms → ~2.4s window, 5000ms → ~1.6s**. Lower is faster but, under network jitter or GC pauses, more prone to *spurious* failovers. (Graceful pod deletion short-circuits the timeout via connection close; node-timeout governs the silent-partition worst case.)

- **Durability vs. latency.** Plain `SET` vs. `SET` + `WAIT 1` (block until a replica acks): **~3.3s vs ~5.4s for 2000 writes**. `WAIT` shrinks the data-loss window at a latency cost — and it's *not* linearizable, just narrower.

- **Persistence.** AOF `everysec` (lose ≤1s) vs. `always` (fsync every write) vs. RDB snapshots (compact, fast restart, lose-since-snapshot, fork-heavy) vs. None (pure cache). Exposed via `persistence.mode`.

- **Memory safety.** `maxmemory` is derived at **70% of the container limit** — headroom for the copy-on-write fork during RDB/AOF saves, which can otherwise double memory under write load and trigger an OOMKill. `maxmemory-policy` toggles datastore (`noeviction`) vs. cache (`allkeys-lfu`, which beats LRU on skewed access).

- **`io-threads`** (Valkey 8): network I/O fans out across cores while command execution stays single-threaded — the headline Valkey-vs-Redis lever, exposed and benchmarkable.

---

## Part 7 — Testing & methodology

**Three test layers, weighted toward the day-two scenarios:**
- **Unit** — the pure logic: slot distribution, the `Decide` planner, config rendering, status derivation. (topology 100%, resources 95% coverage.)
- **envtest** — the reconciler against a real API server with the fake `ClusterAdmin`: asserts the right objects, owner refs, status, and action sequences.
- **kind e2e** — the real thing: provision + cross-shard read/write, failover with data intact, reshard 3→5 with 200 keys preserved, scale-in 5→3 with all keys preserved, and replica scaling. All five scenarios pass.

**Built spec-first with GitHub Spec Kit.** The process is a first-class, committed artifact: a `constitution.md` (five principles incl. test-first and "truthful status"), then `spec.md` (WHAT/WHY, 26 requirements), `plan.md` + `research.md` + `data-model.md` + `contracts/` (the HOW), `tasks.md` (47 tasks), then implementation. The spec is grounded against **KubeBlocks' real Redis addon source** for the operational requirements a working cluster needs — hostname announce, `nodes.conf` on the PVC, drain-before-remove, single-actor bootstrap. AI provides leverage on scaffolding and boilerplate; human judgment owns the correctness-critical parts (the consistency tradeoffs, migrate-before-remove ordering, the Valkey-on-Kubernetes details in Part 5).

---

## Part 8 — Deliberate non-goals

Scope is bounded on purpose. Out of scope, with reasons:

- **Rolling version upgrades, vertical scaling, volume expansion, dynamic reconfigure** — valuable, but each is a day-two feature orthogonal to the clustering core; left out to do sharding/replication/resharding *correctly*.
- **Backup/restore, TLS/ACL/auth, Sentinel mode, a proxy, external (non-cluster-aware-client) access** — documented non-goals. Notably, **Sentinel is unnecessary**: cluster-mode's built-in failover covers HA, and `shards: 1` *is* the replication-only / HA case, so one engine serves both.
- **Prometheus/metrics/UI** — `kubectl` status is the monitoring surface by choice.

A smaller scope done correctly and understood deeply beats an ambitious half-built operator.

---

## In short

> It's a Kubernetes operator for Valkey Cluster. You declare a topology — shards and replicas-per-shard — and it reconciles a real sharded, HA cluster to match: one StatefulSet per shard, hostname-based identity so it survives pod restarts, and data-preserving resharding when you change the topology. Valkey handles failover; the operator handles provisioning, slot migration, drift repair, and truthful status. It drives `CLUSTER` commands with go-redis, runs `valkey-cli --cluster reshard` for scale-out slot migration, and uses a native Go slot-mover for the scale-in drain. The interesting tradeoffs — async-replication data loss, `cluster-node-timeout`, persistence modes, io-threads — are exposed as tunable knobs and quantified in a benchmark. It's built spec-first with Spec Kit and cross-checked against KubeBlocks.

---

*Repo layout, runnable commands, and the full spec/plan/tasks trail are in the repo (`try/README.md` to drive it, `docs/` for the runbook and tradeoffs, `specs/` for the spec-kit artifacts).*
