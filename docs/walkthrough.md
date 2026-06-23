# Building a Valkey Cluster Operator for Kubernetes

*How I turned a one-line desired state — "3 shards, 1 replica each" — into a self-healing, resharding, highly-available Valkey cluster, and what I learned doing it spec-first with AI.*

---

## The problem in one sentence

A user should be able to write a few lines of YAML —

```yaml
apiVersion: cache.razkevich.dev/v1alpha1
kind: ValkeyCluster
metadata: { name: demo }
spec:
  shards: 3
  replicasPerShard: 1
```

— `kubectl apply` it, and get a real, sharded, highly-available Valkey cluster. Then they should be able to *change* that YAML (grow shards, add replicas, tune durability) and have the cluster reshape itself **without losing data**. That's an operator: encode the human runbook as software.

This piece walks through how it works, the decisions that mattered, and the bugs that only showed up when I ran it for real.

---

## Part 1 — Why a cluster operator is genuinely hard

Valkey (the open-source Redis fork) Cluster has a specific model, and most of the difficulty is the collision between that model and how Kubernetes treats pods.

**Sharding by hash slots.** The keyspace is split into a fixed **16384 hash slots**; `slot = CRC16(key) mod 16384`. Each *primary* owns a contiguous range of slots; together they cover all 16384. "Sharding" is just *which primary owns which slots*. "Resharding" is *moving slot ownership (and the keys in those slots) between primaries* — which is the operationally hard part.

**Replication and failover.** Each primary has N async *replicas* for HA. Nodes gossip over a separate "cluster bus" port (16379). If a primary stops answering for `cluster-node-timeout`, a **majority of primaries** must agree it's dead before a replica promotes itself — which is exactly why you need **≥3 primaries** (with 2, there's no majority when one dies). Replication is **asynchronous**, so the system is AP-leaning: an acknowledged write can be lost if a primary dies before its replica sees it. That's not a bug, it's a tunable tradeoff — and being able to say that precisely is half the point of the exercise.

**Why Kubernetes makes it hard.** Everything above assumes nodes with stable identities. Pods don't have that:

- Pods need stable storage and DNS → **StatefulSet** + **headless Service**.
- **Pod IPs change on restart**, but a Valkey node *announces an address* to the cluster and to clients. Announce the ephemeral pod IP and the cluster fragments on the first restart. The fix (Redis 7+/Valkey): announce a stable **hostname** (`cluster-announce-hostname` + `cluster-preferred-endpoint-type hostname`).
- **Scaling ≠ adding pods.** Adding a shard means resharding slots onto it; removing one means draining its slots *first*. Naively resizing a StatefulSet corrupts coverage and loses data.
- **Two failover layers** interact: Valkey fails over to a replica in seconds, *independently* of Kubernetes rescheduling the dead pod. When the pod comes back, it must rejoin as a replica of the new primary.

The operator exists to make all of that converge automatically.

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

There were two defensible layouts: one StatefulSet for the whole cluster, or **one StatefulSet per shard**. I chose per-shard because it makes the topology *legible*: adding a shard is adding a StatefulSet, scaling replicas is changing one StatefulSet's size, and resharding has a clean unit to operate on. Each shard gets pods `demo-shard-0-0`, `demo-shard-0-1`, … behind a single headless Service, with anti-affinity so a shard's replicas don't land on the same node as its primary (otherwise one node failure takes out the whole shard — HA in name only).

Crucially, the operator **never assumes the primary is ordinal 0**. After a failover, any pod can be the primary, so roles are always read live from `CLUSTER NODES` / `CLUSTER MYID`.

### How the operator talks to Valkey: a hybrid that evolved

- **`go-redis`** for inspection and one-shot topology commands: `CLUSTER INFO`, `NODES`, `MEET`, `ADDSLOTS`, `REPLICATE`, `FORGET`, `FAILOVER`, `MYID`.
- **`valkey-cli --cluster reshard`** (pod-exec) for **scale-out** slot/key migration — a *targeted* reshard to specific new-primary IDs.
- **A native Go slot-mover** (`ClusterAdmin.MoveSlots`) for **scale-in** drain, and `ClusterAdmin.RepairSlots` for open-slot finalization.

The split started simpler — *reuse `valkey-cli --cluster` for all migration; don't reimplement a fiddly `SETSLOT IMPORTING/MIGRATING` → `MIGRATE` → `SETSLOT NODE` loop in Go.* That holds for scale-out. But live testing of **scale-in** showed `valkey-cli`'s drain to be non-deterministic — pre-check refusals, `BUSYKEY`, and timeouts would wedge a shrink mid-flight. So the drain path was reimplemented natively: idempotent `MIGRATE … REPLACE` by IP, masters-only `SETSLOT NODE`, in bounded batches. The lesson is the senior move — *start by reusing battle-tested tooling, but measure it on the real path and replace it where it can't give you determinism.* (See Part 5.)

---

## Part 3 — The reconcile loop

The heart of any operator. Kubernetes is **level-triggered**: `Reconcile` isn't told *what changed* — it's told *"reconcile this object,"* looks at the current full state, and converges. So missed events can't break you; there are no events, only state. And it must be **idempotent** — safe to run a hundred times.

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

A subtle but important detail: a "shard" is counted as a **primary that owns slots**, not just any master. A replica pod that briefly appears as an empty master must *not* be mistaken for an extra shard (that bug bit me — see Part 5).

### The testability seam

The thing that lets most of this be tested without a live Valkey is one interface:

```go
type ClusterAdmin interface {
    State(ctx, seed) (ClusterState, error)
    Meet / AddSlots / Replicate / Forget / Failover / MyID ...
    Rebalance / Reshard / Fix ...   // valkey-cli via pod-exec
}
```

Production uses a `go-redis` + pod-exec implementation; tests use an in-memory **fake**. So the reconciler's "given this cluster state, issue these actions in this order" logic runs in `envtest` (a real API server, no kubelet) against the fake — fast and deterministic. Real Valkey behavior is then proven separately in kind e2e.

---

## Part 4 — Day-two operations (the actual assignment)

**Provision.** Apply the CR → operator creates the StatefulSets → waits for pods Ready → forms the cluster: `MEET` all nodes, split 16384 slots across the shard primaries, `REPLICATE` the replicas onto their primary. Status goes `Ready`.

**Failover (automatic).** Kill a primary pod. Valkey's own gossip promotes a replica within seconds — the operator doesn't implement the algorithm, it just keeps the workloads alive and reflects the new roles. The killed pod returns (StatefulSet) and the operator ensures it rejoins as a replica. *Verified live: killed a primary, a replica promoted, all 50 test keys intact.*

**Data-preserving resharding (the hard one).** Change `shards: 3 → 5`. The operator joins the new shard primaries, then runs a **targeted** `valkey-cli --cluster reshard` to move each new primary its fair share of slots — and the keys in them — off the existing primaries, then attaches the new replicas. Shrinking (`5 → 3`) drains the departing shards' slots onto survivors *first*, then forgets the nodes and reclaims their PVCs. *Verified live: 3→5 with 200 keys written beforehand → exactly 5 primaries, all slots covered, 200/200 keys preserved.*

**Replica scaling.** Change `replicasPerShard` → the operator resizes each shard's StatefulSet and attaches/forgets replicas, no keyspace movement.

**Self-healing.** Delete a StatefulSet out of band → the operator recreates it and re-forms. Interrupt the operator mid-reshard → on restart it sees the open slots, runs `RepairSlots`, and converges. No manual repair, no data loss.

---

## Part 5 — Three bugs that only showed up live

This is the part worth telling, because it's where understanding (not tutorials) shows. The unit and envtest suites were green, but the *first run on a real kind cluster* surfaced three real bugs — each a genuine Valkey-on-Kubernetes gotcha:

1. **`CLUSTER MEET` rejects hostnames.** Forming failed with `ERR Invalid node address`. `MEET` needs an *IP*, not a DNS name — even though we *want* nodes to advertise their stable hostname. Fix: resolve the pod FQDN → IP for the `MEET` call, while the node still announces its hostname (so gossip and client redirects keep working across restarts). The "announce hostname, meet by IP" distinction is the detail that separates reading-a-blog from running-this.

2. **Replicas silently didn't attach.** All six nodes came up as empty masters. The cause: I'd looked up the shard primary by its announced *hostname*, but right after `MEET` the hostname hasn't gossiped yet (peers still see it by IP). Fix: detect each shard's primary by dialing its pods directly (`CLUSTER MYID`) and matching against the reliably-gossiped slot ownership — robust to both the gossip lag and to failover (where the primary isn't ordinal 0).

3. **Resharding produced 7 primaries instead of 5.** `valkey-cli --cluster rebalance --use-empty-masters` hands slots to *every* empty master — including replica pods that were momentarily empty masters before being attached. Fix: replace it with a **targeted** reshard to specific new-primary node IDs, so replica pods never accidentally become shards. (Data was preserved throughout; this was a topology-correctness bug.)

The lesson I'd give the interviewer: a clustering operator's correctness lives in the gaps between "it compiled" and "it survived a real failover/reshard." Live verification wasn't a formality — it found three things no unit test would.

---

## Part 6 — The clustering / HA tradeoffs (with measurements)

The exercise specifically asks whether you understand the *settings and their performance tradeoffs*. The operator exposes them as the three axes above, and the benchmarks quantify them:

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
- **kind e2e** — the real thing: provision + cross-shard read/write, failover with data intact, reshard 3→5 with 200 keys preserved, replica scaling. All four pass.

**Built spec-first with GitHub Spec Kit.** The exercise explicitly rewards "AI development methodologies," so the process is a first-class, committed artifact: a `constitution.md` (five principles incl. test-first and "truthful status"), then `spec.md` (WHAT/WHY, 26 requirements), `plan.md` + `research.md` + `data-model.md` + `contracts/` (the HOW), `tasks.md` (47 tasks), then implementation. A standout step: before finalizing the spec, I cross-checked it against **KubeBlocks' real Redis addon source** to find the operational requirements a working cluster needs (hostname announce, `nodes.conf` on the PVC, drain-before-remove, single-actor bootstrap) — and folded the gaps back into the spec. The methodology wasn't "the AI wrote it"; it was leverage on scaffolding/boilerplate plus human judgment on the correctness-critical parts (the consistency tradeoffs, migrate-before-remove ordering, the three live bugs).

---

## Part 8 — What I deliberately left out

Scope discipline is part of the signal. Out of scope, with reasons:

- **Rolling version upgrades, vertical scaling, volume expansion, dynamic reconfigure** — valuable, but each is a day-two feature orthogonal to the clustering core; I cut them to do sharding/replication/resharding *correctly*.
- **Backup/restore, TLS/ACL/auth, Sentinel mode, a proxy, external (non-cluster-aware-client) access** — documented non-goals. Notably, **Sentinel was unnecessary**: cluster-mode's built-in failover covers HA, and `shards: 1` *is* the replication-only / HA case, so one engine serves both.
- **Prometheus/metrics/UI** — `kubectl` status is the monitoring surface by choice.

A smaller scope done correctly and understood deeply beats an ambitious half-built operator — and it's easier to defend in an interview.

---

## How to talk about it in 60 seconds

> "It's a Kubernetes operator for Valkey Cluster. You declare a topology — shards and replicas-per-shard — and it reconciles a real sharded, HA cluster to match: one StatefulSet per shard, hostname-based identity so it survives pod restarts, and data-preserving resharding when you change the topology. Valkey handles failover; the operator handles provisioning, slot migration, drift repair, and truthful status. I drive `CLUSTER` commands with go-redis and reuse `valkey-cli --cluster` for the migration loop rather than reinventing it. The interesting tradeoffs — async-replication data loss, `cluster-node-timeout`, persistence modes, io-threads — are exposed as tunable knobs and quantified in a benchmark. I built it spec-first with Spec Kit, cross-checked the design against KubeBlocks, and the three bugs worth mentioning all surfaced only when I ran it on a real cluster: MEET-needs-an-IP, replica attachment under gossip lag, and rebalance-grabbing-empty-masters during scale-out."

---

*Repo layout, runnable commands, and the full spec/plan/tasks trail are in the repo (`try/README.md` to drive it, `docs/` for the runbook and tradeoffs, `specs/` for the spec-kit artifacts).*
