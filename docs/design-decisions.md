# Design Decisions & How It Differs from KubeBlocks

Every choice here serves one goal: do a single engine well instead of building a platform. That
framing is also the whole answer to "why not just use KubeBlocks?"

## The decisions that matter

**One CRD, two numbers.** You declare `shards` and `replicasPerShard`; the rest is optional tuning.
The entire API fits on a screen — no definition/instance/ops split to learn.

**One StatefulSet per shard.** A shard is exactly one workload, so "add a shard" is "add a
StatefulSet" and resharding has a clean unit to act on. One StatefulSet for the whole cluster makes
the ordinal→shard mapping fall apart the moment you reshard.

**A pure decision function.** The loop is observe → `Decide(desired, observed)` → act. `Decide` does
no I/O and returns one action, so the hard question — what to do next — reads like a truth table and
is unit-tested exhaustively. The messy part (talking to a live cluster) hides behind the
`ClusterAdmin` interface, with an in-memory fake so the reconciler runs in envtest with no Valkey.

**A native slot-mover.** Slot migration (scale-out *and* scale-in) is a native, idempotent
`MIGRATE … REPLACE` loop. `valkey-cli --cluster reshard` refuses on uneven or interrupted
distributions — which a topology change mid-reshard produces — so going native bought determinism.

**Let Valkey do failover.** Quorum promotion already works; reimplementing it adds risk for nothing.
The operator keeps the workloads alive and reports roles honestly — it doesn't play the algorithm.

## vs. KubeBlocks

KubeBlocks is a mature multi-engine platform: many CRDs (`ClusterDefinition`, `ComponentDefinition`,
`Cluster`, `OpsRequest`), an addon model, and a graph/transformer reconciler general enough to drive
Postgres, MySQL, Redis, and Kafka — with backup/restore and upgrades built in. That generality is its
strength, and the reason not to copy it for one engine.

| | This operator | KubeBlocks |
|---|---|---|
| Scope | One engine, Valkey | Many engines, a platform |
| API | 1 CRD | definition + instance + ops CRDs |
| Reconcile | pure `Decide`, one action | transformer DAG (engine-agnostic) |
| Day-2 | provision, failover, reshard, replica-scale | + backup, upgrade, vertical scale, volume expand |
| Extend | change the code | declarative addon |

A transformer DAG and an `OpsRequest` API earn their keep across ten engines; for one they're
overhead — exactly what the constitution's YAGNI rule exists to refuse.

**What I borrowed anyway:** the operational details its Redis addon already got right — announce a
stable hostname (never the pod IP), keep `nodes.conf` on the PVC, drain before you remove, bootstrap
from a single actor. Cheaper to copy than to rediscover in production.

> **The one-liner:** KubeBlocks is the right call if you run many databases. For one engine I wanted
> something I could read and test end-to-end in an afternoon — so I built the clustering core directly
> and stole KubeBlocks' hard-won operational defaults.
