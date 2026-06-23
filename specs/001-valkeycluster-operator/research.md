# Phase 0 Research: ValkeyCluster Operator

Decisions resolving the Technical Context. Grounded in the KubeBlocks Redis-addon cross-check
(see spec history) and Valkey cluster semantics.

## D1 — Cluster-admin command driver
**Decision**: `go-redis/v9` for `CLUSTER` RPCs (`INFO`, `NODES`, `MEET`, `ADDSLOTS`, `REPLICATE`,
`FORGET`, `SETSLOT`, `FAILOVER`); delegate **key migration** (reshard/rebalance/fix) to
`valkey-cli --cluster` executed inside a Valkey pod via the Kubernetes pod-exec API.
**Rationale**: `go-redis` is mature and Valkey-wire-compatible; hand-rolling per-key `MIGRATE`
loops + `SETSLOT MIGRATING/IMPORTING` is error-prone, while `valkey-cli --cluster` is the
battle-tested tool that already handles open-slot recovery (`fix`). Pod-exec avoids shipping a
separate Job image and reuses the cli already in `valkey/valkey:8`.
**Alternatives**: pure-Go MIGRATE (rejected: complexity/risk); separate Job running cli (rejected:
extra image/manifest for no benefit over exec).

## D2 — Network identity / announce (the #1 K8s failure mode)
**Decision**: one headless Service per CR; each pod sets `cluster-announce-hostname
<pod>.<svc>.<ns>.svc` + `cluster-preferred-endpoint-type hostname`, client port 6379, bus port
16379. `cluster-config-file` on the PVC.
**Rationale**: pod IPs change on restart; announcing the stable per-pod DNS name keeps gossip and
client `MOVED`/`ASK` redirects valid across restarts. Matches KubeBlocks default mode.
**Alternatives**: announce pod IP (rejected: breaks on restart); per-pod NodePort/LB advertised
(rejected: only needed for external access, which is out of scope).

## D3 — Workload mapping
**Decision**: one StatefulSet per shard. **Rationale**: clean 1:1 shard↔workload; adding a shard =
add a StatefulSet; stable per-pod identity + PVC; resharding expressible. **Alternatives**: single
StatefulSet for all nodes (rejected: ordinal→shard mapping brittle mid-reshard); bare pods (rejected:
reimplements StatefulSet).

## D4 — HA via native Valkey cluster failover
**Decision**: rely on Valkey's built-in replica promotion; operator does not implement the
algorithm. For planned primary removal (scale-in), operator triggers `CLUSTER FAILOVER` on a
replica first. **Rationale**: cluster mode provides quorum-based auto-failover; reimplementing is
out of scope and unnecessary.

## D5 — Persistence
**Decision**: PVC per pod (`volumeClaimTemplates`), `appendonly yes`, `nodes.conf` + AOF/RDB under
`/data`. **Rationale**: data and cluster identity must survive restart/reschedule (FR-009);
resharding preserves data only if it persists.

## D6 — HA policy knobs → valkey.conf mapping
| `haPolicy` field | valkey.conf | Trade-off |
|---|---|---|
| `minReplicasToWrite` | `min-replicas-to-write` (+`min-replicas-max-lag`) | durability vs write-availability |
| `requireFullCoverage` | `cluster-require-full-coverage` | availability vs correctness |
| `appendFsync` | `appendfsync` (always/everysec/no) | durability vs throughput |
| `clusterNodeTimeout` | `cluster-node-timeout` | failover speed vs false positives |
**Note**: Valkey cluster replication is always asynchronous; there is no sync/quorum-write mode.
Per-write durability is demonstrated with `WAIT` in the benchmark.

## D7 — Testing strategy
**Decision**: (a) unit tests for `slots`, `topology`, `resources`, status derivation (pure, fast);
(b) envtest for the reconciler with a **fake `ClusterAdmin`** (no live Valkey); (c) kind e2e
(Ginkgo) for provision / failover / data-preserving reshard. **Rationale**: isolates K8s-API
reconcile logic from Valkey runtime so most logic is fast/deterministic; e2e proves the real thing.

## D8 — Benchmark
**Decision**: `valkey-benchmark` (and `WAIT` for durability) run as an in-cluster Job; a script
sweeps shard counts and emits a markdown table. **Rationale**: repeatable, in-cluster, demonstrates
the required trade-off dimensions (FR-021, SC-006/008).

## D9 — Slot distribution
**Decision**: even contiguous split of 16384 across shard primaries (shard `i` gets
`[i*16384/n, (i+1)*16384/n)`); reshard computes per-primary target counts and lets
`valkey-cli --cluster rebalance` move the delta. **Rationale**: simple, balanced, matches cli.
