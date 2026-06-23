# Implementation Plan: ValkeyCluster Operator

**Branch**: `main` (working directly on main per project decision) | **Date**: 2026-06-23 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/001-valkeycluster-operator/spec.md`

## Summary

A Kubebuilder/controller-runtime operator that manages a sharded **Valkey Cluster** from a single
`ValkeyCluster` custom resource. Users declare topology (`shards` × `replicasPerShard`) plus image,
storage, and an `haPolicy`; the operator provisions one StatefulSet per shard, forms the Valkey
cluster (gossip + slot assignment + replica attachment), keeps it converged (drift repair,
data-preserving resharding on topology change), relies on Valkey's built-in failover for HA, and
reports truthful status via `kubectl`. Cluster-admin RPCs use `go-redis/v9`; key migration during
resharding is delegated to `valkey-cli --cluster` executed inside a pod. In-cluster, cluster-aware
clients only.

## Technical Context

**Language/Version**: Go 1.26

**Primary Dependencies**: sigs.k8s.io/controller-runtime (Kubebuilder v4 scaffold), k8s.io/api &
apimachinery, `github.com/redis/go-redis/v9` (RESP/cluster client — Valkey-compatible) for
`CLUSTER` RPCs; `valkey-cli --cluster` (shipped in the `valkey/valkey:8` image) invoked via the
Kubernetes pod-exec API for slot/key migration (reshard/rebalance/fix).

**Storage**: One PVC per pod via StatefulSet `volumeClaimTemplates`; AOF on (`appendonly yes`);
`cluster-config-file` (`nodes.conf`) lives on the PVC so node identity survives restarts.

**Testing**: Go `testing` (table-driven unit tests for pure logic); controller-runtime **envtest**
(reconcile behavior against a real API server with a fake ClusterAdmin); **kind** e2e
(Ginkgo/Gomega) against the `valkeycluster-dev` cluster for the three day-two operations.

**Target Platform**: Kubernetes (kind for dev + e2e; any conformant cluster with a default
StorageClass at runtime).

**Project Type**: Single-module Kubernetes operator.

**Performance Goals**: No hard SLO. The benchmark must *demonstrate* trade-offs: write throughput
vs. shard count, failover availability/latency blip, durability-vs-latency (`WAIT`/min-replicas),
and `appendFsync` throughput impact.

**Constraints**: Data must be preserved across topology changes; brief unavailability during
topology change is acceptable; in-cluster cluster-aware clients only; `shards` ∈ {1} ∪ {≥3}.

**Scale/Scope**: Small clusters — 1 or 3–5 shards, 0–2 replicas per shard; e2e exercises up to
5 shards.

## Constitution Check

*GATE: must pass before Phase 0 and re-checked after design.*

| Principle | How this plan complies | Gate |
|-----------|------------------------|------|
| I. Spec-Driven Development | Plan derives from `spec.md`; artifacts (research/data-model/contracts/quickstart/tasks) committed alongside code. | ✅ |
| II. Test-First (non-negotiable) | Pure logic (slots, topology diff, status, config render) is unit-tested first; reconcile behavior via envtest with a **fake `ClusterAdmin`**; the three user stories via kind e2e. The `ClusterAdmin` interface exists specifically to make reconcile logic testable without live Valkey. | ✅ |
| III. Simplicity & YAGNI | Only in-scope features; non-goals (vertical scaling, volume expansion, dynamic reconfigure, backup/restore, TLS/ACL, Sentinel, proxy, external access, metrics/UI) stay out. | ✅ |
| IV. Truthful, Observable State | Status is derived every reconcile from live `CLUSTER NODES`/`CLUSTER INFO`; `Degraded` surfaced on slot gaps / primary-less shards. Monitoring is `kubectl` (printer columns + conditions). | ✅ |
| V. Idempotent, Self-Healing Reconciliation | Every step reads actual state first; bootstrap guarded by `CLUSTER INFO`; reshard resumes via `RepairSlots` (open-slot finalization); safe under operator restarts. | ✅ |

No violations → Complexity Tracking left empty.

## Project Structure

### Documentation (this feature)

```text
specs/001-valkeycluster-operator/
├── spec.md              # signed-off requirements
├── plan.md              # this file
├── research.md          # Phase 0 decisions
├── data-model.md        # CRD schema (spec + status) + invariants
├── contracts/
│   ├── valkeycluster-crd.md      # the user-facing CR contract (fields, validation, examples)
│   └── cluster-admin.md          # internal ClusterAdmin Go interface contract
├── quickstart.md        # runnable validation guide (install → apply → reshard → failover)
└── tasks.md             # Phase 2 (/speckit-tasks)
```

### Source Code (repository root) — extends the existing Kubebuilder scaffold

```text
api/v1alpha1/
├── valkeycluster_types.go        # Spec (shards, replicasPerShard, image, storage, resources, haPolicy) + Status
└── zz_generated.deepcopy.go

cmd/main.go                       # manager entrypoint (existing)

internal/
├── controller/
│   ├── valkeycluster_controller.go   # Reconcile entrypoint: finalizer, ensure, readiness gate, dispatch
│   ├── reconcile.go                  # observe → decide → act core + cluster formation
│   ├── resources.go                  # ensure ConfigMap / Service / StatefulSets + readiness
│   ├── scaling.go                    # scale-out / scale-in + shard & PVC teardown
│   ├── membership.go                 # replica attach, primary discovery, stale-node forgetting
│   ├── status.go                     # status/conditions + decision-input summary
│   └── valkeycluster_controller_test.go  # envtest, fake ClusterAdmin
├── cluster/                          # Valkey cluster orchestration (the "ClusterAdmin")
│   ├── admin.go                      # ClusterAdmin interface + domain types (NodeInfo, Topology)
│   ├── goredis.go                    # go-redis/v9 impl (CLUSTER INFO/NODES/MEET/ADDSLOTS/REPLICATE/FORGET)
│   ├── exec.go                       # pod-exec valkey-cli --cluster reshard|rebalance|fix
│   └── fake.go                       # in-memory fake for unit/envtest
├── slots/
│   └── slots.go + slots_test.go      # pure: distribute 16384 slots across N shards; reshard deltas
├── topology/
│   └── topology.go + topology_test.go # pure: desired vs observed diff → actions
└── resources/
    └── builders.go + builders_test.go # StatefulSet (per shard) / headless Service / ConfigMap + anti-affinity

config/                               # kustomize (CRD, rbac, manager, samples) — existing
hack/kind-cluster.yaml                # existing
bench/
├── benchmark.sh                      # runs valkey-benchmark scenarios, emits markdown table
└── job.yaml                          # in-cluster benchmark Job
test/e2e/                             # kind e2e (Ginkgo) — provision / failover / reshard
docs/
├── day-2-operations.md               # scaling, resharding, failover, reading status
└── clustering-ha-tradeoffs.md        # sharding vs replication, async data-loss window, knobs
```

**Structure Decision**: Single Go module on the existing Kubebuilder layout. The cluster-orchestration
logic is isolated behind `internal/cluster.ClusterAdmin` (real go-redis impl + fake) so the controller
and the pure helpers (`slots`, `topology`, `resources`) are unit/envtest-testable without a live Valkey,
satisfying the Test-First principle. One StatefulSet per shard is the unit of sharding and resharding.

## Architecture decisions (detail)

- **Workload mapping (Approach A)**: per shard `i`, StatefulSet `<cr>-shard-<i>` with `1+replicasPerShard`
  pods; pod `<cr>-shard-<i>-<j>`. Primary role is *not* pinned to ordinal 0 — roles are read live.
- **Networking**: one headless Service `<cr>-nodes` (all pods); each pod announces its stable FQDN via
  `cluster-announce-hostname <pod>.<cr>-nodes.<ns>.svc` + `cluster-preferred-endpoint-type hostname`;
  client port 6379, bus port 16379.
- **Config**: a ConfigMap renders `valkey.conf` from built-in sane defaults overlaid with `haPolicy`
  (`min-replicas-to-write`, `cluster-require-full-coverage`, `appendfsync`, `cluster-node-timeout`).
- **Anti-affinity**: pods of one shard get `preferredDuringScheduling` pod anti-affinity on a
  shard label so replicas avoid co-locating with their primary.
- **Bootstrap (idempotent)**: gate on all pods Ready → `CLUSTER INFO`; if not formed: `MEET` all,
  `ADDSLOTS` even split across shard-0-pod of each shard, `REPLICATE` the rest; verify full coverage.
  A single-leader guard (lock annotation / lexicographic owner) prevents concurrent `create`.
- **Resharding**:
  - *scale-up* → create shard STS → `MEET` → **targeted** reshard moving the new primary its fair
    share of slots (never `--use-empty-masters`, which would also feed empty replica-pods).
  - *scale-down* → drain each departing shard with a **native Go slot-mover** (`ClusterAdmin.MoveSlots`:
    `SETSLOT IMPORTING/MIGRATING` → `MIGRATE … REPLACE` by IP → masters-only `SETSLOT NODE`), in
    bounded batches; once a shard owns 0 slots, `FORGET` its nodes from every survivor and delete the
    STS + PVCs. Departing-shard selection and teardown are driven by which StatefulSets exist, not by
    the live primary count.
  - *open slots* → `ClusterAdmin.RepairSlots` finalizes any importing/migrating slots deterministically
    (handles multi-way open slots) as the FR-024 stability gate.
- **Finalizer**: ordered teardown + PVC reclaim on scale-in (FR-023).
- **Status**: phase + conditions derived from `CLUSTER NODES`/`INFO` each reconcile.

## Complexity Tracking

No constitution violations; section intentionally empty.
