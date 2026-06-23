# Tasks: ValkeyCluster Operator

**Feature**: `specs/001-valkeycluster-operator/` | **Spec**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md)

Test-first is mandated by the constitution, so test tasks precede implementation within each phase.
Paths are relative to the repo root. `[P]` = parallelizable (distinct files, no incomplete deps).

## Phase 1: Setup

- [ ] T001 Add `github.com/redis/go-redis/v9` to `go.mod` and run `go mod tidy`
- [ ] T002 [P] Create package skeletons with doc.go: `internal/cluster/`, `internal/slots/`, `internal/topology/`, `internal/resources/`
- [ ] T003 [P] Add `docs/` and `bench/` directories with placeholder READMEs

## Phase 2: Foundational (blocking prerequisites)

- [ ] T004 Extend `api/v1alpha1/valkeycluster_types.go`: `ValkeyClusterSpec` (shards, replicasPerShard, image, storage{size,storageClassName}, resources, haPolicy{minReplicasToWrite, requireFullCoverage, appendFsync, clusterNodeTimeoutMillis}) with kubebuilder validation markers per data-model.md
- [ ] T005 Add CEL validations in `api/v1alpha1/valkeycluster_types.go`: `shards == 1 || shards >= 3`, and `storage.size` immutability (`self == oldSelf`)
- [ ] T006 Extend `ValkeyClusterStatus` in `api/v1alpha1/valkeycluster_types.go`: phase, observedGeneration, readyShards, shards[]ShardStatus, conditions; add printer-column markers (SHARDS/REPLICAS/PHASE/READY/AGE)
- [ ] T007 Run `make generate manifests` to regenerate deepcopy + CRD; verify CRD renders
- [ ] T008 [P] Define `ClusterAdmin` interface + domain types (NodeInfo, SlotRange, ClusterState, Endpoint, RebalanceOpts) in `internal/cluster/admin.go` per contracts/cluster-admin.md
- [ ] T009 [P] Unit tests for slot math in `internal/slots/slots_test.go` (even split of 16384 over N; reshard deltas; edge n=1)
- [ ] T010 Implement `internal/slots/slots.go` to pass T009
- [ ] T011 [P] Unit tests for topology diff in `internal/topology/topology_test.go` (desired vs observed → ordered actions for: fresh, add-shard, remove-shard, add-replica, remove-replica, drift)
- [ ] T012 Implement `internal/topology/topology.go` to pass T011
- [ ] T013 [P] Unit tests for resource builders in `internal/resources/builders_test.go` (per-shard StatefulSet name/labels/volumeClaimTemplate, headless Service, ConfigMap render incl. haPolicy → valkey.conf, anti-affinity present)
- [ ] T014 Implement `internal/resources/builders.go` to pass T013 (StatefulSet-per-shard, headless Service, ConfigMap, FQDN announce args, preferred pod anti-affinity)
- [ ] T015 [P] Implement in-memory `fake` ClusterAdmin in `internal/cluster/fake.go` (per contracts) for envtest/unit
- [ ] T016 Update RBAC markers on the controller for statefulsets, services, configmaps, persistentvolumeclaims, pods, pods/exec, events; `make manifests`

## Phase 3: User Story 1 — Provision from declared topology (P1) 🎯 MVP

**Goal**: applying a `ValkeyCluster` yields a formed, fully-serving cluster; status reports `Ready`.
**Independent test**: apply shards:3/replicas:1 → `Ready`, 100% slots covered, 3 primaries + 3 replicas, cross-shard read/write works.

- [ ] T017 [P] [US1] go-redis `ClusterAdmin` impl in `internal/cluster/goredis.go`: State (CLUSTER INFO/NODES parse), Meet, AddSlots, Replicate, Forget
- [ ] T018 [P] [US1] pod-exec impl in `internal/cluster/exec.go`: Rebalance/Fix via `valkey-cli --cluster` (uses client-go remotecommand)
- [ ] T019 [US1] Reconcile skeleton + finalizer in `internal/controller/valkeycluster_controller.go`: ensure ConfigMap, headless Service, per-shard StatefulSets (server-side apply, owner refs); requeue until pods Ready
- [ ] T020 [US1] Forming logic in `internal/controller/phases.go`: when not formed, Meet all → AddSlots even split → Replicate; single-leader guard; verify full coverage
- [ ] T021 [US1] Status derivation in `internal/controller/phases.go`: build phase/conditions/shards[] from live ClusterState (Available when 100% covered)
- [ ] T022 [US1] Controller wiring in `cmd/main.go` / SetupWithManager: owns StatefulSet/Service/ConfigMap; inject ClusterAdmin
- [ ] T023 [P] [US1] envtest in `internal/controller/valkeycluster_controller_test.go` with fake ClusterAdmin: creates expected resources + owner refs, reaches Ready, status correct
- [ ] T024 [US1] e2e in `test/e2e/`: apply shards:3/replicas:1 on kind, wait Ready, assert slot coverage + cross-shard set/get via valkey-cli -c
- [ ] T025 [US1] Sample CR `config/samples/cache_v1alpha1_valkeycluster.yaml` updated to shards:3/replicas:1 with haPolicy

**Checkpoint**: US1 independently demonstrable.

## Phase 4: User Story 2 — Automatic failover (P2)

**Goal**: losing a primary auto-promotes a replica; status reflects it.
**Independent test**: delete a primary pod → slots resume <30s, data intact, status shows new primary.

- [ ] T026 [US2] Reconcile handles role changes: re-read roles each loop, never assume ordinal-0 primary; ensure rescheduled pod re-joins (Meet if gossip stale) in `internal/controller/phases.go`
- [ ] T027 [US2] Degraded status when a shard has no reachable primary / `replicasPerShard:0` node lost (`internal/controller/phases.go`)
- [ ] T028 [P] [US2] envtest: fake reports primary down + replica promoted → status transitions Available→Degraded→Available
- [ ] T029 [US2] e2e: write keys, delete primary pod, assert auto-failover + data intact + status updated

**Checkpoint**: US1 + US2 work.

## Phase 5: User Story 3 — Data-preserving resharding + replica scaling (P3)

**Goal**: changing `shards` redistributes the keyspace preserving data; changing `replicasPerShard` adds/removes copies.
**Independent test**: 3→5 shards with data → all prior keys readable, 5 shards cover keyspace.

- [ ] T030 [US3] Stability gate (FR-024): if slots open/uncovered, run Fix and requeue before any topology change (`internal/controller/phases.go`)
- [ ] T031 [US3] Scale-up shards: create new shard STS → Meet → Rebalance(UseEmptyMasters) → verify coverage; phase=Resharding
- [ ] T032 [US3] Scale-down shards: Rebalance(WeightZero on departing primaries) → handoff primary via Failover if needed → Forget → delete STS + reclaim PVCs (FR-013/023)
- [ ] T033 [US3] Replica scaling: scale shard STS for `replicasPerShard` change → Replicate new / Forget removed; phase=ScalingReplicas
- [ ] T034 [P] [US3] envtest: topology transitions (add/remove shard, add/remove replica) drive the expected ClusterAdmin action sequence via fake
- [ ] T035 [US3] e2e: write keyset, patch shards 3→5, wait Ready, assert every prior key readable + 5-shard coverage; then 5→3 and re-assert
- [ ] T036 [US3] e2e: patch replicasPerShard 1→2, assert each shard gains a replica, no resharding

**Checkpoint**: all three user stories work.

## Phase 6: HA policy & scheduling (clustering/HA tradeoff criterion)

- [ ] T037 [P] HA policy → valkey.conf mapping verified in `internal/resources/builders_test.go` (min-replicas-to-write, cluster-require-full-coverage, appendfsync, cluster-node-timeout)
- [ ] T038 Apply haPolicy at form time and on supported changes (rolling pod restart acceptable) in controller
- [ ] T039 [P] Confirm anti-affinity spreads a shard's pods across nodes (assert in e2e or builders test)

## Phase 7: Benchmark & documentation

- [ ] T040 [P] `bench/benchmark.sh` + `bench/job.yaml`: valkey-benchmark sweep over shard counts + WAIT durability + appendFsync; emit markdown table (FR-021, SC-006/008)
- [ ] T041 [P] `docs/day-2-operations.md`: install, apply, scale/reshard, observe failover, read status, teardown
- [ ] T042 [P] `docs/clustering-ha-tradeoffs.md`: sharding vs replication, async data-loss window, ≥3-shard quorum, each haPolicy knob, anti-affinity, benchmark results
- [ ] T043 [P] Update root `README.md`: what it is, quickstart, link to spec-kit artifacts (methodology), docs

## Phase 8: Polish & manual verification

- [ ] T044 `make build && make test && go vet ./...` all green
- [ ] T045 Manual verification on kind (per quickstart.md): deploy operator, apply CR, **use the Valkey instances** (set/get across shards via valkey-cli -c), kill a primary (failover), reshard 3→5 (data preserved), teardown — capture output
- [ ] T046 Run `make test-e2e` (kind) and confirm green; record any flakes/limitations
- [ ] T047 Final commit + push; ensure repo is clean and public repo current

## Dependencies & order

- Setup (P1) → Foundational (P2) → US1 (P3) → US2 (P4) → US3 (P5) → HA/policy (P6) → bench/docs (P7) → polish (P8).
- US2 and US3 depend on US1's forming + status. HA policy (P6) depends on resource builders (P2).
- Within a phase, `[P]` tasks touch different files and may run together.

## Parallel opportunities

- P2: T008/T009/T011/T013/T015 (interface, slots tests, topology tests, builders tests, fake) in parallel.
- US1: T017 (go-redis) and T018 (pod-exec) in parallel; T023 envtest parallel to e2e authoring.
- P7: all of T040–T043 in parallel.

## MVP scope

**US1 (Phase 3)** alone is a demonstrable MVP: declare topology → working, fully-serving Valkey
cluster with truthful status. US2 and US3 layer on incrementally.
