# Feature Specification: ValkeyCluster Operator

**Feature Branch**: `001-valkeycluster-operator`

**Created**: 2026-06-23

**Status**: Draft

**Input**: User description: "Kubernetes operator that manages a Valkey cluster: users declare the desired topology (shards and replicas per shard) and the operator reconciles the cluster to match, including data-preserving resharding and automatic failover"

## Overview

A Kubernetes operator that lets users run and operate a **Valkey cluster** declaratively. The user states the *desired topology* — how many shards (data partitions) and how many replicas per shard (high-availability copies) — in a single custom resource, and the operator continuously reconciles the running cluster to match that intent. The operator covers the cluster's full life cycle: initial provisioning, automatic recovery from node failure, and changing the topology over time without losing data.

The defining behaviors are **sharding** (the keyspace is partitioned across shards so capacity and write throughput scale horizontally), **replication** (each shard keeps HA copies so the loss of a node does not lose data or availability), and **resharding** (the keyspace is redistributed when the shard count changes, preserving existing data).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Provision a clustered Valkey deployment from a declared topology (Priority: P1)

A platform user wants a running Valkey cluster of a specific shape. They create one `ValkeyCluster` resource declaring `shards` and `replicasPerShard` (plus image and storage size). The operator provisions the nodes, forms a single working cluster, partitions the entire keyspace across the shards, and attaches the replicas. When finished, clients can read and write across the whole keyspace and every shard has its HA copies.

**Why this priority**: This is the core value and the minimum viable product — without it there is no cluster. Everything else (failover, resharding) operates on the cluster this story creates.

**Independent Test**: Create a `ValkeyCluster` with `shards: 3, replicasPerShard: 1`; verify the resource reaches a `Ready` state, that the entire keyspace is served (no unassigned partitions), that there are 3 primaries and 3 replicas, and that a client can write keys that hash to different shards and read them all back.

**Acceptance Scenarios**:

1. **Given** no existing cluster, **When** a user creates a `ValkeyCluster` with `shards: 3, replicasPerShard: 1`, **Then** the operator brings up 6 nodes, forms one cluster covering 100% of the keyspace, and the resource reports `Ready`.
2. **Given** a `Ready` cluster, **When** a client writes keys spanning multiple shards and reads them back, **Then** all values are returned correctly.
3. **Given** a `ValkeyCluster` with `shards: 1, replicasPerShard: 2`, **When** it is created, **Then** the operator provisions one primary holding the full keyspace plus 2 replicas (the replication / HA-only configuration).
4. **Given** a `Ready` cluster, **When** the user deletes the `ValkeyCluster` resource, **Then** all nodes, storage, and supporting resources it created are removed.

---

### User Story 2 - Survive node failure automatically (Priority: P2)

A user relies on the cluster staying available when an individual node is lost (crash, eviction, node drain). When a shard's primary fails, one of that shard's replicas is promoted automatically and the cluster keeps serving that part of the keyspace, with no manual intervention. The operator reflects the new reality in the resource's status.

**Why this priority**: High availability is the main reason to declare `replicasPerShard ≥ 1`; a cluster that loses data or availability on a single node failure is not production-credible. It depends on US1 but delivers value independently (it is what "replicas" are *for*).

**Independent Test**: On a `Ready` cluster with `replicasPerShard: 1`, write data, delete a primary pod, and verify the affected slots keep serving within a bounded time, the previously written data is intact, and status reports the promoted node as the new primary.

**Acceptance Scenarios**:

1. **Given** a `Ready` cluster with `replicasPerShard ≥ 1`, **When** a shard's primary node is lost, **Then** a replica of that shard is promoted and the shard's portion of the keyspace resumes serving without operator action.
2. **Given** a primary was lost and a replica promoted, **When** the failed node is replaced/rescheduled, **Then** it rejoins the cluster as a replica of the new primary and status returns to `Available`.
3. **Given** a shard configured with `replicasPerShard: 0`, **When** its only node is lost, **Then** the resource reports `Degraded` for that shard (no replica available to promote) — this is the documented trade-off of running without HA copies.

---

### User Story 3 - Change topology with data preserved (resharding) (Priority: P3)

A user's needs change and they edit the topology — most importantly the number of shards (e.g., grow `shards: 3 → 5` for more capacity, or shrink `5 → 3`). The operator reconciles the cluster to the new topology and **redistributes the keyspace so previously stored data is preserved**. A brief period of unavailability during the change is acceptable.

**Why this priority**: Resharding is the most operationally advanced capability and the strongest signal of correct cluster management, but it is only meaningful once provisioning (US1) and HA (US2) exist. It is valuable on its own: it turns a fixed-size cluster into one that can grow/shrink with the workload.

**Independent Test**: On a `Ready` 3-shard cluster, write a known set of keys spanning the keyspace; change `shards` to 5; after reconciliation completes, verify the keyspace is now balanced across 5 shards and **every previously written key is still readable**.

**Acceptance Scenarios**:

1. **Given** a `Ready` cluster with data and `shards: 3`, **When** the user changes `shards` to 5, **Then** the operator adds capacity, redistributes the keyspace across all 5 shards, and every previously written key remains readable.
2. **Given** a `Ready` cluster with data and `shards: 5`, **When** the user changes `shards` to 3, **Then** the operator migrates the keyspace off the removed shards onto the survivors before removing them, and no data is lost.
3. **Given** a topology change is in progress, **When** the resource is inspected, **Then** it reports a `Resharding` state, and brief unavailability of affected portions of the keyspace during the change is acceptable.
4. **Given** the user changes only `replicasPerShard`, **When** the operator reconciles, **Then** it adds or removes HA copies per shard accordingly without redistributing the keyspace.

---

### Edge Cases

- **Invalid shard count**: A topology of `shards: 2` is rejected at admission (cluster failover voting needs a primary majority; meaningful values are `1` for HA-only or `≥3` for sharding).
- **Operator restart mid-operation**: If the operator crashes during forming or resharding, on restart it detects the partial state and drives the cluster to the declared topology with no manual repair and no data loss.
- **Node reschedule / address change**: When a pod is rescheduled (new network address), it rejoins the cluster under its original identity using its persisted state, and other nodes and clients continue to reach it via its stable advertised address.
- **Lost / empty storage**: If a node's persistent storage is lost and recreated empty (stale identity gone), the operator MUST detect this, drop the stale identity from the cluster, and rejoin the node fresh — rather than letting an empty node corrupt the slot map.
- **All copies of a shard lost simultaneously**: The resource reports `Degraded`/`Failed` for that shard and surfaces that the affected portion of the keyspace is unavailable, rather than silently appearing healthy.
- **Immutable storage size**: An attempt to change `storage.size` after creation is rejected (volume resizing is out of scope).
- **Out-of-band drift**: If a node/workload the operator created is deleted out of band, the operator recreates it and re-forms the cluster.

## Requirements *(mandatory)*

### Functional Requirements

**Topology declaration**
- **FR-001**: Users MUST be able to declare a cluster's desired topology in a single `ValkeyCluster` resource via `shards` (number of data partitions) and `replicasPerShard` (HA copies per shard).
- **FR-002**: Users MUST be able to specify the Valkey image and a per-node persistent storage size.
- **FR-003**: Users MUST be able to optionally supply Valkey configuration overrides applied when the cluster is formed.
- **FR-004**: The system MUST validate topology on admission: `shards` is `1` or `≥3` (reject `2`), and `replicasPerShard ≥ 0`.
- **FR-005**: The system MUST reject changes to a cluster's per-node storage size after creation.

**Provisioning & cluster formation**
- **FR-006**: On creation, the system MUST provision `shards × (1 + replicasPerShard)` nodes and form them into a single Valkey cluster.
- **FR-007**: The system MUST partition the entire keyspace (all hash slots) across the shards so that, when `Ready`, 100% of the keyspace is served.
- **FR-008**: The system MUST attach each shard's replicas to that shard's primary so they hold copies of the shard's data.
- **FR-009**: Each node MUST have a stable in-cluster network identity that survives pod restarts and address (IP) changes, and the node MUST advertise that stable address for both cluster gossip — **including the cluster bus channel** — and client redirects. Cluster membership and client routing MUST continue to work after any node's underlying address changes. The system MUST also expose a discovery endpoint that cluster-aware clients can use to reach the cluster.

**Persistence**
- **FR-010**: Each node's data AND its cluster membership identity (its node identity and view of the cluster) MUST be persisted to durable storage, so that a restarted or rescheduled node keeps its identity and rejoins the *existing* cluster rather than appearing as a new, duplicate node — with no data loss.

**High availability & failover**
- **FR-011**: When a shard's primary is lost and that shard has at least one replica, the system MUST result in automatic promotion of a replica so the shard's keyspace keeps serving without operator intervention.
- **FR-012**: A recovered or replacement node MUST rejoin the cluster (as a replica of the current primary) automatically.

**Resharding & scaling**
- **FR-013**: When `shards` changes, the system MUST reconcile the cluster to the new shard count and **redistribute the keyspace such that all previously stored data is preserved**.
- **FR-014**: When scaling shards down, the system MUST migrate the keyspace off departing shards onto remaining shards before removing them, and MUST hand off any primary role to a healthy replica before removing a node that is currently a primary, so that no acknowledged writes are lost.
- **FR-015**: When `replicasPerShard` changes, the system MUST add or remove HA copies per shard without redistributing the keyspace.
- **FR-016**: The system MAY make affected portions of the keyspace briefly unavailable during a topology change (disruptive topology changes are acceptable; zero-downtime resharding is not required).

**Reconciliation, status & lifecycle**
- **FR-017**: The system MUST continuously reconcile actual cluster state toward the declared topology, repairing drift (e.g., recreating resources deleted out of band, reassigning unserved keyspace).
- **FR-018**: The system MUST be resilient to its own restarts: after an interruption during forming or resharding, it MUST converge to the declared topology with no manual intervention and no data loss.
- **FR-019**: The system MUST report a truthful status derived from the live cluster, including an overall phase (e.g., `Provisioning`, `Forming`, `Resharding`, `Ready`, `Degraded`, `Failed`), per-shard primary identity and served keyspace, ready replica counts, and standard conditions (`Available`, `Progressing`, `Degraded`). This status MUST be the monitoring interface, surfaced through `kubectl` — at-a-glance summary columns on `get` and full detail on `describe` — with no separate metrics system or UI required.
- **FR-020**: Deleting a `ValkeyCluster` MUST remove all resources the operator created for it (nodes, storage, supporting objects).

**Verification (test suite & benchmark)**
- **FR-021**: The project MUST include an automated test suite that verifies provisioning, failover, and data-preserving resharding (the three user stories) as day-two operations.
- **FR-022**: The project MUST include a repeatable benchmark that measures cluster performance and demonstrates the clustering/HA trade-offs (at minimum: write throughput as a function of shard count, and the availability/latency impact of a failover).
- **FR-023**: The project MUST include documentation for day-two operations (scaling/resharding, observing failover, reading status) and a discussion of the clustering vs. HA performance trade-offs.

**Membership safety (working-cluster essentials)**
- **FR-024**: When a node is removed (shard scale-down or replica reduction), the system MUST remove it from cluster membership and reclaim its persistent storage, so that stale identity/membership state cannot later resurrect and corrupt the cluster if a like-named node is created afterward.
- **FR-025**: The system MUST NOT begin a topology change while the cluster has open or uncovered slots (e.g., a previous migration was interrupted); it MUST first repair the cluster to full keyspace coverage, then proceed.

### Key Entities *(include if feature involves data)*

- **ValkeyCluster**: The user's declaration of a desired cluster. Key attributes: `shards`, `replicasPerShard`, image, storage size, optional config. Carries observed status (phase, conditions, per-shard observed state).
- **Shard**: One data partition of the cluster. Owns a contiguous portion of the keyspace and consists of one primary plus its replicas. The unit that sharding and resharding operate on.
- **Node (cluster member)**: A single Valkey process participating in the cluster, acting as either a primary or a replica of some shard. Roles can change over time (e.g., after failover); the operator never assumes a fixed role.
- **Keyspace partition (hash slots)**: The full key space is divided into a fixed number of slots distributed across shards; "serving 100% of the keyspace" means every slot is owned by a reachable primary.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A newly created cluster of up to 5 shards with 1 replica each reaches a `Ready`, fully-serving state (100% of the keyspace owned by reachable primaries) within 5 minutes, with no manual steps.
- **SC-002**: After the loss of a single primary node (with `replicasPerShard ≥ 1`), the affected portion of the keyspace resumes serving within 30 seconds and no acknowledged data is lost.
- **SC-003**: After a shard-count change on a cluster holding data, 100% of previously written keys remain readable once reconciliation completes.
- **SC-004**: Killing the operator at any point during forming or resharding still results in the cluster converging to the declared topology with no manual intervention and no data loss.
- **SC-005**: The status shown for a cluster matches its real state (phase, per-shard primary, served keyspace) on every reconciliation, so an operator can trust `status` without inspecting nodes directly.
- **SC-006**: The benchmark demonstrates a measurable increase in aggregate write throughput as shard count increases (e.g., a 3-shard cluster outperforms a 1-shard cluster on a write-heavy workload), quantifying the sharding trade-off.
- **SC-007**: Restarting or rescheduling any node (with its storage intact) results in that node rejoining the same cluster under its original identity — with no split, no duplicate membership, no manual intervention, and no data loss.

## Assumptions

- **Clients are cluster-aware and in-cluster**: Clients connect from within the Kubernetes cluster and use a cluster-aware protocol (following key-redirection between nodes). External access and non-cluster-aware client compatibility are out of scope. This is the accepted ergonomic trade-off of using native Valkey clustering instead of an external failover manager.
- **Native Valkey clustering provides failover**: HA/failover is handled by Valkey's built-in cluster mechanism (replica promotion), not by a separate failover manager. The operator's job is to provision, form, reshard, reconcile, and report — not to implement the promotion algorithm.
- **Disruptive topology changes are acceptable**: Topology changes may briefly interrupt the affected keyspace; zero-downtime online resharding is explicitly not required.
- **Persistent storage is available**: The target Kubernetes cluster provides a default storage class for per-node persistent volumes; volume expansion is not used.
- **Single namespace per cluster**: A `ValkeyCluster` and all its resources live in one namespace.

### Out of Scope (non-goals for this version)

- Vertical scaling (CPU/memory changes), volume expansion, dynamic reconfiguration, and in-place rolling restart operations.
- Backup and restore to external object storage.
- TLS, ACLs, and authentication/password management.
- Sentinel-based (non-cluster) deployments and proxy topologies.
- External / cross-cluster access and non-cluster-aware client compatibility.
- Multi-namespace or multi-cluster (federated) management.
- Metrics/Prometheus/Grafana integration, custom dashboards, and any bespoke web UI — monitoring is via `kubectl` only (status, conditions, summary columns).
