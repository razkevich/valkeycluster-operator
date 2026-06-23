<!--
Sync Impact Report
- Version change: (template) → 1.0.0
- Rationale: Initial ratification of the project constitution (MAJOR baseline).
- Principles defined:
  1. Spec-Driven Development
  2. Test-First (NON-NEGOTIABLE)
  3. Simplicity & YAGNI
  4. Truthful, Observable State
  5. Idempotent, Self-Healing Reconciliation
- Added sections: Engineering Constraints; Development Workflow & Quality Gates; Governance
- Removed sections: none (initial)
- Templates reviewed:
  ✅ .specify/templates/plan-template.md — Constitution Check section is generic; principles map cleanly, no edit required
  ✅ .specify/templates/spec-template.md — scope/non-goals & testable-requirements align with Principles 1 & 3
  ✅ .specify/templates/tasks-template.md — test-first task ordering aligns with Principle 2
- Deferred TODOs: none
-->

# ValkeyCluster Operator Constitution

## Core Principles

### I. Spec-Driven Development
All work flows in order from `spec.md` → `plan.md` → `tasks.md` → implementation, and
every one of these artifacts is committed to version control and kept in sync with the
code. Implementation MUST NOT begin for a unit of work until its requirements exist in the
spec and its approach exists in the plan. When code and artifacts diverge, the artifacts
are corrected in the same change — stale specs are treated as defects.

**Rationale**: The development methodology is itself a deliverable; a traceable
spec→plan→tasks→code chain makes intent reviewable and prevents unscoped drift.

### II. Test-First (NON-NEGOTIABLE)
Tests are written alongside or before the implementation they cover, and they MUST fail
before the implementing code exists (Red-Green-Refactor). Coverage spans three layers:
unit tests for pure logic (slot distribution, topology diffing, status derivation),
controller tests against a real API server for reconcile behavior, and end-to-end tests on
a real Kubernetes cluster for the three day-two operations — provisioning, automatic
failover, and data-preserving resharding. No day-two operation is "done" without an
automated test that exercises it.

**Rationale**: Correctness of a stateful clustering operator cannot be eyeballed;
data-preservation and failover claims are only credible when a test proves them.

### III. Simplicity & YAGNI
Only the declared scope is built: sharding, replication, and data-preserving resharding.
The documented non-goals — vertical scaling, volume expansion, dynamic reconfiguration,
in-place rolling-restart ops, backup/restore, TLS/ACL/auth, Sentinel (non-cluster) mode,
and proxy topologies — MUST stay out of the implementation. New capability requires a spec
change first; incidental complexity MUST be justified against a concrete requirement or
removed.

**Rationale**: A focused operator that does a few things correctly is more valuable, and
more verifiable, than a broad one that does many things partially.

### IV. Truthful, Observable State
The resource's status MUST be derived from the live cluster on every reconciliation, never
assumed or cached from intent. Conditions (`Available`, `Progressing`, `Degraded`) and the
phase MUST reflect reality — including surfacing `Degraded`/`Failed` when a shard has no
reachable primary rather than appearing healthy. The operator MUST emit structured logs and
metrics sufficient to observe and debug day-two operations.

**Rationale**: Operators are trusted as the source of truth; a status that lies is worse
than no status, and observability is what makes day-two operations safe.

### V. Idempotent, Self-Healing Reconciliation
Every reconcile reads actual cluster state first and is safe to run any number of times with
the same outcome. The operator MUST converge to the declared topology after its own
restarts, including interruptions mid-forming or mid-resharding, with no manual intervention
and no data loss. Disruptive topology changes are acceptable; data loss is not.

**Rationale**: In Kubernetes, reconcile runs repeatedly and processes crash; correctness
depends on every step being a safe, resumable convergence rather than a one-shot mutation.

## Engineering Constraints

- **Platform**: A Kubernetes operator following the controller-runtime reconcile model; the
  desired state is a single namespaced custom resource.
- **HA mechanism**: High availability is provided by Valkey's native cluster failover
  (replica promotion). The operator provisions, forms, reshards, reconciles, and reports —
  it does not implement the promotion algorithm.
- **Persistence**: Each node's data is persisted to durable per-node storage so data and
  cluster identity survive restarts and reschedules.
- **Clients**: Clients are in-cluster and cluster-aware; external access and non-cluster
  client compatibility are out of scope.
- **Resource ownership**: Every resource the operator creates is owned by the custom
  resource so deletion garbage-collects the whole cluster.

## Development Workflow & Quality Gates

- Changes are validated by the test layers in Principle II; the suite MUST pass before work
  is considered complete.
- The benchmark MUST remain repeatable and is the evidence behind the clustering/HA
  performance trade-off documentation.
- Day-two operations MUST be documented (scaling/resharding, observing failover, reading
  status) and kept current with behavior.
- Each Spec Kit phase (`/speckit-specify`, `/speckit-plan`, `/speckit-tasks`,
  `/speckit-implement`) is committed as a discrete, reviewable checkpoint.

## Governance

This constitution supersedes ad-hoc practice for this project. Amendments are made by editing
this file with a Sync Impact Report and a semantic version bump: MAJOR for incompatible
principle removals/redefinitions, MINOR for added or materially expanded principles/sections,
PATCH for clarifications. Every change (plan, review, implementation) MUST verify compliance
with these principles; any deviation MUST be justified in the relevant plan's Complexity
Tracking or removed. The plan template's Constitution Check is the per-feature compliance gate.

**Version**: 1.0.0 | **Ratified**: 2026-06-23 | **Last Amended**: 2026-06-23
