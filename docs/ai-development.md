# AI Development Methodology

How this operator was built with an AI coding agent (Claude Code). The goal was
not "generate code fast" but to use AI under enough structure and verification
that the result is correct on a *real* cluster — which, for a Valkey cluster
operator, is where the hard parts live.

## 1. Spec-driven, not prompt-driven

The work flowed through **GitHub Spec Kit** in order, each artifact committed and
reviewed before the next:

```
constitution  →  spec.md  →  plan.md  →  research.md / data-model.md / contracts/  →  tasks.md  →  implementation
```

- The **constitution** (`.specify/memory/constitution.md`) encoded five non-negotiables up front —
  spec-driven flow, test-first, simplicity/YAGNI, truthful observable status, idempotent self-healing
  reconciliation — so every later decision had a fixed rubric instead of being re-litigated per prompt.
- The **spec** fixed scope and, just as importantly, **non-goals** (no vertical scaling, volume
  expansion, backup/restore, TLS/ACL, Sentinel, proxy, metrics stack). Naming what we would *not*
  build is what kept an AI agent — which will happily gold-plate — on task.
- `plan.md` recorded the architecture decisions (StatefulSet-per-shard, the `ClusterAdmin` seam,
  announce-by-hostname/meet-by-IP) so the implementation phase was execution, not exploration.

The full trail is in the repo, so the reasoning is auditable rather than lost in a chat log.

## 2. Test-first, with a seam built for it

The constitution made test-first non-negotiable, which forced a testable design:

- **Pure logic** (slot distribution, the topology decision, status derivation, config rendering) is
  table-driven unit-tested — `internal/slots` and `internal/topology` sit at high coverage.
- The **`cluster.ClusterAdmin` interface** exists specifically so the reconciler can be driven by a
  **fake** in-memory cluster under controller-runtime **envtest** — reconcile behavior is tested
  against a real API server without a live Valkey.
- The three user stories run end-to-end on **kind** (Ginkgo), now including scale-in.

This is the lever that lets an AI iterate safely: most changes are validated by `make test` in
seconds, so regressions are caught before they reach a cluster.

## 3. Verification against a real cluster, not just green unit tests

Passing unit and envtest suites are necessary but not sufficient: Valkey-on-Kubernetes has runtime
behaviors that don't surface without a live cluster. The workflow treats a real **kind** cluster as a
first-class verification gate — the day-2 matrix (`bench/day2-matrix.sh`) and the Ginkgo e2e suite
exercise provision, failover, and resharding with reads/writes at every pivot, and any defect they
expose is fixed at the **root cause** (the class of issue, not the single instance) and locked in with
a regression test. The Valkey runtime behaviors the operator encodes as a result are documented as
present-tense facts in [Settings for Performance and High Availability](./settings.md) and the
[walkthrough](./walkthrough.md) (e.g. MEET-by-IP with hostname announcement, primary discovery via
`CLUSTER MYID`, masters-only `SETSLOT`, StatefulSet-driven scale-in teardown).

## 4. Verification gates on AI output

- Nothing is claimed "done" without running the command and reading the output — `make test`,
  `go build`/`vet`, `kubectl` / `valkey-cli --cluster check`, and the day-2 matrix.
- Broad, mechanical edits (comment cleanup, splitting large files) are delegated to **focused
  subagents** with explicit rules, then gated on `gofmt` + `go build` + `go vet` + the full test
  suite before commit.

## In one sentence

Structure the work with a spec and a constitution, make the design testable on purpose, and gate every
change on a real cluster — so the AI's speed is spent on correctness, not just output.
