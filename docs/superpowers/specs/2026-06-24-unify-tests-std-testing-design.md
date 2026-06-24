# Design: Unify the test suite on Go's std `testing`

**Date:** 2026-06-24
**Status:** Approved

## Problem

The suite mixes two frameworks: unit tests use Go's std `testing` (table-driven,
idiomatic), while the controller-envtest suite and the e2e suite use Ginkgo/Gomega
(the kubebuilder default). The mix means inconsistent run/debug ergonomics — Ginkgo
specs don't get per-test run/debug gutter icons in IntelliJ, and there are two
mental models for "how a test is written."

## Goal

One framework across all layers, runnable from IntelliJ and the CLI with native
per-test run/debug and clear progress, debuggable with breakpoints, and able to
watch the live cluster while e2e runs.

## Decision

Standardize on **plain std `testing` with `t.Run` subtests** everywhere. No Ginkgo,
no Gomega, **no testify** — the existing unit tests use plain std `testing`, so this
adds zero dependencies and keeps a single assertion style (`if err != nil { t.Fatalf }`).

### Layers

1. **Unit** (`slots`, `topology`, `resources`, `cluster`) — already std `testing`. No change.

2. **Controller envtest** (`internal/controller/`)
   - `suite_test.go` → `TestMain(m)` that starts envtest once, builds the scheme and
     `k8sClient`, runs `m.Run()`, then stops envtest. Retains `getFirstFoundEnvTestBinaryDir`
     so it runs from an IDE after `make setup-envtest`.
   - `valkeycluster_controller_test.go` → `TestReconcile_ProvisionAndForm` and
     `TestReconcile_ScaleReplicas`, each with `t.Run` sub-steps mirroring the old `By(...)`
     narration. Closures (`makeCR`, `markShardsReady`, `reconcileOnce`) become `t.Helper()` funcs.

3. **e2e** (`test/e2e/`, keeps the `e2e` build tag)
   - `e2e_suite_test.go` → `TestMain(m)`: build + load image, cert-manager skip, `m.Run()`, teardown.
   - `valkeycluster_test.go` → one `TestLifecycle(t)` with ordered `t.Run` subtests
     (provision → failover → reshard 3→5 → replica scale → scale-in 5→3). A small
     `eventually(t, timeout, interval, fn) error` poll helper replaces Gomega's `Eventually`.
   - Delete the scaffolded `e2e_test.go` ("Manager runs / metrics endpoint" smoke):
     operator-availability is already asserted in the lifecycle setup, and the project
     has no custom metrics, so the boilerplate adds no coverage.

### Ergonomics

- `KEEP_CLUSTER=true` → e2e `TestMain` skips teardown, so the live cluster can be
  watched (`kubectl get valkeycluster -w`, k9s) in a second pane during a run.
- Makefile: `test` unchanged; `test-e2e` becomes
  `go test -tags=e2e ./test/e2e/ -v -run TestLifecycle -timeout 30m` (no `-ginkgo.v`).
- `go mod tidy` drops ginkgo/gomega once unimported.
- A short "Running & debugging tests" doc section: IntelliJ gutter icons,
  `-run TestName/subtest` to target one case, `KEEP_CLUSTER`.

## Trade-off

Loses Ginkgo's spec-tree console output; gains idiomatic Go, IntelliJ-native per-test
run/debug, and a single mental model. The e2e rewrite carries the most risk, so envtest
is proven green with `make test` and the e2e suite is compile-checked (`go build -tags e2e`);
a full e2e run requires a kind cluster.
