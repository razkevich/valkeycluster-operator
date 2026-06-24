# Manual Verification (live, on kind)

Record of end-to-end verification against a real 3-node `kind` cluster
(`valkeycluster-dev`), exercising the three user stories and **using the Valkey instances**
directly. Reproduce with [quickstart.md](https://github.com/razkevich/valkeycluster-operator/blob/main/specs/001-valkeycluster-operator/quickstart.md).

## Setup
- Operator built and deployed via `make kind-deploy IMG=valkeycluster-operator:dev`.
- `kubectl apply -f config/samples/cache_v1alpha1_valkeycluster.yaml` (shards: 3, replicasPerShard: 1).

## US1 — Provision & use ✅
- CR reached `Ready` with `readyShards=3`.
- `valkey-cli --cluster check`: **3 primaries, 1 replica each, all 16384 slots covered.**
- Slot ranges: shard 0 `0-5460`, shard 1 `5461-10921`, shard 2 `10922-16383`.
- Pod anti-affinity held: each shard's two pods scheduled on different worker nodes.
- **Use test:** wrote 12 keys with `valkey-cli -c` (cluster mode); **read back 12/12** correctly.
  Keys hashed to slots in all three shards (e.g. 6657, 10850, 14915, 2724) — cross-shard
  `MOVED` redirects work.

## US2 — Automatic failover ✅
- Seeded 50 keys.
- `kubectl delete pod valkeycluster-sample-shard-1-0` (a primary).
- Within ~20s: a replica was promoted (cluster still **3 masters, all slots covered**); the
  deleted pod returned and rejoined as a replica.
- **Data intact: read back 50/50** keys after failover.

## US3 — Data-preserving resharding ✅
- Seeded 200 keys, then `kubectl patch ... '{"spec":{"shards":5}}'`.
- CR passed through `Resharding` and returned to `Ready` with `readyShards=5`.
- `valkey-cli --cluster check`: **exactly 5 primaries, all 16384 slots covered**; every
  shard reports `readyReplicas=1` (primaries `shard-0-0`..`shard-4-0`).
- **Data preserved: read back 200/200** keys after resharding.

## Notes
- Two Valkey-on-Kubernetes details the operator handles: `CLUSTER MEET` requires an IP (it resolves
  FQDN → IP while nodes still announce their hostname), and scale-out moves slots with a native
  slot-mover targeted at the new primary, never at empty masters, so replica pods don't become
  spurious primaries.
- These flows are also covered by an **automated e2e suite** (Go's std `testing`) in
  `test/e2e/valkeycluster_test.go` — a single ordered `TestLifecycle` with subtests for
  provision+use, failover, reshard 3→5, replica scaling, and scale-in 5→3, run against a real
  Valkey cluster on Kind. Run it with `make test-e2e` (creates the kind cluster if absent; the
  test installs/undeploys the operator and creates/deletes the CR itself). The reconcile decision
  logic is additionally covered by **envtest** (`internal/controller`, also std `testing` with a
  fake cluster) — no real cluster needed, just `make setup-envtest` once.

  Latest measured lifecycle wall-clock ~225s: provision 52s, failover 17s, reshard 3→5 52s, replica
  scaling 31s, scale-in 5→3 35s (these are operator-behavior timings, unchanged by the test harness).
- For a quick, human-readable smoke test against an **already-running** cluster, `bench/day2-matrix.sh`
  drives provision → scale-out 5 → scale-in 3 and reports read/write hit counts at every pivot
  (including live writes mid-reshard).

## Running & debugging the tests

All tests use Go's standard `testing` package (no Ginkgo/Gomega/testify), so every test and
`t.Run` subtest gets a native run/debug gutter icon in IntelliJ/GoLand and runs from the CLI.

| Layer | Command | Prerequisite |
|---|---|---|
| Unit | `go test ./internal/...` | none |
| Controller (envtest) | `make test` | `make setup-envtest` once (fetches the apiserver binaries; no cluster) |
| e2e (Kind) | `make test-e2e` | Kind installed (the target creates the cluster if absent) |

- **Target one case:** `go test ./internal/controller/ -run TestReconcile_ProvisionAndForm` — or a
  single subtest with `-run 'TestReconcile_ProvisionAndForm/phase_is_Provisioning'` (spaces become `_`).
- **Watch the cluster while e2e runs:** `make test-e2e KEEP_CLUSTER=true` leaves the Kind cluster and
  the deployed ValkeyCluster up after the run; tail it in another pane with `kubectl get valkeycluster -w`
  or k9s. Without it, the suite tears down the operator, the CR, and the Kind cluster.
- **e2e from the IDE:** the e2e files carry a `//go:build e2e` constraint (so a plain `go test ./...`
  never tries to spin up Kind). GoLand ignores tagged files until you enable the tag:
  Settings → Go → Build Tags & Vendoring → **Custom tags: `e2e`**. After that the file lights up and
  its gutter run/debug icons work. Running e2e directly (not via `make`) needs a Kind cluster already
  up with `kubectl` pointed at it.
