# Manual Verification (live, on kind)

Record of end-to-end verification against a real 3-node `kind` cluster
(`valkeycluster-dev`), exercising the three user stories and **using the Valkey instances**
directly. Reproduce with [quickstart.md](../specs/001-valkeycluster-operator/quickstart.md).

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
- `valkey-cli --cluster check`: **5 primaries, all 16384 slots covered**, each shard with its replica.
- **Data preserved: read back 200/200** keys after resharding.

## Notes
- Two real bugs were found and fixed during this verification (see commit history): `CLUSTER MEET`
  requires an IP (resolve FQDN → IP; nodes still announce hostname), and scale-out had to join only
  the new primary before `rebalance --use-empty-masters` so replicas don't become spurious primaries.
- Automated kind e2e (`make test-e2e`) for these flows is a known follow-up; behavior here is
  verified manually and the reconcile decision logic is covered by envtest with a fake cluster.
