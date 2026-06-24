# Lever tradeoff sweep

## What this measures

That looser durability guarantees buy throughput: SET/GET ops/sec across three persistence
profiles (None → AOF everysec → AOF always), plus the per-write cost of `WAIT` (replica-acked).

## How it was tested

- ValkeyCluster **valkeycluster-sample** (replicasPerShard=1), io-threads 4 across all profiles.
- Each profile is applied by **patching the CR** and waiting for the config-hash rolling
  restart to land (gated on the live `CONFIG GET appendonly/appendfsync`, not just phase).
- Throughput: `valkey-benchmark --cluster -t set/get -n 500000 -c 50 -P 16`, run inside a pod.
- WAIT contrast: 2000 sequential `SET`s, plain vs. each followed by `WAIT 1 200`, on the
  loosest profile. The original spec is restored afterward.

## SET/GET throughput by durability profile

| Profile | persistence | appendfsync | SET req/s | GET req/s |
|---------|-------------|-------------|-----------|-----------|
| None (loosest) | None | everysec | 242130.75 | 283286.12 |
| AOF everysec | AOF | everysec | 202183.58 | 277161.88 |
| AOF always (safest) | AOF | always | 39869.23 | 296384.09 |

**Expected:** SET throughput falls as durability tightens (None ≥ everysec ≥ always).

| Check | Verdict |
|-------|---------|
| None ≥ AOF everysec | PASS |
| AOF everysec ≥ AOF always | PASS |

## Per-write durability cost (WAIT, 2000 sequential writes, loosest profile)

| Mode | Total ms | Trade-off |
|------|----------|-----------|
| plain SET (async replication) | 2908 | fastest; may lose the last acked writes on failover |
| SET + WAIT 1 200 (replica-acked) | 4946 | slower; shrinks the data-loss window |

**Expected:** WAIT 1 is slower than plain SET — verdict: PASS

_Verdicts use a 5% tolerance and are report-only. Tunables: REQUESTS, CLIENTS, PIPELINE._
