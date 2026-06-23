# Clustering & HA: settings and their performance trade-offs

This operator manages a **Valkey Cluster** (cluster mode). This doc explains the design choices and
the knobs the operator exposes, and the trade-offs behind each — the substance the exercise asks for.

## Sharding vs. replication

The topology has two independent dials:

- **`shards`** — how many ways the keyspace (16384 hash slots) is partitioned. Each shard is an
  independent primary owning a slot range. More shards ⇒ more aggregate memory capacity and more
  write throughput (writes spread across primaries), at the cost of cross-slot operations
  (multi-key ops must stay within a slot/hash-tag) and resharding overhead when you change it.
- **`replicasPerShard`** — how many full copies each shard keeps for HA. Replicas add read capacity
  and failover safety; they do **not** add write throughput or capacity (every replica holds the
  full shard).

So **sharding scales writes/capacity; replication buys availability/read-scaling.** They compose:
a production cluster is `shards ≥ 3` (for a failover quorum) with `replicasPerShard ≥ 1`.

`shards: 1` is the degenerate "replication-only" case — one primary holding all slots plus replicas.
It behaves like a classic primary/replica HA setup, with the same auto-failover, and is why we did
**not** need a separate Sentinel mode: cluster-mode failover covers it. The one ergonomic cost is
that clients must still be **cluster-aware** even at `shards: 1`.

## Why `shards` must be 1 or ≥ 3

Cluster-mode failover is decided by a **vote among the primaries**. With 2 primaries there is no
majority when one fails, so failover can stall. The operator rejects `shards: 2` at admission;
meaningful values are `1` (no sharding) or `≥ 3`.

## Asynchronous replication and the data-loss window

Valkey cluster replication is **always asynchronous** — there is no synchronous/quorum-write mode.
A primary acknowledges a write before its replicas have it. If the primary dies in that window, the
just-acked write can be lost when a replica is promoted. This is the fundamental
**latency vs. durability** trade-off, and it is what the `haPolicy` knobs let you tune.

## `haPolicy` knobs

The spec groups the dials into three axes — **HA**, **Persistence**, and **Performance**.

### `haPolicy` — availability / consistency

| Field | Maps to | Trade-off | When to raise/change it |
|-------|---------|-----------|--------------------------|
| `minReplicasToWrite` | `min-replicas-to-write` | durability ↑ / write-availability ↓ | Set ≥1 to refuse writes when no replica can receive them — shrinks the data-loss window, but the primary stops accepting writes if replicas are down. |
| `requireFullCoverage` | `cluster-require-full-coverage` | correctness ↑ / availability ↓ | `true` (default): if any slot is unowned the whole cluster refuses writes (consistent). `false`: keep serving the reachable slots during partial failure/resharding. |
| `clusterNodeTimeoutMillis` | `cluster-node-timeout` | fast failover / false positives | Lower = quicker failover but more spurious failovers under load/latency; higher = steadier but slower recovery. Measured in `bench/FAILOVER-RESULTS.md`. |

### `persistence` — durability

| Field | Maps to | Trade-off |
|-------|---------|-----------|
| `mode` (`AOF`/`RDB`/`AOFAndRDB`/`None`) | `appendonly` + `save` | AOF = better durability (write log); RDB = compact + fast restart but loses writes since the last snapshot (and forks, doubling memory under write load); `AOFAndRDB` = both; `None` = pure cache, fastest, no disk durability. |
| `appendFsync` | `appendfsync` | durability ↑ / throughput ↓ — `always` fsyncs every write (safest, slowest); `everysec` (default) loses ≤1s on crash; `no` is fastest. Only applies when AOF is enabled. |

### `performance` — throughput / memory

| Field | Maps to | Trade-off |
|-------|---------|-----------|
| `ioThreads` | `io-threads` | Valkey-8 network I/O parallelism. Higher = more throughput on multi-core nodes (command execution stays single-threaded), at the cost of CPU. The signature Valkey-vs-Redis lever. |
| `maxmemoryPolicy` | `maxmemory-policy` | `noeviction` (default) rejects writes at `maxmemory` — correct for a **datastore**; `allkeys-lru`/`allkeys-lfu` evict — correct for a **cache** (LFU beats LRU on skewed/hot-key access); `volatile-*` evict only keys with a TTL. |

`maxmemory` itself is derived automatically at ~70% of the container memory limit (COW/buffer
headroom). Per-write durability can also be requested by the client with `WAIT <n> <ms>` (block
until `n` replicas ack) — demonstrated in the benchmark.

## Anti-affinity (HA that actually holds)

Replicas are pointless for HA if they share a node with their primary — one node failure would take
the whole shard down. The operator schedules each shard's pods with **preferred pod anti-affinity**
(`topologyKey: kubernetes.io/hostname`), so a shard's primary and replicas spread across nodes.

## What the benchmark shows

The benchmarks in `bench/` quantify these trade-offs:

1. **Throughput vs. shard count** — aggregate SET/GET ops/sec as shards scale (sharding scale-out).
2. **Durability vs. latency** — plain `SET` vs `SET` + `WAIT 1` (replica-acked). → `bench/RESULTS.md`.
3. **Failover window vs. `cluster-node-timeout`** — write-availability gap when a primary is killed,
   swept across node-timeout values. → `bench/FAILOVER-RESULTS.md`.
4. **`io-threads` scaling** and **`appendFsync`/persistence mode** — re-run `bench/benchmark.sh` after
   patching `performance.ioThreads` or `persistence` to compare (the `ioThreads` 1→4 sweep is the
   clearest Valkey-specific throughput signal).

`bench/benchmark.sh <cluster>` writes `bench/RESULTS.md`; `bench/failover-bench.sh <cluster>` writes
`bench/FAILOVER-RESULTS.md`.
