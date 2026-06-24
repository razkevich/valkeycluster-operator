# Settings for Performance and High Availability

The operator's performance and availability behavior is controlled at two layers: the `ValkeyCluster`
custom resource (rendered into `valkey.conf`), and the node/OS beneath it. This page is the single
reference for both, with the trade-off behind each setting.

## Topology: sharding vs. replication

Two independent dials:

- **`shards`** — how many ways the keyspace (16384 hash slots) is partitioned. Each shard is a
  primary owning a slot range. More shards ⇒ more aggregate capacity and write throughput, at the
  cost of cross-slot operations (multi-key ops must share a slot/hash-tag) and resharding overhead.
- **`replicasPerShard`** — how many full copies each shard keeps. Replicas add read capacity and
  failover safety; they do **not** add write throughput or capacity (every replica holds the full shard).

So **sharding scales writes/capacity; replication buys availability/read-scaling.**

- `shards` must be **1 or ≥ 3** (the operator rejects `2`): cluster failover is decided by a vote
  among the primaries, and with 2 there is no majority when one dies. `shards: 1` is the
  replication-only HA case — one primary plus replicas, with the same auto-failover, which is why no
  separate Sentinel mode is needed.
- Replication is **always asynchronous** — a primary acks a write before its replicas have it. If it
  dies in that window, the acked write can be lost on promotion. This latency-vs-durability window is
  what the `haPolicy` knobs below tune.
- **One role per node** — a node is a master (owns slots) or a replica of a single master, never both.
  Unlike MongoDB, it can't be primary for some slots and replica for others.

## CR settings

The spec groups the dials into three axes — **HA**, **persistence**, and **performance**.

### `haPolicy` — availability / consistency

| Field | Maps to | Trade-off | When to change |
|-------|---------|-----------|----------------|
| `minReplicasToWrite` | `min-replicas-to-write` | durability ↑ / write-availability ↓ | Set ≥1 to refuse writes when no replica can receive them — shrinks the data-loss window, but the primary stops accepting writes if replicas are down. |
| `requireFullCoverage` | `cluster-require-full-coverage` | correctness ↑ / availability ↓ | `true` (default): if any slot is unowned the whole cluster refuses commands. `false`: keep serving the reachable slots during partial failure/resharding. |
| `clusterNodeTimeoutMillis` | `cluster-node-timeout` | fast failover / false positives | Lower = quicker failover but more spurious failovers under load/latency; higher = steadier but slower recovery. |

### `persistence` — durability

| Field | Maps to | Trade-off |
|-------|---------|-----------|
| `mode` (`AOF`/`RDB`/`AOFAndRDB`/`None`) | `appendonly` + `save` | AOF = durable write log; RDB = compact + fast restart but loses writes since the last snapshot (and forks, doubling memory under write load); `AOFAndRDB` = both; `None` = pure cache, fastest, no disk durability. |
| `appendFsync` | `appendfsync` | durability ↑ / throughput ↓ — `always` fsyncs every write (safest, slowest); `everysec` (default) loses ≤1s on crash; `no` is fastest. Applies only when AOF is enabled. |

### `performance` — throughput / memory

| Field | Maps to | Trade-off |
|-------|---------|-----------|
| `ioThreads` | `io-threads` | Valkey-8 network I/O parallelism. Higher = more throughput on multi-core nodes (command execution stays single-threaded), at the cost of CPU. The signature Valkey-vs-Redis lever. |
| `maxmemoryPolicy` | `maxmemory-policy` | `noeviction` (default) rejects writes at `maxmemory` — correct for a **datastore**; `allkeys-lru`/`allkeys-lfu` evict — correct for a **cache** (LFU beats LRU on skewed access); `volatile-*` evict only keys with a TTL. |

`maxmemory` is derived automatically at ~70% of the container memory limit (headroom for the
persistence fork's copy-on-write and client buffers). Per-write durability can also be requested by
the client with `WAIT <n> <ms>` (block until `n` replicas ack) — a per-command primitive, not a config
field, demonstrated in the benchmark.

### HA settings left at Valkey defaults

The operator exposes the headline failover/durability levers; these related ones currently use Valkey
defaults (not yet tunable via the CR): `min-replicas-max-lag` (10s — the lag bound that companions
`min-replicas-to-write`), `cluster-replica-validity-factor` (whether a stale replica may promote),
and `cluster-migration-barrier` / `cluster-allow-replica-migration` (auto-covering an orphaned primary).

## Node & OS-level settings

Some performance and HA behavior lives below the CR. Anti-affinity is handled by the operator; the
rest are node prerequisites applied via a `Tuned` profile, node bootstrap, or `securityContext.sysctls`.

| Setting | Recommendation | Why it matters |
|---|---|---|
| **Pod anti-affinity** | operator-managed (`topologyKey: kubernetes.io/hostname`) | A shard's replicas must not share a node with its primary, or one node failure takes the whole shard down. |
| **Transparent Huge Pages** | `never` | THP causes large, unpredictable latency spikes for an in-memory store. The most important node fix. |
| **Swap** | off (or `vm.swappiness=0`) | A swapped-out hot dataset turns microsecond ops into millisecond ones. |
| **`net.core.somaxconn` / `tcp-backlog`** | raise together (1024+) | A small accept backlog drops connections under burst; Valkey's backlog must not exceed the kernel's. |
| **File descriptors (`nofile`)** | high (e.g. 100k) | `maxclients` is bounded by the fd limit. |
| **CPU governor / NUMA** | `performance`, avoid throttling / pinning | Latency-sensitive; cross-NUMA access and CFS throttling add jitter. |
| **Guaranteed QoS** | `requests == limits` | No CPU throttling; last to be evicted under node pressure. |
| **PodDisruptionBudget** | `maxUnavailable: 1` | A node drain or rolling upgrade never breaks a shard's quorum. |

## Worked example: in-memory cache (must stay up, data loss acceptable)

A common profile: the cluster must stay available, but losing data on failure is fine. That single
fact lets you drop durability work and spend the budget on availability and speed.

```yaml
spec:
  shards: 3
  replicasPerShard: 1            # availability (fast failover), not durability
  persistence:
    mode: None                   # no AOF fsync, no RDB fork — max throughput, no COW spike
  performance:
    ioThreads: 4
    maxmemoryPolicy: allkeys-lfu # it's a cache: evict cold keys under pressure
  haPolicy:
    minReplicasToWrite: 0        # never block a write for durability we don't want
    requireFullCoverage: false   # keep serving the slots that are up — the headline "stay up" lever
    clusterNodeTimeoutMillis: 5000
```

The non-obvious parts: with no persistence there's no fork, so `maxmemory` can sit closer to the
limit; replicas stay (for failover, not durability) behind a `PodDisruptionBudget` + anti-affinity so
node drains keep quorum; and the runtime benefits from `lazyfree-lazy-*` (non-blocking eviction) and
reading from replicas for read-scaling.

## What the benchmark shows

The benchmarks in `bench/` quantify these trade-offs:

1. **Throughput vs. shard count** — aggregate SET/GET ops/sec as shards scale → `bench/RESULTS.md`.
2. **Durability vs. latency** — plain `SET` vs `SET` + `WAIT 1` (replica-acked) → `bench/RESULTS.md`.
3. **Failover window vs. `cluster-node-timeout`** — write-availability gap when a primary is killed,
   swept across node-timeout values → `bench/FAILOVER-RESULTS.md`.
4. **`io-threads` scaling** and **`appendFsync`/persistence mode** — re-run `bench/benchmark.sh` after
   patching `performance.ioThreads` or `persistence` (the `ioThreads` 1→4 sweep is the clearest
   Valkey-specific throughput signal).
