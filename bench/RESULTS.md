# Benchmark results

## What this measures

Raw cluster throughput and the durability-vs-latency cost of replica-acked writes, on a
live ValkeyCluster — the baseline numbers behind the performance discussion.

## How it was tested

- Run from **inside a cluster pod** (`valkeycluster-sample-shard-0-0`) so the client speaks the cluster protocol and
  follows `MOVED` redirects across shards.
- **Throughput:** `valkey-benchmark --cluster -t set/get -n 100000 -c 50` (requests/sec).
- **Durability vs latency:** 2000 sequential `SET`s, plain vs. each followed by `WAIT 1 100`
  (block until 1 replica acks), measuring the per-write cost of shrinking the data-loss window.

## Results

Topology: **shards=3, replicasPerShard=1**, 100000 requests, 50 clients.

## Throughput (cluster mode)

| Operation | Requests/sec |
|-----------|--------------|
| SET | 79365.08 |
| GET | 96246.39 |

## Durability vs. latency (2000 sequential writes)

| Mode | Total ms | Trade-off |
|------|----------|-----------|
| plain SET (async replication) | 2942 | fastest, may lose the last acked writes on failover |
| SET + WAIT 1 100 (replica-acked) | 5025 | slower, shrinks the data-loss window |

_Re-run after `kubectl patch valkeycluster valkeycluster-sample --type merge -p '{"spec":{"shards":N}}'` to compare shard counts (sharding write scale-out)._
