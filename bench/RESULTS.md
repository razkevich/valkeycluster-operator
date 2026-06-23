# Benchmark results

Topology: **shards=5, replicasPerShard=1**, 50000 requests, 50 clients.

## Throughput (cluster mode)

| Operation | Requests/sec |
|-----------|--------------|
| SET | 199203.20 |
| GET | 200000.00 |

## Durability vs. latency (2000 sequential writes)

| Mode | Total ms | Trade-off |
|------|----------|-----------|
| plain SET (async replication) | 3343 | fastest, may lose the last acked writes on failover |
| SET + WAIT 1 100 (replica-acked) | 5423 | slower, shrinks the data-loss window |

_Re-run after `kubectl patch valkeycluster valkeycluster-sample --type merge -p '{"spec":{"shards":N}}'` to compare shard counts (sharding write scale-out)._
