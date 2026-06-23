# Failover latency vs. cluster-node-timeout

A cluster-aware probe writes a shard-0 key every ~50ms from a surviving pod while
shard 0's primary is killed. **Window** = ms from the first failed write to the first
recovered write (≈ failure detection + replica election + promotion).

| cluster-node-timeout (ms) | failover window (ms) | failed writes during window |
|---|---|---|
| 15000 | 2428 | 24 |
| 5000 | 1629 | 14 |

**Tradeoff:** a lower `cluster-node-timeout` shortens the unavailability window (faster
failover) but makes the cluster more likely to trigger *false-positive* failovers under
transient network latency or GC pauses. The default (5000ms) balances the two.
