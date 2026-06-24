# Failover latency vs. cluster-node-timeout

## What this measures

The write-availability window when a shard's primary fails: how long clients cannot write
to that shard's keys, as a function of `cluster-node-timeout`. This is the evidence behind
the HA tradeoff — a lower timeout fails over faster but is more prone to false positives.

## How it was tested

- A cluster-aware probe runs from a **surviving pod** (`valkeycluster-sample-shard-2-0`): every ~50ms it issues a
  `SET` of a key owned by **shard 0** and records `ok` only when the reply is literally `OK`
  (a `CLUSTERDOWN`/redirect error counts as a failed write, not a success).
- Each round sets `cluster-node-timeout` live on every node, waits a baseline, then fails
  **shard 0's current primary**. Failure mode for this run: **kill** — clean crash — force-delete the primary pod (`SIGKILL`), so the port closes and peers detect the failure via connection-refused almost immediately.
- **Window** = ms from the first failed write to the first recovered write (≈ failure
  detection + replica election + promotion). The cluster is allowed to return to `Ready`
  between rounds. Probe writes are wrapped in a 1s `timeout` so a frozen node fails fast.

## Results

| cluster-node-timeout (ms) | failover window (ms) | failed writes during window |
|---|---|---|
| 15000 | 2696 | 46 |
| 5000 | 2773 | 47 |

**Reading it:** under a **soft** failure the window tracks `cluster-node-timeout` (you wait
the timeout to detect the dead node) — the tradeoff: a lower timeout recovers faster but is
likelier to misfire under transient latency or GC pauses. Under a **clean crash** (`kill`
mode) the window is near-constant and dominated by election, because connection-refused is
detected almost instantly regardless of the timeout.
