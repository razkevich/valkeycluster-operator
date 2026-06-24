# Operator Best Practices & Edge Cases

A clustering operator's correctness lives in the gaps between "it compiled" and "it survived a real
failover or reshard." This page is the patterns the reconcile loop uses to stay correct under crashes,
races, and partial failures — and the edge cases each one closes.

Reconciliation is **level-triggered**: `Reconcile` is never told *what changed* — it reads the full
current state and converges. So it is **idempotent** (safe to run any number of times) and
**convergent** (a dropped or duplicated trigger can't corrupt state — the next pass re-derives
everything from reality). The decision itself is a **pure function** (`topology.Decide`), tested
independently of the messy "talk to a live cluster" code.

## Patterns that prevent edge cases

The meta-rule: **never act on an assumption you can cheaply verify.** Almost every edge case is closed
by reading authoritative state instead of trusting a name, an ordinal, a count, or one node's view.

1. **Read live state first — never assume identity, role, or position.** The shard primary is found by
   dialing pods (`CLUSTER MYID`) + matching slot ownership, not assumed to be ordinal 0 (any pod can
   be primary after failover). And the loop won't touch the cluster until all pods are `Ready`.
2. **Stability gate — repair before you reshape.** If any slot is open or coverage is incomplete, run
   `RepairSlots` and requeue first. A reshard on an unstable cluster produces multi-way open slots no
   single command can untangle.
3. **Bounded, resumable progress.** Migration runs in bounded batches and returns after each, so the
   next reconcile continues from fresh state. A transient stall costs one short retry, not a wedge.
4. **Query every node where state isn't gossiped.** Open-slot markers live **only on the owning node's
   own `CLUSTER NODES` line** — detecting a stuck migration means querying every node, not the seed.
5. **Drive teardown off what exists, not a derived count.** Removing a shard is gated on which
   **StatefulSets still exist** — once its slots drain, the primary count already equals desired, so a
   count-driven loop would leave the emptied workload behind forever.
6. **Idempotent and safe under concurrency.** Leader election gives one active controller;
   `MIGRATE … REPLACE` and masters-only `SETSLOT` make migration re-runnable; status writes use
   `RetryOnConflict`; transient errors requeue with backoff.

## Concrete edge cases and how they're handled

| Edge case | How the loop handles it |
|---|---|
| **Operator crashes mid-reshard** | On restart: observe → stability gate runs `RepairSlots`; the bounded drain resumes from current state |
| **Spec changed while resharding** | Each pass re-derives one action from *current* state + *latest* spec; converges to the newest topology |
| **Pod rescheduled / IP changes** | Nodes announce a stable hostname; `MEET` resolves FQDN→IP; identity persists in `nodes.conf` on the PVC |
| **Fast primary restart** | The same node rejoins before `cluster-node-timeout` and resumes as primary — no needless failover |
| **Replica briefly an empty master** | Only slot-owning masters count as shards; slots go to specific new primaries, never to empty masters |
| **Departing shard drained but not deleted** | Teardown is driven by which StatefulSets exist (index ≥ desired) |
| **Stale node lingers in gossip** | A removed node is `FORGET`-ed from **every** surviving pod, not just the seed |
| **`Ready` but writes rejected** | Brief `CLUSTERDOWN` while coverage gossips — clients retry (the standard cluster-client contract) |
