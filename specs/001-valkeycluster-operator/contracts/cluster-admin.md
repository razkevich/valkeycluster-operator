# Contract: internal `ClusterAdmin` interface (`internal/cluster`)

The seam between the reconciler and the Valkey cluster. A real `go-redis`/pod-exec implementation
backs production; a fake in-memory implementation backs unit/envtest. This is what makes the
reconciler testable without a live Valkey (Constitution Principle II).

```go
package cluster

// ClusterState is derived from CLUSTER INFO + CLUSTER NODES.
type ClusterState struct {
    Formed       bool
    SlotsCovered bool        // all 16384 slots owned by a reachable primary
    Nodes        []NodeInfo
}

type NodeInfo struct {
    ID        string
    Host      string        // announced FQDN
    Port      int
    Flags     []string      // "master","slave","fail","myself",...
    MasterID  string        // "" if primary
    Slots     []SlotRange
    Connected bool
}

type SlotRange struct{ Start, End int }

// ClusterAdmin orchestrates a Valkey cluster. All methods are idempotent /
// safe to call when the desired state already holds.
type ClusterAdmin interface {
    // Observe
    State(ctx context.Context, seed Endpoint) (ClusterState, error)

    // Form / membership (go-redis RPCs)
    Meet(ctx context.Context, from, target Endpoint) error
    AddSlots(ctx context.Context, primary Endpoint, ranges []SlotRange) error
    Replicate(ctx context.Context, replica Endpoint, primaryID string) error
    Forget(ctx context.Context, from Endpoint, nodeID string) error
    Failover(ctx context.Context, replica Endpoint) error   // planned switchover

    // Slot/key migration (valkey-cli --cluster via pod-exec)
    Rebalance(ctx context.Context, seed Endpoint, opts RebalanceOpts) error
    Fix(ctx context.Context, seed Endpoint) error            // repair open slots
}

type Endpoint struct{ Host string; Port int; PodName string }  // PodName drives pod-exec

type RebalanceOpts struct {
    UseEmptyMasters bool                 // scale-up: pull slots onto new empty primaries
    WeightZeroIDs   []string             // scale-down: drain these primaries to 0
}
```

**Fake contract**: the fake maintains an in-memory node/slot map; `Meet`/`AddSlots`/`Replicate`
mutate it; `State` returns it; `Rebalance`/`Fix` recompute an even slot split. Used to assert the
reconciler issues the right actions in the right order under each topology transition.
