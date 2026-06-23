/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package cluster is the seam between the reconciler and a live Valkey cluster.
// The real implementation issues CLUSTER RPCs via go-redis and runs
// `valkey-cli --cluster` inside a pod for slot/key migration; a fake
// implementation backs unit and envtest runs so the reconciler is testable
// without a live Valkey (constitution principle II).
package cluster

import (
	"context"
	"fmt"
)

// TotalSlots is the fixed number of hash slots in a Valkey/Redis cluster.
const TotalSlots = 16384

// SlotRange is an inclusive range of hash slots.
type SlotRange struct {
	Start int
	End   int
}

// Endpoint addresses a single Valkey node. PodName/Namespace drive pod-exec for
// the valkey-cli migration commands.
type Endpoint struct {
	Host      string
	Port      int
	PodName   string
	Namespace string
}

// Addr returns the host:port dial address.
func (e Endpoint) Addr() string { return fmt.Sprintf("%s:%d", e.Host, e.Port) }

// NodeInfo is one node's view as parsed from CLUSTER NODES.
type NodeInfo struct {
	ID        string
	Host      string
	Port      int
	Flags     []string
	MasterID  string // "" when this node is a primary
	Slots     []SlotRange
	Connected bool
}

// IsPrimary reports whether the node is a primary (master).
func (n NodeInfo) IsPrimary() bool {
	for _, f := range n.Flags {
		if f == "master" {
			return true
		}
	}
	return n.MasterID == ""
}

// HasFlag reports whether the node carries the given flag (e.g. "fail").
func (n NodeInfo) HasFlag(flag string) bool {
	for _, f := range n.Flags {
		if f == flag {
			return true
		}
	}
	return false
}

// SlotCount returns the number of slots this node owns.
func (n NodeInfo) SlotCount() int {
	total := 0
	for _, r := range n.Slots {
		total += r.End - r.Start + 1
	}
	return total
}

// ClusterState is the observed cluster, derived from CLUSTER INFO + CLUSTER NODES.
type ClusterState struct {
	Formed       bool
	SlotsCovered bool
	// OpenSlots is true when any slot is mid-migration (importing/migrating) —
	// a slot is still "assigned" so coverage looks fine, but the cluster is not
	// in a stable state and reshard/rebalance will refuse until it's fixed.
	OpenSlots bool
	Nodes     []NodeInfo
}

// PrimaryByNodeID returns the primary node with the given ID, if present.
func (s ClusterState) PrimaryByNodeID(id string) (NodeInfo, bool) {
	for _, n := range s.Nodes {
		if n.ID == id && n.IsPrimary() {
			return n, true
		}
	}
	return NodeInfo{}, false
}

// RebalanceOpts controls a valkey-cli --cluster rebalance invocation.
type RebalanceOpts struct {
	// UseEmptyMasters pulls slots onto newly added empty primaries (scale-up).
	UseEmptyMasters bool
	// WeightZeroIDs drains these primaries to zero slots (scale-down).
	WeightZeroIDs []string
}

// ClusterAdmin orchestrates a Valkey cluster. All methods are idempotent and
// safe to call when the desired state already holds.
type ClusterAdmin interface {
	// State observes the cluster starting from a reachable seed node.
	State(ctx context.Context, seed Endpoint) (ClusterState, error)

	// MyID returns the node ID of the node at ep (CLUSTER MYID), dialing it directly.
	MyID(ctx context.Context, ep Endpoint) (string, error)

	// Meet introduces target into from's view of the cluster (CLUSTER MEET).
	Meet(ctx context.Context, from, target Endpoint) error
	// AddSlots assigns slot ranges to the primary (CLUSTER ADDSLOTSRANGE).
	AddSlots(ctx context.Context, primary Endpoint, ranges []SlotRange) error
	// Replicate makes replica a replica of primaryID (CLUSTER REPLICATE).
	Replicate(ctx context.Context, replica Endpoint, primaryID string) error
	// Forget removes nodeID from from's view (CLUSTER FORGET).
	Forget(ctx context.Context, from Endpoint, nodeID string) error
	// Failover triggers a planned switchover, promoting replica (CLUSTER FAILOVER).
	Failover(ctx context.Context, replica Endpoint) error

	// Rebalance migrates slots (and their keys) via valkey-cli --cluster rebalance.
	Rebalance(ctx context.Context, seed Endpoint, opts RebalanceOpts) error
	// Reshard moves exactly n slots (and their keys) to the primary identified by
	// toNodeID, via valkey-cli --cluster reshard. fromNodeID selects the source: a
	// specific primary's node ID, or "" for "all" other primaries. Targeting
	// specific nodes (vs. Rebalance(UseEmptyMasters)) keeps slot movement
	// deterministic and never hands slots to empty masters meant to be replicas.
	Reshard(ctx context.Context, seed Endpoint, fromNodeID, toNodeID string, n int) error
	// Fix repairs open/partially-migrated slots via valkey-cli --cluster fix.
	Fix(ctx context.Context, seed Endpoint) error
}
