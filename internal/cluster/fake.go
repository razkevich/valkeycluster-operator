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

package cluster

import (
	"context"
	"sort"
	"sync"
)

// Fake is an in-memory ClusterAdmin used by unit and envtest runs. It models
// nodes keyed by their announced host (which also serves as the node ID), tracks
// slot ownership and replication, and recomputes an even slot split on Rebalance.
type Fake struct {
	mu    sync.Mutex
	nodes map[string]*NodeInfo // key: host == node ID
}

var _ ClusterAdmin = (*Fake)(nil)

// NewFake returns an empty fake cluster.
func NewFake() *Fake { return &Fake{nodes: map[string]*NodeInfo{}} }

func (f *Fake) ensure(e Endpoint) *NodeInfo {
	n, ok := f.nodes[e.Host]
	if !ok {
		n = &NodeInfo{ID: e.Host, Host: e.Host, Port: e.Port, Connected: true, Flags: []string{"master"}}
		f.nodes[e.Host] = n
	}
	return n
}

func (f *Fake) assignedSlots() int {
	total := 0
	for _, n := range f.nodes {
		total += n.SlotCount()
	}
	return total
}

// State returns the current in-memory view.
func (f *Fake) State(_ context.Context, _ Endpoint) (ClusterState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	nodes := make([]NodeInfo, 0, len(f.nodes))
	for _, n := range f.nodes {
		cp := *n
		cp.Slots = append([]SlotRange(nil), n.Slots...)
		cp.Flags = append([]string(nil), n.Flags...)
		nodes = append(nodes, cp)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	assigned := f.assignedSlots()
	return ClusterState{
		Formed:       len(f.nodes) > 0 && assigned > 0,
		SlotsCovered: assigned == TotalSlots,
		Nodes:        nodes,
	}, nil
}

// MyID returns the node ID (== host) for the endpoint, creating it if unseen.
func (f *Fake) MyID(_ context.Context, ep Endpoint) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ensure(ep).ID, nil
}

// Meet adds both endpoints to the membership view.
func (f *Fake) Meet(_ context.Context, from, target Endpoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensure(from)
	f.ensure(target)
	return nil
}

// AddSlots assigns ranges to a primary.
func (f *Fake) AddSlots(_ context.Context, primary Endpoint, ranges []SlotRange) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := f.ensure(primary)
	n.MasterID = ""
	n.Flags = []string{"master"}
	n.Slots = append(n.Slots, ranges...)
	return nil
}

// Replicate makes replica a replica of primaryID.
func (f *Fake) Replicate(_ context.Context, replica Endpoint, primaryID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := f.ensure(replica)
	n.MasterID = primaryID
	n.Flags = []string{"slave"}
	n.Slots = nil
	return nil
}

// Forget removes a node from the view.
func (f *Fake) Forget(_ context.Context, _ Endpoint, nodeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.nodes, nodeID)
	return nil
}

// Failover promotes the replica: it takes its master's slots and they swap roles.
func (f *Fake) Failover(_ context.Context, replica Endpoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.nodes[replica.Host]
	if !ok || r.MasterID == "" {
		return nil
	}
	m, ok := f.nodes[r.MasterID]
	if !ok {
		return nil
	}
	r.Slots, m.Slots = m.Slots, nil
	r.MasterID, m.MasterID = "", r.ID
	r.Flags, m.Flags = []string{"master"}, []string{"slave"}
	return nil
}

// primaries returns master node IDs sorted for determinism.
func (f *Fake) primaries(excluded map[string]bool) []string {
	ids := []string{}
	for id, n := range f.nodes {
		if n.IsPrimary() && !excluded[id] {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// Rebalance redistributes the full slot space evenly across the eligible
// primaries (excluding any drained to weight zero).
func (f *Fake) Rebalance(_ context.Context, _ Endpoint, opts RebalanceOpts) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	excluded := map[string]bool{}
	for _, id := range opts.WeightZeroIDs {
		excluded[id] = true
	}
	targets := f.primaries(excluded)
	if len(targets) == 0 {
		return nil
	}
	// clear all slots, then assign an even contiguous split
	for _, n := range f.nodes {
		n.Slots = nil
	}
	for _, id := range opts.WeightZeroIDs {
		if n, ok := f.nodes[id]; ok {
			n.Slots = nil
		}
	}
	k := len(targets)
	for i, id := range targets {
		start := i * TotalSlots / k
		end := (i+1)*TotalSlots/k - 1
		f.nodes[id].Slots = []SlotRange{{Start: start, End: end}}
	}
	return nil
}

// Reshard (fake) moves n slots to toNodeID. With fromNodeID set, slots come from
// that node; otherwise the keyspace is re-split evenly across slot-owning
// primaries (used by scale-out). The fake tracks slot counts, not exact ranges.
func (f *Fake) Reshard(_ context.Context, _ Endpoint, fromNodeID, toNodeID string, n int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	to, ok := f.nodes[toNodeID]
	if !ok {
		return nil
	}
	to.MasterID = ""
	to.Flags = []string{"master"}

	if fromNodeID != "" {
		// move up to n slots from the named source to the target
		from, ok := f.nodes[fromNodeID]
		if !ok {
			return nil
		}
		moved := n
		if c := from.SlotCount(); c < moved {
			moved = c
		}
		from.Slots = countToRanges(from.SlotCount() - moved)
		to.Slots = countToRanges(to.SlotCount() + moved)
		return nil
	}

	// fromNodeID == "" : even re-split across all slot-owning primaries (+ target)
	if to.SlotCount() == 0 {
		to.Slots = []SlotRange{{Start: 0, End: 0}}
	}
	owners := []string{}
	for id, nd := range f.nodes {
		if nd.IsPrimary() && nd.SlotCount() > 0 {
			owners = append(owners, id)
		}
	}
	sort.Strings(owners)
	for _, nd := range f.nodes {
		if nd.IsPrimary() {
			nd.Slots = nil
		}
	}
	k := len(owners)
	for i, id := range owners {
		f.nodes[id].Slots = []SlotRange{{Start: i * TotalSlots / k, End: (i+1)*TotalSlots/k - 1}}
	}
	return nil
}

// countToRanges encodes a slot count as a single synthetic range (the fake only
// cares about counts/ownership, not exact slot identity).
func countToRanges(count int) []SlotRange {
	if count <= 0 {
		return nil
	}
	return []SlotRange{{Start: 0, End: count - 1}}
}

// RepairSlots is a no-op for the fake: it moves slots atomically, so it never
// produces open/mid-migration slots.
func (f *Fake) RepairSlots(_ context.Context, _ Endpoint) (int, error) { return 0, nil }

// Fix ensures full coverage by rebalancing across current primaries.
func (f *Fake) Fix(ctx context.Context, seed Endpoint) error {
	return f.Rebalance(ctx, seed, RebalanceOpts{})
}
