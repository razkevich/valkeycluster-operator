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

// Package topology decides what reconcile action a cluster needs to converge
// toward the declared topology. Pure logic, no I/O — this is the unit-testable
// core of the reconcile loop.
package topology

// Desired is the declared topology.
type Desired struct {
	Shards           int
	ReplicasPerShard int
}

// Observed is the live cluster summarized for decision-making.
type Observed struct {
	// Formed is true once the cluster has been bootstrapped.
	Formed bool
	// SlotsCovered is true when all 16384 slots are owned by reachable primaries.
	SlotsCovered bool
	// PrimaryCount is the number of shards (primaries) currently in the cluster.
	PrimaryCount int
	// ReplicaCounts is the in-sync replica count per existing shard.
	ReplicaCounts []int
}

// ActionKind enumerates the reconcile decisions.
type ActionKind string

const (
	ActionForm           ActionKind = "Form"
	ActionRepair         ActionKind = "Repair"
	ActionScaleOutShards ActionKind = "ScaleOutShards"
	ActionScaleInShards  ActionKind = "ScaleInShards"
	ActionScaleReplicas  ActionKind = "ScaleReplicas"
	ActionNone           ActionKind = "None"
)

// Plan is the decided next action.
type Plan struct {
	Kind                    ActionKind
	AddShards               int
	RemoveShards            int
	DesiredReplicasPerShard int
}

// Decide computes the single next action to take. Ordering of precedence:
//  1. Form an unformed cluster.
//  2. Repair an unhealthy (uncovered) cluster before any topology change
//     (stability gate, FR-024).
//  3. Change shard count (resharding) — takes precedence over replica changes.
//  4. Adjust replicas per shard.
//  5. Otherwise no change.
func Decide(d Desired, o Observed) Plan {
	if !o.Formed {
		return Plan{Kind: ActionForm}
	}
	if !o.SlotsCovered {
		return Plan{Kind: ActionRepair}
	}
	if d.Shards > o.PrimaryCount {
		return Plan{Kind: ActionScaleOutShards, AddShards: d.Shards - o.PrimaryCount}
	}
	if d.Shards < o.PrimaryCount {
		return Plan{Kind: ActionScaleInShards, RemoveShards: o.PrimaryCount - d.Shards}
	}
	for _, rc := range o.ReplicaCounts {
		if rc != d.ReplicasPerShard {
			return Plan{Kind: ActionScaleReplicas, DesiredReplicasPerShard: d.ReplicasPerShard}
		}
	}
	return Plan{Kind: ActionNone}
}
