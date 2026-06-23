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

package controller

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/razkevich/valkeycluster-operator/api/v1alpha1"
	"github.com/razkevich/valkeycluster-operator/internal/cluster"
	"github.com/razkevich/valkeycluster-operator/internal/slots"
	"github.com/razkevich/valkeycluster-operator/internal/topology"
)

// ---- observe / decide / act ----

// reconcileCluster runs one observe→decide→act→status pass. It returns
// progressing=true when a topology change (form/repair/scale) is still in flight,
// so the caller can requeue quickly instead of waiting the steady interval —
// turning a multi-minute reshard (one batch per steady requeue) into a tight loop.
func (r *ValkeyClusterReconciler) reconcileCluster(ctx context.Context, cr *cachev1alpha1.ValkeyCluster) (progressing bool, err error) {
	l := log.FromContext(ctx)
	seed := r.seed(cr)

	state, err := r.Admin.State(ctx, seed)
	if err != nil {
		return false, fmt.Errorf("observe cluster: %w", err)
	}

	observed := summarize(cr, state)
	plan := topology.Decide(topology.Desired{
		Shards:           int(cr.Spec.Shards),
		ReplicasPerShard: int(cr.Spec.ReplicasPerShard),
	}, observed)

	// Teardown of a departing shard is driven by which StatefulSets still exist,
	// not by the slot-owning primary count. Once a departing shard's slots have
	// fully drained, the live primary count already equals the desired shard
	// count, so topology.Decide stops reporting a scale-in — yet the emptied
	// StatefulSet still needs deleting. Whenever a StatefulSet with index >=
	// desired remains, force the scale-in path so scaleIn finishes the job
	// (drain any residual slots, then forget the nodes and delete the workload).
	if plan.Kind != topology.ActionForm && plan.Kind != topology.ActionScaleOutShards {
		excess, err := r.hasExcessShardStatefulSets(ctx, cr)
		if err != nil {
			return false, err
		}
		if excess {
			plan = topology.Plan{Kind: topology.ActionScaleInShards}
		}
	}

	// A form/repair/reshard is multi-step; signal the caller to requeue fast until it settles.
	progressing = plan.Kind == topology.ActionForm || plan.Kind == topology.ActionRepair ||
		plan.Kind == topology.ActionScaleOutShards || plan.Kind == topology.ActionScaleInShards

	switch plan.Kind {
	case topology.ActionForm:
		l.Info("forming cluster")
		_ = r.setPhase(ctx, cr, cachev1alpha1.PhaseForming, "Forming", "bootstrapping cluster")
		if err := r.formCluster(ctx, cr); err != nil {
			return false, err
		}
	case topology.ActionRepair:
		_ = r.setPhase(ctx, cr, cachev1alpha1.PhaseResharding, "Repairing", "fixing open slots")
		// Deterministic, multi-way-safe slot finalization (see ClusterAdmin.RepairSlots).
		// We do NOT use `valkey-cli --cluster fix`: it can't untangle a slot that
		// several nodes are importing/migrating at once (which interrupted reshards
		// produce), so it would loop forever.
		n, err := r.Admin.RepairSlots(ctx, seed)
		if err != nil {
			l.Info("repair-slots reported an error (will re-check next reconcile)", "err", err.Error())
		} else {
			l.Info("repaired open slots", "count", n)
		}
	case topology.ActionScaleOutShards:
		l.Info("scaling out shards", "add", plan.AddShards)
		_ = r.setPhase(ctx, cr, cachev1alpha1.PhaseResharding, "Resharding", "adding shards")
		if err := r.scaleOut(ctx, cr, observed.PrimaryCount); err != nil {
			return false, err
		}
	case topology.ActionScaleInShards:
		l.Info("scaling in shards", "remove", plan.RemoveShards)
		_ = r.setPhase(ctx, cr, cachev1alpha1.PhaseResharding, "Resharding", "removing shards")
		if err := r.scaleIn(ctx, cr, state, observed.PrimaryCount); err != nil {
			return false, err
		}
	case topology.ActionScaleReplicas:
		l.Info("reconciling replicas")
		_ = r.setPhase(ctx, cr, cachev1alpha1.PhaseScalingReplicas, "ScalingReplicas", "adjusting replicas")
		if err := r.reconcileMembership(ctx, cr, state); err != nil {
			return false, err
		}
	case topology.ActionNone:
		// keep replication wiring healthy (rejoin after failover, forget stale)
		if err := r.reconcileMembership(ctx, cr, state); err != nil {
			return false, err
		}
	}

	// Refresh and publish status.
	state, err = r.Admin.State(ctx, seed)
	if err != nil {
		return false, fmt.Errorf("observe cluster (post-action): %w", err)
	}
	return progressing, r.updateStatus(ctx, cr, state)
}

// formCluster bootstraps a fresh cluster: meet all, assign slots to per-shard
// primaries (ordinal 0), then attach replicas.
func (r *ValkeyClusterReconciler) formCluster(ctx context.Context, cr *cachev1alpha1.ValkeyCluster) error {
	seed := r.seed(cr)
	for _, ep := range r.allEndpoints(cr) {
		if ep.Host == seed.Host {
			continue
		}
		if err := r.Admin.Meet(ctx, seed, ep); err != nil {
			return fmt.Errorf("meet %s: %w", ep.Host, err)
		}
	}
	dist := slots.Distribute(int(cr.Spec.Shards))
	for i := 0; i < int(cr.Spec.Shards); i++ {
		if err := r.Admin.AddSlots(ctx, r.endpoint(cr, i, 0), []cluster.SlotRange{dist[i]}); err != nil {
			return fmt.Errorf("addslots shard %d: %w", i, err)
		}
	}
	state, err := r.Admin.State(ctx, seed)
	if err != nil {
		return err
	}
	return r.attachReplicas(ctx, cr, state)
}
