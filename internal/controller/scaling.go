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
	"sort"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/razkevich/valkeycluster-operator/api/v1alpha1"
	"github.com/razkevich/valkeycluster-operator/internal/cluster"
	"github.com/razkevich/valkeycluster-operator/internal/resources"
)

// drainBatchSlots bounds how many slots a single scale-in drain moves per
// reconcile. Bounded so any one reshard stays short and a transient MIGRATE stall
// costs a small retry rather than aborting a large drain; the controller requeues
// fast between batches (reshardRequeue), so a larger batch + tight loop drains a
// whole shard in seconds for sparse data instead of minutes.
const drainBatchSlots = 1024

// scaleOut joins each new shard's primary and replicas, attaches the replicas,
// then moves each new primary its fair share of slots with the native slot-mover
// (ClusterAdmin.MoveSlots), pulling from the fattest primary first. Replicas are
// attached BEFORE slots move so a new primary has in-sync replicas before it holds
// data — otherwise MIGRATE into it fails with NOREPLICAS when minReplicasToWrite>=1.
// MoveSlots is deterministic where valkey-cli reshard is not (the latter refuses on
// uneven/interrupted distributions), so scale-out converges from any state.
// Idempotent: a primary already holding its share is skipped, so a retry never over-assigns.
func (r *ValkeyClusterReconciler) scaleOut(ctx context.Context, cr *cachev1alpha1.ValkeyCluster, oldShards int) error {
	seed := r.seed(cr)
	desired := int(cr.Spec.Shards)
	perShard := cluster.TotalSlots / desired

	// 1. Join each new shard's primary AND replicas, and attach the replicas before
	// moving any slots (attaching an empty primary's replicas is instant, and gives
	// it the in-sync replica MIGRATE needs under minReplicasToWrite>=1).
	for i := oldShards; i < desired; i++ {
		for j := 0; j <= int(cr.Spec.ReplicasPerShard); j++ {
			if err := r.Admin.Meet(ctx, seed, r.endpoint(cr, i, j)); err != nil {
				return err
			}
		}
	}
	state, err := r.Admin.State(ctx, seed)
	if err != nil {
		return err
	}
	if err := r.attachReplicas(ctx, cr, state); err != nil {
		return err
	}

	// 2. Move each new primary its fair share with the native slot-mover, pulling
	// from the fattest primary first.
	for i := oldShards; i < desired; i++ {
		id, err := r.Admin.MyID(ctx, r.endpoint(cr, i, 0))
		if err != nil || id == "" {
			return fmt.Errorf("resolve new primary %d id: %w", i, err)
		}
		st, err := r.Admin.State(ctx, seed)
		if err != nil {
			return err
		}
		idx := byID(st)
		need := perShard - idx[id].SlotCount()
		for need > 0 {
			srcID, srcSlots := fattestPrimary(idx, id)
			if srcID == "" || srcSlots == 0 {
				break // no source has slots to give
			}
			take := need
			if take > srcSlots {
				take = srcSlots
			}
			moved, err := r.Admin.MoveSlots(ctx, seed, srcID, id, take)
			if err != nil {
				return fmt.Errorf("fill shard %d: %w", i, err)
			}
			if moved == 0 {
				break
			}
			need -= moved
			st, err = r.Admin.State(ctx, seed)
			if err != nil {
				return err
			}
			idx = byID(st)
		}
	}

	// 3. Re-attach replicas (roles may have settled after the move).
	state, err = r.Admin.State(ctx, seed)
	if err != nil {
		return err
	}
	return r.attachReplicas(ctx, cr, state)
}

// fattestPrimary returns the slot-owning primary with the most slots, excluding
// excludeID — the source scale-out pulls from when filling a new primary.
func fattestPrimary(idx map[string]cluster.NodeInfo, excludeID string) (string, int) {
	bestID, best := "", 0
	for id, n := range idx {
		if id == excludeID || !n.IsPrimary() {
			continue
		}
		if c := n.SlotCount(); c > best {
			bestID, best = id, c
		}
	}
	return bestID, best
}

// scaleIn removes every shard whose index is >= desired: it drains each
// departing shard's slots onto a surviving primary with the native slot-mover
// (deterministic and retry-safe — moving whole slots leaves no open-slot
// residue), then forgets the shard's nodes everywhere and deletes its
// StatefulSet + PVCs. State is re-read each iteration so drained slot counts and
// current roles are always fresh.
//
// Departing shards are taken from the StatefulSets that actually exist, NOT from
// a contiguous [desired, count) range. Once an inner shard (say index 3) drains
// to zero and is deleted while a higher shard (index 4) still owns slots, the
// live primary count drops to 4 — a count-bounded loop would then only revisit
// index 3 (already gone) and never touch index 4, wedging the scale-in forever.
// Keying off the real StatefulSet set makes the drain converge regardless of
// which shard was removed first.
func (r *ValkeyClusterReconciler) scaleIn(ctx context.Context, cr *cachev1alpha1.ValkeyCluster, _ cluster.ClusterState, _ int) error {
	seed := r.seed(cr)
	desired := int(cr.Spec.Shards)

	// First, sweep any lingering nodes from already-departed shards. A deleted
	// shard's pods are gone, but its node entries survive in gossip until every
	// surviving node forgets them — otherwise go-redis keeps dialing the dead
	// pods (DNS failures) and the stale entries reappear via gossip.
	if st, err := r.Admin.State(ctx, seed); err == nil {
		r.forgetStaleNodes(ctx, cr, st)
	}

	idxs, err := r.existingShardIndexes(ctx, cr)
	if err != nil {
		return err
	}
	for _, i := range idxs {
		if i < desired {
			continue // surviving shard
		}
		st, err := r.Admin.State(ctx, seed)
		if err != nil {
			return err
		}
		idx := byID(st)

		departID, _ := r.findShardPrimary(ctx, cr, i, idx)
		if departID != "" && idx[departID].SlotCount() > 0 {
			survID := r.firstSurvivorPrimary(ctx, cr, idx, departID, desired)
			if survID == "" {
				return fmt.Errorf("no surviving primary to drain shard %d into", i)
			}
			// Drain in a bounded batch via the native Go slot-mover (idempotent,
			// no valkey-cli pre-check refusals). Each batch commits real progress;
			// return after one batch so the next reconcile re-reads fresh state and
			// continues. The shard isn't deleted until it owns 0 slots.
			batch := drainBatchSlots
			if c := idx[departID].SlotCount(); c < batch {
				batch = c
			}
			log.FromContext(ctx).Info("draining shard", "shard", i,
				"departID", departID, "departSlots", idx[departID].SlotCount(),
				"survID", survID, "batch", batch)
			moved, err := r.Admin.MoveSlots(ctx, seed, departID, survID, batch)
			if err != nil {
				return fmt.Errorf("drain shard %d (moved %d before error): %w", i, moved, err)
			}
			log.FromContext(ctx).Info("drained batch", "shard", i, "moved", moved)
			return nil
		}
		// shard i now owns no slots — forget its nodes everywhere, then delete workload + PVCs.
		for j := 0; j <= int(cr.Spec.ReplicasPerShard); j++ {
			if id, err := r.Admin.MyID(ctx, r.endpoint(cr, i, j)); err == nil && id != "" {
				r.forgetEverywhere(ctx, cr, id)
			}
		}
		if err := r.deleteShard(ctx, cr, i); err != nil {
			return err
		}
	}
	return nil
}

// hasExcessShardStatefulSets reports whether any shard StatefulSet has an index
// >= the desired shard count — i.e. a departing shard whose teardown is not yet
// finished (slots may still be draining, or it may be drained-but-not-deleted).
func (r *ValkeyClusterReconciler) hasExcessShardStatefulSets(ctx context.Context, cr *cachev1alpha1.ValkeyCluster) (bool, error) {
	idxs, err := r.existingShardIndexes(ctx, cr)
	if err != nil {
		return false, err
	}
	desired := int(cr.Spec.Shards)
	for _, i := range idxs {
		if i >= desired {
			return true, nil
		}
	}
	return false, nil
}

// existingShardIndexes returns the shard indexes that currently have a
// StatefulSet, sorted ascending. Scale-in keys off this real set rather than a
// contiguous [desired, count) range (see scaleIn for why).
func (r *ValkeyClusterReconciler) existingShardIndexes(ctx context.Context, cr *cachev1alpha1.ValkeyCluster) ([]int, error) {
	list := &appsv1.StatefulSetList{}
	if err := r.List(ctx, list, client.InNamespace(cr.Namespace),
		client.MatchingLabels{"app.kubernetes.io/instance": cr.Name}); err != nil {
		return nil, err
	}
	idxs := make([]int, 0, len(list.Items))
	for i := range list.Items {
		v, ok := list.Items[i].Labels[resources.LabelShard]
		if !ok {
			continue
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			continue
		}
		idxs = append(idxs, n)
	}
	sort.Ints(idxs)
	return idxs, nil
}

// firstSurvivorPrimary returns the node ID of a primary in a surviving shard
// (index < desired) other than excludeID, to receive drained slots.
func (r *ValkeyClusterReconciler) firstSurvivorPrimary(ctx context.Context, cr *cachev1alpha1.ValkeyCluster, idx map[string]cluster.NodeInfo, excludeID string, desired int) string {
	for s := 0; s < desired; s++ {
		if id, _ := r.findShardPrimary(ctx, cr, s, idx); id != "" && id != excludeID {
			return id
		}
	}
	return ""
}

// ---- teardown ----

func (r *ValkeyClusterReconciler) deleteShard(ctx context.Context, cr *cachev1alpha1.ValkeyCluster, shard int) error {
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resources.StatefulSetName(cr, shard), Namespace: cr.Namespace}}
	if err := ignoreNotFound(r.Delete(ctx, sts)); err != nil {
		return err
	}
	return r.deleteShardPVCs(ctx, cr, shard)
}

func (r *ValkeyClusterReconciler) deleteShardPVCs(ctx context.Context, cr *cachev1alpha1.ValkeyCluster, shard int) error {
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcs, client.InNamespace(cr.Namespace),
		client.MatchingLabels{resources.LabelShard: fmt.Sprintf("%d", shard), "app.kubernetes.io/instance": cr.Name}); err != nil {
		return err
	}
	for i := range pvcs.Items {
		if err := ignoreNotFound(r.Delete(ctx, &pvcs.Items[i])); err != nil {
			return err
		}
	}
	return nil
}

func (r *ValkeyClusterReconciler) deleteOwnedPVCs(ctx context.Context, cr *cachev1alpha1.ValkeyCluster) error {
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcs, client.InNamespace(cr.Namespace),
		client.MatchingLabels{"app.kubernetes.io/instance": cr.Name}); err != nil {
		return err
	}
	for i := range pvcs.Items {
		if err := ignoreNotFound(r.Delete(ctx, &pvcs.Items[i])); err != nil {
			return err
		}
	}
	return nil
}
