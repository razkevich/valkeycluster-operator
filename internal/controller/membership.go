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

	cachev1alpha1 "github.com/razkevich/valkeycluster-operator/api/v1alpha1"
	"github.com/razkevich/valkeycluster-operator/internal/cluster"
	"github.com/razkevich/valkeycluster-operator/internal/resources"
)

// attachReplicas ensures each non-primary pod replicates its shard's current
// primary. The primary is found by dialing the shard's pods directly (CLUSTER
// MYID) and matching against the gossiped slot ownership — robust to the
// announce-hostname gossip lag and to failover (the primary may not be ordinal 0).
func (r *ValkeyClusterReconciler) attachReplicas(ctx context.Context, cr *cachev1alpha1.ValkeyCluster, state cluster.ClusterState) error {
	idx := byID(state)
	for i := 0; i < int(cr.Spec.Shards); i++ {
		primaryID, primaryOrd := r.findShardPrimary(ctx, cr, i, idx)
		if primaryID == "" {
			continue // not observable yet; retry on next reconcile
		}
		for j := 0; j <= int(cr.Spec.ReplicasPerShard); j++ {
			if j == primaryOrd {
				continue
			}
			ep := r.endpoint(cr, i, j)
			id, err := r.Admin.MyID(ctx, ep)
			if err != nil {
				continue
			}
			if n, ok := idx[id]; ok && n.MasterID == primaryID {
				continue // already replicating the right primary
			}
			if err := r.Admin.Replicate(ctx, ep, primaryID); err != nil {
				return fmt.Errorf("replicate %s: %w", ep.Host, err)
			}
		}
	}
	return nil
}

// findShardPrimary returns the node ID and ordinal of shard i's primary: the
// shard-i pod that is a master owning slots. Falls back to ordinal 0 (the
// initial primary during formation, before slot gossip settles).
func (r *ValkeyClusterReconciler) findShardPrimary(ctx context.Context, cr *cachev1alpha1.ValkeyCluster, shard int, idx map[string]cluster.NodeInfo) (string, int) {
	for j := 0; j <= int(cr.Spec.ReplicasPerShard); j++ {
		id, err := r.Admin.MyID(ctx, r.endpoint(cr, shard, j))
		if err != nil {
			continue
		}
		if n, ok := idx[id]; ok && n.IsPrimary() && n.SlotCount() > 0 {
			return id, j
		}
	}
	if id, err := r.Admin.MyID(ctx, r.endpoint(cr, shard, 0)); err == nil {
		return id, 0
	}
	return "", -1
}

// reconcileMembership keeps the live membership aligned with desired pods:
// meet missing pods, attach replicas, and forget nodes that no longer map to a pod.
func (r *ValkeyClusterReconciler) reconcileMembership(ctx context.Context, cr *cachev1alpha1.ValkeyCluster, state cluster.ClusterState) error {
	seed := r.seed(cr)
	want := map[string]bool{}
	for _, ep := range r.allEndpoints(cr) {
		want[ep.Host] = true
	}
	byHost := hostIndex(state)
	for _, ep := range r.allEndpoints(cr) {
		if _, ok := byHost[ep.Host]; !ok {
			if err := r.Admin.Meet(ctx, seed, ep); err != nil {
				return err
			}
		}
	}
	if err := r.attachReplicas(ctx, cr, state); err != nil {
		return err
	}
	// forget nodes that are no longer part of the desired topology — from every
	// survivor, so gossip cannot re-introduce them (CLUSTER FORGET is per-node).
	r.forgetStaleNodes(ctx, cr, state)
	return nil
}

// forgetEverywhere removes nodeID from every surviving pod's view. CLUSTER
// FORGET is per-node with a 60s ban window; issued to only one node, gossip
// re-introduces the entry from the others. Issuing it from every desired
// (surviving) pod keeps the node forgotten cluster-wide. Errors are ignored:
// forgetting an unknown node, or a node forgetting itself, is a harmless no-op.
func (r *ValkeyClusterReconciler) forgetEverywhere(ctx context.Context, cr *cachev1alpha1.ValkeyCluster, nodeID string) {
	for _, ep := range r.allEndpoints(cr) {
		_ = r.Admin.Forget(ctx, ep, nodeID)
	}
}

// forgetStaleNodes forgets, from every surviving pod, any cluster node whose pod
// no longer exists — i.e. the nodes of an already-deleted shard's StatefulSet.
//
// "Stale" is keyed off the StatefulSets that actually exist, NOT off the desired
// topology. During scale-in a departing shard (index >= desired) still has its
// StatefulSet and pods while its slots drain; those nodes must NOT be forgotten
// mid-drain — doing so drops the departing primary from the seed's view, the
// drain loop then sees it owning zero slots and stalls, and gossip re-introduces
// it moments later. Only once a shard's StatefulSet is deleted are its nodes
// genuinely stale.
func (r *ValkeyClusterReconciler) forgetStaleNodes(ctx context.Context, cr *cachev1alpha1.ValkeyCluster, state cluster.ClusterState) {
	want, err := r.existingPodHosts(ctx, cr)
	if err != nil {
		return // best-effort; a transient list error just defers the sweep
	}
	for _, n := range state.Nodes {
		if n.Host != "" && !want[n.Host] {
			r.forgetEverywhere(ctx, cr, n.ID)
		}
	}
}

// existingPodHosts returns the FQDN set of every pod across all StatefulSets that
// currently exist (any shard index, surviving or pending drain).
func (r *ValkeyClusterReconciler) existingPodHosts(ctx context.Context, cr *cachev1alpha1.ValkeyCluster) (map[string]bool, error) {
	idxs, err := r.existingShardIndexes(ctx, cr)
	if err != nil {
		return nil, err
	}
	want := map[string]bool{}
	for _, i := range idxs {
		for j := 0; j <= int(cr.Spec.ReplicasPerShard); j++ {
			want[resources.PodFQDN(cr, i, j)] = true
		}
	}
	return want, nil
}
