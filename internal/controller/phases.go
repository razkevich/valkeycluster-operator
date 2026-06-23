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
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/razkevich/valkeycluster-operator/api/v1alpha1"
	"github.com/razkevich/valkeycluster-operator/internal/cluster"
	"github.com/razkevich/valkeycluster-operator/internal/resources"
	"github.com/razkevich/valkeycluster-operator/internal/slots"
	"github.com/razkevich/valkeycluster-operator/internal/topology"
)

const clientPort = 6379

// drainBatchSlots bounds how many slots a single scale-in drain moves per
// reconcile. Smaller batches commit progress incrementally and keep any single
// reshard short, so a transient MIGRATE stall costs a small retry rather than
// aborting a large drain.
const drainBatchSlots = 256

// ---- endpoints ----

func (r *ValkeyClusterReconciler) endpoint(cr *cachev1alpha1.ValkeyCluster, shard, ordinal int) cluster.Endpoint {
	return cluster.Endpoint{
		Host:      resources.PodFQDN(cr, shard, ordinal),
		Port:      clientPort,
		PodName:   resources.PodName(cr, shard, ordinal),
		Namespace: cr.Namespace,
	}
}

func (r *ValkeyClusterReconciler) seed(cr *cachev1alpha1.ValkeyCluster) cluster.Endpoint {
	return r.endpoint(cr, 0, 0)
}

// allEndpoints returns endpoints for every pod across all desired shards.
func (r *ValkeyClusterReconciler) allEndpoints(cr *cachev1alpha1.ValkeyCluster) []cluster.Endpoint {
	var eps []cluster.Endpoint
	for i := 0; i < int(cr.Spec.Shards); i++ {
		for j := 0; j <= int(cr.Spec.ReplicasPerShard); j++ {
			eps = append(eps, r.endpoint(cr, i, j))
		}
	}
	return eps
}

// ---- ensure resources ----

func (r *ValkeyClusterReconciler) ensureConfigMap(ctx context.Context, cr *cachev1alpha1.ValkeyCluster) error {
	desired := resources.ConfigMap(cr)
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = desired.Labels
		cm.Data = desired.Data
		return controllerutil.SetControllerReference(cr, cm, r.Scheme)
	})
	return err
}

func (r *ValkeyClusterReconciler) ensureHeadlessService(ctx context.Context, cr *cachev1alpha1.ValkeyCluster) error {
	desired := resources.HeadlessService(cr)
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = desired.Labels
		svc.Spec.ClusterIP = "None"
		svc.Spec.PublishNotReadyAddresses = true
		svc.Spec.Selector = desired.Spec.Selector
		svc.Spec.Ports = desired.Spec.Ports
		return controllerutil.SetControllerReference(cr, svc, r.Scheme)
	})
	return err
}

func (r *ValkeyClusterReconciler) ensureStatefulSets(ctx context.Context, cr *cachev1alpha1.ValkeyCluster) error {
	for i := 0; i < int(cr.Spec.Shards); i++ {
		desired := resources.StatefulSet(cr, i)
		sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
			// Mutable fields only (selector/volumeClaimTemplates are immutable on update).
			if sts.CreationTimestamp.IsZero() {
				sts.Spec.Selector = desired.Spec.Selector
				sts.Spec.VolumeClaimTemplates = desired.Spec.VolumeClaimTemplates
				sts.Spec.ServiceName = desired.Spec.ServiceName
				sts.Spec.PodManagementPolicy = desired.Spec.PodManagementPolicy
			}
			sts.Labels = desired.Labels
			sts.Spec.Replicas = desired.Spec.Replicas
			sts.Spec.Template = desired.Spec.Template
			return controllerutil.SetControllerReference(cr, sts, r.Scheme)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// shardsReady reports whether every desired shard's StatefulSet has all pods ready.
func (r *ValkeyClusterReconciler) shardsReady(ctx context.Context, cr *cachev1alpha1.ValkeyCluster) (bool, error) {
	want := int32(1 + cr.Spec.ReplicasPerShard)
	for i := 0; i < int(cr.Spec.Shards); i++ {
		sts := &appsv1.StatefulSet{}
		err := r.Get(ctx, types.NamespacedName{Name: resources.StatefulSetName(cr, i), Namespace: cr.Namespace}, sts)
		if err != nil {
			return false, client.IgnoreNotFound(err)
		}
		if sts.Status.ReadyReplicas != want {
			return false, nil
		}
	}
	return true, nil
}

// ---- observe / decide / act ----

func (r *ValkeyClusterReconciler) reconcileCluster(ctx context.Context, cr *cachev1alpha1.ValkeyCluster) error {
	l := log.FromContext(ctx)
	seed := r.seed(cr)

	state, err := r.Admin.State(ctx, seed)
	if err != nil {
		return fmt.Errorf("observe cluster: %w", err)
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
			return err
		}
		if excess {
			plan = topology.Plan{Kind: topology.ActionScaleInShards}
		}
	}

	switch plan.Kind {
	case topology.ActionForm:
		l.Info("forming cluster")
		_ = r.setPhase(ctx, cr, cachev1alpha1.PhaseForming, "Forming", "bootstrapping cluster")
		if err := r.formCluster(ctx, cr); err != nil {
			return err
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
			return err
		}
	case topology.ActionScaleInShards:
		l.Info("scaling in shards", "remove", plan.RemoveShards)
		_ = r.setPhase(ctx, cr, cachev1alpha1.PhaseResharding, "Resharding", "removing shards")
		if err := r.scaleIn(ctx, cr, state, observed.PrimaryCount); err != nil {
			return err
		}
	case topology.ActionScaleReplicas:
		l.Info("reconciling replicas")
		_ = r.setPhase(ctx, cr, cachev1alpha1.PhaseScalingReplicas, "ScalingReplicas", "adjusting replicas")
		if err := r.reconcileMembership(ctx, cr, state); err != nil {
			return err
		}
	case topology.ActionNone:
		// keep replication wiring healthy (rejoin after failover, forget stale)
		if err := r.reconcileMembership(ctx, cr, state); err != nil {
			return err
		}
	}

	// Refresh and publish status.
	state, err = r.Admin.State(ctx, seed)
	if err != nil {
		return fmt.Errorf("observe cluster (post-action): %w", err)
	}
	return r.updateStatus(ctx, cr, state)
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

// scaleOut joins each new shard's primary (ordinal 0), moves that primary its
// fair share of slots with a *targeted* reshard (never --use-empty-masters, which
// would also hand slots to replica pods that are momentarily empty masters), then
// joins and attaches the new shards' replicas. Idempotent: a primary that already
// holds its share is skipped, so a retry never over-assigns.
func (r *ValkeyClusterReconciler) scaleOut(ctx context.Context, cr *cachev1alpha1.ValkeyCluster, oldShards int) error {
	seed := r.seed(cr)
	desired := int(cr.Spec.Shards)
	perShard := cluster.TotalSlots / desired

	// 1. join only the new primaries
	for i := oldShards; i < desired; i++ {
		if err := r.Admin.Meet(ctx, seed, r.endpoint(cr, i, 0)); err != nil {
			return err
		}
	}
	// 2. move each new primary its fair share, targeted by node ID
	state, err := r.Admin.State(ctx, seed)
	if err != nil {
		return err
	}
	idx := byID(state)
	for i := oldShards; i < desired; i++ {
		id, err := r.Admin.MyID(ctx, r.endpoint(cr, i, 0))
		if err != nil || id == "" {
			return fmt.Errorf("resolve new primary %d id: %w", i, err)
		}
		have := 0
		if n, ok := idx[id]; ok {
			have = n.SlotCount()
		}
		if need := perShard - have; need > 0 {
			if err := r.Admin.Reshard(ctx, seed, "", id, need); err != nil {
				return fmt.Errorf("reshard into shard %d: %w", i, err)
			}
		}
	}
	// 3. join and attach the new shards' replicas (now that slots are placed)
	for i := oldShards; i < desired; i++ {
		for j := 1; j <= int(cr.Spec.ReplicasPerShard); j++ {
			if err := r.Admin.Meet(ctx, seed, r.endpoint(cr, i, j)); err != nil {
				return err
			}
		}
	}
	state, err = r.Admin.State(ctx, seed)
	if err != nil {
		return err
	}
	return r.attachReplicas(ctx, cr, state)
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

// ---- status ----

func (r *ValkeyClusterReconciler) updateStatus(ctx context.Context, cr *cachev1alpha1.ValkeyCluster, state cluster.ClusterState) error {
	byHost := hostIndex(state)
	var shardStatuses []cachev1alpha1.ShardStatus
	ready := 0
	for i := 0; i < int(cr.Spec.Shards); i++ {
		ss := cachev1alpha1.ShardStatus{Index: int32(i)}
		var primary *cluster.NodeInfo
		for j := 0; j <= int(cr.Spec.ReplicasPerShard); j++ {
			n, ok := byHost[resources.PodFQDN(cr, i, j)]
			if !ok {
				continue
			}
			ss.NodeIDs = append(ss.NodeIDs, n.ID)
			if n.IsPrimary() && n.SlotCount() > 0 {
				nc := n
				primary = &nc
			}
		}
		if primary != nil {
			ss.PrimaryPod = podNameFromHost(primary.Host)
			ss.PrimaryNodeID = primary.ID
			ss.Slots = slots.FormatRanges(primary.Slots)
			for _, n := range state.Nodes {
				if n.MasterID == primary.ID && n.Connected {
					ss.ReadyReplicas++
				}
			}
			ready++
		}
		shardStatuses = append(shardStatuses, ss)
	}

	// actualShards = slot-owning masters across the WHOLE cluster. During a
	// scale-in, extra shards still exist (and own slots) until they're drained
	// and removed; if we only looked at shards [0,desired) we'd falsely report
	// Ready while those linger. Truthful status requires actualShards == desired.
	actualShards := 0
	for _, n := range state.Nodes {
		if n.IsPrimary() && n.SlotCount() > 0 {
			actualShards++
		}
	}
	desired := int(cr.Spec.Shards)
	allReady := ready == desired && actualShards == desired && state.SlotsCovered && !state.OpenSlots
	key := types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &cachev1alpha1.ValkeyCluster{}
		if err := r.Get(ctx, key, latest); err != nil {
			return err
		}
		latest.Status.Shards = shardStatuses
		latest.Status.ReadyShards = int32(ready)
		latest.Status.ObservedGeneration = latest.Generation
		switch {
		case allReady:
			latest.Status.Phase = cachev1alpha1.PhaseReady
			setCond(latest, cachev1alpha1.ConditionAvailable, metav1.ConditionTrue, "Ready", "all shards serving, full keyspace covered")
			setCond(latest, cachev1alpha1.ConditionProgressing, metav1.ConditionFalse, "Ready", "converged")
			setCond(latest, cachev1alpha1.ConditionDegraded, metav1.ConditionFalse, "Ready", "healthy")
		case actualShards > desired && ready > 0:
			// scale-in in progress: extra shards not yet drained/removed.
			latest.Status.Phase = cachev1alpha1.PhaseResharding
			setCond(latest, cachev1alpha1.ConditionProgressing, metav1.ConditionTrue, "Resharding", "removing shards")
			setCond(latest, cachev1alpha1.ConditionAvailable, metav1.ConditionTrue, "Serving", "primaries serving during scale-in")
		case state.OpenSlots && ready > 0:
			// mid-migration: slots are briefly open but primaries are serving —
			// this is progress, not degradation.
			latest.Status.Phase = cachev1alpha1.PhaseResharding
			setCond(latest, cachev1alpha1.ConditionProgressing, metav1.ConditionTrue, "Resharding", "slot migration in progress")
			setCond(latest, cachev1alpha1.ConditionAvailable, metav1.ConditionTrue, "Serving", "primaries serving during migration")
		case ready == 0:
			latest.Status.Phase = cachev1alpha1.PhaseDegraded
			setCond(latest, cachev1alpha1.ConditionAvailable, metav1.ConditionFalse, "NoPrimaries", "no shard has a reachable primary")
			setCond(latest, cachev1alpha1.ConditionDegraded, metav1.ConditionTrue, "NoPrimaries", "no shard has a reachable primary")
		default:
			latest.Status.Phase = cachev1alpha1.PhaseDegraded
			setCond(latest, cachev1alpha1.ConditionAvailable, metav1.ConditionFalse, "PartialCoverage", "not all shards serving or keyspace not fully covered")
			setCond(latest, cachev1alpha1.ConditionDegraded, metav1.ConditionTrue, "PartialCoverage", "degraded")
		}
		return r.Status().Update(ctx, latest)
	})
}

func (r *ValkeyClusterReconciler) setPhase(ctx context.Context, cr *cachev1alpha1.ValkeyCluster, phase cachev1alpha1.ValkeyClusterPhase, reason, msg string) error {
	key := types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &cachev1alpha1.ValkeyCluster{}
		if err := r.Get(ctx, key, latest); err != nil {
			return err
		}
		latest.Status.Phase = phase
		setCond(latest, cachev1alpha1.ConditionProgressing, metav1.ConditionTrue, reason, msg)
		return r.Status().Update(ctx, latest)
	})
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

// ---- helpers ----

// summarize reduces a ClusterState to the topology.Observed decision inputs.
// A "shard" is a master that owns slots; empty masters are pending replicas and
// are not counted as shards (so a half-attached cluster is not mistaken for one
// with extra shards).
func summarize(cr *cachev1alpha1.ValkeyCluster, state cluster.ClusterState) topology.Observed {
	replicaByPrimary := map[string]int{}
	for _, n := range state.Nodes {
		if !n.IsPrimary() && n.MasterID != "" {
			replicaByPrimary[n.MasterID]++
		}
	}
	primaries := 0
	var counts []int
	for _, n := range state.Nodes {
		if n.IsPrimary() && n.SlotCount() > 0 {
			primaries++
			counts = append(counts, replicaByPrimary[n.ID])
		}
	}
	return topology.Observed{
		Formed: state.Formed,
		// An open slot keeps coverage looking complete but leaves the cluster
		// unstable; treat it as "not covered" so the repair gate fires before any
		// topology change (reshard/rebalance refuse on an open-slot cluster).
		SlotsCovered:  state.SlotsCovered && !state.OpenSlots,
		PrimaryCount:  primaries,
		ReplicaCounts: counts,
	}
}

func byID(state cluster.ClusterState) map[string]cluster.NodeInfo {
	m := map[string]cluster.NodeInfo{}
	for _, n := range state.Nodes {
		m[n.ID] = n
	}
	return m
}

func hostIndex(state cluster.ClusterState) map[string]cluster.NodeInfo {
	m := map[string]cluster.NodeInfo{}
	for _, n := range state.Nodes {
		m[n.Host] = n
	}
	return m
}

func podNameFromHost(host string) string {
	if i := strings.Index(host, "."); i >= 0 {
		return host[:i]
	}
	return host
}

func setCond(cr *cachev1alpha1.ValkeyCluster, condType string, status metav1.ConditionStatus, reason, msg string) {
	meta := metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: cr.Generation,
		LastTransitionTime: metav1.Now(),
	}
	for i := range cr.Status.Conditions {
		if cr.Status.Conditions[i].Type == condType {
			if cr.Status.Conditions[i].Status == status {
				meta.LastTransitionTime = cr.Status.Conditions[i].LastTransitionTime
			}
			cr.Status.Conditions[i] = meta
			return
		}
	}
	cr.Status.Conditions = append(cr.Status.Conditions, meta)
}
