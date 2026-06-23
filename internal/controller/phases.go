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
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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

	switch plan.Kind {
	case topology.ActionForm:
		l.Info("forming cluster")
		_ = r.setPhase(ctx, cr, cachev1alpha1.PhaseForming, "Forming", "bootstrapping cluster")
		if err := r.formCluster(ctx, cr); err != nil {
			return err
		}
	case topology.ActionRepair:
		l.Info("repairing open slots before proceeding")
		_ = r.setPhase(ctx, cr, cachev1alpha1.PhaseResharding, "Repairing", "fixing open slots")
		if err := r.Admin.Fix(ctx, seed); err != nil {
			return err
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

// scaleOut joins new empty primaries and rebalances slots onto them, then attaches their replicas.
func (r *ValkeyClusterReconciler) scaleOut(ctx context.Context, cr *cachev1alpha1.ValkeyCluster, oldShards int) error {
	seed := r.seed(cr)
	for i := oldShards; i < int(cr.Spec.Shards); i++ {
		for j := 0; j <= int(cr.Spec.ReplicasPerShard); j++ {
			if err := r.Admin.Meet(ctx, seed, r.endpoint(cr, i, j)); err != nil {
				return err
			}
		}
	}
	if err := r.Admin.Rebalance(ctx, seed, cluster.RebalanceOpts{UseEmptyMasters: true}); err != nil {
		return fmt.Errorf("rebalance (scale out): %w", err)
	}
	state, err := r.Admin.State(ctx, seed)
	if err != nil {
		return err
	}
	return r.attachReplicas(ctx, cr, state)
}

// scaleIn drains the departing shards, forgets their nodes, and removes their workloads + PVCs.
func (r *ValkeyClusterReconciler) scaleIn(ctx context.Context, cr *cachev1alpha1.ValkeyCluster, state cluster.ClusterState, oldShards int) error {
	seed := r.seed(cr)
	byHost := hostIndex(state)
	for i := int(cr.Spec.Shards); i < oldShards; i++ {
		primary := byHost[resources.PodFQDN(cr, i, 0)]
		if primary.ID != "" {
			if err := r.Admin.Rebalance(ctx, seed, cluster.RebalanceOpts{WeightZeroIDs: []string{primary.ID}}); err != nil {
				return fmt.Errorf("drain shard %d: %w", i, err)
			}
		}
		// forget every node belonging to this shard, then delete the workload + PVCs.
		for j := 0; j <= int(cr.Spec.ReplicasPerShard); j++ {
			if n, ok := byHost[resources.PodFQDN(cr, i, j)]; ok {
				_ = r.Admin.Forget(ctx, seed, n.ID)
			}
		}
		if err := r.deleteShard(ctx, cr, i); err != nil {
			return err
		}
	}
	return nil
}

// attachReplicas ensures each non-primary pod replicates its shard's current primary.
func (r *ValkeyClusterReconciler) attachReplicas(ctx context.Context, cr *cachev1alpha1.ValkeyCluster, state cluster.ClusterState) error {
	byHost := hostIndex(state)
	for i := 0; i < int(cr.Spec.Shards); i++ {
		primaryID := r.shardPrimaryID(cr, i, byHost)
		if primaryID == "" {
			continue
		}
		for j := 1; j <= int(cr.Spec.ReplicasPerShard); j++ {
			ep := r.endpoint(cr, i, j)
			n, ok := byHost[ep.Host]
			if ok && n.MasterID == primaryID {
				continue // already replicating the right primary
			}
			if err := r.Admin.Replicate(ctx, ep, primaryID); err != nil {
				return fmt.Errorf("replicate %s: %w", ep.Host, err)
			}
		}
	}
	return nil
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
	// meet any desired pod not yet in the view
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
	// forget nodes that are no longer part of the desired topology
	for _, n := range state.Nodes {
		if !want[n.Host] && n.Host != "" {
			_ = r.Admin.Forget(ctx, seed, n.ID)
		}
	}
	return nil
}

// shardPrimaryID returns the node ID of shard i's current primary (the shard-i
// pod that is a master), falling back to ordinal 0.
func (r *ValkeyClusterReconciler) shardPrimaryID(cr *cachev1alpha1.ValkeyCluster, shard int, byHost map[string]cluster.NodeInfo) string {
	for j := 0; j <= int(cr.Spec.ReplicasPerShard); j++ {
		if n, ok := byHost[resources.PodFQDN(cr, shard, j)]; ok && n.IsPrimary() {
			return n.ID
		}
	}
	if n, ok := byHost[resources.PodFQDN(cr, shard, 0)]; ok {
		return n.ID
	}
	return ""
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

	cr.Status.Shards = shardStatuses
	cr.Status.ReadyShards = int32(ready)
	cr.Status.ObservedGeneration = cr.Generation

	allReady := ready == int(cr.Spec.Shards) && state.SlotsCovered
	if allReady {
		cr.Status.Phase = cachev1alpha1.PhaseReady
		setCond(cr, cachev1alpha1.ConditionAvailable, metav1.ConditionTrue, "Ready", "all shards serving, full keyspace covered")
		setCond(cr, cachev1alpha1.ConditionProgressing, metav1.ConditionFalse, "Ready", "converged")
		setCond(cr, cachev1alpha1.ConditionDegraded, metav1.ConditionFalse, "Ready", "healthy")
	} else if ready == 0 {
		cr.Status.Phase = cachev1alpha1.PhaseDegraded
		setCond(cr, cachev1alpha1.ConditionAvailable, metav1.ConditionFalse, "NoPrimaries", "no shard has a reachable primary")
		setCond(cr, cachev1alpha1.ConditionDegraded, metav1.ConditionTrue, "NoPrimaries", "no shard has a reachable primary")
	} else {
		// some shards serving but not all / slots not fully covered
		if cr.Status.Phase == cachev1alpha1.PhaseReady || cr.Status.Phase == "" {
			cr.Status.Phase = cachev1alpha1.PhaseDegraded
		}
		setCond(cr, cachev1alpha1.ConditionAvailable, metav1.ConditionFalse, "PartialCoverage", "not all shards serving or keyspace not fully covered")
		setCond(cr, cachev1alpha1.ConditionDegraded, metav1.ConditionTrue, "PartialCoverage", "degraded")
	}
	return r.Status().Update(ctx, cr)
}

func (r *ValkeyClusterReconciler) setPhase(ctx context.Context, cr *cachev1alpha1.ValkeyCluster, phase cachev1alpha1.ValkeyClusterPhase, reason, msg string) error {
	cr.Status.Phase = phase
	setCond(cr, cachev1alpha1.ConditionProgressing, metav1.ConditionTrue, reason, msg)
	return r.Status().Update(ctx, cr)
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
func summarize(cr *cachev1alpha1.ValkeyCluster, state cluster.ClusterState) topology.Observed {
	primaries := 0
	replicaByPrimary := map[string]int{}
	for _, n := range state.Nodes {
		if n.IsPrimary() {
			primaries++
		} else if n.MasterID != "" {
			replicaByPrimary[n.MasterID]++
		}
	}
	var counts []int
	for _, n := range state.Nodes {
		if n.IsPrimary() {
			counts = append(counts, replicaByPrimary[n.ID])
		}
	}
	return topology.Observed{
		Formed:        state.Formed,
		SlotsCovered:  state.SlotsCovered,
		PrimaryCount:  primaries,
		ReplicaCounts: counts,
	}
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
