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
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	cachev1alpha1 "github.com/razkevich/valkeycluster-operator/api/v1alpha1"
	"github.com/razkevich/valkeycluster-operator/internal/cluster"
	"github.com/razkevich/valkeycluster-operator/internal/resources"
	"github.com/razkevich/valkeycluster-operator/internal/slots"
	"github.com/razkevich/valkeycluster-operator/internal/topology"
)

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
