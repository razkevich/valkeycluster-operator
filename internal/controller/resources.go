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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1alpha1 "github.com/razkevich/valkeycluster-operator/api/v1alpha1"
	"github.com/razkevich/valkeycluster-operator/internal/cluster"
	"github.com/razkevich/valkeycluster-operator/internal/resources"
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
