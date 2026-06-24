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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cachev1alpha1 "github.com/razkevich/valkeycluster-operator/api/v1alpha1"
	"github.com/razkevich/valkeycluster-operator/internal/cluster"
	"github.com/razkevich/valkeycluster-operator/internal/resources"
)

const testNS = "default"

func newReconciler() *ValkeyClusterReconciler {
	return &ValkeyClusterReconciler{
		Client: k8sClient,
		Scheme: k8sClient.Scheme(),
		Admin:  cluster.NewFake(),
	}
}

func makeCR(name string, shards, replicas int32) *cachev1alpha1.ValkeyCluster {
	return &cachev1alpha1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: cachev1alpha1.ValkeyClusterSpec{
			Shards:           shards,
			ReplicasPerShard: replicas,
			Image:            "valkey/valkey:8",
			Storage:          cachev1alpha1.StorageSpec{Size: resource.MustParse("1Gi")},
		},
	}
}

// markShardsReady flips every shard's StatefulSet status to fully ready, simulating
// the pods coming up so the next reconcile proceeds past the readiness gate.
func markShardsReady(t *testing.T, ctx context.Context, cr *cachev1alpha1.ValkeyCluster) {
	t.Helper()
	want := int32(1 + cr.Spec.ReplicasPerShard)
	for i := 0; i < int(cr.Spec.Shards); i++ {
		sts := &appsv1.StatefulSet{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: resources.StatefulSetName(cr, i), Namespace: testNS}, sts); err != nil {
			t.Fatalf("get statefulset %d: %v", i, err)
		}
		sts.Status.Replicas = want
		sts.Status.ReadyReplicas = want
		sts.Status.CurrentReplicas = want
		if err := k8sClient.Status().Update(ctx, sts); err != nil {
			t.Fatalf("update statefulset %d status: %v", i, err)
		}
	}
}

func reconcileOnce(t *testing.T, ctx context.Context, r *ValkeyClusterReconciler, key types.NamespacedName) {
	t.Helper()
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func TestReconcile_ProvisionAndForm(t *testing.T) {
	ctx := testCtx(t)
	cr := makeCR("us1", 3, 1)
	if err := k8sClient.Create(ctx, cr); err != nil {
		t.Fatalf("create CR: %v", err)
	}
	key := types.NamespacedName{Name: cr.Name, Namespace: testNS}
	r := newReconciler()

	t.Run("first reconcile creates owned resources and waits for pods", func(t *testing.T) {
		reconcileOnce(t, ctx, r, key)

		for i := 0; i < 3; i++ {
			sts := &appsv1.StatefulSet{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: resources.StatefulSetName(cr, i), Namespace: testNS}, sts); err != nil {
				t.Fatalf("get statefulset %d: %v", i, err)
			}
			if len(sts.OwnerReferences) == 0 {
				t.Errorf("statefulset %d has no owner reference", i)
			}
			if got := *sts.Spec.Replicas; got != 2 {
				t.Errorf("statefulset %d replicas = %d, want 2", i, got)
			}
		}

		svc := &corev1.Service{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: resources.HeadlessServiceName(cr), Namespace: testNS}, svc); err != nil {
			t.Fatalf("get service: %v", err)
		}
		if svc.Spec.ClusterIP != "None" {
			t.Errorf("service ClusterIP = %q, want None (headless)", svc.Spec.ClusterIP)
		}

		cm := &corev1.ConfigMap{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: resources.ConfigMapName(cr), Namespace: testNS}, cm); err != nil {
			t.Fatalf("get configmap: %v", err)
		}
		if conf := cm.Data["valkey.conf"]; !strings.Contains(conf, "cluster-enabled yes") {
			t.Errorf("valkey.conf missing 'cluster-enabled yes':\n%s", conf)
		}
	})

	t.Run("phase is Provisioning until pods are ready", func(t *testing.T) {
		if err := k8sClient.Get(ctx, key, cr); err != nil {
			t.Fatalf("get CR: %v", err)
		}
		if cr.Status.Phase != cachev1alpha1.PhaseProvisioning {
			t.Errorf("phase = %q, want %q", cr.Status.Phase, cachev1alpha1.PhaseProvisioning)
		}
	})

	t.Run("once pods are ready the cluster forms and reports Ready", func(t *testing.T) {
		markShardsReady(t, ctx, cr)
		reconcileOnce(t, ctx, r, key)

		if err := k8sClient.Get(ctx, key, cr); err != nil {
			t.Fatalf("get CR: %v", err)
		}
		if cr.Status.Phase != cachev1alpha1.PhaseReady {
			t.Errorf("phase = %q, want %q", cr.Status.Phase, cachev1alpha1.PhaseReady)
		}
		if cr.Status.ReadyShards != 3 {
			t.Errorf("readyShards = %d, want 3", cr.Status.ReadyShards)
		}
		if len(cr.Status.Shards) != 3 {
			t.Fatalf("status shards = %d, want 3", len(cr.Status.Shards))
		}
		for i, ss := range cr.Status.Shards {
			if ss.PrimaryPod == "" {
				t.Errorf("shard %d has empty PrimaryPod", i)
			}
			if ss.Slots == "" {
				t.Errorf("shard %d has empty Slots", i)
			}
		}
		if !isAvailable(cr) {
			t.Error("Available condition is not True")
		}
	})
}

func TestReconcile_ScaleReplicas(t *testing.T) {
	ctx := testCtx(t)
	cr := makeCR("rep", 3, 1)
	if err := k8sClient.Create(ctx, cr); err != nil {
		t.Fatalf("create CR: %v", err)
	}
	key := types.NamespacedName{Name: cr.Name, Namespace: testNS}
	r := newReconciler()
	reconcileOnce(t, ctx, r, key)
	markShardsReady(t, ctx, cr)
	reconcileOnce(t, ctx, r, key)

	t.Run("increasing replicasPerShard to 2 scales each StatefulSet to 3 without resharding", func(t *testing.T) {
		if err := k8sClient.Get(ctx, key, cr); err != nil {
			t.Fatalf("get CR: %v", err)
		}
		cr.Spec.ReplicasPerShard = 2
		if err := k8sClient.Update(ctx, cr); err != nil {
			t.Fatalf("update CR: %v", err)
		}
		reconcileOnce(t, ctx, r, key) // sets STS replicas=3, then waits for ready
		markShardsReady(t, ctx, cr)
		reconcileOnce(t, ctx, r, key)

		for i := 0; i < 3; i++ {
			sts := &appsv1.StatefulSet{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: resources.StatefulSetName(cr, i), Namespace: testNS}, sts); err != nil {
				t.Fatalf("get statefulset %d: %v", i, err)
			}
			if got := *sts.Spec.Replicas; got != 3 {
				t.Errorf("statefulset %d replicas = %d, want 3", i, got)
			}
		}
		if err := k8sClient.Get(ctx, key, cr); err != nil {
			t.Fatalf("get CR: %v", err)
		}
		if cr.Status.Phase != cachev1alpha1.PhaseReady {
			t.Errorf("phase = %q, want %q", cr.Status.Phase, cachev1alpha1.PhaseReady)
		}
	})
}

func isAvailable(cr *cachev1alpha1.ValkeyCluster) bool {
	for _, c := range cr.Status.Conditions {
		if c.Type == cachev1alpha1.ConditionAvailable {
			return c.Status == metav1.ConditionTrue
		}
	}
	return false
}
