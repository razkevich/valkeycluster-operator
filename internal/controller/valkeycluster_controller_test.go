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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

var _ = Describe("ValkeyCluster Controller", func() {
	const ns = "default"
	ctx := context.Background()

	newReconciler := func() *ValkeyClusterReconciler {
		return &ValkeyClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			Admin:  cluster.NewFake(),
		}
	}

	makeCR := func(name string, shards, replicas int32) *cachev1alpha1.ValkeyCluster {
		return &cachev1alpha1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: cachev1alpha1.ValkeyClusterSpec{
				Shards:           shards,
				ReplicasPerShard: replicas,
				Image:            "valkey/valkey:8",
				Storage:          cachev1alpha1.StorageSpec{Size: resource.MustParse("1Gi")},
			},
		}
	}

	markShardsReady := func(cr *cachev1alpha1.ValkeyCluster) {
		want := int32(1 + cr.Spec.ReplicasPerShard)
		for i := 0; i < int(cr.Spec.Shards); i++ {
			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resources.StatefulSetName(cr, i), Namespace: ns}, sts)).To(Succeed())
			sts.Status.Replicas = want
			sts.Status.ReadyReplicas = want
			sts.Status.CurrentReplicas = want
			Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())
		}
	}

	reconcileOnce := func(r *ValkeyClusterReconciler, key types.NamespacedName) {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
	}

	It("provisions and forms a cluster, reporting Ready (US1)", func() {
		cr := makeCR("us1", 3, 1)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		key := types.NamespacedName{Name: cr.Name, Namespace: ns}
		r := newReconciler()

		By("first reconcile creates resources and waits for pods")
		reconcileOnce(r, key)

		By("StatefulSets, headless Service, and ConfigMap exist with owner refs")
		for i := 0; i < 3; i++ {
			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resources.StatefulSetName(cr, i), Namespace: ns}, sts)).To(Succeed())
			Expect(sts.OwnerReferences).NotTo(BeEmpty())
			Expect(*sts.Spec.Replicas).To(Equal(int32(2)))
		}
		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resources.HeadlessServiceName(cr), Namespace: ns}, svc)).To(Succeed())
		Expect(svc.Spec.ClusterIP).To(Equal("None"))
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resources.ConfigMapName(cr), Namespace: ns}, cm)).To(Succeed())
		Expect(cm.Data["valkey.conf"]).To(ContainSubstring("cluster-enabled yes"))

		By("phase is Provisioning until pods are ready")
		Expect(k8sClient.Get(ctx, key, cr)).To(Succeed())
		Expect(cr.Status.Phase).To(Equal(cachev1alpha1.PhaseProvisioning))

		By("once pods are ready, the next reconcile forms the cluster")
		markShardsReady(cr)
		reconcileOnce(r, key)

		Expect(k8sClient.Get(ctx, key, cr)).To(Succeed())
		Expect(cr.Status.Phase).To(Equal(cachev1alpha1.PhaseReady))
		Expect(cr.Status.ReadyShards).To(Equal(int32(3)))
		Expect(cr.Status.Shards).To(HaveLen(3))
		for _, ss := range cr.Status.Shards {
			Expect(ss.PrimaryPod).NotTo(BeEmpty())
			Expect(ss.Slots).NotTo(BeEmpty())
		}
		Expect(meta_IsAvailable(cr)).To(BeTrue())
	})

	It("scales replicas without resharding (US3)", func() {
		cr := makeCR("rep", 3, 1)
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		key := types.NamespacedName{Name: cr.Name, Namespace: ns}
		r := newReconciler()
		reconcileOnce(r, key)
		markShardsReady(cr)
		reconcileOnce(r, key)

		By("increasing replicasPerShard to 2")
		Expect(k8sClient.Get(ctx, key, cr)).To(Succeed())
		cr.Spec.ReplicasPerShard = 2
		Expect(k8sClient.Update(ctx, cr)).To(Succeed())
		reconcileOnce(r, key) // ensures STS replicas=3, then waits for ready
		markShardsReady(cr)
		reconcileOnce(r, key)

		for i := 0; i < 3; i++ {
			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resources.StatefulSetName(cr, i), Namespace: ns}, sts)).To(Succeed())
			Expect(*sts.Spec.Replicas).To(Equal(int32(3)), fmt.Sprintf("shard %d", i))
		}
		Expect(k8sClient.Get(ctx, key, cr)).To(Succeed())
		Expect(cr.Status.Phase).To(Equal(cachev1alpha1.PhaseReady))
	})
})

func meta_IsAvailable(cr *cachev1alpha1.ValkeyCluster) bool {
	for _, c := range cr.Status.Conditions {
		if c.Type == cachev1alpha1.ConditionAvailable {
			return c.Status == metav1.ConditionTrue
		}
	}
	return false
}
