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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/razkevich/valkeycluster-operator/api/v1alpha1"
	"github.com/razkevich/valkeycluster-operator/internal/cluster"
)

const finalizer = "cache.razkevich.dev/finalizer"

// requeueAfter paces re-reconciliation while the cluster converges.
const requeueAfter = 15 * time.Second

// reshardRequeue is the (short) requeue while a form/repair/reshard is still in
// flight, so multi-batch operations run as a tight loop rather than one batch
// every requeueAfter.
const reshardRequeue = 1 * time.Second

// ValkeyClusterReconciler reconciles a ValkeyCluster object.
type ValkeyClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Admin orchestrates the live Valkey cluster (go-redis + pod-exec in prod,
	// a fake in tests).
	Admin cluster.ClusterAdmin
}

// +kubebuilder:rbac:groups=cache.razkevich.dev,resources=valkeyclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cache.razkevich.dev,resources=valkeyclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cache.razkevich.dev,resources=valkeyclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives the live cluster toward the declared topology.
func (r *ValkeyClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	cr := &cachev1alpha1.ValkeyCluster{}
	if err := r.Get(ctx, req.NamespacedName, cr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Finalizer-driven teardown (owner refs GC most resources; the finalizer
	// lets us reclaim PVCs, which the StatefulSet does not own).
	if !cr.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, cr)
	}
	if !controllerutil.ContainsFinalizer(cr, finalizer) {
		controllerutil.AddFinalizer(cr, finalizer)
		if err := r.Update(ctx, cr); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 1. Ensure shared infra.
	if err := r.ensureConfigMap(ctx, cr); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureHeadlessService(ctx, cr); err != nil {
		return ctrl.Result{}, err
	}

	// 2. Ensure one StatefulSet per desired shard.
	if err := r.ensureStatefulSets(ctx, cr); err != nil {
		return ctrl.Result{}, err
	}

	// 3. Wait until all desired shards' pods are Ready before touching the cluster.
	ready, err := r.shardsReady(ctx, cr)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !ready {
		l.Info("waiting for shard pods to become ready")
		_ = r.setPhase(ctx, cr, cachev1alpha1.PhaseProvisioning, "Provisioning", "waiting for shard pods")
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	// 4. Observe → decide → act → status.
	progressing, err := r.reconcileCluster(ctx, cr)
	if err != nil {
		l.Error(err, "cluster reconcile step failed; will retry")
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	// While a topology change is in flight, requeue fast so a reshard runs as a
	// tight loop instead of one batch per steady interval. Otherwise idle-poll.
	if progressing {
		return ctrl.Result{RequeueAfter: reshardRequeue}, nil
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

func (r *ValkeyClusterReconciler) reconcileDelete(ctx context.Context, cr *cachev1alpha1.ValkeyCluster) (ctrl.Result, error) {
	if controllerutil.ContainsFinalizer(cr, finalizer) {
		if err := r.deleteOwnedPVCs(ctx, cr); err != nil {
			return ctrl.Result{}, err
		}
		controllerutil.RemoveFinalizer(cr, finalizer)
		if err := r.Update(ctx, cr); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// SetupWithManager wires the controller and its owned resources.
func (r *ValkeyClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.ValkeyCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Named("valkeycluster").
		Complete(r)
}

func ignoreNotFound(err error) error {
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
