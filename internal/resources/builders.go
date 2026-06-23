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

// Package resources builds the Kubernetes objects that back a ValkeyCluster:
// one headless Service, one ConfigMap, and one StatefulSet per shard.
package resources

import (
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	cachev1alpha1 "github.com/razkevich/valkeycluster-operator/api/v1alpha1"
)

const (
	clientPort = 6379
	busPort    = 16379
	dataVolume = "data"
	confVolume = "conf"
	confMount  = "/conf"
	dataMount  = "/data"

	labelName      = "app.kubernetes.io/name"
	labelInstance  = "app.kubernetes.io/instance"
	labelManagedBy = "app.kubernetes.io/managed-by"
	// LabelShard identifies the shard a pod belongs to.
	LabelShard = "valkey.razkevich.dev/shard"
)

// HeadlessServiceName returns the name of the per-cluster headless Service.
func HeadlessServiceName(cr *cachev1alpha1.ValkeyCluster) string { return cr.Name + "-nodes" }

// ConfigMapName returns the name of the rendered-config ConfigMap.
func ConfigMapName(cr *cachev1alpha1.ValkeyCluster) string { return cr.Name + "-config" }

// StatefulSetName returns the StatefulSet name for shard i.
func StatefulSetName(cr *cachev1alpha1.ValkeyCluster, shard int) string {
	return fmt.Sprintf("%s-shard-%d", cr.Name, shard)
}

// PodName returns the pod name for shard i, replica ordinal j.
func PodName(cr *cachev1alpha1.ValkeyCluster, shard, ordinal int) string {
	return fmt.Sprintf("%s-%d", StatefulSetName(cr, shard), ordinal)
}

// PodFQDN returns the stable in-cluster DNS name a pod advertises.
func PodFQDN(cr *cachev1alpha1.ValkeyCluster, shard, ordinal int) string {
	return fmt.Sprintf("%s.%s.%s.svc", PodName(cr, shard, ordinal), HeadlessServiceName(cr), cr.Namespace)
}

func commonLabels(cr *cachev1alpha1.ValkeyCluster) map[string]string {
	return map[string]string{
		labelName:      "valkeycluster",
		labelInstance:  cr.Name,
		labelManagedBy: "valkeycluster-operator",
	}
}

func shardLabels(cr *cachev1alpha1.ValkeyCluster, shard int) map[string]string {
	l := commonLabels(cr)
	l[LabelShard] = fmt.Sprintf("%d", shard)
	return l
}

// RenderValkeyConf renders the static (shared) portion of valkey.conf from the
// spec's HA policy. Per-pod announce settings are appended at container start.
func RenderValkeyConf(cr *cachev1alpha1.ValkeyCluster) string {
	hp := cr.Spec.HAPolicy
	fullCoverage := "yes"
	if hp.RequireFullCoverage != nil && !*hp.RequireFullCoverage {
		fullCoverage = "no"
	}
	fsync := string(hp.AppendFsync)
	if fsync == "" {
		fsync = "everysec"
	}
	timeout := hp.ClusterNodeTimeoutMillis
	if timeout == 0 {
		timeout = 5000
	}
	var b strings.Builder
	fmt.Fprintf(&b, "cluster-enabled yes\n")
	fmt.Fprintf(&b, "cluster-config-file %s/nodes.conf\n", dataMount)
	fmt.Fprintf(&b, "cluster-node-timeout %d\n", timeout)
	fmt.Fprintf(&b, "cluster-require-full-coverage %s\n", fullCoverage)
	fmt.Fprintf(&b, "cluster-preferred-endpoint-type hostname\n")
	fmt.Fprintf(&b, "cluster-port %d\n", busPort)
	fmt.Fprintf(&b, "port %d\n", clientPort)
	fmt.Fprintf(&b, "appendonly yes\n")
	fmt.Fprintf(&b, "appendfsync %s\n", fsync)
	fmt.Fprintf(&b, "min-replicas-to-write %d\n", hp.MinReplicasToWrite)
	fmt.Fprintf(&b, "dir %s\n", dataMount)
	fmt.Fprintf(&b, "save \"\"\n")
	// Keep maxmemory below the container memory limit so RDB/AOF copy-on-write
	// forks and client/replication buffers don't trigger an OOMKill. We reserve
	// ~30% headroom. Datastore-safe policy (reject writes rather than silently
	// evict) when at the cap.
	if lim, ok := cr.Spec.Resources.Limits[corev1.ResourceMemory]; ok && !lim.IsZero() {
		maxBytes := lim.Value() * 70 / 100
		fmt.Fprintf(&b, "maxmemory %d\n", maxBytes)
		fmt.Fprintf(&b, "maxmemory-policy noeviction\n")
	}
	return b.String()
}

// HeadlessService builds the per-cluster headless Service that gives every pod a
// stable DNS name and serves as the client discovery/seed endpoint.
func HeadlessService(cr *cachev1alpha1.ValkeyCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      HeadlessServiceName(cr),
			Namespace: cr.Namespace,
			Labels:    commonLabels(cr),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                "None",
			PublishNotReadyAddresses: true,
			Selector:                 map[string]string{labelInstance: cr.Name},
			Ports: []corev1.ServicePort{
				{Name: "client", Port: clientPort, TargetPort: intstr.FromInt(clientPort)},
				{Name: "cluster-bus", Port: busPort, TargetPort: intstr.FromInt(busPort)},
			},
		},
	}
}

// ConfigMap builds the ConfigMap holding the rendered valkey.conf.
func ConfigMap(cr *cachev1alpha1.ValkeyCluster) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ConfigMapName(cr),
			Namespace: cr.Namespace,
			Labels:    commonLabels(cr),
		},
		Data: map[string]string{"valkey.conf": RenderValkeyConf(cr)},
	}
}

// startupScript builds the final per-pod config (shared base + per-pod announce)
// and execs valkey-server. Per-pod announce settings can't live in the shared
// ConfigMap, so they're appended here from downward-API env vars.
func startupScript() string {
	return fmt.Sprintf(`set -eu
FQDN="${POD_NAME}.${SVC_NAME}.${POD_NAMESPACE}.svc"
cp %[1]s/valkey.conf %[2]s/valkey.conf
{
  echo "cluster-announce-hostname ${FQDN}"
  echo "cluster-announce-ip ${POD_IP}"
  echo "cluster-announce-port %[3]d"
  echo "cluster-announce-bus-port %[4]d"
} >> %[2]s/valkey.conf
exec valkey-server %[2]s/valkey.conf`, confMount, dataMount, clientPort, busPort)
}

// StatefulSet builds the StatefulSet for shard i (1 primary + replicasPerShard replicas).
func StatefulSet(cr *cachev1alpha1.ValkeyCluster, shard int) *appsv1.StatefulSet {
	replicas := int32(1 + cr.Spec.ReplicasPerShard)
	labels := shardLabels(cr, shard)

	vct := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: dataVolume},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: cr.Spec.Storage.Size},
			},
		},
	}
	if cr.Spec.Storage.StorageClassName != "" {
		vct.Spec.StorageClassName = ptr.To(cr.Spec.Storage.StorageClassName)
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      StatefulSetName(cr, shard),
			Namespace: cr.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         HeadlessServiceName(cr),
			Replicas:            ptr.To(replicas),
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector:            &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Affinity: &corev1.Affinity{
						PodAntiAffinity: &corev1.PodAntiAffinity{
							PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
								Weight: 100,
								PodAffinityTerm: corev1.PodAffinityTerm{
									TopologyKey:   "kubernetes.io/hostname",
									LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{LabelShard: fmt.Sprintf("%d", shard), labelInstance: cr.Name}},
								},
							}},
						},
					},
					Containers: []corev1.Container{{
						Name:      "valkey",
						Image:     cr.Spec.Image,
						Command:   []string{"sh", "-c", startupScript()},
						Resources: cr.Spec.Resources,
						Ports: []corev1.ContainerPort{
							{Name: "client", ContainerPort: clientPort},
							{Name: "cluster-bus", ContainerPort: busPort},
						},
						Env: []corev1.EnvVar{
							{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
							{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
							{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
							{Name: "SVC_NAME", Value: HeadlessServiceName(cr)},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: dataVolume, MountPath: dataMount},
							{Name: confVolume, MountPath: confMount},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{Command: []string{"valkey-cli", "-p", fmt.Sprint(clientPort), "ping"}},
							},
							InitialDelaySeconds: 5,
							PeriodSeconds:       5,
							FailureThreshold:    5,
						},
					}},
					Volumes: []corev1.Volume{{
						Name: confVolume,
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: ConfigMapName(cr)},
							},
						},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{vct},
		},
	}
}
