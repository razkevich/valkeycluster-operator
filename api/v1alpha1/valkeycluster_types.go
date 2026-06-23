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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// StorageSpec describes the per-node persistent storage.
type StorageSpec struct {
	// size is the per-node persistent volume size. Immutable after creation
	// (volume expansion is out of scope).
	// +kubebuilder:default="1Gi"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storage.size is immutable"
	Size resource.Quantity `json:"size,omitempty"`

	// storageClassName selects the StorageClass for the per-node PVCs.
	// Empty string uses the cluster default StorageClass.
	// +optional
	StorageClassName string `json:"storageClassName,omitempty"`
}

// +kubebuilder:validation:Enum=always;everysec;no
type AppendFsync string

const (
	AppendFsyncAlways   AppendFsync = "always"
	AppendFsyncEverySec AppendFsync = "everysec"
	AppendFsyncNo       AppendFsync = "no"
)

// HAPolicy exposes the Valkey clustering/HA settings whose performance
// trade-offs the operator lets users tune. See docs/clustering-ha-tradeoffs.md.
type HAPolicy struct {
	// minReplicasToWrite makes a primary refuse writes unless at least this many
	// replicas are connected and in sync (maps to min-replicas-to-write).
	// Trade-off: durability vs. write-availability. Default 0 (always accept writes).
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=0
	// +optional
	MinReplicasToWrite int32 `json:"minReplicasToWrite,omitempty"`

	// requireFullCoverage controls whether the cluster serves its reachable slots
	// when some slots are unavailable (maps to cluster-require-full-coverage).
	// Trade-off: availability vs. correctness. Default true.
	// +kubebuilder:default=true
	// +optional
	RequireFullCoverage *bool `json:"requireFullCoverage,omitempty"`

	// appendFsync sets the AOF fsync cadence (maps to appendfsync).
	// Trade-off: durability vs. throughput. Default everysec.
	// +kubebuilder:default=everysec
	// +optional
	AppendFsync AppendFsync `json:"appendFsync,omitempty"`

	// clusterNodeTimeoutMillis is the failure-detection window in milliseconds
	// (maps to cluster-node-timeout). Trade-off: failover speed vs. false positives.
	// +kubebuilder:validation:Minimum=1000
	// +kubebuilder:default=5000
	// +optional
	ClusterNodeTimeoutMillis int32 `json:"clusterNodeTimeoutMillis,omitempty"`
}

// ValkeyClusterSpec defines the desired topology of a Valkey cluster.
type ValkeyClusterSpec struct {
	// shards is the number of data partitions (primaries). The keyspace is split
	// across shards. Must be 1 (HA-only, no sharding) or >= 3 (failover quorum).
	// +kubebuilder:default=3
	// +kubebuilder:validation:Maximum=16
	// +kubebuilder:validation:XValidation:rule="self == 1 || self >= 3",message="shards must be 1 or >= 3 (2 cannot form a failover quorum)"
	// +optional
	Shards int32 `json:"shards,omitempty"`

	// replicasPerShard is the number of HA replica copies per shard. 0 means no HA.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=5
	// +kubebuilder:default=1
	// +optional
	ReplicasPerShard int32 `json:"replicasPerShard,omitempty"`

	// image is the Valkey container image to run (must include valkey-cli).
	// +kubebuilder:default="valkey/valkey:8"
	// +optional
	Image string `json:"image,omitempty"`

	// storage configures the per-node persistent volume.
	// +optional
	Storage StorageSpec `json:"storage,omitempty"`

	// resources are the compute resources for the valkey container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// haPolicy tunes the clustering/HA settings and their performance trade-offs.
	// +optional
	HAPolicy HAPolicy `json:"haPolicy,omitempty"`
}

// ValkeyClusterPhase is the high-level lifecycle phase of the cluster.
type ValkeyClusterPhase string

const (
	PhasePending         ValkeyClusterPhase = "Pending"
	PhaseProvisioning    ValkeyClusterPhase = "Provisioning"
	PhaseForming         ValkeyClusterPhase = "Forming"
	PhaseReady           ValkeyClusterPhase = "Ready"
	PhaseResharding      ValkeyClusterPhase = "Resharding"
	PhaseScalingReplicas ValkeyClusterPhase = "ScalingReplicas"
	PhaseDegraded        ValkeyClusterPhase = "Degraded"
	PhaseFailed          ValkeyClusterPhase = "Failed"
)

// Condition types.
const (
	ConditionAvailable   = "Available"
	ConditionProgressing = "Progressing"
	ConditionDegraded    = "Degraded"
)

// ShardStatus is the observed state of a single shard.
type ShardStatus struct {
	// index is the shard ordinal.
	Index int32 `json:"index"`

	// primaryPod is the pod currently acting as this shard's primary.
	// +optional
	PrimaryPod string `json:"primaryPod,omitempty"`

	// primaryNodeID is the Valkey node ID of the current primary.
	// +optional
	PrimaryNodeID string `json:"primaryNodeID,omitempty"`

	// slots is the hash-slot range owned by this shard (e.g. "0-5460").
	// +optional
	Slots string `json:"slots,omitempty"`

	// readyReplicas is the number of in-sync replicas for this shard.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// nodeIDs are the Valkey node IDs that belong to this shard.
	// +optional
	NodeIDs []string `json:"nodeIDs,omitempty"`
}

// ValkeyClusterStatus is the observed state of a ValkeyCluster.
type ValkeyClusterStatus struct {
	// phase is the high-level lifecycle phase.
	// +optional
	Phase ValkeyClusterPhase `json:"phase,omitempty"`

	// observedGeneration is the .metadata.generation this status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// readyShards is the number of shards with a reachable primary and assigned slots.
	// +optional
	ReadyShards int32 `json:"readyShards,omitempty"`

	// shards holds the observed per-shard state.
	// +listType=map
	// +listMapKey=index
	// +optional
	Shards []ShardStatus `json:"shards,omitempty"`

	// conditions represent the current state of the ValkeyCluster resource
	// (Available, Progressing, Degraded).
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Shards",type=integer,JSONPath=`.spec.shards`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicasPerShard`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyShards`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ValkeyCluster is the Schema for the valkeyclusters API.
type ValkeyCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of ValkeyCluster
	// +required
	Spec ValkeyClusterSpec `json:"spec"`

	// status defines the observed state of ValkeyCluster
	// +optional
	Status ValkeyClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ValkeyClusterList contains a list of ValkeyCluster
type ValkeyClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ValkeyCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &ValkeyCluster{}, &ValkeyClusterList{})
		return nil
	})
}
