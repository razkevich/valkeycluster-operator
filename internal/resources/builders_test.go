package resources

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/razkevich/valkeycluster-operator/api/v1alpha1"
)

func testCR() *cachev1alpha1.ValkeyCluster {
	return &cachev1alpha1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"},
		Spec: cachev1alpha1.ValkeyClusterSpec{
			Shards:           3,
			ReplicasPerShard: 1,
			Image:            "valkey/valkey:8",
			Storage:          cachev1alpha1.StorageSpec{Size: resource.MustParse("1Gi")},
		},
	}
}

func TestNaming(t *testing.T) {
	cr := testCR()
	if HeadlessServiceName(cr) != "demo-nodes" {
		t.Errorf("headless svc = %s", HeadlessServiceName(cr))
	}
	if StatefulSetName(cr, 2) != "demo-shard-2" {
		t.Errorf("sts = %s", StatefulSetName(cr, 2))
	}
	if PodName(cr, 1, 0) != "demo-shard-1-0" {
		t.Errorf("pod = %s", PodName(cr, 1, 0))
	}
	if PodFQDN(cr, 0, 0) != "demo-shard-0-0.demo-nodes.ns.svc" {
		t.Errorf("fqdn = %s", PodFQDN(cr, 0, 0))
	}
}

func TestHeadlessService(t *testing.T) {
	svc := HeadlessService(testCR())
	if svc.Spec.ClusterIP != "None" {
		t.Errorf("want headless (ClusterIP None), got %q", svc.Spec.ClusterIP)
	}
	if !svc.Spec.PublishNotReadyAddresses {
		t.Error("must publish not-ready addresses so nodes resolve during formation")
	}
	ports := map[string]int32{}
	for _, p := range svc.Spec.Ports {
		ports[p.Name] = p.Port
	}
	if ports["client"] != 6379 || ports["cluster-bus"] != 16379 {
		t.Errorf("ports = %+v, want client 6379 + cluster-bus 16379", ports)
	}
}

func TestStatefulSetReplicasAndStorage(t *testing.T) {
	cr := testCR()
	sts := StatefulSet(cr, 0)
	if *sts.Spec.Replicas != 2 { // 1 primary + 1 replica
		t.Errorf("replicas = %d, want 2", *sts.Spec.Replicas)
	}
	if sts.Spec.ServiceName != "demo-nodes" {
		t.Errorf("serviceName = %s", sts.Spec.ServiceName)
	}
	if len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("want 1 volumeClaimTemplate")
	}
	got := sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]
	if got.String() != "1Gi" {
		t.Errorf("storage = %s, want 1Gi", got.String())
	}
}

func TestStatefulSetAntiAffinity(t *testing.T) {
	sts := StatefulSet(testCR(), 1)
	aff := sts.Spec.Template.Spec.Affinity
	if aff == nil || aff.PodAntiAffinity == nil ||
		len(aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution) == 0 {
		t.Fatal("expected preferred pod anti-affinity so a shard's pods spread across nodes")
	}
	term := aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0].PodAffinityTerm
	if term.TopologyKey != "kubernetes.io/hostname" {
		t.Errorf("topologyKey = %s", term.TopologyKey)
	}
	if term.LabelSelector.MatchLabels[LabelShard] != "1" {
		t.Errorf("anti-affinity must scope to the shard label, got %+v", term.LabelSelector.MatchLabels)
	}
}

func TestRenderValkeyConf(t *testing.T) {
	cr := testCR()
	fc := false
	cr.Spec.HAPolicy = cachev1alpha1.HAPolicy{
		MinReplicasToWrite:       2,
		RequireFullCoverage:      &fc,
		AppendFsync:              cachev1alpha1.AppendFsyncAlways,
		ClusterNodeTimeoutMillis: 8000,
	}
	conf := RenderValkeyConf(cr)
	wants := []string{
		"cluster-enabled yes",
		"cluster-config-file /data/nodes.conf",
		"cluster-node-timeout 8000",
		"cluster-require-full-coverage no",
		"cluster-preferred-endpoint-type hostname",
		"cluster-port 16379",
		"appendfsync always",
		"min-replicas-to-write 2",
	}
	for _, w := range wants {
		if !strings.Contains(conf, w) {
			t.Errorf("rendered conf missing %q\n---\n%s", w, conf)
		}
	}
}

func TestRenderValkeyConfDefaults(t *testing.T) {
	// zero-value HAPolicy should still render sane defaults
	conf := RenderValkeyConf(testCR())
	for _, w := range []string{"cluster-require-full-coverage yes", "appendfsync everysec", "cluster-node-timeout 5000", "min-replicas-to-write 0"} {
		if !strings.Contains(conf, w) {
			t.Errorf("default conf missing %q\n---\n%s", w, conf)
		}
	}
}
