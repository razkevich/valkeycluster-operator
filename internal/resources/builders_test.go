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

// TestConfigHashTriggersRollout pins the propagation guarantee: a change to any
// valkey.conf-rendered setting (here io-threads) changes the pod-template
// config-hash annotation, so the StatefulSet rolls and the new config actually
// takes effect — while an unrelated change leaves the hash stable (no spurious
// restarts).
func TestConfigHashTriggersRollout(t *testing.T) {
	base := testCR()
	hashOf := func(cr *cachev1alpha1.ValkeyCluster) string {
		return StatefulSet(cr, 0).Spec.Template.Annotations[configHashAnnotation]
	}

	if hashOf(base) == "" {
		t.Fatal("pod template is missing the config-hash annotation")
	}
	// identical spec → identical hash (idempotent, no spurious rollout)
	if hashOf(base) != hashOf(testCR()) {
		t.Error("config-hash is not stable for identical specs")
	}
	// a performance change → different hash → rollout
	changed := testCR()
	changed.Spec.Performance.IOThreads = 4
	if hashOf(base) == hashOf(changed) {
		t.Error("changing ioThreads did not change the config-hash (would be inert)")
	}
	// an haPolicy change → different hash → rollout
	hp := testCR()
	hp.Spec.HAPolicy.MinReplicasToWrite = 1
	if hashOf(base) == hashOf(hp) {
		t.Error("changing minReplicasToWrite did not change the config-hash")
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
		ClusterNodeTimeoutMillis: 8000,
	}
	cr.Spec.Persistence = cachev1alpha1.PersistenceSpec{
		Mode:        cachev1alpha1.PersistenceAOF,
		AppendFsync: cachev1alpha1.AppendFsyncAlways,
	}
	conf := RenderValkeyConf(cr)
	wants := []string{
		"cluster-enabled yes",
		"cluster-config-file /data/nodes.conf",
		"cluster-node-timeout 8000",
		"cluster-require-full-coverage no",
		"cluster-preferred-endpoint-type hostname",
		"cluster-port 16379",
		"appendonly yes",
		"appendfsync always",
		"min-replicas-to-write 2",
	}
	for _, w := range wants {
		if !strings.Contains(conf, w) {
			t.Errorf("rendered conf missing %q\n---\n%s", w, conf)
		}
	}
}

func TestRenderValkeyConfMaxMemory(t *testing.T) {
	cr := testCR()
	cr.Spec.Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")}
	conf := RenderValkeyConf(cr)
	// 70% of 2Gi (2147483648) = 1503238553
	if !strings.Contains(conf, "maxmemory 1503238553") {
		t.Errorf("maxmemory not set to 70%% of limit\n---\n%s", conf)
	}
	if !strings.Contains(conf, "maxmemory-policy noeviction") {
		t.Errorf("maxmemory-policy not set\n---\n%s", conf)
	}
}

func TestRenderValkeyConfNoMaxMemoryWithoutLimit(t *testing.T) {
	if strings.Contains(RenderValkeyConf(testCR()), "maxmemory ") {
		t.Error("maxmemory should not be set when no memory limit is given")
	}
}

func TestRenderValkeyConfDefaults(t *testing.T) {
	// zero-value spec should render sane defaults (AOF on, everysec, RDB off)
	conf := RenderValkeyConf(testCR())
	for _, w := range []string{"cluster-require-full-coverage yes", "appendonly yes", "appendfsync everysec", "cluster-node-timeout 5000", "min-replicas-to-write 0", "save \"\"", "io-threads 1", "maxmemory-policy noeviction"} {
		if !strings.Contains(conf, w) {
			t.Errorf("default conf missing %q\n---\n%s", w, conf)
		}
	}
}

func TestRenderValkeyConfPerformance(t *testing.T) {
	cr := testCR()
	cr.Spec.Performance = cachev1alpha1.PerformanceSpec{IOThreads: 4, MaxmemoryPolicy: "allkeys-lfu"}
	conf := RenderValkeyConf(cr)
	for _, w := range []string{"io-threads 4", "maxmemory-policy allkeys-lfu"} {
		if !strings.Contains(conf, w) {
			t.Errorf("missing %q\n---\n%s", w, conf)
		}
	}
}

func TestRenderValkeyConfPersistenceModes(t *testing.T) {
	cases := map[cachev1alpha1.PersistenceMode]struct{ wants, absent []string }{
		cachev1alpha1.PersistenceRDB:       {wants: []string{"appendonly no", "save 3600 1 300 100 60 10000"}, absent: []string{"appendonly yes"}},
		cachev1alpha1.PersistenceAOFAndRDB: {wants: []string{"appendonly yes", "save 3600 1 300 100 60 10000"}},
		cachev1alpha1.PersistenceNone:      {wants: []string{"appendonly no", "save \"\""}, absent: []string{"appendonly yes"}},
	}
	for mode, exp := range cases {
		cr := testCR()
		cr.Spec.Persistence.Mode = mode
		conf := RenderValkeyConf(cr)
		for _, w := range exp.wants {
			if !strings.Contains(conf, w) {
				t.Errorf("mode %s: missing %q\n---\n%s", mode, w, conf)
			}
		}
		for _, a := range exp.absent {
			if strings.Contains(conf, a) {
				t.Errorf("mode %s: unexpected %q", mode, a)
			}
		}
	}
}
