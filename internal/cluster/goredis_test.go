package cluster

import (
	"strings"
	"testing"
)

func TestParseInfo(t *testing.T) {
	in := "cluster_state:ok\ncluster_slots_assigned:16384\ncluster_known_nodes:6\n"
	kv := parseInfo(in)
	if kv["cluster_state"] != "ok" || kv["cluster_slots_assigned"] != "16384" {
		t.Fatalf("parseInfo = %+v", kv)
	}
}

func TestParseNodes(t *testing.T) {
	// id addr flags master ping pong epoch link slots...
	in := strings.Join([]string{
		"a1 10.0.0.1:6379@16379,demo-shard-0-0.demo-nodes.ns.svc master - 0 0 1 connected 0-5460",
		"b2 10.0.0.2:6379@16379,demo-shard-0-1.demo-nodes.ns.svc slave a1 0 0 1 connected",
		"c3 10.0.0.3:6379@16379,demo-shard-1-0.demo-nodes.ns.svc master - 0 0 2 connected 5461-10922",
	}, "\n")
	nodes := parseNodes(in)
	if len(nodes) != 3 {
		t.Fatalf("got %d nodes", len(nodes))
	}
	if !nodes[0].IsPrimary() || nodes[0].Host != "demo-shard-0-0.demo-nodes.ns.svc" {
		t.Errorf("node0 = %+v", nodes[0])
	}
	if nodes[0].SlotCount() != 5461 {
		t.Errorf("node0 slots = %d, want 5461", nodes[0].SlotCount())
	}
	if nodes[1].IsPrimary() || nodes[1].MasterID != "a1" {
		t.Errorf("node1 should be replica of a1, got %+v", nodes[1])
	}
}

func TestHasOpenSlots(t *testing.T) {
	stable := "a1 10.0.0.1:6379@16379 master - 0 0 1 connected 0-5460"
	if hasOpenSlots(stable) {
		t.Error("stable cluster reported open slots")
	}
	migrating := stable + " [5461->-b2deadbeef]"
	if !hasOpenSlots(migrating) {
		t.Error("migrating slot not detected")
	}
	importing := stable + " [5461-<-b2deadbeef]"
	if !hasOpenSlots(importing) {
		t.Error("importing slot not detected")
	}
}

func TestParseAddr(t *testing.T) {
	ip, port, hostname := parseAddr("10.0.0.1:6379@16379,demo-shard-0-0.demo-nodes.ns.svc")
	if ip != "10.0.0.1" || port != 6379 || hostname != "demo-shard-0-0.demo-nodes.ns.svc" {
		t.Fatalf("parseAddr = %s:%d host=%s", ip, port, hostname)
	}
	ip, port, hostname = parseAddr("10.0.0.1:6379@16379")
	if ip != "10.0.0.1" || port != 6379 || hostname != "" {
		t.Fatalf("parseAddr(no hostname) = %s:%d host=%s", ip, port, hostname)
	}
}

func TestParseOpenSlots(t *testing.T) {
	raw := strings.Join([]string{
		"a1 10.0.0.1:6379@16379 master - 0 0 1 connected 0-780 [781->-b2] [900-<-c3]",
		"b2 10.0.0.2:6379@16379 master - 0 0 2 connected 782-16383",
	}, "\n")
	got := parseOpenSlots(raw)
	if len(got) != 2 {
		t.Fatalf("parseOpenSlots = %v, want 2 slots (781, 900)", got)
	}
	m := map[int]bool{got[0]: true, got[1]: true}
	if !m[781] || !m[900] {
		t.Fatalf("parseOpenSlots = %v, want {781,900}", got)
	}
}
