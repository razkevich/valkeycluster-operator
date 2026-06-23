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

func TestParseAddrPrefersHostname(t *testing.T) {
	host, port := parseAddr("10.0.0.1:6379@16379,demo-shard-0-0.demo-nodes.ns.svc")
	if host != "demo-shard-0-0.demo-nodes.ns.svc" || port != 6379 {
		t.Fatalf("parseAddr = %s:%d", host, port)
	}
	host, port = parseAddr("10.0.0.1:6379@16379")
	if host != "10.0.0.1" || port != 6379 {
		t.Fatalf("parseAddr(no hostname) = %s:%d", host, port)
	}
}
