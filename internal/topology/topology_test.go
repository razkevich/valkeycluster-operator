package topology

import "testing"

func TestDecideForm(t *testing.T) {
	p := Decide(Desired{Shards: 3, ReplicasPerShard: 1}, Observed{Formed: false})
	if p.Kind != ActionForm {
		t.Fatalf("unformed cluster: got %s, want %s", p.Kind, ActionForm)
	}
}

func TestDecideRepairBeforeTopologyChange(t *testing.T) {
	// Formed but slots not covered (interrupted migration) -> repair gate first,
	// even if shard count also differs.
	p := Decide(Desired{Shards: 5, ReplicasPerShard: 1}, Observed{
		Formed: true, SlotsCovered: false, PrimaryCount: 3, ReplicaCounts: []int{1, 1, 1},
	})
	if p.Kind != ActionRepair {
		t.Fatalf("uncovered slots: got %s, want %s", p.Kind, ActionRepair)
	}
}

func TestDecideScaleOutShards(t *testing.T) {
	p := Decide(Desired{Shards: 5, ReplicasPerShard: 1}, Observed{
		Formed: true, SlotsCovered: true, PrimaryCount: 3, ReplicaCounts: []int{1, 1, 1},
	})
	if p.Kind != ActionScaleOutShards || p.AddShards != 2 {
		t.Fatalf("got %+v, want ScaleOutShards add=2", p)
	}
}

func TestDecideScaleInShards(t *testing.T) {
	p := Decide(Desired{Shards: 3, ReplicasPerShard: 1}, Observed{
		Formed: true, SlotsCovered: true, PrimaryCount: 5, ReplicaCounts: []int{1, 1, 1, 1, 1},
	})
	if p.Kind != ActionScaleInShards || p.RemoveShards != 2 {
		t.Fatalf("got %+v, want ScaleInShards remove=2", p)
	}
}

func TestDecideScaleReplicas(t *testing.T) {
	p := Decide(Desired{Shards: 3, ReplicasPerShard: 2}, Observed{
		Formed: true, SlotsCovered: true, PrimaryCount: 3, ReplicaCounts: []int{1, 1, 1},
	})
	if p.Kind != ActionScaleReplicas || p.DesiredReplicasPerShard != 2 {
		t.Fatalf("got %+v, want ScaleReplicas desired=2", p)
	}
}

func TestDecideNoChange(t *testing.T) {
	p := Decide(Desired{Shards: 3, ReplicasPerShard: 1}, Observed{
		Formed: true, SlotsCovered: true, PrimaryCount: 3, ReplicaCounts: []int{1, 1, 1},
	})
	if p.Kind != ActionNone {
		t.Fatalf("steady state: got %s, want %s", p.Kind, ActionNone)
	}
}

func TestDecideShardChangeTakesPrecedenceOverReplicas(t *testing.T) {
	// both shard count and replica count differ -> shard change first
	p := Decide(Desired{Shards: 5, ReplicasPerShard: 2}, Observed{
		Formed: true, SlotsCovered: true, PrimaryCount: 3, ReplicaCounts: []int{1, 1, 1},
	})
	if p.Kind != ActionScaleOutShards {
		t.Fatalf("got %s, want ScaleOutShards (shard change precedence)", p.Kind)
	}
}
