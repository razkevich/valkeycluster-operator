package slots

import (
	"testing"

	"github.com/razkevich/valkeycluster-operator/internal/cluster"
)

func totalCovered(ranges []cluster.SlotRange) int {
	n := 0
	for _, r := range ranges {
		n += r.End - r.Start + 1
	}
	return n
}

func TestDistributeCoversAllSlots(t *testing.T) {
	for _, n := range []int{1, 3, 4, 5, 8, 16} {
		ranges := Distribute(n)
		if len(ranges) != n {
			t.Fatalf("n=%d: got %d ranges, want %d", n, len(ranges), n)
		}
		if got := totalCovered(ranges); got != cluster.TotalSlots {
			t.Fatalf("n=%d: covered %d slots, want %d", n, got, cluster.TotalSlots)
		}
		// contiguous, non-overlapping, starting at 0, ending at 16383
		if ranges[0].Start != 0 {
			t.Fatalf("n=%d: first range starts at %d, want 0", n, ranges[0].Start)
		}
		if ranges[n-1].End != cluster.TotalSlots-1 {
			t.Fatalf("n=%d: last range ends at %d, want %d", n, ranges[n-1].End, cluster.TotalSlots-1)
		}
		for i := 1; i < n; i++ {
			if ranges[i].Start != ranges[i-1].End+1 {
				t.Fatalf("n=%d: gap/overlap between range %d and %d", n, i-1, i)
			}
		}
	}
}

func TestDistributeSingleShard(t *testing.T) {
	ranges := Distribute(1)
	if len(ranges) != 1 || ranges[0].Start != 0 || ranges[0].End != cluster.TotalSlots-1 {
		t.Fatalf("Distribute(1) = %+v, want one full range", ranges)
	}
}

func TestDistributeBalanced(t *testing.T) {
	// per-shard counts differ by at most 1
	ranges := Distribute(3)
	counts := []int{}
	for _, r := range ranges {
		counts = append(counts, r.End-r.Start+1)
	}
	min, max := counts[0], counts[0]
	for _, c := range counts {
		if c < min {
			min = c
		}
		if c > max {
			max = c
		}
	}
	if max-min > 1 {
		t.Fatalf("unbalanced distribution %v (spread %d)", counts, max-min)
	}
}

func TestDistributeInvalid(t *testing.T) {
	if r := Distribute(0); r != nil {
		t.Fatalf("Distribute(0) = %+v, want nil", r)
	}
}

func TestFormatRange(t *testing.T) {
	cases := map[cluster.SlotRange]string{
		{Start: 0, End: 5460}:      "0-5460",
		{Start: 100, End: 100}:     "100",
		{Start: 10923, End: 16383}: "10923-16383",
	}
	for in, want := range cases {
		if got := FormatRange(in); got != want {
			t.Fatalf("FormatRange(%+v) = %q, want %q", in, got, want)
		}
	}
}

func TestFormatRanges(t *testing.T) {
	got := FormatRanges([]cluster.SlotRange{{Start: 0, End: 100}, {Start: 200, End: 200}})
	if got != "0-100,200" {
		t.Fatalf("FormatRanges = %q", got)
	}
	if got := FormatRanges(nil); got != "" {
		t.Fatalf("FormatRanges(nil) = %q, want empty", got)
	}
}
