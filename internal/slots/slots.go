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

// Package slots computes how the 16384 Valkey hash slots are partitioned across
// shards. Pure functions, no I/O.
package slots

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/razkevich/valkeycluster-operator/internal/cluster"
)

// Distribute splits the full slot space evenly and contiguously across n shards.
// Shard i owns [i*Total/n, (i+1)*Total/n - 1]. Returns nil for n <= 0.
func Distribute(n int) []cluster.SlotRange {
	if n <= 0 {
		return nil
	}
	ranges := make([]cluster.SlotRange, n)
	for i := 0; i < n; i++ {
		start := i * cluster.TotalSlots / n
		end := (i+1)*cluster.TotalSlots/n - 1
		ranges[i] = cluster.SlotRange{Start: start, End: end}
	}
	return ranges
}

// TargetCounts returns the number of slots each of n shards should own after a
// balanced (re)distribution.
func TargetCounts(n int) []int {
	if n <= 0 {
		return nil
	}
	counts := make([]int, n)
	for i, r := range Distribute(n) {
		counts[i] = r.End - r.Start + 1
	}
	return counts
}

// FormatRange renders a slot range as Valkey does: "start-end", or "n" for a
// single slot.
func FormatRange(r cluster.SlotRange) string {
	if r.Start == r.End {
		return strconv.Itoa(r.Start)
	}
	return fmt.Sprintf("%d-%d", r.Start, r.End)
}

// FormatRanges renders a comma-separated list of ranges (empty string for none).
func FormatRanges(ranges []cluster.SlotRange) string {
	if len(ranges) == 0 {
		return ""
	}
	parts := make([]string, len(ranges))
	for i, r := range ranges {
		parts[i] = FormatRange(r)
	}
	return strings.Join(parts, ",")
}
