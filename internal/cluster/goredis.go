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

package cluster

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

// PodExecutor runs a command inside a container and returns its combined output.
// Implemented over the Kubernetes pod-exec API (see exec.go).
type PodExecutor interface {
	Exec(ctx context.Context, namespace, pod, container string, command []string) (string, error)
}

// Admin is the production ClusterAdmin: CLUSTER RPCs via go-redis, and slot/key
// migration via `valkey-cli --cluster` executed inside a pod.
type Admin struct {
	exec      PodExecutor
	container string
}

var _ ClusterAdmin = (*Admin)(nil)

// NewAdmin returns an Admin that execs valkey-cli in the named container.
func NewAdmin(exec PodExecutor, container string) *Admin {
	return &Admin{exec: exec, container: container}
}

func dial(e Endpoint) *goredis.Client {
	return goredis.NewClient(&goredis.Options{Addr: e.Addr()})
}

// State observes the cluster from a seed node.
func (a *Admin) State(ctx context.Context, seed Endpoint) (ClusterState, error) {
	c := dial(seed)
	defer c.Close()

	info, err := c.ClusterInfo(ctx).Result()
	if err != nil {
		return ClusterState{}, fmt.Errorf("cluster info: %w", err)
	}
	nodesRaw, err := c.ClusterNodes(ctx).Result()
	if err != nil {
		return ClusterState{}, fmt.Errorf("cluster nodes: %w", err)
	}
	kv := parseInfo(info)
	assigned, _ := strconv.Atoi(kv["cluster_slots_assigned"])
	nodes := parseNodes(nodesRaw)
	return ClusterState{
		Formed:       kv["cluster_state"] != "" && assigned > 0,
		SlotsCovered: kv["cluster_state"] == "ok" && assigned == TotalSlots,
		OpenSlots:    hasOpenSlots(nodesRaw),
		Nodes:        nodes,
	}, nil
}

func (a *Admin) MyID(ctx context.Context, ep Endpoint) (string, error) {
	c := dial(ep)
	defer c.Close()
	return c.ClusterMyID(ctx).Result()
}

func (a *Admin) Meet(ctx context.Context, from, target Endpoint) error {
	c := dial(from)
	defer c.Close()
	// CLUSTER MEET requires an IP address, not a hostname. Resolve the target's
	// stable FQDN to an IP for the meet; the node still advertises its hostname
	// (cluster-announce-hostname) so gossip and client redirects use the FQDN.
	ip, err := resolveHost(ctx, target.Host)
	if err != nil {
		return fmt.Errorf("resolve %s: %w", target.Host, err)
	}
	return c.ClusterMeet(ctx, ip, strconv.Itoa(target.Port)).Err()
}

// resolveHost returns an IP for host (host may already be an IP).
func resolveHost(ctx context.Context, host string) (string, error) {
	if ip := net.ParseIP(host); ip != nil {
		return host, nil
	}
	addrs, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return "", err
	}
	if len(addrs) == 0 {
		return "", fmt.Errorf("no addresses for %s", host)
	}
	return addrs[0], nil
}

func (a *Admin) AddSlots(ctx context.Context, primary Endpoint, ranges []SlotRange) error {
	c := dial(primary)
	defer c.Close()
	for _, r := range ranges {
		if err := c.ClusterAddSlotsRange(ctx, r.Start, r.End).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (a *Admin) Replicate(ctx context.Context, replica Endpoint, primaryID string) error {
	c := dial(replica)
	defer c.Close()
	return c.ClusterReplicate(ctx, primaryID).Err()
}

func (a *Admin) Forget(ctx context.Context, from Endpoint, nodeID string) error {
	c := dial(from)
	defer c.Close()
	return c.ClusterForget(ctx, nodeID).Err()
}

func (a *Admin) Failover(ctx context.Context, replica Endpoint) error {
	c := dial(replica)
	defer c.Close()
	return c.ClusterFailover(ctx).Err()
}

// migrateTimeoutMillis caps each MIGRATE during reshard/rebalance/fix. Legit
// MIGRATEs of small values complete in milliseconds, so a low cap turns an
// occasional stalled transfer into a fast failure that the reconcile retries,
// instead of a 60s (default) wedge that aborts the whole operation.
const migrateTimeoutMillis = "10000"

// Rebalance runs `valkey-cli --cluster rebalance` inside the seed pod.
func (a *Admin) Rebalance(ctx context.Context, seed Endpoint, opts RebalanceOpts) error {
	args := []string{"valkey-cli", "--cluster", "rebalance", fmt.Sprintf("127.0.0.1:%d", seed.Port), "--cluster-yes", "--cluster-timeout", migrateTimeoutMillis}
	if opts.UseEmptyMasters {
		args = append(args, "--cluster-use-empty-masters")
	}
	for _, id := range opts.WeightZeroIDs {
		args = append(args, "--cluster-weight", id+"=0")
	}
	_, err := a.exec.Exec(ctx, seed.Namespace, seed.PodName, a.container, args)
	return err
}

// Reshard moves n slots to toNodeID, from fromNodeID (or "all" if empty).
func (a *Admin) Reshard(ctx context.Context, seed Endpoint, fromNodeID, toNodeID string, n int) error {
	if n <= 0 {
		return nil
	}
	from := fromNodeID
	if from == "" {
		from = "all"
	}
	args := []string{
		"valkey-cli", "--cluster", "reshard", fmt.Sprintf("127.0.0.1:%d", seed.Port),
		"--cluster-from", from, "--cluster-to", toNodeID,
		"--cluster-slots", strconv.Itoa(n), "--cluster-yes", "--cluster-timeout", migrateTimeoutMillis,
	}
	_, err := a.exec.Exec(ctx, seed.Namespace, seed.PodName, a.container, args)
	return err
}

// Fix runs `valkey-cli --cluster fix` inside the seed pod to repair open slots.
func (a *Admin) Fix(ctx context.Context, seed Endpoint) error {
	args := []string{"valkey-cli", "--cluster", "fix", fmt.Sprintf("127.0.0.1:%d", seed.Port), "--cluster-yes", "--cluster-timeout", migrateTimeoutMillis}
	_, err := a.exec.Exec(ctx, seed.Namespace, seed.PodName, a.container, args)
	return err
}

// hasOpenSlots reports whether CLUSTER NODES shows any slot mid-migration.
// Migrating slots appear as [slot->-nodeid] and importing as [slot-<-nodeid];
// the "->-" / "-<-" substrings are reliable markers.
func hasOpenSlots(nodesRaw string) bool {
	return strings.Contains(nodesRaw, "->-") || strings.Contains(nodesRaw, "-<-")
}

// parseInfo parses CLUSTER INFO "key:value" lines.
func parseInfo(s string) map[string]string {
	out := map[string]string{}
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if k, v, ok := strings.Cut(line, ":"); ok {
			out[k] = v
		}
	}
	return out
}

// parseNodes parses CLUSTER NODES output into NodeInfo records.
// Line: <id> <ip:port@cport[,hostname]> <flags> <master> <ping> <pong> <epoch> <link> [slots...]
func parseNodes(s string) []NodeInfo {
	var nodes []NodeInfo
	for _, line := range strings.Split(s, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		host, port := parseAddr(fields[1])
		flags := strings.Split(fields[2], ",")
		master := fields[3]
		if master == "-" {
			master = ""
		}
		n := NodeInfo{
			ID:        fields[0],
			Host:      host,
			Port:      port,
			Flags:     flags,
			MasterID:  master,
			Connected: fields[7] == "connected",
		}
		for _, tok := range fields[8:] {
			if strings.HasPrefix(tok, "[") {
				continue // slot in migration; ignore
			}
			if start, end, ok := parseSlotToken(tok); ok {
				n.Slots = append(n.Slots, SlotRange{Start: start, End: end})
			}
		}
		nodes = append(nodes, n)
	}
	return nodes
}

// parseAddr extracts host and client port from "ip:port@cport[,hostname]".
// Prefers the announced hostname when present.
func parseAddr(s string) (string, int) {
	hostname := ""
	if idx := strings.Index(s, ","); idx >= 0 {
		hostname = s[idx+1:]
		s = s[:idx]
	}
	if idx := strings.Index(s, "@"); idx >= 0 {
		s = s[:idx]
	}
	host, portStr, _ := strings.Cut(s, ":")
	port, _ := strconv.Atoi(portStr)
	if hostname != "" {
		host = hostname
	}
	return host, port
}

func parseSlotToken(tok string) (int, int, bool) {
	if lo, hi, ok := strings.Cut(tok, "-"); ok {
		s, e1 := strconv.Atoi(lo)
		e, e2 := strconv.Atoi(hi)
		if e1 == nil && e2 == nil {
			return s, e, true
		}
		return 0, 0, false
	}
	s, err := strconv.Atoi(tok)
	if err != nil {
		return 0, 0, false
	}
	return s, s, true
}
