//go:build e2e
// +build e2e

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

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/razkevich/valkeycluster-operator/test/utils"
)

const (
	e2eName = "e2e"
	e2eSeed = e2eName + "-shard-0-0"
)

// eventually polls fn until it returns nil or the timeout elapses, failing the test
// with the last error on timeout. It is the std-testing equivalent of Gomega's
// Eventually, used so transient redirects during failover/resharding settle out
// rather than flake the test.
func eventually(t *testing.T, timeout, interval time.Duration, desc string, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		if lastErr = fn(); lastErr == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s: %v", timeout, desc, lastErr)
		}
		time.Sleep(interval)
	}
}

// equals adapts a (string, error) getter into an eventually condition that checks
// the trimmed output equals want.
func equals(get func() (string, error), want string) func() error {
	return func() error {
		got, err := get()
		if err != nil {
			return err
		}
		if got != want {
			return fmt.Errorf("got %q, want %q", got, want)
		}
		return nil
	}
}

func kubectl(args ...string) (string, error) {
	out, err := utils.Run(exec.Command("kubectl", args...))
	return strings.TrimSpace(out), err
}

// vexec runs a command inside the seed pod's valkey container in cluster mode.
func vexec(args ...string) (string, error) {
	full := append([]string{"exec", e2eSeed, "--", "valkey-cli", "-c", "-p", "6379"}, args...)
	return kubectl(full...)
}

func clusterCheck() (string, error) {
	return kubectl("exec", e2eSeed, "--", "valkey-cli", "--cluster", "check", "127.0.0.1:6379")
}

func phase() (string, error) {
	return kubectl("get", "valkeycluster", e2eName, "-o", "jsonpath={.status.phase}")
}

func readyShards() (string, error) {
	return kubectl("get", "valkeycluster", e2eName, "-o", "jsonpath={.status.readyShards}")
}

// readAllOK returns a condition that succeeds only when keys e2e:0..n-1 read back as
// their expected values.
func readAllOK(n int) func() error {
	return func() error {
		for i := 0; i < n; i++ {
			v, err := vexec("get", fmt.Sprintf("e2e:%d", i))
			if err != nil {
				return err
			}
			if want := fmt.Sprintf("v%d", i); v != want {
				return fmt.Errorf("key e2e:%d = %q, want %q", i, v, want)
			}
		}
		return nil
	}
}

// writeKeys writes keys e2e:lo..hi-1, retrying as a whole until all succeed (slots
// may be briefly mid-migration).
func writeKeys(lo, hi int) func() error {
	return func() error {
		for i := lo; i < hi; i++ {
			if _, err := vexec("set", fmt.Sprintf("e2e:%d", i), fmt.Sprintf("v%d", i)); err != nil {
				return err
			}
		}
		return nil
	}
}

// TestLifecycle exercises the three user stories end-to-end against a real Valkey
// cluster on Kind, in order: provision + use (US1), failover (US2), data-preserving
// resharding and replica scaling (US3). Steps are ordered and dependent, so the
// suite stops at the first failed step.
func TestLifecycle(t *testing.T) {
	deployOperatorAndCR(t)
	t.Cleanup(func() { teardownDeployment(t) })

	steps := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{"provision: formed cluster serves cross-shard reads/writes (US1)", stepProvision},
		{"failover: replica auto-promotes, data preserved (US2)", stepFailover},
		{"reshard 3->5 preserves data, ends with 5 primaries (US3)", stepReshardOut},
		{"replica scale-up without resharding (US3)", stepReplicaScale},
		{"scale-in 5->3 preserves data, tears down departed shards (US3)", stepScaleIn},
	}
	for _, s := range steps {
		if !t.Run(s.name, s.fn) {
			t.Fatalf("step %q failed; stopping lifecycle (later steps depend on it)", s.name)
		}
	}
}

func deployOperatorAndCR(t *testing.T) {
	t.Helper()

	if _, err := utils.Run(exec.Command("make", "install")); err != nil {
		t.Fatalf("install CRDs: %v", err)
	}
	if _, err := utils.Run(exec.Command("make", "deploy", "IMG="+managerImage)); err != nil {
		t.Fatalf("deploy operator: %v", err)
	}

	eventually(t, 3*time.Minute, 5*time.Second, "operator to become available",
		equals(func() (string, error) {
			return kubectl("-n", "valkeycluster-system", "get", "deploy",
				"valkeycluster-controller-manager", "-o", "jsonpath={.status.availableReplicas}")
		}, "1"))

	manifest := `apiVersion: cache.razkevich.dev/v1alpha1
kind: ValkeyCluster
metadata:
  name: e2e
  namespace: default
spec:
  shards: 3
  replicasPerShard: 1
  image: valkey/valkey:8
  storage:
    size: 512Mi
`
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	if _, err := utils.Run(cmd); err != nil {
		t.Fatalf("apply ValkeyCluster: %v", err)
	}
}

func teardownDeployment(t *testing.T) {
	t.Helper()
	if os.Getenv("KEEP_CLUSTER") == "true" {
		t.Logf("KEEP_CLUSTER=true: leaving ValkeyCluster %q and the operator deployed for inspection", e2eName)
		return
	}
	// Delete only the ValkeyCluster the suite created; leave the operator and CRD
	// installed so the cluster stays usable afterward (e.g. for benchmarks) and
	// re-runs are fast. `make test-e2e` tears down the whole Kind cluster separately.
	_, _ = kubectl("delete", "valkeycluster", e2eName, "--ignore-not-found", "--wait=true", "--timeout=120s")
}

func stepProvision(t *testing.T) {
	eventually(t, 6*time.Minute, 10*time.Second, "phase Ready", equals(phase, "Ready"))
	if got, _ := readyShards(); got != "3" {
		t.Fatalf("readyShards = %q, want 3", got)
	}

	out, err := clusterCheck()
	if err != nil {
		t.Fatalf("cluster check: %v", err)
	}
	if !strings.Contains(out, "All 16384 slots covered") {
		t.Errorf("cluster check missing full slot coverage:\n%s", out)
	}
	if !strings.Contains(out, "3 primaries") {
		t.Errorf("cluster check not reporting 3 primaries:\n%s", out)
	}

	eventually(t, time.Minute, 5*time.Second, "30 keys written across shards", writeKeys(0, 30))
	eventually(t, time.Minute, 5*time.Second, "all 30 keys readable", readAllOK(30))
}

func stepFailover(t *testing.T) {
	if _, err := kubectl("delete", "pod", e2eName+"-shard-1-0", "--wait=false"); err != nil {
		t.Fatalf("delete primary pod: %v", err)
	}

	eventually(t, 2*time.Minute, 5*time.Second, "full coverage after failover", func() error {
		out, err := clusterCheck()
		if err != nil {
			return err
		}
		if !strings.Contains(out, "All 16384 slots covered") {
			return fmt.Errorf("coverage incomplete:\n%s", out)
		}
		return nil
	})

	eventually(t, 3*time.Minute, 10*time.Second, "phase back to Ready", equals(phase, "Ready"))
	eventually(t, 2*time.Minute, 5*time.Second, "data intact after failover", readAllOK(30))
}

func stepReshardOut(t *testing.T) {
	if _, err := kubectl("patch", "valkeycluster", e2eName, "--type", "merge", "-p", `{"spec":{"shards":5}}`); err != nil {
		t.Fatalf("patch shards=5: %v", err)
	}

	eventually(t, 8*time.Minute, 10*time.Second, "readyShards=5", equals(readyShards, "5"))
	eventually(t, 2*time.Minute, 10*time.Second, "phase Ready after reshard", equals(phase, "Ready"))

	out, err := clusterCheck()
	if err != nil {
		t.Fatalf("cluster check: %v", err)
	}
	if !strings.Contains(out, "All 16384 slots covered") || !strings.Contains(out, "5 primaries") {
		t.Errorf("expected 5 primaries covering all slots:\n%s", out)
	}

	eventually(t, 2*time.Minute, 5*time.Second, "all keys survive reshard", readAllOK(30))
}

func stepReplicaScale(t *testing.T) {
	if _, err := kubectl("patch", "valkeycluster", e2eName, "--type", "merge", "-p", `{"spec":{"replicasPerShard":2}}`); err != nil {
		t.Fatalf("patch replicasPerShard=2: %v", err)
	}

	eventually(t, 3*time.Minute, 10*time.Second, "shard-0 StatefulSet at 3 replicas",
		equals(func() (string, error) {
			return kubectl("get", "statefulset", e2eName+"-shard-0", "-o", "jsonpath={.spec.replicas}")
		}, "3"))

	eventually(t, 5*time.Minute, 10*time.Second, "phase Ready after replica scale", equals(phase, "Ready"))
	if got, _ := readyShards(); got != "5" {
		t.Fatalf("readyShards = %q, want 5", got)
	}
}

func stepScaleIn(t *testing.T) {
	eventually(t, 2*time.Minute, 5*time.Second, "second key batch written (live)", writeKeys(30, 60))
	eventually(t, time.Minute, 5*time.Second, "all 60 keys readable", readAllOK(60))

	if _, err := kubectl("patch", "valkeycluster", e2eName, "--type", "merge", "-p", `{"spec":{"shards":3}}`); err != nil {
		t.Fatalf("patch shards=3: %v", err)
	}

	eventually(t, 10*time.Minute, 10*time.Second, "readyShards=3", equals(readyShards, "3"))
	eventually(t, 2*time.Minute, 10*time.Second, "phase Ready after scale-in", equals(phase, "Ready"))

	eventually(t, 2*time.Minute, 5*time.Second, "exactly 3 primaries cover all slots", func() error {
		out, err := clusterCheck()
		if err != nil {
			return err
		}
		if !strings.Contains(out, "All 16384 slots covered") || !strings.Contains(out, "3 primaries") {
			return fmt.Errorf("expected 3 primaries covering all slots:\n%s", out)
		}
		return nil
	})

	eventually(t, 3*time.Minute, 10*time.Second, "departed shard StatefulSets torn down", func() error {
		out, err := kubectl("get", "statefulset", "-l", "app.kubernetes.io/instance="+e2eName,
			"-o", "jsonpath={.items[*].metadata.name}")
		if err != nil {
			return err
		}
		if !strings.Contains(out, e2eName+"-shard-2") {
			return fmt.Errorf("shard-2 missing: %q", out)
		}
		if strings.Contains(out, e2eName+"-shard-3") || strings.Contains(out, e2eName+"-shard-4") {
			return fmt.Errorf("departed shards still present: %q", out)
		}
		return nil
	})

	eventually(t, 2*time.Minute, 5*time.Second, "all 60 keys survive scale-in", readAllOK(60))
}
