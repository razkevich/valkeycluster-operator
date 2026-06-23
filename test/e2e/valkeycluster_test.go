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
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/razkevich/valkeycluster-operator/test/utils"
)

// End-to-end coverage for the three user stories against a real Valkey cluster on
// Kind: provision + use (US1), failover (US2), data-preserving resharding and
// replica scaling (US3). Runs in the `default` namespace.
var _ = Describe("ValkeyCluster lifecycle", Ordered, func() {
	const (
		name = "e2e"
		seed = name + "-shard-0-0"
	)

	// kubectl runs a kubectl command and returns trimmed stdout.
	kubectl := func(args ...string) (string, error) {
		out, err := utils.Run(exec.Command("kubectl", args...))
		return strings.TrimSpace(out), err
	}
	// exec runs a command inside the seed pod's valkey container.
	vexec := func(args ...string) (string, error) {
		full := append([]string{"exec", seed, "--", "valkey-cli", "-c", "-p", "6379"}, args...)
		return kubectl(full...)
	}
	clusterCheck := func() (string, error) {
		return kubectl("exec", seed, "--", "valkey-cli", "--cluster", "check", "127.0.0.1:6379")
	}
	phase := func() string {
		p, _ := kubectl("get", "valkeycluster", name, "-o", "jsonpath={.status.phase}")
		return p
	}
	readyShards := func() string {
		r, _ := kubectl("get", "valkeycluster", name, "-o", "jsonpath={.status.readyShards}")
		return r
	}
	// readAllOK returns a func that succeeds only when all n keys read back
	// correctly; used with Eventually so transient redirects during failover or
	// resharding settle out rather than flake the test.
	readAllOK := func(n int) func() error {
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

	BeforeAll(func() {
		By("installing CRDs")
		_, err := utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred())

		By("deploying the operator")
		_, err = utils.Run(exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage)))
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the operator to be available")
		Eventually(func() (string, error) {
			return kubectl("-n", "valkeycluster-system", "get", "deploy",
				"valkeycluster-controller-manager", "-o", "jsonpath={.status.availableReplicas}")
		}, 3*time.Minute, 5*time.Second).Should(Equal("1"))

		By("creating a 3x1 ValkeyCluster")
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
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		By("deleting the ValkeyCluster")
		_, _ = kubectl("delete", "valkeycluster", name, "--ignore-not-found", "--wait=true", "--timeout=120s")
		By("undeploying the operator")
		_, _ = utils.Run(exec.Command("make", "undeploy", "ignore-not-found=true"))
	})

	It("provisions a formed, fully-serving cluster and serves cross-shard reads/writes (US1)", func() {
		By("reaching Ready with 3 shards")
		Eventually(phase, 6*time.Minute, 10*time.Second).Should(Equal("Ready"))
		Expect(readyShards()).To(Equal("3"))

		By("covering all 16384 slots across exactly 3 primaries")
		out, err := clusterCheck()
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("All 16384 slots covered"))
		Expect(out).To(ContainSubstring("3 primaries"))

		By("writing and reading keys that span shards")
		Eventually(func() error {
			for i := 0; i < 30; i++ {
				if _, err := vexec("set", fmt.Sprintf("e2e:%d", i), fmt.Sprintf("v%d", i)); err != nil {
					return err
				}
			}
			return nil
		}, time.Minute, 5*time.Second).Should(Succeed())
		Eventually(readAllOK(30), time.Minute, 5*time.Second).Should(Succeed())
	})

	It("auto-promotes a replica when a primary is lost, preserving data (US2)", func() {
		By("deleting shard 1's primary pod")
		_, err := kubectl("delete", "pod", name+"-shard-1-0", "--wait=false")
		Expect(err).NotTo(HaveOccurred())

		By("cluster keeps full coverage after failover")
		Eventually(func() (string, error) { return clusterCheck() }, 2*time.Minute, 5*time.Second).
			Should(ContainSubstring("All 16384 slots covered"))

		By("returning to Ready")
		Eventually(phase, 3*time.Minute, 10*time.Second).Should(Equal("Ready"))

		By("data written before the failover is intact")
		Eventually(readAllOK(30), 2*time.Minute, 5*time.Second).Should(Succeed())
	})

	It("reshards 3->5 preserving data, ending with exactly 5 primaries (US3)", func() {
		By("scaling shards to 5")
		_, err := kubectl("patch", "valkeycluster", name, "--type", "merge", "-p", `{"spec":{"shards":5}}`)
		Expect(err).NotTo(HaveOccurred())

		By("settling back to Ready with 5 shards")
		Eventually(readyShards, 8*time.Minute, 10*time.Second).Should(Equal("5"))
		Eventually(phase, 2*time.Minute, 10*time.Second).Should(Equal("Ready"))

		By("exactly 5 primaries cover all slots")
		out, err := clusterCheck()
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("All 16384 slots covered"))
		Expect(out).To(ContainSubstring("5 primaries"))

		By("every previously written key survives")
		Eventually(readAllOK(30), 2*time.Minute, 5*time.Second).Should(Succeed())
	})

	It("scales replicas per shard without resharding (US3)", func() {
		By("increasing replicasPerShard to 2")
		_, err := kubectl("patch", "valkeycluster", name, "--type", "merge", "-p", `{"spec":{"replicasPerShard":2}}`)
		Expect(err).NotTo(HaveOccurred())

		By("each shard StatefulSet scales to 3 pods")
		Eventually(func() (string, error) {
			return kubectl("get", "statefulset", name+"-shard-0", "-o", "jsonpath={.spec.replicas}")
		}, 3*time.Minute, 10*time.Second).Should(Equal("3"))

		By("returning to Ready with 5 shards still covered")
		Eventually(phase, 5*time.Minute, 10*time.Second).Should(Equal("Ready"))
		Expect(readyShards()).To(Equal("5"))
	})

	It("scales in 5->3 preserving data and tearing down departed shards (US3)", func() {
		By("writing a second batch of keys at the scale-in pivot (live writes)")
		Eventually(func() error {
			for i := 30; i < 60; i++ {
				if _, err := vexec("set", fmt.Sprintf("e2e:%d", i), fmt.Sprintf("v%d", i)); err != nil {
					return err
				}
			}
			return nil
		}, 2*time.Minute, 5*time.Second).Should(Succeed())
		Eventually(readAllOK(60), time.Minute, 5*time.Second).Should(Succeed())

		By("scaling shards back to 3")
		_, err := kubectl("patch", "valkeycluster", name, "--type", "merge", "-p", `{"spec":{"shards":3}}`)
		Expect(err).NotTo(HaveOccurred())

		By("settling back to Ready with exactly 3 shards")
		Eventually(readyShards, 10*time.Minute, 10*time.Second).Should(Equal("3"))
		Eventually(phase, 2*time.Minute, 10*time.Second).Should(Equal("Ready"))

		By("exactly 3 primaries cover all slots")
		Eventually(func() (string, error) { return clusterCheck() }, 2*time.Minute, 5*time.Second).
			Should(And(ContainSubstring("All 16384 slots covered"), ContainSubstring("3 primaries")))

		By("departed shard StatefulSets are torn down (only shards 0-2 remain)")
		Eventually(func() (string, error) {
			return kubectl("get", "statefulset", "-l", "app.kubernetes.io/instance="+name,
				"-o", "jsonpath={.items[*].metadata.name}")
		}, 3*time.Minute, 10*time.Second).Should(And(
			ContainSubstring(name+"-shard-2"),
			Not(ContainSubstring(name+"-shard-3")),
			Not(ContainSubstring(name+"-shard-4")),
		))

		By("every key from both batches survives the scale-in")
		Eventually(readAllOK(60), 2*time.Minute, 5*time.Second).Should(Succeed())
	})
})
