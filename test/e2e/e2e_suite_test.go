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
	"testing"

	"github.com/razkevich/valkeycluster-operator/test/utils"
)

// managerImage is the operator image built and loaded into Kind for the run.
const managerImage = "example.com/valkeycluster:v0.0.1"

// certManagerInstalled records whether this run installed cert-manager (so teardown
// only removes what it added).
var certManagerInstalled bool

// TestMain builds and loads the operator image into the (already-running) Kind
// cluster, optionally installs cert-manager, runs the suite, then cleans up.
// The Kind cluster itself is managed by the Makefile's setup/cleanup-test-e2e
// targets; set KEEP_CLUSTER=true to leave the deployed ValkeyCluster in place for
// inspection after the run (and Makefile skips the Kind teardown).
//
// kubectl kuberc is disabled by default for isolation (override KUBECTL_KUBERC=true).
// cert-manager is installed unless CERT_MANAGER_INSTALL_SKIP=true; the operator has
// no webhooks, so skipping it is safe and faster.
func TestMain(m *testing.M) {
	if err := setupSuite(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e setup: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	teardownSuite()
	os.Exit(code)
}

func setupSuite() error {
	// Safety first: this suite deploys the operator and deletes pods, so it must
	// target the Kind cluster — never a real cluster like EKS. Guard before the
	// slow image build so a wrong context fails fast.
	if err := ensureKindContext(); err != nil {
		return err
	}

	if _, err := utils.Run(exec.Command("make", "docker-build", "IMG="+managerImage)); err != nil {
		return fmt.Errorf("build manager image: %w", err)
	}
	if err := utils.LoadImageToKindClusterWithName(managerImage); err != nil {
		return fmt.Errorf("load manager image into Kind: %w", err)
	}

	if os.Getenv("KUBECTL_KUBERC") != "true" {
		if err := os.Setenv("KUBECTL_KUBERC", "false"); err != nil {
			return fmt.Errorf("disable kubectl kuberc: %w", err)
		}
	}

	if os.Getenv("CERT_MANAGER_INSTALL_SKIP") == "true" {
		return nil
	}
	if utils.IsCertManagerCRDsInstalled() {
		return nil
	}
	certManagerInstalled = true
	if err := utils.InstallCertManager(); err != nil {
		return fmt.Errorf("install cert-manager: %w", err)
	}
	return nil
}

func teardownSuite() {
	if certManagerInstalled {
		utils.UninstallCertManager()
	}
}

// kindClusterName is the Kind cluster the suite targets (KIND_CLUSTER, default "kind").
func kindClusterName() string {
	if v := os.Getenv("KIND_CLUSTER"); v != "" {
		return v
	}
	return "kind"
}

// ensureKindContext refuses to run e2e against anything but the Kind cluster — the
// suite deploys the operator and deletes pods, so pointing it at a real cluster (e.g.
// EKS) would be destructive. With E2E_USE_KIND_CONTEXT=true it switches to the Kind
// context automatically (always safe, since the target is Kind); otherwise it aborts
// with instructions when the current context doesn't match.
func ensureKindContext() error {
	want := "kind-" + kindClusterName()

	if os.Getenv("E2E_USE_KIND_CONTEXT") == "true" {
		if _, err := kubectl("config", "use-context", want); err != nil {
			return fmt.Errorf("switch to kube context %q: %w", want, err)
		}
		return nil
	}

	cur, err := kubectl("config", "current-context")
	if err != nil {
		return fmt.Errorf("read current kube context: %w", err)
	}
	if cur != want {
		return fmt.Errorf("refusing to run e2e: current kube context is %q, expected %q "+
			"(the suite deploys the operator and deletes pods, so it must target Kind). "+
			"Run `kubectl config use-context %s`, or set E2E_USE_KIND_CONTEXT=true", cur, want, want)
	}
	return nil
}
