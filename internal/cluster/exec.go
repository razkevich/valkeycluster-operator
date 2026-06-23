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
	"bytes"
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// PodExec runs commands in pods via the Kubernetes pod-exec subresource.
type PodExec struct {
	cfg       *rest.Config
	clientset kubernetes.Interface
}

var _ PodExecutor = (*PodExec)(nil)

// NewPodExec builds a PodExec from a controller-runtime rest.Config.
func NewPodExec(cfg *rest.Config) (*PodExec, error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &PodExec{cfg: cfg, clientset: cs}, nil
}

// Exec runs command in the pod/container and returns combined stdout, erroring
// (with stderr included) on a non-zero exit.
func (p *PodExec) Exec(ctx context.Context, namespace, pod, container string, command []string) (string, error) {
	req := p.clientset.CoreV1().RESTClient().Post().
		Resource("pods").Name(pod).Namespace(namespace).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(p.cfg, "POST", req.URL())
	if err != nil {
		return "", err
	}
	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr})
	if err != nil {
		return stdout.String(), fmt.Errorf("exec %v in %s/%s: %w: %s", command, namespace, pod, err, stderr.String())
	}
	return stdout.String(), nil
}
