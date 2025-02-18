package exec

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// Execute command in a Kubernets pod and store stdout in a variable in memory
func ExecuteCommandInPod(ctx context.Context, kubeClient kubernetes.Interface, kubeConfig *rest.Config, podName, namespace, command string) (*bytes.Buffer, error) {
	log := slog.Default().With("pod", podName, "namespace", namespace, "command", command)
	req := kubeClient.
		CoreV1().
		RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Command: []string{"sh", "-c", command},
			Stdout:  true,
			Stderr:  true,
		}, scheme.ParameterCodec)

	log.Info("Executing command in pod")
	exec, err := remotecommand.NewSPDYExecutor(kubeConfig, "POST", req.URL())
	if err != nil {
		log.Error("Failed to create executor", "error", err)
		return nil, fmt.Errorf("Failed to create executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
		Tty:    false,
	})
	if err != nil {
		log.Error("Failed to stream command result", "error", err)
		return nil, fmt.Errorf("Failed to stream command result: %w", err)
	}

	if stderr.Len() > 0 {
		log.Warn("Command returned stderr", "stderr", stderr.String())
	}

	log.Info("Command executed successfully")
	return &stdout, nil
}
