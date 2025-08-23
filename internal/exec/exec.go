package exec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"rafdir/internal/client"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

type CommandExecutor struct {
	kubeConfig *rest.Config
	kubeClient kubernetes.Interface
	log        *slog.Logger
}

func NewCommandExecutor(kubeConfig *rest.Config, log *slog.Logger) (*CommandExecutor, error) {
	if kubeConfig == nil {
		return nil, fmt.Errorf("kubeConfig is nil, must be set")
	}

	kubeClient, err := client.InitK8sClient(kubeConfig)
	if err != nil {
		return nil, err
	}
	cmd := &CommandExecutor{
		kubeConfig: kubeConfig,
		kubeClient: kubeClient,
		log:        log,
	}
	return cmd, nil
}

// ExecuteWithStdOut executes a command in a pod and writes stdout to
// stdOutWriter and logs warnings for any stderr messages, otherwise it behaves
// like ExecuteCommandInPod
func (c *CommandExecutor) ExecuteWithStdOut(ctx context.Context, podName, namespace, command string, stdOutWriter io.Writer) error {
	log := c.log.With("pod", podName, "namespace", namespace, "command", command)
	var stderr bytes.Buffer

	// empty container means let Kubernetes decide
	container := ""
	err := c.ExecuteCommandInPod(ctx, podName, namespace, command, container, stdOutWriter, &stderr)
	if err != nil {
		log.Error("Command execution failed", "stderr", stderr.String())
		// Message from ExecuteCommandInPod is sufficient
		return err
	}

	if stderr.Len() > 0 {
		log.Warn("Command returned stderr", "stderr", stderr.String())
	}
	return nil
}

// ExecuteCommandInPod executes the command in a Kubernetes pod and write
// stdout to stdOutWriter and stderr to stdErrWriter
func (c *CommandExecutor) ExecuteCommandInPod(ctx context.Context, podName, namespace, command, container string, stdOutWriter io.Writer, stdErrWriter io.Writer) error {
	log := c.log.With("pod", podName, "namespace", namespace, "command", command)
	req := c.kubeClient.
		CoreV1().
		RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Command: []string{"sh", "-c", command},
			// If container is empty string, Kubernetes will pick a default
			Container: container,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	log.Info("Executing command in pod")
	exec, err := remotecommand.NewSPDYExecutor(c.kubeConfig, "POST", req.URL())
	if err != nil {
		log.Error("Failed to create executor", "error", err)
		return fmt.Errorf("Failed to create executor: %w", err)
	}

	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: stdOutWriter,
		Stderr: stdErrWriter,
		Tty:    false,
	})
	if err != nil {
		log.Error("Failed to stream command result", "error", err)
		return fmt.Errorf("Failed to stream command result: %w", err)
	}

	log.Info("Command executed successfully")
	return nil
}
