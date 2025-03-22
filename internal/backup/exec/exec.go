package exec

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// Execute command in a Kubernets pod and store stdout as a file on disk
func ExecuteCommandInPod(ctx context.Context, kubeClient kubernetes.Interface, kubeConfig *rest.Config, podName, namespace, command, cacheDir string) (string, error) {
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
		return "", fmt.Errorf("Failed to create executor: %w", err)
	}

	// Create a cache file to store the command output
	stdOutFilePath := filepath.Join(cacheDir, fmt.Sprintf("%s.stdout", podName))
	stdOutFile, err := newCachedFile(stdOutFilePath)
	if err != nil {
		return "", err
	}
	defer stdOutFile.close()

	var stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: stdOutFile.bufferedCacheFile,
		Stderr: &stderr,
		Tty:    false,
	})
	if err != nil {
		log.Error("Failed to stream command result", "error", err)
		return "", fmt.Errorf("Failed to stream command result: %w", err)
	}

	if stderr.Len() > 0 {
		log.Warn("Command returned stderr", "stderr", stderr.String())
	}

	log.Info("Command executed successfully")
	return stdOutFilePath, nil
}

type cachedFile struct {
	cacheFile         *os.File
	bufferedCacheFile *bufio.Writer
}

func newCachedFile(cacheFilePath string) (*cachedFile, error) {
	log := slog.Default()

	// Create a cache file to store the command output
	cacheFile, err := os.Create(cacheFilePath)
	log.Info("Creating cache file", "file", cacheFilePath)
	if err != nil {
		log.Error("Failed to create cache file", "error", err)
		return nil, fmt.Errorf("Failed to create cache file: %w", err)
	}

	// Create a buffered writer for efficient writing
	bufferedCacheFile := bufio.NewWriter(cacheFile)
	return &cachedFile{
		cacheFile:         cacheFile,
		bufferedCacheFile: bufferedCacheFile,
	}, nil
}

func (c *cachedFile) close() {
	c.bufferedCacheFile.Flush()
	c.cacheFile.Close()
}
