package internal

import (
	"context"
	"fmt"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type NodeBackupTarget struct {
	node   *corev1.Node
	client kubernetes.Interface

	profile *Profile
	podName string
}

func (n *NodeBackupTarget) PodName() string {
	return n.podName
}

func findNodeByName(ctx context.Context, client kubernetes.Interface, nodeName string) (*corev1.Node, error) {
	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return node, nil
}

// AddNodeVolumeToPod adds a host volume from a backup profile to the podspec.
func (n *NodeBackupTarget) AddNodeVolumeToPod(ctx context.Context, pod *corev1.Pod) {
	log := slog.Default()
	if len(n.profile.Folders) == 0 {
		// Should never happen as the constructor checks for this
		log.Error("No folders specified in profile")
		panic("No folders specified in profile")
	}

	pathType := corev1.HostPathDirectory
	path := n.profile.Folders[0]

	log.Debug("Adding host volume to pod", "path", path)

	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: n.profile.Name,
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: path,
				Type: &pathType,
			},
		},
	})

	// Read-only mount of volume into container
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      n.profile.Name,
		MountPath: path,
		ReadOnly:  true,
	})

	// Ensure pod is scheduled on the correct node that has this host volume
	// available
	pod.Spec.NodeName = n.node.Name
}

func NewBackupTargetFromNodeName(ctx context.Context, kubeclient kubernetes.Interface, profile *Profile, runSuffix string) (*NodeBackupTarget, error) {
	nodeName := profile.Node
	namespace := profile.Namespace

	if nodeName == "" {
		return nil, fmt.Errorf("nodeName cannot be empty")
	}

	if namespace != "" {
		return nil, fmt.Errorf("namespace must be empty")
	}

	if len(profile.Folders) == 0 {
		return nil, fmt.Errorf("Folders is required if node is specified for profile %s", profile.Name)
	}

	node, err := findNodeByName(ctx, kubeclient, nodeName)
	if err != nil {
		return nil, err
	}

	return &NodeBackupTarget{
		node:    node,
		client:  kubeclient,
		profile: profile,
		podName: fmt.Sprintf("%s-%s-%s", profile.Name, node.Name, runSuffix),
	}, nil
}
