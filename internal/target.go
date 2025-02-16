package internal

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type BackupTarget struct {
	Pod       *corev1.Pod
	Namespace string
	NodeName  string
	Selector  string

	pvc         *corev1.PersistentVolumeClaim
	volumeMount *corev1.VolumeMount
}

// NewBackupTargetFromDeploymentName creates a BackupTarget from a namespace
// and a deployment name. It finds the deployment, then the pod, then the PVC
// and the node name the pod is running on.
func NewBackupTargetFromDeploymentName(ctx context.Context, log *slog.Logger, kubeclient kubernetes.Interface, namespace string, deploymentName string) (*BackupTarget, error) {
	deployment, err := findDeploymentByName(ctx, kubeclient, namespace, deploymentName)
	if err != nil {
		return nil, err
	}

	selector := selectorFromDeployment(deployment)
	if selector == "" {
		return nil, fmt.Errorf("selector not found for deployment %s", deploymentName)
	}

	podList, err := kubeclient.CoreV1().
		Pods(namespace).
		List(ctx, metav1.ListOptions{
			LabelSelector: selector,
		})
	if err != nil {
		return nil, err
	}

	if len(podList.Items) != 1 {
		return nil, fmt.Errorf("expected 1 pod, got %d", len(podList.Items))
	}

	pod := podList.Items[0]

	nodeName := pod.Spec.NodeName
	if nodeName == "" {
		return nil, fmt.Errorf("pod %s has no node", pod.Name)
	}

	target := &BackupTarget{
		Pod:       &pod,
		Namespace: namespace,
		NodeName:  nodeName,
		Selector:  selector,
	}

	return target, nil
}

// findPvcAndVolume iterates through all volumes and determines a PVC as backup
// target. It is an error if there is no PVC. It is also an error if there is
// more than one PVC. Also looks up the volume mount within the container.
func (t *BackupTarget) findPvcAndVolume(ctx context.Context, log *slog.Logger, kubeclient kubernetes.Interface) error {
	if t.pvc == nil || t.volumeMount == nil {
		pvcName := ""
		volumeName := ""
		for _, volume := range t.Pod.Spec.Volumes {
			if volume.PersistentVolumeClaim == nil {
				continue
			}
			if pvcName != "" {
				return fmt.Errorf("more than one PVC found")
			}

			log.Info("Found PVC", "volumeName", volume.Name)
			pvcName = volume.PersistentVolumeClaim.ClaimName
			volumeName = volume.Name
		}
		if pvcName == "" {
			return fmt.Errorf("no PVC found")
		}

		if volumeName == "" {
			return fmt.Errorf("volume name is empty, should never happen")
		}

		// Find the mount path within the first container of the pod that matches
		// the volume name
	outer:
		for _, container := range t.Pod.Spec.Containers {
			for _, volumeMount := range container.VolumeMounts {
				if volumeMount.Name == volumeName {
					t.volumeMount = &volumeMount
					break outer
				}
			}
		}

		if t.volumeMount == nil {
			return fmt.Errorf("volume mount not found")
		}

		// Look up the PVC
		pvc, err := kubeclient.CoreV1().
			PersistentVolumeClaims(t.Pod.Namespace).
			Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("error looking up PVC %s: %w", pvcName, err)
		}

		t.pvc = pvc
	}

	return nil
}

func (t *BackupTarget) FindPvc(ctx context.Context, log *slog.Logger, kubeclient kubernetes.Interface) (*corev1.PersistentVolumeClaim, error) {
	err := t.findPvcAndVolume(ctx, log, kubeclient)
	if err != nil {
		return nil, err
	}

	return t.pvc, nil
}

func (t *BackupTarget) FindVolumeMount(ctx context.Context, log *slog.Logger, kubeclient kubernetes.Interface) (*corev1.VolumeMount, error) {
	err := t.findPvcAndVolume(ctx, log, kubeclient)
	if err != nil {
		return nil, err
	}

	return t.volumeMount, nil
}

// selectorFromDeployment returns a label selector for a deployment
func selectorFromDeployment(deployment *appsv1.Deployment) string {
	matchLabels := deployment.Spec.Selector.MatchLabels
	// create selector from matchLabels
	selectors := make([]string, 0, len(matchLabels))
	for k, v := range matchLabels {
		selectors = append(selectors, k+"="+v)
	}
	selector := fmt.Sprintf("%s", strings.Join(selectors, ","))
	return selector
}

// selectorFromStatefulSet returns a label selector for a statefulset
func selectorFromStatefulSet(statefulSet *appsv1.StatefulSet) string {
	matchLabels := statefulSet.Spec.Selector.MatchLabels
	// create selector from matchLabels
	selectors := make([]string, 0, len(matchLabels))
	for k, v := range matchLabels {
		selectors = append(selectors, k+"="+v)
	}
	selector := fmt.Sprintf("%s", strings.Join(selectors, ","))
	return selector
}

func findDeploymentByName(ctx context.Context, kubeclient kubernetes.Interface, namespace string, name string) (*appsv1.Deployment, error) {
	deployment, err := kubeclient.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return deployment, nil
}

func findStatefulSetByName(ctx context.Context, kubeclient kubernetes.Interface, namespace string, name string) (*appsv1.StatefulSet, error) {
	statefulSet, err := kubeclient.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return statefulSet, nil
}
