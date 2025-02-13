package internal

import (
	"context"
	"fmt"
	"log/slog"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type BackupTarget struct {
	PodName   string
	Namespace string
	Pvc       *corev1.PersistentVolumeClaim
	NodeName  string
}

// DetermineNodes figures out which node each pod lives on and assigns it
// accordingly
func NewBackupTargetFromDeploymentName(ctx context.Context, log *slog.Logger, kubeclient kubernetes.Interface, namespace string, deploymentName string) (*BackupTarget, error) {
	deployment, err := FindDeploymentByName(ctx, kubeclient, namespace, deploymentName)
	if err != nil {
		return nil, err
	}

	selector := SelectorFromDeployment(deployment)

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

	pvc, err := PvcFromPod(ctx, log, kubeclient, pod)
	if err != nil {
		return nil, err
	}

	target := &BackupTarget{
		PodName:   pod.Name,
		Namespace: namespace,
		Pvc:       pvc,
		NodeName:  nodeName,
	}

	return target, nil
}

// PvcFromPod iterates through all volumes and determines a PVC as backup
// target. It is an error if there is no PVC. It is also an error if there is
// more than one PVC.
func PvcFromPod(ctx context.Context, log *slog.Logger, kubeclient kubernetes.Interface, pod corev1.Pod) (*corev1.PersistentVolumeClaim, error) {
	pvcName := ""
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		if pvcName != "" {
			return nil, fmt.Errorf("more than one PVC found")
		}

		log.Info("Found PVC", "volumeName", volume.Name)
		pvcName = volume.PersistentVolumeClaim.ClaimName
	}
	if pvcName == "" {
		return nil, fmt.Errorf("no PVC found")
	}

	//
	pvc, err := kubeclient.CoreV1().
		PersistentVolumeClaims(pod.Namespace).
		Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("error looking up PVC %s: %w", pvcName, err)
	}

	return pvc, nil
}

// SelectorFromDeployment returns a label selector for a deployment
func SelectorFromDeployment(deployment *appsv1.Deployment) string {
	matchLabels := deployment.Spec.Selector.MatchLabels
	// create selector from matchLabels
	selector := ""
	for k, v := range matchLabels {
		selector += k + "=" + v + ","
	}
	return selector[:len(selector)-1]
}

func FindDeploymentByName(ctx context.Context, kubeclient kubernetes.Interface, namespace string, name string) (*appsv1.Deployment, error) {
	deployment, err := kubeclient.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return deployment, nil
}
