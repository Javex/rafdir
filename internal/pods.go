package internal

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// DetermineNodes figures out which node each pod lives on and assigns it
// accordingly
func DetermineNodes(ctx context.Context, kubeclient kubernetes.Interface, namespace string, selector string) (map[string][]corev1.Pod, error) {
	podList, err := kubeclient.CoreV1().
		Pods(namespace).
		List(ctx, metav1.ListOptions{
			LabelSelector: selector,
		})
	if err != nil {
		return nil, err
	}

	selectedPods := make(map[string][]corev1.Pod)
	for _, pod := range podList.Items {
		nodeName := pod.Spec.NodeName
		if nodeName == "" {
			continue
		}
		podsOnNode, ok := selectedPods[nodeName]
		if !ok {
			podsOnNode = make([]corev1.Pod, 0)
		}
		selectedPods[nodeName] = append(podsOnNode, pod)
	}
	return selectedPods, nil
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
