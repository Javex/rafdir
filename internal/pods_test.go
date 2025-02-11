package internal_test

import (
	"context"
	"resticprofilek8s/internal"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDetermineNodes(t *testing.T) {
	tcs := []struct {
		name       string
		pods       []corev1.Pod
		selector   string
		expNodeMap map[string][]corev1.Pod
	}{
		{
			name: "test1",
			pods: []corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod",
						Namespace: "test-namespace",
						Labels: map[string]string{
							"app": "test-app",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "test-node",
					},
				},
			},
			selector: "app=test-app",
			expNodeMap: map[string][]corev1.Pod{
				"test-node": {
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-pod",
							Namespace: "test-namespace",
							Labels: map[string]string{
								"app": "test-app",
							},
						},
						Spec: corev1.PodSpec{
							NodeName: "test-node",
						},
					},
				},
			},
		},
		{
			name: "testMultiplePodsOnMultipleNodes",
			pods: []corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod1",
						Namespace: "test-namespace",
						Labels: map[string]string{
							"app": "test-app",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "test-node",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod2",
						Namespace: "test-namespace",
						Labels: map[string]string{
							"app": "test-app",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "test-node2",
					},
				},
			},
			selector: "app=test-app",
			expNodeMap: map[string][]corev1.Pod{
				"test-node": {
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-pod1",
							Namespace: "test-namespace",
							Labels: map[string]string{
								"app": "test-app",
							},
						},
						Spec: corev1.PodSpec{
							NodeName: "test-node",
						},
					},
				},
				"test-node2": {
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-pod2",
							Namespace: "test-namespace",
							Labels: map[string]string{
								"app": "test-app",
							},
						},
						Spec: corev1.PodSpec{
							NodeName: "test-node2",
						},
					},
				},
			},
		},
		{
			name: "testOnePodWithoutNode",
			pods: []corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod",
						Namespace: "test-namespace",
						Labels: map[string]string{
							"app": "test-app",
						},
					},
				},
			},
			selector:   "app=test-app",
			expNodeMap: map[string][]corev1.Pod{},
		},
		{
			name: "testOnePodWithDifferentLabel",
			pods: []corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod",
						Namespace: "test-namespace",
						Labels: map[string]string{
							"app": "test-app",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "test-node",
					},
				},
			},
			selector:   "app=test-different-app",
			expNodeMap: map[string][]corev1.Pod{},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			// Create a fake client
			fakeClient := fake.NewClientset()
			for _, pod := range tc.pods {
				_, err := fakeClient.CoreV1().Pods("test-namespace").Create(context.Background(), &pod, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("Error creating pod: %v", err)
				}
			}
			// Call DetermineNodes
			nodes, err := internal.DetermineNodes(context.Background(), fakeClient, "test-namespace", tc.selector)
			if err != nil {
				t.Fatalf("Error calling DetermineNodes: %v", err)
			}

			// Check if the returned map is equal to the expected map
			if len(nodes) != len(tc.expNodeMap) {
				t.Fatalf("Expected %d nodes, got %d", len(tc.expNodeMap), len(nodes))
			}
			for nodeName, expPods := range tc.expNodeMap {
				if _, ok := nodes[nodeName]; !ok {
					t.Fatalf("Expected node %s not found", nodeName)
				}
				if len(nodes[nodeName]) != len(expPods) {
					t.Fatalf("Expected %d pods on node %s, got %d", len(expPods), nodeName, len(nodes[nodeName]))
				}
				for i, expPod := range expPods {
					if nodes[nodeName][i].Name != expPod.Name {
						t.Fatalf("Expected pod %s on node %s, got %s", expPod.Name, nodeName, nodes[nodeName][i].Name)
					}
				}
			}
		})
	}
}

func TestSelectorFromDeployment(t *testing.T) {
	tcs := []struct {
		name        string
		deployment  appsv1.Deployment
		expSelector string
	}{
		{
			name: "test1",
			deployment: appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app": "test-app",
						},
					},
				},
			},
			expSelector: "app=test-app",
		},
		{
			name: "testMultipleLabels",
			deployment: appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app": "test-app",
							"env": "test-env",
						},
					},
				},
			},
			expSelector: "app=test-app,env=test-env",
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			selector := internal.SelectorFromDeployment(&tc.deployment)
			if selector != tc.expSelector {
				t.Fatalf("Expected selector %s, got %s", tc.expSelector, selector)
			}
		})
	}

}
