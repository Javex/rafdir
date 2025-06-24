package internal_test

import (
	"context"
	"log/slog"
	"rafdir/internal"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func newTestLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(&testWriter{t}, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

type testWriter struct {
	t *testing.T
}

func (w *testWriter) Write(p []byte) (n int, err error) {
	w.t.Log(string(p))
	return len(p), nil
}

func TestNewBackupTargetFromDeploymentName(t *testing.T) {
	type targetExpectation struct {
		PodName   string
		Namespace string
		Selector  string
	}
	tcs := []struct {
		name           string
		deploymentName string
		deployment     *appsv1.Deployment
		pods           []*corev1.Pod
		pvcs           []*corev1.PersistentVolumeClaim
		expTarget      *targetExpectation
		expErrContains string
	}{
		{
			"success",
			"testDeployment",
			&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "testDeployment",
					Namespace: "test",
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"testKeySelector": "testValueSelector",
						},
					},
				},
			},
			[]*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "testPod",
						Namespace: "test",
						Labels: map[string]string{
							"testKeySelector": "testValueSelector",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "testNode",
						Volumes: []corev1.Volume{
							{
								Name: "testPvc",
								VolumeSource: corev1.VolumeSource{
									PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
										ClaimName: "testPvc",
									},
								},
							},
						},
					},
				},
			},
			[]*corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "testPvc",
						Namespace: "test",
					},
				},
			},
			&targetExpectation{
				PodName:   "testPod",
				Namespace: "test",
				Selector:  "testKeySelector=testValueSelector",
			},
			"",
		},
		{
			"deploymentNotFound",
			"testDeployment",
			nil,
			nil,
			nil,
			nil,
			`"testDeployment" not found`,
		},
		{
			"selectorEmpty",
			"testDeployment",
			&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "testDeployment",
					Namespace: "test",
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{},
					},
				},
			},
			nil,
			nil,
			nil,
			"selector not found",
		},
		{
			"multiplePods",
			"testDeployment",
			&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "testDeployment",
					Namespace: "test",
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"testKeySelector": "testValueSelector",
						},
					},
				},
			},
			[]*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "testPod-1",
						Namespace: "test",
						Labels: map[string]string{
							"testKeySelector": "testValueSelector",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "testNode",
						Volumes: []corev1.Volume{
							{
								Name: "testPvc",
								VolumeSource: corev1.VolumeSource{
									PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
										ClaimName: "testPvc",
									},
								},
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "testPod-2",
						Namespace: "test",
						Labels: map[string]string{
							"testKeySelector": "testValueSelector",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "testNode",
						Volumes: []corev1.Volume{
							{
								Name: "testPvc",
								VolumeSource: corev1.VolumeSource{
									PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
										ClaimName: "testPvc",
									},
								},
							},
						},
					},
				},
			},
			nil,
			nil,
			"expected 1 pod, got 2",
		},
		{
			"noPods",
			"testDeployment",
			&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "testDeployment",
					Namespace: "test",
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"testKeySelector": "testValueSelector",
						},
					},
				},
			},
			[]*corev1.Pod{},
			nil,
			nil,
			"expected 1 pod, got 0",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			kubeclient := fake.NewSimpleClientset()
			namespace := "test"
			if tc.deployment != nil {
				_, err := kubeclient.
					AppsV1().
					Deployments(namespace).
					Create(ctx, tc.deployment, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("failed to create deployment: %v", err)
				}
			}

			if tc.pods != nil {
				for _, pod := range tc.pods {
					_, err := kubeclient.
						CoreV1().
						Pods(namespace).
						Create(ctx, pod, metav1.CreateOptions{})
					if err != nil {
						t.Fatalf("failed to create pod: %v", err)
					}
				}
			}

			if tc.pvcs != nil {
				for _, pvc := range tc.pvcs {
					_, err := kubeclient.
						CoreV1().
						PersistentVolumeClaims(namespace).
						Create(ctx, pvc, metav1.CreateOptions{})
					if err != nil {
						t.Fatalf("failed to create pvc: %v", err)
					}
				}
			}

			log := newTestLogger(t)
			profile := &internal.Profile{
				Deployment: tc.deploymentName,
				Namespace:  namespace,
			}
			runSuffix := "test"
			target, err := internal.NewBackupTargetFromDeploymentName(ctx, log, kubeclient, profile, runSuffix)
			if tc.expErrContains != "" {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tc.expErrContains) {
					t.Errorf("expected error to contain %q, got %v", tc.expErrContains, err)
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}

				resTarget := &targetExpectation{
					PodName:   target.Pod.Name,
					Namespace: target.Namespace,
					Selector:  target.Selector,
				}

				if !cmp.Equal(resTarget, tc.expTarget) {
					t.Errorf("unexpected difference in targets: %v", cmp.Diff(resTarget, tc.expTarget))
				}
			}
		})
	}
}

func TestGetFolderToPvcMapping(t *testing.T) {
	tcs := []struct {
		name            string
		deploymentName  string
		deployment      *appsv1.Deployment
		pods            []*corev1.Pod
		pvcs            []*corev1.PersistentVolumeClaim
		expVolumeMounts map[string]*corev1.VolumeMount
		expPvcObjects   map[string]*corev1.PersistentVolumeClaim
		expErrContains  string
	}{
		{
			"singleFolder",
			"testDeployment",
			&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "testDeployment",
					Namespace: "test",
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"testKeySelector": "testValueSelector",
						},
					},
				},
			},
			[]*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "testPod",
						Namespace: "test",
						Labels: map[string]string{
							"testKeySelector": "testValueSelector",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "testNode",
						Containers: []corev1.Container{
							{
								Name: "testContainer",
								VolumeMounts: []corev1.VolumeMount{
									{
										Name:      "testPvc",
										MountPath: "/test/mount/path",
									},
								},
							},
						},
						Volumes: []corev1.Volume{
							{
								Name: "testPvc",
								VolumeSource: corev1.VolumeSource{
									PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
										ClaimName: "testPvc",
									},
								},
							},
						},
					},
				},
			},
			[]*corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "testPvc",
						Namespace: "test",
					},
				},
			},
			map[string]*corev1.VolumeMount{
				"/test/mount/path": {
					Name:      "testPvc",
					MountPath: "/test/mount/path",
				},
			},
			map[string]*corev1.PersistentVolumeClaim{
				"/test/mount/path": {
					ObjectMeta: metav1.ObjectMeta{
						Name:      "testPvc",
						Namespace: "test",
					},
				},
			},
			"",
		},
		{
			"multipleFolders",
			"testDeployment",
			&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "testDeployment",
					Namespace: "test",
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"testKeySelector": "testValueSelector",
						},
					},
				},
			},
			[]*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "testPod",
						Namespace: "test",
						Labels: map[string]string{
							"testKeySelector": "testValueSelector",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "testNode",
						Containers: []corev1.Container{
							{
								Name: "testContainer",
								VolumeMounts: []corev1.VolumeMount{
									{
										Name:      "testPvc1",
										MountPath: "/test/mount/path1",
									},
									{
										Name:      "testPvc2",
										MountPath: "/test/mount/path2",
									},
								},
							},
						},
						Volumes: []corev1.Volume{
							{
								Name: "testPvc1",
								VolumeSource: corev1.VolumeSource{
									PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
										ClaimName: "testPvc1",
									},
								},
							},
							{
								Name: "testPvc2",
								VolumeSource: corev1.VolumeSource{
									PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
										ClaimName: "testPvc2",
									},
								},
							},
						},
					},
				},
			},
			[]*corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "testPvc1",
						Namespace: "test",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "testPvc2",
						Namespace: "test",
					},
				},
			},
			map[string]*corev1.VolumeMount{
				"/test/mount/path1": {
					Name:      "testPvc1",
					MountPath: "/test/mount/path1",
				},
				"/test/mount/path2": {
					Name:      "testPvc2",
					MountPath: "/test/mount/path2",
				},
			},
			map[string]*corev1.PersistentVolumeClaim{
				"/test/mount/path1": {
					ObjectMeta: metav1.ObjectMeta{
						Name:      "testPvc1",
						Namespace: "test",
					},
				},
				"/test/mount/path2": {
					ObjectMeta: metav1.ObjectMeta{
						Name:      "testPvc2",
						Namespace: "test",
					},
				},
			},
			"",
		},
		{
			"noPvc",
			"testDeployment",
			&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "testDeployment",
					Namespace: "test",
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"testKeySelector": "testValueSelector",
						},
					},
				},
			},
			[]*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "testPod",
						Namespace: "test",
						Labels: map[string]string{
							"testKeySelector": "testValueSelector",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "testNode",
						Volumes: []corev1.Volume{
							{
								Name: "testEmptyDir",
								VolumeSource: corev1.VolumeSource{
									EmptyDir: &corev1.EmptyDirVolumeSource{},
								},
							},
						},
					},
				},
			},
			nil,
			nil,
			nil,
			"no PVC found",
		},
		{
			"PvcMissing",
			"testDeployment",
			&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "testDeployment",
					Namespace: "test",
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"testKeySelector": "testValueSelector",
						},
					},
				},
			},
			[]*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "testPod",
						Namespace: "test",
						Labels: map[string]string{
							"testKeySelector": "testValueSelector",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "testNode",
						Containers: []corev1.Container{
							{
								Name: "testContainer",
								VolumeMounts: []corev1.VolumeMount{
									{
										Name:      "testPvc",
										MountPath: "/test/mount/path",
									},
								},
							},
						},
						Volumes: []corev1.Volume{
							{
								Name: "testPvc",
								VolumeSource: corev1.VolumeSource{
									PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
										ClaimName: "testPvc",
									},
								},
							},
						},
					},
				},
			},
			nil,
			nil,
			nil,
			"error looking up PVC testPvc",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			kubeclient := fake.NewSimpleClientset()
			namespace := "test"
			if tc.deployment != nil {
				_, err := kubeclient.
					AppsV1().
					Deployments(namespace).
					Create(ctx, tc.deployment, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("failed to create deployment: %v", err)
				}
			}

			if tc.pods != nil {
				for _, pod := range tc.pods {
					_, err := kubeclient.
						CoreV1().
						Pods(namespace).
						Create(ctx, pod, metav1.CreateOptions{})
					if err != nil {
						t.Fatalf("failed to create pod: %v", err)
					}
				}
			}

			if tc.pvcs != nil {
				for _, pvc := range tc.pvcs {
					_, err := kubeclient.
						CoreV1().
						PersistentVolumeClaims(namespace).
						Create(ctx, pvc, metav1.CreateOptions{})
					if err != nil {
						t.Fatalf("failed to create pvc: %v", err)
					}
				}
			}

			log := newTestLogger(t)
			profile := &internal.Profile{
				Deployment: tc.deploymentName,
				Namespace:  namespace,
			}
			runSuffix := "test"
			target, err := internal.NewBackupTargetFromDeploymentName(ctx, log, kubeclient, profile, runSuffix)
			if err != nil {
				t.Fatalf("failed to create target: %v", err)
			}

			volumeMounts, pvcObjects, err := target.GetFolderToPvcMapping(ctx, log, kubeclient)

			if tc.expErrContains != "" {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tc.expErrContains) {
					t.Errorf("expected error to contain %q, got %v", tc.expErrContains, err)
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}

				if !cmp.Equal(volumeMounts, tc.expVolumeMounts) {
					t.Errorf("unexpected difference in volume mounts: %v", cmp.Diff(volumeMounts, tc.expVolumeMounts))
				}

				if !cmp.Equal(pvcObjects, tc.expPvcObjects) {
					t.Errorf("unexpected difference in pvc objects: %v", cmp.Diff(pvcObjects, tc.expPvcObjects))
				}
			}
		})
	}
}
