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
	"k8s.io/client-go/rest"
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
			config := &rest.Config{}
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
			target, err := internal.NewBackupTargetFromDeploymentName(ctx, log, kubeclient, config, profile, runSuffix)
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

func TestGetFolderToVolumeMapping(t *testing.T) {
	tcs := []struct {
		name           string
		deploymentName string
		deployment     *appsv1.Deployment
		pods           []*corev1.Pod
		pvcs           []*corev1.PersistentVolumeClaim
		expVolumeInfos map[string]*internal.VolumeInfo
		expErrContains string
	}{
		{
			"singlePvcFolder",
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
			map[string]*internal.VolumeInfo{
				"/test/mount/path": {
					Type: internal.VolumeTypePVC,
					VolumeMount: &corev1.VolumeMount{
						Name:      "testPvc",
						MountPath: "/test/mount/path",
					},
					PVC: &corev1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "testPvc",
							Namespace: "test",
						},
					},
				},
			},
			"",
		},
		{
			"singleNfsFolder",
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
										Name:      "testNfs",
										MountPath: "/test/nfs/path",
									},
								},
							},
						},
						Volumes: []corev1.Volume{
							{
								Name: "testNfs",
								VolumeSource: corev1.VolumeSource{
									NFS: &corev1.NFSVolumeSource{
										Server: "nfs.example.com",
										Path:   "/shared/data",
									},
								},
							},
						},
					},
				},
			},
			nil,
			map[string]*internal.VolumeInfo{
				"/test/nfs/path": {
					Type: internal.VolumeTypeNFS,
					VolumeMount: &corev1.VolumeMount{
						Name:      "testNfs",
						MountPath: "/test/nfs/path",
					},
					NFS: &corev1.NFSVolumeSource{
						Server: "nfs.example.com",
						Path:   "/shared/data",
					},
				},
			},
			"",
		},
		{
			"mixedPvcAndNfs",
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
										MountPath: "/test/pvc/path",
									},
									{
										Name:      "testNfs",
										MountPath: "/test/nfs/path",
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
							{
								Name: "testNfs",
								VolumeSource: corev1.VolumeSource{
									NFS: &corev1.NFSVolumeSource{
										Server: "nfs.example.com",
										Path:   "/shared/data",
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
			map[string]*internal.VolumeInfo{
				"/test/pvc/path": {
					Type: internal.VolumeTypePVC,
					VolumeMount: &corev1.VolumeMount{
						Name:      "testPvc",
						MountPath: "/test/pvc/path",
					},
					PVC: &corev1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "testPvc",
							Namespace: "test",
						},
					},
				},
				"/test/nfs/path": {
					Type: internal.VolumeTypeNFS,
					VolumeMount: &corev1.VolumeMount{
						Name:      "testNfs",
						MountPath: "/test/nfs/path",
					},
					NFS: &corev1.NFSVolumeSource{
						Server: "nfs.example.com",
						Path:   "/shared/data",
					},
				},
			},
			"",
		},
		{
			"noVolumes",
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
							},
						},
						Volumes: []corev1.Volume{},
					},
				},
			},
			nil,
			nil,
			"no volumes found",
		},
		{
			"pvcLookupError",
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
			"error looking up PVC testPvc",
		},
		{
			"mutipleSubPathFolders",
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
										MountPath: "/test/mount/path/data",
										SubPath:   "data",
									},
									{
										Name:      "testPvc",
										MountPath: "/test/mount/path/config",
										SubPath:   "config",
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
			map[string]*internal.VolumeInfo{
				"/test/mount/path/config": {
					Type: internal.VolumeTypePVC,
					VolumeMount: &corev1.VolumeMount{
						Name:      "testPvc",
						MountPath: "/test/mount/path/config",
						SubPath:   "config",
					},
					PVC: &corev1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "testPvc",
							Namespace: "test",
						},
					},
				},
				"/test/mount/path/data": {
					Type: internal.VolumeTypePVC,
					VolumeMount: &corev1.VolumeMount{
						Name:      "testPvc",
						MountPath: "/test/mount/path/data",
						SubPath:   "data",
					},
					PVC: &corev1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "testPvc",
							Namespace: "test",
						},
					},
				},
			},
			"",
		},
		{
			"overlappingFolders",
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
									{
										Name:      "testNfs",
										MountPath: "/test/mount/path/data",
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
							{
								Name: "testNfs",
								VolumeSource: corev1.VolumeSource{
									NFS: &corev1.NFSVolumeSource{
										Server: "nfs.example.com",
										Path:   "/shared/data",
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
			map[string]*internal.VolumeInfo{
				"/test/mount/path": {
					Type: internal.VolumeTypePVC,
					VolumeMount: &corev1.VolumeMount{
						Name:      "testPvc",
						MountPath: "/test/mount/path",
					},
					PVC: &corev1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "testPvc",
							Namespace: "test",
						},
					},
				},
				"/test/mount/path/data": {
					Type: internal.VolumeTypeNFS,
					VolumeMount: &corev1.VolumeMount{
						Name:      "testNfs",
						MountPath: "/test/mount/path/data",
					},
					NFS: &corev1.NFSVolumeSource{
						Server: "nfs.example.com",
						Path:   "/shared/data",
					},
				},
			},
			"",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			kubeclient := fake.NewSimpleClientset()
			config := &rest.Config{}
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
			target, err := internal.NewBackupTargetFromDeploymentName(ctx, log, kubeclient, config, profile, runSuffix)
			if err != nil {
				t.Fatalf("failed to create target: %v", err)
			}

			volumeInfos, err := target.GetFolderToVolumeMapping(ctx, log, kubeclient)

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

				if !cmp.Equal(volumeInfos, tc.expVolumeInfos) {
					t.Errorf("unexpected difference in volume infos: %v", cmp.Diff(volumeInfos, tc.expVolumeInfos))
				}
			}
		})
	}
}
