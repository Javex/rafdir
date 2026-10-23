package pvc_test

import (
	"context"
	"fmt"
	"log/slog"
	"rafdir/internal/pvc"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
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

// boundPvc simulates binding a PVC.
func boundPvc(name, namespace string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}
}

func TestBackupPvcFromSourcePvc(t *testing.T) {
	const (
		sourceNamespace = "test"
		destNamespace   = "backup"
		sourcePvName    = "testpv"
		sourcePvcName   = "testpvc"
		runSuffix       = "run1"
	)

	destName := fmt.Sprintf("%s-%s", sourcePvcName, runSuffix)

	sourcePvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sourcePvcName,
			Namespace: sourceNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			// FindPvForPvc requires a bound PVC pointing at its PV.
			VolumeName: sourcePvName,
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}

	csiSourcePv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: sourcePvName,
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
			MountOptions:     []string{"nfsvers=4.1"},
			StorageClassName: "test-sc",
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "nfs.csi.k8s.io",
					VolumeHandle: sourcePvName,
					VolumeAttributes: map[string]string{
						"server": "10.0.0.1",
						"share":  "/export",
						"subDir": sourcePvcName,
						"fstype": "ext4",
					},
				},
			},
		},
	}

	csiExpectedPvSpec := corev1.PersistentVolumeSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		Capacity: corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse("1Gi"),
		},
		MountOptions:     []string{"nfsvers=4.1"},
		StorageClassName: "test-sc",
		// Always Retain because the clone only exists for the duration of the
		// backup run. If it were deleted, it would delete the source data.
		PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
		PersistentVolumeSource: corev1.PersistentVolumeSource{
			CSI: &corev1.CSIPersistentVolumeSource{
				Driver:       "nfs.csi.k8s.io",
				VolumeHandle: destName,
				// Only the whitelisted attributes are carried over, extra
				// attributes such as "fstype" are dropped.
				VolumeAttributes: map[string]string{
					"server": "10.0.0.1",
					"share":  "/export",
					"subDir": sourcePvcName,
				},
			},
		},
	}

	nfsSourcePv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: sourcePvName,
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
			MountOptions: []string{"nfsvers=4.1", "hard"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				NFS: &corev1.NFSVolumeSource{
					Server:   "10.0.0.1",
					Path:     "/export/data",
					ReadOnly: false,
				},
			},
		},
	}

	nfsExpectedPvSpec := corev1.PersistentVolumeSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		Capacity: corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse("1Gi"),
		},
		MountOptions:                  []string{"nfsvers=4.1", "hard"},
		PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
		PersistentVolumeSource: corev1.PersistentVolumeSource{
			NFS: &corev1.NFSVolumeSource{
				Server: "10.0.0.1",
				Path:   "/export/data",
				// The backup mount is always forced read-only, even when the
				// source PV is writable.
				ReadOnly: true,
			},
		},
	}

	tcs := []struct {
		name           string
		sourcePvc      *corev1.PersistentVolumeClaim
		sourcePv       *corev1.PersistentVolume
		expDestName    string
		expPvSpec      corev1.PersistentVolumeSpec
		expErrContains string
	}{
		{
			name:        "csiSource",
			sourcePvc:   sourcePvc,
			sourcePv:    csiSourcePv,
			expDestName: destName,
			expPvSpec:   csiExpectedPvSpec,
		},
		{
			name:        "nfsSource",
			sourcePvc:   sourcePvc,
			sourcePv:    nfsSourcePv,
			expDestName: destName,
			expPvSpec:   nfsExpectedPvSpec,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			kubeclient := fake.NewSimpleClientset()

			_, err := kubeclient.
				CoreV1().
				PersistentVolumes().
				Create(ctx, tc.sourcePv, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("failed to create source PV: %v", err)
			}

			_, err = kubeclient.
				CoreV1().
				PersistentVolumeClaims(sourceNamespace).
				Create(ctx, tc.sourcePvc, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("failed to create source PVC: %v", err)
			}

			// The fake clientset does not implement the PV/PVC binding logic,
			// so the backup PVC would stay Pending forever. React on GETs for
			// PVCs in the backup namespace to simulate the binding having
			// succeeded. The reactor is disabled before Cleanup so that the
			// cleanup verification below reads the real state from the
			// tracker instead.
			bindingSimulated := true
			kubeclient.PrependReactor("get", "persistentvolumeclaims",
				func(action clienttesting.Action) (bool, runtime.Object, error) {
					if action.GetNamespace() != destNamespace || !bindingSimulated {
						return false, nil, nil
					}
					return true, boundPvc(tc.expDestName, destNamespace), nil
				})

			log := newTestLogger(t)
			cloner := pvc.NewPvCloner(log, kubeclient, pvc.PvClonerConfig{
				DestNamespace: destNamespace,
				RunSuffix:     runSuffix,
				WaitTimeout:   100 * time.Millisecond,
				SleepDuration: time.Millisecond,
			})

			destPvc, err := cloner.BackupPvcFromSourcePvc(ctx, tc.sourcePvc)
			if tc.expErrContains != "" {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tc.expErrContains) {
					t.Errorf("expected error to contain %q, got %q", tc.expErrContains, err)
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %q", err)
				}

				if destPvc.Name != tc.expDestName {
					t.Errorf("expected backup PVC name %q, got %q", tc.expDestName, destPvc.Name)
				}

				if destPvc.Namespace != destNamespace {
					t.Errorf("expected backup PVC namespace %q, got %q", destNamespace, destPvc.Namespace)
				}

				resPv, err := kubeclient.
					CoreV1().
					PersistentVolumes().
					Get(ctx, tc.expDestName, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("expected backup PV %q to exist: %v", tc.expDestName, err)
				}

				if !cmp.Equal(tc.expPvSpec, resPv.Spec) {
					t.Errorf("unexpected difference in backup PV spec: %v", cmp.Diff(tc.expPvSpec, resPv.Spec))
				}
			}

			// Clean up like a regular backup run would, then verify that the
			// created resources have actually been removed.
			bindingSimulated = false
			cloner.Cleanup(ctx)

			_, err = kubeclient.
				CoreV1().
				PersistentVolumes().
				Get(ctx, tc.expDestName, metav1.GetOptions{})
			if !k8sErrors.IsNotFound(err) {
				t.Errorf("expected backup PV to be deleted, got %v", err)
			}

			_, err = kubeclient.
				CoreV1().
				PersistentVolumeClaims(destNamespace).
				Get(ctx, tc.expDestName, metav1.GetOptions{})
			if !k8sErrors.IsNotFound(err) {
				t.Errorf("expected backup PVC to be deleted, got %v", err)
			}
		})
	}
}
