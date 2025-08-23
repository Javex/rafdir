package pvc

import (
	"context"
	"fmt"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type BackupPvc interface {
	// BackupPvcFromSourcePvc takes a source PVC to be backed up and returns a
	// new PVC that can be used as a backup source. It is up to the individual
	// implementation to define the semantics of how this new PVC is created. The
	// original sourcePvc must not be modified and the returned PVC must continue
	// to exist until Cleanup is called.
	BackupPvcFromSourcePvc(ctx context.Context, sourcePvc *corev1.PersistentVolumeClaim) (*corev1.PersistentVolumeClaim, error)
	// Cleanup removes all resources created purely for backup purposes. After
	// this function returns it is expected that all intermediate resources
	// created by this type have been removed, while original resources are left
	// unchanged.
	Cleanup(ctx context.Context)
}

type BackupPvcMethod string

const (
	BackupPvcMethodClone    BackupPvcMethod = "clone"
	BackupPvcMethodSnapshot BackupPvcMethod = "snapshot"
)

// BackupPvcMethodForPvc returns the appropriate BackupPvcMethod to use for a
// given sourcePvc based on the driver. An error is returned if the driver has
// not been mapped to a method yet.
func BackupPvcMethodForPvc(ctx context.Context, log *slog.Logger, kubeClient kubernetes.Interface, sourcePvc *corev1.PersistentVolumeClaim) (BackupPvcMethod, error) {
	pv, err := FindPvForPvc(ctx, log, kubeClient, sourcePvc)
	if err != nil {
		return "", fmt.Errorf("error looking up PV: %w", err)
	}

	csi := pv.Spec.CSI
	if csi == nil {
		return "", fmt.Errorf("error looking up driver, PV does not have CSI field")
	}

	driver := csi.Driver
	if driver == "" {
		return "", fmt.Errorf("error looking up driver, PV driver is empty")
	}

	log.Debug("Found driver for PVC", "driver", driver, "pvcName", sourcePvc.Name, "pvName", pv.Name)

	switch driver {
	case "nfs.csi.k8s.io":
		return BackupPvcMethodClone, nil
	case "topolvm.io":
		return BackupPvcMethodSnapshot, nil
	default:
		return "", fmt.Errorf("unsupported driver '%s'", driver)
	}
}

// FindPvForPvc find the PV for a given PVC or returns an error if the PV
// doesn't exist, for example because nothing has been bound yet.
func FindPvForPvc(ctx context.Context, log *slog.Logger, kubeClient kubernetes.Interface, sourcePvc *corev1.PersistentVolumeClaim) (*corev1.PersistentVolume, error) {
	namespace := sourcePvc.ObjectMeta.Namespace
	sourcePvcName := sourcePvc.ObjectMeta.Name
	log = log.With("namespace", namespace, "sourcePvcName", sourcePvcName)
	phase := sourcePvc.Status.Phase
	if phase != corev1.ClaimBound {
		log.Error("Unexpected phase, expected 'Bound'", "phase", phase)
		return nil, fmt.Errorf("invalid phase '%s', expected 'Bound'", phase)
	}
	log.Debug("Volume is bound", "phase", phase)

	pvName := sourcePvc.Spec.VolumeName
	if pvName == "" {
		log.Error("VolumeName is empty even though volume is bound")
		return nil, fmt.Errorf("empty VolumeName")
	}
	log = log.With("pvName", pvName)

	pv, err := kubeClient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			log.Error("Bound volume not found", "err", err)
			return nil, fmt.Errorf("volume not found: %s", err)
		}
		log.Error("Unexpected error when looking up PV", "err", err)
		return nil, err
	}
	return pv, nil
}
