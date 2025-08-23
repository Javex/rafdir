package pvc

import (
	"context"

	corev1 "k8s.io/api/core/v1"
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
