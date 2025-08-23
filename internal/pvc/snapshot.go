package pvc

import (
	"context"
	"fmt"
	"log/slog"
	"rafdir/internal/meta"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	volumesnapshot "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	csiClientset "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
)

// snapshot.go contains the process of turning a PVC in one namespaces into a
// PVC in another namespace by taking a snapshot, reading its SnapshotContent,
// and creating a new PVC from that SnapshotContent.

type PvcSnapshotterConfig struct {
	// DestNamespace is the namespace where the new PVC will be created. This
	// is the backup namespace.
	DestNamespace string
	// A unique suffix to append to the names of resources created during the
	// backup process. This is to avoid name collisions.
	RunSuffix string
	// SnapshotClass is the name of the VolumeSnapshotClass to use when creating
	// snapshots.
	SnapshotClass string
	// StorageClass is the name of the StorageClass used for the temporary PVC.
	StorageClass string
	// ScaleUp is a function that will be called once the source PVC has been
	// processed and the service can be started again. It can be nil if the
	// service was not stopped. In that case no call will be attempted.
	ScaleUp func()

	WaitTimeout            time.Duration
	SnapshotContentTimeout time.Duration
	SleepDuration          time.Duration
}

type PvcSnapshotter struct {
	destNamespace string
	runSuffix     string
	snapshotClass string
	storageClass  string
	scaleUp       func()

	waitTimeout            time.Duration
	snapshotContentTimeout time.Duration
	sleepDuration          time.Duration

	kubeClient kubernetes.Interface
	csiClient  csiClientset.Interface
	log        *slog.Logger

	// Internal resources
	sourceNamespace string
	snapshotDriver  string

	// Resources created that need to be cleaned up at the end
	sourceSnapshotName string
	destSnapshotName   string
	destPvcName        string
}

func NewPvcSnapshotter(log *slog.Logger, kubeClient kubernetes.Interface, csiClient csiClientset.Interface, cfg PvcSnapshotterConfig) *PvcSnapshotter {
	return &PvcSnapshotter{
		destNamespace: cfg.DestNamespace,
		runSuffix:     cfg.RunSuffix,
		snapshotClass: cfg.SnapshotClass,
		storageClass:  cfg.StorageClass,
		scaleUp:       cfg.ScaleUp,

		waitTimeout:            cfg.WaitTimeout,
		snapshotContentTimeout: cfg.SnapshotContentTimeout,
		sleepDuration:          cfg.SleepDuration,

		kubeClient: kubeClient,
		csiClient:  csiClient,
		log:        log,
	}
}

// BackupPvcFromSourcePvc takes a source PVC and provides a new PVC in the
// target namespace that is a snapshot of the source PVC.
func (s *PvcSnapshotter) BackupPvcFromSourcePvc(ctx context.Context, sourcePvc *corev1.PersistentVolumeClaim) (*corev1.PersistentVolumeClaim, error) {
	if s.sourceNamespace != "" {
		s.log.Error("PvcSnapshotter already used, cannot use again", "sourceNamespace", s.sourceNamespace)
		return nil, fmt.Errorf("PvcSnapshotter already used for sourceNamespace %s", s.sourceNamespace)
	}

	snapshotDriver, err := s.storageDriverFromPvc(ctx, sourcePvc)
	if err != nil {
		return nil, fmt.Errorf("Failed to get storage driver: %w", err)
	}
	s.snapshotDriver = snapshotDriver

	if err := s.verifyDriver(ctx); err != nil {
		return nil, fmt.Errorf("Failed to verify driver: %w", err)
	}

	s.sourceNamespace = sourcePvc.Namespace
	pvcName := sourcePvc.Name
	snapshotName := fmt.Sprintf("%s-snapshot-%s", pvcName, s.runSuffix)
	snapshotContentName := fmt.Sprintf("%s-snapcontent-%s", pvcName, s.runSuffix)
	backupPVCName := fmt.Sprintf("%s-backup-%s", pvcName, s.runSuffix)
	storageSize := sourcePvc.Spec.Resources.Requests[corev1.ResourceStorage]
	snapshot, err := s.takeSnapshot(ctx, snapshotName, pvcName)
	if err != nil {
		return nil, fmt.Errorf("Failed takeSnapshot: %s", err)
	}
	s.sourceSnapshotName = snapshotName

	sourceContentName, err := s.waitSnapContent(ctx, snapshot)
	if err != nil {
		return nil, fmt.Errorf("Failed to waitSnapContent: %s", err)
	}

	contentHandle, err := s.snapshotHandleFromContent(ctx, sourceContentName)
	if err != nil {
		return nil, fmt.Errorf("Failed to snapshotHandleFromContent: %s", err)
	}

	// Snapshot has been finished, service can start back up
	s.doScaleUp()

	err = s.snapshotContentFromHandle(ctx, snapshotContentName, contentHandle, snapshotName)
	if err != nil {
		return nil, fmt.Errorf("Failed to snapshotContentFromHandle: %s", err)
	}

	err = s.snapshotFromContent(ctx, snapshotName, &snapshotContentName)
	if err != nil {
		return nil, fmt.Errorf("Failed to snapshotFromContent: %s", err)
	}
	s.destSnapshotName = snapshotName

	err = s.waitSnapshotReady(ctx, &snapshotName)
	if err != nil {
		return nil, fmt.Errorf("Failed to waitSnapshotReady: %s", err)
	}

	destPvc, err := s.pvcFromSnapshot(ctx, snapshotName, backupPVCName, storageSize)
	if err != nil {
		return nil, fmt.Errorf("Failed to pvcFromSnapshot: %s", err)
	}
	s.destPvcName = backupPVCName

	return destPvc, nil
}

// doScaleUp calls the scaleUp function passed during object creation. It
// ensures to only call it once, repeated calls will be a no-op. This is
// because the passed in function does not expect to be called more than once.
func (s *PvcSnapshotter) doScaleUp() {
	// This is a function that is passed in to scale up the deployment in case it
	// was stopped. Runs after there's a handle because that indicates that the
	// snapshot has been created and the original volume can be used again.
	if s.scaleUp != nil {
		s.scaleUp()
		s.log.Debug("Scaled back up")
		// Ensure function is only called once
		s.scaleUp = nil
	}

}

func (s *PvcSnapshotter) Cleanup(ctx context.Context) {
	if s.sourceSnapshotName != "" {
		s.deleteSnapshot(ctx, s.sourceNamespace, s.sourceSnapshotName)
	}

	if s.destSnapshotName != "" {
		s.deleteSnapshot(ctx, s.destNamespace, s.destSnapshotName)
	}

	if s.destPvcName != "" {
		s.deletePVC(ctx, s.destPvcName)
	}
}

// storageDriverFromPvc finds the storage driver by querying the class of the
// PVC and then looks up the driver used for that class. Since all actions in
// the snapshotting process must be on the same storage engine, the driver is a
// fixed value that can't change between source and destination PVC.
func (s *PvcSnapshotter) storageDriverFromPvc(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (string, error) {
	storageClass := pvc.Spec.StorageClassName
	if storageClass == nil {
		return "", fmt.Errorf("PVC has no storage class")
	}

	storageClassObj, err := s.kubeClient.
		StorageV1().
		StorageClasses().
		Get(ctx, *storageClass, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("Failed to get storage class: %w", err)
	}

	if storageClassObj.Provisioner == "" {
		return "", fmt.Errorf("Storage class has no provisioner")
	}

	return storageClassObj.Provisioner, nil
}

// verifyDriver makes sure that the driver of the snapshot class, storage class
// and source PVC match
func (s *PvcSnapshotter) verifyDriver(ctx context.Context) error {
	if s.snapshotDriver == "" {
		return fmt.Errorf("Snapshot driver is not set")
	}

	// Get driver for snapshot class
	snapshotClassObj, err := s.csiClient.SnapshotV1().
		VolumeSnapshotClasses().
		Get(ctx, s.snapshotClass, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("Failed to get snapshot class: %w", err)
	}

	if snapshotClassObj.Driver != s.snapshotDriver {
		s.log.Error("Snapshot class driver does not match snapshot driver", "snapshotClassDriver", snapshotClassObj.Driver, "snapshotDriver", s.snapshotDriver)
		return fmt.Errorf("Snapshot class driver %s does not match snapshot driver %s", snapshotClassObj.Driver, s.snapshotDriver)
	}

	// Get driver for storage class
	storageClassObj, err := s.kubeClient.StorageV1().
		StorageClasses().
		Get(ctx, s.storageClass, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("Failed to get storage class: %w", err)
	}

	if storageClassObj.Provisioner != s.snapshotDriver {
		s.log.Error("Storage class driver does not match snapshot driver", "storageClassDriver", storageClassObj.Provisioner, "snapshotDriver", s.snapshotDriver)
		return fmt.Errorf("Storage class driver %s does not match snapshot driver %s", storageClassObj.Provisioner, s.snapshotDriver)
	}

	s.log.Debug("Driver verified to match storage class & snapshot class")

	return nil
}

func (s *PvcSnapshotter) takeSnapshot(ctx context.Context, snapshotName string, pvcName string) (*volumesnapshot.VolumeSnapshot, error) {
	namespace := s.sourceNamespace
	existingSnapshot, err := s.csiClient.SnapshotV1().VolumeSnapshots(namespace).
		Get(ctx, snapshotName, metav1.GetOptions{})
	if err == nil {
		s.log.Info("Snapshot already exists, not creating new one", "namespace", namespace, "snapshotName", snapshotName)
		return existingSnapshot, nil
	}

	// There *is* an error but it might be "not found" which isn't a problem.
	// Everything else is.
	if !k8sErrors.IsNotFound(err) {
		return nil, fmt.Errorf("Error when trying to check for existing snapshot: %w", err)
	}

	snapshot := volumesnapshot.VolumeSnapshot{
		ObjectMeta: meta.NewObjectMeta(snapshotName, namespace, s.runSuffix),
		Spec: volumesnapshot.VolumeSnapshotSpec{
			VolumeSnapshotClassName: &s.snapshotClass,
			Source: volumesnapshot.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvcName,
			},
		},
	}
	createdSnapshot, err := s.csiClient.SnapshotV1().
		VolumeSnapshots(namespace).
		Create(ctx, &snapshot, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("Error creating snapshot: %w", err)
	}

	s.log.Info("Snapshot created", "namespace", namespace, "snapshotName", snapshotName)

	return createdSnapshot, nil
}

func (s *PvcSnapshotter) snapshotFromContent(ctx context.Context, snapshotName string, contentName *string) error {
	_, err := s.csiClient.SnapshotV1().
		VolumeSnapshots(s.destNamespace).
		Get(ctx, snapshotName, metav1.GetOptions{})

	if err == nil {
		s.log.Info("Snapshot already exists, not creating new one", "namespace", s.destNamespace, "snapshotName", snapshotName)
		return nil
	}

	if !k8sErrors.IsNotFound(err) {
		s.log.Error("Unexpected error when checking for existing snapshot", "err", err, "namespace", s.destNamespace, "snapshotName", snapshotName)
		return fmt.Errorf("Error when checking for existing snapshot: %w", err)
	}

	snapshot := volumesnapshot.VolumeSnapshot{
		ObjectMeta: meta.NewObjectMeta(snapshotName, s.destNamespace, s.runSuffix),
		Spec: volumesnapshot.VolumeSnapshotSpec{
			Source: volumesnapshot.VolumeSnapshotSource{
				VolumeSnapshotContentName: contentName,
			},
		},
	}
	_, err = s.csiClient.SnapshotV1().
		VolumeSnapshots(s.destNamespace).
		Create(ctx, &snapshot, metav1.CreateOptions{})

	s.log.Info("Created Snapshot from content", "namespace", s.destNamespace, "snapshotName", snapshotName, "contentName", contentName)

	return err
}

func (s *PvcSnapshotter) deleteSnapshot(ctx context.Context, namespace string, snapshotName string) error {
	err := s.csiClient.SnapshotV1().
		VolumeSnapshots(namespace).
		Delete(ctx, snapshotName, metav1.DeleteOptions{})
	if err != nil {
		s.log.Error("Error deleting snapshot", "namespace", namespace, "snapshotName", snapshotName, "err", err)
		return err
	}

	s.log.Info("Deleted snapshot", "namespace", namespace, "snapshotName", snapshotName)
	return nil
}

func (s *PvcSnapshotter) snapshotContentFromHandle(ctx context.Context, snapshotContentName string, snapshotContentHandle string, snapshotName string) error {
	_, err := s.csiClient.SnapshotV1().
		VolumeSnapshotContents().
		Get(ctx, snapshotContentName, metav1.GetOptions{})
	if err == nil {
		s.log.Info("SnapshotContent already exists, not creating new one", "snapshotContentName", snapshotContentName)
		return nil
	}

	if !k8sErrors.IsNotFound(err) {
		return fmt.Errorf("Error checking for existing SnapshotContent: %w", err)
	}

	snapshotContent := volumesnapshot.VolumeSnapshotContent{
		ObjectMeta: meta.NewObjectMeta(snapshotContentName, "", s.runSuffix),
		Spec: volumesnapshot.VolumeSnapshotContentSpec{
			DeletionPolicy:          volumesnapshot.VolumeSnapshotContentDelete,
			Driver:                  s.snapshotDriver,
			VolumeSnapshotClassName: &s.snapshotClass,
			Source: volumesnapshot.VolumeSnapshotContentSource{
				SnapshotHandle: &snapshotContentHandle,
			},
			VolumeSnapshotRef: corev1.ObjectReference{
				Name:      snapshotName,
				Namespace: s.destNamespace,
			},
		},
	}
	_, err = s.csiClient.SnapshotV1().
		VolumeSnapshotContents().
		Create(ctx, &snapshotContent, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("Error creating SnapshotContents: %w", err)
	}

	s.log.Info("Created SnapshotContent from handle", "snapshotContentName", snapshotContentName, "snapshotContentHandle", snapshotContentHandle)

	return nil
}

func (s *PvcSnapshotter) waitSnapContent(ctx context.Context, snapshot *volumesnapshot.VolumeSnapshot) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.snapshotContentTimeout)
	defer cancel()
	namespace := snapshot.ObjectMeta.Namespace
	snapshotName := snapshot.ObjectMeta.Name
	log := s.log.With("namespace", namespace, "snapshotName", snapshotName)

	for {
		select {
		case <-ctx.Done():
			log.Error("Timed out waiting for SnapshotContent to be bound to Snapshot")
			return "", fmt.Errorf("Timeout")

		default:
			snapshot, err := s.csiClient.SnapshotV1().
				VolumeSnapshots(namespace).
				Get(ctx, snapshotName, metav1.GetOptions{})
			if err != nil {
				return "", fmt.Errorf("Error getting snapshot: %w", err)
			}

			if snapshot.Status != nil && snapshot.Status.BoundVolumeSnapshotContentName != nil {
				log.Info("Snapshot has been bound to content", "namespace", namespace, "snapshotName", snapshotName, "contentName", *snapshot.Status.BoundVolumeSnapshotContentName)
				return *snapshot.Status.BoundVolumeSnapshotContentName, nil
			}

			time.Sleep(s.sleepDuration)

		}
	}
}

func (s *PvcSnapshotter) deleteSnapshotContent(ctx context.Context, snapshotContentName string) error {
	err := s.csiClient.SnapshotV1().
		VolumeSnapshotContents().
		Delete(ctx, snapshotContentName, metav1.DeleteOptions{})
	if err != nil {
		s.log.Error("Failed to delete SnapshotContent", "snapshotContentName", snapshotContentName, "err", err)
		return err
	}
	s.log.Info("Deleted SnapshotContent", "snapshotContentName", snapshotContentName)
	return nil
}

func (s *PvcSnapshotter) snapshotHandleFromContent(ctx context.Context, contentName string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.waitTimeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			s.log.Error("Timed out waiting to get snapshot handle from content")
			return "", fmt.Errorf("Timeout")

		default:
			snapshotContent, err := s.csiClient.SnapshotV1().
				VolumeSnapshotContents().
				Get(ctx, contentName, metav1.GetOptions{})
			if err != nil {
				return "", fmt.Errorf("Error getting snapshotContent: %w", err)
			}

			if snapshotContent.Status != nil && snapshotContent.Status.SnapshotHandle != nil {
				s.log.Info("SnapshotHandle is ready", "contentName", contentName, "SnapshotHandle", *snapshotContent.Status.SnapshotHandle)
				return *snapshotContent.Status.SnapshotHandle, nil
			}

			s.log.Debug("SnapshotHandle is not ready yet", "contentName", contentName)
			time.Sleep(s.sleepDuration)

		}
	}
}

func (s *PvcSnapshotter) waitSnapshotReady(ctx context.Context, snapshotName *string) error {
	ctx, cancel := context.WithTimeout(ctx, s.waitTimeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			s.log.Error("Timed out waiting for snapshot to be ready")
			return fmt.Errorf("Timeout")

		default:
			snapshot, err := s.csiClient.SnapshotV1().
				VolumeSnapshots(s.destNamespace).
				Get(ctx, *snapshotName, metav1.GetOptions{})

			if err != nil {
				s.log.Error("Error when waiting for snapshot to be ready", "namespace", s.destNamespace, "snapshotName", snapshotName, "err", err)
				return fmt.Errorf("Error waiting for snapshot to be ready: %w", err)
			}

			if snapshot.Status != nil && snapshot.Status.ReadyToUse != nil && *snapshot.Status.ReadyToUse {
				s.log.Info("Snapshot is ready to use", "namespace", s.destNamespace, "snapshotName", *snapshotName)
				return nil
			}

			s.log.Debug("Snapshot is not ready yet", "namespace", s.destNamespace, "snapshotName", *snapshotName)
			time.Sleep(s.sleepDuration)
		}
	}
}

func (s *PvcSnapshotter) pvcFromSnapshot(ctx context.Context, snapshotName string, pvcName string, storageSize resource.Quantity) (*corev1.PersistentVolumeClaim, error) {
	log := s.log.With("namespace", s.destNamespace, "pvcName", pvcName)
	// Check if PVC already exists
	pvc, err := s.kubeClient.CoreV1().
		PersistentVolumeClaims(s.destNamespace).
		Get(ctx, pvcName, metav1.GetOptions{})
	if err == nil {
		log.Info("PVC already exists, not creating new one")
		return pvc, nil
	}

	apiGroup := volumesnapshot.GroupName

	pvc = &corev1.PersistentVolumeClaim{
		ObjectMeta: meta.NewObjectMeta(pvcName, s.destNamespace, s.runSuffix),
		Spec: corev1.PersistentVolumeClaimSpec{
			DataSource: &corev1.TypedLocalObjectReference{
				Name:     snapshotName,
				Kind:     "VolumeSnapshot",
				APIGroup: &apiGroup,
			},
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			StorageClassName: &s.storageClass,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: storageSize,
				},
			},
		},
	}

	_, err = s.kubeClient.CoreV1().
		PersistentVolumeClaims(s.destNamespace).
		Create(ctx, pvc, metav1.CreateOptions{})
	if err != nil {
		log.Error("Error creating PVC", "snapshotName", snapshotName, "err", err)
		return nil, fmt.Errorf("Error creating PVC: %w", err)
	}

	log.Info("Created new PVC", "snapshotName", snapshotName)
	return pvc, nil
}

func (s *PvcSnapshotter) deletePVC(ctx context.Context, pvcName string) error {
	log := s.log.With("namespace", s.destNamespace, "pvcName", pvcName)
	_, err := s.kubeClient.CoreV1().
		PersistentVolumeClaims(s.destNamespace).
		Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			log.Warn("PVC does not exist, skipping deletion.")
			return nil
		}
		log.Error("Unexpected error when checking if PVC exists", "err", err)
		return err
	}

	err = s.kubeClient.CoreV1().
		PersistentVolumeClaims(s.destNamespace).
		Delete(ctx, pvcName, metav1.DeleteOptions{})
	if err != nil {
		s.log.Error("Error deleting PVC", "err", err)
		return err
	}

	log.Info("Deleted PVC")
	return nil
}
