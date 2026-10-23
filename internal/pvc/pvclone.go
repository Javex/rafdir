package pvc

import (
	"context"
	"fmt"
	"log/slog"
	"rafdir/internal/meta"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type PvClonerConfig struct {
	// DestNamespace is the namespace where the new PVC will be created. This
	// is the backup namespace.
	DestNamespace string
	// A unique suffix to append to the names of resources created during the
	// backup process. This is to avoid name collisions.
	RunSuffix string
	// WaitTimeout is the time to wait for the binding of the PV & PVC to finish
	// in the backup namespace. If it takes longer than this, an error will be
	// returned. The Cleanup function should still be called to clean up any
	// leftover state.
	WaitTimeout time.Duration
	// SleepDuration is the time to wait between checking the API if binding has
	// succeeded
	SleepDuration time.Duration
}

type PvCloner struct {
	destNamespace string
	runSuffix     string
	waitTimeout   time.Duration
	sleepDuration time.Duration

	// destPvName is the name of the PV that was created as a clone of the
	// target (backup) PV.
	destPvName string
	// destPvcName is the name of the PVC that's bound to the backup PV
	destPvcName string

	kubeClient kubernetes.Interface
	log        *slog.Logger
}

func NewPvCloner(log *slog.Logger, kubeClient kubernetes.Interface, cfg PvClonerConfig) *PvCloner {
	cloner := &PvCloner{
		destNamespace: cfg.DestNamespace,
		runSuffix:     cfg.RunSuffix,
		waitTimeout:   cfg.WaitTimeout,
		sleepDuration: cfg.SleepDuration,

		log:        log,
		kubeClient: kubeClient,
	}
	return cloner
}

// BackupPvcFromSourcePvc creates a new PVC in the backup namespace by cloning
// the Persistent Volume resource and then binding a new PVC to it. This will
// only work if the underlying storage engine supports this kind of behaviour.
// For example, this works for NFS volumes as they can be mounted multiple
// times. Most other storage systems (such as TopoLVM) won't support this
// behaviour and should rely on snapshots. Only when snapshots aren't supported
// does it make sense to use the PvCloner instead.
// The function supports two volume sources: CSI and NFS.
// For CSI, this function only accepts the driver "nfs.csi.k8s.io" as it has
// been tested. Other drivers can be added after they've been tested to work.
func (c *PvCloner) BackupPvcFromSourcePvc(ctx context.Context, sourcePvc *corev1.PersistentVolumeClaim) (*corev1.PersistentVolumeClaim, error) {
	sourcePvcName := sourcePvc.Name
	log := c.log.With("sourcePvcName", sourcePvcName, "namespace", sourcePvc.Namespace)
	// Get current PV from PVC
	sourcePv, err := FindPvForPvc(ctx, c.log, c.kubeClient, sourcePvc)
	if err != nil {
		log.Error("Failed to get source PV from PVC", "err", err)
		return nil, fmt.Errorf("Failed to get source PV from PVC: %w", err)
	}
	log = log.With("sourcePvName", sourcePv.Name)

	// Use the PVC as the name source because the PV will have a randomly
	// generated name like "pvc-<uuid>". In our case, since the PV is manually
	// created and not dynamically from a PVC, using the PVC name makes more
	// sense and looks nicer.
	destPvName := fmt.Sprintf("%s-%s", sourcePvcName, c.runSuffix)
	log = log.With("destPvName", destPvName)

	// Create new PV from existing PV
	var pvSource corev1.PersistentVolumeSource
	switch {
	case sourcePv.Spec.CSI != nil:
		pvSource, err = c.getPvSourceCsi(log, sourcePv, destPvName)
	case sourcePv.Spec.NFS != nil:
		pvSource, err = c.getPvSourceNfs(log, sourcePv, destPvName)
	default:
		log.Error("Unsupported PV configuration. Only CSI & NFS are supported")
		return nil, fmt.Errorf("unsupported PV configuration")
	}

	if err != nil {
		log.Error("Failed to derive PV Source", "err", err)
		return nil, fmt.Errorf("Failed to derive PV Source: %w", err)
	}

	destPv := &corev1.PersistentVolume{
		ObjectMeta: meta.NewObjectMeta(destPvName, "", c.runSuffix),
		Spec: corev1.PersistentVolumeSpec{
			AccessModes:  sourcePv.Spec.AccessModes,
			Capacity:     sourcePv.Spec.Capacity,
			MountOptions: sourcePv.Spec.MountOptions,
			// Always Retain because we're about to delete this one, but it's just a
			// copy. If this were to delete, it'd delete the source data.
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			StorageClassName:              sourcePv.Spec.StorageClassName,
			VolumeMode:                    sourcePv.Spec.VolumeMode,

			PersistentVolumeSource: pvSource,
		},
	}

	destPv, err = c.kubeClient.CoreV1().PersistentVolumes().Create(ctx, destPv, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("error creating PV: %w", err)
	}

	c.log.Info("PV created", "destPvName", destPvName)
	c.destPvName = destPv.ObjectMeta.Name
	// Create new PVC using new PV
	destPvc, err := c.bindPvc(ctx, destPv)
	if err != nil {
		log.Error("Failed to bind PVC for backup PV", "err", err)
		return nil, fmt.Errorf("Failed to bind PVC for backup PV: %w", err)
	}
	c.destPvcName = destPvc.ObjectMeta.Name

	if err = c.waitPvc(ctx, destPvc); err != nil {
		log.Error("Error waiting for PVC to bind", "err", err)
		return nil, fmt.Errorf("error waiting for PVC to bind: %w", err)
	}

	return destPvc, nil
}

// getPvSourceCsi returns the PersistentVolumeSource for a CSI-based PV
func (c *PvCloner) getPvSourceCsi(log *slog.Logger, sourcePv *corev1.PersistentVolume, destPvName string) (corev1.PersistentVolumeSource, error) {
	if sourcePv.Spec.CSI == nil {
		log.Error("Expected .spec.csi to be set, but it's nil, invalid PV")
		return corev1.PersistentVolumeSource{}, fmt.Errorf("invalid PV, expected CSI object")
	}

	if driver, supportedDriver := sourcePv.Spec.CSI.Driver, "nfs.csi.k8s.io"; driver != supportedDriver {
		log.Error("Invalid driver, not supported", "driver", driver, "supportedDriver", supportedDriver)
		return corev1.PersistentVolumeSource{}, fmt.Errorf("invalid driver '%s', expected '%s'", driver, supportedDriver)
	}

	// Note: The below copy process makes some assumptions about the driver, i.e.
	// it uses fields it expects from the NFS driver. This needs to be fixed if
	// other drivers need to be supported.
	volumeAttributes := sourcePv.Spec.PersistentVolumeSource.CSI.VolumeAttributes
	newAttributes, err := nfsCloneVolumeAttributes(volumeAttributes)
	if err != nil {
		return corev1.PersistentVolumeSource{}, fmt.Errorf("failed to clone attributes: %s", err)
	}

	pvSource := corev1.PersistentVolumeSource{
		CSI: &corev1.CSIPersistentVolumeSource{
			Driver: sourcePv.Spec.PersistentVolumeSource.CSI.Driver,
			// VolumeHandle needs to be unique which our PV name should be
			VolumeHandle:     destPvName,
			VolumeAttributes: newAttributes,
		},
	}

	return pvSource, nil
}

// getPvSourceNfs returns the PersistentVolumeSource for an NFS-based PV.
func (c *PvCloner) getPvSourceNfs(log *slog.Logger, sourcePv *corev1.PersistentVolume, destPvName string) (corev1.PersistentVolumeSource, error) {
	nfs := sourcePv.Spec.NFS
	if nfs == nil {
		log.Error("Expected .spec.nfs to be set, but it's nil, invalid PV")
		return corev1.PersistentVolumeSource{}, fmt.Errorf("invalid PV, expected NFS object")
	}

	pvSource := corev1.PersistentVolumeSource{
		NFS: &corev1.NFSVolumeSource{
			Server: nfs.Server,
			Path:   nfs.Path,
			// Backup can always be read-only
			ReadOnly: true,
		},
	}
	return pvSource, nil
}

// nfsCloneVolumeAttributes creates a new set of attributes for the cloned PV.
// This is only a subset of the attributes on the source PV, as many attributes
// are set by the driver after the fact. This has NFS assumptions built in.
func nfsCloneVolumeAttributes(attributes map[string]string) (map[string]string, error) {
	server, ok := attributes["server"]
	if !ok {
		return nil, fmt.Errorf("missing mandatory attribute 'server'")
	}

	share, ok := attributes["share"]
	if !ok {
		return nil, fmt.Errorf("missing mandatory attribute 'share'")
	}

	newAttributes := map[string]string{
		"server": server,
		"share":  share,
	}

	// Only set subDir if it's defined, otherwise leave it out
	if subDir, ok := attributes["subDir"]; ok {
		newAttributes["subDir"] = subDir
	}

	return newAttributes, nil
}

func (c *PvCloner) bindPvc(ctx context.Context, destPv *corev1.PersistentVolume) (*corev1.PersistentVolumeClaim, error) {
	// Use same name for PVC since it's a 1:1 mapping and there shouldn't be
	// another PVC with that name in our namespace since we create unique names
	// and clean up after ourselves
	destPvcName := destPv.ObjectMeta.Name
	storageClassName := destPv.Spec.StorageClassName
	destPvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: meta.NewObjectMeta(destPvcName, c.destNamespace, c.runSuffix),
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: destPv.Spec.AccessModes,
			Resources: corev1.VolumeResourceRequirements{
				Requests: destPv.Spec.Capacity,
			},
			StorageClassName: &storageClassName,
			DataSource: &corev1.TypedLocalObjectReference{
				Kind: destPv.Kind,
				Name: destPv.Name,
			},
		},
	}

	destPvc, err := c.kubeClient.
		CoreV1().
		PersistentVolumeClaims(c.destNamespace).
		Create(ctx, destPvc, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("error creating PVC: %w", err)
	}

	c.log.Info("PVC created", "namespace", c.destNamespace, "pvcName", destPvc.Name)

	return destPvc, nil
}

// waitPvc waits until the PV & PVC have finished created and have been bound
// or the timeout has been reached (in which case an error is raised)
func (c *PvCloner) waitPvc(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	ctx, cancel := context.WithTimeout(ctx, c.waitTimeout)
	defer cancel()
	namespace := c.destNamespace
	pvcName := pvc.Name
	log := c.log.With("namespace", namespace, "pvcName", pvcName)

	for {
		select {
		case <-ctx.Done():
			log.Error("Timed out waiting for PV & PVC to be bound")
			return fmt.Errorf("Timeout")
		default:
			pvc, err := c.kubeClient.
				CoreV1().
				PersistentVolumeClaims(namespace).
				Get(ctx, pvcName, metav1.GetOptions{})

			if err != nil {
				return fmt.Errorf("error getting PVC: %w", err)
			}

			if pvc.Status.Phase == corev1.ClaimBound {
				log.Info("PVC has been bound to PV")
				return nil
			}

			log.Debug("PVC has not been bound yet, sleeping...")
			time.Sleep(c.sleepDuration)
		}
	}
}

func (c *PvCloner) Cleanup(ctx context.Context) {
	if c.destPvcName != "" {
		c.deletePvc(ctx, c.destPvcName)
	}

	if c.destPvName != "" {
		c.deletePv(ctx, c.destPvName)
	}
}

func (c *PvCloner) deletePvc(ctx context.Context, pvcName string) error {
	log := c.log.With("namespace", c.destNamespace, "pvcName", pvcName)
	_, err := c.kubeClient.CoreV1().
		PersistentVolumeClaims(c.destNamespace).
		Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			log.Warn("PVC does not exist, skipping deletion.")
			return nil
		}
		log.Error("Unexpected error when checking if PVC exists", "err", err)
		return err
	}

	err = c.kubeClient.CoreV1().
		PersistentVolumeClaims(c.destNamespace).
		Delete(ctx, pvcName, metav1.DeleteOptions{})
	if err != nil {
		c.log.Error("Error deleting PVC", "err", err)
		return err
	}

	log.Info("Deleted PVC")
	return nil
}

func (c *PvCloner) deletePv(ctx context.Context, pvName string) error {
	log := c.log.With("pvName", pvName)
	_, err := c.kubeClient.CoreV1().
		PersistentVolumes().
		Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			log.Warn("PV does not exist, skipping deletion.")
			return nil
		}
		log.Error("Unexpected error when checking if PV exists", "err", err)
		return err
	}

	err = c.kubeClient.CoreV1().
		PersistentVolumes().
		Delete(ctx, pvName, metav1.DeleteOptions{})
	if err != nil {
		c.log.Error("Error deleting PV", "err", err)
		return err
	}

	log.Info("Deleted PV")
	return nil
}
