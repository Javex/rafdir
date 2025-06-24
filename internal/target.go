package internal

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// VolumeType represents the type of volume being backed up
type VolumeType string

const (
	VolumeTypePVC VolumeType = "pvc"
	VolumeTypeNFS VolumeType = "nfs"
)

// VolumeInfo contains information about a volume that can be backed up
type VolumeInfo struct {
	Type        VolumeType
	VolumeMount *corev1.VolumeMount
	// For PVC volumes
	PVC *corev1.PersistentVolumeClaim
	// For NFS volumes
	NFS *corev1.NFSVolumeSource
}

// BackupTarget interface for different types of backup targets
type BackupTarget interface {
	PodName() string
}

type PodBackupTarget struct {
	Pod       *corev1.Pod
	Namespace string
	Selector  string

	// Support multiple volume types (PVCs and NFS)
	volumeInfos map[string]*VolumeInfo

	profile *Profile

	podName string
}

// NewBackupTargetFromDeploymentName creates a PodBackupTarget from a namespace
// and a deployment name. It finds the deployment, then the pod, then the PVC
// and the node name the pod is running on.
func NewBackupTargetFromDeploymentName(ctx context.Context, log *slog.Logger, kubeclient kubernetes.Interface, profile *Profile, runSuffix string) (*PodBackupTarget, error) {
	deploymentName := profile.Deployment
	namespace := profile.Namespace

	if deploymentName == "" {
		return nil, fmt.Errorf("deploymentName cannot be empty")
	}

	if namespace == "" {
		return nil, fmt.Errorf("namespace cannot be empty")
	}

	deployment, err := findDeploymentByName(ctx, kubeclient, namespace, deploymentName)
	if err != nil {
		return nil, err
	}

	selector := selectorFromDeployment(deployment)
	if selector == "" {
		return nil, fmt.Errorf("selector not found for deployment %s", deploymentName)
	}

	return NewBackupTargetFromSelector(ctx, kubeclient, namespace, selector, profile, runSuffix)
}

func NewBackupTargetFromStatefulSetName(ctx context.Context, log *slog.Logger, kubeclient kubernetes.Interface, profile *Profile, runSuffix string) (*PodBackupTarget, error) {
	statefulSetName := profile.StatefulSet
	namespace := profile.Namespace

	if statefulSetName == "" {
		return nil, fmt.Errorf("statefulSetName cannot be empty")
	}

	if namespace == "" {
		return nil, fmt.Errorf("namespace cannot be empty")
	}

	statefulSet, err := findStatefulSetByName(ctx, kubeclient, namespace, statefulSetName)
	if err != nil {
		return nil, err
	}

	selector := selectorFromStatefulSet(statefulSet)
	if selector == "" {
		return nil, fmt.Errorf("selector not found for statefulSet %s", statefulSetName)
	}

	return NewBackupTargetFromSelector(ctx, kubeclient, namespace, selector, profile, runSuffix)
}

func NewBackupTargetFromSelector(ctx context.Context, kubeclient kubernetes.Interface, namespace, selector string, profile *Profile, runSuffix string) (*PodBackupTarget, error) {
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

	target := &PodBackupTarget{
		Pod:       &pod,
		Namespace: namespace,
		Selector:  selector,

		profile: profile,

		podName: fmt.Sprintf("%s-%s-%s", profile.Name, pod.Name, runSuffix),
	}
	return target, nil
}

// findVolumes iterates through all volumes and determines backup targets.
// It supports both PVCs and NFS volumes. It is an error if there are no volumes.
func (t *PodBackupTarget) findVolumes(ctx context.Context, log *slog.Logger, kubeclient kubernetes.Interface) error {
	if t.volumeInfos == nil {
		t.volumeInfos = make(map[string]*VolumeInfo)

		for _, volume := range t.Pod.Spec.Volumes {
			if err := t.processVolume(ctx, log, kubeclient, volume); err != nil {
				return err
			}
		}

		if len(t.volumeInfos) == 0 {
			return fmt.Errorf("no volumes found")
		}
	}

	return nil
}

// processVolume handles a single volume, determining its type and creating the appropriate VolumeInfo
func (t *PodBackupTarget) processVolume(ctx context.Context, log *slog.Logger, kubeclient kubernetes.Interface, volume corev1.Volume) error {
	if volume.PersistentVolumeClaim != nil {
		return t.processPVCVolume(ctx, log, kubeclient, volume)
	}

	if volume.NFS != nil {
		return t.processNFSVolume(ctx, log, volume)
	}

	// Skip unsupported volume types
	return nil
}

// processPVCVolume handles PVC volumes, looking up the PVC and finding its mount path
func (t *PodBackupTarget) processPVCVolume(ctx context.Context, log *slog.Logger, kubeclient kubernetes.Interface, volume corev1.Volume) error {
	pvcName := volume.PersistentVolumeClaim.ClaimName
	if _, exists := t.volumeInfos[pvcName]; exists {
		return fmt.Errorf("more than one PVC found for %s", pvcName)
	}

	log.Info("Found PVC", "volumeName", volume.Name, "pvcName", pvcName)

	volumeMount, err := t.findVolumeMount(volume.Name)
	if err != nil {
		return fmt.Errorf("volume mount not found for %s: %w", pvcName, err)
	}

	pvc, err := t.lookupPVC(ctx, kubeclient, pvcName)
	if err != nil {
		return err
	}

	t.volumeInfos[pvcName] = &VolumeInfo{
		Type:        VolumeTypePVC,
		VolumeMount: volumeMount,
		PVC:         pvc,
	}

	return nil
}

// processNFSVolume handles NFS volumes, finding their mount path
func (t *PodBackupTarget) processNFSVolume(ctx context.Context, log *slog.Logger, volume corev1.Volume) error {
	volumeName := volume.Name
	if _, exists := t.volumeInfos[volumeName]; exists {
		return fmt.Errorf("more than one NFS volume found for %s", volumeName)
	}

	log.Info("Found NFS", "volumeName", volume.Name, "server", volume.NFS.Server, "path", volume.NFS.Path)

	volumeMount, err := t.findVolumeMount(volumeName)
	if err != nil {
		return fmt.Errorf("volume mount not found for NFS volume %s: %w", volumeName, err)
	}

	t.volumeInfos[volumeName] = &VolumeInfo{
		Type:        VolumeTypeNFS,
		VolumeMount: volumeMount,
		NFS:         volume.NFS,
	}

	return nil
}

// findVolumeMount searches for a volume mount that matches the given volume name
func (t *PodBackupTarget) findVolumeMount(volumeName string) (*corev1.VolumeMount, error) {
	for _, container := range t.Pod.Spec.Containers {
		for _, volumeMount := range container.VolumeMounts {
			if volumeMount.Name == volumeName {
				return &volumeMount, nil
			}
		}
	}
	return nil, fmt.Errorf("no volume mount found for volume %s", volumeName)
}

// lookupPVC retrieves a PVC from the Kubernetes API
func (t *PodBackupTarget) lookupPVC(ctx context.Context, kubeclient kubernetes.Interface, pvcName string) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := kubeclient.CoreV1().
		PersistentVolumeClaims(t.Pod.Namespace).
		Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("error looking up PVC %s: %w", pvcName, err)
	}
	return pvc, nil
}

// GetFolderToVolumeMapping returns a map of folder paths to volume information
func (t *PodBackupTarget) GetFolderToVolumeMapping(ctx context.Context, log *slog.Logger, kubeclient kubernetes.Interface) (map[string]*VolumeInfo, error) {
	err := t.findVolumes(ctx, log, kubeclient)
	if err != nil {
		return nil, err
	}

	folderToVolumeInfo := make(map[string]*VolumeInfo)
	for _, volumeInfo := range t.volumeInfos {
		folderToVolumeInfo[volumeInfo.VolumeMount.MountPath] = volumeInfo
	}

	return folderToVolumeInfo, nil
}

// selectorFromDeployment returns a label selector for a deployment
func selectorFromDeployment(deployment *appsv1.Deployment) string {
	matchLabels := deployment.Spec.Selector.MatchLabels
	// create selector from matchLabels
	selectors := make([]string, 0, len(matchLabels))
	for k, v := range matchLabels {
		selectors = append(selectors, k+"="+v)
	}
	selector := fmt.Sprintf("%s", strings.Join(selectors, ","))
	return selector
}

// selectorFromStatefulSet returns a label selector for a statefulset
func selectorFromStatefulSet(statefulSet *appsv1.StatefulSet) string {
	matchLabels := statefulSet.Spec.Selector.MatchLabels
	// create selector from matchLabels
	selectors := make([]string, 0, len(matchLabels))
	for k, v := range matchLabels {
		selectors = append(selectors, k+"="+v)
	}
	selector := fmt.Sprintf("%s", strings.Join(selectors, ","))
	return selector
}

func findDeploymentByName(ctx context.Context, kubeclient kubernetes.Interface, namespace string, name string) (*appsv1.Deployment, error) {
	deployment, err := kubeclient.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return deployment, nil
}

func findStatefulSetByName(ctx context.Context, kubeclient kubernetes.Interface, namespace string, name string) (*appsv1.StatefulSet, error) {
	statefulSet, err := kubeclient.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return statefulSet, nil
}

// PodName creates a unique name for the pod that runs the backup for this
// target.
func (t *PodBackupTarget) PodName() string {
	return t.podName
}
