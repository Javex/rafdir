package resticprofilek8s

import (
	"context"
	"fmt"
	"log/slog"
	"resticprofilek8s/internal"
	"time"

	// apiv1 "k8s.io/api/core/v1"
	corev1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	volumesnapshot "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	csiClientset "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
)

type SnapshotClient struct {
	kubeClient kubernetes.Interface
	csiClient  csiClientset.Interface
	config     *internal.Config
	log        *slog.Logger
}

func NewClient(log *slog.Logger, kubeClient kubernetes.Interface, csiClient csiClientset.Interface, config *internal.Config) (*SnapshotClient, error) {

	client := SnapshotClient{
		kubeClient: kubeClient,
		csiClient:  csiClient,
		config:     config,
		log:        log,
	}
	return &client, nil
}

func (s *SnapshotClient) TakeBackup(ctx context.Context) error {
	log := s.log
	log.Info("Starting backup run")
	// Suffix to apply to all resources managed by this run. Existing resources
	// will be skipped to create an idempotent run. Resources will be deleted
	// when they are no longer needed.
	runSuffix := "testing"
	config := s.config
	profiles := config.Profiles
	repos := config.Repositories

	baseProfile, err := s.config.BaseProfile()
	if err != nil {
		return fmt.Errorf("Failed to render base profile: %s", err)
	}

	for _, profile := range profiles {

		namespace := profile.Namespace
		target, err := internal.NewBackupTargetFromDeploymentName(ctx, log, s.kubeClient, namespace, profile.Deployment)
		if err != nil {
			return fmt.Errorf("Failed to NewBackupTargetFromDeploymentName: %s", err)
		}

		pvcName := target.Pvc.Name
		snapshotName := fmt.Sprintf("%s-snapshot-%s", pvcName, runSuffix)
		snapshotContentName := fmt.Sprintf("%s-snapcontent-%s", pvcName, runSuffix)
		backupPVCName := fmt.Sprintf("%s-backup-%s", pvcName, runSuffix)
		storageSize := target.Pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		podName := fmt.Sprintf("%s-%s-%s", profile.Name(), target.PodName, runSuffix)
		configMapName := fmt.Sprintf("%s-%s", profile.Name(), runSuffix)

		// oldReplicas, err := s.ScaleTo(ctx, namespace, name, 0)
		// if err != nil {
		// 	log.Fatal(err)
		// }
		//
		// // Ensure target is scaled back up once this function finishes
		// defer func() {
		// 	oldReplicas, err = s.ScaleTo(ctx, namespace, name, oldReplicas)
		// 	if err != nil {
		// 		log.Fatal(err)
		// 	}
		//
		// 	if oldReplicas != 0 {
		// 		return fmt.Errorf("Expected oldReplicas to be 0 but got %d", oldReplicas)
		// 	}
		// }()
		//
		// // Wait until all pods have stopped
		// err = s.WaitStopped(ctx, namespace, selector)
		// if err != nil {
		// 	return fmt.Errorf("Failed WaitStopped: %s", err)
		// }

		snapshot, err := s.TakeSnapshot(ctx, namespace, snapshotName, pvcName)
		if err != nil {
			return fmt.Errorf("Failed TakeSnapshot: %s", err)
		}
		defer s.DeleteSnapshot(ctx, namespace, snapshotName)

		contentName, err := s.WaitSnapContent(ctx, snapshot)
		if err != nil {
			return fmt.Errorf("Failed to WaitSnapContent: %s", err)
		}
		defer s.DeleteSnapshotContent(ctx, contentName)

		contentHandle, err := s.SnapshotHandleFromContent(ctx, contentName)
		if err != nil {
			return fmt.Errorf("Failed to SnapshotHandleFromContent: %s", err)
		}

		err = s.SnapshotContentFromHandle(ctx, snapshotContentName, contentHandle, snapshotName)
		if err != nil {
			return fmt.Errorf("Failed to SnapshotContentFromHandle: %s", err)
		}
		defer s.DeleteSnapshotContent(ctx, snapshotContentName)

		err = s.SnapshotFromContent(ctx, snapshotName, &snapshotContentName)
		if err != nil {
			return fmt.Errorf("Failed to SnapshotFromContent: %s", err)
		}
		defer s.DeleteSnapshot(ctx, s.config.BackupNamespace, snapshotName)

		err = s.WaitSnapshotReady(ctx, &snapshotName)
		if err != nil {
			return fmt.Errorf("Failed to WaitSnapshotReady: %s", err)
		}

		err = s.PVCFromSnapshot(ctx, snapshotName, backupPVCName, storageSize)
		if err != nil {
			return fmt.Errorf("Failed to PVCFromSnapshot: %s", err)
		}
		defer s.DeletePVC(ctx, backupPVCName)

		profileConfigMap, err := profile.ToConfigMap(repos, s.config.BackupNamespace, configMapName)
		if err != nil {
			return fmt.Errorf("Failed to ToConfigMap: %s", err)
		}
		profileConfigMap.Data["profiles.yaml"] = baseProfile

		err = s.CreateBackupPod(ctx, profileConfigMap, podName, backupPVCName)
		if err != nil {
			return fmt.Errorf("Failed to CreateBackupPod: %s", err)
		}
		s.WaitPod(ctx, podName)
		// defer s.DeletePod(ctx, podName)

	}
	return nil
}

func getK8sConfig(kubeconfig *string) (*rest.Config, error) {
	// Build the config from the kubeconfig file
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubeconfig: %w", err)
	}

	return config, err
}

func InitK8sClient(kubeconfig *string) (*kubernetes.Clientset, error) {
	config, err := getK8sConfig(kubeconfig)
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	return clientset, nil
}

func InitCSIClient(kubeconfig *string) (*csiClientset.Clientset, error) {
	config, err := getK8sConfig(kubeconfig)
	if err != nil {
		return nil, err
	}

	csiClient, err := csiClientset.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create CSI client: %w", err)
	}

	return csiClient, nil
}

func (s *SnapshotClient) ScaleTo(ctx context.Context, namespace string, deploymentName string, replicas int32) (int32, error) {
	scale, err := s.kubeClient.AppsV1().
		Deployments(namespace).
		GetScale(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("Failed to GetScale: %w", err)
	}

	currentReplicas := scale.Spec.Replicas
	s.log.Debug("Got current scale", "namespace", namespace, "deployment", deploymentName, "replicas", currentReplicas)

	scale.Spec.Replicas = replicas

	_, err = s.kubeClient.AppsV1().
		Deployments(namespace).
		UpdateScale(ctx, deploymentName, scale, metav1.UpdateOptions{})

	if err != nil {
		return 0, fmt.Errorf("Failed to ApplyScale: %w", err)
	}
	s.log.Debug("Applied new scale", "namespace", namespace, "deployment", deploymentName, "replicas", replicas)

	return currentReplicas, nil
}

func (s *SnapshotClient) WaitStopped(ctx context.Context, namespace string, selector string) error {
	log := s.log.With("namespace", namespace, "selector", selector)
	ctx, cancel := context.WithTimeout(ctx, s.config.WaitTimeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			log.Error("Timed out waiting for pods to stop")
			return fmt.Errorf("Timeout")
		default:
			pods, err := s.kubeClient.CoreV1().
				Pods(namespace).
				List(ctx, metav1.ListOptions{
					LabelSelector: selector,
				})
			if err != nil {
				return err
			}

			if len(pods.Items) == 0 {
				log.Info("Stopped all pods")
				return nil
			}

			log.Debug("Waiting for pods to stop", "podCount", len(pods.Items))
			time.Sleep(s.config.SleepDuration)

		}
	}
}

func (s *SnapshotClient) TakeSnapshot(ctx context.Context, namespace string, snapshotName string, pvcName string) (*volumesnapshot.VolumeSnapshot, error) {
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
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapshotName,
			Namespace: namespace,
		},
		Spec: volumesnapshot.VolumeSnapshotSpec{
			VolumeSnapshotClassName: &s.config.SnapshotClass,
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

func (s *SnapshotClient) SnapshotFromContent(ctx context.Context, snapshotName string, contentName *string) error {
	_, err := s.csiClient.SnapshotV1().
		VolumeSnapshots(s.config.BackupNamespace).
		Get(ctx, snapshotName, metav1.GetOptions{})

	if err == nil {
		s.log.Info("Snapshot already exists, not creating new one", "namespace", s.config.BackupNamespace, "snapshotName", snapshotName)
		return nil
	}

	if !k8sErrors.IsNotFound(err) {
		s.log.Error("Unexpected error when checking for existing snapshot", "err", err, "namespace", s.config.BackupNamespace, "snapshotName", snapshotName)
		return fmt.Errorf("Error when checking for existing snapshot: %w", err)
	}

	snapshot := volumesnapshot.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapshotName,
			Namespace: s.config.BackupNamespace,
		},
		Spec: volumesnapshot.VolumeSnapshotSpec{
			Source: volumesnapshot.VolumeSnapshotSource{
				VolumeSnapshotContentName: contentName,
			},
		},
	}
	_, err = s.csiClient.SnapshotV1().
		VolumeSnapshots(s.config.BackupNamespace).
		Create(ctx, &snapshot, metav1.CreateOptions{})

	s.log.Info("Created Snapshot from content", "namespace", s.config.BackupNamespace, "snapshotName", snapshotName, "contentName", contentName)

	return err
}

func (s *SnapshotClient) DeleteSnapshot(ctx context.Context, namespace string, snapshotName string) error {
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

func (s *SnapshotClient) SnapshotContentFromHandle(ctx context.Context, snapshotContentName string, snapshotContentHandle string, snapshotName string) error {
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
		ObjectMeta: metav1.ObjectMeta{
			Name: snapshotContentName,
		},
		Spec: volumesnapshot.VolumeSnapshotContentSpec{
			DeletionPolicy:          volumesnapshot.VolumeSnapshotContentRetain,
			Driver:                  s.config.SnapshotDriver,
			VolumeSnapshotClassName: &s.config.SnapshotClass,
			Source: volumesnapshot.VolumeSnapshotContentSource{
				SnapshotHandle: &snapshotContentHandle,
			},
			VolumeSnapshotRef: corev1.ObjectReference{
				Name:      snapshotName,
				Namespace: s.config.BackupNamespace,
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

func (s *SnapshotClient) WaitSnapContent(ctx context.Context, snapshot *volumesnapshot.VolumeSnapshot) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.config.WaitTimeout)
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

			time.Sleep(s.config.SleepDuration)

		}
	}
}

func (s *SnapshotClient) DeleteSnapshotContent(ctx context.Context, snapshotContentName string) error {
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

func (s *SnapshotClient) SnapshotHandleFromContent(ctx context.Context, contentName string) (string, error) {
	for {
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
		time.Sleep(s.config.SleepDuration)
	}
}

func (s *SnapshotClient) WaitSnapshotReady(ctx context.Context, snapshotName *string) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.WaitTimeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			s.log.Error("Timed out waiting for snapshot to be ready")
			return fmt.Errorf("Timeout")

		default:
			snapshot, err := s.csiClient.SnapshotV1().
				VolumeSnapshots(s.config.BackupNamespace).
				Get(ctx, *snapshotName, metav1.GetOptions{})

			if err != nil {
				s.log.Error("Error when waiting for snapshot to be ready", "namespace", s.config.BackupNamespace, "snapshotName", snapshotName, "err", err)
				return fmt.Errorf("Error waiting for snapshot to be ready: %w", err)
			}

			if snapshot.Status != nil && snapshot.Status.ReadyToUse != nil && *snapshot.Status.ReadyToUse {
				s.log.Info("Snapshot is ready to use", "namespace", s.config.BackupNamespace, "snapshotName", *snapshotName)
				return nil
			}

			s.log.Debug("Snapshot is not ready yet", "namespace", s.config.BackupNamespace, "snapshotName", *snapshotName)
			time.Sleep(s.config.SleepDuration)
		}
	}
}

func (s *SnapshotClient) PVCFromSnapshot(ctx context.Context, snapshotName string, pvcName string, storageSize resource.Quantity) error {
	log := s.log.With("namespace", s.config.BackupNamespace, "pvcName", pvcName)
	// Check if PVC already exists
	_, err := s.kubeClient.CoreV1().
		PersistentVolumeClaims(s.config.BackupNamespace).
		Get(ctx, pvcName, metav1.GetOptions{})
	if err == nil {
		log.Info("PVC already exists, not creating new one")
		return nil
	}

	apiGroup := volumesnapshot.GroupName

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: s.config.BackupNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			DataSource: &corev1.TypedLocalObjectReference{
				Name:     snapshotName,
				Kind:     "VolumeSnapshot",
				APIGroup: &apiGroup,
			},
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			StorageClassName: &s.config.SnapshotClass,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: storageSize,
				},
			},
		},
	}

	_, err = s.kubeClient.CoreV1().
		PersistentVolumeClaims(s.config.BackupNamespace).
		Create(ctx, pvc, metav1.CreateOptions{})
	if err != nil {
		log.Error("Error creating PVC", "snapshotName", snapshotName, "err", err)
		return fmt.Errorf("Error creating PVC: %w", err)
	}

	log.Info("Created new PVC", "snapshotName", snapshotName, "err", err)

	ctx, cancel := context.WithTimeout(ctx, s.config.WaitTimeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			s.log.Error("Timed out waiting for PVC to be ready")
			return fmt.Errorf("Timeout")

		default:
			pvc, err = s.kubeClient.CoreV1().
				PersistentVolumeClaims(s.config.BackupNamespace).
				Get(ctx, pvcName, metav1.GetOptions{})
			if err != nil {
				if !k8sErrors.IsNotFound(err) {
					log.Error("Unexpected error when waiting for PVC to be ready")
					return err
				}
				log.Warn("PVC does not exist yet, waiting for it to appear")
				continue
			}

			if pvc.Status.Phase == corev1.ClaimBound {
				log.Info("PVC is ready")
				return nil
			}

			log.Debug("Waiting for PVC to be ready")
			time.Sleep(s.config.SleepDuration)

		}
	}
}

func (s *SnapshotClient) DeletePVC(ctx context.Context, pvcName string) error {
	log := s.log.With("namespace", s.config.BackupNamespace, "pvcName", pvcName)
	pvc, err := s.kubeClient.CoreV1().
		PersistentVolumeClaims(s.config.BackupNamespace).
		Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			log.Warn("PVC does not exist, skipping deletion. Warning: PersistentVolume might still exist and be dangling")
			return nil
		}
		log.Error("Unexpected error when checking if PVC exists", "err", err)
		return err
	}
	pvName := pvc.Spec.VolumeName

	err = s.kubeClient.CoreV1().
		PersistentVolumeClaims(s.config.BackupNamespace).
		Delete(ctx, pvcName, metav1.DeleteOptions{})
	if err != nil {
		s.log.Error("Error deleting PVC", "err", err)
		return err
	}

	log.Info("Deleted PVC")

	err = s.kubeClient.CoreV1().
		PersistentVolumes().
		Delete(ctx, pvName, metav1.DeleteOptions{})
	if err != nil {
		log.Error("Error deleting PV", "pvName", pvName)
	}
	log.Info("Deleted PV", "pvName", pvName)
	return nil
}

func (s *SnapshotClient) CreateBackupPod(ctx context.Context, profileConfigMap *corev1.ConfigMap, podName string, backupPVCName string) error {
	log := s.log.With("namespace", s.config.BackupNamespace, "podName", podName)

	pod, err := s.kubeClient.CoreV1().
		Pods(s.config.BackupNamespace).
		Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		if !k8sErrors.IsNotFound(err) {
			log.Error("Error when checking if pod already exists", "err", err)
			return err
		}
	} else {
		log.Warn("Pod already exists, not creating")
		return nil
	}

	optional := false
	var readWriteMode int32 = 0775

	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: s.config.BackupNamespace,
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: "restic",
			RestartPolicy:      corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:    "resticprofile",
					Image:   s.config.Image,
					Command: []string{"sh"},
					Args:    []string{"/usr/local/bin/backup.sh"},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "storage", MountPath: "/var/lib/grafana"},
						{Name: "restic-cfg", MountPath: "/etc/restic", ReadOnly: true},
						{Name: "restic-script", MountPath: "/usr/local/bin"},
						{Name: "restic-cache", MountPath: "/var/cache/restic"},
						{Name: "nfs-restic-repo", MountPath: "/mnt/kubernetes-restic"},
					},

					Env: []corev1.EnvVar{
						{
							Name: "AWS_ACCESS_KEY_ID",
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "resticprofile-kubernetes",
									},
									Key:      "backblaze-key-id",
									Optional: &optional,
								},
							},
						},
						{
							Name: "AWS_SECRET_ACCESS_KEY",
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "resticprofile-kubernetes",
									},
									Key:      "backblaze-application-key",
									Optional: &optional,
								},
							},
						},
						{
							Name: "RESTIC_PASSWORD",
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "resticprofile-kubernetes",
									},
									Key:      "restic-repo-password",
									Optional: &optional,
								},
							},
						},
					},
					// End EnvVars
				},
			},
			// End Containers

			Volumes: []corev1.Volume{
				{
					Name: "storage",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: backupPVCName,
						},
					},
				},
				{
					Name: "restic-cfg",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "resticprofile-kubernetes-config",
							},
							DefaultMode: &readWriteMode,
						},
					},
				},
				{
					Name: "restic-script",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "resticprofile-kubernetes-script",
							},
							DefaultMode: &readWriteMode,
						},
					},
				},
				{
					Name: "restic-cache",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
				{
					Name: "nfs-restic-repo",
					VolumeSource: corev1.VolumeSource{
						NFS: &corev1.NFSVolumeSource{
							Server: "10.0.20.10",
							Path:   "/mnt/kubernetes-restic",
						},
					},
				},
			},
			// End Volumes

		},
	}

	// Mount all profile's ConfigMap in the container
	for profilePath := range profileConfigMap.Data {
		var mountPath string
		if profilePath == "profiles.yaml" {
			mountPath = "/etc/restic/profiles.yaml"
		} else {
			mountPath = fmt.Sprintf("/etc/restic/profiles.d/%s", profilePath)
		}
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
			MountPath: mountPath,
			Name:      profileConfigMap.Name,
			SubPath:   profilePath,
			ReadOnly:  true,
		})
	}

	// Now add the Volume from the ConfigMap
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: profileConfigMap.Name,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: profileConfigMap.Name,
				},
			},
		},
	})

	_, err = s.kubeClient.CoreV1().
		Pods(s.config.BackupNamespace).
		Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		log.Error("Failed to create pod", "err", err)
		return err
	}

	log.Info("Pod created")
	return nil
}

func (s *SnapshotClient) WaitPod(ctx context.Context, podName string) error {
	log := s.log.With("namespace", s.config.BackupNamespace, "podName", podName)

	// Wait for ContainerCreating to be finished
	// Shorter timeout than the entire run to detect issues early
	createCtx, createCancel := context.WithTimeout(ctx, s.config.PodCreationTimeout)
	defer createCancel()
CreateLoop:
	for {
		select {
		case <-createCtx.Done():
			log.Error("Timed out waiting for pod to enter running state")
			return fmt.Errorf("Timeout")
		default:
			pod, err := s.kubeClient.CoreV1().
				Pods(s.config.BackupNamespace).
				Get(createCtx, podName, metav1.GetOptions{})
			if err != nil {
				if k8sErrors.IsNotFound(err) {
					log.Warn("Pod does not exist yet, waiting for it to be created")
					continue
				}
				log.Error("Error while waiting for pod to start running", "err", err)
				return err
			}

			switch phase := pod.Status.Phase; phase {
			case corev1.PodFailed:
				log.Error("Pod failed, backup may not have succeeded")
				return fmt.Errorf("Backup pod failed")
			case corev1.PodSucceeded:
				log.Info("Backup finished successfully")
				return nil
			case corev1.PodRunning:
				log.Info("Pod has entered running state")
				break CreateLoop

			default:
				log.Debug("Pod is not running yet", "phase", string(phase))
				time.Sleep(s.config.SleepDuration)
			}
		}
	}

	// Wait for pod to be finished running
	// This waits longer to give the actual backup time to finish
	runCtx, runCancel := context.WithTimeout(ctx, s.config.PodWaitTimeout)
	defer runCancel()
	for {
		select {
		case <-runCtx.Done():
			log.Error("Timed out waiting for pod to finish running")
			return fmt.Errorf("Timeout")
		default:
			pod, err := s.kubeClient.CoreV1().
				Pods(s.config.BackupNamespace).
				Get(runCtx, podName, metav1.GetOptions{})
			if err != nil {
				log.Error("Error while waiting for pod to finish running", "err", err)
				return err
			}

			switch phase := pod.Status.Phase; phase {
			case corev1.PodFailed:
				log.Error("Pod failed, backup may not have succeeded")
				return fmt.Errorf("Backup pod failed")
			case corev1.PodSucceeded:
				log.Info("Backup finished successfully")
				return nil

			default:
				log.Debug("Pod is still running", "phase", string(phase))
				time.Sleep(s.config.SleepDuration)
			}
		}
	}
}

func (s *SnapshotClient) DeletePod(ctx context.Context, podName string) error {
	log := s.log.With("namespace", s.config.BackupNamespace, "podName", podName)
	err := s.kubeClient.CoreV1().
		Pods(s.config.BackupNamespace).
		Delete(ctx, podName, metav1.DeleteOptions{})
	if err != nil {
		log.Error("Error deleting pod", "err", err)
		return err
	}
	log.Info("Pod deleted")
	return nil
}
