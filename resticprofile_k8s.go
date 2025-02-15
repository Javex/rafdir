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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

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

		podName := fmt.Sprintf("%s-%s-%s", profile.Name, target.PodName, runSuffix)
		configMapName := fmt.Sprintf("%s-%s", profile.Name, runSuffix)

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

		snapshotter := internal.NewPvcSnapshotter(log, s.kubeClient, s.csiClient, internal.PvcSnapshotterConfig{
			DestNamespace: s.config.BackupNamespace,
			RunSuffix:     runSuffix,
			SnapshotClass: s.config.SnapshotClass,
			StorageClass:  s.config.StorageClass,
			WaitTimeout:   s.config.WaitTimeout,
			SleepDuration: s.config.SleepDuration,
		})
		// Schedule cleanup before kicking off the resource creation. If no
		// resources end up being created this does nothing.
		defer snapshotter.Cleanup(ctx)

		backupPvc, err := snapshotter.BackupPvcFromSourcePvc(ctx, target.Pvc)
		if err != nil {
			return fmt.Errorf("Failed to BackupPvcFromSourcePvc: %s", err)
		}

		profileConfigMap, err := profile.ToConfigMap(repos, s.config.BackupNamespace, configMapName)
		if err != nil {
			return fmt.Errorf("Failed to ToConfigMap: %s", err)
		}
		profileConfigMap.Data["profiles.yaml"] = baseProfile

		err = s.CreateConfigMap(ctx, profileConfigMap)
		if err != nil {
			return fmt.Errorf("Failed to CreateConfigMap: %s", err)
		}
		defer s.DeleteConfigMap(ctx, profileConfigMap.Name)

		err = s.CreateBackupPod(ctx, profileConfigMap, podName, backupPvc.Name)
		if err != nil {
			return fmt.Errorf("Failed to CreateBackupPod: %s", err)
		}
		s.WaitPod(ctx, podName)
		defer s.DeletePod(ctx, podName)

	}
	return nil
}

func GetK8sConfig(kubeconfig *string) (*rest.Config, error) {
	// Build the config from the kubeconfig file
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubeconfig: %w", err)
	}

	return config, err
}

func InitK8sClient(config *rest.Config) (*kubernetes.Clientset, error) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	return clientset, nil
}

func InitCSIClient(config *rest.Config) (*csiClientset.Clientset, error) {
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

func (s *SnapshotClient) CreateConfigMap(ctx context.Context, configMap *corev1.ConfigMap) error {
	log := s.log.With("namespace", s.config.BackupNamespace, "configMap", configMap.Name)
	_, err := s.kubeClient.CoreV1().
		ConfigMaps(s.config.BackupNamespace).
		Create(ctx, configMap, metav1.CreateOptions{})
	if err != nil {
		log.Error("Error creating ConfigMap", "err", err)
		return err
	}
	log.Info("Created ConfigMap")
	return nil
}

func (s *SnapshotClient) DeleteConfigMap(ctx context.Context, configMapName string) error {
	log := s.log.With("namespace", s.config.BackupNamespace, "configMap", configMapName)
	err := s.kubeClient.CoreV1().
		ConfigMaps(s.config.BackupNamespace).
		Delete(ctx, configMapName, metav1.DeleteOptions{})
	if err != nil {
		log.Error("Error deleting ConfigMap", "err", err)
		return err
	}
	log.Info("Deleted ConfigMap")
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
