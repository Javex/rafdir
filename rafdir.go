package rafdir

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"rafdir/internal"
	"rafdir/internal/cli"
	"rafdir/internal/client"
	"rafdir/internal/db"
	"rafdir/internal/meta"
	"rafdir/internal/pvc"
	"strings"
	"syscall"
	"time"

	// apiv1 "k8s.io/api/core/v1"
	corev1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/tracelog"
	csiClientset "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
)

type SnapshotClientConfig struct {
	Namespace         string
	ConfigMapName     string
	LogLevel          string
	ProfileFilter     string
	RepoFilter        string
	ImageTag          string
	SkipIntervalCheck bool

	DbUrl      string
	SecretName string
}

func (s *SnapshotClientConfig) Build(ctx context.Context) (*SnapshotClient, error) {

	var logLevel slog.Level
	var dbLogLevel tracelog.LogLevel
	switch l := strings.ToLower(s.LogLevel); l {
	case "trace":
		logLevel = slog.LevelDebug
		dbLogLevel = tracelog.LogLevelTrace
	case "debug":
		logLevel = slog.LevelDebug
		dbLogLevel = tracelog.LogLevelDebug
	case "info":
		logLevel = slog.LevelInfo
		dbLogLevel = tracelog.LogLevelInfo
	case "warn":
		logLevel = slog.LevelWarn
		dbLogLevel = tracelog.LogLevelWarn
	case "error":
		logLevel = slog.LevelError
		dbLogLevel = tracelog.LogLevelError
	default:
		return nil, fmt.Errorf("Invalid log level %s", s.LogLevel)
	}
	cli.InitLogging(logLevel)

	log := slog.Default()

	var kubeConfig *rest.Config
	if cfg, err := rest.InClusterConfig(); err == nil {
		kubeConfig = cfg
	} else {
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			return nil, fmt.Errorf("Missing env var KUBECONFIG")
		}

		cfg, err := client.GetK8sConfig(kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("Failed to get k8s config: %s", err)
		}

		kubeConfig = cfg
	}

	kubeClient, err := client.InitK8sClient(kubeConfig)
	if err != nil {
		return nil, fmt.Errorf("Failed to create k8s client: %s", err)
	}
	csiClient, err := client.InitCSIClient(kubeConfig)
	if err != nil {
		return nil, fmt.Errorf("Failed to create csi client: %s", err)
	}

	config, err := internal.LoadConfigFromKubernetes(
		ctx,
		log,
		kubeClient,
		s.Namespace,
		s.ConfigMapName,
		s.ProfileFilter,
		s.RepoFilter,
		s.ImageTag,
	)
	if err != nil {
		return nil, fmt.Errorf("Failed to load global configmap: %s", err)
	}

	if s.DbUrl == "" {
		return nil, fmt.Errorf("Missing DbUrl in config")
	}

	if s.SecretName == "" {
		return nil, fmt.Errorf("Missing SecretName in config")
	}

	dbConfig, err := pgx.ParseConfig(s.DbUrl)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse database url: %s", err)
	}

	dbConfig.Tracer = &tracelog.TraceLog{
		Logger: &DbLogger{
			log: log.With("component", "db"),
		},
		LogLevel: dbLogLevel,
	}

	dbConn, err := pgx.ConnectConfig(ctx, dbConfig)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to database: %s", err)
	}

	if kubeConfig == nil {
		return nil, fmt.Errorf("kubeConfig is nil, must be set")
	}

	client := &SnapshotClient{
		kubeClient:        kubeClient,
		kubeConfig:        kubeConfig,
		csiClient:         csiClient,
		db:                dbConn,
		queries:           db.New(dbConn),
		config:            config,
		log:               log,
		skipIntervalCheck: s.SkipIntervalCheck,
		secretName:        s.SecretName,
	}

	return client, err
}

type DbLogger struct {
	log *slog.Logger
}

func (l *DbLogger) Log(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]any) {
	args := make([]any, 0, len(data)*2)
	for k, v := range data {
		args = append(args, k, v)
	}
	var slogLevel slog.Level
	switch level {
	case tracelog.LogLevelTrace:
		slogLevel = slog.LevelDebug
	case tracelog.LogLevelDebug:
		slogLevel = slog.LevelDebug
	case tracelog.LogLevelInfo:
		slogLevel = slog.LevelInfo
	case tracelog.LogLevelWarn:
		slogLevel = slog.LevelWarn
	case tracelog.LogLevelError:
		slogLevel = slog.LevelError
	case tracelog.LogLevelNone:
		slogLevel = slog.LevelError // No logging
	default:
		slogLevel = slog.LevelError // Default to error for unknown levels
	}
	l.log.Log(ctx, slogLevel, msg, args...)
}

type SnapshotClient struct {
	kubeClient        kubernetes.Interface
	kubeConfig        *rest.Config
	csiClient         csiClientset.Interface
	db                *pgx.Conn
	queries           *db.Queries
	config            *internal.Config
	log               *slog.Logger
	skipIntervalCheck bool
	secretName        string
}

func (s *SnapshotClient) Close(ctx context.Context) {
	err := s.db.Close(ctx)
	if err != nil {
		s.log.Error("Failed to close database connection", "err", err)
	}
	// It holds a reference to the database connection, so we need to close it
	s.queries = nil
}

func (s *SnapshotClient) TakeBackup(ctx context.Context) []error {
	log := s.log
	log.Info("Starting backup run")
	config := s.config
	profiles := config.Profiles

	baseProfile, err := s.config.BaseProfile()
	if err != nil {
		return []error{fmt.Errorf("Failed to render base profile: %s", err)}
	}

	dbProfiles, err := s.queries.ListProfiles(ctx)
	if err != nil {
		return []error{fmt.Errorf("Failed to list profiles from database: %s", err)}
	}

	dbProfileMap := make(map[string]db.Profile)
	for _, profile := range dbProfiles {
		dbProfileMap[profile.Profile] = profile
	}

	errors := make([]error, 0)
	for _, profile := range profiles {
		// Check if profile exists in db, if not create a new object
		dbProfile, profileExistsInDb := dbProfileMap[profile.Name]
		if profileExistsInDb {

			// If the backup was run recently, skip this profile (unless check is disabled by flag)
			now := time.Now().UTC()
			sinceLastCompletion := now.Sub(dbProfile.Lastcompletion.Time)
			log.Debug("Checking last completion time for profile", "profile", profile.Name, "lastCompletion", dbProfile.Lastcompletion.Time, "sinceLastCompletion", sinceLastCompletion, "now", now)
			if sinceLastCompletion < s.config.MinBackupInterval {
				if s.skipIntervalCheck {
					log.Info("Skipping interval check and running backup anyway", "profile", profile.Name, "lastCompletion", dbProfile.Lastcompletion.Time, "sinceLastCompletion", sinceLastCompletion, "now", now)
				} else {
					log.Info("Skipping profile backup, already completed within last 24 hours", "profile", profile.Name, "lastCompletion", dbProfile.Lastcompletion.Time, "sinceLastCompletion", sinceLastCompletion, "now", now)
					continue
				}
			}
		}

		err = s.profileBackup(ctx, &profile, baseProfile)
		if err != nil {
			errors = append(errors, err)
		}

		// Only update backup timestamp if the backup was successful
		if err == nil {
			// Backup has finished, store the latest completion time in the database
			backupFinishTime := time.Now().UTC()
			dbBackupFinishTime := pgtype.Timestamp{
				Time:  backupFinishTime,
				Valid: true,
			}

			if !profileExistsInDb {
				_, err := s.queries.CreateProfile(ctx, db.CreateProfileParams{
					Profile:        profile.Name,
					Lastcompletion: dbBackupFinishTime,
				})
				if err != nil {
					errors = append(errors, fmt.Errorf("Failed to create profile in database: %s", err))
				} else {
					log.Info("Created new profile in database", "profile", profile.Name)
				}
			} else {
				// Update the last completion time of the profile
				err := s.queries.UpdateProfile(ctx, db.UpdateProfileParams{
					Profile:        dbProfile.Profile,
					Lastcompletion: dbBackupFinishTime,
				})
				if err != nil {
					errors = append(errors, fmt.Errorf("Failed to update profile in database: %s", err))
				} else {
					log.Info("Updated profile in database", "profile", profile.Name, "lastCompletion", dbProfile.Lastcompletion, "backupFinishTime", backupFinishTime)
				}
			}
		}
	}

	log.Info("Backup run finished")
	return errors
}

func (s *SnapshotClient) profileBackup(ctx context.Context, profile *internal.Profile, baseProfile string) error {
	log := s.log.With("profile", profile.Name)

	// Handle keyboard interrupts and SIGTERM and perform cleanup on any interruption
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	var cleanup []func()
	defer func() {
		log.Info("Cleaning up resources")

		for _, f := range cleanup {
			f()
		}
	}()

	// Suffix to apply to all resources managed by this run. Existing resources
	// will be skipped to create an idempotent run. Resources will be deleted
	// when they are no longer needed.
	runSuffix := generateRunSuffix()
	config := s.config
	repos := config.Repositories
	target, err := profile.BackupTarget(ctx, log, s.kubeClient, s.kubeConfig, runSuffix)
	if err != nil {
		return fmt.Errorf("Failed to BackupTarget: %s", err)
	}

	podName := target.PodName()
	configMapName := fmt.Sprintf("%s-%s", profile.Name, runSuffix)

	if profile.CommandBefore.Cmd != "" {
		podTarget, ok := target.(*internal.PodBackupTarget)
		if !ok {
			log.Error("CommandBefore requires a PodBackupTarget")
			return fmt.Errorf("CommandBefore requires a PodBackupTarget")
		}
		err := podTarget.CommandBefore(ctx, log, s.kubeClient)
		if err != nil {
			log.Error("Failed to run CommandBefore", "err", err)
			return fmt.Errorf("error running CommandBefore: %w", err)
		}
	}

	var scaleUp func()
	if profile.Stop {
		podTarget, ok := target.(*internal.PodBackupTarget)
		if !ok {
			log.Error("Stop requires a PodBackupTarget")
			return fmt.Errorf("Stop requires a PodBackupTarget")
		}
		namespace := podTarget.Namespace

		oldReplicas, err := s.ScaleTo(ctx, namespace, profile.Deployment, 0)
		if err != nil {
			log.Error("Error scaling down deployment", "err", err)
			return fmt.Errorf("Failed to ScaleTo: %s", err)
		}

		// Create callback to scale deployment back up once snapshot has been
		// taken.
		scaleUp = func() {
			oldReplicas, err = s.ScaleTo(ctx, namespace, profile.Deployment, oldReplicas)
			if err != nil {
				log.Error("Failed scale replicas back up", "err", err, "deploymentName", profile.Deployment, "namespace", namespace)
				return
			}

			if oldReplicas != 0 {
				log.Error("Unexpected non-zero old replica count", "err", err, "deploymentName", profile.Deployment, "namespace", namespace, "oldReplicas", oldReplicas)
				return
			}
		}

		// Wait until all pods have stopped
		err = s.WaitStopped(ctx, namespace, podTarget.Selector)
		if err != nil {
			log.Error("Error waiting for pods to stop", "err", err, "deploymentName", profile.Deployment, "namespace", namespace)
			return fmt.Errorf("Failed WaitStopped: %s", err)
		}

		log.Info("Deployment scaled down", "deploymentName", profile.Deployment, "namespace", namespace)
	}

	backupPod := s.NewBackupPod(podName, runSuffix)

	// Add label with profile name to backup pod
	backupPod.Labels["rafdir/profile"] = profile.Name

	if profile.StdInCommand != "" {
		// TODO: This logic should not be here but inside profile. Currently the
		// Profile.StdInTarget function is a good place to handle this, but the
		// other targets are created by Profile.BackupTarget. It would probably be
		// best if Profile handled targets internally, then it could dynamically
		// decide which target to return here. Right now there's logic to throw an
		// error and we need to avoid causing it by putting a similar check here.
		var stdInTarget *internal.PodBackupTarget
		if profile.StdInSelector != "" {
			stdInTarget, err = profile.StdInTarget(ctx, s.kubeClient, s.kubeConfig, runSuffix)
			if err != nil {
				log.Error("Failed to get StdInTarget", "err", err)
				return fmt.Errorf("Failed to StdInTarget: %s", err)
			}
		} else {
			var ok bool
			stdInTarget, ok = target.(*internal.PodBackupTarget)
			if !ok {
				log.Error("StdInCommand requires a PodBackupTarget")
				return fmt.Errorf("StdInCommand requires a PodBackupTarget")
			}
		}
		AddStdInCommandArgs(backupPod, profile, stdInTarget.Pod.Name)
	}

	// Only take a snapshot backup if there are folders and it's not a host mount
	// backup
	if len(profile.Folders) > 0 && profile.Node == "" {
		podTarget, ok := target.(*internal.PodBackupTarget)
		if !ok {
			log.Error("Folders without Node requires a PodBackupTarget")
			return fmt.Errorf("Folders without Node requires a PodBackupTarget")
		}

		// In the current implementation the scale up happens when the first
		// folder's snapshot has been created. That would mean if there's multiple
		// folders (thus PVCs) to take snapshots of, then it would scale up after
		// the first folder. The next snapshot would be taken while the service is
		// running.
		// The current implementation tries to speed things up by doing the scale
		// up as early as possible: When the snapshot has succeeded, it does not
		// need to wait for the new PVC to exist, as that's independent. However,
		// the current loop does snapshots one-by-one so there's a conflict: Either
		// take all snapshots and leave the service down for longer, then scale up
		// at the end. Or throw this error and get the service back faster.
		// A better implementation in the future might take snapshots in parallel
		// and scale everything back up once they're all taken.
		if len(profile.Folders) > 1 && scaleUp != nil {
			log.Error("Stop is not supported for multiple folders")
			return fmt.Errorf("stop and multiple folders are not supported")
		}

		// Get the mapping of folders to volume information
		folderToVolumeInfo, err := podTarget.GetFolderToVolumeMapping(ctx, log, s.kubeClient)
		if err != nil {
			return fmt.Errorf("Failed to GetFolderToVolumeMapping: %s", err)
		}

		// Validate that all folders have corresponding volumes
		for _, folder := range profile.Folders {
			if _, exists := folderToVolumeInfo[folder]; !exists {
				return fmt.Errorf("No volume found for folder %s", folder)
			}
		}

		// Create snapshots for each folder
		for _, folder := range profile.Folders {
			volumeInfo := folderToVolumeInfo[folder]

			// Verify the mount path matches the folder
			if volumeInfo.VolumeMount.MountPath != folder {
				return fmt.Errorf("VolumeMount mount path %s does not match profile folder %s", volumeInfo.VolumeMount.MountPath, folder)
			}

			switch volumeInfo.Type {
			case internal.VolumeTypePVC:
				// Find out which backup method to use for this PVC
				backupMethod, err := pvc.BackupPvcMethodForPvc(ctx, log, s.kubeClient, volumeInfo.PVC)
				if err != nil {
					return fmt.Errorf("failed to determine backup method: %w", err)
				}

				// Create the right object based on the chosen method
				var pvcBackup pvc.BackupPvc
				switch backupMethod {
				case pvc.BackupPvcMethodClone:
					if scaleUp != nil {
						log.Error("pvcloner called with stop argument, not implemented yet")
						return fmt.Errorf("stop not implemented for PV cloner yet")
					}
					log.Info("Using cloner for backup of PVC", "pvcName", volumeInfo.PVC.Name)
					// Handle PVC volume with cloner
					pvcBackup = pvc.NewPvCloner(log, s.kubeClient, pvc.PvClonerConfig{
						DestNamespace: s.config.BackupNamespace,
						RunSuffix:     runSuffix,
						WaitTimeout:   s.config.WaitTimeout,
						SleepDuration: s.config.SleepDuration,
					})
				case pvc.BackupPvcMethodSnapshot:
					log.Info("Using snapshotter for backup of PVC", "pvcName", volumeInfo.PVC.Name)
					// Handle PVC volume with snapshotter
					pvcBackup = pvc.NewPvcSnapshotter(log, s.kubeClient, s.csiClient, pvc.PvcSnapshotterConfig{
						DestNamespace:          s.config.BackupNamespace,
						RunSuffix:              runSuffix,
						SnapshotClass:          profile.SnapshotClass,
						StorageClass:           profile.StorageClass,
						ScaleUp:                scaleUp,
						WaitTimeout:            s.config.WaitTimeout,
						SnapshotContentTimeout: s.config.SnapshotContentTimeout,
						SleepDuration:          s.config.SleepDuration,
					})
				default:
					return fmt.Errorf("unexpected backup method: '%s'", backupMethod)
				}

				// Create & add the PVC with backup data

				// Schedule cleanup before kicking off the resource creation. If no
				// resources end up being created this does nothing.
				cleanup = append(cleanup, func() { pvcBackup.Cleanup(ctx) })

				backupPvc, err := pvcBackup.BackupPvcFromSourcePvc(ctx, volumeInfo.PVC)
				if err != nil {
					log.Error("Failed to create backup PVC from source PVC", "err", err, "folder", folder, "pvcName", volumeInfo.PVC.Name)
					return fmt.Errorf("Failed to BackupPvcFromSourcePvc for %s: %w", folder, err)
				}
				s.AddPvcToPod(backupPod, volumeInfo.VolumeMount, backupPvc.Name)

			case internal.VolumeTypeNFS:
				// Handle NFS volumes directly (no snapshot needed)
				s.AddNfsToPod(backupPod, volumeInfo.VolumeMount, volumeInfo.NFS)

			default:
				return fmt.Errorf("Unsupported volume type %s for folder %s", volumeInfo.Type, folder)
			}
		}
	}

	// Backing up a host volume on a node
	if len(profile.Folders) > 0 && profile.Node != "" {
		nodeTarget, ok := target.(*internal.NodeBackupTarget)
		if !ok {
			log.Error("Folders without Node requires a NodeBackupTarget")
			return fmt.Errorf("Folders without Node requires a NodeBackupTarget")
		}

		nodeTarget.AddNodeVolumeToPod(ctx, backupPod)
	}

	profileConfigMap, err := profile.ToConfigMap(repos, s.config.BackupNamespace, configMapName, runSuffix)
	if err != nil {
		return fmt.Errorf("Failed to ToConfigMap: %s", err)
	}
	profileConfigMap.Data["profiles.yaml"] = baseProfile

	err = s.CreateConfigMap(ctx, profileConfigMap)
	if err != nil {
		return fmt.Errorf("Failed to CreateConfigMap: %s", err)
	}
	cleanup = append(cleanup, func() { s.DeleteConfigMap(context.Background(), profileConfigMap.Name) })

	err = s.CreateBackupPod(ctx, profileConfigMap, backupPod)
	if err != nil {
		return fmt.Errorf("Failed to CreateBackupPod: %s", err)
	}

	err = s.WaitPod(ctx, podName)
	if err != nil {
		return fmt.Errorf("Failed to WaitPod: %s", err)
	}

	// Only delete pod if the backup was successful, otherwise leave it for
	// debugging
	s.DeletePod(ctx, podName)

	if profile.CommandAfter.Cmd != "" {
		podTarget, ok := target.(*internal.PodBackupTarget)
		if !ok {
			log.Error("CommandAfter requires a PodBackupTarget")
			return fmt.Errorf("CommandAfter requires a PodBackupTarget")
		}
		err := podTarget.CommandAfter(ctx, log, s.kubeClient)
		if err != nil {
			log.Error("Failed to run CommandAfter", "err", err)
			return fmt.Errorf("error running CommandAfter: %w", err)
		}
	}

	return nil
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

func (s *SnapshotClient) NewBackupPod(podName, runSuffix string) *corev1.Pod {
	optional := false
	pod := &corev1.Pod{
		ObjectMeta: meta.NewObjectMeta(podName, s.config.BackupNamespace, runSuffix),
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:            "resticprofile",
					Image:           s.config.Image,
					ImagePullPolicy: corev1.PullAlways,
					Command:         []string{"/usr/bin/rafdir-backup"},
					// Enable this argument to debug a backup pod. The backup will run
					// until the end and then pause, printing whether there was an error
					// or not.
					// Args: []string{"--pause"},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "cache", MountPath: "/var/cache/restic"},
						{Name: "nfs-restic-repo", MountPath: "/mnt/restic-repo"},
					},

					Env: []corev1.EnvVar{
						{
							Name: "AWS_ACCESS_KEY_ID",
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: s.secretName,
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
										Name: s.secretName,
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
										Name: s.secretName,
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
					Name: "cache",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: "/var/cache/restic",
						},
					},
				},
				{
					Name: "nfs-restic-repo",
					VolumeSource: corev1.VolumeSource{
						NFS: &corev1.NFSVolumeSource{
							Server: "10.0.20.10",
							Path:   "/mnt/restic-repo",
						},
					},
				},
			},
			// End Volumes

		},
	}

	return pod
}

func AddStdInCommandArgs(pod *corev1.Pod, profile *internal.Profile, stdinPod string) {
	pod.Spec.ServiceAccountName = "rafdir-backup"

	args := []string{
		"--stdin-pod",
		stdinPod,
		"--stdin-namespace",
		profile.StdInCommandNamespace(),
		"--stdin-command",
		profile.StdInCommand,
	}

	if path := profile.StdInFilepath(); path != "" {
		args = append(args, "--stdin-filepath", path)
	}

	// Append all arguments to pod spec
	pod.Spec.Containers[0].Args = append(pod.Spec.Containers[0].Args, args...)
}

func (s *SnapshotClient) AddPvcToPod(pod *corev1.Pod, volumeMount *corev1.VolumeMount, sourcePvcName string) {
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, *volumeMount)
	// pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
	// 	// TODO: Fix
	// 	Name:      "storage",
	// 	MountPath: "/var/lib/grafana",
	// })

	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: volumeMount.Name,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: sourcePvcName,
			},
		},
	})
}

func (s *SnapshotClient) AddNfsToPod(pod *corev1.Pod, volumeMount *corev1.VolumeMount, nfsSource *corev1.NFSVolumeSource) {
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, *volumeMount)

	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: volumeMount.Name,
		VolumeSource: corev1.VolumeSource{
			NFS: nfsSource,
		},
	})
}

func (s *SnapshotClient) CreateBackupPod(ctx context.Context, profileConfigMap *corev1.ConfigMap, pod *corev1.Pod) error {
	log := s.log.With("namespace", s.config.BackupNamespace, "podName", pod.Name)

	_, err := s.kubeClient.CoreV1().
		Pods(s.config.BackupNamespace).
		Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		if !k8sErrors.IsNotFound(err) {
			log.Error("Error when checking if pod already exists", "err", err)
			return err
		}
	} else {
		log.Warn("Pod already exists, not creating")
		return nil
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

const charset = "abcdefghijklmnopqrstuvwxyz0123456789"

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

func generateRunSuffix() string {
	length := 6
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}
