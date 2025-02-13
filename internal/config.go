package internal

import (
	"context"
	"fmt"
	"log/slog"
	"text/template"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Config struct {
	GlobalConfigFile   string
	SnapshotClass      string
	SnapshotDriver     string
	BackupNamespace    string
	SleepDuration      time.Duration
	WaitTimeout        time.Duration
	PodCreationTimeout time.Duration
	PodWaitTimeout     time.Duration
	Image              string

	Profiles     map[string]Profile
	Repositories []Repository
}

func LoadConfigFromKubernetes(ctx context.Context, log *slog.Logger, kubeClient kubernetes.Interface, namespace string, configMapName string) (*Config, error) {
	cm, err := kubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("Failed to get global configmap %s: %w", configMapName, err)
	}
	log.Info("Loaded global ConfigMap")
	return NewConfigFromConfigMap(log, namespace, cm)
}

func NewConfigFromConfigMap(log *slog.Logger, backupNamespace string, configMap *corev1.ConfigMap) (*Config, error) {
	globalConfigFile, ok := configMap.Data["profiles.yaml"]
	if !ok {
		return nil, fmt.Errorf("ConfigMap %s has no key `profiles.yaml`", configMap.Name)
	}

	repositories, err := RepositoriesFromConfigMap(configMap)
	if err != nil {
		return nil, err
	}
	log.Info("Loaded repositories")

	profiles, err := ProfilesFromGlobalConfigMap(configMap)
	if err != nil {
		return nil, err
	}
	log.Info("Loaded profiles")

	return &Config{
		GlobalConfigFile:   globalConfigFile,
		SnapshotClass:      "longhorn",
		SnapshotDriver:     "driver.longhorn.io",
		BackupNamespace:    backupNamespace,
		SleepDuration:      1 * time.Second,
		WaitTimeout:        10 * time.Second,
		PodCreationTimeout: 1 * time.Minute,
		PodWaitTimeout:     5 * time.Minute,
		Image:              "ghcr.io/javex/resticprofile-kubernetes:0.29.0",

		Profiles:     profiles,
		Repositories: repositories,
	}, nil
}
