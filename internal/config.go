package internal

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"text/template"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Config struct {
	GlobalConfigFile string
	SnapshotClass    string
	BackupNamespace  string
	// DefaultStorageClass is used for the temporary backup PVC which is different from
	// the default one. It should still be the same underlying provisioner/driver
	// but, it can have different parameters. For example, it might not have any
	// replication (replicas=1) because it's a temporary backup PVC.
	DefaultStorageClass string
	// SleepDuration is the time to wait between API calls to the cluster when
	// checking if an operation has completed, so it determines the polling
	// frequency. It should be a low number, but not so low as to overwhelm the
	// cluster.
	SleepDuration time.Duration
	// WaitTimeout is a brief period of time to wait for most activities
	// performed against the cluster such as object creation.
	WaitTimeout time.Duration
	// SnapshotContentTimeout is the maximum time to wait for the snapshot
	// content to be ready for a snapshot. This is when data is actually handled
	// and storage providers have to do some work so it could be a bit longer
	// than regular cluster activities.
	SnapshotContentTimeout time.Duration
	// PodCreationTimeout is a longer duration that gives more time to the
	// cluster for pods to actually start. This can take a while if storage
	// providers need to get stuff ready at this stage.
	PodCreationTimeout time.Duration
	// PodWaitTimeout is the maximum time to wait for a pod to actually perform
	// the backup. This is the task that could take the longest because it
	// actually backs data up.
	PodWaitTimeout time.Duration
	Image          string

	Profiles     map[string]Profile
	Repositories []Repository
}

func LoadConfigFromKubernetes(ctx context.Context, log *slog.Logger, kubeClient kubernetes.Interface, namespace string, configMapName string, profileFilter, repoFilter, imageTag string) (*Config, error) {
	cm, err := kubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("Failed to get global configmap %s: %w", configMapName, err)
	}
	log.Info("Loaded global ConfigMap")
	return NewConfigFromConfigMap(log, namespace, cm, profileFilter, repoFilter, imageTag)
}

func NewConfigFromConfigMap(log *slog.Logger, backupNamespace string, configMap *corev1.ConfigMap, profileFilter, repoFilter, imageTag string) (*Config, error) {
	globalConfigFile, ok := configMap.Data["profiles.yaml"]
	if !ok {
		return nil, fmt.Errorf("ConfigMap %s has no key `profiles.yaml`", configMap.Name)
	}

	repositories, err := RepositoriesFromConfigMap(configMap, repoFilter)
	if err != nil {
		return nil, err
	}
	log.Info("Loaded repositories")

	defaultStorageClass, ok := configMap.Data["defaultStorageClass"]
	if !ok {
		return nil, fmt.Errorf("ConfigMap %s has no key `defaultStorageClass`", configMap.Name)
	}

	image := fmt.Sprintf("ghcr.io/javex/rafdir:%s", imageTag)
	log.Info("Using image", "image", image)

	config := &Config{
		GlobalConfigFile:       globalConfigFile,
		SnapshotClass:          "longhorn",
		BackupNamespace:        backupNamespace,
		DefaultStorageClass:    defaultStorageClass,
		SleepDuration:          1 * time.Second,
		WaitTimeout:            10 * time.Second,
		SnapshotContentTimeout: 5 * time.Minute,
		PodCreationTimeout:     10 * time.Minute,
		PodWaitTimeout:         20 * time.Minute,
		Image:                  image,

		Repositories: repositories,
	}

	profiles, errs := ProfilesFromGlobalConfigMap(config, configMap, profileFilter)
	if len(errs) > 0 {
		log.Warn("There were errors when loading profiles", "errors", errs)
	}

	if profiles == nil || len(profiles) == 0 {
		log.Error("No valid profiles found, can't continue")
		return nil, fmt.Errorf("No valid profiles found")
	}

	log.Info("Loaded profiles", "profileCount", len(profiles))
	config.Profiles = profiles

	return config, nil
}

func nindent(n int, s string) string {
	return strings.ReplaceAll(s, "\n", "\n"+strings.Repeat(" ", n))
}

func trim(s string) string {
	return strings.TrimSpace(s)
}

var baseProfileTemplate = template.Must(
	template.
		New("baseProfile").
		Funcs(template.FuncMap{
			"nindent": nindent,
			"trim":    trim,
		}).
		Parse(`{{ .GlobalConfigFile | trim }}
{{ range .Repositories }}
{{- .Name }}:
  {{ .ProfileYaml | nindent 2 }}
{{ end -}}
`))

// BaseProfile returns the contents of the base profile that gets written to
// /etc/restic/profiles.yaml
func (c *Config) BaseProfile() (string, error) {
	templateBuffer := new(bytes.Buffer)
	err := baseProfileTemplate.Execute(templateBuffer, c)
	if err != nil {
		return "", fmt.Errorf("Failed to execute base profile template: %w", err)
	}
	return templateBuffer.String(), nil
}
