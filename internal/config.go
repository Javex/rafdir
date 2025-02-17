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
	SnapshotDriver   string
	BackupNamespace  string
	// StorageClass is used for the temporary backup PVC which is different from
	// the default one. It should still be the same underlying provisioner/driver
	// but, it can have different parameters. For example, it might not have any
	// replication (replicas=1) because it's a temporary backup PVC.
	StorageClass       string
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

	profiles, errs := ProfilesFromGlobalConfigMap(configMap)
	if len(errs) > 0 {
		log.Warn("There were errors when loading profiles", "errors", errs)
	}

	if profiles == nil || len(profiles) == 0 {
		log.Error("No valid profiles found, can't continue")
		return nil, fmt.Errorf("No valid profiles found")
	}

	log.Info("Loaded profiles", "profileCount", len(profiles))

	return &Config{
		GlobalConfigFile:   globalConfigFile,
		SnapshotClass:      "longhorn",
		SnapshotDriver:     "driver.longhorn.io",
		BackupNamespace:    backupNamespace,
		StorageClass:       "rafdir",
		SleepDuration:      1 * time.Second,
		WaitTimeout:        10 * time.Second,
		PodCreationTimeout: 1 * time.Minute,
		PodWaitTimeout:     5 * time.Minute,
		Image:              "ghcr.io/javex/rafdir:0.29.0",

		Profiles:     profiles,
		Repositories: repositories,
	}, nil
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
