package internal

import (
	"fmt"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
)

type Config struct {
	GlobalConfigFile string

	Profiles     map[string]Profile
	Repositories []RepositoryName
}

func NewConfigFromConfigMap(log *slog.Logger, configMap *corev1.ConfigMap) (*Config, error) {
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
		GlobalConfigFile: globalConfigFile,
		Profiles:         profiles,
		Repositories:     repositories,
	}, nil
}
