package internal

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

type RepositoryName string

func RepositoriesFromConfigMap(configMap *corev1.ConfigMap) ([]RepositoryName, error) {
	// Get the keys out of the first level of yaml data, ignoring any fields
	// inside, only getting the key names.
	repoYaml, ok := configMap.Data["repositories"]
	if !ok {
		return nil, fmt.Errorf("Key `repositories` not found in configmap %s", configMap.Name)
	}
	if repoYaml == "" {
		return nil, fmt.Errorf("Key `repositories` is empty %s", configMap.Name)
	}

	// Load yaml into map with arbitrary values, string keys
	repoList := make(map[string]any)
	err := yaml.Unmarshal([]byte(repoYaml), &repoList)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal repositories: %v", err)
	}

	repositories := make([]RepositoryName, 0, len(repoList))
	for repository := range repoList {
		repositories = append(repositories, RepositoryName(repository))
	}

	return repositories, nil
}
