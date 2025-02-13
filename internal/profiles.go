package internal

import (
	"bytes"
	"fmt"
	"text/template"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// A resticprofile profile in TOML format based on the `Profile` struct
var tomlTemplate = template.Must(template.New("tomlTemplate").Parse(`
[{{ .Name }}]
  inherit = "{{ .Inherit }}"
  [{{ .Name }}.backup]
    tag = ["{{ .Tag }}"]
    source = [
    {{ range .Folders -}}
      "{{ . }}",
    {{ end -}}
    ]
    host = "{{ .Host }}"
  [{{ .Name }}.snapshots]
    tag = ["{{ .Tag }}"]
    host = "{{ .Host }}"
`))

type Profile struct {
	Namespace  string   `json:"namespace"`
	Deployment string   `json:"deployment"`
	Stop       bool     `json:"stop"`
	Host       string   `json:"host"`
	Folders    []string `json:"folders"`

	name string
}

func ProfilesFromGlobalConfigMap(globalConfigMap *corev1.ConfigMap) (map[string]Profile, error) {
	if globalConfigMap.Data == nil {
		return nil, fmt.Errorf("ConfigMap %s has no data", globalConfigMap.Name)
	}

	profilesString, ok := globalConfigMap.Data["profiles"]
	if !ok {
		return nil, fmt.Errorf("ConfigMap %s has no key `profiles`", globalConfigMap.Name)
	}

	if profilesString == "" {
		return nil, fmt.Errorf("ConfigMap %s has empty key `profiles`", globalConfigMap.Name)
	}

	return ProfilesFromYamlString(profilesString)
}

func ProfilesFromYamlString(profilesString string) (map[string]Profile, error) {
	profiles := make(map[string]Profile)

	// Parse the yaml
	unmarshalErr := yaml.Unmarshal([]byte(profilesString), &profiles)
	if unmarshalErr != nil {
		return nil, unmarshalErr
	}

	for profileName := range profiles {
		profile := profiles[profileName]
		profile.name = profileName
		profiles[profileName] = profile

		if len(profile.Folders) == 0 {
			return nil, fmt.Errorf("Expected at least one folder for profile %s", profileName)
		}
	}

	return profiles, nil
}

func (p Profile) Name() string {
	return p.name
}

func (p Profile) ToTOML(repoName RepositoryName) (string, error) {
	templateBuffer := new(bytes.Buffer)
	templateData := struct {
		Name    string
		Inherit RepositoryName
		Tag     string
		Folders []string
		Host    string
	}{
		Name:    p.fullProfileName(repoName),
		Inherit: repoName,
		Tag:     p.name,
		Folders: p.Folders,
		Host:    p.Host,
	}
	tomlTemplate.Execute(templateBuffer, templateData)
	return templateBuffer.String(), nil
}

func (p Profile) fullProfileName(repoName RepositoryName) string {
	return fmt.Sprintf("%s%s", p.name, repoName)
}

func (p Profile) ToConfigMap(repos []RepositoryName, backupNamespace string, cmName string) (*corev1.ConfigMap, error) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: backupNamespace,
		},
		Data: make(map[string]string),
	}

	for _, repoName := range repos {
		tomlString, err := p.ToTOML(repoName)
		if err != nil {
			return nil, err
		}
		cm.Data[fmt.Sprintf("%s.toml", p.fullProfileName(repoName))] = tomlString
	}

	return cm, nil
}

func ProfilesToConfigMap(profiles map[string]Profile, repos []RepositoryName, backupNamespace string, cmName string) (*corev1.ConfigMap, error) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: backupNamespace,
		},
		Data: make(map[string]string),
	}

	for _, profile := range profiles {
		for _, repoName := range repos {
			tomlString, err := profile.ToTOML(repoName)
			if err != nil {
				return nil, err
			}
			cm.Data[fmt.Sprintf("%s.toml", profile.fullProfileName(repoName))] = tomlString
		}

	}

	return cm, nil
}
