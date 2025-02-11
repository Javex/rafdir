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
	Name       string   `json:"name"`
	Namespace  string   `json:"namespace"`
	Deployment string   `json:"deployment"`
	Stop       bool     `json:"stop"`
	Host       string   `json:"host"`
	Folders    []string `json:"folders"`
}

func ProfilesFromYamlString(profilesString string) (map[string]Profile, error) {
	profiles := make(map[string]Profile)

	// Parse the yaml
	unmarshalErr := yaml.Unmarshal([]byte(profilesString), &profiles)
	if unmarshalErr != nil {
		return nil, unmarshalErr
	}

	for profileName, profile := range profiles {
		if len(profile.Folders) == 0 {
			return nil, fmt.Errorf("Expected at least one folder for profile %s", profileName)
		}
	}

	return profiles, nil
}

func (p Profile) ToTOML(repo ResticRepository) (string, error) {
	templateBuffer := new(bytes.Buffer)
	templateData := struct {
		Name    string
		Inherit string
		Tag     string
		Folders []string
		Host    string
	}{
		Name:    p.fullProfileName(repo),
		Inherit: repo.Name,
		Tag:     p.Name,
		Folders: p.Folders,
		Host:    p.Host,
	}
	tomlTemplate.Execute(templateBuffer, templateData)
	return templateBuffer.String(), nil
}

func (p Profile) fullProfileName(repo ResticRepository) string {
	return fmt.Sprintf("%s%s", p.Name, repo.Suffix)
}

func ProfilesToConfigMap(profiles map[string]Profile, repos []ResticRepository, backupNamespace string, cmName string) (*corev1.ConfigMap, error) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: backupNamespace,
		},
		Data: make(map[string]string),
	}

	for _, profile := range profiles {
		for _, repo := range repos {
			tomlString, err := profile.ToTOML(repo)
			if err != nil {
				return nil, err
			}
			cm.Data[fmt.Sprintf("%s.toml", profile.fullProfileName(repo))] = tomlString
		}

	}

	return cm, nil
}
