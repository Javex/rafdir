package internal

import (
	"bytes"
	"fmt"
	"log/slog"
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
    {{ if .Folders -}}
    source = [
    {{ range .Folders -}}
      "{{ . }}",
    {{ end -}}
    ]
    {{- end }}
    {{- if .StdInCommand -}}
    stdin = true
    {{- if .StdInFilename }}
    stdin-filename = "{{ .StdInFilename }}"
    {{- end }}
    {{- end }}
    host = "{{ .Host }}"
  [{{ .Name }}.snapshots]
    tag = ["{{ .Tag }}"]
    host = "{{ .Host }}"
`))

type Profile struct {
	Disabled      bool     `json:"disabled"`
	Namespace     string   `json:"namespace"`
	Deployment    string   `json:"deployment"`
	StatefulSet   string   `json:"statefulset"`
	Stop          bool     `json:"stop"`
	Host          string   `json:"host"`
	Folders       []string `json:"folders"`
	StdInCommand  string   `json:"stdin-command"`
	StdInFilename string   `json:"stdin-filename"`

	Name string
}

func ProfilesFromGlobalConfigMap(globalConfigMap *corev1.ConfigMap) (map[string]Profile, []error) {
	if globalConfigMap.Data == nil {
		return nil, []error{fmt.Errorf("ConfigMap %s has no data", globalConfigMap.Name)}
	}

	profilesString, ok := globalConfigMap.Data["profiles"]
	if !ok {
		return nil, []error{fmt.Errorf("ConfigMap %s has no key `profiles`", globalConfigMap.Name)}
	}

	if profilesString == "" {
		return nil, []error{fmt.Errorf("ConfigMap %s has empty key `profiles`", globalConfigMap.Name)}
	}

	return ProfilesFromYamlString(profilesString)
}

func ProfilesFromYamlString(profilesString string) (map[string]Profile, []error) {
	log := slog.Default()
	profiles := make(map[string]Profile)

	// Parse the yaml
	unmarshalErr := yaml.Unmarshal([]byte(profilesString), &profiles)
	if unmarshalErr != nil {
		return nil, []error{unmarshalErr}
	}

	errs := make([]error, 0)
	for profileName := range profiles {
		profile := profiles[profileName]
		profile.Name = profileName

		// Validate the profile
		err := profile.Validate()
		if err != nil {
			log.Error("Error validating profile, skipping.", "profile", profileName, "err", err)
			errs = append(errs, fmt.Errorf("Error validating profile %s: %w", profileName, err))
			delete(profiles, profileName)
		} else if profile.Disabled {
			log.Info("Profile is disabled, skipping.", "profile", profileName)
			delete(profiles, profileName)
		} else {
			profiles[profileName] = profile
		}
	}

	return profiles, errs
}

func (p Profile) Validate() error {
	if p.Namespace == "" {
		return fmt.Errorf("Namespace is required for profile %s", p.Name)
	}

	if p.Host == "" {
		return fmt.Errorf("Host is required for profile %s", p.Name)
	}

	if p.Deployment == "" && p.StatefulSet == "" {
		return fmt.Errorf("Either Deployment or StatefulSet is required for profile %s", p.Name)
	}

	if p.Deployment != "" && p.StatefulSet != "" {
		return fmt.Errorf("Only one of Deployment or StatefulSet is allowed for profile %s", p.Name)
	}

	if len(p.Folders) == 0 && p.StdInCommand == "" {
		return fmt.Errorf("Either Folders or StdInCommand is required for profile %s", p.Name)
	}

	if len(p.Folders) > 0 && p.StdInCommand != "" {
		return fmt.Errorf("Currently only one of Folders or StdInCommand is allowed for profile %s", p.Name)
	}

	if p.StdInFilename != "" && p.StdInCommand == "" {
		return fmt.Errorf("StdInFilename is only allowed when StdInCommand is set for profile %s", p.Name)
	}

	return nil
}

func (p Profile) ToTOML(repoName RepositoryName) (string, error) {
	templateBuffer := new(bytes.Buffer)
	templateData := struct {
		Name          string
		Inherit       RepositoryName
		Tag           string
		Folders       []string
		StdInCommand  string
		StdInFilename string
		Host          string
	}{
		Name:          p.fullProfileName(repoName),
		Inherit:       repoName,
		Tag:           p.Name,
		Folders:       p.Folders,
		StdInCommand:  p.StdInCommand,
		StdInFilename: p.StdInFilename,
		Host:          p.Host,
	}
	tomlTemplate.Execute(templateBuffer, templateData)
	return templateBuffer.String(), nil
}

func (p Profile) fullProfileName(repoName RepositoryName) string {
	return fmt.Sprintf("%s-%s", p.Name, repoName)
}

func (p Profile) ToConfigMap(repos []Repository, backupNamespace string, cmName string) (*corev1.ConfigMap, error) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: backupNamespace,
		},
		Data: make(map[string]string),
	}

	for _, repoName := range repos {
		tomlString, err := p.ToTOML(repoName.Name)
		if err != nil {
			return nil, err
		}
		cm.Data[fmt.Sprintf("%s.toml", p.fullProfileName(repoName.Name))] = tomlString
	}

	return cm, nil
}
