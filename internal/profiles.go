package internal

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"path"
	"rafdir/internal/meta"
	"text/template"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
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
	Disabled       bool     `json:"disabled"`
	Namespace      string   `json:"namespace"`
	Deployment     string   `json:"deployment"`
	StatefulSet    string   `json:"statefulset"`
	Selector       string   `json:"selector"`
	Node           string   `json:"node"`
	Stop           bool     `json:"stop"`
	Host           string   `json:"host"`
	Folders        []string `json:"folders"`
	StdInCommand   string   `json:"stdin-command"`
	StdInFilename  string   `json:"stdin-filename"`
	StdInNamespace string   `json:"stdin-namespace"`
	StdInSelector  string   `json:"stdin-selector"`
	SnapshotClass  string   `json:"snapshot-class"`
	StorageClass   string   `json:"storage-class"`

	Name string
}

func ProfilesFromGlobalConfigMap(config *Config, globalConfigMap *corev1.ConfigMap, profileFilter string) (map[string]Profile, []error) {
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

	log := slog.Default()
	profiles := make(map[string]Profile)

	// Parse the yaml
	unmarshalErr := yaml.Unmarshal([]byte(profilesString), &profiles)
	if unmarshalErr != nil {
		return nil, []error{unmarshalErr}
	}

	errs := make([]error, 0)
	for profileName := range profiles {
		if profileFilter != "" && profileName != profileFilter {
			log.Info("Skipping profile due to filter", "profile", profileName, "profileFilter", profileFilter)
			delete(profiles, profileName)
			continue
		}

		profile := profiles[profileName]
		profile.Name = profileName

		// If a profile filter is set, don't skip disabled profile, assume it was
		// done on purpose.
		if profile.Disabled && profileFilter == "" {
			log.Info("Profile is disabled, skipping.", "profile", profileName)
			delete(profiles, profileName)
			continue
		}

		if profile.SnapshotClass == "" {
			profile.SnapshotClass = config.SnapshotClass
		}

		if profile.StorageClass == "" {
			profile.StorageClass = config.DefaultStorageClass
		}

		// Validate the profile
		err := profile.Validate()
		if err != nil {
			log.Error("Error validating profile, skipping.", "profile", profileName, "err", err)
			errs = append(errs, fmt.Errorf("Error validating profile %s: %w", profileName, err))
			delete(profiles, profileName)
		} else {
			profiles[profileName] = profile
		}
	}

	return profiles, errs
}

func (p Profile) Validate() error {
	// Check for any type of profile
	if p.SnapshotClass == "" {
		return fmt.Errorf("SnapshotClass is required for profile %s", p.Name)
	}

	if p.StorageClass == "" {
		return fmt.Errorf("StorageClass is required for profile %s", p.Name)
	}

	// Run checks depending on profile type
	if p.Node != "" {
		if err := p.validateNode(); err != nil {
			return err
		}
	} else {
		if p.Namespace == "" {
			return fmt.Errorf("Namespace is required for profile %s", p.Name)
		}

		if p.Host == "" {
			return fmt.Errorf("Host is required for profile %s", p.Name)
		}

		if p.Deployment == "" && p.StatefulSet == "" && p.Selector == "" {
			return fmt.Errorf("Either Deployment, StatefulSet, Selector or Node is required for profile %s", p.Name)
		}

		if p.Deployment != "" && p.StatefulSet != "" && p.Selector != "" {
			return fmt.Errorf("Only one of Deployment, StatefulSet, Selector or Node is allowed for profile %s", p.Name)
		}

		if len(p.Folders) == 0 && p.StdInCommand == "" {
			return fmt.Errorf("Either Folders or StdInCommand is required for profile %s", p.Name)
		}

		if len(p.Folders) > 0 && p.StdInCommand != "" && p.StdInFilename == "" {
			return fmt.Errorf("StdInFilename is required when StdInCommand is set and Folders are not empty for profile %s", p.Name)
		}

		if p.StdInNamespace != "" && p.StdInSelector == "" {
			return fmt.Errorf("StdInSelector is required when StdInNamespace is set for profile %s", p.Name)
		}

		if p.StdInFilename != "" && p.StdInCommand == "" {
			return fmt.Errorf("StdInFilename is only allowed when StdInCommand is set for profile %s", p.Name)
		}

	}
	return nil
}

func (p Profile) validateNode() error {
	if p.Namespace != "" {
		return fmt.Errorf("Namespace is not allowed if node is specified for profile %s", p.Name)
	}

	if p.Host != "" {
		return fmt.Errorf("Host is not allowed if node is specified as it is inferred from node for profile %s", p.Name)
	}

	if p.Deployment != "" {
		return fmt.Errorf("Deployment is not allowed if node is specified for profile %s", p.Name)
	}

	if p.StatefulSet != "" {
		return fmt.Errorf("StatefulSet is not allowed if node is specified for profile %s", p.Name)
	}

	if p.Selector != "" {
		return fmt.Errorf("Selector is not allowed if node is specified for profile %s", p.Name)
	}

	if p.StdInCommand != "" {
		return fmt.Errorf("StdInCommand is not allowed if node is specified for profile %s", p.Name)
	}

	if len(p.Folders) == 0 {
		return fmt.Errorf("Folders is required f node is sepecified for profile %s", p.Name)
	}

	return nil
}

func (p Profile) ToTOML(repoName RepositoryName) (string, error) {
	templateBuffer := new(bytes.Buffer)
	var host string
	if p.Node != "" {
		host = p.Node
	} else {
		host = p.Host
	}
	templateData := struct {
		Name          string
		Inherit       RepositoryName
		Tag           string
		Folders       []string
		StdInCommand  string
		StdInFilename string
		Host          string
	}{
		Name:    p.fullProfileName(repoName),
		Inherit: repoName,
		Tag:     p.Name,
		Folders: p.Folders,
		Host:    host,
	}

	// Only when there's no folders do we use the resticprofile stdin option. If
	// there's a folder, the file will be written to that folder instead.
	if len(p.Folders) == 0 {
		templateData.StdInCommand = p.StdInCommand
		templateData.StdInFilename = p.StdInFilename
	}

	tomlTemplate.Execute(templateBuffer, templateData)
	return templateBuffer.String(), nil
}

func (p Profile) fullProfileName(repoName RepositoryName) string {
	return fmt.Sprintf("%s-%s", p.Name, repoName)
}

func (p Profile) ToConfigMap(repos []Repository, backupNamespace, cmName, runSuffix string) (*corev1.ConfigMap, error) {
	cm := &corev1.ConfigMap{
		ObjectMeta: meta.NewObjectMeta(cmName, backupNamespace, runSuffix),
		Data:       make(map[string]string),
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

func (p *Profile) BackupTarget(ctx context.Context, log *slog.Logger, kubeclient kubernetes.Interface, runSuffix string) (BackupTarget, error) {
	var target BackupTarget
	var err error

	if p.Deployment != "" {
		log.Debug("Creating backup target from deployment", "deployment", p.Deployment)
		target, err = NewBackupTargetFromDeploymentName(ctx, log, kubeclient, p, runSuffix)
	} else if p.StatefulSet != "" {
		log.Debug("Creating backup target from statefulset", "statefulset", p.StatefulSet)
		target, err = NewBackupTargetFromStatefulSetName(ctx, log, kubeclient, p, runSuffix)
	} else if p.Selector != "" {
		log.Debug("Creating backup target from selector", "selector", p.Selector)
		target, err = NewBackupTargetFromSelector(ctx, kubeclient, p.Namespace, p.Selector, p, runSuffix)
	} else if p.Node != "" {
		log.Debug("Creating backup target from node", "node", p.Node)
		target, err = NewBackupTargetFromNodeName(ctx, kubeclient, p, runSuffix)
	} else {
		return nil, fmt.Errorf("Profile %s has no Deployment, StatefulSet, Selector or Node", p.Name)
	}

	if err != nil {
		return nil, err
	}

	return target, nil
}

func (p *Profile) StdInTarget(ctx context.Context, kubeclient kubernetes.Interface, runSuffix string) (*PodBackupTarget, error) {
	log := slog.Default()
	if p.StdInSelector == "" {
		return nil, fmt.Errorf("Profile %s has no StdInSelector", p.Name)
	}

	log.Debug("Creating backup target from stdin selector", "selector", p.StdInSelector)
	return NewBackupTargetFromSelector(ctx, kubeclient, p.StdInNamespace, p.StdInSelector, p, runSuffix)
}

// StdInCommandNamespace either returns the explicit namespace of
// StdInNamespace or the regular Namespace field.
func (p *Profile) StdInCommandNamespace() string {
	if p.StdInNamespace != "" {
		return p.StdInNamespace
	}
	return p.Namespace
}

// StdInFilepath joins the first backup folder with the StdInFilepath if set,
// or returns an empty string if no file should be written.
func (p *Profile) StdInFilepath() string {
	// If there are no folders, the command comes via stdin and doesn't need a
	// path
	if len(p.Folders) == 0 {
		return ""
	}

	// If there is no file name there is no path to save.
	if p.StdInFilename == "" {
		return ""
	}

	// If there's both folder and filename, that's when command output needs to
	// be saved to a file inside that folder.
	firstFolder := p.Folders[0]

	// Join the filename with the path of the first folder
	return path.Join(firstFolder, p.StdInFilename)
}
