package internal_test

import (
	"resticprofilek8s/internal"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestBaseProfile(t *testing.T) {
	tcs := []struct {
		name      string
		config    *internal.Config
		expString string
	}{
		{
			"baseTest",
			&internal.Config{
				GlobalConfigFile: `
version: "1"
includes:
  - "/etc/restic/profiles.d/*.toml"
default_configuration:
  cache-dir: /var/cache/restic
  backup:
    exclude-caches: true`,
				Repositories: []internal.Repository{
					{
						Name: "firstRepo",
						ProfileYaml: `inherit: default_configuration
repository: someS3Url`,
					},
					{
						Name: "secondRepo",
						ProfileYaml: `inherit: default_configuration
repository: someOtherS3Url
backup:
  limit-upload: "123"`,
					},
				},
			},
			`version: "1"
includes:
  - "/etc/restic/profiles.d/*.toml"
default_configuration:
  cache-dir: /var/cache/restic
  backup:
    exclude-caches: true
firstRepo:
  inherit: default_configuration
  repository: someS3Url
secondRepo:
  inherit: default_configuration
  repository: someOtherS3Url
  backup:
    limit-upload: "123"
`,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.config.BaseProfile()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.expString {
				t.Errorf("unexpected profile difference:\n%s", cmp.Diff(got, tc.expString))
			}
		})
	}
}
