package internal_test

import (
	"resticprofilek8s/internal"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
)

func TestRepositoriesFromConfigMap(t *testing.T) {
	tcs := []struct {
		name            string
		configMap       *corev1.ConfigMap
		expRepositories []internal.Repository
		isErr           bool
	}{
		{
			"emptyConfigMap",
			&corev1.ConfigMap{
				Data: map[string]string{},
			},
			nil,
			true,
		},
		{
			"RepositoriesEmpty",
			&corev1.ConfigMap{
				Data: map[string]string{
					"repositories": "",
				},
			},
			nil,
			true,
		},
		{
			"RepositoriesNotYaml",
			&corev1.ConfigMap{
				Data: map[string]string{
					"repositories": "1",
				},
			},
			nil,
			true,
		},
		{
			"OneRepository",
			&corev1.ConfigMap{
				Data: map[string]string{
					"repositories": `
repo1:
  inherit: baseRepo
  repository: someS3Url
`,
				},
			},
			[]internal.Repository{
				{
					Name: "repo1",
					ProfileYaml: `inherit: baseRepo
repository: someS3Url
`,
				},
			},
			false,
		},
		{
			"TwoRepositories",
			&corev1.ConfigMap{
				Data: map[string]string{
					"repositories": `
repo1:
  inherit: baseRepo
  repository: someS3Url
repo2:
  inherit: baseRepo
  repository: differentS3Url
`,
				},
			},
			[]internal.Repository{
				{
					Name: "repo1",
					ProfileYaml: `inherit: baseRepo
repository: someS3Url
`,
				},
				{
					Name: "repo2",
					ProfileYaml: `inherit: baseRepo
repository: differentS3Url
`,
				},
			},
			false,
		},
		{
			"RepositoryWithNestedValues",
			&corev1.ConfigMap{
				Data: map[string]string{
					"repositories": `
repo1:
  inherit: baseRepo
  backup:
    limit-upload: "123"`,
				},
			},
			[]internal.Repository{
				{
					Name: "repo1",
					ProfileYaml: `backup:
  limit-upload: "123"
inherit: baseRepo
`,
				},
			},
			false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			repos, err := internal.RepositoriesFromConfigMap(tc.configMap)
			if err != nil && !tc.isErr {
				t.Fatalf("unexpected error: %v", err)
			}

			if err == nil && tc.isErr {
				t.Fatalf("expected error, got nil")
			}

			if len(repos) != len(tc.expRepositories) {
				t.Fatalf("expected %d repositories, got %d", len(tc.expRepositories), len(repos))
			}

			for i, repo := range repos {
				expRepo := tc.expRepositories[i]
				if repo.Name != expRepo.Name {
					t.Errorf("expected name %s, got %s", expRepo.Name, repo.Name)
				}
				if repo.ProfileYaml != expRepo.ProfileYaml {
					t.Errorf("profileYaml mismatch:\n%s", cmp.Diff(repo.ProfileYaml, expRepo.ProfileYaml))
				}
			}
		})
	}
}
