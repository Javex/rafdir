package internal_test

import (
	"rafdir/internal"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestProfilesFromYaml(t *testing.T) {
	tcs := []struct {
		name          string
		profileString string
		profileFilter string
		expProfile    map[string]internal.Profile
	}{
		{
			"defaultStop",
			`
        test:
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
          folders:
            - /test/folder
      `,
			"",

			map[string]internal.Profile{
				"test": {
					Name:       "test",
					Namespace:  "testNamespace",
					Deployment: "testDeployment",
					Stop:       false,
					Host:       "test.example.com",
					Folders:    []string{"/test/folder"},
				},
			},
		},
		{
			"allFields",
			`
        test:
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
          stop: true
          folders:
            - /test/folder
      `,
			"",

			map[string]internal.Profile{
				"test": {
					Name:       "test",
					Namespace:  "testNamespace",
					Deployment: "testDeployment",
					Stop:       true,
					Host:       "test.example.com",
					Folders:    []string{"/test/folder"},
				},
			},
		},
		{
			"stdInCommand",
			`
        test:
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
          stdin-command: "test command"
      `,
			"",

			map[string]internal.Profile{
				"test": {
					Name:         "test",
					Namespace:    "testNamespace",
					Deployment:   "testDeployment",
					Host:         "test.example.com",
					StdInCommand: "test command",
				},
			},
		},
		{
			"disabled",
			`
        test:
          disabled: true
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
          stdin-command: "test command"
      `,
			"",
			map[string]internal.Profile{},
		},
		{
			"filterMatch",
			`
        test:
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
          folders:
            - /test/folder
      `,
			"test",

			map[string]internal.Profile{
				"test": {
					Name:       "test",
					Namespace:  "testNamespace",
					Deployment: "testDeployment",
					Stop:       false,
					Host:       "test.example.com",
					Folders:    []string{"/test/folder"},
				},
			},
		},
		{
			"filterNoMatch",
			`
        test:
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
          folders:
            - /test/folder
      `,
			"otherFilter",

			map[string]internal.Profile{},
		},
		{
			"filterMatchDisabled",
			`
        test:
          disabled: true
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
          folders:
            - /test/folder
      `,
			"test",

			map[string]internal.Profile{
				"test": {
					Disabled:   true,
					Name:       "test",
					Namespace:  "testNamespace",
					Deployment: "testDeployment",
					Stop:       false,
					Host:       "test.example.com",
					Folders:    []string{"/test/folder"},
				},
			},
		},
		{
			"invalidButFiltered",
			`
        test:
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
      `,
			"otherFilter",

			map[string]internal.Profile{},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {

			profiles, errs := internal.ProfilesFromYamlString(tc.profileString, tc.profileFilter)
			if len(errs) > 0 {
				t.Fatalf("Error parsing yaml: %v", errs)
			}

			if !cmp.Equal(profiles, tc.expProfile) {
				t.Fatalf("Unexpected difference in profiles: %v", cmp.Diff(profiles, tc.expProfile))
			}
		})
	}
}

func TestProfilesFromYamlErrors(t *testing.T) {
	tcs := []struct {
		name          string
		profileString string
		// expErr        error
	}{
		{
			name: "emptyFolders",
			profileString: `
        test:
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
      `,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			res, errs := internal.ProfilesFromYamlString(tc.profileString, "")
			if len(errs) == 0 {
				t.Fatalf("Expected an error, but got nil")
			}
			if len(res) > 0 {
				t.Fatalf("Expected empty profiles, but got %v", len(res))
			}
		})
	}
}

func TestProfileToTOML(t *testing.T) {
	tcs := []struct {
		name    string
		profile internal.Profile
		repo    internal.Repository
		expTOML string
	}{
		{
			name: "default",
			profile: internal.Profile{
				Name:       "testName",
				Namespace:  "testNamespace",
				Deployment: "testDeployment",
				Host:       "test.example.com",
				Folders:    []string{"/test/folder", "/test/folder2"},
			},
			repo: internal.Repository{
				Name: "testRepo",
			},
			expTOML: `
[testName-testRepo]
  inherit = "testRepo"
  [testName-testRepo.backup]
    tag = ["testName"]
    source = [
    "/test/folder",
    "/test/folder2",
    ]
    host = "test.example.com"
  [testName-testRepo.snapshots]
    tag = ["testName"]
    host = "test.example.com"
`,
		},
		{
			name: "stdinCommand",
			profile: internal.Profile{
				Name:         "testName",
				Namespace:    "testNamespace",
				Host:         "test.example.com",
				StdInCommand: "test command",
			},
			repo: internal.Repository{
				Name: "testRepo",
			},
			expTOML: `
[testName-testRepo]
  inherit = "testRepo"
  [testName-testRepo.backup]
    tag = ["testName"]
    stdin = true
    host = "test.example.com"
  [testName-testRepo.snapshots]
    tag = ["testName"]
    host = "test.example.com"
`,
		},
		{
			name: "stdinCommandWithFilename",
			profile: internal.Profile{
				Name:          "testName",
				Namespace:     "testNamespace",
				Host:          "test.example.com",
				StdInCommand:  "test command",
				StdInFilename: "testfile",
			},
			repo: internal.Repository{
				Name: "testRepo",
			},
			expTOML: `
[testName-testRepo]
  inherit = "testRepo"
  [testName-testRepo.backup]
    tag = ["testName"]
    stdin = true
    stdin-filename = "testfile"
    host = "test.example.com"
  [testName-testRepo.snapshots]
    tag = ["testName"]
    host = "test.example.com"
`,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			toml, err := tc.profile.ToTOML(tc.repo.Name)
			if err != nil {
				t.Fatalf("Error generating TOML: %v", err)
			}

			if toml != tc.expTOML {
				t.Fatalf("Unexpected difference in TOML: %v", cmp.Diff(toml, tc.expTOML))
			}
		})
	}
}

func TestProfilesToConfigMap(t *testing.T) {
	tcs := []struct {
		name            string
		profile         internal.Profile
		repo            internal.Repository
		configMapName   string
		backupNamespace string
		expConfigMap    corev1.ConfigMap
	}{
		{
			name: "default",
			profile: internal.Profile{
				Name:       "testName",
				Namespace:  "testNamespace",
				Deployment: "testDeployment",
				Host:       "test.example.com",
				Folders:    []string{"/test/folder", "/test/folder2"},
			},
			repo: internal.Repository{
				Name: "testRepo",
			},
			configMapName:   "testConfigMap",
			backupNamespace: "testBackupNamespace",
			expConfigMap: corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "testConfigMap",
					Namespace: "testBackupNamespace",
				},
				Data: map[string]string{
					"testName-testRepo.toml": `
[testName-testRepo]
  inherit = "testRepo"
  [testName-testRepo.backup]
    tag = ["testName"]
    source = [
    "/test/folder",
    "/test/folder2",
    ]
    host = "test.example.com"
  [testName-testRepo.snapshots]
    tag = ["testName"]
    host = "test.example.com"
`,
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			cm, err := tc.profile.ToConfigMap(
				[]internal.Repository{tc.repo},
				tc.backupNamespace,
				tc.configMapName,
			)
			if err != nil {
				t.Fatalf("Error generating ConfigMap: %v", err)
			}

			if !cmp.Equal(*cm, tc.expConfigMap) {
				t.Fatalf("Unexpected difference in ConfigMap: %v", cmp.Diff(*cm, tc.expConfigMap))
			}
		})
	}
}
