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
		name                string
		profileString       string
		profileFilter       string
		expProfile          map[string]internal.Profile
		expCommandNamespace string
		expFilepath         string
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
					Name:          "test",
					Namespace:     "testNamespace",
					Deployment:    "testDeployment",
					Stop:          false,
					Host:          "test.example.com",
					Folders:       []string{"/test/folder"},
					SnapshotClass: "testSnapshotClass",
					StorageClass:  "testStorageClass",
				},
			},
			"testNamespace",
			"", // expFilepath
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
					Name:          "test",
					Namespace:     "testNamespace",
					Deployment:    "testDeployment",
					Stop:          true,
					Host:          "test.example.com",
					Folders:       []string{"/test/folder"},
					SnapshotClass: "testSnapshotClass",
					StorageClass:  "testStorageClass",
				},
			},
			"testNamespace",
			"", // expFilepath
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
					Name:          "test",
					Namespace:     "testNamespace",
					Deployment:    "testDeployment",
					Host:          "test.example.com",
					StdInCommand:  "test command",
					SnapshotClass: "testSnapshotClass",
					StorageClass:  "testStorageClass",
				},
			},
			"testNamespace",
			"", // expFilepath
		},
		{
			"foldersAndCommand",
			`
        test:
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
          stdin-command: test command
          stdin-namespace: testCmdNamespace
          stdin-selector: app=testApp,instance=testInstance
          stdin-filename: testfile
          folders:
            - /test/folder
      `,
			"",

			map[string]internal.Profile{
				"test": {
					Name:           "test",
					Namespace:      "testNamespace",
					Deployment:     "testDeployment",
					Host:           "test.example.com",
					Stop:           false,
					StdInCommand:   "test command",
					StdInNamespace: "testCmdNamespace",
					StdInSelector:  "app=testApp,instance=testInstance",
					StdInFilename:  "testfile",
					Folders:        []string{"/test/folder"},
					SnapshotClass:  "testSnapshotClass",
					StorageClass:   "testStorageClass",
				},
			},
			"testCmdNamespace",
			"/test/folder/testfile", // expFilepath
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
			"testNamespace",
			"", // expFilepath
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
					Name:          "test",
					Namespace:     "testNamespace",
					Deployment:    "testDeployment",
					Stop:          false,
					Host:          "test.example.com",
					Folders:       []string{"/test/folder"},
					SnapshotClass: "testSnapshotClass",
					StorageClass:  "testStorageClass",
				},
			},
			"testNamespace",
			"", // expFilepath
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
			"testNamespace",
			"", // expFilepath
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
					Disabled:      true,
					Name:          "test",
					Namespace:     "testNamespace",
					Deployment:    "testDeployment",
					Stop:          false,
					Host:          "test.example.com",
					Folders:       []string{"/test/folder"},
					SnapshotClass: "testSnapshotClass",
					StorageClass:  "testStorageClass",
				},
			},
			"testNamespace",
			"", // expFilepath
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
			"testNamespace",
			"", // expFilepath
		},
		{
			"node",
			`
        test:
          name: testName
          node: test-node.cluster
          folders:
            - /test/folder
      `,
			"",

			map[string]internal.Profile{
				"test": {
					Name:          "test",
					Node:          "test-node.cluster",
					Stop:          false,
					Folders:       []string{"/test/folder"},
					SnapshotClass: "testSnapshotClass",
					StorageClass:  "testStorageClass",
				},
			},
			"", // expCommandNamespace
			"", // expFilepath
		},
		{
			"snapshotClassOverride",
			`
        test:
          name: testName
          namespace: testNamespace
          snapshot-class: testSnapshotClassOverride
          storage-class: testStorageClassOverride
          deployment: testDeployment
          host: test.example.com
          folders:
            - /test/folder
      `,
			"",

			map[string]internal.Profile{
				"test": {
					Name:          "test",
					Namespace:     "testNamespace",
					Deployment:    "testDeployment",
					Stop:          false,
					Host:          "test.example.com",
					Folders:       []string{"/test/folder"},
					SnapshotClass: "testSnapshotClassOverride",
					StorageClass:  "testStorageClassOverride",
				},
			},
			"testNamespace",
			"", // expFilepath
		},
		{
			"multipleFolders",
			`
        test:
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
          folders:
            - /test/folder1
            - /test/folder2
            - /test/folder3
      `,
			"",

			map[string]internal.Profile{
				"test": {
					Name:          "test",
					Namespace:     "testNamespace",
					Deployment:    "testDeployment",
					Stop:          false,
					Host:          "test.example.com",
					Folders:       []string{"/test/folder1", "/test/folder2", "/test/folder3"},
					SnapshotClass: "testSnapshotClass",
					StorageClass:  "testStorageClass",
				},
			},
			"testNamespace",
			"", // expFilepath
		},
		{
			"commandBeforeAndAfter",
			`
        test:
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
          command-before:
            cmd: echo foo
            container: main
          command-after:
            cmd: echo foo
            container: main
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
					CommandBefore: internal.ProfileCommand{
						Cmd:       "echo foo",
						Container: "main",
					},
					CommandAfter: internal.ProfileCommand{
						Cmd:       "echo foo",
						Container: "main",
					},
					Host:          "test.example.com",
					Folders:       []string{"/test/folder"},
					SnapshotClass: "testSnapshotClass",
					StorageClass:  "testStorageClass",
				},
			},
			"testNamespace",
			"", // expFilepath
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {

			config := &internal.Config{
				SnapshotClass:       "testSnapshotClass",
				DefaultStorageClass: "testStorageClass",
			}
			configMap := &corev1.ConfigMap{
				Data: map[string]string{
					"profiles": tc.profileString,
				},
			}

			profiles, errs := internal.ProfilesFromGlobalConfigMap(config, configMap, tc.profileFilter)
			if len(errs) > 0 {
				t.Fatalf("Error parsing yaml: %v", errs)
			}

			if !cmp.Equal(profiles, tc.expProfile) {
				t.Fatalf("Unexpected difference in profiles: %v", cmp.Diff(profiles, tc.expProfile))
			}

			for _, profile := range profiles {
				if profile.StdInCommandNamespace() != tc.expCommandNamespace {
					t.Fatalf("Unexpected difference in command namespace: %v", cmp.Diff(profile.StdInNamespace, tc.expCommandNamespace))
				}
				if profile.StdInFilepath() != tc.expFilepath {
					t.Fatalf("Unexpected difference in filepath: %v", cmp.Diff(profile.StdInFilename, tc.expFilepath))
				}
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
		{
			"deploymentAndStatefulSetMixed",
			`
        test:
          name: testName
          namespace: testNamespace
          statefulset: testDeployment
          deployment: test-node.cluster
          host: test.example.com
          folders:
            - /test/folder
      `,
		},
		{
			"deploymentAndSelectorMixed",
			`
        test:
          name: testName
          namespace: testNamespace
			    deployment: testDeployment
          selector: foo=bar
          host: test.example.com
          folders:
            - /test/folder
      `,
		},
		{
			"nodeAndStatefulSetMixed",
			`
        test:
          name: testName
          statefulset: testDeployment
          node: test-node.cluster
          folders:
            - /test/folder
      `,
		},
		{
			"nodeAndDeploymentMixed",
			`
        test:
          name: testName
          deployment: testDeployment
          node: test-node.cluster
          folders:
            - /test/folder
      `,
		},
		{
			"nodeWithNamespace",
			`
        test:
          name: testName
          node: test-node.cluster
          namespace: testNamespace
          folders:
            - /test/folder
      `,
		},
		{
			"nodeWithHostname",
			`
        test:
          name: testName
          node: test-node.cluster
          host: test.example.com
          folders:
            - /test/folder
      `,
		},
		{
			"nodeWithStop",
			`
        test:
          name: testName
          node: test-node.cluster
          stop: true
          folders:
            - /test/folder
      `,
		},
		{
			"nodeWithCommandBefore",
			`
        test:
          name: testName
          node: test-node.cluster
          command-before:
            cmd: echo foo
            container: main
          folders:
            - /test/folder
      `,
		},
		{
			"nodeWithCommandAfter",
			`
        test:
          name: testName
          node: test-node.cluster
          command-after:
            cmd: echo foo
            container: main
          folders:
            - /test/folder
      `,
		},
		{
			"FoldersAndCommandWithoutFilename",
			`
        test:
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
          stdin-command: test command
          stdin-namespace: testCmdNamespace
          folders:
            - /test/folder
      `,
		},
		{
			"StdInNamespaceWithoutSelector",
			`
        test:
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
          stdin-command: test command
          stdin-namespace: testCmdNamespace
          stdin-filename: testfile
          folders:
            - /test/folder
      `,
		},
		{
			"commandBeforeEmptyCmdButContainer",
			`
        test:
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
          command-before:
            container: main
          command-after:
            cmd: echo foo
            container: main
          folders:
            - /test/folder
      `,
		},
		{
			"commandAfterEmptyCmdButContainer",
			`
        test:
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
          command-before:
            cmd: echo foo
            container: main
          command-after:
            container: main
          folders:
            - /test/folder
      `,
		},
		{
			"commandBeforeAndStop",
			`
        test:
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
          stop: true
          command-before:
            cmd: echo foo
            container: main
          folders:
            - /test/folder
      `,
		},
		{
			"commandAfterAndStop",
			`
        test:
          name: testName
          namespace: testNamespace
          deployment: testDeployment
          host: test.example.com
          stop: true
          command-after:
            cmd: echo foo
            container: main
          folders:
            - /test/folder
      `,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {

			config := &internal.Config{
				SnapshotClass:       "testSnapshotClass",
				DefaultStorageClass: "testStorageClass",
			}
			configMap := &corev1.ConfigMap{
				Data: map[string]string{
					"profiles": tc.profileString,
				},
			}

			res, errs := internal.ProfilesFromGlobalConfigMap(config, configMap, "")
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
		{
			name: "stdinCommandWithFoldersAndFilename",
			profile: internal.Profile{
				Name:          "testName",
				Namespace:     "testNamespace",
				Host:          "test.example.com",
				Folders:       []string{"/data"},
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
    source = [
    "/data",
    ]
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
					Labels: map[string]string{
						"app.kubernetes.io/component":  "resticprofile",
						"app.kubernetes.io/instance":   "testConfigMap",
						"app.kubernetes.io/managed-by": "rafdir",
						"app.kubernetes.io/name":       "rafdir",
						"app.kubernetes.io/part-of":    "rafdir",
						"rafdir/runSuffix":             "testSuffix",
					},
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
				"testSuffix",
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
