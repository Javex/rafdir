package exec

import (
	"os"
	"path/filepath"
	"testing"
)

func TestListProfiles(t *testing.T) {
	tcs := []struct {
		name     string
		files    []string
		expected []string
	}{
		{
			name:     "empty",
			files:    []string{},
			expected: []string{},
		},
		{
			name:     "one file",
			files:    []string{"testProfile.toml"},
			expected: []string{"testProfile"},
		},
		{
			name:     "multiple files",
			files:    []string{"testProfile1.toml", "testProfile2.toml", "testProfile3.toml"},
			expected: []string{"testProfile1", "testProfile2", "testProfile3"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			// Create a temporary folder for the files
			tmpDir := t.TempDir()
			for _, f := range tc.files {
				// Create an empty file inside the temporary folder with the name from
				// the test case file
				if _, err := os.Create(filepath.Join(tmpDir, f)); err != nil {
					t.Fatalf("could not create file %s: %v", f, err)
				}
			}
			actual, err := listProfiles(tmpDir)
			if err != nil {
				t.Fatalf("could not list profiles: %v", err)
			}

			if len(actual) != len(tc.expected) {
				t.Fatalf("expected %v, got %v", tc.expected, actual)
			}

			for i := range actual {
				if actual[i] != tc.expected[i] {
					t.Fatalf("expected %v, got %v", tc.expected, actual)
				}
			}
		})
	}
}
