package internal

import "testing"

func TestCreatePodName(t *testing.T) {
	t.Run("should return a pod name with the node name and run suffix", func(t *testing.T) {
		profileName := "profile1"
		nodeName := "node1.com"
		runSuffix := "suffix"
		expected := "profile1-node1-com-suffix"
		actual, err := createPodName(profileName, nodeName, runSuffix)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if actual != expected {
			t.Errorf("expected %s but got %s", expected, actual)
		}
	})
}
