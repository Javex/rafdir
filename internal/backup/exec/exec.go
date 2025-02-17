package exec

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
)

func Backup() []error {
	log := slog.Default()
	basePath := "/etc/restic/profiles.d/"
	profiles, err := listProfiles(basePath)
	if err != nil {
		log.Error("Failed to list profiles", "error", err)
		return []error{err}
	}

	errs := make([]error, 0)
	for _, profile := range profiles {
		log = log.With("profile", profile)
		log.Info("Starting backup")

		// Execute the profile
		err = runResticprofile(profile)
		if err != nil {
			log.Error("Backup failed", "error", err)
			errs = append(errs, err)
		}
	}

	return errs
}

// List profiles to be executed
func listProfiles(basePath string) ([]string, error) {
	suffix := ".toml"

	files, err := os.ReadDir(basePath)
	if err != nil {
		return nil, err
	}

	var profiles []string
	for _, file := range files {
		if strings.HasSuffix(file.Name(), suffix) {
			// Get the profile name without the suffix
			profiles = append(profiles, strings.TrimSuffix(file.Name(), suffix))
		}
	}

	return profiles, nil
}

func runResticprofile(profile string) error {
	log := slog.Default().With("profile", profile)
	args := []string{
		"--no-prio",
		"--no-ansi",
		"--config", "/etc/restic/profiles.yaml",
		"--name", profile, "backup",
	}
	cmd := exec.Command(
		"/usr/bin/resticprofile",
		args...,
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	if err != nil {
		log.Error("Failed to start backup for profile", "error", err)
		return fmt.Errorf("Failed to start backup for profile %s: %w", profile, err)
	}

	err = cmd.Wait()
	if err != nil {
		log.Error("Failed to wait for backup to finish", "error", err)
		return fmt.Errorf("Failed to wait for backup to finish for profile %s: %w", profile, err)
	}

	return nil
}
