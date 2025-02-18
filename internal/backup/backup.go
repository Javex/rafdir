package backup

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
)

type Backup struct {
	// Path to directory that contains the individual profiles to run
	ProfilePath string
	// Path to resticprofile config file
	ConfigFile string
	// Path to resticprofile binary
	CmdPath string
}

func (b *Backup) Run() []error {
	log := slog.Default()
	profiles, err := b.listProfiles()
	if err != nil {
		log.Error("Failed to list profiles", "error", err)
		return []error{err}
	}

	errs := make([]error, 0)
	for _, profile := range profiles {
		log = log.With("profile", profile)
		log.Info("Starting backup")

		// Execute the profile
		err = b.runResticprofile(profile)
		if err != nil {
			log.Error("Backup failed", "error", err)
			errs = append(errs, err)
		}
	}

	return errs
}

// List profiles to be executed
func (b *Backup) listProfiles() ([]string, error) {
	suffix := ".toml"

	files, err := os.ReadDir(b.ProfilePath)
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

func (b *Backup) runResticprofile(profile string) error {
	log := slog.Default().With("profile", profile)
	args := []string{
		"--no-prio",
		"--no-ansi",
		"--config", b.ConfigFile,
		"--name", profile, "backup",
	}
	cmd := exec.Command(
		b.CmdPath,
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
