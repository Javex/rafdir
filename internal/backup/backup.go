package backup

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"

	backupExec "rafdir/internal/backup/exec"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Backup struct {
	// Path to directory that contains the individual profiles to run
	ProfilePath string
	// Path to resticprofile config file
	ConfigFile string
	// Path to resticprofile binary
	CmdPath string

	// Pod in which to run the stdin command to create backup data
	StdInPod string
	// Namespace in which to run the stdin command to create backup data
	StdInNamespace string
	// Command to run to get the backup data
	StdInCommand string
	// Destination path where to save file from running StdInCommand. This can be
	// empty which means the file is saved directly by resticprofile through its
	// stdin option. However, when backing up both folder & a command, then the
	// file is saved in the first folder which is this option.
	StdInFilepath string

	KubernetesClient kubernetes.Interface
	Kubeconfig       *rest.Config
}

func (b *Backup) Validate() error {
	if b.ProfilePath == "" {
		return fmt.Errorf("ProfilePath cannot be empty")
	}

	if b.ConfigFile == "" {
		return fmt.Errorf("ConfigFile cannot be empty")
	}

	if b.CmdPath == "" {
		return fmt.Errorf("CmdPath cannot be empty")
	}

	// Return an error if only some of the stdin flags are set
	// StdInFilepath is optional
	if (b.StdInPod != "" || b.StdInNamespace != "" || b.StdInCommand != "") &&
		(b.StdInPod == "" || b.StdInNamespace == "" || b.StdInCommand == "") {
		return fmt.Errorf("If one of the stdin flags is set, all of them must be set")
	}

	if b.KubernetesClient == nil {
		return fmt.Errorf("KubernetesClient cannot be nil")
	}

	if b.Kubeconfig == nil {
		return fmt.Errorf("Kubeconfig cannot be nil")
	}

	return nil
}

func (b *Backup) Run() []error {
	log := slog.Default()
	profiles, err := b.listProfiles()
	if err != nil {
		log.Error("Failed to list profiles", "error", err)
		return []error{err}
	}

	// If there's a stdin command to run, run it and return
	var stdout *bytes.Buffer
	if b.StdInPod != "" {
		ctx := context.Background()
		stdout, err = backupExec.ExecuteCommandInPod(
			ctx,
			b.KubernetesClient,
			b.Kubeconfig,
			b.StdInPod,
			b.StdInNamespace,
			b.StdInCommand,
		)

		if err != nil {
			log.Error("Failed to run stdin command", "error", err)
			return []error{err}
		}
	}

	errs := make([]error, 0)
	var stdoutReader *bytes.Reader
	var reader io.Reader
	if stdout != nil {
		log.Debug("Have stdout from stdin command")
		// If StdInFilepath is set, save the stdout to a file, otherwise provide it
		// directly to resticprofile as stdin.
		if b.StdInFilepath != "" {
			if err := os.WriteFile(b.StdInFilepath, stdout.Bytes(), 0644); err != nil {
				log.Error("Failed to write file", "error", err, "filepath", b.StdInFilepath)
				return []error{err}
			}
			log.Debug("Wrote file", "filepath", b.StdInFilepath)
		} else {
			stdoutReader = bytes.NewReader(stdout.Bytes())
			log.Debug("Created reader from stdout to pass as stdin to resticprofile")
		}
	}

	for _, profile := range profiles {
		log = log.With("profile", profile)
		log.Info("Starting backup")

		if stdoutReader != nil {
			log.Debug("Resetting reader to beginning of buffer")
			// Reset the reader to the beginning of the buffer
			stdoutReader.Seek(0, 0)
			// This needs to be explicitly assigned to an interface type which *can*
			// be nil. If the stdoutReader is directly passed to the runResticprofile
			// function, the nil check won't work due to some Go interface
			// shenanigans.
			reader = stdoutReader
		}

		// Execute the profile
		err = b.runResticprofile(profile, reader)
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

func (b *Backup) runResticprofile(profile string, stdout io.Reader) error {
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

	// Stdout might be nil if no command was run, so in that case don't attach
	// it, all backup data will be as folders attached as volumes.
	if stdout != nil {
		cmd.Stdin = stdout
		log.Debug("Attaching stdin to resticprofile")
	}

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
