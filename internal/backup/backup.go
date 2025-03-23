package backup

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
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

	// CacheDir is a path where files can be written to avoid keeping them in
	// memory. Usually something like /var/cache/
	CacheDir string

	// Pause indicates whether to pause the backup process right before exiting
	// the process. This is useful for debugging.
	Pause bool

	KubernetesClient kubernetes.Interface
	Kubeconfig       *rest.Config
}

func (b *Backup) Validate() error {
	if b.ProfilePath == "" {
		return fmt.Errorf("ProfilePath cannot be empty")
	} else if _, err := os.Stat(b.ProfilePath); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("ProfilePath does not exist: %w", err)
		}
		return fmt.Errorf("Failed to stat ProfilePath: %w", err)
	}

	if b.ConfigFile == "" {
		return fmt.Errorf("ConfigFile cannot be empty")
	} else if _, err := os.Stat(b.ConfigFile); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("ConfigFile does not exist: %w", err)
		}
		return fmt.Errorf("Failed to stat ConfigFile: %w", err)
	}

	if b.CmdPath == "" {
		return fmt.Errorf("CmdPath cannot be empty")
	} else if _, err := os.Stat(b.CmdPath); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("CmdPath does not exist: %w", err)
		}
		return fmt.Errorf("Failed to stat CmdPath: %w", err)
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

	if _, err := os.Stat(b.CacheDir); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("CacheDir does not exist: %w", err)
		}
		return fmt.Errorf("Failed to stat CacheDir: %w", err)
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
	var stdoutPath string
	if b.StdInPod != "" {

		if b.StdInFilepath == "" {
			// If there's no filepath to save the output of the command, save it in
			// the cache directory
			stdoutPath = filepath.Join(b.CacheDir, fmt.Sprintf("%s.stdout", b.StdInPod))
		} else {
			// If there's a filepath to save the output of the command, save it there
			// This avoid having to copy it around afterwards.
			stdoutPath = b.StdInFilepath
		}

		stdOutFile, err := newBufferedFile(stdoutPath)
		if err != nil {
			return []error{fmt.Errorf("Failed to create buffered file: %w", err)}
		}

		ctx := context.Background()
		err = backupExec.ExecuteCommandInPod(
			ctx,
			b.KubernetesClient,
			b.Kubeconfig,
			b.StdInPod,
			b.StdInNamespace,
			b.StdInCommand,
			stdOutFile.bufferedFileWriter,
		)

		stdOutFile.close()

		if err != nil {
			log.Error("Failed to run stdin command", "error", err)
			return []error{err}
		}
	}

	errs := make([]error, 0)
	var stdoutReader *bufio.Reader
	var stdoutFile *os.File
	var reader io.Reader
	if stdoutPath != "" {
		log.Debug("Have stdout from stdin command")

		// If StdInFilepath is set, then the file is already in the right place.
		// Otherwise, open the file and pass it to the resticprofile command as
		// stdin.
		if b.StdInFilepath == "" {
			// Open the file that contains stdout data
			var err error
			stdoutFile, err = os.Open(stdoutPath)
			if err != nil {
				log.Error("Failed to open file", "error", err, "filepath", stdoutPath)
				return []error{err}
			}
			defer stdoutFile.Close()

			stdoutReader = bufio.NewReader(stdoutFile)
			log.Debug("Opened file for stdout to pass as stdin to resticprofile")
		}
	}

	for _, profile := range profiles {
		log = log.With("profile", profile)
		log.Info("Starting backup")

		if stdoutFile != nil {
			log.Debug("Resetting reader to beginning of buffer")
			// Reset file reader to the beginning of the file
			stdoutFile.Seek(0, 0)
			// Wrap in a new buffered reader
			stdoutReader = bufio.NewReader(stdoutFile)
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

type bufferedFile struct {
	rawFile            *os.File
	bufferedFileWriter *bufio.Writer
}

func newBufferedFile(filePath string) (*bufferedFile, error) {
	log := slog.Default()

	// Create a file to store the command output
	rawFile, err := os.Create(filePath)
	log.Info("Creating file", "file", filePath)
	if err != nil {
		log.Error("Failed to create file", "error", err)
		return nil, fmt.Errorf("Failed to create file: %w", err)
	}

	// Create a buffered writer for efficient writing
	bufferedFileWriter := bufio.NewWriter(rawFile)
	return &bufferedFile{
		rawFile:            rawFile,
		bufferedFileWriter: bufferedFileWriter,
	}, nil
}

func (c *bufferedFile) close() {
	c.bufferedFileWriter.Flush()
	c.rawFile.Close()
}
