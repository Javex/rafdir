package main

import (
	"fmt"
	"strings"

	"rafdir/internal/backup"
	"rafdir/internal/cli"

	"github.com/spf13/cobra"
)

func rootRun(backup *backup.Backup) error {
	cli.InitLogging()

	errs := backup.Run()
	if len(errs) > 0 {
		// Handle errors
		errStrings := make([]string, len(errs))
		for i, err := range errs {
			errStrings[i] = err.Error()
		}
		return fmt.Errorf("Errors while tacking backup: %s", strings.Join(errStrings, ", "))
	}
	return nil
}

func addFlags(cmd *cobra.Command, backup *backup.Backup) {
	cmd.Flags().StringVarP(&backup.ConfigFile, "config", "c", "/etc/restic/profiles.yaml", "Path to resticprofile config")
	cmd.Flags().StringVarP(&backup.ProfilePath, "profile-path", "p", "/etc/restic/profiles.d/", "Path to individual profiles to back up")
	cmd.Flags().StringVarP(&backup.CmdPath, "cmd-path", "b", "/usr/local/bin/resticprofile", "Path to resticprofile binary")
}

func main() {
	var backup backup.Backup
	var rootCmd = &cobra.Command{
		Use:   "backup",
		Short: "Start a resticprofile backup",
		Long: `Start a backup inside a pod by invoking resticprofile with the right
         arguments`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rootRun(&backup)
		},
	}
	addFlags(rootCmd, &backup)

	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}
