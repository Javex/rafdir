package main

import (
	"context"
	"fmt"
	"os"
	"rafdir"
	"strings"

	"github.com/spf13/cobra"
)

func addFlags(cmd *cobra.Command, clientConfig *rafdir.SnapshotClientConfig) {
	cmd.Flags().StringVarP(&clientConfig.Namespace, "namespace", "n", "backup", "Namespace from which backups will be orchestrated and run in")
	cmd.Flags().StringVarP(&clientConfig.ConfigMapName, "config-map-name", "c", "rafdir-config", "Name of ConfigMap that contains main backup config")
	cmd.Flags().StringVarP(&clientConfig.LogLevel, "log-level", "l", "INFO", "Log level, case-insensitive. Accepts debug, info, warn and error.")
	cmd.Flags().StringVarP(&clientConfig.ProfileFilter, "profile", "p", "", "Only back up a single profile. By default all profiles are updated. Value must be the name of a profile in the ConfigMap")
	cmd.Flags().StringVarP(&clientConfig.RepoFilter, "repository", "r", "", "Only back up profiles for a single repository. By default all repositories get a backup. Value must be the name of a repo in the ConfigMap.")
	cmd.Flags().StringVar(&clientConfig.ImageTag, "image-tag", "latest", "Tag of the rafdir image to use for the backup run.")

}

func rootRun(clientConfig *rafdir.SnapshotClientConfig) error {
	// Augment client config with env vars
	clientConfig.DbUrl = os.Getenv("RAFDIR_DB_URL")
	if clientConfig.DbUrl == "" {
		return fmt.Errorf("RAFDIR_DB_URL environment variable must be set")
	}

	ctx := context.Background()
	client, err := clientConfig.Build(ctx)
	if err != nil {
		return fmt.Errorf("Failed to create client: %s", err)
	}
	defer client.Close(ctx)

	errs := client.TakeBackup(ctx)
	if len(errs) > 0 {
		var errMsgs []string
		for _, err := range errs {
			errMsgs = append(errMsgs, err.Error())
		}
		return fmt.Errorf("Error taking backup: %s", strings.Join(errMsgs, "\n"))
	}
	return nil
}

func main() {
	var clientConfig rafdir.SnapshotClientConfig
	var rootCmd = &cobra.Command{
		Use:   "rafdir",
		Short: "Create a full backup run using rafdir",
		Long: `Kick off a complete backup run that iterates through all profiles
           and takes all backups as configured.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rootRun(&clientConfig)
		},
	}

	addFlags(rootCmd, &clientConfig)

	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}
