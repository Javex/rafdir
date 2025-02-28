package main

import (
	"context"
	"fmt"
	"rafdir"
	"rafdir/internal/cli"
	"strings"

	"github.com/spf13/cobra"
)

func addFlags(cmd *cobra.Command, clientConfig *rafdir.SnapshotClientConfig) {
	cmd.Flags().StringVarP(&clientConfig.Namespace, "namespace", "n", "backup", "Namespace from which backups will be orchestrated and run in")
	cmd.Flags().StringVarP(&clientConfig.ConfigMapName, "config-map-name", "c", "rafdir-config", "Name of ConfigMap that contains main backup config")
}

func rootRun(clientConfig *rafdir.SnapshotClientConfig) error {
	cli.InitLogging()
	ctx := context.Background()
	client, err := clientConfig.Build(ctx)
	if err != nil {
		return fmt.Errorf("Failed to create client: %s", err)
	}
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
