package main

import (
	"fmt"
	"log/slog"
	"os"
	"rafdir"
	"strings"

	"rafdir/internal/backup"
	"rafdir/internal/cli"

	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func rootRun(backup *backup.Backup) error {
	cli.InitLogging(slog.LevelDebug)

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

func addFlags(cmd *cobra.Command, backup *backup.Backup) error {
	cmd.Flags().StringVarP(&backup.ConfigFile, "config", "c", "/etc/restic/profiles.yaml", "Path to resticprofile config")
	cmd.Flags().StringVarP(&backup.ProfilePath, "profile-path", "p", "/etc/restic/profiles.d/", "Path to individual profiles to back up")
	cmd.Flags().StringVarP(&backup.CmdPath, "cmd-path", "b", "/usr/bin/resticprofile", "Path to resticprofile binary")

	cmd.Flags().StringVarP(&backup.StdInPod, "stdin-pod", "", "", "Pod in which to run the stdin command to create backup data")
	cmd.Flags().StringVarP(&backup.StdInNamespace, "stdin-namespace", "", "", "Namespace in which to run the stdin command to create backup data")
	cmd.Flags().StringVarP(&backup.StdInCommand, "stdin-command", "", "", "Command to run to get the backup data")
	cmd.Flags().StringVarP(&backup.StdInFilepath, "stdin-filepath", "", "", "Destination path where to save file from running StdInCommand")

	var kubeClient *kubernetes.Clientset
	var cfg *rest.Config
	var err error

	if cfg, err = rest.InClusterConfig(); err == nil {
		kubeClient, err = rafdir.InitK8sClient(cfg)
		if err != nil {
			return fmt.Errorf("Failed to create k8s client: %s", err)
		}
	} else {
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			return fmt.Errorf("KUBECONFIG environment variable not set")
		}
		cfg, err = rafdir.GetK8sConfig(kubeconfig)
		if err != nil {
			return fmt.Errorf("Failed to get k8s config: %s", err)
		}

		kubeClient, err = rafdir.InitK8sClient(cfg)
		if err != nil {
			return fmt.Errorf("Failed to create k8s client: %s", err)
		}
	}

	backup.KubernetesClient = kubeClient
	backup.Kubeconfig = cfg

	return nil
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
	err := addFlags(rootCmd, &backup)
	if err != nil {
		panic(err)
	}

	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}
