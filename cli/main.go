package main

import (
	"flag"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"resticprofilek8s"
	"time"

	"k8s.io/client-go/util/homedir"
)

func main() {
	initLogging()
	kubeconfig := getKubeconfig()
	snapshotClass := "longhorn"
	snapshotDriver := "driver.longhorn.io"
	backupNamespace := "backup"
	sleepDuration := 1 * time.Second
	waitTimeout := 10 * time.Second
	client, err := resticprofilek8s.NewClient(kubeconfig, snapshotClass, snapshotDriver, backupNamespace, sleepDuration, waitTimeout)
	if err != nil {
		log.Fatal(err)
	}
	err = client.TakeBackup()
	if err != nil {
		log.Fatalf("Error taking backup: %s", err)
	}
}

func initLogging() {
	opts := slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &opts))
	slog.SetDefault(logger)
}

func getKubeconfig() *string {
	var kubeconfig *string
	if kubeEnv := os.Getenv("KUBECONFIG"); kubeEnv != "" {
		kubeconfig = flag.String("kubeconfig", kubeEnv, "absolute path to the kubeconfig file from KUBECONFIG env var")
	} else if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()
	return kubeconfig
}
