package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"resticprofilek8s"
	"resticprofilek8s/internal"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/homedir"

	csiClientset "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
)

func main() {
	initLogging()
	kubeconfig := getKubeconfig()
	namespace := "backup"
	configMapName := "resticprofile-kubernetes-config"
	ctx := context.Background()

	var kubeClient *kubernetes.Clientset
	var csiClient *csiClientset.Clientset

	if cfg, err := rest.InClusterConfig(); err == nil {
		kubeClient, err = resticprofilek8s.InitK8sClient(cfg)
		if err != nil {
			panic(fmt.Errorf("Failed to create k8s client: %s", err))
		}
		csiClient, err = resticprofilek8s.InitCSIClient(cfg)
		if err != nil {
			panic(fmt.Errorf("Failed to create csi client: %s", err))
		}
	} else {
		cfg, err := resticprofilek8s.GetK8sConfig(kubeconfig)
		if err != nil {
			panic(fmt.Errorf("Failed to get k8s config: %s", err))
		}

		kubeClient, err = resticprofilek8s.InitK8sClient(cfg)
		if err != nil {
			panic(fmt.Errorf("Failed to create k8s client: %s", err))
		}

		csiClient, err = resticprofilek8s.InitCSIClient(cfg)
		if err != nil {
			panic(fmt.Errorf("Failed to create csi client: %s", err))
		}
	}

	log := slog.Default()

	config, err := internal.LoadConfigFromKubernetes(ctx, log, kubeClient, namespace, configMapName)
	if err != nil {
		panic(fmt.Errorf("Failed to load global configmap: %s", err))
	}

	client, err := resticprofilek8s.NewClient(log, kubeClient, csiClient, config)
	if err != nil {
		panic(fmt.Errorf("Failed to create client: %s", err))
	}
	err = client.TakeBackup(ctx)
	if err != nil {
		panic(fmt.Errorf("Error taking backup: %s", err))
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
