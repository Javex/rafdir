package main

import (
	"context"
	"fmt"
	"log/slog"
	"rafdir"
	"rafdir/internal"
	"rafdir/internal/cli"
	"strings"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	csiClientset "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
)

func main() {
	cli.InitLogging()
	namespace := "backup"
	configMapName := "rafdir-config"
	ctx := context.Background()

	var kubeClient *kubernetes.Clientset
	var csiClient *csiClientset.Clientset

	if cfg, err := rest.InClusterConfig(); err == nil {
		kubeClient, err = rafdir.InitK8sClient(cfg)
		if err != nil {
			panic(fmt.Errorf("Failed to create k8s client: %s", err))
		}
		csiClient, err = rafdir.InitCSIClient(cfg)
		if err != nil {
			panic(fmt.Errorf("Failed to create csi client: %s", err))
		}
	} else {
		kubeconfig := cli.GetKubeconfig()
		cfg, err := rafdir.GetK8sConfig(kubeconfig)
		if err != nil {
			panic(fmt.Errorf("Failed to get k8s config: %s", err))
		}

		kubeClient, err = rafdir.InitK8sClient(cfg)
		if err != nil {
			panic(fmt.Errorf("Failed to create k8s client: %s", err))
		}

		csiClient, err = rafdir.InitCSIClient(cfg)
		if err != nil {
			panic(fmt.Errorf("Failed to create csi client: %s", err))
		}
	}

	log := slog.Default()

	config, err := internal.LoadConfigFromKubernetes(ctx, log, kubeClient, namespace, configMapName)
	if err != nil {
		panic(fmt.Errorf("Failed to load global configmap: %s", err))
	}

	client, err := rafdir.NewClient(log, kubeClient, csiClient, config)
	if err != nil {
		panic(fmt.Errorf("Failed to create client: %s", err))
	}
	errs := client.TakeBackup(ctx)
	if len(errs) > 0 {
		var errMsgs []string
		for _, err := range errs {
			errMsgs = append(errMsgs, err.Error())
		}
		panic(fmt.Errorf("Error taking backup: %s", strings.Join(errMsgs, "\n")))
	}
}
