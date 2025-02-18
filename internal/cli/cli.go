package cli

import (
	"flag"
	"log/slog"
	"os"
	"path/filepath"

	"k8s.io/client-go/util/homedir"
)

func GetKubeconfig() string {
	var kubeconfig string
	if kubeEnv := os.Getenv("KUBECONFIG"); kubeEnv != "" {
		flag.StringVar(&kubeconfig, "kubeconfig", kubeEnv, "absolute path to the kubeconfig file from KUBECONFIG env var")
	} else if home := homedir.HomeDir(); home != "" {
		flag.StringVar(&kubeconfig, "kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		flag.StringVar(&kubeconfig, "kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()
	return kubeconfig
}

func InitLogging() {
	opts := slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &opts))
	slog.SetDefault(logger)
}
