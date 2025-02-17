package cli

import (
	"flag"
	"log/slog"
	"os"
	"path/filepath"

	"k8s.io/client-go/util/homedir"
)

func GetKubeconfig() *string {
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

func InitLogging() {
	opts := slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &opts))
	slog.SetDefault(logger)
}
