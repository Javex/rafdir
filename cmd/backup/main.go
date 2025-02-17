package main

import (
	"fmt"
	"rafdir/internal/backup/exec"
	"rafdir/internal/cli"
	"strings"
)

func main() {
	cli.InitLogging()

	errs := exec.Backup()
	if len(errs) > 0 {
		// Handle errors
		errStrings := make([]string, len(errs))
		for i, err := range errs {
			errStrings[i] = err.Error()
		}
		panic(fmt.Errorf("Errors while tacking backup: %s", strings.Join(errStrings, ", ")))
	}
}
