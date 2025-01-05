# Resticprofile with kubectl

Literally just [resticprofile](https://github.com/creativeprojects/resticprofile)
but with `kubectl` installed in it so it can run on Kubernetes and orchestrate
backups by managing pods. For example, Grafana with SQLite recommends stopping
it before taking a backup which requires `kubectl`:

```bash
#!/bin/sh

set -euo pipefail

echo "Stopping Grafana to take backup"
kubectl scale -n monitoring deployments/grafana --replicas=0
echo "Waiting until all pods have stopped"
kubectl wait --for=delete pod -l app.kubernetes.io/name=grafana
resticprofile grafana.backup
kubectl scale -n monitoring deployments/grafana --replicas=1
echo "Started Grafana again"
```
