# rafdir

This application creates backups using resticprofile on Kubernetes. It does
this by creating a separate namespace to deploy all resources in. At backup
time a CronJob is created to run the backup. It goes over each configured
profile and find the source data to backup. It then takes a snapshot of the PVC
and creates a new backup PVC in the backup namespace. This PVC is then mounted
into a per-profile pod that actually runs the backup using resticprofile.

This means there are a few requirements to running this:

* The cluster must have snapshot support installed
* The PVC driver must support snapshots and be set up correctly
* An existing snapshot class must be available, it should have a retention
  policy of "Delete"

A lot of values are hardcoded at the moment, but I'm open to pull requests to
make the system more dynamic. It currently just reflects my cluster setup.

To install it, first create a `values.yaml` and a `secret.yaml` file. Adjust
the values as needed. Lots of specific assumptions at the moment.

Then install the Helm chart:

```bash
helm upgrade --install --namespace backup rafdir ./helm/charts/rafdir -f secrets.yaml -f values.yaml
````

This creates the resources, e.g. service account, role, etc. so that the backup
can run.

## Development

Run tests:

```bash
go test ./internal/...
```

If you install the Helm chart and then run the application locally, you can
test pretty easily:

```
go run ./cmd/rafdir.go
```

Make sure you have a `KUBECONFIG` env var set up.


## FAQ

### Why not use Velero?

Velero doesn't support restic any more (deprecated), but more importantly it is
opinionated to take backups for cluster resources (e.g. ConfigMaps, Secrets,
etc.). I only want to backup PVCs. In addition, those cluster resources are
backed up unecrypted. I only want PVCs to be backed up encrypted.

### Why not use k8up?

k8up is great, but it requires per-namespace repositories and I wanted to have
a central repository that all backups get pulled into. That way, the individual
namespaces that are backed up don't need access to the repo password. By
leveraging some snapshot magic it's possible to achieve that but k8up doesn't
have support for snapshots at the moment and even if it did, it's designed for
in-namepsace backups.

### Why the name?

I needed something short and snappy. According to ChatGPT it's a combination of
old Norse words "ráðfǫrr". It was good enough.
