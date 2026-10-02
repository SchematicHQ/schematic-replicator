# schematic-replicator

Helm chart for the Schematic Datastream Replicator — a service that mirrors
Schematic data into Redis so SDK clients can evaluate flags against a local
cache instead of calling the API.

## Prerequisites

- Kubernetes 1.23+
- Helm 3.8+
- **A reachable Redis.** Redis is mandatory: the replicator exits at startup if
  it cannot connect. This chart does not deploy Redis.

## Install

The chart is published to Amazon ECR Public as an OCI artifact. No AWS account or
login is needed to pull it. Pin `--version` to a chart release.

```bash
# Recommended: keep the API key in a Secret you manage
kubectl create secret generic schematic-api \
  --from-literal=api-key='your-api-key'

helm install replicator oci://public.ecr.aws/n5h3a7j9/charts/schematic-replicator \
  --version 0.1.0 \
  --set schematic.existingSecret=schematic-api \
  --set redis.addr=my-redis:6379
```

Passing the key inline works too, but it is then stored in the Helm release and
readable via `helm get values`:

```bash
helm install replicator oci://public.ecr.aws/n5h3a7j9/charts/schematic-replicator \
  --version 0.1.0 \
  --set schematic.apiKey='your-api-key' \
  --set redis.addr=my-redis:6379
```

The image defaults to Docker Hub (`getschematic/schematic-replicator`) at the
chart's `appVersion`. To pull it from Amazon ECR Public instead, add
`--set image.repository=public.ecr.aws/n5h3a7j9/schematic-replicator`.

Full deployment and configuration documentation:
https://docs.schematichq.com/production_readiness/replicator

## Configuration

Every value is documented inline in [`values.yaml`](values.yaml) — that file is
the reference, not a table here. Two rules are worth calling out because getting
them wrong fails at runtime rather than at install:

- **`redis.addr` is `host:port`, not a `redis://` URL.** It is passed straight to
  the go-redis client's `Addr` field, and a URL scheme fails to dial. The chart
  rejects one at template time.
- **`writerLock.disabled` is dangerous.** It is only safe on an instance that
  never writes the cache. Setting it on a second writer reintroduces exactly the
  corruption the lease prevents.

Any value left empty is omitted from the pod spec entirely, so the
application's own default applies. `extraEnv` covers anything the chart does not
model as a first-class value.

## The single-writer constraint

Exactly one replicator instance may write a given Redis. Instances take a Redis
lease at startup and **exit** if another holds it; a second writer would corrupt
the replay cursor and double-write cache entries.

The chart therefore hard-codes two things that are **not** exposed as values:

- `replicas: 1` — a second replica cannot acquire the lease and crash-loops.
- `strategy: Recreate` — the default `RollingUpdate` deadlocks. The new pod
  starts while the old still holds the lease, so it never becomes ready, so the
  old pod is never terminated. Kubernetes does not recover from this on its own.

The tradeoff is a short gap during upgrades: the old pod stops before the new
one starts. That is inherent to the app's design, not a chart limitation.

If you see `Could not acquire writer lock` in the logs, another instance is
already writing that Redis.

## Tracing

Tracing is off by default. It is configured with the standard OpenTelemetry
environment variables, which the chart passes through `extraEnv` rather than
modelling as values:

```yaml
extraEnv:
  OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel-collector:4318"
  OTEL_SERVICE_NAME: "schematic-replicator"
  OTEL_RESOURCE_ATTRIBUTES: "deployment.environment=prod"
```

See the [documentation](https://docs.schematichq.com/production_readiness/replicator#tracing)
for every supported variable.

## Connecting SDK clients

Cache reads go to Redis directly, but the Schematic SDK in replicator mode polls
the replicator's `/ready` endpoint. Point it at the Service:

```go
client := schematic.NewClient(
    core.WithAPIKey("your-api-key"),
    core.WithDatastream(
        core.WithReplicatorMode(),
        core.WithReplicatorHealthURL(
            "http://replicator-schematic-replicator:8090/ready"),
    ),
)
```

The replicator reports ready once it is connected to the datastream and
subscribed to updates. Companies and users continue loading into Redis in the
background, so on a first install against an empty Redis, lookups for companies
and users that have not loaded yet miss the cache until that load finishes.

## Development

```bash
helm lint deployments/charts/schematic-replicator --set schematic.apiKey=x
helm template rep deployments/charts/schematic-replicator --set schematic.apiKey=x
helm template rep deployments/charts/schematic-replicator --set schematic.apiKey=x \
  | kubeconform -strict -summary
```

Release a chart version by bumping `version` in `Chart.yaml` and pushing a matching
`chart-vX.Y.Z` tag; `.github/workflows/release-chart.yml` validates the chart and
pushes it to `oci://public.ecr.aws/n5h3a7j9/charts/schematic-replicator`.

The chart fails at template time — before anything reaches the cluster — when the
API key is missing, `redis.addr` is a URL, cluster mode has no addresses, or an
`existingSecret` is set without its key.
