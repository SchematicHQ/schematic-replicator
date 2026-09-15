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

```bash
# Recommended: keep the API key in a Secret you manage
kubectl create secret generic schematic-api \
  --from-literal=api-key='your-api-key'

helm install replicator ./deployments/charts/schematic-replicator \
  --set schematic.existingSecret=schematic-api \
  --set redis.addr=my-redis:6379
```

Passing the key inline works too, but it is then stored in the Helm release and
readable via `helm get values`:

```bash
helm install replicator ./deployments/charts/schematic-replicator \
  --set schematic.apiKey='your-api-key' \
  --set redis.addr=my-redis:6379
```

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

Readiness reflects initial load progress, so a large dataset can take a while to
become ready on first install.

## Development

```bash
helm lint deployments/charts/schematic-replicator --set schematic.apiKey=x
helm template rep deployments/charts/schematic-replicator --set schematic.apiKey=x
helm template rep deployments/charts/schematic-replicator --set schematic.apiKey=x \
  | kubeconform -strict -summary
```

The chart fails at template time — before anything reaches the cluster — when the
API key is missing, `redis.addr` is a URL, cluster mode has no addresses, or an
`existingSecret` is set without its key.
