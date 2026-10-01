# Schematic Datastream Replicator

Replicates Schematic flag, company, and user data from the datastream into a
customer-hosted Redis, so backend SDKs in replicator mode can evaluate flags
without calling the Schematic API.

Customers run the published image, not this source:

- Docker Hub: `getschematic/schematic-replicator`
- Amazon ECR Public: `public.ecr.aws/n5h3a7j9/schematic-replicator`

Deployment, configuration, health checks, and the full environment variable
reference live in the public docs:
**https://docs.schematichq.com/production_readiness/replicator**. Update that page
(in `schematic-fern-config`) when you add or change configuration here.

## Development

See [docs/DEV-README.md](docs/DEV-README.md) for local setup, the Task targets,
and running against a local API.

```bash
task build   # build the binary
task test    # run the tests
task quick   # build, test, and start the local stack with Redis
```

## Releasing

Push a `vX.Y.Z` tag. The [release workflow](.github/workflows/release-docker-image.yml)
builds a multi-arch image (`linux/amd64`, `linux/arm64`), pushes it to Docker Hub
and ECR Public tagged `X.Y.Z`, `X.Y`, `X`, and `latest` with SBOM and provenance
attestations, and scans both with Trivy.

## Redis key layout

The replicator writes, and SDKs in replicator mode read, these keys (`<version>`
is the rules engine cache version reported on `/health` as `cache_version`):

| Data | Key |
| -- | -- |
| Flag | `schematic:flags:<version>:<flag key, lowercased>` |
| Company by id | `schematic:company:<version>:<company id>` |
| Company by lookup key | `schematic:company:<version>:<key name, lowercased>:<value, lowercased>` |
| User by id | `schematic:user:<version>:<user id>` |
| User by lookup key | `schematic:user:<version>:<key name, lowercased>:<value, lowercased>` |

The `schematic:` prefix is not configurable here. An SDK's Redis provider prefix
plus its datastream key must produce exactly these strings, so an SDK configured
with any other prefix (or one that adds `schematic:` twice) never finds a key and
silently falls back to the REST API. `testdata/redis_key_layout.json` is the
contract: `TestRedisKeyLayoutMatchesFixture` checks the builders here, and each
SDK carries a unit test against the same cases.
