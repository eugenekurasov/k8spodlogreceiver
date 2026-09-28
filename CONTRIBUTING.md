# Contributing

## Make targets

`make help` lists every target; `make check` runs everything that does not
need a cluster (formatting, lint, SPDX headers, unit tests) and is what the
pull request checks come down to.

```bash
make check      # fmt-check + lint + license-check + test
make test       # unit tests only
make build      # assemble a local collector containing this receiver
make generate   # regenerate the mdatagen files from metadata.yaml
```

`make fmt`, `make lint-fix` and `make license-fix` apply what the
corresponding check asks for.

The tools those targets need — `golangci-lint`, `addlicense`, `ocb` and
`mdatagen` — are pinned in the Makefile and installed into `./bin` on first
use, so they cannot collide with a different version already on your `PATH`,
and CI lints with the same version you do. `make clean` removes them along
with the built collector.

Extra `go test` flags go through `GOTESTFLAGS`:

```bash
make test GOTESTFLAGS="-run TestConfig -v"
```

The GitHub workflows call these same targets, so what runs locally is what
runs in CI.

## Running tests locally

### Unit tests

No external dependencies:

```bash
make test
```

### Integration tests

These run [`TestIntegration_LogsArrive`](./integration_test.go) against a
real Kubernetes cluster — a pod is created that emits a marker log line,
and the test asserts the receiver reads it back through the full
watch → stream → consumer path.

**Prerequisites**

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or
  another local Docker daemon), running.
- [`kind`](https://kind.sigs.k8s.io/), installed via Homebrew:

  ```bash
  brew install kind
  ```

- On macOS with Docker Desktop, the daemon socket isn't at the usual
  `/var/run/docker.sock` — export `DOCKER_HOST` so `kind`/`docker` find it:

  ```bash
  export DOCKER_HOST="unix://$HOME/.docker/run/docker.sock"
  ```

**Create a cluster** with the node image the Makefile pins, or override it
with any recent `kindest/node` tag — see
[kind releases](https://github.com/kubernetes-sigs/kind/releases) for current
ones:

```bash
make kind-up
make kind-up KIND_NODE_IMAGE=kindest/node:v1.34.11  # a different minor
```

**Run the tests:**

```bash
make test-integration
```

Creating the cluster sets `kind-k8spodlog-test` as your current
`kubectl` context and merges it into `~/.kube/config`, which is what the
test picks up by default (or set `KUBECONFIG` to point elsewhere).

**Clean up** when done:

```bash
make kind-down
```

If you re-run the tests immediately after a previous run, you may see
`object is being deleted: namespaces "k8spodlog-inttest" already exists`
— that's just the previous run's namespace still terminating (Kubernetes
namespace deletion isn't instant), not a real failure. Wait a few seconds
and retry.

## Generated files

The files under `internal/metadata`, `internal/metadatatest`,
`documentation.md` and the `generated_*.go` files are produced by `mdatagen`
from [`metadata.yaml`](metadata.yaml). Run `make generate` to refresh them
rather than editing them by hand.

## Dependency updates

[Renovate](https://docs.renovatebot.com/) runs from
[`.github/workflows/renovate.yml`](.github/workflows/renovate.yml) on a
weekday schedule, configured by
[`.github/renovate.jsonc`](.github/renovate.jsonc).

Anything released in lockstep is grouped into one pull request, so an
update to the Collector touches `go.mod`,
[`builder-config.yaml`](builder-config.yaml), the tool versions pinned in the
[`Makefile`](Makefile) and the versions quoted in the README together. That is
deliberate — a partial bump does not build.

Those Makefile pins are picked up through `# renovate:` comments above each
one; keep the comment with the variable when you move or rename it, or the
version quietly stops being updated.

Renovate authenticates as a GitHub App, which must be installed on the
repository and needs a `RENOVATE_APP_CLIENT_ID` variable plus a secret
holding the App's `.pem` private key — not its OAuth client secret, which
will not work.

## Releases

Commit subjects follow [Conventional Commits](https://www.conventionalcommits.org/)
— [release-please](https://github.com/googleapis/release-please) reads them
from [`release-please.yml`](.github/workflows/release-please.yml) and keeps a
release pull request open. Merging it writes `CHANGELOG.md`, bumps the module
version quoted in [`README.md`](README.md) and
[`builder-config.yaml`](builder-config.yaml), tags the commit and publishes
the GitHub release.

## Third-party code

Two packages are derived from `opentelemetry-collector-contrib` (Copyright
The OpenTelemetry Authors, licensed Apache-2.0). Both live under an
`internal/` path upstream, so Go's package visibility rules allow them to be
imported only from within the contrib module tree — they are redistributed
here with attribution rather than reimplemented, as Apache-2.0 permits. Each
file carries the upstream copyright header and a note on how it diverges:

- [`internal/consumerretry`](internal/consumerretry/logs.go) — a copy of
  contrib's `internal/coreinternal/consumerretry`.
- [`internal/k8sconfig`](internal/k8sconfig/config.go) — adapted from
  contrib's `internal/k8sconfig` (`APIConfig`, `CreateRestConfig`).

Keep those headers and divergence notes intact when refreshing either package
from upstream, and update this list if what is borrowed changes.

All other code in this repository is original to it and licensed under
Apache-2.0.
