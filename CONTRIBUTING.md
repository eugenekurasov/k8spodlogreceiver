# Contributing

## Running tests locally

### Unit tests

No external dependencies:

```bash
go test -v ./...
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

**Create a cluster** (any recent `kindest/node` tag works — see
[kind releases](https://github.com/kubernetes-sigs/kind/releases) for
current ones):

```bash
kind create cluster --name k8spodlog-test --image kindest/node:v1.34.8
```

**Run the tests** (`-mod=vendor` needs a populated `vendor/` — run
`go mod vendor` first if you don't already have one):

```bash
go mod vendor  # only if vendor/ doesn't already exist
go test -v -mod=vendor -tags integration -timeout 180s ./...
```

`kind create cluster` sets `kind-k8spodlog-test` as your current
`kubectl` context and merges it into `~/.kube/config`, which is what the
test picks up by default (or set `KUBECONFIG` to point elsewhere).

**Clean up** when done:

```bash
kind delete cluster --name k8spodlog-test
```

If you re-run the tests immediately after a previous run, you may see
`object is being deleted: namespaces "k8spodlog-inttest" already exists`
— that's just the previous run's namespace still terminating (Kubernetes
namespace deletion isn't instant), not a real failure. Wait a few seconds
and retry.

## Generated files

The files under `internal/metadata`, `internal/metadatatest`, and the
`generated_*.go` files are produced by `mdatagen` from
[`metadata.yaml`](metadata.yaml); regenerate them rather than editing them.

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
