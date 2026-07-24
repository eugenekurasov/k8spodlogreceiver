# k8spodlog receiver

An OpenTelemetry Collector receiver that streams Kubernetes pod logs via
the Kubernetes API server — the same mechanism `kubectl logs -f` uses —
instead of mounting the host filesystem or requiring a DaemonSet with
node-level access.

## Status

🚧 Development / proof-of-concept. Not yet built against a pinned
`opentelemetry-collector-contrib` module version. See
[open issues](#known-limitations--open-questions) before relying on this
in production.

## Kubernetes version compatibility

[![K8s compatibility tests](https://github.com/eugenekurasov/security-observability-stack/actions/workflows/integration.yml/badge.svg?branch=main)](https://github.com/eugenekurasov/security-observability-stack/actions/workflows/integration.yml)

All APIs used by this receiver are stable `core/v1` endpoints (`pods`,
`pods/log`) present and unchanged since Kubernetes 1.3. Label selectors
on List/Watch are stable since 1.0. There are no alpha or beta API
dependencies.

## Why this exists

[open-telemetry/opentelemetry-collector-contrib#23339](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/23339)
proposes a `k8slog` receiver that mounts the node's log directory
(`hostPath`) into a DaemonSet pod. Reviewers flagged this as a broad
privilege grant for a narrow task. This project explores an alternative:
collecting logs purely through the API server, scoped by ordinary
Kubernetes RBAC on the `pods/log` subresource.
[open-telemetry/opentelemetry-collector-contrib#24641](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/24641) Propose a New component: Kubernetes api logs receiver 
but the issue was closed as inactive.


|                            | hostPath + DaemonSet         | k8spodlog (this project)                              |
|----------------------------|------------------------------|-------------------------------------------------------|
| Node filesystem access     | Yes (read-only host mount)   | None                                                  |
| Deployment shape           | DaemonSet (one per node)     | Deployment (one per tenant, any node)                 |
| GPU / specialized nodes    | Collector scheduled on every node, including expensive GPU nodes | No collector pod on GPU nodes — they stay 100% dedicated to workloads |
| Compute cost               | One collector pod per node   | One (or few) pods per tenant, on cheap CPU nodes      |
| Intra-cluster network      | None (local file read)       | Log data traverses the API server — negligible on EKS/GKE (managed, auto-scaled control plane); worth planning for on self-hosted clusters with a fixed-spec API server |
| RBAC granularity           | Node-level                   | Namespace / label-selector scoped                     |
| Serverless node pools (Fargate, AKS Virtual Nodes, GCP Autopilot) | Not supported — DaemonSets are blocked or restricted on these platforms; `hostPath` mounts are unavailable | Fully supported — plain Deployment, no DaemonSet or `hostPath` required; API endpoint is the same regardless of underlying node type |
| Log continuity on rotation | Direct file access, robust   | Depends on kubelet log retention                      |

## Intentional scope

This receiver collects **application container logs only** — what a pod writes to stdout/stderr. It does not and cannot collect:

- Node logs (systemd journal, kubelet, containerd daemon logs) — these live on the host filesystem and require hostPath access
- Control plane logs (kube-apiserver, etcd, scheduler)
- Any logs below the pod/container API boundary

This is a deliberate scope choice. The target user is a **tenant team** that wants full visibility into their own application pods without any node-level privilege granted. If node-level log collection is required, it belongs in a separate cluster-operator-managed pipeline with explicitly granted node access — not mixed into per-tenant collectors.

## Compliance / multi-tenancy fit

Because access is mediated entirely by the Kubernetes API and scoped via
RBAC, a single cluster can run per-tenant collector instances that are
only authorized to read logs for their own namespace(s) — useful for
environments with SOC 2-style log-access segregation
requirements, without relying on node-level trust boundaries.

## Quick start

**Preferred — use the Helm chart**, which generates the correct RBAC
(namespace-scoped Role or cluster-wide ClusterRole) based on your chosen mode
and enabled signals:

```bash
helm install my-obs ../../helm/observability-stack \
  --namespace payments \
  --set tenantId=payments
```

**Standalone / local development only** — apply `deploy/rbac.yaml` directly.
This creates a ClusterRole and is intended for component-level testing outside
the full chart. See comments in that file for what the chart covers that it
does not.

Example collector config:

```yaml
receivers:
  k8s_podlog:
    namespaces: ["payments", "billing"]
    pod_label_selector: "app.kubernetes.io/part-of=payments-platform"
    since_seconds: 300

exporters:
  otlp:
    endpoint: "otel-gateway:4317"

service:
  pipelines:
    logs:
      receivers: [k8s_podlog]
      exporters: [otlp]
```

## Configuration reference

The [Quick start](#quick-start) above covers deploying the receiver *via
the Helm chart*, which sets these fields for you from `values.yaml`. This
section documents the receiver's own configuration surface directly, for
anyone hand-writing a collector config or embedding this component in a
different distribution.

```yaml
receivers:
  k8s_podlog:
    api_config:
      auth_type: serviceAccount
      kube_api_qps: 20
      kube_api_burst: 40
    namespaces: ["payments", "billing"]
    pod_label_selector: "app.kubernetes.io/part-of=payments-platform"
    since_seconds: 300
    reconnect_backoff:
      initial_interval: 1s
      max_interval: 30s
      max_elapsed_time: 5m
    max_batch_size: 1000
    flush_interval: 200ms
    max_log_size: 1048576
    max_log_size_behavior: split
```

- `api_config.auth_type` (default `serviceAccount`): how to authenticate to
  the API server.
  - `serviceAccount`: use the pod's mounted ServiceAccount token (standard
    production mode).
  - `kubeConfig`: use `api_config.kubeconfig_path` if set, otherwise the
    standard kubeconfig-loading chain (`KUBECONFIG` env, then
    `~/.kube/config`) — use for local development.
  - `none`: build the API host from `KUBERNETES_SERVICE_HOST` /
    `KUBERNETES_SERVICE_PORT` with no client credentials, for an
    unauthenticated proxy in front of the API server (e.g. `kubectl
    proxy`). Not for production use.
- `api_config.kubeconfig_path`: path to a kubeconfig file, used only when
  `auth_type` is `kubeConfig`.
- `api_config.kube_api_qps` (default `0`, meaning client-go's own built-in
  default of 5): maximum queries per second to the Kubernetes API. This
  bounds the rate of *new* connection/reconnect attempts, not the number of
  concurrently open log streams — `pods/log?follow=true` is a long-running
  request exempt from the apiserver's inflight-request limits, so it isn't
  what this setting protects against. Increase if you see "client-side
  throttling" warnings in the collector logs, e.g. under heavy reconnect
  churn across many pods.
- `api_config.kube_api_burst` (default `0`, meaning client-go's own
  built-in default of 10): maximum burst of requests to the Kubernetes API,
  used alongside `kube_api_qps`.
- `namespaces`: restrict log collection to these namespaces. Empty (default)
  means all namespaces visible to the ServiceAccount's RBAC.
- `pod_label_selector`: only watch pods matching this label selector, e.g.
  `"app.kubernetes.io/part-of=payments"`.
- `since_seconds`: how far back into existing logs to read when a
  pod/container is first discovered (mirrors `kubectl logs --since`).
  Three states:
  - unset / key absent (default): full available log history (whatever the
    kubelet still has retained), no bound.
  - `0`: fresh logs only — no historical backfill, just lines written after
    the stream connects.
  - `N > 0`: last `N` seconds of history.

  Set an explicit bound in production to avoid a thundering-herd re-read of
  full available log history across every container on collector restart.
- `reconnect_backoff.initial_interval` / `max_interval` / `max_elapsed_time`:
  exponential backoff applied when a log stream drops (pod restart, kubelet
  log rotation, transient API server error) before reconnecting. The delay
  starts at `initial_interval`, doubles each failed attempt, and is capped at
  `max_interval`. `max_elapsed_time` bounds the total time spent retrying a
  single stream through an unbroken run of failures: once it is exceeded the
  receiver gives up on that stream (a successful reconnect resets the clock).
  Set `max_elapsed_time: 0` to retry indefinitely. Independently of backoff, a
  stream is always stopped when the pod is deleted or reaches a terminal
  (`Succeeded`/`Failed`) phase.
- `max_batch_size` (default `1000`, `0` means use the default): the maximum
  number of log lines coalesced into a single `plog.Logs` / `ConsumeLogs` push
  per container stream. Each container's log stream is read independently and
  its lines share the same resource attributes, so they are batched into one
  payload instead of one push per line — at high line rates (e.g. 10k
  lines/sec) this avoids allocating a `ResourceLogs` and invoking the pipeline
  once per line. Larger values amortize per-push overhead further at the cost
  of more memory held per in-flight batch.
- `flush_interval` (default `200ms`, `0` means use the default): the maximum
  time a partially-filled batch waits before being forwarded. This bounds the
  latency a low-volume stream would otherwise incur waiting to accumulate
  `max_batch_size` lines, so batching never trades throughput for unbounded
  delivery delay.
- `max_log_size` (default `1048576` = 1 MiB, `0` means use the default): the
  maximum size, in bytes, of a single emitted log record body. It also bounds
  the per-stream read buffer, so a pathologically long line can't grow memory
  without limit. A physical log line longer than this is handled per
  `max_log_size_behavior` — never silently dropped.
- `max_log_size_behavior` (default `split`): what to do with a log line longer
  than `max_log_size`, mirroring the filelog receiver's option of the same
  name.
  - `split`: preserve all data by emitting the line as consecutive
    `max_log_size`-sized records. Nothing is lost; a very long line simply
    arrives as several records. Only the first record carries the line's
    original timestamp — continuation records are stamped with the receive
    time, since the kubelet emits the RFC3339 prefix only once per physical
    line.
  - `truncate`: emit the first `max_log_size` bytes of the line and drop the
    remainder up to the next newline. Use when a bounded head of each line is
    enough and you'd rather not fan a huge line out into many records.

The full field definitions live in [`config.go`](./config.go), with a
working sample in [`testdata/config.yaml`](./testdata/config.yaml).

## Running tests locally

### Unit tests

No external dependencies:

```bash
cd otel-components/k8spodlogreceiver
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
cd otel-components/k8spodlogreceiver
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

## Known limitations / open questions

- **API server load at scale**: one persistent HTTP stream per container.
  On managed clusters (EKS, GKE) the control plane is auto-scaled by the
  cloud provider and this is not a meaningful concern. On self-hosted
  clusters the standard solution is a kube-apiserver HA setup — a load
  balancer in front of multiple API server replicas (the kubeadm HA
  pattern) distributes the streaming connections across replicas and
  removes the single-instance bottleneck without changes to the collector.
- **Log rotation gaps**: if a stream is disconnected longer than the
  kubelet's retained rotated logs, some lines are unrecoverable. A
  hostPath-based collector doesn't have this limitation. Worth
  documenting as an explicit trade-off, not solving away.
- **Multiline / structured parsing**: this skeleton emits one log record
  per line with no stack-trace/multiline joining. Would need a
  stanza-based parsing operator pipeline, similar to `filelogreceiver`,
  layered on top.
- **Replicas > 1 is unsafe**: `activeStreams` tracking is in-process only,
  with no coordination across replicas. Running more than one collector
  replica means every replica independently discovers and streams the
  same pods, producing duplicate log records. HA requires distributed
  stream ownership (e.g. consistent-hash ring + lease coordination) —
  not yet implemented.

## Relation to the broader security-observability-stack project

This receiver is one component of a larger effort
(`../../helm`) to package a compliance-oriented,
multi-tenant observability stack (OTel Collector → Prometheus →
backend) with sane defaults for regulated environments. See
[`../../docs/architecture.md`](../../docs/architecture.md).
