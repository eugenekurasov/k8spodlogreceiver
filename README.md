# k8spodlog receiver

An OpenTelemetry Collector receiver that streams Kubernetes pod logs via
the Kubernetes API server — the same mechanism `kubectl logs -f` uses —
instead of mounting the host filesystem or requiring a DaemonSet with
node-level access.

## Status

| | |
|---|---|
| Stability | [development](https://github.com/open-telemetry/opentelemetry-collector/blob/main/docs/component-stability.md#development): logs |
| Supported signals | logs |
| Collector core | pinned to v1.65.0 / v0.159.0 |

## Kubernetes version compatibility

[![K8s compatibility tests](https://github.com/eugenekurasov/k8spodlogreceiver/actions/workflows/integration.yml/badge.svg?branch=main)](https://github.com/eugenekurasov/k8spodlogreceiver/actions/workflows/integration.yml)

All APIs used by this receiver are stable `core/v1` endpoints (`pods`,
`pods/log`) present and unchanged since Kubernetes 1.3. Label selectors
on List/Watch are stable since 1.0. There are no alpha or beta API
dependencies.

## Why this exists

[open-telemetry/opentelemetry-collector-contrib#23339](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/23339)
proposes a `k8slog` receiver that mounts the node's log directory
(`hostPath`) into a DaemonSet pod. Reviewers flagged this as a broad
privilege grant for a narrow task. This receiver takes the alternative:
it collects logs purely through the API server, scoped by ordinary
Kubernetes RBAC on the `pods/log` subresource.

The same idea was proposed before, in
[open-telemetry/opentelemetry-collector-contrib#24641](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/24641)
("New component: Kubernetes api logs receiver"), but that issue was closed
as inactive without an implementation.

Because access is mediated entirely by the API server and scoped by RBAC, a
single cluster can run per-tenant collector instances authorized to read logs
only for their own namespace(s) — log-access segregation between tenants
becomes an RBAC object that can be inspected and audited, rather than a
configuration convention enforced by a trusted node-level agent.

|                            | hostPath + DaemonSet         | k8spodlog (this project)                              |
|----------------------------|------------------------------|-------------------------------------------------------|
| Node filesystem access     | Yes (read-only host mount)   | None                                                  |
| Deployment shape           | DaemonSet (one per node)     | Deployment (one per tenant, any node)                 |
| GPU / specialized nodes    | Collector scheduled on every node, including expensive GPU nodes | No collector pod on GPU nodes — they stay 100% dedicated to workloads |
| Compute cost               | One collector pod per node   | One (or few) pods per tenant, on cheap CPU nodes      |
| Intra-cluster network      | None (local file read)       | Log data traverses the API server — negligible on EKS/GKE (managed, auto-scaled control plane); worth planning for on self-hosted clusters with a fixed-spec API server |
| RBAC granularity           | Node-level                   | Namespace / label-selector scoped                     |
| Serverless node pools (Fargate, AKS Virtual Nodes, GCP Autopilot) | Not supported — DaemonSets are blocked or restricted on these platforms; `hostPath` mounts are unavailable | Fully supported — plain Deployment, no DaemonSet or `hostPath` required; API endpoint is the same regardless of underlying node type |
| Log continuity on rotation | No network in the read path, but still bounded by the runtime's log GC — and `filelog`'s default `on_truncate: ignore` skips data written after a copytruncate rotation | Reconnects resume from the last delivered line, so ordinary drops cost nothing; lines are lost only when an outage outlasts kubelet retention |

## Intentional scope

This receiver collects **application container logs only** — what a pod writes to stdout/stderr. It streams every container in a discovered pod: regular containers, init containers (migrations, secret fetchers), and native sidecars (init containers with `restartPolicy: Always`). A stream is opened only once a container has actually started (it is Running, or a previous instance terminated and left logs); while a container is still `ContainerCreating` / pulling its image the receiver attempts no connection — the pod update emitted when the container starts triggers the stream. It does not and cannot collect:

- Node logs (systemd journal, kubelet, containerd daemon logs) — these live on the host filesystem and require hostPath access
- Control plane logs (kube-apiserver, etcd, scheduler)
- Any logs below the pod/container API boundary

This is a deliberate scope choice. The target user is a **tenant team** that wants full visibility into their own application pods without any node-level privilege granted. If node-level log collection is required, it belongs in a separate cluster-operator-managed pipeline with explicitly granted node access — not mixed into per-tenant collectors.

## Quick start

### Building a collector

This receiver is a Go module, not a binary. To use it you assemble a Collector
that includes it, with the
[OpenTelemetry Collector Builder](https://opentelemetry.io/docs/collector/extend/ocb/):

```bash
go install go.opentelemetry.io/collector/cmd/builder@v0.159.0
builder --config builder-config.yaml
```

[`builder-config.yaml`](./builder-config.yaml) in this repo builds a local
collector from the working tree (`path: ./`) and is what CI builds on every
change. To use a released version instead, drop the `path` line and pin the
tag:

```yaml
receivers:
  - gomod: github.com/eugenekurasov/k8spodlogreceiver v0.1.0
```

Keep the collector component versions in your builder config aligned with the
ones this module pins (core `v1.65.0` / `v0.159.0`) — a mismatch surfaces as
confusing build errors rather than a clear version conflict.

### Configuration

```yaml
receivers:
  k8s_pod_log:
    namespaces: ["payments", "billing"]
    pod_label_selector: "app.kubernetes.io/part-of=payments-platform"
    since_seconds: 300

exporters:
  otlp:
    endpoint: "otel-gateway:4317"

service:
  pipelines:
    logs:
      receivers: [k8s_pod_log]
      exporters: [otlp]
```

### Service Account

The collector authenticates to the API server with its pod's ServiceAccount
token (`auth_type: serviceAccount`, the default). Create one in the namespace
the collector runs in:

```bash
<<EOF | kubectl apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: k8spodlog-collector
  namespace: observability
EOF
```

Reference it from your collector's pod spec with `serviceAccountName:
k8spodlog-collector`. The examples below use these names — substitute your own.

### RBAC

The receiver needs two permissions: `get`/`list`/`watch` on `pods` (the
discovery informer) and `get` on `pods/log` (the log streams themselves).
Without them the API server returns 403 and the receiver reports
`otelcol_log_connection_errors_total{reason="rbac_denied"}`.

To collect across the whole cluster, bind a ClusterRole:

```bash
<<EOF | kubectl apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: k8spodlog-collector
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list", "watch"]
- apiGroups: [""]
  resources: ["pods/log"]
  verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: k8spodlog-collector
subjects:
- kind: ServiceAccount
  name: k8spodlog-collector
  namespace: observability
roleRef:
  kind: ClusterRole
  name: k8spodlog-collector
  apiGroup: rbac.authorization.k8s.io
EOF
```

To scope collection to specific namespaces instead, create a Role and
RoleBinding with the same `rules` in each namespace you want to collect from,
and set `namespaces` in the receiver config to match. The API server then
enforces the boundary: the collector cannot read logs outside those namespaces
even if it is misconfigured to try.

## Emitted data

Each log line becomes one `plog` log record. Lines from the same container
share a `ResourceLogs` with these resource attributes:

| Attribute | Value |
|-----------|-------|
| `k8s.namespace.name` | Namespace of the pod |
| `k8s.pod.name` | Pod name |
| `k8s.pod.uid` | Pod UID — stable across a rename, distinguishes two pods that reuse a name |
| `k8s.container.name` | Container name, including init containers and native sidecars |

The instrumentation scope name is
`github.com/eugenekurasov/k8spodlogreceiver`.

Per record:

- **Body**: the log line as a string, with the kubelet's RFC3339 timestamp
  prefix removed. The receiver requests logs with `Timestamps: true` and strips
  the prefix it asked for, so the body is the line your application actually
  wrote.
- **Timestamp**: parsed from that prefix — the time the *container* emitted the
  line. If the leading token does not parse as RFC3339, the receiver falls back
  to the receive time and leaves the body untouched, prefix included, rather
  than guessing where the timestamp ends.
- **ObservedTimestamp**: when the receiver read the line. Comparing the two
  gives you the collection lag, which is the quickest way to spot a stream that
  is replaying backlog after a reconnect.

Two fields are deliberately **not** set:

- **Severity** (`SeverityNumber` / `SeverityText`) is never populated — every
  record is emitted unspecified. This receiver does no parsing of the line
  itself. Derive severity downstream with a transform processor, or with the
  stanza operators the `filelog` receiver exposes.
- **`log.iostream`** (stdout vs stderr) is absent, and cannot be added. The
  `pods/log` API returns the two streams merged with no per-line marker; the
  distinction exists in the container runtime's on-disk format but is not
  carried through the API server endpoint this receiver uses. See
  [Known limitations](#known-limitations).

For the metrics the receiver emits about *itself*, see
[`documentation.md`](./documentation.md).

## Configuration reference

This section documents the receiver's full configuration surface. Every
field is optional; the [Quick start](#quick-start) config above is a minimal
subset.

```yaml
receivers:
  k8s_pod_log:
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
    retry_on_failure:
      enabled: true
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
- `since_seconds` (default `300`): how far back into existing logs to read
  when a pod/container is first discovered (mirrors `kubectl logs --since`).
  - unset / key absent: `300` — the last five minutes, enough to cover a
    collector restart or a rollout without re-reading everything.
  - `0`: fresh logs only — no historical backfill, just lines written after
    the stream connects.
  - `N > 0`: last `N` seconds of history.

  The bound matters because the receiver rediscovers every container at once
  on startup: an unbounded read would ask the kubelet for each container's
  full retained history simultaneously. Raise it if you need deeper backfill,
  and use a large value (a day, a year) if you want everything the kubelet
  still holds — there is no separate "unbounded" setting, and a value beyond
  the retention window is equivalent to one.
- `pod_resync_period`: how often the pod informer re-delivers every pod it has
  cached, which re-runs the receiver's stream bookkeeping. Because that
  bookkeeping is idempotent, a resync doubles as a self-healing sweep: a
  container stream that gave up (see `reconnect_backoff.max_elapsed_time`) is
  started again on the next resync rather than waiting for that pod to change.
  Resyncs are served from the informer's local cache and cost no API server
  traffic. Three states:
  - unset / key absent (default): 10 minutes.
  - `0`: no resync — streams start only on real pod events.
  - `N > 0`: resync every `N`.
- `reconnect_backoff.initial_interval` / `max_interval` / `max_elapsed_time`:
  exponential backoff applied when a log stream drops (pod restart, kubelet
  log rotation, transient API server error) before reconnecting. The delay
  starts at `initial_interval`, doubles each failed attempt, and is capped at
  `max_interval`. `max_elapsed_time` bounds the total time spent retrying a
  single stream through an unbroken run of failures: once it is exceeded the
  receiver gives up on that stream (a successful reconnect resets the clock).
  Set `max_elapsed_time: 0` to retry indefinitely. Independently of backoff, a
  container's stream is always stopped when the pod is deleted or when that
  container has permanently terminated — evaluated per container from its own
  `ContainerStatus`, not from the pod's `Succeeded`/`Failed` phase, so a
  finished container in a still-`Running` pod (e.g. one container of a
  multi-container `Job` with `restartPolicy: Never`) stops reconnecting
  immediately instead of hammering the API server. The check is restart-policy
  aware: a CrashLooping container (`restartPolicy: Always`), an `OnFailure`
  container that exited non-zero, and native sidecars (init containers with
  `restartPolicy: Always`) are not treated as terminal, so their streams keep
  following restarts.
- `retry_on_failure.enabled` (default `true`) / `initial_interval` /
  `max_interval` / `max_elapsed_time`: what happens when the *pipeline* refuses
  a batch, as opposed to the API stream dropping (which `reconnect_backoff`
  covers). The common case is `memory_limiter` shedding load: it returns
  `data refused due to high memory usage` and reports itself as a *recoverable*
  error — "slow down and come back", not "this data is bad". With retry enabled
  the batch is re-sent with jittered exponential backoff until it is accepted,
  instead of being dropped. Because the retry blocks the container's read loop,
  it also produces the backpressure `memory_limiter` is asking for: the receiver
  stops reading that stream's socket, and the kubelet holds the data. Ordering
  within a container stream is preserved — no new lines are read while a refused
  batch is outstanding.

  `max_elapsed_time` bounds the total time spent trying to send one batch,
  including retries, before it is discarded; `0` retries indefinitely.

  When `max_elapsed_time` does expire, the receiver **closes that container's
  stream and reconnects from the last delivered timestamp**, rather than
  reading on. This is deliberate: the cursor only advances on a delivered
  batch, so continuing to read would let the next successful flush move it past
  the refused records, and no future `SinceTime` would ever ask the kubelet for
  them again — they would be unreachable even though the kubelet still has
  them. Reconnecting re-reads them instead. The cost is duplicates, because
  `SinceTime` is truncated to whole seconds; the trade is deliberate, since
  duplicate records are recoverable downstream and dropped ones are not. It is
  logged at warn level as `pipeline refused a batch, reconnecting to re-read
  it`, which is a reliable signal that the pipeline cannot keep up with the
  configured stream count and batch size.
  Disabling this restores drop-on-refusal, which loses data under memory
  pressure — it is on by default for that reason. This mirrors the
  `retry_on_failure` block the `filelog` receiver and the other stanza-based
  receivers expose, except that those default it to `false`.

  Note the interaction with `max_batch_size`: a retrying stream holds its batch
  in memory for the duration, so the memory floor while the pipeline is refusing
  is roughly `max_batch_size` × number of concurrently retrying streams. Keep
  `max_batch_size` modest when collecting cluster-wide.
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

## Troubleshooting

Each symptom below maps to a knob documented in the
[Configuration reference](#configuration-reference) — see there for the full
explanation of any field named here. The receiver's own telemetry (the
`otelcol_*` metrics referenced below) is specified in
[`documentation.md`](./documentation.md).

| Symptom | Cause | What to do |
|---------|-------|------------|
| `client-side throttling` warnings in collector logs | Reconnect attempts exceed client-go's rate limiter. Note this bounds *new* connections, not concurrently open streams. | Raise `api_config.kube_api_qps` and `kube_api_burst` |
| `pipeline refused a batch, reconnecting to re-read it` (warn) | `retry_on_failure.max_elapsed_time` expired while the pipeline kept refusing — a reliable signal the pipeline can't keep up with the configured stream count and batch size | Lower `max_batch_size`, narrow `namespaces` / `pod_label_selector`, or scale the downstream pipeline |
| `otelcol_log_connection_errors_total{reason="rbac_denied"}` climbing | The ServiceAccount lacks `get` on `pods/log`, or the pod is outside the namespaces its Role covers | Check the [RBAC](#rbac) grant |
| `otelcol_log_connection_errors_total{reason="pod_gone"}` climbing | A connect attempt got `404` — the pod is gone but the informer's cache still lists it | Expected in bursts during rollouts; see [Interpreting the metrics](#interpreting-the-metrics) for when it isn't |
| Memory growth while `memory_limiter` is shedding load | Each retrying stream holds its batch in memory: roughly `max_batch_size` x concurrently retrying streams | Keep `max_batch_size` modest when collecting cluster-wide |
| Duplicate records after a refused batch | `SinceTime` is truncated to whole seconds when re-reading | Working as intended — duplicates are recoverable downstream, dropped records are not |
| Large history re-read on every collector restart | No cursor persistence; discovery re-reads from `since_seconds` | Set an explicit `since_seconds` bound in production |
| Duplicate records across collector replicas | `activeStreams` is in-process only, with no cross-replica coordination | Run a single replica — see [Known limitations](#known-limitations) |
| A container's stream stopped and never resumed | `reconnect_backoff.max_elapsed_time` was exceeded for that stream | The next `pod_resync_period` sweep restarts it; set `max_elapsed_time: 0` to retry indefinitely |

### Interpreting the metrics

**`otelcol_active_log_streams`** is the one number to put on a dashboard
before anything goes wrong. In steady state it should equal the number of
*started containers* — regular, init, and native sidecar — across the pods
matched by `namespaces` and `pod_label_selector`. Compare it against
`kube_pod_container_status_running` filtered the same way; a persistent gap in
either direction is the earliest signal available.

The important subtlety: this gauge counts streams the receiver is *managing*,
not streams that are *delivering*. A stream stays counted for the whole time
it is failing and backing off — its bookkeeping entry is only released once
the stream gives up entirely (`reconnect_backoff.max_elapsed_time`) or its pod
is deleted. So the gauge alone cannot distinguish healthy from stuck. Read it
against the error counter:

| `active_log_streams` | `log_connection_errors_total` | Reading |
|---|---|---|
| Matches container count | Flat | Healthy |
| Matches container count | Climbing | Streams are held open but retrying, not delivering — check `reason` |
| Below container count | Flat | Containers are matched but never streamed — check `pod_label_selector` and RBAC |
| Below container count | Climbing `rbac_denied` | Streams are being refused and abandoned — the Role is too narrow |
| Above container count | Any | Streams outliving their containers, or more than one replica running |
| Zero, non-zero before | Any | The informer stopped delivering, or the selector no longer matches anything |

**`otelcol_log_connection_errors_total{reason="pod_gone"}`** is the one whose
meaning genuinely depends on shape, so it is worth reading carefully. It is
recorded when a connect attempt returns `404 NotFound`, which means the
receiver tried to open a stream for a pod the API server no longer has. That
is a race, not a fault: the informer's cache still lists a pod that has since
been deleted.

- **Bursts that decay** — normal. Every rollout, scale-down, or Job completion
  produces them. The pod-deleted event arrives shortly after and the stream is
  torn down. Nothing to do.
- **A steady, non-decaying rate** — investigate. It means dead pods are being
  retried indefinitely, which happens when delete events aren't arriving or
  aren't being applied: a broken or stale watch, or a `pod_resync_period`
  sweep repeatedly restarting streams from a cache that never converged.
  Check that `otelcol_pod_discovery_events_total{event_type="deleted"}` is
  advancing during churn; if adds advance but deletes don't, the watch is the
  problem, not the streams.

Note that this counter increments **per connect attempt, not per pod** — one
genuinely-gone pod contributes several increments as backoff retries it. The
absolute rate is therefore shaped by your `reconnect_backoff` settings, so
compare it against itself over time rather than against another deployment's.

The remaining reasons are simpler: `rbac_denied` is a `403` and always
actionable; `other` is everything else, most often a transient API server or
network error, and is only interesting if it fails to decay.

## Known limitations

- **API server load at scale**: one persistent HTTP stream per container.
  On managed clusters (EKS, GKE) the control plane is auto-scaled by the
  cloud provider and this is not a meaningful concern. On self-hosted
  clusters the standard solution is a kube-apiserver HA setup — a load
  balancer in front of multiple API server replicas (the kubeadm HA
  pattern) distributes the streaming connections across replicas and
  removes the single-instance bottleneck without changes to the collector.
- **Log rotation gaps**: if a stream is disconnected for longer than the
  kubelet retains rotated logs, the lines written in that window are
  unrecoverable. This is a trade-off of API-server-mediated collection, not a
  bug to be fixed: without node access there is nothing to read but what the
  kubelet still holds. Bound the exposure by keeping `reconnect_backoff`
  aggressive and the kubelet's `containerLogMaxFiles` / `containerLogMaxSize`
  generous.

  File-based collection is not immune to the same class of failure — it races
  the runtime's log GC just as this does, and `filelog`'s default
  `on_truncate: ignore` silently skips data written after a copytruncate
  rotation until the file grows past the old offset. The difference is
  exposure, not kind. This receiver's path is longer — collector → load
  balancer → API server → kubelet → runtime log file — so it disconnects more
  often: API server rollouts, kubelet restarts, and load balancer idle
  timeouts all end a stream that a reader on the node would never have noticed.

  Ordinary disconnects cost nothing. `reconnect_backoff` reconnects and
  `SinceTime` resumes from the last delivered line, so a control plane restart
  or an idle-timeout reap is a gap of seconds. Lines are only lost when an
  outage outlasts the kubelet's retention window — a sustained network
  partition, or the collector itself being down long enough — which is the
  no-cursor-persistence limitation below, not this one.

- **Previous container instance logs not recovered on restart**: when a
  container restarts, logs from its prior instance are not backfilled. The
  receiver streams only the current instance — it does not set
  `Previous: true` in `PodLogOptions`, so it never requests the
  `?previous=true` variant of the API server's pod log endpoint. Any lines
  emitted by the crashed instance before the new stream attaches are lost.
- **No cursor persistence across restarts**: reconnect position is held in
memory (bounded by `since_seconds`); after a collector restart, history is
  re-read within that window (possible duplicates) or lost beyond it. There
  is no persistent checkpoint equivalent to `filelogreceiver`'s file offset
  storage.
- **stdout and stderr are indistinguishable**: the `pods/log` endpoint
  returns both streams merged, with no per-line marker identifying which one
  a line came from. The container runtime records it on disk, but the API
  server does not expose it, so no `log.iostream` attribute can be set. A
  hostPath-based collector reading the CRI log format directly does not have
  this limitation.
- **Multiline / structured parsing**: the receiver emits one log record per
  line and does no stack-trace or multiline joining. Join multiline records
  downstream, or layer a stanza-based parsing operator pipeline on top as
  `filelogreceiver` does.
- **Run a single replica**: stream ownership is tracked in-process, so two
  replicas would each discover and stream the same pods, producing duplicate
  records. Scale by partitioning instead — one collector per tenant or
  namespace group, which is the deployment shape this receiver is built for
  anyway. Cross-replica stream ownership (consistent-hash ring plus lease
  coordination) would be required for HA within a single scope.

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

The files under `internal/metadata`, `internal/metadatatest`, and the
`generated_*.go` files are produced by `mdatagen` from
[`metadata.yaml`](metadata.yaml); regenerate them rather than editing them.

All other code in this repository is original to it and licensed under
Apache-2.0.
