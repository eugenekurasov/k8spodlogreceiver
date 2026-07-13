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

| Kubernetes | Status |
|---|---|
| 1.36 | CI configured — pending first run |
| 1.35 | CI configured — pending first run |
| 1.34 | CI configured — pending first run |

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
    rate_limit:
      qps: 5
      burst: 10

exporters:
  otlp:
    endpoint: "otel-gateway:4317"

service:
  pipelines:
    logs:
      receivers: [k8s_podlog]
      exporters: [otlp]
```

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
- Not yet submitted upstream. Intent is to prototype, test against a
  real cluster, and open a discussion on #23339 proposing this as an
  additional collection mode rather than a replacement.

## Relation to the broader security-observability-stack project

This receiver is one component of a larger effort
(`../../terraform`, `../../helm`) to package a compliance-oriented,
multi-tenant observability stack (OTel Collector → Prometheus →
backend) with sane defaults for regulated environments. See
[`../../docs/architecture.md`](../../docs/architecture.md).
