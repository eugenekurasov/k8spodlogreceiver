// Package k8sapilogreceiver collects Kubernetes pod logs by streaming them
// through the Kubernetes API server (the same path `kubectl logs -f` uses),
// instead of requiring a DaemonSet with a read-only host-root mount.
//
// Motivation
//
// The existing proposal for a k8slog receiver
// (open-telemetry/opentelemetry-collector-contrib#23339) collects logs by
// mounting the node's log directory into a DaemonSet pod. Reviewers on
// that issue raised a security concern: granting a workload read access
// to the entire host filesystem is a broad privilege for what is
// functionally a narrow task (reading container log files).
//
// This receiver instead uses the API-server-mediated log endpoint
// (corev1.PodInterface.GetLogs), which:
//
//   - requires no hostPath volume, no privileged securityContext, and no
//     DaemonSet (a normal Deployment/StatefulSet is sufficient);
//   - is scoped by standard Kubernetes RBAC on the "pods/log" subresource,
//     so access can be restricted per-namespace or per-label-selector —
//     a natural fit for multi-tenant clusters with compliance
//     requirements (e.g., SOC 2 log-access boundaries);
//   - reuses the same code path already trusted for `kubectl logs`.
//
// Trade-offs (see README.md for details): higher API server load at
// scale, and reliance on the kubelet's log retention window rather than
// direct file access, which means aggressive log rotation on the node can
// cause small gaps if the receiver is disconnected for longer than the
// kubelet retains rotated logs.
package k8sapilogreceiver
