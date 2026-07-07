module github.com/YOUR_GITHUB_HANDLE/security-observability-stack/otel-components/k8sapilogreceiver

go 1.26

// NOTE: pin these to the exact versions used by the collector-contrib
// release you build against via the OpenTelemetry Collector Builder
// (ocb) manifest — mismatched pdata/component versions are the most
// common build breakage when adding a custom component.
//
// As of collector v0.156.0, the stable modules (component, consumer,
// pdata, receiver) moved to independent v1.x versioning (v1.62.0),
// while the collector core itself remains at v0.x (v0.156.0).
require (
	go.opentelemetry.io/collector/component v1.62.0
	go.opentelemetry.io/collector/consumer v1.62.0
	go.opentelemetry.io/collector/pdata v1.62.0
	go.opentelemetry.io/collector/receiver v1.62.0
	go.uber.org/zap v1.27.0
	k8s.io/api v0.30.2
	k8s.io/apimachinery v0.30.2
	k8s.io/client-go v0.30.2
)

require (
	github.com/stretchr/testify v1.10.0
	go.opentelemetry.io/collector/consumer/consumertest v0.156.0
	go.opentelemetry.io/collector/receiver/receivertest v0.156.0
)
