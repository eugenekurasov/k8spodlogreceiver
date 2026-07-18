package k8spodlogreceiver

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/k8sconfig"
	"github.com/eugenekurasov/security-observability-stack/otel-components/k8spodlogreceiver/internal/metadata"
)

type logsReceiver struct {
	cfg      *Config
	settings receiver.Settings
	consumer consumer.Logs

	// kubernetes.Interface instead of *kubernetes.Clientset so tests can
	// inject fake.NewSimpleClientset() without a real API server.
	clientset kubernetes.Interface

	// httpClient is the transport backing clientset. Kept so Shutdown can
	// force its idle keep-alive connections closed — cancelling the
	// informer/stream context only aborts in-flight requests, it doesn't
	// close already-established connections sitting in the transport's
	// pool, which otherwise leak as goroutines blocked in IO wait.
	httpClient *http.Client

	cancel context.CancelFunc
	wg     sync.WaitGroup

	// activeStreams tracks which pod/container log streams are already
	// being tailed, so the informer's resync doesn't spawn duplicates.
	mu           sync.Mutex
	activeStreams map[string]context.CancelFunc

	// startStream is called for each newly discovered container. It is a
	// field so tests can substitute a no-op without a real API server.
	startStream func(ctx context.Context, namespace, podName, containerName, key string)

	// obsrep records the standard otelcol_receiver_accepted_log_records /
	// otelcol_receiver_refused_log_records metrics.
	obsrep *receiverhelper.ObsReport

	// telemetry records this receiver's component-specific metrics
	// (active_streams, stream_reconnects_total, stream_errors_total,
	// informer_events_total).
	telemetry *metadata.TelemetryBuilder
}

func newLogsReceiver(settings receiver.Settings, cfg *Config, c consumer.Logs) (receiver.Logs, error) {
	obsrep, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             settings.ID,
		Transport:              "http",
		ReceiverCreateSettings: settings,
	})
	if err != nil {
		return nil, fmt.Errorf("k8spodlogreceiver: building obsreport: %w", err)
	}

	telemetryBuilder, err := metadata.NewTelemetryBuilder(settings.TelemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("k8spodlogreceiver: building telemetry: %w", err)
	}

	r := &logsReceiver{
		cfg:          cfg,
		settings:     settings,
		consumer:     c,
		activeStreams: make(map[string]context.CancelFunc),
		obsrep:        obsrep,
		telemetry:     telemetryBuilder,
	}
	r.startStream = r.streamContainerLogs
	return r, nil
}

func (r *logsReceiver) Start(_ context.Context, _ component.Host) error {
	restCfg, err := r.buildRESTConfig()
	if err != nil {
		return fmt.Errorf("k8spodlogreceiver: building kube client config: %w", err)
	}

	httpClient, err := rest.HTTPClientFor(restCfg)
	if err != nil {
		return fmt.Errorf("k8spodlogreceiver: building kube HTTP client: %w", err)
	}
	r.httpClient = httpClient

	clientset, err := kubernetes.NewForConfigAndClient(restCfg, httpClient)
	if err != nil {
		return fmt.Errorf("k8spodlogreceiver: %w (%v)", errNoRBACHint, err)
	}
	r.clientset = clientset

	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	r.wg.Add(1)
	go r.runPodInformer(ctx)

	return nil
}

func (r *logsReceiver) Shutdown(_ context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	if r.httpClient != nil {
		r.httpClient.CloseIdleConnections()
	}
	return nil
}

func (r *logsReceiver) buildRESTConfig() (*rest.Config, error) {
	return k8sconfig.CreateRestConfig(r.cfg.APIConfig)
}
