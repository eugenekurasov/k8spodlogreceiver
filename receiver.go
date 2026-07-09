package k8sapilogreceiver

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type logsReceiver struct {
	cfg      *Config
	settings receiver.Settings
	consumer consumer.Logs

	// kubernetes.Interface instead of *kubernetes.Clientset so tests can
	// inject fake.NewSimpleClientset() without a real API server.
	clientset kubernetes.Interface

	cancel context.CancelFunc
	wg     sync.WaitGroup

	// activeStreams tracks which pod/container log streams are already
	// being tailed, so the informer's resync doesn't spawn duplicates.
	mu           sync.Mutex
	activeStreams map[string]context.CancelFunc

	// startStream is called for each newly discovered container. It is a
	// field so tests can substitute a no-op without a real API server.
	startStream func(ctx context.Context, namespace, podName, containerName, key string)
}

func newLogsReceiver(settings receiver.Settings, cfg *Config, c consumer.Logs) (receiver.Logs, error) {
	r := &logsReceiver{
		cfg:          cfg,
		settings:     settings,
		consumer:     c,
		activeStreams: make(map[string]context.CancelFunc),
	}
	r.startStream = r.streamContainerLogs
	return r, nil
}

func (r *logsReceiver) Start(_ context.Context, _ component.Host) error {
	restCfg, err := r.buildRESTConfig()
	if err != nil {
		return fmt.Errorf("k8sapilogreceiver: building kube client config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("k8sapilogreceiver: %w (%v)", errNoRBACHint, err)
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
	return nil
}

func (r *logsReceiver) buildRESTConfig() (*rest.Config, error) {
	var cfg *rest.Config
	var err error

	switch {
	case r.cfg.APIConfig.InCluster:
		cfg, err = rest.InClusterConfig()
	case r.cfg.APIConfig.KubeconfigPath != "":
		cfg, err = clientcmd.BuildConfigFromFlags("", r.cfg.APIConfig.KubeconfigPath)
	default:
		// No explicit path: honour KUBECONFIG env then fall back to ~/.kube/config,
		// matching the behaviour of kubectl and client-go's standard tooling.
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			clientcmd.NewDefaultClientConfigLoadingRules(),
			nil,
		).ClientConfig()
	}
	if err != nil {
		return nil, err
	}

	cfg.QPS = r.cfg.RateLimit.QPS
	cfg.Burst = r.cfg.RateLimit.Burst
	return cfg, nil
}
