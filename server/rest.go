package server

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/onflow/flow-go/engine/access/rest"
	"github.com/onflow/flow-go/engine/access/rest/routes"
	"github.com/onflow/flow-go/engine/access/state_stream"
	"github.com/onflow/flow-go/engine/access/state_stream/backend"
	"github.com/onflow/flow-go/engine/access/subscription"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/module"
	"github.com/onflow/flow-go/module/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

type RestServer struct {
	logger   zerolog.Logger
	host     string
	port     int
	server   *http.Server
	listener net.Listener
}

func (r *RestServer) Listen() error {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", r.host, r.port))
	if err != nil {
		return err
	}
	r.listener = l
	return nil
}

func (r *RestServer) Start() error {
	if r.listener == nil {
		if err := r.Listen(); err != nil {
			return err
		}
	}

	r.logger.Info().
		Int("port", r.port).
		Msgf("✅  Started REST API server on port %d", r.port)

	err := r.server.Serve(r.listener)
	if err != nil {
		return err
	}
	return nil
}

func (r *RestServer) Stop() {
	_ = r.server.Shutdown(context.Background())
}

func NewRestServer(logger zerolog.Logger, chain flow.Chain, adapter *AccessAdapter, host string, port int, debug bool) (*RestServer, error) {

	debugLogger := zerolog.Logger{}
	if debug {
		debugLogger = zerolog.New(os.Stdout)
	}
	var restCollector module.RestMetrics = metrics.NewNoopCollector()

	// only collect metrics if not test
	if flag.Lookup("test.v") == nil {
		var err error
		restCollector, err = metrics.NewRestCollector(routes.URLToRoute, prometheus.DefaultRegisterer)
		if err != nil {
			return nil, err
		}
	}

	streamConfig := backend.Config{
		EventFilterConfig:    state_stream.DefaultEventFilterConfig,
		RpcMetricsEnabled:    false,
		MaxGlobalStreams:     subscription.DefaultMaxGlobalStreams,
		ClientSendTimeout:    subscription.DefaultSendTimeout,
		ClientSendBufferSize: subscription.DefaultSendBufferSize,
		ResponseLimit:        subscription.DefaultResponseLimit,
		HeartbeatInterval:    subscription.DefaultHeartbeatInterval,
	}

	srv, err := rest.NewServer(
		adapter,
		rest.Config{
			ListenAddress: fmt.Sprintf("%s:3333", host),
			WriteTimeout:  rest.DefaultWriteTimeout,
			ReadTimeout:   rest.DefaultReadTimeout,
			IdleTimeout:   rest.DefaultIdleTimeout,
		},
		debugLogger,
		chain,
		restCollector,
		nil,
		streamConfig,
	)

	if err != nil {
		return nil, err
	}

	return &RestServer{
		logger: logger,
		host:   host,
		port:   port,
		server: srv,
	}, nil
}
