package server

import (
	"fmt"
	"github.com/onflow/flow-go/engine/access/subscription"
	"net"

	"github.com/bluesign/tinyAN/storage"

	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow/protobuf/go/flow/access"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"

	goAccess "github.com/onflow/flow-go/access"
	mockModule "github.com/onflow/flow-go/module/mock"
)

type GRPCServer struct {
	logger     zerolog.Logger
	host       string
	port       int
	grpcServer *grpc.Server
	listener   net.Listener
}
type mockHeaderCache struct {
}

func (mockHeaderCache) Get() *flow.Header {
	return &flow.Header{}
}
func NewGRPCServer(chainID flow.ChainID, store *storage.HeightBasedStorage, host string, port int) *GRPCServer {
	grpcServer := grpc.NewServer()

	me := new(mockModule.Local)
	me.On("NodeID").Return(flow.ZeroID)

	logger := zerolog.Logger{}
	adapter := &AccessAdapter{
		logger: logger,
		store:  store,
	}

	access.RegisterAccessAPIServer(grpcServer,
		goAccess.NewHandler(adapter,
			flow.Mainnet.Chain(), mockHeaderCache{}, me, subscription.DefaultMaxGlobalStreams,
		))

	return &GRPCServer{
		logger:     logger,
		host:       host,
		port:       port,
		grpcServer: grpcServer,
	}
}

func (g *GRPCServer) Server() *grpc.Server {
	return g.grpcServer
}

func (g *GRPCServer) Listen() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", g.host, g.port))
	if err != nil {
		return err
	}
	g.listener = lis
	return nil
}

func (g *GRPCServer) Start() error {
	if g.listener == nil {
		if err := g.Listen(); err != nil {
			fmt.Println(err)
			return err
		}
	}
	fmt.Println("started GRPC")
	g.logger.Info().Int("port", g.port).Msgf("✅  Started gRPC server on port %d", g.port)

	err := g.grpcServer.Serve(g.listener)
	if err != nil {
		return err
	}

	return nil
}

func (g *GRPCServer) Stop() {
	g.grpcServer.GracefulStop()
}
