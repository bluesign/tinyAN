package server

import (
	"fmt"
	"net"

	"github.com/bluesign/tinyAN/storage"

	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow/protobuf/go/flow/access"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

type GRPCServer struct {
	logger     *zerolog.Logger
	host       string
	port       int
	grpcServer *grpc.Server
	listener   net.Listener
}

func NewGRPCServer(chainID flow.ChainID, client *grpc.ClientConn, store *storage.ProtocolStorage, host string, port int) *GRPCServer {
	grpcServer := grpc.NewServer()

	logger := &zerolog.Logger{}
	accessClient := access.NewAccessAPIClient(client)

	access.RegisterAccessAPIServer(grpcServer, NewHandler(chainID, store, accessClient))

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
