package server

import (
	"context"
	"github.com/bluesign/tinyAN/storage"
	"github.com/gorilla/mux"
	"github.com/onflow/flow-evm-gateway/api"
	"github.com/onflow/go-ethereum/rpc"
	"net"
	"net/http"
)

type EVMServer struct {
	router     *mux.Router
	httpServer *http.Server
	rpcServer  *rpc.Server
	listener   net.Listener
	storage    *storage.ProtocolStorage
}

type APINamespace struct {
	storage *storage.ProtocolStorage
}

func (a *APINamespace) GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, full bool) (*api.Block, error) {
	return &api.Block{
		Number: 1,
	}, nil
}
