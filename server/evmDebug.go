package server

import (
	"context"
	"fmt"
	"github.com/bluesign/tinyAN/storage"
	"github.com/onflow/cadence/runtime/common"
	"github.com/onflow/flow-evm-gateway/models"
	errs "github.com/onflow/flow-evm-gateway/models/errors"
	"github.com/onflow/go-ethereum/common/hexutil"
	"github.com/onflow/go-ethereum/crypto"
	"os"

	"github.com/goccy/go-json"
	gethCommon "github.com/onflow/go-ethereum/common"
	"github.com/onflow/go-ethereum/eth/tracers"
	"github.com/onflow/go-ethereum/rpc"
	"github.com/rs/zerolog"
)

// txTraceResult is the result of a single transaction trace.
type txTraceResult struct {
	TxHash gethCommon.Hash `json:"txHash"`           // transaction hash
	Result interface{}     `json:"result,omitempty"` // Trace results produced by the tracer
	Error  string          `json:"error,omitempty"`  // Trace failure produced by the tracer
}

type DebugAPI struct {
	logger zerolog.Logger
	store  *storage.HeightBasedStorage
}

func NewDebugAPI(store *storage.HeightBasedStorage) *DebugAPI {
	return &DebugAPI{
		logger: zerolog.New(os.Stdout).With().Timestamp().Logger(),
		store:  store,
	}
}

func (d *DebugAPI) TraceTransaction(
	_ context.Context,
	tx models.Transaction,
	_ *tracers.TraceConfig,
) (json.RawMessage, error) {

	return json.RawMessage{}, nil
}

func (d *DebugAPI) TraceBlockByNumber(
	ctx context.Context,
	number rpc.BlockNumber,
	cfg *tracers.TraceConfig,
) ([]*txTraceResult, error) {
	height := uint64(0)
	if number.Int64() >= 0 {
		height = uint64(number.Int64())
	} else {
		height = d.store.Latest().EVM().LastProcessedHeight()
	}

	block, err := d.store.StorageForEVMHeight(height).EVM().GetEvmBlockByHeight(height)
	if err != nil {
		return handleError[[]*txTraceResult](err)
	}

	return d.traceBlock(ctx, block, cfg)
}

func (d *DebugAPI) TraceBlockByHash(
	ctx context.Context,
	hash gethCommon.Hash,
	cfg *tracers.TraceConfig,
) ([]*txTraceResult, error) {

	var height uint64 = 0
	var err error
	for _, spork := range d.store.Sporks() {
		height, err = spork.EVM().GetEVMHeightFromHash(hash)
		if err == nil {
			break
		}
	}

	if err != nil {
		return handleError[[]*txTraceResult](errs.ErrEntityNotFound)
	}

	block, err := d.store.StorageForEVMHeight(height).EVM().GetEvmBlockByHeight(height)
	if err != nil {
		return handleError[[]*txTraceResult](err)
	}

	return d.traceBlock(ctx, block, cfg)
}

func (d *DebugAPI) traceBlock(
	ctx context.Context,
	block *storage.EVMBlock,
	_ *tracers.TraceConfig,
) ([]*txTraceResult, error) {

	results := make([]*txTraceResult, len(block.Transactions))
	for i, txBytes := range block.Transactions {
		tx, err := models.UnmarshalTransaction(txBytes)
		if err != nil {
			continue
		}
		txTrace, err := d.TraceTransaction(ctx, tx, nil)

		if err != nil {
			results[i] = &txTraceResult{TxHash: tx.Hash(), Error: err.Error()}
		} else {
			results[i] = &txTraceResult{TxHash: tx.Hash(), Result: txTrace}
		}
	}

	return results, nil
}

// NetAPI offers network related RPC methods
type NetAPI struct {
}

// Listening returns an indication if the node is
// listening for network connections.
func (s *NetAPI) Listening() bool {
	return true // always listening
}

// PeerCount returns the number of connected peers
func (s *NetAPI) PeerCount() hexutil.Uint {
	return 1
}

// Version returns the current ethereum protocol version.
func (s *NetAPI) Version() string {
	return fmt.Sprintf("%d", EVMMainnetChainID.Int64())
}

// Web3API offers helper utils
type Web3API struct{}

// ClientVersion returns the node name
func (s *Web3API) ClientVersion() string {
	return fmt.Sprintf("tinyAN@beta")
}

// Sha3 applies the ethereum sha3 implementation on the input.
// It assumes the input is hex encoded.
func (s *Web3API) Sha3(input hexutil.Bytes) hexutil.Bytes {
	return crypto.Keccak256(input)
}

type TxPool struct{}

type content struct {
	Pending any
	Queued  any
}

func emptyPool() content {
	return content{
		Pending: struct{}{},
		Queued:  struct{}{},
	}
}

func (s *TxPool) Content() content {
	return emptyPool()
}

func (s *TxPool) ContentFrom(addr common.Address) content {
	return emptyPool()
}

func (s *TxPool) Status() map[string]hexutil.Uint {
	return map[string]hexutil.Uint{
		"pending": hexutil.Uint(0),
		"queued":  hexutil.Uint(0),
	}
}

func (s *TxPool) Inspect() content {
	return emptyPool()
}
