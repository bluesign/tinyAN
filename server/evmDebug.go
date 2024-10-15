package server

import (
	"context"
	"fmt"
	"github.com/onflow/cadence/runtime/common"
	"github.com/onflow/flow-evm-gateway/models"
	errs "github.com/onflow/flow-evm-gateway/models/errors"
	"github.com/onflow/flow-go/fvm/evm/debug"
	emulator2 "github.com/onflow/flow-go/fvm/evm/emulator"
	evmTypes "github.com/onflow/flow-go/fvm/evm/types"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/go-ethereum/common/hexutil"
	gethTypes "github.com/onflow/go-ethereum/core/types"
	"github.com/onflow/go-ethereum/crypto"
	"math/big"
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
	api    *APINamespace
}

func NewDebugAPI(api *APINamespace) *DebugAPI {
	return &DebugAPI{
		logger: zerolog.New(os.Stdout).With().Timestamp().Logger(),
		api:    api,
	}
}

type EVMTraceListener struct {
	Data map[string]json.RawMessage
}

// Upload implements debug.Uploader.
func (e *EVMTraceListener) Upload(id string, data json.RawMessage) error {
	e.Data[id] = data
	return nil
}

var _ debug.Uploader = &EVMTraceListener{}

func (d *DebugAPI) TraceTransaction(
	_ context.Context,
	txId gethCommon.Hash,
	_ *tracers.TraceConfig,
) (json.RawMessage, error) {

	evmListener := &EVMTraceListener{
		Data: make(map[string]json.RawMessage),
	}

	tracer, _ := debug.NewEVMCallTracer(evmListener, zerolog.New(os.Stdout).With().Timestamp().Logger())

	cadenceHeight := uint64(0)
	evmHeight := uint64(0)

	for _, spork := range d.api.storage.Sporks() {
		height, err := spork.EVM().GetCadenceBlockHeightForTransaction(txId)
		if err == nil {
			cadenceHeight = height
			break
		}
	}
	if cadenceHeight == 0 {
		return handleError[json.RawMessage](errs.ErrEntityNotFound)
	}
	fmt.Println("cadenceHeight", cadenceHeight)
	block, err := d.api.blockFromBlockStorageByCadenceHeight(cadenceHeight)
	if err != nil {
		return handleError[json.RawMessage](errs.ErrInternal)
	}

	transactions, _, err := d.api.blockTransactions(block.Height)
	if err != nil {
		return handleError[json.RawMessage](errs.ErrInternal)
	}

	snap := d.api.storage.LedgerSnapshot(cadenceHeight)
	base, _ := flow.StringToAddress("d421a63faae318f9")
	emulator := emulator2.NewEmulator(&ViewOnlyLedger{
		snapshot: snap,
	}, base)
	fmt.Println("emulator", emulator)
	ctx := evmTypes.BlockContext{
		ChainID:                evmTypes.FlowEVMMainNetChainID,
		BlockNumber:            evmHeight,
		DirectCallBaseGasUsage: evmTypes.DefaultDirectCallBaseGasUsage,
		DirectCallGasPrice:     evmTypes.DefaultDirectCallGasPrice,
		GetHashFunc: func(n uint64) gethCommon.Hash { // default returns some random hash values
			return gethCommon.BytesToHash(crypto.Keccak256([]byte(new(big.Int).SetUint64(n).String())))
		},
		Tracer: tracer.TxTracer(),
		//Tracer: debug.NewEVMCallTracer(nil, nil),
	}
	fmt.Println("ctx", ctx)

	rbv, err := emulator.NewBlockView(ctx)
	fmt.Println("rbv", rbv)

	for _, tx := range transactions {

		if tx.Hash() == txId {
			fmt.Println("tx", tx)

			var gethTx *gethTypes.Transaction
			switch v := tx.(type) {

			case models.DirectCall:
				gethTx = v.Transaction()
			case models.TransactionCall:
				gethTx = v.Transaction
			default:
				fmt.Println(fmt.Sprintf("%T", v))
				panic("invalid transaction type")
			}
			fmt.Println("gethTx", gethTx)

			// step 5 - run transaction
			res, err := rbv.RunTransaction(gethTx)
			fmt.Println(res)
			fmt.Println(err)
			if err != nil {
				return nil, err
			}
			if res == nil { // safety check for result
				return nil, evmTypes.ErrUnexpectedEmptyResult
			}

			// step 11 - collect traces
			//tracer.Collect(res.TxHash)
		}
	}

	return tracer.GetResultByTxHash(txId), nil
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
		height = d.api.storage.Latest().EVM().LastProcessedHeight()
	}

	return d.traceBlock(ctx, height, cfg)
}

func (d *DebugAPI) TraceBlockByHash(
	ctx context.Context,
	hash gethCommon.Hash,
	cfg *tracers.TraceConfig,
) ([]*txTraceResult, error) {

	var height uint64 = 0
	var err error
	for _, spork := range d.api.storage.Sporks() {
		height, err = spork.EVM().GetEVMHeightFromHash(hash)
		if err == nil {
			break
		}
	}

	if err != nil {
		return handleError[[]*txTraceResult](errs.ErrEntityNotFound)
	}

	return d.traceBlock(ctx, height, cfg)
}

func (d *DebugAPI) traceBlock(
	ctx context.Context,
	height uint64,
	_ *tracers.TraceConfig,
) ([]*txTraceResult, error) {

	transactions, receipts, err := d.api.blockTransactions(height)
	if err != nil {
		return nil, err
	}

	results := make([]*txTraceResult, len(transactions))
	for i, tx := range transactions {

		txTrace, err := d.TraceTransaction(ctx, tx.Hash(), nil)

		if err != nil {
			results[i] = &txTraceResult{TxHash: receipts[i].TxHash, Error: err.Error()}
		} else {
			results[i] = &txTraceResult{TxHash: receipts[i].TxHash, Result: txTrace}
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
