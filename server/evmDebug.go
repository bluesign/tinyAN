package server

import (
	"context"
	"fmt"
	"github.com/goccy/go-json"
	"github.com/onflow/cadence/runtime"
	"github.com/onflow/cadence/runtime/common"
	"github.com/onflow/flow-evm-gateway/models"
	errs "github.com/onflow/flow-evm-gateway/models/errors"
	"github.com/onflow/flow-go/fvm"
	"github.com/onflow/flow-go/fvm/environment"
	"github.com/onflow/flow-go/fvm/evm"
	emulator2 "github.com/onflow/flow-go/fvm/evm/emulator"
	"github.com/onflow/flow-go/fvm/evm/handler"
	evmTypes "github.com/onflow/flow-go/fvm/evm/types"
	reusableRuntime "github.com/onflow/flow-go/fvm/runtime"
	fvmStorage "github.com/onflow/flow-go/fvm/storage"
	fvmState "github.com/onflow/flow-go/fvm/storage/state"
	"github.com/onflow/flow-go/fvm/systemcontracts"
	"github.com/onflow/flow-go/fvm/tracing"
	"github.com/onflow/flow-go/model/flow"
	gethCommon "github.com/onflow/go-ethereum/common"
	"github.com/onflow/go-ethereum/common/hexutil"
	gethTypes "github.com/onflow/go-ethereum/core/types"
	"github.com/onflow/go-ethereum/crypto"
	"github.com/onflow/go-ethereum/eth/tracers"
	"github.com/onflow/go-ethereum/rpc"
	"github.com/rs/zerolog"
	"math/big"
	"os"
)

type Pool struct {
	c chan func()
}

func NewPool() *Pool {
	p := &Pool{make(chan func(), 10000)}

	for i := 0; i < 30; i++ {
		go p.worker()
	}

	return p
}

func (p *Pool) worker() {
	for work := range p.c {
		work()
	}
}

func (p *Pool) Do(work func()) {
	p.c <- work
}

// txTraceResult is the result of a single transaction trace.
type txTraceResult struct {
	TxHash gethCommon.Hash `json:"txHash"`           // transaction hash
	Result interface{}     `json:"result,omitempty"` // Trace results produced by the tracer
	Error  string          `json:"error,omitempty"`  // Trace failure produced by the tracer
}

type DebugAPI struct {
	logger zerolog.Logger
	api    *APINamespace
	pool   *Pool
}

func NewDebugApi(api *APINamespace) *DebugAPI {
	return &DebugAPI{
		logger: zerolog.New(os.Stdout).With().Timestamp().Logger(),
		api:    api,
		pool:   NewPool(),
	}
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
		height, err = spork.EVM().EVMHeightForBlockHash(hash)
		if err == nil {
			break
		}
	}

	if err != nil {
		return handleError[[]*txTraceResult](errs.ErrEntityNotFound)
	}

	return d.traceBlock(ctx, height, cfg)
}

func (d *DebugAPI) TraceTransaction(
	_ context.Context,
	txId gethCommon.Hash,
	_ *tracers.TraceConfig,
) (json.RawMessage, error) {

	fmt.Println("TraceTransaction", txId)
	cadenceHeight := uint64(0)
	for _, spork := range d.api.storage.Sporks() {
		height, err := spork.EVM().CadenceBlockHeightForTransactionHash(txId)
		if err == nil {
			cadenceHeight = height
			break
		}
	}
	if cadenceHeight == 0 {
		return handleError[json.RawMessage](errs.ErrEntityNotFound)
	}
	block, err := d.api.blockFromBlockStorageByCadenceHeight(cadenceHeight)
	if err != nil {
		return handleError[json.RawMessage](errs.ErrInternal)
	}
	traced, err := d.traceBlock(context.Background(), block.Height, nil)
	if err != nil {
		return handleError[json.RawMessage](errs.ErrInternal)
	}

	for _, txResult := range traced {
		if txResult.TxHash == txId {
			jsonRaw, _ := json.Marshal(txResult.Result)
			return jsonRaw, nil
		}
	}

	return handleError[json.RawMessage](errs.ErrEntityNotFound)

}

type TraceResponse struct {
	Trace []*txTraceResult
	Error error
}

func (d *DebugAPI) traceBlock(
	_ context.Context,
	height uint64,
	_ *tracers.TraceConfig) ([]*txTraceResult, error) {

	respC := make(chan TraceResponse)

	d.pool.Do(func() {
		// Compute your response
		computedResponse, err := d.traceBlockInner(height)
		respC <- TraceResponse{Trace: computedResponse, Error: err}
	})

	resp := <-respC

	if resp.Error != nil {
		return nil, resp.Error
	}

	return resp.Trace, nil
}

func (d *DebugAPI) traceBlockInner(
	height uint64,
) ([]*txTraceResult, error) {

	fmt.Println("traceBlockInner", height)
	cadenceHeight, err := d.api.storage.StorageForEVMHeight(height).EVM().CadenceHeightFromEVMHeight(height)
	fmt.Println("cadenceHeight", cadenceHeight)
	if err != nil {
		return nil, err
	}
	block, err := d.api.blockFromBlockStorageByCadenceHeight(cadenceHeight)
	if err != nil {
		return nil, err
	}

	blockHeader, err := d.api.storage.GetBlockByHeight(height)
	if err != nil {
		return nil, err
	}

	base, _ := flow.StringToAddress("d421a63faae318f9")
	snap := d.api.storage.LedgerSnapshot(cadenceHeight - 1)
	emulator := emulator2.NewEmulator(NewViewOnlyLedger(snap), base)

	transactions, receipts, err := d.api.blockTransactions(height)
	if err != nil {
		return nil, err
	}

	tracer, _ := NewEVMCallTracer(zerolog.New(os.Stdout).With().Timestamp().Logger())

	results := make([]*txTraceResult, len(transactions))

	fvmContext := fvm.NewContext(
		fvm.WithBlockHeader(blockHeader),
		fvm.WithBlocks(d.api.storage),
		fvm.WithCadenceLogging(true),
		fvm.WithAuthorizationChecksEnabled(false),
		fvm.WithSequenceNumberCheckAndIncrementEnabled(false),
		fvm.WithEVMEnabled(true),
		fvm.WithReusableCadenceRuntimePool(
			reusableRuntime.NewReusableCadenceRuntimePool(
				0,
				runtime.Config{
					TracingEnabled:     false,
					AttachmentsEnabled: true,
				},
			),
		),
		//fvm.WithEntropyProvider(emulator),
	)

	blockDatabase := fvmStorage.NewBlockDatabase(snap, 0, nil)
	txnState, err := blockDatabase.NewTransaction(0, fvmState.DefaultParameters())
	if err != nil {
		panic(err)
	}
	env := environment.NewTransactionEnvironment(
		tracing.NewMockTracerSpan(),
		fvmContext.EnvironmentParams,
		txnState)

	totalGasUsed := uint64(0)
	for i, tx := range transactions {

		var gethTx *gethTypes.Transaction
		var res *evmTypes.Result

		sc := systemcontracts.SystemContractsForChain(flow.Mainnet)
		randomBeaconAddress := sc.RandomBeaconHistory.Address
		addressAllocator := handler.NewAddressAllocator()

		blockContext := evmTypes.BlockContext{
			ChainID:                evmTypes.FlowEVMMainNetChainID,
			BlockNumber:            block.Height,
			Random:                 block.PrevRandao,
			BlockTimestamp:         block.Timestamp,
			TotalGasUsedSoFar:      totalGasUsed,
			TxCountSoFar:           uint(i),
			DirectCallBaseGasUsage: evmTypes.DefaultDirectCallBaseGasUsage,
			DirectCallGasPrice:     evmTypes.DefaultDirectCallGasPrice,
			GasFeeCollector:        evmTypes.CoinbaseAddress,
			GetHashFunc: func(n uint64) gethCommon.Hash { // default returns some random hash values
				return gethCommon.BytesToHash(crypto.Keccak256([]byte(new(big.Int).SetUint64(n).String())))
			},
			Tracer: tracer.TxTracer(),
			ExtraPrecompiledContracts: PreparePrecompiledContracts(
				evm.ContractAccountAddress(flow.Mainnet),
				randomBeaconAddress,
				addressAllocator,
				env,
			),
		}
		rbv, err := emulator.NewBlockView(blockContext)

		switch v := tx.(type) {

		case models.DirectCall:
			fmt.Println("DirectCall")
			fmt.Println(v.DirectCall.Type)
			res, err = rbv.DirectCall(v.DirectCall)

		case models.TransactionCall:
			gethTx = v.Transaction
			res, err = rbv.RunTransaction(gethTx)

		default:
			fmt.Println(fmt.Sprintf("%T", v))
			panic("invalid transaction type")
		}

		totalGasUsed += res.GasConsumed
		if err != nil {
			return nil, err
		}
		if res == nil { // safety check for result
			return nil, evmTypes.ErrUnexpectedEmptyResult
		}

		txTrace := tracer.GetResultByTxHash(receipts[i].TxHash)

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

type Content struct {
	Pending any `json:"pending"`
	Queued  any `json:"queued"`
}

func emptyPool() Content {
	return Content{
		Pending: struct{}{},
		Queued:  struct{}{},
	}
}

func (s *TxPool) Content() Content {
	return emptyPool()
}

func (s *TxPool) ContentFrom(_ common.Address) Content {
	return emptyPool()
}

func (s *TxPool) Status() map[string]hexutil.Uint {
	return map[string]hexutil.Uint{
		"pending": hexutil.Uint(0),
		"queued":  hexutil.Uint(0),
	}
}

func (s *TxPool) Inspect() Content {
	return emptyPool()
}
