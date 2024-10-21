package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/goccy/go-json"
	"github.com/onflow/cadence/runtime/common"
	"github.com/onflow/flow-evm-gateway/models"
	errs "github.com/onflow/flow-evm-gateway/models/errors"
	emulator2 "github.com/onflow/flow-go/fvm/evm/emulator"
	"github.com/onflow/flow-go/fvm/evm/precompiles"
	evmTypes "github.com/onflow/flow-go/fvm/evm/types"
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
	"strings"
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

func NewDebugApi(api *APINamespace) *DebugAPI {
	return &DebugAPI{
		logger: zerolog.New(os.Stdout).With().Timestamp().Logger(),
		api:    api,
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
	ctx context.Context,
	txId gethCommon.Hash,
	_ *tracers.TraceConfig,
) (json.RawMessage, error) {

	cadenceHeight, err := d.api.storage.CadenceBlockHeightForTransactionHash(txId)

	transactions, err := d.api.evmTransactionsAtCadenceHeight(cadenceHeight)
	if err != nil {
		return handleError[json.RawMessage](errs.ErrEntityNotFound)
	}

	tx, found := transactions[txId]
	if !found {
		return handleError[json.RawMessage](errs.ErrEntityNotFound)
	}

	blockHeight := tx.Receipt.BlockNumber

	block, err := d.api.blockFromBlockStorage(blockHeight.Uint64())
	if err != nil {
		return handleError[json.RawMessage](errs.ErrInternal)
	}
	traced, err := d.traceBlock(ctx, block.Height, nil)
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

	cadenceHeight, err := d.api.storage.StorageForEVMHeight(height).EVM().CadenceHeightFromEVMHeight(height)
	if err != nil {
		return nil, err
	}
	block, err := d.api.blockFromBlockStorageByCadenceHeight(cadenceHeight)
	if err != nil {
		return nil, err
	}
	fmt.Println("traceBlock", height)
	fmt.Println("cadenceHeight", cadenceHeight)
	fmt.Println("block", block.Height)

	blockProposalFromCadenceHeight, err := d.api.blockProposalFromCadenceHeight(cadenceHeight - 1)
	fmt.Println(blockProposalFromCadenceHeight.TxHashes)
	fmt.Println(len(blockProposalFromCadenceHeight.TxHashes))
	fmt.Println(blockProposalFromCadenceHeight.TxHashes.RootHash())
	//0xd30a36549af3c98d7cee8d01707c0ed7eb066a8e2d90567e3413e6ba0d951a11 0x92320adeb3be429c41012cd207eafa82e5f98a1bb8d2be5c5557c72492ea9585 0x0915f8f221d4d19b0616b48cf52da21e9fadde7cb50f48d2eee10d70dff4c1f4 0x20b45a6f154255770df4dfb0db0bfe17cad8c66025d904d89bd0ab41d0c95261 0x6adb6f4b9e941a07a5156f6bf8d417a23d8d71c699ab853cde087cf609ac24a3 0xa981ab38c54a46bca1988d46c51a68e068d3840774f6e3b05d1ea7c66acd804d 0x8abd5aad834fec02e65e517778233616eee491f7adfc9e1ac7ac99df57369c45 0x5bf9836a4a7c2fe4dbfc902feaefb9baf33859ccfcf3bc2e01a37b7861cc8799 0x2c1c0c3e75577084466ffe40cf2e9da6ef24cd8faa88040f46329aadc4be413a 0xdb77eed375b52520e36180fdc98d9a61f66ce1273842f26221199a9fe8be04e0 0xc4e17f5218053814701a64e15ab1e87514942d20f925bffe6738facd90dbb5f1 0xc12de69dd0d6812b351c1ba5dafd1525055577161b5d818fd09852c5b8a868b5 0x921a54d21bba68a63a6e9ba4207abd09ac1b3443ce415511204414c52273948c 0x027babebf82ec6fc7fc714b5ff382a1164d6d84063ab1dca9a8f0b1a017354b7 0x51cc41aceb2cd0de4323d4d317c96af32a0d146c4ceed2bee8ce6ee4a600fa9e 0xca9ec2b50e16a4854e885907e41b5e8fe82db4b57de00dab801cbb569e9a4ab0 0x6980e146196a812d8950a957caa50acad258be570f24dfbe45995f74980c6ec3 0xfde1dc3d8f3302f28ff0cea35f4ede35bf4392ef54143f71f7d34d66e50a483e 0x5134a661cef2b74f3b761e3709798384e83f3bf2978c7371de838f18821b05c2 0x4e690499fa9b97eb55fe0d6b2f8fff28614270d61b3b5c77f468cd221eaeee55 0x74b29de1010c4d459c6f546eb7e6268293849053bd8bb1f648a4a6c784b803c5 0x41eab00ac4dcefe4076c8e59042143b99474deb98c86cab475c762365948880e 0x74aa5df917cc972af60bc1e1f1da50cc1185105b13d9e0b1a8bfd2812fabb242 0x5b9a84f2a4f44a264f3f9f47e6746a281d7801b191f3a9797f169d6f645607ae 0x5281c116a70f70e07e5695f691e80e692cb80fbf512442e3f1f047dc53fd3b1c 0x16cd5724026fe94548555dcc9b215188ac5732870bf6bce4c3e69e7514204674 0x9236fe93e42f0626a350aae8c48c0de0264265e675a2ae945c52c0cfcf01da7d 0x555ec1161fd5dbecca77e86398c06c6f1ab84e9903bb729fbaaecf649dc39517 0xc4084041e0f28d2b8c8ab475bffa03164f7b022b824141407ee50afd3554bc4a 0x8353dee4f42e3aa9293eaee7a0d8ba0a4c9b9c3696b9dc36f11b55f0e45265d5 0x199621420a2358e7f5ffc9611a66de9e2ad7cf11261c0b3474497499ace4dd4d 0xa81584b8e8c876db746fabad7d00705fc43d164eb3821e43d9301a8ff5d8802e 0xf0c62488ccf7abbe9c6fb7c20a2f16ad1793073ff04b578bfbeb716e908bf89d 0x8a6d0d78c7144de1b587288ecdf86ddb815f9b5500a6553ee4dda025d0f2fb57 0x0db3455e13a703cf16af3b941e47d17fd38d5b7e994a26c7485f1f00c22b2b1c 0x34497034213d43b0c175e4be43f000ec28a1c92ff1e2927a5b1a1e3e0ada9204 0x5a9f6a67cb5be8ddcbdf7a0521e190fb1cf23f519a74633155ad9fda7ee0fd5c 0xac9002ffdf4b862e8663d38ef042500bb78f4e052e7633f43abce415fcd00a2d 0xd6e21fc28273450dfb2439182c40339202f24b0d10f879fdc2809aa06ed87c65 0xe83834fa5d68f748ca6ec23b889a4fe038975f7e85828658c26ea85cde24b987 0x8e510412a9d40697505e1f196b7614e044c0c0ca7e57bd172cf05fbcea1232f8 0xccf2e780dd4b74eea28056cbdcbd003e69deed96c354021a48520a7b9403f0fd 0x102391c65f2ac7964a1951b272fdc0a2990b590a70b0b3c15bdc6b295966282f 0xe36e520bf293e533ea747051ee0eb4d4fcad238fdee13b08f45b3e1afe6ebddd 0x932cd72605ebb1f883c31e44bcc51d385f69322de10ce2898fbcaddffae33802 0x4de7116e80d67bd58cd1b75e5d9787a299fa593fd23c3a31f8fb2880d354ef15 0xd8e1c330a1eba19840b1cdc9a42fbcb980f5c0f543390b3d9a95fa808ed3c39d 0xf9f6437310a8774d4bac53598931fc663624d7bc1c524ffcc3e2b66c302d643a 0x7ced98ff92931db8f8f8daa690607b5450fe33b5b4ddd30be6d28f83c7c5bb5d

	base, _ := flow.StringToAddress("d421a63faae318f9")
	snap := d.api.storage.LedgerSnapshot(cadenceHeight - 1)
	snapAfter := d.api.storage.LedgerSnapshot(cadenceHeight)

	roView := NewViewOnlyLedger(snap)
	emulator := emulator2.NewEmulator(roView, base)

	transactions, err := d.api.blockTransactions(height)
	if err != nil {
		fmt.Println(err)

		return nil, err
	}

	tracer, err := NewEVMCallTracer(zerolog.New(os.Stdout).With().Timestamp().Logger())
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	results := make([]*txTraceResult, len(transactions))

	totalGasUsed := uint64(0)
	for i, tx := range transactions {

		var gethTx *gethTypes.Transaction
		var res *evmTypes.Result

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
		}
		if tx.PrecompiledCalls != nil {
			pcs, err := evmTypes.AggregatedPrecompileCallsFromEncoded(tx.PrecompiledCalls)
			if err != nil {
				return nil, fmt.Errorf("error decoding precompiled calls [%x]: %w", tx.PrecompiledCalls, err)
			}
			blockContext.ExtraPrecompiledContracts = precompiles.AggregatedPrecompiledCallsToPrecompiledContracts(pcs)
		}
		rbv, err := emulator.NewBlockView(blockContext)
		if err != nil {
			return nil, err
		}
		switch v := tx.Transaction.(type) {

		case models.DirectCall:
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
			fmt.Println(err)
			return nil, err
		}

		if res == nil { // safety check for result
			return nil, evmTypes.ErrUnexpectedEmptyResult
		}

		txTrace, ok := tracer.ResultsByTxID[tx.Receipt.TxHash]

		if !ok {
			results[i] = &txTraceResult{TxHash: tx.Receipt.TxHash, Result: map[string]string{}}
		} else {
			results[i] = &txTraceResult{TxHash: tx.Receipt.TxHash, Result: txTrace}
		}

		if res == nil {
			fmt.Println("res is nil")
		}

		if res.StateChangeCommitment == nil {
			fmt.Println("res.StateChangeCommitment is nil")
			fmt.Println(res.TxHash)
			fmt.Println(res.Logs)
			fmt.Println(res.Receipt().BlockNumber)
			fmt.Println(txTrace)
			fmt.Println(res.VMError)
		}
		var emptyChecksum = [4]byte{0, 0, 0, 0}
		//check checksum
		if !bytes.Equal(tx.Checksum[:], emptyChecksum[:]) && !bytes.Equal(res.StateChangeCommitment[:4], tx.Checksum[:]) {
			fmt.Println("checksum failed")
		}
	}

	//check changes
	failed := false
	for k, v := range roView.GetPendingWrites() {

		if !strings.Contains(k.String(), "/$") {
			continue
		}
		nextValue, _ := snapAfter.Get(k)

		if bytes.Compare(v, nextValue) != 0 {
			fmt.Println("key", k)
			fmt.Println("value", hex.EncodeToString(v))
			fmt.Println("nextValue", hex.EncodeToString(nextValue))

			failed = true
			fmt.Println("^^^^^ differ")

		}

		if failed {
			fmt.Println("traceBlockInner", height)
			fmt.Println("cadenceHeight", cadenceHeight)
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
