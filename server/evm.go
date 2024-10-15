package server

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/bluesign/tinyAN/storage"
	"github.com/onflow/atree"
	"github.com/onflow/cadence"
	"github.com/onflow/cadence/encoding/ccf"
	"github.com/onflow/flow-evm-gateway/api"
	"github.com/onflow/flow-evm-gateway/models"
	errs "github.com/onflow/flow-evm-gateway/models/errors"
	emulator2 "github.com/onflow/flow-go/fvm/evm/emulator"
	"github.com/onflow/flow-go/fvm/evm/emulator/state"
	evmTypes "github.com/onflow/flow-go/fvm/evm/types"
	"github.com/onflow/flow-go/fvm/storage/snapshot"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/go-ethereum/common"
	"github.com/onflow/go-ethereum/common/hexutil"
	"github.com/onflow/go-ethereum/common/math"
	"github.com/onflow/go-ethereum/core/types"
	types2 "github.com/onflow/go-ethereum/core/types"
	"github.com/onflow/go-ethereum/eth/filters"
	"github.com/onflow/go-ethereum/rlp"
	"github.com/onflow/go-ethereum/rpc"
	"math/big"
	"strings"
)

var (
	EVMMainnetChainID        = big.NewInt(747)
	BlockStoreLatestBlockKey = "LatestBlock"
)

const maxFeeHistoryBlockCount = 1024
const blockGasLimit uint64 = 120_000_000

type APINamespace struct {
	storage *storage.HeightBasedStorage
}

func handleError[T any](err error) (T, error) {
	var (
		zero        T
		revertedErr *errs.RevertError
	)

	switch {
	// as per specification returning nil and nil for not found resources
	case errors.Is(err, errs.ErrEntityNotFound):
		return zero, nil
	case errors.Is(err, errs.ErrInvalid):
		return zero, err
	case errors.Is(err, errs.ErrFailedTransaction):
		return zero, err
	case errors.As(err, &revertedErr):
		return zero, revertedErr
	default:
		return zero, errs.ErrInternal
	}
}

// encodeTxFromArgs will create a transaction from the given arguments.
// The resulting unsigned transaction is only supposed to be used through
// `EVM.dryRun` inside Cadence scripts, meaning that no state change
// will occur.
// This is only useful for `eth_estimateGas` and `eth_call` endpoints.
func encodeTxFromArgs(args api.TransactionArgs) ([]byte, error) {
	var data []byte
	if args.Data != nil {
		data = *args.Data
	} else if args.Input != nil {
		data = *args.Input
	}

	// provide a high enough gas for the tx to be able to execute,
	// capped by the gas set in transaction args.
	gasLimit := blockGasLimit
	if args.Gas != nil {
		gasLimit = uint64(*args.Gas)
	}

	value := big.NewInt(0)
	if args.Value != nil {
		value = args.Value.ToInt()
	}

	tx := types.NewTx(
		&types.LegacyTx{
			Nonce:    0,
			To:       args.To,
			Value:    value,
			Gas:      gasLimit,
			GasPrice: big.NewInt(0),
			Data:     data,
		},
	)

	enc, err := tx.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errs.ErrInvalid, err)
	}

	return enc, nil
}

func txFromArgs(args api.TransactionArgs) (*types2.Transaction, error) {
	var data []byte
	if args.Data != nil {
		data = *args.Data
	} else if args.Input != nil {
		data = *args.Input
	}

	// provide a high enough gas for the tx to be able to execute,
	// capped by the gas set in transaction args.
	gasLimit := blockGasLimit
	if args.Gas != nil {
		gasLimit = uint64(*args.Gas)
	}

	value := big.NewInt(0)
	if args.Value != nil {
		value = args.Value.ToInt()
	}

	return types.NewTx(
		&types.LegacyTx{
			Nonce:    0,
			To:       args.To,
			Value:    value,
			Gas:      gasLimit,
			GasPrice: big.NewInt(0),
			Data:     data,
		},
	), nil

}

// BlockNumber returns the block number of the chain head.
func (a *APINamespace) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	latestBlockHeight := a.storage.Latest().EVM().LastProcessedHeight()
	return hexutil.Uint64(latestBlockHeight), nil
}

func (a *APINamespace) blockNumberToHeight(blockNumber rpc.BlockNumber) (uint64, error) {
	if blockNumber < 0 {
		return a.storage.Latest().EVM().LastProcessedHeight(), nil
	}
	return uint64(blockNumber), nil
}

func (a *APINamespace) blockNumberOrHashToHeight(blockNumberOrHash rpc.BlockNumberOrHash) (uint64, error) {
	blockNumber, ok := blockNumberOrHash.Number()
	fmt.Println("blockNumber", blockNumber)
	if ok {
		return a.blockNumberToHeight(blockNumber)
	}
	blockHash, ok := blockNumberOrHash.Hash()
	if !ok {
		return 0, fmt.Errorf("%w %w", errs.ErrInvalid, "neither block number nor hash specified")
	}

	for _, spork := range a.storage.Sporks() {
		height, err := spork.EVM().GetEVMHeightFromHash(blockHash)
		if err == nil {
			return height, nil
		}
	}
	return 0, errs.ErrMissingBlock

}

// GetBlockByNumber returns the requested canonical block.
//   - When blockNr is -1 the chain pending block is returned.
//   - When blockNr is -2 the chain latest block is returned.
//   - When blockNr is -3 the chain finalized block is returned.
//   - When blockNr is -4 the chain safe block is returned.
//   - When fullTx is true all transactions in the block are returned, otherwise
//     only the transaction hash is returned.
func (a *APINamespace) GetBlockByNumber(ctx context.Context, blockNumber rpc.BlockNumber, full bool) (*api.Block, error) {

	height, err := a.blockNumberToHeight(blockNumber)
	if err != nil {
		return handleError[*api.Block](errs.ErrEntityNotFound)
	}

	block, err := a.blockFromBlockStorage(height)
	if err != nil {
		return handleError[*api.Block](errs.ErrEntityNotFound)
	}

	h, err := block.Hash()

	blockResponse := &api.Block{
		Hash:             h,
		Number:           hexutil.Uint64(block.Height),
		ParentHash:       block.ParentBlockHash,
		ReceiptsRoot:     block.ReceiptRoot,
		TransactionsRoot: block.TransactionHashRoot,
		Transactions:     []common.Hash{},
		Uncles:           []common.Hash{},
		GasLimit:         hexutil.Uint64(blockGasLimit),
		Nonce:            types.BlockNonce{0x1},
		Timestamp:        hexutil.Uint64(block.Timestamp),
		BaseFeePerGas:    hexutil.Big(*big.NewInt(0)),
		LogsBloom:        types.LogsBloom([]*types.Log{}),
		Miner:            evmTypes.CoinbaseAddress.ToCommon(),
		Sha3Uncles:       types.EmptyUncleHash,
	}

	cadenceHeight, err := a.storage.StorageForEVMHeight(block.Height).EVM().GetCadenceHeightFromEVMHeight(block.Height)
	if err != nil {
		return handleError[*api.Block](errs.ErrInternal)
	}

	cadenceBlockId, err := a.storage.StorageForHeight(cadenceHeight).Protocol().GetBlockIdByHeight(cadenceHeight)
	if err != nil {
		return handleError[*api.Block](errs.ErrInternal)
	}

	cadenceEvents := a.storage.StorageForHeight(cadenceHeight).Protocol().EventsByName(cadenceBlockId, "A.e467b9dd11fa00df.EVM.TransactionExecuted")

	blockBytes, err := block.ToBytes()
	if err != nil {
		return handleError[*api.Block](errs.ErrInternal)
	}
	blockSize := rlp.ListSize(uint64(len(blockBytes)))
	transactionResults := make([]*api.Transaction, len(cadenceEvents))
	transactionHashes := make([]common.Hash, len(cadenceEvents))

	if cadenceEvents != nil && len(cadenceEvents) > 0 {
		totalGasUsed := hexutil.Uint64(0)
		logs := make([]*types.Log, 0)
		for _, eventRaw := range cadenceEvents {
			eventDecoded, err := ccf.Decode(nil, eventRaw.Payload)
			if err != nil {
				return handleError[*api.Block](errs.ErrInternal)
			}
			event, ok := eventDecoded.(cadence.Event)
			if !ok {
				return handleError[*api.Block](errs.ErrInternal)
			}
			tx, receipt, err := storage.DecodeTransactionEvent(event)
			receipt.BlockHash = h
			if err != nil {
				return handleError[*api.Block](errs.ErrInternal)
			}
			transactionHashes[receipt.TransactionIndex] = receipt.TxHash
			txResult, _ := api.NewTransactionResult(tx, *receipt, EVMMainnetChainID)
			transactionResults[receipt.TransactionIndex] = txResult
			totalGasUsed += hexutil.Uint64(receipt.GasUsed)
			logs = append(logs, receipt.Logs...)
			blockSize += tx.Size()
		}
		blockResponse.GasUsed = totalGasUsed
		blockResponse.LogsBloom = types.LogsBloom(logs)
	}
	blockResponse.Size = hexutil.Uint64(rlp.ListSize(blockSize))

	if full {
		blockResponse.Transactions = transactionResults
	} else {
		blockResponse.Transactions = transactionHashes
	}

	return blockResponse, nil
}

func (a *APINamespace) Syncing(ctx context.Context) (interface{}, error) {
	return false, nil
}

// SendRawTransaction will add the signed transaction to the transaction pool.
// The sender is responsible for signing the transaction and using the correct nonce.
func (a *APINamespace) SendRawTransaction(
	ctx context.Context,
	input hexutil.Bytes,
) (common.Hash, error) {
	//TODO: implement transaction simulation
	return common.Hash{}, errs.ErrIndexOnlyMode
}

type ViewOnlyLedger struct {
	snapshot snapshot.StorageSnapshot
}

func (v ViewOnlyLedger) GetValue(owner, key []byte) (value []byte, err error) {

	reg := flow.RegisterID{
		Owner: string(storage.DeepCopy(owner)),
		Key:   string(storage.DeepCopy(key)),
	}
	vv, err := v.snapshot.Get(reg)

	return vv, err
}

func (v ViewOnlyLedger) SetValue(owner, key, value []byte) (err error) {
	fmt.Println("!!!!!!!!! SetValue called")
	return nil
}

func (v ViewOnlyLedger) ValueExists(owner, key []byte) (exists bool, err error) {
	_, err = v.snapshot.Get(flow.NewRegisterID(flow.BytesToAddress(owner), string(key)))
	if err != nil {
		return false, err
	}
	return true, nil
}

func (v ViewOnlyLedger) AllocateSlabIndex(owner []byte) (atree.SlabIndex, error) {
	fmt.Println("!!!!!!!!! AllocateSlabIndex called")
	return atree.SlabIndex{}, nil

}

var _ atree.Ledger = (*ViewOnlyLedger)(nil)

func (a *APINamespace) baseViewForEVMHeight(height uint64) (*state.BaseView, error) {
	store := a.storage.StorageForEVMHeight(height)
	cadenceHeight, err := store.EVM().GetCadenceHeightFromEVMHeight(height)
	if err != nil {
		return nil, err
	}
	snap := a.storage.LedgerSnapshot(cadenceHeight)
	base, _ := flow.StringToAddress("d421a63faae318f9")
	return state.NewBaseView(&ViewOnlyLedger{
		snapshot: snap,
	}, base)

}

func (a *APINamespace) blockFromBlockStorage(height uint64) (*evmTypes.Block, error) {
	store := a.storage.StorageForEVMHeight(height)
	cadenceHeight, err := store.EVM().GetCadenceHeightFromEVMHeight(height)
	if err != nil {
		return nil, err
	}
	base, _ := flow.StringToAddress("d421a63faae318f9")
	view := &ViewOnlyLedger{
		snapshot: a.storage.LedgerSnapshot(cadenceHeight),
	}
	data, err := view.GetValue(base[:], []byte(BlockStoreLatestBlockKey))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return evmTypes.GenesisBlock(flow.Mainnet), nil
	}
	return evmTypes.NewBlockFromBytes(data)

}

// GetBalance returns the amount of wei for the given address in the state of the
// given block number. The rpc.LatestBlockNumber and rpc.PendingBlockNumber meta
// block numbers are also allowed.
func (a *APINamespace) GetBalance(
	ctx context.Context,
	address common.Address,
	blockNumberOrHash rpc.BlockNumberOrHash,
) (*hexutil.Big, error) {

	height, err := a.blockNumberOrHashToHeight(blockNumberOrHash)
	if err != nil {
		return handleError[*hexutil.Big](err)
	}
	bv, err := a.baseViewForEVMHeight(height)
	if err != nil {
		return handleError[*hexutil.Big](errs.ErrInternal)
	}

	bal, err := bv.GetBalance(address)
	if err != nil {
		return nil, err
	}
	return (*hexutil.Big)(bal.ToBig()), nil
}

// GetTransactionByHash returns the transaction for the given hash
func (a *APINamespace) GetTransactionByHash(
	ctx context.Context,
	hash common.Hash,
) (*api.Transaction, error) {

	targetHeight := uint64(0)
	for _, spork := range a.storage.Sporks() {
		height, err := spork.EVM().GetEVMBlockHeightForTransaction(hash)
		if err == nil {
			targetHeight = height
			break
		}
	}

	if targetHeight == 0 {
		return handleError[*api.Transaction](errs.ErrEntityNotFound)
	}

	evmBlock, err := a.storage.StorageForEVMHeight(targetHeight).EVM().GetEvmBlockByHeight(targetHeight)
	if err != nil {
		return handleError[*api.Transaction](errs.ErrEntityNotFound)
	}

	block := evmBlock.Transactions
	for i, txBytes := range block {
		tx, err := models.UnmarshalTransaction(txBytes)
		if err != nil {
			return handleError[*api.Transaction](errs.ErrInternal)
		}
		if tx.Hash() == hash {
			//TODO: chain config
			return api.NewTransactionResult(tx, *evmBlock.Receipts[i], EVMMainnetChainID)
		}
	}
	return handleError[*api.Transaction](errs.ErrEntityNotFound)
}

// GetTransactionByBlockHashAndIndex returns the transaction for the given block hash and index.
func (a *APINamespace) GetTransactionByBlockHashAndIndex(
	ctx context.Context,
	blockHash common.Hash,
	index hexutil.Uint,
) (*api.Transaction, error) {

	var height uint64 = 0
	var err error
	for _, spork := range a.storage.Sporks() {
		height, err = spork.EVM().GetEVMHeightFromHash(blockHash)
		if err == nil {
			break
		}
	}
	if err != nil {
		return handleError[*api.Transaction](errs.ErrEntityNotFound)
	}

	evmBlock, err := a.storage.StorageForEVMHeight(height).EVM().GetEvmBlockByHeight(height)
	if err != nil {
		return handleError[*api.Transaction](errs.ErrEntityNotFound)
	}

	txIndex := int(index)
	if txIndex >= len(evmBlock.Block.TransactionHashes) {
		return handleError[*api.Transaction](errs.ErrEntityNotFound)
	}
	txHash := evmBlock.Block.TransactionHashes[index]
	return a.GetTransactionByHash(ctx, txHash)
}

// GetTransactionByBlockNumberAndIndex returns the transaction
// for the given block number and index.
func (a *APINamespace) GetTransactionByBlockNumberAndIndex(
	ctx context.Context,
	blockNumber rpc.BlockNumber,
	index hexutil.Uint,
) (*api.Transaction, error) {

	height, err := a.blockNumberToHeight(blockNumber)
	if err != nil {
		return handleError[*api.Transaction](errs.ErrEntityNotFound)
	}

	evmBlock, err := a.storage.StorageForEVMHeight(height).EVM().GetEvmBlockByHeight(height)
	if err != nil {
		return handleError[*api.Transaction](errs.ErrEntityNotFound)
	}

	txIndex := int(index)
	if txIndex >= len(evmBlock.Block.TransactionHashes) {
		return handleError[*api.Transaction](errs.ErrEntityNotFound)
	}
	txHash := evmBlock.Block.TransactionHashes[index]
	return a.GetTransactionByHash(ctx, txHash)
}

// GetTransactionReceipt returns the transaction receipt for the given transaction hash.
func (a *APINamespace) GetTransactionReceipt(
	ctx context.Context,
	hash common.Hash,
) (map[string]interface{}, error) {

	var height uint64 = 0
	var err error
	for _, spork := range a.storage.Sporks() {
		height, err = spork.EVM().GetEVMBlockHeightForTransaction(hash)
		if err == nil {
			break
		}
	}

	if err != nil {
		return handleError[map[string]interface{}](errs.ErrEntityNotFound)
	}

	evmBlock, err := a.storage.StorageForHeight(height).EVM().GetEvmBlockByHeight(height)
	if err != nil {
		return handleError[map[string]interface{}](errs.ErrEntityNotFound)
	}

	transactions := evmBlock.Transactions
	for i, txBytes := range transactions {
		tx, err := models.UnmarshalTransaction(txBytes)
		if err != nil {
			return handleError[map[string]interface{}](errs.ErrInternal)
		}
		if tx.Hash() == hash {
			txReceipt, err := api.MarshalReceipt(evmBlock.Receipts[i], tx)
			if err != nil {
				return handleError[map[string]interface{}](errs.ErrInternal)
			}
			return txReceipt, nil
		}
	}
	return handleError[map[string]interface{}](errs.ErrInternal)

}

// GetBlockByHash returns the requested block. When fullTx is true all transactions in the block are returned in full
// detail, otherwise only the transaction hash is returned.
func (a *APINamespace) GetBlockByHash(
	ctx context.Context,
	hash common.Hash,
	fullTx bool,
) (*api.Block, error) {

	var height uint64 = 0
	var err error
	for _, spork := range a.storage.Sporks() {
		height, err = spork.EVM().GetEVMHeightFromHash(hash)
		if err == nil {
			break
		}
	}

	if err != nil {
		return handleError[*api.Block](errs.ErrEntityNotFound)
	}
	return a.GetBlockByNumber(ctx, rpc.BlockNumber(height), fullTx)
}

// GetBlockReceipts returns the block receipts for the given block hash or number or tag.
func (a *APINamespace) GetBlockReceipts(
	ctx context.Context,
	blockNumberOrHash rpc.BlockNumberOrHash,
) ([]map[string]interface{}, error) {

	height, err := a.blockNumberOrHashToHeight(blockNumberOrHash)
	if err != nil {
		return handleError[[]map[string]interface{}](errs.ErrEntityNotFound)
	}
	evmBlock, err := a.storage.StorageForEVMHeight(height).EVM().GetEvmBlockByHeight(height)
	if err != nil {
		return handleError[[]map[string]interface{}](errs.ErrEntityNotFound)
	}

	receipts := make([]map[string]interface{}, len(evmBlock.Block.TransactionHashes))
	transactions := evmBlock.Transactions
	for i, txBytes := range transactions {
		tx, err := models.UnmarshalTransaction(txBytes)
		if err != nil {
			return handleError[[]map[string]interface{}](errs.ErrInternal)
		}
		txReceipt, err := api.MarshalReceipt(evmBlock.Receipts[i], tx)
		if err != nil {
			return handleError[[]map[string]interface{}](errs.ErrInternal)
		}
		receipts = append(receipts, txReceipt)

	}

	return receipts, nil
}

// GetBlockTransactionCountByHash returns the number of transactions
// in the block with the given hash.
func (a *APINamespace) GetBlockTransactionCountByHash(
	ctx context.Context,
	blockHash common.Hash,
) (*hexutil.Uint, error) {

	height, err := a.blockNumberOrHashToHeight(rpc.BlockNumberOrHash{
		BlockHash: &blockHash,
	})

	if err != nil {
		return handleError[*hexutil.Uint](errs.ErrEntityNotFound)
	}

	evmBlock, err := a.storage.StorageForEVMHeight(height).EVM().GetEvmBlockByHeight(height)
	if err != nil {
		return handleError[*hexutil.Uint](errs.ErrEntityNotFound)
	}

	count := hexutil.Uint(len(evmBlock.Block.TransactionHashes))
	return &count, nil
}

// GetBlockTransactionCountByNumber returns the number of transactions
// in the block with the given block number.
func (a *APINamespace) GetBlockTransactionCountByNumber(
	ctx context.Context,
	blockNumber rpc.BlockNumber,
) (*hexutil.Uint, error) {

	height, err := a.blockNumberToHeight(blockNumber)
	if err != nil {
		return handleError[*hexutil.Uint](errs.ErrEntityNotFound)
	}

	evmBlock, err := a.storage.StorageForEVMHeight(height).EVM().GetEvmBlockByHeight(height)
	if err != nil {
		return handleError[*hexutil.Uint](errs.ErrEntityNotFound)
	}

	count := hexutil.Uint(len(evmBlock.Block.TransactionHashes))
	return &count, nil
}

// Call executes the given transaction on the state for the given block number.
// Additionally, the caller can specify a batch of contract for fields overriding.
// Note, this function doesn't make and changes in the state/blockchain and is
// useful to execute and retrieve values.
func (a *APINamespace) Call(
	ctx context.Context,
	args api.TransactionArgs,
	blockNumberOrHash *rpc.BlockNumberOrHash,
	overrides *api.StateOverride,
	blockOverrides *api.BlockOverrides,
) (hexutil.Bytes, error) {

	err := args.Validate()
	if err != nil {
		return handleError[hexutil.Bytes](err)
	}
	if blockNumberOrHash == nil {
		latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
		blockNumberOrHash = &latest
	}
	height, err := a.blockNumberOrHashToHeight(*blockNumberOrHash)
	if err != nil {
		return handleError[hexutil.Bytes](errs.ErrEntityNotFound)
	}

	store := a.storage.StorageForEVMHeight(height)
	cadenceHeight, err := store.EVM().GetCadenceHeightFromEVMHeight(height)
	if err != nil {
		return nil, err
	}
	snap := a.storage.LedgerSnapshot(cadenceHeight)
	base, _ := flow.StringToAddress("d421a63faae318f9")
	emulator := emulator2.NewEmulator(&ViewOnlyLedger{
		snapshot: snap,
	}, base)

	rbv, err := emulator.NewBlockView(evmTypes.NewDefaultBlockContext(height))

	tx, err := txFromArgs(args)
	if err != nil {
		return handleError[hexutil.Bytes](err)
	}

	// Default address in case user does not provide one
	from, _ := a.Coinbase(ctx)
	if args.From != nil {
		from = *args.From
	}

	result, err := rbv.DryRunTransaction(tx, from)

	if err != nil {
		return handleError[hexutil.Bytes](err)
	}

	return result.ReturnedData, nil
}

// GetLogs returns logs matching the given argument that are stored within the state.
func (a *APINamespace) GetLogs(
	ctx context.Context,
	criteria filters.FilterCriteria,
) ([]*types.Log, error) {

	filter := FilterCriteria{
		Addresses: criteria.Addresses,
		Topics:    criteria.Topics,
	}

	// if filter provided specific block ID
	if criteria.BlockHash != nil {

		height, err := a.blockNumberOrHashToHeight(rpc.BlockNumberOrHash{
			BlockHash: criteria.BlockHash,
		})
		if err != nil {
			return handleError[[]*types.Log](errs.ErrEntityNotFound)
		}

		f, err := NewIDFilter(*criteria.BlockHash, filter, a.storage.StorageForEVMHeight(height).EVM())
		if err != nil {
			return handleError[[]*types.Log](err)
		}

		res, err := f.Match()
		if err != nil {
			return handleError[[]*types.Log](err)
		}

		return res, nil
	}

	// otherwise we use the block range as the filter

	// assign default values to latest block number, unless provided
	from := models.LatestBlockNumber
	if criteria.FromBlock != nil {
		from = criteria.FromBlock
	}
	to := models.LatestBlockNumber
	if criteria.ToBlock != nil {
		to = criteria.ToBlock
	}

	h := a.storage.Latest().EVM().LastProcessedHeight()
	if h == 0 {
		return handleError[[]*types.Log](fmt.Errorf("failed to get latest block height"))
	}
	latest := big.NewInt(int64(h))

	// if special value, use latest block number
	if from.Cmp(models.EarliestBlockNumber) < 0 {
		from = latest
	}
	if to.Cmp(models.EarliestBlockNumber) < 0 {
		to = latest
	}

	f, err := NewRangeFilter(from.Uint64(), to.Uint64(), filter, a.storage.StorageForEVMHeight(from.Uint64()).EVM())
	if err != nil {
		return handleError[[]*types.Log](err)
	}

	res, err := f.Match()
	if err != nil {
		return handleError[[]*types.Log](err)
	}

	// makes sure the response is correctly serialized
	if res == nil {
		return []*types.Log{}, nil
	}

	return res, nil
}

// GetTransactionCount returns the number of transactions the given address
// has sent for the given block number.
func (a *APINamespace) GetTransactionCount(
	ctx context.Context,
	address common.Address,
	blockNumberOrHash rpc.BlockNumberOrHash,
) (*hexutil.Uint64, error) {

	height, err := a.blockNumberOrHashToHeight(blockNumberOrHash)
	if err != nil {
		return handleError[*hexutil.Uint64](errs.ErrEntityNotFound)
	}
	bv, err := a.baseViewForEVMHeight(height)
	if err != nil {
		return handleError[*hexutil.Uint64](errs.ErrInternal)
	}
	nonce, err := bv.GetNonce(address)
	if err != nil {
		return nil, err
	}

	return (*hexutil.Uint64)(&nonce), nil
}

// EstimateGas returns the lowest possible gas limit that allows the transaction to run
// successfully at block `blockNrOrHash`, or the latest block if `blockNrOrHash` is unspecified. It
// returns error if the transaction would revert or if there are unexpected failures. The returned
// value is capped by both `args.Gas` (if non-nil & non-zero) and the backend's RPCGasCap
// configuration (if non-zero).
func (a *APINamespace) EstimateGas(
	ctx context.Context,
	args api.TransactionArgs,
	blockNumberOrHash *rpc.BlockNumberOrHash,
	overrides *api.StateOverride,
) (hexutil.Uint64, error) {
	err := args.Validate()
	if err != nil {
		return handleError[hexutil.Uint64](err)
	}

	_, err = encodeTxFromArgs(args)
	if err != nil {
		return hexutil.Uint64(blockGasLimit), nil // return block gas limit
	}
	if blockNumberOrHash == nil {
		latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
		blockNumberOrHash = &latest
	}

	height, err := a.blockNumberOrHashToHeight(*blockNumberOrHash)
	if err != nil {
		return handleError[hexutil.Uint64](errs.ErrEntityNotFound)
	}

	store := a.storage.StorageForEVMHeight(height)
	cadenceHeight, err := store.EVM().GetCadenceHeightFromEVMHeight(height)
	if err != nil {
		return handleError[hexutil.Uint64](errs.ErrInternal)
	}
	snap := a.storage.LedgerSnapshot(cadenceHeight)
	base, _ := flow.StringToAddress("d421a63faae318f9")
	emulator := emulator2.NewEmulator(&ViewOnlyLedger{
		snapshot: snap,
	}, base)

	rbv, err := emulator.NewBlockView(evmTypes.NewDefaultBlockContext(height))

	tx, err := txFromArgs(args)
	if err != nil {
		return handleError[hexutil.Uint64](err)
	}

	// Default address in case user does not provide one
	from, _ := a.Coinbase(ctx)
	if args.From != nil {
		from = *args.From
	}

	result, err := rbv.DryRunTransaction(tx, from)

	if err != nil {
		return handleError[hexutil.Uint64](err)
	}

	return hexutil.Uint64(result.GasConsumed), nil

}

// GetCode returns the code stored at the given address in
// the state for the given block number.
func (a *APINamespace) GetCode(
	ctx context.Context,
	address common.Address,
	blockNumberOrHash rpc.BlockNumberOrHash,
) (hexutil.Bytes, error) {

	height, err := a.blockNumberOrHashToHeight(blockNumberOrHash)
	if err != nil {
		return handleError[hexutil.Bytes](errs.ErrEntityNotFound)
	}
	bv, err := a.baseViewForEVMHeight(height)
	if err != nil {
		return handleError[hexutil.Bytes](errs.ErrInternal)
	}
	code, err := bv.GetCode(address)
	if err != nil {
		return nil, err
	}

	return code, nil
}

// FeeHistory returns transaction base fee per gas and effective priority fee
// per gas for the requested/supported block range.
// blockCount: Requested range of blocks. Clients will return less than the
// requested range if not all blocks are available.
// lastBlock: Highest block of the requested range.
// rewardPercentiles: A monotonically increasing list of percentile values.
// For each block in the requested range, the transactions will be sorted in
// ascending order by effective tip per gas and the coresponding effective tip
// for the percentile will be determined, accounting for gas consumed.
func (a *APINamespace) FeeHistory(
	ctx context.Context,
	blockCount math.HexOrDecimal64,
	lastBlock rpc.BlockNumber,
	rewardPercentiles []float64,
) (*api.FeeHistoryResult, error) {

	if blockCount > maxFeeHistoryBlockCount {
		return handleError[*api.FeeHistoryResult](
			fmt.Errorf("block count has to be between 1 and %d, got: %d", maxFeeHistoryBlockCount, blockCount),
		)
	}

	lastBlockNumber := uint64(lastBlock)
	if lastBlock < 0 {
		// From the special block tags, we only support "latest".
		lastBlockNumber = a.storage.Latest().EVM().LastProcessedHeight()
		if lastBlockNumber == 0 {
			return handleError[*api.FeeHistoryResult](fmt.Errorf("invalid height: %d", lastBlockNumber))
		}
	}

	var (
		oldestBlock   *hexutil.Big
		baseFees      []*hexutil.Big
		rewards       [][]*hexutil.Big
		gasUsedRatios []float64
	)

	maxCount := uint64(blockCount)
	if maxCount > lastBlockNumber {
		maxCount = lastBlockNumber
	}

	blockRewards := make([]*hexutil.Big, len(rewardPercentiles))
	for i := range rewardPercentiles {
		blockRewards[i] = (*hexutil.Big)(big.NewInt(0))
	}

	for i := maxCount; i >= uint64(1); i-- {
		// If the requested block count is 5, and the last block number
		// is 20, then we need the blocks [16, 17, 18, 19, 20] in this
		// specific order. The first block we fetch is 20 - 5 + 1 = 16.
		blockHeight := lastBlockNumber - i + 1
		evmBlock, err := a.storage.StorageForEVMHeight(blockHeight).EVM().GetEvmBlockByHeight(blockHeight)
		if err != nil {
			continue
		}

		if i == maxCount {
			oldestBlock = (*hexutil.Big)(big.NewInt(int64(evmBlock.Block.Height)))
		}

		baseFees = append(baseFees, (*hexutil.Big)(big.NewInt(0)))

		rewards = append(rewards, blockRewards)

		gasUsedRatio := float64(evmBlock.Block.TotalGasUsed) / float64(blockGasLimit)
		gasUsedRatios = append(gasUsedRatios, gasUsedRatio)
	}

	return &api.FeeHistoryResult{
		OldestBlock:  oldestBlock,
		Reward:       rewards,
		BaseFee:      baseFees,
		GasUsedRatio: gasUsedRatios,
	}, nil
}

// GetStorageAt returns the storage from the state at the given address, key and
// block number. The rpc.LatestBlockNumber and rpc.PendingBlockNumber meta block
// numbers are also allowed.
func (a *APINamespace) GetStorageAt(
	ctx context.Context,
	address common.Address,
	storageSlot string,
	blockNumberOrHash rpc.BlockNumberOrHash,
) (hexutil.Bytes, error) {

	key, _, err := decodeHash(storageSlot)
	if err != nil {
		return handleError[hexutil.Bytes](
			fmt.Errorf("%w: %w", errs.ErrInvalid, err),
		)
	}

	height, err := a.blockNumberOrHashToHeight(blockNumberOrHash)
	if err != nil {
		return handleError[hexutil.Bytes](errs.ErrEntityNotFound)
	}
	bv, err := a.baseViewForEVMHeight(height)
	if err != nil {
		return handleError[hexutil.Bytes](errs.ErrInternal)
	}
	data, err := bv.GetState(evmTypes.SlotAddress{
		Address: address,
		Key:     key,
	})
	if err != nil {
		return nil, err
	}

	return data[:], nil

}

// decodeHash parses a hex-encoded 32-byte hash. The input may optionally
// be prefixed by 0x and can have a byte length up to 32.
func decodeHash(s string) (h common.Hash, inputLength int, err error) {
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		s = s[2:]
	}
	if (len(s) & 1) > 0 {
		s = "0" + s
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return common.Hash{}, 0, fmt.Errorf("invalid hex string: %s", s)
	}
	if len(b) > common.HashLength {
		return common.Hash{}, len(b), fmt.Errorf(
			"hex string too long, want at most 32 bytes, have %d bytes",
			len(b),
		)
	}
	return common.BytesToHash(b), len(b), nil
}

/*
Static responses section

The API endpoints bellow return a static response because the values are not relevant for Flow EVM implementation
or because it doesn't make sense yet to implement more complex solution
*/

// ChainId is the EIP-155 replay-protection chain id for the current Ethereum chain config.
//
// Note, this method does not conform to EIP-695 because the configured chain ID is always
// returned, regardless of the current head block. We used to return an error when the chain
// wasn't synced up to a block where EIP-155 is enabled, but this behavior caused issues
// in CL clients.
func (a *APINamespace) ChainId(ctx context.Context) (*hexutil.Big, error) {
	return (*hexutil.Big)(EVMMainnetChainID), nil
}

// Coinbase is the address that mining rewards will be sent to (alias for Etherbase).
func (a *APINamespace) Coinbase(ctx context.Context) (common.Address, error) {
	return evmTypes.CoinbaseAddress.ToCommon(), nil
}

// GasPrice returns a suggestion for a gas price for legacy transactions.
func (a *APINamespace) GasPrice(ctx context.Context) (*hexutil.Big, error) {
	return (*hexutil.Big)(big.NewInt(0)), nil
}

// GetUncleCountByBlockHash returns number of uncles in the block for the given block hash
func (a *APINamespace) GetUncleCountByBlockHash(
	ctx context.Context,
	blockHash common.Hash,
) *hexutil.Uint {
	count := hexutil.Uint(0)
	return &count
}

// GetUncleCountByBlockNumber returns number of uncles in the block for the given block number
func (a *APINamespace) GetUncleCountByBlockNumber(
	ctx context.Context,
	blockNumber rpc.BlockNumber,
) *hexutil.Uint {
	count := hexutil.Uint(0)
	return &count
}

// GetUncleByBlockHashAndIndex returns the uncle block for the given block hash and index.
func (a *APINamespace) GetUncleByBlockHashAndIndex(
	ctx context.Context,
	blockHash common.Hash,
	index hexutil.Uint,
) (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

// GetUncleByBlockNumberAndIndex returns the uncle block for the given block hash and index.
func (a *APINamespace) GetUncleByBlockNumberAndIndex(
	ctx context.Context,
	blockNumber rpc.BlockNumber,
	index hexutil.Uint,
) (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

// MaxPriorityFeePerGas returns a suggestion for a gas tip cap for dynamic fee transactions.
func (a *APINamespace) MaxPriorityFeePerGas(ctx context.Context) (*hexutil.Big, error) {
	fee := hexutil.Big(*big.NewInt(1))
	return &fee, nil
}

// Mining returns true if client is actively mining new blocks.
// This can only return true for proof-of-work networks and may
// not be available in some clients since The Merge.
func (a *APINamespace) Mining() bool {
	return false
}

// Hashrate returns the number of hashes per second that the
// node is mining with.
// This can only return true for proof-of-work networks and
// may not be available in some clients since The Merge.
func (a *APINamespace) Hashrate() hexutil.Uint64 {
	return hexutil.Uint64(0)
}

/*
Not supported section

The API endpoints bellow return a non-supported error indicating the API requested is not supported (yet).
This is because a decision to not support this API was made either because we don't intend to support it
ever or we don't support it at this phase.
*/

// GetProof returns the Merkle-proof for a given account and optionally some storage keys.
func (a *APINamespace) GetProof(
	ctx context.Context,
	address common.Address,
	storageKeys []string,
	blockNumberOrHash rpc.BlockNumberOrHash,
) (*api.AccountResult, error) {
	return nil, errs.NewEndpointNotSupportedError("eth_getProof")
}

// CreateAccessList creates an EIP-2930 type AccessList for the given transaction.
// Reexec and blockNumberOrHash can be specified to create the accessList on top of a certain state.
func (a *APINamespace) CreateAccessList(
	ctx context.Context,
	args api.TransactionArgs,
	blockNumberOrHash *rpc.BlockNumberOrHash,
) (*api.AccessListResult, error) {
	return nil, errs.NewEndpointNotSupportedError("eth_createAccessList")
}
