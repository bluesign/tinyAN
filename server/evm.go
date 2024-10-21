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
	"github.com/onflow/flow-go/fvm/environment"
	emulator2 "github.com/onflow/flow-go/fvm/evm/emulator"
	"github.com/onflow/flow-go/fvm/evm/emulator/state"
	evmTypes "github.com/onflow/flow-go/fvm/evm/types"
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
	"sort"
	"strings"
)

var (
	EVMMainnetChainID                = big.NewInt(747)
	BlockStoreLatestBlockKey         = "LatestBlock"
	BlockStoreLatestBlockProposalKey = "LatestBlockProposal"
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
func (a *APINamespace) BlockNumber(_ context.Context) (hexutil.Uint64, error) {
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
	if ok {
		return a.blockNumberToHeight(blockNumber)
	}
	blockHash, ok := blockNumberOrHash.Hash()
	if !ok {
		return 0, fmt.Errorf("%w: %w", errs.ErrInvalid, errors.New("neither block number nor hash specified"))
	}

	for _, spork := range a.storage.Sporks() {
		height, err := spork.EVM().EVMHeightForBlockHash(blockHash)
		if err == nil {
			return height, nil
		}
	}
	return 0, errs.ErrMissingBlock

}

type TransactionWithReceipt struct {
	Transaction      models.Transaction
	Receipt          models.Receipt
	PrecompiledCalls []byte
	Checksum         [4]byte
}

func (a *APINamespace) evmTransactionsAtCadenceHeight(cadenceHeight uint64) (map[common.Hash]TransactionWithReceipt, error) {
	cadenceBlockId, err := a.storage.GetBlockIdByHeight(cadenceHeight)
	if err != nil {
		return nil, err
	}
	transactions := make(map[common.Hash]TransactionWithReceipt)

	cadenceEvents := a.storage.StorageForHeight(cadenceHeight).Protocol().EventsByName(cadenceBlockId, "A.e467b9dd11fa00df.EVM.TransactionExecuted")

	sort.Slice(cadenceEvents, func(i, j int) bool {
		if cadenceEvents[i].TransactionIndex != cadenceEvents[j].TransactionIndex {
			return cadenceEvents[i].TransactionIndex < cadenceEvents[j].TransactionIndex
		}
		return cadenceEvents[i].EventIndex < cadenceEvents[j].EventIndex
	})

	if cadenceEvents == nil || len(cadenceEvents) == 0 {
		return nil, fmt.Errorf("not found")
	}

	transactionIndex := 0
	for _, eventRaw := range cadenceEvents {
		eventDecoded, err := ccf.Decode(nil, eventRaw.Payload)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		event, ok := eventDecoded.(cadence.Event)

		if !ok {
			fmt.Println(err)
			return nil, errors.New("failed to decode event")
		}
		tx, receipt, payload, err := storage.DecodeTransactionEvent(transactionIndex, event)
		if err != nil {
			return nil, err
		}
		transactions[tx.Hash()] = TransactionWithReceipt{
			Transaction:      tx,
			Receipt:          *receipt,
			PrecompiledCalls: payload.PrecompiledCalls,
			Checksum:         payload.StateUpdateChecksum,
		}

		transactionIndex = transactionIndex + 1
	}
	return nil, fmt.Errorf("not found")

}

func (a *APINamespace) blockTransactions(blockHeight uint64) ([]TransactionWithReceipt, error) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
	}()

	prevCadenceHeight, err := a.storage.CadenceHeightFromEVMHeight(blockHeight - 1)
	if blockHeight != 0 && err != nil {
		return nil, err
	}

	cadenceHeight, err := a.storage.CadenceHeightFromEVMHeight(blockHeight)
	if err != nil {
		return nil, err
	}

	block, err := a.blockFromBlockStorageByCadenceHeight(cadenceHeight)
	if err != nil {
		return nil, err
	}

	transactions := make([]TransactionWithReceipt, 0)

	startCadenceHeight := prevCadenceHeight + 1
	if blockHeight == 0 {
		startCadenceHeight = cadenceHeight
	}
	endCadenceHeight := cadenceHeight

	if startCadenceHeight != endCadenceHeight {
		fmt.Println("has gap")
		fmt.Println("prevCadenceHeight", prevCadenceHeight)
		fmt.Println("cadenceHeight", cadenceHeight)
		fmt.Println("startCadenceHeight", startCadenceHeight)
		fmt.Println("endCadenceHeight", endCadenceHeight)
		fmt.Println("blockHeight", blockHeight)
	}

	current := startCadenceHeight
	transactionIndex := 0
	for current <= endCadenceHeight {

		cadenceBlockId, err := a.storage.GetBlockIdByHeight(current)
		if err != nil {
			return nil, err
		}

		cadenceEvents := a.storage.StorageForHeight(current).Protocol().EventsByName(cadenceBlockId, "A.e467b9dd11fa00df.EVM.TransactionExecuted")

		sort.Slice(cadenceEvents, func(i, j int) bool {
			if cadenceEvents[i].TransactionIndex != cadenceEvents[j].TransactionIndex {
				return cadenceEvents[i].TransactionIndex < cadenceEvents[j].TransactionIndex
			}
			return cadenceEvents[i].EventIndex < cadenceEvents[j].EventIndex
		})

		logIndex := uint(0)
		cumulativeGasUsed := uint64(0)

		if cadenceEvents != nil && len(cadenceEvents) > 0 {
			for _, eventRaw := range cadenceEvents {
				eventDecoded, err := ccf.Decode(nil, eventRaw.Payload)
				if err != nil {
					fmt.Println(err)
					return nil, err
				}
				event, ok := eventDecoded.(cadence.Event)

				if !ok {
					fmt.Println(err)
					return nil, errors.New("failed to decode event")
				}
				tx, receipt, payload, err := storage.DecodeTransactionEvent(transactionIndex, event)

				if err != nil {
					fmt.Println(err)
					return nil, err
				}
				transactionIndex = transactionIndex + 1

				cumulativeGasUsed += receipt.GasUsed
				receipt.CumulativeGasUsed = cumulativeGasUsed

				receipt.BlockHash, _ = block.Hash()

				for _, log := range receipt.Logs {
					log.Index = logIndex
					log.BlockNumber = block.Height
					log.TxHash = receipt.TxHash
					log.TxIndex = receipt.TransactionIndex
					log.BlockHash = receipt.BlockHash
					logIndex++
				}

				transactions = append(transactions, TransactionWithReceipt{
					Transaction:      tx,
					Receipt:          *receipt,
					PrecompiledCalls: payload.PrecompiledCalls,
					Checksum:         payload.StateUpdateChecksum,
				})

			}
		}
		current = current + 1

	}

	return transactions, nil
}

// GetBlockByNumber returns the requested canonical block.
//   - When blockNr is -1 the chain pending block is returned.
//   - When blockNr is -2 the chain latest block is returned.
//   - When blockNr is -3 the chain finalized block is returned.
//   - When blockNr is -4 the chain safe block is returned.
//   - When fullTx is true all transactions in the block are returned, otherwise
//     only the transaction hash is returned.
func (a *APINamespace) GetBlockByNumber(_ context.Context, blockNumber rpc.BlockNumber, full bool) (*api.Block, error) {

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

	blockBytes, err := block.ToBytes()
	if err != nil {
		return handleError[*api.Block](errs.ErrInternal)
	}
	blockSize := rlp.ListSize(uint64(len(blockBytes)))

	transactions, err := a.blockTransactions(height)
	if err != nil {
		return handleError[*api.Block](errs.ErrInternal)
	}

	transactionResults := make([]*api.Transaction, len(transactions))
	transactionHashes := make([]common.Hash, len(transactions))

	if transactions != nil && len(transactions) > 0 {
		totalGasUsed := hexutil.Uint64(0)
		logs := make([]*types.Log, 0)
		for _, tx := range transactions {
			receipt := tx.Receipt
			transactionHashes[receipt.TransactionIndex] = receipt.TxHash
			txResult, _ := api.NewTransactionResult(tx.Transaction, tx.Receipt, EVMMainnetChainID)
			transactionResults[receipt.TransactionIndex] = txResult
			totalGasUsed += hexutil.Uint64(receipt.GasUsed)
			logs = append(logs, receipt.Logs...)
			blockSize += tx.Transaction.Size()
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

func (a *APINamespace) Syncing(_ context.Context) (interface{}, error) {
	return false, nil
}

// SendRawTransaction will add the signed transaction to the transaction pool.
// The sender is responsible for signing the transaction and using the correct nonce.
func (a *APINamespace) SendRawTransaction(
	_ context.Context,
	input hexutil.Bytes,
) (common.Hash, error) {
	//TODO: implement transaction simulation
	return common.Hash{}, errs.ErrIndexOnlyMode
}

type ViewOnlyLedger struct {
	snapshot storage.FVMStorageSnapshot
	writes   map[flow.RegisterID]flow.RegisterValue
}

func NewViewOnlyLedger(snapshot storage.FVMStorageSnapshot) *ViewOnlyLedger {
	return &ViewOnlyLedger{
		snapshot: snapshot,
		writes:   make(map[flow.RegisterID]flow.RegisterValue),
	}
}

func (v *ViewOnlyLedger) GetValue(owner, key []byte) ([]byte, error) {
	reg := flow.RegisterID{
		Owner: string(owner),
		Key:   string(key),
	}
	if value, ok := v.writes[reg]; ok {
		return value, nil
	}
	return v.snapshot.Get(reg)
}

func (v *ViewOnlyLedger) SetValue(owner, key, value []byte) (err error) {
	reg := flow.RegisterID{
		Owner: string(owner),
		Key:   string(key),
	}

	v.writes[reg] = value
	return nil
}

func (v *ViewOnlyLedger) ValueExists(owner, key []byte) (exists bool, err error) {
	value, err := v.GetValue(owner, key)
	if err != nil {
		return false, err
	}
	return len(value) > 0, nil
}

func (v *ViewOnlyLedger) GetPendingWrites() map[flow.RegisterID]flow.RegisterValue {
	return v.writes
}

func (v *ViewOnlyLedger) AllocateSlabIndex(owner []byte) (atree.SlabIndex, error) {
	statusBytes, err := v.GetValue(owner, []byte(flow.AccountStatusKey))
	if err != nil {
		return atree.SlabIndex{}, err
	}
	if len(statusBytes) == 0 {
		return atree.SlabIndex{}, fmt.Errorf("state for account not found")
	}

	status, err := environment.AccountStatusFromBytes(statusBytes)
	if err != nil {
		return atree.SlabIndex{}, err
	}

	// get and increment the index
	index := status.SlabIndex()
	newIndexBytes := index.Next()

	// update the storageIndex bytes
	status.SetStorageIndex(newIndexBytes)
	err = v.SetValue(owner, []byte(flow.AccountStatusKey), status.ToBytes())
	if err != nil {
		return atree.SlabIndex{}, err
	}
	return index, nil
}

var _ atree.Ledger = (*ViewOnlyLedger)(nil)

func (a *APINamespace) baseViewForEVMHeight(height uint64) (*state.BaseView, error) {
	cadenceHeight, err := a.storage.CadenceHeightFromEVMHeight(height)
	if err != nil {
		return nil, err
	}
	snap := a.storage.LedgerSnapshot(cadenceHeight)
	base, _ := flow.StringToAddress("d421a63faae318f9")
	return state.NewBaseView(NewViewOnlyLedger(snap), base)
}

func (a *APINamespace) blockProposalFromCadenceHeight(cadenceHeight uint64) (*evmTypes.BlockProposal, error) {
	base, _ := flow.StringToAddress("d421a63faae318f9")
	store := a.storage.StorageForHeight(cadenceHeight)
	snap := store.Ledger().StorageSnapshot(cadenceHeight)

	data, err := snap.Get(flow.NewRegisterID(base, BlockStoreLatestBlockProposalKey))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("not found")
	}
	return evmTypes.NewBlockProposalFromBytes(data)

}

func (a *APINamespace) blockFromBlockStorageByCadenceHeight(cadenceHeight uint64) (*evmTypes.Block, error) {
	base, _ := flow.StringToAddress("d421a63faae318f9")
	store := a.storage.StorageForHeight(cadenceHeight)
	snap := store.Ledger().StorageSnapshot(cadenceHeight)

	data, err := snap.Get(flow.NewRegisterID(base, BlockStoreLatestBlockKey))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	if len(data) == 0 {
		return evmTypes.GenesisBlock(flow.Mainnet), nil
	}
	return evmTypes.NewBlockFromBytes(data)
}

func (a *APINamespace) blockFromBlockStorage(height uint64) (*evmTypes.Block, error) {
	cadenceHeight, err := a.storage.CadenceHeightFromEVMHeight(height)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return a.blockFromBlockStorageByCadenceHeight(cadenceHeight)
}

// GetBalance returns the amount of wei for the given address in the state of the
// given block number. The rpc.LatestBlockNumber and rpc.PendingBlockNumber meta
// block numbers are also allowed.
func (a *APINamespace) GetBalance(
	_ context.Context,
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
	_ context.Context,
	hash common.Hash,
) (*api.Transaction, error) {
	cadenceHeight, err := a.storage.CadenceBlockHeightForTransactionHash(hash)
	if err != nil {
		return handleError[*api.Transaction](errs.ErrEntityNotFound)
	}
	block, err := a.blockFromBlockStorageByCadenceHeight(cadenceHeight)
	if err != nil {
		fmt.Println(err)
		return handleError[*api.Transaction](errs.ErrInternal)
	}
	transactions, err := a.blockTransactions(block.Height)
	if err != nil {
		return handleError[*api.Transaction](errs.ErrInternal)
	}
	for _, tx := range transactions {
		if tx.Transaction.Hash() == hash {
			receipt := tx.Receipt
			receipt.BlockHash, _ = block.Hash()
			return api.NewTransactionResult(tx.Transaction, tx.Receipt, EVMMainnetChainID)
		}
	}
	return handleError[*api.Transaction](errs.ErrEntityNotFound)
}

// GetTransactionByBlockHashAndIndex returns the transaction for the given block hash and index.
func (a *APINamespace) GetTransactionByBlockHashAndIndex(
	_ context.Context,
	blockHash common.Hash,
	index hexutil.Uint,
) (*api.Transaction, error) {

	cadenceHeight, err := a.storage.CadenceHeightForBlockHash(blockHash)
	if err != nil {
		return handleError[*api.Transaction](errs.ErrEntityNotFound)
	}
	block, err := a.blockFromBlockStorageByCadenceHeight(cadenceHeight)
	if err != nil {
		fmt.Println(err)
		return handleError[*api.Transaction](errs.ErrInternal)
	}
	transactions, err := a.blockTransactions(block.Height)
	if err != nil {
		return handleError[*api.Transaction](errs.ErrInternal)
	}

	txIndex := int(index)
	if txIndex >= len(transactions) {
		return handleError[*api.Transaction](errs.ErrEntityNotFound)
	}
	receipt := transactions[txIndex].Receipt
	receipt.BlockHash, _ = block.Hash()
	return api.NewTransactionResult(transactions[txIndex].Transaction, receipt, EVMMainnetChainID)
}

// GetTransactionByBlockNumberAndIndex returns the transaction
// for the given block number and index.
func (a *APINamespace) GetTransactionByBlockNumberAndIndex(
	_ context.Context,
	blockNumber rpc.BlockNumber,
	index hexutil.Uint,
) (*api.Transaction, error) {

	height, err := a.blockNumberToHeight(blockNumber)
	if err != nil {
		return handleError[*api.Transaction](errs.ErrEntityNotFound)
	}

	cadenceHeight, err := a.storage.CadenceHeightFromEVMHeight(height)
	if err != nil {
		return handleError[*api.Transaction](errs.ErrEntityNotFound)
	}

	block, err := a.blockFromBlockStorageByCadenceHeight(cadenceHeight)
	if err != nil {
		fmt.Println(err)
		return handleError[*api.Transaction](errs.ErrInternal)
	}
	transactions, err := a.blockTransactions(block.Height)
	if err != nil {
		return handleError[*api.Transaction](errs.ErrInternal)
	}

	txIndex := int(index)
	if txIndex >= len(transactions) {
		return handleError[*api.Transaction](errs.ErrEntityNotFound)
	}
	receipt := transactions[txIndex].Receipt
	receipt.BlockHash, _ = block.Hash()
	return api.NewTransactionResult(transactions[txIndex].Transaction, receipt, EVMMainnetChainID)
}

// GetTransactionReceipt returns the transaction receipt for the given transaction hash.
func (a *APINamespace) GetTransactionReceipt(
	_ context.Context,
	hash common.Hash,
) (map[string]interface{}, error) {

	cadenceHeight, err := a.storage.CadenceBlockHeightForTransactionHash(hash)

	if err != nil {
		fmt.Println(err)

		return handleError[map[string]interface{}](errs.ErrEntityNotFound)
	}

	block, err := a.blockFromBlockStorageByCadenceHeight(cadenceHeight)
	if err != nil {
		fmt.Println(err)
		return handleError[map[string]interface{}](errs.ErrInternal)
	}

	transactions, err := a.blockTransactions(block.Height)
	if err != nil {
		fmt.Println(err)
		return handleError[map[string]interface{}](errs.ErrInternal)
	}

	cumulativeGasUsed := uint64(0)
	for _, tx := range transactions {
		cumulativeGasUsed += tx.Receipt.GasUsed
		if tx.Transaction.Hash() == hash {
			receipt := tx.Receipt
			receipt.BlockHash, _ = block.Hash()
			receipt.CumulativeGasUsed = cumulativeGasUsed
			txReceipt, err := api.MarshalReceipt(&receipt, tx.Transaction)
			if err != nil {
				fmt.Println(err)

				return handleError[map[string]interface{}](errs.ErrInternal)
			}

			return txReceipt, nil
		}
	}
	fmt.Println("should not come here")
	return handleError[map[string]interface{}](errs.ErrInternal)

}

// GetBlockByHash returns the requested block. When fullTx is true all transactions in the block are returned in full
// detail, otherwise only the transaction hash is returned.
func (a *APINamespace) GetBlockByHash(
	ctx context.Context,
	hash common.Hash,
	fullTx bool,
) (*api.Block, error) {

	height, err := a.storage.EVMHeightForBlockHash(hash)
	if err != nil {
		return handleError[*api.Block](errs.ErrEntityNotFound)
	}
	return a.GetBlockByNumber(ctx, rpc.BlockNumber(height), fullTx)
}

// GetBlockReceipts returns the block receipts for the given block hash or number or tag.
func (a *APINamespace) GetBlockReceipts(
	_ context.Context,
	blockNumberOrHash rpc.BlockNumberOrHash,
) ([]map[string]interface{}, error) {

	height, err := a.blockNumberOrHashToHeight(blockNumberOrHash)
	if err != nil {
		return handleError[[]map[string]interface{}](errs.ErrEntityNotFound)
	}

	block, err := a.blockFromBlockStorage(height)
	if err != nil {
		return handleError[[]map[string]interface{}](errs.ErrInternal)
	}
	transactions, err := a.blockTransactions(block.Height)
	if err != nil {
		return handleError[[]map[string]interface{}](errs.ErrInternal)
	}

	result := make([]map[string]interface{}, len(transactions))
	for i, tx := range transactions {
		txReceipt, err := api.MarshalReceipt(&tx.Receipt, tx.Transaction)
		if err != nil {
			return handleError[[]map[string]interface{}](errs.ErrInternal)
		}
		result[i] = txReceipt
	}

	return result, nil
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

	return a.GetBlockTransactionCountByNumber(ctx, rpc.BlockNumber(height))
}

// GetBlockTransactionCountByNumber returns the number of transactions
// in the block with the given block number.
func (a *APINamespace) GetBlockTransactionCountByNumber(
	_ context.Context,
	blockNumber rpc.BlockNumber,
) (*hexutil.Uint, error) {

	height, err := a.blockNumberToHeight(blockNumber)
	if err != nil {
		return handleError[*hexutil.Uint](errs.ErrEntityNotFound)
	}

	block, err := a.blockFromBlockStorage(height)
	if err != nil {
		return handleError[*hexutil.Uint](errs.ErrInternal)
	}

	transactions, err := a.blockTransactions(block.Height)
	if err != nil {
		return handleError[*hexutil.Uint](errs.ErrInternal)
	}

	count := hexutil.Uint(len(transactions))
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
	cadenceHeight, err := store.EVM().CadenceHeightFromEVMHeight(height)
	if err != nil {
		return nil, err
	}
	snap := a.storage.LedgerSnapshot(cadenceHeight)
	base, _ := flow.StringToAddress("d421a63faae318f9")
	emulator := emulator2.NewEmulator(NewViewOnlyLedger(snap), base)

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
	_ context.Context,
	criteria filters.FilterCriteria,
) ([]*types.Log, error) {

	filter := FilterCriteria{
		Addresses: criteria.Addresses,
		Topics:    criteria.Topics,
	}
	// if filter provided specific block ID
	if criteria.BlockHash != nil {

		f, err := NewIDFilter(*criteria.BlockHash, filter, a)
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

	f, err := NewRangeFilter(from.Uint64(), to.Uint64(), filter, a)
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
	_ context.Context,
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

	cadenceHeight, err := a.storage.CadenceHeightFromEVMHeight(height)
	if err != nil {
		return handleError[hexutil.Uint64](errs.ErrInternal)
	}
	snap := a.storage.LedgerSnapshot(cadenceHeight)
	base, _ := flow.StringToAddress("d421a63faae318f9")
	emulator := emulator2.NewEmulator(NewViewOnlyLedger(snap), base)

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
	_ context.Context,
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
	_ context.Context,
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
		blockRewards[i] = (*hexutil.Big)(big.NewInt(0x5f5e100))
	}

	for i := maxCount; i >= uint64(1); i-- {
		// If the requested block count is 5, and the last block number
		// is 20, then we need the blocks [16, 17, 18, 19, 20] in this
		// specific order. The first block we fetch is 20 - 5 + 1 = 16.

		blockHeight := lastBlockNumber - i + 1
		block, err := a.blockFromBlockStorage(blockHeight)
		if err != nil {
			continue
		}

		if i == maxCount {
			oldestBlock = (*hexutil.Big)(big.NewInt(int64(block.Height)))
		}

		baseFees = append(baseFees, (*hexutil.Big)(big.NewInt(0)))

		rewards = append(rewards, blockRewards)

		gasUsedRatio := float64(block.TotalGasUsed) / float64(blockGasLimit)
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
	_ context.Context,
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
func (a *APINamespace) ChainId(_ context.Context) (*hexutil.Big, error) {
	return (*hexutil.Big)(EVMMainnetChainID), nil
}

// Coinbase is the address that mining rewards will be sent to (alias for Etherbase).
func (a *APINamespace) Coinbase(_ context.Context) (common.Address, error) {
	return evmTypes.CoinbaseAddress.ToCommon(), nil
}

// GasPrice returns a suggestion for a gas price for legacy transactions.
func (a *APINamespace) GasPrice(_ context.Context) (*hexutil.Big, error) {
	return (*hexutil.Big)(big.NewInt(0)), nil
}

// GetUncleCountByBlockHash returns number of uncles in the block for the given block hash
func (a *APINamespace) GetUncleCountByBlockHash(
	_ context.Context,
	_ common.Hash,
) *hexutil.Uint {
	count := hexutil.Uint(0)
	return &count
}

// GetUncleCountByBlockNumber returns number of uncles in the block for the given block number
func (a *APINamespace) GetUncleCountByBlockNumber(
	_ context.Context,
	_ rpc.BlockNumber,
) *hexutil.Uint {
	count := hexutil.Uint(0)
	return &count
}

// GetUncleByBlockHashAndIndex returns the uncle block for the given block hash and index.
func (a *APINamespace) GetUncleByBlockHashAndIndex(
	_ context.Context,
	_ common.Hash,
	_ hexutil.Uint,
) (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

// GetUncleByBlockNumberAndIndex returns the uncle block for the given block hash and index.
func (a *APINamespace) GetUncleByBlockNumberAndIndex(
	_ context.Context,
	_ rpc.BlockNumber,
	_ hexutil.Uint,
) (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

// MaxPriorityFeePerGas returns a suggestion for a gas tip cap for dynamic fee transactions.
func (a *APINamespace) MaxPriorityFeePerGas(_ context.Context) (*hexutil.Big, error) {
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
	_ context.Context,
	_ common.Address,
	_ []string,
	_ rpc.BlockNumberOrHash,
) (*api.AccountResult, error) {
	return nil, errs.NewEndpointNotSupportedError("eth_getProof")
}

// CreateAccessList creates an EIP-2930 type AccessList for the given transaction.
// Reexec and blockNumberOrHash can be specified to create the accessList on top of a certain state.
func (a *APINamespace) CreateAccessList(
	_ context.Context,
	_ api.TransactionArgs,
	_ *rpc.BlockNumberOrHash,
) (*api.AccessListResult, error) {
	return nil, errs.NewEndpointNotSupportedError("eth_createAccessList")
}
