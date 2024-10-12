package server

import (
	"context"
	"github.com/bluesign/tinyAN/storage"
	"github.com/gorilla/mux"
	"github.com/onflow/flow-evm-gateway/api"
	evmTypes "github.com/onflow/flow-go/fvm/evm/types"
	"github.com/onflow/go-ethereum/common"
	"github.com/onflow/go-ethereum/common/hexutil"
	"github.com/onflow/go-ethereum/core/types"
	"github.com/onflow/go-ethereum/rlp"
	"github.com/onflow/go-ethereum/rpc"
	"math/big"
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
	cadenceEvents, err := a.storage.GetEvmBlockByHeight(uint64(number))
	if err != nil {
		return nil, err
	}

	block := cadenceEvents.Block()
	blockGasLimit := uint64(0)
	h, _ := block.Hash()

	blockResponse := &api.Block{
		Hash:             h,
		Number:           hexutil.Uint64(block.Height),
		ParentHash:       block.ParentBlockHash,
		ReceiptsRoot:     block.ReceiptRoot,
		TransactionsRoot: block.TransactionHashRoot,
		Transactions:     block.TransactionHashes,
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
		return nil, err
	}
	blockSize := rlp.ListSize(uint64(len(blockBytes)))

	transactions := cadenceEvents.Transactions()

	if len(transactions) > 0 {
		totalGasUsed := hexutil.Uint64(0)
		logs := make([]*types.Log, 0)
		for i, tx := range transactions {
			txReceipt := cadenceEvents.Receipts()[i]
			totalGasUsed += hexutil.Uint64(txReceipt.GasUsed)
			logs = append(logs, txReceipt.Logs...)
			blockSize += tx.Size()
		}
		blockResponse.GasUsed = totalGasUsed
		blockResponse.LogsBloom = types.LogsBloom(logs)
	}
	blockResponse.Size = hexutil.Uint64(rlp.ListSize(blockSize))

	if full {
		blockResponse.Transactions = transactions
	}

	return blockResponse, nil
}
