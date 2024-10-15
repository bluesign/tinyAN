package storage

import (
	"bytes"
	"fmt"
	"github.com/onflow/cadence/encoding/ccf"
	jsoncdc "github.com/onflow/cadence/encoding/json"
	"github.com/onflow/flow-evm-gateway/models"
	"github.com/onflow/flow-go/fvm/evm/types"
	flowgo "github.com/onflow/flow-go/model/flow"
	gethCommon "github.com/onflow/go-ethereum/common"
	gethTypes "github.com/onflow/go-ethereum/core/types"
	"github.com/onflow/go-ethereum/rlp"
	"math/big"
	"strings"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go/fvm/evm/events"
)

var zeroGethHash = gethCommon.HexToHash("0x0")

// isBlockExecutedEvent checks whether the given event contains block executed data.
func isBlockExecutedEvent(event cadence.Event) bool {
	if event.EventType == nil {
		return false
	}
	return strings.Contains(event.EventType.ID(), string(events.EventTypeBlockExecuted))
}

// isTransactionExecutedEvent checks whether the given event contains transaction executed data.
func isTransactionExecutedEvent(event cadence.Event) bool {
	if event.EventType == nil {
		return false
	}
	return strings.Contains(event.EventType.ID(), string(events.EventTypeTransactionExecuted))
}

func DecodeTransactionEvent(event cadence.Event) (models.Transaction, *models.Receipt, error) {
	txEvent, err := events.DecodeTransactionEventPayload(event)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to Cadence decode transaction event [%s]: %w", event.String(), err)
	}

	gethReceipt := &gethTypes.Receipt{
		BlockNumber:       big.NewInt(int64(txEvent.BlockHeight)),
		Type:              txEvent.TransactionType,
		TxHash:            txEvent.Hash,
		ContractAddress:   gethCommon.HexToAddress(txEvent.ContractAddress),
		GasUsed:           txEvent.GasConsumed,
		TransactionIndex:  uint(txEvent.Index),
		EffectiveGasPrice: big.NewInt(0),
	}

	if len(txEvent.Logs) > 0 {
		err = rlp.Decode(bytes.NewReader(txEvent.Logs), &gethReceipt.Logs)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to RLP-decode logs: %w", err)
		}
	}

	if txEvent.ErrorCode == uint16(types.ErrCodeNoError) {
		gethReceipt.Status = gethTypes.ReceiptStatusSuccessful
	} else {
		gethReceipt.Status = gethTypes.ReceiptStatusFailed
	}

	gethReceipt.Bloom = gethTypes.CreateBloom([]*gethTypes.Receipt{gethReceipt})

	var revertReason []byte
	if txEvent.ErrorCode == uint16(types.ExecutionErrCodeExecutionReverted) {
		revertReason = txEvent.ReturnedData
	}

	receipt := models.NewReceipt(gethReceipt, revertReason, txEvent.PrecompiledCalls)

	var tx models.Transaction
	// check if the transaction payload is actually from a direct call,
	// which is a special state transition in Flow EVM.
	if txEvent.TransactionType == types.DirectCallTxType {
		directCall, err := types.DirectCallFromEncoded(txEvent.Payload)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to RLP-decode direct call [%x]: %w", txEvent.Payload, err)
		}
		tx = models.DirectCall{DirectCall: directCall}
	} else {
		gethTx := &gethTypes.Transaction{}
		if err := gethTx.UnmarshalBinary(txEvent.Payload); err != nil {
			return nil, nil, fmt.Errorf("failed to RLP-decode transaction [%x]: %w", txEvent.Payload, err)
		}
		receipt.EffectiveGasPrice = gethTx.EffectiveGasTipValue(nil)
		tx = models.TransactionCall{Transaction: gethTx}
	}

	return tx, receipt, nil
}

func decodeBlockEvent(event cadence.Event) (*models.Block, error) {
	payload, err := events.DecodeBlockEventPayload(event)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to Cadence-decode EVM block event [%s]: %w",
			event.String(),
			err,
		)
	}

	var fixedHash *string
	// If the `PrevRandao` field is the zero hash, we know that
	// this is a block with the legacy format, and we need to
	// fix its hash, due to the hash calculation breaking change.
	if payload.PrevRandao == zeroGethHash {
		hash := payload.Hash.String()
		fixedHash = &hash
	}

	return &models.Block{
		Block: &types.Block{
			ParentBlockHash:     payload.ParentBlockHash,
			Height:              payload.Height,
			Timestamp:           payload.Timestamp,
			TotalSupply:         payload.TotalSupply.Value,
			ReceiptRoot:         payload.ReceiptRoot,
			TransactionHashRoot: payload.TransactionHashRoot,
			TotalGasUsed:        payload.TotalGasUsed,
			PrevRandao:          payload.PrevRandao,
		},
		FixedHash: fixedHash,
	}, nil
}

// NewCadenceEvents decodes the events into evm types.
func ParseCadenceEvents(events []flowgo.Event) (*CadenceEvents, error) {
	cadenceEvents, err := DecodeCadenceEvents(events)
	if err != nil {
		return nil, err
	}

	// if cadence event is empty don't calculate any dynamic values
	if cadenceEvents.Block == nil {
		return cadenceEvents, nil
	}

	// calculate dynamic values
	cumulativeGasUsed := uint64(0)
	blockHash, err := cadenceEvents.Block.Hash()
	if err != nil {
		return nil, err
	}

	// Log index field holds the index position in the entire block
	logIndex := uint(0)
	for i, rcp := range cadenceEvents.Receipts {
		// add transaction hashes to the block
		cadenceEvents.Block.TransactionHashes = append(cadenceEvents.Block.TransactionHashes, rcp.TxHash)
		// calculate cumulative gas used up to that point
		cumulativeGasUsed += rcp.GasUsed
		rcp.CumulativeGasUsed = cumulativeGasUsed
		// set the transaction index
		rcp.TransactionIndex = uint(i)
		// set calculate block hash
		rcp.BlockHash = blockHash
		// dynamically add missing log fields
		for _, l := range rcp.Logs {
			l.BlockNumber = rcp.BlockNumber.Uint64()
			l.BlockHash = rcp.BlockHash
			l.TxHash = rcp.TxHash
			l.TxIndex = rcp.TransactionIndex
			l.Index = logIndex
			l.Removed = false
			logIndex++
		}
	}

	return cadenceEvents, nil
}

type CadenceEvents struct {
	Block        *models.Block        // EVM block (at most one per Flow block)
	Transactions []models.Transaction // transactions in the EVM block
	Receipts     []*models.Receipt    // receipts for transactions
}

func DecodeCadenceEvents(events []flowgo.Event) (*CadenceEvents, error) {
	// decode and cache block and transactions
	var e CadenceEvents

	for _, event := range events {
		decodedEvent, err := ccf.Decode(nil, event.Payload)
		if err != nil {
			decodedEvent, err = jsoncdc.Decode(nil, event.Payload)
			if err != nil {
				panic("cant decode event")
			}
		}
		val, _ := decodedEvent.(cadence.Event)

		if isBlockExecutedEvent(val) {
			block, err := decodeBlockEvent(val)
			if err != nil {
				return nil, err
			}
			e.Block = block
		}

		if isTransactionExecutedEvent(val) {
			tx, receipt, err := DecodeTransactionEvent(val)
			if err != nil {
				return &e, err
			}
			e.Transactions = append(e.Transactions, tx)
			e.Receipts = append(e.Receipts, receipt)
		}
	}

	return &e, nil
}
