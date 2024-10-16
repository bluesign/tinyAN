package storage

import (
	"bytes"
	"fmt"
	"github.com/onflow/cadence"
	"github.com/onflow/flow-evm-gateway/models"
	"github.com/onflow/flow-go/fvm/evm/events"
	"github.com/onflow/flow-go/fvm/evm/types"
	gethCommon "github.com/onflow/go-ethereum/common"
	gethTypes "github.com/onflow/go-ethereum/core/types"
	"github.com/onflow/go-ethereum/rlp"
	"math/big"
)

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
