package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/onflow/flow-archive/codec/zbor"
	"github.com/onflow/flow-go/fvm/storage/snapshot"
	"github.com/onflow/flow-go/ledger"
	"github.com/onflow/flow-go/ledger/common/convert"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/module/executiondatasync/execution_data"
)

var (
	codeBlock             byte = 0x0
	codeCollection        byte = 0x1 // can be unused
	codeTransaction       byte = 0x2
	codeHeight            byte = 0x3
	codeEvent             byte = 0x4
	codeTransactionResult byte = 0x5

	codeLatestTransaction byte = 0x6
	codeLastHeight        byte = 0x7

	codePair byte = 0x8

	codeCollectionAtBlock       byte = 0x9
	codeTransactionAtCollection byte = 0xa

	codeLedgerPayload byte = 0xf0
)

type ProtocolStorage struct {
	protocolDb   *pebble.DB
	checkpointDb *pebble.DB
	ledgerDb     *pebble.DB
	codec        *zbor.Codec
	ledgerBatch  *pebble.Batch
	batch        *pebble.Batch
}

func NewProtocolStorage() (*ProtocolStorage, error) {

	opts := &pebble.Options{}

	checkpointDb, err := pebble.Open("tinyCheckpoint", opts)
	if err != nil {
		return nil, err
	}

	protocolDb, err := pebble.Open("tinyProto", &pebble.Options{})
	if err != nil {
		return nil, err
	}

	ledgerDb, err := pebble.Open("tinyLedger", opts)
	if err != nil {
		return nil, err
	}

	return &ProtocolStorage{
		protocolDb:   protocolDb,
		checkpointDb: checkpointDb,
		ledgerDb:     ledgerDb,
		codec:        zbor.NewCodec(),
	}, nil

}

func (s *ProtocolStorage) Bootstrap(spork string, sporkHeight uint64) {
	var wg sync.WaitGroup
	ch := make(chan int)

	for w := 1; w <= 5; w++ {
		go importWorker(s.checkpointDb, spork, sporkHeight, ch, &wg)
	}

	for i := 0; i < 16; i++ {
		wg.Add(1)
		ch <- i
	}

	wg.Wait()
	close(ch)

}

func (s *ProtocolStorage) SetLedger(key []byte, value []byte) error {
	if err := s.ledgerBatch.Set(key, value, pebble.Sync); err != nil {
		return err
	}
	return nil
}

func (s *ProtocolStorage) Set(key []byte, value []byte) error {
	if err := s.batch.Set(key, value, pebble.Sync); err != nil {
		return err
	}
	return nil
}

func (s *ProtocolStorage) Get(key []byte) ([]byte, error) {
	value, closer, err := s.protocolDb.Get(key)
	if err != nil {
		return nil, err
	}
	_ = closer.Close()
	return value, nil
}

func (s *ProtocolStorage) InsertTransaction(blockId flow.Identifier, collectionId flow.Identifier, transaction *flow.TransactionBody) error {
	transactionId := transaction.ID()
	transactionEncoded, err := s.codec.Encode(transaction)

	if err != nil {
		return err
	}

	err = s.Set(makePrefix(codeTransaction, blockId, collectionId, transactionId), transactionEncoded)
	if err != nil {
		return err
	}
	err = s.Set(makePrefix(codeTransactionAtCollection, collectionId, transactionId), []byte{})
	if err != nil {
		return err
	}

	err = s.Set(makePrefix(codeLatestTransaction, transactionId), makePrefix(codePair, blockId, collectionId))
	if err != nil {
		return err
	}

	return nil
}

func (s *ProtocolStorage) InsertCollection(blockId flow.Identifier, collection *flow.Collection) error {
	err := s.Set(makePrefix(codeCollection, collection.ID(), blockId), []byte{})
	if err != nil {
		return err
	}
	err = s.Set(makePrefix(codeCollectionAtBlock, blockId, collection.ID()), []byte{})
	if err != nil {
		return err
	}
	return nil
}

func (s *ProtocolStorage) InsertEvent(blockId flow.Identifier, collectionId flow.Identifier, event flow.Event) error {
	eventEncoded, err := s.codec.Encode(event)
	if err != nil {
		return err
	}

	err = s.Set(makePrefix(codeEvent, blockId, collectionId, event.TransactionID, event.EventIndex, reverse(string(event.Type))), eventEncoded)
	if err != nil {
		return err
	}

	return nil
}

func (s *ProtocolStorage) InsertTransactionResult(blockId flow.Identifier, collectionId flow.Identifier, transactionResult flow.LightTransactionResult) error {
	transactionResultEncoded, err := s.codec.Encode(transactionResult)
	if err != nil {
		return err
	}

	err = s.Set(makePrefix(codeTransactionResult, blockId, collectionId, transactionResult.TransactionID), transactionResultEncoded)
	if err != nil {
		return err
	}
	return nil
}

func (s *ProtocolStorage) LastHeight() uint64 {
	height, err := s.Get(makePrefix(codeLastHeight))
	if err != nil {
		return 0
	}
	return binary.BigEndian.Uint64(height)
}

func (s *ProtocolStorage) BlockId(height uint64) flow.Identifier {
	blockIdBytes, err := s.Get(makePrefix(codeHeight, height))
	if err != nil {
		return flow.ZeroID
	}
	blockId, err := flow.ByteSliceToId(blockIdBytes)
	if err != nil {
		return flow.ZeroID
	}

	return blockId
}

func (s *ProtocolStorage) BlockHeight(blockId flow.Identifier) uint64 {
	height, err := s.Get(makePrefix(codeBlock, blockId))
	if err != nil {
		return 0
	}
	return binary.BigEndian.Uint64(height)
}

func (s *ProtocolStorage) CollectionsAtBlock(blockId flow.Identifier) (result []flow.Identifier) {
	prefix := makePrefix(codeCollectionAtBlock, blockId)
	options := &pebble.IterOptions{}
	iter, _ := s.protocolDb.NewIter(options)

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		_, err := iter.ValueAndErr()
		if err != nil {
			break
		}
		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}
		id, _ := flow.ByteSliceToId(iter.Key()[33:])
		result = append(result, id)
	}
	return result
}

func (s *ProtocolStorage) TransactionsAtCollection(collectionId flow.Identifier) (result []flow.Identifier) {
	prefix := makePrefix(codeTransactionAtCollection, collectionId)
	options := &pebble.IterOptions{}
	iter, _ := s.protocolDb.NewIter(options)

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		_, err := iter.ValueAndErr()
		if err != nil {
			break
		}
		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}
		id, _ := flow.ByteSliceToId(iter.Key()[33:])
		result = append(result, id)
	}
	return result

}

func (s *ProtocolStorage) Transaction(blockId flow.Identifier, collectionId flow.Identifier, transactionId flow.Identifier) (result flow.TransactionBody) {
	txEncoded, err := s.Get(makePrefix(codeTransaction, blockId, collectionId, transactionId))
	if err != nil {
		return result
	}

	err = s.codec.Decode(txEncoded, &result)
	if err != nil {
		return result
	}

	return result
}
func (s *ProtocolStorage) TransactionById(transactionId flow.Identifier) flow.TransactionBody {
	var result = flow.TransactionBody{}
	txBlockAndCollection, err := s.Get(makePrefix(codeLatestTransaction, transactionId))
	if err != nil {
		return result
	}

	blockId, _ := flow.ByteSliceToId(txBlockAndCollection[1:33])
	collectionId, _ := flow.ByteSliceToId(txBlockAndCollection[33:])

	return s.Transaction(blockId, collectionId, transactionId)
}

func (s *ProtocolStorage) TransactionResult(blockId flow.Identifier, collectionId flow.Identifier, transactionId flow.Identifier) (result flow.LightTransactionResult) {
	txResultEncoded, err := s.Get(makePrefix(codeTransactionResult, blockId, collectionId, transactionId))
	if err != nil {
		return result
	}

	err = s.codec.Decode(txResultEncoded, &result)
	if err != nil {
		return result
	}

	return result
}

func (s *ProtocolStorage) TransactionResultById(transactionId flow.Identifier) (flow.Identifier, uint64, flow.Identifier, flow.LightTransactionResult) {
	var result = flow.LightTransactionResult{}

	txBlockAndCollection, err := s.Get(makePrefix(codeLatestTransaction, transactionId))
	if err != nil {
		return flow.ZeroID, 0, flow.ZeroID, result
	}

	blockId, _ := flow.ByteSliceToId(txBlockAndCollection[1:33])
	collectionId, _ := flow.ByteSliceToId(txBlockAndCollection[33:])

	result = s.TransactionResult(blockId, collectionId, transactionId)
	return blockId, s.BlockHeight(blockId), collectionId, result
}

func reverse(str string) (result string) {
	for _, v := range str {
		result = string(v) + result
	}
	return
}

func (s *ProtocolStorage) EventsByName(blockId flow.Identifier, eventType string) (result []flow.Event) {
	prefix := makePrefix(codeEvent, blockId)
	options := &pebble.IterOptions{}
	iter, _ := s.protocolDb.NewIter(options)
	eventTypeReversed := reverse(eventType)

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}
		if !bytes.HasSuffix(iter.Key(), b(eventTypeReversed)) {
			continue
		}
		value, err := iter.ValueAndErr()
		if err != nil {
			break
		}
		var event flow.Event
		s.codec.Decode(value, &event)
		result = append(result, event)
	}
	return result
}

func (s *ProtocolStorage) GetRegister(register flow.RegisterID, height uint64) ledger.Value {

	fmt.Println("GetRegister")
	fmt.Println(register)
	key := convert.RegisterIDToLedgerKey(register)
	fmt.Println(key)

	prefix := makePrefix(codeLedgerPayload, key.CanonicalForm())

	fmt.Println(prefix)

	options := &pebble.IterOptions{}
	iter, _ := s.ledgerDb.NewIter(options)

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		k := iter.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if len(k)-len(prefix) > 8 {
			continue
		}
		v := iter.Value()
		var payload ledger.Value
		s.codec.Decode(v, &payload)
		return payload
	}

	iterCheckpoint, _ := s.protocolDb.NewIter(options)

	for iterCheckpoint.SeekGE(prefix); iterCheckpoint.Valid(); iterCheckpoint.Next() {
		k := iterCheckpoint.Key()

		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if len(k)-len(prefix) > 8 {
			continue
		}
		v := iterCheckpoint.Value()
		var payload ledger.Value
		s.codec.Decode(v, &payload)
		fmt.Println("checkpoint2")
		return payload
	}

	iterCheckpoint, _ = s.checkpointDb.NewIter(options)

	for iterCheckpoint.SeekGE(prefix); iterCheckpoint.Valid(); iterCheckpoint.Next() {
		k := iterCheckpoint.Key()

		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if len(k)-len(prefix) > 8 {
			continue
		}
		v := iterCheckpoint.Value()
		var payload ledger.Value
		s.codec.Decode(v, &payload)
		fmt.Println("checkpointDb")
		return payload
	}

	fmt.Println("nil")
	return nil
}

func (s *ProtocolStorage) GetRegisterFunc(
	height uint64,
) func(flow.RegisterID) (flow.RegisterValue, error) {
	return func(regID flow.RegisterID) (flow.RegisterValue, error) {
		value := s.GetRegister(regID, height)

		fmt.Println("value:", hex.EncodeToString(value))
		return []byte(value), nil
	}
}

func (s *ProtocolStorage) StorageSnapshot(height uint64) snapshot.StorageSnapshot {
	return snapshot.NewReadFuncStorageSnapshot(
		s.GetRegisterFunc(height))
}

func (s *ProtocolStorage) Events(blockId flow.Identifier, collectionId flow.Identifier, transactionId flow.Identifier) (result []flow.Event) {
	prefix := makePrefix(codeEvent, blockId, collectionId, transactionId)
	options := &pebble.IterOptions{}
	iter, _ := s.protocolDb.NewIter(options)

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}
		value, err := iter.ValueAndErr()
		if err != nil {
			break
		}
		var event flow.Event
		s.codec.Decode(value, &event)

		result = append(result, event)
	}
	return result
}

func (s *ProtocolStorage) NewBatch() {
	s.batch = s.protocolDb.NewBatch()
	s.ledgerBatch = s.ledgerDb.NewBatch()

}

func (s *ProtocolStorage) CommitBatch() {
	s.batch.Commit(pebble.Sync)
	s.ledgerBatch.Commit(pebble.Sync)

}

func (s *ProtocolStorage) ProcessExecutionData(height uint64, executionData *execution_data.BlockExecutionData) error {

	for _, chunk := range executionData.ChunkExecutionDatas {
		//if index == len(executionData.ChunkExecutionDatas)-1 {
		//		continue
		//	}
		err := s.InsertCollection(executionData.BlockID, chunk.Collection)
		if err != nil {
			return err
		}

		for _, transaction := range chunk.Collection.Transactions {
			err := s.InsertTransaction(executionData.BlockID, chunk.Collection.ID(), transaction)
			if err != nil {
				return err
			}
		}

		for _, event := range chunk.Events {
			err := s.InsertEvent(executionData.BlockID, chunk.Collection.ID(), event)
			if err != nil {
				return err
			}
		}

		for _, transactionResult := range chunk.TransactionResults {
			err := s.InsertTransactionResult(executionData.BlockID, chunk.Collection.ID(), transactionResult)
			if err != nil {
				return err
			}
		}
		for _, payload := range chunk.TrieUpdate.Payloads {
			if payload == nil {
				continue
			}

			key, err := payload.Key()
			if err != nil {
				continue
			}
			value := payload.Value()

			valueEncoded, err := s.codec.Encode(value)
			if err != nil {
				fmt.Println("error encoding payload")
				continue
			}

			s.SetLedger(makePrefix(codeLedgerPayload, key.CanonicalForm(), uint64(0xFFFFFFFFFFFFFFFF-height)), valueEncoded)

		}

	}

	err := s.Set(makePrefix(codeBlock, executionData.BlockID), b(height))
	if err != nil {
		return err
	}

	err = s.Set(makePrefix(codeHeight, height), b(executionData.BlockID))
	if err != nil {
		return err
	}

	err = s.Set(makePrefix(codeLastHeight), b(height))
	if err != nil {
		return err
	}

	return nil
}

func (s *ProtocolStorage) Close() {
	s.protocolDb.Close()
	s.ledgerDb.Close()
	s.checkpointDb.Close()
}
