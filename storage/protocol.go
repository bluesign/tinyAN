package storage

import (
	"bytes"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/onflow/flow-go/model/flow"
	"github.com/rs/zerolog"
)

var (
	codeTransaction             byte = 0x01
	codeTransactionAtCollection byte = 0x02
	codeLatestTransaction       byte = 0x03
	codeTransactionResult       byte = 0x04

	codeCollection        byte = 0x11
	codeCollectionAtBlock byte = 0x12

	codeEvent byte = 0x21
)

type ProtocolStorage struct {
	logger      zerolog.Logger
	startHeight uint64
	protocolDB  *pebble.DB
	codec       *Codec
	batch       *pebble.Batch
}

func NewProtocolStorage(spork string, startHeight uint64) (*ProtocolStorage, error) {
	protocolDb := MustOpenPebbleDB(fmt.Sprintf("db/tinyProtocol_%s", spork))

	return &ProtocolStorage{
		startHeight: startHeight,
		protocolDB:  protocolDb,
		codec:       NewCodec(),
	}, nil
}

func (s *ProtocolStorage) StartHeight() uint64 {
	return s.startHeight
}

func (s *ProtocolStorage) SaveProgress(height uint64) error {
	return s.codec.MarshalAndSet(s.batch, b(keyProgress), height)
}

func (s *ProtocolStorage) LastProcessedHeight() uint64 {
	var height uint64
	err := s.codec.UnmarshalAndGet(s.protocolDB, b(keyProgress), height)
	if err != nil {
		return 0
	}
	return height
}

func (s *ProtocolStorage) NewBatch() *pebble.Batch {
	s.batch = s.protocolDB.NewBatch()
	return s.batch
}

func (s *ProtocolStorage) CommitBatch() error {
	return s.batch.Commit(pebble.Sync)
}

func (s *ProtocolStorage) Close() {
	err := s.protocolDB.Close()
	if err != nil {
		s.logger.Log().Err(err).Msg("error closing database")
	}
}

func (s *ProtocolStorage) SaveTransaction(blockId flow.Identifier, collectionId flow.Identifier, transaction *flow.TransactionBody) error {
	transactionId := transaction.ID()

	err := s.codec.MarshalAndSet(s.batch,
		makePrefix(codeTransaction, blockId, collectionId, transactionId),
		transaction,
	)
	if err != nil {
		return err
	}

	err = s.codec.MarshalAndSet(s.batch,
		makePrefix(codeTransactionAtCollection, collectionId, transaction),
		[]byte{},
	)

	if err != nil {
		return err
	}

	err = s.codec.MarshalAndSet(s.batch,
		makePrefix(codeLatestTransaction, transactionId),
		makePrefix(codeBinary, blockId, collectionId),
	)
	if err != nil {
		return err
	}

	return nil
}

func (s *ProtocolStorage) SaveCollection(blockId flow.Identifier, index uint32, collection *flow.Collection) error {
	err := s.codec.MarshalAndSet(s.batch,
		makePrefix(codeCollection, collection.ID(), blockId),
		[]byte{},
	)
	if err != nil {
		return err
	}
	return s.codec.MarshalAndSet(s.batch,
		makePrefix(codeCollectionAtBlock, blockId, index, collection.ID()),
		[]byte{},
	)
}

func (s *ProtocolStorage) SaveEvent(cadenceHeight uint64, blockId flow.Identifier, collectionId flow.Identifier, event flow.Event) error {
	return s.codec.MarshalAndSet(s.batch,
		makePrefix(codeEvent, blockId, collectionId, event.TransactionID, event.EventIndex, reverse(string(event.Type))),
		event)
}

func (s *ProtocolStorage) SaveTransactionResult(blockId flow.Identifier, collectionId flow.Identifier, transactionResult flow.LightTransactionResult) error {
	return s.codec.MarshalAndSet(s.batch,
		makePrefix(codeTransactionResult, blockId, collectionId, transactionResult.TransactionID),
		transactionResult,
	)
}

func (s *ProtocolStorage) CollectionsAtBlock(blockId flow.Identifier) (result []flow.Identifier) {
	prefix := makePrefix(codeCollectionAtBlock, blockId)
	options := &pebble.IterOptions{}
	iter, _ := s.protocolDB.NewIter(options)

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		_, err := iter.ValueAndErr()
		if err != nil {
			break
		}
		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}
		id, _ := flow.ByteSliceToId(iter.Key()[37:])
		result = append(result, id)
	}
	return result
}

func (s *ProtocolStorage) TransactionsAtCollection(collectionId flow.Identifier) (result []flow.Identifier) {
	prefix := makePrefix(codeTransactionAtCollection, collectionId)
	options := &pebble.IterOptions{}
	iter, _ := s.protocolDB.NewIter(options)

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

func (s *ProtocolStorage) Transaction(blockId flow.Identifier, collectionId flow.Identifier, transactionId flow.Identifier) (flow.TransactionBody, error) {
	var result = flow.TransactionBody{}
	err := s.codec.UnmarshalAndGet(s.protocolDB, makePrefix(codeTransaction, blockId, collectionId, transactionId), &result)
	return result, err
}

func (s *ProtocolStorage) TransactionById(transactionId flow.Identifier) (flow.TransactionBody, error) {
	var result []byte
	err := s.codec.UnmarshalAndGet(s.protocolDB, makePrefix(codeLatestTransaction, transactionId), &result)
	if err != nil {
		return flow.TransactionBody{}, err
	}

	blockId, _ := flow.ByteSliceToId(result[1:33])
	collectionId, _ := flow.ByteSliceToId(result[33:])

	return s.Transaction(blockId, collectionId, transactionId)
}

func (s *ProtocolStorage) TransactionResult(blockId flow.Identifier, collectionId flow.Identifier, transactionId flow.Identifier) (flow.LightTransactionResult, error) {
	var result flow.LightTransactionResult
	err := s.codec.UnmarshalAndGet(s.protocolDB, makePrefix(codeTransactionResult, blockId, collectionId, transactionId), &result)
	return result, err
}

func (s *ProtocolStorage) TransactionResultById(blocks *BlocksStorage, transactionId flow.Identifier) (flow.Identifier, uint64, flow.Identifier, flow.LightTransactionResult, error) {
	var location []byte
	err := s.codec.UnmarshalAndGet(s.protocolDB, makePrefix(codeLatestTransaction, transactionId), &location)
	if err != nil {
		return flow.ZeroID, 0, flow.ZeroID, flow.LightTransactionResult{}, err
	}

	blockId, _ := flow.ByteSliceToId(location[1:33])
	collectionId, _ := flow.ByteSliceToId(location[33:])

	result, err := s.TransactionResult(blockId, collectionId, transactionId)
	height, _ := blocks.GetBlockHeightByID(blockId)

	return blockId, height, collectionId, result, err
}

func (s *ProtocolStorage) EventsByName(blockId flow.Identifier, eventType string) (result []flow.Event) {
	prefix := makePrefix(codeEvent, blockId)
	options := &pebble.IterOptions{}
	iter, _ := s.protocolDB.NewIter(options)
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

func (s *ProtocolStorage) Events(blockId flow.Identifier, collectionId flow.Identifier, transactionId flow.Identifier) (result []flow.Event) {
	prefix := makePrefix(codeEvent, blockId, collectionId, transactionId)
	options := &pebble.IterOptions{}
	iter, _ := s.protocolDB.NewIter(options)

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
