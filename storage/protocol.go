package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/onflow/flow-go/model/flow"
	"github.com/rs/zerolog"
	"os"
)

var (
	codeTransaction             byte = 0x01
	codeTransactionAtCollection byte = 0x02
	codeLatestTransaction       byte = 0x03
	codeTransactionResult       byte = 0x04

	codeCollection        byte = 0x11
	codeCollectionAtBlock byte = 0x12

	codeEvent byte = 0x21

	codeBlockHeightByID byte = 0x31
	codeBlockByHeight   byte = 0x32
	codeLastHeight      byte = 0x33
	codeBlockIdByHeight byte = 0x34
)

type ProtocolStorage struct {
	logger      zerolog.Logger
	startHeight uint64
	protocolDB  *pebble.DB
	codec       *Codec
}

func NewProtocolStorage(spork string, startHeight uint64) (*ProtocolStorage, error) {
	protocolDb := MustOpenPebbleDB(fmt.Sprintf("db/Protocol_%s", spork))

	return &ProtocolStorage{
		startHeight: startHeight,
		protocolDB:  protocolDb,
		codec:       NewCodec(),
		logger:      zerolog.New(os.Stdout),
	}, nil
}

func (s *ProtocolStorage) StartHeight() uint64 {
	return s.startHeight
}

func (s *ProtocolStorage) SaveProgress(batch *pebble.Batch, height uint64) error {
	return s.codec.MarshalAndSet(batch, b(keyProgress), height)
}

func (s *ProtocolStorage) LastProcessedHeight() uint64 {
	var height uint64
	err := s.codec.UnmarshalAndGet(s.protocolDB, b(keyProgress), &height)
	if err != nil {
		return 0
	}
	return height
}

func (s *ProtocolStorage) NewBatch() *pebble.Batch {
	return s.protocolDB.NewBatch()
}

func (s *ProtocolStorage) Close() {
	err := s.protocolDB.Close()
	if err != nil {
		s.logger.Log().Err(err).Msg("error closing database")
	}
}

func (s *ProtocolStorage) SaveTransaction(batch *pebble.Batch, blockId flow.Identifier, collectionId flow.Identifier, transaction *flow.TransactionBody) error {
	transactionId := transaction.ID()

	err := s.codec.MarshalAndSet(batch,
		makePrefix(codeTransaction, blockId, collectionId, transactionId),
		transaction,
	)
	if err != nil {
		return err
	}

	err = s.codec.MarshalAndSet(batch,
		makePrefix(codeTransactionAtCollection, collectionId, transactionId),
		[]byte{},
	)

	if err != nil {
		return err
	}

	err = s.codec.MarshalAndSet(batch,
		makePrefix(codeLatestTransaction, transactionId),
		makePrefix(codeBinary, blockId, collectionId),
	)
	if err != nil {
		return err
	}

	return nil
}

func (s *ProtocolStorage) GetCollectionById(id flow.Identifier) (*flow.Collection, error) {
	var collection flow.Collection
	panic("TODO")
	err := s.codec.UnmarshalAndGet(s.protocolDB, makePrefix(codeCollection, id), &collection)
	return &collection, err
}

func (s *ProtocolStorage) SaveCollection(batch *pebble.Batch, blockId flow.Identifier, index uint32, collection *flow.Collection) error {
	err := s.codec.MarshalAndSet(batch,
		makePrefix(codeCollection, collection.ID(), blockId),
		[]byte{},
	)
	if err != nil {
		return err
	}
	return s.codec.MarshalAndSet(batch,
		makePrefix(codeCollectionAtBlock, blockId, index, collection.ID()),
		[]byte{},
	)
}

func (s *ProtocolStorage) SaveEvent(batch *pebble.Batch, cadenceHeight uint64, blockId flow.Identifier, collectionId flow.Identifier, event flow.Event) error {
	return s.codec.MarshalAndSet(batch,
		makePrefix(codeEvent, blockId, collectionId, event.TransactionID, event.EventIndex, reverse(string(event.Type))),
		event)
}

func (s *ProtocolStorage) SaveTransactionResult(batch *pebble.Batch, blockId flow.Identifier, collectionId flow.Identifier, transactionResult flow.LightTransactionResult) error {
	return s.codec.MarshalAndSet(batch,
		makePrefix(codeTransactionResult, blockId, collectionId, transactionResult.TransactionID),
		transactionResult,
	)
}

func (s *ProtocolStorage) CollectionsAtBlock(blockId flow.Identifier) (result []flow.Identifier) {
	fmt.Println("CollectionsAtBlock", blockId.String())

	prefix := makePrefix(codeCollectionAtBlock, blockId)
	options := &pebble.IterOptions{}
	iter, _ := s.protocolDB.NewIter(options)

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		_, err := iter.ValueAndErr()
		if err != nil {
			break
		}
		fmt.Println(iter.Key())

		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}
		fmt.Println(iter.Key())
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

func (s *ProtocolStorage) TransactionResultById(transactionId flow.Identifier) (flow.Identifier, uint64, flow.Identifier, flow.LightTransactionResult, error) {
	var location []byte
	err := s.codec.UnmarshalAndGet(s.protocolDB, makePrefix(codeLatestTransaction, transactionId), &location)
	if err != nil {
		return flow.ZeroID, 0, flow.ZeroID, flow.LightTransactionResult{}, err
	}

	blockId, _ := flow.ByteSliceToId(location[1:33])
	collectionId, _ := flow.ByteSliceToId(location[33:])

	result, err := s.TransactionResult(blockId, collectionId, transactionId)
	height, _ := s.GetBlockHeightByID(blockId)

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

func (s *ProtocolStorage) SaveBlock(batch *pebble.Batch, header *flow.Header) error {
	id := header.ID()
	height := header.Height

	if err := s.codec.MarshalAndSet(batch,
		makePrefix(codeBlockHeightByID, id),
		b(height),
	); err != nil {
		return err
	}

	if err := s.codec.MarshalAndSet(batch,
		makePrefix(codeBlockByHeight, height),
		header,
	); err != nil {
		return err
	}

	if err := s.codec.MarshalAndSet(batch,
		makePrefix(codeBlockIdByHeight, height),
		header.ID(),
	); err != nil {
		return err
	}

	s.SaveProgress(batch, height)

	return nil
}

func (s *ProtocolStorage) GetBlockHeightByID(id flow.Identifier) (uint64, error) {
	s.logger.Log().Msgf("GetBlockHeightByID %s", id.String())

	dbKey := makePrefix(codeBlockHeightByID, b(id))
	var height []byte
	err := s.codec.UnmarshalAndGet(s.protocolDB, dbKey, &height)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(height), nil
}

func (s *ProtocolStorage) LastHeight() uint64 {
	return s.LastProcessedHeight()
}

func (s *ProtocolStorage) GetLatestBlock() (*flow.Header, error) {
	height := s.LastHeight()
	return s.GetBlockByHeight(height)
}

func (s *ProtocolStorage) GetBlockByHeight(height uint64) (*flow.Header, error) {
	s.logger.Log().Msgf("GetBlockByHeight %d", height)
	dbKey := makePrefix(codeBlockByHeight, b(height))
	var block flow.Header
	err := s.codec.UnmarshalAndGet(s.protocolDB, dbKey, &block)
	if err != nil {
		s.logger.Log().Err(err).Msg("error getting block by height")
		return nil, err
	}
	return &block, nil
}

func (s *ProtocolStorage) GetBlockIdByHeight(height uint64) (flow.Identifier, error) {
	s.logger.Log().Msgf("GetBlockIdByHeight %d", height)

	dbKey := makePrefix(codeBlockIdByHeight, b(height))
	var id flow.Identifier
	err := s.codec.UnmarshalAndGet(s.protocolDB, dbKey, &id)
	if err != nil {
		return flow.ZeroID, err
	}
	return id, nil
}

func (s *ProtocolStorage) GetBlockById(id flow.Identifier) (*flow.Header, error) {
	s.logger.Log().Msgf("GetBlockById %s", id.String())

	height, err := s.GetBlockHeightByID(id)
	if err != nil {
		return nil, err
	}
	return s.GetBlockByHeight(height)
}
