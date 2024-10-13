package storage

import (
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/onflow/flow-go/model/flow"
	"github.com/rs/zerolog"
)

const (
	codeBlockHeightByID byte = 0x01
	codeBlockByHeight   byte = 0x02
	codeLastHeight      byte = 0x03
)

type BlocksStorage struct {
	logger      zerolog.Logger
	startHeight uint64
	blocksDb    *pebble.DB
	codec       *Codec
	batch       *pebble.Batch
}

func NewBlocksStorage(spork string, startHeight uint64) (*BlocksStorage, error) {
	blocksDb := MustOpenPebbleDB(fmt.Sprintf("db/tinyBlocks_%s", spork))

	return &BlocksStorage{
		startHeight: startHeight,
		blocksDb:    blocksDb,
		codec:       NewCodec(),
	}, nil
}

func (s *BlocksStorage) SaveProgress(height uint64) error {
	return s.codec.MarshalAndSet(s.batch, b(keyProgress), height)
}

func (s *BlocksStorage) LastProcessedHeight() uint64 {
	var height uint64
	err := s.codec.UnmarshalAndGet(s.blocksDb, b(keyProgress), height)
	if err != nil {
		return 0
	}
	return height
}

func (s *BlocksStorage) StartHeight() uint64 {
	return s.startHeight
}

func (s *BlocksStorage) NewBatch() *pebble.Batch {
	s.batch = s.blocksDb.NewBatch()
	return s.batch
}

func (s *BlocksStorage) CommitBatch() error {
	return s.batch.Commit(pebble.Sync)
}

func (s *BlocksStorage) Close() {
	err := s.blocksDb.Close()
	if err != nil {
		s.logger.Log().Err(err).Msg("error closing database")
	}
}

func (s *BlocksStorage) LastHeight() uint64 {
	height, closer, err := s.blocksDb.Get(makePrefix(codeLastHeight))
	if err != nil {
		return 0
	}
	v := binary.BigEndian.Uint64(height)
	_ = closer.Close()
	return v
}

func (s *BlocksStorage) SaveLastHeight(height uint64) error {
	return s.codec.MarshalAndSet(s.batch,
		makePrefix(codeLastHeight),
		b(height),
	)
}

func (s *BlocksStorage) SaveBlock(block *flow.Header) error {
	id := block.ID()
	height := block.Height

	if err := s.codec.MarshalAndSet(s.batch,
		makePrefix(codeBlockHeightByID, id),
		b(height),
	); err != nil {
		return err
	}

	if err := s.codec.MarshalAndSet(s.batch,
		makePrefix(codeBlockByHeight, height),
		block,
	); err != nil {
		return err
	}
	return nil
}

func (s *BlocksStorage) GetBlockHeightByID(id flow.Identifier) (uint64, error) {
	dbKey := makePrefix(codeBlockHeightByID, b(id))
	var height uint64
	err := s.codec.UnmarshalAndGet(s.blocksDb, dbKey, &height)
	if err != nil {
		return 0, err
	}
	return height, nil
}

func (s *BlocksStorage) GetBlockByHeight(height uint64) (*flow.Header, error) {
	dbKey := makePrefix(codeBlockByHeight, b(height))
	var block flow.Header
	err := s.codec.UnmarshalAndGet(s.blocksDb, dbKey, &block)
	if err != nil {
		return nil, err
	}
	return &block, nil
}

func (s *BlocksStorage) GetBlockById(id flow.Identifier) (*flow.Header, error) {
	height, err := s.GetBlockHeightByID(id)
	if err != nil {
		return nil, err
	}
	return s.GetBlockByHeight(height)
}
