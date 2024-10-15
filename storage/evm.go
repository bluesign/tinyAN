package storage

import (
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/onflow/flow-evm-gateway/models"
	gethCommon "github.com/onflow/go-ethereum/common"
	"github.com/rs/zerolog"
	"os"
)

const (
	codeEVMLastHeight                   byte = 0x01
	codeEVMHeightByCadenceHeight        byte = 0x02
	codeEVMCadenceHeightByEVMHeight     byte = 0x03
	codeEVMTransactionIDToCadenceHeight byte = 0x04

	codeEVMBlockIDToCadenceHeight byte = 0x05
	codeEVMBlockIDToEVMHeight     byte = 0x06

	codeEVMBlock = 0x07
)

type EVMHeightLookup interface {
	EVMHeightForBlockHash(hash gethCommon.Hash) (uint64, error)
	CadenceHeightForBlockHash(hash gethCommon.Hash) (uint64, error)
	CadenceBlockHeightForTransactionHash(hash gethCommon.Hash) (uint64, error)
	CadenceHeightFromEVMHeight(evmHeight uint64) (uint64, error)
	EVMHeightFromCadenceHeight(cadenceHeight uint64) (uint64, error)
}

type EVMStorage struct {
	logger      zerolog.Logger
	startHeight uint64
	evmDB       *pebble.DB
	codec       *Codec
}

type EVMBlock struct {
	Block        *models.Block
	Transactions [][]byte
	Receipts     []*models.Receipt
}

func NewEVMStorage(spork string, startHeight uint64) (*EVMStorage, error) {
	evmDb := MustOpenPebbleDB(fmt.Sprintf("db/EVM_%s", spork))

	return &EVMStorage{
		startHeight: startHeight,
		evmDB:       evmDb,
		codec:       NewCodec(),
		logger:      zerolog.New(os.Stdout).With().Timestamp().Logger(),
	}, nil
}

func (s *EVMStorage) SaveProgress(batch *pebble.Batch, height uint64) error {
	return s.codec.MarshalAndSet(batch, b(keyProgress), height)
}

func (s *EVMStorage) LastProcessedHeight() uint64 {
	var height uint64
	err := s.codec.UnmarshalAndGet(s.evmDB, b(keyProgress), &height)
	if err != nil {
		return 0
	}
	return height
}

func (s *EVMStorage) StartHeight() uint64 {
	return s.startHeight
}

func (s *EVMStorage) NewBatch() *pebble.Batch {
	return s.evmDB.NewBatch()
}

func (s *EVMStorage) Close() {
	err := s.evmDB.Close()
	if err != nil {
		s.logger.Log().Err(err).Msg("error closing database")
	}
}

func (s *EVMStorage) LastHeight() uint64 {
	height, closer, err := s.evmDB.Get(makePrefix(codeLastHeight))
	if err != nil {
		return 0
	}
	v := binary.BigEndian.Uint64(height)
	_ = closer.Close()
	return v
}

func (s *EVMStorage) SaveLastHeight(batch *pebble.Batch, height uint64) error {
	return s.codec.MarshalAndSet(batch,
		makePrefix(codeEVMLastHeight),
		b(height),
	)
}

func (s *EVMStorage) SaveBlock(evmEvents *models.CadenceEvents) error {
	batch := s.evmDB.NewBatch()
	defer batch.Commit(pebble.Sync)

	for _, transaction := range evmEvents.Transactions() {
		err := s.codec.MarshalAndSet(batch,
			makePrefix(codeEVMTransactionIDToCadenceHeight, transaction.Hash()),
			evmEvents.CadenceHeight(),
		)
		if err != nil {
			s.logger.Log().Err(err).Msg("error saving transaction id to cadence height")
		}
	}

	evmBlockHash, err := evmEvents.Block().Hash()
	if err != nil {
		s.logger.Log().Err(err).Msg("error getting evm block hash")
		panic(err) //shouldn't happen
	}

	//insert evm block
	err = s.codec.MarshalAndSet(batch,
		makePrefix(codeEVMBlockIDToCadenceHeight, evmBlockHash),
		evmEvents.CadenceHeight(),
	)
	if err != nil {
		s.logger.Log().Err(err).Msg("error saving evm block id to cadence height")
	}

	err = s.codec.MarshalAndSet(batch,
		makePrefix(codeEVMBlockIDToEVMHeight, evmBlockHash),
		evmEvents.Block().Height,
	)
	if err != nil {
		s.logger.Log().Err(err).Msg("error saving evm block id to evm height")
	}

	err = s.codec.MarshalAndSet(batch,
		makePrefix(codeEVMHeightByCadenceHeight, evmEvents.CadenceHeight()),
		evmEvents.Block().Height,
	)
	if err != nil {
		s.logger.Log().Err(err).Msg("error saving evm height by cadence height")
	}

	err = s.codec.MarshalAndSet(batch,
		makePrefix(codeEVMCadenceHeightByEVMHeight, evmEvents.Block().Height),
		evmEvents.CadenceHeight(),
	)
	if err != nil {
		s.logger.Log().Err(err).Msg("error saving cadence height by evm height")
	}

	//save block data
	block := &EVMBlock{
		Block:        evmEvents.Block(),
		Transactions: make([][]byte, len(evmEvents.Transactions())),
		Receipts:     evmEvents.Receipts(),
	}

	for i, tx := range evmEvents.Transactions() {
		block.Transactions[i], err = tx.MarshalBinary()
		if err != nil {
			s.logger.Log().Err(err).Msg("error marshalling transaction")
			panic(err)
		}
	}

	err = s.codec.MarshalAndSet(batch,
		makePrefix(codeEVMBlock, evmEvents.Block().Height),
		block,
	)
	if err != nil {
		s.logger.Log().Err(err).Msg("error saving evm block")
	}
	err = s.SaveProgress(batch, evmEvents.Block().Height)

	if err != nil {
		s.logger.Log().Err(err).Msg("error saving evm progress")
	}

	return nil
}

func (s *EVMStorage) EVMHeightForBlockHash(hash gethCommon.Hash) (uint64, error) {
	var height uint64
	err := s.codec.UnmarshalAndGet(s.evmDB, makePrefix(codeEVMBlockIDToEVMHeight, hash), &height)
	if err != nil {
		return 0, err
	}
	return height, nil
}

func (s *EVMStorage) CadenceHeightForBlockHash(hash gethCommon.Hash) (uint64, error) {
	var height uint64
	err := s.codec.UnmarshalAndGet(s.evmDB, makePrefix(codeEVMBlockIDToCadenceHeight, hash), &height)
	if err != nil {
		return 0, err
	}
	return height, nil
}

func (s *EVMStorage) CadenceBlockHeightForTransactionHash(hash gethCommon.Hash) (uint64, error) {
	var height uint64
	err := s.codec.UnmarshalAndGet(s.evmDB, makePrefix(codeEVMTransactionIDToCadenceHeight, hash), &height)
	if err != nil {
		return 0, err
	}
	return height, nil
}

func (s *EVMStorage) CadenceHeightFromEVMHeight(evmHeight uint64) (uint64, error) {
	var height uint64
	err := s.codec.UnmarshalAndGet(s.evmDB, makePrefix(codeEVMCadenceHeightByEVMHeight, evmHeight), &height)
	if err != nil {
		return 0, err
	}
	return height, nil
}

func (s *EVMStorage) EVMHeightFromCadenceHeight(cadenceHeight uint64) (uint64, error) {
	var height uint64
	err := s.codec.UnmarshalAndGet(s.evmDB, makePrefix(codeEVMHeightByCadenceHeight, cadenceHeight), &height)
	if err != nil {
		return 0, err
	}
	return height, nil
}
