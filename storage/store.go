package storage

import (
	"github.com/onflow/cadence"
	"github.com/onflow/cadence/encoding/ccf"
	jsoncdc "github.com/onflow/cadence/encoding/json"
	sdk "github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go/module/executiondatasync/execution_data"
	"github.com/rs/zerolog"
	"sync"
	"time"
)

var keyProgress []byte = []byte("lastHeight")

type ProgressTracker interface {
	LastProcessedHeight() uint64
}

type HeightBasedStorage struct {
	logger zerolog.Logger
	sporks []*SporkStorage
}

func NewHeightBasedStorage(sporks []*SporkStorage) *HeightBasedStorage {
	return &HeightBasedStorage{
		sporks: sporks,
	}
}

func (s *HeightBasedStorage) Sync() {
	for _, spork := range s.sporks {
		spork.Bootstrap()
	}
}

func (s *HeightBasedStorage) StorageForHeight(height uint64) *SporkStorage {
	var storage *SporkStorage
	for _, spork := range s.sporks {
		if spork.StartHeight() <= height {
			storage = spork
		}
	}
	return storage
}

func (s *HeightBasedStorage) Latest() *SporkStorage {
	return s.sporks[0]
}

type SporkStorage struct {
	logger      zerolog.Logger
	startHeight uint64
	name        string
	accessURL   string
	protocol    *ProtocolStorage
	ledger      *LedgerStorage
	blocks      *BlocksStorage
	index       *IndexStorage
	evm         *EVMStorage

	progress []ProgressTracker
}

func NewSporkStorage(spork string, accessURL string, startHeight uint64) *SporkStorage {
	protocol, _ := NewProtocolStorage(spork, startHeight)
	ledger, _ := NewLedgerStorage(spork, startHeight)
	blocks, _ := NewBlocksStorage(spork, startHeight)
	index, _ := NewIndexStorage(spork, startHeight, ledger)
	evm, _ := NewEVMStorage(spork, startHeight)

	return &SporkStorage{
		startHeight: startHeight,
		name:        spork,
		accessURL:   accessURL,
		protocol:    protocol,
		ledger:      ledger,
		blocks:      blocks,
		index:       index,
		evm:         evm,
		progress:    []ProgressTracker{protocol, ledger, blocks, index, evm},
	}
}

func (s *SporkStorage) AccessURL() string {
	return s.accessURL
}

func (s *SporkStorage) Name() string {
	return s.name
}

func (s *SporkStorage) Ledger() *LedgerStorage {
	return s.ledger
}

func (s *SporkStorage) Blocks() *BlocksStorage {
	return s.blocks
}

func (s *SporkStorage) Index() *IndexStorage {
	return s.index
}

func (s *SporkStorage) EVM() *EVMStorage {
	return s.evm
}

func (s *SporkStorage) Protocol() *ProtocolStorage {
	return s.protocol
}

func (s *SporkStorage) Close() {
	s.protocol.Close()
	s.ledger.Close()
	s.blocks.Close()
	s.index.Close()
	s.evm.Close()
}

func (s *SporkStorage) StartHeight() uint64 {
	return s.startHeight
}

func (s *SporkStorage) LastBlocksHeight() uint64 {
	return s.blocks.LastHeight()
}

func (s *SporkStorage) Bootstrap() {
	var wg sync.WaitGroup
	ch := make(chan int)

	for w := 1; w <= 4; w++ {
		go s.ledger.importWorker(s.index, s.name, s.startHeight, ch, &wg)
	}

	for i := 0; i < 16; i++ {
		wg.Add(1)
		ch <- i
	}

	wg.Wait()
	close(ch)
}

func (s *SporkStorage) ProcessExecutionData(height uint64, executionData *execution_data.BlockExecutionData) error {

	blockEvents := sdk.BlockEvents{
		BlockID:        sdk.Identifier(executionData.BlockID),
		Height:         height,
		BlockTimestamp: time.Now(), //executionData.BlockTimestamp,
		Events:         []sdk.Event{},
	}

	for index, chunk := range executionData.ChunkExecutionDatas {

		err := s.protocol.SaveCollection(executionData.BlockID, uint32(index), chunk.Collection)
		if err != nil {
			return err
		}

		for _, transaction := range chunk.Collection.Transactions {
			err := s.protocol.SaveTransaction(executionData.BlockID, chunk.Collection.ID(), transaction)
			if err != nil {
				return err
			}
		}

		for _, event := range chunk.Events {

			decodedEvent, err := ccf.Decode(nil, event.Payload)
			if err != nil {
				decodedEvent, err = jsoncdc.Decode(nil, event.Payload)
				if err != nil {
					panic("cant decode event")
				}
			}

			eventValue, _ := decodedEvent.(cadence.Event)

			blockEvents.Events = append(blockEvents.Events, sdk.Event{
				TransactionID: sdk.Identifier(event.TransactionID),
				EventIndex:    int(event.EventIndex),
				Type:          string(event.Type),
				Payload:       event.Payload,
				Value:         eventValue,
			})

			err = s.protocol.SaveEvent(height, executionData.BlockID, chunk.Collection.ID(), event)
			if err != nil {
				return err
			}
		}

		for _, transactionResult := range chunk.TransactionResults {
			err := s.protocol.SaveTransactionResult(executionData.BlockID, chunk.Collection.ID(), transactionResult)
			if err != nil {
				return err
			}
		}

		for _, payload := range chunk.TrieUpdate.Payloads {
			if payload == nil {
				continue
			}

			err = s.ledger.SavePayload(payload, height)
			if err != nil {
				s.logger.Log().Err(err).Msg("error saving payload")
				return err
			}

		}

	}
	return nil
}
