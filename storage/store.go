package storage

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/onflow/cadence"
	"github.com/onflow/cadence/encoding/ccf"
	jsoncdc "github.com/onflow/cadence/encoding/json"
	"github.com/onflow/flow-evm-gateway/models"
	sdk "github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go/access"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/module/executiondatasync/execution_data"
	flowStorage "github.com/onflow/flow-go/storage"
	"github.com/rs/zerolog"
	"sync"
	"time"
)

type FVMStorageSnapshot interface {
	Get(id flow.RegisterID) ([]byte, error)
}

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

func (s *HeightBasedStorage) Sporks() []*SporkStorage {
	return s.sporks
}
func (s *HeightBasedStorage) GetBlockByHeight(height uint64) (*flow.Header, error) {
	storage := s.StorageForHeight(height)
	return storage.Protocol().GetBlockByHeight(height)
}

func (s *HeightBasedStorage) GetLatestBlock() (*flow.Header, error) {
	storage := s.Latest()
	return storage.Protocol().GetBlockByHeight(storage.LastBlocksHeight())
}

func (s *HeightBasedStorage) LedgerSnapshot(height uint64) FVMStorageSnapshot {
	storage := s.StorageForHeight(height)
	return storage.ledger.StorageSnapshot(height)
}

func (s *HeightBasedStorage) ByHeightFrom(height uint64, header *flow.Header) (*flow.Header, error) {
	if header.Height == height {
		return header, nil
	}

	if header.Height > height {
		return nil, flowStorage.ErrNotFound
	}

	storage := s.StorageForHeight(height)
	block, err := storage.Protocol().GetBlockByHeight(height)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (s *HeightBasedStorage) GetBlockById(id flow.Identifier) (*flow.Header, error) {
	for _, storage := range s.sporks {
		block, err := storage.Protocol().GetBlockById(id)
		if err == nil {
			return block, nil
		}
	}
	return nil, fmt.Errorf("block not found")
}

func (s *HeightBasedStorage) GetCollectionByID(collectionID flow.Identifier) (*flow.Collection, error) {
	for _, storage := range s.sporks {
		collection, err := storage.Protocol().GetCollectionById(collectionID)
		if err == nil {
			return collection, nil
		}
	}
	return nil, fmt.Errorf("collection not found")
}

func (s *HeightBasedStorage) GetTransactionById(transactionID flow.Identifier) (*flow.TransactionBody, error) {
	for _, storage := range s.sporks {
		transaction, err := storage.Protocol().TransactionById(transactionID)
		if err == nil {
			return &transaction, nil
		}
	}
	return nil, fmt.Errorf("transaction not found")
}

func (s *HeightBasedStorage) EventsByName(height uint64, id flow.Identifier, name string) []flow.Event {
	storage := s.StorageForHeight(height)
	return storage.Protocol().EventsByName(id, name)
}

func (s *HeightBasedStorage) GetTransactionResult(transactionID flow.Identifier) (*access.TransactionResult, error) {
	for _, storage := range s.sporks {
		blockId, height, collectionId, result, err := storage.Protocol().TransactionResultById(transactionID)
		if err == nil {
			errorMessage := ""
			statusCode := uint(0)
			if result.Failed {
				statusCode = 1
				errorMessage = "Transaction failed"
			}
			events := storage.Protocol().Events(blockId, collectionId, transactionID)
			for _, event := range events {
				events = append(events, event)
			}
			result := &access.TransactionResult{
				Status:        flow.TransactionStatusSealed,
				StatusCode:    statusCode,
				Events:        events,
				ErrorMessage:  errorMessage,
				BlockID:       blockId,
				TransactionID: result.TransactionID,
				CollectionID:  collectionId,
				BlockHeight:   height,
			}

			return result, nil
		}
	}
	return nil, fmt.Errorf("transaction result not found")
}

func (s *HeightBasedStorage) Sync() {
	for _, spork := range s.sporks {
		//TODO: remove me
		spork.ledger.MarkBootstrapComplete()
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

func (s *HeightBasedStorage) StorageForEVMHeight(height uint64) *SporkStorage {
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
	logger         zerolog.Logger
	startHeight    uint64
	evmStartHeight uint64
	endHeight      uint64
	name           string
	accessURL      string
	protocol       *ProtocolStorage
	ledger         *LedgerStorage
	index          *IndexStorage
	evm            *EVMStorage

	progress []ProgressTracker
}

func NewSporkStorage(spork string, accessURL string, startHeight uint64, endHeight uint64, evmStartHeight uint64) *SporkStorage {
	protocol, _ := NewProtocolStorage(spork, startHeight)
	ledger, _ := NewLedgerStorage(spork, startHeight)
	index, _ := NewIndexStorage(spork, startHeight, ledger)
	evm, _ := NewEVMStorage(spork, startHeight)

	return &SporkStorage{
		startHeight:    startHeight,
		evmStartHeight: evmStartHeight,
		endHeight:      endHeight,
		name:           spork,
		accessURL:      accessURL,
		protocol:       protocol,
		ledger:         ledger,
		index:          index,
		evm:            evm,
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
	s.index.Close()
	s.evm.Close()
}

func (s *SporkStorage) StartHeight() uint64 {
	return s.startHeight
}
func (s *SporkStorage) EVMStartHeight() uint64 {
	return s.evmStartHeight
}

func (s *SporkStorage) EndHeight() uint64 {
	return s.endHeight
}

func (s *SporkStorage) LastBlocksHeight() uint64 {
	return s.protocol.LastHeight()
}

func (s *SporkStorage) Bootstrap() {

	if s.ledger.IsBootstrapComplete() {
		return
	}

	var wg sync.WaitGroup
	ch := make(chan int)

	for w := 1; w <= 8; w++ {
		go s.ledger.importWorker(s.index, s.name, s.startHeight, ch, &wg)
	}

	for i := 0; i < 16; i++ {
		wg.Add(1)
		ch <- i
	}

	wg.Wait()
	close(ch)

	s.ledger.MarkBootstrapComplete()
	s.logger.Log().Msg("bootstrap complete")
}

func (s *SporkStorage) ProcessExecutionData(batch *pebble.Batch, height uint64, executionData *execution_data.BlockExecutionData) error {

	blockEvents := sdk.BlockEvents{
		BlockID:        sdk.Identifier(executionData.BlockID),
		Height:         height,
		BlockTimestamp: time.Now(), //executionData.BlockTimestamp,
		Events:         []sdk.Event{},
	}

	for index, chunk := range executionData.ChunkExecutionDatas {

		err := s.protocol.SaveCollection(batch, executionData.BlockID, uint32(index), chunk.Collection)
		if err != nil {
			return err
		}

		for _, transaction := range chunk.Collection.Transactions {
			err := s.protocol.SaveTransaction(batch, executionData.BlockID, chunk.Collection.ID(), transaction)
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

			err = s.protocol.SaveEvent(batch, height, executionData.BlockID, chunk.Collection.ID(), event)
			if err != nil {
				return err
			}
		}

		for _, transactionResult := range chunk.TransactionResults {
			err := s.protocol.SaveTransactionResult(batch, executionData.BlockID, chunk.Collection.ID(), transactionResult)
			if err != nil {
				return err
			}
		}

		ledger_batch := s.ledger.NewBatch()
		for _, payload := range chunk.TrieUpdate.Payloads {
			if payload == nil {
				continue
			}

			err = s.ledger.SavePayload(batch, payload, height)
			if err != nil {
				s.logger.Log().Err(err).Msg("error saving payload")
				return err
			}

		}
		s.ledger.SaveProgress(ledger_batch, height)
		ledger_batch.Commit(pebble.Sync)

	}

	evmBatch := s.evm.NewBatch()
	cadenceEvents, err := models.NewCadenceEvents(blockEvents)
	if err != nil {
		panic(err)
	}
	err = s.evm.SaveBlock(evmBatch, cadenceEvents)
	if err != nil {
		s.logger.Log().Err(err).Msg("error saving evm block")
	}
	err = evmBatch.Commit(pebble.Sync)
	if err != nil {
		s.logger.Log().Err(err).Msg("error committing evm batch")
	}

	s.protocol.SaveProgress(batch, height)
	return nil
}
