package storage

import (
	"bytes"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/onflow/flow-go/fvm/storage/snapshot"
	"github.com/onflow/flow-go/ledger"
	"github.com/onflow/flow-go/ledger/common/convert"
	"github.com/onflow/flow-go/model/flow"
	"github.com/rs/zerolog"
	"os"
	"strings"
	"sync"
)

const (
	codeLedgerPayload byte = 0x01
)

type LedgerStorage struct {
	logger       zerolog.Logger
	startHeight  uint64
	databases    []*pebble.DB
	ledgerDb     *pebble.DB
	checkpointDb *pebble.DB
	codec        *Codec
}

func NewLedgerStorage(spork string, startHeight uint64) (*LedgerStorage, error) {
	checkpointDb := MustOpenPebbleDB(fmt.Sprintf("db/Checkpoint_%s", spork))
	ledgerDb := MustOpenPebbleDB(fmt.Sprintf("db/Ledger_%s", spork))

	return &LedgerStorage{
		startHeight:  startHeight,
		databases:    []*pebble.DB{ledgerDb, checkpointDb},
		ledgerDb:     ledgerDb,
		checkpointDb: checkpointDb,
		codec:        NewCodec(),
		logger:       zerolog.New(os.Stdout).With().Timestamp().Logger(),
	}, nil
}

func (s *LedgerStorage) MarkBootstrapComplete() {
	s.checkpointDb.Set(b("bootstrap"), b("done"), pebble.Sync)
}

func (s *LedgerStorage) IsBootstrapComplete() bool {
	v, closer, err := s.checkpointDb.Get(b("bootstrap"))
	if err != nil {
		return false
	}
	defer closer.Close()
	if string(v) == "done" {
		return true
	}
	return false
}

func (s *LedgerStorage) SaveProgress(batch *pebble.Batch, height uint64) error {
	return s.codec.MarshalAndSet(batch, keyProgress, height)
}

func (s *LedgerStorage) LastProcessedHeight() uint64 {
	var height uint64
	err := s.codec.UnmarshalAndGet(s.ledgerDb, keyProgress, &height)
	fmt.Println("LastProcessedHeight", height)
	if err != nil {
		fmt.Println(err)
		return 0
	}
	return height
}

func (s *LedgerStorage) StartHeight() uint64 {
	return s.startHeight
}

func (s *LedgerStorage) NewBatch() *pebble.Batch {
	return s.ledgerDb.NewBatch()
}

func (s *LedgerStorage) NewCheckpointBatch() *pebble.Batch {
	return s.checkpointDb.NewBatch()
}

func (s *LedgerStorage) Close() {
	for _, db := range s.databases {
		err := db.Close()
		if err != nil {
			s.logger.Log().Err(err).Msg("error closing database")
		}
	}
}

func (s *LedgerStorage) SavePayload(batch *pebble.Batch, payload *ledger.Payload, height uint64) error {
	key, err := payload.Key()
	if err != nil {
		return fmt.Errorf("error getting payload key: %w", err)
	}

	if err := s.codec.MarshalAndSet(batch,
		makePrefix(codeLedgerPayload, key.CanonicalForm(), uint64(0xFFFFFFFFFFFFFFFF-height)),
		payload.Value(),
	); err != nil {
		s.logger.Log().Err(err).Msg("error saving payload")
		return err

	}

	return nil
}

func (s *LedgerStorage) GetRegister(register flow.RegisterID, height uint64) ledger.Value {
	key := convert.RegisterIDToLedgerKey(register)

	prefix := makePrefix(codeLedgerPayload, key.CanonicalForm())
	preFixHeight := makePrefix(codeLedgerPayload, key.CanonicalForm(), uint64(0xFFFFFFFFFFFFFFFF-height))

	//fmt.Println("Key", key.CanonicalForm())
	options := &pebble.IterOptions{}
	var v []byte
	var k []byte

	debug := false
	if strings.Contains(string(key.CanonicalForm()), "AccountsStorageIDKey") {
		fmt.Println("Key", key.CanonicalForm())
		debug = true
	}

	for _, db := range s.databases {
		if debug {
			fmt.Println("DB", db)
		}
		iter, _ := db.NewIter(options)

		for iter.SeekGE(preFixHeight); iter.Valid(); iter.Next() {
			k = iter.Key()
			v = iter.Value()

			if debug {
				fmt.Println("Key", k)
			}
			if !bytes.HasPrefix(k, prefix) {
				break
			}

			if len(k)-len(prefix) > 8 {
				continue
			}
			//found the key
			err := iter.Close()
			if err != nil {
				s.logger.Log().Err(err).Msg("error closing iterator")
				return nil
			}
			var data []byte
			err = s.codec.Unmarshal(v, &data)
			if err != nil {
				s.logger.Log().Err(err).Msg("error unmarshalling data")
				return nil
			}
			return data
		}
		err := iter.Close()
		if err != nil {
			s.logger.Log().Err(err).Msg("error closing iterator")
			return nil
		}
	}
	//fmt.Println("GetRegister Failed", register, height)

	return nil
}

func (s *LedgerStorage) GetRegisterFunc(
	height uint64,
) func(flow.RegisterID) (flow.RegisterValue, error) {
	return func(regID flow.RegisterID) (flow.RegisterValue, error) {
		value := s.GetRegister(regID, height)
		return value, nil
	}
}

func (s *LedgerStorage) StorageSnapshot(height uint64) snapshot.StorageSnapshot {
	return snapshot.NewReadFuncStorageSnapshot(
		s.GetRegisterFunc(height))
}

func (s *LedgerStorage) importWorker(index *IndexStorage, spork string, sporkHeight uint64, ch chan int, wg *sync.WaitGroup) {
	for {
		select {
		case part, ok := <-ch:
			if !ok {
				break
			}
			importCheckpointSlabs(s, index, spork, sporkHeight, part)
			wg.Done()
		}
	}
}
