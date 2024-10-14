package storage

import (
	"encoding/hex"
	"fmt"
	"github.com/bluesign/tinyAN/indexer"
	"github.com/cockroachdb/pebble"
	"github.com/onflow/flow-go/ledger"
	"github.com/onflow/flow-go/ledger/common/convert"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/module/executiondatasync/execution_data"
	"github.com/rs/zerolog"
	"os"
	"reflect"
)

const (
	codeAction_delete           byte = 0x01
	codeAction_new              byte = 0x02
	codeAction_update           byte = 0x03
	codeAction_location_by_uuid byte = 0x04
	codeAction_location_by_type byte = 0x05
	codeAction_index            byte = 0x06
	codeAction_index_reverse    byte = 0x07
)

type IndexStorage struct {
	logger      zerolog.Logger
	startHeight uint64
	ledger      *LedgerStorage
	indexDb     *pebble.DB
	codec       *Codec
}

func NewIndexStorage(spork string, startHeight uint64, ledger *LedgerStorage) (*IndexStorage, error) {
	indexDb := MustOpenPebbleDB(fmt.Sprintf("db/Index_%s", spork))

	return &IndexStorage{
		startHeight: startHeight,
		ledger:      ledger,
		indexDb:     indexDb,
		codec:       NewCodec(),
		logger:      zerolog.New(os.Stdout).With().Timestamp().Logger(),
	}, nil
}

func (s *IndexStorage) StartHeight() uint64 {
	return s.startHeight
}

func (s *IndexStorage) NewBatch() *pebble.Batch {
	return s.indexDb.NewBatch()
}

func (s *IndexStorage) Close() {
	err := s.indexDb.Close()
	if err != nil {
		s.logger.Log().Err(err).Msg("error closing database")
	}
}

func (s *IndexStorage) IndexCheckpoint(batch *pebble.Batch, payload *ledger.Payload, height uint64) error {
	blockResources := make(map[uint64]*indexer.Resource)
	s.IndexPayload(blockResources, payload, height, true)
	s.CommitIndex(batch, height, blockResources, false)
	return nil
}

func (s *IndexStorage) SetIndex(batch *pebble.Batch, action byte, address string, uuid uint64, cadenceType string, height uint64, id uint64, balance uint64) error {

	addressBytes := flow.HexToAddress(address).Bytes()

	recHeight := uint64(0xFFFFFFFFFFFFFFFF - height)
	key := makePrefix(codeAction_index, []byte(addressBytes), recHeight, []byte(cadenceType), uuid)
	value := makePrefix(action, id, balance)
	batch.Set(key, value, pebble.Sync)

	key = makePrefix(codeAction_index_reverse, []byte(cadenceType), []byte(addressBytes), recHeight, uuid)
	value = makePrefix(action, id, balance)
	batch.Set(key, value, pebble.Sync)
	return nil
}

func (s *IndexStorage) SetMaps(batch *pebble.Batch, address string, uuid uint64, cadenceType string, height uint64) error {
	addressBytes := flow.HexToAddress(address).Bytes()

	recHeight := uint64(0xFFFFFFFFFFFFFFFF - height)
	key := makePrefix(codeAction_location_by_uuid, uuid, recHeight)
	value := makePrefix(0x1, []byte(addressBytes))
	batch.Set(key, value, pebble.Sync)

	key = makePrefix(codeAction_location_by_type, []byte(cadenceType), recHeight, uuid)
	batch.Set(key, value, pebble.Sync)

	return nil
}

func (s *IndexStorage) CommitIndex(batch *pebble.Batch, height uint64, blockResources map[uint64]*indexer.Resource, verbose bool) {
	//index chunk resources
	for _, r := range blockResources {
		var id uint64 = 0
		var balance uint64 = 0

		idField, hasId := r.Fields["id"]
		if hasId {
			id = idField.NewValue.(uint64)
		}

		balanceField, hasBalance := r.Fields["balance"]
		if hasBalance {
			balance = balanceField.NewValue.(uint64)
		}

		//Move
		if r.Fields["address"].HasUpdated {
			//fmt.Println("MOVE:\t", r.Fields["address"].OldValue, r.Fields["address"].NewValue, r.Uuid, r.TypeName, height, id, balance)
			s.SetIndex(batch, codeAction_delete, r.Fields["address"].OldValue.(string), r.Uuid, r.TypeName, height, id, balance)

			s.SetIndex(batch, codeAction_new, r.Fields["address"].NewValue.(string), r.Uuid, r.TypeName, height, id, balance)
			s.SetMaps(batch, r.Fields["address"].NewValue.(string), r.Uuid, r.TypeName, height)
		}

		//Update
		updatedFields := []string{}

		for _, g := range r.Fields {
			if g.HasUpdated && g.Name != "address" {
				updatedFields = append(updatedFields, g.Name)
			}
		}

		if len(updatedFields) > 0 {
			if verbose {
				//fmt.Println("UPDATE:\t", r.Fields["address"].NewValue, updatedFields, r.Uuid, r.TypeName, height, id, balance)
			}
			s.SetIndex(batch, codeAction_update, r.Fields["address"].NewValue.(string), r.Uuid, r.TypeName, height, id, balance)
			s.SetMaps(batch, r.Fields["address"].NewValue.(string), r.Uuid, r.TypeName, height)
		} else {
			if verbose {
				//fmt.Println("NEW:\t", r.Fields["address"].NewValue, updatedFields, r.Uuid, r.TypeName, height, id, balance)
			}
			s.SetIndex(batch, codeAction_new, r.Fields["address"].NewValue.(string), r.Uuid, r.TypeName, height, id, balance)
			s.SetMaps(batch, r.Fields["address"].NewValue.(string), r.Uuid, r.TypeName, height)
		}

	}

}

func (s *IndexStorage) IndexPayload2(chunkResources map[uint64]*indexer.Resource, payload flow.RegisterEntry, height uint64, isCheckpoint bool) error {
	register := payload.Key
	address := register.Owner
	sAddress := hex.EncodeToString([]byte(address))

	if hex.EncodeToString([]byte(address)) == "f919ee77447b7497" {
		//skip fees
		return nil
	}
	value := payload.Value

	if !register.IsSlabIndex() && len(value) > 0 {
		//skip non slab stuff
		return nil
	}

	var oldResources map[uint64]map[string]any = make(map[uint64]map[string]any)

	if !isCheckpoint {
		oldData := s.ledger.GetRegister(register, height-1)
		oldResources = indexer.ParseLedgerData(sAddress, oldData)
	}
	newResources := indexer.ParseLedgerData(sAddress, value)

	for k, v := range newResources {

		oldFields := oldResources[k]
		newFields := v

		if len(oldFields) == 0 && len(newFields) == 0 {
			fmt.Println("no fields")
			continue
		}

		if reflect.DeepEqual(oldFields, newFields) {
			// no change
			//fmt.Println("no change")
			continue
		}

		oldType, hasTypeOld := oldFields["type"]

		newType, hasTypeNew := newFields["type"]

		var typeName string

		if hasTypeOld {
			typeName = oldType.(string)
		} else if hasTypeNew {
			typeName = newType.(string)
		} else {
			fmt.Println("no type")
			continue
		}

		uuid, ok := oldFields["uuid"].(uint64)
		if !ok {
			uuid = newFields["uuid"].(uint64)
		}

		r, exists := chunkResources[uuid]
		if !exists {
			fields := make(map[string]*indexer.ResourceField)
			r = &indexer.Resource{Uuid: uuid, TypeName: typeName, Fields: fields}
			chunkResources[uuid] = r
		}

		for k, v := range oldFields {
			if k == "type" {
				continue
			}
			field := &indexer.ResourceField{Name: k, OldValue: v, NewValue: v, HasUpdated: false}
			r.Fields[k] = field
		}

		for k, v := range newFields {
			if k == "type" {
				continue
			}
			_, ok := r.Fields[k]
			if !ok {
				field := &indexer.ResourceField{Name: k, NewValue: v, IsNew: true, HasUpdated: false}
				r.Fields[k] = field
			} else {
				if k == "address" {
					r.SetField(k, v.(string))
				} else {
					r.SetField(k, v.(uint64))
				}
			}
		}
	}
	return nil
}

func (s *IndexStorage) IndexPayload(chunkResources map[uint64]*indexer.Resource, payload *ledger.Payload, height uint64, isCheckpoint bool) error {
	key, err := payload.Key()
	if err != nil {
		return nil
	}
	register, _ := convert.LedgerKeyToRegisterID(key)
	address := register.Owner
	sAddress := hex.EncodeToString([]byte(address))

	if hex.EncodeToString([]byte(address)) == "f919ee77447b7497" {
		//skip fees
		return nil
	}
	value := payload.Value()

	if !register.IsSlabIndex() && len(value) > 0 {
		//skip non slab stuff
		return nil
	}

	var oldResources map[uint64]map[string]any = make(map[uint64]map[string]any)

	if !isCheckpoint {
		oldData := s.ledger.GetRegister(register, height-1)
		oldResources = indexer.ParseLedgerData(sAddress, oldData)
	}
	newResources := indexer.ParseLedgerData(sAddress, value)

	//fmt.Println("old", oldResources)
	//fmt.Println("new", newResources)

	for k, v := range newResources {

		oldFields := oldResources[k]
		newFields := v

		if len(oldFields) == 0 && len(newFields) == 0 {
			fmt.Println("no fields")
			continue
		}

		if reflect.DeepEqual(oldFields, newFields) {
			// no change
			//fmt.Println("no change")
			continue
		}

		oldType, hasTypeOld := oldFields["type"]

		newType, hasTypeNew := newFields["type"]

		var typeName string

		if hasTypeOld {
			typeName = oldType.(string)
		} else if hasTypeNew {
			typeName = newType.(string)
		} else {
			fmt.Println("no type")
			continue
		}

		uuid, ok := oldFields["uuid"].(uint64)
		if !ok {
			uuid = newFields["uuid"].(uint64)
		}

		r, exists := chunkResources[uuid]
		if !exists {
			fields := make(map[string]*indexer.ResourceField)
			r = &indexer.Resource{Uuid: uuid, TypeName: typeName, Fields: fields}
			chunkResources[uuid] = r
		}

		for k, v := range oldFields {
			if k == "type" {
				continue
			}
			field := &indexer.ResourceField{Name: k, OldValue: v, NewValue: v, HasUpdated: false}
			r.Fields[k] = field
		}

		for k, v := range newFields {
			if k == "type" {
				continue
			}
			_, ok := r.Fields[k]
			if !ok {
				field := &indexer.ResourceField{Name: k, NewValue: v, IsNew: true, HasUpdated: false}
				r.Fields[k] = field
			} else {
				if k == "address" {
					r.SetField(k, v.(string))
				} else {
					r.SetField(k, v.(uint64))
				}
			}
		}
	}
	return nil
}

func (s *IndexStorage) IndexLedger(height uint64, executionData *execution_data.BlockExecutionData) error {

	batch := s.NewBatch()
	defer batch.Commit(pebble.Sync)

	blockResources := make(map[uint64]*indexer.Resource)
	for _, chunk := range executionData.ChunkExecutionDatas {

		for _, payload := range chunk.TrieUpdate.Payloads {
			if payload == nil {
				continue
			}

			err := s.IndexPayload(blockResources, payload, height, false)
			if err != nil {
				return err
			}
		}
	}

	s.CommitIndex(batch, height, blockResources, true)

	return nil
}
