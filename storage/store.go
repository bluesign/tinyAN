package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/fxamacker/cbor/v2"
	"github.com/onflow/atree"
	"github.com/onflow/cadence"
	"github.com/onflow/cadence/encoding/ccf"
	"github.com/onflow/cadence/runtime/common"
	"github.com/onflow/cadence/runtime/interpreter"
	"github.com/onflow/flow-archive/codec/zbor"
	"github.com/onflow/flow-evm-gateway/models"
	sdk "github.com/onflow/flow-go-sdk"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"
	//"github.com/onflow/flow-go/cmd/util/ledger/util/registers"
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
	codeBlockHeader             byte = 0xb
	codeLastBlockHeaderHeight   byte = 0xc

	codeEVMTransaction   byte = 0xd
	codeEVMBlock         byte = 0xe
	codeEVMBlockByHeight byte = 0xf
	codeEVMBlockRaw      byte = 0x10

	codeLedgerPayload byte = 0xf0

	codeIndex_uuid_type          byte = 0xc0
	codeIndex_owner_uuid         byte = 0xc1
	codeIndex_type_uuid          byte = 0xc2
	codeIndex_owner_type         byte = 0xc3
	codeIndex_extra_type         byte = 0xc4
	codeIndex_extra_uuid         byte = 0xc5
	codeIndex_extra_address_type byte = 0xc6

	codeAction_new    byte = 0xd1
	codeAction_update byte = 0xd2
	codeAction_delete byte = 0xd3

	codeAction_index            byte = 0xe0
	codeAction_index_reverse    byte = 0xe1
	codeAction_location_by_uuid byte = 0xe2
	codeAction_location_by_type byte = 0xe3
)

type ProtocolStorage struct {
	protocolDb   *pebble.DB
	checkpointDb *pebble.DB
	ledgerDb     *pebble.DB
	indexDb      *pebble.DB

	evmDb    *pebble.DB
	evmRawDb *pebble.DB
	blocksDb *pebble.DB

	codec       *zbor.Codec
	ledgerBatch *pebble.Batch
	batch       *pebble.Batch
}

func (p *ProtocolStorage) GetDB(code byte) *pebble.DB {

	switch code {
	case 'i':
		return p.indexDb
	case 'l':
		return p.ledgerDb
	case 'c':
		return p.checkpointDb
	case 'e':
		return p.evmDb
	case 'r':
		return p.evmRawDb
	case 'b':
		return p.blocksDb
	default:
		return nil
	}

}

func NewProtocolStorage(bootstrap bool) (*ProtocolStorage, error) {

	opts := &pebble.Options{
		FormatMajorVersion: pebble.FormatNewest,

		// Soft and hard limits on read amplificaction of L0 respectfully.
		L0CompactionThreshold: 2,
		L0StopWritesThreshold: 1000,

		// When the maximum number of bytes for a level is exceeded, compaction is requested.
		LBaseMaxBytes: 64 << 20, // 64 MB
		Levels:        make([]pebble.LevelOptions, 7),
		MaxOpenFiles:  16384,

		// Writes are stopped when the sum of the queued memtable sizes exceeds MemTableStopWritesThreshold*MemTableSize.
		MemTableSize:                64 << 20 << 5,
		MemTableStopWritesThreshold: 4,

		// The default is 1.
		MaxConcurrentCompactions: func() int { return 4 },
	}

	checkpointDb, err := pebble.Open("tinyCheckpoint", opts)
	if err != nil {
		return nil, err
	}
	protocolDb, err := pebble.Open("tinyProto", &pebble.Options{})
	if err != nil {
		return nil, err
	}

	ledgerDb, err := pebble.Open("tinyLedger", &pebble.Options{})
	if err != nil {
		return nil, err
	}

	indexDb, err := pebble.Open("tinyIndex", &pebble.Options{})
	if err != nil {
		return nil, err
	}

	evmDb, err := pebble.Open("tinyEVM", &pebble.Options{})
	if err != nil {
		return nil, err
	}

	evmRawDb, err := pebble.Open("tinyEVMRaw", &pebble.Options{})
	if err != nil {
		return nil, err
	}

	blocksDb, err := pebble.Open("tinyBlocks", &pebble.Options{})
	if err != nil {
		return nil, err
	}

	return &ProtocolStorage{
		protocolDb:   protocolDb,
		checkpointDb: checkpointDb,
		ledgerDb:     ledgerDb,
		indexDb:      indexDb,
		evmDb:        evmDb,
		evmRawDb:     evmRawDb,
		blocksDb:     blocksDb,
		codec:        zbor.NewCodec(),
	}, nil

}

func (s *ProtocolStorage) Bootstrap(spork string, sporkHeight uint64) {
	var wg sync.WaitGroup
	ch := make(chan int)

	for w := 1; w <= 4; w++ {
		go importWorker(s.checkpointDb, spork, sporkHeight, ch, &wg, s)
	}

	for i := 0; i < 16; i++ {
		wg.Add(1)
		ch <- i
	}

	wg.Wait()
	close(ch)

}

type slabType int

const (
	slabTypeUndefined slabType = iota
	slabArray
	slabMap
	slabStorable
)

func getSlabType(h []byte) slabType {
	if len(h) < 2 {
		return slabTypeUndefined
	}
	f := h[1]
	// Extract 4th and 5th bits for slab type.
	dataType := (f & byte(0b000_11000)) >> 3
	switch dataType {
	case 0:
		// 4th and 5th bits are 0.
		return slabArray
	case 1:
		// 4th bit is 0 and 5th bit is 1.
		return slabMap
	case 3:
		// 4th and 5th bit are 1.
		return slabStorable
	default:
		return slabTypeUndefined
	}
}

type slabArrayType int

const (
	slabArrayUndefined slabArrayType = iota
	slabArrayData
	slabArrayMeta
	slabLargeImmutableArray
	slabBasicArray
)

type slabMapType int

const (
	slabMapUndefined slabMapType = iota
	slabMapData
	slabMapMeta
	slabMapLargeEntry
	slabMapCollisionGroup
)

func getSlabArrayType(h []byte) slabArrayType {

	f := h[1]

	// Extract 3 low bits for slab array type.
	dataType := (f & byte(0b000_00111))
	switch dataType {
	case 0:
		return slabArrayData
	case 1:
		return slabArrayMeta
	case 2:
		return slabLargeImmutableArray
	case 3:
		return slabBasicArray
	default:
		return slabArrayUndefined
	}
}

func getSlabMapType(h []byte) slabMapType {
	f := h[1]

	// Extract 3 low bits for slab map type.
	dataType := (f & byte(0b000_00111))
	switch dataType {
	case 0:
		return slabMapData
	case 1:
		return slabMapMeta
	case 2:
		return slabMapLargeEntry
	case 3:
		return slabMapCollisionGroup
	default:
		return slabMapUndefined
	}
}

func decodeStorable(dec *cbor.StreamDecoder, sid atree.SlabID, inlinedExtraData []atree.ExtraData) (atree.Storable, error) {
	return interpreter.DecodeStorable(dec, sid, inlinedExtraData, nil)
}

func decodeTypeInfo(decoder *cbor.StreamDecoder) (atree.TypeInfo, error) {
	decodedType, err := interpreter.DecodeTypeInfo(decoder, nil)
	return decodedType, err
}

func (s *ProtocolStorage) ForEachPayloadsByAccount(address []byte, f func(owner string, key string, value []byte) error, commit bool, max uint64, cache map[string][]byte) int {
	exists := make(map[string]bool, 0)

	var flowAddress flow.Address
	copy(flowAddress[:], address)

	initial_prefix := makePrefix(codeLedgerPayload, []byte{0x2F, 0x30, 0x2F}, []byte(address), []byte{0x2F, 0x32, 0x2F})
	prefix := initial_prefix
	options := &pebble.IterOptions{}
	var v []byte
	var k []byte
	var c uint64 = 0
	iterIndex, _ := s.indexDb.NewIter(options)
	defer iterIndex.Close()

	for iterIndex.SeekGE(initial_prefix); iterIndex.Valid(); iterIndex.Next() {
		k = iterIndex.Key()
		v = iterIndex.Value()

		if !bytes.HasPrefix(k, initial_prefix) {
			break
		}

		exists[string(k[15:])] = true
		c = c + 1
		if c > max {
			return int(c)
		}

		if commit {
			if c%1000000 == 0 {
				fmt.Println(c/1000000, "M payloads")
			}
			lastKey := string(DeepCopy(k[15:]))
			if len(v) > 0 {
				cache[lastKey] = DeepCopy(v)
			}
		}
	}

	iter, _ := s.ledgerDb.NewIter(options)
	defer iter.Close()
	for {
		iter.SeekGE(prefix)
		if !iter.Valid() {
			break
		}

		k = DeepCopy(iter.Key())

		if !bytes.HasPrefix(k, initial_prefix) {
			break
		}

		_, ok := exists[string(k[15:len(k)-8])]
		if ok {
			prefix = makePrefix(codeLedgerPayload, []byte{0x2F, 0x30, 0x2F}, []byte(address), []byte{0x2F, 0x32, 0x2F}, k[15:len(k)-8], []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
			continue
		}

		exists[string(k[15:len(k)-8])] = true

		c = c + 1
		if c > max {
			return int(c)
		}

		if commit {
			if c%1000000 == 0 {
				fmt.Println(c/1000000, "M payloads")
			}
			lastKey := string(DeepCopy(k[15 : len(k)-8]))
			if len(iter.Value()) > 0 {
				cache[lastKey] = DeepCopy(iter.Value())
			}

		}

		prefix = makePrefix(codeLedgerPayload, []byte{0x2F, 0x30, 0x2F}, []byte(address), []byte{0x2F, 0x32, 0x2F}, k[15:len(k)-8], []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	}

	iterCheckpoint, _ := s.checkpointDb.NewIter(options)
	defer iterCheckpoint.Close()
	for iterCheckpoint.SeekGE(initial_prefix); iterCheckpoint.Valid(); iterCheckpoint.Next() {

		k = iterCheckpoint.Key()
		v = iterCheckpoint.Value()

		if !bytes.HasPrefix(k, initial_prefix) {
			break
		}

		_, ok := exists[string(k[15:len(k)-8])]
		if ok {
			continue
		}

		c = c + 1
		if c > max {
			return int(c)
		}

		if commit {
			if c%1000000 == 0 {
				fmt.Println(c/1000000, "M payloads")
			}

			lastKey := string(DeepCopy(k[15 : len(k)-8]))
			if len(v) > 0 {
				cache[lastKey] = DeepCopy(v) //[]byte(lookupKey)
			}
		}
	}

	return int(c)
}

func (s *ProtocolStorage) checkCompositeKeys(address string, v *atree.MapDataSlab) map[string]any {
	var result = make(map[string]any)

	if v.ExtraData() == nil {
		return result
	}

	decodedType := v.ExtraData().TypeInfo
	if !decodedType.IsComposite() {
		return result
	}

	for i := 0; i < int(v.Count()); i++ {
		el, _ := v.Element(i)
		key := reflect.Indirect(reflect.ValueOf(el)).Field(0)
		keyValue := reflect.NewAt(key.Type(), unsafe.Pointer(key.UnsafeAddr())).Elem().Interface().(atree.Storable)

		value := reflect.Indirect(reflect.ValueOf(el)).Field(1)
		valueValue := reflect.NewAt(value.Type(), unsafe.Pointer(value.UnsafeAddr())).Elem().Interface().(atree.Storable)

		storedKey, _ := keyValue.StoredValue(nil)
		if stringValue, ok := storedKey.(interpreter.StringAtreeValue); ok {
			switch string(stringValue) {
			case "id", "balance", "uuid":
				storedValue, _ := valueValue.StoredValue(nil)
				switch sv := storedValue.(type) {
				case interpreter.UFix64Value:
					result[string(stringValue)] = uint64(sv)
				case interpreter.UInt64Value:
					result[string(stringValue)] = uint64(sv)
				default:
					//fmt.Printf("%T\n", sv)
					//fmt.Println(storedValue)
					continue //panic("type")
				}
			}
		}
	}

	_, hasBalance := result["balance"]
	_, hasUuid := result["uuid"]
	if !hasUuid {
		if hasBalance {
			fmt.Println("no uuid", v)
		}
		return make(map[string]any)
	}

	reflectedType := reflect.ValueOf(decodedType)
	if reflectedType.Kind() == reflect.Ptr {
		reflectedType = reflectedType.Elem()
	}
	typeCopy := reflect.New(reflectedType.Type()).Elem()
	typeCopy.Set(reflectedType)

	//kind := reflect.NewAt(typeCopy.Field(2).Type(), unsafe.Pointer(typeCopy.Field(2).UnsafeAddr())).Elem().Interface().(common.CompositeKind)

	location, hasLocatioon := reflect.NewAt(typeCopy.Field(0).Type(), unsafe.Pointer(typeCopy.Field(0).UnsafeAddr())).Elem().Interface().(common.AddressLocation)
	if !hasLocatioon {
		return result
	}
	qualifiedIdentifier := reflect.NewAt(typeCopy.Field(1).Type(), unsafe.Pointer(typeCopy.Field(1).UnsafeAddr())).Elem().Interface().(string)
	result["type"] = fmt.Sprintf("A.%s.%s", location.Address.Hex(), qualifiedIdentifier)
	result["address"] = address

	return result

}
func (s *ProtocolStorage) processArrayDataSlab(address string, v *atree.ArrayDataSlab, result map[uint64]map[string]any) map[uint64]map[string]any {

	v.PopIterate(nil, func(value atree.Storable) {

		valueSlab, isMap := value.(*atree.MapDataSlab)
		if isMap {
			s.processMapDataSlab(address, valueSlab, result)
		}

		arraySlab, isArray := value.(*atree.ArrayDataSlab)
		if isArray {
			fmt.Println("Array in Array", arraySlab)
			s.processArrayDataSlab(address, arraySlab, result)
		}

	})

	return result

}

func (s *ProtocolStorage) processMapDataSlab(address string, v *atree.MapDataSlab, result map[uint64]map[string]any) map[uint64]map[string]any {

	compositeKeys := s.checkCompositeKeys(address, v)
	if len(compositeKeys) > 0 {
		result[compositeKeys["uuid"].(uint64)] = compositeKeys
	}

	for i := 0; i < int(v.Count()); i++ {
		el, _ := v.Element(i)
		//key := reflect.Indirect(reflect.ValueOf(el)).Field(0)
		//keyValue := reflect.NewAt(key.Type(), unsafe.Pointer(key.UnsafeAddr())).Elem().Interface().(atree.Storable)

		value := reflect.Indirect(reflect.ValueOf(el)).Field(1)
		valueValue := reflect.NewAt(value.Type(), unsafe.Pointer(value.UnsafeAddr())).Elem().Interface().(atree.Storable)

		valueSlab, isMap := valueValue.(*atree.MapDataSlab)
		if isMap {
			s.processMapDataSlab(address, valueSlab, result)
		}

	}

	return result

}

func (s *ProtocolStorage) ParseLedgerData(address string, value []byte) map[uint64]map[string]any {

	result := make(map[uint64]map[string]any, 0)

	if !strings.Contains(string(value), "uuid") {

		if strings.Contains(string(value), "balance") {

		}
		return result
	}

	switch getSlabType(value) {
	case slabArray:
		decMode, _ := cbor.DecOptions{}.DecMode()
		slab, err := atree.DecodeSlab(atree.SlabID{}, value, decMode, decodeStorable, decodeTypeInfo)

		switch v := slab.(type) {
		case *atree.ArrayDataSlab:
			s.processArrayDataSlab(address, v, result)

		default:

			fmt.Println("Unhandled")
			fmt.Println(s, err)
			panic("p1")
		}

	case slabMap:
		switch getSlabMapType(value) {
		case slabMapData:
			decMode, _ := cbor.DecOptions{}.DecMode()
			slab, err := atree.DecodeSlab(atree.SlabID{}, value, decMode, decodeStorable, decodeTypeInfo)

			switch v := slab.(type) {
			case *atree.MapDataSlab:
				s.processMapDataSlab(address, v, result)

			default:
				fmt.Println("Unhandled")
				fmt.Println(s, err)
				panic("p2")
			}

		default:
			panic("p4")
		}

	case slabStorable:
		fmt.Println("Storable")

	default:
		fmt.Println("Unhandled")
		panic("p3")
	}
	return result
}

type Resource struct {
	typeName string
	uuid     uint64
	fields   map[string]*ResourceField
}

type ResourceResult struct {
	Address  string
	TypeName string
	Uuid     uint64
	Id       uint64
	Balance  uint64
}

type ResourceField struct {
	name       string
	oldValue   any
	newValue   any
	hasUpdated bool
	isNew      bool
}

func (r *ResourceField) String() string {
	status := ""
	if r.isNew {
		if r.name == "balance" {
			return fmt.Sprintf("%s: %d.%08d %s (NEW)", r.name, r.newValue.(uint64)/100000000, r.newValue.(uint64)%100000000, status)
		}
		return fmt.Sprintf("%s: %v %s", r.name, r.newValue, status)
	}
	if r.hasUpdated {
		if r.name == "balance" {
			status = fmt.Sprintf("-> %d.%08d (updated)", r.newValue.(uint64)/100000000, r.newValue.(uint64)%100000000)
		} else {
			status = fmt.Sprintf("-> %v (updated)", r.newValue)
		}
	}
	if r.name == "balance" {
		return fmt.Sprintf("%s: %d.%08d %s", r.name, r.oldValue.(uint64)/100000000, r.oldValue.(uint64)%100000000, status)
	}
	return fmt.Sprintf("%s: %v %s", r.name, r.oldValue, status)
}

func (r *Resource) String() string {
	result := fmt.Sprintf("\n%s: (uuid=%d)\n", r.typeName, r.uuid)
	for _, field := range r.fields {
		if field.name == "uuid" {
			continue
		}
		result += fmt.Sprintf("\t%s\n", field)
	}
	return result
}

func (r *Resource) setField(name string, value any) {
	field := r.fields[name]
	field.newValue = value
	field.hasUpdated = (field.newValue != field.oldValue) && (field.oldValue != nil)
}

func (s *ProtocolStorage) IndexPayload2(chunkResources map[uint64]*Resource, payload flow.RegisterEntry, height uint64, isCheckpoint bool) error {
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
		oldData := s.GetRegister(register, height-1)
		oldResources = s.ParseLedgerData(sAddress, oldData)
	}
	newResources := s.ParseLedgerData(sAddress, value)

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
			fields := make(map[string]*ResourceField)
			r = &Resource{uuid: uuid, typeName: typeName, fields: fields}
			chunkResources[uuid] = r
		}

		for k, v := range oldFields {
			if k == "type" {
				continue
			}
			field := &ResourceField{name: k, oldValue: v, newValue: v, hasUpdated: false}
			r.fields[k] = field
		}

		for k, v := range newFields {
			if k == "type" {
				continue
			}
			_, ok := r.fields[k]
			if !ok {
				field := &ResourceField{name: k, newValue: v, isNew: true, hasUpdated: false}
				r.fields[k] = field
			} else {
				if k == "address" {
					r.setField(k, v.(string))
				} else {
					r.setField(k, v.(uint64))
				}
			}
		}
	}
	return nil
}

func (s *ProtocolStorage) IndexPayload(chunkResources map[uint64]*Resource, payload *ledger.Payload, height uint64, isCheckpoint bool) error {
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
		oldData := s.GetRegister(register, height-1)
		oldResources = s.ParseLedgerData(sAddress, oldData)
	}
	newResources := s.ParseLedgerData(sAddress, value)

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
			fields := make(map[string]*ResourceField)
			r = &Resource{uuid: uuid, typeName: typeName, fields: fields}
			chunkResources[uuid] = r
		}

		for k, v := range oldFields {
			if k == "type" {
				continue
			}
			field := &ResourceField{name: k, oldValue: v, newValue: v, hasUpdated: false}
			r.fields[k] = field
		}

		for k, v := range newFields {
			if k == "type" {
				continue
			}
			_, ok := r.fields[k]
			if !ok {
				field := &ResourceField{name: k, newValue: v, isNew: true, hasUpdated: false}
				r.fields[k] = field
			} else {
				if k == "address" {
					r.setField(k, v.(string))
				} else {
					r.setField(k, v.(uint64))
				}
			}
		}
	}
	return nil
}

func (s *ProtocolStorage) commitIndex(height uint64, blockResources map[uint64]*Resource, verbose bool) {
	//index chunk resources
	for _, r := range blockResources {
		var id uint64 = 0
		var balance uint64 = 0

		idField, hasId := r.fields["id"]
		if hasId {
			id = idField.newValue.(uint64)
		}

		balanceField, hasBalance := r.fields["balance"]
		if hasBalance {
			balance = balanceField.newValue.(uint64)
		}

		//Move
		if r.fields["address"].hasUpdated {
			fmt.Println("MOVE:\t", r.fields["address"].oldValue, r.fields["address"].newValue, r.uuid, r.typeName, height, id, balance)
			s.SetIndex(codeAction_delete, r.fields["address"].oldValue.(string), r.uuid, r.typeName, height, id, balance)

			s.SetIndex(codeAction_new, r.fields["address"].newValue.(string), r.uuid, r.typeName, height, id, balance)
			s.SetMaps(r.fields["address"].newValue.(string), r.uuid, r.typeName, height)
		}

		//Update
		updatedFields := []string{}

		for _, g := range r.fields {
			if g.hasUpdated && g.name != "address" {
				updatedFields = append(updatedFields, g.name)
			}
		}

		if len(updatedFields) > 0 {
			if verbose {
				fmt.Println("UPDATE:\t", r.fields["address"].newValue, updatedFields, r.uuid, r.typeName, height, id, balance)
			}
			s.SetIndex(codeAction_update, r.fields["address"].newValue.(string), r.uuid, r.typeName, height, id, balance)
			s.SetMaps(r.fields["address"].newValue.(string), r.uuid, r.typeName, height)
		} else {
			if verbose {
				fmt.Println("NEW:\t", r.fields["address"].newValue, updatedFields, r.uuid, r.typeName, height, id, balance)
			}
			s.SetIndex(codeAction_new, r.fields["address"].newValue.(string), r.uuid, r.typeName, height, id, balance)
			s.SetMaps(r.fields["address"].newValue.(string), r.uuid, r.typeName, height)
		}

	}

}

func (s *ProtocolStorage) IndexCheckpoint(height uint64, payload *ledger.Payload) error {
	blockResources := make(map[uint64]*Resource)
	s.IndexPayload(blockResources, payload, height, true)
	s.commitIndex(height, blockResources, false)
	return nil
}

func (s *ProtocolStorage) IndexLedger(height uint64, executionData *execution_data.BlockExecutionData) error {

	blockResources := make(map[uint64]*Resource)
	fmt.Println("=== block ===")
	for _, chunk := range executionData.ChunkExecutionDatas {
		fmt.Println("=== # chunk ===")

		for _, payload := range chunk.TrieUpdate.Payloads {
			if payload == nil {
				continue
			}

			s.IndexPayload(blockResources, payload, height, false)
		}
	}

	s.commitIndex(height, blockResources, true)

	return nil
}

type BalanceInfo struct {
	Address     string
	Balance     uint64
	Action      string
	CadenceType string
	Uuid        uint64
	Height      uint64
}

func (s *ProtocolStorage) OwnerOfUuid(uuid uint64, targetHeight uint64) string {
	reverseHeight := uint64(0xFFFFFFFFFFFFFFFF - targetHeight)
	prefix := makePrefix(codeAction_location_by_uuid, uuid)
	key := makePrefix(codeAction_location_by_uuid, uuid, reverseHeight)
	options := &pebble.IterOptions{}
	iterIndex, _ := s.indexDb.NewIter(options)
	defer iterIndex.Close()

	iterIndex.SeekGE(key)
	if !iterIndex.Valid() {
		fmt.Println("no valid")
		return "not found"
	}
	k := iterIndex.Key()
	v := iterIndex.Value()

	if !bytes.HasPrefix(k, prefix) {
		fmt.Println("no prefix")
		return "not found"
	}
	fmt.Println("v", v)
	address := v[1:]
	return hex.EncodeToString(address)

}

func (s *ProtocolStorage) BalanceHistoryByAddress(addressString string, targetType string, targetHeight uint64) []BalanceInfo {

	address := flow.HexToAddress(addressString).Bytes()

	fmt.Println("address", address)

	reverseHeight := uint64(0xFFFFFFFFFFFFFFFF - targetHeight)
	prefix := makePrefix(codeAction_index, []byte(address))
	currentPrefix := makePrefix(codeAction_index, []byte(address), reverseHeight)

	balances := make([]BalanceInfo, 0)

	options := &pebble.IterOptions{}
	var v []byte
	var k []byte

	iterIndex, _ := s.indexDb.NewIter(options)
	defer iterIndex.Close()

	iterIndex.SeekGE(currentPrefix)
	for {
		if !iterIndex.Valid() {
			break
		}
		if !bytes.HasPrefix(iterIndex.Key(), prefix) {
			break
		}
		k = iterIndex.Key()
		v = iterIndex.Value()

		height := binary.BigEndian.Uint64(k[9:17])
		cadenceType := string(k[17 : len(k)-8])
		uuid := binary.BigEndian.Uint64(k[len(k)-8:])
		balance := binary.BigEndian.Uint64(v[len(v)-8:])

		if targetType != cadenceType {
			iterIndex.Next()
			continue
		}

		if height < reverseHeight {
			//skip
			currentPrefix = makePrefix(codeAction_index, []byte(address), reverseHeight)
			iterIndex.SeekGE(currentPrefix)
			continue
		}

		action := "UPDATED"
		if v[0] == codeAction_new {
			action = "ADDED"
		}
		if v[0] == codeAction_delete {
			action = "REMOVED"
		}

		if v[0] == codeAction_delete {
			fmt.Println("delete address")
			//addressUint64 := binary.BigEndian.Uint64([]byte(address))
			//addressUint64 = addressUint64 + 1
			//currentPrefix = makePrefix(codeAction_index, addressUint64, reverseHeight)
			//iterIndex.SeekGE(currentPrefix)
		}

		balances = append(balances, BalanceInfo{
			Address:     hex.EncodeToString([]byte(address)),
			Action:      action,
			Balance:     balance,
			CadenceType: cadenceType,
			Uuid:        uuid,
			Height:      0xFFFFFFFFFFFFFFFF - height,
		})

		if len(balances) > 100 {
			break
		}
		iterIndex.Next()

	}

	return balances

}

func (s *ProtocolStorage) OwnersByType(cadenceType string, targetHeight uint64) []ResourceResult {
	reverseHeight := uint64(0xFFFFFFFFFFFFFFFF - targetHeight)
	prefix := makePrefix(codeAction_index_reverse, []byte(cadenceType))
	currentPrefix := prefix

	//type = address - height - uuid  // action - id - balance
	//type = address - uuid - height   // action - id - balance

	addresses := map[uint64]ResourceResult{}

	options := &pebble.IterOptions{}
	var v []byte
	var k []byte

	iterIndex, _ := s.indexDb.NewIter(options)
	defer iterIndex.Close()
	count := 0

	iterIndex.SeekGE(currentPrefix)
	for {
		fmt.Println("prefix", currentPrefix)
		if !iterIndex.Valid() {
			break
		}
		if !bytes.HasPrefix(iterIndex.Key(), prefix) {
			break
		}
		k = iterIndex.Key()
		v = iterIndex.Value()

		storedType := string(k[1 : len(k)-24])
		address := string(k[len(k)-24 : len(k)-16])
		height := binary.BigEndian.Uint64(k[len(k)-16 : len(k)-8])
		uuid := binary.BigEndian.Uint64(k[len(k)-8:])
		codeAction := v[0]

		id := binary.BigEndian.Uint64(v[1:9])
		balance := binary.BigEndian.Uint64(v[len(v)-8:])

		//encodedAddress := hex.EncodeToString([]byte(address))
		//fmt.Println("address", encodedAddress, "height", height, "uuid", uuid, "codeAction", codeAction)

		if height < reverseHeight {
			//skip
			fmt.Println("skip height", height, reverseHeight)
			currentPrefix = makePrefix(codeAction_index_reverse, []byte(cadenceType), []byte(address), reverseHeight)
			iterIndex.SeekGE(currentPrefix)
			continue
		}

		if codeAction == codeAction_delete {
			fmt.Println("delete address")
			fmt.Println("skip address")
			//addressUint64 := binary.BigEndian.Uint64([]byte(address))
			//addressUint64 = addressUint64 + 1
			//currentPrefix = makePrefix(codeAction_index_reverse, []byte(cadenceType), addressUint64, reverseHeight)
			//iterIndex.SeekGE(currentPrefix)
			iterIndex.Next()
			continue
		} else {
			saddress := hex.EncodeToString([]byte(address))
			fmt.Println("add address", saddress)

			if _, ok := addresses[uuid]; ok {
				fmt.Println("uuid exists")
				iterIndex.Next()
				continue
			}

			addresses[uuid] = ResourceResult{
				Address:  saddress,
				Uuid:     uuid,
				TypeName: storedType,
				Id:       id,
				Balance:  balance,
			}

			count = count + 1
			if count > 5000 {
				break
			}
		}

		iterIndex.Next()

	}

	addr := []ResourceResult{}
	for _, v := range addresses {
		addr = append(addr, v)

	}
	return addr

}

func (s *ProtocolStorage) SetIndex(action byte, address string, uuid uint64, cadenceType string, height uint64, id uint64, balance uint64) error {

	addressBytes := flow.HexToAddress(address).Bytes()

	recHeight := uint64(0xFFFFFFFFFFFFFFFF - height)
	key := makePrefix(codeAction_index, []byte(addressBytes), recHeight, []byte(cadenceType), uuid)
	value := makePrefix(action, id, balance)
	s.indexDb.Set(key, value, pebble.Sync)

	key = makePrefix(codeAction_index_reverse, []byte(cadenceType), []byte(addressBytes), recHeight, uuid)
	value = makePrefix(action, id, balance)
	//fmt.Println("key", key, "value", value)
	s.indexDb.Set(key, value, pebble.Sync)
	return nil
}

func (s *ProtocolStorage) SetMaps(address string, uuid uint64, cadenceType string, height uint64) error {
	addressBytes := flow.HexToAddress(address).Bytes()

	recHeight := uint64(0xFFFFFFFFFFFFFFFF - height)
	key := makePrefix(codeAction_location_by_uuid, uuid, recHeight)
	value := makePrefix(0x1, []byte(addressBytes))
	s.indexDb.Set(key, value, pebble.Sync)

	key = makePrefix(codeAction_location_by_type, []byte(cadenceType), recHeight, uuid)
	s.indexDb.Set(key, value, pebble.Sync)

	return nil
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

func (s *ProtocolStorage) InsertCollection(blockId flow.Identifier, index uint32, collection *flow.Collection) error {
	err := s.Set(makePrefix(codeCollection, collection.ID(), blockId), []byte{})
	if err != nil {
		return err
	}
	err = s.Set(makePrefix(codeCollectionAtBlock, blockId, index, collection.ID()), []byte{})
	if err != nil {
		return err
	}
	return nil
}

func (s *ProtocolStorage) InsertEvent(cadenceHeight uint64, blockId flow.Identifier, collectionId flow.Identifier, event flow.Event) error {
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

func (s *ProtocolStorage) LastBlockHeaderHeight() uint64 {
	height, err := s.Get(makePrefix(codeLastBlockHeaderHeight))
	if err != nil {
		return 0
	}
	return binary.BigEndian.Uint64(height)
}

func (s *ProtocolStorage) BlockId(height uint64) flow.Identifier {
	fmt.Println("block id", height)
	blockIdBytes, err := s.Get(makePrefix(codeHeight, height))
	if err != nil {
		return flow.ZeroID
	}
	blockId, err := flow.ByteSliceToId(blockIdBytes)

	if err != nil {
		return flow.ZeroID
	}
	fmt.Println(blockId)
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
		id, _ := flow.ByteSliceToId(iter.Key()[37:])
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

func DeepCopy(v []byte) []byte {
	newV := make([]byte, len(v))
	copy(newV, v)
	return newV
}

func (s *ProtocolStorage) GetKeyFromDB(db *pebble.DB, key []byte) ledger.Value {
	value, closer, err := db.Get(key)
	if err != nil {
		panic(err)
	}
	v := ledger.Value(DeepCopy(value))
	if err := closer.Close(); err != nil {
		panic(err)
	}
	return v
}

func (s *ProtocolStorage) GetRegister(register flow.RegisterID, height uint64) ledger.Value {

	key := convert.RegisterIDToLedgerKey(register)

	prefix := makePrefix(codeLedgerPayload, key.CanonicalForm())
	preFixHeight := makePrefix(codeLedgerPayload, key.CanonicalForm(), uint64(0xFFFFFFFFFFFFFFFF-height))

	options := &pebble.IterOptions{}
	var v []byte
	var k []byte

	iterIndex, _ := s.indexDb.NewIter(options)
	defer iterIndex.Close()

	for iterIndex.SeekGE(prefix); iterIndex.Valid(); iterIndex.Next() {
		k = iterIndex.Key()
		v = iterIndex.Value()

		if !bytes.HasPrefix(k, prefix) {
			break
		}

		v = iterIndex.Value()
		return DeepCopy(v)
	}

	iter, _ := s.ledgerDb.NewIter(options)
	defer iter.Close()

	for iter.SeekGE(preFixHeight); iter.Valid(); iter.Next() {
		k = iter.Key()
		v = iter.Value()

		if !bytes.HasPrefix(k, prefix) {
			break
		}

		if len(k)-len(prefix) > 8 {
			continue
		}
		return DeepCopy(v)
	}

	iterCheckpoint, _ := s.checkpointDb.NewIter(options)
	defer iterCheckpoint.Close()

	for iterCheckpoint.SeekGE(prefix); iterCheckpoint.Valid(); iterCheckpoint.Next() {
		k = iterCheckpoint.Key()
		v = iterCheckpoint.Value()

		if !bytes.HasPrefix(k, prefix) {
			break
		}

		if len(k)-len(prefix) > 8 {
			continue
		}
		return DeepCopy(v)
	}

	return nil
}

func (s *ProtocolStorage) GetRegisterFunc(
	height uint64,
) func(flow.RegisterID) (flow.RegisterValue, error) {
	return func(regID flow.RegisterID) (flow.RegisterValue, error) {
		value := s.GetRegister(regID, height)
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

func (s *ProtocolStorage) SaveBlockHeader(header *flow.Header) error {
	data, err := header.MarshalCBOR()
	if err != nil {
		return err
	}
	err = s.blocksDb.Set(makePrefix(codeBlockHeader, header.Height), data, pebble.Sync)
	if err != nil {
		return err
	}
	err = s.blocksDb.Set(makePrefix(codeLastBlockHeaderHeight), b(header.Height), pebble.Sync)
	if err != nil {
		return err
	}
	return nil
}

func (s *ProtocolStorage) GetBlockHeader(blockId flow.Identifier) (*flow.Header, error) {
	height := s.BlockHeight(blockId)
	return s.GetBlockHeaderByHeight(height)
}

func (s *ProtocolStorage) GetBlockHeaderByHeight(height uint64) (*flow.Header, error) {
	if height == 0 {
		return nil, fmt.Errorf("block not found")
	}

	data, closer, err := s.blocksDb.Get(makePrefix(codeBlockHeader, height))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	header := new(flow.Header)
	err = header.UnmarshalCBOR(data)
	if err != nil {
		return nil, err
	}
	return header, nil

}

func (s *ProtocolStorage) ProcessExecutionData(height uint64, executionData *execution_data.BlockExecutionData) error {

	blockEvents := sdk.BlockEvents{
		BlockID:        sdk.Identifier(executionData.BlockID),
		Height:         height,
		BlockTimestamp: time.Now(), //executionData.BlockTimestamp,
		Events:         []sdk.Event{},
	}

	for index, chunk := range executionData.ChunkExecutionDatas {

		err := s.InsertCollection(executionData.BlockID, uint32(index), chunk.Collection)
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
			decodedEvent, err := ccf.Decode(nil, event.Payload)
			fmt.Println(decodedEvent)
			if err != nil {
				fmt.Println(err)
			}
			eventValue, _ := decodedEvent.(cadence.Event)
			fmt.Println(eventValue.String())
			blockEvents.Events = append(blockEvents.Events, sdk.Event{
				TransactionID: sdk.Identifier(event.TransactionID),
				EventIndex:    int(event.EventIndex),
				Type:          string(event.Type),
				Payload:       event.Payload,
				Value:         eventValue,
			})

			err = s.InsertEvent(height, executionData.BlockID, chunk.Collection.ID(), event)
			if err != nil {
				return err
			}
		}

		//index EVM blocks
		evmEvents, err := models.NewCadenceEvents(blockEvents)
		if err != nil {
			fmt.Println("error decoding evm events")
			panic(err)
		}

		//insert evm transactions
		for _, transaction := range evmEvents.Transactions() {
			fmt.Println("EVM TX: evmHeight", evmEvents.Block().Height, "evmTxHash", transaction.Hash())
			err = s.evmDb.Set(makePrefix(codeEVMTransaction, transaction.Hash()), b(evmEvents.CadenceHeight()), pebble.Sync)
			if err != nil {
				fmt.Println("save error", err)
			}
		}
		fmt.Println(evmEvents)
		evmBlockHash, err := evmEvents.Block().Hash()
		if err != nil {
			fmt.Println("error decoding evm block hash")
			panic(err)
		}
		//insert evm block
		fmt.Println("evmHeight", evmEvents.Block().Height, "evmBlockHash", evmBlockHash)
		err = s.evmDb.Set(makePrefix(codeEVMBlockByHeight, evmEvents.Block().Height), b(evmEvents.CadenceHeight()), pebble.Sync)
		if err != nil {
			fmt.Println("save error", err)
		}

		err = s.evmDb.Set(makePrefix(codeEVMBlock, evmBlockHash), b(evmEvents.Block().Height), pebble.Sync)
		if err != nil {
			fmt.Println("save error", err)
		}

		data, err := s.codec.Encode(evmEvents)
		if err != nil {
			panic("can't serialize evm events")
		}
		err = s.evmRawDb.Set(makePrefix(codeEVMBlockRaw, evmEvents.Block().Height), data, pebble.Sync)
		if err != nil {
			fmt.Println("save error", err)
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

			s.SetLedger(makePrefix(codeLedgerPayload, key.CanonicalForm(), uint64(0xFFFFFFFFFFFFFFFF-height)), payload.Value())

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
