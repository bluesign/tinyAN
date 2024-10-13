package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/bluesign/tinyAN/indexer"
	"github.com/cockroachdb/pebble"
	"github.com/onflow/flow-go/model/flow"
)

type BalanceInfo struct {
	Address     string
	Balance     uint64
	Action      string
	CadenceType string
	Uuid        uint64
	Height      uint64
}

func (s *IndexStorage) OwnerOfUuid(uuid uint64, targetHeight uint64) string {
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

func (s *IndexStorage) BalanceHistoryByAddress(addressString string, targetType string, targetHeight uint64) []BalanceInfo {

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

func (s *IndexStorage) OwnersByType(cadenceType string, targetHeight uint64) []indexer.ResourceResult {
	reverseHeight := uint64(0xFFFFFFFFFFFFFFFF - targetHeight)
	prefix := makePrefix(codeAction_index_reverse, []byte(cadenceType))
	currentPrefix := prefix

	//type = address - height - uuid  // action - id - balance
	//type = address - uuid - height   // action - id - balance

	addresses := map[uint64]indexer.ResourceResult{}

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

			addresses[uuid] = indexer.ResourceResult{
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

	addr := []indexer.ResourceResult{}
	for _, v := range addresses {
		addr = append(addr, v)

	}
	return addr

}
