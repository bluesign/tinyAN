package indexer

import (
	"fmt"
	"github.com/fxamacker/cbor/v2"
	"github.com/onflow/atree"
	"github.com/onflow/cadence/runtime/common"
	"github.com/onflow/cadence/runtime/interpreter"
	"reflect"
	"strings"
	"unsafe"
)

type Resource struct {
	TypeName string
	Uuid     uint64
	Fields   map[string]*ResourceField
}

type ResourceResult struct {
	Address  string
	TypeName string
	Uuid     uint64
	Id       uint64
	Balance  uint64
}

type ResourceField struct {
	Name       string
	OldValue   any
	NewValue   any
	HasUpdated bool
	IsNew      bool
}

func (r *ResourceField) String() string {
	status := ""
	if r.IsNew {
		if r.Name == "balance" {
			return fmt.Sprintf("%s: %d.%08d %s (NEW)", r.Name, r.NewValue.(uint64)/100000000, r.NewValue.(uint64)%100000000, status)
		}
		return fmt.Sprintf("%s: %v %s", r.Name, r.NewValue, status)
	}
	if r.HasUpdated {
		if r.Name == "balance" {
			status = fmt.Sprintf("-> %d.%08d (updated)", r.NewValue.(uint64)/100000000, r.NewValue.(uint64)%100000000)
		} else {
			status = fmt.Sprintf("-> %v (updated)", r.NewValue)
		}
	}
	if r.Name == "balance" {
		return fmt.Sprintf("%s: %d.%08d %s", r.Name, r.OldValue.(uint64)/100000000, r.OldValue.(uint64)%100000000, status)
	}
	return fmt.Sprintf("%s: %v %s", r.Name, r.OldValue, status)
}

func (r *Resource) String() string {
	result := fmt.Sprintf("\n%s: (uuid=%d)\n", r.TypeName, r.Uuid)
	for _, field := range r.Fields {
		if field.Name == "uuid" {
			continue
		}
		result += fmt.Sprintf("\t%s\n", field)
	}
	return result
}

func (r *Resource) SetField(name string, value any) {
	field := r.Fields[name]
	field.NewValue = value
	field.HasUpdated = (field.NewValue != field.OldValue) && (field.OldValue != nil)
}

func checkCompositeKeys(address string, v *atree.MapDataSlab) map[string]any {
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
func processArrayDataSlab(address string, v *atree.ArrayDataSlab, result map[uint64]map[string]any) map[uint64]map[string]any {

	v.PopIterate(nil, func(value atree.Storable) {

		valueSlab, isMap := value.(*atree.MapDataSlab)
		if isMap {
			processMapDataSlab(address, valueSlab, result)
		}

		arraySlab, isArray := value.(*atree.ArrayDataSlab)
		if isArray {
			fmt.Println("Array in Array", arraySlab)
			processArrayDataSlab(address, arraySlab, result)
		}

	})

	return result

}
func processMapDataSlab(address string, v *atree.MapDataSlab, result map[uint64]map[string]any) map[uint64]map[string]any {

	compositeKeys := checkCompositeKeys(address, v)
	if len(compositeKeys) > 0 {
		result[compositeKeys["uuid"].(uint64)] = compositeKeys
	}

	for i := 0; i < int(v.Count()); i++ {
		el, _ := v.Element(i)

		value := reflect.Indirect(reflect.ValueOf(el)).Field(1)
		valueValue := reflect.NewAt(value.Type(), unsafe.Pointer(value.UnsafeAddr())).Elem().Interface().(atree.Storable)

		valueSlab, isMap := valueValue.(*atree.MapDataSlab)
		if isMap {
			processMapDataSlab(address, valueSlab, result)
		}

	}

	return result

}
func ParseLedgerData(address string, value []byte) map[uint64]map[string]any {

	result := make(map[uint64]map[string]any, 0)

	if !strings.Contains(string(value), "uuid") {

		if strings.Contains(string(value), "balance") {

		}
		return result
	}

	switch getSlabType(value) {
	case slabArray:
		decMode, _ := cbor.DecOptions{}.DecMode()
		slab, _ := atree.DecodeSlab(atree.SlabID{}, value, decMode, decodeStorable, decodeTypeInfo)

		switch v := slab.(type) {
		case *atree.ArrayDataSlab:
			processArrayDataSlab(address, v, result)

		default:

			fmt.Println("Unhandled")
			panic("p1")
		}

	case slabMap:
		switch getSlabMapType(value) {
		case slabMapData:
			decMode, _ := cbor.DecOptions{}.DecMode()
			slab, _ := atree.DecodeSlab(atree.SlabID{}, value, decMode, decodeStorable, decodeTypeInfo)

			switch v := slab.(type) {
			case *atree.MapDataSlab:
				processMapDataSlab(address, v, result)

			default:
				fmt.Println("Unhandled")
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
