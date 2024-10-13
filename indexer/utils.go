package indexer

import (
	"github.com/fxamacker/cbor/v2"
	"github.com/onflow/atree"
	"github.com/onflow/cadence/runtime/interpreter"
)

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
