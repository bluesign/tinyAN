package storage

import (
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/onflow/flow-go/model/flow"
	gethCommon "github.com/onflow/go-ethereum/common"
)

var (
	codeBinary byte = 0xFF
)

// config is taken from https://github.com/onflow/flow-archive/blob/c75ac6cde86f2be2425e1058146c98eb7b01c872/service/storage2/config/config.go#L10

var defaultPebbleOptions = pebble.Options{
	FormatMajorVersion: pebble.FormatNewest,

	// Soft and hard limits on read amplificaction of L0 respectfully.
	L0CompactionThreshold: 2,
	L0StopWritesThreshold: 1000,

	// When the maximum number of bytes for a level is exceeded, compaction is requested.
	LBaseMaxBytes: 64 << 20, // 64 MB
	Levels:        make([]pebble.LevelOptions, 7),
	MaxOpenFiles:  16384,

	// Writes are stopped when the sum of the queued memtable sizes exceeds MemTableStopWritesThreshold*MemTableSize.
	MemTableSize:                64 << 20,
	MemTableStopWritesThreshold: 4,

	// The default is 1.
	MaxConcurrentCompactions: func() int { return 4 },
}

func MustOpenPebbleDB(path string) *pebble.DB {
	//TODO: check this with new run
	/*opts := defaultPebbleOptions
	for i := 0; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		// The default is 4KiB (uncompressed), which is too small
		// for good performance (esp. on stripped storage).
		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB

		// 10 bits per key yields a filter with <1% false positive rate.
		//
		// The bloom filter is speedsup our SeekPrefixGE by skipping
		// sstables that do not contain the prefix.
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter

		if i > 0 {
			// L0 starts at 2MiB, each level is 2x the previous.
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}

	opts.FlushSplitBytes = opts.Levels[0].TargetFileSize
	opts.EnsureDefaults()
	*/
	db, err := pebble.Open(path, &defaultPebbleOptions)
	if err != nil {
		panic(fmt.Errorf("error opening db: %w", err))
	}
	return db
}

func b(v interface{}) []byte {
	switch i := v.(type) {
	case gethCommon.Hash:
		return i[:]
	case uint8:
		return []byte{i}
	case uint32:
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, i)
		return b
	case uint64:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, i)
		return b
	case []byte:
		return i
	case string:
		return []byte(i)
	case flow.Role:
		return []byte{byte(i)}
	case flow.Identifier:
		return i[:]
	case flow.ChainID:
		return []byte(i)
	default:
		panic(fmt.Sprintf("unsupported type to convert (%T)", v))
	}
}

func makePrefix(code byte, keys ...interface{}) []byte {
	prefix := make([]byte, 1)
	prefix[0] = code
	for _, key := range keys {
		prefix = append(prefix, b(key)...)
	}
	return prefix
}

func DeepCopy(v []byte) []byte {
	newV := make([]byte, len(v))
	copy(newV, v)
	return newV
}

func reverse(str string) (result string) {
	for _, v := range str {
		result = string(v) + result
	}
	return
}
