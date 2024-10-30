package storage

import (
	"fmt"
	"github.com/onflow/atree"
	"github.com/onflow/flow-go/fvm/environment"
	"github.com/onflow/flow-go/model/flow"
)

type ViewOnlyLedger struct {
	snapshot FVMStorageSnapshot
	writes   map[flow.RegisterID]flow.RegisterValue
}

func NewViewOnlyLedger(snapshot FVMStorageSnapshot) *ViewOnlyLedger {
	return &ViewOnlyLedger{
		snapshot: snapshot,
		writes:   make(map[flow.RegisterID]flow.RegisterValue),
	}
}

func (v *ViewOnlyLedger) GetValue(owner, key []byte) ([]byte, error) {
	reg := flow.RegisterID{
		Owner: string(owner),
		Key:   string(key),
	}
	if value, ok := v.writes[reg]; ok {
		return value, nil
	}
	return v.snapshot.Get(reg)
}

func (v *ViewOnlyLedger) SetValue(owner, key, value []byte) (err error) {
	reg := flow.RegisterID{
		Owner: string(owner),
		Key:   string(key),
	}

	v.writes[reg] = value
	return nil
}

func (v *ViewOnlyLedger) ValueExists(owner, key []byte) (exists bool, err error) {
	value, err := v.GetValue(owner, key)
	if err != nil {
		return false, err
	}
	return len(value) > 0, nil
}

func (v *ViewOnlyLedger) GetPendingWrites() map[flow.RegisterID]flow.RegisterValue {
	return v.writes
}

func (v *ViewOnlyLedger) AllocateSlabIndex(owner []byte) (atree.SlabIndex, error) {
	statusBytes, err := v.GetValue(owner, []byte(flow.AccountStatusKey))
	if err != nil {
		return atree.SlabIndex{}, err
	}
	if len(statusBytes) == 0 {
		return atree.SlabIndex{}, fmt.Errorf("state for account not found")
	}

	status, err := environment.AccountStatusFromBytes(statusBytes)
	if err != nil {
		return atree.SlabIndex{}, err
	}

	// get and increment the index
	index := status.SlabIndex()
	newIndexBytes := index.Next()

	// update the storageIndex bytes
	status.SetStorageIndex(newIndexBytes)
	err = v.SetValue(owner, []byte(flow.AccountStatusKey), status.ToBytes())
	if err != nil {
		return atree.SlabIndex{}, err
	}
	return index, nil
}

var _ atree.Ledger = (*ViewOnlyLedger)(nil)
