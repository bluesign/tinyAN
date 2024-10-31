package repl

import (
	"encoding/hex"
	"fmt"
	"github.com/onflow/atree"
	"github.com/onflow/cadence"
	"github.com/onflow/cadence/ast"
	"github.com/onflow/cadence/common"
	"github.com/onflow/cadence/interpreter"
	"github.com/onflow/cadence/runtime"
	"github.com/onflow/cadence/sema"
	"go.opentelemetry.io/otel/attribute"
	"time"
)

type RuntimeWrapper struct {
	BaseRuntime    runtime.Interface
	CurrentRuntime runtime.Interface
	REPL           *REPL
}

func (d *RuntimeWrapper) MeterMemory(usage common.MemoryUsage) error {
	return d.BaseRuntime.MeterMemory(usage)
}

func (d *RuntimeWrapper) MeterComputation(operationType common.ComputationKind, intensity uint) error {
	return d.BaseRuntime.MeterComputation(operationType, intensity)
}

func (d *RuntimeWrapper) ComputationUsed() (uint64, error) {
	return d.BaseRuntime.ComputationUsed()
}

func (d *RuntimeWrapper) MemoryUsed() (uint64, error) {
	return d.BaseRuntime.MemoryUsed()
}

func (d *RuntimeWrapper) InteractionUsed() (uint64, error) {
	return d.BaseRuntime.InteractionUsed()
}

func (d *RuntimeWrapper) ResolveLocation(identifiers []runtime.Identifier, location runtime.Location) ([]runtime.ResolvedLocation, error) {
	fmt.Println("ResolveLocation", identifiers, location)
	result, err := d.BaseRuntime.ResolveLocation(identifiers, location)
	fmt.Println(result)
	return result, err
}

func (d *RuntimeWrapper) GetCode(location runtime.Location) ([]byte, error) {
	fmt.Println("GetCode", location)
	return d.BaseRuntime.GetCode(location)
}

func (d *RuntimeWrapper) GetOrLoadProgram(location runtime.Location, load func() (*interpreter.Program, error)) (*interpreter.Program, error) {
	fmt.Println("GetOrLoadProgram", location)

	return d.BaseRuntime.GetOrLoadProgram(location, load)
}

func (d *RuntimeWrapper) SetInterpreterSharedState(state *interpreter.SharedState) {
	fmt.Println("SetInterpreterSharedState")
	d.BaseRuntime.SetInterpreterSharedState(state)
}

func (d *RuntimeWrapper) GetInterpreterSharedState() *interpreter.SharedState {
	fmt.Println("GetInterpreterSharedState")
	return d.BaseRuntime.GetInterpreterSharedState()
}

func (d *RuntimeWrapper) GetValue(owner, key []byte) (value []byte, err error) {
	fmt.Println("GetValue", hex.EncodeToString(owner), hex.EncodeToString(key))
	return d.BaseRuntime.GetValue(owner, key)
}

func (d *RuntimeWrapper) SetValue(owner, key, value []byte) (err error) {
	return d.BaseRuntime.SetValue(owner, key, value)
}

func (d *RuntimeWrapper) ValueExists(owner, key []byte) (exists bool, err error) {
	fmt.Println("ValueExists", hex.EncodeToString(owner), hex.EncodeToString(key))
	return d.BaseRuntime.ValueExists(owner, key)
}

func (d *RuntimeWrapper) AllocateSlabIndex(owner []byte) (atree.SlabIndex, error) {
	return d.BaseRuntime.AllocateSlabIndex(owner)
}

func (d *RuntimeWrapper) CreateAccount(payer runtime.Address) (address runtime.Address, err error) {
	return d.BaseRuntime.CreateAccount(payer)
}

func (d *RuntimeWrapper) AddAccountKey(address runtime.Address, publicKey *runtime.PublicKey, hashAlgo runtime.HashAlgorithm, weight int) (*runtime.AccountKey, error) {
	return d.BaseRuntime.AddAccountKey(address, publicKey, hashAlgo, weight)
}

func (d *RuntimeWrapper) GetAccountKey(address runtime.Address, index uint32) (*runtime.AccountKey, error) {
	return d.BaseRuntime.GetAccountKey(address, index)
}

func (d *RuntimeWrapper) AccountKeysCount(address runtime.Address) (uint32, error) {
	return d.BaseRuntime.AccountKeysCount(address)
}

func (d *RuntimeWrapper) RevokeAccountKey(address runtime.Address, index uint32) (*runtime.AccountKey, error) {
	return d.BaseRuntime.RevokeAccountKey(address, index)
}

func (d *RuntimeWrapper) UpdateAccountContractCode(location common.AddressLocation, code []byte) (err error) {
	return d.BaseRuntime.UpdateAccountContractCode(location, code)
}

func (d *RuntimeWrapper) GetAccountContractCode(location common.AddressLocation) (code []byte, err error) {
	return d.BaseRuntime.GetAccountContractCode(location)
}

func (d *RuntimeWrapper) RemoveAccountContractCode(location common.AddressLocation) (err error) {
	return d.BaseRuntime.RemoveAccountContractCode(location)
}

func (d *RuntimeWrapper) GetSigningAccounts() ([]runtime.Address, error) {
	return d.BaseRuntime.GetSigningAccounts()
}

func (d *RuntimeWrapper) ProgramLog(s string) error {
	return d.BaseRuntime.ProgramLog(s)
}

func (d *RuntimeWrapper) EmitEvent(event cadence.Event) error {
	return d.BaseRuntime.EmitEvent(event)
}

func (d *RuntimeWrapper) GenerateUUID() (uint64, error) {
	return d.BaseRuntime.GenerateUUID()
}

func (d *RuntimeWrapper) DecodeArgument(argument []byte, argumentType cadence.Type) (cadence.Value, error) {
	return d.BaseRuntime.DecodeArgument(argument, argumentType)
}

func (d *RuntimeWrapper) GetCurrentBlockHeight() (uint64, error) {
	return d.BaseRuntime.GetCurrentBlockHeight()
}

func (d *RuntimeWrapper) GetBlockAtHeight(height uint64) (block runtime.Block, exists bool, err error) {
	return d.BaseRuntime.GetBlockAtHeight(height)
}

func (d *RuntimeWrapper) ReadRandom(bytes []byte) error {
	return d.BaseRuntime.ReadRandom(bytes)
}

func (d *RuntimeWrapper) VerifySignature(signature []byte, tag string, signedData []byte, publicKey []byte, signatureAlgorithm runtime.SignatureAlgorithm, hashAlgorithm runtime.HashAlgorithm) (bool, error) {
	return d.BaseRuntime.VerifySignature(signature, tag, signedData, publicKey, signatureAlgorithm, hashAlgorithm)
}

func (d *RuntimeWrapper) Hash(data []byte, tag string, hashAlgorithm runtime.HashAlgorithm) ([]byte, error) {
	return d.BaseRuntime.Hash(data, tag, hashAlgorithm)
}

func (d *RuntimeWrapper) GetAccountBalance(address common.Address) (value uint64, err error) {
	return d.BaseRuntime.GetAccountBalance(address)
}

func (d *RuntimeWrapper) GetAccountAvailableBalance(address common.Address) (value uint64, err error) {
	return d.BaseRuntime.GetAccountAvailableBalance(address)
}

func (d *RuntimeWrapper) GetStorageUsed(address runtime.Address) (value uint64, err error) {
	fmt.Println("GetStorageUsed", address)
	return d.BaseRuntime.GetStorageUsed(address)
}

func (d *RuntimeWrapper) GetStorageCapacity(address runtime.Address) (value uint64, err error) {
	return d.BaseRuntime.GetStorageCapacity(address)
}

func (d *RuntimeWrapper) ImplementationDebugLog(message string) error {
	return d.BaseRuntime.ImplementationDebugLog(message)
}

func (d *RuntimeWrapper) ValidatePublicKey(key *runtime.PublicKey) error {
	return d.BaseRuntime.ValidatePublicKey(key)
}

func (d *RuntimeWrapper) GetAccountContractNames(address runtime.Address) ([]string, error) {
	return d.BaseRuntime.GetAccountContractNames(address)
}

func (d *RuntimeWrapper) RecordTrace(operation string, location runtime.Location, duration time.Duration, attrs []attribute.KeyValue) {
	d.BaseRuntime.RecordTrace(operation, location, duration, attrs)
}

func (d *RuntimeWrapper) BLSVerifyPOP(publicKey *runtime.PublicKey, signature []byte) (bool, error) {
	return d.BaseRuntime.BLSVerifyPOP(publicKey, signature)
}

func (d *RuntimeWrapper) BLSAggregateSignatures(signatures [][]byte) ([]byte, error) {
	return d.BaseRuntime.BLSAggregateSignatures(signatures)
}

func (d *RuntimeWrapper) BLSAggregatePublicKeys(publicKeys []*runtime.PublicKey) (*runtime.PublicKey, error) {
	return d.BaseRuntime.BLSAggregatePublicKeys(publicKeys)
}

func (d *RuntimeWrapper) ResourceOwnerChanged(interpreter *interpreter.Interpreter, resource *interpreter.CompositeValue, oldOwner common.Address, newOwner common.Address) {
	d.BaseRuntime.ResourceOwnerChanged(interpreter, resource, oldOwner, newOwner)
}

func (d *RuntimeWrapper) GenerateAccountID(address common.Address) (uint64, error) {
	return d.BaseRuntime.GenerateAccountID(address)
}

func (d *RuntimeWrapper) RecoverProgram(program *ast.Program, location common.Location) ([]byte, error) {
	return d.BaseRuntime.RecoverProgram(program, location)
}

func (d *RuntimeWrapper) ValidateAccountCapabilitiesGet(inter *interpreter.Interpreter, locationRange interpreter.LocationRange, address interpreter.AddressValue, path interpreter.PathValue, wantedBorrowType *sema.ReferenceType, capabilityBorrowType *sema.ReferenceType) (bool, error) {
	return d.BaseRuntime.ValidateAccountCapabilitiesGet(inter, locationRange, address, path, wantedBorrowType, capabilityBorrowType)
}

func (d *RuntimeWrapper) ValidateAccountCapabilitiesPublish(inter *interpreter.Interpreter, locationRange interpreter.LocationRange, address interpreter.AddressValue, path interpreter.PathValue, capabilityBorrowType *interpreter.ReferenceStaticType) (bool, error) {
	return d.BaseRuntime.ValidateAccountCapabilitiesPublish(inter, locationRange, address, path, capabilityBorrowType)
}

var _ runtime.Interface = &RuntimeWrapper{}
