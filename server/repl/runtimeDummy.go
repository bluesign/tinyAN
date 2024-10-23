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

type runtimeWrapper struct {
	baseRuntime runtime.Interface
	REPL        *REPL
}

func (d *runtimeWrapper) MeterMemory(usage common.MemoryUsage) error {
	return d.baseRuntime.MeterMemory(usage)
}

func (d *runtimeWrapper) MeterComputation(operationType common.ComputationKind, intensity uint) error {
	return d.baseRuntime.MeterComputation(operationType, intensity)
}

func (d *runtimeWrapper) ComputationUsed() (uint64, error) {
	return d.baseRuntime.ComputationUsed()
}

func (d *runtimeWrapper) MemoryUsed() (uint64, error) {
	return d.baseRuntime.MemoryUsed()
}

func (d *runtimeWrapper) InteractionUsed() (uint64, error) {
	return d.baseRuntime.InteractionUsed()
}

func (d *runtimeWrapper) ResolveLocation(identifiers []runtime.Identifier, location runtime.Location) ([]runtime.ResolvedLocation, error) {
	return d.baseRuntime.ResolveLocation(identifiers, location)
}

func (d *runtimeWrapper) GetCode(location runtime.Location) ([]byte, error) {
	return d.baseRuntime.GetCode(location)
}

func (d *runtimeWrapper) GetOrLoadProgram(location runtime.Location, load func() (*interpreter.Program, error)) (*interpreter.Program, error) {
	return d.baseRuntime.GetOrLoadProgram(location, load)
}

func (d *runtimeWrapper) SetInterpreterSharedState(state *interpreter.SharedState) {
	fmt.Println("SetInterpreterSharedState")
	d.baseRuntime.SetInterpreterSharedState(state)
}

func (d *runtimeWrapper) GetInterpreterSharedState() *interpreter.SharedState {
	fmt.Println("GetInterpreterSharedState")
	return d.baseRuntime.GetInterpreterSharedState()
}

func (d *runtimeWrapper) GetValue(owner, key []byte) (value []byte, err error) {
	fmt.Println("GetValue", hex.EncodeToString(owner), hex.EncodeToString(key))
	return d.baseRuntime.GetValue(owner, key)
}

func (d *runtimeWrapper) SetValue(owner, key, value []byte) (err error) {
	return d.baseRuntime.SetValue(owner, key, value)
}

func (d *runtimeWrapper) ValueExists(owner, key []byte) (exists bool, err error) {
	fmt.Println("ValueExists", hex.EncodeToString(owner), hex.EncodeToString(key))
	return d.baseRuntime.ValueExists(owner, key)
}

func (d *runtimeWrapper) AllocateSlabIndex(owner []byte) (atree.SlabIndex, error) {
	return d.baseRuntime.AllocateSlabIndex(owner)
}

func (d *runtimeWrapper) CreateAccount(payer runtime.Address) (address runtime.Address, err error) {
	return d.baseRuntime.CreateAccount(payer)
}

func (d *runtimeWrapper) AddAccountKey(address runtime.Address, publicKey *runtime.PublicKey, hashAlgo runtime.HashAlgorithm, weight int) (*runtime.AccountKey, error) {
	return d.baseRuntime.AddAccountKey(address, publicKey, hashAlgo, weight)
}

func (d *runtimeWrapper) GetAccountKey(address runtime.Address, index uint32) (*runtime.AccountKey, error) {
	return d.baseRuntime.GetAccountKey(address, index)
}

func (d *runtimeWrapper) AccountKeysCount(address runtime.Address) (uint32, error) {
	return d.baseRuntime.AccountKeysCount(address)
}

func (d *runtimeWrapper) RevokeAccountKey(address runtime.Address, index uint32) (*runtime.AccountKey, error) {
	return d.baseRuntime.RevokeAccountKey(address, index)
}

func (d *runtimeWrapper) UpdateAccountContractCode(location common.AddressLocation, code []byte) (err error) {
	return d.baseRuntime.UpdateAccountContractCode(location, code)
}

func (d *runtimeWrapper) GetAccountContractCode(location common.AddressLocation) (code []byte, err error) {
	return d.baseRuntime.GetAccountContractCode(location)
}

func (d *runtimeWrapper) RemoveAccountContractCode(location common.AddressLocation) (err error) {
	return d.baseRuntime.RemoveAccountContractCode(location)
}

func (d *runtimeWrapper) GetSigningAccounts() ([]runtime.Address, error) {
	return d.baseRuntime.GetSigningAccounts()
}

func (d *runtimeWrapper) ProgramLog(s string) error {
	fmt.Println("LOG:", s)
	return d.baseRuntime.ProgramLog(s)
}

func (d *runtimeWrapper) EmitEvent(event cadence.Event) error {
	return d.baseRuntime.EmitEvent(event)
}

func (d *runtimeWrapper) GenerateUUID() (uint64, error) {
	return d.baseRuntime.GenerateUUID()
}

func (d *runtimeWrapper) DecodeArgument(argument []byte, argumentType cadence.Type) (cadence.Value, error) {
	return d.baseRuntime.DecodeArgument(argument, argumentType)
}

func (d *runtimeWrapper) GetCurrentBlockHeight() (uint64, error) {
	return d.baseRuntime.GetCurrentBlockHeight()
}

func (d *runtimeWrapper) GetBlockAtHeight(height uint64) (block runtime.Block, exists bool, err error) {
	return d.baseRuntime.GetBlockAtHeight(height)
}

func (d *runtimeWrapper) ReadRandom(bytes []byte) error {
	return d.baseRuntime.ReadRandom(bytes)
}

func (d *runtimeWrapper) VerifySignature(signature []byte, tag string, signedData []byte, publicKey []byte, signatureAlgorithm runtime.SignatureAlgorithm, hashAlgorithm runtime.HashAlgorithm) (bool, error) {
	return d.baseRuntime.VerifySignature(signature, tag, signedData, publicKey, signatureAlgorithm, hashAlgorithm)
}

func (d *runtimeWrapper) Hash(data []byte, tag string, hashAlgorithm runtime.HashAlgorithm) ([]byte, error) {
	return d.baseRuntime.Hash(data, tag, hashAlgorithm)
}

func (d *runtimeWrapper) GetAccountBalance(address common.Address) (value uint64, err error) {
	return d.baseRuntime.GetAccountBalance(address)
}

func (d *runtimeWrapper) GetAccountAvailableBalance(address common.Address) (value uint64, err error) {
	return d.baseRuntime.GetAccountAvailableBalance(address)
}

func (d *runtimeWrapper) GetStorageUsed(address runtime.Address) (value uint64, err error) {
	return d.baseRuntime.GetStorageUsed(address)
}

func (d *runtimeWrapper) GetStorageCapacity(address runtime.Address) (value uint64, err error) {
	return d.baseRuntime.GetStorageCapacity(address)
}

func (d *runtimeWrapper) ImplementationDebugLog(message string) error {
	return d.baseRuntime.ImplementationDebugLog(message)
}

func (d *runtimeWrapper) ValidatePublicKey(key *runtime.PublicKey) error {
	return d.baseRuntime.ValidatePublicKey(key)
}

func (d *runtimeWrapper) GetAccountContractNames(address runtime.Address) ([]string, error) {
	return d.baseRuntime.GetAccountContractNames(address)
}

func (d *runtimeWrapper) RecordTrace(operation string, location runtime.Location, duration time.Duration, attrs []attribute.KeyValue) {
	d.baseRuntime.RecordTrace(operation, location, duration, attrs)
}

func (d *runtimeWrapper) BLSVerifyPOP(publicKey *runtime.PublicKey, signature []byte) (bool, error) {
	return d.baseRuntime.BLSVerifyPOP(publicKey, signature)
}

func (d *runtimeWrapper) BLSAggregateSignatures(signatures [][]byte) ([]byte, error) {
	return d.baseRuntime.BLSAggregateSignatures(signatures)
}

func (d *runtimeWrapper) BLSAggregatePublicKeys(publicKeys []*runtime.PublicKey) (*runtime.PublicKey, error) {
	return d.baseRuntime.BLSAggregatePublicKeys(publicKeys)
}

func (d *runtimeWrapper) ResourceOwnerChanged(interpreter *interpreter.Interpreter, resource *interpreter.CompositeValue, oldOwner common.Address, newOwner common.Address) {
	d.baseRuntime.ResourceOwnerChanged(interpreter, resource, oldOwner, newOwner)
}

func (d *runtimeWrapper) GenerateAccountID(address common.Address) (uint64, error) {
	return d.baseRuntime.GenerateAccountID(address)
}

func (d *runtimeWrapper) RecoverProgram(program *ast.Program, location common.Location) ([]byte, error) {
	return d.baseRuntime.RecoverProgram(program, location)
}

func (d *runtimeWrapper) ValidateAccountCapabilitiesGet(inter *interpreter.Interpreter, locationRange interpreter.LocationRange, address interpreter.AddressValue, path interpreter.PathValue, wantedBorrowType *sema.ReferenceType, capabilityBorrowType *sema.ReferenceType) (bool, error) {
	return d.baseRuntime.ValidateAccountCapabilitiesGet(inter, locationRange, address, path, wantedBorrowType, capabilityBorrowType)
}

func (d *runtimeWrapper) ValidateAccountCapabilitiesPublish(inter *interpreter.Interpreter, locationRange interpreter.LocationRange, address interpreter.AddressValue, path interpreter.PathValue, capabilityBorrowType *interpreter.ReferenceStaticType) (bool, error) {
	return d.baseRuntime.ValidateAccountCapabilitiesPublish(inter, locationRange, address, path, capabilityBorrowType)
}

var _ runtime.Interface = &runtimeWrapper{}
