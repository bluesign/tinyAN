package server

import (
	"context"
	"github.com/onflow/cadence/runtime"
	"github.com/onflow/flow-go/engine/execution/computation/query"
	"github.com/onflow/flow-go/fvm"
	reusableRuntime "github.com/onflow/flow-go/fvm/runtime"
	"github.com/onflow/flow-go/fvm/storage/derived"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/module/metrics"
	"github.com/rs/zerolog"
	"time"
)

type FVMBlocks interface {
	ByHeightFrom(height uint64, header *flow.Header) (*flow.Header, error)
}

type FVMStorageSnapshot interface {
	Get(id flow.RegisterID) ([]byte, error)
}

type Executor interface {
	ChainID() flow.ChainID
	Setup(blocks FVMBlocks, chainID string) error
	ExecuteScript(context.Context, []byte, [][]byte, uint64, FVMStorageSnapshot) ([]byte, error)
	GetAccount(context.Context, []byte, uint64, FVMStorageSnapshot) (*flow.Account, error)
}

var _ Executor = &ScriptExecutor{}

type ScriptExecutor struct {
	logger        zerolog.Logger
	blocks        FVMBlocks
	chainID       flow.ChainID
	queryExecutor *query.QueryExecutor
}

func (e *ScriptExecutor) ChainID() flow.ChainID {
	return e.chainID
}

func (e *ScriptExecutor) Setup(blocks FVMBlocks, chainID string) error {
	e.chainID = flow.ChainID(chainID)
	e.blocks = blocks

	var vm fvm.VM
	vm = fvm.NewVirtualMachine()

	fvmOptions := []fvm.Option{
		fvm.WithReusableCadenceRuntimePool(
			reusableRuntime.NewReusableCadenceRuntimePool(
				0,
				runtime.Config{
					TracingEnabled:     false,
					AttachmentsEnabled: true,
				},
			),
		),

		fvm.WithBlocks(e.blocks),
		fvm.WithChain(e.chainID.Chain()),
		fvm.WithComputationLimit(100_000_000),
		fvm.WithEVMEnabled(true),
	}

	vmCtx := fvm.NewContext(fvmOptions...)
	derivedChainData, err := derived.NewDerivedChainData(10)
	if err != nil {
		e.logger.Panic().Msgf("cannot create derived data cache: %v", err)
	}
	config := query.NewDefaultConfig()
	config.ExecutionTimeLimit = time.Hour * 2
	e.queryExecutor = query.NewQueryExecutor(
		config,
		e.logger,
		&metrics.NoopCollector{}, // TODO: add metrics
		vm,
		vmCtx,
		derivedChainData,
		&EntropyProviderPerBlockProvider{},
	)

	return nil
}

func (e *ScriptExecutor) ExecuteScript(ctx context.Context, script []byte, args [][]byte, block *flow.Block, snapshot FVMStorageSnapshot) ([]byte, error) {
	header, err := e.blocks.ByHeightFrom(block.Header.Height, block.Header)

	if err != nil {
		return nil, err
	}
	result, _, err := e.queryExecutor.ExecuteScript(
		ctx,
		script,
		args,
		header,
		snapshot,
	)
	return result, err
}

func (e *ScriptExecutor) GetAccount(ctx context.Context, address []byte, blockHeight uint64, snapshot FVMStorageSnapshot) (*flow.Account, error) {
	header, err := e.blocks.ByHeightFrom(blockHeight, &flow.Header{})
	if err != nil {
		return nil, err
	}
	return e.queryExecutor.GetAccount(
		ctx,
		flow.BytesToAddress(address),
		header,
		snapshot,
	)
}
