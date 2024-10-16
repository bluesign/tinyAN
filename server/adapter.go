package server

import (
	"context"
	"fmt"
	"github.com/bluesign/tinyAN/indexer"
	"github.com/bluesign/tinyAN/storage"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/onflow/cadence/runtime"
	"github.com/onflow/cadence/runtime/interpreter"
	"github.com/onflow/flow-go/access"
	"github.com/onflow/flow-go/engine/access/subscription"
	"github.com/onflow/flow-go/engine/common/rpc/convert"
	"github.com/onflow/flow-go/fvm"
	"sync"

	"github.com/onflow/flow-go/fvm/environment"
	reusableRuntime "github.com/onflow/flow-go/fvm/runtime"
	fvmStorage "github.com/onflow/flow-go/fvm/storage"
	fvmState "github.com/onflow/flow-go/fvm/storage/state"

	flowgo "github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow/protobuf/go/flow/entities"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
)

type TemporaryTransactionResult struct {
	Transaction flowgo.TransactionBody
	Output      fvm.ProcedureOutput
	BlockHeight uint64
	BlockID     flowgo.Identifier
}

type EntropyProviderPerBlockProvider struct {
	// AtBlockID returns an entropy provider at the given block ID.
}

func (e *EntropyProviderPerBlockProvider) RandomSource() ([]byte, error) {
	return []byte{42}, nil
}

func (e *EntropyProviderPerBlockProvider) AtBlockID(blockID flowgo.Identifier) environment.EntropyProvider {
	return e
}

var _ access.API = &AccessAdapter{}

type AccessAdapter struct {
	logger   zerolog.Logger
	store    *storage.HeightBasedStorage
	executor *ScriptExecutor
	txCache  *lru.Cache[flowgo.Identifier, TemporaryTransactionResult]
}

// NewAccessAdapter returns a new AccessAdapter.
func NewAccessAdapter(logger zerolog.Logger, store *storage.HeightBasedStorage) *AccessAdapter {
	executor := &ScriptExecutor{
		logger: logger,
	}
	executor.Setup(store, "flow-mainnet")

	cache, _ := lru.New[flowgo.Identifier, TemporaryTransactionResult](1000)
	return &AccessAdapter{
		logger:   logger,
		store:    store,
		executor: executor,
		txCache:  cache,
	}
}

func convertError(err error, defaultStatusCode codes.Code) error {
	if err != nil {
		switch err.(type) {
		default:
			return status.Error(defaultStatusCode, err.Error())
		}
	}
	return nil
}

func (a *AccessAdapter) Ping(_ context.Context) error {
	return nil
}

func (a *AccessAdapter) GetNetworkParameters(_ context.Context) access.NetworkParameters {
	return access.NetworkParameters{
		ChainID: "flow-mainnet",
	}
}

func (a *AccessAdapter) GetLatestBlockHeader(_ context.Context, _ bool) (*flowgo.Header, flowgo.BlockStatus, error) {
	block, err := a.store.GetLatestBlock()
	if err != nil {
		return nil, flowgo.BlockStatusUnknown, convertError(err, codes.Internal)
	}
	a.logger.Debug().Fields(map[string]any{
		"blockHeight": block.Height,
		"blockID":     block.ID().String(),
	}).Msg("🎁  GetLatestBlockHeader called")

	return block, flowgo.BlockStatusSealed, nil
}

func (a *AccessAdapter) GetBlockHeaderByHeight(_ context.Context, height uint64) (*flowgo.Header, flowgo.BlockStatus, error) {
	block, err := a.store.GetBlockByHeight(height)
	if err != nil {
		return nil, flowgo.BlockStatusUnknown, convertError(err, codes.Internal)
	}
	a.logger.Debug().Fields(map[string]any{
		"blockHeight": block.Height,
		"blockID":     block.ID().String(),
	}).Msg("🎁  GetBlockHeaderByHeight called")

	return block, flowgo.BlockStatusSealed, nil
}

func (a *AccessAdapter) GetBlockHeaderByID(_ context.Context, id flowgo.Identifier) (*flowgo.Header, flowgo.BlockStatus, error) {
	block, err := a.store.GetBlockById(id)

	if err != nil {
		return nil, flowgo.BlockStatusUnknown, convertError(err, codes.Internal)
	}

	a.logger.Debug().Fields(map[string]any{
		"blockHeight": block.Height,
		"blockID":     block.ID().String(),
	}).Msg("🎁  GetBlockHeaderByID called")

	return block, flowgo.BlockStatusSealed, nil
}

func (a *AccessAdapter) GetLatestBlock(_ context.Context, _ bool) (*flowgo.Block, flowgo.BlockStatus, error) {
	header, err := a.store.GetLatestBlock()
	if err != nil {
		return nil, flowgo.BlockStatusUnknown, convertError(err, codes.Internal)
	}

	a.logger.Debug().Fields(map[string]any{
		"blockHeight": header.Height,
		"blockID":     header.ID().String(),
	}).Msg("🎁  GetLatestBlock called")

	collections, _ := a.store.CollectionsAtBlock(header.ID())
	guarantees := make([]*flowgo.CollectionGuarantee, 0)
	if len(collections) > 0 {
		for _, collection := range collections[:len(collections)-1] {
			guarantees = append(guarantees, &flowgo.CollectionGuarantee{
				CollectionID: collection,
				Signature:    nil,
			})
		}
	}

	block := &flowgo.Block{
		Header: header,
		Payload: &flowgo.Payload{
			Guarantees: guarantees,
		},
	}
	return block, flowgo.BlockStatusSealed, nil
}

func (a *AccessAdapter) GetBlockByHeight(_ context.Context, height uint64) (*flowgo.Block, flowgo.BlockStatus, error) {
	header, err := a.store.GetBlockByHeight(height)
	if err != nil {
		return nil, flowgo.BlockStatusUnknown, convertError(err, codes.Internal)
	}

	a.logger.Debug().Fields(map[string]any{
		"blockHeight": header.Height,
		"blockID":     header.ID().String(),
	}).Msg("🎁  getBlockByHeight called")

	collections, _ := a.store.CollectionsAtBlock(header.ID())
	guarantees := make([]*flowgo.CollectionGuarantee, 0)
	if len(collections) > 0 {
		for _, collection := range collections[:len(collections)-1] {
			guarantees = append(guarantees, &flowgo.CollectionGuarantee{
				CollectionID: collection,
				Signature:    nil,
			})
		}
	}
	block := &flowgo.Block{
		Header: header,
		Payload: &flowgo.Payload{
			Guarantees: guarantees,
		},
	}
	return block, flowgo.BlockStatusSealed, nil
}

func (a *AccessAdapter) GetBlockByID(_ context.Context, id flowgo.Identifier) (*flowgo.Block, flowgo.BlockStatus, error) {
	header, err := a.store.GetBlockById(id)
	if err != nil {
		return nil, flowgo.BlockStatusUnknown, convertError(err, codes.Internal)
	}

	a.logger.Debug().Fields(map[string]any{
		"blockHeight": header.Height,
		"blockID":     header.ID().String(),
	}).Msg("🎁  GetBlockByID called")

	collections, _ := a.store.CollectionsAtBlock(id)
	guarantees := make([]*flowgo.CollectionGuarantee, 0)
	if len(collections) > 0 {

		for _, collection := range collections[:len(collections)-1] {
			guarantees = append(guarantees, &flowgo.CollectionGuarantee{
				CollectionID: collection,
				Signature:    nil,
			})
		}
	}
	block := &flowgo.Block{
		Header: header,
		Payload: &flowgo.Payload{
			Guarantees: guarantees,
		},
	}
	return block, flowgo.BlockStatusSealed, nil
}

func (a *AccessAdapter) GetCollectionByID(_ context.Context, id flowgo.Identifier) (*flowgo.LightCollection, error) {
	transactionsAtCollection, err := a.store.TransactionsAtCollection(id)
	if err != nil {
		return nil, convertError(err, codes.Internal)
	}
	light := &flowgo.LightCollection{
		Transactions: transactionsAtCollection,
	}
	return light, nil
}

func (a *AccessAdapter) GetFullCollectionByID(_ context.Context, id flowgo.Identifier) (*flowgo.Collection, error) {
	panic("TODO: implement")
}

func (a *AccessAdapter) GetTransaction(_ context.Context, id flowgo.Identifier) (*flowgo.TransactionBody, error) {

	txCached, ok := a.txCache.Get(id)
	if ok {
		return &txCached.Transaction, nil
	}

	tx, err := a.store.GetTransactionById(id)
	if err != nil {
		return nil, convertError(err, codes.Internal)
	}
	a.logger.Debug().
		Str("txID", id.String()).
		Msg("💵  GetTransaction called")

	return tx, nil
}

func (a *AccessAdapter) GetTransactionResult(
	_ context.Context,
	id flowgo.Identifier,
	_ flowgo.Identifier,
	_ flowgo.Identifier,
	requiredEventEncodingVersion entities.EventEncodingVersion,
) (
	*access.TransactionResult,
	error,
) {
	result, err := a.store.GetTransactionResult(id)
	if err != nil {
		return nil, convertError(err, codes.Internal)
	}

	a.logger.Debug().
		Str("txID", id.String()).
		Msg("📝  GetTransactionResult called")

	return result, nil

}

func (a *AccessAdapter) GetAccount(ctx context.Context, address flowgo.Address) (*flowgo.Account, error) {

	block, err := a.store.GetLatestBlock()
	if err != nil {
		return nil, convertError(err, codes.Internal)
	}

	account, err := a.executor.GetAccount(ctx,
		address.Bytes(),
		block.Height,
		a.store.LedgerSnapshot(block.Height),
	)

	if err != nil {
		return nil, convertError(err, codes.Internal)
	}

	a.logger.Debug().
		Stringer("address", address).
		Msg("👤  GetAccount called")

	return account, nil
}

func (a *AccessAdapter) GetAccountAtLatestBlock(ctx context.Context, address flowgo.Address) (*flowgo.Account, error) {
	account, err := a.GetAccount(ctx, address)
	if err != nil {
		return nil, convertError(err, codes.Internal)
	}

	a.logger.Debug().
		Stringer("address", address).
		Msg("👤  GetAccountAtLatestBlock called")

	return account, nil
}

func (a *AccessAdapter) GetAccountAtBlockHeight(
	ctx context.Context,
	address flowgo.Address,
	height uint64,
) (*flowgo.Account, error) {

	a.logger.Debug().
		Stringer("address", address).
		Uint64("height", height).
		Msg("👤  GetAccountAtBlockHeight called")

	account, err := a.executor.GetAccount(ctx,
		address.Bytes(),
		height,
		a.store.LedgerSnapshot(height),
	)

	if err != nil {
		return nil, convertError(err, codes.Internal)
	}

	a.logger.Debug().
		Stringer("address", address).
		Msg("👤  GetAccount called")

	return account, nil

}

func (a *AccessAdapter) ExecuteScriptAtLatestBlock(
	ctx context.Context,
	script []byte,
	arguments [][]byte,
) ([]byte, error) {
	latestBlock, err := a.store.GetLatestBlock()
	if err != nil {
		return nil, err
	}
	a.logger.Debug().
		Uint64("blockHeight", latestBlock.Height).
		Msg("👤  ExecuteScriptAtLatestBlock called")

	return a.executor.ExecuteScript(ctx, script, arguments, latestBlock.Height, a.store.LedgerSnapshot(latestBlock.Height))
}

func (a *AccessAdapter) ExecuteScriptAtBlockHeight(
	ctx context.Context,
	blockHeight uint64,
	script []byte,
	arguments [][]byte,
) ([]byte, error) {
	a.logger.Debug().
		Uint64("blockHeight", blockHeight).
		Msg("👤  ExecuteScriptAtBlockHeight called")

	return a.executor.ExecuteScript(ctx, script, arguments, blockHeight, a.store.LedgerSnapshot(blockHeight))
}

func (a *AccessAdapter) ExecuteScriptAtBlockID(
	ctx context.Context,
	blockID flowgo.Identifier,
	script []byte,
	arguments [][]byte,
) ([]byte, error) {

	block, err := a.store.GetBlockById(blockID)
	if err != nil {
		return nil, err
	}

	a.logger.Debug().
		Stringer("blockID", blockID).
		Msg("👤  ExecuteScriptAtBlockID called")

	return a.executor.ExecuteScript(ctx, script, arguments, block.Height, a.store.LedgerSnapshot(block.Height))
}

func (a *AccessAdapter) GetEventsForHeightRange(
	_ context.Context,
	eventType string,
	startHeight, endHeight uint64,
	requiredEventEncodingVersion entities.EventEncodingVersion,
) ([]flowgo.BlockEvents, error) {

	eventType, err := convert.EventType(eventType)
	if err != nil {
		return nil, err
	}

	var results []flowgo.BlockEvents

	for i := startHeight; i <= endHeight; i++ {
		block, err := a.store.GetBlockByHeight(i)
		if err != nil {
			continue
		}
		events := a.store.EventsByName(block.Height, block.ID(), eventType)
		results = append(results, flowgo.BlockEvents{
			BlockID:        block.ID(),
			BlockHeight:    block.Height,
			BlockTimestamp: block.Timestamp,
			Events:         events,
		})
	}

	a.logger.Debug().Fields(map[string]any{
		"eventType":   eventType,
		"startHeight": startHeight,
		"endHeight":   endHeight,
	}).Msg("🎁  GetEventsForHeightRange called")

	return results, nil
}

func (a *AccessAdapter) GetEventsForBlockIDs(
	_ context.Context,
	eventType string,
	blockIDs []flowgo.Identifier,
	requiredEventEncodingVersion entities.EventEncodingVersion,
) ([]flowgo.BlockEvents, error) {

	eventType, err := convert.EventType(eventType)
	if err != nil {
		return nil, err
	}

	var results []flowgo.BlockEvents

	for _, blockID := range blockIDs {
		block, err := a.store.GetBlockById(blockID)
		if err != nil {
			continue
		}
		events := a.store.EventsByName(block.Height, blockID, eventType)
		results = append(results, flowgo.BlockEvents{
			BlockID:        block.ID(),
			BlockHeight:    block.Height,
			BlockTimestamp: block.Timestamp,
			Events:         events,
		})
	}

	a.logger.Debug().Fields(map[string]any{
		"eventType": eventType,
	}).Msg("🎁  GetEventsForBlockIDs called")

	return results, nil
}

func (a *AccessAdapter) GetLatestProtocolStateSnapshot(_ context.Context) ([]byte, error) {
	return nil, nil
}

func (a *AccessAdapter) GetProtocolStateSnapshotByBlockID(_ context.Context, _ flowgo.Identifier) ([]byte, error) {
	return nil, nil
}

func (a *AccessAdapter) GetProtocolStateSnapshotByHeight(_ context.Context, _ uint64) ([]byte, error) {
	return nil, nil
}

func (a *AccessAdapter) GetExecutionResultForBlockID(_ context.Context, _ flowgo.Identifier) (*flowgo.ExecutionResult, error) {
	return nil, nil
}

func (a *AccessAdapter) GetExecutionResultByID(_ context.Context, _ flowgo.Identifier) (*flowgo.ExecutionResult, error) {
	return nil, nil
}

func (a *AccessAdapter) GetSystemTransaction(_ context.Context, _ flowgo.Identifier) (*flowgo.TransactionBody, error) {
	return nil, nil
}

func (a *AccessAdapter) GetSystemTransactionResult(_ context.Context, _ flowgo.Identifier, _ entities.EventEncodingVersion) (*access.TransactionResult, error) {
	return nil, nil
}

func (a *AccessAdapter) GetAccountBalanceAtLatestBlock(ctx context.Context, address flowgo.Address) (uint64, error) {

	account, err := a.GetAccount(ctx, address)
	if err != nil {
		return 0, convertError(err, codes.Internal)
	}

	a.logger.Debug().
		Stringer("address", address).
		Msg("👤  GetAccountBalanceAtLatestBlock called")

	return account.Balance, nil
}

func (a *AccessAdapter) GetAccountBalanceAtBlockHeight(ctx context.Context, address flowgo.Address, height uint64) (uint64, error) {
	account, err := a.GetAccountAtBlockHeight(ctx, address, height)
	if err != nil {
		return 0, convertError(err, codes.Internal)
	}

	a.logger.Debug().
		Stringer("address", address).
		Uint64("height", height).
		Msg("👤  GetAccountBalanceAtBlockHeight called")

	return account.Balance, nil
}

func (a *AccessAdapter) GetAccountKeyAtLatestBlock(ctx context.Context, address flowgo.Address, keyIndex uint32) (*flowgo.AccountPublicKey, error) {
	account, err := a.GetAccount(ctx, address)
	if err != nil {
		return nil, convertError(err, codes.Internal)
	}

	for _, key := range account.Keys {
		if key.Index == keyIndex {
			return &key, nil
		}
	}

	a.logger.Debug().
		Stringer("address", address).
		Uint32("keyIndex", keyIndex).
		Msg("👤  GetAccountKeyAtLatestBlock called")

	return nil, status.Errorf(codes.NotFound, "failed to get account key by index: %d", keyIndex)
}

func (a *AccessAdapter) GetAccountKeyAtBlockHeight(ctx context.Context, address flowgo.Address, keyIndex uint32, height uint64) (*flowgo.AccountPublicKey, error) {
	account, err := a.GetAccountAtBlockHeight(ctx, address, height)
	if err != nil {
		return nil, convertError(err, codes.Internal)
	}

	for _, key := range account.Keys {
		if key.Index == keyIndex {
			return &key, nil
		}
	}

	a.logger.Debug().
		Stringer("address", address).
		Uint32("keyIndex", keyIndex).
		Uint64("height", height).
		Msg("👤  GetAccountKeyAtBlockHeight called")

	return nil, status.Errorf(codes.NotFound, "failed to get account key by index: %d", keyIndex)
}

func (a *AccessAdapter) GetAccountKeysAtLatestBlock(ctx context.Context, address flowgo.Address) ([]flowgo.AccountPublicKey, error) {
	account, err := a.GetAccount(ctx, address)
	if err != nil {
		return nil, convertError(err, codes.Internal)
	}

	a.logger.Debug().
		Stringer("address", address).
		Msg("👤  GetAccountKeysAtLatestBlock called")

	return account.Keys, nil
}

func (a *AccessAdapter) GetAccountKeysAtBlockHeight(ctx context.Context, address flowgo.Address, height uint64) ([]flowgo.AccountPublicKey, error) {
	account, err := a.GetAccountAtBlockHeight(ctx, address, height)
	if err != nil {
		return nil, convertError(err, codes.Internal)
	}

	a.logger.Debug().
		Stringer("address", address).
		Uint64("height", height).
		Msg("👤  GetAccountKeysAtBlockHeight called")

	return account.Keys, nil
}

func (a *AccessAdapter) GetTransactionResultByIndex(
	_ context.Context,
	blockID flowgo.Identifier,
	index uint32,
	requiredEventEncodingVersion entities.EventEncodingVersion,
) (*access.TransactionResult, error) {
	return nil, fmt.Errorf("not implemented")
}

func (a *AccessAdapter) GetTransactionsByBlockID(_ context.Context, blockID flowgo.Identifier) ([]*flowgo.TransactionBody, error) {
	return nil, fmt.Errorf("not implemented")
}

func (a *AccessAdapter) GetTransactionResultsByBlockID(
	_ context.Context,
	blockID flowgo.Identifier,
	requiredEventEncodingVersion entities.EventEncodingVersion,
) ([]*access.TransactionResult, error) {
	return nil, fmt.Errorf("not implemented")

}

func (a *AccessAdapter) SendTransaction(_ context.Context, tx *flowgo.TransactionBody) error {
	a.logger.Debug().
		Str("txID", tx.ID().String()).
		Msg(`✉️   Transaction simulated`)

	block, err := a.store.GetBlockById(tx.ReferenceBlockID)
	if err != nil {
		return err
	}
	snapshot := a.store.LedgerSnapshot(block.Height)

	proc := fvm.Transaction(tx, 0)
	debugger := interpreter.NewDebugger()

	context := fvm.NewContext(
		fvm.WithBlockHeader(block),
		fvm.WithBlocks(a.store),
		fvm.WithCadenceLogging(true),
		fvm.WithAuthorizationChecksEnabled(false),
		fvm.WithSequenceNumberCheckAndIncrementEnabled(false),
		fvm.WithEVMEnabled(true),
		fvm.WithReusableCadenceRuntimePool(
			reusableRuntime.NewReusableCadenceRuntimePool(
				0,
				runtime.Config{
					Debugger:           debugger,
					TracingEnabled:     false,
					AttachmentsEnabled: true,
				},
			),
		),
	)

	blockDatabase := fvmStorage.NewBlockDatabase(snapshot, 0, nil)
	txnState, err := blockDatabase.NewTransaction(0, fvmState.DefaultParameters())
	if err != nil {
		panic(err)
	}
	executor := proc.NewExecutor(context, txnState)

	var wg sync.WaitGroup
	wg.Add(1)

	debugger.RequestPause()

	go func() {
		defer wg.Done()
		fmt.Println("before run")

		err = fvm.Run(executor)
		fmt.Println("after run")
	}()

	for {
		fmt.Println("d")
		stop := debugger.Next()
		fmt.Println(stop.Statement.String())
		if debugger.CurrentActivation(stop.Interpreter).Depth == 0 {
			break
		}
	}

	wg.Wait()
	if err != nil {
		return err
	}
	txId := tx.ID()

	output := executor.Output()
	fmt.Println("output", output)
	fmt.Println("logs", output.Logs)

	txnState.Finalize()
	resultSnapshot, err := txnState.Commit()
	if err != nil {
		fmt.Println("err", err)
	}

	blockResources := make(map[uint64]*indexer.Resource)

	for _, w := range resultSnapshot.UpdatedRegisters() {
		a.store.StorageForHeight(block.Height).Index().IndexPayload2(blockResources, w, block.Height, false)
	}

	logs := strings.Join(output.Logs, "\n")

	tx.Script = []byte(fmt.Sprintf("%s\n\nLogs\n\n%s", string(tx.Script), logs))
	tx.Script = []byte(fmt.Sprintf("%s\n\nComputation Details\n\n%v", string(tx.Script), output.ComputationIntensities))

	stateChanges := ""
	for _, v := range blockResources {
		stateChanges = fmt.Sprintf("%s%v", stateChanges, v)
	}

	tx.Script = []byte(fmt.Sprintf("%s\n\nResource State Changes\n\n%v", string(tx.Script), stateChanges))

	a.txCache.Add(txId, TemporaryTransactionResult{
		Transaction: *tx,
		Output:      output,
		BlockHeight: block.Height,
		BlockID:     block.ID(),
	})

	return nil

}

func (a *AccessAdapter) GetNodeVersionInfo(
	_ context.Context,
) (
	*access.NodeVersionInfo,
	error,
) {
	return &access.NodeVersionInfo{}, nil
}

func (a *AccessAdapter) SubscribeBlocksFromStartBlockID(ctx context.Context, startBlockID flowgo.Identifier, blockStatus flowgo.BlockStatus) subscription.Subscription {
	return nil
}

func (a *AccessAdapter) SubscribeBlocksFromStartHeight(ctx context.Context, startHeight uint64, blockStatus flowgo.BlockStatus) subscription.Subscription {
	return nil
}

func (a *AccessAdapter) SubscribeBlocksFromLatest(ctx context.Context, blockStatus flowgo.BlockStatus) subscription.Subscription {
	return nil
}

func (a *AccessAdapter) SubscribeBlockHeadersFromStartBlockID(ctx context.Context, startBlockID flowgo.Identifier, blockStatus flowgo.BlockStatus) subscription.Subscription {
	return nil
}

func (a *AccessAdapter) SubscribeBlockHeadersFromStartHeight(ctx context.Context, startHeight uint64, blockStatus flowgo.BlockStatus) subscription.Subscription {
	return nil
}

func (a *AccessAdapter) SubscribeBlockHeadersFromLatest(ctx context.Context, blockStatus flowgo.BlockStatus) subscription.Subscription {
	return nil
}

func (a *AccessAdapter) SubscribeBlockDigestsFromStartBlockID(ctx context.Context, startBlockID flowgo.Identifier, blockStatus flowgo.BlockStatus) subscription.Subscription {
	return nil
}

func (a *AccessAdapter) SubscribeBlockDigestsFromStartHeight(ctx context.Context, startHeight uint64, blockStatus flowgo.BlockStatus) subscription.Subscription {
	return nil
}

func (a *AccessAdapter) SubscribeBlockDigestsFromLatest(ctx context.Context, blockStatus flowgo.BlockStatus) subscription.Subscription {
	return nil
}

func (a *AccessAdapter) SubscribeTransactionStatuses(ctx context.Context, tx *flowgo.TransactionBody, _ entities.EventEncodingVersion) subscription.Subscription {
	return nil
}
