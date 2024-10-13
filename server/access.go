package server

/*
import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/bluesign/tinyAN/storage"
	"github.com/onflow/cadence/runtime"
	"github.com/onflow/flow-go/engine/common/rpc/convert"
	"github.com/onflow/flow-go/fvm"
	"github.com/onflow/flow-go/fvm/environment"
	"github.com/onflow/flow-go/fvm/evm/debug"
	reusableRuntime "github.com/onflow/flow-go/fvm/runtime"
	fvmStorage "github.com/onflow/flow-go/fvm/storage"
	fvmState "github.com/onflow/flow-go/fvm/storage/state"
	"github.com/onflow/flow-go/fvm/tracing"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/module"
	mTrace "github.com/onflow/flow-go/module/trace"
	flowStorage "github.com/onflow/flow-go/storage"
	"github.com/onflow/flow/protobuf/go/flow/access"
	"github.com/onflow/flow/protobuf/go/flow/entities"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Bytes(v interface{}) []byte {
	switch i := v.(type) {
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

type Handler struct {
	blocks   *Blocks
	store    *storage.HeightBasedStorage
	client   access.AccessAPIClient
	executor Executor
}

var _ access.AccessAPIServer = (*Handler)(nil)

func (h *Handler) GetFullCollectionByID(ctx context.Context, request *access.GetFullCollectionByIDRequest) (*access.FullCollectionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (h *Handler) GetAccountBalanceAtLatestBlock(ctx context.Context, request *access.GetAccountBalanceAtLatestBlockRequest) (*access.AccountBalanceResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (h *Handler) GetAccountBalanceAtBlockHeight(ctx context.Context, request *access.GetAccountBalanceAtBlockHeightRequest) (*access.AccountBalanceResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (h *Handler) GetAccountKeysAtLatestBlock(ctx context.Context, request *access.GetAccountKeysAtLatestBlockRequest) (*access.AccountKeysResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (h *Handler) GetAccountKeysAtBlockHeight(ctx context.Context, request *access.GetAccountKeysAtBlockHeightRequest) (*access.AccountKeysResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (h *Handler) GetAccountKeyAtLatestBlock(ctx context.Context, request *access.GetAccountKeyAtLatestBlockRequest) (*access.AccountKeyResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (h *Handler) GetAccountKeyAtBlockHeight(ctx context.Context, request *access.GetAccountKeyAtBlockHeightRequest) (*access.AccountKeyResponse, error) {
	//TODO implement me
	panic("implement me")
}

type TransactionContainer struct {
	TransactionResult flow.LightTransactionResult
	BlockID           string
	BlockHeight       uint64
	CollectionID      string
}

var _ environment.Blocks = (*Blocks)(nil)

type Blocks struct {
	store *storage.HeightBasedStorage
}

// ByHeightFrom implements the fvm/env/blocks interface
func (b *Blocks) ByHeightFrom(height uint64, header *flow.Header) (*flow.Header, error) {

	fmt.Println("getBlockByHeight", height)
	if header.Height == height {
		fmt.Println("height == header.Height", header)
		return header, nil
	}

	if header.Height > height {
		fmt.Println("height > header.Height")
		return nil, flowStorage.ErrNotFound
	}

	blocks := b.store.storageForHeight(height).Blocks()

	blockHeader, err := blocks.getBlockByHeight(height)
	if err != nil {
		//not found
		return nil, flowStorage.ErrNotFound
	}

	return blockHeader, nil
}

func NewBlocks(storage *storage.HeightBasedStorage) *Blocks {
	return &Blocks{
		store: storage,
	}
}

// HandlerOption is used to hand over optional constructor parameters
type HandlerOption func(*Handler)

var _ access.AccessAPIServer = (*Handler)(nil)

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

func NewHandler(chainID flow.ChainID, store *storage.HeightBasedStorage, options ...HandlerOption) *Handler {

	blocks := NewBlocks(store)

	executor := &ScriptExecutor{}
	executor.Setup(blocks, string(chainID))

	h := &Handler{
		blocks:   blocks,
		store:    store,
		executor: executor,
	}

	for _, opt := range options {
		opt(h)
	}
	return h
}

// GetNodeVersionInfo gets node version information such as semver, commit, sporkID, protocolVersion, etc
func (h *Handler) GetNodeVersionInfo(
	_ context.Context,
	_ *access.GetNodeVersionInfoRequest,
) (*access.GetNodeVersionInfoResponse, error) {
	return &access.GetNodeVersionInfoResponse{
		Info: &entities.NodeVersionInfo{
			Semver:          "0.0.1",
			Commit:          "",
			SporkId:         []byte{},
			ProtocolVersion: 0,
		},
	}, nil
}

// GetCollectionByID gets a collection by ID.
func (h *Handler) GetCollectionByID(
	ctx context.Context,
	req *access.GetCollectionByIDRequest,
) (*access.CollectionResponse, error) {
	metadata := h.buildMetadataResponse()
	collectionID, _ := flow.ByteSliceToId(req.GetId())

	transactions := h.store.TransactionsAtCollection(collectionID)

	results := make([][]byte, len(transactions))
	for i, item := range transactions {
		results[i] = Bytes(item)
	}

	return &access.CollectionResponse{
		Collection: &entities.Collection{
			Id:             req.GetId(),
			TransactionIds: results,
		},
		Metadata: metadata,
	}, nil
}

type TemporaryTransactionResult struct {
	Transaction flow.TransactionBody
	Output      fvm.ProcedureOutput
	BlockHeight uint64
	BlockID     flow.Identifier
}

var txresults map[flow.Identifier]TemporaryTransactionResult

var _ module.Tracer = &myTracer{}

type myTracer struct {
	Traces []string
}

// BlockRootSpan implements module.Tracer.
func (m *myTracer) BlockRootSpan(blockID flow.Identifier) trace.Span {
	return nil
}

// Done implements module.Tracer.
func (m *myTracer) Done() <-chan struct{} {
	panic("unimplemented")
}

// Ready implements module.Tracer.
func (m *myTracer) Ready() <-chan struct{} {
	panic("unimplemented")
}

// ShouldSample implements module.Tracer.
func (m *myTracer) ShouldSample(entityID flow.Identifier) bool {
	return true
}

// StartBlockSpan implements module.Tracer.
func (m *myTracer) StartBlockSpan(ctx context.Context, blockID flow.Identifier, spanName mTrace.SpanName, opts ...trace.SpanStartOption) (trace.Span, context.Context) {
	span := tracing.NewMockTracerSpan()
	return span, ctx
}

// StartCollectionSpan implements module.Tracer.
func (m *myTracer) StartCollectionSpan(ctx context.Context, collectionID flow.Identifier, spanName mTrace.SpanName, opts ...trace.SpanStartOption) (trace.Span, context.Context) {
	span := tracing.NewMockTracerSpan()
	return span, ctx
}

// StartSampledSpanFromParent implements module.Tracer.
func (m *myTracer) StartSampledSpanFromParent(parentSpan trace.Span, entityID flow.Identifier, operationName mTrace.SpanName, opts ...trace.SpanStartOption) trace.Span {
	span := tracing.NewMockTracerSpan()
	return span
}

// StartSpanFromContext implements module.Tracer.
func (m *myTracer) StartSpanFromContext(ctx context.Context, operationName mTrace.SpanName, opts ...trace.SpanStartOption) (trace.Span, context.Context) {
	span := tracing.NewMockTracerSpan()
	return span, ctx
}

// StartSpanFromParent implements module.Tracer.
func (m *myTracer) StartSpanFromParent(parentSpan trace.Span, operationName mTrace.SpanName, opts ...trace.SpanStartOption) trace.Span {
	m.Traces = append(m.Traces, string(operationName))
	span := tracing.NewMockTracerSpan()

	return span
}

// WithSpanFromContext implements module.Tracer.
func (m *myTracer) WithSpanFromContext(ctx context.Context, operationName mTrace.SpanName, f func(), opts ...trace.SpanStartOption) {
	m.Traces = append(m.Traces, string(operationName))
}

type EVMTraceListener struct {
	Data map[string]json.RawMessage
}

// Upload implements debug.Uploader.
func (e *EVMTraceListener) Upload(id string, data json.RawMessage) error {
	e.Data[id] = data
	return nil
}

var _ debug.Uploader = &EVMTraceListener{}

// SendTransaction submits a transaction to the network.
func (h *Handler) SendTransaction(
	ctx context.Context,
	req *access.SendTransactionRequest,
) (*access.SendTransactionResponse, error) {


// GetTransactionResult gets a transaction by ID.
func (h *Handler) GetTransactionResult(
	ctx context.Context,
	req *access.GetTransactionRequest,
) (*access.TransactionResultResponse, error) {
	metadata := h.buildMetadataResponse()
	fmt.Println("GetTransactionResult")
	transactionId, _ := flow.ByteSliceToId(req.GetId())
	transaction := h.store.TransactionById(transactionId)

	if txResult, ok := txresults[transactionId]; ok {
		code := 0
		message := ""
		if txResult.Output.Err != nil {
			code = 1
			message = txResult.Output.Err.Error()
		}
		return &access.TransactionResultResponse{
			Status:        entities.TransactionStatus_SEALED,
			StatusCode:    uint32(code),
			ErrorMessage:  message,
			Events:        convert.EventsToMessages(txResult.Output.Events),
			BlockId:       txResult.BlockID[:],
			TransactionId: Bytes(transactionId),
			BlockHeight:   txResult.BlockHeight,
			Metadata:      metadata,
		}, nil

	}

	if transaction.ReferenceBlockID == flow.ZeroID {
		return nil, status.Error(codes.NotFound, "Not Found 1")
	}

	blockId := flow.ZeroID
	var err error
	requestBlockId := req.GetBlockId()
	if requestBlockId != nil {
		blockId, err = convert.BlockID(requestBlockId)
		if err != nil {
			return nil, err
		}
	}

	collectionId := flow.ZeroID
	requestCollectionId := req.GetCollectionId()
	if requestCollectionId != nil {
		collectionId, err = convert.CollectionID(requestCollectionId)
		if err != nil {
			return nil, err
		}
	}

	if collectionId != flow.ZeroID && blockId != flow.ZeroID {
		//TODO: implement
		return h.client.GetTransactionResult(ctx, req)
	}

	blockId, blockHeight, collectionId, transactionResult := h.store.TransactionResultById(transactionId)
	if transactionResult.TransactionID == flow.ZeroID {
		return nil, status.Error(codes.NotFound, "Not Found 2")
	}

	events := h.store.Events(blockId, collectionId, transactionId)

	statusCode := uint32(0)
	errorMessage := ""
	if transactionResult.Failed {
		statusCode = 1
		errorMessage = "Some Error"
	}

	message := &access.TransactionResultResponse{
		Status:        entities.TransactionStatus_SEALED,
		StatusCode:    statusCode,
		ErrorMessage:  errorMessage,
		Events:        convert.EventsToMessages(events),
		BlockId:       Bytes(blockId),
		TransactionId: req.GetId(),
		BlockHeight:   blockHeight,
	}
	message.Metadata = metadata

	return message, nil
}

func (h *Handler) GetTransactionResultsByBlockID(
	ctx context.Context,
	req *access.GetTransactionsByBlockIDRequest,
) (*access.TransactionResultsResponse, error) {
	message := &access.TransactionResultsResponse{}
	metadata := h.buildMetadataResponse()

	blockId, _ := flow.ByteSliceToId(req.GetBlockId())
	blockHeight := h.store.BlockHeight(blockId)

	var results []*access.TransactionResultResponse

	collections := h.store.CollectionsAtBlock(blockId)
	for _, collectionId := range collections {
		transactions := h.store.TransactionsAtCollection(collectionId)
		for _, transactionId := range transactions {
			transactionResult := h.store.TransactionResult(blockId, collectionId, transactionId)

			statusCode := uint32(0)
			errorMessage := ""
			if transactionResult.Failed {
				statusCode = 1
				errorMessage = "Error Message is not available in TinyAN"
			}
			events := h.store.Events(blockId, collectionId, transactionResult.TransactionID)

			results = append(results, &access.TransactionResultResponse{
				Status:        entities.TransactionStatus_SEALED,
				StatusCode:    statusCode,
				ErrorMessage:  errorMessage,
				Events:        convert.EventsToMessages(events),
				BlockId:       Bytes(blockId),
				TransactionId: Bytes(transactionResult.TransactionID),
				BlockHeight:   blockHeight,
			})
		}
	}
	message.Metadata = metadata
	message.TransactionResults = results

	return message, nil
}

func (h *Handler) GetTransactionsByBlockID(
	ctx context.Context,
	req *access.GetTransactionsByBlockIDRequest,
) (*access.TransactionsResponse, error) {

	message := &access.TransactionsResponse{}
	metadata := h.buildMetadataResponse()

	blockId, _ := flow.ByteSliceToId(req.GetBlockId())

	var results []*entities.Transaction

	collections := h.store.CollectionsAtBlock(blockId)
	for _, collectionId := range collections {
		transactions := h.store.TransactionsAtCollection(collectionId)
		for _, transactionId := range transactions {
			transaction := h.store.Transaction(blockId, collectionId, transactionId)
			results = append(results, convert.TransactionToMessage(transaction))
		}
	}
	message.Metadata = metadata
	message.Transactions = results

	return message, nil
}

// GetTransactionResultByIndex gets a transaction at a specific index for in a block that is executed,
// pending or finalized transactions return errors
func (h *Handler) GetTransactionResultByIndex(
	ctx context.Context,
	req *access.GetTransactionByIndexRequest,
) (*access.TransactionResultResponse, error) {
	//TODO: implement me
	return h.client.GetTransactionResultByIndex(ctx, req)
}

// GetAccount returns an account by address at the latest sealed block.
func (h *Handler) GetAccount(
	ctx context.Context,
	req *access.GetAccountRequest,
) (*access.GetAccountResponse, error) {
	protocol := h.store.latest().Protocol()
	ledger := h.store.latest().Ledger()
	blockHeight := protocol.LastProcessedHeight()

	account, err := h.executor.GetAccount(
		ctx,
		req.GetAddress(),
		blockHeight,
		ledger.StorageSnapshot(blockHeight),
	)

	if err != nil {
		return nil, err
	}

	msg, err := convert.AccountToMessage(account)
	if err != nil {
		return nil, err
	}

	return &access.GetAccountResponse{
		Account: msg,
	}, nil

}

// GetAccountAtLatestBlock returns an account by address at the latest sealed block.
func (h *Handler) GetAccountAtLatestBlock(
	ctx context.Context,
	req *access.GetAccountAtLatestBlockRequest,
) (*access.AccountResponse, error) {

	blockHeight := h.store.LastHeight()

	account, err := h.executor.GetAccount(
		ctx,
		req.GetAddress(),
		blockHeight,
		h.store.StorageSnapshot(blockHeight),
	)

	if err != nil {
		return nil, err
	}

	msg, err := convert.AccountToMessage(account)
	if err != nil {
		return nil, err
	}

	return &access.AccountResponse{
		Account: msg,
	}, nil

}

func (h *Handler) GetAccountAtBlockHeight(
	ctx context.Context,
	req *access.GetAccountAtBlockHeightRequest,
) (*access.AccountResponse, error) {

	account, err := h.executor.GetAccount(
		ctx,
		req.GetAddress(),
		req.GetBlockHeight(),
		h.store.StorageSnapshot(req.GetBlockHeight()),
	)

	if err != nil {
		return nil, err
	}

	msg, err := convert.AccountToMessage(account)
	if err != nil {
		return nil, err
	}

	return &access.AccountResponse{
		Account: msg,
	}, nil

}

// ExecuteScriptAtLatestBlock executes a script at a the latest block.
func (h *Handler) ExecuteScriptAtLatestBlock(
	ctx context.Context,
	req *access.ExecuteScriptAtLatestBlockRequest,
) (*access.ExecuteScriptResponse, error) {

	script := req.GetScript()
	args := req.GetArguments()
	blockHeight := h.store.LastHeight()

	encodedValue, err := h.executor.ExecuteScript(
		ctx,
		script,
		args,
		blockHeight,
		h.store.StorageSnapshot(blockHeight),
	)

	return &access.ExecuteScriptResponse{
		Value: encodedValue,
	}, err
}

// ExecuteScriptAtBlockHeight executes a script at a specific block height.
func (h *Handler) ExecuteScriptAtBlockHeight(
	ctx context.Context,
	req *access.ExecuteScriptAtBlockHeightRequest,
) (*access.ExecuteScriptResponse, error) {

	script := req.GetScript()
	args := req.GetArguments()
	blockHeight := req.GetBlockHeight()

	encodedValue, err := h.executor.ExecuteScript(
		ctx,
		script,
		args,
		blockHeight,
		h.store.StorageSnapshot(blockHeight),
	)

	return &access.ExecuteScriptResponse{
		Value: encodedValue,
	}, err

}

// ExecuteScriptAtBlockID executes a script at a specific block ID.
func (h *Handler) ExecuteScriptAtBlockID(
	ctx context.Context,
	req *access.ExecuteScriptAtBlockIDRequest,
) (*access.ExecuteScriptResponse, error) {

	script := req.GetScript()
	args := req.GetArguments()
	blockID, _ := flow.ByteSliceToId(req.GetBlockId())
	blockHeight := h.store.BlockHeight(blockID)

	encodedValue, err := h.executor.ExecuteScript(
		ctx,
		script,
		args,
		blockHeight,
		h.store.StorageSnapshot(blockHeight),
	)

	return &access.ExecuteScriptResponse{
		Value: encodedValue,
	}, err

}

// GetEventsForHeightRange returns events matching a query.
func (h *Handler) GetEventsForHeightRange(
	_ context.Context,
	req *access.GetEventsForHeightRangeRequest,
) (*access.EventsResponse, error) {
	metadata := h.buildMetadataResponse()

	eventType, err := convert.EventType(req.GetType())
	if err != nil {
		return nil, err
	}

	startHeight := req.GetStartHeight()
	endHeight := req.GetEndHeight()

	var results []flow.BlockEvents

	for i := startHeight; i <= endHeight; i++ {
		blockId := h.store.BlockId(i)
		if blockId == flow.ZeroID {
			fmt.Println("zeor")
			fmt.Println(blockId)
			continue
		}
		results = append(results, flow.BlockEvents{
			BlockID:        blockId,
			BlockHeight:    h.store.BlockHeight(blockId),
			BlockTimestamp: time.Now(),
			Events:         h.store.EventsByName(blockId, eventType),
		})
	}

	resultEvents, err := blockEventsToMessages(results)
	if err != nil {
		return nil, err
	}
	return &access.EventsResponse{
		Results:  resultEvents,
		Metadata: metadata,
	}, nil
}

// GetEventsForBlockIDs returns events matching a set of block IDs.
func (h *Handler) GetEventsForBlockIDs(
	_ context.Context,
	req *access.GetEventsForBlockIDsRequest,
) (*access.EventsResponse, error) {
	metadata := h.buildMetadataResponse()

	eventType, err := convert.EventType(req.GetType())
	if err != nil {
		return nil, err
	}

	blockIds, err := convert.BlockIDs(req.GetBlockIds())
	if err != nil {
		return nil, err
	}

	var results []flow.BlockEvents

	for _, blockId := range blockIds {
		if blockId == flow.ZeroID {
			break
		}
		results = append(results, flow.BlockEvents{
			BlockID:        blockId,
			BlockHeight:    h.store.BlockHeight(blockId),
			BlockTimestamp: time.Now(),
			Events:         h.store.EventsByName(blockId, eventType),
		})
	}
	resultEvents, err := blockEventsToMessages(results)
	if err != nil {
		return nil, err
	}

	return &access.EventsResponse{
		Results:  resultEvents,
		Metadata: metadata,
	}, nil
}

// GetLatestProtocolStateSnapshot returns the latest serializable Snapshot
func (h *Handler) GetLatestProtocolStateSnapshot(ctx context.Context, req *access.GetLatestProtocolStateSnapshotRequest) (*access.ProtocolStateSnapshotResponse, error) {
	return h.client.GetLatestProtocolStateSnapshot(ctx, req)

}

// GetExecutionResultForBlockID returns the latest received execution result for the given block ID.
// AN might receive multiple receipts with conflicting results for unsealed blocks.
// If this case happens, since AN is not able to determine which result is the correct one until the block is sealed, it has to pick one result to respond to this query. For now, we return the result from the latest received receipt.
func (h *Handler) GetExecutionResultForBlockID(ctx context.Context, req *access.GetExecutionResultForBlockIDRequest) (*access.ExecutionResultForBlockIDResponse, error) {
	return h.client.GetExecutionResultForBlockID(ctx, req)

}

func (h *Handler) GetExecutionResultByID(ctx context.Context, req *access.GetExecutionResultByIDRequest) (*access.ExecutionResultByIDResponse, error) {
	return h.client.GetExecutionResultByID(ctx, req)
}

func (h *Handler) blockResponse(id flow.Identifier, parentId flow.Identifier, height uint64, collections []flow.Identifier) (*access.BlockResponse, error) {
	metadata := h.buildMetadataResponse()

	var collectionGuarantees []*entities.CollectionGuarantee

	for index, collection := range collections {
		if index == len(collections)-1 {
			continue
		}
		collectionGuarantees = append(collectionGuarantees, &entities.CollectionGuarantee{
			CollectionId: Bytes(collection),
		})
	}

	msg := &entities.Block{
		Id:                   Bytes(id),
		Height:               height,
		ParentId:             Bytes(parentId),
		Timestamp:            timestamppb.Now(),
		CollectionGuarantees: collectionGuarantees,
		Signatures:           [][]byte{},
	}

	return &access.BlockResponse{
		Block:       msg,
		BlockStatus: entities.BlockStatus_BLOCK_SEALED,
		Metadata:    metadata,
	}, nil
}

func (h *Handler) blockHeaderResponse(block *flow.Header, collections []flow.Identifier) (*access.BlockHeaderResponse, error) {
	metadata := h.buildMetadataResponse()

	var collectionGuarantees []*entities.CollectionGuarantee

	for index, collection := range collections {
		if index == len(collections)-1 {
			continue
		}
		collectionGuarantees = append(collectionGuarantees, &entities.CollectionGuarantee{
			CollectionId: Bytes(collection),
		})
	}

	convert.BlockHeaderToMessage(block)

	msg := &entities.BlockHeader{
		Id:        Bytes(block.ID()),
		Height:    block.Height,
		ParentId:  Bytes(block.ParentID),
		Timestamp: timestamppb.Now(),
	}

	return &access.BlockHeaderResponse{
		Block:       msg,
		BlockStatus: entities.BlockStatus_BLOCK_SEALED,
		Metadata:    metadata,
	}, nil
}

// buildMetadataResponse builds and returns the metadata response object.
func (h *Handler) buildMetadataResponse() *entities.Metadata {
	var blockId []byte

	return &entities.Metadata{
		LatestFinalizedBlockId: blockId[:],
		LatestFinalizedHeight:  0,
		NodeId:                 []byte{},
	}
}

func blockEventsToMessages(blocks []flow.BlockEvents) ([]*access.EventsResponse_Result, error) {
	results := make([]*access.EventsResponse_Result, len(blocks))

	for i, block := range blocks {
		event, err := blockEventsToMessage(block)
		if err != nil {
			return nil, err
		}
		results[i] = event
	}

	return results, nil
}

func blockEventsToMessage(block flow.BlockEvents) (*access.EventsResponse_Result, error) {
	eventMessages := make([]*entities.Event, len(block.Events))
	for i, event := range block.Events {
		eventMessages[i] = convert.EventToMessage(event)
	}
	timestamp := timestamppb.New(block.BlockTimestamp)
	return &access.EventsResponse_Result{
		BlockId:        block.BlockID[:],
		BlockHeight:    block.BlockHeight,
		BlockTimestamp: timestamp,
		Events:         eventMessages,
	}, nil
}
*/
