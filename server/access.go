package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/bluesign/tinyAN/storage"
	"github.com/onflow/cadence/runtime"
	"github.com/onflow/flow-go/engine/execution/computation/query"
	"github.com/onflow/flow-go/fvm/environment"
	"github.com/onflow/flow/protobuf/go/flow/access"
	"github.com/onflow/flow/protobuf/go/flow/entities"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/onflow/flow-go/engine/common/rpc/convert"
	"github.com/onflow/flow-go/fvm"
	reusableRuntime "github.com/onflow/flow-go/fvm/runtime"
	"github.com/onflow/flow-go/fvm/storage/derived"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/module/metrics"
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
	store         *storage.ProtocolStorage
	client        access.AccessAPIClient
	queryExecutor *query.QueryExecutor
}

type TransactionContainer struct {
	TransactionResult flow.LightTransactionResult
	BlockID           string
	BlockHeight       uint64
	CollectionID      string
}

var _ environment.Blocks = (*Blocks)(nil)

type Blocks struct {
	storage *storage.ProtocolStorage
}

// ByHeightFrom implements the fvm/env/blocks interface
func (b *Blocks) ByHeightFrom(height uint64, header *flow.Header) (*flow.Header, error) {

	fmt.Println("GetBlockByHeight")
	if header.Height == height {
		return header, nil
	}

	return &flow.Header{
		Height: 0,
	}, nil
}

func NewBlocks(storage *storage.ProtocolStorage) *Blocks {
	return &Blocks{
		storage: storage,
	}
}

type EntropyProviderPerBlockProvider struct {
	// AtBlockID returns an entropy provider at the given block ID.
}

func (e *EntropyProviderPerBlockProvider) RandomSource() ([]byte, error) {
	return []byte{42}, nil
}

func (e *EntropyProviderPerBlockProvider) AtBlockID(blockID flow.Identifier) environment.EntropyProvider {
	return e
}

// HandlerOption is used to hand over optional constructor parameters
type HandlerOption func(*Handler)

var _ access.AccessAPIServer = (*Handler)(nil)

func NewHandler(chainID flow.ChainID, store *storage.ProtocolStorage, client access.AccessAPIClient, options ...HandlerOption) *Handler {

	blocks := NewBlocks(store)

	var vm fvm.VM
	vm = fvm.NewVirtualMachine()

	fvmOptions := []fvm.Option{
		fvm.WithReusableCadenceRuntimePool(
			reusableRuntime.NewReusableCadenceRuntimePool(
				0,
				runtime.Config{
					TracingEnabled:        false,
					AccountLinkingEnabled: true,
					// Attachments are enabled everywhere except for Mainnet
					AttachmentsEnabled: chainID != flow.Mainnet,
					// Capability Controllers are enabled everywhere except for Mainnet
					CapabilityControllersEnabled: chainID != flow.Mainnet,
				},
			),
		),
		fvm.WithBlocks(blocks),
		fvm.WithChain(chainID.Chain()),
	}

	vmCtx := fvm.NewContext(fvmOptions...)
	derivedChainData, err := derived.NewDerivedChainData(10)
	if err != nil {
		log.Panic().Msgf("cannot create derived data cache: %w", err)
	}

	queryExecutor := query.NewQueryExecutor(
		query.NewDefaultConfig(),
		log.Logger,
		&metrics.NoopCollector{}, // TODO: add metrics
		vm,
		vmCtx,
		derivedChainData,
		&EntropyProviderPerBlockProvider{},
	)

	h := &Handler{
		store:         store,
		client:        client,
		queryExecutor: queryExecutor,
	}

	for _, opt := range options {
		opt(h)
	}
	return h
}

// Ping the Access API server for a response.
func (h *Handler) Ping(_ context.Context, _ *access.PingRequest) (*access.PingResponse, error) {
	return &access.PingResponse{}, nil
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

func (h *Handler) GetNetworkParameters(
	ctx context.Context,
	req *access.GetNetworkParametersRequest,
) (*access.GetNetworkParametersResponse, error) {
	return h.client.GetNetworkParameters(ctx, req)

}

// GetLatestBlockHeader gets the latest sealed block header.
func (h *Handler) GetLatestBlockHeader(
	ctx context.Context,
	_ *access.GetLatestBlockHeaderRequest,
) (*access.BlockHeaderResponse, error) {

	blockHeight := h.store.LastHeight()
	parentHeight := blockHeight - 1

	blockID := h.store.BlockId(blockHeight)
	parentID := h.store.BlockId(parentHeight)
	collections := h.store.CollectionsAtBlock(blockID)

	return h.blockHeaderResponse(blockID, parentID, blockHeight, collections)
}

// GetBlockHeaderByHeight gets a block header by height.
func (h *Handler) GetBlockHeaderByHeight(
	ctx context.Context,
	req *access.GetBlockHeaderByHeightRequest,
) (*access.BlockHeaderResponse, error) {
	finalHeight := h.store.LastHeight()

	blockHeight := req.GetHeight()
	if blockHeight > finalHeight {
		return nil, status.Error(codes.NotFound, "Not Found")
	}
	parentHeight := blockHeight - 1

	blockID := h.store.BlockId(blockHeight)
	if blockID == flow.ZeroID {
		return nil, status.Error(codes.NotFound, "Not Found")
	}

	parentID := h.store.BlockId(parentHeight)
	if parentID == flow.ZeroID {
		return nil, status.Error(codes.NotFound, "Not Found")
	}

	collections := h.store.CollectionsAtBlock(blockID)
	return h.blockHeaderResponse(blockID, parentID, blockHeight, collections)
}

// GetBlockHeaderByID gets a block header by ID.
func (h *Handler) GetBlockHeaderByID(
	ctx context.Context,
	req *access.GetBlockHeaderByIDRequest,
) (*access.BlockHeaderResponse, error) {
	blockID, _ := flow.ByteSliceToId(req.GetId())

	blockHeight := h.store.BlockHeight(blockID)
	if blockHeight == 0 {
		return nil, status.Error(codes.NotFound, "Not Found")
	}

	parentHeight := blockHeight - 1
	parentID := h.store.BlockId(parentHeight)
	if parentID != flow.ZeroID {
		return nil, status.Error(codes.NotFound, "Not Found")
	}

	collections := h.store.CollectionsAtBlock(blockID)
	return h.blockHeaderResponse(blockID, parentID, blockHeight, collections)
}

// GetLatestBlock gets the latest sealed block.
func (h *Handler) GetLatestBlock(
	ctx context.Context,
	_ *access.GetLatestBlockRequest,
) (*access.BlockResponse, error) {

	blockHeight := h.store.LastHeight()
	parentHeight := blockHeight - 1

	blockID := h.store.BlockId(blockHeight)
	parentID := h.store.BlockId(parentHeight)

	collections := h.store.CollectionsAtBlock(blockID)
	return h.blockResponse(blockID, parentID, blockHeight, collections)
}

// GetBlockByHeight gets a block by height.
func (h *Handler) GetBlockByHeight(
	ctx context.Context,
	req *access.GetBlockByHeightRequest,
) (*access.BlockResponse, error) {
	finalHeight := h.store.LastHeight()

	blockHeight := req.GetHeight()
	if blockHeight > finalHeight {
		return nil, status.Error(codes.NotFound, "Not Found")
	}
	parentHeight := blockHeight - 1

	blockID := h.store.BlockId(blockHeight)
	if blockID == flow.ZeroID {
		return nil, status.Error(codes.NotFound, "Not Found")
	}

	parentID := h.store.BlockId(parentHeight)
	if parentID == flow.ZeroID {
		return nil, status.Error(codes.NotFound, "Not Found")
	}

	collections := h.store.CollectionsAtBlock(blockID)

	return h.blockResponse(blockID, parentID, blockHeight, collections)
}

// GetBlockByID gets a block by ID.
func (h *Handler) GetBlockByID(
	ctx context.Context,
	req *access.GetBlockByIDRequest,
) (*access.BlockResponse, error) {

	blockID, _ := flow.ByteSliceToId(req.GetId())

	blockHeight := h.store.BlockHeight(blockID)
	if blockHeight == 0 {
		return nil, status.Error(codes.NotFound, "Not Found")
	}

	parentHeight := blockHeight - 1
	parentID := h.store.BlockId(parentHeight)
	if parentID != flow.ZeroID {
		return nil, status.Error(codes.NotFound, "Not Found")
	}

	collections := h.store.CollectionsAtBlock(blockID)
	return h.blockResponse(blockID, parentID, blockHeight, collections)
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

// SendTransaction submits a transaction to the network.
func (h *Handler) SendTransaction(
	ctx context.Context,
	req *access.SendTransactionRequest,
) (*access.SendTransactionResponse, error) {
	return h.client.SendTransaction(ctx, req)
}

// GetTransaction gets a transaction by ID.
func (h *Handler) GetTransaction(
	ctx context.Context,
	req *access.GetTransactionRequest,
) (*access.TransactionResponse, error) {
	metadata := h.buildMetadataResponse()
	transactionId, _ := flow.ByteSliceToId(req.GetId())

	tx := h.store.TransactionById(transactionId)
	msg := convert.TransactionToMessage(tx)

	return &access.TransactionResponse{
		Transaction: msg,
		Metadata:    metadata,
	}, nil
}

// GetTransactionResult gets a transaction by ID.
func (h *Handler) GetTransactionResult(
	ctx context.Context,
	req *access.GetTransactionRequest,
) (*access.TransactionResultResponse, error) {
	metadata := h.buildMetadataResponse()

	transactionId, _ := flow.ByteSliceToId(req.GetId())
	transaction := h.store.TransactionById(transactionId)

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
	blockHeight := h.store.LastHeight()

	address := flow.BytesToAddress(req.GetAddress())

	account, err := h.queryExecutor.GetAccount(
		ctx,
		address,
		&flow.Header{},
		h.store.StorageSnapshot(blockHeight),
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

	address := flow.BytesToAddress(req.GetAddress())
	blockHeight := h.store.LastHeight()

	account, err := h.queryExecutor.GetAccount(
		ctx,
		address,
		&flow.Header{},
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

	address := flow.BytesToAddress(req.GetAddress())
	blockHeight := req.GetBlockHeight()
	account, err := h.queryExecutor.GetAccount(
		ctx,
		address,
		&flow.Header{},
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

// ExecuteScriptAtLatestBlock executes a script at a the latest block.
func (h *Handler) ExecuteScriptAtLatestBlock(
	ctx context.Context,
	req *access.ExecuteScriptAtLatestBlockRequest,
) (*access.ExecuteScriptResponse, error) {

	script := req.GetScript()
	args := req.GetArguments()
	blockHeight := h.store.LastHeight()

	encodedValue, err := h.queryExecutor.ExecuteScript(
		ctx,
		script,
		args,
		&flow.Header{},
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

	encodedValue, err := h.queryExecutor.ExecuteScript(
		ctx,
		script,
		args,
		&flow.Header{},
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

	encodedValue, err := h.queryExecutor.ExecuteScript(
		ctx,
		script,
		args,
		&flow.Header{},
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

func (h *Handler) blockHeaderResponse(id flow.Identifier, parentId flow.Identifier, height uint64, collections []flow.Identifier) (*access.BlockHeaderResponse, error) {
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

	msg := &entities.BlockHeader{
		Id:        Bytes(id),
		Height:    height,
		ParentId:  Bytes(parentId),
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
