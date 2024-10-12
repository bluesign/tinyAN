package client

import (
	"context"
	"fmt"
	"github.com/onflow/flow/protobuf/go/flow/entities"
	"io"
	"log"

	"github.com/onflow/flow-go/engine/common/rpc/convert"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/module/executiondatasync/execution_data"
	"github.com/onflow/flow/protobuf/go/flow/access"
	executiondata "github.com/onflow/flow/protobuf/go/flow/executiondata"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type BlockFollower struct {
	client access.AccessAPIClient
	chain  flow.Chain
}

type BlockDataResponse struct {
	Header *flow.Header
}

func NewBlockFollower(address string, chain flow.Chain, opts ...grpc.DialOption) (*BlockFollower, error) {
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		return nil, err
	}

	return &BlockFollower{
		client: access.NewAccessAPIClient(conn),
		chain:  chain,
	}, nil
}

func (c *BlockFollower) SubscribeBlockData(
	ctx context.Context,
	startHeight uint64,
	opts ...grpc.CallOption,
) (*Subscription[BlockDataResponse], error) {

	req := access.SubscribeBlockHeadersFromStartHeightRequest{
		StartBlockHeight: startHeight - 1,
		BlockStatus:      entities.BlockStatus_BLOCK_SEALED,
	}

	fmt.Println("starting from ", startHeight-1)

	stream, err := c.client.SubscribeBlockHeadersFromStartHeight(ctx, &req, opts...)
	if err != nil {
		return nil, err
	}

	sub := NewSubscription[BlockDataResponse]()
	go func() {
		defer close(sub.ch)

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				sub.err = fmt.Errorf("error receiving execution data: %w", err)
				return
			}

			blockHeader, err := convert.MessageToBlockHeader(resp.GetHeader())
			if err != nil {
				log.Printf("error converting block header data:\n%v", resp.GetHeader())
				sub.err = fmt.Errorf("error converting block header data: %w", err)
				return
			}

			log.Printf("received block header data for block %d %x", resp.Header.Height, resp.Header.Id)

			sub.ch <- BlockDataResponse{
				Header: blockHeader,
			}
		}
	}()

	return sub, nil
}

type ExecutionDataClient struct {
	client executiondata.ExecutionDataAPIClient
	chain  flow.Chain
}

func NewExecutionDataClient(address string, chain flow.Chain, opts ...grpc.DialOption) (*ExecutionDataClient, error) {
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		return nil, err
	}

	return &ExecutionDataClient{
		client: executiondata.NewExecutionDataAPIClient(conn),
		chain:  chain,
	}, nil
}

// GetExecutionDataForBlockID returns the BlockExecutionData for the given block ID.
func (c *ExecutionDataClient) GetExecutionDataForBlockID(
	ctx context.Context,
	blockID flow.Identifier,
	opts ...grpc.CallOption,
) (*execution_data.BlockExecutionData, error) {
	req := &executiondata.GetExecutionDataByBlockIDRequest{
		BlockId: blockID[:],
	}
	resp, err := c.client.GetExecutionDataByBlockID(ctx, req, opts...)
	if err != nil {
		return nil, err
	}

	execData, err := convert.MessageToBlockExecutionData(resp.GetBlockExecutionData(), c.chain)
	if err != nil {
		return nil, err
	}

	return execData, nil
}

type ExecutionDataResponse struct {
	BlockID       flow.Identifier
	Height        uint64
	ExecutionData *execution_data.BlockExecutionData
}

// SubscribeExecutionData subscribes to execution data updates starting at the given block ID or height.
func (c *ExecutionDataClient) SubscribeExecutionData(
	ctx context.Context,
	startBlockID flow.Identifier,
	startHeight uint64,
	opts ...grpc.CallOption,
) (*Subscription[ExecutionDataResponse], error) {
	if startBlockID != flow.ZeroID && startHeight > 0 {
		return nil, fmt.Errorf("cannot specify both start block ID and start height")
	}

	req := executiondata.SubscribeExecutionDataRequest{
		EventEncodingVersion: entities.EventEncodingVersion_CCF_V0,
	}
	if startBlockID != flow.ZeroID {
		req.StartBlockId = startBlockID[:]
	}
	if startHeight > 0 {
		req.StartBlockHeight = startHeight
	}

	stream, err := c.client.SubscribeExecutionData(ctx, &req, opts...)
	if err != nil {
		return nil, err
	}

	sub := NewSubscription[ExecutionDataResponse]()
	go func() {
		defer close(sub.ch)

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				sub.err = fmt.Errorf("error receiving execution data: %w", err)
				return
			}

			execData, err := convert.MessageToBlockExecutionData(resp.GetBlockExecutionData(), c.chain)
			if err != nil {
				log.Printf("error converting execution data:\n%v", resp.GetBlockExecutionData())
				sub.err = fmt.Errorf("error converting execution data: %w", err)
				return
			}

			log.Printf("received execution data for block %d %x with %d chunks", resp.BlockHeight, execData.BlockID, len(execData.ChunkExecutionDatas))

			sub.ch <- ExecutionDataResponse{
				Height:        resp.BlockHeight,
				ExecutionData: execData,
			}
		}
	}()

	return sub, nil
}

type EventFilter struct {
	EventTypes []string
	Addresses  []string
	Contracts  []string
}

type EventsResponse struct {
	Height  uint64
	BlockID flow.Identifier
	Events  []flow.Event
}

func (c *ExecutionDataClient) SubscribeEvents(
	ctx context.Context,
	startBlockID flow.Identifier,
	startHeight uint64,
	filter EventFilter,
	opts ...grpc.CallOption,
) (*Subscription[EventsResponse], error) {
	if startBlockID != flow.ZeroID && startHeight > 0 {
		return nil, fmt.Errorf("cannot specify both start block ID and start height")
	}

	req := executiondata.SubscribeEventsRequest{
		Filter: &executiondata.EventFilter{
			EventType: filter.EventTypes,
			Address:   filter.Addresses,
			Contract:  filter.Contracts,
		},
	}
	if startBlockID != flow.ZeroID {
		req.StartBlockId = startBlockID[:]
	}
	if startHeight > 0 {
		req.StartBlockHeight = startHeight
	}

	stream, err := c.client.SubscribeEvents(ctx, &req, opts...)
	if err != nil {
		return nil, err
	}

	sub := NewSubscription[EventsResponse]()
	go func() {
		defer close(sub.ch)

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				sub.err = fmt.Errorf("error receiving execution data: %w", err)
				return
			}

			sub.ch <- EventsResponse{
				Height:  resp.GetBlockHeight(),
				BlockID: convert.MessageToIdentifier(resp.GetBlockId()),
				Events:  convert.MessagesToEvents(resp.GetEvents()),
			}
		}
	}()

	return sub, nil
}
