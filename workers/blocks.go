package workers

import (
	"context"
	"fmt"
	"github.com/bluesign/tinyAN/client"
	"github.com/bluesign/tinyAN/storage"
	"github.com/onflow/flow-go/model/flow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"log"
	"strings"
	"time"
)

func UpdateBlocks(store *storage.SporkStorage, chain flow.Chain) {
	// Update blocks
	ctxBlocks := context.Background()
	height := store.Protocol().LastProcessedHeight()
	if height == 0 {
		height = store.StartHeight() + 1
	}
	endHeight := store.EndHeight()
	if endHeight > 0 && height == store.EndHeight() {
		return
	}
	fmt.Println("Starting BlockFollower from height", height)
	for {

		reconnect := false
		blockFollower, err := client.NewBlockFollower(
			store.AccessURL(),
			chain,
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(1024*1024*100),
				grpc.UseCompressor(gzip.Name)),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)

		if err != nil {
			log.Fatalf("could not block follower client: %v", err)
		}
		subBlock, err := blockFollower.SubscribeBlockData(ctxBlocks, height)

		if err != nil {
			log.Fatalf("could not subscribe to block data: %v", err)
		}

		for {
			select {
			case <-ctxBlocks.Done():
				return
			case response, ok := <-subBlock.Channel():
				if subBlock.Err() != nil || !ok {
					if strings.Contains(subBlock.Err().Error(), "Unimplemented") {
						fmt.Println("BlockFollower endpoint not implemented, skipping")
						return
					}

					fmt.Println("Reconnecting to BlockFollower", subBlock.Err())
					time.Sleep(1 * time.Second)
					reconnect = true
					break
				}

				if height == 0 {
					height = response.Header.Height
				}

				if height != response.Header.Height {
					log.Fatal("invalid height", height, response.Header.Height)
				}

				err = store.Protocol().SaveBlock(response.Header)

				if err != nil {
					log.Fatalf("failed to process block header: %v", err)
				}
				if err != nil {
					log.Fatalf("failed to process block data: %v", err)
				}
				height = height + 1
				if endHeight > 0 && height >= endHeight {
					fmt.Println("End height reached")
					fmt.Println("Last height", height)
					return
				}
			}
			if reconnect {
				break
			}
		}

	}

}

func TestMissing(store *storage.SporkStorage, height uint64, endHeight uint64, chain flow.Chain) {
	// Update blocks
	ctxBlocks := context.Background()
	fmt.Println("Starting BlockFollower Debugger from height", height)
	for {

		reconnect := false
		blockFollower, err := client.NewBlockFollower(
			store.AccessURL(),
			chain,
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(1024*1024*100),
				grpc.UseCompressor(gzip.Name)),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)

		if err != nil {
			log.Fatalf("could not block follower client: %v", err)
		}
		subBlock, err := blockFollower.SubscribeBlockData(ctxBlocks, height)

		if err != nil {
			log.Fatalf("could not subscribe to block data: %v", err)
		}

		for {
			select {
			case <-ctxBlocks.Done():
				return
			case response, ok := <-subBlock.Channel():
				if subBlock.Err() != nil || !ok {
					if strings.Contains(subBlock.Err().Error(), "Unimplemented") {
						fmt.Println("BlockFollower endpoint not implemented, skipping")
						return
					}

					fmt.Println("Reconnecting to BlockFollower", subBlock.Err())
					time.Sleep(1 * time.Second)
					reconnect = true
					break
				}

				if height == 0 {
					height = response.Header.Height
				}

				fmt.Println("Height", height, "Response", response.Header.Height)
				if height != response.Header.Height {
					log.Fatal("invalid height", height, response.Header.Height)
				}

				err = store.Protocol().SaveBlockWithoutProgress(response.Header)

				if err != nil {
					log.Fatalf("failed to process block header: %v", err)
				}
				if err != nil {
					log.Fatalf("failed to process block data: %v", err)
				}
				height = height + 1
				if endHeight > 0 && height >= endHeight {
					fmt.Println("End height reached")
					fmt.Println("Last height", height)
					return
				}
			}
			if reconnect {
				break
			}
		}

	}

}
