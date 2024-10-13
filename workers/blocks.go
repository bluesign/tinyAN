package workers

import (
	"context"
	"fmt"
	"github.com/bluesign/tinyAN/client"
	"github.com/bluesign/tinyAN/storage"
	"github.com/cockroachdb/pebble"
	"github.com/onflow/flow-go/model/flow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"log"
	"time"
)

func UpdateBlocks(store *storage.SporkStorage, chain flow.Chain) {
	// Update blocks
	ctxBlocks := context.Background()
	height := store.Ledger().LastProcessedHeight()
	if height == 0 {
		height = store.StartHeight() + 1
	}
	endHeight := store.EndHeight()
	if endHeight > 0 && height == store.EndHeight() {
		return
	}

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
					fmt.Println("Reconnecting to BlockFollower")
					time.Sleep(5 * time.Second)
					reconnect = true
					break
				}

				if height == 0 {
					height = response.Header.Height
				}

				if height != response.Header.Height {
					log.Fatal("invalid height", height, response.Header.Height)
				}

				batch := store.Protocol().NewBatch()
				err = store.Protocol().SaveBlock(batch, response.Header)
				batch.Commit(pebble.Sync)

				if err != nil {
					log.Fatalf("failed to process block header: %v", err)
				}
				if err != nil {
					log.Fatalf("failed to process block data: %v", err)
				}
				height = height + 1
				if endHeight > 0 && height >= endHeight {
					return
				}
			}
			if reconnect {
				break
			}
		}

	}

}
