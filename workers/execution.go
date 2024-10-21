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
	"time"
)

func UpdateExecution(store *storage.SporkStorage, chain flow.Chain) {

	// Update blocks
	ctxExecution := context.Background()
	height := store.Ledger().LastProcessedHeight()

	if height == 0 {
		height = store.StartHeight() + 1
	}

	endHeight := store.EndHeight()
	if endHeight > 0 && height == store.EndHeight() {
		fmt.Println("Already at end height")
		return
	}

	for {
		for {
			reconnect := false

			execClient, err := client.NewExecutionDataClient(
				store.AccessURL(),
				chain,
				grpc.WithDefaultCallOptions(
					grpc.MaxCallRecvMsgSize(1024*1024*100),
					grpc.UseCompressor(gzip.Name)),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				log.Fatalf("could not create execution data client: %v", err)
			}
			subExec, err := execClient.SubscribeExecutionData(ctxExecution, flow.ZeroID, height)

			if err != nil {
				log.Fatalf("could not subscribe to execution data: %v", err)
			}

			for {
				select {
				case <-ctxExecution.Done():
					return
				case response, ok := <-subExec.Channel():
					if subExec.Err() != nil || !ok {
						fmt.Println("Reconnecting to ExecutionData", subExec.Err())
						time.Sleep(1 * time.Second)
						reconnect = true
						break
					}

					if height == 0 {
						height = response.Height
					}

					if height != response.Height {
						log.Fatal("invalid height")
					}

					err := store.ProcessExecutionData(response.Height, response.ExecutionData)
					store.Index().IndexLedger(response.Height, response.ExecutionData)

					//panic("done")
					if err != nil {
						log.Fatalf("failed to process execution data: %v", err)
					}
					height = height + 1
					if endHeight > 0 && height > endHeight {
						return
					}
				}
				if reconnect {
					break
				}
			}
		}

	}

}
