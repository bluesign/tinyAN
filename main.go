package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/bluesign/tinyAN/client"
	"github.com/bluesign/tinyAN/server"
	"github.com/bluesign/tinyAN/storage"
	"github.com/onflow/flow-go/model/flow"
	"github.com/psiemens/sconfig"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
)

func main() {
	if err := startCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

type Config struct {
	Port        int    `default:"9000" flag:"port,p" info:"port to run RPC server"`
	Bootstrap   bool   `default:"false" flag:"bootstrap,b" info:"bootstrap network after spork"`
	SporkName   string `default:"" flag:"spork" info:"spork name (testnet-xx, mainnet-xx)"`
	SporkHeight uint64 `default:"0" flag:"height" info:"spork height"`
	AccessNode  string `default:"" flag:"host" info:"access node to follow (host:9000) "`
	Chain       string `default:"testnet" flag:"chain" info:"testnet or mainnet"`
	Follow      bool   `default:"true" flag:"follow,f" info:"follow with streaming"`
}

var conf Config

var startCmd = initConfig(&cobra.Command{
	Use:   "tinyAN",
	Short: "tinyAN is poor man's Access Node for Flow Network",
	Long: `It just follows the chain with streaming API, and 
                answers Grpc Api calls @bluesign`,
	Run: func(cmd *cobra.Command, args []string) {
		StartExecute(cmd, args)
	},
})

func initConfig(cmd *cobra.Command) *cobra.Command {
	err := sconfig.New(&conf).
		FromEnvironment("TINY").
		BindFlags(cmd.PersistentFlags()).
		Parse()
	if err != nil {
		log.Fatal(err)
	}
	return cmd
}

var bigAccount sync.Mutex
var lockedBig string
var readerLock sync.Mutex

type MainnetAccounts struct {
	store *storage.ProtocolStorage
	cache sync.Map
}

type MainnetRegisters struct {
	address  string
	accounts *MainnetAccounts
	cache    map[string][]byte
}

func StartExecute(cmd *cobra.Command, args []string) {

	sporkName := conf.SporkName
	sporkHeight := conf.SporkHeight
	accessURL := conf.AccessNode

	if conf.Bootstrap {
		if sporkName == "" {
			log.Fatal("SporkName is required when bootstrapping")
		}
		if sporkHeight == 0 {
			log.Fatal("SporkHeight is required when bootstrapping")
		}
	}

	if conf.AccessNode == "" {
		log.Fatal("AccessNode is required parameter")
	}

	var chain flow.Chain
	switch conf.Chain {
	case "mainnet":
		chain = flow.Mainnet.Chain()
	case "testnet":
		chain = flow.Testnet.Chain()
	default:
		log.Fatal("Invalid Chain")
	}

	log.Println("Start tinyAN")

	ctxExecution := context.Background()
	ctxBlocks := context.Background()

	store, err := storage.NewProtocolStorage(conf.Bootstrap)
	if err != nil {
		log.Fatal(err)
	}

	height := store.LastHeight()
	headerHeight := store.LastBlockHeaderHeight()

	if conf.Bootstrap {
		store.Bootstrap(sporkName, sporkHeight)
		fmt.Println("Bootstrap done")
		store.Close()
		os.Exit(0)
	}

	if height == 0 {
		if sporkHeight == 0 {
			log.Println("Following with current height")
			height = 0
		} else {
			log.Println("Starting from SporkHeight")
			height = sporkHeight + 1
		}
	}

	if headerHeight == 0 {
		if sporkHeight == 0 {
			log.Println("Following blocks with current height")
			headerHeight = 0
		} else {
			log.Println("Starting blocks from SporkHeight")
			headerHeight = sporkHeight + 1
		}
	}

	conn, err := grpc.Dial(accessURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("could not connect to access api server: %v", err)
	}
	grpcServer := server.NewGRPCServer(chain.ChainID(), conn, store, "0.0.0.0", 9001)
	go grpcServer.Start()
	defer grpcServer.Stop()

	apiServer := server.NewAPIServer(store)
	go apiServer.Start()

	if conf.Follow {
		go func() {
			for {
				reconnect := false

				execClient, err := client.NewExecutionDataClient(
					accessURL,
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
						if subExec.Err() != nil {
							fmt.Println("Reconnecting to ExecutionData")
							reconnect = true
						}
						if !ok {
							fmt.Println("Reconnecting to ExecutionData")
							time.Sleep(10 * time.Second)
							reconnect = true
						}

						if height == 0 {
							height = response.Height
						}

						if height != response.Height {
							log.Fatal("invalid height")
						}

						store.NewBatch()
						err := store.ProcessExecutionData(response.Height, response.ExecutionData)
						store.IndexLedger(response.Height, response.ExecutionData)
						store.CommitBatch()

						//panic("done")
						if err != nil {
							log.Fatalf("failed to process execution data: %v", err)
						}
						height = height + 1
					}
					if reconnect {
						break
					}
				}
			}
		}()

		go func() {

			for {
				reconnect := false
				blockFollower, err := client.NewBlockFollower(
					accessURL,
					chain,
					grpc.WithDefaultCallOptions(
						grpc.MaxCallRecvMsgSize(1024*1024*100),
						grpc.UseCompressor(gzip.Name)),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
				)

				if err != nil {
					log.Fatalf("could not block follower client: %v", err)
				}
				subBlock, err := blockFollower.SubscribeBlockData(ctxBlocks, headerHeight)

				if err != nil {
					log.Fatalf("could not subscribe to block data: %v", err)
				}

				for {
					select {
					case <-ctxBlocks.Done():
						return
					case response, ok := <-subBlock.Channel():
						if subBlock.Err() != nil {
							fmt.Println("Reconnecting to BlockFollower")
							reconnect = true
						}
						if !ok {
							//TODO: handle me
							fmt.Println("Reconnecting to BlockFollower")
							time.Sleep(10 * time.Second)
							reconnect = true
						}

						if headerHeight == 0 {
							headerHeight = response.Header.Height
						}

						if headerHeight != response.Header.Height {
							log.Fatal("invalid height", headerHeight, response.Header.Height)
						}

						err = store.SaveBlockHeader(response.Header)
						if err != nil {
							log.Fatalf("failed to process block header: %v", err)
						}
						if err != nil {
							log.Fatalf("failed to process block data: %v", err)
						}
						headerHeight = headerHeight + 1
					}
					if reconnect {
						break
					}
				}

			}
		}()

	}

	for {
		time.Sleep(time.Second)
	}

}
