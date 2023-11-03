package main

import (
	"context"
	"github.com/bluesign/tinyAN/server"
	"github.com/bluesign/tinyAN/storage"
	"github.com/onflow/flow-go/model/flow"
	"github.com/peterargue/execdata-client/client"
	"github.com/psiemens/sconfig"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"log"
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

func StartExecute(cmd *cobra.Command, args []string) {

	sporkName := conf.SporkName     //testnet-49
	sporkHeight := conf.SporkHeight //uint64(129578013)
	accessURL := conf.AccessNode    //"access-003.devnet49.nodes.onflow.org:9000"

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

	ctx := context.Background()

	store, err := storage.NewProtocolStorage()
	if err != nil {
		log.Fatal(err)
	}

	if conf.Bootstrap {
		store.Bootstrap(sporkName, sporkHeight)
	}

	height := store.LastHeight()

	if height == 0 {
		if sporkHeight == 0 {
			log.Println("Following with current height")
			height = 0
		} else {
			log.Println("Starting from SporkHeight")
			height = sporkHeight + 1
		}
	}

	conn, err := grpc.Dial(accessURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("could not connect to access api server: %v", err)
	}

	grpcServer := server.NewGRPCServer(chain.ChainID(), conn, store, "0.0.0.0", 9000)
	go grpcServer.Start()
	defer grpcServer.Stop()

	execClient, err := client.NewExecutionDataClient(accessURL, chain, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*20), grpc.UseCompressor(gzip.Name)), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("could not create execution data client: %v", err)
	}
	sub, err := execClient.SubscribeExecutionData(ctx, flow.ZeroID, height)

	if err != nil {
		log.Fatalf("could not subscribe to execution data: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case response, ok := <-sub.Channel():
			if sub.Err() != nil {
				log.Fatalf("error in subscription: %v", sub.Err())
			}
			if !ok {
				//TODO: handle me
				log.Fatalf("subscription closed")
			}

			if height == 0 {
				height = response.Height
			}

			if height != response.Height {
				log.Fatal("invalid height")
			}
			store.NewBatch()
			err := store.ProcessExecutionData(response.Height, response.ExecutionData)
			store.CommitBatch()

			if err != nil {
				log.Fatalf("failed to process execution data: %v", err)
			}
			height = height + 1
		}
	}
}
