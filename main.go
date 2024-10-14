package main

import (
	"fmt"
	"log"
	"time"

	"github.com/bluesign/tinyAN/workers"
	"github.com/rs/zerolog"

	"github.com/bluesign/tinyAN/server"
	"github.com/bluesign/tinyAN/storage"
	"github.com/onflow/flow-go/model/flow"
	"github.com/psiemens/sconfig"
	"github.com/spf13/cobra"
)

func main() {
	if err := startCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

type Config struct {
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

	chain := flow.Mainnet.Chain()

	log.Println("Start tinyAN")

	store := storage.NewHeightBasedStorage(
		[]*storage.SporkStorage{

			storage.NewSporkStorage(
				"mainnet-26",
				"access-007.mainnet26.nodes.onflow.org:9000",
				88226267,
				0,
				0,
			),

			storage.NewSporkStorage(
				"mainnet-25",
				"access-001.mainnet25.nodes.onflow.org:9000",
				85981135,
				88226267,
				0,
			),
		},
	)

	//bootstrap
	store.Sync()

	access := server.NewAccessAdapter(zerolog.Logger{}, store)

	//start grpc server
	grpcServer := server.NewGRPCServer(chain.ChainID(), access, "0.0.0.0", 9000)
	go grpcServer.Start()
	defer grpcServer.Stop()

	apiServer := server.NewAPIServer(zerolog.Logger{}, access, chain, store)
	go apiServer.Start()

	for i, s := range store.Sporks() {
		fmt.Println("Starting worker for ", s.Name(), i)
		go workers.UpdateExecution(s, chain)
		go workers.UpdateBlocks(s, chain)
	}

	for {
		time.Sleep(time.Second)
	}

}
