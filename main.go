package main

import (
	"log"
	"time"

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
	Port int `default:"9000" flag:"port,p" info:"port to run RPC server"`
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
			storage.NewSporkStorage("mainnet26", "access.mainnet.nodes.onflow.org:9000", 88226267),
			storage.NewSporkStorage("mainnet25", "access-001.mainnet25.nodes.onflow.org:9000", 85981135),
		},
	)

	store.Sync()

	grpcServer := server.NewGRPCServer(chain.ChainID(), store, "0.0.0.0", 9001)
	go grpcServer.Start()
	defer grpcServer.Stop()

	apiServer := server.NewAPIServer(store)
	go apiServer.Start()

	for {
		time.Sleep(time.Second)
	}

}
