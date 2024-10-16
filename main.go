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
	/*
		strings := []string{
			"1c984c6c8b666653321a87fa1b97bf8034f13674174a85f576bf1f975af6c19f",
			"7554de796a10ab56ae213f5cd58e83a7dddc59b9d351e1feb1979adfa0305327",
			"601a0bca45c91f716d4c43898873551b7adb9fd4568d954b4b197c7e74ca4920",
			"18fc3aa069c164010c5c765b9ed47c8d64690cf7de1db3f80d14dcaf575f4436",
			"8ffd87ce8cee1f89944357b652c5a45aa873449b23f801bf2d481ce965dbf76f",
			"5ef12812e4387ba52e270b05e90e2682a60e80074ecfda2cef3b6e733852f696",
			"b43ac2334ab5d935efa4106e98afdc01366f12469cdef71ded5b7d96fd198552",
			"cc0aec2fb9d8f063d0fef28746bf88479f028ccd79743352c5e3bfdbbbac9006",
			"38aa75afeb0467b712f526938084e8dfb5b6b2c50e7d1a85a21254e534c661ea",
			"583051190d30a32d415db2df3a2ac52a7fde0a4a9a85423a4bc869b4790af427",
			"f3d9b205ce91f9158e5e359e1cd67157949a2f87b5421ae37457cf8b86110913",
		}
		txHashes := evmTypes.TransactionHashes{}
		for _, s := range strings {
			txHash, _ := hex.DecodeString(s)
			txHashes = append(txHashes, common.BytesToHash(txHash))
		}

		var ss = txHashes.RootHash().String()
		fmt.Println(ss)

		for {
			time.Sleep(time.Second)
		}
	*/
	chain := flow.Mainnet.Chain()

	log.Println("Start tinyAN")

	store := storage.NewHeightBasedStorage(
		[]*storage.SporkStorage{

			storage.NewSporkStorage(
				"mainnet-26",
				"access-007.mainnet26.nodes.onflow.org:9000",
				88226267,
				0,
				2221582,
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

	/*ui.MainUI(store)
	if true {
		return
	}*/

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
