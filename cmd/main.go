package main

import (
	"fmt"
	"github.com/bluesign/tinyAN/storage"
	"github.com/cockroachdb/pebble"
)

func main() {
	checkpointDb := storage.MustOpenPebbleDB(fmt.Sprintf("db/%s/checkpoint", "mainnet-26"))
	//ledgerDb := storage.MustOpenPebbleDB(fmt.Sprintf("db/%s/ledger", "mainnet26"))

	options := &pebble.IterOptions{}

	iter, err := checkpointDb.NewIter(options)
	if err != nil {
		return
	}
	iter.First()
	for iter.Next() {

		fmt.Println(string(iter.Key()))
		break
	}
	defer iter.Close()

}
