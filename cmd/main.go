package main

import (
	"fmt"
	"github.com/bluesign/tinyAN/storage"
	"github.com/cockroachdb/pebble"
)

func main() {
	checkpointDb := storage.MustOpenPebbleDB(fmt.Sprintf("db/%s/checkpoint", "mainnet26"))
	//ledgerDb := storage.MustOpenPebbleDB(fmt.Sprintf("db/%s/ledger", "mainnet26"))

	options := &pebble.IterOptions{
		LowerBound: []byte(""),
		UpperBound: []byte("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"),
	}
	var k []byte

	iter, err := checkpointDb.NewIter(options)
	if err != nil {
		return
	}
	for iter.Next() {

		fmt.Println(string(iter.Key()))
		break
	}
	defer iter.Close()

}
