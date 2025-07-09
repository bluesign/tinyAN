package main

import (
	"bytes"
	"encoding/hex"
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
		if !bytes.Contains(iter.Key(), []byte("public_key_")) {
			continue
		}

		fmt.Println(hex.EncodeToString(iter.Key()[4:12]), string(iter.Key()[15:len(iter.Key())-8]), hex.EncodeToString(iter.Value()))
		break
	}
	defer iter.Close()

}
