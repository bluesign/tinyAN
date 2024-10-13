package storage

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/cockroachdb/pebble"
	"github.com/onflow/flow-go/ledger/complete/mtrie/flattener"
	"github.com/onflow/flow-go/ledger/complete/mtrie/node"
)

func importCheckpointSlabs(ledger *LedgerStorage, index *IndexStorage, spork string, sporkHeight uint64, part int) {
	getNode := func(nodeIndex uint64) (*node.Node, error) {
		return nil, nil
	}
	height := sporkHeight
	log.Println("Loading checkpointDb :", spork, part)
	checkpoint := fmt.Sprintf("https://storage.googleapis.com/flow-genesis-bootstrap/%s-execution/public-root-information/root.checkpoint.%03d", spork, part)
	resp, err := http.Get(checkpoint)
	if err != nil {
		log.Println("Error making request", err.Error())
		return
	}
	defer resp.Body.Close()

	r := resp.Body

	bufReader := bufio.NewReaderSize(r, 1024*100)

	//read header
	header := make([]byte, 4)
	_, err = io.ReadFull(bufReader, header)
	if err != nil {
		return
	}

	interim := make([]byte, 51)
	sc := make([]byte, 1024)
	i := 0

	height = sporkHeight
	batch := ledger.NewCheckpointBatch()
	indexBatch := index.NewBatch()
	for {
		interim, err = bufReader.Peek(51)
		if err != nil {
			break
		}
		if interim[0] != 0 {
			bufReader.Read(interim)
			continue
		}
		storableNode, err := flattener.ReadNode(bufReader, sc, getNode)
		if err != nil {
			log.Fatal(err)
		}

		payload := storableNode.Payload()

		index.IndexCheckpoint(indexBatch, payload, height)
		err = ledger.SavePayload(batch, payload, height)
		if err != nil {
			panic(err)
		}
		i = i + 1
		if i%1_000_000 == 0 {
			log.Print("CheckpointDb :", spork, part, i/1_000_000, height)
			batch.Commit(pebble.Sync)
			batch = ledger.NewCheckpointBatch()

			indexBatch.Commit(pebble.Sync)
			indexBatch = index.NewBatch()
		}

	}
	batch.Commit(pebble.Sync)
	indexBatch.Commit(pebble.Sync)
	log.Println("Finished checkpointDb :", spork, part, i)

}
