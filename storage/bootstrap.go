package storage

import (
	"bufio"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/onflow/flow-go/ledger/complete/mtrie/flattener"
	"github.com/onflow/flow-go/ledger/complete/mtrie/node"
	"io"
	"log"
	"net/http"
	"sync"
)

func importCheckpointSlabs(ledger *pebble.DB, spork string, sporkHeight uint64, part int, s *ProtocolStorage) {
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

	height = 0 // todo: as we dont know spork height on checkpoint state this is faster
	batch := ledger.NewBatch()
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
		key, _ := payload.Key()

		k := makePrefix(0xf0, key.CanonicalForm(), uint64(0xFFFFFFFFFFFFFFFF-height))
		s.IndexCheckpoint(height, payload)
		batch.Set(k, payload.Value(), pebble.Sync)

		i = i + 1
		if i%1_000_000 == 0 {
			fmt.Println(".", part, i/1000_000)
			batch.Commit(pebble.Sync)
			batch.Close()
			batch = ledger.NewBatch()
		}

	}
	batch.Commit(pebble.Sync)
	log.Println("Finished checkpointDb :", spork, part, i)

}

func importWorker(ledger *pebble.DB, spork string, sporkHeight uint64, ch chan int, wg *sync.WaitGroup, s *ProtocolStorage) {
	for {
		select {
		case part, ok := <-ch:
			if !ok {
				break
			}
			importCheckpointSlabs(ledger, spork, sporkHeight, part, s)
			wg.Done()
		}
	}
}
