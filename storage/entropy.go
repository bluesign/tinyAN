package storage

import (
	"encoding/hex"
	"fmt"
	"github.com/onflow/flow-go/consensus/hotstuff/model"
	"github.com/onflow/flow-go/fvm/environment"
	flowgo "github.com/onflow/flow-go/model/flow"
)

type EntropyProviderPerBlockProvider struct {
	// AtBlockID returns an entropy provider at the given block ID.
	Store *HeightBasedStorage
}

type EntropyProvider struct {
	seed  []byte
	error error
}

func (e EntropyProvider) RandomSource() ([]byte, error) {
	return e.seed, e.error
}

func (e *EntropyProviderPerBlockProvider) AtBlockID(blockID flowgo.Identifier) environment.EntropyProvider {
	block, err := e.Store.GetBlockById(blockID)
	if err != nil {
		fmt.Println("error getting entropy seed")
		return nil
	}
	next, err := e.Store.GetBlockByHeight(block.Height + 1)
	if err != nil {
		fmt.Println("error getting entropy seed")
		return nil
	}
	packer := model.SigDataPacker{}
	sigData, err := packer.Decode(next.ParentVoterSigData)
	if err != nil {
		fmt.Println("error getting entropy seed")
		return nil
	}
	fmt.Println("beacon:", hex.EncodeToString(sigData.ReconstructedRandomBeaconSig))
	return EntropyProvider{seed: sigData.ReconstructedRandomBeaconSig, error: err}
}
