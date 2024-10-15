package server

import (
	"fmt"
	"github.com/onflow/go-ethereum/common"
	gethTypes "github.com/onflow/go-ethereum/core/types"
	"github.com/onflow/go-ethereum/rpc"
	"golang.org/x/exp/slices"

	errs "github.com/onflow/flow-evm-gateway/models/errors"
)

// The maximum number of topic criteria allowed
const maxTopics = 4

// The maximum number of addresses allowed
const maxAddresses = 6

// FilterCriteria for log filtering.
// Address of the contract emitting the log.
// Topics that match the log topics, following the format:
// [] “anything”
// [A] “A in first position (and anything after)”
// [null, B] “anything in first position AND B in second position (and anything after)”
// [A, B] “A in first position AND B in second position (and anything after)”
// [[A, B], [A, B]] “(A OR B) in first position AND (A OR B) in second position (and anything after)”
type FilterCriteria struct {
	Addresses []common.Address
	Topics    [][]common.Hash
}

func NewFilterCriteria(addresses []common.Address, topics [][]common.Hash) (*FilterCriteria, error) {
	if len(topics) > maxTopics {
		return nil, fmt.Errorf("max topics exceeded, only %d allowed, got %d", maxTopics, len(topics))
	}
	if len(addresses) > maxAddresses {
		return nil, fmt.Errorf("max addresses exceeded, only %d allowed, got %d", maxAddresses, len(addresses))
	}

	return &FilterCriteria{
		Addresses: addresses,
		Topics:    topics,
	}, nil
}

// RangeFilter matches all the indexed logs within the range defined as
// start and end block height. The start must be strictly smaller or equal than end value.
type RangeFilter struct {
	start, end uint64
	criteria   *FilterCriteria
	api        *APINamespace
}

func NewRangeFilter(
	start, end uint64,
	criteria FilterCriteria,
	api *APINamespace,
) (*RangeFilter, error) {
	if len(criteria.Topics) > maxTopics {
		return nil, fmt.Errorf("max topics exceeded, only %d allowed, got %d", maxTopics, len(criteria.Topics))
	}

	// make sure that beginning number is not bigger than end
	if start > end {
		return nil, fmt.Errorf(
			"%w: start block number (%d) must be smaller or equal to end block number (%d)",
			errs.ErrInvalid,
			start,
			end,
		)
	}

	return &RangeFilter{
		start:    start,
		end:      end,
		criteria: &criteria,
		api:      api,
	}, nil
}

func (r *RangeFilter) Match() ([]*gethTypes.Log, error) {

	var logs []*gethTypes.Log

	for height := r.start; height <= r.end; height++ {
		fmt.Println("height", height)
		_, receipts, err := r.api.blockTransactions(height)
		if err != nil {
			return nil, err
		}

		for _, receipt := range receipts {
			if bloomMatch(receipt.Bloom, r.criteria) {
				for _, log := range receipt.Logs {
					if ExactMatch(log, r.criteria) {
						logs = append(logs, log)
					}
				}
			}
		}
	}

	return logs, nil
}

// todo add HeightFilter

// IDFilter matches all logs against the criteria found in a single block identified
// by the provided block ID.
type IDFilter struct {
	id       common.Hash
	criteria *FilterCriteria
	api      *APINamespace
}

func NewIDFilter(
	id common.Hash,
	criteria FilterCriteria,
	api *APINamespace,
) (*IDFilter, error) {
	if len(criteria.Topics) > maxTopics {
		return nil, fmt.Errorf("max topics exceeded, only %d allowed, got %d", maxTopics, len(criteria.Topics))
	}

	return &IDFilter{
		id:       id,
		criteria: &criteria,
		api:      api,
	}, nil
}

func (i *IDFilter) Match() ([]*gethTypes.Log, error) {
	height, err := i.api.blockNumberOrHashToHeight(
		rpc.BlockNumberOrHash{
			BlockHash: &i.id,
		})
	if err != nil {
		return nil, err
	}
	_, receipts, err := i.api.blockTransactions(height)
	if err != nil {
		return nil, err
	}
	logs := make([]*gethTypes.Log, 0)
	for _, receipt := range receipts {
		for _, log := range receipt.Logs {
			if ExactMatch(log, i.criteria) {
				logs = append(logs, log)
			}
		}
	}
	return logs, nil
}

// ExactMatch checks the topic and address values of the log match the filter exactly.
func ExactMatch(log *gethTypes.Log, criteria *FilterCriteria) bool {
	// check criteria doesn't have more topics than the log, but it can have less due to wildcards
	if len(criteria.Topics) > len(log.Topics) {
		return false
	}

	for i, sub := range criteria.Topics {
		// wildcard matching all
		if len(sub) == 0 {
			continue
		}

		if !slices.Contains(sub, log.Topics[i]) {
			return false
		}
	}

	// no addresses is a wildcard to match all
	if len(criteria.Addresses) == 0 {
		return true
	}

	return slices.Contains(criteria.Addresses, log.Address)
}

// bloomMatch takes a bloom value and tests if the addresses and topics provided pass the bloom filter.
// This acts as a fast probabilistic test that might produce false-positives but not false-negatives.
// If true is returned we should further check against the exactMatch to really make sure the log is matched.
//
// source: https://github.com/ethereum/go-ethereum/blob/8d1db1601d3a9e4fd067558a49db6f0b879c9b48/eth/filters/filter.go#L395
func bloomMatch(bloom gethTypes.Bloom, criteria *FilterCriteria) bool {
	if len(criteria.Addresses) > 0 {
		var included bool
		for _, addr := range criteria.Addresses {
			if gethTypes.BloomLookup(bloom, addr) {
				included = true
				break
			}
		}
		if !included {
			return false
		}
	}

	for _, sub := range criteria.Topics {
		included := len(sub) == 0 // empty rule set == wildcard
		for _, topic := range sub {
			if gethTypes.BloomLookup(bloom, topic) {
				included = true
				break
			}
		}
		if !included {
			return false
		}
	}
	return true
}
