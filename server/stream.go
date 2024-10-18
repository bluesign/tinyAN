package server

import (
	"context"
	"fmt"
	"github.com/bluesign/tinyAN/storage"
	"github.com/onflow/flow-evm-gateway/api"
	"math/big"

	"github.com/onflow/flow-evm-gateway/models"
	"github.com/onflow/flow-evm-gateway/services/logs"
	"github.com/onflow/go-ethereum/common"
	"github.com/onflow/go-ethereum/common/hexutil"
	gethTypes "github.com/onflow/go-ethereum/core/types"
	"github.com/onflow/go-ethereum/eth/filters"
	"github.com/onflow/go-ethereum/rpc"
)

type DataProvider struct {
	api                   *APINamespace
	store                 *storage.HeightBasedStorage
	blocksPublisher       *models.Publisher[*models.Block]
	transactionsPublisher *models.Publisher[*gethTypes.Transaction]
	logsPublisher         *models.Publisher[[]*gethTypes.Log]
}

func (d *DataProvider) OnHeightChanged(number uint64) {
	block, err := d.api.blockFromBlockStorage(number - 1)
	if err != nil {
		fmt.Println("1")
		fmt.Println(err)
		return
	}

	transactions, err := d.api.blockTransactions(number - 1)
	if err != nil {
		fmt.Println("2")

		fmt.Println(err)
		return
	}

	rawTransactions := make([]*gethTypes.Transaction, 0)
	txHashes := make([]common.Hash, 0)
	for _, tx := range transactions {
		txHashes = append(txHashes, tx.Transaction.Hash())
		switch v := tx.Transaction.(type) {
		case models.DirectCall:
			rawTransactions = append(rawTransactions, v.Transaction())
		case models.TransactionCall:
			rawTransactions = append(rawTransactions, v.Transaction)
		}
	}

	blockResponse := &models.Block{
		Block:             block,
		TransactionHashes: txHashes,
	}

	d.blocksPublisher.Publish(blockResponse)
	for _, tx := range rawTransactions {
		d.transactionsPublisher.Publish(tx)
	}
	for _, tx := range transactions {
		d.logsPublisher.Publish(tx.Receipt.Logs)
	}

}

type StreamAPI struct {
	dataProvider *DataProvider
}

func NewStreamAPI(
	dataProvider *DataProvider,
) *StreamAPI {
	return &StreamAPI{
		dataProvider: dataProvider,
	}
}

// NewHeads send a notification each time a new block is appended to the chain.
func (s *StreamAPI) NewHeads(ctx context.Context) (*rpc.Subscription, error) {
	return newSubscription(
		ctx,
		s.dataProvider.blocksPublisher,
		func(notifier *rpc.Notifier, sub *rpc.Subscription) func(block *models.Block) error {
			return func(block *models.Block) error {
				h, err := block.Hash()
				if err != nil {
					return err
				}

				return notifier.Notify(sub.ID, &api.Block{
					Hash:          h,
					Number:        hexutil.Uint64(block.Height),
					ParentHash:    block.ParentBlockHash,
					ReceiptsRoot:  block.ReceiptRoot,
					Transactions:  block.TransactionHashes,
					Uncles:        []common.Hash{},
					GasLimit:      hexutil.Uint64(blockGasLimit),
					Nonce:         gethTypes.BlockNonce{0x1},
					Timestamp:     hexutil.Uint64(block.Timestamp),
					BaseFeePerGas: hexutil.Big(*big.NewInt(0)),
				})
			}
		},
	)
}

// NewPendingTransactions creates a subscription that is triggered each time a
// transaction enters the transaction pool. If fullTx is true the full tx is
// sent to the client, otherwise the hash is sent.
func (s *StreamAPI) NewPendingTransactions(ctx context.Context, fullTx *bool) (*rpc.Subscription, error) {
	return newSubscription(
		ctx,
		s.dataProvider.transactionsPublisher,
		func(notifier *rpc.Notifier, sub *rpc.Subscription) func(*gethTypes.Transaction) error {
			return func(tx *gethTypes.Transaction) error {
				if fullTx != nil && *fullTx {
					return notifier.Notify(sub.ID, tx)
				}

				return notifier.Notify(sub.ID, tx.Hash())
			}
		},
	)
}

// Logs creates a subscription that fires for all new log that match the given filter criteria.
func (s *StreamAPI) Logs(ctx context.Context, criteria filters.FilterCriteria) (*rpc.Subscription, error) {
	logCriteria, err := logs.NewFilterCriteria(criteria.Addresses, criteria.Topics)
	if err != nil {
		return nil, fmt.Errorf("failed to create log subscription filter: %w", err)
	}

	return newSubscription(
		ctx,
		s.dataProvider.logsPublisher,
		func(notifier *rpc.Notifier, sub *rpc.Subscription) func([]*gethTypes.Log) error {
			return func(allLogs []*gethTypes.Log) error {
				for _, log := range allLogs {
					// todo we could optimize this matching for cases where we have multiple subscriptions
					// using the same filter criteria, we could only filter once and stream to all subscribers
					if !logs.ExactMatch(log, logCriteria) {
						continue
					}

					if err := notifier.Notify(sub.ID, log); err != nil {
						return err
					}
				}

				return nil
			}
		},
	)
}

func newSubscription[T any](
	ctx context.Context,
	publisher *models.Publisher[T],
	callback func(notifier *rpc.Notifier, sub *rpc.Subscription) func(T) error,
) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return nil, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()

	subs := models.NewSubscription(callback(notifier, rpcSub))

	publisher.Subscribe(subs)

	go func() {
		defer publisher.Unsubscribe(subs)

		for {
			select {
			case err := <-subs.Error():
				fmt.Println(err)
				return
			case _ = <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}
