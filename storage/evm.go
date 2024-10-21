package storage

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/onflow/flow-evm-gateway/models"
	"github.com/onflow/go-ethereum/common"
	gethCommon "github.com/onflow/go-ethereum/common"
	"github.com/rs/zerolog"
	"os"
)

const (
	codeEVMLastHeight                   byte = 0x01
	codeEVMHeightByCadenceHeight        byte = 0x02
	codeEVMCadenceHeightByEVMHeight     byte = 0x03
	codeEVMTransactionIDToCadenceHeight byte = 0x04

	codeEVMBlockIDToCadenceHeight byte = 0x05
	codeEVMBlockIDToEVMHeight     byte = 0x06

	codeEVMBlock = 0x07
)

type EVMHeightLookup interface {
	EVMHeightForBlockHash(hash gethCommon.Hash) (uint64, error)
	CadenceHeightForBlockHash(hash gethCommon.Hash) (uint64, error)
	CadenceBlockHeightForTransactionHash(hash gethCommon.Hash) (uint64, error)
	CadenceHeightFromEVMHeight(evmHeight uint64) (uint64, error)
	EVMHeightFromCadenceHeight(cadenceHeight uint64) (uint64, error)
}

type EVMStorage struct {
	logger          zerolog.Logger
	startHeight     uint64
	evmDB           *pebble.DB
	codec           *Codec
	OnHeightChanged func(uint64)
}

type EVMBlock struct {
	Block        *models.Block
	Transactions [][]byte
	Receipts     []*models.Receipt
}

func NewEVMStorage(spork string, startHeight uint64) (*EVMStorage, error) {
	evmDb := MustOpenPebbleDB(fmt.Sprintf("db/%s/evm", spork))

	return &EVMStorage{
		startHeight: startHeight,
		evmDB:       evmDb,
		codec:       NewCodec(),
		logger:      zerolog.New(os.Stdout).With().Timestamp().Logger(),
	}, nil
}

func (s *EVMStorage) SaveProgress(batch *pebble.Batch, height uint64) error {
	if s.OnHeightChanged != nil {
		s.OnHeightChanged(height)
	}
	return s.codec.MarshalAndSet(batch, b(keyProgress), height)
}

func (s *EVMStorage) LastProcessedHeight() uint64 {
	var height uint64
	err := s.codec.UnmarshalAndGet(s.evmDB, b(keyProgress), &height)
	if err != nil {
		return 0
	}
	return height
}

func (s *EVMStorage) StartHeight() uint64 {
	return s.startHeight
}

func (s *EVMStorage) NewBatch() *pebble.Batch {
	return s.evmDB.NewBatch()
}

func (s *EVMStorage) Close() {
	err := s.evmDB.Close()
	if err != nil {
		s.logger.Log().Err(err).Msg("error closing database")
	}
}

func (s *EVMStorage) FixBroken() {

	evmBlockHash := common.HexToHash("0x2bf9011cada49d26aeba1f8893ff6f609c798c44126d521e2dd97f31665357db")
	batch := s.NewBatch()
	//insert evm block
	err := s.codec.MarshalAndSet(batch,
		makePrefix(codeEVMBlockIDToCadenceHeight, evmBlockHash),
		uint64(86718842),
	)
	if err != nil {
		s.logger.Log().Err(err).Msg("error saving evm block id to cadence height")
	}

	err = s.codec.MarshalAndSet(batch,
		makePrefix(codeEVMBlockIDToEVMHeight, evmBlockHash),
		uint64(714157),
	)
	if err != nil {
		s.logger.Log().Err(err).Msg("error saving evm block id to evm height")
	}

	err = s.codec.MarshalAndSet(batch,
		makePrefix(codeEVMHeightByCadenceHeight, uint64(86718842)),
		uint64(714157),
	)
	if err != nil {
		s.logger.Log().Err(err).Msg("error saving evm height by cadence height")
	}

	err = s.codec.MarshalAndSet(batch,
		makePrefix(codeEVMCadenceHeightByEVMHeight, uint64(714157)),
		uint64(86718842),
	)
	if err != nil {
		s.logger.Log().Err(err).Msg("error saving cadence height by evm height")
	}
	batch.Commit(pebble.Sync)

	/*
		var txIdss = []string{"0xd30a36549af3c98d7cee8d01707c0ed7eb066a8e2d90567e3413e6ba0d951a11",
			"0x92320adeb3be429c41012cd207eafa82e5f98a1bb8d2be5c5557c72492ea9585",
			"0x0915f8f221d4d19b0616b48cf52da21e9fadde7cb50f48d2eee10d70dff4c1f4",
			"0x20b45a6f154255770df4dfb0db0bfe17cad8c66025d904d89bd0ab41d0c95261",
			"0x6adb6f4b9e941a07a5156f6bf8d417a23d8d71c699ab853cde087cf609ac24a3",
			"0xa981ab38c54a46bca1988d46c51a68e068d3840774f6e3b05d1ea7c66acd804d",
			"0x8abd5aad834fec02e65e517778233616eee491f7adfc9e1ac7ac99df57369c45",
			"0x5bf9836a4a7c2fe4dbfc902feaefb9baf33859ccfcf3bc2e01a37b7861cc8799",
			"0x2c1c0c3e75577084466ffe40cf2e9da6ef24cd8faa88040f46329aadc4be413a",
			"0xdb77eed375b52520e36180fdc98d9a61f66ce1273842f26221199a9fe8be04e0",
			"0xc4e17f5218053814701a64e15ab1e87514942d20f925bffe6738facd90dbb5f1",
			"0xc12de69dd0d6812b351c1ba5dafd1525055577161b5d818fd09852c5b8a868b5",
			"0x921a54d21bba68a63a6e9ba4207abd09ac1b3443ce415511204414c52273948c",
			"0x027babebf82ec6fc7fc714b5ff382a1164d6d84063ab1dca9a8f0b1a017354b7",
			"0x51cc41aceb2cd0de4323d4d317c96af32a0d146c4ceed2bee8ce6ee4a600fa9e",
			"0xca9ec2b50e16a4854e885907e41b5e8fe82db4b57de00dab801cbb569e9a4ab0",
			"0x6980e146196a812d8950a957caa50acad258be570f24dfbe45995f74980c6ec3",
			"0xfde1dc3d8f3302f28ff0cea35f4ede35bf4392ef54143f71f7d34d66e50a483e",
			"0x5134a661cef2b74f3b761e3709798384e83f3bf2978c7371de838f18821b05c2",
			"0x4e690499fa9b97eb55fe0d6b2f8fff28614270d61b3b5c77f468cd221eaeee55",
			"0x74b29de1010c4d459c6f546eb7e6268293849053bd8bb1f648a4a6c784b803c5",
			"0x41eab00ac4dcefe4076c8e59042143b99474deb98c86cab475c762365948880e",
			"0x74aa5df917cc972af60bc1e1f1da50cc1185105b13d9e0b1a8bfd2812fabb242",
			"0x5b9a84f2a4f44a264f3f9f47e6746a281d7801b191f3a9797f169d6f645607ae",
			"0x5281c116a70f70e07e5695f691e80e692cb80fbf512442e3f1f047dc53fd3b1c",
			"0x16cd5724026fe94548555dcc9b215188ac5732870bf6bce4c3e69e7514204674",
			"0x9236fe93e42f0626a350aae8c48c0de0264265e675a2ae945c52c0cfcf01da7d",
			"0x555ec1161fd5dbecca77e86398c06c6f1ab84e9903bb729fbaaecf649dc39517",
			"0xc4084041e0f28d2b8c8ab475bffa03164f7b022b824141407ee50afd3554bc4a",
			"0x8353dee4f42e3aa9293eaee7a0d8ba0a4c9b9c3696b9dc36f11b55f0e45265d5",
			"0x199621420a2358e7f5ffc9611a66de9e2ad7cf11261c0b3474497499ace4dd4d",
			"0xa81584b8e8c876db746fabad7d00705fc43d164eb3821e43d9301a8ff5d8802e",
			"0xf0c62488ccf7abbe9c6fb7c20a2f16ad1793073ff04b578bfbeb716e908bf89d",
			"0x8a6d0d78c7144de1b587288ecdf86ddb815f9b5500a6553ee4dda025d0f2fb57",
			"0x0db3455e13a703cf16af3b941e47d17fd38d5b7e994a26c7485f1f00c22b2b1c",
			"0x34497034213d43b0c175e4be43f000ec28a1c92ff1e2927a5b1a1e3e0ada9204",
			"0x5a9f6a67cb5be8ddcbdf7a0521e190fb1cf23f519a74633155ad9fda7ee0fd5c",
			"0xac9002ffdf4b862e8663d38ef042500bb78f4e052e7633f43abce415fcd00a2d",
			"0xd6e21fc28273450dfb2439182c40339202f24b0d10f879fdc2809aa06ed87c65",
			"0xe83834fa5d68f748ca6ec23b889a4fe038975f7e85828658c26ea85cde24b987",
			"0x8e510412a9d40697505e1f196b7614e044c0c0ca7e57bd172cf05fbcea1232f8",
			"0xccf2e780dd4b74eea28056cbdcbd003e69deed96c354021a48520a7b9403f0fd",
			"0x102391c65f2ac7964a1951b272fdc0a2990b590a70b0b3c15bdc6b295966282f",
			"0xe36e520bf293e533ea747051ee0eb4d4fcad238fdee13b08f45b3e1afe6ebddd",
			"0x932cd72605ebb1f883c31e44bcc51d385f69322de10ce2898fbcaddffae33802",
			"0x4de7116e80d67bd58cd1b75e5d9787a299fa593fd23c3a31f8fb2880d354ef15",
			"0xd8e1c330a1eba19840b1cdc9a42fbcb980f5c0f543390b3d9a95fa808ed3c39d",
			"0xf9f6437310a8774d4bac53598931fc663624d7bc1c524ffcc3e2b66c302d643a",
			"0x7ced98ff92931db8f8f8daa690607b5450fe33b5b4ddd30be6d28f83c7c5bb5d",
		}*/

}

func (s *EVMStorage) SaveBlock(evmEvents *models.CadenceEvents) error {
	batch := s.evmDB.NewBatch()
	defer batch.Commit(pebble.Sync)

	for _, transaction := range evmEvents.Transactions() {
		err := s.codec.MarshalAndSet(batch,
			makePrefix(codeEVMTransactionIDToCadenceHeight, transaction.Hash()),
			evmEvents.CadenceHeight(),
		)
		if err != nil {
			s.logger.Log().Err(err).Msg("error saving transaction id to cadence height")
		}
	}

	if evmEvents.Block() != nil {
		evmBlockHash, err := evmEvents.Block().Hash()
		if err != nil {
			s.logger.Log().Err(err).Msg("error getting evm block hash")
			panic(err) //shouldn't happen
		}

		//insert evm block
		err = s.codec.MarshalAndSet(batch,
			makePrefix(codeEVMBlockIDToCadenceHeight, evmBlockHash),
			evmEvents.CadenceHeight(),
		)
		if err != nil {
			s.logger.Log().Err(err).Msg("error saving evm block id to cadence height")
		}

		err = s.codec.MarshalAndSet(batch,
			makePrefix(codeEVMBlockIDToEVMHeight, evmBlockHash),
			evmEvents.Block().Height,
		)
		if err != nil {
			s.logger.Log().Err(err).Msg("error saving evm block id to evm height")
		}

		err = s.codec.MarshalAndSet(batch,
			makePrefix(codeEVMHeightByCadenceHeight, evmEvents.CadenceHeight()),
			evmEvents.Block().Height,
		)
		if err != nil {
			s.logger.Log().Err(err).Msg("error saving evm height by cadence height")
		}

		err = s.codec.MarshalAndSet(batch,
			makePrefix(codeEVMCadenceHeightByEVMHeight, evmEvents.Block().Height),
			evmEvents.CadenceHeight(),
		)
		if err != nil {
			s.logger.Log().Err(err).Msg("error saving cadence height by evm height")
		}

		err = s.SaveProgress(batch, evmEvents.Block().Height)

		if err != nil {
			s.logger.Log().Err(err).Msg("error saving evm progress")
		}

	} else {
		fmt.Println("empty block")
		s.logger.Log().Msgf("empty block at height: %d", evmEvents.CadenceHeight())
	}

	return nil
}

func (s *EVMStorage) EVMHeightForBlockHash(hash gethCommon.Hash) (uint64, error) {
	var height uint64
	err := s.codec.UnmarshalAndGet(s.evmDB, makePrefix(codeEVMBlockIDToEVMHeight, hash), &height)
	if err != nil {
		return 0, err
	}
	return height, nil
}

func (s *EVMStorage) CadenceHeightForBlockHash(hash gethCommon.Hash) (uint64, error) {
	var height uint64
	err := s.codec.UnmarshalAndGet(s.evmDB, makePrefix(codeEVMBlockIDToCadenceHeight, hash), &height)
	if err != nil {
		return 0, err
	}
	return height, nil
}

func (s *EVMStorage) CadenceBlockHeightForTransactionHash(hash gethCommon.Hash) (uint64, error) {
	var height uint64
	err := s.codec.UnmarshalAndGet(s.evmDB, makePrefix(codeEVMTransactionIDToCadenceHeight, hash), &height)
	if err != nil {
		return 0, err
	}
	return height, nil
}

func (s *EVMStorage) CadenceHeightFromEVMHeight(evmHeight uint64) (uint64, error) {
	var height uint64
	err := s.codec.UnmarshalAndGet(s.evmDB, makePrefix(codeEVMCadenceHeightByEVMHeight, evmHeight), &height)
	if err != nil {
		return 0, err
	}
	return height, nil
}

func (s *EVMStorage) EVMHeightFromCadenceHeight(cadenceHeight uint64) (uint64, error) {
	var height uint64
	err := s.codec.UnmarshalAndGet(s.evmDB, makePrefix(codeEVMHeightByCadenceHeight, cadenceHeight), &height)
	if err != nil {
		return 0, err
	}
	return height, nil
}
