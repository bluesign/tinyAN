package server

import (
	"github.com/bluesign/tinyAN/server/repl"
	"github.com/fxamacker/cbor/v2"
	ssh "github.com/gliderlabs/ssh"
	"github.com/onflow/atree"
	"github.com/onflow/cadence/interpreter"

	"encoding/json"
	"fmt"
	"github.com/bluesign/tinyAN/storage"
	"github.com/gorilla/mux"
	"github.com/onflow/flow-evm-gateway/models"
	"github.com/onflow/flow-go/engine/access/rest/routes"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/module"
	"github.com/onflow/flow-go/module/metrics"
	gethTypes "github.com/onflow/go-ethereum/core/types"
	"github.com/onflow/go-ethereum/rpc"
	"github.com/rs/cors"
	"github.com/rs/zerolog"
	"log"
	"net"
	"net/http"
)

type APIServer struct {
	router     *mux.Router
	httpServer *http.Server
	listener   net.Listener
	storage    *storage.HeightBasedStorage
}

func NewAPIServer(logger zerolog.Logger, adapter *AccessAdapter, chain flow.Chain, storage *storage.HeightBasedStorage) *APIServer {

	ssh.Handle(func(s ssh.Session) {
		replx, err := repl.NewConsoleREPL(storage, s)
		fmt.Println("repl", replx)
		if err != nil {
			log.Println("error creating new console repl", err)
			return
		}
		replx.Run()
	})
	go ssh.ListenAndServe(":2222", nil, ssh.HostKeyPEM([]byte(`-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAaAAAABNlY2RzYS
1zaGEyLW5pc3RwMjU2AAAACG5pc3RwMjU2AAAAQQQpaN+53nWrV+TCvhjzWkWeDB+jBvaQ
Jan75fr4/I4lTvQFUuYG+PV8VK8EIPb6b26Y3OTSec+JZqdvtsbO6u4oAAAAwDY58kU2Of
JFAAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBClo37nedatX5MK+
GPNaRZ4MH6MG9pAlqfvl+vj8jiVO9AVS5gb49XxUrwQg9vpvbpjc5NJ5z4lmp2+2xs7q7i
gAAAAhAOD/iTyljm3f6YoULZ/ITrC7Jc/mjS1X0lp8oh1MDE6oAAAAI2JsdWVzaWduQERl
bml6cy1NYWNCb29rLVByby0yLmxvY2FsAQIDBA==
-----END OPENSSH PRIVATE KEY-----`)))

	var restCollector module.RestMetrics = metrics.NewNoopCollector()
	builder := routes.NewRouterBuilder(logger, restCollector).AddRestRoutes(adapter, chain)
	router := builder.Build()

	r := &APIServer{
		router:  router,
		storage: storage,
	}

	rpcServer := rpc.NewServer()

	apiEth := &APINamespace{storage: storage}
	dataProvider := &DataProvider{
		api:                   apiEth,
		store:                 storage,
		blocksPublisher:       models.NewPublisher[*models.Block](),
		transactionsPublisher: models.NewPublisher[*gethTypes.Transaction](),
		logsPublisher:         models.NewPublisher[[]*gethTypes.Log](),
	}

	storage.Latest().EVM().OnHeightChanged = dataProvider.OnHeightChanged

	rpcServer.RegisterName("eth", apiEth)
	rpcServer.RegisterName("debug", NewDebugApi(apiEth))
	rpcServer.RegisterName("net", &NetAPI{})
	rpcServer.RegisterName("web3", &Web3API{})
	rpcServer.RegisterName("txpool", &TxPool{})
	rpcServer.RegisterName("eth", &StreamAPI{dataProvider: dataProvider})

	router.Headers("Upgrade", "websocket").Handler(rpcServer.WebsocketHandler([]string{"*"}))
	router.Handle("/", rpcServer)

	router.HandleFunc("/api/resourceByType", r.ResourceByType)
	router.HandleFunc("/api/addressBalanceHistory", r.AddressBalanceHistory)
	router.HandleFunc("/api/ownerOfUuid", r.OwnerOfUuid)
	router.HandleFunc("/api/accountSize", r.AccountSize)
	c := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedHeaders: []string{"*"},
		AllowedMethods: []string{
			http.MethodGet,
			http.MethodPost,
			http.MethodOptions,
			http.MethodHead},
	})

	r.httpServer = &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:80"),
		Handler: c.Handler(router),
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", 80))
	if err != nil {
		panic(err)
	}
	r.listener = listener

	return r
}

func stringToUint64(s string) uint64 {
	var result uint64
	for _, c := range s {
		result = result*10 + uint64(c-'0')
	}
	return result
}

type api_result interface {
}

type location struct {
	Uuid    string `json:"uuid"`
	Address string `json:"address"`
}

type balanceHistory struct {
	Address string `json:"address"`
	Height  uint64 `json:"height"`
	Action  string `json:"action"`
	Uuid    uint64 `json:"uuid"`
	Balance string `json:"balance"`
}

type typeResult struct {
	Address string `json:"address"`
	Type    string `json:"type"`
	Uuid    uint64 `json:"uuid"`
	Id      uint64 `json:"id"`
	Balance string `json:"balance"`
}

type apiResults struct {
	Results []api_result `json:"results"`
}

type singleResult struct {
	Result api_result `json:"result"`
}

func (m APIServer) AccountSize(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)

	address := r.URL.Query().Get("address")
	if address == "" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("address is required"))
		return
	}

	flowAddress, _ := flow.StringToAddress(address)
	addressBytes := flowAddress.Bytes()

	var height uint64 = m.storage.Latest().Ledger().LastProcessedHeight()

	ledger := m.storage.StorageForHeight(height).Ledger()
	snapshot := ledger.StorageSnapshot(height)
	roView := NewViewOnlyLedger(snapshot)

	decodeStorable := func(
		decoder *cbor.StreamDecoder,
		slabID atree.SlabID,
		inlinedExtraData []atree.ExtraData,
	) (
		atree.Storable,
		error,
	) {
		return interpreter.DecodeStorable(
			decoder,
			slabID,
			inlinedExtraData,
			nil,
		)
	}

	decodeTypeInfo := func(decoder *cbor.StreamDecoder) (atree.TypeInfo, error) {
		return interpreter.DecodeTypeInfo(decoder, nil)
	}

	ledgerStorage := atree.NewLedgerBaseStorage(roView)

	persistentSlabStorage := atree.NewPersistentSlabStorage(
		ledgerStorage,
		interpreter.CBOREncMode,
		interpreter.CBORDecMode,
		decodeStorable,
		decodeTypeInfo,
	)

	storageSlabId, err := roView.GetValue(addressBytes, []byte("storage"))
	if err != nil {
		fmt.Println("error", err)
		return
	}

	storageSlab, _, err := persistentSlabStorage.Retrieve(atree.NewSlabID(atree.Address(addressBytes), atree.SlabIndex(storageSlabId)))
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		fmt.Println("error", err)
		return
	}

	fmt.Println("storageSlab", storageSlab)
	mapSlab := storageSlab.(*atree.MapDataSlab)

	err = mapSlab.PopIterate(persistentSlabStorage, func(key atree.Storable, value atree.Storable) {
		fmt.Println("key", key)
		fmt.Println("value", value)
	})
	fmt.Println("err", err)

	/*for len(childStorables) > 0 {

		var next []atree.Storable

		for _, s := range childStorables {

			fmt.Println("s", s)
			fmt.Println("s", s.ByteSize())
			sv, err := s.StoredValue(persistentSlabStorage)
			if err != nil {
				fmt.Println("error2", err)
				return
			}
			fmt.Println("sv", sv)

			if _, ok := s.(atree.MapValue); ok {

			}

			// This handles inlined slab because inlined slab is a child storable (s) and
			// we traverse s.ChildStorables() for its inlined elements.
			next = append(next, s.ChildStorables()...)
		}

		childStorables = next
	}*/

	p, _ := json.Marshal(singleResult{Result: nil})
	w.Write(p)

}

func (m APIServer) OwnerOfUuid(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)

	uuidString := r.URL.Query().Get("uuid")
	if uuidString == "" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("uuid is required"))
		return
	}

	var height uint64 = 0xFFFFFFFFFFFFFFFF
	heightString := r.URL.Query().Get("height")
	if heightString != "" {
		height = stringToUint64(heightString)
	}

	//convert uuid string to uint64
	var uuid uint64 = stringToUint64(uuidString)

	result := m.storage.StorageForHeight(height).Index().OwnerOfUuid(uuid, height)
	if result == "not found" {
		w.WriteHeader(http.StatusNotFound)

	} else {
		location := location{
			Uuid:    uuidString,
			Address: result,
		}

		p, _ := json.Marshal(singleResult{Result: location})
		w.Write(p)

	}

}

func (m APIServer) ResourceByType(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)

	query := r.URL.Query()

	t := query.Get("type")
	if t == "" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("type is required"))
		return
	}

	var height uint64 = 0xFFFFFFFFFFFFFFFF
	heightString := r.URL.Query().Get("height")
	if heightString != "" {
		height = stringToUint64(heightString)
	}

	result := m.storage.Latest().Index().OwnersByType(t, height)
	res := make([]api_result, 0)

	for _, resource := range result {
		res = append(res, &typeResult{
			Address: resource.Address,
			Type:    resource.TypeName,
			Uuid:    resource.Uuid,
			Id:      resource.Id,
			Balance: fmt.Sprintf("%d.%08d", resource.Balance/1000000000, resource.Balance%1000000000),
		})
	}

	p, _ := json.Marshal(apiResults{Results: res})
	w.Write(p)

}

// done
func (m APIServer) AddressBalanceHistory(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)

	query := r.URL.Query()

	address := query.Get("address")
	if address == "" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("address is required"))
		return
	}
	t := query.Get("type")
	if t == "" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("type is required"))
		return
	}

	var height uint64 = 0xFFFFFFFFFFFFFFFF
	heightString := r.URL.Query().Get("height")
	if heightString != "" {
		height = stringToUint64(heightString)
	}

	result := m.storage.Latest().Index().BalanceHistoryByAddress(address, t, height)
	response := apiResults{}
	response.Results = make([]api_result, 0)
	for _, v := range result {
		history := balanceHistory{
			Address: v.Address,
			Action:  v.Action,
			Height:  v.Height,
			Uuid:    v.Uuid,
			Balance: fmt.Sprintf("%d.%08d", v.Balance/1000000000, v.Balance%1000000000),
		}
		response.Results = append(response.Results, history)
	}
	p, _ := json.Marshal(response)
	w.Write(p)

}

func (m APIServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.router.ServeHTTP(w, r)
}

func (m APIServer) Start() {
	m.httpServer.Serve(m.listener)
}
