package server

import (
	"encoding/json"
	"fmt"
	"github.com/onflow/go-ethereum/rpc"
	"net"
	"net/http"

	"github.com/bluesign/tinyAN/storage"
	"github.com/gorilla/mux"
)

type APIServer struct {
	router     *mux.Router
	httpServer *http.Server
	listener   net.Listener
	storage    *storage.HeightBasedStorage
}

func NewAPIServer(storage *storage.HeightBasedStorage) *APIServer {
	router := mux.NewRouter().StrictSlash(true)
	r := &APIServer{
		router:  router,
		storage: storage,
	}

	rpcServer := rpc.NewServer()
	rpcServer.RegisterName("eth", &APINamespace{
		storage: storage,
	})

	router.HandleFunc("/", rpcServer.ServeHTTP)
	router.HandleFunc("/api/resourceByType", r.ResourceByType)
	router.HandleFunc("/api/addressBalanceHistory", r.AddressBalanceHistory)
	router.HandleFunc("/api/ownerOfUuid", r.OwnerOfUuid)

	r.httpServer = &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:80"),
		Handler: router,
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

type results struct {
	Results []api_result `json:"results"`
}

type singleResult struct {
	Result api_result `json:"result"`
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

	fmt.Println("uuid:", uuid)
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

	result := m.storage.OwnersByType(t, height)
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

	p, _ := json.Marshal(results{Results: res})
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

	result := m.storage.BalanceHistoryByAddress(address, t, height)
	response := results{}
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
