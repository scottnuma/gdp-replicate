package daemon

import (
	"database/sql"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"github.com/tonyyanga/gdp-replicate/peers"
	"github.com/tonyyanga/gdp-replicate/policy"
	"go.uber.org/zap"
)

type Daemon struct {
	httpAddr string
	myAddr   gdplogd.Hash
	network  peers.ReplicateNetworkMgr
	policy   policy.Policy
	conn     gdplogd.LogDaemonConnection
	// Controls the randomness of sending heart beats to peers
	heartBeatState int
	peerList       []gdplogd.Hash
}

// NewDaemon initializes Daemon for a log
func NewDaemon(
	httpAddr,
	sqlFile string,
	myHashAddr gdplogd.Hash,
	peerAddrMap map[gdplogd.Hash]string,
) (Daemon, error) {
	db, err := sql.Open("sqlite3", sqlFile)
	if err != nil {
		return Daemon{}, err
	}

	conn, err := gdplogd.InitLogDaemonConnector(db, "default")
	if err != nil {
		return Daemon{}, err
	}

	graph, err := conn.GetGraph("default")
	if err != nil {
		return Daemon{}, err
	}
	policy := policy.NewGraphDiffPolicy(conn, "policy-name", graph)

	// Create list of peers
	peerList := make([]gdplogd.Hash, 0)
	for peer := range peerAddrMap {
		peerList = append(peerList, peer)
	}

	return Daemon{
		httpAddr:       httpAddr,
		myAddr:         myHashAddr,
		network:        peers.NewSimpleReplicateMgr(peerAddrMap),
		policy:         policy,
		conn:           conn,
		heartBeatState: 0,
		peerList:       peerList,
	}, nil
}

// NewNaiveDaemon initializes Daemon for a log with the naive algorithm
func NewNaiveDaemon(
	httpAddr,
	sqlFile string,
	myHashAddr gdplogd.Hash,
	peerAddrMap map[gdplogd.Hash]string,
) (Daemon, error) {
	db, err := sql.Open("sqlite3", sqlFile)
	if err != nil {
		return Daemon{}, err
	}

	policy := policy.NewNaivePolicy(db, "policy-name")

	// Create list of peers
	peerList := make([]gdplogd.Hash, 0)
	for peer := range peerAddrMap {
		peerList = append(peerList, peer)
	}

	return Daemon{
		httpAddr:       httpAddr,
		myAddr:         myHashAddr,
		network:        peers.NewSimpleReplicateMgr(peerAddrMap),
		policy:         policy,
		heartBeatState: 0,
		peerList:       peerList,
	}, nil
}

// Start begins listening for and sending heartbeats.
func (daemon Daemon) Start(fanoutDegree int) error {
	zap.S().Info("starting daemon")
	go daemon.scheduleHeartBeat(500, daemon.fanOutHeartBeat(fanoutDegree))

	handler := func(src gdplogd.Hash, msg *policy.Message) {
		returnMsg := daemon.policy.ProcessMessage(msg, src)

		if returnMsg != nil {
			go daemon.network.Send(daemon.myAddr, src, returnMsg)
		}
	}

	err := daemon.network.ListenAndServe(daemon.httpAddr, handler)
	return err
}
