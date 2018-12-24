package daemon

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
	"github.com/tonyyanga/gdp-replicate/gdp"
	"github.com/tonyyanga/gdp-replicate/loggraph"
	"github.com/tonyyanga/gdp-replicate/logserver"
	"github.com/tonyyanga/gdp-replicate/peers"
	"github.com/tonyyanga/gdp-replicate/policy"
	"go.uber.org/zap"
)

type Daemon struct {
	httpAddr string
	myAddr   gdp.Hash
	network  peers.ReplicationServer
	policy   policy.Policy

	// Controls the randomness of sending heart beats to peers
	heartBeatState int
	peerList       []gdp.Hash
}

// NewDaemon initializes Daemon for a log
func NewDaemon(
	httpAddr,
	sqlFile string,
	myHashAddr gdp.Hash,
	peerAddrMap map[gdp.Hash]string,
	policyType string,
) (*Daemon, error) {
	zap.S().Infow(
		"Initializing new naive daemon",
		"httpAddr", httpAddr,
		"sqlFile", sqlFile,
		"gdpAddr", myHashAddr.Readable(),
		"numPeers", len(peerAddrMap),
	)
	db, err := sql.Open("sqlite3", sqlFile)
	if err != nil {
		return nil, err
	}

	logServer := logserver.NewSqliteServer(db)
	logGraph, err := loggraph.NewSimpleGraph(logServer)
	if err != nil {
		return nil, err
	}
	var chosenPolicy policy.Policy
	switch policyType {
	case "naive":
		chosenPolicy = policy.NewNaivePolicy(logGraph)
	default:
		chosenPolicy = policy.NewGraphDiffPolicy(logGraph)
	}

	// Create list of peers
	peerList := make([]gdp.Hash, 0)
	for peer := range peerAddrMap {
		peerList = append(peerList, peer)
	}

	return &Daemon{
		httpAddr:       httpAddr,
		myAddr:         myHashAddr,
		network:        peers.NewGobServer(myHashAddr, peerAddrMap),
		policy:         chosenPolicy,
		heartBeatState: 0,
		peerList:       peerList,
	}, nil
}

// Start begins listening for and sending heartbeats.
func (daemon Daemon) Start(fanoutDegree int) error {
	zap.S().Info("starting daemon")
	go daemon.scheduleHeartBeat(500, daemon.fanOutHeartBeat(fanoutDegree))

	handler := func(src gdp.Hash, msg interface{}) {
		returnMsg, err := daemon.policy.ProcessMessage(src, msg)
		if err == policy.ErrConversationFinished {
			zap.S().Infow(
				"heartbeat finished",
			)
			return
		}
		if err != nil {
			zap.S().Errorw(
				"failed to process msg",
				"msg", msg,
				"error", err,
			)
			return
		}

		// Daemon will always send content over the network,
		// even if returnMsg is nil
		daemon.network.Send(src, returnMsg)
	}

	err := daemon.network.ListenAndServe(daemon.httpAddr, handler)
	return err
}
