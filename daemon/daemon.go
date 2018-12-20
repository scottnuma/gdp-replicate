package daemon

import (
	"database/sql"

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
func NewNaiveDaemon(
	httpAddr,
	sqlFile string,
	myHashAddr gdp.Hash,
	peerAddrMap map[gdp.Hash]string,
) (*Daemon, error) {
	db, err := sql.Open("sqlite3", sqlFile)
	if err != nil {
		return nil, err
	}

	logServer := logserver.NewSqliteServer(db)
	logGraph, err := loggraph.NewSimpleGraph(logServer)
	if err != nil {
		return nil, err
	}
	policy := policy.NewNaivePolicy(logGraph)

	// Create list of peers
	peerList := make([]gdp.Hash, 0)
	for peer := range peerAddrMap {
		peerList = append(peerList, peer)
	}

	return &Daemon{
		httpAddr:       httpAddr,
		myAddr:         myHashAddr,
		network:        peers.NewGobServer(myHashAddr, peerAddrMap),
		policy:         policy,
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
		if err != nil {
			zap.S().Errorw(
				"failed to process msg",
				"msg", msg,
				"error", err,
			)
			return
		}

		if returnMsg != nil {
			daemon.network.Send(src, returnMsg)
		}
	}

	err := daemon.network.ListenAndServe(daemon.httpAddr, handler)
	return err
}
