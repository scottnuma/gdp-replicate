package daemon

import (
	"math/rand"
	"time"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"go.uber.org/zap"
)

// Send a heart beat every INTERVAL seconds
func (daemon Daemon) scheduleHeartBeat(interval int, heartBeat heartBeatSender) error {
	zap.S().Infow(
		"scheduling heartbeat",
		"interval", interval,
	)
	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	for _ = range ticker.C {
		err := heartBeat()
		if err != nil {
			return err
		}
	}
	return nil
}

// Sends a heartbeat message to PEER if necessary
func (daemon Daemon) sendHeartBeat(peer gdplogd.Hash) error {
	// Update the graph view before generating message
	daemon.policy.UpdateCurrGraph()
	msg := daemon.policy.GenerateMessage(peer)

	// A msg may not need to be sent
	if msg == nil {
		zap.S().Infow(
			"no heartbeat sent",
			"dst", gdplogd.ReadableAddr(peer),
		)
		return nil
	}

	zap.S().Infow(
		"heart beat sent",
		"dst", gdplogd.ReadableAddr(peer),
		"msg", msg,
	)
	return daemon.network.Send(daemon.myAddr, peer, msg)
}

type heartBeatSender func() error

// Send a heartbeat message to one of daemon peers.
// Cycles through each of the peers
func (daemon Daemon) cycleHeartBeat() error {
	peerIndex := daemon.heartBeatState % len(daemon.peerList)
	peer := daemon.peerList[peerIndex]
	daemon.heartBeatState += 1
	return daemon.sendHeartBeat(peer)
}

// Send a heartbeat message to one of daemon peers.
// Randomly selects a peer with repetition
func (daemon Daemon) randomHeartBeat() error {
	peerIndex := rand.Intn(len(daemon.peerList))
	peer := daemon.peerList[peerIndex]
	return daemon.sendHeartBeat(peer)
}

// fanOutHeartBeat returns a function that sends heartbeats to fanoutDegree peers.
func (daemon Daemon) fanOutHeartBeat(fanoutDegree int) heartBeatSender {
	if fanoutDegree > len(daemon.peerList) {
		zap.S().Fatalf(
			"fanout degree too large for num peers",
			"numPeers", len(daemon.peerList),
			"fanoutDegree", fanoutDegree,
		)
	}
	return func() error {
		randomOrder := rand.Perm(len(daemon.peerList))
		peerIndices := randomOrder[:fanoutDegree]
		zap.S().Infow(
			"sending fanout heart beat",
			"chosen indices", peerIndices,
		)
		for _, peerIndex := range peerIndices {
			err := daemon.sendHeartBeat(daemon.peerList[peerIndex])
			if err != nil {
				return err
			}
		}
		return nil

	}
}
