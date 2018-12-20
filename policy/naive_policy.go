package policy

import (
	"errors"

	"github.com/tonyyanga/gdp-replicate/gdp"
	"github.com/tonyyanga/gdp-replicate/loggraph"
	"go.uber.org/zap"
)

// NaivePolicy provides a naive approach to replication through the
// brute force comparison of the hash sets on two peers.
type NaivePolicy struct {
	logGraph loggraph.LogGraph
	name     string
	myState  map[gdp.Hash]PeerState
}

// NaiveMsgContent holds all communication info for naive policy
// peers. All fields are labelled from the perspective of a
// receiver.
type NaiveMsgContent struct {
	MsgNum          int
	HashesAll       []gdp.Hash
	HashesTheyWant  []gdp.Hash
	HashesWeWant    []gdp.Hash
	RecordsTheyWant []gdp.Record
	RecordsWeWant   []gdp.Record
}

// PeerState describes the state of each peer and what messages they
// expect.
type PeerState int

const (
	resting          PeerState = 0
	initHeartBeat    PeerState = 1
	receiveHeartBeat PeerState = 2
)

// types of messages
const (
	first  = 0
	second = 1
	third  = 2
	fourth = 3
)

var (
	errInconsistentStateAndMsgNum = errors.New(
		"expected different msg num based on state",
	)
	errNaiveMsgContentConversion = errors.New(
		"Unable to cast packedMsg to *NaiveMsgContent",
	)
)

func (policy *NaivePolicy) GenerateMessage(
	dest gdp.Hash,
) (*NaiveMsgContent, error) {
	policy.initPeerIfNeeded(dest)

	msg := &NaiveMsgContent{}
	msg.HashesAll = policy.getAllLogHashes()
	msg.MsgNum = first

	policy.myState[dest] = initHeartBeat
	return msg, nil
}

func (policy *NaivePolicy) ProcessMessage(
	src gdp.Hash,
	packedMsg interface{},
) (*NaiveMsgContent, error) {
	zap.S().Debugw(
		"processing message",
		"src", src.Readable(),
	)
	policy.initPeerIfNeeded(src)

	myState := policy.myState[src]

	msg, ok := packedMsg.(*NaiveMsgContent)
	if !ok {
		return nil, errNaiveMsgContentConversion
	}

	if myState == resting && msg.MsgNum == first {
		return policy.processFirstMsg(src, msg)
	} else if myState == initHeartBeat && msg.MsgNum == second {
		return policy.processSecondMsg(src, msg)
	} else if myState == receiveHeartBeat && msg.MsgNum == third {
		return policy.processThirdMsg(src, msg)
	} else {
		zap.S().Errorw(
			"expected different msg based on state",
			"state", myState,
			"msgNum", msg.MsgNum,
		)
		policy.myState[src] = resting
		return nil, errInconsistentStateAndMsgNum
	}
}

func (policy *NaivePolicy) processFirstMsg(
	src gdp.Hash,
	msg *NaiveMsgContent,
) (*NaiveMsgContent, error) {
	zap.S().Debug("processing first msg")

	// compute my hashes
	myHashes := policy.getAllLogHashes()

	// find the differences
	onlyMine, onlyTheirs := findDifferences(myHashes, msg.HashesAll)

	// load the logs with hashes that only I have
	onlyMyLogs, err := policy.logGraph.ReadRecords(onlyMine)
	if err != nil {
		return nil, err
	}

	// send data, requests
	responseContent := &NaiveMsgContent{
		MsgNum:         second,
		HashesTheyWant: onlyTheirs,
		RecordsWeWant:  onlyMyLogs,
	}
	policy.myState[src] = receiveHeartBeat
	return responseContent, nil
}

func (policy *NaivePolicy) processSecondMsg(
	src gdp.Hash,
	msg *NaiveMsgContent,
) (*NaiveMsgContent, error) {
	zap.S().Debug("processing second msg")

	var err error
	resp := &NaiveMsgContent{MsgNum: third}
	resp.RecordsWeWant, err = policy.logGraph.ReadRecords(
		msg.HashesTheyWant,
	)
	if err != nil {
		return nil, err
	}

	// save received data
	err = policy.logGraph.WriteRecords(msg.RecordsWeWant)
	if err != nil {
		zap.S().Errorw(
			"Failed to save given logs",
			"error", err.Error(),
		)
		return nil, err
	}

	// send data for requests
	policy.myState[src] = resting
	return resp, nil
}

func (policy *NaivePolicy) processThirdMsg(
	src gdp.Hash,
	msg *NaiveMsgContent,
) (*NaiveMsgContent, error) {
	zap.S().Debug("processing third msg")

	err := policy.logGraph.WriteRecords(msg.RecordsWeWant)
	if err != nil {
		return nil, err
	}

	policy.myState[src] = resting
	return nil, nil
}

func NewNaivePolicy(
	logGraph loggraph.LogGraph,
	name string,
) *NaivePolicy {
	return &NaivePolicy{
		logGraph: logGraph,
		name:     name,
		myState:  make(map[gdp.Hash]PeerState),
	}
}

func (policy *NaivePolicy) initPeerIfNeeded(peer gdp.Hash) {
	_, present := policy.myState[peer]
	if !present {
		policy.myState[peer] = resting
	}
}
