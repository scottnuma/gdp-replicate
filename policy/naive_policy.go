package policy

import (
	"errors"

	"github.com/tonyyanga/gdp-replicate/gdp"
	"github.com/tonyyanga/gdp-replicate/loggraph"
	"github.com/tonyyanga/gdp-replicate/logserver"
	"go.uber.org/zap"
)

var errInconsistentStateAndMsgNum = errors.New(
	"expected different msg num based on state",
)

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

// PeerStates

type PeerState int

const (
	resting          = 0
	initHeartBeat    = 1
	receiveHeartBeat = 2
)

const (
	first  = 0
	second = 1
	third  = 2
	fourth = 3
)

type NaivePolicy struct {
	logGraph  loggraph.LogGraph
	logServer logserver.LogServer
	name      string
	myState   map[gdp.Hash]PeerState
}

var errNaiveMsgContentConversion = errors.New("Unable to cast packedMsg to *NaiveMsgContent")

func (policy *NaivePolicy) GenerateMessage(dest gdp.Hash) (*NaiveMsgContent, error) {
	policy.initPeerIfNeeded(dest)

	msg := &NaiveMsgContent{}

	// Write every hash into message
	msg.HashesAll = policy.getAllLogHashes()
	msg.MsgNum = first

	// change my state to initHeartBeat right before send
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
	onlyMyLogs, err := policy.logServer.ReadRecords(onlyMine)
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
	resp.RecordsWeWant, err = policy.logServer.ReadRecords(msg.HashesTheyWant)
	if err != nil {
		return nil, err
	}

	// save received data
	err = policy.logServer.WriteRecords(msg.RecordsWeWant)
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

	err := policy.logServer.WriteRecords(msg.RecordsWeWant)
	if err != nil {
		return nil, err
	}

	policy.myState[src] = resting
	return nil, nil
}

func NewNaivePolicy(
	logServer logserver.LogServer,
	logGraph loggraph.LogGraph,
	name string,
) *NaivePolicy {
	return &NaivePolicy{
		logServer: logServer,
		logGraph:  logGraph,
		name:      name,
		myState:   make(map[gdp.Hash]PeerState),
	}
}

func (policy *NaivePolicy) initPeerIfNeeded(peer gdp.Hash) {
	_, present := policy.myState[peer]
	if !present {
		policy.myState[peer] = resting
	}
}
