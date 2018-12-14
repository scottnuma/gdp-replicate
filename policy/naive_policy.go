package policy

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io/ioutil"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"go.uber.org/zap"
)

// PeerStates

const (
	resting          = 0
	initHeartBeat    = 1
	receiveHeartBeat = 2
)

type NaivePolicy struct {
	db      *sql.DB
	name    string
	myState map[gdplogd.Hash]PeerState
}

// Unused for NaivePolicy
func (policy *NaivePolicy) getLogDaemonConnection() gdplogd.LogDaemonConnection {
	return nil
}

// Unused for NaivePolicy
func (policy *NaivePolicy) UpdateCurrGraph() error {
	return nil
}

func (policy *NaivePolicy) GenerateMessage(dest gdplogd.Hash) *Message {
	policy.initPeerIfNeeded(dest)

	// Write every hash into message
	hashes, err := GetAllLogHashes(policy.db)
	if err != nil {
		zap.S().Errorw(
			"Failed to read all log hashes from db",
			"error", err.Error(),
		)
		return nil
	}

	firstMsgReader, err := encodeFirstMsg(hashes)
	if err != nil {
		zap.S().Errorw(
			"Failed to encode log hashes",
			"error", err.Error(),
		)
		return nil
	}

	// change my state to initHeartBeat right before send
	policy.myState[dest] = initHeartBeat
	return &Message{
		Type: first,
		Body: firstMsgReader,
	}
}

func (policy *NaivePolicy) ProcessMessage(
	msg *Message,
	src gdplogd.Hash,
) *Message {
	zap.S().Debugw(
		"processing message",
		"msg", msg,
		"src", gdplogd.ReadableAddr(src),
	)
	policy.initPeerIfNeeded(src)

	myState := policy.myState[src]

	if msg.Type == first && myState == resting {
		return policy.processFirstMsg(msg, src)
	} else if msg.Type == second && myState == initHeartBeat {
		return policy.processSecondMsg(msg, src)
	} else if msg.Type == third && myState == receiveHeartBeat {
		return policy.processThirdMsg(msg, src)
	} else {
		policy.myState[src] = resting
		return nil
	}
}

func (policy *NaivePolicy) processFirstMsg(msg *Message, src gdplogd.Hash) *Message {
	zap.S().Debug("processing first msg")
	// read all their hashes
	theirHashes, err := decodeFirstMsg(msg)
	if err != nil {
		zap.S().Errorw(
			"Error decoding first msg",
			"error", err.Error(),
		)
		policy.myState[src] = resting
		return nil
	}

	// compute my hashes
	myHashes, err := GetAllLogHashes(policy.db)
	if err != nil {
		zap.S().Errorw(
			"Failed to read all log hashes from db",
			"error", err.Error(),
		)
		policy.myState[src] = resting
		return nil
	}

	// find the differences
	onlyMine, onlyTheirs := findDifferences(myHashes, theirHashes)

	// load the logs with hashes that only I have
	onlyMyLogs, err := GetLogEntries(policy.db, onlyMine)

	// send data, requests
	secondMessageBytes, err := json.Marshal(SecondMsgContent{
		LogEntries: onlyMyLogs,
		Hashes:     onlyTheirs,
	})

	policy.myState[src] = receiveHeartBeat
	return &Message{
		Type: second,
		Body: bytes.NewReader(secondMessageBytes),
	}

}

func (policy *NaivePolicy) processSecondMsg(msg *Message, src gdplogd.Hash) *Message {
	zap.S().Debug("processing second msg")
	//parse Message
	secondMsgLogEntries, secondMsgHashes, err := decodeSecondMsg(msg)
	if err != nil {
		zap.S().Errorw(
			"Failed to read bytes from second message",
			"error", err.Error(),
		)
		policy.myState[src] = resting
		return nil
	}

	// load requests
	requestedLogEntries, err := GetLogEntries(policy.db, secondMsgHashes)
	if err != nil {
		zap.S().Errorw(
			"Failed to fetch requested logs",
			"error", err.Error(),
		)
		policy.myState[src] = resting
		return nil
	}

	// save received data
	err = WriteLogEntries(policy.db, secondMsgLogEntries)
	if err != nil {
		zap.S().Errorw(
			"Failed to save given logs",
			"error", err.Error(),
		)
		policy.myState[src] = resting
		return nil
	}

	zap.S().Debugw("requesting log entries",
		"numLogs", len(requestedLogEntries),
	)

	// send data for requests
	thirdMsgBytes, err := json.Marshal(ThirdMsgContent{
		LogEntries: requestedLogEntries,
	})
	policy.myState[src] = resting
	return &Message{
		Type: third,
		Body: bytes.NewReader(thirdMsgBytes),
	}
}

func (policy *NaivePolicy) processThirdMsg(msg *Message, src gdplogd.Hash) *Message {
	zap.S().Debug("processing third msg")

	// save receivved data
	bytesRead, err := ioutil.ReadAll(msg.Body)
	if err != nil {
		zap.S().Errorw(
			"Failed to read bytes from third message",
			"error", err.Error(),
		)
		policy.myState[src] = resting
		return nil
	}
	thirdMsgContent := ThirdMsgContent{}
	err = json.Unmarshal(bytesRead, &thirdMsgContent)
	if err != nil {
		zap.S().Errorw(
			"Failed to decode bytes from third message",
			"error", err.Error(),
		)
		policy.myState[src] = resting
		return nil
	}

	err = WriteLogEntries(policy.db, thirdMsgContent.LogEntries)
	if err != nil {
		zap.S().Errorw(
			"Failed to write provided logs to db",
			"error", err.Error(),
		)
		policy.myState[src] = resting
		return nil
	}

	policy.myState[src] = resting
	return nil
}

func NewNaivePolicy(db *sql.DB, name string) *NaivePolicy {
	return &NaivePolicy{
		db:      db,
		name:    name,
		myState: make(map[gdplogd.Hash]PeerState),
	}
}

func (policy *NaivePolicy) initPeerIfNeeded(peer gdplogd.Hash) {
	_, present := policy.myState[peer]
	if !present {
		policy.myState[peer] = resting
	}
}
