package policy

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tonyyanga/gdp-replicate/gdplogd"
)

const SQL_FILE = "/home/scott/go/src/github.com/tonyyanga/gdp-replicate/gdplogd/sample.glog"

func TestGetAllLogHashes(t *testing.T) {
	db, err := sql.Open("sqlite3", SQL_FILE)
	assert.Nil(t, err)
	defer db.Close()

	hashes, err := GetAllLogHashes(db)
	assert.Nil(t, err)
	assert.NotEqual(t, 0, len(hashes))
	for hash := range hashes {
		t.Logf("%X\n", hash)
	}
}

func aTestWriteLogEntry(t *testing.T) {
	db, err := sql.Open("sqlite3", SQL_FILE)
	assert.Nil(t, err)
	defer db.Close()

	logEntries := []LogEntry{
		LogEntry{
			Hash:      gdplogd.Hash{},
			RecNo:     1,
			Timestamp: 2,
			Accuracy:  2,
			PrevHash:  gdplogd.Hash{},
			Value:     []byte{},
			Sig:       []byte{},
		},
	}
	assert.Nil(t, WriteLogEntries(db, logEntries))
}

func TestReadLogEntry(t *testing.T) {
	db, err := sql.Open("sqlite3", SQL_FILE)
	assert.Nil(t, err)
	defer db.Close()

	logEntry, err := GetLogEntry(db, gdplogd.Hash{})
	assert.Nil(t, err)
	assert.Equal(t, 1, logEntry.RecNo)
}

func TestBinaryConversion(t *testing.T) {
	hash := gdplogd.Hash{}
	hash[0] = 1
	prevHash := gdplogd.Hash{}
	prevHash[1] = 1
	logEntry := LogEntry{
		Hash:      hash,
		RecNo:     1,
		Timestamp: 2,
		Accuracy:  2,
		PrevHash:  prevHash,
		Value:     []byte{},
		Sig:       []byte{},
	}
	newLogEntry := LogEntry{}
	data, err := logEntry.MarshalBinary()

	assert.Nil(t, err)
	assert.Nil(t, newLogEntry.UnmarshalBinary(data))
	assert.Equal(t, logEntry.Hash, newLogEntry.Hash)
	assert.Equal(t, logEntry.PrevHash, newLogEntry.PrevHash)
	lgs := []LogEntry{logEntry}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	assert.Nil(t, enc.Encode(lgs))
}

func TestSecondMessageContentConversion(t *testing.T) {
	hash := gdplogd.Hash{}
	hash[0] = 1
	prevHash := gdplogd.Hash{}
	prevHash[1] = 1
	logEntry := LogEntry{
		Hash:      hash,
		RecNo:     1,
		Timestamp: 2,
		Accuracy:  2,
		PrevHash:  prevHash,
		Value:     []byte{},
		Sig:       []byte{},
	}
	s := SecondMsgContent{
		[]LogEntry{logEntry},
		[]gdplogd.Hash{hash, prevHash},
	}

	b, err := json.Marshal(s)
	sPrime := SecondMsgContent{}
	err = json.Unmarshal(b, &sPrime)
	assert.Nil(t, err)
	assert.Equal(t, sPrime.LogEntries[0].RecNo, 1)
	assert.Equal(t, len(sPrime.LogEntries), 1)
	assert.Equal(t, len(sPrime.Hashes), 2)
	assert.Equal(t, sPrime.Hashes[0], hash)
	assert.NotEqual(t, sPrime.Hashes[0], prevHash)
}

func TestGenerateMessage(t *testing.T) {
	db, err := sql.Open("sqlite3", SQL_FILE)
	assert.Nil(t, err)

	policy := NewNaivePolicy(db, "default")

	var dest gdplogd.Hash
	msg := policy.GenerateMessage(dest)
	bytesRead, err := ioutil.ReadAll(msg.Body)
	assert.Nil(t, err)

	var firstMessageContent FirstMsgContent
	assert.Nil(t, json.Unmarshal(bytesRead, &firstMessageContent))
	assert.Equal(t, 9, len(firstMessageContent.Hashes))
}

func TestProcessFirstMsg(t *testing.T) {
	db, err := sql.Open("sqlite3", SQL_FILE)
	assert.Nil(t, err)

	policy := NewNaivePolicy(db, "default")
	var addr gdplogd.Hash
	firstMsg := policy.GenerateMessage(addr)
	secondMsg := policy.processFirstMsg(firstMsg, addr)

	bytesRead, err := ioutil.ReadAll(secondMsg.Body)
	assert.Nil(t, err)
	secondMessageContent := SecondMsgContent{}
	assert.Nil(t, json.Unmarshal(bytesRead, &secondMessageContent))
	assert.Equal(t, 0, len(secondMessageContent.LogEntries))
	assert.Equal(t, 0, len(secondMessageContent.Hashes))
}

func TestExchange(t *testing.T) {
	dbA, err := sql.Open("sqlite3", "/tmp/gdp/simple_long.glob")
	assert.Nil(t, err)
	dbB, err := sql.Open("sqlite3", "/tmp/gdp/simple_short.glob")
	assert.Nil(t, err)
	policyA := NewNaivePolicy(dbA, "A")
	hashA := gdplogd.Hash{}
	hashA[0] = 1
	policyB := NewNaivePolicy(dbB, "B")
	hashB := gdplogd.Hash{}
	hashB[1] = 1
	firstMsg := policyA.GenerateMessage(hashB)

	// do checks on msgA
	assert.Equal(t, PeerState(initHeartBeat), policyA.myState[hashB])
	assert.NotNil(t, firstMsg)

	secondMsg := policyB.ProcessMessage(firstMsg, hashA)
	assert.NotNil(t, secondMsg)
	assert.Equal(t, PeerState(receiveHeartBeat), policyB.myState[hashA])

	assert.NotNil(t, policyB)
}
