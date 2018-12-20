package policy

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/tonyyanga/gdp-replicate/gdp"
	"github.com/tonyyanga/gdp-replicate/loggraph"
	"github.com/tonyyanga/gdp-replicate/logserver"
)

const DB_LOC = "/home/scott/go/src/github.com/tonyyanga/gdp-replicate/benchmark/example_db/%s.glob"

func policyFromFile(t *testing.T, dbName string) *NaivePolicy {
	sqlFile := fmt.Sprintf(DB_LOC, dbName)
	fmt.Printf(sqlFile)
	db, err := sql.Open("sqlite3", sqlFile)
	assert.Nil(t, err)

	logServer := logserver.NewSqliteServer(db)
	logGraph, err := loggraph.NewSimpleGraph(logServer)
	assert.Nil(t, err)

	return NewNaivePolicy(logGraph)
}

func TestGenerateMessage(t *testing.T) {
	policyLong := policyFromFile(t, "simple_long")
	policyShort := policyFromFile(t, "simple_short")

	dest := gdp.NullHash
	assert.Equal(t, resting, policyLong.myState[dest])
	assert.Equal(t, resting, policyShort.myState[dest])

	packedMsg, err := policyLong.GenerateMessage(dest)
	msg := packedMsg.(*NaiveMsgContent)
	assert.Nil(t, err)
	assert.Equal(t, first, msg.MsgNum)
	assert.Equal(t, 5, len(msg.HashesAll))
	printHashes(msg.HashesAll)
	assert.Equal(t, initHeartBeat, policyLong.myState[dest])

	packedMsg, err = policyShort.ProcessMessage(gdp.NullHash, msg)
	msg = packedMsg.(*NaiveMsgContent)
	assert.Nil(t, err)
	assert.Equal(t, second, msg.MsgNum)

	printHashes(msg.HashesTheyWant)
	assert.Equal(t, 3, len(msg.HashesTheyWant))
	assert.Equal(t, 0, len(msg.RecordsWeWant))
	assert.Equal(t, receiveHeartBeat, policyShort.myState[dest])

	packedMsg, err = policyLong.ProcessMessage(gdp.NullHash, msg)
	msg = packedMsg.(*NaiveMsgContent)
	assert.Nil(t, err)
	assert.Equal(t, third, msg.MsgNum)
	assert.Equal(t, 3, len(msg.RecordsWeWant))
	assert.Equal(t, resting, policyLong.myState[dest])

	packedMsg, err = policyShort.ProcessMessage(gdp.NullHash, msg)
	msg = packedMsg.(*NaiveMsgContent)
	assert.Nil(t, err)
	assert.Nil(t, msg)
	assert.Equal(t, resting, policyShort.myState[dest])

	packedMsg, err = policyShort.GenerateMessage(gdp.NullHash)
	msg = packedMsg.(*NaiveMsgContent)
	assert.Nil(t, err)
	assert.Equal(t, first, msg.MsgNum)
	assert.Equal(t, 5, len(msg.HashesAll))
	assert.Equal(t, initHeartBeat, policyShort.myState[gdp.NullHash])

	packedMsg, err = policyLong.ProcessMessage(gdp.NullHash, msg)
	msg = packedMsg.(*NaiveMsgContent)
	assert.Nil(t, err)
	assert.Equal(t, second, msg.MsgNum)
	assert.Equal(t, 0, len(msg.HashesTheyWant))
	assert.Equal(t, 0, len(msg.RecordsWeWant))
	assert.Equal(t, receiveHeartBeat, policyLong.myState[dest])
}

func printHashes(hashes []gdp.Hash) {
	fmt.Println(len(hashes))
	for _, hash := range hashes {
		fmt.Println(hash.Readable())
	}
}
