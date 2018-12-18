package logserver

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/tonyyanga/gdp-replicate/gdp"
)

func TestSqliteReadRecords(t *testing.T) {
	DB_DIR := "/home/scott/go/src/github.com/tonyyanga/gdp-replicate/benchmark/example_db"
	dbFile := fmt.Sprintf("%s/simple_long.glob", DB_DIR)
	db, err := sql.Open("sqlite3", dbFile)
	assert.Nil(t, err)

	s := NewSqliteServer(db)
	logServerTest(t, s)

}

func logServerTest(t *testing.T, logServer LogServer) {
	testMetadataReading(t, logServer)
	testRecordReading(t, logServer)

}

func testRecordReading(t *testing.T, logServer LogServer) {
	records, err := logServer.ReadAllRecords()
	assert.Nil(t, err)
	assert.Equal(t, 5, len(records))

	hashes := make([]gdp.Hash, 0, 3)
	for _, record := range records[:3] {
		hashes = append(hashes, record.Hash)
	}

	records, err = logServer.ReadRecords(hashes)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(records))
}

func testMetadataReading(t *testing.T, logServer LogServer) {
	metadata, err := logServer.ReadAllMetadata()
	assert.Nil(t, err)
	assert.Equal(t, 5, len(metadata))

	hashes := make([]gdp.Hash, 0, 3)
	for _, metadatum := range metadata[:3] {
		hashes = append(hashes, metadatum.Hash)
	}

	metadata, err = logServer.ReadMetadata(hashes)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(metadata))
}
