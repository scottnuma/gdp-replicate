package logserver

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestSqliteReadRecords(t *testing.T) {
	db, err := sql.Open("sqlite3", "/tmp/gdb/simple_long.glob")
	assert.Nil(t, err)

	s := NewSqliteServer(db)
	records, err := s.ReadAllRecords()
	assert.Nil(t, err)
	assert.Equal(t, 5, len(records))

}
