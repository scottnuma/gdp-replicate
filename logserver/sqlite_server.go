package logserver

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/tonyyanga/gdp-replicate/gdp"
)

type SqliteServer struct {
	db *sql.DB
}

func NewSqliteServer(db *sql.DB) *SqliteServer {
	return &SqliteServer{db: db}
}

// ReadRecords will retrieive the records with specified hashes from
// the database.
func (s *SqliteServer) ReadRecords(hashes []gdp.Hash) ([]gdp.Record, error) {
	records := make([]gdp.Record, 0, len(hashes))

	hexHashes := make([]string, 0, len(hashes))
	for hash := range hashes {
		hexHashes = append(hexHashes, fmt.Sprintf("%X", hash))
	}

	queryString := fmt.Sprintf(
		"SELECT hash, recno, timestamp, accuracy, prevhash, value, sig FROM log_entry WHERE hex(hash) IN (%s)",
		strings.Join(hexHashes, ","),
	)
	rows, err := s.db.Query(queryString)
	if err != nil {
		return nil, err
	}

	err = parseRecordRows(rows, records)
	if err != nil {
		return nil, err
	}

	return records, nil
}

// ReadAllRecords will retrieve all records from the database.
func (s *SqliteServer) ReadAllRecords() ([]gdp.Record, error) {
	var records []gdp.Record
	queryString := "SELECT hash, recno, timestamp, accuracy, prevhash, value, sig FROM log_entry"

	rows, err := s.db.Query(queryString)
	if err != nil {
		return nil, err
	}

	err = parseRecordRows(rows, records)
	if err != nil {
		return nil, err
	}

	return records, nil
}

// WriteRecords will write all records to the database.
func (s *SqliteServer) WriteRecords(records []gdp.Record) error {
	return nil
}
