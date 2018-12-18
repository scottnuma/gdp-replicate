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

// ReadRecords will retrieive the metadat of records with specified
// hashes from the database.
func (s *SqliteServer) ReadMetadata(hashes []gdp.Hash) ([]gdp.Metadatum, error) {
	hexHashes := make([]string, 0, len(hashes))
	for _, hash := range hashes {
		hexHashes = append(hexHashes, fmt.Sprintf("\"%X\"", hash))
	}

	queryString := fmt.Sprintf(
		"SELECT hash, recno, timestamp, accuracy, prevhash, sig FROM log_entry WHERE hex(hash) IN (%s)",
		strings.Join(hexHashes, ","),
	)
	fmt.Println(queryString)
	rows, err := s.db.Query(queryString)
	if err != nil {
		return nil, err
	}

	return parseMetadataRows(rows)
}

// ReadRecords will retrieive the records with specified hashes from
// the database.
func (s *SqliteServer) ReadRecords(hashes []gdp.Hash) ([]gdp.Record, error) {
	hexHashes := make([]string, 0, len(hashes))
	for _, hash := range hashes {
		hexHashes = append(hexHashes, fmt.Sprintf("\"%X\"", hash))
	}

	queryString := fmt.Sprintf(
		"SELECT hash, recno, timestamp, accuracy, prevhash, value, sig FROM log_entry WHERE hex(hash) IN (%s)",
		strings.Join(hexHashes, ","),
	)
	rows, err := s.db.Query(queryString)
	if err != nil {
		return nil, err
	}

	records, err := parseRecordRows(rows)
	if err != nil {
		return nil, err
	}

	return records, nil
}

// ReadAllRecords will retrieve all records from the database.
func (s *SqliteServer) ReadAllMetadata() ([]gdp.Metadatum, error) {
	queryString := "SELECT hash, recno, timestamp, accuracy, prevhash, sig FROM log_entry"

	rows, err := s.db.Query(queryString)
	if err != nil {
		return nil, err
	}

	metadata, err := parseMetadataRows(rows)
	if err != nil {
		return nil, err
	}

	return metadata, nil
}

// ReadAllRecords will retrieve all records from the database.
func (s *SqliteServer) ReadAllRecords() ([]gdp.Record, error) {
	queryString := "SELECT hash, recno, timestamp, accuracy, prevhash, value, sig FROM log_entry"

	rows, err := s.db.Query(queryString)
	if err != nil {
		return nil, err
	}

	records, err := parseRecordRows(rows)
	if err != nil {
		return nil, err
	}

	return records, nil
}

// WriteRecords will write all records to the database.
func (s *SqliteServer) WriteRecords(records []gdp.Record) error {
	return nil
}
