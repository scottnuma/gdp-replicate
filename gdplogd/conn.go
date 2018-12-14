package gdplogd

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"

	"go.uber.org/zap"
)

// LogDaemonConnection represents a connection to a log daemon.
type LogDaemonConnection interface {
	GetGraphs() (map[string]LogGraphWrapper, error)

	GetConnection() *sql.DB
	GetGraph(name string) (LogGraphWrapper, error)

	ReadLogMetadata(name string, addr Hash) (*Record, error)
	ReadLogItem(name string, addr Hash) (io.Reader, error)

	WriteLogItem(name string, metadata *Record, data io.Reader) error

	ContainsLogItem(name string, addr Hash) (bool, error)
}

// LogDaemonConnector supports only a single database per instance
type LogDaemonConnector struct {
	db     *sql.DB
	graphs map[string]LogGraphWrapper
}

// InitLogDaemonConnector initializes LogDaemonConnector and its LogGraph.
func InitLogDaemonConnector(db *sql.DB, name string) (LogDaemonConnector, error) {
	var conn LogDaemonConnector
	conn.db = db
	conn.graphs = make(map[string]LogGraphWrapper)

	logGraph, err := InitLogGraph(Hash{}, conn.db)
	if err != nil {
		return conn, err
	}
	conn.graphs[name] = &logGraph
	return conn, nil
}

func (conn LogDaemonConnector) GetConnection() *sql.DB {
	return conn.db
}

// GetGraphs returns all graphs of the LogDaemonConnector
func (conn LogDaemonConnector) GetGraphs() (map[string]LogGraphWrapper, error) {
	return conn.graphs, nil
}

// GetGraph returns the graph representation of NAME database
func (conn LogDaemonConnector) GetGraph(name string) (LogGraphWrapper, error) {
	newGraph, err := InitLogGraph(Hash{}, conn.db)
	return &newGraph, err
}

// ReadLogMetadata returns the log entry metadata with ADDR from database with NAME.
func (conn LogDaemonConnector) ReadLogMetadata(
	name string,
	addr Hash,
) (*Record, error) {
	var logEntry Record

	queryString := fmt.Sprintf(
		"select hash, recno, timestamp, accuracy, prevhash, sig from log_entry where hex(hash) == '%X'",
		addr,
	)
	rows, err := conn.db.Query(queryString)
	if err != nil {
		return &logEntry, err
	}

	var hashHolder []byte
	var prevHashHolder []byte

	for rows.Next() {
		err = rows.Scan(
			&hashHolder,
			&logEntry.RecNo,
			&logEntry.Timestamp,
			&logEntry.Accuracy,
			&prevHashHolder,
			&logEntry.Sig,
		)
		if err != nil {
			return &logEntry, err
		}

		// Copy the byte slices into byte arrays
		copy(logEntry.Hash[:], hashHolder[0:32])

		// Previous hashes may not be populated
		if len(prevHashHolder) > 0 {
			copy(logEntry.PrevHash[:], prevHashHolder[0:32])
		}

	}

	return &logEntry, nil
}

// Return the log entry value with ADDR from database with NAME.
func (conn LogDaemonConnector) ReadLogItem(name string, addr Hash) (io.Reader, error) {
	var value []byte

	queryString := fmt.Sprintf("select value from log_entry where hex(hash) == '%X'", addr)
	rows, err := conn.db.Query(queryString)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		err = rows.Scan(&value)
		if err != nil {
			return nil, err
		}
	}

	return bytes.NewReader(value), nil
}

// Add LogEntries to the database.
func (conn LogDaemonConnector) WriteLogItem(name string, logEntry *Record, data io.Reader) error {
	tx, err := conn.db.Begin()
	if err != nil {
		return err
	}

	insert_statement := `insert into log_entry(
		hash, recno, timestamp, accuracy, prevhash, value, sig) 
		values(?, ?, ?, ?, ?, ?, ?);`
	insert, err := tx.Prepare(insert_statement)
	if err != nil {
		return err
	}

	defer insert.Close()

	value, err := ioutil.ReadAll(data)
	if err != nil {
		return err
	}

	_, err = insert.Exec(
		logEntry.Hash[:],
		logEntry.RecNo,
		logEntry.Timestamp,
		logEntry.Accuracy,
		logEntry.PrevHash[:],
		value,
		logEntry.Sig,
	)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	zap.S().Infow(
		"wrote log item to database",
		"hash", ReadableAddr(logEntry.Hash),
		"prevHash", ReadableAddr(logEntry.PrevHash),
	)
	return nil
}

// ContainsLogItem determines if a log entry with a specific hash is present in the database
func (conn LogDaemonConnector) ContainsLogItem(name string, addr Hash) (bool, error) {
	queryString := fmt.Sprintf("select count(hash) from log_entry where hex(hash) == '%X'\n", addr)
	rows, err := conn.db.Query(queryString)
	if err != nil {
		return false, err
	}

	var hashPresent int
	for rows.Next() {
		err = rows.Scan(&hashPresent)
		if err != nil {
			return false, err
		}
	}
	return hashPresent == 1, nil
}
