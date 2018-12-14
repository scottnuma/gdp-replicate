package gdplogd

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

// LogGraph descriobe the graph of a DataCapsule on a log server
type LogGraph struct {
	// Address of the DataCapsule
	GraphAddr Hash

	db *sql.DB

	// All log entries in the database as of last refresh
	logEntries []Record

	forwardEdges  HashAddrMultiMap
	backwardEdges map[Hash]Hash
	logicalEnds   []Hash
	logicalBegins []Hash
	nodeMap       map[Hash]int
}

func (logGraph *LogGraph) GetActualPtrMap() map[Hash]Hash {
	return logGraph.backwardEdges
}

func (logGraph *LogGraph) GetLogicalPtrMap() HashAddrMultiMap {
	return logGraph.forwardEdges
}

func (logGraph *LogGraph) GetLogicalEnds() []Hash {
	return logGraph.logicalEnds
}

func (logGraph *LogGraph) GetLogicalBegins() []Hash {
	return logGraph.logicalBegins
}

func (logGraph *LogGraph) GetNodeMap() map[Hash]int {
	return logGraph.nodeMap
}

func (logGraph *LogGraph) AcceptNewLogEntries(entries []Record) {
	logGraph.logEntries = append(logGraph.logEntries, entries...)
	logGraph.forwardEdges, logGraph.backwardEdges = getLogGraphs(
		logGraph.logEntries,
	)

	logGraph.CalcLogicalEnds()
	logGraph.CalcLogicalBegins()
	logGraph.CalcNodeMap()
}

// Return LogGraph and calculate its logical represntation
func InitLogGraph(graphAddr Hash, db *sql.DB) (LogGraph, error) {
	var logGraph LogGraph
	logGraph.GraphAddr = graphAddr
	logGraph.db = db

	err := logGraph.RefreshLogGraph()
	if err != nil {
		return logGraph, err
	}

	return logGraph, nil
}

// Update log properties from the logs database.
// Specifically updates logEntries, forwardEdges, backwardEdges
func (logGraph *LogGraph) RefreshLogGraph() error {
	logEntries, err := logGraph.GetAllLogs()
	if err != nil {
		return err
	}

	logGraph.logEntries = logEntries
	logGraph.forwardEdges, logGraph.backwardEdges = getLogGraphs(
		logGraph.logEntries,
	)

	logGraph.CalcLogicalEnds()
	logGraph.CalcLogicalBegins()
	logGraph.CalcNodeMap()

	return nil
}

func (logGraph *LogGraph) CalcNodeMap() {
	logGraph.nodeMap = make(map[Hash]int)

	for key := range logGraph.backwardEdges {
		logGraph.nodeMap[key] = 1
	}
}

func getLogGraphs(logEntries []Record) (forwardEdges HashAddrMultiMap, backwardEdges map[Hash]Hash) {
	forwardEdges = make(HashAddrMultiMap)
	backwardEdges = make(map[Hash]Hash)

	for _, logEntry := range logEntries {
		backwardEdges[logEntry.Hash] = logEntry.PrevHash

		nodeForwardEdges, present := forwardEdges[logEntry.PrevHash]

		if !present {
			forwardEdges[logEntry.PrevHash] = []Hash{logEntry.Hash}
		} else {
			forwardEdges[logEntry.PrevHash] = append(nodeForwardEdges, logEntry.Hash)
		}
	}
	return forwardEdges, backwardEdges
}

func (logGraph *LogGraph) CalcLogicalEnds() {
	logicalEnds := []Hash{}

	for _, logEntry := range logGraph.logEntries {
		_, present := logGraph.forwardEdges[logEntry.Hash]
		if !present {
			logicalEnds = append(logicalEnds, logEntry.Hash)
		}
	}

	logGraph.logicalEnds = logicalEnds
}

func (logGraph *LogGraph) CalcLogicalBegins() {
	logicalBegins := []Hash{}

	var emptyHashAddr Hash

	for _, logEntry := range logGraph.logEntries {
		// logEntries that are the start of a chain are logical begins
		if logEntry.PrevHash == emptyHashAddr {
			logicalBegins = append(logicalBegins, logEntry.Hash)
			continue
		}

		_, present := logGraph.backwardEdges[logEntry.PrevHash]
		if !present {
			logicalBegins = append(logicalBegins, logEntry.Hash)
		}
	}

	logGraph.logicalBegins = logicalBegins
}

// Return all log entries in the database
func (logGraph *LogGraph) GetAllLogs() ([]Record, error) {
	rows, err := logGraph.db.Query("select hash, recno, timestamp, accuracy, prevhash, sig from log_entry")
	if err != nil {
		return nil, err
	}

	var logEntries []Record
	var hashHolder []byte
	var prevHashHolder []byte
	for rows.Next() {
		var logEntry Record
		err = rows.Scan(
			&hashHolder,
			&logEntry.RecNo,
			&logEntry.Timestamp,
			&logEntry.Accuracy,
			&prevHashHolder,
			&logEntry.Sig,
		)
		if err != nil {
			// Return the log entries read so far with the error
			return logEntries, err
		}

		// Copy the byte slices into byte arrays
		copy(logEntry.Hash[:], hashHolder[0:32])

		// Previous hashes may not be populated
		if len(prevHashHolder) > 0 {
			copy(logEntry.PrevHash[:], prevHashHolder[0:32])
		}

		logEntries = append(logEntries, logEntry)
	}

	return logEntries, nil
}

// Return log entry with hash
func (logGraph *LogGraph) GetLog(hash []byte) (Record, error) {
	db := logGraph.db
	var logEntry Record

	queryString := fmt.Sprintf("select hash, recno, timestamp, accuracy, prevhash, value, sig from log_entry where hex(hash) == '%X'", hash)
	rows, err := db.Query(queryString)
	if err != nil {
		return logEntry, err
	}

	for rows.Next() {
		err = rows.Scan(
			&logEntry.Hash,
			&logEntry.RecNo,
			&logEntry.Timestamp,
			&logEntry.Accuracy,
			&logEntry.PrevHash,
			&logEntry.Sig,
		)
		if err != nil {
			return logEntry, err
		}
	}

	return logEntry, nil
}

// Determine if a log entry with a specific hash is present in the database
func (logGraph *LogGraph) HashPresent(hash Hash) (bool, error) {
	db := logGraph.db
	queryString := fmt.Sprintf("select count(hash) from log_entry where hex(hash) == '%X'\n", hash)
	rows, err := db.Query(queryString)
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

// Add LogEntries to the database.
func (logGraph *LogGraph) AppendLogEntry(logEntries []Record) error {
	db := logGraph.db
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	insert_statement := `insert into log_entry(
		hash, recno, timestamp, accuracy, prevhash, sig) 
		values(?, ?, ?, ?, ?, ?);`
	insert, err := tx.Prepare(insert_statement)
	if err != nil {
		return err
	}

	defer insert.Close()

	for _, storedLogEntry := range logEntries {
		_, err = insert.Exec(
			storedLogEntry.Hash,
			storedLogEntry.RecNo,
			storedLogEntry.Timestamp,
			storedLogEntry.Accuracy,
			storedLogEntry.PrevHash,
			storedLogEntry.Sig,
		)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

// Create the table as used in GDP.
func CreateTable(db *sql.DB) error {
	create_table_statement := `
    CREATE TABLE log_entry (
            hash BLOB(32) PRIMARY KEY ON CONFLICT IGNORE,
            recno INTEGER, 
            timestamp INTEGER,
            accuracy FLOAT,
            prevhash BLOB(32),
            value BLOB,
            sig BLOB);
	`
	_, err := db.Exec(create_table_statement)
	return err
}
