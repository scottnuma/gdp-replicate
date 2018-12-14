package policy

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"go.uber.org/zap"
)

// GetAllLogHashes returns a slice of all log hashes in the log server
func GetAllLogHashes(db *sql.DB) ([]gdplogd.Hash, error) {
	allHashes := []gdplogd.Hash{}

	queryString := "select hash from log_entry;"
	rows, err := db.Query(queryString)
	if err != nil {
		return nil, err
	}

	var hash gdplogd.Hash
	var hashHolder []byte
	for rows.Next() {
		err = rows.Scan(&hashHolder)
		if err != nil {
			return nil, err
		}

		// Copy the byte slices into byte arrays
		copy(hash[:], hashHolder[0:32])

		allHashes = append(allHashes, hash)
	}
	return allHashes, nil
}

func WriteLogEntries(db *sql.DB, logEntries []LogEntry) error {
	zap.S().Infow("Attempting to batch write log entries")
	insert_statement := `insert into log_entry(
		hash, recno, timestamp, accuracy, prevhash, value, sig) 
		values(?, ?, ?, ?, ?, ?, ?);`

	tx, err := db.Begin()
	insert, err := tx.Prepare(insert_statement)
	defer insert.Close()

	if err != nil {
		return err
	}

	for _, storedLogEntry := range logEntries {
		_, err = insert.Exec(
			storedLogEntry.Hash[:],
			storedLogEntry.RecNo,
			storedLogEntry.Timestamp,
			storedLogEntry.Accuracy,
			storedLogEntry.PrevHash[:],
			storedLogEntry.Value,
			storedLogEntry.Sig,
		)
		if err != nil {
			return err
		}
		zap.S().Debugw("wrote log entry to db",
			"hash", gdplogd.ReadableAddr(storedLogEntry.Hash),
		)
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func GetLogEntries(db *sql.DB, hashes []gdplogd.Hash) ([]LogEntry, error) {
	logEntries := []LogEntry{}
	for _, hash := range hashes {
		logEntry, err := GetLogEntry(db, hash)
		if err != nil {
			zap.S().Errorw(
				"Failed to load log entry",
				"error", err.Error(),
				"hash", gdplogd.ReadableAddr(hash),
			)
		}
		logEntries = append(logEntries, *logEntry)
	}
	return logEntries, nil
}

func GetLogEntry(db *sql.DB, hash gdplogd.Hash) (*LogEntry, error) {
	var logEntry LogEntry

	queryString := fmt.Sprintf("select hash, recno, timestamp, accuracy, prevhash, value, sig from log_entry where hex(hash) == '%X'", hash)
	rows, err := db.Query(queryString)
	if err != nil {
		return nil, err
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
			&logEntry.Value,
			&logEntry.Sig,
		)
		if err != nil {
			return nil, err
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

// findDifferences determines which hashes are exclusive to only one list.
// e.g. finding the non-union parts of a Venn diagram
func findDifferences(myHashes, theirHashes []gdplogd.Hash) (onlyMine []gdplogd.Hash, onlyTheirs []gdplogd.Hash) {
	mySet := initSet(myHashes)
	theirSet := initSet(theirHashes)

	for myHash := range mySet {
		_, present := theirSet[myHash]
		if !present {
			onlyMine = append(onlyMine, myHash)
		}
	}
	for theirHash := range theirSet {
		_, present := mySet[theirHash]
		if !present {
			onlyTheirs = append(onlyTheirs, theirHash)
		}
	}

	return onlyMine, onlyTheirs
}

// initSet converts a HashAddr slice to a set
func initSet(hashes []gdplogd.Hash) map[gdplogd.Hash]bool {
	set := make(map[gdplogd.Hash]bool)
	for _, hash := range hashes {
		set[hash] = false
	}
	return set
}

type FirstMsgContent struct {
	Hashes []gdplogd.Hash
}

func encodeFirstMsg(hashes []gdplogd.Hash) (io.Reader, error) {
	firstMessageBytes, err := json.Marshal(FirstMsgContent{
		Hashes: hashes,
	})
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(firstMessageBytes), nil
}

func decodeFirstMsg(msg *Message) ([]gdplogd.Hash, error) {
	if msg.Type != first {
		return nil, fmt.Errorf("expected first message but received %d message", msg.Type)
	}

	bytesRead, err := ioutil.ReadAll(msg.Body)
	if err != nil {
		return nil, err
	}

	var firstMsgContent FirstMsgContent
	err = json.Unmarshal(bytesRead, &firstMsgContent)
	if err != nil {
		return nil, err
	}

	return firstMsgContent.Hashes, nil
}

type SecondMsgContent struct {
	LogEntries []LogEntry
	Hashes     []gdplogd.Hash
}

func decodeSecondMsg(msg *Message) ([]LogEntry, []gdplogd.Hash, error) {
	if msg.Type != second {
		return nil, nil, fmt.Errorf("expected second message but received %d message", msg.Type)
	}

	bytesRead, err := ioutil.ReadAll(msg.Body)
	if err != nil {
		return nil, nil, err
	}
	secondMsgContent := SecondMsgContent{}
	err = json.Unmarshal(bytesRead, &secondMsgContent)
	if err != nil {
		return nil, nil, err
	}
	return secondMsgContent.LogEntries, secondMsgContent.Hashes, nil
}

type ThirdMsgContent struct {
	LogEntries []LogEntry
}
