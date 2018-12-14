package logserver

import (
	"database/sql"

	"github.com/tonyyanga/gdp-replicate/gdp"
)

// parseRecordRows parses sql rows into Records.
//
// records will be mutated to include any read rows, eve upon error.
func parseRecordRows(rows *sql.Rows, records []gdp.Record) error {
	var hashHolder []byte
	var prevHashHolder []byte

	for rows.Next() {
		record := gdp.Record{}
		err := rows.Scan(
			&hashHolder,
			&record.RecNo,
			&record.Timestamp,
			&record.Accuracy,
			&prevHashHolder,
			&record.Value,
			&record.Sig,
		)
		if err != nil {
			return err
		}

		// Copy the byte slices into byte arrays
		copy(record.Hash[:], hashHolder[0:32])

		// Previous hashes may not be populated
		if len(prevHashHolder) > 0 {
			copy(record.PrevHash[:], prevHashHolder[0:32])
		}

		records = append(records, record)
	}
	return nil
}
