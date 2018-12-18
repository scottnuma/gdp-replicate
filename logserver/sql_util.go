package logserver

import (
	"database/sql"

	"github.com/tonyyanga/gdp-replicate/gdp"
)

// parseRecordRows parses sql rows into Records.
func parseRecordRows(rows *sql.Rows) ([]gdp.Record, error) {
	var hashHolder []byte
	var prevHashHolder []byte
	var records []gdp.Record

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
			return nil, err
		}

		// Copy the byte slices into byte arrays
		copy(record.Hash[:], hashHolder[0:32])

		// Previous hashes may not be populated
		if len(prevHashHolder) > 0 {
			copy(record.PrevHash[:], prevHashHolder[0:32])
		}

		records = append(records, record)
	}
	return records, nil
}

// parseMetadataRows parses sql rows into Record Metadata
func parseMetadataRows(rows *sql.Rows) ([]gdp.Metadatum, error) {
	var hashHolder []byte
	var prevHashHolder []byte
	var metadata []gdp.Metadatum

	for rows.Next() {
		metadatum := gdp.Metadatum{}
		err := rows.Scan(
			&hashHolder,
			&metadatum.RecNo,
			&metadatum.Timestamp,
			&metadatum.Accuracy,
			&prevHashHolder,
			&metadatum.Sig,
		)
		if err != nil {
			return nil, err
		}

		// Copy the byte slices into byte arrays
		copy(metadatum.Hash[:], hashHolder[0:32])

		// Previous hashes may not be populated
		if len(prevHashHolder) > 0 {
			copy(metadatum.PrevHash[:], prevHashHolder[0:32])
		}

		metadata = append(metadata, metadatum)
	}
	return metadata, nil
}
