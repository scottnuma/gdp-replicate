package gdp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecordSerialization(t *testing.T) {
	record := &Record{
		Metadatum: Metadatum{
			Hash:      Hash{},
			RecNo:     1,
			Timestamp: 2,
			Accuracy:  3.4,
			PrevHash:  Hash{},
			Sig:       []byte{},
		},
		Value: []byte{},
	}

	recordBytes, err := record.MarshalBinary()
	assert.Nil(t, err)

	newRecord := &Record{}
	assert.Nil(t, newRecord.UnmarshalBinary(recordBytes))
	assert.Equal(t, record.RecNo, newRecord.RecNo)
	assert.Equal(t, record.Timestamp, newRecord.Timestamp)
}
