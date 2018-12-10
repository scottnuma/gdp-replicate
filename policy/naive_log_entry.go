package policy

import (
	"encoding/json"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
)

type LogEntry struct {
	Hash      gdplogd.HashAddr
	RecNo     int
	Timestamp int64
	Accuracy  float64
	PrevHash  gdplogd.HashAddr
	Value     []byte
	Sig       []byte
}

func (logEntry *LogEntry) MarshalBinary() (data []byte, err error) {
	return json.Marshal(logEntry)
}

func (logEntry *LogEntry) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, &logEntry)
}
