package gdp

import (
	"encoding/json"
	"fmt"
)

func (record *Record) MarshalBinary() (data []byte, err error) {
	return json.Marshal(record)
}

func (record *Record) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, &record)
}

func (hash Hash) Readable() string {
	return fmt.Sprintf("%X", hash)[:4]
}
