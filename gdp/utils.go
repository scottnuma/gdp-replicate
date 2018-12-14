package gdp

import "encoding/json"

func (record *Record) MarshalBinary() (data []byte, err error) {
	return json.Marshal(record)
}

func (record *Record) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, &record)
}
