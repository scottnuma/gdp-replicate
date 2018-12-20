package gdp

import (
	"crypto/sha256"
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

func GenerateHash(seed string) Hash {
	return sha256.Sum256([]byte(seed))
}
