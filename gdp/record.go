package gdp

type hash [32]byte

type Record struct {
	Hash      hash
	RecNo     int
	Timestamp int64
	Accuracy  float64
	PrevHash  hash
	Value     []byte
	Sig       []byte
}
