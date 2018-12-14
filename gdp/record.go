package gdp

type Hash [32]byte

type Record struct {
	Hash      Hash
	RecNo     int
	Timestamp int64
	Accuracy  float64
	PrevHash  Hash
	Value     []byte
	Sig       []byte
}
