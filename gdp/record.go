package gdp

type Hash [32]byte

var NullHash = Hash{}

type Record struct {
	Metadatum
	Value []byte
}
type Metadatum struct {
	Hash      Hash
	RecNo     int
	Timestamp int64
	Accuracy  float64
	PrevHash  Hash
	Value     []byte
	Sig       []byte
}
