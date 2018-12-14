/*
Package gdplogd supports interacting with gdplogd daemon which maintains
the source of truth on the log server
*/
package gdplogd

// A GDP graph supports query with Hash addresses of 32 bytes (256 bits)
type Hash [32]byte

// Multimap for HashAddr
type HashAddrMultiMap map[Hash][]Hash

type Record struct {
	Hash Hash

	// monotonically increasing number that increases by 1 for each new record,
	// represents the count of records starting from the very first record
	RecNo int

	// 64 bit, nanoseconds since 1/1/70
	Timestamp int64

	// in seconds (single precision)
	Accuracy float64
	PrevHash Hash
	Sig      []byte
}

// LogGraphWrapper provides (and caches) typical usage of the Graph
type LogGraphWrapper interface {

	// Node map is a map with keys as all nodes found
	GetNodeMap() map[Hash]int

	// The actual hash pointer map, which follows:
	// A (oldest) <- B <- C (newest)
	GetActualPtrMap() map[Hash]Hash

	// The logical hash pointer map, which follows:
	// A (oldest) -> B -> C (newest)
	GetLogicalPtrMap() HashAddrMultiMap

	// Nodes that have no entry in logical pointer map, e.g. C
	GetLogicalEnds() []Hash

	// Nodes that have dangling entries in the actual map
	// E.g. [X] <- D but there is no entry for X in the actual map; D has a dangling entry
	GetLogicalBegins() []Hash

	// Accept a list of new log items
	AcceptNewLogEntries(entries []Record)
}
