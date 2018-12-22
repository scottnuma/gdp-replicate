package loggraph

import (
	"github.com/tonyyanga/gdp-replicate/gdp"
)

// LogGraph provides an abstracted view of records in the database.
// Users of LogGraph should not have to interface with LogServer.
type LogGraph interface {

	// Node map is a map with keys as all nodes found
	GetNodeMap() map[gdp.Hash]bool

	// The actual hash pointer map, which follows:
	// A (oldest) <- B <- C (newest)
	GetActualPtrMap() map[gdp.Hash]gdp.Hash

	// The logical hash pointer map, which follows:
	// A (oldest) -> B -> C (newest)
	GetLogicalPtrMap() map[gdp.Hash][]gdp.Hash

	// Nodes that have no entry in logical pointer map, e.g. C
	GetLogicalEnds() []gdp.Hash

	// Nodes that have dangling entries in the actual map
	// E.g. [X] <- D but there is no entry for X in the actual map; D has a dangling entry
	GetLogicalBegins() []gdp.Hash

	// WriteRecords writes new records to the log server
	WriteRecords(records []gdp.Record) error

	// ReadRecords returns records with hashes
	ReadRecords(hashes []gdp.Hash) ([]gdp.Record, error)

	// CreateClone creates a static read only version of the graph
	CreateClone() (*SimpleGraphClone, error)
}

// LogGraphClone provides a static view of the state of a LogGraph at one time
type LogGraphClone interface {
	GetNodeMap() map[gdp.Hash]bool
	GetActualPtrMap() map[gdp.Hash]gdp.Hash
	GetLogicalPtrMap() map[gdp.Hash][]gdp.Hash
	GetLogicalEnds() []gdp.Hash
	GetLogicalBegins() []gdp.Hash
}
