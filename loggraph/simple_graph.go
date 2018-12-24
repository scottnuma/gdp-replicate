package loggraph

import (
	"github.com/tonyyanga/gdp-replicate/gdp"
	"github.com/tonyyanga/gdp-replicate/logserver"

	"github.com/jinzhu/copier"
)

type SimpleGraph struct {
	logServer logserver.LogServer
	// All log entries in the database as of last refresh
	forwardEdges  map[gdp.Hash][]gdp.Hash
	backwardEdges map[gdp.Hash]gdp.Hash
	logicalEnds   map[gdp.Hash]bool

	// logicalStarts maps from a record's PrevHash to their Hash
	logicalStarts map[gdp.Hash][]gdp.Hash
	nodeMap       map[gdp.Hash]bool
}

func NewSimpleGraph(logServer logserver.LogServer) (*SimpleGraph, error) {
	simpleGraph := &SimpleGraph{
		logServer:     logServer,
		forwardEdges:  make(map[gdp.Hash][]gdp.Hash),
		backwardEdges: make(map[gdp.Hash]gdp.Hash),
		logicalEnds:   make(map[gdp.Hash]bool),
		logicalStarts: make(map[gdp.Hash][]gdp.Hash),
		nodeMap:       make(map[gdp.Hash]bool),
	}

	metadata, err := simpleGraph.logServer.ReadAllMetadata()
	if err != nil {
		return nil, err
	}

	simpleGraph.addMetadata(metadata)
	return simpleGraph, nil
}

// addMetadata updates all SimpleGraph fields to reflect new Metadata
func (graph *SimpleGraph) addMetadata(metadata []gdp.Metadatum) {
	for _, metadatum := range metadata {
		graph.nodeMap[metadatum.Hash] = true

		// Edges are those between the hashes of two records
		if metadatum.PrevHash != gdp.NullHash {
			graph.backwardEdges[metadatum.Hash] = metadatum.PrevHash

			edges, present := graph.forwardEdges[metadatum.PrevHash]
			if !present {
				graph.forwardEdges[metadatum.PrevHash] = []gdp.Hash{metadatum.Hash}
			} else {
				graph.forwardEdges[metadatum.PrevHash] = append(edges, metadatum.Hash)
			}
		}

		// determine if logical start
		_, present := graph.nodeMap[metadatum.PrevHash]
		if metadatum.PrevHash == gdp.NullHash || !present {
			starts, present := graph.logicalStarts[metadatum.PrevHash]
			if present {
				graph.logicalStarts[metadatum.PrevHash] = append(starts, metadatum.Hash)
			} else {
				graph.logicalStarts[metadatum.PrevHash] = []gdp.Hash{metadatum.Hash}
			}
		}

		// determine if logical end
		_, present = graph.forwardEdges[metadatum.Hash]
		if !present {
			graph.logicalEnds[metadatum.Hash] = true
		}

		// determine if changing a logical start
		delete(graph.logicalStarts, metadatum.Hash)

		// determine if changing a logical end
		delete(graph.logicalEnds, metadatum.PrevHash)
	}
}

func (graph *SimpleGraph) GetNodeMap() map[gdp.Hash]bool {
	return graph.nodeMap
}

func (graph *SimpleGraph) GetActualPtrMap() map[gdp.Hash]gdp.Hash {
	return graph.backwardEdges
}

func (graph *SimpleGraph) GetLogicalPtrMap() map[gdp.Hash][]gdp.Hash {
	return graph.forwardEdges
}

func (graph *SimpleGraph) GetLogicalEnds() []gdp.Hash {
	ends := make([]gdp.Hash, 0, len(graph.logicalEnds))
	for hash, _ := range graph.logicalEnds {
		ends = append(ends, hash)
	}
	return ends
}

func (graph *SimpleGraph) GetLogicalBegins() []gdp.Hash {
	starts := make([]gdp.Hash, 0, len(graph.logicalStarts))
	for _, hashes := range graph.logicalStarts {
		for _, hash := range hashes {
			starts = append(starts, hash)
		}
	}
	return starts
}

// WriteRecords writes records to the graph's log server and
// updates the graph with those records
func (graph *SimpleGraph) WriteRecords(records []gdp.Record) error {
	err := graph.logServer.WriteRecords(records)
	if err != nil {
		return err
	}

	metadata := make([]gdp.Metadatum, 0, len(records))
	for _, record := range records {
		metadata = append(metadata, record.Metadatum)
	}
	graph.addMetadata(metadata)
	return nil
}

func (graph *SimpleGraph) ReadRecords(hashes []gdp.Hash) ([]gdp.Record, error) {
	return graph.logServer.ReadRecords(hashes)
}

// CreateClone uses encoding to clone the SimpleGraph.
func (graph *SimpleGraph) CreateClone() (*SimpleGraphClone, error) {
	forwardEdges := make(map[gdp.Hash][]gdp.Hash)
	backwardEdges := make(map[gdp.Hash]gdp.Hash)
	nodeMap := make(map[gdp.Hash]bool)

	err := copier.Copy(&forwardEdges, &graph.forwardEdges)
	if err != nil {
		return nil, err
	}
	err = copier.Copy(&backwardEdges, &graph.backwardEdges)
	if err != nil {
		return nil, err
	}
	err = copier.Copy(&nodeMap, &graph.nodeMap)
	if err != nil {
		return nil, err
	}

	return &SimpleGraphClone{
		forwardEdges:  forwardEdges,
		backwardEdges: backwardEdges,
		logicalEnds:   graph.GetLogicalEnds(),
		logicalStarts: graph.GetLogicalBegins(),
		nodeMap:       nodeMap,
	}, nil
}
