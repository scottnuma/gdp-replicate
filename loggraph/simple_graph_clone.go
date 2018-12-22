package loggraph

import "github.com/tonyyanga/gdp-replicate/gdp"

type SimpleGraphClone struct {
	forwardEdges  map[gdp.Hash][]gdp.Hash
	backwardEdges map[gdp.Hash]gdp.Hash
	logicalEnds   []gdp.Hash
	logicalStarts []gdp.Hash
	nodeMap       map[gdp.Hash]bool
}

func (graph *SimpleGraphClone) GetNodeMap() map[gdp.Hash]bool {
	return graph.nodeMap
}

func (graph *SimpleGraphClone) GetActualPtrMap() map[gdp.Hash]gdp.Hash {
	return graph.backwardEdges
}

func (graph *SimpleGraphClone) GetLogicalPtrMap() map[gdp.Hash][]gdp.Hash {
	return graph.forwardEdges
}

func (graph *SimpleGraphClone) GetLogicalEnds() []gdp.Hash {
	return graph.logicalEnds
}

func (graph *SimpleGraphClone) GetLogicalBegins() []gdp.Hash {
	return graph.logicalEnds
}
