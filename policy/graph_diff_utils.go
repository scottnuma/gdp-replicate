package policy

import "github.com/tonyyanga/gdp-replicate/gdp"

// Get peer policy context
func (policy *GraphDiffPolicy) getPeerPolicyContext(peer gdp.Hash) *peerPolicyContext {
	return &peerPolicyContext{
		graph:  policy.graphInUse[peer],
		policy: policy,
	}
}

// Return all connected hash addresses in the graph from a list of requested
// This function should handle deduplication
func (ctx *peerPolicyContext) getConnectedAddrs(addrs []gdp.Hash) []gdp.Hash {
	empty := []gdp.Hash{}

	result := make(map[gdp.Hash]int)

	_getConnected := func(addr gdp.Hash) {
		// Add addr itself
		result[addr] = 1

		prev, _ := ctx.searchAhead(addr, empty)
		for _, node := range prev {
			result[node] = 1
		}

		next, _ := ctx.searchAfter(addr, empty)
		for _, node := range next {
			result[node] = 1
		}
	}

	for _, addr := range addrs {
		_getConnected(addr)
	}

	ret := []gdp.Hash{}
	for key := range result {
		ret = append(ret, key)
	}

	return ret
}

// Traverse ahead in the graph starting from "start". Traversal on a certain path ends when meeting a node in
// "terminals"
// Return:
//   a list of hash addresses visited, not including start or terminals
//   a list of begins / ends in local graph reached
func (ctx *peerPolicyContext) searchAhead(start gdp.Hash, terminals []gdp.Hash) ([]gdp.Hash, []gdp.Hash) {
	actualMap := ctx.graph.GetActualPtrMap()
	terminalMap := initSet(terminals)

	visited := make([]gdp.Hash, 0)
	localEnds := make([]gdp.Hash, 0)

	current := start
	prev, found := actualMap[current]
	for found {
		if _, terminate := terminalMap[current]; terminate {
			// early termination because reaching terminal
			return visited, localEnds
		}

		current = prev
		prev, found = actualMap[current]

		// Do not store the last pointer on the map
		if found {
			visited = append(visited, current)
		}
	}

	localEnds = append(localEnds, current)
	return visited, localEnds
}

// Traverse after in the graph starting from "start". Traversal on a certain path ends when meeting a node in
// "terminals"
// Return:
//   a list of hash addresses visited, not including start or terminals
//   a list of begins / ends in local graph reached
func (ctx *peerPolicyContext) searchAfter(start gdp.Hash, terminals []gdp.Hash) ([]gdp.Hash, []gdp.Hash) {
	return ctx._searchAfter(start, initSet(terminals))
}

func (ctx *peerPolicyContext) _searchAfter(start gdp.Hash, terminals map[gdp.Hash]bool) ([]gdp.Hash, []gdp.Hash) {
	logicalMap := ctx.graph.GetLogicalPtrMap()

	// Use recursion since we may have branches, start is never included
	after, found := logicalMap[start]

	// base cases
	if _, terminate := terminals[start]; terminate {
		return []gdp.Hash{}, []gdp.Hash{}
	}

	if !found {
		return []gdp.Hash{}, []gdp.Hash{start}
	}

	visited := []gdp.Hash{}
	localEnds := make([]gdp.Hash, 0)
	for _, node := range after {
		visited_, localEnds_ := ctx._searchAfter(node, terminals)
		visited = append(visited, node)
		visited = append(visited, visited_...)
		localEnds = append(localEnds, localEnds_...)
	}

	return visited, localEnds
}

// Compare peer's begins and ends with my own.
// Return in the following order:
//   local begins not matched
//   local ends not matched
//   peer begins not matched
//   peer ends not matched
func (ctx *peerPolicyContext) compareBeginsEnds(
	peerBegins,
	peerEnds []gdp.Hash,
) ([]gdp.Hash, []gdp.Hash, []gdp.Hash, []gdp.Hash) {
	localBegins := ctx.graph.GetLogicalBegins()
	localEnds := ctx.graph.GetLogicalEnds()
	localBeginsRet, peerBeginsRet := findDifferences(localBegins, peerBegins)
	localEndsRet, peerEndsRet := findDifferences(localEnds, peerEnds)
	return localBeginsRet, localEndsRet, peerBeginsRet, peerEndsRet
}
