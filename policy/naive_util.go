package policy

import (
	"github.com/tonyyanga/gdp-replicate/gdp"
)

// getAllLogHashes returns a slice of hashes in the graph
func (policy *NaivePolicy) getAllLogHashes() []gdp.Hash {
	hashes := make([]gdp.Hash, 0, len(policy.logGraph.GetNodeMap()))
	for hash, _ := range policy.logGraph.GetNodeMap() {
		hashes = append(hashes, hash)
	}
	return hashes
}

// findDifferences determines which hashes are exclusive to only one list.
// e.g. finding the non-union parts of a Venn diagram
func findDifferences(
	myHashes,
	theirHashes []gdp.Hash,
) (onlyMine []gdp.Hash, onlyTheirs []gdp.Hash) {
	mySet := initSet(myHashes)
	theirSet := initSet(theirHashes)

	for myHash := range mySet {
		_, present := theirSet[myHash]
		if !present {
			onlyMine = append(onlyMine, myHash)
		}
	}
	for theirHash := range theirSet {
		_, present := mySet[theirHash]
		if !present {
			onlyTheirs = append(onlyTheirs, theirHash)
		}
	}

	return onlyMine, onlyTheirs
}

// initSet converts a HashAddr slice to a set
func initSet(hashes []gdp.Hash) map[gdp.Hash]bool {
	set := make(map[gdp.Hash]bool)
	for _, hash := range hashes {
		set[hash] = false
	}
	return set
}
