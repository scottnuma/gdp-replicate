package peers

import (
	"github.com/tonyyanga/gdp-replicate/gdp"
	"github.com/tonyyanga/gdp-replicate/policy"
)

// Entry point to talk to other replication daemons
type ReplicateNetworkMgr interface {
	// handler for incoming messages
	ListenAndServe(address string, handler func(src gdp.Hash, msg *policy.Message)) error

	Send(src, peer gdp.Hash, msg *policy.Message) error

	Broadcast(src gdp.Hash, msg *policy.Message) map[gdp.Hash]error
}
