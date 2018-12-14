package peers

import (
	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"github.com/tonyyanga/gdp-replicate/policy"
)

// Entry point to talk to other replication daemons
type ReplicateNetworkMgr interface {
	// handler for incoming messages
	ListenAndServe(address string, handler func(src gdplogd.Hash, msg *policy.Message)) error

	Send(src, peer gdplogd.Hash, msg *policy.Message) error

	Broadcast(src gdplogd.Hash, msg *policy.Message) map[gdplogd.Hash]error
}
