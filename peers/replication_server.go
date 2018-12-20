package peers

import "github.com/tonyyanga/gdp-replicate/gdp"

type ReplicationServer interface {
	ListenAndServe(
		address string,
		handler func(src gdp.Hash, msg interface{}),
	) error
	Send(peer gdp.Hash, msg interface{}) error
}
