/*
Package policy provides different policies used to

1) generate heartbeat messages that represent the state of gdplogd
2) process heartbeat messages to find out required updates for gdplogd

*/

package policy

import (
	"errors"
	"io"

	"github.com/tonyyanga/gdp-replicate/gdp"
)

type MessageType int

// Message is a data structure to talk to peers
type Message struct {
	Type MessageType
	Body io.Reader
}

// Interface for a Policy that deals with Messages in regard to
// a specific graph
//
// Since messaging between a pair of replication daemons can have
// multiple stages, this Policy interface simply specifies the peer
// involved in message generation. Implementation of the Policy
// interface should keep track of its internal state.
type Policy interface {
	// Get the LogDaemonConnection, implemention should support it
	// Implementations can use this connection to retrieve specific
	// data items from the graph
	// getLogDaemonConnection() gdplogd.LogDaemonConnection

	// UpdateeCurrGraph updates the policy's view of the log
	// UpdateCurrGraph() error

	// Generate message to be sent to a server at dest
	// Used to begin the state machine with a peer at certain timeout
	// Returns nil if a message exchange with the dest server is in progress
	GenerateMessage(dest gdp.Hash) (interface{}, error)

	// Process a message from server at src and construct a return message
	// If no message is needed, return nil
	ProcessMessage(src gdp.Hash, packedMsg interface{}) (interface{}, error)
}

var ErrConversationFinished = errors.New("conversation finished")
