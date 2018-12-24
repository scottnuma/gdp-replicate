package peers

import (
	"encoding/gob"
	"errors"
	"net"

	"github.com/tonyyanga/gdp-replicate/gdp"
	"github.com/tonyyanga/gdp-replicate/policy"
	"go.uber.org/zap"
)

var errUnknownPeerAddr = errors.New("peer with unknown addr")

// GobServer is a ReplicationServer that communicates with other
// servers through TCP and gob serialization. One choice behind
// is the high level of abstraction of the network communication
// and serializiation.
type GobServer struct {
	peerAddrs map[gdp.Hash]string
	Addr      gdp.Hash
}

// NewGobServer initializes a GobServer
func NewGobServer(addr gdp.Hash, peerAddrs map[gdp.Hash]string) *GobServer {
	return &GobServer{
		Addr:      addr,
		peerAddrs: peerAddrs,
	}
}

// ListenAndServe makes a GobServer begin listening for connections
// at the specified address. Incoming connections are handled through
// the handler asynchronously.
func (server *GobServer) ListenAndServe(
	address string,
	handler func(src gdp.Hash, msg interface{}),
) error {
	zap.S().Infow(
		"Starting server",
		"address", address,
	)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			zap.S().Errorw(
				"Failed to accept incoming connection",
				"error", err,
			)
			continue
		}
		go func(conn net.Conn) {
			zap.S().Infow(
				"Handling connection",
				"receiver", conn.LocalAddr(),
				"sender", conn.RemoteAddr(),
			)
			defer conn.Close()

			dec := gob.NewDecoder(conn)
			gob.Register(&policy.NaiveMsgContent{})
			gob.Register(&policy.GraphMsgContent{})
			msg := &Message{}
			err := dec.Decode(msg)
			if err != nil {
				zap.S().Errorw(
					"Failed to decode msg",
					"error", err,
				)
				return
			}
			handler(msg.Sender, msg.Content)
		}(conn)
	}
}

// Send sends content to a peer.
// Any type can be used for content, as long as the handler of the
// receiver is expecting that type.
func (server *GobServer) Send(peer gdp.Hash, content interface{}) error {
	ipAddr, present := server.peerAddrs[peer]
	if !present {
		zap.S().Errorw(
			"Failed to resolve peer to addr",
			"peer", peer,
		)
		return errUnknownPeerAddr
	}

	conn, err := net.Dial("tcp", ipAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	msg := Message{
		Sender:  server.Addr,
		Content: content,
	}

	encoder := gob.NewEncoder(conn)
	gob.Register(&policy.NaiveMsgContent{})
	gob.Register(&policy.GraphMsgContent{})

	return encoder.Encode(msg)
}

// Message is the wrapper for communication between peers.
// Messages contain the identifciation of the sender.
type Message struct {
	Sender  gdp.Hash
	Content interface{}
}
