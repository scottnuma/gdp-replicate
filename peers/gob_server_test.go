package peers

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tonyyanga/gdp-replicate/gdp"
)

func TestGobServer(t *testing.T) {
	fmt.Println("Starting test")

	serverAddr := "localhost:8000"
	peerAddrs := make(map[gdp.Hash]string)
	peerAddrs[gdp.NullHash] = serverAddr

	server := NewGobServer(gdp.NullHash, peerAddrs)

	var receivedMsg string

	go server.ListenAndServe(serverAddr, func(src gdp.Hash, msg interface{}) {
		fmt.Println("from src", src.Readable(), "content", msg.(string))
		receivedMsg = msg.(string)
	})
	assert.Nil(t, server.Send(gdp.NullHash, "hello there"))
	time.Sleep(1 * time.Millisecond)
	assert.Equal(t, "hello there", receivedMsg)
	fmt.Println("Finishing test")
}
