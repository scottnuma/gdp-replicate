package peers

import (
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"github.com/tonyyanga/gdp-replicate/policy"
	"go.uber.org/zap"
)

// Simple Replication Manager that directly connects to peers
type SimpleReplicateMgr struct {
	// Store the IP:Port address for each peer
	PeerAddrMap map[gdplogd.HashAddr]string
}

// Constructor for SimpleReplicateMgr
func NewSimpleReplicateMgr(peerAddrMap map[gdplogd.HashAddr]string) *SimpleReplicateMgr {
	return &SimpleReplicateMgr{
		PeerAddrMap: peerAddrMap,
	}
}

// ListenAndServe serves HTTP request at ADDRESS with HANDLER
func (mgr *SimpleReplicateMgr) ListenAndServe(address string, handler func(src gdplogd.HashAddr, msg *policy.Message)) error {
	// msgHandler translates HTTP to messages
	msgHandler := func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "POST" {
			http.Error(w, "Only POST requests are supported", 500)
			return
		}

		msgTypeStr := req.Header.Get("MessageType")
		if msgTypeStr == "" {
			http.Error(w, "Expect MessageType in header", 500)
			return
		}

		src := req.Header.Get("Source")
		if src == "" {
			http.Error(w, "Expect Source in header", 500)
			return
		}

		src_, err := hex.DecodeString(src)
		if err != nil {
			http.Error(w, "Corrupted Source", 500)
			return
		}

		msgType, err := strconv.Atoi(msgTypeStr)
		if err != nil {
			http.Error(w, "Corrupted MessageType in header", 500)
			return
		}

		msg := &policy.Message{
			Type: policy.MessageType(msgType),
			Body: req.Body,
		}

		var srcAddr gdplogd.HashAddr
		copy(srcAddr[:], src_[:32])

		zap.S().Infow(
			"received message",
			"msg", msg,
			"srcAddr", gdplogd.ReadableAddr(srcAddr),
		)

		handler(srcAddr, msg)

		io.WriteString(w, "Accepted")
	}

	http.HandleFunc("/", msgHandler)
	return http.ListenAndServe(address, nil)
}

func (mgr *SimpleReplicateMgr) Send(src, peer gdplogd.HashAddr, msg *policy.Message) error {
	// Look up peer's actual IP address
	ipAddr, ok := mgr.PeerAddrMap[peer]
	if !ok {
		return fmt.Errorf("Cannot find peer's address: %v", peer)
	}

	c := &http.Client{}

	// Construct HTTP request
	req, err := http.NewRequest("POST", "http://"+ipAddr, msg.Body)
	if err != nil {
		return err
	}
	req.Header.Add("MessageType", fmt.Sprint(msg.Type))
	req.Header.Add("Source", hex.EncodeToString(src[:]))

	resp, err := c.Do(req)

	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("Response code is not 200: %v", resp.StatusCode)
	}

	return nil
}

func (mgr *SimpleReplicateMgr) Broadcast(src gdplogd.HashAddr, msg *policy.Message) map[gdplogd.HashAddr]error {
	// Dispatch several Send at the same time
	ret := &sync.Map{}
	wg := &sync.WaitGroup{}

	for peer := range mgr.PeerAddrMap {
		wg.Add(1)
		go func(peer gdplogd.HashAddr) {
			defer wg.Done()

			err := mgr.Send(src, peer, msg)
			ret.Store(peer, err)
		}(peer)
	}

	wg.Wait()

	exportMap := make(map[gdplogd.HashAddr]error)
	ret.Range(func(key, value interface{}) bool {
		exportMap[key.(gdplogd.HashAddr)] = value.(error)
		return true
	})
	return exportMap
}
