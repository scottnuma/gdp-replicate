package policy

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"sync"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"go.uber.org/zap"
)

// Message Types, four messages are required in chronological order
const (
	first  = 0
	second = 1
	third  = 2
	fourth = 3
)

/*
Messages follow the following format

1. "begins\n" if applicable, followed by "<length>\n", then an array of hash addresses
2. "ends\n" if applicable, followed by "<length>\n", then an array of hash addresses
3. "requests\n" if applicable, followed by "<length>\n", then an array of hash addresses
4. "data\n" if applicable, followed by "<length>\n", then an array of hash addresses

TODO(tonyyanga): for data section, put actual data there in addition to hash addresses
*/

type PeerState int

// Peer states
const (
	noMsgExchanged = 0 // initial state

	firstMsgSent // initiator
	thirdMsgSent

	firstMsgRecved // receiver
	thirdMsgRecved
)

// GraphDiffPolicy implements the Policy interface and uses diff of
// begins and ends of graph to detect differences
// See algorithm spec on Dropbox Paper for more details
// TODO(tonyyanga): move the algorithm here
type GraphDiffPolicy struct {
	conn gdplogd.LogDaemonConnection // connection

	name string // name for the graph

	currentGraph gdplogd.LogGraphWrapper // most up to date graph

	// current graph in use for a specific peer
	// should reset to nil when message exchange ends
	graphInUse map[gdplogd.Hash]gdplogd.LogGraphWrapper

	// last message sent to the peer
	// used to keep track of message exchanges state
	peerLastMsgType map[gdplogd.Hash]PeerState

	// mutex for each peer
	peerMutex map[gdplogd.Hash]*sync.Mutex
}

// Context for a specific peer
type peerPolicyContext struct {
	graph gdplogd.LogGraphWrapper

	policy *GraphDiffPolicy
}

// Constructor with log daemon connection and initial graph

func NewGraphDiffPolicy(conn gdplogd.LogDaemonConnection, name string, graph gdplogd.LogGraphWrapper) *GraphDiffPolicy {
	return &GraphDiffPolicy{
		conn:            conn,
		name:            name,
		currentGraph:    graph,
		graphInUse:      make(map[gdplogd.Hash]gdplogd.LogGraphWrapper),
		peerLastMsgType: make(map[gdplogd.Hash]PeerState),
		peerMutex:       make(map[gdplogd.Hash]*sync.Mutex),
	}
}

func (policy *GraphDiffPolicy) getLogDaemonConnection() gdplogd.LogDaemonConnection {
	return policy.conn
}

// UpdateCurrGraph updates the currentGraph from the connection.
// Traverses the entire database
func (policy *GraphDiffPolicy) UpdateCurrGraph() error {
	newGraph, err := policy.conn.GetGraph(policy.name)
	if err != nil {
		return err
	}
	policy.currentGraph = newGraph
	return nil
}

func (policy *GraphDiffPolicy) initPeerIfNeeded(peer gdplogd.Hash) {
	mutex, ok := policy.peerMutex[peer]
	if !ok {
		policy.peerMutex[peer] = &sync.Mutex{}
		mutex = policy.peerMutex[peer]
	}

	mutex.Lock()
	defer mutex.Unlock()

	_, ok = policy.graphInUse[peer]
	if !ok {
		policy.graphInUse[peer] = nil
	}

	_, ok = policy.peerLastMsgType[peer]
	if !ok {
		policy.peerLastMsgType[peer] = noMsgExchanged
	}
}

func (policy *GraphDiffPolicy) resetPeerStatus(peer gdplogd.Hash) {
	policy.graphInUse[peer] = nil
	policy.peerLastMsgType[peer] = noMsgExchanged
}

func (policy *GraphDiffPolicy) GenerateMessage(dest gdplogd.Hash) *Message {
	// only create a message if the dest is at noMsgExchanged state
	// otherwise return nil
	policy.initPeerIfNeeded(dest)

	policy.peerMutex[dest].Lock()
	defer policy.peerMutex[dest].Unlock()

	if policy.peerLastMsgType[dest] != noMsgExchanged {
		return nil
	}

	// update states to firstMsgSent
	policy.graphInUse[dest] = policy.currentGraph
	policy.peerLastMsgType[dest] = firstMsgSent

	// generate message
	var buf bytes.Buffer
	buf.WriteString("begins\n")
	begins := policy.currentGraph.GetLogicalBegins()
	addrListToReader(begins, &buf)

	buf.WriteString("ends\n")
	ends := policy.currentGraph.GetLogicalEnds()
	addrListToReader(ends, &buf)

	zap.S().Infof("Generate msg %v", first)
	zap.S().Infow(
		"Generating message 0 WERFOIJJ",
		"numBegins", len(begins),
		"numEnds", len(ends),
	)

	return &Message{
		Type: first,
		Body: &buf,
	}
}

func (policy *GraphDiffPolicy) ProcessMessage(msg *Message, src gdplogd.Hash) *Message {
	zap.S().Debugw(
		"processing message",
		"msg", msg,
		"src", gdplogd.ReadableAddr(src),
	)
	policy.initPeerIfNeeded(src)

	policy.peerMutex[src].Lock()
	defer policy.peerMutex[src].Unlock()

	peerStatus := policy.peerLastMsgType[src]

	// validate peer status with incoming message
	// if status doesn't match the message type, simply reset the state machine
	if msg.Type == first {
		if peerStatus != noMsgExchanged {
			policy.resetPeerStatus(src)
			return nil
		}

		return policy.processFirstMsg(msg, src)
	} else if msg.Type == second {
		if peerStatus != firstMsgSent {
			policy.resetPeerStatus(src)
			return nil
		}

		return policy.processSecondMsg(msg, src)
	} else if msg.Type == third {
		if peerStatus != firstMsgRecved {
			policy.resetPeerStatus(src)
			return nil
		}

		return policy.processThirdMsg(msg, src)
	} else if msg.Type == fourth {
		if peerStatus != thirdMsgSent {
			policy.resetPeerStatus(src)
			return nil
		}

		return policy.processFourthMsg(msg, src)
	} else {
		panic(fmt.Sprintf("Reach unknown message type: %v", msg.Type))
	}

}

// Below are handlers for specific messages
// Handlers assume the mutex of the src is held by caller
func (policy *GraphDiffPolicy) processFirstMsg(msg *Message, src gdplogd.Hash) *Message {
	peerBegins, peerEnds, err := processBeginsEnds(msg.Body)

	if err != nil {
		// Message corrupted
		log.Printf("%v", err)
		policy.resetPeerStatus(src)
		return nil
	}

	policy.graphInUse[src] = policy.currentGraph
	policy.peerLastMsgType[src] = firstMsgRecved

	ctx := policy.getPeerPolicyContext(src)

	// Now that we have peer begins and ends, we start processing
	_, _, peerBeginsNotMatched, peerEndsNotMatched := ctx.compareBeginsEnds(peerBegins, peerEnds)

	graph := policy.graphInUse[src]
	nodeMap := graph.GetNodeMap()

	nodesToSend := make([]gdplogd.Hash, 0)

	for _, begin := range peerBeginsNotMatched {
		if _, found := nodeMap[begin]; found {
			// Search all nodes ahead of begin to be sent to peer
			visited, _ := ctx.searchAhead(begin, peerEnds)
			nodesToSend = append(nodesToSend, visited...)
		}
	}

	for _, end := range peerEndsNotMatched {
		if _, found := nodeMap[end]; found {
			// Search all nodes after end to be sent to peer
			visited, _ := ctx.searchAfter(end, peerBegins)
			nodesToSend = append(nodesToSend, visited...)
		}
	}

	var buf bytes.Buffer
	buf.WriteString("data\n")
	err = ctx.constructDataSection(nodesToSend, &buf)
	if err != nil {
		policy.resetPeerStatus(src)
		log.Printf("%v", err)
		return nil
	}

	begins := graph.GetLogicalBegins()
	buf.WriteString("begins\n")
	addrListToReader(begins, &buf)

	ends := graph.GetLogicalEnds()
	buf.WriteString("ends\n")
	addrListToReader(ends, &buf)

	policy.peerLastMsgType[src] = firstMsgRecved
	zap.S().Infof("Generate msg %v", second)
	zap.S().Infow(
		"Generating message 1 SDLKJKJ",
		"numRecords", len(nodesToSend),
		"numBegins", len(begins),
		"numEnds", len(ends),
	)

	return &Message{
		Type: second,
		Body: &buf,
	}
}

func (policy *GraphDiffPolicy) processSecondMsg(msg *Message, src gdplogd.Hash) *Message {
	ctx := policy.getPeerPolicyContext(src)

	reader := bufio.NewReader(msg.Body)
	line, err := reader.ReadBytes('\n')

	if err != nil || bytes.Compare(line, []byte("data\n")) != 0 {
		// Message is corrupted
		policy.resetPeerStatus(src)
		return nil
	}

	ctx.processDataSection(reader)

	// Since the data section has been used to update the graph, we can compare digest of the
	// peer's graph with up-to-date information
	peerBegins, peerEnds, err := processBeginsEnds(reader)

	if err != nil {
		// Message corrupted
		policy.resetPeerStatus(src)
		return nil
	}

	myBeginsNotMatched, myEndsNotMatched, peerBeginsNotMatched, peerEndsNotMatched := ctx.compareBeginsEnds(peerBegins, peerEnds)

	graph := policy.graphInUse[src]
	nodeMap := graph.GetNodeMap()

	nodesToSend := make([]gdplogd.Hash, 0)
	componentsToSend := make([]gdplogd.Hash, 0)
	requests := make([]gdplogd.Hash, 0)

	myBeginsEndsToSend := make(map[gdplogd.Hash]int)

	for _, begin := range peerBeginsNotMatched {
		if _, found := nodeMap[begin]; found {
			// Search all nodes ahead of begin to be sent to peer
			// If we reach a begin / end of local graph, add to myBeginsEndsToSend
			visited, localEnds := ctx.searchAhead(begin, peerEnds)
			nodesToSend = append(nodesToSend, visited...)

			for _, node := range localEnds {
				myBeginsEndsToSend[node] = 1
			}
		} else {
			// Add the entire connected component to request
			requests = append(requests, begin)
		}
	}

	for _, end := range peerEndsNotMatched {
		if _, found := nodeMap[end]; found {
			// Search all nodes ahead of begin to be sent to peer
			// If we reach a begin / end of local graph, add to myBeginsEndsToSend
			visited, localEnds := ctx.searchAfter(end, peerBegins)
			nodesToSend = append(nodesToSend, visited...)

			for _, node := range localEnds {
				myBeginsEndsToSend[node] = 1
			}
		} else {
			// Add the entire connected component to request
			requests = append(requests, end)
		}
	}

	for _, begin := range myBeginsNotMatched {
		if _, found := myBeginsEndsToSend[begin]; !found {
			// Add the connected component to nodesToSend
			componentsToSend = append(componentsToSend, begin)
		}
	}

	for _, end := range myEndsNotMatched {
		if _, found := myBeginsEndsToSend[end]; !found {
			// Add the connected component to nodesToSend
			componentsToSend = append(componentsToSend, end)
		}
	}

	componentsToSend = ctx.getConnectedAddrs(componentsToSend)
	nodesToSend = append(nodesToSend, componentsToSend...)

	var buf bytes.Buffer
	buf.WriteString("requests\n")
	addrListToReader(requests, &buf)

	buf.WriteString("data\n")
	err = ctx.constructDataSection(nodesToSend, &buf)
	if err != nil {
		policy.resetPeerStatus(src)
		return nil
	}

	zap.S().Infow(
		"Generating message 2 NWFEIJ",
		"numRecords", len(nodesToSend),
		"numRequests", len(requests),
	)

	policy.peerLastMsgType[src] = thirdMsgSent

	zap.S().Infof("Generate msg %v", third)
	return &Message{
		Type: third,
		Body: &buf,
	}
}

func (policy *GraphDiffPolicy) processThirdMsg(msg *Message, src gdplogd.Hash) *Message {
	ctx := policy.getPeerPolicyContext(src)

	reader := bufio.NewReader(msg.Body)
	line, err := reader.ReadBytes('\n')

	if err != nil || bytes.Compare(line, []byte("requests\n")) != 0 {
		// Message is corrupted
		policy.resetPeerStatus(src)
		return nil
	}

	reqAddrs, err := addrListFromReader(reader)
	if err != nil {
		policy.resetPeerStatus(src)
		return nil
	}

	// For each addr requested, send the entire connected component
	addrs := ctx.getConnectedAddrs(reqAddrs)

	retBody := &bytes.Buffer{}
	retBody.WriteString("data\n")
	err = ctx.constructDataSection(addrs, retBody)
	if err != nil {
		policy.resetPeerStatus(src)
		return nil
	}

	zap.S().Infow(
		"Generating message 3 SDFW",
		"numRecords", len(addrs),
	)
	ret := &Message{
		Type: fourth,
		Body: retBody,
	}

	line, err = reader.ReadBytes('\n')

	if err != nil || bytes.Compare(line, []byte("data\n")) != 0 {
		// Message is corrupted
		policy.resetPeerStatus(src)
		return nil
	}

	ctx.processDataSection(reader)

	policy.peerLastMsgType[src] = thirdMsgRecved
	zap.S().Infof("Generate msg %v", fourth)

	return ret
}

func (policy *GraphDiffPolicy) processFourthMsg(msg *Message, src gdplogd.Hash) *Message {
	ctx := policy.getPeerPolicyContext(src)

	reader := bufio.NewReader(msg.Body)
	line, err := reader.ReadBytes('\n')

	if err != nil || bytes.Compare(line, []byte("data\n")) != 0 {
		// Message is corrupted
		policy.resetPeerStatus(src)
		return nil
	}

	ctx.processDataSection(reader)

	// last message, nothing to respond, reset state
	policy.resetPeerStatus(src)

	return nil
}
