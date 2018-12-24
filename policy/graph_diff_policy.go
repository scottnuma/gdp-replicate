package policy

import (
	"errors"
	"sync"

	"github.com/tonyyanga/gdp-replicate/gdp"
	"github.com/tonyyanga/gdp-replicate/loggraph"
	"go.uber.org/zap"
)

var errConversionError = errors.New("Unable to convert to graph message content")
var errInconsistentStateAndMessage = errors.New("Inconsistent message number and state")
var errUnknownMessageType = errors.New("Unknown message type")

/*
Messages follow the following format

1. "begins\n" if applicable, followed by "<length>\n", then an array of hash addresses
2. "ends\n" if applicable, followed by "<length>\n", then an array of hash addresses
3. "requests\n" if applicable, followed by "<length>\n", then an array of hash addresses
4. "data\n" if applicable, followed by "<length>\n", then an array of hash addresses

TODO(tonyyanga): for data section, put actual data there in addition to hash addresses
*/

// Peer states
const (
	noMsgExchanged PeerState = iota // initial state
	firstMsgSent                    // initiator
	thirdMsgSent
	firstMsgRecved // receiver
	thirdMsgRecved
)

// GraphDiffPolicy is a Policy and uses diff of
// begins and ends of graph to detect differences
// See algorithm spec on Dropbox Paper for more details
type GraphDiffPolicy struct {
	graph loggraph.LogGraph // most up to date graph

	// current graph in use for a specific peer
	// should reset to nil when message exchange ends
	graphInUse map[gdp.Hash]loggraph.LogGraphClone

	// last message sent to the peer
	// used to keep track of message exchanges state
	peerLastMsgType map[gdp.Hash]PeerState

	// mutex for each peer
	peerMutex map[gdp.Hash]*sync.Mutex
}

type GraphMsgContent struct {
	Num            int
	LogicalBegins  []gdp.Hash
	LogicalEnds    []gdp.Hash
	RecordsNotInRX []gdp.Record
	HashesTXWants  []gdp.Hash
}

// Context for a specific peer
type peerPolicyContext struct {
	graph  loggraph.LogGraphClone
	policy *GraphDiffPolicy
}

// NewGraphDiffPolicy constructs policy
func NewGraphDiffPolicy(graph loggraph.LogGraph) *GraphDiffPolicy {
	return &GraphDiffPolicy{
		graph:           graph,
		graphInUse:      make(map[gdp.Hash]loggraph.LogGraphClone),
		peerLastMsgType: make(map[gdp.Hash]PeerState),
		peerMutex:       make(map[gdp.Hash]*sync.Mutex),
	}
}

// initPeerIfNeeded initializes a peer's state for use if necessary.
func (policy *GraphDiffPolicy) initPeerIfNeeded(peer gdp.Hash) {
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

// resetPeerState resets a peer's state to before any contact
func (policy *GraphDiffPolicy) resetPeerStatus(peer gdp.Hash) {
	policy.graphInUse[peer] = nil
	policy.peerLastMsgType[peer] = noMsgExchanged
}

// GenerateMessage begins the heartbeat process with a peer
func (policy *GraphDiffPolicy) GenerateMessage(dest gdp.Hash) (
	interface{},
	error,
) {
	policy.initPeerIfNeeded(dest)

	policy.peerMutex[dest].Lock()
	defer policy.peerMutex[dest].Unlock()

	// update states to firstMsgSent
	clone, err := policy.graph.CreateClone()
	if err != nil {
		zap.S().Errorw(
			"Failed to clone graph",
			"error", err,
		)
		return nil, err
	}

	policy.graphInUse[dest] = clone
	policy.peerLastMsgType[dest] = firstMsgSent

	// generate message
	content := &GraphMsgContent{
		Num:           first,
		LogicalBegins: policy.graphInUse[dest].GetLogicalBegins(),
		LogicalEnds:   policy.graphInUse[dest].GetLogicalEnds(),
	}

	zap.S().Infow("Generate first msg")
	return content, nil
}

func (policy *GraphDiffPolicy) ProcessMessage(src gdp.Hash, packedMsg interface{}) (interface{}, error) {
	zap.S().Debugw(
		"processing message",
		"src", src.Readable(),
	)

	msg, ok := packedMsg.(*GraphMsgContent)
	if !ok {
		return nil, errConversionError
	}
	policy.initPeerIfNeeded(src)

	policy.peerMutex[src].Lock()
	defer policy.peerMutex[src].Unlock()

	peerStatus := policy.peerLastMsgType[src]

	// validate peer status with incoming message
	// if status doesn't match the message type, simply reset the state machine
	switch msg.Num {
	case first:
		if peerStatus != noMsgExchanged {
			policy.resetPeerStatus(src)
			zap.S().Errorw(
				"inconsistent state and msg",
				"peerStatus", peerStatus,
				"msgNum", msg.Num,
			)
			return nil, errInconsistentStateAndMessage
		}

		return policy.processFirstMsg(msg, src)
	case second:
		if peerStatus != firstMsgSent {
			policy.resetPeerStatus(src)
			zap.S().Errorw(
				"inconsistent state and msg",
				"peerStatus", peerStatus,
				"msgNum", msg.Num,
			)
			return nil, errInconsistentStateAndMessage
		}

		return policy.processSecondMsg(msg, src)
	case third:
		if peerStatus != firstMsgRecved {
			policy.resetPeerStatus(src)
			zap.S().Errorw(
				"inconsistent state and msg",
				"peerStatus", peerStatus,
				"msgNum", msg.Num,
			)
			return nil, errInconsistentStateAndMessage
		}

		return policy.processThirdMsg(msg, src)
	case fourth:
		if peerStatus != thirdMsgSent {
			policy.resetPeerStatus(src)
			zap.S().Errorw(
				"inconsistent state and msg",
				"peerStatus", peerStatus,
				"msgNum", msg.Num,
			)
			return nil, errInconsistentStateAndMessage
		}

		return policy.processFourthMsg(msg, src)
	default:
		return nil, errUnknownMessageType
	}

}

// Below are handlers for specific messages
// Handlers assume the mutex of the src is held by caller
func (policy *GraphDiffPolicy) processFirstMsg(msg *GraphMsgContent, src gdp.Hash) (*GraphMsgContent, error) {
	clone, err := policy.graph.CreateClone()
	if err != nil {
		policy.resetPeerStatus(src)
		return nil, err
	}
	policy.graphInUse[src] = clone
	policy.peerLastMsgType[src] = firstMsgRecved

	ctx := policy.getPeerPolicyContext(src)

	// Now that we have peer begins and ends, we start processing
	_, _, peerBeginsNotMatched, peerEndsNotMatched :=
		ctx.compareBeginsEnds(msg.LogicalBegins, msg.LogicalEnds)

	graph := policy.graphInUse[src]
	nodeMap := graph.GetNodeMap()

	nodesToSend := make([]gdp.Hash, 0)

	// Send all nodes before (a node both of us have,
	// but that the peer thinks is a beginning)
	for _, begin := range peerBeginsNotMatched {
		if _, found := nodeMap[begin]; found {
			// Search all nodes ahead of begin to be sent to peer
			visited, _ := ctx.searchAhead(begin, msg.LogicalEnds)
			nodesToSend = append(nodesToSend, visited...)
		}
	}

	// Send all nodes after (a node that both of us have,
	// but that the peer thinks is an end)
	for _, end := range peerEndsNotMatched {
		if _, found := nodeMap[end]; found {
			// Search all nodes after end to be sent to peer
			visited, _ := ctx.searchAfter(end, msg.LogicalBegins)
			nodesToSend = append(nodesToSend, visited...)
		}
	}

	recordsNotInRX, err := policy.graph.ReadRecords(nodesToSend)
	if err != nil {
		policy.resetPeerStatus(src)
		return nil, err
	}

	msgContent := &GraphMsgContent{
		Num:            second,
		RecordsNotInRX: recordsNotInRX,
		LogicalBegins:  graph.GetLogicalBegins(),
		LogicalEnds:    graph.GetLogicalEnds(),
	}

	policy.peerLastMsgType[src] = firstMsgRecved
	zap.S().Infow(
		"Generating second message",
		"numRecords", len(msgContent.RecordsNotInRX),
		"numBegins", len(msgContent.LogicalBegins),
		"numEnds", len(msgContent.LogicalEnds),
	)

	return msgContent, nil
}

func (policy *GraphDiffPolicy) processSecondMsg(msg *GraphMsgContent, src gdp.Hash) (*GraphMsgContent, error) {
	ctx := policy.getPeerPolicyContext(src)

	// Since the data section has been used to update the graph, we can compare digest of the
	// peer's graph with up-to-date information

	myBeginsNotMatched,
		myEndsNotMatched,
		peerBeginsNotMatched,
		peerEndsNotMatched :=
		ctx.compareBeginsEnds(msg.LogicalBegins, msg.LogicalEnds)

	graph := policy.graphInUse[src]
	nodeMap := graph.GetNodeMap()

	nodesToSend := make([]gdp.Hash, 0)
	componentsToSend := make([]gdp.Hash, 0)
	requests := make([]gdp.Hash, 0)

	myBeginsEndsToSend := make(map[gdp.Hash]int)

	for _, begin := range peerBeginsNotMatched {
		if _, found := nodeMap[begin]; found {
			// Search all nodes ahead of begin to be sent to peer
			// If we reach a begin / end of local graph, add to myBeginsEndsToSend
			visited, localEnds := ctx.searchAhead(begin, msg.LogicalEnds)
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
			visited, localEnds := ctx.searchAfter(end, msg.LogicalBegins)
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
	recordsToSend, err := policy.graph.ReadRecords(nodesToSend)
	if err != nil {
		policy.resetPeerStatus(src)
		return nil, err
	}

	resp := &GraphMsgContent{
		Num:            third,
		HashesTXWants:  requests,
		RecordsNotInRX: recordsToSend,
	}

	zap.S().Infow(
		"Generating message third",
	)

	policy.peerLastMsgType[src] = thirdMsgSent
	return resp, nil
}

func (policy *GraphDiffPolicy) processThirdMsg(msg *GraphMsgContent, src gdp.Hash) (*GraphMsgContent, error) {
	ctx := policy.getPeerPolicyContext(src)

	err := policy.graph.WriteRecords(msg.RecordsNotInRX)
	if err != nil {
		policy.resetPeerStatus(src)
		return nil, err
	}

	reqAddrs := msg.HashesTXWants

	// For each addr requested, send the entire connected component
	addrs := ctx.getConnectedAddrs(reqAddrs)
	recordsRXWants, err := policy.graph.ReadRecords(addrs)
	if err != nil {
		policy.resetPeerStatus(src)
		return nil, err
	}

	resp := &GraphMsgContent{
		Num:            fourth,
		RecordsNotInRX: recordsRXWants,
	}

	zap.S().Infow(
		"Generating fourth message",
		"numRecords", len(recordsRXWants),
	)

	// When to revert to netural state?
	policy.peerLastMsgType[src] = thirdMsgRecved

	return resp, nil
}

func (policy *GraphDiffPolicy) processFourthMsg(msg *GraphMsgContent, src gdp.Hash) (*GraphMsgContent, error) {
	err := policy.graph.WriteRecords(msg.RecordsNotInRX)
	if err != nil {
		policy.resetPeerStatus(src)
		return nil, err
	}

	// last message, nothing to respond, reset state
	policy.resetPeerStatus(src)
	return nil, ErrConversationFinished
}
