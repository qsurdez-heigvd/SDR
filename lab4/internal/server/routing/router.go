package routing

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/messages"
	"chatsapp/internal/server/pulsing"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils"
	"fmt"
)

// Router is an interface for sending messages to any node in the network, or to broadcast to all nodes in the network,
// even if the network is not fully connected.
type Router interface {
	// Broadcast broadcasts a message to all nodes in the network, blocking until all nodes have received it,
	// and returning the list of nodes that received it.
	Broadcast(msg messages.Message) (receivers []transport.Address, err error)

	// Send sends a message to a specific destination address, routing it through intermediate nodes as necessary.
	// It may not block.
	Send(msg messages.Message, dest transport.Address) error

	// ReceivedMessageChan returns a channel on which messages received from the network are delivered.
	ReceivedMessageChan() <-chan receivedMessage
}

type routedSendReq struct {
	RoutedMessage
	response chan error
}

// receivedMessage is a message received by the router, along with the source address
type receivedMessage = messages.Sourced[messages.Message]

// routingTable maps destination addresses to the next-hop address to reach them
type routingTable = map[transport.Address]transport.Address

// router is the local implementation of the Router interface
type router struct {
	logger    *logging.Logger
	self      transport.Address
	neighbors []transport.Address

	// Pulsar for network-wide operations
	pulsar pulsing.Pulsar[messages.Message]

	// Network communication
	routerToNet  chan<- messages.Destined[RoutedMessage]
	routingTable routingTable

	// Internal channels
	receivedChan     chan<- receivedMessage
	sendRequestsChan chan<- routedSendReq
	messagesOutChan  <-chan receivedMessage
}

// NewRouter creates a new router instance.
func NewRouter(
	logger *logging.Logger,
	self transport.Address,
	neighbors []transport.Address,
	routerToNet chan<- messages.Destined[RoutedMessage],
	netToRouter <-chan messages.Sourced[RoutedMessage],
	pulsarBuilder pulsing.Builder[messages.Message],
) Router {
	initialTable := buildInitialRoutingTable(neighbors)
	return newRouter(logger, self, routerToNet, netToRouter, initialTable, pulsarBuilder)
}

// newRouter is the internal constructor for the router implementation.
func newRouter(
	logger *logging.Logger,
	self transport.Address,
	routerToNet chan<- messages.Destined[RoutedMessage],
	netToRouter <-chan messages.Sourced[RoutedMessage],
	table routingTable,
	pulsarBuilder pulsing.Builder[messages.Message],
) *router {

	BroadcastRequest{}.RegisterToGob()
	BroadcastResponse{}.RegisterToGob()
	ExplorationRequest{}.RegisterToGob()
	ExplorationResponse{}.RegisterToGob()

	sendRequestsChan := utils.NewBufferedChan[routedSendReq]()
	receivedChan := utils.NewBufferedChan[receivedMessage]()

	r := &router{
		logger:           logger.WithPostfix("Router"),
		self:             self,
		routerToNet:      routerToNet,
		routingTable:     table,
		sendRequestsChan: sendRequestsChan.Inlet(),
		receivedChan:     receivedChan.Inlet(),
		messagesOutChan:  receivedChan.Outlet(),
	}

	r.pulsar = pulsarBuilder.Build(
		logger.WithPostfix("Pulsar"),
		r.handlePulse,
		r.aggregateEchoes,
	)

	go r.handleSendRequests(sendRequestsChan.Outlet())
	go r.handleIncomingMessage(netToRouter)

	return r

}

func buildInitialRoutingTable(neighbors []transport.Address) routingTable {
	table := make(routingTable, len(neighbors))
	for _, neighbor := range neighbors {
		table[neighbor] = neighbor
	}
	return table
}

// getRoutingTable returns the current routing table; used in tests only.
func (r *router) getRoutingTable() routingTable {
	return r.routingTable
}

func (r *router) handlePulse(
	pulseID pulsing.PulseID,
	messageReceived messages.Message,
	from transport.Address,
) (propagated messages.Message, err error) {
	r.logger.Infof("Handling pulse from %s with ID %v", from, pulseID)

	if req, ok := messageReceived.(BroadcastRequest); ok {
		r.receivedChan <- receivedMessage{
			Message: req.Msg,
			From:    from,
		}
	}
	return messageReceived, nil
}

func (r *router) aggregateEchoes(
	pulseID pulsing.PulseID,
	messageReceived messages.Message,
	echoes []pulsing.SourcedMessage[messages.Message],
) (propagated messages.Message, err error) {
	r.logger.Infof("Aggregate echoes from %s with ID %v", pulseID, echoes)

	switch messageReceived.(type) {
	case ExplorationRequest:
		return r.aggregateExplorationEchoes(echoes)
	case BroadcastRequest:
		return r.aggregateBroadcastEchoes(echoes)
	default:
		return messageReceived, nil
	}
}

func (r *router) aggregateBroadcastEchoes(
	echoes []pulsing.SourcedMessage[messages.Message],
) (propagated messages.Message, err error) {
	receivers := []transport.Address{r.self}

	for _, echo := range echoes {
		receivers = append(receivers, echo.From)
	}

	return BroadcastResponse{Receivers: receivers}, nil
}

func (r *router) aggregateExplorationEchoes(
	echoes []pulsing.SourcedMessage[messages.Message],
) (propagated messages.Message, err error) {
	merged := make(routingTable)
	for _, echo := range echoes {
		r.mergeTableFromEcho(merged, echo)
	}
	r.routingTable = merged
	return ExplorationResponse{RoutingTable: merged}, nil
}

func (r *router) mergeTableFromEcho(merged routingTable, echo pulsing.SourcedMessage[messages.Message]) {
	echoResponse, ok := echo.Msg.(ExplorationResponse)
	if !ok {
		r.logger.Warnf("Received message with unexpected type %T", echo.Msg)
		return
	}

	for dest := range echoResponse.RoutingTable {
		if _, exists := merged[dest]; !exists {
			merged[dest] = echo.From
		}
	}

	if echo.From != r.self {
		merged[echo.From] = echo.From
	}
}

func (r *router) handleSendRequests(sendRequestChan <-chan routedSendReq) {
	for req := range sendRequestChan {
		err := r.processSendRequest(req)
		req.response <- err
	}
}

func (r *router) processSendRequest(req routedSendReq) error {
	if req.To == r.self {
		r.logger.Errorf("Trying to send message to self: %v", req)
		return fmt.Errorf("trying to send message to self: %v", req)
	}

	nextHop, exists := r.routingTable[req.To]
	if exists {
		return r.forwardMessage(req.RoutedMessage, nextHop)
	}

	return r.exploreAndSend(req)
}

func (r *router) forwardMessage(msg RoutedMessage, nextHop transport.Address) error {
	r.logger.Infof("Forwarding message to %v via %v", nextHop, msg.To)

	r.routerToNet <- messages.Destined[RoutedMessage]{
		Message: msg,
		To:      nextHop,
	}

	return nil
}

func (r *router) exploreAndSend(req routedSendReq) error {

	if err := r.exploreNetwork(); err != nil {
		r.logger.Errorf("Failed to explore network: %v", err)
		return err
	}

	nextHop, exists := r.routingTable[req.To]
	if !exists {
		return fmt.Errorf("desination not found for %v", req.To)
	}

	r.logger.Infof("Routing message to next hop %v after exploration", nextHop)
	return r.forwardMessage(req.RoutedMessage, nextHop)
}

func (r *router) exploreNetwork() error {
	resp, err := r.pulsar.StartPulse(ExplorationRequest{})
	if err != nil {
		r.logger.Errorf("Failed to start pulse for exploration: %v", err)
		return err
	}

	exploration, ok := resp.(ExplorationResponse)
	if !ok {
		r.logger.Errorf("Unexcpected type of response: %v", resp)
		return fmt.Errorf("unexcpected type of response: %v", resp)
	}

	r.routingTable = exploration.RoutingTable
	return nil
}

func (r *router) handleIncomingMessage(netToRouter <-chan messages.Sourced[RoutedMessage]) {
	for msg := range netToRouter {
		r.processIncomingMessage(msg)
	}
}

func (r *router) processIncomingMessage(msg messages.Sourced[RoutedMessage]) {
	if msg.Message.To != r.self {
		r.forwardReceivedMessage(msg)
	} else {
		r.deliverReceivedMessage(msg)
	}
}

func (r *router) forwardReceivedMessage(msg messages.Sourced[RoutedMessage]) {
	r.logger.Infof("Forwarding received message from %v to %v", msg.From, msg.Message.To)

	resp := make(chan error)
	req := routedSendReq{
		RoutedMessage: msg.Message,
		response:      resp,
	}

	r.sendRequestsChan <- req
	if err := <-resp; err != nil {
		r.logger.Errorf("Failed to send message to %v: %v", msg.From, err)
	}
}

func (r *router) deliverReceivedMessage(msg messages.Sourced[RoutedMessage]) {
	r.logger.Infof("Received message from %v to %v", msg.From, msg.Message.To)

	r.receivedChan <- receivedMessage{
		From:    msg.Message.From,
		Message: msg.Message.Msg,
	}
}

// Broadcast implements the Router interface method.
func (r *router) Broadcast(msg messages.Message) (receivers []transport.Address, err error) {
	r.logger.Infof("Beginning broadcast of %v", msg)

	resp, err := r.pulsar.StartPulse(BroadcastRequest{Msg: msg})
	if err != nil {
		return nil, err
	}

	finalEcho, ok := resp.(BroadcastResponse)
	if !ok {
		r.logger.Errorf("Unexcpected type of response: %v", resp)
		return nil, fmt.Errorf("unexcpected type of response: %v", resp)
	}

	r.logger.Infof("Broadcast to %v nodes", len(finalEcho.Receivers))
	return finalEcho.Receivers, nil
}

// ReceivedMessageChan implements the Router interface method.
func (r *router) ReceivedMessageChan() <-chan messages.Sourced[messages.Message] {
	return r.messagesOutChan
}

// Send implements the Router interface method.
func (r *router) Send(msg messages.Message, dest transport.Address) error {
	response := make(chan error)
	req := routedSendReq{
		RoutedMessage: RoutedMessage{
			Msg:  msg,
			From: r.self,
			To:   dest,
		},
		response: response,
	}

	r.sendRequestsChan <- req
	return <-response
}
