package routing

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/messages"
	"chatsapp/internal/server/pulsing"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils"
	"reflect"
	"testing"
	"time"
)

func getTestAddr(number uint16) transport.Address {
	return transport.Address{IP: "127.0.0.1", Port: 5000 + number}
}

type mockMessage struct {
	Payload string
}

func (m mockMessage) RegisterToGob() {}

type routerWrapper struct {
	*router
}

func (r routerWrapper) expectRoutingTable(t *testing.T, expected routingTable) {
	actual := r.getRoutingTable()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Unexpected routing table. Expected %v, got %v", expected, actual)
	}
}

func (r routerWrapper) expectMessage(t *testing.T, msg messages.Message, from transport.Address) {
	received := <-r.ReceivedMessageChan()
	actualMsg := received.Message
	actualFrom := received.From

	if actualMsg != msg {
		t.Errorf("Unexpected message. Expected %v, got %v", msg, actualMsg)
	}

	if actualFrom != from {
		t.Errorf("Unexpected sender. Expected %v, got %v", from, actualFrom)
	}
}

type mockNet struct {
	fromRouter chan messages.Destined[RoutedMessage]
	intoRouter chan messages.Sourced[RoutedMessage]
	fromPulsar chan pulsing.SentMessage[messages.Message]
	intoPulsar chan pulsing.ReceivedMessage[messages.Message]
}

func newMockNet() mockNet {
	m := mockNet{
		fromRouter: make(chan messages.Destined[RoutedMessage], 100),
		intoRouter: make(chan messages.Sourced[RoutedMessage], 100),
		fromPulsar: make(chan pulsing.SentMessage[messages.Message], 100),
		intoPulsar: make(chan pulsing.ReceivedMessage[messages.Message], 100),
	}

	return m
}

func (n mockNet) SimulateReception(msg RoutedMessage, from transport.Address) {
	n.intoRouter <- messages.Sourced[RoutedMessage]{Message: msg, From: from}
}

func (n mockNet) ExpectSentMessage(msg RoutedMessage, to transport.Address) {
	actual := <-n.fromRouter

	if actual.Message != msg {
		panic("Unexpected message")
	}

	if actual.To != to {
		panic("Unexpected destination")
	}
}

func (n mockNet) ExpectNothingFor(d time.Duration) {
	select {
	case <-n.fromRouter:
		panic("Unexpected message")
	case <-time.After(d):
	}
}

func createRouter(t *testing.T, self uint16, neighbors []uint16) (mockNet, *pulsing.MockPulsar, *routerWrapper) {
	return createRouterWithRoutingTable(t, self, neighbors, make(routingTable))
}

func createRouterWithRoutingTable(t *testing.T, self uint16, neighbors []uint16, routingTable routingTable) (mockNet, *pulsing.MockPulsar, *routerWrapper) {
	neighborAddrs := make([]transport.Address, len(neighbors))
	for i, n := range neighbors {
		neighborAddrs[i] = getTestAddr(n)
	}

	net := newMockNet()

	pulsarBuilder := pulsing.NewMockPulsarBuilder(t)
	pulsarBuilder.SetNetConnection(getTestAddr(self), neighborAddrs, net.fromPulsar, net.intoPulsar)

	router := newRouter(logging.NewStdLogger("router"), getTestAddr(self), net.fromRouter, net.intoRouter, routingTable, pulsarBuilder)

	return net, pulsarBuilder.GetPulsar(), &routerWrapper{router: router}
}

// TODO test Broadcasting

func TestBroadcastRequest(t *testing.T) {
	_, pulsar, router := createRouter(t, 0, []uint16{1, 2})

	msg := mockMessage{Payload: "test"}
	actualReceivers := make(chan []transport.Address)
	go func() {
		receivers, err := router.Broadcast(msg)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		actualReceivers <- receivers
	}()

	expectedReceivers := []transport.Address{getTestAddr(1), getTestAddr(2)}
	pulsar.ExpectSend(
		BroadcastRequest{Msg: msg},
		BroadcastResponse{Receivers: expectedReceivers},
	)

	if !utils.SliceContainsSame(<-actualReceivers, expectedReceivers) {
		t.Errorf("Unexpected receivers. Expected %v, got %v", expectedReceivers, actualReceivers)
	}
}

func TestBroadcastResponse(t *testing.T) {
	_, pulsar, router := createRouter(t, 0, []uint16{1, 2})

	go func() {
		router.expectMessage(t, mockMessage{Payload: "test"}, getTestAddr(1))
	}()

	msg := mockMessage{Payload: "test"}
	pulsar.SimulatePulseHandler(
		pulsing.NewPulseID(getTestAddr(1), 0),
		BroadcastRequest{Msg: msg}, getTestAddr(1),
		BroadcastRequest{Msg: msg},
	)
}

// Tests that sending to an unknown address triggers an exploration
func TestExplorationRequest(t *testing.T) {
	disp, pulsar, router := createRouter(t, 0, []uint16{1, 2})

	destAddr := getTestAddr(3)
	msg := mockMessage{Payload: "Hello, world!"}
	go func() {
		err := router.Send(msg, destAddr)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}()

	disp.ExpectNothingFor(100 * time.Millisecond)

	table := routingTable{
		destAddr:       getTestAddr(1),
		getTestAddr(2): getTestAddr(2),
		getTestAddr(1): getTestAddr(1),
	}
	pulsar.ExpectSend(
		ExplorationRequest{},
		ExplorationResponse{
			RoutingTable: table,
		},
	)

	routedMsg := RoutedMessage{Msg: msg, From: getTestAddr(0), To: destAddr}
	disp.ExpectSentMessage(routedMsg, getTestAddr(1))

	router.expectRoutingTable(t, table)
}

// Tests that receiving an exploration pulse propagates the pulse
func TestExplorationPropagation(t *testing.T) {
	_, pulsar, router := createRouter(t, 0, []uint16{1, 2})

	go func() {
		router.expectMessage(t, ExplorationRequest{}, getTestAddr(1))
	}()

	pulseID := pulsing.NewPulseID(getTestAddr(1), 0)
	pulsar.SimulatePulseHandler(pulseID, ExplorationRequest{}, getTestAddr(1), ExplorationRequest{})
}

// Tests that exploration echoes are aggregated correctly and update the routing table accordingly
func TestExplorationEchoAggregation(t *testing.T) {
	/*
		1 - 0 - 3 - 4 - 5
		    |
		    2
	*/

	_, pulsar, router := createRouter(t, 0, []uint16{1, 2, 3})

	pulseSource := getTestAddr(1)
	pulseID := pulsing.NewPulseID(pulseSource, 0)
	explorationRequest := ExplorationRequest{}

	pulsar.SimulatePulseHandler(pulseID, explorationRequest, pulseSource, explorationRequest)

	childrenTables := []routingTable{
		{},
		{getTestAddr(5): getTestAddr(4), getTestAddr(4): getTestAddr(4)},
	}
	resultingTable := routingTable{
		getTestAddr(2): getTestAddr(2),
		getTestAddr(3): getTestAddr(3),
		getTestAddr(4): getTestAddr(3),
		getTestAddr(5): getTestAddr(3),
	}

	pulsar.SimulateEchoHandler(pulseID, explorationRequest, []pulsing.SourcedMessage[messages.Message]{
		{Msg: ExplorationResponse{RoutingTable: childrenTables[0]}, From: getTestAddr(2)},
		{Msg: ExplorationResponse{RoutingTable: childrenTables[1]}, From: getTestAddr(3)},
	}, ExplorationResponse{RoutingTable: resultingTable})

	time.Sleep(100 * time.Millisecond)

	router.expectRoutingTable(t, resultingTable)
}

// Tests that sending a routed message to a known address sends the message to the dispatcher
func TestRoutedSendToKnownAddress(t *testing.T) {
	routingTable := routingTable{
		getTestAddr(1): getTestAddr(1),
		getTestAddr(2): getTestAddr(2),
		getTestAddr(3): getTestAddr(2),
	}
	disp, pulsar, router := createRouterWithRoutingTable(t, 0, []uint16{1, 2}, routingTable)

	destAddr := getTestAddr(3)
	msg := mockMessage{Payload: "Hello, world!"}
	go func() {
		err := router.Send(msg, destAddr)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}()

	pulsar.ExpectNothingFor(100 * time.Millisecond)

	routedMsg := RoutedMessage{Msg: msg, From: getTestAddr(0), To: destAddr}
	disp.ExpectSentMessage(routedMsg, getTestAddr(2))
}

// Tests that receiving a routed message destined to an unknown address triggers an exploration
func TestReceiveRoutedMessageToUnknown(t *testing.T) {
	disp, pulsar, _ := createRouter(t, 0, []uint16{1, 2})

	destAddr := getTestAddr(3)
	msg := mockMessage{Payload: "Hello, world!"}

	go func() {
		disp.SimulateReception(RoutedMessage{Msg: msg, From: getTestAddr(1), To: destAddr}, getTestAddr(1))
	}()

	routingTable := routingTable{
		getTestAddr(1): getTestAddr(1),
		getTestAddr(2): getTestAddr(2),
		getTestAddr(3): getTestAddr(2),
	}

	pulsar.ExpectSend(ExplorationRequest{}, ExplorationResponse{RoutingTable: routingTable})

	routedMsg := RoutedMessage{Msg: msg, From: getTestAddr(1), To: destAddr}
	disp.ExpectSentMessage(routedMsg, getTestAddr(2))
}

// Tests that receiving a routed message destined to a known address sends the message to the dispatcher
func TestReceiveRoutedMessageToKnown(t *testing.T) {
	routingTable := routingTable{
		getTestAddr(1): getTestAddr(1),
		getTestAddr(2): getTestAddr(2),
		getTestAddr(3): getTestAddr(2),
		getTestAddr(4): getTestAddr(2),
	}
	disp, pulsar, _ := createRouterWithRoutingTable(t, 0, []uint16{1, 2}, routingTable)

	msg := mockMessage{Payload: "Hello, world!"}
	go disp.SimulateReception(RoutedMessage{Msg: msg, From: getTestAddr(1), To: getTestAddr(3)}, getTestAddr(1))

	routedMsg := RoutedMessage{Msg: msg, From: getTestAddr(1), To: getTestAddr(3)}
	disp.ExpectSentMessage(routedMsg, getTestAddr(2))
	pulsar.ExpectNothingFor(100 * time.Millisecond)
}

// Tests that receiving a routed message for self notifies of reception
func TestReceiveRoutedMessageForSelf(t *testing.T) {
	disp, pulsar, router := createRouter(t, 0, []uint16{1, 2})

	msg := mockMessage{Payload: "Hello, world!"}
	go disp.SimulateReception(RoutedMessage{Msg: msg, From: getTestAddr(3), To: getTestAddr(0)}, getTestAddr(1))

	router.expectMessage(t, msg, getTestAddr(3))

	disp.ExpectNothingFor(100 * time.Millisecond)
	pulsar.ExpectNothingFor(100 * time.Millisecond)
}
