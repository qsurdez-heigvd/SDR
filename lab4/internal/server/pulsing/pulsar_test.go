package pulsing

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils"
	"chatsapp/internal/utils/option"
	"fmt"
	"sync"
	"testing"
	"time"
)

func getTestAddr(number uint16) transport.Address {
	return transport.Address{IP: "127.0.0.1", Port: 5000 + number}
}

type pulseHandlingRequest struct {
	id       PulseID
	received int
	from     transport.Address
	prop     chan int
}

type echoHandlingRequest struct {
	id     PulseID
	pulse  int
	echoes []SourcedMessage[int]
	prop   chan int
}

type testPulsarWrapper struct {
	Pulsar[int]

	intoNet chan SentMessage[int]
	fromNet chan ReceivedMessage[int]

	pulseHandlingRequests chan pulseHandlingRequest
	echoHandlingRequests  chan echoHandlingRequest

	neigAddrs []transport.Address
}

func fromIdsToAddrs(ids []uint16) []transport.Address {
	addrs := make([]transport.Address, len(ids))
	for i, id := range ids {
		addrs[i] = getTestAddr(id)
	}
	return addrs
}

func newTestPulsarWrapperFromAddrs(neighbors []transport.Address) *testPulsarWrapper {
	w := testPulsarWrapper{
		intoNet:               make(chan SentMessage[int]),
		fromNet:               make(chan ReceivedMessage[int]),
		pulseHandlingRequests: make(chan pulseHandlingRequest),
		echoHandlingRequests:  make(chan echoHandlingRequest),
		neigAddrs:             neighbors,
	}

	w.Pulsar = NewPulsar[int](logging.NewStdLogger("test"),
		getTestAddr(0),
		func(id PulseID, pulse int, from transport.Address) (int, error) {
			prop := make(chan int)
			w.pulseHandlingRequests <- pulseHandlingRequest{id: id, received: pulse, from: from, prop: prop}
			return <-prop, nil
		},
		func(id PulseID, pulse int, echoes []SourcedMessage[int]) (int, error) {
			prop := make(chan int)
			w.echoHandlingRequests <- echoHandlingRequest{id: id, pulse: pulse, echoes: echoes, prop: prop}
			return <-prop, nil
		},
		w.intoNet,
		w.fromNet,
		neighbors,
	)

	return &w
}

func newTestPulsarWrapper(neighbors []uint16) *testPulsarWrapper {
	neighAddrs := fromIdsToAddrs(neighbors)
	return newTestPulsarWrapperFromAddrs(neighAddrs)
}

func (w *testPulsarWrapper) getNeighbors() []transport.Address {
	return w.neigAddrs
}

func (w *testPulsarWrapper) send(t *testing.T, pulse int) <-chan int {
	echoChan := make(chan int)
	go func() {
		echo, err := w.StartPulse(pulse)
		if err != nil {
			t.Errorf("Error sending pulse: %v", err)
		}
		echoChan <- echo
	}()
	return echoChan
}

func (w *testPulsarWrapper) interceptNext() SentMessage[int] {
	return <-w.intoNet
}

func (w *testPulsarWrapper) expectSentMessage(t *testing.T, id *option.Option[PulseID], typ Type, payload int, to transport.Address) {
	sent := w.interceptNext()
	if id.IsNone() {
		*id = option.Some(sent.Message.ID)
	} else if sent.Message.ID != id.Get() {
		t.Fatalf("Expected pulse id %v, got %v", id, sent.Message.ID)
	}
	if sent.Message.Payload != payload {
		t.Fatalf("Expected sent message to be %v, got %v", payload, sent.Message.Payload)
	}
	if sent.To != to {
		t.Fatalf("Expected sent message to be to %v, got %v", to, sent.To)
	}
	if sent.Message.Type != typ {
		t.Fatalf("Expected sent message type to be %v, got %v", typ, sent.Message.Type)
	}
}

func (w *testPulsarWrapper) expectSentMessages(t *testing.T, id *option.Option[PulseID], typ Type, payload int, tos []transport.Address) {
	sents := make([]SentMessage[int], len(tos))

	for i := range tos {
		sents[i] = w.interceptNext()
	}

	destinations := make([]transport.Address, 0, len(tos))
	actuaID := option.None[PulseID]()
	for _, sent := range sents {
		if actuaID == option.None[PulseID]() {
			actuaID = option.Some(sent.Message.ID)
		} else if sent.Message.ID != actuaID.Get() {
			t.Fatalf("Expected pulse id %v, got %v", actuaID, sent.Message.ID)
		}
		expected := SentMessage[int]{Message: PulsarMessage[int]{Type: typ, Payload: payload, ID: actuaID.Get()}, To: sent.To}
		if sent.Message.Payload != payload {
			t.Fatalf("Expected sent message to be %v, got %v : %v, got %v", payload, sent.Message.Payload, expected, sent)
		}
		if sent.Message.Type != typ {
			t.Fatalf("Expected sent message type to be %v, got %v", typ, sent.Message.Type)
		}
		destinations = append(destinations, sent.To)
	}

	if id.IsNone() {
		*id = actuaID
	} else if actuaID.Get() != id.Get() {
		t.Fatalf("Expected pulse id %v, got %v", id, actuaID)
	}

	if !utils.SliceContainsSame(destinations, tos) {
		t.Fatalf("Expected sent messages to be to %v, got %v", tos, destinations)
	}
}

func (w *testPulsarWrapper) simulateNextReceived(msg PulsarMessage[int], from transport.Address) {
	w.fromNet <- ReceivedMessage[int]{Message: msg, From: from}
}

func (w *testPulsarWrapper) expectPulseHandling(t *testing.T, id *option.Option[PulseID], pulse int, from transport.Address, prop int) {
	req := <-w.pulseHandlingRequests
	if id.IsNone() {
		*id = option.Some(req.id)
	} else if req.id != id.Get() {
		t.Fatalf("Expected pulse id %v, got %v", id, req.id)
	}
	if req.received != pulse {
		t.Fatalf("Expected pulse %v, got %v", pulse, req.received)
	}
	if req.from != from {
		t.Fatalf("Expected pulse from %v, got %v", from, req.from)
	}
	req.prop <- prop
}

func (w *testPulsarWrapper) expectEchoHandling(t *testing.T, id *option.Option[PulseID], pulse int, echoes []SourcedMessage[int], prop int) {
	req := <-w.echoHandlingRequests
	if id.IsNone() {
		*id = option.Some(req.id)
	} else if req.id != id.Get() {
		t.Fatalf("Expected pulse id %v, got %v", id, req.id)
	}
	if req.pulse != pulse {
		t.Fatalf("Expected pulse %v, got %v", pulse, req.pulse)
	}
	if !utils.SliceContainsSame(req.echoes, echoes) {
		t.Fatalf("Expected echoes %v, got %v", echoes, req.echoes)
	}
	req.prop <- prop
}

func (w *testPulsarWrapper) expectNoPulseHandling(t *testing.T, timeout int) {
	select {
	case <-w.pulseHandlingRequests:
		t.Fatal("Expected no pulse handling request")
	case <-time.After(time.Duration(timeout) * time.Millisecond):
	}
}

func (w *testPulsarWrapper) expectNoEchoHandling(t *testing.T, timeout int) {
	select {
	case <-w.echoHandlingRequests:
		t.Fatal("Expected no echo handling request")
	case <-time.After(time.Duration(timeout) * time.Millisecond):
	}
}

func (w *testPulsarWrapper) expectNoSentMessage(t *testing.T, timeout int) {
	select {
	case <-w.intoNet:
		t.Fatal("Expected no sent message")
	case <-time.After(time.Duration(timeout) * time.Millisecond):
	}
}

func (w *testPulsarWrapper) expectNoHandling(t *testing.T, timeout int) {
	select {
	case <-w.echoHandlingRequests:
		t.Fatalf("Expected no echo handling request")
	case <-w.pulseHandlingRequests:
		t.Fatalf("Expected no pulse handling request")
	case <-time.After(time.Duration(timeout) * time.Millisecond):
	}
}

func TestSendPulseNoNeighbors(t *testing.T) {
	p := newTestPulsarWrapper([]uint16{})

	finalEcho := p.send(t, 42)

	p.expectNoSentMessage(t, 100)
	p.expectNoPulseHandling(t, 100)

	// p.expectEchoHandling(t, SourcedMessage[int]{Msg: 42, From: getTestAddr(0)}, []SourcedMessage[int]{}, 44)
	id := option.None[PulseID]()
	p.expectEchoHandling(t, &id, 42, []SourcedMessage[int]{}, 44)

	if <-finalEcho != 44 {
		t.Fatalf("Expected final echo to be 44, got %v", finalEcho)
	}
}

func TestSendPulseOneNeighbor(t *testing.T) {
	p := newTestPulsarWrapper([]uint16{1})

	finalEcho := p.send(t, 42)

	p.expectNoPulseHandling(t, 100)

	id := option.None[PulseID]()
	p.expectSentMessage(t, &id, Pulse, 42, getTestAddr(1))

	p.simulateNextReceived(PulsarMessage[int]{Type: Echo, Payload: 43, ID: id.Get()}, getTestAddr(1))

	p.expectEchoHandling(
		t,
		&id,
		42,
		[]SourcedMessage[int]{{Msg: 43, From: getTestAddr(1)}}, 44)

	if <-finalEcho != 44 {
		t.Fatalf("Expected final echo to be 44, got %v", finalEcho)
	}
}

func TestSendPulseManyNeighbors(t *testing.T) {
	numNeighbors := 5

	neighbors := make([]uint16, numNeighbors)
	for i := range neighbors {
		neighbors[i] = uint16(i + 1)
	}

	p := newTestPulsarWrapper(neighbors)

	finalEcho := p.send(t, 42)

	id := option.None[PulseID]()
	p.expectSentMessages(t, &id, Pulse, 42, p.getNeighbors())

	expectedEchoes := make([]SourcedMessage[int], numNeighbors)
	for i := 0; i < numNeighbors; i++ {
		payload := 43 + i
		p.simulateNextReceived(PulsarMessage[int]{Type: Echo, Payload: payload, ID: id.Get()}, getTestAddr(neighbors[i]))
		expectedEchoes[i] = SourcedMessage[int]{Msg: payload, From: getTestAddr(neighbors[i])}
		if i < numNeighbors-1 {
			p.expectNoEchoHandling(t, 100)
		} else {
			p.expectEchoHandling(
				t,
				&id,
				42,
				expectedEchoes,
				1)
		}
	}

	if <-finalEcho != 1 {
		t.Fatalf("Expected final echo to be 1, got %v", finalEcho)
	}
}

// TODO add tests for correctly handling the reception of pulses

func TestReceivePulseOneNeighbor(t *testing.T) {
	p := newTestPulsarWrapper([]uint16{1})

	id := option.Some(NewPulseID(getTestAddr(1), 1))

	p.simulateNextReceived(PulsarMessage[int]{Type: Pulse, Payload: 1, ID: id.Get()}, getTestAddr(1))

	p.expectPulseHandling(t, &id, 1, getTestAddr(1), 2)

	p.expectEchoHandling(t, &id, 1, []SourcedMessage[int]{}, 2)

	p.expectSentMessage(t, &id, Echo, 2, getTestAddr(1))

}

func TestReceivePulseManyNeighbors(t *testing.T) {
	numNeighbors := 5

	neighbors := make([]uint16, numNeighbors)
	for i := range neighbors {
		neighbors[i] = uint16(i + 1)
	}

	p := newTestPulsarWrapper(neighbors)

	id := option.Some(NewPulseID(getTestAddr(1), 1))

	p.simulateNextReceived(PulsarMessage[int]{Type: Pulse, Payload: 1, ID: id.Get()}, getTestAddr(1))

	p.expectPulseHandling(t, &id, 1, getTestAddr(1), 2)

	expectedSendAddrs := make([]transport.Address, 0, numNeighbors-1)
	for _, n := range p.neigAddrs {
		if n != getTestAddr(1) {
			expectedSendAddrs = append(expectedSendAddrs, n)
		}
	}

	p.expectSentMessages(t, &id, Pulse, 2, expectedSendAddrs)

	expectedEchoes := make([]SourcedMessage[int], 0, numNeighbors-1)
	for i, n := range p.neigAddrs {
		if n != getTestAddr(1) {
			payload := 3 + i
			expectedEchoes = append(expectedEchoes, SourcedMessage[int]{Msg: payload, From: n})
			p.simulateNextReceived(PulsarMessage[int]{Type: Echo, Payload: payload, ID: id.Get()}, n)
		}
	}

	p.expectEchoHandling(t, &id, 1, expectedEchoes, 1)

	p.expectSentMessage(t, &id, Echo, 1, getTestAddr(1))

}

func TestSendTwoDistinctPulses(t *testing.T) {
	p := newTestPulsarWrapper([]uint16{1, 2})

	finalEcho1 := p.send(t, 42)
	time.Sleep(100 * time.Millisecond)
	finalEcho2 := p.send(t, 43)

	id1 := option.None[PulseID]()
	p.expectSentMessages(t, &id1, Pulse, 42, p.getNeighbors())

	id2 := option.None[PulseID]()
	p.expectSentMessages(t, &id2, Pulse, 43, p.getNeighbors())

	p.expectNoHandling(t, 100)
	p.expectNoSentMessage(t, 100)

	p.simulateNextReceived(PulsarMessage[int]{Type: Echo, Payload: 44, ID: id1.Get()}, getTestAddr(1))
	p.expectNoHandling(t, 100)

	p.simulateNextReceived(PulsarMessage[int]{Type: Echo, Payload: 45, ID: id2.Get()}, getTestAddr(2))
	p.expectNoHandling(t, 100)

	p.simulateNextReceived(PulsarMessage[int]{Type: Echo, Payload: 46, ID: id1.Get()}, getTestAddr(2))
	p.expectEchoHandling(t, &id1, 42, []SourcedMessage[int]{{Msg: 44, From: getTestAddr(1)}, {Msg: 46, From: getTestAddr(2)}}, 47)
	if <-finalEcho1 != 47 {
		t.Fatalf("Expected final echo to be 47, got %v", finalEcho1)
	}

	p.simulateNextReceived(PulsarMessage[int]{Type: Echo, Payload: 48, ID: id2.Get()}, getTestAddr(1))
	p.expectEchoHandling(t, &id2, 43, []SourcedMessage[int]{{Msg: 45, From: getTestAddr(2)}, {Msg: 48, From: getTestAddr(1)}}, 49)
	if <-finalEcho2 != 49 {
		t.Fatalf("Expected final echo to be 49, got %v", finalEcho2)
	}
}

func TestReceiveTwoDistinctPulses(t *testing.T) {
	p := newTestPulsarWrapper([]uint16{1, 2})

	id1 := option.Some(NewPulseID(getTestAddr(1), 142))
	p.simulateNextReceived(PulsarMessage[int]{Type: Pulse, Payload: 42, ID: id1.Get()}, getTestAddr(1))

	p.expectNoSentMessage(t, 100)
	p.expectPulseHandling(t, &id1, 42, getTestAddr(1), 43)
	p.expectNoHandling(t, 100)
	p.expectSentMessage(t, &id1, Pulse, 43, getTestAddr(2))

	id2 := option.Some(NewPulseID(getTestAddr(2), 143))
	p.simulateNextReceived(PulsarMessage[int]{Type: Pulse, Payload: 44, ID: id2.Get()}, getTestAddr(2))

	p.expectNoSentMessage(t, 100)
	p.expectPulseHandling(t, &id2, 44, getTestAddr(2), 45)
	p.expectNoHandling(t, 100)
	p.expectSentMessage(t, &id2, Pulse, 45, getTestAddr(1))

	p.simulateNextReceived(PulsarMessage[int]{Type: Echo, Payload: 46, ID: id1.Get()}, getTestAddr(2))
	p.expectEchoHandling(t, &id1, 42, []SourcedMessage[int]{{Msg: 46, From: getTestAddr(2)}}, 47)
	p.expectSentMessage(t, &id1, Echo, 47, getTestAddr(1))

	p.simulateNextReceived(PulsarMessage[int]{Type: Echo, Payload: 47, ID: id2.Get()}, getTestAddr(1))
	p.expectEchoHandling(t, &id2, 44, []SourcedMessage[int]{{Msg: 47, From: getTestAddr(1)}}, 48)
	p.expectSentMessage(t, &id2, Echo, 48, getTestAddr(2))

	p.expectNoHandling(t, 100)
	p.expectNoSentMessage(t, 100)
}

type networkOfPulsars struct {
	pulsars     map[transport.Address]Pulsar[int]
	intoNets    map[transport.Address]chan SentMessage[int]
	fromNets    map[transport.Address]chan ReceivedMessage[int]
	mainIntoNet *utils.BufferedChan[SourcedMessage[SentMessage[int]]]
}

func newNetOfPulsars(graph utils.Graph[transport.Address]) *networkOfPulsars {
	pulsars := make(map[transport.Address]Pulsar[int])
	intoNets := make(map[transport.Address]chan SentMessage[int])
	fromNets := make(map[transport.Address]chan ReceivedMessage[int])

	mainChan := utils.NewBufferedChan[SourcedMessage[SentMessage[int]]]()

	for i := 0; i < graph.GetSize(); i++ {
		addr := getTestAddr(uint16(i))
		neighbors := graph.GetNeighbors(addr)
		intoNets[addr] = make(chan SentMessage[int])
		fromNets[addr] = make(chan ReceivedMessage[int])
		pulsars[addr] = NewPulsar(
			logging.NewStdLogger(fmt.Sprintf("pulsar-%v", addr)),
			addr,
			func(id PulseID, pulse int, from transport.Address) (int, error) {
				return pulse, nil
			},
			func(id PulseID, pulse int, echoes []SourcedMessage[int]) (int, error) {
				sum := 0
				for _, echo := range echoes {
					sum += echo.Msg
				}
				return sum + 1, nil
			},
			intoNets[addr],
			fromNets[addr],
			neighbors,
		)

		// Funnel sent messages to main chan
		go func(addr transport.Address, intoNet chan SentMessage[int], neighs []transport.Address) {
			for msg := range intoNet {
				if !utils.SliceContains(neighs, msg.To) {
					panic(fmt.Sprintf("Pulsar %v tried to send message to non-neighbor %v", addr, msg.To))
				}
				mainChan.Inlet() <- SourcedMessage[SentMessage[int]]{Msg: msg, From: addr}
			}
		}(addr, intoNets[addr], neighbors)
	}

	// Dispatch output of main chan
	go func() {
		for sourced := range mainChan.Outlet() {
			msg := sourced.Msg
			from := sourced.From
			fromNets[msg.To] <- ReceivedMessage[int]{Message: msg.Message, From: from}
		}
	}()

	return &networkOfPulsars{
		pulsars:     pulsars,
		intoNets:    intoNets,
		fromNets:    fromNets,
		mainIntoNet: mainChan,
	}
}

type perPulsarScenario struct {
	sender transport.Address
	count  int
}

type scenario []perPulsarScenario

func newScenario() scenario {
	return make(scenario, 0)
}

func (s scenario) WithPulse(from transport.Address, count int) scenario {
	return append(s, perPulsarScenario{from, count})
}

func (n *networkOfPulsars) execute(t *testing.T, sc scenario) {
	var wg sync.WaitGroup
	expectedResult := len(n.pulsars)
	for _, s := range sc {
		wg.Add(1)
		go func(s perPulsarScenario) {
			for i := 0; i < s.count; i++ {
				result, err := n.pulsars[s.sender].StartPulse(i)
				if err != nil {
					t.Errorf("Error sending pulse: %v", err)
					return
				}
				if result != expectedResult {
					t.Errorf("Expected result for pulse %v from %v to be %v, got %v", i, s.sender, expectedResult, result)
				}
			}
			wg.Done()
		}(s)
	}

	wg.Wait()
}

func testManyPulsars(t *testing.T, graph utils.Graph[transport.Address], numPulses int) {
	net := newNetOfPulsars(graph)
	size := graph.GetSize()

	sc := newScenario()

	for i := 0; i < size; i++ {
		sc = sc.WithPulse(getTestAddr(uint16(i)), numPulses)
	}

	net.execute(t, sc)
}

func TestManyPulsarsOnLineGraph(t *testing.T) {
	size := 10
	graph := utils.GenLineGraph(size, func(id int) transport.Address {
		return getTestAddr(uint16(id))
	})
	testManyPulsars(t, graph, 5)
}

func TestManyPulsarsOnCycleGraph(t *testing.T) {
	size := 10
	graph := utils.GenCycleGraph(size, func(id int) transport.Address {
		return getTestAddr(uint16(id))
	})
	testManyPulsars(t, graph, 5)
}

func TestManyPulsarsOnTreeGraph(t *testing.T) {
	size := 10
	graph := utils.GenRandomTreeGraph(size, func(id int) transport.Address {
		return getTestAddr(uint16(id))
	})
	testManyPulsars(t, graph, 5)
}

func TestManyPulsarsOnCliqueGraph(t *testing.T) {
	size := 10
	graph := utils.GenCliqueGraph(size, func(id int) transport.Address {
		return getTestAddr(uint16(id))
	})
	testManyPulsars(t, graph, 5)
}

func TestManyPulsarsOnRandomGraph(t *testing.T) {
	size := 10
	graph := utils.GenRandomGraph(size, func(id int) transport.Address {
		return getTestAddr(uint16(id))
	})
	testManyPulsars(t, graph, 5)
}
