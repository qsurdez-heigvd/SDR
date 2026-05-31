package tcp

import (
	"chatsapp/internal/logging"
	"fmt"
	"sync"
	"testing"
	"time"
)

type handler struct {
	t               *testing.T
	receivedMessage chan *message
}

func (h handler) HandleNetworkMessage(m *message) (wasHandled bool) {
	h.receivedMessage <- m
	return true
}

func newHandlerNonBlocking(t *testing.T) handler {
	received := make(chan *message, 1000)
	return handler{t: t, receivedMessage: received}
}

func newHandler(t *testing.T) handler {
	received := make(chan *message)
	return handler{t: t, receivedMessage: received}
}

func (h handler) interceptMessage() *message {
	return <-h.receivedMessage
}

func (h handler) expectMessage(expected *message) {
	received := <-h.receivedMessage
	if received.Source != expected.Source {
		h.t.Errorf("Expected source %v, got %v", expected.Source, received.Source)
	}
	if string(received.Payload) != string(expected.Payload) {
		h.t.Errorf("Expected payload %v, got %v", expected.Payload, received.Payload)
	}
}

func (h handler) expectNoMessageFor(d time.Duration) {
	select {
	case <-h.receivedMessage:
		h.t.Errorf("Was not expecting a message")
	case <-time.After(d):
	}
}

func TestUnidirectionalSend(t *testing.T) {
	addrs := []address{
		{IP: "127.0.0.1", Port: 5000},
		{IP: "127.0.0.1", Port: 5001},
	}

	log0 := logging.NewStdLogger("0")
	log1 := logging.NewStdLogger("1")

	tcp0 := NewTCP(addrs[0], log0)
	tcp1 := NewTCP(addrs[1], log1)

	h0 := newHandler(t)
	h1 := newHandler(t)

	tcp0.RegisterHandler(h0)
	tcp1.RegisterHandler(h1)

	numMessages := 100

	go func() {
		for i := 0; i < numMessages; i++ {
			payload := []byte{42, byte(i)}
			tcp0.Send(addrs[1], payload)
		}
	}()

	for i := 0; i < numMessages; i++ {
		payload := []byte{42, byte(i)}
		h1.expectMessage(&message{Source: addrs[0], Payload: payload})
	}

	tcp0.Close()
	tcp1.Close()

	time.Sleep(100 * time.Millisecond)
}

func TestSend(t *testing.T) {
	addrs := []address{
		{IP: "127.0.0.1", Port: 5000},
		{IP: "127.0.0.1", Port: 5001},
		{IP: "127.0.0.1", Port: 5002},
	}

	log0 := logging.NewStdLogger("0")
	log1 := logging.NewStdLogger("1")
	log2 := logging.NewStdLogger("2")

	tcp0 := NewTCP(addrs[0], log0)
	tcp1 := NewTCP(addrs[1], log1)
	tcp2 := NewTCP(addrs[2], log2)

	h0 := newHandler(t)
	h1 := newHandler(t)
	h2 := newHandler(t)

	h0ID := tcp0.RegisterHandler(h0)
	h1ID := tcp1.RegisterHandler(h1)
	h2ID := tcp2.RegisterHandler(h2)

	payload1 := []byte{42}
	payload2 := []byte{43}
	payload3 := []byte{44}
	go tcp0.Send(addrs[1], payload1)
	go tcp1.Send(addrs[2], payload2)
	go tcp2.Send(addrs[0], payload3)

	h1.expectMessage(&message{Source: addrs[0], Payload: payload1})
	h2.expectMessage(&message{Source: addrs[1], Payload: payload2})
	h0.expectMessage(&message{Source: addrs[2], Payload: payload3})

	payload4 := []byte{45}
	tcp2.Send(addrs[1], payload4)
	h1.expectMessage(&message{Source: addrs[2], Payload: payload4})

	tcp0.UnregisterHandler(h0ID)
	tcp1.UnregisterHandler(h1ID)
	tcp2.UnregisterHandler(h2ID)

	tcp0.Close()
	tcp1.Close()
	tcp2.Close()

	time.Sleep(100 * time.Millisecond)
}

func TestClose(t *testing.T) {
	// s1 sends to s2, implicitly creating a connection,
	// then s1 closes the connection,
	// then s2 tries to send to s1, but the connection is closed; should create a new connection.
	// s2 closes the connection
	// s2 tries to send to s1, but the connection is closed; should create a new connection.

	addrs := []address{
		{IP: "127.0.0.1", Port: 5000},
		{IP: "127.0.0.1", Port: 5001},
	}

	log0 := logging.NewStdLogger("0")
	log1 := logging.NewStdLogger("1")

	tcp0 := NewTCP(addrs[0], log0)
	tcp1 := NewTCP(addrs[1], log1)

	h0 := newHandler(t)
	h1 := newHandler(t)

	tcp0.RegisterHandler(h0)
	tcp1.RegisterHandler(h1)

	payloads := [][]byte{
		{42},
		{43},
	}

	tcp0.Send(addrs[1], payloads[0])
	h1.expectMessage(&message{Source: addrs[0], Payload: payloads[0]})

	tcp0.Close()
	time.Sleep(100 * time.Millisecond)
	log0 = logging.NewStdLogger("0b")
	tcp0 = NewTCP(addrs[0], log0)
	tcp0.RegisterHandler(h0)
	time.Sleep(100 * time.Millisecond)

	tcp1.Send(addrs[0], payloads[1])
	h0.expectMessage(&message{Source: addrs[1], Payload: payloads[1]})

	tcp1.Close()
	time.Sleep(100 * time.Millisecond)
	log1 = logging.NewStdLogger("1b")
	tcp1 = NewTCP(addrs[1], log1)
	tcp1.RegisterHandler(h1)
	time.Sleep(100 * time.Millisecond)

	tcp1.Send(addrs[0], payloads[0])
	h0.expectMessage(&message{Source: addrs[1], Payload: payloads[0]})

	tcp0.Close()
	tcp1.Close()

	time.Sleep(100 * time.Millisecond)
}

func TestCloseWhileSending(t *testing.T) {
	// sender sends a message to closed receiver. When it gets back, it should receive the message.

	addrs := []address{
		{IP: "127.0.0.1", Port: 5000},
		{IP: "127.0.0.1", Port: 5001},
	}

	log0 := logging.NewStdLogger("0")
	log1 := logging.NewStdLogger("1")

	tcp0 := NewTCP(addrs[0], log0)
	tcp1 := NewTCP(addrs[1], log1)

	h0 := newHandler(t)
	h1 := newHandler(t)

	tcp0.RegisterHandler(h0)
	tcp1.RegisterHandler(h1)

	payload := []byte{42}
	go tcp0.Send(addrs[1], payload)
	h1.expectMessage(&message{Source: addrs[0], Payload: payload})

	tcp1.Close()
	time.Sleep(500 * time.Millisecond)

	go tcp0.Send(addrs[1], payload)
	h1.expectNoMessageFor(1000 * time.Millisecond)

	log1 = logging.NewStdLogger("1b")
	tcp1 = NewTCP(addrs[1], log1)
	tcp1.RegisterHandler(h1)
	time.Sleep(500 * time.Millisecond)

	h1.expectMessage(&message{Source: addrs[0], Payload: payload})

	tcp0.Close()
	tcp1.Close()

	time.Sleep(100 * time.Millisecond)
}

func TestSpam(t *testing.T) {
	numAddrs := 10
	numMessages := 100

	addrs := make([]address, numAddrs)
	logs := make([]*logging.Logger, numAddrs)
	tcps := make([]netInterface, numAddrs)
	portBase := 5000
	for i := 0; i < numAddrs; i++ {
		addrs[i] = address{IP: "127.0.0.1", Port: uint16(portBase) + uint16(i)}
		logs[i] = logging.NewStdLogger(fmt.Sprintf("%v", i))
		tcps[i] = NewTCP(addrs[i], logs[i])
		time.Sleep(10 * time.Millisecond)
	}

	handlers := make([]handler, numAddrs)
	for i := 0; i < numAddrs; i++ {
		handlers[i] = newHandlerNonBlocking(t)
		tcps[i].RegisterHandler(handlers[i])
	}

	var wg sync.WaitGroup
	fatalChan := make(chan struct{})

	for i, addr := range addrs {
		wg.Add(1)
		go func(i int, addr address) {
			defer wg.Done()
			for msg := 0; msg < numMessages; msg++ {
				for j, addrj := range addrs {
					if i == j {
						continue
					}
					payload := []byte{byte(i), byte(j), byte(msg)}
					tcps[i].Send(addrj, payload)
				}
			}
		}(i, addr)

		wg.Add(1)
		go func(i int, addr address) {
			defer wg.Done()
			lastReceived := make(map[address][]byte)
			for m := 0; m < numMessages*(numAddrs-1); m++ {
				received := handlers[i].interceptMessage()
				if len(received.Payload) != 3 {
					t.Errorf("Expected payload of length 3, got %v", received.Payload)
					fatalChan <- struct{}{}
					return
				}
				j := received.Source.Port - uint16(portBase)
				expected := []byte{byte(j), byte(i), 0}
				if _, ok := lastReceived[received.Source]; ok {
					lastReceived := lastReceived[received.Source]
					expected = []byte{lastReceived[0], lastReceived[1], lastReceived[2] + 1}
				}
				if string(received.Payload) != string(expected) {
					t.Errorf("Expected %v, got %v", expected, received.Payload)
					fatalChan <- struct{}{}
					return
				}
				lastReceived[received.Source] = received.Payload
			}
		}(i, addr)
	}

	doneChan := make(chan struct{})
	go func() {
		wg.Wait()
		doneChan <- struct{}{}
	}()

	select {
	case <-fatalChan:
		t.Fail()
	case <-doneChan:
	}

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < numAddrs; i++ {
		tcps[i].Close()
	}

	time.Sleep(100 * time.Millisecond)
}
