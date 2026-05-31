package dispatcher

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/server/pulsing"
	"chatsapp/internal/server/routing"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils/ioutils"
	"encoding/gob"
	"reflect"
	"testing"
	"time"
)

type MutexMessage struct {
	Pid uint32
}

type ChatMessage struct {
	Content string
}

func (MutexMessage) RegisterToGob() {
	gob.Register(MutexMessage{})
}

func (ChatMessage) RegisterToGob() {
	gob.Register(ChatMessage{})
}

func TestRegister(t *testing.T) {
	addr1 := transport.Address{IP: "127.0.0.1", Port: 5000}
	addr2 := transport.Address{IP: "127.0.0.1", Port: 5001}

	expectedMsg := MutexMessage{42}

	mockNet := transport.NewMockNetworkInterface(addr1)

	logger := logging.NewLogger(ioutils.NewStdStream(), nil, "disp_test", false)
	d := NewDispatcher(logger, addr1, []transport.Address{addr2}, mockNet)
	d.Register(MutexMessage{}, func(msg Message, source transport.Address) {
		if _, ok := msg.(MutexMessage); !ok {
			t.Fatalf("expected MutexMessage, got %T", msg)
		}

		if source != addr2 {
			t.Fatalf("expected source %v, got %v", addr2, source)
		}

		if !reflect.DeepEqual(msg, expectedMsg) {
			t.Fatalf("expected message %v, got %v", expectedMsg, msg)
		}
	})

	go func() {
		receivedMsg := mockNet.InterceptSentMessage()
		responseMsg := transport.Message{Source: addr2, Payload: receivedMsg.Message.Payload}
		mockNet.SimulateReception(&responseMsg)
	}()

	d.Send(expectedMsg, addr2)

	time.Sleep(300 * time.Millisecond)
}

func TestCallBroadcast(t *testing.T) {
	addr1 := transport.Address{IP: "127.0.0.1", Port: 5000}
	addr2 := transport.Address{IP: "127.0.0.1", Port: 5001}

	expectedMsg := ChatMessage{"Hello, World!"}

	mockNet := transport.NewMockNetworkInterface(addr1)
	logger := logging.NewLogger(ioutils.NewStdStream(), nil, "disp_test", false)
	d := newDispatcher(logger, addr1, []transport.Address{addr2}, mockNet)
	d.Register(ChatMessage{}, func(msg Message, source transport.Address) {
		t.Errorf("No message should be received")
	})

	complete := make(chan struct{})
	go func() {
		d.Broadcast(expectedMsg)
		close(complete)
	}()

	msg := <-mockNet.SentMessages
	actualPayload := d.decodeMessage(msg.Message.Payload)

	if pulse, ok := actualPayload.(pulsing.PulsarMessage[Message]); !ok {
		t.Fatalf("expected PulsarMessage, got %T", actualPayload)
	} else if pulse.Type != pulsing.Pulse {
		t.Fatalf("expected message type %v, got %v", pulsing.Pulse, pulse.Type)
	} else if broadcast, ok := pulse.Payload.(routing.BroadcastRequest); !ok {
		t.Fatalf("expected BroadcastRequest, got %T", pulse.Payload)
	} else if !reflect.DeepEqual(broadcast.Msg, expectedMsg) {
		t.Fatalf("expected message %v, got %v", expectedMsg, broadcast.Msg)
	} else {
		if msg.Source != addr1 {
			t.Fatalf("expected source %v, got %v", addr1, msg.Source)
		}
		if msg.To != addr2 {
			t.Fatalf("expected destination %v, got %v", addr2, msg.To)
		}

		select {
		case <-complete:
			t.Error("broadcast should not have completed before the pulsar has received an echo")
		case <-time.After(300 * time.Millisecond):
		}

		response := pulsing.PulsarMessage[Message]{
			Type: pulsing.Echo,
			Payload: routing.BroadcastResponse{
				Receivers: []transport.Address{addr1},
			},
			ID: pulse.ID,
		}
		bytes := d.encodeMessage(response)

		mockNet.SimulateReception(&transport.Message{
			Source:  addr2,
			Payload: bytes,
		})
	}

	select {
	case <-complete:
	case <-time.After(300 * time.Millisecond):
		t.Error("timeout waiting for broadcast to complete")
	}
}

func TestCallSend(t *testing.T) {
	addr1 := transport.Address{IP: "127.0.0.1", Port: 5000}
	addr2 := transport.Address{IP: "127.0.0.1", Port: 5001}
	addr3 := transport.Address{IP: "127.0.0.1", Port: 5002}

	expectedMsg := ChatMessage{"Hello, World!"}

	mockNet := transport.NewMockNetworkInterface(addr1)
	logger := logging.NewLogger(ioutils.NewStdStream(), nil, "disp_test", false)
	d := newDispatcher(logger, addr1, []transport.Address{addr2}, mockNet)

	d.Register(ChatMessage{}, func(msg Message, source transport.Address) {
		t.Errorf("No message should be received")
	})

	go d.Send(expectedMsg, addr3)

	msg := <-mockNet.SentMessages
	actualPayload := d.decodeMessage(msg.Message.Payload)

	if pulse, ok := actualPayload.(pulsing.PulsarMessage[Message]); !ok {
		t.Fatalf("expected PulsarMessage, got %T", actualPayload)
	} else if pulse.Type != pulsing.Pulse {
		t.Fatalf("expected message type %v, got %v", pulsing.Pulse, pulse.Type)
	} else if _, ok := pulse.Payload.(routing.ExplorationRequest); !ok {
		t.Fatalf("expected ExplorationRequest, got %T", pulse.Payload)
	} else {
		if msg.Source != addr1 {
			t.Fatalf("expected source %v, got %v", addr1, msg.Source)
		}
		if msg.To != addr2 {
			t.Fatalf("expected destination %v, got %v", addr2, msg.To)
		}

		response := pulsing.PulsarMessage[Message]{
			Type: pulsing.Echo,
			Payload: routing.ExplorationResponse{
				RoutingTable: map[transport.Address]transport.Address{
					addr3: addr2,
				},
			},
			ID: pulse.ID,
		}
		bytes := d.encodeMessage(response)

		mockNet.SimulateReception(&transport.Message{
			Source:  addr2,
			Payload: bytes,
		})

		msg = <-mockNet.SentMessages
		actualPayload = d.decodeMessage(msg.Message.Payload)

		if routed, ok := actualPayload.(routing.RoutedMessage); !ok {
			t.Fatalf("expected RoutedMessage, got %T", actualPayload)
		} else {
			if routed.From != addr1 || routed.To != addr3 {
				t.Fatalf("expected from %v and to %v, got %v and %v", addr1, addr3, routed.From, routed.To)
			}
			if !reflect.DeepEqual(routed.Msg, expectedMsg) {
				t.Fatalf("expected message %v, got %v", expectedMsg, routed.Msg)
			}
			if msg.Source != addr1 {
				t.Fatalf("expected source %v, got %v", addr1, msg.Source)
			}
			if msg.To != addr2 {
				t.Fatalf("expected destination %v, got %v", addr2, msg.To)
			}
		}
	}
}

func TestReceiveBroadcast(t *testing.T) {
	addr1 := transport.Address{IP: "127.0.0.1", Port: 5000}
	addr2 := transport.Address{IP: "127.0.0.1", Port: 5001}
	// addr3 := transport.Address{IP: "127.0.0.1", Port: 5002}

	expectedMsg := ChatMessage{"Hello, World!"}

	mockNet := transport.NewMockNetworkInterface(addr1)
	logger := logging.NewLogger(ioutils.NewStdStream(), nil, "disp_test", false)
	d := newDispatcher(logger, addr1, []transport.Address{addr2}, mockNet)

	receivedChan := make(chan struct{})
	d.Register(ChatMessage{}, func(msg Message, source transport.Address) {
		if source != addr2 {
			t.Fatalf("expected source %v, got %v", addr2, source)
		} else if chat, ok := msg.(ChatMessage); !ok {
			t.Fatalf("expected ChatMessage, got %T", msg)
		} else if !reflect.DeepEqual(chat, expectedMsg) {
			t.Fatalf("expected message %v, got %v", expectedMsg, chat)
		}
		close(receivedChan)
	})

	receivedMsg := pulsing.PulsarMessage[Message]{
		Type: pulsing.Pulse,
		Payload: routing.BroadcastRequest{
			Msg: expectedMsg,
		},
		ID: pulsing.NewPulseID(addr2, 42),
	}
	bytes := d.encodeMessage(receivedMsg)

	mockNet.SimulateReception(&transport.Message{
		Source:  addr2,
		Payload: bytes,
	})

	select {
	case <-receivedChan:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("timeout waiting for message reception")
	}

	actual := <-mockNet.SentMessages
	actualPayload := d.decodeMessage(actual.Message.Payload)
	if pulse, ok := actualPayload.(pulsing.PulsarMessage[Message]); !ok {
		t.Fatalf("expected PulsarMessage, got %T", actualPayload)
	} else if pulse.Type != pulsing.Echo {
		t.Fatalf("expected message type %v, got %v", pulsing.Echo, pulse.Type)
	} else if response, ok := pulse.Payload.(routing.BroadcastResponse); !ok {
		t.Fatalf("expected BroadcastResponse, got %T", pulse.Payload)
	} else if len(response.Receivers) != 1 || response.Receivers[0] != addr1 {
		t.Fatalf("expected receivers %v, got %v", []transport.Address{addr1}, response.Receivers)
	} else if pulse.ID != receivedMsg.ID {
		t.Fatalf("expected ID %v, got %v", receivedMsg.ID, pulse.ID)
	}
}

func TestReceivedSend(t *testing.T) {
	addr1 := transport.Address{IP: "127.0.0.1", Port: 5000}
	addr2 := transport.Address{IP: "127.0.0.1", Port: 5001}
	addr3 := transport.Address{IP: "127.0.0.1", Port: 5002}

	expectedMsg := ChatMessage{"Hello, World!"}

	mockNet := transport.NewMockNetworkInterface(addr1)
	logger := logging.NewLogger(ioutils.NewStdStream(), nil, "disp_test", false)
	d := newDispatcher(logger, addr1, []transport.Address{addr2}, mockNet)

	receivedChan := make(chan struct{})
	d.Register(ChatMessage{}, func(msg Message, source transport.Address) {
		if source != addr3 {
			t.Fatalf("expected source %v, got %v", addr3, source)
		} else if chat, ok := msg.(ChatMessage); !ok {
			t.Fatalf("expected ChatMessage, got %T", msg)
		} else if !reflect.DeepEqual(chat, expectedMsg) {
			t.Fatalf("expected message %v, got %v", expectedMsg, chat)
		}
		close(receivedChan)
	})

	receivedMsg := routing.RoutedMessage{
		Msg:  expectedMsg,
		From: addr3,
		To:   addr1,
	}
	bytes := d.encodeMessage(receivedMsg)
	mockNet.SimulateReception(&transport.Message{
		Source:  addr2,
		Payload: bytes,
	})

	select {
	case <-receivedChan:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("timeout waiting for message reception")
	}
}
