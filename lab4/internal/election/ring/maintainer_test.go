package ring

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/messages"
	"chatsapp/internal/mocks"
	"chatsapp/internal/timestamps"
	"chatsapp/internal/transport"
	"testing"
	"time"
)

type mockMessage string

type addressedMsg struct {
	msg  messages.Message
	addr transport.Address
}

func (mockMessage) RegisterToGob() {
}

func expectMessage(t *testing.T, payload mockMessage, msgType messageType, to address, actual addressedMsg) {
	ringMsg := actual.msg.(message)

	if actual.addr != to {
		t.Errorf("Expected message to %v, got %v", to, actual.addr)
	}

	if ringMsg.Type != msgType {
		t.Errorf("Expected message type %v, got %v", msgType, ringMsg.Type)
	}

	if msgType == payloadType {
		recvdMsg := ringMsg.Payload
		if recvdMsg != payload {
			t.Errorf("Expected message %v, got %v", payload, actual)
		}
	}
}

func expectSentMessage(d mocks.MockDispatcher, payload mockMessage, msgType messageType, from address) {
	msg, from := d.InterceptNextSend()
	expectMessage(d.GetTesting(), payload, msgType, from, addressedMsg{msg, from})
}

func compareSuccessors(t *testing.T, expected []transport.Address, ring maintainer) {
	if len(ring.successors) != len(expected) {
		t.Errorf("Expected %v successors, got %v", len(expected), len(ring.successors))
	}
	for i, addr := range ring.successors {
		if addr != expected[i] {
			t.Errorf("Expected successor %v to be %v, got %v", i, expected[i], addr)
		}
	}
}

func TestSuccessorsCreation(t *testing.T) {
	addrs := []transport.Address{
		{IP: "127.0.0.1", Port: 5000},
		{IP: "127.0.0.1", Port: 5001},
		{IP: "127.0.0.1", Port: 5002},
		{IP: "127.0.0.1", Port: 5003},
	}

	logger := logging.NewStdLogger("test")
	dispatcher := mocks.NewMockDispatcher(t)

	ring := newRingMaintainer(logger, dispatcher, addrs[0], addrs, 0)

	expectedSuccessors := []transport.Address{addrs[1], addrs[2], addrs[3], addrs[0]}
	compareSuccessors(t, expectedSuccessors, *ring)

	ring = newRingMaintainer(logger, dispatcher, addrs[3], addrs, 0)

	expectedSuccessors = []transport.Address{addrs[0], addrs[1], addrs[2], addrs[3]}
	compareSuccessors(t, expectedSuccessors, *ring)

	ring = newRingMaintainer(logger, dispatcher, addrs[2], addrs, 0)

	expectedSuccessors = []transport.Address{addrs[3], addrs[0], addrs[1], addrs[2]}
	compareSuccessors(t, expectedSuccessors, *ring)
}

func TestSendToNext(t *testing.T) {
	addrs := []transport.Address{
		{IP: "127.0.0.1", Port: 5000},
		{IP: "127.0.0.1", Port: 5001},
		{IP: "127.0.0.1", Port: 5002},
	}

	logger := logging.NewStdLogger("test")
	dispatcher := mocks.NewMockDispatcher(t)
	ring := newRingMaintainer(logger, dispatcher, addrs[0], addrs, 500*time.Millisecond)

	ring.SendToNext(mockMessage("Hello, World!"))

	sentMsg, to := dispatcher.InterceptNextSend()
	expectMessage(t, mockMessage("Hello, World!"), payloadType, addrs[1], addressedMsg{sentMsg, to})

	dispatcher.SimulateReception(message{ackMsg, sentMsg.(message).Timestamp, mockMessage("")}, addrs[1])

	dispatcher.ExpectNothingFor(1 * time.Second)
}

func TestReceiveFromPrev(t *testing.T) {
	addrs := []transport.Address{
		{IP: "127.0.0.1", Port: 5000},
		{IP: "127.0.0.1", Port: 5001},
		{IP: "127.0.0.1", Port: 5002},
	}

	logger := logging.NewStdLogger("test")
	dispatcher := mocks.NewMockDispatcher(t)
	ring := newRingMaintainer(logger, dispatcher, addrs[0], addrs, 1*time.Second)

	ts := timestamps.NewLamportHandler("test", 0).GetTimestamp()
	msg := message{payloadType, ts, mockMessage("Hello, World!")}
	go dispatcher.SimulateReception(msg, addrs[2])

	deliveredMsg := ring.ReceiveFromPrev()
	if deliveredMsg != msg.Payload {
		t.Errorf("Expected delivered message to be '%v', got %v", msg.Payload, deliveredMsg)
	}

	sentMsg, to := dispatcher.InterceptNextSend()
	expectMessage(t, mockMessage(""), ackMsg, addrs[2], addressedMsg{sentMsg, to})
}

func TestTimeout(t *testing.T) {
	addrs := []transport.Address{
		{IP: "127.0.0.1", Port: 5000},
		{IP: "127.0.0.1", Port: 5001},
		{IP: "127.0.0.1", Port: 5002},
	}

	logger := logging.NewStdLogger("test")
	dispatcher := mocks.NewMockDispatcher(t)
	ring := newRingMaintainer(logger, dispatcher, addrs[0], addrs, 1*time.Second)

	ring.SendToNext(mockMessage("Hello, World!"))

	sentMsg, to := dispatcher.InterceptNextSend()
	expectMessage(t, mockMessage("Hello, World!"), payloadType, addrs[1], addressedMsg{sentMsg, to})

	sentMsg, to = dispatcher.InterceptNextSend()
	expectMessage(t, mockMessage("Hello, World!"), payloadType, addrs[2], addressedMsg{sentMsg, to})

	dispatcher.SimulateReception(message{ackMsg, sentMsg.(message).Timestamp, mockMessage("")}, addrs[2])

	dispatcher.ExpectNothingFor(2 * time.Second)
}

func TestRetriesFirstNeighborForNewMessage(t *testing.T) {
	addrs := []transport.Address{
		{IP: "127.0.0.1", Port: 5000},
		{IP: "127.0.0.1", Port: 5001},
		{IP: "127.0.0.1", Port: 5002},
		{IP: "127.0.0.1", Port: 5003},
	}

	logger := logging.NewStdLogger("test")
	dispatcher := mocks.NewMockDispatcher(t)
	ring := newRingMaintainer(logger, dispatcher, addrs[0], addrs, 1*time.Second)

	// Sending a message to the next neighbor, which times out.
	ring.SendToNext(mockMessage("Hello, World!"))

	sentMsg, to := dispatcher.InterceptNextSend()
	expectMessage(t, mockMessage("Hello, World!"), payloadType, addrs[1], addressedMsg{sentMsg, to})

	// Second next neighbor acks the message.
	sentMsg, to = dispatcher.InterceptNextSend()
	expectMessage(t, mockMessage("Hello, World!"), payloadType, addrs[2], addressedMsg{sentMsg, to})

	dispatcher.SimulateReception(message{ackMsg, sentMsg.(message).Timestamp, mockMessage("")}, addrs[2])

	dispatcher.ExpectNothingFor(2 * time.Second)

	// New send request, should go to first neighbor.
	ring.SendToNext(mockMessage("Hello, World again!"))
	sentMsg, to = dispatcher.InterceptNextSend()
	expectMessage(t, mockMessage("Hello, World again!"), payloadType, addrs[1], addressedMsg{sentMsg, to})

	dispatcher.SimulateReception(message{ackMsg, sentMsg.(message).Timestamp, mockMessage("")}, addrs[1])

	dispatcher.ExpectNothingFor(2 * time.Second)
}
