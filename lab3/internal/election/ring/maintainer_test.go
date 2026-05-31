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

func TestSendToNextWithTimeout(t *testing.T) {
	addrs := []transport.Address{
		{IP: "127.0.0.1", Port: 5000},
		{IP: "127.0.0.1", Port: 5001},
		{IP: "127.0.0.1", Port: 5002},
	}

	logger := logging.NewStdLogger("test")
	dispatcher := mocks.NewMockDispatcher(t)
	timeout := 200 * time.Millisecond
	ring := newRingMaintainer(logger, dispatcher, addrs[0], addrs, timeout)

	ring.SendToNext(mockMessage("Timeout Test"))

	// First send to addrs[1]
	sentMsg, to := dispatcher.InterceptNextSend()
	expectMessage(t, mockMessage("Timeout Test"), payloadType, addrs[1], addressedMsg{sentMsg, to})
	timestamp := sentMsg.(message).Timestamp

	// Don't send ACK, wait for timeout
	time.Sleep(timeout + 100*time.Millisecond)

	// Should retry to addrs[2]
	retriedMsg, retriedTo := dispatcher.InterceptNextSend()
	expectMessage(t, mockMessage("Timeout Test"), payloadType, addrs[2], addressedMsg{retriedMsg, retriedTo})

	// Verify same timestamp (same message)
	if retriedMsg.(message).Timestamp != timestamp {
		t.Errorf("Expected same timestamp for retried message")
	}

	// Send ACK from second node
	dispatcher.SimulateReception(message{ackMsg, timestamp, nil}, addrs[2])

	dispatcher.ExpectNothingFor(timeout + 100*time.Millisecond)
}

func TestMultipleTimeouts(t *testing.T) {
	addrs := []transport.Address{
		{IP: "127.0.0.1", Port: 5000},
		{IP: "127.0.0.1", Port: 5001},
		{IP: "127.0.0.1", Port: 5002},
		{IP: "127.0.0.1", Port: 5003},
	}

	logger := logging.NewStdLogger("test")
	dispatcher := mocks.NewMockDispatcher(t)
	timeout := 150 * time.Millisecond
	ring := newRingMaintainer(logger, dispatcher, addrs[0], addrs, timeout)

	ring.SendToNext(mockMessage("Multi-Timeout Test"))

	// First send to addrs[1]
	sentMsg, to := dispatcher.InterceptNextSend()
	expectMessage(t, mockMessage("Multi-Timeout Test"), payloadType, addrs[1], addressedMsg{sentMsg, to})
	timestamp := sentMsg.(message).Timestamp

	// Wait for timeout - should send to addrs[2]
	time.Sleep(timeout + 50*time.Millisecond)
	sentMsg2, to2 := dispatcher.InterceptNextSend()
	expectMessage(t, mockMessage("Multi-Timeout Test"), payloadType, addrs[2], addressedMsg{sentMsg2, to2})

	// Wait for another timeout - should send to addrs[3]
	time.Sleep(timeout + 50*time.Millisecond)
	sentMsg3, to3 := dispatcher.InterceptNextSend()
	expectMessage(t, mockMessage("Multi-Timeout Test"), payloadType, addrs[3], addressedMsg{sentMsg3, to3})

	// Finally send ACK from third retry
	dispatcher.SimulateReception(message{ackMsg, timestamp, nil}, addrs[3])

	dispatcher.ExpectNothingFor(timeout + 50*time.Millisecond)
}

func TestRetriesDirectNextForNewMessage(t *testing.T) {
	addrs := []transport.Address{
		{IP: "127.0.0.1", Port: 5000},
		{IP: "127.0.0.1", Port: 5001},
		{IP: "127.0.0.1", Port: 5002},
		{IP: "127.0.0.1", Port: 5003},
	}
	logger := logging.NewStdLogger("test")
	dispatcher := mocks.NewMockDispatcher(t)
	timeout := 150 * time.Millisecond
	ring := newRingMaintainer(logger, dispatcher, addrs[0], addrs, timeout)

	// Send a message to the direct next neighbor which times out
	ring.SendToNext(mockMessage("Retries direct next"))

	sentMsg, to := dispatcher.InterceptNextSend()
	expectMessage(t, mockMessage("Retries direct next"), payloadType, addrs[1], addressedMsg{sentMsg, to})

	// Second next neighbor acks the message
	sentMsg, to = dispatcher.InterceptNextSend()
	expectMessage(t, mockMessage("Retries direct next"), payloadType, addrs[2], addressedMsg{sentMsg, to})

	dispatcher.SimulateReception(message{ackMsg, sentMsg.(message).Timestamp, mockMessage("")}, addrs[2])

	dispatcher.ExpectNothingFor(2 * time.Second)

	// New send request should send to direct next neighbor
	ring.SendToNext(mockMessage("Retries direct next again"))
	sentMsg, to = dispatcher.InterceptNextSend()
	expectMessage(t, mockMessage("Retries direct next again"), payloadType, addrs[1], addressedMsg{sentMsg, to})

	dispatcher.SimulateReception(message{ackMsg, sentMsg.(message).Timestamp, mockMessage("")}, addrs[1])

	dispatcher.ExpectNothingFor(2 * time.Second)
}
