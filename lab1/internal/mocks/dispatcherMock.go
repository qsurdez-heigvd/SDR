package mocks

import (
	"chatsapp/internal/messages"
	"chatsapp/internal/server/dispatcher"
	"chatsapp/internal/transport"
	"reflect"
	"testing"
	"time"
)

type message = messages.Message
type address = transport.Address

type destinedMessage messages.Destined[message]

// MockDispatcher is a mock implementation of the dispatcher interface.
type MockDispatcher struct {
	t *testing.T

	routed bool

	handlers map[reflect.Type]dispatcher.ProtocolHandler

	sentMessages      chan destinedMessage
	broadcastMessages chan message
}

// NewMockRoutedDispatcher creates a new mock dispatcher that supports routing (specifically the Broadcast method).
func NewMockRoutedDispatcher(t *testing.T) MockDispatcher {
	disp := NewMockDispatcher(t)
	disp.routed = true
	disp.broadcastMessages = make(chan message, 100)
	return disp
}

// NewMockDispatcher creates a new mock dispatcher.
func NewMockDispatcher(t *testing.T) MockDispatcher {
	return MockDispatcher{
		t: t,

		handlers: make(map[reflect.Type]dispatcher.ProtocolHandler),

		sentMessages: make(chan destinedMessage, 100),
	}
}

func expectMessage(t *testing.T, expected messages.Message, to address, actual destinedMessage) {
	if actual.To != to {
		t.Errorf("Expected message to %v, got %v", to, actual.To)
	}

	if !reflect.DeepEqual(actual.Message, expected) {
		t.Errorf("Expected message %v, got %v", expected, actual.Message)
	}
}

// ExpectSentMessage waits for a message to be sent and then compares it to the expected message.
func (d MockDispatcher) ExpectSentMessage(expected messages.Message, to address) {
	expectMessage(d.GetTesting(), expected, to, d.getSentMessage())
}

// GetTesting returns the testing.T instance associated with the dispatcher.
func (d MockDispatcher) GetTesting() *testing.T {
	return d.t
}

// Register registers a handler for a specific message type.
func (d MockDispatcher) Register(msg message, handler dispatcher.ProtocolHandler) {
	d.handlers[reflect.TypeOf(msg)] = handler
}

// Send sends a message to a specific node in the network.
func (d MockDispatcher) Send(msg message, dest address) {
	d.sentMessages <- destinedMessage{Message: msg, To: dest}
}

// Broadcast sends a message to all nodes in the network.
func (d MockDispatcher) Broadcast(msg message) (received []transport.Address) {
	if d.routed {
		d.t.Fatal("Broadcast not supported in non-routed dispatcher")
	} else {
		d.broadcastMessages <- msg
	}
	return []transport.Address{}
}

// Close closes the dispatcher.
func (d MockDispatcher) Close() {
}

func (d MockDispatcher) getSentMessage() destinedMessage {
	return <-d.sentMessages
}

// InterceptNextSend returns the next message passed to the Send method of the dispatcher, along with the destination address.
func (d MockDispatcher) InterceptNextSend() (messages.Message, transport.Address) {
	sentMsg := d.getSentMessage()
	return sentMsg.Message, sentMsg.To
}

// InterceptBroadcast returns the next message passed to the Broadcast method of the dispatcher.
func (d MockDispatcher) InterceptBroadcast() messages.Message {
	if !d.routed {
		d.t.Fatal("InterceptBroadcast not supported in non-routed dispatcher")
		return nil
	}
	return <-d.broadcastMessages
}

// ExpectNothingFor waits for a duration and then fails the test if a message is sent during that time.
func (d MockDispatcher) ExpectNothingFor(duration time.Duration) {
	select {
	case msg := <-d.sentMessages:
		d.t.Error("Expected no message to be sent; received", msg)
	case <-time.After(duration):
	}
}

// SimulateReception simulates the reception of a message by the dispatcher.
func (d MockDispatcher) SimulateReception(msg message, source address) {
	for msgType, handler := range d.handlers {
		if reflect.TypeOf(msg) == msgType {
			handler(msg, source)
			return
		}
	}
	d.t.Errorf("No handler found for message %v", msg)
}
