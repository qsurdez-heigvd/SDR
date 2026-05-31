package pulsing

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/messages"
	"chatsapp/internal/transport"
	"reflect"
	"testing"
	"time"
)

// MockPulsarBuilder is a mock of Builder
type MockPulsarBuilder struct {
	t *testing.T

	self      transport.Address
	neighbors []transport.Address
	intoNet   NetSender[messages.Message]
	fromNet   NetReceiver[messages.Message]

	pulsar       *MockPulsar
	readyToBuild bool
	built        bool
}

// NewMockPulsarBuilder returns a new MockPulsarBuilder
func NewMockPulsarBuilder(t *testing.T) *MockPulsarBuilder {
	return &MockPulsarBuilder{
		t: t,
	}
}

type sendRequest struct {
	sentPulse     messages.Message
	resultingEcho chan messages.Message
}

// MockPulsar is a mock of Pulsar
type MockPulsar struct {
	t            *testing.T
	logger       *logging.Logger
	self         transport.Address
	pulseHandler PulseHandler[messages.Message]
	echoHandler  EchoHandler[messages.Message]
	sender       NetSender[messages.Message]
	receiver     NetReceiver[messages.Message]
	neighbors    []transport.Address

	sendRequests chan sendRequest
}

// SetNetConnection sets the connection to the network for the Pulsar to use
func (b *MockPulsarBuilder) SetNetConnection(self transport.Address, neighbors []transport.Address, sender NetSender[messages.Message], receiver NetReceiver[messages.Message]) {
	if b.built {
		b.t.Fatal("MockPulsarBuilder already built but trying to set the net connection")
	}
	b.self = self
	b.neighbors = neighbors
	b.intoNet = sender
	b.fromNet = receiver
	b.readyToBuild = true
}

// Build returns a new MockPulsar
func (b *MockPulsarBuilder) Build(logger *logging.Logger, pulseHandler PulseHandler[messages.Message], echoHandler EchoHandler[messages.Message]) Pulsar[messages.Message] {
	if b.built || !b.readyToBuild {
		b.t.Fatal("MockPulsarBuilder not ready to build : either already built or not set up")
	}
	mock := MockPulsar{
		t:            b.t,
		logger:       logger,
		pulseHandler: pulseHandler,
		echoHandler:  echoHandler,

		sendRequests: make(chan sendRequest, 100),
	}
	b.pulsar = &mock
	b.built = true
	return &mock
}

// GetPulsar returns the MockPulsar that was built
func (b *MockPulsarBuilder) GetPulsar() *MockPulsar {
	if !b.built {
		b.t.Fatal("MockPulsarBuilder not built when trying to get the pulsar")
	}
	return b.pulsar
}

// StartPulse begins a new pulse and returns the final echo
func (p *MockPulsar) StartPulse(pulse messages.Message) (echo messages.Message, err error) {
	res := make(chan messages.Message)
	p.sendRequests <- sendRequest{sentPulse: pulse, resultingEcho: res}
	return <-res, nil
}

// SimulateSendToNetwork sends a message to the network
func (p *MockPulsar) SimulateSendToNetwork(msg PulsarMessage[messages.Message], to transport.Address) {
	p.sender <- SentMessage[messages.Message]{Message: msg, To: to}
}

// ExpectReceiveFromNetwork expects a message from the network
func (p *MockPulsar) ExpectReceiveFromNetwork(msg PulsarMessage[messages.Message], from transport.Address) {
	actual := <-p.receiver

	if actual.Message.Type != msg.Type {
		p.t.Errorf("Expected message type %v, got %v", msg.Type, actual.Message.Type)
	}

	if actual.Message.ID != msg.ID {
		p.t.Errorf("Expected message ID %v, got %v", msg.ID, actual.Message.ID)
	}

	if !reflect.DeepEqual(actual.Message.Payload, msg.Payload) {
		p.t.Errorf("Expected message payload %v, got %v", msg.Payload, actual.Message.Payload)
	}

	if actual.From != from {
		p.t.Errorf("Expected message from %v, got %v", from, actual.From)
	}
}

// SimulatePulseHandler simulates a call to the pulse handler
func (p *MockPulsar) SimulatePulseHandler(id PulseID, msg messages.Message, from transport.Address, expectedResulting messages.Message) {
	p.pulseHandler(id, msg, from)
}

// SimulateEchoHandler simulates a call to the echo handler
func (p *MockPulsar) SimulateEchoHandler(id PulseID, pulse messages.Message, echoes []SourcedMessage[messages.Message], expectedResulting messages.Message) {
	p.echoHandler(id, pulse, echoes)
}

// ExpectSend expects a send request
func (p *MockPulsar) ExpectSend(pulse messages.Message, resultingEcho messages.Message) {
	actual := <-p.sendRequests

	if actual.sentPulse != pulse {
		p.t.Errorf("Expected message %v, got %v", pulse, actual.sentPulse)
	}

	actual.resultingEcho <- resultingEcho
}

// ExpectNothingFor expects no send requests for a given duration
func (p *MockPulsar) ExpectNothingFor(d time.Duration) {
	select {
	case <-p.sendRequests:
		p.t.Errorf("Expected no send requests, got one")
	case <-p.receiver:
		p.t.Errorf("Expected no received messages from the network, got one")
	case <-time.After(d):
	}
}
