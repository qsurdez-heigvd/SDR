package transport

import (
	"chatsapp/internal/utils"
)

// MockNetworkInterface is a mock network interface that can simulate sending and receiving messages.
type MockNetworkInterface struct {
	source Address

	uidGenerator <-chan HandlerID
	handlers     map[HandlerID]MessageHandler

	SentMessages chan *DestinedMessage
}

// NewMockNetworkInterface creates a new [MockNetworkInterface].
func NewMockNetworkInterface(addr Address) *MockNetworkInterface {
	return &MockNetworkInterface{
		source:       addr,
		uidGenerator: genHandlerIds(),
		handlers:     make(map[HandlerID]MessageHandler),
		SentMessages: make(chan *DestinedMessage, 1),
	}
}

// DestinedMessage is a transport message together with its destination.
type DestinedMessage struct {
	Message
	To Address
}

// Creates a new [mockMessage] with the given source, destination and payload.
func newDestinedMessage(Source Address, Destination Address, Payload []byte) DestinedMessage {
	return DestinedMessage{
		Message: Message{
			Source:  Source,
			Payload: Payload,
		},
		To: Destination,
	}
}

// Returns a generator of unique handler IDs
func genHandlerIds() <-chan HandlerID {
	g := utils.NewUIDGenerator()
	ch := make(chan HandlerID)
	go func() {
		for {
			ch <- HandlerID(<-g)
		}
	}()
	return ch
}

// Send sends a message to the given destination.
func (m *MockNetworkInterface) Send(dest Address, payload []byte) error {
	msg := newDestinedMessage(m.source, dest, payload)
	m.SentMessages <- &msg
	return nil
}

// RegisterHandler registers a handler for messages of a given type.
func (m *MockNetworkInterface) RegisterHandler(handler MessageHandler) HandlerID {
	id := <-m.uidGenerator
	m.handlers[id] = handler
	return id
}

// UnregisterHandler unregisters a handler for messages of a given type.
func (m *MockNetworkInterface) UnregisterHandler(id HandlerID) {
	_, ok := m.handlers[id]
	if ok {
		delete(m.handlers, id)
	}
}

// SimulateReception simulates the reception of a message.
func (m *MockNetworkInterface) SimulateReception(msg *Message) {
	for _, handler := range m.handlers {
		handler.HandleNetworkMessage(msg)
	}
}

// InterceptSentMessage intercepts a sent message.
func (m *MockNetworkInterface) InterceptSentMessage() *DestinedMessage {
	return <-m.SentMessages
}

// Close closes the network interface.
func (m *MockNetworkInterface) Close() {
	close(m.SentMessages)
}
