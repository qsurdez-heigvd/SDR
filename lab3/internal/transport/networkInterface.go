package transport

import "encoding/gob"

// MessageHandler represents any structure capable of handling a message received from the network.
type MessageHandler interface {
	HandleNetworkMessage(*Message) (wasHandled bool)
}

// HandlerID is a unique identifier for a handler.
type HandlerID uint32

// NetworkInterface represents a network interface that can send and receive messages.
type NetworkInterface interface {
	// Send a message to the given address.
	Send(addr Address, payload []byte) error
	// RegisterHandler registers a handler for incoming messages.
	RegisterHandler(MessageHandler) HandlerID
	// UnregisterHandler unregisters a handler.
	UnregisterHandler(id HandlerID)
	// Close the network interface.
	Close()
}

// Message represents what is sent and received by a network interface.
type Message struct {
	/** Source address */
	Source  Address
	Payload []byte
}

// RegisterToGob registers the message type to gob.
func (m Message) RegisterToGob() {
	gob.Register(m)
}
