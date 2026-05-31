package messages

import "chatsapp/internal/transport"

// Message is a generic message interface, that can be encoded with gob.
type Message interface {
	RegisterToGob()
}

// Sourced wraps a message together with the address of the sender.
type Sourced[M Message] struct {
	Message M
	From    transport.Address
}

// Destined wraps a message together with the address of the receiver.
type Destined[M Message] struct {
	Message M
	To      transport.Address
}
