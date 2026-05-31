package pulsing

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/transport"
)

// Builder is an interface for building a Pulsar in two phases: connection to the network, and describing the handlers.
type Builder[M any] interface {
	// SetNetConnection sets the connection to the network for the Pulsar to use.
	SetNetConnection(transport.Address, []transport.Address, NetSender[M], NetReceiver[M])
	// Build creates a new Pulsar instance, given handlers for reception of pulses and echoes.
	Build(*logging.Logger, PulseHandler[M], EchoHandler[M]) Pulsar[M]
}

type builder[M any] struct {
	self         transport.Address
	neighbors    []transport.Address
	intoNet      NetSender[M]
	fromNet      NetReceiver[M]
	readyToBuild bool
}

// NewBuilder creates a new Pulsar builder.
func NewBuilder[M any]() Builder[M] {
	return &builder[M]{}
}

func (b *builder[M]) SetNetConnection(
	self transport.Address,
	neighbors []transport.Address,
	intoNet NetSender[M],
	fromNet NetReceiver[M],
) {
	b.self = self
	b.neighbors = neighbors
	b.intoNet = intoNet
	b.fromNet = fromNet
	b.readyToBuild = true
}

func (b *builder[M]) Build(
	logger *logging.Logger,
	pulseHandler PulseHandler[M],
	echoHandler EchoHandler[M],
) Pulsar[M] {
	if !b.readyToBuild {
		panic("Pulsar builder not ready to build. SetNetConnection must be called first")
	}
	return NewPulsar(logger, b.self, pulseHandler, echoHandler, b.intoNet, b.fromNet, b.neighbors)
}
