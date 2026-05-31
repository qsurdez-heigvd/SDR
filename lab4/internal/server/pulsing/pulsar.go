package pulsing

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/messages"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils"
	"chatsapp/internal/utils/option"
	"encoding/gob"
	"fmt"
)

// PulseID is a unique identifier for a pulse
type PulseID struct {
	Source transport.Address
	utils.UID
}

// String returns a string representation of the PulseID.
func (id PulseID) String() string {
	return fmt.Sprintf("%v-%v", id.Source, id.UID)
}

// NewPulseID creates a new pulse ID with the given source address and UID.
func NewPulseID(source transport.Address, id utils.UID) PulseID {
	return PulseID{Source: source, UID: id}
}

// Type is the type of a pulsar message.
type Type int

const (
	// Pulse is the type given to a pulse message.
	Pulse Type = iota
	// Echo is the type given to an echo message.
	Echo
)

// SourcedMessage is a message with a source address.
type SourcedMessage[M any] struct {
	Msg  M
	From transport.Address
}

// PulsarMessage is the type of messages a pulsar uses to communicate with other pulsars.
type PulsarMessage[M any] struct {
	Type    Type
	Payload M
	ID      PulseID
}

// RegisterToGob registers the PulsarMessage type to Gob for encoding/decoding.
func (p PulsarMessage[M]) RegisterToGob() {
	gob.Register(p)
	gob.Register(PulseID{})
}

// String returns a string representation of the PulsarMessage.
func (p PulsarMessage[M]) String() string {
	tp := "Unknown"
	switch p.Type {
	case Pulse:
		tp = "Pulse"
	case Echo:
		tp = "Echo"
	}
	return fmt.Sprintf("Pulsar-%v{%v, %v}", tp, p.ID, p.Payload)
}

// SentMessage is a [PulsarMessage] that a [Pulsar] instance wants to send over the network to a given destination address.
type SentMessage[M any] messages.Destined[PulsarMessage[M]]

// ReceivedMessage is a [PulsarMessage] that a [Pulsar] instance has received from the network, along with the address of the sender.
type ReceivedMessage[M any] messages.Sourced[PulsarMessage[M]]

// PulseHandler is the type of functions that can handle a received pulse and return the pulse to be sent further.
type PulseHandler[M any] func(id PulseID, received M, from transport.Address) (propagated M, err error)

// EchoHandler is the type of functions that can handle received echoes and aggregate them into the one that will be propagated.
type EchoHandler[M any] func(id PulseID, pulse M, echoes []SourcedMessage[M]) (propagated M, err error)

// NetSender is the type of channels on which a [Pulsar] instance sends messages intended to be over the network.
type NetSender[M any] chan<- SentMessage[M]

// NetReceiver is the type of channels on which a [Pulsar] instance receives messages from the network.
type NetReceiver[M any] <-chan ReceivedMessage[M]

// Pulsar is the interface for a pulsar instance.
type Pulsar[M any] interface {
	// StartPulse initiates a pulse-and-echo algorithm starting from this node.
	// It blocks until all echoes have been received and aggregated, then returns the final aggregated echo.
	StartPulse(pulse M) (echo M, err error)
}

// pulsar is the local implementation of the Pulsar interface.
type pulsar[M any] struct {
	logger            *logging.Logger
	receivedPulseChan chan ReceivedMessage[M]
	receivedEchoChan  chan ReceivedMessage[M]
	newPulseRequested chan pulseRequested[M]
	uuidGenerator     utils.UIDGenerator
}

// nodeState is the concatenation of the node info regarding the ongoing pulse
type nodeState[M any] struct {
	pulseMsgReceived M
	finalEchoChan    option.Option[chan M]
	echoes           []SourcedMessage[M]
	nbEchoesReceived int
	nbEchoesExpected int
	parent           option.Option[transport.Address]
}

type pulseRequested[M any] struct {
	msg  M
	echo chan M
}

// NewPulsar creates a new pulsar instance.
func NewPulsar[M any](
	logger *logging.Logger,
	self transport.Address,
	pulseHandler PulseHandler[M],
	echoHandler EchoHandler[M],
	pulsarToNet NetSender[M],
	netToPulsar NetReceiver[M],
	neighbors []transport.Address,
) Pulsar[M] {
	p := pulsar[M]{
		logger:            logger,
		receivedPulseChan: make(chan ReceivedMessage[M]),
		receivedEchoChan:  make(chan ReceivedMessage[M]),
		newPulseRequested: make(chan pulseRequested[M]),
		uuidGenerator:     utils.NewUIDGenerator(),
	}

	go p.handleReceivedMessages(netToPulsar)
	go p.handleEvents(
		echoHandler,
		pulseHandler,
		pulsarToNet,
		neighbors,
		self,
	)

	return &p
}

// StartPulse implements the Pulsar interface method.
func (p *pulsar[M]) StartPulse(pulse M) (echo M, err error) {
	echoChan := make(chan M)
	p.newPulseRequested <- pulseRequested[M]{
		msg:  pulse,
		echo: echoChan,
	}
	return <-echoChan, nil
}

func (p *pulsar[M]) handleEvents(
	callbackEcho EchoHandler[M],
	callbackPulse PulseHandler[M],
	pulsarToNet NetSender[M],
	neighbors []transport.Address,
	self transport.Address,
) {

	p.logger.Info("Start of the event handler")

	expectedEchos := len(neighbors)
	pulsarState := make(map[PulseID]*nodeState[M])

	for {
		select {
		case newPulse := <-p.newPulseRequested:
			p.logger.Info("New pulse requested")
			pulseID := NewPulseID(self, <-p.uuidGenerator)
			pulsarState[pulseID] = &nodeState[M]{
				pulseMsgReceived: newPulse.msg,
				finalEchoChan:    option.Some(newPulse.echo),
				echoes:           make([]SourcedMessage[M], 0, expectedEchos),
				nbEchoesReceived: 0,
				nbEchoesExpected: expectedEchos,
				parent:           option.None[transport.Address](),
			}
			p.sendPulseToNeighbors(
				pulseID,
				newPulse.msg,
				option.None[transport.Address](),
				pulsarToNet,
				neighbors,
			)
			p.checkReceivedEchoes(pulseID, pulsarState, callbackEcho, pulsarToNet)
		case echoReceived := <-p.receivedEchoChan:
			p.logger.Info("Received a echo")
			pulseID := echoReceived.Message.ID
			pulseState := pulsarState[pulseID]
			pulseState.echoes = append(pulseState.echoes, SourcedMessage[M]{
				Msg:  echoReceived.Message.Payload,
				From: echoReceived.From,
			})
			pulseState.nbEchoesReceived++
			p.checkReceivedEchoes(pulseID, pulsarState, callbackEcho, pulsarToNet)

		case pulseReceived := <-p.receivedPulseChan:
			p.logger.Info("Received a pulse")
			pulseID := pulseReceived.Message.ID
			_, ok := pulsarState[pulseID]
			if ok {
				p.logger.Info("Already received a pulse for this propagation, reducing the number of expected echoes")
				pulsarState[pulseID].nbEchoesExpected--
			} else {
				pulsarState[pulseID] = &nodeState[M]{
					pulseMsgReceived: pulseReceived.Message.Payload,
					finalEchoChan:    option.None[chan M](),
					echoes:           make([]SourcedMessage[M], 0, expectedEchos),
					nbEchoesReceived: 0,
					nbEchoesExpected: expectedEchos - 1,
					parent:           option.Some[transport.Address](pulseReceived.From),
				}
				pulseToPropagate, _ := callbackPulse(pulseID, pulseReceived.Message.Payload, pulseReceived.From)
				p.sendPulseToNeighbors(
					pulseID,
					pulseToPropagate,
					option.Some[transport.Address](pulseReceived.From),
					pulsarToNet,
					neighbors,
				)
			}
			p.checkReceivedEchoes(pulseID, pulsarState, callbackEcho, pulsarToNet)
		}
	}
}

func (p *pulsar[M]) handleReceivedMessages(fromNet NetReceiver[M]) {
	for {
		select {
		case messageReceived := <-fromNet:
			p.logger.Info("Received a message")
			switch messageReceived.Message.Type {
			case Pulse:
				p.receivedPulseChan <- messageReceived
			case Echo:
				p.receivedEchoChan <- messageReceived
			}
		}
	}
}

func (p *pulsar[M]) sendPulseToNeighbors(
	pulseID PulseID,
	pulseMessage M,
	parent option.Option[transport.Address],
	pulsarToNet NetSender[M],
	neighbors []transport.Address,
) {
	for _, neighbor := range neighbors {
		if parent.IsNone() || neighbor != parent.Get() {
			pulsarToNet <- SentMessage[M]{
				Message: PulsarMessage[M]{
					Type:    Pulse,
					Payload: pulseMessage,
					ID:      pulseID,
				},
				To: neighbor,
			}
		}
	}
}

func (p *pulsar[M]) checkReceivedEchoes(
	pulseID PulseID,
	pulsarState map[PulseID]*nodeState[M],
	callbackEcho EchoHandler[M],
	pulsarToNet NetSender[M],
) {
	pulseState := pulsarState[pulseID]
	if pulseState.nbEchoesReceived == pulseState.nbEchoesExpected {
		aggregatedEchoes, _ := callbackEcho(
			pulseID,
			pulseState.pulseMsgReceived,
			pulseState.echoes,
		)
		if pulseState.parent.IsNone() {
			pulseState.finalEchoChan.Get() <- aggregatedEchoes
			delete(pulsarState, pulseID)
		} else {
			pulsarToNet <- SentMessage[M]{
				Message: PulsarMessage[M]{
					Type:    Echo,
					Payload: aggregatedEchoes,
					ID:      pulseID,
				},
				To: pulseState.parent.Get(),
			}
			delete(pulsarState, pulseID)
		}
	}
}
