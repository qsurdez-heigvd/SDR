package ring

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/messages"
	"chatsapp/internal/server/dispatcher"
	"chatsapp/internal/timestamps"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils"
	"encoding/gob"
	"fmt"
	"time"
)

type address = transport.Address

type messageType uint8

const (
	payloadType messageType = iota
	ackMsg
)

type message struct {
	Type      messageType
	Timestamp timestamps.Timestamp
	Payload   messages.Message
}

func (m message) String() string {
	tp := "MSG"
	if m.Type == ackMsg {
		tp = "ACK"
	}
	return fmt.Sprintf("Ring-%s{%s, %s}", tp, m.Timestamp, m.Payload)
}

func (m message) RegisterToGob() {
	gob.Register(m.Timestamp)
	if m.Payload != nil {
		m.Payload.RegisterToGob()
	}
	gob.Register(m)
}

// Maintainer is the interface for a Ring Maintainer.
//
// It garantees that messages sent with [SentToNext] will be received by the next correct node in the ring, assuming low network latency. In case of network latencies, some nodes on the ring may be skipped.
type Maintainer interface {
	// SendToNext sends a message to the next correct node in the ring.
	SendToNext(msg messages.Message)
	// ReceiveFromPrev receives a message from the previous correct node in the ring.
	ReceiveFromPrev() messages.Message
}

// Unexported implementation of the RingMaintainer interface.
type maintainer struct {
	timeoutDuration time.Duration

	logger *logging.Logger

	// The ring of addresses, in order, such that the last address is the current process.
	successors []address

	dispatcher         dispatcher.Dispatcher
	dispatchedMessages *utils.BufferedChan[messages.Sourced[messages.Message]]

	sendRequests chan messages.Message
	receivedAcks chan timestamps.Timestamp
	receivedMsgs *utils.BufferedChan[messages.Message]
}

/*
NewRingMaintainer constructs and returns a new RingMaintainer instance.

Parameters
  - logger: The logger to use for logging messages.
  - dispatcher: The dispatcher to use for sending messages to processes in the network.
  - self: The address of the current process.
  - ring: A slice of addresses, the order of which represents the ring. Note that [self] *must* appear in the slice, though it may appear in any position.
*/
func NewRingMaintainer(logger *logging.Logger, dispatcher dispatcher.Dispatcher, self address, ring []address) Maintainer {
	return newRingMaintainer(logger, dispatcher, self, ring, 1*time.Second)
}

// Creates a new ringMaintainer instance.
func newRingMaintainer(logger *logging.Logger, disp dispatcher.Dispatcher, self address, ring []address, timeoutDuration time.Duration) *maintainer {
	// This function restructures the ring so that the current process is at the end of the slice, so that finding the next node is easier.

	idx := 0
	foundSelf := false
	successors := make([]address, 0, len(ring))
	for {
		if !foundSelf {
			if idx == len(ring) {
				panic("self not found in provided ring")
			}
			foundSelf = ring[idx] == self
		} else {
			successors = append(successors, ring[idx%len(ring)])
			if len(successors) == len(ring) {
				break
			}
		}
		idx++
	}
	if !foundSelf {
		panic("self not found in provided ring")
	}
	r := maintainer{
		timeoutDuration:    timeoutDuration,
		logger:             logger,
		successors:         successors,
		dispatcher:         disp,
		dispatchedMessages: utils.NewBufferedChan[messages.Sourced[messages.Message]](),
		sendRequests:       make(chan messages.Message),
		receivedAcks:       make(chan timestamps.Timestamp),
		receivedMsgs:       utils.NewBufferedChan[messages.Message](),
	}
	disp.Register(message{}, r.handleDispatchedMessage)
	go r.handleDispatchedMessages()
	go r.handleRing()
	return &r
}

func (r *maintainer) SendToNext(msg messages.Message) {
	r.sendRequests <- msg
}

func (r *maintainer) ReceiveFromPrev() messages.Message {
	return <-r.receivedMsgs.Outlet()
}

// Helper function to get the address of the current process.
func (r *maintainer) self() address {
	return r.successors[len(r.successors)-1]
}

func (r *maintainer) handleDispatchedMessages() {
	for dispatched := range r.dispatchedMessages.Outlet() {
		r.logger.Infof("Starting to handle dispatched message %s", dispatched)
		dispatchedMsg := dispatched.Message
		source := dispatched.From
		msg := dispatchedMsg.(message)
		switch msg.Type {
		case payloadType:
			r.receivedMsgs.Inlet() <- msg.Payload
			ack := message{
				Type:      ackMsg,
				Timestamp: msg.Timestamp,
			}
			r.dispatcher.Send(ack, source)
		case ackMsg:
			r.receivedAcks <- msg.Timestamp
		}
		r.logger.Infof("Ready to receive more dispatched messages after %s", dispatched)
	}
}

// Handles a message received from the network.
func (r *maintainer) handleDispatchedMessage(dispatchedMsg messages.Message, source address) {
	r.dispatchedMessages.Inlet() <- messages.Sourced[messages.Message]{Message: dispatchedMsg, From: source}
}

// Main loop for the ring maintainer.
func (r *maintainer) handleRing() {
	ts := timestamps.NewLamportHandler(timestamps.Pid(r.self().String()), 0)

	for payload := range r.sendRequests {
		r.logger.Infof("Received request to send %s", payload)
		t := ts.IncrementTimestamp()
		msg := message{payloadType, t, payload}
		acked := false

		nextIndex := -1
		for !acked {
			nextIndex++
			if nextIndex == len(r.successors)-1 {
				r.logger.Warnf("Next index is self... skipping")
				continue
			}
			next := r.successors[nextIndex%len(r.successors)]

			r.dispatcher.Send(msg, next)
			select {
			case ack := <-r.receivedAcks:
				acked = ack == t
				r.logger.Infof("Received an ack. Was it the one expected? %v", acked)
			case <-time.After(1 * time.Second):
				// nothing to do; just retry with the next successor
				r.logger.Warnf("Timeout waiting for ack from %s", next)
			}
		}
		r.logger.Infof("Ready to receive more requests to send")
	}
}
