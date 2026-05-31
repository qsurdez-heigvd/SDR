package ring

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/messages"
	"chatsapp/internal/server/dispatcher"
	"chatsapp/internal/timestamps"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils"
	ringFromGo "container/ring"
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

type messageToSend struct {
	Message messages.Destined[message]
	Ring    ringFromGo.Ring
}

type awaitingPayload struct {
	Awaiting chan messages.Sourced[message]
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
	timeout               time.Duration
	logger                *logging.Logger
	dispatcher            dispatcher.Dispatcher
	self                  address
	ring                  []address
	toSend                chan messages.Message
	fromDispatcherAck     chan messages.Sourced[message]
	fromDispatcherPayload chan messages.Sourced[message]
	outgoingMessages      *utils.BufferedChan[messageToSend]
	requestForPrev        chan awaitingPayload
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
	var ringMaintainer = maintainer{
		timeout:               timeoutDuration,
		logger:                logger,
		dispatcher:            disp,
		self:                  self,
		ring:                  ring,
		toSend:                make(chan messages.Message),
		fromDispatcherAck:     make(chan messages.Sourced[message]),
		fromDispatcherPayload: make(chan messages.Sourced[message]),
		outgoingMessages:      utils.NewBufferedChan[messageToSend](),
		requestForPrev:        make(chan awaitingPayload),
	}

	// goroutine that need to send messages and another one to send ? or handle state ?
	disp.Register(message{}, ringMaintainer.handleIncomingMessages)

	go ringMaintainer.handleSendingMessages()
	go ringMaintainer.sendMessages()

	return &ringMaintainer
}

// what goroutines should we need ?
// go handleIncomingMessages
// go handleSendingMessages

func (r *maintainer) handleIncomingMessages(dispMsg messages.Message, source transport.Address) {
	msg, ok := dispMsg.(message)
	if !ok {
		r.logger.Warnf("Received invalid message from dispatcher: %T", dispMsg)
	}

	incomingMessage := messages.Sourced[message]{Message: msg, From: source}
	switch msg.Type {
	case payloadType:
		r.fromDispatcherPayload <- incomingMessage
	case ackMsg:
		r.fromDispatcherAck <- incomingMessage
	}
}

func (r *maintainer) handleSendingMessages() {
	// Implement it with the ringContainer go implementation
	ringContainer := ringFromGo.New(len(r.ring))
	for _, address := range r.ring {
		ringContainer.Value = address
		ringContainer = ringContainer.Next()
	}

	for ringContainer.Value != r.self {
		ringContainer = ringContainer.Next()
	}

	ts := timestamps.NewLamportHandler(timestamps.Pid(r.self.String()), 0)

	for {
		select {
		case msg := <-r.toSend:
			ringMessage := message{payloadType, ts.IncrementTimestamp(), msg}
			r.outgoingMessages.Inlet() <- messageToSend{Message: messages.Destined[message]{Message: ringMessage, To: ringContainer.Next().Value.(address)}, Ring: *ringContainer.Next()}
		case awaitingPayload := <-r.requestForPrev:
			// create a goroutine that waits for the the next time fromDispatcherPayload is talking
			go r.handleIncomingPayload(awaitingPayload)
		}
	}
}

func (r *maintainer) handleIncomingPayload(awaitingPayload awaitingPayload) {
	msg := <-r.fromDispatcherPayload
	// channel specific to each request ?
	awaitingPayload.Awaiting <- msg
	r.dispatcher.Send(message{ackMsg, msg.Message.Timestamp, nil}, msg.From)
}

func (r *maintainer) sendMessages() {

	for {
		select {
		case msg := <-r.outgoingMessages.Outlet():
			// send the message
			// wait for ack or timeout passed
			ring := &msg.Ring
			r.dispatcher.Send(msg.Message.Message, msg.Message.To)

		waitForResponse:
			for {
				select {
				case <-time.After(r.timeout):
					ring = ring.Next()
					nextAddress := ring.Value.(address)
					r.dispatcher.Send(msg.Message.Message, nextAddress)

				case ack := <-r.fromDispatcherAck:
					if ack.Message.Timestamp != msg.Message.Message.Timestamp {
						// if this ack does not concern this specific message we ignore it
						continue
					}

					break waitForResponse
				}
			}
		}
	}

}

func (r *maintainer) SendToNext(msg messages.Message) {
	r.toSend <- msg
}

func (r *maintainer) ReceiveFromPrev() messages.Message {
	awaiting := awaitingPayload{make(chan messages.Sourced[message])}

	r.requestForPrev <- awaiting
	response := <-awaiting.Awaiting

	return response.Message.Payload
}
