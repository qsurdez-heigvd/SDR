package dispatcher

import (
	"bytes"
	"chatsapp/internal/logging"
	"chatsapp/internal/messages"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils"
	"encoding/gob"
	"reflect"
)

// Message is a generic interface for messages that can be sent and received through the dispatcher
type Message = messages.Message

// ProtocolHandler is any function capable of handling a dispatched message
type ProtocolHandler func(msg Message, source transport.Address)

type receivedMessage struct {
	from       transport.Address
	msg        Message
	wasHandled chan<- bool
}

type protocolRegistration struct {
	msgType reflect.Type
	handler ProtocolHandler
}

// Dispatcher is responsible for routing messages to the appropriate handlers
type Dispatcher interface {
	// Register a handler for a given message type
	//   - msg: An istance of the message type to register
	//   - handler: The handler to call when a message of this type is received
	Register(msg Message, handler ProtocolHandler)
	// Send a message to a given destination. Will block until the message is guaranteed to have been received by the destination
	Send(msg Message, dest transport.Address)
	// SendAsync a message to a given destination. Returns a channel that returns an error or nil when the message has been sent
	SendAsync(msg Message, dest transport.Address) (done chan error)
	// Close the dispatcher, cleaning up resources
	Close()
}

// Local implementation of the Dispatcher interface. Hiding the implementation behind an interface allows for easier testing of modules using the dispatcher.
type dispatcherImpl struct {
	logger *logging.Logger

	selfAddr transport.Address

	network transport.NetworkInterface

	receivedMessages chan receivedMessage
	registrations    chan protocolRegistration

	sendChan chan<- sendRequest

	closeChan chan struct{}
}

type sendRequest struct {
	messages.Destined[messages.Message]
	response chan error
}

/*
NewDispatcher constructs a new dispatcher instance
  - logger: The logger to use for logging messages
  - selfAddr: The address of this process
  - directNeighbors: The addresses of the direct neighbors of this process
  - network: The network interface to use for sending and receiving messages
*/
func NewDispatcher(logger *logging.Logger, selfAddr transport.Address, directNeighbors []transport.Address, network transport.NetworkInterface) Dispatcher {
	return newDispatcher(logger, selfAddr, directNeighbors, network)
}

func newDispatcher(logger *logging.Logger, selfAddr transport.Address, directNeighbors []transport.Address, network transport.NetworkInterface) *dispatcherImpl {
	if utils.SliceContains(directNeighbors, selfAddr) {
		// Remove self from neighbors
		newNeighbors := make([]transport.Address, 0, len(directNeighbors)-1)
		for _, neighbor := range directNeighbors {
			if neighbor != selfAddr {
				newNeighbors = append(newNeighbors, neighbor)
			}
		}
		directNeighbors = newNeighbors
	}

	sendChan := utils.NewBufferedChan[sendRequest]()

	d := dispatcherImpl{
		logger:   logger,
		selfAddr: selfAddr,

		network: network,

		receivedMessages: make(chan receivedMessage, 100),
		registrations:    make(chan protocolRegistration),

		sendChan: sendChan.Inlet(),

		closeChan: make(chan struct{}),
	}

	network.RegisterHandler(&d)

	go d.dispatch()

	go func() {
		for msg := range sendChan.Outlet() {
			encodedMsg := d.encodeMessage(msg.Message)

			err := d.network.Send(msg.To, encodedMsg)

			msg.response <- err
		}
	}()

	return &d
}

func (d *dispatcherImpl) HandleNetworkMessage(msg *transport.Message) (wasHandled bool) {
	wasHandled = true

	decodedMsg := d.decodeMessage(msg.Payload)
	wasHandledChan := make(chan bool, 1)
	d.receivedMessages <- receivedMessage{msg: decodedMsg, from: msg.Source, wasHandled: wasHandledChan}

	return <-wasHandledChan
}

func (d *dispatcherImpl) Close() {
	close(d.closeChan)
}

// Reports whether the dispatcher is closed
func (d *dispatcherImpl) isClosed() bool {
	select {
	case <-d.closeChan:
		return true
	default:
		return false
	}
}

/*
Main goroutine that maintains the handlers and dispatches messages to them.

Because handlers can be registered dynamically during the execution, they represent dynamic state that must be maintained in a thread-safe way. In order to achieve this, they are handled by a single goroutine, which is this one.

This goroutine handles registration of handlers, and dispatching of messages to the appropriate handlers.
*/
func (d *dispatcherImpl) dispatch() {
	handlers := make(map[reflect.Type]ProtocolHandler)
	for {
		select {
		case reg, ok := <-d.registrations:
			if !ok {
				return
			}
			if _, ok := handlers[reg.msgType]; ok {
				d.logger.Warn("Handler already registered for message type. Overwriting it...", reg.msgType)
			}
			handlers[reg.msgType] = reg.handler
		case received, ok := <-d.receivedMessages:
			if !ok {
				return
			}
			d.logger.Infof("Received message %v from %v", received.msg, received.from)
			handler, exists := handlers[reflect.TypeOf(received.msg)]
			if !exists {
				d.logger.Infof("No handler for message %v. Not handling it.", received.msg)
				received.wasHandled <- false
				continue
			}
			received.wasHandled <- true

			handler(received.msg, received.from)
			d.logger.Infof("Done handling message %v", received.msg)
		case <-d.closeChan:
			return
		}
	}
}

func (d *dispatcherImpl) Register(msg Message, handler ProtocolHandler) {
	msg.RegisterToGob()

	if d.isClosed() {
		d.logger.Warn("Dispatcher is closed, not registering handler")
		return
	}

	d.registrations <- protocolRegistration{
		msgType: reflect.TypeOf(msg),
		handler: handler,
	}
}

// Send sends and waits for the remote to acknowledge having received the answer
func (d *dispatcherImpl) Send(msg Message, dest transport.Address) {
	response := d.SendAsync(msg, dest)
	err := <-response

	if err != nil {
		d.logger.Errorf("Error sending message to %v: %v", dest, err)
	}
}

func (d *dispatcherImpl) SendAsync(msg Message, dest transport.Address) (done chan error) {
	response := make(chan error, 1)

	d.sendChan <- sendRequest{
		Destined: messages.Destined[messages.Message]{
			To:      dest,
			Message: msg,
		},
		response: response,
	}

	return response
}

// Encodes a message to a byte slice
func (d *dispatcherImpl) encodeMessage(msg Message) []byte {
	encodedMsg := bytes.Buffer{}
	encoder := gob.NewEncoder(&encodedMsg)
	err := encoder.Encode(&msg)
	if err != nil {
		panic(err)
	}

	return encodedMsg.Bytes()
}

// Decodes a message from a byte slice
func (d *dispatcherImpl) decodeMessage(encodedMsg []byte) Message {
	var decodedMsg Message
	decoder := gob.NewDecoder(bytes.NewReader(encodedMsg))
	err := decoder.Decode(&decodedMsg)
	if err != nil {
		panic(err)
	}

	return decodedMsg
}
