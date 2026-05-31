package dispatcher

import (
	"bytes"
	"chatsapp/internal/logging"
	"chatsapp/internal/messages"
	"chatsapp/internal/server/pulsing"
	"chatsapp/internal/server/routing"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils"
	"encoding/gob"
	"log"
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
	// Send a message to a given destination asynchronously. The message is queued for sending and the method returns immediately.
	Send(msg Message, dest transport.Address)
	// Broadcast a message to all processes in the network. Will block until the message is guaranteed to have been received by all processes in the network
	Broadcast(msg Message) (received []transport.Address)
	// Close the dispatcher, cleaning up resources
	Close()
}

// Local implementation of the Dispatcher interface. Hiding the implementation behind an interface allows for easier testing of modules using the dispatcher.
type dispatcherImpl struct {
	logger *logging.Logger

	selfAddr transport.Address

	network         transport.NetworkInterface
	directNeighbors []transport.Address
	router          routing.Router

	receivedMessages chan<- receivedMessage
	registrations    chan protocolRegistration
	netToRouter      chan<- messages.Sourced[routing.RoutedMessage]
	netToPulsar      chan<- pulsing.ReceivedMessage[messages.Message]

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

	routerToNet := make(chan messages.Destined[routing.RoutedMessage])
	netToRouter := utils.NewBufferedChan[messages.Sourced[routing.RoutedMessage]]()
	pulsarToNet := make(chan pulsing.SentMessage[messages.Message])
	netToPulsar := utils.NewBufferedChan[pulsing.ReceivedMessage[messages.Message]]()

	receivedMessages := utils.NewBufferedChan[receivedMessage]()

	d := dispatcherImpl{
		logger:   logger,
		selfAddr: selfAddr,

		network:         network,
		directNeighbors: directNeighbors,

		receivedMessages: receivedMessages.Inlet(),
		registrations:    make(chan protocolRegistration),

		netToRouter: netToRouter.Inlet(),
		netToPulsar: netToPulsar.Inlet(),

		closeChan: make(chan struct{}),
	}

	pulsarBuilder := pulsing.NewBuilder[messages.Message]()
	pulsarBuilder.SetNetConnection(selfAddr, directNeighbors, pulsarToNet, netToPulsar.Outlet())

	d.router = routing.NewRouter(
		logger.WithPostfix("Router"),
		selfAddr,
		directNeighbors,
		routerToNet,
		netToRouter.Outlet(),
		pulsarBuilder,
	)

	go d.dispatch(receivedMessages.Outlet())
	d.Register(routing.RoutedMessage{}, d.handleIncomingRouterMessage)
	d.Register(pulsing.PulsarMessage[messages.Message]{}, d.handlePulsarMessage)

	go d.handleRPMessages(d.router.ReceivedMessageChan(), routerToNet, pulsarToNet)

	network.RegisterHandler(&d)

	return &d
}

func (d *dispatcherImpl) handleRPMessages(
	routerIncoming <-chan messages.Sourced[messages.Message],
	routerMessages <-chan messages.Destined[routing.RoutedMessage],
	pulsarMessages <-chan pulsing.SentMessage[messages.Message],
) {
	for {
		select {
		case msg := <-routerIncoming:
			wasHandledChan := make(chan bool, 1)
			d.receivedMessages <- receivedMessage{msg.From, msg.Message, wasHandledChan}
			<-wasHandledChan
		case msg := <-routerMessages:
			d.sendToNet(msg.Message, msg.To)
		case msg := <-pulsarMessages:
			d.sendToNet(msg.Message, msg.To)
		case <-d.closeChan:
			return
		}
	}
}

func (d *dispatcherImpl) handlePulsarMessage(msg messages.Message, source transport.Address) {
	if pulsarMsg, ok := msg.(pulsing.PulsarMessage[messages.Message]); !ok {
		log.Panicf("Received unexpected message of type %T, was expecting PulsarMessage", msg)
	} else {
		d.netToPulsar <- pulsing.ReceivedMessage[messages.Message]{Message: pulsarMsg, From: source}
	}
}

func (d *dispatcherImpl) handleIncomingRouterMessage(msg messages.Message, source transport.Address) {
	if routedMsg, ok := msg.(routing.RoutedMessage); !ok {
		log.Panicf("Received unecpected message of type %T, was expecting RoutedMessage", msg)
	} else {
		d.netToRouter <- messages.Sourced[routing.RoutedMessage]{Message: routedMsg, From: source}
	}
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
func (d *dispatcherImpl) dispatch(receivedMessages <-chan receivedMessage) {
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
		case received, ok := <-receivedMessages:
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

// Send sends a message to a given destination asynchronously.
func (d *dispatcherImpl) Send(msg Message, dest transport.Address) {
	if d.isClosed() {
		d.logger.Warn("Dispatcher is closed, not sending message")
		return
	}

	err := d.router.Send(msg, dest)
	if err != nil {
		d.logger.Error("Error sending message", err)
	}
}

func (d *dispatcherImpl) Broadcast(msg Message) []transport.Address {
	if d.isClosed() {
		d.logger.Warn("Dispatcher is closed, not broadcasting message")
		return nil
	}

	received, err := d.router.Broadcast(msg)
	if err != nil {
		d.logger.Error("Error broadcasting message", err)
		return nil
	}

	return received
}

func (d *dispatcherImpl) sendToNet(msg Message, dest transport.Address) {
	encodedMsg := d.encodeMessage(msg)

	err := d.network.Send(dest, encodedMsg)

	if err != nil {
		d.logger.Error("Error sending message")
		return
	}
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
