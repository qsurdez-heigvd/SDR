package tcp

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils"
	"encoding/gob"
	"time"
)

type address = transport.Address
type message = transport.Message
type handlerID = transport.HandlerID
type msgHandler = transport.MessageHandler
type netInterface = transport.NetworkInterface

// Payload associated to the address to which it is destined.
type sendRequest struct {
	addr    address
	payload []byte
	err     chan<- error
}

// Internal representation of a handler registration.
type registration struct {
	id      handlerID
	handler msgHandler
}

// TCP implements the [NetworkInterface] interface for TCP connections.
type TCP struct {
	uidGenerator utils.UIDGenerator
	logger       *logging.Logger

	local address

	remotes *remotesHandler

	sendRequests     chan sendRequest
	receivedMessages chan message
	registrations    chan registration
	unregistrations  chan handlerID

	closeChan chan struct{}
}

// NewTCP constructs and returns a new [TCP] instance capable of sending messages to a fixed set of neighbors.
//   - self: The address of the local node.
//   - neighbors: The addresses of the neighbors of the local node, itself excluded.
//   - logger: The logger to use for logging messages.
func NewTCP(local address, log *logging.Logger) transport.NetworkInterface {
	gob.Register(message{})

	receivedRequests := make(chan message)

	tcp := TCP{
		local:        local,
		logger:       log,
		uidGenerator: utils.NewUIDGenerator(),

		remotes: newRemoteHandler(local, log.WithPostfix("remotes"), receivedRequests),

		sendRequests:     make(chan sendRequest),
		receivedMessages: receivedRequests,

		registrations:   make(chan registration),
		unregistrations: make(chan handlerID),

		closeChan: make(chan struct{}),
	}

	go tcp.handleSendRequests()
	go tcp.handleReceivedMessages()

	return &tcp
}

// Main goroutine for handling received messages.
//
// Because handlers can register and unregister dynamically during execution, it must be handled by a single goroutine to avoid concurrent access.
//
// This goroutine manages these handlers, (un)registration requests, and dispatching received messages to the registered handlers.
func (tcp *TCP) handleReceivedMessages() {
	registeredHandlers := make(map[handlerID]msgHandler)

	for {
		tcp.logger.Infof("Waiting for new messages or registrations...")
		select {
		case msg := <-tcp.receivedMessages:
			tcp.logger.Info("TCP received message from ", msg.Source, " from remote. Dispatching it among ", len(registeredHandlers), " handlers.")
			tcp.handleReceivedMessage(msg, registeredHandlers)
			tcp.logger.Infof("Message from %v dispatched.", msg.Source)
		case reg := <-tcp.registrations:
			registeredHandlers[reg.id] = reg.handler
		case id := <-tcp.unregistrations:
			delete(registeredHandlers, id)
		case <-tcp.closeChan:
			tcp.logger.Warn("TCP's received-messages handler is closing.")
			return
		}
	}
}

// Dispatches a received message to the registered handlers.
func (tcp *TCP) handleReceivedMessage(msg message, handlers map[handlerID]msgHandler) {
	done := make(chan bool, 1)
	defer close(done)
	go func() {
		select {
		case <-done:
			return
		case <-time.After(10 * time.Second):
			panic("A handler seems to be stuck in a deadlock.")
		}
	}()
	wasHandled := false
	for _, handler := range handlers {
		if handler.HandleNetworkMessage(&msg) {
			wasHandled = true
			break
		}
	}
	if !wasHandled {
		tcp.logger.Warn("No handler found for message type")
	}
}

// Main goroutine for sending requests.
//
// This goroutine listens for requests to send messages and forwards them to the appropriate remote.
func (tcp *TCP) handleSendRequests() {
	for {
		select {
		case req := <-tcp.sendRequests:
			tcp.logger.Info("TCP received request to send message to ", req.addr)
			tcp.remotes.SendToRemote(req)
		case <-tcp.closeChan:
			tcp.logger.Warn("TCP's state handler is closing.")
			tcp.remotes.Close()
			return
		}
	}
}

// Send a message to the given destination.
func (tcp *TCP) Send(dest address, payload []byte) error {
	if dest == tcp.local {
		panic("Cannot send message to self")
	}
	errChan := make(chan error)
	tcp.sendRequests <- sendRequest{addr: dest, payload: payload, err: errChan}

	return <-errChan
}

// RegisterHandler registers a new handler for messages of a given type.
func (tcp *TCP) RegisterHandler(handler msgHandler) transport.HandlerID {
	nextUID := handlerID(<-tcp.uidGenerator)
	tcp.registrations <- registration{
		id:      nextUID,
		handler: handler,
	}
	return nextUID
}

// UnregisterHandler unregisters a handler.
func (tcp *TCP) UnregisterHandler(id handlerID) {
	tcp.unregistrations <- id
}

// Close the TCP connection.
func (tcp *TCP) Close() {
	tcp.logger.Warnf("Closing TCP")
	close(tcp.closeChan)
}
