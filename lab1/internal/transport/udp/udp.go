/*
@author: Quentin Surdez
@date: 15/10/2025
@description: This file is an UDP module that implements the logic to use an RR module
that enforces the Request-Reply protocol. The UDP logic is kept untouched as well the API.
*/

package udp

import (
	"bytes"
	"chatsapp/internal/logging"
	"chatsapp/internal/transport"
	"chatsapp/internal/transport/rr"
	"chatsapp/internal/utils"
	"encoding/gob"
	"io"
	"net"
	"time"
)

// Payload associated to the address to which it is destined.
type sendRequest struct {
	dest    transport.Address
	payload []byte
	result  chan error
}

// Internal representation of a handler registration.
type registration struct {
	id      transport.HandlerID
	handler transport.MessageHandler
}

// RRWrapper wraps the channels for communication with an RR instance.
type RRWrapper struct {
	outgoing chan []byte // RR writes here, we read and send via UDP
	incoming chan []byte // We write here, RR reads
}

// UDP implements the [NetworkInterface] interface for UDP connections.
type UDP struct {
	logger       *logging.Logger
	uidGenerator utils.UIDGenerator

	local transport.Address

	sendRequests     chan sendRequest
	networkMessages  *utils.BufferedChan[transport.Message] // Raw messages from network, need routing to RR
	receivedMessages *utils.BufferedChan[transport.Message] // Application messages from RR, need dispatch to handlers

	registrations   chan registration
	unregistrations chan transport.HandlerID

	// This brings a race condition ... We can improve it further but not today
	rrMap        map[transport.Address]rr.RR
	rrWrapperMap map[transport.Address]RRWrapper

	closeChan chan struct{}
}

// NewUDP creates a new [UDP] instance with the given local address and logger.
func NewUDP(local transport.Address, log *logging.Logger) transport.NetworkInterface {
	udp := UDP{
		logger:       log,
		uidGenerator: utils.NewUIDGenerator(),
		local:        local,

		sendRequests:     make(chan sendRequest),
		networkMessages:  utils.NewBufferedChan[transport.Message](),
		receivedMessages: utils.NewBufferedChan[transport.Message](),

		registrations:   make(chan registration),
		unregistrations: make(chan transport.HandlerID),

		rrMap:        make(map[transport.Address]rr.RR),
		rrWrapperMap: make(map[transport.Address]RRWrapper),

		closeChan: make(chan struct{}),
	}

	go udp.listenIncomingMessages()
	go udp.handleState()

	return &udp
}

/*
Main goroutine for handling the state of the UDP connection.

The state is anything that may change dynamically, i.e. the set of registered handlers, and the set of send-channels for each known remote. In order to prevent concurrent access to this state, it must be handled by a single goroutine and all accesses or modifications must be done as instructions to this goroutine, passed through channels.

It handles the following events:
  - On send requests, launches `handleSend()` goroutine for non-blocking processing.
  - On network messages: creates RR instances for new remotes and routes payloads to their incoming channels via goroutines.
  - On application messages from RR instances: broadcasts to all registered handlers.
  - On handler (un)registration: updates the set of registered handlers.
  - On close: closes all channels and returns.
*/
func (udp *UDP) handleState() {
	registeredHandlers := make(map[transport.HandlerID]transport.MessageHandler)

	for {
		select {
		case msg := <-udp.sendRequests:
			go udp.handleSend(msg)

		case msg := <-udp.networkMessages.Outlet():
			if _, exists := udp.rrWrapperMap[msg.Source]; !exists {
				// Create RR instance if remote initiated contact
				udp.initRRInstance(msg.Source)
			}
			// Get incoming channel from rrWrapper so that the write is not concurrent
			incomingChan := udp.rrWrapperMap[msg.Source].incoming
			go func() {
				select {
				case incomingChan <- msg.Payload:
				case <-udp.closeChan:
					return
				}
			}()

		case msg := <-udp.receivedMessages.Outlet():
			// Dispatch application message to registered handlers
			udp.logger.Info("UDP received message from", msg.Source, ". Dispatching it among", len(registeredHandlers), "handlers.")
			for _, handler := range registeredHandlers {
				if handler.HandleNetworkMessage(&msg) {
					break
				}
			}

		case registration := <-udp.registrations:
			udp.logger.Info("UDP registering handler", registration.id)
			registeredHandlers[registration.id] = registration.handler

		case id := <-udp.unregistrations:
			udp.logger.Info("UDP unregistering handler", id)
			delete(registeredHandlers, id)

		case <-udp.closeChan:
			udp.logger.Warn("UDP's state-handler is closing.")
			return
		}
	}
}

// handleSend sends a message using the RR protocol for reliability.
//
// For each send request:
// - Creates RR instance if it doesn't exist for the destination
// - Sends payload via RR protocol and waits for acknowledgment
// - Handles timeout and close signals gracefully
// - Returns result to caller via the result channel
//
// Note: Timeout is not treated as a hard error since the message may still be delivered.
// The result channel is always closed after sending any error.
func (udp *UDP) handleSend(msg sendRequest) {
	// Create RR instance if doesn't exist
	if _, exists := udp.rrMap[msg.dest]; !exists {
		udp.initRRInstance(msg.dest)
	}

	// Send via RR protocol
	responseChan := udp.rrMap[msg.dest].SendRequest(msg.payload)

	// Wait for RR response with timeout
	var err error
	select {
	case response := <-responseChan:
		if response.Err != nil {
			udp.logger.Error("Error from RR SendRequest:", response.Err)
			err = response.Err
		} else {
			udp.logger.Info("UDP message acknowledged by", msg.dest)
		}
	case <-time.After(1500 * time.Millisecond):
		udp.logger.Warn("UDP SendRequest timed out for", msg.dest)
	case <-udp.closeChan:
		udp.logger.Warn("UDP closing while sending to", msg.dest)
		err = io.EOF
	}

	// Send result back to caller
	if err != nil {
		msg.result <- err
	}
	close(msg.result)
}

// initRRInstance creates a new RR instance for the given remote address.
//
// For each destination:
// - Creates RRWrapper with buffered incoming/outgoing channels
// - Starts send handling goroutine for the destination
// - Initializes RR protocol instance with network wrapper
// - Sets request handler to deliver incoming messages to application
//
// Note: Request handler returns "OK" acknowledgment for all received messages.
// Messages are delivered to application via goroutine to avoid blocking RR.
func (udp *UDP) initRRInstance(dest transport.Address) {
	if _, exists := udp.rrMap[dest]; exists {
		return
	}

	udp.logger.Info("Creating RR instance for", dest)

	// Create bidirectional wrapper
	wrapper := RRWrapper{
		outgoing: make(chan []byte, 10),
		incoming: make(chan []byte, 10),
	}
	udp.rrWrapperMap[dest] = wrapper

	// Start goroutine to handle sends for this RR instance
	udp.startHandlingSends(dest, wrapper.outgoing)

	// Create RR instance
	udp.rrMap[dest] = rr.NewRR(udp.logger, dest, rr.NetWrapper{
		IntoNet: wrapper.outgoing,
		FromNet: wrapper.incoming,
	})

	// Set request handler: when RR receives a request, deliver to application
	udp.rrMap[dest].SetRequestHandler(func(payload []byte) []byte {
		msg := transport.Message{
			Source:  dest,
			Payload: payload,
		}
		go func() {
			select {
			case udp.receivedMessages.Inlet() <- msg:
				udp.logger.Info("Application message delivered from", dest)
			case <-udp.closeChan:
				udp.logger.Warn("Cannot deliver message, UDP closing")
			}
		}()
		return []byte("OK") // Simple ACK
	})
}

// startHandlingSends launches a goroutine that sends RR-encoded messages via UDP.
//
// For each destination:
// - Establishes UDP connection to remote address
// - Listens on outgoing channel for RR payloads to send
// - Wraps payloads in transport messages with local source address
// - Handles graceful shutdown via close channel and channel closure
//
// The goroutine runs until either closeChan signals or outgoing channel closes.
func (udp *UDP) startHandlingSends(dest transport.Address, outgoing <-chan []byte) {
	udp.logger.Info("Starting send handler for", dest)

	go func() {
		conn, err := net.Dial("udp", dest.String())
		if err != nil {
			udp.logger.Error("Error dialing UDP connection:", err)
			return
		}
		defer conn.Close()

		for {
			select {
			case <-udp.closeChan:
				udp.logger.Warn("Send handler for", dest, "closing")
				return

			case rrPayload, ok := <-outgoing:
				if !ok {
					udp.logger.Warn("Outgoing channel closed for", dest)
					return
				}

				select {
				case <-udp.closeChan:
					return
				default:
				}

				// Wrap RR payload in transport.Message to include source
				message := transport.Message{
					Source:  udp.local,
					Payload: rrPayload,
				}

				udp.logger.Info("UDP sending RR message to", dest)
				if err := udp.writeToConn(message, conn); err != nil {
					udp.logger.Error("Error writing to UDP connection:", err)
				}
			}
		}
	}()
}

// Send a message to the given destination.
func (udp *UDP) Send(dest transport.Address, payload []byte) error {
	resultChan := make(chan error)
	udp.sendRequests <- sendRequest{
		dest:    dest,
		payload: payload,
		result:  resultChan,
	}
	err, _ := <-resultChan
	return err
}

// RegisterHandler registers a handler for messages of a given type.
func (udp *UDP) RegisterHandler(handler transport.MessageHandler) transport.HandlerID {
	nextUID := transport.HandlerID(<-udp.uidGenerator)
	udp.registrations <- registration{
		id:      nextUID,
		handler: handler,
	}
	return nextUID
}

// UnregisterHandler unregisters a handler for messages of a given type.
func (udp *UDP) UnregisterHandler(id transport.HandlerID) {
	udp.unregistrations <- id
}

// Main goroutine for handling incoming messages. Messages received from the network are decoded and routes them.
//
// For incoming network traffic:
// - Listens on UDP socket and decodes messages
// - Routes received messages to networkMessages channel for RR instance processing
// - RR instances in handleState() will process these via their incoming channels
func (udp *UDP) listenIncomingMessages() error {
	udpAddr, err := net.ResolveUDPAddr("udp", udp.local.String())
	if err != nil {
		return err
	}

	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return err
	}
	defer udpConn.Close()

	udp.logger.Info("Listening for messages on", udp.local)

	receivedChan := make(chan transport.Message, 10)
	go func() {
		buf := make([]byte, max_message_size)
		for {
			receivedMessage, err := udp.readFromConn(udpConn, buf)
			if err != nil {
				select {
				case <-udp.closeChan:
					udp.logger.Warn("UDP receive handler closed")
					return
				default:
					udp.logger.Error("Error decoding message:", err)
					continue
				}
			}
			receivedChan <- receivedMessage
		}
	}()

	for {
		select {
		case <-udp.closeChan:
			udp.logger.Warn("UDP receive dispatcher closing")
			return nil
		case receivedMessage := <-receivedChan:
			// Route to RR instance via main goroutine
			select {
			case udp.networkMessages.Inlet() <- receivedMessage:
			case <-udp.closeChan:
				return nil
			}
		}
	}
}

// Close the UDP connection and all RR instances.
func (udp *UDP) Close() {

	close(udp.closeChan)

	// Close all RR instances
	for dest, rrInst := range udp.rrMap {
		udp.logger.Info("Closing RR instance for", dest)
		rrInst.Close()
	}

	// Close all wrapper channels
	for dest, wrapper := range udp.rrWrapperMap {
		udp.logger.Info("Closing wrapper channels for", dest)
		close(wrapper.outgoing)
		close(wrapper.incoming)
	}
}

const max_message_size = 65000 // UDP max size is 65507 bytes

func (udp *UDP) writeToConn(m transport.Message, conn net.Conn) error {
	// Encode message using gob
	var msgBytes bytes.Buffer
	encoder := gob.NewEncoder(&msgBytes)
	if err := encoder.Encode(m); err != nil {
		return err
	}

	if msgBytes.Len() > max_message_size {
		return io.ErrShortBuffer
	}

	// Send to connection
	udp.logger.Infof("UDP writing %d bytes to connection", msgBytes.Len())
	_, err := conn.Write(msgBytes.Bytes())
	return err
}

func (udp *UDP) readFromConn(conn *net.UDPConn, buf []byte) (transport.Message, error) {
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		return transport.Message{}, err
	}
	udp.logger.Infof("UDP read %d bytes from conn", n)

	var m transport.Message
	bufReader := bytes.NewBuffer(buf[:n])
	decoder := gob.NewDecoder(bufReader)
	err = decoder.Decode(&m)
	if err != nil {
		if err == io.EOF {
			udp.logger.Warn("UDP reached EOF while decoding message")
		} else {
			udp.logger.Error("UDP error decoding message:", err)
		}
		return transport.Message{}, err
	}
	return m, nil
}
