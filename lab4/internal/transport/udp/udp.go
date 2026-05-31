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
)

type responsePromise = <-chan rr.SendResponse

// Payload associated to the address to which it is destined.
type sendRequest struct {
	dest            transport.Address
	payload         []byte
	responsePromise chan responsePromise
}

// Internal representation of a handler registration.
type registration struct {
	id      transport.HandlerID
	handler transport.MessageHandler
}

// UDP implements the [NetworkInterface] interface for UDP connections.
type UDP struct {
	logger       *logging.Logger
	uidGenerator utils.UIDGenerator

	local transport.Address

	sendRequests     chan sendRequest
	receivedMessages chan transport.Message

	fromNetwork chan transport.Message

	registrations   chan registration
	unregistrations chan transport.HandlerID

	closeChan chan struct{}
}

// NewUDP creates a new [UDP] instance with the given local address and logger.
func NewUDP(local transport.Address, log *logging.Logger) transport.NetworkInterface {
	udp := UDP{
		logger:       log,
		uidGenerator: utils.NewUIDGenerator(),
		local:        local,

		sendRequests:     make(chan sendRequest),
		receivedMessages: make(chan transport.Message),

		fromNetwork: make(chan transport.Message),

		registrations:   make(chan registration),
		unregistrations: make(chan transport.HandlerID),

		closeChan: make(chan struct{}),
	}

	go udp.listenIncomingMessages()
	go udp.handleState()

	return &udp
}

type remote struct {
	rr      rr.RR
	fromNet *utils.BufferedChan[[]byte]
}

/*
Main goroutine for handling the state of the UDP connection.

The state is anything that may change dynamically, i.e. the set of registered handlers, and the set of send-channels for each known remote. In order to prevent concurrent access to this state, it must be handled by a single goroutine and all accesses or modifications must be done as instructions to this goroutine, passed through channels.

It handles the following events:
  - On send requests made through [Send], forwards it to that remote's send-channel.
  - On handler (un)registration, updates the set of registered handlers.
  - On messages received from the network, dispatches it to all registered handlers.
  - On close, closes all send-channels and returns.
*/
func (udp *UDP) handleState() {
	registeredHandlers := make(map[transport.HandlerID]transport.MessageHandler)
	remotes := make(map[transport.Address]*remote)

	for {
		select {
		case msg := <-udp.sendRequests:
			udp.handleSend(msg, remotes)
		case msg := <-udp.fromNetwork:
			udp.handleFromNetwork(msg, remotes)
		case msg := <-udp.receivedMessages:
			udp.logger.Info("UDP received message from", msg.Source, ". Dispatching it among ", len(registeredHandlers), " handlers.")
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

			for _, remote := range remotes {
				remote.rr.Close()
				remote.fromNet.Close()
			}

			return
		}
	}
}

func (udp *UDP) getRemote(addr transport.Address, remotes map[transport.Address]*remote) *remote {
	if _, exists := remotes[addr]; !exists {
		toNet := make(chan []byte)
		fromNet := utils.NewBufferedChan[[]byte]()
		netWrapper := rr.NetWrapper{IntoNet: toNet, FromNet: fromNet.Outlet()}
		rrInst := rr.NewRR(udp.logger.WithPostfix("RR"+addr.String()), addr, netWrapper)

		rrInst.SetRequestHandler(func(payload []byte) []byte {
			udp.logger.Infof("UDP handling request from %s with payload of size %d", addr, len(payload))
			udp.receivedMessages <- transport.Message{
				Source:  addr,
				Payload: payload,
			}
			return nil
		})

		remotes[addr] = &remote{
			rr:      rrInst,
			fromNet: fromNet,
		}
		udp.startHandlingSends(addr, toNet)
	}
	return remotes[addr]
}

func (udp *UDP) handleFromNetwork(msg transport.Message, remotes map[transport.Address]*remote) {
	remote := udp.getRemote(msg.Source, remotes)
	remote.fromNet.Inlet() <- msg.Payload
}

// Handles a send request by pushing the payload to the appropriate send-channel. If none exist, it means that the remote is not yet known; it thus creates a new send-channel for that remote and starts handling it using [startHandlingSends].
func (udp *UDP) handleSend(msg sendRequest, remotes map[transport.Address]*remote) {
	remote := udp.getRemote(msg.dest, remotes)
	responseChan := remote.rr.SendRequest(msg.payload)
	msg.responsePromise <- responseChan
}

// Launches a goroutine that handles the send-channel for the given remote. It forwards any message received on that channel to the remote connection.
func (udp *UDP) startHandlingSends(dest transport.Address, sendRequests chan []byte) {
	udp.logger.Info("Starting to handle sends to", dest)

	handleSends := func() {
		conn, err := net.Dial("udp", dest.String())
		if err != nil {
			udp.logger.Error("Error dialing connection in transport:", err)
		}
		defer conn.Close()

		for payload := range sendRequests {
			udp.logger.Info("UDP starts sending message to", dest)
			message := transport.Message{Source: udp.local, Payload: payload}
			err = udp.writeToConn(message, conn)
			udp.logger.Info("UDP done sending message to", dest)
			if err != nil {
				udp.logger.Error("Error encoding message in transport:", err)
			}
		}

		udp.logger.Warn("UDP's send-request-handler closed due to closed chan")
	}

	go handleSends()
}

// Send a message to the given destination.
func (udp *UDP) Send(dest transport.Address, payload []byte) error {
	promiseChan := make(chan responsePromise)
	udp.sendRequests <- sendRequest{
		dest:            dest,
		payload:         payload,
		responsePromise: promiseChan,
	}
	promise := <-promiseChan
	response, ok := <-promise
	if !ok {
		udp.logger.Warnf("Response channel to %s was closed, returning nil error", dest)
		return nil
	}
	return response.Err
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

// Main goroutine for handling incoming messages. Messages received from the network are decoded and forwarded to the main goroutine that handles state.
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

	udp.logger.Info("Listening for messages on ", udp.local)

	receivedChan := make(chan transport.Message)
	go func() {
		buf := make([]byte, maxMessageSize)
		for {
			receivedMessage, err := udp.readFromConn(udpConn, buf)
			if err != nil {
				select {
				case <-udp.closeChan:
					udp.logger.Warn("UDP's receive-handler closed due to closed chan")
					return
				default:
					udp.logger.Error("Error decoding message in transport:", err)
					continue
				}
			}
			receivedChan <- receivedMessage
		}
	}()

	for {
		select {
		case <-udp.closeChan:
			udp.logger.Warn("UDP's receive-handler is closing.")
			return nil
		case receivedMessage := <-receivedChan:
			udp.fromNetwork <- receivedMessage
		}
	}
}

// Close the UDP connection.
func (udp *UDP) Close() {
	close(udp.closeChan)
}

const maxMessageSize = 65000 // UDP max size is 65507 bytes

func (udp *UDP) writeToConn(m transport.Message, conn net.Conn) error {
	// Encode message using gob
	var msgBytes bytes.Buffer
	encoder := gob.NewEncoder(&msgBytes)
	if err := encoder.Encode(m); err != nil {
		return err
	}

	if msgBytes.Len() > maxMessageSize {
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
