package tcp

import (
	"chatsapp/internal/logging"
	"fmt"
	"net"
	"time"
)

/*
A remotes handler is responsible for managing remote connections. A separate structure is helpful for this because the management is relatively involved, as it needs to satisfy the following requirements:
  - An unknown remote should be able to request to this process at any time
  - Two processes may attempt to establish a connection to each other concurrently.
  - The user of the TCP network interface should be oblivious of the connection establishment process, and be able to request sending a message to a remote at any time. If that remote is not yet connected, a connection should be established and the message sent on it.

The difficulty in implementing a solution for this arises in the following scenario. If two processes attempt to establish a connection to each other concurrently, either both are accepted, or some logic must exist to keep only one. While keeping both would be functionnally correct, it would be a waste of resources.

In order to keep only one, we enforce that only connections established by a server with lower address to a server with higher address be accepted and used for communication. This allows arbitration in the case of concurrent connection attempts. However, this prevents a higher-address server from establishing a connection to a lower-address server. To solve this, we use the following protocol:
  - If a lower address server wishes to connect to a higher address server, it does so and expects the connection to be accepted.
  - If a server with higher address wishes to connect to a lower address server, it still attempts to establish a connection, but expects it to be closed immediately by the other server. This will however indicate to the other server that a connection is desired. The other server will therefore establish a connection itself, which will this time be valid.

An additional difficulty is that when a connection is accepted, the remote's address provided by the connection will not be the one on which the remote is listening, even though it is the one that we use to identify that remote. This is why [message] objects contain the source address, as it may differ from the actual source of the message at the TCP layer.

The remotes handler is the structure that implements and abstracts away this logic. Its interface is reduced to the following unique method:
  - [GetRemote]: Given an address, returns a [remote] object that can be used to send messages to that address. Behind the scenes, this method may trigger the connection establishement protocol described above, without blocking. However, attemps to send to it will block until the connection is established.

Any message received from a remote is sent to the [receivedMessages] channel that is passed to the constructor.

The user of the [remotesHandler] can thus use the [GetRemote] method to obtain a [remote] object, and use its [sendChan] channel to send messages to that remote. The [remotesHandler] will take care of the rest.

	// Create a remote handler
	receivedMessages := make(chan Message)
	rh := newRemoteHandler(localAddr, log, receivedMessages)

	// Obtain a remote to send a message to. This may trigger the connection establishment protocol.
	remote := rh.GetRemote(remoteAddr)
	remote.sendChan <- []byte("Hello, remote!")

	// Get the next message received from any remote
	msg := <-receivedMessages
*/
type remotesHandler struct {
	logger *logging.Logger
	local  address

	// In-events
	sendReqs       chan sendRequest
	newConnNotifs  chan addressedCodec
	newConnRequest chan address
	closeReqs      chan struct{}
	// Used to guarantee that address is no longer bound after close
	listenerIsClosed chan struct{}

	// Out-events
	receivedMessages chan message
}

// Whether a connection as established by source to dest is valid and should be kept.
func isConnectionValid(source, dest address) bool {
	return source.LessThan(dest)
}

// Constructs and returns a new [remotesHandler] instance.
//   - local: The address of the local server.
//   - log: The logger to use for logging messages.
//   - receivedMessages: The channel to which received messages should be sent.
func newRemoteHandler(local address, log *logging.Logger, receivedMessages chan message) *remotesHandler {
	h := remotesHandler{
		logger:           log,
		local:            local,
		sendReqs:         make(chan sendRequest),
		newConnNotifs:    make(chan addressedCodec),
		newConnRequest:   make(chan address),
		closeReqs:        make(chan struct{}),
		listenerIsClosed: make(chan struct{}),
		receivedMessages: receivedMessages,
	}

	go h.handleRemotes()
	go h.listenForConns()

	return &h
}

// SendToRemote returns a remote object that can be used to send messages to the given address. Note that this may trigger the connection establishment protocol, though it will not block.
func (h *remotesHandler) SendToRemote(req sendRequest) {
	h.sendReqs <- req
}

// Close the remotes handler, stopping all connections and listeners.
func (h *remotesHandler) Close() {
	h.logger.Warn("Closing remotes handler.")
	close(h.closeReqs)
	<-h.listenerIsClosed
}

/*
Attempts to establish a connection to the given remote.

This function establishes a connection to the given remote and
  - either that connection is valid and it is notified as such through [newConnNotifs]
  - or it is invalid and this function returns, expecting the remote to establish a connection back.

This function is thus blocking until the first connection is established, and closed if invalid.
*/
func (h *remotesHandler) tryConnect(addr address, newCodec chan<- connCodec) {
	var conn net.Conn
	var err error

	for {
		h.logger.Info("Dialing TCP connection to ", addr)
		conn, err = net.Dial("tcp", addr.String())
		if err != nil {
			h.logger.Warn("Error dialing TCP connection to ", addr, " . Retrying in a bit. ", err)
			time.Sleep(1 * time.Second)
			continue
		}

		break // Connection established, exit loop
	}

	h.logger.Info("Connection established to ", addr, ". Sending greeting...")

	// Send source with greeting
	msg := message{Source: h.local, Payload: []byte("greeting")}
	codec := newConnCodec(conn)

	err = codec.SendMessage(msg)
	if err != nil {
		h.logger.Error("Error encoding greeting message in transport:", err)
	}

	// Check that connection is valid
	isValid := isConnectionValid(h.local, addr)
	if isValid {
		h.logger.Info("Connection to ", addr, " is valid. Handling remote.")
		connectedRemote := codec.WithAddress(addr)
		newCodec <- connectedRemote.connCodec
	} else {
		h.logger.Info("Connection to ", addr, " is invalid. Waiting for connection to close.")
		codec.Receive()
		h.logger.Info("Connection to ", addr, " closed. Will let them connect to us")
	}
}

/*
Main goroutine for handling the remotes and their associated connections. It listens for the following events:
  - [GetRemote] was called; either the connection is known and returned, or [tryConnect] is called to establish a new connection.
  - A new connection was established; it is stored and handled through [handleRemote]
  - A request to establish a connection was made; this happens when an invalid connection is received and we are expected to establish a connection back.
  - A request to close the remotes handler was made; all connections are closed and the goroutine returns. The behavior is essentially the same as for a [GetRemote] call.
*/
func (h *remotesHandler) handleRemotes() {
	//remotes := make(map[Address]*remote)
	conns := make(map[address]*remoteConn)

	closedConn := make(chan address)

	// Helper function to either get an existing remote or call [tryConnect] to create a new one.
	getConn := func(addr address) *remoteConn {
		if conn, ok := conns[addr]; ok {
			h.logger.Info("Requested remote found for ", addr)
			return conn
		} else {
			h.logger.Info("Requested remote not found for ", addr, ". Creating new remote.")
			conn := newRemoteConn(h.logger.WithPostfix(addr.String()), h.local, addr, h.receivedMessages, closedConn)
			conns[addr] = conn

			go h.tryConnect(addr, conn.newCodec)

			return conn
		}
	}

	for {
		select {
		case req := <-h.sendReqs:
			h.logger.Info("Received request to send message to ", req.addr)
			r := getConn(req.addr)
			r.toRemote.Inlet() <- req
			// Might happen that the connection got closed between getConn and send. Message will be lost in that case. (higher level should handle this if needed, e.g. through RR)
		case c := <-h.newConnNotifs:
			if _, ok := conns[c.remote]; ok {
				h.logger.Error("Received connection to neighbor I already had a connection to... Overwriting existing connection to try save the day...?")
			} else {
				h.logger.Info("Received connection for remote I am not handling yet. Creating new remote.")
				conns[c.remote] = newRemoteConn(h.logger.WithPostfix(c.remote.String()), h.local, c.remote, h.receivedMessages, closedConn)
			}

			conn := conns[c.remote]

			h.logger.Info("Received connection for ", c.remote, " . Storing it.")
			conn.newCodec <- c.connCodec

		case addr := <-closedConn:
			if conn, ok := conns[addr]; ok {
				h.logger.Info("Received close notification for ", addr, ". Removing all associated information.")
				// Notify the connection that it should close.
				conn.toRemote.Close()
				delete(conns, addr)
			} else {
				h.logger.Error("Received close notification for unknown connection. Ignoring.")
			}
		case addr := <-h.newConnRequest:
			getConn(addr)
		case <-h.closeReqs:
			h.logger.Warn("TCP's remotes handler is closing. Closing all connections.")
			for _, conn := range conns {
				h.logger.Warnf("Closing remote to %s", conn.remoteAddr)
				conn.toRemote.Close()
				close(conn.newCodec)
			}
			return
		}
	}
}

// Main goroutine for listening for new connections. It listens for incoming connections and forwards them to the remotes handling goroutine.
func (h *remotesHandler) listenForConns() {
	h.logger.Info("Listening for connections on ", h.local.String())
	listener, err := net.Listen("tcp", h.local.String())
	if err != nil {
		panic(fmt.Sprintf("Error listening in transport: %s", err))
	}
	defer func() {
		listener.Close()
		close(h.listenerIsClosed)
	}()

	newConnChan := make(chan net.Conn)
	go func() {
		for {
			h.logger.Info("Listening for new connections...")
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-h.closeReqs:
					h.logger.Warn("TCP's new conn listener is closing.")
					return
				default:
					h.logger.Error("Error accepting connection in transport:", err)
					continue
				}
			}
			h.logger.Info("New connection accepted, forwarding it to the new-connection-handler...")
			newConnChan <- conn
		}
	}()

	for {
		select {
		case <-h.closeReqs:
			h.logger.Warn("TCP's new conn handler is closing.")
			return
		case conn := <-newConnChan:
			go func() {
				h.logger.Info("Received connection, waiting for greeting...")
				codec := newConnCodec(conn)

				msg, err := codec.Receive()
				if err != nil {
					h.logger.Error("Error decoding greeting message in transport:", err)
				}
				h.logger.Info("Greeting received, it was from ", msg.Source)

				if !isConnectionValid(msg.Source, h.local) {
					h.logger.Info("Connection is invalid, closing it and requesting new connection from us.")
					codec.conn.Close()
					h.newConnRequest <- msg.Source
					return
				} else {
					h.logger.Info("Connection is valid, accepting it.")
					h.newConnNotifs <- codec.WithAddress(msg.Source)
				}
				h.logger.Info("New connection to ", msg.Source, " handled; listening for more.")
			}()
		}
	}
}
