package server

import (
	"chatsapp/internal/common"
	"chatsapp/internal/logging"
	"chatsapp/internal/messages"
	"chatsapp/internal/mutex"
	"chatsapp/internal/server/dispatcher"
	"chatsapp/internal/transport"
	"chatsapp/internal/transport/udp"
	"chatsapp/internal/utils/ioutils"
	"fmt"
	"strconv"
	"time"
)

// Server represents a server in ChatsApp's distributed system.
type Server struct {
	ioStream        ioutils.IOStream
	logger          *logging.Logger
	self            transport.Address
	ring            []transport.Address
	directNeighbors []transport.Address
	clientComm      clientCommStrategy
	printReadAck    bool

	network    transport.NetworkInterface
	dispatcher dispatcher.Dispatcher
	mutex      mutex.Mutex

	closeNotifier chan bool

	debug       bool
	slowdownMs  uint32
	ownsNetwork bool
}

/*
NewServer constructs and returns a new server instance.
  - config: The configuration for the server.
*/
func NewServer(config *Config) *Server {
	logFile := logging.NewLogFile(fmt.Sprintf("%s/server-%s.log", config.LogPath, strconv.Itoa(int(config.Addr.Port))))

	ioStream := ioutils.NewStdStream()

	log := logging.NewLogger(ioStream, logFile, fmt.Sprintf("srv(%v<->..)", config.Addr.Port), !config.Debug)
	log.Info("Starting server ", config.Addr.Port)

	networkInterface := udp.NewUDP(config.Addr, log.WithPostfix("udp"))

	s := newServer(ioutils.NewStdStream(), log, config.Debug, config.Addr, config.ClientStrategy, config.Ring, config.DirectNeighbors, config.PrintReadAck, networkInterface, config.SlowdownMs)
	s.ownsNetwork = true

	return s
}

/*
Constructs a new server instance from detailed parameters. This is intended to be used directly only by the tests.
  - ioStream: The input/output stream to use for user interaction.
  - log: The logger instance to use.
  - debug: Whether to run in debug mode.
  - selfAddr: The address of the server.
  - clientCommStrat: The strategy to use for communication with clients.
  - neighbors: The addresses of the server's neighbors, ordered according to their order in the ring.
  - printReadAck: Whether to print read acknowledgements.
  - networkInterface: The network interface to use for communication.
  - slowdown: The amount of time to sleep after sending a message.
*/
func newServer(ioStream ioutils.IOStream, log *logging.Logger, debug bool, selfAddr transport.Address, clientCommStrat clientCommStrategy, ring []transport.Address, directNeighbors []transport.Address, printReadAck bool, networkInterface transport.NetworkInterface, slowdown uint32) *Server {
	server := Server{
		ioStream:        ioStream,
		debug:           debug,
		logger:          log,
		self:            selfAddr,
		ring:            ring,
		directNeighbors: directNeighbors,
		network:         networkInterface,
		printReadAck:    printReadAck,
		slowdownMs:      slowdown,
		closeNotifier:   make(chan bool),
		ownsNetwork:     false,
	}

	server.dispatcher = dispatcher.NewDispatcher(log.WithPostfix("disp").WithLogLevel(logging.WARN), selfAddr, directNeighbors, networkInterface)
	server.dispatcher.Register(ChatMessage{}, server.handleDispatchedChatMessage)

	server.initMutex()

	server.clientComm = clientCommStrat
	server.clientComm.Initialize(&server)

	return &server
}

// Initializes the server's mutex.
func (s *Server) initMutex() {
	mutexNetwork := newMutexNetworkWrapper(s.logger.WithPostfix("dmw").WithLogLevel(logging.WARN), s.dispatcher)

	selfPid := mutex.Pid(s.self.String())
	neighbors := make([]mutex.Pid, 0, len(s.ring))
	for _, neighbor := range s.ring {
		neighbors = append(neighbors, mutex.Pid(neighbor.String()))
	}

	s.mutex = mutex.NewLamportMutex(s.logger.WithPostfix("mtx").WithLogLevel(logging.WARN), mutexNetwork, selfPid, neighbors)
}

// Handles chat messages dispatched by the [dispatcher.Dispatcher] instance. Essentially only forwards them to clients.
func (s *Server) handleDispatchedChatMessage(msg messages.Message, from transport.Address) {
	s.slowSelfDown()

	chatMsg, ok := msg.(ChatMessage)
	if !ok {
		s.logger.Error("Received message of unknown type ; ingoring")
		return
	}

	s.logger.Info("Chat message from", from, ":", chatMsg.User, ":", chatMsg.Content)

	s.clientComm.Broadcast(common.ClientChatMessage{
		User:    chatMsg.User,
		Content: chatMsg.Content,
	})
}

// Start launches the server's main loop.
func (s *Server) Start() {
	s.logger.Info("Starting server")

	// Listening to user inputs
	clientMessages := make(chan common.ClientChatMessage)
	go func() {
		for {
			msg, err := s.clientComm.ReceiveMessage()
			if err != nil {
				s.logger.Error("Error receiving message from client:", err)
				return
			}
			if s.isClosed() {
				s.logger.Warn("Server is closed. Ignoring client input.")
				return
			}
			clientMessages <- msg
		}
	}()

	for {
		select {
		case msg := <-clientMessages:
			s.logger.Info("Received message from client:", msg)
			s.broadcast(msg.User, msg.Content)
		case <-s.closeNotifier:
			s.logger.Warn("Server stopped due to close request")
			return
		}
	}
}

// May be called to sleep for the configured duration. Used to simulate slow systems in tests.
func (s *Server) slowSelfDown() {
	if s.slowdownMs > 0 {
		time.Sleep(time.Duration(s.slowdownMs) * time.Millisecond)
	}
}

// Constructs the string intended for printing a message receipt acknowledgement.
func (s *Server) constructMsgReceiptAckString(from transport.Address, message string) string {
	return fmt.Sprintf("[%s received: %s]", from, message)
}

// Broadcasts a message to all neighbor servers.
func (s *Server) broadcast(from common.Username, text string) {
	s.logger.Info("Broadcasting to neighbors:", s.ring)

	if s.isClosed() {
		s.logger.Warn("Server is closed. Ignoring broadcast")
		return
	}

	// Request the mutex to ensure mutual exclusion and hence total-order.
	release, err := s.mutex.Request()
	if err != nil {
		s.logger.Error("Error requesting mutex:", err)
		return
	}
	defer func() {
		release()
	}()

	// Broacast to connected clients too, once the mutex is acquired.
	s.clientComm.Broadcast(common.ClientChatMessage{
		User:    from,
		Content: text,
	})

	// broadcast the message
	message := ChatMessage{User: from, Content: text}
	received := s.dispatcher.Broadcast(message)
	if s.printReadAck {
		for _, addr := range received {
			if addr != s.self {
				s.ioStream.Println(s.constructMsgReceiptAckString(addr, text))
			}
		}
	}
}

func (s *Server) isClosed() bool {
	select {
	case <-s.closeNotifier:
		return true
	default:
		return false
	}
}

// Close stops the server.
func (s *Server) Close() {
	s.logger.Info("Closing server")
	if s.isClosed() {
		s.logger.Warn("Server already closed")
		return
	}
	close(s.closeNotifier)

	s.dispatcher.Close()

	if s.ownsNetwork {
		// If server was created by the test, it doesn't own the network and shouldn't close it.
		s.network.Close()
	}

}
