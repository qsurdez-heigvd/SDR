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
	ioStream     ioutils.IOStream
	logger       *logging.Logger
	self         transport.Address
	neighbors    []transport.Address
	user         common.Username
	printReadAck bool

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

	s := newServer(ioutils.NewStdStream(), log, config.Debug, config.Addr, config.User, config.Neighbors, config.PrintReadAck, networkInterface, config.SlowdownMs)
	s.ownsNetwork = true

	return s
}

/*
Constructs a new server instance from detailed parameters. This is intended to be used directly only by the tests.
  - ioStream: The input/output stream to use for user interaction.
  - log: The logger instance to use.
  - debug: Whether to run in debug mode.
  - selfAddr: The address of the server.
  - username: The username of the server's user.
  - neighbors: The addresses of the server's neighbors.
  - printReadAck: Whether to print read acknowledgements.
  - networkInterface: The network interface to use for communication.
  - slowdown: The amount of time to sleep after sending a message.
*/
func newServer(ioStream ioutils.IOStream, log *logging.Logger, debug bool, selfAddr transport.Address, username common.Username, neighbors []transport.Address, printReadAck bool, networkInterface transport.NetworkInterface, slowdown uint32) *Server {
	server := Server{
		ioStream:      ioStream,
		debug:         debug,
		logger:        log,
		self:          selfAddr,
		neighbors:     neighbors,
		network:       networkInterface,
		printReadAck:  printReadAck,
		slowdownMs:    slowdown,
		user:          username,
		closeNotifier: make(chan bool),
		ownsNetwork:   false,
	}

	server.dispatcher = dispatcher.NewDispatcher(log.WithPostfix("disp").WithLogLevel(logging.WARN), selfAddr, neighbors, networkInterface)
	server.dispatcher.Register(ChatMessage{}, server.handleDispatchedChatMessage)

	server.createMutex()

	// TODO listen incoming message and write them on the mutex + Request/release
	return &server
}

func (s *Server) createMutex() {
	netToMutex := make(chan mutex.Message)
	mutexToNet := make(chan mutex.OutgoingMessage)

	netWrapper := mutex.NetWrapper{
		IntoNet: mutexToNet,
		FromNet: netToMutex,
	}

	var selfPid = mutex.Pid(s.self.String())
	var neighbordsPid []mutex.Pid
	for _, addr := range s.neighbors {
		neighbordsPid = append(neighbordsPid, mutex.Pid(addr.String()))
	}

	s.mutex = mutex.NewLamportMutex(s.logger.WithPostfix("mutex"), netWrapper, selfPid, neighbordsPid)

	s.dispatcher.Register(mutex.Message{}, s.handleDispatchedMutexMessage(netToMutex))
	go func() {
		for {
			select {
			case msg := <-mutexToNet:
				s.logger.Infof("Sending mutex message to network: %v", msg)
				addr, err := transport.NewAddress(string(msg.Destination))
				if err != nil {
					s.logger.Errorf("Error creating address: %v", err)
					continue
				}
				s.dispatcher.Send(msg.Message, addr)
			case <-s.closeNotifier:
				s.logger.Warn("Mutex network closed due to closeNotifier")
				return
			}
		}
	}()
}

func (s *Server) handleDispatchedMutexMessage(fromNet chan mutex.Message) dispatcher.ProtocolHandler {
	return func(msg dispatcher.Message, source transport.Address) {
		mutexMsg, ok := msg.(mutex.Message)
		if !ok {
			s.logger.Warn("Unable to cast message to mutex")
		} else {
			fromNet <- mutexMsg
		}
	}
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

	s.ioStream.Println(fmt.Sprintf("%s: %s", chatMsg.User, chatMsg.Content))
}

// Start launches the server's main loop.
func (s *Server) Start() {
	s.logger.Info("Starting server")

	// Listening to user inputs
	clientMessages := make(chan string)
	go func() {
		for {
			nextLine, err := s.ioStream.ReadLine()
			if err != nil {
				s.logger.Error("Error receiving message from client:", err)
				return
			}
			if s.isClosed() {
				s.logger.Warn("Server is closed. Ignoring client input.")
				return
			}
			clientMessages <- nextLine
		}
	}()

	for {
		select {
		case msg := <-clientMessages:
			s.logger.Info("Received message from client:", msg)
			s.broadcast(s.user, msg)
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
	s.logger.Info("Broadcasting to neighbors:", s.neighbors)

	if s.isClosed() {
		s.logger.Warn("Server is closed. Ignoring broadcast")
		return
	}

	release, err := s.mutex.Request()
	if err != nil {
		s.logger.Warn("Error requesting mutex:", err)
		return
	}
	defer release()

	// broadcast the message
	message := ChatMessage{User: from, Content: text}

	for _, addr := range s.neighbors {
		if addr == s.self {
			continue
		}
		s.slowSelfDown()
		s.logger.Infof("Sending message to %s", addr)
		s.dispatcher.Send(message, addr)
		if s.printReadAck {
			s.ioStream.Println(s.constructMsgReceiptAckString(addr, text))
		}
		if s.isClosed() {
			s.logger.Warn("Broadcast stopped due to close request")
			return
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
