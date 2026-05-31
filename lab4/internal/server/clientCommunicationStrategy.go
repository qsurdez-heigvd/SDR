package server

import (
	"chatsapp/internal/common"
	"chatsapp/internal/election"
	"chatsapp/internal/logging"
)

/*
A Client Communication Strategy is responsible for managing the communication between the server and the clients. Its existence allows for different implementations of the client communication strategy to be swapped out easily, depending on the requirements of the server.

Two implementations are provided:
  - [LocalClientCommStrategy] implements the behavior of a client that is running on the same machine as the server.
  - [RemoteClientCommStrategy] implements the behavior of clients that connect to the server over the network, using the [ClientManager] interface to manage the clients.
*/
type clientCommStrategy interface {
	// Initialize the client communication strategy with the server instance. Must be called before any other methods are called.
	Initialize(server *Server)
	// ClientsManager inherits from the [ClientManager] interface, i.e. the
	// [Broadcast] and [ReceiveMessage] methods.
	ClientsManager
}

// LOCAL CLIENT COMM STRATEGY

// Imlements a local client communication strategy, where the client is running on the same machine as the server and there is no network communication involved.
type localClientCommStrategy struct {
	userInputs chan string
	server     *Server
	user       common.Username
}

// Constructs and returns a new local client communication strategy instance.
func newLocalClientCommStrategy(user common.Username) *localClientCommStrategy {
	return &localClientCommStrategy{
		userInputs: make(chan string),
		user:       user,
	}
}

func (l *localClientCommStrategy) Initialize(server *Server) {
	l.server = server
	go l.readUserInputs()
}

func (l *localClientCommStrategy) Broadcast(msg common.ClientChatMessage) {
	server := l.server
	if msg.User != l.user {
		server.ioStream.Println(msg.User, ": ", msg.Content)
	}
}

// Main goroutine that reads user inputs from the input stream and sends them to the server.
func (l *localClientCommStrategy) readUserInputs() {
	server := l.server
	for {
		l.server.logger.Info("Waiting for user input")
		nextLine, err := l.server.ioStream.ReadLine()
		if err != nil {
			server.logger.Error("Failed to read user input:", err)
			return
		}
		l.userInputs <- nextLine
		select {
		case <-server.closeNotifier:
			return
		default:
		}
	}
}

func (l *localClientCommStrategy) ReceiveMessage() (common.ClientChatMessage, error) {
	server := l.server
	select {
	case userInput := <-l.userInputs:
		return common.ClientChatMessage{User: l.user, Content: userInput}, nil
	case <-server.closeNotifier:
		return common.ClientChatMessage{}, ClientsManagerClosedError{}
	}
}

// REMOTE CLIENT COMM STRATEGY

// Implements a remote client communication strategy, where the clients connect to the server over the network. This essentually wraps the [ClientManager] interface.
type remoteClientCommStrategy struct {
	clients ClientsManager
}

// Constructs and returns a new remote client communication strategy instance.
func newRemoteClientCommStrategy() *remoteClientCommStrategy {
	return &remoteClientCommStrategy{}
}

func (r *remoteClientCommStrategy) Initialize(s *Server) {
	log := s.logger.WithLogLevel(logging.WARN)
	elector := election.NewCRElector(
		log.WithPostfix("elector"),
		s.self,
		s.dispatcher,
		s.ring,
	)

	r.clients = NewClientsManager(
		log.WithPostfix("clients"),
		s.self,
		elector,
		s.network,
	)
}

func (r *remoteClientCommStrategy) Broadcast(msg common.ClientChatMessage) {
	r.clients.Broadcast(msg)
}

func (r *remoteClientCommStrategy) ReceiveMessage() (common.ClientChatMessage, error) {
	return r.clients.ReceiveMessage()
}
