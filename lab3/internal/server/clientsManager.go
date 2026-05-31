package server

import (
	"chatsapp/internal/common"
	"chatsapp/internal/election"
	"chatsapp/internal/logging"
	"chatsapp/internal/messages"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils"
)

/*
ClientsManager is responsible for managing remote clients connected to this client. This includes:
- Accepting and redirecting new connections
- Handling client disconnections
- Receiving messages from clients
- Broadcasting messages to all clients
*/
type ClientsManager interface {
	// Broadcast broadcasts a message to all connected clients
	Broadcast(msg common.ClientChatMessage)
	// ReceiveMessage returns the next message received from a client. Blocks until a message is received. If the manager is closed, returns a [ClientsManagerClosedError].
	ReceiveMessage() (common.ClientChatMessage, error)
}

// ClientsManagerClosedError is returned by [ClientsManager.ReceiveMessage] when the manager is closed.
type ClientsManagerClosedError struct{}

func (ClientsManagerClosedError) Error() string {
	return "Clients manager closed"
}

// Represents a connection to a single client, i.e. the address of the client and the username of user on whose behalf the client is communicating.
type clientConnection struct {
	source transport.Address
	user   common.Username
}

// Represents a message received from a client.
type incomingMessage struct {
	messages.Message
	source transport.Address
}

// Implements the [ClientsManager] interface.
type clientsManager struct {
	logger *logging.Logger

	self transport.Address

	elector election.Elector
	network transport.NetworkInterface

	incomingMessages *utils.BufferedChan[incomingMessage]

	broadcastReqs chan common.ClientChatMessage

	validatedMsgs *utils.BufferedChan[common.ClientChatMessage]
}

/*
NewClientsManager constructs and returns a new [ClientsManager].
  - logger: The logger to use for logging messages.
  - self: The address of the current client.
  - elector: The elector to use for leader election.
  - network: The network interface to use for sending and receiving messages to/from the clients.

Note that communication with the clients does not go through a dispatcher, but through teh network interface directly, for simplicity.
*/
func NewClientsManager(logger *logging.Logger, self transport.Address, elector election.Elector, net transport.NetworkInterface) ClientsManager {
	m := clientsManager{
		logger:           logger,
		self:             self,
		elector:          elector,
		network:          net,
		incomingMessages: utils.NewBufferedChan[incomingMessage](),
		broadcastReqs:    make(chan common.ClientChatMessage),
		validatedMsgs:    utils.NewBufferedChan[common.ClientChatMessage](),
	}

	common.RegisterAllToGob()

	net.RegisterHandler(&m)

	go m.handleClients()

	return &m
}

func (m *clientsManager) HandleNetworkMessage(msg *transport.Message) (wasHandled bool) {
	addr := msg.Source
	payload := msg.Payload

	m.logger.Infof("Received message from %s.", addr)

	var err error
	decoded, err := common.DecodeMessage(payload)

	if err != nil {
		m.logger.Errorf("Error decoding message from %s: %s", addr, err)
		return false
	}

	switch decoded := decoded.(type) {
	case common.ConnRequestMessage:
	case common.ConnResponseMessage:
	case common.ClientChatMessage:
	case common.ConnClose:
	default:
		m.logger.Infof("Received message of unknown type from %s: %s", addr, decoded)
		return false
	}

	m.incomingMessages.Inlet() <- incomingMessage{
		Message: decoded,
		source:  addr,
	}

	return true
}

func (m *clientsManager) handleDispatchedMessage(msg messages.Message, source transport.Address) {
	m.incomingMessages.Inlet() <- incomingMessage{
		Message: msg,
		source:  source,
	}
}

func (m *clientsManager) Broadcast(msg common.ClientChatMessage) {
	m.logger.Infof("Sending message to all clients: %s", msg)
	m.broadcastReqs <- msg
}

func (m *clientsManager) ReceiveMessage() (common.ClientChatMessage, error) {
	msg, ok := <-m.validatedMsgs.Outlet()
	if !ok {
		return common.ClientChatMessage{}, ClientsManagerClosedError{}
	}
	return msg, nil
}

// Main goroutine that handles the clients.
//
// The clientsManager maintains a map of all connected clients. Because clients may connect and disconnect dynamically over time, this is state that must be handled by a single goroutine to avoid concurrent access. This is that goroutine.
//
// It listens for incoming messages from clients.
//   - If a client connection request, it responds with the current leader and adds the client to the map if it itself the leader,
//   - If a chat message, it ensures it comes from the expected user and forwards it to be received by the [clientsManager.ReceiveMessage] method,
//   - If a connection close message, it removes the client from the map.
//
// It also listens for requests from the server to broadcast a message to all clients.
func (m *clientsManager) handleClients() {
	clients := make(map[transport.Address]clientConnection)

	for {
		select {
		case incomingMsg := <-m.incomingMessages.Outlet():
			msg := incomingMsg.Message
			source := incomingMsg.source

			switch clientMsg := msg.(type) {
			case common.ConnRequestMessage:
				m.logger.Infof("Received conn req from %s (%s)", source, clientMsg.User)

				if _, ok := clients[source]; ok {
					m.logger.Warn("Client already connected: ", source)
					continue
				}

				leader := m.elector.GetLeader()

				m.send(common.ConnResponseMessage{
					Leader: leader,
				}, source)

				if leader == m.self {
					clients[source] = clientConnection{
						source: source,
						user:   clientMsg.User,
					}
					m.elector.UpdateAbility(-len(clients))
					m.logger.Infof("Client %s (%s) connected", source, clientMsg.User)
				} else {
					m.logger.Infof("Redirecting client %s to leader %s", source, leader)
				}

			case common.ClientChatMessage:
				if client, ok := clients[source]; !ok {
					m.logger.Warnf("Received chat msg from unconnected client %s. Ignoring", source)
					continue
				} else if client.user != clientMsg.User {
					m.logger.Warnf("Received chat msg from %s with username %s while expecting %s. Ignoring", source, clientMsg.User, client.user)
					continue
				}
				m.logger.Infof("Received msg from %s: %s", source, clientMsg)
				m.validatedMsgs.Inlet() <- clientMsg
			case common.ConnClose:
				if _, ok := clients[source]; !ok {
					m.logger.Warn("Received close request from unconnected client; ignoring: ", source)
					continue
				}
				m.logger.Info("Client disconnected: ", source)
				delete(clients, source)
				m.elector.UpdateAbility(-len(clients))
			}
		case msg := <-m.broadcastReqs:
			for addr, client := range clients {
				if client.user != msg.User {
					m.send(msg, addr)
				}
			}
		}
	}
}

func (m *clientsManager) send(msg messages.Message, dest transport.Address) {
	bytes, err := common.EncodeMessage(msg)
	if err != nil {
		m.logger.Errorf("Error encoding message: %s", err)
		return
	}
	m.network.Send(dest, bytes)
}
