package client

import (
	"chatsapp/internal/common"
	"chatsapp/internal/logging"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils/ioutils"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"syscall"
)

type username = common.Username
type address = transport.Address

// Client represents a client that connects to a server.
type Client interface {
	// Run launches the client's main loop.
	Run()
}

// Implements a Client interface.
//
// We hide this implementation behind a Client interface so that [client.HandleMessage] is not exposed to the user of the client.
type client struct {
	logger   *logging.Logger
	ioStream ioutils.IOStream

	network transport.NetworkInterface

	self   address
	server address

	user username

	connRespMsgs chan common.ConnResponseMessage
}

/*
NewClient constructs and returns a new client instance.

Parameters:
  - logger: The logger instance to use.
  - serverAddr: The address of the server to connect to.
  - selfAddr: The address of the client.
  - user: The username with which the client will communicate with the server.
  - network: The network interface to use for communication.
  - ioStream: The input/output stream to use for user interaction.

Returns:
  - A new client instance.
*/
func NewClient(logger *logging.Logger, serverAddr address, selfAddr address, user username, network transport.NetworkInterface, ioStream ioutils.IOStream) Client {
	c := client{
		logger:       logger,
		ioStream:     ioStream,
		network:      network,
		self:         selfAddr,
		server:       serverAddr,
		user:         user,
		connRespMsgs: make(chan common.ConnResponseMessage),
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	go func() {
		<-sig
		byts, _ := common.EncodeMessage(common.ConnClose{User: user})
		network.Send(serverAddr, byts)
		os.Exit(1)
	}()

	common.RegisterAllToGob()
	network.RegisterHandler(&c)
	return &c
}

// HandleNetworkMessage handles a message received from the network.
//
// Connection response messages are sent to the [connRespMsgs] channel, and chat messages are printed to the user.
func (c *client) HandleNetworkMessage(msg *transport.Message) (wasHandled bool) {
	wasHandled = true
	payload := msg.Payload
	decMsg, err := common.DecodeMessage(payload)
	if err != nil {
		c.ioStream.Println("ERROR: Failed to decode message:", err)
		return
	}
	switch m := decMsg.(type) {
	case common.ConnResponseMessage:
		c.connRespMsgs <- m
	case common.ClientChatMessage:
		c.logger.Info("Received chat message: ", m.User, ":", m.Content)
		c.ioStream.Println(fmt.Sprintf("%s: %s", m.User, m.Content))
	default:
		c.logger.Error("Received unknown message type.", reflect.TypeOf(m))
		c.ioStream.Println("ERROR: Unknown message type")
	}
	return
}

func (c *client) Run() {
	c.handleConn()
}

// Handles the process of connecting to the server, and then looping on user inputs.
func (c *client) handleConn() {
	for {
		c.logger.Info("Client trying to connect to", c.server)
		connReq := common.ConnRequestMessage{User: c.user}
		data, err := common.EncodeMessage(connReq)
		if err != nil {
			c.logger.Error("Failed to encode message:", err)
			return
		}
		c.network.Send(c.server, data)

		c.logger.Info("Waiting for server response...")
		connResp := <-c.connRespMsgs
		if connResp.Leader == c.server {
			break
		} else {
			c.logger.Info("Server redirects me to", connResp.Leader)
			c.server = connResp.Leader
		}
	}
	go func() {
		for range c.connRespMsgs {
			// Drain the channel, just in case
		}
	}()
	c.logger.Info("Connected to", c.server)
	c.readUserInputs()
}

// Loops on user inputs and sends them to the server as is.
func (c *client) readUserInputs() {
	in := c.ioStream
	for {
		c.logger.Info("Waiting for user input...")
		userInput, _ := in.ReadLine()

		msg := common.ClientChatMessage{User: c.user, Content: userInput}
		data, err := common.EncodeMessage(msg)
		if err != nil {
			c.ioStream.Println("ERROR: Failed to encode message:", err)
			return
		}
		c.logger.Info("Sending user input to server.")
		c.network.Send(c.server, data)
	}
}
