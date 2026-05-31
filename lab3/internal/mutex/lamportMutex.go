package mutex

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/timestamps"
	"chatsapp/internal/utils"
	"fmt"
)

// OutgoingMessage represents a message that the mutex instance wants to send to the network.
type OutgoingMessage struct {
	// The destination of the message. If none, the message is intended to be broadcasted.
	Destination Pid
	// The message to send.
	Message Message
}

// NetWrapper is a wrapper around two channels used to communicate with the network.
type NetWrapper struct {
	// The channel on which the mutex instance will send messages to the network.
	IntoNet chan<- OutgoingMessage
	// The channel on which the mutex instance will receive messages from the network.
	FromNet <-chan Message
}

// An implementation of the Lamport mutex algorithm, aligning to the Mutex interface.
type lamportMutex struct {
	logger *logging.Logger

	net  NetWrapper
	self Pid

	neighbors []timestamps.Pid

	toSendMessages      *utils.BufferedChan[messageToSend]
	toBroadcastMessages chan messageType
	csDoneRequests      chan chan<- struct{}

	csPermissions chan struct{}
}

// Descriptor of a message that must be sent by the mutex.
type messageToSend struct {
	dest    Pid
	msgType messageType
}

/*
Everything that may change over the course of the execution, and thus must be handled by a single goroutine to avoid concurrent access.

Having this inside a single struct makes it easier to pass around.
*/
type lamportMutexState struct {
	lastMessages map[Pid]Message
	isInCS       bool
	timestamps   *timestamps.Handler
}

/*
NewLamportMutex constructs and returns a new Lamport Mutex.

Parameters:
  - logger: The logger to use for logging messages.
  - networkWrapper: The network that the mutex will use to communicate with neighbors.
  - self: The process ID of the current process.
  - neighbors: A slice of process IDs representing the neighboring processes.

Returns:
  - Mutex: A Lamport Mutex.
*/
func NewLamportMutex(logger *logging.Logger, networkWrapper NetWrapper, self Pid, neighbors []Pid) Mutex {
	logger = logger.WithLogLevel(logging.WARN)

	neighbors = append(neighbors, self)

	toSendMessages := utils.NewBufferedChan[messageToSend]()
	toBroadcastMesages := make(chan messageType)
	csPermissions := make(chan struct{})
	csExits := make(chan chan<- struct{})

	m := lamportMutex{logger, networkWrapper, self, neighbors, toSendMessages, toBroadcastMesages, csExits, csPermissions}

	logger.Infof("Starting Lamport mutex with self %v and neighbors %v", self, neighbors)

	go m.handleState()

	return &m
}

// The main goroutine that handles the state of the mutex, represented by a local [lamportMutexState] instance.
func (m *lamportMutex) handleState() {
	state := lamportMutexState{
		lastMessages: make(map[Pid]Message),
		isInCS:       false,
		timestamps:   timestamps.NewLamportHandler(m.self, 0),
	}
	for _, pid := range m.neighbors {
		state.lastMessages[pid] = Message{Type: relMsg, TS: timestamp{Seqnum: 0, Pid: pid}}
	}

	for {
		select {
		case message := <-m.net.FromNet:
			m.handleMessage(&state, message)
		case toSend := <-m.toSendMessages.Outlet():
			m.send(&state, toSend.dest, toSend.msgType)
		case msgType := <-m.toBroadcastMessages:
			m.broadcast(&state, msgType)
		case doneCh := <-m.csDoneRequests:
			state.isInCS = false
			m.broadcast(&state, relMsg)
			doneCh <- struct{}{}
		}
	}
}

// Handles a single message received from the network.
func (m *lamportMutex) handleMessage(state *lamportMutexState, message Message) {
	m.logger.Info("Mutex receives message ", message)

	if message.TS.Pid != m.self {
		state.timestamps.UpdateTimestamp(message.TS)
	}

	// Store message in table
	if message.Type == ackMsg && state.lastMessages[message.TS.Pid].Type == reqMsg {
		// Do not overwrite REQ with ACK
		m.logger.Info("Not storing ACK over REQ ")
	} else {
		if message.TS.LessThan(state.lastMessages[message.TS.Pid].TS) {
			m.logger.Warn("Received message ", message.Type, "|", message.TS.Seqnum, " for ", message.TS.Pid, " that is older than the last message ", state.lastMessages[message.TS.Pid].Type, "|", state.lastMessages[message.TS.Pid].TS.Seqnum, ". This should never happen because the network garantees us total order. Not storing that message, since it is outdated. Treating it, though.")
		} else {
			m.logger.Info("Storing ", message.Type, "|", message.TS.Seqnum, " for ", message.TS.Pid)
			state.lastMessages[message.TS.Pid] = message
		}
	}

	// Handle message
	if message.Type == reqMsg {
		m.toSendMessages.Inlet() <- messageToSend{message.TS.Pid, ackMsg}
	}

	// Check if it can enter critical section
	m.tryEnterCS(state)
}

// Called by user of mutex to release the critical section.
//
// Will wait for the mutex to be released before returning, so as to prevent requesting the mutex again before it is released.
func (m *lamportMutex) release() {
	m.logger.Info("Releasing mutex")

	ch := make(chan struct{})
	m.csDoneRequests <- ch
	<-ch
}

// Enters the critical section if (1) it is not already in it, (2) it has a pending request, and (3) its request is the oldest.
func (m *lamportMutex) tryEnterCS(state *lamportMutexState) {
	if state.isInCS {
		m.logger.Info("Already in CS; not trying to enter")
		return
	}

	if state.lastMessages[m.self].Type != reqMsg {
		m.logger.Info("No pending request, currently ", state.lastMessages[m.self].Type, "; not trying to enter CS")
		return
	}

	oldestMsg := Message{Type: reqMsg, TS: timestamp{Seqnum: ^uint32(0), Pid: ""}}
	for _, msg := range state.lastMessages {
		if msg.TS.LessThan(oldestMsg.TS) {
			oldestMsg = msg
		}
	}
	if oldestMsg.GetSource() == m.self {
		m.logger.Info("My REQ is oldest: ", m.lastMessagesToString(state))
		state.isInCS = true
		m.enterCS()
	} else {
		m.logger.Info("Another message is older than my REQ; not entering CS: ", m.lastMessagesToString(state))
	}
}

// Converts the last messages table to a string for logging.
func (m *lamportMutex) lastMessagesToString(state *lamportMutexState) string {
	s := "{"
	for pid, msg := range state.lastMessages {
		s += fmt.Sprintf("%v: %s|%v, ", pid, msg.Type, msg.TS.Seqnum)
	}
	s += "}"
	return s
}

// Enters the critical section.
func (m *lamportMutex) enterCS() {
	m.logger.Info("Sending permission")
	m.csPermissions <- struct{}{}
}

// Sends a message to a given process.
func (m *lamportMutex) send(state *lamportMutexState, dest Pid, msgType messageType) {
	m.logger.Info("Sending message ", msgType, " to ", dest)
	msg := Message{msgType, state.timestamps.GetTimestamp()}
	if dest == m.self {
		m.handleMessage(state, msg)
	} else {
		m.net.IntoNet <- OutgoingMessage{Destination: dest, Message: msg}
	}
}

// Broadcasts a message to all neighbors.
func (m *lamportMutex) broadcast(state *lamportMutexState, msgType messageType) {
	m.logger.Info("Broadcasting message ", msgType)
	state.timestamps.IncrementTimestamp()
	for _, pid := range m.neighbors {
		m.send(state, pid, msgType)
	}
}

// Request requests to enter the critical section.
func (m *lamportMutex) Request() (release func(), err error) {
	m.logger.Info("Requesting mutex")

	release = m.release

	// Broadcast request
	m.toBroadcastMessages <- reqMsg

	// Wait for permission
	m.logger.Info("Waiting for permission")
	<-m.csPermissions
	m.logger.Info("Got permission")

	return
}
