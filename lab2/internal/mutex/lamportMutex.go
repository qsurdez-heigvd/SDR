package mutex

import (
	"chatsapp/internal/logging"
)

type requestSignal struct {
	responseChan chan struct{}
}

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
	receivedMessages chan Message
	requestReceived  chan struct{}
	releaseReceived  chan struct{}
	inCS             bool
	canEnterCS       chan struct{}

	logger           logging.Logger
	netWrapper       NetWrapper
	currentTimeStamp timestamp
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
	lm := lamportMutex{
		receivedMessages: make(chan Message, 100),
		requestReceived:  make(chan struct{}, 100),
		releaseReceived:  make(chan struct{}, 100),
		inCS:             false,
		canEnterCS:       make(chan struct{}),

		logger:           *logger,
		netWrapper:       networkWrapper,
		currentTimeStamp: timestamp{Seqnum: 0, Pid: self},
	}

	go lm.listenIncomingMessage(self, neighbors)

	return &lm
}

func (lm *lamportMutex) listenIncomingMessage(self Pid, neighbors []Pid) {

	// We create a map with all Pid, with default value of {REL, 0}
	neighborsMap := make(map[Pid]Message)
	for _, neighbor := range neighbors {
		neighborsMap[neighbor] = Message{
			Type: relMsg,
			TS:   timestamp{Seqnum: 0, Pid: neighbor},
		}
	}

	neighborsMap[self] = Message{
		Type: relMsg,
		TS:   timestamp{Seqnum: 0, Pid: self},
	}

	for {
		select {
		case msg := <-lm.netWrapper.FromNet:
			lm.receivedMessages <- msg
		case <-lm.requestReceived:
			lm.currentTimeStamp.Seqnum++
			for _, neighbor := range neighbors {
				// Write REQ with correct timestamp
				if neighbor != self {
					lm.netWrapper.IntoNet <- OutgoingMessage{
						Destination: neighbor,
						Message: Message{
							Type: reqMsg,
							TS:   lm.currentTimeStamp,
						},
					}
				}
			}
			// Simulation of received req in self
			lm.receivedMessages <- Message{
				Type: reqMsg,
				TS:   lm.currentTimeStamp,
			}

		case <-lm.releaseReceived:
			lm.inCS = false
			lm.currentTimeStamp.Seqnum++
			for _, neighbor := range neighbors {
				if neighbor != self {
					lm.netWrapper.IntoNet <- OutgoingMessage{
						Destination: neighbor,
						Message: Message{
							Type: relMsg,
							TS:   lm.currentTimeStamp,
						},
					}
				}
			}
			// Simulation of received rel in self
			lm.receivedMessages <- Message{
				Type: relMsg,
				TS:   lm.currentTimeStamp,
			}

		case message := <-lm.receivedMessages:
			switch message.Type {
			case reqMsg:
				// Increment Seqnum of Timestamp
				lm.handleTS(message, self)
				// maybe refactor ?
				neighborsMap[message.TS.Pid] = message
				if message.TS.Pid != self {
					lm.netWrapper.IntoNet <- OutgoingMessage{
						Destination: message.TS.Pid,
						Message: Message{
							Type: ackMsg,
							TS:   lm.currentTimeStamp,
						},
					}
				}
				lm.tryEnterCS(neighborsMap, self)

			case relMsg:
				// Increment Seqnum of Timestamp
				lm.handleTS(message, self)
				// maybe refactor ?
				neighborsMap[message.TS.Pid] = message
				lm.tryEnterCS(neighborsMap, self)

			case ackMsg:
				lm.handleTS(message, self)
				if neighborsMap[message.TS.Pid].Type == reqMsg {
					// Keep the REQ, discard the ACK
				} else {
					neighborsMap[message.TS.Pid] = message
				}
				lm.tryEnterCS(neighborsMap, self)
			}
		}
	}
}

func (lm *lamportMutex) handleTS(message Message, self Pid) {
	if message.TS.Pid != self {
		lm.currentTimeStamp.Seqnum = lm.currentTimeStamp.Max(message.TS).Seqnum + 1
	}
}

// Private method
func (lm *lamportMutex) tryEnterCS(neighborsMap map[Pid]Message, self Pid) {

	if lm.inCS {
		return
	}

	if neighborsMap[self].Type != reqMsg {
		return
	}

	// Find the message with the SMALLEST timestamp globally
	oldestMsg := Message{
		Type: reqMsg,
		TS:   timestamp{Seqnum: ^uint32(0), Pid: ""}, // Start with max value
	}

	for _, msg := range neighborsMap {
		if msg.TS.LessThan(oldestMsg.TS) {
			oldestMsg = msg
		}
	}

	// Only enter CS if OUR request is the globally oldest message
	if oldestMsg.TS.Pid == self && oldestMsg.Type == reqMsg {
		lm.inCS = true
		lm.canEnterCS <- struct{}{}
	}
}

func (lm *lamportMutex) Request() (release func(), err error) {

	lm.requestReceived <- struct{}{}

	<-lm.canEnterCS

	return func() {
		lm.releaseReceived <- struct{}{}

	}, nil

}
