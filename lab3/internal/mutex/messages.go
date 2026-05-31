package mutex

import (
	"encoding/gob"
)

// Enum for REQ, REL and ACK messages
type messageType int

func (m messageType) String() string {
	switch m {
	case reqMsg:
		return "REQ"
	case relMsg:
		return "REL"
	case ackMsg:
		return "ACK"
	default:
		return "INVALID"
	}
}

const (
	reqMsg messageType = iota
	relMsg
	ackMsg
)

// Message struct for REQ, REL and ACK messages
type Message struct {
	Type messageType
	TS   timestamp
}

// RegisterToGob registers this message type to Gob.
func (Message) RegisterToGob() {
	gob.Register(timestamp{})
	gob.Register(Message{})
}

// GetSource returns the source of the message
func (m Message) GetSource() Pid {
	return m.TS.Pid
}
