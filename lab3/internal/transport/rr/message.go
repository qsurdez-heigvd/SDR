package rr

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// Types of RR messages (request and response)
type msgType uint8

const (
	reqMsg msgType = iota
	rspMsg
)

func (m msgType) String() string {
	switch m {
	case reqMsg:
		return "REQ"
	case rspMsg:
		return "RSP"
	default:
		return "UNKNOWN"
	}
}

// An RR message, defined by its type, sequence number, and payload
type message struct {
	Type    msgType
	Seqnum  seqnum
	Payload []byte
}

func (m *message) String() string {
	var typeStr string
	if m.Type == reqMsg {
		typeStr = "REQ"
	} else {
		typeStr = "RSP"
	}
	return fmt.Sprintf("RR-%s{%d, %v bytes}", typeStr, m.Seqnum, len(m.Payload))
}

// Encodes the message to a byte slice
func (m *message) encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(m)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decodes a message from a byte slice
func decode(encodedMessage []byte) (*message, error) {
	var message message
	buf := bytes.NewBuffer(encodedMessage)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&message)
	if err != nil {
		return nil, err
	}
	return &message, nil
}
