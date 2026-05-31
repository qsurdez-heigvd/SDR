package routing

import (
	"chatsapp/internal/messages"
	"chatsapp/internal/transport"
	"encoding/gob"
	"fmt"
)

// ExplorationRequest is a message sent by a node to its neighbors to explore the network.
type ExplorationRequest struct {
}

// ExplorationResponse is a message sent by a node to a node that sent an ExplorationRequest, containing the routing table of the responding node.
type ExplorationResponse struct {
	RoutingTable map[transport.Address]transport.Address
}

// BroadcastRequest is a message sent by a node to its neighbors to broadcast a message to the network.
type BroadcastRequest struct {
	Msg messages.Message
}

// BroadcastResponse is a message sent by a node to a node that sent a BroadcastRequest, containing the addresses of the nodes that received the broadcast.
type BroadcastResponse struct {
	Receivers []transport.Address
}

// RoutedMessage is a message that should be routed to a specific node.
type RoutedMessage struct {
	Msg  messages.Message
	From transport.Address
	To   transport.Address
}

// RegisterToGob registers the types to gob.
func (e ExplorationRequest) RegisterToGob() {
	gob.Register(e)
}

// RegisterToGob registers the types to gob.
func (e ExplorationResponse) RegisterToGob() {
	gob.Register(e)
}

// RegisterToGob registers the types to gob.
func (b BroadcastRequest) RegisterToGob() {
	gob.Register(b)
}

// RegisterToGob registers the types to gob.
func (b BroadcastResponse) RegisterToGob() {
	gob.Register(b)
}

// RegisterToGob registers the types to gob.
func (r RoutedMessage) RegisterToGob() {
	gob.Register(r)
}

func (e ExplorationRequest) String() string {
	return "ExploReq{}"
}

func (e ExplorationResponse) String() string {
	return fmt.Sprintf("ExploRsp{table: %v}", e.RoutingTable)
}

func (b BroadcastRequest) String() string {
	return fmt.Sprintf("BrdcstReq{Msg: %v}", b.Msg)
}

func (b BroadcastResponse) String() string {
	return fmt.Sprintf("BrdcstRsp{Receivers: %v}", b.Receivers)
}

func (r RoutedMessage) String() string {
	return fmt.Sprintf("RoutedMsg{Msg: %v, To: %v}", r.Msg, r.To)
}
