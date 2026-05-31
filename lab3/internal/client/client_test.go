package client

import (
	"chatsapp/internal/common"
	"chatsapp/internal/logging"
	"chatsapp/internal/messages"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils/ioutils"
	"testing"
)

func expectSentMessage(t *testing.T, m messages.Message, dest transport.Address, ni *transport.MockNetworkInterface) {
	received := <-ni.SentMessages
	dec, err := common.DecodeMessage(received.Payload)
	if err != nil {
		t.Error("Error decoding message:", err)
	}
	if dest != received.To {
		t.Error("Sent message to wrong destination. Expected:", dest, "Got:", received.To)
	}
	if dec != m {
		t.Error("Decoded message is different from expected. Expected:", m, "Got:", dec)
	}
}

func TestClientSuccessfulFirstConnTry(t *testing.T) {
	common.RegisterAllToGob()

	serverAddr := transport.Address{IP: "127.0.0.1", Port: 5000}
	selfAddr := transport.Address{IP: "127.0.0.1", Port: 5001}
	username := common.Username("Alex")
	msgContent := "Hi there!"

	logger := logging.NewStdLogger("t")
	ni := transport.NewMockNetworkInterface(selfAddr)
	stream := ioutils.NewMockReader()

	client := NewClient(logger, serverAddr, selfAddr, username, ni, stream)
	go client.Run()

	go func() {
		stream.SimulateNextInputLine(msgContent)
	}()

	expectSentMessage(t, common.ConnRequestMessage{User: username}, serverAddr, ni)

	connResp := common.ConnResponseMessage{Leader: serverAddr}
	connRespEncoded, err := common.EncodeMessage(connResp)
	if err != nil {
		t.Error("Error encoding message:", err)
	}
	ni.SimulateReception(&transport.Message{Source: serverAddr, Payload: connRespEncoded})

	expectSentMessage(t, common.ClientChatMessage{User: username, Content: msgContent}, serverAddr, ni)
}

func TestClientMultipleConnTries(t *testing.T) {
	common.RegisterAllToGob()

	serverAddr := transport.Address{IP: "127.0.0.1", Port: 5000}
	leaderAddr := transport.Address{IP: "127.0.0.1", Port: 5002}
	selfAddr := transport.Address{IP: "127.0.0.1", Port: 5001}
	username := common.Username("Alex")
	msgContent := "Hi there!"

	logger := logging.NewStdLogger("t")
	ni := transport.NewMockNetworkInterface(selfAddr)
	stream := ioutils.NewMockReader()

	client := NewClient(logger, serverAddr, selfAddr, username, ni, stream)
	go client.Run()

	go func() {
		stream.SimulateNextInputLine(msgContent)
	}()

	expectSentMessage(t, common.ConnRequestMessage{User: username}, serverAddr, ni)

	connResp := common.ConnResponseMessage{Leader: leaderAddr}
	connRespEncoded, err := common.EncodeMessage(connResp)
	if err != nil {
		t.Error("Error encoding message:", err)
	}
	ni.SimulateReception(&transport.Message{Source: serverAddr, Payload: connRespEncoded})

	expectSentMessage(t, common.ConnRequestMessage{User: username}, leaderAddr, ni)

	connResp = common.ConnResponseMessage{Leader: leaderAddr}
	connRespEncoded, err = common.EncodeMessage(connResp)
	if err != nil {
		t.Error("Error encoding message:", err)
	}
	ni.SimulateReception(&transport.Message{Source: leaderAddr, Payload: connRespEncoded})

	expectSentMessage(t, common.ClientChatMessage{User: username, Content: msgContent}, leaderAddr, ni)
}
