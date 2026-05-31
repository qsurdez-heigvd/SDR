package common

import "testing"

func TestCodecChat(t *testing.T) {
	msg := ClientChatMessage{"Hello, world!", "John"}

	RegisterAllToGob()

	data, err := EncodeMessage(msg)
	if err != nil {
		t.Error("Error encoding message:", err)
	}
	decMsg, err := DecodeMessage(data)
	if err != nil {
		t.Error("Error decoding message:", err)
	}
	if decChatMsg, ok := decMsg.(ClientChatMessage); !ok {
		t.Error("Decoded message is not a ClientChatMessage")
	} else if decChatMsg.Content != msg.Content {
		t.Error("Decoded message has different content")
	}
}

func TestCodecConnRequest(t *testing.T) {
	msg := ConnRequestMessage{}

	RegisterAllToGob()

	data, err := EncodeMessage(msg)
	if err != nil {
		t.Error("Error encoding message:", err)
	}
	decMsg, err := DecodeMessage(data)
	if err != nil {
		t.Error("Error decoding message:", err)
	}
	if _, ok := decMsg.(ConnRequestMessage); !ok {
		t.Error("Decoded message is not a ConnRequestMessage")
	}
}

func TestCodecConnResponse(t *testing.T) {
	msg := ConnResponseMessage{address{IP: "127.0.0.1", Port: 1234}}

	RegisterAllToGob()

	data, err := EncodeMessage(msg)
	if err != nil {
		t.Error("Error encoding message:", err)
	}

	decMsg, err := DecodeMessage(data)
	if err != nil {
		t.Error("Error decoding message:", err)
	}
	if decConnRespMsg, ok := decMsg.(ConnResponseMessage); !ok {
		t.Error("Decoded message is not a ConnResponseMessage")
	} else if decConnRespMsg.Leader != msg.Leader {
		t.Error("Decoded message has different leader")
	}
}
