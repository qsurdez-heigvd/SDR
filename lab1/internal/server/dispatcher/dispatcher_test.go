package dispatcher

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils/ioutils"
	"encoding/gob"
	"reflect"
	"testing"
)

type MutexMessage struct {
	Pid uint32
}

type ChatMessage struct {
	Content string
}

func (MutexMessage) RegisterToGob() {
	gob.Register(MutexMessage{})
}

func (ChatMessage) RegisterToGob() {
	gob.Register(ChatMessage{})
}

func TestRegister(t *testing.T) {
	addr1 := transport.Address{IP: "127.0.0.1", Port: 5000}
	addr2 := transport.Address{IP: "127.0.0.1", Port: 5001}

	expectedMsg := MutexMessage{42}

	mockNet := transport.NewMockNetworkInterface(addr1)

	logger := logging.NewLogger(ioutils.NewStdStream(), nil, "disp_test", false)
	d := NewDispatcher(logger, addr1, []transport.Address{addr2}, mockNet)
	d.Register(MutexMessage{}, func(msg Message, source transport.Address) {
		if _, ok := msg.(MutexMessage); !ok {
			t.Fatalf("expected MutexMessage, got %T", msg)
		}

		if source != addr2 {
			t.Fatalf("expected source %v, got %v", addr2, source)
		}

		if !reflect.DeepEqual(msg, expectedMsg) {
			t.Fatalf("expected message %v, got %v", expectedMsg, msg)
		}
	})

	go func() {
		receivedMsg := mockNet.InterceptSentMessage()
		responseMsg := transport.Message{Source: addr2, Payload: receivedMsg.Message.Payload}
		mockNet.SimulateReception(&responseMsg)
	}()

	d.Send(expectedMsg, addr2)
}
