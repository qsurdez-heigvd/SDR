package rr

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils/ioutils"
	"fmt"
	"reflect"
	"testing"
	"time"
)

type netWrapper struct {
	intoNet chan []byte
	fromNet chan []byte
}

func newWrappedConn() netWrapper {
	intoNet := make(chan []byte)
	fromNet := make(chan []byte)
	return netWrapper{
		intoNet: intoNet,
		fromNet: fromNet,
	}
}

func (c netWrapper) simulateReceivedMessage(msg message) {
	bytes, err := msg.encode()
	if err != nil {
		panic(err)
	}
	c.fromNet <- bytes
}

func (c netWrapper) interceptSentMessage() *message {
	bytes := <-c.intoNet
	msg, err := decode(bytes)
	if err != nil {
		panic(err)
	}
	return msg
}

func (c netWrapper) toRRWrapper() NetWrapper {
	return NetWrapper{
		IntoNet: c.intoNet,
		FromNet: c.fromNet,
	}
}

/** Test basic send and reception of response */
func TestRRSend(t *testing.T) {
	// Sending a message to a RR should result in the message being sent to the network interface
	log := logging.NewStdLogger("rr_test")
	destAddr := transport.Address{IP: "127.0.0.1", Port: 5001}

	conn := newWrappedConn()

	rr1 := NewRR(log, destAddr, conn.toRRWrapper())
	fmt.Println("Sending request to RR")

	payload := []byte("Hello, World!")
	responseChan := make(chan []byte)
	go func() {
		rrResponseChan := rr1.SendRequest(payload)
		response := <-rrResponseChan
		if response.Err != nil {
			t.Error("Error sending RR request:", response.Err)
		}
		responseChan <- response.Response
	}()

	// Get message
	rrRequest := conn.interceptSentMessage()
	if rrRequest.Type != reqMsg {
		t.Error("Expected type to be RRRequest, got:", rrRequest.Type)
	}
	if !reflect.DeepEqual(rrRequest.Payload, payload) {
		t.Error("Expected payload to be 'Hello, World!', got:", string(rrRequest.Payload))
	}

	// Send response
	responsePayload := []byte("Hello from the world!")
	rrResponse := message{Type: rspMsg, Seqnum: rrRequest.Seqnum, Payload: responsePayload}
	conn.simulateReceivedMessage(rrResponse)

	receivedFromRR := <-responseChan
	if !reflect.DeepEqual(receivedFromRR, responsePayload) {
		t.Errorf("Expected response to be %s, got: %s", responsePayload, receivedFromRR)
	}
}

/** Test not responding to request ; should resend it after timeout */
func TestRRNoResponse(t *testing.T) {
	log := logging.NewLogger(ioutils.NewStdStream(), nil, "rr_test", false)
	conn := newWrappedConn()

	destAddr, _ := transport.NewAddress("127.0.0.1:5001")
	rr1 := NewRR(log, destAddr, conn.toRRWrapper())

	fmt.Println("Sending request to RR")
	go func() {
		rrResponseChan := rr1.SendRequest([]byte("Hello, World!"))
		response := <-rrResponseChan
		if response.Err != nil {
			t.Error("Error sending RR request:", response.Err)
		}
	}()

	// Get message
	rrRequest := conn.interceptSentMessage()
	seqnum := rrRequest.Seqnum

	// Do not send response

	// Start timer
	resendReceived := make(chan struct{})
	go func() {
		select {
		case <-resendReceived:
			return
		case <-time.After(15 * time.Second):
			t.Error("Timeout waiting for re-sending request")
		}
	}()

	// Wait for next request
	rrRequest = conn.interceptSentMessage()
	if rrRequest.Seqnum != seqnum {
		t.Error("Expected seqnum to be", seqnum, "got:", rrRequest.Seqnum)
	}

	close(resendReceived)
}

/** Test sending response twice ; second should be ignored */
func TestRRTwoSends(t *testing.T) {
	log := logging.NewLogger(ioutils.NewStdStream(), nil, "rr_test", false)
	conn := newWrappedConn()

	destAddr, _ := transport.NewAddress("127.0.0.1:5001")
	rr1 := NewRR(log, destAddr, conn.toRRWrapper())
	fmt.Println("Sending request to RR")

	payload := []byte("Hello, World!")
	responseChan := make(chan []byte)
	go func() {
		rrResponseChan := rr1.SendRequest(payload)
		response := <-rrResponseChan
		if response.Err != nil {
			t.Error("Error sending RR request:", response.Err)
		}
		responseChan <- response.Response
	}()

	// Get message
	rrRequest := conn.interceptSentMessage()

	responsePayload := []byte("Hello from the world!")
	rrResponse := message{Type: rspMsg, Seqnum: rrRequest.Seqnum, Payload: responsePayload}

	// Send response twice
	conn.simulateReceivedMessage(rrResponse)

	// Wait for	response
	receivedFromRR := <-responseChan
	if !reflect.DeepEqual(receivedFromRR, responsePayload) {
		t.Errorf("Expected response to be %s, got: %s", responsePayload, receivedFromRR)
	}

	// Send new request
	payload = []byte("Hello, World again!")
	response2Chan := make(chan []byte)
	go func() {
		rrResponse2Chan := rr1.SendRequest(payload)
		response2 := <-rrResponse2Chan
		if response2.Err != nil {
			t.Error("Error sending RR request:", response2.Err)
		}
		response2Chan <- response2.Response
	}()

	// Get message
	rrRequest2 := conn.interceptSentMessage()
	responsePayload = []byte("Hello from the world again!")

	// Send response
	rrResponse2 := message{Type: rspMsg, Seqnum: rrRequest2.Seqnum, Payload: responsePayload}
	conn.simulateReceivedMessage(rrResponse2)

	receivedFromRR2 := <-response2Chan
	if !reflect.DeepEqual(receivedFromRR2, responsePayload) {
		t.Errorf("Expected response to be %s, got: %s", responsePayload, receivedFromRR2)
	}
}

func TestRRDuplicateResponseIgnored(t *testing.T) {
	log := logging.NewLogger(ioutils.NewStdStream(), nil, "rr_test", false)
	conn := newWrappedConn()
	destAddr, _ := transport.NewAddress("127.0.0.1:5001")
	rr1 := NewRR(log, destAddr, conn.toRRWrapper())

	receivedReqs := make(chan []byte, 1)
	rr1.SetRequestHandler(func(b []byte) []byte {
		receivedReqs <- b
		return nil
	})

	fmt.Println("Sending request to RR")

	payload := []byte("Hello, World!")
	responseChan := make(chan []byte)
	go func() {
		rrResponseChan := rr1.SendRequest(payload)
		response := <-rrResponseChan
		if response.Err != nil {
			t.Error("Error sending RR request:", response.Err)
		}
		responseChan <- response.Response
	}()

	// Get message
	rrRequest := conn.interceptSentMessage()

	responsePayload := []byte("Hello from the world!")
	rrResponse := message{Type: rspMsg, Seqnum: rrRequest.Seqnum, Payload: responsePayload}

	// Send response twice
	conn.simulateReceivedMessage(rrResponse)
	conn.simulateReceivedMessage(rrResponse)

	// Wait for	response
	receivedFromRR := <-responseChan
	if !reflect.DeepEqual(receivedFromRR, responsePayload) {
		t.Errorf("Expected response to be %s, got: %s", responsePayload, receivedFromRR)
	}

	// Don't expect a received request from the RR
	fmt.Println("Waiting to ensure no request is received")
	select {
	case <-receivedReqs:
		t.Errorf("Expected no request to be received")
	case <-time.After(2 * time.Second):
	}

	// Send new request
	payload = []byte("Hello, World again!")
	response2Chan := make(chan []byte)
	go func() {
		rrResponse2Chan := rr1.SendRequest(payload)
		response2 := <-rrResponse2Chan
		if response2.Err != nil {
			t.Error("Error sending RR request:", response2.Err)
		}
		response2Chan <- response2.Response
	}()

	// Get message
	rrRequest2 := conn.interceptSentMessage()
	responsePayload = []byte("Hello from the world again!")

	// Send response
	rrResponse2 := message{Type: rspMsg, Seqnum: rrRequest2.Seqnum, Payload: responsePayload}
	conn.simulateReceivedMessage(rrResponse2)

	receivedFromRR2 := <-response2Chan
	if !reflect.DeepEqual(receivedFromRR2, responsePayload) {
		t.Errorf("Expected response to be %s, got: %s", responsePayload, receivedFromRR2)
	}
}

/** Test reception of request calls onRequest handler and sends its return value */
func TestRRCustomHandler(t *testing.T) {
	log := logging.NewLogger(ioutils.NewStdStream(), nil, "rr_test", false)
	conn := newWrappedConn()

	destAddr, _ := transport.NewAddress("127.0.0.1:5001")
	rr1 := NewRR(log, destAddr, conn.toRRWrapper())
	rr1.SetRequestHandler(func(payload []byte) []byte {
		return []byte("Custom response")
	})

	fmt.Println("Receiving request into RR")
	rrRequest := message{Type: reqMsg, Seqnum: seqnum{0, 1}, Payload: []byte("Custom request")}

	conn.simulateReceivedMessage(rrRequest)

	// Expect receiving response into mock network
	rrResponse := conn.interceptSentMessage()

	if rrResponse.Seqnum.MsgID != 1 {
		t.Error("Expected seqnum to be 1, got:", rrResponse.Seqnum)
	}
	if rrResponse.Type != rspMsg {
		t.Error("Expected type to be RRResponse, got:", rrResponse.Type)
	}
	if string(rrResponse.Payload) != "Custom response" {
		t.Error("Expected payload to be 'Custom response', got:", string(rrResponse.Payload))
	}
}

/** Test receiving multiple requests, should send multiple responses */
func TestRRMultipleRequests(t *testing.T) {
	log := logging.NewLogger(ioutils.NewStdStream(), nil, "rr_test", false)
	conn := newWrappedConn()

	destAddr, _ := transport.NewAddress("127.0.0.1:5001")
	rr := NewRR(log, destAddr, conn.toRRWrapper())
	rr.SetRequestHandler(func(payload []byte) []byte {
		return []byte(fmt.Sprintf("Response to %s", payload))
	})

	rrRequest1 := message{Type: reqMsg, Seqnum: seqnum{0, 1}, Payload: []byte("Request 1")}
	rrRequest2 := message{Type: reqMsg, Seqnum: seqnum{0, 2}, Payload: []byte("Request 2")}

	conn.simulateReceivedMessage(rrRequest1)

	rrResponse1 := conn.interceptSentMessage()
	if rrResponse1.Seqnum.MsgID != 1 {
		t.Error("Expected seqnum to be 1, got:", rrResponse1.Seqnum)
	}
	if string(rrResponse1.Payload) != "Response to Request 1" {
		t.Error("Expected payload to be 'Response to Request 1', got:", string(rrResponse1.Payload))
	}

	conn.simulateReceivedMessage(rrRequest2)

	rrResponse2 := conn.interceptSentMessage()
	if rrResponse2.Seqnum.MsgID != 2 {
		t.Error("Expected seqnum to be 2, got:", rrResponse2.Seqnum)
	}
	if string(rrResponse2.Payload) != "Response to Request 2" {
		t.Error("Expected payload to be 'Response to Request 2', got:", string(rrResponse2.Payload))
	}
}

/** Test receiving multiple requests with same seqnum, should send same response each time */
func TestRRMultipleRequestsSameSeqnum(t *testing.T) {
	log := logging.NewLogger(ioutils.NewStdStream(), nil, "rr_test", false)
	conn := newWrappedConn()
	destAddr, _ := transport.NewAddress("127.0.0.1:5001")
	rr := NewRR(log, destAddr, conn.toRRWrapper())
	rr.SetRequestHandler(func(payload []byte) []byte {
		return []byte(fmt.Sprintf("Response to %s", payload))
	})

	rrRequest := message{Type: reqMsg, Seqnum: seqnum{0, 1}, Payload: []byte("Request 1")}
	conn.simulateReceivedMessage(rrRequest)

	rrResponse := conn.interceptSentMessage()
	if rrResponse.Seqnum.MsgID != 1 {
		t.Error("Expected seqnum to be 1, got:", rrResponse.Seqnum)
	}
	if string(rrResponse.Payload) != "Response to Request 1" {
		t.Error("Expected payload to be 'Response to Request 1', got:", string(rrResponse.Payload))
	}

	rrRequest = message{Type: reqMsg, Seqnum: seqnum{0, 1}, Payload: []byte("Request 2")}
	conn.simulateReceivedMessage(rrRequest)

	rrResponse = conn.interceptSentMessage()
	if rrResponse.Seqnum.MsgID != 1 {
		t.Error("Expected seqnum to be 1, got:", rrResponse.Seqnum)
	}
	if string(rrResponse.Payload) != "Response to Request 1" {
		t.Error("Expected payload to be 'Response to Request 1', got:", string(rrResponse.Payload))
	}
}
