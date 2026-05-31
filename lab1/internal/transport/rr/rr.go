/*
@author: Quentin Surdez
@date: 15/10/2025
@description: This file implements an RR module that enforces the Request-Reply
			protocol. This RR module can be used with several network protocols
			(i.e TCP/UDP) as it uses a wrapper to receive/send messages from/to
			the network.
*/

package rr

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils"
	"errors"
	"fmt"
	"time"
)

// Sequence number
type seqnum struct {
	// ID of the RR instance that generated that sequence number. Could be UNIX time in milliseconds, for example.
	InstID int64
	// ID of the message associated with that sequence number. Must be unique in the context of the RR instance identified by InstId.
	MsgID utils.UID
}

// LessThan compares two sequence numbers ; by instance id and then by message id.
func (s seqnum) LessThan(other seqnum) bool {
	return s.InstID < other.InstID || (s.InstID == other.InstID && s.MsgID < other.MsgID)
}

// Implements the Stringer interface for sequence numbers.
func (s seqnum) String() string {
	return fmt.Sprintf("%d-%d", s.InstID, s.MsgID)
}

// RR represents an instance of a Request-Response (RR) protocol.
//
// This abstraction allows this server to be used as both sender and receiver in the RR protocol, for a given remote. It is thus intended that there be one RR instance per known remote.
type RR interface {
	// SendRequest sends a request to the associated remote, and blocks until a Response is received from that remote.
	SendRequest(payload []byte) <-chan SendResponse
	// SetRequestHandler sets the handler for incoming requests. The handler should return the Response that should be replied to the sender. Response may be nil in which case empty Response will be sent to the sender.
	SetRequestHandler(handleRequest func([]byte) []byte)
	// Close destroys the RR instance, cleaning up resources.
	Close()
	// SetSlowdown sets the duration to wait before sending a response to a request. Useful for testing.
	SetSlowdown(duration time.Duration)
}

// NetWrapper contains means of communication with the network for the RR protocol. RR instances will use this struct's channels to send and receive messages to/from the corresponding remote. This allows the RR protocol to be oblivious to the underlying network implementation. Helpful for testing, in particular.
type NetWrapper struct {
	// The channel on which the RR instance will send messages to the remote.
	IntoNet chan<- []byte
	// The channel on which the RR instance will receive messages from the remote.
	FromNet <-chan []byte
}

type SendResponse = struct {
	Response []byte
	Err      error
}

// ActiveRequest represents a request that is waiting for a response.
// It tracks the request payload and provides a channel to deliver the response asynchronously.
type ActiveRequest = struct {
	Payload  []byte
	Response chan SendResponse
}

// Implementation of the RR interface. Hiding the implementation behind an interface allows for easier testing.
type rrImpl struct {
	// id of the rr
	id int64

	//slowdown time.Duration
	newSlowDown chan time.Duration

	// NetWrapper to communicate with the network
	netWrapper NetWrapper

	// timeout to send an error or retry if response takes too long
	timeout time.Duration

	// requestHandler for the incoming request
	newRequestHandler chan func([]byte) []byte

	// uidGenerator for message ids
	uidGenerator utils.UIDGenerator

	// logger of the RR
	logger *logging.Logger

	// receivedResponseChan is a channel to communicate between the "main" loop and the go routine that waits for an incoming response
	receivedResponseChan chan message

	// receivedRequestChan is a channel to communicate between the "main" loop and the go routines that awits for an incoming request
	receivedRequestChan chan message

	// activeRequestChan is a chennel to communicate between go routines on what the active request is, this can serve as a lock point for future request to be treated. What we want ? I think so
	activeRequestChan chan ActiveRequest

	// done is a channel to communicate between every goroutines to close themselves and stop what they're doing
	done chan struct{}
}

// NewRR creates a new RR instance.
func NewRR(log *logging.Logger, destinationAddr transport.Address, network NetWrapper) RR {
	rr := &rrImpl{
		id:                   time.Now().UnixNano(),
		newSlowDown:          make(chan time.Duration),
		netWrapper:           network,
		timeout:              time.Second * 5,
		logger:               log,
		receivedResponseChan: make(chan message),
		receivedRequestChan:  make(chan message),
		activeRequestChan:    make(chan ActiveRequest),
		done:                 make(chan struct{}),
		uidGenerator:         utils.NewUIDGenerator(),
		newRequestHandler:    make(chan func([]byte) []byte),
	}

	go rr.handleState()

	return rr
}

// handleReceivingRequest processes incoming requests from the receivedRequestChan.
// It handles request deduplication, applies slowdown for testing, and sends responses back to the network.
// The function maintains state about previous sequence numbers and payloads to handle duplicate requests.
func (rr *rrImpl) handleReceivingRequest() {
	var requestHandler func([]byte) []byte
	var slowDown time.Duration
	var previousSeqnum seqnum
	var previousPayload []byte
	for {
		select {
		case req := <-rr.receivedRequestChan:
			rr.logger.Info("Received a request")
			if slowDown > 0 {
				time.Sleep(slowDown)
			}

			if req.Seqnum.LessThan(previousSeqnum) {
				rr.logger.Info("Received an old request, ignoring it")
				continue
			}
			// Handle duplicate or old requests by comparing sequence numbers
			if req.Seqnum == previousSeqnum {
				rr.logger.Info("Received a duplicate request, resending the previous payload")
			} else {
				// New request - process it and store the result
				previousSeqnum = req.Seqnum
				previousPayload = req.Payload
				if requestHandler != nil {
					previousPayload = requestHandler(previousPayload)
				}
			}

			message := message{
				Type:    rspMsg,
				Seqnum:  previousSeqnum,
				Payload: previousPayload,
			}

			rr.logger.Info("Message for sending response ready: ", message.String())

			response, err := message.encode()
			if err != nil {
				rr.logger.Error(err)
			}

			// Write to the Network
			select {
			case rr.netWrapper.IntoNet <- response:
			case <-rr.done:
				rr.logger.Warn("Received a close signal while sending a response")
			}

		case rH := <-rr.newRequestHandler:
			requestHandler = rH

		case sD := <-rr.newSlowDown:
			slowDown = sD
		case <-rr.done:
			rr.logger.Warn("RR instance is shutting down, sending a response")
			return
		}
	}
}

// handleState is the main event loop that coordinates the RR protocol.
// It listens for incoming network messages, decodes them, and routes them to the appropriate handlers
// based on message type (request or response). It also manages the lifecycle of subsidiary goroutines.
func (rr *rrImpl) handleState() {
	rr.logger.Info("Starting state handling")

	go rr.handleSendingRequest()
	go rr.handleReceivingRequest()

	for {
		select {
		case messageRaw := <-rr.netWrapper.FromNet:
			rr.logger.Info("Received message from the network: ", messageRaw)
			messageDecoded, err := decode(messageRaw)
			if err != nil {
				rr.logger.Error(err)
			}

			switch messageDecoded.Type {
			case rspMsg:
				rr.logger.Info("Received a response: ", messageDecoded.String())
				rr.receivedResponseChan <- *messageDecoded
			case reqMsg:
				rr.logger.Info("Received a request: ", messageDecoded.String())
				rr.receivedRequestChan <- *messageDecoded
			}
		case <-rr.done:
			rr.logger.Warn("RR state handling shutting down ")
			return
		}
	}

}

func (rr *rrImpl) SetSlowdown(duration time.Duration) {
	rr.newSlowDown <- duration
}

func (rr *rrImpl) Close() {
	close(rr.done)
}

// handleSendingRequest manages the sending of requests and waiting for responses.
// It handles request timeouts, retransmissions, and response processing.
// The function generates unique sequence numbers for each request and ensures
// proper handling of responses while filtering out old or duplicate responses.
func (rr *rrImpl) handleSendingRequest() {
	for {
		select {
		case activeRequest := <-rr.activeRequestChan:
			var seqnumber = seqnum{
				InstID: rr.id,
				MsgID:  <-rr.uidGenerator,
			}

			message := message{
				Type:    reqMsg,
				Seqnum:  seqnumber,
				Payload: activeRequest.Payload,
			}
			rr.logger.Info("Message for sending request ready: ", message.String())

			request, err := message.encode()
			if err != nil {
				rr.logger.Error(err)
			}

			// Write to the Network
			select {
			case rr.netWrapper.IntoNet <- request:
			case <-rr.done:
				rr.logger.Warn("RR instance is shutting down, sending a request")
				return
			}

			// Wait for response with timeout and retry logic
		waitForResponse:
			for {

				select {
				case <-time.After(rr.timeout):
					rr.logger.Info("Timed out waiting for response: " + message.String())
					select {
					case rr.netWrapper.IntoNet <- request:
					case <-rr.done:
						return
					}
					continue

				case response := <-rr.receivedResponseChan:
					// Filter out old responses by comparing sequence numbers
					if response.Seqnum.LessThan(seqnumber) {
						rr.logger.Info("Ignoring old response")
						continue
					}

					rr.logger.Info("Sending response: ", response.String())
					activeRequest.Response <- SendResponse{Response: response.Payload, Err: nil}
					break waitForResponse

				case <-rr.done:
					rr.logger.Warn("RR received done signal while waiting for a response")
					activeRequest.Response <- SendResponse{Response: nil, Err: errors.New("RR closed while waiting for a response")}
					return
				}
			}
		}
	}
}

// SendRequest sends a request to the associated remote and returns a channel that will receive the response.
// The method is non-blocking - it returns immediately with a channel that will eventually contain the response.
// If the RR instance is closed during the operation, an error response is returned.
func (rr *rrImpl) SendRequest(payload []byte) <-chan SendResponse {
	responseChan := make(chan SendResponse)
	select {
	case rr.activeRequestChan <- ActiveRequest{payload, responseChan}: // This induce order I think
		rr.logger.Info("Sent a request: ", payload)
	case <-rr.done:
		rr.logger.Warn("RR closed while sending a request")
		sendResponseChan := make(chan SendResponse, 1)
		sendResponseChan <- SendResponse{nil, errors.New("RR closed while sending a request")}
		return sendResponseChan
	}

	return responseChan
}

func (rr *rrImpl) SetRequestHandler(handleRequest func([]byte) []byte) {
	rr.newRequestHandler <- handleRequest
}
