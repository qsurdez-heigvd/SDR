package rr

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils"
	"encoding/gob"
	"fmt"
	"time"
)

// Sequence number
type seqnum struct {
	// ID of the RR instance that generated that sequence number. Could be UNIX time in milliseconds, for example.
	InstID int64
	// ID of the message associated with that sequence number. Must be unique in the context of the RR instance identified by InstId.
	MsgID int64
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

// Represents an internal request to send a Request-type message.
type sendRequest struct {
	payload  []byte
	response chan SendResponse
}

// Implementation of the RR interface. Hiding the implementation behind an interface allows for easier testing.
type rrImpl struct {
	creationTime int64 // nanoseconds since epoch
	nextSeqnum   utils.UIDGenerator

	logger   *logging.Logger
	slowdown time.Duration

	destinationAddr transport.Address
	network         NetWrapper

	onRequest func([]byte) []byte

	sendRequests      chan sendRequest
	receivedRequests  chan *message
	receivedResponses chan *message

	closeChan chan struct{}
}

// NewRR creates a new RR instance.
//   - log: The logger to use for logging messages.
//   - destinationAddr: The address of the remote to associate with this RR instance.
//   - networkInterface: The network interface to use for sending and receiving messages.
func NewRR(log *logging.Logger, destinationAddr transport.Address, network NetWrapper) RR {
	log.Infof("Creating new RR for %v", destinationAddr)

	rr := rrImpl{
		creationTime:    time.Now().UnixNano(),
		logger:          log,
		destinationAddr: destinationAddr,
		nextSeqnum:      utils.NewUIDGenerator(),
		network:         network,
		closeChan:       make(chan struct{}),
	}

	gob.Register(message{})

	rr.sendRequests = make(chan sendRequest)
	rr.receivedRequests = make(chan *message)
	rr.receivedResponses = make(chan *message)

	go rr.handleSendRequests()
	go rr.handleReceiveRequests()
	go rr.dispatchFromNetwork()

	return &rr
}

func (rr *rrImpl) dispatchFromNetwork() {
	for {
		select {
		case <-rr.closeChan:
			rr.logger.Warn("Killing RR's fromNet receptor")
			return
		case msg := <-rr.network.FromNet:

			message, err := decode(msg)
			if err != nil {
				rr.logger.Errorf("Error decoding message: %s. Ignoring", err)
				continue
			}

			rr.logger.Infof("RR received message of type %v (%v), handling it as such...", message.Type, message.Seqnum)

			switch message.Type {
			case reqMsg:
				rr.receivedRequests <- message
			case rspMsg:
				rr.receivedResponses <- message
			}
		}
	}
}

func (rr *rrImpl) sendToNet(bytes []byte) {
	rr.network.IntoNet <- bytes
}

func (rr *rrImpl) SetSlowdown(duration time.Duration) {
	rr.slowdown = duration
}

func (rr *rrImpl) Close() {
	rr.logger.Warn("Closing RR")
	close(rr.closeChan)
}

// Main goroutine for sending requests. It sends requests to the remote address and waits for a response.
func (rr *rrImpl) handleSendRequests() {
	<-rr.nextSeqnum // Skip 0

	var n seqnum

	rr.logger.Info("Starting handleSendRequests")

	for {
		select {
		case <-rr.closeChan:
			rr.logger.Warn("Killing handleSendRequests")
			return
		case response := <-rr.receivedResponses:
			rr.logger.Warnf("Received response for seqnum %v while not waiting any response. Ignoring.", response.Seqnum)
		case sendRequest := <-rr.sendRequests:
			rr.logger.Info("Received send request")

			nextMsgID := <-rr.nextSeqnum
			n = seqnum{rr.creationTime, int64(nextMsgID)}

			log := rr.logger.WithPostfix(fmt.Sprintf("(%v)", n))

			message := message{
				Type:    reqMsg,
				Seqnum:  n,
				Payload: sendRequest.payload,
			}

			bytes, err := message.encode()
			if err != nil {
				log.Error("Error encoding message:", err)
				continue
			}

			log.Infof("Sending request with seqnum %v to %v", n, rr.destinationAddr)
			rr.sendToNet(bytes)

			log.Info("waiting for response for seqnum", n)
			tryAgain := true
			for tryAgain {
				select {
				case <-rr.closeChan:
					log.Warn("Killing one of handleSendRequests chan's response handler")
					return
				case response := <-rr.receivedResponses:
					if response.Seqnum == n {
						log.Infof("Received response for seqnum %v", n)
						sendRequest.response <- SendResponse{response.Payload, nil}
						tryAgain = false
					} else {
						log.Warn("Warning: Received response with seqnum", response.Seqnum, "different from last seqnum", n, ". Ignoring.")
					}
				case <-time.After(500 * time.Millisecond):
					log.Warn("timeout for seqnum ", n)
					rr.sendToNet(bytes)
				}
			}

			log.Infof("Finished sending and acking msg with seqnum %v", n)
		}
	}
}

// Main goroutine for receiving requests. It processes incoming requests and sends responses.
func (rr *rrImpl) handleReceiveRequests() {
	lastSeqnum := seqnum{0, -1}
	lastResponse := []byte(nil)

	for {
		rr.logger.Infof("Waiting for requests. Last seqnum: %v", lastSeqnum)
		select {
		case <-rr.closeChan:
			rr.logger.Warn("Killing handleReceiveRequests")
			return
		case msg := <-rr.receivedRequests:
			log := rr.logger.WithPostfix(fmt.Sprintf("(%v)", msg.Seqnum))

			log.Info("Received request with seqnum", msg.Seqnum)
			seqnum := msg.Seqnum

			if rr.slowdown > 0 {
				time.Sleep(rr.slowdown)
				select {
				case <-rr.closeChan:
					log.Warn("Killing handleReceiveRequests")
					return
				default:
				}
			}

			if seqnum == lastSeqnum {
				if lastResponse != nil {
					log.Info("Request already processed. Resending Response.")
					// Resend response
					rr.sendToNet(lastResponse)
				} else {
					log.Warn("Request already received, but no known response. Ignoring")
				}
			} else if lastSeqnum.LessThan(seqnum) {
				log.Info("Request is new. Processing...")
				// Handle request
				lastResponse = nil
				lastSeqnum = seqnum

				if rr.onRequest == nil {
					log.Error("Warning: No onRequest handler set. Ignoring request.")
					continue
				}
				response := rr.onRequest(msg.Payload)
				if response == nil {
					log.Info("onRequest handler returned nil response. Responding with empty payload.")
				}

				rrResponse := message{
					Type:    rspMsg,
					Seqnum:  msg.Seqnum,
					Payload: response,
				}

				bytes, err := rrResponse.encode()
				if err != nil {
					log.Error("Error encoding response:", err)
					panic(err)
				}

				lastResponse = bytes

				rr.sendToNet(bytes)
			} else {
				log.Warn("Warning: Received request with seqnum", seqnum, "less than last seqnum", lastSeqnum)
			}

			log.Infof("Finished processing request with seqnum %v", seqnum)
		}
	}
}

func (rr *rrImpl) SendRequest(payload []byte) <-chan SendResponse {
	responseChan := make(chan SendResponse, 1)

	rr.logger.Infof("TCP requests to send message to %v", rr.destinationAddr)

	rr.sendRequests <- sendRequest{
		payload:  payload,
		response: responseChan,
	}

	return responseChan
}

func (rr *rrImpl) SetRequestHandler(handleRequest func([]byte) []byte) {
	if rr.onRequest != nil {
		rr.logger.Warn("Warning: Overwriting existing onRequest handler")
	}
	rr.onRequest = handleRequest
}
