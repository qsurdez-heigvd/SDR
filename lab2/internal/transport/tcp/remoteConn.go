package tcp

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/utils"
	"chatsapp/internal/utils/option"
)

type remoteConn struct {
	logger *logging.Logger

	selfAddr   address
	remoteAddr address

	closedCodec chan<- address
	toRemote    *utils.BufferedChan[sendRequest]
	fromRemote  chan message

	newCodec chan connCodec
}

func newRemoteConn(logger *logging.Logger, selfAddr address, remoteAddress address, fromRemote chan message, closedCodec chan<- address) *remoteConn {
	toRemote := utils.NewBufferedChan[sendRequest]()
	newCodec := make(chan connCodec)

	conn := &remoteConn{
		logger:      logger,
		selfAddr:    selfAddr,
		remoteAddr:  remoteAddress,
		closedCodec: closedCodec,
		toRemote:    toRemote,
		fromRemote:  fromRemote,
		newCodec:    newCodec,
	}

	go conn.handleSelf()

	return conn
}

func (rc *remoteConn) handleSelf() {
	messageToSend := option.None[sendRequest]()

	for {
		codec, ok := <-rc.newCodec
		if !ok {
			rc.logger.Infof("New codec channel closed, closing connection to %s", rc.remoteAddr)
			// No need to notify, since the parent is the one that asked to close
			//rc.closedCodec <- rc.remoteAddr
			// Notify receiver to close
			// Since close request comes from user, we don't expect a new codec to be set, so we just close
			return
		}

		// Lets this func notify the receiver goroutine to close
		closeReceiver := make(chan struct{})

		// Read from codec into fromRemote channel
		go func() {
			for {
				rc.logger.Infof("Waiting for message from codec for %s", rc.remoteAddr)
				msg, err := codec.Receive()
				rc.logger.Infof("Received message from codec for %s: %+v (err: %v)", rc.remoteAddr, msg, err)

				if err != nil {
					rc.logger.Warnf("Error receiving message from %s: %s. Closing connection and waiting for new codec", rc.remoteAddr, err)
					rc.closedCodec <- rc.remoteAddr
					return
				}
				rc.fromRemote <- msg

				select {
				case <-closeReceiver:
					// Notification from the main goroutine to close
					return
				default:
				}
			}
		}()

		// Write from toRemote channel into codec
		for {
			// If there is a message to send, try
			if !messageToSend.IsNone() {
				req := messageToSend.Get()
				err := codec.SendMessage(message{Source: rc.selfAddr, Payload: req.payload})
				req.err <- err
				if err != nil {
					rc.logger.Warnf("Error sending message to %s: %s. Closing connection and waiting for new codec", rc.remoteAddr, err)
					rc.closedCodec <- rc.remoteAddr
					codec.Close()
					// Exit listening loop and wait for new codec
					break
				}
			}

			// If it was successful, pull another message to send
			select {
			case req, ok := <-rc.toRemote.Outlet():
				if !ok {
					rc.logger.Warnf("toRemote channel closed, closing connection to %s", rc.remoteAddr)
					// No need to notify, since the parent is the one that asked to close
					//rc.closedCodec <- rc.remoteAddr
					codec.Close()
					// Notify receiver to close
					close(closeReceiver)
					// Since close request comes from user, we don't expect a new codec to be set, so we just close
					return
				}
				messageToSend = option.Some(req)
			}
		}
	}
}
