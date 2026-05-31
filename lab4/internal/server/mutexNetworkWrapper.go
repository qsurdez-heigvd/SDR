package server

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/messages"
	"chatsapp/internal/mutex"
	"chatsapp/internal/server/dispatcher"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils"
)

// Constructs a new [mutexMsgPassingWrapper] instance.
func newMutexNetworkWrapper(log *logging.Logger, d dispatcher.Dispatcher) mutex.NetWrapper {
	netToMutex := utils.NewBufferedChan[mutex.Message]()
	mutexToNet := utils.NewBufferedChan[mutex.OutgoingMessage]()

	wrapper := mutex.NetWrapper{
		IntoNet: mutexToNet.Inlet(),
		FromNet: netToMutex.Outlet(),
	}

	d.Register(mutex.Message{}, func(msg messages.Message, source transport.Address) {
		if mutexMsg, ok := msg.(mutex.Message); ok {
			netToMutex.Inlet() <- mutexMsg
		} else {
			log.Error("Received a message that is not a mutex.Message")
		}
	})

	go func() {
		for msg := range mutexToNet.Outlet() {
			if msg.Destination.IsNone() {
				// Broadcast message
				d.Broadcast(msg.Message)
			} else {
				dst := msg.Destination.Get()
				addr, err := transport.NewAddress(string(dst))
				if err != nil {
					log.Error("Error creating address from pid:", err)
					return
				}
				d.Send(msg.Message, addr)
			}

		}
	}()

	return wrapper
}
