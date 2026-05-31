# Architecture

## Pulsar

### Goroutines

Within the Pulsar two goroutines are launched:

- `handleReceivedMessages` is in charge of dispatching the messages received from the Net interface according to their
  type to their respective channel (`ECHO` or `PULSE`)
- `handleEvents` is in charge to handle all the events according to the pulses and echoes algorithm

`handleEvents` will listen different channels that will receive
messages according to ongoing events. It's also the goroutine that
will change the state of the pulsar by manipulating the map of `PulseID, nodeState`.

This map will be used to send many pulses and echoes in parallels. The only goroutine responsible
for the map is `handleEvents`, thus it prevents race condition.

#### Channels

Channels are used to represent the different events within the pulsar:

| Channel             | Usage                                                                                                                |
|---------------------|----------------------------------------------------------------------------------------------------------------------|
| `receivedPulseChan` | Send received pulses from `handleReceivedMessages` to `handleEvents`                                                 |
| `receivedEchoChan`  | Send received echoes from `handleReceivedMessages` to `handleEvents`                                                 |
| `newPulseRequested` | Channel used to send the application request to start the algo                                                       |
| `echo`              | Aggregation of the echoes, returns its read value with `StartPulse`. This blocks until echo aggregation is complete. |

## Dispatcher

### Goroutines

One goroutine has been added:

- `handleRPMessages` passes messages received from the Router or Pulsar to the Net. It also handles, in the same way as
  `handleNetworkMessage`, verifying that received messages are processed.

#### Channels

| Channel            | Usage                                                                                                                                                                        |
|--------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `receivedMessages` | Used to pass received messages from the network to the `dispatch` goroutine for handler dispatching                                                                          |
| `registrations`    | Used to register new protocol handlers dynamically                                                                                                                           |
| `netToRouter`      | The `Inlet` side is used in the handler for `RoutedMessage` type messages to retrieve messages of this type, these messages are then read on the `Outlet` side by the router |
| `netToPulsar`      | Its usage is equivalent to `netToRouter` but for `PulsarMessage` type messages and therefore for the pulsar                                                                  |
| `routerToNet`      | Allows redirecting messages outgoing from the router to the Net in the `handleRPMessages` goroutine                                                                          |
| `pulsarToNet`      | Same as `routerToNet` but for the pulsar                                                                                                                                     |

## Router

### Goroutines

The Router uses 2 main goroutines:

1. **handleSendRequests**

This goroutine is responsible for processing message send requests (Send).

- If the destination is already known in the routing table (`routingTable`), the message is directly sent to
  `routerToNet`.
- If the destination is unknown, it starts a probe via the Pulsar to explore the network and update the routing table
  before attempting a new send.

This goroutine guarantees that each blocking call to Send completes only after routing resolution, while processing
other requests in parallel through the use of channels. In deed, it can do so concurrently thanks to the fact that each
probe is independent.

2. **handleIncomingMessage**

This goroutine handles messages received via the `netToRouter` channel.

- If the message is destined for the current process, it is transferred to the `receivedChan` channel for local
  processing.
- Otherwise, the message is retransmitted to the final destination via the routing table or triggers exploration if
  necessary.

This allows the Router to continue receiving and propagating messages, even in case of temporary blocking in other parts
of the network.

#### Channels

| Channel            | Usage                                                                                       |
|--------------------|---------------------------------------------------------------------------------------------|
| `sendRequestsChan` | Used to queue send requests for processing by the `handleSendRequests` goroutine            |
| `receivedChan`     | Used to deliver messages that have reached their final destination to the application layer |
| `routerToNet`      | Used to send routed messages to the network                                                 |
| `netToRouter`      | Used to receive routed messages from the network                                            |

