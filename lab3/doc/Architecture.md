# Software Architecture Document

## Ring Maintainer

The ring maintainer leverages Go's `container/ring.Ring` data structure to simplify server management according to a
predefined order while enabling circular traversal. The ring is initialized based on the list provided in the
constructor. When sending messages, the implementation verifies that the ring is properly positioned on the current
server to ensure messages are sent to the next server in sequence, with the current server processed last.

### Goroutines

The ring maintainer employs two goroutines for concurrent operation:

#### 1. `handleSendingMessages`

This goroutine manages the internal state of the ring maintainer:

- Maintains the ring of servers using `container/ring.Ring`
- Manages a Lamport timestamp handler for outgoing message identification
- Handles two types of requests via dedicated channels:
    - **Send requests** (`toSend`): Creates a ring message with timestamp and forwards to the sending goroutine
    - **Receive requests** (`requestForPrev`): Spawns a new goroutine to wait for and deliver the next incoming payload

The goroutine ensures the ring is properly positioned on the current server before any operations. For send operations,
a copy of the ring is passed to the sending goroutine, allowing iteration during failures without affecting the ring
position for other messages.

#### 2. `sendMessages`

This goroutine handles the actual transmission of outgoing messages:

- Processes messages from the `outgoingMessages` buffered channel
- Implements the timeout and retry mechanism per the algorithm specification
- Sends the message to the next server in the ring
- Waits for ACK or timeout:
    - On timeout: advances to the next server in the ring and retries
    - On ACK: verifies timestamp matches before completing the send operation
- Only one message is processed at a time, ensuring proper ACK handling

### Channels

The ring maintainer uses several channels for inter-goroutine communication:

| Channel                 | Type                                 | Purpose                                                              |
|-------------------------|--------------------------------------|----------------------------------------------------------------------|
| `toSend`                | `chan messages.Message`              | Request to send a message to the next node                           |
| `requestForPrev`        | `chan awaitingPayload`               | Request to wait for a message, includes response channel             |
| `fromDispatcherAck`     | `chan messages.Sourced[message]`     | ACK messages from dispatcher, handled by `sendMessages`              |
| `fromDispatcherPayload` | `chan messages.Sourced[message]`     | Payload messages from dispatcher, handled by `handleSendingMessages` |
| `outgoingMessages`      | `*utils.BufferedChan[messageToSend]` | Outgoing messages from `handleSendingMessages` to `sendMessages`     |

## Chang-Roberts Elector

The Chang-Roberts elector builds upon the `ring.RingMaintainer` abstraction without introducing additional layers. It
implements the Chang-Roberts leader election algorithm with ability-based priorities.

### Goroutines

The elector utilizes two goroutines for its operation:

#### 1. `handleRingMessages`

Started in the constructor, this goroutine continuously receives messages from the `ring.RingMaintainer` and routes them
to dedicated channels based on message type:

- **Announcement messages**: Routed to `incAnnouncement`
- **Result messages**: Routed to `incResult`
- Unknown message types are logged and ignored

#### 2. `handleState`

This is the main goroutine of the elector, managing the complete election state and implementing the Chang-Roberts
algorithm logic. It maintains:

**Internal State:**

- `chosen`: Current leader (Option type, initially None)
- `selfAbility`: This process's ability value (initially 0)
- `inElection`: Boolean flag indicating if an election is in progress
- `electionWaiters`: List of callbacks to invoke when election completes

**Event Processing:**

The goroutine processes four types of events via select statement:

1. **Ability Updates** (`newAbility` channel):
    - If election in progress: reschedule request after election completion
    - Otherwise: update ability, signal completion, and start new election

2. **Leader Requests** (`getLeader` channel):
    - If no leader exists and not in election: initiate election
    - If not in election: return current leader immediately
    - If election in progress: add requester to waiters list

3. **Announcement Messages** (`incAnnouncement` channel):
    - If self is in participants list (full ring traversal):
        - Determine leader (participant with highest ability)
        - Send result message with self as participant
        - End election with new leader
    - Otherwise:
        - Add self to participants with current ability
        - Forward announcement to next node
        - Set election state to active

4. **Result Messages** (`incResult` channel):
    - If self is in participants list: ignore (already seen)
    - If not in election and result has new leader:
        - Start new election (leader mismatch detected)
    - Otherwise:
        - Accept result and update leader
        - Add self to participants and forward
        - End election

### Channels

The `handleState` goroutine uses the following channels to represent algorithm events:

| Channel           | Type                                        | Purpose                                                      |
|-------------------|---------------------------------------------|--------------------------------------------------------------|
| `getLeader`       | `chan chan<- address`                       | Retrieve current leader; may trigger election if none exists |
| `newAbility`      | `*utils.BufferedChan[abilityUpdateRequest]` | Update ability and trigger new election                      |
| `incAnnouncement` | `chan announcementMessage`                  | Incoming announcement from previous node                     |
| `incResult`       | `chan resultMessage`                        | Incoming result from previous node                           |

### Election Behavior

**Starting an Election:**

- Set `inElection` flag to true
- Send announcement message with self as first participant

**Ending an Election:**

- Set `inElection` flag to false
- Store new leader in `chosen`
- Notify all queued waiters with the new leader
- Clear the waiters list

## Client Management

The elector is integrated into the server system to manage client connections and leader selection.

### Connection Handling

When a client requests connection, the server queries the current leader:

```go
leader := m.elector.GetLeader()
m.send(common.ConnResponseMessage{
Leader: leader,
}, source)
```

### Client Registration

Clients are only added to the server's client map if the current server is the leader. Upon successful registration, the
server updates its ability:

```go
if leader == m.self {
clients[source] = clientConnection{
source: source,
user:   clientMsg.User,
}
m.elector.UpdateAbility(-len(clients))
m.logger.Infof("Client %s (%s) connected", source, clientMsg.User)
}
```

### Ability Definition

> **Note**: In the context of this system, a server's ability is defined as the negative of its client count. This
> prioritizes servers with fewer connected clients, as they will have higher ability values and thus higher priority
> during leader election.

### Disconnection Handling

When a client disconnects, the ability is updated using the same mechanism, effectively increasing the server's
ability (making it more attractive for future leadership).
