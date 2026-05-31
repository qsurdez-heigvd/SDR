package election

import (
	"chatsapp/internal/election/ring"
	"chatsapp/internal/logging"
	"chatsapp/internal/server/dispatcher"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils"
	"chatsapp/internal/utils/option"
)

// Ability represents the ability of a process. The higher the ability, the more likely it is to be elected as leader.
type Ability = int

type address = transport.Address

type abilityUpdateRequest struct {
	ability Ability
	done    chan struct{}
}

// Elector is an interface for an elector, which is responsible for electing a leader among a group of processes, based on their abilities.
//
// It guarantees that all processes will eventually agree on the same leader, and that the leader will be the one with the highest ability.
type Elector interface {
	// GetLeader returns the current leader of the group. May be blocking if an election is ongoing or required to determine the leader.
	GetLeader() address
	// UpdateAbility updates the ability of this process, and starts a new election. May be blocking if an election is ongoing.
	UpdateAbility(ability Ability)
}

// Local implementation of an elector, based on the Chang-Roberts algorithm.
type crElector struct {
	logger *logging.Logger

	self address
	ring ring.Maintainer

	// Incoming messages from the ring
	incAnnouncement chan announcementMessage
	incResult       chan resultMessage

	// Requests from the application-facing API
	getLeader  chan chan<- address
	newAbility *utils.BufferedChan[abilityUpdateRequest]
}

/*
NewCRElector constructs and returns a new Chang-Roberts elector.

Parameters
  - logger: The logger to use for logging messages.
  - self: The address of the current process.
  - dispatcher: The dispatcher to use for sending messages to processes in the network.
  - ringAddrs: A slice of addresses, the order of which represents the ring. Note that [self] *must* appear in the slice, though it may appear in any position.

Returns:
  - A new Chang-Roberts elector instance.

Note that the elector will start running in the background as soon as it is created.
*/
func NewCRElector(
	logger *logging.Logger,
	self address,
	dispatcher dispatcher.Dispatcher,
	ringAddrs []address,
) Elector {
	ringMaintainer := ring.NewRingMaintainer(
		logger.WithPostfix("ring"),
		dispatcher,
		self,
		ringAddrs,
	)
	return newCRElector(logger, self, ringMaintainer)
}

// Creates a new crElector instance. This function is used to facilitate testing.
func newCRElector(
	logger *logging.Logger,
	self address,
	ring ring.Maintainer,
) Elector {
	announcementMessage{}.RegisterToGob()
	resultMessage{}.RegisterToGob()

	cre := &crElector{
		logger:          logger,
		self:            self,
		ring:            ring,
		incAnnouncement: make(chan announcementMessage),
		incResult:       make(chan resultMessage),
		getLeader:       make(chan chan<- address),
		newAbility:      utils.NewBufferedChan[abilityUpdateRequest](),
	}

	go cre.handleRingMessages()
	go cre.handleState()

	return cre
}

// GetLeader blocks if an election is ongoing, then returns the current leader of the group.
func (c *crElector) GetLeader() address {
	response := make(chan address)
	c.getLeader <- response
	return <-response
}

// UpdateAbility updates the ability of this process, and starts a new election.
func (c *crElector) UpdateAbility(ability Ability) {
	done := make(chan struct{})
	c.newAbility.Inlet() <- abilityUpdateRequest{ability, done}
	<-done
}

// handleRingMessages listens for incoming messages from the ring and dispatches them to the appropriate handlers.
func (c *crElector) handleRingMessages() {
	for {
		msg := c.ring.ReceiveFromPrev()
		switch msg := msg.(type) {
		case announcementMessage:
			c.logger.Infof("Received announcement message: %+v", msg)
			c.incAnnouncement <- msg
		case resultMessage:
			c.logger.Infof("Received result message: %+v", msg)
			c.incResult <- msg
		default:
			c.logger.Warnf("Received unexpected message type %T from ring, ignoring", msg)
		}
	}
}

// handleState is the main goroutine of the elector, it manages the inner state
// and handles incoming requests from the application-facing API and the ring.
func (c *crElector) handleState() {
	chosen := option.None[address]()
	selfAbility := 0
	inElection := false

	// Queue for requests that arrive during an election
	electionWaiters := make([]func(leader address), 0)

	// Define helpful closures for the election process
	startElection := func() {
		inElection = true
		c.logger.Infof("Starting new election with ability %v", selfAbility)
		c.ring.SendToNext(announcementMessage{
			Participants: []announcement{{Ability: selfAbility, Addr: c.self}},
		})
	}

	endElection := func(leader address) {
		inElection = false
		chosen = option.Some(leader)
		c.logger.Infof("Election completed. Leader is now %v with %d waiters to notify",
			leader, len(electionWaiters))

		// As an addition to the algorithm, we notify all
		// the election waiters of the newly acquired result
		for _, waiter := range electionWaiters {
			waiter(leader)
		}

		electionWaiters = electionWaiters[:0]
	}

	for {
		select {
		// 1. Election requests, through ability updates
		case req := <-c.newAbility.Outlet():
			// 1.a. If an election is ongoing, reschedule the request after the election
			if inElection {
				c.logger.Infof("Received ability update to %v during election, rescheduling after completion",
					req.ability)
				electionWaiters = append(electionWaiters, func(_ address) {
					c.newAbility.Inlet() <- req
				})
				continue
			}

			// 1.b. Update the ability
			c.logger.Infof("Updating ability from %v to %v and starting new election",
				selfAbility, req.ability)
			selfAbility = req.ability
			req.done <- struct{}{}

			// 1.c. Start a new election
			startElection()

		// 2. Leader requests from the application side
		case response := <-c.getLeader:
			// If we have no leader, we start an election now
			if !inElection && chosen.IsNone() {
				c.logger.Info("No leader exists, initiating election in response to leader request")
				startElection()
			}

			// If we are not in an election, we return the leader immediately,
			// otherwise we add the requester to the waiters list
			if !inElection {
				c.logger.Infof("Returning current leader %v to requester", chosen.Get())
				response <- chosen.Get()
			} else {
				c.logger.Info("Election in progress, queuing leader request")
				electionWaiters = append(electionWaiters, func(leader address) {
					response <- leader
				})
			}

		// 3. When receiving an announcement message
		case announcement := <-c.incAnnouncement:
			// 3.a. If self is in the participants list
			if announcement.Contains(c.self) {
				// We have gone through a full round of the ring
				// 3.a.i.   The leader is the participant with the highest ability
				newChosen, highestAbility := announcement.GetHighest()
				c.logger.Infof("Announcement completed ring traversal. Highest ability is %v from %v",
					highestAbility, newChosen)
				// 3.a.ii.  Start a result loop with the leader and us as participant
				c.ring.SendToNext(resultMessage{
					Leader:       newChosen,
					Participants: []address{c.self},
				})
				// 3.a.iii. Signal that the election is over
				endElection(newChosen)
			} else {
				// 3.b.i.   Add ourselves to the participants
				// 3.b.ii.  Forward the message to the next node in the ring
				c.logger.Infof("Adding self to announcement with ability %v", selfAbility)
				c.ring.SendToNext(announcement.WithParticipant(c.self, selfAbility))
				// 3.b.iii. Ensure the election state is up to date
				inElection = true
			}

		// 4. When receiving a result message
		case result := <-c.incResult:
			// 4.a. If self is in the participants list, ignore the message
			if result.Contains(c.self) {
				c.logger.Info("Ignoring result message we've already seen")
				continue
			}

			// 4.b. If we are not in an election, and the result leader is new
			if !inElection && (chosen.IsNone() || chosen.Get() != result.Leader) {
				// 4.b.i. We need to start a new election
				c.logger.Infof("Received result with new leader %v while not in election, starting new election",
					result.Leader)
				startElection()
			} else {
				// We can accept the result
				// 4.c.i.   Update the leader
				c.logger.Infof("Updating leader to %v and propagating result", result.Leader)
				// 4.c.ii.  Add ourselves to the participants
				// 4.c.iii. Forward the message to the next node in the ring
				c.ring.SendToNext(result.WithParticipant(c.self))
				// 4.c.iv.  Signal that the election is over
				endElection(result.Leader)
			}
		}
	}
}
