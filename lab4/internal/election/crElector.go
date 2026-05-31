package election

import (
	"chatsapp/internal/election/ring"
	"chatsapp/internal/logging"
	"chatsapp/internal/server/dispatcher"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils/option"
)

// Ability represents the ability of a process. The higher the ability, the more likely it is to be elected as leader.
type Ability = int

type address = transport.Address

// Elector is an interface for an elector, which is responsible for electing a leader among a group of processes, based on their abilities.
//
// It guarantees that all processes will eventually agree on the same leader, and that the leader will be the one with the highest ability.
type Elector interface {
	// GetLeader returns the current leader of the group. May be blocking if an election is ongoing or required to determine the leader.
	GetLeader() address
	// UpdateAbility updates the ability of this process, and starts a new election. May be blocking if an election is ongoing.
	UpdateAbility(ability Ability)
}

// A goroutine may request the leader from the main goroutine. This structure provides the channel on which the main goroutine will send the leader.
type leaderRequest struct {
	response chan<- address
}

// Local implementation of an elector, based on the Chang-Roberts algorithm.
type crElector struct {
	logger *logging.Logger

	self address

	ring ring.Maintainer

	updateAbility  chan Ability
	leaderRequest  chan leaderRequest
	receivedAdvert chan announcementMessage
	receivedResult chan resultMessage
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
func NewCRElector(logger *logging.Logger, self address, dispatcher dispatcher.Dispatcher, ringAddrs []address) Elector {
	ringMgr := ring.NewRingMaintainer(logger.WithPostfix("ring"), dispatcher, self, ringAddrs)
	return newCRElector(logger, self, ringMgr)
}

// Creates a new crElector instance. This function is used to facilitate testing.
func newCRElector(logger *logging.Logger, self address, ring ring.Maintainer) Elector {
	e := crElector{
		logger,
		self,
		ring,
		make(chan Ability),
		make(chan leaderRequest),
		make(chan announcementMessage),
		make(chan resultMessage),
	}

	resultMessage{}.RegisterToGob()
	announcementMessage{}.RegisterToGob()

	go e.handleRingMessages()
	go e.handleLeader()

	return &e
}

// Handles the reception of messages from the ring.
func (e *crElector) handleRingMessages() {
	for {
		msg := e.ring.ReceiveFromPrev()
		switch m := msg.(type) {
		case announcementMessage:
			e.receivedAdvert <- m
		case resultMessage:
			e.receivedResult <- m
		}
	}
}

// Main goroutine for the elector. It handles the state of who the leader is, and executes the election process.
func (e *crElector) handleLeader() {
	leader := option.None[address]()
	ability := 0

	for {
		e.logger.Infof("handleLeader ready to handle new event")
		select {
		case newAbility := <-e.updateAbility:
			e.logger.Info("Updating ability to ", newAbility, "; starting new election")
			ability = newAbility
			leader = option.Some(e.launchNewElection(ability))
		case req := <-e.leaderRequest:
			e.logger.Info("Leader requested")
			if leader.IsNone() {
				leader = option.Some(e.launchNewElection(ability))
			}
			req.response <- leader.Get()
		case advert := <-e.receivedAdvert:
			e.logger.Info("Advert received while not in election")
			if advert.Contains(e.self) {
				e.logger.Info("Advert known; sending result without entering election")
				maxAddr, _ := advert.GetHighest()
				leader = option.Some(maxAddr)
				e.ring.SendToNext(resultMessage{leader.Get(), []address{e.self}})
			} else {
				advert = advert.WithParticipant(e.self, ability)
				e.ring.SendToNext(advert)
				leader = option.Some(e.handleElection(ability))
			}
		case result := <-e.receivedResult:
			e.logger.Info("Result received while not in election")
			if !result.Contains(e.self) {
				if leader.IsNone() || result.Leader != leader.Get() {
					e.logger.Info("Leader different from mine; starting new election")
					leader = option.Some(e.launchNewElection(ability))
				} else {
					e.logger.Info("Leader same as mine; propagating result")
					e.ring.SendToNext(result.WithParticipant(e.self))
				}
			} else {
				e.logger.Info("Present in result; ignoring")
			}
		}
	}
}

// Handles the election process. Blocks and returns the new leader once the election is complete.
//
// This function is *not* responsible for *starting* an election, only for handling one that is ongoing. Use [crElector.launchNewElection] for an election that must also be started.
func (e *crElector) handleElection(currentAbility Ability) (newLeader address) {
	e.logger.Info("Entered election")
	defer e.logger.Info("Exited election")

	for {
		select {
		case advert := <-e.receivedAdvert:
			e.logger.Info("Advert received while in election")
			if advert.Contains(e.self) {
				newLeader, _ = advert.GetHighest()
				e.logger.Infof("Present in advert, new leader is %v; sending result and exiting election", newLeader)
				e.ring.SendToNext(resultMessage{newLeader, []address{e.self}})
				return
			}
			e.logger.Info("Not present in advert; forwarding with self")
			advert = advert.WithParticipant(e.self, currentAbility)
			e.ring.SendToNext(advert)
		case result := <-e.receivedResult:
			e.logger.Info("Received result while in election")
			if !result.Contains(e.self) {
				e.logger.Infof("Not present in result; updating leader to %v, sending result and exiting election", result.Leader)
				newLeader = result.Leader
				e.ring.SendToNext(result.WithParticipant(e.self))
				return
			}
			e.logger.Info("Present in result; ignoring that result")
		}
	}
}

// Starts a new election. Blocks during the election process and returns the leader once complete.
func (e *crElector) launchNewElection(currentAbility Ability) (newLeader address) {
	e.logger.Info("Starting new election by sending advert")
	adv := announcement{currentAbility, e.self}
	e.ring.SendToNext(announcementMessage{[]announcement{adv}})
	return e.handleElection(currentAbility)
}

func (e *crElector) GetLeader() address {
	response := make(chan address)
	e.leaderRequest <- leaderRequest{response}
	return <-response
}

func (e *crElector) UpdateAbility(ability int) {
	e.updateAbility <- ability
}
