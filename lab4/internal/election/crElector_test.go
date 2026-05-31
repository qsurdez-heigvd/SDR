package election

import (
	"chatsapp/internal/logging"
	"chatsapp/internal/messages"
	"testing"
	"time"
)

type mockRingMaintainer struct {
	t *testing.T

	sentMessages        chan messages.Message
	simulatedReceptions chan messages.Message
}

func (m mockRingMaintainer) SendToNext(msg messages.Message) {
	m.sentMessages <- msg
}

func (m mockRingMaintainer) ReceiveFromPrev() messages.Message {
	return <-m.simulatedReceptions
}

func (m mockRingMaintainer) expectSentMessage(msg messages.Message) {
	sentMsg := <-m.sentMessages
	switch sentMsg.(type) {
	case announcementMessage:
		advSentMsg := sentMsg.(announcementMessage)
		if advMsg, ok := msg.(announcementMessage); ok {
			if len(advSentMsg.Participants) != len(advMsg.Participants) {
				m.t.Error("Expected message with ", len(advMsg.Participants), " participants, got ", len(advSentMsg.Participants))
			}
			for i, p := range advMsg.Participants {
				if advSentMsg.Participants[i] != p {
					m.t.Error("Expected participant ", p, " at index ", i, ", got ", advSentMsg.Participants[i])
				}
			}
		} else {
			m.t.Error("Expected AdvertMessage, got", msg)
		}
	case resultMessage:
		resSentMsg := sentMsg.(resultMessage)
		if resMsg, ok := msg.(resultMessage); ok {
			if resSentMsg.Leader != resMsg.Leader {
				m.t.Error("Expected leader ", resMsg.Leader, ", got ", resSentMsg.Leader)
			}
			if len(resSentMsg.Participants) != len(resMsg.Participants) {
				m.t.Error("Expected ", len(resMsg.Participants), " participants, got ", len(resSentMsg.Participants))
			}
			for i, p := range resMsg.Participants {
				if resSentMsg.Participants[i] != p {
					m.t.Error("Expected participant ", p, " at index ", i, ", got ", resSentMsg.Participants[i])
				}
			}
		} else {
			m.t.Error("Expected ResultMessage, got", msg)
		}
	}
}

func (m mockRingMaintainer) expectNothingFor(duration time.Duration) {
	select {
	case msg := <-m.sentMessages:
		m.t.Error("Expected no message, got one: ", msg)
	case <-time.After(duration):
	}
}

func (m mockRingMaintainer) simulateReception(msg messages.Message) {
	m.simulatedReceptions <- msg
}

func newMockRingMaintainer(t *testing.T) mockRingMaintainer {
	return mockRingMaintainer{
		t:                   t,
		sentMessages:        make(chan messages.Message, 100),
		simulatedReceptions: make(chan messages.Message, 100),
	}
}

var addrs = []address{
	{IP: "127.0.0.1", Port: 5000},
	{IP: "127.0.0.1", Port: 5001},
	{IP: "127.0.0.1", Port: 5002},
	{IP: "127.0.0.1", Port: 5003},
}

func TestAnnouncementWhileNotInElection(t *testing.T) {
	ring := newMockRingMaintainer(t)
	logger := logging.NewStdLogger("t")
	newCRElector(logger, addrs[0], ring)

	// Simulate announcement message
	ring.simulateReception(announcementMessage{[]announcement{
		{Ability: 10, Addr: addrs[1]},
	}})

	// Expect announcement to be propagated
	ring.expectSentMessage(announcementMessage{[]announcement{
		{Ability: 10, Addr: addrs[1]},
		{Ability: 0, Addr: addrs[0]},
	}})
}

func TestNewAnnouncementWhileInElection(t *testing.T) {
	ring := newMockRingMaintainer(t)
	logger := logging.NewStdLogger("t")
	e := newCRElector(logger, addrs[0], ring)

	// Update ability to trigger election
	e.UpdateAbility(10)

	// Expect an advertisement to be sent
	ring.expectSentMessage(announcementMessage{[]announcement{
		{Ability: 10, Addr: addrs[0]},
	}})

	// Simulate announcement message
	ring.simulateReception(announcementMessage{[]announcement{
		{Ability: 15, Addr: addrs[1]},
	}})

	// Expect announcement to be propagated
	ring.expectSentMessage(announcementMessage{[]announcement{
		{Ability: 15, Addr: addrs[1]},
		{Ability: 10, Addr: addrs[0]},
	}})
}

func TestCompleteAnnoncementRound(t *testing.T) {
	ring := newMockRingMaintainer(t)
	logger := logging.NewStdLogger("t")
	e := newCRElector(logger, addrs[0], ring)

	// Update ability to trigger election
	e.UpdateAbility(10)

	// Expect an advertisement to be sent
	ring.expectSentMessage(announcementMessage{[]announcement{
		{Ability: 10, Addr: addrs[0]},
	}})

	// Simulate the advertisement message having done a full round
	ring.simulateReception(announcementMessage{[]announcement{
		{Ability: 10, Addr: addrs[0]},
		{Ability: 15, Addr: addrs[1]},
	}})

	// Expect result to be sent
	ring.expectSentMessage(resultMessage{
		Leader:       addrs[1],
		Participants: []address{addrs[0]},
	})
}

func TestKnownResultWhileNotInElection(t *testing.T) {
	ring := newMockRingMaintainer(t)
	logger := logging.NewStdLogger("t")
	newCRElector(logger, addrs[0], ring)

	// Simulate result message
	ring.simulateReception(resultMessage{
		Leader:       addrs[1],
		Participants: []address{addrs[0]},
	})

	// Expect no message to be sent
	ring.expectNothingFor(1 * time.Second)
}

func TestUnknownResultWhileNotInElection(t *testing.T) {
	ring := newMockRingMaintainer(t)
	logger := logging.NewStdLogger("t")
	newCRElector(logger, addrs[0], ring)

	// Simulate result message
	ring.simulateReception(resultMessage{
		Leader:       addrs[1],
		Participants: []address{addrs[2]},
	})

	// Expect new election to be started
	ring.expectSentMessage(announcementMessage{[]announcement{
		{Ability: 0, Addr: addrs[0]},
	}})
}

func TestUnknownResultWhileInAnnouncement(t *testing.T) {
	ring := newMockRingMaintainer(t)
	logger := logging.NewStdLogger("t")
	newCRElector(logger, addrs[0], ring)

	// Simulate reception of announcement message
	ring.simulateReception(announcementMessage{[]announcement{
		{Ability: 10, Addr: addrs[1]},
	}})

	// Expect announcement to be propagated
	ring.expectSentMessage(announcementMessage{[]announcement{
		{Ability: 10, Addr: addrs[1]},
		{Ability: 0, Addr: addrs[0]},
	}})

	// Simulate result message
	ring.simulateReception(resultMessage{
		Leader:       addrs[1],
		Participants: []address{addrs[2]},
	})

	// Expect result to be propagated
	ring.expectSentMessage(resultMessage{
		Leader:       addrs[1],
		Participants: []address{addrs[2], addrs[0]},
	})

	ring.expectNothingFor(1 * time.Second)
}

func TestKnownResultWhileInAnnouncement(t *testing.T) {
	ring := newMockRingMaintainer(t)
	logger := logging.NewStdLogger("t")
	newCRElector(logger, addrs[0], ring)

	// Simulate reception of announcement message
	ring.simulateReception(announcementMessage{[]announcement{
		{Ability: 10, Addr: addrs[1]},
	}})

	// Expect announcement to be propagated
	ring.expectSentMessage(announcementMessage{[]announcement{
		{Ability: 10, Addr: addrs[1]},
		{Ability: 0, Addr: addrs[0]},
	}})

	// Simulate result message
	ring.simulateReception(resultMessage{
		Leader:       addrs[1],
		Participants: []address{addrs[0]},
	})

	// Expect no message to be sent
	ring.expectNothingFor(1 * time.Second)
}

func TestGetLeaderOnEmpty(t *testing.T) {
	//logger := logger.NewMockLogger("t")
	ring := newMockRingMaintainer(t)
	logger := logging.NewStdLogger("t")
	e := newCRElector(logger, addrs[0], ring)

	// Request leader at beginning when there is none
	leaderChan := make(chan address)
	go func() {
		leader := e.GetLeader()
		leaderChan <- leader
	}()

	// Expect a new election to start
	expectedMsg := announcementMessage{[]announcement{
		{Ability: 0, Addr: addrs[0]},
	}}
	ring.expectSentMessage(expectedMsg)

	// Expect no leader to be returned
	select {
	case leader := <-leaderChan:
		t.Error("Expected no leader until end of election, got ", leader)
	case <-time.After(1 * time.Second):
	}

	// Simulate the advertisement message having done a full round
	ring.simulateReception(announcementMessage{[]announcement{
		{Ability: 0, Addr: addrs[0]},
		{Ability: 10, Addr: addrs[1]},
	}})

	// Expect result to be sent
	ring.expectSentMessage(resultMessage{
		Leader:       addrs[1],
		Participants: []address{addrs[0]},
	})

	// Expect leader to be returned
	select {
	case leader := <-leaderChan:
		if leader != addrs[1] {
			t.Error("Expected leader to be ", addrs[1], ", got ", leader)
		}
	case <-time.After(1 * time.Second):
		t.Error("Expected leader to be returned")
	}
}

func TestUpdateAbility(t *testing.T) {
	ring := newMockRingMaintainer(t)
	logger := logging.NewStdLogger("t")
	e := newCRElector(logger, addrs[0], ring)

	// Update ability
	e.UpdateAbility(10)

	// Expect an advertisement to be sent
	expectedMsg := announcementMessage{[]announcement{
		{Ability: 10, Addr: addrs[0]},
	}}
	ring.expectSentMessage(expectedMsg)

	// Simulate the advertisement message having done a full round
	ring.simulateReception(announcementMessage{[]announcement{
		{Ability: 10, Addr: addrs[0]},
		{Ability: 0, Addr: addrs[1]},
	}})

	// Expect result to be sent
	ring.expectSentMessage(resultMessage{
		Leader:       addrs[0],
		Participants: []address{addrs[0]},
	})

	// Request leader; should return immediately
	leader := e.GetLeader()

	if leader != addrs[0] {
		t.Error("Expected leader to be ", addrs[0], ", got ", leader)
	}
}

func TestUpdateAbilityDuringElection(t *testing.T) {
	ring := newMockRingMaintainer(t)
	logger := logging.NewStdLogger("t")
	e := newCRElector(logger, addrs[0], ring)

	// Update ability to trigger election
	e.UpdateAbility(10)

	go func() {
		// Update ability during election
		e.UpdateAbility(20)
	}()

	// Expect an advertisement to be sent
	ring.expectSentMessage(announcementMessage{[]announcement{
		{Ability: 10, Addr: addrs[0]},
	}})

	// Simulate the advertisement message having done a full round
	ring.simulateReception(announcementMessage{[]announcement{
		{Ability: 10, Addr: addrs[0]},
		{Ability: 0, Addr: addrs[1]},
	}})

	// Expect result to be sent
	ring.expectSentMessage(resultMessage{
		Leader:       addrs[0],
		Participants: []address{addrs[0]},
	})

	// Expect new election to be started
	ring.expectSentMessage(announcementMessage{[]announcement{
		{Ability: 20, Addr: addrs[0]},
	}})
}

func TestConcurrentElections(t *testing.T) {
	ring := newMockRingMaintainer(t)
	logger := logging.NewStdLogger("t")
	e := newCRElector(logger, addrs[0], ring)

	// Update ability to trigger election
	e.UpdateAbility(10)
	go func() {
		// Second update should be delayed
		e.UpdateAbility(20)
	}()
	// Should send fresh advertisement
	ring.expectSentMessage(announcementMessage{[]announcement{
		{Ability: 10, Addr: addrs[0]},
	}})

	// Simulate new concurrent election
	ring.simulateReception(announcementMessage{[]announcement{
		{Ability: 15, Addr: addrs[1]},
	}})

	// Expect advert to be propagated
	ring.expectSentMessage(announcementMessage{[]announcement{
		{Ability: 15, Addr: addrs[1]},
		{Ability: 10, Addr: addrs[0]},
	}})

	// Simulate the advertisement message having done a full round and become a result
	ring.simulateReception(resultMessage{
		Leader:       addrs[2],
		Participants: []address{addrs[1]},
	})

	// Expect result to be propagated
	ring.expectSentMessage(resultMessage{
		Leader:       addrs[2],
		Participants: []address{addrs[1], addrs[0]},
	})

	// Expect second election to be started
	ring.expectSentMessage(announcementMessage{[]announcement{
		{Ability: 20, Addr: addrs[0]},
	}})

	// Simulate second election to have completed full round
	ring.simulateReception(announcementMessage{[]announcement{
		{Ability: 20, Addr: addrs[0]},
		{Ability: 10, Addr: addrs[1]},
		{Ability: 25, Addr: addrs[2]},
	}})

	// Expect result to be sent
	ring.expectSentMessage(resultMessage{
		Leader:       addrs[2],
		Participants: []address{addrs[0]},
	})

	// Simulate result to have completed full round
	ring.simulateReception(resultMessage{
		Leader:       addrs[2],
		Participants: []address{addrs[0], addrs[1], addrs[2]},
	})
}

func TestResultWhileNotInElection(t *testing.T) {
	ring := newMockRingMaintainer(t)
	logger := logging.NewStdLogger("t")
	newCRElector(logger, addrs[0], ring)

	// Simulate result message in which it appears
	ring.simulateReception(resultMessage{
		Leader:       addrs[1],
		Participants: []address{addrs[0]},
	})

	// Expect no message to be sent
	ring.expectNothingFor(1 * time.Second)

	// Simulate result in which it does not appear
	ring.simulateReception(resultMessage{
		Leader:       addrs[1],
		Participants: []address{addrs[2]},
	})

	// Expect new election to be started
	ring.expectSentMessage(announcementMessage{[]announcement{
		{Ability: 0, Addr: addrs[0]},
	}})

	// Simulate the advertisement message having done a full round
	ring.simulateReception(announcementMessage{[]announcement{
		{Ability: 0, Addr: addrs[0]},
		{Ability: 10, Addr: addrs[1]},
	}})

	// Expect result to be sent
	ring.expectSentMessage(resultMessage{
		Leader:       addrs[1],
		Participants: []address{addrs[0]},
	})
}
