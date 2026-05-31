package server

import (
	"chatsapp/internal/common"
	"chatsapp/internal/logging"
	"chatsapp/internal/transport"
	"chatsapp/internal/transport/udp"
	"chatsapp/internal/utils/ioutils"
	"fmt"
	"strconv"
	"testing"
	"time"
)

var addrs = []transport.Address{
	{IP: "127.0.0.1", Port: 5000},
	{IP: "127.0.0.1", Port: 5001},
	{IP: "127.0.0.1", Port: 5002},
}

func createServer(t *testing.T, logFileName string, user common.Username, addr transport.Address, neighbors []transport.Address, printReadAck bool, slowdownMs uint32) (ioStream ioutils.MockIOStream, server *Server, networkInterface transport.NetworkInterface) {
	debug := true
	ioStream = ioutils.NewMockReader()
	logFile := logging.NewLogFile(logFileName)
	log := logging.NewLogger(ioStream, logFile, strconv.Itoa(int(addr.Port)), false)
	networkInterface = udp.NewUDP(addr, log.WithPostfix("udp").WithLogLevel(logging.WARN))
	containsSelf := false
	for _, neighbor := range neighbors {
		if neighbor == addr {
			containsSelf = true
			break
		}
	}
	if !containsSelf {
		neighbors = append(neighbors, addr)
	}

	log.Infof("Creating server at %s with neighbors %v", addr, neighbors)
	server = newServer(ioStream, log, debug, addr, user, neighbors, printReadAck, networkInterface, slowdownMs)
	go server.Start()
	t.Cleanup(func() {
		server.Close()
		networkInterface.Close()
		fmt.Println("Closing server")
		time.Sleep(100 * time.Millisecond)
	})
	return
}

// createServerClique creates a server connected to all processes in the network.
func createServerClique(t *testing.T, logFileName string, user common.Username, addr transport.Address, neighbors []transport.Address, printReadAck bool, slowdownMs uint32) (ioStream ioutils.MockIOStream, server *Server, networkInterface transport.NetworkInterface) {
	return createServer(t, logFileName, user, addr, neighbors, printReadAck, slowdownMs)
}

func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

func TestUniqueMessage(t *testing.T) {
	input := "Hello, I'm Alice!"

	i1, _, _ := createServerClique(t, "./server1.log", "Alice", addrs[0], []transport.Address{addrs[1]}, true, 0)
	i2, _, _ := createServerClique(t, "./server2.log", "Bob", addrs[1], []transport.Address{addrs[0]}, true, 0)

	i1.SimulateNextInputLine(input)
	expected := "Alice: " + input + "\n"
	s := i2.InterceptNextPrintln()
	if s != expected {
		t.Errorf("Expected %s, got %s", expected, s)
	}
}

func TestMultipleUnidirectionalMessages(t *testing.T) {
	inputs := []string{
		"Hello, I'm Alice!",
		"Hi Bob!",
		"Hi Bob again!",
	}

	i1, _, _ := createServerClique(t, "./server1.log", "Alice", addrs[0], []transport.Address{addrs[1]}, true, 0)
	i2, _, _ := createServerClique(t, "./server2.log", "Bob", addrs[1], []transport.Address{addrs[0]}, true, 0)

	for _, input := range inputs {
		i1.SimulateNextInputLine(input)
		expected := "Alice: " + input + "\n"
		s := i2.InterceptNextPrintln()
		if s != expected {
			t.Errorf("Expected %s, got %s", expected, s)
		}
	}
}

func TestServerBidirectionalMessages(t *testing.T) {
	s1Inputs := []string{
		"Hello, I'm Alice!",
		"Hi Bob!",
		"Hi Bob again!",
	}

	s2Inputs := []string{
		"Hi, I'm Bob.",
		"Hi Alice",
	}

	i1, _, _ := createServerClique(t, "./server1.log", "Alice", addrs[0], []transport.Address{addrs[1]}, false, 0)
	i2, _, _ := createServerClique(t, "./server2.log", "Bob", addrs[1], []transport.Address{addrs[0]}, false, 0)

	for _, input := range s1Inputs {
		i1.SimulateNextInputLine(input)
		expected := "Alice: " + input + "\n"
		s := i2.InterceptNextPrintln()
		if s != expected {
			t.Errorf("Expected %s, got %s", expected, s)
		}
	}

	for _, input := range s2Inputs {
		i2.SimulateNextInputLine(input)
		expected := "Bob: " + input + "\n"
		s := i1.InterceptNextPrintln()
		if s != expected {
			t.Errorf("Expected %s, got %s", expected, s)
		}
	}
}

func TestServerSpamBidirectionalMessages(t *testing.T) {
	numMessages := 200

	s1Inputs := make([]string, numMessages)
	s2Inputs := make([]string, numMessages)

	for i := 0; i < numMessages; i++ {
		s1Inputs[i] = "Alice message " + strconv.Itoa(i)
		s2Inputs[i] = "Bob message " + strconv.Itoa(i)
	}

	i1, _, _ := createServerClique(t, "./server1.log", "Alice", addrs[0], []transport.Address{addrs[1]}, false, 0)
	i2, _, _ := createServerClique(t, "./server2.log", "Bob", addrs[1], []transport.Address{addrs[0]}, false, 0)

	go func() {
		for _, input := range s1Inputs {
			i1.SimulateNextInputLine(input)
		}
	}()

	go func() {
		for _, input := range s2Inputs {
			i2.SimulateNextInputLine(input)
		}
	}()

	for i := 0; i < numMessages; i++ {
		expectedAlice := "Alice: " + s1Inputs[i] + "\n"
		expectedBob := "Bob: " + s2Inputs[i] + "\n"
		s := i2.InterceptNextPrintln()
		if s != expectedAlice {
			t.Errorf("Expected %s, got %s", expectedAlice, s)
		}
		s = i1.InterceptNextPrintln()
		if s != expectedBob {
			t.Errorf("Expected %s, got %s", expectedBob, s)
		}
	}
}

/** Test : Servers send messages one after the other */
func TestServerSendsMessage(t *testing.T) {
	s1Inputs := []string{
		"Hello, I'm Alice!",
	}

	s2Inputs := []string{
		"Hi, I'm Bob.",
		"Nice to meet you, Alice!",
	}

	i1, _, _ := createServerClique(t, "./server1.log", "Alice", addrs[0], []transport.Address{addrs[1]}, true, 0)
	i2, _, _ := createServerClique(t, "./server2.log", "Bob", addrs[1], []transport.Address{addrs[0]}, true, 0)

	for _, input := range s1Inputs {
		i1.SimulateNextInputLine(input)
		expected := "Alice: " + input + "\n"
		s := i2.InterceptNextPrintln()
		if s != expected {
			t.Errorf("Expected %s, got %s", expected, s)
		}
		s = i1.InterceptNextPrintln()
		expected = "[127.0.0.1:5001 received: " + input + "]\n"
		if s != expected {
			t.Errorf("Expected %s, got %s", expected, s)
		}

	}

	for _, input := range s2Inputs {
		i2.SimulateNextInputLine(input)
		expected := "Bob: " + input + "\n"
		s := i1.InterceptNextPrintln()
		if s != expected {
			t.Errorf("Expected >%s<, got >%s<", expected, s)
		}
		s = i2.InterceptNextPrintln()
		expected = "[127.0.0.1:5000 received: " + input + "]\n"
		if s != expected {
			t.Errorf("Expected >%s<, got >%s<", expected, s)
		}
	}

	time.Sleep(500 * time.Millisecond)
}

/** Test : Servers spam messages to each other */
func TestServerSpamMessages(t *testing.T) {
	s1Inputs := []string{
		"Hello, I'm Alice!",
		"Hi Bob!",
		"Hi Bob again!",
	}

	s2Inputs := []string{
		"Hi, I'm Bob.",
		"Hi Alice",
	}

	s3Inputs := []string{
		"Hi, I'm Charlie.",
		"Hi Alice from Charlie",
	}

	// Create two connected servers
	i1, _, _ := createServerClique(t, "./server1.log", "Alice", addrs[0], []transport.Address{addrs[1], addrs[2]}, false, 0)

	i2, _, _ := createServerClique(t, "./server2.log", "Bob", addrs[1], []transport.Address{addrs[0], addrs[2]}, false, 0)

	i3, _, _ := createServerClique(t, "./server3.log", "Charlie", addrs[2], []transport.Address{addrs[0], addrs[1]}, false, 0)

	var expectedFromAlice []string
	for _, input := range s1Inputs {
		i1.SimulateNextInputLine(input)
		expectedFromAlice = append(expectedFromAlice, "Alice: "+input+"\n")
	}
	var expectedFromBob []string
	for _, input := range s2Inputs {
		i2.SimulateNextInputLine(input)
		expectedFromBob = append(expectedFromBob, "Bob: "+input+"\n")
	}
	var expectedFromCharlie []string
	for _, input := range s3Inputs {
		i3.SimulateNextInputLine(input)
		expectedFromCharlie = append(expectedFromCharlie, "Charlie: "+input+"\n")
	}

	aliceExpectedCount := len(expectedFromBob) + len(expectedFromCharlie)
	bobExpectedCount := len(expectedFromAlice) + len(expectedFromCharlie)
	charlieExpectedCount := len(expectedFromAlice) + len(expectedFromBob)

	var aliceReceived []string
	for i := 0; i < aliceExpectedCount; i++ {
		aliceReceived = append(aliceReceived, i1.InterceptNextPrintln())
	}
	var bobReceived []string
	for i := 0; i < bobExpectedCount; i++ {
		bobReceived = append(bobReceived, i2.InterceptNextPrintln())
	}
	var charlieReceived []string
	for i := 0; i < charlieExpectedCount; i++ {
		charlieReceived = append(charlieReceived, i3.InterceptNextPrintln())
	}

	if len(aliceReceived) != aliceExpectedCount {
		t.Errorf("Expected %d messages from Alice, got %d", aliceExpectedCount, len(aliceReceived))
	}
	if len(bobReceived) != bobExpectedCount {
		t.Errorf("Expected %d messages from Bob, got %d", bobExpectedCount, len(bobReceived))
	}
	if len(charlieReceived) != charlieExpectedCount {
		t.Errorf("Expected %d messages from Charlie, got %d", charlieExpectedCount, len(charlieReceived))
	}

	for _, s := range aliceReceived {
		if !contains(expectedFromBob, s) && !contains(expectedFromCharlie, s) {
			t.Errorf("Unexpected message from Alice: %s", s)
		}
	}
	for _, s := range bobReceived {
		if !contains(expectedFromAlice, s) && !contains(expectedFromCharlie, s) {
			t.Errorf("Unexpected message from Bob: %s", s)
		}
	}
	for _, s := range charlieReceived {
		if !contains(expectedFromAlice, s) && !contains(expectedFromBob, s) {
			t.Errorf("Unexpected message from Charlie: %s", s)
		}
	}
}

func TestGlobalOrdering(t *testing.T) {
	msgCountEach := 100

	aliceInputs := make([]string, msgCountEach)
	bobInputs := make([]string, msgCountEach)

	for i := 0; i < msgCountEach; i++ {
		aliceInputs[i] = fmt.Sprintf("Alice-%d", i)
		bobInputs[i] = fmt.Sprintf("Bob-%d", i)
	}

	// Create 4 connected servers
	addrs := []transport.Address{
		{IP: "127.0.0.1", Port: 5000},
		{IP: "127.0.0.1", Port: 5001},
		{IP: "127.0.0.1", Port: 5002},
		{IP: "127.0.0.1", Port: 5003},
	}

	i1, s1, _ := createServerClique(t, "./server1.log", "Alice", addrs[0], []transport.Address{addrs[1], addrs[2], addrs[3]}, false, 0)
	i2, s2, _ := createServerClique(t, "./server2.log", "Bob", addrs[1], []transport.Address{addrs[0], addrs[2], addrs[3]}, false, 0)
	i3, s3, _ := createServerClique(t, "./server3.log", "Charlie", addrs[2], []transport.Address{addrs[0], addrs[1], addrs[3]}, false, 0)
	i4, s4, _ := createServerClique(t, "./server4.log", "David", addrs[3], []transport.Address{addrs[0], addrs[1], addrs[2]}, false, 0)

	// Send Alice's and Bob's messages
	go func() {
		for _, input := range aliceInputs {
			i1.SimulateNextInputLine(input)
		}
	}()
	go func() {
		for _, input := range bobInputs {
			i2.SimulateNextInputLine(input)
		}
	}()

	// Receive messages on Charlie and David
	var charlieReceived []string
	var davidReceived []string

	for i := 0; i < msgCountEach*2; i++ {
		charlieReceived = append(charlieReceived, i3.InterceptNextPrintln())
		davidReceived = append(davidReceived, i4.InterceptNextPrintln())
	}

	// Check that the messages are in the same order
	for i := 0; i < msgCountEach*2; i++ {
		if charlieReceived[i] != davidReceived[i] {
			t.Errorf("Unordered messages: message #%d received by Charlie was %s; David's was %s", i+1, charlieReceived[i], davidReceived[i])
		}
	}

	s1.Close()
	s2.Close()
	s3.Close()
	s4.Close()

	time.Sleep(2 * time.Second)
}
