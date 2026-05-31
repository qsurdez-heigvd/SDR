package server

import (
	"chatsapp/internal/client"
	"chatsapp/internal/common"
	"chatsapp/internal/logging"
	"chatsapp/internal/transport"
	"chatsapp/internal/transport/udp"
	"chatsapp/internal/utils"
	"chatsapp/internal/utils/ioutils"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

var addrs = []transport.Address{
	{IP: "127.0.0.1", Port: 5000},
	{IP: "127.0.0.1", Port: 5001},
	{IP: "127.0.0.1", Port: 5002},
}

func localClient(username string) clientCommStrategy {
	return newLocalClientCommStrategy(common.Username(username))
}

func remoteClient() clientCommStrategy {
	return newRemoteClientCommStrategy()
}

func createServer(t *testing.T, logFileName string, clientComm clientCommStrategy, addr transport.Address, ring []transport.Address, neighbors []transport.Address, printReadAck bool, slowdownMs uint32) (ioStream ioutils.MockIOStream, server *Server, networkInterface transport.NetworkInterface) {
	debug := true
	ioStream = ioutils.NewMockReader()
	logFile := logging.NewLogFile(logFileName)
	log := logging.NewLogger(ioStream, logFile, strconv.Itoa(int(addr.Port)), false)
	networkInterface = udp.NewUDP(addr, log.WithPostfix("udp").WithLogLevel(logging.WARN))

	log.Infof("Creating server at %s with neighbors %v", addr, neighbors)
	server = newServer(ioStream, log, debug, addr, clientComm, ring, neighbors, printReadAck, networkInterface, slowdownMs)
	go server.Start()
	t.Cleanup(func() {
		server.Close()
		networkInterface.Close()
		fmt.Println("Closing server")
		time.Sleep(100 * time.Millisecond)
	})
	return
}

// createServerClique creates a server connected to all processes in the ring.
func createServerClique(t *testing.T, logFileName string, clientComm clientCommStrategy, addr transport.Address, ring []transport.Address, printReadAck bool, slowdownMs uint32) (ioStream ioutils.MockIOStream, server *Server, networkInterface transport.NetworkInterface) {
	neighbors := make([]transport.Address, 0, len(ring)-1)
	for _, n := range ring {
		if n != addr {
			neighbors = append(neighbors, n)
		}
	}
	return createServer(t, logFileName, clientComm, addr, ring, neighbors, printReadAck, slowdownMs)
}

func createClient(username string, serverAddr transport.Address, selfAddr transport.Address) (c client.Client, ni transport.NetworkInterface, ioStream ioutils.MockIOStream) {
	ioStream = ioutils.NewMockReader()
	log := logging.NewStdLogger("cli-" + username)
	ni = udp.NewUDP(selfAddr, log.WithPostfix("udp").WithLogLevel(logging.WARN))
	c = client.NewClient(log, serverAddr, selfAddr, common.Username(username), ni, ioStream)
	go c.Run()
	return
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

	i1, _, _ := createServerClique(t, "./server1.log", localClient("Alice"), addrs[0], []transport.Address{addrs[1]}, true, 0)
	i2, _, _ := createServerClique(t, "./server2.log", localClient("Bob"), addrs[1], []transport.Address{addrs[0]}, true, 0)

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

	i1, _, _ := createServerClique(t, "./server1.log", localClient("Alice"), addrs[0], []transport.Address{addrs[1]}, true, 0)
	i2, _, _ := createServerClique(t, "./server2.log", localClient("Bob"), addrs[1], []transport.Address{addrs[0]}, true, 0)

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

	i1, _, _ := createServerClique(t, "./server1.log", localClient("Alice"), addrs[0], []transport.Address{addrs[1]}, false, 0)
	i2, _, _ := createServerClique(t, "./server2.log", localClient("Bob"), addrs[1], []transport.Address{addrs[0]}, false, 0)

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

	i1, _, _ := createServerClique(t, "./server1.log", localClient("Alice"), addrs[0], []transport.Address{addrs[1]}, false, 0)
	i2, _, _ := createServerClique(t, "./server2.log", localClient("Bob"), addrs[1], []transport.Address{addrs[0]}, false, 0)

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

	i1, _, _ := createServerClique(t, "./server1.log", localClient("Alice"), addrs[0], []transport.Address{addrs[1]}, true, 0)
	i2, _, _ := createServerClique(t, "./server2.log", localClient("Bob"), addrs[1], []transport.Address{addrs[0]}, true, 0)

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
	i1, _, _ := createServerClique(t, "./server1.log", localClient("Alice"), addrs[0], []transport.Address{addrs[1], addrs[2]}, false, 0)

	i2, _, _ := createServerClique(t, "./server2.log", localClient("Bob"), addrs[1], []transport.Address{addrs[0], addrs[2]}, false, 0)

	i3, _, _ := createServerClique(t, "./server3.log", localClient("Charlie"), addrs[2], []transport.Address{addrs[0], addrs[1]}, false, 0)

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

	i1, s1, _ := createServerClique(t, "./server1.log", localClient("Alice"), addrs[0], []transport.Address{addrs[1], addrs[2], addrs[3]}, false, 0)
	i2, s2, _ := createServerClique(t, "./server2.log", localClient("Bob"), addrs[1], []transport.Address{addrs[0], addrs[2], addrs[3]}, false, 0)
	i3, s3, _ := createServerClique(t, "./server3.log", localClient("Charlie"), addrs[2], []transport.Address{addrs[0], addrs[1], addrs[3]}, false, 0)
	i4, s4, _ := createServerClique(t, "./server4.log", localClient("David"), addrs[3], []transport.Address{addrs[0], addrs[1], addrs[2]}, false, 0)

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

func TestUnidirectionalWithClients(t *testing.T) {
	aliceInputs := []string{
		"Hello, I'm Alice!",
		"Hi Bob!",
		"Hi Bob again!",
	}

	// Create 4 connected servers
	serverAddrs := []transport.Address{
		{IP: "127.0.0.1", Port: 5050},
		{IP: "127.0.0.1", Port: 5051},
	}

	_, s1, _ := createServerClique(t, "./server1.log", remoteClient(), serverAddrs[0], []transport.Address{serverAddrs[0], serverAddrs[1]}, false, 0)
	_, s2, _ := createServerClique(t, "./server2.log", remoteClient(), serverAddrs[1], []transport.Address{serverAddrs[0], serverAddrs[1]}, false, 0)

	_, nc2, c2Stream := createClient("Bob", serverAddrs[1], transport.Address{IP: "127.0.0.1", Port: 7001})
	time.Sleep(1 * time.Second)
	_, nc1, c1Stream := createClient("Alice", serverAddrs[0], transport.Address{IP: "127.0.0.1", Port: 7000})

	for _, input := range aliceInputs {
		c1Stream.SimulateNextInputLine(input)
		l := c2Stream.InterceptNextPrintln()
		expected := "Alice: " + input + "\n"
		if l != expected {
			t.Errorf("Expected %s, got %s", expected, l)
		}
	}

	time.Sleep(1 * time.Second)

	s1.Close()
	s2.Close()

	nc1.Close()
	nc2.Close()

	time.Sleep(2 * time.Second)
}

func TestTwoClientsOnSameServer(t *testing.T) {
	// In this test, 4 users connect on 2 servers, so that each server has 2 users.
	// One user then sends messages; all 3 other users should receive them, especially the one sharing the server with the sender.

	inputs := []string{
		"Hello there!",
		"Nice to meet you all",
	}

	serverAddrs := []transport.Address{
		{IP: "127.0.0.1", Port: 5050},
		{IP: "127.0.0.1", Port: 5051},
	}

	clientAddrs := []transport.Address{
		{IP: "127.0.0.1", Port: 6050},
		{IP: "127.0.0.1", Port: 6051},
		{IP: "127.0.0.1", Port: 6052},
		{IP: "127.0.0.1", Port: 6053},
	}

	_, s1, _ := createServerClique(t, "./server1.log", remoteClient(), serverAddrs[0], []transport.Address{serverAddrs[0], serverAddrs[1]}, false, 0)
	_, s2, _ := createServerClique(t, "./server2.log", remoteClient(), serverAddrs[1], []transport.Address{serverAddrs[0], serverAddrs[1]}, false, 0)

	_, nc1, c1Stream := createClient("Alice", serverAddrs[0], clientAddrs[0])
	time.Sleep(500 * time.Millisecond)
	_, nc2, c2Stream := createClient("Bob", serverAddrs[0], clientAddrs[1])
	time.Sleep(500 * time.Millisecond)
	_, nc3, c3Stream := createClient("Charlie", serverAddrs[1], clientAddrs[2])
	time.Sleep(500 * time.Millisecond)
	_, nc4, c4Stream := createClient("David", serverAddrs[1], clientAddrs[3])
	time.Sleep(500 * time.Millisecond)

	for _, input := range inputs {
		c1Stream.SimulateNextInputLine(input)
		l := c2Stream.InterceptNextPrintln()
		expected := "Alice: " + input + "\n"
		if l != expected {
			t.Errorf("Expected %s, got %s", expected, l)
		}
		l = c3Stream.InterceptNextPrintln()
		if l != expected {
			t.Errorf("Expected %s, got %s", expected, l)
		}
		l = c4Stream.InterceptNextPrintln()
		if l != expected {
			t.Errorf("Expected %s, got %s", expected, l)
		}
	}

	time.Sleep(1 * time.Second)

	s1.Close()
	s2.Close()
	nc1.Close()
	nc2.Close()
	nc3.Close()
	nc4.Close()

	time.Sleep(2 * time.Second)
}

func TestConcurrentConnection(t *testing.T) {
	// 2 servers, 2 clients connecting to the same server

	serverAddrs := []transport.Address{
		{IP: "127.0.0.1", Port: 5050},
		{IP: "127.0.0.1", Port: 5051},
	}

	clientAddrs := []transport.Address{
		{IP: "127.0.0.1", Port: 7050},
		{IP: "127.0.0.1", Port: 7051},
	}

	_, s1, _ := createServerClique(t, "./server1.log", remoteClient(), serverAddrs[0], []transport.Address{serverAddrs[0], serverAddrs[1]}, false, 0)
	_, s2, _ := createServerClique(t, "./server2.log", remoteClient(), serverAddrs[1], []transport.Address{serverAddrs[0], serverAddrs[1]}, false, 0)

	_, nc1, c1Stream := createClient("Alice", serverAddrs[0], clientAddrs[0])
	_, nc2, c2Stream := createClient("Bob", serverAddrs[0], clientAddrs[1])

	time.Sleep(1 * time.Second)

	input := "Hello there!"
	c1Stream.SimulateNextInputLine(input)
	output := c2Stream.InterceptNextPrintln()
	expected := "Alice: " + input + "\n"
	if output != expected {
		t.Errorf("Expected %s, got %s", expected, output)
	}

	time.Sleep(1 * time.Second)

	s1.Close()
	s2.Close()

	nc1.Close()
	nc2.Close()

	time.Sleep(2 * time.Second)
}

func TestStress(t *testing.T) {
	// 3 servers, 10 clients, they connect all at the same time and all but 2 of them send messages.
	// The remaining two clients should receive all messages in the same order.

	serverCount := 3
	clientCount := 10
	msgsPerClient := 1000

	serverAddrs := make([]transport.Address, serverCount)
	for i := 0; i < serverCount; i++ {
		serverAddrs[i] = transport.Address{IP: "127.0.0.1", Port: 5550 + uint16(i)}
	}

	servers := make([]*Server, serverCount)
	for i := 0; i < serverCount; i++ {
		_, servers[i], _ = createServerClique(t, "./server"+strconv.Itoa(i)+".log", remoteClient(), serverAddrs[i], serverAddrs, false, 0)
	}

	clients := make([]ioutils.MockIOStream, clientCount)
	clientNetworks := make([]transport.NetworkInterface, clientCount)
	for i := 0; i < clientCount; i++ {
		addr := transport.Address{IP: "127.0.0.1", Port: 6550 + uint16(i)}
		serverAddr := serverAddrs[rand.Intn(len(serverAddrs))]
		_, clientNetworks[i], clients[i] = createClient("User"+strconv.Itoa(i), serverAddr, addr)
	}

	time.Sleep(2 * time.Second)

	for i := 0; i < clientCount-2; i++ {
		go func(i int) {
			for j := 0; j < msgsPerClient; j++ {
				clients[i].SimulateNextInputLine("User" + strconv.Itoa(i) + " message " + strconv.Itoa(j))
			}
		}(i)
	}

	time.Sleep(2 * time.Second)

	for j := 0; j < msgsPerClient*(clientCount-2); j++ {
		msg0 := clients[clientCount-2].InterceptNextPrintln()
		msg1 := clients[clientCount-1].InterceptNextPrintln()

		if msg0 != msg1 {
			t.Errorf("Mismatched messages: '%s' vs '%s'", msg0, msg1)
		}
	}

	time.Sleep(2 * time.Second)

	for i := 0; i < serverCount; i++ {
		servers[i].Close()
	}

	for i := 0; i < clientCount; i++ {
		clientNetworks[i].Close()
	}

	time.Sleep(2 * time.Second)
}

func getIDToAddress(base uint16) func(i int) transport.Address {
	return func(i int) transport.Address {
		return transport.Address{IP: "127.0.0.1", Port: base + uint16(i)}
	}
}

func testStressWithGivenGraph(t *testing.T, graph utils.Graph[transport.Address], local bool) {
	msgsPerClient := 20

	serverCount := graph.GetSize()
	numClients := serverCount * 2

	allServerAddrs := make([]transport.Address, 0, graph.GetSize())
	for _, addr := range graph.GetVertices() {
		allServerAddrs = append(allServerAddrs, addr)
	}
	getRandomServer := func() transport.Address {
		return allServerAddrs[rand.Intn(len(allServerAddrs))]
	}

	servers := make([]*Server, 0, serverCount)
	ioStreams := make([]ioutils.MockIOStream, 0)
	for _, server := range graph.GetVertices() {
		var cl clientCommStrategy
		if local {
			clientName := fmt.Sprintf("User%d", int(server.Port))
			cl = localClient(clientName)
		} else {
			cl = remoteClient()
		}
		io, s, _ := createServer(t, "./server"+strconv.Itoa(int(server.Port)-5550)+".log", cl, server, allServerAddrs, graph.GetNeighbors(server), false, 0)
		servers = append(servers, s)
		if local {
			ioStreams = append(ioStreams, io)
		}
	}

	if !local {
		for i := 0; i < numClients; i++ {
			addr := transport.Address{IP: "127.0.0.1", Port: 6550 + uint16(i)}
			serverAddr := getRandomServer()
			_, ni, ioStream := createClient("User"+strconv.Itoa(i), serverAddr, addr)
			ioStreams = append(ioStreams, ioStream)
			t.Cleanup(func() {
				ni.Close()
				time.Sleep(50 * time.Millisecond)
			})
		}
	}

	time.Sleep(2 * time.Second)

	for i := 0; i < len(ioStreams)-2; i++ {
		go func(i int) {
			for j := 0; j < msgsPerClient; j++ {
				ioStreams[i].SimulateNextInputLine("User" + strconv.Itoa(i) + " message " + strconv.Itoa(j))
			}
		}(i)
	}

	time.Sleep(2 * time.Second)

	numStreams := len(ioStreams)
	for j := 0; j < msgsPerClient*(numStreams-2); j++ {
		msg0 := ioStreams[numStreams-2].InterceptNextPrintln()
		msg1 := ioStreams[numStreams-1].InterceptNextPrintln()

		if msg0 != msg1 {
			t.Errorf("Mismatched messages: '%s' vs '%s'", msg0, msg1)
		}
	}

	time.Sleep(2 * time.Second)

	for i := 0; i < serverCount; i++ {
		servers[i].Close()
	}

	time.Sleep(2 * time.Second)
}

// func TestStressOnNonCliques(t *testing.T) {
// 	numServers := 10

// 	graphNames := []string{"Line", "Random", "Cycle", "RandomTree", "Clique"}

// 	graphs := []utils.Graph[transport.Address]{
// 		utils.GenLineGraph(numServers, getIDToAddress(5550)),
// 		utils.GenRandomGraph(numServers, getIDToAddress(5550)),
// 		utils.GenCycleGraph(numServers, getIDToAddress(5550)),
// 		utils.GenRandomTreeGraph(numServers, getIDToAddress(5550)),
// 		utils.GenCliqueGraph(numServers, getIDToAddress(5550)),
// 	}
// 	tests := make([]struct {
// 		graphName string
// 		graph     utils.Graph[transport.Address]
// 		local     bool
// 	}, len(graphs)*2)
// 	for i, g := range graphs {
// 		tests[2*i].graphName = graphNames[i]
// 		tests[2*i].graph = g
// 		tests[2*i].local = false

// 		tests[2*i+1].graphName = graphNames[i]
// 		tests[2*i+1].graph = g
// 		tests[2*i+1].local = true
// 	}

// 	for _, test := range tests {
// 		var local string
// 		if test.local {
// 			local = "Local"
// 		} else {
// 			local = "Remote"
// 		}
// 		testname := fmt.Sprintf("TestStressOnNonCliques-%v-%s", test.graphName, local)
// 		t.Run(testname, func(t *testing.T) {
// 			testStressWithGivenGraph(t, test.graph, test.local)
// 		})
// 		time.Sleep(2 * time.Second)
// 	}
// }

func TestStressOnLineGraphLocal(t *testing.T) {
	testStressWithGivenGraph(t, utils.GenLineGraph(5, getIDToAddress(5550)), true)
}

func TestStressOnLineGraphRemote(t *testing.T) {
	testStressWithGivenGraph(t, utils.GenLineGraph(5, getIDToAddress(5550)), false)
}

func TestStressOnCycleGraphLocal(t *testing.T) {
	testStressWithGivenGraph(t, utils.GenCycleGraph(5, getIDToAddress(5550)), true)
}

func TestStressOnCycleGraphRemote(t *testing.T) {
	testStressWithGivenGraph(t, utils.GenCycleGraph(5, getIDToAddress(5550)), false)
}

func TestStressOnRandomGraphLocal(t *testing.T) {
	testStressWithGivenGraph(t, utils.GenRandomGraph(5, getIDToAddress(5550)), true)
}

func TestStressOnRandomGraphRemote(t *testing.T) {
	testStressWithGivenGraph(t, utils.GenRandomGraph(5, getIDToAddress(5550)), false)
}

func TestStressOnRandomTreeGraphLocal(t *testing.T) {
	testStressWithGivenGraph(t, utils.GenRandomTreeGraph(5, getIDToAddress(5550)), true)
}

func TestStressOnRandomTreeGraphRemote(t *testing.T) {
	testStressWithGivenGraph(t, utils.GenRandomTreeGraph(5, getIDToAddress(5550)), false)
}

func TestStressOnCliqueGraphLocal(t *testing.T) {
	testStressWithGivenGraph(t, utils.GenCliqueGraph(5, getIDToAddress(5550)), true)
}

func TestStressOnCliqueGraphRemote(t *testing.T) {
	testStressWithGivenGraph(t, utils.GenCliqueGraph(5, getIDToAddress(5550)), false)
}
