package server

import (
	"chatsapp/internal/common"
	"chatsapp/internal/transport"
	"chatsapp/internal/utils"
	"encoding/json"
	"log"
	"os"
)

// ConfigFile represents the JSON configuration file
type ConfigFile struct {
	Debug           bool
	LogPath         string
	DirectNeighbors []string
	Ring            []string
	PrintReadAck    bool `json:"PrintReadAck,omitempty"`
	SlowdownMs      uint32
	Username        string `json:"Username,omitempty"`
}

// Config represents the server configuration, parsed from the JSON configuration file
type Config struct {
	Debug           bool
	LogPath         string
	Addr            transport.Address
	ClientStrategy  clientCommStrategy
	DirectNeighbors []transport.Address
	Ring            []transport.Address
	PrintReadAck    bool
	SlowdownMs      uint32
}

func neighborStringToAddress(neighbors []string) []transport.Address {
	addresses := make([]transport.Address, len(neighbors))
	for i, neighborStr := range neighbors {
		neighbor, err := transport.NewAddress(neighborStr)
		if err != nil {
			log.Fatalf("Failed to parse neighbor address %s. %v", neighborStr, err)
		}
		addresses[i] = neighbor
	}
	return addresses
}

// NewConfig creates a new server configuration from the given arguments
func NewConfig(args []string) (*Config, error) {
	if len(args) < 2 {
		log.Fatal("Not enough arguments. Usage: <local_address> <config_file>")
	}

	addr := args[0]
	neighborsFile := args[1]

	// Read config file
	config, err := readConfigFile(neighborsFile)
	if err != nil {
		log.Fatalf("Failed to read config file %s. %v", neighborsFile, err)
		return nil, err
	}

	selfAddr, err := transport.NewAddress(addr)
	if err != nil {
		return nil, err
	}

	directNeighbors := neighborStringToAddress(config.DirectNeighbors)
	ring := neighborStringToAddress(config.Ring)

	if !utils.SliceContainsAll(ring, directNeighbors) {
		log.Fatalf("Direct neighbors %v not found in ring %v", directNeighbors, ring)
	}

	for _, neighbor := range directNeighbors {
		if neighbor == selfAddr {
			log.Fatalf("Self address %s found in direct neighbors list %v", selfAddr, directNeighbors)
		}
	}
	foundSelf := false
	for _, neighbor := range ring {
		if neighbor == selfAddr {
			foundSelf = true
		}
	}
	if !foundSelf {
		log.Fatalf("Self address %s not found in ring list %v", selfAddr, ring)
	}

	var clientCommStrategy clientCommStrategy
	if config.Username != "" {
		clientCommStrategy = newLocalClientCommStrategy(common.Username(config.Username))
	} else {
		clientCommStrategy = newRemoteClientCommStrategy()
	}

	return &Config{
		Debug:           config.Debug,
		Addr:            selfAddr,
		ClientStrategy:  clientCommStrategy,
		LogPath:         config.LogPath,
		DirectNeighbors: directNeighbors,
		Ring:            ring,
		PrintReadAck:    config.PrintReadAck,
		SlowdownMs:      config.SlowdownMs,
	}, nil
}

// Reads the configuration file and returns the parsed configuration
func readConfigFile(filename string) (*ConfigFile, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	var config ConfigFile
	err = decoder.Decode(&config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
