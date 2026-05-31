package transport

import (
	"fmt"
	"strconv"
	"strings"
)

// Address represents an address on the IP network
type Address struct {
	IP   string
	Port uint16
}

func (a Address) String() string {
	return a.IP + ":" + strconv.FormatUint(uint64(a.Port), 10)
}

// NewAddress constructs a new address from a string in the format "IP:Port".
func NewAddress(str string) (Address, error) {
	split := strings.Split(str, ":")
	if len(split) != 2 {
		return Address{}, fmt.Errorf("invalid address format")
	}
	port, err := strconv.ParseUint(split[1], 10, 16)
	if err != nil {
		return Address{}, err
	}
	return Address{
		IP:   split[0],
		Port: uint16(port),
	}, nil
}

// LessThan reports whether the address is strictly less than the given address in a lexicographical order.
func (a Address) LessThan(b Address) bool {
	if a.IP < b.IP {
		return true
	} else if a.IP > b.IP {
		return false
	} else {
		return a.Port < b.Port
	}
}
