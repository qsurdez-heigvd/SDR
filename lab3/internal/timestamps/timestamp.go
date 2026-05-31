package timestamps

import "fmt"

// Pid represents the identifier of a process
type Pid string

// Timestamp is defined by a sequence number and a process identifier
type Timestamp struct {
	// The sequence number
	Seqnum uint32
	// The Pid of the process on which the timestamp was generated. Used to break ties in sequence numbers.
	Pid Pid
}

// LessThan returns true iff the timestamp is strictly less than the other timestamp
func (ts Timestamp) LessThan(other Timestamp) bool {
	return ts.Seqnum < other.Seqnum || (ts.Seqnum == other.Seqnum && ts.Pid < other.Pid)
}

func (ts Timestamp) String() string {
	return fmt.Sprintf("TS(%s:%v)", ts.Pid, ts.Seqnum)
}
