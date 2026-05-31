package mutex

import "chatsapp/internal/timestamps"

// Pid is a unique identifier for a process participating in the mutex algorithm.
type Pid = timestamps.Pid
type timestamp = timestamps.Timestamp

/*
Mutex is the interface for a mutex that can be acquired and released.
*/
type Mutex interface {
	/*
		Request permission to enter critical section.

		Returns a function that should be called when the critical section is done, to signal that the mutex can be released.

		It is recommended to defer the closing of the channel immediately after acquiring the mutex to avoid deadlocks.
	*/
	Request() (release func(), err error)
}
