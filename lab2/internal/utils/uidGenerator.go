package utils

// UID is a unique identifier.
type UID uint32

// UIDGenerator is a generator for unique identifiers.
type UIDGenerator <-chan UID

// NewUIDGenerator creates a new UID generator.
func NewUIDGenerator() UIDGenerator {
	c := make(chan UID)
	go func() {
		var i UID
		for {
			c <- i
			i++
		}
	}()
	return c
}
