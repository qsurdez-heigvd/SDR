package ioutils

// IOStream is a generic interface for input/output streams, used to abstract i/o operations, to aid testing.
type IOStream interface {
	// ReadLine returns the next line of input from the stream.
	ReadLine() (string, error)
	// Println prints the given values to the stream. Items are not space-separated.
	Println(...interface{})
	// Print prints the given values to the stream. Items are not space-separated.
	Print(...interface{})
}
