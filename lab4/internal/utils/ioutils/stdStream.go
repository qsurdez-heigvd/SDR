package ioutils

import (
	"bufio"
	"fmt"
	"os"
)

type stdStream struct {
	ioBuffer bufio.Reader
}

// NewStdStream creates a new instance of an IOStream that pipes to the standard input/output.
func NewStdStream() IOStream {
	return stdStream{
		ioBuffer: *bufio.NewReader(os.Stdin),
	}
}

func (s stdStream) ReadLine() (string, error) {
	line, err := s.ioBuffer.ReadString('\n')
	if err == nil && len(line) > 0 {
		line = line[:len(line)-1]
	}
	return line, err
}

// Println prints the given values to the stream. Items are not space-separated.
func (s stdStream) Println(strs ...interface{}) {
	str := fmt.Sprintln(strs...)
	s.Print(str)
}

// Print prints the given values to the stream. Items are not space-separated.
func (stdStream) Print(s ...interface{}) {
	fmt.Print(s...)
}
