package ioutils

import (
	"chatsapp/internal/utils"
	"fmt"
)

// MockIOStream is a mock implementation of the IOStream interface, useful for testing.
type MockIOStream interface {
	IOStream
	// SimulateNextInputLine provides the next line of input that will be read by the stream.
	SimulateNextInputLine(string)
	// InterceptNextPrintln retrieves the next line that will be written to the stream.
	InterceptNextPrintln() string
}

type mockReader struct {
	nextReadLine    *utils.BufferedChan[string]
	nextWrittenLine *utils.BufferedChan[string]
}

// NewMockReader creates a new mock IOStream instance.
func NewMockReader() MockIOStream {
	reader := mockReader{
		nextReadLine:    utils.NewBufferedChan[string](),
		nextWrittenLine: utils.NewBufferedChan[string](),
	}
	return reader
}

func (m mockReader) ReadLine() (string, error) {
	s := <-m.nextReadLine.Outlet()
	return s, nil
}

func (m mockReader) Println(s ...interface{}) {
	str := fmt.Sprint(s...)
	m.Print(str, "\n")
}

func (m mockReader) Print(s ...interface{}) {
	str := fmt.Sprint(s...)
	m.nextWrittenLine.Inlet() <- str
	fmt.Print("MOCK: " + str)
}

func (m mockReader) SimulateNextInputLine(s string) {
	m.nextReadLine.Inlet() <- s
}

func (m mockReader) InterceptNextPrintln() string {
	return <-m.nextWrittenLine.Outlet()
}
