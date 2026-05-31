package utils

// BufferedChan is a channel that has a dynamic buffer size.
type BufferedChan[T any] struct {
	inChan  chan T
	outChan chan T
}

// NewBufferedChan creates a new BufferedChan instance.
func NewBufferedChan[T any]() *BufferedChan[T] {
	c := BufferedChan[T]{
		inChan:  make(chan T),
		outChan: make(chan T),
	}
	go c.run()
	return &c
}

// Inlet returns an input channel for the BufferedChan.
func (b *BufferedChan[T]) Inlet() chan<- T {
	return b.inChan
}

// Outlet returns an output channel for the BufferedChan.
func (b *BufferedChan[T]) Outlet() <-chan T {
	return b.outChan
}

// Close closes the BufferedChan.
func (b *BufferedChan[T]) Close() {
	close(b.inChan)
}

// Main goroutine for handling the BufferedChan.
func (b *BufferedChan[T]) run() {
	// Buffered messages
	buffer := make([]T, 0)
	defer func() {
		// If this function returns, it means closeChan was closed, meaning the BufferedChan is being closed and we must close all channels.
		close(b.outChan)
	}()
	for {
		if len(buffer) == 0 {
			// If the buffer is empty, wait for a message to be received and buffer it.
			msg, ok := <-b.inChan
			if !ok {
				return
			}
			buffer = append(buffer, msg)
		} else {
			// If the buffer contains message, wait for whichever comes first: a new message be buffered, or the oldest message be sent.
			select {
			case msg, ok := <-b.inChan:
				if !ok {
					return
				}
				buffer = append(buffer, msg)
			case b.outChan <- buffer[0]:
				buffer = buffer[1:]
			}
		}
	}
}
