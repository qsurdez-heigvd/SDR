package timestamps

// Handler is responsible for generating and updating timestamps according to the Lamport algorithm.
type Handler struct {
	lastSeqnum uint32
	pid        Pid
}

// NewLamportHandler constructs and returns a new Lamport timestamp handler.
func NewLamportHandler(pid Pid, initialSeqnum uint32) *Handler {
	handler := Handler{initialSeqnum, pid}
	return &handler
}

// UpdateTimestamp updates and returns the current timestamp based on the provided timestamp. The new timestamp is garanteed to be greater than the provided timestamp *and* the current local timestamp.
func (handler *Handler) UpdateTimestamp(ts Timestamp) Timestamp {
	handler.lastSeqnum = max(ts.Seqnum, handler.lastSeqnum) + 1
	return Timestamp{handler.lastSeqnum, handler.pid}
}

// IncrementTimestamp increments and returns the local timestamp.
func (handler *Handler) IncrementTimestamp() Timestamp {
	handler.lastSeqnum++
	return Timestamp{handler.lastSeqnum, handler.pid}
}

// GetTimestamp returns the current local timestamp.
func (handler *Handler) GetTimestamp() Timestamp {
	return Timestamp{handler.lastSeqnum, handler.pid}
}
