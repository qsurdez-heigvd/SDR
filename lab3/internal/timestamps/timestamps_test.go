package timestamps

import "testing"

func assertEqual(t *testing.T, expected, actual Timestamp) {
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}

func TestNextTimestampFromSelf(t *testing.T) {
	handler := NewLamportHandler("A", 1)
	next := handler.IncrementTimestamp()
	assertEqual(t, Timestamp{Pid: "A", Seqnum: 2}, next)
}

func TestNextTimestampFromOtherWithHigherTimestamp(t *testing.T) {
	handler := NewLamportHandler("A", 1)
	ts := Timestamp{Pid: "B", Seqnum: 2}
	next := handler.UpdateTimestamp(ts)
	assertEqual(t, Timestamp{Pid: "A", Seqnum: 3}, next)
}

func TestNextTimestampFromOtherWithLowerTimestamp(t *testing.T) {
	handler := NewLamportHandler("A", 1)
	ts := Timestamp{Pid: "B", Seqnum: 0}
	next := handler.UpdateTimestamp(ts)
	assertEqual(t, Timestamp{Pid: "A", Seqnum: 2}, next)
}

func TestNextTimestampFromOtherWithEqualTimestamp(t *testing.T) {
	handler := NewLamportHandler("A", 1)
	ts := Timestamp{Pid: "B", Seqnum: 1}
	next := handler.UpdateTimestamp(ts)
	assertEqual(t, Timestamp{Pid: "A", Seqnum: 2}, next)
}

func TestMultipleIncrements(t *testing.T) {
	handler := NewLamportHandler("A", 1)
	ts := Timestamp{Pid: "B", Seqnum: 0}
	next := handler.UpdateTimestamp(ts)
	assertEqual(t, Timestamp{Pid: "A", Seqnum: 2}, next)

	next = handler.UpdateTimestamp(next)
	assertEqual(t, Timestamp{Pid: "A", Seqnum: 3}, next)

	next = handler.IncrementTimestamp()
	assertEqual(t, Timestamp{Pid: "A", Seqnum: 4}, next)

	ts = Timestamp{Pid: "B", Seqnum: 5}
	next = handler.UpdateTimestamp(ts)
	assertEqual(t, Timestamp{Pid: "A", Seqnum: 6}, next)
}

func TestCompareTimestamps(t *testing.T) {
	ts1 := Timestamp{1, "A"}
	ts2 := Timestamp{1, "B"}
	if !ts1.LessThan(ts2) {
		t.Errorf("Expected %v to be less than %v", ts1, ts2)
	}

	ts1 = Timestamp{1, "A"}
	ts2 = Timestamp{2, "A"}
	if !ts1.LessThan(ts2) {
		t.Errorf("Expected %v to be less than %v", ts1, ts2)
	}

	ts1 = Timestamp{1, "A"}
	ts2 = Timestamp{1, "A"}
	if ts1.LessThan(ts2) {
		t.Errorf("Expected %v to be less than %v", ts1, ts2)
	}
}
