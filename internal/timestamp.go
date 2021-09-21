package internal

import (
	"fmt"
	"time"
)

//go:generate msgp -tests=false

// Timestamp - represents a millisecond timestamp
type Timestamp int64

func (ts *Timestamp) ToTime() time.Time {
	var (
		sec  int64 = int64(*ts) / 1000
		nsec int64 = int64(*ts) % 1000 * int64(time.Millisecond/time.Nanosecond)
	)
	return time.Unix(sec, nsec)
}

func (ts *Timestamp) FromTime(t time.Time) *Timestamp {
	return ts.FromNanoseconds(t.UnixNano())
}

func (ts *Timestamp) FromNanoseconds(ticks int64) *Timestamp {
	v := ticks / int64(time.Millisecond)
	return ts.FromMilliseconds(v)
}

func (ts *Timestamp) FromMilliseconds(ticks int64) *Timestamp {
	v := ticks
	(*ts) = Timestamp(v)
	return ts
}

func (ts *Timestamp) Value() Timestamp {
	return *ts
}

func (ts *Timestamp) String() string {
	return fmt.Sprintf("%d", *ts)
}

func CurrentTimestamp() *Timestamp {
	return new(Timestamp).FromNanoseconds(time.Now().UnixNano())
}
