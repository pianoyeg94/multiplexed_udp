package timer

import (
	"time"
)

const (
	uvinf                     = 0x7FF0000000000000
	MaxDuration time.Duration = 1<<63 - 1
)

func NewTimer(d time.Duration) *Timer {
	t := Timer{
		Timer: time.NewTimer(d),
	}
	if d != MaxDuration {
		t.isSet = true
	}
	return &t
}

type Timer struct {
	*time.Timer
	isSet bool
}

func (t *Timer) Stop() bool {
	wasActive := t.Timer.Stop()
	if !wasActive {
		select {
		case <-t.C:
		default:
		}
	}
	t.isSet = false
	return wasActive
}

func (t *Timer) Reset(delay time.Duration) bool {
	t.Stop()
	if delay != MaxDuration {
		t.isSet = true
	}
	return t.Timer.Reset(delay)
}

func (t *Timer) IsSet() bool {
	return t.isSet
}
