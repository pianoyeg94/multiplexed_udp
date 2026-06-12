package stream

import (
	"context"
	"sync"
	"time"

	"github.com/pianoyeg94/multiplexed_udp/pkg/timer"
)

type resetSignal struct {
	backoff time.Duration
	seqNum  uint16
}

type firedInfo struct {
	seqNum              uint16
	retransmissionsLeft int
}

type rtoTimer struct {
	maxRetransmissions int
	backoff            time.Duration

	t           *timer.Timer
	firedCh     chan firedInfo
	resetSignal chan resetSignal

	stopCtx context.Context
	stopFn  context.CancelFunc
	stopWg  sync.WaitGroup
}

func newRtoTimer(maxRetransmissions int) *rtoTimer {
	stopCtx, stopFn := context.WithCancel(context.Background())
	rt := rtoTimer{
		maxRetransmissions: maxRetransmissions,
		backoff:            timer.MaxDuration,

		t:           timer.NewTimer(timer.MaxDuration),
		firedCh:     make(chan firedInfo, maxRetransmissions),
		resetSignal: make(chan resetSignal, 1),

		stopCtx: stopCtx,
		stopFn:  stopFn,
	}
	rt.start()
	return &rt
}

func (rt *rtoTimer) reset(delay time.Duration, seqNum uint16, ctx context.Context) bool {
	select {
	case rt.resetSignal <- resetSignal{delay, seqNum}:
	case <-ctx.Done():
		return false
	}
	rt.backoff = delay

	return true
}

func (rt *rtoTimer) fired() <-chan firedInfo {
	return rt.firedCh
}

func (rt *rtoTimer) isSet() bool {
	return rt.backoff != timer.MaxDuration
}

func (rt *rtoTimer) stop() {
	rt.stopFn()
	rt.stopWg.Wait()
	_ = rt.t.Stop()
	close(rt.resetSignal)
	close(rt.firedCh)
}

func (rt *rtoTimer) start() {
	rt.stopWg.Go(func() {
		var seqNum uint16
		var backoff time.Duration
		retransmissionsLeft := rt.maxRetransmissions
		for {
			select {
			case resetSignal := <-rt.resetSignal:
				seqNum = resetSignal.seqNum
				backoff = resetSignal.backoff
			l:
				for {
					select {
					case <-rt.firedCh:
					default:
						break l
					}
				}
			case <-rt.t.C:
				select {
				case rt.firedCh <- firedInfo{seqNum, retransmissionsLeft}:
				case <-rt.stopCtx.Done():
					return
				}

				backoff *= 2
				if retransmissionsLeft == 0 {
					backoff = timer.MaxDuration
				}
				retransmissionsLeft--
			case <-rt.stopCtx.Done():
				return
			}
			rt.t.Reset(backoff)
		}
	})
}
