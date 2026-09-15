/*
Copyright 2023 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package smartconnpool

import (
	"runtime"
	"sync/atomic"

	"vitess.io/vitess/go/atomic2"
	"vitess.io/vitess/go/vt/log"
)

// stackDoublePush counts connections observed being pushed onto a stack while
// already on one (a double-return). It backs the double-push diagnostic and is
// asserted by tests; it does not affect pool behavior.
var stackDoublePush atomic.Int64

// pushSite identifies which return path pushed a connection onto a stack. It is
// recorded on the connection at push time so the double-push diagnostic can
// report both ends of a double-return: which path put the connection on the
// stack first, and which path pushed it again.
type pushSite int32

const (
	pushSiteUnknown      pushSite = iota
	pushSiteRecycle               // put(): a client returning a connection via Recycle
	pushSiteIdleWorker            // closeIdleResources: the idle reaper returning/reopening conns
	pushSiteExpireWorker          // tryReturnAnyConn: the expire worker feeding starving waiters
)

func (p pushSite) String() string {
	switch p {
	case pushSiteRecycle:
		return "recycle"
	case pushSiteIdleWorker:
		return "idle-worker"
	case pushSiteExpireWorker:
		return "expire-worker"
	default:
		return "unknown"
	}
}

// connStack is a lock-free stack for Connection objects. It is safe to
// use from several goroutines.
//
// ALIGNMENT: the 128-bit atomic in this struct faults (SIGBUS) on amd64 and
// arm64 unless it is 16-byte aligned. Go has no supported way to demand
// 16-byte alignment (the maximum natural alignment is 8 bytes), so it
// depends entirely on where the allocator places the enclosing allocation,
// which varies with the allocation's exact size, pointer layout, and Go
// version — e.g. allocation headers (Go 1.22+) shift some pointer-bearing
// objects larger than 512 bytes to an odd 8-byte boundary. ConnPool
// currently lands in a bucket where allocations are 16-byte aligned; growing
// it can silently break that, which is why its waitlist is held behind a
// pointer. Be careful when adding fields to ConnPool or anything embedded
// in it.
type connStack[C Connection] struct {
	// top is a pointer to the top node on the stack and to an increasing
	// counter of pop operations, to prevent A-B-A races.
	// See: https://en.wikipedia.org/wiki/ABA_problem
	top atomic2.PointerAndUint64[Pooled[C]]
}

func (s *connStack[C]) Push(item *Pooled[C]) {
	// Claim the on-stack marker before the item becomes visible on the stack.
	// The item cannot be popped until the CAS below links it in, so nothing can
	// clear the marker in this window; Pop clears it only after it has unlinked
	// the item. That keeps the marker consistent with stack membership in
	// correct single-ownership operation.
	if item.onStack.CompareAndSwap(false, true) {
		// We put it on the stack; commit the site the returning path stashed on
		// the conn (pendingSite) so a later double-push can name where the
		// connection was first placed.
		item.pushSite.Store(item.pendingSite.Load())
	} else {
		// The marker was already set: this same *Pooled is already on a stack,
		// so this push puts it on twice (a double-return) -- the corruption
		// behind the "timestampBusy when borrowing a time" panic. Log both ends
		// (the path that first placed it and this second push) plus the culprit
		// stack; the borrow() panic only ever shows the victim that pops the
		// duplicate afterwards. Do not panic here: Push can run on a background
		// worker that no recover() covers. The push itself proceeds unchanged.
		stackDoublePush.Add(1)
		var buf [8192]byte
		n := runtime.Stack(buf[:], false)
		log.Errorf("smartconnpool: connection double-pushed onto a stack (double-return); first push via %s, second push via %s; culprit stack:\n%s",
			pushSite(item.pushSite.Load()), pushSite(item.pendingSite.Load()), buf[:n])
	}
	for {
		oldHead, popCount := s.top.Load()
		item.next.Store(oldHead)
		if s.top.CompareAndSwap(oldHead, popCount, item, popCount) {
			return
		}
		runtime.Gosched()
	}
}

func (s *connStack[C]) Pop() (*Pooled[C], bool) {
	for {
		oldHead, popCount := s.top.Load()
		if oldHead == nil {
			return nil, false
		}

		newHead := oldHead.next.Load()
		if s.top.CompareAndSwap(oldHead, popCount, newHead, popCount+1) {
			oldHead.next.Store(nil)
			oldHead.onStack.Store(false)
			return oldHead, true
		}
		runtime.Gosched()
	}
}

func (s *connStack[C]) Peek() *Pooled[C] {
	top, _ := s.top.Load()
	return top
}
