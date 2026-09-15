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
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/list"
	"vitess.io/vitess/go/vt/priority"
)

// waiter represents a client waiting for a connection in the waitlist
type waiter[C Connection] struct {
	// setting is the connection Setting that we'd like, or nil if we'd like a
	// a connection with no Setting applied
	setting *Setting
	// conn is a channel that will receive the connection when it's ready
	conn chan *Pooled[C]
	// ctx is the request context of the waiting client; returners sample it
	// under the waitlist mutex to evict waiters that can no longer use a
	// connection
	ctx context.Context
	// age is the amount of cycles this client has been on the waitlist
	age uint32
	//priority is the priority of the waiter
	priority priority.Priority
}

type priorityQueue[C Connection] struct {
	queues                []*list.List[*waiter[C]]
	waiterToQueuedElement map[*waiter[C]]*list.Element[*waiter[C]]
	size                  atomic.Int64
}

func newPriorityQueue[C Connection](priorities int) *priorityQueue[C] {
	pq := &priorityQueue[C]{
		queues:                make([]*list.List[*waiter[C]], priorities),
		waiterToQueuedElement: make(map[*waiter[C]]*list.Element[*waiter[C]]),
	}
	for i := range pq.queues {
		pq.queues[i] = list.New[*waiter[C]]()
	}
	return pq
}

func (pq *priorityQueue[C]) add(waiter *waiter[C]) {
	element := pq.queues[waiter.priority].PushBack(waiter)
	pq.waiterToQueuedElement[waiter] = element
	pq.size.Add(1)
}

func (pq *priorityQueue[C]) remove(waiter *waiter[C]) bool {
	element, ok := pq.waiterToQueuedElement[waiter]
	if !ok {
		return false
	}
	pq.removeElement(element)
	return true
}

func (pq *priorityQueue[C]) removeElement(element *list.Element[*waiter[C]]) {
	request := element.Value
	delete(pq.waiterToQueuedElement, request)
	pq.queues[request.priority].Remove(element)
	pq.size.Add(-1)
}

func (pq *priorityQueue[C]) len() int {
	return int(pq.size.Load())
}

type waitlist[C Connection] struct {
	nodes sync.Pool
	mu    sync.Mutex
	pq    *priorityQueue[C]
}

// waitForConn blocks until a connection with the given Setting is returned by another client,
// or until the given context expires.
// The returned connection may _not_ have the requested Setting. This function can
// also return a `nil` connection even if our context has expired, if the pool has
// forced an expiration of all waiters in the waitlist.
func (wl *waitlist[C]) waitForConn(ctx context.Context, setting *Setting, closeChan <-chan struct{}) (*Pooled[C], error) {
	elem := wl.nodes.Get().(*list.Element[waiter[C]])
	defer func() {
		// Drop the references to the request-scoped ctx and setting before
		// recycling the node, so they aren't pinned in the pool. The element
		// is off the list by now, so no returner can observe this write.
		elem.Value = waiter[C]{conn: elem.Value.conn}
		wl.nodes.Put(elem)
	}()

	// Extract priority from context, default to Medium
	pri, ok := priority.FromContext(ctx)
	if !ok {
		pri = priority.Medium
	}

	elem.Value = waiter[C]{conn: elem.Value.conn, setting: setting, ctx: ctx, priority: pri, age: 0}

	wl.mu.Lock()
	wl.pq.add(&elem.Value)
	wl.mu.Unlock()

	select {
	case <-closeChan:
		// Pool was closed while we were waiting.
		wl.mu.Lock()
		removed := wl.pq.remove(&elem.Value)
		wl.mu.Unlock()

		if removed {
			return nil, ErrConnPoolClosed
		}

		// if we weren't able to remove ourselves from the waitlist, it means
		// a returner reached us first: it either handed us a connection or
		// evicted us (a nil on the channel), so read the outcome.
		return waitResult(ctx, <-elem.Value.conn)

	case <-ctx.Done():
		// Context expired. We need to try to remove ourselves from the waitlist to
		// prevent another goroutine from trying to hand us a connection later on.
		wl.mu.Lock()
		removed := wl.pq.remove(&elem.Value)
		wl.mu.Unlock()

		if removed {
			return nil, context.Cause(ctx)
		}

		// if we weren't able to remove ourselves from the waitlist, it means
		// a returner reached us first: it either handed us a connection or
		// evicted us (a nil on the channel), so read the outcome.
		return waitResult(ctx, <-elem.Value.conn)

	case conn := <-elem.Value.conn:
		return waitResult(ctx, conn)
	}
}

// waitResult interprets what a returner left on the waiter's channel: a real
// connection is a successful handoff, while a nil means the returner evicted
// the waiter because its context had expired.
func waitResult[C Connection](ctx context.Context, conn *Pooled[C]) (*Pooled[C], error) {
	if conn != nil {
		return conn, nil
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	return nil, ErrTimeout
}

func (wl *waitlist[C]) maybeStarvingCount() (maybeStarving int) {
	if wl.pq.len() == 0 {
		return 0
	}
	wl.mu.Lock()
	defer wl.mu.Unlock()

	// Count the waiters that no returner has aged yet (age == 0); they may be
	// starving. Waiters whose context has already expired cannot use a
	// connection and are only listed until a returner evicts them, so they
	// don't count.
	for i := 0; i < int(priority.SupportedPriorities); i++ {
		for elem := wl.pq.queues[i].Front(); elem != nil; elem = elem.Next() {
			if elem.Value.ctx.Err() != nil {
				continue
			}
			if elem.Value.age == 0 {
				maybeStarving++
			}
		}
	}

	return
}

// tryReturnConn tries handing over a connection to one of the waiters in the
// pool. Waiters whose context has already expired are evicted; if every
// waiter has expired, the connection is not handed over at all.
func (wl *waitlist[D]) tryReturnConn(conn *Pooled[D]) bool {
	// fast path: if there's nobody waiting there's nothing to do
	if wl.pq.len() == 0 {
		return false
	}
	// split the slow path into a separate function to enable inlining
	return wl.tryReturnConnSlow(conn)
}

func (wl *waitlist[D]) tryReturnConnSlow(conn *Pooled[D]) bool {
	const maxAge = 8

	connSetting := conn.Conn.Setting()

	wl.mu.Lock()
	// we maintain the original vitess connection pool behavior that favors returning
	// the connection to a waiter waiting for a connection with the same settings or
	// a waiter that has reached the max age.
	// The difference is that we do this in priority order.
	for pri := int(priority.Critical); pri >= int(priority.Penalized); pri-- {
		queue := wl.pq.queues[pri]

		var (
			target *list.Element[*waiter[D]]
			next   *list.Element[*waiter[D]]
		)
		// iterate through this priority's waitlist looking for either waiters
		// that have been here too long, or a waiter that is looking exactly for
		// the same Setting as the one we have in our connection.
		for elem := queue.Front(); elem != nil; elem = next {
			next = elem.Next() // capture before any Remove unlinks elem
			w := elem.Value

			// Evict waiters whose context has already expired: they cannot use the
			// connection, and removing them here is what keeps the list from
			// accumulating a dead prefix that every later return must re-scan. Wake
			// them with a nil so they stop waiting. This send can't block while we
			// hold the mutex: the channel is buffered and a listed waiter's buffer
			// is always empty, since only a returner sends and only while removing
			// the waiter from the list.
			if w.ctx.Err() != nil {
				wl.pq.removeElement(elem)
				w.conn <- nil
				continue
			}
			if target == nil {
				// the front-most live waiter is the fallback handover target
				target = elem
			}
			if w.age > maxAge || w.setting == connSetting {
				target = elem
				break
			}
			// this only ages the waiters that are being skipped over: we'll start
			// aging the waiters in the back once they get to the front of the pool.
			// the maxAge of 8 has been set empirically: smaller values cause clients
			// with a specific setting to slightly starve, and aging all the clients
			// in the list every time leads to unfairness when the system is at capacity
			w.age++
		}

		// every waiter in this priority had an expired context; try the next
		// priority down.
		if target == nil {
			continue
		}

		wl.pq.removeElement(target)
		wl.mu.Unlock()

		// hand the connection to the live target. The channel is buffered, so the
		// send completes without waiting for the waiter to be scheduled.
		target.Value.conn <- conn
		// Allow the goroutine waiting on the channel to start running _now_.
		runtime.Gosched()
		return true
	}
	wl.mu.Unlock()

	// maybe there isn't anybody to hand over the connection to, because we've
	// raced with another client returning another connection, or because all
	// the waiters in the list have an expired context
	return false
}

func (wl *waitlist[C]) init() {
	wl.nodes.New = func() any {
		return &list.Element[waiter[C]]{
			// buffered (cap 1) so returners never block handing off a
			// connection or waking an evicted waiter
			Value: waiter[C]{conn: make(chan *Pooled[C], 1)},
		}
	}
	wl.pq = newPriorityQueue[C](priority.SupportedPriorities)
}

func (wl *waitlist[C]) waiting() int {
	return wl.pq.len()
}
