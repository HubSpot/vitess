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
	// conn will be set by another client to hand over the connection to use
	conn *Pooled[C]
	// ctx is the context of the waiting client to check for expiration
	ctx context.Context
	// sema is a synchronization primitive that allows us to block until our request
	// has been fulfilled
	sema semaphore
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
	defer wl.nodes.Put(elem)

	// Extract priority from context, default to Medium
	pri, ok := priority.FromContext(ctx)
	if !ok {
		pri = priority.Medium
	}

	elem.Value = waiter[C]{setting: setting, conn: nil, ctx: ctx, priority: pri, age: 0}

	wl.mu.Lock()
	wl.pq.add(&elem.Value)
	wl.mu.Unlock()

	done := make(chan struct{})
	go func() {
		// Block on our waiter's semaphore until somebody can hand over a connection to us.
		elem.Value.sema.wait()
		close(done)
	}()

	select {
	case <-closeChan:
		// Pool was closed while we were waiting.
		removed := false

		wl.mu.Lock()
		removed = wl.pq.remove(&elem.Value)
		wl.mu.Unlock()

		// If we removed ourselves from the waitlist, we need to notify our semaphore
		if removed {
			elem.Value.sema.notify(false)
		}

		// Wait for the semaphore to have been notified, either by us or by someone else
		<-done

		if removed {
			return nil, ErrConnPoolClosed
		}

		return elem.Value.conn, nil

	case <-ctx.Done():
		// Context expired. We need to try to remove ourselves from the waitlist to
		// prevent another goroutine from trying to hand us a connection later on.
		removed := false

		wl.mu.Lock()
		removed = wl.pq.remove(&elem.Value)
		wl.mu.Unlock()

		// If we removed ourselves from the waitlist, we need to notify our semaphore
		if removed {
			elem.Value.sema.notify(false)
		}

		// Wait for the semaphore to have been notified, either by us or by someone else
		<-done

		if removed {
			return nil, context.Cause(ctx)
		}
		return elem.Value.conn, nil

	case <-done:
		return elem.Value.conn, nil
	}
}

func (wl *waitlist[C]) maybeStarvingCount() (maybeStarving int) {
	if wl.pq.len() == 0 {
		return 0
	}
	wl.mu.Lock()
	defer wl.mu.Unlock()

	// Count waiters that have never been evaluated (age == 0).
	// There is a race condition where a waiter checks for a connection, cannot get one, but Put returns one before they go in the waitlist
	// proactive connection handoff by the background worker.
	for i := 0; i < int(priority.SupportedPriorities); i++ {
		for elem := wl.pq.queues[i].Front(); elem != nil; elem = elem.Next() {
			if elem.Value.age == 0 {
				maybeStarving++
			}
		}
	}

	return
}

// tryReturnConn tries handing over a connection to one of the waiters in the pool.
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
	//we maintain the original vitess connection pool behavior that favors returning
	//the connection to a waiter waiting for a connection with the same settings or
	//a waiter that has reached the max age.
	//The difference is that we do this in priority order
	for pri := int(priority.Critical); pri >= int(priority.Penalized); pri-- {
		queue := wl.pq.queues[pri]
		front := queue.Front()
		if front == nil {
			continue
		}
		target := front
		// iterate through the waitlist looking for either waiters that have been
		// here too long, or a waiter that is looking exactly for the same Setting
		// as the one we have in our connection.
		for elem := front; elem != nil; elem = elem.Next() {
			w := elem.Value
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
		wl.pq.removeElement(target)
		wl.mu.Unlock()

		// we have a target to return the connection to, simply write the connection
		// into the waiter and signal their semaphore. they'll wake up to pick up the
		// connection.
		target.Value.conn = conn
		target.Value.sema.notify(true)
		return true
	}
	wl.mu.Unlock()
	//maybe there isn't anybody to hand over the connection to, because we've
	//raced with another client returning another connection
	return false
}

func (wl *waitlist[C]) init() {
	wl.nodes.New = func() any {
		return &list.Element[waiter[C]]{}
	}
	wl.pq = newPriorityQueue[C](priority.SupportedPriorities)
}

func (wl *waitlist[C]) waiting() int {
	return wl.pq.len()
}
