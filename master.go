/*
Copyright 2014 Datawise Systems Inc. All rights reserved.

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

package utils

import (
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/glog"
)

// Various event types for the events channel.
type MasterEventType int

const (
	MasterAdded MasterEventType = iota // this node has the lock.
	MasterDeleted
	MasterModified
	MasterError
)

// MasterEvent represents a single event sent on the events channel.
type MasterEvent struct {
	Type   MasterEventType // event type
	Master string          // identity of the lock holder
}

// Interface used by the etcd master lock clients.
type MasterInterface interface {
	// Start the election and attempt to acquire the lock. If acquired, the
	// lock is refreshed periodically based on the ttl.
	Start()

	// Stops watching the lock. Closes the events channel.
	Stop()

	// Returns the event channel used by the etcd lock.
	EventsChan() <-chan MasterEvent

	// Method to get the current lockholder. Returns "" if free.
	GetHolder() string
}

// Internal structure to represent an etcd lock.
type etcdLock struct {
	client        Registry         // etcd interface
	name          string           // name of the lock
	id            string           // identity of the lockholder
	ttl           uint64           // ttl of the lock
	eventsCh      chan MasterEvent // channel to send lock ownership updates
	stopCh        chan bool        // channel to stop the acquire routine
	stoppedCh     chan bool        // channel that waits for acquire to finish
	holding       bool             // whether this node is holding the lock
	modifiedIndex uint64           // valid only when this node is holding the lock
}

// Method to create a new etcd lock.
func NewMaster(client Registry, name string, id string,
	ttl uint64) (MasterInterface, error) {
	// client is mandatory. Min ttl is 5 seconds.
	if client == nil || ttl < 5 {
		return nil, errors.New("Invalid args")
	}

	return &etcdLock{client: client, name: name, id: id, ttl: ttl,
		eventsCh:  make(chan MasterEvent, 1),
		stopCh:    make(chan bool, 1),
		stoppedCh: make(chan bool, 1),
		holding:   false, modifiedIndex: 0}, nil
}

// Method to start the attempt to acquire the lock.
// TODO: Check duplicate call.
func (e *etcdLock) Start() {
	// Acquire in the background.
	go func() {
		// If acquire returns without error, exit. If not, acquire
		// crashed and needs to be called again.
		for {
			if err := e.acquire(); err == nil {
				break
			}
		}
	}()
}

// Method to stop the acquisition of lock and release it if holding the lock.
func (e *etcdLock) Stop() {
	// Stop the acquire routine (watch & refresh).
	e.stopCh <- true
	// Wait for acquire to finish.
	<-e.stoppedCh
}

// Method to get the event channel used by the etcd lock.
func (e *etcdLock) EventsChan() <-chan MasterEvent {
	return e.eventsCh
}

// Method to get the lockholder.
func (e *etcdLock) GetHolder() string {
	// Get the key.
	if resp, err := e.client.Get(e.name, false, false); err == nil {
		return resp.Node.Value
	}

	return ""
}

// Method to acquire the lock. It launches up to two goroutines, one to watch
// the changes on the lock and another to refresh the ttl if successful in
// acquiring the lock.
func (e *etcdLock) acquire() (ret error) {
	glog.V(2).Infof("acquire lock %s", e.name)

	watchCh := make(chan *etcd.Response, 1)
	watchFailCh := make(chan bool, 1)
	watchStopCh := make(chan bool, 1)
	refreshStopCh := make(chan bool, 1)

	defer func() {
		if r := recover(); r != nil {
			// If this routine crashes, make sure the other watch and
			// refresh routines are also stopped. The caller of this
			// routine will restart this routine if an error is returned.
			watchStopCh <- true
			if e.holding {
				e.holding = false
				refreshStopCh <- true
			}
			callers := ""
			for i := 0; true; i++ {
				_, file, line, ok := runtime.Caller(i)
				if !ok {
					break
				}
				callers = callers + fmt.Sprintf("%v:%v\n", file,
					line)
			}
			errMsg := fmt.Sprintf("Recovered from panic: %#v (%v)\n%v",
				r, r, callers)
			glog.Errorf(errMsg)
			ret = errors.New(errMsg)
		}
	}()

	// Setup the watch first in order to not miss any notifications.
	go e.watch(watchCh, watchStopCh, watchFailCh)

	// Get the key.
	if resp, err := e.client.Get(e.name, false, false); err == nil {
		// This can happen when the process restarts and ttl has not
		// yet expired.
		if resp.Node.Value == e.id {
			glog.Infof("Acquired lock %s", e.name)
			e.holding = true
			e.modifiedIndex = resp.Node.ModifiedIndex
			e.eventsCh <- MasterEvent{Type: MasterAdded, Master: e.id}
			go e.refresh(refreshStopCh)
		} else {
			e.eventsCh <- MasterEvent{Type: MasterModified,
				Master: resp.Node.Value}
		}
	} else if IsEtcdNotFound(err) {
		// Try to acquire the lock.
		e.tryAcquire(refreshStopCh)
	} else {
		// TODO: Retry get?
		glog.Fatalf("Unexpected get error for lock %s: %s", e.id,
			err.Error())
	}

	// TODO: What happens if etcd loses quorum?
	for {
		select {
		case resp := <-watchCh:
			if resp == nil {
				glog.Info("Got nil resp in watch channel")
				continue
			}
			if resp.Action == "expire" || resp.Action == "delete" {
				if e.holding {
					// This shouldn't normally happen.
					glog.Errorf("Unexpected delete for lock %s", e.name)

					e.holding = false
					refreshStopCh <- true
					// Create a new channel for the next go routine.
					refreshStopCh = make(chan bool, 1)
					e.eventsCh <- MasterEvent{Type: MasterDeleted,
						Master: ""}
				}

				// Some other node gave up the lock, try to acquire.
				e.tryAcquire(refreshStopCh)
			} else if resp.Node.Value != e.id &&
				(resp.PrevNode == nil ||
					resp.Node.Value != resp.PrevNode.Value) {
				// Lock acquired by some other node. PrevNode should
				// never be nil as tryAcquire would have got the lock
				// then. Suppress sending changed notification when the
				// value doesn't change.
				if e.holding {
					// Somehow this node's lock was released.
					glog.Errorf("Lost lock: old value %s, new "+
						"value %s", e.id, resp.Node.Value)

					e.holding = false
					refreshStopCh <- true
					// Create a new channel for the next go routine.
					refreshStopCh = make(chan bool, 1)
					e.eventsCh <- MasterEvent{Type: MasterDeleted,
						Master: ""}
				}
				glog.V(2).Infof("Lock holder for %s changed to %s",
					e.name, resp.Node.Value)
				e.eventsCh <- MasterEvent{Type: MasterModified,
					Master: resp.Node.Value}
			}
		case <-e.stopCh:
			// Release was called, stop the watch routine. If holding
			// the lock, stop the refresh routine and delete the key.
			glog.V(2).Infof("Stopping watch for lock %s", e.name)
			watchStopCh <- true
			if e.holding {
				refreshStopCh <- true
				refreshStopCh = make(chan bool, 1)
				glog.Infof("Deleting lock %s", e.name)
				if _, err := e.client.Delete(e.name,
					false); err != nil {
					// Not fatal because ttl would expire.
					glog.Errorf("Failed to delete lock %s with "+
						"error: %s", e.name, err.Error())
				}

				e.holding = false
				e.eventsCh <- MasterEvent{Type: MasterDeleted,
					Master: ""}
			}
			e.stoppedCh <- true
			return nil
		case <-watchFailCh:
			// etcd client Watch closes the channel, hence the need
			// to recreate.
			watchCh = make(chan *etcd.Response, 1)

			// Restart the watch.
			go e.watch(watchCh, watchStopCh, watchFailCh)
		}
	}

	return ret
}

// Method to try and acquire the lock.
func (e *etcdLock) tryAcquire(refreshStopCh chan bool) {
	glog.V(2).Infof("tryAcquire lock %s", e.name)
	// Attempt to create the key.
	if resp, err := e.client.Create(e.name, e.id, e.ttl); err == nil {
		// Acquired the lock.
		glog.Infof("Acquired lock %s", e.name)
		e.holding = true
		e.modifiedIndex = resp.Node.ModifiedIndex
		e.eventsCh <- MasterEvent{Type: MasterAdded, Master: e.id}
		go e.refresh(refreshStopCh)
	}
}

// Method to watch the lock.
func (e *etcdLock) watch(watchCh chan *etcd.Response, watchStopCh chan bool,
	watchFailCh chan bool) {
	glog.V(2).Infof("watch lock %s", e.name)
	if _, err := e.client.Watch(e.name, 0, false, watchCh,
		watchStopCh); IsEtcdWatchStoppedByUser(err) {
		glog.V(2).Infof("Stopping watch for lock %s", e.name)
		return
	} else {
		glog.Infof("Watch returned err %s for key %s", err.Error(),
			e.name)
		watchFailCh <- true
	}
}

// Method to refresh the lock. It refreshes the ttl at ttl*4/10 interval.
func (e *etcdLock) refresh(stopCh chan bool) {
	for {
		select {
		case <-time.After(time.Second * time.Duration(e.ttl*4/10)):
			// Uses CompareAndSwap to protect against the case where a
			// watch is received with a "delete" and refresh routine is
			// still running.
			if resp, err := e.client.CompareAndSwap(e.name, e.id, e.ttl,
				e.id, e.modifiedIndex); err != nil {
				// Failure here could mean that some other node
				// acquired the lock. Should also get a watch
				// notification if that happens and this go routine
				// is stopped there.
				glog.Errorf("Failed to set the ttl for lock %s with "+
					"error: %s", e.name, err.Error())
			} else {
				e.modifiedIndex = resp.Node.ModifiedIndex
			}
		case <-stopCh:
			glog.V(2).Infof("Stopping refresh for lock %s", e.name)
			// Lock released.
			return
		}
	}
}
