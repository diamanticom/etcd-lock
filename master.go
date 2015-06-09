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

// Utility to perform master election/failover using etcd.

package utils

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/glog"
)

const kRetrySleep time.Duration = 100 // milliseconds

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
	sync.Mutex
	client        Registry         // etcd interface
	name          string           // name of the lock
	id            string           // identity of the lockholder
	ttl           uint64           // ttl of the lock
	enabled       bool             // Used to enable/disable the lock
	master        string           // Lock holder
	watchStopCh   chan bool        // To stop the watch
	eventsCh      chan MasterEvent // channel to send lock ownership updates
	stoppedCh     chan bool        // channel that waits for acquire to finish
	refreshStopCh chan bool        // channel used to stop the refresh routine
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
		enabled:       false,
		master:        "",
		watchStopCh:   make(chan bool, 1),
		eventsCh:      make(chan MasterEvent, 1),
		stoppedCh:     make(chan bool, 1),
		refreshStopCh: make(chan bool, 1),
		holding:       false,
		modifiedIndex: 0}, nil
}

// Method to start the attempt to acquire the lock.
func (e *etcdLock) Start() {
	glog.Infof("Starting attempt to acquire lock %s", e.name)

	e.Lock()
	if e.enabled {
		e.Unlock()
		// Already running
		glog.Warningf("Duplicate Start for lock %s", e.name)
		return
	}

	e.enabled = true
	e.Unlock()

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
	glog.Infof("Stopping attempt to acquire lock %s", e.name)

	e.Lock()
	if !e.enabled {
		e.Unlock()
		// Not running
		glog.Warningf("Duplicate Stop for lock %s", e.name)
		return
	}

	// Disable the lock and stop the watch.
	e.enabled = false
	e.Unlock()

	e.watchStopCh <- true

	// Wait for acquire to finish.
	<-e.stoppedCh
}

// Method to get the event channel used by the etcd lock.
func (e *etcdLock) EventsChan() <-chan MasterEvent {
	return e.eventsCh
}

// Method to get the lockholder.
func (e *etcdLock) GetHolder() string {
	e.Lock()
	defer e.Unlock()
	return e.master
}

// Method to acquire the lock. It launches another goroutine to refresh the ttl
// if successful in acquiring the lock.
func (e *etcdLock) acquire() (ret error) {
	defer func() {
		if r := recover(); r != nil {
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

	var resp *etcd.Response
	// Initialize error to dummy.
	err := fmt.Errorf("Dummy error")

	for {
		// Stop was called, stop the refresh routine if needed and
		// abort the acquire routine.
		if !e.enabled {
			if e.holding {
				glog.V(2).Infof("Deleting lock %s", e.name)
				// Delete the lock so other nodes can get it sooner.
				// Otherwise, they have to wait until ttl expiry.
				if _, err = e.client.Delete(e.name, false); err != nil {
					glog.V(2).Infof("Failed to delete lock %s, "+
						"error %v", e.name, err)
				}
				e.holding = false
				e.refreshStopCh <- true
			}
			// Wont be able to track the master.
			e.Lock()
			e.master = ""
			e.Unlock()

			e.stoppedCh <- true
			break
		}

		// If there is an error (at the beginning or with watch) or if
		// the lock is deleted, try to get the lockholder/acquire the lock.
		if err != nil || resp.Node.Value == "" {
			resp, err = e.client.Get(e.name, false, false)
			if err != nil {
				if IsEtcdErrorNotFound(err) {
					// Try to acquire the lock.
					glog.V(2).Infof("Trying to acquire lock %s", e.name)
					resp, err = e.client.Create(e.name, e.id, e.ttl)
					if err != nil {
						// Failed to acquire the lock.
						continue
					}
				} else {
					glog.V(2).Infof("Failed to get lock %s, error: %v",
						e.name, err)
					time.Sleep(kRetrySleep * time.Millisecond)
					continue
				}
			}
		}

		if resp.Node.Value == e.id {
			// This node is the lock holder.
			if !e.holding {
				// If not already holding the lock, send an
				// event and start the refresh routine.
				glog.Infof("Acquired lock %s", e.name)
				e.holding = true
				e.eventsCh <- MasterEvent{Type: MasterAdded,
					Master: e.id}
				go e.refresh()
			}
		} else {
			// Some other node is the lock holder.
			if e.holding {
				// If previously holding the lock, stop the
				// refresh routine and send a deleted event.
				glog.Errorf("Lost lock %s to %s", e.name,
					resp.Node.Value)
				e.holding = false
				e.refreshStopCh <- true
				e.eventsCh <- MasterEvent{Type: MasterDeleted,
					Master: ""}
			}
			if e.master != resp.Node.Value {
				// If master changed, send a modified event.
				e.eventsCh <- MasterEvent{Type: MasterModified,
					Master: resp.Node.Value}
			}
		}

		// Record the new master and modified index.
		e.Lock()
		e.master = resp.Node.Value
		e.Unlock()
		e.modifiedIndex = resp.Node.ModifiedIndex

		var prevIndex uint64

		// Intent is to start the watch using EtcdIndex. Sometimes, etcd
		// is returning EtcdIndex lower than ModifiedIndex. In such
		// cases, use ModifiedIndex to set the watch.
		// TODO: Change this code when etcd behavior changes.
		if resp.EtcdIndex < resp.Node.ModifiedIndex {
			prevIndex = resp.Node.ModifiedIndex + 1
		} else {
			prevIndex = resp.EtcdIndex + 1
		}

		// Start watching for changes to lock.
		resp, err = e.client.Watch(e.name, prevIndex, false, nil, e.watchStopCh)
		if IsEtcdErrorWatchStoppedByUser(err) {
			glog.Infof("Watch for lock %s stopped by user", e.name)
		} else if err != nil {
			// Log only if its not too old event index error.
			if !IsEtcdErrorEventIndexCleared(err) {
				glog.Errorf("Failed to watch lock %s, error %v",
					e.name, err)
			}
		}
	}

	return nil
}

// Method to refresh the lock. It refreshes the ttl at ttl*4/10 interval.
func (e *etcdLock) refresh() {
	for {
		select {
		case <-e.refreshStopCh:
			glog.V(2).Infof("Stopping refresh for lock %s", e.name)
			// Lock released.
			return
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
		}
	}
}
