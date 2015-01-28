/*
Copyright 2015 Datawise Systems Inc. All rights reserved.

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

// Utility that implements distributed read/write locks using etcd.
//
// Assumptions:
// 1) Each lock request is added to a per lock queue in etcd. The queue is
//    implemented using atomic in order keys in etcd with a queue-ttl. The
//    queue-ttl is a constant for now (30 seconds). This is the maximum
//    amount of time a lock request can be waiting in queue.
// 2) If a node queues a lock request and goes away, queue-ttl expiry will
//    ensure that the next set of lock requests can be served.
// 3) lock-ttl is specified in the lock request. This controls how long a
//    lockholder can hang on to the lock. If a lockholder dies, the lock-ttl
//    expiry ensures that the lock is released in a reasonable time.
// 4) Fairness is ensured by never letting reads leapfrog writes ahead of them.
//
// Implementation notes:
// 1) Each lock request adds a key to the wait queue for the lock. The value
//    stored contains the type of operation(Read/Write) and the ID of the
//    requestor. The key has a queue-ttl associated with it.
// 2) A lock can be assigned if all the queue entries ahead of the requestor
//    are non-conflicting. Reads dont conflict with each other. Writes conflict
//    with both Reads and Writes.
// 3) Every time a watch event is received with expire or delete action, the
//    queue is reevaluated to see if the lock can be granted.
// 4) When a lock is granted, the corresponding key in the queue is refereshed
//    with lock-ttl.
// 5) The queue entry is deleted in the unlock function or on ttl expiry.

// NOTE:
//
// Please provide the following function to compile this code:
// func GetLockID() (string, error)

package utils

import (
	"encoding/json"
	"errors"
	"flag"
	"path"

	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/glog"
)

// Possible errors returned by Lock/Unlock.
var (
	ErrLockID        = errors.New("Can't get lock id")
	ErrLockExpired   = errors.New("Lock request expired")
	ErrLockReqLost   = errors.New("Lock request lost")
	ErrLockEnq       = errors.New("Error enqueuing lock request")
	ErrLockGet       = errors.New("Error reading lock queue")
	ErrLockDelete    = errors.New("Error deleting lock")
	ErrLockMarshal   = errors.New("Marshal error")
	ErrLockUnmarshal = errors.New("Unmarshal error")
)

var QTTL = flag.Uint64("queue-ttl", 30, "Timeout in seconds for a lock queue entry")

const (
	kLocksDir = "/rwlocks" // Locks directory
)

// Lock types.
type LockType int
type LockHandle string

const (
	LockTypeRead LockType = iota
	LockTypeWrite
)

var LockTypes = map[LockType]string{
	LockTypeRead:  "read-lock",
	LockTypeWrite: "write-lock",
}

// LockState is used to track lock holders/requests for a given lock and is
// stored as the value in etcd.
type LockState struct {
	LockType string
	Id       string
}

// RLock is a function to acquire a read lock.
//
// Inputs:
// client     - etcd client object.
// lockType   - type of the lock, read or write.
// name       - name of the lock.
// ttl        - time needed to complete the work after acquiring lock. If the
//              lock is not released within this time by calling Unlock, it is
//              automatically released.
// Outputs:
// LockHandle - lock handle to provide to UnLock.
// error      - any etcd errors or ttl expiry.
func RLock(client Registry, name string, ttl uint64) (LockHandle, error) {
	return rwLock(client, LockTypeRead, name, ttl)
}

// WLock is a function to acquire a write lock.
//
// Inputs:
// client     - etcd client object.
// lockType   - type of the lock, read or write.
// name       - name of the lock.
// ttl        - time needed to complete the work after acquiring lock. If the
//              lock is not released within this time by calling Unlock, it is
//              automatically released.
// Outputs:
// LockHandle - lock handle to provide to UnLock.
// error      - any etcd errors or ttl expiry.
func WLock(client Registry, name string, ttl uint64) (LockHandle, error) {
	return rwLock(client, LockTypeWrite, name, ttl)
}

// RUnlock is used to unlock a read lock.
func RUnlock(client Registry, name string, handle LockHandle) error {
	return rwUnlock(client, name, handle)
}

// WUnlock is used to unlock a write lock.
func WUnlock(client Registry, name string, handle LockHandle) error {
	return rwUnlock(client, name, handle)
}

// rwLock is a helper function to attempt acquiring a lock.
func rwLock(client Registry, lockType LockType, name string, ttl uint64) (LockHandle,
	error) {

	// id is the value to be stored against the lock request. It is node
	// id + ":" + process id.
	id, err := GetLockID()
	if err != nil {
		glog.Errorf("Unable to get lock id for lock %s", name)
		return "", ErrLockID
	}

	glog.V(2).Infof("%s %s for %s with ttl %d", LockTypes[lockType], name,
		id, ttl)

	// Enqueue the lock request.
	handle, err := enqLock(client, lockType, name, id)
	if err != nil {
		return "", err
	}

	// Wait until the lock is acquired or ttl expires.
	if err := waitForLock(client, lockType, name, handle, id, ttl); err != nil {
		return "", err
	}

	return LockHandle(handle), nil
}

// rwUnlock releases the lock with the provided name and lock handle.
func rwUnlock(client Registry, name string, handle LockHandle) error {
	glog.V(2).Infof("Unlock called for lock %s, handle %s", name, handle)

	key := path.Join(kLocksDir, name, string(handle))

	if _, err := client.Delete(key, false); err != nil {
		glog.Errorf("Failed to unlock %s/%s with error %s", name,
			handle, err.Error())
		return ErrLockDelete
	}

	return nil
}

// Helper function to enqueue the lock request.
func enqLock(client Registry, lockType LockType, name string, id string) (string, error) {

	// Value to write to etcd.
	state := &LockState{
		LockType: LockTypes[lockType],
		Id:       id,
	}

	out, err := json.Marshal(&state)
	if err != nil {
		glog.Errorf("Failed to marshal lock %s with id %s: %s",
			name, id, err.Error())
		return "", ErrLockMarshal
	}

	// CreateInOrder is used to atomically insert at the end of the lock queue.
	resp, err := client.CreateInOrder(path.Join(kLocksDir, name),
		string(out), *QTTL)
	if err != nil {
		glog.Errorf("Error enqueuing lock %s with id %s: %s",
			name, id, err.Error())
		return "", ErrLockEnq
	}

	_, handle := path.Split(resp.Node.Key)

	glog.V(2).Infof("Got handle %s for lock %s with id %s", handle, name, id)

	return handle, nil
}

// Helper function to wait for a lock.
func waitForLock(client Registry, lockType LockType, name string, handle string,
	id string, ttl uint64) error {

	// Setup the watch to look for Unlock, ttl expiry events.
	watchCh := make(chan *etcd.Response, 1)
	watchFailCh := make(chan bool, 1)
	watchStopCh := make(chan bool, 1)

	go func() {
		glog.V(2).Infof("Starting watch for %s %s with handle %s",
			LockTypes[lockType], name, handle)
		watch(client, name, watchCh, watchStopCh, watchFailCh)
	}()

	defer func() {
		glog.V(2).Infof("Stopping watch for %s %s with handle %s",
			LockTypes[lockType], name, handle)
		watchStopCh <- true
	}()

	// Check to see if the lock can be acquired.
	if acquired, err := tryLock(client, lockType, name, handle, id, ttl); err != nil {
		return err
	} else if acquired {
		glog.V(2).Infof("Got %s %s with handle %s", LockTypes[lockType],
			name, handle)
		return nil
	}

	// Need to wait until a lock expires or is released.
	for {
		select {
		case resp := <-watchCh:
			if resp == nil {
				glog.Info("Got nil resp in watch channel")
				continue
			}

			glog.V(2).Infof("Watch resp action is %s for %s %s with "+
				"handle %s", resp.Action, LockTypes[lockType],
				name, handle)

			// Some node called unlock or a ttl expired. See if the
			// request can go through now. If this request expired,
			// return an error.
			if resp.Action == "expire" || resp.Action == "delete" {

				if _, h := path.Split(resp.PrevNode.Key); h == handle {
					// TODO: Delete should not happen when waiting.
					glog.Errorf("Lock request for %s with handle "+
						"%s expired", name, handle)
					return ErrLockExpired
				}

				if acquired, err := tryLock(client, lockType, name,
					handle, id, ttl); err != nil {
					return err
				} else if acquired {
					glog.V(2).Infof("Got %s %s with handle %s",
						LockTypes[lockType], name, handle)
					return nil
				}
			}
		case <-watchFailCh:
			// etcd client watch closes the channel, hence the need
			// to recreate.
			watchCh = make(chan *etcd.Response, 1)

			// Restart the watch.
			go watch(client, name, watchCh, watchStopCh, watchFailCh)
		}
	}

	return nil
}

// Helper function to check if the lock can be acquired. Returns true on success.
func tryLock(client Registry, lockType LockType, name string, handle string,
	id string, ttl uint64) (bool, error) {

	// Get the lock queue in sorted order.
	resp, err := client.Get(path.Join(kLocksDir, name), true, true)
	if err != nil {
		glog.Errorf("Failed to read lock queue for %s with error %s",
			name, err.Error())
		return false, ErrLockGet
	}

	for ii, node := range resp.Node.Nodes {
		var state LockState
		if err := json.Unmarshal([]byte(node.Value), &state); err != nil {
			glog.Errorf("Unmarshal of lock %s failed with error %s",
				name, err.Error())
			return false, ErrLockUnmarshal
		}

		_, h := path.Split(node.Key)

		// For a write lock request to be successful, it needs to be
		// first in the queue.
		if lockType == LockTypeWrite && ii == 0 {
			if state.LockType != LockTypes[LockTypeWrite] ||
				h != handle {
				return false, nil
			}
			if err := refreshTTL(client, lockType, name, handle, id,
				ttl); err != nil {
				return false, err
			}
			return true, nil
		}

		// For a read lock request to be successful, it should not hit
		// a write until its position in the queue.
		if state.LockType != LockTypes[LockTypeRead] {
			return false, nil
		}

		// Reached the entry with matching handle, got the lock.
		if h == handle {
			if err := refreshTTL(client, lockType, name, handle, id,
				ttl); err != nil {
				return false, err
			}
			return true, nil
		}
	}

	return false, ErrLockReqLost
}

// Helper function to refresh ttl.
func refreshTTL(client Registry, lockType LockType, name string, handle string,
	id string, ttl uint64) error {
	// Value to write to etcd.
	state := &LockState{
		LockType: LockTypes[lockType],
		Id:       id,
	}

	out, err := json.Marshal(&state)
	if err != nil {
		glog.Errorf("Failed to marshal lock %s with id %s: %s",
			name, id, err.Error())
		return ErrLockMarshal
	}
	if _, err := client.Update(path.Join(kLocksDir, name, handle), string(out),
		ttl); err != nil {
		glog.Errorf("Failed to refresh TTL lock %s with id %s: %s",
			name, id, err.Error())
		return ErrLockExpired
	}

	return nil
}

// Helper function to watch the lock. Watch is a blocking call. Launched in a
// separate go routine.
func watch(client Registry, name string, watchCh chan *etcd.Response,
	watchStopCh chan bool, watchFailCh chan bool) {
	if _, err := client.Watch(path.Join(kLocksDir, name), 0, true, watchCh,
		watchStopCh); IsEtcdWatchStoppedByUser(err) {
		return
	} else {
		glog.Errorf("Watch returned err %s for key %s", err.Error(), name)
		watchFailCh <- true
	}
}
