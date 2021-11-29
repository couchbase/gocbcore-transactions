// Copyright 2021 Couchbase
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transactions

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v10"
)

type transactionAttempt struct {
	// immutable state
	expiryTime              time.Time
	txnStartTime            time.Time
	keyValueTimeout         time.Duration
	durabilityLevel         DurabilityLevel
	transactionID           string
	id                      string
	hooks                   TransactionHooks
	enableNonFatalGets      bool
	enableParallelUnstaging bool
	enableExplicitATRs      bool
	enableMutationCaching   bool
	atrLocation             ATRLocation
	bucketAgentProvider     BucketAgentProviderFn

	// mutable state
	state             AttemptState
	stateBits         uint32
	stagedMutations   []*stagedMutation
	atrAgent          *gocbcore.Agent
	atrOboUser        string
	atrScopeName      string
	atrCollectionName string
	atrKey            []byte

	unstagingComplete bool

	lock  asyncMutex
	opsWg asyncWaitGroup

	hasCleanupRequest bool
	addCleanupRequest addCleanupRequest
}

func (t *transactionAttempt) State() Attempt {
	state := Attempt{}

	t.lock.LockSync()

	stateBits := atomic.LoadUint32(&t.stateBits)

	state.State = t.state
	state.ID = t.id

	if stateBits&transactionStateBitHasExpired != 0 {
		state.Expired = true
	} else {
		state.Expired = false
	}

	if stateBits&transactionStateBitPreExpiryAutoRollback != 0 {
		state.PreExpiryAutoRollback = true
	} else {
		state.PreExpiryAutoRollback = false
	}

	if t.atrAgent != nil {
		state.AtrBucketName = t.atrAgent.BucketName()
		state.AtrScopeName = t.atrScopeName
		state.AtrCollectionName = t.atrCollectionName
		state.AtrID = t.atrKey
	} else {
		state.AtrBucketName = ""
		state.AtrScopeName = ""
		state.AtrCollectionName = ""
		state.AtrID = []byte{}
	}

	if t.state == AttemptStateCompleted {
		state.UnstagingComplete = true
	} else {
		state.UnstagingComplete = false
	}

	t.lock.UnlockSync()

	return state
}

func (t *transactionAttempt) HasExpired() bool {
	return t.isExpiryOvertimeAtomic()
}

func (t *transactionAttempt) CanCommit() bool {
	stateBits := atomic.LoadUint32(&t.stateBits)
	return (stateBits & transactionStateBitShouldNotCommit) == 0
}

func (t *transactionAttempt) ShouldRollback() bool {
	stateBits := atomic.LoadUint32(&t.stateBits)
	return (stateBits & transactionStateBitShouldNotRollback) == 0
}

func (t *transactionAttempt) ShouldRetry() bool {
	stateBits := atomic.LoadUint32(&t.stateBits)
	return (stateBits&transactionStateBitShouldNotRetry) == 0 &&
		(stateBits&transactionStateBitHasExpired) == 0
}

func (t *transactionAttempt) GetATRLocation() ATRLocation {
	t.lock.LockSync()

	if t.atrAgent != nil {
		location := ATRLocation{
			Agent:          t.atrAgent,
			ScopeName:      t.atrScopeName,
			CollectionName: t.atrCollectionName,
		}
		t.lock.UnlockSync()

		return location
	}
	t.lock.UnlockSync()

	return t.atrLocation
}

func (t *transactionAttempt) SetATRLocation(location ATRLocation) error {
	t.lock.LockSync()
	if t.atrAgent != nil {
		t.lock.UnlockSync()
		return errors.New("atr location cannot be set after mutations have occurred")
	}

	if t.atrLocation.Agent != nil {
		t.lock.UnlockSync()
		return errors.New("atr location can only be set once")
	}

	t.atrLocation = location

	t.lock.UnlockSync()
	return nil
}

func (t *transactionAttempt) GetMutations() []StagedMutation {
	mutations := make([]StagedMutation, len(t.stagedMutations))

	t.lock.LockSync()

	for mutationIdx, mutation := range t.stagedMutations {
		mutations[mutationIdx] = StagedMutation{
			OpType:         mutation.OpType,
			BucketName:     mutation.Agent.BucketName(),
			ScopeName:      mutation.ScopeName,
			CollectionName: mutation.CollectionName,
			Key:            mutation.Key,
			Cas:            mutation.Cas,
			Staged:         mutation.Staged,
		}
	}

	t.lock.UnlockSync()

	return mutations
}

func (t *transactionAttempt) TimeRemaining() time.Duration {
	curTime := time.Now()

	timeLeft := time.Duration(0)
	if curTime.Before(t.expiryTime) {
		timeLeft = t.expiryTime.Sub(curTime)
	}

	return timeLeft
}

func (t *transactionAttempt) Serialize(cb func([]byte, error)) error {
	var res jsonSerializedAttempt

	t.waitForOpsAndLock(func(unlock func()) {
		if err := t.checkCanCommitLocked(); err != nil {
			unlock()
			cb(nil, err)
			return
		}

		res.ID.Transaction = t.transactionID
		res.ID.Attempt = t.id

		if t.atrAgent != nil {
			res.ATR.Bucket = t.atrAgent.BucketName()
			res.ATR.Scope = t.atrScopeName
			res.ATR.Collection = t.atrCollectionName
			res.ATR.ID = string(t.atrKey)
		} else if t.atrLocation.Agent != nil {
			res.ATR.Bucket = t.atrLocation.Agent.BucketName()
			res.ATR.Scope = t.atrLocation.ScopeName
			res.ATR.Collection = t.atrLocation.CollectionName
			res.ATR.ID = ""
		}

		res.Config.KeyValueTimeoutMs = int(t.keyValueTimeout / time.Millisecond)
		res.Config.DurabilityLevel = durabilityLevelToString(t.durabilityLevel)
		res.Config.NumAtrs = 1024

		res.State.TimeLeftMs = int(t.TimeRemaining().Milliseconds())

		for _, mutation := range t.stagedMutations {
			var mutationData jsonSerializedMutation

			mutationData.Bucket = mutation.Agent.BucketName()
			mutationData.Scope = mutation.ScopeName
			mutationData.Collection = mutation.CollectionName
			mutationData.ID = string(mutation.Key)
			mutationData.Cas = fmt.Sprintf("%d", mutation.Cas)
			mutationData.Type = stagedMutationTypeToString(mutation.OpType)

			res.Mutations = append(res.Mutations, mutationData)
		}
		if len(res.Mutations) == 0 {
			res.Mutations = []jsonSerializedMutation{}
		}

		unlock()

		resBytes, err := json.Marshal(res)
		if err != nil {
			cb(nil, err)
			return
		}

		cb(resBytes, nil)
	})
	return nil
}
