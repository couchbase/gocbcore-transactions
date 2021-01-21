package transactions

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v9"
)

type transactionAttempt struct {
	// immutable state
	expiryTime          time.Time
	txnStartTime        time.Time
	operationTimeout    time.Duration
	durabilityLevel     DurabilityLevel
	transactionID       string
	id                  string
	hooks               TransactionHooks
	enableNonFatalGets  bool
	disableCompoundOps  bool
	disableCBD3838Fix   bool
	serialUnstaging     bool
	explicitAtrs        bool
	atrLocation         ATRLocation
	bucketAgentProvider BucketAgentProviderFn

	// mutable state
	state             AttemptState
	stateBits         uint32
	stagedMutations   []*stagedMutation
	atrAgent          *gocbcore.Agent
	atrScopeName      string
	atrCollectionName string
	atrKey            []byte
	previousErrors    []*TransactionOperationFailedError

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

func (t *transactionAttempt) Serialize(cb func([]byte, error)) error {
	var res jsonSerializedAttempt

	t.waitForOpsAndLock(func(unlock func()) {
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

		res.Config.OperationTimeoutMs = int(t.operationTimeout / time.Millisecond)
		res.Config.DurabilityLevel = durabilityLevelToString(t.durabilityLevel)
		res.Config.NumAtrs = 1024

		res.State.TimeLeftMs = int(t.expiryTime.Sub(time.Now()) / time.Millisecond)

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
