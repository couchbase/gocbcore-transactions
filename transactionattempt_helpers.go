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
	"bytes"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/pkg/errors"
)

func hasExpired(expiryTime time.Time) bool {
	return time.Now().After(expiryTime)
}

func (t *transactionAttempt) beginOpAndLock(cb func(unlock func(), endOp func())) {
	t.lock.Lock(func(unlock func()) {
		t.opsWg.Add(1)

		cb(unlock, func() {
			t.opsWg.Done()
		})
	})
}

func (t *transactionAttempt) waitForOpsAndLock(cb func(unlock func())) {
	var tryWaitAndLock func()
	tryWaitAndLock = func() {
		t.opsWg.Wait(func() {
			t.lock.Lock(func(unlock func()) {
				if !t.opsWg.IsEmpty() {
					unlock()
					tryWaitAndLock()
					return
				}

				cb(unlock)
			})
		})
	}
	tryWaitAndLock()
}

func (t *transactionAttempt) checkCanPerformOpLocked() *TransactionOperationFailedError {
	switch t.state {
	case AttemptStateNothingWritten:
		fallthrough
	case AttemptStatePending:
		// Good to continue
	case AttemptStateCommitting:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				errors.Wrap(ErrIllegalState, "transaction is ambiguously committed")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            ErrorReasonTransactionFailed,
		})
	case AttemptStateCommitted:
		fallthrough
	case AttemptStateCompleted:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				errors.Wrap(ErrIllegalState, "transaction already committed")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            ErrorReasonTransactionFailed,
		})
	case AttemptStateAborted:
		fallthrough
	case AttemptStateRolledBack:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				errors.Wrap(ErrIllegalState, "transaction already aborted")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            ErrorReasonTransactionFailed,
		})
	default:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				errors.Wrap(ErrIllegalState, fmt.Sprintf("invalid transaction state: %v", t.state))),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            ErrorReasonTransactionFailed,
		})
	}

	stateBits := atomic.LoadUint32(&t.stateBits)
	if (stateBits & transactionStateBitShouldNotCommit) != 0 {
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				errors.Wrap(ErrPreviousOperationFailed, "previous operation prevents further operations")),
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            ErrorReasonTransactionFailed,
		})
	}

	return nil
}

func (t *transactionAttempt) checkCanCommitRollbackLocked() *TransactionOperationFailedError {
	switch t.state {
	case AttemptStateNothingWritten:
		fallthrough
	case AttemptStatePending:
		// Good to continue
	case AttemptStateCommitting:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				errors.Wrap(ErrIllegalState, "transaction is ambiguously committed")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            ErrorReasonTransactionFailed,
		})
	case AttemptStateCommitted:
		fallthrough
	case AttemptStateCompleted:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				errors.Wrap(ErrIllegalState, "transaction already committed")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            ErrorReasonTransactionFailed,
		})
	case AttemptStateAborted:
		fallthrough
	case AttemptStateRolledBack:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				errors.Wrap(ErrIllegalState, "transaction already aborted")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            ErrorReasonTransactionFailed,
		})
	default:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				errors.Wrap(ErrIllegalState, fmt.Sprintf("invalid transaction state: %v", t.state))),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            ErrorReasonTransactionFailed,
		})
	}

	return nil
}

func (t *transactionAttempt) checkCanCommitLocked() *TransactionOperationFailedError {
	err := t.checkCanCommitRollbackLocked()
	if err != nil {
		return err
	}

	stateBits := atomic.LoadUint32(&t.stateBits)
	if (stateBits & transactionStateBitShouldNotCommit) != 0 {
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				errors.Wrap(ErrPreviousOperationFailed, "previous operation prevents commit")),
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            ErrorReasonTransactionFailed,
		})
	}

	return nil
}

func (t *transactionAttempt) checkCanRollbackLocked() *TransactionOperationFailedError {
	err := t.checkCanCommitRollbackLocked()
	if err != nil {
		return err
	}

	stateBits := atomic.LoadUint32(&t.stateBits)
	if (stateBits & transactionStateBitShouldNotRollback) != 0 {
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				errors.Wrap(ErrPreviousOperationFailed, "previous operation prevents rollback")),
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            ErrorReasonTransactionFailed,
		})
	}

	return nil
}

func (t *transactionAttempt) setExpiryOvertimeAtomic() {
	t.applyStateBits(transactionStateBitHasExpired)
}

func (t *transactionAttempt) isExpiryOvertimeAtomic() bool {
	stateBits := atomic.LoadUint32(&t.stateBits)
	return (stateBits & transactionStateBitHasExpired) != 0
}

func (t *transactionAttempt) checkExpiredAtomic(stage string, id []byte, proceedInOvertime bool, cb func(*classifiedError)) {
	if proceedInOvertime && t.isExpiryOvertimeAtomic() {
		cb(nil)
		return
	}

	t.hooks.HasExpiredClientSideHook(stage, id, func(expired bool, err error) {
		if err != nil {
			cb(classifyError(errors.Wrap(err, "HasExpired hook returned an unexpected error")))
			return
		}

		if expired {
			cb(classifyError(errors.Wrap(ErrAttemptExpired, "a hook has marked this attempt expired")))
			return
		} else if hasExpired(t.expiryTime) {
			cb(classifyError(errors.Wrap(ErrAttemptExpired, "the expiry for the attempt was reached")))
			return
		}

		cb(nil)
	})
}

func (t *transactionAttempt) confirmATRPending(
	firstAgent *gocbcore.Agent,
	firstOboUser string,
	firstScopeName string,
	firstCollectionName string,
	firstKey []byte,
	cb func(*TransactionOperationFailedError),
) {
	t.lock.Lock(func(unlock func()) {
		unlockAndCb := func(err *TransactionOperationFailedError) {
			unlock()
			cb(err)
		}

		if t.state != AttemptStateNothingWritten {
			unlockAndCb(nil)
			return
		}

		t.selectAtrLocked(
			firstAgent,
			firstOboUser,
			firstScopeName,
			firstCollectionName,
			firstKey,
			func(err *TransactionOperationFailedError) {
				if err != nil {
					unlockAndCb(err)
					return
				}

				t.setATRPendingLocked(func(err *TransactionOperationFailedError) {
					if err != nil {
						unlockAndCb(err)
						return
					}

					t.state = AttemptStatePending

					unlockAndCb(nil)
				})
			})
	})
}

func (t *transactionAttempt) getStagedMutationLocked(
	bucketName, scopeName, collectionName string, key []byte,
) (int, *stagedMutation) {
	for i, mutation := range t.stagedMutations {
		if mutation.Agent.BucketName() == bucketName &&
			mutation.ScopeName == scopeName &&
			mutation.CollectionName == collectionName &&
			bytes.Compare(mutation.Key, key) == 0 {
			return i, mutation
		}
	}

	return -1, nil
}

func (t *transactionAttempt) removeStagedMutation(
	bucketName, scopeName, collectionName string, key []byte,
	cb func(),
) {
	t.lock.Lock(func(unlock func()) {
		mutIdx, _ := t.getStagedMutationLocked(bucketName, scopeName, collectionName, key)
		if mutIdx >= 0 {
			// Not finding the item should be basically impossible, but we wrap it just in case...
			t.stagedMutations = append(t.stagedMutations[:mutIdx], t.stagedMutations[mutIdx+1:]...)
		}

		unlock()
		cb()
	})
}

func (t *transactionAttempt) recordStagedMutation(
	stagedInfo *stagedMutation,
	cb func(),
) {
	if !t.enableMutationCaching {
		stagedInfo.Staged = nil
	}

	t.lock.Lock(func(unlock func()) {
		mutIdx, _ := t.getStagedMutationLocked(
			stagedInfo.Agent.BucketName(),
			stagedInfo.ScopeName,
			stagedInfo.CollectionName,
			stagedInfo.Key)
		if mutIdx >= 0 {
			t.stagedMutations[mutIdx] = stagedInfo
		} else {
			t.stagedMutations = append(t.stagedMutations, stagedInfo)
		}

		unlock()
		cb()
	})
}

func (t *transactionAttempt) checkForwardCompatability(
	stage forwardCompatStage,
	fc map[string][]ForwardCompatibilityEntry,
	forceNonFatal bool,
	cb func(*TransactionOperationFailedError),
) {
	isCompat, shouldRetry, retryWait, err := checkForwardCompatability(stage, fc)
	if err != nil {
		cb(t.operationFailed(operationFailedDef{
			Cerr:              classifyError(err),
			CanStillCommit:    forceNonFatal,
			ShouldNotRetry:    false,
			ShouldNotRollback: false,
			Reason:            ErrorReasonTransactionFailed,
		}))
		return
	}

	if !isCompat {
		if shouldRetry {
			cbRetryError := func() {
				cb(t.operationFailed(operationFailedDef{
					Cerr:              classifyError(ErrForwardCompatibilityFailure),
					CanStillCommit:    forceNonFatal,
					ShouldNotRetry:    false,
					ShouldNotRollback: false,
					Reason:            ErrorReasonTransactionFailed,
				}))
			}

			if retryWait > 0 {
				time.AfterFunc(retryWait, cbRetryError)
			} else {
				cbRetryError()
			}

			return
		}

		cb(t.operationFailed(operationFailedDef{
			Cerr:              classifyError(ErrForwardCompatibilityFailure),
			CanStillCommit:    forceNonFatal,
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            ErrorReasonTransactionFailed,
		}))
		return
	}

	cb(nil)
	return
}

func (t *transactionAttempt) getTxnState(
	srcBucketName string,
	srcScopeName string,
	srcCollectionName string,
	srcDocID []byte,
	atrBucketName string,
	atrScopeName string,
	atrCollectionName string,
	atrDocID string,
	attemptID string,
	forceNonFatal bool,
	cb func(*jsonAtrAttempt, time.Time, *TransactionOperationFailedError),
) {
	ecCb := func(res *jsonAtrAttempt, txnExp time.Time, cerr *classifiedError) {
		if cerr == nil {
			cb(res, txnExp, nil)
			return
		}

		switch cerr.Class {
		case ErrorClassFailPathNotFound:
			// If the path is not found, we just return as if there was no
			// entry data available for that atr entry.
			cb(nil, time.Time{}, nil)
		case ErrorClassFailDocNotFound:
			// If the ATR is not found, we just return as if there was no
			// entry data available for that atr entry.
			cb(nil, time.Time{}, nil)
		default:
			cb(nil, time.Time{}, t.operationFailed(operationFailedDef{
				Cerr: classifyError(&writeWriteConflictError{
					Source:         cerr.Source,
					BucketName:     srcBucketName,
					ScopeName:      srcScopeName,
					CollectionName: srcCollectionName,
					DocumentKey:    srcDocID,
				}),
				CanStillCommit:    forceNonFatal,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		}
	}

	atrAgent, atrOboUser, err := t.bucketAgentProvider(atrBucketName)
	if err != nil {
		ecCb(nil, time.Time{}, classifyError(err))
		return
	}

	t.hooks.BeforeCheckATREntryForBlockingDoc([]byte(atrDocID), func(err error) {
		if err != nil {
			ecCb(nil, time.Time{}, classifyHookError(err))
			return
		}

		var deadline time.Time
		if t.keyValueTimeout > 0 {
			deadline = time.Now().Add(t.keyValueTimeout)
		}

		_, err = atrAgent.LookupIn(gocbcore.LookupInOptions{
			ScopeName:      atrScopeName,
			CollectionName: atrCollectionName,
			Key:            []byte(atrDocID),
			Ops: []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpGet,
					Path:  "attempts." + attemptID,
					Flags: memd.SubdocFlagXattrPath,
				},
				{
					Op:    memd.SubDocOpGet,
					Path:  hlcMacro,
					Flags: memd.SubdocFlagXattrPath,
				},
			},
			Deadline: deadline,
			User:     atrOboUser,
		}, func(result *gocbcore.LookupInResult, err error) {
			if err != nil {
				ecCb(nil, time.Time{}, classifyError(err))
				return
			}

			for _, op := range result.Ops {
				if op.Err != nil {
					ecCb(nil, time.Time{}, classifyError(op.Err))
					return
				}
			}

			var txnAttempt *jsonAtrAttempt
			if err := json.Unmarshal(result.Ops[0].Value, &txnAttempt); err != nil {
				ecCb(nil, time.Time{}, classifyError(err))
				return
			}

			var hlc *jsonHLC
			if err := json.Unmarshal(result.Ops[1].Value, &hlc); err != nil {
				ecCb(nil, time.Time{}, classifyError(err))
				return
			}

			nowSecs, err := parseHLCToSeconds(*hlc)
			if err != nil {
				ecCb(nil, time.Time{}, classifyError(err))
				return
			}

			txnStartMs, err := parseCASToMilliseconds(txnAttempt.PendingCAS)
			if err != nil {
				ecCb(nil, time.Time{}, classifyError(err))
				return
			}

			nowTime := time.Duration(nowSecs) * time.Second
			txnStartTime := time.Duration(txnStartMs) * time.Millisecond
			txnExpiryTime := time.Duration(txnAttempt.ExpiryTime) * time.Millisecond

			txnElapsedTime := nowTime - txnStartTime
			txnExpiry := time.Now().Add(txnExpiryTime - txnElapsedTime)

			ecCb(txnAttempt, txnExpiry, nil)
		})
		if err != nil {
			ecCb(nil, time.Time{}, classifyError(err))
			return
		}
	})
}

func (t *transactionAttempt) writeWriteConflictPoll(
	stage forwardCompatStage,
	agent *gocbcore.Agent,
	oboUser string,
	scopeName string,
	collectionName string,
	key []byte,
	cas gocbcore.Cas,
	meta *MutableItemMeta,
	existingMutation *stagedMutation,
	cb func(*TransactionOperationFailedError),
) {
	if meta == nil {
		// There is no write-write conflict.
		cb(nil)
		return
	}

	if meta.TransactionID == t.transactionID {
		if meta.AttemptID == t.id {
			if existingMutation != nil {
				if cas != existingMutation.Cas {
					// There was an existing mutation but it doesn't match the expected
					// CAS.  We throw a CAS mismatch to early detect this.
					cb(t.operationFailed(operationFailedDef{
						Cerr: classifyError(
							errors.Wrap(ErrCasMismatch, "cas mismatch occured against local staged mutation")),
						ShouldNotRetry:    false,
						ShouldNotRollback: false,
						Reason:            ErrorReasonTransactionFailed,
					}))
					return
				}

				cb(nil)
				return
			}

			// This means that we are trying to overwrite a previous write this specific
			// attempt has performed without actually having found the existing mutation,
			// this is never going to work correctly.
			cb(t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					errors.Wrap(ErrIllegalState, "attempted to overwrite local staged mutation but couldn't find it")),
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
			return
		}

		// The transaction matches our transaction.  We can safely overwrite the existing
		// data in the txn meta and continue.
		cb(nil)
		return
	}

	deadline := time.Now().Add(1 * time.Second)

	var onePoll func()
	onePoll = func() {
		if !time.Now().Before(deadline) {
			// If the deadline expired, lets just immediately return.
			cb(t.operationFailed(operationFailedDef{
				Cerr: classifyError(&writeWriteConflictError{
					Source: fmt.Errorf(
						"deadline expired before WWC was resolved on %s.%s.%s.%s",
						meta.ATR.BucketName,
						meta.ATR.ScopeName,
						meta.ATR.CollectionName,
						meta.ATR.DocID),
					BucketName:     agent.BucketName(),
					ScopeName:      scopeName,
					CollectionName: collectionName,
					DocumentKey:    key,
				}),
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
			return
		}

		t.checkForwardCompatability(stage, meta.ForwardCompat, false, func(err *TransactionOperationFailedError) {
			if err != nil {
				cb(err)
				return
			}

			t.checkExpiredAtomic(hookWWC, key, false, func(cerr *classifiedError) {
				if cerr != nil {
					cb(t.operationFailed(operationFailedDef{
						Cerr:              cerr,
						ShouldNotRetry:    true,
						ShouldNotRollback: false,
						Reason:            ErrorReasonTransactionExpired,
					}))
					return
				}

				t.getTxnState(
					agent.BucketName(),
					scopeName,
					collectionName,
					key,
					meta.ATR.BucketName,
					meta.ATR.ScopeName,
					meta.ATR.CollectionName,
					meta.ATR.DocID,
					meta.AttemptID,
					false,
					func(attempt *jsonAtrAttempt, expiry time.Time, err *TransactionOperationFailedError) {
						if err != nil {
							cb(err)
							return
						}

						if attempt == nil {
							// The ATR entry is missing, which counts as it being completed.
							cb(nil)
							return
						}

						state := jsonAtrState(attempt.State)
						if state == jsonAtrStateCompleted || state == jsonAtrStateRolledBack {
							// If we have progressed enough to continue, let's do that.
							cb(nil)
							return
						}

						time.AfterFunc(200*time.Millisecond, onePoll)
					})
			})
		})
	}
	onePoll()
}

func (t *transactionAttempt) ensureCleanUpRequest() {
	// BUG(TXNG-59): Do not use a synchronous lock for cleanup requests.
	// Because of the need to include the state of the transaction within the cleanup
	// request, we are not able to do registration until the end of commit/rollback,
	// which means that we no longer have the lock on the transaction, and need to
	// relock it.
	t.lock.LockSync()

	if t.state == AttemptStateCompleted || t.state == AttemptStateRolledBack {
		t.lock.UnlockSync()
		return
	}

	if t.hasCleanupRequest {
		t.lock.UnlockSync()
		return
	}

	t.hasCleanupRequest = true

	var inserts []DocRecord
	var replaces []DocRecord
	var removes []DocRecord
	for _, staged := range t.stagedMutations {
		dr := DocRecord{
			CollectionName: staged.CollectionName,
			ScopeName:      staged.ScopeName,
			BucketName:     staged.Agent.BucketName(),
			ID:             staged.Key,
		}

		switch staged.OpType {
		case StagedMutationInsert:
			inserts = append(inserts, dr)
		case StagedMutationReplace:
			replaces = append(replaces, dr)
		case StagedMutationRemove:
			removes = append(removes, dr)
		}
	}

	var bucketName string
	if t.atrAgent != nil {
		bucketName = t.atrAgent.BucketName()
	}

	cleanupState := t.state
	if cleanupState == AttemptStateCommitting {
		cleanupState = AttemptStatePending
	}

	req := &CleanupRequest{
		AttemptID:         t.id,
		AtrID:             t.atrKey,
		AtrCollectionName: t.atrCollectionName,
		AtrScopeName:      t.atrScopeName,
		AtrBucketName:     bucketName,
		Inserts:           inserts,
		Replaces:          replaces,
		Removes:           removes,
		State:             cleanupState,
		ForwardCompat:     nil, // Let's just be explicit about this, it'll change in the future anyway.
		DurabilityLevel:   t.durabilityLevel,
	}

	t.lock.UnlockSync()

	t.addCleanupRequest(req)
}
