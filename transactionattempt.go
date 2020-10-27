package transactions

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
)

var crc32cMacro = []byte("\"${Mutation.value_crc32c}\"")

type transactionAttempt struct {
	// immutable state
	expiryTime      time.Time
	txnStartTime    time.Time
	keyValueTimeout time.Duration
	durabilityLevel DurabilityLevel
	transactionID   string
	id              string
	hooks           TransactionHooks
	serialUnstaging bool

	// mutable state
	state               AttemptState
	stagedMutations     []*stagedMutation
	finalMutationTokens []MutationToken
	atrAgent            *gocbcore.Agent
	atrScopeName        string
	atrCollectionName   string
	atrKey              []byte
	expiryOvertimeMode  bool
	previousErrors      []*TransactionOperationFailedError

	unstagingComplete bool

	lock          sync.Mutex
	txnAtrSection atomicWaitQueue
	txnOpSection  atomicWaitQueue

	addCleanupRequest addCleanupRequest
}

func (t *transactionAttempt) Serialize(cb func([]byte, error)) error {
	// TODO(brett19): I think this needs to guarentee the transaction attempts
	// has already been started before generating the serialized result.

	// TODO(brett19): Ensure ATR and Op critical sections are completed.
	// This is probably not safe to do without first guarenteeing that all
	// pending operations have completed, and all pending ATR changes
	// have completed...
	var res jsonSerializedAttempt

	t.lock.Lock()

	res.ID.Transaction = t.transactionID
	res.ID.Attempt = t.id
	res.ATR.Bucket = t.atrAgent.BucketName()
	res.ATR.Scope = t.atrScopeName
	res.ATR.Collection = t.atrCollectionName
	res.ATR.ID = string(t.atrKey)

	// TODO(brett19): Use durable rather than kv timeout during serialization.
	res.Config.KvTimeoutMs = int(t.keyValueTimeout / time.Millisecond)
	res.Config.KvDurableTimeoutMs = int(t.keyValueTimeout / time.Millisecond)
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

	t.lock.Unlock()

	resBytes, err := json.Marshal(res)
	if err != nil {
		return err
	}

	cb(resBytes, nil)
	return nil
}

func (t *transactionAttempt) GetMutations() []StagedMutation {
	mutations := make([]StagedMutation, len(t.stagedMutations))
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
	return mutations
}

func (t *transactionAttempt) createAndStashOperationFailedError(shouldRetry, shouldNotRollback bool, cause error,
	raise ErrorReason, class ErrorClass, skipStash bool) error {
	err := &TransactionOperationFailedError{
		shouldRetry:       shouldRetry,
		shouldNotRollback: shouldNotRollback,
		errorCause:        cause,
		shouldRaise:       raise,
		errorClass:        class,
	}

	if !skipStash {
		t.lock.Lock()
		t.previousErrors = append(t.previousErrors, err)
		t.lock.Unlock()
	}

	return err
}

func (t *transactionAttempt) checkDone() error {
	t.lock.Lock()
	defer t.lock.Unlock()

	if t.state != AttemptStateNothingWritten && t.state != AttemptStatePending {
		return ErrOther
	}

	return nil
}

func (t *transactionAttempt) checkError() error {
	t.lock.Lock()
	defer t.lock.Unlock()

	if len(t.previousErrors) > 0 {
		return &TransactionOperationFailedError{
			errorCause:  ErrPreviousOperationFailed,
			shouldRaise: ErrorReasonTransactionFailed,
			errorClass:  ErrorClassFailOther,
		}
	}

	return nil
}

func hasExpired(expiryTime time.Time) bool {
	return time.Now().After(expiryTime)
}

func (t *transactionAttempt) checkExpired(stage string, id []byte, cb func(error)) {
	t.hooks.HasExpiredClientSideHook(stage, id, func(expired bool, err error) {
		if err != nil {
			cb(err)
			return
		}
		if expired {
			cb(ErrAttemptExpired)
			return
		}
		if hasExpired(t.expiryTime) {
			cb(ErrAttemptExpired)
			return
		}

		cb(nil)
	})
}

func (t *transactionAttempt) confirmATRPending(agent *gocbcore.Agent, firstKey []byte, cb func(error)) {
	handler := func(err error) {
		if err == nil {
			cb(nil)
			return
		}
		ec := t.classifyError(err)
		if t.expiryOvertimeMode {
			cb(t.createAndStashOperationFailedError(false, true, ErrAttemptExpired, ErrorReasonTransactionExpired, ErrorClassFailExpiry, false))
			return
		}

		var failErr error
		switch ec {
		case ErrorClassFailExpiry:
			t.expiryOvertimeMode = true
			failErr = t.createAndStashOperationFailedError(false, false, ErrAttemptExpired, ErrorReasonTransactionExpired, ec, false)
		case ErrorClassFailATRFull:
			failErr = t.createAndStashOperationFailedError(false, false, ErrAtrFull, ErrorReasonTransactionFailed, ec, false)
		case ErrorClassFailAmbiguous:
			time.AfterFunc(3*time.Millisecond, func() {
				t.confirmATRPending(agent, firstKey, cb)
			})
			return
		case ErrorClassFailHard:
			failErr = t.createAndStashOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec, false)
		case ErrorClassFailTransient:
			failErr = t.createAndStashOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec, false)
		case ErrorClassFailPathAlreadyExists:
			t.lock.Lock()
			t.state = AttemptStatePending
			t.lock.Unlock()
			cb(nil)
			return
		default:
			failErr = t.createAndStashOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec, false)
		}

		cb(failErr)
	}

	t.lock.Lock()
	if t.state != AttemptStateNothingWritten {
		t.lock.Unlock()

		t.txnAtrSection.Wait(func() {
			handler(nil)
		})

		return
	}

	atrID := int(cbcVbMap(firstKey, 1024))
	atrKey := []byte(atrIDList[atrID])

	t.atrAgent = agent
	t.atrScopeName = "_default"
	t.atrCollectionName = "_default"
	t.atrKey = atrKey

	t.txnAtrSection.Add(1)
	t.lock.Unlock()

	t.checkExpired(hookATRPending, []byte{}, func(err error) {
		if err != nil {
			t.lock.Lock()
			t.txnAtrSection.Done()
			t.lock.Unlock()
			handler(err)
			return
		}

		t.hooks.BeforeATRPending(func(err error) {
			if err != nil {
				t.lock.Lock()
				t.txnAtrSection.Done()
				t.lock.Unlock()
				handler(err)
				return
			}

			var duraTimeout time.Duration
			var deadline time.Time
			if t.keyValueTimeout > 0 {
				deadline = time.Now().Add(t.keyValueTimeout)
				duraTimeout = t.keyValueTimeout * 10 / 9
			}

			var marshalErr error
			atrFieldOp := func(fieldName string, data interface{}, flags memd.SubdocFlag) gocbcore.SubDocOp {
				b, err := json.Marshal(data)
				if err != nil {
					marshalErr = err
					return gocbcore.SubDocOp{}
				}

				return gocbcore.SubDocOp{
					Op:    memd.SubDocOpDictAdd,
					Flags: memd.SubdocFlagMkDirP | flags,
					Path:  "attempts." + t.id + "." + fieldName,
					Value: b,
				}
			}

			opts := gocbcore.MutateInOptions{
				ScopeName:      t.atrScopeName,
				CollectionName: t.atrCollectionName,
				Key:            atrKey,
				Ops: []gocbcore.SubDocOp{
					atrFieldOp("tst", "${Mutation.CAS}", memd.SubdocFlagXattrPath|memd.SubdocFlagExpandMacros),
					atrFieldOp("tid", t.transactionID, memd.SubdocFlagXattrPath),
					atrFieldOp("st", jsonAtrStatePending, memd.SubdocFlagXattrPath),
					atrFieldOp("exp", t.expiryTime.Sub(time.Now())/time.Millisecond, memd.SubdocFlagXattrPath),
				},
				DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
				DurabilityLevelTimeout: duraTimeout,
				Deadline:               deadline,
				Flags:                  memd.SubdocDocFlagMkDoc,
			}

			if marshalErr != nil {
				t.lock.Lock()
				t.txnAtrSection.Done()
				t.lock.Unlock()
				handler(err)
				return
			}

			_, err = agent.MutateIn(opts, func(result *gocbcore.MutateInResult, err error) {
				if err != nil {
					t.lock.Lock()
					t.txnAtrSection.Done()
					t.lock.Unlock()
					handler(err)
					return
				}

				for _, op := range result.Ops {
					if op.Err != nil {
						t.lock.Lock()
						t.txnAtrSection.Done()
						t.lock.Unlock()
						handler(op.Err)
						return
					}
				}

				t.hooks.AfterATRPending(func(err error) {
					if err != nil {
						t.lock.Lock()
						t.txnAtrSection.Done()
						t.lock.Unlock()
						handler(err)
						return
					}

					t.lock.Lock()
					t.state = AttemptStatePending
					t.txnAtrSection.Done()
					t.lock.Unlock()

					handler(nil)
				})
			})
			if err != nil {
				t.lock.Lock()
				t.txnAtrSection.Done()
				t.lock.Unlock()
				handler(err)
				return
			}
		})
	})
}

func (t *transactionAttempt) getStagedMutationLocked(bucketName, scopeName, collectionName string, key []byte) (int, *stagedMutation) {
	for i, mutation := range t.stagedMutations {
		if mutation.Agent.BucketName() == bucketName &&
			mutation.ScopeName == scopeName &&
			mutation.CollectionName == collectionName &&
			bytes.Compare(mutation.Key, key) == 0 {
			return i, mutation
		}
	}

	return 0, nil
}

func (t *transactionAttempt) getTxnState(opts GetOptions, deadline time.Time, xattr *jsonTxnXattr, cb func(*jsonAtrAttempt, error)) {
	_, err := opts.Agent.LookupIn(gocbcore.LookupInOptions{
		ScopeName:      opts.ScopeName,
		CollectionName: opts.CollectionName,
		Key:            []byte(xattr.ATR.DocID),
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpGet,
				Path:  "attempts." + xattr.ID.Attempt,
				Flags: memd.SubdocFlagXattrPath,
			},
		},
		Deadline: deadline,
	}, func(result *gocbcore.LookupInResult, err error) {
		if err != nil {
			if errors.Is(err, gocbcore.ErrDocumentNotFound) {
				cb(nil, ErrAtrNotFound)
				return
			}

			cb(nil, err)
			return
		}

		err = result.Ops[0].Err
		if err != nil {
			if errors.Is(err, gocbcore.ErrPathNotFound) {
				// TODO(brett19): Discuss with Graham if this is correct.
				cb(nil, ErrAtrEntryNotFound)
				return
			}

			cb(nil, err)
			return
		}

		var txnAttempt *jsonAtrAttempt
		if err := json.Unmarshal(result.Ops[0].Value, &txnAttempt); err != nil {
			cb(nil, err)
		}

		cb(txnAttempt, nil)
	})
	if err != nil {
		cb(nil, err)
		return
	}
}

func (t *transactionAttempt) classifyError(err error) ErrorClass {
	ec := ErrorClassFailOther
	if errors.Is(err, ErrDocAlreadyInTransaction) || errors.Is(err, ErrWriteWriteConflict) {
		ec = ErrorClassFailWriteWriteConflict
	} else if errors.Is(err, gocbcore.ErrDocumentNotFound) {
		ec = ErrorClassFailDocNotFound
	} else if errors.Is(err, gocbcore.ErrDocumentExists) {
		ec = ErrorClassFailDocAlreadyExists
	} else if errors.Is(err, gocbcore.ErrPathExists) {
		ec = ErrorClassFailPathAlreadyExists
	} else if errors.Is(err, gocbcore.ErrPathNotFound) {
		ec = ErrorClassFailPathNotFound
	} else if errors.Is(err, gocbcore.ErrCasMismatch) {
		ec = ErrorClassFailCasMismatch
	} else if errors.Is(err, gocbcore.ErrUnambiguousTimeout) || errors.Is(err, ErrTransient) {
		ec = ErrorClassFailTransient
	} else if errors.Is(err, gocbcore.ErrDurabilityAmbiguous) || errors.Is(err, gocbcore.ErrAmbiguousTimeout) ||
		errors.Is(err, ErrAmbiguous) || errors.Is(err, gocbcore.ErrRequestCanceled) {
		ec = ErrorClassFailAmbiguous
	} else if errors.Is(err, ErrHard) {
		ec = ErrorClassFailHard
	} else if errors.Is(err, ErrAttemptExpired) {
		ec = ErrorClassFailExpiry
	} else if errors.Is(err, gocbcore.ErrMemdTooBig) {
		ec = ErrorClassFailATRFull
	}

	return ec
}

func (t *transactionAttempt) writeWriteConflictPoll(res *GetResult, cb func(error)) {
	handler := func(err error) {
		if err != nil {
			ec := t.classifyError(err)

			switch ec {
			case ErrorClassFailExpiry:
				cb(t.createAndStashOperationFailedError(false, false, err,
					ErrorReasonTransactionExpired, ec, true))
				return
			default:
			}

			cb(t.createAndStashOperationFailedError(true, false, err,
				ErrorReasonTransactionFailed, ErrorClassFailWriteWriteConflict, true))
			return
		}

		cb(nil)
	}

	if res.Meta.TxnMeta == nil {
		// There is no write-write conflict.
		handler(nil)
		return
	}

	if res.Meta.TxnMeta.ID.Transaction == t.transactionID {
		// The transaction matches our transaction.  We can safely overwrite the existing
		// data in the txn meta and continue.
		handler(nil)
		return
	}

	deadline := time.Now().Add(1 * time.Second)

	var onePoll func()
	onePoll = func() {
		if !time.Now().Before(deadline) {
			// If the deadline expired, lets just immediately return.
			handler(ErrWriteWriteConflict)
			return
		}

		t.checkExpired("", res.key, func(err error) {
			if err != nil {
				t.expiryOvertimeMode = true
				handler(err)
				return
			}

			t.hooks.BeforeCheckATREntryForBlockingDoc([]byte(res.Meta.TxnMeta.ATR.DocID), func(err error) {
				if err != nil {
					handler(err)
					return
				}

				t.getTxnState(GetOptions{
					Agent:          res.agent,
					ScopeName:      res.Meta.TxnMeta.ATR.ScopeName,
					CollectionName: res.Meta.TxnMeta.ATR.CollectionName,
					Key:            []byte(res.Meta.TxnMeta.ATR.DocID),
				}, deadline, res.Meta.TxnMeta, func(attempt *jsonAtrAttempt, err error) {
					if err == ErrAtrEntryNotFound {
						// The ATR isn't there anymore, which counts as it being completed.
						handler(nil)
						return
					} else if err != nil {
						handler(err)
						return
					}

					td := time.Duration(attempt.ExpiryTime) * time.Millisecond
					if t.txnStartTime.Add(td).Before(time.Now()) {
						handler(nil)
						return
					}

					state := jsonAtrState(attempt.State)
					if state == jsonAtrStateCommitted || state == jsonAtrStateRolledBack {
						// If we have progressed enough to continue, let's do that.
						handler(nil)
						return
					}

					time.AfterFunc(200*time.Millisecond, onePoll)
				})
			})

		})
	}
	onePoll()
}

func (t *transactionAttempt) createCleanUpRequest() *CleanupRequest {
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

	return &CleanupRequest{
		AttemptID:         t.id,
		AtrID:             t.atrKey,
		AtrCollectionName: t.atrCollectionName,
		AtrScopeName:      t.atrScopeName,
		AtrBucketName:     bucketName,
		Inserts:           inserts,
		Replaces:          replaces,
		Removes:           removes,
		State:             t.state,
		readyTime:         t.expiryTime,
	}
}
