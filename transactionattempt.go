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

// AttemptState represents the current state of a transaction
type AttemptState int

const (
	// AttemptStateNothingWritten indicates that nothing has been written yet.
	AttemptStateNothingWritten = AttemptState(1)

	// AttemptStatePending indicates that the transaction ATR has been written and
	// the transaction is currently pending.
	AttemptStatePending = AttemptState(2)

	// AttemptStateCommitted indicates that the transaction is now logically committed
	// but the unstaging of documents is still underway.
	AttemptStateCommitted = AttemptState(3)

	// AttemptStateCompleted indicates that the transaction has been fully completed
	// and no longer has work to perform.
	AttemptStateCompleted = AttemptState(4)

	// AttemptStateAborted indicates that the transaction was aborted.
	AttemptStateAborted = AttemptState(5)

	// AttemptStateRolledBack indicates that the transaction was not committed and instead
	// was rolled back in its entirety.
	AttemptStateRolledBack = AttemptState(6)
)

// ErrorReason is the reason why a transaction should be failed.
// Internal: This should never be used and is not supported.
type ErrorReason uint8

const (
	// ErrorReasonTransactionFailed indicates the transaction should be failed because it failed.
	ErrorReasonTransactionFailed ErrorReason = iota

	// ErrorReasonTransactionExpired indicates the transaction should be failed because it expired.
	ErrorReasonTransactionExpired

	// ErrorReasonTransactionCommitAmbiguous indicates the transaction should be failed and the commit was ambiguous.
	ErrorReasonTransactionCommitAmbiguous

	// ErrorReasonTransactionFailedPostCommit indicates the transaction should be failed because it failed post commit.
	ErrorReasonTransactionFailedPostCommit
)

// ErrorClass describes the reason that a transaction error occurred.
// Internal: This should never be used and is not supported.
type ErrorClass uint8

const (
	// ErrorClassFailOther indicates an error occurred because it did not fit into any other reason.
	ErrorClassFailOther ErrorClass = iota

	// ErrorClassFailTransient indicates an error occurred because of a transient reason.
	ErrorClassFailTransient

	// ErrorClassFailDocNotFound indicates an error occurred because of a document not found.
	ErrorClassFailDocNotFound

	// ErrorClassFailDocAlreadyExists indicates an error occurred because a document already exists.
	ErrorClassFailDocAlreadyExists

	// ErrorClassFailPathNotFound indicates an error occurred because a path was not found.
	ErrorClassFailPathNotFound

	// ErrorClassFailPathAlreadyExists indicates an error occurred because a path already exists.
	ErrorClassFailPathAlreadyExists

	// ErrorClassFailWriteWriteConflict indicates an error occurred because of a write write conflict.
	ErrorClassFailWriteWriteConflict

	// ErrorClassFailCasMismatch indicates an error occurred because of a cas mismatch.
	ErrorClassFailCasMismatch

	// ErrorClassFailHard indicates an error occurred because of a hard error.
	ErrorClassFailHard

	// ErrorClassFailAmbiguous indicates an error occurred leaving the transaction in an ambiguous way.
	ErrorClassFailAmbiguous

	// ErrorClassFailExpiry indicates an error occurred because the transaction expired.
	ErrorClassFailExpiry

	// ErrorClassFailATRFull indicates an error occurred because the ATR is full.
	ErrorClassFailATRFull
)

var crc32cMacro = []byte("\"${Mutation.value_crc32c}\"")

type transactionAttempt struct {
	// immutable state
	expiryTime      time.Time
	keyValueTimeout time.Duration
	durabilityLevel DurabilityLevel
	transactionID   string
	id              string
	hooks           TransactionHooks

	// mutable state
	state               AttemptState
	stagedMutations     []*stagedMutation
	finalMutationTokens []MutationToken
	atrAgent            *gocbcore.Agent
	atrScopeName        string
	atrCollectionName   string
	atrKey              []byte
	expiryOvertimeMode  bool

	lock          sync.Mutex
	txnAtrSection atomicWaitQueue
	txnOpSection  atomicWaitQueue
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

func (t *transactionAttempt) createOperationFailedError(shouldRetry, shouldNotRollback bool, cause error,
	raise ErrorReason, class ErrorClass) error {
	return &TransactionOperationFailedError{
		shouldRetry:       shouldRetry,
		shouldNotRollback: shouldNotRollback,
		errorCause:        cause,
		shouldRaise:       raise,
		errorClass:        class,
	}
}

func (t *transactionAttempt) checkDone() error {
	t.lock.Lock()
	defer t.lock.Unlock()

	if t.state != AttemptStateNothingWritten && t.state != AttemptStatePending {
		return ErrOther
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

func (t *transactionAttempt) confirmATRPending(
	agent *gocbcore.Agent,
	scopeName string,
	collectionName string,
	firstKey []byte,
	cb func(error),
) error {
	handler := func(err error) {
		if err == nil {
			cb(nil)
			return
		}
		ec := t.classifyError(err)
		if t.expiryOvertimeMode {
			cb(t.createOperationFailedError(false, true, ErrAttemptExpired,
				ErrorReasonTransactionExpired, ErrorClassFailExpiry))
			return
		}

		var failErr error
		switch ec {
		case ErrorClassFailExpiry:
			t.expiryOvertimeMode = true
			failErr = t.createOperationFailedError(false, false, ErrAttemptExpired, ErrorReasonTransactionExpired, ec)
		case ErrorClassFailATRFull:
			failErr = t.createOperationFailedError(false, false, ErrAtrFull, ErrorReasonTransactionFailed, ec)
		case ErrorClassFailAmbiguous:
			time.AfterFunc(3*time.Millisecond, func() {
				t.confirmATRPending(agent, scopeName, collectionName, firstKey, cb)
			})
			return
		case ErrorClassFailHard:
			failErr = t.createOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec)
		case ErrorClassFailTransient:
			failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec)
		case ErrorClassFailPathAlreadyExists:
			t.lock.Lock()
			t.state = AttemptStatePending
			t.lock.Unlock()
			cb(nil)
			return
		default:
			failErr = t.createOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec)
		}

		cb(failErr)
	}

	t.lock.Lock()
	if t.state != AttemptStateNothingWritten {
		t.lock.Unlock()

		t.txnAtrSection.Wait(func() {
			handler(nil)
		})

		return nil
	}

	atrID := int(cbcVbMap(firstKey, 1024))
	atrKey := []byte(atrIDList[atrID])

	t.atrAgent = agent
	t.atrScopeName = scopeName
	t.atrCollectionName = collectionName
	t.atrKey = atrKey

	t.txnAtrSection.Add(1)
	t.lock.Unlock()

	atrFieldOp := func(fieldName string, data interface{}, flags memd.SubdocFlag) gocbcore.SubDocOp {
		bytes, _ := json.Marshal(data)

		return gocbcore.SubDocOp{
			Op:    memd.SubDocOpDictAdd,
			Flags: memd.SubdocFlagMkDirP | flags,
			Path:  "attempts." + t.id + "." + fieldName,
			Value: bytes,
		}
	}

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

			_, err = agent.MutateIn(gocbcore.MutateInOptions{
				ScopeName:      scopeName,
				CollectionName: collectionName,
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
			}, func(result *gocbcore.MutateInResult, err error) {
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

	return nil
}

func (t *transactionAttempt) setATRCommittedAmbiguityResolution(cb func(error)) {
	handler := func(st jsonAtrState, err error) {
		if err != nil {
			var failErr error
			ec := t.classifyError(err)
			switch ec {
			case ErrorClassFailExpiry:
				t.expiryOvertimeMode = true
				failErr = t.createOperationFailedError(false, true, ErrAttemptExpired, ErrorReasonTransactionCommitAmbiguous, ec)
			case ErrorClassFailHard:
				failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec)
			case ErrorClassFailTransient:
				time.AfterFunc(3*time.Millisecond, func() {
					t.setATRCommittedAmbiguityResolution(cb)
				})
				return
			case ErrorClassFailOther:
				time.AfterFunc(3*time.Millisecond, func() {
					t.setATRCommittedAmbiguityResolution(cb)
				})
				return
			default:
				failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec)
			}

			t.lock.Lock()
			t.txnAtrSection.Done()
			t.lock.Unlock()

			cb(failErr)
			return
		}

		switch st {
		case jsonAtrStateCommitted:
			t.lock.Lock()
			t.state = AttemptStateCommitted
			t.txnAtrSection.Done()
			t.lock.Unlock()
			cb(nil)
		case jsonAtrStatePending:
			t.setATRCommitted(cb)
		case jsonAtrStateAborted:
			t.lock.Lock()
			t.txnAtrSection.Done()
			t.lock.Unlock()
			cb(t.createOperationFailedError(false, true, nil, ErrorReasonTransactionFailed, ErrorClassFailOther))
		case jsonAtrStateRolledBack:
			t.lock.Lock()
			t.txnAtrSection.Done()
			t.lock.Unlock()
			cb(t.createOperationFailedError(false, true, nil, ErrorReasonTransactionFailed, ErrorClassFailOther))
		default:
			t.lock.Lock()
			t.txnAtrSection.Done()
			t.lock.Unlock()
			cb(t.createOperationFailedError(false, true, ErrIllegalState, ErrorReasonTransactionFailed, ErrorClassFailOther))
		}
	}

	t.checkExpired(hookATRCommitAmbiguityResolution, []byte{}, func(err error) {
		if err != nil {
			handler("", err)
			return
		}

		t.hooks.BeforeATRCommitAmbiguityResolution(func(err error) {
			if err != nil {
				handler("", err)
				return
			}

			var deadline time.Time
			if t.keyValueTimeout > 0 {
				deadline = time.Now().Add(t.keyValueTimeout)
			}

			_, err = t.atrAgent.LookupIn(gocbcore.LookupInOptions{
				ScopeName:      t.atrScopeName,
				CollectionName: t.atrCollectionName,
				Key:            t.atrKey,
				Ops: []gocbcore.SubDocOp{
					{
						Op:    memd.SubDocOpGet,
						Path:  "attempts." + t.id + ".st",
						Flags: memd.SubdocFlagXattrPath,
					},
				},
				Deadline: deadline,
				Flags:    memd.SubdocDocFlagNone,
			}, func(result *gocbcore.LookupInResult, err error) {
				if err != nil {
					handler("", err)
					return
				}

				if result.Ops[0].Err != nil {
					handler("", result.Ops[0].Err)
					return
				}

				var st jsonAtrState
				// TODO(brett19): Don't ignore the error here
				json.Unmarshal(result.Ops[0].Value, &st)

				handler(st, nil)
			})

		})
	})
}

func (t *transactionAttempt) setATRCommitted(
	cb func(error),
) error {
	handler := func(err error) {
		if err == nil {
			cb(nil)
			return
		}

		var failErr error
		ec := t.classifyError(err)
		switch ec {
		case ErrorClassFailExpiry:
			failErr = t.createOperationFailedError(false, false, ErrAttemptExpired, ErrorReasonTransactionExpired, ec)
		case ErrorClassFailAmbiguous:
			t.setATRCommittedAmbiguityResolution(cb)
			return
		case ErrorClassFailHard:
			failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec)
		case ErrorClassFailTransient:
			failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec)
		default:
			failErr = t.createOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec)
		}

		t.lock.Lock()
		t.txnAtrSection.Done()
		t.lock.Unlock()

		cb(failErr)
	}

	t.checkExpired(hookATRCommit, []byte{}, func(err error) {
		if err != nil {
			handler(err)
			return
		}

		t.lock.Lock()
		if t.state != AttemptStatePending {
			t.lock.Unlock()

			t.txnAtrSection.Wait(func() {
				cb(t.createOperationFailedError(false, true, nil, ErrorReasonTransactionFailed, ErrorClassFailOther))
			})

			return
		}

		atrAgent := t.atrAgent
		atrScopeName := t.atrScopeName
		atrKey := t.atrKey
		atrCollectionName := t.atrCollectionName

		insMutations := []jsonAtrMutation{}
		repMutations := []jsonAtrMutation{}
		remMutations := []jsonAtrMutation{}

		for _, mutation := range t.stagedMutations {
			jsonMutation := jsonAtrMutation{
				BucketName:     mutation.Agent.BucketName(),
				ScopeName:      mutation.ScopeName,
				CollectionName: mutation.CollectionName,
				DocID:          string(mutation.Key),
			}

			if mutation.OpType == StagedMutationInsert {
				insMutations = append(insMutations, jsonMutation)
			} else if mutation.OpType == StagedMutationReplace {
				repMutations = append(repMutations, jsonMutation)
			} else if mutation.OpType == StagedMutationRemove {
				remMutations = append(remMutations, jsonMutation)
			} else {
				// TODO(brett19): Signal an error here
			}
		}

		t.txnAtrSection.Add(1)
		t.lock.Unlock()

		t.hooks.BeforeATRCommit(func(err error) {
			if err != nil {
				handler(err)
				return
			}

			atrFieldOp := func(fieldName string, data interface{}, flags memd.SubdocFlag) gocbcore.SubDocOp {
				bytes, _ := json.Marshal(data)

				return gocbcore.SubDocOp{
					Op:    memd.SubDocOpDictSet,
					Flags: memd.SubdocFlagMkDirP | flags,
					Path:  "attempts." + t.id + "." + fieldName,
					Value: bytes,
				}
			}

			var duraTimeout time.Duration
			var deadline time.Time
			if t.keyValueTimeout > 0 {
				deadline = time.Now().Add(t.keyValueTimeout)
				duraTimeout = t.keyValueTimeout * 10 / 9
			}

			_, err = atrAgent.MutateIn(gocbcore.MutateInOptions{
				ScopeName:      atrScopeName,
				CollectionName: atrCollectionName,
				Key:            atrKey,
				Ops: []gocbcore.SubDocOp{
					atrFieldOp("st", jsonAtrStateCommitted, memd.SubdocFlagXattrPath),
					atrFieldOp("tsc", "${Mutation.CAS}", memd.SubdocFlagXattrPath|memd.SubdocFlagExpandMacros),
					atrFieldOp("p", 0, memd.SubdocFlagXattrPath),
					atrFieldOp("ins", insMutations, memd.SubdocFlagXattrPath),
					atrFieldOp("rep", repMutations, memd.SubdocFlagXattrPath),
					atrFieldOp("rem", remMutations, memd.SubdocFlagXattrPath),
				},
				DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
				DurabilityLevelTimeout: duraTimeout,
				Flags:                  memd.SubdocDocFlagNone,
				Deadline:               deadline,
			}, func(result *gocbcore.MutateInResult, err error) {
				if err != nil {
					handler(err)
					return
				}

				t.hooks.AfterATRCommit(func(err error) {
					if err != nil {
						handler(err)
						return
					}

					t.lock.Lock()
					t.state = AttemptStateCommitted
					t.txnAtrSection.Done()
					t.lock.Unlock()

					handler(nil)
				})
			})
			if err != nil {
				handler(err)
			}
		})
	})
	return nil
}

func (t *transactionAttempt) setATRCompleted(
	cb func(error),
) error {
	handler := func(err error) {
		if err == nil {
			cb(nil)
			return
		}

		ec := t.classifyError(err)
		switch ec {
		case ErrorClassFailHard:
			cb(t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec))
			return
		}

		cb(nil)
	}

	t.checkExpired(hookATRComplete, []byte{}, func(err error) {
		if err != nil {
			handler(err)
			return
		}

		t.hooks.BeforeATRComplete(func(err error) {
			if err != nil {
				handler(err)
				return
			}

			t.lock.Lock()
			if t.state != AttemptStateCommitted {
				t.lock.Unlock()

				t.txnAtrSection.Wait(func() {
					handler(nil)
				})

				return
			}

			atrAgent := t.atrAgent
			atrScopeName := t.atrScopeName
			atrKey := t.atrKey
			atrCollectionName := t.atrCollectionName

			t.txnAtrSection.Add(1)
			t.lock.Unlock()

			atrFieldOp := func(fieldName string, data interface{}, flags memd.SubdocFlag) gocbcore.SubDocOp {
				bytes, _ := json.Marshal(data)

				return gocbcore.SubDocOp{
					Op:    memd.SubDocOpDictSet,
					Flags: memd.SubdocFlagMkDirP | flags,
					Path:  "attempts." + t.id + "." + fieldName,
					Value: bytes,
				}
			}

			var duraTimeout time.Duration
			var deadline time.Time
			if t.keyValueTimeout > 0 {
				deadline = time.Now().Add(t.keyValueTimeout)
				duraTimeout = t.keyValueTimeout * 10 / 9
			}

			_, err = atrAgent.MutateIn(gocbcore.MutateInOptions{
				ScopeName:      atrScopeName,
				CollectionName: atrCollectionName,
				Key:            atrKey,
				Ops: []gocbcore.SubDocOp{
					atrFieldOp("st", jsonAtrStateCompleted, memd.SubdocFlagXattrPath),
					atrFieldOp("tsco", "${Mutation.CAS}", memd.SubdocFlagXattrPath|memd.SubdocFlagExpandMacros),
				},
				DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
				DurabilityLevelTimeout: duraTimeout,
				Deadline:               deadline,
				Flags:                  memd.SubdocDocFlagNone,
			}, func(result *gocbcore.MutateInResult, err error) {
				if err != nil {
					t.lock.Lock()
					t.txnAtrSection.Done()
					t.lock.Unlock()

					handler(err)
					return
				}

				t.hooks.AfterATRComplete(func(err error) {
					if err != nil {
						t.lock.Lock()
						t.txnAtrSection.Done()
						t.lock.Unlock()

						handler(err)
						return
					}

					t.lock.Lock()
					t.state = AttemptStateCompleted
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

	return nil
}

func (t *transactionAttempt) getStagedMutationLocked(bucketName, scopeName, collectionName string, key []byte) (int, *stagedMutation) {
	for i, mutation := range t.stagedMutations {
		// TODO(brett19): Need to check the bucket names here
		if mutation.Agent.BucketName() == bucketName &&
			mutation.ScopeName == scopeName &&
			mutation.CollectionName == collectionName &&
			bytes.Compare(mutation.Key, key) == 0 {
			return i, mutation
		}
	}

	return 0, nil
}

func (t *transactionAttempt) getFullDoc(opts GetOptions, deadline time.Time,
	cb func(*getDoc, error)) {
	t.hooks.BeforeDocGet(opts.Key, func(err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		_, err = opts.Agent.LookupIn(gocbcore.LookupInOptions{
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Ops: []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpGet,
					Path:  "$document",
					Flags: memd.SubdocFlagXattrPath,
				},
				{
					Op:    memd.SubDocOpGet,
					Path:  "txn",
					Flags: memd.SubdocFlagXattrPath,
				},
				{
					Op:    memd.SubDocOpGetDoc,
					Path:  "",
					Flags: 0,
				},
			},
			Deadline: deadline,
			Flags:    memd.SubdocDocFlagAccessDeleted,
		}, func(result *gocbcore.LookupInResult, err error) {
			if errors.Is(err, gocbcore.ErrDocumentNotFound) {
				cb(nil, err)
				return
			}

			var meta *docMeta
			// TODO(brett19): Don't ignore the error here
			json.Unmarshal(result.Ops[0].Value, &meta)

			var txnMeta *jsonTxnXattr
			if result.Ops[1].Err == nil {
				var txnMetaVal jsonTxnXattr
				// TODO(brett19): Don't ignore the error here
				json.Unmarshal(result.Ops[1].Value, &txnMetaVal)
				txnMeta = &txnMetaVal

				cb(&getDoc{
					Body:    result.Ops[2].Value,
					TxnMeta: txnMeta,
					DocMeta: meta,
					Cas:     result.Cas,
				}, nil)

				return
			}

			if result.Internal.IsDeleted {
				cb(nil, gocbcore.ErrDocumentNotFound)
				return
			}

			cb(&getDoc{
				Body:    result.Ops[2].Value,
				DocMeta: meta,
				Cas:     result.Cas,
			}, nil)
		})
		if err != nil {
			cb(nil, err)
		}
	})
}

func (t *transactionAttempt) getTxnState(opts GetOptions, deadline time.Time, xattr *jsonTxnXattr, cb func(jsonAtrState, error)) {
	_, err := opts.Agent.LookupIn(gocbcore.LookupInOptions{
		ScopeName:      opts.ScopeName,
		CollectionName: opts.CollectionName,
		Key:            []byte(xattr.ATR.DocID),
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpGet,
				Path:  "attempts." + xattr.ID.Attempt + ".st",
				Flags: memd.SubdocFlagXattrPath,
			},
		},
		Deadline: deadline,
	}, func(result *gocbcore.LookupInResult, err error) {
		if err != nil {
			if errors.Is(err, gocbcore.ErrDocumentNotFound) {
				cb("", ErrAtrNotFound)
				return
			}

			cb("", err)
			return
		}

		err = result.Ops[0].Err
		if err != nil {
			if errors.Is(err, gocbcore.ErrPathNotFound) {
				// TODO(brett19): Discuss with Graham if this is correct.
				cb("", ErrAtrEntryNotFound)
				return
			}

			cb("", err)
			return
		}

		// TODO(brett19): Don't ignore the error here.
		var txnState jsonAtrState
		json.Unmarshal(result.Ops[0].Value, &txnState)

		t.hooks.AfterGetComplete(opts.Key, func(err error) {
			if err != nil {
				cb("", err)
				return
			}
			cb(txnState, nil)
		})
	})
	if err != nil {
		cb("", err)
		return
	}
}

func (t *transactionAttempt) Get(opts GetOptions, cb GetCallback) error {
	if err := t.checkDone(); err != nil {
		ec := t.classifyError(err)
		return t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec)
	}

	t.checkExpired(hookGet, opts.Key, func(err error) {
		if err != nil {
			t.expiryOvertimeMode = true
			cb(nil, t.createOperationFailedError(false, false, ErrAttemptExpired, ErrorReasonTransactionExpired, ErrorClassFailExpiry))
			return
		}

		t.lock.Lock()

		_, existingMutation := t.getStagedMutationLocked(opts.Agent.BucketName(), opts.ScopeName, opts.CollectionName, opts.Key)
		if existingMutation != nil {
			if existingMutation.OpType == StagedMutationInsert || existingMutation.OpType == StagedMutationReplace {
				getRes := &GetResult{
					agent:          existingMutation.Agent,
					scopeName:      existingMutation.ScopeName,
					collectionName: existingMutation.CollectionName,
					key:            existingMutation.Key,
					Value:          existingMutation.Staged,
					Cas:            existingMutation.Cas,
					Meta: MutableItemMeta{
						Deleted: existingMutation.IsTombstone,
					},
				}

				t.lock.Unlock()
				cb(getRes, nil)
				return
			} else if existingMutation.OpType == StagedMutationRemove {
				t.lock.Unlock()

				cb(nil, t.createOperationFailedError(false, false, gocbcore.ErrDocumentNotFound,
					ErrorReasonTransactionFailed, ErrorClassFailDocNotFound))
				return
			}
		}

		t.lock.Unlock()

		var deadline time.Time
		if t.keyValueTimeout > 0 {
			deadline = time.Now().Add(t.keyValueTimeout)
		}

		t.getFullDoc(opts, deadline, func(doc *getDoc, err error) {
			if err != nil {
				var failErr error
				ec := t.classifyError(err)
				switch ec {
				case ErrorClassFailDocNotFound:
					failErr = t.createOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec)
				case ErrorClassFailHard:
					failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec)
				case ErrorClassFailTransient:
					failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec)
				default:
					failErr = t.createOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec)
				}

				cb(nil, failErr)
				return
			}

			if doc.TxnMeta != nil {
				if doc.TxnMeta.ID.Attempt == t.id {
					cb(&GetResult{
						agent:          opts.Agent,
						scopeName:      opts.ScopeName,
						collectionName: opts.CollectionName,
						key:            opts.Key,
						Value:          doc.TxnMeta.Operation.Staged,
						Cas:            doc.Cas,
						Meta: MutableItemMeta{
							RevID:   doc.DocMeta.RevID,
							Expiry:  doc.DocMeta.Expiration,
							Deleted: false,
							TxnMeta: doc.TxnMeta,
						},
					}, nil)
					return
				}

				t.getTxnState(opts, deadline, doc.TxnMeta, func(state jsonAtrState, err error) {
					if err != nil {
						ec := t.classifyError(err)
						if errors.Is(err, ErrAtrNotFound) {
							cb(nil, t.createOperationFailedError(false, false, err,
								ErrorReasonTransactionFailed, ec))
							return
						} else if errors.Is(err, ErrAtrEntryNotFound) {
							// TODO
						}
						var failErr error
						switch ec {
						case ErrorClassFailHard:
							failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec)
						case ErrorClassFailTransient:
							failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec)
						default:
							failErr = t.createOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec)
						}
						cb(nil, failErr)
						return
					}

					if state == jsonAtrStateCommitted || state == jsonAtrStateCompleted {
						if doc.TxnMeta.Operation.Type == jsonMutationRemove {

							cb(nil, t.createOperationFailedError(false, false,
								gocbcore.ErrDocumentNotFound, ErrorReasonTransactionFailed, ErrorClassFailDocNotFound))
							return
						}

						// TODO(brett19): Discuss virtual CAS with Graham
						cb(&GetResult{
							agent:          opts.Agent,
							scopeName:      opts.ScopeName,
							collectionName: opts.CollectionName,
							key:            opts.Key,
							Value:          doc.TxnMeta.Operation.Staged,
							Cas:            doc.Cas,
							Meta: MutableItemMeta{
								RevID:   doc.DocMeta.RevID,
								Expiry:  doc.DocMeta.Expiration,
								Deleted: false,
								TxnMeta: doc.TxnMeta,
							},
						}, nil)
						return
					}

					if doc.TxnMeta.Operation.Type == jsonMutationInsert {
						cb(nil, t.createOperationFailedError(false, false, gocbcore.ErrDocumentNotFound,
							ErrorReasonTransactionFailed, ErrorClassFailDocNotFound))
						return
					}

					cb(&GetResult{
						agent:          opts.Agent,
						scopeName:      opts.ScopeName,
						collectionName: opts.CollectionName,
						key:            opts.Key,
						Value:          doc.Body,
						Cas:            doc.Cas,
						Meta: MutableItemMeta{
							RevID:   doc.DocMeta.RevID,
							Expiry:  doc.DocMeta.Expiration,
							Deleted: false,
							TxnMeta: nil,
						},
					}, nil)
				})
				return
			}

			cb(&GetResult{
				agent:          opts.Agent,
				scopeName:      opts.ScopeName,
				collectionName: opts.CollectionName,
				key:            opts.Key,
				Value:          doc.Body,
				Cas:            doc.Cas,
				Meta: MutableItemMeta{
					RevID:   doc.DocMeta.RevID,
					Expiry:  doc.DocMeta.Expiration,
					Deleted: false,
					TxnMeta: nil,
				},
			}, nil)
		})
	})

	return nil
}

func (t *transactionAttempt) classifyError(err error) ErrorClass {
	ec := ErrorClassFailOther
	if errors.Is(err, ErrDocAlreadyInTransaction) {
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

func (t *transactionAttempt) getForInsert(opts InsertOptions, cb func(gocbcore.Cas, error)) {
	t.hooks.BeforeGetDocInExistsDuringStagedInsert(opts.Key, func(err error) {
		if err != nil {
			cb(0, err)
			return
		}

		var deadline time.Time
		if t.keyValueTimeout > 0 {
			deadline = time.Now().Add(t.keyValueTimeout)
		}

		_, err = opts.Agent.LookupIn(gocbcore.LookupInOptions{
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Ops: []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpGet,
					Path:  "$document",
					Flags: memd.SubdocFlagXattrPath,
				},
				{
					Op:    memd.SubDocOpGet,
					Path:  "txn",
					Flags: memd.SubdocFlagXattrPath,
				},
				{
					Op:    memd.SubDocOpGetDoc,
					Path:  "",
					Flags: 0,
				},
			},
			Deadline: deadline,
			Flags:    memd.SubdocDocFlagAccessDeleted,
		}, func(result *gocbcore.LookupInResult, err error) {
			if err != nil {
				cb(0, err)
				return
			}

			var txnMeta *jsonTxnXattr
			if result.Ops[1].Err == nil {
				var txnMetaVal jsonTxnXattr
				// TODO(brett19): Don't ignore the error here
				json.Unmarshal(result.Ops[1].Value, &txnMetaVal)
				txnMeta = &txnMetaVal
			}

			if txnMeta == nil {
				// This doc isn't in a transaction
				if result.Internal.IsDeleted {
					cb(result.Cas, nil)
					return
				}

				cb(0, gocbcore.ErrDocumentExists)
				return
			}

			// TODO: checkwritewrite here
			cb(result.Cas, nil)
			return
		})
		if err != nil {
			cb(0, err)
			return
		}
	})
}
func (t *transactionAttempt) Insert(opts InsertOptions, cb StoreCallback) error {
	return t.insert(opts, 0, cb)
}

func (t *transactionAttempt) insert(opts InsertOptions, cas gocbcore.Cas, cb StoreCallback) error {
	handler := func(result *GetResult, err error) {
		if err != nil {
			var failErr error
			ec := t.classifyError(err)
			if errors.Is(err, gocbcore.ErrFeatureNotAvailable) {
				cb(nil, t.createOperationFailedError(false, false, err,
					ErrorReasonTransactionFailed, ec))
				return
			}
			if t.expiryOvertimeMode {
				cb(nil, t.createOperationFailedError(false, true, ErrAttemptExpired,
					ErrorReasonTransactionExpired, ErrorClassFailExpiry))
				return
			}

			switch ec {
			case ErrorClassFailExpiry:
				t.expiryOvertimeMode = true
				failErr = t.createOperationFailedError(false, false, ErrAttemptExpired,
					ErrorReasonTransactionExpired, ec)
			case ErrorClassFailAmbiguous:
				time.AfterFunc(3*time.Millisecond, func() {
					err := t.insert(opts, 0, cb)
					if err != nil {
						cb(nil, err)
					}
				})
				return
			case ErrorClassFailTransient:
				failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec)
			case ErrorClassFailHard:
				failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec)
			case ErrorClassFailDocAlreadyExists:
				fallthrough
			case ErrorClassFailCasMismatch:
				t.getForInsert(opts, func(result gocbcore.Cas, err error) {
					if err != nil {
						var failErr error
						ec := t.classifyError(err)
						switch ec {
						case ErrorClassFailDocNotFound:
							failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec)
						case ErrorClassFailPathNotFound:
							failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec)
						case ErrorClassFailTransient:
							failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec)
						default:
							failErr = t.createOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec)
						}

						cb(nil, failErr)
						return
					}

					err = t.insert(opts, result, cb)
					if err != nil {
						cb(nil, err)
					}
				})

				return
			default:
				failErr = t.createOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec)
			}

			cb(nil, failErr)
			return
		}

		cb(result, nil)
	}

	if err := t.checkDone(); err != nil {
		ec := t.classifyError(err)
		return t.createOperationFailedError(false, false, ErrAttemptExpired, ErrorReasonTransactionExpired, ec)
	}

	t.checkExpired(hookInsert, opts.Key, func(err error) {
		if err != nil {
			handler(nil, ErrAttemptExpired)
			return
		}

		err = t.confirmATRPending(opts.Agent, opts.ScopeName, opts.CollectionName, opts.Key, func(err error) {
			if err != nil {
				// We've already classified the error so just hit the callback.
				cb(nil, err)
				return
			}

			t.hooks.BeforeStagedInsert(opts.Key, func(err error) {
				if err != nil {
					handler(nil, err)
					return
				}

				stagedInfo := &stagedMutation{
					OpType:         StagedMutationInsert,
					Agent:          opts.Agent,
					ScopeName:      opts.ScopeName,
					CollectionName: opts.CollectionName,
					Key:            opts.Key,
					Staged:         opts.Value,
					IsTombstone:    true,
				}

				var txnMeta jsonTxnXattr
				txnMeta.ID.Transaction = t.transactionID
				txnMeta.ID.Attempt = t.id
				txnMeta.ATR.CollectionName = t.atrCollectionName
				txnMeta.ATR.ScopeName = t.atrScopeName
				txnMeta.ATR.BucketName = t.atrAgent.BucketName()
				txnMeta.ATR.DocID = string(t.atrKey)
				txnMeta.Operation.Type = jsonMutationInsert
				txnMeta.Operation.Staged = stagedInfo.Staged

				txnMetaBytes, _ := json.Marshal(txnMeta)
				// TODO(brett19): Don't ignore the error here.

				var duraTimeout time.Duration
				var deadline time.Time
				if t.keyValueTimeout > 0 {
					deadline = time.Now().Add(t.keyValueTimeout)
					duraTimeout = t.keyValueTimeout * 10 / 9
				}

				flags := memd.SubdocDocFlagCreateAsDeleted | memd.SubdocDocFlagAccessDeleted
				var txnOp memd.SubDocOpType
				if cas == 0 {
					flags |= memd.SubdocDocFlagAddDoc
					txnOp = memd.SubDocOpDictAdd
				} else {
					txnOp = memd.SubDocOpDictSet
				}

				_, err = stagedInfo.Agent.MutateIn(gocbcore.MutateInOptions{
					ScopeName:      stagedInfo.ScopeName,
					CollectionName: stagedInfo.CollectionName,
					Key:            stagedInfo.Key,
					Cas:            cas,
					Ops: []gocbcore.SubDocOp{
						{
							Op:    txnOp,
							Path:  "txn",
							Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath,
							Value: txnMetaBytes,
						},
						{
							Op:    memd.SubDocOpDictAdd,
							Path:  "txn.op.crc32",
							Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
							Value: crc32cMacro,
						},
					},
					DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
					DurabilityLevelTimeout: duraTimeout,
					Deadline:               deadline,
					Flags:                  flags,
				}, func(result *gocbcore.MutateInResult, err error) {
					if err != nil {
						handler(nil, err)
						return
					}

					t.hooks.AfterStagedInsertComplete(opts.Key, func(err error) {
						if err != nil {
							handler(nil, err)
							return
						}

						t.lock.Lock()
						stagedInfo.Cas = result.Cas
						t.stagedMutations = append(t.stagedMutations, stagedInfo)
						t.lock.Unlock()

						handler(&GetResult{
							agent:          stagedInfo.Agent,
							scopeName:      stagedInfo.ScopeName,
							collectionName: stagedInfo.CollectionName,
							key:            stagedInfo.Key,
							Value:          stagedInfo.Staged,
							Cas:            result.Cas,
						}, err)
					})
				})
				if err != nil {
					handler(nil, err)
				}
			})
		})
		if err != nil {
			handler(nil, err)
			return
		}
	})

	return nil
}

func (t *transactionAttempt) Replace(opts ReplaceOptions, cb StoreCallback) error {
	if err := t.checkDone(); err != nil {
		ec := t.classifyError(err)
		return t.createOperationFailedError(false, false, ErrAttemptExpired, ErrorReasonTransactionExpired, ec)
	}

	t.checkExpired(hookReplace, opts.Document.key, func(err error) {
		if err != nil {
			t.expiryOvertimeMode = true
			cb(nil, t.createOperationFailedError(false, false, ErrAttemptExpired,
				ErrorReasonTransactionExpired, ErrorClassFailExpiry))
			return
		}

		agent := opts.Document.agent
		scopeName := opts.Document.scopeName
		collectionName := opts.Document.collectionName
		key := opts.Document.key

		err = t.confirmATRPending(agent, scopeName, collectionName, key, func(err error) {
			if err != nil {
				cb(nil, err)
				return
			}

			t.doReplace(opts, func(stagedInfo *stagedMutation, err error) {
				if err != nil {
					var failErr error
					ec := t.classifyError(err)
					switch ec {
					case ErrorClassFailExpiry:
						t.expiryOvertimeMode = true
						failErr = t.createOperationFailedError(false, false, ErrAttemptExpired, ErrorReasonTransactionExpired, ec)
					case ErrorClassFailDocNotFound:
						failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec)
					case ErrorClassFailDocAlreadyExists:
						failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ErrorClassFailCasMismatch)
					case ErrorClassFailCasMismatch:
						failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec)
					case ErrorClassFailTransient:
						failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec)
					case ErrorClassFailAmbiguous:
						failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec)
					case ErrorClassFailHard:
						failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec)
					default:
						failErr = t.createOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec)
					}

					cb(nil, failErr)
					return
				}
				t.lock.Lock()

				idx, existingMutation := t.getStagedMutationLocked(agent.BucketName(), opts.Document.scopeName, opts.Document.collectionName,
					opts.Document.key)
				if existingMutation == nil {
					t.stagedMutations = append(t.stagedMutations, stagedInfo)
				} else {
					if existingMutation.OpType == StagedMutationReplace {
						t.stagedMutations[idx] = stagedInfo
					} else if existingMutation.OpType == StagedMutationInsert {
						stagedInfo.OpType = StagedMutationInsert
						t.stagedMutations = append(t.stagedMutations[:idx+copy(t.stagedMutations[idx:], t.stagedMutations[idx+1:])], stagedInfo)
					}

				}
				t.lock.Unlock()

				cb(&GetResult{
					agent:          stagedInfo.Agent,
					scopeName:      stagedInfo.ScopeName,
					collectionName: stagedInfo.CollectionName,
					key:            stagedInfo.Key,
					Value:          stagedInfo.Staged,
					Cas:            stagedInfo.Cas,
					Meta: MutableItemMeta{
						Deleted: stagedInfo.IsTombstone,
					},
				}, nil)
			})
		})
		if err != nil {
			cb(nil, err)
			return
		}
	})

	return nil
}

func (t *transactionAttempt) writeWriteConflictPoll(opts GetOptions, xattr *jsonTxnXattr, cb func(error)) {
	deadline := time.Now().Add(1 * time.Second)

	var onePoll func()
	onePoll = func() {
		if !time.Now().Before(deadline) {
			// If the deadline expired, lets just immediately return.
			cb(nil)
			return
		}

		t.getTxnState(opts, deadline, xattr, func(state jsonAtrState, err error) {
			if err != nil {
				cb(err)
				return
			}

			if state == jsonAtrStateCommitted || state == jsonAtrStateCompleted ||
				state == jsonAtrStateAborted || state == jsonAtrStateRolledBack {
				// If we have progressed enough to continue, let's do that.
				cb(nil)
				return
			}

			time.AfterFunc(100*time.Millisecond, onePoll)
		})
	}
	onePoll()
}

func (t *transactionAttempt) doReplace(opts ReplaceOptions, cb func(*stagedMutation, error)) {
	agent := opts.Document.agent
	scopeName := opts.Document.scopeName
	collectionName := opts.Document.collectionName
	key := opts.Document.key
	deleted := opts.Document.Meta.Deleted

	t.hooks.BeforeStagedReplace(key, func(err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		stagedInfo := &stagedMutation{
			OpType:         StagedMutationReplace,
			Agent:          agent,
			ScopeName:      scopeName,
			CollectionName: collectionName,
			Key:            key,
			Staged:         opts.Value,
			IsTombstone:    deleted,
		}

		var txnMeta jsonTxnXattr
		txnMeta.ID.Transaction = t.transactionID
		txnMeta.ID.Attempt = t.id
		txnMeta.ATR.CollectionName = t.atrCollectionName
		txnMeta.ATR.ScopeName = t.atrScopeName
		txnMeta.ATR.BucketName = t.atrAgent.BucketName()
		txnMeta.ATR.DocID = string(t.atrKey)
		txnMeta.Operation.Type = jsonMutationReplace
		txnMeta.Operation.Staged = stagedInfo.Staged
		restore := struct {
			OriginalCAS string
			ExpiryTime  uint
			RevID       string
		}{
			OriginalCAS: fmt.Sprintf("%d", opts.Document.Cas),
			ExpiryTime:  opts.Document.Meta.Expiry,
			RevID:       opts.Document.Meta.RevID,
		}
		txnMeta.Restore = (*struct {
			OriginalCAS string `json:"CAS,omitempty"`
			ExpiryTime  uint   `json:"exptime"`
			RevID       string `json:"revid,omitempty"`
		})(&restore)

		txnMetaBytes, _ := json.Marshal(txnMeta)
		// TODO(brett19): Don't ignore the error here.

		var duraTimeout time.Duration
		var deadline time.Time
		if t.keyValueTimeout > 0 {
			deadline = time.Now().Add(t.keyValueTimeout)
			duraTimeout = t.keyValueTimeout * 10 / 9
		}

		flags := memd.SubdocDocFlagNone
		if deleted {
			flags = memd.SubdocDocFlagAccessDeleted
		}

		_, err = stagedInfo.Agent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      stagedInfo.ScopeName,
			CollectionName: stagedInfo.CollectionName,
			Key:            stagedInfo.Key,
			Cas:            opts.Document.Cas,
			Ops: []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpDictSet,
					Path:  "txn",
					Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath,
					Value: txnMetaBytes,
				},
				{
					Op:    memd.SubDocOpDictSet,
					Path:  "txn.op.crc32",
					Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
					Value: crc32cMacro,
				},
			},
			Flags:                  flags,
			DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
			DurabilityLevelTimeout: duraTimeout,
			Deadline:               deadline,
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				var sdErr gocbcore.SubDocumentError
				if errors.As(err, &sdErr) {
					if errors.Is(sdErr.InnerError, gocbcore.ErrPathExists) {
						_, err = stagedInfo.Agent.LookupIn(gocbcore.LookupInOptions{
							ScopeName:      stagedInfo.ScopeName,
							CollectionName: stagedInfo.CollectionName,
							Key:            stagedInfo.Key,
							Ops: []gocbcore.SubDocOp{
								{
									Op:    memd.SubDocOpGet,
									Path:  "txn",
									Flags: memd.SubdocFlagXattrPath,
								},
							},
							Deadline: deadline,
							Flags:    memd.SubdocDocFlagAccessDeleted,
						}, func(result *gocbcore.LookupInResult, err error) {
							if err != nil {
								cb(nil, err)
								return
							}

							var txnMeta *jsonTxnXattr
							if result.Ops[0].Err == nil {
								var txnMetaVal jsonTxnXattr
								// TODO(brett19): Don't ignore the error here
								json.Unmarshal(result.Ops[0].Value, &txnMetaVal)
								txnMeta = &txnMetaVal
							}

							if txnMeta == nil {
								// There is no longer any txn meta-data, this means the write-write conflict
								// is resolved and we can throw a write-write conflict to retry immediately.
								cb(nil, t.createOperationFailedError(true, false, sdErr.InnerError, ErrorReasonTransactionFailed, ErrorClassFailWriteWriteConflict))
								return
							}

							t.writeWriteConflictPoll(GetOptions{
								Agent:          opts.Document.agent,
								ScopeName:      opts.Document.scopeName,
								CollectionName: opts.Document.collectionName,
								Key:            opts.Document.key,
							}, txnMeta, func(err error) {
								// We have either resolved the write-write conflict, or we have elapsed our
								// maximal waiting period, let's send the error to retry.
								cb(nil, t.createOperationFailedError(true, false, sdErr.InnerError, ErrorReasonTransactionFailed, ErrorClassFailWriteWriteConflict))
							})
							return
						})
						return
					}
				}

				cb(nil, err)
				return
			}

			t.hooks.AfterStagedReplaceComplete(key, func(err error) {
				if err != nil {
					cb(nil, err)
					return
				}

				stagedInfo.Cas = result.Cas
				cb(stagedInfo, nil)
			})
		})
	})
}

func (t *transactionAttempt) Remove(opts RemoveOptions, cb StoreCallback) error {
	if err := t.checkDone(); err != nil {
		ec := t.classifyError(err)
		return t.createOperationFailedError(false, false, ErrAttemptExpired, ErrorReasonTransactionExpired, ec)
	}

	t.checkExpired(hookRemove, opts.Document.key, func(err error) {
		if err != nil {
			t.expiryOvertimeMode = true
			cb(nil, t.createOperationFailedError(false, false, ErrAttemptExpired,
				ErrorReasonTransactionExpired, ErrorClassFailExpiry))
			return
		}

		agent := opts.Document.agent
		scopeName := opts.Document.scopeName
		collectionName := opts.Document.collectionName
		key := opts.Document.key

		t.lock.Lock()

		_, existingMutation := t.getStagedMutationLocked(agent.BucketName(), opts.Document.scopeName, opts.Document.collectionName,
			opts.Document.key)
		if existingMutation != nil && existingMutation.OpType == StagedMutationInsert {
			t.lock.Unlock()
			cb(nil, t.createOperationFailedError(false, false, ErrIllegalState, ErrorReasonTransactionFailed, ErrorClassFailOther))
			return
		}
		t.lock.Unlock()

		err = t.confirmATRPending(agent, scopeName, collectionName, key, func(err error) {
			if err != nil {
				cb(nil, err)
				return
			}

			t.remove(opts.Document, func(res *GetResult, err error) {
				if err != nil {
					var failErr error
					ec := t.classifyError(err)
					switch ec {
					case ErrorClassFailExpiry:
						t.expiryOvertimeMode = true
						failErr = t.createOperationFailedError(false, false, ErrAttemptExpired, ErrorReasonTransactionExpired, ec)
					case ErrorClassFailDocNotFound:
						failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec)
					case ErrorClassFailCasMismatch:
						failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec)
					case ErrorClassFailDocAlreadyExists:
						failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ErrorClassFailCasMismatch)
					case ErrorClassFailTransient:
						failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec)
					case ErrorClassFailAmbiguous:
						failErr = t.createOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec)
					case ErrorClassFailHard:
						failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec)
					default:
						failErr = t.createOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec)
					}

					cb(nil, failErr)
					return
				}

				cb(res, nil)
			})
		})
		if err != nil {
			cb(nil, err)
			return
		}
	})

	return nil
}

func (t *transactionAttempt) remove(doc *GetResult, cb StoreCallback) {
	t.hooks.BeforeStagedRemove(doc.key, func(err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		stagedInfo := &stagedMutation{
			OpType:         StagedMutationRemove,
			Agent:          doc.agent,
			ScopeName:      doc.scopeName,
			CollectionName: doc.collectionName,
			Key:            doc.key,
			IsTombstone:    true,
		}

		var txnMeta jsonTxnXattr
		txnMeta.ID.Transaction = t.transactionID
		txnMeta.ID.Attempt = t.id
		txnMeta.ATR.CollectionName = t.atrCollectionName
		txnMeta.ATR.ScopeName = t.atrScopeName
		txnMeta.ATR.BucketName = t.atrAgent.BucketName()
		txnMeta.ATR.DocID = string(t.atrKey)
		txnMeta.Operation.Type = jsonMutationRemove

		restore := struct {
			OriginalCAS string
			ExpiryTime  uint
			RevID       string
		}{
			OriginalCAS: fmt.Sprintf("%d", doc.Cas),
			ExpiryTime:  doc.Meta.Expiry,
			RevID:       doc.Meta.RevID,
		}
		txnMeta.Restore = (*struct {
			OriginalCAS string `json:"CAS,omitempty"`
			ExpiryTime  uint   `json:"exptime"`
			RevID       string `json:"revid,omitempty"`
		})(&restore)

		txnMetaBytes, _ := json.Marshal(txnMeta)
		// TODO(brett19): Don't ignore the error here.

		var duraTimeout time.Duration
		var deadline time.Time
		if t.keyValueTimeout > 0 {
			deadline = time.Now().Add(t.keyValueTimeout)
			duraTimeout = t.keyValueTimeout * 10 / 9
		}

		flags := memd.SubdocDocFlagNone
		if doc.Meta.Deleted {
			flags = memd.SubdocDocFlagAccessDeleted
		}

		_, err = stagedInfo.Agent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      stagedInfo.ScopeName,
			CollectionName: stagedInfo.CollectionName,
			Key:            stagedInfo.Key,
			Cas:            doc.Cas,
			Ops: []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpDictSet,
					Path:  "txn",
					Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath,
					Value: txnMetaBytes,
				},
				{
					Op:    memd.SubDocOpDictSet,
					Path:  "txn.op.crc32",
					Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
					Value: crc32cMacro,
				},
			},
			Flags:                  flags,
			DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
			DurabilityLevelTimeout: duraTimeout,
			Deadline:               deadline,
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				cb(nil, err)
				return
			}

			t.hooks.AfterStagedRemoveComplete(doc.key, func(err error) {
				if err != nil {
					cb(nil, err)
					return
				}

				t.lock.Lock()
				stagedInfo.Cas = result.Cas
				t.stagedMutations = append(t.stagedMutations, stagedInfo)
				t.lock.Unlock()

				cb(&GetResult{
					agent:          stagedInfo.Agent,
					scopeName:      stagedInfo.ScopeName,
					collectionName: stagedInfo.CollectionName,
					key:            stagedInfo.Key,
					Value:          stagedInfo.Staged,
					Cas:            result.Cas,
				}, nil)
			})
		})
		if err != nil {
			cb(nil, err)
		}
	})
}

func (t *transactionAttempt) unstageRepMutation(mutation stagedMutation, casZero, ambiguityResolution bool, cb func(error)) {
	handler := func(err error) {
		if err == nil {
			cb(nil)
			return
		}

		ec := t.classifyError(err)
		if t.expiryOvertimeMode {
			cb(t.createOperationFailedError(false, true, ErrAttemptExpired,
				ErrorReasonTransactionFailedPostCommit, ErrorClassFailExpiry))
			return
		}

		var failErr error
		switch ec {
		case ErrorClassFailAmbiguous:
			time.AfterFunc(3*time.Millisecond, func() {
				t.unstageRepMutation(mutation, casZero, true, cb)
			})
			return
		case ErrorClassFailCasMismatch:
			fallthrough
		case ErrorClassFailDocAlreadyExists:
			if !ambiguityResolution {
				time.AfterFunc(3*time.Millisecond, func() {
					t.unstageRepMutation(mutation, true, ambiguityResolution, cb)
				})
				return
			}
			failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailedPostCommit, ec)
		case ErrorClassFailDocNotFound:
			t.unstageInsMutation(mutation, ambiguityResolution, cb)
			return
		case ErrorClassFailHard:
			failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec)
		default:
			failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailedPostCommit, ec)
		}

		cb(failErr)
	}

	t.hooks.BeforeDocCommitted(mutation.Key, func(err error) {
		if err != nil {
			handler(err)
			return
		}

		t.checkExpired("", mutation.Key, func(err error) {
			if err != nil {
				t.expiryOvertimeMode = true
			}

			var duraTimeout time.Duration
			var deadline time.Time
			if t.keyValueTimeout > 0 {
				deadline = time.Now().Add(t.keyValueTimeout)
				duraTimeout = t.keyValueTimeout * 10 / 9
			}

			cas := mutation.Cas
			if casZero {
				cas = 0
			}

			_, err = mutation.Agent.MutateIn(gocbcore.MutateInOptions{
				ScopeName:      mutation.ScopeName,
				CollectionName: mutation.CollectionName,
				Key:            mutation.Key,
				Cas:            cas,
				Ops: []gocbcore.SubDocOp{
					{
						Op:    memd.SubDocOpDictSet,
						Path:  "txn",
						Flags: memd.SubdocFlagXattrPath,
						Value: []byte{110, 117, 108, 108}, // null
					},
					{
						Op:    memd.SubDocOpDelete,
						Path:  "txn",
						Flags: memd.SubdocFlagXattrPath,
					},
					{
						Op:    memd.SubDocOpSetDoc,
						Path:  "",
						Value: mutation.Staged,
					},
				},
				Deadline:               deadline,
				DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
				DurabilityLevelTimeout: duraTimeout,
			}, func(result *gocbcore.MutateInResult, err error) {
				if err != nil {
					handler(err)
					return
				}

				for _, op := range result.Ops {
					if op.Err != nil {
						handler(op.Err)
						return
					}
				}

				t.hooks.AfterDocCommittedBeforeSavingCAS(mutation.Key, func(err error) {
					if err != nil {
						handler(err)
						return
					}

					t.lock.Lock()
					t.finalMutationTokens = append(t.finalMutationTokens, MutationToken{
						BucketName:    mutation.Agent.BucketName(),
						MutationToken: result.MutationToken,
					})
					t.lock.Unlock()

					handler(nil)
				})
			})
			if err != nil {
				cb(err)
				return
			}
		})
	})
}

func (t *transactionAttempt) unstageInsMutation(mutation stagedMutation, ambiguityResolution bool, cb func(error)) {
	handler := func(err error) {
		if err == nil {
			cb(nil)
			return
		}

		ec := t.classifyError(err)
		if t.expiryOvertimeMode {
			cb(t.createOperationFailedError(false, true, ErrAttemptExpired,
				ErrorReasonTransactionFailedPostCommit, ErrorClassFailExpiry))
			return
		}

		var failErr error
		switch ec {
		case ErrorClassFailAmbiguous:
			time.AfterFunc(3*time.Millisecond, func() {
				t.unstageInsMutation(mutation, true, cb)
			})
			return
		case ErrorClassFailDocAlreadyExists:
			if !ambiguityResolution {
				time.AfterFunc(3*time.Millisecond, func() {
					t.unstageRepMutation(mutation, true, ambiguityResolution, cb)
				})
				return
			}
			failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailedPostCommit, ec)
		case ErrorClassFailHard:
			failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec)
		default:
			failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailedPostCommit, ec)
		}

		cb(failErr)
	}

	t.hooks.BeforeDocCommitted(mutation.Key, func(err error) {
		if err != nil {
			handler(err)
			return
		}

		t.checkExpired("", mutation.Key, func(err error) {
			if err != nil {
				t.expiryOvertimeMode = true
			}

			var duraTimeout time.Duration
			var deadline time.Time
			if t.keyValueTimeout > 0 {
				deadline = time.Now().Add(t.keyValueTimeout)
				duraTimeout = t.keyValueTimeout * 10 / 9
			}

			_, err = mutation.Agent.Add(gocbcore.AddOptions{
				ScopeName:              mutation.ScopeName,
				CollectionName:         mutation.CollectionName,
				Key:                    mutation.Key,
				Value:                  mutation.Staged,
				Deadline:               deadline,
				DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
				DurabilityLevelTimeout: duraTimeout,
			}, func(result *gocbcore.StoreResult, err error) {
				if err != nil {
					handler(err)
					return
				}

				t.hooks.AfterDocCommittedBeforeSavingCAS(mutation.Key, func(err error) {
					if err != nil {
						handler(err)
						return
					}

					t.lock.Lock()
					t.finalMutationTokens = append(t.finalMutationTokens, MutationToken{
						BucketName:    mutation.Agent.BucketName(),
						MutationToken: result.MutationToken,
					})
					t.lock.Unlock()

					handler(nil)
				})
			})
			if err != nil {
				handler(err)
				return
			}
		})
	})
}

func (t *transactionAttempt) unstageRemMutation(mutation stagedMutation, cb func(error)) {
	if mutation.OpType != StagedMutationRemove {
		cb(ErrUhOh)
		return
	}

	handler := func(err error) {
		if err == nil {
			cb(nil)
			return
		}

		ec := t.classifyError(err)
		if t.expiryOvertimeMode {
			cb(t.createOperationFailedError(false, true, ErrAttemptExpired,
				ErrorReasonTransactionFailedPostCommit, ErrorClassFailExpiry))
			return
		}

		var failErr error
		switch ec {
		case ErrorClassFailAmbiguous:
			time.AfterFunc(3*time.Millisecond, func() {
				t.unstageRemMutation(mutation, cb)
			})
			return
		case ErrorClassFailDocNotFound:
			failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailedPostCommit, ec)
		case ErrorClassFailHard:
			failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec)
		default:
			failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailedPostCommit, ec)
		}

		cb(failErr)
	}

	t.hooks.BeforeDocRemoved(mutation.Key, func(err error) {
		if err != nil {
			handler(err)
			return
		}

		t.checkExpired("", mutation.Key, func(err error) {
			if err != nil {
				t.expiryOvertimeMode = true
			}

			var duraTimeout time.Duration
			var deadline time.Time
			if t.keyValueTimeout > 0 {
				deadline = time.Now().Add(t.keyValueTimeout)
				duraTimeout = t.keyValueTimeout * 10 / 9
			}

			_, err = mutation.Agent.Delete(gocbcore.DeleteOptions{
				ScopeName:              mutation.ScopeName,
				CollectionName:         mutation.CollectionName,
				Key:                    mutation.Key,
				Cas:                    0,
				Deadline:               deadline,
				DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
				DurabilityLevelTimeout: duraTimeout,
			}, func(result *gocbcore.DeleteResult, err error) {
				if err != nil {
					handler(err)
					return
				}

				t.hooks.AfterDocRemovedPreRetry(mutation.Key, func(err error) {
					if err != nil {
						handler(err)
						return
					}

					t.finalMutationTokens = append(t.finalMutationTokens, MutationToken{
						BucketName:    mutation.Agent.BucketName(),
						MutationToken: result.MutationToken,
					})

					t.hooks.AfterDocRemovedPostRetry(mutation.Key, func(err error) {
						if err != nil {
							handler(err)
							return
						}

						handler(nil)
					})
				})
			})
			if err != nil {
				handler(err)
				return
			}
		})
	})
}

func (t *transactionAttempt) Commit(cb CommitCallback) error {
	t.txnOpSection.Wait(func() {
		t.lock.Lock()
		if t.state == AttemptStateNothingWritten {
			t.lock.Unlock()

			t.txnAtrSection.Wait(func() {
				cb(nil)
			})

			return
		}
		t.lock.Unlock()

		// TODO(brett19): Move the wait logic from setATRCommitted to here
		t.setATRCommitted(func(err error) {
			if err != nil {
				cb(err)
				return
			}

			// TODO(brett19): Use atomic counters instead of a goroutine here
			go func() {
				numMutations := len(t.stagedMutations)
				waitCh := make(chan error, numMutations)

				// Unlike the RFC we do insert and replace separately. We have a bug in gocbcore where subdocs
				// will raise doc exists rather than a cas mismatch so we need to do these ops separately to tell
				// how to handle that error.
				for _, mutation := range t.stagedMutations {
					if mutation.OpType == StagedMutationInsert {
						t.unstageInsMutation(*mutation, false, func(err error) {
							waitCh <- err
						})
					} else if mutation.OpType == StagedMutationReplace {
						t.unstageRepMutation(*mutation, false, false, func(err error) {
							waitCh <- err
						})
					} else if mutation.OpType == StagedMutationRemove {
						t.unstageRemMutation(*mutation, func(err error) {
							waitCh <- err
						})
					} else {
						// TODO(brett19): Pretty sure I can do better than this
						waitCh <- ErrUhOh
					}
				}

				var mutErr error
				for i := 0; i < numMutations; i++ {
					// TODO(brett19): Handle errors here better
					err := <-waitCh
					if mutErr == nil && err != nil {
						mutErr = err
					}
				}
				if mutErr != nil {
					// The unstage operations themselves will handle enhancing the error.
					cb(err)
					return
				}

				t.setATRCompleted(func(err error) {
					if err != nil {
						cb(err)
						return
					}

					cb(nil)
				})
			}()
		})
	})
	return nil
}

func (t *transactionAttempt) abort(
	cb func(error),
) {
	var handler func(err error)
	handler = func(err error) {
		if err != nil {
			ec := t.classifyError(err)

			if t.expiryOvertimeMode {
				cb(t.createOperationFailedError(false, true, ErrAttemptExpired,
					ErrorReasonTransactionExpired, ErrorClassFailExpiry))
				return
			}

			var failErr error
			switch ec {
			case ErrorClassFailExpiry:
				t.expiryOvertimeMode = true
				time.AfterFunc(3*time.Millisecond, func() {
					t.abort(cb)
				})
				return
			case ErrorClassFailPathNotFound:
				failErr = t.createOperationFailedError(false, true, ErrAtrEntryNotFound, ErrorReasonTransactionFailed, ec)
			case ErrorClassFailDocNotFound:
				failErr = t.createOperationFailedError(false, true, ErrAtrNotFound, ErrorReasonTransactionFailed, ec)
			case ErrorClassFailATRFull:
				failErr = t.createOperationFailedError(false, true, ErrAtrFull, ErrorReasonTransactionFailed, ec)
			case ErrorClassFailHard:
				failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec)
			default:
				time.AfterFunc(3*time.Millisecond, func() {
					t.abort(cb)
				})
				return
			}

			cb(failErr)
			return
		}

		cb(nil)
	}

	if !t.expiryOvertimeMode {
		t.checkExpired(hookATRAbort, []byte{}, func(err error) {
			if err != nil {
				handler(err)
				return
			}

			t.setATRAborted(handler)
		})
		return
	}

	t.setATRAborted(handler)
}

func (t *transactionAttempt) setATRAborted(
	cb func(error),
) error {
	t.hooks.BeforeATRAborted(func(err error) {
		if err != nil {
			cb(err)
			return
		}

		t.lock.Lock()
		atrAgent := t.atrAgent
		atrScopeName := t.atrScopeName
		atrKey := t.atrKey
		atrCollectionName := t.atrCollectionName

		insMutations := []jsonAtrMutation{}
		repMutations := []jsonAtrMutation{}
		remMutations := []jsonAtrMutation{}

		for _, mutation := range t.stagedMutations {
			jsonMutation := jsonAtrMutation{
				BucketName:     mutation.Agent.BucketName(),
				ScopeName:      mutation.ScopeName,
				CollectionName: mutation.CollectionName,
				DocID:          string(mutation.Key),
			}

			if mutation.OpType == StagedMutationInsert {
				insMutations = append(insMutations, jsonMutation)
			} else if mutation.OpType == StagedMutationReplace {
				repMutations = append(repMutations, jsonMutation)
			} else if mutation.OpType == StagedMutationRemove {
				remMutations = append(remMutations, jsonMutation)
			} else {
				// TODO(brett19): Signal an error here
			}
		}

		t.txnAtrSection.Add(1)
		t.lock.Unlock()

		atrFieldOp := func(fieldName string, data interface{}, flags memd.SubdocFlag) gocbcore.SubDocOp {
			bytes, _ := json.Marshal(data)

			return gocbcore.SubDocOp{
				Op:    memd.SubDocOpDictSet,
				Flags: memd.SubdocFlagMkDirP | flags,
				Path:  "attempts." + t.id + "." + fieldName,
				Value: bytes,
			}
		}

		var duraTimeout time.Duration
		var deadline time.Time
		if t.keyValueTimeout > 0 {
			deadline = time.Now().Add(t.keyValueTimeout)
			duraTimeout = t.keyValueTimeout * 10 / 9
		}

		_, err = atrAgent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      atrScopeName,
			CollectionName: atrCollectionName,
			Key:            atrKey,
			Ops: []gocbcore.SubDocOp{
				atrFieldOp("st", jsonAtrStateAborted, memd.SubdocFlagXattrPath),
				atrFieldOp("tsrs", "${Mutation.CAS}", memd.SubdocFlagXattrPath|memd.SubdocFlagExpandMacros),
				atrFieldOp("p", 0, memd.SubdocFlagXattrPath),
				atrFieldOp("ins", insMutations, memd.SubdocFlagXattrPath),
				atrFieldOp("rep", repMutations, memd.SubdocFlagXattrPath),
				atrFieldOp("rem", remMutations, memd.SubdocFlagXattrPath),
			},
			DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
			DurabilityLevelTimeout: duraTimeout,
			Flags:                  memd.SubdocDocFlagNone,
			Deadline:               deadline,
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				t.lock.Lock()
				t.txnAtrSection.Done()
				t.lock.Unlock()

				cb(err)
				return
			}

			t.hooks.AfterATRAborted(func(err error) {
				if err != nil {
					t.lock.Lock()
					t.txnAtrSection.Done()
					t.lock.Unlock()
					cb(err)
					return
				}

				t.lock.Lock()
				t.state = AttemptStateAborted
				t.txnAtrSection.Done()
				t.lock.Unlock()

				cb(nil)
			})
		})
		if err != nil {
			t.lock.Lock()
			t.txnAtrSection.Done()
			t.lock.Unlock()

			cb(err)
		}
	})
	return nil
}

func (t *transactionAttempt) rollbackInsMutation(mutation stagedMutation, cb func(error)) {
	var handler func(error)
	handler = func(err error) {
		if err != nil {
			ec := t.classifyError(err)
			if t.expiryOvertimeMode {
				cb(t.createOperationFailedError(false, true, ErrAttemptExpired,
					ErrorReasonTransactionExpired, ErrorClassFailExpiry))
				return
			}

			switch ec {
			case ErrorClassFailExpiry:
				t.expiryOvertimeMode = true
			case ErrorClassFailPathNotFound:
				cb(nil)
				return
			case ErrorClassFailDocNotFound:
				cb(nil)
				return
			case ErrorClassFailCasMismatch:
				cb(t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec))
				return
			case ErrorClassFailDocAlreadyExists:
				cb(t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed,
					ErrorClassFailCasMismatch))
				return
			case ErrorClassFailHard:
				cb(t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec))
				return
			}

			time.AfterFunc(3*time.Millisecond, func() {
				t.rollbackInsMutation(mutation, cb)
			})
			return
		}

		cb(nil)
	}
	if mutation.OpType != StagedMutationInsert {
		cb(ErrUhOh)
		return
	}

	t.checkExpired(hookRollbackDoc, mutation.Key, func(err error) {
		if err != nil && !t.expiryOvertimeMode {
			handler(err)
			return
		}
		t.hooks.BeforeRollbackDeleteInserted(mutation.Key, func(err error) {
			if err != nil {
				handler(err)
				return
			}

			_, err = mutation.Agent.MutateIn(gocbcore.MutateInOptions{
				ScopeName:      mutation.ScopeName,
				CollectionName: mutation.CollectionName,
				Key:            mutation.Key,
				Cas:            mutation.Cas,
				Flags:          memd.SubdocDocFlagAccessDeleted,
				Ops: []gocbcore.SubDocOp{
					{
						Op:    memd.SubDocOpDictSet,
						Path:  "txn",
						Flags: memd.SubdocFlagXattrPath,
						Value: []byte{110, 117, 108, 108}, // null
					},
					{
						Op:    memd.SubDocOpDelete,
						Path:  "txn",
						Flags: memd.SubdocFlagXattrPath,
					},
				},
			}, func(result *gocbcore.MutateInResult, err error) {
				if err != nil {
					handler(err)
					return
				}

				t.hooks.AfterRollbackDeleteInserted(mutation.Key, func(err error) {
					if err != nil {
						handler(err)
						return
					}

					handler(nil)
				})
			})
			if err != nil {
				handler(err)
				return
			}
		})
	})
}

func (t *transactionAttempt) rollbackRepRemMutation(mutation stagedMutation, cb func(error)) {
	var handler func(error)
	handler = func(err error) {
		if err != nil {
			ec := t.classifyError(err)
			if t.expiryOvertimeMode {
				cb(t.createOperationFailedError(false, true, ErrAttemptExpired,
					ErrorReasonTransactionExpired, ErrorClassFailExpiry))
				return
			}

			switch ec {
			case ErrorClassFailExpiry:
				t.expiryOvertimeMode = true
			case ErrorClassFailPathNotFound:
				cb(nil)
				return
			case ErrorClassFailDocNotFound:
				cb(t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec))
				return
			case ErrorClassFailCasMismatch:
				cb(t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec))
				return
			case ErrorClassFailDocAlreadyExists:
				cb(t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed,
					ErrorClassFailCasMismatch))
				return
			case ErrorClassFailHard:
				cb(t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec))
				return
			}

			time.AfterFunc(3*time.Millisecond, func() {
				t.rollbackRepRemMutation(mutation, cb)
			})
			return
		}

		cb(nil)
	}

	if mutation.OpType != StagedMutationRemove && mutation.OpType != StagedMutationReplace {
		cb(ErrUhOh)
		return
	}

	t.checkExpired(hookRollbackDoc, mutation.Key, func(err error) {
		if err != nil && !t.expiryOvertimeMode {
			handler(err)
			return
		}
		t.hooks.BeforeDocRolledBack(mutation.Key, func(err error) {
			if err != nil {
				handler(err)
				return
			}

			_, err = mutation.Agent.MutateIn(gocbcore.MutateInOptions{
				ScopeName:      mutation.ScopeName,
				CollectionName: mutation.CollectionName,
				Key:            mutation.Key,
				Cas:            mutation.Cas,
				Ops: []gocbcore.SubDocOp{
					{
						Op:    memd.SubDocOpDictSet,
						Path:  "txn",
						Flags: memd.SubdocFlagXattrPath,
						Value: []byte{110, 117, 108, 108}, // null
					},
					{
						Op:    memd.SubDocOpDelete,
						Path:  "txn",
						Flags: memd.SubdocFlagXattrPath,
					},
				},
			}, func(result *gocbcore.MutateInResult, err error) {
				if err != nil {
					handler(err)
					return
				}

				t.hooks.AfterRollbackReplaceOrRemove(mutation.Key, func(err error) {
					if err != nil {
						handler(err)
						return
					}

					handler(nil)
				})
			})
			if err != nil {
				handler(err)
				return
			}
		})
	})
}

func (t *transactionAttempt) setATRRolledBack(
	cb func(error),
) error {
	handler := func(err error) {
		if err != nil {
			ec := t.classifyError(err)

			if t.expiryOvertimeMode {
				cb(t.createOperationFailedError(false, true, ErrAttemptExpired,
					ErrorReasonTransactionExpired, ErrorClassFailExpiry))
				return
			}

			var failErr error
			switch ec {
			case ErrorClassFailExpiry:
				t.expiryOvertimeMode = true
				time.AfterFunc(3*time.Millisecond, func() {
					t.setATRRolledBack(cb)
				})
				return
			case ErrorClassFailPathNotFound:
				failErr = t.createOperationFailedError(false, true, ErrAtrEntryNotFound, ErrorReasonTransactionFailed, ec)
			case ErrorClassFailDocNotFound:
				failErr = t.createOperationFailedError(false, true, ErrAtrNotFound, ErrorReasonTransactionFailed, ec)
			case ErrorClassFailATRFull:
				failErr = t.createOperationFailedError(false, true, ErrAtrFull, ErrorReasonTransactionFailed, ec)
			case ErrorClassFailHard:
				failErr = t.createOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec)
			default:
				time.AfterFunc(3*time.Millisecond, func() {
					t.setATRRolledBack(cb)
				})
				return
			}

			cb(failErr)
			return
		}

		cb(nil)
	}

	t.checkExpired(hookATRRollbackComplete, []byte{}, func(err error) {
		if err != nil {
			t.expiryOvertimeMode = true
		}

		t.hooks.BeforeATRRolledBack(func(err error) {
			if err != nil {
				handler(err)
				return
			}

			t.lock.Lock()
			if t.state != AttemptStateAborted {
				t.lock.Unlock()

				t.txnAtrSection.Wait(func() {
					handler(nil)
				})

				return
			}

			atrAgent := t.atrAgent
			atrScopeName := t.atrScopeName
			atrKey := t.atrKey
			atrCollectionName := t.atrCollectionName

			t.txnAtrSection.Add(1)
			t.lock.Unlock()

			atrFieldOp := func(fieldName string, data interface{}, flags memd.SubdocFlag) gocbcore.SubDocOp {
				bytes, _ := json.Marshal(data)

				return gocbcore.SubDocOp{
					Op:    memd.SubDocOpDictSet,
					Flags: memd.SubdocFlagMkDirP | flags,
					Path:  "attempts." + t.id + "." + fieldName,
					Value: bytes,
				}
			}

			var duraTimeout time.Duration
			var deadline time.Time
			if t.keyValueTimeout > 0 {
				deadline = time.Now().Add(t.keyValueTimeout)
				duraTimeout = t.keyValueTimeout * 10 / 9
			}

			_, err = atrAgent.MutateIn(gocbcore.MutateInOptions{
				ScopeName:      atrScopeName,
				CollectionName: atrCollectionName,
				Key:            atrKey,
				Ops: []gocbcore.SubDocOp{
					atrFieldOp("st", jsonAtrStateRolledBack, memd.SubdocFlagXattrPath),
					atrFieldOp("tsrc", "${Mutation.CAS}", memd.SubdocFlagXattrPath|memd.SubdocFlagExpandMacros),
				},
				DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
				DurabilityLevelTimeout: duraTimeout,
				Deadline:               deadline,
				Flags:                  memd.SubdocDocFlagNone,
			}, func(result *gocbcore.MutateInResult, err error) {
				if err != nil {
					t.lock.Lock()
					t.txnAtrSection.Done()
					t.lock.Unlock()

					handler(err)
					return
				}

				t.hooks.AfterATRRolledBack(func(err error) {
					if err != nil {
						handler(err)
						return
					}

					t.lock.Lock()
					t.state = AttemptStateRolledBack
					t.txnAtrSection.Done()
					t.lock.Unlock()

					cb(nil)
				})
			})
			if err != nil {
				t.lock.Lock()
				t.txnAtrSection.Done()
				t.lock.Unlock()

				handler(err)
			}
		})
	})

	return nil
}

func (t *transactionAttempt) Rollback(cb RollbackCallback) error {
	t.txnOpSection.Wait(func() {
		t.lock.Lock()
		if t.state == AttemptStateNothingWritten {
			t.lock.Unlock()
			t.txnAtrSection.Wait(func() {
				cb(nil)
			})

			return
		}

		if t.state == AttemptStateRolledBack || t.state == AttemptStateCompleted || t.state == AttemptStateCommitted {
			t.lock.Unlock()

			t.txnAtrSection.Wait(func() {
				cb(t.createOperationFailedError(false, true, nil,
					ErrorReasonTransactionFailed, ErrorClassFailOther))
			})

			return
		}
		t.lock.Unlock()

		t.abort(func(err error) {
			if err != nil {
				cb(err)
				return
			}

			// TODO(brett19): Use atomic counters instead of a goroutine here
			// TODO: This is running the unstages sequentially because of testing, in future we will optimise this
			go func() {
				for _, mutation := range t.stagedMutations {
					waitCh := make(chan error, 1)
					if mutation.OpType == StagedMutationInsert {
						t.rollbackInsMutation(*mutation, func(err error) {
							waitCh <- err
						})
					} else if mutation.OpType == StagedMutationRemove || mutation.OpType == StagedMutationReplace {
						t.rollbackRepRemMutation(*mutation, func(err error) {
							waitCh <- err
						})
					} else {
						// TODO(brett19): Pretty sure I can do better than this
						waitCh <- err
					}

					err := <-waitCh
					if err != nil {
						cb(err)
						return
					}
				}

				t.setATRRolledBack(func(err error) {
					if err != nil {
						cb(err)
						return
					}

					cb(nil)
				})
			}()
		})
	})

	return nil
}
