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
var revidMacro = []byte("\"${$document.revid}\"")
var exptimeMacro = []byte("\"${$document.exptime}\"")
var casMacro = []byte("\"${$document.CAS}\"")

type transactionAttempt struct {
	// immutable state
	expiryTime       time.Time
	txnStartTime     time.Time
	operationTimeout time.Duration
	durabilityLevel  DurabilityLevel
	transactionID    string
	id               string
	hooks            TransactionHooks
	serialUnstaging  bool
	explicitAtrs     bool
	atrLocation      ATRLocation

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

func (t *transactionAttempt) GetATRLocation() ATRLocation {
	t.lock.Lock()
	if t.atrAgent != nil {
		location := ATRLocation{
			Agent:          t.atrAgent,
			ScopeName:      t.atrScopeName,
			CollectionName: t.atrCollectionName,
		}
		t.lock.Unlock()

		return location
	}
	t.lock.Unlock()

	return t.atrLocation
}

func (t *transactionAttempt) SetATRLocation(location ATRLocation) error {
	t.lock.Lock()
	if t.atrAgent != nil {
		t.lock.Unlock()
		return errors.New("atr location cannot be set after mutations have occurred")
	}

	if t.atrLocation.Agent != nil {
		t.lock.Unlock()
		return errors.New("atr location can only be set once")
	}

	t.atrLocation = location

	t.lock.Unlock()
	return nil
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
			t.lock.Lock()
			if t.state == AttemptStateNothingWritten {
				t.lock.Unlock()
				handler(ErrPreviousOperationFailed)
				return
			}
			t.lock.Unlock()

			handler(nil)
		})

		return
	}

	t.state = AttemptStatePending
	t.txnAtrSection.Add(1)
	t.lock.Unlock()

	atrID := int(cbcVbMap(firstKey, 1024))
	t.hooks.RandomATRIDForVbucket(func(s string, err error) {
		if err != nil {
			t.lock.Lock()
			t.state = AttemptStateNothingWritten
			t.lock.Unlock()
			t.txnAtrSection.Done()
			handler(err)
			return
		}

		var atrKey []byte
		if s == "" {
			atrKey = []byte(atrIDList[atrID])
		} else {
			atrKey = []byte(s)
		}

		atrAgent := agent
		atrScopeName := "_default"
		atrCollectionName := "_default"
		if t.atrLocation.Agent != nil {
			atrAgent = t.atrLocation.Agent
			atrScopeName = t.atrLocation.ScopeName
			atrCollectionName = t.atrLocation.CollectionName
		} else {
			if t.explicitAtrs {
				handler(errors.New("atrs must be explicitly defined"))
				return
			}
		}

		t.lock.Lock()
		t.atrAgent = atrAgent
		t.atrScopeName = atrScopeName
		t.atrCollectionName = atrCollectionName
		t.atrKey = atrKey
		t.lock.Unlock()

		t.checkExpired(hookATRPending, []byte{}, func(err error) {
			if err != nil {
				t.lock.Lock()
				t.state = AttemptStateNothingWritten
				t.lock.Unlock()
				t.txnAtrSection.Done()
				handler(err)
				return
			}

			t.hooks.BeforeATRPending(func(err error) {
				if err != nil {
					t.lock.Lock()
					t.state = AttemptStateNothingWritten
					t.lock.Unlock()
					t.txnAtrSection.Done()
					handler(err)
					return
				}

				var duraTimeout time.Duration
				var deadline time.Time
				if t.operationTimeout > 0 {
					deadline = time.Now().Add(t.operationTimeout)
					duraTimeout = t.operationTimeout * 10 / 9
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
						{
							Op:    memd.SubDocOpSetDoc,
							Flags: memd.SubdocFlagNone,
							Path:  "",
							Value: []byte{0},
						},
					},
					DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
					DurabilityLevelTimeout: duraTimeout,
					Deadline:               deadline,
					Flags:                  memd.SubdocDocFlagMkDoc,
				}

				if marshalErr != nil {
					t.lock.Lock()
					t.state = AttemptStateNothingWritten
					t.lock.Unlock()
					t.txnAtrSection.Done()
					handler(err)
					return
				}

				_, err = agent.MutateIn(opts, func(result *gocbcore.MutateInResult, err error) {
					if err != nil {
						t.lock.Lock()
						t.state = AttemptStateNothingWritten
						t.lock.Unlock()
						t.txnAtrSection.Done()
						handler(err)
						return
					}

					for _, op := range result.Ops {
						if op.Err != nil {
							t.lock.Lock()
							t.state = AttemptStateNothingWritten
							t.lock.Unlock()
							t.txnAtrSection.Done()
							handler(op.Err)
							return
						}
					}

					t.hooks.AfterATRPending(func(err error) {
						if err != nil {
							t.lock.Lock()
							t.state = AttemptStateNothingWritten
							t.lock.Unlock()
							t.txnAtrSection.Done()
							handler(err)
							return
						}

						t.txnAtrSection.Done()
						handler(nil)
					})
				})
				if err != nil {
					t.lock.Lock()
					t.state = AttemptStateNothingWritten
					t.lock.Unlock()
					t.txnAtrSection.Done()
					handler(err)
					return
				}
			})
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

	return -1, nil
}

func (t *transactionAttempt) getTxnState(
	agent *gocbcore.Agent,
	scopeName string,
	collectionName string,
	atrDocID string,
	attemptID string,
	deadline time.Time,
	cb func(*jsonAtrAttempt, error),
) {
	_, err := agent.LookupIn(gocbcore.LookupInOptions{
		ScopeName:      scopeName,
		CollectionName: collectionName,
		Key:            []byte(atrDocID),
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpGet,
				Path:  "attempts." + attemptID,
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

func (t *transactionAttempt) writeWriteConflictPoll(res *GetResult, existingMutation *stagedMutation, stage forwardCompatStage, cb func(error)) {
	handler := func(err error) {
		if err != nil {
			ec := t.classifyError(err)

			// TODO(brett19): Probably need to add CASMismatch handling here...

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

	if res.Meta == nil {
		// There is no write-write conflict.
		handler(nil)
		return
	}

	if res.Meta.TransactionID == t.transactionID {
		if res.Meta.AttemptID == t.id {
			if existingMutation != nil {
				if res.Cas != existingMutation.Cas {
					// There was an existing mutation but it doesn't match the expected
					// CAS.  We throw a CAS mismatch to early detect this.
					handler(gocbcore.ErrCasMismatch)
					return
				}

				handler(nil)
				return
			}

			// This means that we are trying to overwrite a previous write this specific
			// attempt has performed without actually having found the existing mutation,
			// this is never going to work correctly.
			handler(ErrIllegalState)
			return
		}

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

		var forwardCompat map[string][]ForwardCompatibilityEntry
		if res.Meta != nil {
			forwardCompat = res.Meta.ForwardCompat
		}

		t.checkForwardCompatbility(stage, forwardCompat, func(err error) {
			if err != nil {
				// We've already enhanced this error
				cb(err)
				return
			}

			t.checkExpired("", res.key, func(err error) {
				if err != nil {
					t.expiryOvertimeMode = true
					handler(err)
					return
				}

				t.hooks.BeforeCheckATREntryForBlockingDoc([]byte(res.Meta.ATR.DocID), func(err error) {
					if err != nil {
						handler(err)
						return
					}

					t.getTxnState(
						res.agent,
						res.Meta.ATR.ScopeName,
						res.Meta.ATR.CollectionName,
						res.Meta.ATR.DocID,
						res.Meta.AttemptID,
						deadline,
						func(attempt *jsonAtrAttempt, err error) {
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
		ForwardCompat:     nil, // Let's just be explicit about this, it'll change in the future anyway.
	}
}

func (t *transactionAttempt) checkForwardCompatbility(stage forwardCompatStage, fc map[string][]ForwardCompatibilityEntry,
	cb func(error)) {
	checkForwardCompatbility(stage, fc, func(shouldRetry bool, retryInterval time.Duration, err error) {
		if err == nil {
			cb(nil)
			return
		}

		if retryInterval > 0 {
			time.AfterFunc(retryInterval, func() {
				cb(t.createAndStashOperationFailedError(shouldRetry, false, ErrForwardCompatibilityFailure,
					ErrorReasonTransactionFailed, ErrorClassFailOther, false))
			})
			return
		}

		cb(t.createAndStashOperationFailedError(shouldRetry, false, ErrForwardCompatibilityFailure,
			ErrorReasonTransactionFailed, ErrorClassFailOther, false))
	})
}

func jsonForwardCompatToForwardCompat(fc map[string][]jsonForwardCompatibilityEntry) map[string][]ForwardCompatibilityEntry {
	if fc == nil {
		return nil
	}
	forwardCompat := make(map[string][]ForwardCompatibilityEntry)

	for k, entries := range fc {
		if _, ok := forwardCompat[k]; !ok {
			forwardCompat[k] = make([]ForwardCompatibilityEntry, len(entries))
		}

		for i, entry := range entries {
			forwardCompat[k][i] = ForwardCompatibilityEntry(entry)
		}
	}

	return forwardCompat
}
