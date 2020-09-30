package transactions

import (
	"encoding/json"
	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
	"time"
)

func (t *transactionAttempt) abort(
	cb func(error),
) {
	var handler func(err error)
	handler = func(err error) {
		if err != nil {
			ec := t.classifyError(err)

			if t.expiryOvertimeMode {
				cb(t.createAndStashOperationFailedError(false, true, ErrAttemptExpired,
					ErrorReasonTransactionExpired, ErrorClassFailExpiry, true))
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
				failErr = t.createAndStashOperationFailedError(false, true, ErrAtrEntryNotFound,
					ErrorReasonTransactionFailed, ec, true)
			case ErrorClassFailDocNotFound:
				failErr = t.createAndStashOperationFailedError(false, true, ErrAtrNotFound,
					ErrorReasonTransactionFailed, ec, true)
			case ErrorClassFailATRFull:
				failErr = t.createAndStashOperationFailedError(false, true, ErrAtrFull,
					ErrorReasonTransactionFailed, ec, true)
			case ErrorClassFailHard:
				failErr = t.createAndStashOperationFailedError(false, true, err,
					ErrorReasonTransactionFailed, ec, true)
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
				cb(t.createAndStashOperationFailedError(false, true, ErrAttemptExpired,
					ErrorReasonTransactionExpired, ErrorClassFailExpiry, true))
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
				cb(t.createAndStashOperationFailedError(false, true, err,
					ErrorReasonTransactionFailed, ec, true))
				return
			case ErrorClassFailDocAlreadyExists:
				cb(t.createAndStashOperationFailedError(false, true, err,
					ErrorReasonTransactionFailed, ErrorClassFailCasMismatch, true))
				return
			case ErrorClassFailHard:
				cb(t.createAndStashOperationFailedError(false, true, err,
					ErrorReasonTransactionFailed, ec, true))
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
				cb(t.createAndStashOperationFailedError(false, true, ErrAttemptExpired,
					ErrorReasonTransactionExpired, ErrorClassFailExpiry, true))
				return
			}

			switch ec {
			case ErrorClassFailExpiry:
				t.expiryOvertimeMode = true
			case ErrorClassFailPathNotFound:
				cb(nil)
				return
			case ErrorClassFailDocNotFound:
				cb(t.createAndStashOperationFailedError(false, true, err,
					ErrorReasonTransactionFailed, ec, true))
				return
			case ErrorClassFailCasMismatch:
				cb(t.createAndStashOperationFailedError(false, true, err,
					ErrorReasonTransactionFailed, ec, true))
				return
			case ErrorClassFailDocAlreadyExists:
				cb(t.createAndStashOperationFailedError(false, true, err,
					ErrorReasonTransactionFailed, ErrorClassFailCasMismatch, true))
				return
			case ErrorClassFailHard:
				cb(t.createAndStashOperationFailedError(false, true, err,
					ErrorReasonTransactionFailed, ec, true))
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
				cb(t.createAndStashOperationFailedError(false, true, ErrAttemptExpired,
					ErrorReasonTransactionExpired, ErrorClassFailExpiry, true))
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
				failErr = t.createAndStashOperationFailedError(false, true, ErrAtrEntryNotFound,
					ErrorReasonTransactionFailed, ec, true)
			case ErrorClassFailDocNotFound:
				failErr = t.createAndStashOperationFailedError(false, true, ErrAtrNotFound,
					ErrorReasonTransactionFailed, ec, true)
			case ErrorClassFailATRFull:
				failErr = t.createAndStashOperationFailedError(false, true, ErrAtrFull,
					ErrorReasonTransactionFailed, ec, true)
			case ErrorClassFailHard:
				failErr = t.createAndStashOperationFailedError(false, true, err,
					ErrorReasonTransactionFailed, ec, true)
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
				cb(t.createAndStashOperationFailedError(false, true, nil,
					ErrorReasonTransactionFailed, ErrorClassFailOther, true))
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
