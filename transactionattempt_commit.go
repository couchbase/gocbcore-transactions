package transactions

import (
	"encoding/json"
	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
	"time"
)

func (t *transactionAttempt) unstageRepMutation(mutation stagedMutation, casZero, ambiguityResolution bool, cb func(error)) {
	handler := func(err error) {
		if err == nil {
			cb(nil)
			return
		}

		ec := t.classifyError(err)
		if t.expiryOvertimeMode {
			cb(t.createAndStashOperationFailedError(false, true, ErrAttemptExpired,
				ErrorReasonTransactionFailedPostCommit, ErrorClassFailExpiry, true))
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
			failErr = t.createAndStashOperationFailedError(false, true, err,
				ErrorReasonTransactionFailedPostCommit, ec, true)
		case ErrorClassFailDocNotFound:
			t.unstageInsMutation(mutation, ambiguityResolution, cb)
			return
		case ErrorClassFailHard:
			failErr = t.createAndStashOperationFailedError(false, true, err,
				ErrorReasonTransactionFailed, ec, true)
		default:
			failErr = t.createAndStashOperationFailedError(false, true, err,
				ErrorReasonTransactionFailedPostCommit, ec, true)
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

					t.hooks.AfterDocCommitted(mutation.Key, func(err error) {
						if err != nil {
							handler(err)
							return
						}
						handler(nil)
					})
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
			cb(t.createAndStashOperationFailedError(false, true, ErrAttemptExpired, ErrorReasonTransactionFailedPostCommit,
				ErrorClassFailExpiry, true))
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
			failErr = t.createAndStashOperationFailedError(false, true, err, ErrorReasonTransactionFailedPostCommit,
				ec, true)
		case ErrorClassFailHard:
			failErr = t.createAndStashOperationFailedError(false, true, err, ErrorReasonTransactionFailed,
				ec, true)
		default:
			failErr = t.createAndStashOperationFailedError(false, true, err, ErrorReasonTransactionFailedPostCommit,
				ec, true)
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

					t.hooks.AfterDocCommitted(mutation.Key, func(err error) {
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
			cb(t.createAndStashOperationFailedError(false, true, ErrAttemptExpired,
				ErrorReasonTransactionFailedPostCommit, ErrorClassFailExpiry, true))
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
			failErr = t.createAndStashOperationFailedError(false, true, err,
				ErrorReasonTransactionFailedPostCommit, ec, true)
		case ErrorClassFailHard:
			failErr = t.createAndStashOperationFailedError(false, true, err,
				ErrorReasonTransactionFailed, ec, true)
		default:
			failErr = t.createAndStashOperationFailedError(false, true, err,
				ErrorReasonTransactionFailedPostCommit, ec, true)
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

		prevErrors := t.previousErrors
		t.lock.Unlock()
		if len(prevErrors) > 0 {
			shouldRetry := true
			shouldRollback := true

			for _, err := range prevErrors {
				shouldRetry = shouldRetry && err.shouldRetry
				shouldRollback = shouldRollback && !err.shouldNotRollback
			}

			cb(&TransactionOperationFailedError{
				shouldRetry:       shouldRetry,
				shouldNotRollback: !shouldRollback,
				errorCause:        ErrPreviousOperationFailed,
				shouldRaise:       ErrorReasonTransactionFailed,
			})
			return
		}

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
			cb(t.createAndStashOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec, true))
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

func (t *transactionAttempt) setATRCommittedAmbiguityResolution(cb func(error)) {
	handler := func(st jsonAtrState, err error) {
		if err != nil {
			var failErr error
			ec := t.classifyError(err)
			switch ec {
			case ErrorClassFailExpiry:
				t.expiryOvertimeMode = true
				failErr = t.createAndStashOperationFailedError(false, true, ErrAttemptExpired, ErrorReasonTransactionCommitAmbiguous, ec, false)
			case ErrorClassFailHard:
				failErr = t.createAndStashOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec, false)
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
				failErr = t.createAndStashOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec, false)
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
			cb(t.createAndStashOperationFailedError(false, true, nil, ErrorReasonTransactionFailed, ErrorClassFailOther, false))
		case jsonAtrStateRolledBack:
			t.lock.Lock()
			t.txnAtrSection.Done()
			t.lock.Unlock()
			cb(t.createAndStashOperationFailedError(false, true, nil, ErrorReasonTransactionFailed, ErrorClassFailOther, false))
		default:
			t.lock.Lock()
			t.txnAtrSection.Done()
			t.lock.Unlock()
			cb(t.createAndStashOperationFailedError(false, true, ErrIllegalState, ErrorReasonTransactionFailed, ErrorClassFailOther, false))
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
			failErr = t.createAndStashOperationFailedError(false, false, ErrAttemptExpired, ErrorReasonTransactionExpired, ec, true)
		case ErrorClassFailAmbiguous:
			t.setATRCommittedAmbiguityResolution(cb)
			return
		case ErrorClassFailHard:
			failErr = t.createAndStashOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec, true)
		case ErrorClassFailTransient:
			failErr = t.createAndStashOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec, true)
		default:
			failErr = t.createAndStashOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec, true)
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
				cb(t.createAndStashOperationFailedError(false, true, nil, ErrorReasonTransactionFailed, ErrorClassFailOther, true))
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
