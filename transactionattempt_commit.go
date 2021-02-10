package transactions

import (
	"encoding/json"
	"log"
	"time"

	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/pkg/errors"
)

func (t *transactionAttempt) Commit(cb CommitCallback) error {
	return t.commit(func(err *TransactionOperationFailedError) {
		if err != nil {
			if t.ShouldRollback() {
				if !t.isExpiryOvertimeAtomic() {
					t.applyStateBits(transactionStateBitPreExpiryAutoRollback)
				}

				t.rollback(func(rerr *TransactionOperationFailedError) {
					if rerr != nil {
						log.Printf("implicit rollback after commit failure errored: %s", rerr)
					}

					t.ensureCleanUpRequest()
					cb(err)
				})
				return
			}

			t.ensureCleanUpRequest()
			cb(err)
			return
		}

		t.applyStateBits(transactionStateBitShouldNotRetry | transactionStateBitShouldNotRollback)
		t.ensureCleanUpRequest()
		cb(nil)
	})
}

func (t *transactionAttempt) commit(
	cb func(err *TransactionOperationFailedError),
) error {
	t.waitForOpsAndLock(func(unlock func()) {
		unlockAndCb := func(err *TransactionOperationFailedError) {
			unlock()
			cb(err)
		}

		err := t.checkCanCommitLocked()
		if err != nil {
			unlockAndCb(err)
			return
		}

		t.applyStateBits(transactionStateBitShouldNotCommit)

		if t.state == AttemptStateNothingWritten {
			//t.state = AttemptStateCompleted
			unlockAndCb(nil)
			return
		}

		t.checkExpiredAtomic(hookCommit, []byte{}, false, func(cerr *classifiedError) {
			if cerr != nil {
				unlockAndCb(t.operationFailed(operationFailedDef{
					Cerr:              cerr,
					ShouldNotRetry:    true,
					ShouldNotRollback: false,
					Reason:            ErrorReasonTransactionExpired,
				}))
				return
			}

			t.state = AttemptStateCommitting

			t.setATRCommittedLocked(false, func(err *TransactionOperationFailedError) {
				if err != nil {
					if err.shouldRaise == ErrorReasonTransactionFailedPostCommit {
						t.state = AttemptStateCommitted
					} else if err.shouldRaise != ErrorReasonTransactionCommitAmbiguous {
						t.state = AttemptStatePending
					}

					unlockAndCb(err)
					return
				}

				t.state = AttemptStateCommitted

				go func() {
					commitStagedMutation := func(
						mutation *stagedMutation,
						unstageCb func(*TransactionOperationFailedError),
					) {
						t.ensureMutation(mutation, func(err *TransactionOperationFailedError) {
							if err != nil {
								unstageCb(err)
								return
							}

							switch mutation.OpType {
							case StagedMutationInsert:
								t.commitStagedInsert(*mutation, false, unstageCb)
							case StagedMutationReplace:
								t.commitStagedReplace(*mutation, false, false, unstageCb)
							case StagedMutationRemove:
								t.commitStagedRemove(*mutation, false, unstageCb)
							default:
								unstageCb(t.operationFailed(operationFailedDef{
									Cerr: classifyError(
										errors.Wrap(ErrIllegalState, "unexpected staged mutation type")),
									ShouldNotRetry:    true,
									ShouldNotRollback: true,
									Reason:            ErrorReasonTransactionFailedPostCommit,
								}))
							}
						})
					}

					var mutErrs []*TransactionOperationFailedError
					if !t.enableParallelUnstaging {
						for _, mutation := range t.stagedMutations {
							waitCh := make(chan struct{}, 1)

							commitStagedMutation(mutation, func(err *TransactionOperationFailedError) {
								if err != nil {
									mutErrs = append(mutErrs, err)
									waitCh <- struct{}{}
									return
								}

								waitCh <- struct{}{}
							})

							<-waitCh
							if len(mutErrs) > 0 {
								break
							}
						}
					} else {
						type mutResult struct {
							Err *TransactionOperationFailedError
						}

						numMutations := len(t.stagedMutations)
						waitCh := make(chan mutResult, numMutations)

						// Unlike the RFC we do insert and replace separately. We have a bug in gocbcore where subdocs
						// will raise doc exists rather than a cas mismatch so we need to do these ops separately to tell
						// how to handle that error.
						for _, mutation := range t.stagedMutations {
							commitStagedMutation(mutation, func(err *TransactionOperationFailedError) {
								waitCh <- mutResult{
									Err: err,
								}
							})
						}

						for i := 0; i < numMutations; i++ {
							res := <-waitCh

							if res.Err != nil {
								mutErrs = append(mutErrs, res.Err)
								continue
							}
						}
					}
					err = mergeOperationFailedErrors(mutErrs)
					if err != nil {
						unlockAndCb(err)
						return
					}

					t.setATRCompletedLocked(func(err *TransactionOperationFailedError) {
						if err != nil {
							if err.errorClass != ErrorClassFailHard {
								unlockAndCb(nil)
								return
							}

							unlockAndCb(err)
							return
						}

						t.state = AttemptStateCompleted

						unlockAndCb(nil)
					})
				}()
			})
		})
	})

	return nil
}

func (t *transactionAttempt) ensureMutation(
	mutation *stagedMutation,
	cb func(*TransactionOperationFailedError),
) {
	ecCb := func(cerr *classifiedError) {
		if cerr == nil {
			cb(nil)
			return
		}

		cb(t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				errors.New("failed to fetch staged data to commit")),
			ShouldNotRetry:    false,
			ShouldNotRollback: false,
			Reason:            ErrorReasonTransactionFailed,
		}))
	}

	if mutation.OpType != StagedMutationInsert && mutation.OpType != StagedMutationReplace {
		ecCb(nil)
		return
	}

	if mutation.Staged != nil {
		ecCb(nil)
		return
	}

	_, err := mutation.Agent.LookupIn(gocbcore.LookupInOptions{
		ScopeName:      mutation.ScopeName,
		CollectionName: mutation.CollectionName,
		Key:            mutation.Key,
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpGet,
				Path:  "txn.op.stgd",
				Flags: memd.SubdocFlagXattrPath,
			},
		},
		Deadline: t.expiryTime,
		Flags:    memd.SubdocDocFlagAccessDeleted,
	}, func(result *gocbcore.LookupInResult, err error) {
		if err != nil {
			ecCb(classifyError(err))
			return
		}

		if result.Cas != mutation.Cas {
			// Something changed the document, we leave the staged data blank
			// knowing that the operation against it will fail anyways.  We do
			// need to check this first, so we don't accidentally include path
			// errors that occurred below DUE to the CAS change.
			ecCb(nil)
			return
		}

		if result.Ops[0].Err != nil {
			ecCb(classifyError(result.Ops[0].Err))
			return
		}

		mutation.Staged = json.RawMessage(result.Ops[0].Value)
		ecCb(nil)
	})
	if err != nil {
		ecCb(classifyError(err))
		return
	}
}

func (t *transactionAttempt) commitStagedReplace(
	mutation stagedMutation,
	forceWrite bool,
	ambiguityResolution bool,
	cb func(*TransactionOperationFailedError),
) {
	ecCb := func(cerr *classifiedError) {
		if cerr == nil {
			cb(nil)
			return
		}

		if t.isExpiryOvertimeAtomic() {
			cb(t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					errors.Wrap(ErrAttemptExpired, "committing a replace failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailedPostCommit,
			}))
			return
		}

		switch cerr.Class {
		case ErrorClassFailAmbiguous:
			time.AfterFunc(3*time.Millisecond, func() {
				ambiguityResolution = true
				t.commitStagedReplace(mutation, forceWrite, ambiguityResolution, cb)
			})
		case ErrorClassFailDocAlreadyExists:
			cerr.Class = ErrorClassFailCasMismatch
			fallthrough
		case ErrorClassFailCasMismatch:
			if !ambiguityResolution {
				time.AfterFunc(3*time.Millisecond, func() {
					forceWrite = true
					t.commitStagedReplace(mutation, forceWrite, ambiguityResolution, cb)
				})
				return
			}

			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailedPostCommit,
			}))
		case ErrorClassFailDocNotFound:
			t.commitStagedInsert(mutation, ambiguityResolution, cb)
			return
		case ErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()
			time.AfterFunc(3*time.Millisecond, func() {
				t.commitStagedReplace(mutation, forceWrite, ambiguityResolution, cb)
			})
		case ErrorClassFailHard:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailedPostCommit,
			}))
		default:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailedPostCommit,
			}))
		}
	}

	t.checkExpiredAtomic(hookCommitDoc, mutation.Key, false, func(cerr *classifiedError) {
		if cerr != nil {
			t.setExpiryOvertimeAtomic()
		}

		t.hooks.BeforeDocCommitted(mutation.Key, func(err error) {
			if err != nil {
				ecCb(classifyHookError(err))
				return
			}

			deadline, duraTimeout := mutationTimeouts(t.keyValueTimeout, t.durabilityLevel)

			cas := mutation.Cas
			if forceWrite {
				cas = 0
			}

			stagedData := mutation.Staged
			if stagedData == nil {
				// If there is no staged data, it probably means we are about to CAS conflict
				// and fail, but we need to keep the data valid for the call to CAS fail.
				stagedData = []byte{0}
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
						Value: stagedData,
					},
				},
				Deadline:               deadline,
				DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
				DurabilityLevelTimeout: duraTimeout,
			}, func(result *gocbcore.MutateInResult, err error) {
				if err != nil {
					ecCb(classifyError(err))
					return
				}

				for _, op := range result.Ops {
					if op.Err != nil {
						ecCb(classifyError(op.Err))
						return
					}
				}

				t.hooks.AfterDocCommittedBeforeSavingCAS(mutation.Key, func(err error) {
					if err != nil {
						ecCb(classifyHookError(err))
						return
					}

					t.hooks.AfterDocCommitted(mutation.Key, func(err error) {
						if err != nil {
							ecCb(classifyHookError(err))
							return
						}

						ecCb(nil)
					})
				})
			})
			if err != nil {
				ecCb(classifyError(err))
				return
			}
		})
	})
}

func (t *transactionAttempt) commitStagedInsert(
	mutation stagedMutation,
	ambiguityResolution bool,
	cb func(*TransactionOperationFailedError),
) {
	ecCb := func(cerr *classifiedError) {
		if cerr == nil {
			cb(nil)
			return
		}

		if t.isExpiryOvertimeAtomic() {
			cb(t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					errors.Wrap(ErrAttemptExpired, "committing an insert failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailedPostCommit,
			}))
			return
		}

		switch cerr.Class {
		case ErrorClassFailAmbiguous:
			time.AfterFunc(3*time.Millisecond, func() {
				ambiguityResolution = true
				t.commitStagedInsert(mutation, ambiguityResolution, cb)
			})
		case ErrorClassFailDocAlreadyExists:
			cerr.Class = ErrorClassFailCasMismatch
			fallthrough
		case ErrorClassFailCasMismatch:
			if !ambiguityResolution {
				time.AfterFunc(3*time.Millisecond, func() {
					t.commitStagedReplace(mutation, true, ambiguityResolution, cb)
				})
				return
			}

			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailedPostCommit,
			}))
		case ErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()
			time.AfterFunc(3*time.Millisecond, func() {
				t.commitStagedInsert(mutation, ambiguityResolution, cb)
			})
		case ErrorClassFailHard:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailedPostCommit,
			}))
		default:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailedPostCommit,
			}))
		}
	}

	t.checkExpiredAtomic(hookCommitDoc, mutation.Key, false, func(cerr *classifiedError) {
		if cerr != nil {
			t.setExpiryOvertimeAtomic()
		}

		t.hooks.BeforeDocCommitted(mutation.Key, func(err error) {
			if err != nil {
				ecCb(classifyHookError(err))
				return
			}

			deadline, duraTimeout := mutationTimeouts(t.keyValueTimeout, t.durabilityLevel)

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
					ecCb(classifyError(err))
					return
				}

				t.hooks.AfterDocCommittedBeforeSavingCAS(mutation.Key, func(err error) {
					if err != nil {
						ecCb(classifyHookError(err))
						return
					}

					t.hooks.AfterDocCommitted(mutation.Key, func(err error) {
						if err != nil {
							ecCb(classifyHookError(err))
							return
						}

						ecCb(nil)
					})
				})
			})
			if err != nil {
				ecCb(classifyError(err))
				return
			}
		})
	})
}

func (t *transactionAttempt) commitStagedRemove(
	mutation stagedMutation,
	ambiguityResolution bool,
	cb func(*TransactionOperationFailedError),
) {
	ecCb := func(cerr *classifiedError) {
		if cerr == nil {
			cb(nil)
			return
		}

		if t.isExpiryOvertimeAtomic() {
			cb(t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					errors.Wrap(ErrAttemptExpired, "committing a remove failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailedPostCommit,
			}))
			return
		}

		switch cerr.Class {
		case ErrorClassFailAmbiguous:
			time.AfterFunc(3*time.Millisecond, func() {
				ambiguityResolution = true
				t.commitStagedRemove(mutation, ambiguityResolution, cb)
			})
			return
		case ErrorClassFailDocNotFound:
			// Not finding the document during ambiguity resolution likely indicates
			// that it simply successfully performed the operation already. However, the mutation
			// token of that won't be available, so we need to just error it anyways :(
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailedPostCommit,
			}))
		case ErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()
			time.AfterFunc(3*time.Millisecond, func() {
				t.commitStagedRemove(mutation, ambiguityResolution, cb)
			})
		case ErrorClassFailHard:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailedPostCommit,
			}))
		default:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailedPostCommit,
			}))
		}
	}

	t.checkExpiredAtomic(hookCommitDoc, mutation.Key, false, func(cerr *classifiedError) {
		if cerr != nil {
			t.setExpiryOvertimeAtomic()
		}

		t.hooks.BeforeDocRemoved(mutation.Key, func(err error) {
			if err != nil {
				ecCb(classifyHookError(err))
				return
			}

			deadline, duraTimeout := mutationTimeouts(t.keyValueTimeout, t.durabilityLevel)

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
					ecCb(classifyError(err))
					return
				}

				t.hooks.AfterDocRemovedPreRetry(mutation.Key, func(err error) {
					if err != nil {
						ecCb(classifyHookError(err))
						return
					}

					t.hooks.AfterDocRemovedPostRetry(mutation.Key, func(err error) {
						if err != nil {
							ecCb(classifyHookError(err))
							return
						}

						ecCb(nil)
					})
				})
			})
			if err != nil {
				ecCb(classifyError(err))
				return
			}
		})
	})
}
