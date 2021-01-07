package transactions

import (
	"time"

	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/pkg/errors"
)

func (t *transactionAttempt) Rollback(cb RollbackCallback) error {
	return t.rollback(func(err *TransactionOperationFailedError) {
		if err != nil {
			t.ensureCleanUpRequest()
			cb(err)
			return
		}

		t.ensureCleanUpRequest()
		cb(nil)
	})
}

func (t *transactionAttempt) rollback(
	cb func(*TransactionOperationFailedError),
) error {
	t.waitForOpsAndLock(func(unlock func()) {
		unlockAndCb := func(err *TransactionOperationFailedError) {
			unlock()
			cb(err)
		}

		err := t.checkCanRollbackLocked()
		if err != nil {
			unlockAndCb(err)
			return
		}

		t.applyStateBits(transactionStateBitShouldNotCommit | transactionStateBitShouldNotRollback)

		if t.state == AttemptStateNothingWritten {
			//t.state = AttemptStateRolledBack
			unlockAndCb(nil)
			return
		}

		t.checkExpiredAtomic(hookRollback, []byte{}, true, func(cerr *classifiedError) {
			if cerr != nil {
				t.setExpiryOvertimeAtomic()
			}

			t.setATRAbortedLocked(func(err *TransactionOperationFailedError) {
				if err != nil {
					unlockAndCb(err)
					return
				}

				t.state = AttemptStateAborted

				go func() {
					removeStagedMutation := func(
						mutation *stagedMutation,
						unstageCb func(*TransactionOperationFailedError),
					) {
						switch mutation.OpType {
						case StagedMutationInsert:
							t.removeStagedInsert(*mutation, unstageCb)
						case StagedMutationReplace:
							fallthrough
						case StagedMutationRemove:
							t.removeStagedRemoveReplace(*mutation, unstageCb)
						default:
							unstageCb(t.operationFailed(operationFailedDef{
								Cerr: &classifiedError{
									Source: errors.New("unexpected staged operation type"),
									Class:  ErrorClassFailOther,
								},
								ShouldNotRetry:    true,
								ShouldNotRollback: true,
								Reason:            ErrorReasonTransactionFailed,
							}))
						}
					}

					var mutErrs []*TransactionOperationFailedError
					if t.serialUnstaging {
						for _, mutation := range t.stagedMutations {
							waitCh := make(chan struct{}, 1)

							removeStagedMutation(mutation, func(err *TransactionOperationFailedError) {
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
							removeStagedMutation(mutation, func(err *TransactionOperationFailedError) {
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

					t.setATRRolledBackLocked(func(err *TransactionOperationFailedError) {
						if err != nil {
							unlockAndCb(err)
							return
						}

						t.state = AttemptStateRolledBack

						unlockAndCb(nil)
					})
				}()
			})
		})
	})

	return nil
}

func (t *transactionAttempt) removeStagedInsert(
	mutation stagedMutation,
	cb func(*TransactionOperationFailedError),
) {
	ecCb := func(cerr *classifiedError) {
		if cerr == nil {
			cb(nil)
			return
		}

		if t.isExpiryOvertimeAtomic() {
			cb(t.operationFailed(operationFailedDef{
				Cerr: &classifiedError{
					Source: errors.Wrap(ErrAttemptExpired, "removing a staged insert failed during overtime"),
					Class:  ErrorClassFailExpiry,
				},
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionExpired,
			}))
			return
		}

		switch cerr.Class {
		case ErrorClassFailAmbiguous:
			time.AfterFunc(3*time.Millisecond, func() {
				t.removeStagedInsert(mutation, cb)
			})
		case ErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()
			time.AfterFunc(3*time.Millisecond, func() {
				t.removeStagedInsert(mutation, cb)
			})
		case ErrorClassFailDocNotFound:
			cb(nil)
			return
		case ErrorClassFailPathNotFound:
			cb(nil)
			return
		case ErrorClassFailCasMismatch:
			fallthrough
		case ErrorClassFailDocAlreadyExists:
			cb(t.operationFailed(operationFailedDef{
				Cerr: &classifiedError{
					Source: cerr.Source,
					Class:  ErrorClassFailCasMismatch,
				},
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailHard:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailed,
			}))
		default:
			time.AfterFunc(3*time.Millisecond, func() {
				t.removeStagedInsert(mutation, cb)
			})
		}
	}

	t.checkExpiredAtomic(hookDeleteInserted, mutation.Key, true, func(cerr *classifiedError) {
		if cerr != nil {
			ecCb(cerr)
			return
		}

		t.hooks.BeforeRollbackDeleteInserted(mutation.Key, func(err error) {
			if err != nil {
				ecCb(classifyHookError(err))
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
					ecCb(classifyError(err))
					return
				}

				for _, op := range result.Ops {
					if op.Err != nil {
						ecCb(classifyError(op.Err))
						return
					}
				}

				t.hooks.AfterRollbackDeleteInserted(mutation.Key, func(err error) {
					if err != nil {
						ecCb(classifyHookError(err))
						return
					}

					ecCb(nil)
				})
			})
			if err != nil {
				ecCb(classifyError(err))
				return
			}
		})
	})
}

func (t *transactionAttempt) removeStagedRemoveReplace(
	mutation stagedMutation,
	cb func(*TransactionOperationFailedError),
) {
	ecCb := func(cerr *classifiedError) {
		if cerr == nil {
			cb(nil)
			return
		}

		if t.isExpiryOvertimeAtomic() {
			cb(t.operationFailed(operationFailedDef{
				Cerr: &classifiedError{
					Source: errors.Wrap(ErrAttemptExpired, "removing a staged remove or replace failed during overtime"),
					Class:  ErrorClassFailExpiry,
				},
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionExpired,
			}))
			return
		}

		switch cerr.Class {
		case ErrorClassFailAmbiguous:
			time.AfterFunc(3*time.Millisecond, func() {
				t.removeStagedRemoveReplace(mutation, cb)
			})
		case ErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()
			time.AfterFunc(3*time.Millisecond, func() {
				t.removeStagedRemoveReplace(mutation, cb)
			})
		case ErrorClassFailPathNotFound:
			cb(nil)
			return
		case ErrorClassFailDocNotFound:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailCasMismatch:
			fallthrough
		case ErrorClassFailDocAlreadyExists:
			cb(t.operationFailed(operationFailedDef{
				Cerr: &classifiedError{
					Source: cerr.Source,
					Class:  ErrorClassFailCasMismatch,
				},
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailHard:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailed,
			}))
		default:
			time.AfterFunc(3*time.Millisecond, func() {
				t.removeStagedRemoveReplace(mutation, cb)
			})
		}
	}

	t.checkExpiredAtomic(hookRollbackDoc, mutation.Key, true, func(cerr *classifiedError) {
		if cerr != nil {
			ecCb(cerr)
			return
		}

		t.hooks.BeforeDocRolledBack(mutation.Key, func(err error) {
			if err != nil {
				ecCb(classifyHookError(err))
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
					ecCb(classifyError(err))
					return
				}

				for _, op := range result.Ops {
					if op.Err != nil {
						ecCb(classifyError(op.Err))
						return
					}
				}

				t.hooks.AfterRollbackReplaceOrRemove(mutation.Key, func(err error) {
					if err != nil {
						ecCb(classifyHookError(err))
						return
					}

					ecCb(nil)
				})
			})
			if err != nil {
				ecCb(classifyError(err))
				return
			}
		})
	})
}
