package transactions

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/pkg/errors"
)

func (t *transactionAttempt) selectAtrLocked(
	firstAgent *gocbcore.Agent,
	firstScopeName string,
	firstCollectionName string,
	firstKey []byte,
	cb func(*TransactionOperationFailedError),
) {
	atrID := int(cbcVbMap(firstKey, 1024))
	atrKey := []byte(atrIDList[atrID])

	t.hooks.RandomATRIDForVbucket(func(s string, err error) {
		if err != nil {
			cb(t.operationFailed(operationFailedDef{
				Cerr:              classifyHookError(err),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailed,
			}))
			return
		}

		if s != "" {
			atrKey = []byte(s)
		}

		atrAgent := firstAgent
		atrScopeName := "_default"
		atrCollectionName := "_default"
		if t.atrLocation.Agent != nil {
			atrAgent = t.atrLocation.Agent
			atrScopeName = t.atrLocation.ScopeName
			atrCollectionName = t.atrLocation.CollectionName
		} else {
			if t.explicitAtrs {
				cb(t.operationFailed(operationFailedDef{
					Cerr:              classifyError(errors.New("atrs must be explicitly defined")),
					ShouldNotRetry:    true,
					ShouldNotRollback: true,
					Reason:            ErrorReasonTransactionFailed,
				}))
				return
			}
		}

		t.atrAgent = atrAgent
		t.atrScopeName = atrScopeName
		t.atrCollectionName = atrCollectionName
		t.atrKey = atrKey

		cb(nil)
	})
}

func (t *transactionAttempt) setATRPendingLocked(
	cb func(*TransactionOperationFailedError),
) {
	ecCb := func(cerr *classifiedError) {
		if cerr == nil {
			cb(nil)
			return
		}

		switch cerr.Class {
		case ErrorClassFailAmbiguous:
			time.AfterFunc(3*time.Millisecond, func() {
				t.setATRPendingLocked(cb)
			})
			return
		case ErrorClassFailPathAlreadyExists:
			cb(nil)
			return
		case ErrorClassFailExpiry:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionExpired,
			}))
		case ErrorClassFailOutOfSpace:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrFull),
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailTransient:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
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
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		}
	}

	t.checkExpiredAtomic(hookATRPending, []byte{}, false, func(cerr *classifiedError) {
		if cerr != nil {
			ecCb(cerr)
			return
		}

		t.hooks.BeforeATRPending(func(err error) {
			if err != nil {
				ecCb(classifyHookError(err))
				return
			}

			deadline, duraTimeout := mutationTimeouts(t.operationTimeout, t.durabilityLevel)

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

			atrOps := []gocbcore.SubDocOp{
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
				atrFieldOp("d", durabilityLevelToShorthand(t.durabilityLevel), memd.SubdocFlagXattrPath),
			}
			if marshalErr != nil {
				ecCb(classifyError(marshalErr))
				return
			}

			_, err = t.atrAgent.MutateIn(gocbcore.MutateInOptions{
				ScopeName:              t.atrScopeName,
				CollectionName:         t.atrCollectionName,
				Key:                    t.atrKey,
				Ops:                    atrOps,
				DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
				DurabilityLevelTimeout: duraTimeout,
				Deadline:               deadline,
				Flags:                  memd.SubdocDocFlagMkDoc,
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

				t.hooks.AfterATRPending(func(err error) {
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

func (t *transactionAttempt) resolveATRCommitConflictLocked(
	ambiguityResolution bool,
	cb func(*TransactionOperationFailedError),
) {
	ecCb := func(cerr *classifiedError) {
		if cerr == nil {
			cb(nil)
			return
		}

		errorReason := ErrorReasonTransactionFailed
		if ambiguityResolution {
			errorReason = ErrorReasonTransactionCommitAmbiguous
		}

		switch cerr.Class {
		case ErrorClassFailTransient:
			time.AfterFunc(3*time.Millisecond, func() {
				t.resolveATRCommitConflictLocked(ambiguityResolution, cb)
			})
		case ErrorClassFailExpiry:
			if errorReason == ErrorReasonTransactionFailed {
				errorReason = ErrorReasonTransactionExpired
			}

			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            errorReason,
			}))
		case ErrorClassFailHard:
			if t.disableCBD3838Fix {
				errorReason = ErrorReasonTransactionFailed
			}

			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            errorReason,
			}))
		default:
			if t.disableCBD3838Fix {
				// With TXNG53, this appears to be a divergence from the spec in Java?
				t.resolveATRCommitConflictLocked(ambiguityResolution, cb)
			} else {
				cb(t.operationFailed(operationFailedDef{
					Cerr:              cerr,
					ShouldNotRetry:    false,
					ShouldNotRollback: true,
					Reason:            errorReason,
				}))
			}
		}
	}

	t.checkExpiredAtomic(hookATRCommitAmbiguityResolution, []byte{}, false, func(cerr *classifiedError) {
		if cerr != nil {
			ecCb(cerr)
			return
		}

		t.hooks.BeforeATRCommitAmbiguityResolution(func(err error) {
			if err != nil {
				ecCb(classifyHookError(err))
				return
			}

			var deadline time.Time
			if t.operationTimeout > 0 {
				deadline = time.Now().Add(t.operationTimeout)
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
					ecCb(classifyError(err))
					return
				}

				if result.Ops[0].Err != nil {
					ecCb(classifyError(err))
					return
				}

				var st jsonAtrState
				if err := json.Unmarshal(result.Ops[0].Value, &st); err != nil {
					ecCb(classifyError(err))
					return
				}

				switch st {
				case jsonAtrStatePending:
					if t.disableCBD3838Fix {
						// With TXNG53, we can get here while pending, so we need to loop
						// back around until that bug gets fixed in FIT.
						t.setATRCommittedLocked(ambiguityResolution, cb)
					} else {
						ecCb(classifyError(
							errors.Wrap(ErrIllegalState, "transaction still pending even with p set during commit")))
					}
				case jsonAtrStateCommitted:
					ecCb(nil)
				case jsonAtrStateCompleted:
					ecCb(classifyError(
						errors.Wrap(ErrIllegalState, "transaction already completed during commit")))
				case jsonAtrStateAborted:
					ecCb(classifyError(
						errors.Wrap(ErrIllegalState, "transaction already aborted during commit")))
				case jsonAtrStateRolledBack:
					ecCb(classifyError(
						errors.Wrap(ErrIllegalState, "transaction already rolled back during commit")))
				default:
					ecCb(classifyError(
						errors.Wrap(ErrIllegalState, fmt.Sprintf("illegal transaction state during commit: %s", st))))
				}
			})
			if err != nil {
				ecCb(classifyError(err))
				return
			}
		})
	})
}

func (t *transactionAttempt) setATRCommittedLocked(
	ambiguityResolution bool,
	cb func(*TransactionOperationFailedError),
) {
	ecCb := func(cerr *classifiedError) {
		if cerr == nil {
			cb(nil)
			return
		}

		errorReason := ErrorReasonTransactionFailed
		if ambiguityResolution {
			errorReason = ErrorReasonTransactionCommitAmbiguous
		}

		switch cerr.Class {
		case ErrorClassFailAmbiguous:
			if t.disableCBD3838Fix {
				ambiguityResolution = true
				t.resolveATRCommitConflictLocked(ambiguityResolution, cb)
			} else {
				ambiguityResolution = true
				t.setATRCommittedLocked(ambiguityResolution, cb)
			}
			return
		case ErrorClassFailPathAlreadyExists:
			t.resolveATRCommitConflictLocked(ambiguityResolution, cb)
			return
		case ErrorClassFailExpiry:
			if errorReason == ErrorReasonTransactionFailed {
				errorReason = ErrorReasonTransactionExpired
			}

			if t.disableCBD3838Fix {
				errorReason = ErrorReasonTransactionExpired
			}

			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            errorReason,
			}))
		case ErrorClassFailTransient:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            errorReason,
			}))
		case ErrorClassFailHard:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            errorReason,
			}))
		default:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            errorReason,
			}))
		}
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
			ecCb(classifyError(errors.Wrap(ErrIllegalState, "unexpected staged mutation type")))
			return
		}
	}

	t.checkExpiredAtomic(hookATRCommit, []byte{}, false, func(cerr *classifiedError) {
		if cerr != nil {
			ecCb(cerr)
			return
		}

		t.hooks.BeforeATRCommit(func(err error) {
			if err != nil {
				ecCb(classifyHookError(err))
				return
			}

			deadline, duraTimeout := mutationTimeouts(t.operationTimeout, t.durabilityLevel)

			var marshalErr error
			atrFieldOp := func(fieldName string, data interface{}, flags memd.SubdocFlag, op memd.SubDocOpType) gocbcore.SubDocOp {
				bytes, err := json.Marshal(data)
				if err != nil {
					marshalErr = err
				}

				return gocbcore.SubDocOp{
					Op:    op,
					Flags: flags,
					Path:  "attempts." + t.id + "." + fieldName,
					Value: bytes,
				}
			}

			atrOps := []gocbcore.SubDocOp{
				atrFieldOp("st", jsonAtrStateCommitted, memd.SubdocFlagXattrPath, memd.SubDocOpDictSet),
				atrFieldOp("tsc", "${Mutation.CAS}", memd.SubdocFlagXattrPath|memd.SubdocFlagExpandMacros, memd.SubDocOpDictSet),
				atrFieldOp("p", 0, memd.SubdocFlagXattrPath, memd.SubDocOpDictAdd),
				atrFieldOp("ins", insMutations, memd.SubdocFlagXattrPath, memd.SubDocOpDictSet),
				atrFieldOp("rep", repMutations, memd.SubdocFlagXattrPath, memd.SubDocOpDictSet),
				atrFieldOp("rem", remMutations, memd.SubdocFlagXattrPath, memd.SubDocOpDictSet),
			}
			if marshalErr != nil {
				ecCb(classifyError(marshalErr))
				return
			}

			_, err = atrAgent.MutateIn(gocbcore.MutateInOptions{
				ScopeName:              atrScopeName,
				CollectionName:         atrCollectionName,
				Key:                    atrKey,
				Ops:                    atrOps,
				DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
				DurabilityLevelTimeout: duraTimeout,
				Flags:                  memd.SubdocDocFlagNone,
				Deadline:               deadline,
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

				t.hooks.AfterATRCommit(func(err error) {
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

func (t *transactionAttempt) setATRCompletedLocked(
	cb func(*TransactionOperationFailedError),
) {
	ecCb := func(cerr *classifiedError) {
		if cerr == nil {
			cb(nil)
			return
		}

		switch cerr.Class {
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

	atrAgent := t.atrAgent
	atrScopeName := t.atrScopeName
	atrKey := t.atrKey
	atrCollectionName := t.atrCollectionName

	t.checkExpiredAtomic(hookATRComplete, []byte{}, true, func(cerr *classifiedError) {
		if cerr != nil {
			ecCb(cerr)
			return
		}

		t.hooks.BeforeATRComplete(func(err error) {
			if err != nil {
				ecCb(classifyHookError(err))
				return
			}

			deadline, duraTimeout := mutationTimeouts(t.operationTimeout, t.durabilityLevel)

			var marshalErr error
			atrFieldOp := func(fieldName string, data interface{}, flags memd.SubdocFlag) gocbcore.SubDocOp {
				bytes, err := json.Marshal(data)
				if err != nil {
					marshalErr = err
				}

				return gocbcore.SubDocOp{
					Op:    memd.SubDocOpDictSet,
					Flags: flags,
					Path:  "attempts." + t.id + "." + fieldName,
					Value: bytes,
				}
			}

			atrOps := []gocbcore.SubDocOp{
				atrFieldOp("st", jsonAtrStateCompleted, memd.SubdocFlagXattrPath),
				atrFieldOp("tsco", "${Mutation.CAS}", memd.SubdocFlagXattrPath|memd.SubdocFlagExpandMacros),
			}
			if marshalErr != nil {
				ecCb(classifyError(marshalErr))
				return
			}

			_, err = atrAgent.MutateIn(gocbcore.MutateInOptions{
				ScopeName:              atrScopeName,
				CollectionName:         atrCollectionName,
				Key:                    atrKey,
				Ops:                    atrOps,
				DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
				DurabilityLevelTimeout: duraTimeout,
				Deadline:               deadline,
				Flags:                  memd.SubdocDocFlagNone,
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

				t.hooks.AfterATRComplete(func(err error) {
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

func (t *transactionAttempt) resolveATRAbortConflictLocked(
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
					errors.Wrap(ErrAttemptExpired, "atr abort ambiguity resolution failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionExpired,
			}))
			return
		}

		switch cerr.Class {
		case ErrorClassFailTransient:
			time.AfterFunc(3*time.Millisecond, func() {
				t.resolveATRAbortConflictLocked(cb)
			})
		case ErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()
			time.AfterFunc(3*time.Millisecond, func() {
				t.resolveATRAbortConflictLocked(cb)
			})
		case ErrorClassFailPathNotFound:
			fallthrough
		case ErrorClassFailDocNotFound:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrNotFound),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailOutOfSpace:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrFull),
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
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		}
	}

	t.checkExpiredAtomic(hookATRAbort, []byte{}, true, func(cerr *classifiedError) {
		if cerr != nil {
			ecCb(cerr)
			return
		}

		var deadline time.Time
		if t.operationTimeout > 0 {
			deadline = time.Now().Add(t.operationTimeout)
		}

		_, err := t.atrAgent.LookupIn(gocbcore.LookupInOptions{
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
				ecCb(classifyError(err))
				return
			}

			if result.Ops[0].Err != nil {
				ecCb(classifyError(err))
				return
			}

			var st jsonAtrState
			if err := json.Unmarshal(result.Ops[0].Value, &st); err != nil {
				ecCb(classifyError(err))
				return
			}

			switch st {
			case jsonAtrStateCommitted:
				ecCb(classifyError(
					errors.Wrap(ErrIllegalState, "transaction became committed during abort")))
			case jsonAtrStatePending:
				ecCb(classifyError(
					errors.Wrap(ErrIllegalState, "transaction still pending even with p set during abort")))
			case jsonAtrStateAborted:
				ecCb(nil)
			case jsonAtrStateRolledBack:
				ecCb(classifyError(
					errors.Wrap(ErrIllegalState, "transaction already rolled back during abort")))
			default:
				ecCb(classifyError(
					errors.Wrap(ErrIllegalState, fmt.Sprintf("illegal transaction state during abort: %s", st))))
			}
		})
		if err != nil {
			ecCb(classifyError(err))
			return
		}
	})
}

func (t *transactionAttempt) setATRAbortedLocked(
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
					errors.Wrap(ErrAttemptExpired, "atr abort failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionExpired,
			}))
			return
		}

		switch cerr.Class {
		case ErrorClassFailAmbiguous:
			time.AfterFunc(3*time.Millisecond, func() {
				t.setATRAbortedLocked(cb)
			})
		case ErrorClassFailPathAlreadyExists:
			time.AfterFunc(3*time.Millisecond, func() {
				t.resolveATRAbortConflictLocked(cb)
			})
			return
		case ErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()
			time.AfterFunc(3*time.Millisecond, func() {
				t.setATRAbortedLocked(cb)
			})
		case ErrorClassFailDocNotFound:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrNotFound),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailPathNotFound:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrEntryNotFound),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailOutOfSpace:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrFull),
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
				t.setATRAbortedLocked(cb)
			})
		}
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
			ecCb(classifyError(errors.Wrap(ErrIllegalState, "unexpected staged mutation type")))
			return
		}
	}

	t.checkExpiredAtomic(hookATRAbort, []byte{}, true, func(cerr *classifiedError) {
		if cerr != nil {
			ecCb(cerr)
			return
		}

		t.hooks.BeforeATRAborted(func(err error) {
			if err != nil {
				ecCb(classifyHookError(err))
				return
			}

			deadline, duraTimeout := mutationTimeouts(t.operationTimeout, t.durabilityLevel)

			var marshalErr error
			atrFieldOp := func(fieldName string, data interface{}, flags memd.SubdocFlag) gocbcore.SubDocOp {
				bytes, err := json.Marshal(data)
				if err != nil {
					marshalErr = err
				}

				return gocbcore.SubDocOp{
					Op:    memd.SubDocOpDictSet,
					Flags: flags,
					Path:  "attempts." + t.id + "." + fieldName,
					Value: bytes,
				}
			}

			atrOps := []gocbcore.SubDocOp{
				atrFieldOp("st", jsonAtrStateAborted, memd.SubdocFlagXattrPath),
				atrFieldOp("tsrs", "${Mutation.CAS}", memd.SubdocFlagXattrPath|memd.SubdocFlagExpandMacros),
				atrFieldOp("p", 0, memd.SubdocFlagXattrPath),
				atrFieldOp("ins", insMutations, memd.SubdocFlagXattrPath),
				atrFieldOp("rep", repMutations, memd.SubdocFlagXattrPath),
				atrFieldOp("rem", remMutations, memd.SubdocFlagXattrPath),
			}
			if marshalErr != nil {
				ecCb(classifyError(marshalErr))
				return
			}

			_, err = atrAgent.MutateIn(gocbcore.MutateInOptions{
				ScopeName:              atrScopeName,
				CollectionName:         atrCollectionName,
				Key:                    atrKey,
				Ops:                    atrOps,
				DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
				DurabilityLevelTimeout: duraTimeout,
				Flags:                  memd.SubdocDocFlagNone,
				Deadline:               deadline,
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

				t.hooks.AfterATRAborted(func(err error) {
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

func (t *transactionAttempt) setATRRolledBackLocked(
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
					errors.Wrap(ErrAttemptExpired, "atr rolledback failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionExpired,
			}))
			return
		}

		switch cerr.Class {
		case ErrorClassFailPathNotFound:
			cb(nil)
			return
		case ErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()
			time.AfterFunc(3*time.Millisecond, func() {
				t.setATRRolledBackLocked(cb)
			})
		case ErrorClassFailDocNotFound:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrNotFound),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailOutOfSpace:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrFull),
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
				t.setATRRolledBackLocked(cb)
			})
		}
	}

	atrAgent := t.atrAgent
	atrScopeName := t.atrScopeName
	atrKey := t.atrKey
	atrCollectionName := t.atrCollectionName

	t.checkExpiredAtomic(hookATRRollback, []byte{}, true, func(cerr *classifiedError) {
		if cerr != nil {
			ecCb(cerr)
			return
		}

		t.hooks.BeforeATRRolledBack(func(err error) {
			if err != nil {
				ecCb(classifyHookError(err))
				return
			}

			deadline, duraTimeout := mutationTimeouts(t.operationTimeout, t.durabilityLevel)

			var marshalErr error
			atrFieldOp := func(fieldName string, data interface{}, flags memd.SubdocFlag) gocbcore.SubDocOp {
				bytes, err := json.Marshal(data)
				if err != nil {
					marshalErr = err
				}

				return gocbcore.SubDocOp{
					Op:    memd.SubDocOpDictSet,
					Flags: flags,
					Path:  "attempts." + t.id + "." + fieldName,
					Value: bytes,
				}
			}

			atrOps := []gocbcore.SubDocOp{
				atrFieldOp("st", jsonAtrStateRolledBack, memd.SubdocFlagXattrPath),
				atrFieldOp("tsrc", "${Mutation.CAS}", memd.SubdocFlagXattrPath|memd.SubdocFlagExpandMacros),
			}
			if marshalErr != nil {
				ecCb(classifyError(marshalErr))
				return
			}

			_, err = atrAgent.MutateIn(gocbcore.MutateInOptions{
				ScopeName:              atrScopeName,
				CollectionName:         atrCollectionName,
				Key:                    atrKey,
				Ops:                    atrOps,
				DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
				DurabilityLevelTimeout: duraTimeout,
				Deadline:               deadline,
				Flags:                  memd.SubdocDocFlagNone,
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

				t.hooks.AfterATRRolledBack(func(err error) {
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
