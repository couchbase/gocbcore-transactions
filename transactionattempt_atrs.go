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
	"encoding/json"
	"fmt"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/pkg/errors"
)

func (t *transactionAttempt) selectAtrLocked(
	firstAgent *gocbcore.Agent,
	firstOboUser string,
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
		atrOboUser := firstOboUser
		atrScopeName := "_default"
		atrCollectionName := "_default"
		if t.atrLocation.Agent != nil {
			atrAgent = t.atrLocation.Agent
			atrOboUser = t.atrLocation.OboUser
			atrScopeName = t.atrLocation.ScopeName
			atrCollectionName = t.atrLocation.CollectionName
		} else {
			if t.enableExplicitATRs {
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
		t.atrOboUser = atrOboUser
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

			deadline, duraTimeout := mutationTimeouts(t.keyValueTimeout, t.durabilityLevel)

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
				User:                   t.atrOboUser,
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

func (t *transactionAttempt) fetchATRCommitConflictLocked(
	cb func(jsonAtrState, *TransactionOperationFailedError),
) {
	ecCb := func(st jsonAtrState, cerr *classifiedError) {
		if cerr == nil {
			cb(st, nil)
			return
		}

		switch cerr.Class {
		case ErrorClassFailTransient:
			fallthrough
		case ErrorClassFailOther:
			time.AfterFunc(3*time.Millisecond, func() {
				t.fetchATRCommitConflictLocked(cb)
			})
			return
		case ErrorClassFailDocNotFound:
			cb(jsonAtrStateUnknown, t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrNotFound),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionCommitAmbiguous,
			}))
		case ErrorClassFailPathNotFound:
			cb(jsonAtrStateUnknown, t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrEntryNotFound),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionCommitAmbiguous,
			}))
		case ErrorClassFailExpiry:
			cb(jsonAtrStateUnknown, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionCommitAmbiguous,
			}))
		case ErrorClassFailHard:
			cb(jsonAtrStateUnknown, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionCommitAmbiguous,
			}))
		default:
			cb(jsonAtrStateUnknown, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionCommitAmbiguous,
			}))
			return
		}
	}

	t.checkExpiredAtomic(hookATRCommitAmbiguityResolution, []byte{}, false, func(cerr *classifiedError) {
		if cerr != nil {
			ecCb(jsonAtrStateUnknown, cerr)
			return
		}

		t.hooks.BeforeATRCommitAmbiguityResolution(func(err error) {
			if err != nil {
				ecCb(jsonAtrStateUnknown, classifyHookError(err))
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
				User:     t.atrOboUser,
			}, func(result *gocbcore.LookupInResult, err error) {
				if err != nil {
					ecCb(jsonAtrStateUnknown, classifyError(err))
					return
				}

				if result.Ops[0].Err != nil {
					ecCb(jsonAtrStateUnknown, classifyError(err))
					return
				}

				var st jsonAtrState
				if err := json.Unmarshal(result.Ops[0].Value, &st); err != nil {
					ecCb(jsonAtrStateUnknown, classifyError(err))
					return
				}

				ecCb(st, nil)
			})
			if err != nil {
				ecCb(jsonAtrStateUnknown, classifyError(err))
				return
			}
		})
	})
}

func (t *transactionAttempt) resolveATRCommitConflictLocked(
	cb func(*TransactionOperationFailedError),
) {
	t.fetchATRCommitConflictLocked(func(st jsonAtrState, err *TransactionOperationFailedError) {
		if err != nil {
			cb(err)
			return
		}

		switch st {
		case jsonAtrStatePending:
			cb(t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					errors.Wrap(ErrIllegalState, "transaction still pending even with p set during commit")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case jsonAtrStateCommitted:
			cb(nil)
		case jsonAtrStateCompleted:
			cb(t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					errors.Wrap(ErrIllegalState, "transaction already completed during commit")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case jsonAtrStateAborted:
			cb(t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					errors.Wrap(ErrIllegalState, "transaction already aborted during commit")),
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case jsonAtrStateRolledBack:
			cb(t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					errors.Wrap(ErrIllegalState, "transaction already rolled back during commit")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailed,
			}))
		default:
			cb(t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					errors.Wrap(ErrIllegalState, fmt.Sprintf("illegal transaction state during commit: %s", st))),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailed,
			}))
		}
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
			time.AfterFunc(3*time.Millisecond, func() {
				ambiguityResolution = true
				t.setATRCommittedLocked(ambiguityResolution, cb)
			})
			return
		case ErrorClassFailTransient:
			if ambiguityResolution {
				time.AfterFunc(3*time.Millisecond, func() {
					t.setATRCommittedLocked(ambiguityResolution, cb)
				})
				return
			}

			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            errorReason,
			}))
		case ErrorClassFailPathAlreadyExists:
			t.resolveATRCommitConflictLocked(cb)
			return
		case ErrorClassFailDocNotFound:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrNotFound),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            errorReason,
			}))
		case ErrorClassFailPathNotFound:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrEntryNotFound),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            errorReason,
			}))
		case ErrorClassFailOutOfSpace:
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrFull),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            errorReason,
			}))
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
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            errorReason,
			}))
		default:
			if ambiguityResolution {
				cb(t.operationFailed(operationFailedDef{
					Cerr:              cerr,
					ShouldNotRetry:    true,
					ShouldNotRollback: true,
					Reason:            errorReason,
				}))
				return
			}

			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            errorReason,
			}))
		}
	}

	atrAgent := t.atrAgent
	atrOboUser := t.atrOboUser
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

			deadline, duraTimeout := mutationTimeouts(t.keyValueTimeout, t.durabilityLevel)

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
				User:                   atrOboUser,
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

		if t.isExpiryOvertimeAtomic() {
			cb(t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					errors.Wrap(ErrAttemptExpired, "completed atr removal failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailedPostCommit,
			}))
			return
		}

		switch cerr.Class {
		case ErrorClassFailDocNotFound:
			fallthrough
		case ErrorClassFailPathNotFound:
			// This is technically a full success, but FIT expects unstagingCompleted=false...
			cb(t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailedPostCommit,
			}))
		case ErrorClassFailExpiry:
			cb(t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					errors.Wrap(ErrAttemptExpired, "completed atr removal operation expired")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailedPostCommit,
			}))
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
	atrOboUser := t.atrOboUser
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

			deadline, duraTimeout := mutationTimeouts(t.keyValueTimeout, t.durabilityLevel)

			atrOps := []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpDelete,
					Flags: memd.SubdocFlagXattrPath,
					Path:  "attempts." + t.id,
				},
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
				User:                   atrOboUser,
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
	atrOboUser := t.atrOboUser
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

			deadline, duraTimeout := mutationTimeouts(t.keyValueTimeout, t.durabilityLevel)

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
				User:                   atrOboUser,
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
					errors.Wrap(ErrAttemptExpired, "rolled back atr removal failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionExpired,
			}))
			return
		}

		switch cerr.Class {
		case ErrorClassFailDocNotFound:
			fallthrough
		case ErrorClassFailPathNotFound:
			cb(nil)
			return
		case ErrorClassFailExpiry:
			cb(t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					errors.Wrap(ErrAttemptExpired, "rolled back atr removal operation expired")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionExpired,
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
	atrOboUser := t.atrOboUser
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

			deadline, duraTimeout := mutationTimeouts(t.keyValueTimeout, t.durabilityLevel)

			atrOps := []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpDelete,
					Flags: memd.SubdocFlagXattrPath,
					Path:  "attempts." + t.id,
				},
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
				User:                   atrOboUser,
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
