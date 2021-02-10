package transactions

import (
	"encoding/json"

	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/pkg/errors"
)

func (t *transactionAttempt) Remove(opts RemoveOptions, cb StoreCallback) error {
	return t.remove(opts, func(res *GetResult, err *TransactionOperationFailedError) {
		if err != nil {
			if err.shouldNotRollback {
				t.ensureCleanUpRequest()
			}

			cb(nil, err)
			return
		}

		cb(res, nil)
	})
}

func (t *transactionAttempt) remove(
	opts RemoveOptions,
	cb func(*GetResult, *TransactionOperationFailedError),
) error {
	t.beginOpAndLock(func(unlock func(), endOp func()) {
		endAndCb := func(result *GetResult, err *TransactionOperationFailedError) {
			endOp()
			cb(result, err)
		}

		err := t.checkCanPerformOpLocked()
		if err != nil {
			unlock()
			endAndCb(nil, err)
			return
		}

		agent := opts.Document.agent
		scopeName := opts.Document.scopeName
		collectionName := opts.Document.collectionName
		key := opts.Document.key
		cas := opts.Document.Cas
		meta := opts.Document.Meta

		t.checkExpiredAtomic(hookRemove, key, false, func(cerr *classifiedError) {
			if cerr != nil {
				unlock()
				endAndCb(nil, t.operationFailed(operationFailedDef{
					Cerr:              cerr,
					ShouldNotRetry:    true,
					ShouldNotRollback: false,
					Reason:            ErrorReasonTransactionExpired,
				}))
				return
			}

			_, existingMutation := t.getStagedMutationLocked(agent.BucketName(), scopeName, collectionName, key)
			unlock()

			if existingMutation != nil {
				switch existingMutation.OpType {
				case StagedMutationInsert:
					if !t.enableCompoundOps {
						endAndCb(nil, t.operationFailed(operationFailedDef{
							Cerr: classifyError(
								errors.Wrap(ErrIllegalState, "attempted to remove a document previously inserted in this transaction")),
							ShouldNotRetry:    true,
							ShouldNotRollback: false,
							Reason:            ErrorReasonTransactionFailed,
						}))
						return
					}

					t.stageRemoveOfInsert(
						agent, scopeName, collectionName, key,
						cas,
						func(result *GetResult, err *TransactionOperationFailedError) {
							endAndCb(result, err)
						})
					return
				case StagedMutationReplace:
					// We can overwrite other replaces without issue, any conflicts between the mutation
					// the user passed to us and the existing mutation is caught by WriteWriteConflict.
				case StagedMutationRemove:
					endAndCb(nil, t.operationFailed(operationFailedDef{
						Cerr: classifyError(
							errors.Wrap(ErrDocumentNotFound, "attempted to remove a document previously removed in this transaction")),
						ShouldNotRetry:    true,
						ShouldNotRollback: false,
						Reason:            ErrorReasonTransactionFailed,
					}))
					return
				default:
					endAndCb(nil, t.operationFailed(operationFailedDef{
						Cerr: classifyError(
							errors.Wrap(ErrIllegalState, "unexpected staged mutation type")),
						ShouldNotRetry:    true,
						ShouldNotRollback: false,
						Reason:            ErrorReasonTransactionFailed,
					}))
					return
				}
			}

			t.writeWriteConflictPoll(
				forwardCompatStageWWCRemoving,
				agent, scopeName, collectionName, key, cas,
				meta,
				existingMutation,
				func(err *TransactionOperationFailedError) {
					if err != nil {
						endAndCb(nil, err)
						return
					}

					t.confirmATRPending(agent, scopeName, collectionName, key, func(err *TransactionOperationFailedError) {
						if err != nil {
							endAndCb(nil, err)
							return
						}

						t.stageRemove(
							agent, scopeName, collectionName, key,
							cas,
							func(result *GetResult, err *TransactionOperationFailedError) {
								endAndCb(result, err)
							})
					})

				})
		})
	})

	return nil
}

func (t *transactionAttempt) stageRemove(
	agent *gocbcore.Agent,
	scopeName string,
	collectionName string,
	key []byte,
	cas gocbcore.Cas,
	cb func(*GetResult, *TransactionOperationFailedError),
) {
	ecCb := func(result *GetResult, cerr *classifiedError) {
		if cerr == nil {
			cb(result, nil)
			return
		}

		switch cerr.Class {
		case ErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionExpired,
			}))
		case ErrorClassFailDocNotFound:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					errors.Wrap(ErrDocumentNotFound, "document not found during staging")),
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailDocAlreadyExists:
			cerr.Class = ErrorClassFailCasMismatch
			fallthrough
		case ErrorClassFailCasMismatch:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailTransient:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailAmbiguous:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailHard:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailed,
			}))
		default:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		}
	}

	t.checkExpiredAtomic(hookRemove, key, false, func(cerr *classifiedError) {
		if cerr != nil {
			ecCb(nil, cerr)
			return
		}

		t.hooks.BeforeStagedRemove(key, func(err error) {
			if err != nil {
				ecCb(nil, classifyHookError(err))
				return
			}

			stagedInfo := &stagedMutation{
				OpType:         StagedMutationRemove,
				Agent:          agent,
				ScopeName:      scopeName,
				CollectionName: collectionName,
				Key:            key,
			}

			var txnMeta jsonTxnXattr
			txnMeta.ID.Transaction = t.transactionID
			txnMeta.ID.Attempt = t.id
			txnMeta.ATR.CollectionName = t.atrCollectionName
			txnMeta.ATR.ScopeName = t.atrScopeName
			txnMeta.ATR.BucketName = t.atrAgent.BucketName()
			txnMeta.ATR.DocID = string(t.atrKey)
			txnMeta.Operation.Type = jsonMutationRemove
			txnMeta.Restore = &jsonTxnXattrRestore{
				OriginalCAS: "",
				ExpiryTime:  0,
				RevID:       "",
			}

			txnMetaBytes, err := json.Marshal(txnMeta)
			if err != nil {
				ecCb(nil, classifyError(err))
				return
			}

			deadline, duraTimeout := mutationTimeouts(t.keyValueTimeout, t.durabilityLevel)

			flags := memd.SubdocDocFlagAccessDeleted

			_, err = stagedInfo.Agent.MutateIn(gocbcore.MutateInOptions{
				ScopeName:      stagedInfo.ScopeName,
				CollectionName: stagedInfo.CollectionName,
				Key:            stagedInfo.Key,
				Cas:            cas,
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
						Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
						Value: crc32cMacro,
					},
					{
						Op:    memd.SubDocOpDictSet,
						Path:  "txn.restore.CAS",
						Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
						Value: casMacro,
					},
					{
						Op:    memd.SubDocOpDictSet,
						Path:  "txn.restore.exptime",
						Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
						Value: exptimeMacro,
					},
					{
						Op:    memd.SubDocOpDictSet,
						Path:  "txn.restore.revid",
						Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
						Value: revidMacro,
					},
				},
				Flags:                  flags,
				DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
				DurabilityLevelTimeout: duraTimeout,
				Deadline:               deadline,
			}, func(result *gocbcore.MutateInResult, err error) {
				if err != nil {
					ecCb(nil, classifyError(err))
					return
				}

				stagedInfo.Cas = result.Cas

				t.recordStagedMutation(stagedInfo, func() {

					t.hooks.AfterStagedRemoveComplete(key, func(err error) {
						if err != nil {
							ecCb(nil, classifyHookError(err))
							return
						}

						ecCb(&GetResult{
							agent:          stagedInfo.Agent,
							scopeName:      stagedInfo.ScopeName,
							collectionName: stagedInfo.CollectionName,
							key:            stagedInfo.Key,
							Value:          stagedInfo.Staged,
							Cas:            stagedInfo.Cas,
							Meta:           nil,
						}, nil)
					})
				})
			})
			if err != nil {
				ecCb(nil, classifyError(err))
				return
			}
		})
	})
}

func (t *transactionAttempt) stageRemoveOfInsert(
	agent *gocbcore.Agent,
	scopeName string,
	collectionName string,
	key []byte,
	cas gocbcore.Cas,
	cb func(*GetResult, *TransactionOperationFailedError),
) {
	ecCb := func(result *GetResult, cerr *classifiedError) {
		if cerr == nil {
			cb(result, nil)
			return
		}

		switch cerr.Class {
		case ErrorClassFailDocNotFound:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					errors.Wrap(ErrDocumentNotFound, "staged document was modified since insert")),
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailDocAlreadyExists:
			cerr.Class = ErrorClassFailCasMismatch
			fallthrough
		case ErrorClassFailCasMismatch:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailTransient:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailAmbiguous:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailHard:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailed,
			}))
		default:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		}
	}

	t.hooks.BeforeStagedRemove(key, func(err error) {
		if err != nil {
			ecCb(nil, classifyHookError(err))
			return
		}

		deadline, duraTimeout := mutationTimeouts(t.keyValueTimeout, t.durabilityLevel)

		_, err = agent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      scopeName,
			CollectionName: collectionName,
			Key:            key,
			Cas:            cas,
			Flags:          memd.SubdocDocFlagAccessDeleted,
			Ops: []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpDelete,
					Path:  "txn",
					Flags: memd.SubdocFlagXattrPath,
				},
			},
			DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
			DurabilityLevelTimeout: duraTimeout,
			Deadline:               deadline,
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				ecCb(nil, classifyError(err))
				return
			}

			t.hooks.AfterStagedRemoveComplete(key, func(err error) {
				if err != nil {
					ecCb(nil, classifyHookError(err))
					return
				}

				t.removeStagedMutation(agent.BucketName(), scopeName, collectionName, key, func() {
					cb(&GetResult{
						agent:          agent,
						scopeName:      scopeName,
						collectionName: collectionName,
						key:            key,
						Cas:            result.Cas,
					}, nil)
				})
			})
		})
		if err != nil {
			ecCb(nil, classifyError(err))
			return
		}
	})
}
