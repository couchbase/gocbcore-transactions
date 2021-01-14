package transactions

import (
	"encoding/json"
	"time"

	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
)

func (t *transactionAttempt) Replace(opts ReplaceOptions, cb StoreCallback) error {
	return t.replace(opts, func(res *GetResult, err *TransactionOperationFailedError) {
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

func (t *transactionAttempt) replace(
	opts ReplaceOptions,
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

		unlock()

		agent := opts.Document.agent
		scopeName := opts.Document.scopeName
		collectionName := opts.Document.collectionName
		key := opts.Document.key
		value := opts.Value
		cas := opts.Document.Cas
		meta := opts.Document.Meta

		t.checkExpiredAtomic(hookReplace, key, false, func(cerr *classifiedError) {
			if cerr != nil {
				endAndCb(nil, t.operationFailed(operationFailedDef{
					Cerr:              cerr,
					ShouldNotRetry:    true,
					ShouldNotRollback: false,
					Reason:            ErrorReasonTransactionExpired,
				}))
				return
			}

			_, existingMutation := t.getStagedMutationLocked(agent.BucketName(), scopeName, collectionName, key)
			if existingMutation != nil {
				switch existingMutation.OpType {
				case StagedMutationInsert:
					t.stageInsert(
						agent, scopeName, collectionName, key,
						value, cas,
						true,
						func(result *GetResult, err *TransactionOperationFailedError) {
							endAndCb(result, err)
						})
					return

				case StagedMutationReplace:
					// We can overwrite other replaces without issue, any conflicts between the mutation
					// the user passed to us and the existing mutation is caught by WriteWriteConflict.
				case StagedMutationRemove:
					endAndCb(nil, t.operationFailed(operationFailedDef{
						Cerr: &classifiedError{
							Source: nil,
							Class:  ErrorClassFailDocNotFound,
						},
						ShouldNotRetry:    false,
						ShouldNotRollback: false,
						Reason:            ErrorReasonTransactionFailed,
					}))
					return
				default:
					endAndCb(nil, t.operationFailed(operationFailedDef{
						Cerr: &classifiedError{
							Source: ErrIllegalState,
							Class:  ErrorClassFailOther,
						},
						ShouldNotRetry:    true,
						ShouldNotRollback: false,
						Reason:            ErrorReasonTransactionFailed,
					}))
					return
				}
			}

			t.writeWriteConflictPoll(
				forwardCompatStageWWCReplacing,
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

						t.stageReplace(
							agent, scopeName, collectionName, key,
							value, cas,
							func(result *GetResult, err *TransactionOperationFailedError) {
								endAndCb(result, err)
							})
					})
				})
		})
	})

	return nil
}

func (t *transactionAttempt) stageReplace(
	agent *gocbcore.Agent,
	scopeName string,
	collectionName string,
	key []byte,
	value json.RawMessage,
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
				Cerr: &classifiedError{
					Source: ErrDocumentNotFound,
					Class:  ErrorClassFailDocNotFound,
				},
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailDocAlreadyExists:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr: &classifiedError{
					Source: cerr.Source,
					Class:  ErrorClassFailCasMismatch,
				},
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
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

		t.hooks.BeforeStagedReplace(key, func(err error) {
			if err != nil {
				ecCb(nil, classifyHookError(err))
				return
			}

			stagedInfo := &stagedMutation{
				OpType:         StagedMutationReplace,
				Agent:          agent,
				ScopeName:      scopeName,
				CollectionName: collectionName,
				Key:            key,
				Staged:         value,
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
			txnMeta.Restore = &jsonTxnXattrRestore{
				OriginalCAS: "",
				ExpiryTime:  0,
				RevID:       "",
			}

			txnMetaBytes, err := json.Marshal(txnMeta)
			if err != nil {
				ecCb(nil, &classifiedError{
					Source: err,
					Class:  ErrorClassFailOther,
				})
				return
			}

			var duraTimeout time.Duration
			var deadline time.Time
			if t.operationTimeout > 0 {
				duraTimeout = t.operationTimeout * 10 / 9
				deadline = time.Now().Add(t.operationTimeout)
			}

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
				Flags:                  memd.SubdocDocFlagAccessDeleted,
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

					t.hooks.AfterStagedReplaceComplete(key, func(err error) {
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
