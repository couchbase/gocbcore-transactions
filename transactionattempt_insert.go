package transactions

import (
	"encoding/json"
	"time"

	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
)

func (t *transactionAttempt) Insert(opts InsertOptions, cb StoreCallback) error {
	return t.insert(opts, func(res *GetResult, err *TransactionOperationFailedError) {
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

func (t *transactionAttempt) insert(
	opts InsertOptions,
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

		agent := opts.Agent
		scopeName := opts.ScopeName
		collectionName := opts.CollectionName
		key := opts.Key
		value := opts.Value

		t.checkExpiredAtomic(hookInsert, key, false, func(cerr *classifiedError) {
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
				case StagedMutationRemove:
					t.stageReplace(
						agent, scopeName, collectionName, key,
						value, existingMutation.Cas,
						func(result *GetResult, err *TransactionOperationFailedError) {
							endAndCb(result, err)
						})
					return
				case StagedMutationInsert:
					fallthrough
				case StagedMutationReplace:
					endAndCb(nil, t.operationFailed(operationFailedDef{
						Cerr: &classifiedError{
							Source: nil,
							Class:  ErrorClassFailDocAlreadyExists,
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

			t.confirmATRPending(agent, scopeName, collectionName, key, func(err *TransactionOperationFailedError) {
				if err != nil {
					endAndCb(nil, err)
					return
				}

				t.stageInsert(
					agent, scopeName, collectionName, key,
					value, 0,
					false,
					func(result *GetResult, err *TransactionOperationFailedError) {
						endAndCb(result, err)
					})
			})
		})
	})

	return nil
}

func (t *transactionAttempt) resolveConflictedInsert(
	agent *gocbcore.Agent,
	scopeName string,
	collectionName string,
	key []byte,
	value json.RawMessage,
	replaceOfInsert bool,
	cb func(*GetResult, *TransactionOperationFailedError),
) {
	t.getMetaForConflictedInsert(agent, scopeName, collectionName, key,
		func(cas gocbcore.Cas, meta *MutableItemMeta, err *TransactionOperationFailedError) {
			if err != nil {
				cb(nil, err)
				return
			}

			if meta == nil {
				// There wasn't actually a staged mutation there.
				t.stageInsert(agent, scopeName, collectionName, key, value, cas, replaceOfInsert, cb)
				return
			}

			// We have guards in place within the write write conflict polling to prevent miss-use when
			// an existing mutation must have been discovered before it's safe to overwrite.  This logic
			// is unneccessary, as is the forwards compatibility check when resolving conflicted inserts
			// so we can safely just ignore it.
			if meta.TransactionID == t.transactionID && meta.AttemptID == t.id {
				t.stageInsert(agent, scopeName, collectionName, key, value, cas, replaceOfInsert, cb)
				return
			}

			t.checkForwardCompatability(forwardCompatStageWWCInsertingGet, meta.ForwardCompat, false, func(err *TransactionOperationFailedError) {
				if err != nil {
					cb(nil, err)
					return
				}

				t.writeWriteConflictPoll(forwardCompatStageWWCInserting, agent, scopeName, collectionName, key, cas, meta, nil, func(err *TransactionOperationFailedError) {
					if err != nil {
						cb(nil, err)
						return
					}

					t.stageInsert(agent, scopeName, collectionName, key, value, cas, replaceOfInsert, cb)
				})
			})
		})
}

func (t *transactionAttempt) stageInsert(
	agent *gocbcore.Agent,
	scopeName string,
	collectionName string,
	key []byte,
	value json.RawMessage,
	cas gocbcore.Cas,
	replaceOfInsert bool,
	cb func(*GetResult, *TransactionOperationFailedError),
) {
	ecCb := func(result *GetResult, cerr *classifiedError) {
		if cerr == nil {
			cb(result, nil)
			return
		}

		switch cerr.Class {
		case ErrorClassFailAmbiguous:
			time.AfterFunc(3*time.Millisecond, func() {
				t.stageInsert(agent, scopeName, collectionName, key, value, cas, replaceOfInsert, cb)
			})
		case ErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionExpired,
			}))
		case ErrorClassFailTransient:
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
		case ErrorClassFailDocAlreadyExists:
			fallthrough
		case ErrorClassFailCasMismatch:
			t.resolveConflictedInsert(agent, scopeName, collectionName, key, value, replaceOfInsert, cb)
		default:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		}
	}

	// BUG(TXNG-70): Remove this once FIT has been updated.
	// Note that replaceOfInsert should also be removed from stageInsert and
	// resolveConflictedInsert.
	beforeHook := t.hooks.BeforeStagedInsert
	afterHook := t.hooks.AfterStagedInsertComplete
	if replaceOfInsert {
		beforeHook = t.hooks.BeforeStagedReplace
		afterHook = t.hooks.AfterStagedReplaceComplete
	}

	t.checkExpiredAtomic(hookInsert, key, false, func(cerr *classifiedError) {
		if cerr != nil {
			ecCb(nil, cerr)
			return
		}

		beforeHook(key, func(err error) {
			if err != nil {
				ecCb(nil, classifyHookError(err))
				return
			}

			stagedInfo := &stagedMutation{
				OpType:         StagedMutationInsert,
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
			txnMeta.Operation.Type = jsonMutationInsert
			txnMeta.Operation.Staged = stagedInfo.Staged

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
						Op:    memd.SubDocOpDictSet,
						Path:  "txn.op.crc32",
						Flags: memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
						Value: crc32cMacro,
					},
				},
				DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
				DurabilityLevelTimeout: duraTimeout,
				Deadline:               deadline,
				Flags:                  flags,
			}, func(result *gocbcore.MutateInResult, err error) {
				if err != nil {
					ecCb(nil, classifyError(err))
					return
				}

				stagedInfo.Cas = result.Cas

				t.recordStagedMutation(stagedInfo, func() {

					afterHook(key, func(err error) {
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
			}
		})
	})
}

func (t *transactionAttempt) getMetaForConflictedInsert(
	agent *gocbcore.Agent,
	scopeName string,
	collectionName string,
	key []byte,
	cb func(gocbcore.Cas, *MutableItemMeta, *TransactionOperationFailedError),
) {
	ecCb := func(cas gocbcore.Cas, meta *MutableItemMeta, cerr *classifiedError) {
		if cerr == nil {
			cb(cas, meta, nil)
			return
		}

		switch cerr.Class {
		case ErrorClassFailDocNotFound:
			fallthrough
		case ErrorClassFailPathNotFound:
			fallthrough
		case ErrorClassFailTransient:
			cb(0, nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		default:
			cb(0, nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		}
	}

	t.hooks.BeforeGetDocInExistsDuringStagedInsert(key, func(err error) {
		if err != nil {
			ecCb(0, nil, classifyHookError(err))
			return
		}

		var deadline time.Time
		if t.operationTimeout > 0 {
			deadline = time.Now().Add(t.operationTimeout)
		}

		_, err = agent.LookupIn(gocbcore.LookupInOptions{
			ScopeName:      scopeName,
			CollectionName: collectionName,
			Key:            key,
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
				ecCb(0, nil, classifyError(err))
				return
			}

			var txnMeta *jsonTxnXattr
			if result.Ops[0].Err == nil {
				var txnMetaVal jsonTxnXattr
				if err := json.Unmarshal(result.Ops[0].Value, &txnMetaVal); err != nil {
					ecCb(0, nil, &classifiedError{
						Source: err,
						Class:  ErrorClassFailOther,
					})
					return
				}
				txnMeta = &txnMetaVal
			}

			if txnMeta == nil {
				// This doc isn't in a transaction
				if !result.Internal.IsDeleted {
					ecCb(0, nil, &classifiedError{
						Source: ErrDocumentAlreadyExists,
						Class:  ErrorClassFailDocAlreadyExists,
					})
					return
				}

				ecCb(result.Cas, nil, nil)
				return
			}

			ecCb(result.Cas, &MutableItemMeta{
				TransactionID: txnMeta.ID.Transaction,
				AttemptID:     txnMeta.ID.Attempt,
				ATR: MutableItemMetaATR{
					BucketName:     txnMeta.ATR.BucketName,
					ScopeName:      txnMeta.ATR.ScopeName,
					CollectionName: txnMeta.ATR.CollectionName,
					DocID:          txnMeta.ATR.DocID,
				},
				ForwardCompat: jsonForwardCompatToForwardCompat(txnMeta.ForwardCompat),
			}, nil)
		})
		if err != nil {
			ecCb(0, nil, classifyError(err))
			return
		}
	})
}
