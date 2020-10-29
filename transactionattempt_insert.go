package transactions

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
)

func (t *transactionAttempt) Insert(opts InsertOptions, cb StoreCallback) error {
	return t.insert(opts, 0, func(result *GetResult, err error) {
		var tErr *TransactionOperationFailedError
		if errors.As(err, &tErr) {
			if tErr.shouldNotRollback {
				t.addCleanupRequest(t.createCleanUpRequest())
			}
		}

		cb(result, err)
	})
}

func (t *transactionAttempt) insert(opts InsertOptions, cas gocbcore.Cas, cb StoreCallback) error {
	handler := func(result *GetResult, err error) {
		if err != nil {
			var failErr error
			ec := t.classifyError(err)
			if errors.Is(err, gocbcore.ErrFeatureNotAvailable) {
				cb(nil, t.createAndStashOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec, false))
				return
			}
			if t.expiryOvertimeMode {
				cb(nil, t.createAndStashOperationFailedError(false, true, ErrAttemptExpired, ErrorReasonTransactionExpired, ErrorClassFailExpiry, false))
				return
			}

			switch ec {
			case ErrorClassFailExpiry:
				t.expiryOvertimeMode = true
				failErr = t.createAndStashOperationFailedError(false, false, ErrAttemptExpired, ErrorReasonTransactionExpired, ec, false)
			case ErrorClassFailAmbiguous:
				time.AfterFunc(3*time.Millisecond, func() {
					err := t.insert(opts, 0, cb)
					if err != nil {
						cb(nil, err)
					}
				})
				return
			case ErrorClassFailTransient:
				failErr = t.createAndStashOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec, false)
			case ErrorClassFailHard:
				failErr = t.createAndStashOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec, false)
			case ErrorClassFailDocAlreadyExists:
				fallthrough
			case ErrorClassFailCasMismatch:
				t.getForInsert(opts, func(result *GetResult, err error) {
					if err != nil {
						if errors.Is(err, ErrForwardCompatibilityFailure) {
							cb(nil, err)
							return
						}

						var failErr error
						ec := t.classifyError(err)
						switch ec {
						case ErrorClassFailDocNotFound:
							failErr = t.createAndStashOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec, false)
						case ErrorClassFailPathNotFound:
							failErr = t.createAndStashOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec, false)
						case ErrorClassFailTransient:
							failErr = t.createAndStashOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec, false)
						default:
							failErr = t.createAndStashOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec, false)
						}

						cb(nil, failErr)
						return
					}

					t.writeWriteConflictPoll(result, forwardCompatStageWWCInserting, func(err error) {
						if err != nil {
							cb(nil, err)
							return
						}

						err = t.insert(opts, result.Cas, cb)
						if err != nil {
							cb(nil, err)
						}
					})
				})

				return
			default:
				failErr = t.createAndStashOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec, false)
			}

			cb(nil, failErr)
			return
		}

		cb(result, nil)
	}

	if err := t.checkDone(); err != nil {
		ec := t.classifyError(err)
		return t.createAndStashOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec, false)
	}

	if err := t.checkError(); err != nil {
		return err
	}

	t.checkExpired(hookInsert, opts.Key, func(err error) {
		if err != nil {
			handler(nil, ErrAttemptExpired)
			return
		}

		t.confirmATRPending(opts.Agent, opts.Key, func(err error) {
			if err != nil {
				// We've already classified the error so just hit the callback.
				cb(nil, err)
				return
			}

			t.hooks.BeforeStagedInsert(opts.Key, func(err error) {
				if err != nil {
					handler(nil, err)
					return
				}

				stagedInfo := &stagedMutation{
					OpType:         StagedMutationInsert,
					Agent:          opts.Agent,
					ScopeName:      opts.ScopeName,
					CollectionName: opts.CollectionName,
					Key:            opts.Key,
					Staged:         opts.Value,
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
					handler(nil, err)
					return
				}

				var duraTimeout time.Duration
				var deadline time.Time
				if t.keyValueTimeout > 0 {
					deadline = time.Now().Add(t.keyValueTimeout)
					duraTimeout = t.keyValueTimeout * 10 / 9
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
							Op:    memd.SubDocOpDictAdd,
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
						handler(nil, err)
						return
					}

					t.hooks.AfterStagedInsertComplete(opts.Key, func(err error) {
						if err != nil {
							handler(nil, err)
							return
						}

						t.lock.Lock()
						stagedInfo.Cas = result.Cas
						t.stagedMutations = append(t.stagedMutations, stagedInfo)
						t.lock.Unlock()

						handler(&GetResult{
							agent:          stagedInfo.Agent,
							scopeName:      stagedInfo.ScopeName,
							collectionName: stagedInfo.CollectionName,
							key:            stagedInfo.Key,
							Value:          stagedInfo.Staged,
							Cas:            result.Cas,
						}, err)
					})
				})
				if err != nil {
					handler(nil, err)
				}
			})
		})
	})

	return nil
}

func (t *transactionAttempt) getForInsert(opts InsertOptions, cb func(*GetResult, error)) {
	t.hooks.BeforeGetDocInExistsDuringStagedInsert(opts.Key, func(err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		var deadline time.Time
		if t.keyValueTimeout > 0 {
			deadline = time.Now().Add(t.keyValueTimeout)
		}

		_, err = opts.Agent.LookupIn(gocbcore.LookupInOptions{
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Ops: []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpGet,
					Path:  "$document",
					Flags: memd.SubdocFlagXattrPath,
				},
				{
					Op:    memd.SubDocOpGet,
					Path:  "txn",
					Flags: memd.SubdocFlagXattrPath,
				},
				{
					Op:    memd.SubDocOpGetDoc,
					Path:  "",
					Flags: 0,
				},
			},
			Deadline: deadline,
			Flags:    memd.SubdocDocFlagAccessDeleted,
		}, func(result *gocbcore.LookupInResult, err error) {
			if err != nil {
				cb(nil, err)
				return
			}

			var txnMeta *jsonTxnXattr
			if result.Ops[1].Err == nil {
				var txnMetaVal jsonTxnXattr
				if err := json.Unmarshal(result.Ops[1].Value, &txnMetaVal); err != nil {
					cb(nil, err)
					return
				}
				txnMeta = &txnMetaVal
			}

			var forwardCompat map[string][]ForwardCompatibilityEntry
			if txnMeta != nil {
				forwardCompat = jsonForwardCompatToForwardCompat(txnMeta.ForwardCompat)
			}

			t.checkForwardCompatbility(forwardCompatStageWWCInsertingGet, forwardCompat, func(err error) {
				if err != nil {
					cb(nil, err)
					return
				}

				var val []byte
				if result.Ops[2].Err != nil {
					val = result.Ops[2].Value
				}

				if txnMeta == nil {
					// This doc isn't in a transaction
					if result.Internal.IsDeleted {
						cb(&GetResult{
							agent:          opts.Agent,
							scopeName:      opts.ScopeName,
							collectionName: opts.CollectionName,
							key:            opts.Key,
							Meta:           nil,
							Value:          val,
							Cas:            result.Cas,
						}, nil)
						return
					}

					cb(nil, gocbcore.ErrDocumentExists)
					return
				}

				cb(&GetResult{
					agent:          opts.Agent,
					scopeName:      opts.ScopeName,
					collectionName: opts.CollectionName,
					key:            opts.Key,
					Meta: &MutableItemMeta{
						TransactionID: txnMeta.ID.Transaction,
						AttemptID:     txnMeta.ID.Attempt,
						ATR: MutableItemMetaATR{
							BucketName:     txnMeta.ATR.BucketName,
							ScopeName:      txnMeta.ATR.ScopeName,
							CollectionName: txnMeta.ATR.CollectionName,
							DocID:          txnMeta.ATR.DocID,
						},
						ForwardCompat: forwardCompat,
					},
					Value: val,
					Cas:   result.Cas,
				}, nil)
			})
		})
		if err != nil {
			cb(nil, err)
			return
		}
	})
}
