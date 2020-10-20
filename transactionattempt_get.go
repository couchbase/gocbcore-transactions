package transactions

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
)

func (t *transactionAttempt) Get(opts GetOptions, cb GetCallback) error {
	return t.get(opts, "", func(result *GetResult, err error) {
		if err != nil {
			var tErr *TransactionOperationFailedError
			if errors.As(err, &tErr) {
				if tErr.shouldNotRollback {
					t.addCleanupRequest(t.createCleanUpRequest())
				}
			}

			cb(nil, err)
			return
		}
		t.hooks.AfterGetComplete(opts.Key, func(err error) {
			if err != nil {
				cb(nil, err)
				return
			}
			cb(result, nil)
		})
	})
}

func (t *transactionAttempt) get(opts GetOptions, resolvingATREntry string, cb GetCallback) error {
	if err := t.checkDone(); err != nil {
		ec := t.classifyError(err)
		return t.createAndStashOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec, true)
	}

	if err := t.checkError(); err != nil {
		return err
	}

	t.checkExpired(hookGet, opts.Key, func(err error) {
		if err != nil {
			t.expiryOvertimeMode = true
			cb(nil, t.createAndStashOperationFailedError(false, false, ErrAttemptExpired, ErrorReasonTransactionExpired, ErrorClassFailExpiry, true))
			return
		}

		t.lock.Lock()

		_, existingMutation := t.getStagedMutationLocked(opts.Agent.BucketName(), opts.ScopeName, opts.CollectionName, opts.Key)
		if existingMutation != nil {
			if existingMutation.OpType == StagedMutationInsert || existingMutation.OpType == StagedMutationReplace {
				getRes := &GetResult{
					agent:          existingMutation.Agent,
					scopeName:      existingMutation.ScopeName,
					collectionName: existingMutation.CollectionName,
					key:            existingMutation.Key,
					Value:          existingMutation.Staged,
					Cas:            existingMutation.Cas,
					Meta: MutableItemMeta{
						Deleted: existingMutation.IsTombstone,
					},
				}

				t.lock.Unlock()
				cb(getRes, nil)
				return
			} else if existingMutation.OpType == StagedMutationRemove {
				t.lock.Unlock()

				cb(nil, t.createAndStashOperationFailedError(false, false, gocbcore.ErrDocumentNotFound, ErrorReasonTransactionFailed, ErrorClassFailDocNotFound, true))
				return
			}
		}

		t.lock.Unlock()

		var deadline time.Time
		if t.keyValueTimeout > 0 {
			deadline = time.Now().Add(t.keyValueTimeout)
		}

		t.getFullDoc(opts, deadline, func(doc *getDoc, err error) {
			if err != nil {
				var failErr error
				ec := t.classifyError(err)
				switch ec {
				case ErrorClassFailDocNotFound:
					failErr = t.createAndStashOperationFailedError(false, false, err, ErrorReasonTransactionFailed,
						ec, true)
				case ErrorClassFailHard:
					failErr = t.createAndStashOperationFailedError(false, true, err, ErrorReasonTransactionFailed,
						ec, true)
				case ErrorClassFailTransient:
					failErr = t.createAndStashOperationFailedError(true, false, err, ErrorReasonTransactionFailed,
						ec, true)
				default:
					failErr = t.createAndStashOperationFailedError(false, false, err, ErrorReasonTransactionFailed,
						ec, true)
				}

				cb(nil, failErr)
				return
			}

			// Doc not involved in another transaction.
			if doc.TxnMeta == nil {
				if doc.Deleted {
					cb(nil, t.createAndStashOperationFailedError(false, false, gocbcore.ErrDocumentNotFound, ErrorReasonTransactionFailed, ErrorClassFailDocNotFound, false))
					return
				}

				cb(&GetResult{
					agent:          opts.Agent,
					scopeName:      opts.ScopeName,
					collectionName: opts.CollectionName,
					key:            opts.Key,
					Value:          doc.Body,
					Cas:            doc.Cas,
					Meta: MutableItemMeta{
						RevID:   doc.DocMeta.RevID,
						Expiry:  doc.DocMeta.Expiration,
						Deleted: false,
						TxnMeta: nil,
					},
				}, nil)
				return
			}

			if doc.TxnMeta.ID.Attempt == resolvingATREntry {
				if doc.Deleted {
					cb(nil, t.createAndStashOperationFailedError(false, false, gocbcore.ErrDocumentNotFound, ErrorReasonTransactionFailed, ErrorClassFailDocNotFound, false))
					return
				}

				cb(&GetResult{
					agent:          opts.Agent,
					scopeName:      opts.ScopeName,
					collectionName: opts.CollectionName,
					key:            opts.Key,
					Value:          doc.Body,
					Cas:            doc.Cas,
					Meta: MutableItemMeta{
						RevID:   doc.DocMeta.RevID,
						Expiry:  doc.DocMeta.Expiration,
						Deleted: false,
						TxnMeta: doc.TxnMeta,
					},
				}, nil)
				return
			}

			t.getTxnState(opts, deadline, doc.TxnMeta, func(attempt *jsonAtrAttempt, err error) {
				if err != nil {
					ec := t.classifyError(err)
					if errors.Is(err, ErrAtrNotFound) {
						cb(nil, t.createAndStashOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec, false))
						return
					} else if errors.Is(err, ErrAtrEntryNotFound) {
						if err := t.get(opts, doc.TxnMeta.ID.Attempt, cb); err != nil {
							cb(nil, err)
						}
						return
					}
					var failErr error
					switch ec {
					case ErrorClassFailHard:
						failErr = t.createAndStashOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec, false)
					case ErrorClassFailTransient:
						failErr = t.createAndStashOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec, false)
					default:
						failErr = t.createAndStashOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec, false)
					}
					cb(nil, failErr)
					return
				}

				state := jsonAtrState(attempt.State)
				if state == jsonAtrStateCommitted || state == jsonAtrStateCompleted {
					if doc.TxnMeta.Operation.Type == jsonMutationRemove {

						cb(nil, t.createAndStashOperationFailedError(false, false, gocbcore.ErrDocumentNotFound, ErrorReasonTransactionFailed, ErrorClassFailDocNotFound, false))
						return
					}

					// TODO(brett19): Discuss virtual CAS with Graham
					cb(&GetResult{
						agent:          opts.Agent,
						scopeName:      opts.ScopeName,
						collectionName: opts.CollectionName,
						key:            opts.Key,
						Value:          doc.TxnMeta.Operation.Staged,
						Cas:            doc.Cas,
						Meta: MutableItemMeta{
							RevID:   doc.DocMeta.RevID,
							Expiry:  doc.DocMeta.Expiration,
							Deleted: false,
							TxnMeta: doc.TxnMeta,
						},
					}, nil)
					return
				}

				if doc.Deleted {
					cb(nil, t.createAndStashOperationFailedError(false, false, gocbcore.ErrDocumentNotFound, ErrorReasonTransactionFailed, ErrorClassFailDocNotFound, false))
					return
				}

				cb(&GetResult{
					agent:          opts.Agent,
					scopeName:      opts.ScopeName,
					collectionName: opts.CollectionName,
					key:            opts.Key,
					Value:          doc.Body,
					Cas:            doc.Cas,
					Meta: MutableItemMeta{
						RevID:   doc.DocMeta.RevID,
						Expiry:  doc.DocMeta.Expiration,
						Deleted: false,
						TxnMeta: doc.TxnMeta,
					},
				}, nil)
			})
		})
	})

	return nil
}

func (t *transactionAttempt) getFullDoc(opts GetOptions, deadline time.Time,
	cb func(*getDoc, error)) {
	t.hooks.BeforeDocGet(opts.Key, func(err error) {
		if err != nil {
			cb(nil, err)
			return
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

			if len(result.Ops) != 3 {
				cb(nil, ErrOther)
				return
			}

			if result.Ops[0].Err != nil {
				cb(nil, result.Ops[0].Err)
				return
			}

			var docBody []byte
			if result.Ops[2].Err == nil {
				docBody = result.Ops[2].Value
			}

			var meta *docMeta
			if err := json.Unmarshal(result.Ops[0].Value, &meta); err != nil {
				cb(nil, err)
				return
			}

			var txnMeta *jsonTxnXattr
			if result.Ops[1].Err == nil {
				// Doc isn't currently in a txn.
				var txnMetaVal jsonTxnXattr
				if err := json.Unmarshal(result.Ops[1].Value, &txnMetaVal); err != nil {
					cb(nil, err)
					return
				}

				txnMeta = &txnMetaVal

				cb(&getDoc{
					Body:    docBody,
					TxnMeta: txnMeta,
					DocMeta: meta,
					Cas:     result.Cas,
					Deleted: result.Internal.IsDeleted,
				}, nil)

				return
			}

			if result.Internal.IsDeleted {
				cb(nil, gocbcore.ErrDocumentNotFound)
				return
			}

			cb(&getDoc{
				Body:    docBody,
				DocMeta: meta,
				Cas:     result.Cas,
			}, nil)
		})
		if err != nil {
			cb(nil, err)
		}
	})
}
