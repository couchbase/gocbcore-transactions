package transactions

import (
	"encoding/json"
	"time"

	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/pkg/errors"
)

func (t *transactionAttempt) Get(opts GetOptions, cb GetCallback) error {
	return t.get(opts, func(res *GetResult, err *TransactionOperationFailedError) {
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

func (t *transactionAttempt) get(
	opts GetOptions,
	cb func(*GetResult, *TransactionOperationFailedError),
) error {
	forceNonFatal := t.enableNonFatalGets

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

		t.checkExpiredAtomic(hookGet, opts.Key, false, func(cerr *classifiedError) {
			if cerr != nil {
				endAndCb(nil, t.operationFailed(operationFailedDef{
					Cerr:              cerr,
					ShouldNotRetry:    true,
					ShouldNotRollback: false,
					Reason:            ErrorReasonTransactionExpired,
				}))
				return
			}

			t.mavRead(opts.Agent, opts.ScopeName, opts.CollectionName, opts.Key, opts.NoRYOW, "", forceNonFatal, func(result *GetResult, err *TransactionOperationFailedError) {
				if err != nil {
					endAndCb(nil, err)
					return
				}

				t.hooks.AfterGetComplete(opts.Key, func(err error) {
					if err != nil {
						endAndCb(nil, t.operationFailed(operationFailedDef{
							Cerr:              classifyHookError(err),
							CanStillCommit:    forceNonFatal,
							ShouldNotRetry:    true,
							ShouldNotRollback: true,
							Reason:            ErrorReasonTransactionFailed,
						}))
						return
					}

					endAndCb(result, nil)
				})
			})
		})
	})

	return nil
}

func (t *transactionAttempt) mavRead(
	agent *gocbcore.Agent,
	scopeName string,
	collectionName string,
	key []byte,
	disableRYOW bool,
	resolvingATREntry string,
	forceNonFatal bool,
	cb func(*GetResult, *TransactionOperationFailedError),
) {
	t.fetchDocWithMeta(
		agent,
		scopeName,
		collectionName,
		key,
		forceNonFatal,
		func(doc *getDoc, err *TransactionOperationFailedError) {
			if err != nil {
				cb(nil, err)
				return
			}

			if disableRYOW {
				if doc.TxnMeta != nil && doc.TxnMeta.ID.Attempt == t.id {
					// This is going to be a RYOW, we can just clear the TxnMeta which
					// will cause us to fall into the block below.
					doc.TxnMeta = nil
				}
			}

			// Doc not involved in another transaction.
			if doc.TxnMeta == nil {
				if doc.Deleted {
					cb(nil, t.operationFailed(operationFailedDef{
						Cerr: classifyError(
							errors.Wrap(ErrDocumentNotFound, "doc was a tombstone")),
						CanStillCommit:    true,
						ShouldNotRetry:    true,
						ShouldNotRollback: false,
						Reason:            ErrorReasonTransactionFailed,
					}))
					return
				}

				cb(&GetResult{
					agent:          agent,
					scopeName:      scopeName,
					collectionName: collectionName,
					key:            key,
					Value:          doc.Body,
					Cas:            doc.Cas,
					Meta:           nil,
				}, nil)
				return
			}

			if doc.TxnMeta.ID.Attempt == t.id {
				switch doc.TxnMeta.Operation.Type {
				case jsonMutationInsert:
					fallthrough
				case jsonMutationReplace:
					cb(&GetResult{
						agent:          agent,
						scopeName:      scopeName,
						collectionName: collectionName,
						key:            key,
						Value:          doc.TxnMeta.Operation.Staged,
						Cas:            doc.Cas,
					}, nil)
				case jsonMutationRemove:
					cb(nil, t.operationFailed(operationFailedDef{
						Cerr: classifyError(
							errors.Wrap(ErrDocumentNotFound, "doc was a staged remove")),
						CanStillCommit:    true,
						ShouldNotRetry:    true,
						ShouldNotRollback: false,
						Reason:            ErrorReasonTransactionFailed,
					}))
				default:
					cb(nil, t.operationFailed(operationFailedDef{
						Cerr: classifyError(
							errors.Wrap(ErrIllegalState, "unexpected staged mutation type")),
						CanStillCommit:    forceNonFatal,
						ShouldNotRetry:    false,
						ShouldNotRollback: false,
						Reason:            ErrorReasonTransactionFailed,
					}))
				}
				return
			}

			if doc.TxnMeta.ID.Attempt == resolvingATREntry {
				if doc.Deleted {
					cb(nil, t.operationFailed(operationFailedDef{
						Cerr: classifyError(
							errors.Wrap(ErrDocumentNotFound, "doc was a staged tombstone during resolution")),
						CanStillCommit:    true,
						ShouldNotRetry:    true,
						ShouldNotRollback: false,
						Reason:            ErrorReasonTransactionFailed,
					}))
					return
				}

				cb(&GetResult{
					agent:          agent,
					scopeName:      scopeName,
					collectionName: collectionName,
					key:            key,
					Value:          doc.Body,
					Cas:            doc.Cas,
				}, nil)
				return
			}

			docFc := jsonForwardCompatToForwardCompat(doc.TxnMeta.ForwardCompat)
			docMeta := &MutableItemMeta{
				TransactionID: doc.TxnMeta.ID.Transaction,
				AttemptID:     doc.TxnMeta.ID.Attempt,
				ATR: MutableItemMetaATR{
					BucketName:     doc.TxnMeta.ATR.BucketName,
					ScopeName:      doc.TxnMeta.ATR.ScopeName,
					CollectionName: doc.TxnMeta.ATR.CollectionName,
					DocID:          doc.TxnMeta.ATR.DocID,
				},
				ForwardCompat: docFc,
			}

			t.checkForwardCompatability(
				forwardCompatStageGets,
				docFc,
				forceNonFatal,
				func(err *TransactionOperationFailedError) {
					if err != nil {
						cb(nil, err)
						return
					}

					t.getTxnState(
						agent.BucketName(),
						scopeName,
						collectionName,
						key,
						doc.TxnMeta.ATR.BucketName,
						doc.TxnMeta.ATR.ScopeName,
						doc.TxnMeta.ATR.CollectionName,
						doc.TxnMeta.ATR.DocID,
						doc.TxnMeta.ID.Attempt,
						forceNonFatal,
						func(attempt *jsonAtrAttempt, expiry time.Time, err *TransactionOperationFailedError) {
							if err != nil {
								cb(nil, err)
								return
							}

							if attempt == nil {
								// The ATR entry is missing, it's likely that we just raced the other transaction
								// cleaning up it's documents and then cleaning itself up.  Lets run ATR resolution.
								t.mavRead(agent, scopeName, collectionName, key, disableRYOW, doc.TxnMeta.ID.Attempt, forceNonFatal, cb)
								return
							}

							atmptFc := jsonForwardCompatToForwardCompat(attempt.ForwardCompat)
							t.checkForwardCompatability(forwardCompatStageGetsReadingATR, atmptFc, forceNonFatal, func(err *TransactionOperationFailedError) {
								if err != nil {
									cb(nil, err)
									return
								}

								state := jsonAtrState(attempt.State)
								if state == jsonAtrStateCommitted || state == jsonAtrStateCompleted {
									switch doc.TxnMeta.Operation.Type {
									case jsonMutationInsert:
										fallthrough
									case jsonMutationReplace:
										cb(&GetResult{
											agent:          agent,
											scopeName:      scopeName,
											collectionName: collectionName,
											key:            key,
											Value:          doc.TxnMeta.Operation.Staged,
											Cas:            doc.Cas,
											Meta:           docMeta,
										}, nil)
									case jsonMutationRemove:
										cb(nil, t.operationFailed(operationFailedDef{
											Cerr: classifyError(
												errors.Wrap(ErrDocumentNotFound, "doc was a staged remove")),
											CanStillCommit:    true,
											ShouldNotRetry:    true,
											ShouldNotRollback: false,
											Reason:            ErrorReasonTransactionFailed,
										}))
									default:
										cb(nil, t.operationFailed(operationFailedDef{
											Cerr: classifyError(
												errors.Wrap(ErrIllegalState, "unexpected staged mutation type")),
											ShouldNotRetry:    false,
											ShouldNotRollback: false,
											Reason:            ErrorReasonTransactionFailed,
										}))
									}
									return
								}

								if doc.Deleted {
									cb(nil, t.operationFailed(operationFailedDef{
										Cerr: classifyError(
											errors.Wrap(ErrDocumentNotFound, "doc was a tombstone")),
										CanStillCommit:    true,
										ShouldNotRetry:    true,
										ShouldNotRollback: false,
										Reason:            ErrorReasonTransactionFailed,
									}))
									return
								}

								cb(&GetResult{
									agent:          agent,
									scopeName:      scopeName,
									collectionName: collectionName,
									key:            key,
									Value:          doc.Body,
									Cas:            doc.Cas,
									Meta:           docMeta,
								}, nil)
							})
						})
				})
		})
}

func (t *transactionAttempt) fetchDocWithMeta(
	agent *gocbcore.Agent,
	scopeName string,
	collectionName string,
	key []byte,
	forceNonFatal bool,
	cb func(*getDoc, *TransactionOperationFailedError),
) {
	ecCb := func(doc *getDoc, cerr *classifiedError) {
		if cerr == nil {
			cb(doc, nil)
			return
		}

		switch cerr.Class {
		case ErrorClassFailDocNotFound:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					errors.Wrap(ErrDocumentNotFound, "doc was not found")),
				CanStillCommit:    true,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailTransient:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				CanStillCommit:    forceNonFatal,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		case ErrorClassFailHard:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				CanStillCommit:    forceNonFatal,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            ErrorReasonTransactionFailed,
			}))
		default:
			cb(nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				CanStillCommit:    forceNonFatal,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            ErrorReasonTransactionFailed,
			}))
		}

	}

	t.hooks.BeforeDocGet(key, func(err error) {
		if err != nil {
			ecCb(nil, classifyHookError(err))
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
				ecCb(nil, classifyError(err))
				return
			}

			if result.Ops[0].Err != nil {
				ecCb(nil, classifyError(result.Ops[0].Err))
				return
			}

			var meta *docMeta
			if err := json.Unmarshal(result.Ops[0].Value, &meta); err != nil {
				ecCb(nil, classifyError(err))
				return
			}

			var txnMeta *jsonTxnXattr
			if result.Ops[1].Err == nil {
				// Doc is currently in a txn.
				var txnMetaVal jsonTxnXattr
				if err := json.Unmarshal(result.Ops[1].Value, &txnMetaVal); err != nil {
					ecCb(nil, classifyError(err))
					return
				}

				txnMeta = &txnMetaVal
			}

			var docBody []byte
			if result.Ops[2].Err == nil {
				docBody = result.Ops[2].Value
			}

			ecCb(&getDoc{
				Body:    docBody,
				TxnMeta: txnMeta,
				DocMeta: meta,
				Cas:     result.Cas,
				Deleted: result.Internal.IsDeleted,
			}, nil)
		})
		if err != nil {
			ecCb(nil, classifyError(err))
		}
	})
}
