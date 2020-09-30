package transactions

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
	"time"
)

func (t *transactionAttempt) Remove(opts RemoveOptions, cb StoreCallback) error {
	if err := t.checkDone(); err != nil {
		ec := t.classifyError(err)
		return t.createAndStashOperationFailedError(false, false, ErrAttemptExpired, ErrorReasonTransactionExpired, ec, false)
	}

	if err := t.checkError(); err != nil {
		return err
	}

	t.checkExpired(hookRemove, opts.Document.key, func(err error) {
		if err != nil {
			t.expiryOvertimeMode = true
			cb(nil, t.createAndStashOperationFailedError(false, false, ErrAttemptExpired, ErrorReasonTransactionExpired, ErrorClassFailExpiry, false))
			return
		}

		agent := opts.Document.agent
		scopeName := opts.Document.scopeName
		collectionName := opts.Document.collectionName
		key := opts.Document.key

		t.lock.Lock()

		_, existingMutation := t.getStagedMutationLocked(agent.BucketName(), opts.Document.scopeName, opts.Document.collectionName,
			opts.Document.key)
		if existingMutation != nil && existingMutation.OpType == StagedMutationInsert {
			t.lock.Unlock()
			cb(nil, t.createAndStashOperationFailedError(false, false, ErrIllegalState, ErrorReasonTransactionFailed, ErrorClassFailOther, false))
			return
		}
		t.lock.Unlock()

		err = t.confirmATRPending(agent, scopeName, collectionName, key, func(err error) {
			if err != nil {
				cb(nil, err)
				return
			}

			t.remove(opts.Document, func(res *GetResult, err error) {
				if err != nil {
					var failErr error
					ec := t.classifyError(err)
					switch ec {
					case ErrorClassFailExpiry:
						t.expiryOvertimeMode = true
						failErr = t.createAndStashOperationFailedError(false, false, ErrAttemptExpired, ErrorReasonTransactionExpired, ec, false)
					case ErrorClassFailDocNotFound:
						failErr = t.createAndStashOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec, false)
					case ErrorClassFailCasMismatch:
						failErr = t.createAndStashOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec, false)
					case ErrorClassFailDocAlreadyExists:
						failErr = t.createAndStashOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ErrorClassFailCasMismatch, false)
					case ErrorClassFailTransient:
						failErr = t.createAndStashOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec, false)
					case ErrorClassFailAmbiguous:
						failErr = t.createAndStashOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec, false)
					case ErrorClassFailHard:
						failErr = t.createAndStashOperationFailedError(false, true, err, ErrorReasonTransactionFailed, ec, false)
					default:
						failErr = t.createAndStashOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec, false)
					}

					cb(nil, failErr)
					return
				}

				cb(res, nil)
			})
		})
		if err != nil {
			cb(nil, err)
			return
		}
	})

	return nil
}

func (t *transactionAttempt) remove(doc *GetResult, cb StoreCallback) {
	t.hooks.BeforeStagedRemove(doc.key, func(err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		stagedInfo := &stagedMutation{
			OpType:         StagedMutationRemove,
			Agent:          doc.agent,
			ScopeName:      doc.scopeName,
			CollectionName: doc.collectionName,
			Key:            doc.key,
			IsTombstone:    true,
		}

		var txnMeta jsonTxnXattr
		txnMeta.ID.Transaction = t.transactionID
		txnMeta.ID.Attempt = t.id
		txnMeta.ATR.CollectionName = t.atrCollectionName
		txnMeta.ATR.ScopeName = t.atrScopeName
		txnMeta.ATR.BucketName = t.atrAgent.BucketName()
		txnMeta.ATR.DocID = string(t.atrKey)
		txnMeta.Operation.Type = jsonMutationRemove

		restore := struct {
			OriginalCAS string
			ExpiryTime  uint
			RevID       string
		}{
			OriginalCAS: fmt.Sprintf("%d", doc.Cas),
			ExpiryTime:  doc.Meta.Expiry,
			RevID:       doc.Meta.RevID,
		}
		txnMeta.Restore = (*struct {
			OriginalCAS string `json:"CAS,omitempty"`
			ExpiryTime  uint   `json:"exptime"`
			RevID       string `json:"revid,omitempty"`
		})(&restore)

		txnMetaBytes, _ := json.Marshal(txnMeta)
		// TODO(brett19): Don't ignore the error here.

		var duraTimeout time.Duration
		var deadline time.Time
		if t.keyValueTimeout > 0 {
			deadline = time.Now().Add(t.keyValueTimeout)
			duraTimeout = t.keyValueTimeout * 10 / 9
		}

		flags := memd.SubdocDocFlagNone
		if doc.Meta.Deleted {
			flags = memd.SubdocDocFlagAccessDeleted
		}

		_, err = stagedInfo.Agent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      stagedInfo.ScopeName,
			CollectionName: stagedInfo.CollectionName,
			Key:            stagedInfo.Key,
			Cas:            doc.Cas,
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
					Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
					Value: crc32cMacro,
				},
			},
			Flags:                  flags,
			DurabilityLevel:        durabilityLevelToMemd(t.durabilityLevel),
			DurabilityLevelTimeout: duraTimeout,
			Deadline:               deadline,
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				cb(nil, err)
				return
			}

			t.hooks.AfterStagedRemoveComplete(doc.key, func(err error) {
				if err != nil {
					cb(nil, err)
					return
				}

				t.lock.Lock()
				stagedInfo.Cas = result.Cas
				t.stagedMutations = append(t.stagedMutations, stagedInfo)
				t.lock.Unlock()

				cb(&GetResult{
					agent:          stagedInfo.Agent,
					scopeName:      stagedInfo.ScopeName,
					collectionName: stagedInfo.CollectionName,
					key:            stagedInfo.Key,
					Value:          stagedInfo.Staged,
					Cas:            result.Cas,
				}, nil)
			})
		})
		if err != nil {
			cb(nil, err)
		}
	})
}
