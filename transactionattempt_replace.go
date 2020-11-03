package transactions

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
)

func (t *transactionAttempt) Replace(opts ReplaceOptions, cb StoreCallback) error {
	return t.replace(opts, func(result *GetResult, err error) {
		var tErr *TransactionOperationFailedError
		if errors.As(err, &tErr) {
			if tErr.shouldNotRollback {
				t.addCleanupRequest(t.createCleanUpRequest())
			}
		}

		cb(result, err)
	})
}

func (t *transactionAttempt) replace(opts ReplaceOptions, cb StoreCallback) error {
	if err := t.checkDone(); err != nil {
		ec := t.classifyError(err)
		return t.createAndStashOperationFailedError(false, false, err, ErrorReasonTransactionFailed, ec, false)
	}

	if err := t.checkError(); err != nil {
		return err
	}

	t.checkExpired(hookReplace, opts.Document.key, func(err error) {
		if err != nil {
			t.expiryOvertimeMode = true
			cb(nil, t.createAndStashOperationFailedError(false, false, ErrAttemptExpired, ErrorReasonTransactionExpired, ErrorClassFailExpiry, false))
			return
		}

		agent := opts.Document.agent
		key := opts.Document.key

		t.writeWriteConflictPoll(opts.Document, forwardCompatStageWWCReplacing, func(err error) {
			if err != nil {
				cb(nil, err)
				return
			}

			t.confirmATRPending(agent, key, func(err error) {
				if err != nil {
					cb(nil, err)
					return
				}

				t.doReplace(opts, func(stagedInfo *stagedMutation, err error) {
					if err != nil {
						var failErr error
						ec := t.classifyError(err)
						switch ec {
						case ErrorClassFailExpiry:
							t.expiryOvertimeMode = true
							failErr = t.createAndStashOperationFailedError(false, false, ErrAttemptExpired, ErrorReasonTransactionExpired, ec, false)
						case ErrorClassFailDocNotFound:
							failErr = t.createAndStashOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec, false)
						case ErrorClassFailDocAlreadyExists:
							failErr = t.createAndStashOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ErrorClassFailCasMismatch, false)
						case ErrorClassFailCasMismatch:
							failErr = t.createAndStashOperationFailedError(true, false, err, ErrorReasonTransactionFailed, ec, false)
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
					t.lock.Lock()

					idx, existingMutation := t.getStagedMutationLocked(agent.BucketName(), opts.Document.scopeName, opts.Document.collectionName,
						opts.Document.key)
					if existingMutation == nil {
						t.stagedMutations = append(t.stagedMutations, stagedInfo)
					} else {
						if existingMutation.OpType == StagedMutationReplace {
							t.stagedMutations[idx] = stagedInfo
						} else if existingMutation.OpType == StagedMutationInsert {
							stagedInfo.OpType = StagedMutationInsert
							t.stagedMutations = append(t.stagedMutations[:idx+copy(t.stagedMutations[idx:], t.stagedMutations[idx+1:])], stagedInfo)
						}

					}
					t.lock.Unlock()

					cb(&GetResult{
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
	})

	return nil
}

func (t *transactionAttempt) doReplace(opts ReplaceOptions, cb func(*stagedMutation, error)) {
	agent := opts.Document.agent
	scopeName := opts.Document.scopeName
	collectionName := opts.Document.collectionName
	key := opts.Document.key

	t.hooks.BeforeStagedReplace(key, func(err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		stagedInfo := &stagedMutation{
			OpType:         StagedMutationReplace,
			Agent:          agent,
			ScopeName:      scopeName,
			CollectionName: collectionName,
			Key:            key,
			Staged:         opts.Value,
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
			cb(nil, err)
			return
		}

		var duraTimeout time.Duration
		var deadline time.Time
		if t.operationTimeout > 0 {
			deadline = time.Now().Add(t.operationTimeout)
			duraTimeout = t.operationTimeout * 10 / 9
		}

		flags := memd.SubdocDocFlagAccessDeleted

		_, err = stagedInfo.Agent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      stagedInfo.ScopeName,
			CollectionName: stagedInfo.CollectionName,
			Key:            stagedInfo.Key,
			Cas:            opts.Document.Cas,
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
				cb(nil, err)
				return
			}

			t.hooks.AfterStagedReplaceComplete(key, func(err error) {
				if err != nil {
					cb(nil, err)
					return
				}

				stagedInfo.Cas = result.Cas
				cb(stagedInfo, nil)
			})
		})
		if err != nil {
			cb(nil, err)
			return
		}
	})
}
