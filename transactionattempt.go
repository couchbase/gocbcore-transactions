package transactions

import (
	"bytes"
	"encoding/json"
	"errors"
	"sync"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
)

type attemptState int

const (
	attemptStateNothingWritten = attemptState(1)
	attemptStatePending        = attemptState(2)
	attemptStateCommitted      = attemptState(3)
	attemptStateCompleted      = attemptState(4)
	attemptStateAborted        = attemptState(5)
	attemptStateRolledBack     = attemptState(6)
)

type stagedMutationType int

const (
	stagedMutationInsert  = stagedMutationType(1)
	stagedMutationReplace = stagedMutationType(2)
	stagedMutationRemove  = stagedMutationType(3)
)

type stagedMutation struct {
	OpType         stagedMutationType
	Agent          *gocbcore.Agent
	ScopeName      string
	CollectionName string
	Key            []byte
	Cas            gocbcore.Cas
	Staged         json.RawMessage
}

type transactionAttempt struct {
	// immutable state
	expiryTime      time.Time
	keyValueTimeout time.Duration
	durabilityLevel DurabilityLevel
	transactionID   string
	id              string

	// mutable state
	state               attemptState
	stagedMutations     []*stagedMutation
	finalMutationTokens []gocbcore.MutationToken
	atrAgent            *gocbcore.Agent
	atrScopeName        string
	atrCollectionName   string
	atrKey              []byte
	expiryOvertimeMode  bool

	lock          sync.Mutex
	txnAtrSection atomicWaitQueue
}

func (t *transactionAttempt) checkDone() error {
	t.lock.Lock()
	defer t.lock.Unlock()

	if t.state != attemptStateNothingWritten && t.state != attemptStatePending {
		return ErrOther
	}

	return nil
}

func (t *transactionAttempt) checkExpired() error {
	if time.Now().After(t.expiryTime) {
		return ErrAttemptExpired
	}
	return nil
}

func (t *transactionAttempt) confirmATRPending(
	agent *gocbcore.Agent,
	scopeName string,
	collectionName string,
	firstKey []byte,
	cb func(error),
) error {
	t.lock.Lock()
	if t.state != attemptStateNothingWritten {
		t.lock.Unlock()

		t.txnAtrSection.Wait(func() {
			cb(nil)
		})

		return nil
	}

	atrID := int(cbcVbMap(firstKey, 1024))
	atrKey := []byte(atrIDList[atrID])

	t.atrAgent = agent
	t.atrScopeName = scopeName
	t.atrCollectionName = collectionName
	t.atrKey = atrKey

	t.state = attemptStatePending

	t.txnAtrSection.Add(1)
	t.lock.Unlock()

	atrFieldOp := func(fieldName string, data interface{}, flags memd.SubdocFlag) gocbcore.SubDocOp {
		bytes, _ := json.Marshal(data)

		return gocbcore.SubDocOp{
			Op:    memd.SubDocOpDictSet,
			Flags: memd.SubdocFlagMkDirP | flags,
			Path:  "attempts." + t.id + "." + fieldName,
			Value: bytes,
		}
	}

	_, err := agent.MutateIn(gocbcore.MutateInOptions{
		ScopeName:      scopeName,
		CollectionName: collectionName,
		Key:            atrKey,
		Ops: []gocbcore.SubDocOp{
			atrFieldOp("tst", "${Mutation.CAS}", 0),
			atrFieldOp("tid", t.transactionID, 0),
			atrFieldOp("st", jsonAtrStatePending, 0),
			atrFieldOp("exp", t.expiryTime.Sub(time.Now()), 0),
		},
		DurabilityLevel: memd.DurabilityLevel(t.durabilityLevel),
		Flags:           memd.SubdocDocFlagMkDoc,
	}, func(result *gocbcore.MutateInResult, err error) {
		if err != nil {
			t.lock.Lock()

			// TODO(brett19): Do other things to cancel it here....
			t.state = attemptStateAborted

			t.txnAtrSection.Done()
			t.lock.Unlock()

			cb(err)
			return
		}

		t.lock.Lock()
		t.txnAtrSection.Done()
		t.lock.Unlock()

		cb(nil)
	})
	if err != nil {
		t.lock.Lock()

		// TODO(brett19): Do other things to cancel it here....
		t.state = attemptStateAborted

		t.txnAtrSection.Done()
		t.lock.Unlock()

		return err
	}

	return nil
}

func (t *transactionAttempt) handleError(err error) {
	t.lock.Lock()

	// TODO(brett19): Do other things to cancel it here....
	t.state = attemptStateAborted

	t.lock.Unlock()
}

func (t *transactionAttempt) getStagedMutationLocked(bucketName, scopeName, collectionName string, key []byte) *stagedMutation {
	for _, mutation := range t.stagedMutations {
		// TODO(brett19): Need to check the bucket names here
		//if mutation.BucketName == bucketName &&
		if mutation.ScopeName == scopeName &&
			mutation.CollectionName == collectionName &&
			bytes.Compare(mutation.Key, key) == 0 {
			return mutation
		}
	}

	return nil
}

func (t *transactionAttempt) Get(opts GetOptions, cb GetCallback) error {
	if err := t.checkDone(); err != nil {
		return err
	}

	if err := t.checkExpired(); err != nil {
		return err
	}

	t.lock.Lock()

	// TODO(brett19): Use the bucket name below
	existingMutation := t.getStagedMutationLocked("", opts.ScopeName, opts.CollectionName, opts.Key)
	if existingMutation != nil {
		if existingMutation.OpType == stagedMutationInsert || existingMutation.OpType == stagedMutationReplace {
			getRes := &GetResult{
				agent:          existingMutation.Agent,
				scopeName:      existingMutation.ScopeName,
				collectionName: existingMutation.CollectionName,
				key:            existingMutation.Key,
				Value:          existingMutation.Staged,
				Cas:            existingMutation.Cas,
			}

			t.lock.Unlock()
			cb(getRes, nil)
			return nil
		} else if existingMutation.OpType == stagedMutationRemove {
			t.lock.Unlock()
			cb(nil, ErrDocNotFound)
			return nil
		}
	}

	t.lock.Unlock()

	_, err := opts.Agent.LookupIn(gocbcore.LookupInOptions{
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
	}, func(result *gocbcore.LookupInResult, err error) {
		if errors.Is(err, gocbcore.ErrDocumentNotFound) {
			cb(nil, ErrDocNotFound)
			return
		}

		var docMeta struct {
			Cas string `json:"CAS"`
		}
		// TODO(brett19): Don't ignore the error here
		json.Unmarshal(result.Ops[0].Value, &docMeta)

		var txnMeta *jsonTxnXattr
		if result.Ops[1].Err == nil {
			var txnMetaVal jsonTxnXattr
			// TODO(brett19): Don't ignore the error here
			json.Unmarshal(result.Ops[1].Value, &txnMetaVal)
			txnMeta = &txnMetaVal
		}

		docBytes := result.Ops[2].Value
		docCas := result.Cas

		if txnMeta != nil {
			// Default to committed in the case that this attempt matches
			// the attempt marker in the transaction meta-data.
			txnAtrState := jsonAtrStateCommitted

		}

		cb(&GetResult{
			agent:          opts.Agent,
			scopeName:      opts.ScopeName,
			collectionName: opts.CollectionName,
			key:            opts.Key,
			Value:          docBytes,
			Cas:            docCas,
		}, nil)
	})
	if err != nil {
		return err
	}

	return nil
}

func (t *transactionAttempt) Insert(opts InsertOptions, cb StoreCallback) error {
	if err := t.checkDone(); err != nil {
		return err
	}

	if err := t.checkExpired(); err != nil {
		return err
	}

	err := t.confirmATRPending(opts.Agent, opts.ScopeName, opts.CollectionName, opts.Key, func(err error) {
		if err != nil {
			t.handleError(err)
			cb(nil, err)
			return
		}

		stagedInfo := &stagedMutation{
			OpType:         stagedMutationInsert,
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
		txnMeta.ATR.BucketName = "" // TODO(brett19): Need the bucket name.
		txnMeta.ATR.DocID = string(t.atrKey)
		txnMeta.Operation.Type = jsonMutationInsert
		txnMeta.Operation.Staged = stagedInfo.Staged

		txnMetaBytes, _ := json.Marshal(txnMeta)
		// TODO(brett19): Don't ignore the error here.

		_, err = stagedInfo.Agent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      stagedInfo.ScopeName,
			CollectionName: stagedInfo.CollectionName,
			Key:            stagedInfo.Key,
			Ops: []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpDictAdd,
					Path:  "txn",
					Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath,
					Value: txnMetaBytes,
				},
			},
			DurabilityLevel: memd.DurabilityLevel(t.durabilityLevel),
			Flags:           memd.SubdocDocFlagAddDoc,
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				t.handleError(err)
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
			}, err)
		})
		if err != nil {
			t.handleError(err)
			cb(nil, err)
		}
	})
	if err != nil {
		t.handleError(err)
		return err
	}

	return nil
}

func (t *transactionAttempt) Replace(opts ReplaceOptions, cb StoreCallback) error {
	if err := t.checkDone(); err != nil {
		return err
	}

	if err := t.checkExpired(); err != nil {
		return err
	}

	agent := opts.Document.agent
	scopeName := opts.Document.scopeName
	collectionName := opts.Document.collectionName
	key := opts.Document.key
	err := t.confirmATRPending(agent, scopeName, collectionName, key, func(err error) {
		if err != nil {
			t.handleError(err)
			cb(nil, err)
			return
		}

		stagedInfo := &stagedMutation{
			OpType:         stagedMutationReplace,
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
		txnMeta.ATR.BucketName = "" // TODO(brett19): Need the bucket name.
		txnMeta.ATR.DocID = string(t.atrKey)
		txnMeta.Operation.Type = jsonMutationReplace
		txnMeta.Operation.Staged = stagedInfo.Staged

		txnMetaBytes, _ := json.Marshal(txnMeta)
		// TODO(brett19): Don't ignore the error here.

		_, err = stagedInfo.Agent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      stagedInfo.ScopeName,
			CollectionName: stagedInfo.CollectionName,
			Key:            stagedInfo.Key,
			Cas:            opts.Document.Cas,
			Ops: []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpDictAdd,
					Path:  "txn",
					Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath,
					Value: txnMetaBytes,
				},
			},
			DurabilityLevel: memd.DurabilityLevel(t.durabilityLevel),
			Flags:           memd.SubdocDocFlagAddDoc,
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				t.handleError(err)
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
			}, err)
		})
		if err != nil {
			t.handleError(err)
			cb(nil, err)
		}
	})
	if err != nil {
		t.handleError(err)
		return err
	}

	return nil
}

func (t *transactionAttempt) Remove(opts RemoveOptions, cb StoreCallback) error {
	if err := t.checkDone(); err != nil {
		return err
	}

	if err := t.checkExpired(); err != nil {
		return err
	}

	agent := opts.Document.agent
	scopeName := opts.Document.scopeName
	collectionName := opts.Document.collectionName
	key := opts.Document.key
	err := t.confirmATRPending(agent, scopeName, collectionName, key, func(err error) {
		if err != nil {
			t.handleError(err)
			cb(nil, err)
			return
		}

		stagedInfo := &stagedMutation{
			OpType:         stagedMutationRemove,
			Agent:          agent,
			ScopeName:      scopeName,
			CollectionName: collectionName,
			Key:            key,
		}

		var txnMeta jsonTxnXattr
		txnMeta.ID.Transaction = t.transactionID
		txnMeta.ID.Attempt = t.id
		txnMeta.ATR.CollectionName = t.atrCollectionName
		txnMeta.ATR.BucketName = "" // TODO(brett19): Need the bucket name.
		txnMeta.ATR.DocID = string(t.atrKey)
		txnMeta.Operation.Type = jsonMutationRemove
		txnMeta.Operation.Staged = stagedInfo.Staged

		txnMetaBytes, _ := json.Marshal(txnMeta)
		// TODO(brett19): Don't ignore the error here.

		_, err = stagedInfo.Agent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      stagedInfo.ScopeName,
			CollectionName: stagedInfo.CollectionName,
			Key:            stagedInfo.Key,
			Cas:            opts.Document.Cas,
			Ops: []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpDictAdd,
					Path:  "txn",
					Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath,
					Value: txnMetaBytes,
				},
			},
			DurabilityLevel: memd.DurabilityLevel(t.durabilityLevel),
			Flags:           memd.SubdocDocFlagAddDoc,
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				t.handleError(err)
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
			}, err)
		})
		if err != nil {
			t.handleError(err)
			cb(nil, err)
		}
	})
	if err != nil {
		t.handleError(err)
		return err
	}

	return nil
}

func (t *transactionAttempt) Commit(cb CommitCallback) error {
	go func() {
		cb(nil)
	}()

	return nil
}

func (t *transactionAttempt) Rollback(cb RollbackCallback) error {
	go func() {
		cb(nil)
	}()

	return nil
}
