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
	txnOpSection  atomicWaitQueue
}

func (t *transactionAttempt) atrCollName() string {
	return t.atrScopeName + "." + t.atrCollectionName
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

	var duraTimeout time.Duration
	var deadline time.Time
	if t.keyValueTimeout > 0 {
		deadline = time.Now().Add(t.keyValueTimeout)
		duraTimeout = t.keyValueTimeout * 10 / 9
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
		DurabilityLevel:        memd.DurabilityLevel(t.durabilityLevel),
		DurabilityLevelTimeout: duraTimeout,
		Deadline:               deadline,
		Flags:                  memd.SubdocDocFlagMkDoc,
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

func (t *transactionAttempt) setATRCommitted(
	cb func(error),
) error {
	t.txnOpSection.Wait(func() {

		t.lock.Lock()
		if t.state != attemptStatePending {
			t.lock.Unlock()

			t.txnAtrSection.Wait(func() {
				cb(nil)
			})

			return
		}

		atrAgent := t.atrAgent
		atrScopeName := t.atrScopeName
		atrKey := t.atrKey
		atrCollectionName := t.atrCollectionName

		t.state = attemptStateCommitted

		var insMutations []jsonAtrMutation
		var repMutations []jsonAtrMutation
		var remMutations []jsonAtrMutation

		for _, mutation := range t.stagedMutations {
			jsonMutation := jsonAtrMutation{
				BucketName:     "",
				ScopeName:      "",
				CollectionName: mutation.CollectionName,
				DocID:          string(mutation.Key),
			}

			if mutation.OpType == stagedMutationInsert {
				insMutations = append(insMutations, jsonMutation)
			} else if mutation.OpType == stagedMutationReplace {
				repMutations = append(repMutations, jsonMutation)
			} else if mutation.OpType == stagedMutationRemove {
				remMutations = append(remMutations, jsonMutation)
			} else {
				// TODO(brett19): Signal an error here
			}
		}

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

		_, err := atrAgent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      atrScopeName,
			CollectionName: atrCollectionName,
			Key:            atrKey,
			Ops: []gocbcore.SubDocOp{
				atrFieldOp("st", jsonAtrStateCommitted, 0),
				atrFieldOp("tsc", "${Mutation.CAS}", 0),
				atrFieldOp("ins", insMutations, 0),
				atrFieldOp("rep", repMutations, 0),
				atrFieldOp("rem", remMutations, 0),
				atrFieldOp("p", 0, 0),
			},
			DurabilityLevel: memd.DurabilityLevel(t.durabilityLevel),
			Flags:           memd.SubdocDocFlagNone,
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

			cb(err)
		}

	})
	return nil
}

func (t *transactionAttempt) setATRCompleted(
	cb func(error),
) error {
	t.lock.Lock()
	if t.state != attemptStateCommitted {
		t.lock.Unlock()

		t.txnAtrSection.Wait(func() {
			cb(nil)
		})

		return nil
	}

	atrAgent := t.atrAgent
	atrScopeName := t.atrScopeName
	atrKey := t.atrKey
	atrCollectionName := t.atrCollectionName

	t.state = attemptStateCompleted

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

	_, err := atrAgent.MutateIn(gocbcore.MutateInOptions{
		ScopeName:      atrScopeName,
		CollectionName: atrCollectionName,
		Key:            atrKey,
		Ops: []gocbcore.SubDocOp{
			atrFieldOp("st", jsonAtrStateCompleted, 0),
			atrFieldOp("tsco", "${Mutation.CAS}", 0),
		},
		DurabilityLevel: memd.DurabilityLevel(t.durabilityLevel),
		Flags:           memd.SubdocDocFlagNone,
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

		cb(err)
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

	var deadline time.Time
	if t.keyValueTimeout > 0 {
		deadline = time.Now().Add(t.keyValueTimeout)
	}

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
		Deadline: deadline,
	}, func(result *gocbcore.LookupInResult, err error) {
		if errors.Is(err, gocbcore.ErrDocumentNotFound) {
			cb(nil, ErrDocNotFound)
			return
		}

		var docMeta struct {
			Cas        string `json:"CAS"`
			RevID      string `json:"rev"`
			Expiration uint   `json:"expiration"`
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
			getTxnState := func(cb func(jsonAtrState, error)) {
				if txnMeta.ID.Attempt != t.id {
					_, err := opts.Agent.LookupIn(gocbcore.LookupInOptions{
						ScopeName:      opts.ScopeName,
						CollectionName: opts.CollectionName,
						Key:            []byte(txnMeta.ATR.DocID),
						Ops: []gocbcore.SubDocOp{
							{
								Op:    memd.SubDocOpGet,
								Path:  "attempts." + t.id + ".st",
								Flags: 0,
							},
						},
					}, func(result *gocbcore.LookupInResult, err error) {
						if err != nil {
							if errors.Is(err, gocbcore.ErrDocumentNotFound) {
								cb(jsonAtrStateCommitted, ErrAtrNotFound)
								return
							}

							cb(jsonAtrStateCommitted, err)
							return
						}

						err = result.Ops[0].Err
						if err != nil {
							if errors.Is(err, gocbcore.ErrPathNotFound) {
								// TODO(brett19): Discuss with Graham if this is correct.
								cb(jsonAtrStateCommitted, nil)
								return
							}

							cb(jsonAtrStateCommitted, err)
							return
						}

						// TODO(brett19): Don't ignore the error here.
						var txnState jsonAtrState
						json.Unmarshal(result.Ops[0].Value, &txnState)

						cb(txnState, nil)
					})
					if err != nil {
						cb(jsonAtrStateCommitted, err)
						return
					}
				} else {
					cb(jsonAtrStateCommitted, nil)
					return
				}
			}

			getTxnState(func(state jsonAtrState, err error) {
				if state == jsonAtrStateCommitted {
					// TODO(brett19): Discuss virtual CAS with Graham
					cb(&GetResult{
						agent:          opts.Agent,
						scopeName:      opts.ScopeName,
						collectionName: opts.CollectionName,
						key:            opts.Key,
						Value:          txnMeta.Operation.Staged,
						Cas:            docCas,
					}, nil)
				} else if txnMeta.Operation.Type == jsonMutationRemove {
					cb(nil, ErrDocNotFound)
				} else {
					cb(nil, ErrOther)
				}
			})
			return
		}

		cb(&GetResult{
			agent:          opts.Agent,
			scopeName:      opts.ScopeName,
			collectionName: opts.CollectionName,
			key:            opts.Key,
			Value:          docBytes,
			Cas:            docCas,
			revid:          docMeta.RevID,
			expiry:         docMeta.Expiration,
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
		txnMeta.ATR.CollectionName = t.atrCollName()
		txnMeta.ATR.BucketName = "" // TODO(brett19): Need the bucket name.
		txnMeta.ATR.DocID = string(t.atrKey)
		txnMeta.Operation.Type = jsonMutationInsert
		txnMeta.Operation.Staged = stagedInfo.Staged

		txnMetaBytes, _ := json.Marshal(txnMeta)
		// TODO(brett19): Don't ignore the error here.

		var duraTimeout time.Duration
		var deadline time.Time
		if t.keyValueTimeout > 0 {
			deadline = time.Now().Add(t.keyValueTimeout)
			duraTimeout = t.keyValueTimeout * 10 / 9
		}

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
			DurabilityLevel:        memd.DurabilityLevel(t.durabilityLevel),
			DurabilityLevelTimeout: duraTimeout,
			Deadline:               deadline,
			Flags:                  memd.SubdocDocFlagAddDoc,
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
		txnMeta.ATR.CollectionName = t.atrCollName()
		txnMeta.ATR.BucketName = "" // TODO(brett19): Need the bucket name.
		txnMeta.ATR.DocID = string(t.atrKey)
		txnMeta.Operation.Type = jsonMutationReplace
		txnMeta.Operation.Staged = stagedInfo.Staged
		restore := struct {
			OriginalCAS uint64
			ExpiryTime  uint
			RevID       string
		}{
			OriginalCAS: uint64(opts.Document.Cas),
			ExpiryTime:  opts.Document.expiry,
			RevID:       opts.Document.revid,
		}
		txnMeta.Restore = (*struct {
			OriginalCAS uint64 `json:"cas,omitempty"`
			ExpiryTime  uint   `json:"exptime,omitempty"`
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
			Flags:                  memd.SubdocDocFlagNone,
			DurabilityLevel:        memd.DurabilityLevel(t.durabilityLevel),
			DurabilityLevelTimeout: duraTimeout,
			Deadline:               deadline,
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
		txnMeta.ATR.CollectionName = t.atrCollName()
		txnMeta.ATR.BucketName = "" // TODO(brett19): Need the bucket name.
		txnMeta.ATR.DocID = string(t.atrKey)
		txnMeta.Operation.Type = jsonMutationRemove
		txnMeta.Operation.Staged = stagedInfo.Staged

		txnMetaBytes, _ := json.Marshal(txnMeta)
		// TODO(brett19): Don't ignore the error here.

		var duraTimeout time.Duration
		var deadline time.Time
		if t.keyValueTimeout > 0 {
			deadline = time.Now().Add(t.keyValueTimeout)
			duraTimeout = t.keyValueTimeout * 10 / 9
		}

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
			Flags:                  memd.SubdocDocFlagNone,
			DurabilityLevel:        memd.DurabilityLevel(t.durabilityLevel),
			DurabilityLevelTimeout: duraTimeout,
			Deadline:               deadline,
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

func (t *transactionAttempt) unstageInsRepMutation(mutation stagedMutation, cb func(error)) {
	if mutation.OpType != stagedMutationInsert && mutation.OpType != stagedMutationReplace {
		cb(ErrUhOh)
		return
	}

	_, err := mutation.Agent.Replace(gocbcore.ReplaceOptions{
		ScopeName:      mutation.ScopeName,
		CollectionName: mutation.CollectionName,
		Key:            mutation.Key,
		Cas:            mutation.Cas,
		Flags:          (2 << 24),
		Datatype:       0,
		Value:          mutation.Staged,
	}, func(result *gocbcore.StoreResult, err error) {
		if err != nil {
			cb(err)
			return
		}

		cb(nil)
	})
	if err != nil {
		cb(err)
		return
	}

	cb(nil)
}

func (t *transactionAttempt) unstageRemMutation(mutation stagedMutation, cb func(error)) {
	if mutation.OpType != stagedMutationRemove {
		cb(ErrUhOh)
		return
	}

	_, err := mutation.Agent.Delete(gocbcore.DeleteOptions{
		ScopeName:      mutation.ScopeName,
		CollectionName: mutation.CollectionName,
		Key:            mutation.Key,
		Cas:            mutation.Cas,
	}, func(result *gocbcore.DeleteResult, err error) {
		if err != nil {
			cb(err)
			return
		}

		cb(nil)
	})
	if err != nil {
		cb(err)
		return
	}

	cb(nil)
}

func (t *transactionAttempt) Commit(cb CommitCallback) error {
	// TODO(brett19): Move the wait logic from setATRCommitted to here
	t.setATRCommitted(func(err error) {
		if err != nil {
			cb(err)
			return
		}

		// TODO(brett19): Use atomic counters instead of a goroutine here
		go func() {
			numMutations := len(t.stagedMutations)
			waitCh := make(chan error, numMutations)

			for _, mutation := range t.stagedMutations {
				if mutation.OpType == stagedMutationInsert || mutation.OpType == stagedMutationReplace {
					t.unstageInsRepMutation(*mutation, func(err error) {
						waitCh <- err
					})
				} else if mutation.OpType == stagedMutationRemove {
					t.unstageRemMutation(*mutation, func(err error) {
						waitCh <- err
					})
				} else {
					// TODO(brett19): Pretty sure I can do better than this
					waitCh <- ErrUhOh
				}
			}

			for i := 0; i < numMutations; i++ {
				// TODO(brett19): Handle errors here better
				<-waitCh
			}

			t.setATRCompleted(func(err error) {
				if err != nil {
					cb(err)
					return
				}

				cb(nil)
			})
		}()
	})
	return nil
}

func (t *transactionAttempt) Rollback(cb RollbackCallback) error {
	go func() {
		cb(nil)
	}()

	return nil
}
