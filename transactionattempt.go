package transactions

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
)

// AttemptState represents the current state of a transaction
type AttemptState int

const (
	// AttemptStateNothingWritten indicates that nothing has been written yet.
	AttemptStateNothingWritten = AttemptState(1)

	// AttemptStatePending indicates that the transaction ATR has been written and
	// the transaction is currently pending.
	AttemptStatePending = AttemptState(2)

	// AttemptStateCommitted indicates that the transaction is now logically committed
	// but the unstaging of documents is still underway.
	AttemptStateCommitted = AttemptState(3)

	// AttemptStateCompleted indicates that the transaction has been fully completed
	// and no longer has work to perform.
	AttemptStateCompleted = AttemptState(4)

	// AttemptStateAborted indicates that the transaction was aborted.
	AttemptStateAborted = AttemptState(5)

	// AttemptStateRolledBack indicates that the transaction was not committed and instead
	// was rolled back in its entirety.
	AttemptStateRolledBack = AttemptState(6)
)

var crc32cMacro = []byte("\"${Mutation.value_crc32c}\"")

type transactionAttempt struct {
	// immutable state
	expiryTime      time.Time
	keyValueTimeout time.Duration
	durabilityLevel DurabilityLevel
	transactionID   string
	id              string
	hooks           TransactionHooks

	// mutable state
	state               AttemptState
	stagedMutations     []*stagedMutation
	finalMutationTokens []MutationToken
	atrAgent            *gocbcore.Agent
	atrScopeName        string
	atrCollectionName   string
	atrKey              []byte
	expiryOvertimeMode  bool
	shouldRetry         bool
	shouldNotRollback   bool

	lock          sync.Mutex
	txnAtrSection atomicWaitQueue
	txnOpSection  atomicWaitQueue
}

func (t *transactionAttempt) GetMutations() []StagedMutation {
	mutations := make([]StagedMutation, len(t.stagedMutations))
	for mutationIdx, mutation := range t.stagedMutations {
		mutations[mutationIdx] = StagedMutation{
			OpType:         mutation.OpType,
			BucketName:     mutation.Agent.BucketName(),
			ScopeName:      mutation.ScopeName,
			CollectionName: mutation.CollectionName,
			Key:            mutation.Key,
			Cas:            mutation.Cas,
			Staged:         mutation.Staged,
		}
	}
	return mutations
}

func (t *transactionAttempt) atrCollName() string {
	return t.atrScopeName + "." + t.atrCollectionName
}

func (t *transactionAttempt) checkDone() error {
	t.lock.Lock()
	defer t.lock.Unlock()

	if t.state != AttemptStateNothingWritten && t.state != AttemptStatePending {
		return ErrOther
	}

	return nil
}

func (t *transactionAttempt) checkExpired(stage string, id []byte, cb func(error)) {
	t.hooks.HasExpiredClientSideHook(stage, id, func(expired bool, err error) {
		if err != nil {
			cb(err)
			return
		}
		if expired {
			cb(ErrAttemptExpired)
			return
		}
		if time.Now().After(t.expiryTime) {
			cb(ErrAttemptExpired)
			return
		}

		cb(nil)
	})
}

func (t *transactionAttempt) confirmATRPending(
	agent *gocbcore.Agent,
	scopeName string,
	collectionName string,
	firstKey []byte,
	cb func(error),
) error {
	t.lock.Lock()
	if t.state != AttemptStateNothingWritten {
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

	t.state = AttemptStatePending

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

	t.hooks.BeforeATRPending(func(err error) {
		if err != nil {
			err = t.classifyError(err)
			if errors.Is(err, ErrHard) {
				t.shouldNotRollback = true
			} else if errors.Is(err, ErrTransient) {
				t.shouldRetry = true
			} else if errors.Is(err, ErrAmbiguous) {
				// need to retry
			} else if errors.Is(err, ErrPathAlreadyExists) {
				t.lock.Lock()
				t.state = AttemptStatePending
				t.lock.Unlock()
				cb(nil)
				return
			}

			cb(err)
			return
		}

		_, err = agent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      scopeName,
			CollectionName: collectionName,
			Key:            atrKey,
			Ops: []gocbcore.SubDocOp{
				atrFieldOp("tst", "${Mutation.CAS}", memd.SubdocFlagXattrPath|memd.SubdocFlagExpandMacros),
				atrFieldOp("tid", t.transactionID, memd.SubdocFlagXattrPath),
				atrFieldOp("st", jsonAtrStatePending, memd.SubdocFlagXattrPath),
				atrFieldOp("exp", t.expiryTime.Sub(time.Now())/time.Millisecond, memd.SubdocFlagXattrPath),
			},
			DurabilityLevel:        memd.DurabilityLevel(t.durabilityLevel),
			DurabilityLevelTimeout: duraTimeout,
			Deadline:               deadline,
			Flags:                  memd.SubdocDocFlagMkDoc,
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				err = t.classifyError(err)
				if errors.Is(err, ErrHard) {
					t.shouldNotRollback = true
				} else if errors.Is(err, ErrTransient) {
					t.shouldRetry = true
				}
				t.lock.Lock()
				t.txnAtrSection.Done()
				t.lock.Unlock()
				cb(err)
				return
			}

			t.lock.Lock()
			t.txnAtrSection.Done()
			t.lock.Unlock()

			t.hooks.AfterATRPending(func(err error) {
				if err != nil {
					err = t.classifyError(err)
					if errors.Is(err, ErrHard) {
						t.shouldNotRollback = true
					} else if errors.Is(err, ErrTransient) {
						t.shouldRetry = true
					}
					cb(err)
					return
				}

				cb(nil)
			})
		})
		if err != nil {
			err = t.classifyError(err)
			if errors.Is(err, ErrHard) {
				t.shouldNotRollback = true
			} else if errors.Is(err, ErrTransient) {
				t.shouldRetry = true
			}
			t.lock.Lock()
			t.txnAtrSection.Done()
			t.lock.Unlock()
			cb(err)
			return
		}
	})

	return nil
}

func (t *transactionAttempt) setATRCommitted(
	cb func(error),
) error {
	errHandler := func(err error) {
		if err == nil {
			cb(nil)
		}

		err = t.classifyError(err)
		if errors.Is(err, ErrHard) {
			t.shouldNotRollback = true
		} else if errors.Is(err, ErrTransient) {
			t.shouldRetry = true
		}

		cb(err)
	}

	t.hooks.BeforeATRCommit(func(err error) {
		if err != nil {
			errHandler(err)
			return
		}

		t.lock.Lock()
		if t.state != AttemptStatePending {
			t.lock.Unlock()

			t.txnAtrSection.Wait(func() {
				errHandler(ErrOther)
			})

			return
		}

		atrAgent := t.atrAgent
		atrScopeName := t.atrScopeName
		atrKey := t.atrKey
		atrCollectionName := t.atrCollectionName

		t.state = AttemptStateCommitted

		insMutations := []jsonAtrMutation{}
		repMutations := []jsonAtrMutation{}
		remMutations := []jsonAtrMutation{}

		for _, mutation := range t.stagedMutations {
			jsonMutation := jsonAtrMutation{
				BucketName:     mutation.Agent.BucketName(),
				ScopeName:      mutation.ScopeName,
				CollectionName: mutation.CollectionName,
				DocID:          string(mutation.Key),
			}

			if mutation.OpType == StagedMutationInsert {
				insMutations = append(insMutations, jsonMutation)
			} else if mutation.OpType == StagedMutationReplace {
				repMutations = append(repMutations, jsonMutation)
			} else if mutation.OpType == StagedMutationRemove {
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

		var duraTimeout time.Duration
		var deadline time.Time
		if t.keyValueTimeout > 0 {
			deadline = time.Now().Add(t.keyValueTimeout)
			duraTimeout = t.keyValueTimeout * 10 / 9
		}

		_, err = atrAgent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      atrScopeName,
			CollectionName: atrCollectionName,
			Key:            atrKey,
			Ops: []gocbcore.SubDocOp{
				atrFieldOp("st", jsonAtrStateCommitted, memd.SubdocFlagXattrPath),
				atrFieldOp("tsc", "${Mutation.CAS}", memd.SubdocFlagXattrPath|memd.SubdocFlagExpandMacros),
				atrFieldOp("p", 0, memd.SubdocFlagXattrPath),
				atrFieldOp("ins", insMutations, memd.SubdocFlagXattrPath),
				atrFieldOp("rep", repMutations, memd.SubdocFlagXattrPath),
				atrFieldOp("rem", remMutations, memd.SubdocFlagXattrPath),
			},
			DurabilityLevel:        memd.DurabilityLevel(t.durabilityLevel),
			DurabilityLevelTimeout: duraTimeout,
			Flags:                  memd.SubdocDocFlagNone,
			Deadline:               deadline,
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				t.lock.Lock()

				t.txnAtrSection.Done()
				t.lock.Unlock()

				errHandler(err)
				return
			}

			t.lock.Lock()
			t.txnAtrSection.Done()
			t.lock.Unlock()

			t.hooks.AfterATRCommit(func(err error) {
				if err != nil {
					errHandler(err)
					return
				}

				cb(nil)
			})
		})
		if err != nil {
			t.lock.Lock()
			t.txnAtrSection.Done()
			t.lock.Unlock()

			errHandler(err)
		}
	})
	return nil
}

func (t *transactionAttempt) setATRCompleted(
	cb func(error),
) error {
	t.hooks.BeforeATRComplete(func(err error) {
		if err != nil {
			cb(err)
			return
		}

		t.lock.Lock()
		if t.state != AttemptStateCommitted {
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

		t.state = AttemptStateCompleted

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

		_, err = atrAgent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      atrScopeName,
			CollectionName: atrCollectionName,
			Key:            atrKey,
			Ops: []gocbcore.SubDocOp{
				atrFieldOp("st", jsonAtrStateCompleted, memd.SubdocFlagXattrPath),
				atrFieldOp("tsco", "${Mutation.CAS}", memd.SubdocFlagXattrPath|memd.SubdocFlagExpandMacros),
			},
			DurabilityLevel:        memd.DurabilityLevel(t.durabilityLevel),
			DurabilityLevelTimeout: duraTimeout,
			Deadline:               deadline,
			Flags:                  memd.SubdocDocFlagNone,
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				t.lock.Lock()
				t.txnAtrSection.Done()
				t.lock.Unlock()

				cb(err)
				return
			}

			t.lock.Lock()
			t.txnAtrSection.Done()
			t.lock.Unlock()

			t.hooks.AfterATRComplete(func(err error) {
				if err != nil {
					cb(err)
					return
				}

				cb(nil)
			})
		})
		if err != nil {
			t.lock.Lock()
			t.txnAtrSection.Done()
			t.lock.Unlock()

			cb(err)
			return
		}
	})

	return nil
}

func (t *transactionAttempt) getStagedMutationLocked(bucketName, scopeName, collectionName string, key []byte) (int, *stagedMutation) {
	for i, mutation := range t.stagedMutations {
		// TODO(brett19): Need to check the bucket names here
		if mutation.Agent.BucketName() == bucketName &&
			mutation.ScopeName == scopeName &&
			mutation.CollectionName == collectionName &&
			bytes.Compare(mutation.Key, key) == 0 {
			return i, mutation
		}
	}

	return 0, nil
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
			if errors.Is(err, gocbcore.ErrDocumentNotFound) {
				cb(nil, ErrDocNotFound)
				return
			}

			var meta *docMeta
			// TODO(brett19): Don't ignore the error here
			json.Unmarshal(result.Ops[0].Value, &meta)

			var txnMeta *jsonTxnXattr
			if result.Ops[1].Err == nil {
				var txnMetaVal jsonTxnXattr
				// TODO(brett19): Don't ignore the error here
				json.Unmarshal(result.Ops[1].Value, &txnMetaVal)
				txnMeta = &txnMetaVal

				cb(&getDoc{
					Body:    result.Ops[2].Value,
					TxnMeta: txnMeta,
					DocMeta: meta,
					Cas:     result.Cas,
				}, nil)

				return
			}

			if result.Internal.IsDeleted {
				cb(nil, ErrDocNotFound)
				return
			}

			cb(&getDoc{
				Body:    result.Ops[2].Value,
				DocMeta: meta,
				Cas:     result.Cas,
			}, nil)
		})
		if err != nil {
			cb(nil, err)
		}
	})
}

func (t *transactionAttempt) getTxnState(opts GetOptions, deadline time.Time, doc *getDoc, cb func(jsonAtrState, error)) {
	_, err := opts.Agent.LookupIn(gocbcore.LookupInOptions{
		ScopeName:      opts.ScopeName,
		CollectionName: opts.CollectionName,
		Key:            []byte(doc.TxnMeta.ATR.DocID),
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpGet,
				Path:  "attempts." + doc.TxnMeta.ID.Attempt + ".st",
				Flags: memd.SubdocFlagXattrPath,
			},
		},
		Deadline: deadline,
	}, func(result *gocbcore.LookupInResult, err error) {
		if err != nil {
			if errors.Is(err, gocbcore.ErrDocumentNotFound) {
				cb("", ErrAtrNotFound)
				return
			}

			cb("", err)
			return
		}

		err = result.Ops[0].Err
		if err != nil {
			if errors.Is(err, gocbcore.ErrPathNotFound) {
				// TODO(brett19): Discuss with Graham if this is correct.
				cb("", ErrAtrEntryNotFound)
				return
			}

			cb("", err)
			return
		}

		// TODO(brett19): Don't ignore the error here.
		var txnState jsonAtrState
		json.Unmarshal(result.Ops[0].Value, &txnState)

		t.hooks.AfterGetComplete(opts.Key, func(err error) {
			if err != nil {
				cb("", err)
				return
			}
			cb(txnState, nil)
		})
	})
	if err != nil {
		cb("", err)
		return
	}
}

func (t *transactionAttempt) Get(opts GetOptions, cb GetCallback) error {
	if err := t.checkDone(); err != nil {
		return err
	}

	t.checkExpired(hookGet, opts.Key, func(err error) {
		if err != nil {
			cb(nil, t.classifyError(err))
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
					deleted:        existingMutation.IsTombstone,
				}

				t.lock.Unlock()
				cb(getRes, nil)
				return
			} else if existingMutation.OpType == StagedMutationRemove {
				t.lock.Unlock()
				cb(nil, ErrDocNotFound)
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
				err = t.classifyError(err)
				if errors.Is(err, ErrHard) {
					t.lock.Lock()
					t.state = AttemptStateCompleted
					t.lock.Unlock()
				} else if errors.Is(err, ErrTransient) {
					t.shouldRetry = true
				}
				cb(nil, err)
				return
			}

			if doc.TxnMeta != nil {
				if doc.TxnMeta.ID.Attempt == t.id {
					cb(&GetResult{
						agent:          opts.Agent,
						scopeName:      opts.ScopeName,
						collectionName: opts.CollectionName,
						key:            opts.Key,
						Value:          doc.TxnMeta.Operation.Staged,
						Cas:            doc.Cas,
					}, nil)
					return
				}

				t.getTxnState(opts, deadline, doc, func(state jsonAtrState, err error) {
					if err != nil {
						err = t.classifyError(err)
						if errors.Is(err, ErrHard) {
							t.shouldNotRollback = true
						} else if errors.Is(err, ErrTransient) {
							t.shouldRetry = true
						}
						cb(nil, err)
						return
					}

					if state == jsonAtrStateCommitted || state == jsonAtrStateCompleted {
						if doc.TxnMeta.Operation.Type == jsonMutationRemove {
							cb(nil, ErrDocNotFound)
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
						}, nil)
						return
					}

					if doc.TxnMeta.Operation.Type == jsonMutationInsert {
						cb(nil, ErrDocNotFound)
						return
					}

					cb(&GetResult{
						agent:          opts.Agent,
						scopeName:      opts.ScopeName,
						collectionName: opts.CollectionName,
						key:            opts.Key,
						Value:          doc.Body,
						Cas:            doc.Cas,
					}, nil)
				})
				return
			}

			cb(&GetResult{
				agent:          opts.Agent,
				scopeName:      opts.ScopeName,
				collectionName: opts.CollectionName,
				key:            opts.Key,
				Value:          doc.Body,
				Cas:            doc.Cas,
				revid:          doc.DocMeta.RevID,
				expiry:         doc.DocMeta.Expiration,
			}, nil)
		})
	})

	return nil
}

func (t *transactionAttempt) classifyError(err error) error {
	if errors.Is(err, gocbcore.ErrDocumentNotFound) {
		err = ErrDocNotFound
	} else if errors.Is(err, gocbcore.ErrDocumentExists) {
		err = ErrDocAlreadyExists
	} else if errors.Is(err, gocbcore.ErrPathExists) {
		err = ErrPathAlreadyExists
	} else if errors.Is(err, gocbcore.ErrPathNotFound) {
		err = ErrPathNotFound
	} else if errors.Is(err, gocbcore.ErrCasMismatch) {
		err = ErrCasMismatch
	} else if errors.Is(err, gocbcore.ErrUnambiguousTimeout) {
		err = ErrTransient
	} else if errors.Is(err, gocbcore.ErrDurabilityAmbiguous) || errors.Is(err, gocbcore.ErrAmbiguousTimeout) {
		err = ErrAmbiguous
	} else if errors.Is(err, gocbcore.ErrValueTooLarge) {
		err = ErrAtrFull
	} else if errors.Is(err, gocbcore.ErrFeatureNotAvailable) {
		err = ErrOther
	}

	return err
}

func (t *transactionAttempt) Insert(opts InsertOptions, cb StoreCallback) error {
	var handler func(result *GetResult, err error)
	handler = func(result *GetResult, err error) {
		if err != nil {
			err = t.classifyError(err)

			if errors.Is(err, ErrHard) {
				t.shouldNotRollback = true
			} else if errors.Is(err, ErrTransient) {
				t.shouldRetry = true
			} else if errors.Is(err, ErrAttemptExpired) {
				if t.expiryOvertimeMode {
					t.shouldNotRollback = true
				} else {
					t.expiryOvertimeMode = true
				}
			} else if errors.Is(err, ErrAmbiguous) {
				time.AfterFunc(3*time.Millisecond, func() {
					err := t.insert(opts, 0, handler)
					if err != nil {
						cb(nil, t.classifyError(err))
					}
				})
				return
			} else if errors.Is(err, ErrCasMismatch) || errors.Is(err, ErrDocAlreadyExists) {
				t.getForInsert(opts, func(result gocbcore.Cas, err error) {
					if err != nil {
						err = t.classifyError(err)
						if errors.Is(err, ErrDocNotFound) {
							t.shouldRetry = true
						} else if errors.Is(err, ErrPathNotFound) {
							t.shouldRetry = true
						} else if errors.Is(err, ErrTransient) {
							t.shouldRetry = true
						}

						cb(nil, err)
						return
					}

					err = t.insert(opts, result, handler)
					if err != nil {
						cb(nil, t.classifyError(err))
					}
				})

				return
			}

			cb(nil, err)
			return
		}

		cb(result, nil)
	}

	return t.insert(opts, 0, handler)
}

func (t *transactionAttempt) getForInsert(opts InsertOptions, cb func(gocbcore.Cas, error)) {
	t.hooks.BeforeGetDocInExistsDuringStagedInsert(opts.Key, func(err error) {
		if err != nil {
			cb(0, err)
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
				cb(0, err)
				return
			}

			var txnMeta *jsonTxnXattr
			if result.Ops[1].Err == nil {
				var txnMetaVal jsonTxnXattr
				// TODO(brett19): Don't ignore the error here
				json.Unmarshal(result.Ops[1].Value, &txnMetaVal)
				txnMeta = &txnMetaVal
			}

			if txnMeta == nil {
				// This doc isn't in a transaction
				if result.Internal.IsDeleted {
					cb(result.Cas, nil)
					return
				}

				cb(0, ErrDocAlreadyExists)
				return
			}

			// TODO: checkwritewrite here
			cb(result.Cas, nil)
			return
		})
		if err != nil {
			cb(0, err)
			return
		}
	})
}

func (t *transactionAttempt) insert(opts InsertOptions, cas gocbcore.Cas, cb StoreCallback) error {
	if err := t.checkDone(); err != nil {
		return err
	}

	t.checkExpired(hookInsert, opts.Key, func(err error) {
		if err != nil {
			cb(nil, t.classifyError(err))
			return
		}

		err = t.confirmATRPending(opts.Agent, opts.ScopeName, opts.CollectionName, opts.Key, func(err error) {
			if err != nil {
				cb(nil, err)
				return
			}

			t.hooks.BeforeStagedInsert(opts.Key, func(err error) {
				if err != nil {
					cb(nil, err)
					return
				}

				stagedInfo := &stagedMutation{
					OpType:         StagedMutationInsert,
					Agent:          opts.Agent,
					ScopeName:      opts.ScopeName,
					CollectionName: opts.CollectionName,
					Key:            opts.Key,
					Staged:         opts.Value,
					IsTombstone:    true,
				}

				var txnMeta jsonTxnXattr
				txnMeta.ID.Transaction = t.transactionID
				txnMeta.ID.Attempt = t.id
				txnMeta.ATR.CollectionName = t.atrCollName()
				txnMeta.ATR.BucketName = t.atrAgent.BucketName()
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
							Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
							Value: crc32cMacro,
						},
					},
					DurabilityLevel:        memd.DurabilityLevel(t.durabilityLevel),
					DurabilityLevelTimeout: duraTimeout,
					Deadline:               deadline,
					Flags:                  flags,
				}, func(result *gocbcore.MutateInResult, err error) {
					if err != nil {
						cb(nil, err)
						return
					}

					t.hooks.AfterStagedInsertComplete(opts.Key, func(err error) {
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
						}, err)
					})
				})
				if err != nil {
					cb(nil, err)
				}
			})
		})
		if err != nil {
			cb(nil, err)
			return
		}
	})

	return nil
}

func (t *transactionAttempt) Replace(opts ReplaceOptions, cb StoreCallback) error {
	if err := t.checkDone(); err != nil {
		return err
	}

	t.checkExpired(hookReplace, opts.Document.key, func(err error) {
		if err != nil {
			cb(nil, t.classifyError(err))
			return
		}

		agent := opts.Document.agent
		scopeName := opts.Document.scopeName
		collectionName := opts.Document.collectionName
		key := opts.Document.key

		err = t.confirmATRPending(agent, scopeName, collectionName, key, func(err error) {
			if err != nil {
				cb(nil, err)
				return
			}

			t.doReplace(opts, func(stagedInfo *stagedMutation, err error) {
				if err != nil {
					err = t.classifyError(err)
					if errors.Is(err, ErrDocNotFound) || errors.Is(err, ErrCasMismatch) || errors.Is(err, ErrDocAlreadyExists) {
						t.shouldRetry = true
					} else if errors.Is(err, ErrTransient) || errors.Is(err, ErrAmbiguous) {
						t.shouldRetry = true
					} else if errors.Is(err, ErrHard) {
						t.shouldNotRollback = true
					}

					cb(nil, err)
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
					deleted:        stagedInfo.IsTombstone,
				}, nil)
			})
		})
		if err != nil {
			cb(nil, err)
			return
		}
	})

	return nil
}

func (t *transactionAttempt) doReplace(opts ReplaceOptions, cb func(*stagedMutation, error)) {
	agent := opts.Document.agent
	scopeName := opts.Document.scopeName
	collectionName := opts.Document.collectionName
	key := opts.Document.key
	deleted := opts.Document.deleted

	t.hooks.BeforeStagedReplace(key, func(err error) {
		if err != nil {
			cb(nil, t.classifyError(err))
			return
		}

		stagedInfo := &stagedMutation{
			OpType:         StagedMutationReplace,
			Agent:          agent,
			ScopeName:      scopeName,
			CollectionName: collectionName,
			Key:            key,
			Staged:         opts.Value,
			IsTombstone:    deleted,
		}

		var txnMeta jsonTxnXattr
		txnMeta.ID.Transaction = t.transactionID
		txnMeta.ID.Attempt = t.id
		txnMeta.ATR.CollectionName = t.atrCollName()
		txnMeta.ATR.BucketName = t.atrAgent.BucketName()
		txnMeta.ATR.DocID = string(t.atrKey)
		txnMeta.Operation.Type = jsonMutationReplace
		txnMeta.Operation.Staged = stagedInfo.Staged
		restore := struct {
			OriginalCAS string
			ExpiryTime  uint
			RevID       string
		}{
			OriginalCAS: fmt.Sprintf("%d", opts.Document.Cas),
			ExpiryTime:  opts.Document.expiry,
			RevID:       opts.Document.revid,
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
		if deleted {
			flags = memd.SubdocDocFlagAccessDeleted
		}

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
					Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
					Value: crc32cMacro,
				},
			},
			Flags:                  flags,
			DurabilityLevel:        memd.DurabilityLevel(t.durabilityLevel),
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
	})
}

func (t *transactionAttempt) Remove(opts RemoveOptions, cb StoreCallback) error {
	if err := t.checkDone(); err != nil {
		return err
	}

	t.checkExpired(hookRemove, opts.Document.key, func(err error) {
		if err != nil {
			cb(nil, t.classifyError(err))
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
			cb(nil, ErrOther)
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
					err = t.classifyError(err)
					if errors.Is(err, ErrDocNotFound) || errors.Is(err, ErrCasMismatch) || errors.Is(err, ErrDocAlreadyExists) {
						t.shouldRetry = true
					} else if errors.Is(err, ErrTransient) || errors.Is(err, ErrAmbiguous) {
						t.shouldRetry = true
					} else if errors.Is(err, ErrHard) {
						t.shouldNotRollback = true
					}

					cb(nil, err)
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
		txnMeta.ATR.CollectionName = t.atrCollName()
		txnMeta.ATR.BucketName = t.atrAgent.BucketName()
		txnMeta.ATR.DocID = string(t.atrKey)
		txnMeta.Operation.Type = jsonMutationRemove

		restore := struct {
			OriginalCAS string
			ExpiryTime  uint
			RevID       string
		}{
			OriginalCAS: fmt.Sprintf("%d", doc.Cas),
			ExpiryTime:  doc.expiry,
			RevID:       doc.revid,
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
		if doc.deleted {
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
			DurabilityLevel:        memd.DurabilityLevel(t.durabilityLevel),
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

func (t *transactionAttempt) unstageRepMutation(mutation stagedMutation, cb func(error)) {
	if mutation.OpType != StagedMutationReplace {
		cb(ErrUhOh)
		return
	}

	t.hooks.BeforeDocCommitted(mutation.Key, func(err error) {
		if err != nil {
			cb(err)
			return
		}

		_, err = mutation.Agent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      mutation.ScopeName,
			CollectionName: mutation.CollectionName,
			Key:            mutation.Key,
			Cas:            mutation.Cas,
			Ops: []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpDictSet,
					Path:  "txn",
					Flags: memd.SubdocFlagXattrPath,
					Value: []byte{110, 117, 108, 108}, // null
				},
				{
					Op:    memd.SubDocOpDelete,
					Path:  "txn",
					Flags: memd.SubdocFlagXattrPath,
				},
				{
					Op:    memd.SubDocOpSetDoc,
					Path:  "",
					Value: mutation.Staged,
				},
			},
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				cb(err)
				return
			}

			t.hooks.AfterDocCommittedBeforeSavingCAS(mutation.Key, func(err error) {
				if err != nil {
					cb(err)
					return
				}

				t.lock.Lock()
				t.finalMutationTokens = append(t.finalMutationTokens, MutationToken{
					BucketName:    mutation.Agent.BucketName(),
					MutationToken: result.MutationToken,
				})
				t.lock.Unlock()

				cb(nil)
			})
		})
		if err != nil {
			cb(err)
			return
		}
	})
}

func (t *transactionAttempt) unstageInsMutation(mutation stagedMutation, cb func(error)) {
	if mutation.OpType != StagedMutationInsert {
		cb(ErrUhOh)
		return
	}

	t.hooks.BeforeDocCommitted(mutation.Key, func(err error) {
		if err != nil {
			cb(err)
			return
		}

		_, err = mutation.Agent.Add(gocbcore.AddOptions{
			ScopeName:      mutation.ScopeName,
			CollectionName: mutation.CollectionName,
			Key:            mutation.Key,
			Value:          mutation.Staged,
		}, func(result *gocbcore.StoreResult, err error) {
			if err != nil {
				cb(err)
				return
			}

			t.hooks.AfterDocCommittedBeforeSavingCAS(mutation.Key, func(err error) {
				if err != nil {
					cb(err)
					return
				}

				t.lock.Lock()
				t.finalMutationTokens = append(t.finalMutationTokens, MutationToken{
					BucketName:    mutation.Agent.BucketName(),
					MutationToken: result.MutationToken,
				})
				t.lock.Unlock()

				cb(nil)
			})
		})
		if err != nil {
			cb(err)
			return
		}
	})
}

func (t *transactionAttempt) unstageRemMutation(mutation stagedMutation, cb func(error)) {
	if mutation.OpType != StagedMutationRemove {
		cb(ErrUhOh)
		return
	}

	t.hooks.BeforeDocRemoved(mutation.Key, func(err error) {
		if err != nil {
			cb(err)
			return
		}

		_, err = mutation.Agent.Delete(gocbcore.DeleteOptions{
			ScopeName:      mutation.ScopeName,
			CollectionName: mutation.CollectionName,
			Key:            mutation.Key,
			Cas:            mutation.Cas,
		}, func(result *gocbcore.DeleteResult, err error) {
			if err != nil {
				cb(err)
				return
			}

			t.hooks.AfterDocRemovedPreRetry(mutation.Key, func(err error) {
				if err != nil {
					cb(err)
					return
				}

				t.finalMutationTokens = append(t.finalMutationTokens, MutationToken{
					BucketName:    mutation.Agent.BucketName(),
					MutationToken: result.MutationToken,
				})

				// TODO(chvck): Is this in the right place?!
				t.hooks.AfterDocRemovedPostRetry(mutation.Key, func(err error) {
					if err != nil {
						cb(err)
						return
					}

					cb(nil)
				})
			})
		})
		if err != nil {
			cb(err)
			return
		}
	})
}

func (t *transactionAttempt) Commit(cb CommitCallback) error {
	t.txnOpSection.Wait(func() {
		t.lock.Lock()
		if t.state == AttemptStateNothingWritten {
			t.lock.Unlock()

			t.txnAtrSection.Wait(func() {
				cb(nil)
			})

			return
		}
		t.lock.Unlock()

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
					if mutation.OpType == StagedMutationInsert {
						t.unstageInsMutation(*mutation, func(err error) {
							waitCh <- err
						})
					} else if mutation.OpType == StagedMutationReplace {
						t.unstageRepMutation(*mutation, func(err error) {
							waitCh <- err
						})
					} else if mutation.OpType == StagedMutationRemove {
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
	})
	return nil
}

func (t *transactionAttempt) abort(
	cb func(error),
) error {
	var handler func(err error)
	handler = func(err error) {
		if err != nil {
			err = t.classifyError(err)

			if t.expiryOvertimeMode {
				t.shouldNotRollback = true
				cb(err)
				return
			}
			if errors.Is(err, ErrAttemptExpired) {
				t.expiryOvertimeMode = true
				time.AfterFunc(3*time.Millisecond, func() {
					err := t.abort(handler)
					if err != nil {
						cb(t.classifyError(err))
					}
				})
				return
			} else if errors.Is(err, ErrPathNotFound) {
				t.shouldNotRollback = true
				err = ErrAtrEntryNotFound
			} else if errors.Is(err, ErrDocNotFound) {
				t.shouldNotRollback = true
				err = ErrAtrNotFound
			} else if errors.Is(err, ErrAtrFull) {
				t.shouldNotRollback = true
			} else if errors.Is(err, ErrHard) {
				t.shouldNotRollback = true
			} else {
				time.AfterFunc(3*time.Millisecond, func() {
					t.abort(handler)
				})
				return
			}

			cb(err)
			return
		}

		cb(nil)
	}

	t.txnOpSection.Wait(func() {
		if !t.expiryOvertimeMode {
			t.checkExpired(hookATRAbort, []byte{}, func(err error) {
				if err != nil {
					handler(err)
					return
				}

				t.setATRAborted(handler)
			})
			return
		}

		t.setATRAborted(handler)
	})

	return nil
}

func (t *transactionAttempt) setATRAborted(
	cb func(error),
) error {
	t.hooks.BeforeATRAborted(func(err error) {
		if err != nil {
			cb(err)
			return
		}

		t.lock.Lock()
		if t.state == AttemptStateNothingWritten {
			t.lock.Unlock()
			t.txnAtrSection.Wait(func() {
				cb(nil)
			})

			return
		}

		if t.state == AttemptStateRolledBack || t.state == AttemptStateCompleted || t.state == AttemptStateCommitted {
			t.lock.Unlock()

			t.txnAtrSection.Wait(func() {
				cb(ErrOther)
			})

			return
		}

		atrAgent := t.atrAgent
		atrScopeName := t.atrScopeName
		atrKey := t.atrKey
		atrCollectionName := t.atrCollectionName

		t.state = AttemptStateAborted

		insMutations := []jsonAtrMutation{}
		repMutations := []jsonAtrMutation{}
		remMutations := []jsonAtrMutation{}

		for _, mutation := range t.stagedMutations {
			jsonMutation := jsonAtrMutation{
				BucketName:     mutation.Agent.BucketName(),
				ScopeName:      mutation.ScopeName,
				CollectionName: mutation.CollectionName,
				DocID:          string(mutation.Key),
			}

			if mutation.OpType == StagedMutationInsert {
				insMutations = append(insMutations, jsonMutation)
			} else if mutation.OpType == StagedMutationReplace {
				repMutations = append(repMutations, jsonMutation)
			} else if mutation.OpType == StagedMutationRemove {
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

		var duraTimeout time.Duration
		var deadline time.Time
		if t.keyValueTimeout > 0 {
			deadline = time.Now().Add(t.keyValueTimeout)
			duraTimeout = t.keyValueTimeout * 10 / 9
		}

		_, err = atrAgent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      atrScopeName,
			CollectionName: atrCollectionName,
			Key:            atrKey,
			Ops: []gocbcore.SubDocOp{
				atrFieldOp("st", jsonAtrStateAborted, memd.SubdocFlagXattrPath),
				atrFieldOp("tsrs", "${Mutation.CAS}", memd.SubdocFlagXattrPath|memd.SubdocFlagExpandMacros),
				atrFieldOp("p", 0, memd.SubdocFlagXattrPath),
				atrFieldOp("ins", insMutations, memd.SubdocFlagXattrPath),
				atrFieldOp("rep", repMutations, memd.SubdocFlagXattrPath),
				atrFieldOp("rem", remMutations, memd.SubdocFlagXattrPath),
			},
			DurabilityLevel:        memd.DurabilityLevel(t.durabilityLevel),
			DurabilityLevelTimeout: duraTimeout,
			Flags:                  memd.SubdocDocFlagNone,
			Deadline:               deadline,
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				t.lock.Lock()

				// TODO(brett19): Do other things to cancel it here....
				t.state = AttemptStateAborted

				t.txnAtrSection.Done()
				t.lock.Unlock()

				cb(err)
				return
			}

			t.lock.Lock()
			t.txnAtrSection.Done()
			t.lock.Unlock()

			t.hooks.AfterATRAborted(func(err error) {
				if err != nil {
					cb(err)
					return
				}

				cb(nil)
			})
		})
		if err != nil {
			t.lock.Lock()
			t.txnAtrSection.Done()
			t.lock.Unlock()

			cb(err)
		}
	})
	return nil
}

func (t *transactionAttempt) rollbackInsMutation(mutation stagedMutation, cb func(error)) {
	if mutation.OpType != StagedMutationInsert {
		cb(ErrUhOh)
		return
	}

	t.hooks.BeforeRollbackDeleteInserted(mutation.Key, func(err error) {
		if err != nil {
			cb(err)
			return
		}

		_, err = mutation.Agent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      mutation.ScopeName,
			CollectionName: mutation.CollectionName,
			Key:            mutation.Key,
			Cas:            mutation.Cas,
			Flags:          memd.SubdocDocFlagAccessDeleted,
			Ops: []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpDictSet,
					Path:  "txn",
					Flags: memd.SubdocFlagXattrPath,
					Value: []byte{110, 117, 108, 108}, // null
				},
				{
					Op:    memd.SubDocOpDelete,
					Path:  "txn",
					Flags: memd.SubdocFlagXattrPath,
				},
			},
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				cb(err)
				return
			}

			t.hooks.AfterRollbackDeleteInserted(mutation.Key, func(err error) {
				if err != nil {
					cb(err)
					return
				}

				cb(nil)
			})
		})
		if err != nil {
			cb(err)
			return
		}
	})
}

func (t *transactionAttempt) rollbackRepRemMutation(mutation stagedMutation, cb func(error)) {
	if mutation.OpType != StagedMutationRemove && mutation.OpType != StagedMutationReplace {
		cb(ErrUhOh)
		return
	}

	t.hooks.BeforeDocRolledBack(mutation.Key, func(err error) {
		if err != nil {
			cb(err)
			return
		}

		_, err = mutation.Agent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      mutation.ScopeName,
			CollectionName: mutation.CollectionName,
			Key:            mutation.Key,
			Cas:            mutation.Cas,
			Flags:          memd.SubdocDocFlagAccessDeleted,
			Ops: []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpDictSet,
					Path:  "txn",
					Flags: memd.SubdocFlagXattrPath,
					Value: []byte{110, 117, 108, 108}, // null
				},
				{
					Op:    memd.SubDocOpDelete,
					Path:  "txn",
					Flags: memd.SubdocFlagXattrPath,
				},
			},
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				cb(err)
				return
			}

			t.hooks.AfterRollbackReplaceOrRemove(mutation.Key, func(err error) {
				if err != nil {
					cb(err)
					return
				}

				cb(nil)
			})
		})
		if err != nil {
			cb(err)
			return
		}
	})
}

func (t *transactionAttempt) setATRRolledBack(
	cb func(error),
) error {

	t.hooks.BeforeATRRolledBack(func(err error) {
		if err != nil {
			cb(err)
			return
		}

		t.lock.Lock()
		if t.state != AttemptStateAborted {
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

		t.state = AttemptStateRolledBack

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

		_, err = atrAgent.MutateIn(gocbcore.MutateInOptions{
			ScopeName:      atrScopeName,
			CollectionName: atrCollectionName,
			Key:            atrKey,
			Ops: []gocbcore.SubDocOp{
				atrFieldOp("st", jsonAtrStateRolledBack, memd.SubdocFlagXattrPath),
				atrFieldOp("tsrc", "${Mutation.CAS}", memd.SubdocFlagXattrPath|memd.SubdocFlagExpandMacros),
			},
			DurabilityLevel:        memd.DurabilityLevel(t.durabilityLevel),
			DurabilityLevelTimeout: duraTimeout,
			Deadline:               deadline,
			Flags:                  memd.SubdocDocFlagNone,
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				t.lock.Lock()

				// TODO(brett19): Do other things to cancel it here....
				t.state = AttemptStateAborted

				t.txnAtrSection.Done()
				t.lock.Unlock()

				cb(err)
				return
			}

			t.lock.Lock()
			t.txnAtrSection.Done()
			t.lock.Unlock()

			t.hooks.AfterATRRolledBack(func(err error) {
				if err != nil {
					cb(err)
					return
				}

				cb(nil)
			})
		})
		if err != nil {
			t.lock.Lock()

			// TODO(brett19): Do other things to cancel it here....
			t.state = AttemptStateAborted

			t.txnAtrSection.Done()
			t.lock.Unlock()

			cb(err)
		}
	})

	return nil
}

func (t *transactionAttempt) Rollback(cb RollbackCallback) error {
	t.abort(func(err error) {
		if err != nil {
			cb(err)
			return
		}

		// TODO(brett19): Use atomic counters instead of a goroutine here
		go func() {
			numMutations := len(t.stagedMutations)
			waitCh := make(chan error, numMutations)

			for _, mutation := range t.stagedMutations {
				if mutation.OpType == StagedMutationInsert {
					t.rollbackInsMutation(*mutation, func(err error) {
						waitCh <- err
					})
				} else if mutation.OpType == StagedMutationRemove || mutation.OpType == StagedMutationReplace {
					t.rollbackRepRemMutation(*mutation, func(err error) {
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

			t.setATRRolledBack(func(err error) {
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
