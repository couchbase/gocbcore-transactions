package transactions

import (
	"container/heap"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
)

// CleanupRequest represents a complete transaction attempt that requires cleanup.
// Internal: This should never be used and is not supported.
type CleanupRequest struct {
	AttemptID         string
	AtrID             []byte
	AtrCollectionName string
	AtrScopeName      string
	AtrBucketName     string
	Inserts           []DocRecord
	Replaces          []DocRecord
	Removes           []DocRecord
	State             AttemptState
	ForwardCompat     map[string][]ForwardCompatibilityEntry

	readyTime time.Time
}

func (cr *CleanupRequest) ready() bool {
	return time.Now().After(cr.readyTime)
}

// DocRecord represents an individual document operation requiring cleanup.
// Internal: This should never be used and is not supported.
type DocRecord struct {
	CollectionName string
	ScopeName      string
	BucketName     string
	ID             []byte
}

// CleanupAttempt represents the result of running cleanup for a transaction attempt.
// Internal: This should never be used and is not supported.
type CleanupAttempt struct {
	Success           bool
	IsReqular         bool
	AttemptID         string
	AtrID             []byte
	AtrCollectionName string
	AtrScopeName      string
	AtrBucketName     string
	Request           *CleanupRequest
}

// Cleaner is responsible for performing cleanup of completed transactions.
// Internal: This should never be used and is not supported.
type Cleaner interface {
	AddRequest(req *CleanupRequest) bool
	PopRequest() *CleanupRequest
	ForceCleanupQueue(cb func([]CleanupAttempt))
	QueueLength() int32
	CleanupAttempt(atrAgent *gocbcore.Agent, req *CleanupRequest, regular bool, cb func(attempt CleanupAttempt))
	Close()
}

// NewCleaner returns a Cleaner implementation.
// Internal: This should never be used and is not supported.
func NewCleaner(config *Config) Cleaner {
	return &stdCleaner{
		hooks:               config.Internal.CleanUpHooks,
		qSize:               config.CleanupQueueSize,
		stop:                make(chan struct{}),
		bucketAgentProvider: config.BucketAgentProvider,
		q:                   make(delayQueue, 0, config.CleanupQueueSize),
		kvTimeout:           config.KeyValueTimeout,
		durabilityLevel:     config.DurabilityLevel,
	}
}

type noopCleaner struct {
}

func (nc *noopCleaner) AddRequest(req *CleanupRequest) bool {
	return true
}
func (nc *noopCleaner) PopRequest() *CleanupRequest {
	return nil
}

func (nc *noopCleaner) ForceCleanupQueue(cb func([]CleanupAttempt)) {
	cb([]CleanupAttempt{})
}

func (nc *noopCleaner) QueueLength() int32 {
	return 0
}

func (nc *noopCleaner) CleanupAttempt(atrAgent *gocbcore.Agent, req *CleanupRequest, regular bool, cb func(attempt CleanupAttempt)) {
	cb(CleanupAttempt{})
}

func (nc *noopCleaner) Close() {}

type stdCleaner struct {
	hooks               CleanUpHooks
	qSize               uint32
	q                   delayQueue
	qLock               sync.Mutex
	stop                chan struct{}
	bucketAgentProvider BucketAgentProviderFn
	kvTimeout           time.Duration
	durabilityLevel     DurabilityLevel
}

func startCleanupThread(config *Config) *stdCleaner {
	cleaner := &stdCleaner{
		hooks:               config.Internal.CleanUpHooks,
		qSize:               config.CleanupQueueSize,
		stop:                make(chan struct{}),
		bucketAgentProvider: config.BucketAgentProvider,
		q:                   make(delayQueue, 0, config.CleanupQueueSize),
		kvTimeout:           config.KeyValueTimeout,
		durabilityLevel:     config.DurabilityLevel,
	}

	go cleaner.processQ()

	return cleaner
}

func (c *stdCleaner) AddRequest(req *CleanupRequest) bool {
	c.qLock.Lock()
	defer c.qLock.Unlock()
	if c.q.Len() == int(c.qSize) {
		return false
	}

	heap.Push(&c.q, req)
	return true
}

func (c *stdCleaner) PopRequest() *CleanupRequest {
	c.qLock.Lock()
	defer c.qLock.Unlock()
	if c.q.Len() == 0 {
		return nil
	}

	cr := heap.Pop(&c.q)
	if cr == nil {
		return nil
	}
	return cr.(*CleanupRequest)
}

// Used only for tests
func (c *stdCleaner) ForceCleanupQueue(cb func([]CleanupAttempt)) {
	c.qLock.Lock()
	var reqs []*CleanupRequest
	for {
		if c.q.Len() == 0 {
			break
		}
		req := c.q.ForcePop()
		if req == nil {
			break
		}

		reqs = append(reqs, req.(*CleanupRequest))
	}
	c.qLock.Unlock()

	results := make([]CleanupAttempt, 0, len(reqs))
	var l sync.Mutex
	handler := func(attempt CleanupAttempt) {
		l.Lock()
		defer l.Unlock()
		results = append(results, attempt)
		if len(results) == len(reqs) {
			cb(results)
		}
	}

	for _, req := range reqs {
		agent, err := c.bucketAgentProvider(req.AtrBucketName)
		if err != nil {
			handler(CleanupAttempt{
				Success:           false,
				IsReqular:         false,
				AttemptID:         req.AttemptID,
				AtrID:             req.AtrID,
				AtrCollectionName: req.AtrCollectionName,
				AtrScopeName:      req.AtrScopeName,
				AtrBucketName:     req.AtrBucketName,
				Request:           req})
			continue
		}

		c.CleanupAttempt(agent, req, true, func(attempt CleanupAttempt) {
			handler(attempt)
		})
	}
}

// Used only for tests
func (c *stdCleaner) QueueLength() int32 {
	c.qLock.Lock()
	defer c.qLock.Unlock()
	return int32(c.q.Len())
}

// Used only for tests
func (c *stdCleaner) Close() {
	close(c.stop)
}

func (c *stdCleaner) processQ() {
	q := make(chan *CleanupRequest, c.qSize)
	go func() {
		for {
			select {
			case <-c.stop:
				return
			case <-time.After(100 * time.Millisecond):
			}
			for {
				req := c.PopRequest()
				if req == nil {
					break
				}

				q <- req
			}
		}
	}()

	go func() {
		for {
			select {
			case <-c.stop:
				return
			case req := <-q:
				agent, err := c.bucketAgentProvider(req.AtrBucketName)
				if err != nil {
					select {
					case <-time.After(10 * time.Second):
						c.AddRequest(req)
					case <-c.stop:
					}
					return
				}

				c.CleanupAttempt(agent, req, true, func(attempt CleanupAttempt) {
					if !attempt.Success {
						select {
						case <-time.After(10 * time.Second):
							c.AddRequest(req)
						case <-c.stop:
						}
					}
				})
			}
		}
	}()
}

func (c *stdCleaner) CleanupAttempt(atrAgent *gocbcore.Agent, req *CleanupRequest, regular bool, cb func(attempt CleanupAttempt)) {
	checkForwardCompatbility(forwardCompatStageGetsCleanupEntry, req.ForwardCompat, func(shouldRetry bool, retryInterval time.Duration, err error) {
		if err != nil {
			cb(CleanupAttempt{
				Success:           false,
				IsReqular:         regular,
				AttemptID:         req.AttemptID,
				AtrID:             req.AtrID,
				AtrCollectionName: req.AtrCollectionName,
				AtrScopeName:      req.AtrScopeName,
				AtrBucketName:     req.AtrBucketName,
				Request:           req,
			})
			return
		}

		c.cleanupDocs(req, func(err error) {
			if err != nil {
				cb(CleanupAttempt{
					Success:           false,
					IsReqular:         regular,
					AttemptID:         req.AttemptID,
					AtrID:             req.AtrID,
					AtrCollectionName: req.AtrCollectionName,
					AtrScopeName:      req.AtrScopeName,
					AtrBucketName:     req.AtrBucketName,
					Request:           req,
				})
				return
			}

			c.cleanupATR(atrAgent, req, func(err error) {
				success := true
				if err != nil {
					success = false
				}

				cb(CleanupAttempt{
					Success:           success,
					IsReqular:         regular,
					AttemptID:         req.AttemptID,
					AtrID:             req.AtrID,
					AtrCollectionName: req.AtrCollectionName,
					AtrScopeName:      req.AtrScopeName,
					AtrBucketName:     req.AtrBucketName,
					Request:           req,
				})
			})
		})
	})
}

func (c *stdCleaner) cleanupATR(agent *gocbcore.Agent, req *CleanupRequest, cb func(error)) {
	c.hooks.BeforeATRRemove(req.AtrID, func(err error) {
		if err != nil {
			if errors.Is(err, gocbcore.ErrPathNotFound) {
				cb(nil)
				return
			}
			cb(err)
			return
		}

		var specs []gocbcore.SubDocOp
		if req.State == AttemptStatePending {
			specs = append(specs, gocbcore.SubDocOp{
				Op:    memd.SubDocOpDictAdd,
				Value: []byte{110, 117, 108, 108},
				Path:  "attempts." + req.AttemptID + ".p",
				Flags: memd.SubdocFlagXattrPath,
			})
		}

		specs = append(specs, gocbcore.SubDocOp{
			Op:    memd.SubDocOpDelete,
			Path:  "attempts." + req.AttemptID,
			Flags: memd.SubdocFlagXattrPath,
		})

		_, err = agent.MutateIn(gocbcore.MutateInOptions{
			Key:            req.AtrID,
			ScopeName:      req.AtrScopeName,
			CollectionName: req.AtrCollectionName,
			Ops:            specs,
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				if errors.Is(err, gocbcore.ErrPathNotFound) {
					cb(nil)
					return
				}

				cb(err)
				return
			}

			cb(nil)
		})
		if err != nil {
			cb(err)
			return
		}

	})
}

func (c *stdCleaner) cleanupDocs(req *CleanupRequest, cb func(error)) {
	switch req.State {
	case AttemptStateCommitted:

		waitCh := make(chan error, 1)
		c.commitInsRepDocs(req.AttemptID, req.Inserts, func(err error) {
			waitCh <- err
		})
		err := <-waitCh
		if err != nil {
			cb(err)
			return
		}

		waitCh = make(chan error, 1)
		c.commitInsRepDocs(req.AttemptID, req.Replaces, func(err error) {
			waitCh <- err
		})
		err = <-waitCh
		if err != nil {
			cb(err)
			return
		}

		waitCh = make(chan error, 1)
		c.commitRemDocs(req.AttemptID, req.Removes, func(err error) {
			waitCh <- err
		})
		err = <-waitCh
		if err != nil {
			cb(err)
			return
		}

		cb(nil)
	case AttemptStateAborted:
		waitCh := make(chan error, 3)
		c.rollbackInsDocs(req.AttemptID, req.Inserts, func(err error) {
			waitCh <- err
		})
		err := <-waitCh
		if err != nil {
			cb(err)
			return
		}

		waitCh = make(chan error, 1)
		c.rollbackRepRemDocs(req.AttemptID, req.Replaces, func(err error) {
			waitCh <- err
		})
		err = <-waitCh
		if err != nil {
			cb(err)
			return
		}

		waitCh = make(chan error, 1)
		c.rollbackRepRemDocs(req.AttemptID, req.Removes, func(err error) {
			waitCh <- err
		})
		err = <-waitCh
		if err != nil {
			cb(err)
			return
		}

		cb(nil)
	case AttemptStatePending:
		cb(nil)
	case AttemptStateCompleted:
		cb(nil)
	case AttemptStateRolledBack:
		cb(nil)
	case AttemptStateNothingWritten:
		cb(nil)
	default:
		cb(nil)
	}
}

func (c *stdCleaner) rollbackRepRemDocs(attemptID string, docs []DocRecord, cb func(err error)) {
	var overallErr error

	for _, doc := range docs {
		waitCh := make(chan error, 1)

		agent, err := c.bucketAgentProvider(doc.BucketName)
		if err != nil {
			cb(err)
			return
		}

		c.perDoc(false, attemptID, doc, agent, func(getRes *getDoc, err error) {
			if err != nil {
				waitCh <- err
				return
			}

			if getRes == nil {
				// This violates implicit contract idioms but needs must.
				waitCh <- nil
				return
			}

			c.hooks.BeforeRemoveLinks(doc.ID, func(err error) {
				if err != nil {
					waitCh <- err
					return
				}

				_, err = agent.MutateIn(gocbcore.MutateInOptions{
					Key:            doc.ID,
					ScopeName:      doc.ScopeName,
					CollectionName: doc.CollectionName,
					Cas:            getRes.Cas,
					Ops: []gocbcore.SubDocOp{
						{
							Op:    memd.SubDocOpDelete,
							Path:  "txn",
							Flags: memd.SubdocFlagXattrPath,
						},
					},
					Flags: memd.SubdocDocFlagAccessDeleted,
				}, func(result *gocbcore.MutateInResult, err error) {
					if err != nil {
						waitCh <- err
						return
					}

					waitCh <- nil
				})
				if err != nil {
					waitCh <- err
					return
				}
			})
		})

		err = <-waitCh

		if err != nil && overallErr == nil {
			overallErr = err
		}
	}

	cb(overallErr)
}

func (c *stdCleaner) rollbackInsDocs(attemptID string, docs []DocRecord, cb func(err error)) {
	var overallErr error

	for _, doc := range docs {
		waitCh := make(chan error, 1)

		agent, err := c.bucketAgentProvider(doc.BucketName)
		if err != nil {
			cb(err)
			return
		}

		c.perDoc(false, attemptID, doc, agent, func(getRes *getDoc, err error) {
			if err != nil {
				waitCh <- err
				return
			}

			if getRes == nil {
				// This violates implicit contract idioms but needs must.
				waitCh <- nil
				return
			}

			c.hooks.BeforeRemoveDoc(doc.ID, func(err error) {
				if err != nil {
					waitCh <- err
					return
				}

				if getRes.Deleted {
					_, err := agent.MutateIn(gocbcore.MutateInOptions{
						Key:            doc.ID,
						ScopeName:      doc.ScopeName,
						CollectionName: doc.CollectionName,
						Cas:            getRes.Cas,
						Ops: []gocbcore.SubDocOp{
							{
								Op:    memd.SubDocOpDelete,
								Path:  "txn",
								Flags: memd.SubdocFlagXattrPath,
							},
						},
						Flags: memd.SubdocDocFlagAccessDeleted,
					}, func(result *gocbcore.MutateInResult, err error) {
						if err != nil {
							waitCh <- err
							return
						}

						waitCh <- nil
					})
					if err != nil {
						waitCh <- err
						return
					}
				} else {
					_, err := agent.Delete(gocbcore.DeleteOptions{
						Key:            doc.ID,
						ScopeName:      doc.ScopeName,
						CollectionName: doc.CollectionName,
						Cas:            getRes.Cas,
					}, func(result *gocbcore.DeleteResult, err error) {
						if err != nil {
							waitCh <- err
							return
						}

						waitCh <- nil
					})
					if err != nil {
						waitCh <- err
						return
					}
				}
			})
		})

		err = <-waitCh
		if err != nil && overallErr == nil {
			overallErr = err
		}
	}

	cb(overallErr)
}

func (c *stdCleaner) commitRemDocs(attemptID string, docs []DocRecord, cb func(err error)) {
	var overallErr error
	for _, doc := range docs {
		waitCh := make(chan error, 1)

		agent, err := c.bucketAgentProvider(doc.BucketName)
		if err != nil {
			cb(err)
			return
		}

		c.perDoc(true, attemptID, doc, agent, func(getRes *getDoc, err error) {
			if err != nil {
				waitCh <- err
				return
			}

			if getRes == nil {
				// This violates implicit contract idioms but needs must.
				waitCh <- nil
				return
			}

			c.hooks.BeforeRemoveDocStagedForRemoval(doc.ID, func(err error) {
				if err != nil {
					waitCh <- err
					return
				}

				if getRes.TxnMeta.Operation.Type != jsonMutationRemove {
					waitCh <- nil
					return
				}

				_, err = agent.Delete(gocbcore.DeleteOptions{
					Key:            doc.ID,
					ScopeName:      doc.ScopeName,
					CollectionName: doc.CollectionName,
					Cas:            getRes.Cas,
				}, func(result *gocbcore.DeleteResult, err error) {
					if err != nil {
						waitCh <- err
						return
					}

					waitCh <- nil
				})
				if err != nil {
					waitCh <- err
					return
				}
			})
		})
		err = <-waitCh
		if err != nil && overallErr == nil {
			overallErr = err
		}
	}

	cb(overallErr)
}

func (c *stdCleaner) commitInsRepDocs(attemptID string, docs []DocRecord, cb func(err error)) {
	var overallErr error

	for _, doc := range docs {
		waitCh := make(chan error, 1)

		agent, err := c.bucketAgentProvider(doc.BucketName)
		if err != nil {
			cb(err)
			return
		}

		c.perDoc(true, attemptID, doc, agent, func(getRes *getDoc, err error) {
			if err != nil {
				waitCh <- err
				return
			}

			if getRes == nil {
				// This violates implicit contract idioms but needs must.
				waitCh <- nil
				return
			}

			c.hooks.BeforeCommitDoc(doc.ID, func(err error) {
				if err != nil {
					waitCh <- err
					return
				}

				if getRes.Deleted {
					_, err := agent.Set(gocbcore.SetOptions{
						Value:          getRes.Body,
						Key:            doc.ID,
						ScopeName:      doc.ScopeName,
						CollectionName: doc.CollectionName,
					}, func(result *gocbcore.StoreResult, err error) {
						if err != nil {
							waitCh <- err
							return
						}

						waitCh <- nil
					})
					if err != nil {
						waitCh <- err
						return
					}
				} else {
					_, err := agent.MutateIn(gocbcore.MutateInOptions{
						Key:            doc.ID,
						ScopeName:      doc.ScopeName,
						CollectionName: doc.CollectionName,
						Cas:            getRes.Cas,
						Ops: []gocbcore.SubDocOp{
							{
								Op:    memd.SubDocOpDelete,
								Path:  "txn",
								Flags: memd.SubdocFlagXattrPath,
							},
							{
								Op:    memd.SubDocOpSetDoc,
								Path:  "",
								Value: getRes.Body,
							},
						},
					}, func(result *gocbcore.MutateInResult, err error) {
						if err != nil {
							waitCh <- err
							return
						}

						waitCh <- nil
					})
					if err != nil {
						waitCh <- err
						return
					}
				}
			})
		})
		err = <-waitCh
		if err != nil && overallErr == nil {
			overallErr = err
		}
	}

	cb(overallErr)
}

func (c *stdCleaner) perDoc(crc32MatchStaging bool, attemptID string, dr DocRecord, agent *gocbcore.Agent,
	cb func(getRes *getDoc, err error)) {
	c.hooks.BeforeDocGet(dr.ID, func(err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		_, err = agent.LookupIn(gocbcore.LookupInOptions{
			ScopeName:      dr.ScopeName,
			CollectionName: dr.CollectionName,
			Key:            dr.ID,
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
			},
			// Deadline: deadline, TODO
			Flags: memd.SubdocDocFlagAccessDeleted,
		}, func(result *gocbcore.LookupInResult, err error) {
			if err != nil {
				if errors.Is(err, gocbcore.ErrDocumentNotFound) {
					// We can consider this success.
					cb(nil, nil)
					return
				}

				cb(nil, err)
				return
			}

			if result.Ops[0].Err != nil {
				// This is not so good.
				cb(nil, result.Ops[0].Err)
				return
			}

			if result.Ops[1].Err != nil {
				// Txn probably committed so this is success.
				cb(nil, nil)
				return
			}

			var txnMetaVal *jsonTxnXattr
			if err := json.Unmarshal(result.Ops[1].Value, &txnMetaVal); err != nil {
				cb(nil, err)
				return
			}

			if attemptID != txnMetaVal.ID.Attempt {
				// Document involved in another txn, was probably committed, this is success.
				cb(nil, nil)
				return
			}

			var meta *docMeta
			if err := json.Unmarshal(result.Ops[0].Value, &meta); err != nil {
				cb(nil, err)
				return
			}
			if crc32MatchStaging {
				if meta.CRC32 != txnMetaVal.Operation.CRC32 {
					// This document is a part of this txn but its body has changed, we'll continue as success.
					cb(nil, nil)
					return
				}
			}

			cb(&getDoc{
				Body:    txnMetaVal.Operation.Staged,
				DocMeta: meta,
				Cas:     result.Cas,
				Deleted: result.Internal.IsDeleted,
				TxnMeta: txnMetaVal,
			}, nil)
		})
		if err != nil {
			cb(nil, err)
		}
	})
}
