package transactions

import (
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/gocbcore/v8"
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
	DurabilityLevel   DurabilityLevel

	readyTime time.Time
}

func (cr *CleanupRequest) ready() bool {
	return time.Now().After(cr.readyTime)
}

func (cr *CleanupRequest) String() string {
	return fmt.Sprintf("bucket: %s, collection: %s, scope: %s, atr: %s, attempt: %s", cr.AtrBucketName, cr.AtrCollectionName,
		cr.AtrScopeName, cr.AtrID, cr.AttemptID)
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

func (ca CleanupAttempt) String() string {
	return fmt.Sprintf("bucket: %s, collection: %s, scope: %s, atr: %s, attempt: %s", ca.AtrBucketName, ca.AtrCollectionName,
		ca.AtrScopeName, ca.AtrID, ca.AttemptID)
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
		q:                   make(priorityQueue, 0, config.CleanupQueueSize),
		operationTimeout:    config.KeyValueTimeout,
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
	q                   priorityQueue
	qLock               sync.Mutex
	stop                chan struct{}
	bucketAgentProvider BucketAgentProviderFn
	operationTimeout    time.Duration
	durabilityLevel     DurabilityLevel
}

func startCleanupThread(config *Config) *stdCleaner {
	cleaner := &stdCleaner{
		hooks:               config.Internal.CleanUpHooks,
		qSize:               config.CleanupQueueSize,
		stop:                make(chan struct{}),
		bucketAgentProvider: config.BucketAgentProvider,
		q:                   make(priorityQueue, 0, config.CleanupQueueSize),
		operationTimeout:    config.KeyValueTimeout,
		durabilityLevel:     config.DurabilityLevel,
	}

	// No point in running this if we can't get agents.
	if config.BucketAgentProvider != nil {
		go cleaner.processQ()
	}

	return cleaner
}

func (c *stdCleaner) AddRequest(req *CleanupRequest) bool {
	c.qLock.Lock()
	defer c.qLock.Unlock()
	if c.q.Len() == int(c.qSize) {
		logDebugf("Not queueing request for: %s, limit size reached",
			req.String())
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

	peek := c.q.Peek()
	if peek == nil {
		return nil
	}
	if !peek.(*CleanupRequest).ready() {
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
		req := heap.Pop(&c.q)
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
					go func() {
						logDebugf("Failed to get agent for request: %s, err: %v", req.String(), err)
						select {
						case <-time.After(10 * time.Second):
							c.AddRequest(req)
						case <-c.stop:
						}
					}()
					return
				}

				logTracef("Running cleanup for request: %s", req.String())
				waitCh := make(chan struct{}, 1)
				c.CleanupAttempt(agent, req, true, func(attempt CleanupAttempt) {

					if !attempt.Success {
						go func() {
							age := time.Now().Sub(req.readyTime)
							logDebugf("Cleanup attempt failed for entry: %s, age is %s",
								attempt.String(), age)

							if age > 2*time.Hour {
								logWarnf("Cleanup request is %s old which could indicate a serious error - "+
									"please raise with support.", age)
							}

							select {
							case <-time.After(10 * time.Second):
								c.AddRequest(req)
							case <-c.stop:
							}
						}()
					}
					waitCh <- struct{}{}
				})
				<-waitCh
			}
		}
	}()
}

func (c *stdCleaner) checkForwardCompatability(
	stage forwardCompatStage,
	fc map[string][]ForwardCompatibilityEntry,
	cb func(error),
) {
	isCompat, _, _, err := checkForwardCompatability(stage, fc)
	if err != nil {
		cb(err)
		return
	}

	if !isCompat {
		cb(ErrForwardCompatibilityFailure)
		return
	}

	cb(nil)
}

func (c *stdCleaner) CleanupAttempt(atrAgent *gocbcore.Agent, req *CleanupRequest, regular bool, cb func(attempt CleanupAttempt)) {
	c.checkForwardCompatability(forwardCompatStageGetsCleanupEntry, req.ForwardCompat, func(err error) {
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

		if req.DurabilityLevel == DurabilityLevelUnknown {
			req.DurabilityLevel = c.durabilityLevel
		}
		deadline, duraTimeout := mutationTimeouts(c.operationTimeout, req.DurabilityLevel)

		_, err = agent.MutateIn(gocbcore.MutateInOptions{
			Key:                    req.AtrID,
			ScopeName:              req.AtrScopeName,
			CollectionName:         req.AtrCollectionName,
			Ops:                    specs,
			Deadline:               deadline,
			DurabilityLevel:        durabilityLevelToMemd(req.DurabilityLevel),
			DurabilityLevelTimeout: duraTimeout,
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				if errors.Is(err, gocbcore.ErrPathNotFound) {
					cb(nil)
					return
				}

				logDebugf("Failed to cleanup ATR for request: %s, err: %v", req.String(), err)
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
	var memdDuraLevel memd.DurabilityLevel
	if req.DurabilityLevel > DurabilityLevelUnknown {
		// We want to ensure that we don't panic here, if the durability level is unknown then we'll just not set
		// a durability level.
		memdDuraLevel = durabilityLevelToMemd(req.DurabilityLevel)
	}
	deadline, duraTimeout := mutationTimeouts(c.operationTimeout, req.DurabilityLevel)

	switch req.State {
	case AttemptStateCommitted:

		waitCh := make(chan error, 1)
		c.commitInsRepDocs(req.AttemptID, req.Inserts, deadline, memdDuraLevel, duraTimeout, func(err error) {
			waitCh <- err
		})
		err := <-waitCh
		if err != nil {
			cb(err)
			return
		}

		waitCh = make(chan error, 1)
		c.commitInsRepDocs(req.AttemptID, req.Replaces, deadline, memdDuraLevel, duraTimeout, func(err error) {
			waitCh <- err
		})
		err = <-waitCh
		if err != nil {
			cb(err)
			return
		}

		waitCh = make(chan error, 1)
		c.commitRemDocs(req.AttemptID, req.Removes, deadline, memdDuraLevel, duraTimeout, func(err error) {
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
		c.rollbackInsDocs(req.AttemptID, req.Inserts, deadline, memdDuraLevel, duraTimeout, func(err error) {
			waitCh <- err
		})
		err := <-waitCh
		if err != nil {
			cb(err)
			return
		}

		waitCh = make(chan error, 1)
		c.rollbackRepRemDocs(req.AttemptID, req.Replaces, deadline, memdDuraLevel, duraTimeout, func(err error) {
			waitCh <- err
		})
		err = <-waitCh
		if err != nil {
			cb(err)
			return
		}

		waitCh = make(chan error, 1)
		c.rollbackRepRemDocs(req.AttemptID, req.Removes, deadline, memdDuraLevel, duraTimeout, func(err error) {
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

func (c *stdCleaner) rollbackRepRemDocs(attemptID string, docs []DocRecord, deadline time.Time, durability memd.DurabilityLevel,
	duraTimeout time.Duration, cb func(err error)) {
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
					Flags:                  memd.SubdocDocFlagAccessDeleted,
					Deadline:               deadline,
					DurabilityLevel:        durability,
					DurabilityLevelTimeout: duraTimeout,
				}, func(result *gocbcore.MutateInResult, err error) {
					if err != nil {
						logDebugf("Failed to rollback for bucket: %s, collection: %s, scope: %s, id: %s, err: %v",
							doc.BucketName, doc.CollectionName, doc.ScopeName, doc.ID, err)
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

func (c *stdCleaner) rollbackInsDocs(attemptID string, docs []DocRecord, deadline time.Time, durability memd.DurabilityLevel,
	duraTimeout time.Duration, cb func(err error)) {
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
						Flags:                  memd.SubdocDocFlagAccessDeleted,
						Deadline:               deadline,
						DurabilityLevel:        durability,
						DurabilityLevelTimeout: duraTimeout,
					}, func(result *gocbcore.MutateInResult, err error) {
						if err != nil {
							logDebugf("Failed to rollback for bucket: %s, collection: %s, scope: %s, id: %s, err: %v",
								doc.BucketName, doc.CollectionName, doc.ScopeName, doc.ID, err)
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
						Key:                    doc.ID,
						ScopeName:              doc.ScopeName,
						CollectionName:         doc.CollectionName,
						Cas:                    getRes.Cas,
						Deadline:               deadline,
						DurabilityLevel:        durability,
						DurabilityLevelTimeout: duraTimeout,
					}, func(result *gocbcore.DeleteResult, err error) {
						if err != nil {
							logDebugf("Failed to rollback for bucket: %s, collection: %s, scope: %s, id: %s, err: %v",
								doc.BucketName, doc.CollectionName, doc.ScopeName, doc.ID, err)
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

func (c *stdCleaner) commitRemDocs(attemptID string, docs []DocRecord, deadline time.Time, durability memd.DurabilityLevel,
	duraTimeout time.Duration, cb func(err error)) {
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
					Key:                    doc.ID,
					ScopeName:              doc.ScopeName,
					CollectionName:         doc.CollectionName,
					Cas:                    getRes.Cas,
					Deadline:               deadline,
					DurabilityLevel:        durability,
					DurabilityLevelTimeout: duraTimeout,
				}, func(result *gocbcore.DeleteResult, err error) {
					if err != nil {
						logDebugf("Failed to commit for bucket: %s, collection: %s, scope: %s, id: %s, err: %v",
							doc.BucketName, doc.CollectionName, doc.ScopeName, doc.ID, err)
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

func (c *stdCleaner) commitInsRepDocs(attemptID string, docs []DocRecord, deadline time.Time, durability memd.DurabilityLevel,
	duraTimeout time.Duration, cb func(err error)) {
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
						Value:                  getRes.Body,
						Key:                    doc.ID,
						ScopeName:              doc.ScopeName,
						CollectionName:         doc.CollectionName,
						Deadline:               deadline,
						DurabilityLevel:        durability,
						DurabilityLevelTimeout: duraTimeout,
					}, func(result *gocbcore.StoreResult, err error) {
						if err != nil {
							logDebugf("Failed to commit for bucket: %s, collection: %s, scope: %s, id: %s, err: %v",
								doc.BucketName, doc.CollectionName, doc.ScopeName, doc.ID, err)
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
						Deadline:               deadline,
						DurabilityLevel:        durability,
						DurabilityLevelTimeout: duraTimeout,
					}, func(result *gocbcore.MutateInResult, err error) {
						if err != nil {
							logDebugf("Failed to commit for bucket: %s, collection: %s, scope: %s, id: %s, err: %v",
								doc.BucketName, doc.CollectionName, doc.ScopeName, doc.ID, err)
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

		var deadline time.Time
		if c.operationTimeout > 0 {
			deadline = time.Now().Add(c.operationTimeout)
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
			Deadline: deadline,
			Flags:    memd.SubdocDocFlagAccessDeleted,
		}, func(result *gocbcore.LookupInResult, err error) {
			if err != nil {
				if errors.Is(err, gocbcore.ErrDocumentNotFound) {
					// We can consider this success.
					cb(nil, nil)
					return
				}

				logDebugf("Failed to lookup doc for bucket: %s, collection: %s, scope: %s, id: %s, err: %v",
					dr.BucketName, dr.CollectionName, dr.ScopeName, dr.ID, err)
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
