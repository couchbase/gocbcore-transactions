package transactions

// TransactionHooks provides a number of internal hooks used for testing.
// Internal: This should never be used and is not supported.
type TransactionHooks interface {
	BeforeATRCommit(func(err error))
	AfterATRCommit(func(err error))
	BeforeDocCommitted(docID []byte, cb func(err error))
	BeforeRemovingDocDuringStagedInsert(docID []byte, cb func(err error))
	BeforeRollbackDeleteInserted(docID []byte, cb func(err error))
	AfterDocCommittedBeforeSavingCAS(docID []byte, cb func(err error))
	AfterDocCommitted(docID []byte, cb func(err error))
	BeforeStagedInsert(docID []byte, cb func(err error))
	BeforeStagedRemove(docID []byte, cb func(err error))
	BeforeStagedReplace(docID []byte, cb func(err error))
	BeforeDocRemoved(docID []byte, cb func(err error))
	BeforeDocRolledBack(docID []byte, cb func(err error))
	AfterDocRemovedPreRetry(docID []byte, cb func(err error))
	AfterDocRemovedPostRetry(docID []byte, cb func(err error))
	AfterGetComplete(docID []byte, cb func(err error))
	AfterStagedReplaceComplete(docID []byte, cb func(err error))
	AfterStagedRemoveComplete(docID []byte, cb func(err error))
	AfterStagedInsertComplete(docID []byte, cb func(err error))
	AfterRollbackReplaceOrRemove(docID []byte, cb func(err error))
	AfterRollbackDeleteInserted(docID []byte, cb func(err error))
	BeforeCheckATREntryForBlockingDoc(docID []byte, cb func(err error))
	BeforeDocGet(docID []byte, cb func(err error))
	BeforeGetDocInExistsDuringStagedInsert(docID []byte, cb func(err error))
	AfterDocsCommitted(func(err error))
	AfterDocsRemoved(func(err error))
	AfterATRPending(func(err error))
	BeforeATRPending(func(err error))
	BeforeATRComplete(func(err error))
	BeforeATRRolledBack(func(err error))
	AfterATRComplete(func(err error))
	BeforeATRAborted(func(err error))
	AfterATRAborted(func(err error))
	AfterATRRolledBack(func(err error))
	RandomATRIDForVbucket(vbID []byte, cb func(string, error))
	HasExpiredClientSideHook(stage string, vbID []byte, cb func(bool, error))
}

// DefaultHooks is default set of noop hooks used within the library.
// Internal: This should never be used and is not supported.
type DefaultHooks struct {
}

func (dh *DefaultHooks) BeforeATRCommit(cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) AfterATRCommit(cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) BeforeDocCommitted(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) BeforeRemovingDocDuringStagedInsert(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) BeforeRollbackDeleteInserted(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) AfterDocCommittedBeforeSavingCAS(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) AfterDocCommitted(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) BeforeStagedInsert(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) BeforeStagedRemove(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) BeforeStagedReplace(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) BeforeDocRemoved(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) BeforeDocRolledBack(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) AfterDocRemovedPreRetry(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) AfterDocRemovedPostRetry(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) AfterGetComplete(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) AfterStagedReplaceComplete(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) AfterStagedRemoveComplete(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) AfterStagedInsertComplete(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) AfterRollbackReplaceOrRemove(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) AfterRollbackDeleteInserted(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) BeforeCheckATREntryForBlockingDoc(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) BeforeDocGet(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) BeforeGetDocInExistsDuringStagedInsert(docID []byte, cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) AfterDocsCommitted(cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) AfterDocsRemoved(cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) AfterATRPending(cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) BeforeATRPending(cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) BeforeATRComplete(cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) BeforeATRRolledBack(cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) AfterATRComplete(cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) BeforeATRAborted(cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) AfterATRAborted(cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) AfterATRRolledBack(cb func(err error)) {
	cb(nil)
}

func (dh *DefaultHooks) RandomATRIDForVbucket(vbID []byte, cb func(string, error)) {
	cb("", nil)
}

func (dh *DefaultHooks) HasExpiredClientSideHook(stage string, docID []byte, cb func(bool, error)) {
	cb(false, nil)
}
