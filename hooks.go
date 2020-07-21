package transactions

// TransactionHooks provides a number of internal hooks used for testing.
// Internal: This should never be used and is not supported.
type TransactionHooks interface {
	BeforeATRCommit() error
	AfterATRCommit() error
	BeforeDocCommitted(docID []byte) error
	BeforeRemovingDocDuringStagedInsert(docID []byte) error
	BeforeRollbackDeleteInserted(docID []byte) error
	AfterDocCommittedBeforeSavingCAS(docID []byte) error
	AfterDocCommitted(docID []byte) error
	BeforeStagedInsert(docID []byte) error
	BeforeStagedRemove(docID []byte) error
	BeforeStagedReplace(docID []byte) error
	BeforeDocRemoved(docID []byte) error
	BeforeDocRolledBack(docID []byte) error
	AfterDocRemovedPreRetry(docID []byte) error
	AfterDocRemovedPostRetry(docID []byte) error
	AfterGetComplete(docID []byte) error
	AfterStagedReplaceComplete(docID []byte) error
	AfterStagedRemoveComplete(docID []byte) error
	AfterStagedInsertComplete(docID []byte) error
	AfterRollbackReplaceOrRemove(docID []byte) error
	AfterRollbackDeleteInserted(docID []byte) error
	BeforeCheckATREntryForBlockingDoc(docID []byte) error
	BeforeDocGet(docID []byte) error
	BeforeGetDocInExistsDuringStagedInsert(docID []byte) error
	AfterDocsCommitted() error
	AfterDocsRemoved() error
	AfterATRPending() error
	BeforeATRPending() error
	BeforeATRComplete() error
	BeforeATRRolledBack() error
	AfterATRComplete() error
	BeforeATRAborted() error
	AfterATRAborted() error
	AfterATRRolledBack() error
	RandomATRIDForVbucket(vbID []byte) (string, error)
	HasExpiredClientSideHook(stage string, vbID []byte) (bool, error)
}

type defaultHooks struct {
}

func (dh *defaultHooks) BeforeATRCommit() error {
	return nil
}

func (dh *defaultHooks) AfterATRCommit() error {
	return nil
}

func (dh *defaultHooks) BeforeDocCommitted(docID []byte) error {
	return nil
}

func (dh *defaultHooks) BeforeRemovingDocDuringStagedInsert(docID []byte) error {
	return nil
}

func (dh *defaultHooks) BeforeRollbackDeleteInserted(docID []byte) error {
	return nil
}

func (dh *defaultHooks) AfterDocCommittedBeforeSavingCAS(docID []byte) error {
	return nil
}

func (dh *defaultHooks) AfterDocCommitted(docID []byte) error {
	return nil
}

func (dh *defaultHooks) BeforeStagedInsert(docID []byte) error {
	return nil
}

func (dh *defaultHooks) BeforeStagedRemove(docID []byte) error {
	return nil
}

func (dh *defaultHooks) BeforeStagedReplace(docID []byte) error {
	return nil
}

func (dh *defaultHooks) BeforeDocRemoved(docID []byte) error {
	return nil
}

func (dh *defaultHooks) BeforeDocRolledBack(docID []byte) error {
	return nil
}

func (dh *defaultHooks) AfterDocRemovedPreRetry(docID []byte) error {
	return nil
}

func (dh *defaultHooks) AfterDocRemovedPostRetry(docID []byte) error {
	return nil
}

func (dh *defaultHooks) AfterGetComplete(docID []byte) error {
	return nil
}

func (dh *defaultHooks) AfterStagedReplaceComplete(docID []byte) error {
	return nil
}

func (dh *defaultHooks) AfterStagedRemoveComplete(docID []byte) error {
	return nil
}

func (dh *defaultHooks) AfterStagedInsertComplete(docID []byte) error {
	return nil
}

func (dh *defaultHooks) AfterRollbackReplaceOrRemove(docID []byte) error {
	return nil
}

func (dh *defaultHooks) AfterRollbackDeleteInserted(docID []byte) error {
	return nil
}

func (dh *defaultHooks) BeforeCheckATREntryForBlockingDoc(docID []byte) error {
	return nil
}

func (dh *defaultHooks) BeforeDocGet(docID []byte) error {
	return nil
}

func (dh *defaultHooks) BeforeGetDocInExistsDuringStagedInsert(docID []byte) error {
	return nil
}

func (dh *defaultHooks) AfterDocsCommitted() error {
	return nil
}

func (dh *defaultHooks) AfterDocsRemoved() error {
	return nil
}

func (dh *defaultHooks) AfterATRPending() error {
	return nil
}

func (dh *defaultHooks) BeforeATRPending() error {
	return nil
}

func (dh *defaultHooks) BeforeATRComplete() error {
	return nil
}

func (dh *defaultHooks) BeforeATRRolledBack() error {
	return nil
}

func (dh *defaultHooks) AfterATRComplete() error {
	return nil
}

func (dh *defaultHooks) BeforeATRAborted() error {
	return nil
}

func (dh *defaultHooks) AfterATRAborted() error {
	return nil
}

func (dh *defaultHooks) AfterATRRolledBack() error {
	return nil
}

func (dh *defaultHooks) RandomATRIDForVbucket(vbID []byte) (string, error) {
	return "", nil
}

func (dh *defaultHooks) HasExpiredClientSideHook(stage string, docID []byte) (bool, error) {
	return false, nil
}
