// Copyright 2021 Couchbase
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	BeforeRemoveStagedInsert(docID []byte, cb func(err error))
	AfterRemoveStagedInsert(docID []byte, cb func(err error))
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
	BeforeATRCommitAmbiguityResolution(func(err error))
	RandomATRIDForVbucket(cb func(string, error))
	HasExpiredClientSideHook(stage string, docID []byte, cb func(bool, error))
}

// CleanUpHooks provides a number of internal hooks used for testing.
// Internal: This should never be used and is not supported.
type CleanUpHooks interface {
	BeforeATRGet(id []byte, cb func(error))
	BeforeDocGet(id []byte, cb func(error))
	BeforeRemoveLinks(id []byte, cb func(error))
	BeforeCommitDoc(id []byte, cb func(error))
	BeforeRemoveDocStagedForRemoval(id []byte, cb func(error))
	BeforeRemoveDoc(id []byte, cb func(error))
	BeforeATRRemove(id []byte, cb func(error))
}

// ClientRecordHooks provides a number of internal hooks used for testing.
// Internal: This should never be used and is not supported.
type ClientRecordHooks interface {
	BeforeCreateRecord(cb func(error))
	BeforeRemoveClient(cb func(error))
	BeforeUpdateCAS(cb func(error))
	BeforeGetRecord(cb func(error))
	BeforeUpdateRecord(cb func(error))
}

// DefaultHooks is default set of noop hooks used within the library.
// Internal: This should never be used and is not supported.
type DefaultHooks struct {
}

// BeforeATRCommit occurs before an ATR is committed.
func (dh *DefaultHooks) BeforeATRCommit(cb func(err error)) {
	cb(nil)
}

// AfterATRCommit occurs after an ATR is committed.
func (dh *DefaultHooks) AfterATRCommit(cb func(err error)) {
	cb(nil)
}

// BeforeDocCommitted occurs before a document is committed.
func (dh *DefaultHooks) BeforeDocCommitted(docID []byte, cb func(err error)) {
	cb(nil)
}

// BeforeRemovingDocDuringStagedInsert occurs before removing a document during staged insert.
func (dh *DefaultHooks) BeforeRemovingDocDuringStagedInsert(docID []byte, cb func(err error)) {
	cb(nil)
}

// BeforeRollbackDeleteInserted occurs before rolling back a delete.
func (dh *DefaultHooks) BeforeRollbackDeleteInserted(docID []byte, cb func(err error)) {
	cb(nil)
}

// AfterDocCommittedBeforeSavingCAS occurs after committed a document before saving the CAS.
func (dh *DefaultHooks) AfterDocCommittedBeforeSavingCAS(docID []byte, cb func(err error)) {
	cb(nil)
}

// AfterDocCommitted occurs after a document is committed.
func (dh *DefaultHooks) AfterDocCommitted(docID []byte, cb func(err error)) {
	cb(nil)
}

// BeforeStagedInsert occurs before staging an insert.
func (dh *DefaultHooks) BeforeStagedInsert(docID []byte, cb func(err error)) {
	cb(nil)
}

// BeforeStagedRemove occurs before staging a remove.
func (dh *DefaultHooks) BeforeStagedRemove(docID []byte, cb func(err error)) {
	cb(nil)
}

// BeforeStagedReplace occurs before staging a replace.
func (dh *DefaultHooks) BeforeStagedReplace(docID []byte, cb func(err error)) {
	cb(nil)
}

// BeforeDocRemoved occurs before removing a document.
func (dh *DefaultHooks) BeforeDocRemoved(docID []byte, cb func(err error)) {
	cb(nil)
}

// BeforeDocRolledBack occurs before a document is rolled back.
func (dh *DefaultHooks) BeforeDocRolledBack(docID []byte, cb func(err error)) {
	cb(nil)
}

// AfterDocRemovedPreRetry occurs after removing a document before retry.
func (dh *DefaultHooks) AfterDocRemovedPreRetry(docID []byte, cb func(err error)) {
	cb(nil)
}

// AfterDocRemovedPostRetry occurs after removing a document after retry.
func (dh *DefaultHooks) AfterDocRemovedPostRetry(docID []byte, cb func(err error)) {
	cb(nil)
}

// AfterGetComplete occurs after a get completes.
func (dh *DefaultHooks) AfterGetComplete(docID []byte, cb func(err error)) {
	cb(nil)
}

// AfterStagedReplaceComplete occurs after staging a replace is completed.
func (dh *DefaultHooks) AfterStagedReplaceComplete(docID []byte, cb func(err error)) {
	cb(nil)
}

// AfterStagedRemoveComplete occurs after staging a remove is completed.
func (dh *DefaultHooks) AfterStagedRemoveComplete(docID []byte, cb func(err error)) {
	cb(nil)
}

// AfterStagedInsertComplete occurs after staging an insert is completed.
func (dh *DefaultHooks) AfterStagedInsertComplete(docID []byte, cb func(err error)) {
	cb(nil)
}

// AfterRollbackReplaceOrRemove occurs after rolling back a replace or remove.
func (dh *DefaultHooks) AfterRollbackReplaceOrRemove(docID []byte, cb func(err error)) {
	cb(nil)
}

// AfterRollbackDeleteInserted occurs after rolling back a delete.
func (dh *DefaultHooks) AfterRollbackDeleteInserted(docID []byte, cb func(err error)) {
	cb(nil)
}

// BeforeCheckATREntryForBlockingDoc occurs before checking the ATR of a blocking document.
func (dh *DefaultHooks) BeforeCheckATREntryForBlockingDoc(docID []byte, cb func(err error)) {
	cb(nil)
}

// BeforeDocGet occurs before a document is fetched.
func (dh *DefaultHooks) BeforeDocGet(docID []byte, cb func(err error)) {
	cb(nil)
}

// BeforeGetDocInExistsDuringStagedInsert occurs before getting a document for an insert.
func (dh *DefaultHooks) BeforeGetDocInExistsDuringStagedInsert(docID []byte, cb func(err error)) {
	cb(nil)
}

// BeforeRemoveStagedInsert occurs before removing a staged insert.
func (dh *DefaultHooks) BeforeRemoveStagedInsert(docID []byte, cb func(err error)) {
	cb(nil)
}

// AfterRemoveStagedInsert occurs after removing a staged insert.
func (dh *DefaultHooks) AfterRemoveStagedInsert(docID []byte, cb func(err error)) {
	cb(nil)
}

// AfterDocsCommitted occurs after all documents are committed.
func (dh *DefaultHooks) AfterDocsCommitted(cb func(err error)) {
	cb(nil)
}

// AfterDocsRemoved occurs after all documents are removed.
func (dh *DefaultHooks) AfterDocsRemoved(cb func(err error)) {
	cb(nil)
}

// AfterATRPending occurs after the ATR transitions to pending.
func (dh *DefaultHooks) AfterATRPending(cb func(err error)) {
	cb(nil)
}

// BeforeATRPending occurs before the ATR transitions to pending.
func (dh *DefaultHooks) BeforeATRPending(cb func(err error)) {
	cb(nil)
}

// BeforeATRComplete occurs before the ATR transitions to complete.
func (dh *DefaultHooks) BeforeATRComplete(cb func(err error)) {
	cb(nil)
}

// BeforeATRRolledBack occurs before the ATR transitions to rolled back.
func (dh *DefaultHooks) BeforeATRRolledBack(cb func(err error)) {
	cb(nil)
}

// AfterATRComplete occurs after the ATR transitions to complete.
func (dh *DefaultHooks) AfterATRComplete(cb func(err error)) {
	cb(nil)
}

// BeforeATRAborted occurs before the ATR transitions to aborted.
func (dh *DefaultHooks) BeforeATRAborted(cb func(err error)) {
	cb(nil)
}

// AfterATRAborted occurs after the ATR transitions to aborted.
func (dh *DefaultHooks) AfterATRAborted(cb func(err error)) {
	cb(nil)
}

// AfterATRRolledBack occurs after the ATR transitions to rolled back.
func (dh *DefaultHooks) AfterATRRolledBack(cb func(err error)) {
	cb(nil)
}

// BeforeATRCommitAmbiguityResolution occurs before ATR commit ambiguity resolution.
func (dh *DefaultHooks) BeforeATRCommitAmbiguityResolution(cb func(err error)) {
	cb(nil)
}

// RandomATRIDForVbucket generates a random ATRID for a vbucket.
func (dh *DefaultHooks) RandomATRIDForVbucket(cb func(string, error)) {
	cb("", nil)
}

// HasExpiredClientSideHook checks if a transaction has expired.
func (dh *DefaultHooks) HasExpiredClientSideHook(stage string, docID []byte, cb func(bool, error)) {
	cb(false, nil)
}

// DefaultCleanupHooks is default set of noop hooks used within the library.
// Internal: This should never be used and is not supported.
type DefaultCleanupHooks struct {
}

// BeforeATRGet happens before an ATR get.
func (dh *DefaultCleanupHooks) BeforeATRGet(id []byte, cb func(error)) {
	cb(nil)
}

// BeforeDocGet happens before an doc get.
func (dh *DefaultCleanupHooks) BeforeDocGet(id []byte, cb func(error)) {
	cb(nil)
}

// BeforeRemoveLinks happens before we remove links.
func (dh *DefaultCleanupHooks) BeforeRemoveLinks(id []byte, cb func(error)) {
	cb(nil)
}

// BeforeCommitDoc happens before we commit a document.
func (dh *DefaultCleanupHooks) BeforeCommitDoc(id []byte, cb func(error)) {
	cb(nil)
}

// BeforeRemoveDocStagedForRemoval happens before we remove a staged document.
func (dh *DefaultCleanupHooks) BeforeRemoveDocStagedForRemoval(id []byte, cb func(error)) {
	cb(nil)
}

// BeforeRemoveDoc happens before we remove a document.
func (dh *DefaultCleanupHooks) BeforeRemoveDoc(id []byte, cb func(error)) {
	cb(nil)
}

// BeforeATRRemove happens before we remove an ATR.
func (dh *DefaultCleanupHooks) BeforeATRRemove(id []byte, cb func(error)) {
	cb(nil)
}

// DefaultClientRecordHooks is default set of noop hooks used within the library.
// Internal: This should never be used and is not supported.
type DefaultClientRecordHooks struct {
}

// BeforeCreateRecord happens before we create a cleanup client record.
func (dh *DefaultClientRecordHooks) BeforeCreateRecord(cb func(error)) {
	cb(nil)
}

// BeforeRemoveClient happens before we remove a cleanup client record.
func (dh *DefaultClientRecordHooks) BeforeRemoveClient(cb func(error)) {
	cb(nil)
}

// BeforeUpdateCAS happens before we update a CAS.
func (dh *DefaultClientRecordHooks) BeforeUpdateCAS(cb func(error)) {
	cb(nil)
}

// BeforeGetRecord happens before we get a cleanup client record.
func (dh *DefaultClientRecordHooks) BeforeGetRecord(cb func(error)) {
	cb(nil)
}

// BeforeUpdateRecord happens before we update a cleanup client record.
func (dh *DefaultClientRecordHooks) BeforeUpdateRecord(cb func(error)) {
	cb(nil)
}

const (
	hookRollback           = "rollback"
	hookGet                = "get"
	hookInsert             = "insert"
	hookReplace            = "replace"
	hookRemove             = "remove"
	hookCommit             = "commit"
	hookAbortGetATR        = "abortGetAtr"
	hookRollbackDoc        = "rollbackDoc"
	hookDeleteInserted     = "deleteInserted"
	hookCreateStagedInsert = "createdStagedInsert"
	hookRemoveStagedInsert = "removeStagedInsert"
	hookRemoveDoc          = "removeDoc"
	hookCommitDoc          = "commitDoc"

	hookWWC = "writeWriteConflict"

	hookATRCommit                    = "atrCommit"
	hookATRCommitAmbiguityResolution = "atrCommitAmbiguityResolution"
	hookATRAbort                     = "atrAbort"
	hookATRRollback                  = "atrRollbackComplete"
	hookATRPending                   = "atrPending"
	hookATRComplete                  = "atrComplete"
)
