package transactions

import (
	"encoding/json"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v9"
	"github.com/google/uuid"
)

// TransactionHooks provides a number of internal hooks used for testing.
type TransactionHooks interface {
	BeforeStagedInsert(ctx *transactionAttempt, key []byte, cb func(error))
}

// Transaction represents a single active transaction, it can be used to
// stage mutations and finally commit them.
type Transaction struct {
	expiryTime      time.Time
	keyValueTimeout time.Duration
	durabilityLevel DurabilityLevel

	transactionID string
	attempt       *transactionAttempt
}

// NewAttempt begins a new attempt with this transaction
func (t *Transaction) NewAttempt() error {
	attemptUUID := uuid.New().String()

	t.attempt = &transactionAttempt{
		expiryTime:      t.expiryTime,
		keyValueTimeout: t.keyValueTimeout,
		durabilityLevel: t.durabilityLevel,
		transactionID:   t.transactionID,

		id:                  attemptUUID,
		state:               attemptStateNothingWritten,
		stagedMutations:     nil,
		finalMutationTokens: nil,
		atrAgent:            nil,
		atrScopeName:        "",
		atrCollectionName:   "",
		atrKey:              nil,
		expiryOvertimeMode:  false,
	}

	return nil
}

type GetOptions struct {
	Agent          *gocbcore.Agent
	ScopeName      string
	CollectionName string
	Key            []byte
}

// GetResult represents the result of a Get or GetOptional operation.
type GetResult struct {
	agent          *gocbcore.Agent
	scopeName      string
	collectionName string
	key            []byte
	revid          string
	expiry         uint

	Value []byte
	Cas   gocbcore.Cas
}

// GetCallback describes a callback for a completed Get or GetOptional operation.
type GetCallback func(*GetResult, error)

// Get will attempt to fetch a document, and fail the transaction if it does not exist.
func (t *Transaction) Get(opts GetOptions, cb GetCallback) error {
	if t.attempt == nil {
		return ErrNoAttempt
	}

	return t.attempt.Get(opts, cb)
}

type InsertOptions struct {
	Agent          *gocbcore.Agent
	ScopeName      string
	CollectionName string
	Key            []byte
	Value          json.RawMessage
}

// StoreCallback describes a callback for a completed Replace operation.
type StoreCallback func(*GetResult, error)

// Insert will attempt to insert a document.
func (t *Transaction) Insert(opts InsertOptions, cb StoreCallback) error {
	if t.attempt == nil {
		return ErrNoAttempt
	}

	return t.attempt.Insert(opts, cb)
}

type ReplaceOptions struct {
	Document *GetResult
	Value    json.RawMessage
}

// Replace will attempt to replace an existing document.
func (t *Transaction) Replace(opts ReplaceOptions, cb StoreCallback) error {
	if t.attempt == nil {
		return ErrNoAttempt
	}

	return t.attempt.Replace(opts, cb)
}

type RemoveOptions struct {
	Document *GetResult
}

// Remove will attempt to remove a previously fetched document.
func (t *Transaction) Remove(opts RemoveOptions, cb StoreCallback) error {
	if t.attempt == nil {
		return ErrNoAttempt
	}

	return t.attempt.Remove(opts, cb)
}

// CommitCallback describes a callback for a completed commit operation.
type CommitCallback func(error)

// Commit will attempt to commit the transaction, rolling it back and cancelling
// it if it is not capable of doing so.
func (t *Transaction) Commit(cb CommitCallback) error {
	if t.attempt == nil {
		return ErrNoAttempt
	}

	return t.attempt.Commit(cb)
}

// RollbackCallback describes a callback for a completed rollback operation.
type RollbackCallback func(error)

// Rollback will attempt to rollback the transaction.
func (t *Transaction) Rollback(cb RollbackCallback) error {
	if t.attempt == nil {
		return ErrNoAttempt
	}

	return t.attempt.Rollback(cb)
}
