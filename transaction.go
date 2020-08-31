package transactions

import (
	"encoding/json"
	"errors"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v9"
	"github.com/google/uuid"
)

// Transaction represents a single active transaction, it can be used to
// stage mutations and finally commit them.
type Transaction struct {
	expiryTime       time.Time
	keyValueTimeout  time.Duration
	kvDurableTimeout time.Duration
	durabilityLevel  DurabilityLevel

	transactionID string
	attempt       *transactionAttempt
	hooks         TransactionHooks
}

// ID returns the transaction ID of this transaction.
func (t *Transaction) ID() string {
	return t.transactionID
}

// Attempt returns meta-data about the current attempt to complete the transaction.
func (t *Transaction) Attempt() Attempt {
	return Attempt{
		State:         t.attempt.state,
		ID:            t.attempt.id,
		MutationState: t.attempt.finalMutationTokens,

		Internal: struct {
			Expired bool
		}{
			Expired: hasExpired(t.expiryTime),
		},
	}
}

// NewAttempt begins a new attempt with this transaction.
func (t *Transaction) NewAttempt() error {
	attemptUUID := uuid.New().String()

	t.attempt = &transactionAttempt{
		expiryTime:      t.expiryTime,
		keyValueTimeout: t.keyValueTimeout,
		durabilityLevel: t.durabilityLevel,
		transactionID:   t.transactionID,

		id:                  attemptUUID,
		state:               AttemptStateNothingWritten,
		stagedMutations:     nil,
		finalMutationTokens: nil,
		atrAgent:            nil,
		atrScopeName:        "",
		atrCollectionName:   "",
		atrKey:              nil,
		expiryOvertimeMode:  false,
		hooks:               t.hooks,
	}

	return nil
}

// GetOptions provides options for a Get operation.
type GetOptions struct {
	Agent          *gocbcore.Agent
	ScopeName      string
	CollectionName string
	Key            []byte
}

// MutableItemMeta represents all the meta-data for a fetched
// item.  Most of this is used for later mutation operations.
type MutableItemMeta struct {
	RevID   string        `json:"revid,omitempty"`
	Expiry  uint          `json:"expiry,omitempty"`
	Deleted bool          `json:"deleted,omitempty"`
	TxnMeta *jsonTxnXattr `json:"txn,omitempty"`
}

// GetResult represents the result of a Get or GetOptional operation.
type GetResult struct {
	agent          *gocbcore.Agent
	scopeName      string
	collectionName string
	key            []byte

	Meta  MutableItemMeta
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

// InsertOptions provides options for a Insert operation.
type InsertOptions struct {
	Agent          *gocbcore.Agent
	ScopeName      string
	CollectionName string
	Key            []byte
	Value          json.RawMessage
}

// StoreCallback describes a callback for a completed Replace operation.
type StoreCallback func(*GetResult, error)

// GetMutations returns a list of all the current mutations that have been performed
// under this transaction.
func (t *Transaction) GetMutations() []StagedMutation {
	if t.attempt == nil {
		return nil
	}

	return t.attempt.GetMutations()
}

// Insert will attempt to insert a document.
func (t *Transaction) Insert(opts InsertOptions, cb StoreCallback) error {
	if t.attempt == nil {
		return ErrNoAttempt
	}

	return t.attempt.Insert(opts, cb)
}

// ReplaceOptions provides options for a Replace operation.
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

// RemoveOptions provides options for a Remove operation.
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

// SerializeAttempt will serialize the current transaction attempt, allowing it
// to be resumed later, potentially under a different transactions client.
func (t *Transaction) SerializeAttempt() ([]byte, error) {
	return nil, errors.New("not implemented")
}
