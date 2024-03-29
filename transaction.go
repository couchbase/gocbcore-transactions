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

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v10"
	"github.com/google/uuid"
)

type addCleanupRequest func(req *CleanupRequest) bool

// Transaction represents a single active transaction, it can be used to
// stage mutations and finally commit them.
type Transaction struct {
	parent *Manager

	expiryTime              time.Time
	startTime               time.Time
	keyValueTimeout         time.Duration
	durabilityLevel         DurabilityLevel
	enableParallelUnstaging bool
	enableNonFatalGets      bool
	enableExplicitATRs      bool
	enableMutationCaching   bool
	atrLocation             ATRLocation
	bucketAgentProvider     BucketAgentProviderFn

	transactionID string
	attempt       *transactionAttempt
	hooks         TransactionHooks

	addCleanupRequest addCleanupRequest
}

// ID returns the transaction ID of this transaction.
func (t *Transaction) ID() string {
	return t.transactionID
}

// Attempt returns meta-data about the current attempt to complete the transaction.
func (t *Transaction) Attempt() Attempt {
	if t.attempt == nil {
		return Attempt{}
	}

	return t.attempt.State()
}

// NewAttempt begins a new attempt with this transaction.
func (t *Transaction) NewAttempt() error {
	attemptUUID := uuid.New().String()

	t.attempt = &transactionAttempt{
		expiryTime:              t.expiryTime,
		txnStartTime:            t.startTime,
		keyValueTimeout:         t.keyValueTimeout,
		durabilityLevel:         t.durabilityLevel,
		transactionID:           t.transactionID,
		enableNonFatalGets:      t.enableNonFatalGets,
		enableParallelUnstaging: t.enableParallelUnstaging,
		enableMutationCaching:   t.enableMutationCaching,
		enableExplicitATRs:      t.enableExplicitATRs,
		atrLocation:             t.atrLocation,
		bucketAgentProvider:     t.bucketAgentProvider,

		id:                attemptUUID,
		state:             AttemptStateNothingWritten,
		stagedMutations:   nil,
		atrAgent:          nil,
		atrScopeName:      "",
		atrCollectionName: "",
		atrKey:            nil,
		hooks:             t.hooks,

		addCleanupRequest: t.addCleanupRequest,
	}

	return nil
}

func (t *Transaction) resumeAttempt(txnData *jsonSerializedAttempt) error {
	if txnData.ID.Attempt == "" {
		return errors.New("invalid txn data - no attempt id")
	}

	attemptUUID := txnData.ID.Attempt

	var txnState AttemptState
	var atrAgent *gocbcore.Agent
	var atrOboUser string
	var atrScope, atrCollection string
	var atrKey []byte
	if txnData.ATR.ID != "" {
		// ATR references the specific ATR for this transaction.

		if txnData.ATR.Bucket == "" {
			return errors.New("invalid atr data - no bucket")
		}

		foundAtrAgent, foundAtrOboUser, err := t.parent.config.BucketAgentProvider(txnData.ATR.Bucket)
		if err != nil {
			return err
		}

		txnState = AttemptStatePending
		atrAgent = foundAtrAgent
		atrOboUser = foundAtrOboUser
		atrScope = txnData.ATR.Scope
		atrCollection = txnData.ATR.Collection
		atrKey = []byte(txnData.ATR.ID)
	} else {
		// No ATR information means its pending with no custom.

		txnState = AttemptStateNothingWritten
		atrAgent = nil
		atrOboUser = ""
		atrScope = ""
		atrCollection = ""
		atrKey = nil
	}

	stagedMutations := make([]*stagedMutation, len(txnData.Mutations))
	for mutationIdx, mutationData := range txnData.Mutations {
		if mutationData.Bucket == "" {
			return errors.New("invalid staged mutation - no bucket")
		}
		if mutationData.ID == "" {
			return errors.New("invalid staged mutation - no key")
		}
		if mutationData.Cas == "" {
			return errors.New("invalid staged mutation - no cas")
		}
		if mutationData.Type == "" {
			return errors.New("invalid staged mutation - no type")
		}

		agent, oboUser, err := t.parent.config.BucketAgentProvider(mutationData.Bucket)
		if err != nil {
			return err
		}

		cas, err := strconv.ParseUint(mutationData.Cas, 10, 64)
		if err != nil {
			return err
		}

		opType, err := stagedMutationTypeFromString(mutationData.Type)
		if err != nil {
			return err
		}

		stagedMutations[mutationIdx] = &stagedMutation{
			OpType:         opType,
			Agent:          agent,
			OboUser:        oboUser,
			ScopeName:      mutationData.Scope,
			CollectionName: mutationData.Collection,
			Key:            []byte(mutationData.ID),
			Cas:            gocbcore.Cas(cas),
			Staged:         nil,
		}
	}

	t.attempt = &transactionAttempt{
		expiryTime:              t.expiryTime,
		txnStartTime:            t.startTime,
		keyValueTimeout:         t.keyValueTimeout,
		durabilityLevel:         t.durabilityLevel,
		transactionID:           t.transactionID,
		enableNonFatalGets:      t.enableNonFatalGets,
		enableParallelUnstaging: t.enableParallelUnstaging,
		enableMutationCaching:   t.enableMutationCaching,
		enableExplicitATRs:      t.enableExplicitATRs,
		atrLocation:             t.atrLocation,
		bucketAgentProvider:     t.bucketAgentProvider,

		id:                attemptUUID,
		state:             txnState,
		stagedMutations:   stagedMutations,
		atrAgent:          atrAgent,
		atrOboUser:        atrOboUser,
		atrScopeName:      atrScope,
		atrCollectionName: atrCollection,
		atrKey:            atrKey,
		hooks:             t.hooks,

		addCleanupRequest: t.addCleanupRequest,
	}

	return nil
}

// GetOptions provides options for a Get operation.
type GetOptions struct {
	Agent          *gocbcore.Agent
	OboUser        string
	ScopeName      string
	CollectionName string
	Key            []byte

	// NoRYOW will disable the RYOW logic used to enable transactions
	// to naturally read any mutations they have performed.
	// VOLATILE: This parameter is subject to change.
	NoRYOW bool
}

// MutableItemMetaATR represents the ATR for meta.
type MutableItemMetaATR struct {
	BucketName     string `json:"bkt"`
	ScopeName      string `json:"scp"`
	CollectionName string `json:"coll"`
	DocID          string `json:"key"`
}

// MutableItemMeta represents all the meta-data for a fetched
// item.  Most of this is used for later mutation operations.
type MutableItemMeta struct {
	TransactionID string                                 `json:"txn"`
	AttemptID     string                                 `json:"atmpt"`
	ATR           MutableItemMetaATR                     `json:"atr"`
	ForwardCompat map[string][]ForwardCompatibilityEntry `json:"fc,omitempty"`
}

// GetResult represents the result of a Get or GetOptional operation.
type GetResult struct {
	agent          *gocbcore.Agent
	oboUser        string
	scopeName      string
	collectionName string
	key            []byte

	Meta  *MutableItemMeta
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
	OboUser        string
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

// HasExpired indicates whether this attempt has expired.
func (t *Transaction) HasExpired() bool {
	if t.attempt == nil {
		return false
	}

	return t.attempt.HasExpired()
}

// CanCommit indicates whether this attempt can still be committed.
func (t *Transaction) CanCommit() bool {
	if t.attempt == nil {
		return false
	}

	return t.attempt.CanCommit()
}

// ShouldRollback indicates if this attempt should be rolled back.
func (t *Transaction) ShouldRollback() bool {
	if t.attempt == nil {
		return false
	}

	return t.attempt.ShouldRollback()
}

// ShouldRetry indicates if this attempt thinks we can retry.
func (t *Transaction) ShouldRetry() bool {
	if t.attempt == nil {
		return false
	}

	return t.attempt.ShouldRetry()
}

func (t *Transaction) TimeRemaining() time.Duration {
	if t.attempt == nil {
		return 0
	}

	return t.attempt.TimeRemaining()
}

// SerializeAttempt will serialize the current transaction attempt, allowing it
// to be resumed later, potentially under a different transactions client.  It
// is no longer safe to use this attempt once this has occurred, a new attempt
// must be started to use this object following this call.
func (t *Transaction) SerializeAttempt(cb func([]byte, error)) error {
	return t.attempt.Serialize(cb)
}

// GetMutations returns a list of all the current mutations that have been performed
// under this transaction.
func (t *Transaction) GetMutations() []StagedMutation {
	if t.attempt == nil {
		return nil
	}

	return t.attempt.GetMutations()
}

// GetATRLocation returns the ATR location for the current attempt, either by
// identifying where it was placed, or where it will be based on custom atr
// configurations.
func (t *Transaction) GetATRLocation() ATRLocation {
	if t.attempt != nil {
		return t.attempt.GetATRLocation()
	}

	return t.atrLocation
}

// SetATRLocation forces the ATR location for the current attempt to a specific
// location.  Note that this cannot be called if it has already been set.  This
// is currently only safe to call before any mutations have occurred.
func (t *Transaction) SetATRLocation(location ATRLocation) error {
	if t.attempt == nil {
		return errors.New("cannot set ATR location without an active attempt")
	}

	return t.attempt.SetATRLocation(location)
}

// Config returns the configured parameters for this transaction.
// Note that the Expiration time is adjusted based on the time left.
// Note also that after a transaction is resumed, the custom atr location
// may no longer reflect the originally configured value.
func (t *Transaction) Config() PerTransactionConfig {
	return PerTransactionConfig{
		CustomATRLocation: t.atrLocation,
		ExpirationTime:    t.TimeRemaining(),
		DurabilityLevel:   t.durabilityLevel,
		KeyValueTimeout:   t.keyValueTimeout,
	}
}

// UpdateStateOptions are the settings available to UpdateState.
// This function must only be called once the transaction has entered query mode.
// Internal: This should never be used and is not supported.
type UpdateStateOptions struct {
	ShouldNotCommit   bool
	ShouldNotRollback bool
	ShouldNotRetry    bool
	HasExpired        bool
	State             AttemptState
}

// UpdateState will update the internal state of the current attempt.
// Internal: This should never be used and is not supported.
func (t *Transaction) UpdateState(opts UpdateStateOptions) {
	if t.attempt == nil {
		return
	}

	t.attempt.UpdateState(opts)
}
