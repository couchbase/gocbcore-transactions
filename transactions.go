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
	"time"

	"github.com/couchbase/gocbcore/v9"
	"github.com/google/uuid"
)

// Manager is the top level wrapper object for all transactions
// handling.  It also manages the cleanup process in the background.
type Manager struct {
	config      Config
	cleaner     Cleaner
	lostCleanup lostTransactionCleaner
}

// Init will initialize the transactions library and return a Manager
// object which can be used to perform transactions.
func Init(config *Config) (*Manager, error) {
	defaultConfig := &Config{
		ExpirationTime:        10000 * time.Millisecond,
		DurabilityLevel:       DurabilityLevelMajority,
		KeyValueTimeout:       2500 * time.Millisecond,
		CleanupWindow:         60000 * time.Millisecond,
		CleanupClientAttempts: true,
		CleanupLostAttempts:   true,
		BucketAgentProvider: func(bucketName string) (*gocbcore.Agent, error) {
			return nil, errors.New("no bucket agent provider was specified")
		},
	}

	if config == nil {
		config = defaultConfig
	}

	if config.ExpirationTime == 0 {
		config.ExpirationTime = defaultConfig.ExpirationTime
	}
	if config.KeyValueTimeout == 0 {
		config.KeyValueTimeout = defaultConfig.KeyValueTimeout
	}
	if config.CleanupWindow == 0 {
		config.CleanupWindow = defaultConfig.CleanupWindow
	}
	if config.BucketAgentProvider == nil {
		config.BucketAgentProvider = defaultConfig.BucketAgentProvider
	}
	if config.Internal.Hooks == nil {
		config.Internal.Hooks = &DefaultHooks{}
	}
	if config.Internal.CleanUpHooks == nil {
		config.Internal.CleanUpHooks = &DefaultCleanupHooks{}
	}
	if config.Internal.ClientRecordHooks == nil {
		config.Internal.ClientRecordHooks = &DefaultClientRecordHooks{}
	}
	if config.CleanupQueueSize == 0 {
		config.CleanupQueueSize = 100000
	}
	if config.Internal.NumATRs == 0 {
		config.Internal.NumATRs = 1024
	}

	t := &Manager{
		config: *config,
	}

	if config.CleanupClientAttempts {
		t.cleaner = startCleanupThread(config)
	} else {
		t.cleaner = &noopCleaner{}
	}

	if config.CleanupLostAttempts {
		t.lostCleanup = startLostTransactionCleaner(config)
	} else {
		t.lostCleanup = &noopLostTransactionCleaner{}
	}

	return t, nil
}

// Config returns the config that was used during the initialization
// of this Manager object.
func (t *Manager) Config() Config {
	return t.config
}

// BeginTransaction will begin a new transaction.  The returned object can be used
// to begin a new attempt and subsequently perform operations before finally committing.
func (t *Manager) BeginTransaction(perConfig *PerTransactionConfig) (*Transaction, error) {
	transactionUUID := uuid.New().String()

	expirationTime := t.config.ExpirationTime
	durabilityLevel := t.config.DurabilityLevel
	keyValueTimeout := t.config.KeyValueTimeout
	customATRLocation := t.config.CustomATRLocation
	bucketAgentProvider := t.config.BucketAgentProvider

	if perConfig != nil {
		if perConfig.ExpirationTime != 0 {
			expirationTime = perConfig.ExpirationTime
		}
		if perConfig.DurabilityLevel != DurabilityLevelUnknown {
			durabilityLevel = perConfig.DurabilityLevel
		}
		if perConfig.KeyValueTimeout != 0 {
			keyValueTimeout = perConfig.KeyValueTimeout
		}
		if perConfig.CustomATRLocation.Agent != nil {
			customATRLocation = perConfig.CustomATRLocation
		}
		if perConfig.BucketAgentProvider != nil {
			bucketAgentProvider = perConfig.BucketAgentProvider
		}
	}

	now := time.Now()
	return &Transaction{
		parent:                  t,
		expiryTime:              now.Add(expirationTime),
		startTime:               now,
		durabilityLevel:         durabilityLevel,
		transactionID:           transactionUUID,
		keyValueTimeout:         keyValueTimeout,
		atrLocation:             customATRLocation,
		addCleanupRequest:       t.addCleanupRequest,
		hooks:                   t.config.Internal.Hooks,
		enableNonFatalGets:      t.config.Internal.EnableNonFatalGets,
		enableParallelUnstaging: t.config.Internal.EnableParallelUnstaging,
		enableExplicitATRs:      t.config.Internal.EnableExplicitATRs,
		enableMutationCaching:   t.config.Internal.EnableMutationCaching,
		bucketAgentProvider:     bucketAgentProvider,
	}, nil
}

// ResumeTransactionAttempt allows the resumption of an existing transaction attempt
// which was previously serialized, potentially by a different transaction client.
func (t *Manager) ResumeTransactionAttempt(txnBytes []byte) (*Transaction, error) {
	var txnData jsonSerializedAttempt
	err := json.Unmarshal(txnBytes, &txnData)
	if err != nil {
		return nil, err
	}

	if txnData.ID.Transaction == "" {
		return nil, errors.New("invalid txn data - no transaction id")
	}
	if txnData.Config.DurabilityLevel == "" {
		return nil, errors.New("invalid txn data - no durability level")
	}
	if txnData.State.TimeLeftMs <= 0 {
		return nil, errors.New("invalid txn data - time left must be greater than 0")
	}
	if txnData.Config.KeyValueTimeoutMs <= 0 {
		return nil, errors.New("invalid txn data - operation timeout must be greater than 0")
	}
	if txnData.Config.NumAtrs <= 0 || txnData.Config.NumAtrs > 1024 {
		return nil, errors.New("invalid txn data - num atrs must be greater than 0 and less than 1024")
	}

	atrLocation := ATRLocation{}
	if txnData.ATR.Bucket != "" && txnData.ATR.ID == "" {
		// ATR references the specific ATR for this transaction.

		foundAtrAgent, err := t.config.BucketAgentProvider(txnData.ATR.Bucket)
		if err != nil {
			return nil, err
		}

		atrLocation = ATRLocation{
			Agent:          foundAtrAgent,
			ScopeName:      txnData.ATR.Scope,
			CollectionName: txnData.ATR.Collection,
		}
	} else {
		// No ATR information means its pending with no custom.

		atrLocation = ATRLocation{
			Agent:          nil,
			ScopeName:      "",
			CollectionName: "",
		}
	}

	transactionUUID := txnData.ID.Transaction

	durabilityLevel, err := durabilityLevelFromString(txnData.Config.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	expirationTime := time.Duration(txnData.State.TimeLeftMs) * time.Millisecond
	keyValueTimeout := time.Duration(txnData.Config.KeyValueTimeoutMs) * time.Millisecond

	now := time.Now()
	txn := &Transaction{
		parent:                  t,
		expiryTime:              now.Add(expirationTime),
		startTime:               now,
		durabilityLevel:         durabilityLevel,
		transactionID:           transactionUUID,
		keyValueTimeout:         keyValueTimeout,
		atrLocation:             atrLocation,
		addCleanupRequest:       t.addCleanupRequest,
		hooks:                   t.config.Internal.Hooks,
		enableNonFatalGets:      t.config.Internal.EnableNonFatalGets,
		enableParallelUnstaging: t.config.Internal.EnableParallelUnstaging,
		enableExplicitATRs:      t.config.Internal.EnableExplicitATRs,
		enableMutationCaching:   t.config.Internal.EnableMutationCaching,
		bucketAgentProvider:     t.config.BucketAgentProvider,
	}

	err = txn.resumeAttempt(&txnData)
	if err != nil {
		return nil, err
	}

	return txn, nil
}

// Close will shut down this Manager object, shutting down all
// background tasks associated with it.
func (t *Manager) Close() error {
	t.cleaner.Close()
	t.lostCleanup.Close()

	return nil
}

func (t *Manager) addCleanupRequest(req *CleanupRequest) bool {
	return t.cleaner.AddRequest(req)
}

// ManagerInternal exposes internal methods that are useful for testing and/or
// other forms of internal use.
type ManagerInternal struct {
	parent *Manager
}

// Internal returns an ManagerInternal object which can be used for specialized
// internal use cases.
func (t *Manager) Internal() *ManagerInternal {
	return &ManagerInternal{
		parent: t,
	}
}

// CreateGetResultOptions exposes options for the Internal CreateGetResult method.
type CreateGetResultOptions struct {
	Agent          *gocbcore.Agent
	ScopeName      string
	CollectionName string
	Key            []byte
	Cas            gocbcore.Cas
	Meta           *MutableItemMeta
}

// CreateGetResult creates a false GetResult which can be used with Replace/Remove operations
// where the original GetResult is no longer available.
func (t *ManagerInternal) CreateGetResult(opts CreateGetResultOptions) *GetResult {
	return &GetResult{
		agent:          opts.Agent,
		scopeName:      opts.ScopeName,
		collectionName: opts.CollectionName,
		key:            opts.Key,
		Meta:           opts.Meta,

		Value: nil,
		Cas:   opts.Cas,
	}
}

// ForceCleanupQueue forces the transactions client cleanup queue to drain without waiting for expirations.
func (t *ManagerInternal) ForceCleanupQueue(cb func([]CleanupAttempt)) {
	t.parent.cleaner.ForceCleanupQueue(cb)
}

// CleanupQueueLength returns the current length of the client cleanup queue.
func (t *ManagerInternal) CleanupQueueLength() int32 {
	return t.parent.cleaner.QueueLength()
}
