package transactions

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/couchbase/gocbcore/v9"
	"github.com/google/uuid"
)

// Transactions is the top level wrapper object for all transactions
// handling.  It also manages the cleanup process in the background.
type Transactions struct {
	config  Config
	cleaner Cleaner
}

// Init will initialize the transactions library and return a Transactions
// object which can be used to perform transactions.
func Init(config *Config) (*Transactions, error) {
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
	if config.CleanupQueueSize == 0 {
		config.CleanupQueueSize = 100000
	}

	t := &Transactions{
		config: *config,
	}

	if config.CleanupClientAttempts {
		t.cleaner = startCleanupThread(config)
	} else {
		t.cleaner = &noopCleaner{}
	}

	return t, nil
}

// Config returns the config that was used during the initialization
// of this Transactions object.
func (t *Transactions) Config() Config {
	return t.config
}

// BeginTransaction will begin a new transaction.  The returned object can be used
// to begin a new attempt and subsequently perform operations before finally committing.
func (t *Transactions) BeginTransaction(perConfig *PerTransactionConfig) (*Transaction, error) {
	transactionUUID := uuid.New().String()

	expirationTime := t.config.ExpirationTime
	durabilityLevel := t.config.DurabilityLevel
	operationTimeout := t.config.KeyValueTimeout

	if perConfig != nil {
		if perConfig.ExpirationTime != 0 {
			expirationTime = perConfig.ExpirationTime
		}
		if perConfig.DurabilityLevel != DurabilityLevelUnknown {
			durabilityLevel = perConfig.DurabilityLevel
		}
		if perConfig.KeyValueTimeout != 0 {
			operationTimeout = perConfig.KeyValueTimeout
		}
	}

	now := time.Now()
	return &Transaction{
		parent:            t,
		expiryTime:        now.Add(expirationTime),
		startTime:         now,
		durabilityLevel:   durabilityLevel,
		transactionID:     transactionUUID,
		operationTimeout:  operationTimeout,
		hooks:             t.config.Internal.Hooks,
		addCleanupRequest: t.cleaner.AddRequest,
		serialUnstaging:   t.config.Internal.SerialUnstaging,
	}, nil
}

// ResumeTransactionAttempt allows the resumption of an existing transaction attempt
// which was previously serialized, potentially by a different transaction client.
func (t *Transactions) ResumeTransactionAttempt(txnBytes []byte) (*Transaction, error) {
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
	if txnData.Config.OperationTimeoutMs <= 0 {
		return nil, errors.New("invalid txn data - operation timeout must be greater than 0")
	}
	if txnData.Config.NumAtrs <= 0 || txnData.Config.NumAtrs > 1024 {
		return nil, errors.New("invalid txn data - num atrs must be greater than 0 and less than 1024")
	}

	transactionUUID := txnData.ID.Transaction

	durabilityLevel, err := durabilityLevelFromString(txnData.Config.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	expirationTime := time.Duration(txnData.State.TimeLeftMs) * time.Millisecond
	operationTimeout := time.Duration(txnData.Config.OperationTimeoutMs) * time.Millisecond

	now := time.Now()
	txn := &Transaction{
		parent:            t,
		expiryTime:        now.Add(expirationTime),
		startTime:         now,
		durabilityLevel:   durabilityLevel,
		transactionID:     transactionUUID,
		operationTimeout:  operationTimeout,
		hooks:             t.config.Internal.Hooks,
		addCleanupRequest: t.cleaner.AddRequest,
	}

	err = txn.resumeAttempt(&txnData)
	if err != nil {
		return nil, err
	}

	return txn, nil
}

// Close will shut down this Transactions object, shutting down all
// background tasks associated with it.
func (t *Transactions) Close() error {
	t.cleaner.Close()

	return nil
}

// TransactionsInternal exposes internal methods that are useful for testing and/or
// other forms of internal use.
type TransactionsInternal struct {
	parent *Transactions
}

// Internal returns an TransactionsInternal object which can be used for specialized
// internal use cases.
func (t *Transactions) Internal() *TransactionsInternal {
	return &TransactionsInternal{
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
func (t *TransactionsInternal) CreateGetResult(opts CreateGetResultOptions) *GetResult {
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
func (t *TransactionsInternal) ForceCleanupQueue(cb func([]CleanupAttempt)) {
	t.parent.cleaner.ForceCleanupQueue(cb)
}

// CleanupQueueLength returns the current length of the client cleanup queue.
func (t *TransactionsInternal) CleanupQueueLength() int32 {
	return t.parent.cleaner.QueueLength()
}
