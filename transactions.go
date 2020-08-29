package transactions

import (
	"errors"
	"time"

	"github.com/couchbase/gocbcore/v9"
	"github.com/google/uuid"
)

// Transactions is the top level wrapper object for all transactions
// handling.  It also manages the cleanup process in the background.
type Transactions struct {
	config Config
}

// Init will initialize the transactions library and return a Transactions
// object which can be used to perform transactions.
func Init(config *Config) (*Transactions, error) {
	defaultConfig := &Config{
		ExpirationTime:        10000 * time.Millisecond,
		DurabilityLevel:       DurabilityLevelMajority,
		KeyValueTimeout:       2500 * time.Millisecond,
		KvDurableTimeout:      2500 * time.Millisecond,
		CleanupWindow:         60000 * time.Millisecond,
		CleanupClientAttempts: true,
		CleanupLostAttempts:   true,
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
	if config.KvDurableTimeout == 0 {
		config.KvDurableTimeout = defaultConfig.KvDurableTimeout
	}
	if config.CleanupWindow == 0 {
		config.CleanupWindow = defaultConfig.CleanupWindow
	}
	if config.Internal.Hooks == nil {
		config.Internal.Hooks = &DefaultHooks{}
	}

	return &Transactions{
		config: *config,
	}, nil
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
	keyValueTimeout := t.config.KeyValueTimeout
	kvDurableTimeout := t.config.KvDurableTimeout

	if perConfig != nil {
		if perConfig.ExpirationTime != 0 {
			expirationTime = perConfig.ExpirationTime
		}
		if perConfig.DurabilityLevel != DurabilityLevelUnset {
			durabilityLevel = perConfig.DurabilityLevel
		}
		if perConfig.KeyValueTimeout != 0 {
			keyValueTimeout = perConfig.KeyValueTimeout
		}
		if perConfig.KvDurableTimeout != 0 {
			kvDurableTimeout = perConfig.KvDurableTimeout
		}
	}

	return &Transaction{
		expiryTime:       time.Now().Add(expirationTime),
		durabilityLevel:  durabilityLevel,
		transactionID:    transactionUUID,
		keyValueTimeout:  keyValueTimeout,
		kvDurableTimeout: kvDurableTimeout,
		hooks:            t.config.Internal.Hooks,
	}, nil
}

// ResumeTransactionAttempt allows the resumption of an existing transaction attempt
// which was previously serialized, potentially by a different transaction client.
func (t *Transactions) ResumeTransactionAttempt(txnData []byte) (*Transaction, error) {
	return nil, errors.New("not implemented")
}

// Close will shut down this Transactions object, shutting down all
// background tasks associated with it.
func (t *Transactions) Close() error {
	return errors.New("not implemented")
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
	Meta           MutableItemMeta
}

// CreateGetResult creates a false GetResult which can be used with Replace/Remove operations
// where the original GetResult is no longer available.
func (t *TransactionsInternal) CreateGetResult(opts CreateGetResultOptions) *GetResult {
	return &GetResult{
		agent:          opts.Agent,
		scopeName:      opts.ScopeName,
		collectionName: opts.CollectionName,
		key:            opts.Key,
		meta:           opts.Meta,

		Value: nil,
		Cas:   opts.Cas,
	}
}
