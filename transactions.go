package transactions

import (
	"errors"
	"time"

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
	if config.CleanupWindow == 0 {
		config.CleanupWindow = defaultConfig.CleanupWindow
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

	durabilityLevel := t.config.DurabilityLevel
	if perConfig != nil {
		durabilityLevel = perConfig.DurabilityLevel
	}

	return &Transaction{
		expiryTime:      time.Now().Add(t.config.ExpirationTime),
		durabilityLevel: durabilityLevel,
		transactionID:   transactionUUID,
		keyValueTimeout: t.config.KeyValueTimeout,
	}, nil
}

// Close will shut down this Transactions object, shutting down all
// background tasks associated with it.
func (t *Transactions) Close() error {
	return errors.New("not implemented")
}
