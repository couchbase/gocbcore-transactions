package transactions

import (
	"time"
)

// DurabilityLevel specifies the durability level to use for a mutation.
type DurabilityLevel int

const (
	// DurabilityLevelUnset indicates to use the default level.
	DurabilityLevelUnset = DurabilityLevel(0)

	// DurabilityLevelNone indicates that no durability is needed.
	DurabilityLevelNone = DurabilityLevel(1)

	// DurabilityLevelMajority indicates the operation must be replicated to the majority.
	DurabilityLevelMajority = DurabilityLevel(2)

	// DurabilityLevelMajorityAndPersistToActive indicates the operation must be replicated
	// to the majority and persisted to the active server.
	DurabilityLevelMajorityAndPersistToActive = DurabilityLevel(3)

	// DurabilityLevelPersistToMajority indicates the operation must be persisted to the active server.
	DurabilityLevelPersistToMajority = DurabilityLevel(4)
)

// Config specifies various tunable options related to transactions.
type Config struct {
	// ExpirationTime sets the maximum time that transactions created
	// by this Transactions object can run for, before expiring.
	ExpirationTime time.Duration

	// DurabilityLevel specifies the durability level that should be used
	// for all write operations performed by this Transactions object.
	DurabilityLevel DurabilityLevel

	// KeyValueTimeout specifies the default timeout used for all KV writes.
	KeyValueTimeout time.Duration

	// CleanupWindow specifies how often to the cleanup process runs
	// attempting to garbage collection transactions that have failed but
	// were not cleaned up by the previous client.
	CleanupWindow time.Duration

	// CleanupClientAttempts controls where any transaction attempts made
	// by this client are automatically removed.
	CleanupClientAttempts bool

	// CleanupLostAttempts controls where a background process is created
	// to cleanup any ‘lost’ transaction attempts.
	CleanupLostAttempts bool

	// Internal specifies a set of options for internal use.
	// Internal: This should never be used and is not supported.
	Internal struct {
		Hooks TransactionHooks
	}
}

//PerTransactionConfig specifies options which can be overriden on a per transaction basis.
type PerTransactionConfig struct {
	// ExpirationTime sets the maximum time that this transaction will
	// run for, before expiring.
	ExpirationTime time.Duration

	// DurabilityLevel specifies the durability level that should be used
	// for all write operations performed by this transaction.
	DurabilityLevel DurabilityLevel

	// KeyValueTimeout specifies the default timeout used for all KV writes.
	KeyValueTimeout time.Duration
}
