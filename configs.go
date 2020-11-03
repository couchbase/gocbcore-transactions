package transactions

import (
	"errors"
	"time"

	"github.com/couchbase/gocbcore/v9"
)

// DurabilityLevel specifies the durability level to use for a mutation.
type DurabilityLevel int

const (
	// DurabilityLevelUnknown indicates to use the default level.
	DurabilityLevelUnknown = DurabilityLevel(0)

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

func durabilityLevelToString(level DurabilityLevel) string {
	switch level {
	case DurabilityLevelUnknown:
		return "UNSET"
	case DurabilityLevelNone:
		return "NONE"
	case DurabilityLevelMajority:
		return "MAJORITY"
	case DurabilityLevelMajorityAndPersistToActive:
		return "MAJORITY_AND_PERSIST_TO_ACTIVE"
	case DurabilityLevelPersistToMajority:
		return "PERSIST_TO_MAJORITY"
	}
	return ""
}

func durabilityLevelFromString(level string) (DurabilityLevel, error) {
	switch level {
	case "UNSET":
		return DurabilityLevelUnknown, nil
	case "NONE":
		return DurabilityLevelNone, nil
	case "MAJORITY":
		return DurabilityLevelMajority, nil
	case "MAJORITY_AND_PERSIST_TO_ACTIVE":
		return DurabilityLevelMajorityAndPersistToActive, nil
	case "PERSIST_TO_MAJORITY":
		return DurabilityLevelPersistToMajority, nil
	}
	return DurabilityLevelUnknown, errors.New("invalid durability level string")
}

// ATRLocation specifies a specific location where ATR entries should be
// placed when performing transactions.
type ATRLocation struct {
	Agent          *gocbcore.Agent
	ScopeName      string
	CollectionName string
}

// BucketAgentProviderFn is a function used to provide an agent for
// a particular bucket by name.
type BucketAgentProviderFn func(bucketName string) (*gocbcore.Agent, error)

// BUG(TXNG-28): KeyValueTimeout, KvDurableTimeout are deprecated in Config.

// Config specifies various tunable options related to transactions.
type Config struct {
	// CustomATRLocation specifies a specific location to place meta-data.
	CustomATRLocation ATRLocation

	// ExpirationTime sets the maximum time that transactions created
	// by this Transactions object can run for, before expiring.
	ExpirationTime time.Duration

	// DurabilityLevel specifies the durability level that should be used
	// for all write operations performed by this Transactions object.
	DurabilityLevel DurabilityLevel

	// KeyValueTimeout specifies the default timeout used for all KV writes.
	KeyValueTimeout time.Duration

	// KeyValueTimeout specifies the default timeout used for durable KV writes.
	KvDurableTimeout time.Duration

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

	// CleanupQueueSize controls the maximum queue size for the cleanup thread.
	CleanupQueueSize uint32

	// BucketAgentProvider provides a function which returns an agent for
	// a particular bucket by name.
	BucketAgentProvider BucketAgentProviderFn

	// Internal specifies a set of options for internal use.
	// Internal: This should never be used and is not supported.
	Internal struct {
		Hooks           TransactionHooks
		CleanUpHooks    CleanUpHooks
		SerialUnstaging bool
		ExplicitATRs    bool
	}
}

// BUG(TXNG-28): KeyValueTimeout, KvDurableTimeout are deprecated in PerTransactionConfig.

// PerTransactionConfig specifies options which can be overriden on a per transaction basis.
type PerTransactionConfig struct {
	// CustomATRLocation specifies a specific location to place meta-data.
	CustomATRLocation ATRLocation

	// ExpirationTime sets the maximum time that this transaction will
	// run for, before expiring.
	ExpirationTime time.Duration

	// DurabilityLevel specifies the durability level that should be used
	// for all write operations performed by this transaction.
	DurabilityLevel DurabilityLevel

	// KeyValueTimeout specifies the timeout used for all KV writes.
	KeyValueTimeout time.Duration

	// KeyValueTimeout specifies the timeout used for durable KV writes.
	KvDurableTimeout time.Duration
}
