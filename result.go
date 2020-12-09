package transactions

import (
	gocbcore "github.com/couchbase/gocbcore/v9"
)

// MutationToken holds the mutation State information from an operation.
type MutationToken struct {
	BucketName string
	gocbcore.MutationToken
}

// Attempt represents a singular attempt at executing a transaction.  A
// transaction may require multiple attempts before being successful.
type Attempt struct {
	State             AttemptState
	StateIsAmbiguous  bool
	ID                string
	MutationState     []MutationToken
	AtrID             []byte
	AtrBucketName     string
	AtrScopeName      string
	AtrCollectionName string

	// UnstagingComplete indicates whether the transaction was succesfully
	// unstaged, or if a later cleanup job will be responsible.
	UnstagingComplete bool

	// Expired indicates whether this attempt expired during execution.
	Expired bool

	// PreExpiryAutoRollback indicates whether an auto-rollback occured
	// before the transaction was expired.
	PreExpiryAutoRollback bool
}

// Result represents the result of a transaction which was executed.
type Result struct {
	// TransactionID represents the UUID assigned to this transaction
	TransactionID string

	// Attempts records all attempts that were performed when executing
	// this transaction.
	Attempts []Attempt

	// MutationState represents the State associated with this transaction
	// and can be used to perform RYOW queries at a later point.
	MutationState []gocbcore.MutationToken

	// UnstagingComplete indicates whether the transaction was succesfully
	// unstaged, or if a later cleanup job will be responsible.
	UnstagingComplete bool
}
