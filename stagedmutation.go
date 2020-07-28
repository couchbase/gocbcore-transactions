package transactions

import (
	"encoding/json"

	gocbcore "github.com/couchbase/gocbcore/v9"
)

// StagedMutationType represents the type of a mutation performed in a transaction.
type StagedMutationType int

const (
	// StagedMutationInsert indicates the staged mutation was an insert operation.
	StagedMutationInsert = StagedMutationType(1)

	// StagedMutationReplace indicates the staged mutation was an replace operation.
	StagedMutationReplace = StagedMutationType(2)

	// StagedMutationRemove indicates the staged mutation was an remove operation.
	StagedMutationRemove = StagedMutationType(3)
)

// StagedMutation wraps all of the information about a mutation which has been staged
// as part of the transaction and which should later be unstaged when the transaction
// has been committed.
type StagedMutation struct {
	OpType         StagedMutationType
	BucketName     string
	ScopeName      string
	CollectionName string
	Key            []byte
	Cas            gocbcore.Cas
	Staged         json.RawMessage
}

type stagedMutation struct {
	OpType         StagedMutationType
	Agent          *gocbcore.Agent
	ScopeName      string
	CollectionName string
	Key            []byte
	Cas            gocbcore.Cas
	Staged         json.RawMessage
}
