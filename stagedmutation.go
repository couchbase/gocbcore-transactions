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

	gocbcore "github.com/couchbase/gocbcore/v9"
)

// StagedMutationType represents the type of a mutation performed in a transaction.
type StagedMutationType int

const (
	// StagedMutationUnknown indicates an error has occured.
	StagedMutationUnknown = StagedMutationType(0)

	// StagedMutationInsert indicates the staged mutation was an insert operation.
	StagedMutationInsert = StagedMutationType(1)

	// StagedMutationReplace indicates the staged mutation was an replace operation.
	StagedMutationReplace = StagedMutationType(2)

	// StagedMutationRemove indicates the staged mutation was an remove operation.
	StagedMutationRemove = StagedMutationType(3)
)

func stagedMutationTypeToString(mtype StagedMutationType) string {
	switch mtype {
	case StagedMutationInsert:
		return "INSERT"
	case StagedMutationReplace:
		return "REPLACE"
	case StagedMutationRemove:
		return "REMOVE"
	}
	return ""
}

func stagedMutationTypeFromString(mtype string) (StagedMutationType, error) {
	switch mtype {
	case "INSERT":
		return StagedMutationInsert, nil
	case "REPLACE":
		return StagedMutationReplace, nil
	case "REMOVE":
		return StagedMutationRemove, nil
	}
	return StagedMutationUnknown, errors.New("invalid mutation type string")
}

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
