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
	"testing"

	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/stretchr/testify/assert"
)

func TestAggregateErrorMarshals(t *testing.T) {
	terr := &aggregateError{
		errors.New("some-error"),
		&TransactionOperationFailedError{
			shouldNotRetry:    true,
			shouldNotRollback: true,
			errorCause:        errors.New("some-cause"),
			shouldRaise:       ErrorReasonTransactionExpired,
			errorClass:        ErrorClassFailCasMismatch,
		},
	}

	bytes, err := json.Marshal(terr)
	assert.NoErrorf(t, err, "marshal failed")

	assert.EqualValues(t, []byte(`["some-error",{"retry":false,"rollback":false,"raise":"expired","cause":"some-cause"}]`), bytes)
}

func TestGocbcoreErrorMarshals(t *testing.T) {
	terr := &TransactionOperationFailedError{
		shouldNotRetry:    true,
		shouldNotRollback: true,
		errorCause: gocbcore.KeyValueError{
			InnerError:         gocbcore.ErrCasMismatch,
			StatusCode:         memd.StatusAccessError,
			DocumentKey:        "key",
			BucketName:         "bucket",
			ScopeName:          "scope",
			CollectionName:     "collection",
			CollectionID:       19,
			ErrorName:          "",
			ErrorDescription:   "",
			Opaque:             4019,
			Context:            "",
			Ref:                "",
			RetryReasons:       nil,
			RetryAttempts:      1,
			LastDispatchedTo:   "127.0.0.1:11210",
			LastDispatchedFrom: "127.0.0.1:79654",
			LastConnectionID:   "",
		},
		shouldRaise: ErrorReasonTransactionExpired,
		errorClass:  ErrorClassFailCasMismatch,
	}

	bytes, err := json.Marshal(terr)
	assert.NoErrorf(t, err, "marshal failed")

	assert.EqualValues(t, []byte(`{"retry":false,"rollback":false,"raise":"expired","cause":{"msg":"cas mismatch","status_code":36,"document_key":"key","bucket":"bucket","scope":"scope","collection":"collection","collection_id":19,"opaque":4019,"retry_attempts":1,"last_dispatched_to":"127.0.0.1:11210","last_dispatched_from":"127.0.0.1:79654"}}`), bytes)
}
