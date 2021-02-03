package transactions

import (
	"encoding/json"
	"errors"
	"testing"

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
