package transactions

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/couchbase/gocbcore/v9"
	"github.com/pkg/errors"
)

var (
	// ErrNoAttempt indicates no attempt was started before an operation was performed.
	ErrNoAttempt = errors.New("attempt was not started")

	// ErrOther indicates an non-specific error has occured.
	ErrOther = errors.New("other error")

	// ErrTransient indicates a transient error occured which may succeed at a later point in time.
	ErrTransient = errors.New("transient error")

	// ErrWriteWriteConflict indicates that another transaction conflicted with this one.
	ErrWriteWriteConflict = errors.New("write write conflict")

	// ErrHard indicates that an unrecoverable error occured.
	ErrHard = errors.New("hard")

	// ErrAmbiguous indicates that a failure occured but the outcome was not known.
	ErrAmbiguous = errors.New("ambiguous error")

	// ErrAtrFull indicates that the ATR record was too full to accept a new mutation.
	ErrAtrFull = errors.New("atr full")

	// ErrAttemptExpired indicates an attempt expired.
	ErrAttemptExpired = errors.New("attempt expired")

	// ErrAtrNotFound indicates that an expected ATR document was missing.
	ErrAtrNotFound = errors.New("atr not found")

	// ErrAtrEntryNotFound indicates that an expected ATR entry was missing.
	ErrAtrEntryNotFound = errors.New("atr entry not found")

	// ErrDocAlreadyInTransaction indicates that a document is already in a transaction.
	ErrDocAlreadyInTransaction = errors.New("doc already in transaction")

	// ErrIllegalState is used for when a transaction enters an illegal State.
	ErrIllegalState = errors.New("illegal State")

	// ErrTransactionAbortedExternally indicates the transaction was aborted externally.
	ErrTransactionAbortedExternally = errors.New("transaction aborted externally")

	// ErrPreviousOperationFailed indicates a previous operation in the transaction failed.
	ErrPreviousOperationFailed = errors.New("previous operation failed")

	// ErrForwardCompatibilityFailure indicates an operation failed due to involving a document in another transaction
	// which contains features this transaction does not support.
	ErrForwardCompatibilityFailure = errors.New("forward compatibility error")

	// ErrDocumentNotFound indicates that a document was not found.
	ErrDocumentNotFound = errors.New("document not found")

	// ErrDocumentAlreadyExists indicates that a document already existed.
	ErrDocumentAlreadyExists = errors.New("document already exists")

	// ErrCasMismatch indicates a document's cas did not match during a mutation.
	ErrCasMismatch = errors.New("cas mismatch")
)

type classifiedError struct {
	Source error
	Class  ErrorClass
}

func (ce classifiedError) Wrap(errType error) *classifiedError {
	return &classifiedError{
		Source: &basicRetypedError{
			ErrType: errType,
			Source:  ce.Source,
		},
		Class: ce.Class,
	}
}

// TransactionOperationFailedError is used when a transaction operation fails.
// Internal: This should never be used and is not supported.
type TransactionOperationFailedError struct {
	shouldNotRetry    bool
	shouldNotRollback bool
	errorCause        error
	shouldRaise       ErrorReason
	errorClass        ErrorClass
}

// MarshalJSON will marshal this error for the wire.
func (tfe TransactionOperationFailedError) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Retry    bool            `json:"retry"`
		Rollback bool            `json:"rollback"`
		Raise    string          `json:"raise"`
		Cause    json.RawMessage `json:"cause"`
	}{
		Retry:    !tfe.shouldNotRetry,
		Rollback: !tfe.shouldNotRollback,
		Raise:    errorReasonToString(tfe.shouldRaise),
		Cause:    marshalErrorToJSON(tfe.errorCause),
	})
}

func (tfe TransactionOperationFailedError) Error() string {
	errStr := "transaction operation failed"
	errStr += " | " + fmt.Sprintf(
		"shouldRetry:%v, shouldRollback:%v, shouldRaise:%d, class:%d",
		!tfe.shouldNotRetry,
		!tfe.shouldNotRollback,
		tfe.shouldRaise,
		tfe.errorClass)
	if tfe.errorCause != nil {
		errStr += " | " + tfe.errorCause.Error()
	}
	return errStr
}

func (tfe TransactionOperationFailedError) Unwrap() error {
	return tfe.errorCause
}

// Retry signals whether a new attempt should be made at rollback.
func (tfe TransactionOperationFailedError) Retry() bool {
	return !tfe.shouldNotRetry
}

// Rollback signals whether the attempt should be auto-rolled back.
func (tfe TransactionOperationFailedError) Rollback() bool {
	return !tfe.shouldNotRollback
}

// ToRaise signals which error type should be raised to the application.
func (tfe TransactionOperationFailedError) ToRaise() ErrorReason {
	return tfe.shouldRaise
}

// ErrorClass is the class of error which caused this error.
func (tfe TransactionOperationFailedError) ErrorClass() ErrorClass {
	return tfe.errorClass
}

type aggregateError []error

func (agge aggregateError) MarshalJSON() ([]byte, error) {
	suberrs := make([]json.RawMessage, len(agge))
	for i, err := range agge {
		suberrs[i] = marshalErrorToJSON(err)
	}
	return json.Marshal(suberrs)
}

func (agge aggregateError) Error() string {
	errStrs := []string{}
	for _, err := range agge {
		errStrs = append(errStrs, err.Error())
	}
	return "[" + strings.Join(errStrs, ", ") + "]"
}

func (agge aggregateError) Is(err error) bool {
	for _, aerr := range agge {
		if errors.Is(aerr, err) {
			return true
		}
	}
	return false
}

type writeWriteConflictError struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	DocumentKey    []byte
	Source         error
}

func (wwce writeWriteConflictError) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Msg            string          `json:"msg"`
		Cause          json.RawMessage `json:"cause"`
		BucketName     string          `json:"bucket"`
		ScopeName      string          `json:"scope"`
		CollectionName string          `json:"collection"`
		DocumentKey    string          `json:"document_key"`
	}{
		Msg:            "write write conflict",
		Cause:          marshalErrorToJSON(wwce.Source),
		BucketName:     wwce.BucketName,
		ScopeName:      wwce.ScopeName,
		CollectionName: wwce.CollectionName,
		DocumentKey:    string(wwce.DocumentKey),
	})
}

func (wwce writeWriteConflictError) Error() string {
	errStr := "write write conflict"
	errStr += " | " + fmt.Sprintf(
		"bucket:%s, scope:%s, collection:%s, key:%s",
		wwce.BucketName,
		wwce.ScopeName,
		wwce.CollectionName,
		wwce.DocumentKey)
	if wwce.Source != nil {
		errStr += " | " + wwce.Source.Error()
	}
	return errStr
}

func (wwce writeWriteConflictError) Is(err error) bool {
	if err == ErrWriteWriteConflict {
		return true
	}
	return errors.Is(wwce.Source, err)
}

func (wwce writeWriteConflictError) Unwrap() error {
	return wwce.Source
}

type basicRetypedError struct {
	ErrType error
	Source  error
}

func (bre basicRetypedError) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Msg   string          `json:"msg"`
		Cause json.RawMessage `json:"cause"`
	}{
		Msg:   bre.ErrType.Error(),
		Cause: marshalErrorToJSON(bre.Source),
	})
}

func (bre basicRetypedError) Error() string {
	errStr := bre.ErrType.Error()
	if bre.Source != nil {
		errStr += " | " + bre.Source.Error()
	}
	return errStr
}

func (bre basicRetypedError) Is(err error) bool {
	if errors.Is(bre.ErrType, err) {
		return true
	}
	return errors.Is(bre.Source, err)
}

func (bre basicRetypedError) Unwrap() error {
	return bre.Source
}

func marshalErrorToJSON(err error) json.RawMessage {
	// BUG(TXNG-78): Remove gocbcore error serialization workaround.
	// We currently need to manually serialize these because gocbcore has a bug that
	// causes JSON serialization of its errors to loose information.
	if coreErr, ok := err.(*gocbcore.KeyValueError); ok {
		var coreErrFields map[string]interface{}
		if coreErrData, err := json.Marshal(coreErr); err == nil {
			if err := json.Unmarshal(coreErrData, &coreErrFields); err == nil {
				coreErrFields["msg"] = coreErr.InnerError.Error()
				if errData, err := json.Marshal(coreErr); err == nil {
					return errData
				}
			}
		}
	}

	if marshaler, ok := err.(json.Marshaler); ok {
		if data, err := marshaler.MarshalJSON(); err == nil {
			return json.RawMessage(data)
		}
	}

	data, _ := json.Marshal(err.Error())
	return json.RawMessage(data)
}
