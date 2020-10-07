package transactions

import "errors"

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

	// ErrUhOh is used for now to describe errors I yet know how to categorize.
	ErrUhOh = errors.New("uh oh")

	// ErrDocAlreadyInTransaction indicates that a document is already in a transaction.
	ErrDocAlreadyInTransaction = errors.New("doc already in transaction")

	// ErrIllegalState is used for when a transaction enters an illegal state.
	ErrIllegalState = errors.New("illegal state")

	// ErrTransactionAbortedExternally indicates the transaction was aborted externally.
	ErrTransactionAbortedExternally = errors.New("transaction aborted externally")

	// ErrPreviousOperationFailed indicates a previous operation in the transaction failed.
	ErrPreviousOperationFailed = errors.New("previous operation failed")
)

// ErrTransactionOperationFailed is used when a transaction operation fails.
// Internal: This should never be used and is not supported.
type TransactionOperationFailedError struct {
	shouldRetry       bool
	shouldNotRollback bool
	errorCause        error
	shouldRaise       ErrorReason
	errorClass        ErrorClass
}

func (tfe TransactionOperationFailedError) Error() string {
	if tfe.errorCause == nil {
		return "transaction operation failed"
	}
	return "transaction operation failed | " + tfe.errorCause.Error()
}

func (tfe TransactionOperationFailedError) Unwrap() error {
	return tfe.errorCause
}

// Retry signals whether a new attempt should be made at rollback.
func (tfe TransactionOperationFailedError) Retry() bool {
	return tfe.shouldRetry
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
