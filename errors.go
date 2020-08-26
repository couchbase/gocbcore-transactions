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

	// ErrTransactionOperationFailed is used for when a transaction operation fails.
	ErrTransactionOperationFailed = errors.New("transaction operation failed")

	// ErrDocAlreadyInTransaction indicates that a document is already in a transaction.
	ErrDocAlreadyInTransaction = errors.New("doc already in transaction")

	// ErrTransactionOperationFailed is used for when a transaction enters an illegal state.
	ErrIllegalState = errors.New("illegal state")
)
