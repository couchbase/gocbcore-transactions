package transactions

import "errors"

var (
	// ErrNoAttempt indicates no attempt was started before an operation was performed.
	ErrNoAttempt = errors.New("attempt was not started")

	// ErrOther indicates an non-specific error has occured.
	ErrOther = errors.New("other error")

	// ErrTransient indicates a transient error occured which may succeed at a later point in time.
	ErrTransient = errors.New("transient error")

	// ErrDocNotFound indicates that a needed document was not found.
	ErrDocNotFound = errors.New("doc not found")

	// ErrDocAlreadyExists indicates that a document already existed unexpectedly.
	ErrDocAlreadyExists = errors.New("doc already exists")

	// ErrPathNotFound indicates that a needed path was not found.
	ErrPathNotFound = errors.New("path not found")

	// ErrPathAlreadyExists indicates that a path already existed unexpectedly.
	ErrPathAlreadyExists = errors.New("path already exists")

	// ErrWriteWriteConflict indicates that another transaction conflicted with this one.
	ErrWriteWriteConflict = errors.New("write write conflict")

	// ErrCasMismatch indicates that a cas mismatch occured during a store operation.
	ErrCasMismatch = errors.New("cas mismatch")

	// ErrHard indicates that an unrecoverable error occured.
	ErrHard = errors.New("hard")

	// ErrAmbiguous indicates that a failure occured but the outcome was not known.
	ErrAmbiguous = errors.New("ambiguous error")

	// ErrExpiry indicates that an operation expired before completion.
	ErrExpiry = errors.New("expiry")

	// ErrAtrFull indicates that the ATR record was too full to accept a new mutation.
	ErrAtrFull = errors.New("atr full")

	// ErrAttemptExpired indicates an attempt expired
	ErrAttemptExpired = errors.New("attempt expired")

	// ErrAtrNotFound indicates that an expected ATR document was missing
	ErrAtrNotFound = errors.New("atr not found")

	// ErrAtrEntryNotFound indicates that an expected ATR entry was missing
	ErrAtrEntryNotFound = errors.New("atr entry not found")

	// ErrUhOh is used for now to describe errors I yet know how to categorize
	ErrUhOh = errors.New("uh oh")
)
