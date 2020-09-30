package transactions

// AttemptState represents the current state of a transaction
type AttemptState int

const (
	// AttemptStateNothingWritten indicates that nothing has been written yet.
	AttemptStateNothingWritten = AttemptState(1)

	// AttemptStatePending indicates that the transaction ATR has been written and
	// the transaction is currently pending.
	AttemptStatePending = AttemptState(2)

	// AttemptStateCommitted indicates that the transaction is now logically committed
	// but the unstaging of documents is still underway.
	AttemptStateCommitted = AttemptState(3)

	// AttemptStateCompleted indicates that the transaction has been fully completed
	// and no longer has work to perform.
	AttemptStateCompleted = AttemptState(4)

	// AttemptStateAborted indicates that the transaction was aborted.
	AttemptStateAborted = AttemptState(5)

	// AttemptStateRolledBack indicates that the transaction was not committed and instead
	// was rolled back in its entirety.
	AttemptStateRolledBack = AttemptState(6)
)

// ErrorReason is the reason why a transaction should be failed.
// Internal: This should never be used and is not supported.
type ErrorReason uint8

const (
	// ErrorReasonTransactionFailed indicates the transaction should be failed because it failed.
	ErrorReasonTransactionFailed ErrorReason = iota

	// ErrorReasonTransactionExpired indicates the transaction should be failed because it expired.
	ErrorReasonTransactionExpired

	// ErrorReasonTransactionCommitAmbiguous indicates the transaction should be failed and the commit was ambiguous.
	ErrorReasonTransactionCommitAmbiguous

	// ErrorReasonTransactionFailedPostCommit indicates the transaction should be failed because it failed post commit.
	ErrorReasonTransactionFailedPostCommit
)

// ErrorClass describes the reason that a transaction error occurred.
// Internal: This should never be used and is not supported.
type ErrorClass uint8

const (
	// ErrorClassFailOther indicates an error occurred because it did not fit into any other reason.
	ErrorClassFailOther ErrorClass = iota

	// ErrorClassFailTransient indicates an error occurred because of a transient reason.
	ErrorClassFailTransient

	// ErrorClassFailDocNotFound indicates an error occurred because of a document not found.
	ErrorClassFailDocNotFound

	// ErrorClassFailDocAlreadyExists indicates an error occurred because a document already exists.
	ErrorClassFailDocAlreadyExists

	// ErrorClassFailPathNotFound indicates an error occurred because a path was not found.
	ErrorClassFailPathNotFound

	// ErrorClassFailPathAlreadyExists indicates an error occurred because a path already exists.
	ErrorClassFailPathAlreadyExists

	// ErrorClassFailWriteWriteConflict indicates an error occurred because of a write write conflict.
	ErrorClassFailWriteWriteConflict

	// ErrorClassFailCasMismatch indicates an error occurred because of a cas mismatch.
	ErrorClassFailCasMismatch

	// ErrorClassFailHard indicates an error occurred because of a hard error.
	ErrorClassFailHard

	// ErrorClassFailAmbiguous indicates an error occurred leaving the transaction in an ambiguous way.
	ErrorClassFailAmbiguous

	// ErrorClassFailExpiry indicates an error occurred because the transaction expired.
	ErrorClassFailExpiry

	// ErrorClassFailATRFull indicates an error occurred because the ATR is full.
	ErrorClassFailATRFull
)
