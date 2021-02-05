package transactions

import (
	"errors"
	"sync/atomic"

	"github.com/couchbase/gocbcore/v9"
)

func mergeOperationFailedErrors(errs []*TransactionOperationFailedError) *TransactionOperationFailedError {
	if len(errs) == 0 {
		return nil
	}

	if len(errs) == 1 {
		return errs[0]
	}

	shouldNotRetry := false
	shouldNotRollback := false
	aggCauses := aggregateError{}
	shouldRaise := ErrorReasonTransactionFailed

	for errIdx := 0; errIdx < len(errs); errIdx++ {
		tErr := errs[errIdx]

		aggCauses = append(aggCauses, tErr)

		if tErr.shouldNotRetry {
			shouldNotRetry = true
		}
		if tErr.shouldNotRollback {
			shouldNotRollback = true
		}
		if tErr.shouldRaise > shouldRaise {
			shouldRaise = tErr.shouldRaise
		}
	}

	return &TransactionOperationFailedError{
		shouldNotRetry:    shouldNotRetry,
		shouldNotRollback: shouldNotRollback,
		errorCause:        aggCauses,
		shouldRaise:       shouldRaise,
		errorClass:        ErrorClassFailOther,
	}
}

type operationFailedDef struct {
	Cerr              *classifiedError
	ShouldNotRetry    bool
	ShouldNotRollback bool
	CanStillCommit    bool
	Reason            ErrorReason
}

func (t *transactionAttempt) applyStateBits(stateBits uint32) {
	// This is a bit dirty, but its maximum going to do one retry per bit.
	for {
		oldStateBits := atomic.LoadUint32(&t.stateBits)
		newStateBits := oldStateBits | stateBits
		if atomic.CompareAndSwapUint32(&t.stateBits, oldStateBits, newStateBits) {
			break
		}
	}
}

func (t *transactionAttempt) operationFailed(def operationFailedDef) *TransactionOperationFailedError {
	err := &TransactionOperationFailedError{
		shouldNotRetry:    def.ShouldNotRetry,
		shouldNotRollback: def.ShouldNotRollback,
		errorCause:        def.Cerr.Source,
		errorClass:        def.Cerr.Class,
		shouldRaise:       def.Reason,
	}

	stateBits := uint32(0)
	if !def.CanStillCommit {
		stateBits |= transactionStateBitShouldNotCommit
	}
	if def.ShouldNotRollback {
		stateBits |= transactionStateBitShouldNotRollback
	}
	if def.ShouldNotRetry {
		stateBits |= transactionStateBitShouldNotRetry
	}
	if def.Reason == ErrorReasonTransactionExpired {
		stateBits |= transactionStateBitHasExpired
	}
	t.applyStateBits(stateBits)

	return err
}

func classifyHookError(err error) *classifiedError {
	// We currently have to classify the errors that are returned from the hooks, but
	// we should really just directly return the classifications and make the source
	// some special internal source showing it came from a hook...
	return classifyError(err)
}

func classifyError(err error) *classifiedError {
	ec := ErrorClassFailOther
	if errors.Is(err, ErrDocAlreadyInTransaction) || errors.Is(err, ErrWriteWriteConflict) {
		ec = ErrorClassFailWriteWriteConflict
	} else if errors.Is(err, ErrHard) {
		ec = ErrorClassFailHard
	} else if errors.Is(err, ErrAttemptExpired) {
		ec = ErrorClassFailExpiry
	} else if errors.Is(err, ErrTransient) {
		ec = ErrorClassFailTransient
	} else if errors.Is(err, ErrDocumentNotFound) {
		ec = ErrorClassFailDocNotFound
	} else if errors.Is(err, ErrDocumentAlreadyExists) {
		ec = ErrorClassFailDocAlreadyExists
	} else if errors.Is(err, ErrAmbiguous) {
		ec = ErrorClassFailAmbiguous
	} else if errors.Is(err, ErrCasMismatch) {
		ec = ErrorClassFailCasMismatch

	} else if errors.Is(err, gocbcore.ErrDocumentNotFound) {
		ec = ErrorClassFailDocNotFound
	} else if errors.Is(err, gocbcore.ErrDocumentExists) {
		ec = ErrorClassFailDocAlreadyExists
	} else if errors.Is(err, gocbcore.ErrPathExists) {
		ec = ErrorClassFailPathAlreadyExists
	} else if errors.Is(err, gocbcore.ErrPathNotFound) {
		ec = ErrorClassFailPathNotFound
	} else if errors.Is(err, gocbcore.ErrCasMismatch) {
		ec = ErrorClassFailCasMismatch
	} else if errors.Is(err, gocbcore.ErrUnambiguousTimeout) {
		ec = ErrorClassFailTransient
	} else if errors.Is(err, gocbcore.ErrDurabilityAmbiguous) ||
		errors.Is(err, gocbcore.ErrAmbiguousTimeout) ||
		errors.Is(err, gocbcore.ErrRequestCanceled) {
		ec = ErrorClassFailAmbiguous
	} else if errors.Is(err, gocbcore.ErrMemdTooBig) {
		ec = ErrorClassFailOutOfSpace
	}

	return &classifiedError{
		Source: err,
		Class:  ec,
	}
}
