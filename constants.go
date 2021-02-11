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

import "fmt"

var crc32cMacro = []byte("\"${Mutation.value_crc32c}\"")
var revidMacro = []byte("\"${$document.revid}\"")
var exptimeMacro = []byte("\"${$document.exptime}\"")
var casMacro = []byte("\"${$document.CAS}\"")
var hlcMacro = "$vbucket.HLC"

// AttemptState represents the current State of a transaction
type AttemptState int

const (
	// AttemptStateNothingWritten indicates that nothing has been written yet.
	AttemptStateNothingWritten = AttemptState(1)

	// AttemptStatePending indicates that the transaction ATR has been written and
	// the transaction is currently pending.
	AttemptStatePending = AttemptState(2)

	// AttemptStateCommitting indicates that the transaction is now trying to become
	// committed, if we stay in this state, it implies ambiguity.
	AttemptStateCommitting = AttemptState(3)

	// AttemptStateCommitted indicates that the transaction is now logically committed
	// but the unstaging of documents is still underway.
	AttemptStateCommitted = AttemptState(4)

	// AttemptStateCompleted indicates that the transaction has been fully completed
	// and no longer has work to perform.
	AttemptStateCompleted = AttemptState(5)

	// AttemptStateAborted indicates that the transaction was aborted.
	AttemptStateAborted = AttemptState(6)

	// AttemptStateRolledBack indicates that the transaction was not committed and instead
	// was rolled back in its entirety.
	AttemptStateRolledBack = AttemptState(7)
)

// ErrorReason is the reason why a transaction should be failed.
// Internal: This should never be used and is not supported.
type ErrorReason uint8

// NOTE: The errors within this section are critically ordered, as the order of
// precedence used when merging errors together is based on this.
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

func errorReasonToString(reason ErrorReason) string {
	switch reason {
	case ErrorReasonTransactionFailed:
		return "failed"
	case ErrorReasonTransactionExpired:
		return "expired"
	case ErrorReasonTransactionCommitAmbiguous:
		return "commit_ambiguous"
	case ErrorReasonTransactionFailedPostCommit:
		return "failed_post_commit"
	default:
		return fmt.Sprintf("unknown:%d", reason)
	}
}

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

	// ErrorClassFailOutOfSpace indicates an error occurred because the ATR is full.
	ErrorClassFailOutOfSpace
)

func errorClassToString(class ErrorClass) string {
	switch class {
	case ErrorClassFailOther:
		return "other"
	case ErrorClassFailTransient:
		return "transient"
	case ErrorClassFailDocNotFound:
		return "document_not_found"
	case ErrorClassFailDocAlreadyExists:
		return "document_already_exists"
	case ErrorClassFailPathNotFound:
		return "path_not_found"
	case ErrorClassFailPathAlreadyExists:
		return "path_already_exists"
	case ErrorClassFailWriteWriteConflict:
		return "write_write_conflict"
	case ErrorClassFailCasMismatch:
		return "cas_mismatch"
	case ErrorClassFailHard:
		return "hard"
	case ErrorClassFailAmbiguous:
		return "ambiguous"
	case ErrorClassFailExpiry:
		return "expiry"
	case ErrorClassFailOutOfSpace:
		return "out_of_space"
	default:
		return fmt.Sprintf("unknown:%d", class)
	}
}

const (
	transactionStateBitShouldNotCommit       = 1 << 0
	transactionStateBitShouldNotRollback     = 1 << 1
	transactionStateBitShouldNotRetry        = 1 << 2
	transactionStateBitHasExpired            = 1 << 3
	transactionStateBitPreExpiryAutoRollback = 1 << 4
)
