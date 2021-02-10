package transactions

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// ProtocolVersion returns the protocol version that this library supports.
func ProtocolVersion() string {
	return "2.0"
}

// ProtocolExtensions returns a list strings representing the various features
// that this specific version of the library supports within its protocol version.
func ProtocolExtensions() []string {
	return []string{
		"EXT_TRANSACTION_ID",
		"EXT_MEMORY_OPT_UNSTAGING",
		"EXT_BINARY_METADATA",
		"EXT_CUSTOM_METADATA_COLLECTION",
		"EXT_STORE_DURABILITY",
		"EXT_REMOVE_COMPLETED",
		"BF_CBD_3787",
		"BF_CBD_3705",
		"BF_CBD_3838",
	}
}

type forwardCompatBehaviour string

const (
	forwardCompatBehaviourRetry forwardCompatBehaviour = "r"
	forwardCompatBehaviourFail  forwardCompatBehaviour = "f"
)

type forwardCompatExtension string

const (
	forwardCompatExtensionTransactionID            forwardCompatExtension = "TI"
	forwardCompatExtensionDeferredCommit           forwardCompatExtension = "DC"
	forwardCompatExtensionTimeOptUnstaging         forwardCompatExtension = "TO"
	forwardCompatExtensionMemoryOptUnstaging       forwardCompatExtension = "MO"
	forwardCompatExtensionCustomMetadataCollection forwardCompatExtension = "CM"
	forwardCompatExtensionBinaryMetadata           forwardCompatExtension = "BM"
	forwardCompatExtensionQuery                    forwardCompatExtension = "QU"
	forwardCompatExtensionStoreDurability          forwardCompatExtension = "SD"
	forwardCompatExtensionRemoveCompleted          forwardCompatExtension = "RC"
	forwardCompatExtensionBFCBD3787                forwardCompatExtension = "BF3787"
	forwardCompatExtensionBFCBD3705                forwardCompatExtension = "BF3705"
	forwardCompatExtensionBFCBD3838                forwardCompatExtension = "BF3838"
)

type forwardCompatStage string

const (
	forwardCompatStageWWCReadingATR    forwardCompatStage = "WW_R"
	forwardCompatStageWWCReplacing     forwardCompatStage = "WW_RP"
	forwardCompatStageWWCRemoving      forwardCompatStage = "WW_RM"
	forwardCompatStageWWCInserting     forwardCompatStage = "WW_I"
	forwardCompatStageWWCInsertingGet  forwardCompatStage = "WW_IG"
	forwardCompatStageGets             forwardCompatStage = "G"
	forwardCompatStageGetsReadingATR   forwardCompatStage = "G_A"
	forwardCompatStageGetsCleanupEntry forwardCompatStage = "CL_E"
)

const (
	protocolMajor = 2
	protocolMinor = 0
)

// ForwardCompatibilityEntry represents a forward compatibility entry.
// Internal: This should never be used and is not supported.
type ForwardCompatibilityEntry struct {
	ProtocolVersion   string `json:"p,omitempty"`
	ProtocolExtension string `json:"e,omitempty"`
	Behaviour         string `json:"b,omitempty"`
	RetryInterval     int    `json:"ra,omitempty"`
}

var supportedforwardCompatExtensions = []forwardCompatExtension{
	forwardCompatExtensionTransactionID,
	forwardCompatExtensionMemoryOptUnstaging,
	forwardCompatExtensionCustomMetadataCollection,
	forwardCompatExtensionBinaryMetadata,
	forwardCompatExtensionQuery,
	forwardCompatExtensionStoreDurability,
	forwardCompatExtensionRemoveCompleted,
	forwardCompatExtensionBFCBD3787,
	forwardCompatExtensionBFCBD3705,
	forwardCompatExtensionBFCBD3838,
}

func jsonForwardCompatToForwardCompat(fc map[string][]jsonForwardCompatibilityEntry) map[string][]ForwardCompatibilityEntry {
	if fc == nil {
		return nil
	}
	forwardCompat := make(map[string][]ForwardCompatibilityEntry)

	for k, entries := range fc {
		if _, ok := forwardCompat[k]; !ok {
			forwardCompat[k] = make([]ForwardCompatibilityEntry, len(entries))
		}

		for i, entry := range entries {
			forwardCompat[k][i] = ForwardCompatibilityEntry(entry)
		}
	}

	return forwardCompat
}

func checkForwardCompatProtocol(protocolVersion string) (bool, error) {
	if protocolVersion == "" {
		return false, nil
	}

	protocol := strings.Split(protocolVersion, ".")
	if len(protocol) != 2 {
		return false, fmt.Errorf("invalid protocol string: %s", protocolVersion)
	}
	major, err := strconv.Atoi(protocol[0])
	if err != nil {
		return false, errors.Wrapf(err, "invalid protocol string: %s", protocolVersion)
	}
	if protocolMajor < major {
		return false, nil
	}
	if protocolMajor == major {
		minor, err := strconv.Atoi(protocol[1])
		if err != nil {
			return false, errors.Wrapf(err, "invalid protocol string: %s", protocolVersion)
		}
		if protocolMinor < minor {
			return false, nil
		}
	}

	return true, nil
}

func checkForwardCompatExtension(extension string) bool {
	if extension == "" {
		return false
	}

	for _, supported := range supportedforwardCompatExtensions {
		if string(supported) == extension {
			return true
		}
	}

	return false
}

func checkForwardCompatability(
	stage forwardCompatStage,
	fc map[string][]ForwardCompatibilityEntry,
) (isCompatOut bool, shouldRetryOut bool, retryWaitOut time.Duration, errOut error) {
	if fc == nil || len(fc) == 0 {
		return true, false, 0, nil
	}

	if checks, ok := fc[string(stage)]; ok {
		for _, c := range checks {
			protocolOk, err := checkForwardCompatProtocol(c.ProtocolVersion)
			if err != nil {
				return false, false, 0, err
			}

			if protocolOk {
				continue
			}

			if extensionOk := checkForwardCompatExtension(c.ProtocolExtension); extensionOk {
				continue
			}

			// If we get here then neither protocol or extension are ok.
			switch forwardCompatBehaviour(c.Behaviour) {
			case forwardCompatBehaviourRetry:
				retryWait := time.Duration(c.RetryInterval) * time.Millisecond
				return false, true, retryWait, nil
			default:
				return false, false, 0, nil
			}
		}
	}

	return true, false, 0, nil
}
