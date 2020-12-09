package transactions

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type forwardCompatExtension string

const (
	forwardCompatExtensionTransactionID  forwardCompatExtension = "TI"
	forwardCompatExtensionDeferredCommit forwardCompatExtension = "DC"
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

var supportedforwardCompatExtensions = []forwardCompatExtension{forwardCompatExtensionTransactionID, forwardCompatExtensionDeferredCommit}

func checkForwardCompatProtocol(protocolVersion string) (bool, error) {
	if protocolVersion == "" {
		return false, nil
	}

	protocol := strings.Split(protocolVersion, ".")
	if len(protocol) != 2 {
		return false, fmt.Errorf("invalid protocol: %s", protocolVersion)
	}
	major, err := strconv.Atoi(protocol[0])
	if err != nil {
		return false, err
	}
	if protocolMajor < major {
		return false, nil
	}
	if protocolMajor == major {
		minor, err := strconv.Atoi(protocol[1])
		if err != nil {
			return false, err
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

func checkForwardCompatability(stage forwardCompatStage, fc map[string][]ForwardCompatibilityEntry,
	cb func(shouldRetry bool, retryInterval time.Duration, err error)) {
	if fc == nil || len(fc) == 0 {
		cb(false, 0, nil)
		return
	}

	if checks, ok := fc[string(stage)]; ok {
		for _, c := range checks {
			protocolOk, err := checkForwardCompatProtocol(c.ProtocolVersion)
			if err != nil {
				cb(false, 0, err)
				return
			}

			if protocolOk {
				continue
			}

			if extensionOk := checkForwardCompatExtension(c.ProtocolExtension); extensionOk {
				continue
			}

			// If we get here then neither protocol or extension are ok.
			if c.Behaviour == "r" {
				if c.RetryInterval > 0 {
					time.AfterFunc(time.Duration(c.RetryInterval)*time.Millisecond, func() {
						cb(true, time.Duration(c.RetryInterval)*time.Millisecond, ErrForwardCompatibilityFailure)
					})
					return
				}

				cb(true, 0, ErrForwardCompatibilityFailure)
				return
			} else if c.Behaviour == "f" {
				cb(false, 0, ErrForwardCompatibilityFailure)
				return
			}
		}
	}

	cb(false, 0, nil)
}
