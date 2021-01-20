package transactions

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"time"
)

func durabilityLevelToMemd(durabilityLevel DurabilityLevel) memd.DurabilityLevel {
	switch durabilityLevel {
	case DurabilityLevelNone:
		return memd.DurabilityLevel(0)
	case DurabilityLevelMajority:
		return memd.DurabilityLevelMajority
	case DurabilityLevelMajorityAndPersistToActive:
		return memd.DurabilityLevelMajorityAndPersistOnMaster
	case DurabilityLevelPersistToMajority:
		return memd.DurabilityLevelPersistToMajority
	case DurabilityLevelUnknown:
		panic("unexpected unset durability level")
	default:
		panic("unexpected durability level")
	}
}

func durabilityLevelToShorthand(durabilityLevel DurabilityLevel) string {
	switch durabilityLevel {
	case DurabilityLevelNone:
		return "n"
	case DurabilityLevelMajority:
		return "m"
	case DurabilityLevelMajorityAndPersistToActive:
		return "pa"
	case DurabilityLevelPersistToMajority:
		return "pm"
	default:
		// If it's an unknown durability level, default to majority.
		return "m"
	}
}

func durabilityLevelFromShorthand(durabilityLevel string) DurabilityLevel {
	switch durabilityLevel {
	case "m":
		return DurabilityLevelMajority
	case "pa":
		return DurabilityLevelMajorityAndPersistToActive
	case "pm":
		return DurabilityLevelPersistToMajority
	default:
		// If there is no durability level present or it's set to none then we'll set to majority.
		return DurabilityLevelMajority
	}
}

func mutationTimeouts(opTimeout time.Duration, durability DurabilityLevel) (time.Time, time.Duration) {
	var deadline time.Time
	var duraTimeout time.Duration
	if opTimeout > 0 {
		deadline = time.Now().Add(opTimeout)
		if durability > DurabilityLevelNone {
			duraTimeout = opTimeout
		}
	}

	return deadline, duraTimeout
}
