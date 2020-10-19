package transactions

import "github.com/couchbase/gocbcore/v9/memd"

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
