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

import (
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
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
