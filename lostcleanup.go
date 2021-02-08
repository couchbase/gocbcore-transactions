package transactions

import (
	"encoding/json"
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/google/uuid"
)

var clientRecordKey = []byte("_txn:client-record")

type jsonClientRecord struct {
	HeartbeatMS string `json:"heartbeat_ms,omitempty"`
	ExpiresMS   int    `json:"expires_ms,omitempty"`
	NumATRs     int    `json:"num_atrs,omitempty"`
}

type jsonClientOverride struct {
	Enabled      bool  `json:"enabled,omitempty"`
	ExpiresNanos int64 `json:"expires,omitempty"`
}

type jsonClientRecords struct {
	Clients  map[string]jsonClientRecord `json:"clients"`
	Override *jsonClientOverride         `json:"override,omitempty"`
}

type jsonHLC struct {
	NowSecs string `json:"now"`
}

// ClientRecordDetails is the result of processing a client record.
// Internal: This should never be used and is not supported.
type ClientRecordDetails struct {
	NumActiveClients     int
	IndexOfThisClient    int
	ClientIsNew          bool
	ExpiredClientIDs     []string
	NumExistingClients   int
	NumExpiredClients    int
	OverrideEnabled      bool
	OverrideActive       bool
	OverrideExpiresCas   int64
	CasNowNanos          int64
	AtrsHandledByClient  []string
	CheckAtrEveryNMillis int
	ClientUUID           string
}

// ProcessATRStats is the stats recorded when running a ProcessATR request.
// Internal: This should never be used and is not supported.
type ProcessATRStats struct {
	NumEntries        int
	NumEntriesExpired int
}

// LostTransactionCleaner is responsible for cleaning up lost transactions.
// Internal: This should never be used and is not supported.
type LostTransactionCleaner interface {
	ProcessClient(agent *gocbcore.Agent, collection, scope, uuid string, cb func(*ClientRecordDetails, error))
	ProcessATR(agent *gocbcore.Agent, collection, scope, atrID string, cb func([]CleanupAttempt, ProcessATRStats))
	RemoveClientFromAllBuckets(uuid string) error
	Close()
}

type lostTransactionCleaner interface {
	AddATRLocation(location LostATRLocation)
	Close()
}

type noopLostTransactionCleaner struct {
}

func (ltc *noopLostTransactionCleaner) AddATRLocation(location LostATRLocation) {
}

func (ltc *noopLostTransactionCleaner) Close() {
}

type stdLostTransactionCleaner struct {
	uuid                string
	cleanupHooks        CleanUpHooks
	clientRecordHooks   ClientRecordHooks
	numAtrs             int
	cleanupWindow       time.Duration
	cleaner             Cleaner
	operationTimeout    time.Duration
	bucketAgentProvider BucketAgentProviderFn
	locations           map[LostATRLocation]chan struct{}
	locationsLock       sync.Mutex
	newLocationCh       chan lostATRLocationWithShutdown
	stop                chan struct{}
	atrLocationFinder   LostCleanupATRLocationProviderFn
}

type lostATRLocationWithShutdown struct {
	location LostATRLocation
	shutdown chan struct{}
}

// NewLostTransactionCleaner returns new lost transaction cleaner.
// Internal: This should never be used and is not supported.
func NewLostTransactionCleaner(config *Config) LostTransactionCleaner {
	return newStdLostTransactionCleaner(config)
}

func newStdLostTransactionCleaner(config *Config) *stdLostTransactionCleaner {
	return &stdLostTransactionCleaner{
		uuid:                uuid.New().String(),
		numAtrs:             config.Internal.NumATRs,
		cleanupWindow:       config.CleanupWindow,
		cleanupHooks:        config.Internal.CleanUpHooks,
		clientRecordHooks:   config.Internal.ClientRecordHooks,
		cleaner:             NewCleaner(config),
		operationTimeout:    config.KeyValueTimeout,
		bucketAgentProvider: config.BucketAgentProvider,
		locations:           make(map[LostATRLocation]chan struct{}),
		newLocationCh:       make(chan lostATRLocationWithShutdown, 20), // Buffer of 20 should be plenty
		stop:                make(chan struct{}),
		atrLocationFinder:   config.LostCleanupATRLocationProvider,
	}
}

func startLostTransactionCleaner(config *Config) *stdLostTransactionCleaner {
	t := newStdLostTransactionCleaner(config)

	if config.BucketAgentProvider != nil {
		go t.start()
	}

	return t
}

func (ltc *stdLostTransactionCleaner) start() {
	go func() {
		for {
			ltc.pollForLocations()

			select {
			case <-ltc.stop:
				return
			case <-time.After(10 * time.Minute):
			}
		}
	}()

	for {
		select {
		case <-ltc.stop:
			return
		case location := <-ltc.newLocationCh:
			agent, err := ltc.bucketAgentProvider(location.location.BucketName)
			if err != nil {
				logDebugf("Failed to fetch agent for %v:, err: %v",
					location, err)
				// We should probably do something here...
				return
			}
			go ltc.perLocation(agent, location.location.CollectionName, location.location.ScopeName, location.shutdown)
		}
	}
}

func (ltc *stdLostTransactionCleaner) AddATRLocation(location LostATRLocation) {
	ltc.locationsLock.Lock()
	if _, ok := ltc.locations[location]; ok {
		ltc.locationsLock.Unlock()
		return
	}
	ch := make(chan struct{})
	ltc.locations[location] = ch
	ltc.locationsLock.Unlock()
	logDebugf("Adding location %v to lost cleanup", location)
	ltc.newLocationCh <- lostATRLocationWithShutdown{
		location: location,
		shutdown: ch,
	}
}

func (ltc *stdLostTransactionCleaner) Close() {
	close(ltc.stop)
	ltc.RemoveClientFromAllBuckets(ltc.uuid)
}

func (ltc *stdLostTransactionCleaner) RemoveClientFromAllBuckets(uuid string) error {
	ltc.locationsLock.Lock()
	locations := ltc.locations
	ltc.locationsLock.Unlock()
	if ltc.atrLocationFinder != nil {
		bs, err := ltc.atrLocationFinder()
		if err != nil {
			logDebugf("Failed to get atr locations: %v", err)
			return err
		}

		for _, b := range bs {
			if _, ok := locations[b]; !ok {
				locations[b] = make(chan struct{})
			}
		}
	}

	return ltc.removeClient(uuid, locations)
}

func (ltc *stdLostTransactionCleaner) removeClient(uuid string, locations map[LostATRLocation]chan struct{}) error {
	var err error
	var wg sync.WaitGroup
	for l := range locations {
		wg.Add(1)
		func(location LostATRLocation) {
			// There's a possible race between here and the client record being updated/created.
			// If that happens then it'll be expired and removed by another client anyway
			deadline := time.Now().Add(500 * time.Millisecond)

			ltc.unregisterClientRecord(location, uuid, deadline, func(unregErr error) {
				if unregErr != nil {
					logDebugf("Failed to unregister %s from cleanup record on from location %v", uuid, location)
					err = unregErr
				}
				wg.Done()
			})
		}(l)
	}
	wg.Wait()

	return err
}

func (ltc *stdLostTransactionCleaner) unregisterClientRecord(location LostATRLocation, uuid string, deadline time.Time, cb func(error)) {
	agent, err := ltc.bucketAgentProvider(location.BucketName)
	if err != nil {
		select {
		case <-time.After(deadline.Sub(time.Now())):
			cb(gocbcore.ErrTimeout)
			return
		case <-time.After(10 * time.Millisecond):
		}
		ltc.unregisterClientRecord(location, uuid, deadline, cb)
		return
	}

	ltc.clientRecordHooks.BeforeRemoveClient(func(err error) {
		if err != nil {
			if errors.Is(err, gocbcore.ErrDocumentNotFound) || errors.Is(err, gocbcore.ErrPathNotFound) {
				cb(nil)
				return
			}

			select {
			case <-time.After(deadline.Sub(time.Now())):
				cb(gocbcore.ErrTimeout)
				return
			case <-time.After(10 * time.Millisecond):
			}
			ltc.unregisterClientRecord(location, uuid, deadline, cb)
			return
		}

		var opDeadline time.Time
		if ltc.operationTimeout > 0 {
			opDeadline = time.Now().Add(ltc.operationTimeout)
		}

		_, err = agent.MutateIn(gocbcore.MutateInOptions{
			Key: clientRecordKey,
			Ops: []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpDelete,
					Flags: memd.SubdocFlagXattrPath,
					Path:  "records.clients." + uuid,
				},
			},
			Deadline:       opDeadline,
			CollectionName: location.CollectionName,
			ScopeName:      location.ScopeName,
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				if errors.Is(err, gocbcore.ErrDocumentNotFound) || errors.Is(err, gocbcore.ErrPathNotFound) {
					cb(nil)
					return
				}

				select {
				case <-time.After(deadline.Sub(time.Now())):
					cb(gocbcore.ErrTimeout)
					return
				case <-time.After(10 * time.Millisecond):
				}
				ltc.unregisterClientRecord(location, uuid, deadline, cb)
				return
			}

			cb(nil)
		})
		if err != nil {
			select {
			case <-time.After(deadline.Sub(time.Now())):
				cb(gocbcore.ErrTimeout)
				return
			case <-time.After(10 * time.Millisecond):
			}
			ltc.unregisterClientRecord(location, uuid, deadline, cb)
			return
		}
	})
}

func (ltc *stdLostTransactionCleaner) perLocation(agent *gocbcore.Agent, collection, scope string, shutdownCh chan struct{}) {
	ltc.process(agent, collection, scope, func(err error) {
		if err != nil {
			select {
			case <-ltc.stop:
				return
			case <-shutdownCh:
				return
			case <-time.After(1 * time.Second):
				ltc.perLocation(agent, collection, scope, shutdownCh)
				return
			}
		}

		select {
		case <-ltc.stop:
			return
		case <-shutdownCh:
			return
		default:
		}
		ltc.perLocation(agent, collection, scope, shutdownCh)
	})
}

func (ltc *stdLostTransactionCleaner) process(agent *gocbcore.Agent, collection, scope string, cb func(error)) {
	ltc.ProcessClient(agent, collection, scope, ltc.uuid, func(recordDetails *ClientRecordDetails, err error) {
		if err != nil {
			logDebugf("Failed to process client %s on bucket %s", ltc.uuid, agent.BucketName())
			cb(err)
			return
		}

		// We need this goroutine so we can release the scope of the callback. We're still in the callback from the
		// LookupIn here so we're blocking the gocbcore read loop for the node, any further requests against that node
		// will never complete and timeout.
		go func() {
			d := time.Duration(recordDetails.CheckAtrEveryNMillis) * time.Millisecond
			for _, atr := range recordDetails.AtrsHandledByClient {
				select {
				case <-ltc.stop:
					return
				case <-time.After(d):
				}

				waitCh := make(chan struct{}, 1)
				ltc.ProcessATR(agent, collection, scope, atr, func(attempts []CleanupAttempt, _ ProcessATRStats) {
					// We don't actually care what happened
					waitCh <- struct{}{}
				})
				<-waitCh
			}

			cb(nil)
		}()
	})
}

// We pass uuid to this so that it's testable externally.
func (ltc *stdLostTransactionCleaner) ProcessClient(agent *gocbcore.Agent, collection, scope, uuid string, cb func(*ClientRecordDetails, error)) {
	ltc.clientRecordHooks.BeforeGetRecord(func(err error) {
		if err != nil {
			ec := classifyHookError(err)
			switch ec.Class {
			default:
				cb(nil, err)
				return
			case ErrorClassFailDocAlreadyExists:
			case ErrorClassFailCasMismatch:
			}
		}

		var deadline time.Time
		if ltc.operationTimeout > 0 {
			deadline = time.Now().Add(ltc.operationTimeout)
		}

		_, err = agent.LookupIn(gocbcore.LookupInOptions{
			Key: clientRecordKey,
			Ops: []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpGet,
					Path:  "records",
					Flags: memd.SubdocFlagXattrPath,
				},
				{
					Op:    memd.SubDocOpGet,
					Path:  hlcMacro,
					Flags: memd.SubdocFlagXattrPath,
				},
			},
			Deadline:       deadline,
			CollectionName: collection,
			ScopeName:      scope,
		}, func(result *gocbcore.LookupInResult, err error) {
			if err != nil {
				ec := classifyError(err)

				switch ec.Class {
				case ErrorClassFailDocNotFound:
					ltc.createClientRecord(agent, collection, scope, func(err error) {
						if err != nil {
							cb(nil, err)
							return
						}

						ltc.ProcessClient(agent, collection, scope, uuid, cb)
					})
				default:
					cb(nil, err)
				}
				return
			}

			recordOp := result.Ops[0]
			hlcOp := result.Ops[1]
			if recordOp.Err != nil {
				cb(nil, recordOp.Err)
				return
			}

			if hlcOp.Err != nil {
				cb(nil, hlcOp.Err)
				return
			}

			var records jsonClientRecords
			err = json.Unmarshal(recordOp.Value, &records)
			if err != nil {
				cb(nil, err)
				return
			}

			var hlc jsonHLC
			err = json.Unmarshal(hlcOp.Value, &hlc)
			if err != nil {
				cb(nil, err)
				return
			}

			nowSecs, err := parseHLCToSeconds(hlc)
			if err != nil {
				cb(nil, err)
				return
			}
			nowMS := nowSecs * 1000 // we need it in millis

			recordDetails, err := ltc.parseClientRecords(records, uuid, nowMS)
			if err != nil {
				cb(nil, err)
				return
			}

			if recordDetails.OverrideActive {
				cb(&recordDetails, nil)
				return
			}

			ltc.processClientRecord(agent, collection, scope, uuid, recordDetails, func(err error) {
				if err != nil {
					cb(nil, err)
					return
				}

				cb(&recordDetails, nil)
			})

		})
	})
}

func (ltc *stdLostTransactionCleaner) ProcessATR(agent *gocbcore.Agent, collection, scope, atrID string, cb func([]CleanupAttempt, ProcessATRStats)) {
	ltc.getATR(agent, collection, scope, atrID, func(attempts map[string]jsonAtrAttempt, hlc int64, err error) {
		if err != nil {
			logDebugf("Failed to get atr %s on bucket %s", atrID, agent.BucketName())
			cb(nil, ProcessATRStats{})
			return
		}

		if len(attempts) == 0 {
			cb([]CleanupAttempt{}, ProcessATRStats{})
			return
		}

		stats := ProcessATRStats{
			NumEntries: len(attempts),
		}

		// See the explanation in process, same idea.
		go func() {
			var results []CleanupAttempt
			for key, attempt := range attempts {
				select {
				case <-ltc.stop:
					return
				default:
				}
				parsedCAS, err := parseCASToMilliseconds(attempt.PendingCAS)
				if err != nil {
					cb(nil, ProcessATRStats{})
					return
				}
				var inserts []DocRecord
				var replaces []DocRecord
				var removes []DocRecord
				for _, staged := range attempt.Inserts {
					inserts = append(inserts, DocRecord{
						CollectionName: staged.CollectionName,
						ScopeName:      staged.ScopeName,
						BucketName:     staged.BucketName,
						ID:             []byte(staged.DocID),
					})
				}
				for _, staged := range attempt.Replaces {
					replaces = append(replaces, DocRecord{
						CollectionName: staged.CollectionName,
						ScopeName:      staged.ScopeName,
						BucketName:     staged.BucketName,
						ID:             []byte(staged.DocID),
					})
				}
				for _, staged := range attempt.Removes {
					removes = append(removes, DocRecord{
						CollectionName: staged.CollectionName,
						ScopeName:      staged.ScopeName,
						BucketName:     staged.BucketName,
						ID:             []byte(staged.DocID),
					})
				}

				var st AttemptState
				switch jsonAtrState(attempt.State) {
				case jsonAtrStateCommitted:
					st = AttemptStateCommitted
				case jsonAtrStateCompleted:
					st = AttemptStateCompleted
				case jsonAtrStatePending:
					st = AttemptStatePending
				case jsonAtrStateAborted:
					st = AttemptStateAborted
				case jsonAtrStateRolledBack:
					st = AttemptStateRolledBack
				default:
					continue
				}

				if int64(attempt.ExpiryTime)+parsedCAS < int64(hlc) {
					req := &CleanupRequest{
						AttemptID:         key,
						AtrID:             []byte(atrID),
						AtrCollectionName: collection,
						AtrScopeName:      scope,
						AtrBucketName:     agent.BucketName(),
						Inserts:           inserts,
						Replaces:          replaces,
						Removes:           removes,
						State:             st,
						ForwardCompat:     jsonForwardCompatToForwardCompat(attempt.ForwardCompat),
						DurabilityLevel:   durabilityLevelFromShorthand(attempt.DurabilityLevel),
					}

					waitCh := make(chan CleanupAttempt, 1)
					ltc.cleaner.CleanupAttempt(agent, req, false, func(attempt CleanupAttempt) {
						waitCh <- attempt
					})
					attempt := <-waitCh
					results = append(results, attempt)
					stats.NumEntriesExpired++
				}
			}
			cb(results, stats)
		}()
	})
}

func (ltc *stdLostTransactionCleaner) getATR(agent *gocbcore.Agent, collection, scope, atrID string,
	cb func(map[string]jsonAtrAttempt, int64, error)) {
	ltc.cleanupHooks.BeforeATRGet([]byte(atrID), func(err error) {
		if err != nil {
			cb(nil, 0, err)
			return
		}

		var deadline time.Time
		if ltc.operationTimeout > 0 {
			deadline = time.Now().Add(ltc.operationTimeout)
		}

		_, err = agent.LookupIn(gocbcore.LookupInOptions{
			Key: []byte(atrID),
			Ops: []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpGet,
					Path:  "attempts",
					Flags: memd.SubdocFlagXattrPath,
				},
				{
					Op:    memd.SubDocOpGet,
					Path:  hlcMacro,
					Flags: memd.SubdocFlagXattrPath,
				},
			},
			Deadline:       deadline,
			CollectionName: collection,
			ScopeName:      scope,
		}, func(result *gocbcore.LookupInResult, err error) {
			if err != nil {
				cb(nil, 0, err)
				return
			}

			if result.Ops[0].Err != nil {
				cb(nil, 0, result.Ops[0].Err)
				return
			}

			if result.Ops[1].Err != nil {
				cb(nil, 0, result.Ops[1].Err)
				return
			}

			var attempts map[string]jsonAtrAttempt
			err = json.Unmarshal(result.Ops[0].Value, &attempts)
			if err != nil {
				cb(nil, 0, err)
				return
			}

			var hlc jsonHLC
			err = json.Unmarshal(result.Ops[1].Value, &hlc)
			if err != nil {
				cb(nil, 0, err)
				return
			}

			nowSecs, err := parseHLCToSeconds(hlc)
			if err != nil {
				cb(nil, 0, err)
				return
			}
			nowMS := nowSecs * 1000 // we need it in millis

			cb(attempts, nowMS, err)
		})
		if err != nil {
			cb(nil, 0, err)
			return
		}
	})
}

func (ltc *stdLostTransactionCleaner) parseClientRecords(records jsonClientRecords, uuid string, hlc int64) (ClientRecordDetails, error) {
	var expiredIDs []string
	var activeIDs []string
	var clientAlreadyExists bool

	for u, client := range records.Clients {
		if u == uuid {
			activeIDs = append(activeIDs, u)
			clientAlreadyExists = true
			continue
		}

		heartbeatMS, err := parseCASToMilliseconds(client.HeartbeatMS)
		if err != nil {
			return ClientRecordDetails{}, err
		}
		expiredPeriod := hlc - heartbeatMS

		if expiredPeriod >= int64(client.ExpiresMS) {
			expiredIDs = append(expiredIDs, u)
		} else {
			activeIDs = append(activeIDs, u)
		}
	}

	if !clientAlreadyExists {
		activeIDs = append(activeIDs, uuid)
	}

	sort.Strings(activeIDs)

	clientIndex := 0
	for i, u := range activeIDs {
		if u == uuid {
			clientIndex = i
			break
		}
	}

	var overrideEnabled bool
	var overrideActive bool
	var overrideExpiresCas int64

	if records.Override != nil {
		overrideEnabled = records.Override.Enabled
		overrideExpiresCas = records.Override.ExpiresNanos
		hlcNanos := hlc * 1000000

		if overrideEnabled && overrideExpiresCas > hlcNanos {
			overrideActive = true
		}
	}

	numActive := len(activeIDs)
	numExpired := len(expiredIDs)

	atrsHandled := atrsToHandle(clientIndex, numActive, ltc.numAtrs)

	checkAtrEveryNS := ltc.cleanupWindow.Milliseconds() / int64(len(atrsHandled))
	checkAtrEveryNMS := int(math.Max(1, float64(checkAtrEveryNS)))

	return ClientRecordDetails{
		NumActiveClients:     numActive,
		IndexOfThisClient:    clientIndex,
		ClientIsNew:          clientAlreadyExists,
		ExpiredClientIDs:     expiredIDs,
		NumExistingClients:   numActive + numExpired,
		NumExpiredClients:    numExpired,
		OverrideEnabled:      overrideEnabled,
		OverrideActive:       overrideActive,
		OverrideExpiresCas:   overrideExpiresCas,
		CasNowNanos:          hlc,
		AtrsHandledByClient:  atrsHandled,
		CheckAtrEveryNMillis: checkAtrEveryNMS,
		ClientUUID:           uuid,
	}, nil
}

func (ltc *stdLostTransactionCleaner) processClientRecord(agent *gocbcore.Agent, collection, scope, uuid string,
	recordDetails ClientRecordDetails, cb func(error)) {
	ltc.clientRecordHooks.BeforeUpdateRecord(func(err error) {
		if err != nil {
			cb(err)
			return
		}

		prefix := "records.clients." + uuid + "."
		var marshalErr error
		fieldOp := func(fieldName string, data interface{}, op memd.SubDocOpType, flags memd.SubdocFlag) gocbcore.SubDocOp {
			b, err := json.Marshal(data)
			if err != nil {
				marshalErr = err
				return gocbcore.SubDocOp{}
			}

			return gocbcore.SubDocOp{
				Op:    op,
				Flags: flags,
				Path:  prefix + fieldName,
				Value: b,
			}
		}

		if marshalErr != nil {
			cb(err)
			return
		}

		opts := gocbcore.MutateInOptions{
			Key: clientRecordKey,
			Ops: []gocbcore.SubDocOp{
				fieldOp("heartbeat_ms", "${Mutation.CAS}", memd.SubDocOpDictSet,
					memd.SubdocFlagXattrPath|memd.SubdocFlagExpandMacros|memd.SubdocFlagMkDirP),
				fieldOp("expires_ms", (ltc.cleanupWindow + 20000*time.Millisecond).Milliseconds(),
					memd.SubDocOpDictSet, memd.SubdocFlagXattrPath),
				fieldOp("num_atrs", ltc.numAtrs, memd.SubDocOpDictSet, memd.SubdocFlagXattrPath),
				{
					Op:    memd.SubDocOpSetDoc,
					Flags: memd.SubdocFlagNone,
					Value: []byte{0},
				},
			},
			CollectionName: collection,
			ScopeName:      scope,
		}

		numOps := 12
		if len(recordDetails.ExpiredClientIDs) < 12 {
			numOps = len(recordDetails.ExpiredClientIDs)
		}

		for i := 0; i < numOps; i++ {
			opts.Ops = append(opts.Ops, gocbcore.SubDocOp{
				Op:    memd.SubDocOpDelete,
				Flags: memd.SubdocFlagXattrPath,
				Path:  "records.clients." + recordDetails.ExpiredClientIDs[i],
			})
		}

		if ltc.operationTimeout > 0 {
			opts.Deadline = time.Now().Add(ltc.operationTimeout)
		}
		_, err = agent.MutateIn(opts, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				cb(err)
				return
			}

			cb(nil)
		})
		if err != nil {
			cb(err)
			return
		}
	})
}

func (ltc *stdLostTransactionCleaner) createClientRecord(agent *gocbcore.Agent, collection, scope string, cb func(error)) {
	ltc.clientRecordHooks.BeforeCreateRecord(func(err error) {
		if err != nil {
			ec := classifyHookError(err)

			switch ec.Class {
			default:
				cb(err)
				return
			case ErrorClassFailDocNotFound:
			}
		}

		var deadline time.Time
		if ltc.operationTimeout > 0 {
			deadline = time.Now().Add(ltc.operationTimeout)
		}

		_, err = agent.MutateIn(gocbcore.MutateInOptions{
			Key: clientRecordKey,
			Ops: []gocbcore.SubDocOp{
				{
					Op:    memd.SubDocOpDictAdd,
					Flags: memd.SubdocFlagXattrPath,
					Path:  "records.clients",
					Value: []byte{123, 125}, // {}
				},
				{
					Op:    memd.SubDocOpSetDoc,
					Flags: memd.SubdocFlagNone,
					Path:  "",
					Value: []byte{0},
				},
			},
			Flags:          memd.SubdocDocFlagAddDoc,
			Deadline:       deadline,
			CollectionName: collection,
			ScopeName:      scope,
		}, func(result *gocbcore.MutateInResult, err error) {
			if err != nil {
				ec := classifyError(err)

				switch ec.Class {
				default:
					cb(err)
					return
				case ErrorClassFailDocAlreadyExists:
				case ErrorClassFailCasMismatch:
				}
			}
			cb(nil)
		})
		if err != nil {
			cb(err)
			return
		}
	})
}

func (ltc *stdLostTransactionCleaner) pollForLocations() {
	if ltc.atrLocationFinder != nil {
		locations, err := ltc.atrLocationFinder()
		if err != nil {
			logDebugf("Failed to poll for locations: %v", err)
			return
		}

		locationMap := make(map[LostATRLocation]struct{})
		for _, location := range locations {
			ltc.AddATRLocation(location)
			locationMap[location] = struct{}{}
		}

		ltc.locationsLock.Lock()
		for location, shutdown := range ltc.locations {
			if _, ok := locationMap[location]; ok {
				continue
			}

			close(shutdown)
		}
		ltc.locationsLock.Unlock()
	}

}

func atrsToHandle(index int, numActive int, numAtrs int) []string {
	allAtrs := atrIDList[:numAtrs]
	var selectedAtrs []string
	for i := index; i < len(allAtrs); i += numActive {
		selectedAtrs = append(selectedAtrs, allAtrs[i])
	}

	return selectedAtrs
}
