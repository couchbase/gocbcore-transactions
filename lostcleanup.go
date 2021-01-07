package transactions

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/google/uuid"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
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

// lostTransactionCleaner is responsible for cleaning up lost transactions.
// Internal: This should never be used and is not supported.
type LostTransactionCleaner interface {
	ProcessClient(agent *gocbcore.Agent, uuid string, cb func(*ClientRecordDetails, error))
	ProcessATR(agent *gocbcore.Agent, atrID string, cb func([]CleanupAttempt, ProcessATRStats))
	RemoveClientFromAllBuckets(uuid string) error
	Close()
}

type lostTransactionCleaner interface {
	AddBucket(bucket string)
	Close()
}

type noopLostTransactionCleaner struct {
}

func (ltc *noopLostTransactionCleaner) AddBucket(bucket string) {
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
	buckets             map[string]struct{}
	bucketsLock         sync.Mutex
	newBucketCh         chan string
	stop                chan struct{}
	bucketListProvider  BucketListProviderFn
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
		buckets:             make(map[string]struct{}),
		newBucketCh:         make(chan string, 20), // Buffer of 20 should be plenty
		stop:                make(chan struct{}),
		bucketListProvider:  config.BucketListProvider,
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
		ltc.pollForBuckets()

		select {
		case <-ltc.stop:
			return
		case <-time.After(10 * time.Minute):
			ltc.pollForBuckets()
		}
	}()

	for {
		select {
		case <-ltc.stop:
			return
		case bucket := <-ltc.newBucketCh:
			agent, err := ltc.bucketAgentProvider(bucket)
			if err != nil {
				// We should probably do something here...
				return
			}
			go ltc.perBucket(agent)
		}
	}
}

func (ltc *stdLostTransactionCleaner) AddBucket(bucket string) {
	ltc.bucketsLock.Lock()
	if _, ok := ltc.buckets[bucket]; ok {
		ltc.bucketsLock.Unlock()
		return
	}
	ltc.buckets[bucket] = struct{}{}
	ltc.bucketsLock.Unlock()
	ltc.newBucketCh <- bucket
}

func (ltc *stdLostTransactionCleaner) Close() {
	close(ltc.stop)
	ltc.RemoveClientFromAllBuckets(ltc.uuid)
}

func (ltc *stdLostTransactionCleaner) RemoveClientFromAllBuckets(uuid string) error {
	ltc.bucketsLock.Lock()
	buckets := ltc.buckets
	ltc.bucketsLock.Unlock()
	if ltc.bucketListProvider != nil {
		bs, err := ltc.bucketListProvider()
		if err != nil {
			return err
		}

		for _, b := range bs {
			if _, ok := buckets[b]; !ok {
				buckets[b] = struct{}{}
			}
		}
	}

	return ltc.removeClient(uuid, buckets)
}

func (ltc *stdLostTransactionCleaner) removeClient(uuid string, buckets map[string]struct{}) error {
	var err error
	var wg sync.WaitGroup
	for bucket := range buckets {
		wg.Add(1)
		func(bucketName string) {
			// There's a possible race between here and the client record being updated/created.
			// If that happens then it'll be expired and removed by another client anyway
			deadline := time.Now().Add(500 * time.Millisecond)

			ltc.unregisterClientRecord(bucketName, uuid, deadline, func(unregErr error) {
				if unregErr != nil {
					err = unregErr
				}
				wg.Done()
			})
		}(bucket)
	}
	wg.Wait()

	return err
}

func (ltc *stdLostTransactionCleaner) unregisterClientRecord(bucket, uuid string, deadline time.Time, cb func(error)) {
	agent, err := ltc.bucketAgentProvider(bucket)
	if err != nil {
		select {
		case <-time.After(deadline.Sub(time.Now())):
			cb(gocbcore.ErrTimeout)
			return
		case <-time.After(10 * time.Millisecond):
		}
		ltc.unregisterClientRecord(bucket, uuid, deadline, cb)
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
			ltc.unregisterClientRecord(bucket, uuid, deadline, cb)
			return
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
			Deadline: time.Now().Add(ltc.operationTimeout),
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
				ltc.unregisterClientRecord(bucket, uuid, deadline, cb)
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
			ltc.unregisterClientRecord(bucket, uuid, deadline, cb)
			return
		}
	})
}

func (ltc *stdLostTransactionCleaner) perBucket(agent *gocbcore.Agent) {
	ltc.process(agent, func(err error) {
		if err != nil {
			select {
			case <-ltc.stop:
				return
			case <-time.After(1 * time.Second):
				ltc.perBucket(agent)
				return
			}
		}

		select {
		case <-ltc.stop:
			return
		default:
		}
		ltc.perBucket(agent)
	})
}

func (ltc *stdLostTransactionCleaner) process(agent *gocbcore.Agent, cb func(error)) {
	ltc.ProcessClient(agent, ltc.uuid, func(recordDetails *ClientRecordDetails, err error) {
		if err != nil {
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
				ltc.ProcessATR(agent, atr, func(attempts []CleanupAttempt, _ ProcessATRStats) {
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
func (ltc *stdLostTransactionCleaner) ProcessClient(agent *gocbcore.Agent, uuid string, cb func(*ClientRecordDetails, error)) {
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
			Deadline: time.Now().Add(ltc.operationTimeout),
		}, func(result *gocbcore.LookupInResult, err error) {
			if err != nil {
				ec := classifyError(err)

				switch ec.Class {
				case ErrorClassFailDocNotFound:
					ltc.createClientRecord(agent, func(err error) {
						if err != nil {
							cb(nil, err)
							return
						}

						ltc.ProcessClient(agent, uuid, cb)
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

			nowSecs, err := strconv.Atoi(hlc.NowSecs)
			if err != nil {
				cb(nil, err)
				return
			}
			nowMS := nowSecs * 1000 // we need it in millis

			recordDetails, err := ltc.parseClientRecords(records, uuid, int64(nowMS))
			if err != nil {
				cb(nil, err)
				return
			}

			if recordDetails.OverrideActive {
				cb(&recordDetails, nil)
				return
			}

			ltc.processClientRecord(agent, uuid, recordDetails, func(err error) {
				if err != nil {
					cb(nil, err)
					return
				}

				cb(&recordDetails, nil)
			})

		})
	})
}

func (ltc *stdLostTransactionCleaner) ProcessATR(agent *gocbcore.Agent, atrID string, cb func([]CleanupAttempt, ProcessATRStats)) {
	ltc.getATR(agent, atrID, func(attempts map[string]jsonAtrAttempt, hlc int, err error) {
		if err != nil {
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
				parsedCAS, err := parseMutationCAS(attempt.PendingCAS)
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
						AtrCollectionName: "_default",
						AtrScopeName:      "_default",
						AtrBucketName:     agent.BucketName(),
						Inserts:           inserts,
						Replaces:          replaces,
						Removes:           removes,
						State:             st,
						ForwardCompat:     jsonForwardCompatToForwardCompat(attempt.ForwardCompat),
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

func (ltc *stdLostTransactionCleaner) getATR(agent *gocbcore.Agent, atrID string,
	cb func(map[string]jsonAtrAttempt, int, error)) {
	ltc.cleanupHooks.BeforeATRGet([]byte(atrID), func(err error) {
		if err != nil {
			cb(nil, 0, err)
			return
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

			nowSecs, err := strconv.Atoi(hlc.NowSecs)
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

		heartbeatMS, err := parseMutationCAS(client.HeartbeatMS)
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

func (ltc *stdLostTransactionCleaner) processClientRecord(agent *gocbcore.Agent, uuid string,
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

		opts.Deadline = time.Now().Add(ltc.operationTimeout)
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

func (ltc *stdLostTransactionCleaner) createClientRecord(agent *gocbcore.Agent, cb func(error)) {
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
			Flags:    memd.SubdocDocFlagAddDoc,
			Deadline: time.Now().Add(ltc.operationTimeout),
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

func (ltc *stdLostTransactionCleaner) pollForBuckets() error {
	if ltc.bucketListProvider != nil {
		buckets, err := ltc.bucketListProvider()
		if err != nil {
			return err
		}

		for _, bucket := range buckets {
			ltc.AddBucket(bucket)
		}
	}

	return nil
}

func atrsToHandle(index int, numActive int, numAtrs int) []string {
	allAtrs := atrIDList[:numAtrs]
	var selectedAtrs []string
	for i := index; i < len(allAtrs); i += numActive {
		selectedAtrs = append(selectedAtrs, allAtrs[i])
	}

	return selectedAtrs
}

// From Java impl:
// ${Mutation.CAS} is written by kvengine with 'macroToString(htonll(info.cas))'.  Discussed this with KV team and,
// though there is consensus that this is off (htonll is definitely wrong, and a string is an odd choice), there are
// clients (SyncGateway) that consume the current string, so it can't be changed.  Note that only little-endian
// servers are supported for Couchbase, so the 8 byte long inside the string will always be little-endian ordered.
//
// Looks like: "0x000058a71dd25c15"
// Want:        0x155CD21DA7580000   (1539336197457313792 in base10, an epoch time in millionths of a second)
func parseMutationCAS(in string) (int64, error) {
	offsetIndex := 2 // for the initial "0x"
	result := int64(0)

	for octetIndex := 7; octetIndex >= 0; octetIndex -= 1 {
		char1 := in[offsetIndex+(octetIndex*2)]
		char2 := in[offsetIndex+(octetIndex*2)+1]

		octet1 := int64(0)
		octet2 := int64(0)

		if char1 >= 'a' && char1 <= 'f' {
			octet1 = int64(char1 - 'a' + 10)
		} else if char1 >= 'A' && char1 <= 'F' {
			octet1 = int64(char1 - 'A' + 10)
		} else if char1 >= '0' && char1 <= '9' {
			octet1 = int64(char1 - '0')
		} else {
			return 0, fmt.Errorf("could not parse CAS: %s", in)
		}

		if char2 >= 'a' && char2 <= 'f' {
			octet2 = int64(char2 - 'a' + 10)
		} else if char2 >= 'A' && char2 <= 'F' {
			octet2 = int64(char2 - 'A' + 10)
		} else if char2 >= '0' && char2 <= '9' {
			octet2 = int64(char2 - '0')
		} else {
			return 0, fmt.Errorf("could not parse CAS: %s", in)
		}

		result |= octet1 << ((octetIndex * 8) + 4)
		result |= octet2 << (octetIndex * 8)
	}

	// It's in millionths of a second
	return result / 1000000, nil
}
