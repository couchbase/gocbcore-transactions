package transactions

import (
	"errors"
	"log"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v9"
	"github.com/google/uuid"
)

func TestParseCas(t *testing.T) {
	// assertEquals(1539336197457L, ActiveTransactionRecord.parseMutationCAS("0x000058a71dd25c15"));
	cas, err := parseCASToMilliseconds("0x000058a71dd25c15")
	if err != nil {
		t.Fatalf("Failed to parse cas: %v", err)
	}

	if cas != 1539336197457 {
		t.Fatalf("Parsed cas was incorrect, expected %d but was %d", 1539336197457, cas)
	}
}

func TestLostCleanupProcessATR(t *testing.T) {
	cluster, err := gocb.Connect("couchbase://10.112.210.101", gocb.ClusterOptions{
		Username: "Administrator",
		Password: "password",
	})
	if err != nil {
		log.Printf("Connect Error: %+v", err)
		panic(err)
	}

	testScopeName := ""
	testCollectionName := ""

	bucket := cluster.Bucket("default")

	agent, err := bucket.Internal().IORouter()
	if err != nil {
		log.Printf("Agent Fetch Error: %+v", err)
		panic(err)
	}

	transactions, err := Init(&Config{
		DurabilityLevel: DurabilityLevelNone,
		BucketAgentProvider: func(bucketName string) (*gocbcore.Agent, error) {
			// We can always return just this one agent as we only actually
			// use a single bucket for this entire test.
			return agent, nil
		},
		ExpirationTime:  500 * time.Millisecond,
		KeyValueTimeout: 2500 * time.Millisecond,
	})
	if err != nil {
		log.Printf("Init failed: %+v", err)
		panic(err)
	}

	txn, err := transactions.BeginTransaction(nil)
	if err != nil {
		log.Printf("BeginTransaction failed: %+v", err)
		panic(err)
	}

	// Start the attempt
	err = txn.NewAttempt()
	if err != nil {
		log.Printf("NewAttempt failed: %+v", err)
		panic(err)
	}

	testKey := []byte(uuid.New().String())
	testData := []byte(`{"name":"mike"}`)

	log.Printf("TXNATMPT: %+v", txn.attempt)

	// Try a random insert+get operation.
	insRes, err := testBlkInsert(txn, InsertOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            testKey,
		Value:          testData,
	})
	if err != nil {
		log.Printf("Insert failed: %+v", err)
		panic(err)
	}
	log.Printf("Insert result: %+v", insRes)

	log.Printf("TXNATMPT: %+v", txn.attempt)

	err = testBlkCommit(txn)
	if err != nil {
		log.Printf("Commit failed: %+v", err)
		panic(err)
	}

	a := txn.Attempt()

	config := &Config{}
	config.DurabilityLevel = DurabilityLevelNone
	config.BucketAgentProvider = func(bucketName string) (*gocbcore.Agent, error) {
		// We can always return just this one agent as we only actually
		// use a single bucket for this entire test.
		return agent, nil
	}
	config.ExpirationTime = 500 * time.Millisecond
	config.KeyValueTimeout = 2500 * time.Millisecond
	config.Internal.Hooks = nil
	config.Internal.CleanUpHooks = &DefaultCleanupHooks{}
	config.Internal.ClientRecordHooks = &DefaultClientRecordHooks{}
	cleaner := NewLostTransactionCleaner(config)

	time.Sleep(2 * time.Second)

	wait := make(chan []CleanupAttempt)
	cleaner.ProcessATR(agent, "", "", string(a.AtrID), func(attempts []CleanupAttempt, stats ProcessATRStats) {
		wait <- attempts
	})

	attempts := <-wait
	if len(attempts) != 1 {
		t.Fatalf("Attempts should have been 1 was %d", len(attempts))
	}

	c := bucket.DefaultCollection()
	res, err := c.LookupIn(string(a.AtrID), []gocb.LookupInSpec{
		gocb.GetSpec("attempts."+attempts[0].AttemptID, &gocb.GetSpecOptions{
			IsXattr: true,
		}),
	}, nil)
	if err != nil {
		t.Fatalf("LookupIn failed")
	}

	var shouldntexist interface{}
	err = res.ContentAt(0, &shouldntexist)
	if !errors.Is(err, gocb.ErrPathNotFound) {
		t.Fatalf("Error should have been path not found but was %v", err)
	}

	transactions.Close()
}

func TestLostCleanupProcessClient(t *testing.T) {
	cluster, err := gocb.Connect("couchbase://10.112.210.101", gocb.ClusterOptions{
		Username: "Administrator",
		Password: "password",
	})
	if err != nil {
		log.Printf("Connect Error: %+v", err)
		panic(err)
	}

	testScopeName := ""
	testCollectionName := ""

	bucket := cluster.Bucket("default")
	c := bucket.DefaultCollection()

	_, err = c.Remove(string(clientRecordKey), nil)
	if err != nil && !errors.Is(err, gocb.ErrDocumentNotFound) {
		log.Printf("Remove Error: %+v", err)
		panic(err)
	}

	agent, err := bucket.Internal().IORouter()
	if err != nil {
		log.Printf("Agent Fetch Error: %+v", err)
		panic(err)
	}

	transactions, err := Init(&Config{
		DurabilityLevel: DurabilityLevelNone,
		BucketAgentProvider: func(bucketName string) (*gocbcore.Agent, error) {
			// We can always return just this one agent as we only actually
			// use a single bucket for this entire test.
			return agent, nil
		},
		ExpirationTime:  500 * time.Millisecond,
		KeyValueTimeout: 2500 * time.Millisecond,
	})
	if err != nil {
		log.Printf("Init failed: %+v", err)
		panic(err)
	}

	txn, err := transactions.BeginTransaction(nil)
	if err != nil {
		log.Printf("BeginTransaction failed: %+v", err)
		panic(err)
	}

	// Start the attempt
	err = txn.NewAttempt()
	if err != nil {
		log.Printf("NewAttempt failed: %+v", err)
		panic(err)
	}

	testKey := []byte(uuid.New().String())
	testData := []byte(`{"name":"mike"}`)

	log.Printf("TXNATMPT: %+v", txn.attempt)

	// Try a random insert+get operation.
	insRes, err := testBlkInsert(txn, InsertOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            testKey,
		Value:          testData,
	})
	if err != nil {
		log.Printf("Insert failed: %+v", err)
		panic(err)
	}
	log.Printf("Insert result: %+v", insRes)

	log.Printf("TXNATMPT: %+v", txn.attempt)

	err = testBlkCommit(txn)
	if err != nil {
		log.Printf("Commit failed: %+v", err)
		panic(err)
	}

	a := txn.Attempt()

	clientUUID := uuid.New().String()
	config := &Config{}
	config.DurabilityLevel = DurabilityLevelNone
	config.BucketAgentProvider = func(bucketName string) (*gocbcore.Agent, error) {
		// We can always return just this one agent as we only actually
		// use a single bucket for this entire test.
		return agent, nil
	}
	config.CleanupWindow = 1 * time.Second
	config.ExpirationTime = 500 * time.Millisecond
	config.KeyValueTimeout = 2500 * time.Millisecond
	config.Internal.Hooks = nil
	config.Internal.CleanUpHooks = &DefaultCleanupHooks{}
	config.Internal.ClientRecordHooks = &DefaultClientRecordHooks{}
	config.Internal.NumATRs = 1024
	cleaner := newStdLostTransactionCleaner(config)
	cleaner.locations = map[LostATRLocation]chan struct{}{
		LostATRLocation{
			BucketName: "default",
		}: make(chan struct{}),
	}
	time.Sleep(2 * time.Second)

	wait := make(chan error, 1)
	cleaner.process(agent, "", "", func(err error) {
		wait <- err
	})
	err = <-wait
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}

	res, err := c.LookupIn(string(a.AtrID), []gocb.LookupInSpec{
		gocb.GetSpec("attempts", &gocb.GetSpecOptions{
			IsXattr: true,
		}),
	}, nil)
	if err != nil {
		t.Fatalf("LookupIn failed: %v", err)
	}

	var attempts map[string]interface{}
	if err := res.ContentAt(0, &attempts); err != nil {
		t.Fatalf("ContentAt failed: %v", err)
	}

	if len(attempts) != 0 {
		t.Fatalf("Attempts should have been empty: %v", attempts)
	}

	clientRes, err := c.LookupIn(string(clientRecordKey), []gocb.LookupInSpec{
		gocb.GetSpec("records.clients."+clientUUID, &gocb.GetSpecOptions{
			IsXattr: true,
		}),
	}, nil)
	if err != nil {
		t.Fatalf("LookupIn failed: %v", err)
	}

	var records map[string]interface{}
	if err := clientRes.ContentAt(0, &records); !errors.Is(err, gocb.ErrPathNotFound) {
		t.Fatalf("ContentAt should have failed with path not found: %v", err)
	}

	transactions.Close()
	cleaner.Close()

	clientRes2, err := c.LookupIn(string(clientRecordKey), []gocb.LookupInSpec{
		gocb.GetSpec("records", &gocb.GetSpecOptions{
			IsXattr: true,
		}),
	}, nil)
	if err != nil {
		t.Fatalf("LookupIn failed: %v", err)
	}

	var resultingClients jsonClientRecords
	err = clientRes2.ContentAt(0, &resultingClients)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if len(resultingClients.Clients) != 0 {
		t.Fatalf("Expected client records to have 0 clients after close: %v", resultingClients)
	}
}

func TestLostCleanupCleansUpExpiredClients(t *testing.T) {
	cluster, err := gocb.Connect("couchbase://10.112.210.101", gocb.ClusterOptions{
		Username: "Administrator",
		Password: "password",
	})
	if err != nil {
		log.Printf("Connect Error: %+v", err)
		panic(err)
	}

	testScopeName := ""
	testCollectionName := ""

	bucket := cluster.Bucket("default")
	c := bucket.DefaultCollection()

	_, err = c.Remove(string(clientRecordKey), nil)
	if err != nil && !errors.Is(err, gocb.ErrDocumentNotFound) {
		log.Printf("Remove Error: %+v", err)
		panic(err)
	}

	type clientRecord struct {
		Records jsonClientRecords `json:"records"`
	}

	// Create an expired client record that will be cleaned up
	expiredClientID := uuid.New().String()
	newClient := jsonClientRecords{
		Clients: map[string]jsonClientRecord{
			expiredClientID: {
				HeartbeatMS: "0x000056a9039a4416",
				ExpiresMS:   1,
			},
		},
	}

	_, err = c.MutateIn(string(clientRecordKey), []gocb.MutateInSpec{
		gocb.UpsertSpec("records", newClient, &gocb.UpsertSpecOptions{
			IsXattr:    true,
			CreatePath: true,
		}),
	}, &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsUpsert,
	})
	if err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	agent, err := bucket.Internal().IORouter()
	if err != nil {
		log.Printf("Agent Fetch Error: %+v", err)
		panic(err)
	}

	transactions, err := Init(&Config{
		DurabilityLevel: DurabilityLevelNone,
		BucketAgentProvider: func(bucketName string) (*gocbcore.Agent, error) {
			// We can always return just this one agent as we only actually
			// use a single bucket for this entire test.
			return agent, nil
		},
		ExpirationTime:  500 * time.Millisecond,
		KeyValueTimeout: 2500 * time.Millisecond,
	})
	if err != nil {
		log.Printf("Init failed: %+v", err)
		panic(err)
	}

	txn, err := transactions.BeginTransaction(nil)
	if err != nil {
		log.Printf("BeginTransaction failed: %+v", err)
		panic(err)
	}

	// Start the attempt
	err = txn.NewAttempt()
	if err != nil {
		log.Printf("NewAttempt failed: %+v", err)
		panic(err)
	}

	testKey := []byte(uuid.New().String())
	testData := []byte(`{"name":"mike"}`)

	log.Printf("TXNATMPT: %+v", txn.attempt)

	// Try a random insert+get operation.
	insRes, err := testBlkInsert(txn, InsertOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            testKey,
		Value:          testData,
	})
	if err != nil {
		log.Printf("Insert failed: %+v", err)
		panic(err)
	}
	log.Printf("Insert result: %+v", insRes)

	log.Printf("TXNATMPT: %+v", txn.attempt)

	err = testBlkCommit(txn)
	if err != nil {
		log.Printf("Commit failed: %+v", err)
		panic(err)
	}

	clientUUID := uuid.New().String()
	config := &Config{}
	config.DurabilityLevel = DurabilityLevelNone
	config.BucketAgentProvider = func(bucketName string) (*gocbcore.Agent, error) {
		// We can always return just this one agent as we only actually
		// use a single bucket for this entire test.
		return agent, nil
	}
	config.CleanupWindow = 1 * time.Second
	config.ExpirationTime = 500 * time.Millisecond
	config.KeyValueTimeout = 2500 * time.Millisecond
	config.Internal.Hooks = nil
	config.Internal.CleanUpHooks = &DefaultCleanupHooks{}
	config.Internal.ClientRecordHooks = &DefaultClientRecordHooks{}
	config.Internal.NumATRs = 1024
	cleaner := newStdLostTransactionCleaner(config)
	time.Sleep(1 * time.Second)

	wait := make(chan error, 1)
	cleaner.ProcessClient(agent, "", "", clientUUID, func(details *ClientRecordDetails, err error) {
		wait <- err
	})
	err = <-wait
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}

	clientRes, err := c.LookupIn(string(clientRecordKey), []gocb.LookupInSpec{
		gocb.GetSpec("records", &gocb.GetSpecOptions{
			IsXattr: true,
		}),
	}, nil)
	if err != nil {
		t.Fatalf("LookupIn failed: %v", err)
	}

	var resultingClients jsonClientRecords
	err = clientRes.ContentAt(0, &resultingClients)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if len(resultingClients.Clients) != 1 {
		t.Fatalf("Expected client records to have 1 client: %v", resultingClients)
	}

	if _, ok := resultingClients.Clients[expiredClientID]; ok {
		t.Fatalf("Expected client records not contain old client id %s: %v", expiredClientID, resultingClients)
	}

	transactions.Close()
}
