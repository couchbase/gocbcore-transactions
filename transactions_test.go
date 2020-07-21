package transactions

import (
	"log"
	"testing"

	gocb "github.com/couchbase/gocb/v2"
)

func testSetup(t *testing.T) {

}

func TestSomething(t *testing.T) {
	cluster, err := gocb.Connect("couchbase://172.23.111.129", gocb.ClusterOptions{
		Username: "Administrator",
		Password: "password",
	})
	if err != nil {
		log.Printf("Connect Error: %+v", err)
		panic(err)
	}

	bucket := cluster.Bucket("default")
	collection := bucket.DefaultCollection()

	// Do the setup stuff

	collection.Remove("_txn:atr-296-#f79", nil)
	collection.Remove("test-id", nil)
	collection.Remove("anotherDoc", nil)
	collection.Remove("yetAnotherDoc", nil)

	testDummy := map[string]string{"name": "joel"}
	_, err = collection.Upsert("anotherDoc", testDummy, nil)
	if err == nil {
		_, err = collection.Upsert("yetAnotherDoc", testDummy, nil)
	}
	if err != nil {
		log.Printf("Insert Test Error: %+v", err)
		panic(err)
	}

	// Setup Complete, start doing some work

	agent, err := bucket.Internal().IORouter()
	if err != nil {
		log.Printf("Agent Fetch Error: %+v", err)
		panic(err)
	}

	testScopeName := ""
	testCollectionName := ""

	transactions, err := Init(&Config{
		DurabilityLevel: DurabilityLevelNone,
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

	testKey := []byte(`test-id`)
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

	getRes, err := testBlkGet(txn, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            testKey,
	})
	if err != nil {
		log.Printf("Get failed: %+v", err)
		panic(err)
	}
	log.Printf("Get result: %+v", getRes)

	// Lets check a get+replace operation.
	testKeyRep := []byte("anotherDoc")
	testDataRep := []byte(`{"name":"frank"}`)
	repGetRes, err := testBlkGet(txn, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            testKeyRep,
	})
	if err != nil {
		log.Printf("Replace-Get failed: %+v", err)
		panic(err)
	}
	log.Printf("Replace-Get result: %+v", repGetRes)

	repRes, err := testBlkReplace(txn, ReplaceOptions{
		Document: repGetRes,
		Value:    testDataRep,
	})
	if err != nil {
		log.Printf("Replace failed: %+v", err)
		panic(err)
	}
	log.Printf("Replace result: %+v", repRes)

	// Lets check a get+remove operation.
	testKeyRem := []byte("yetAnotherDoc")
	remGetRes, err := testBlkGet(txn, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            testKeyRem,
	})
	if err != nil {
		log.Printf("Remove-Get failed: %+v", err)
		panic(err)
	}
	log.Printf("Remove-Get result: %+v", remGetRes)

	remRes, err := testBlkRemove(txn, RemoveOptions{
		Document: remGetRes,
	})
	if err != nil {
		log.Printf("Remove failed: %+v", err)
		panic(err)
	}
	log.Printf("Remove result: %+v", remRes)

	mutations := txn.GetMutations()
	log.Printf("Mutations List: %+v", mutations)

	err = testBlkCommit(txn)
	if err != nil {
		log.Printf("Commit failed: %+v", err)
		panic(err)
	}
}
