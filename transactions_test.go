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
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	gocb "github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	"github.com/stretchr/testify/assert"
)

func TestSomethingElse(t *testing.T) {
	cluster, err := gocb.Connect("couchbase://10.112.210.101/", gocb.ClusterOptions{
		Username: "Administrator",
		Password: "password",
	})
	assert.NoError(t, err, "connect failed")

	bucket := cluster.Bucket("default")
	collection := bucket.DefaultCollection()

	// Do the setup stuff

	err = cluster.Buckets().FlushBucket("default", nil)
	assert.NoError(t, err, "delete-all failed")
	time.Sleep(1 * time.Second)

	testDummy1 := map[string]string{"name": "joel"}
	testDummy2 := []byte(`{"name":"mike"}`)
	// insertDoc
	_, err = collection.Upsert("replaceDoc", testDummy1, nil)
	assert.NoError(t, err, "replaceDoc upsert failed")

	// Setup Complete, start doing some work

	agent, err := bucket.Internal().IORouter()
	assert.NoError(t, err, "agent fetch failed")

	testScopeName := ""
	testCollectionName := ""

	transactions, err := Init(&Config{
		DurabilityLevel: DurabilityLevelNone,
		BucketAgentProvider: func(bucketName string) (*gocbcore.Agent, error) {
			// We can always return just this one agent as we only actually
			// use a single bucket for this entire test.
			return agent, nil
		},
		ExpirationTime:        30 * time.Second,
		CleanupClientAttempts: true,
	})
	assert.NoError(t, err, "txn init failed")

	txn, err := transactions.BeginTransaction(nil)
	assert.NoError(t, err, "txn begin failed")

	// Start the attempt
	err = txn.NewAttempt()
	assert.NoError(t, err, "txn attempt creation failed")
	log.Printf("initial attempt: %+v", txn.attempt)

	// INSERT - pre-serialization
	insertRes, err := testBlkInsert(txn, InsertOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`insertDoc2`),
		Value:          testDummy2,
	})
	assert.NoError(t, err, "insert of insertDoc failed")
	log.Printf("insert of insertDoc result: %+v", insertRes)

	err = testBlkCommit(txn)
	assert.NoError(t, err, "commit of insertDoc failed")

	txn, err = transactions.BeginTransaction(&PerTransactionConfig{
		ExpirationTime: 3 * time.Second,
	})
	assert.NoError(t, err, "txn begin failed")

	// Start the attempt
	err = txn.NewAttempt()
	assert.NoError(t, err, "txn attempt creation failed")
	log.Printf("initial attempt: %+v", txn.attempt)

	// INSERT - pre-serialization
	insertRes, err = testBlkInsert(txn, InsertOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`insertDoc2`),
		Value:          testDummy2,
	})
	assert.NoError(t, err, "insert of insertDoc failed")
	log.Printf("insert of insertDoc result: %+v", insertRes)

	err = testBlkCommit(txn)
	// assert.NoError(t, err, "commit of insertDoc failed")
	fmt.Println(transactions.cleaner.QueueLength())
	time.Sleep(4 * time.Second)
	fmt.Println(transactions.cleaner.QueueLength())

	time.Sleep(31 * time.Second)
	fmt.Println(transactions.cleaner.QueueLength())
}

func TestSomething(t *testing.T) {
	cluster, err := gocb.Connect("couchbase://10.112.210.101/", gocb.ClusterOptions{
		Username: "Administrator",
		Password: "password",
	})
	assert.NoError(t, err, "connect failed")

	bucket := cluster.Bucket("default")
	collection := bucket.DefaultCollection()

	// Do the setup stuff

	err = cluster.Buckets().FlushBucket("default", nil)
	assert.NoError(t, err, "delete-all failed")
	time.Sleep(1 * time.Second)

	/* CASES TO TEST FOR
	INSERT = INSERT
	REPLACE = REPLACE
	REMOVE = REMOVE
	INSERT -> INSERT --
	INSERT -> REPLACE = INSERT
	INSERT -> REMOVE = *NOTHING*
	REPLACE -> INSERT --
	REPLACE -> REPLACE = REPLACE
	REPLACE -> REMOVE = REMOVE
	REMOVE -> INSERT = REPLACE
	REMOVE -> REPLACE --
	REMOVE -> REMOVE --
	*/

	testDummy1 := map[string]string{"name": "joel"}
	testDummy2 := []byte(`{"name":"mike"}`)
	testDummy3 := []byte(`{"name":"frank"}`)
	// insertDoc
	_, err = collection.Upsert("replaceDoc", testDummy1, nil)
	assert.NoError(t, err, "replaceDoc upsert failed")
	_, err = collection.Upsert("removeDoc", testDummy1, nil)
	assert.NoError(t, err, "removeDoc upsert failed")
	// insertToInsertDoc
	// insertToReplaceDoc
	// insertToRemoveDoc
	_, err = collection.Upsert("replaceToInsertDoc", testDummy1, nil)
	assert.NoError(t, err, "replaceToInsertDoc upsert failed")
	_, err = collection.Upsert("replaceToReplaceDoc", testDummy1, nil)
	assert.NoError(t, err, "replaceToReplaceDoc upsert failed")
	_, err = collection.Upsert("replaceToRemoveDoc", testDummy1, nil)
	assert.NoError(t, err, "replaceToRemoveDoc upsert failed")
	_, err = collection.Upsert("removeToInsertDoc", testDummy1, nil)
	assert.NoError(t, err, "removeToInsertDoc upsert failed")
	_, err = collection.Upsert("removeToReplaceDoc", testDummy1, nil)
	assert.NoError(t, err, "removeToReplaceDoc upsert failed")
	_, err = collection.Upsert("removeToRemoveDoc", testDummy1, nil)
	assert.NoError(t, err, "removeToRemoveDoc upsert failed")

	// Setup Complete, start doing some work

	agent, err := bucket.Internal().IORouter()
	assert.NoError(t, err, "agent fetch failed")

	testScopeName := ""
	testCollectionName := ""

	transactions, err := Init(&Config{
		DurabilityLevel: DurabilityLevelNone,
		BucketAgentProvider: func(bucketName string) (*gocbcore.Agent, error) {
			// We can always return just this one agent as we only actually
			// use a single bucket for this entire test.
			return agent, nil
		},
		ExpirationTime: 60 * time.Second,
	})
	assert.NoError(t, err, "txn init failed")

	txn, err := transactions.BeginTransaction(nil)
	assert.NoError(t, err, "txn begin failed")

	// Start the attempt
	err = txn.NewAttempt()
	assert.NoError(t, err, "txn attempt creation failed")
	log.Printf("initial attempt: %+v", txn.attempt)

	// INSERT - pre-serialization
	insertRes, err := testBlkInsert(txn, InsertOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`insertDoc`),
		Value:          testDummy2,
	})
	assert.NoError(t, err, "insert of insertDoc failed")
	log.Printf("insert of insertDoc result: %+v", insertRes)

	// REPLACE - pre-serialization
	replaceGetRes, err := testBlkGet(txn, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`replaceDoc`),
	})
	assert.NoError(t, err, "replaceDoc get failed")
	log.Printf("replaceDoc get result: %+v", replaceGetRes)

	replaceRes, err := testBlkReplace(txn, ReplaceOptions{
		Document: replaceGetRes,
		Value:    testDummy2,
	})
	assert.NoError(t, err, "replaceDoc replace failed")
	log.Printf("replaceDoc replace result: %+v", replaceRes)

	// REMOVE - pre-serialization
	removeGetRes, err := testBlkGet(txn, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`removeDoc`),
	})
	assert.NoError(t, err, "removeDoc get failed")
	log.Printf("removeDoc get result: %+v", removeGetRes)

	removeRes, err := testBlkRemove(txn, RemoveOptions{
		Document: removeGetRes,
	})
	assert.NoError(t, err, "removeRes remove failed")
	log.Printf("removeRes remove result: %+v", removeRes)

	// INSERT -> INSERT - pre-serialization
	insertToInsertRes, err := testBlkInsert(txn, InsertOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`insertToInsertDoc`),
		Value:          testDummy2,
	})
	assert.NoError(t, err, "insertToInsertDoc insert failed")
	log.Printf("insertToInsertDoc insert result: %+v", insertToInsertRes)

	// INSERT -> REPLACE - pre-serialization
	insertToReplaceInsertRes, err := testBlkInsert(txn, InsertOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`insertToReplaceDoc`),
		Value:          testDummy2,
	})
	assert.NoError(t, err, "insertToReplaceDoc insert failed")
	log.Printf("insertToReplaceDoc insert result: %+v", insertToReplaceInsertRes)

	// INSERT -> REMOVE - pre-serialization
	insertToRemoveInsertRes, err := testBlkInsert(txn, InsertOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`insertToRemoveDoc`),
		Value:          testDummy2,
	})
	assert.NoError(t, err, "insertToRemoveDoc insert failed")
	log.Printf("insertToRemoveDoc insert result: %+v", insertToRemoveInsertRes)

	// REPLACE -> INSERT - pre-serialization
	replaceToInsertGetRes, err := testBlkGet(txn, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`replaceToInsertDoc`),
	})
	assert.NoError(t, err, "replaceToInsertDoc get failed")
	log.Printf("replaceToInsertDoc get result: %+v", replaceToInsertGetRes)

	replaceToInsertReplaceRes, err := testBlkReplace(txn, ReplaceOptions{
		Document: replaceToInsertGetRes,
		Value:    testDummy2,
	})
	assert.NoError(t, err, "replaceToInsertDoc replace failed")
	log.Printf("replaceToInsertDoc replace result: %+v", replaceToInsertReplaceRes)

	// REPLACE -> REPLACE - pre-serialization
	replaceToReplaceGetRes, err := testBlkGet(txn, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`replaceToReplaceDoc`),
	})
	assert.NoError(t, err, "replaceToReplaceDoc get1 failed")
	log.Printf("replaceToReplaceDoc get1 result: %+v", replaceToInsertGetRes)

	replaceToReplaceReplaceRes, err := testBlkReplace(txn, ReplaceOptions{
		Document: replaceToReplaceGetRes,
		Value:    testDummy2,
	})
	assert.NoError(t, err, "replaceToReplaceDoc replace failed")
	log.Printf("replaceToReplaceDoc replace result: %+v", replaceToReplaceReplaceRes)

	// REPLACE -> REMOVE - pre-serialization
	replaceToRemoveGetRes, err := testBlkGet(txn, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`replaceToRemoveDoc`),
	})
	assert.NoError(t, err, "replaceToRemoveDoc get1 failed")
	log.Printf("replaceToRemoveDoc get1 result: %+v", replaceToRemoveGetRes)

	replaceToRemoveReplaceRes, err := testBlkReplace(txn, ReplaceOptions{
		Document: replaceToRemoveGetRes,
		Value:    testDummy2,
	})
	assert.NoError(t, err, "replaceToRemoveDoc replace failed")
	log.Printf("replaceToRemoveDoc replace result: %+v", replaceToRemoveReplaceRes)

	// REMOVE -> INSERT - pre-serialization
	removeToInsertGetRes, err := testBlkGet(txn, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`removeToInsertDoc`),
	})
	assert.NoError(t, err, "removeToInsertDoc get failed")
	log.Printf("removeToInsertDoc get result: %+v", removeToInsertGetRes)

	removeToInsertRemoveRes, err := testBlkRemove(txn, RemoveOptions{
		Document: removeToInsertGetRes,
	})
	assert.NoError(t, err, "removeToInsertDoc remove failed")
	log.Printf("removeToInsertDoc remove result: %+v", removeToInsertRemoveRes)

	// REMOVE -> REPLACE - pre-serialization
	removeToReplaceGetRes, err := testBlkGet(txn, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`removeToReplaceDoc`),
	})
	assert.NoError(t, err, "removeToReplaceDoc get failed")
	log.Printf("removeToReplaceDoc get result: %+v", removeToReplaceGetRes)

	removeToReplaceRemoveRes, err := testBlkRemove(txn, RemoveOptions{
		Document: removeToReplaceGetRes,
	})
	assert.NoError(t, err, "removeToReplaceDoc remove failed")
	log.Printf("removeToReplaceDoc remove result: %+v", removeToReplaceRemoveRes)

	// REMOVE -> REMOVE - pre-serialization
	removeToRemoveGetRes, err := testBlkGet(txn, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`removeToRemoveDoc`),
	})
	assert.NoError(t, err, "removeToRemoveDoc get failed")
	log.Printf("removeToRemoveDoc get result: %+v", removeToRemoveGetRes)

	removeToRemoveRemoveRes, err := testBlkRemove(txn, RemoveOptions{
		Document: removeToRemoveGetRes,
	})
	assert.NoError(t, err, "removeToRemoveDoc remove failed")
	log.Printf("removeToRemoveDoc remove result: %+v", removeToRemoveRemoveRes)

	log.Printf("pre-serialize attempt: %+v", txn.attempt)

	txnBytes, err := testBlkSerialize(txn)
	assert.NoError(t, err, "txn serialize failed")

	log.Printf("resume data was... %s", txnBytes)

	txn, err = transactions.ResumeTransactionAttempt(txnBytes)
	assert.NoError(t, err, "txn resume failed")

	log.Printf("resumed attempt: %+v", txn.attempt)

	// INSERT - post-serialization

	// REPLACE - post-serialization

	// REMOVE - post-serialization

	// INSERT -> INSERT - post-serialization
	_, err = testBlkInsert(txn, InsertOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`insertToInsertDoc`),
		Value:          testDummy3,
	})
	assert.Error(t, err, "insertToInsertDoc insert should have failed")

	// INSERT -> REPLACE - post-serialization
	insertToReplaceGet2Res, err := testBlkGet(txn, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`insertToReplaceDoc`),
	})
	assert.NoError(t, err, "insertToReplaceDoc get2 failed")
	assert.EqualValues(t, insertToReplaceInsertRes.Cas, insertToReplaceGet2Res.Cas, "insertToReplaceDoc insert and get cas did not match")
	assert.EqualValues(t, insertToReplaceInsertRes.Meta, insertToReplaceGet2Res.Meta, "insertToReplaceDoc insert and get meta did not match")
	log.Printf("insertToReplaceDoc get2 result: %+v", insertToReplaceGet2Res)

	insertToReplaceReplaceRes, err := testBlkReplace(txn, ReplaceOptions{
		Document: insertToReplaceInsertRes,
		Value:    testDummy3,
	})
	assert.NoError(t, err, "insertToReplaceDoc replace failed")
	log.Printf("insertToReplaceDoc replace result: %+v", insertToReplaceReplaceRes)

	// INSERT -> REMOVE - post-serialization
	insertToRemoveGet2Res, err := testBlkGet(txn, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`insertToRemoveDoc`),
	})
	assert.NoError(t, err, "insertToRemoveDoc get2 failed")
	assert.EqualValues(t, insertToRemoveInsertRes.Cas, insertToRemoveGet2Res.Cas, "insertToRemoveDoc insert and get cas did not match")
	assert.EqualValues(t, insertToRemoveInsertRes.Meta, insertToRemoveGet2Res.Meta, "insertToRemoveDoc insert and get meta did not match")
	log.Printf("insertToRemoveDoc get2 result: %+v", insertToRemoveGet2Res)

	insertToRemoveRemoveRes, err := testBlkRemove(txn, RemoveOptions{
		Document: insertToRemoveGet2Res,
	})
	assert.NoError(t, err, "insertToRemoveDoc remove failed")
	log.Printf("insertToRemoveDoc remove result: %+v", insertToRemoveRemoveRes)

	// REPLACE -> INSERT - post-serialization
	_, err = testBlkInsert(txn, InsertOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`replaceToInsertDoc`),
		Value:          testDummy3,
	})
	assert.Error(t, err, "replaceToInsertDoc insert should have failed")

	// REPLACE -> REPLACE - post-serialization
	replaceToReplaceGet2Res, err := testBlkGet(txn, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`replaceToReplaceDoc`),
	})
	assert.NoError(t, err, "replaceToReplaceDoc get2 failed")
	assert.EqualValues(t, replaceToReplaceReplaceRes.Cas, replaceToReplaceGet2Res.Cas, "replaceToReplaceDoc replace and get cas did not match")
	assert.EqualValues(t, replaceToReplaceReplaceRes.Meta, replaceToReplaceGet2Res.Meta, "replaceToReplaceDoc replace and get meta did not match")
	log.Printf("replaceToReplaceDoc get2 result: %+v", replaceToReplaceGet2Res)

	replaceToReplaceReplace2Res, err := testBlkReplace(txn, ReplaceOptions{
		Document: replaceToReplaceGet2Res,
		Value:    testDummy3,
	})
	assert.NoError(t, err, "replaceToReplaceDoc replace failed")
	log.Printf("replaceToReplaceDoc replace result: %+v", replaceToReplaceReplace2Res)

	// REPLACE -> REMOVE - post-serialization
	replaceToRemoveGet2Res, err := testBlkGet(txn, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`replaceToRemoveDoc`),
	})
	assert.NoError(t, err, "replaceToRemoveDoc get2 failed")
	assert.EqualValues(t, replaceToRemoveReplaceRes.Cas, replaceToRemoveGet2Res.Cas, "replaceToRemoveDoc replace and get cas did not match")
	assert.EqualValues(t, replaceToRemoveReplaceRes.Meta, replaceToRemoveGet2Res.Meta, "replaceToRemoveDoc replace and get meta did not match")
	log.Printf("replaceToRemoveDoc get2 result: %+v", replaceToRemoveGet2Res)

	replaceToRemoveRemoveRes, err := testBlkRemove(txn, RemoveOptions{
		Document: replaceToRemoveGet2Res,
	})
	assert.NoError(t, err, "replaceToRemoveDoc remove failed")
	log.Printf("replaceToRemoveDoc remove result: %+v", replaceToRemoveRemoveRes)

	// REMOVE -> INSERT - post-serialization
	removeToInsertInsertRes, err := testBlkInsert(txn, InsertOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`removeToInsertDoc`),
		Value:          testDummy3,
	})
	assert.NoError(t, err, "removeToInsertDoc insert failed")
	log.Printf("removeToInsertDoc insert result: %+v", removeToInsertInsertRes)

	// REMOVE -> REPLACE - post-serialization
	_, err = testBlkGet(txn, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`removeToReplaceDoc`),
	})
	assert.Error(t, err, "removeToReplaceDoc get2 should have failed")

	// REMOVE -> REMOVE - post-serialization
	_, err = testBlkGet(txn, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`removeToRemoveDoc`),
	})
	assert.Error(t, err, "removeToRemoveDoc get2 should have failed")

	mutations := txn.GetMutations()
	log.Printf("Mutations List:")
	for _, mutation := range mutations {
		log.Printf("%s - %s %s %s %s - %+v",
			stagedMutationTypeToString(mutation.OpType),
			mutation.BucketName,
			mutation.ScopeName,
			mutation.CollectionName,
			mutation.Key,
			mutation.Staged)
	}

	testDummy1Bytes, _ := json.Marshal(testDummy1)

	txn2, err := transactions.BeginTransaction(nil)
	assert.NoError(t, err, "txn2 begin failed")

	err = txn2.NewAttempt()
	assert.NoError(t, err, "txn2 attempt start failed")

	// T1-INSERT -> T2-GET
	_, err = testBlkGet(txn2, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`insertDoc`),
	})
	assert.Error(t, err, "insertDoc get from T2 should have failed")

	// T1-INSERT -> T2-INSERT
	_, err = testBlkInsert(txn2, InsertOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`insertDoc`),
		Value:          testDummy3,
	})
	assert.Error(t, err, "insertDoc insert from T2 should have failed")

	// T1-INSERT -> T2-REPLACE = IMPOSSIBLE
	// T1-INSERT -> T2-REMOVE = IMPOSSIBLE

	// T1-REPLACE -> T2-GET
	getOfReplace, err := testBlkGet(txn2, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`replaceDoc`),
	})
	assert.NoError(t, err, "replaceDoc get from T2 should have succeeded")
	assert.EqualValues(t, testDummy1Bytes, getOfReplace.Value, "replaceDoc get from T2 should have right data")

	// T1-REPLACE -> T2-INSERT
	_, err = testBlkInsert(txn2, InsertOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`replaceDoc`),
	})
	assert.Error(t, err, "replaceDoc insert from T2 should have failed")

	// T1-REPLACE -> T2->REPLACE
	_, err = testBlkReplace(txn2, ReplaceOptions{
		Document: getOfReplace,
		Value:    testDummy3,
	})
	assert.Error(t, err, "replaceDoc replace from T2 should have failed")

	// T1-REPLACE -> T2->REMOVE
	_, err = testBlkRemove(txn2, RemoveOptions{
		Document: getOfReplace,
	})
	assert.Error(t, err, "replaceDoc remove from T2 should have failed")

	// T1-REMOVE -> T2->GET
	getOfRemove, err := testBlkGet(txn2, GetOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`removeDoc`),
	})
	assert.NoError(t, err, "removeDoc get from T2 should have succeeded")
	assert.EqualValues(t, testDummy1Bytes, getOfRemove.Value, "removeDoc get from T2 should have right data")

	// T1-REMOVE -> T2->INSERT
	_, err = testBlkInsert(txn2, InsertOptions{
		Agent:          agent,
		ScopeName:      testScopeName,
		CollectionName: testCollectionName,
		Key:            []byte(`removeDoc`),
	})
	assert.Error(t, err, "removeDoc insert from T2 should have failed")

	// T1-REMOVE -> T2->REPLACE
	_, err = testBlkReplace(txn2, ReplaceOptions{
		Document: getOfRemove,
		Value:    testDummy3,
	})
	assert.Error(t, err, "removeDoc replace from T2 should have failed")

	// T1-REMOVE -> T2->REMOVE
	_, err = testBlkRemove(txn2, RemoveOptions{
		Document: getOfRemove,
	})
	assert.Error(t, err, "removeDoc remove from T2 should have failed")

	fetchStagedOpData := func(key string) (jsonMutationType, []byte, bool) {
		lookupOpts := &gocb.LookupInOptions{}
		lookupOpts.Internal.DocFlags |= gocb.SubdocDocFlagAccessDeleted
		res, err := collection.LookupIn(string(key), []gocb.LookupInSpec{
			gocb.GetSpec("txn.op.type", &gocb.GetSpecOptions{
				IsXattr: true,
			}),
			gocb.GetSpec("txn.op.stgd", &gocb.GetSpecOptions{
				IsXattr: true,
			}),
		}, lookupOpts)
		if err != nil {
			return "", nil, false
		}

		var opType string
		res.ContentAt(0, &opType)

		var stgdData []byte
		res.ContentAt(1, &stgdData)

		exists, _ := collection.Exists(key, nil)

		return jsonMutationType(opType), stgdData, exists.Exists()
	}

	assertStagedDoc := func(key string, expOpType jsonMutationType, expStgdData []byte, expTombstone bool) {
		stgdOpType, stgdData, docExists := fetchStagedOpData(key)

		assert.EqualValues(t,
			expOpType,
			stgdOpType,
			fmt.Sprintf("%s had an incorrect op type", key))
		assert.EqualValues(t,
			expStgdData,
			stgdData,
			fmt.Sprintf("%s had an incorrect staged data", key))
		assert.EqualValues(t,
			expTombstone,
			!docExists,
			fmt.Sprintf("%s document state did not match", key))
	}

	assertStagedDoc("insertDoc", jsonMutationInsert, testDummy2, true)
	assertStagedDoc("replaceDoc", jsonMutationReplace, testDummy2, false)
	assertStagedDoc("removeDoc", jsonMutationRemove, nil, false)
	assertStagedDoc("insertToInsertDoc", jsonMutationInsert, testDummy2, true)
	assertStagedDoc("insertToReplaceDoc", jsonMutationInsert, testDummy3, true)
	assertStagedDoc("insertToRemoveDoc", "", nil, true)
	assertStagedDoc("replaceToInsertDoc", jsonMutationReplace, testDummy2, false)
	assertStagedDoc("replaceToReplaceDoc", jsonMutationReplace, testDummy3, false)
	assertStagedDoc("replaceToRemoveDoc", jsonMutationRemove, nil, false)
	assertStagedDoc("removeToInsertDoc", jsonMutationReplace, testDummy3, false)
	assertStagedDoc("removeToReplaceDoc", jsonMutationRemove, nil, false)
	assertStagedDoc("removeToRemoveDoc", jsonMutationRemove, nil, false)

	err = testBlkCommit(txn)
	assert.NoError(t, err, "commit failed")

	assertStagedDoc("insertDoc", "", nil, false)
	assertStagedDoc("replaceDoc", "", nil, false)
	assertStagedDoc("removeDoc", "", nil, true)
	assertStagedDoc("insertToInsertDoc", "", nil, false)
	assertStagedDoc("insertToReplaceDoc", "", nil, false)
	assertStagedDoc("insertToRemoveDoc", "", nil, true)
	assertStagedDoc("replaceToInsertDoc", "", nil, false)
	assertStagedDoc("replaceToReplaceDoc", "", nil, false)
	assertStagedDoc("replaceToRemoveDoc", "", nil, true)
	assertStagedDoc("removeToInsertDoc", "", nil, false)
	assertStagedDoc("removeToReplaceDoc", "", nil, true)
	assertStagedDoc("removeToRemoveDoc", "", nil, true)
}
