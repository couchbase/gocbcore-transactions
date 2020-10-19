package transactions

func testBlkGet(txn *Transaction, opts GetOptions) (resOut *GetResult, errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Get(opts, func(res *GetResult, err error) {
		resOut = res
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkInsert(txn *Transaction, opts InsertOptions) (resOut *GetResult, errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Insert(opts, func(res *GetResult, err error) {
		resOut = res
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkReplace(txn *Transaction, opts ReplaceOptions) (resOut *GetResult, errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Replace(opts, func(res *GetResult, err error) {
		resOut = res
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkRemove(txn *Transaction, opts RemoveOptions) (resOut *GetResult, errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Remove(opts, func(res *GetResult, err error) {
		resOut = res
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkCommit(txn *Transaction) (errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Commit(func(err error) {
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkRollback(txn *Transaction) (errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.Rollback(func(err error) {
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}
	<-waitCh
	return
}

func testBlkSerialize(txn *Transaction) (txnBytesOut []byte, errOut error) {
	waitCh := make(chan struct{}, 1)
	err := txn.SerializeAttempt(func(txnBytes []byte, err error) {
		txnBytesOut = txnBytes
		errOut = err
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}
	<-waitCh
	return
}
