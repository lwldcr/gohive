### GoHive

A hive client with SASL support based on thrift.

#### Usage

```go
package main

import (
	"fmt"
	"tcliservice"
	"github.com/lwldcr/gohive"
	"os"
)

func main() {
	// first build a new transport
	// of course we can wrap all these routine operations into functions for repeatedly using
	t, err := gohive.NewTSaslTransport(HIVE_HOST, PORT, HIVE_USER, HIVE_PASSWD)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// now open it
	if err := t.Open(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// then you get a session, and you can execute query now
	sessionHandler := t.Session
	execReq := tcliservice.NewTExecuteStatementReq()
	execReq.SessionHandle = sessionHandler
	execReq.Statement = "show databases"
	execResp, err := t.Client.ExecuteStatement(execReq)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// fetch result after executing
	operationHandler := execResp.GetOperationHandle()
	fetchReq := tcliservice.NewTFetchResultsReq()
	fetchReq.OperationHandle = operationHandler
	fetchReq.Orientation = tcliservice.TFetchOrientation_FETCH_FIRST
	fetchReq.MaxRows = 10
	fetchResp, err := t.Client.FetchResults(fetchReq)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// there you get your data
	data := make([]string, 0)
	res := fetchResp.GetResults().GetRows()
	for _,  r := range res {
		row := r.GetColVals()
		for _, field := range row {
			data = append(data, field.GetStringVal().GetValue())
		}
	}


	fmt.Println("data:", data)

	// do cleaning
	closeOperationReq := tcliservice.NewTCloseOperationReq()
	closeOperationReq.OperationHandle = operationHandler
	t.Client.CloseOperation(closeOperationReq)
	t.Close()
}
```