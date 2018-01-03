package main

import (
	"fmt"
	"github.com/lwldcr/gohive"
	"os"
)

func main() {
	// first build a new transport
	// of course we can wrap all these routine operations into functions for repeatedly using
	// replace HIVE_HOST, PORT, HIVE_USER, HIVE_PASSWD with your own configurations
	t, err := gohive.NewTSaslTransport(HIVE_HOST, PORT, HIVE_USER, HIVE_PASSWD, gohive.DefaultOptions)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// now open it
	if err := t.Open(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer t.Close()

	// execute query
	rows, err := t.Query("SHOW TABLES")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer rows.Close()

	// check status
	status, err := rows.Wait()
	if !status.IsSuccess() {
		fmt.Println("unsuccessful query", status)
		os.Exit(1)
	}

	var (
		tableName string
	)

	// scan results
	for rows.Next() {
		rows.Scan(&tableName)
		fmt.Println("tablename:", tableName)
	}
}
