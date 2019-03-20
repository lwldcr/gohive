### GoHive

A hive client with SASL support based on thrift.

#### Supported Versions
The latest commit is built with:
* Thrift: 0.12.0
* Hive: 2.2.0

Other versions' compatibilities are not guaranteed, use at your own risk.
Also, you could build your own version hive client follow the steps at the end of this README.

#### Problems not solved
I found one strange thing: my Hive is configured to NONE authorization, but every time I got an "unexpected EOF" error 
when I tried to connect with emply username & password given, but It works whatever username & password given. 
So i use a DefaultUser & DefaultPass variables when no username & password given in my code to make it works. Please 
 tell me if anybody knows the reason. 
 
#### Usage

```go
package main

import (
	"fmt"
	"os"

	"github.com/lwldcr/gohive"
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
    		fmt.Println("open failed:", err)
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
    		fmt.Println("table:", tableName)
    	}
}
```

#### How to Build
I built things on CentOS 6.5, here are the steps:
1. build thrift
    * follow steps from [apache web page](http://thrift.apache.org/docs/install/centos), build & install thrift
    * if you doesnot need cpp/lua/py support, disable them with configure command:
    ```bash
      ./configure --with-lua=no --with-php=no
    ``` 
     that will save you much time building dependencies
    * if you build thrift with go support inside GFW, a proxy is needed to fetch golang.org/x/net/context
2. get Hive source code of your Hive version from [github](https://github.com/apache/hive)
3. use thrift definition of your Hive version and generate tcliservice with command:
    ```bash
    thrift --gen go -o src/gen/thrift if/TCLIService.thrift
    ```
4. debug & update until it works(mostly handling response data)

  