package gdplogd

import (
	"database/sql"
	"fmt"
	"log"
)

const SQL_FILE = "/home/scott/go/src/github.com/tonyyanga/gdp-replicate/gdplogd/sample.glog"

// Demonstrate the ability to create, write to and read from a database.
func SqlDemo() {
	db, err := sql.Open("sqlite3", SQL_FILE)
	checkError(err)
	defer db.Close()

	var log LogGraphWrapper

	logGraph, err := InitLogGraph([32]byte{}, db)
	log = &logGraph
	checkError(err)

	fmt.Println("Logical Begins:")
	for _, hash := range log.GetLogicalBegins() {
		fmt.Printf("%x\n", hash)
	}

}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
