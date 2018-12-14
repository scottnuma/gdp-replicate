package gdplogd

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogGraph(t *testing.T) {
	db, err := sql.Open("sqlite3", SQL_FILE)
	assert.Nil(t, err)
	defer db.Close()

	log, err := InitLogGraph(Hash{}, db)
	require.Nil(t, err)

	assert.NotEqual(t, 0, len(log.logEntries))
	assert.NotEqual(t, 0, len(log.GetLogicalBegins()))
	assert.NotEqual(t, 0, len(log.GetLogicalEnds()))
}

func printLogGraph(log LogGraphWrapper) {
	fmt.Println("Actual Pointer Map:")
	for key, val := range log.GetActualPtrMap() {
		fmt.Printf("%x -> %x\n", key, val)
	}

	fmt.Println("Logical Pointer Map:")
	for key, hashes := range log.GetLogicalPtrMap() {
		fmt.Printf("\n%x -> \n", key)
		for _, hash := range hashes {
			fmt.Printf("\t%x\n", hash)
		}
	}

	fmt.Println("Logical Begins:")
	for _, hash := range log.GetLogicalBegins() {
		fmt.Printf("%x\n", hash)
	}

	fmt.Println("Logical Ends:")
	for _, hash := range log.GetLogicalEnds() {
		fmt.Printf("%x\n", hash)
	}
}
