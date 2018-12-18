package loggraph

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/tonyyanga/gdp-replicate/logserver"
)

func TestSimpleGraph(t *testing.T) {
	DB_DIR := "/home/scott/go/src/github.com/tonyyanga/gdp-replicate/benchmark/example_db"
	dbFile := fmt.Sprintf("%s/hole_and_branch.glob", DB_DIR)
	db, err := sql.Open("sqlite3", dbFile)
	assert.Nil(t, err)

	logServer := logserver.NewSqliteServer(db)
	graph, err := NewSimpleGraph(logServer)
	assert.Nil(t, err)

	assert.Equal(t, 3, len(graph.GetLogicalEnds()))
	assert.Equal(t, 2, len(graph.GetLogicalBegins()))
	assert.Equal(t, 5, len(graph.nodeMap))
	assert.Equal(t, 5, len(graph.backwardEdges))
	assert.Equal(t, 4, len(graph.forwardEdges))

	fmt.Println("forward edges")
	for k, vs := range graph.forwardEdges {
		fmt.Printf("%s->\n", k.Readable())
		for _, v := range vs {
			fmt.Printf("\t%s\n", v.Readable())
		}

	}
	fmt.Println("backward edges")
	for k, v := range graph.backwardEdges {
		fmt.Printf("%s<-%s\n", v.Readable(), k.Readable())
	}

	fmt.Println("nodes")
	for v, _ := range graph.nodeMap {
		fmt.Println(v.Readable())
	}

	fmt.Println("logical starts")
	for _, vs := range graph.logicalStarts {
		for _, v := range vs {
			fmt.Println(v.Readable())
		}
	}

	fmt.Println("logical ends")
	for v, _ := range graph.logicalEnds {
		fmt.Println(v.Readable())
	}
}
