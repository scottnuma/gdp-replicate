package daemon

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tonyyanga/gdp-replicate/gdp"
	"go.uber.org/zap"
)

func generateDaemons(dbFiles []string) ([]Daemon, error) {
	numDaemons := len(dbFiles)

	ports := make([]string, numDaemons, numDaemons)
	seedPort := 8000
	for i := 0; i < numDaemons; i++ {
		port := seedPort + i
		ports[i] = "localhost:" + strconv.Itoa(port)
	}

	hashAddrs := make([]gdp.Hash, numDaemons, numDaemons)
	for i := 0; i < numDaemons; i++ {
		hashAddrs[i] = gdp.GenerateHash(ports[i])
	}

	peerAddrMap := make(map[gdp.Hash]string)
	for i := 0; i < numDaemons; i++ {
		peerAddrMap[hashAddrs[i]] = ports[i]
	}

	daemons := make([]Daemon, 0, numDaemons)
	for i := 0; i < numDaemons; i++ {
		thisPeerAddrMap := make(map[gdp.Hash]string)
		for hash, addr := range peerAddrMap {
			if hash == hashAddrs[i] {
				continue
			}
			thisPeerAddrMap[hash] = addr
		}

		daemon, err := NewNaiveDaemon(
			ports[i],
			dbFiles[i],
			hashAddrs[i],
			thisPeerAddrMap,
		)

		if err != nil {
			return nil, err
		}

		daemons = append(daemons, *daemon)
	}
	return daemons, nil
}

func TestDaemon(t *testing.T) {
	zapLogger, err := zap.NewDevelopment()
	assert.Nil(t, err)
	zap.ReplaceGlobals(zapLogger)

	zap.S().Info("Beginning test")

	dbDir := "/home/scott/go/src/github.com/tonyyanga/gdp-replicate/benchmark/example_db/%s.glob"
	dbNames := []string{"simple_long", "simple_short"}
	dbFiles := make([]string, 0, len(dbNames))

	for _, name := range dbNames {
		dbFiles = append(dbFiles, fmt.Sprintf(dbDir, name))
	}
	daemons, err := generateDaemons(dbFiles)
	for _, daemon := range daemons {
		go daemon.Start(1)
	}
	zap.S().Info("Waiting")
	time.Sleep(time.Duration(1200) * time.Millisecond)
	assert.Nil(t, 3)
}
