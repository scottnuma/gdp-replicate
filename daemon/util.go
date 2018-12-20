package daemon

import (
	"fmt"
	"log"

	"github.com/tonyyanga/gdp-replicate/gdp"
	"github.com/tonyyanga/gdp-replicate/policy"
	"go.uber.org/zap"
)

// msgPrinter is a message handler that displays the msg.
func msgPrinter(src gdp.Hash, msg *policy.Message) {
	fmt.Printf("received message")
}

// InitLogger initializes the Zap logger.
// All logs produced are tagged as from the replciation daemon with
// the address.
func InitLogger(addr gdp.Hash) {
	zapLogger, err := zap.NewDevelopment()
	zapLogger = zapLogger.With(
		zap.String("selfAddr", addr.Readable()),
	)
	if err != nil {
		log.Fatal("failed to create logger:", err.Error())
	}
	zap.ReplaceGlobals(zapLogger)
}
