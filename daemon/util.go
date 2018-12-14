package daemon

import (
	"fmt"
	"log"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"github.com/tonyyanga/gdp-replicate/policy"
	"go.uber.org/zap"
)

// msgPrinter is a message handler that displays the msg.
func msgPrinter(src gdplogd.Hash, msg *policy.Message) {
	fmt.Printf("received message")
}

// InitLogger initializes the Zap logger
func InitLogger(addr gdplogd.Hash) {
	zapLogger, err := zap.NewDevelopment()
	zapLogger = zapLogger.With(
		zap.String("selfAddr", gdplogd.ReadableAddr(addr)),
	)
	if err != nil {
		log.Fatal("failed to create logger:", err.Error())
	}
	zap.ReplaceGlobals(zapLogger)
}
