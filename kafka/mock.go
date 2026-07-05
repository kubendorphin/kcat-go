package kafka

import (
	"fmt"
	"os"

	"github.com/edenhill/kcat-go/config"
)

func RunMock() error {
	fmt.Fprintf(os.Stderr, "%% Mock cluster mode: broker count %d\n",
		config.Cfg.MockBrokerCount)
	fmt.Printf("BROKERS=localhost:9092\n")
	fmt.Fprintf(os.Stderr, "Press Ctrl-C+Enter or Ctrl-D to terminate.\n")

	for config.Cfg.Run {
		break
	}

	return nil
}
