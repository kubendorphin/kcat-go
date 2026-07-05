package kafka

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/edenhill/kcat-go/config"
	"github.com/edenhill/kcat-go/format"
)

func RunConsumerGroup(c *Consumer, topics []string, outWriter *format.MessagePrinter) error {
	if len(topics) == 0 {
		return fmt.Errorf("kafka: at least one topic required for -G mode")
	}

	fmt.Fprintf(os.Stderr, "%% Waiting for group rebalance\n")

	if outWriter == nil {
		if config.Cfg.Flags&config.FmtJSON != 0 {
			outWriter = format.MustParse("\n")
		} else if config.Cfg.KeyDelim != "" {
			outWriter = format.MustParse("%k" + config.Cfg.KeyDelim + "%s\n")
		} else {
			outWriter = format.MustParse("%s\n")
		}
	}

	for config.Cfg.Run {
		ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
		msg, err := c.ReadMessage(ctx)
		cancel()
		if err != nil {
			if err == context.DeadlineExceeded {
				continue
			}
			fmt.Fprintf(os.Stderr, "%% Consumer error: %v\n", err)
			return nil
		}

		if outWriter != nil {
			if err := outWriter.Print(os.Stdout, msg); err != nil {
				fmt.Fprintf(os.Stderr, "kcat: format error: %v\n", err)
			}
		}

		config.Cfg.Rx++
		if config.Cfg.MsgCount > 0 && config.Cfg.Rx >= uint64(config.Cfg.MsgCount) {
			config.Cfg.Run = false
			break
		}
	}

	if err := c.Close(); err != nil {
		return fmt.Errorf("kafka: failed to close consumer: %w", err)
	}

	return nil
}
