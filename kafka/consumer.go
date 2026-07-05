package kafka

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/edenhill/kcat-go/config"
	"github.com/edenhill/kcat-go/format"
)

func RunConsumer(c *Consumer, outWriter *format.MessagePrinter) error {
	topicName := config.Cfg.Topic

	if topicName == "" {
		return fmt.Errorf("kafka: -t <topic> required")
	}

	if outWriter == nil {
		if config.Cfg.Flags&config.FmtJSON != 0 {
			outWriter = format.MustParse("\n")
		} else if config.Cfg.KeyDelim != "" {
			outWriter = format.MustParse("%k" + config.Cfg.KeyDelim + "%s\n")
		} else {
			outWriter = format.MustParse("%s\n")
		}
	}

	partStop := make(map[int]bool)
	partStopCnt := 0
	partStopThres := 0
	if config.Cfg.ExitEOF || config.Cfg.StopTS != 0 {
		if config.Cfg.Partition != -1000 {
			partStopThres = 1
		} else {
			partStopThres = 1
		}
	}

	for config.Cfg.Run {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		msg, err := c.ReadMessage(ctx)
		cancel()
		if err != nil {
			if err == context.DeadlineExceeded {
				continue
			}
			return fmt.Errorf("kafka: consumer error: %w", err)
		}

		if config.Cfg.StopTS != 0 {
			ts := msg.Time.UnixMilli()
			if ts >= config.Cfg.StopTS {
				pid := int(msg.Partition)
				if !partStop[pid] {
					partStop[pid] = true
					partStopCnt++
					if partStopCnt >= partStopThres {
						config.Cfg.Run = false
					}
				}
				continue
			}
		}

		if outWriter != nil {
			if err := outWriter.Print(os.Stdout, msg); err != nil {
				fmt.Fprintf(os.Stderr, "kcat: format error: %v\n", err)
			}
		}

		if config.Cfg.Mode == config.ModeConsumer {
			c.CommitMessages(context.Background(), msg)
		}

		config.Cfg.Rx++
		if config.Cfg.MsgCount > 0 && config.Cfg.Rx >= uint64(config.Cfg.MsgCount) {
			config.Cfg.Run = false
			c.Close()
			break
		}
	}

	return nil
}
