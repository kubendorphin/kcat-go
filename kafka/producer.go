package kafka

import (
	"context"
	"fmt"
	"os"

	"github.com/segmentio/kafka-go"

	"github.com/edenhill/kcat-go/config"
	"github.com/edenhill/kcat-go/input"
	util "github.com/edenhill/kcat-go/util"
)

type ProduceResult struct {
	messagesProduced  uint64
	messagesDelivered uint64
	errors            uint64
}

func RunProducer(c *Producer, filePaths []string, drChan chan kafka.Message) error {
	topicName := config.Cfg.Topic

	if len(filePaths) > 0 && config.Cfg.Flags&config.LineMode == 0 {
		return produceFromFiles(c, filePaths, topicName)
	}

	return produceFromStdin(c, topicName, drChan)
}

func produceFromFiles(c *Producer, filePaths []string, topic string) error {
	good := 0
	for _, path := range filePaths {
		if err := produceFile(c, path, topic); err != nil {
			fmt.Fprintf(os.Stderr, "kcat: Failed to produce from %s: %v\n", path, err)
		} else {
			good++
		}
	}

	if good == 0 {
		config.Cfg.ExitCode = 1
		return fmt.Errorf("no files produced successfully")
	}
	if good < len(filePaths) {
		fmt.Fprintf(os.Stderr, "Warning: failed to produce from %d/%d files\n",
			len(filePaths)-good, len(filePaths))
	}
	return nil
}

func produceFile(c *Producer, path, topic string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}

	if len(data) == 0 {
		return nil
	}

	msg := kafka.Message{
		Topic: topic,
		Value: data,
	}

	if err := c.WriteMessages(context.Background(), msg); err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	return nil
}

func produceFromStdin(c *Producer, topic string, drChan chan kafka.Message) error {
	delim := []byte("\n")
	if config.Cfg.Delim != "" {
		delim = []byte(config.Cfg.Delim)
	}

	buf := input.NewBuffer(delim, config.Cfg.MsgSize)

	keyDelim := []byte{}
	if config.Cfg.KeyDelim != "" {
		keyDelim = []byte(config.Cfg.KeyDelim)
	}

	fixedKey := config.Cfg.FixedKey

	msgCount := uint64(0)

	for config.Cfg.Run {
		data, more, _ := buf.Next(os.Stdin)
		if !more {
			break
		}

		if len(data) == 0 {
			continue
		}

		var key []byte
		value := data
		if config.Cfg.Flags&config.KeyDelim != 0 && len(keyDelim) > 0 {
			idx := util.Strnstr(data, keyDelim)
			if idx >= 0 {
				key = data[:idx]
				value = data[idx+len(keyDelim):]
				if config.Cfg.Flags&config.NullEmpty != 0 {
					if len(value) == 0 {
						value = nil
					}
					if len(key) == 0 {
						key = nil
					}
				}
			}
		}

		if key == nil && len(fixedKey) > 0 {
			key = fixedKey
		}

		var msgKey, msgValue []byte
		if len(key) < 1024 {
			msgKey = make([]byte, len(key))
			copy(msgKey, key)
		} else {
			msgKey = key
		}
		if len(value) < 1024 {
			msgValue = make([]byte, len(value))
			copy(msgValue, value)
		} else {
			msgValue = value
		}

		msg := kafka.Message{
			Topic: topic,
			Key:   msgKey,
			Value: msgValue,
		}

		if err := c.WriteMessages(context.Background(), msg); err != nil {
			return fmt.Errorf("kcat: Failed to produce message (%d bytes): %w", len(value), err)
		}

		msgCount++

		if config.Cfg.MsgCount > 0 && msgCount >= uint64(config.Cfg.MsgCount) {
			config.Cfg.Run = false
			break
		}

		if config.Cfg.Flags&config.TeeOutput != 0 {
			if _, err := os.Stdout.Write(data); err != nil {
				return fmt.Errorf("kcat: Tee write error: %w", err)
			}
		}
	}

	return nil
}
