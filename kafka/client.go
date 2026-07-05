package kafka

import (
	"crypto/tls"
	"fmt"
	"os"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"

	"github.com/edenhill/kcat-go/config"
)

type Producer struct {
	*kafka.Writer
}

type Consumer struct {
	*kafka.Reader
}

func NewProducer() (*Producer, error) {
	brokers := strings.Split(config.Cfg.Brokers, ",")
	if len(brokers) == 0 || brokers[0] == "" {
		return nil, fmt.Errorf("kafka: no bootstrap brokers specified")
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        config.Cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
	}

	if v, ok := config.Cfg.KafkaConf["acks"]; ok {
		switch v {
		case "0":
			writer.RequiredAcks = kafka.RequireNone
		case "all", "-1":
			writer.RequiredAcks = kafka.RequireAll
		default:
			writer.RequiredAcks = kafka.RequireOne
		}
	}

	setupSecurity(writer)

	return &Producer{Writer: writer}, nil
}

func NewConsumer() (*Consumer, error) {
	brokers := strings.Split(config.Cfg.Brokers, ",")
	if len(brokers) == 0 || brokers[0] == "" {
		return nil, fmt.Errorf("kafka: no bootstrap brokers specified")
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     config.Cfg.Topic,
		Partition: int(config.Cfg.Partition),
		MinBytes:  1,
		MaxBytes:  10e6,
	}

	if config.Cfg.Offset == config.OffsetBeginning {
		readerConfig.StartOffset = kafka.FirstOffset
	} else if config.Cfg.Offset == config.OffsetEnd {
		readerConfig.StartOffset = kafka.LastOffset
	} else if config.Cfg.Offset >= 0 {
		readerConfig.StartOffset = config.Cfg.Offset
	}

	r := kafka.NewReader(readerConfig)
	return &Consumer{Reader: r}, nil
}

func NewConsumerGroup() (*Consumer, error) {
	brokers := strings.Split(config.Cfg.Brokers, ",")
	if len(brokers) == 0 || brokers[0] == "" {
		return nil, fmt.Errorf("kafka: no bootstrap brokers specified")
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        config.Cfg.Group,
		Topic:          config.Cfg.Topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	}

	r := kafka.NewReader(readerConfig)
	return &Consumer{Reader: r}, nil
}

func setupSecurity(writer *kafka.Writer) {
	t := &kafka.Transport{}

	if v, ok := config.Cfg.KafkaConf["security.protocol"]; ok && v == "SSL" {
		t.TLS = &tls.Config{}
	}

	if v, ok := config.Cfg.KafkaConf["sasl.mechanism"]; ok && v == "PLAIN" {
		username := config.Cfg.KafkaConf["sasl.username"]
		password := config.Cfg.KafkaConf["sasl.password"]
		t.SASL = plain.Mechanism{
			Username: username,
			Password: password,
		}
	}

	writer.Transport = t
}

func LogCallback(msg string) {
	if config.Cfg.Verbosity < 3 {
		return
	}
	fmt.Fprintf(os.Stderr, "%% %s\n", msg)
}

func ErrorCallback(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "%% ERROR: %s\n", err.Error())
}

func TopicName(msg kafka.Message) string {
	return msg.Topic
}

func ErrStr(err error) string {
	if err == nil {
		return "no error"
	}
	return err.Error()
}
