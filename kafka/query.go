package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/edenhill/kcat-go/config"
)

type TopicPartition struct {
	Topic     string
	Partition int
	Offset    int64
}

type PartitionOffset struct {
	Topic     string
	Partition int
	Offset    int64
}

func RunQuery(c *Producer, toppars []TopicPartition) error {
	brokers := strings.Split(config.Cfg.Brokers, ",")
	if len(brokers) == 0 || brokers[0] == "" {
		return fmt.Errorf("kafka: no bootstrap brokers specified")
	}

	client := &kafka.Client{}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Cfg.MetadataTimeout)*time.Second)
	defer cancel()

	topics := make(map[string][]kafka.OffsetRequest)
	for _, tp := range toppars {
		if _, ok := topics[tp.Topic]; !ok {
			topics[tp.Topic] = make([]kafka.OffsetRequest, 0)
		}
		topics[tp.Topic] = append(topics[tp.Topic], kafka.OffsetRequest{
			Partition: tp.Partition,
			Timestamp: tp.Offset,
		})
	}

	req := &kafka.ListOffsetsRequest{
		Addr:   kafka.TCP(brokers[0]),
		Topics: topics,
	}

	resp, err := client.ListOffsets(ctx, req)
	if err != nil {
		return fmt.Errorf("kafka: failed to list offsets: %w", err)
	}

	results := make([]PartitionOffset, 0, len(toppars))
	for _, tp := range toppars {
		offset := int64(-1)
		if partitions, ok := resp.Topics[tp.Topic]; ok {
			for _, p := range partitions {
				if p.Partition == tp.Partition {
					if p.Error != nil {
						offset = -1
					} else if p.FirstOffset != -1 {
						offset = p.FirstOffset
					} else if p.LastOffset != -1 {
						offset = p.LastOffset
					} else {
						for o := range p.Offsets {
							offset = o
							break
						}
					}
					break
				}
			}
		}
		results = append(results, PartitionOffset{
			Topic:     tp.Topic,
			Partition: tp.Partition,
			Offset:    offset,
		})
	}

	printPartitionList(results)
	return nil
}

func ParseToppar(s string) (TopicPartition, error) {
	parts := splitN(s, ':', 3)
	if len(parts) < 3 {
		return TopicPartition{}, fmt.Errorf("expected topic:partition:offset_or_timestamp")
	}

	topic := parts[0]
	partition := parseInt32(parts[1])
	offset := parseInt64(parts[2])

	tp := TopicPartition{Topic: topic, Partition: int(partition)}
	if offset >= 0 {
		tp.Offset = offset
	} else {
		tp.Offset = config.OffsetBeginning
	}

	return tp, nil
}

func splitN(s string, sep rune, n int) []string {
	result := make([]string, 0, n)
	start := 0
	count := 0
	for i, r := range s {
		if count >= n-1 {
			break
		}
		if r == sep {
			result = append(result, s[start:i])
			start = i + 1
			count++
		}
	}
	result = append(result, s[start:])
	return result
}

func parseInt32(s string) int32 {
	var v int64
	fmt.Sscanf(s, "%d", &v)
	return int32(v)
}

func parseInt64(s string) int64 {
	var v int64
	fmt.Sscanf(s, "%d", &v)
	return v
}

func printPartitionList(parts []PartitionOffset) {
	for _, p := range parts {
		fmt.Printf("%s [%d] offset %d\n",
			p.Topic, p.Partition, p.Offset)
	}
}
