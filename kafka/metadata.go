package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/edenhill/kcat-go/config"
)

func RunMetadata(c *Producer) error {
	topicName := config.Cfg.Topic

	client := &kafka.Client{}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Cfg.MetadataTimeout)*time.Second)
	defer cancel()

	req := &kafka.MetadataRequest{
		Addr: kafka.TCP(strings.Split(config.Cfg.Brokers, ",")[0]),
	}
	if topicName != "" {
		req.Topics = []string{topicName}
	}

	metadata, err := client.Metadata(ctx, req)
	if err != nil {
		return fmt.Errorf("kafka: Failed to acquire metadata: %w", err)
	}

	if config.Cfg.Flags&config.FmtJSON != 0 {
		return metadataPrintJSON(metadata)
	}

	metadataPrint(metadata)
	return nil
}

func metadataPrint(metadata *kafka.MetadataResponse) {
	topicStr := "all topics"
	if config.Cfg.Topic != "" {
		topicStr = config.Cfg.Topic
	}

	if len(metadata.Brokers) == 0 {
		fmt.Printf("Metadata for %s\n", topicStr)
		fmt.Printf(" No brokers available\n")
		return
	}

	fmt.Printf("Metadata for %s (from broker %s):\n",
		topicStr, metadata.Brokers[0].Host)

	fmt.Printf(" %d brokers:\n", len(metadata.Brokers))
	for _, b := range metadata.Brokers {
		fmt.Printf("  broker %d at %s:%d\n", b.ID, b.Host, b.Port)
	}

	fmt.Printf(" %d topics:\n", len(metadata.Topics))
	for _, tMeta := range metadata.Topics {
		errStr := ""
		if tMeta.Error != nil {
			errStr = fmt.Sprintf(" %s", tMeta.Error)
		}
		fmt.Printf("  topic \"%s\" with %d partitions:%s\n",
			tMeta.Name, len(tMeta.Partitions), errStr)

		for _, pMeta := range tMeta.Partitions {
			replicas := formatBrokers(pMeta.Replicas)
			isrs := formatBrokers(pMeta.Isr)
			fmt.Printf("    partition %d, leader %d, replicas: %s, isrs: %s",
				pMeta.ID, pMeta.Leader.ID, replicas, isrs)
			if pMeta.Error != nil {
				fmt.Printf(", %s", pMeta.Error)
			}
			fmt.Println()
		}
	}
}

func metadataPrintJSON(metadata *kafka.MetadataResponse) error {
	type partitionInfo struct {
		Partition int32   `json:"partition"`
		Error     string  `json:"error,omitempty"`
		Leader    int32   `json:"leader"`
		Replicas  []int32 `json:"replicas"`
		ISRs      []int32 `json:"isrs"`
	}
	type topicInfo struct {
		Topic      string                   `json:"topic"`
		Error      string                   `json:"error,omitempty"`
		Partitions map[string]partitionInfo `json:"partitions"`
	}

	topics := make([]topicInfo, 0, len(metadata.Topics))
	for _, tMeta := range metadata.Topics {
		parts := make(map[string]partitionInfo)
		for _, pMeta := range tMeta.Partitions {
			errStr := ""
			if pMeta.Error != nil {
				errStr = pMeta.Error.Error()
			}
			parts[fmt.Sprintf("%d", pMeta.ID)] = partitionInfo{
				Partition: int32(pMeta.ID),
				Error:     errStr,
				Leader:    int32(pMeta.Leader.ID),
				Replicas:  formatBrokerIDs(pMeta.Replicas),
				ISRs:      formatBrokerIDs(pMeta.Isr),
			}
		}

		tInfo := topicInfo{
			Topic:      tMeta.Name,
			Partitions: parts,
		}
		if tMeta.Error != nil {
			tInfo.Error = tMeta.Error.Error()
		}
		topics = append(topics, tInfo)
	}

	output := map[string]interface{}{
		"originating_broker": map[string]interface{}{
			"id":   int32(0),
			"name": metadata.Brokers[0].Host,
		},
		"controllerid": int32(-1),
		"brokers":      metadata.Brokers,
		"topics":       topics,
	}

	data, err := json.Marshal(output)
	if err != nil {
		return fmt.Errorf("json: failed to marshal: %w", err)
	}
	fmt.Fprintln(os.Stdout, string(data))
	return nil
}

func formatBrokers(brokers []kafka.Broker) string {
	ids := make([]string, 0, len(brokers))
	for _, b := range brokers {
		ids = append(ids, fmt.Sprintf("%d", b.ID))
	}
	return strings.Join(ids, ",")
}

func formatBrokerIDs(brokers []kafka.Broker) []int32 {
	ids := make([]int32, 0, len(brokers))
	for _, b := range brokers {
		ids = append(ids, int32(b.ID))
	}
	return ids
}
