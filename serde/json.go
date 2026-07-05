// Package serde provides serialization/deserialization support for Kafka messages.
package serde

import (
	"encoding/json"
)

// JSONEnvelope is the JSON output format for consumed messages.
type JSONEnvelope struct {
	Topic         string                 `json:"topic"`
	Partition     int32                  `json:"partition"`
	Offset        int64                  `json:"offset"`
	TimestampType string                 `json:"tstype,omitempty"`
	Timestamp     int64                  `json:"ts,omitempty"`
	Broker        int32                  `json:"broker"`
	Headers       []json.RawMessage      `json:"headers,omitempty"`
	Key           json.RawMessage        `json:"key"`
	Payload       json.RawMessage        `json:"payload"`
	KeyError      string                 `json:"key_error,omitempty"`
	PayloadError  string                 `json:"payload_error,omitempty"`
	KeySchemaID   int                    `json:"key_schema_id,omitempty"`
	ValueSchemaID int                    `json:"value_schema_id,omitempty"`
}

// MarshalEnvelope marshals a JSON envelope to bytes.
func MarshalEnvelope(env JSONEnvelope) ([]byte, error) {
	return json.Marshal(env)
}

// MetadataJSON is the JSON output format for metadata listing.
type MetadataJSON struct {
	OriginatingBroker struct {
		ID   int32  `json:"id"`
		Name string `json:"name"`
	} `json:"originating_broker"`
	ControllerID int32    `json:"controllerid"`
	Brokers      []BrokerJSON `json:"brokers"`
	Topics       []TopicJSON  `json:"topics"`
}

// BrokerJSON represents a broker in metadata JSON.
type BrokerJSON struct {
	ID   int32  `json:"id"`
	Name string `json:"name"`
}

// TopicJSON represents a topic in metadata JSON.
type TopicJSON struct {
	Topic      string          `json:"topic"`
	Error      string          `json:"error,omitempty"`
	Partitions []PartitionJSON `json:"partitions"`
}

// PartitionJSON represents a partition in metadata JSON.
type PartitionJSON struct {
	Partition int32    `json:"partition"`
	Error     string   `json:"error,omitempty"`
	Leader    int32    `json:"leader"`
	Replicas  []int32  `json:"replicas"`
	ISRs      []int32  `json:"isrs"`
}

// MarshalMetadataJSON marshals metadata to JSON bytes.
func MarshalMetadataJSON(meta MetadataJSON) ([]byte, error) {
	return json.Marshal(meta)
}

// NullValue represents a NULL key or payload value in JSON.
var NullValue = json.RawMessage("null")

// EncodeJSONKey encodes a key as JSON.
func EncodeJSONKey(key []byte) json.RawMessage {
	if key == nil {
		return NullValue
	}
	return json.RawMessage(key)
}

// EncodeJSONPayload encodes a payload as JSON.
func EncodeJSONPayload(value []byte, avro bool, schema *json.RawMessage, client *SchemaRegistryClient) (json.RawMessage, string, int) {
	if value == nil {
		return NullValue, "", 0
	}
	if avro {
		jsonStr, err := AvroToJSON(value, schema)
		if err != nil {
			if client != nil {
				// Try to fetch schema
				sid, _ := AvroSchemaID(value)
				s, _ := client.GetSchema(sid)
				if s != nil {
					jsonStr, err = AvroToJSON(value, &s.JSON)
				}
			}
			if err != nil {
				return NullValue, err.Error(), 0
			}
		}
		sid, _ := AvroSchemaID(value)
		return json.RawMessage(jsonStr), "", sid
	}
	return json.RawMessage(value), "", 0
}
