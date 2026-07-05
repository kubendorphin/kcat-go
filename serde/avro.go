package serde

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sync"
)

// Global Schema Registry client (initialized via InitSchemaRegistry)
var globalSRClient *SchemaRegistryClient

// InitSchemaRegistry initializes the global Schema Registry client.
func InitSchemaRegistry(baseURL string) {
	globalSRClient = NewSchemaRegistryClient(baseURL)
}

// AvroToJSON deserializes a schema-id framed Avro message and returns JSON.
// Data format (Confluent Schema Registry):
//
//	1 byte:  0x00 (magic byte)
//	4 bytes: schema ID (big-endian)
//	rest:    Avro binary data
func AvroToJSON(data []byte, schema *json.RawMessage) (string, error) {
	if len(data) < 5 {
		return "", fmt.Errorf("avro: data too short for schema-id framing (got %d bytes, need at least 5)", len(data))
	}
	if data[0] != 0x00 {
		return "", fmt.Errorf("avro: invalid CP1 magic byte 0x%02x (message not produced with Schema-Registry Avro framing)", data[0])
	}
	schemaID := int(binary.BigEndian.Uint32(data[1:5]))
	avroData := data[5:]

	// If we have a pre-loaded schema, use it
	if schema != nil && len(*schema) > 0 {
		return avroDataToJSON(avroData, *schema)
	}

	// Use global SR client to fetch schema
	if globalSRClient == nil {
		return "", fmt.Errorf("avro: schema ID %d: no Schema Registry configured (use -r flag)", schemaID)
	}

	avroSchema, err := globalSRClient.GetSchema(schemaID)
	if err != nil {
		return "", fmt.Errorf("avro: failed to fetch schema ID %d: %w", schemaID, err)
	}

	return avroDataToJSON(avroData, avroSchema.JSON)
}

// AvroSchemaID extracts the schema ID from schema-id framed data.
func AvroSchemaID(data []byte) (int, error) {
	if len(data) < 5 {
		return 0, fmt.Errorf("avro: data too short for schema-id framing")
	}
	if data[0] != 0x00 {
		return 0, fmt.Errorf("avro: invalid magic byte 0x%02x", data[0])
	}
	return int(binary.BigEndian.Uint32(data[1:5])), nil
}

// AvroSchema holds a compiled Avro schema.
type AvroSchema struct {
	ID   int
	JSON json.RawMessage
}

// SchemaRegistryClient handles communication with Confluent Schema Registry.
type SchemaRegistryClient struct {
	baseURL    string
	cache      map[int]*AvroSchema
	cacheMx    sync.RWMutex
	httpClient *http.Client
}

// NewSchemaRegistryClient creates a new Schema Registry client.
func NewSchemaRegistryClient(baseURL string) *SchemaRegistryClient {
	return &SchemaRegistryClient{
		baseURL:    baseURL,
		cache:      make(map[int]*AvroSchema),
		httpClient: &http.Client{},
	}
}

// GetSchema fetches a schema by ID from the Schema Registry.
func (c *SchemaRegistryClient) GetSchema(id int) (*AvroSchema, error) {
	c.cacheMx.RLock()
	if schema, ok := c.cache[id]; ok {
		c.cacheMx.RUnlock()
		return schema, nil
	}
	c.cacheMx.RUnlock()

	// Fixed: correct endpoint is /schemas/ids/{id}, not /subjects/{id}/versions/latest
	url := fmt.Sprintf("%s/schemas/ids/%d", c.baseURL, id)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("schema registry: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("schema registry: HTTP %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("schema registry: failed to read response: %w", err)
	}

	var result struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("schema registry: failed to decode response: %w", err)
	}

	schema := json.RawMessage(result.Schema)
	s := &AvroSchema{ID: id, JSON: schema}

	c.cacheMx.Lock()
	c.cache[id] = s
	c.cacheMx.Unlock()

	return s, nil
}

// GetSchemaByName fetches the latest schema with the given subject name.
func (c *SchemaRegistryClient) GetSchemaByName(subject string) (*AvroSchema, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions/latest", c.baseURL, subject)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("schema registry: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("schema registry: failed to read response: %w", err)
	}

	var result struct {
		ID     int    `json:"id"`
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("schema registry: failed to decode response: %w", err)
	}

	schema := json.RawMessage(result.Schema)
	s := &AvroSchema{ID: result.ID, JSON: schema}

	c.cacheMx.Lock()
	c.cache[result.ID] = s
	c.cacheMx.Unlock()

	return s, nil
}

// LoadSchemaFromFile reads an Avro schema from a .avsc file.
func LoadSchemaFromFile(path string) (*AvroSchema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("avro: failed to read schema file %s: %w", path, err)
	}

	// Remove whitespace/newlines and ensure valid JSON
	data = bytes.TrimSpace(data)
	var schema json.RawMessage
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("avro: failed to parse schema file %s: %w", path, err)
	}

	return &AvroSchema{JSON: schema}, nil
}

// avroDataToJSON converts Avro binary data to JSON using the provided schema.
func avroDataToJSON(data []byte, schemaJSON json.RawMessage) (string, error) {
	// Parse the schema JSON
	var schema interface{}
	if err := json.Unmarshal(schemaJSON, &schema); err != nil {
		return "", fmt.Errorf("avro: failed to parse schema: %w", err)
	}

	// Decode the Avro binary data
	decoder := newAvroDecoder(data)
	value, err := decoder.decode(schema)
	if err != nil {
		return "", fmt.Errorf("avro: decode failed: %w", err)
	}

	// Marshal to JSON
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("avro: json marshal failed: %w", err)
	}

	return string(jsonBytes), nil
}

// avroDecoder decodes Avro binary data according to a parsed schema.
type avroDecoder struct {
	data []byte
	pos  int
}

func newAvroDecoder(data []byte) *avroDecoder {
	return &avroDecoder{data: data}
}

func (d *avroDecoder) readByte() (byte, error) {
	if d.pos >= len(d.data) {
		return 0, fmt.Errorf("unexpected end of data at position %d", d.pos)
	}
	b := d.data[d.pos]
	d.pos++
	return b, nil
}

func (d *avroDecoder) readLong() (int64, error) {
	var n uint64
	shift := uint(0)
	for {
		b, err := d.readByte()
		if err != nil {
			return 0, err
		}
		n |= uint64(b&0x7f) << shift
		shift += 7
		if b&0x80 == 0 {
			break
		}
	}
	// Zigzag decode
	return int64((n >> 1) ^ -(n & 1)), nil
}

func (d *avroDecoder) readBytes() ([]byte, error) {
	n, err := d.readLong()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, fmt.Errorf("negative byte count %d", n)
	}
	if d.pos+int(n) > len(d.data) {
		return nil, fmt.Errorf("not enough data for %d bytes at position %d", n, d.pos)
	}
	b := d.data[d.pos : d.pos+int(n)]
	d.pos += int(n)
	return b, nil
}

// decode decodes an Avro value according to the given schema.
func (d *avroDecoder) decode(schema interface{}) (interface{}, error) {
	switch s := schema.(type) {
	case string:
		return d.decodePrimitive(s)
	case map[string]interface{}:
		t, _ := s["type"].(string)
		switch t {
		case "record":
			return d.decodeRecord(s)
		case "array":
			return d.decodeArray(s)
		case "map":
			return d.decodeMap(s)
		case "enum":
			return d.decodeEnum(s)
		case "fixed":
			return d.decodeFixed(s)
		default:
			return d.decodePrimitive(t)
		}
	case []interface{}:
		return d.decodeUnion(s)
	}
	return nil, fmt.Errorf("unknown schema type %T", schema)
}

func (d *avroDecoder) decodePrimitive(t string) (interface{}, error) {
	switch t {
	case "null":
		return nil, nil
	case "boolean":
		b, err := d.readByte()
		return b != 0, err
	case "int":
		n, err := d.readLong()
		return int32(n), err
	case "long":
		return d.readLong()
	case "float":
		if d.pos+4 > len(d.data) {
			return nil, fmt.Errorf("not enough data for float")
		}
		bits := binary.LittleEndian.Uint32(d.data[d.pos : d.pos+4])
		d.pos += 4
		return math.Float32frombits(bits), nil
	case "double":
		if d.pos+8 > len(d.data) {
			return nil, fmt.Errorf("not enough data for double")
		}
		bits := binary.LittleEndian.Uint64(d.data[d.pos : d.pos+8])
		d.pos += 8
		return math.Float64frombits(bits), nil
	case "bytes":
		return d.readBytes()
	case "string":
		b, err := d.readBytes()
		if err != nil {
			return nil, err
		}
		return string(b), nil
	default:
		return nil, fmt.Errorf("unknown primitive type %q", t)
	}
}

func (d *avroDecoder) decodeRecord(schema map[string]interface{}) (interface{}, error) {
	result := make(map[string]interface{})
	fields, _ := schema["fields"].([]interface{})
	for _, f := range fields {
		field, _ := f.(map[string]interface{})
		name, _ := field["name"].(string)
		fieldSchema := field["type"]
		val, err := d.decode(fieldSchema)
		if err != nil {
			return nil, fmt.Errorf("record field %q: %w", name, err)
		}
		result[name] = val
	}
	return result, nil
}

func (d *avroDecoder) decodeArray(schema map[string]interface{}) (interface{}, error) {
	itemSchema := schema["items"]
	var result []interface{}
	for {
		count, err := d.readLong()
		if err != nil {
			return nil, err
		}
		if count == 0 {
			break
		}
		// Negative count means block size in bytes follows
		if count < 0 {
			count = -count
			if _, err := d.readLong(); err != nil {
				return nil, err
			}
		}
		for i := int64(0); i < count; i++ {
			item, err := d.decode(itemSchema)
			if err != nil {
				return nil, fmt.Errorf("array item: %w", err)
			}
			result = append(result, item)
		}
	}
	return result, nil
}

func (d *avroDecoder) decodeMap(schema map[string]interface{}) (interface{}, error) {
	valueSchema := schema["values"]
	result := make(map[string]interface{})
	for {
		count, err := d.readLong()
		if err != nil {
			return nil, err
		}
		if count == 0 {
			break
		}
		if count < 0 {
			count = -count
			if _, err := d.readLong(); err != nil {
				return nil, err
			}
		}
		for i := int64(0); i < count; i++ {
			keyBytes, err := d.readBytes()
			if err != nil {
				return nil, fmt.Errorf("map key: %w", err)
			}
			val, err := d.decode(valueSchema)
			if err != nil {
				return nil, fmt.Errorf("map value: %w", err)
			}
			result[string(keyBytes)] = val
		}
	}
	return result, nil
}

func (d *avroDecoder) decodeEnum(schema map[string]interface{}) (interface{}, error) {
	idx, err := d.readLong()
	if err != nil {
		return nil, err
	}
	symbols, _ := schema["symbols"].([]interface{})
	if int(idx) >= len(symbols) {
		return nil, fmt.Errorf("enum index %d out of range (%d symbols)", idx, len(symbols))
	}
	return symbols[idx], nil
}

func (d *avroDecoder) decodeFixed(schema map[string]interface{}) (interface{}, error) {
	size, _ := schema["size"].(float64)
	n := int(size)
	if d.pos+n > len(d.data) {
		return nil, fmt.Errorf("not enough data for fixed(%d)", n)
	}
	b := d.data[d.pos : d.pos+n]
	d.pos += n
	return b, nil
}

func (d *avroDecoder) decodeUnion(schemas []interface{}) (interface{}, error) {
	idx, err := d.readLong()
	if err != nil {
		return nil, err
	}
	if int(idx) >= len(schemas) {
		return nil, fmt.Errorf("union index %d out of range (%d branches)", idx, len(schemas))
	}
	return d.decode(schemas[idx])
}
