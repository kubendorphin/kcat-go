package format

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/edenhill/kcat-go/config"
	"github.com/edenhill/kcat-go/serde"
	"github.com/segmentio/kafka-go"
)

type MessagePrinter struct {
	tokens []Token
}

func NewPrinter(fmtStr string) (*MessagePrinter, error) {
	tokens, err := Parse(fmtStr)
	if err != nil {
		return nil, err
	}
	return &MessagePrinter{tokens: tokens}, nil
}

func (p *MessagePrinter) Print(w io.Writer, msg kafka.Message) error {
	for _, tok := range p.tokens {
		switch tok.Type {
		case TypeStr:
			if _, err := io.WriteString(w, tok.Str); err != nil {
				return err
			}

		case TypeOffset:
			if _, err := fmt.Fprintf(w, "%d", msg.Offset); err != nil {
				return err
			}

		case TypeKey:
			if err := printKey(w, msg); err != nil {
				return err
			}

		case TypeKeyLen:
			keyLen := -1
			if msg.Key != nil {
				keyLen = len(msg.Key)
			}
			if _, err := fmt.Fprintf(w, "%d", keyLen); err != nil {
				return err
			}

		case TypePayload:
			if err := printPayload(w, msg); err != nil {
				return err
			}

		case TypePayloadLen:
			payloadLen := -1
			if msg.Value != nil {
				payloadLen = len(msg.Value)
			}
			if _, err := fmt.Fprintf(w, "%d", payloadLen); err != nil {
				return err
			}

		case TypePayloadLenBinary:
			payloadLen := int64(-1)
			if msg.Value != nil {
				payloadLen = int64(len(msg.Value))
			}
			belen := make([]byte, 4)
			binary.BigEndian.PutUint32(belen, uint32(payloadLen))
			if _, err := w.Write(belen); err != nil {
				return err
			}

		case TypeTopic:
			if _, err := io.WriteString(w, msg.Topic); err != nil {
				return err
			}

		case TypePartition:
			if _, err := fmt.Fprintf(w, "%d", msg.Partition); err != nil {
				return err
			}

		case TypeTimestamp:
			ts := msg.Time.UnixMilli()
			if _, err := fmt.Fprintf(w, "%d", ts); err != nil {
				return err
			}

		case TypeHeaders:
			if len(msg.Headers) > 0 {
				var hdrParts []string
				for _, h := range msg.Headers {
					val := "NULL"
					if h.Value != nil {
						val = string(h.Value)
					}
					hdrParts = append(hdrParts, fmt.Sprintf("%s=%s", h.Key, val))
				}
				if _, err := io.WriteString(w, strings.Join(hdrParts, ",")); err != nil {
					return err
				}
			}

		default:
			return fmt.Errorf("format: unknown token type %d", tok.Type)
		}
	}
	return nil
}

func printKey(w io.Writer, msg kafka.Message) error {
	if msg.Key != nil {
		if config.Cfg.Flags&config.AvroKey != 0 {
			jsonStr, err := serde.AvroToJSON(msg.Key, nil)
			if err != nil {
				return fmt.Errorf("format: avro key deserialization: %w", err)
			}
			if _, err := io.WriteString(w, jsonStr); err != nil {
				return err
			}
			return nil
		}
		if config.Cfg.PackKey != "" {
			if err := serde.Unpack(w, "key", config.Cfg.PackKey, msg.Key); err != nil {
				return err
			}
			return nil
		}
		if _, err := w.Write(msg.Key); err != nil {
			return err
		}
	} else if config.Cfg.Flags&config.NullEmpty != 0 {
		if _, err := io.WriteString(w, config.Cfg.NullStr); err != nil {
			return err
		}
	}
	return nil
}

func printPayload(w io.Writer, msg kafka.Message) error {
	if msg.Value != nil {
		if config.Cfg.Flags&config.AvroValue != 0 {
			jsonStr, err := serde.AvroToJSON(msg.Value, nil)
			if err != nil {
				return fmt.Errorf("format: avro value deserialization: %w", err)
			}
			if _, err := io.WriteString(w, jsonStr); err != nil {
				return err
			}
			return nil
		}
		if config.Cfg.PackValue != "" {
			if err := serde.Unpack(w, "value", config.Cfg.PackValue, msg.Value); err != nil {
				return err
			}
			return nil
		}
		if _, err := w.Write(msg.Value); err != nil {
			return err
		}
	} else if config.Cfg.Flags&config.NullEmpty != 0 {
		if _, err := io.WriteString(w, config.Cfg.NullStr); err != nil {
			return err
		}
	}
	return nil
}

type FormatToken = Token

func PackCheck(name, fmtStr string) error {
	valid := " <>bBhHiIqQcs$"
	for _, c := range fmtStr {
		if strings.IndexRune(valid, c) < 0 {
			return fmt.Errorf("invalid token '%c' in %s pack-format", c, name)
		}
	}
	return nil
}

func MustParse(fmtStr string) *MessagePrinter {
	p, err := NewPrinter(fmtStr)
	if err != nil {
		panic(fmt.Sprintf("format: %v", err))
	}
	return p
}
