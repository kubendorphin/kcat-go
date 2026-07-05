package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	ModeProducer      = 'P'
	ModeConsumer      = 'C'
	ModeKafkaConsumer = 'G'
	ModeMetadata      = 'L'
	ModeQuery         = 'Q'
	ModeMock          = 'M'
)

const (
	FmtJSON       = 0x0001
	KeyDelim      = 0x0002
	PrintOffset   = 0x0004
	TeeOutput     = 0x0008
	NullEmpty     = 0x0010
	LineMode      = 0x0020
	APIVerReq     = 0x0040
	APIVerReqUser = 0x0080
	NoConfSearch  = 0x0100
	BrokersSeen   = 0x0200
	AvroKey       = 0x0400
	AvroValue     = 0x0800
	SRURLSeen     = 0x1000
)

const (
	OffsetBeginning int64 = -2
	OffsetEnd       int64 = -1
	OffsetStored    int64 = -1000
	OffsetInvalid   int64 = -1001
)

type Header struct {
	Key   string
	Value []byte
}

type TopicPartition struct {
	Topic     string
	Partition int
	Offset    int64
}

type Config struct {
	Run       bool
	Verbosity int
	ExitCode  int
	ExitOnErr bool

	Mode rune

	Flags int

	Delim      string
	DelimSz    int
	KeyDelim   string
	KeyDelimSz int

	FormatStr string

	PackKey   string
	PackValue string

	MsgSize     int
	Brokers     string
	Topic       string
	Partition   int32
	Headers     []Header
	Group       string
	FixedKey    []byte
	FixedKeyLen int
	Offset      int64
	StartTS     int64
	StopTS      int64
	MsgCount    int64
	ExitEOF     bool
	EOFCount    int

	Assignment []TopicPartition

	MetadataTimeout int
	NullStr         string
	NullStrLen      int
	Transactional   bool

	KafkaConf map[string]string
	TopicConf map[string]string
	Client    interface{}

	Debug string

	TermSignal int

	SRURL string

	MockBrokerCount int

	Tx      uint64
	TxErrQ  uint64
	TxErrDr uint64
	TxDeliv uint64
	Rx      uint64
}

var Cfg Config

func InitDefaults() {
	Cfg = Config{
		Run:             true,
		Verbosity:       1,
		ExitOnErr:       true,
		Partition:       -1000,
		MsgSize:         1024 * 1024,
		NullStr:         "NULL",
		MetadataTimeout: 5,
		KafkaConf:       make(map[string]string),
		TopicConf:       make(map[string]string),
	}
	Cfg.NullStrLen = len(Cfg.NullStr)
}

func SetGlobalProperty(name, val string) error {
	if err := setProperty(Cfg.KafkaConf, name, val); err != nil {
		return err
	}
	if name == "metadata.broker.list" || name == "bootstrap.servers" {
		Cfg.Flags |= BrokersSeen
	}
	if name == "api.version.request" {
		Cfg.Flags |= APIVerReqUser
	}
	return nil
}

func SetTopicProperty(name, val string) error {
	return setProperty(Cfg.TopicConf, name, val)
}

func SetProperty(name, val string) error {
	if err := setProperty(Cfg.TopicConf, name, val); err == nil {
		return nil
	}
	return SetGlobalProperty(name, val)
}

func setProperty(conf map[string]string, name, val string) error {
	if strings.HasPrefix(name, "schema.registry.") {
		return handleSerdesProperty(name, val)
	}
	topicName := name
	if strings.HasPrefix(name, "topic.") {
		topicName = name[6:]
	}
	conf[topicName] = val
	return nil
}

func handleSerdesProperty(name, val string) error {
	switch name {
	case "schema.registry.url":
		url := strings.TrimRight(val, "/")
		if url == "" {
			return fmt.Errorf("schema.registry.url is empty")
		}
		Cfg.SRURL = url
		Cfg.Flags |= SRURLSeen
	}
	return nil
}

func HandleJavaConf(name, val string) (bool, error) {
	switch name {
	case "ssl.endpoint.identification.algorithm":
		return true, nil
	case "sasl.jaas.config":
		re := regexp.MustCompile(`username="([^"]+)"\s+password="([^"]+)"`)
		matches := re.FindStringSubmatch(val)
		if len(matches) == 3 {
			if err := SetGlobalProperty("sasl.username", matches[1]); err != nil {
				return true, err
			}
			if err := SetGlobalProperty("sasl.password", matches[2]); err != nil {
				return true, err
			}
			return true, nil
		}
		return true, fmt.Errorf("failed to parse sasl.jaas.config")
	default:
		return false, nil
	}
}

func FindConfigFile() string {
	home := os.Getenv("HOME")
	if home == "" {
		home = os.Getenv("USERPROFILE")
	}
	if home == "" {
		return ""
	}
	path := filepath.Join(home, ".config", "kcat.conf")
	if _, err := os.Stat(path); err == nil {
		return path
	}
	path = filepath.Join(home, ".config", "kafkacat.conf")
	if _, err := os.Stat(path); err == nil {
		return path
	}
	return ""
}

func EnvConfigFile() string {
	if v := os.Getenv("KCAT_CONFIG"); v != "" {
		return v
	}
	if v := os.Getenv("KAFKACAT_CONFIG"); v != "" {
		fmt.Fprintf(os.Stderr, "%% KAFKACAT_CONFIG is deprecated, use KCAT_CONFIG\n")
		return v
	}
	return ""
}

func init() {
	InitDefaults()
}
