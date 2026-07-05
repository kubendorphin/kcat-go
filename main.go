package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/segmentio/kafka-go"

	"github.com/edenhill/kcat-go/cmd"
	"github.com/edenhill/kcat-go/config"
	"github.com/edenhill/kcat-go/format"
	"github.com/edenhill/kcat-go/input"
	kafkaPkg "github.com/edenhill/kcat-go/kafka"
	"github.com/edenhill/kcat-go/serde"
	util "github.com/edenhill/kcat-go/util"
)

var version string

func main() {
	args := os.Args[1:]
	if len(args) > 0 && (args[0] == "kcat" || args[0] == "kafkacat") {
		args = args[1:]
	}

	config.InitDefaults()

	groupID := flag.String("G", "", "Consumer group ID")
	mockCnt := flag.Int("M", 0, "Mock cluster broker count")
	topic := flag.String("t", "", "Topic name")
	partition := flag.Int("p", -1000, "Partition")
	brokers := flag.String("b", "", "Bootstrap brokers")
	compression := flag.String("z", "", "Compression codec")
	offset := flag.String("o", "beginning", "Offset to start from")
	exitEOF := flag.Bool("e", false, "Exit on EOF")
	noExitOnError := flag.Bool("E", false, "Don't exit on error")
	delim := flag.String("D", "\n", "Message delimiter")
	keyDelim := flag.String("K", "", "Key delimiter")
	fixedKey := flag.String("k", "", "Fixed key for all messages")
	teeOutput := flag.Bool("T", false, "Tee output to stdout")
	msgCount := flag.Int64("c", 0, "Limit message count")
	metaTimeout := flag.Int("m", 5, "Metadata timeout in seconds")
	nullStr := flag.Bool("Z", false, "Treat empty as NULL")
	printOffset := flag.Bool("O", false, "Print offset")
	lineMode := flag.Bool("l", false, "Line mode from file")
	formatStr := flag.String("f", "", "Format string")
	serdes := flag.String("s", "", "Serializer/deserializer (pack-format or avro)")
	srURL := flag.String("r", "", "Schema registry URL")
	jsonOutput := flag.Bool("J", false, "JSON output")
	quiet := flag.Bool("q", false, "Quiet mode")
	verbose := flag.Bool("v", false, "Verbose")
	debug := flag.String("d", "", "Debug contexts")
	configFile := flag.String("F", "", "Config file")
	versionFlag := flag.Bool("V", false, "Print version")
	helpFlag := flag.Bool("h", false, "Print help")
	unitTest := flag.Bool("U", false, "Run unit tests")

	var xArgs []string
	extraArgs := []string{}
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "-X" && i+1 < len(os.Args) {
			xArgs = append(xArgs, os.Args[i+1])
			i++
		} else if strings.HasPrefix(os.Args[i], "-X") {
			xArgs = append(xArgs, os.Args[i][2:])
		} else if os.Args[i] != "-h" && os.Args[i] != "-V" && os.Args[i] != "-U" &&
			os.Args[i] != "-P" && os.Args[i] != "-C" && os.Args[i] != "-L" &&
			os.Args[i] != "-Q" && os.Args[i] != "-M" && os.Args[i] != "-G" &&
			os.Args[i] != "-b" && os.Args[i] != "-t" && os.Args[i] != "-p" &&
			os.Args[i] != "-o" && os.Args[i] != "-D" && os.Args[i] != "-K" &&
			os.Args[i] != "-k" && os.Args[i] != "-c" && os.Args[i] != "-m" &&
			os.Args[i] != "-f" && os.Args[i] != "-s" && os.Args[i] != "-r" &&
			os.Args[i] != "-F" && os.Args[i] != "-z" && os.Args[i] != "-d" {
			extraArgs = append(extraArgs, os.Args[i])
		}
	}

	flag.CommandLine.Parse(xArgs)

	if hasModeFlag("-C") {
		config.Cfg.Mode = config.ModeConsumer
	}
	if *groupID != "" {
		config.Cfg.Mode = config.ModeKafkaConsumer
		config.Cfg.Group = *groupID
		_ = config.SetGlobalProperty("group.id", *groupID)
	}
	if hasModeFlag("-L") {
		config.Cfg.Mode = config.ModeMetadata
	}
	if hasModeFlag("-Q") {
		config.Cfg.Mode = config.ModeQuery
	}
	if hasModeFlag("-M") || *mockCnt > 0 {
		config.Cfg.Mode = config.ModeMock
		config.Cfg.MockBrokerCount = *mockCnt
	}
	if *verbose {
		config.Cfg.Verbosity++
	}
	if *quiet {
		config.Cfg.Verbosity = 0
	}
	if *noExitOnError {
		config.Cfg.ExitOnErr = false
	}

	if *brokers != "" {
		config.Cfg.Brokers = *brokers
		_ = config.SetGlobalProperty("bootstrap.servers", *brokers)
		config.Cfg.Flags |= config.BrokersSeen
	}
	if *topic != "" {
		config.Cfg.Topic = *topic
	}
	if *partition != -1000 {
		config.Cfg.Partition = int32(*partition)
	}
	if *compression != "" {
		_ = config.SetGlobalProperty("compression.codec", *compression)
	}
	if *metaTimeout != 5 {
		config.Cfg.MetadataTimeout = *metaTimeout
	}
	if *debug != "" {
		config.Cfg.Debug = *debug
		_ = config.SetGlobalProperty("debug", *debug)
	}

	if d, err := input.ParseDelim(*delim); err == nil {
		config.Cfg.Delim = string(d)
		config.Cfg.DelimSz = len(d)
	}
	if *keyDelim != "" {
		config.Cfg.Flags |= config.KeyDelim
		if d, err := input.ParseDelim(*keyDelim); err == nil {
			config.Cfg.KeyDelim = string(d)
			config.Cfg.KeyDelimSz = len(d)
		}
	}

	if *serdes != "" {
		field := -1
		t := *serdes
		if strings.HasPrefix(t, "key=") {
			t = t[4:]
			field = 0
		} else if strings.HasPrefix(t, "value=") {
			t = t[6:]
			field = 1
		}

		if t == "avro" {
			if field == -1 || field == 0 {
				config.Cfg.Flags |= config.AvroKey
			}
			if field == -1 || field == 1 {
				config.Cfg.Flags |= config.AvroValue
			}
		} else {
			serde.PackCheck("serdes", t)
			if field == -1 || field == 0 {
				config.Cfg.PackKey = t
			}
			if field == -1 || field == 1 {
				config.Cfg.PackValue = t
			}
		}
	}

	if *srURL != "" {
		config.Cfg.Flags |= config.SRURLSeen
		config.Cfg.SRURL = *srURL
		_ = config.SetGlobalProperty("schema.registry.url", *srURL)
		serde.InitSchemaRegistry(*srURL)
	}

	if *jsonOutput {
		config.Cfg.Flags |= config.FmtJSON
	}

	if *nullStr {
		config.Cfg.Flags |= config.NullEmpty
	}
	if *teeOutput {
		config.Cfg.Flags |= config.TeeOutput
	}
	if *lineMode {
		config.Cfg.Flags |= config.LineMode
	}
	if *printOffset {
		config.Cfg.Flags |= config.PrintOffset
	}
	if *exitEOF {
		config.Cfg.ExitEOF = true
	}

	if *offset != "" {
		switch *offset {
		case "end":
			config.Cfg.Offset = config.OffsetEnd
		case "beginning":
			config.Cfg.Offset = config.OffsetBeginning
		case "stored":
			config.Cfg.Offset = config.OffsetStored
		default:
			if strings.HasPrefix(*offset, "s@") {
				config.Cfg.StartTS = parseInt64((*offset)[2:])
				config.Cfg.Flags |= config.APIVerReq
			} else if strings.HasPrefix(*offset, "e@") {
				config.Cfg.StopTS = parseInt64((*offset)[2:])
				config.Cfg.Flags |= config.APIVerReq
			} else {
				config.Cfg.Offset = parseInt64(*offset)
				if config.Cfg.Offset < 0 {
					config.Cfg.Offset = -config.Cfg.Offset
				}
			}
		}
	}

	if *msgCount != 0 {
		config.Cfg.MsgCount = *msgCount
	}

	if *fixedKey != "" {
		config.Cfg.FixedKey = []byte(*fixedKey)
		config.Cfg.FixedKeyLen = len(*fixedKey)
	}

	if *configFile != "" && *configFile != "-" && *configFile != "none" {
		if err := config.ReadConfFile(*configFile); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n\n", err)
			cmd.PrintUsage(1)
		}
	}

	if *unitTest {
		if runTests() {
			os.Exit(1)
		}
		os.Exit(0)
	}

	if *versionFlag {
		cmd.Version = version
		cmd.PrintVersion()
	}

	if *helpFlag {
		cmd.PrintUsage(0)
	}

	if config.Cfg.Flags&config.BrokersSeen == 0 && config.Cfg.Mode != config.ModeMock {
		cmd.PrintUsage(1)
	}

	if config.Cfg.Mode == config.ModeQuery && config.Cfg.Topic == "" {
		cmd.PrintUsage(1)
	}

	if config.Cfg.Mode == 0 {
		if len(extraArgs) > 0 {
			config.Cfg.Mode = config.ModeProducer
		} else {
			config.Cfg.Mode = config.ModeConsumer
			fmt.Fprintf(os.Stderr, "Auto-selecting Consumer mode (use -P or -C to override)\n")
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		config.Cfg.Run = false
	}()

	switch config.Cfg.Mode {
	case config.ModeConsumer:
		runConsumerMode(*formatStr, extraArgs)
	case config.ModeKafkaConsumer:
		runGroupMode(extraArgs)
	case config.ModeProducer:
		runProducerMode(*formatStr, extraArgs)
	case config.ModeMetadata:
		runMetadataMode()
	case config.ModeQuery:
		runQueryMode(extraArgs)
	case config.ModeMock:
		runMockMode()
	default:
		cmd.PrintUsage(0)
	}

	os.Exit(config.Cfg.ExitCode)
}

func hasModeFlag(flagName string) bool {
	for _, arg := range os.Args[1:] {
		if arg == flagName {
			return true
		}
	}
	return false
}

func runConsumerMode(fmtStr string, extraArgs []string) {
	consumer, err := kafkaPkg.NewConsumer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer consumer.Close()

	if config.Cfg.Topic == "" {
		fmt.Fprintf(os.Stderr, "Error: -t <topic> required\n")
		os.Exit(1)
	}

	var printer *format.MessagePrinter
	if fmtStr != "" {
		printer = format.MustParse(fmtStr)
	}

	if err := kafkaPkg.RunConsumer(consumer, printer); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runGroupMode(topics []string) {
	if len(topics) == 0 {
		fmt.Fprintf(os.Stderr, "Error: at least one topic required for -G mode\n")
		os.Exit(1)
	}

	consumer, err := kafkaPkg.NewConsumerGroup()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer consumer.Close()

	if config.Cfg.StopTS != 0 || config.Cfg.StartTS != 0 {
		fmt.Fprintf(os.Stderr, "Error: -o ..@ timestamps can't be used with -G mode\n")
		os.Exit(1)
	}

	var printer *format.MessagePrinter
	if config.Cfg.Flags&config.FmtJSON != 0 {
		printer = format.MustParse("\n")
	} else if config.Cfg.KeyDelim != "" {
		printer = format.MustParse("%k" + config.Cfg.KeyDelim + "%s\n")
	} else {
		printer = format.MustParse("%s\n")
	}

	if err := kafkaPkg.RunConsumerGroup(consumer, topics, printer); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runProducerMode(fmtStr string, extraArgs []string) {
	producer, err := kafkaPkg.NewProducer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer producer.Close()

	drChan := make(chan kafka.Message, 100)

	if v, ok := config.Cfg.KafkaConf["transactional.id"]; ok && v != "" {
		fmt.Fprintf(os.Stderr, "%% Using transactional producer\n")
	}

	if v, ok := config.Cfg.KafkaConf["message.max.bytes"]; ok && v != "" {
		_ = v
	}

	_ = fmtStr

	if err := kafkaPkg.RunProducer(producer, extraArgs, drChan); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	producer.Close()
}

func runMetadataMode() {
	producer, err := kafkaPkg.NewProducer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer producer.Close()

	if err := kafkaPkg.RunMetadata(producer); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runQueryMode(extraArgs []string) {
	if len(extraArgs) == 0 {
		fmt.Fprintf(os.Stderr, "Error: -Q requires topic:partition:timestamp arguments\n")
		fmt.Fprintf(os.Stderr, "Usage: kcat -Q -b <broker> <topic>:<partition>:<timestamp_ms> ...\n")
		os.Exit(1)
	}

	producer, err := kafkaPkg.NewProducer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer producer.Close()

	toppars := make([]kafkaPkg.TopicPartition, 0, len(extraArgs))
	for _, arg := range extraArgs {
		tp, err := kafkaPkg.ParseToppar(arg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid toppar %q: %v\n", arg, err)
			os.Exit(1)
		}
		toppars = append(toppars, tp)
	}

	if err := kafkaPkg.RunQuery(producer, toppars); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runMockMode() {
	if err := kafkaPkg.RunMock(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runTests() bool {
	fails := 0

	strnstrTests := []struct {
		sep    string
		hay    string
		offset int
	}{
		{"Sep;", "Sep;Post", 0},
		{"Sep;", "Sep;", 0},
		{"Sep;", "PreSep;Post", 3},
		{"Sep;", "PreSepPost", -1},
		{"KeyDel;", "Key1KeyDel;Value1", 4},
		{";", "Is The;", 6},
	}

	for _, tt := range strnstrTests {
		got := util.Strnstr([]byte(tt.hay), []byte(tt.sep))
		if tt.offset == -1 {
			if got != -1 {
				fmt.Fprintf(os.Stderr, "strnstr FAILED: expected no match for %q in %q\n", tt.sep, tt.hay)
				fails++
			}
		} else if got != tt.offset {
			fmt.Fprintf(os.Stderr, "strnstr FAILED: for %q in %q: got offset %d, want %d\n",
				tt.sep, tt.hay, got, tt.offset)
			fails++
		}
	}

	delimTests := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"\\n", "\n"},
		{"\\t\\n\\n", "\t\n\n"},
		{"\\x54!\\x45\\x53T", "T!EST"},
		{"\\x30", "0"},
	}

	for _, tt := range delimTests {
		got, err := input.ParseDelim(tt.in)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse_delim FAILED: %v\n", err)
			fails++
			continue
		}
		if string(got) != tt.want {
			fmt.Fprintf(os.Stderr, "parse_delim FAILED: expected %q to return %q, not %q\n",
				tt.in, tt.want, string(got))
			fails++
		}
	}

	return fails > 0
}

func parseInt64(s string) int64 {
	var v int64
	fmt.Sscanf(s, "%d", &v)
	return v
}
