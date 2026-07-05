// Package cmd provides CLI argument setup.
package cmd

import (
	"fmt"
	"os"
	"runtime"

	"github.com/edenhill/kcat-go/config"
)

// Version is set at build time via ldflags.
var Version = "dev"

// PrintUsage prints the help/usage message.
func PrintUsage(exitCode int) {
	out := os.Stdout
	if exitCode != 0 {
		out = os.Stderr
	}

	fmt.Fprintf(out, "Usage: %s <options> [file1 file2 .. | topic1 topic2 ..]]\n", os.Args[0])
	fmt.Fprintf(out, "\n")
	fmt.Fprintf(out, "kcat - Apache Kafka producer and consumer tool\n")
	fmt.Fprintf(out, "https://github.com/edenhill/kcat\n")
	fmt.Fprintf(out, "Copyright (c) 2014-2021, Magnus Edenhill\n")
	fmt.Fprintf(out, "Version %s (Go %s)\n", Version, runtime.Version())
	fmt.Fprintf(out, "\n")
	fmt.Fprintf(out, "Mode:\n")
	fmt.Fprintf(out, "  -P                 Producer\n")
	fmt.Fprintf(out, "  -C                 Consumer\n")
	fmt.Fprintf(out, "  -G <group-id>      High-level KafkaConsumer (Kafka >=0.9 balanced consumer groups)\n")
	fmt.Fprintf(out, "  -L                 Metadata List\n")
	fmt.Fprintf(out, "  -Q                 Query mode\n")
	fmt.Fprintf(out, "  -M <broker-cnt>    Start Mock cluster\n")
	fmt.Fprintf(out, "\n")
	fmt.Fprintf(out, "General options for most modes:\n")
	fmt.Fprintf(out, "  -t <topic>         Topic to consume from, produce to, or list\n")
	fmt.Fprintf(out, "  -p <partition>     Partition\n")
	fmt.Fprintf(out, "  -b <brokers,..>    Bootstrap broker(s) (host[:port])\n")
	fmt.Fprintf(out, "  -D <delim>         Message delimiter string:\n")
	fmt.Fprintf(out, "                     a-z | \\r | \\n | \\t | \\xNN\n")
	fmt.Fprintf(out, "                     Default: \\n\n")
	fmt.Fprintf(out, "  -K <delim>         Key delimiter (same format as -D)\n")
	fmt.Fprintf(out, "  -c <cnt>           Limit message count\n")
	fmt.Fprintf(out, "  -m <seconds>       Metadata (et.al.) request timeout. Default: 5 seconds.\n")
	fmt.Fprintf(out, "  -F <config-file>   Read configuration properties from file\n")
	fmt.Fprintf(out, "  -X list            List available librdkafka configuration properties\n")
	fmt.Fprintf(out, "  -X prop=val        Set librdkafka configuration property.\n")
	fmt.Fprintf(out, "                     Properties prefixed with \"topic.\" are applied as topic properties.\n")
	fmt.Fprintf(out, "  -X dump            Dump configuration and exit.\n")
	fmt.Fprintf(out, "  -d <dbg1,...>      Enable librdkafka debugging\n")
	fmt.Fprintf(out, "  -q                 Be quiet (verbosity set to 0)\n")
	fmt.Fprintf(out, "  -v                 Increase verbosity\n")
	fmt.Fprintf(out, "  -E                 Do not exit on non-fatal error\n")
	fmt.Fprintf(out, "  -V                 Print version\n")
	fmt.Fprintf(out, "  -h                 Print usage help\n")
	fmt.Fprintf(out, "\n")
	fmt.Fprintf(out, "Producer options:\n")
	fmt.Fprintf(out, "  -z snappy|gzip|lz4 Message compression. Default: none\n")
	fmt.Fprintf(out, "  -p -1              Use random partitioner\n")
	fmt.Fprintf(out, "  -D <delim>         Delimiter to split input into messages\n")
	fmt.Fprintf(out, "  -K <delim>         Delimiter to split input key and message\n")
	fmt.Fprintf(out, "  -k <str>           Use a fixed key for all messages\n")
	fmt.Fprintf(out, "  -H <header=value>  Add Message Headers\n")
	fmt.Fprintf(out, "  -l                 Send messages from a file separated by delimiter\n")
	fmt.Fprintf(out, "  -T                 Output sent messages to stdout (tee)\n")
	fmt.Fprintf(out, "  -c <cnt>           Exit after producing this many messages\n")
	fmt.Fprintf(out, "  -Z                 Send empty messages as NULL messages\n")
	fmt.Fprintf(out, "  file1 file2..      Read messages from files\n")
	fmt.Fprintf(out, "  -X transactional.id=.. Enable transactions\n")
	fmt.Fprintf(out, "\n")
	fmt.Fprintf(out, "Consumer options:\n")
	fmt.Fprintf(out, "  -o <offset>        Offset to start consuming from:\n")
	fmt.Fprintf(out, "                     beginning | end | stored | <value> | -<value> (tail)\n")
	fmt.Fprintf(out, "  -e                 Exit successfully when last message received\n")
	fmt.Fprintf(out, "  -f <fmt..>         Output formatting string, see below\n")
	fmt.Fprintf(out, "  -J                 Output with JSON envelope\n")
	fmt.Fprintf(out, "  -s key=<serdes>    Deserialize non-NULL keys\n")
	fmt.Fprintf(out, "  -s value=<serdes>  Deserialize non-NULL values\n")
	fmt.Fprintf(out, "  -s <serdes>        Deserialize both keys and values\n")
	fmt.Fprintf(out, "  -D <delim>         Delimiter to separate messages on output\n")
	fmt.Fprintf(out, "  -K <delim>         Print message keys prefixing the message\n")
	fmt.Fprintf(out, "  -O                 Print message offset\n")
	fmt.Fprintf(out, "  -c <cnt>           Exit after consuming this many messages\n")
	fmt.Fprintf(out, "  -Z                 Print NULL values and keys as \"%s\"\n", config.Cfg.NullStr)
	fmt.Fprintf(out, "  -u                 Unbuffered output\n")
	fmt.Fprintf(out, "\n")
	fmt.Fprintf(out, "Format string tokens:\n")
	fmt.Fprintf(out, "  %%s                 Message payload\n")
	fmt.Fprintf(out, "  %%S                 Message payload length (or -1 for NULL)\n")
	fmt.Fprintf(out, "  %%k                 Message key\n")
	fmt.Fprintf(out, "  %%K                 Message key length (or -1 for NULL)\n")
	fmt.Fprintf(out, "  %%t                 Topic\n")
	fmt.Fprintf(out, "  %%p                 Partition\n")
	fmt.Fprintf(out, "  %%o                 Message offset\n")
	fmt.Fprintf(out, "  %%T                 Message timestamp\n")
	fmt.Fprintf(out, "  %%h                 Message headers (n=v CSV)\n")
	fmt.Fprintf(out, "\n")
	fmt.Fprintf(out, "Examples:\n")
	fmt.Fprintf(out, "  kcat -b <broker> -t <topic> -p <partition>\n")
	fmt.Fprintf(out, "  kcat -b <broker> -G <group-id> topic1 topic2\n")
	fmt.Fprintf(out, "  kcat -L -b <broker> [-t <topic>]\n")
	fmt.Fprintf(out, "  kcat -Q -b <broker> -t <topic>:<partition>:<timestamp>\n")
	fmt.Fprintf(out, "\n")

	os.Exit(exitCode)
}

// PrintVersion prints version and exits.
func PrintVersion() {
	fmt.Printf("kcat-go %s (%s)\n", Version, runtime.Version())
	os.Exit(0)
}
