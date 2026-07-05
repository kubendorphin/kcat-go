package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// ReadConfFile reads a property=value configuration file.
// Lines starting with # are comments. Property=value is the format.
// Supports Java config escapes: \: and \=.
func ReadConfFile(path string) error {
	fp, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fp.Close()

	scanner := bufio.NewScanner(fp)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Left-trim
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}

		// Handle Java config escapes: \: and \=
		line = strings.ReplaceAll(line, "\\:", "\x00COLON\x00")
		line = strings.ReplaceAll(line, "\\=", "\x00EQUALS\x00")

		// Parse prop=value
		idx := strings.IndexByte(line, '=')
		if idx <= 0 {
			return fmt.Errorf("%s:%d: expected property=value", path, lineNum)
		}

		name := line[:idx]
		val := line[idx+1:]

		// Restore escapes
		name = strings.ReplaceAll(name, "\x00COLON\x00", ":")
		name = strings.ReplaceAll(name, "\x00EQUALS\x00", "=")
		val = strings.ReplaceAll(val, "\x00COLON\x00", ":")
		val = strings.ReplaceAll(val, "\x00EQUALS\x00", "=")

		// Handle Java config conversions
		if handled, err := HandleJavaConf(name, val); handled {
			if err != nil {
				return fmt.Errorf("%s:%d: %s (java config conversion)", path, lineNum, err)
			}
			continue
		}

		// Split topic. prefix properties
		if strings.HasPrefix(name, "topic.") {
			if err := SetTopicProperty(name, val); err != nil {
				return fmt.Errorf("%s:%d: %s", path, lineNum, err)
			}
		} else {
			if err := SetGlobalProperty(name, val); err != nil {
				return fmt.Errorf("%s:%d: %s", path, lineNum, err)
			}
		}
	}
	return scanner.Err()
}

// ReadConfigFiles reads all configured config files in priority order.
// Returns the first fatal error encountered.
func ReadConfigFiles(fatalOnErr bool) error {
	// Check environment variable first
	if envPath := EnvConfigFile(); envPath != "" {
		if err := ReadConfFile(envPath); err != nil {
			if fatalOnErr {
				return fmt.Errorf("config: failed to read %s: %w", envPath, err)
			}
			return err
		}
		return nil
	}

	// Search default config file
	if path := FindConfigFile(); path != "" {
		if err := ReadConfFile(path); err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		}
	}

	return nil
}
