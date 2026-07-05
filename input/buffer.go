// Package input provides delimiter-based input buffering.
package input

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

// ParseDelim parses a delimiter string from command line arguments.
// Handles escape sequences: \n, \t, \xNN.
func ParseDelim(instr string) ([]byte, error) {
	s := instr
	result := make([]byte, 0, len(s))

	for len(s) > 0 {
		if s[0] == '\\' && len(s) > 1 {
			s = s[1:]
			switch s[0] {
			case 'n':
				result = append(result, '\n')
				s = s[1:]
			case 't':
				result = append(result, '\t')
				s = s[1:]
			case 'r':
				result = append(result, '\r')
				s = s[1:]
			case 'x':
				// Hex escape: \xNN
				s = s[1:]
				hexStr := ""
				for len(s) > 0 && len(hexStr) < 3 {
					c := s[0]
					if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') {
						hexStr += string(c)
						s = s[1:]
					} else {
						break
					}
				}
				if hexStr == "" {
					return nil, fmt.Errorf("parse_delim: \\x expects hex number")
				}
				var val byte
				for _, c := range hexStr {
					val = val*16 + byte(c)
				}
				// Manual hex parsing
				val = 0
				for _, c := range hexStr {
					val <<= 4
					if c >= '0' && c <= '9' {
						val += byte(c - '0')
					} else if c >= 'a' && c <= 'f' {
						val += byte(c - 'a' + 10)
					} else if c >= 'A' && c <= 'F' {
						val += byte(c - 'A' + 10)
					}
				}
				result = append(result, val&0xff)
			default:
				// Unknown escape, keep as-is
				result = append(result, '\\', s[0])
				s = s[1:]
			}
		} else {
			result = append(result, s[0])
			s = s[1:]
		}
	}

	return result, nil
}

// IndexBM returns the index of the first occurrence of sep in buf,
// using Boyer-Moore inspired matching (search right-to-left).
func IndexBM(buf, sep []byte) int {
	if len(sep) == 0 {
		return 0
	}
	if len(sep) > len(buf) {
		return -1
	}

	last := len(sep) - 1
	for i := 0; i <= len(buf)-len(sep); {
		match := true
		for j := last; j >= 0; j-- {
			if buf[i+j] != sep[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
		i++
	}
	return -1
}

// Index returns the index of the first occurrence of sep in buf.
// Uses bytes.Index for single-byte delimiters (fast built-in),
// or Boyer-Moore for multi-byte.
func Index(buf, sep []byte) int {
	if len(sep) == 1 {
		return bytes.Index(buf, sep)
	}
	return IndexBM(buf, sep)
}

// Buffer is a delimiter-based input buffer.
// It reads from an io.Reader and returns chunks of data between delimiters.
type Buffer struct {
	delim []byte
	maxSz int
	buf   []byte // growable buffer
}

// NewBuffer creates a new input buffer.
// delim is the delimiter to split on, maxSz is the maximum buffer size.
func NewBuffer(delim []byte, maxSz int) *Buffer {
	return &Buffer{
		delim: delim,
		maxSz: maxSz,
		buf:   make([]byte, 0, 4096),
	}
}

// Next reads from r until it finds the delimiter, then returns the data before the delimiter.
// Returns (data, more, eof):
//   - (data, true, false)  — data found, more to read
//   - (data, true, true)   — EOF with remaining data (last chunk)
//   - (nil, false, true)   — EOF with no data
//   - (nil, false, false)  — error
func (b *Buffer) Next(r io.Reader) ([]byte, bool, bool) {
	if b.buf == nil {
		return nil, false, true
	}

	for {
		remaining := b.maxSz - len(b.buf)
		if remaining < 0 {
			return nil, false, false // exceeded max size
		}

		// Ensure capacity
		if cap(b.buf)-len(b.buf) < remaining {
			newCap := cap(b.buf) * 2
			if newCap > b.maxSz {
				newCap = b.maxSz
			}
			if newCap <= len(b.buf) {
				newCap = len(b.buf) + 1
			}
			newBuf := make([]byte, len(b.buf), newCap)
			copy(newBuf, b.buf)
			b.buf = newBuf
		}

		readBuf := b.buf[len(b.buf):cap(b.buf)]
		n, err := r.Read(readBuf)
		if n > 0 {
			b.buf = b.buf[:len(b.buf)+n]
		}

		// Search for delimiter
		idx := Index(b.buf, b.delim)
		if idx >= 0 {
			// Delimiter found: extract left side, keep remainder
			data := make([]byte, idx)
			copy(data, b.buf[:idx])
			b.buf = b.buf[idx+len(b.delim):]
			if n == 0 && err == nil {
				return data, true, false
			}
			return data, true, false
		}

		if err == io.EOF {
			if len(b.buf) == 0 {
				b.buf = nil
				return nil, false, true // EOF with no data
			}
			// Return remaining data as last chunk
			data := b.buf
			b.buf = nil
			return data, true, true
		}
		if err != nil {
			return nil, false, false
		}
	}
}

// FromString converts a C-style delimiter string to []byte.
func FromString(s string) ([]byte, error) {
	return ParseDelim(s)
}

// MustFromString is like FromString but panics on error.
func MustFromString(s string) []byte {
	d, err := ParseDelim(s)
	if err != nil {
		panic(fmt.Sprintf("input: failed to parse delimiter %q: %v", s, err))
	}
	return d
}

// Strnstr finds the first occurrence of needle in haystack,
// returning the offset or -1 if not found.
func Strnstr(haystack, needle []byte) int {
	if len(needle) == 0 {
		return 0
	}
	if len(needle) > len(haystack) {
		return -1
	}
	last := len(needle) - 1
	for i := 0; i <= len(haystack)-len(needle); {
		match := true
		for j := last; j >= 0; j-- {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
		i++
	}
	return -1
}

func init() {
	// Register common delimiters
	_ = MustFromString("\n")
	_ = MustFromString("\t")
}

// Quick test helper
var _ = strings.TrimSpace
