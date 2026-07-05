package serde

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

// endianType represents byte order.
type endianType int

const (
	endianBig endianType = iota
	endianLittle
)

// Unpack deserializes binary data using a pack-format string (Python struct-like).
// Supports: < (little-endian), > (big-endian), b (int8), B (uint8),
// h (int16), H (uint16), i (int32), I (uint32), q (int64), Q (uint64),
// c (char), s (rest-of-data string), $ (end-of-input), ' ' (space).
func Unpack(w io.Writer, what string, fmtStr string, data []byte) error {
	f := 0
	var eo endianType
	for f < len(fmtStr) {
		ch := fmtStr[f]
		switch ch {
		case ' ':
			if _, err := io.WriteString(w, " "); err != nil {
				return err
			}

		case '<':
			eo = endianLittle
		case '>':
			eo = endianBig

		case 'b':
			if f+1 >= len(data) {
				return fmt.Errorf("serde: %s truncated, expected 1 bytes to unpack %c but only %d bytes remaining", what, ch, len(data))
			}
			v := int8(data[f+1])
			if _, err := fmt.Fprintf(w, "%d", v); err != nil {
				return err
			}
			f += 2

		case 'B':
			if f+1 >= len(data) {
				return fmt.Errorf("serde: %s truncated, expected 1 bytes to unpack %c but only %d bytes remaining", what, ch, len(data))
			}
			v := uint8(data[f+1])
			if _, err := fmt.Fprintf(w, "%d", v); err != nil {
				return err
			}
			f += 2

		case 'h':
			if f+2 >= len(data) {
				return fmt.Errorf("serde: %s truncated, expected 2 bytes to unpack %c but only %d bytes remaining", what, ch, len(data))
			}
			var v int16
			b := data[f+1 : f+3]
			if eo == endianBig {
				v = int16(binary.BigEndian.Uint16(b))
			} else {
				v = int16(binary.LittleEndian.Uint16(b))
			}
			if _, err := fmt.Fprintf(w, "%d", v); err != nil {
				return err
			}
			f += 3

		case 'H':
			if f+2 >= len(data) {
				return fmt.Errorf("serde: %s truncated, expected 2 bytes to unpack %c but only %d bytes remaining", what, ch, len(data))
			}
			var v uint16
			b := data[f+1 : f+3]
			if eo == endianBig {
				v = binary.BigEndian.Uint16(b)
			} else {
				v = binary.LittleEndian.Uint16(b)
			}
			if _, err := fmt.Fprintf(w, "%d", v); err != nil {
				return err
			}
			f += 3

		case 'i':
			if f+3 >= len(data) {
				return fmt.Errorf("serde: %s truncated, expected 4 bytes to unpack %c but only %d bytes remaining", what, ch, len(data))
			}
			var v int32
			b := data[f+1 : f+5]
			if eo == endianBig {
				v = int32(binary.BigEndian.Uint32(b))
			} else {
				v = int32(binary.LittleEndian.Uint32(b))
			}
			if _, err := fmt.Fprintf(w, "%d", v); err != nil {
				return err
			}
			f += 5

		case 'I':
			if f+3 >= len(data) {
				return fmt.Errorf("serde: %s truncated, expected 4 bytes to unpack %c but only %d bytes remaining", what, ch, len(data))
			}
			var v uint32
			b := data[f+1 : f+5]
			if eo == endianBig {
				v = binary.BigEndian.Uint32(b)
			} else {
				v = binary.LittleEndian.Uint32(b)
			}
			if _, err := fmt.Fprintf(w, "%d", v); err != nil {
				return err
			}
			f += 5

		case 'q':
			if f+7 >= len(data) {
				return fmt.Errorf("serde: %s truncated, expected 8 bytes to unpack %c but only %d bytes remaining", what, ch, len(data))
			}
			var v int64
			b := data[f+1 : f+9]
			if eo == endianBig {
				v = int64(binary.BigEndian.Uint64(b))
			} else {
				v = int64(binary.LittleEndian.Uint64(b))
			}
			if _, err := fmt.Fprintf(w, "%d", v); err != nil {
				return err
			}
			f += 9

		case 'Q':
			if f+7 >= len(data) {
				return fmt.Errorf("serde: %s truncated, expected 8 bytes to unpack %c but only %d bytes remaining", what, ch, len(data))
			}
			var v uint64
			b := data[f+1 : f+9]
			if eo == endianBig {
				v = binary.BigEndian.Uint64(b)
			} else {
				v = binary.LittleEndian.Uint64(b)
			}
			if _, err := fmt.Fprintf(w, "%d", v); err != nil {
				return err
			}
			f += 9

		case 'c':
			if f+1 >= len(data) {
				return fmt.Errorf("serde: %s truncated, expected 1 bytes to unpack %c but only %d bytes remaining", what, ch, len(data))
			}
			if _, err := io.WriteString(w, string(data[f+1])); err != nil {
				return err
			}
			f += 2

		case 's':
			remaining := len(data) - f - 1
			if remaining > 0 {
				if _, err := io.WriteString(w, string(data[f+1:])); err != nil {
					return err
				}
			}
			f = len(fmtStr) // Done

		case '$':
			remaining := len(data) - f - 1
			if remaining > 0 {
				return fmt.Errorf("serde: %s expected end-of-input, but %d bytes remaining", what, remaining)
			}
			f++

		default:
			return fmt.Errorf("serde: invalid pack-format token '%c'", ch)
		}
	}
	return nil
}

// PackCheck validates a pack-format string.
func PackCheck(name, fmtStr string) {
	valid := " <>bBhHiIqQcs$"
	for _, c := range fmtStr {
		if !strings.ContainsRune(valid, c) {
			panic(fmt.Sprintf("serde: invalid token '%c' in %s pack-format", c, name))
		}
	}
}
