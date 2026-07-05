package util

// BytesToStr converts a byte slice to a string without allocation when the slice
// is already backed by a string. For byte slices from other sources, it copies.
func BytesToStr(b []byte) string {
	return string(b)
}

// StrToBytes converts a string to a byte slice.
func StrToBytes(s string) []byte {
	return []byte(s)
}

// TrimRightSpace trims trailing space characters (space, tab, \r, \n).
func TrimRightSpace(s string) string {
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t' || s[len(s)-1] == '\r' || s[len(s)-1] == '\n') {
		s = s[:len(s)-1]
	}
	return s
}

// HasPrefixAny checks if s starts with any of the given prefixes.
func HasPrefixAny(s string, prefixes ...string) bool {
	for _, p := range prefixes {
		if len(s) >= len(p) && s[:len(p)] == p {
			return true
		}
	}
	return false
}
