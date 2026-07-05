// Package format provides the format string parser and printer for kcat.
// Format strings like "Topic %t [%p]: %s\n" are parsed into tokens.
package format

// TokenType represents a format token type.
type TokenType int

const (
	TypeStr TokenType = iota
	TypeOffset
	TypeKey
	TypeKeyLen
	TypePayload
	TypePayloadLen
	TypePayloadLenBinary
	TypeTopic
	TypePartition
	TypeTimestamp
	TypeHeaders
)

// Token represents a parsed format element.
type Token struct {
	Type TokenType
	Str  string // For TypeStr tokens, with escape sequences resolved
	Len  int    // Length of Str
}

// Parse parses a format string into a list of tokens.
// Supported tokens: %s (payload), %k (key), %S (payload len), %K (key len),
// %o (offset), %t (topic), %p (partition), %T (timestamp), %h (headers), %% (literal %).
// Literal strings can contain escape sequences: \n, \t, \r, \xNN.
func Parse(fmtStr string) ([]Token, error) {
	var tokens []Token
	s := []byte(fmtStr)
	i := 0

	for i < len(s) {
		if s[i] == '%' {
			if i+1 >= len(s) {
				return nil, &ParseError{Msg: "trailing %"}
			}
			switch s[i+1] {
			case 'o':
				tokens = append(tokens, Token{Type: TypeOffset})
			case 'k':
				tokens = append(tokens, Token{Type: TypeKey})
			case 'K':
				tokens = append(tokens, Token{Type: TypeKeyLen})
			case 's':
				tokens = append(tokens, Token{Type: TypePayload})
			case 'S':
				tokens = append(tokens, Token{Type: TypePayloadLen})
			case 'R':
				tokens = append(tokens, Token{Type: TypePayloadLenBinary})
			case 't':
				tokens = append(tokens, Token{Type: TypeTopic})
			case 'p':
				tokens = append(tokens, Token{Type: TypePartition})
			case 'T':
				tokens = append(tokens, Token{Type: TypeTimestamp})
			case 'h':
				tokens = append(tokens, Token{Type: TypeHeaders})
			case '%':
				tokens = append(tokens, Token{Type: TypeStr, Str: "%", Len: 1})
			default:
				return nil, &ParseError{Msg: "unsupported formatter %%" + string(s[i+1])}
			}
			i += 2
		} else {
			// Find next % or end
			j := i + 1
			for j < len(s) && s[j] != '%' {
				j++
			}
			literal := s[i:j]
			// Resolve escape sequences
			resolved, err := resolveEscapes(literal)
			if err != nil {
				return nil, err
			}
			if len(resolved) > 0 {
				tokens = append(tokens, Token{Type: TypeStr, Str: resolved, Len: len(resolved)})
			}
			i = j
		}
	}

	return tokens, nil
}

// resolveEscapes processes escape sequences in a literal string.
func resolveEscapes(s []byte) (string, error) {
	result := make([]byte, 0, len(s))
	i := 0
	for i < len(s) {
		if s[i] == '\\' && i+1 < len(s) {
			i++
			switch s[i] {
			case 'n':
				result = append(result, '\n')
			case 't':
				result = append(result, '\t')
			case 'r':
				result = append(result, '\r')
			case 'x':
				// Hex escape: \xNN
				i++
				hexStr := ""
				for i < len(s) && len(hexStr) < 2 {
					c := s[i]
					if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') {
						hexStr += string(c)
						i++
					} else {
						break
					}
				}
				if hexStr == "" {
					return "", &ParseError{Msg: "invalid hex escape"}
				}
				val := byte(0)
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
				result = append(result, val)
			default:
				result = append(result, s[i])
			}
		} else {
			result = append(result, s[i])
		}
		i++
	}
	return string(result), nil
}

// ParseError represents a format parsing error.
type ParseError struct {
	Msg string
}

func (e *ParseError) Error() string {
	return "format: " + e.Msg
}
