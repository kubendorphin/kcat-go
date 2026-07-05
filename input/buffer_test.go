package input

import "testing"

func TestParseDelim(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"\\n", "\n"},
		{"\\t\\n\\n", "\t\n\n"},
		{"\\x54!\\x45\\x53T", "T!EST"},
		{"\\x30", "0"},
		{"hello", "hello"},
		{"a\\nb", "a\nb"},
	}

	for _, tt := range tests {
		got, err := ParseDelim(tt.in)
		if err != nil {
			t.Errorf("ParseDelim(%q) error: %v", tt.in, err)
			continue
		}
		if string(got) != tt.want {
			t.Errorf("ParseDelim(%q) = %q, want %q", tt.in, string(got), tt.want)
		}
	}
}

func TestIndexBM(t *testing.T) {
	tests := []struct {
		hay    []byte
		needle []byte
		want   int
	}{
		{[]byte("hello world"), []byte("world"), 6},
		{[]byte("hello world"), []byte("xyz"), -1},
		{[]byte("aaa"), []byte("a"), 0},
		{[]byte("abcabc"), []byte("bc"), 1},
		{[]byte(""), []byte("a"), -1},
		{[]byte("a"), []byte("abc"), -1},
		{[]byte("key1;KeyDel;Value1"), []byte(";KeyDel;"), 4},
		{[]byte("Is The;"), []byte(";"), 6},
	}

	for _, tt := range tests {
		got := IndexBM(tt.hay, tt.needle)
		if got != tt.want {
			t.Errorf("IndexBM(%q, %q) = %d, want %d", tt.hay, tt.needle, got, tt.want)
		}
	}
}

func TestStrnstr(t *testing.T) {
	tests := []struct {
		hay    string
		needle string
		want   int
	}{
		{"Sep;Post", "Sep;", 0},
		{"Sep;", "Sep;", 0},
		{"PreSep;Post", "Sep;", 3},
		{"PreSep;Post", "SepPost", -1},
		{"Key1KeyDel;Value1", "KeyDel;", 4},
		{"Is The;", ";", 6},
		{"no match here", "xyz", -1},
	}

	for _, tt := range tests {
		got := Strnstr([]byte(tt.hay), []byte(tt.needle))
		if got != tt.want {
			t.Errorf("Strnstr(%q, %q) = %d, want %d", tt.hay, tt.needle, got, tt.want)
		}
	}
}

func TestIndexSingleByte(t *testing.T) {
	tests := []struct {
		hay    []byte
		needle []byte
		want   int
	}{
		{[]byte("hello\nworld"), []byte{'\n'}, 5},
		{[]byte("abc"), []byte{','}, -1},
		{[]byte{0x01, 0x02, 0x03}, []byte{0x02}, 1},
	}

	for _, tt := range tests {
		got := Index(tt.hay, tt.needle)
		if got != tt.want {
			t.Errorf("Index(%v, %v) = %d, want %d", tt.hay, tt.needle, got, tt.want)
		}
	}
}
