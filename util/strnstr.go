package util

// Strnstr finds the first occurrence of needle in haystack[:haystack_size],
// returning the offset into haystack or -1 if not found.
func Strnstr(haystack []byte, needle []byte) int {
	hlen := len(haystack)
	nlen := len(needle)
	if nlen == 0 {
		return 0
	}
	if nlen > hlen {
		return -1
	}

	nlast := nlen - 1
	for i := nlast; i <= hlen-nlen; {
		// Match needle from right to left (Boyer-Moore-inspired)
		match := true
		for j := nlast; j >= 0; j-- {
			if haystack[i-nlast+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return i - nlast
		}
		i++
	}
	return -1
}
