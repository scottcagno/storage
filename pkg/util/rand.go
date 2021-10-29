package util

import (
	"bytes"
	"math/rand"
	"strings"
	"time"
)

var src = rand.NewSource(time.Now().UnixNano())

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandString(n int) string {
	sb := strings.Builder{}
	sb.Grow(n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			sb.WriteByte(letterBytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return sb.String()
}

func RandBytes(n int) []byte {
	bb := bytes.Buffer{}
	bb.Grow(n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			bb.WriteByte(letterBytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return bb.Bytes()
}

func RandIntn(min, max int) int {
	return rand.Intn(max-min) + min
}

func FormattedWidthString(s string, n int) string {
	if len(s) == 0 {
		return ""
	}
	if n >= len(s) {
		return s
	}
	lines := make([]string, 0, (len(s)-1)/n+1)
	j, k := 0, 0
	for i := range s {
		if j == n {
			lines = append(lines, s[k:i])
			j, k = 0, i
		}
		j++
	}
	lines = append(lines, s[k:])
	return strings.Join(lines, "\n")
}

func FormattedWidthBytes(b []byte, n int) []byte {
	return []byte(FormattedWidthString(string(b), n))
}
