package types

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"

	"go.uber.org/zap/zapcore"
)

// RangeEndSentinel is used as the upper bound for unbounded ranges in Pebble.
// It's 256 bytes of 0xFF, which should exceed any realistic key length.
// This is a package-level variable rather than a const because Go doesn't support slice consts.
var RangeEndSentinel = bytes.Repeat([]byte{0xFF}, 256)

// Range represents a half-open byte range [start, end).
// Empty slices represent unbounded endpoints.
type Range [2][]byte

// String returns a compact, token-efficient representation of the range.
// Keys are hex-encoded for consistency. Empty keys (unbounded) are omitted.
// Examples:
//   - [616263,78797a) for "abc" to "xyz"
//   - [,ff) for unbounded start to 0xff
//   - [61,) for "a" to unbounded end
func (r Range) String() string {
	return fmt.Sprintf("[%s,%s)", formatRangeKey(r[0]), formatRangeKey(r[1]))
}

// RangeFromString parses a range string in the format "[start,end)".
// Both start and end are hex-encoded. Empty strings represent unbounded endpoints.
// Examples: "[616263,78797a)", "[,ff)", "[61,)"
func RangeFromString(s string) (Range, error) {
	if !strings.HasPrefix(s, "[") || !strings.HasSuffix(s, ")") {
		return Range{}, fmt.Errorf("invalid range format: expected '[start,end)', got %q", s)
	}

	content := s[1 : len(s)-1]
	parts := strings.SplitN(content, ",", 2)
	if len(parts) != 2 {
		return Range{}, fmt.Errorf("invalid range format: expected '[start,end)', got %q", s)
	}

	var r Range
	var err error

	if parts[0] != "" {
		r[0], err = hex.DecodeString(parts[0])
		if err != nil {
			return Range{}, fmt.Errorf("invalid range start key %q: %w", parts[0], err)
		}
	}

	if parts[1] != "" {
		r[1], err = hex.DecodeString(parts[1])
		if err != nil {
			return Range{}, fmt.Errorf("invalid range end key %q: %w", parts[1], err)
		}
	}

	return r, nil
}

// Equal returns true if both ranges have identical start and end keys.
func (r Range) Equal(other Range) bool {
	return bytes.Equal(r[0], other[0]) && bytes.Equal(r[1], other[1])
}

// Contains returns true if key k is within the range [start, end).
// An empty slice for start means unbounded lower limit (-infinity).
// An empty slice for end means unbounded upper limit (+infinity).
func (r Range) Contains(k []byte) bool {
	start, end := r[0], r[1]
	lowerOK := len(start) == 0 || bytes.Compare(k, start) >= 0
	upperOK := len(end) == 0 || bytes.Compare(k, end) < 0
	return lowerOK && upperOK
}

// EndForPebble returns the End value suitable for Pebble APIs.
// Empty/nil End (unbounded) is converted to RangeEndSentinel (256 bytes of 0xFF).
// This is needed because Pebble/bytes.Compare treats empty byte slice as sorting
// BEFORE all non-empty keys, but Range treats empty End as unbounded (+infinity).
func (r Range) EndForPebble() []byte {
	if len(r[1]) == 0 {
		return RangeEndSentinel
	}
	return r[1]
}

// formatRangeKey formats a single range key for logging.
// Returns empty string for nil/empty (unbounded), otherwise hex.
func formatRangeKey(b []byte) string {
	return FormatKey(b)
}

// FormatKey formats a byte slice as hex for display.
// Returns empty string for nil/empty slices.
func FormatKey(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return hex.EncodeToString(b)
}

// MarshalLogObject implements zapcore.ObjectMarshaler so Range logs as its
// string representation instead of an array of byte values.
func (r Range) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("start", formatRangeKey(r[0]))
	enc.AddString("end", formatRangeKey(r[1]))
	return nil
}
