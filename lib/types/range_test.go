package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRange_String(t *testing.T) {
	tests := []struct {
		name string
		r    Range
		want string
	}{
		{"both bounded ASCII", Range{[]byte("abc"), []byte("xyz")}, "[616263,78797a)"},
		{"both bounded binary", Range{[]byte{0x00, 0xff}, []byte{0x80}}, "[00ff,80)"},
		{"unbounded start", Range{nil, []byte{0xff}}, "[,ff)"},
		{"unbounded end", Range{[]byte("a"), nil}, "[61,)"},
		{"fully unbounded", Range{nil, nil}, "[,)"},
		{"empty slices", Range{[]byte{}, []byte{}}, "[,)"},
		{"single byte", Range{[]byte{0x00}, []byte{0x01}}, "[00,01)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.r.String())
		})
	}
}

func TestRangeFromString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Range
		wantErr bool
	}{
		{"both bounded", "[616263,78797a)", Range{[]byte("abc"), []byte("xyz")}, false},
		{"unbounded start", "[,ff)", Range{nil, []byte{0xff}}, false},
		{"unbounded end", "[61,)", Range{[]byte("a"), nil}, false},
		{"fully unbounded", "[,)", Range{nil, nil}, false},
		{"single byte range", "[00,01)", Range{[]byte{0x00}, []byte{0x01}}, false},
		{"binary keys", "[00ff,80)", Range{[]byte{0x00, 0xff}, []byte{0x80}}, false},

		// Error cases
		{"missing brackets", "abc,xyz", Range{}, true},
		{"missing open bracket", "abc,xyz)", Range{}, true},
		{"missing close paren", "[abc,xyz", Range{}, true},
		{"wrong close bracket", "[abc,xyz]", Range{}, true},
		{"no comma", "[abcxyz)", Range{}, true},
		{"invalid hex start", "[zz,ff)", Range{}, true},
		{"invalid hex end", "[ff,zz)", Range{}, true},
		{"odd hex length", "[fff,ff)", Range{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RangeFromString(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.True(t, tt.want.Equal(got), "want %v, got %v", tt.want, got)
		})
	}
}

func TestRange_Roundtrip(t *testing.T) {
	tests := []Range{
		{[]byte("abc"), []byte("xyz")},
		{nil, []byte{0xff}},
		{[]byte("a"), nil},
		{nil, nil},
		{[]byte{0x00, 0x01, 0x02}, []byte{0xfe, 0xff}},
	}
	for _, r := range tests {
		t.Run(r.String(), func(t *testing.T) {
			s := r.String()
			parsed, err := RangeFromString(s)
			require.NoError(t, err)
			assert.True(t, r.Equal(parsed), "roundtrip failed: %v -> %s -> %v", r, s, parsed)
		})
	}
}

func TestRange_Equal(t *testing.T) {
	tests := []struct {
		name string
		a, b Range
		want bool
	}{
		{"equal bounded", Range{[]byte("a"), []byte("z")}, Range{[]byte("a"), []byte("z")}, true},
		{"equal unbounded", Range{nil, nil}, Range{nil, nil}, true},
		{"equal empty slices", Range{[]byte{}, []byte{}}, Range{nil, nil}, true},
		{"different start", Range{[]byte("a"), []byte("z")}, Range{[]byte("b"), []byte("z")}, false},
		{"different end", Range{[]byte("a"), []byte("z")}, Range{[]byte("a"), []byte("y")}, false},
		{"one unbounded", Range{nil, []byte("z")}, Range{[]byte("a"), []byte("z")}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.a.Equal(tt.b))
		})
	}
}

func TestRange_Contains(t *testing.T) {
	tests := []struct {
		name string
		r    Range
		key  []byte
		want bool
	}{
		// Basic bounded range
		{"in range", Range{[]byte("a"), []byte("z")}, []byte("m"), true},
		{"at start", Range{[]byte("a"), []byte("z")}, []byte("a"), true},
		{"before end", Range{[]byte("a"), []byte("z")}, []byte("y"), true},
		{"at end (excluded)", Range{[]byte("a"), []byte("z")}, []byte("z"), false},
		{"before start", Range{[]byte("b"), []byte("z")}, []byte("a"), false},
		{"after end", Range{[]byte("a"), []byte("m")}, []byte("z"), false},
		{"awesome in [a, z)", Range{[]byte("a"), []byte("z")}, []byte("awesome"), true},
		{"awesome not in [b, z)", Range{[]byte("b"), []byte("z")}, []byte("awesome"), false},
		{"zebra not in [a, z)", Range{[]byte("a"), []byte("z")}, []byte("zebra"), false},
		{"z in [a, z\\x00)", Range{[]byte("a"), []byte("z\x00")}, []byte("z"), true},

		// Unbounded upper range (end is nil or empty)
		{"unbounded end", Range{[]byte("m"), nil}, []byte("z"), true},
		{"zebra in [a, +inf)", Range{[]byte("a"), nil}, []byte("zebra"), true},
		{"zebra in [a, +inf) with empty slice", Range{[]byte("a"), {}}, []byte("zebra"), true},
		{"aardvark not in [b, +inf)", Range{[]byte("b"), nil}, []byte("aardvark"), false},

		// Unbounded lower range (start is nil or empty)
		{"unbounded start", Range{nil, []byte("m")}, []byte("a"), true},
		{"apple in (-inf, z)", Range{nil, []byte("z")}, []byte("apple"), true},
		{"apple in (-inf, z) with empty slice", Range{{}, []byte("z")}, []byte("apple"), true},
		{"zebra not in (-inf, y)", Range{nil, []byte("y")}, []byte("zebra"), false},

		// Fully unbounded range
		{"fully unbounded", Range{nil, nil}, []byte("anything"), true},
		{"anything in (-inf, +inf) with empty slices", Range{{}, {}}, []byte("anything"), true},

		// Edge cases with exact bounds
		{"a in [a, b)", Range{[]byte("a"), []byte("b")}, []byte("a"), true},
		{"b not in [a, b)", Range{[]byte("a"), []byte("b")}, []byte("b"), false},
		{"a in [a, +inf)", Range{[]byte("a"), nil}, []byte("a"), true},
		{"z not in (-inf, z)", Range{nil, []byte("z")}, []byte("z"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.r.Contains(tt.key))
		})
	}
}
