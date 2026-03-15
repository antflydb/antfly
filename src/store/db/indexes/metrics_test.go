// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package indexes

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyToUint64(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
		want uint64
	}{
		{
			name: "nil key",
			key:  nil,
			want: 0,
		},
		{
			name: "empty key",
			key:  []byte{},
			want: 0,
		},
		{
			name: "single byte",
			key:  []byte{0xFF},
			want: 0xFF00000000000000,
		},
		{
			name: "two bytes",
			key:  []byte{0x01, 0x02},
			want: 0x0102000000000000,
		},
		{
			name: "full 8 bytes",
			key:  []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
			want: 0x0102030405060708,
		},
		{
			name: "more than 8 bytes uses first 8",
			key:  []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0xFF, 0xFF},
			want: 0x0102030405060708,
		},
		{
			name: "all zeros",
			key:  []byte{0x00, 0x00, 0x00, 0x00},
			want: 0,
		},
		{
			name: "all ones",
			key:  []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			want: math.MaxUint64,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := keyToUint64(tt.key)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEstimateProgress(t *testing.T) {
	tests := []struct {
		name       string
		rangeStart []byte
		rangeEnd   []byte
		currentKey []byte
		want       float64
	}{
		{
			name:       "empty range end",
			rangeStart: []byte{0x00},
			rangeEnd:   nil,
			currentKey: []byte{0x50},
			want:       0,
		},
		{
			name:       "empty current key",
			rangeStart: []byte{0x00},
			rangeEnd:   []byte{0xFF},
			currentKey: nil,
			want:       0,
		},
		{
			name:       "current at start",
			rangeStart: []byte{0x00},
			rangeEnd:   []byte{0xFF},
			currentKey: []byte{0x00},
			want:       0,
		},
		{
			name:       "current before start",
			rangeStart: []byte{0x10},
			rangeEnd:   []byte{0xFF},
			currentKey: []byte{0x05},
			want:       0,
		},
		{
			name:       "current at end",
			rangeStart: []byte{0x00},
			rangeEnd:   []byte{0xFF},
			currentKey: []byte{0xFF},
			want:       1.0,
		},
		{
			name:       "current past end",
			rangeStart: []byte{0x00},
			rangeEnd:   []byte{0x80},
			currentKey: []byte{0xFF},
			want:       1.0,
		},
		{
			name:       "inverted range",
			rangeStart: []byte{0xFF},
			rangeEnd:   []byte{0x00},
			currentKey: []byte{0x50},
			want:       1.0,
		},
		{
			name:       "midpoint",
			rangeStart: []byte{0x00},
			rangeEnd:   []byte{0x80},
			currentKey: []byte{0x40},
			want:       0.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := estimateProgress(tt.rangeStart, tt.rangeEnd, tt.currentKey)
			require.InDelta(t, tt.want, got, 0.01,
				"estimateProgress(%x, %x, %x) = %f, want %f",
				tt.rangeStart, tt.rangeEnd, tt.currentKey, got, tt.want)
		})
	}
}

func TestEstimateProgress_Monotonic(t *testing.T) {
	// Progress should be monotonically increasing as the key advances
	start := []byte("aaa")
	end := []byte("zzz")

	keys := [][]byte{
		[]byte("aab"),
		[]byte("bbb"),
		[]byte("mmm"),
		[]byte("xxx"),
		[]byte("zzz"),
	}

	prev := 0.0
	for _, key := range keys {
		progress := estimateProgress(start, end, key)
		assert.GreaterOrEqual(t, progress, prev,
			"progress should be monotonically increasing: key=%s prev=%f current=%f",
			key, prev, progress)
		prev = progress
	}
}
