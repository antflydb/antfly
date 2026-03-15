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

package common

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/antflydb/antfly/lib/types"
)

var ErrSplitImpossible = errors.New("range split impossible: no median")

// FindSplitKeyByNaive finds a split key S for the range r = [startKey, endKey)
// such that startKey <= S < endKey. The key S is chosen based on the byte prefixes
// of startKey and endKey to provide a reasonable lexicographical split point.
// If no key strictly greater than startKey can be found (e.g., range is ["a", "a\x00")),
// it may return startKey.
// If the range is invalid (start >= end), it returns an error.
// If endKey is nil or empty (unbounded), it finds the shortest key greater than startKey
// by incrementing the first non-0xFF byte.
func FindSplitKeyByNaive(r types.Range) ([]byte, error) {
	startKey := r[0]
	endKey := r[1]

	// 1. Basic validation
	if len(endKey) > 0 && bytes.Compare(startKey, endKey) >= 0 {
		return nil, fmt.Errorf("invalid range: start key %q >= end key %q", startKey, endKey)
	}

	// 2. Handle unbounded endKey
	if len(endKey) == 0 {
		if len(startKey) == 0 {
			// Range is [nil, nil), effectively the entire keyspace. Split at midpoint.
			return []byte{0x80}, nil
		}
		// Find the first byte we can increment
		i := 0
		for i < len(startKey) && startKey[i] == 0xFF {
			i++
		}
		if i == len(startKey) {
			// startKey consists entirely of 0xFF bytes, cannot increment further.
			return startKey, nil
		}
		// Increment the first non-0xFF byte
		splitKey := make([]byte, i+1)
		copy(splitKey, startKey[:i])
		splitKey[i] = startKey[i] + 1
		return splitKey, nil
	}

	if bytes.Equal(startKey, []byte{0}) && bytes.Equal(endKey, []byte{0xFF}) {
		return []byte{'M'}, nil
	}

	// 3. Find the length l of the longest common prefix (LCP)
	l := 0
	maxL := min(len(endKey), len(startKey))
	for l < maxL && startKey[l] == endKey[l] {
		l++
	}

	// 4. Case 1: startKey is a prefix of endKey (l == len(startKey))
	if l == len(startKey) {
		if endKey[l] == 0 {
			return nil, ErrSplitImpossible
		}
		splitKey := append(startKey, endKey[l]/2)
		if bytes.Compare(splitKey, endKey) > 0 {
			splitKey := append(startKey, endKey[l]/2)
			return splitKey, nil
		}
		return splitKey, nil
	}
	if l == len(endKey) {
		splitKey := append(startKey, 0x80)
		return splitKey, nil
	}

	// 5. Case 2: Neither is a prefix (l < len(startKey) and l < len(endKey))
	x := uint16(startKey[l])
	y := uint16(endKey[l])

	if y == x+1 {
		// Bytes differ by 1
		splitKey := append(startKey, 0x80)
		return splitKey, nil
	}
	if bytes.Compare(endKey, []byte{123}) > 0 {
		if bytes.Compare(startKey, []byte{122}) >= 0 {
			return startKey, nil
		}
		splitKey := make([]byte, l+1)
		copy(splitKey, startKey[:l])
		splitKey[l] = startKey[l] + 1
		return splitKey, nil
	}
	// Bytes differ by more than 1
	splitKey := make([]byte, l+1)
	copy(splitKey, startKey[:l])
	z := (x + y) / 2
	if z > math.MaxUint8 {
		panic("integer overflow when computing split")
	}
	splitKey[l] = uint8(z)
	return splitKey, nil
}
