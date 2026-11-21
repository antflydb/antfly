// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embedded

import (
	"math"
	"strconv"
	"testing"
)

func TestBatchWireSizerRejectsLengthsTheFormatCannotEncode(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("an int cannot hold a 4 GiB field length here")
	}
	// Run-time conversions: the constants overflow a 32-bit int.
	var limit uint64 = math.MaxUint32
	var s batchWireSizer
	if err := s.addLength(int(limit)); err != nil {
		t.Fatalf("a 4 GiB - 1 field fits the u32 prefix: %v", err)
	}
	if err := s.addLength(int(limit + 1)); err == nil {
		t.Fatal("a field longer than MaxUint32 must be refused, not truncated")
	}
}

func TestBatchWireSizerRejectsALengthWhosePrefixWouldWrap(t *testing.T) {
	// On a 32-bit int, MaxInt fits the u32 prefix, but 4 + MaxInt wraps.
	var s batchWireSizer
	if err := s.addLength(math.MaxInt); err == nil {
		t.Fatal("a field whose prefixed size passes MaxInt must be refused")
	}
	if err := s.addLength(-1); err == nil {
		t.Fatal("a negative length must be refused")
	}
}

func TestBatchWireSizerRejectsAnOverflowingTotal(t *testing.T) {
	s := batchWireSizer{size: math.MaxInt - 8}
	if err := s.addBytes(16); err == nil {
		t.Fatal("a total past MaxInt must be refused")
	}
}

func TestBatchWireSizerRejectsTooManyEntries(t *testing.T) {
	s := batchWireSizer{entries: math.MaxUint32}
	if err := s.addEntry(); err == nil {
		t.Fatal("more than MaxUint32 entries must be refused")
	}
}
