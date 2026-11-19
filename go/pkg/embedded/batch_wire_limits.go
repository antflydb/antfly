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
	"fmt"
	"math"
)

// batchWireSizer totals an encoded batch wire payload, refusing anything the
// format's u32 length prefixes and counts cannot represent and any total that
// would overflow int.
type batchWireSizer struct {
	size    int
	entries uint64
}

func (s *batchWireSizer) addLength(n int) error {
	if uint64(n) > math.MaxUint32 {
		return fmt.Errorf("embedded: batch wire field of %d bytes exceeds the format's 4 GiB limit", n)
	}
	return s.addBytes(4 + n)
}

func (s *batchWireSizer) addBytes(n int) error {
	if n > math.MaxInt-s.size {
		return fmt.Errorf("embedded: batch wire payload exceeds the addressable size")
	}
	s.size += n
	return nil
}

func (s *batchWireSizer) addEntry() error {
	s.entries++
	if s.entries > math.MaxUint32 {
		return fmt.Errorf("embedded: batch wire holds at most %d writes or deletes", uint32(math.MaxUint32))
	}
	return nil
}
