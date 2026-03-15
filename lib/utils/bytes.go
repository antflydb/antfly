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

package utils

import "bytes"

// PrefixSuccessor returns the prefix immediately following prefix.
// This is useful for creating exclusive upper bounds in key-value iterators.
func PrefixSuccessor(prefix []byte) []byte {
	if len(prefix) == 0 {
		return []byte{}
	}
	n := len(prefix)
	for n--; n >= 0 && prefix[n] == '\xff'; n-- {
	}
	if n == -1 {
		return prefix
	}
	ans := bytes.Clone(prefix[:n+1])
	ans[n]++
	return ans
}
