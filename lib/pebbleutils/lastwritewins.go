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

package pebbleutils

import "io"

// NewLastWriteWins returns a ValueMerger that keeps only the newest value.
// This is the safe fallback for keys that should only use Set(), not Merge().
func NewLastWriteWins(value []byte) *LastWriteWinsMerger {
	return newLastWriteWins(value)
}

func newLastWriteWins(value []byte) *LastWriteWinsMerger {
	m := &LastWriteWinsMerger{}
	m.value = append(m.value[:0], value...)
	return m
}

// LastWriteWinsMerger keeps the newest value and discards older ones.
type LastWriteWinsMerger struct {
	value []byte
}

func (m *LastWriteWinsMerger) MergeNewer(value []byte) error {
	m.value = append(m.value[:0], value...)
	return nil
}

func (m *LastWriteWinsMerger) MergeOlder(value []byte) error {
	// Older values are discarded — we already have a newer one.
	return nil
}

func (m *LastWriteWinsMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	return m.value, nil, nil
}
