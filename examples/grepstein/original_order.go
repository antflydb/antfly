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

package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// Completion order belongs to the worker pool; publication order belongs to a
// bounded disk index. This keeps every upload slot usable even when an early
// large object is slow, without retaining a corpus-sized reorder map in RAM.
type originalOrder struct {
	data, index *os.File
	bytes       int64
	count       int
}

func newOriginalOrder(state string) (*originalOrder, error) {
	data, err := os.CreateTemp(state, ".originals-data-*")
	if err != nil {
		return nil, err
	}
	index, err := os.CreateTemp(state, ".originals-index-*")
	if err != nil {
		data.Close()
		os.Remove(data.Name())
		return nil, err
	}
	return &originalOrder{data: data, index: index}, nil
}
func (o *originalOrder) Close() {
	for _, f := range []*os.File{o.data, o.index} {
		f.Close()
		os.Remove(f.Name())
	}
}
func (o *originalOrder) Add(position int, record originalRecord) error {
	if position < 0 || position >= 2_000_000 {
		return fmt.Errorf("original order position exceeds inventory budget")
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if len(raw) > 16<<20 {
		return fmt.Errorf("original catalog record exceeds 16 MiB")
	}
	if _, err := o.data.Write(raw); err != nil {
		return err
	}
	var entry [16]byte
	binary.LittleEndian.PutUint64(entry[:8], uint64(o.bytes))
	binary.LittleEndian.PutUint64(entry[8:], uint64(len(raw)))
	if _, err := o.index.WriteAt(entry[:], int64(position)*16); err != nil {
		return err
	}
	o.bytes += int64(len(raw))
	o.count++
	return nil
}
func (o *originalOrder) Emit(count int, out io.Writer) error {
	if o.count != count {
		return fmt.Errorf("original completion inventory differs from submitted work")
	}
	if _, err := o.index.Seek(0, io.SeekStart); err != nil {
		return err
	}
	index := bufio.NewReader(o.index)
	var entry [16]byte
	var buffer []byte
	for i := 0; i < count; i++ {
		if _, err := io.ReadFull(index, entry[:]); err != nil {
			return err
		}
		offset, size := binary.LittleEndian.Uint64(entry[:8]), binary.LittleEndian.Uint64(entry[8:])
		if size == 0 || size > 16<<20 || offset > uint64(o.bytes) || size > uint64(o.bytes)-offset {
			return fmt.Errorf("invalid original completion index")
		}
		if cap(buffer) < int(size) {
			buffer = make([]byte, size)
		} else {
			buffer = buffer[:size]
		}
		if _, err := o.data.ReadAt(buffer, int64(offset)); err != nil {
			return err
		}
		if _, err := out.Write(buffer); err != nil {
			return err
		}
	}
	return nil
}
