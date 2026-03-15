// Copyright 2015 The etcd Authors
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

package multirafthttp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/antflydb/antfly/lib/multirafthttp/pbutil"
)

// msgCodecBufferPool is a pool of byte slices for decoding messages
var msgCodecBufferPool = sync.Pool{
	New: func() any {
		// Start with a reasonable default size, will grow as needed
		return make([]byte, 0, 4096)
	},
}

// messageEncoder is a encoder that can encode all kinds of messages.
// It MUST be used with a paired messageDecoder.
type messageEncoder struct {
	w io.Writer
}

func (enc *messageEncoder) encode(m *multiMessage) error {
	if err := binary.Write(enc.w, binary.BigEndian, uint64(m.shardID)); err != nil {
		return err
	}
	size := m.msg.Size()
	if size < 0 {
		return fmt.Errorf("message %d has negative size", size)
	}
	if err := binary.Write(enc.w, binary.BigEndian, uint64(size)); err != nil {
		return err
	}
	_, err := enc.w.Write(pbutil.MustMarshal(&m.msg))
	return err
}

// messageDecoder is a decoder that can decode all kinds of messages.
type messageDecoder struct {
	r io.Reader
}

var (
	readBytesLimit     uint64 = 512 * 1024 * 1024 // 512 MB
	ErrExceedSizeLimit        = errors.New("rafthttp: error limit exceeded")
)

func (dec *messageDecoder) decode() (multiMessage, error) {
	return dec.decodeLimit(readBytesLimit)
}

func (dec *messageDecoder) decodeLimit(numBytes uint64) (multiMessage, error) {
	var m multiMessage
	if err := binary.Read(dec.r, binary.BigEndian, &m.shardID); err != nil {
		return m, err
	}
	var l uint64
	if err := binary.Read(dec.r, binary.BigEndian, &l); err != nil {
		return m, err
	}
	if l > numBytes {
		return m, ErrExceedSizeLimit
	}
	if l > math.MaxInt {
		return m, fmt.Errorf("integer overflow decoding message %d", l)
	}

	// Get a buffer from the pool
	buf := msgCodecBufferPool.Get().([]byte) //nolint:staticcheck // SA6002: slice pool is faster than *[]byte for this use case
	if cap(buf) < int(l) {
		// If the buffer is too small, create a new one with the exact size needed
		buf = make([]byte, int(l))
	} else {
		// Resize the buffer to the exact length needed
		buf = buf[:int(l)]
	}

	// Ensure we return the buffer to the pool
	defer func() {
		// Reset the slice to zero length but keep capacity for reuse
		msgCodecBufferPool.Put(buf[:0]) //nolint:staticcheck // SA6002
	}()

	if _, err := io.ReadFull(dec.r, buf); err != nil {
		return m, err
	}
	return m, m.msg.Unmarshal(buf)
}
