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
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/multirafthttp/pbutil"
	"github.com/antflydb/antfly/lib/multirafthttp/stats"
	"github.com/antflydb/antfly/lib/types"

	"go.etcd.io/raft/v3/raftpb"
)

const (
	msgTypeLinkHeartbeat uint8 = 0
	msgTypeAppEntries    uint8 = 1
	msgTypeApp           uint8 = 2

	msgAppV2BufSize = 5 * 1024 * 1024
)

// bufferPool is a pool of byte buffers to avoid repeated allocations
var bufferPool = sync.Pool{
	New: func() any {
		return make([]byte, msgAppV2BufSize)
	},
}

// largeBufferPool is a pool for buffers larger than msgAppV2BufSize
var largeBufferPool = sync.Pool{
	New: func() any {
		return make([]byte, msgAppV2BufSize*2)
	},
}

// msgappv2 stream sends three types of message: linkHeartbeatMessage,
// AppEntries and MsgApp. AppEntries is the MsgApp that is sent in
// replicate state in raft, whose index and term are fully predictable.
//
// Data format of linkHeartbeatMessage:
// | offset | bytes | description |
// +--------+-------+-------------+
// | 0      | 1     | \x00        |
//
// Data format of AppEntries:
// | offset | bytes | description |
// +--------+-------+-------------+
// | 0      | 1     | \x01        |
// | 1      | 8     | length of entries |
// | 9      | 8     | length of first entry |
// | 17     | n1    | first entry |
// ...
// | x      | 8     | length of k-th entry data |
// | x+8    | nk    | k-th entry data |
// | x+8+nk | 8     | commit index |
//
// Data format of MsgApp:
// | offset | bytes | description |
// +--------+-------+-------------+
// | 0      | 1     | \x02        |
// | 1      | 8     | length of encoded message |
// | 9      | n     | encoded message |
type msgAppV2Encoder struct {
	w  io.Writer
	fs *stats.FollowerStats

	term      uint64
	index     uint64
	buf       []byte
	uint64buf []byte
	uint8buf  []byte
}

func newMsgAppV2Encoder(w io.Writer, fs *stats.FollowerStats) *msgAppV2Encoder {
	return &msgAppV2Encoder{
		w:         w,
		fs:        fs,
		buf:       make([]byte, msgAppV2BufSize),
		uint64buf: make([]byte, 8),
		uint8buf:  make([]byte, 1),
	}
}

func (enc *msgAppV2Encoder) encode(m *multiMessage) error {
	start := time.Now()
	switch {
	case isLinkHeartbeatMessage(m):
		enc.uint8buf[0] = msgTypeLinkHeartbeat
		if _, err := enc.w.Write(enc.uint8buf); err != nil {
			return err
		}
		return nil
	case enc.index == m.msg.Index && enc.term == m.msg.LogTerm && m.msg.LogTerm == m.msg.Term:
		enc.uint8buf[0] = msgTypeAppEntries
		if _, err := enc.w.Write(enc.uint8buf); err != nil {
			return err
		}
		// write shardID for the message
		binary.BigEndian.PutUint64(enc.uint64buf, uint64(m.shardID))
		if _, err := enc.w.Write(enc.uint64buf); err != nil {
			return err
		}
		// write length of entries
		binary.BigEndian.PutUint64(enc.uint64buf, uint64(len(m.msg.Entries)))
		if _, err := enc.w.Write(enc.uint64buf); err != nil {
			return err
		}
		for i := range len(m.msg.Entries) {
			// write length of entry
			//
			size := m.msg.Entries[i].Size()
			if size < 0 {
				return fmt.Errorf("entry %d in msgAppEntries has negative size", i)
			}
			binary.BigEndian.PutUint64(enc.uint64buf, uint64(size))
			if _, err := enc.w.Write(enc.uint64buf); err != nil {
				return err
			}
			if size < msgAppV2BufSize {
				if _, err := m.msg.Entries[i].MarshalTo(enc.buf); err != nil {
					return err
				}
				if _, err := enc.w.Write(enc.buf[:size]); err != nil {
					return err
				}
			} else {
				if _, err := enc.w.Write(pbutil.MustMarshal(&m.msg.Entries[i])); err != nil {
					return err
				}
			}
			enc.index++
		}
		// write commit index
		binary.BigEndian.PutUint64(enc.uint64buf, m.msg.Commit)
		if _, err := enc.w.Write(enc.uint64buf); err != nil {
			return err
		}
	default:
		if err := binary.Write(enc.w, binary.BigEndian, msgTypeApp); err != nil {
			return err
		}
		// write shardID for the message
		if err := binary.Write(enc.w, binary.BigEndian, uint64(m.shardID)); err != nil {
			return err
		}
		// write size of message
		size := m.msg.Size()
		if size < 0 {
			return fmt.Errorf("entry %d in msgApp has negative size", size)
		}
		if err := binary.Write(enc.w, binary.BigEndian, uint64(size)); err != nil {
			return err
		}
		// write message
		if _, err := enc.w.Write(pbutil.MustMarshal(&m.msg)); err != nil {
			return err
		}

		enc.term = m.msg.Term
		enc.index = m.msg.Index
		if l := len(m.msg.Entries); l > 0 {
			enc.index = m.msg.Entries[l-1].Index
		}
	}
	enc.fs.Succ(time.Since(start))
	return nil
}

type msgAppV2Decoder struct {
	r             io.Reader
	local, remote types.ID

	term      uint64
	index     uint64
	buf       []byte
	uint64buf []byte
	uint8buf  []byte
}

func newMsgAppV2Decoder(r io.Reader, local, remote types.ID) *msgAppV2Decoder {
	return &msgAppV2Decoder{
		r:         r,
		local:     local,
		remote:    remote,
		buf:       make([]byte, msgAppV2BufSize),
		uint64buf: make([]byte, 8),
		uint8buf:  make([]byte, 1),
	}
}

func (dec *msgAppV2Decoder) decode() (multiMessage, error) {
	var (
		m   multiMessage
		typ uint8
	)
	if _, err := io.ReadFull(dec.r, dec.uint8buf); err != nil {
		return m, err
	}
	typ = dec.uint8buf[0]
	switch typ {
	case msgTypeLinkHeartbeat:
		return linkHeartbeatMessage, nil
	case msgTypeAppEntries:
		m = multiMessage{msg: raftpb.Message{
			Type:    raftpb.MsgApp,
			From:    uint64(dec.remote),
			To:      uint64(dec.local),
			Term:    dec.term,
			LogTerm: dec.term,
			Index:   dec.index,
		}}

		// decode shardID
		if _, err := io.ReadFull(dec.r, dec.uint64buf); err != nil {
			return m, err
		}
		m.shardID = types.ID(binary.BigEndian.Uint64(dec.uint64buf))
		// decode entries
		if _, err := io.ReadFull(dec.r, dec.uint64buf); err != nil {
			return m, err
		}
		l := binary.BigEndian.Uint64(dec.uint64buf)
		if l > math.MaxInt {
			return m, fmt.Errorf("unexepcted length %d of msgAppEntries messages: int overflow", l)
		}
		m.msg.Entries = make([]raftpb.Entry, int(l))
		for i := range int(l) {
			if _, err := io.ReadFull(dec.r, dec.uint64buf); err != nil {
				return m, err
			}
			size := binary.BigEndian.Uint64(dec.uint64buf)
			if size > math.MaxInt {
				return m, fmt.Errorf("unexpected size %d of msgAppEntries message: int overflow", size)
			}
			var buf []byte
			var pooledBuf []byte
			if size < msgAppV2BufSize {
				buf = dec.buf[:size]
			} else {
				// Get buffer from pool and resize if needed
				pooledBuf = largeBufferPool.Get().([]byte) //nolint:staticcheck // SA6002
				if cap(pooledBuf) < int(size) {
					pooledBuf = make([]byte, int(size))
				} else {
					pooledBuf = pooledBuf[:int(size)]
				}
				buf = pooledBuf
			}
			if _, err := io.ReadFull(dec.r, buf); err != nil {
				if pooledBuf != nil {
					largeBufferPool.Put(pooledBuf) //nolint:staticcheck // SA6002
				}
				return m, err
			}
			dec.index++
			// 1 alloc
			pbutil.MustUnmarshal(&m.msg.Entries[i], buf)
			// Return buffer to pool if we used one
			if pooledBuf != nil {
				largeBufferPool.Put(pooledBuf) //nolint:staticcheck // SA6002
			}
		}
		// decode commit index
		if _, err := io.ReadFull(dec.r, dec.uint64buf); err != nil {
			return m, err
		}
		m.msg.Commit = binary.BigEndian.Uint64(dec.uint64buf)
	case msgTypeApp:
		var size uint64
		if err := binary.Read(dec.r, binary.BigEndian, &m.shardID); err != nil {
			return m, err
		}
		if err := binary.Read(dec.r, binary.BigEndian, &size); err != nil {
			return m, err
		}
		if size > math.MaxInt {
			return m, fmt.Errorf("unexepcted size %d of msgApp messages: int overflow", size)
		}
		var buf []byte
		if size <= msgAppV2BufSize {
			// Get buffer from standard pool
			buf = bufferPool.Get().([]byte)[:size] //nolint:staticcheck // SA6002
		} else {
			// Get buffer from large pool and resize if needed
			buf = largeBufferPool.Get().([]byte) //nolint:staticcheck // SA6002
			if cap(buf) < int(size) {
				buf = make([]byte, int(size))
			} else {
				buf = buf[:int(size)]
			}
		}
		if _, err := io.ReadFull(dec.r, buf); err != nil {
			if size <= msgAppV2BufSize {
				bufferPool.Put(buf) //nolint:staticcheck // SA6002
			} else {
				largeBufferPool.Put(buf) //nolint:staticcheck // SA6002
			}
			return m, err
		}
		pbutil.MustUnmarshal(&m.msg, buf)
		// Return buffer to appropriate pool
		if size <= msgAppV2BufSize {
			bufferPool.Put(buf) //nolint:staticcheck // SA6002
		} else {
			largeBufferPool.Put(buf) //nolint:staticcheck // SA6002
		}

		dec.term = m.msg.Term
		dec.index = m.msg.Index
		if l := len(m.msg.Entries); l > 0 {
			dec.index = m.msg.Entries[l-1].Index
		}
	default:
		return m, fmt.Errorf("failed to parse type %d in msgappv2 stream", typ)
	}
	return m, nil
}
