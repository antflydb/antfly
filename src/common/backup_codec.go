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
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
)

// AFB (Antfly Format for Backups) is a cross-backend portable backup format.
// Both Go and Zig backends can independently read and write this format.
//
// Layout:
//   [File Header 64 bytes]
//   [Block]*
//
// Each block:
//   [type: u8] [flags: u8] [payload_len: u32 LE] [payload: N bytes] [crc32: u32]

// AFB file magic bytes: "ANTFLYB\n"
var afbMagic = [8]byte{'A', 'N', 'T', 'F', 'L', 'Y', 'B', '\n'}

const (
	// AFBFormatVersion is the current format version.
	AFBFormatVersion uint32 = 1

	// AFBHeaderSize is the fixed size of the file header.
	AFBHeaderSize = 64

	// Block type envelope overhead: type(1) + flags(1) + payload_len(4) + crc32(4) = 10 bytes.
	blockEnvelopeOverhead = 10
)

// AFBHeaderFlag is a bit flag for the file header.
type AFBHeaderFlag uint32

const (
	// AFBFlagCompression indicates blocks may use zstd compression.
	AFBFlagCompression AFBHeaderFlag = 1 << 0
)

// AFBBlockType identifies the type of a block.
type AFBBlockType uint8

const (
	BlockClusterManifest  AFBBlockType = 0x01
	BlockTableManifest    AFBBlockType = 0x02
	BlockShardHeader      AFBBlockType = 0x03
	BlockDocumentBatch    AFBBlockType = 0x10
	BlockEmbeddingBatch   AFBBlockType = 0x11
	BlockSparseBatch      AFBBlockType = 0x12
	BlockSummaryBatch     AFBBlockType = 0x13
	BlockChunkBatch       AFBBlockType = 0x14
	BlockEdgeBatch        AFBBlockType = 0x15
	BlockTransactionBatch AFBBlockType = 0x16
	BlockShardFooter      AFBBlockType = 0xF0
	BlockFileFooter       AFBBlockType = 0xFF
)

// AFBBlockFlag is a bit flag for each block envelope.
type AFBBlockFlag uint8

const (
	// BlockFlagCompressed indicates the payload is zstd-compressed.
	BlockFlagCompressed AFBBlockFlag = 1 << 0
)

// AFBFileHeader is the 64-byte file header written at the start of an AFB file.
type AFBFileHeader struct {
	FormatVersion uint32
	Flags         AFBHeaderFlag
	CreatedAtNs   int64
	BackupID      uuid.UUID
	TableCount    uint32
	ShardCount    uint32
}

// --- Writer ---

// AFBWriter writes AFB-format backup files to an io.Writer.
type AFBWriter struct {
	w           io.Writer
	zstdEncoder *zstd.Encoder
	compress    bool
}

// NewAFBWriter creates a writer that emits AFB-format data.
// If compress is true, data blocks will be zstd-compressed.
func NewAFBWriter(w io.Writer, compress bool) (*AFBWriter, error) {
	var enc *zstd.Encoder
	if compress {
		var err error
		enc, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			return nil, fmt.Errorf("create zstd encoder: %w", err)
		}
	}
	return &AFBWriter{w: w, zstdEncoder: enc, compress: compress}, nil
}

// WriteHeader writes the 64-byte file header.
func (w *AFBWriter) WriteHeader(h AFBFileHeader) error {
	var buf [AFBHeaderSize]byte

	copy(buf[0:8], afbMagic[:])
	binary.LittleEndian.PutUint32(buf[8:12], h.FormatVersion)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(h.Flags))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(h.CreatedAtNs))
	copy(buf[24:40], h.BackupID[:])
	binary.LittleEndian.PutUint32(buf[40:44], h.TableCount)
	binary.LittleEndian.PutUint32(buf[44:48], h.ShardCount)

	// CRC32 of bytes [0..48)
	crc := crc32.ChecksumIEEE(buf[:48])
	binary.LittleEndian.PutUint32(buf[48:52], crc)
	// bytes [52..64) are reserved zeros

	_, err := w.w.Write(buf[:])
	return err
}

// WriteBlock writes a single block with the given type and payload.
// Data blocks (0x10-0x16) are compressed if the writer was created with compress=true.
// Metadata blocks (manifests, headers, footers) are never compressed.
func (w *AFBWriter) WriteBlock(blockType AFBBlockType, payload []byte) error {
	shouldCompress := w.compress && w.zstdEncoder != nil && isDataBlock(blockType)

	var flags AFBBlockFlag
	writePayload := payload

	if shouldCompress {
		compressed := w.zstdEncoder.EncodeAll(payload, nil)
		// Only use compressed if it's actually smaller.
		if len(compressed) < len(payload) {
			writePayload = compressed
			flags |= BlockFlagCompressed
		}
	}

	// Envelope: type(1) + flags(1) + payload_len(4) + payload(N)
	envelopeLen := 1 + 1 + 4 + len(writePayload)
	envelope := make([]byte, envelopeLen)
	envelope[0] = byte(blockType)
	envelope[1] = byte(flags)
	binary.LittleEndian.PutUint32(envelope[2:6], uint32(len(writePayload)))
	copy(envelope[6:], writePayload)

	// CRC32 of the entire envelope (type + flags + len + payload)
	crc := crc32.ChecksumIEEE(envelope)
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)

	if _, err := w.w.Write(envelope); err != nil {
		return err
	}
	_, err := w.w.Write(crcBuf[:])
	return err
}

// Close releases resources held by the writer.
func (w *AFBWriter) Close() error {
	if w.zstdEncoder != nil {
		w.zstdEncoder.Close()
	}
	return nil
}

// NewFileHeader creates a file header with sensible defaults.
func NewFileHeader(backupID uuid.UUID, tableCount, shardCount uint32, compress bool) AFBFileHeader {
	var flags AFBHeaderFlag
	if compress {
		flags |= AFBFlagCompression
	}
	return AFBFileHeader{
		FormatVersion: AFBFormatVersion,
		Flags:         flags,
		CreatedAtNs:   time.Now().UnixNano(),
		BackupID:      backupID,
		TableCount:    tableCount,
		ShardCount:    shardCount,
	}
}

// --- Reader ---

// AFBBlock is a decoded block read from an AFB file.
type AFBBlock struct {
	Type    AFBBlockType
	Payload []byte // decompressed payload
}

// AFBReader reads AFB-format backup files from an io.Reader.
type AFBReader struct {
	r           io.Reader
	zstdDecoder *zstd.Decoder
}

// NewAFBReader creates a reader that decodes AFB-format data.
func NewAFBReader(r io.Reader) (*AFBReader, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("create zstd decoder: %w", err)
	}
	return &AFBReader{r: r, zstdDecoder: dec}, nil
}

// ReadHeader reads and validates the 64-byte file header.
func (r *AFBReader) ReadHeader() (AFBFileHeader, error) {
	var buf [AFBHeaderSize]byte
	if _, err := io.ReadFull(r.r, buf[:]); err != nil {
		return AFBFileHeader{}, fmt.Errorf("read header: %w", err)
	}

	// Validate magic
	if [8]byte(buf[0:8]) != afbMagic {
		return AFBFileHeader{}, fmt.Errorf("invalid AFB magic: %x", buf[0:8])
	}

	// Validate CRC
	storedCRC := binary.LittleEndian.Uint32(buf[48:52])
	computedCRC := crc32.ChecksumIEEE(buf[:48])
	if storedCRC != computedCRC {
		return AFBFileHeader{}, fmt.Errorf("header CRC mismatch: stored=%x computed=%x", storedCRC, computedCRC)
	}

	h := AFBFileHeader{
		FormatVersion: binary.LittleEndian.Uint32(buf[8:12]),
		Flags:         AFBHeaderFlag(binary.LittleEndian.Uint32(buf[12:16])),
		CreatedAtNs:   int64(binary.LittleEndian.Uint64(buf[16:24])),
		TableCount:    binary.LittleEndian.Uint32(buf[40:44]),
		ShardCount:    binary.LittleEndian.Uint32(buf[44:48]),
	}
	copy(h.BackupID[:], buf[24:40])

	if h.FormatVersion > AFBFormatVersion {
		return AFBFileHeader{}, fmt.Errorf("unsupported AFB version %d (max %d)", h.FormatVersion, AFBFormatVersion)
	}

	return h, nil
}

// ReadBlock reads the next block from the stream.
// Returns io.EOF when the stream ends cleanly after a file footer.
func (r *AFBReader) ReadBlock() (AFBBlock, error) {
	// Read envelope header: type(1) + flags(1) + payload_len(4) = 6 bytes
	var envHeader [6]byte
	if _, err := io.ReadFull(r.r, envHeader[:]); err != nil {
		return AFBBlock{}, err
	}

	blockType := AFBBlockType(envHeader[0])
	flags := AFBBlockFlag(envHeader[1])
	payloadLen := binary.LittleEndian.Uint32(envHeader[2:6])

	// Read payload
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r.r, payload); err != nil {
		return AFBBlock{}, fmt.Errorf("read block payload (type=0x%02x, len=%d): %w", blockType, payloadLen, err)
	}

	// Read CRC32
	var crcBuf [4]byte
	if _, err := io.ReadFull(r.r, crcBuf[:]); err != nil {
		return AFBBlock{}, fmt.Errorf("read block CRC: %w", err)
	}

	// Verify CRC: computed over [type + flags + payload_len + payload]
	envelope := make([]byte, 6+payloadLen)
	copy(envelope[:6], envHeader[:])
	copy(envelope[6:], payload)
	storedCRC := binary.LittleEndian.Uint32(crcBuf[:])
	computedCRC := crc32.ChecksumIEEE(envelope)
	if storedCRC != computedCRC {
		return AFBBlock{}, fmt.Errorf("block CRC mismatch (type=0x%02x): stored=%x computed=%x", blockType, storedCRC, computedCRC)
	}

	// Decompress if needed
	if flags&BlockFlagCompressed != 0 {
		decompressed, err := r.zstdDecoder.DecodeAll(payload, nil)
		if err != nil {
			return AFBBlock{}, fmt.Errorf("decompress block (type=0x%02x): %w", blockType, err)
		}
		payload = decompressed
	}

	return AFBBlock{Type: blockType, Payload: payload}, nil
}

// Close releases resources held by the reader.
func (r *AFBReader) Close() {
	if r.zstdDecoder != nil {
		r.zstdDecoder.Close()
	}
}

// --- Batch Encoding Helpers ---

// EncodeDocumentBatch encodes a batch of document entries into a binary payload.
// Each entry is: [key_len:u32] [key] [value_flags:u8] [value_len:u32] [value] [timestamp_ns:u64]
func EncodeDocumentBatch(entries []DocumentEntry) []byte {
	// Pre-calculate size
	size := 4 // entry_count
	for _, e := range entries {
		size += 4 + len(e.Key) + 1 + 4 + len(e.Value) + 8
	}

	buf := make([]byte, 0, size)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entries)))

	for _, e := range entries {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.Key)))
		buf = append(buf, e.Key...)
		buf = append(buf, e.ValueFlags)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.Value)))
		buf = append(buf, e.Value...)
		buf = binary.LittleEndian.AppendUint64(buf, e.TimestampNs)
	}

	return buf
}

// DecodeDocumentBatch decodes a binary payload into document entries.
func DecodeDocumentBatch(data []byte) ([]DocumentEntry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("document batch too short: %d bytes", len(data))
	}

	count := binary.LittleEndian.Uint32(data[:4])
	off := 4
	entries := make([]DocumentEntry, 0, count)

	for i := uint32(0); i < count; i++ {
		if off+4 > len(data) {
			return nil, fmt.Errorf("truncated document batch at entry %d", i)
		}
		keyLen := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4

		if off+keyLen > len(data) {
			return nil, fmt.Errorf("truncated key at entry %d", i)
		}
		key := make([]byte, keyLen)
		copy(key, data[off:off+keyLen])
		off += keyLen

		if off+1 > len(data) {
			return nil, fmt.Errorf("truncated value_flags at entry %d", i)
		}
		valueFlags := data[off]
		off++

		if off+4 > len(data) {
			return nil, fmt.Errorf("truncated value_len at entry %d", i)
		}
		valueLen := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4

		if off+valueLen > len(data) {
			return nil, fmt.Errorf("truncated value at entry %d", i)
		}
		value := make([]byte, valueLen)
		copy(value, data[off:off+valueLen])
		off += valueLen

		if off+8 > len(data) {
			return nil, fmt.Errorf("truncated timestamp at entry %d", i)
		}
		ts := binary.LittleEndian.Uint64(data[off:])
		off += 8

		entries = append(entries, DocumentEntry{
			Key:         key,
			ValueFlags:  valueFlags,
			Value:       value,
			TimestampNs: ts,
		})
	}

	return entries, nil
}

// EncodeEmbeddingBatch encodes a batch of embedding entries into a binary payload.
// Layout: [index_name_len:u32] [index_name] [dimension:u16] [count:u32]
//
//	repeated: [doc_key_len:u32] [doc_key] [hash_id:u64] [float32 * dimension]
func EncodeEmbeddingBatch(indexName string, dimension uint16, entries []EmbeddingEntry) []byte {
	size := 4 + len(indexName) + 2 + 4
	for _, e := range entries {
		size += 4 + len(e.DocKey) + 8 + int(dimension)*4
	}

	buf := make([]byte, 0, size)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(indexName)))
	buf = append(buf, indexName...)
	buf = binary.LittleEndian.AppendUint16(buf, dimension)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entries)))

	for _, e := range entries {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.DocKey)))
		buf = append(buf, e.DocKey...)
		buf = binary.LittleEndian.AppendUint64(buf, e.HashID)
		for _, f := range e.Vector {
			buf = appendFloat32LE(buf, f)
		}
	}

	return buf
}

// DecodeEmbeddingBatch decodes a binary payload into embedding batch metadata and entries.
func DecodeEmbeddingBatch(data []byte) (indexName string, dimension uint16, entries []EmbeddingEntry, err error) {
	if len(data) < 4 {
		return "", 0, nil, fmt.Errorf("embedding batch too short")
	}
	off := 0

	nameLen := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4
	if off+nameLen > len(data) {
		return "", 0, nil, fmt.Errorf("truncated index name")
	}
	indexName = string(data[off : off+nameLen])
	off += nameLen

	if off+2 > len(data) {
		return "", 0, nil, fmt.Errorf("truncated dimension")
	}
	dimension = binary.LittleEndian.Uint16(data[off:])
	off += 2

	if off+4 > len(data) {
		return "", 0, nil, fmt.Errorf("truncated entry count")
	}
	count := binary.LittleEndian.Uint32(data[off:])
	off += 4

	entries = make([]EmbeddingEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		if off+4 > len(data) {
			return "", 0, nil, fmt.Errorf("truncated doc_key_len at entry %d", i)
		}
		keyLen := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4

		if off+keyLen > len(data) {
			return "", 0, nil, fmt.Errorf("truncated doc_key at entry %d", i)
		}
		docKey := make([]byte, keyLen)
		copy(docKey, data[off:off+keyLen])
		off += keyLen

		if off+8 > len(data) {
			return "", 0, nil, fmt.Errorf("truncated hash_id at entry %d", i)
		}
		hashID := binary.LittleEndian.Uint64(data[off:])
		off += 8

		vecBytes := int(dimension) * 4
		if off+vecBytes > len(data) {
			return "", 0, nil, fmt.Errorf("truncated vector at entry %d", i)
		}
		vec := make([]float32, dimension)
		for j := range vec {
			vec[j] = readFloat32LE(data[off:])
			off += 4
		}

		entries = append(entries, EmbeddingEntry{
			DocKey: docKey,
			HashID: hashID,
			Vector: vec,
		})
	}

	return indexName, dimension, entries, nil
}

// EncodeSparseBatch encodes a batch of sparse vector entries.
// Layout: [index_name_len:u32] [index_name] [count:u32]
//
//	repeated: [doc_key_len:u32] [doc_key] [hash_id:u64] [nnz:u32] [indices:u32*nnz] [values:f32*nnz]
func EncodeSparseBatch(indexName string, entries []SparseEntry) []byte {
	size := 4 + len(indexName) + 4
	for _, e := range entries {
		size += 4 + len(e.DocKey) + 8 + 4 + len(e.Indices)*4 + len(e.Values)*4
	}

	buf := make([]byte, 0, size)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(indexName)))
	buf = append(buf, indexName...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entries)))

	for _, e := range entries {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.DocKey)))
		buf = append(buf, e.DocKey...)
		buf = binary.LittleEndian.AppendUint64(buf, e.HashID)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.Indices)))
		for _, idx := range e.Indices {
			buf = binary.LittleEndian.AppendUint32(buf, idx)
		}
		for _, v := range e.Values {
			buf = appendFloat32LE(buf, v)
		}
	}

	return buf
}

// DecodeSparseBatch decodes a binary payload into sparse batch metadata and entries.
func DecodeSparseBatch(data []byte) (indexName string, entries []SparseEntry, err error) {
	if len(data) < 4 {
		return "", nil, fmt.Errorf("sparse batch too short")
	}
	off := 0

	nameLen := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4
	if off+nameLen > len(data) {
		return "", nil, fmt.Errorf("truncated index name")
	}
	indexName = string(data[off : off+nameLen])
	off += nameLen

	if off+4 > len(data) {
		return "", nil, fmt.Errorf("truncated entry count")
	}
	count := binary.LittleEndian.Uint32(data[off:])
	off += 4

	entries = make([]SparseEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		if off+4 > len(data) {
			return "", nil, fmt.Errorf("truncated doc_key_len at entry %d", i)
		}
		keyLen := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
		if off+keyLen > len(data) {
			return "", nil, fmt.Errorf("truncated doc_key at entry %d", i)
		}
		docKey := make([]byte, keyLen)
		copy(docKey, data[off:off+keyLen])
		off += keyLen

		if off+8 > len(data) {
			return "", nil, fmt.Errorf("truncated hash_id at entry %d", i)
		}
		hashID := binary.LittleEndian.Uint64(data[off:])
		off += 8

		if off+4 > len(data) {
			return "", nil, fmt.Errorf("truncated nnz at entry %d", i)
		}
		nnz := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4

		if off+nnz*4 > len(data) {
			return "", nil, fmt.Errorf("truncated indices at entry %d", i)
		}
		indices := make([]uint32, nnz)
		for j := range indices {
			indices[j] = binary.LittleEndian.Uint32(data[off:])
			off += 4
		}

		if off+nnz*4 > len(data) {
			return "", nil, fmt.Errorf("truncated values at entry %d", i)
		}
		values := make([]float32, nnz)
		for j := range values {
			values[j] = readFloat32LE(data[off:])
			off += 4
		}

		entries = append(entries, SparseEntry{
			DocKey:  docKey,
			HashID:  hashID,
			Indices: indices,
			Values:  values,
		})
	}

	return indexName, entries, nil
}

// EncodeEdgeBatch encodes a batch of graph edge entries.
// Layout: [index_name_len:u32] [index_name] [count:u32]
//
//	repeated: [src_key_len:u32] [src_key] [tgt_key_len:u32] [tgt_key] [edge_type_len:u32] [edge_type] [value_len:u32] [value]
func EncodeEdgeBatch(indexName string, entries []EdgeEntry) []byte {
	size := 4 + len(indexName) + 4
	for _, e := range entries {
		size += 4 + len(e.SourceKey) + 4 + len(e.TargetKey) + 4 + len(e.EdgeType) + 4 + len(e.Value)
	}

	buf := make([]byte, 0, size)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(indexName)))
	buf = append(buf, indexName...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entries)))

	for _, e := range entries {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.SourceKey)))
		buf = append(buf, e.SourceKey...)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.TargetKey)))
		buf = append(buf, e.TargetKey...)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.EdgeType)))
		buf = append(buf, e.EdgeType...)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.Value)))
		buf = append(buf, e.Value...)
	}

	return buf
}

// DecodeEdgeBatch decodes a binary payload into edge batch metadata and entries.
func DecodeEdgeBatch(data []byte) (indexName string, entries []EdgeEntry, err error) {
	if len(data) < 4 {
		return "", nil, fmt.Errorf("edge batch too short")
	}
	off := 0

	nameLen := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4
	if off+nameLen > len(data) {
		return "", nil, fmt.Errorf("truncated index name")
	}
	indexName = string(data[off : off+nameLen])
	off += nameLen

	if off+4 > len(data) {
		return "", nil, fmt.Errorf("truncated entry count")
	}
	count := binary.LittleEndian.Uint32(data[off:])
	off += 4

	entries = make([]EdgeEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		var e EdgeEntry

		if off+4 > len(data) {
			return "", nil, fmt.Errorf("truncated src_key_len at entry %d", i)
		}
		srcLen := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
		if off+srcLen > len(data) {
			return "", nil, fmt.Errorf("truncated src_key at entry %d", i)
		}
		e.SourceKey = make([]byte, srcLen)
		copy(e.SourceKey, data[off:off+srcLen])
		off += srcLen

		if off+4 > len(data) {
			return "", nil, fmt.Errorf("truncated tgt_key_len at entry %d", i)
		}
		tgtLen := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
		if off+tgtLen > len(data) {
			return "", nil, fmt.Errorf("truncated tgt_key at entry %d", i)
		}
		e.TargetKey = make([]byte, tgtLen)
		copy(e.TargetKey, data[off:off+tgtLen])
		off += tgtLen

		if off+4 > len(data) {
			return "", nil, fmt.Errorf("truncated edge_type_len at entry %d", i)
		}
		etLen := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
		if off+etLen > len(data) {
			return "", nil, fmt.Errorf("truncated edge_type at entry %d", i)
		}
		e.EdgeType = make([]byte, etLen)
		copy(e.EdgeType, data[off:off+etLen])
		off += etLen

		if off+4 > len(data) {
			return "", nil, fmt.Errorf("truncated value_len at entry %d", i)
		}
		valLen := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
		if off+valLen > len(data) {
			return "", nil, fmt.Errorf("truncated value at entry %d", i)
		}
		e.Value = make([]byte, valLen)
		copy(e.Value, data[off:off+valLen])
		off += valLen

		entries = append(entries, e)
	}

	return indexName, entries, nil
}

// EncodeShardHeader encodes a shard header into binary.
func EncodeShardHeader(h ShardHeaderEntry) []byte {
	size := 4 + len(h.TableName) + 4 + 4 + len(h.StartKey) + 4 + len(h.EndKey)
	buf := make([]byte, 0, size)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(h.TableName)))
	buf = append(buf, h.TableName...)
	buf = binary.LittleEndian.AppendUint32(buf, h.ShardID)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(h.StartKey)))
	buf = append(buf, h.StartKey...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(h.EndKey)))
	buf = append(buf, h.EndKey...)
	return buf
}

// DecodeShardHeader decodes a shard header from binary.
func DecodeShardHeader(data []byte) (ShardHeaderEntry, error) {
	if len(data) < 4 {
		return ShardHeaderEntry{}, fmt.Errorf("shard header too short")
	}
	off := 0

	nameLen := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4
	if off+nameLen > len(data) {
		return ShardHeaderEntry{}, fmt.Errorf("truncated table name")
	}
	tableName := string(data[off : off+nameLen])
	off += nameLen

	if off+4 > len(data) {
		return ShardHeaderEntry{}, fmt.Errorf("truncated shard_id")
	}
	shardID := binary.LittleEndian.Uint32(data[off:])
	off += 4

	if off+4 > len(data) {
		return ShardHeaderEntry{}, fmt.Errorf("truncated start_key_len")
	}
	skLen := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4
	if off+skLen > len(data) {
		return ShardHeaderEntry{}, fmt.Errorf("truncated start_key")
	}
	startKey := make([]byte, skLen)
	copy(startKey, data[off:off+skLen])
	off += skLen

	if off+4 > len(data) {
		return ShardHeaderEntry{}, fmt.Errorf("truncated end_key_len")
	}
	ekLen := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4
	if off+ekLen > len(data) {
		return ShardHeaderEntry{}, fmt.Errorf("truncated end_key")
	}
	endKey := make([]byte, ekLen)
	copy(endKey, data[off:off+ekLen])

	return ShardHeaderEntry{
		TableName: tableName,
		ShardID:   shardID,
		StartKey:  startKey,
		EndKey:    endKey,
	}, nil
}

// EncodeShardFooter encodes a shard footer into binary.
func EncodeShardFooter(f ShardFooterEntry) []byte {
	buf := make([]byte, 0, 36)
	buf = binary.LittleEndian.AppendUint32(buf, f.ShardID)
	buf = binary.LittleEndian.AppendUint64(buf, f.DocumentCount)
	buf = binary.LittleEndian.AppendUint64(buf, f.EmbeddingCount)
	buf = binary.LittleEndian.AppendUint64(buf, f.EdgeCount)
	buf = binary.LittleEndian.AppendUint64(buf, f.TransactionCount)
	return buf
}

// DecodeShardFooter decodes a shard footer from binary.
func DecodeShardFooter(data []byte) (ShardFooterEntry, error) {
	if len(data) < 36 {
		return ShardFooterEntry{}, fmt.Errorf("shard footer too short: %d bytes", len(data))
	}
	return ShardFooterEntry{
		ShardID:          binary.LittleEndian.Uint32(data[0:4]),
		DocumentCount:    binary.LittleEndian.Uint64(data[4:12]),
		EmbeddingCount:   binary.LittleEndian.Uint64(data[12:20]),
		EdgeCount:        binary.LittleEndian.Uint64(data[20:28]),
		TransactionCount: binary.LittleEndian.Uint64(data[28:36]),
	}, nil
}

// EncodeFileFooter encodes the file footer into binary.
func EncodeFileFooter(f FileFooterEntry) []byte {
	buf := make([]byte, 0, 24)
	buf = binary.LittleEndian.AppendUint32(buf, f.TableCount)
	buf = binary.LittleEndian.AppendUint32(buf, f.ShardCount)
	buf = binary.LittleEndian.AppendUint64(buf, f.TotalDocuments)
	buf = binary.LittleEndian.AppendUint64(buf, f.TotalBytes)
	return buf
}

// DecodeFileFooter decodes the file footer from binary.
func DecodeFileFooter(data []byte) (FileFooterEntry, error) {
	if len(data) < 24 {
		return FileFooterEntry{}, fmt.Errorf("file footer too short: %d bytes", len(data))
	}
	return FileFooterEntry{
		TableCount:     binary.LittleEndian.Uint32(data[0:4]),
		ShardCount:     binary.LittleEndian.Uint32(data[4:8]),
		TotalDocuments: binary.LittleEndian.Uint64(data[8:16]),
		TotalBytes:     binary.LittleEndian.Uint64(data[16:24]),
	}, nil
}

// --- Entry Types ---

// DocumentEntry represents a single document in a document batch.
type DocumentEntry struct {
	Key         []byte // user-facing document key (no backend encoding)
	ValueFlags  byte   // bit 0: value is zstd-compressed JSON
	Value       []byte // document JSON (possibly compressed)
	TimestampNs uint64 // TTL/transaction timestamp, 0 = none
}

// DocValueFlagCompressed indicates the document value is zstd-compressed.
const DocValueFlagCompressed byte = 1

// EmbeddingEntry represents a single dense embedding vector.
type EmbeddingEntry struct {
	DocKey []byte    // user-facing document key
	HashID uint64    // content hash for dedup
	Vector []float32 // LE float32 array, length = dimension
}

// SparseEntry represents a single sparse vector.
type SparseEntry struct {
	DocKey  []byte    // user-facing document key
	HashID  uint64    // content hash
	Indices []uint32  // non-zero indices
	Values  []float32 // corresponding values
}

// EdgeEntry represents a single outgoing graph edge.
type EdgeEntry struct {
	SourceKey []byte // source document key
	TargetKey []byte // target document key
	EdgeType  []byte // edge type name
	Value     []byte // optional edge payload
}

// ShardHeaderEntry represents shard metadata.
type ShardHeaderEntry struct {
	TableName string
	ShardID   uint32
	StartKey  []byte // inclusive
	EndKey    []byte // exclusive, empty = unbounded
}

// ShardFooterEntry contains counts for verification.
type ShardFooterEntry struct {
	ShardID          uint32
	DocumentCount    uint64
	EmbeddingCount   uint64
	EdgeCount        uint64
	TransactionCount uint64
}

// FileFooterEntry contains final totals.
type FileFooterEntry struct {
	TableCount     uint32
	ShardCount     uint32
	TotalDocuments uint64
	TotalBytes     uint64
}

// --- Internal helpers ---

func isDataBlock(t AFBBlockType) bool {
	return t >= 0x10 && t <= 0x16
}

func appendFloat32LE(buf []byte, f float32) []byte {
	return binary.LittleEndian.AppendUint32(buf, math.Float32bits(f))
}

func readFloat32LE(data []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(data))
}

// IsAFBFormat checks if the first 8 bytes match the AFB magic.
func IsAFBFormat(header []byte) bool {
	if len(header) < 8 {
		return false
	}
	return [8]byte(header[0:8]) == afbMagic
}
