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

package sparseindex

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"unsafe"

	"github.com/ajroetker/go-highway/hwy/contrib/algo"
	"github.com/ajroetker/go-highway/hwy/contrib/quantize"
	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/vector"
)

// chunk is the in-memory representation of a posting list chunk.
type chunk struct {
	DocNums []uint64
	DocIDs  [][]byte // transient: used during batch operations only, not persisted in chunk encoding
	Weights []float32
}

// termMeta stores metadata for a term's posting list.
type termMeta struct {
	MaxWeight  float32
	ChunkCount uint32
}

// fwdEntry is the forward index entry for a document.
type fwdEntry struct {
	DocNum uint64
	Vec    *vector.SparseVector
}

// --- Chunk encoding/decoding ---

// encodeChunk encodes a chunk into the binary format:
//
//	[format_version uint8]
//	[num_entries    uint32]
//	[max_weight     float32]
//	[min_weight     float32]
//	[doc_id_deltas  N × uint32]  -- delta-encoded doc nums (truncated to uint32)
//	[weights        N × uint8]   -- quantized weights
func encodeChunk(c *chunk) ([]byte, error) {
	n := len(c.DocNums)
	if n == 0 {
		return nil, fmt.Errorf("cannot encode empty chunk")
	}
	if len(c.Weights) != n {
		return nil, fmt.Errorf("doc_nums length %d != weights length %d", n, len(c.Weights))
	}

	minW, maxW := vec.MinMax[float32](c.Weights)

	// Prepare delta-encoded doc nums as uint32.
	// Doc nums are uint64 internally but stored as uint32 deltas in the chunk
	// format. This limits the maximum doc num to ~4.3 billion per index.
	docDeltas := make([]uint32, n)
	for i, dn := range c.DocNums {
		if dn > math.MaxUint32 {
			return nil, fmt.Errorf("doc num %d exceeds uint32 range (max %d)", dn, uint32(math.MaxUint32))
		}
		docDeltas[i] = uint32(dn)
	}
	// Sort by doc num (should already be sorted, but be safe)
	// Delta encode: [d0-0, d1-d0, d2-d1, ...]
	algo.DeltaEncode(docDeltas, 0)

	// Quantize weights to uint8
	scale := maxW - minW
	if scale == 0 {
		scale = 1 // avoid division by zero when all weights are equal
	}
	quantizedWeights := make([]uint8, n)
	quantize.QuantizeFloat32(c.Weights, quantizedWeights, minW, scale/255.0)

	// Header: version(1) + num_entries(4) + max_weight(4) + min_weight(4) = 13
	// Data: doc_deltas(4*n) + weights(1*n)
	bufSize := 13 + 4*n + n
	buf := make([]byte, bufSize)
	offset := 0

	buf[offset] = formatVersion
	offset++

	binary.LittleEndian.PutUint32(buf[offset:], uint32(n)) //nolint:gosec // G115: bounded value, cannot overflow in practice
	offset += 4

	binary.LittleEndian.PutUint32(buf[offset:], math.Float32bits(maxW))
	offset += 4

	binary.LittleEndian.PutUint32(buf[offset:], math.Float32bits(minW))
	offset += 4

	vec.EncodeFloat32s(buf[offset:offset+4*n], unsafe.Slice((*float32)(unsafe.Pointer(&docDeltas[0])), n)) //nolint:gosec // G103: intentional unsafe for zero-copy performance
	offset += 4 * n

	copy(buf[offset:], quantizedWeights)

	return buf, nil
}

// decodeChunk decodes a chunk from the binary format.
// Returns absolute doc nums and float32 weights.
func decodeChunk(data []byte) (docNums []uint64, weights []float32, maxW float32, err error) {
	if len(data) < 13 {
		return nil, nil, 0, fmt.Errorf("chunk data too short: %d bytes", len(data))
	}

	version := data[0]
	if version != formatVersion {
		return nil, nil, 0, fmt.Errorf("unsupported chunk format version: %d", version)
	}

	n := int(binary.LittleEndian.Uint32(data[1:5]))
	maxW = math.Float32frombits(binary.LittleEndian.Uint32(data[5:9]))
	minW := math.Float32frombits(binary.LittleEndian.Uint32(data[9:13]))

	expectedSize := 13 + 4*n + n
	if len(data) < expectedSize {
		return nil, nil, 0, fmt.Errorf("chunk data too short for %d entries: need %d, got %d", n, expectedSize, len(data))
	}

	docDeltas := make([]uint32, n)
	offset := 13
	vec.DecodeFloat32s(unsafe.Slice((*float32)(unsafe.Pointer(&docDeltas[0])), n), data[offset:offset+4*n]) //nolint:gosec // G103: intentional unsafe for zero-copy performance
	offset += 4 * n

	algo.DeltaDecode(docDeltas, 0)

	// Convert to uint64
	docNums = make([]uint64, n)
	for i, d := range docDeltas {
		docNums[i] = uint64(d)
	}

	// Dequantize weights
	quantizedWeights := data[offset : offset+n]
	weights = make([]float32, n)
	scale := maxW - minW
	if scale == 0 {
		scale = 1
	}
	quantize.DequantizeUint8(quantizedWeights, weights, minW, scale/255.0)

	return docNums, weights, maxW, nil
}

// --- Forward index encoding/decoding ---

// encodeFwdEntry encodes a forward index entry:
//
//	[doc_num uint64]
//	[num_pairs uint32]
//	[term_ids  N × uint32]  -- sorted by term_id
//	[weights   N × float32]
func encodeFwdEntry(e *fwdEntry) []byte {
	indices := e.Vec.GetIndices()
	values := e.Vec.GetValues()
	n := len(indices)
	// 8 (doc_num) + 4 (num_pairs) + 4*n (term_ids) + 4*n (weights)
	buf := make([]byte, 12+8*n)
	offset := 0

	binary.LittleEndian.PutUint64(buf[offset:], e.DocNum)
	offset += 8

	binary.LittleEndian.PutUint32(buf[offset:], uint32(n)) //nolint:gosec // G115: bounded value, cannot overflow in practice
	offset += 4

	for i := range n {
		binary.LittleEndian.PutUint32(buf[offset:], uint32(indices[i]))
		offset += 4
	}

	for i := range n {
		binary.LittleEndian.PutUint32(buf[offset:], math.Float32bits(values[i]))
		offset += 4
	}

	return buf
}

// decodeFwdEntry decodes a forward index entry.
func decodeFwdEntry(data []byte) (*fwdEntry, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("forward entry data too short: %d bytes", len(data))
	}

	docNum := binary.LittleEndian.Uint64(data[0:8])
	n := int(binary.LittleEndian.Uint32(data[8:12]))

	expectedSize := 12 + 8*n
	if len(data) < expectedSize {
		return nil, fmt.Errorf("forward entry data too short for %d pairs: need %d, got %d", n, expectedSize, len(data))
	}

	indices := make([]uint32, n)
	values := make([]float32, n)

	offset := 12
	for i := range n {
		indices[i] = binary.LittleEndian.Uint32(data[offset:])
		offset += 4
	}
	for i := range n {
		values[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
	}

	return &fwdEntry{
		DocNum: docNum,
		Vec:    vector.NewSparseVector(indices, values),
	}, nil
}

// --- Term metadata encoding/decoding ---

// encodeTermMeta encodes term metadata:
//
//	[max_weight float32]
//	[chunk_count uint32]
func encodeTermMeta(tm *termMeta) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[0:4], math.Float32bits(tm.MaxWeight))
	binary.LittleEndian.PutUint32(buf[4:8], tm.ChunkCount)
	return buf
}

// decodeTermMeta decodes term metadata.
func decodeTermMeta(data []byte) (*termMeta, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("term meta data too short: %d bytes", len(data))
	}
	return &termMeta{
		MaxWeight:  math.Float32frombits(binary.LittleEndian.Uint32(data[0:4])),
		ChunkCount: binary.LittleEndian.Uint32(data[4:8]),
	}, nil
}

// --- Merge operand encoding/decoding ---
//
// Merge operands use version byte 2 to distinguish from full chunks (version 1).
// Format:
//
//	[version     uint8  = 2]
//	[op_type     uint8]         0x01 = add, 0x02 = delete
//	[num_entries uint32]
//	[doc_nums    N × uint32]    absolute (NOT delta-encoded)
//	[weights     N × float32]   raw (NOT quantized); zero-length for deletes

const (
	mergeOperandVersion uint8 = 2
	opTypeAdd           uint8 = 0x01
	opTypeDelete        uint8 = 0x02
)

// encodeMergeOperand encodes a merge operand for chunk keys.
// For add operands, docNums and weights must have the same length.
// For delete operands, weights is ignored.
func encodeMergeOperand(opType uint8, docNums []uint32, weights []float32) []byte {
	n := len(docNums)
	weightBytes := 0
	if opType == opTypeAdd {
		weightBytes = 4 * n
	}
	buf := make([]byte, 6+4*n+weightBytes)
	buf[0] = mergeOperandVersion
	buf[1] = opType
	binary.LittleEndian.PutUint32(buf[2:6], uint32(n)) //nolint:gosec // G115: bounded value, cannot overflow in practice

	offset := 6
	for _, dn := range docNums {
		binary.LittleEndian.PutUint32(buf[offset:], dn)
		offset += 4
	}
	if opType == opTypeAdd {
		for _, w := range weights {
			binary.LittleEndian.PutUint32(buf[offset:], math.Float32bits(w))
			offset += 4
		}
	}
	return buf
}

// decodeMergeOperand decodes a merge operand.
func decodeMergeOperand(data []byte) (opType uint8, docNums []uint32, weights []float32, err error) {
	if len(data) < 6 {
		return 0, nil, nil, fmt.Errorf("merge operand too short: %d bytes", len(data))
	}
	if data[0] != mergeOperandVersion {
		return 0, nil, nil, fmt.Errorf("unexpected merge operand version: %d", data[0])
	}
	opType = data[1]
	n := int(binary.LittleEndian.Uint32(data[2:6]))

	expectedSize := 6 + 4*n
	if opType == opTypeAdd {
		expectedSize += 4 * n
	}
	if len(data) < expectedSize {
		return 0, nil, nil, fmt.Errorf("merge operand too short for %d entries: need %d, got %d", n, expectedSize, len(data))
	}

	docNums = make([]uint32, n)
	offset := 6
	for i := range n {
		docNums[i] = binary.LittleEndian.Uint32(data[offset:])
		offset += 4
	}

	if opType == opTypeAdd {
		weights = make([]float32, n)
		for i := range n {
			weights[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[offset:]))
			offset += 4
		}
	}
	return opType, docNums, weights, nil
}

// --- Utility functions ---

func encodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, v)
	return buf
}

// decodeUint64 decodes a little-endian uint64. Caller must ensure len(data) >= 8.
func decodeUint64(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

// sortSparseVec ensures the sparse vector is sorted by term index.
func sortSparseVec(v *vector.SparseVector) {
	indices := v.GetIndices()
	values := v.GetValues()
	if len(indices) <= 1 {
		return
	}
	// Check if already sorted
	sorted := true
	for i := 1; i < len(indices); i++ {
		if indices[i] < indices[i-1] {
			sorted = false
			break
		}
	}
	if sorted {
		return
	}
	// Build index pairs and sort
	type pair struct {
		idx uint32
		val float32
	}
	pairs := make([]pair, len(indices))
	for i := range indices {
		pairs[i] = pair{indices[i], values[i]}
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].idx < pairs[j].idx
	})
	for i := range pairs {
		indices[i] = pairs[i].idx
		values[i] = pairs[i].val
	}
}
