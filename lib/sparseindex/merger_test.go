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
	"math"
	"testing"

	"github.com/antflydb/antfly/lib/pebbleutils"
)

func TestMergeOperandRoundTrip(t *testing.T) {
	// Add operand
	docNums := []uint32{10, 20, 30}
	weights := []float32{0.5, 1.0, 0.75}
	data := encodeMergeOperand(opTypeAdd, docNums, weights)

	opType, gotDN, gotW, err := decodeMergeOperand(data)
	if err != nil {
		t.Fatal(err)
	}
	if opType != opTypeAdd {
		t.Errorf("expected opTypeAdd, got %d", opType)
	}
	if len(gotDN) != 3 || gotDN[0] != 10 || gotDN[1] != 20 || gotDN[2] != 30 {
		t.Errorf("unexpected docNums: %v", gotDN)
	}
	for i, w := range weights {
		if math.Abs(float64(gotW[i]-w)) > 1e-6 {
			t.Errorf("weight[%d]: expected %f, got %f", i, w, gotW[i])
		}
	}

	// Delete operand
	data = encodeMergeOperand(opTypeDelete, docNums, nil)
	opType, gotDN, gotW, err = decodeMergeOperand(data)
	if err != nil {
		t.Fatal(err)
	}
	if opType != opTypeDelete {
		t.Errorf("expected opTypeDelete, got %d", opType)
	}
	if len(gotDN) != 3 {
		t.Errorf("expected 3 docNums, got %d", len(gotDN))
	}
	if gotW != nil {
		t.Errorf("expected nil weights for delete, got %v", gotW)
	}
}

func TestChunkValueMergerSingleAdd(t *testing.T) {
	operand := encodeMergeOperand(opTypeAdd, []uint32{5, 10, 15}, []float32{0.3, 0.7, 0.5})
	vm, err := newChunkValueMerger(operand)
	if err != nil {
		t.Fatal(err)
	}

	data, _, err := vm.Finish(true)
	if err != nil {
		t.Fatal(err)
	}

	// Should produce a valid v1 chunk
	docNums, weights, _, err := decodeChunk(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(docNums) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(docNums))
	}
	if docNums[0] != 5 || docNums[1] != 10 || docNums[2] != 15 {
		t.Errorf("unexpected docNums: %v", docNums)
	}
	// Weights are quantized, so check with tolerance
	for i, expected := range []float32{0.3, 0.7, 0.5} {
		if math.Abs(float64(weights[i]-expected)) > 0.05 {
			t.Errorf("weight[%d]: expected ~%f, got %f", i, expected, weights[i])
		}
	}
}

func TestChunkValueMergerMultipleAdds(t *testing.T) {
	op1 := encodeMergeOperand(opTypeAdd, []uint32{5, 15}, []float32{0.3, 0.5})
	op2 := encodeMergeOperand(opTypeAdd, []uint32{10, 20}, []float32{0.7, 0.9})

	vm, err := newChunkValueMerger(op1)
	if err != nil {
		t.Fatal(err)
	}
	if err := vm.MergeNewer(op2); err != nil {
		t.Fatal(err)
	}

	data, _, err := vm.Finish(true)
	if err != nil {
		t.Fatal(err)
	}

	docNums, _, _, err := decodeChunk(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(docNums) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(docNums))
	}
	// Should be sorted
	for i := 1; i < len(docNums); i++ {
		if docNums[i] <= docNums[i-1] {
			t.Errorf("docNums not sorted: %v", docNums)
			break
		}
	}
}

func TestChunkValueMergerAddThenDelete(t *testing.T) {
	addOp := encodeMergeOperand(opTypeAdd, []uint32{5, 10, 15, 20}, []float32{0.3, 0.7, 0.5, 0.9})
	delOp := encodeMergeOperand(opTypeDelete, []uint32{10, 20}, nil)

	vm, err := newChunkValueMerger(addOp)
	if err != nil {
		t.Fatal(err)
	}
	if err := vm.MergeNewer(delOp); err != nil {
		t.Fatal(err)
	}

	data, _, err := vm.Finish(true)
	if err != nil {
		t.Fatal(err)
	}

	docNums, _, _, err := decodeChunk(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(docNums) != 2 {
		t.Fatalf("expected 2 entries after delete, got %d", len(docNums))
	}
	if docNums[0] != 5 || docNums[1] != 15 {
		t.Errorf("unexpected docNums after delete: %v", docNums)
	}
}

func TestChunkValueMergerDeleteAll(t *testing.T) {
	addOp := encodeMergeOperand(opTypeAdd, []uint32{5, 10}, []float32{0.3, 0.7})
	delOp := encodeMergeOperand(opTypeDelete, []uint32{5, 10}, nil)

	vm, err := newChunkValueMerger(addOp)
	if err != nil {
		t.Fatal(err)
	}
	if err := vm.MergeNewer(delOp); err != nil {
		t.Fatal(err)
	}

	_, del, _, err := vm.DeletableFinish(true)
	if err != nil {
		t.Fatal(err)
	}
	if !del {
		t.Error("expected DeletableFinish to return delete=true when all entries deleted")
	}
}

func TestChunkValueMergerWithBase(t *testing.T) {
	// Create a base chunk via encodeChunk (v1 format)
	baseChunk := &chunk{
		DocNums: []uint64{1, 2, 3},
		Weights: []float32{0.1, 0.2, 0.3},
	}
	baseData, err := encodeChunk(baseChunk)
	if err != nil {
		t.Fatal(err)
	}

	// Create merger with base chunk, then merge in new entries
	vm, err := newChunkValueMerger(baseData)
	if err != nil {
		t.Fatal(err)
	}

	addOp := encodeMergeOperand(opTypeAdd, []uint32{4, 5}, []float32{0.4, 0.5})
	if err := vm.MergeNewer(addOp); err != nil {
		t.Fatal(err)
	}

	data, _, err := vm.Finish(true)
	if err != nil {
		t.Fatal(err)
	}

	docNums, _, _, err := decodeChunk(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(docNums) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(docNums))
	}
	for i, expected := range []uint64{1, 2, 3, 4, 5} {
		if docNums[i] != expected {
			t.Errorf("docNums[%d]: expected %d, got %d", i, expected, docNums[i])
		}
	}
}

func TestChunkValueMergerPartialCompaction(t *testing.T) {
	op1 := encodeMergeOperand(opTypeAdd, []uint32{5, 15}, []float32{0.3, 0.5})
	op2 := encodeMergeOperand(opTypeAdd, []uint32{10}, []float32{0.7})

	vm, err := newChunkValueMerger(op1)
	if err != nil {
		t.Fatal(err)
	}
	if err := vm.MergeNewer(op2); err != nil {
		t.Fatal(err)
	}

	// Partial compaction — includesBase=false
	data, _, err := vm.Finish(false)
	if err != nil {
		t.Fatal(err)
	}

	// Result should be a v2 operand (not a v1 chunk)
	if len(data) == 0 {
		t.Fatal("expected non-empty partial result")
	}
	if data[0] != mergeOperandVersion {
		t.Errorf("expected version %d for partial result, got %d", mergeOperandVersion, data[0])
	}

	// The partial result should be usable as input to another merge
	vm2, err := newChunkValueMerger(data)
	if err != nil {
		t.Fatal(err)
	}
	finalData, _, err := vm2.Finish(true)
	if err != nil {
		t.Fatal(err)
	}
	docNums, _, _, err := decodeChunk(finalData)
	if err != nil {
		t.Fatal(err)
	}
	if len(docNums) != 3 {
		t.Fatalf("expected 3 entries from partial→full, got %d", len(docNums))
	}
}

func TestChunkValueMergerAssociativity(t *testing.T) {
	opA := encodeMergeOperand(opTypeAdd, []uint32{1, 3}, []float32{0.1, 0.3})
	opB := encodeMergeOperand(opTypeAdd, []uint32{2, 4}, []float32{0.2, 0.4})

	// Order 1: Merge(A).MergeNewer(B)
	vm1, err := newChunkValueMerger(opA)
	if err != nil {
		t.Fatal(err)
	}
	if err := vm1.MergeNewer(opB); err != nil {
		t.Fatal(err)
	}
	data1, _, err := vm1.Finish(true)
	if err != nil {
		t.Fatal(err)
	}

	// Order 2: Merge(B).MergeNewer(A)
	vm2, err := newChunkValueMerger(opB)
	if err != nil {
		t.Fatal(err)
	}
	if err := vm2.MergeNewer(opA); err != nil {
		t.Fatal(err)
	}
	data2, _, err := vm2.Finish(true)
	if err != nil {
		t.Fatal(err)
	}

	// Both should produce the same chunk
	dn1, w1, _, _ := decodeChunk(data1)
	dn2, w2, _, _ := decodeChunk(data2)

	if len(dn1) != len(dn2) {
		t.Fatalf("different lengths: %d vs %d", len(dn1), len(dn2))
	}
	for i := range dn1 {
		if dn1[i] != dn2[i] {
			t.Errorf("docNum[%d]: %d vs %d", i, dn1[i], dn2[i])
		}
		if math.Abs(float64(w1[i]-w2[i])) > 1e-6 {
			t.Errorf("weight[%d]: %f vs %f", i, w1[i], w2[i])
		}
	}
}

func TestRegisterChunkMerger(t *testing.T) {
	reg := pebbleutils.NewRegistry()
	RegisterChunkMerger(reg, []byte("test:"))

	merger := reg.NewMerger("test.v1")

	// Chunk key should get chunk merger
	op := encodeMergeOperand(opTypeAdd, []uint32{1}, []float32{0.5})
	vm, err := merger.Merge([]byte("test:inv:42:chunk0"), op)
	if err != nil {
		t.Fatal(err)
	}
	// Should be a chunkValueMerger
	if _, ok := vm.(*chunkValueMerger); !ok {
		t.Errorf("expected *chunkValueMerger for chunk key, got %T", vm)
	}

	// Meta key should get last-write-wins
	vm, err = merger.Merge([]byte("test:inv:42:meta"), []byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := vm.(*pebbleutils.LastWriteWinsMerger); !ok {
		t.Errorf("expected *LastWriteWinsMerger for meta key, got %T", vm)
	}

	// Non-inv key should get last-write-wins (registry fallback)
	vm, err = merger.Merge([]byte("test:fwd:doc1"), []byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := vm.(*pebbleutils.LastWriteWinsMerger); !ok {
		t.Errorf("expected *LastWriteWinsMerger for fwd key, got %T", vm)
	}
}
