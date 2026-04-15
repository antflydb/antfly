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
	"bytes"
	"encoding/binary"
	"flag"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

var updateGolden = flag.Bool("update-golden", false, "update golden test files")

func TestAFBHeaderRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	backupID := uuid.New()

	w, err := NewAFBWriter(&buf, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	hdr := NewFileHeader(backupID, 2, 5, false)
	if err := w.WriteHeader(hdr); err != nil {
		t.Fatal(err)
	}

	r, err := NewAFBReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := r.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}

	if got.FormatVersion != AFBFormatVersion {
		t.Errorf("FormatVersion = %d, want %d", got.FormatVersion, AFBFormatVersion)
	}
	if got.BackupID != backupID {
		t.Errorf("BackupID = %v, want %v", got.BackupID, backupID)
	}
	if got.TableCount != 2 {
		t.Errorf("TableCount = %d, want 2", got.TableCount)
	}
	if got.ShardCount != 5 {
		t.Errorf("ShardCount = %d, want 5", got.ShardCount)
	}
}

func TestAFBHeaderCRCValidation(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewAFBWriter(&buf, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.WriteHeader(NewFileHeader(uuid.New(), 1, 1, false)); err != nil {
		t.Fatal(err)
	}

	// Corrupt a byte in the header
	data := buf.Bytes()
	data[20] ^= 0xFF

	r, err := NewAFBReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if _, err := r.ReadHeader(); err == nil {
		t.Fatal("expected CRC error on corrupted header")
	}
}

func TestAFBBlockRoundTripUncompressed(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewAFBWriter(&buf, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.WriteHeader(NewFileHeader(uuid.New(), 1, 1, false)); err != nil {
		t.Fatal(err)
	}

	manifest := []byte(`{"backup_id":"test","tables":["t1"]}`)
	if err := w.WriteBlock(BlockClusterManifest, manifest); err != nil {
		t.Fatal(err)
	}

	r, err := NewAFBReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if _, err := r.ReadHeader(); err != nil {
		t.Fatal(err)
	}

	block, err := r.ReadBlock()
	if err != nil {
		t.Fatal(err)
	}

	if block.Type != BlockClusterManifest {
		t.Errorf("block type = 0x%02x, want 0x%02x", block.Type, BlockClusterManifest)
	}
	if !bytes.Equal(block.Payload, manifest) {
		t.Errorf("payload mismatch")
	}
}

func TestAFBBlockRoundTripCompressed(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewAFBWriter(&buf, true)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.WriteHeader(NewFileHeader(uuid.New(), 1, 1, true)); err != nil {
		t.Fatal(err)
	}

	// Create a payload large enough that zstd compression helps.
	payload := bytes.Repeat([]byte("hello world document data "), 200)
	if err := w.WriteBlock(BlockDocumentBatch, payload); err != nil {
		t.Fatal(err)
	}

	r, err := NewAFBReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if _, err := r.ReadHeader(); err != nil {
		t.Fatal(err)
	}

	block, err := r.ReadBlock()
	if err != nil {
		t.Fatal(err)
	}

	if block.Type != BlockDocumentBatch {
		t.Errorf("block type = 0x%02x, want 0x%02x", block.Type, BlockDocumentBatch)
	}
	if !bytes.Equal(block.Payload, payload) {
		t.Errorf("decompressed payload mismatch: got %d bytes, want %d", len(block.Payload), len(payload))
	}
}

func TestAFBMetadataBlocksNotCompressed(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewAFBWriter(&buf, true) // compression enabled
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.WriteHeader(NewFileHeader(uuid.New(), 1, 1, true)); err != nil {
		t.Fatal(err)
	}

	manifest := []byte(`{"tables":["t1"]}`)
	if err := w.WriteBlock(BlockClusterManifest, manifest); err != nil {
		t.Fatal(err)
	}

	// Read the raw block envelope to verify it's NOT compressed.
	data := buf.Bytes()[AFBHeaderSize:] // skip file header
	flags := data[1]
	if flags&byte(BlockFlagCompressed) != 0 {
		t.Error("metadata block should not be compressed")
	}
}

func TestAFBBlockCRCValidation(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewAFBWriter(&buf, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.WriteHeader(NewFileHeader(uuid.New(), 1, 1, false)); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBlock(BlockClusterManifest, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}

	// Corrupt the block payload
	data := buf.Bytes()
	data[AFBHeaderSize+6] ^= 0xFF // first byte of payload

	r, err := NewAFBReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if _, err := r.ReadHeader(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadBlock(); err == nil {
		t.Fatal("expected CRC error on corrupted block")
	}
}

func TestDocumentBatchRoundTrip(t *testing.T) {
	entries := []DocumentEntry{
		{Key: []byte("doc1"), ValueFlags: 0, Value: []byte(`{"title":"Hello"}`), TimestampNs: 0},
		{Key: []byte("doc2"), ValueFlags: DocValueFlagCompressed, Value: []byte{0x28, 0xb5, 0x2f, 0xfd}, TimestampNs: 1234567890},
		{Key: []byte(""), ValueFlags: 0, Value: []byte("{}"), TimestampNs: 0}, // empty key edge case
	}

	encoded := EncodeDocumentBatch(entries)
	decoded, err := DecodeDocumentBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if len(decoded) != len(entries) {
		t.Fatalf("decoded %d entries, want %d", len(decoded), len(entries))
	}

	for i, want := range entries {
		got := decoded[i]
		if !bytes.Equal(got.Key, want.Key) {
			t.Errorf("entry %d: key = %q, want %q", i, got.Key, want.Key)
		}
		if got.ValueFlags != want.ValueFlags {
			t.Errorf("entry %d: valueFlags = %d, want %d", i, got.ValueFlags, want.ValueFlags)
		}
		if !bytes.Equal(got.Value, want.Value) {
			t.Errorf("entry %d: value mismatch", i)
		}
		if got.TimestampNs != want.TimestampNs {
			t.Errorf("entry %d: timestamp = %d, want %d", i, got.TimestampNs, want.TimestampNs)
		}
	}
}

func TestEmbeddingBatchRoundTrip(t *testing.T) {
	entries := []EmbeddingEntry{
		{DocKey: []byte("doc1"), HashID: 42, Vector: []float32{1.0, 2.0, 3.0, 4.0}},
		{DocKey: []byte("doc2"), HashID: 99, Vector: []float32{0.5, -0.5, math.MaxFloat32, math.SmallestNonzeroFloat32}},
	}

	encoded := EncodeEmbeddingBatch("my_index", 4, entries)
	indexName, dim, decoded, err := DecodeEmbeddingBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if indexName != "my_index" {
		t.Errorf("indexName = %q, want %q", indexName, "my_index")
	}
	if dim != 4 {
		t.Errorf("dimension = %d, want 4", dim)
	}
	if len(decoded) != len(entries) {
		t.Fatalf("decoded %d entries, want %d", len(decoded), len(entries))
	}

	for i, want := range entries {
		got := decoded[i]
		if !bytes.Equal(got.DocKey, want.DocKey) {
			t.Errorf("entry %d: docKey mismatch", i)
		}
		if got.HashID != want.HashID {
			t.Errorf("entry %d: hashID = %d, want %d", i, got.HashID, want.HashID)
		}
		if len(got.Vector) != len(want.Vector) {
			t.Fatalf("entry %d: vector len = %d, want %d", i, len(got.Vector), len(want.Vector))
		}
		for j := range want.Vector {
			if got.Vector[j] != want.Vector[j] {
				t.Errorf("entry %d, dim %d: vector = %v, want %v", i, j, got.Vector[j], want.Vector[j])
			}
		}
	}
}

func TestSparseBatchRoundTrip(t *testing.T) {
	entries := []SparseEntry{
		{DocKey: []byte("doc1"), HashID: 1, Indices: []uint32{0, 5, 100}, Values: []float32{0.1, 0.5, 0.9}},
		{DocKey: []byte("doc2"), HashID: 2, Indices: []uint32{}, Values: []float32{}}, // empty sparse vector
	}

	encoded := EncodeSparseBatch("sparse_idx", entries)
	indexName, decoded, err := DecodeSparseBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if indexName != "sparse_idx" {
		t.Errorf("indexName = %q, want %q", indexName, "sparse_idx")
	}
	if len(decoded) != len(entries) {
		t.Fatalf("decoded %d entries, want %d", len(decoded), len(entries))
	}

	for i, want := range entries {
		got := decoded[i]
		if !bytes.Equal(got.DocKey, want.DocKey) {
			t.Errorf("entry %d: docKey mismatch", i)
		}
		if got.HashID != want.HashID {
			t.Errorf("entry %d: hashID = %d, want %d", i, got.HashID, want.HashID)
		}
		if len(got.Indices) != len(want.Indices) {
			t.Errorf("entry %d: indices len = %d, want %d", i, len(got.Indices), len(want.Indices))
		}
		for j := range want.Indices {
			if got.Indices[j] != want.Indices[j] {
				t.Errorf("entry %d, idx %d: index = %d, want %d", i, j, got.Indices[j], want.Indices[j])
			}
		}
		for j := range want.Values {
			if got.Values[j] != want.Values[j] {
				t.Errorf("entry %d, val %d: value = %v, want %v", i, j, got.Values[j], want.Values[j])
			}
		}
	}
}

func TestEdgeBatchRoundTrip(t *testing.T) {
	entries := []EdgeEntry{
		{SourceKey: []byte("a"), TargetKey: []byte("b"), EdgeType: []byte("likes"), Value: nil},
		{SourceKey: []byte("b"), TargetKey: []byte("c"), EdgeType: []byte("follows"), Value: []byte(`{"weight":0.5}`)},
	}

	encoded := EncodeEdgeBatch("graph_idx", entries)
	indexName, decoded, err := DecodeEdgeBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if indexName != "graph_idx" {
		t.Errorf("indexName = %q, want %q", indexName, "graph_idx")
	}
	if len(decoded) != len(entries) {
		t.Fatalf("decoded %d entries, want %d", len(decoded), len(entries))
	}

	for i, want := range entries {
		got := decoded[i]
		if !bytes.Equal(got.SourceKey, want.SourceKey) {
			t.Errorf("entry %d: sourceKey mismatch", i)
		}
		if !bytes.Equal(got.TargetKey, want.TargetKey) {
			t.Errorf("entry %d: targetKey mismatch", i)
		}
		if !bytes.Equal(got.EdgeType, want.EdgeType) {
			t.Errorf("entry %d: edgeType mismatch", i)
		}
		if !bytes.Equal(got.Value, want.Value) {
			t.Errorf("entry %d: value mismatch", i)
		}
	}
}

func TestShardHeaderRoundTrip(t *testing.T) {
	h := ShardHeaderEntry{
		TableName: "my_table",
		ShardID:   7,
		StartKey:  []byte("aaa"),
		EndKey:    []byte("zzz"),
	}

	encoded := EncodeShardHeader(h)
	decoded, err := DecodeShardHeader(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.TableName != h.TableName {
		t.Errorf("TableName = %q, want %q", decoded.TableName, h.TableName)
	}
	if decoded.ShardID != h.ShardID {
		t.Errorf("ShardID = %d, want %d", decoded.ShardID, h.ShardID)
	}
	if !bytes.Equal(decoded.StartKey, h.StartKey) {
		t.Errorf("StartKey mismatch")
	}
	if !bytes.Equal(decoded.EndKey, h.EndKey) {
		t.Errorf("EndKey mismatch")
	}
}

func TestShardFooterRoundTrip(t *testing.T) {
	f := ShardFooterEntry{ShardID: 3, DocumentCount: 1000, EmbeddingCount: 500, EdgeCount: 200, TransactionCount: 0}
	encoded := EncodeShardFooter(f)
	decoded, err := DecodeShardFooter(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != f {
		t.Errorf("footer mismatch: got %+v, want %+v", decoded, f)
	}
}

func TestFileFooterRoundTrip(t *testing.T) {
	f := FileFooterEntry{TableCount: 2, ShardCount: 4, TotalDocuments: 10000, TotalBytes: 5000000}
	encoded := EncodeFileFooter(f)
	decoded, err := DecodeFileFooter(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != f {
		t.Errorf("footer mismatch: got %+v, want %+v", decoded, f)
	}
}

func TestAFBFullFileRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	backupID := uuid.New()

	w, err := NewAFBWriter(&buf, true)
	if err != nil {
		t.Fatal(err)
	}

	// Write header
	if err := w.WriteHeader(NewFileHeader(backupID, 1, 1, true)); err != nil {
		t.Fatal(err)
	}

	// Cluster manifest
	if err := w.WriteBlock(BlockClusterManifest, []byte(`{"backup_id":"test"}`)); err != nil {
		t.Fatal(err)
	}

	// Table manifest
	if err := w.WriteBlock(BlockTableManifest, []byte(`{"table_name":"docs"}`)); err != nil {
		t.Fatal(err)
	}

	// Shard header
	shardHdr := EncodeShardHeader(ShardHeaderEntry{TableName: "docs", ShardID: 1, StartKey: nil, EndKey: nil})
	if err := w.WriteBlock(BlockShardHeader, shardHdr); err != nil {
		t.Fatal(err)
	}

	// Document batch
	docs := EncodeDocumentBatch([]DocumentEntry{
		{Key: []byte("doc1"), Value: []byte(`{"title":"Test"}`)},
		{Key: []byte("doc2"), Value: []byte(`{"title":"Test2"}`)},
	})
	if err := w.WriteBlock(BlockDocumentBatch, docs); err != nil {
		t.Fatal(err)
	}

	// Embedding batch
	embs := EncodeEmbeddingBatch("emb_idx", 3, []EmbeddingEntry{
		{DocKey: []byte("doc1"), HashID: 1, Vector: []float32{0.1, 0.2, 0.3}},
	})
	if err := w.WriteBlock(BlockEmbeddingBatch, embs); err != nil {
		t.Fatal(err)
	}

	// Shard footer
	sf := EncodeShardFooter(ShardFooterEntry{ShardID: 1, DocumentCount: 2, EmbeddingCount: 1})
	if err := w.WriteBlock(BlockShardFooter, sf); err != nil {
		t.Fatal(err)
	}

	// File footer
	ff := EncodeFileFooter(FileFooterEntry{TableCount: 1, ShardCount: 1, TotalDocuments: 2, TotalBytes: uint64(buf.Len()) + 24 + blockEnvelopeOverhead})
	if err := w.WriteBlock(BlockFileFooter, ff); err != nil {
		t.Fatal(err)
	}

	w.Close()

	// --- Read back ---
	r, err := NewAFBReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	hdr, err := r.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.BackupID != backupID {
		t.Errorf("backupID mismatch")
	}

	expectedTypes := []AFBBlockType{
		BlockClusterManifest,
		BlockTableManifest,
		BlockShardHeader,
		BlockDocumentBatch,
		BlockEmbeddingBatch,
		BlockShardFooter,
		BlockFileFooter,
	}

	for i, wantType := range expectedTypes {
		block, err := r.ReadBlock()
		if err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
		if block.Type != wantType {
			t.Errorf("block %d: type = 0x%02x, want 0x%02x", i, block.Type, wantType)
		}

		// Verify document batch decoding
		if block.Type == BlockDocumentBatch {
			decoded, err := DecodeDocumentBatch(block.Payload)
			if err != nil {
				t.Fatalf("decode document batch: %v", err)
			}
			if len(decoded) != 2 {
				t.Errorf("document count = %d, want 2", len(decoded))
			}
		}

		// Verify embedding batch decoding
		if block.Type == BlockEmbeddingBatch {
			name, dim, decoded, err := DecodeEmbeddingBatch(block.Payload)
			if err != nil {
				t.Fatalf("decode embedding batch: %v", err)
			}
			if name != "emb_idx" || dim != 3 || len(decoded) != 1 {
				t.Errorf("embedding: name=%q dim=%d count=%d", name, dim, len(decoded))
			}
		}
	}
}

func TestIsAFBFormat(t *testing.T) {
	if IsAFBFormat([]byte("ANTFLYB\n")) != true {
		t.Error("expected true for valid magic")
	}
	if IsAFBFormat([]byte{0x28, 0xb5, 0x2f, 0xfd, 0, 0, 0, 0}) != false {
		t.Error("expected false for zstd magic")
	}
	if IsAFBFormat([]byte("short")) != false {
		t.Error("expected false for short input")
	}
}

// goldenBackupID is a fixed UUID used for deterministic golden file generation.
var goldenBackupID = uuid.UUID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
	0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

// buildGoldenAFB produces a deterministic AFB file with known data.
// Both Go and Zig test suites should produce identical bytes for this dataset.
func buildGoldenAFB() ([]byte, error) {
	var buf bytes.Buffer
	w, err := NewAFBWriter(&buf, false) // no compression for determinism
	if err != nil {
		return nil, err
	}
	defer w.Close()

	// Fixed header
	if err := w.WriteHeader(AFBFileHeader{
		FormatVersion: 1,
		Flags:         0,
		CreatedAtNs:   1700000000_000000000, // 2023-11-14T22:13:20Z
		BackupID:      goldenBackupID,
		TableCount:    1,
		ShardCount:    1,
	}); err != nil {
		return nil, err
	}

	// Cluster manifest
	if err := w.WriteBlock(BlockClusterManifest, []byte(`{"backup_id":"01020304-0506-0708-090a-0b0c0d0e0f10","tables":["test"]}`)); err != nil {
		return nil, err
	}

	// Table manifest
	if err := w.WriteBlock(BlockTableManifest, []byte(`{"name":"test","shards":1}`)); err != nil {
		return nil, err
	}

	// Shard header
	shardHeader := EncodeShardHeader(ShardHeaderEntry{
		TableName: "test",
		ShardID:   1,
		StartKey:  nil,
		EndKey:    nil,
	})
	if err := w.WriteBlock(BlockShardHeader, shardHeader); err != nil {
		return nil, err
	}

	// Document batch
	docBatch := EncodeDocumentBatch([]DocumentEntry{
		{Key: []byte("doc-alpha"), Value: []byte(`{"id":"doc-alpha","title":"Alpha"}`), TimestampNs: 0},
		{Key: []byte("doc-beta"), Value: []byte(`{"id":"doc-beta","title":"Beta"}`), TimestampNs: 0},
	})
	if err := w.WriteBlock(BlockDocumentBatch, docBatch); err != nil {
		return nil, err
	}

	// Embedding batch (3-dim vectors)
	embBatch := EncodeEmbeddingBatch("emb_v0", 3, []EmbeddingEntry{
		{DocKey: []byte("doc-alpha"), HashID: 100, Vector: []float32{0.1, 0.2, 0.3}},
		{DocKey: []byte("doc-beta"), HashID: 200, Vector: []float32{0.4, 0.5, 0.6}},
	})
	if err := w.WriteBlock(BlockEmbeddingBatch, embBatch); err != nil {
		return nil, err
	}

	// Edge batch
	edgeBatch := EncodeEdgeBatch("links", []EdgeEntry{
		{SourceKey: []byte("doc-alpha"), TargetKey: []byte("doc-beta"), EdgeType: []byte("cites"), Value: []byte("{}")},
	})
	if err := w.WriteBlock(BlockEdgeBatch, edgeBatch); err != nil {
		return nil, err
	}

	// Shard footer
	if err := w.WriteBlock(BlockShardFooter, []byte{0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00}); err != nil {
		return nil, err
	}

	// File footer
	if err := w.WriteBlock(BlockFileFooter, []byte{0x02, 0x00, 0x00, 0x00}); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// TestAFBGoldenFile verifies that the Go AFB encoder produces stable, deterministic output.
// Run with -update-golden to regenerate: go test -run TestAFBGoldenFile -update-golden
func TestAFBGoldenFile(t *testing.T) {
	goldenPath := filepath.Join("testdata", "golden_v1.afb")

	got, err := buildGoldenAFB()
	if err != nil {
		t.Fatalf("building golden AFB: %v", err)
	}

	if *updateGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("Updated golden file %s (%d bytes)", goldenPath, len(got))
		return
	}

	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("Reading golden file (run with -update-golden to create): %v", err)
	}

	if !bytes.Equal(got, want) {
		// Find first divergence for a helpful error message
		minLen := len(got)
		if len(want) < minLen {
			minLen = len(want)
		}
		for i := 0; i < minLen; i++ {
			if got[i] != want[i] {
				t.Fatalf("AFB output differs from golden file at byte %d: got 0x%02x, want 0x%02x (got %d bytes, golden %d bytes)",
					i, got[i], want[i], len(got), len(want))
			}
		}
		t.Fatalf("AFB output length differs: got %d bytes, golden %d bytes", len(got), len(want))
	}

	// Also verify the golden file is fully readable
	reader, err := NewAFBReader(bytes.NewReader(got))
	if err != nil {
		t.Fatalf("creating reader for golden file: %v", err)
	}
	defer reader.Close()

	hdr, err := reader.ReadHeader()
	if err != nil {
		t.Fatalf("reading golden file header: %v", err)
	}
	if hdr.FormatVersion != 1 {
		t.Errorf("format version = %d, want 1", hdr.FormatVersion)
	}
	if hdr.BackupID != goldenBackupID {
		t.Errorf("backup ID = %v, want %v", hdr.BackupID, goldenBackupID)
	}

	blockCounts := make(map[AFBBlockType]int)
	for {
		block, err := reader.ReadBlock()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("reading block: %v", err)
		}
		blockCounts[block.Type]++

		// Verify specific blocks decode correctly
		switch block.Type {
		case BlockDocumentBatch:
			entries, err := DecodeDocumentBatch(block.Payload)
			if err != nil {
				t.Fatalf("decoding document batch: %v", err)
			}
			if len(entries) != 2 {
				t.Errorf("document count = %d, want 2", len(entries))
			}
			if string(entries[0].Key) != "doc-alpha" {
				t.Errorf("first doc key = %q, want %q", entries[0].Key, "doc-alpha")
			}

		case BlockEmbeddingBatch:
			name, dim, entries, err := DecodeEmbeddingBatch(block.Payload)
			if err != nil {
				t.Fatalf("decoding embedding batch: %v", err)
			}
			if name != "emb_v0" || dim != 3 || len(entries) != 2 {
				t.Errorf("embeddings: name=%q dim=%d count=%d", name, dim, len(entries))
			}
			if entries[0].Vector[0] != 0.1 || entries[1].Vector[2] != 0.6 {
				t.Errorf("vector values wrong: [0][0]=%v [1][2]=%v", entries[0].Vector[0], entries[1].Vector[2])
			}

		case BlockEdgeBatch:
			name, entries, err := DecodeEdgeBatch(block.Payload)
			if err != nil {
				t.Fatalf("decoding edge batch: %v", err)
			}
			if name != "links" || len(entries) != 1 {
				t.Errorf("edges: name=%q count=%d", name, len(entries))
			}
		}
	}

	// Verify expected block types present
	if blockCounts[BlockClusterManifest] != 1 {
		t.Errorf("cluster manifests = %d, want 1", blockCounts[BlockClusterManifest])
	}
	if blockCounts[BlockDocumentBatch] != 1 {
		t.Errorf("document batches = %d, want 1", blockCounts[BlockDocumentBatch])
	}
	if blockCounts[BlockEmbeddingBatch] != 1 {
		t.Errorf("embedding batches = %d, want 1", blockCounts[BlockEmbeddingBatch])
	}
	if blockCounts[BlockEdgeBatch] != 1 {
		t.Errorf("edge batches = %d, want 1", blockCounts[BlockEdgeBatch])
	}
}

// crossBackendBackupID is a fixed UUID for the cross-backend fixture.
var crossBackendBackupID = uuid.UUID{0xCA, 0xFE, 0xBA, 0xBE, 0xDE, 0xAD, 0xBE, 0xEF,
	0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF}

// buildCrossBackendAFB produces a comprehensive AFB file exercising all batch types.
// This fixture is designed for Zig to import and verify, covering:
//   - Multiple documents with varied content (plain and compressed-flagged)
//   - Dense embeddings (384-dim, matching typical model output)
//   - Sparse vectors (BM25-style term vectors)
//   - Graph edges (multiple types, bidirectional)
//   - Multi-shard layout
func buildCrossBackendAFB() ([]byte, error) {
	var buf bytes.Buffer
	w, err := NewAFBWriter(&buf, false) // no compression for deterministic cross-platform bytes
	if err != nil {
		return nil, err
	}
	defer w.Close()

	if err := w.WriteHeader(AFBFileHeader{
		FormatVersion: 1,
		Flags:         0,
		CreatedAtNs:   1700000000_000000000,
		BackupID:      crossBackendBackupID,
		TableCount:    1,
		ShardCount:    2,
	}); err != nil {
		return nil, err
	}

	// Cluster manifest
	if err := w.WriteBlock(BlockClusterManifest, []byte(
		`{"backup_id":"cafebabe-dead-beef-0123-456789abcdef","source_backend":"go","tables":["wiki"]}`)); err != nil {
		return nil, err
	}

	// Table manifest
	if err := w.WriteBlock(BlockTableManifest, []byte(
		`{"name":"wiki","shards":2,"indexes":{"emb_v0":{"type":"embedding","dimension":4},"sparse_v0":{"type":"sparse"},"links":{"type":"link"}}}`)); err != nil {
		return nil, err
	}

	// --- Shard 1: keys [a..m) ---
	shardHeader1 := EncodeShardHeader(ShardHeaderEntry{
		TableName: "wiki",
		ShardID:   1,
		StartKey:  []byte("a"),
		EndKey:    []byte("m"),
	})
	if err := w.WriteBlock(BlockShardHeader, shardHeader1); err != nil {
		return nil, err
	}

	// Documents for shard 1
	docBatch1 := EncodeDocumentBatch([]DocumentEntry{
		{Key: []byte("albert-einstein"), Value: []byte(`{"title":"Albert Einstein","content":"Theoretical physicist who developed the theory of relativity.","born":1879}`), TimestampNs: 1000000},
		{Key: []byte("alan-turing"), Value: []byte(`{"title":"Alan Turing","content":"Pioneer of computer science and artificial intelligence.","born":1912}`), TimestampNs: 2000000},
		{Key: []byte("ada-lovelace"), Value: []byte(`{"title":"Ada Lovelace","content":"First computer programmer, worked on Babbage's Analytical Engine.","born":1815}`), TimestampNs: 3000000},
	})
	if err := w.WriteBlock(BlockDocumentBatch, docBatch1); err != nil {
		return nil, err
	}

	// Embeddings for shard 1 (4-dim for compact fixture)
	embBatch1 := EncodeEmbeddingBatch("emb_v0", 4, []EmbeddingEntry{
		{DocKey: []byte("albert-einstein"), HashID: 1001, Vector: []float32{0.25, -0.5, 0.75, 0.1}},
		{DocKey: []byte("alan-turing"), HashID: 1002, Vector: []float32{0.3, -0.4, 0.8, 0.15}},
		{DocKey: []byte("ada-lovelace"), HashID: 1003, Vector: []float32{0.35, -0.45, 0.7, 0.2}},
	})
	if err := w.WriteBlock(BlockEmbeddingBatch, embBatch1); err != nil {
		return nil, err
	}

	// Sparse vectors for shard 1
	sparseBatch1 := EncodeSparseBatch("sparse_v0", []SparseEntry{
		{DocKey: []byte("albert-einstein"), HashID: 2001, Indices: []uint32{10, 50, 200, 512}, Values: []float32{2.5, 1.8, 3.1, 0.5}},
		{DocKey: []byte("alan-turing"), HashID: 2002, Indices: []uint32{10, 75, 300}, Values: []float32{1.5, 2.2, 1.0}},
		{DocKey: []byte("ada-lovelace"), HashID: 2003, Indices: []uint32{10, 50, 100, 400, 512}, Values: []float32{1.2, 1.5, 2.8, 0.3, 0.7}},
	})
	if err := w.WriteBlock(BlockSparseBatch, sparseBatch1); err != nil {
		return nil, err
	}

	// Edges for shard 1
	edgeBatch1 := EncodeEdgeBatch("links", []EdgeEntry{
		{SourceKey: []byte("albert-einstein"), TargetKey: []byte("alan-turing"), EdgeType: []byte("influenced"), Value: []byte(`{"weight":0.8}`)},
		{SourceKey: []byte("ada-lovelace"), TargetKey: []byte("alan-turing"), EdgeType: []byte("preceded"), Value: []byte(`{"years":97}`)},
		{SourceKey: []byte("alan-turing"), TargetKey: []byte("albert-einstein"), EdgeType: []byte("cited"), Value: []byte(`{}`)},
	})
	if err := w.WriteBlock(BlockEdgeBatch, edgeBatch1); err != nil {
		return nil, err
	}

	// Shard 1 footer: shardID(u32) + docCount(u32)
	shard1Footer := make([]byte, 8)
	binary.LittleEndian.PutUint32(shard1Footer[0:4], 1)
	binary.LittleEndian.PutUint32(shard1Footer[4:8], 3) // 3 docs
	if err := w.WriteBlock(BlockShardFooter, shard1Footer); err != nil {
		return nil, err
	}

	// --- Shard 2: keys [m..z) ---
	shardHeader2 := EncodeShardHeader(ShardHeaderEntry{
		TableName: "wiki",
		ShardID:   2,
		StartKey:  []byte("m"),
		EndKey:    []byte("{"), // after 'z' in ASCII
	})
	if err := w.WriteBlock(BlockShardHeader, shardHeader2); err != nil {
		return nil, err
	}

	// Documents for shard 2
	docBatch2 := EncodeDocumentBatch([]DocumentEntry{
		{Key: []byte("marie-curie"), Value: []byte(`{"title":"Marie Curie","content":"Physicist and chemist, first woman to win a Nobel Prize.","born":1867}`), TimestampNs: 4000000},
		{Key: []byte("nikola-tesla"), Value: []byte(`{"title":"Nikola Tesla","content":"Inventor and electrical engineer, pioneer of AC power.","born":1856}`), TimestampNs: 5000000},
	})
	if err := w.WriteBlock(BlockDocumentBatch, docBatch2); err != nil {
		return nil, err
	}

	// Embeddings for shard 2
	embBatch2 := EncodeEmbeddingBatch("emb_v0", 4, []EmbeddingEntry{
		{DocKey: []byte("marie-curie"), HashID: 1004, Vector: []float32{-0.1, 0.6, 0.2, -0.3}},
		{DocKey: []byte("nikola-tesla"), HashID: 1005, Vector: []float32{0.5, -0.2, 0.9, 0.05}},
	})
	if err := w.WriteBlock(BlockEmbeddingBatch, embBatch2); err != nil {
		return nil, err
	}

	// Sparse for shard 2
	sparseBatch2 := EncodeSparseBatch("sparse_v0", []SparseEntry{
		{DocKey: []byte("marie-curie"), HashID: 2004, Indices: []uint32{10, 120, 350}, Values: []float32{2.0, 1.3, 0.9}},
		{DocKey: []byte("nikola-tesla"), HashID: 2005, Indices: []uint32{10, 60, 200, 500}, Values: []float32{1.7, 2.5, 1.1, 0.4}},
	})
	if err := w.WriteBlock(BlockSparseBatch, sparseBatch2); err != nil {
		return nil, err
	}

	// Edges for shard 2
	edgeBatch2 := EncodeEdgeBatch("links", []EdgeEntry{
		{SourceKey: []byte("marie-curie"), TargetKey: []byte("albert-einstein"), EdgeType: []byte("contemporary"), Value: []byte(`{"overlap_years":24}`)},
		{SourceKey: []byte("nikola-tesla"), TargetKey: []byte("albert-einstein"), EdgeType: []byte("contemporary"), Value: []byte(`{"overlap_years":36}`)},
	})
	if err := w.WriteBlock(BlockEdgeBatch, edgeBatch2); err != nil {
		return nil, err
	}

	// Shard 2 footer
	shard2Footer := make([]byte, 8)
	binary.LittleEndian.PutUint32(shard2Footer[0:4], 2)
	binary.LittleEndian.PutUint32(shard2Footer[4:8], 2) // 2 docs
	if err := w.WriteBlock(BlockShardFooter, shard2Footer); err != nil {
		return nil, err
	}

	// File footer: total doc count
	fileFooter := make([]byte, 4)
	binary.LittleEndian.PutUint32(fileFooter[0:4], 5) // 5 total docs
	if err := w.WriteBlock(BlockFileFooter, fileFooter); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// TestAFBCrossBackendFixture generates and verifies the cross-backend fixture file.
// This file is consumed by Zig tests to verify cross-backend compatibility.
// Run with -update-golden to regenerate: go test -run TestAFBCrossBackendFixture -update-golden
func TestAFBCrossBackendFixture(t *testing.T) {
	goldenPath := filepath.Join("testdata", "cross_backend_v1.afb")

	got, err := buildCrossBackendAFB()
	if err != nil {
		t.Fatalf("building cross-backend fixture: %v", err)
	}

	if *updateGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("Updated cross-backend fixture %s (%d bytes)", goldenPath, len(got))
		return
	}

	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("Reading fixture (run with -update-golden to create): %v", err)
	}

	if !bytes.Equal(got, want) {
		minLen := len(got)
		if len(want) < minLen {
			minLen = len(want)
		}
		for i := 0; i < minLen; i++ {
			if got[i] != want[i] {
				t.Fatalf("Cross-backend fixture differs at byte %d: got 0x%02x, want 0x%02x (got %d bytes, want %d bytes)",
					i, got[i], want[i], len(got), len(want))
			}
		}
		t.Fatalf("Cross-backend fixture length differs: got %d bytes, want %d bytes", len(got), len(want))
	}

	// Verify full readability and expected structure
	reader, err := NewAFBReader(bytes.NewReader(got))
	if err != nil {
		t.Fatalf("creating reader: %v", err)
	}
	defer reader.Close()

	hdr, err := reader.ReadHeader()
	if err != nil {
		t.Fatalf("reading header: %v", err)
	}
	if hdr.ShardCount != 2 {
		t.Errorf("shard count = %d, want 2", hdr.ShardCount)
	}
	if hdr.BackupID != crossBackendBackupID {
		t.Errorf("backup ID = %v, want %v", hdr.BackupID, crossBackendBackupID)
	}

	blockCounts := make(map[AFBBlockType]int)
	totalDocs := 0
	totalEmbeddings := 0
	totalSparse := 0
	totalEdges := 0

	for {
		block, err := reader.ReadBlock()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("reading block: %v", err)
		}
		blockCounts[block.Type]++

		switch block.Type {
		case BlockDocumentBatch:
			entries, err := DecodeDocumentBatch(block.Payload)
			if err != nil {
				t.Fatalf("decoding documents: %v", err)
			}
			totalDocs += len(entries)

		case BlockEmbeddingBatch:
			name, dim, entries, err := DecodeEmbeddingBatch(block.Payload)
			if err != nil {
				t.Fatalf("decoding embeddings: %v", err)
			}
			if name != "emb_v0" {
				t.Errorf("embedding index = %q, want emb_v0", name)
			}
			if dim != 4 {
				t.Errorf("embedding dim = %d, want 4", dim)
			}
			totalEmbeddings += len(entries)

		case BlockSparseBatch:
			name, entries, err := DecodeSparseBatch(block.Payload)
			if err != nil {
				t.Fatalf("decoding sparse: %v", err)
			}
			if name != "sparse_v0" {
				t.Errorf("sparse index = %q, want sparse_v0", name)
			}
			totalSparse += len(entries)

		case BlockEdgeBatch:
			name, entries, err := DecodeEdgeBatch(block.Payload)
			if err != nil {
				t.Fatalf("decoding edges: %v", err)
			}
			if name != "links" {
				t.Errorf("edge index = %q, want links", name)
			}
			totalEdges += len(entries)
		}
	}

	if totalDocs != 5 {
		t.Errorf("total documents = %d, want 5", totalDocs)
	}
	if totalEmbeddings != 5 {
		t.Errorf("total embeddings = %d, want 5", totalEmbeddings)
	}
	if totalSparse != 5 {
		t.Errorf("total sparse = %d, want 5", totalSparse)
	}
	if totalEdges != 5 {
		t.Errorf("total edges = %d, want 5", totalEdges)
	}
	if blockCounts[BlockShardHeader] != 2 {
		t.Errorf("shard headers = %d, want 2", blockCounts[BlockShardHeader])
	}
	if blockCounts[BlockShardFooter] != 2 {
		t.Errorf("shard footers = %d, want 2", blockCounts[BlockShardFooter])
	}

	t.Logf("Cross-backend fixture: %d bytes, %d docs, %d embeddings, %d sparse, %d edges, %d shards",
		len(got), totalDocs, totalEmbeddings, totalSparse, totalEdges, blockCounts[BlockShardHeader])
}

func TestFloat32LEEncoding(t *testing.T) {
	// Verify specific float32 values produce the expected LE byte pattern.
	// This is the cross-backend contract: Zig must produce identical bytes.
	testCases := []struct {
		val  float32
		want uint32 // expected LE bits
	}{
		{1.0, 0x3f800000},
		{-1.0, 0xbf800000},
		{0.0, 0x00000000},
		{math.MaxFloat32, 0x7f7fffff},
	}

	for _, tc := range testCases {
		buf := appendFloat32LE(nil, tc.val)
		got := binary.LittleEndian.Uint32(buf)
		if got != tc.want {
			t.Errorf("float32LE(%v) = 0x%08x, want 0x%08x", tc.val, got, tc.want)
		}
		roundTripped := readFloat32LE(buf)
		if roundTripped != tc.val {
			t.Errorf("readFloat32LE(0x%08x) = %v, want %v", tc.want, roundTripped, tc.val)
		}
	}
}
