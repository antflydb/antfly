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

package storeutils

import (
	"bytes"
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"

	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/vector"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/klauspost/compress/zstd"
)

func KeyRangeStart(key []byte) []byte {
	return append(bytes.Clone(key), DBRangeStart...)
}

func KeyRangeEnd(key []byte) []byte {
	return append(bytes.Clone(key), DBRangeEnd...)
}

var ErrEmptyRequest = errors.New("empty request")
var (
	DBRangeStart = []byte{':', '\x00'}
	DBRangeEnd   = []byte{':', '\xFF'}

	// Used for enrichment "i:<indexName>:e"
	EmbeddingSuffix = []byte{':', 'e'}
	// Used for enrichment "i:<indexName>:s"
	SummarySuffix = []byte{':', 's'}
	// Used for enrichment "i:<indexName>:c:<chunkID>"
	ChunkingSuffix = []byte{':', 'c'}
	// Used for full-text chunking "i:<indexName>:cft:<chunkID>"
	ChunkingFullTextSuffix = []byte{':', 'c', 'f', 't'}
	// Used for transaction timestamp metadata (also used for TTL)
	TransactionSuffix = []byte{':', 't'}
	// Used for graph edges "i:<indexName>:out:<edgeType>:<targetKey>:o"
	EdgeOutSuffix = []byte{':', 'o'}
	// Used for sparse embeddings "i:<indexName>:sp"
	SparseSuffix = []byte{':', 's', 'p'}

	// DudEnrichmentValue is a sentinel value written as an enrichment key's value
	// to mark an item as permanently unenrichable. ScanForEnrichment treats any
	// existing enrichment key as "already enriched", so writing this value prevents
	// the item from being re-queued on every backfill. It is distinguishable from
	// real enrichments which always start with an 8-byte hashID.
	DudEnrichmentValue = []byte{0xDD}

	// Special keys for storing metadata
	MetadataPrefix = []byte("\x00\x00__meta__")
)

// Document represents the result of a document query with optional embeddings and summaries
type Document struct {
	// Document is the decoded JSON document
	Document map[string]any
	// Embedding is a single vector embedding if requested via EmbeddingSuffix
	Embedding vector.T
	// EmbeddingHashID is the hash ID of the single embedding
	EmbeddingHashID uint64
	// Summary is a single summary text if requested via SummarySuffix
	Summary string
	// SummaryHashID is the hash ID of the single summary
	SummaryHashID uint64
	// Embeddings maps index names to their embeddings when AllEmbeddings is true
	Embeddings map[string][]float32
	// Summaries maps index names to their summaries when AllSummaries is true
	Summaries map[string]string
	// Chunks maps index names to their chunks when AllChunks is true
	Chunks map[string][]chunking.Chunk
	// ChunksHashID is the hash ID of the chunked field for the index specified in ChunkSuffix
	// Only populated when ChunkSuffix is set in QueryOptions
	// Retrieved from chunk 0 of the specified index
	ChunksHashID uint64
}

// IsDudEnrichment returns true if the value is a dud enrichment marker.
func IsDudEnrichment(value []byte) bool {
	return bytes.Equal(value, DudEnrichmentValue)
}

// Context key for HLC timestamp
type timestampKey struct{}

// WithTimestamp adds an HLC timestamp to the context
func WithTimestamp(ctx context.Context, ts uint64) context.Context {
	return context.WithValue(ctx, timestampKey{}, ts)
}

// GetTimestampFromContext retrieves the HLC timestamp from the context
// Returns 0 if no timestamp is present
func GetTimestampFromContext(ctx context.Context) uint64 {
	if ts, ok := ctx.Value(timestampKey{}).(uint64); ok {
		return ts
	}
	return 0
}

// IsEdgeKey determines if a key is an edge key
// Edge keys have pattern: <baseKey>:i:<indexName>:out:<edgeType>:<target>:o
// or: <baseKey>:i:<indexName>:in:<edgeType>:<source>:i
func IsEdgeKey(key []byte) bool {
	return bytes.Contains(key, []byte(":i:")) &&
		(bytes.Contains(key, []byte(":out:")) || bytes.Contains(key, []byte(":in:")))
}

// HasSuffix checks if key has the given suffix
func HasSuffix(key []byte, suffix []byte) bool {
	return bytes.HasSuffix(key, suffix)
}

// IsChunkKey checks if a key is a chunk key by validating the suffix :c, :cft, or :cm
// New format: :i:<indexName>:<chunkID>:c or :i:<indexName>:<chunkID>:cft or :i:<indexName>:<chunkID>:cm
func IsChunkKey(key []byte) bool {
	return bytes.HasSuffix(key, []byte(":c")) || bytes.HasSuffix(key, []byte(":cft")) || bytes.HasSuffix(key, []byte(":cm"))
}

// ParseChunkKey extracts the document key and index name from a chunk key.
// Chunk keys have format: <docKey>:i:<indexName>:<chunkID>:c|cft|cm.
// It uses the last :i: marker so document IDs may themselves contain :i:.
func ParseChunkKey(chunkKey []byte) (docKey []byte, indexName string, ok bool) {
	if !IsChunkKey(chunkKey) {
		return nil, "", false
	}

	var keyWithoutSuffix []byte
	switch {
	case bytes.HasSuffix(chunkKey, []byte(":cft")):
		keyWithoutSuffix = chunkKey[:len(chunkKey)-len(":cft")]
	case bytes.HasSuffix(chunkKey, []byte(":cm")):
		keyWithoutSuffix = chunkKey[:len(chunkKey)-len(":cm")]
	case bytes.HasSuffix(chunkKey, []byte(":c")):
		keyWithoutSuffix = chunkKey[:len(chunkKey)-len(":c")]
	default:
		return nil, "", false
	}

	chunkIDSep := bytes.LastIndexByte(keyWithoutSuffix, ':')
	if chunkIDSep <= 0 || chunkIDSep == len(keyWithoutSuffix)-1 {
		return nil, "", false
	}

	keyWithoutChunkID := keyWithoutSuffix[:chunkIDSep]
	indexMarker := bytes.LastIndex(keyWithoutChunkID, []byte(":i:"))
	if indexMarker <= 0 || indexMarker+len(":i:") >= len(keyWithoutChunkID) {
		return nil, "", false
	}

	return bytes.Clone(keyWithoutChunkID[:indexMarker]),
		string(keyWithoutChunkID[indexMarker+len(":i:"):]),
		true
}

// ParseSummaryKey extracts the document key and index name from a summary key.
// Summary keys have format: <docKey>:i:<indexName>:s.
// It uses the last :i: marker so document IDs may themselves contain :i:.
func ParseSummaryKey(summaryKey []byte) (docKey []byte, indexName string, ok bool) {
	if !bytes.HasSuffix(summaryKey, SummarySuffix) {
		return nil, "", false
	}

	keyWithoutSuffix := summaryKey[:len(summaryKey)-len(SummarySuffix)]
	indexMarker := bytes.LastIndex(keyWithoutSuffix, []byte(":i:"))
	if indexMarker <= 0 || indexMarker+len(":i:") >= len(keyWithoutSuffix) {
		return nil, "", false
	}

	return bytes.Clone(keyWithoutSuffix[:indexMarker]),
		string(keyWithoutSuffix[indexMarker+len(":i:"):]),
		true
}

// ExtractDocKeyFromChunk extracts the document key from a chunk key.
// Chunk keys have format: <docKey>:i:<indexName>:<chunkID>:c or <docKey>:i:<indexName>:<chunkID>:cft
// Returns the document key and true if successful, or nil and false if not a chunk key.
func ExtractDocKeyFromChunk(chunkKey []byte) ([]byte, bool) {
	docKey, _, ok := ParseChunkKey(chunkKey)
	if !ok {
		return nil, false
	}
	return docKey, true
}

// MakeChunkKey creates a chunk key for a specific chunk ID
// Format: <docKey>:i:<indexName>:<chunkID>:c
func MakeChunkKey(docKey []byte, indexName string, chunkID uint32) []byte {
	return fmt.Appendf(bytes.Clone(docKey), ":i:%s:%d:c", indexName, chunkID)
}

// MakeChunkPrefix creates a prefix for scanning all chunks of a document for an index
// Format: <docKey>:i:<indexName>:
// This prefix matches all chunk IDs ending in :c for the given document and index
func MakeChunkPrefix(docKey []byte, indexName string) []byte {
	key := bytes.Clone(docKey)
	key = append(key, []byte(":i:")...)
	key = append(key, []byte(indexName)...)
	key = append(key, []byte(":")...)
	return key
}

// MakeChunkFullTextKey creates a full-text chunk key for a specific chunk ID
// Format: <docKey>:i:<indexName>:<chunkID>:cft
func MakeChunkFullTextKey(docKey []byte, indexName string, chunkID uint32) []byte {
	return fmt.Appendf(bytes.Clone(docKey), ":i:%s:%d:cft", indexName, chunkID)
}

// MakeChunkFullTextPrefix creates a prefix for scanning all full-text chunks of a document for an index
// Format: <docKey>:i:<indexName>:
// This prefix matches all chunk IDs ending in :cft for the given document and index
func MakeChunkFullTextPrefix(docKey []byte, indexName string) []byte {
	key := bytes.Clone(docKey)
	key = append(key, []byte(":i:")...)
	key = append(key, []byte(indexName)...)
	key = append(key, []byte(":")...)
	return key
}

// MakeMediaChunkKey creates a media chunk key for a specific chunk ID
// Format: <docKey>:i:<indexName>:<chunkID>:cm
func MakeMediaChunkKey(docKey []byte, indexName string, chunkID uint32) []byte {
	return fmt.Appendf(bytes.Clone(docKey), ":i:%s:%d:cm", indexName, chunkID)
}

// MakeMediaChunkPrefix creates a prefix for scanning all media chunks of a document for an index
// Format: <docKey>:i:<indexName>:
// This prefix matches all chunk IDs ending in :cm for the given document and index
// (same prefix as MakeChunkPrefix since they share the same key space)
func MakeMediaChunkPrefix(docKey []byte, indexName string) []byte {
	return MakeChunkPrefix(docKey, indexName)
}

// IsMediaChunkKey checks if a key is a media chunk key by validating the suffix :cm
func IsMediaChunkKey(key []byte) bool {
	return bytes.HasSuffix(key, []byte(":cm"))
}

// MakeEmbeddingKey creates an embedding key for a specific index
// Format: <docKey>:i:<indexName>:e
func MakeEmbeddingKey(docKey []byte, indexName string) []byte {
	return fmt.Appendf(bytes.Clone(docKey), ":i:%s:e", indexName)
}

// MakeSummaryKey creates a summary key for a specific index
// Format: <docKey>:i:<indexName>:s
func MakeSummaryKey(docKey []byte, indexName string) []byte {
	return fmt.Appendf(bytes.Clone(docKey), ":i:%s:s", indexName)
}

// MakeSparseKey creates a sparse embedding key for a specific index
// Format: <docKey>:i:<indexName>:sp
func MakeSparseKey(docKey []byte, indexName string) []byte {
	return fmt.Appendf(bytes.Clone(docKey), ":i:%s:sp", indexName)
}

// zstdDecoder is a shared concurrent-safe decoder for zstd decompression.
var zstdDecoder = must(zstd.NewReader(nil))

func must[T any](v T, err error) T {
	if err != nil {
		panic(fmt.Sprintf("storeutils init: %v", err))
	}
	return v
}

// IsZstdCompressed checks if data starts with the zstd magic number (0x28B52FFD).
func IsZstdCompressed(data []byte) bool {
	return len(data) >= 4 && data[0] == 0x28 && data[1] == 0xB5 && data[2] == 0x2F && data[3] == 0xFD
}

// DecodeDocumentJSON decodes a document value that may be zstd-compressed or plain JSON.
// Uses the shared zstd decoder (concurrent-safe via DecodeAll).
//
// Note: scanner.go intentionally uses a streaming zstd.Reader.Reset()+io.Copy approach
// instead of DecodeAll. The streaming path reuses an io.Reader across many documents
// during a hot scan, avoiding a per-document allocation. This function is for
// non-scanning call sites where simplicity is preferred over streaming performance.
func DecodeDocumentJSON(data []byte) (map[string]any, error) {
	var raw []byte
	if IsZstdCompressed(data) {
		var err error
		raw, err = zstdDecoder.DecodeAll(data, nil)
		if err != nil {
			return nil, fmt.Errorf("zstd decompress: %w", err)
		}
	} else {
		raw = data
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}
	return doc, nil
}

// ValidateDocumentJSON checks if data is valid zstd-compressed or plain JSON
// without allocating a map[string]any. Use this instead of DecodeDocumentJSON
// when you only need to validate decodability.
func ValidateDocumentJSON(data []byte) error {
	var raw []byte
	if IsZstdCompressed(data) {
		var err error
		raw, err = zstdDecoder.DecodeAll(data, nil)
		if err != nil {
			return fmt.Errorf("zstd decompress: %w", err)
		}
	} else {
		raw = data
	}
	if !stdjson.Valid(raw) {
		return fmt.Errorf("invalid JSON")
	}
	return nil
}

// DecodeUint64Ascending decodes a uint64 from a byte slice using ascending encoding
func DecodeUint64Ascending(buf []byte) (remaining []byte, value uint64, err error) {
	return encoding.DecodeUint64Ascending(buf)
}

// EncodeUint64Ascending encodes a uint64 to a byte slice using ascending encoding
func EncodeUint64Ascending(buf []byte, value uint64) []byte {
	return encoding.EncodeUint64Ascending(buf, value)
}
