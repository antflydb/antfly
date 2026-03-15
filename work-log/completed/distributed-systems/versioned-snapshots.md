# Archive System Enhancement: Compression Auto-Detection and Versioned Metadata

## Summary

Enhance Antfly's archive system to support:
1. **Magic byte detection** for auto-detecting compression formats (gzip/snappy/zstd)
2. **Full metadata header** with Antfly version, archive format version, timestamp, and shard info
3. **No backward compatibility** - all archives use new format (v3)

## New Types

```go
// src/common/archive.go

const (
    CurrentArchiveFormatVersion = 3
    MetadataFileName = "__antfly_metadata__.json"
)

type ArchiveMetadata struct {
    FormatVersion int        `json:"format_version"`
    AntflyVersion string     `json:"antfly_version"`
    CreatedAt     string     `json:"created_at"`     // RFC3339
    Compression   string     `json:"compression"`    // "gzip", "snappy", "zstd"
    Shard         *ShardInfo `json:"shard,omitempty"`
}

type ShardInfo struct {
    ShardID    string `json:"shard_id"`    // Hex-encoded
    NodeID     string `json:"node_id"`     // Hex-encoded
    RangeStart string `json:"range_start"` // Base64-encoded
    RangeEnd   string `json:"range_end"`   // Base64-encoded
    TableName  string `json:"table_name,omitempty"`
}

type CreateArchiveOptions struct {
    ArchiveType ArchiveType
    Metadata    *ArchiveMetadata
}

type ExtractArchiveResult struct {
    Metadata *ArchiveMetadata
}
```

## Implementation Steps

### Step 1: Add Version to Utils Package
**File:** `lib/utils/version.go` (NEW)

```go
package utils

var Version = "dev"  // Set via ldflags

func GetVersion() string { return Version }
```

Update `cmd/antfly/main.go` to set `utils.Version = version` in init().

### Step 2: Add Magic Byte Detection
**File:** `src/common/archive.go`

```go
var (
    magicGzip   = []byte{0x1f, 0x8b}
    magicZstd   = []byte{0x28, 0xb5, 0x2f, 0xfd}
    magicSnappy = []byte{0xff, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59}
)

func DetectArchiveType(filePath string) (ArchiveType, error)
func DetectArchiveTypeFromReader(r io.Reader) (ArchiveType, error)
```

### Step 3: Update CreateArchive
**File:** `src/common/archive.go`

- Change signature to accept `CreateArchiveOptions`
- Write `__antfly_metadata__.json` as **first** tar entry
- Auto-populate `AntflyVersion` from `version.Get()`, `CreatedAt`, `Compression`

### Step 4: Update ExtractArchive
**File:** `src/common/archive.go`

- Remove `archiveType` parameter, use auto-detection
- Parse metadata from first tar entry if present
- Return `*ExtractArchiveResult` with metadata

### Step 5: Update SnapStore Interface
**File:** `src/snapstore/snapstore.go`

```go
type SnapshotOptions struct {
    ShardID   types.ID
    NodeID    types.ID
    Range     types.Range
    TableName string
}

// Updated signatures:
CreateSnapshot(ctx, snapID, sourceDir string, opts *SnapshotOptions) (int64, error)
ExtractSnapshot(ctx, snapID, targetDir string, removeExisting bool) (*common.ArchiveMetadata, error)
```

### Step 6: Update Callers
**Files to update:**
- `src/store/db.go` - `DBImpl.Snapshot()` passes shard info
- `src/store/dbwrapper.go` - Handle metadata in `loadPersistentSnapshot`
- `src/metadatakv/kv.go` - Handle new return type

## File Changes Summary

| File | Action |
|------|--------|
| `lib/utils/version.go` | CREATE - Version in utils package |
| `cmd/antfly/main.go` | UPDATE - Set utils.Version |
| `src/common/archive.go` | UPDATE - Add types, magic detection, metadata |
| `src/common/archive_test.go` | UPDATE - Add tests |
| `src/snapstore/snapstore.go` | UPDATE - Interface + implementation |
| `src/store/db.go` | UPDATE - Pass shard info to Snapshot() |
| `src/store/dbwrapper.go` | UPDATE - Handle metadata in restore |
| `src/metadatakv/kv.go` | UPDATE - Handle new ExtractSnapshot signature |

## Testing

1. **Unit tests** in `archive_test.go`:
   - `TestDetectArchiveType` - All compression formats
   - `TestArchiveWithMetadata` - Roundtrip with shard info
   - `TestArchiveAutoDetection` - Extract without explicit type

2. **Integration tests** in `snapstore/`:
   - `TestSnapStoreWithMetadata` - Full snapshot cycle with metadata

## Key Design Decisions

1. **Metadata as first tar entry** (`__antfly_metadata__.json`) - Simple, extensible, compressed with data
2. **Version in utils package** (`lib/utils/version.go`) - Avoids import cycles, single source of truth, fits existing lib structure
3. **Magic byte order**: Check snappy first (10 bytes), then zstd (4 bytes), then gzip (2 bytes)
