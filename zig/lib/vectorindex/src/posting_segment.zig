// Copyright 2026 Antfly, Inc.
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

//! Immutable posting-family segment container.
//!
//! This is the physical file-format foundation for a future
//! `backend = segments, format = base_delta` vector posting store. The payloads
//! remain the existing logical posting values (`PostingFormat` base/delta and
//! `CentroidDirectoryFormat` records); this module only changes the physical
//! container from one LSM key/value per record to one posting-local indexed
//! segment blob.

const std = @import("std");
const Allocator = std.mem.Allocator;
const posting = @import("posting.zig");

pub const PostingId = posting.PostingId;

const magic: [4]u8 = "AFPS".*;
const manifest_magic: [4]u8 = "AFPM".*;
const version: u16 = 1;
const index_entry_size: usize = 8 + 1 + 8 + 8 + 8 + 4;
const footer_size: usize = 8 + 8 + 4 + 4 + 2 + 4;
const manifest_header_size: usize = 4 + 2 + 4 + 8;
const manifest_checksum_size: usize = 4;
const overlay_plan_min_delta_records: usize = 8;
const compact_sorted_delta_max_records: usize = 64;

pub const segment_directory = "postings";
pub const default_manifest_path = "postings/manifest.afpm";

pub const EntryKind = enum(u8) {
    base = 1,
    delta = 2,
    centroid_directory = 3,
};

pub const DeltaValue = struct {
    sequence: u64,
    value: []const u8,
};

pub const EntryValue = struct {
    posting_id: PostingId,
    kind: EntryKind,
    sequence: u64,
    value: []const u8,
};

pub const SegmentMeta = struct {
    segment_id: u64,
    min_posting_id: PostingId = 0,
    max_posting_id: PostingId = 0,
    min_delta_sequence: u64 = 0,
    max_delta_sequence: u64 = 0,
    byte_len: usize = 0,
    entry_count: usize = 0,
    index_offset: usize = 0,
    index_checksum: u32 = 0,

    pub fn mayContainPosting(self: SegmentMeta, posting_id: PostingId) bool {
        return self.entry_count != 0 and posting_id >= self.min_posting_id and posting_id <= self.max_posting_id;
    }
};

pub const SegmentBlob = struct {
    meta: SegmentMeta,
    data: []const u8,
};

pub const BuiltSegment = struct {
    meta: SegmentMeta,
    data: []u8,

    pub fn deinit(self: *BuiltSegment, alloc: Allocator) void {
        alloc.free(self.data);
        self.* = .{
            .meta = .{ .segment_id = 0 },
            .data = &.{},
        };
    }

    pub fn blob(self: BuiltSegment) SegmentBlob {
        return .{
            .meta = self.meta,
            .data = self.data,
        };
    }
};

pub const CompactionStats = struct {
    input_segments: usize = 0,
    input_bytes: usize = 0,
    input_entries: usize = 0,
    input_base_records: usize = 0,
    input_centroid_records: usize = 0,
    input_delta_values: usize = 0,
    input_delta_records: usize = 0,
    retained_base_records: usize = 0,
    retained_centroid_records: usize = 0,
    retained_delta_records: usize = 0,
    dropped_superseded_base_records: usize = 0,
    dropped_superseded_centroid_records: usize = 0,
    dropped_stale_delta_records: usize = 0,
    dropped_duplicate_delta_records: usize = 0,
    output_bytes: usize = 0,
    output_entries: usize = 0,
};

pub const CompactResult = struct {
    segment: BuiltSegment,
    stats: CompactionStats,

    pub fn deinit(self: *CompactResult, alloc: Allocator) void {
        self.segment.deinit(alloc);
        self.stats = .{};
    }
};

pub const ManifestReplacementStats = struct {
    input_segments: usize = 0,
    removed_segments: usize = 0,
    added_segments: usize = 0,
    output_segments: usize = 0,
    next_segment_id: u64 = 0,
};

pub const ManifestReplacementResult = struct {
    encoded: []u8,
    stats: ManifestReplacementStats,

    pub fn deinit(self: *ManifestReplacementResult, alloc: Allocator) void {
        alloc.free(self.encoded);
        self.* = .{
            .encoded = &.{},
            .stats = .{},
        };
    }
};

pub const SegmentCommitStats = struct {
    input_segments: usize = 0,
    output_segments: usize = 0,
    segment_id: u64 = 0,
    next_segment_id: u64 = 0,
    segment_bytes: usize = 0,
    manifest_bytes: usize = 0,
};

pub const SegmentCommitResult = struct {
    entry: OwnedManifestEntry,
    stats: SegmentCommitStats,

    pub fn deinit(self: *SegmentCommitResult, alloc: Allocator) void {
        alloc.free(self.entry.path);
        self.* = .{
            .entry = .{
                .meta = .{ .segment_id = 0 },
                .path = &.{},
            },
            .stats = .{},
        };
    }
};

pub const DirectoryCompactionStats = struct {
    compaction: CompactionStats = .{},
    manifest: ManifestReplacementStats = .{},
    segment_id: u64 = 0,
    segment_bytes: usize = 0,
    manifest_bytes: usize = 0,
};

pub const DirectoryCompactionResult = struct {
    entry: OwnedManifestEntry,
    stats: DirectoryCompactionStats,

    pub fn deinit(self: *DirectoryCompactionResult, alloc: Allocator) void {
        alloc.free(self.entry.path);
        self.* = .{
            .entry = .{
                .meta = .{ .segment_id = 0 },
                .path = &.{},
            },
            .stats = .{},
        };
    }
};

pub const DirectoryGarbageCollectionStats = struct {
    manifest_segments: usize = 0,
    scanned_entries: usize = 0,
    skipped_entries: usize = 0,
    segment_files: usize = 0,
    referenced_segment_files: usize = 0,
    orphan_segment_files: usize = 0,
    deleted_segment_files: usize = 0,
};

pub const DirectoryTemporaryCleanupStats = struct {
    scanned_entries: usize = 0,
    skipped_entries: usize = 0,
    segment_temp_files: usize = 0,
    manifest_temp_files: usize = 0,
    deleted_temp_files: usize = 0,
};

pub const DirectoryVerificationStats = struct {
    manifest_segments: usize = 0,
    manifest_bytes: usize = 0,
    segment_files: usize = 0,
    segment_bytes: usize = 0,
    entries: usize = 0,
    base_records: usize = 0,
    centroid_records: usize = 0,
    delta_values: usize = 0,
    delta_records: usize = 0,
};

pub const DirectoryCopyStats = struct {
    manifest_segments: usize = 0,
    manifest_bytes: usize = 0,
    segment_files: usize = 0,
    segment_bytes: usize = 0,
    entries: usize = 0,
};

pub const DirectoryManifestStats = struct {
    segments: usize = 0,
    bytes: usize = 0,
    entries: usize = 0,
    min_segment_id: u64 = 0,
    max_segment_id: u64 = 0,
    next_segment_id: u64 = 0,
    min_posting_id: PostingId = 0,
    max_posting_id: PostingId = 0,
    min_delta_sequence: u64 = 0,
    max_delta_sequence: u64 = 0,
};

pub const DirectoryCompactionPlanOptions = struct {
    min_input_segments: usize = 2,
    max_input_segments: usize = 0,
    max_input_bytes: usize = 0,
};

pub const DirectoryCompactionPlanStats = struct {
    manifest_segments: usize = 0,
    selected_segments: usize = 0,
    selected_bytes: usize = 0,
    selected_entries: usize = 0,
    stopped_on_segment_limit: bool = false,
    stopped_on_byte_limit: bool = false,
    insufficient_segments: bool = false,
};

pub const DirectoryCompactionPlan = struct {
    segment_ids: []u64,
    stats: DirectoryCompactionPlanStats,

    pub fn deinit(self: *DirectoryCompactionPlan, alloc: Allocator) void {
        alloc.free(self.segment_ids);
        self.* = .{
            .segment_ids = &.{},
            .stats = .{},
        };
    }
};

pub const DirectoryMaintenanceOptions = struct {
    commit: CommitOptions = .{},
    plan: DirectoryCompactionPlanOptions = .{},
    compact: bool = true,
    collect_orphan_segments: bool = true,
    collect_temporary_files: bool = true,
};

pub const DirectoryMaintenanceStats = struct {
    manifest: DirectoryManifestStats = .{},
    plan: DirectoryCompactionPlanStats = .{},
    compacted: bool = false,
    compaction: DirectoryCompactionStats = .{},
    garbage: DirectoryGarbageCollectionStats = .{},
    temporary: DirectoryTemporaryCleanupStats = .{},
};

pub const ManifestEntry = struct {
    meta: SegmentMeta,
    path: []const u8,
};

pub const Manifest = struct {
    next_segment_id: u64,
    segments: []const ManifestEntry,
};

pub const OwnedManifestEntry = struct {
    meta: SegmentMeta,
    path: []u8,
};

pub const OwnedManifest = struct {
    next_segment_id: u64,
    segments: []OwnedManifestEntry,

    pub fn deinit(self: *OwnedManifest, alloc: Allocator) void {
        for (self.segments) |entry| alloc.free(entry.path);
        alloc.free(self.segments);
        self.* = .{
            .next_segment_id = 0,
            .segments = &.{},
        };
    }
};

pub const OwnedSegmentStore = struct {
    manifest: OwnedManifest,
    owned_data: [][]u8,
    segments: []SegmentBlob,

    pub fn deinit(self: *OwnedSegmentStore, alloc: Allocator) void {
        for (self.owned_data) |data| alloc.free(data);
        alloc.free(self.owned_data);
        alloc.free(self.segments);
        self.manifest.deinit(alloc);
        self.* = .{
            .manifest = .{
                .next_segment_id = 0,
                .segments = &.{},
            },
            .owned_data = &.{},
            .segments = &.{},
        };
    }

    pub fn catalog(self: *const OwnedSegmentStore) Catalog {
        return .{ .segments = self.segments };
    }

    pub fn snapshot(self: *const OwnedSegmentStore) Snapshot {
        return .{ .catalog = self.catalog() };
    }
};

pub const LazyDirectoryStore = struct {
    manifest: OwnedManifest,
    io: std.Io,
    dir: std.Io.Dir,
    options: OpenStoreOptions,

    pub fn deinit(self: *LazyDirectoryStore, alloc: Allocator) void {
        self.manifest.deinit(alloc);
        self.* = undefined;
    }

    pub fn snapshot(self: *const LazyDirectoryStore) LazyDirectorySnapshot {
        return .{
            .manifest = &self.manifest,
            .io = self.io,
            .dir = self.dir,
            .options = self.options,
        };
    }
};

pub const OpenStoreOptions = struct {
    manifest_path: []const u8 = default_manifest_path,
    max_manifest_bytes: usize = 64 * 1024 * 1024,
    max_segment_bytes: usize = 1024 * 1024 * 1024,
};

pub const CommitOptions = struct {
    manifest_path: []const u8 = default_manifest_path,
    max_manifest_bytes: usize = 64 * 1024 * 1024,
    max_segment_bytes: usize = 1024 * 1024 * 1024,
    initial_segment_id: u64 = 1,
};

pub const DirectoryBatchWriterOptions = struct {
    commit: CommitOptions = .{},
    max_pending_entries: usize = 1024,
    max_pending_value_bytes: usize = 4 * 1024 * 1024,
    max_pending_delta_records: usize = 4096,
};

pub const DirectoryBatchWriterStats = struct {
    flushed_segments: usize = 0,
    committed_entries: usize = 0,
    committed_delta_records: usize = 0,
    committed_value_bytes: usize = 0,
    committed_segment_bytes: usize = 0,
    committed_manifest_bytes: usize = 0,
    last_segment_id: u64 = 0,
    next_segment_id: u64 = 0,
};

pub const RuntimeDirectoryStoreOptions = struct {
    manifest_path: []const u8 = default_manifest_path,
    max_manifest_bytes: usize = 64 * 1024 * 1024,
    max_segment_bytes: usize = 1024 * 1024 * 1024,
    initial_segment_id: u64 = 1,
    max_pending_entries: usize = 1024,
    max_pending_value_bytes: usize = 4 * 1024 * 1024,
    max_pending_delta_records: usize = 4096,
    compaction_plan: DirectoryCompactionPlanOptions = .{},
    compact_on_maintenance: bool = true,
    collect_orphan_segments_on_maintenance: bool = true,
    collect_temporary_files_on_maintenance: bool = true,

    pub fn openOptions(self: RuntimeDirectoryStoreOptions) OpenStoreOptions {
        return .{
            .manifest_path = self.manifest_path,
            .max_manifest_bytes = self.max_manifest_bytes,
            .max_segment_bytes = self.max_segment_bytes,
        };
    }

    pub fn commitOptions(self: RuntimeDirectoryStoreOptions) CommitOptions {
        return .{
            .manifest_path = self.manifest_path,
            .max_manifest_bytes = self.max_manifest_bytes,
            .max_segment_bytes = self.max_segment_bytes,
            .initial_segment_id = self.initial_segment_id,
        };
    }

    pub fn batchOptions(self: RuntimeDirectoryStoreOptions) DirectoryBatchWriterOptions {
        return .{
            .commit = self.commitOptions(),
            .max_pending_entries = self.max_pending_entries,
            .max_pending_value_bytes = self.max_pending_value_bytes,
            .max_pending_delta_records = self.max_pending_delta_records,
        };
    }

    pub fn maintenanceOptions(self: RuntimeDirectoryStoreOptions) DirectoryMaintenanceOptions {
        return .{
            .commit = self.commitOptions(),
            .plan = self.compaction_plan,
            .compact = self.compact_on_maintenance,
            .collect_orphan_segments = self.collect_orphan_segments_on_maintenance,
            .collect_temporary_files = self.collect_temporary_files_on_maintenance,
        };
    }
};

pub const DeltaTailWithStats = struct {
    records: []posting.PostingDeltaRecord,
    stats: posting.PostingDeltaTailStats,

    pub fn deinit(self: DeltaTailWithStats, alloc: Allocator) void {
        alloc.free(self.records);
    }
};

pub const RuntimeDirectoryStore = struct {
    alloc: Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    options: RuntimeDirectoryStoreOptions,
    lazy: LazyDirectoryStore,
    batch: DirectoryBatchWriter,

    pub fn openAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, options: RuntimeDirectoryStoreOptions) !RuntimeDirectoryStore {
        var manifest = try readManifestForCommitAlloc(alloc, io, dir, options.commitOptions());
        errdefer manifest.deinit(alloc);
        return initWithOwnedManifest(alloc, io, dir, options, manifest);
    }

    pub fn openAllocWithManifestData(alloc: Allocator, io: std.Io, dir: std.Io.Dir, options: RuntimeDirectoryStoreOptions, manifest_data: ?[]const u8) !RuntimeDirectoryStore {
        var manifest = if (manifest_data) |data|
            try decodeManifestAlloc(alloc, data)
        else
            try emptyManifestAlloc(alloc, options.initial_segment_id);
        errdefer manifest.deinit(alloc);
        return initWithOwnedManifest(alloc, io, dir, options, manifest);
    }

    fn initWithOwnedManifest(alloc: Allocator, io: std.Io, dir: std.Io.Dir, options: RuntimeDirectoryStoreOptions, manifest: OwnedManifest) RuntimeDirectoryStore {
        return .{
            .alloc = alloc,
            .io = io,
            .dir = dir,
            .options = options,
            .lazy = .{
                .manifest = manifest,
                .io = io,
                .dir = dir,
                .options = options.openOptions(),
            },
            .batch = DirectoryBatchWriter.init(alloc, io, dir, options.batchOptions()),
        };
    }

    pub fn deinit(self: *RuntimeDirectoryStore) void {
        self.batch.deinit();
        self.lazy.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn snapshot(self: *const RuntimeDirectoryStore) LazyDirectorySnapshot {
        return self.lazy.snapshot();
    }

    pub fn pendingEntries(self: RuntimeDirectoryStore) usize {
        return self.batch.pendingEntries();
    }

    pub fn pendingDeltaRecords(self: RuntimeDirectoryStore) usize {
        return self.batch.pendingDeltaRecords();
    }

    pub fn batchStats(self: RuntimeDirectoryStore) DirectoryBatchWriterStats {
        return self.batch.stats;
    }

    pub fn pendingBaseHeader(self: *const RuntimeDirectoryStore, posting_id: PostingId) !?posting.PostingBaseHeader {
        return try self.batch.pendingBaseHeader(posting_id);
    }

    pub fn pendingBaseStats(self: *const RuntimeDirectoryStore, posting_id: PostingId) !?posting.PostingBaseStats {
        return try self.batch.pendingBaseStats(posting_id);
    }

    pub fn pendingBase(self: *const RuntimeDirectoryStore, alloc: Allocator, posting_id: PostingId) !?posting.OwnedPostingBase {
        return try self.batch.pendingBase(alloc, posting_id);
    }

    pub fn pendingBaseDataAlloc(self: *const RuntimeDirectoryStore, alloc: Allocator, posting_id: PostingId) !?[]u8 {
        return try self.batch.pendingBaseDataAlloc(alloc, posting_id);
    }

    pub fn pendingCentroidDirectoryRecord(self: *const RuntimeDirectoryStore, alloc: Allocator, posting_id: PostingId) !?posting.OwnedCentroidDirectoryRecord {
        return try self.batch.pendingCentroidDirectoryRecord(alloc, posting_id);
    }

    pub fn pendingDeltaTailAlloc(self: *const RuntimeDirectoryStore, alloc: Allocator, posting_id: PostingId, min_generation: ?u64) ![]posting.PostingDeltaRecord {
        return try self.batch.pendingDeltaTailAlloc(alloc, posting_id, min_generation);
    }

    pub fn pendingDeltaTailWithStatsAlloc(self: *const RuntimeDirectoryStore, alloc: Allocator, posting_id: PostingId, base_generation: u64) !DeltaTailWithStats {
        return try self.batch.pendingDeltaTailWithStatsAlloc(alloc, posting_id, base_generation);
    }

    pub fn appendPendingDeltaTailIntoScratchWithStats(
        self: *const RuntimeDirectoryStore,
        alloc: Allocator,
        posting_id: PostingId,
        base_generation: u64,
        scratch: anytype,
    ) !posting.PostingDeltaTailStats {
        return try self.batch.appendPendingDeltaTailIntoScratchWithStats(alloc, posting_id, base_generation, scratch);
    }

    pub fn pendingDeltaTailStats(self: *const RuntimeDirectoryStore, posting_id: PostingId, base_generation: u64) !posting.PostingDeltaTailStats {
        return try self.batch.pendingDeltaTailStats(posting_id, base_generation);
    }

    pub fn pendingLatestDeltaOpAfterGenerationForMember(
        self: *const RuntimeDirectoryStore,
        posting_id: PostingId,
        vector_id: posting.VectorId,
        base_generation: u64,
    ) !?posting.PostingDeltaOp {
        return try self.batch.pendingLatestDeltaOpAfterGenerationForMember(posting_id, vector_id, base_generation);
    }

    pub fn pendingLatestDeltaRecordAfterGenerationForMember(
        self: *const RuntimeDirectoryStore,
        posting_id: PostingId,
        vector_id: posting.VectorId,
        base_generation: u64,
    ) !?posting.PostingDeltaRecord {
        return try self.batch.pendingLatestDeltaRecordAfterGenerationForMember(posting_id, vector_id, base_generation);
    }

    pub fn appendBase(self: *RuntimeDirectoryStore, posting_id: PostingId, value: []const u8) !void {
        try self.batch.appendBase(posting_id, value);
    }

    pub fn appendPostingBase(self: *RuntimeDirectoryStore, base: posting.PostingBase) !void {
        try self.batch.appendPostingBase(base);
    }

    pub fn appendCentroidDirectory(self: *RuntimeDirectoryStore, posting_id: PostingId, value: []const u8) !void {
        try self.batch.appendCentroidDirectory(posting_id, value);
    }

    pub fn appendCentroidDirectoryRecord(self: *RuntimeDirectoryStore, record: posting.CentroidDirectoryRecord) !void {
        try self.batch.appendCentroidDirectoryRecord(record);
    }

    pub fn appendDelta(self: *RuntimeDirectoryStore, posting_id: PostingId, sequence: u64, value: []const u8) !void {
        try self.batch.appendDelta(posting_id, sequence, value);
    }

    pub fn appendPostingDeltaRecords(self: *RuntimeDirectoryStore, posting_id: PostingId, records: []const posting.PostingDeltaRecord) !void {
        try self.batch.appendPostingDeltaRecords(posting_id, records);
    }

    pub fn appendDeltaRecord(self: *RuntimeDirectoryStore, posting_id: PostingId, record: posting.PostingDeltaRecord) !void {
        try self.batch.appendDeltaRecord(posting_id, record);
    }

    pub fn flush(self: *RuntimeDirectoryStore) !?SegmentCommitStats {
        const result = try self.batch.flushWithManifest(self.lazy.manifest);
        if (result != null) try self.reloadManifest();
        return result;
    }

    pub fn maintain(self: *RuntimeDirectoryStore) !DirectoryMaintenanceStats {
        _ = try self.flush();
        const stats = try maintainDirectoryStoreAlloc(self.alloc, self.io, self.dir, self.options.maintenanceOptions());
        try self.reloadManifest();
        return stats;
    }

    pub fn writeLoadedManifest(self: RuntimeDirectoryStore) !void {
        const manifest_data = try self.manifestBytesAlloc(self.alloc);
        defer self.alloc.free(manifest_data);
        try writeManifestFileAlloc(self.alloc, self.io, self.dir, self.options.manifest_path, manifest_data);
    }

    pub fn maintainLoadedManifest(self: *RuntimeDirectoryStore) !DirectoryMaintenanceStats {
        return try self.maintainLoadedManifestWithOptions(self.options.maintenanceOptions());
    }

    pub fn maintainLoadedManifestWithOptions(self: *RuntimeDirectoryStore, options: DirectoryMaintenanceOptions) !DirectoryMaintenanceStats {
        if (self.pendingEntries() != 0 or self.pendingDeltaRecords() != 0) return error.PostingSegmentPendingBatch;
        try self.writeLoadedManifest();
        const stats = try maintainDirectoryStoreAlloc(self.alloc, self.io, self.dir, options);
        try self.reloadManifest();
        return stats;
    }

    pub fn reloadManifest(self: *RuntimeDirectoryStore) !void {
        var manifest = try readManifestForCommitAlloc(self.alloc, self.io, self.dir, self.options.commitOptions());
        errdefer manifest.deinit(self.alloc);
        self.lazy.manifest.deinit(self.alloc);
        self.lazy.manifest = manifest;
    }

    pub fn reloadManifestData(self: *RuntimeDirectoryStore, manifest_data: ?[]const u8) !void {
        var manifest = if (manifest_data) |data|
            try decodeManifestAlloc(self.alloc, data)
        else
            try emptyManifestAlloc(self.alloc, self.options.initial_segment_id);
        errdefer manifest.deinit(self.alloc);
        self.lazy.manifest.deinit(self.alloc);
        self.lazy.manifest = manifest;
    }

    pub fn manifestBytesAlloc(self: RuntimeDirectoryStore, alloc: Allocator) ![]u8 {
        const entries = try manifestEntryViewAlloc(alloc, self.lazy.manifest.segments);
        defer alloc.free(entries);
        return try encodeManifestAlloc(alloc, .{
            .next_segment_id = self.lazy.manifest.next_segment_id,
            .segments = entries,
        });
    }
};

pub const Catalog = struct {
    segments: []const SegmentBlob,

    pub fn getBase(self: Catalog, posting_id: PostingId) !?[]const u8 {
        return try self.getLatestExact(posting_id, .base);
    }

    pub fn getCentroidDirectory(self: Catalog, posting_id: PostingId) !?[]const u8 {
        return try self.getLatestExact(posting_id, .centroid_directory);
    }

    pub fn collectDeltas(self: Catalog, alloc: Allocator, posting_id: PostingId) ![]DeltaValue {
        var deltas = std.ArrayListUnmanaged(DeltaValue).empty;
        errdefer deltas.deinit(alloc);
        for (self.segments) |segment| {
            if (!segment.meta.mayContainPosting(posting_id)) continue;
            var reader = try Reader.init(segment.data);
            var iter = reader.deltas(posting_id);
            while (try iter.next()) |delta| {
                try deltas.append(alloc, delta);
            }
        }
        std.mem.sort(DeltaValue, deltas.items, {}, deltaValueLessThan);
        return try deltas.toOwnedSlice(alloc);
    }

    pub fn collectDeltasAfterGeneration(self: Catalog, alloc: Allocator, posting_id: PostingId, base_generation: u64) ![]DeltaValue {
        var deltas = std.ArrayListUnmanaged(DeltaValue).empty;
        errdefer deltas.deinit(alloc);
        for (self.segments) |segment| {
            if (!segment.meta.mayContainPosting(posting_id)) continue;
            if (segment.meta.max_delta_sequence != 0 and posting.PostingFormat.deltaSequenceGeneration(segment.meta.max_delta_sequence) <= base_generation) continue;
            var reader = try Reader.init(segment.data);
            var iter = reader.deltas(posting_id);
            while (try iter.next()) |delta| {
                try deltas.append(alloc, delta);
            }
        }
        std.mem.sort(DeltaValue, deltas.items, {}, deltaValueLessThan);
        return try deltas.toOwnedSlice(alloc);
    }

    pub fn appendDeltaRecords(
        self: Catalog,
        alloc: Allocator,
        posting_id: PostingId,
        min_generation: ?u64,
        records: *std.ArrayListUnmanaged(posting.PostingDeltaRecord),
    ) !void {
        for (self.segments) |segment| {
            if (!segment.meta.mayContainPosting(posting_id)) continue;
            if (min_generation) |generation| {
                if (segment.meta.max_delta_sequence != 0 and posting.PostingFormat.deltaSequenceGeneration(segment.meta.max_delta_sequence) <= generation) continue;
            }

            var reader = try Reader.init(segment.data);
            var iter = reader.deltas(posting_id);
            while (try iter.next()) |delta_value| {
                var delta_iter = try posting.PostingFormat.DeltaTailIterator.init(delta_value.value);
                if (min_generation) |generation| {
                    while (try delta_iter.next()) |record| {
                        if (posting.PostingFormat.deltaSequenceGeneration(record.sequence) <= generation) continue;
                        try records.append(alloc, record);
                    }
                } else {
                    try records.ensureUnusedCapacity(alloc, delta_iter.recordCount());
                    while (try delta_iter.next()) |record| records.appendAssumeCapacity(record);
                }
            }
        }
    }

    fn getLatestExact(self: Catalog, posting_id: PostingId, kind: EntryKind) !?[]const u8 {
        var best_segment_id: u64 = 0;
        var best: ?[]const u8 = null;
        for (self.segments) |segment| {
            if (!segment.meta.mayContainPosting(posting_id)) continue;
            if (best != null and segment.meta.segment_id <= best_segment_id) continue;
            var reader = try Reader.init(segment.data);
            const value = switch (kind) {
                .base => try reader.getBase(posting_id),
                .centroid_directory => try reader.getCentroidDirectory(posting_id),
                .delta => null,
            };
            if (value) |found| {
                best_segment_id = segment.meta.segment_id;
                best = found;
            }
        }
        return best;
    }
};

pub const LazyDirectorySnapshot = struct {
    manifest: *const OwnedManifest,
    io: std.Io,
    dir: std.Io.Dir,
    options: OpenStoreOptions,

    pub fn loadBaseHeader(self: LazyDirectorySnapshot, alloc: Allocator, posting_id: PostingId) !?posting.PostingBaseHeader {
        const base_data = (try self.readLatestPointValueAlloc(alloc, posting_id, .base)) orelse return null;
        defer alloc.free(base_data);
        return try posting.PostingFormat.decodeBaseHeader(base_data);
    }

    pub fn loadBaseStats(self: LazyDirectorySnapshot, alloc: Allocator, posting_id: PostingId) !?posting.PostingBaseStats {
        const base_data = (try self.readLatestPointValueAlloc(alloc, posting_id, .base)) orelse return null;
        defer alloc.free(base_data);
        return try posting.PostingFormat.decodeBaseStats(base_data);
    }

    pub fn loadBase(self: LazyDirectorySnapshot, alloc: Allocator, posting_id: PostingId) !?posting.OwnedPostingBase {
        const base_data = (try self.readLatestPointValueAlloc(alloc, posting_id, .base)) orelse return null;
        defer alloc.free(base_data);
        return try posting.PostingFormat.decodeBase(alloc, base_data);
    }

    pub fn loadBaseData(self: LazyDirectorySnapshot, alloc: Allocator, posting_id: PostingId) !?[]u8 {
        return try self.readLatestPointValueAlloc(alloc, posting_id, .base);
    }

    pub fn loadCentroidDirectoryRecord(self: LazyDirectorySnapshot, alloc: Allocator, posting_id: PostingId) !?posting.OwnedCentroidDirectoryRecord {
        const centroid_data = (try self.readLatestPointValueAlloc(alloc, posting_id, .centroid_directory)) orelse return null;
        defer alloc.free(centroid_data);
        return try posting.CentroidDirectoryFormat.decode(alloc, centroid_data);
    }

    pub fn loadDeltaTail(self: LazyDirectorySnapshot, alloc: Allocator, posting_id: PostingId) ![]posting.PostingDeltaRecord {
        return try self.loadDeltaTailFilteredAlloc(alloc, posting_id, null);
    }

    pub fn loadDeltaTailAfterGeneration(self: LazyDirectorySnapshot, alloc: Allocator, posting_id: PostingId, generation: u64) ![]posting.PostingDeltaRecord {
        return try self.loadDeltaTailFilteredAlloc(alloc, posting_id, generation);
    }

    pub fn loadDeltaTailAfterGenerationWithStats(self: LazyDirectorySnapshot, alloc: Allocator, posting_id: PostingId, generation: u64) !DeltaTailWithStats {
        return try self.loadDeltaTailFilteredWithStatsAlloc(alloc, posting_id, generation);
    }

    pub fn appendDeltaTailAfterGenerationIntoScratchWithStats(
        self: LazyDirectorySnapshot,
        alloc: Allocator,
        posting_id: PostingId,
        generation: u64,
        scratch: anytype,
    ) !posting.PostingDeltaTailStats {
        var stats = posting.PostingDeltaTailStats{};
        for (self.manifest.segments) |entry| {
            if (!entry.meta.mayContainPosting(posting_id)) continue;
            if (entry.meta.max_delta_sequence != 0 and posting.PostingFormat.deltaSequenceGeneration(entry.meta.max_delta_sequence) <= generation) continue;
            try self.readDeltaRecordsIntoScratchWithStatsAlloc(alloc, entry, posting_id, generation, scratch, &stats);
        }
        return stats;
    }

    pub fn applyDeltaTailAfterGenerationIntoScratch(
        self: LazyDirectorySnapshot,
        alloc: Allocator,
        posting_id: PostingId,
        scratch: anytype,
        member_count: *usize,
        base_generation: u64,
        base_members_are_sorted: bool,
    ) !posting.DeltaReplayResult {
        scratch.resetDeltaRecords();
        errdefer scratch.resetDeltaRecords();

        var result = posting.DeltaReplayResult{};
        for (self.manifest.segments) |entry| {
            if (!entry.meta.mayContainPosting(posting_id)) continue;
            if (entry.meta.max_delta_sequence != 0 and posting.PostingFormat.deltaSequenceGeneration(entry.meta.max_delta_sequence) <= base_generation) continue;
            if (entry.meta.byte_len > self.options.max_segment_bytes) return error.PostingSegmentTooLarge;

            const manifest_entry = ManifestEntry{
                .meta = entry.meta,
                .path = entry.path,
            };
            const index_data = try readSegmentIndexAlloc(alloc, self.io, self.dir, manifest_entry);
            defer alloc.free(index_data);

            var delta_index = lowerBoundIndexData(index_data, entry.meta.entry_count, posting_id, .delta, 0);
            while (delta_index < entry.meta.entry_count) : (delta_index += 1) {
                const delta_entry = try indexEntryFromBytes(index_data[delta_index * index_entry_size ..][0..index_entry_size]);
                if (delta_entry.posting_id != posting_id or delta_entry.kind != .delta) break;
                const value = try readSegmentEntryValueAlloc(alloc, self.io, self.dir, manifest_entry, delta_entry);
                defer alloc.free(value);
                var iterator = try posting.PostingFormat.DeltaTailIterator.init(value);
                while (try iterator.next()) |record| {
                    if (posting.PostingFormat.deltaSequenceGeneration(record.sequence) <= base_generation) continue;
                    try appendDeltaRecordToScratch(alloc, scratch, record);
                    result.records += 1;
                    result.max_sequence = @max(result.max_sequence, record.sequence);
                }
            }
        }

        const records = scratch.deltaRecordsMut();
        std.mem.sort(posting.PostingDeltaRecord, records, {}, postingDeltaRecordLessThan);

        if (base_members_are_sorted and records.len <= compact_sorted_delta_max_records) {
            var compact_ids: [compact_sorted_delta_max_records]posting.VectorId = undefined;
            var compact_ops: [compact_sorted_delta_max_records]posting.PostingDeltaOp = undefined;
            for (records, 0..) |record, i| {
                compact_ids[i] = record.vector_id;
                compact_ops[i] = record.op;
            }
            member_count.* = try posting.PostingFormat.applySortedCompactOpsToSortedScratch(
                alloc,
                scratch,
                member_count.*,
                compact_ids[0..records.len],
                compact_ops[0..records.len],
            );
            return result;
        }

        const Scratch = switch (@typeInfo(@TypeOf(scratch))) {
            .pointer => |ptr| ptr.child,
            else => @TypeOf(scratch),
        };
        const can_use_overlay_plan = comptime @hasDecl(Scratch, "resetPostingOverlayApply") and
            @hasDecl(Scratch, "ensurePostingOverlayAppendCapacity") and
            @hasField(Scratch, "member_ids");
        if (can_use_overlay_plan and records.len > overlay_plan_min_delta_records) {
            scratch.resetPostingOverlayApply();
            for (records) |record| {
                try posting.PostingFormat.applyPostingOverlayRecord(alloc, scratch, record);
            }
            member_count.* = try compactMembersWithOverlayPlan(alloc, scratch, member_count.*);
            return result;
        }

        try scratch.ensureMemberIdCapacity(alloc, member_count.* + posting.PostingFormat.liveDeltaRecordCount(records));
        for (records) |record| {
            posting.PostingFormat.applyDeltaRecordToScratch(scratch, member_count, record);
        }
        return result;
    }

    pub fn deltaTailStats(self: LazyDirectorySnapshot, alloc: Allocator, posting_id: PostingId, base_generation: u64) !posting.PostingDeltaTailStats {
        var out = posting.PostingDeltaTailStats{};
        for (self.manifest.segments) |entry| {
            if (!entry.meta.mayContainPosting(posting_id)) continue;
            if (entry.meta.max_delta_sequence != 0 and posting.PostingFormat.deltaSequenceGeneration(entry.meta.max_delta_sequence) <= base_generation) continue;

            const stats = try self.readDeltaTailStatsAlloc(alloc, entry, posting_id, base_generation);
            out.records += stats.records;
            out.records_after_generation += stats.records_after_generation;
            out.tombstones_after_generation += stats.tombstones_after_generation;
            out.encoded_value_bytes += stats.encoded_value_bytes;
            out.max_sequence_after_generation = @max(out.max_sequence_after_generation, stats.max_sequence_after_generation);
        }
        return out;
    }

    pub fn latestDeltaOpAfterGenerationForMember(
        self: LazyDirectorySnapshot,
        alloc: Allocator,
        posting_id: PostingId,
        vector_id: posting.VectorId,
        base_generation: u64,
    ) !?posting.PostingDeltaOp {
        const record = try self.latestDeltaRecordAfterGenerationForMember(alloc, posting_id, vector_id, base_generation);
        return if (record) |found| found.op else null;
    }

    pub fn latestDeltaRecordAfterGenerationForMember(
        self: LazyDirectorySnapshot,
        alloc: Allocator,
        posting_id: PostingId,
        vector_id: posting.VectorId,
        base_generation: u64,
    ) !?posting.PostingDeltaRecord {
        var best_sequence: u64 = 0;
        var best_record: ?posting.PostingDeltaRecord = null;
        for (self.manifest.segments) |entry| {
            if (!entry.meta.mayContainPosting(posting_id)) continue;
            if (entry.meta.max_delta_sequence != 0 and posting.PostingFormat.deltaSequenceGeneration(entry.meta.max_delta_sequence) <= base_generation) continue;
            if (entry.meta.byte_len > self.options.max_segment_bytes) return error.PostingSegmentTooLarge;

            const manifest_entry = ManifestEntry{
                .meta = entry.meta,
                .path = entry.path,
            };
            const index_data = try readSegmentIndexAlloc(alloc, self.io, self.dir, manifest_entry);
            defer alloc.free(index_data);

            var delta_index = lowerBoundIndexData(index_data, entry.meta.entry_count, posting_id, .delta, 0);
            while (delta_index < entry.meta.entry_count) : (delta_index += 1) {
                const delta_entry = try indexEntryFromBytes(index_data[delta_index * index_entry_size ..][0..index_entry_size]);
                if (delta_entry.posting_id != posting_id or delta_entry.kind != .delta) break;
                const value = try readSegmentEntryValueAlloc(alloc, self.io, self.dir, manifest_entry, delta_entry);
                defer alloc.free(value);
                var iterator = try posting.PostingFormat.DeltaTailIterator.init(value);
                while (try iterator.next()) |record| {
                    if (record.vector_id != vector_id) continue;
                    if (posting.PostingFormat.deltaSequenceGeneration(record.sequence) <= base_generation) continue;
                    if (best_record == null or record.sequence >= best_sequence) {
                        best_sequence = record.sequence;
                        best_record = record;
                    }
                }
            }
        }
        return best_record;
    }

    pub fn materializeMembers(self: LazyDirectorySnapshot, alloc: Allocator, posting_id: PostingId) !?[]posting.VectorId {
        var scratch = posting.PostingStore.FoldScratch{};
        defer scratch.deinit(alloc);

        const member_count = (try self.materializeMembersIntoScratch(alloc, posting_id, &scratch)) orelse return null;
        return try alloc.dupe(posting.VectorId, scratch.member_ids[0..member_count]);
    }

    pub fn materializeMembersIntoScratch(self: LazyDirectorySnapshot, alloc: Allocator, posting_id: PostingId, scratch: anytype) !?usize {
        var found_base = (try self.loadBase(alloc, posting_id)) orelse return null;
        defer found_base.deinit(alloc);

        scratch.resetDeltaRecords();
        errdefer scratch.resetDeltaRecords();

        _ = try self.appendDeltaTailAfterGenerationIntoScratchWithStats(alloc, posting_id, found_base.generation, scratch);
        const records = scratch.deltaRecordsMut();
        std.mem.sort(posting.PostingDeltaRecord, records, {}, postingDeltaRecordLessThan);

        try scratch.ensureMemberIdCapacity(alloc, found_base.members.len + posting.PostingFormat.liveDeltaRecordCount(records));
        @memcpy(scratch.member_ids[0..found_base.members.len], found_base.members);
        var member_count = found_base.members.len;
        for (records) |record| {
            posting.PostingFormat.applyDeltaRecordToScratch(scratch, &member_count, record);
        }
        return member_count;
    }

    pub fn materializeSortedMembersIntoScratch(self: LazyDirectorySnapshot, alloc: Allocator, posting_id: PostingId, scratch: anytype) !?usize {
        const found_base_data = (try self.loadBaseData(alloc, posting_id)) orelse return null;
        defer alloc.free(found_base_data);
        const base_header = try posting.PostingFormat.decodeBaseHeader(found_base_data);
        scratch.resetDeltaRecords();
        scratch.resetCompactDeltaRecords();
        errdefer {
            scratch.resetDeltaRecords();
            scratch.resetCompactDeltaRecords();
        }

        _ = try self.appendDeltaTailAfterGenerationIntoScratchWithStats(alloc, posting_id, base_header.generation, scratch);
        const records = scratch.deltaRecordsMut();
        std.mem.sort(posting.PostingDeltaRecord, records, {}, postingDeltaRecordLessThan);
        try scratch.ensureCompactDeltaCapacity(alloc, records.len);
        for (records) |record| {
            scratch.appendCompactDeltaRecordAssumeCapacity(record);
        }
        return try posting.PostingFormat.materializeSortedBaseWithCompactDeltaRecordsIntoScratch(alloc, scratch, found_base_data);
    }

    fn loadDeltaTailFilteredAlloc(self: LazyDirectorySnapshot, alloc: Allocator, posting_id: PostingId, min_generation: ?u64) ![]posting.PostingDeltaRecord {
        const loaded = try self.loadDeltaTailFilteredWithStatsAlloc(alloc, posting_id, min_generation);
        return loaded.records;
    }

    fn loadDeltaTailFilteredWithStatsAlloc(self: LazyDirectorySnapshot, alloc: Allocator, posting_id: PostingId, min_generation: ?u64) !DeltaTailWithStats {
        var records = std.ArrayListUnmanaged(posting.PostingDeltaRecord).empty;
        errdefer records.deinit(alloc);
        var stats = posting.PostingDeltaTailStats{};

        for (self.manifest.segments) |entry| {
            if (!entry.meta.mayContainPosting(posting_id)) continue;
            if (min_generation) |generation| {
                if (entry.meta.max_delta_sequence != 0 and posting.PostingFormat.deltaSequenceGeneration(entry.meta.max_delta_sequence) <= generation) continue;
            }

            try self.readDeltaRecordsWithStatsAlloc(alloc, entry, posting_id, min_generation, &records, &stats);
        }

        std.mem.sort(posting.PostingDeltaRecord, records.items, {}, postingDeltaRecordLessThan);
        return .{
            .records = try records.toOwnedSlice(alloc),
            .stats = stats,
        };
    }

    fn readSegmentAlloc(self: LazyDirectorySnapshot, alloc: Allocator, entry: OwnedManifestEntry) ![]u8 {
        if (entry.meta.byte_len > self.options.max_segment_bytes) return error.PostingSegmentTooLarge;
        const segment_data = try readSegmentFileAlloc(alloc, self.io, self.dir, entry.path, entry.meta.byte_len);
        errdefer alloc.free(segment_data);
        try validateSegmentDataMatchesMeta(segment_data, entry.meta);
        return segment_data;
    }

    fn readPointValueAlloc(self: LazyDirectorySnapshot, alloc: Allocator, entry: OwnedManifestEntry, posting_id: PostingId, kind: EntryKind) !?[]u8 {
        if (entry.meta.byte_len > self.options.max_segment_bytes) return error.PostingSegmentTooLarge;
        return try readSegmentPointValueAlloc(alloc, self.io, self.dir, .{
            .meta = entry.meta,
            .path = entry.path,
        }, posting_id, kind);
    }

    fn readLatestPointValueAlloc(self: LazyDirectorySnapshot, alloc: Allocator, posting_id: PostingId, kind: EntryKind) !?[]u8 {
        var best_segment_id: u64 = 0;
        var best_value: ?[]u8 = null;
        errdefer if (best_value) |value| alloc.free(value);

        for (self.manifest.segments) |entry| {
            if (!entry.meta.mayContainPosting(posting_id)) continue;
            if (entry.meta.segment_id <= best_segment_id) continue;

            const value = (try self.readPointValueAlloc(alloc, entry, posting_id, kind)) orelse continue;
            if (best_value) |previous| alloc.free(previous);
            best_value = value;
            best_segment_id = entry.meta.segment_id;
        }
        return best_value;
    }

    fn readDeltaRecordsAlloc(self: LazyDirectorySnapshot, alloc: Allocator, entry: OwnedManifestEntry, posting_id: PostingId, min_generation: ?u64) ![]posting.PostingDeltaRecord {
        if (entry.meta.byte_len > self.options.max_segment_bytes) return error.PostingSegmentTooLarge;
        return try readSegmentDeltaRecordsAlloc(alloc, self.io, self.dir, .{
            .meta = entry.meta,
            .path = entry.path,
        }, posting_id, min_generation);
    }

    fn readDeltaRecordsWithStatsAlloc(
        self: LazyDirectorySnapshot,
        alloc: Allocator,
        entry: OwnedManifestEntry,
        posting_id: PostingId,
        min_generation: ?u64,
        records: *std.ArrayListUnmanaged(posting.PostingDeltaRecord),
        stats: *posting.PostingDeltaTailStats,
    ) !void {
        if (entry.meta.byte_len > self.options.max_segment_bytes) return error.PostingSegmentTooLarge;
        try readSegmentDeltaRecordsWithStatsAlloc(alloc, self.io, self.dir, .{
            .meta = entry.meta,
            .path = entry.path,
        }, posting_id, min_generation, records, stats);
    }

    fn readDeltaRecordsIntoScratchWithStatsAlloc(
        self: LazyDirectorySnapshot,
        alloc: Allocator,
        entry: OwnedManifestEntry,
        posting_id: PostingId,
        base_generation: u64,
        scratch: anytype,
        stats: *posting.PostingDeltaTailStats,
    ) !void {
        if (entry.meta.byte_len > self.options.max_segment_bytes) return error.PostingSegmentTooLarge;
        try readSegmentDeltaRecordsIntoScratchWithStatsAlloc(alloc, self.io, self.dir, .{
            .meta = entry.meta,
            .path = entry.path,
        }, posting_id, base_generation, scratch, stats);
    }

    fn readDeltaTailStatsAlloc(self: LazyDirectorySnapshot, alloc: Allocator, entry: OwnedManifestEntry, posting_id: PostingId, base_generation: u64) !posting.PostingDeltaTailStats {
        if (entry.meta.byte_len > self.options.max_segment_bytes) return error.PostingSegmentTooLarge;
        return try readSegmentDeltaTailStatsAlloc(alloc, self.io, self.dir, .{
            .meta = entry.meta,
            .path = entry.path,
        }, posting_id, base_generation);
    }
};

pub const Snapshot = struct {
    catalog: Catalog,

    pub fn getBaseBytes(self: Snapshot, posting_id: PostingId) !?[]const u8 {
        return try self.catalog.getBase(posting_id);
    }

    pub fn loadBaseHeader(self: Snapshot, posting_id: PostingId) !?posting.PostingBaseHeader {
        const base_data = (try self.getBaseBytes(posting_id)) orelse return null;
        return try posting.PostingFormat.decodeBaseHeader(base_data);
    }

    pub fn loadBaseStats(self: Snapshot, posting_id: PostingId) !?posting.PostingBaseStats {
        const base_data = (try self.getBaseBytes(posting_id)) orelse return null;
        return try posting.PostingFormat.decodeBaseStats(base_data);
    }

    pub fn loadBase(self: Snapshot, alloc: Allocator, posting_id: PostingId) !?posting.OwnedPostingBase {
        const base_data = (try self.getBaseBytes(posting_id)) orelse return null;
        return try posting.PostingFormat.decodeBase(alloc, base_data);
    }

    pub fn loadCentroidDirectoryRecord(self: Snapshot, alloc: Allocator, posting_id: PostingId) !?posting.OwnedCentroidDirectoryRecord {
        const centroid_data = (try self.catalog.getCentroidDirectory(posting_id)) orelse return null;
        return try posting.CentroidDirectoryFormat.decode(alloc, centroid_data);
    }

    pub fn loadDeltaTail(self: Snapshot, alloc: Allocator, posting_id: PostingId) ![]posting.PostingDeltaRecord {
        var records = std.ArrayListUnmanaged(posting.PostingDeltaRecord).empty;
        errdefer records.deinit(alloc);
        try self.catalog.appendDeltaRecords(alloc, posting_id, null, &records);
        std.mem.sort(posting.PostingDeltaRecord, records.items, {}, postingDeltaRecordLessThan);
        return try records.toOwnedSlice(alloc);
    }

    pub fn loadDeltaTailAfterGeneration(self: Snapshot, alloc: Allocator, posting_id: PostingId, generation: u64) ![]posting.PostingDeltaRecord {
        var records = std.ArrayListUnmanaged(posting.PostingDeltaRecord).empty;
        errdefer records.deinit(alloc);
        try self.catalog.appendDeltaRecords(alloc, posting_id, generation, &records);
        std.mem.sort(posting.PostingDeltaRecord, records.items, {}, postingDeltaRecordLessThan);
        return try records.toOwnedSlice(alloc);
    }

    pub fn materializeMembers(self: Snapshot, alloc: Allocator, posting_id: PostingId) !?[]posting.VectorId {
        var base = (try self.loadBase(alloc, posting_id)) orelse return null;
        defer base.deinit(alloc);
        const records = try self.loadDeltaTailAfterGeneration(alloc, posting_id, base.generation);
        defer alloc.free(records);
        return try posting.PostingFormat.materializeMembersAfterGeneration(alloc, base.members, records, base.generation);
    }
};

pub fn encodeManifestAlloc(alloc: Allocator, manifest: Manifest) ![]u8 {
    if (manifest.segments.len > std.math.maxInt(u32)) return error.PostingSegmentManifestTooLarge;
    try validateManifest(manifest);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, &manifest_magic);
    try appendU16(alloc, &out, version);
    try appendU32(alloc, &out, @intCast(manifest.segments.len));
    try appendU64(alloc, &out, manifest.next_segment_id);
    for (manifest.segments) |entry| {
        try appendManifestEntry(alloc, &out, entry);
    }
    try appendU32(alloc, &out, manifestChecksum(out.items));
    return try out.toOwnedSlice(alloc);
}

pub fn decodeManifestAlloc(alloc: Allocator, data: []const u8) !OwnedManifest {
    if (data.len < manifest_header_size + manifest_checksum_size) return error.CorruptedPostingSegmentManifest;
    if (!std.mem.eql(u8, data[0..4], &manifest_magic)) return error.BadPostingSegmentManifestMagic;
    if (readU16(data[4..6]) != version) return error.UnsupportedPostingSegmentManifestVersion;
    const manifest_end = data.len - manifest_checksum_size;
    const stored_checksum = readU32(data[manifest_end..][0..4]);
    if (manifestChecksum(data[0..manifest_end]) != stored_checksum) return error.BadPostingSegmentManifestChecksum;
    const entry_count_u32 = readU32(data[6..10]);
    const next_segment_id = readU64(data[10..18]);
    const entry_count = std.math.cast(usize, entry_count_u32) orelse return error.CorruptedPostingSegmentManifest;
    var pos: usize = manifest_header_size;
    const entries = try alloc.alloc(OwnedManifestEntry, entry_count);
    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |entry| alloc.free(entry.path);
        alloc.free(entries);
    }
    while (initialized < entries.len) {
        const entry = try readManifestEntry(alloc, data, &pos);
        if (initialized != 0 and entries[initialized - 1].meta.segment_id >= entry.meta.segment_id) {
            alloc.free(entry.path);
            return error.InvalidPostingSegmentManifest;
        }
        entries[initialized] = entry;
        initialized += 1;
    }
    if (pos != manifest_end) return error.CorruptedPostingSegmentManifest;
    return .{
        .next_segment_id = next_segment_id,
        .segments = entries,
    };
}

pub fn replaceManifestSegmentsAlloc(
    alloc: Allocator,
    manifest: Manifest,
    remove_segment_ids: []const u64,
    new_entries: []const ManifestEntry,
) ![]u8 {
    const result = try replaceManifestSegmentsWithStatsAlloc(alloc, manifest, remove_segment_ids, new_entries);
    return result.encoded;
}

pub fn replaceManifestSegmentsWithStatsAlloc(
    alloc: Allocator,
    manifest: Manifest,
    remove_segment_ids: []const u64,
    new_entries: []const ManifestEntry,
) !ManifestReplacementResult {
    try validateManifest(manifest);
    try rejectDuplicateSegmentIds(remove_segment_ids);
    for (new_entries) |entry| try validateManifestEntry(entry);

    var stats = ManifestReplacementStats{
        .input_segments = manifest.segments.len,
        .added_segments = new_entries.len,
    };
    var found_remove_count: usize = 0;
    var output_entries = std.ArrayListUnmanaged(ManifestEntry).empty;
    errdefer output_entries.deinit(alloc);
    try output_entries.ensureTotalCapacity(alloc, manifest.segments.len + new_entries.len);

    for (manifest.segments) |entry| {
        if (segmentIdIn(entry.meta.segment_id, remove_segment_ids)) {
            found_remove_count += 1;
            continue;
        }
        output_entries.appendAssumeCapacity(entry);
    }
    if (found_remove_count != remove_segment_ids.len) return error.PostingSegmentManifestReplacementMissingSegment;
    stats.removed_segments = found_remove_count;

    for (new_entries) |entry| output_entries.appendAssumeCapacity(entry);
    std.mem.sort(ManifestEntry, output_entries.items, {}, manifestEntryLessThan);

    var next_segment_id = manifest.next_segment_id;
    for (new_entries) |entry| {
        next_segment_id = @max(next_segment_id, std.math.add(u64, entry.meta.segment_id, 1) catch return error.PostingSegmentManifestTooLarge);
    }
    stats.output_segments = output_entries.items.len;
    stats.next_segment_id = next_segment_id;

    const encoded = try encodeManifestAlloc(alloc, .{
        .next_segment_id = next_segment_id,
        .segments = output_entries.items,
    });
    output_entries.deinit(alloc);
    return .{
        .encoded = encoded,
        .stats = stats,
    };
}

pub fn summarizeManifest(manifest: Manifest) !DirectoryManifestStats {
    try validateManifest(manifest);
    var stats = DirectoryManifestStats{
        .segments = manifest.segments.len,
        .next_segment_id = manifest.next_segment_id,
    };
    var saw_segment = false;
    for (manifest.segments) |entry| {
        stats.bytes = std.math.add(usize, stats.bytes, entry.meta.byte_len) catch return error.PostingSegmentTooLarge;
        stats.entries = std.math.add(usize, stats.entries, entry.meta.entry_count) catch return error.PostingSegmentTooLarge;
        if (!saw_segment) {
            saw_segment = true;
            stats.min_segment_id = entry.meta.segment_id;
            stats.max_segment_id = entry.meta.segment_id;
            stats.min_posting_id = entry.meta.min_posting_id;
            stats.max_posting_id = entry.meta.max_posting_id;
        } else {
            stats.min_segment_id = @min(stats.min_segment_id, entry.meta.segment_id);
            stats.max_segment_id = @max(stats.max_segment_id, entry.meta.segment_id);
            stats.min_posting_id = @min(stats.min_posting_id, entry.meta.min_posting_id);
            stats.max_posting_id = @max(stats.max_posting_id, entry.meta.max_posting_id);
        }
        if (entry.meta.min_delta_sequence != 0) {
            if (stats.min_delta_sequence == 0 or entry.meta.min_delta_sequence < stats.min_delta_sequence) {
                stats.min_delta_sequence = entry.meta.min_delta_sequence;
            }
            stats.max_delta_sequence = @max(stats.max_delta_sequence, entry.meta.max_delta_sequence);
        }
    }
    return stats;
}

pub fn summarizeDirectoryManifestAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, options: OpenStoreOptions) !DirectoryManifestStats {
    var manifest = try readManifestFromDirectoryAlloc(alloc, io, dir, options);
    defer manifest.deinit(alloc);
    const entries = try manifestEntryViewAlloc(alloc, manifest.segments);
    defer alloc.free(entries);
    return try summarizeManifest(.{
        .next_segment_id = manifest.next_segment_id,
        .segments = entries,
    });
}

pub fn planDirectoryCompactionAlloc(alloc: Allocator, manifest: Manifest, options: DirectoryCompactionPlanOptions) !DirectoryCompactionPlan {
    try validateManifest(manifest);
    const min_input_segments = @max(options.min_input_segments, 1);

    var stats = DirectoryCompactionPlanStats{
        .manifest_segments = manifest.segments.len,
    };
    var selected_ids = std.ArrayListUnmanaged(u64).empty;
    errdefer selected_ids.deinit(alloc);

    var selected_bytes: usize = 0;
    var selected_entries: usize = 0;
    for (manifest.segments) |entry| {
        if (options.max_input_segments != 0 and selected_ids.items.len >= options.max_input_segments) {
            stats.stopped_on_segment_limit = true;
            break;
        }
        const next_bytes = std.math.add(usize, selected_bytes, entry.meta.byte_len) catch return error.PostingSegmentTooLarge;
        if (options.max_input_bytes != 0 and next_bytes > options.max_input_bytes) {
            stats.stopped_on_byte_limit = true;
            break;
        }
        try selected_ids.append(alloc, entry.meta.segment_id);
        selected_bytes = next_bytes;
        selected_entries += entry.meta.entry_count;
    }

    if (selected_ids.items.len < min_input_segments) {
        stats.insufficient_segments = true;
        selected_ids.clearRetainingCapacity();
        selected_bytes = 0;
        selected_entries = 0;
    }

    stats.selected_segments = selected_ids.items.len;
    stats.selected_bytes = selected_bytes;
    stats.selected_entries = selected_entries;
    return .{
        .segment_ids = try selected_ids.toOwnedSlice(alloc),
        .stats = stats,
    };
}

pub fn planDirectoryCompactionFromDirectoryAlloc(
    alloc: Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    store_options: OpenStoreOptions,
    plan_options: DirectoryCompactionPlanOptions,
) !DirectoryCompactionPlan {
    var manifest = try readManifestFromDirectoryAlloc(alloc, io, dir, store_options);
    defer manifest.deinit(alloc);
    const entries = try manifestEntryViewAlloc(alloc, manifest.segments);
    defer alloc.free(entries);
    return try planDirectoryCompactionAlloc(alloc, .{
        .next_segment_id = manifest.next_segment_id,
        .segments = entries,
    }, plan_options);
}

pub fn openStoreAlloc(alloc: Allocator, manifest_data: []const u8, context: anytype, comptime readSegmentAlloc: anytype) !OwnedSegmentStore {
    var manifest = try decodeManifestAlloc(alloc, manifest_data);
    errdefer manifest.deinit(alloc);

    const owned_data = try alloc.alloc([]u8, manifest.segments.len);
    var owned_count: usize = 0;
    errdefer {
        for (owned_data[0..owned_count]) |data| alloc.free(data);
        alloc.free(owned_data);
    }

    const segments = try alloc.alloc(SegmentBlob, manifest.segments.len);
    errdefer alloc.free(segments);

    for (manifest.segments, 0..) |entry, i| {
        const data = try readSegmentAlloc(context, alloc, entry.path, entry.meta.byte_len);
        errdefer alloc.free(data);
        try validateSegmentDataMatchesMeta(data, entry.meta);
        owned_data[i] = data;
        owned_count += 1;
        segments[i] = .{
            .meta = entry.meta,
            .data = data,
        };
    }

    return .{
        .manifest = manifest,
        .owned_data = owned_data,
        .segments = segments,
    };
}

pub fn openStoreFromDirectoryAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, options: OpenStoreOptions) !OwnedSegmentStore {
    const manifest_data = try dir.readFileAlloc(io, options.manifest_path, alloc, .limited(options.max_manifest_bytes));
    defer alloc.free(manifest_data);

    var manifest = try decodeManifestAlloc(alloc, manifest_data);
    errdefer manifest.deinit(alloc);

    const owned_data = try alloc.alloc([]u8, manifest.segments.len);
    var owned_count: usize = 0;
    errdefer {
        for (owned_data[0..owned_count]) |data| alloc.free(data);
        alloc.free(owned_data);
    }

    const segments = try alloc.alloc(SegmentBlob, manifest.segments.len);
    errdefer alloc.free(segments);

    for (manifest.segments, 0..) |entry, i| {
        if (entry.meta.byte_len > options.max_segment_bytes) return error.PostingSegmentTooLarge;
        const data = try readSegmentFileAlloc(alloc, io, dir, entry.path, entry.meta.byte_len);
        errdefer alloc.free(data);
        try validateSegmentDataMatchesMeta(data, entry.meta);
        owned_data[i] = data;
        owned_count += 1;
        segments[i] = .{
            .meta = entry.meta,
            .data = data,
        };
    }

    return .{
        .manifest = manifest,
        .owned_data = owned_data,
        .segments = segments,
    };
}

pub fn openLazyStoreFromDirectoryAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, options: OpenStoreOptions) !LazyDirectoryStore {
    return .{
        .manifest = try readManifestFromDirectoryAlloc(alloc, io, dir, options),
        .io = io,
        .dir = dir,
        .options = options,
    };
}

pub fn verifyDirectoryStoreAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, options: OpenStoreOptions) !DirectoryVerificationStats {
    const manifest_data = try dir.readFileAlloc(io, options.manifest_path, alloc, .limited(options.max_manifest_bytes));
    defer alloc.free(manifest_data);

    var manifest = try decodeManifestAlloc(alloc, manifest_data);
    defer manifest.deinit(alloc);

    var stats = DirectoryVerificationStats{
        .manifest_segments = manifest.segments.len,
        .manifest_bytes = manifest_data.len,
    };

    for (manifest.segments) |entry| {
        if (entry.meta.byte_len > options.max_segment_bytes) return error.PostingSegmentTooLarge;
        const data = try readSegmentFileAlloc(alloc, io, dir, entry.path, entry.meta.byte_len);
        defer alloc.free(data);
        try validateSegmentDataMatchesMeta(data, entry.meta);

        stats.segment_files += 1;
        stats.segment_bytes += data.len;
        stats.entries += entry.meta.entry_count;

        var reader = try Reader.init(data);
        var iter = reader.entries();
        while (try iter.next()) |logical_entry| {
            switch (logical_entry.kind) {
                .base => stats.base_records += 1,
                .centroid_directory => stats.centroid_records += 1,
                .delta => {
                    stats.delta_values += 1;
                    var delta_iter = try posting.PostingFormat.DeltaTailIterator.init(logical_entry.value);
                    while (try delta_iter.next()) |_| stats.delta_records += 1;
                },
            }
        }
    }

    return stats;
}

pub fn copyDirectoryStoreAlloc(
    alloc: Allocator,
    io: std.Io,
    source_dir: std.Io.Dir,
    destination_dir: std.Io.Dir,
    options: OpenStoreOptions,
) !DirectoryCopyStats {
    const manifest_data = try source_dir.readFileAlloc(io, options.manifest_path, alloc, .limited(options.max_manifest_bytes));
    defer alloc.free(manifest_data);
    return try copyDirectoryStoreManifestDataAlloc(alloc, io, source_dir, destination_dir, options, manifest_data);
}

pub fn copyDirectoryStoreManifestDataAlloc(
    alloc: Allocator,
    io: std.Io,
    source_dir: std.Io.Dir,
    destination_dir: std.Io.Dir,
    options: OpenStoreOptions,
    manifest_data: []const u8,
) !DirectoryCopyStats {
    var manifest = try decodeManifestAlloc(alloc, manifest_data);
    defer manifest.deinit(alloc);

    var stats = DirectoryCopyStats{
        .manifest_segments = manifest.segments.len,
        .manifest_bytes = manifest_data.len,
    };

    for (manifest.segments) |entry| {
        if (entry.meta.byte_len > options.max_segment_bytes) return error.PostingSegmentTooLarge;
        const data = try readSegmentFileAlloc(alloc, io, source_dir, entry.path, entry.meta.byte_len);
        defer alloc.free(data);
        try validateSegmentDataMatchesMeta(data, entry.meta);
        try writeFileAtomicallyAlloc(alloc, io, destination_dir, entry.path, data);

        stats.segment_files += 1;
        stats.segment_bytes += data.len;
        stats.entries += entry.meta.entry_count;
    }

    try writeManifestFileAlloc(alloc, io, destination_dir, options.manifest_path, manifest_data);
    return stats;
}

pub fn commitWriterToDirectoryAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, writer: *Writer, options: CommitOptions) !SegmentCommitResult {
    var manifest = try readManifestForCommitAlloc(alloc, io, dir, options);
    defer manifest.deinit(alloc);

    var built = try writer.buildSegment(manifest.next_segment_id);
    defer built.deinit(alloc);
    return try commitBuiltSegmentToDirectoryWithManifestAlloc(alloc, io, dir, built, manifest, options);
}

pub fn commitBuiltSegmentToDirectoryAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, segment: BuiltSegment, options: CommitOptions) !SegmentCommitResult {
    var manifest = try readManifestForCommitAlloc(alloc, io, dir, options);
    defer manifest.deinit(alloc);
    if (segment.meta.segment_id != manifest.next_segment_id) return error.InvalidPostingSegmentManifest;
    return try commitBuiltSegmentToDirectoryWithManifestAlloc(alloc, io, dir, segment, manifest, options);
}

pub fn compactDirectoryStoreAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, options: CommitOptions) !DirectoryCompactionResult {
    var store = try openStoreFromDirectoryAlloc(alloc, io, dir, .{
        .manifest_path = options.manifest_path,
        .max_manifest_bytes = options.max_manifest_bytes,
        .max_segment_bytes = options.max_segment_bytes,
    });
    defer store.deinit(alloc);
    if (store.segments.len == 0) return error.NoPostingSegmentsToCompact;

    var compacted = try compactSegmentsWithStatsAlloc(alloc, store.manifest.next_segment_id, store.segments);
    defer compacted.deinit(alloc);
    if (compacted.segment.meta.byte_len > options.max_segment_bytes) return error.PostingSegmentTooLarge;

    const existing_entries = try manifestEntryViewAlloc(alloc, store.manifest.segments);
    defer alloc.free(existing_entries);
    const remove_segment_ids = try alloc.alloc(u64, store.manifest.segments.len);
    defer alloc.free(remove_segment_ids);
    for (store.manifest.segments, 0..) |entry, i| remove_segment_ids[i] = entry.meta.segment_id;

    const written = try writeSegmentFileAlloc(alloc, io, dir, compacted.segment);
    errdefer alloc.free(written.path);
    const new_entry = ManifestEntry{
        .meta = written.meta,
        .path = written.path,
    };

    var replacement = try replaceManifestSegmentsWithStatsAlloc(alloc, .{
        .next_segment_id = store.manifest.next_segment_id,
        .segments = existing_entries,
    }, remove_segment_ids, &.{new_entry});
    defer replacement.deinit(alloc);

    try writeManifestFileAlloc(alloc, io, dir, options.manifest_path, replacement.encoded);
    return .{
        .entry = written,
        .stats = .{
            .compaction = compacted.stats,
            .manifest = replacement.stats,
            .segment_id = written.meta.segment_id,
            .segment_bytes = written.meta.byte_len,
            .manifest_bytes = replacement.encoded.len,
        },
    };
}

pub fn compactDirectoryStoreSegmentIdsAlloc(
    alloc: Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    segment_ids: []const u64,
    options: CommitOptions,
) !DirectoryCompactionResult {
    if (segment_ids.len == 0) return error.NoPostingSegmentsToCompact;
    try rejectDuplicateSegmentIds(segment_ids);

    var manifest = try readManifestFromDirectoryAlloc(alloc, io, dir, .{
        .manifest_path = options.manifest_path,
        .max_manifest_bytes = options.max_manifest_bytes,
        .max_segment_bytes = options.max_segment_bytes,
    });
    defer manifest.deinit(alloc);

    const existing_entries = try manifestEntryViewAlloc(alloc, manifest.segments);
    defer alloc.free(existing_entries);

    const selected = try alloc.alloc(SegmentBlob, segment_ids.len);
    defer alloc.free(selected);
    const selected_data = try alloc.alloc([]u8, segment_ids.len);
    var selected_data_count: usize = 0;
    defer {
        for (selected_data[0..selected_data_count]) |data| alloc.free(data);
        alloc.free(selected_data);
    }

    var selected_count: usize = 0;
    for (manifest.segments) |entry| {
        if (!segmentIdIn(entry.meta.segment_id, segment_ids)) continue;
        if (entry.meta.byte_len > options.max_segment_bytes) return error.PostingSegmentTooLarge;

        const data = try readSegmentFileAlloc(alloc, io, dir, entry.path, entry.meta.byte_len);
        errdefer alloc.free(data);
        try validateSegmentDataMatchesMeta(data, entry.meta);
        selected_data[selected_data_count] = data;
        selected_data_count += 1;
        selected[selected_count] = .{
            .meta = entry.meta,
            .data = data,
        };
        selected_count += 1;
    }
    if (selected_count != segment_ids.len) return error.PostingSegmentManifestReplacementMissingSegment;

    var compacted = try compactSegmentsWithStatsAlloc(alloc, manifest.next_segment_id, selected[0..selected_count]);
    defer compacted.deinit(alloc);
    if (compacted.segment.meta.byte_len > options.max_segment_bytes) return error.PostingSegmentTooLarge;

    const written = try writeSegmentFileAlloc(alloc, io, dir, compacted.segment);
    errdefer alloc.free(written.path);
    const new_entry = ManifestEntry{
        .meta = written.meta,
        .path = written.path,
    };

    var replacement = try replaceManifestSegmentsWithStatsAlloc(alloc, .{
        .next_segment_id = manifest.next_segment_id,
        .segments = existing_entries,
    }, segment_ids, &.{new_entry});
    defer replacement.deinit(alloc);

    try writeManifestFileAlloc(alloc, io, dir, options.manifest_path, replacement.encoded);
    return .{
        .entry = written,
        .stats = .{
            .compaction = compacted.stats,
            .manifest = replacement.stats,
            .segment_id = written.meta.segment_id,
            .segment_bytes = written.meta.byte_len,
            .manifest_bytes = replacement.encoded.len,
        },
    };
}

pub fn maintainDirectoryStoreAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, options: DirectoryMaintenanceOptions) !DirectoryMaintenanceStats {
    const open_options = OpenStoreOptions{
        .manifest_path = options.commit.manifest_path,
        .max_manifest_bytes = options.commit.max_manifest_bytes,
        .max_segment_bytes = options.commit.max_segment_bytes,
    };
    var stats = DirectoryMaintenanceStats{};

    if (options.collect_temporary_files) {
        stats.temporary = try collectDirectoryTemporaryGarbageAlloc(alloc, io, dir, open_options);
    }

    var manifest = readManifestFromDirectoryAlloc(alloc, io, dir, open_options) catch |err| switch (err) {
        error.FileNotFound => return stats,
        else => return err,
    };
    defer manifest.deinit(alloc);

    const entries = try manifestEntryViewAlloc(alloc, manifest.segments);
    defer alloc.free(entries);
    const manifest_view = Manifest{
        .next_segment_id = manifest.next_segment_id,
        .segments = entries,
    };
    stats.manifest = try summarizeManifest(manifest_view);

    if (options.compact) {
        var plan = try planDirectoryCompactionAlloc(alloc, manifest_view, options.plan);
        defer plan.deinit(alloc);
        stats.plan = plan.stats;
        if (plan.segment_ids.len != 0) {
            var compacted = try compactDirectoryStoreSegmentIdsAlloc(alloc, io, dir, plan.segment_ids, options.commit);
            defer compacted.deinit(alloc);
            stats.compacted = true;
            stats.compaction = compacted.stats;
        }
    }

    if (options.collect_orphan_segments) {
        stats.garbage = try collectDirectoryGarbageAlloc(alloc, io, dir, open_options);
    }

    return stats;
}

pub fn collectDirectoryGarbageAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, options: OpenStoreOptions) !DirectoryGarbageCollectionStats {
    var manifest = try readManifestFromDirectoryAlloc(alloc, io, dir, options);
    defer manifest.deinit(alloc);

    var live_paths = std.StringHashMapUnmanaged(void).empty;
    defer live_paths.deinit(alloc);
    const live_path_capacity = std.math.cast(u32, manifest.segments.len) orelse return error.PostingSegmentManifestTooLarge;
    try live_paths.ensureTotalCapacity(alloc, live_path_capacity);
    for (manifest.segments) |entry| live_paths.putAssumeCapacity(entry.path, {});

    var stats = DirectoryGarbageCollectionStats{
        .manifest_segments = manifest.segments.len,
    };

    var postings_dir = try dir.openDir(io, segment_directory, .{ .iterate = true });
    defer postings_dir.close(io);

    var iter = postings_dir.iterate();
    while (try iter.next(io)) |entry| {
        stats.scanned_entries += 1;
        if (entry.kind != .file or !isCanonicalSegmentFileName(entry.name)) {
            stats.skipped_entries += 1;
            continue;
        }

        stats.segment_files += 1;
        const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ segment_directory, entry.name });
        defer alloc.free(path);

        if (live_paths.contains(path)) {
            stats.referenced_segment_files += 1;
            continue;
        }

        stats.orphan_segment_files += 1;
        try dir.deleteFile(io, path);
        stats.deleted_segment_files += 1;
    }

    return stats;
}

pub fn collectDirectoryTemporaryGarbageAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, options: OpenStoreOptions) !DirectoryTemporaryCleanupStats {
    var stats = DirectoryTemporaryCleanupStats{};
    const manifest_tmp_name = try manifestTemporaryFileNameAlloc(alloc, options.manifest_path);
    defer alloc.free(manifest_tmp_name);

    var postings_dir = dir.openDir(io, segment_directory, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return stats,
        else => return err,
    };
    defer postings_dir.close(io);

    var iter = postings_dir.iterate();
    while (try iter.next(io)) |entry| {
        stats.scanned_entries += 1;
        if (entry.kind != .file) {
            stats.skipped_entries += 1;
            continue;
        }

        const is_segment_tmp = isCanonicalSegmentTemporaryFileName(entry.name);
        const is_manifest_tmp = std.mem.eql(u8, entry.name, manifest_tmp_name);
        if (!is_segment_tmp and !is_manifest_tmp) {
            stats.skipped_entries += 1;
            continue;
        }

        if (is_segment_tmp) stats.segment_temp_files += 1;
        if (is_manifest_tmp) stats.manifest_temp_files += 1;

        const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ segment_directory, entry.name });
        defer alloc.free(path);
        try dir.deleteFile(io, path);
        stats.deleted_temp_files += 1;
    }

    return stats;
}

pub fn writeSegmentFileAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, segment: BuiltSegment) !OwnedManifestEntry {
    const path = try segmentPathAlloc(alloc, segment.meta.segment_id);
    errdefer alloc.free(path);
    try writeFileAtomicallyAlloc(alloc, io, dir, path, segment.data);
    return .{
        .meta = segment.meta,
        .path = path,
    };
}

pub fn writeManifestFileAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, path: []const u8, data: []const u8) !void {
    try writeFileAtomicallyAlloc(alloc, io, dir, path, data);
}

pub fn readSegmentFileAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, path: []const u8, max_bytes: usize) ![]u8 {
    const read_limit = std.math.add(usize, max_bytes, 1) catch max_bytes;
    const data = try dir.readFileAlloc(io, path, alloc, .limited(read_limit));
    errdefer alloc.free(data);
    if (data.len > max_bytes) return error.PostingSegmentTooLarge;
    _ = try Reader.init(data);
    return data;
}

pub fn readSegmentPointValueAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, entry: ManifestEntry, posting_id: PostingId, kind: EntryKind) !?[]u8 {
    if (kind == .delta) return error.InvalidPostingSegmentEntryKind;
    const index_data = try readSegmentIndexAlloc(alloc, io, dir, entry);
    defer alloc.free(index_data);

    const match_index = lowerBoundIndexData(index_data, entry.meta.entry_count, posting_id, kind, 0);
    if (match_index >= entry.meta.entry_count) return null;
    const found = try indexEntryFromBytes(index_data[match_index * index_entry_size ..][0..index_entry_size]);
    if (found.posting_id != posting_id or found.kind != kind or found.sequence != 0) return null;

    return try readSegmentEntryValueAlloc(alloc, io, dir, entry, found);
}

pub fn readSegmentDeltaRecordsAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, entry: ManifestEntry, posting_id: PostingId, min_generation: ?u64) ![]posting.PostingDeltaRecord {
    const index_data = try readSegmentIndexAlloc(alloc, io, dir, entry);
    defer alloc.free(index_data);

    var records = std.ArrayListUnmanaged(posting.PostingDeltaRecord).empty;
    errdefer records.deinit(alloc);

    const range = (try readSegmentDeltaValueRangeAlloc(alloc, io, dir, entry, index_data, posting_id)) orelse return try records.toOwnedSlice(alloc);
    defer range.deinit(alloc);

    var index = range.first_index;
    while (index < range.past_index) : (index += 1) {
        const value = try deltaValueFromRange(index_data, range, index);
        var iterator = try posting.PostingFormat.DeltaTailIterator.init(value);
        if (min_generation) |generation| {
            while (try iterator.next()) |record| {
                if (posting.PostingFormat.deltaSequenceGeneration(record.sequence) <= generation) continue;
                try records.append(alloc, record);
            }
        } else {
            try records.ensureUnusedCapacity(alloc, iterator.recordCount());
            while (try iterator.next()) |record| records.appendAssumeCapacity(record);
        }
    }

    return try records.toOwnedSlice(alloc);
}

pub fn readSegmentDeltaRecordsWithStatsAlloc(
    alloc: Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    entry: ManifestEntry,
    posting_id: PostingId,
    min_generation: ?u64,
    records: *std.ArrayListUnmanaged(posting.PostingDeltaRecord),
    stats: *posting.PostingDeltaTailStats,
) !void {
    const index_data = try readSegmentIndexAlloc(alloc, io, dir, entry);
    defer alloc.free(index_data);

    const range = (try readSegmentDeltaValueRangeAlloc(alloc, io, dir, entry, index_data, posting_id)) orelse return;
    defer range.deinit(alloc);

    const base_generation = min_generation orelse 0;
    var index = range.first_index;
    while (index < range.past_index) : (index += 1) {
        const value = try deltaValueFromRange(index_data, range, index);
        var iterator = try posting.PostingFormat.DeltaTailIterator.init(value);
        stats.records += iterator.recordCount();
        stats.encoded_value_bytes += value.len;
        while (try iterator.next()) |record| {
            if (posting.PostingFormat.deltaSequenceGeneration(record.sequence) <= base_generation) continue;
            stats.records_after_generation += 1;
            if (record.op == .tombstone) stats.tombstones_after_generation += 1;
            stats.max_sequence_after_generation = @max(stats.max_sequence_after_generation, record.sequence);
            try records.append(alloc, record);
        }
    }
}

pub fn readSegmentDeltaRecordsIntoScratchWithStatsAlloc(
    alloc: Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    entry: ManifestEntry,
    posting_id: PostingId,
    base_generation: u64,
    scratch: anytype,
    stats: *posting.PostingDeltaTailStats,
) !void {
    const index_data = try readSegmentIndexAlloc(alloc, io, dir, entry);
    defer alloc.free(index_data);

    const range = (try readSegmentDeltaValueRangeAlloc(alloc, io, dir, entry, index_data, posting_id)) orelse return;
    defer range.deinit(alloc);

    var index = range.first_index;
    while (index < range.past_index) : (index += 1) {
        const value = try deltaValueFromRange(index_data, range, index);
        var iterator = try posting.PostingFormat.DeltaTailIterator.init(value);
        stats.records += iterator.recordCount();
        stats.encoded_value_bytes += value.len;
        while (try iterator.next()) |record| {
            if (posting.PostingFormat.deltaSequenceGeneration(record.sequence) <= base_generation) continue;
            stats.records_after_generation += 1;
            if (record.op == .tombstone) stats.tombstones_after_generation += 1;
            stats.max_sequence_after_generation = @max(stats.max_sequence_after_generation, record.sequence);
            try appendDeltaRecordToScratch(alloc, scratch, record);
        }
    }
}

pub fn readSegmentDeltaTailStatsAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, entry: ManifestEntry, posting_id: PostingId, base_generation: u64) !posting.PostingDeltaTailStats {
    const index_data = try readSegmentIndexAlloc(alloc, io, dir, entry);
    defer alloc.free(index_data);

    var out = posting.PostingDeltaTailStats{};
    const range = (try readSegmentDeltaValueRangeAlloc(alloc, io, dir, entry, index_data, posting_id)) orelse return out;
    defer range.deinit(alloc);

    var index = range.first_index;
    while (index < range.past_index) : (index += 1) {
        const value = try deltaValueFromRange(index_data, range, index);
        const stats = try posting.PostingFormat.deltaTailStatsAfterGeneration(value, base_generation);
        out.records += stats.records;
        out.records_after_generation += stats.records_after_generation;
        out.tombstones_after_generation += stats.tombstones_after_generation;
        out.encoded_value_bytes += stats.encoded_value_bytes;
        out.max_sequence_after_generation = @max(out.max_sequence_after_generation, stats.max_sequence_after_generation);
    }
    return out;
}

fn commitBuiltSegmentToDirectoryWithManifestAlloc(
    alloc: Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    segment: BuiltSegment,
    manifest: OwnedManifest,
    options: CommitOptions,
) !SegmentCommitResult {
    if (segment.meta.byte_len > options.max_segment_bytes) return error.PostingSegmentTooLarge;

    const existing_entries = try manifestEntryViewAlloc(alloc, manifest.segments);
    defer alloc.free(existing_entries);

    const written = try writeSegmentFileAlloc(alloc, io, dir, segment);
    errdefer alloc.free(written.path);
    const new_entry = ManifestEntry{
        .meta = written.meta,
        .path = written.path,
    };

    var replacement = try replaceManifestSegmentsWithStatsAlloc(alloc, .{
        .next_segment_id = manifest.next_segment_id,
        .segments = existing_entries,
    }, &.{}, &.{new_entry});
    defer replacement.deinit(alloc);

    try writeManifestFileAlloc(alloc, io, dir, options.manifest_path, replacement.encoded);
    return .{
        .entry = written,
        .stats = .{
            .input_segments = manifest.segments.len,
            .output_segments = replacement.stats.output_segments,
            .segment_id = written.meta.segment_id,
            .next_segment_id = replacement.stats.next_segment_id,
            .segment_bytes = written.meta.byte_len,
            .manifest_bytes = replacement.encoded.len,
        },
    };
}

fn readManifestForCommitAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, options: CommitOptions) !OwnedManifest {
    const manifest_data = dir.readFileAlloc(io, options.manifest_path, alloc, .limited(options.max_manifest_bytes)) catch |err| switch (err) {
        error.FileNotFound => return emptyManifestAlloc(alloc, options.initial_segment_id),
        else => return err,
    };
    defer alloc.free(manifest_data);
    return try decodeManifestAlloc(alloc, manifest_data);
}

fn readManifestFromDirectoryAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, options: OpenStoreOptions) !OwnedManifest {
    const manifest_data = try dir.readFileAlloc(io, options.manifest_path, alloc, .limited(options.max_manifest_bytes));
    defer alloc.free(manifest_data);
    return try decodeManifestAlloc(alloc, manifest_data);
}

fn emptyManifestAlloc(alloc: Allocator, next_segment_id: u64) !OwnedManifest {
    return .{
        .next_segment_id = next_segment_id,
        .segments = try alloc.alloc(OwnedManifestEntry, 0),
    };
}

fn manifestEntryViewAlloc(alloc: Allocator, entries: []const OwnedManifestEntry) ![]ManifestEntry {
    const view = try alloc.alloc(ManifestEntry, entries.len);
    for (entries, 0..) |entry, i| {
        view[i] = .{
            .meta = entry.meta,
            .path = entry.path,
        };
    }
    return view;
}

pub fn segmentPathAlloc(alloc: Allocator, segment_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "postings/{x:0>16}.afps", .{segment_id});
}

fn isCanonicalSegmentFileName(name: []const u8) bool {
    if (name.len != 21) return false;
    if (!std.mem.eql(u8, name[16..], ".afps")) return false;
    for (name[0..16]) |byte| {
        if (!isLowerHex(byte)) return false;
    }
    return true;
}

fn isCanonicalSegmentTemporaryFileName(name: []const u8) bool {
    return name.len == 25 and
        std.mem.eql(u8, name[21..], ".tmp") and
        isCanonicalSegmentFileName(name[0..21]);
}

fn manifestTemporaryFileNameAlloc(alloc: Allocator, manifest_path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}.tmp", .{std.fs.path.basename(manifest_path)});
}

fn isLowerHex(byte: u8) bool {
    return (byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f');
}

pub fn compactSegmentsAlloc(alloc: Allocator, segment_id: u64, segments: []const SegmentBlob) !BuiltSegment {
    const result = try compactSegmentsWithStatsAlloc(alloc, segment_id, segments);
    return result.segment;
}

pub fn compactSegmentsWithStatsAlloc(alloc: Allocator, segment_id: u64, segments: []const SegmentBlob) !CompactResult {
    var stats = CompactionStats{
        .input_segments = segments.len,
    };
    var bases = std.AutoHashMapUnmanaged(PostingId, PointCandidate).empty;
    defer bases.deinit(alloc);
    var centroids = std.AutoHashMapUnmanaged(PostingId, PointCandidate).empty;
    defer centroids.deinit(alloc);

    for (segments) |segment| {
        stats.input_bytes += segment.data.len;
        stats.input_entries += segment.meta.entry_count;
        var reader = try Reader.init(segment.data);
        var iter = reader.entries();
        while (try iter.next()) |entry| {
            switch (entry.kind) {
                .base => {
                    stats.input_base_records += 1;
                    try putNewestPoint(alloc, &bases, entry.posting_id, segment.meta.segment_id, entry.value);
                },
                .centroid_directory => {
                    stats.input_centroid_records += 1;
                    try putNewestPoint(alloc, &centroids, entry.posting_id, segment.meta.segment_id, entry.value);
                },
                .delta => stats.input_delta_values += 1,
            }
        }
    }

    var deltas = std.ArrayListUnmanaged(DeltaCandidate).empty;
    defer deltas.deinit(alloc);
    for (segments) |segment| {
        var reader = try Reader.init(segment.data);
        var iter = reader.entries();
        while (try iter.next()) |entry| {
            if (entry.kind != .delta) continue;
            const base_generation = if (bases.get(entry.posting_id)) |base|
                (try posting.PostingFormat.decodeBaseHeader(base.value)).generation
            else
                null;
            var delta_iterator = try posting.PostingFormat.DeltaTailIterator.init(entry.value);
            stats.input_delta_records += delta_iterator.recordCount();
            while (try delta_iterator.next()) |record| {
                if (base_generation) |generation| {
                    if (posting.PostingFormat.deltaSequenceGeneration(record.sequence) <= generation) {
                        stats.dropped_stale_delta_records += 1;
                        continue;
                    }
                }
                try deltas.append(alloc, .{
                    .posting_id = entry.posting_id,
                    .segment_id = segment.meta.segment_id,
                    .record = record,
                });
            }
        }
    }
    std.mem.sort(DeltaCandidate, deltas.items, {}, deltaCandidateLessThan);

    var writer = Writer.init(alloc);
    defer writer.deinit();
    {
        var iter = bases.iterator();
        while (iter.next()) |entry| {
            try writer.appendBase(entry.key_ptr.*, entry.value_ptr.value);
            stats.retained_base_records += 1;
        }
    }
    {
        var iter = centroids.iterator();
        while (iter.next()) |entry| {
            try writer.appendCentroidDirectory(entry.key_ptr.*, entry.value_ptr.value);
            stats.retained_centroid_records += 1;
        }
    }
    stats.dropped_superseded_base_records = stats.input_base_records - stats.retained_base_records;
    stats.dropped_superseded_centroid_records = stats.input_centroid_records - stats.retained_centroid_records;

    var posting_start: usize = 0;
    while (posting_start < deltas.items.len) {
        const posting_id = deltas.items[posting_start].posting_id;
        var posting_end = posting_start + 1;
        while (posting_end < deltas.items.len and deltas.items[posting_end].posting_id == posting_id) : (posting_end += 1) {}

        var records = std.ArrayListUnmanaged(posting.PostingDeltaRecord).empty;
        defer records.deinit(alloc);
        var i = posting_start;
        while (i < posting_end) {
            var chosen = deltas.items[i];
            var j = i + 1;
            while (j < posting_end and deltas.items[j].record.sequence == chosen.record.sequence) : (j += 1) {
                chosen = deltas.items[j];
            }
            stats.dropped_duplicate_delta_records += j - i - 1;
            try records.append(alloc, chosen.record);
            stats.retained_delta_records += 1;
            i = j;
        }

        if (records.items.len != 0) {
            const encoded = try posting.PostingFormat.encodeDeltaTail(alloc, records.items);
            try writer.appendOwnedDelta(posting_id, records.items[0].sequence, encoded);
        }
        posting_start = posting_end;
    }

    const segment = try writer.buildSegment(segment_id);
    stats.output_bytes = segment.data.len;
    stats.output_entries = segment.meta.entry_count;
    return .{
        .segment = segment,
        .stats = stats,
    };
}

const PendingEntry = struct {
    posting_id: PostingId,
    kind: EntryKind,
    sequence: u64,
    value: []u8,
};

const PendingDeltaBatch = struct {
    records: std.ArrayListUnmanaged(posting.PostingDeltaRecord) = .empty,
    encoded_value_bytes: usize = 0,
    min_sequence: u64 = 0,

    fn deinit(self: *PendingDeltaBatch, alloc: Allocator) void {
        self.records.deinit(alloc);
        self.* = .{};
    }

    fn noteAppendedRecord(self: *PendingDeltaBatch, record: posting.PostingDeltaRecord, encoded_value_bytes: usize) void {
        self.encoded_value_bytes = encoded_value_bytes;
        self.min_sequence = if (self.records.items.len == 1)
            record.sequence
        else
            @min(self.min_sequence, record.sequence);
    }

    fn noteAppendedRecords(self: *PendingDeltaBatch, records: []const posting.PostingDeltaRecord, encoded_value_bytes: usize) void {
        self.encoded_value_bytes = encoded_value_bytes;
        var min_sequence = if (self.records.items.len == records.len) records[0].sequence else self.min_sequence;
        for (records) |record| min_sequence = @min(min_sequence, record.sequence);
        self.min_sequence = min_sequence;
    }
};

fn deltaRecordEncodedSizeFromBaseSequence(record: posting.PostingDeltaRecord, base_sequence: u64) !usize {
    var total = posting.PostingFormat.varintSize(record.sequence - base_sequence);
    total = try std.math.add(usize, total, 1);
    total = try std.math.add(usize, total, posting.PostingFormat.varintSize(record.vector_id));
    return total;
}

fn pendingDeltaBatchEncodedSizeWithRecord(batch: PendingDeltaBatch, record: posting.PostingDeltaRecord) !usize {
    if (batch.records.items.len == 0) {
        return try posting.PostingFormat.encodedDeltaTailSize(&.{record});
    }

    if (batch.records.items.len >= 2 and record.sequence >= batch.min_sequence) {
        return try std.math.add(
            usize,
            batch.encoded_value_bytes,
            try deltaRecordEncodedSizeFromBaseSequence(record, batch.min_sequence),
        );
    }

    const base_sequence = @min(record.sequence, batch.min_sequence);
    var total = posting.PostingFormat.delta_header_size;
    for (batch.records.items) |existing| {
        total = try std.math.add(usize, total, try deltaRecordEncodedSizeFromBaseSequence(existing, base_sequence));
    }
    total = try std.math.add(usize, total, try deltaRecordEncodedSizeFromBaseSequence(record, base_sequence));
    return total;
}

fn pendingDeltaBatchEncodedSizeWithRecords(batch: PendingDeltaBatch, records: []const posting.PostingDeltaRecord) !usize {
    if (records.len == 0) return batch.encoded_value_bytes;
    if (records.len == 1) return try pendingDeltaBatchEncodedSizeWithRecord(batch, records[0]);
    if (batch.records.items.len == 0) return try posting.PostingFormat.encodedDeltaTailSize(records);

    var all_new_sequences_after_min = batch.records.items.len >= 2;
    var base_sequence = batch.min_sequence;
    for (records) |record| {
        if (record.sequence < batch.min_sequence) all_new_sequences_after_min = false;
        base_sequence = @min(base_sequence, record.sequence);
    }
    if (all_new_sequences_after_min) {
        var total = batch.encoded_value_bytes;
        for (records) |record| {
            total = try std.math.add(usize, total, try deltaRecordEncodedSizeFromBaseSequence(record, batch.min_sequence));
        }
        return total;
    }

    var total = posting.PostingFormat.delta_header_size;
    for (batch.records.items) |existing| {
        total = try std.math.add(usize, total, try deltaRecordEncodedSizeFromBaseSequence(existing, base_sequence));
    }
    for (records) |record| {
        total = try std.math.add(usize, total, try deltaRecordEncodedSizeFromBaseSequence(record, base_sequence));
    }
    return total;
}

const PointCandidate = struct {
    segment_id: u64,
    value: []const u8,
};

const DeltaCandidate = struct {
    posting_id: PostingId,
    segment_id: u64,
    record: posting.PostingDeltaRecord,
};

const IndexEntry = struct {
    posting_id: PostingId,
    kind: EntryKind,
    sequence: u64,
    offset: usize,
    len: usize,
    value_checksum: u32,

    fn location(self: IndexEntry) ValueLocation {
        return .{
            .offset = self.offset,
            .len = self.len,
            .value_checksum = self.value_checksum,
        };
    }

    fn value(self: IndexEntry, data: []const u8) ![]const u8 {
        return try self.location().value(data);
    }
};

const OwnedDeltaValueRange = struct {
    data: []u8,
    base_offset: usize,
    first_index: usize,
    past_index: usize,

    fn deinit(self: OwnedDeltaValueRange, alloc: Allocator) void {
        alloc.free(self.data);
    }
};

pub const ValueLocation = struct {
    offset: usize,
    len: usize,
    value_checksum: u32,

    pub fn value(self: ValueLocation, data: []const u8) ![]const u8 {
        const end = std.math.add(usize, self.offset, self.len) catch return error.CorruptedPostingSegment;
        if (end > data.len) return error.CorruptedPostingSegment;
        const bytes = data[self.offset..end];
        try self.verifyValue(bytes);
        return bytes;
    }

    pub fn verifyValue(self: ValueLocation, bytes: []const u8) !void {
        if (bytes.len != self.len) return error.CorruptedPostingSegment;
        if (valueChecksum(bytes) != self.value_checksum) return error.BadPostingSegmentChecksum;
    }
};

pub const Writer = struct {
    alloc: Allocator,
    entries: std.ArrayListUnmanaged(PendingEntry) = .empty,

    pub fn init(alloc: Allocator) Writer {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Writer) void {
        for (self.entries.items) |entry| self.alloc.free(entry.value);
        self.entries.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn appendBase(self: *Writer, posting_id: PostingId, value: []const u8) !void {
        try self.appendEntry(.{
            .posting_id = posting_id,
            .kind = .base,
            .sequence = 0,
        }, value);
    }

    pub fn appendPostingBase(self: *Writer, base: posting.PostingBase) !void {
        const encoded = try posting.PostingFormat.encodeBase(self.alloc, base);
        try self.appendOwnedEntry(.{
            .posting_id = base.posting_id,
            .kind = .base,
            .sequence = 0,
        }, encoded);
    }

    pub fn appendCentroidDirectory(self: *Writer, posting_id: PostingId, value: []const u8) !void {
        try self.appendEntry(.{
            .posting_id = posting_id,
            .kind = .centroid_directory,
            .sequence = 0,
        }, value);
    }

    pub fn appendCentroidDirectoryRecord(self: *Writer, record: posting.CentroidDirectoryRecord) !void {
        const encoded = try posting.CentroidDirectoryFormat.encode(self.alloc, record);
        try self.appendOwnedEntry(.{
            .posting_id = record.posting_id,
            .kind = .centroid_directory,
            .sequence = 0,
        }, encoded);
    }

    pub fn appendDelta(self: *Writer, posting_id: PostingId, sequence: u64, value: []const u8) !void {
        try self.appendEntry(.{
            .posting_id = posting_id,
            .kind = .delta,
            .sequence = sequence,
        }, value);
    }

    pub fn appendPostingDeltaRecords(self: *Writer, posting_id: PostingId, records: []const posting.PostingDeltaRecord) !void {
        if (records.len == 0) return;
        var min_sequence = records[0].sequence;
        for (records[1..]) |record| min_sequence = @min(min_sequence, record.sequence);
        const encoded = try posting.PostingFormat.encodeDeltaTail(self.alloc, records);
        try self.appendOwnedDelta(posting_id, min_sequence, encoded);
    }

    fn appendOwnedDelta(self: *Writer, posting_id: PostingId, sequence: u64, value: []u8) !void {
        try self.appendOwnedEntry(.{
            .posting_id = posting_id,
            .kind = .delta,
            .sequence = sequence,
        }, value);
    }

    fn appendEntry(self: *Writer, key: struct {
        posting_id: PostingId,
        kind: EntryKind,
        sequence: u64,
    }, value: []const u8) !void {
        const owned = try self.alloc.dupe(u8, value);
        errdefer self.alloc.free(owned);
        try self.entries.append(self.alloc, .{
            .posting_id = key.posting_id,
            .kind = key.kind,
            .sequence = key.sequence,
            .value = owned,
        });
    }

    fn appendOwnedEntry(self: *Writer, key: struct {
        posting_id: PostingId,
        kind: EntryKind,
        sequence: u64,
    }, value: []u8) !void {
        errdefer self.alloc.free(value);
        try self.entries.append(self.alloc, .{
            .posting_id = key.posting_id,
            .kind = key.kind,
            .sequence = key.sequence,
            .value = value,
        });
    }

    pub fn build(self: *Writer) ![]u8 {
        std.mem.sort(PendingEntry, self.entries.items, {}, pendingEntryLessThan);
        try rejectDuplicateEntries(self.entries.items);

        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(self.alloc);
        var index_entries = try std.ArrayListUnmanaged(IndexEntry).initCapacity(self.alloc, self.entries.items.len);
        defer index_entries.deinit(self.alloc);

        for (self.entries.items) |entry| {
            const offset = out.items.len;
            try out.appendSlice(self.alloc, entry.value);
            index_entries.appendAssumeCapacity(.{
                .posting_id = entry.posting_id,
                .kind = entry.kind,
                .sequence = entry.sequence,
                .offset = offset,
                .len = entry.value.len,
                .value_checksum = valueChecksum(entry.value),
            });
        }

        const index_offset = out.items.len;
        for (index_entries.items) |entry| try appendIndexEntry(self.alloc, &out, entry);
        const index_end = out.items.len;
        const stored_index_checksum = indexChecksum(out.items[index_offset..index_end]);
        try appendU64(self.alloc, &out, @intCast(index_offset));
        try appendU64(self.alloc, &out, @intCast(index_entries.items.len));
        try appendU32(self.alloc, &out, stored_index_checksum);
        try appendU32(self.alloc, &out, segmentChecksum(out.items));
        try appendU16(self.alloc, &out, version);
        try out.appendSlice(self.alloc, &magic);
        return try out.toOwnedSlice(self.alloc);
    }

    pub fn buildSegment(self: *Writer, segment_id: u64) !BuiltSegment {
        const data = try self.build();
        errdefer self.alloc.free(data);
        const reader = try Reader.init(data);
        const meta = try reader.metadata(segment_id);
        return .{
            .meta = meta,
            .data = data,
        };
    }
};

pub const DirectoryBatchWriter = struct {
    alloc: Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    options: DirectoryBatchWriterOptions,
    writer: Writer,
    pending_delta_batches: std.AutoHashMapUnmanaged(PostingId, PendingDeltaBatch) = .empty,
    pending_delta_records: usize = 0,
    pending_value_bytes: usize = 0,
    stats: DirectoryBatchWriterStats = .{},

    pub fn init(alloc: Allocator, io: std.Io, dir: std.Io.Dir, options: DirectoryBatchWriterOptions) DirectoryBatchWriter {
        return .{
            .alloc = alloc,
            .io = io,
            .dir = dir,
            .options = options,
            .writer = Writer.init(alloc),
        };
    }

    pub fn deinit(self: *DirectoryBatchWriter) void {
        self.clearPendingDeltaBatches();
        self.pending_delta_batches.deinit(self.alloc);
        self.writer.deinit();
        self.* = undefined;
    }

    pub fn pendingEntries(self: DirectoryBatchWriter) usize {
        return self.writer.entries.items.len + self.pending_delta_batches.count();
    }

    pub fn pendingDeltaRecords(self: DirectoryBatchWriter) usize {
        return self.pending_delta_records;
    }

    pub fn pendingBaseHeader(self: *const DirectoryBatchWriter, posting_id: PostingId) !?posting.PostingBaseHeader {
        const value = self.pendingPointValue(posting_id, .base) orelse return null;
        return try posting.PostingFormat.decodeBaseHeader(value);
    }

    pub fn pendingBaseStats(self: *const DirectoryBatchWriter, posting_id: PostingId) !?posting.PostingBaseStats {
        const value = self.pendingPointValue(posting_id, .base) orelse return null;
        return try posting.PostingFormat.decodeBaseStats(value);
    }

    pub fn pendingBase(self: *const DirectoryBatchWriter, alloc: Allocator, posting_id: PostingId) !?posting.OwnedPostingBase {
        const value = self.pendingPointValue(posting_id, .base) orelse return null;
        return try posting.PostingFormat.decodeBase(alloc, value);
    }

    pub fn pendingBaseDataAlloc(self: *const DirectoryBatchWriter, alloc: Allocator, posting_id: PostingId) !?[]u8 {
        const value = self.pendingPointValue(posting_id, .base) orelse return null;
        return try alloc.dupe(u8, value);
    }

    pub fn pendingCentroidDirectoryRecord(self: *const DirectoryBatchWriter, alloc: Allocator, posting_id: PostingId) !?posting.OwnedCentroidDirectoryRecord {
        const value = self.pendingPointValue(posting_id, .centroid_directory) orelse return null;
        return try posting.CentroidDirectoryFormat.decode(alloc, value);
    }

    pub fn pendingDeltaTailAlloc(self: *const DirectoryBatchWriter, alloc: Allocator, posting_id: PostingId, min_generation: ?u64) ![]posting.PostingDeltaRecord {
        var records = std.ArrayListUnmanaged(posting.PostingDeltaRecord).empty;
        errdefer records.deinit(alloc);

        try self.appendPendingDeltaEntries(alloc, &records, posting_id, min_generation);
        if (self.pending_delta_batches.get(posting_id)) |batch| {
            if (min_generation) |generation| {
                for (batch.records.items) |record| {
                    if (posting.PostingFormat.deltaSequenceGeneration(record.sequence) <= generation) continue;
                    try records.append(alloc, record);
                }
            } else {
                try records.ensureUnusedCapacity(alloc, batch.records.items.len);
                for (batch.records.items) |record| records.appendAssumeCapacity(record);
            }
        }

        std.mem.sort(posting.PostingDeltaRecord, records.items, {}, postingDeltaRecordLessThan);
        return try records.toOwnedSlice(alloc);
    }

    pub fn pendingDeltaTailWithStatsAlloc(self: *const DirectoryBatchWriter, alloc: Allocator, posting_id: PostingId, base_generation: u64) !DeltaTailWithStats {
        var records = std.ArrayListUnmanaged(posting.PostingDeltaRecord).empty;
        errdefer records.deinit(alloc);
        var stats = posting.PostingDeltaTailStats{};

        try self.appendPendingDeltaEntriesWithStats(alloc, &records, &stats, posting_id, base_generation);
        if (self.pending_delta_batches.get(posting_id)) |batch| {
            stats.encoded_value_bytes += batch.encoded_value_bytes;
            for (batch.records.items) |record| {
                stats.records += 1;
                if (posting.PostingFormat.deltaSequenceGeneration(record.sequence) <= base_generation) continue;
                stats.records_after_generation += 1;
                if (record.op == .tombstone) stats.tombstones_after_generation += 1;
                stats.max_sequence_after_generation = @max(stats.max_sequence_after_generation, record.sequence);
                try records.append(alloc, record);
            }
        }

        std.mem.sort(posting.PostingDeltaRecord, records.items, {}, postingDeltaRecordLessThan);
        return .{
            .records = try records.toOwnedSlice(alloc),
            .stats = stats,
        };
    }

    pub fn appendPendingDeltaTailIntoScratchWithStats(
        self: *const DirectoryBatchWriter,
        alloc: Allocator,
        posting_id: PostingId,
        base_generation: u64,
        scratch: anytype,
    ) !posting.PostingDeltaTailStats {
        var stats = posting.PostingDeltaTailStats{};

        try self.appendPendingDeltaEntriesIntoScratchWithStats(alloc, scratch, &stats, posting_id, base_generation);
        if (self.pending_delta_batches.get(posting_id)) |batch| {
            stats.encoded_value_bytes += batch.encoded_value_bytes;
            for (batch.records.items) |record| {
                stats.records += 1;
                if (posting.PostingFormat.deltaSequenceGeneration(record.sequence) <= base_generation) continue;
                stats.records_after_generation += 1;
                if (record.op == .tombstone) stats.tombstones_after_generation += 1;
                stats.max_sequence_after_generation = @max(stats.max_sequence_after_generation, record.sequence);
                try appendDeltaRecordToScratch(alloc, scratch, record);
            }
        }

        return stats;
    }

    pub fn pendingDeltaTailStats(self: *const DirectoryBatchWriter, posting_id: PostingId, base_generation: u64) !posting.PostingDeltaTailStats {
        var out = posting.PostingDeltaTailStats{};
        try self.accumulatePendingDeltaEntryStats(&out, posting_id, base_generation);
        if (self.pending_delta_batches.get(posting_id)) |batch| {
            out.encoded_value_bytes += batch.encoded_value_bytes;
            for (batch.records.items) |record| {
                out.records += 1;
                if (posting.PostingFormat.deltaSequenceGeneration(record.sequence) <= base_generation) continue;
                out.records_after_generation += 1;
                if (record.op == .tombstone) out.tombstones_after_generation += 1;
                out.max_sequence_after_generation = @max(out.max_sequence_after_generation, record.sequence);
            }
        }
        return out;
    }

    pub fn pendingLatestDeltaOpAfterGenerationForMember(
        self: *const DirectoryBatchWriter,
        posting_id: PostingId,
        vector_id: posting.VectorId,
        base_generation: u64,
    ) !?posting.PostingDeltaOp {
        const record = try self.pendingLatestDeltaRecordAfterGenerationForMember(posting_id, vector_id, base_generation);
        return if (record) |found| found.op else null;
    }

    pub fn pendingLatestDeltaRecordAfterGenerationForMember(
        self: *const DirectoryBatchWriter,
        posting_id: PostingId,
        vector_id: posting.VectorId,
        base_generation: u64,
    ) !?posting.PostingDeltaRecord {
        var best_sequence: u64 = 0;
        var best_record: ?posting.PostingDeltaRecord = null;
        try self.latestPendingDeltaEntryAfterGenerationForMember(posting_id, vector_id, base_generation, &best_sequence, &best_record);
        if (self.pending_delta_batches.get(posting_id)) |batch| {
            for (batch.records.items) |record| {
                if (record.vector_id != vector_id) continue;
                if (posting.PostingFormat.deltaSequenceGeneration(record.sequence) <= base_generation) continue;
                if (best_record == null or record.sequence >= best_sequence) {
                    best_sequence = record.sequence;
                    best_record = record;
                }
            }
        }
        return best_record;
    }

    pub fn appendBase(self: *DirectoryBatchWriter, posting_id: PostingId, value: []const u8) !void {
        try self.appendLatestPoint(posting_id, .base, value);
    }

    pub fn appendPostingBase(self: *DirectoryBatchWriter, base: posting.PostingBase) !void {
        const encoded = try posting.PostingFormat.encodeBase(self.alloc, base);
        try self.appendLatestPointOwned(base.posting_id, .base, encoded);
    }

    pub fn appendCentroidDirectory(self: *DirectoryBatchWriter, posting_id: PostingId, value: []const u8) !void {
        try self.appendLatestPoint(posting_id, .centroid_directory, value);
    }

    pub fn appendCentroidDirectoryRecord(self: *DirectoryBatchWriter, record: posting.CentroidDirectoryRecord) !void {
        const encoded = try posting.CentroidDirectoryFormat.encode(self.alloc, record);
        try self.appendLatestPointOwned(record.posting_id, .centroid_directory, encoded);
    }

    pub fn appendDelta(self: *DirectoryBatchWriter, posting_id: PostingId, sequence: u64, value: []const u8) !void {
        if (self.pending_delta_records != 0) _ = try self.flush();
        try self.prepareForAppend(value.len);
        try self.writer.appendDelta(posting_id, sequence, value);
        try self.notePendingAppend(value.len);
    }

    pub fn appendPostingDeltaRecords(self: *DirectoryBatchWriter, posting_id: PostingId, records: []const posting.PostingDeltaRecord) !void {
        if (records.len == 0) return;
        if (records.len == 1) return try self.appendDeltaRecord(posting_id, records[0]);
        if (self.options.max_pending_delta_records != 0 and records.len > self.options.max_pending_delta_records) {
            for (records) |record| try self.appendDeltaRecord(posting_id, record);
            return;
        }

        if (self.options.max_pending_delta_records != 0 and self.pending_delta_records + records.len > self.options.max_pending_delta_records) {
            _ = try self.flush();
        }

        if (!self.pending_delta_batches.contains(posting_id) and self.options.max_pending_entries != 0 and self.pendingEntries() + 1 > self.options.max_pending_entries) {
            _ = try self.flush();
        }

        var existing_batch = self.pending_delta_batches.get(posting_id) orelse PendingDeltaBatch{};
        var next_encoded_value_bytes = try pendingDeltaBatchEncodedSizeWithRecords(existing_batch, records);
        if (self.options.max_pending_value_bytes != 0 and self.pendingEntries() != 0) {
            const existing_encoded_value_bytes = existing_batch.encoded_value_bytes;
            if (self.pending_value_bytes - existing_encoded_value_bytes + next_encoded_value_bytes > self.options.max_pending_value_bytes) {
                _ = try self.flush();
                existing_batch = PendingDeltaBatch{};
                next_encoded_value_bytes = try pendingDeltaBatchEncodedSizeWithRecords(existing_batch, records);
            }
        }

        const gop = try self.pending_delta_batches.getOrPut(self.alloc, posting_id);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{};
        }
        errdefer if (!gop.found_existing) {
            gop.value_ptr.deinit(self.alloc);
            _ = self.pending_delta_batches.remove(posting_id);
        };

        const previous_encoded_value_bytes = gop.value_ptr.encoded_value_bytes;
        const pending_value_bytes_without_batch = self.pending_value_bytes - previous_encoded_value_bytes;
        const next_pending_value_bytes = std.math.add(usize, pending_value_bytes_without_batch, next_encoded_value_bytes) catch return error.PostingSegmentTooLarge;
        try gop.value_ptr.records.appendSlice(self.alloc, records);
        gop.value_ptr.noteAppendedRecords(records, next_encoded_value_bytes);
        self.pending_value_bytes = next_pending_value_bytes;
        self.pending_delta_records += records.len;
        if ((self.options.max_pending_delta_records != 0 and self.pending_delta_records >= self.options.max_pending_delta_records) or
            (self.options.max_pending_value_bytes != 0 and self.pending_value_bytes >= self.options.max_pending_value_bytes))
        {
            _ = try self.flush();
        }
    }

    pub fn appendDeltaRecord(self: *DirectoryBatchWriter, posting_id: PostingId, record: posting.PostingDeltaRecord) !void {
        if (self.options.max_pending_delta_records != 0 and self.pending_delta_records + 1 > self.options.max_pending_delta_records) {
            _ = try self.flush();
        }

        if (self.pending_delta_batches.get(posting_id)) |batch| {
            const next_encoded_value_bytes = try pendingDeltaBatchEncodedSizeWithRecord(batch, record);
            if (self.options.max_pending_value_bytes != 0 and
                self.pendingEntries() != 0 and
                self.pending_value_bytes - batch.encoded_value_bytes + next_encoded_value_bytes > self.options.max_pending_value_bytes)
            {
                _ = try self.flush();
            }
        } else {
            const next_encoded_value_bytes = try pendingDeltaBatchEncodedSizeWithRecord(.{}, record);
            if (!self.pending_delta_batches.contains(posting_id) and self.options.max_pending_entries != 0 and self.pendingEntries() + 1 > self.options.max_pending_entries) {
                _ = try self.flush();
            }
            if (self.options.max_pending_value_bytes != 0 and
                self.pendingEntries() != 0 and
                self.pending_value_bytes + next_encoded_value_bytes > self.options.max_pending_value_bytes)
            {
                _ = try self.flush();
            }
        }

        const gop = try self.pending_delta_batches.getOrPut(self.alloc, posting_id);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{};
        }
        errdefer if (!gop.found_existing) {
            gop.value_ptr.deinit(self.alloc);
            _ = self.pending_delta_batches.remove(posting_id);
        };
        const previous_encoded_value_bytes = gop.value_ptr.encoded_value_bytes;
        const next_encoded_value_bytes = try pendingDeltaBatchEncodedSizeWithRecord(gop.value_ptr.*, record);
        const pending_value_bytes_without_batch = self.pending_value_bytes - previous_encoded_value_bytes;
        const next_pending_value_bytes = std.math.add(usize, pending_value_bytes_without_batch, next_encoded_value_bytes) catch return error.PostingSegmentTooLarge;
        try gop.value_ptr.records.append(self.alloc, record);
        gop.value_ptr.noteAppendedRecord(record, next_encoded_value_bytes);
        self.pending_value_bytes = next_pending_value_bytes;
        self.pending_delta_records += 1;
        if ((self.options.max_pending_delta_records != 0 and self.pending_delta_records >= self.options.max_pending_delta_records) or
            (self.options.max_pending_value_bytes != 0 and self.pending_value_bytes >= self.options.max_pending_value_bytes))
        {
            _ = try self.flush();
        }
    }

    pub fn flush(self: *DirectoryBatchWriter) !?SegmentCommitStats {
        var manifest = try readManifestForCommitAlloc(self.alloc, self.io, self.dir, self.options.commit);
        defer manifest.deinit(self.alloc);
        return try self.flushWithManifest(manifest);
    }

    pub fn flushWithManifest(self: *DirectoryBatchWriter, manifest: OwnedManifest) !?SegmentCommitStats {
        const pending_delta_records = self.pending_delta_records;
        try self.drainPendingDeltaBatches();
        if (self.writer.entries.items.len == 0) return null;
        const pending_entries = self.writer.entries.items.len;
        const pending_value_bytes = self.pending_value_bytes;
        var built = try self.writer.buildSegment(manifest.next_segment_id);
        defer built.deinit(self.alloc);
        var result = try commitBuiltSegmentToDirectoryWithManifestAlloc(self.alloc, self.io, self.dir, built, manifest, self.options.commit);
        defer result.deinit(self.alloc);

        self.stats.flushed_segments += 1;
        self.stats.committed_entries += pending_entries;
        self.stats.committed_delta_records += pending_delta_records;
        self.stats.committed_value_bytes += pending_value_bytes;
        self.stats.committed_segment_bytes += result.stats.segment_bytes;
        self.stats.committed_manifest_bytes += result.stats.manifest_bytes;
        self.stats.last_segment_id = result.stats.segment_id;
        self.stats.next_segment_id = result.stats.next_segment_id;

        const commit_stats = result.stats;
        self.resetPendingWriter();
        return commit_stats;
    }

    fn prepareForAppend(self: *DirectoryBatchWriter, value_len: usize) !void {
        if (self.pendingEntries() == 0) return;
        if (self.options.max_pending_entries != 0 and self.pendingEntries() + 1 > self.options.max_pending_entries) {
            _ = try self.flush();
            return;
        }
        if (self.options.max_pending_value_bytes != 0 and self.pending_value_bytes + value_len > self.options.max_pending_value_bytes) {
            _ = try self.flush();
        }
    }

    fn appendLatestPoint(self: *DirectoryBatchWriter, posting_id: PostingId, kind: EntryKind, value: []const u8) !void {
        std.debug.assert(kind != .delta);
        if (try self.replacePendingPoint(posting_id, kind, value)) return;
        try self.prepareForAppend(value.len);
        switch (kind) {
            .base => try self.writer.appendBase(posting_id, value),
            .centroid_directory => try self.writer.appendCentroidDirectory(posting_id, value),
            .delta => unreachable,
        }
        try self.notePendingAppend(value.len);
    }

    fn appendLatestPointOwned(self: *DirectoryBatchWriter, posting_id: PostingId, kind: EntryKind, value: []u8) !void {
        std.debug.assert(kind != .delta);
        var owned = value;
        errdefer if (owned.len != 0) self.alloc.free(owned);

        if (try self.replacePendingPointOwned(posting_id, kind, owned)) {
            owned = &.{};
            return;
        }

        try self.prepareForAppend(owned.len);
        const transferred = owned;
        owned = &.{};
        switch (kind) {
            .base => try self.writer.appendOwnedEntry(.{
                .posting_id = posting_id,
                .kind = .base,
                .sequence = 0,
            }, transferred),
            .centroid_directory => try self.writer.appendOwnedEntry(.{
                .posting_id = posting_id,
                .kind = .centroid_directory,
                .sequence = 0,
            }, transferred),
            .delta => unreachable,
        }
        try self.notePendingAppend(transferred.len);
    }

    fn pendingPointValue(self: *const DirectoryBatchWriter, posting_id: PostingId, kind: EntryKind) ?[]const u8 {
        std.debug.assert(kind != .delta);
        for (self.writer.entries.items) |entry| {
            if (entry.posting_id == posting_id and entry.kind == kind and entry.sequence == 0) return entry.value;
        }
        return null;
    }

    fn appendPendingDeltaEntries(
        self: *const DirectoryBatchWriter,
        alloc: Allocator,
        records: *std.ArrayListUnmanaged(posting.PostingDeltaRecord),
        posting_id: PostingId,
        min_generation: ?u64,
    ) !void {
        for (self.writer.entries.items) |entry| {
            if (entry.posting_id != posting_id or entry.kind != .delta) continue;
            var iterator = try posting.PostingFormat.DeltaTailIterator.init(entry.value);
            if (min_generation) |generation| {
                while (try iterator.next()) |record| {
                    if (posting.PostingFormat.deltaSequenceGeneration(record.sequence) <= generation) continue;
                    try records.append(alloc, record);
                }
            } else {
                try records.ensureUnusedCapacity(alloc, iterator.recordCount());
                while (try iterator.next()) |record| records.appendAssumeCapacity(record);
            }
        }
    }

    fn appendPendingDeltaEntriesWithStats(
        self: *const DirectoryBatchWriter,
        alloc: Allocator,
        records: *std.ArrayListUnmanaged(posting.PostingDeltaRecord),
        stats: *posting.PostingDeltaTailStats,
        posting_id: PostingId,
        base_generation: u64,
    ) !void {
        for (self.writer.entries.items) |entry| {
            if (entry.posting_id != posting_id or entry.kind != .delta) continue;
            var iterator = try posting.PostingFormat.DeltaTailIterator.init(entry.value);
            stats.records += iterator.recordCount();
            stats.encoded_value_bytes += entry.value.len;
            while (try iterator.next()) |record| {
                if (posting.PostingFormat.deltaSequenceGeneration(record.sequence) <= base_generation) continue;
                stats.records_after_generation += 1;
                if (record.op == .tombstone) stats.tombstones_after_generation += 1;
                stats.max_sequence_after_generation = @max(stats.max_sequence_after_generation, record.sequence);
                try records.append(alloc, record);
            }
        }
    }

    fn appendPendingDeltaEntriesIntoScratchWithStats(
        self: *const DirectoryBatchWriter,
        alloc: Allocator,
        scratch: anytype,
        stats: *posting.PostingDeltaTailStats,
        posting_id: PostingId,
        base_generation: u64,
    ) !void {
        for (self.writer.entries.items) |entry| {
            if (entry.posting_id != posting_id or entry.kind != .delta) continue;
            var iterator = try posting.PostingFormat.DeltaTailIterator.init(entry.value);
            stats.records += iterator.recordCount();
            stats.encoded_value_bytes += entry.value.len;
            while (try iterator.next()) |record| {
                if (posting.PostingFormat.deltaSequenceGeneration(record.sequence) <= base_generation) continue;
                stats.records_after_generation += 1;
                if (record.op == .tombstone) stats.tombstones_after_generation += 1;
                stats.max_sequence_after_generation = @max(stats.max_sequence_after_generation, record.sequence);
                try appendDeltaRecordToScratch(alloc, scratch, record);
            }
        }
    }

    fn accumulatePendingDeltaEntryStats(
        self: *const DirectoryBatchWriter,
        out: *posting.PostingDeltaTailStats,
        posting_id: PostingId,
        base_generation: u64,
    ) !void {
        for (self.writer.entries.items) |entry| {
            if (entry.posting_id != posting_id or entry.kind != .delta) continue;
            out.encoded_value_bytes += entry.value.len;
            const stats = try posting.PostingFormat.deltaTailStatsAfterGeneration(entry.value, base_generation);
            out.records += stats.records;
            out.records_after_generation += stats.records_after_generation;
            out.tombstones_after_generation += stats.tombstones_after_generation;
            out.max_sequence_after_generation = @max(out.max_sequence_after_generation, stats.max_sequence_after_generation);
        }
    }

    fn latestPendingDeltaEntryAfterGenerationForMember(
        self: *const DirectoryBatchWriter,
        posting_id: PostingId,
        vector_id: posting.VectorId,
        base_generation: u64,
        best_sequence: *u64,
        best_record: *?posting.PostingDeltaRecord,
    ) !void {
        for (self.writer.entries.items) |entry| {
            if (entry.posting_id != posting_id or entry.kind != .delta) continue;
            var iterator = try posting.PostingFormat.DeltaTailIterator.init(entry.value);
            while (try iterator.next()) |record| {
                if (record.vector_id != vector_id) continue;
                if (posting.PostingFormat.deltaSequenceGeneration(record.sequence) <= base_generation) continue;
                if (best_record.* == null or record.sequence >= best_sequence.*) {
                    best_sequence.* = record.sequence;
                    best_record.* = record;
                }
            }
        }
    }

    fn replacePendingPoint(self: *DirectoryBatchWriter, posting_id: PostingId, kind: EntryKind, value: []const u8) !bool {
        std.debug.assert(kind != .delta);
        for (self.writer.entries.items) |*entry| {
            if (entry.posting_id != posting_id or entry.kind != kind or entry.sequence != 0) continue;
            const owned = try self.alloc.dupe(u8, value);
            const old_len = entry.value.len;
            self.alloc.free(entry.value);
            entry.value = owned;
            if (value.len >= old_len) {
                self.pending_value_bytes = std.math.add(usize, self.pending_value_bytes, value.len - old_len) catch return error.PostingSegmentTooLarge;
            } else {
                self.pending_value_bytes -= old_len - value.len;
            }
            if (self.options.max_pending_value_bytes != 0 and self.pending_value_bytes >= self.options.max_pending_value_bytes) {
                _ = try self.flush();
            }
            return true;
        }
        return false;
    }

    fn replacePendingPointOwned(self: *DirectoryBatchWriter, posting_id: PostingId, kind: EntryKind, value: []u8) !bool {
        std.debug.assert(kind != .delta);
        for (self.writer.entries.items) |*entry| {
            if (entry.posting_id != posting_id or entry.kind != kind or entry.sequence != 0) continue;
            const old_len = entry.value.len;
            const next_pending_value_bytes = if (value.len >= old_len)
                std.math.add(usize, self.pending_value_bytes, value.len - old_len) catch return error.PostingSegmentTooLarge
            else
                self.pending_value_bytes - (old_len - value.len);
            self.alloc.free(entry.value);
            entry.value = value;
            self.pending_value_bytes = next_pending_value_bytes;
            if (self.options.max_pending_value_bytes != 0 and self.pending_value_bytes >= self.options.max_pending_value_bytes) {
                _ = try self.flush();
            }
            return true;
        }
        return false;
    }

    fn notePendingAppend(self: *DirectoryBatchWriter, value_len: usize) !void {
        self.pending_value_bytes = std.math.add(usize, self.pending_value_bytes, value_len) catch return error.PostingSegmentTooLarge;
        if (self.options.max_pending_entries != 0 and self.pendingEntries() >= self.options.max_pending_entries) {
            _ = try self.flush();
            return;
        }
        if (self.options.max_pending_value_bytes != 0 and self.pending_value_bytes >= self.options.max_pending_value_bytes) {
            _ = try self.flush();
        }
    }

    fn drainPendingDeltaBatches(self: *DirectoryBatchWriter) !void {
        if (self.pending_delta_records == 0) return;
        var iter = self.pending_delta_batches.iterator();
        while (iter.next()) |entry| {
            if (entry.value_ptr.records.items.len == 0) continue;
            var min_sequence = entry.value_ptr.records.items[0].sequence;
            for (entry.value_ptr.records.items[1..]) |record| min_sequence = @min(min_sequence, record.sequence);
            const encoded = try posting.PostingFormat.encodeDeltaTail(self.alloc, entry.value_ptr.records.items);
            try self.writer.appendOwnedDelta(entry.key_ptr.*, min_sequence, encoded);
        }
        self.clearPendingDeltaBatches();
    }

    fn clearPendingDeltaBatches(self: *DirectoryBatchWriter) void {
        var iter = self.pending_delta_batches.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.deinit(self.alloc);
        }
        self.pending_delta_batches.clearRetainingCapacity();
        self.pending_delta_records = 0;
    }

    fn resetPendingWriter(self: *DirectoryBatchWriter) void {
        self.writer.deinit();
        self.writer = Writer.init(self.alloc);
        self.pending_value_bytes = 0;
    }
};

pub const Reader = struct {
    data: []const u8,
    index_offset: usize,
    entry_count: usize,

    pub fn init(data: []const u8) !Reader {
        if (data.len < footer_size) return error.CorruptedPostingSegment;
        const footer = data[data.len - footer_size ..];
        if (!std.mem.eql(u8, footer[footer_size - magic.len ..], &magic)) return error.BadPostingSegmentMagic;
        const segment_version = readU16(footer[24..26]);
        if (segment_version != version) return error.UnsupportedPostingSegmentVersion;
        const stored_checksum = readU32(footer[20..24]);
        const checksum_end = data.len - footer_size + 20;
        if (segmentChecksum(data[0..checksum_end]) != stored_checksum) return error.BadPostingSegmentChecksum;
        const index_offset_u64 = readU64(footer[0..8]);
        const entry_count_u64 = readU64(footer[8..16]);
        const index_offset = std.math.cast(usize, index_offset_u64) orelse return error.CorruptedPostingSegment;
        const entry_count = std.math.cast(usize, entry_count_u64) orelse return error.CorruptedPostingSegment;
        const index_bytes = std.math.mul(usize, entry_count, index_entry_size) catch return error.CorruptedPostingSegment;
        const index_end = std.math.add(usize, index_offset, index_bytes) catch return error.CorruptedPostingSegment;
        if (index_offset > data.len - footer_size or index_end != data.len - footer_size) return error.CorruptedPostingSegment;
        const stored_index_checksum = readU32(footer[16..20]);
        if (indexChecksum(data[index_offset..index_end]) != stored_index_checksum) return error.BadPostingSegmentChecksum;
        return .{
            .data = data,
            .index_offset = index_offset,
            .entry_count = entry_count,
        };
    }

    pub fn getBase(self: Reader, posting_id: PostingId) !?[]const u8 {
        return try self.getExact(posting_id, .base, 0);
    }

    pub fn getBaseLocation(self: Reader, posting_id: PostingId) !?ValueLocation {
        return try self.getExactLocation(posting_id, .base, 0);
    }

    pub fn getCentroidDirectory(self: Reader, posting_id: PostingId) !?[]const u8 {
        return try self.getExact(posting_id, .centroid_directory, 0);
    }

    pub fn getCentroidDirectoryLocation(self: Reader, posting_id: PostingId) !?ValueLocation {
        return try self.getExactLocation(posting_id, .centroid_directory, 0);
    }

    pub fn deltas(self: Reader, posting_id: PostingId) DeltaIterator {
        return .{
            .reader = self,
            .posting_id = posting_id,
            .index = self.lowerBound(posting_id, .delta, 0),
        };
    }

    pub fn entries(self: Reader) EntryIterator {
        return .{
            .reader = self,
            .index = 0,
        };
    }

    pub fn metadata(self: Reader, segment_id: u64) !SegmentMeta {
        var meta = SegmentMeta{
            .segment_id = segment_id,
            .byte_len = self.data.len,
            .entry_count = self.entry_count,
            .index_offset = self.index_offset,
            .index_checksum = indexChecksum(self.data[self.index_offset .. self.index_offset + self.entry_count * index_entry_size]),
        };
        if (self.entry_count == 0) return meta;

        var i: usize = 0;
        while (i < self.entry_count) : (i += 1) {
            const entry = try self.indexEntry(i);
            if (i == 0) {
                meta.min_posting_id = entry.posting_id;
                meta.max_posting_id = entry.posting_id;
            } else {
                meta.min_posting_id = @min(meta.min_posting_id, entry.posting_id);
                meta.max_posting_id = @max(meta.max_posting_id, entry.posting_id);
            }
            if (entry.kind == .delta) {
                var delta_iter = try posting.PostingFormat.DeltaTailIterator.init(try entry.value(self.data));
                while (try delta_iter.next()) |record| {
                    if (meta.min_delta_sequence == 0 or record.sequence < meta.min_delta_sequence) {
                        meta.min_delta_sequence = record.sequence;
                    }
                    meta.max_delta_sequence = @max(meta.max_delta_sequence, record.sequence);
                }
            }
        }
        return meta;
    }

    fn getExact(self: Reader, posting_id: PostingId, kind: EntryKind, sequence: u64) !?[]const u8 {
        const location = (try self.getExactLocation(posting_id, kind, sequence)) orelse return null;
        return try location.value(self.data);
    }

    fn getExactLocation(self: Reader, posting_id: PostingId, kind: EntryKind, sequence: u64) !?ValueLocation {
        const index = self.lowerBound(posting_id, kind, sequence);
        if (index >= self.entry_count) return null;
        const entry = try self.indexEntry(index);
        if (entry.posting_id != posting_id or entry.kind != kind or entry.sequence != sequence) return null;
        const location = entry.location();
        _ = try location.value(self.data);
        return location;
    }

    fn lowerBound(self: Reader, posting_id: PostingId, kind: EntryKind, sequence: u64) usize {
        var lo: usize = 0;
        var hi: usize = self.entry_count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const entry = self.indexEntry(mid) catch {
                hi = mid;
                continue;
            };
            if (compareEntryKey(entry.posting_id, entry.kind, entry.sequence, posting_id, kind, sequence) == .lt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    fn indexEntry(self: Reader, index: usize) !IndexEntry {
        if (index >= self.entry_count) return error.CorruptedPostingSegment;
        const pos = self.index_offset + index * index_entry_size;
        return try indexEntryFromBytes(self.data[pos .. pos + index_entry_size]);
    }
};

pub const DeltaIterator = struct {
    reader: Reader,
    posting_id: PostingId,
    index: usize,

    pub fn next(self: *DeltaIterator) !?DeltaValue {
        if (self.index >= self.reader.entry_count) return null;
        const entry = try self.reader.indexEntry(self.index);
        if (entry.posting_id != self.posting_id or entry.kind != .delta) return null;
        self.index += 1;
        return .{
            .sequence = entry.sequence,
            .value = try entry.value(self.reader.data),
        };
    }
};

pub const EntryIterator = struct {
    reader: Reader,
    index: usize,

    pub fn next(self: *EntryIterator) !?EntryValue {
        if (self.index >= self.reader.entry_count) return null;
        const entry = try self.reader.indexEntry(self.index);
        self.index += 1;
        return .{
            .posting_id = entry.posting_id,
            .kind = entry.kind,
            .sequence = entry.sequence,
            .value = try entry.value(self.reader.data),
        };
    }
};

fn lowerBoundIndexData(index_data: []const u8, entry_count: usize, posting_id: PostingId, kind: EntryKind, sequence: u64) usize {
    var lo: usize = 0;
    var hi: usize = entry_count;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const entry = indexEntryFromBytes(index_data[mid * index_entry_size ..][0..index_entry_size]) catch {
            hi = mid;
            continue;
        };
        if (compareEntryKey(entry.posting_id, entry.kind, entry.sequence, posting_id, kind, sequence) == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

fn indexEntryFromBytes(raw: []const u8) !IndexEntry {
    if (raw.len != index_entry_size) return error.CorruptedPostingSegment;
    const kind: EntryKind = switch (raw[8]) {
        @intFromEnum(EntryKind.base) => .base,
        @intFromEnum(EntryKind.delta) => .delta,
        @intFromEnum(EntryKind.centroid_directory) => .centroid_directory,
        else => return error.CorruptedPostingSegment,
    };
    const offset = std.math.cast(usize, readU64(raw[17..25])) orelse return error.CorruptedPostingSegment;
    const len = std.math.cast(usize, readU64(raw[25..33])) orelse return error.CorruptedPostingSegment;
    return .{
        .posting_id = readU64(raw[0..8]),
        .kind = kind,
        .sequence = readU64(raw[9..17]),
        .offset = offset,
        .len = len,
        .value_checksum = readU32(raw[33..37]),
    };
}

fn appendIndexEntry(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), entry: IndexEntry) !void {
    try appendU64(alloc, out, entry.posting_id);
    try out.append(alloc, @intFromEnum(entry.kind));
    try appendU64(alloc, out, entry.sequence);
    try appendU64(alloc, out, @intCast(entry.offset));
    try appendU64(alloc, out, @intCast(entry.len));
    try appendU32(alloc, out, entry.value_checksum);
}

fn validateManifest(manifest: Manifest) !void {
    var previous_segment_id: ?u64 = null;
    for (manifest.segments) |entry| {
        try validateManifestEntry(entry);
        if (entry.meta.segment_id >= manifest.next_segment_id) return error.InvalidPostingSegmentManifest;
        if (previous_segment_id) |previous| {
            if (previous >= entry.meta.segment_id) return error.InvalidPostingSegmentManifest;
        }
        previous_segment_id = entry.meta.segment_id;
    }
}

fn validateManifestEntry(entry: ManifestEntry) !void {
    if (entry.path.len == 0 or entry.path.len > std.math.maxInt(u32)) return error.InvalidPostingSegmentManifest;
    if (entry.meta.entry_count == 0) return error.InvalidPostingSegmentManifest;
    if (entry.meta.byte_len == 0) return error.InvalidPostingSegmentManifest;
    if (entry.meta.min_posting_id > entry.meta.max_posting_id) return error.InvalidPostingSegmentManifest;
    if (entry.meta.min_delta_sequence != 0 and entry.meta.max_delta_sequence < entry.meta.min_delta_sequence) return error.InvalidPostingSegmentManifest;
}

fn validateSegmentDataMatchesMeta(data: []const u8, expected: SegmentMeta) !void {
    if (data.len != expected.byte_len) return error.InvalidPostingSegment;
    const reader = try Reader.init(data);
    const actual = try reader.metadata(expected.segment_id);
    if (!segmentMetaEql(actual, expected)) return error.InvalidPostingSegment;
}

fn readSegmentIndexAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, entry: ManifestEntry) ![]u8 {
    try validateManifestEntry(entry);
    const index_bytes = std.math.mul(usize, entry.meta.entry_count, index_entry_size) catch return error.CorruptedPostingSegment;
    const index_end = std.math.add(usize, entry.meta.index_offset, index_bytes) catch return error.CorruptedPostingSegment;
    if (entry.meta.index_offset > entry.meta.byte_len or index_end > entry.meta.byte_len - footer_size) return error.CorruptedPostingSegment;

    const index_data = try readFileRangeAlloc(alloc, io, dir, entry.path, @intCast(entry.meta.index_offset), index_bytes);
    errdefer alloc.free(index_data);
    if (indexChecksum(index_data) != entry.meta.index_checksum) return error.BadPostingSegmentChecksum;
    return index_data;
}

fn readSegmentEntryValueAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, entry: ManifestEntry, found: IndexEntry) ![]u8 {
    const value_end = std.math.add(usize, found.offset, found.len) catch return error.CorruptedPostingSegment;
    if (found.offset > entry.meta.index_offset or value_end > entry.meta.index_offset) return error.CorruptedPostingSegment;

    const value = try readFileRangeAlloc(alloc, io, dir, entry.path, @intCast(found.offset), found.len);
    errdefer alloc.free(value);
    try found.location().verifyValue(value);
    return value;
}

fn readSegmentDeltaValueRangeAlloc(
    alloc: Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    entry: ManifestEntry,
    index_data: []const u8,
    posting_id: PostingId,
) !?OwnedDeltaValueRange {
    const first_index = lowerBoundIndexData(index_data, entry.meta.entry_count, posting_id, .delta, 0);
    if (first_index >= entry.meta.entry_count) return null;

    const first_entry = try indexEntryFromBytes(index_data[first_index * index_entry_size ..][0..index_entry_size]);
    if (first_entry.posting_id != posting_id or first_entry.kind != .delta) return null;

    var range_offset = first_entry.offset;
    var range_end = std.math.add(usize, first_entry.offset, first_entry.len) catch return error.CorruptedPostingSegment;
    if (first_entry.offset > entry.meta.index_offset or range_end > entry.meta.index_offset) return error.CorruptedPostingSegment;

    var past_index = first_index + 1;
    while (past_index < entry.meta.entry_count) : (past_index += 1) {
        const found = try indexEntryFromBytes(index_data[past_index * index_entry_size ..][0..index_entry_size]);
        if (found.posting_id != posting_id or found.kind != .delta) break;
        const value_end = std.math.add(usize, found.offset, found.len) catch return error.CorruptedPostingSegment;
        if (found.offset > entry.meta.index_offset or value_end > entry.meta.index_offset) return error.CorruptedPostingSegment;
        range_offset = @min(range_offset, found.offset);
        range_end = @max(range_end, value_end);
    }

    const range_len = range_end - range_offset;
    const data = try readFileRangeAlloc(alloc, io, dir, entry.path, @intCast(range_offset), range_len);
    return .{
        .data = data,
        .base_offset = range_offset,
        .first_index = first_index,
        .past_index = past_index,
    };
}

fn deltaValueFromRange(index_data: []const u8, range: OwnedDeltaValueRange, index: usize) ![]const u8 {
    const found = try indexEntryFromBytes(index_data[index * index_entry_size ..][0..index_entry_size]);
    const relative_offset = std.math.sub(usize, found.offset, range.base_offset) catch return error.CorruptedPostingSegment;
    const relative_end = std.math.add(usize, relative_offset, found.len) catch return error.CorruptedPostingSegment;
    if (relative_end > range.data.len) return error.CorruptedPostingSegment;
    const value = range.data[relative_offset..relative_end];
    try found.location().verifyValue(value);
    return value;
}

fn segmentMetaEql(lhs: SegmentMeta, rhs: SegmentMeta) bool {
    return lhs.segment_id == rhs.segment_id and
        lhs.min_posting_id == rhs.min_posting_id and
        lhs.max_posting_id == rhs.max_posting_id and
        lhs.min_delta_sequence == rhs.min_delta_sequence and
        lhs.max_delta_sequence == rhs.max_delta_sequence and
        lhs.byte_len == rhs.byte_len and
        lhs.entry_count == rhs.entry_count and
        lhs.index_offset == rhs.index_offset and
        lhs.index_checksum == rhs.index_checksum;
}

fn putNewestPoint(
    alloc: Allocator,
    map: *std.AutoHashMapUnmanaged(PostingId, PointCandidate),
    posting_id: PostingId,
    segment_id: u64,
    value: []const u8,
) !void {
    const entry = try map.getOrPut(alloc, posting_id);
    if (!entry.found_existing or segment_id > entry.value_ptr.segment_id) {
        entry.value_ptr.* = .{
            .segment_id = segment_id,
            .value = value,
        };
    }
}

fn writeFileAtomicallyAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, path: []const u8, data: []const u8) !void {
    try dir.createDirPath(io, segment_directory);
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp", .{path});
    defer alloc.free(tmp_path);
    try dir.writeFile(io, .{
        .sub_path = tmp_path,
        .data = data,
    });
    std.Io.Dir.rename(dir, tmp_path, dir, path, io) catch |err| {
        dir.deleteFile(io, tmp_path) catch {};
        return err;
    };
}

fn readFileRangeAlloc(alloc: Allocator, io: std.Io, dir: std.Io.Dir, path: []const u8, offset: u64, len: usize) ![]u8 {
    const file = try dir.openFile(io, path, .{});
    defer file.close(io);

    var reader = file.reader(io, &.{});
    try reader.seekTo(offset);
    const out = try alloc.alloc(u8, len);
    errdefer alloc.free(out);
    reader.interface.readSliceAll(out) catch |err| switch (err) {
        error.EndOfStream => return error.CorruptedPostingSegment,
        else => return err,
    };
    return out;
}

fn segmentChecksum(data: []const u8) u32 {
    return std.hash.Crc32.hash(data);
}

fn indexChecksum(data: []const u8) u32 {
    return std.hash.Crc32.hash(data);
}

fn valueChecksum(data: []const u8) u32 {
    return std.hash.Crc32.hash(data);
}

fn manifestChecksum(data: []const u8) u32 {
    return std.hash.Crc32.hash(data);
}

fn appendManifestEntry(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), entry: ManifestEntry) !void {
    try appendU64(alloc, out, entry.meta.segment_id);
    try appendU64(alloc, out, entry.meta.min_posting_id);
    try appendU64(alloc, out, entry.meta.max_posting_id);
    try appendU64(alloc, out, entry.meta.min_delta_sequence);
    try appendU64(alloc, out, entry.meta.max_delta_sequence);
    try appendU64(alloc, out, @intCast(entry.meta.byte_len));
    try appendU64(alloc, out, @intCast(entry.meta.entry_count));
    try appendU64(alloc, out, @intCast(entry.meta.index_offset));
    try appendU32(alloc, out, entry.meta.index_checksum);
    try appendU32(alloc, out, @intCast(entry.path.len));
    try out.appendSlice(alloc, entry.path);
}

fn readManifestEntry(alloc: Allocator, data: []const u8, pos: *usize) !OwnedManifestEntry {
    const fixed_size = 8 * @sizeOf(u64) + 2 * @sizeOf(u32);
    if (pos.* > data.len or data.len - pos.* < fixed_size) return error.CorruptedPostingSegmentManifest;
    const segment_id = readU64(data[pos.*..][0..8]);
    pos.* += 8;
    const min_posting_id = readU64(data[pos.*..][0..8]);
    pos.* += 8;
    const max_posting_id = readU64(data[pos.*..][0..8]);
    pos.* += 8;
    const min_delta_sequence = readU64(data[pos.*..][0..8]);
    pos.* += 8;
    const max_delta_sequence = readU64(data[pos.*..][0..8]);
    pos.* += 8;
    const byte_len_u64 = readU64(data[pos.*..][0..8]);
    pos.* += 8;
    const entry_count_u64 = readU64(data[pos.*..][0..8]);
    pos.* += 8;
    const index_offset_u64 = readU64(data[pos.*..][0..8]);
    pos.* += 8;
    const index_checksum = readU32(data[pos.*..][0..4]);
    pos.* += 4;
    const path_len_u32 = readU32(data[pos.*..][0..4]);
    pos.* += 4;
    const path_len = std.math.cast(usize, path_len_u32) orelse return error.CorruptedPostingSegmentManifest;
    if (path_len == 0 or pos.* > data.len or data.len - pos.* < path_len) return error.CorruptedPostingSegmentManifest;
    const path = try alloc.dupe(u8, data[pos.* .. pos.* + path_len]);
    errdefer alloc.free(path);
    pos.* += path_len;
    const meta = SegmentMeta{
        .segment_id = segment_id,
        .min_posting_id = min_posting_id,
        .max_posting_id = max_posting_id,
        .min_delta_sequence = min_delta_sequence,
        .max_delta_sequence = max_delta_sequence,
        .byte_len = std.math.cast(usize, byte_len_u64) orelse return error.CorruptedPostingSegmentManifest,
        .entry_count = std.math.cast(usize, entry_count_u64) orelse return error.CorruptedPostingSegmentManifest,
        .index_offset = std.math.cast(usize, index_offset_u64) orelse return error.CorruptedPostingSegmentManifest,
        .index_checksum = index_checksum,
    };
    try validateManifestEntry(.{ .meta = meta, .path = path });
    return .{
        .meta = meta,
        .path = path,
    };
}

fn rejectDuplicateEntries(entries: []const PendingEntry) !void {
    if (entries.len < 2) return;
    var i: usize = 1;
    while (i < entries.len) : (i += 1) {
        const prev = entries[i - 1];
        const cur = entries[i];
        if (prev.posting_id == cur.posting_id and prev.kind == cur.kind and prev.sequence == cur.sequence) {
            return error.DuplicatePostingSegmentEntry;
        }
    }
}

fn pendingEntryLessThan(_: void, lhs: PendingEntry, rhs: PendingEntry) bool {
    return compareEntryKey(lhs.posting_id, lhs.kind, lhs.sequence, rhs.posting_id, rhs.kind, rhs.sequence) == .lt;
}

fn manifestEntryLessThan(_: void, lhs: ManifestEntry, rhs: ManifestEntry) bool {
    return lhs.meta.segment_id < rhs.meta.segment_id;
}

fn deltaValueLessThan(_: void, lhs: DeltaValue, rhs: DeltaValue) bool {
    return lhs.sequence < rhs.sequence;
}

fn postingDeltaRecordLessThan(_: void, lhs: posting.PostingDeltaRecord, rhs: posting.PostingDeltaRecord) bool {
    return lhs.sequence < rhs.sequence;
}

fn appendDeltaRecordToScratch(alloc: Allocator, scratch: anytype, record: posting.PostingDeltaRecord) !void {
    const Scratch = std.meta.Child(@TypeOf(scratch));
    const needed = scratch.deltaRecordCount() + 1;
    if (comptime @hasField(Scratch, "delta_records")) {
        if (needed > scratch.delta_records.len) {
            const doubled = scratch.delta_records.len *| 2;
            try scratch.ensureDeltaRecordCapacity(alloc, @max(needed, @max(doubled, @as(usize, 8))));
        }
    } else if (comptime @hasField(Scratch, "posting_delta_records")) {
        if (needed > scratch.posting_delta_records.len) {
            const doubled = scratch.posting_delta_records.len *| 2;
            try scratch.ensureDeltaRecordCapacity(alloc, @max(needed, @max(doubled, @as(usize, 8))));
        }
    } else {
        try scratch.ensureDeltaRecordCapacity(alloc, needed);
    }
    scratch.appendDeltaRecordAssumeCapacity(record);
}

fn compactMembersWithOverlayPlan(alloc: Allocator, scratch: anytype, base_member_count: usize) !usize {
    const appended_count = posting.PostingFormat.overlayAppendedCount(scratch).*;
    try scratch.ensureMemberIdCapacity(alloc, base_member_count + appended_count);
    const base_members = scratch.member_ids[0..base_member_count];
    const out = scratch.member_ids;
    var member_count: usize = 0;
    for (base_members) |member| {
        if (posting.PostingFormat.overlayRemovedMembers(scratch).contains(member)) continue;
        out[member_count] = member;
        member_count += 1;
    }

    const appended_ids = posting.PostingFormat.overlayAppendedIds(scratch);
    const appended_live = posting.PostingFormat.overlayAppendedLive(scratch);
    var append_index: usize = 0;
    while (append_index < appended_count) : (append_index += 1) {
        if (!appended_live[append_index]) continue;
        out[member_count] = appended_ids[append_index];
        member_count += 1;
    }
    return member_count;
}

fn deltaCandidateLessThan(_: void, lhs: DeltaCandidate, rhs: DeltaCandidate) bool {
    if (lhs.posting_id < rhs.posting_id) return true;
    if (lhs.posting_id > rhs.posting_id) return false;
    if (lhs.record.sequence < rhs.record.sequence) return true;
    if (lhs.record.sequence > rhs.record.sequence) return false;
    return lhs.segment_id < rhs.segment_id;
}

fn compareEntryKey(lhs_posting_id: PostingId, lhs_kind: EntryKind, lhs_sequence: u64, rhs_posting_id: PostingId, rhs_kind: EntryKind, rhs_sequence: u64) std.math.Order {
    if (lhs_posting_id < rhs_posting_id) return .lt;
    if (lhs_posting_id > rhs_posting_id) return .gt;
    if (@intFromEnum(lhs_kind) < @intFromEnum(rhs_kind)) return .lt;
    if (@intFromEnum(lhs_kind) > @intFromEnum(rhs_kind)) return .gt;
    if (lhs_sequence < rhs_sequence) return .lt;
    if (lhs_sequence > rhs_sequence) return .gt;
    return .eq;
}

fn segmentIdIn(segment_id: u64, segment_ids: []const u64) bool {
    for (segment_ids) |candidate| {
        if (candidate == segment_id) return true;
    }
    return false;
}

fn rejectDuplicateSegmentIds(segment_ids: []const u64) !void {
    if (segment_ids.len < 2) return;
    var i: usize = 1;
    while (i < segment_ids.len) : (i += 1) {
        var j: usize = 0;
        while (j < i) : (j += 1) {
            if (segment_ids[i] == segment_ids[j]) return error.DuplicatePostingSegmentId;
        }
    }
}

fn appendU16(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u16) !void {
    var buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &buf, value, .big);
    try out.appendSlice(alloc, &buf);
}

fn appendU32(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .big);
    try out.appendSlice(alloc, &buf);
}

fn appendU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .big);
    try out.appendSlice(alloc, &buf);
}

fn readU16(bytes: *const [2]u8) u16 {
    return std.mem.readInt(u16, bytes, .big);
}

fn readU32(bytes: *const [4]u8) u32 {
    return std.mem.readInt(u32, bytes, .big);
}

fn readU64(bytes: *const [8]u8) u64 {
    return std.mem.readInt(u64, bytes, .big);
}

pub fn testStoresBaseCentroidAndOrderedDeltaValues() !void {
    const alloc = std.testing.allocator;
    const base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 3,
        .members = &.{ 10, 20, 30 },
    });
    defer alloc.free(base);
    const delta_4 = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = 4, .op = .insert, .vector_id = 40 },
    });
    defer alloc.free(delta_4);
    const delta_5 = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = 5, .op = .tombstone, .vector_id = 20 },
    });
    defer alloc.free(delta_5);
    const centroid = try posting.CentroidDirectoryFormat.encode(alloc, .{
        .posting_id = 7,
        .generation = 3,
        .mutation_version = 5,
        .payload_version = 3,
        .flags = 0,
        .parent = 1,
        .level = 0,
        .member_count = 3,
        .bounds_radius = 1.5,
        .centroid = &.{ 1.0, 2.0 },
    });
    defer alloc.free(centroid);

    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendDelta(7, 5, delta_5);
    try writer.appendCentroidDirectory(7, centroid);
    try writer.appendBase(7, base);
    try writer.appendDelta(7, 4, delta_4);

    const bytes = try writer.build();
    defer alloc.free(bytes);

    const reader = try Reader.init(bytes);
    try std.testing.expectEqualSlices(u8, base, (try reader.getBase(7)).?);
    try std.testing.expectEqualSlices(u8, centroid, (try reader.getCentroidDirectory(7)).?);
    try std.testing.expect(try reader.getBase(8) == null);

    var iter = reader.deltas(7);
    const first = (try iter.next()).?;
    try std.testing.expectEqual(@as(u64, 4), first.sequence);
    try std.testing.expectEqualSlices(u8, delta_4, first.value);
    const second = (try iter.next()).?;
    try std.testing.expectEqual(@as(u64, 5), second.sequence);
    try std.testing.expectEqualSlices(u8, delta_5, second.value);
    try std.testing.expect(try iter.next() == null);

    var entry_iter = reader.entries();
    const base_entry = (try entry_iter.next()).?;
    try std.testing.expectEqual(@as(PostingId, 7), base_entry.posting_id);
    try std.testing.expectEqual(EntryKind.base, base_entry.kind);
    try std.testing.expectEqual(@as(u64, 0), base_entry.sequence);
    try std.testing.expectEqualSlices(u8, base, base_entry.value);

    const delta_4_entry = (try entry_iter.next()).?;
    try std.testing.expectEqual(@as(PostingId, 7), delta_4_entry.posting_id);
    try std.testing.expectEqual(EntryKind.delta, delta_4_entry.kind);
    try std.testing.expectEqual(@as(u64, 4), delta_4_entry.sequence);
    try std.testing.expectEqualSlices(u8, delta_4, delta_4_entry.value);

    const delta_5_entry = (try entry_iter.next()).?;
    try std.testing.expectEqual(@as(PostingId, 7), delta_5_entry.posting_id);
    try std.testing.expectEqual(EntryKind.delta, delta_5_entry.kind);
    try std.testing.expectEqual(@as(u64, 5), delta_5_entry.sequence);
    try std.testing.expectEqualSlices(u8, delta_5, delta_5_entry.value);

    const centroid_entry = (try entry_iter.next()).?;
    try std.testing.expectEqual(@as(PostingId, 7), centroid_entry.posting_id);
    try std.testing.expectEqual(EntryKind.centroid_directory, centroid_entry.kind);
    try std.testing.expectEqual(@as(u64, 0), centroid_entry.sequence);
    try std.testing.expectEqualSlices(u8, centroid, centroid_entry.value);
    try std.testing.expect(try entry_iter.next() == null);
}

pub fn testReaderReportsPointValueLocations() !void {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBase(7, "base-value");
    try writer.appendCentroidDirectory(7, "centroid-value");
    try writer.appendBase(9, "other-base");

    const bytes = try writer.build();
    defer alloc.free(bytes);

    const reader = try Reader.init(bytes);
    const base_location = (try reader.getBaseLocation(7)).?;
    const centroid_location = (try reader.getCentroidDirectoryLocation(7)).?;
    try std.testing.expect(base_location.offset < reader.index_offset);
    try std.testing.expect(centroid_location.offset < reader.index_offset);
    try std.testing.expectEqualSlices(u8, "base-value", try base_location.value(bytes));
    try std.testing.expectEqualSlices(u8, "centroid-value", try centroid_location.value(bytes));
    try base_location.verifyValue("base-value");
    try std.testing.expectError(error.BadPostingSegmentChecksum, base_location.verifyValue("base-VALUE"));
    try std.testing.expectError(error.CorruptedPostingSegment, base_location.verifyValue("base"));
    try std.testing.expectEqualSlices(u8, (try reader.getBase(7)).?, try base_location.value(bytes));
    try std.testing.expectEqualSlices(u8, (try reader.getCentroidDirectory(7)).?, try centroid_location.value(bytes));
    try std.testing.expect(try reader.getBaseLocation(8) == null);
    try std.testing.expect(try reader.getCentroidDirectoryLocation(8) == null);

    var bad_value_checksum = try alloc.dupe(u8, bytes);
    defer alloc.free(bad_value_checksum);
    bad_value_checksum[reader.index_offset + 33] ^= 0xff;
    const index_checksum_pos = bad_value_checksum.len - footer_size + 16;
    const segment_checksum_pos = bad_value_checksum.len - footer_size + 20;
    std.mem.writeInt(u32, bad_value_checksum[index_checksum_pos..][0..4], indexChecksum(bad_value_checksum[reader.index_offset .. bad_value_checksum.len - footer_size]), .big);
    std.mem.writeInt(u32, bad_value_checksum[segment_checksum_pos..][0..4], segmentChecksum(bad_value_checksum[0..segment_checksum_pos]), .big);
    const bad_reader = try Reader.init(bad_value_checksum);
    try std.testing.expectError(error.BadPostingSegmentChecksum, bad_reader.getBaseLocation(7));
}

pub fn testSegmentPointValueRangeReadsVerifyIndexAndValue() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 2,
        .members = &.{ 10, 20, 30 },
    });
    defer alloc.free(base);
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBase(7, base);
    const stale_sequence = (@as(u64, 1) << 32) | 1;
    const live_sequence = (@as(u64, 3) << 32) | 1;
    const delta = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = stale_sequence, .op = .insert, .vector_id = 40 },
        .{ .sequence = live_sequence, .op = .tombstone, .vector_id = 20 },
    });
    defer alloc.free(delta);
    try writer.appendDelta(7, stale_sequence, delta);

    var committed = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer, .{});
    defer committed.deinit(alloc);
    const entry = ManifestEntry{
        .meta = committed.entry.meta,
        .path = committed.entry.path,
    };

    const loaded = (try readSegmentPointValueAlloc(alloc, std.testing.io, tmp.dir, entry, 7, .base)).?;
    defer alloc.free(loaded);
    try std.testing.expectEqualSlices(u8, base, loaded);
    try std.testing.expect(try readSegmentPointValueAlloc(alloc, std.testing.io, tmp.dir, entry, 8, .base) == null);
    try std.testing.expectError(error.InvalidPostingSegmentEntryKind, readSegmentPointValueAlloc(alloc, std.testing.io, tmp.dir, entry, 7, .delta));
    const delta_records = try readSegmentDeltaRecordsAlloc(alloc, std.testing.io, tmp.dir, entry, 7, null);
    defer alloc.free(delta_records);
    try std.testing.expectEqual(@as(usize, 2), delta_records.len);
    try std.testing.expectEqual(stale_sequence, delta_records[0].sequence);
    try std.testing.expectEqual(live_sequence, delta_records[1].sequence);
    const live_delta_records = try readSegmentDeltaRecordsAlloc(alloc, std.testing.io, tmp.dir, entry, 7, 2);
    defer alloc.free(live_delta_records);
    try std.testing.expectEqual(@as(usize, 1), live_delta_records.len);
    try std.testing.expectEqual(live_sequence, live_delta_records[0].sequence);

    const original = try tmp.dir.readFileAlloc(std.testing.io, committed.entry.path, alloc, .limited(committed.entry.meta.byte_len + 1));
    defer alloc.free(original);
    const reader = try Reader.init(original);
    const base_location = (try reader.getBaseLocation(7)).?;
    const delta_index = reader.lowerBound(7, .delta, 0);
    const delta_entry = try reader.indexEntry(delta_index);

    var corrupt_footer = try alloc.dupe(u8, original);
    defer alloc.free(corrupt_footer);
    corrupt_footer[corrupt_footer.len - footer_size + 20] ^= 0xff;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = committed.entry.path, .data = corrupt_footer });
    try std.testing.expectError(error.BadPostingSegmentChecksum, readSegmentFileAlloc(alloc, std.testing.io, tmp.dir, committed.entry.path, committed.entry.meta.byte_len));
    const footer_corrupt_value = (try readSegmentPointValueAlloc(alloc, std.testing.io, tmp.dir, entry, 7, .base)).?;
    defer alloc.free(footer_corrupt_value);
    try std.testing.expectEqualSlices(u8, base, footer_corrupt_value);

    var corrupt_index = try alloc.dupe(u8, original);
    defer alloc.free(corrupt_index);
    corrupt_index[committed.entry.meta.index_offset] ^= 0xff;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = committed.entry.path, .data = corrupt_index });
    try std.testing.expectError(error.BadPostingSegmentChecksum, readSegmentPointValueAlloc(alloc, std.testing.io, tmp.dir, entry, 7, .base));

    var corrupt_value = try alloc.dupe(u8, original);
    defer alloc.free(corrupt_value);
    corrupt_value[base_location.offset] ^= 0xff;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = committed.entry.path, .data = corrupt_value });
    try std.testing.expectError(error.BadPostingSegmentChecksum, readSegmentPointValueAlloc(alloc, std.testing.io, tmp.dir, entry, 7, .base));

    var corrupt_delta_value = try alloc.dupe(u8, original);
    defer alloc.free(corrupt_delta_value);
    corrupt_delta_value[delta_entry.offset] ^= 0xff;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = committed.entry.path, .data = corrupt_delta_value });
    try std.testing.expectError(error.BadPostingSegmentChecksum, readSegmentDeltaRecordsAlloc(alloc, std.testing.io, tmp.dir, entry, 7, null));
}

pub fn testRejectsDuplicateLogicalEntries() !void {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBase(1, "a");
    try writer.appendBase(1, "b");
    try std.testing.expectError(error.DuplicatePostingSegmentEntry, writer.build());
}

pub fn testValidatesFooterAndVersion() !void {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBase(1, "a");
    const bytes = try writer.build();
    defer alloc.free(bytes);

    var bad_magic = try alloc.dupe(u8, bytes);
    defer alloc.free(bad_magic);
    bad_magic[bad_magic.len - 1] = 'x';
    try std.testing.expectError(error.BadPostingSegmentMagic, Reader.init(bad_magic));

    var bad_version = try alloc.dupe(u8, bytes);
    defer alloc.free(bad_version);
    bad_version[bad_version.len - magic.len - 1] = 2;
    try std.testing.expectError(error.UnsupportedPostingSegmentVersion, Reader.init(bad_version));

    var bad_payload = try alloc.dupe(u8, bytes);
    defer alloc.free(bad_payload);
    bad_payload[0] ^= 0xff;
    try std.testing.expectError(error.BadPostingSegmentChecksum, Reader.init(bad_payload));

    var bad_footer_index_offset = try alloc.dupe(u8, bytes);
    defer alloc.free(bad_footer_index_offset);
    bad_footer_index_offset[bad_footer_index_offset.len - footer_size] ^= 0xff;
    try std.testing.expectError(error.BadPostingSegmentChecksum, Reader.init(bad_footer_index_offset));
}

pub fn testCatalogLooksUpNewestPointRecordsAndMergedDeltas() !void {
    const alloc = std.testing.allocator;
    const old_base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    defer alloc.free(old_base);
    const new_base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 2,
        .members = &.{ 10, 20, 30 },
    });
    defer alloc.free(new_base);
    const delta_10_sequence = (@as(u64, 10) << 32) | 1;
    const delta_8_sequence = (@as(u64, 8) << 32) | 1;
    const delta_10 = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = delta_10_sequence, .op = .insert, .vector_id = 40 },
    });
    defer alloc.free(delta_10);
    const delta_8 = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = delta_8_sequence, .op = .tombstone, .vector_id = 20 },
    });
    defer alloc.free(delta_8);

    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendBase(7, old_base);
    try writer_1.appendDelta(7, delta_10_sequence, delta_10);
    const segment_1 = try writer_1.build();
    defer alloc.free(segment_1);

    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendBase(7, new_base);
    try writer_2.appendDelta(7, delta_8_sequence, delta_8);
    const segment_2 = try writer_2.build();
    defer alloc.free(segment_2);

    const reader_1 = try Reader.init(segment_1);
    const reader_2 = try Reader.init(segment_2);
    const meta_1 = try reader_1.metadata(1);
    const meta_2 = try reader_2.metadata(2);
    try std.testing.expect(meta_1.mayContainPosting(7));
    try std.testing.expectEqual(@as(usize, 2), meta_1.entry_count);
    try std.testing.expectEqual(delta_10_sequence, meta_1.min_delta_sequence);
    try std.testing.expectEqual(delta_10_sequence, meta_1.max_delta_sequence);
    try std.testing.expectEqual(segment_1.len, meta_1.byte_len);

    const blobs = [_]SegmentBlob{
        .{ .meta = meta_1, .data = segment_1 },
        .{ .meta = meta_2, .data = segment_2 },
    };
    const catalog = Catalog{ .segments = blobs[0..] };
    try std.testing.expectEqualSlices(u8, new_base, (try catalog.getBase(7)).?);
    try std.testing.expect(try catalog.getBase(8) == null);

    const deltas = try catalog.collectDeltas(alloc, 7);
    defer alloc.free(deltas);
    try std.testing.expectEqual(@as(usize, 2), deltas.len);
    try std.testing.expectEqual(delta_8_sequence, deltas[0].sequence);
    try std.testing.expectEqualSlices(u8, delta_8, deltas[0].value);
    try std.testing.expectEqual(delta_10_sequence, deltas[1].sequence);
    try std.testing.expectEqualSlices(u8, delta_10, deltas[1].value);

    const later_deltas = try catalog.collectDeltasAfterGeneration(alloc, 7, 8);
    defer alloc.free(later_deltas);
    try std.testing.expectEqual(@as(usize, 1), later_deltas.len);
    try std.testing.expectEqual(delta_10_sequence, later_deltas[0].sequence);
    try std.testing.expectEqualSlices(u8, delta_10, later_deltas[0].value);
}

pub fn testSnapshotLoadsTypedPostingValues() !void {
    const alloc = std.testing.allocator;
    const base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 3,
        .members = &.{ 10, 20, 30 },
    });
    defer alloc.free(base);
    const centroid = try posting.CentroidDirectoryFormat.encode(alloc, .{
        .posting_id = 7,
        .generation = 3,
        .mutation_version = 11,
        .payload_version = 9,
        .flags = posting.CentroidDirectoryFormat.dirty_flag,
        .parent = 1,
        .level = 0,
        .member_count = 3,
        .bounds_radius = 2.5,
        .centroid = &.{ 1.0, 2.0, 3.0 },
    });
    defer alloc.free(centroid);
    const delta_20_sequence = (@as(u64, 20) << 32) | 1;
    const delta_2_sequence = (@as(u64, 2) << 32) | 1;
    const delta_10_sequence = (@as(u64, 10) << 32) | 1;
    const delta_12_sequence = (@as(u64, 12) << 32) | 1;
    const delta_20 = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = delta_20_sequence, .op = .insert, .vector_id = 40 },
    });
    defer alloc.free(delta_20);
    const delta_2 = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = delta_2_sequence, .op = .insert, .vector_id = 15 },
    });
    defer alloc.free(delta_2);
    const delta_10 = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = delta_10_sequence, .op = .tombstone, .vector_id = 20 },
        .{ .sequence = delta_12_sequence, .op = .replace, .vector_id = 30 },
    });
    defer alloc.free(delta_10);

    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendBase(7, base);
    try writer_1.appendCentroidDirectory(7, centroid);
    try writer_1.appendDelta(7, delta_20_sequence, delta_20);
    try writer_1.appendDelta(7, delta_2_sequence, delta_2);
    const segment_1 = try writer_1.build();
    defer alloc.free(segment_1);

    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendDelta(7, delta_10_sequence, delta_10);
    const segment_2 = try writer_2.build();
    defer alloc.free(segment_2);

    const reader_1 = try Reader.init(segment_1);
    const reader_2 = try Reader.init(segment_2);
    const meta_1 = try reader_1.metadata(1);
    const meta_2 = try reader_2.metadata(2);
    const blobs = [_]SegmentBlob{
        .{ .meta = meta_1, .data = segment_1 },
        .{ .meta = meta_2, .data = segment_2 },
    };
    const snapshot = Snapshot{ .catalog = .{ .segments = blobs[0..] } };

    const header = (try snapshot.loadBaseHeader(7)).?;
    try std.testing.expectEqual(@as(PostingId, 7), header.posting_id);
    try std.testing.expectEqual(@as(u64, 3), header.generation);
    try std.testing.expectEqual(@as(usize, 3), header.member_count);
    try std.testing.expect(try snapshot.loadBaseHeader(8) == null);

    var loaded_base = (try snapshot.loadBase(alloc, 7)).?;
    defer loaded_base.deinit(alloc);
    try std.testing.expectEqual(@as(PostingId, 7), loaded_base.posting_id);
    try std.testing.expectEqual(@as(u64, 3), loaded_base.generation);
    try std.testing.expectEqualSlices(posting.VectorId, &.{ 10, 20, 30 }, loaded_base.members);
    try std.testing.expect(try snapshot.loadBase(alloc, 8) == null);

    var loaded_centroid = (try snapshot.loadCentroidDirectoryRecord(alloc, 7)).?;
    defer loaded_centroid.deinit(alloc);
    try std.testing.expectEqual(@as(PostingId, 7), loaded_centroid.posting_id);
    try std.testing.expectEqual(@as(u64, 3), loaded_centroid.generation);
    try std.testing.expectEqual(@as(u64, 11), loaded_centroid.mutation_version);
    try std.testing.expectEqual(@as(u64, 9), loaded_centroid.payload_version);
    try std.testing.expectEqual(posting.CentroidDirectoryFormat.dirty_flag, loaded_centroid.flags);
    try std.testing.expectEqual(@as(PostingId, 1), loaded_centroid.parent);
    try std.testing.expectEqual(@as(u16, 0), loaded_centroid.level);
    try std.testing.expectEqual(@as(u64, 3), loaded_centroid.member_count);
    try std.testing.expectEqual(@as(f32, 2.5), loaded_centroid.bounds_radius);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0, 3.0 }, loaded_centroid.centroid);
    try std.testing.expect(try snapshot.loadCentroidDirectoryRecord(alloc, 8) == null);

    const records = try snapshot.loadDeltaTail(alloc, 7);
    defer alloc.free(records);
    try std.testing.expectEqual(@as(usize, 4), records.len);
    try std.testing.expectEqual(delta_2_sequence, records[0].sequence);
    try std.testing.expectEqual(posting.PostingDeltaOp.insert, records[0].op);
    try std.testing.expectEqual(@as(posting.VectorId, 15), records[0].vector_id);
    try std.testing.expectEqual(delta_10_sequence, records[1].sequence);
    try std.testing.expectEqual(posting.PostingDeltaOp.tombstone, records[1].op);
    try std.testing.expectEqual(@as(posting.VectorId, 20), records[1].vector_id);
    try std.testing.expectEqual(delta_12_sequence, records[2].sequence);
    try std.testing.expectEqual(posting.PostingDeltaOp.replace, records[2].op);
    try std.testing.expectEqual(@as(posting.VectorId, 30), records[2].vector_id);
    try std.testing.expectEqual(delta_20_sequence, records[3].sequence);
    try std.testing.expectEqual(posting.PostingDeltaOp.insert, records[3].op);
    try std.testing.expectEqual(@as(posting.VectorId, 40), records[3].vector_id);

    const current_records = try snapshot.loadDeltaTailAfterGeneration(alloc, 7, 3);
    defer alloc.free(current_records);
    try std.testing.expectEqual(@as(usize, 3), current_records.len);
    try std.testing.expectEqual(delta_10_sequence, current_records[0].sequence);
    try std.testing.expectEqual(delta_12_sequence, current_records[1].sequence);
    try std.testing.expectEqual(delta_20_sequence, current_records[2].sequence);

    const later_records = try snapshot.loadDeltaTailAfterGeneration(alloc, 7, 12);
    defer alloc.free(later_records);
    try std.testing.expectEqual(@as(usize, 1), later_records.len);
    try std.testing.expectEqual(delta_20_sequence, later_records[0].sequence);

    const missing_records = try snapshot.loadDeltaTail(alloc, 8);
    defer alloc.free(missing_records);
    try std.testing.expectEqual(@as(usize, 0), missing_records.len);
}

pub fn testManifestCodecRoundTripsSegmentMetadata() !void {
    const alloc = std.testing.allocator;
    const entries = [_]ManifestEntry{
        .{
            .meta = .{
                .segment_id = 1,
                .min_posting_id = 7,
                .max_posting_id = 9,
                .min_delta_sequence = 10,
                .max_delta_sequence = 20,
                .byte_len = 4096,
                .entry_count = 12,
                .index_offset = 3200,
                .index_checksum = 0x1234abcd,
            },
            .path = "postings/000001.afps",
        },
        .{
            .meta = .{
                .segment_id = 2,
                .min_posting_id = 10,
                .max_posting_id = 12,
                .byte_len = 2048,
                .entry_count = 3,
                .index_offset = 1800,
                .index_checksum = 0x4567cdef,
            },
            .path = "postings/000002.afps",
        },
    };
    const encoded = try encodeManifestAlloc(alloc, .{
        .next_segment_id = 3,
        .segments = entries[0..],
    });
    defer alloc.free(encoded);

    var decoded = try decodeManifestAlloc(alloc, encoded);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 3), decoded.next_segment_id);
    try std.testing.expectEqual(@as(usize, 2), decoded.segments.len);
    for (entries, decoded.segments) |expected, actual| {
        try std.testing.expectEqual(expected.meta.segment_id, actual.meta.segment_id);
        try std.testing.expectEqual(expected.meta.min_posting_id, actual.meta.min_posting_id);
        try std.testing.expectEqual(expected.meta.max_posting_id, actual.meta.max_posting_id);
        try std.testing.expectEqual(expected.meta.min_delta_sequence, actual.meta.min_delta_sequence);
        try std.testing.expectEqual(expected.meta.max_delta_sequence, actual.meta.max_delta_sequence);
        try std.testing.expectEqual(expected.meta.byte_len, actual.meta.byte_len);
        try std.testing.expectEqual(expected.meta.entry_count, actual.meta.entry_count);
        try std.testing.expectEqual(expected.meta.index_offset, actual.meta.index_offset);
        try std.testing.expectEqual(expected.meta.index_checksum, actual.meta.index_checksum);
        try std.testing.expectEqualStrings(expected.path, actual.path);
    }
}

pub fn testManifestCodecRejectsInvalidData() !void {
    const alloc = std.testing.allocator;
    const entries = [_]ManifestEntry{
        .{
            .meta = .{
                .segment_id = 2,
                .min_posting_id = 7,
                .max_posting_id = 7,
                .byte_len = 128,
                .entry_count = 1,
            },
            .path = "postings/000002.afps",
        },
        .{
            .meta = .{
                .segment_id = 1,
                .min_posting_id = 8,
                .max_posting_id = 8,
                .byte_len = 128,
                .entry_count = 1,
            },
            .path = "postings/000001.afps",
        },
    };
    try std.testing.expectError(error.InvalidPostingSegmentManifest, encodeManifestAlloc(alloc, .{
        .next_segment_id = 3,
        .segments = entries[0..],
    }));

    const valid_entries = [_]ManifestEntry{
        .{
            .meta = .{
                .segment_id = 1,
                .min_posting_id = 7,
                .max_posting_id = 7,
                .byte_len = 128,
                .entry_count = 1,
            },
            .path = "postings/000001.afps",
        },
    };
    const encoded = try encodeManifestAlloc(alloc, .{
        .next_segment_id = 2,
        .segments = valid_entries[0..],
    });
    defer alloc.free(encoded);

    var bad_magic = try alloc.dupe(u8, encoded);
    defer alloc.free(bad_magic);
    bad_magic[0] = 'x';
    try std.testing.expectError(error.BadPostingSegmentManifestMagic, decodeManifestAlloc(alloc, bad_magic));

    var bad_version = try alloc.dupe(u8, encoded);
    defer alloc.free(bad_version);
    bad_version[5] = 2;
    try std.testing.expectError(error.UnsupportedPostingSegmentManifestVersion, decodeManifestAlloc(alloc, bad_version));

    var bad_entry = try alloc.dupe(u8, encoded);
    defer alloc.free(bad_entry);
    bad_entry[18] ^= 0xff;
    try std.testing.expectError(error.BadPostingSegmentManifestChecksum, decodeManifestAlloc(alloc, bad_entry));

    try std.testing.expectError(error.BadPostingSegmentManifestChecksum, decodeManifestAlloc(alloc, encoded[0 .. encoded.len - 1]));
    try std.testing.expectError(error.CorruptedPostingSegmentManifest, decodeManifestAlloc(alloc, encoded[0..manifest_header_size]));
}

pub fn testManifestReplacementEncodesCompactionCommit() !void {
    const alloc = std.testing.allocator;
    const entries = [_]ManifestEntry{
        .{
            .meta = .{
                .segment_id = 1,
                .min_posting_id = 1,
                .max_posting_id = 2,
                .byte_len = 128,
                .entry_count = 2,
            },
            .path = "postings/0000000000000001.afps",
        },
        .{
            .meta = .{
                .segment_id = 2,
                .min_posting_id = 3,
                .max_posting_id = 4,
                .byte_len = 256,
                .entry_count = 3,
            },
            .path = "postings/0000000000000002.afps",
        },
        .{
            .meta = .{
                .segment_id = 3,
                .min_posting_id = 5,
                .max_posting_id = 5,
                .byte_len = 512,
                .entry_count = 1,
            },
            .path = "postings/0000000000000003.afps",
        },
    };
    const replacement = [_]ManifestEntry{.{
        .meta = .{
            .segment_id = 4,
            .min_posting_id = 1,
            .max_posting_id = 4,
            .min_delta_sequence = (@as(u64, 6) << 32) | 1,
            .max_delta_sequence = (@as(u64, 7) << 32) | 2,
            .byte_len = 1024,
            .entry_count = 4,
        },
        .path = "postings/0000000000000004.afps",
    }};
    var result = try replaceManifestSegmentsWithStatsAlloc(alloc, .{
        .next_segment_id = 4,
        .segments = entries[0..],
    }, &.{ 1, 2 }, replacement[0..]);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), result.stats.input_segments);
    try std.testing.expectEqual(@as(usize, 2), result.stats.removed_segments);
    try std.testing.expectEqual(@as(usize, 1), result.stats.added_segments);
    try std.testing.expectEqual(@as(usize, 2), result.stats.output_segments);
    try std.testing.expectEqual(@as(u64, 5), result.stats.next_segment_id);

    var decoded = try decodeManifestAlloc(alloc, result.encoded);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 5), decoded.next_segment_id);
    try std.testing.expectEqual(@as(usize, 2), decoded.segments.len);
    try std.testing.expectEqual(@as(u64, 3), decoded.segments[0].meta.segment_id);
    try std.testing.expectEqualStrings("postings/0000000000000003.afps", decoded.segments[0].path);
    try std.testing.expectEqual(@as(u64, 4), decoded.segments[1].meta.segment_id);
    try std.testing.expectEqual(replacement[0].meta.min_posting_id, decoded.segments[1].meta.min_posting_id);
    try std.testing.expectEqual(replacement[0].meta.max_posting_id, decoded.segments[1].meta.max_posting_id);
    try std.testing.expectEqualStrings(replacement[0].path, decoded.segments[1].path);

    try std.testing.expectError(error.PostingSegmentManifestReplacementMissingSegment, replaceManifestSegmentsWithStatsAlloc(alloc, .{
        .next_segment_id = 4,
        .segments = entries[0..],
    }, &.{99}, replacement[0..]));
    try std.testing.expectError(error.DuplicatePostingSegmentId, replaceManifestSegmentsWithStatsAlloc(alloc, .{
        .next_segment_id = 4,
        .segments = entries[0..],
    }, &.{ 1, 1 }, replacement[0..]));

    const colliding = [_]ManifestEntry{.{
        .meta = .{
            .segment_id = 3,
            .min_posting_id = 6,
            .max_posting_id = 6,
            .byte_len = 64,
            .entry_count = 1,
        },
        .path = "postings/0000000000000003-copy.afps",
    }};
    try std.testing.expectError(error.InvalidPostingSegmentManifest, replaceManifestSegmentsWithStatsAlloc(alloc, .{
        .next_segment_id = 4,
        .segments = entries[0..],
    }, &.{1}, colliding[0..]));
}

pub fn testDirectoryCompactionPlannerSelectsWithinBudgets() !void {
    const alloc = std.testing.allocator;
    const entries = [_]ManifestEntry{
        .{
            .meta = .{
                .segment_id = 1,
                .min_posting_id = 1,
                .max_posting_id = 2,
                .byte_len = 100,
                .entry_count = 2,
            },
            .path = "postings/0000000000000001.afps",
        },
        .{
            .meta = .{
                .segment_id = 2,
                .min_posting_id = 3,
                .max_posting_id = 4,
                .byte_len = 150,
                .entry_count = 3,
            },
            .path = "postings/0000000000000002.afps",
        },
        .{
            .meta = .{
                .segment_id = 3,
                .min_posting_id = 5,
                .max_posting_id = 6,
                .byte_len = 250,
                .entry_count = 4,
            },
            .path = "postings/0000000000000003.afps",
        },
    };
    const manifest = Manifest{
        .next_segment_id = 4,
        .segments = entries[0..],
    };

    var by_count = try planDirectoryCompactionAlloc(alloc, manifest, .{
        .max_input_segments = 2,
    });
    defer by_count.deinit(alloc);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2 }, by_count.segment_ids);
    try std.testing.expectEqual(@as(usize, 3), by_count.stats.manifest_segments);
    try std.testing.expectEqual(@as(usize, 2), by_count.stats.selected_segments);
    try std.testing.expectEqual(@as(usize, 250), by_count.stats.selected_bytes);
    try std.testing.expectEqual(@as(usize, 5), by_count.stats.selected_entries);
    try std.testing.expect(by_count.stats.stopped_on_segment_limit);
    try std.testing.expect(!by_count.stats.insufficient_segments);

    var by_bytes = try planDirectoryCompactionAlloc(alloc, manifest, .{
        .max_input_bytes = 260,
    });
    defer by_bytes.deinit(alloc);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2 }, by_bytes.segment_ids);
    try std.testing.expectEqual(@as(usize, 250), by_bytes.stats.selected_bytes);
    try std.testing.expect(by_bytes.stats.stopped_on_byte_limit);
    try std.testing.expect(!by_bytes.stats.insufficient_segments);

    var too_small = try planDirectoryCompactionAlloc(alloc, manifest, .{
        .max_input_bytes = 200,
    });
    defer too_small.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), too_small.segment_ids.len);
    try std.testing.expect(too_small.stats.stopped_on_byte_limit);
    try std.testing.expect(too_small.stats.insufficient_segments);
}

pub fn testManifestSummaryAggregatesMetadataWithoutSegmentReads() !void {
    const entries = [_]ManifestEntry{
        .{
            .meta = .{
                .segment_id = 1,
                .min_posting_id = 10,
                .max_posting_id = 20,
                .min_delta_sequence = 30,
                .max_delta_sequence = 40,
                .byte_len = 100,
                .entry_count = 2,
            },
            .path = "postings/0000000000000001.afps",
        },
        .{
            .meta = .{
                .segment_id = 3,
                .min_posting_id = 5,
                .max_posting_id = 25,
                .byte_len = 250,
                .entry_count = 4,
            },
            .path = "postings/0000000000000003.afps",
        },
        .{
            .meta = .{
                .segment_id = 5,
                .min_posting_id = 30,
                .max_posting_id = 35,
                .min_delta_sequence = 20,
                .max_delta_sequence = 80,
                .byte_len = 150,
                .entry_count = 3,
            },
            .path = "postings/0000000000000005.afps",
        },
    };

    const stats = try summarizeManifest(.{
        .next_segment_id = 6,
        .segments = entries[0..],
    });
    try std.testing.expectEqual(@as(usize, 3), stats.segments);
    try std.testing.expectEqual(@as(usize, 500), stats.bytes);
    try std.testing.expectEqual(@as(usize, 9), stats.entries);
    try std.testing.expectEqual(@as(u64, 1), stats.min_segment_id);
    try std.testing.expectEqual(@as(u64, 5), stats.max_segment_id);
    try std.testing.expectEqual(@as(u64, 6), stats.next_segment_id);
    try std.testing.expectEqual(@as(PostingId, 5), stats.min_posting_id);
    try std.testing.expectEqual(@as(PostingId, 35), stats.max_posting_id);
    try std.testing.expectEqual(@as(u64, 20), stats.min_delta_sequence);
    try std.testing.expectEqual(@as(u64, 80), stats.max_delta_sequence);

    const empty_segments: [0]ManifestEntry = .{};
    const empty = try summarizeManifest(.{
        .next_segment_id = 10,
        .segments = empty_segments[0..],
    });
    try std.testing.expectEqual(@as(usize, 0), empty.segments);
    try std.testing.expectEqual(@as(u64, 10), empty.next_segment_id);
    try std.testing.expectEqual(@as(usize, 0), empty.bytes);
}

const TestSegmentFile = struct {
    path: []const u8,
    data: []const u8,
};

const TestSegmentLoader = struct {
    files: []const TestSegmentFile,

    fn read(self: *const TestSegmentLoader, alloc: Allocator, path: []const u8, byte_limit: usize) ![]u8 {
        for (self.files) |file| {
            if (!std.mem.eql(u8, file.path, path)) continue;
            if (file.data.len > byte_limit) return error.PostingSegmentTooLarge;
            return try alloc.dupe(u8, file.data);
        }
        return error.FileNotFound;
    }
};

pub fn testOpenStoreValidatesManifestBackedSegments() !void {
    const alloc = std.testing.allocator;
    const base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 3,
        .members = &.{ 10, 20, 30 },
    });
    defer alloc.free(base);
    const delta = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = 20, .op = .insert, .vector_id = 40 },
    });
    defer alloc.free(delta);

    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBase(7, base);
    try writer.appendDelta(7, 20, delta);
    const segment = try writer.build();
    defer alloc.free(segment);

    const reader = try Reader.init(segment);
    const meta = try reader.metadata(1);
    const entries = [_]ManifestEntry{.{
        .meta = meta,
        .path = "postings/000001.afps",
    }};
    const manifest_data = try encodeManifestAlloc(alloc, .{
        .next_segment_id = 2,
        .segments = entries[0..],
    });
    defer alloc.free(manifest_data);

    const files = [_]TestSegmentFile{.{
        .path = "postings/000001.afps",
        .data = segment,
    }};
    const loader = TestSegmentLoader{ .files = files[0..] };
    var store = try openStoreAlloc(alloc, manifest_data, &loader, TestSegmentLoader.read);
    defer store.deinit(alloc);

    try std.testing.expectEqual(@as(u64, 2), store.manifest.next_segment_id);
    try std.testing.expectEqual(@as(usize, 1), store.segments.len);
    const snapshot = store.snapshot();
    const header = (try snapshot.loadBaseHeader(7)).?;
    try std.testing.expectEqual(@as(PostingId, 7), header.posting_id);
    try std.testing.expectEqual(@as(u64, 3), header.generation);
    try std.testing.expectEqual(@as(usize, 3), header.member_count);

    const records = try snapshot.loadDeltaTail(alloc, 7);
    defer alloc.free(records);
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqual(@as(u64, 20), records[0].sequence);
    try std.testing.expectEqual(posting.PostingDeltaOp.insert, records[0].op);
    try std.testing.expectEqual(@as(posting.VectorId, 40), records[0].vector_id);

    var stale_entries = [_]ManifestEntry{.{
        .meta = meta,
        .path = "postings/000001.afps",
    }};
    stale_entries[0].meta.max_posting_id = 8;
    const stale_manifest_data = try encodeManifestAlloc(alloc, .{
        .next_segment_id = 2,
        .segments = stale_entries[0..],
    });
    defer alloc.free(stale_manifest_data);
    try std.testing.expectError(error.InvalidPostingSegment, openStoreAlloc(alloc, stale_manifest_data, &loader, TestSegmentLoader.read));
}

pub fn testBuildSegmentProducesManifestReadyMetadata() !void {
    const alloc = std.testing.allocator;
    const base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 9,
        .generation = 4,
        .members = &.{ 100, 200 },
    });
    defer alloc.free(base);

    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBase(9, base);

    var built = try writer.buildSegment(42);
    defer built.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 42), built.meta.segment_id);
    try std.testing.expectEqual(@as(PostingId, 9), built.meta.min_posting_id);
    try std.testing.expectEqual(@as(PostingId, 9), built.meta.max_posting_id);
    try std.testing.expectEqual(built.data.len, built.meta.byte_len);
    try std.testing.expectEqual(@as(usize, 1), built.meta.entry_count);

    const path = try segmentPathAlloc(alloc, built.meta.segment_id);
    defer alloc.free(path);
    try std.testing.expectEqualStrings("postings/000000000000002a.afps", path);

    const entries = [_]ManifestEntry{.{
        .meta = built.meta,
        .path = path,
    }};
    const manifest_data = try encodeManifestAlloc(alloc, .{
        .next_segment_id = 43,
        .segments = entries[0..],
    });
    defer alloc.free(manifest_data);

    const files = [_]TestSegmentFile{.{
        .path = path,
        .data = built.data,
    }};
    const loader = TestSegmentLoader{ .files = files[0..] };
    var store = try openStoreAlloc(alloc, manifest_data, &loader, TestSegmentLoader.read);
    defer store.deinit(alloc);
    const snapshot = store.snapshot();
    const header = (try snapshot.loadBaseHeader(9)).?;
    try std.testing.expectEqual(@as(u64, 4), header.generation);
    try std.testing.expectEqual(@as(usize, 2), header.member_count);
}

pub fn testDirectoryStoreRoundTripsSegmentFiles() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 11,
        .generation = 6,
        .members = &.{ 100, 200, 300 },
    });
    defer alloc.free(base);
    const delta_sequence = (@as(u64, 7) << 32) | 1;
    const delta = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = delta_sequence, .op = .insert, .vector_id = 400 },
    });
    defer alloc.free(delta);

    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBase(11, base);
    try writer.appendDelta(11, delta_sequence, delta);
    var built = try writer.buildSegment(12);
    defer built.deinit(alloc);

    const written = try writeSegmentFileAlloc(alloc, std.testing.io, tmp.dir, built);
    defer alloc.free(written.path);
    try std.testing.expectEqual(built.meta.segment_id, written.meta.segment_id);
    try std.testing.expectEqualStrings("postings/000000000000000c.afps", written.path);

    const manifest_entry = ManifestEntry{
        .meta = written.meta,
        .path = written.path,
    };
    const manifest_data = try encodeManifestAlloc(alloc, .{
        .next_segment_id = 13,
        .segments = &.{manifest_entry},
    });
    defer alloc.free(manifest_data);
    try writeManifestFileAlloc(alloc, std.testing.io, tmp.dir, default_manifest_path, manifest_data);

    var store = try openStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer store.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 13), store.manifest.next_segment_id);
    try std.testing.expectEqual(@as(usize, 1), store.segments.len);

    const snapshot = store.snapshot();
    const header = (try snapshot.loadBaseHeader(11)).?;
    try std.testing.expectEqual(@as(u64, 6), header.generation);
    try std.testing.expectEqual(@as(usize, 3), header.member_count);
    const records = try snapshot.loadDeltaTailAfterGeneration(alloc, 11, 6);
    defer alloc.free(records);
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqual(delta_sequence, records[0].sequence);
    try std.testing.expectEqual(@as(posting.VectorId, 400), records[0].vector_id);

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = written.path,
        .data = "not a segment",
    });
    try std.testing.expectError(error.CorruptedPostingSegment, openStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{}));
}

pub fn testDirectoryCommitAppendsManifestSegments() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_7 = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    defer alloc.free(base_7);

    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendBase(7, base_7);

    var committed_1 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_1, .{});
    defer committed_1.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), committed_1.entry.meta.segment_id);
    try std.testing.expectEqual(@as(u64, 2), committed_1.stats.next_segment_id);
    try std.testing.expectEqual(@as(usize, 0), committed_1.stats.input_segments);
    try std.testing.expectEqual(@as(usize, 1), committed_1.stats.output_segments);

    const delta_sequence = (@as(u64, 2) << 32) | 1;
    const delta_7 = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = delta_sequence, .op = .insert, .vector_id = 30 },
    });
    defer alloc.free(delta_7);

    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendDelta(7, delta_sequence, delta_7);

    var committed_2 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_2, .{});
    defer committed_2.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), committed_2.entry.meta.segment_id);
    try std.testing.expectEqual(@as(u64, 3), committed_2.stats.next_segment_id);
    try std.testing.expectEqual(@as(usize, 1), committed_2.stats.input_segments);
    try std.testing.expectEqual(@as(usize, 2), committed_2.stats.output_segments);

    var store = try openStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer store.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 3), store.manifest.next_segment_id);
    try std.testing.expectEqual(@as(usize, 2), store.segments.len);
    try std.testing.expectEqual(@as(u64, 1), store.segments[0].meta.segment_id);
    try std.testing.expectEqual(@as(u64, 2), store.segments[1].meta.segment_id);

    const snapshot = store.snapshot();
    const header = (try snapshot.loadBaseHeader(7)).?;
    try std.testing.expectEqual(@as(u64, 1), header.generation);
    try std.testing.expectEqual(@as(usize, 2), header.member_count);
    const records = try snapshot.loadDeltaTailAfterGeneration(alloc, 7, 1);
    defer alloc.free(records);
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqual(delta_sequence, records[0].sequence);
    try std.testing.expectEqual(@as(posting.VectorId, 30), records[0].vector_id);
}

pub fn testDirectoryBatchWriterFlushesBoundedSegments() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    defer alloc.free(base);
    const stale_delta_sequence = (@as(u64, 2) << 32) | 1;
    const live_delta_sequence = (@as(u64, 3) << 32) | 1;
    const centroid = try posting.CentroidDirectoryFormat.encode(alloc, .{
        .posting_id = 7,
        .generation = 1,
        .mutation_version = 2,
        .payload_version = 3,
        .flags = posting.CentroidDirectoryFormat.dirty_flag,
        .parent = 1,
        .level = 0,
        .member_count = 2,
        .bounds_radius = 1.25,
        .centroid = &.{ 1.0, 2.0 },
    });
    defer alloc.free(centroid);

    var batcher = DirectoryBatchWriter.init(alloc, std.testing.io, tmp.dir, .{
        .max_pending_entries = 2,
        .max_pending_value_bytes = 0,
    });
    defer batcher.deinit();

    try batcher.appendBase(7, base);
    try std.testing.expectEqual(@as(usize, 1), batcher.pendingEntries());
    try batcher.appendDeltaRecord(7, .{ .sequence = stale_delta_sequence, .op = .insert, .vector_id = 30 });
    try std.testing.expectEqual(@as(usize, 2), batcher.pendingEntries());
    try std.testing.expectEqual(@as(usize, 1), batcher.pendingDeltaRecords());
    try batcher.appendDeltaRecord(7, .{ .sequence = live_delta_sequence, .op = .tombstone, .vector_id = 20 });
    try std.testing.expectEqual(@as(usize, 2), batcher.pendingEntries());
    try std.testing.expectEqual(@as(usize, 2), batcher.pendingDeltaRecords());
    try batcher.appendPostingDeltaRecords(7, &.{
        .{ .sequence = (@as(u64, 4) << 32) | 1, .op = .insert, .vector_id = 40 },
        .{ .sequence = (@as(u64, 5) << 32) | 1, .op = .tombstone, .vector_id = 30 },
    });
    try std.testing.expectEqual(@as(usize, 2), batcher.pendingEntries());
    try std.testing.expectEqual(@as(usize, 4), batcher.pendingDeltaRecords());

    try batcher.appendCentroidDirectory(7, centroid);
    try std.testing.expectEqual(@as(usize, 1), batcher.stats.flushed_segments);
    try std.testing.expectEqual(@as(usize, 2), batcher.stats.committed_entries);
    try std.testing.expectEqual(@as(usize, 4), batcher.stats.committed_delta_records);
    try std.testing.expectEqual(@as(usize, 1), batcher.pendingEntries());
    const final_flush = (try batcher.flush()).?;
    try std.testing.expectEqual(@as(u64, 2), final_flush.segment_id);
    try std.testing.expectEqual(@as(usize, 2), batcher.stats.flushed_segments);
    try std.testing.expectEqual(@as(usize, 3), batcher.stats.committed_entries);
    try std.testing.expect(batcher.stats.committed_value_bytes >= base.len + centroid.len);
    try std.testing.expectEqual(@as(usize, 0), batcher.pendingEntries());
    try std.testing.expectEqual(@as(usize, 0), batcher.pendingDeltaRecords());
    try std.testing.expect(try batcher.flush() == null);

    var store = try openStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer store.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), store.segments.len);
    try std.testing.expectEqual(@as(u64, 3), store.manifest.next_segment_id);

    const snapshot = store.snapshot();
    const header = (try snapshot.loadBaseHeader(7)).?;
    try std.testing.expectEqual(@as(u64, 1), header.generation);
    const records = try snapshot.loadDeltaTailAfterGeneration(alloc, 7, 1);
    defer alloc.free(records);
    try std.testing.expectEqual(@as(usize, 4), records.len);
    try std.testing.expectEqual(stale_delta_sequence, records[0].sequence);
    try std.testing.expectEqual(live_delta_sequence, records[1].sequence);
    try std.testing.expectEqual((@as(u64, 4) << 32) | 1, records[2].sequence);
    try std.testing.expectEqual((@as(u64, 5) << 32) | 1, records[3].sequence);
    var loaded_centroid = (try snapshot.loadCentroidDirectoryRecord(alloc, 7)).?;
    defer loaded_centroid.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), loaded_centroid.mutation_version);
    try std.testing.expectEqual(@as(u64, 3), loaded_centroid.payload_version);

    {
        var value_tmp = std.testing.tmpDir(.{});
        defer value_tmp.cleanup();

        const record_1 = posting.PostingDeltaRecord{
            .sequence = (@as(u64, 2) << 32) | 1,
            .op = .insert,
            .vector_id = 10,
        };
        const record_2 = posting.PostingDeltaRecord{
            .sequence = (@as(u64, 2) << 32) | 2,
            .op = .insert,
            .vector_id = 20,
        };
        const single_delta_value_bytes = try posting.PostingFormat.encodedDeltaTailSize(&.{record_1});
        var value_bounded = DirectoryBatchWriter.init(alloc, std.testing.io, value_tmp.dir, .{
            .max_pending_entries = 64,
            .max_pending_value_bytes = single_delta_value_bytes + 1,
            .max_pending_delta_records = 64,
        });
        defer value_bounded.deinit();

        try value_bounded.appendDeltaRecord(7, record_1);
        try std.testing.expectEqual(@as(usize, 0), value_bounded.stats.flushed_segments);
        try std.testing.expectEqual(@as(usize, 1), value_bounded.pendingEntries());
        try value_bounded.appendDeltaRecord(9, record_2);
        try std.testing.expectEqual(@as(usize, 1), value_bounded.stats.flushed_segments);
        try std.testing.expectEqual(@as(usize, 1), value_bounded.stats.committed_entries);
        try std.testing.expectEqual(@as(usize, 1), value_bounded.stats.committed_delta_records);
        try std.testing.expectEqual(single_delta_value_bytes, value_bounded.stats.committed_value_bytes);
        try std.testing.expectEqual(@as(usize, 1), value_bounded.pendingEntries());
        try std.testing.expectEqual(@as(usize, 1), value_bounded.pendingDeltaRecords());
    }
}

pub fn testDirectoryBatchWriterCoalescesPendingPointRecords() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const old_base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    defer alloc.free(old_base);
    const new_base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 2,
        .members = &.{ 10, 20, 30 },
    });
    defer alloc.free(new_base);
    const old_centroid = try posting.CentroidDirectoryFormat.encode(alloc, .{
        .posting_id = 7,
        .generation = 1,
        .mutation_version = 2,
        .payload_version = 3,
        .flags = 0,
        .parent = 1,
        .level = 0,
        .member_count = 2,
        .bounds_radius = 1.25,
        .centroid = &.{ 1.0, 2.0 },
    });
    defer alloc.free(old_centroid);
    const new_centroid = try posting.CentroidDirectoryFormat.encode(alloc, .{
        .posting_id = 7,
        .generation = 2,
        .mutation_version = 5,
        .payload_version = 8,
        .flags = posting.CentroidDirectoryFormat.dirty_flag,
        .parent = 1,
        .level = 0,
        .member_count = 3,
        .bounds_radius = 2.5,
        .centroid = &.{ 3.0, 4.0 },
    });
    defer alloc.free(new_centroid);

    var batcher = DirectoryBatchWriter.init(alloc, std.testing.io, tmp.dir, .{
        .max_pending_entries = 64,
        .max_pending_value_bytes = 0,
    });
    defer batcher.deinit();

    try batcher.appendBase(7, old_base);
    try batcher.appendCentroidDirectory(7, old_centroid);
    try batcher.appendBase(7, new_base);
    try batcher.appendCentroidDirectory(7, new_centroid);
    try std.testing.expectEqual(@as(usize, 2), batcher.pendingEntries());

    _ = (try batcher.flush()).?;
    try std.testing.expectEqual(@as(usize, 1), batcher.stats.flushed_segments);
    try std.testing.expectEqual(@as(usize, 2), batcher.stats.committed_entries);

    var store = try openStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer store.deinit(alloc);
    const snapshot = store.snapshot();
    const header = (try snapshot.loadBaseHeader(7)).?;
    try std.testing.expectEqual(@as(u64, 2), header.generation);
    try std.testing.expectEqual(@as(usize, 3), header.member_count);

    var loaded_centroid = (try snapshot.loadCentroidDirectoryRecord(alloc, 7)).?;
    defer loaded_centroid.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), loaded_centroid.generation);
    try std.testing.expectEqual(@as(u64, 5), loaded_centroid.mutation_version);
    try std.testing.expectEqual(@as(usize, 3), loaded_centroid.member_count);
}

pub fn testRuntimeDirectoryStoreExposesPendingReadOverlay() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try RuntimeDirectoryStore.openAlloc(alloc, std.testing.io, tmp.dir, .{
        .max_pending_entries = 64,
        .max_pending_value_bytes = 64 * 1024,
        .max_pending_delta_records = 64,
    });
    defer store.deinit();

    try store.appendPostingBase(.{
        .posting_id = 7,
        .generation = 3,
        .members = &.{ 10, 20 },
    });
    try store.appendCentroidDirectoryRecord(.{
        .posting_id = 7,
        .generation = 4,
        .mutation_version = 5,
        .payload_version = 6,
        .flags = posting.CentroidDirectoryFormat.dirty_flag,
        .parent = 1,
        .level = 0,
        .member_count = 2,
        .bounds_radius = 1.25,
        .centroid = &.{ 1.0, 2.0 },
    });
    try store.appendDeltaRecord(7, .{
        .sequence = (@as(u64, 4) << 32) | 1,
        .op = .insert,
        .vector_id = 30,
    });
    try store.appendDeltaRecord(7, .{
        .sequence = (@as(u64, 4) << 32) | 2,
        .op = .tombstone,
        .vector_id = 10,
    });

    try std.testing.expectEqual(@as(?posting.PostingBaseHeader, null), try store.snapshot().loadBaseHeader(alloc, 7));
    const header = (try store.pendingBaseHeader(7)).?;
    try std.testing.expectEqual(@as(u64, 3), header.generation);
    try std.testing.expectEqual(@as(usize, 2), header.member_count);

    var base = (try store.pendingBase(alloc, 7)).?;
    defer base.deinit(alloc);
    try std.testing.expectEqualSlices(posting.VectorId, &.{ 10, 20 }, base.members);

    var centroid = (try store.pendingCentroidDirectoryRecord(alloc, 7)).?;
    defer centroid.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 5), centroid.mutation_version);
    try std.testing.expectEqual(@as(usize, 2), centroid.member_count);

    const records = try store.pendingDeltaTailAlloc(alloc, 7, 3);
    defer alloc.free(records);
    try std.testing.expectEqual(@as(usize, 2), records.len);
    try std.testing.expectEqual(@as(posting.VectorId, 30), records[0].vector_id);
    try std.testing.expectEqual(posting.PostingDeltaOp.tombstone, records[1].op);

    const stats = try store.pendingDeltaTailStats(7, 3);
    try std.testing.expectEqual(@as(usize, 2), stats.records);
    try std.testing.expectEqual(@as(usize, 2), stats.records_after_generation);
    try std.testing.expectEqual(@as(usize, 1), stats.tombstones_after_generation);
    try std.testing.expectEqual(try posting.PostingFormat.encodedDeltaTailSize(records), stats.encoded_value_bytes);
    const records_with_stats = try store.pendingDeltaTailWithStatsAlloc(alloc, 7, 3);
    defer records_with_stats.deinit(alloc);
    try std.testing.expectEqualSlices(posting.PostingDeltaRecord, records, records_with_stats.records);
    try std.testing.expectEqual(stats.records, records_with_stats.stats.records);
    try std.testing.expectEqual(stats.records_after_generation, records_with_stats.stats.records_after_generation);
    try std.testing.expectEqual(stats.tombstones_after_generation, records_with_stats.stats.tombstones_after_generation);
    try std.testing.expectEqual(stats.encoded_value_bytes, records_with_stats.stats.encoded_value_bytes);
    var delta_scratch = posting.PostingStore.FoldScratch{};
    defer delta_scratch.deinit(alloc);
    const scratch_stats = try store.appendPendingDeltaTailIntoScratchWithStats(alloc, 7, 3, &delta_scratch);
    try std.testing.expectEqualSlices(posting.PostingDeltaRecord, records, delta_scratch.deltaRecords());
    try std.testing.expectEqual(stats.records, scratch_stats.records);
    try std.testing.expectEqual(stats.records_after_generation, scratch_stats.records_after_generation);
    try std.testing.expectEqual(stats.tombstones_after_generation, scratch_stats.tombstones_after_generation);
    try std.testing.expectEqual(stats.encoded_value_bytes, scratch_stats.encoded_value_bytes);
    var filtered_delta_scratch = posting.PostingStore.FoldScratch{};
    defer filtered_delta_scratch.deinit(alloc);
    const filtered_scratch_stats = try store.appendPendingDeltaTailIntoScratchWithStats(alloc, 7, 4, &filtered_delta_scratch);
    try std.testing.expectEqual(@as(usize, 2), filtered_scratch_stats.records);
    try std.testing.expectEqual(@as(usize, 0), filtered_scratch_stats.records_after_generation);
    try std.testing.expectEqual(@as(usize, 0), filtered_delta_scratch.deltaRecordCount());
    try std.testing.expectEqual(@as(usize, 0), filtered_delta_scratch.delta_records.len);
    const latest_record = (try store.pendingLatestDeltaRecordAfterGenerationForMember(7, 10, 3)).?;
    try std.testing.expectEqual((@as(u64, 4) << 32) | 2, latest_record.sequence);
    try std.testing.expectEqual(posting.PostingDeltaOp.tombstone, latest_record.op);
    try std.testing.expectEqual(posting.PostingDeltaOp.tombstone, (try store.pendingLatestDeltaOpAfterGenerationForMember(7, 10, 3)).?);
}

pub fn testRuntimeDirectoryStoreRefreshesSnapshots() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store = try RuntimeDirectoryStore.openAlloc(alloc, std.testing.io, tmp.dir, .{
        .max_pending_entries = 64,
        .max_pending_value_bytes = 64 * 1024,
        .max_pending_delta_records = 64,
        .compaction_plan = .{
            .min_input_segments = 2,
            .max_input_segments = 2,
        },
    });
    defer store.deinit();

    try std.testing.expectEqual(@as(usize, 0), store.snapshot().manifest.segments.len);
    try std.testing.expectEqual(@as(?posting.PostingBaseHeader, null), try store.snapshot().loadBaseHeader(alloc, 7));

    try store.appendPostingBase(.{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    try std.testing.expectEqual(@as(usize, 1), store.pendingEntries());
    try std.testing.expectEqual(@as(?posting.PostingBaseHeader, null), try store.snapshot().loadBaseHeader(alloc, 7));

    const first_flush = (try store.flush()).?;
    try std.testing.expectEqual(@as(u64, 1), first_flush.segment_id);
    try std.testing.expectEqual(@as(usize, 1), store.snapshot().manifest.segments.len);
    const base_header = (try store.snapshot().loadBaseHeader(alloc, 7)).?;
    try std.testing.expectEqual(@as(u64, 1), base_header.generation);
    try std.testing.expectEqual(@as(u32, 2), base_header.member_count);

    try store.appendDeltaRecord(7, .{
        .sequence = (@as(u64, 2) << 32) | 1,
        .op = .insert,
        .vector_id = 30,
    });
    try store.appendCentroidDirectoryRecord(.{
        .posting_id = 7,
        .generation = 2,
        .mutation_version = 3,
        .payload_version = 4,
        .flags = posting.CentroidDirectoryFormat.dirty_flag,
        .parent = 1,
        .level = 0,
        .member_count = 3,
        .bounds_radius = 5.0,
        .centroid = &.{ 1.0, 2.0 },
    });

    const second_flush = (try store.flush()).?;
    try std.testing.expectEqual(@as(u64, 2), second_flush.segment_id);
    try std.testing.expectEqual(@as(usize, 2), store.snapshot().manifest.segments.len);
    const members_before_maintenance = (try store.snapshot().materializeMembers(alloc, 7)).?;
    defer alloc.free(members_before_maintenance);
    try std.testing.expectEqualSlices(posting.VectorId, &.{ 10, 20, 30 }, members_before_maintenance);
    var centroid_before_maintenance = (try store.snapshot().loadCentroidDirectoryRecord(alloc, 7)).?;
    defer centroid_before_maintenance.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 3), centroid_before_maintenance.mutation_version);

    const maintenance = try store.maintain();
    try std.testing.expect(maintenance.compacted);
    try std.testing.expectEqual(@as(usize, 1), store.snapshot().manifest.segments.len);
    try std.testing.expectEqual(@as(usize, 2), maintenance.garbage.deleted_segment_files);

    const members_after_maintenance = (try store.snapshot().materializeMembers(alloc, 7)).?;
    defer alloc.free(members_after_maintenance);
    try std.testing.expectEqualSlices(posting.VectorId, &.{ 10, 20, 30 }, members_after_maintenance);
    try std.testing.expectEqual(@as(usize, 0), store.pendingEntries());
    try std.testing.expectEqual(@as(usize, 0), store.pendingDeltaRecords());
}

pub fn testRuntimeDirectoryStoreFlushUsesLoadedManifest() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const stale_entries = [_]ManifestEntry{.{
        .meta = .{
            .segment_id = 99,
            .min_posting_id = 1,
            .max_posting_id = 100,
            .byte_len = 123,
            .entry_count = 1,
        },
        .path = "postings/stale.afps",
    }};
    const stale_manifest_data = try encodeManifestAlloc(alloc, .{
        .next_segment_id = 100,
        .segments = stale_entries[0..],
    });
    defer alloc.free(stale_manifest_data);
    try writeManifestFileAlloc(alloc, std.testing.io, tmp.dir, default_manifest_path, stale_manifest_data);

    const committed_empty_manifest = try encodeManifestAlloc(alloc, .{
        .next_segment_id = 1,
        .segments = &.{},
    });
    defer alloc.free(committed_empty_manifest);

    var store = try RuntimeDirectoryStore.openAllocWithManifestData(alloc, std.testing.io, tmp.dir, .{
        .max_pending_entries = 64,
        .max_pending_value_bytes = 64 * 1024,
        .max_pending_delta_records = 64,
    }, committed_empty_manifest);
    defer store.deinit();

    try store.appendPostingBase(.{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    const flushed = (try store.flush()).?;
    try std.testing.expectEqual(@as(u64, 1), flushed.segment_id);
    try std.testing.expectEqual(@as(usize, 1), store.snapshot().manifest.segments.len);
    try std.testing.expectEqual(@as(u64, 2), store.snapshot().manifest.next_segment_id);

    const manifest_data = try tmp.dir.readFileAlloc(std.testing.io, default_manifest_path, alloc, .limited(1024 * 1024));
    defer alloc.free(manifest_data);
    var manifest = try decodeManifestAlloc(alloc, manifest_data);
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), manifest.segments.len);
    try std.testing.expectEqual(@as(u64, 1), manifest.segments[0].meta.segment_id);
}

pub fn testDirectoryCompactionReplacesManifestSegments() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_old = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    defer alloc.free(base_old);
    const stale_delta_sequence = (@as(u64, 2) << 32) | 1;
    const stale_delta = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = stale_delta_sequence, .op = .insert, .vector_id = 30 },
    });
    defer alloc.free(stale_delta);

    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendBase(7, base_old);
    try writer_1.appendDelta(7, stale_delta_sequence, stale_delta);
    var committed_1 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_1, .{});
    defer committed_1.deinit(alloc);

    const base_new = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 3,
        .members = &.{ 10, 20, 30 },
    });
    defer alloc.free(base_new);

    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendBase(7, base_new);
    var committed_2 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_2, .{});
    defer committed_2.deinit(alloc);

    var compacted = try compactDirectoryStoreAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer compacted.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 3), compacted.entry.meta.segment_id);
    try std.testing.expectEqual(@as(usize, 2), compacted.stats.manifest.removed_segments);
    try std.testing.expectEqual(@as(usize, 1), compacted.stats.manifest.added_segments);
    try std.testing.expectEqual(@as(usize, 1), compacted.stats.manifest.output_segments);
    try std.testing.expectEqual(@as(u64, 4), compacted.stats.manifest.next_segment_id);
    try std.testing.expectEqual(@as(usize, 1), compacted.stats.compaction.dropped_stale_delta_records);

    var store = try openStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer store.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 4), store.manifest.next_segment_id);
    try std.testing.expectEqual(@as(usize, 1), store.segments.len);
    try std.testing.expectEqual(@as(u64, 3), store.segments[0].meta.segment_id);

    const snapshot = store.snapshot();
    const header = (try snapshot.loadBaseHeader(7)).?;
    try std.testing.expectEqual(@as(u64, 3), header.generation);
    try std.testing.expectEqual(@as(usize, 3), header.member_count);
    const records = try snapshot.loadDeltaTailAfterGeneration(alloc, 7, 3);
    defer alloc.free(records);
    try std.testing.expectEqual(@as(usize, 0), records.len);
}

pub fn testDirectoryCompactionCanReplaceSelectedSegments() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendPostingBase(.{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    var committed_1 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_1, .{});
    defer committed_1.deinit(alloc);

    const delta_sequence = (@as(u64, 2) << 32) | 1;
    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendPostingDeltaRecords(7, &.{
        .{ .sequence = delta_sequence, .op = .insert, .vector_id = 30 },
    });
    var committed_2 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_2, .{});
    defer committed_2.deinit(alloc);

    var writer_3 = Writer.init(alloc);
    defer writer_3.deinit();
    try writer_3.appendPostingBase(.{
        .posting_id = 9,
        .generation = 1,
        .members = &.{90},
    });
    var committed_3 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_3, .{});
    defer committed_3.deinit(alloc);

    try std.testing.expectError(error.NoPostingSegmentsToCompact, compactDirectoryStoreSegmentIdsAlloc(alloc, std.testing.io, tmp.dir, &.{}, .{}));
    try std.testing.expectError(error.DuplicatePostingSegmentId, compactDirectoryStoreSegmentIdsAlloc(alloc, std.testing.io, tmp.dir, &.{ committed_1.entry.meta.segment_id, committed_1.entry.meta.segment_id }, .{}));
    try std.testing.expectError(error.PostingSegmentManifestReplacementMissingSegment, compactDirectoryStoreSegmentIdsAlloc(alloc, std.testing.io, tmp.dir, &.{99}, .{}));

    var compacted = try compactDirectoryStoreSegmentIdsAlloc(alloc, std.testing.io, tmp.dir, &.{ committed_1.entry.meta.segment_id, committed_2.entry.meta.segment_id }, .{});
    defer compacted.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 4), compacted.entry.meta.segment_id);
    try std.testing.expectEqual(@as(usize, 2), compacted.stats.manifest.removed_segments);
    try std.testing.expectEqual(@as(usize, 1), compacted.stats.manifest.added_segments);
    try std.testing.expectEqual(@as(usize, 2), compacted.stats.manifest.output_segments);
    try std.testing.expectEqual(@as(u64, 5), compacted.stats.manifest.next_segment_id);

    var store = try openStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer store.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), store.segments.len);
    try std.testing.expectEqual(committed_3.entry.meta.segment_id, store.segments[0].meta.segment_id);
    try std.testing.expectEqual(compacted.entry.meta.segment_id, store.segments[1].meta.segment_id);

    const snapshot = store.snapshot();
    const posting_7 = (try snapshot.materializeMembers(alloc, 7)).?;
    defer alloc.free(posting_7);
    try std.testing.expectEqualSlices(posting.VectorId, &.{ 10, 20, 30 }, posting_7);
    const posting_9 = (try snapshot.materializeMembers(alloc, 9)).?;
    defer alloc.free(posting_9);
    try std.testing.expectEqualSlices(posting.VectorId, &.{90}, posting_9);
}

pub fn testDirectoryCompactionPlanFromDirectoryFeedsSelectedCompaction() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendPostingBase(.{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    var committed_1 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_1, .{});
    defer committed_1.deinit(alloc);

    const delta_sequence = (@as(u64, 2) << 32) | 1;
    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendPostingDeltaRecords(7, &.{
        .{ .sequence = delta_sequence, .op = .insert, .vector_id = 30 },
    });
    var committed_2 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_2, .{});
    defer committed_2.deinit(alloc);

    var writer_3 = Writer.init(alloc);
    defer writer_3.deinit();
    try writer_3.appendPostingBase(.{
        .posting_id = 9,
        .generation = 1,
        .members = &.{90},
    });
    var committed_3 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_3, .{});
    defer committed_3.deinit(alloc);

    var plan = try planDirectoryCompactionFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{}, .{
        .max_input_segments = 2,
    });
    defer plan.deinit(alloc);
    try std.testing.expectEqualSlices(u64, &.{ committed_1.entry.meta.segment_id, committed_2.entry.meta.segment_id }, plan.segment_ids);
    try std.testing.expectEqual(@as(usize, 3), plan.stats.manifest_segments);
    try std.testing.expectEqual(@as(usize, 2), plan.stats.selected_segments);
    try std.testing.expect(plan.stats.stopped_on_segment_limit);

    var compacted = try compactDirectoryStoreSegmentIdsAlloc(alloc, std.testing.io, tmp.dir, plan.segment_ids, .{});
    defer compacted.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), compacted.stats.manifest.output_segments);

    var store = try openStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer store.deinit(alloc);
    try std.testing.expectEqual(committed_3.entry.meta.segment_id, store.segments[0].meta.segment_id);
    try std.testing.expectEqual(compacted.entry.meta.segment_id, store.segments[1].meta.segment_id);
}

pub fn testDirectoryMaintenanceCompactsAndCleansInOneStep() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendPostingBase(.{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    var committed_1 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_1, .{});
    defer committed_1.deinit(alloc);

    const delta_sequence = (@as(u64, 2) << 32) | 1;
    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendPostingDeltaRecords(7, &.{
        .{ .sequence = delta_sequence, .op = .insert, .vector_id = 30 },
    });
    var committed_2 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_2, .{});
    defer committed_2.deinit(alloc);

    var writer_3 = Writer.init(alloc);
    defer writer_3.deinit();
    try writer_3.appendPostingBase(.{
        .posting_id = 9,
        .generation = 1,
        .members = &.{90},
    });
    var committed_3 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_3, .{});
    defer committed_3.deinit(alloc);

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "postings/0000000000000099.afps",
        .data = "orphan segment",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "postings/0000000000000100.afps.tmp",
        .data = "partial segment",
    });

    const stats = try maintainDirectoryStoreAlloc(alloc, std.testing.io, tmp.dir, .{
        .plan = .{ .max_input_segments = 2 },
    });
    try std.testing.expectEqual(@as(usize, 3), stats.manifest.segments);
    try std.testing.expectEqual(@as(usize, 2), stats.plan.selected_segments);
    try std.testing.expect(stats.plan.stopped_on_segment_limit);
    try std.testing.expect(stats.compacted);
    try std.testing.expectEqual(@as(usize, 2), stats.compaction.manifest.removed_segments);
    try std.testing.expectEqual(@as(usize, 1), stats.compaction.manifest.added_segments);
    try std.testing.expectEqual(@as(usize, 1), stats.temporary.deleted_temp_files);
    try std.testing.expectEqual(@as(usize, 3), stats.garbage.deleted_segment_files);

    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(std.testing.io, committed_1.entry.path, alloc, .limited(1)));
    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(std.testing.io, committed_2.entry.path, alloc, .limited(1)));
    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(std.testing.io, "postings/0000000000000099.afps", alloc, .limited(1)));
    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(std.testing.io, "postings/0000000000000100.afps.tmp", alloc, .limited(1)));

    var store = try openStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer store.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), store.segments.len);
    try std.testing.expectEqual(committed_3.entry.meta.segment_id, store.segments[0].meta.segment_id);
    try std.testing.expectEqual(stats.compaction.segment_id, store.segments[1].meta.segment_id);

    const snapshot = store.snapshot();
    const posting_7 = (try snapshot.materializeMembers(alloc, 7)).?;
    defer alloc.free(posting_7);
    try std.testing.expectEqualSlices(posting.VectorId, &.{ 10, 20, 30 }, posting_7);
}

pub fn testDirectoryManifestSummaryReadsOnlyManifest() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendPostingBase(.{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    var committed_1 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_1, .{});
    defer committed_1.deinit(alloc);

    const delta_sequence = (@as(u64, 2) << 32) | 1;
    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendPostingDeltaRecords(7, &.{
        .{ .sequence = delta_sequence, .op = .insert, .vector_id = 30 },
    });
    var committed_2 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_2, .{});
    defer committed_2.deinit(alloc);

    const stats = try summarizeDirectoryManifestAlloc(alloc, std.testing.io, tmp.dir, .{});
    try std.testing.expectEqual(@as(usize, 2), stats.segments);
    try std.testing.expectEqual(committed_1.entry.meta.byte_len + committed_2.entry.meta.byte_len, stats.bytes);
    try std.testing.expectEqual(@as(usize, 2), stats.entries);
    try std.testing.expectEqual(@as(u64, 1), stats.min_segment_id);
    try std.testing.expectEqual(@as(u64, 2), stats.max_segment_id);
    try std.testing.expectEqual(@as(u64, 3), stats.next_segment_id);
    try std.testing.expectEqual(@as(PostingId, 7), stats.min_posting_id);
    try std.testing.expectEqual(@as(PostingId, 7), stats.max_posting_id);
    try std.testing.expectEqual(delta_sequence, stats.min_delta_sequence);
    try std.testing.expectEqual(delta_sequence, stats.max_delta_sequence);

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = committed_2.entry.path,
        .data = "not a segment",
    });
    _ = try summarizeDirectoryManifestAlloc(alloc, std.testing.io, tmp.dir, .{});
    try std.testing.expectError(error.CorruptedPostingSegment, verifyDirectoryStoreAlloc(alloc, std.testing.io, tmp.dir, .{}));
}

pub fn testDirectorySelectedCompactionDoesNotReadUnselectedSegments() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendPostingBase(.{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    var committed_1 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_1, .{});
    defer committed_1.deinit(alloc);

    const delta_sequence = (@as(u64, 2) << 32) | 1;
    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendPostingDeltaRecords(7, &.{
        .{ .sequence = delta_sequence, .op = .insert, .vector_id = 30 },
    });
    var committed_2 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_2, .{});
    defer committed_2.deinit(alloc);

    var writer_3 = Writer.init(alloc);
    defer writer_3.deinit();
    try writer_3.appendPostingBase(.{
        .posting_id = 9,
        .generation = 1,
        .members = &.{90},
    });
    var committed_3 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_3, .{});
    defer committed_3.deinit(alloc);

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = committed_3.entry.path,
        .data = "not a segment",
    });
    try std.testing.expectError(error.CorruptedPostingSegment, openStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{}));

    var compacted = try compactDirectoryStoreSegmentIdsAlloc(alloc, std.testing.io, tmp.dir, &.{ committed_1.entry.meta.segment_id, committed_2.entry.meta.segment_id }, .{});
    defer compacted.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), compacted.stats.manifest.output_segments);

    var lazy = try openLazyStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer lazy.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), lazy.manifest.segments.len);
    try std.testing.expectEqual(committed_3.entry.meta.segment_id, lazy.manifest.segments[0].meta.segment_id);
    try std.testing.expectEqual(compacted.entry.meta.segment_id, lazy.manifest.segments[1].meta.segment_id);

    const snapshot = lazy.snapshot();
    const posting_7 = (try snapshot.materializeMembers(alloc, 7)).?;
    defer alloc.free(posting_7);
    try std.testing.expectEqualSlices(posting.VectorId, &.{ 10, 20, 30 }, posting_7);
    try std.testing.expectError(error.CorruptedPostingSegment, snapshot.materializeMembers(alloc, 9));
}

pub fn testDirectoryGarbageCollectionDeletesManifestOrphans() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_old = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    defer alloc.free(base_old);

    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendBase(7, base_old);
    var committed_1 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_1, .{});
    defer committed_1.deinit(alloc);

    const base_new = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 9,
        .generation = 2,
        .members = &.{ 30, 40 },
    });
    defer alloc.free(base_new);

    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendBase(9, base_new);
    var committed_2 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_2, .{});
    defer committed_2.deinit(alloc);

    var compacted = try compactDirectoryStoreAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer compacted.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 3), compacted.entry.meta.segment_id);

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "postings/0000000000009999.afps.tmp",
        .data = "pending-write",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "postings/notes.txt",
        .data = "ignored",
    });

    const stats = try collectDirectoryGarbageAlloc(alloc, std.testing.io, tmp.dir, .{});
    try std.testing.expectEqual(@as(usize, 1), stats.manifest_segments);
    try std.testing.expectEqual(@as(usize, 3), stats.segment_files);
    try std.testing.expectEqual(@as(usize, 1), stats.referenced_segment_files);
    try std.testing.expectEqual(@as(usize, 2), stats.orphan_segment_files);
    try std.testing.expectEqual(@as(usize, 2), stats.deleted_segment_files);
    try std.testing.expect(stats.skipped_entries >= 2);

    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(std.testing.io, committed_1.entry.path, alloc, .limited(1)));
    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(std.testing.io, committed_2.entry.path, alloc, .limited(1)));
    const live_segment = try tmp.dir.readFileAlloc(std.testing.io, compacted.entry.path, alloc, .limited(compacted.entry.meta.byte_len + 1));
    defer alloc.free(live_segment);
    try std.testing.expectEqual(compacted.entry.meta.byte_len, live_segment.len);

    var store = try openStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer store.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), store.segments.len);
    try std.testing.expectEqual(compacted.entry.meta.segment_id, store.segments[0].meta.segment_id);
}

pub fn testDirectoryTemporaryGarbageCollectionDeletesOnlyKnownTemps() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const empty_stats = try collectDirectoryTemporaryGarbageAlloc(alloc, std.testing.io, tmp.dir, .{});
    try std.testing.expectEqual(@as(usize, 0), empty_stats.scanned_entries);
    try std.testing.expectEqual(@as(usize, 0), empty_stats.deleted_temp_files);

    try tmp.dir.createDirPath(std.testing.io, segment_directory);
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "postings/0000000000000001.afps.tmp",
        .data = "partial segment",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "postings/manifest.afpm.tmp",
        .data = "partial manifest",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "postings/0000000000000002.afps",
        .data = "committed-looking file",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "postings/random.tmp",
        .data = "not ours",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "postings/0000000000000003.afps.tmp2",
        .data = "not ours either",
    });

    const stats = try collectDirectoryTemporaryGarbageAlloc(alloc, std.testing.io, tmp.dir, .{});
    try std.testing.expectEqual(@as(usize, 1), stats.segment_temp_files);
    try std.testing.expectEqual(@as(usize, 1), stats.manifest_temp_files);
    try std.testing.expectEqual(@as(usize, 2), stats.deleted_temp_files);
    try std.testing.expect(stats.skipped_entries >= 3);

    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(std.testing.io, "postings/0000000000000001.afps.tmp", alloc, .limited(100)));
    try std.testing.expectError(error.FileNotFound, tmp.dir.readFileAlloc(std.testing.io, "postings/manifest.afpm.tmp", alloc, .limited(100)));

    const committed = try tmp.dir.readFileAlloc(std.testing.io, "postings/0000000000000002.afps", alloc, .limited(100));
    defer alloc.free(committed);
    try std.testing.expectEqualStrings("committed-looking file", committed);
    const random = try tmp.dir.readFileAlloc(std.testing.io, "postings/random.tmp", alloc, .limited(100));
    defer alloc.free(random);
    try std.testing.expectEqualStrings("not ours", random);
    const tmp2 = try tmp.dir.readFileAlloc(std.testing.io, "postings/0000000000000003.afps.tmp2", alloc, .limited(100));
    defer alloc.free(tmp2);
    try std.testing.expectEqualStrings("not ours either", tmp2);
}

pub fn testLazyDirectoryStoreReadsOnlyCandidateSegments() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_7 = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    defer alloc.free(base_7);
    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendBase(7, base_7);
    var committed_1 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_1, .{});
    defer committed_1.deinit(alloc);

    const base_99 = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 99,
        .generation = 2,
        .members = &.{ 30, 40 },
    });
    defer alloc.free(base_99);
    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendBase(99, base_99);
    var committed_2 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_2, .{});
    defer committed_2.deinit(alloc);

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = committed_2.entry.path,
        .data = "not a segment",
    });

    try std.testing.expectError(error.CorruptedPostingSegment, openStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{}));

    var lazy = try openLazyStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer lazy.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), lazy.manifest.segments.len);

    const snapshot = lazy.snapshot();
    const header = (try snapshot.loadBaseHeader(alloc, 7)).?;
    try std.testing.expectEqual(@as(PostingId, 7), header.posting_id);
    try std.testing.expectEqual(@as(u64, 1), header.generation);
    try std.testing.expectEqual(@as(usize, 2), header.member_count);
    try std.testing.expectError(error.CorruptedPostingSegment, snapshot.loadBaseHeader(alloc, 99));

    const missing = try snapshot.loadBaseHeader(alloc, 12345);
    try std.testing.expect(missing == null);
}

pub fn testLazyDirectoryStoreLoadsDeltaTail() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    defer alloc.free(base);
    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendBase(7, base);
    var committed_1 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_1, .{});
    defer committed_1.deinit(alloc);

    const stale_sequence = (@as(u64, 1) << 32) | 1;
    const live_sequence = (@as(u64, 2) << 32) | 1;
    const delta = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = stale_sequence, .op = .insert, .vector_id = 30 },
        .{ .sequence = live_sequence, .op = .insert, .vector_id = 40 },
    });
    defer alloc.free(delta);
    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendDelta(7, stale_sequence, delta);
    var committed_2 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_2, .{});
    defer committed_2.deinit(alloc);

    var lazy = try openLazyStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer lazy.deinit(alloc);
    try std.testing.expectEqual(stale_sequence, lazy.manifest.segments[1].meta.min_delta_sequence);
    try std.testing.expectEqual(live_sequence, lazy.manifest.segments[1].meta.max_delta_sequence);
    const snapshot = lazy.snapshot();

    const all_records = try snapshot.loadDeltaTail(alloc, 7);
    defer alloc.free(all_records);
    try std.testing.expectEqual(@as(usize, 2), all_records.len);
    try std.testing.expectEqual(stale_sequence, all_records[0].sequence);
    try std.testing.expectEqual(live_sequence, all_records[1].sequence);
    const all_stats = try snapshot.deltaTailStats(alloc, 7, 0);
    try std.testing.expectEqual(@as(usize, 2), all_stats.records);
    try std.testing.expectEqual(@as(usize, 2), all_stats.records_after_generation);
    try std.testing.expectEqual(delta.len, all_stats.encoded_value_bytes);
    try std.testing.expectEqual(live_sequence, all_stats.max_sequence_after_generation);

    const current_records = try snapshot.loadDeltaTailAfterGeneration(alloc, 7, 1);
    defer alloc.free(current_records);
    try std.testing.expectEqual(@as(usize, 1), current_records.len);
    try std.testing.expectEqual(live_sequence, current_records[0].sequence);
    const current_stats = try snapshot.deltaTailStats(alloc, 7, 1);
    try std.testing.expectEqual(@as(usize, 2), current_stats.records);
    try std.testing.expectEqual(@as(usize, 1), current_stats.records_after_generation);
    try std.testing.expectEqual(delta.len, current_stats.encoded_value_bytes);
    try std.testing.expectEqual(live_sequence, current_stats.max_sequence_after_generation);
    const current_with_stats = try snapshot.loadDeltaTailAfterGenerationWithStats(alloc, 7, 1);
    defer current_with_stats.deinit(alloc);
    try std.testing.expectEqualSlices(posting.PostingDeltaRecord, current_records, current_with_stats.records);
    try std.testing.expectEqual(current_stats.records, current_with_stats.stats.records);
    try std.testing.expectEqual(current_stats.records_after_generation, current_with_stats.stats.records_after_generation);
    try std.testing.expectEqual(current_stats.encoded_value_bytes, current_with_stats.stats.encoded_value_bytes);
    try std.testing.expectEqual(current_stats.max_sequence_after_generation, current_with_stats.stats.max_sequence_after_generation);
    var delta_scratch = posting.PostingStore.FoldScratch{};
    defer delta_scratch.deinit(alloc);
    const scratch_stats = try snapshot.appendDeltaTailAfterGenerationIntoScratchWithStats(alloc, 7, 1, &delta_scratch);
    try std.testing.expectEqualSlices(posting.PostingDeltaRecord, current_records, delta_scratch.deltaRecords());
    try std.testing.expectEqual(current_stats.records, scratch_stats.records);
    try std.testing.expectEqual(current_stats.records_after_generation, scratch_stats.records_after_generation);
    try std.testing.expectEqual(current_stats.encoded_value_bytes, scratch_stats.encoded_value_bytes);
    try std.testing.expectEqual(current_stats.max_sequence_after_generation, scratch_stats.max_sequence_after_generation);
    var filtered_delta_scratch = posting.PostingStore.FoldScratch{};
    defer filtered_delta_scratch.deinit(alloc);
    var filtered_direct_stats = posting.PostingDeltaTailStats{};
    try readSegmentDeltaRecordsIntoScratchWithStatsAlloc(alloc, std.testing.io, tmp.dir, .{
        .meta = committed_2.entry.meta,
        .path = committed_2.entry.path,
    }, 7, 2, &filtered_delta_scratch, &filtered_direct_stats);
    try std.testing.expectEqual(@as(usize, 2), filtered_direct_stats.records);
    try std.testing.expectEqual(@as(usize, 0), filtered_direct_stats.records_after_generation);
    try std.testing.expectEqual(@as(usize, 0), filtered_delta_scratch.deltaRecordCount());
    try std.testing.expectEqual(@as(usize, 0), filtered_delta_scratch.delta_records.len);

    const materialized = (try snapshot.materializeMembers(alloc, 7)).?;
    defer alloc.free(materialized);
    var scratch = posting.PostingStore.FoldScratch{};
    defer scratch.deinit(alloc);
    const scratch_count = (try snapshot.materializeMembersIntoScratch(alloc, 7, &scratch)).?;
    try std.testing.expectEqualSlices(posting.VectorId, materialized, scratch.member_ids[0..scratch_count]);
    const sorted_scratch_count = (try snapshot.materializeSortedMembersIntoScratch(alloc, 7, &scratch)).?;
    try std.testing.expectEqualSlices(posting.VectorId, materialized, scratch.member_ids[0..sorted_scratch_count]);
}

pub fn testLazyDirectoryStoreUsesNewestPointRecordsBySegmentId() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const old_base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 1,
        .members = &.{10},
    });
    defer alloc.free(old_base);
    const old_centroid = try posting.CentroidDirectoryFormat.encode(alloc, .{
        .posting_id = 7,
        .generation = 1,
        .mutation_version = 1,
        .payload_version = 1,
        .flags = 0,
        .parent = 1,
        .level = 0,
        .member_count = 1,
        .bounds_radius = 1.0,
        .centroid = &.{ 1.0, 2.0 },
    });
    defer alloc.free(old_centroid);
    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendBase(7, old_base);
    try writer_1.appendCentroidDirectory(7, old_centroid);
    var committed_1 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_1, .{});
    defer committed_1.deinit(alloc);

    const new_base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 2,
        .members = &.{ 20, 30 },
    });
    defer alloc.free(new_base);
    const new_centroid = try posting.CentroidDirectoryFormat.encode(alloc, .{
        .posting_id = 7,
        .generation = 2,
        .mutation_version = 4,
        .payload_version = 5,
        .flags = posting.CentroidDirectoryFormat.dirty_flag,
        .parent = 2,
        .level = 0,
        .member_count = 2,
        .bounds_radius = 2.0,
        .centroid = &.{ 3.0, 4.0 },
    });
    defer alloc.free(new_centroid);
    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendBase(7, new_base);
    try writer_2.appendCentroidDirectory(7, new_centroid);
    var committed_2 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_2, .{});
    defer committed_2.deinit(alloc);

    const stale_sequence = (@as(u64, 2) << 32) | 1;
    const live_sequence = (@as(u64, 3) << 32) | 1;
    var writer_3 = Writer.init(alloc);
    defer writer_3.deinit();
    try writer_3.appendPostingDeltaRecords(7, &.{
        .{ .sequence = stale_sequence, .op = .insert, .vector_id = 99 },
        .{ .sequence = live_sequence, .op = .insert, .vector_id = 40 },
    });
    var committed_3 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_3, .{});
    defer committed_3.deinit(alloc);

    var lazy = try openLazyStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer lazy.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), lazy.manifest.segments.len);
    std.mem.swap(OwnedManifestEntry, &lazy.manifest.segments[0], &lazy.manifest.segments[1]);

    const snapshot = lazy.snapshot();
    const header = (try snapshot.loadBaseHeader(alloc, 7)).?;
    try std.testing.expectEqual(@as(u64, 2), header.generation);
    try std.testing.expectEqual(@as(usize, 2), header.member_count);

    var centroid = (try snapshot.loadCentroidDirectoryRecord(alloc, 7)).?;
    defer centroid.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), centroid.generation);
    try std.testing.expectEqual(@as(u64, 4), centroid.mutation_version);
    try std.testing.expectEqual(@as(usize, 2), centroid.member_count);

    const materialized = (try snapshot.materializeMembers(alloc, 7)).?;
    defer alloc.free(materialized);
    try std.testing.expectEqualSlices(posting.VectorId, &.{ 20, 30, 40 }, materialized);

    var scratch = posting.PostingStore.FoldScratch{};
    defer scratch.deinit(alloc);
    const scratch_count = (try snapshot.materializeSortedMembersIntoScratch(alloc, 7, &scratch)).?;
    try std.testing.expectEqualSlices(posting.VectorId, &.{ 20, 30, 40 }, scratch.member_ids[0..scratch_count]);
}

pub fn testTypedBaseDeltaFacadeRoundTripsThroughDirectoryStore() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendPostingBase(.{
        .posting_id = 7,
        .generation = 2,
        .members = &.{ 10, 20 },
    });
    try writer_1.appendCentroidDirectoryRecord(.{
        .posting_id = 7,
        .generation = 2,
        .mutation_version = 9,
        .payload_version = 4,
        .flags = posting.CentroidDirectoryFormat.dirty_flag,
        .parent = 1,
        .level = 0,
        .member_count = 2,
        .bounds_radius = 3.5,
        .centroid = &.{ 1.0, 2.0 },
    });
    var committed_1 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_1, .{});
    defer committed_1.deinit(alloc);

    const stale_sequence = (@as(u64, 1) << 32) | 1;
    const tombstone_sequence = (@as(u64, 3) << 32) | 1;
    const insert_sequence = (@as(u64, 3) << 32) | 2;
    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendPostingDeltaRecords(7, &.{
        .{ .sequence = stale_sequence, .op = .insert, .vector_id = 99 },
        .{ .sequence = tombstone_sequence, .op = .tombstone, .vector_id = 20 },
        .{ .sequence = insert_sequence, .op = .insert, .vector_id = 30 },
    });
    var committed_2 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_2, .{});
    defer committed_2.deinit(alloc);

    var eager = try openStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer eager.deinit(alloc);
    const eager_snapshot = eager.snapshot();
    const eager_members = (try eager_snapshot.materializeMembers(alloc, 7)).?;
    defer alloc.free(eager_members);
    try std.testing.expectEqualSlices(posting.VectorId, &.{ 10, 30 }, eager_members);
    var eager_centroid = (try eager_snapshot.loadCentroidDirectoryRecord(alloc, 7)).?;
    defer eager_centroid.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 9), eager_centroid.mutation_version);
    try std.testing.expectEqual(posting.CentroidDirectoryFormat.dirty_flag, eager_centroid.flags);

    var lazy = try openLazyStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer lazy.deinit(alloc);
    const lazy_snapshot = lazy.snapshot();
    const lazy_members = (try lazy_snapshot.materializeMembers(alloc, 7)).?;
    defer alloc.free(lazy_members);
    try std.testing.expectEqualSlices(posting.VectorId, eager_members, lazy_members);
    const missing = try lazy_snapshot.materializeMembers(alloc, 12345);
    try std.testing.expect(missing == null);
}

pub fn testDirectoryVerificationReportsStatsAndRejectsCorruption() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendPostingBase(.{
        .posting_id = 7,
        .generation = 2,
        .members = &.{ 10, 20 },
    });
    try writer_1.appendCentroidDirectoryRecord(.{
        .posting_id = 7,
        .generation = 2,
        .mutation_version = 3,
        .payload_version = 4,
        .flags = 0,
        .parent = 1,
        .level = 0,
        .member_count = 2,
        .centroid = &.{ 1.0, 2.0 },
    });
    var committed_1 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_1, .{});
    defer committed_1.deinit(alloc);

    const delta_sequence_1 = (@as(u64, 3) << 32) | 1;
    const delta_sequence_2 = (@as(u64, 3) << 32) | 2;
    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendPostingDeltaRecords(7, &.{
        .{ .sequence = delta_sequence_1, .op = .tombstone, .vector_id = 20 },
        .{ .sequence = delta_sequence_2, .op = .insert, .vector_id = 30 },
    });
    var committed_2 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &writer_2, .{});
    defer committed_2.deinit(alloc);

    const stats = try verifyDirectoryStoreAlloc(alloc, std.testing.io, tmp.dir, .{});
    try std.testing.expectEqual(@as(usize, 2), stats.manifest_segments);
    try std.testing.expect(stats.manifest_bytes > 0);
    try std.testing.expectEqual(@as(usize, 2), stats.segment_files);
    try std.testing.expectEqual(committed_1.entry.meta.byte_len + committed_2.entry.meta.byte_len, stats.segment_bytes);
    try std.testing.expectEqual(@as(usize, 3), stats.entries);
    try std.testing.expectEqual(@as(usize, 1), stats.base_records);
    try std.testing.expectEqual(@as(usize, 1), stats.centroid_records);
    try std.testing.expectEqual(@as(usize, 1), stats.delta_values);
    try std.testing.expectEqual(@as(usize, 2), stats.delta_records);

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = committed_2.entry.path,
        .data = "not a segment",
    });
    try std.testing.expectError(error.CorruptedPostingSegment, verifyDirectoryStoreAlloc(alloc, std.testing.io, tmp.dir, .{}));
}

pub fn testDirectoryCopyPublishesManifestAfterSegments() !void {
    const alloc = std.testing.allocator;
    var source = std.testing.tmpDir(.{});
    defer source.cleanup();
    var destination = std.testing.tmpDir(.{});
    defer destination.cleanup();

    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendPostingBase(.{
        .posting_id = 7,
        .generation = 2,
        .members = &.{ 10, 20 },
    });
    var committed_1 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, source.dir, &writer_1, .{});
    defer committed_1.deinit(alloc);

    const delta_sequence = (@as(u64, 3) << 32) | 1;
    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendPostingDeltaRecords(7, &.{
        .{ .sequence = delta_sequence, .op = .insert, .vector_id = 30 },
    });
    var committed_2 = try commitWriterToDirectoryAlloc(alloc, std.testing.io, source.dir, &writer_2, .{});
    defer committed_2.deinit(alloc);

    const stats = try copyDirectoryStoreAlloc(alloc, std.testing.io, source.dir, destination.dir, .{});
    try std.testing.expectEqual(@as(usize, 2), stats.manifest_segments);
    try std.testing.expect(stats.manifest_bytes > 0);
    try std.testing.expectEqual(@as(usize, 2), stats.segment_files);
    try std.testing.expectEqual(committed_1.entry.meta.byte_len + committed_2.entry.meta.byte_len, stats.segment_bytes);
    try std.testing.expectEqual(@as(usize, 2), stats.entries);

    var copied = try openStoreFromDirectoryAlloc(alloc, std.testing.io, destination.dir, .{});
    defer copied.deinit(alloc);
    const snapshot = copied.snapshot();
    const members = (try snapshot.materializeMembers(alloc, 7)).?;
    defer alloc.free(members);
    try std.testing.expectEqualSlices(posting.VectorId, &.{ 10, 20, 30 }, members);

    var failed_destination = std.testing.tmpDir(.{});
    defer failed_destination.cleanup();
    try source.dir.writeFile(std.testing.io, .{
        .sub_path = committed_2.entry.path,
        .data = "not a segment",
    });
    try std.testing.expectError(error.CorruptedPostingSegment, copyDirectoryStoreAlloc(alloc, std.testing.io, source.dir, failed_destination.dir, .{}));
    try std.testing.expectError(error.FileNotFound, failed_destination.dir.readFileAlloc(std.testing.io, default_manifest_path, alloc, .limited(1)));
}

pub fn testCompactsSegmentsToLivePostingEntries() !void {
    const alloc = std.testing.allocator;
    const old_base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    defer alloc.free(old_base);
    const new_base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 3,
        .members = &.{ 10, 20, 30 },
    });
    defer alloc.free(new_base);
    const old_centroid = try posting.CentroidDirectoryFormat.encode(alloc, .{
        .posting_id = 7,
        .generation = 1,
        .mutation_version = 1,
        .payload_version = 1,
        .flags = 0,
        .parent = 1,
        .level = 0,
        .member_count = 2,
        .centroid = &.{ 1.0, 1.0 },
    });
    defer alloc.free(old_centroid);
    const new_centroid = try posting.CentroidDirectoryFormat.encode(alloc, .{
        .posting_id = 7,
        .generation = 3,
        .mutation_version = 4,
        .payload_version = 5,
        .flags = posting.CentroidDirectoryFormat.dirty_flag,
        .parent = 1,
        .level = 0,
        .member_count = 3,
        .centroid = &.{ 3.0, 3.0 },
    });
    defer alloc.free(new_centroid);

    const stale_sequence = (@as(u64, 2) << 32) | 1;
    const live_sequence = (@as(u64, 4) << 32) | 1;
    const duplicate_sequence = (@as(u64, 4) << 32) | 2;
    const old_delta = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = stale_sequence, .op = .insert, .vector_id = 40 },
        .{ .sequence = duplicate_sequence, .op = .insert, .vector_id = 50 },
    });
    defer alloc.free(old_delta);
    const new_delta = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = stale_sequence, .op = .tombstone, .vector_id = 40 },
        .{ .sequence = live_sequence, .op = .insert, .vector_id = 60 },
        .{ .sequence = duplicate_sequence, .op = .tombstone, .vector_id = 50 },
    });
    defer alloc.free(new_delta);

    var writer_1 = Writer.init(alloc);
    defer writer_1.deinit();
    try writer_1.appendBase(7, old_base);
    try writer_1.appendCentroidDirectory(7, old_centroid);
    try writer_1.appendDelta(7, stale_sequence, old_delta);
    var segment_1 = try writer_1.buildSegment(1);
    defer segment_1.deinit(alloc);

    var writer_2 = Writer.init(alloc);
    defer writer_2.deinit();
    try writer_2.appendBase(7, new_base);
    try writer_2.appendCentroidDirectory(7, new_centroid);
    try writer_2.appendDelta(7, stale_sequence, new_delta);
    var segment_2 = try writer_2.buildSegment(2);
    defer segment_2.deinit(alloc);

    const inputs = [_]SegmentBlob{ segment_1.blob(), segment_2.blob() };
    var compacted = try compactSegmentsWithStatsAlloc(alloc, 9, inputs[0..]);
    defer compacted.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 9), compacted.segment.meta.segment_id);
    try std.testing.expectEqual(@as(usize, 3), compacted.segment.meta.entry_count);
    try std.testing.expectEqual(@as(usize, 2), compacted.stats.input_segments);
    try std.testing.expectEqual(segment_1.data.len + segment_2.data.len, compacted.stats.input_bytes);
    try std.testing.expectEqual(segment_1.meta.entry_count + segment_2.meta.entry_count, compacted.stats.input_entries);
    try std.testing.expectEqual(@as(usize, 2), compacted.stats.input_base_records);
    try std.testing.expectEqual(@as(usize, 2), compacted.stats.input_centroid_records);
    try std.testing.expectEqual(@as(usize, 2), compacted.stats.input_delta_values);
    try std.testing.expectEqual(@as(usize, 5), compacted.stats.input_delta_records);
    try std.testing.expectEqual(@as(usize, 1), compacted.stats.retained_base_records);
    try std.testing.expectEqual(@as(usize, 1), compacted.stats.retained_centroid_records);
    try std.testing.expectEqual(@as(usize, 2), compacted.stats.retained_delta_records);
    try std.testing.expectEqual(@as(usize, 1), compacted.stats.dropped_superseded_base_records);
    try std.testing.expectEqual(@as(usize, 1), compacted.stats.dropped_superseded_centroid_records);
    try std.testing.expectEqual(@as(usize, 2), compacted.stats.dropped_stale_delta_records);
    try std.testing.expectEqual(@as(usize, 1), compacted.stats.dropped_duplicate_delta_records);
    try std.testing.expectEqual(compacted.segment.data.len, compacted.stats.output_bytes);
    try std.testing.expectEqual(compacted.segment.meta.entry_count, compacted.stats.output_entries);

    const snapshot = Snapshot{ .catalog = .{ .segments = &.{compacted.segment.blob()} } };
    const header = (try snapshot.loadBaseHeader(7)).?;
    try std.testing.expectEqual(@as(u64, 3), header.generation);
    try std.testing.expectEqual(@as(usize, 3), header.member_count);

    var centroid = (try snapshot.loadCentroidDirectoryRecord(alloc, 7)).?;
    defer centroid.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 4), centroid.mutation_version);
    try std.testing.expectEqual(@as(u64, 5), centroid.payload_version);
    try std.testing.expectEqual(posting.CentroidDirectoryFormat.dirty_flag, centroid.flags);

    const all_records = try snapshot.loadDeltaTail(alloc, 7);
    defer alloc.free(all_records);
    try std.testing.expectEqual(@as(usize, 2), all_records.len);
    try std.testing.expectEqual(live_sequence, all_records[0].sequence);
    try std.testing.expectEqual(posting.PostingDeltaOp.insert, all_records[0].op);
    try std.testing.expectEqual(@as(posting.VectorId, 60), all_records[0].vector_id);
    try std.testing.expectEqual(duplicate_sequence, all_records[1].sequence);
    try std.testing.expectEqual(posting.PostingDeltaOp.tombstone, all_records[1].op);
    try std.testing.expectEqual(@as(posting.VectorId, 50), all_records[1].vector_id);

    const current_records = try snapshot.loadDeltaTailAfterGeneration(alloc, 7, 3);
    defer alloc.free(current_records);
    try std.testing.expectEqual(@as(usize, 2), current_records.len);
}

test "posting segment stores base centroid and ordered delta values" {
    try testStoresBaseCentroidAndOrderedDeltaValues();
}

test "posting segment reader reports point value locations" {
    try testReaderReportsPointValueLocations();
}

test "posting segment point value range reads verify index and value" {
    try testSegmentPointValueRangeReadsVerifyIndexAndValue();
}

test "posting segment rejects duplicate logical entries" {
    try testRejectsDuplicateLogicalEntries();
}

test "posting segment validates footer and version" {
    try testValidatesFooterAndVersion();
}

test "posting segment catalog looks up newest point records and merged deltas" {
    try testCatalogLooksUpNewestPointRecordsAndMergedDeltas();
}

test "posting segment snapshot loads typed posting values" {
    try testSnapshotLoadsTypedPostingValues();
}

test "posting segment manifest codec round trips segment metadata" {
    try testManifestCodecRoundTripsSegmentMetadata();
}

test "posting segment manifest codec rejects invalid data" {
    try testManifestCodecRejectsInvalidData();
}

test "posting segment manifest replacement encodes compaction commit" {
    try testManifestReplacementEncodesCompactionCommit();
}

test "posting segment directory compaction planner selects within budgets" {
    try testDirectoryCompactionPlannerSelectsWithinBudgets();
}

test "posting segment manifest summary aggregates metadata without segment reads" {
    try testManifestSummaryAggregatesMetadataWithoutSegmentReads();
}

test "posting segment store validates manifest backed segments" {
    try testOpenStoreValidatesManifestBackedSegments();
}

test "posting segment build produces manifest ready metadata" {
    try testBuildSegmentProducesManifestReadyMetadata();
}

test "posting segment directory store round trips segment files" {
    try testDirectoryStoreRoundTripsSegmentFiles();
}

test "posting segment directory commit appends manifest segments" {
    try testDirectoryCommitAppendsManifestSegments();
}

test "posting segment pending delta batch size uses cached min sequence" {
    const alloc = std.testing.allocator;
    const records = [_]posting.PostingDeltaRecord{
        .{ .sequence = (@as(u64, 10) << 32) | 2, .op = .insert, .vector_id = 100 },
        .{ .sequence = (@as(u64, 8) << 32) | 1, .op = .tombstone, .vector_id = 50 },
        .{ .sequence = (@as(u64, 12) << 32) | 3, .op = .insert, .vector_id = 150 },
    };

    var batch = PendingDeltaBatch{};
    defer batch.deinit(alloc);

    var i: usize = 0;
    while (i < records.len) : (i += 1) {
        const expected = try posting.PostingFormat.encodedDeltaTailSize(records[0 .. i + 1]);
        const actual = try pendingDeltaBatchEncodedSizeWithRecord(batch, records[i]);
        try std.testing.expectEqual(expected, actual);
        try batch.records.append(alloc, records[i]);
        batch.noteAppendedRecord(records[i], actual);
    }

    try std.testing.expectEqual(records[1].sequence, batch.min_sequence);
}

test "posting segment directory batch writer flushes bounded segments" {
    try testDirectoryBatchWriterFlushesBoundedSegments();
}

test "posting segment runtime directory store refreshes snapshots" {
    try testRuntimeDirectoryStoreRefreshesSnapshots();
}

test "posting segment directory compaction replaces manifest segments" {
    try testDirectoryCompactionReplacesManifestSegments();
}

test "posting segment directory compaction can replace selected segments" {
    try testDirectoryCompactionCanReplaceSelectedSegments();
}

test "posting segment directory compaction plan from directory feeds selected compaction" {
    try testDirectoryCompactionPlanFromDirectoryFeedsSelectedCompaction();
}

test "posting segment directory maintenance compacts and cleans in one step" {
    try testDirectoryMaintenanceCompactsAndCleansInOneStep();
}

test "posting segment directory manifest summary reads only manifest" {
    try testDirectoryManifestSummaryReadsOnlyManifest();
}

test "posting segment directory selected compaction does not read unselected segments" {
    try testDirectorySelectedCompactionDoesNotReadUnselectedSegments();
}

test "posting segment directory garbage collection deletes manifest orphans" {
    try testDirectoryGarbageCollectionDeletesManifestOrphans();
}

test "posting segment directory temporary garbage collection deletes only known temps" {
    try testDirectoryTemporaryGarbageCollectionDeletesOnlyKnownTemps();
}

test "posting segment lazy directory store reads only candidate segments" {
    try testLazyDirectoryStoreReadsOnlyCandidateSegments();
}

test "posting segment lazy directory store loads delta tail" {
    try testLazyDirectoryStoreLoadsDeltaTail();
}

test "posting segment lazy materialization sizes member scratch by live deltas" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try posting.PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 1,
        .members = &.{ 10, 20 },
    });
    defer alloc.free(base);
    var base_writer = Writer.init(alloc);
    defer base_writer.deinit();
    try base_writer.appendBase(7, base);
    var committed_base = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &base_writer, .{});
    defer committed_base.deinit(alloc);

    const sequence_1 = (@as(u64, 2) << 32) | 1;
    const sequence_2 = (@as(u64, 2) << 32) | 2;
    const delta = try posting.PostingFormat.encodeDeltaTail(alloc, &.{
        .{ .sequence = sequence_1, .op = .tombstone, .vector_id = 10 },
        .{ .sequence = sequence_2, .op = .tombstone, .vector_id = 999 },
    });
    defer alloc.free(delta);
    var delta_writer = Writer.init(alloc);
    defer delta_writer.deinit();
    try delta_writer.appendDelta(7, sequence_1, delta);
    var committed_delta = try commitWriterToDirectoryAlloc(alloc, std.testing.io, tmp.dir, &delta_writer, .{});
    defer committed_delta.deinit(alloc);

    var lazy = try openLazyStoreFromDirectoryAlloc(alloc, std.testing.io, tmp.dir, .{});
    defer lazy.deinit(alloc);
    var scratch = posting.PostingStore.FoldScratch{};
    defer scratch.deinit(alloc);

    const member_count = (try lazy.snapshot().materializeMembersIntoScratch(alloc, 7, &scratch)).?;

    try std.testing.expectEqual(@as(usize, 1), member_count);
    try std.testing.expectEqualSlices(posting.VectorId, &.{20}, scratch.member_ids[0..member_count]);
    try std.testing.expectEqual(@as(usize, 2), scratch.member_ids.len);
}

test "posting segment lazy directory store uses newest point records by segment id" {
    try testLazyDirectoryStoreUsesNewestPointRecordsBySegmentId();
}

test "posting segment typed base delta facade round trips through directory store" {
    try testTypedBaseDeltaFacadeRoundTripsThroughDirectoryStore();
}

test "posting segment directory verification reports stats and rejects corruption" {
    try testDirectoryVerificationReportsStatsAndRejectsCorruption();
}

test "posting segment directory copy publishes manifest after segments" {
    try testDirectoryCopyPublishesManifestAfterSegments();
}

test "posting segment compacts segments to live posting entries" {
    try testCompactsSegmentsToLivePostingEntries();
}
