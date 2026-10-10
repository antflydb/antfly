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

//! Native single-file Antfly Lite format primitives.
//!
//! This module owns revision-3 compatibility and the revision-4 indexed `.aflite`
//! format, checkpoint publication, ownership retirement, and physical page reuse.

const std = @import("std");
const builtin = @import("builtin");
const Crc32 = @import("antfly_hash").Crc32;
const antfly_platform = @import("antfly_platform");
const platform_sync = antfly_platform.sync;
const fs_paths = @import("antfly_runtime_fs").fs_paths;
const threaded_io_limits = @import("antfly_runtime_fs").threaded_io_limits;
const resource_manager_mod = @import("../resource_manager.zig");
const maintenance = @import("../maintenance.zig");
pub const allocator_v4 = @import("allocator_v4.zig");

const Allocator = std.mem.Allocator;

test "lite allocator v4 small checkpoints bound counter and queue boundary growth" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "small-checkpoint-boundaries.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .indexed_reclamation = true, .no_sync = true });
    defer file.close();
    file.reserve_retirement_capacity = true;
    var pages = try file.pageAllocatorFromFreeMap(file.activeCheckpoint());
    defer pages.deinit();
    const counter_entries = (file.maxPagePayloadBytes() - 24) / 4;
    const queue_entries = (file.maxPagePayloadBytes() - 16) / 48;
    const first = pages.next_page_id;
    while (pages.next_page_id < counter_entries - 2) {
        const page = try pages.allocate();
        try pages.writePage(page, .data, "raw block");
    }
    for (0..queue_entries - 1) |i| try pages.ledger.?.retire(.{ .epoch = file.publicationEpoch(), .page = first + i, .kind = .page });
    var checkpoint = file.activeCheckpoint();
    checkpoint.commit_sequence += 1;
    checkpoint.free_map_root_page = try pages.allocate();
    try file.advanceAllocatorCheckpoint(&pages, 1);
    try std.testing.expect(pages.ledger.?.force_checkpoint);
    try pages.flush();
    const writes = file.test_page_writes.load(.monotonic);
    try file.persistAllocator(&pages, &checkpoint);
    try std.testing.expect(file.test_page_writes.load(.monotonic) - writes <= 5);
    try std.testing.expectEqual(@as(u64, 0), pages.ledger.?.root.deltas);
    try std.testing.expectEqual(@as(u64, 0), pages.ledger.?.root.build_limit);
    try file.publishCheckpoint(checkpoint);
    file.invalidateLedger();
    try std.testing.expect((try file.check()).valid);
}

test "lite allocator v4 owner checkpoints bound metadata work across reopen and cancellation" {
    const CancelWrite = struct {
        var token: ?*maintenance.CancelToken = null;
        fn write(userdata: ?*anyopaque, file: std.Io.File, header: []const u8, data: []const []const u8, splat: usize, offset: u64) std.Io.File.WritePositionalError!usize {
            const written = try std.testing.io.vtable.fileWritePositional(userdata, file, header, data, splat, offset);
            if (token) |cancel| cancel.request();
            return written;
        }
    };
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "incremental-owner-checkpoint.aflite");
    defer a.free(path);
    {
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .indexed_reclamation = true, .no_sync = true });
        defer file.close();
        file.reserve_retirement_capacity = true;
        var pages = try file.pageAllocatorFromFreeMap(file.activeCheckpoint());
        defer pages.deinit();
        for (0..20_000) |_| {
            const page = try pages.allocate();
            try pages.writePage(page, .data, "raw block");
        }
        var checkpoint = file.activeCheckpoint();
        checkpoint.commit_sequence += 1;
        checkpoint.free_map_root_page = try pages.allocate();
        try file.persistAllocator(&pages, &checkpoint);
        try file.publishCheckpoint(checkpoint);
        const writes = file.test_page_writes.load(.monotonic);
        _ = try file.reclaimPages(1);
        try std.testing.expect(file.ledger.?.root.build_limit != 0);
        // One counter chunk plus bounded service/journal publication, regardless
        // of the twenty-page counter table that remains to be snapshotted.
        try std.testing.expect(file.test_page_writes.load(.monotonic) - writes < 16);
    }
    var vtable = std.testing.io.vtable.*;
    vtable.fileWritePositional = CancelWrite.write;
    const io = std.Io{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    var file = try NativeFile.openWithIo(a, io, path, .{ .no_sync = true });
    defer file.close();
    file.reserve_retirement_capacity = true;
    const before = file.activeCheckpoint();
    const bytes = (try file.file.stat(std.testing.io)).size;
    var cancel = maintenance.CancelToken{};
    cancel.request();
    try std.testing.expectError(error.MaintenanceCanceled, file.reclaimPagesWithCancel(1, &cancel));
    try std.testing.expectEqualDeep(before, file.activeCheckpoint());
    try std.testing.expectEqual(bytes, (try file.file.stat(std.testing.io)).size);
    cancel.requested.store(false, .release);
    CancelWrite.token = &cancel;
    defer CancelWrite.token = null;
    try std.testing.expectError(error.MaintenanceCanceled, file.reclaimPagesWithCancel(1, &cancel));
    CancelWrite.token = null;
    try std.testing.expectEqualDeep(before, file.activeCheckpoint());
    try std.testing.expectEqual(bytes, (try file.file.stat(io)).size);
    try std.testing.expect(!file.checkpoint_publication_uncertain);
    cancel.requested.store(false, .release);
    file.invalidateLedger();
    const reads = file.test_page_reads.load(.monotonic);
    file.test_cancel_on_read = &cancel;
    defer file.test_cancel_on_read = null;
    try std.testing.expectError(error.MaintenanceCanceled, file.reclaimPagesWithCancel(1, &cancel));
    file.test_cancel_on_read = null;
    try std.testing.expect(file.test_page_reads.load(.monotonic) - reads <= 2);
    try std.testing.expectEqualDeep(before, file.activeCheckpoint());
    cancel.requested.store(false, .release);
    const remaining = (try file.loadLedger(before)).root.build_end;
    try file.putDocumentBatch(&.{.{ .key = "live", .value = "concurrent change" }});
    try std.testing.expectEqual(@max(@as(u64, 1), remaining -| (2 * ((file.maxPagePayloadBytes() - 24) / 4))), file.ledger.?.root.build_end);
    var batches: usize = 0;
    while (try file.retirementNeedsService()) : (batches += 1) {
        try std.testing.expect(batches < 500);
        _ = try file.reclaimPagesWithCancel(1, &cancel);
    }
    try std.testing.expect(batches > 16);
    try std.testing.expect((try file.check()).valid);
    const value = (try file.getDocumentAlloc(a, "live")).?;
    defer a.free(value);
    try std.testing.expectEqualStrings("concurrent change", value);
    // Private catch-up may have a sequence newer than the live generation.
    // Normalization must finish an active builder and persist rebased events.
    file.ledger.?.force_checkpoint = true;
    _ = try file.reclaimPages(1);
    try std.testing.expect(file.ledger.?.root.build_limit != 0);
    try file.preparePublicationSequence(1);
    file.invalidateLedger();
    try std.testing.expect((try file.check()).valid);
}

/// Per-operation admission for newly written external payload pages. Catalog
/// records and tree navigation pages remain eligible for caching. Bypassing
/// admission still invalidates both cached bytes and links for reused page IDs.
pub const WriteOptions = struct {
    payload_cache: enum { normal, cold_sequential } = .normal,
};

pub const magic = "AFLITE\x03P";
const unpacked_v3_magic = "AFLITE\x03N";
const indexed_v4_magic = "AFLITE\x04P";
pub const format_version: u32 = 3;
pub const indexed_format_version: u32 = 4;
pub const default_page_size: u32 = 4096;

test "lite reclamation catalog snapshots visit live indexes rather than history" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "reclamation-catalog-scan.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    for (0..512) |revision| {
        var value: [32]u8 = undefined;
        try file.putCatalogRecord("key", try std.fmt.bufPrint(&value, "{d}", .{revision}));
        try file.putIndexCatalogRecord("index", try std.fmt.bufPrint(&value, "{d}", .{revision}));
    }
    try file.putCatalogRecord("deleted", "gone");
    try file.deleteCatalogRecord("deleted");
    file.page_cache.clear(alloc);
    file.test_page_reads.store(0, .monotonic);
    const metadata = try file.snapshotCatalogRecordsAlloc(alloc);
    defer NativeFile.freeSnapshotCatalogRecords(alloc, metadata);
    const indexes = try file.snapshotIndexCatalogRecordsAlloc(alloc);
    defer NativeFile.freeSnapshotCatalogRecords(alloc, indexes);
    try std.testing.expectEqual(@as(usize, 1), metadata.len);
    try std.testing.expectEqual(@as(usize, 1), indexes.len);
    try std.testing.expectEqualStrings("511", metadata[0].value);
    try std.testing.expectEqualStrings("511", indexes[0].value);
    try std.testing.expect(file.test_page_reads.load(.monotonic) < 32);
}
test "lite allocator v4 overwrite reuses pages across reopen" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "allocator-v4.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .indexed_reclamation = true });
    defer file.close();
    const value = try a.alloc(u8, 16384);
    defer a.free(value);
    @memset(value, 'x');
    for (0..80) |_| {
        try file.putDocument("doc", value);
        try file.putCatalogRecord("catalog", value);
    }
    const before = file.activeCheckpoint().page_count;
    for (0..80) |_| {
        try file.putDocument("doc", value);
        try file.putCatalogRecord("catalog", value);
    }
    try std.testing.expect(file.activeCheckpoint().page_count < before + 32);
    const got = (try file.getDocumentAlloc(a, "doc")).?;
    defer a.free(got);
    try std.testing.expectEqualSlices(u8, value, got);
    file.close();
    file = try NativeFile.openWithIo(a, std.testing.io, path, .{});
    for (0..20) |_| try file.putDocument("doc", value);
    const reopened = (try file.getDocumentAlloc(a, "doc")).?;
    defer a.free(reopened);
    try std.testing.expectEqualSlices(u8, value, reopened);
}
test "lite allocator v4 long keys shared values incremental deletion and vacuum" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "reuse-shared.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .indexed_reclamation = true, .no_sync = true });
    defer file.close();
    var key: [1024]u8 = @splat('k');
    const value: [32768]u8 = @splat('v');
    for (0..150) |i| {
        _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        try file.putDocument(&key, &value);
    }
    for (0..150) |i| {
        _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        try file.putDocument(&key, "small");
    }
    try std.testing.expect((try file.check()).valid);
    try file.putIndexCatalogRecord("first", &value);
    try file.renameIndexCatalogRecord("first", "second");
    try file.appendIndexCatalogRecord("second", "tail");
    try file.deleteIndexCatalogRecord("first");
    for (0..150) |i| {
        _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        try file.deleteDocument(&key);
    }
    var work: usize = 0;
    while (try file.retirementNeedsService()) : (work += 1) {
        try std.testing.expect(work < 1000);
        try std.testing.expect(try file.reclaimPages(8) <= 8);
    }
    const got = (try file.getIndexCatalogRecordAlloc(a, "second")).?;
    defer a.free(got);
    try std.testing.expectEqualSlices(u8, &value, got[0..value.len]);
    try std.testing.expectEqualStrings("tail", got[value.len..]);
    try std.testing.expect((try file.check()).valid);
    _ = try file.vacuum();
    try std.testing.expect(file.header.indexed_reclamation);
    try std.testing.expect((try file.check()).valid);
}

test "lite allocator v4 external readers and deferred durability fence reuse" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "reuse-fences.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .indexed_reclamation = true });
    defer file.close();
    try file.putDocument("doc", "durable");
    var external = try NativeFile.openWithIo(a, std.testing.io, path, .{ .read_only = true });
    var external_open = true;
    defer if (external_open) external.close();
    for (0..40) |_| try file.putDocument("doc", "new");
    const old = (try external.getDocumentAlloc(a, "doc")).?;
    defer a.free(old);
    try std.testing.expectEqualStrings("durable", old);
    external.close();
    external_open = false;
    for (0..20) |_| try file.putDocument("doc", "barrier");
    const durable = file.header;
    for (0..40) |_| {
        try file.beginTransaction();
        errdefer file.abortTransaction();
        try file.putDocument("doc", "unsynced");
        try file.commitTransactionWithDurability(false);
    }
    try std.testing.expect(file.durable_header != null);
    var pinned: NativeFile = try NativeFile.openWithIo(a, std.testing.io, path, .{ .read_only = true });
    defer pinned.close();
    try std.testing.expectEqual(durable.checkpoints[durable.active_checkpoint].commit_sequence, pinned.activeCheckpoint().commit_sequence);
    const persisted = (try pinned.getDocumentAlloc(a, "doc")).?;
    defer a.free(persisted);
    try std.testing.expectEqualStrings("barrier", persisted);
    try file.sync();
    try std.testing.expect((try file.check()).valid);
}

test "lite allocator v4 rollback and recovery retain the fallback root" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "reuse-recovery.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .indexed_reclamation = true, .no_sync = true });
    defer file.close();
    for (0..100) |_| try file.putDocument("doc", "stable");
    const checkpoint = file.activeCheckpoint();
    try file.beginTransaction();
    try file.putDocument("doc", "discarded");
    _ = try file.materializeTransactionCheckpoint();
    file.abortTransaction();
    try std.testing.expect(std.meta.eql(checkpoint, file.activeCheckpoint()));
    try std.testing.expect((try file.check()).valid);
    try file.putDocument("doc", "newest");
    const damaged = checkpointOffset(file.header.active_checkpoint) + checkpoint_slot_checksum_offset;
    var bad: [4]u8 = @splat(0);
    try file.file.writePositionalAll(file.runtimeIo(), &bad, damaged);
    file.close();
    file = try NativeFile.openWithIo(a, std.testing.io, path, .{ .no_sync = true });
    const recovered = (try file.getDocumentAlloc(a, "doc")).?;
    defer a.free(recovered);
    try std.testing.expectEqualStrings("stable", recovered);
    for (0..40) |_| try file.putDocument("doc", "recovered");
    try std.testing.expect((try file.check()).valid);
}

test "lite allocator v4 rejects a durable free bit for a live page" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "reuse-corruption.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .indexed_reclamation = true, .no_sync = true });
    defer file.close();
    try file.putDocument("doc", "safe");
    var checkpoint = file.activeCheckpoint();
    const state = try file.loadLedger(checkpoint);
    try std.testing.expect(try state.release(checkpoint.document_index_root_page));
    var pages = PageAllocator{ .file = &file, .free_pages = &.{}, .next_page_id = checkpoint.page_count, .ledger = state, .can_reuse = false };
    defer pages.deinit();
    checkpoint.commit_sequence += 1;
    checkpoint.free_map_root_page = try pages.allocate();
    try file.persistAllocator(&pages, &checkpoint);
    try file.publishCheckpoint(checkpoint);
    const report = try file.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("invalid_allocator", report.issue.?);
    file.free_pages_verified = false;
    try std.testing.expectError(error.InvalidNativeAllocator, file.putDocument("other", "refused"));
}

test "lite allocator v4 allocation failures release every private owner" {
    const Runner = struct {
        fn run(a: Allocator) !void {
            const backing = std.testing.allocator;
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            const path = try testPath(backing, tmp, "reuse-allocation-failures.aflite");
            defer backing.free(path);
            var file = try NativeFile.createWithIo(backing, std.testing.io, path, .{ .indexed_reclamation = true, .no_sync = true });
            file.invalidateLedger();
            file.page_cache.clear(backing);
            file.page_cache_enabled.store(false, .monotonic);
            file.allocator = a;
            defer {
                file.abortTransaction();
                file.allocator = backing;
                file.close();
            }
            const value: [8192]u8 = @splat('v');
            try file.putDocument("doc", &value);
            try file.beginTransaction();
            errdefer file.abortTransaction();
            try file.putDocument("doc", "updated");
            try file.putIndexCatalogRecord("index", &value);
            try file.appendIndexCatalogRecord("index", "tail");
            try file.commitTransaction();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "lite allocator v4 large deletes enqueue bounded foreground work" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "reuse-large-delete.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .indexed_reclamation = true, .no_sync = true });
    defer file.close();
    const value = try a.alloc(u8, 4 * 1024 * 1024);
    defer a.free(value);
    @memset(value, 'v');
    try file.putDocument("large", value);
    file.test_page_reads.store(0, .monotonic);
    try file.deleteDocument("large");
    try std.testing.expect(file.test_page_reads.load(.monotonic) < 16);
    var rounds: usize = 0;
    while (try file.retirementNeedsService()) : (rounds += 1) {
        try std.testing.expect(rounds < 1000);
        try std.testing.expect(try file.reclaimPages(8) <= 8);
    }
    try std.testing.expect(rounds > 100);
    try std.testing.expect(file.ledger.?.free_pages > 700);
    try std.testing.expect(file.ledger.?.collected > 1000);
    try std.testing.expect((try file.check()).valid);
}

pub const header_size: usize = 4096;
pub const checkpoint_slot_count = 2;
pub const checkpoint_slot_size: usize = 72;
pub const page_magic = "AFLP";
pub const page_header_size: usize = 16;

const magic_offset: usize = 0;
const version_offset: usize = 8;
const page_size_offset: usize = 12;
const header_size_offset: usize = 16;
const active_checkpoint_offset: usize = 20;
const checkpoint_slots_offset: usize = 64;
const checkpoint_slots_end: usize = checkpoint_slots_offset + checkpoint_slot_count * checkpoint_slot_size;
const checkpoint_slot_payload_size: usize = 64;
const checkpoint_slot_checksum_offset: usize = checkpoint_slot_payload_size;
const header_checksum_offset: usize = header_size - 4;
const page_crc_offset: usize = 12;

pub const PageKind = enum(u8) {
    data = 1,
    catalog = 2,
    document = 3,
    value = 4,
    free_map = 5,
    document_index = 6,
    value_extent = 7,
    catalog_index = 8,
    record_bundle = 9,
    allocator = 10,
    key = 11,
};

// Record references reserve the high bit and a 16-bit byte offset. Ordinary
// page IDs retain their existing encoding; packed references never name value
// or index pages. Bounds and record boundaries are checked before decoding.
const packed_record_flag: u64 = @as(u64, 1) << 63;
const packed_page_mask: u64 = (@as(u64, 1) << 47) - 1;
fn physicalPage(reference: u64) u64 {
    return if (reference & packed_record_flag != 0) reference & packed_page_mask else reference;
}

fn recordWalkLimit(checkpoint: CheckpointSlot, page_size: u32) u64 {
    return checkpoint.page_count *| @as(u64, page_size / 4);
}

const catalog_key_len_mask: u32 = 0x00ff_ffff;
const catalog_delete_flag: u32 = 1 << 31;
const catalog_external_value_flag: u32 = 1 << 30;
const document_delete_flag: u8 = 1 << 0;
const document_external_value_flag: u8 = 1 << 1;
const document_namespace_link_flag: u8 = 1 << 2;
const namespace_directory_key = "\x00antfly.document_namespaces.v1";
pub const secret_catalog_prefix = "\x00antfly.secrets.v1/";
const namespace_directory_magic = "AFNSIDX2";
const namespace_directory_snapshot_interval: u16 = 256;
const value_page_header_size: usize = 8;
const free_map_format_version: u32 = 1;
const free_map_header_size: usize = 16;
const document_index_magic = "AFDIDX02";
const document_index_header_size: usize = 12;

const DocumentIndexNodeKind = enum(u8) {
    leaf = 1,
    internal = 2,
};

// Bound every encoded key slot so an insertion can always split into two
// pages. Long keys borrow their bytes from immutable catalog/document records;
// separators retain the same record reference through copy-on-write splits.
const index_inline_key_limit = 512;
const index_external_key_marker = std.math.maxInt(u16);

const DocumentIndexNode = struct {
    kind: DocumentIndexNodeKind,
    keys: [][]u8,
    pointers: []u64,
    key_pages: ?[]u64 = null,

    pub fn deinit(self: *DocumentIndexNode, allocator: Allocator) void {
        for (self.keys) |key| allocator.free(key);
        allocator.free(self.keys);
        allocator.free(self.pointers);
        if (self.key_pages) |pages| allocator.free(pages);
        self.* = undefined;
    }
};

pub const DocumentIndexEntry = struct {
    key: []u8,
    document_page_id: u64,

    pub fn deinit(self: *DocumentIndexEntry, allocator: Allocator) void {
        allocator.free(self.key);
        self.* = undefined;
    }
};

/// Validated immutable index page. Inline keys borrow the retained page;
/// overflow comparisons use one reusable record buffer, never a key array.
const IndexReadFrame = struct {
    raw: ?[]u8 = null,
    payload: []const u8 = &.{},
    offsets: std.ArrayListUnmanaged(u16) = .empty,
    kind: DocumentIndexNodeKind = .leaf,
    position: usize = 0,
    overflow: ?[]u8 = null,
    overflow_reference: u64 = 0,
    overflow_key: []const u8 = &.{},

    pub fn deinit(self: *IndexReadFrame, allocator: Allocator) void {
        if (self.raw) |raw| allocator.free(raw);
        if (self.overflow) |raw| allocator.free(raw);
        self.offsets.deinit(allocator);
    }

    fn load(self: *IndexReadFrame, file: *NativeFile, checkpoint: CheckpointSlot, page: u64) !void {
        self.payload = &.{};
        if (self.raw == null) self.raw = try file.allocator.alloc(u8, file.header.page_size);
        const payload = try decodePagePayload(try file.readPageInto(page, checkpoint, self.raw.?), .document_index);
        try self.decode(file.allocator, payload);
    }

    fn decode(self: *IndexReadFrame, allocator: Allocator, payload: []const u8) !void {
        self.payload = &.{};
        self.offsets.clearRetainingCapacity();
        self.position = 0;
        if (payload.len < document_index_header_size or !std.mem.eql(u8, payload[0..8], document_index_magic) or payload[9] != 0)
            return error.InvalidDocumentIndex;
        self.kind = switch (payload[8]) {
            1 => .leaf,
            2 => .internal,
            else => return error.InvalidDocumentIndex,
        };
        const count = std.mem.readInt(u16, payload[10..12], .little);
        var pos: usize = document_index_header_size + @as(usize, if (self.kind == .internal) 8 else 0);
        if (pos > payload.len or count > (payload.len - pos) / 10 or (self.kind == .leaf and count == 0)) return error.InvalidDocumentIndex;
        try self.offsets.ensureTotalCapacity(allocator, count);
        var previous_inline: ?[]const u8 = null;
        for (0..count) |_| {
            if (pos + 10 > payload.len) return error.InvalidDocumentIndex;
            const offset: u16 = @intCast(pos);
            const len = std.mem.readInt(u16, payload[pos..][0..2], .little);
            pos += 10;
            if (len == index_external_key_marker) {
                if (pos + 8 > payload.len or std.mem.readInt(u64, payload[pos..][0..8], .little) == 0) return error.InvalidDocumentIndex;
                pos += 8;
                previous_inline = null;
            } else {
                if (len > index_inline_key_limit or pos + len > payload.len) return error.InvalidDocumentIndex;
                const key_bytes = payload[pos..][0..len];
                if (previous_inline) |previous| if (std.mem.order(u8, previous, key_bytes) != .lt) return error.InvalidDocumentIndex;
                previous_inline = key_bytes;
                pos += len;
            }
            self.offsets.appendAssumeCapacity(offset);
        }
        if (pos != payload.len) return error.InvalidDocumentIndex;
        self.payload = payload;
    }

    fn pointerCount(self: *const IndexReadFrame) usize {
        return self.offsets.items.len + @as(usize, if (self.kind == .internal) 1 else 0);
    }

    fn pointer(self: *const IndexReadFrame, index: usize) u64 {
        if (self.kind == .internal and index == 0)
            return std.mem.readInt(u64, self.payload[document_index_header_size..][0..8], .little);
        const slot = if (self.kind == .internal) index - 1 else index;
        const offset = @as(usize, self.offsets.items[slot]) + 2;
        return std.mem.readInt(u64, self.payload[offset..][0..8], .little);
    }

    fn key(self: *IndexReadFrame, file: *NativeFile, checkpoint: CheckpointSlot, index: usize) ![]const u8 {
        const offset: usize = self.offsets.items[index];
        const len = std.mem.readInt(u16, self.payload[offset..][0..2], .little);
        if (len != index_external_key_marker) return self.payload[offset + 10 ..][0..len];
        const reference = std.mem.readInt(u64, self.payload[offset + 10 ..][0..8], .little);
        if (reference == self.overflow_reference) return self.overflow_key;
        // Invalidate before a fallible read: an error must not leave a cached
        // reference pointing at overwritten or partially validated bytes.
        self.overflow_reference = 0;
        self.overflow_key = &.{};
        if (self.overflow == null) self.overflow = try file.allocator.alloc(u8, file.header.page_size);
        const raw = try file.readPageInto(reference, checkpoint, self.overflow.?);
        const bytes = switch (raw[4]) {
            @backingInt(PageKind.catalog) => (try decodeCatalogEntry(try decodePagePayload(raw, .catalog))).key,
            @backingInt(PageKind.document) => (try decodeDocumentEntry(try decodePagePayload(raw, .document))).key,
            @backingInt(PageKind.key) => try NativeFile.decodeKey(try decodePagePayload(raw, .key)),
            else => return error.InvalidDocumentIndex,
        };
        if (bytes.len <= index_inline_key_limit) return error.InvalidDocumentIndex;
        self.overflow_key = bytes;
        self.overflow_reference = reference;
        return bytes;
    }

    fn keyReference(self: *const IndexReadFrame, index: usize) u64 {
        const offset: usize = self.offsets.items[index];
        if (std.mem.readInt(u16, self.payload[offset..][0..2], .little) != index_external_key_marker) return 0;
        return std.mem.readInt(u64, self.payload[offset + 10 ..][0..8], .little);
    }

    fn bound(self: *IndexReadFrame, file: *NativeFile, checkpoint: CheckpointSlot, wanted: []const u8, upper: bool) !usize {
        var low: usize = 0;
        var high = self.offsets.items.len;
        while (low < high) {
            if (builtin.is_test) _ = file.test_index_comparisons.fetchAdd(1, .monotonic);
            const mid = low + (high - low) / 2;
            const order = std.mem.order(u8, try self.key(file, checkpoint, mid), wanted);
            if (order == .lt or (upper and order == .eq)) low = mid + 1 else high = mid;
        }
        return low;
    }

    // Sorted requests normally stay at the same separator or advance one
    // slot. Gallop from the previous bound, then binary-search the gap; this
    // avoids re-reading overflow separators for every dense request while
    // sparse batches still skip unrelated keys logarithmically.
    fn boundFrom(self: *IndexReadFrame, file: *NativeFile, checkpoint: CheckpointSlot, wanted: []const u8, upper: bool, start: usize) !usize {
        const count = self.offsets.items.len;
        if (start == count) return count;
        const first_order = std.mem.order(u8, try self.key(file, checkpoint, start), wanted);
        if (first_order == .gt or (!upper and first_order == .eq)) return start;
        var low = start + 1;
        var high = count;
        var stride: usize = 1;
        while (low < count) : (stride *= 2) {
            const probe = @min(start + stride, count - 1);
            const order = std.mem.order(u8, try self.key(file, checkpoint, probe), wanted);
            if (order == .gt or (!upper and order == .eq)) {
                high = probe;
                break;
            }
            low = probe + 1;
        }
        while (low < high) {
            const mid = low + (high - low) / 2;
            const order = std.mem.order(u8, try self.key(file, checkpoint, mid), wanted);
            if (order == .lt or (upper and order == .eq)) low = mid + 1 else high = mid;
        }
        return low;
    }
};

/// Reuse buffers by depth across siblings and seeks. Frame addresses remain
/// stable when the path grows, including during recursive batch traversal.
const IndexReadPath = struct {
    frames: std.ArrayListUnmanaged(*IndexReadFrame) = .empty,

    pub fn deinit(self: *IndexReadPath, allocator: Allocator) void {
        for (self.frames.items) |frame| {
            frame.deinit(allocator);
            allocator.destroy(frame);
        }
        self.frames.deinit(allocator);
    }

    fn load(self: *IndexReadPath, file: *NativeFile, checkpoint: CheckpointSlot, depth: usize, page: u64) !*IndexReadFrame {
        if (depth > 64) return error.InvalidDocumentIndex;
        if (depth == self.frames.items.len) {
            const frame = try file.allocator.create(IndexReadFrame);
            errdefer file.allocator.destroy(frame);
            frame.* = .{};
            try self.frames.append(file.allocator, frame);
        }
        const frame = self.frames.items[depth];
        try frame.load(file, checkpoint, page);
        return frame;
    }
};

/// Cursor over one pinned index root. Page views and overflow scratch are
/// reused by tree depth; each returned key remains independently owned.
pub const DocumentIndexCursor = struct {
    file: *NativeFile,
    checkpoint: CheckpointSlot,
    path: IndexReadPath = .{},
    depth: usize = 0,
    key_allocator: Allocator,

    pub fn init(file: *NativeFile, checkpoint: CheckpointSlot) DocumentIndexCursor {
        return initWithKeyAllocator(file, checkpoint, file.allocator);
    }

    /// Returned entries own their keys through key_allocator. Traversal scratch
    /// remains file-owned; callers must free entries with the supplied allocator.
    pub fn initWithKeyAllocator(file: *NativeFile, checkpoint: CheckpointSlot, key_allocator: Allocator) DocumentIndexCursor {
        return .{ .file = file, .checkpoint = checkpoint, .key_allocator = key_allocator };
    }

    pub fn deinit(self: *DocumentIndexCursor) void {
        self.path.deinit(self.file.allocator);
        self.* = undefined;
    }

    pub fn first(self: *DocumentIndexCursor) !?DocumentIndexEntry {
        self.clear();
        errdefer self.clear();
        if (self.checkpoint.document_index_root_page == 0) return null;
        try self.descendExtreme(self.checkpoint.document_index_root_page, true);
        return try self.currentEntry();
    }

    pub fn last(self: *DocumentIndexCursor) !?DocumentIndexEntry {
        self.clear();
        errdefer self.clear();
        if (self.checkpoint.document_index_root_page == 0) return null;
        try self.descendExtreme(self.checkpoint.document_index_root_page, false);
        return try self.currentEntry();
    }

    pub fn seekAtOrAfter(self: *DocumentIndexCursor, key: []const u8, strict: bool) !?DocumentIndexEntry {
        self.clear();
        errdefer self.clear();
        var page = self.checkpoint.document_index_root_page;
        while (page != 0) {
            const frame = try self.push(page);
            frame.position = try frame.bound(self.file, self.checkpoint, key, if (frame.kind == .leaf) strict else true);
            if (frame.kind == .leaf) {
                if (frame.position < frame.offsets.items.len) return try self.currentEntry();
                return try self.next();
            }
            page = frame.pointer(frame.position);
        }
        return null;
    }

    pub fn seekAtOrBefore(self: *DocumentIndexCursor, key: []const u8, strict: bool) !?DocumentIndexEntry {
        self.clear();
        errdefer self.clear();
        var page = self.checkpoint.document_index_root_page;
        while (page != 0) {
            const frame = try self.push(page);
            const bound = try frame.bound(self.file, self.checkpoint, key, if (frame.kind == .leaf) !strict else true);
            if (frame.kind == .leaf) {
                frame.position = if (bound == 0) 0 else bound - 1;
                if (bound > 0) return try self.currentEntry();
                return try self.prev();
            }
            frame.position = bound;
            page = frame.pointer(bound);
        }
        return null;
    }

    pub fn next(self: *DocumentIndexCursor) !?DocumentIndexEntry {
        errdefer self.clear();
        if (self.depth == 0) return null;
        const leaf = self.path.frames.items[self.depth - 1];
        if (leaf.kind != .leaf) return error.InvalidDocumentIndex;
        if (leaf.position + 1 < leaf.offsets.items.len) {
            leaf.position += 1;
            return try self.currentEntry();
        }
        self.depth -= 1;
        while (self.depth > 0) {
            const parent = self.path.frames.items[self.depth - 1];
            if (parent.kind != .internal) return error.InvalidDocumentIndex;
            if (parent.position + 1 < parent.pointerCount()) {
                parent.position += 1;
                try self.descendExtreme(parent.pointer(parent.position), true);
                return try self.currentEntry();
            }
            self.depth -= 1;
        }
        return null;
    }

    pub fn prev(self: *DocumentIndexCursor) !?DocumentIndexEntry {
        errdefer self.clear();
        if (self.depth == 0) return null;
        const leaf = self.path.frames.items[self.depth - 1];
        if (leaf.kind != .leaf) return error.InvalidDocumentIndex;
        if (leaf.position > 0 and leaf.position <= leaf.offsets.items.len) {
            leaf.position -= 1;
            return try self.currentEntry();
        }
        self.depth -= 1;
        while (self.depth > 0) {
            const parent = self.path.frames.items[self.depth - 1];
            if (parent.kind != .internal) return error.InvalidDocumentIndex;
            if (parent.position > 0) {
                parent.position -= 1;
                try self.descendExtreme(parent.pointer(parent.position), false);
                return try self.currentEntry();
            }
            self.depth -= 1;
        }
        return null;
    }

    fn push(self: *DocumentIndexCursor, page: u64) !*IndexReadFrame {
        const frame = try self.path.load(self.file, self.checkpoint, self.depth, page);
        self.depth += 1;
        return frame;
    }

    fn descendExtreme(self: *DocumentIndexCursor, root: u64, first_key: bool) !void {
        var page = root;
        while (page != 0) {
            const frame = try self.push(page);
            frame.position = if (first_key) 0 else frame.pointerCount() - 1;
            page = if (frame.kind == .internal) frame.pointer(frame.position) else 0;
        }
    }

    fn currentEntry(self: *DocumentIndexCursor) !?DocumentIndexEntry {
        if (self.depth == 0) return null;
        const frame = self.path.frames.items[self.depth - 1];
        if (frame.kind != .leaf or frame.position >= frame.offsets.items.len) return null;
        return .{
            .key = try self.key_allocator.dupe(u8, try frame.key(self.file, self.checkpoint, frame.position)),
            .document_page_id = frame.pointer(frame.position),
        };
    }

    fn clear(self: *DocumentIndexCursor) void {
        self.depth = 0;
    }
};

/// A cursor-local view of one immutable physical record page. Decode and check
/// its checksum once, index bundle boundaries once, and borrow payload slices
/// until the next physical page is loaded. Memory is bounded by page size.
/// The caller must keep the file generation and checkpoint pinned for its life.
pub const RecordPageReader = struct {
    raw: ?[]u8 = null,
    payload: []const u8 = &.{},
    page_id: u64 = 0,
    kind: PageKind = .data,
    offsets: std.ArrayListUnmanaged(u16) = .empty,

    pub fn deinit(self: *RecordPageReader, allocator: Allocator) void {
        if (self.raw) |raw| allocator.free(raw);
        self.offsets.deinit(allocator);
        self.* = .{};
    }

    pub fn read(self: *RecordPageReader, file: *NativeFile, allocator: Allocator, checkpoint: CheckpointSlot, reference: u64, expected: PageKind) ![]const u8 {
        const page = physicalPage(reference);
        if (page == 0 or page >= checkpoint.page_count) return error.InvalidPageId;
        const bundled = reference & packed_record_flag != 0;
        const kind: PageKind = if (bundled) .record_bundle else expected;
        if (self.page_id != page) {
            // Invalidate before any fallible load, so a failed validation can
            // never leave a reusable, partially validated view.
            self.page_id = 0;
            self.payload = &.{};
            self.offsets.clearRetainingCapacity();
            if (self.raw == null) self.raw = try allocator.alloc(u8, file.header.page_size);
            _ = try file.readPageInto(page, checkpoint, self.raw.?);
            self.payload = try decodePagePayload(self.raw.?, kind);
            if (bundled) {
                var offset: usize = 0;
                while (offset < self.payload.len) {
                    const record = try packedRecordAtOffset(self.payload, offset);
                    try self.offsets.append(allocator, @intCast(offset));
                    offset += 4 + record.bytes.len;
                }
            }
            self.kind = kind;
            self.page_id = page;
        }
        if (self.kind != kind) return error.InvalidNativePageKind;
        if (!bundled) return self.payload;
        const wanted: u16 = @intCast((reference >> 47) & 0xffff);
        var low: usize = 0;
        var high = self.offsets.items.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            if (self.offsets.items[mid] < wanted) low = mid + 1 else high = mid;
        }
        if (low == self.offsets.items.len or self.offsets.items[low] != wanted) return error.InvalidPageId;
        const record = try packedRecordAtOffset(self.payload, wanted);
        if (record.kind != expected) return error.InvalidNativePageKind;
        return record.bytes;
    }

    /// Validate the record and its key without touching external value pages.
    /// Legacy v3 indexes may still reference tombstones.
    pub fn documentIsLive(self: *RecordPageReader, file: *NativeFile, allocator: Allocator, checkpoint: CheckpointSlot, indexed: DocumentIndexEntry) !bool {
        const entry = try decodeDocumentEntry(try self.read(file, allocator, checkpoint, indexed.document_page_id, .document));
        if (!std.mem.eql(u8, entry.key, indexed.key)) return error.InvalidDocumentIndex;
        return !entry.is_delete;
    }

    pub fn documentValueAlloc(self: *RecordPageReader, file: *NativeFile, allocator: Allocator, checkpoint: CheckpointSlot, indexed: DocumentIndexEntry) !?[]u8 {
        const entry = try decodeDocumentEntry(try self.read(file, allocator, checkpoint, indexed.document_page_id, .document));
        if (!std.mem.eql(u8, entry.key, indexed.key)) return error.InvalidDocumentIndex;
        if (entry.is_delete) return null;
        return if (entry.external_value_root_page != 0)
            try file.readValuePagesAtCheckpointAlloc(allocator, entry.external_value_root_page, entry.external_value_len, checkpoint)
        else
            try allocator.dupe(u8, entry.value);
    }
};

/// Ordered live catalog keys at a pinned checkpoint. The prefix is borrowed
/// for the cursor lifetime. Callers keep the checkpoint pinned and fence
/// generation replacement (vacuum) while using this cursor; ordinary copy-on-write
/// commits may continue concurrently.
pub const CatalogCursor = struct {
    records: RecordPageReader = .{},
    index: DocumentIndexCursor,
    prefix: []const u8,
    started: bool = false,
    done: bool = false,
    immediate_children_only: bool = false,
    start_after: ?[]const u8 = null,

    pub fn deinit(self: *CatalogCursor) void {
        self.records.deinit(self.index.file.allocator);
        self.index.deinit();
        self.* = undefined;
    }

    pub fn nextRecordAlloc(self: *CatalogCursor, allocator: Allocator) !?OwnedCatalogRecord {
        const file = self.index.file;
        const key = (try self.next()) orelse return null;
        defer file.allocator.free(key.key);
        const entry = try decodeCatalogEntry(try self.records.read(file, file.allocator, self.index.checkpoint, key.page_id, .catalog));
        const value = try file.catalogEntryValueAtCheckpointAlloc(allocator, entry, self.index.checkpoint);
        errdefer allocator.free(value);
        return .{ .key = try allocator.dupe(u8, key.key), .value = value };
    }

    /// The caller owns the returned key, allocated with the native file allocator.
    pub fn next(self: *CatalogCursor) !?OwnedCatalogKey {
        if (self.done) return null;
        const file = self.index.file;
        var candidate = if (self.started) try self.index.next() else if (self.start_after) |key| try self.index.seekAtOrAfter(key, true) else try self.index.seekAtOrAfter(self.prefix, false);
        self.started = true;
        while (candidate) |owned| {
            var entry = owned;
            defer entry.deinit(file.allocator);
            if (!std.mem.startsWith(u8, entry.key, self.prefix)) break;
            if (self.immediate_children_only) {
                if (std.mem.indexOfScalar(u8, entry.key[self.prefix.len..], '/')) |slash| {
                    // '/' is not the maximum byte, so incrementing the final
                    // slash is the exact exclusive upper bound of this subtree.
                    // Seek before decoding any descendant catalog records.
                    const bound = try file.allocator.dupe(u8, entry.key[0 .. self.prefix.len + slash + 1]);
                    defer file.allocator.free(bound);
                    bound[bound.len - 1] += 1;
                    candidate = try self.index.seekAtOrAfter(bound, false);
                    continue;
                }
            }
            const record = try decodeCatalogEntry(try self.records.read(file, file.allocator, self.index.checkpoint, entry.document_page_id, .catalog));
            if (!std.mem.eql(u8, entry.key, record.key)) return error.InvalidDocumentIndex;
            if (record.is_delete) {
                candidate = try self.index.next();
                continue;
            }
            const key = entry.key;
            entry.key = &.{};
            return .{ .key = key, .page_id = entry.document_page_id };
        }
        self.done = true;
        return null;
    }
};

fn recordReferenceLessThan(a: u64, b: u64) bool {
    const pa = physicalPage(a);
    const pb = physicalPage(b);
    return pa < pb or (pa == pb and a < b);
}

fn mutationKeysSorted(mutations: anytype) bool {
    if (mutations.len < 2) return true;
    for (mutations[1..], mutations[0 .. mutations.len - 1]) |right, left| {
        if (std.mem.order(u8, left.key, right.key) == .gt) return false;
    }
    return true;
}

/// Transaction-local copy-on-write B+ tree editor. External keys remain
/// references until compared. Sorted batches seal and release completed subtrees
/// while retaining the active frontier and rebalance neighbors; unsorted batches
/// retain visited nodes until finish. All writes remain private until checkpoint
/// publication, preserving old pages and separators for pinned readers.
const IndexEditor = struct {
    const Key = struct {
        bytes: []const u8,
        page: u64 = 0,

        fn slotSize(self: Key) usize {
            return 10 + (if (self.page != 0) @as(usize, 8) else self.bytes.len);
        }
    };
    const Link = struct { page: u64 = 0, node: ?*Node = null };
    const Node = struct {
        kind: DocumentIndexNodeKind,
        arena: ?std.heap.ArenaAllocator = null,
        previous: ?*Node = null,
        next: ?*Node = null,
        sealed_before: usize = 0,
        compact_at: usize = 128 * 1024,
        page: u64 = 0,
        original_page: u64 = 0,
        dirty: bool = true,
        keys: std.ArrayListUnmanaged(Key) = .empty,
        links: std.ArrayListUnmanaged(Link) = .empty,

        fn size(self: *const Node) usize {
            var result: usize = document_index_header_size + @as(usize, if (self.kind == .internal) 8 else 0);
            for (self.keys.items) |key| result += key.slotSize();
            return result;
        }
    };
    const Split = struct { separator: Key, right: Link };
    const Erased = struct { removed: bool, split: ?Split = null };

    file: *NativeFile,
    checkpoint: CheckpointSlot,
    arena: std.heap.ArenaAllocator,
    root: Link,
    streaming_pages: ?*PageAllocator = null,
    accounting_pages: ?*PageAllocator = null,
    original_page_count: u64,
    nodes: ?*Node = null,

    fn init(file: *NativeFile, checkpoint: CheckpointSlot, root: u64) IndexEditor {
        return .{ .file = file, .checkpoint = checkpoint, .arena = .init(file.allocator), .root = .{ .page = root }, .original_page_count = checkpoint.page_count };
    }

    pub fn deinit(self: *IndexEditor) void {
        while (self.nodes) |node| self.destroyNode(node);
        self.arena.deinit();
    }

    // Sorted batches keep only the active frontier and its rebalance neighbors.
    // Unsorted/single-key writes retain the transaction arena fast path.
    fn useSortedBatch(self: *IndexEditor, pages: *PageAllocator, mutations: anytype) void {
        self.accounting_pages = pages;
        if (mutations.len > 1 and mutationKeysSorted(mutations)) self.streaming_pages = pages;
    }

    fn nodeAllocator(self: *IndexEditor, node: *Node) Allocator {
        return if (node.arena) |*arena| arena.allocator() else self.arena.allocator();
    }

    fn retainKey(self: *IndexEditor, node: *Node, key: Key) !Key {
        return .{ .bytes = if (self.streaming_pages != null) try self.nodeAllocator(node).dupe(u8, key.bytes) else key.bytes, .page = key.page };
    }

    fn newNode(self: *IndexEditor, kind: DocumentIndexNodeKind) !*Node {
        const node = try (if (self.streaming_pages != null) self.file.allocator else self.arena.allocator()).create(Node);
        node.* = .{ .kind = kind };
        if (self.streaming_pages != null) {
            node.arena = .init(self.file.allocator);
            node.next = self.nodes;
            if (self.nodes) |head| head.previous = node;
            self.nodes = node;
        }
        return node;
    }

    fn destroyNode(self: *IndexEditor, node: *Node) void {
        if (node.arena) |*arena| {
            if (node.previous) |previous| previous.next = node.next else self.nodes = node.next;
            if (node.next) |next| next.previous = node.previous;
            arena.deinit();
            self.file.allocator.destroy(node);
        }
    }

    fn releaseSubtree(self: *IndexEditor, node: *Node) void {
        if (node.kind == .internal) for (node.links.items) |link| {
            if (link.node) |child| self.releaseSubtree(child);
        };
        self.destroyNode(node);
    }

    // Rebalancing may revisit a sealed private page. Make queued records/pages
    // readable only then; flushing at every frontier step would defeat packing.
    fn prepareRead(self: *IndexEditor, reference: u64) !void {
        const pages = self.streaming_pages orelse return;
        const page = physicalPage(reference);
        const reused = pages.wasReused(page);
        if (page >= self.original_page_count or reused) {
            try pages.flush();
            self.checkpoint.page_count = pages.next_page_id;
        }
    }

    // A repeatedly merged node must not accumulate dead arena allocations for
    // the entire batch. Repack geometrically, preserving child ownership.
    fn compactNode(self: *IndexEditor, node: *Node) !void {
        const arena = if (node.arena) |*value| value else return;
        const capacity = arena.queryCapacity();
        if (capacity < node.compact_at) return;
        var live = node.keys.items.len * @sizeOf(Key) + node.links.items.len * @sizeOf(Link);
        for (node.keys.items) |key| live += key.bytes.len;
        if (capacity < live * 4) {
            node.compact_at = capacity * 2;
            return;
        }
        var fresh: Node = .{ .kind = node.kind, .arena = .init(self.file.allocator) };
        errdefer fresh.arena.?.deinit();
        const alloc = fresh.arena.?.allocator();
        try fresh.keys.ensureTotalCapacity(alloc, node.keys.items.len);
        for (node.keys.items) |key| fresh.keys.appendAssumeCapacity(.{ .bytes = try alloc.dupe(u8, key.bytes), .page = key.page });
        try fresh.links.appendSlice(alloc, node.links.items);
        arena.deinit();
        node.arena = fresh.arena;
        node.keys = fresh.keys;
        node.links = fresh.links;
        node.compact_at = @max(128 * 1024, node.arena.?.queryCapacity() * 2);
    }

    fn advance(self: *IndexEditor, key: []const u8) !void {
        if (self.streaming_pages == null or self.root.node == null) return;
        try self.sealBefore(self.root.node.?, key, 0);
    }

    fn sealBefore(self: *IndexEditor, node: *Node, key: []const u8, depth: usize) anyerror!void {
        if (depth > 64) return error.InvalidDocumentIndex;
        try self.compactNode(node);
        if (node.kind == .leaf) return;
        const index = try self.bound(node, key, true);
        const stop = index -| 1; // Retain the immediate left sibling for deletion.
        while (node.sealed_before < stop) : (node.sealed_before += 1) {
            const link = &node.links.items[node.sealed_before];
            if (link.node) |child| {
                link.page = try self.flush(link, self.streaming_pages.?, depth + 1);
                self.releaseSubtree(child);
                link.node = null;
            }
        }
        if (node.links.items[index].node) |child| try self.sealBefore(child, key, depth + 1);
    }

    fn load(self: *IndexEditor, link: *Link) !*Node {
        if (link.node) |node| return node;
        try self.prepareRead(link.page);
        const payload = try self.file.readPagePayloadByKindAllocForCheckpoint(self.file.allocator, link.page, .document_index, self.checkpoint);
        defer self.file.allocator.free(payload);
        const node = try self.newNode(.leaf);
        const alloc = self.nodeAllocator(node);
        // Decoded buffers belong to the editor arena; transfer key slices
        // directly instead of allocating another copy of every inline key.
        const decoded = try decodeDocumentIndexNode(alloc, payload);
        node.kind = decoded.kind;
        node.page = link.page;
        node.original_page = link.page;
        node.dirty = false;
        // Reserve the decoded entries and one insertion without geometric
        // over-allocation in the arena's bounded single-key edit scratch.
        try node.keys.ensureTotalCapacityPrecise(alloc, decoded.keys.len + 1);
        try node.links.ensureTotalCapacityPrecise(alloc, decoded.pointers.len + 1);
        for (decoded.keys, decoded.key_pages.?) |key, page| {
            node.keys.appendAssumeCapacity(.{ .bytes = key, .page = page });
        }
        for (decoded.pointers) |page| node.links.appendAssumeCapacity(.{ .page = page });
        link.node = node;
        return node;
    }

    fn resolve(self: *IndexEditor, node: *Node, key: *Key) ![]const u8 {
        if (key.page == 0 or key.bytes.len != 0) return key.bytes;
        try self.prepareRead(key.page);
        const raw = try self.file.readPageAllocForCheckpoint(self.file.allocator, key.page, self.checkpoint);
        defer self.file.allocator.free(raw);
        const bytes = switch (raw[4]) {
            @backingInt(PageKind.catalog) => (try decodeCatalogEntry(try decodePagePayload(raw, .catalog))).key,
            @backingInt(PageKind.document) => (try decodeDocumentEntry(try decodePagePayload(raw, .document))).key,
            @backingInt(PageKind.key) => try NativeFile.decodeKey(try decodePagePayload(raw, .key)),
            else => return error.InvalidDocumentIndex,
        };
        if (bytes.len <= index_inline_key_limit) return error.InvalidDocumentIndex;
        key.bytes = try self.nodeAllocator(node).dupe(u8, bytes);
        return key.bytes;
    }

    fn bound(self: *IndexEditor, node: *Node, key: []const u8, upper: bool) !usize {
        var low: usize = 0;
        var high = node.keys.items.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            const order = std.mem.order(u8, try self.resolve(node, &node.keys.items[mid]), key);
            if (order == .lt or (upper and order == .eq)) low = mid + 1 else high = mid;
        }
        return low;
    }

    fn put(self: *IndexEditor, key: []const u8, page: u64) !void {
        try self.advance(key);
        if (self.root.page == 0 and self.root.node == null) self.root.node = try self.newNode(.leaf);
        if (try self.insert(try self.load(&self.root), key, page, 0)) |split| {
            const root = try self.newNode(.internal);
            try root.keys.append(self.nodeAllocator(root), try self.retainKey(root, split.separator));
            try root.links.appendSlice(self.nodeAllocator(root), &.{ self.root, split.right });
            self.root = .{ .node = root };
        }
    }

    fn insert(self: *IndexEditor, node: *Node, key: []const u8, page: u64, depth: usize) anyerror!?Split {
        if (depth > 64) return error.InvalidDocumentIndex;
        const alloc = self.nodeAllocator(node);
        const index = try self.bound(node, key, node.kind == .internal);
        if (node.kind == .leaf) {
            const replacement = index < node.keys.items.len and std.mem.eql(u8, try self.resolve(node, &node.keys.items[index]), key);
            if (replacement) {
                if (self.accounting_pages) |pages| if (node.links.items[index].page != page) try pages.retire(node.links.items[index].page, .record);
                if (!self.file.header.indexed_reclamation) node.keys.items[index].page = if (key.len > index_inline_key_limit) page else 0;
                node.links.items[index] = .{ .page = page };
            } else {
                const owned = Key{ .bytes = try alloc.dupe(u8, key), .page = if (key.len > index_inline_key_limit) page else 0 };
                try node.keys.insert(alloc, index, owned);
                try node.links.insert(alloc, index, .{ .page = page });
            }
        } else {
            const child = try self.load(&node.links.items[index]);
            if (try self.insert(child, key, page, depth + 1)) |split| {
                try node.keys.insert(alloc, index, try self.retainKey(node, split.separator));
                node.sealed_before = 0;
                try node.links.insert(alloc, index + 1, split.right);
            }
        }
        node.dirty = true;
        if (node.size() <= self.file.maxPagePayloadBytes()) return null;
        return try self.splitNode(node);
    }

    /// Choose a byte-balanced split in linear time without fetching key bytes.
    fn splitNode(self: *IndexEditor, node: *Node) !Split {
        const capacity = self.file.maxPagePayloadBytes();
        const header: usize = document_index_header_size + @as(usize, if (node.kind == .internal) 8 else 0);
        const total = node.size() - header;
        var prefix: usize = 0;
        var best: ?usize = null;
        var imbalance: usize = std.math.maxInt(usize);
        for (node.keys.items, 0..) |key, i| {
            const left = header + prefix;
            const right = header + total - prefix - (if (node.kind == .internal) key.slotSize() else 0);
            if ((node.kind == .internal or i > 0) and left <= capacity and right <= capacity) {
                const delta = if (left > right) left - right else right - left;
                if (delta < imbalance) {
                    best = i;
                    imbalance = delta;
                }
            }
            prefix += key.slotSize();
        }
        const at = best orelse return error.DocumentIndexNodeTooLarge;
        const right = try self.newNode(node.kind);
        const separator = node.keys.items[at];
        const key_start = at + @as(usize, if (node.kind == .internal) 1 else 0);
        const link_start = key_start;
        try right.keys.ensureTotalCapacity(self.nodeAllocator(right), node.keys.items.len - key_start);
        for (node.keys.items[key_start..]) |key| right.keys.appendAssumeCapacity(try self.retainKey(right, key));
        try right.links.appendSlice(self.nodeAllocator(right), node.links.items[link_start..]);
        node.sealed_before = 0;
        node.keys.items.len = at;
        node.links.items.len = at + @as(usize, if (node.kind == .internal) 1 else 0);
        node.dirty = true;
        return .{ .separator = separator, .right = .{ .node = right } };
    }

    fn remove(self: *IndexEditor, key: []const u8) !void {
        try self.advance(key);
        if (self.root.page == 0 and self.root.node == null) return;
        const root = try self.load(&self.root);
        const result = try self.erase(root, key, 0);
        if (!result.removed) return;
        if (result.split) |split| {
            const parent = try self.newNode(.internal);
            try parent.keys.append(self.nodeAllocator(parent), try self.retainKey(parent, split.separator));
            try parent.links.appendSlice(self.nodeAllocator(parent), &.{ self.root, split.right });
            self.root = .{ .node = parent };
        }
        if (root.links.items.len == 0) {
            self.root = .{};
            try self.retireNode(root);
            self.destroyNode(root);
            return;
        }
        var depth: usize = 0;
        while (true) : (depth += 1) {
            if (depth > 64) return error.InvalidDocumentIndex;
            const current = try self.load(&self.root);
            if (current.kind != .internal or current.links.items.len != 1) break;
            self.root = current.links.items[0];
            try self.retireNode(current);
            self.destroyNode(current);
        }
    }

    fn erase(self: *IndexEditor, node: *Node, key: []const u8, depth: usize) anyerror!Erased {
        if (depth > 64) return error.InvalidDocumentIndex;
        const index = try self.bound(node, key, node.kind == .internal);
        if (node.kind == .leaf) {
            if (index == node.keys.items.len or !std.mem.eql(u8, try self.resolve(node, &node.keys.items[index]), key)) return .{ .removed = false };
            if (self.accounting_pages) |pages| try pages.retire(node.links.items[index].page, .record);
            _ = node.keys.orderedRemove(index);
            _ = node.links.orderedRemove(index);
        } else {
            const child = try self.load(&node.links.items[index]);
            const result = try self.erase(child, key, depth + 1);
            if (!result.removed) return result;
            if (result.split) |split| {
                try node.keys.insert(self.nodeAllocator(node), index, try self.retainKey(node, split.separator));
                node.sealed_before = 0;
                try node.links.insert(self.nodeAllocator(node), index + 1, split.right);
            } else if (child.links.items.len == 0) {
                _ = node.links.orderedRemove(index);
                node.sealed_before = 0;
                try self.retireNode(child);
                self.destroyNode(child);
                if (node.keys.items.len != 0) _ = node.keys.orderedRemove(if (index == 0) 0 else index - 1);
            } else if (node.links.items.len > 1 and child.size() < self.file.maxPagePayloadBytes() / 2) {
                try self.rebalance(node, if (index == 0) 0 else index - 1);
            }
        }
        node.dirty = true;
        // Replacing a separator during redistribution can grow a parent even
        // during deletion: variable-width inline keys need the same split
        // propagation as insertion.
        return .{ .removed = true, .split = if (node.size() > self.file.maxPagePayloadBytes()) try self.splitNode(node) else null };
    }

    fn rebalance(self: *IndexEditor, parent: *Node, left_index: usize) !void {
        const left = try self.load(&parent.links.items[left_index]);
        const right = try self.load(&parent.links.items[left_index + 1]);
        if (left.kind != right.kind) return error.InvalidDocumentIndex;
        const alloc = self.nodeAllocator(left);
        if (left.kind == .internal) try left.keys.append(alloc, try self.retainKey(left, parent.keys.items[left_index]));
        try left.keys.ensureUnusedCapacity(alloc, right.keys.items.len);
        for (right.keys.items) |key| left.keys.appendAssumeCapacity(try self.retainKey(left, key));
        try left.links.appendSlice(alloc, right.links.items);
        left.dirty = true;
        left.sealed_before = 0;
        parent.sealed_before = 0;
        if (left.size() <= self.file.maxPagePayloadBytes()) {
            _ = parent.keys.orderedRemove(left_index);
            _ = parent.links.orderedRemove(left_index + 1);
        } else {
            const halves = try self.splitNode(left);
            parent.keys.items[left_index] = try self.retainKey(parent, halves.separator);
            parent.links.items[left_index + 1] = halves.right;
        }
        try self.retireNode(right);
        self.destroyNode(right);
    }

    fn retireNode(self: *IndexEditor, node: *Node) !void {
        if (self.accounting_pages) |pages| try pages.retire(node.original_page, .index);
        node.original_page = 0;
    }

    fn finish(self: *IndexEditor, pages: *PageAllocator) !u64 {
        if (self.root.page == 0 and self.root.node == null) return 0;
        return try self.flush(&self.root, pages, 0);
    }

    fn flush(self: *IndexEditor, link: *Link, pages: *PageAllocator, depth: usize) anyerror!u64 {
        if (depth > 64) return error.InvalidDocumentIndex;
        const node = link.node orelse return link.page;
        if (!node.dirty) return node.page;
        const alloc = if (self.streaming_pages != null) self.file.allocator else self.arena.allocator();
        const pointers = try alloc.alloc(u64, node.links.items.len);
        defer alloc.free(pointers);
        for (node.links.items, 0..) |*child, i| pointers[i] = if (node.kind == .leaf) child.page else try self.flush(child, pages, depth + 1);
        const keys = try alloc.alloc([]u8, node.keys.items.len);
        defer alloc.free(keys);
        const key_pages = try alloc.alloc(u64, node.keys.items.len);
        defer alloc.free(key_pages);
        for (node.keys.items, 0..) |key, i| {
            keys[i] = @constCast(key.bytes);
            key_pages[i] = key.page;
        }
        try self.retireNode(node);
        const page = try self.file.writeDocumentIndexNode(pages, .{ .kind = node.kind, .keys = keys, .pointers = pointers, .key_pages = key_pages });
        node.page = page;
        node.original_page = page;
        if (self.file.header.indexed_reclamation) for (node.keys.items, key_pages) |*key, key_page| {
            key.page = key_page;
        };
        node.dirty = false;
        link.page = page;
        return page;
    }
};

/// Scan-based proof that an index names each key's newest history record.
/// Retain record references, not full keys. Hash buckets are only candidates:
/// collisions and older versions are resolved by comparing the complete key.
const HistoryIndexCoverage = struct {
    const Entry = struct { reference: u64, next: ?usize, seen: bool = false };
    buckets: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    reader: RecordPageReader = .{},
    unresolved: usize = 0,
    // Tests force collisions to exercise the exact-key proof.
    hash_mask: u64 = std.math.maxInt(u64),

    pub fn deinit(self: *HistoryIndexCoverage, allocator: Allocator) void {
        self.buckets.deinit(allocator);
        self.entries.deinit(allocator);
        self.reader.deinit(allocator);
    }

    fn add(self: *HistoryIndexCoverage, allocator: Allocator, hash: u64, reference: u64, seen: bool) !void {
        try self.entries.ensureUnusedCapacity(allocator, 1);
        const bucket = try self.buckets.getOrPut(allocator, hash);
        self.entries.appendAssumeCapacity(.{ .reference = reference, .next = if (bucket.found_existing) bucket.value_ptr.* else null, .seen = seen });
        bucket.value_ptr.* = self.entries.items.len - 1;
    }

    fn initIndex(self: *HistoryIndexCoverage, file: *NativeFile, checkpoint: CheckpointSlot, root: u64, cancel: ?*const maintenance.CancelToken) !void {
        var indexed = checkpoint;
        indexed.document_index_root_page = root;
        var cursor = DocumentIndexCursor.init(file, indexed);
        defer cursor.deinit();
        var current = try cursor.first();
        while (current) |entry| {
            var owned = entry;
            defer owned.deinit(file.allocator);
            if (cancel) |token| try token.check();
            try self.add(file.allocator, std.hash_map.hashString(entry.key) & self.hash_mask, entry.document_page_id, false);
            self.unresolved += 1;
            current = try cursor.next();
        }
    }

    /// History must be visited newest first. A missing key is legal only when
    /// its newest record is a tombstone. Returns true for the first occurrence.
    fn observe(self: *HistoryIndexCoverage, file: *NativeFile, checkpoint: CheckpointSlot, kind: PageKind, reference: u64, key: []const u8, is_delete: bool) !bool {
        const hash = std.hash_map.hashString(key) & self.hash_mask;
        var candidate = self.buckets.get(hash);
        while (candidate) |index| {
            const entry = &self.entries.items[index];
            if (entry.reference != reference) {
                const payload = try self.reader.read(file, file.allocator, checkpoint, entry.reference, kind);
                const other_key = if (kind == .catalog) (try decodeCatalogEntry(payload)).key else (try decodeDocumentEntry(payload)).key;
                if (!std.mem.eql(u8, key, other_key)) {
                    candidate = entry.next;
                    continue;
                }
            }
            if (entry.seen) return false;
            if (entry.reference != reference) return error.InvalidDocumentIndex;
            entry.seen = true;
            self.unresolved -= 1;
            return true;
        }
        if (!is_delete) return error.InvalidDocumentIndex;
        try self.add(file.allocator, hash, reference, true);
        return true;
    }
};

const CatalogRoot = enum {
    metadata,
    index,
};

pub const CatalogEntry = struct {
    previous_page: u64,
    key: []const u8,
    value: []const u8,
    is_delete: bool = false,
    external_value_root_page: u64 = 0,
    external_value_len: usize = 0,
};

pub const DocumentEntry = struct {
    previous_page: u64,
    previous_namespace_page: u64 = 0,
    key: []const u8,
    value: []const u8 = "",
    is_delete: bool = false,
    external_value_root_page: u64 = 0,
    external_value_len: usize = 0,
};

const ValuePage = struct {
    next_page: u64,
    chunk: []const u8,
};

const EncodedCatalogEntry = struct {
    previous_page: u64,
    key: []const u8,
    value: []const u8 = "",
    is_delete: bool = false,
    external_value_root_page: u64 = 0,
    external_value_len: usize = 0,
};

const FreeMap = struct {
    covered_page_count: u64,
    free_pages: []u64,
};

const PageAllocator = struct {
    file: *NativeFile,
    free_pages: []u64,
    next_free_index: usize = 0,
    next_page_id: u64,
    batch: ?PageWriteBatch = null,
    pack_records: bool = false,
    record_buffer: [65536]u8 = undefined,
    record_used: usize = 0,
    record_page: u64 = 0,
    encoded_record: std.ArrayListUnmanaged(u8) = .empty,
    data_lock_file: ?std.Io.File = null,
    ledger: ?*allocator_v4.State = null,
    ledger_persisted: bool = false,
    can_reuse: bool = false,
    reused: std.AutoHashMapUnmanaged(u64, void) = .empty,
    pending_values: std.AutoHashMapUnmanaged(u64, u64) = .empty,
    metadata_reserve: std.ArrayList(u64) = .empty,
    metadata_cursor: usize = 0,

    pub fn deinit(self: *PageAllocator) void {
        if (self.data_lock_file) |lock_file| {
            lock_file.close(self.file.runtimeIo());
        }
        self.file.allocator.free(self.free_pages);
        self.encoded_record.deinit(self.file.allocator);
        self.reused.deinit(self.file.allocator);
        self.pending_values.deinit(self.file.allocator);
        self.metadata_reserve.deinit(self.file.allocator);
        if (self.ledger != null and self.file.ledger == self.ledger) {
            const state = self.ledger.?;
            if (!self.ledger_persisted or state.changes.count() != 0 or state.added.items.len != 0 or state.completed.items.len != 0) self.file.invalidateLedger();
        }
    }

    fn retire(self: *PageAllocator, reference: u64, kind: allocator_v4.Kind) !void {
        if (self.ledger) |state| if (reference != 0) try state.retire(.{
            .epoch = self.file.publicationEpoch(),
            .page = reference,
            .kind = kind,
        });
    }
    fn noteValue(self: *PageAllocator, page: u64, length: u64) !void {
        if (self.ledger) |state| if (state.count(page) == 0) {
            // A temporary builder root owns its children until it is linked or
            // retired. This also collects orphaned intermediate append roots.
            try state.retain(page);
            try self.pending_values.put(self.file.allocator, page, length);
        };
    }
    fn retainValue(self: *PageAllocator, page: u64) !void {
        if (self.ledger) |state| {
            try state.retain(page);
            if (self.pending_values.remove(page)) _ = try state.release(page);
        }
    }
    fn takeMetadataPage(context: *anyopaque) !u64 {
        const self: *PageAllocator = @ptrCast(@alignCast(context));
        if (self.metadata_cursor == self.metadata_reserve.items.len) return error.InvalidNativeAllocator;
        const page = self.metadata_reserve.items[self.metadata_cursor];
        self.metadata_cursor += 1;
        return page;
    }
    fn readLedgerPage(context: *anyopaque, a: Allocator, page: u64) ![]u8 {
        const self: *PageAllocator = @ptrCast(@alignCast(context));
        return NativeFile.ledgerRead(self.file, a, page);
    }
    fn writeLedgerPage(context: *anyopaque, page: u64, payload: []const u8) !void {
        const self: *PageAllocator = @ptrCast(@alignCast(context));
        const kind: PageKind = if (std.mem.startsWith(u8, payload, "AFL4ALOC")) .free_map else .allocator;
        try self.file.writePage(page, kind, payload);
    }

    fn writePage(self: *PageAllocator, page: u64, kind: PageKind, payload: []const u8) !void {
        if (self.ledger) |state| switch (kind) {
            .catalog_index, .data => try state.retain(page),
            .catalog, .document => try self.retainRecord(page, kind, payload),
            .document_index => {
                try state.retain(page);
                // Ownership needs encoded references, not copies of every key.
                var frame: IndexReadFrame = .{};
                defer frame.deinit(self.file.allocator);
                try frame.decode(self.file.allocator, payload);
                for (0..frame.offsets.items.len) |i| {
                    const key = frame.keyReference(i);
                    if (key == 0) continue;
                    try state.retain(physicalPage(key));
                }
            },
            else => {},
        };
        if (self.batch == null) self.batch = .{ .file = self.file };
        try self.batch.?.appendPage(page, kind, payload);
    }

    fn flush(self: *PageAllocator) !void {
        try self.flushRecords();
        if (self.batch) |*batch| try batch.flush();
    }

    // Encoded payloads fit one native page. Reuse scratch across records and
    // roots; writeRecord copies bytes before the next encoding can overwrite it.
    fn writeDocument(self: *PageAllocator, entry: DocumentEntry) !u64 {
        self.encoded_record.clearRetainingCapacity();
        try encodeDocumentEntry(self.file.allocator, &self.encoded_record, entry);
        return self.writeRecord(.document, self.encoded_record.items);
    }

    fn writeCatalog(self: *PageAllocator, entry: CatalogEntry) !u64 {
        self.encoded_record.clearRetainingCapacity();
        try encodeCatalogEntry(self.file.allocator, &self.encoded_record, entry);
        return self.writeRecord(.catalog, self.encoded_record.items);
    }

    fn writeRecord(self: *PageAllocator, kind: PageKind, payload: []const u8) !u64 {
        std.debug.assert(kind == .catalog or kind == .document);
        const capacity = self.file.maxPagePayloadBytes();
        if (!self.pack_records or payload.len + 4 > capacity / 2) {
            const page = try self.allocate();
            try self.writePage(page, kind, payload);
            return page;
        }
        if (self.record_used + 4 + payload.len > capacity) try self.flushRecords();
        if (self.record_used == 0) {
            self.record_page = try self.allocate();
            if (self.record_page > packed_page_mask) return error.RecordTooLarge;
        }
        const offset = self.record_used;
        const out = self.record_buffer[offset..][0 .. 4 + payload.len];
        std.mem.writeInt(u16, out[0..2], @intCast(payload.len), .little);
        out[2] = @backingInt(kind);
        out[3] = 0;
        @memcpy(out[4..], payload);
        self.record_used += out.len;
        const reference = packed_record_flag | (@as(u64, @intCast(offset)) << 47) | self.record_page;
        try self.retainRecord(reference, kind, payload);
        return reference;
    }

    fn retainRecord(self: *PageAllocator, reference: u64, kind: PageKind, payload: []const u8) !void {
        const state = self.ledger orelse return;
        try state.retain(physicalPage(reference));
        const root = switch (kind) {
            .document => (try decodeDocumentEntry(payload)).external_value_root_page,
            .catalog => (try decodeCatalogEntry(payload)).external_value_root_page,
            else => return error.InvalidNativeAllocator,
        };
        if (root != 0) try self.retainValue(root);
    }

    fn flushRecords(self: *PageAllocator) !void {
        if (self.record_used == 0) return;
        try self.writePage(self.record_page, .record_bundle, self.record_buffer[0..self.record_used]);
        self.record_used = 0;
    }

    fn allocate(self: *PageAllocator) !u64 {
        if (self.ledger) |state| if (self.can_reuse) {
            if (try state.allocate()) |page| {
                try self.reused.put(self.file.allocator, page, {});
                self.file.page_cache.remove(self.file.allocator, page);
                if (self.file.secondary_page_cache) |cache| cache.remove(self.file.allocator, page);
                return page;
            }
        };
        if (self.next_free_index < self.free_pages.len) {
            const page_id = self.free_pages[self.next_free_index];
            self.next_free_index += 1;
            return page_id;
        }

        const page_id = self.next_page_id;
        if (self.file.max_file_bytes) |limit| {
            if (page_id >= limit / self.file.header.page_size) return error.LiteStorageBudgetExceeded;
        }
        self.next_page_id = try std.math.add(u64, self.next_page_id, 1);
        if (self.ledger) |state| try state.reserveTail(page_id);
        return page_id;
    }

    /// Free pages were validated against both durable checkpoint slots when
    /// this allocator was created. Pages not consumed by the current commit
    /// remain safe to advertise without re-walking every historical chain.
    fn remainingFreePages(self: *const PageAllocator) []const u64 {
        return self.free_pages[self.next_free_index..];
    }

    fn wasReused(self: *const PageAllocator, page: u64) bool {
        if (self.ledger != null) return self.reused.contains(page);
        var low: usize = 0;
        var high = self.next_free_index;
        while (low < high) {
            const mid = low + (high - low) / 2;
            if (self.free_pages[mid] < page) low = mid + 1 else high = mid;
        }
        return low < self.next_free_index and self.free_pages[low] == page;
    }
};

pub const DocumentMutation = struct {
    key: []const u8,
    value: []const u8 = "",
    is_delete: bool = false,
    /// Internal transaction-local reference to a streamed, already written value.
    external_value_root_page: u64 = 0,
    external_value_len: usize = 0,
};

const PendingDocumentIndexEntry = struct {
    key: []const u8,
    document_page_id: u64,
    ordinal: usize,

    fn lessThan(_: void, lhs: PendingDocumentIndexEntry, rhs: PendingDocumentIndexEntry) bool {
        return switch (std.mem.order(u8, lhs.key, rhs.key)) {
            .lt => true,
            .gt => false,
            .eq => lhs.ordinal < rhs.ordinal,
        };
    }
};

pub const CatalogMutation = struct {
    key: []const u8,
    value: []const u8 = "",
    is_delete: bool = false,
    /// Internal transaction-local reference to a streamed, already written value.
    external_value_root_page: u64 = 0,
    external_value_len: usize = 0,
};

/// Bounded final-key write set shared by all callbacks in a native transaction.
/// Payloads larger than one record are streamed immediately; only their extent
/// references are retained. A spill rebuilds each touched index once, without
/// publishing a checkpoint or ending the transaction.
const TransactionWrites = struct {
    const Root = enum { metadata, index, documents };
    const max_keys = 1024;
    const max_bytes = 1024 * 1024;
    const max_append_files = 4;
    maps: [3]std.StringHashMapUnmanaged(CatalogMutation) = .{ .empty, .empty, .empty },
    bytes: usize = 0,
    count: usize = 0,
    pages: PageAllocator,
    // Each active file retains one page tail, a write batch, and a bounded
    // extent frontier. Limit simultaneous frontiers independently of key/value
    // staging; a fifth file spills the transaction's private state.
    appends: [2]std.StringHashMapUnmanaged(*NativeFile.CatalogAppendState) = .{ .empty, .empty },
    append_count: usize = 0,

    fn clear(self: *TransactionWrites, allocator: Allocator) void {
        for (&self.appends) |*map| {
            var it = map.valueIterator();
            while (it.next()) |state| state.*.destroy();
            map.deinit(allocator);
            map.* = .empty;
        }
        self.append_count = 0;
        for (&self.maps) |*map| {
            var it = map.valueIterator();
            while (it.next()) |mutation| {
                allocator.free(mutation.key);
                allocator.free(mutation.value);
            }
            map.clearRetainingCapacity();
        }
        self.bytes = 0;
        self.count = 0;
    }

    pub fn deinit(self: *TransactionWrites, allocator: Allocator) void {
        self.clear(allocator);
        for (&self.maps) |*map| map.deinit(allocator);
        self.pages.deinit();
        allocator.destroy(self);
    }

    fn entry(mutation: CatalogMutation) CatalogEntry {
        return .{ .previous_page = 0, .key = mutation.key, .value = mutation.value, .is_delete = mutation.is_delete, .external_value_root_page = mutation.external_value_root_page, .external_value_len = mutation.external_value_len };
    }
};

pub const OwnedDocument = struct {
    key: []u8,
    value: []u8,
};

pub const CheckpointSlot = struct {
    commit_sequence: u64 = 0,
    catalog_root_page: u64 = 0,
    document_root_page: u64 = 0,
    index_catalog_root_page: u64 = 0,
    free_map_root_page: u64 = 0,
    page_count: u64 = 1,
    namespace_directory_root_page: u64 = 0,
    document_index_root_page: u64 = 0,
};

pub const LockMode = enum {
    writer,
    reader,
    internal_reader,
};

pub const OpenOptions = struct {
    externally_locked: bool = false,
    /// Owner read-only opens wait for in-place service; primitives default to fail-fast.
    wait_for_reader_lock: bool = false,
    internal_reader: bool = false,
    read_only: bool = false,
    no_sync: bool = false,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
};

pub const PathWriterLock = struct {
    io_impl: std.Io.Threaded,
    borrowed_io: ?std.Io = null,
    file: std.Io.File,

    pub fn close(self: *PathWriterLock) void {
        const io = self.borrowed_io orelse self.io_impl.io();
        self.file.close(io);
        if (self.borrowed_io == null) self.io_impl.deinit();
        self.* = undefined;
    }
};

const LockFile = struct {
    file: std.Io.File,
};

pub const CreateOptions = struct {
    indexed_reclamation: bool = false,
    exclusive: bool = false,
    no_sync: bool = false,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    writer_lock_marker: []const u8 = "",
};

pub const Header = struct {
    indexed_reclamation: bool = false,
    packed_records: bool = true,
    page_size: u32 = default_page_size,
    active_checkpoint: u8 = 0,
    checkpoints: [checkpoint_slot_count]CheckpointSlot = .{ .{}, .{} },
};

pub const InspectReport = struct {
    valid: bool,
    format_version: u32,
    page_size: u32,
    active_checkpoint: u8,
    commit_sequence: u64,
    page_count: u64,
    issue: ?[]const u8 = null,
};

pub const CheckReport = struct {
    valid: bool,
    file_size: u64,
    valid_prefix_size: u64,
    tail_bytes: u64,
    record_count: u64,
    live_file_count: u64,
    live_bytes: u64,
    compact_size: u64,
    reclaimable_bytes: u64,
    issue: ?[]const u8 = null,
};

pub const VacuumReport = struct {
    before_size: u64,
    after_size: u64,
    reclaimed_bytes: u64,
    live_file_count: u64,
    live_bytes: u64,
};

/// Result of publishing a replacement generation after the atomic rename has
/// crossed its commit point. `durability_unknown` means the live process has
/// adopted the replacement, but the parent-directory sync failed, so crash
/// durability cannot be promised and callers must not retry automatically.
pub const GenerationPublicationOutcome = enum {
    complete,
    durability_unknown,
};

pub const StableSnapshotReport = struct {
    source_size: u64,
    snapshot_size: u64,
    checkpoint_sequence: u64,
    page_count: u64,
    tail_bytes: u64,
};

pub const OwnedCatalogRecord = struct {
    key: []u8,
    value: []u8,
};

pub const OwnedCatalogKey = struct {
    key: []u8,
    page_id: u64 = 0,
};

const OwnedLiveRecordRef = struct {
    key: []u8,
    page_id: u64,
};

fn liveRecordRefLessThan(_: void, lhs: OwnedLiveRecordRef, rhs: OwnedLiveRecordRef) bool {
    return std.mem.order(u8, lhs.key, rhs.key) == .lt;
}

/// Decoded chain-navigation metadata for a page, cached so reachability
/// walks can traverse chains without re-reading and re-decoding page
/// payloads. Mirrors exactly the fields the walks consume.
const PageLinkInfo = struct {
    kind: PageKind,
    /// catalog/document: previous page in the chain; value: next page.
    link_page: u64 = 0,
    external_value_root_page: u64 = 0,
    external_value_len: usize = 0,
    /// value pages only.
    chunk_len: usize = 0,
    is_delete: bool = false,
    value_len: usize = 0,
    /// catalog pages only; owned by the cache.
    key: []u8 = &.{},
};

/// A copy of one page's link info handed out by the cache. `key` (catalog
/// pages) is owned by the caller.
const PageLinkCopy = struct {
    kind: PageKind,
    link_page: u64,
    external_value_root_page: u64,
    external_value_len: usize,
    chunk_len: usize,
    key: ?[]u8,
};

/// In-memory cache of encoded pages, keyed by page id.
///
/// Safe when OS file locks are available because page contents are stable for
/// the lifetime of an open handle: all in-process page writes flow through
/// `writePage` (which updates the cache) or vacuum replacement (which
/// clears it), sidecar writer locks serialize writers, and read-only
/// data-file shared locks block the exclusive data-rewrite lock needed for
/// free-page reuse and vacuum. Filesystems that cannot provide those locks are
/// rejected before a `NativeFile` is returned, so cached pages are never used
/// by an unfenced handle.
const PageCache = struct {
    const default_limit_bytes: usize = 64 * 1024 * 1024;
    const default_link_limit_bytes: usize = 16 * 1024 * 1024;
    const link_entry_overhead: usize = @sizeOf(PageLinkInfo) + @sizeOf(u64);

    // Promotion shares the encoded page with an immutable decoded view. Pins
    // permit overflow-key I/O without holding the cache mutex; eviction drops
    // residency immediately, but charges pinned storage until its last reader
    // releases it. Cursor positions and overflow scratch are never shared.
    const IndexView = struct {
        frame: IndexReadFrame,
        references: usize = 1,

        fn size(self: *const IndexView) usize {
            return @sizeOf(IndexView) + self.frame.raw.?.len + self.frame.offsets.capacity * @sizeOf(u16);
        }
    };
    /// Immutable encoded bytes, owned jointly by residency and active readers.
    /// Retired pinned pages remain charged until the last reader releases them.
    const RawView = struct {
        bytes: []u8,
        references: usize = 1,

        fn size(self: *const RawView) usize {
            return @sizeOf(RawView) + self.bytes.len;
        }
    };
    const CachedPage = struct { bytes: []u8, credit: u8, metadata: bool, index: ?*IndexView = null, raw_view: ?*RawView = null };
    mutex: std.atomic.Mutex = .unlocked,
    pages: std.AutoArrayHashMapUnmanaged(u64, CachedPage) = .empty,
    clock_hand: usize = 0,
    links: std.AutoHashMapUnmanaged(u64, PageLinkInfo) = .empty,
    total_bytes: usize = 0,
    link_bytes: usize = 0,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    resource_page_accounted_bytes: u64 = 0,
    resource_link_accounted_bytes: u64 = 0,
    /// CLOCK admission starts payloads cold; accesses promote them, while
    /// navigation metadata starts with a second chance. Overflow evicts only
    /// enough bytes for the incoming page, preserving the rest of the cache.
    limit_bytes: usize = default_limit_bytes,
    link_limit_bytes: usize = default_link_limit_bytes,

    fn getCopy(self: *PageCache, allocator: Allocator, page_id: u64) !?[]u8 {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const cached = self.pages.getPtr(page_id) orelse return null;
        cached.credit = if (cached.metadata) 3 else 2;
        return try allocator.dupe(u8, cached.bytes);
    }

    fn copyInto(self: *PageCache, page_id: u64, out: []u8) bool {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const cached = self.pages.getPtr(page_id) orelse return false;
        if (cached.bytes.len != out.len) return false;
        cached.credit = if (cached.metadata) 3 else 2;
        @memcpy(out, cached.bytes);
        return true;
    }

    fn acquireRaw(self: *PageCache, allocator: Allocator, page_id: u64, size: usize) ?*RawView {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const before_bytes = self.total_bytes;
        defer if (self.total_bytes != before_bytes) self.refreshPageResourceUsageLocked();
        if (self.clearPagesForHardPressureLocked(allocator)) return null;
        const cached = self.pages.getPtr(page_id) orelse return null;
        if (cached.bytes.len != size or cached.index != null) return null;
        if (cached.raw_view) |view| {
            cached.credit = if (cached.metadata) 3 else 2;
            view.references += 1;
            return view;
        }
        const extra = @sizeOf(RawView);
        if (extra > self.limit_bytes or size > self.limit_bytes - extra) return null;
        const view = allocator.create(RawView) catch return null;
        var promoted = false;
        defer if (!promoted) allocator.destroy(view);
        self.evictPagesToExceptLocked(allocator, self.limit_bytes - extra, page_id);
        if (self.total_bytes > self.limit_bytes - extra) return null;
        const resident = self.pages.getPtr(page_id).?;
        view.* = .{ .bytes = resident.bytes, .references = 2 };
        resident.raw_view = view;
        resident.credit = if (resident.metadata) 3 else 2;
        self.total_bytes += extra;
        promoted = true;
        self.refreshPageResourceUsageLocked();
        _ = self.clearPagesForHardPressureLocked(allocator);
        return view;
    }

    fn releaseRaw(self: *PageCache, allocator: Allocator, view: *RawView) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const before_bytes = self.total_bytes;
        self.releaseRawLocked(allocator, view);
        if (self.total_bytes != before_bytes) self.refreshPageResourceUsageLocked();
    }

    fn releaseRawLocked(self: *PageCache, allocator: Allocator, view: *RawView) void {
        std.debug.assert(view.references > 0);
        view.references -= 1;
        if (view.references != 0) return;
        self.total_bytes -= view.size();
        allocator.free(view.bytes);
        allocator.destroy(view);
    }

    fn acquireIndex(self: *PageCache, allocator: Allocator, page_id: u64) !?*IndexView {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const before_bytes = self.total_bytes;
        defer if (self.total_bytes != before_bytes) self.refreshPageResourceUsageLocked();
        if (self.clearPagesForHardPressureLocked(allocator)) return null;
        const cached = self.pages.getPtr(page_id) orelse return null;
        if (cached.index) |view| {
            cached.credit = 3;
            view.references += 1;
            return view;
        }
        if (cached.raw_view != null) return null;
        const payload = try decodePagePayload(cached.bytes, .document_index);
        const view = allocator.create(IndexView) catch return null;
        view.* = .{ .frame = .{} };
        var promoted = false;
        defer if (!promoted) {
            view.frame.deinit(allocator);
            allocator.destroy(view);
        };
        view.frame.decode(allocator, payload) catch |err| switch (err) {
            error.OutOfMemory => return null,
            else => return err,
        };
        const extra = @sizeOf(IndexView) + view.frame.offsets.capacity * @sizeOf(u16);
        if (extra > self.limit_bytes or cached.bytes.len > self.limit_bytes - extra) return null;
        self.evictPagesToExceptLocked(allocator, self.limit_bytes - extra, page_id);
        if (self.total_bytes > self.limit_bytes - extra) return null;
        // Eviction can move the map entry. No I/O or unlock occurs between
        // validation and publication, so this view describes these exact bytes.
        const resident = self.pages.getPtr(page_id).?;
        view.frame.raw = resident.bytes;
        view.references = 2; // cache and caller
        resident.index = view;
        resident.credit = 3;
        self.total_bytes += extra;
        promoted = true;
        self.refreshPageResourceUsageLocked();
        _ = self.clearPagesForHardPressureLocked(allocator);
        return view;
    }

    fn releaseIndex(self: *PageCache, allocator: Allocator, view: *IndexView) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const before_bytes = self.total_bytes;
        self.releaseIndexLocked(allocator, view);
        if (self.total_bytes != before_bytes) self.refreshPageResourceUsageLocked();
    }

    fn releaseIndexLocked(self: *PageCache, allocator: Allocator, view: *IndexView) void {
        std.debug.assert(view.references > 0);
        view.references -= 1;
        if (view.references != 0) return;
        self.total_bytes -= view.size();
        view.frame.deinit(allocator);
        allocator.destroy(view);
    }

    fn freePageLocked(self: *PageCache, allocator: Allocator, page: CachedPage) void {
        if (page.index) |view| {
            self.releaseIndexLocked(allocator, view);
        } else if (page.raw_view) |view| {
            self.releaseRawLocked(allocator, view);
        } else {
            self.total_bytes -= page.bytes.len;
            allocator.free(page.bytes);
        }
    }

    fn attachResourceManager(self: *PageCache, manager: *resource_manager_mod.ResourceManager) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.resource_manager = manager;
        self.refreshPageResourceUsageLocked();
        self.refreshLinkResourceUsageLocked();
    }

    fn put(self: *PageCache, allocator: Allocator, page_id: u64, page: []const u8) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        // Replacement must invalidate the old bytes even if admission fails.
        if (self.pages.fetchSwapRemove(page_id)) |old| {
            self.freePageLocked(allocator, old.value);
        }
        defer self.refreshPageResourceUsageLocked();
        if (self.clearPagesForHardPressureLocked(allocator)) return;
        if (page.len > self.limit_bytes) return;
        self.evictPagesToLocked(allocator, self.limit_bytes - page.len);
        if (self.total_bytes > self.limit_bytes - page.len) return;
        const owned = allocator.dupe(u8, page) catch return;
        const metadata = page.len >= page_header_size and switch (page[4]) {
            @backingInt(PageKind.document_index), @backingInt(PageKind.catalog_index), @backingInt(PageKind.value_extent) => true,
            else => false,
        };
        self.pages.put(allocator, page_id, .{ .bytes = owned, .credit = if (metadata) 2 else 0, .metadata = metadata }) catch {
            allocator.free(owned);
            return;
        };
        self.total_bytes += page.len;
        self.refreshPageResourceUsageLocked();
        _ = self.clearPagesForHardPressureLocked(allocator);
    }

    fn evictPagesToLocked(self: *PageCache, allocator: Allocator, target: usize) void {
        self.evictPagesToExceptLocked(allocator, target, null);
    }

    fn evictPagesToExceptLocked(self: *PageCache, allocator: Allocator, target: usize, protected: ?u64) void {
        while (self.total_bytes > target and self.pages.count() != 0) {
            if (self.clock_hand >= self.pages.count()) self.clock_hand = 0;
            if (protected != null and self.pages.keys()[self.clock_hand] == protected.?) {
                if (self.pages.count() == 1) break;
                self.clock_hand += 1;
                continue;
            }
            const entry = &self.pages.values()[self.clock_hand];
            if (entry.credit != 0) {
                entry.credit -= 1;
                self.clock_hand += 1;
            } else {
                self.freePageLocked(allocator, entry.*);
                self.pages.swapRemoveAt(self.clock_hand);
            }
        }
    }

    fn getLinksCopy(self: *PageCache, allocator: Allocator, page_id: u64) !?PageLinkCopy {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const cached = self.links.get(page_id) orelse return null;
        return .{
            .kind = cached.kind,
            .link_page = cached.link_page,
            .external_value_root_page = cached.external_value_root_page,
            .external_value_len = cached.external_value_len,
            .chunk_len = cached.chunk_len,
            .key = if (cached.key.len > 0) try allocator.dupe(u8, cached.key) else null,
        };
    }

    fn putLinks(self: *PageCache, allocator: Allocator, page_id: u64, info: PageLinkInfo) void {
        const owned_key = if (info.key.len > 0) allocator.dupe(u8, info.key) catch return else @as([]u8, &.{});
        var owned = info;
        owned.key = owned_key;

        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.clearLinksForHardPressureLocked(allocator)) {
            if (owned.key.len > 0) allocator.free(owned.key);
            return;
        }
        if (self.links.getEntry(page_id)) |entry| {
            self.link_bytes -= entry.value_ptr.key.len + link_entry_overhead;
            if (entry.value_ptr.key.len > 0) allocator.free(entry.value_ptr.key);
            entry.value_ptr.* = owned;
            self.link_bytes += owned.key.len + link_entry_overhead;
            self.refreshLinkResourceUsageLocked();
            _ = self.clearLinksForHardPressureLocked(allocator);
            return;
        }
        if (self.link_bytes + owned.key.len + link_entry_overhead > self.link_limit_bytes) {
            // At capacity, prefer keeping the resident entries: chain walks
            // revisit the same old pages every commit, so evicting them to
            // admit one new entry would thrash the whole walk.
            if (owned.key.len > 0) allocator.free(owned.key);
            return;
        }
        self.links.put(allocator, page_id, owned) catch {
            if (owned.key.len > 0) allocator.free(owned.key);
            return;
        };
        self.link_bytes += owned.key.len + link_entry_overhead;
        self.refreshLinkResourceUsageLocked();
        _ = self.clearLinksForHardPressureLocked(allocator);
    }

    fn remove(self: *PageCache, allocator: Allocator, page_id: u64) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.removeLocked(allocator, page_id);
    }

    fn removeLinks(self: *PageCache, allocator: Allocator, page_id: u64) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.links.fetchRemove(page_id)) |entry| {
            self.link_bytes -= entry.value.key.len + link_entry_overhead;
            if (entry.value.key.len > 0) allocator.free(entry.value.key);
            self.refreshLinkResourceUsageLocked();
        }
    }

    fn removeLocked(self: *PageCache, allocator: Allocator, page_id: u64) void {
        var removed_page = false;
        var removed_links = false;
        if (self.pages.fetchSwapRemove(page_id)) |entry| {
            self.freePageLocked(allocator, entry.value);
            removed_page = true;
        }
        if (self.links.fetchRemove(page_id)) |entry| {
            self.link_bytes -= entry.value.key.len + link_entry_overhead;
            if (entry.value.key.len > 0) allocator.free(entry.value.key);
            removed_links = true;
        }
        if (removed_page) self.refreshPageResourceUsageLocked();
        if (removed_links) self.refreshLinkResourceUsageLocked();
    }

    /// Rollback makes the append tail reusable. Streamed private-image writes
    /// can bypass this cache, so discard both bytes and links for those IDs.
    /// Preserve cached pages from the restored checkpoint.
    fn discardFrom(self: *PageCache, allocator: Allocator, first_page: u64) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        var i: usize = 0;
        while (i < self.pages.count()) {
            const page = self.pages.keys()[i];
            if (page >= first_page) self.removeLocked(allocator, page) else i += 1;
        }
        // Hash-map removal leaves other entries and the iterator stable.
        var links = self.links.keyIterator();
        while (links.next()) |page| {
            if (page.* >= first_page) self.removeLocked(allocator, page.*);
        }
    }

    fn clear(self: *PageCache, allocator: Allocator) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.clearLocked(allocator);
    }

    fn clearLocked(self: *PageCache, allocator: Allocator) void {
        self.clearPagesLocked(allocator);
        self.clearLinksLocked(allocator);
        self.refreshPageResourceUsageLocked();
        self.refreshLinkResourceUsageLocked();
    }

    fn clearPagesLocked(self: *PageCache, allocator: Allocator) void {
        for (self.pages.values()) |page| self.freePageLocked(allocator, page);
        self.clock_hand = 0;
        self.pages.clearRetainingCapacity();
    }

    fn clearLinksLocked(self: *PageCache, allocator: Allocator) void {
        var link_it = self.links.valueIterator();
        while (link_it.next()) |info| {
            if (info.key.len > 0) allocator.free(info.key);
        }
        self.links.clearRetainingCapacity();
        self.link_bytes = 0;
    }

    pub fn deinit(self: *PageCache, allocator: Allocator) void {
        self.clearLocked(allocator);
        self.releaseResourceUsageLocked();
        self.pages.deinit(allocator);
        self.links.deinit(allocator);
    }

    fn refreshPageResourceUsageLocked(self: *PageCache) void {
        const manager = self.resource_manager orelse return;
        manager.observeUsage(.lite_native_page_cache, &self.resource_page_accounted_bytes, @intCast(self.total_bytes));
    }

    fn refreshLinkResourceUsageLocked(self: *PageCache) void {
        const manager = self.resource_manager orelse return;
        manager.observeUsage(.lite_native_link_cache, &self.resource_link_accounted_bytes, @intCast(self.link_bytes));
    }

    fn releaseResourceUsageLocked(self: *PageCache) void {
        const manager = self.resource_manager orelse return;
        manager.observeUsage(.lite_native_page_cache, &self.resource_page_accounted_bytes, 0);
        manager.observeUsage(.lite_native_link_cache, &self.resource_link_accounted_bytes, 0);
        self.resource_manager = null;
    }

    fn clearPagesForHardPressureLocked(self: *PageCache, allocator: Allocator) bool {
        const manager = self.resource_manager orelse return false;
        const stats = manager.sliceStats(.lite_native_page_cache);
        if (stats.pressure != .hard or stats.hard_action != .shrink_cache) return false;
        // Budgets are shared across generations and handles. Reclaim this
        // cache's share of the aggregate excess, even when it is individually
        // smaller than the slice's soft limit.
        const reclaim: usize = @intCast(@min(stats.used_bytes -| stats.soft_limit_bytes, self.total_bytes));
        self.evictPagesToLocked(allocator, @min(self.total_bytes - reclaim, self.limit_bytes));
        self.refreshPageResourceUsageLocked();
        return true;
    }

    fn clearLinksForHardPressureLocked(self: *PageCache, allocator: Allocator) bool {
        const manager = self.resource_manager orelse return false;
        const stats = manager.sliceStats(.lite_native_link_cache);
        if (stats.pressure != .hard or stats.hard_action != .shrink_cache) return false;
        self.clearLinksLocked(allocator);
        self.refreshLinkResourceUsageLocked();
        return true;
    }
};

/// Bounded, page-addressed write staging. Pages may finish out of allocation
/// order (notably packed records and streaming index nodes). Keep ready pages
/// ordered in one fixed buffer, then write contiguous runs without filling gaps.
/// Cache admission happens only after every run succeeds; failure poisons the
/// batch so partially written private pages cannot be retried or published.
const PageWriteBatch = struct {
    const capacity = 64 * 1024;
    const max_pages = capacity / 4096;
    file: *NativeFile,
    options: WriteOptions = .{},
    buffer: [capacity]u8 = undefined,
    page_ids: [max_pages]u64 = undefined,
    used: usize = 0,
    failure: ?anyerror = null,

    fn reserve(self: *PageWriteBatch, page_id: u64) ![]u8 {
        if (self.failure) |err| return err;
        const size: usize = self.file.header.page_size;
        var count = self.used / size;
        var slot = count;
        // Consecutive writes append without moving bytes. Late packed pages
        // normally move only their immediately following index page.
        while (slot > 0 and self.page_ids[slot - 1] > page_id) : (slot -= 1) {}
        if (slot > 0 and self.page_ids[slot - 1] == page_id)
            return self.buffer[(slot - 1) * size ..][0..size];
        if (self.used + size > self.buffer.len) {
            // Drain only the earliest contiguous run. Keep the later run in
            // the window so an unfinished packed page can still join it.
            var end: usize = 1;
            while (end < count and self.page_ids[end] - self.page_ids[end - 1] == 1) : (end += 1) {}
            try self.flushCount(end);
            count = self.used / size;
            slot = count;
            while (slot > 0 and self.page_ids[slot - 1] > page_id) : (slot -= 1) {}
        }
        std.mem.copyBackwards(u8, self.buffer[(slot + 1) * size .. self.used + size], self.buffer[slot * size .. self.used]);
        std.mem.copyBackwards(u64, self.page_ids[slot + 1 .. count + 1], self.page_ids[slot..count]);
        self.page_ids[slot] = page_id;
        self.used += size;
        return self.buffer[slot * size ..][0..size];
    }

    fn appendPage(self: *PageWriteBatch, page_id: u64, kind: PageKind, payload: []const u8) !void {
        if (payload.len > self.file.maxPagePayloadBytes()) return error.PageTooLarge;
        encodePage(try self.reserve(page_id), kind, payload);
    }

    fn appendValue(self: *PageWriteBatch, page_id: u64, next: u64, chunk: []const u8) !void {
        if (chunk.len == 0 or chunk.len > self.file.maxValuePagePayloadBytes()) return error.InvalidNativeValueChain;
        const page = try self.reserve(page_id);
        var prefix: [value_page_header_size]u8 = undefined;
        std.mem.writeInt(u64, &prefix, next, .little);
        encodePageParts(page, .value, &prefix, chunk);
    }

    fn flush(self: *PageWriteBatch) !void {
        try self.flushCount(self.used / self.file.header.page_size);
    }

    fn flushCount(self: *PageWriteBatch, count: usize) !void {
        var zig017_return_error: ?anyerror = null;
        if (self.failure) |err| return err;
        if (count == 0) return;
        errdefer {
            if (zig017_return_error) |err| self.failure = err;
        }
        const file = self.file;
        const size: usize = file.header.page_size;
        var start: usize = 0;
        while (start < count) {
            var end = start + 1;
            while (end < count and self.page_ids[end] - self.page_ids[end - 1] == 1) : (end += 1) {}
            if (builtin.is_test) {
                if (file.test_page_write_fail_after) |remaining| {
                    if (remaining == 0) return zig017_failure: {
                        zig017_return_error = error.TestPageWriteFailure;
                        break :zig017_failure error.TestPageWriteFailure;
                    };
                    file.test_page_write_fail_after = remaining - 1;
                }
                _ = file.test_page_write_calls.fetchAdd(1, .monotonic);
                _ = file.test_page_writes.fetchAdd(@intCast(end - start), .monotonic);
            }
            (file.file.writePositionalAll(file.runtimeIo(), self.buffer[start * size .. end * size], self.page_ids[start] * @as(u64, size)) catch |zig017_err| {
                zig017_return_error = zig017_err;
                return zig017_err;
            });
            start = end;
        }
        for (self.page_ids[0..count], 0..) |page_id, i|
            file.cacheWrittenPage(page_id, self.buffer[i * size ..][0..size], self.options);
        const remaining = self.used - count * size;
        std.mem.copyForwards(u8, self.buffer[0..remaining], self.buffer[count * size .. self.used]);
        std.mem.copyForwards(u64, self.page_ids[0 .. remaining / size], self.page_ids[count .. self.used / size]);
        self.used = remaining;
    }
};

pub const ChangeCapture = struct {
    pub const Root = enum { metadata, index, documents };
    const max_key_bytes = 4 * 1024 * 1024;
    const max_keys = 65536;
    keys: [3]std.StringHashMapUnmanaged(void) = .{ .empty, .empty, .empty },
    key_bytes: usize = 0,
    count: usize = 0,
    overflow: bool = false,

    pub fn deinit(self: *ChangeCapture, allocator: Allocator) void {
        for (&self.keys) |*map| {
            var it = map.keyIterator();
            while (it.next()) |key| allocator.free(key.*);
            map.deinit(allocator);
        }
        self.* = .{};
    }

    fn record(self: *ChangeCapture, allocator: Allocator, root: Root, key: []const u8) void {
        if (self.overflow) return;
        const map = &self.keys[@backingInt(root)];
        if (map.contains(key)) return;
        if (key.len > max_key_bytes - self.key_bytes or self.count == max_keys) {
            self.overflow = true;
            return;
        }
        const owned = allocator.dupe(u8, key) catch {
            self.overflow = true;
            return;
        };
        map.put(allocator, owned, {}) catch {
            allocator.free(owned);
            self.overflow = true;
            return;
        };
        self.count += 1;
        self.key_bytes += key.len;
    }
};

pub const VacuumImage = struct {
    prepared: NativeFile,
    report: VacuumReport,

    pub fn deinit(self: *VacuumImage) void {
        {
            const io = self.prepared.runtimeIo();
            const previous = io.swapCancelProtection(.blocked);
            defer _ = io.swapCancelProtection(previous);
            deleteFilePath(io, self.prepared.path) catch {};
        }
        // Restore protection before close can release an owned I/O runtime.
        self.prepared.close();
        self.* = undefined;
    }
};

pub const NativeFile = struct {
    allocator: Allocator,
    /// The default constructors own this Threaded runtime. Borrowed-I/O
    /// constructors leave it undefined and retain `borrowed_io` instead. This
    /// keeps the production convenience API while making the complete Lite
    /// file lifecycle executable by deterministic std.Io implementations.
    io_impl: std.Io.Threaded,
    borrowed_io: ?std.Io = null,
    path: []u8,
    file: std.Io.File,
    writer_lock_file: ?std.Io.File = null,
    /// The embedded connection layer holds the path lease around complete calls.
    externally_locked: bool = false,
    header: Header,
    transaction_header: ?Header = null,
    transaction_writes: ?*TransactionWrites = null,
    flushing_transaction: bool = false,
    transaction_pages: ?*PageAllocator = null,
    durable_header: ?Header = null,
    change_capture: ?*ChangeCapture = null,
    read_only: bool = false,
    no_sync: bool = false,
    // An error after slot publication may leave disk ahead of header. Do not
    // elide a subsequent write based on the old in-memory checkpoint then.
    checkpoint_publication_uncertain: bool = false,
    /// Owner admission budget. Reuse remains permitted at the file-size cap.
    /// Null means unlimited; the owner subtracts retired/workspace capacity.
    max_file_bytes: ?u64 = null,
    /// Foreground owners preserve allocator workspace inside the hard limit.
    /// Private rewrite images use their separately admitted workspace instead.
    reserve_retirement_capacity: bool = false,
    maintenance_publication: bool = false,
    /// Committed reclamation activity, independent of physical file growth.
    retirement_activity_bytes: u64 = 0,
    vacuum_workspace_limit: ?u64 = null,
    vacuum_target_indexed: bool = false,
    ledger: ?*allocator_v4.State = null,
    minimum_reader_sequence: ?u64 = null,
    reader_frontier: ?*const std.atomic.Value(u64) = null,
    publication_observer: ?struct { context: *anyopaque, notify: *const fn (*anyopaque, CheckpointSlot, bool) void } = null,
    secondary_page_cache: ?*PageCache = null,
    retirement_work_pages: usize = 128,
    allocator_cancel_token: ?*const maintenance.CancelToken = null,
    private_retirement_epoch: ?u64 = null,
    incremental_checkpoints: bool = false,
    page_cache_enabled: std.atomic.Value(bool) = .init(true),
    /// Maintenance readers and private images retain navigation pages only.
    page_cache_policy: enum { normal, metadata_only } = .normal,
    page_cache: PageCache = .{},
    namespace_directory_cache: NamespaceDirectory = .empty,
    namespace_directory_cache_root: u64 = std.math.maxInt(u64),
    namespace_directory_delta_depth: u16 = 0,
    /// While non-zero, page reads bypass the page cache and hit disk.
    /// Integrity checks hold this so they verify on-disk state rather than
    /// cached copies.
    page_cache_bypass: std.atomic.Value(u32) = .init(0),
    test_fail_vacuum_after_adoption: bool = false,
    test_fail_generation_directory_sync: bool = false,
    /// Set once the on-disk free map has been cross-checked against every
    /// valid checkpoint slot (including the crash-safety fallback slot) for
    /// this open file handle. That full-database reachability scan is only
    /// needed to catch corruption present when the file was opened (or
    /// written to out of band); this process's own commits can only ever
    /// consume pages that a prior, already-verified free map declared free,
    /// so re-running the scan on every single mutation is unnecessary and,
    /// for large stores, quadratic in the number of commits. `check()` (via
    /// `validateReachableFreeMap`) always re-verifies regardless of this
    /// flag, for explicit integrity audits.
    free_pages_verified: bool = false,
    // Structural scaling assertions count page operations, independent of
    // filesystem speed and cache warmth. No counters exist in production.
    // Exact backing-file page reads, after shared and cursor-local caches.
    // These count read operations/bytes, not storage-device cache misses.
    test_backing_read_calls: if (builtin.is_test) @import("antfly_platform").atomic.Value(u64) else void = if (builtin.is_test) .init(0) else {},
    test_backing_read_bytes: if (builtin.is_test) @import("antfly_platform").atomic.Value(u64) else void = if (builtin.is_test) .init(0) else {},
    test_value_read_bytes: if (builtin.is_test) @import("antfly_platform").atomic.Value(u64) else void = if (builtin.is_test) .init(0) else {},
    test_value_cache_borrowed_bytes: if (builtin.is_test) @import("antfly_platform").atomic.Value(u64) else void = if (builtin.is_test) .init(0) else {},
    test_value_cache_copy_bytes: if (builtin.is_test) @import("antfly_platform").atomic.Value(u64) else void = if (builtin.is_test) .init(0) else {},
    test_value_read_calls: if (builtin.is_test) @import("antfly_platform").atomic.Value(u64) else void = if (builtin.is_test) .init(0) else {},
    test_cancel_on_read: if (builtin.is_test) ?*maintenance.CancelToken else void = if (builtin.is_test) null else {},
    test_page_reads: if (builtin.is_test) @import("antfly_platform").atomic.Value(u64) else void = if (builtin.is_test) .init(0) else {},
    test_index_view_hits: if (builtin.is_test) @import("antfly_platform").atomic.Value(u64) else void = if (builtin.is_test) .init(0) else {},
    test_index_comparisons: if (builtin.is_test) @import("antfly_platform").atomic.Value(u64) else void = if (builtin.is_test) .init(0) else {},
    test_page_writes: if (builtin.is_test) @import("antfly_platform").atomic.Value(u64) else void = if (builtin.is_test) .init(0) else {},

    test_page_write_calls: if (builtin.is_test) @import("antfly_platform").atomic.Value(u64) else void = if (builtin.is_test) .init(0) else {},
    test_page_write_fail_after: if (builtin.is_test) ?usize else void = if (builtin.is_test) null else {},

    pub fn open(allocator: Allocator, path: []const u8, read_only: bool) !NativeFile {
        return try openWithOptions(allocator, path, .{ .read_only = read_only });
    }

    pub fn openWithOptions(allocator: Allocator, path: []const u8, opts: OpenOptions) !NativeFile {
        var io_impl = threaded_io_limits.initService(allocator);
        errdefer io_impl.deinit();
        return try openWithRuntime(allocator, path, opts, io_impl, null);
    }

    /// Opens a Lite file using a caller-owned std.Io runtime. The runtime must
    /// outlive the returned file. No native thread or host-I/O escape is
    /// created by this path.
    pub fn openWithIo(allocator: Allocator, io: std.Io, path: []const u8, opts: OpenOptions) !NativeFile {
        return try openWithRuntime(allocator, path, opts, undefined, io);
    }

    fn openWithRuntime(
        allocator: Allocator,
        path: []const u8,
        opts: OpenOptions,
        io_impl: std.Io.Threaded,
        borrowed_io: ?std.Io,
    ) !NativeFile {
        var owned_io_impl = io_impl;
        const io = borrowed_io orelse owned_io_impl.io();

        const owned_path = try allocator.dupe(u8, path);
        errdefer allocator.free(owned_path);

        var writer_lock_file: ?std.Io.File = null;
        if (!opts.read_only and !opts.externally_locked) {
            const writer_lock = try acquireWriterLock(allocator, io, path);
            writer_lock_file = writer_lock.file;
        }
        errdefer if (writer_lock_file) |lock_file| lock_file.close(io);

        const opened_file = try openDataFile(io, path, if (opts.read_only) (if (opts.internal_reader or opts.externally_locked) .internal_reader else .reader) else .writer, opts.wait_for_reader_lock);
        const file = opened_file.file;
        errdefer file.close(io);

        var header_bytes: [header_size]u8 = undefined;
        try readHeaderExactAt(file, io, &header_bytes);
        var header = try decodeHeader(&header_bytes);
        const file_size = (try file.stat(io)).size;
        header.active_checkpoint = try selectCompleteCheckpointForFile(header, file_size);

        var result = NativeFile{
            .allocator = allocator,
            .io_impl = owned_io_impl,
            .borrowed_io = borrowed_io,
            .path = owned_path,
            .file = file,
            .writer_lock_file = writer_lock_file,
            .externally_locked = opts.externally_locked,
            .header = header,
            .read_only = opts.read_only,
            .no_sync = opts.no_sync,
            .page_cache_enabled = .init(true),
        };
        if (opts.resource_manager) |manager| result.page_cache.attachResourceManager(manager);
        return result;
    }

    pub fn create(allocator: Allocator, path: []const u8) !NativeFile {
        return try createWithMode(allocator, path, false, false, null, "", false);
    }

    pub fn createNew(allocator: Allocator, path: []const u8) !NativeFile {
        return try createWithMode(allocator, path, true, false, null, "", false);
    }

    pub fn createWithOptions(allocator: Allocator, path: []const u8, opts: CreateOptions) !NativeFile {
        return try createWithMode(allocator, path, opts.exclusive, opts.no_sync, opts.resource_manager, opts.writer_lock_marker, opts.indexed_reclamation);
    }

    /// Creates a Lite file using a caller-owned std.Io runtime. The runtime
    /// must outlive the returned file.
    pub fn createWithIo(allocator: Allocator, io: std.Io, path: []const u8, opts: CreateOptions) !NativeFile {
        return try createWithRuntime(allocator, path, opts.exclusive, opts.no_sync, opts.resource_manager, opts.writer_lock_marker, undefined, io, opts.indexed_reclamation);
    }

    fn createWithMode(
        allocator: Allocator,
        path: []const u8,
        exclusive: bool,
        no_sync: bool,
        resource_manager: ?*resource_manager_mod.ResourceManager,
        writer_lock_marker: []const u8,
        indexed: bool,
    ) !NativeFile {
        var io_impl = threaded_io_limits.initService(allocator);
        errdefer io_impl.deinit();
        return try createWithRuntime(allocator, path, exclusive, no_sync, resource_manager, writer_lock_marker, io_impl, null, indexed);
    }

    fn createWithRuntime(
        allocator: Allocator,
        path: []const u8,
        exclusive: bool,
        no_sync: bool,
        resource_manager: ?*resource_manager_mod.ResourceManager,
        writer_lock_marker: []const u8,
        io_impl: std.Io.Threaded,
        borrowed_io: ?std.Io,
        indexed: bool,
    ) !NativeFile {
        var owned_io_impl = io_impl;
        const io = borrowed_io orelse owned_io_impl.io();

        const owned_path = try allocator.dupe(u8, path);
        errdefer allocator.free(owned_path);

        const writer_lock = try acquireWriterLock(allocator, io, path);
        var writer_lock_file = writer_lock.file;
        errdefer writer_lock_file.close(io);
        if (writer_lock_marker.len > 0) {
            try writer_lock_file.writePositionalAll(io, writer_lock_marker, 0);
            try writer_lock_file.setLength(io, writer_lock_marker.len);
            if (!no_sync) try writer_lock_file.sync(io);
        }

        var encoded: [header_size]u8 = undefined;
        encodeHeader(&encoded, .{});

        const replace_existing = !exclusive and pathExists(io, path);
        const replacement_path = if (replace_existing)
            try realPathAlloc(allocator, io, path)
        else
            null;
        defer if (replacement_path) |canonical| allocator.free(canonical);
        const create_target = if (replacement_path) |canonical| canonical else path;
        const staging_path = if (replace_existing)
            try std.fmt.allocPrint(allocator, "{s}.tmp-aflite-create", .{create_target})
        else
            null;
        defer if (staging_path) |tmp_path| allocator.free(tmp_path);
        errdefer if (staging_path) |tmp_path| deleteFilePath(io, tmp_path) catch {};

        // Reinitializing an existing artifact is an atomic generation swap.
        // Truncating the existing inode would corrupt snapshots held by
        // concurrent read-only processes, whose shared lock intentionally
        // permits an append-only writer.
        const create_path = staging_path orelse create_target;
        var file = try createDataFile(io, create_path, .{
            .truncate = true,
            .exclusive = exclusive,
        });
        var file_open = true;
        errdefer if (file_open) file.close(io);

        try file.writePositionalAll(io, &encoded, 0);
        var initialized_header: Header = .{};
        if (indexed) {
            // Finish every fallible initialization step on the private inode.
            var staged = NativeFile{ .allocator = allocator, .io_impl = undefined, .borrowed_io = io, .path = owned_path, .file = file, .header = .{}, .read_only = false, .no_sync = no_sync, .page_cache_enabled = .init(false) };
            defer staged.invalidateLedger();
            defer staged.page_cache.deinit(allocator);
            try staged.initializeIndexedAllocator();
            initialized_header = staged.header;
        }
        if (!no_sync) {
            try file.sync(io);
        }
        if (staging_path) |tmp_path| {
            file.close(io);
            file_open = false;
            renameFilePath(io, tmp_path, create_target) catch |err| {
                deleteFilePath(io, tmp_path) catch {};
                return err;
            };
            if (!no_sync) try fs_paths.syncDirPortable(io, std.fs.path.dirname(create_target) orelse ".");
            file = (try openDataFile(io, path, .writer, false)).file;
            file_open = true;
        } else if (!no_sync) {
            try fs_paths.syncDirPortable(io, std.fs.path.dirname(create_target) orelse ".");
        }

        var result = NativeFile{
            .allocator = allocator,
            .io_impl = owned_io_impl,
            .borrowed_io = borrowed_io,
            .path = owned_path,
            .file = file,
            .writer_lock_file = writer_lock_file,
            .header = initialized_header,
            .read_only = false,
            .no_sync = no_sync,
            .page_cache_enabled = .init(true),
        };
        if (resource_manager) |manager| result.page_cache.attachResourceManager(manager);
        return result;
    }

    /// Relinquish lifetime ownership after the embedded connection finishes initialization.
    /// No mutation may run until its caller holds lockWriterPath again.
    pub fn releaseWriterOwnership(self: *NativeFile) void {
        if (self.writer_lock_file) |file| file.close(self.runtime());
        self.writer_lock_file = null;
        self.externally_locked = true;
    }

    pub fn close(self: *NativeFile) void {
        if (self.transaction_writes) |writes| writes.deinit(self.allocator);
        self.transaction_writes = null;
        const io = self.runtimeIo();
        deinitNamespaceDirectory(self.allocator, &self.namespace_directory_cache);
        self.page_cache.deinit(self.allocator);
        self.invalidateLedger();
        if (self.writer_lock_file) |lock_file| {
            lock_file.close(io);
        }
        self.file.close(io);
        self.allocator.free(self.path);
        if (self.borrowed_io == null) self.io_impl.deinit();
        self.* = undefined;
    }

    pub fn usesBorrowedIo(self: *const NativeFile) bool {
        return self.borrowed_io != null;
    }

    /// Returns the runtime used for file operations and synchronization. The
    /// returned interface is borrowed from this file and is invalid after
    /// `close`.
    pub fn runtime(self: *NativeFile) std.Io {
        return self.borrowed_io orelse self.io_impl.io();
    }

    fn runtimeIo(self: *NativeFile) std.Io {
        return self.runtime();
    }

    pub fn activeCheckpoint(self: *const NativeFile) CheckpointSlot {
        return self.header.checkpoints[self.header.active_checkpoint];
    }

    fn invalidateLedger(self: *NativeFile) void {
        if (self.ledger) |state| {
            state.deinit();
            self.allocator.destroy(state);
            self.ledger = null;
        }
    }

    fn ledgerRead(context: *anyopaque, a: Allocator, page: u64) ![]u8 {
        const file: *NativeFile = @ptrCast(@alignCast(context));
        const raw = try file.readPageAlloc(a, page);
        defer a.free(raw);
        const kind: PageKind = if (page == file.activeCheckpoint().free_map_root_page) .free_map else .allocator;
        return try a.dupe(u8, try decodePagePayload(raw, kind));
    }
    fn ledgerReadOnlyAllocate(_: *anyopaque) !u64 {
        return error.ReadOnly;
    }
    fn ledgerReadOnlyWrite(_: *anyopaque, _: u64, _: []const u8) !void {
        return error.ReadOnly;
    }

    fn loadLedger(self: *NativeFile, checkpoint: CheckpointSlot) !*allocator_v4.State {
        return self.loadLedgerWithCancel(checkpoint, self.allocator_cancel_token);
    }
    fn loadLedgerWithCancel(self: *NativeFile, checkpoint: CheckpointSlot, cancel: ?*const maintenance.CancelToken) !*allocator_v4.State {
        if (cancel) |token| try token.check();
        if (self.ledger) |state| if (state.root_page == checkpoint.free_map_root_page) return state;
        self.invalidateLedger();
        if (checkpoint.free_map_root_page != 0) {
            const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, checkpoint.free_map_root_page, .free_map, checkpoint);
            defer self.allocator.free(payload);
            if ((try allocator_v4.State.decodeRoot(payload)).covered_pages != checkpoint.page_count) return error.InvalidNativeAllocator;
        }
        const state = try self.allocator.create(allocator_v4.State);
        errdefer self.allocator.destroy(state);
        state.* = if (checkpoint.free_map_root_page == 0) allocator_v4.State.init(self.allocator) else try allocator_v4.State.load(self.allocator, .{
            .context = self,
            .allocate = ledgerReadOnlyAllocate,
            .read = ledgerRead,
            .write = ledgerReadOnlyWrite,
            .payload_bytes = self.maxPagePayloadBytes(),
            .cancel_requested = if (cancel) |token| &token.requested else null,
        }, checkpoint.free_map_root_page);
        self.ledger = state;
        return state;
    }

    const IndexedGraph = struct {
        pages: ReachablePageSet = .empty,
        nodes: ReachablePageSet = .empty,
        records: ReachablePageSet = .empty,
        values: std.AutoHashMapUnmanaged(u64, u64) = .empty,
        owners: ?*allocator_v4.State = null,
        record_count: u64 = 0,
        pub fn deinit(graph: *IndexedGraph, a: Allocator) void {
            graph.pages.deinit(a);
            graph.nodes.deinit(a);
            graph.records.deinit(a);
            graph.values.deinit(a);
        }
    };

    fn indexedMark(self: *NativeFile, graph: *IndexedGraph, checkpoint: CheckpointSlot, reference: u64) !void {
        const page = physicalPage(reference);
        if (page == 0 or page >= checkpoint.page_count) return error.InvalidPageId;
        try graph.pages.put(self.allocator, page, {});
    }

    fn indexedValue(self: *NativeFile, graph: *IndexedGraph, checkpoint: CheckpointSlot, root: u64, length: u64, cancel: ?*const maintenance.CancelToken) !void {
        const Value = struct { page: u64, length: u64, height: ?u8 = null };
        var work: std.ArrayList(Value) = .empty;
        defer work.deinit(self.allocator);
        try work.append(self.allocator, .{ .page = root, .length = length });
        while (work.pop()) |value| {
            if (cancel) |token| try token.check();
            if (value.length == 0 or physicalPage(value.page) != value.page) return error.InvalidNativeValueChain;
            try self.indexedMark(graph, checkpoint, value.page);
            if (graph.owners) |owners| try owners.retain(value.page);
            if (graph.values.get(value.page)) |old_length| {
                if (old_length != value.length) return error.InvalidNativeValueChain;
                continue;
            }
            try graph.values.put(self.allocator, value.page, value.length);
            const raw = try self.readPageAllocForCheckpoint(self.allocator, value.page, checkpoint);
            defer self.allocator.free(raw);
            switch (raw[4]) {
                @backingInt(PageKind.value) => {
                    const leaf = try decodeValuePage(try decodePagePayload(raw, .value));
                    if (leaf.chunk.len == 0 or leaf.chunk.len > value.length) return error.InvalidNativeValueChain;
                    if (value.height) |height| if (height != 0 or leaf.next_page != 0) return error.InvalidNativeValueChain;
                    const remaining = value.length - leaf.chunk.len;
                    if ((remaining == 0) != (leaf.next_page == 0)) return error.InvalidNativeValueChain;
                    if (leaf.next_page != 0) try work.append(self.allocator, .{ .page = leaf.next_page, .length = remaining });
                },
                @backingInt(PageKind.value_extent) => {
                    const node = try decodeExtentNode(try decodePagePayload(raw, .value_extent), value.length);
                    if (value.height) |height| if (height != node.height) return error.InvalidNativeValueChain;
                    for (node.children[0..node.count]) |child| try work.append(self.allocator, .{ .page = child.page, .length = child.len, .height = child.height });
                },
                else => return error.InvalidNativeValueChain,
            }
        }
    }

    fn indexedTree(self: *NativeFile, graph: *IndexedGraph, checkpoint: CheckpointSlot, page: u64, kind: PageKind, lower: ?[]const u8, upper: ?[]const u8, depth: usize, cancel: ?*const maintenance.CancelToken) anyerror!void {
        if (page == 0) return;
        if (depth > 64 or physicalPage(page) != page or graph.nodes.contains(page)) return error.InvalidDocumentIndex;
        if (cancel) |token| try token.check();
        try graph.nodes.put(self.allocator, page, {});
        try self.indexedMark(graph, checkpoint, page);
        if (graph.owners) |owners| try owners.retain(page);
        var node = try self.readDocumentIndexNode(page, checkpoint);
        defer node.deinit(self.allocator);
        for (node.keys, node.key_pages.?, 0..) |key, key_page, i| {
            if (i != 0 and std.mem.order(u8, node.keys[i - 1], key) != .lt) return error.InvalidDocumentIndex;
            if (lower) |bound| if (std.mem.order(u8, key, bound) == .lt) return error.InvalidDocumentIndex;
            if (upper) |bound| if (std.mem.order(u8, key, bound) != .lt) return error.InvalidDocumentIndex;
            if (key_page != 0) {
                if (physicalPage(key_page) != key_page) return error.InvalidDocumentIndex;
                try self.indexedMark(graph, checkpoint, key_page);
                const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, key_page, .key, checkpoint);
                defer self.allocator.free(payload);
                if (!std.mem.eql(u8, key, try decodeKey(payload))) return error.InvalidDocumentIndex;
                if (graph.owners) |owners| try owners.retain(key_page);
            }
        }
        switch (node.kind) {
            .internal => for (node.pointers, 0..) |child, i| {
                try self.indexedTree(graph, checkpoint, child, kind, if (i == 0) lower else node.keys[i - 1], if (i == node.keys.len) upper else node.keys[i], depth + 1, cancel);
            },
            .leaf => for (node.keys, node.pointers) |key, reference| {
                if (graph.records.contains(reference)) return error.InvalidDocumentIndex;
                try graph.records.put(self.allocator, reference, {});
                try self.indexedMark(graph, checkpoint, reference);
                const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, reference, kind, checkpoint);
                defer self.allocator.free(payload);
                const record: CatalogEntry = switch (kind) {
                    .catalog => try decodeCatalogEntry(payload),
                    .document => blk: {
                        const doc = try decodeDocumentEntry(payload);
                        if (doc.previous_namespace_page != 0) return error.InvalidNativePageChain;
                        break :blk .{ .previous_page = doc.previous_page, .key = doc.key, .value = doc.value, .is_delete = doc.is_delete, .external_value_root_page = doc.external_value_root_page, .external_value_len = doc.external_value_len };
                    },
                    else => unreachable,
                };
                if (record.previous_page != 0 or record.is_delete or !std.mem.eql(u8, key, record.key)) return error.InvalidDocumentIndex;
                if (graph.owners) |owners| try owners.retain(physicalPage(reference));
                if (record.external_value_root_page != 0) try self.indexedValue(graph, checkpoint, record.external_value_root_page, record.external_value_len, cancel);
                graph.record_count += 1;
            },
        }
    }

    fn indexedGraph(self: *NativeFile, checkpoint: CheckpointSlot, owners: ?*allocator_v4.State, cancel: ?*const maintenance.CancelToken) !IndexedGraph {
        var graph: IndexedGraph = .{ .owners = owners };
        errdefer graph.deinit(self.allocator);
        if (checkpoint.document_root_page != 0 or checkpoint.namespace_directory_root_page != 0) return error.InvalidNativePageChain;
        for ([_]u64{ checkpoint.catalog_root_page, checkpoint.index_catalog_root_page }) |descriptor| {
            if (descriptor == 0) continue;
            try self.indexedMark(&graph, checkpoint, descriptor);
            const roots = try self.readCatalogRoots(descriptor, checkpoint);
            if (!roots.indexed or roots.history != 0) return error.InvalidNativePageChain;
            if (owners) |state| try state.retain(descriptor);
            try self.indexedTree(&graph, checkpoint, roots.index, .catalog, null, null, 0, cancel);
        }
        try self.indexedTree(&graph, checkpoint, checkpoint.document_index_root_page, .document, null, null, 0, cancel);
        return graph;
    }

    fn validateIndexedOwnership(self: *NativeFile, checkpoint: CheckpointSlot, state: *allocator_v4.State, cancel: ?*const maintenance.CancelToken) !void {
        var expected = allocator_v4.State.init(self.allocator);
        defer expected.deinit();
        var graph = try self.indexedGraph(checkpoint, &expected, cancel);
        defer graph.deinit(self.allocator);
        var current_metadata: ReachablePageSet = .empty;
        defer current_metadata.deinit(self.allocator);
        for (state.metadata.items) |page| {
            if (cancel) |token| try token.check();
            try current_metadata.put(self.allocator, page, {});
        }
        var metadata_retirements: ReachablePageSet = .empty;
        defer metadata_retirements.deinit(self.allocator);
        var pending = state.pending.valueIterator();
        while (pending.next()) |item| {
            if (cancel) |token| try token.check();
            if (item.kind != .record and physicalPage(item.page) != item.page) return error.InvalidNativeAllocator;
            if (item.epoch > checkpoint.commit_sequence) return error.InvalidNativeAllocator;
            try self.indexedMark(&graph, checkpoint, item.page);
            switch (item.kind) {
                .allocator_chain => {
                    var page = item.page;
                    while (page != 0) {
                        if (cancel) |token| try token.check();
                        try self.indexedMark(&graph, checkpoint, page);
                        if (state.count(page) != 0 or page == state.root_page or metadata_retirements.contains(page) or current_metadata.contains(page)) return error.InvalidNativeAllocator;
                        try metadata_retirements.put(self.allocator, page, {});
                        const next = try self.retiredAllocatorNext(page, checkpoint);
                        page = if (next == item.length) 0 else next;
                    }
                },
                .metadata => {
                    if (state.count(item.page) != 0 or item.page == state.root_page or metadata_retirements.contains(item.page) or current_metadata.contains(item.page)) return error.InvalidNativeAllocator;
                    try metadata_retirements.put(self.allocator, item.page, {});
                },
                .page => try expected.retain(item.page),
                .value => try self.indexedValue(&graph, checkpoint, item.page, item.length, cancel),
                .index => {
                    if (graph.nodes.contains(item.page)) return error.InvalidNativeAllocator;
                    try graph.nodes.put(self.allocator, item.page, {});
                    try expected.retain(item.page);
                    const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, item.page, .document_index, checkpoint);
                    defer self.allocator.free(payload);
                    var node = try decodeDocumentIndexNode(self.allocator, payload);
                    defer node.deinit(self.allocator);
                    for (node.key_pages.?) |key_page| if (key_page != 0) {
                        try self.indexedMark(&graph, checkpoint, key_page);
                        try expected.retain(key_page);
                    };
                },
                .record => {
                    if (graph.records.contains(item.page)) return error.InvalidNativeAllocator;
                    try graph.records.put(self.allocator, item.page, {});
                    try expected.retain(physicalPage(item.page));
                    const raw = try self.readPageAllocForCheckpoint(self.allocator, item.page, checkpoint);
                    defer self.allocator.free(raw);
                    const root, const length = switch (raw[4]) {
                        @backingInt(PageKind.document) => blk: {
                            const record = try decodeDocumentEntry(try decodePagePayload(raw, .document));
                            break :blk .{ record.external_value_root_page, record.external_value_len };
                        },
                        @backingInt(PageKind.catalog) => blk: {
                            const record = try decodeCatalogEntry(try decodePagePayload(raw, .catalog));
                            break :blk .{ record.external_value_root_page, record.external_value_len };
                        },
                        else => return error.InvalidNativeAllocator,
                    };
                    if (root != 0) try self.indexedValue(&graph, checkpoint, root, length, cancel);
                },
            }
        }
        for (state.counts.items, 0..) |references, page| {
            if (cancel) |token| try token.check();
            const wanted = expected.count(page);
            if (references == allocator_v4.free) {
                if (wanted != 0) return error.InvalidNativeAllocator;
            } else if (references != wanted) {
                // allocatePage exposes owned raw blocks without an index edge.
                if (wanted != 0 or references != 1) return error.InvalidNativeAllocator;
                const raw = try self.readPageAllocForCheckpoint(self.allocator, page, checkpoint);
                defer self.allocator.free(raw);
                _ = try decodePagePayload(raw, .data);
            }
        }
    }

    fn validateIndexedAllocator(self: *NativeFile, checkpoint: CheckpointSlot, state: *allocator_v4.State, cancel: ?*const maintenance.CancelToken) !u64 {
        var active_records: u64 = 0;
        for (self.header.checkpoints, 0..) |slot, index| {
            if (!validCheckpointSlot(slot)) continue;
            var graph = try self.indexedGraph(slot, null, cancel);
            defer graph.deinit(self.allocator);
            if (index == self.header.active_checkpoint) active_records = graph.record_count;
            var pages = graph.pages.keyIterator();
            while (pages.next()) |page| {
                if (cancel) |token| try token.check();
                if (state.count(page.*) == allocator_v4.free or state.count(page.*) == 0) return error.InvalidNativeAllocator;
            }
            const old_payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, slot.free_map_root_page, .free_map, slot);
            defer self.allocator.free(old_payload);
            const old_root = try allocator_v4.State.decodeRoot(old_payload);
            if (old_root.covered_pages != slot.page_count or state.count(slot.free_map_root_page) == allocator_v4.free) return error.InvalidNativeAllocator;
            // The fallback may name an older ledger. Read it independently;
            // loading it into the owner's cache would discard current state.
            var old_state = try allocator_v4.State.load(self.allocator, .{ .context = self, .allocate = ledgerReadOnlyAllocate, .read = ledgerReadAny, .write = ledgerReadOnlyWrite, .payload_bytes = self.maxPagePayloadBytes(), .cancel_requested = if (cancel) |token| &token.requested else null }, slot.free_map_root_page);
            defer old_state.deinit();
            for (old_state.metadata.items) |page| {
                if (cancel) |token| try token.check();
                if (state.count(page) == allocator_v4.free) return error.InvalidNativeAllocator;
            }
        }
        if (state.root.covered_pages != checkpoint.page_count) return error.InvalidNativeAllocator;
        try self.validateIndexedOwnership(checkpoint, state, cancel);
        return active_records;
    }

    fn ledgerReadAny(context: *anyopaque, a: Allocator, page: u64) ![]u8 {
        const file: *NativeFile = @ptrCast(@alignCast(context));
        const raw = try file.readPageAlloc(a, page);
        defer a.free(raw);
        const kind: PageKind = switch (raw[4]) {
            @backingInt(PageKind.free_map) => .free_map,
            @backingInt(PageKind.allocator) => .allocator,
            else => return error.InvalidNativeAllocator,
        };
        return try a.dupe(u8, try decodePagePayload(raw, kind));
    }

    fn publicationEpoch(self: *NativeFile) u64 {
        if (self.private_retirement_epoch) |epoch| return epoch;
        const header = self.transaction_header orelse self.header;
        return header.checkpoints[header.active_checkpoint].commit_sequence + 1;
    }
    fn metadataReuseFrontier(self: *NativeFile) u64 {
        var frontier: u64 = std.math.maxInt(u64);
        const header = self.transaction_header orelse self.header;
        for (header.checkpoints) |slot| if (validCheckpointSlot(slot)) {
            frontier = @min(frontier, slot.commit_sequence);
        };
        if (self.durable_header) |durable| for (durable.checkpoints) |slot| {
            if (validCheckpointSlot(slot)) frontier = @min(frontier, slot.commit_sequence);
        };
        return frontier;
    }
    fn reuseFrontier(self: *NativeFile) u64 {
        const frontier = self.metadataReuseFrontier();
        return @min(@min(frontier, self.minimum_reader_sequence orelse frontier), if (self.reader_frontier) |readers| readers.load(.acquire) else frontier);
    }

    /// Work is bounded by retired graph objects, including value subtrees. A
    /// delete only enqueues its record; value-page traversal happens here.
    fn retiredAllocatorNext(self: *NativeFile, page: u64, checkpoint: CheckpointSlot) !u64 {
        const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, page, .allocator, checkpoint);
        defer self.allocator.free(payload);
        if (payload.len < 16 or (!std.mem.eql(u8, payload[0..4], "L4SS") and !std.mem.eql(u8, payload[0..4], "L4RQ") and !std.mem.eql(u8, payload[0..4], "L4DL"))) return error.InvalidNativeAllocator;
        return std.mem.readInt(u64, payload[8..16], .little);
    }

    fn drainRetirement(self: *NativeFile, state: *allocator_v4.State, checkpoint: CheckpointSlot, budget: usize) !usize {
        if (budget == 0) return 0;
        const frontier = self.reuseFrontier();
        const metadata_frontier = self.metadataReuseFrontier();
        // Completed data/chain pages use the metadata fence too: recovery can
        // replay an older retirement queue that still needs their bytes. Scale
        // physical reuse promotion with the maximum key fanout of data work.
        const promotions = @max(@as(usize, 8), budget *| (self.maxPagePayloadBytes() / 18 + 2));
        for (0..promotions) |_| {
            if (self.allocator_cancel_token) |token| try token.check();
            const root = state.metadata_heap.peek() orelse break;
            if (root.epoch > metadata_frontier) break;
            if (root.page == 0 or root.page >= checkpoint.page_count) return error.InvalidNativeAllocator;
            try state.complete(root);
            try state.releaseMetadata(root.page);
        }
        // Metadata service must retire more pages than its own publication
        // creates. Scale cleanup with data work and keep an eight-page minimum
        // so even a one-object budget drains its publication overhead.
        for (0..@max(@as(usize, 8), budget)) |_| {
            if (self.allocator_cancel_token) |token| try token.check();
            const item = state.chains_heap.peek() orelse break;
            if (item.epoch > metadata_frontier) break;
            if (state.pending.count() + 8 >= state.pending_limit + allocator_v4.collector_reserve) break;
            const linked_page = try self.retiredAllocatorNext(item.page, checkpoint);
            const next_page = if (linked_page == item.length) 0 else linked_page;
            try state.complete(item);
            try state.retireCollected(.{ .epoch = self.publicationEpoch(), .page = item.page, .kind = .metadata });
            if (next_page != 0) try state.retireCollected(.{ .epoch = item.epoch, .page = next_page, .length = item.length, .kind = .allocator_chain });
        }
        var done: usize = 0;
        while (done < budget) : (done += 1) {
            if (self.allocator_cancel_token) |token| try token.check();
            const item = state.heap.peek() orelse break;
            if (item.epoch > frontier) break;
            // A single node can discover value children or release many typed
            // keys. Keep publication space when collector admission is full;
            // advancing slots then promotes older completed pages first.
            const expansion: usize = switch (item.kind) {
                .value => 64 + 8,
                .index => self.maxPagePayloadBytes() / 18 + 8,
                else => 8,
            };
            if (state.pending.count() + expansion >= state.pending_limit + allocator_v4.collector_reserve) break;
            if (physicalPage(item.page) == 0 or physicalPage(item.page) >= checkpoint.page_count) return error.InvalidNativeAllocator;
            try state.complete(item);
            switch (item.kind) {
                .metadata => try state.releaseMetadata(item.page),
                .allocator_chain => return error.InvalidNativeAllocator,
                .page => {
                    _ = try self.releaseRetiredPage(state, item.page);
                },
                .record => {
                    const raw = try self.readPageAllocForCheckpoint(self.allocator, item.page, checkpoint);
                    defer self.allocator.free(raw);
                    const root, const length = switch (raw[4]) {
                        @backingInt(PageKind.document) => blk: {
                            const record = try decodeDocumentEntry(try decodePagePayload(raw, .document));
                            state.retired_inline_bytes +|= record.key.len + record.value.len;
                            break :blk .{ record.external_value_root_page, record.external_value_len };
                        },
                        @backingInt(PageKind.catalog) => blk: {
                            const record = try decodeCatalogEntry(try decodePagePayload(raw, .catalog));
                            state.retired_inline_bytes +|= record.key.len + record.value.len;
                            break :blk .{ record.external_value_root_page, record.external_value_len };
                        },
                        else => return error.InvalidNativeAllocator,
                    };
                    if (root != 0) try state.retireCollected(.{ .epoch = item.epoch, .page = root, .length = length, .kind = .value });
                    _ = try self.releaseRetiredPage(state, physicalPage(item.page));
                },
                .index => {
                    const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, item.page, .document_index, checkpoint);
                    defer self.allocator.free(payload);
                    var frame: IndexReadFrame = .{};
                    defer frame.deinit(self.allocator);
                    try frame.decode(self.allocator, payload);
                    for (0..frame.offsets.items.len) |i| {
                        const key = frame.keyReference(i);
                        if (key == 0) continue;
                        _ = try self.releaseRetiredPage(state, physicalPage(key));
                    }
                    _ = try self.releaseRetiredPage(state, item.page);
                },
                .value => {
                    // Children belong to a shared immutable node, rather than
                    // separately to every document that points at its root.
                    if (try self.releaseRetiredPage(state, item.page)) {
                        const raw = try self.readPageAllocForCheckpoint(self.allocator, item.page, checkpoint);
                        defer self.allocator.free(raw);
                        switch (raw[4]) {
                            @backingInt(PageKind.value) => {
                                const value = try decodeValuePage(try decodePagePayload(raw, .value));
                                if (value.chunk.len == 0 or value.chunk.len > item.length) return error.InvalidNativeValueChain;
                                if (value.next_page != 0) try state.retireCollected(.{ .epoch = item.epoch, .page = value.next_page, .length = item.length - value.chunk.len, .kind = .value });
                            },
                            @backingInt(PageKind.value_extent) => {
                                const node = try decodeExtentNode(try decodePagePayload(raw, .value_extent), item.length);
                                for (node.children[0..node.count]) |child| try state.retireCollected(.{ .epoch = item.epoch, .page = child.page, .length = child.len, .kind = .value });
                            },
                            else => return error.InvalidNativeValueChain,
                        }
                    }
                },
            }
        }
        return done;
    }

    fn releaseRetiredPage(self: *NativeFile, state: *allocator_v4.State, page: u64) !bool {
        const last = try state.releaseDeferred(page);
        if (last) {
            try state.retireCollected(.{ .epoch = self.publicationEpoch(), .page = page, .kind = .metadata });
            state.released_data_pages +|= 1;
        }
        return last;
    }

    fn initializeIndexedAllocator(self: *NativeFile) !void {
        if (self.activeCheckpoint().page_count != 1) return error.InvalidTransactionState;
        self.header.indexed_reclamation = true;
        self.header.packed_records = true;
        var pages = try self.pageAllocatorFromFreeMap(self.activeCheckpoint());
        defer pages.deinit();
        var checkpoint = self.activeCheckpoint();
        checkpoint.free_map_root_page = try pages.allocate();
        try self.persistAllocator(&pages, &checkpoint);
        self.header.checkpoints = .{ checkpoint, checkpoint };
        var encoded: [header_size]u8 = undefined;
        encodeHeader(&encoded, self.header);
        try self.file.writePositionalAll(self.runtimeIo(), &encoded, 0);
        try self.syncIfRequired();
    }

    fn persistAllocator(self: *NativeFile, pages: *PageAllocator, checkpoint: *CheckpointSlot) !void {
        if (!self.header.indexed_reclamation) {
            checkpoint.page_count = pages.next_page_id;
            return self.writeFreeMapPage(checkpoint.free_map_root_page, checkpoint.page_count, pages.remainingFreePages());
        }
        const state = pages.ledger orelse return error.InvalidNativeAllocator;
        var orphaned = pages.pending_values.iterator();
        while (orphaned.next()) |entry| try state.retire(.{ .epoch = self.publicationEpoch(), .page = entry.key_ptr.*, .length = entry.value_ptr.*, .kind = .value });
        pages.pending_values.clearRetainingCapacity();
        // Writers contribute bounded metadata work too, so a busy owner does
        // not depend on scheduling to compact journals before reaching quota.
        // Small tables finish in one bounded publication; large builders advance
        // at most two pages. Explicit service and private normalization supply
        // their own checkpoint budget.
        if (state.incremental and state.root.snapshot != 0 and self.retirement_work_pages != 0 and self.private_retirement_epoch == null) {
            if (state.checkpointDue() or self.budgetedCheckpointDue(state)) {
                try self.advanceAllocatorCheckpoint(pages, 2);
            }
        }
        try state.preparePersist(self.publicationEpoch(), self.maxPagePayloadBytes());
        // Consume free bits before emitting the journal/snapshot. Otherwise the
        // next open could allocate one of the allocator's own metadata pages.
        while (pages.metadata_reserve.items.len < state.pagesRequired(self.maxPagePayloadBytes())) {
            if (self.allocator_cancel_token) |token| try token.check();
            try pages.metadata_reserve.append(self.allocator, try pages.allocate());
        }
        try pages.flush();
        try state.persist(.{ .context = pages, .allocate = PageAllocator.takeMetadataPage, .read = PageAllocator.readLedgerPage, .write = PageAllocator.writeLedgerPage, .payload_bytes = self.maxPagePayloadBytes(), .cancel_requested = if (self.allocator_cancel_token) |token| &token.requested else null }, checkpoint.free_map_root_page, &pages.next_page_id);
        if (pages.metadata_cursor != pages.metadata_reserve.items.len) return error.InvalidNativeAllocator;
        checkpoint.page_count = pages.next_page_id;
        pages.ledger_persisted = true;
        if (self.reserve_retirement_capacity and !self.maintenance_publication) {
            if (self.max_file_bytes) |limit| {
                const available = (limit / self.header.page_size -| pages.next_page_id) +|
                    (if (pages.can_reuse) state.free_pages else 0);
                if (available < allocator_v4.retirementReservePages(self.maxPagePayloadBytes(), state.counts.items.len, state.pending.count())) return error.LiteStorageBudgetExceeded;
            }
        }
    }

    pub fn retirementReserveBytes(self: *NativeFile) !u64 {
        return self.retirementReserveBytesWithCancel(self.allocator_cancel_token);
    }

    pub fn retirementReserveBytesWithCancel(self: *NativeFile, cancel: ?*const maintenance.CancelToken) !u64 {
        if (cancel) |token| try token.check();
        if (!self.header.indexed_reclamation) return 0;
        const state = try self.loadLedgerWithCancel(self.activeCheckpoint(), cancel);
        return allocator_v4.retirementReservePages(self.maxPagePayloadBytes(), state.counts.items.len, state.pending.count()) *| self.header.page_size;
    }

    pub fn estimatedGenerationReserveBytes(self: *const NativeFile, compact_bytes: u64) u64 {
        if (!(self.vacuum_target_indexed or self.header.indexed_reclamation)) return 0;
        const covered = std.math.divCeil(u64, compact_bytes, self.header.page_size) catch return std.math.maxInt(u64);
        return allocator_v4.retirementReservePages(self.maxPagePayloadBytes(), covered, 0) *| self.header.page_size;
    }

    /// Creation is admitted before opening/truncating/renaming any artifact.
    pub fn minimumOwnerStorageBytes(indexed: bool) u64 {
        if (!indexed) return default_page_size;
        return (3 + allocator_v4.retirementReservePages(default_page_size - page_header_size, 3, 0)) * default_page_size;
    }

    pub const AllocatorStats = struct {
        reusable_pages: u64,
        pending_objects: u64,
        pending_data_objects: u64,
        reused_pages: u64,
        serviced_objects: u64,
    };

    pub fn allocatorStats(self: *NativeFile) !?AllocatorStats {
        return self.allocatorStatsWithCancel(self.allocator_cancel_token);
    }

    pub fn allocatorStatsWithCancel(self: *NativeFile, cancel: ?*const maintenance.CancelToken) !?AllocatorStats {
        if (cancel) |token| try token.check();
        if (!self.header.indexed_reclamation) return null;
        const state = try self.loadLedgerWithCancel(self.activeCheckpoint(), cancel);
        return .{ .reusable_pages = state.free_pages, .pending_objects = state.pending.count(), .pending_data_objects = state.data_pending, .reused_pages = state.allocations, .serviced_objects = state.collected };
    }

    pub fn retirementNeedsService(self: *NativeFile) !bool {
        return self.retirementNeedsServiceWithCancel(self.allocator_cancel_token);
    }

    pub fn retirementNeedsServiceWithCancel(self: *NativeFile, cancel: ?*const maintenance.CancelToken) !bool {
        if (cancel) |token| try token.check();
        if (!self.header.indexed_reclamation or self.read_only) return false;
        const state = try self.loadLedgerWithCancel(self.activeCheckpoint(), cancel);
        if (state.checkpointDue() or self.budgetedCheckpointDue(state)) return true;
        const excess_promotions = state.metadata_heap.count() > 2;
        if (state.data_pending == 0 and state.chains_heap.count() == 0 and !excess_promotions) return false;
        if (excess_promotions) {
            const root = state.metadata_heap.peek().?;
            if (root.epoch <= self.metadataReuseFrontier() or self.durable_header == null) return true;
        }
        if (state.eligibleWork(self.reuseFrontier(), self.metadataReuseFrontier()) != null) return true;
        // Metadata readers are fenced by recovery/durability slots, so a
        // pinned data transaction cannot prevent advancing an idle chain.
        if (state.chains_heap.count() != 0 and self.durable_header == null) return true;
        const item = state.heap.peek() orelse return false;
        // Advancing a fallback slot makes progress only if a reader or a
        // deferred durability barrier is not the remaining visibility fence.
        return item.epoch <= self.reuseFrontier() or (self.minimum_reader_sequence == null and (if (self.reader_frontier) |readers| readers.load(.acquire) == std.math.maxInt(u64) else true) and self.durable_header == null);
    }

    fn budgetedCheckpointDue(self: *NativeFile, state: *allocator_v4.State) bool {
        if (!self.reserve_retirement_capacity) return false;
        _ = self.max_file_bytes orelse return false;
        // Budgeted owners compact profitable sparse journals proactively. A
        // write may need substantially more than collector reserve; waiting
        // until reserve itself is threatened can leave a rejected large write
        // with no eligible data debt and no maintenance work to free capacity.
        // Require twice the snapshot size to amortize construction/publication
        // and avoid a checkpoint-per-service loop.
        const snapshot_bytes = allocator_v4.snapshotPages(self.maxPagePayloadBytes(), state.counts.items.len, state.pending.count()) *| self.maxPagePayloadBytes();
        return state.root.delta_bytes >= 2 *| snapshot_bytes;
    }

    fn advanceAllocatorCheckpoint(self: *NativeFile, pages: *PageAllocator, budget: usize) !void {
        const state = pages.ledger.?;
        if (state.root.build_limit == 0 and allocator_v4.snapshotPages(self.maxPagePayloadBytes(), state.counts.items.len, state.pending.count()) <= 3) {
            // Root/old-chain retirement can add a queue page and reserving
            // metadata can add a counter page. preparePersist caps this fast
            // path at four snapshot pages after those events have been added.
            state.force_checkpoint = true;
            return;
        }
        for (0..budget) |_| {
            const page = try pages.allocate();
            try state.checkpointStep(.{ .context = pages, .allocate = PageAllocator.takeMetadataPage, .read = PageAllocator.readLedgerPage, .write = PageAllocator.writeLedgerPage, .payload_bytes = self.maxPagePayloadBytes(), .cancel_requested = if (self.allocator_cancel_token) |token| &token.requested else null }, page, self.publicationEpoch());
            if (state.root.build_limit == 0) break;
        }
    }

    /// Services idle retirement and advances recovery slots independently of
    /// file shrinking. The owner serializes this with mutations and snapshots.
    pub fn reclaimPages(self: *NativeFile, budget: usize) !usize {
        return self.reclaimPagesWithCancel(budget, null);
    }

    pub fn reclaimPagesWithCancel(self: *NativeFile, budget: usize, cancel: ?*const maintenance.CancelToken) !usize {
        if (!self.header.indexed_reclamation or self.read_only) return 0;
        if (cancel) |token| try token.check();
        const previous_cancel = self.allocator_cancel_token;
        self.allocator_cancel_token = cancel;
        defer self.allocator_cancel_token = previous_cancel;
        try self.beginTransaction();
        errdefer self.abortTransaction();
        const before = self.activeCheckpoint();
        const previous_budget = self.retirement_work_pages;
        self.retirement_work_pages = 0;
        defer self.retirement_work_pages = previous_budget;
        var pages = try self.pageAllocatorFromFreeMap(before);
        defer pages.deinit();
        if (self.budgetedCheckpointDue(pages.ledger.?)) pages.ledger.?.force_checkpoint = true;
        var work = budget;
        if (self.max_file_bytes) |limit| {
            const available = (limit / self.header.page_size -| pages.next_page_id) +|
                (if (pages.can_reuse) pages.ledger.?.free_pages else 0);
            // Keep normal-sized batches when capacity permits. Under pressure
            // bound journal allocation before traversing/mutating the ledger.
            const advances = 2 *| allocator_v4.serviceJournalPages(self.maxPagePayloadBytes(), 1);
            while (work > 1 and allocator_v4.serviceJournalPages(self.maxPagePayloadBytes(), work) +| advances > available) work = work / 2 + work % 2;
        }
        const count = try self.drainRetirement(pages.ledger.?, before, work);
        const state = pages.ledger.?;
        if (state.checkpointDue()) {
            // Bound metadata construction independently of the physical ledger
            // size. Each service publication makes its partial chains durable.
            try self.advanceAllocatorCheckpoint(&pages, @max(@as(usize, 1), @min(work, 16)));
        }
        const previous_maintenance = self.maintenance_publication;
        self.maintenance_publication = true;
        defer self.maintenance_publication = previous_maintenance;
        var next = before;
        next.commit_sequence += 1;
        next.free_map_root_page = try pages.allocate();
        try self.persistAllocator(&pages, &next);
        try self.syncIfRequired();
        try self.publishCheckpoint(next);
        if (cancel) |token| try token.check();
        // No cancellation checks after checkpoint publication begins: preserve
        // the usual uncertain-outcome fencing across the durability boundary.
        try self.commitTransaction();
        return count;
    }

    pub fn check(self: *NativeFile) !CheckReport {
        return try self.checkWithCancel(null);
    }

    pub fn checkWithCancel(self: *NativeFile, cancel: ?*const maintenance.CancelToken) !CheckReport {
        if (cancel) |token| try token.check();
        return self.checkAtFileSizeWithCancel((try self.file.stat(self.runtimeIo())).size, cancel);
    }

    /// Check a pinned header against the file length captured with that header.
    /// Appends after pinning do not turn a valid snapshot into a tail error.
    pub fn checkAtFileSizeWithCancel(self: *NativeFile, file_size: u64, cancel: ?*const maintenance.CancelToken) !CheckReport {
        if (cancel) |token| try token.check();
        // Integrity checking must observe on-disk state, not cached pages.
        _ = self.page_cache_bypass.fetchAdd(1, .monotonic);
        defer _ = self.page_cache_bypass.fetchSub(1, .monotonic);

        const checkpoint = self.activeCheckpoint();
        const expected_size = try checkpointPrefixSize(checkpoint, self.header.page_size);

        const report = CheckReport{
            .valid = true,
            .file_size = file_size,
            .valid_prefix_size = @min(file_size, expected_size),
            .tail_bytes = if (file_size > expected_size) file_size - expected_size else 0,
            .record_count = if (checkpoint.page_count > 0) checkpoint.page_count - 1 else 0,
            .live_file_count = 0,
            .live_bytes = 0,
            .compact_size = expected_size,
            .reclaimable_bytes = 0,
        };

        if (checkpoint.page_count == 0) return invalidCheck(report, "invalid_page_count");
        if (file_size < expected_size) return invalidCheck(report, "truncated_file");

        if (self.header.indexed_reclamation) {
            const state = self.loadLedgerWithCancel(checkpoint, cancel) catch |err| {
                if (err == error.MaintenanceCanceled) return err;
                return invalidCheck(report, issueForPageCheckError(err));
            };
            const records = self.validateIndexedAllocator(checkpoint, state, cancel) catch |err| {
                if (err == error.MaintenanceCanceled) return err;
                return invalidCheck(report, issueForPageCheckError(err));
            };
            const live = try self.liveStats(cancel);
            var valid = report;
            valid.record_count = records;
            valid.live_file_count = live.record_count;
            valid.live_bytes = live.bytes;
            valid.compact_size = live.compact_size;
            valid.reclaimable_bytes = file_size -| live.compact_size;
            if (valid.tail_bytes != 0) return invalidCheck(valid, "tail_bytes");
            return valid;
        }
        var reachable_pages = std.AutoHashMapUnmanaged(u64, void){};
        defer reachable_pages.deinit(self.allocator);

        const catalog_records = self.countReachableChainPagesWithCancel(.catalog, checkpoint.catalog_root_page, &reachable_pages, cancel) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        if (cancel) |token| try token.check();
        const namespace_directory_records = self.countReachableChainPagesWithCancel(.catalog, checkpoint.namespace_directory_root_page, &reachable_pages, cancel) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        if ((checkpoint.document_root_page == 0 and namespace_directory_records != 0) or
            (checkpoint.document_root_page != 0 and namespace_directory_records == 0))
            return invalidCheck(report, "invalid_namespace_directory");
        self.validateNamespaceDirectory(checkpoint) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        if (cancel) |token| try token.check();
        const index_catalog_records = self.countReachableChainPagesWithCancel(.catalog, checkpoint.index_catalog_root_page, &reachable_pages, cancel) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        const document_records = self.countReachableChainPagesWithCancel(.document, checkpoint.document_root_page, &reachable_pages, cancel) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        if (checkpoint.document_root_page == 0 and checkpoint.document_index_root_page != 0)
            return invalidCheck(report, "invalid_document_index");
        const document_index_pages = self.collectDocumentIndexPages(checkpoint, &reachable_pages, true, true, cancel) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        self.validateDocumentIndexCoverage(checkpoint, cancel) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        if (cancel) |token| try token.check();
        self.validateReachableFreeMap(checkpoint, &reachable_pages) catch |err| {
            return invalidCheck(report, issueForPageCheckError(err));
        };
        const live = self.liveStats(cancel) catch |err| {
            if (err == error.MaintenanceCanceled) return err;
            return invalidCheck(report, issueForPageCheckError(err));
        };

        var valid = report;
        _ = document_index_pages;
        valid.record_count = catalog_records + index_catalog_records + document_records;
        valid.live_file_count = live.record_count;
        valid.live_bytes = live.bytes;
        valid.compact_size = live.compact_size;
        valid.reclaimable_bytes = if (file_size > live.compact_size) file_size - live.compact_size else 0;
        if (valid.tail_bytes != 0) return invalidCheck(valid, "tail_bytes");
        return valid;
    }

    pub fn allocatePage(self: *NativeFile, contents: []const u8) !u64 {
        try self.flushTransactionWrites();
        if (self.read_only) return error.ReadOnly;
        const previous = self.activeCheckpoint();
        var page_allocator = try self.pageAllocatorFromFreeMap(previous);
        defer page_allocator.deinit();

        const page_id = try page_allocator.allocate();
        try page_allocator.writePage(page_id, .data, contents);

        var next = previous;
        next.commit_sequence += 1;
        next.free_map_root_page = try page_allocator.allocate();
        next.page_count = page_allocator.next_page_id;

        try page_allocator.flush();
        try self.persistAllocator(&page_allocator, &next);
        try self.syncIfRequired();

        try self.publishCheckpoint(next);
        return page_id;
    }

    pub fn readPageAlloc(self: *NativeFile, allocator: Allocator, page_id: u64) ![]u8 {
        return try self.readPageAllocForCheckpoint(allocator, page_id, self.activeCheckpoint());
    }

    fn readPageAllocForCheckpoint(self: *NativeFile, allocator: Allocator, page_id: u64, checkpoint: CheckpointSlot) ![]u8 {
        const page = try self.readPhysicalPageAlloc(allocator, physicalPage(page_id), checkpoint);
        errdefer allocator.free(page);
        if (page_id & packed_record_flag != 0) try unpackRecordPage(page, page_id);
        return page;
    }

    fn admitPageToCache(self: *const NativeFile, page: []const u8) bool {
        if (page.len <= 4 or page[4] == @backingInt(PageKind.free_map)) return false;
        if (self.page_cache_policy == .normal) return true;
        return switch (page[4]) {
            @backingInt(PageKind.document_index), @backingInt(PageKind.catalog_index), @backingInt(PageKind.value_extent) => true,
            else => false,
        };
    }

    fn readPhysicalPageAlloc(self: *NativeFile, allocator: Allocator, page_id: u64, checkpoint: CheckpointSlot) ![]u8 {
        if (builtin.is_test) if (self.test_cancel_on_read) |token| token.request();
        if (builtin.is_test) _ = self.test_page_reads.fetchAdd(1, .monotonic);
        if (page_id == 0 or page_id >= checkpoint.page_count) return error.InvalidPageId;

        const use_cache = self.page_cache_enabled.load(.monotonic) and self.page_cache_bypass.load(.monotonic) == 0;
        if (use_cache) {
            if (try self.page_cache.getCopy(allocator, page_id)) |cached| return cached;
        }

        const page_size: usize = @intCast(self.header.page_size);
        const page = try allocator.alloc(u8, page_size);
        errdefer allocator.free(page);

        try self.readBackingPages(page, page_id * @as(u64, self.header.page_size));
        // Free-map pages are rewritten every commit and read once, so caching
        // them buys nothing; skipping them also keeps free-map validation
        // reading disk truth before any pages are handed out for reuse.
        if (use_cache and self.admitPageToCache(page)) {
            self.page_cache.put(self.allocator, page_id, page);
        }
        return page;
    }

    fn readBackingPages(self: *NativeFile, out: []u8, offset: u64) !void {
        if (builtin.is_test) {
            _ = self.test_backing_read_calls.fetchAdd(1, .monotonic);
            _ = self.test_backing_read_bytes.fetchAdd(out.len, .monotonic);
        }
        try readExactAt(self.file, self.runtimeIo(), out, offset);
    }

    /// Bounded scratch reads for point lookups avoid allocating decoded keys
    /// and page copies at every level of the catalog/document B-tree.
    fn readPageInto(self: *NativeFile, page_id: u64, checkpoint: CheckpointSlot, scratch: []u8) ![]const u8 {
        if (scratch.len < self.header.page_size) return error.InvalidNativePageLength;
        if (page_id & packed_record_flag != 0) {
            _ = try self.readPageInto(physicalPage(page_id), checkpoint, scratch);
            try unpackRecordPage(scratch[0..self.header.page_size], page_id);
            return scratch[0..self.header.page_size];
        }
        if (builtin.is_test) _ = self.test_page_reads.fetchAdd(1, .monotonic);
        if (page_id == 0 or page_id >= checkpoint.page_count) return error.InvalidPageId;
        const page = scratch[0..self.header.page_size];
        const use_cache = self.page_cache_enabled.load(.monotonic) and self.page_cache_bypass.load(.monotonic) == 0;
        if (use_cache and self.page_cache.copyInto(page_id, page)) return page;
        try self.readBackingPages(page, page_id * @as(u64, self.header.page_size));
        if (use_cache and self.admitPageToCache(page)) self.page_cache.put(self.allocator, page_id, page);
        return page;
    }

    pub fn readPagePayloadAlloc(self: *NativeFile, allocator: Allocator, page_id: u64) ![]u8 {
        const page = try self.readPageAlloc(allocator, page_id);
        defer allocator.free(page);
        return try decodePagePayloadAlloc(allocator, page, .data);
    }

    pub fn putCatalogRecord(self: *NativeFile, key: []const u8, value: []const u8) !void {
        try self.putCatalogBatch(&.{.{ .key = key, .value = value }});
    }

    pub fn deleteCatalogRecord(self: *NativeFile, key: []const u8) !void {
        try self.putCatalogBatch(&.{.{ .key = key, .is_delete = true }});
    }

    pub fn putCatalogBatch(self: *NativeFile, mutations: []const CatalogMutation) !void {
        return try self.putCatalogBatchForRoot(.metadata, mutations, .{});
    }

    pub fn putIndexCatalogRecord(self: *NativeFile, key: []const u8, value: []const u8) !void {
        try self.putIndexCatalogRecordWithOptions(key, value, .{});
    }

    pub fn putIndexCatalogRecordWithOptions(self: *NativeFile, key: []const u8, value: []const u8, options: WriteOptions) !void {
        try self.putCatalogBatchForRoot(.index, &.{.{ .key = key, .value = value }}, options);
    }

    /// Import a private, seekable staging file using bounded buffers. The
    /// caller serializes publication with other native mutations and keeps the
    /// source alive and unchanged throughout this call. No staged data enters
    /// the committed catalog until the final checkpoint publication.
    pub fn putIndexCatalogRecordFromFile(self: *NativeFile, key: []const u8, source: std.Io.File, len: usize, options: WriteOptions) !void {
        if (self.change_capture) |capture| capture.record(self.allocator, .index, key);
        if (self.read_only) return error.ReadOnly;
        if (key.len > catalog_key_len_mask or len > std.math.maxInt(u32)) return error.RecordTooLarge;
        const fixed_len = 16 + key.len;
        if (fixed_len > self.maxPagePayloadBytes()) return error.PageTooLarge;
        var buffer: [65536]u8 = undefined;
        if (len <= self.maxPagePayloadBytes() - fixed_len) {
            try readExactAt(source, self.runtimeIo(), buffer[0..len], 0);
            return try self.putIndexCatalogRecordWithOptions(key, buffer[0..len], options);
        }
        if (fixed_len + 8 > self.maxPagePayloadBytes()) return error.PageTooLarge;
        const previous = self.activeCheckpoint();
        const roots = try self.readCatalogRoots(previous.index_catalog_root_page, previous);
        var local_pages: PageAllocator = undefined;
        if (!self.stagingTransaction()) local_pages = try self.pageAllocatorFromFreeMap(previous);
        defer if (!self.stagingTransaction()) local_pages.deinit();
        const pages = if (self.stagingTransaction()) &(try self.transactionWrites()).pages else &local_pages;
        var builder = ExtentAppender{ .file = self, .pages = pages, .tail = undefined, .batch = .{ .file = self, .options = options } };
        defer builder.deinit();
        const chunk_size = self.maxValuePagePayloadBytes();
        const read_size = buffer.len / chunk_size * chunk_size;
        var offset: usize = 0;
        while (offset < len) {
            const n = @min(read_size, len - offset);
            try readExactAt(source, self.runtimeIo(), buffer[0..n], offset);
            var pos: usize = 0;
            while (pos < n) {
                const end = @min(n, pos + chunk_size);
                try builder.pushValue(buffer[pos..end]);
                pos = end;
            }
            offset += n;
        }
        const value = try builder.finish();
        if (self.stagingTransaction()) {
            try self.flushTransactionPayloads();
            return try self.putIndexCatalogBatch(&.{.{ .key = key, .external_value_root_page = value.page, .external_value_len = len }});
        }
        var payload = std.ArrayListUnmanaged(u8).empty;
        defer payload.deinit(self.allocator);
        try encodeCatalogEntryRaw(self.allocator, &payload, .{ .previous_page = if (self.header.indexed_reclamation) 0 else roots.history, .key = key, .external_value_root_page = value.page, .external_value_len = len });
        const record = try pages.allocate();
        try pages.writePage(record, .catalog, payload.items);
        var editor = IndexEditor.init(self, previous, roots.index);
        editor.accounting_pages = pages;
        defer editor.deinit();
        try editor.put(key, record);
        const index = try editor.finish(pages);
        var next = previous;
        next.commit_sequence += 1;
        next.index_catalog_root_page = try self.writeCatalogRoot(pages, record, index, previous.index_catalog_root_page);
        next.free_map_root_page = try pages.allocate();
        next.page_count = pages.next_page_id;
        try pages.flush();
        try self.persistAllocator(pages, &next);
        try self.syncIfRequired();
        try self.publishCheckpoint(next);
    }

    pub fn appendIndexCatalogRecord(self: *NativeFile, key: []const u8, suffix: []const u8) !void {
        try self.appendCatalogRecordForRoot(.index, key, suffix);
    }

    pub fn deleteIndexCatalogRecord(self: *NativeFile, key: []const u8) !void {
        try self.putIndexCatalogBatch(&.{.{ .key = key, .is_delete = true }});
    }

    pub fn renameIndexCatalogRecord(self: *NativeFile, old_key: []const u8, new_key: []const u8) !void {
        try self.renameCatalogRecordForRoot(.index, old_key, new_key);
    }

    pub fn putIndexCatalogBatch(self: *NativeFile, mutations: []const CatalogMutation) !void {
        return try self.putCatalogBatchForRoot(.index, mutations, .{});
    }

    const CatalogRoots = struct { history: u64, index: u64 = 0, indexed: bool = false };

    fn readCatalogRoots(self: *NativeFile, page: u64, checkpoint: CheckpointSlot) !CatalogRoots {
        if (page == 0) return .{ .history = 0 };
        var scratch: [65536]u8 = undefined;
        const raw = try self.readPageInto(page, checkpoint, &scratch);
        if (raw[4] != @backingInt(PageKind.catalog_index)) {
            // Namespace-directory records have their own delta-chain format.
            // Metadata and index catalogs must carry the revision-3 descriptor.
            if (page != checkpoint.namespace_directory_root_page) return error.UnexpectedNativePageKind;
            _ = try decodePagePayload(raw, .catalog);
            return .{ .history = page };
        }
        const payload = try decodePagePayload(raw, .catalog_index);
        if (payload.len != 16) return error.InvalidNativePageChain;
        const history = std.mem.readInt(u64, payload[0..8], .little);
        const index = std.mem.readInt(u64, payload[8..16], .little);
        if ((!self.header.indexed_reclamation and history == 0) or physicalPage(history) >= checkpoint.page_count or index >= checkpoint.page_count) return error.InvalidNativePageChain;
        return .{ .history = history, .index = index, .indexed = true };
    }

    fn catalogHistoryRoot(self: *NativeFile, checkpoint: CheckpointSlot, root: CatalogRoot) !u64 {
        return (try self.readCatalogRoots(catalogRootPage(checkpoint, root), checkpoint)).history;
    }

    fn lookupCatalogPage(self: *NativeFile, checkpoint: CheckpointSlot, root: CatalogRoot, key: []const u8) !?u64 {
        const roots = try self.readCatalogRoots(catalogRootPage(checkpoint, root), checkpoint);
        var indexed = checkpoint;
        indexed.document_index_root_page = roots.index;
        return try self.lookupDocumentIndexPage(indexed, key);
    }

    fn upsertCatalogIndex(self: *NativeFile, pages: *PageAllocator, index: u64, key: []const u8, page: u64) !u64 {
        var checkpoint = self.activeCheckpoint();
        checkpoint.page_count = pages.next_page_id;
        return try self.upsertDocumentIndex(pages, index, key, page, checkpoint);
    }

    fn writeCatalogRoot(self: *NativeFile, pages: *PageAllocator, history: u64, index: u64, previous_root: u64) !u64 {
        if (self.header.indexed_reclamation and index == 0) {
            try pages.retire(previous_root, .page);
            return 0;
        }
        var payload: [16]u8 = undefined;
        std.mem.writeInt(u64, payload[0..8], if (self.header.indexed_reclamation) 0 else history, .little);
        std.mem.writeInt(u64, payload[8..16], index, .little);
        const page = try pages.allocate();
        try pages.writePage(page, .catalog_index, &payload);
        try pages.retire(previous_root, .page);
        return page;
    }

    fn validateCatalogIndexEntries(self: *NativeFile, checkpoint: CheckpointSlot, index: u64, reachable: *ReachablePageSet) !void {
        var records = RecordPageReader{};
        defer records.deinit(self.allocator);
        var indexed = checkpoint;
        indexed.document_index_root_page = index;
        var cursor = DocumentIndexCursor.init(self, indexed);
        defer cursor.deinit();
        var current = try cursor.first();
        while (current) |entry| {
            var owned = entry;
            defer owned.deinit(self.allocator);
            if (!reachable.contains(owned.document_page_id)) return error.InvalidNativePageChain;
            const payload = try records.read(self, self.allocator, checkpoint, owned.document_page_id, .catalog);
            const record = try decodeCatalogEntry(payload);
            if (!std.mem.eql(u8, record.key, owned.key)) return error.InvalidNativePageChain;
            current = try cursor.next();
        }
    }

    fn putCatalogBatchForRoot(self: *NativeFile, root: CatalogRoot, mutations: []const CatalogMutation, options: WriteOptions) anyerror!void {
        if (self.read_only) return error.ReadOnly;
        if (mutations.len == 0) return;
        for (mutations) |mutation| try self.validateCatalogMutation(mutation);
        if (self.stagingTransaction()) for (mutations) |mutation| self.discardCatalogAppend(root, mutation.key);
        if (self.change_capture) |capture| for (mutations) |mutation| capture.record(self.allocator, if (root == .metadata) .metadata else .index, mutation.key);

        // Small index files include WAL control records, which are frequently
        // reset to the same contents. Avoid growing catalog history for those
        // writes. Bound comparison work to one inline record; bulk mutations
        // and spilled values retain the normal publication path.
        if (!self.flushing_transaction and root == .index and mutations.len == 1 and !self.checkpoint_publication_uncertain) {
            const mutation = mutations[0];
            const size = try self.getCatalogRecordSizeFromRoot(root, mutation.key);
            if (mutation.is_delete and size == null) return self.syncIfRequired();
            if (mutation.external_value_root_page == 0 and !mutation.is_delete and size != null and size.? == mutation.value.len and self.catalogEntryFitsInline(mutation.key, mutation.value)) {
                const existing = try self.getCatalogRecordFromRootAlloc(self.allocator, root, mutation.key);
                defer if (existing) |bytes| self.allocator.free(bytes);
                if (existing) |bytes| {
                    if (std.mem.eql(u8, bytes, mutation.value)) return self.syncIfRequired();
                }
            }
        }

        if (self.stagingTransaction() and mutations.len >= TransactionWrites.max_keys) {
            // The caller already owns a complete batch until this call returns.
            // Consume it directly instead of copying it into bounded staging
            // and rebuilding the same index for every staging-sized chunk.
            try self.flushTransactionWrites();
            const writes = try self.transactionWrites();
            self.flushing_transaction = true;
            self.transaction_pages = &writes.pages;
            defer {
                self.transaction_pages = null;
                self.flushing_transaction = false;
            }
            try self.putCatalogBatchForRoot(root, mutations, options);
            try writes.pages.flush();
            return;
        }
        if (self.stagingTransaction()) {
            for (mutations) |m| try self.stageMutation(if (root == .metadata) .metadata else .index, .{ .key = m.key, .value = m.value, .is_delete = m.is_delete, .external_value_root_page = m.external_value_root_page, .external_value_len = m.external_value_len }, options);
            return;
        }

        const previous = self.activeCheckpoint();
        const roots = try self.readCatalogRoots(catalogRootPage(previous, root), previous);
        var next_root_page = roots.history;
        var local_pages: PageAllocator = undefined;
        if (self.transaction_pages == null) local_pages = try self.pageAllocatorFromFreeMap(previous);
        defer if (self.transaction_pages == null) local_pages.deinit();
        const page_allocator = self.transaction_pages orelse &local_pages;
        page_allocator.pack_records = self.header.packed_records and (mutations.len > 1 or self.transaction_pages != null);
        var editor = IndexEditor.init(self, previous, roots.index);
        defer editor.deinit();
        editor.useSortedBatch(page_allocator, mutations);

        for (mutations) |mutation| {
            if (self.header.indexed_reclamation and mutation.is_delete) {
                try editor.remove(mutation.key);
                continue;
            }
            var external_value_root_page: u64 = mutation.external_value_root_page;
            if (external_value_root_page == 0 and !mutation.is_delete and !self.catalogEntryFitsInline(mutation.key, mutation.value)) {
                external_value_root_page = try self.writeCatalogValue(page_allocator, mutation.value, options);
            }

            const page_id = try page_allocator.writeCatalog(.{
                .previous_page = if (self.header.indexed_reclamation) 0 else next_root_page,
                .key = mutation.key,
                .value = mutation.value,
                .is_delete = mutation.is_delete,
                .external_value_root_page = external_value_root_page,
                .external_value_len = mutation.external_value_len,
            });
            next_root_page = page_id;
            if (mutation.is_delete) {
                try page_allocator.retire(page_id, .record);
                try editor.remove(mutation.key);
            } else try editor.put(mutation.key, page_id);
        }

        const key_index = try editor.finish(page_allocator);
        var next = previous;
        next.commit_sequence += 1;
        setCatalogRootPage(&next, root, try self.writeCatalogRoot(page_allocator, next_root_page, key_index, catalogRootPage(previous, root)));
        if (self.transaction_pages == null) next.free_map_root_page = try page_allocator.allocate();
        next.page_count = page_allocator.next_page_id;

        if (self.transaction_pages == null) {
            try page_allocator.flush();
            try self.persistAllocator(page_allocator, &next);
            try self.syncIfRequired();
        }

        try self.publishCheckpoint(next);
    }

    fn transactionCatalogEntry(self: *NativeFile, root: CatalogRoot, key: []const u8, scratch: *[65536]u8) !?CatalogEntry {
        try self.finishCatalogAppend(root, key);
        const checkpoint = self.activeCheckpoint();
        if (self.stagedEntry(if (root == .metadata) .metadata else .index, key)) |entry| return if (entry.is_delete) null else entry;
        const page = (try self.lookupCatalogPage(checkpoint, root, key)) orelse return null;
        const payload = try decodePagePayload(try self.readPageInto(page, checkpoint, scratch), .catalog);
        const entry = try decodeCatalogEntry(payload);
        if (!std.mem.eql(u8, entry.key, key)) return error.InvalidNativePageChain;
        return if (entry.is_delete) null else entry;
    }

    fn stageCatalogAppend(self: *NativeFile, root: CatalogRoot, key: []const u8, suffix: []const u8) !void {
        if (self.transaction_writes) |writes| {
            if (writes.appends[@backingInt(root)].get(key)) |state| {
                if (suffix.len == 0) return;
                try state.append(suffix);
                self.header.checkpoints[self.header.active_checkpoint].commit_sequence += 1;
                return;
            }
            if (writes.append_count >= TransactionWrites.max_append_files) try self.flushTransactionWrites();
        }
        var scratch: [65536]u8 = undefined;
        const entry = (try self.transactionCatalogEntry(root, key, &scratch)) orelse
            return self.putCatalogBatchForRoot(root, &.{.{ .key = key, .value = suffix }}, .{});
        if (suffix.len == 0) return;
        const old_len = if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len;
        const len = try std.math.add(usize, old_len, suffix.len);
        if (len > std.math.maxInt(u32)) return error.RecordTooLarge;
        const fixed_len = 16 + key.len;
        if (fixed_len > self.maxPagePayloadBytes()) return error.PageTooLarge;
        if (len <= self.maxPagePayloadBytes() - fixed_len) {
            const value = try self.allocator.alloc(u8, len);
            defer self.allocator.free(value);
            if (entry.external_value_root_page == 0) @memcpy(value[0..old_len], entry.value) else {
                const old = try self.catalogEntryValueAlloc(self.allocator, entry);
                defer self.allocator.free(old);
                @memcpy(value[0..old_len], old);
            }
            @memcpy(value[old_len..], suffix);
            return self.putCatalogBatchForRoot(root, &.{.{ .key = key, .value = value }}, .{});
        }
        if (fixed_len + 8 > self.maxPagePayloadBytes()) return error.PageTooLarge;
        // Own the base entry before any staging spill can release its bytes.
        // Its slot also owns the key borrowed by the append-state map.
        try self.stageMutation(if (root == .metadata) .metadata else .index, .{
            .key = key,
            .value = entry.value,
            .external_value_root_page = entry.external_value_root_page,
            .external_value_len = entry.external_value_len,
        }, .{});
        const writes = self.transaction_writes.?;
        const base = writes.maps[@backingInt(root)].get(key).?;
        const state = try CatalogAppendState.create(self, &writes.pages, TransactionWrites.entry(base));
        errdefer state.destroy();
        try state.append(suffix);
        try writes.appends[@backingInt(root)].put(self.allocator, base.key, state);
        writes.append_count += 1;
    }

    fn discardCatalogAppend(self: *NativeFile, root: CatalogRoot, key: []const u8) void {
        const writes = self.transaction_writes orelse return;
        if (writes.appends[@backingInt(root)].fetchRemove(key)) |removed| {
            removed.value.destroy();
            writes.append_count -= 1;
        }
    }

    // Writer-only visibility barrier. Pinned checkpoint APIs deliberately never
    // touch append frontiers or any other private transaction state.
    fn finishCatalogAppend(self: *NativeFile, root: CatalogRoot, key: []const u8) !void {
        if (!self.stagingTransaction()) return;
        const writes = self.transaction_writes orelse return;
        const map = &writes.appends[@backingInt(root)];
        const state = map.get(key) orelse return;
        try self.finishCatalogAppendState(writes, root, key, state);
        _ = map.remove(key);
        state.destroy();
        writes.append_count -= 1;
    }

    fn finishCatalogAppendState(self: *NativeFile, writes: *TransactionWrites, root: CatalogRoot, key: []const u8, state: *CatalogAppendState) !void {
        const ref = try state.finish();
        const mutation = writes.maps[@backingInt(root)].getPtr(key).?;
        writes.bytes -= mutation.value.len;
        self.allocator.free(mutation.value);
        mutation.value = &.{};
        mutation.external_value_root_page = ref.page;
        mutation.external_value_len = state.len;
        self.header.checkpoints[self.header.active_checkpoint].page_count = writes.pages.next_page_id;
    }

    fn finishCatalogAppends(self: *NativeFile, writes: *TransactionWrites) !void {
        for (&writes.appends, 0..) |*map, root| {
            var it = map.iterator();
            while (it.next()) |entry| {
                try self.finishCatalogAppendState(writes, @fromBackingInt(@intCast(root)), entry.key_ptr.*, entry.value_ptr.*);
                entry.value_ptr.*.destroy();
                _ = map.remove(entry.key_ptr.*);
                writes.append_count -= 1;
            }
        }
    }

    fn stageCatalogRename(self: *NativeFile, root: CatalogRoot, old_key: []const u8, new_key: []const u8) !void {
        var scratch: [65536]u8 = undefined;
        const entry = (try self.transactionCatalogEntry(root, old_key, &scratch)) orelse return;
        // put validates the destination (which may require spilling an inline
        // value after a longer rename) before changing either key.
        try self.putCatalogBatchForRoot(root, &.{.{ .key = new_key, .value = entry.value, .external_value_root_page = entry.external_value_root_page, .external_value_len = entry.external_value_len }}, .{});
        try self.putCatalogBatchForRoot(root, &.{.{ .key = old_key, .is_delete = true }}, .{});
    }

    fn appendCatalogRecordForRoot(self: *NativeFile, root: CatalogRoot, key: []const u8, suffix: []const u8) !void {
        if (self.change_capture) |capture| capture.record(self.allocator, if (root == .metadata) .metadata else .index, key);
        if (self.read_only) return error.ReadOnly;

        if (self.stagingTransaction()) return self.stageCatalogAppend(root, key, suffix);

        const previous = self.activeCheckpoint();
        const found_page = (try self.lookupCatalogPage(previous, root, key)) orelse {
            return try self.putCatalogBatchForRoot(root, &.{.{ .key = key, .value = suffix }}, .{});
        };
        const found_payload = try self.readPagePayloadByKindAlloc(self.allocator, found_page, .catalog);
        defer self.allocator.free(found_payload);
        const entry = try decodeCatalogEntry(found_payload);
        if (!std.mem.eql(u8, entry.key, key)) return error.InvalidNativePageChain;
        if (entry.is_delete) return try self.putCatalogBatchForRoot(root, &.{.{ .key = key, .value = suffix }}, .{});

        const old_len = if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len;
        const total_len = try std.math.add(usize, old_len, suffix.len);

        const roots = try self.readCatalogRoots(catalogRootPage(previous, root), previous);
        var next_root_page = roots.history;
        var page_allocator = try self.pageAllocatorFromFreeMap(previous);
        defer page_allocator.deinit();
        var key_index = roots.index;

        var external_value_root_page: u64 = 0;
        var inline_value: []u8 = &.{};
        defer if (inline_value.len > 0) self.allocator.free(inline_value);

        const fixed_len = 16 + key.len;
        const fits_inline = fixed_len <= self.maxPagePayloadBytes() and total_len <= self.maxPagePayloadBytes() - fixed_len;
        if (fits_inline) {
            inline_value = try self.allocator.alloc(u8, total_len);
            if (entry.external_value_root_page != 0) {
                const old_value = try self.readValuePagesAlloc(self.allocator, entry.external_value_root_page, entry.external_value_len);
                defer self.allocator.free(old_value);
                @memcpy(inline_value[0..old_value.len], old_value);
            } else {
                @memcpy(inline_value[0..entry.value.len], entry.value);
            }
            @memcpy(inline_value[old_len..], suffix);
        } else {
            external_value_root_page = try self.appendCatalogValueTree(&page_allocator, entry, suffix);
        }

        const catalog_page_id = try page_allocator.allocate();
        var payload = std.ArrayListUnmanaged(u8).empty;
        defer payload.deinit(self.allocator);
        try encodeCatalogEntryRaw(self.allocator, &payload, .{
            .previous_page = if (self.header.indexed_reclamation) 0 else next_root_page,
            .key = key,
            .value = inline_value,
            .external_value_root_page = external_value_root_page,
            .external_value_len = if (external_value_root_page != 0) total_len else 0,
        });
        try page_allocator.writePage(catalog_page_id, .catalog, payload.items);
        next_root_page = catalog_page_id;
        key_index = try self.upsertCatalogIndex(&page_allocator, key_index, key, catalog_page_id);

        var next = previous;
        next.commit_sequence += 1;
        setCatalogRootPage(&next, root, try self.writeCatalogRoot(&page_allocator, next_root_page, key_index, catalogRootPage(previous, root)));
        next.free_map_root_page = try page_allocator.allocate();
        next.page_count = page_allocator.next_page_id;

        try page_allocator.flush();
        try self.persistAllocator(&page_allocator, &next);
        try self.syncIfRequired();

        try self.publishCheckpoint(next);
    }

    fn renameCatalogRecordForRoot(self: *NativeFile, root: CatalogRoot, old_key: []const u8, new_key: []const u8) !void {
        if (self.change_capture) |capture| {
            capture.record(self.allocator, if (root == .metadata) .metadata else .index, old_key);
            capture.record(self.allocator, if (root == .metadata) .metadata else .index, new_key);
        }
        if (self.read_only) return error.ReadOnly;
        if (std.mem.eql(u8, old_key, new_key)) return;

        if (self.stagingTransaction()) return self.stageCatalogRename(root, old_key, new_key);

        const previous = self.activeCheckpoint();
        const found_page = (try self.lookupCatalogPage(previous, root, old_key)) orelse {
            return;
        };
        const found_payload = try self.readPagePayloadByKindAlloc(self.allocator, found_page, .catalog);
        defer self.allocator.free(found_payload);
        const entry = try decodeCatalogEntry(found_payload);
        if (!std.mem.eql(u8, entry.key, old_key)) return error.InvalidNativePageChain;
        if (entry.is_delete) return;

        const roots = try self.readCatalogRoots(catalogRootPage(previous, root), previous);
        var next_root_page = roots.history;
        var page_allocator = try self.pageAllocatorFromFreeMap(previous);
        defer page_allocator.deinit();
        var editor = IndexEditor.init(self, previous, roots.index);
        editor.accounting_pages = &page_allocator;
        defer editor.deinit();

        var external_value_root_page: u64 = 0;
        if (entry.external_value_root_page != 0) {
            external_value_root_page = entry.external_value_root_page;
        }

        {
            const new_page_id = try page_allocator.allocate();
            var payload = std.ArrayListUnmanaged(u8).empty;
            defer payload.deinit(self.allocator);
            try encodeCatalogEntryRaw(self.allocator, &payload, .{
                .previous_page = if (self.header.indexed_reclamation) 0 else next_root_page,
                .key = new_key,
                .value = entry.value,
                .external_value_root_page = external_value_root_page,
                .external_value_len = entry.external_value_len,
            });
            try page_allocator.writePage(new_page_id, .catalog, payload.items);
            next_root_page = new_page_id;
            try editor.put(new_key, new_page_id);
        }

        if (self.header.indexed_reclamation) {
            try editor.remove(old_key);
        } else {
            const tombstone_page_id = try page_allocator.allocate();
            var payload = std.ArrayListUnmanaged(u8).empty;
            defer payload.deinit(self.allocator);
            try encodeCatalogEntryRaw(self.allocator, &payload, .{
                .previous_page = if (self.header.indexed_reclamation) 0 else next_root_page,
                .key = old_key,
                .is_delete = true,
            });
            try page_allocator.writePage(tombstone_page_id, .catalog, payload.items);
            next_root_page = tombstone_page_id;
            try editor.remove(old_key);
        }

        const key_index = try editor.finish(&page_allocator);
        var next = previous;
        next.commit_sequence += 1;
        setCatalogRootPage(&next, root, try self.writeCatalogRoot(&page_allocator, next_root_page, key_index, catalogRootPage(previous, root)));
        next.free_map_root_page = try page_allocator.allocate();
        next.page_count = page_allocator.next_page_id;

        try page_allocator.flush();
        try self.persistAllocator(&page_allocator, &next);
        try self.syncIfRequired();

        try self.publishCheckpoint(next);
    }

    pub fn getCatalogRecordAlloc(self: *NativeFile, allocator: Allocator, key: []const u8) !?[]u8 {
        return try self.getCatalogRecordFromRootAlloc(allocator, .metadata, key);
    }

    /// Includes scope heads retained after every secret has been deleted.
    /// Caller holds the catalog lock; values never need to be loaded/decrypted.
    pub fn hasSecretState(self: *NativeFile) !bool {
        var cursor = try self.metadataCatalogCursor(try self.materializeTransactionCheckpoint(), secret_catalog_prefix);
        defer cursor.deinit();
        const entry = (try cursor.next()) orelse return false;
        self.allocator.free(entry.key);
        return true;
    }

    pub fn metadataCatalogCursor(self: *NativeFile, checkpoint: CheckpointSlot, prefix: []const u8) !CatalogCursor {
        return try self.catalogCursor(checkpoint, .metadata, prefix);
    }

    pub fn getIndexCatalogRecordAlloc(self: *NativeFile, allocator: Allocator, key: []const u8) !?[]u8 {
        return try self.getCatalogRecordFromRootAlloc(allocator, .index, key);
    }

    pub fn getIndexCatalogRecordAtCheckpointAlloc(self: *NativeFile, allocator: Allocator, key: []const u8, checkpoint: CheckpointSlot) !?[]u8 {
        return try self.getIndexCatalogRecordLimitedAtCheckpointAlloc(allocator, key, std.math.maxInt(usize), checkpoint);
    }

    /// Enforce the caller's limit against catalog metadata before allocating
    /// or reading any external payload, within the same pinned checkpoint.
    pub fn getIndexCatalogRecordLimitedAtCheckpointAlloc(self: *NativeFile, allocator: Allocator, key: []const u8, max_bytes: usize, checkpoint: CheckpointSlot) !?[]u8 {
        return try self.getCatalogRecordFromRootAtCheckpointAlloc(allocator, .index, key, checkpoint, max_bytes);
    }

    pub fn getIndexCatalogRecordSize(self: *NativeFile, key: []const u8) !?usize {
        return try self.getCatalogRecordSizeFromRoot(.index, key);
    }

    pub fn getIndexCatalogRecordSizeAtCheckpoint(self: *NativeFile, key: []const u8, checkpoint: CheckpointSlot) !?usize {
        return try self.getCatalogRecordSizeFromRootAtCheckpoint(.index, key, checkpoint);
    }

    pub fn getIndexCatalogRecordRangeAlloc(
        self: *NativeFile,
        allocator: Allocator,
        key: []const u8,
        offset: u64,
        len: usize,
    ) !?[]u8 {
        return try self.getCatalogRecordRangeFromRootAlloc(allocator, .index, key, offset, len);
    }

    pub fn getIndexCatalogRecordRangeAtCheckpointAlloc(
        self: *NativeFile,
        allocator: Allocator,
        key: []const u8,
        offset: u64,
        len: usize,
        checkpoint: CheckpointSlot,
    ) !?[]u8 {
        return try self.getCatalogRecordRangeFromRootAtCheckpointAlloc(allocator, .index, key, offset, len, checkpoint);
    }

    fn getCatalogRecordFromRootAlloc(
        self: *NativeFile,
        allocator: Allocator,
        root: CatalogRoot,
        key: []const u8,
    ) !?[]u8 {
        try self.finishCatalogAppend(root, key);
        if (self.stagedEntry(if (root == .metadata) .metadata else .index, key)) |entry| {
            if (entry.is_delete) return null;
            return try self.catalogEntryValueAtCheckpointAlloc(allocator, entry, self.activeCheckpoint());
        }
        return try self.getCatalogRecordFromRootAtCheckpointAlloc(allocator, root, key, self.activeCheckpoint(), std.math.maxInt(usize));
    }

    fn getCatalogRecordFromRootAtCheckpointAlloc(
        self: *NativeFile,
        allocator: Allocator,
        root: CatalogRoot,
        key: []const u8,
        checkpoint: CheckpointSlot,
        max_bytes: usize,
    ) !?[]u8 {
        const page_id = (try self.lookupCatalogPage(checkpoint, root, key)) orelse return null;
        var scratch: [65536]u8 = undefined;
        const payload = try decodePagePayload(try self.readPageInto(page_id, checkpoint, &scratch), .catalog);
        const entry = try decodeCatalogEntry(payload);
        if (!std.mem.eql(u8, entry.key, key)) return error.InvalidNativePageChain;
        if (entry.is_delete) return null;
        const value_len = if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len;
        if (value_len > max_bytes) return error.FileTooBig;
        return try self.catalogEntryValueAtCheckpointAlloc(allocator, entry, checkpoint);
    }

    fn getCatalogRecordSizeFromRoot(
        self: *NativeFile,
        root: CatalogRoot,
        key: []const u8,
    ) !?usize {
        if (self.stagingTransaction()) {
            if (self.transaction_writes) |writes| {
                if (writes.appends[@backingInt(root)].get(key)) |state| return state.len;
            }
        }
        if (self.stagedEntry(if (root == .metadata) .metadata else .index, key)) |entry| {
            if (entry.is_delete) return null;
            return if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len;
        }
        return try self.getCatalogRecordSizeFromRootAtCheckpoint(root, key, self.activeCheckpoint());
    }

    fn getCatalogRecordSizeFromRootAtCheckpoint(
        self: *NativeFile,
        root: CatalogRoot,
        key: []const u8,
        checkpoint: CheckpointSlot,
    ) !?usize {
        const page_id = (try self.lookupCatalogPage(checkpoint, root, key)) orelse return null;
        var scratch: [65536]u8 = undefined;
        const payload = try decodePagePayload(try self.readPageInto(page_id, checkpoint, &scratch), .catalog);
        const entry = try decodeCatalogEntry(payload);
        if (!std.mem.eql(u8, entry.key, key)) return error.InvalidNativePageChain;
        if (entry.is_delete) return null;
        return if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len;
    }

    fn getCatalogRecordRangeFromRootAlloc(
        self: *NativeFile,
        allocator: Allocator,
        root: CatalogRoot,
        key: []const u8,
        offset: u64,
        len: usize,
    ) !?[]u8 {
        try self.finishCatalogAppend(root, key);
        if (self.stagedEntry(if (root == .metadata) .metadata else .index, key)) |entry| {
            if (entry.is_delete) return null;
            return try self.catalogEntryRangeAlloc(allocator, entry, offset, len, self.activeCheckpoint());
        }
        return try self.getCatalogRecordRangeFromRootAtCheckpointAlloc(allocator, root, key, offset, len, self.activeCheckpoint());
    }

    fn getCatalogRecordRangeFromRootAtCheckpointAlloc(
        self: *NativeFile,
        allocator: Allocator,
        root: CatalogRoot,
        key: []const u8,
        offset: u64,
        len: usize,
        checkpoint: CheckpointSlot,
    ) !?[]u8 {
        const page_id = (try self.lookupCatalogPage(checkpoint, root, key)) orelse return null;
        var scratch: [65536]u8 = undefined;
        const payload = try decodePagePayload(try self.readPageInto(page_id, checkpoint, &scratch), .catalog);
        const entry = try decodeCatalogEntry(payload);
        if (!std.mem.eql(u8, entry.key, key)) return error.InvalidNativePageChain;
        if (entry.is_delete) return null;
        return try self.catalogEntryRangeAlloc(allocator, entry, offset, len, checkpoint);
    }

    pub const IndexValue = struct {
        root: u64,
        length: usize,
        inline_bytes: []u8 = &.{},

        pub fn deinit(self: *IndexValue, allocator: Allocator) void {
            allocator.free(self.inline_bytes);
            self.* = undefined;
        }
    };

    /// Resolve an immutable artifact once. A generation/checkpoint pin must
    /// outlive this descriptor; only small inline values require owned bytes.
    pub fn openIndexValue(self: *NativeFile, allocator: Allocator, key: []const u8, checkpoint: CheckpointSlot) !IndexValue {
        const page_id = (try self.lookupCatalogPage(checkpoint, .index, key)) orelse return error.FileNotFound;
        var scratch: [65536]u8 = undefined;
        const payload = try decodePagePayload(try self.readPageInto(page_id, checkpoint, &scratch), .catalog);
        const entry = try decodeCatalogEntry(payload);
        if (!std.mem.eql(u8, entry.key, key)) return error.InvalidNativePageChain;
        if (entry.is_delete) return error.FileNotFound;
        if (entry.external_value_root_page != 0) return .{ .root = entry.external_value_root_page, .length = entry.external_value_len };
        return .{ .root = 0, .length = entry.value.len, .inline_bytes = try allocator.dupe(u8, entry.value) };
    }

    pub fn readIndexValueInto(self: *NativeFile, value: IndexValue, offset: u64, out: []u8, checkpoint: CheckpointSlot) !void {
        if (offset > value.length or out.len > value.length - offset) return error.EndOfStream;
        if (value.root != 0) {
            return self.readValuePagesRangeInto(value.root, value.length, @intCast(offset), out, checkpoint);
        }
        @memcpy(out, value.inline_bytes[@intCast(offset)..][0..out.len]);
    }

    /// Authenticate a logical range in one forward traversal. Extent subtrees
    /// preceding the range are skipped without reading their payload pages.
    pub fn checksumIndexValue(self: *NativeFile, value: IndexValue, offset: u64, length: u64, checkpoint: CheckpointSlot) !u32 {
        if (offset > value.length or length > value.length - offset) return error.EndOfStream;
        if (length == 0) return Crc32.hash("");
        if (value.root == 0) return Crc32.hash(value.inline_bytes[@intCast(offset)..][0..@intCast(length)]);
        var cursor = try ValueChunkCursor.initAtCheckpoint(self, value.root, value.length, checkpoint);
        defer cursor.deinit();
        cursor.skip = @intCast(offset);
        cursor.read_end = @intCast(offset + length);
        var remaining: usize = @intCast(length);
        var crc = Crc32.init();
        while (remaining != 0) {
            const chunk = (try cursor.next(null)) orelse return error.InvalidNativeValueChain;
            const take = @min(remaining, chunk.len);
            crc.update(chunk[0..take]);
            remaining -= take;
        }
        return crc.final();
    }

    /// One forward traversal for cache fills, checksumming, and range delivery.
    pub fn visitIndexValue(self: *NativeFile, value: IndexValue, offset: u64, length: u64, checkpoint: CheckpointSlot, context: *anyopaque, visit: *const fn (*anyopaque, u64, []const u8) anyerror!void) !void {
        if (offset > value.length or length > value.length - offset) return error.EndOfStream;
        if (length == 0) return;
        if (value.root == 0) return visit(context, 0, value.inline_bytes[@intCast(offset)..][0..@intCast(length)]);
        var cursor = try ValueChunkCursor.initAtCheckpoint(self, value.root, value.length, checkpoint);
        defer cursor.deinit();
        cursor.skip = @intCast(offset);
        cursor.read_end = @intCast(offset + length);
        var position: u64 = 0;
        while (position < length) {
            const chunk = (try cursor.next(null)) orelse return error.InvalidNativeValueChain;
            const take: usize = @intCast(@min(chunk.len, length - position));
            try visit(context, position, chunk[0..take]);
            position += take;
        }
    }

    /// Authenticate and deliver a logical subrange in one forward traversal.
    /// Output is private scratch until the expected CRC has been checked.
    pub fn readAuthenticatedIndexValue(self: *NativeFile, value: IndexValue, offset: u64, length: u64, within: usize, out: []u8, expected: ?u32, checkpoint: CheckpointSlot) !void {
        if (offset > value.length or length > value.length - offset or within > length or out.len > length - within) return error.EndOfStream;
        if (expected == null) return self.readIndexValueInto(value, offset + within, out, checkpoint);
        if (value.root == 0) {
            const bytes = value.inline_bytes[@intCast(offset)..][0..@intCast(length)];
            if (Crc32.hash(bytes) != expected.?) return error.CrcMismatch;
            @memcpy(out, bytes[within..][0..out.len]);
            return;
        }
        var cursor = try ValueChunkCursor.initAtCheckpoint(self, value.root, value.length, checkpoint);
        defer cursor.deinit();
        cursor.skip = @intCast(offset);
        cursor.read_end = @intCast(offset + length);
        var position: u64 = 0;
        var crc = Crc32.init();
        while (position < length) {
            const chunk = (try cursor.next(null)) orelse return error.InvalidNativeValueChain;
            const take: usize = @intCast(@min(chunk.len, length - position));
            crc.update(chunk[0..take]);
            const begin = @max(position, within);
            const end = @min(position + take, @as(u64, within) + out.len);
            if (begin < end) @memcpy(out[@intCast(begin - within)..][0..@intCast(end - begin)], chunk[@intCast(begin - position)..][0..@intCast(end - begin)]);
            position += take;
        }
        if (crc.final() != expected.?) return error.CrcMismatch;
    }

    /// Export a pinned artifact with bounded scratch and one forward page
    /// traversal. The destination remains private until its caller publishes.
    pub fn copyIndexValueTo(self: *NativeFile, value: IndexValue, destination: std.Io.File, checkpoint: CheckpointSlot) !void {
        if (value.root == 0) return destination.writePositionalAll(self.runtimeIo(), value.inline_bytes, 0);
        var cursor = try ValueChunkCursor.initWithCacheIntent(self, value.root, value.length, checkpoint, false);
        defer cursor.deinit();
        var position: u64 = 0;
        while (try cursor.next(null)) |chunk| {
            try destination.writePositionalAll(self.runtimeIo(), chunk, position);
            position += chunk.len;
        }
        if (position != value.length) return error.InvalidNativeValueChain;
    }

    pub fn snapshotCatalogRecordsAlloc(self: *NativeFile, allocator: Allocator) ![]OwnedCatalogRecord {
        return try self.snapshotCatalogRecordsFromRootAlloc(allocator, .metadata);
    }

    pub fn snapshotIndexCatalogRecordsAlloc(self: *NativeFile, allocator: Allocator) ![]OwnedCatalogRecord {
        return try self.snapshotCatalogRecordsFromRootAlloc(allocator, .index);
    }

    pub fn snapshotIndexCatalogKeysAlloc(self: *NativeFile, allocator: Allocator) ![]OwnedCatalogKey {
        return try self.snapshotCatalogKeysFromRootAlloc(allocator, .index);
    }

    pub fn indexCatalogCursor(self: *NativeFile, checkpoint: CheckpointSlot, prefix: []const u8) !CatalogCursor {
        return try self.catalogCursor(checkpoint, .index, prefix);
    }

    /// Requires canonical file keys with no empty path components. The
    /// borrowed prefix must end at a directory separator. Callers accepting
    /// repeated/trailing separators must use indexCatalogCursor and dirname
    /// filtering instead: such byte ranges can contain immediate files.
    /// Nested subtrees are skipped within the same checkpoint.
    pub fn indexCatalogDirectoryCursor(self: *NativeFile, checkpoint: CheckpointSlot, prefix: []const u8) !CatalogCursor {
        if (prefix.len == 0 or prefix[prefix.len - 1] != '/') return error.InvalidNativeIndexPath;
        var cursor = try self.catalogCursor(checkpoint, .index, prefix);
        cursor.immediate_children_only = true;
        return cursor;
    }

    fn catalogCursor(self: *NativeFile, checkpoint: CheckpointSlot, root: CatalogRoot, prefix: []const u8) !CatalogCursor {
        const roots = try self.readCatalogRoots(catalogRootPage(checkpoint, root), checkpoint);
        var indexed = checkpoint;
        indexed.document_index_root_page = roots.index;
        return .{ .index = DocumentIndexCursor.init(self, indexed), .prefix = prefix };
    }

    fn snapshotCatalogRecordsFromRootAlloc(self: *NativeFile, allocator: Allocator, root: CatalogRoot) ![]OwnedCatalogRecord {
        const checkpoint = try self.materializeTransactionCheckpoint();
        var cursor = try self.catalogCursor(checkpoint, root, "");
        defer cursor.deinit();
        var records = std.ArrayListUnmanaged(OwnedCatalogRecord).empty;
        errdefer {
            for (records.items) |record| {
                allocator.free(record.key);
                allocator.free(record.value);
            }
            records.deinit(allocator);
        }
        while (try cursor.nextRecordAlloc(allocator)) |record| {
            errdefer allocator.free(record.key);
            errdefer allocator.free(record.value);
            try records.append(allocator, record);
        }

        return try records.toOwnedSlice(allocator);
    }

    fn snapshotCatalogKeysFromRootAlloc(self: *NativeFile, allocator: Allocator, root: CatalogRoot) ![]OwnedCatalogKey {
        var cursor = try self.catalogCursor(try self.materializeTransactionCheckpoint(), root, "");
        defer cursor.deinit();
        var keys = std.ArrayListUnmanaged(OwnedCatalogKey).empty;
        errdefer {
            for (keys.items) |record| allocator.free(record.key);
            keys.deinit(allocator);
        }
        while (try cursor.next()) |record| {
            defer self.allocator.free(record.key);
            const key = try allocator.dupe(u8, record.key);
            errdefer allocator.free(key);
            try keys.append(allocator, .{ .key = key });
        }
        return try keys.toOwnedSlice(allocator);
    }

    pub fn freeSnapshotCatalogRecords(allocator: Allocator, records: []OwnedCatalogRecord) void {
        for (records) |record| {
            allocator.free(record.key);
            allocator.free(record.value);
        }
        allocator.free(records);
    }

    pub fn freeSnapshotCatalogKeys(allocator: Allocator, records: []OwnedCatalogKey) void {
        for (records) |record| allocator.free(record.key);
        allocator.free(records);
    }

    fn snapshotCatalogRefsFromRootAlloc(self: *NativeFile, allocator: Allocator, root: CatalogRoot) ![]OwnedLiveRecordRef {
        var seen = std.StringHashMapUnmanaged(void).empty;
        defer seen.deinit(allocator);
        var tombstones = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (tombstones.items) |key| allocator.free(key);
            tombstones.deinit(allocator);
        }
        var refs = std.ArrayListUnmanaged(OwnedLiveRecordRef).empty;
        errdefer {
            for (refs.items) |record| allocator.free(record.key);
            refs.deinit(allocator);
        }

        var page_id = try self.catalogHistoryRoot(self.activeCheckpoint(), root);
        while (page_id != 0) {
            const payload = try self.readPagePayloadByKindAlloc(allocator, page_id, .catalog);
            defer allocator.free(payload);
            const entry = try decodeCatalogEntry(payload);
            if (!seen.contains(entry.key)) {
                try seen.ensureUnusedCapacity(allocator, 1);
                const key = try allocator.dupe(u8, entry.key);
                errdefer allocator.free(key);
                if (entry.is_delete) {
                    try tombstones.append(allocator, key);
                } else {
                    try refs.append(allocator, .{ .key = key, .page_id = page_id });
                }
                seen.putAssumeCapacity(key, {});
            }
            page_id = entry.previous_page;
        }
        std.mem.sort(OwnedLiveRecordRef, refs.items, {}, liveRecordRefLessThan);
        return try refs.toOwnedSlice(allocator);
    }

    fn snapshotDocumentRefsAlloc(self: *NativeFile, allocator: Allocator) ![]OwnedLiveRecordRef {
        var seen = std.StringHashMapUnmanaged(void).empty;
        defer seen.deinit(allocator);
        var tombstones = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (tombstones.items) |key| allocator.free(key);
            tombstones.deinit(allocator);
        }
        var refs = std.ArrayListUnmanaged(OwnedLiveRecordRef).empty;
        errdefer {
            for (refs.items) |record| allocator.free(record.key);
            refs.deinit(allocator);
        }

        var page_id = self.activeCheckpoint().document_root_page;
        while (page_id != 0) {
            const payload = try self.readPagePayloadByKindAlloc(allocator, page_id, .document);
            defer allocator.free(payload);
            const entry = try decodeDocumentEntry(payload);
            if (!seen.contains(entry.key)) {
                try seen.ensureUnusedCapacity(allocator, 1);
                const key = try allocator.dupe(u8, entry.key);
                errdefer allocator.free(key);
                if (entry.is_delete) {
                    try tombstones.append(allocator, key);
                } else {
                    try refs.append(allocator, .{ .key = key, .page_id = page_id });
                }
                seen.putAssumeCapacity(key, {});
            }
            page_id = entry.previous_page;
        }
        std.mem.sort(OwnedLiveRecordRef, refs.items, {}, liveRecordRefLessThan);
        return try refs.toOwnedSlice(allocator);
    }

    fn freeLiveRecordRefs(allocator: Allocator, refs: []OwnedLiveRecordRef) void {
        for (refs) |record| allocator.free(record.key);
        allocator.free(refs);
    }

    const LiveRecordSource = union(enum) {
        catalog: CatalogRoot,
        documents,
    };

    /// Borrow record metadata from the checkpoint's ordered index. Values stay
    /// on disk; returned slices remain valid until the next call to next().
    const LiveRecordCursor = struct {
        index: DocumentIndexCursor,
        kind: PageKind,
        started: bool = false,
        records: RecordPageReader = .{},

        fn init(file: *NativeFile, source: LiveRecordSource) !LiveRecordCursor {
            var checkpoint = try file.materializeTransactionCheckpoint();
            const kind: PageKind = switch (source) {
                .catalog => |root| blk: {
                    const roots = try file.readCatalogRoots(catalogRootPage(checkpoint, root), checkpoint);
                    checkpoint.document_index_root_page = roots.index;
                    break :blk .catalog;
                },
                .documents => .document,
            };
            return .{ .index = DocumentIndexCursor.init(file, checkpoint), .kind = kind };
        }

        pub fn deinit(self: *LiveRecordCursor) void {
            self.records.deinit(self.index.file.allocator);
            self.index.deinit();
        }

        fn next(self: *LiveRecordCursor, cancel: ?*const maintenance.CancelToken) !?CatalogEntry {
            const file = self.index.file;
            while (true) {
                if (cancel) |token| try token.check();
                var indexed = (if (self.started) try self.index.next() else try self.index.first()) orelse return null;
                self.started = true;
                defer indexed.deinit(file.allocator);
                const payload = try self.records.read(file, file.allocator, self.index.checkpoint, indexed.document_page_id, self.kind);
                const entry = if (self.kind == .catalog) try decodeCatalogEntry(payload) else blk: {
                    const document = try decodeDocumentEntry(payload);
                    break :blk CatalogEntry{
                        .previous_page = document.previous_page,
                        .key = document.key,
                        .value = document.value,
                        .is_delete = document.is_delete,
                        .external_value_root_page = document.external_value_root_page,
                        .external_value_len = document.external_value_len,
                    };
                };
                if (!std.mem.eql(u8, entry.key, indexed.key)) return error.InvalidDocumentIndex;
                if (!entry.is_delete) return entry;
            }
        }
    };

    const NamespaceDirectory = std.StringHashMapUnmanaged(u64);
    const NamespaceDirectoryRecordKind = enum(u8) {
        snapshot = 0,
        delta = 1,
    };

    const LoadedNamespaceDirectory = struct {
        entries: NamespaceDirectory,
        delta_depth: u16,
    };

    fn documentNamespace(key: []const u8) []const u8 {
        const end = (std.mem.indexOfScalar(u8, key, 0) orelse return "") + 1;
        return key[0..end];
    }

    fn deinitNamespaceDirectory(allocator: Allocator, directory: *NamespaceDirectory) void {
        var it = directory.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        directory.deinit(allocator);
    }

    fn applyNamespaceDirectoryRecord(
        allocator: Allocator,
        directory: *NamespaceDirectory,
        raw: []const u8,
    ) !NamespaceDirectoryRecordKind {
        if (raw.len < namespace_directory_magic.len + 1 + 4 or
            !std.mem.eql(u8, raw[0..namespace_directory_magic.len], namespace_directory_magic))
            return error.InvalidNamespaceDirectory;
        var offset: usize = namespace_directory_magic.len;
        const kind: NamespaceDirectoryRecordKind = switch (raw[offset]) {
            0 => .snapshot,
            1 => .delta,
            else => return error.InvalidNamespaceDirectory,
        };
        offset += 1;
        const count = std.mem.readInt(u32, raw[offset..][0..4], .little);
        offset += 4;
        if (@as(usize, count) > (raw.len - offset) / 12) return error.InvalidNamespaceDirectory;
        try directory.ensureUnusedCapacity(allocator, count);
        var record_keys = std.StringHashMapUnmanaged(void).empty;
        defer record_keys.deinit(allocator);
        try record_keys.ensureTotalCapacity(allocator, count);
        var i: u32 = 0;
        while (i < count) : (i += 1) {
            if (raw.len - offset < 4) return error.InvalidNamespaceDirectory;
            const len = std.mem.readInt(u32, raw[offset..][0..4], .little);
            offset += 4;
            if (len > raw.len - offset or raw.len - offset - len < 8) return error.InvalidNamespaceDirectory;
            const raw_key = raw[offset..][0..len];
            offset += len;
            const head = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            if ((raw_key.len > 0 and raw_key[raw_key.len - 1] != 0) or head == 0 or record_keys.contains(raw_key))
                return error.InvalidNamespaceDirectory;
            record_keys.putAssumeCapacity(raw_key, {});

            // Records are replayed newest to oldest. The first head for a
            // namespace is authoritative; older snapshots/deltas fill only
            // namespaces not mentioned by a newer record.
            if (!directory.contains(raw_key)) {
                const key = try allocator.dupe(u8, raw_key);
                directory.putAssumeCapacity(key, head);
            }
        }
        if (offset != raw.len) return error.InvalidNamespaceDirectory;
        return kind;
    }

    fn encodeNamespaceDirectoryAlloc(
        allocator: Allocator,
        kind: NamespaceDirectoryRecordKind,
        directory: *const NamespaceDirectory,
    ) ![]u8 {
        var out: std.Io.Writer.Allocating = .init(allocator);
        errdefer out.deinit();
        try out.writer.writeAll(namespace_directory_magic);
        try out.writer.writeByte(@backingInt(kind));
        try out.writer.writeInt(u32, std.math.cast(u32, directory.count()) orelse return error.RecordTooLarge, .little);
        var it = directory.iterator();
        while (it.next()) |entry| {
            try out.writer.writeInt(u32, @intCast(entry.key_ptr.*.len), .little);
            try out.writer.writeAll(entry.key_ptr.*);
            try out.writer.writeInt(u64, entry.value_ptr.*, .little);
        }
        return try out.toOwnedSlice();
    }

    fn loadNamespaceDirectoryWithDepthAlloc(self: *NativeFile, allocator: Allocator) !?LoadedNamespaceDirectory {
        return try self.loadNamespaceDirectoryWithDepthAtCheckpointAlloc(allocator, self.activeCheckpoint());
    }

    fn loadNamespaceDirectoryWithDepthAtCheckpointAlloc(self: *NativeFile, allocator: Allocator, checkpoint: CheckpointSlot) !?LoadedNamespaceDirectory {
        var root = checkpoint.namespace_directory_root_page;
        if (root == 0) return null;
        const roots = try self.readCatalogRoots(root, checkpoint);
        if (roots.indexed) {
            var directory = NamespaceDirectory.empty;
            errdefer deinitNamespaceDirectory(allocator, &directory);
            var indexed = checkpoint;
            indexed.document_index_root_page = roots.index;
            var cursor = DocumentIndexCursor.init(self, indexed);
            defer cursor.deinit();
            var reader = RecordPageReader{};
            defer reader.deinit(allocator);
            var current = try cursor.first();
            while (current) |entry| {
                var owned = entry;
                defer owned.deinit(self.allocator);
                const record = try decodeCatalogEntry(try reader.read(self, allocator, checkpoint, entry.document_page_id, .catalog));
                if (!std.mem.eql(u8, record.key, entry.key)) return error.InvalidNamespaceDirectory;
                const head = try decodeNamespaceHead(record, checkpoint);
                const key = try allocator.dupe(u8, entry.key);
                directory.put(allocator, key, head) catch |err| {
                    allocator.free(key);
                    return err;
                };
                current = try cursor.next();
            }
            return .{ .entries = directory, .delta_depth = 0 };
        }
        var directory = NamespaceDirectory.empty;
        errdefer deinitNamespaceDirectory(allocator, &directory);
        var depth: u16 = 0;
        var walked: u64 = 0;
        while (root != 0) {
            const payload = try self.readPagePayloadByKindAllocForCheckpoint(allocator, root, .catalog, checkpoint);
            defer allocator.free(payload);
            const entry = try decodeCatalogEntry(payload);
            if (!std.mem.eql(u8, entry.key, namespace_directory_key) or entry.is_delete)
                return error.InvalidNamespaceDirectory;
            const raw = try self.catalogEntryValueAtCheckpointAlloc(allocator, entry, checkpoint);
            defer allocator.free(raw);
            const kind = try applyNamespaceDirectoryRecord(allocator, &directory, raw);
            walked += 1;
            if (walked > recordWalkLimit(checkpoint, self.header.page_size)) return error.InvalidNativePageChain;
            switch (kind) {
                .snapshot => {
                    if (entry.previous_page != 0) return error.InvalidNamespaceDirectory;
                    return .{ .entries = directory, .delta_depth = depth };
                },
                .delta => {
                    depth = std.math.add(u16, depth, 1) catch return error.InvalidNamespaceDirectory;
                    root = entry.previous_page;
                    if (root == 0) return error.InvalidNamespaceDirectory;
                },
            }
        }
        return error.InvalidNamespaceDirectory;
    }

    fn loadNamespaceDirectoryAlloc(self: *NativeFile, allocator: Allocator) !?NamespaceDirectory {
        const loaded = (try self.loadNamespaceDirectoryWithDepthAlloc(allocator)) orelse return null;
        return loaded.entries;
    }

    fn loadNamespaceDirectoryAtCheckpointAlloc(self: *NativeFile, allocator: Allocator, checkpoint: CheckpointSlot) !?NamespaceDirectory {
        const loaded = (try self.loadNamespaceDirectoryWithDepthAtCheckpointAlloc(allocator, checkpoint)) orelse return null;
        return loaded.entries;
    }

    fn namespaceHeadAtCheckpoint(self: *NativeFile, checkpoint: CheckpointSlot, index: u64, namespace: []const u8) !u64 {
        var indexed = checkpoint;
        indexed.document_index_root_page = index;
        const page = (try self.lookupDocumentIndexPage(indexed, namespace)) orelse return 0;
        var scratch: [65536]u8 = undefined;
        const entry = try decodeCatalogEntry(try decodePagePayload(try self.readPageInto(page, checkpoint, &scratch), .catalog));
        if (!std.mem.eql(u8, entry.key, namespace)) return error.InvalidNamespaceDirectory;
        return decodeNamespaceHead(entry, checkpoint);
    }

    // One borrowed key per touched namespace. Collect before writing so map
    // storage and lookup scratch scale with namespaces, not document count,
    // and repeated mutations can update stable head pointers in input order.
    fn prepareNamespaceHeads(self: *NativeFile, checkpoint: CheckpointSlot, roots: CatalogRoots, mutations: []const DocumentMutation) !NamespaceDirectory {
        var heads = NamespaceDirectory.empty;
        errdefer heads.deinit(self.allocator);
        var last: ?[]const u8 = null;
        for (mutations) |mutation| {
            const namespace = documentNamespace(mutation.key);
            if (last) |previous| if (std.mem.eql(u8, previous, namespace)) continue;
            const entry = try heads.getOrPut(self.allocator, namespace);
            if (!entry.found_existing) entry.value_ptr.* = 0;
            last = namespace;
        }
        if (roots.indexed) {
            try self.loadNamespaceHeadsAtCheckpoint(checkpoint, roots.index, &heads);
        } else {
            var it = heads.iterator();
            while (it.next()) |entry| entry.value_ptr.* = self.namespace_directory_cache.get(entry.key_ptr.*) orelse 0;
        }
        return heads;
    }

    fn sortedNamespaceKeysAlloc(allocator: Allocator, heads: *const NamespaceDirectory) ![][]const u8 {
        const keys = try allocator.alloc([]const u8, heads.count());
        var it = heads.keyIterator();
        for (keys) |*key| key.* = it.next().?.*;
        std.mem.sort([]const u8, keys, {}, struct {
            fn less(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.lessThan(u8, a, b);
            }
        }.less);
        return keys;
    }

    fn loadNamespaceHeadsAtCheckpoint(self: *NativeFile, checkpoint: CheckpointSlot, index: u64, heads: *NamespaceDirectory) !void {
        if (heads.count() == 0 or index == 0) return;
        if (heads.count() == 1) {
            var it = heads.iterator();
            const entry = it.next().?;
            entry.value_ptr.* = try self.namespaceHeadAtCheckpoint(checkpoint, index, entry.key_ptr.*);
            return;
        }
        const keys = try sortedNamespaceKeysAlloc(self.allocator, heads);
        defer self.allocator.free(keys);
        const references = try self.allocator.alloc(u64, keys.len);
        defer self.allocator.free(references);
        @memset(references, 0);
        var indexed = checkpoint;
        indexed.document_index_root_page = index;
        // Share the sorted multi-read traversal with documents: each relevant
        // tree node is decoded once, even when many keys share its path.
        try self.readDocumentBatchNode(indexed, index, keys, references);
        // Key order and physical order diverge after independent updates.
        // Reorder both borrowed keys and references together so every packed
        // record page is fetched/decoded once, without a third ordering array.
        const PhysicalOrder = struct {
            keys: [][]const u8,
            references: []u64,
            pub fn lessThan(ctx: @This(), a: usize, b: usize) bool {
                return recordReferenceLessThan(ctx.references[a], ctx.references[b]);
            }
            pub fn swap(ctx: @This(), a: usize, b: usize) void {
                std.mem.swap([]const u8, &ctx.keys[a], &ctx.keys[b]);
                std.mem.swap(u64, &ctx.references[a], &ctx.references[b]);
            }
        };
        std.sort.pdqContext(0, keys.len, PhysicalOrder{ .keys = keys, .references = references });
        var reader = RecordPageReader{};
        defer reader.deinit(self.allocator);
        for (keys, references) |key, reference| {
            if (reference == 0) continue;
            const entry = try decodeCatalogEntry(try reader.read(self, self.allocator, checkpoint, reference, .catalog));
            if (!std.mem.eql(u8, entry.key, key)) return error.InvalidNamespaceDirectory;
            heads.getPtr(key).?.* = try decodeNamespaceHead(entry, checkpoint);
        }
    }

    fn decodeNamespaceHead(entry: CatalogEntry, checkpoint: CheckpointSlot) !u64 {
        if (entry.is_delete or entry.external_value_root_page != 0 or entry.value.len != 8 or
            (entry.key.len > 0 and entry.key[entry.key.len - 1] != 0)) return error.InvalidNamespaceDirectory;
        const head = std.mem.readInt(u64, entry.value[0..8], .little);
        if (head == 0 or physicalPage(head) >= checkpoint.page_count) return error.InvalidNamespaceDirectory;
        return head;
    }

    // Uses existing revision-3 catalog descriptors, records and B+ tree pages.
    // A legacy directory is converted once; indexed updates retain only the
    // touched paths. Bulk construction also serves vacuum and initial ingest.
    const WrittenNamespaceDirectory = struct { page: u64, indexed: bool };

    fn writeNamespaceIndex(self: *NativeFile, pages: *PageAllocator, checkpoint: CheckpointSlot, roots: CatalogRoots, heads: *const NamespaceDirectory) !WrittenNamespaceDirectory {
        // Keep tiny directories in the existing inline snapshot encoding.
        // This bounds their cost while avoiding an extra tree/descriptor on
        // the common single-namespace path. Promotion is one-way until vacuum.
        if (!self.header.indexed_reclamation and !roots.indexed and heads.count() <= 32) {
            const encoded = try encodeNamespaceDirectoryAlloc(self.allocator, .snapshot, heads);
            defer self.allocator.free(encoded);
            if (self.catalogEntryFitsInline(namespace_directory_key, encoded)) {
                var payload = std.ArrayListUnmanaged(u8).empty;
                defer payload.deinit(self.allocator);
                try encodeCatalogEntry(self.allocator, &payload, .{ .previous_page = 0, .key = namespace_directory_key, .value = encoded });
                const page = try pages.allocate();
                try pages.writePage(page, .catalog, payload.items);
                return .{ .page = page, .indexed = false };
            }
        }
        const keys = try sortedNamespaceKeysAlloc(self.allocator, heads);
        defer self.allocator.free(keys);
        const bulk = roots.index == 0;
        var builder = DocumentIndexBulkBuilder{ .owner = self, .file = self.file, .next_page_id = &pages.next_page_id, .pages = pages };
        defer builder.deinit();
        var editor = IndexEditor.init(self, checkpoint, roots.index);
        editor.accounting_pages = pages;
        defer editor.deinit();
        if (keys.len > 1) editor.streaming_pages = pages;
        var history = if (roots.indexed) roots.history else 0;
        for (keys) |key| {
            var value: [8]u8 = undefined;
            std.mem.writeInt(u64, &value, heads.get(key).?, .little);
            history = try pages.writeCatalog(.{ .previous_page = if (self.header.indexed_reclamation) 0 else history, .key = key, .value = &value });
            if (bulk) try builder.add(key, history) else try editor.put(key, history);
        }
        const index = if (bulk) try builder.finish() else try editor.finish(pages);
        return .{ .page = try self.writeCatalogRoot(pages, history, index, checkpoint.namespace_directory_root_page), .indexed = true };
    }

    fn ensureNamespaceDirectoryCache(self: *NativeFile) !void {
        const root = self.activeCheckpoint().namespace_directory_root_page;
        if (self.namespace_directory_cache_root == root) return;
        const loaded = (try self.loadNamespaceDirectoryWithDepthAlloc(self.allocator)) orelse LoadedNamespaceDirectory{
            .entries = NamespaceDirectory.empty,
            .delta_depth = 0,
        };
        deinitNamespaceDirectory(self.allocator, &self.namespace_directory_cache);
        self.namespace_directory_cache = loaded.entries;
        self.namespace_directory_delta_depth = loaded.delta_depth;
        self.namespace_directory_cache_root = root;
    }

    fn validateNamespaceDirectory(self: *NativeFile, checkpoint: CheckpointSlot) !void {
        var records = RecordPageReader{};
        defer records.deinit(self.allocator);
        var directory = (try self.loadNamespaceDirectoryAlloc(self.allocator)) orelse return;
        defer deinitNamespaceDirectory(self.allocator, &directory);

        // Verify the complete index in one global-history pass. Each map value
        // is the document page that the next occurrence of that namespace must
        // have. This proves directory heads are current, every document is
        // indexed exactly once, and every namespace link targets the next older
        // document without adding a second O(history) namespace traversal.
        var expected_pages = std.StringHashMapUnmanaged(u64).empty;
        defer expected_pages.deinit(self.allocator);
        try expected_pages.ensureTotalCapacity(self.allocator, directory.count());
        var directory_it = directory.iterator();
        while (directory_it.next()) |entry| {
            expected_pages.putAssumeCapacity(entry.key_ptr.*, entry.value_ptr.*);
        }

        var page_id = checkpoint.document_root_page;
        var walked: u64 = 0;
        while (page_id != 0) {
            if (physicalPage(page_id) >= checkpoint.page_count) return error.InvalidPageId;
            const payload = try records.read(self, self.allocator, checkpoint, page_id, .document);
            const entry = try decodeDocumentEntry(payload);
            const expected = expected_pages.getPtr(documentNamespace(entry.key)) orelse return error.InvalidNamespaceDirectory;
            if (expected.* != page_id) return error.InvalidNamespaceDirectory;
            expected.* = entry.previous_namespace_page;
            page_id = entry.previous_page;
            walked += 1;
            if (walked > recordWalkLimit(checkpoint, self.header.page_size)) return error.InvalidNativePageChain;
        }
        var expected_it = expected_pages.valueIterator();
        while (expected_it.next()) |expected| {
            if (expected.* != 0) return error.InvalidNamespaceDirectory;
        }
    }

    pub fn putDocument(self: *NativeFile, key: []const u8, value: []const u8) !void {
        try self.putDocumentBatch(&.{.{ .key = key, .value = value }});
    }

    pub fn deleteDocument(self: *NativeFile, key: []const u8) !void {
        try self.putDocumentBatch(&.{.{ .key = key, .is_delete = true }});
    }

    pub fn putDocumentBatch(self: *NativeFile, mutations: []const DocumentMutation) anyerror!void {
        if (self.read_only) return error.ReadOnly;
        if (mutations.len == 0) return;
        for (mutations) |mutation| try self.validateDocumentMutation(mutation);
        if (self.change_capture) |capture| for (mutations) |mutation| capture.record(self.allocator, .documents, mutation.key);

        if (self.stagingTransaction() and mutations.len >= TransactionWrites.max_keys) {
            // The caller already owns a complete batch until this call returns.
            // Consume it directly instead of copying it into bounded staging
            // and rebuilding the same index for every staging-sized chunk.
            try self.flushTransactionWrites();
            const writes = try self.transactionWrites();
            self.flushing_transaction = true;
            self.transaction_pages = &writes.pages;
            defer {
                self.transaction_pages = null;
                self.flushing_transaction = false;
            }
            try self.putDocumentBatch(mutations);
            try writes.pages.flush();
            return;
        }
        if (self.stagingTransaction()) {
            for (mutations) |m| try self.stageMutation(.documents, .{ .key = m.key, .value = m.value, .is_delete = m.is_delete, .external_value_root_page = m.external_value_root_page, .external_value_len = m.external_value_len }, .{});
            return;
        }

        if (self.header.indexed_reclamation) return self.putIndexedDocumentBatch(mutations);
        const previous = self.activeCheckpoint();
        const namespace_roots = try self.readCatalogRoots(previous.namespace_directory_root_page, previous);
        if (!namespace_roots.indexed) try self.ensureNamespaceDirectoryCache();
        if (previous.document_root_page != 0 and
            (if (namespace_roots.indexed) namespace_roots.index == 0 else self.namespace_directory_cache.count() == 0))
            return error.InvalidNamespaceDirectory;
        var changed_heads = try self.prepareNamespaceHeads(previous, namespace_roots, mutations);
        defer changed_heads.deinit(self.allocator);
        var next_root_page = previous.document_root_page;
        var next_index_root_page = previous.document_index_root_page;
        const bulk_build_initial_index = next_index_root_page == 0;
        const stream_initial_index = bulk_build_initial_index and mutationKeysSorted(mutations);
        var initial_index_entries = std.ArrayListUnmanaged(PendingDocumentIndexEntry).empty;
        defer initial_index_entries.deinit(self.allocator);
        if (bulk_build_initial_index and !stream_initial_index) try initial_index_entries.ensureTotalCapacity(self.allocator, mutations.len);
        var local_pages: PageAllocator = undefined;
        if (self.transaction_pages == null) local_pages = try self.pageAllocatorFromFreeMap(previous);
        defer if (self.transaction_pages == null) local_pages.deinit();
        const page_allocator = self.transaction_pages orelse &local_pages;
        page_allocator.pack_records = self.header.packed_records and (mutations.len > 1 or self.transaction_pages != null);
        var editor = IndexEditor.init(self, previous, next_index_root_page);
        defer editor.deinit();
        if (!bulk_build_initial_index) editor.useSortedBatch(page_allocator, mutations);
        var builder = DocumentIndexBulkBuilder{
            .owner = self,
            .file = self.file,
            .next_page_id = &page_allocator.next_page_id,
            .pages = page_allocator,
        };
        defer builder.deinit();

        for (mutations, 0..) |mutation, ordinal| {
            var external_value_root_page: u64 = mutation.external_value_root_page;
            if (external_value_root_page == 0 and !mutation.is_delete and !self.documentEntryFitsInline(mutation.key, mutation.value)) {
                external_value_root_page = try self.writeValuePagesAllocated(page_allocator, mutation.value);
            }

            const namespace = documentNamespace(mutation.key);
            const namespace_head = changed_heads.getPtr(namespace).?;
            const page_id = try page_allocator.writeDocument(.{
                .previous_page = if (self.header.indexed_reclamation) 0 else next_root_page,
                .previous_namespace_page = if (self.header.indexed_reclamation) 0 else namespace_head.*,
                .key = mutation.key,
                .value = mutation.value,
                .is_delete = mutation.is_delete,
                .external_value_root_page = external_value_root_page,
                .external_value_len = mutation.external_value_len,
            });
            next_root_page = page_id;
            if (stream_initial_index) {
                // Preserve every history record, but index only the last
                // mutation in each equal-key group, including tombstones.
                const last = ordinal + 1 == mutations.len or !std.mem.eql(u8, mutation.key, mutations[ordinal + 1].key);
                if (last and !mutation.is_delete) try builder.add(mutation.key, page_id) else try page_allocator.retire(page_id, .record);
            } else if (bulk_build_initial_index) {
                initial_index_entries.appendAssumeCapacity(.{
                    .key = mutation.key,
                    .document_page_id = page_id,
                    .ordinal = ordinal,
                });
            } else if (mutation.is_delete) {
                // History retains the tombstone; immutable older index roots
                // retain their values. The current index contains only live keys.
                try page_allocator.retire(page_id, .record);
                try editor.remove(mutation.key);
            } else {
                try editor.put(mutation.key, page_id);
            }
            namespace_head.* = page_id;
        }

        if (bulk_build_initial_index) {
            // Sorted input has already streamed through the builder. Unsorted
            // input keeps the ordinal-aware sort and latest-write-wins fallback.
            std.mem.sort(PendingDocumentIndexEntry, initial_index_entries.items, {}, PendingDocumentIndexEntry.lessThan);
            var index: usize = 0;
            while (index < initial_index_entries.items.len) {
                var end = index + 1;
                while (end < initial_index_entries.items.len and
                    std.mem.eql(u8, initial_index_entries.items[index].key, initial_index_entries.items[end].key)) : (end += 1)
                {}
                const latest = initial_index_entries.items[end - 1];
                if (!mutations[latest.ordinal].is_delete)
                    try builder.add(latest.key, latest.document_page_id);
                for (initial_index_entries.items[index..end]) |candidate| {
                    if (candidate.ordinal != latest.ordinal or mutations[latest.ordinal].is_delete) try page_allocator.retire(candidate.document_page_id, .record);
                }
                index = end;
            }
            next_index_root_page = try builder.finish();
        } else {
            next_index_root_page = try editor.finish(page_allocator);
        }

        // Migration materializes the legacy map only once. New and already
        // indexed directories write only changed heads, without a global cache.
        var migrated = NamespaceDirectory.empty;
        defer migrated.deinit(self.allocator);
        const heads = if (!namespace_roots.indexed and self.namespace_directory_cache.count() != 0) blk: {
            try migrated.ensureTotalCapacity(self.allocator, self.namespace_directory_cache.count() + changed_heads.count());
            var old = self.namespace_directory_cache.iterator();
            while (old.next()) |entry| migrated.putAssumeCapacity(entry.key_ptr.*, entry.value_ptr.*);
            var changed = changed_heads.iterator();
            while (changed.next()) |entry| migrated.putAssumeCapacity(entry.key_ptr.*, entry.value_ptr.*);
            break :blk &migrated;
        } else &changed_heads;
        const directory = try self.writeNamespaceIndex(page_allocator, previous, namespace_roots, heads);

        // The inline cache is bounded by one page and 32 namespaces. Reserve
        // ownership before publication; updating its heads afterward cannot
        // fail. Ordinary overwrites acquire no new keys or cache allocations.
        var new_entries: [32]struct { key: []u8, head: u64 } = undefined;
        var new_count: usize = 0;
        defer for (new_entries[0..new_count]) |entry| self.allocator.free(entry.key);
        if (!directory.indexed) {
            try self.namespace_directory_cache.ensureTotalCapacity(self.allocator, heads.count());
            var it = heads.iterator();
            while (it.next()) |entry| {
                if (!self.namespace_directory_cache.contains(entry.key_ptr.*)) {
                    new_entries[new_count] = .{ .key = try self.allocator.dupe(u8, entry.key_ptr.*), .head = entry.value_ptr.* };
                    new_count += 1;
                }
            }
        }

        var next = previous;
        next.commit_sequence += 1;
        next.document_root_page = if (self.header.indexed_reclamation) 0 else next_root_page;
        next.namespace_directory_root_page = directory.page;
        next.document_index_root_page = next_index_root_page;
        if (self.transaction_pages == null) next.free_map_root_page = try page_allocator.allocate();
        next.page_count = page_allocator.next_page_id;

        if (self.transaction_pages == null) {
            try page_allocator.flush();
            try self.persistAllocator(page_allocator, &next);
            try self.syncIfRequired();
        }

        try self.publishCheckpoint(next);
        if (directory.indexed) {
            deinitNamespaceDirectory(self.allocator, &self.namespace_directory_cache);
            self.namespace_directory_cache = .empty;
            self.namespace_directory_cache_root = std.math.maxInt(u64);
        } else {
            var it = changed_heads.iterator();
            while (it.next()) |entry| {
                if (self.namespace_directory_cache.getPtr(entry.key_ptr.*)) |head| head.* = entry.value_ptr.*;
            }
            for (new_entries[0..new_count]) |entry| self.namespace_directory_cache.putAssumeCapacity(entry.key, entry.head);
            new_count = 0;
            self.namespace_directory_cache_root = directory.page;
        }
        self.namespace_directory_delta_depth = 0;
    }

    fn readDocumentIndexNode(self: *NativeFile, page_id: u64, checkpoint: CheckpointSlot) !DocumentIndexNode {
        const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, page_id, .document_index, checkpoint);
        defer self.allocator.free(payload);
        return try self.decodeResolvedDocumentIndexNode(payload, checkpoint);
    }

    fn decodeResolvedDocumentIndexNode(self: *NativeFile, payload: []const u8, checkpoint: CheckpointSlot) !DocumentIndexNode {
        var node = try decodeDocumentIndexNode(self.allocator, payload);
        errdefer node.deinit(self.allocator);
        for (0..node.keys.len) |i| {
            const key = try self.resolveIndexKey(&node, i, checkpoint);
            if (i > 0 and std.mem.order(u8, node.keys[i - 1], key) != .lt) return error.InvalidDocumentIndex;
        }
        return node;
    }

    fn resolveIndexKey(self: *NativeFile, node: *DocumentIndexNode, index: usize, checkpoint: CheckpointSlot) ![]const u8 {
        const page = node.key_pages.?[index];
        if (page == 0 or node.keys[index].len != 0) return node.keys[index];
        const raw = try self.readPageAllocForCheckpoint(self.allocator, page, checkpoint);
        defer self.allocator.free(raw);
        const bytes = switch (raw[4]) {
            @backingInt(PageKind.catalog) => (try decodeCatalogEntry(try decodePagePayload(raw, .catalog))).key,
            @backingInt(PageKind.document) => (try decodeDocumentEntry(try decodePagePayload(raw, .document))).key,
            @backingInt(PageKind.key) => try NativeFile.decodeKey(try decodePagePayload(raw, .key)),
            else => return error.InvalidDocumentIndex,
        };
        if (bytes.len <= index_inline_key_limit) return error.InvalidDocumentIndex;
        const owned = try self.allocator.dupe(u8, bytes);
        self.allocator.free(node.keys[index]);
        node.keys[index] = owned;
        return owned;
    }

    fn writeDocumentIndexNode(self: *NativeFile, page_allocator: *PageAllocator, node: DocumentIndexNode) !u64 {
        if (self.header.indexed_reclamation) try self.materializeIndexKeys(page_allocator, node);
        const encoded = try encodeDocumentIndexNode(self.allocator, node);
        defer self.allocator.free(encoded);
        if (encoded.len > self.maxPagePayloadBytes()) return error.DocumentIndexNodeTooLarge;
        const page_id = try page_allocator.allocate();
        try page_allocator.writePage(page_id, .document_index, encoded);
        return page_id;
    }

    fn putIndexedDocumentBatch(self: *NativeFile, mutations: []const DocumentMutation) !void {
        const previous = self.activeCheckpoint();
        var local: PageAllocator = undefined;
        if (self.transaction_pages == null) local = try self.pageAllocatorFromFreeMap(previous);
        defer if (self.transaction_pages == null) local.deinit();
        const pages = self.transaction_pages orelse &local;
        pages.pack_records = true;
        var editor = IndexEditor.init(self, previous, previous.document_index_root_page);
        defer editor.deinit();
        editor.useSortedBatch(pages, mutations);
        const bulk = previous.document_index_root_page == 0 and mutationKeysSorted(mutations);
        var builder = DocumentIndexBulkBuilder{ .owner = self, .file = self.file, .next_page_id = &pages.next_page_id, .pages = pages };
        defer builder.deinit();
        for (mutations, 0..) |mutation, ordinal| {
            if (bulk and ordinal + 1 < mutations.len and std.mem.eql(u8, mutation.key, mutations[ordinal + 1].key)) continue;
            if (mutation.is_delete) {
                try editor.remove(mutation.key);
                continue;
            }
            var value_root = mutation.external_value_root_page;
            if (value_root == 0 and !self.documentEntryFitsInline(mutation.key, mutation.value)) value_root = try self.writeValuePagesAllocated(pages, mutation.value);
            const record = try pages.writeDocument(.{ .previous_page = 0, .previous_namespace_page = 0, .key = mutation.key, .value = mutation.value, .external_value_root_page = value_root, .external_value_len = mutation.external_value_len });
            if (bulk) try builder.add(mutation.key, record) else try editor.put(mutation.key, record);
        }
        var next = previous;
        next.commit_sequence += 1;
        next.document_root_page = 0;
        next.namespace_directory_root_page = 0;
        next.document_index_root_page = if (bulk) try builder.finish() else try editor.finish(pages);
        next.page_count = pages.next_page_id;
        if (self.transaction_pages == null) {
            next.free_map_root_page = try pages.allocate();
            try self.persistAllocator(pages, &next);
            try self.syncIfRequired();
        }
        try self.publishCheckpoint(next);
    }

    fn decodeKey(payload: []const u8) ![]const u8 {
        if (payload.len <= 8 or !std.mem.eql(u8, payload[0..8], "AFKEY004")) return error.InvalidDocumentIndex;
        return payload[8..];
    }
    fn materializeIndexKeys(self: *NativeFile, pages: *PageAllocator, node: DocumentIndexNode) !void {
        const references = node.key_pages orelse return error.InvalidDocumentIndex;
        for (references, node.keys) |*reference, key| {
            if (reference.* == 0) continue;
            // Buffered record bundles must be readable before resolving keys.
            try pages.flush();
            var checkpoint = self.activeCheckpoint();
            checkpoint.page_count = pages.next_page_id;
            const raw = try self.readPageAllocForCheckpoint(self.allocator, reference.*, checkpoint);
            defer self.allocator.free(raw);
            if (raw[4] == @backingInt(PageKind.key)) {
                _ = try decodeKey(try decodePagePayload(raw, .key));
                continue;
            }
            const bytes = if (key.len != 0) key else switch (raw[4]) {
                @backingInt(PageKind.document) => (try decodeDocumentEntry(try decodePagePayload(raw, .document))).key,
                @backingInt(PageKind.key) => try NativeFile.decodeKey(try decodePagePayload(raw, .key)),
                @backingInt(PageKind.catalog) => (try decodeCatalogEntry(try decodePagePayload(raw, .catalog))).key,
                else => return error.InvalidDocumentIndex,
            };
            const payload = try self.allocator.alloc(u8, 8 + bytes.len);
            defer self.allocator.free(payload);
            @memcpy(payload[0..8], "AFKEY004");
            @memcpy(payload[8..], bytes);
            const page = try pages.allocate();
            try pages.writePage(page, .key, payload);
            reference.* = page;
        }
    }

    fn upsertDocumentIndex(
        self: *NativeFile,
        page_allocator: *PageAllocator,
        root_page_id: u64,
        key: []const u8,
        document_page_id: u64,
        checkpoint: CheckpointSlot,
    ) !u64 {
        var editor = IndexEditor.init(self, checkpoint, root_page_id);
        editor.accounting_pages = page_allocator;
        defer editor.deinit();
        try editor.put(key, document_page_id);
        return try editor.finish(page_allocator);
    }

    fn lookupDocumentIndexPage(self: *NativeFile, checkpoint: CheckpointSlot, key: []const u8) !?u64 {
        var page_id = checkpoint.document_index_root_page;
        var scratch: [65536]u8 = undefined;
        var view = IndexReadFrame{};
        defer view.deinit(self.allocator);
        var depth: usize = 0;
        while (page_id != 0) : (depth += 1) {
            if (depth > 64) return error.InvalidDocumentIndex;
            if (page_id >= checkpoint.page_count) return error.InvalidPageId;
            if (try self.probeCachedDocumentIndex(page_id, checkpoint, key, &view)) |probe| {
                if (builtin.is_test) _ = self.test_page_reads.fetchAdd(1, .monotonic);
                if (probe.leaf) return probe.page;
                page_id = probe.page orelse return error.InvalidDocumentIndex;
                continue;
            }
            const raw = try decodePagePayload(try self.readPageInto(page_id, checkpoint, &scratch), .document_index);
            const probe = (try self.probeCachedDocumentIndex(page_id, checkpoint, key, &view)) orelse (probeDocumentIndexNode(raw, key) catch |err| switch (err) {
                error.ExternalIndexKey => blk: {
                    try view.decode(self.allocator, raw);
                    const low = try view.bound(self, checkpoint, key, view.kind == .internal);
                    if (view.kind == .leaf) {
                        const matches = low < view.offsets.items.len and std.mem.eql(u8, try view.key(self, checkpoint, low), key);
                        break :blk IndexProbe{ .leaf = true, .page = if (matches) view.pointer(low) else null };
                    }
                    break :blk IndexProbe{ .leaf = false, .page = view.pointer(low) };
                },
                else => return err,
            });
            if (probe.leaf) return probe.page;
            page_id = probe.page orelse return error.InvalidDocumentIndex;
        }
        return null;
    }

    fn probeCachedDocumentIndex(self: *NativeFile, page_id: u64, checkpoint: CheckpointSlot, key: []const u8, scratch: *IndexReadFrame) !?IndexProbe {
        if (!self.page_cache_enabled.load(.monotonic) or self.page_cache_bypass.load(.monotonic) != 0) return null;
        const cached = (try self.page_cache.acquireIndex(self.allocator, page_id)) orelse return null;
        defer self.page_cache.releaseIndex(self.allocator, cached);
        if (builtin.is_test) _ = self.test_index_view_hits.fetchAdd(1, .monotonic);
        // Only the caller's overflow scratch is mutable. The pin protects raw
        // bytes and offsets across eviction, replacement, and recursive reads.
        var view = cached.frame;
        view.overflow = scratch.overflow;
        view.overflow_reference = scratch.overflow_reference;
        view.overflow_key = scratch.overflow_key;
        defer {
            scratch.overflow = view.overflow;
            scratch.overflow_reference = view.overflow_reference;
            scratch.overflow_key = view.overflow_key;
        }
        const low = try view.bound(self, checkpoint, key, view.kind == .internal);
        if (view.kind == .leaf) {
            const matches = low < view.offsets.items.len and std.mem.eql(u8, try view.key(self, checkpoint, low), key);
            return .{ .leaf = true, .page = if (matches) view.pointer(low) else null };
        }
        return .{ .leaf = false, .page = view.pointer(low) };
    }

    pub fn documentValueAtIndexEntryAlloc(self: *NativeFile, allocator: Allocator, checkpoint: CheckpointSlot, indexed: DocumentIndexEntry) !?[]u8 {
        const payload = try self.readPagePayloadByKindAllocForCheckpoint(allocator, indexed.document_page_id, .document, checkpoint);
        defer allocator.free(payload);
        const entry = try decodeDocumentEntry(payload);
        if (!std.mem.eql(u8, entry.key, indexed.key)) return error.InvalidDocumentIndex;
        if (entry.is_delete) return null;
        return if (entry.external_value_root_page != 0) try self.readValuePagesAtCheckpointAlloc(allocator, entry.external_value_root_page, entry.external_value_len, checkpoint) else try allocator.dupe(u8, entry.value);
    }

    pub fn getDocumentAlloc(self: *NativeFile, allocator: Allocator, key: []const u8) !?[]u8 {
        const checkpoint = self.activeCheckpoint();
        if (self.stagedEntry(.documents, key)) |entry| {
            if (entry.is_delete) return null;
            return try self.catalogEntryValueAtCheckpointAlloc(allocator, entry, checkpoint);
        }
        return try self.getDocumentAtCheckpointAlloc(allocator, checkpoint, key);
    }

    /// Resolves a key against the ordered index root pinned by `checkpoint`.
    /// Referenced document pages are reclaimed only by vacuum.
    pub fn getDocumentAtCheckpointAlloc(self: *NativeFile, allocator: Allocator, checkpoint: CheckpointSlot, key: []const u8) !?[]u8 {
        const page_id = (try self.lookupDocumentIndexPage(checkpoint, key)) orelse return null;
        var scratch: [65536]u8 = undefined;
        const payload = try decodePagePayload(try self.readPageInto(page_id, checkpoint, &scratch), .document);
        const entry = try decodeDocumentEntry(payload);
        if (!std.mem.eql(u8, entry.key, key)) return error.InvalidDocumentIndex;
        if (entry.is_delete) return null;
        return if (entry.external_value_root_page != 0) try self.readValuePagesAtCheckpointAlloc(allocator, entry.external_value_root_page, entry.external_value_len, checkpoint) else try allocator.dupe(u8, entry.value);
    }

    /// Read one document into reusable caller storage. Oversized values are
    /// rejected before loading external pages, and no transaction read cache
    /// retains a copy. Only the initialized prefix of out is returned.
    pub fn readDocumentInto(self: *NativeFile, checkpoint: CheckpointSlot, key: []const u8, out: []u8) ![]const u8 {
        const page_id = (try self.lookupDocumentIndexPage(checkpoint, key)) orelse return error.NotFound;
        var scratch: [65536]u8 = undefined;
        const payload = try decodePagePayload(try self.readPageInto(page_id, checkpoint, &scratch), .document);
        const entry = try decodeDocumentEntry(payload);
        if (!std.mem.eql(u8, entry.key, key)) return error.InvalidDocumentIndex;
        if (entry.is_delete) return error.NotFound;
        const length = if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len;
        if (length > out.len) return error.BufferTooSmall;
        if (entry.external_value_root_page != 0) {
            try self.readValuePagesRangeInto(entry.external_value_root_page, length, 0, out[0..length], checkpoint);
        } else {
            @memcpy(out[0..length], entry.value);
        }
        return out[0..length];
    }

    /// Sorted multi-read: decode each visited index node once, and descend
    /// only into children containing requested keys. Each returned value is
    /// independently owned; errors release and clear every output.
    pub fn getDocumentsAtCheckpointAlloc(self: *NativeFile, allocator: Allocator, checkpoint: CheckpointSlot, keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        @memset(values, null);
        for (keys, 0..) |key, i| {
            if (i > 0 and std.mem.order(u8, keys[i - 1], key) == .gt) return error.InvalidBatch;
        }
        errdefer {
            for (values) |value| if (value) |bytes| allocator.free(bytes);
            @memset(values, null);
        }
        if (keys.len == 0 or checkpoint.document_index_root_page == 0) return;
        const references = try allocator.alloc(u64, keys.len);
        defer allocator.free(references);
        @memset(references, 0);
        try self.readDocumentBatchNode(checkpoint, checkpoint.document_index_root_page, keys, references);
        const order = try allocator.alloc(usize, keys.len);
        defer allocator.free(order);
        for (order, 0..) |*position, i| position.* = i;
        std.mem.sort(usize, order, references, struct {
            fn less(refs: []const u64, lhs: usize, rhs: usize) bool {
                return recordReferenceLessThan(refs[lhs], refs[rhs]);
            }
        }.less);
        var reader = RecordPageReader{};
        defer reader.deinit(allocator);
        for (order) |i| {
            const reference = references[i];
            if (reference == 0) continue;
            const bytes = try reader.read(self, allocator, checkpoint, reference, .document);
            const entry = try decodeDocumentEntry(bytes);
            if (!std.mem.eql(u8, entry.key, keys[i])) return error.InvalidDocumentIndex;
            if (!entry.is_delete) values[i] = if (entry.external_value_root_page != 0)
                try self.readValuePagesAtCheckpointAlloc(allocator, entry.external_value_root_page, entry.external_value_len, checkpoint)
            else
                try allocator.dupe(u8, entry.value);
        }
    }

    fn readDocumentBatchNode(self: *NativeFile, checkpoint: CheckpointSlot, page: u64, keys: []const []const u8, references: []u64) !void {
        var path = IndexReadPath{};
        defer path.deinit(self.allocator);
        try self.readDocumentBatchWithPath(checkpoint, page, keys, references, &path, 0);
    }

    fn readDocumentBatchWithPath(self: *NativeFile, checkpoint: CheckpointSlot, page: u64, keys: []const []const u8, references: []u64, path: *IndexReadPath, depth: usize) !void {
        const node = try path.load(self, checkpoint, depth, page);
        var group_start: usize = 0;
        var group_child: usize = 0;
        var previous_bound: usize = 0;
        for (keys, 0..) |key, i| {
            const low = if (i == 0) try node.bound(self, checkpoint, key, node.kind == .internal) else try node.boundFrom(self, checkpoint, key, node.kind == .internal, previous_bound);
            previous_bound = low;
            if (node.kind == .leaf) {
                if (low < node.offsets.items.len and std.mem.eql(u8, try node.key(self, checkpoint, low), key)) references[i] = node.pointer(low);
            } else {
                if (i > 0 and low != group_child) {
                    try self.readDocumentBatchWithPath(checkpoint, node.pointer(group_child), keys[group_start..i], references[group_start..i], path, depth + 1);
                    group_start = i;
                }
                group_child = low;
            }
        }
        if (node.kind == .internal) try self.readDocumentBatchWithPath(checkpoint, node.pointer(group_child), keys[group_start..], references[group_start..], path, depth + 1);
    }

    /// Existence query over live record metadata, never document values. Skip
    /// the excluded key range in one seek; document indexes retain tombstones,
    /// so an index entry alone is not proof that a live document exists.
    pub fn hasLiveDocumentOutsidePrefix(self: *NativeFile, checkpoint: CheckpointSlot, excluded_prefix: []const u8) !bool {
        if (excluded_prefix.len == 0) return false;
        var cursor = DocumentIndexCursor.init(self, checkpoint);
        defer cursor.deinit();
        var records = RecordPageReader{};
        defer records.deinit(self.allocator);
        var current = try cursor.first();
        while (current) |indexed| {
            var owned = indexed;
            defer owned.deinit(self.allocator);
            if (std.mem.startsWith(u8, owned.key, excluded_prefix)) {
                const successor = try self.allocator.dupe(u8, excluded_prefix);
                defer self.allocator.free(successor);
                var end = successor.len;
                while (end > 0 and successor[end - 1] == 0xff) end -= 1;
                if (end == 0) return false;
                successor[end - 1] += 1;
                current = try cursor.seekAtOrAfter(successor[0..end], false);
                continue;
            }
            const entry = try decodeDocumentEntry(try records.read(self, self.allocator, checkpoint, owned.document_page_id, .document));
            if (!std.mem.eql(u8, entry.key, owned.key)) return error.InvalidDocumentIndex;
            if (!entry.is_delete) return true;
            current = try cursor.next();
        }
        return false;
    }

    pub fn snapshotDocumentsAlloc(self: *NativeFile, allocator: Allocator) ![]OwnedDocument {
        return try self.snapshotDocumentsWithPrefixAlloc(allocator, "");
    }

    /// Materializes live documents in key order from the pinned index. Prefix
    /// seeks skip unrelated keys; historical versions never enter the scan.
    pub fn snapshotDocumentsWithPrefixAlloc(self: *NativeFile, allocator: Allocator, prefix: []const u8) ![]OwnedDocument {
        return try self.snapshotDocumentsWithPrefixAtCheckpointAlloc(allocator, prefix, try self.materializeTransactionCheckpoint());
    }

    fn snapshotDocumentsWithPrefixAtCheckpointAlloc(self: *NativeFile, allocator: Allocator, prefix: []const u8, checkpoint: CheckpointSlot) ![]OwnedDocument {
        var docs = std.ArrayListUnmanaged(OwnedDocument).empty;
        errdefer {
            for (docs.items) |doc| {
                allocator.free(doc.key);
                allocator.free(doc.value);
            }
            docs.deinit(allocator);
        }
        var references = std.ArrayListUnmanaged(u64).empty;
        defer references.deinit(allocator);
        var cursor = DocumentIndexCursor.initWithKeyAllocator(self, checkpoint, allocator);
        defer cursor.deinit();
        var current = if (prefix.len == 0) try cursor.first() else try cursor.seekAtOrAfter(prefix, false);
        while (current) |indexed| {
            var owned = indexed;
            defer owned.deinit(allocator);
            if (!std.mem.startsWith(u8, indexed.key, prefix)) break;
            try docs.ensureUnusedCapacity(allocator, 1);
            try references.ensureUnusedCapacity(allocator, 1);
            docs.appendAssumeCapacity(.{ .key = owned.key, .value = &.{} });
            references.appendAssumeCapacity(indexed.document_page_id);
            owned.key = &.{}; // Transfer caller-allocated ownership exactly once.
            current = try cursor.next();
        }
        // Materialization already retains every result. One reference per live
        // index entry lets us read each packed record page once, even after
        // scattered updates. Sort the owned results back into key order below.
        const PhysicalOrder = struct {
            docs: []OwnedDocument,
            references: []u64,
            pub fn lessThan(ctx: @This(), a: usize, b: usize) bool {
                return recordReferenceLessThan(ctx.references[a], ctx.references[b]);
            }
            pub fn swap(ctx: @This(), a: usize, b: usize) void {
                std.mem.swap(OwnedDocument, &ctx.docs[a], &ctx.docs[b]);
                std.mem.swap(u64, &ctx.references[a], &ctx.references[b]);
            }
        };
        std.sort.pdqContext(0, docs.items.len, PhysicalOrder{ .docs = docs.items, .references = references.items });
        var records = RecordPageReader{};
        defer records.deinit(allocator);
        for (docs.items, references.items) |*doc, *reference| {
            const entry = try decodeDocumentEntry(try records.read(self, allocator, checkpoint, reference.*, .document));
            if (!std.mem.eql(u8, entry.key, doc.key)) return error.InvalidDocumentIndex;
            if (entry.is_delete) {
                // Legacy revision-3 indexes may retain their latest tombstones.
                allocator.free(doc.key);
                doc.key = &.{};
                reference.* = 0;
                continue;
            }
            doc.value = if (entry.external_value_root_page != 0)
                try self.readValuePagesAtCheckpointAlloc(allocator, entry.external_value_root_page, entry.external_value_len, checkpoint)
            else
                try allocator.dupe(u8, entry.value);
        }
        var live: usize = 0;
        for (docs.items, references.items) |doc, reference| {
            if (reference == 0) continue;
            docs.items[live] = doc;
            live += 1;
        }
        docs.items.len = live;
        std.mem.sort(OwnedDocument, docs.items, {}, struct {
            fn lessThan(_: void, a: OwnedDocument, b: OwnedDocument) bool {
                return std.mem.lessThan(u8, a.key, b.key);
            }
        }.lessThan);
        return try docs.toOwnedSlice(allocator);
    }

    pub fn freeSnapshotDocuments(allocator: Allocator, docs: []OwnedDocument) void {
        for (docs) |doc| {
            allocator.free(doc.key);
            allocator.free(doc.value);
        }
        allocator.free(docs);
    }

    /// Immutable page window. Extents supply proven contiguous leaf runs;
    /// linked chains grow read-ahead only while physical adjacency persists.
    /// Admit requested pages according to policy, never speculative neighbors.
    const ValuePageReader = struct {
        bytes: [64 * 1024]u8 = undefined,
        lease: ?*PageCache.RawView = null,
        first: u64 = 0,
        count: usize = 0,
        admitted: u16 = 0,
        chain_previous: u64 = 0,
        chain_run: usize = 0,
        /// Cold artifact export keeps navigation hot without admitting payloads.
        cache_payload: bool = true,

        fn deinit(self: *@This(), file: *NativeFile) void {
            if (self.lease) |lease| file.page_cache.releaseRaw(file.allocator, lease);
            self.lease = null;
            self.count = 0;
            self.admitted = 0;
        }

        fn requested(self: *@This(), file: *NativeFile, index: usize) []const u8 {
            const size: usize = file.header.page_size;
            const page = self.bytes[index * size ..][0..size];
            const bit = @as(u16, 1) << @as(u4, @intCast(index));
            if (self.admitted & bit == 0 and file.page_cache_enabled.load(.monotonic) and
                file.page_cache_bypass.load(.monotonic) == 0 and
                (self.cache_payload or page[4] != @backingInt(PageKind.value)) and file.admitPageToCache(page))
            {
                file.page_cache.put(file.allocator, self.first + index, page);
                self.admitted |= bit;
            }
            return page;
        }

        pub fn read(self: *@This(), file: *NativeFile, checkpoint: CheckpointSlot, page: u64, ahead: usize) ![]const u8 {
            if (page == 0 or page >= checkpoint.page_count) return error.InvalidPageId;
            if (builtin.is_test) _ = file.test_page_reads.fetchAdd(1, .monotonic);
            const size: usize = file.header.page_size;
            if (page >= self.first and page - self.first < self.count)
                return if (self.lease) |lease| lease.bytes else self.requested(file, @intCast(page - self.first));
            self.deinit(file);
            const use_cache = file.page_cache_enabled.load(.monotonic) and file.page_cache_bypass.load(.monotonic) == 0;
            if (use_cache) {
                if (file.page_cache.acquireRaw(file.allocator, page, size)) |lease| {
                    self.lease = lease;
                    self.first = page;
                    self.count = 1;
                    self.admitted = 1;
                    if (builtin.is_test) _ = file.test_value_cache_borrowed_bytes.fetchAdd(size, .monotonic);
                    return lease.bytes;
                }
            }
            if (use_cache and file.page_cache.copyInto(page, self.bytes[0..size])) {
                if (builtin.is_test) _ = file.test_value_cache_copy_bytes.fetchAdd(size, .monotonic);
                self.first = page;
                self.count = 1;
                self.admitted = 1;
                return self.bytes[0..size];
            }
            const count: usize = @intCast(@min(@max(ahead, 1), self.bytes.len / size, checkpoint.page_count - page));
            if (builtin.is_test) {
                _ = file.test_value_read_calls.fetchAdd(1, .monotonic);
                _ = file.test_value_read_bytes.fetchAdd(count * size, .monotonic);
            }
            try file.readBackingPages(self.bytes[0 .. count * size], page * size);
            self.first = page;
            self.count = count;
            return self.requested(file, 0);
        }

        fn readChain(self: *@This(), file: *NativeFile, checkpoint: CheckpointSlot, page: u64, remaining: usize) ![]const u8 {
            self.chain_run = if (self.chain_previous != 0 and page > self.chain_previous and page - self.chain_previous == 1) @min(16, self.chain_run + 1) else 1;
            self.chain_previous = page;
            const needed = 1 + (remaining -| 1) / file.maxValuePagePayloadBytes();
            return self.read(file, checkpoint, page, @min(self.chain_run, needed));
        }
    };

    // The parent already owns these references. Bound prefetch by both
    // physical adjacency and the requested logical range, without allocating
    // a separate run list or reading unrelated pages between fragments.
    fn extentLeafReadAhead(children: []const ExtentRef, index: usize, remaining: u64) usize {
        var count: usize = 1;
        var covered = children[index].len;
        while (count < 16 and index + count < children.len and covered < remaining) : (count += 1) {
            const next = children[index + count];
            if (next.height != 0 or next.page <= children[index].page or next.page - children[index].page != count) break;
            covered += next.len;
        }
        return count;
    }

    /// Sequential, checksum-checked reader for both extent trees and document
    /// value chains. Holds one page and one extent node per tree level.
    const ValueChunkCursor = struct {
        const Frame = struct { node: ExtentNode, position: usize = 0 };
        file: *NativeFile,
        checkpoint: CheckpointSlot,
        // Four levels cover ordinary artifact trees without allocator traffic.
        // The format permits at most 63 levels; deeper trees use one exact,
        // bounded overflow allocation, reused for the entire traversal.
        frames: std.ArrayListUnmanaged(Frame) = .empty,
        inline_frames: [4]Frame = undefined,
        frame_count: usize = 0,
        max_depth: usize = 0,
        pending: ?ExtentRef,
        chain_page: u64,
        remaining: usize,
        value_length: usize = 0,
        read_end: ?usize = null,
        skip: usize = 0,
        reader: ValuePageReader = .{},

        fn init(file: *NativeFile, root: u64, len: usize) !ValueChunkCursor {
            return initAtCheckpoint(file, root, len, file.activeCheckpoint());
        }

        fn initAtCheckpoint(file: *NativeFile, root: u64, len: usize, checkpoint: CheckpointSlot) !ValueChunkCursor {
            if (len == 0) return error.InvalidNativeValueChain;
            return initWithCacheIntent(file, root, len, checkpoint, true);
        }

        fn initWithCacheIntent(file: *NativeFile, root: u64, len: usize, checkpoint: CheckpointSlot, cache_payload: bool) !ValueChunkCursor {
            if (len == 0) return error.InvalidNativeValueChain;
            var cursor = ValueChunkCursor{ .file = file, .checkpoint = checkpoint, .pending = null, .chain_page = root, .remaining = len, .value_length = len, .reader = .{ .cache_payload = cache_payload } };
            errdefer cursor.deinit();
            const raw = try cursor.reader.read(file, checkpoint, root, 1);
            if (raw[4] == @backingInt(PageKind.value_extent)) {
                const node = try decodeExtentNode(try decodePagePayload(raw, .value_extent), len);
                cursor.max_depth = node.height;
                cursor.pending = .{ .page = root, .len = len, .height = node.height };
                cursor.chain_page = 0;
            } else {
                const leaf = try decodeValuePage(try decodePagePayload(raw, .value));
                if (leaf.next_page == 0) {
                    if (leaf.chunk.len != len) return error.InvalidNativeValueChain;
                    cursor.pending = .{ .page = root, .len = len };
                    cursor.chain_page = 0;
                }
            }
            return cursor;
        }

        fn readAheadRemaining(self: *const ValueChunkCursor) usize {
            const position = self.value_length - self.remaining;
            return @min(self.remaining, (self.read_end orelse self.value_length) -| position);
        }

        fn frameSlice(self: *ValueChunkCursor) []Frame {
            return if (self.frames.capacity != 0) self.frames.items else self.inline_frames[0..self.frame_count];
        }

        fn pushFrame(self: *ValueChunkCursor, node: ExtentNode) !void {
            if (self.frame_count >= self.max_depth) return error.InvalidNativeValueChain;
            if (self.frames.capacity == 0 and self.frame_count < self.inline_frames.len) {
                self.inline_frames[self.frame_count] = .{ .node = node };
            } else {
                if (self.frames.capacity == 0) {
                    try self.frames.ensureTotalCapacityPrecise(self.file.allocator, self.max_depth);
                    self.frames.appendSliceAssumeCapacity(self.inline_frames[0..self.frame_count]);
                }
                self.frames.appendAssumeCapacity(.{ .node = node });
            }
            self.frame_count += 1;
        }

        fn popFrame(self: *ValueChunkCursor) void {
            self.frame_count -= 1;
            if (self.frames.capacity != 0) self.frames.items.len = self.frame_count;
        }

        pub fn deinit(self: *ValueChunkCursor) void {
            self.reader.deinit(self.file);
            self.frames.deinit(self.file.allocator);
        }

        /// The returned slice is borrowed until next(). Cancellation is checked
        /// on every page, including internal nodes of large extent trees.
        fn next(self: *ValueChunkCursor, cancel: ?*const maintenance.CancelToken) !?[]const u8 {
            while (true) {
                if (cancel) |token| try token.check();
                if (self.chain_page != 0) {
                    const raw = try self.reader.readChain(self.file, self.checkpoint, self.chain_page, self.readAheadRemaining());
                    const value = try decodeValuePage(try decodePagePayload(raw, .value));
                    if (value.chunk.len == 0 or value.chunk.len > self.remaining) return error.InvalidNativeValueChain;
                    self.remaining -= value.chunk.len;
                    if ((self.remaining == 0) != (value.next_page == 0)) return error.InvalidNativeValueChain;
                    self.chain_page = value.next_page;
                    const skipped = @min(self.skip, value.chunk.len);
                    self.skip -= skipped;
                    if (skipped == value.chunk.len) continue;
                    return value.chunk[skipped..];
                }
                const ref = self.pending orelse blk: {
                    while (self.frame_count > 0) {
                        const frame = &self.frameSlice()[self.frame_count - 1];
                        if (frame.position == frame.node.count) {
                            self.popFrame();
                            continue;
                        }
                        const child = frame.node.children[frame.position];
                        frame.position += 1;
                        break :blk child;
                    }
                    if (self.remaining != 0) return error.InvalidNativeValueChain;
                    return null;
                };
                self.pending = null;
                if (self.skip >= ref.len) {
                    if (ref.len > self.remaining) return error.InvalidNativeValueChain;
                    self.skip -= ref.len;
                    self.remaining -= ref.len;
                    continue;
                }
                const ahead = if (ref.height == 0 and self.frame_count != 0) blk: {
                    const frame = &self.frameSlice()[self.frame_count - 1];
                    break :blk extentLeafReadAhead(frame.node.children[0..frame.node.count], frame.position - 1, self.readAheadRemaining());
                } else 1;
                const raw = try self.reader.read(self.file, self.checkpoint, ref.page, ahead);
                if (ref.height != 0) {
                    const node = try decodeExtentNode(try decodePagePayload(raw, .value_extent), ref.len);
                    if (node.height != ref.height) return error.InvalidNativeValueChain;
                    try self.pushFrame(node);
                    continue;
                }
                const value = try decodeValuePage(try decodePagePayload(raw, .value));
                if (value.next_page != 0 or value.chunk.len == 0 or value.chunk.len != ref.len or value.chunk.len > self.remaining)
                    return error.InvalidNativeValueChain;
                self.remaining -= value.chunk.len;
                const skipped = self.skip;
                self.skip = 0;
                return value.chunk[skipped..];
            }
        }
    };

    /// Repack arbitrary source chunk boundaries into full output pages. Catalog
    /// values use the bounded extent frontier; documents use contiguous chains.
    fn copyExternalValue(self: *NativeFile, file: std.Io.File, next_page: *u64, root: u64, len: usize, tree: bool, cancel: ?*const maintenance.CancelToken, tracking: ?*PageAllocator) !u64 {
        var source = try ValueChunkCursor.init(self, root, len);
        defer source.deinit();
        var writer = NativeFile{
            .allocator = self.allocator,
            .io_impl = undefined,
            .borrowed_io = self.runtimeIo(),
            .path = @constCast(""),
            .file = file,
            .header = .{ .page_size = self.header.page_size },
            .no_sync = true,
            .page_cache_enabled = .init(false),
            .max_file_bytes = self.vacuum_workspace_limit,
        };
        var pages = PageAllocator{ .file = &writer, .free_pages = &.{}, .next_page_id = next_page.*, .ledger = if (tracking) |target| target.ledger else null, .can_reuse = false };
        defer pages.deinit();
        var builder = ExtentAppender{ .file = &writer, .pages = &pages, .tail = undefined, .batch = .{ .file = &writer } };
        defer builder.deinit();
        const chain_root = next_page.*;
        var buffer: [65536]u8 = undefined;
        const chunk_size = self.maxValuePagePayloadBytes();
        var buffered: usize = 0;
        var written: usize = 0;
        while (try source.next(cancel)) |chunk| {
            var offset: usize = 0;
            while (offset < chunk.len) {
                const n = @min(chunk_size - buffered, chunk.len - offset);
                @memcpy(buffer[value_page_header_size + buffered ..][0..n], chunk[offset..][0..n]);
                buffered += n;
                offset += n;
                if (buffered == chunk_size or written + buffered == len) {
                    if (cancel) |token| try token.check();
                    if (tree) {
                        try builder.pushValue(buffer[value_page_header_size..][0..buffered]);
                    } else {
                        const page = try pages.allocate();
                        const next = if (written + buffered < len) page + 1 else 0;
                        try pages.noteValue(page, len - written);
                        if (next != 0) try pages.retainValue(next);
                        try builder.batch.appendValue(page, next, buffer[value_page_header_size..][0..buffered]);
                    }
                    written += buffered;
                    buffered = 0;
                }
            }
        }
        if (buffered != 0 or written != len) return error.InvalidNativeValueChain;
        if (!tree) {
            try builder.batch.flush();
            next_page.* = pages.next_page_id;
            try transferPendingValues(&pages, tracking);
            return chain_root;
        }
        const result = try builder.finish();
        next_page.* = pages.next_page_id;
        try transferPendingValues(&pages, tracking);
        return result.page;
    }

    fn transferPendingValues(source: *PageAllocator, tracking: ?*PageAllocator) !void {
        const target = tracking orelse return;
        var pending = source.pending_values.iterator();
        while (pending.next()) |entry| try target.pending_values.put(target.file.allocator, entry.key_ptr.*, entry.value_ptr.*);
    }

    const VacuumValue = struct {
        inline_value: []const u8,
        owned: ?[]u8 = null,
        root: u64 = 0,
        len: usize,

        pub fn deinit(self: VacuumValue, allocator: Allocator) void {
            if (self.owned) |value| allocator.free(value);
        }
    };

    fn copyVacuumValue(self: *NativeFile, file: std.Io.File, next_page: *u64, entry: CatalogEntry, tree: bool, cancel: ?*const maintenance.CancelToken, tracking: ?*PageAllocator) !VacuumValue {
        const len = if (entry.external_value_root_page == 0) entry.value.len else entry.external_value_len;
        if (entry.external_value_root_page == 0) return .{ .inline_value = entry.value, .len = len };
        const header_len: usize = if (tree) 16 else 28;
        if (header_len + entry.key.len + len <= self.maxPagePayloadBytes()) {
            // Re-inlining an external value allocates at most one page.
            const value = try self.catalogEntryValueAlloc(self.allocator, entry);
            return .{ .inline_value = value, .owned = value, .len = len };
        }
        const root = try self.copyExternalValue(file, next_page, entry.external_value_root_page, len, tree, cancel, tracking);
        return .{ .inline_value = "", .root = root, .len = len };
    }

    // These catalog references lease pages only in their source inode.
    // Readers own that descriptor across adoption; copying a temporary lease
    // into the replacement would retain an otherwise dead artifact twice.
    pub const artifact_lease_key_prefix = "\x00AFLEASE/";

    fn copyVacuumCatalogRecords(
        self: *NativeFile,
        compact_file: std.Io.File,
        root: CatalogRoot,
        next_page_id: *u64,
        destination_root_page: *u64,
        live_bytes: *u64,
        cancel: ?*const maintenance.CancelToken,
    ) !usize {
        var cursor = try LiveRecordCursor.init(self, .{ .catalog = root });
        defer cursor.deinit();

        const io = self.runtimeIo();
        var writer = NativeFile{ .allocator = self.allocator, .io_impl = undefined, .borrowed_io = io, .path = @constCast(""), .file = compact_file, .header = self.header, .page_cache_enabled = .init(false), .max_file_bytes = self.vacuum_workspace_limit };
        writer.header.indexed_reclamation = self.header.indexed_reclamation or self.vacuum_target_indexed;
        var pages = PageAllocator{ .file = &writer, .free_pages = &.{}, .next_page_id = next_page_id.*, .pack_records = true };
        defer pages.deinit();
        var key_index = DocumentIndexBulkBuilder{ .owner = &writer, .file = compact_file, .next_page_id = &pages.next_page_id, .pages = &pages };
        defer key_index.deinit();
        var count: usize = 0;
        while (try cursor.next(cancel)) |record| {
            if (root == .metadata and std.mem.startsWith(u8, record.key, artifact_lease_key_prefix)) continue;
            const value = try self.copyVacuumValue(compact_file, &pages.next_page_id, record, true, cancel, null);
            defer value.deinit(self.allocator);
            destination_root_page.* = try pages.writeCatalog(.{
                .previous_page = if (writer.header.indexed_reclamation) 0 else destination_root_page.*,
                .key = record.key,
                .value = value.inline_value,
                .external_value_root_page = value.root,
                .external_value_len = value.len,
            });
            try key_index.add(record.key, destination_root_page.*);
            live_bytes.* +|= record.key.len + value.len;
            count += 1;
        }
        const index_root = try key_index.finish();
        if (count > 0) {
            destination_root_page.* = try writer.writeCatalogRoot(&pages, destination_root_page.*, index_root, 0);
        }
        try pages.flush();
        next_page_id.* = pages.next_page_id;
        return count;
    }

    fn copyVacuumDocumentRecords(
        self: *NativeFile,
        compact_file: std.Io.File,
        next_page_id: *u64,
        document_root_page: *u64,
        document_index_root_page: *u64,
        namespace_directory: *NamespaceDirectory,
        live_bytes: *u64,
        cancel: ?*const maintenance.CancelToken,
    ) !usize {
        var cursor = try LiveRecordCursor.init(self, .documents);
        defer cursor.deinit();

        const io = self.runtimeIo();
        var writer = NativeFile{ .allocator = self.allocator, .io_impl = undefined, .borrowed_io = io, .path = @constCast(""), .file = compact_file, .header = self.header, .page_cache_enabled = .init(false), .max_file_bytes = self.vacuum_workspace_limit };
        writer.header.indexed_reclamation = self.header.indexed_reclamation or self.vacuum_target_indexed;
        var pages = PageAllocator{ .file = &writer, .free_pages = &.{}, .next_page_id = next_page_id.*, .pack_records = true };
        defer pages.deinit();
        var document_index = DocumentIndexBulkBuilder{
            .owner = &writer,
            .file = compact_file,
            .next_page_id = &pages.next_page_id,
            .pages = &pages,
        };
        defer document_index.deinit();
        var count: usize = 0;
        while (try cursor.next(cancel)) |record| {
            const value = try self.copyVacuumValue(compact_file, &pages.next_page_id, record, false, cancel, null);
            defer value.deinit(self.allocator);
            const namespace = documentNamespace(record.key);
            const previous_namespace_page = namespace_directory.get(namespace) orelse 0;
            document_root_page.* = try pages.writeDocument(.{
                .previous_page = if (writer.header.indexed_reclamation) 0 else document_root_page.*,
                .previous_namespace_page = if (writer.header.indexed_reclamation) 0 else previous_namespace_page,
                .key = record.key,
                .value = value.inline_value,
                .external_value_root_page = value.root,
                .external_value_len = value.len,
            });
            try document_index.add(record.key, document_root_page.*);
            if (namespace_directory.getPtr(namespace)) |head| {
                head.* = document_root_page.*;
            } else {
                const owned_namespace = try self.allocator.dupe(u8, namespace);
                namespace_directory.put(self.allocator, owned_namespace, document_root_page.*) catch |err| {
                    self.allocator.free(owned_namespace);
                    return err;
                };
            }
            live_bytes.* +|= record.key.len + value.len;
            count += 1;
        }
        document_index_root_page.* = try document_index.finish();
        if (writer.header.indexed_reclamation) document_root_page.* = 0;
        try pages.flush();
        next_page_id.* = pages.next_page_id;
        return count;
    }

    pub fn vacuum(self: *NativeFile) !VacuumReport {
        return try self.vacuumWithCancel(null);
    }

    pub fn vacuumWithCancel(self: *NativeFile, cancel: ?*const maintenance.CancelToken) !VacuumReport {
        if (self.read_only) return error.ReadOnly;
        var data_lock = try acquireDataRewriteLock(self.runtimeIo(), self.path);
        defer data_lock.file.close(self.runtimeIo());
        var image = try self.prepareVacuum(cancel);
        defer image.deinit();
        try self.publishVacuum(&image);
        return image.report;
    }

    pub fn publishVacuum(self: *NativeFile, image: *VacuumImage) !void {
        const outcome = try self.replaceWithPreparedGeneration(&image.prepared);
        if (self.test_fail_vacuum_after_adoption) {
            self.test_fail_vacuum_after_adoption = false;
            return error.InjectedVacuumPostRenameFailure;
        }
        if (outcome == .durability_unknown) return error.OutcomeUnknown;
    }

    pub fn preparePublicationSequence(self: *NativeFile, sequence: u64) !void {
        var checkpoint = self.activeCheckpoint();
        if (self.header.indexed_reclamation) {
            const old_budget = self.retirement_work_pages;
            self.retirement_work_pages = 0;
            defer self.retirement_work_pages = old_budget;
            var pages = try self.pageAllocatorFromFreeMap(checkpoint);
            defer pages.deinit();
            // Private-image normalization may resume an interrupted owner
            // checkpoint. Complete that builder before replacing its baseline.
            while (pages.ledger.?.root.build_limit != 0) {
                const page = try pages.allocate();
                try pages.ledger.?.checkpointStep(.{ .context = &pages, .allocate = PageAllocator.takeMetadataPage, .read = PageAllocator.readLedgerPage, .write = PageAllocator.writeLedgerPage, .payload_bytes = self.maxPagePayloadBytes(), .cancel_requested = if (self.allocator_cancel_token) |token| &token.requested else null }, page, self.publicationEpoch());
            }
            // Epoch rebasing changes existing events rather than appending
            // new IDs, so this private boundary must serialize the full queue.
            pages.ledger.?.incremental = false;
            try pages.ledger.?.rebaseEpochs(sequence, if (self.allocator_cancel_token) |token| &token.requested else null);
            self.header.checkpoints[self.header.active_checkpoint].commit_sequence = sequence -| 1;
            checkpoint.free_map_root_page = try pages.allocate();
            try self.persistAllocator(&pages, &checkpoint);
        }
        try self.finishPublicationSequence(sequence, checkpoint);
    }

    /// Private generations normalize retirement once before catch-up. Later
    /// changes retire at epoch zero, so final sequence publication needs only
    /// the two header slots and never another ownership-table snapshot.
    pub fn finishPublicationSequence(self: *NativeFile, sequence: u64, selected: CheckpointSlot) !void {
        var checkpoint = selected;
        checkpoint.commit_sequence = sequence;
        // Catch-up can replay keys from failed/no-op foreground mutations, so
        // its private sequence may exceed the live store's publication sequence.
        // Both recovery slots must identify the final image; a newer private
        // fallback slot must never win checkpoint selection after reopen.
        self.header.checkpoints = .{ checkpoint, checkpoint };
        self.header.active_checkpoint = 0;
        var encoded: [header_size]u8 = undefined;
        encodeHeader(&encoded, self.header);
        try self.file.writePositionalAll(self.runtimeIo(), &encoded, 0);
        try self.syncIfRequired();
    }

    // Bound editor state and retained inline values independently. External
    // values are streamed directly into the unpublished image, never retained.
    const catchup_batch_keys = 1024;
    const catchup_batch_bytes = 1024 * 1024;

    pub fn applyCapturedChanges(self: *NativeFile, destination: *NativeFile, capture: *const ChangeCapture, report: *VacuumReport, cancel: ?*const maintenance.CancelToken) !void {
        if (capture.overflow) return error.FileBusy;
        var updated_report = report.*;
        try destination.beginTransaction();
        errdefer destination.abortTransaction();
        const checkpoint = self.activeCheckpoint();
        inline for (0..3) |root| {
            const Mutation = if (root == 2) DocumentMutation else CatalogMutation;
            const map = capture.keys[root];
            const keys = try self.allocator.alloc([]const u8, map.count());
            defer self.allocator.free(keys);
            var iter = map.keyIterator();
            for (keys) |*key| key.* = iter.next().?.*;
            std.mem.sort([]const u8, keys, {}, struct {
                fn less(_: void, lhs: []const u8, rhs: []const u8) bool {
                    return std.mem.order(u8, lhs, rhs) == .lt;
                }
            }.less);
            var reader = RecordPageReader{};
            defer reader.deinit(self.allocator);
            var position: usize = 0;
            while (position < keys.len) {
                var arena = std.heap.ArenaAllocator.init(self.allocator);
                defer arena.deinit();
                const alloc = arena.allocator();
                var mutations = std.ArrayListUnmanaged(Mutation).empty;
                var retained_bytes: usize = 0;
                while (position < keys.len and mutations.items.len < catchup_batch_keys and retained_bytes < catchup_batch_bytes) : (position += 1) {
                    if (cancel) |token| try token.check();
                    const key = keys[position];
                    if (root == 0 and std.mem.startsWith(u8, key, artifact_lease_key_prefix)) continue;
                    const page = if (root == 2) try self.lookupDocumentIndexPage(checkpoint, key) else try self.lookupCatalogPage(checkpoint, if (root == 0) .metadata else .index, key);
                    const old_size = try destination.liveRecordSize(root, key);
                    var record: ?CatalogEntry = null;
                    if (page) |id| {
                        const payload = try reader.read(self, self.allocator, checkpoint, id, if (root == 2) .document else .catalog);
                        if (root == 2) {
                            const doc = try decodeDocumentEntry(payload);
                            if (!doc.is_delete) record = .{ .previous_page = 0, .key = doc.key, .value = doc.value, .external_value_root_page = doc.external_value_root_page, .external_value_len = doc.external_value_len };
                        } else {
                            const entry = try decodeCatalogEntry(payload);
                            if (!entry.is_delete) record = entry;
                        }
                    }
                    const copied: VacuumValue = if (record) |entry| blk: {
                        if (!std.mem.eql(u8, entry.key, key)) return error.InvalidNativePageChain;
                        const writes = try destination.transactionWrites();
                        const value = try self.copyVacuumValue(destination.file, &writes.pages.next_page_id, entry, root != 2, cancel, &writes.pages);
                        errdefer value.deinit(self.allocator);
                        try destination.flushTransactionPayloads();
                        break :blk value;
                    } else .{ .inline_value = "", .len = 0 };
                    defer copied.deinit(self.allocator);
                    if (old_size) |len| {
                        updated_report.live_file_count -= 1;
                        updated_report.live_bytes -= len;
                    }
                    if (record != null) {
                        updated_report.live_file_count += 1;
                        updated_report.live_bytes += key.len + copied.len;
                    }
                    const value = try alloc.dupe(u8, copied.inline_value);
                    retained_bytes += key.len + value.len;
                    try mutations.append(alloc, .{ .key = key, .value = value, .is_delete = record == null, .external_value_root_page = copied.root, .external_value_len = if (copied.root != 0) copied.len else 0 });
                }
                if (root == 2) {
                    try destination.putDocumentBatch(mutations.items);
                } else {
                    try destination.putCatalogBatchForRoot(if (root == 0) .metadata else .index, mutations.items, .{ .payload_cache = .cold_sequential });
                }
            }
        }
        if (cancel) |token| try token.check();
        try destination.commitTransaction();
        updated_report.after_size = destination.activeCheckpoint().page_count * @as(u64, destination.header.page_size);
        updated_report.reclaimed_bytes = updated_report.before_size -| updated_report.after_size;
        report.* = updated_report;
    }

    fn liveRecordSize(self: *NativeFile, root: usize, key: []const u8) !?usize {
        const checkpoint = self.activeCheckpoint();
        if (self.stagedEntry(@fromBackingInt(@intCast(root)), key)) |entry| {
            return if (entry.is_delete) null else key.len + (if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len);
        }
        const page = (if (root == 2) try self.lookupDocumentIndexPage(checkpoint, key) else try self.lookupCatalogPage(checkpoint, if (root == 0) .metadata else .index, key)) orelse return null;
        const raw = try self.readPageAllocForCheckpoint(self.allocator, page, checkpoint);
        defer self.allocator.free(raw);
        if (root == 2) {
            const entry = try decodeDocumentEntry(try decodePagePayload(raw, .document));
            return if (entry.is_delete) null else entry.key.len + (if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len);
        }
        const entry = try decodeCatalogEntry(try decodePagePayload(raw, .catalog));
        return if (entry.is_delete) null else entry.key.len + (if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len);
    }

    pub fn prepareVacuum(self: *NativeFile, cancel: ?*const maintenance.CancelToken) !VacuumImage {
        if (cancel) |token| try token.check();

        const io = self.runtimeIo();

        const before_size = (try self.file.stat(io)).size;
        const previous = self.activeCheckpoint();

        const page_size: usize = @intCast(self.header.page_size);
        var random: [16]u8 = undefined;
        try io.randomSecure(&random);
        const basename = try std.fmt.allocPrint(self.allocator, ".aflite-compact-{x}", .{random});
        defer self.allocator.free(basename);
        const tmp_path = try std.fs.path.join(self.allocator, &.{ std.fs.path.dirname(self.path) orelse ".", basename });
        errdefer self.allocator.free(tmp_path);
        errdefer deleteFilePath(io, tmp_path) catch {};
        var compact_file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .read = true, .exclusive = true, .permissions = .fromMode(0o600) });
        var compact_file_open = true;
        defer if (compact_file_open) compact_file.close(io);
        try compact_file.setLength(io, page_size);

        var next_page_id: u64 = 1;
        var catalog_root_page: u64 = 0;
        var index_catalog_root_page: u64 = 0;
        var document_root_page: u64 = 0;
        var document_index_root_page: u64 = 0;
        var namespace_directory_root_page: u64 = 0;
        var free_map_root_page: u64 = 0;
        var live_bytes: u64 = 0;
        var live_record_count: usize = 0;
        var namespace_directory = NamespaceDirectory.empty;
        defer deinitNamespaceDirectory(self.allocator, &namespace_directory);

        live_record_count += try self.copyVacuumCatalogRecords(compact_file, .metadata, &next_page_id, &catalog_root_page, &live_bytes, cancel);
        live_record_count += try self.copyVacuumCatalogRecords(compact_file, .index, &next_page_id, &index_catalog_root_page, &live_bytes, cancel);
        const document_count = try self.copyVacuumDocumentRecords(compact_file, &next_page_id, &document_root_page, &document_index_root_page, &namespace_directory, &live_bytes, cancel);
        live_record_count += document_count;

        const indexed = self.header.indexed_reclamation or self.vacuum_target_indexed;
        if (document_count > 0 and !indexed) {
            var writer = NativeFile{ .allocator = self.allocator, .io_impl = undefined, .borrowed_io = io, .path = @constCast(""), .file = compact_file, .header = self.header, .page_cache_enabled = .init(false), .max_file_bytes = self.vacuum_workspace_limit };
            var pages = PageAllocator{ .file = &writer, .free_pages = &.{}, .next_page_id = next_page_id, .pack_records = true };
            defer pages.deinit();
            namespace_directory_root_page = (try writer.writeNamespaceIndex(&pages, previous, .{ .history = 0 }, &namespace_directory)).page;
            try pages.flush();
            next_page_id = pages.next_page_id;
        }

        if (self.vacuum_workspace_limit) |limit| {
            if (next_page_id >= limit / self.header.page_size) return error.LiteStorageBudgetExceeded;
        }
        if (!indexed) free_map_root_page = try appendFreeMapPageToFile(self.allocator, compact_file, io, page_size, &next_page_id, next_page_id + 1, &.{});

        var checkpoint = CheckpointSlot{
            .commit_sequence = previous.commit_sequence + 1,
            .catalog_root_page = catalog_root_page,
            .document_root_page = document_root_page,
            .index_catalog_root_page = index_catalog_root_page,
            .free_map_root_page = free_map_root_page,
            .page_count = next_page_id,
            .namespace_directory_root_page = namespace_directory_root_page,
            .document_index_root_page = document_index_root_page,
        };
        var compact_header = Header{
            .indexed_reclamation = indexed,
            .page_size = self.header.page_size,
            .active_checkpoint = 0,
            .checkpoints = .{ checkpoint, .{} },
        };

        if (indexed) {
            // One-time graph bootstrap belongs to migration/shrinking, never a
            // normal commit. Ownership follows immutable DAG edges and records.
            var writer = NativeFile{ .allocator = self.allocator, .io_impl = undefined, .borrowed_io = io, .path = @constCast(""), .file = compact_file, .header = compact_header, .page_cache_enabled = .init(false), .max_file_bytes = self.vacuum_workspace_limit };
            writer.allocator_cancel_token = cancel;
            const state = try self.allocator.create(allocator_v4.State);
            state.* = allocator_v4.State.init(self.allocator);
            writer.ledger = state;
            defer writer.invalidateLedger();
            for (1..@intCast(next_page_id)) |page| {
                if (cancel) |token| try token.check();
                try state.reserveTail(page);
            }
            var graph = try writer.indexedGraph(checkpoint, state, cancel);
            defer graph.deinit(self.allocator);
            var pages = PageAllocator{ .file = &writer, .free_pages = &.{}, .next_page_id = next_page_id, .ledger = state, .can_reuse = false };
            defer pages.deinit();
            checkpoint.free_map_root_page = try pages.allocate();
            try writer.persistAllocator(&pages, &checkpoint);
            next_page_id = pages.next_page_id;
            compact_header.checkpoints = .{ checkpoint, checkpoint };
        }

        var encoded_header: [header_size]u8 = undefined;
        encodeHeader(&encoded_header, compact_header);
        try compact_file.writePositionalAll(io, &encoded_header, 0);
        const after_size = next_page_id * @as(u64, self.header.page_size);
        try compact_file.setLength(io, after_size);
        if (!self.no_sync) try compact_file.sync(io);
        if (cancel) |token| try token.check();
        compact_file_open = false;
        return .{ .prepared = .{ .allocator = self.allocator, .io_impl = undefined, .borrowed_io = io, .path = tmp_path, .file = compact_file, .header = compact_header, .no_sync = self.no_sync, .page_cache_policy = .metadata_only, .page_cache = .{ .resource_manager = self.page_cache.resource_manager }, .max_file_bytes = self.vacuum_workspace_limit }, .report = .{
            .before_size = before_size,
            .after_size = after_size,
            .reclaimed_bytes = if (before_size > after_size) before_size - after_size else 0,
            .live_file_count = @intCast(live_record_count),
            .live_bytes = live_bytes,
        } };
    }

    pub fn copyStableSnapshotToPath(self: *NativeFile, dest_path: []const u8, replace: bool) !StableSnapshotReport {
        const io = self.runtimeIo();
        if (std.mem.eql(u8, self.path, dest_path) or try pathsReferToSameExistingFile(self.allocator, io, self.path, dest_path)) {
            return error.InvalidNativeSnapshotPath;
        }
        const dest_exists = pathExists(io, dest_path);
        if (!replace and dest_exists) return error.PathAlreadyExists;

        var dest_lock = try lockWriterPathWithIo(self.allocator, io, dest_path);
        defer dest_lock.close();

        if (!replace and !dest_exists and pathExists(io, dest_path)) return error.PathAlreadyExists;

        const checkpoint = self.activeCheckpoint();
        const snapshot_size = try checkpointPrefixSize(checkpoint, self.header.page_size);
        const source_size = (try self.file.stat(io)).size;
        if (source_size < snapshot_size) return error.TruncatedNativeSnapshotSource;

        const tmp_path = try std.fmt.allocPrint(self.allocator, "{s}.tmp-aflite-snapshot", .{dest_path});
        defer self.allocator.free(tmp_path);
        errdefer deleteFilePath(io, tmp_path) catch {};

        {
            var out_file = try createSnapshotFile(io, tmp_path);
            defer out_file.close(io);

            const chunk_size: usize = 1024 * 1024;
            const buffer_len: usize = @intCast(@min(@as(u64, chunk_size), snapshot_size));
            const buffer = try self.allocator.alloc(u8, @max(buffer_len, 1));
            defer self.allocator.free(buffer);

            var offset: u64 = 0;
            while (offset < snapshot_size) {
                const len: usize = @intCast(@min(@as(u64, buffer.len), snapshot_size - offset));
                try readExactAt(self.file, io, buffer[0..len], offset);
                try out_file.writePositionalAll(io, buffer[0..len], offset);
                offset += len;
            }
            var snapshot_header: [header_size]u8 = undefined;
            encodeHeader(&snapshot_header, self.header);
            try out_file.writePositionalAll(io, &snapshot_header, 0);
            try out_file.setLength(io, snapshot_size);
            try out_file.sync(io);
        }

        renameFilePath(io, tmp_path, dest_path) catch |err| {
            deleteFilePath(io, tmp_path) catch {};
            return err;
        };
        try fs_paths.syncDirPortable(
            io,
            std.fs.path.dirname(dest_path) orelse ".",
        );

        return .{
            .source_size = source_size,
            .snapshot_size = snapshot_size,
            .checkpoint_sequence = checkpoint.commit_sequence,
            .page_count = checkpoint.page_count,
            .tail_bytes = source_size - snapshot_size,
        };
    }

    pub fn maxPagePayloadBytes(self: *const NativeFile) usize {
        return @as(usize, @intCast(self.header.page_size)) - page_header_size;
    }

    pub fn maxValuePagePayloadBytes(self: *const NativeFile) usize {
        return self.maxPagePayloadBytes() - value_page_header_size;
    }

    fn validateCatalogMutation(self: *const NativeFile, mutation: CatalogMutation) !void {
        if (mutation.key.len > catalog_key_len_mask or mutation.value.len > std.math.maxInt(u32)) return error.RecordTooLarge;
        const fixed_len = 16 + mutation.key.len;
        if (fixed_len > self.maxPagePayloadBytes()) return error.PageTooLarge;
        if (mutation.external_value_root_page != 0) {
            if (self.transaction_header == null or mutation.is_delete or mutation.value.len != 0 or mutation.external_value_len == 0 or mutation.external_value_len > std.math.maxInt(u32) or mutation.external_value_root_page >= self.activeCheckpoint().page_count) return error.InvalidNativeValueChain;
            if (fixed_len + 8 > self.maxPagePayloadBytes()) return error.PageTooLarge;
            return;
        }
        if (mutation.is_delete) return;
        if (mutation.value.len <= self.maxPagePayloadBytes() - fixed_len) return;
        if (value_page_header_size > self.maxPagePayloadBytes()) return error.InvalidNativePageLength;
        if (fixed_len + 8 > self.maxPagePayloadBytes()) return error.PageTooLarge;
    }

    fn catalogEntryFitsInline(self: *const NativeFile, key: []const u8, value: []const u8) bool {
        const fixed_len = 16 + key.len;
        return fixed_len <= self.maxPagePayloadBytes() and value.len <= self.maxPagePayloadBytes() - fixed_len;
    }

    fn catalogEntryValueAlloc(self: *NativeFile, allocator: Allocator, entry: CatalogEntry) ![]u8 {
        return try self.catalogEntryValueAtCheckpointAlloc(allocator, entry, self.activeCheckpoint());
    }

    fn catalogEntryValueAtCheckpointAlloc(self: *NativeFile, allocator: Allocator, entry: CatalogEntry, checkpoint: CheckpointSlot) ![]u8 {
        if (entry.external_value_root_page != 0) {
            return try self.readValuePagesAtCheckpointAlloc(allocator, entry.external_value_root_page, entry.external_value_len, checkpoint);
        }
        return try allocator.dupe(u8, entry.value);
    }

    fn catalogEntryRangeAlloc(self: *NativeFile, allocator: Allocator, entry: CatalogEntry, offset: u64, len: usize, checkpoint: CheckpointSlot) ![]u8 {
        const value_len = if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len;
        if (offset > std.math.maxInt(usize)) return error.EndOfStream;
        const start: usize = @intCast(offset);
        if (start > value_len or value_len - start < len) return error.EndOfStream;
        if (entry.external_value_root_page != 0) {
            return try self.readValuePagesRangeAlloc(allocator, entry.external_value_root_page, value_len, start, len, checkpoint);
        }
        return try allocator.dupe(u8, entry.value[start..][0..len]);
    }

    fn validateDocumentMutation(self: *const NativeFile, mutation: DocumentMutation) !void {
        if (mutation.key.len > std.math.maxInt(u32) or mutation.value.len > std.math.maxInt(u32)) return error.RecordTooLarge;
        const fixed_len = 28 + mutation.key.len;
        if (fixed_len > self.maxPagePayloadBytes()) return error.PageTooLarge;
        if (mutation.external_value_root_page != 0) {
            if (self.transaction_header == null or mutation.is_delete or mutation.value.len != 0 or mutation.external_value_len == 0 or mutation.external_value_len > std.math.maxInt(u32) or mutation.external_value_root_page >= self.activeCheckpoint().page_count) return error.InvalidNativeValueChain;
            if (fixed_len + 8 > self.maxPagePayloadBytes()) return error.PageTooLarge;
            return;
        }
        if (mutation.is_delete) return;
        if (mutation.value.len <= self.maxPagePayloadBytes() - fixed_len) return;
        if (value_page_header_size > self.maxPagePayloadBytes()) return error.InvalidNativePageLength;
        if (fixed_len + 8 > self.maxPagePayloadBytes()) return error.PageTooLarge;
    }

    fn documentEntryFitsInline(self: *const NativeFile, key: []const u8, value: []const u8) bool {
        const fixed_len = 28 + key.len;
        return fixed_len <= self.maxPagePayloadBytes() and value.len <= self.maxPagePayloadBytes() - fixed_len;
    }

    fn documentEntryValueAlloc(self: *NativeFile, allocator: Allocator, entry: DocumentEntry) ![]u8 {
        if (entry.external_value_root_page != 0) {
            return try self.readValuePagesAlloc(allocator, entry.external_value_root_page, entry.external_value_len);
        }
        return try allocator.dupe(u8, entry.value);
    }

    fn readPagePayloadByKindAlloc(self: *NativeFile, allocator: Allocator, page_id: u64, kind: PageKind) ![]u8 {
        return try self.readPagePayloadByKindAllocForCheckpoint(allocator, page_id, kind, self.activeCheckpoint());
    }

    fn readPagePayloadByKindAllocForCheckpoint(
        self: *NativeFile,
        allocator: Allocator,
        page_id: u64,
        kind: PageKind,
        checkpoint: CheckpointSlot,
    ) ![]u8 {
        const page = try self.readPageAllocForCheckpoint(allocator, page_id, checkpoint);
        defer allocator.free(page);
        return try decodePagePayloadAlloc(allocator, page, kind);
    }

    const ReachablePageSet = std.AutoHashMapUnmanaged(u64, void);

    fn markReachablePage(self: *NativeFile, reachable_pages: *ReachablePageSet, page_id: u64, page_count: u64) !void {
        const physical = physicalPage(page_id);
        if (physical == 0 or physical >= page_count) return error.InvalidPageId;
        const entry = try reachable_pages.getOrPut(self.allocator, page_id);
        if (entry.found_existing) return error.InvalidNativePageChain;
        // Multiple records may share a physical page. Keep both identities:
        // record addresses detect cycles; physical addresses protect reuse.
        if (physical != page_id) try reachable_pages.put(self.allocator, physical, {});
    }

    fn countReachableChainPages(self: *NativeFile, kind: PageKind, root_page_id: u64, reachable_pages: *ReachablePageSet) !u64 {
        return try self.countReachableChainPagesWithCancel(kind, root_page_id, reachable_pages, null);
    }

    fn countReachableChainPagesWithCancel(self: *NativeFile, kind: PageKind, root_page_id: u64, reachable_pages: *ReachablePageSet, cancel: ?*const maintenance.CancelToken) !u64 {
        return try self.countReachableChainPagesForCheckpoint(kind, root_page_id, self.activeCheckpoint(), reachable_pages, cancel);
    }

    fn countReachableChainPagesForCheckpoint(
        self: *NativeFile,
        kind: PageKind,
        root_page_id: u64,
        checkpoint: CheckpointSlot,
        reachable_pages: *ReachablePageSet,
        cancel: ?*const maintenance.CancelToken,
    ) !u64 {
        var records = RecordPageReader{};
        defer records.deinit(self.allocator);
        var seen_catalog_keys = std.StringHashMapUnmanaged(void).empty;
        defer {
            var it = seen_catalog_keys.iterator();
            while (it.next()) |entry| self.allocator.free(entry.key_ptr.*);
            seen_catalog_keys.deinit(self.allocator);
        }

        var count: u64 = 0;
        var page_id = root_page_id;
        var catalog_index: u64 = 0;
        var has_catalog_index = false;
        if (kind == .catalog and root_page_id != 0) {
            const roots = try self.readCatalogRoots(root_page_id, checkpoint);
            if (roots.indexed) {
                try self.markReachablePage(reachable_pages, root_page_id, checkpoint.page_count);
                catalog_index = roots.index;
                has_catalog_index = true;
                page_id = roots.history;
            }
        }
        var coverage = HistoryIndexCoverage{};
        defer coverage.deinit(self.allocator);
        if (has_catalog_index) try coverage.initIndex(self, checkpoint, catalog_index, cancel);
        const use_link_cache = !has_catalog_index and self.page_cache_enabled.load(.monotonic) and self.page_cache_bypass.load(.monotonic) == 0;
        while (page_id != 0) {
            if (cancel) |token| try token.check();
            try self.markReachablePage(reachable_pages, page_id, checkpoint.page_count);

            if (use_link_cache) blk: {
                const links = (try self.page_cache.getLinksCopy(self.allocator, page_id)) orelse break :blk;
                defer if (links.key) |key| self.allocator.free(key);
                if (links.kind != kind) return error.UnexpectedNativePageKind;
                switch (kind) {
                    .catalog => {
                        const entry_key = links.key orelse &[_]u8{};
                        const seen = seen_catalog_keys.contains(entry_key);
                        if (!seen) {
                            const owned_key = try self.allocator.dupe(u8, entry_key);
                            errdefer self.allocator.free(owned_key);
                            try seen_catalog_keys.put(self.allocator, owned_key, {});
                        }
                        if (!seen and links.external_value_root_page != 0) {
                            try self.validateReachableValuePages(links.external_value_root_page, links.external_value_len, checkpoint, reachable_pages);
                        }
                    },
                    .document => {
                        if (links.external_value_root_page != 0) {
                            try self.validateReachableValuePages(links.external_value_root_page, links.external_value_len, checkpoint, reachable_pages);
                        }
                    },
                    .data, .value, .free_map, .document_index, .value_extent, .catalog_index, .record_bundle, .allocator, .key => return error.UnexpectedNativePageKind,
                }
                page_id = links.link_page;
                count += 1;
                if (count > recordWalkLimit(checkpoint, self.header.page_size)) return error.InvalidNativePageChain;
                continue;
            }

            const payload = try records.read(self, self.allocator, checkpoint, page_id, kind);
            page_id = switch (kind) {
                .catalog => blk: {
                    const entry = try decodeCatalogEntry(payload);
                    const seen = if (has_catalog_index)
                        !(try coverage.observe(self, checkpoint, .catalog, page_id, entry.key, entry.is_delete))
                    else
                        seen_catalog_keys.contains(entry.key);
                    if (!seen and !has_catalog_index) {
                        const owned_key = try self.allocator.dupe(u8, entry.key);
                        errdefer self.allocator.free(owned_key);
                        try seen_catalog_keys.put(self.allocator, owned_key, {});
                    }
                    if (!seen and entry.external_value_root_page != 0) {
                        try self.validateReachableValuePages(entry.external_value_root_page, entry.external_value_len, checkpoint, reachable_pages);
                    }
                    if (use_link_cache) self.cachePageLinks(page_id, .catalog, payload);
                    break :blk entry.previous_page;
                },
                .document => blk: {
                    const entry = try decodeDocumentEntry(payload);
                    if (entry.external_value_root_page != 0) {
                        try self.validateReachableValuePages(entry.external_value_root_page, entry.external_value_len, checkpoint, reachable_pages);
                    }
                    if (use_link_cache) self.cachePageLinks(page_id, .document, payload);
                    break :blk entry.previous_page;
                },
                .data, .value, .free_map, .document_index, .value_extent, .catalog_index, .record_bundle, .allocator, .key => return error.UnexpectedNativePageKind,
            };
            count += 1;
            if (count > recordWalkLimit(checkpoint, self.header.page_size)) return error.InvalidNativePageChain;
        }
        if (coverage.unresolved != 0) return error.InvalidDocumentIndex;
        if (catalog_index != 0) {
            var indexed = checkpoint;
            indexed.document_index_root_page = catalog_index;
            _ = try self.collectDocumentIndexPages(indexed, reachable_pages, false, true, cancel);
            try self.validateCatalogIndexEntries(checkpoint, catalog_index, reachable_pages);
        }
        return count;
    }

    const extent_fanout = 64;
    const extent_header_size = 16;
    const extent_magic = "AFEXT003";
    const ExtentRef = struct { page: u64, len: u64, height: u8 = 0 };
    const ExtentNode = struct {
        height: u8,
        count: usize,
        children: [extent_fanout]ExtentRef = undefined,
    };

    fn decodeExtentNode(payload: []const u8, expected_len: u64) !ExtentNode {
        if (payload.len < extent_header_size or !std.mem.eql(u8, payload[0..8], extent_magic)) return error.InvalidNativeValueChain;
        const height = payload[8];
        const count = std.mem.readInt(u16, payload[10..12], .little);
        if (height == 0 or height > 63 or count == 0 or count > extent_fanout or payload.len != extent_header_size + @as(usize, count) * 16)
            return error.InvalidNativeValueChain;
        var node = ExtentNode{ .height = height, .count = count };
        var total: u64 = 0;
        for (node.children[0..count], 0..) |*child, i| {
            const bytes = payload[extent_header_size + i * 16 ..][0..16];
            child.* = .{
                .page = std.mem.readInt(u64, bytes[0..8], .little),
                .len = std.mem.readInt(u64, bytes[8..16], .little),
                .height = height - 1,
            };
            if (child.page == 0 or child.len == 0) return error.InvalidNativeValueChain;
            total = std.math.add(u64, total, child.len) catch return error.InvalidNativeValueChain;
        }
        if (total != expected_len) return error.InvalidNativeValueChain;
        return node;
    }

    fn writeExtentNode(self: *NativeFile, pages: *PageAllocator, batch: *PageWriteBatch, height: u8, children: []const ExtentRef) !ExtentRef {
        _ = self;
        if (height == 0 or height > 63 or children.len == 0 or children.len > extent_fanout) return error.InvalidNativeValueChain;
        var payload: [extent_header_size + extent_fanout * 16]u8 = @splat(0);
        @memcpy(payload[0..8], extent_magic);
        payload[8] = height;
        std.mem.writeInt(u16, payload[10..12], @intCast(children.len), .little);
        var len: u64 = 0;
        for (children, 0..) |child, i| {
            if (child.height + 1 != height or child.len == 0) return error.InvalidNativeValueChain;
            len = try std.math.add(u64, len, child.len);
            const bytes = payload[extent_header_size + i * 16 ..][0..16];
            std.mem.writeInt(u64, bytes[0..8], child.page, .little);
            std.mem.writeInt(u64, bytes[8..16], child.len, .little);
        }
        const page = try pages.allocate();
        try pages.noteValue(page, len);
        for (children) |child| try pages.retainValue(child.page);
        try batch.appendPage(page, .value_extent, payload[0 .. extent_header_size + children.len * 16]);
        return .{ .page = page, .len = len, .height = height };
    }

    /// Stream a packed tree through the same bounded frontier used by appends.
    /// Neither initial writes nor vacuum need an array of every leaf reference.
    fn writeValueTree(self: *NativeFile, pages: *PageAllocator, value: []const u8, options: WriteOptions) !ExtentRef {
        if (value.len == 0) return error.InvalidNativeValueChain;
        var builder = ExtentAppender{ .file = self, .pages = pages, .tail = undefined, .batch = .{ .file = self, .options = options } };
        defer builder.deinit();
        var offset: usize = 0;
        while (offset < value.len) {
            const end = @min(value.len, offset + self.maxValuePagePayloadBytes());
            try builder.pushValue(value[offset..end]);
            offset = end;
        }
        return try builder.finish();
    }

    fn writeCatalogValue(self: *NativeFile, pages: *PageAllocator, value: []const u8, options: WriteOptions) !u64 {
        return (try self.writeValueTree(pages, value, options)).page;
    }

    fn valueTreeRoot(self: *NativeFile, root: u64, len: usize, checkpoint: CheckpointSlot) !?ExtentRef {
        var scratch: [65536]u8 = undefined;
        const raw = try self.readPageInto(root, checkpoint, &scratch);
        if (raw[4] == @backingInt(PageKind.value_extent)) {
            const payload = try decodePagePayload(raw, .value_extent);
            const node = try decodeExtentNode(payload, len);
            return .{ .page = root, .len = len, .height = node.height };
        }
        const payload = try decodePagePayload(raw, .value);
        const leaf = try decodeValuePage(payload);
        if (leaf.next_page != 0) return null; // document/namespace linked value
        if (leaf.chunk.len != len or len == 0) return error.InvalidNativeValueChain;
        return .{ .page = root, .len = len };
    }

    /// A bounded right frontier: one unfinished node at each height. Sealed
    /// suffix nodes are written once and fed into the next level; existing
    /// full subtrees remain shared. Memory is O(fanout * height), independent
    /// of suffix length, and work is O(new leaves + height).
    const ExtentAppender = struct {
        const Level = struct {
            children: [extent_fanout]ExtentRef = undefined,
            count: usize = 0,
            original: ?ExtentRef = null,
            original_count: usize = 0,
            original_last: ExtentRef = undefined,
            unchanged: bool = true,
        };
        file: *NativeFile,
        pages: *PageAllocator,
        levels: std.ArrayListUnmanaged(Level) = .empty,
        tail: ExtentRef,
        batch: PageWriteBatch,

        fn pushValue(self: *ExtentAppender, value: []const u8) !void {
            const page = try self.pages.allocate();
            try self.pages.noteValue(page, value.len);
            try self.batch.appendValue(page, 0, value);
            try self.push(.{ .page = page, .len = value.len });
        }

        fn init(file: *NativeFile, pages: *PageAllocator, root: ExtentRef) !ExtentAppender {
            var result = ExtentAppender{ .file = file, .pages = pages, .tail = root, .batch = .{ .file = file } };
            errdefer result.deinit();
            var checkpoint = file.activeCheckpoint();
            checkpoint.page_count = pages.next_page_id;
            while (result.tail.height > 0) {
                const ref = result.tail;
                const payload = try file.readPagePayloadByKindAllocForCheckpoint(file.allocator, ref.page, .value_extent, checkpoint);
                defer file.allocator.free(payload);
                const node = try decodeExtentNode(payload, ref.len);
                if (node.height != ref.height) return error.InvalidNativeValueChain;
                try result.ensureLevel(ref.height);
                result.levels.items[ref.height] = .{
                    .children = node.children,
                    .count = node.count - 1,
                    .original = ref,
                    .original_count = node.count,
                    .original_last = node.children[node.count - 1],
                };
                result.tail = node.children[node.count - 1];
            }
            return result;
        }

        pub fn deinit(self: *ExtentAppender) void {
            self.levels.deinit(self.file.allocator);
        }

        fn ensureLevel(self: *ExtentAppender, height: usize) !void {
            if (height >= 64) return error.InvalidNativeValueChain;
            if (height < self.levels.items.len) return;
            const before = self.levels.items.len;
            try self.levels.resize(self.file.allocator, height + 1);
            @memset(self.levels.items[before..], .{});
        }

        fn push(self: *ExtentAppender, child: ExtentRef) anyerror!void {
            if (child.height >= 63) return error.InvalidNativeValueChain;
            const height = child.height + 1;
            try self.ensureLevel(height);
            const level = &self.levels.items[height];
            if (level.original == null or level.count + 1 != level.original_count or
                !std.meta.eql(child, level.original_last)) level.unchanged = false;
            level.children[level.count] = child;
            level.count += 1;
            if (level.count == extent_fanout) {
                const parent = try self.seal(height);
                try self.push(parent);
            }
        }

        fn seal(self: *ExtentAppender, height: u8) !ExtentRef {
            const level = &self.levels.items[height];
            const ref = if (level.unchanged and level.original != null and level.count == level.original_count)
                level.original.?
            else
                try self.file.writeExtentNode(self.pages, &self.batch, height, level.children[0..level.count]);
            level.* = .{};
            return ref;
        }

        fn finish(self: *ExtentAppender) !ExtentRef {
            const root = try self.finishTree();
            // Callers may read newly built trees, then publish their roots.
            // Never return a root while any of its pages remain buffered.
            try self.batch.flush();
            return root;
        }

        fn finishTree(self: *ExtentAppender) !ExtentRef {
            var height: usize = 1;
            while (height < self.levels.items.len) : (height += 1) {
                const level = &self.levels.items[height];
                if (level.count == 0) continue;
                var higher_pending = false;
                for (self.levels.items[height + 1 ..]) |higher| {
                    if (higher.count != 0) higher_pending = true;
                }
                // Do not introduce a unary root above the final subtree.
                if (!higher_pending and level.count == 1) return level.children[0];
                const ref = try self.seal(@intCast(height));
                if (!higher_pending) return ref;
                try self.push(ref);
            }
            return error.InvalidNativeValueChain;
        }
    };

    /// Transaction-owned append state: retain the partial leaf and unfinished
    /// right frontier, stream full leaves once, and seal ancestors only at a
    /// visibility barrier. Abort/overwrite discard buffers without flushing.
    const CatalogAppendState = struct {
        builder: ExtentAppender,
        tail: []u8,
        used: usize = 0,
        len: usize,

        fn create(file: *NativeFile, pages: *PageAllocator, entry: CatalogEntry) !*CatalogAppendState {
            const self = try file.allocator.create(CatalogAppendState);
            errdefer file.allocator.destroy(self);
            self.* = .{
                .builder = .{ .file = file, .pages = pages, .tail = undefined, .batch = .{ .file = file } },
                .tail = try file.allocator.alloc(u8, file.maxValuePagePayloadBytes()),
                .len = if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len,
            };
            errdefer self.builder.deinit();
            errdefer file.allocator.free(self.tail);
            if (entry.external_value_root_page == 0) {
                @memcpy(self.tail[0..entry.value.len], entry.value);
                self.used = entry.value.len;
            } else {
                var checkpoint = file.activeCheckpoint();
                checkpoint.page_count = pages.next_page_id;
                const root = (try file.valueTreeRoot(entry.external_value_root_page, entry.external_value_len, checkpoint)) orelse return error.InvalidNativeValueChain;
                self.builder = try ExtentAppender.init(file, pages, root);
                const payload = try file.readPagePayloadByKindAllocForCheckpoint(file.allocator, self.builder.tail.page, .value, checkpoint);
                defer file.allocator.free(payload);
                const leaf = try decodeValuePage(payload);
                if (leaf.next_page != 0 or leaf.chunk.len != self.builder.tail.len) return error.InvalidNativeValueChain;
                if (leaf.chunk.len == file.maxValuePagePayloadBytes()) {
                    try self.builder.push(self.builder.tail);
                } else {
                    @memcpy(self.tail[0..leaf.chunk.len], leaf.chunk);
                    self.used = leaf.chunk.len;
                }
            }
            return self;
        }

        fn append(self: *CatalogAppendState, suffix: []const u8) !void {
            const len = try std.math.add(usize, self.len, suffix.len);
            if (len > std.math.maxInt(u32)) return error.RecordTooLarge;
            const capacity = self.builder.file.maxValuePagePayloadBytes();
            var offset: usize = 0;
            while (offset < suffix.len) {
                const n = @min(capacity - self.used, suffix.len - offset);
                @memcpy(self.tail[self.used..][0..n], suffix[offset..][0..n]);
                self.used += n;
                offset += n;
                if (self.used == capacity) {
                    try self.builder.pushValue(self.tail[0..self.used]);
                    self.used = 0;
                }
            }
            self.len = len;
        }

        fn finish(self: *CatalogAppendState) !ExtentRef {
            if (self.used != 0) {
                try self.builder.pushValue(self.tail[0..self.used]);
                self.used = 0;
            }
            return try self.builder.finish();
        }

        fn destroy(self: *CatalogAppendState) void {
            const allocator = self.builder.file.allocator;
            self.builder.deinit();
            allocator.free(self.tail);
            allocator.destroy(self);
        }
    };

    fn appendCatalogValueTree(self: *NativeFile, pages: *PageAllocator, entry: CatalogEntry, suffix: []const u8) !u64 {
        const root = if (entry.external_value_root_page != 0)
            (try self.valueTreeRoot(entry.external_value_root_page, entry.external_value_len, self.activeCheckpoint())) orelse return error.InvalidNativeValueChain
        else if (entry.value.len > 0)
            try self.writeValueTree(pages, entry.value, .{})
        else
            return (try self.writeValueTree(pages, suffix, .{})).page;
        if (suffix.len == 0) return root.page;
        var appender = try ExtentAppender.init(self, pages, root);
        defer appender.deinit();
        var checkpoint = self.activeCheckpoint();
        checkpoint.page_count = pages.next_page_id;
        const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, appender.tail.page, .value, checkpoint);
        defer self.allocator.free(payload);
        const tail = try decodeValuePage(payload);
        if (tail.next_page != 0 or tail.chunk.len != appender.tail.len) return error.InvalidNativeValueChain;
        const take = @min(self.maxValuePagePayloadBytes() - tail.chunk.len, suffix.len);
        if (take == 0) {
            try appender.push(appender.tail);
        } else {
            const combined = try self.allocator.alloc(u8, tail.chunk.len + take);
            defer self.allocator.free(combined);
            @memcpy(combined[0..tail.chunk.len], tail.chunk);
            @memcpy(combined[tail.chunk.len..], suffix[0..take]);
            try appender.pushValue(combined);
        }
        var offset = take;
        while (offset < suffix.len) {
            const end = @min(suffix.len, offset + self.maxValuePagePayloadBytes());
            try appender.pushValue(suffix[offset..end]);
            offset = end;
        }
        return (try appender.finish()).page;
    }

    fn readExtentRange(self: *NativeFile, ref: ExtentRef, checkpoint: CheckpointSlot, start: usize, out: []u8) !void {
        var reader = ValuePageReader{};
        defer reader.deinit(self);
        return self.readExtentRangeBuffered(ref, checkpoint, start, out, &reader, 1);
    }

    fn readExtentRangeBuffered(self: *NativeFile, ref: ExtentRef, checkpoint: CheckpointSlot, start: usize, out: []u8, reader: *ValuePageReader, ahead: usize) !void {
        if (start > ref.len or out.len > ref.len - start) return error.InvalidNativeValueChain;
        if (ref.height == 0) {
            const payload = try decodePagePayload(try reader.read(self, checkpoint, ref.page, ahead), .value);
            const leaf = try decodeValuePage(payload);
            if (leaf.next_page != 0 or leaf.chunk.len != ref.len) return error.InvalidNativeValueChain;
            @memcpy(out, leaf.chunk[start..][0..out.len]);
            return;
        }
        const payload = try decodePagePayload(try reader.read(self, checkpoint, ref.page, 1), .value_extent);
        const node = try decodeExtentNode(payload, ref.len);
        if (node.height != ref.height) return error.InvalidNativeValueChain;
        var offset: u64 = 0;
        var written: usize = 0;
        for (node.children[0..node.count], 0..) |child, i| {
            const end = offset + child.len;
            if (end > start and offset < start + out.len) {
                const from: usize = @intCast(@max(offset, start) - offset);
                const count: usize = @intCast(@min(end, start + out.len) - @max(offset, start));
                const child_ahead = if (child.height == 0) extentLeafReadAhead(node.children[0..node.count], i, start + out.len - offset) else 1;
                try self.readExtentRangeBuffered(child, checkpoint, from, out[written..][0..count], reader, child_ahead);
                written += count;
            }
            offset = end;
            if (written == out.len) break;
        }
        if (written != out.len) return error.InvalidNativeValueChain;
    }

    fn validateExtent(self: *NativeFile, ref: ExtentRef, checkpoint: CheckpointSlot, reachable: *ReachablePageSet) !void {
        try self.markReachablePage(reachable, ref.page, checkpoint.page_count);
        if (ref.height == 0) {
            const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, ref.page, .value, checkpoint);
            defer self.allocator.free(payload);
            const leaf = try decodeValuePage(payload);
            if (leaf.next_page != 0 or leaf.chunk.len != ref.len) return error.InvalidNativeValueChain;
            return;
        }
        const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, ref.page, .value_extent, checkpoint);
        defer self.allocator.free(payload);
        const node = try decodeExtentNode(payload, ref.len);
        if (node.height != ref.height) return error.InvalidNativeValueChain;
        for (node.children[0..node.count]) |child| try self.validateExtent(child, checkpoint, reachable);
    }

    fn writeValuePagesAllocated(self: *NativeFile, page_allocator: *PageAllocator, value: []const u8) !u64 {
        if (value.len == 0) return error.InvalidNativeValueChain;
        const chunk_size = self.maxValuePagePayloadBytes();
        if (chunk_size == 0) return error.InvalidNativePageLength;
        var batch = PageWriteBatch{ .file = self };
        var page = try page_allocator.allocate();
        const root = page;
        var offset: usize = 0;
        while (offset < value.len) {
            const len = @min(chunk_size, value.len - offset);
            // One-page lookahead preserves arbitrary free-page allocation
            // order without retaining an array of every page in the chain.
            const next = if (offset + len < value.len) try page_allocator.allocate() else 0;
            try page_allocator.noteValue(page, value.len - offset);
            if (next != 0) try page_allocator.retainValue(next);
            try batch.appendValue(page, next, value[offset..][0..len]);
            page = next;
            offset += len;
        }
        try batch.flush();
        return root;
    }

    fn writeValuePageChunk(self: *NativeFile, page_ids: []const u64, page_index: usize, chunk: []const u8, options: WriteOptions) !void {
        if (chunk.len == 0 or chunk.len > self.maxValuePagePayloadBytes()) return error.InvalidNativeValueChain;
        const next_page_id = if (page_index + 1 < page_ids.len) page_ids[page_index + 1] else 0;
        var batch = PageWriteBatch{ .file = self, .options = options };
        try batch.appendValue(page_ids[page_index], next_page_id, chunk);
        try batch.flush();
    }

    fn pageAllocatorFromFreeMap(self: *NativeFile, checkpoint: CheckpointSlot) !PageAllocator {
        if (self.header.indexed_reclamation) {
            if (self.checkpoint_publication_uncertain) return error.OutcomeUnknown;
            const state = try self.loadLedger(checkpoint);
            state.incremental = self.reserve_retirement_capacity or self.incremental_checkpoints;
            errdefer self.invalidateLedger();
            if (!self.free_pages_verified and checkpoint.free_map_root_page != 0) {
                _ = try self.validateIndexedAllocator(checkpoint, state, self.allocator_cancel_token);
                self.free_pages_verified = true;
            }
            const lock = acquireDataRewriteLock(self.runtimeIo(), self.path) catch |err| switch (err) {
                error.WouldBlock => null,
                else => return err,
            };
            errdefer if (lock) |held| held.file.close(self.runtimeIo());
            _ = try self.drainRetirement(state, checkpoint, self.retirement_work_pages);
            return .{ .file = self, .free_pages = &.{}, .next_page_id = checkpoint.page_count, .ledger = state, .can_reuse = lock != null, .data_lock_file = if (lock) |held| held.file else null };
        }
        var free_pages = try self.readFreePagesAlloc(checkpoint);
        errdefer self.allocator.free(free_pages);
        if (!self.free_pages_verified) {
            try self.validateFreePagesSafeForCheckpointSlots(free_pages);
            self.free_pages_verified = true;
        }
        var data_lock_file: ?std.Io.File = null;
        if (free_pages.len > 0) {
            const data_lock = acquireDataRewriteLock(self.runtimeIo(), self.path) catch |err| switch (err) {
                error.WouldBlock => blk: {
                    self.allocator.free(free_pages);
                    free_pages = try self.allocator.alloc(u64, 0);
                    break :blk null;
                },
                else => return err,
            };
            if (data_lock) |lock| {
                data_lock_file = lock.file;
            }
        }
        return .{
            .file = self,
            .free_pages = free_pages,
            .next_page_id = checkpoint.page_count,
            .data_lock_file = data_lock_file,
        };
    }

    fn readFreePagesAlloc(self: *NativeFile, checkpoint: CheckpointSlot) ![]u64 {
        if (checkpoint.free_map_root_page == 0) return try self.allocator.alloc(u64, 0);

        const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, checkpoint.free_map_root_page, .free_map, checkpoint);
        defer self.allocator.free(payload);
        const free_map = try decodeFreeMapAlloc(self.allocator, payload, checkpoint.page_count);
        return free_map.free_pages;
    }

    fn writeFreeMapPage(self: *NativeFile, page_id: u64, covered_page_count: u64, free_pages: []const u64) !void {
        const payload = try encodeFreeMapAlloc(self.allocator, self.header.page_size, covered_page_count, free_pages);
        defer self.allocator.free(payload);
        try self.writePage(page_id, .free_map, payload);
    }

    fn computeFreePagesForPublishedCheckpoint(self: *NativeFile, next: CheckpointSlot, previous: CheckpointSlot) ![]u64 {
        var reachable_pages = std.AutoHashMapUnmanaged(u64, void){};
        defer reachable_pages.deinit(self.allocator);

        try self.collectCheckpointReachablePages(next, &reachable_pages);
        if (validCheckpointSlot(previous)) {
            try self.collectCheckpointReachablePages(previous, &reachable_pages);
        }

        var free_pages = std.ArrayListUnmanaged(u64).empty;
        errdefer free_pages.deinit(self.allocator);

        const max_entries = maxFreeMapEntries(self.header.page_size);
        var page_id: u64 = 1;
        while (page_id < next.page_count and free_pages.items.len < max_entries) : (page_id += 1) {
            if (!reachable_pages.contains(page_id)) {
                try free_pages.append(self.allocator, page_id);
            }
        }

        return try free_pages.toOwnedSlice(self.allocator);
    }

    fn collectCheckpointReachablePages(self: *NativeFile, checkpoint: CheckpointSlot, out: *ReachablePageSet) !void {
        var checkpoint_pages = std.AutoHashMapUnmanaged(u64, void){};
        defer checkpoint_pages.deinit(self.allocator);

        _ = try self.countReachableChainPagesForCheckpoint(.catalog, checkpoint.catalog_root_page, checkpoint, &checkpoint_pages, null);
        _ = try self.countReachableChainPagesForCheckpoint(.catalog, checkpoint.index_catalog_root_page, checkpoint, &checkpoint_pages, null);
        _ = try self.countReachableChainPagesForCheckpoint(.catalog, checkpoint.namespace_directory_root_page, checkpoint, &checkpoint_pages, null);
        _ = try self.countReachableChainPagesForCheckpoint(.document, checkpoint.document_root_page, checkpoint, &checkpoint_pages, null);
        _ = try self.collectDocumentIndexPages(checkpoint, &checkpoint_pages, false, true, null);
        if (checkpoint.free_map_root_page != 0) {
            try self.markReachablePage(&checkpoint_pages, checkpoint.free_map_root_page, checkpoint.page_count);
        }

        var it = checkpoint_pages.iterator();
        while (it.next()) |entry| {
            try out.put(self.allocator, entry.key_ptr.*, {});
        }
    }

    fn collectDocumentIndexPages(
        self: *NativeFile,
        checkpoint: CheckpointSlot,
        reachable_pages: *ReachablePageSet,
        validate_documents: bool,
        validate_key_references: bool,
        cancel: ?*const maintenance.CancelToken,
    ) !u64 {
        if (checkpoint.document_index_root_page == 0) return 0;
        var records = RecordPageReader{};
        defer records.deinit(self.allocator);
        return try self.collectDocumentIndexSubtree(
            checkpoint,
            checkpoint.document_index_root_page,
            null,
            null,
            reachable_pages,
            validate_documents,
            validate_key_references,
            cancel,
            0,
            &records,
        );
    }

    fn collectDocumentIndexSubtree(
        self: *NativeFile,
        checkpoint: CheckpointSlot,
        page_id: u64,
        lower: ?[]const u8,
        upper: ?[]const u8,
        reachable_pages: *ReachablePageSet,
        validate_documents: bool,
        validate_key_references: bool,
        cancel: ?*const maintenance.CancelToken,
        depth: usize,
        records: *RecordPageReader,
    ) !u64 {
        if (depth > 64) return error.InvalidDocumentIndex;
        if (cancel) |token| try token.check();
        try self.markReachablePage(reachable_pages, page_id, checkpoint.page_count);
        var node = try self.readDocumentIndexNode(page_id, checkpoint);
        defer node.deinit(self.allocator);
        if (validate_key_references) for (node.key_pages.?) |page| {
            if (page != 0 and !reachable_pages.contains(page)) return error.InvalidDocumentIndex;
        };
        for (node.keys, 0..) |key, i| {
            if (i > 0 and std.mem.order(u8, node.keys[i - 1], key) != .lt) return error.InvalidDocumentIndex;
            if (lower) |bound| if (std.mem.order(u8, key, bound) == .lt) return error.InvalidDocumentIndex;
            if (upper) |bound| if (std.mem.order(u8, key, bound) != .lt) return error.InvalidDocumentIndex;
        }

        switch (node.kind) {
            .leaf => {
                if (validate_documents) {
                    for (node.keys, node.pointers) |key, document_page_id| {
                        if (!reachable_pages.contains(document_page_id)) return error.InvalidDocumentIndex;
                        const payload = try records.read(self, self.allocator, checkpoint, document_page_id, .document);
                        const entry = try decodeDocumentEntry(payload);
                        if (!std.mem.eql(u8, key, entry.key)) return error.InvalidDocumentIndex;
                    }
                }
                return 1;
            },
            .internal => {
                var count: u64 = 1;
                for (node.pointers, 0..) |child, i| {
                    if (physicalPage(child) == 0 or physicalPage(child) >= checkpoint.page_count) return error.InvalidPageId;
                    count += try self.collectDocumentIndexSubtree(
                        checkpoint,
                        child,
                        if (i == 0) lower else node.keys[i - 1],
                        if (i == node.keys.len) upper else node.keys[i],
                        reachable_pages,
                        validate_documents,
                        validate_key_references,
                        cancel,
                        depth + 1,
                        records,
                    );
                }
                return count;
            },
        }
    }

    /// Proves coverage in one index scan and one newest-first history scan.
    fn validateDocumentIndexCoverage(self: *NativeFile, checkpoint: CheckpointSlot, cancel: ?*const maintenance.CancelToken) !void {
        var coverage = HistoryIndexCoverage{};
        defer coverage.deinit(self.allocator);
        try coverage.initIndex(self, checkpoint, checkpoint.document_index_root_page, cancel);
        var records = RecordPageReader{};
        defer records.deinit(self.allocator);
        var page_id = checkpoint.document_root_page;
        var walked: u64 = 0;
        while (page_id != 0) {
            if (cancel) |token| try token.check();
            const payload = try records.read(self, self.allocator, checkpoint, page_id, .document);
            const entry = try decodeDocumentEntry(payload);
            _ = try coverage.observe(self, checkpoint, .document, page_id, entry.key, entry.is_delete);
            page_id = entry.previous_page;
            walked += 1;
            if (walked > recordWalkLimit(checkpoint, self.header.page_size)) return error.InvalidNativePageChain;
        }
        if (coverage.unresolved != 0) return error.InvalidDocumentIndex;
    }

    fn collectAllValidCheckpointReachablePages(self: *NativeFile, out: *ReachablePageSet) !void {
        const file_size = (try self.file.stat(self.runtimeIo())).size;
        for (self.header.checkpoints) |slot| {
            if (!validCheckpointSlot(slot)) continue;
            const expected_size = checkpointPrefixSize(slot, self.header.page_size) catch continue;
            if (expected_size > file_size) continue;
            try self.collectCheckpointReachablePages(slot, out);
        }
    }

    fn validateFreePagesSafeForCheckpointSlots(self: *NativeFile, free_pages: []const u64) !void {
        // Append-only writes have nothing to reclaim. Walking both checkpoint
        // graphs here makes each small catalog write proportional to history.
        // Nonempty free maps still require the full protected-page proof.
        if (free_pages.len == 0) return;
        var protected_pages = std.AutoHashMapUnmanaged(u64, void){};
        defer protected_pages.deinit(self.allocator);

        try self.collectAllValidCheckpointReachablePages(&protected_pages);
        for (free_pages) |page_id| {
            if (protected_pages.contains(page_id)) return error.InvalidNativeFreeMap;
        }
    }

    fn readValuePagesAlloc(self: *NativeFile, allocator: Allocator, root_page_id: u64, value_len: usize) ![]u8 {
        return try self.readValuePagesAtCheckpointAlloc(allocator, root_page_id, value_len, self.activeCheckpoint());
    }

    fn readValuePagesAtCheckpointAlloc(self: *NativeFile, allocator: Allocator, root_page_id: u64, value_len: usize, checkpoint: CheckpointSlot) ![]u8 {
        var cursor = try ValueChunkCursor.initAtCheckpoint(self, root_page_id, value_len, checkpoint);
        defer cursor.deinit();
        const out = try allocator.alloc(u8, value_len);
        errdefer allocator.free(out);
        var offset: usize = 0;
        while (try cursor.next(null)) |chunk| {
            @memcpy(out[offset..][0..chunk.len], chunk);
            offset += chunk.len;
        }
        if (offset != value_len) return error.InvalidNativeValueChain;
        return out;
    }

    fn readValuePagesRangeAlloc(
        self: *NativeFile,
        allocator: Allocator,
        root_page_id: u64,
        value_len: usize,
        range_start: usize,
        range_len: usize,
        checkpoint: CheckpointSlot,
    ) ![]u8 {
        if (value_len == 0 or root_page_id == 0) return error.InvalidNativeValueChain;
        if (range_start > value_len or range_len > value_len - range_start) return error.EndOfStream;
        const out = try allocator.alloc(u8, range_len);
        errdefer allocator.free(out);
        try self.readValuePagesRangeInto(root_page_id, value_len, range_start, out, checkpoint);
        return out;
    }

    fn readValuePagesRangeInto(self: *NativeFile, root_page_id: u64, value_len: usize, range_start: usize, out: []u8, checkpoint: CheckpointSlot) !void {
        const range_len = out.len;
        if (range_start > value_len or range_len > value_len - range_start) return error.EndOfStream;
        if (value_len == 0 or root_page_id == 0) return error.InvalidNativeValueChain;
        if (range_len == 0) return;

        if (try self.valueTreeRoot(root_page_id, value_len, checkpoint)) |ref| {
            try self.readExtentRange(ref, checkpoint, range_start, out);
            return;
        }

        var reader = ValuePageReader{};
        defer reader.deinit(self);
        const range_end = range_start + range_len;
        var value_offset: usize = 0;
        var written: usize = 0;
        var page_id = root_page_id;
        var pages_seen: u64 = 0;
        while (page_id != 0) {
            pages_seen += 1;
            if (pages_seen > checkpoint.page_count) return error.InvalidNativeValueChain;

            const payload = try decodePagePayload(try reader.readChain(self, checkpoint, page_id, range_end - value_offset), .value);
            const page = try decodeValuePage(payload);
            if (page.chunk.len == 0) return error.InvalidNativeValueChain;
            if (page.chunk.len > value_len - value_offset) return error.InvalidNativeValueChain;

            const page_start = value_offset;
            const page_end = page_start + page.chunk.len;
            if (page_end > range_start and page_start < range_end) {
                const copy_start = if (range_start > page_start) range_start - page_start else 0;
                const copy_end = @min(page.chunk.len, range_end - page_start);
                const copy_len = copy_end - copy_start;
                @memcpy(out[written..][0..copy_len], page.chunk[copy_start..][0..copy_len]);
                written += copy_len;
            }

            value_offset = page_end;
            page_id = page.next_page;
            if (value_offset == value_len and page_id != 0) return error.InvalidNativeValueChain;
            if (value_offset >= range_end and written == range_len) {
                break;
            }
        }

        if (written != range_len) return error.InvalidNativeValueChain;
    }

    fn validateReachableValuePages(
        self: *NativeFile,
        root_page_id: u64,
        value_len: usize,
        checkpoint: CheckpointSlot,
        reachable_pages: *ReachablePageSet,
    ) !void {
        if (value_len == 0 or root_page_id == 0) return error.InvalidNativeValueChain;

        if (try self.valueTreeRoot(root_page_id, value_len, checkpoint)) |ref| {
            return try self.validateExtent(ref, checkpoint, reachable_pages);
        }

        var remaining = value_len;
        var page_id = root_page_id;
        var pages_seen: u64 = 0;
        const use_link_cache = self.page_cache_enabled.load(.monotonic) and self.page_cache_bypass.load(.monotonic) == 0;
        while (page_id != 0) {
            pages_seen += 1;
            if (pages_seen > checkpoint.page_count) return error.InvalidNativeValueChain;

            try self.markReachablePage(reachable_pages, page_id, checkpoint.page_count);

            var chunk_len: usize = 0;
            var next_page: u64 = 0;
            var resolved = false;
            if (use_link_cache) {
                if (try self.page_cache.getLinksCopy(self.allocator, page_id)) |links| {
                    defer if (links.key) |key| self.allocator.free(key);
                    if (links.kind != .value) return error.UnexpectedNativePageKind;
                    chunk_len = links.chunk_len;
                    next_page = links.link_page;
                    resolved = true;
                }
            }
            if (!resolved) {
                const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, page_id, .value, checkpoint);
                defer self.allocator.free(payload);
                const page = try decodeValuePage(payload);
                chunk_len = page.chunk.len;
                next_page = page.next_page;
                if (use_link_cache) self.cachePageLinks(page_id, .value, payload);
            }

            if (chunk_len == 0) return error.InvalidNativeValueChain;
            if (chunk_len > remaining) return error.InvalidNativeValueChain;
            remaining -= chunk_len;
            page_id = next_page;
            if (remaining == 0 and page_id != 0) return error.InvalidNativeValueChain;
        }

        if (remaining != 0) return error.InvalidNativeValueChain;
    }

    fn validateReachableFreeMap(self: *NativeFile, checkpoint: CheckpointSlot, reachable_pages: *ReachablePageSet) !void {
        if (checkpoint.free_map_root_page == 0) return;
        try self.markReachablePage(reachable_pages, checkpoint.free_map_root_page, checkpoint.page_count);

        const payload = try self.readPagePayloadByKindAllocForCheckpoint(self.allocator, checkpoint.free_map_root_page, .free_map, checkpoint);
        defer self.allocator.free(payload);
        const free_map = try decodeFreeMapAlloc(self.allocator, payload, checkpoint.page_count);
        defer self.allocator.free(free_map.free_pages);

        for (free_map.free_pages) |page_id| {
            if (reachable_pages.contains(page_id)) return error.InvalidNativeFreeMap;
        }
        try self.validateFreePagesSafeForCheckpointSlots(free_map.free_pages);
    }

    pub const LiveStats = struct {
        record_count: u64,
        bytes: u64,
        compact_size: u64,
    };

    /// Compact-layout accounting uses only record metadata and ordered keys.
    /// Integrity checking separately visits and validates all reachable payloads.
    pub fn liveStats(self: *NativeFile, cancel: ?*const maintenance.CancelToken) !LiveStats {
        var live_bytes: u64 = 0;
        const indexed = self.header.indexed_reclamation or self.vacuum_target_indexed;
        var compact_pages: u64 = if (indexed) 1 else 2; // header; allocator added below
        var record_count: u64 = 0;
        var namespace_directory = NamespaceDirectory.empty;
        defer deinitNamespaceDirectory(self.allocator, &namespace_directory);

        for ([_]LiveRecordSource{ .{ .catalog = .metadata }, .{ .catalog = .index }, .documents }) |source| {
            var cursor = try LiveRecordCursor.init(self, source);
            defer cursor.deinit();
            var counter = DocumentIndexBulkBuilder{ .owner = self, .file = undefined, .next_page_id = &compact_pages, .count_only = true };
            defer counter.deinit();
            var count: u64 = 0;
            var packed_used: usize = 0;
            while (try cursor.next(cancel)) |record| {
                if (source == .catalog and source.catalog == .metadata and std.mem.startsWith(u8, record.key, artifact_lease_key_prefix)) continue;
                const len = if (record.external_value_root_page == 0) record.value.len else record.external_value_len;
                const is_catalog = cursor.kind == .catalog;
                const header_len: usize = if (is_catalog) 16 else 28;
                live_bytes +|= record.key.len + len;
                const external = header_len + record.key.len + len > self.maxPagePayloadBytes();
                const record_size = header_len + record.key.len + (if (external) @as(usize, 8) else len);
                if (external) compact_pages += if (is_catalog) self.valueTreePageCount(len) else self.valuePageCount(len);
                if (record_size + 4 > self.maxPagePayloadBytes() / 2) {
                    compact_pages += 1;
                } else {
                    if (packed_used == 0 or packed_used + 4 + record_size > self.maxPagePayloadBytes()) {
                        compact_pages += 1;
                        packed_used = 0;
                    }
                    packed_used += 4 + record_size;
                }
                if (indexed and record.key.len > index_inline_key_limit) compact_pages += 1;
                try counter.add(record.key, 1);
                count += 1;
                if (!is_catalog and !indexed) {
                    const namespace = documentNamespace(record.key);
                    if (!namespace_directory.contains(namespace)) {
                        const owned = try self.allocator.dupe(u8, namespace);
                        errdefer self.allocator.free(owned);
                        try namespace_directory.put(self.allocator, owned, 1);
                    }
                }
            }
            _ = try counter.finish();
            if (cursor.kind == .catalog and count > 0) compact_pages += 1; // descriptor
            record_count += count;
        }
        if (namespace_directory.count() > 0) namespace_count: {
            if (namespace_directory.count() <= 32) {
                const encoded = try encodeNamespaceDirectoryAlloc(self.allocator, .snapshot, &namespace_directory);
                defer self.allocator.free(encoded);
                if (self.catalogEntryFitsInline(namespace_directory_key, encoded)) {
                    compact_pages += 1;
                    break :namespace_count;
                }
            }
            const keys = try self.allocator.alloc([]const u8, namespace_directory.count());
            defer self.allocator.free(keys);
            var it = namespace_directory.keyIterator();
            for (keys) |*key| key.* = it.next().?.*;
            std.mem.sort([]const u8, keys, {}, struct {
                fn less(_: void, a: []const u8, b: []const u8) bool {
                    return std.mem.lessThan(u8, a, b);
                }
            }.less);
            var counter = DocumentIndexBulkBuilder{ .owner = self, .file = undefined, .next_page_id = &compact_pages, .count_only = true };
            defer counter.deinit();
            var packed_used: usize = 0;
            for (keys) |key| {
                const size = 16 + key.len + 8;
                if (size + 4 > self.maxPagePayloadBytes() / 2) {
                    compact_pages += 1;
                } else {
                    if (packed_used == 0 or packed_used + 4 + size > self.maxPagePayloadBytes()) {
                        compact_pages += 1;
                        packed_used = 0;
                    }
                    packed_used += 4 + size;
                }
                try counter.add(key, 1);
            }
            _ = try counter.finish();
            compact_pages += 1; // directory catalog descriptor
        }
        if (indexed) {
            const data_pages = compact_pages;
            const per_page = (self.maxPagePayloadBytes() - 24) / 4;
            while (true) {
                const total = data_pages + 1 + (try std.math.divCeil(u64, compact_pages - 1, per_page));
                if (total == compact_pages) break;
                compact_pages = total;
            }
        }
        return .{ .record_count = record_count, .bytes = live_bytes, .compact_size = compact_pages * @as(u64, self.header.page_size) };
    }

    fn valueTreePageCount(self: *const NativeFile, value_len: usize) u64 {
        var level = self.valuePageCount(value_len);
        var total = level;
        while (level > 1) {
            level = std.math.divCeil(u64, level, extent_fanout) catch unreachable;
            total += level;
        }
        return total;
    }

    fn valuePageCount(self: *const NativeFile, value_len: usize) u64 {
        std.debug.assert(value_len > 0);
        return @intCast(std.math.divCeil(usize, value_len, self.maxValuePagePayloadBytes()) catch unreachable);
    }

    fn writePage(self: *NativeFile, page_id: u64, kind: PageKind, contents: []const u8) !void {
        return self.writePageWithOptions(page_id, kind, contents, .{});
    }

    fn writePageWithOptions(self: *NativeFile, page_id: u64, kind: PageKind, contents: []const u8, options: WriteOptions) !void {
        var batch = PageWriteBatch{ .file = self, .options = options };
        try batch.appendPage(page_id, kind, contents);
        try batch.flush();
    }

    /// Cache admission happens only after the complete encoded write succeeds.
    fn cacheWrittenPage(self: *NativeFile, page_id: u64, page: []const u8, options: WriteOptions) void {
        const kind: PageKind = @fromBackingInt(page[4]);
        const len = std.mem.readInt(u32, page[8..12], .little);
        const contents = page[page_header_size..][0..len];
        if (!self.admitPageToCache(page) or (kind == .value and options.payload_cache == .cold_sequential)) {
            // Skipping admission must still remove stale bytes AND navigation
            // links from a previous use of this page ID. Cold payload pages
            // also skip link admission; extent/catalog metadata stays warm.
            self.page_cache.remove(self.allocator, page_id);
        } else if (self.page_cache_enabled.load(.monotonic)) {
            self.page_cache.put(self.allocator, page_id, page);
            self.cachePageLinks(page_id, kind, contents);
        }
    }

    /// Best-effort: decode and cache the chain-navigation metadata for a page
    /// just written, so reachability walks can traverse it without re-reading
    /// the payload. On any decode surprise the stale entry is dropped and the
    /// walks fall back to the payload path.
    fn cachePageLinks(self: *NativeFile, page_id: u64, kind: PageKind, payload: []const u8) void {
        if (page_id & packed_record_flag != 0) return;
        if (!self.page_cache_enabled.load(.monotonic)) return;
        switch (kind) {
            .document => {
                const entry = decodeDocumentEntry(payload) catch {
                    self.page_cache.remove(self.allocator, page_id);
                    return;
                };
                self.page_cache.putLinks(self.allocator, page_id, .{
                    .kind = .document,
                    .link_page = entry.previous_page,
                    .external_value_root_page = entry.external_value_root_page,
                    .external_value_len = entry.external_value_len,
                });
            },
            .catalog => {
                const entry = decodeCatalogEntry(payload) catch {
                    self.page_cache.remove(self.allocator, page_id);
                    return;
                };
                self.page_cache.putLinks(self.allocator, page_id, .{
                    .kind = .catalog,
                    .link_page = entry.previous_page,
                    .external_value_root_page = entry.external_value_root_page,
                    .external_value_len = entry.external_value_len,
                    .key = @constCast(entry.key),
                    .is_delete = entry.is_delete,
                    .value_len = if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len,
                });
            },
            .value => {
                const page = decodeValuePage(payload) catch {
                    self.page_cache.remove(self.allocator, page_id);
                    return;
                };
                self.page_cache.putLinks(self.allocator, page_id, .{
                    .kind = .value,
                    .link_page = page.next_page,
                    .chunk_len = page.chunk.len,
                });
            },
            // A reused page id may carry stale link info from a previous life.
            .data, .value_extent, .catalog_index, .record_bundle, .allocator, .key => self.page_cache.removeLinks(self.allocator, page_id),
            .document_index => self.page_cache.removeLinks(self.allocator, page_id),
            .free_map => unreachable,
        }
    }

    fn stagingTransaction(self: *const NativeFile) bool {
        return self.transaction_header != null and !self.flushing_transaction;
    }

    fn transactionWrites(self: *NativeFile) !*TransactionWrites {
        if (self.transaction_writes) |writes| return writes;
        const writes = try self.allocator.create(TransactionWrites);
        errdefer self.allocator.destroy(writes);
        writes.* = .{ .pages = try self.pageAllocatorFromFreeMap(self.activeCheckpoint()) };
        self.transaction_writes = writes;
        return writes;
    }

    fn stagedEntry(self: *NativeFile, root: TransactionWrites.Root, key: []const u8) ?CatalogEntry {
        if (!self.stagingTransaction()) return null;
        const writes = self.transaction_writes orelse return null;
        return TransactionWrites.entry(writes.maps[@backingInt(root)].get(key) orelse return null);
    }

    fn stageMutation(self: *NativeFile, root: TransactionWrites.Root, mutation: CatalogMutation, options: WriteOptions) !void {
        // Acquire ownership before a spill: callers may be appending/renaming
        // an entry borrowed from the write set that the spill will release.
        var owned = mutation;
        owned.key = try self.allocator.dupe(u8, mutation.key);
        errdefer self.allocator.free(owned.key);
        const inline_value = !mutation.is_delete and mutation.external_value_root_page == 0 and
            (if (root == .documents) self.documentEntryFitsInline(mutation.key, mutation.value) else self.catalogEntryFitsInline(mutation.key, mutation.value));
        owned.value = try self.allocator.dupe(u8, if (inline_value) mutation.value else "");
        errdefer self.allocator.free(owned.value);
        const spill_copy = if (!mutation.is_delete and !inline_value and mutation.external_value_root_page == 0 and mutation.value.len <= self.maxPagePayloadBytes()) try self.allocator.dupe(u8, mutation.value) else null;
        defer if (spill_copy) |bytes| self.allocator.free(bytes);
        const source_value = spill_copy orelse mutation.value;
        var writes = try self.transactionWrites();
        const replaced = writes.maps[@backingInt(root)].get(owned.key);
        const replaced_bytes = if (replaced) |old| old.key.len + old.value.len else 0;
        if ((replaced == null and writes.count >= TransactionWrites.max_keys) or writes.bytes - replaced_bytes + owned.key.len + owned.value.len > TransactionWrites.max_bytes) {
            try self.flushTransactionWrites();
            writes = try self.transactionWrites();
        }
        if (!mutation.is_delete and mutation.external_value_root_page == 0 and !inline_value) {
            owned.external_value_root_page = if (root == .documents)
                try self.writeValuePagesAllocated(&writes.pages, source_value)
            else
                try self.writeCatalogValue(&writes.pages, source_value, options);
            owned.external_value_len = mutation.value.len;
            try self.flushTransactionPayloads();
        }
        const map = &writes.maps[@backingInt(root)];
        const slot = try map.getOrPut(self.allocator, owned.key);
        if (slot.found_existing) {
            const old = slot.value_ptr.*;
            writes.bytes -= old.key.len + old.value.len;
            self.allocator.free(old.key);
            self.allocator.free(old.value);
        } else writes.count += 1;
        slot.key_ptr.* = owned.key;
        slot.value_ptr.* = owned;
        writes.bytes += owned.key.len + owned.value.len;
        // Track private changes; commit collapses them to one revision.
        self.header.checkpoints[self.header.active_checkpoint].commit_sequence += 1;
    }

    fn flushTransactionPayloads(self: *NativeFile) !void {
        const writes = self.transaction_writes.?;
        try writes.pages.flush();
        self.header.checkpoints[self.header.active_checkpoint].page_count = writes.pages.next_page_id;
    }

    fn flushTransactionWrites(self: *NativeFile) anyerror!void {
        if (self.flushing_transaction) return;
        const writes = self.transaction_writes orelse return;
        try self.finishCatalogAppends(writes);
        self.flushing_transaction = true;
        self.transaction_pages = &writes.pages;
        defer {
            self.transaction_pages = null;
            self.flushing_transaction = false;
        }
        // Keep entries alive through every root's finalization. Editors own
        // their navigation state; the allocator/record packer is shared.
        for (&writes.maps, 0..) |*map, root| {
            if (map.count() == 0) continue;
            var batch = std.ArrayListUnmanaged(CatalogMutation).empty;
            defer batch.deinit(self.allocator);
            try batch.ensureTotalCapacity(self.allocator, map.count());
            var it = map.valueIterator();
            while (it.next()) |mutation| batch.appendAssumeCapacity(mutation.*);
            std.mem.sort(CatalogMutation, batch.items, {}, struct {
                fn less(_: void, a: CatalogMutation, b: CatalogMutation) bool {
                    return std.mem.order(u8, a.key, b.key) == .lt;
                }
            }.less);
            if (root == @backingInt(TransactionWrites.Root.documents)) {
                const docs = try self.allocator.alloc(DocumentMutation, batch.items.len);
                defer self.allocator.free(docs);
                for (batch.items, docs) |m, *doc| doc.* = .{ .key = m.key, .value = m.value, .is_delete = m.is_delete, .external_value_root_page = m.external_value_root_page, .external_value_len = m.external_value_len };
                try self.putDocumentBatch(docs);
            } else try self.putCatalogBatchForRoot(if (root == 0) .metadata else .index, batch.items, .{});
            // The next root may resolve external index keys written here.
            try writes.pages.flush();
        }
        var next = self.activeCheckpoint();
        next.free_map_root_page = try writes.pages.allocate();
        next.page_count = writes.pages.next_page_id;
        try writes.pages.flush();
        try self.persistAllocator(&writes.pages, &next);
        try self.publishCheckpoint(next);
        self.transaction_writes = null;
        writes.deinit(self.allocator);
    }

    /// Writer-only barrier for a scan that must include prior staged writes.
    /// The caller holds the mutation lock. Checkpoint-based read APIs never
    /// access transaction state, so pinned readers need no mutation lock.
    pub fn materializeTransactionCheckpoint(self: *NativeFile) !CheckpointSlot {
        try self.flushTransactionWrites();
        return self.activeCheckpoint();
    }

    /// The caller serializes writers and checkpoint acquisition throughout a
    /// transaction. Intermediate roots are private; existing pinned readers
    /// continue reading immutable pages from their own checkpoint.
    pub fn beginTransaction(self: *NativeFile) !void {
        if (self.read_only) return error.ReadOnly;
        if (self.transaction_header != null) return error.TransactionAlreadyActive;
        if (self.checkpoint_publication_uncertain) return error.OutcomeUnknown;
        self.transaction_header = self.header;
    }

    pub fn abortTransaction(self: *NativeFile) void {
        self.invalidateLedger();
        const previous = self.transaction_header orelse return;
        if (self.transaction_writes) |writes| writes.deinit(self.allocator);
        self.transaction_writes = null;
        self.header = previous;
        self.transaction_header = null;
        self.discardTransactionTail();
        self.namespace_directory_cache_root = std.math.maxInt(u64);
        deinitNamespaceDirectory(self.allocator, &self.namespace_directory_cache);
        self.namespace_directory_cache = .empty;
        self.namespace_directory_delta_depth = 0;
    }

    fn discardTransactionTail(self: *NativeFile) void {
        const first_page = self.activeCheckpoint().page_count;
        self.page_cache.discardFrom(self.allocator, first_page);
        const size = first_page * @as(u64, self.header.page_size);
        self.file.setLength(self.runtimeIo(), size) catch {
            self.checkpoint_publication_uncertain = true;
        };
    }

    pub fn commitTransaction(self: *NativeFile) !void {
        return self.commitTransactionWithDurability(true);
    }

    pub fn commitTransactionWithDurability(self: *NativeFile, durable: bool) !void {
        const previous = self.transaction_header orelse return error.InvalidTransactionState;
        try self.flushTransactionWrites();
        var next = self.activeCheckpoint();
        const changed = next.commit_sequence != previous.checkpoints[previous.active_checkpoint].commit_sequence;
        self.header = previous;
        self.transaction_header = null;
        errdefer {
            if (!self.checkpoint_publication_uncertain) self.discardTransactionTail();
            self.namespace_directory_cache_root = std.math.maxInt(u64);
            deinitNamespaceDirectory(self.allocator, &self.namespace_directory_cache);
            self.namespace_directory_cache = .empty;
            self.namespace_directory_delta_depth = 0;
        }
        if (changed) next.commit_sequence = previous.checkpoints[previous.active_checkpoint].commit_sequence + 1;
        if (!durable and !self.no_sync and !self.externally_locked) {
            defer self.notifyPublication();
            // Keep the last durable checkpoint slots intact until an explicit
            // durability barrier. Readers on this handle see the new roots;
            // reopening after a crash sees the previous complete checkpoint.
            if (changed) {
                if (self.durable_header == null) self.durable_header = previous;
                self.header.checkpoints[self.header.active_checkpoint] = next;
            }
            return;
        }
        if (changed) {
            try self.syncIfRequired();
            try self.publishCheckpoint(next);
        } else try self.sync();
    }

    fn publishCheckpoint(self: *NativeFile, checkpoint: CheckpointSlot) !void {
        if (self.transaction_header != null) {
            self.header.checkpoints[self.header.active_checkpoint] = checkpoint;
            return;
        }
        defer self.notifyPublication();
        if (self.durable_header) |durable| {
            self.header = durable;
            self.durable_header = null;
        }
        const next_slot: u8 = if (self.header.active_checkpoint == 0) 1 else 0;

        var encoded_slot: [checkpoint_slot_size]u8 = undefined;
        encodeCheckpointSlot(&encoded_slot, checkpoint);

        const io = self.runtimeIo();
        self.checkpoint_publication_uncertain = true;
        try self.file.writePositionalAll(io, &encoded_slot, checkpointOffset(next_slot));
        try self.syncIfRequired();
        const active_checkpoint: [1]u8 = .{next_slot};
        try self.file.writePositionalAll(io, &active_checkpoint, active_checkpoint_offset);
        try self.syncIfRequired();

        self.header.checkpoints[next_slot] = checkpoint;
        self.header.active_checkpoint = next_slot;
        self.checkpoint_publication_uncertain = false;
        if (self.ledger) |state| {
            self.retirement_activity_bytes +|= state.released_data_pages *| self.header.page_size;
            self.retirement_activity_bytes +|= state.retired_inline_bytes;
            state.released_data_pages = 0;
            state.retired_inline_bytes = 0;
        }
    }

    fn notifyPublication(self: *NativeFile) void {
        if (self.publication_observer) |observer| observer.notify(observer.context, self.activeCheckpoint(), self.checkpoint_publication_uncertain);
    }

    fn syncIfRequired(self: *NativeFile) !void {
        if (!self.no_sync and self.transaction_header == null) try self.file.sync(self.runtimeIo());
    }

    pub fn sync(self: *NativeFile) !void {
        if (self.transaction_header != null) return;
        try self.syncIfRequired();
        if (self.durable_header != null) try self.publishCheckpoint(self.activeCheckpoint());
    }

    pub fn failNextGenerationDirectorySyncForTest(self: *NativeFile) void {
        std.debug.assert(builtin.is_test);
        self.test_fail_generation_directory_sync = true;
    }

    /// Atomically publishes a fully built Lite generation into this file and
    /// adopts its already-open descriptor. The destination writer lock stays
    /// with `self`; `prepared` receives the retired descriptor and can be
    /// closed normally after this returns.
    pub fn replaceWithPreparedGeneration(self: *NativeFile, prepared: *NativeFile) !GenerationPublicationOutcome {
        if (self.read_only or prepared.read_only) return error.ReadOnly;
        if (std.mem.eql(u8, self.path, prepared.path)) return error.InvalidNativeSnapshotPath;

        try prepared.syncIfRequired();
        const io = self.runtimeIo();
        try renameFilePath(io, prepared.path, self.path);

        std.mem.swap(std.Io.File, &self.file, &prepared.file);
        const retired_header = self.header;
        self.header = prepared.header;
        prepared.header = retired_header;
        self.invalidateLedger();
        prepared.invalidateLedger();
        self.durable_header = null;
        prepared.durable_header = null;
        self.free_pages_verified = false;
        prepared.free_pages_verified = false;
        self.namespace_directory_cache_root = std.math.maxInt(u64);
        self.namespace_directory_delta_depth = 0;
        deinitNamespaceDirectory(self.allocator, &self.namespace_directory_cache);
        self.namespace_directory_cache = .empty;
        self.page_cache.clear(self.allocator);

        // The generation is already visible and the live handle has adopted
        // it. Preserve that committed state while returning an explicit
        // durability outcome to the layer that can finish rebinding runtime
        // metadata before surfacing it to the caller.
        if (!self.no_sync) {
            if (builtin.is_test and self.test_fail_generation_directory_sync) {
                self.test_fail_generation_directory_sync = false;
                std.log.err("Lite restore published but parent directory sync failed path={s} class={s}", .{ self.path, @errorName(error.InjectedGenerationDirectorySyncFailure) });
                return .durability_unknown;
            }
            fs_paths.syncDirPortable(io, std.fs.path.dirname(self.path) orelse ".") catch |err| {
                std.log.err("Lite restore published but parent directory sync failed path={s} class={s}", .{ self.path, @errorName(err) });
                return .durability_unknown;
            };
        }
        return .complete;
    }
};

fn appendPageToFile(
    allocator: Allocator,
    file: std.Io.File,
    io: std.Io,
    page_size: usize,
    next_page_id: *u64,
    kind: PageKind,
    contents: []const u8,
) !u64 {
    if (contents.len > page_size - page_header_size) return error.PageTooLarge;
    const page_id = next_page_id.*;
    const page = try allocator.alloc(u8, page_size);
    defer allocator.free(page);
    encodePage(page, kind, contents);
    try file.writePositionalAll(io, page, page_id * @as(u64, @intCast(page_size)));
    next_page_id.* += 1;
    return page_id;
}

const DocumentIndexChild = struct {
    first_key: []u8,
    key_page: u64,
    page_id: u64,
};

/// Packed B+ tree builder with one unfinished node per level. Long keys
/// remain record references, including separators; only the last input key is
/// materialized for order validation. Counting uses exactly the same frontier.
const DocumentIndexBulkBuilder = struct {
    const Level = struct {
        children: std.ArrayListUnmanaged(DocumentIndexChild) = .empty,
        bytes: usize = document_index_header_size + @sizeOf(u64),
    };

    owner: *NativeFile,
    file: std.Io.File,
    next_page_id: *u64,
    pages: ?*PageAllocator = null,
    count_only: bool = false,
    leaf_keys: std.ArrayListUnmanaged([]u8) = .empty,
    leaf_key_storage: ?[]u8 = null,
    leaf_key_used: usize = 0,
    leaf_pointers: std.ArrayListUnmanaged(u64) = .empty,
    leaf_key_pages: std.ArrayListUnmanaged(u64) = .empty,
    leaf_bytes: usize = document_index_header_size,
    last_key: std.ArrayListUnmanaged(u8) = .empty,
    has_last_key: bool = false,
    levels: std.ArrayListUnmanaged(Level) = .empty,

    pub fn deinit(self: *DocumentIndexBulkBuilder) void {
        if (self.leaf_key_storage) |storage| self.owner.allocator.free(storage);
        self.leaf_keys.deinit(self.owner.allocator);
        self.leaf_pointers.deinit(self.owner.allocator);
        self.leaf_key_pages.deinit(self.owner.allocator);
        self.last_key.deinit(self.owner.allocator);
        for (self.levels.items) |*level| {
            for (level.children.items) |child| self.owner.allocator.free(child.first_key);
            level.children.deinit(self.owner.allocator);
        }
        self.levels.deinit(self.owner.allocator);
    }

    fn appendNode(self: *DocumentIndexBulkBuilder, node: DocumentIndexNode) !u64 {
        if (self.owner.vacuum_workspace_limit) |limit| {
            if (self.next_page_id.* >= limit / self.owner.header.page_size) return error.LiteStorageBudgetExceeded;
        }
        if (self.count_only) {
            const page = self.next_page_id.*;
            self.next_page_id.* += 1;
            return page;
        }
        if (self.pages) |pages| return self.owner.writeDocumentIndexNode(pages, node);
        const encoded = try encodeDocumentIndexNode(self.owner.allocator, node);
        defer self.owner.allocator.free(encoded);
        return try appendPageToFile(self.owner.allocator, self.file, self.owner.runtimeIo(), self.owner.header.page_size, self.next_page_id, .document_index, encoded);
    }

    fn add(self: *DocumentIndexBulkBuilder, key: []const u8, document_page_id: u64) !void {
        if (self.has_last_key and std.mem.order(u8, self.last_key.items, key) != .lt)
            return error.InvalidDocumentIndexOrder;
        if (key.len > std.math.maxInt(u16)) return error.RecordTooLarge;
        const external = key.len > index_inline_key_limit;
        const slot_size = 10 + (if (external) @as(usize, 8) else key.len);
        if (document_index_header_size + slot_size > self.owner.maxPagePayloadBytes()) return error.DocumentIndexNodeTooLarge;
        if (self.leaf_bytes + slot_size > self.owner.maxPagePayloadBytes()) try self.flushLeaf();
        const alloc = self.owner.allocator;
        try self.leaf_keys.ensureUnusedCapacity(alloc, 1);
        try self.leaf_pointers.ensureUnusedCapacity(alloc, 1);
        try self.leaf_key_pages.ensureUnusedCapacity(alloc, 1);
        try self.last_key.ensureTotalCapacity(alloc, key.len);
        var stored: []u8 = &.{};
        if (!external and key.len != 0) {
            if (self.leaf_key_storage == null) self.leaf_key_storage = try alloc.alloc(u8, self.owner.maxPagePayloadBytes());
            stored = self.leaf_key_storage.?[self.leaf_key_used..][0..key.len];
            @memcpy(stored, key);
            self.leaf_key_used += key.len;
        }
        self.leaf_keys.appendAssumeCapacity(stored);
        self.leaf_pointers.appendAssumeCapacity(document_page_id);
        self.leaf_key_pages.appendAssumeCapacity(if (external) document_page_id else 0);
        self.leaf_bytes += slot_size;
        self.last_key.clearRetainingCapacity();
        self.last_key.appendSliceAssumeCapacity(key);
        self.has_last_key = true;
    }

    fn flushLeaf(self: *DocumentIndexBulkBuilder) !void {
        if (self.leaf_keys.items.len == 0) return;
        const page = try self.appendNode(.{ .kind = .leaf, .keys = self.leaf_keys.items, .pointers = self.leaf_pointers.items, .key_pages = self.leaf_key_pages.items });
        const child = DocumentIndexChild{
            .first_key = try self.owner.allocator.dupe(u8, self.leaf_keys.items[0]),
            .key_page = self.leaf_key_pages.items[0],
            .page_id = page,
        };
        // pushChild consumes the separator even on failure.
        try self.pushChild(0, child);
        self.leaf_key_used = 0;
        self.leaf_keys.clearRetainingCapacity();
        self.leaf_pointers.clearRetainingCapacity();
        self.leaf_key_pages.clearRetainingCapacity();
        self.leaf_bytes = document_index_header_size;
    }

    fn pushChild(self: *DocumentIndexBulkBuilder, height: usize, child: DocumentIndexChild) anyerror!void {
        errdefer self.owner.allocator.free(child.first_key);
        if (height >= 64) return error.InvalidDocumentIndex;
        if (height >= self.levels.items.len) {
            const old = self.levels.items.len;
            try self.levels.resize(self.owner.allocator, height + 1);
            @memset(self.levels.items[old..], .{});
        }
        const slot_size = 10 + (if (child.key_page != 0) @as(usize, 8) else child.first_key.len);
        if (self.levels.items[height].children.items.len > 0 and
            self.levels.items[height].bytes + slot_size > self.owner.maxPagePayloadBytes())
        {
            if (self.levels.items[height].children.items.len < 2) return error.DocumentIndexNodeTooLarge;
            const parent = try self.sealLevel(height);
            try self.pushChild(height + 1, parent);
        }
        // Recursion can relocate levels, so reacquire this pointer afterward.
        const level = &self.levels.items[height];
        const extra = if (level.children.items.len == 0) 0 else slot_size;
        try level.children.append(self.owner.allocator, child);
        level.bytes += extra;
    }

    fn sealLevel(self: *DocumentIndexBulkBuilder, height: usize) !DocumentIndexChild {
        const level = &self.levels.items[height];
        const group = level.children.items;
        const page = try self.appendInternalGroup(group);
        const parent = DocumentIndexChild{ .first_key = group[0].first_key, .key_page = group[0].key_page, .page_id = page };
        for (group[1..]) |child| self.owner.allocator.free(child.first_key);
        level.children.clearRetainingCapacity();
        level.bytes = document_index_header_size + @sizeOf(u64);
        return parent;
    }

    fn finish(self: *DocumentIndexBulkBuilder) !u64 {
        try self.flushLeaf();
        var height: usize = 0;
        while (height < self.levels.items.len) : (height += 1) {
            const count = self.levels.items[height].children.items.len;
            if (count == 0) continue;
            var higher_pending = false;
            for (self.levels.items[height + 1 ..]) |level| {
                if (level.children.items.len != 0) higher_pending = true;
            }
            if (count == 1 and !higher_pending) return self.levels.items[height].children.items[0].page_id;
            const parent = try self.sealLevel(height);
            try self.pushChild(height + 1, parent);
        }
        return 0;
    }

    fn appendInternalGroup(self: *DocumentIndexBulkBuilder, children: []const DocumentIndexChild) !u64 {
        const alloc = self.owner.allocator;
        const keys = try alloc.alloc([]u8, children.len - 1);
        defer alloc.free(keys);
        const pointers = try alloc.alloc(u64, children.len);
        defer alloc.free(pointers);
        const key_pages = try alloc.alloc(u64, children.len - 1);
        defer alloc.free(key_pages);
        for (children, 0..) |child, i| {
            pointers[i] = child.page_id;
            if (i > 0) {
                keys[i - 1] = child.first_key;
                key_pages[i - 1] = child.key_page;
            }
        }
        return try self.appendNode(.{ .kind = .internal, .keys = keys, .pointers = pointers, .key_pages = key_pages });
    }
};

fn appendValuePagesToFile(
    allocator: Allocator,
    file: std.Io.File,
    io: std.Io,
    page_size: usize,
    chunk_size: usize,
    next_page_id: *u64,
    value: []const u8,
) !u64 {
    if (value.len == 0) return error.InvalidNativeValueChain;
    if (chunk_size == 0) return error.InvalidNativePageLength;
    const page_count = std.math.divCeil(usize, value.len, chunk_size) catch unreachable;
    const root_page_id = next_page_id.*;

    var offset: usize = 0;
    var page_index: usize = 0;
    while (offset < value.len) : (page_index += 1) {
        const len = @min(chunk_size, value.len - offset);
        const current_page_id = next_page_id.*;
        const next_value_page = if (page_index + 1 < page_count) current_page_id + 1 else 0;
        const payload = try allocator.alloc(u8, value_page_header_size + len);
        defer allocator.free(payload);
        std.mem.writeInt(u64, payload[0..8], next_value_page, .little);
        @memcpy(payload[value_page_header_size..][0..len], value[offset..][0..len]);
        _ = try appendPageToFile(allocator, file, io, page_size, next_page_id, .value, payload);
        offset += len;
    }
    return root_page_id;
}

fn appendFreeMapPageToFile(
    allocator: Allocator,
    file: std.Io.File,
    io: std.Io,
    page_size: usize,
    next_page_id: *u64,
    covered_page_count: u64,
    free_pages: []const u64,
) !u64 {
    const payload = try encodeFreeMapAlloc(allocator, @intCast(page_size), covered_page_count, free_pages);
    defer allocator.free(payload);
    return try appendPageToFile(allocator, file, io, page_size, next_page_id, .free_map, payload);
}

fn catalogRootPage(slot: CheckpointSlot, root: CatalogRoot) u64 {
    return switch (root) {
        .metadata => slot.catalog_root_page,
        .index => slot.index_catalog_root_page,
    };
}

fn setCatalogRootPage(slot: *CheckpointSlot, root: CatalogRoot, page_id: u64) void {
    switch (root) {
        .metadata => slot.catalog_root_page = page_id,
        .index => slot.index_catalog_root_page = page_id,
    }
}

pub fn create(io: std.Io, path: []const u8) !void {
    var writer_lock_file = (try acquireWriterLock(std.heap.page_allocator, io, path)).file;
    defer writer_lock_file.close(io);

    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{});

    const replace_existing = pathExists(io, path);
    const replacement_path = if (replace_existing)
        try realPathAlloc(std.heap.page_allocator, io, path)
    else
        null;
    defer if (replacement_path) |canonical| std.heap.page_allocator.free(canonical);
    const create_target = if (replacement_path) |canonical| canonical else path;
    const staging_path = if (replace_existing)
        try std.fmt.allocPrint(std.heap.page_allocator, "{s}.tmp-aflite-create", .{create_target})
    else
        null;
    defer if (staging_path) |tmp_path| std.heap.page_allocator.free(tmp_path);
    errdefer if (staging_path) |tmp_path| deleteFilePath(io, tmp_path) catch {};

    var file = try createDataFile(io, staging_path orelse create_target, .{ .truncate = true });
    var file_open = true;
    defer if (file_open) file.close(io);

    try file.writePositionalAll(io, &encoded, 0);
    try file.sync(io);
    if (staging_path) |tmp_path| {
        file.close(io);
        file_open = false;
        renameFilePath(io, tmp_path, create_target) catch |err| {
            deleteFilePath(io, tmp_path) catch {};
            return err;
        };
    }
    try fs_paths.syncDirPortable(io, std.fs.path.dirname(create_target) orelse ".");
}

pub fn lockWriterPath(allocator: Allocator, path: []const u8) !PathWriterLock {
    var io_impl = threaded_io_limits.initService(allocator);
    errdefer io_impl.deinit();

    const file = (try acquireWriterLock(allocator, io_impl.io(), path)).file;
    errdefer file.close(io_impl.io());

    return .{
        .io_impl = io_impl,
        .file = file,
    };
}

/// Acquires the same cross-process writer lock through a caller-owned std.Io.
pub fn lockWriterPathWithIo(allocator: Allocator, io: std.Io, path: []const u8) !PathWriterLock {
    const file = (try acquireWriterLock(allocator, io, path)).file;
    errdefer file.close(io);
    return .{
        .io_impl = undefined,
        .borrowed_io = io,
        .file = file,
    };
}

/// Read-only embedded calls coordinate with commits without reserving a writer.
pub fn lockReaderPathWithIo(allocator: Allocator, io: std.Io, path: []const u8) !PathWriterLock {
    const lock_path = try writerLockPathAlloc(allocator, io, path);
    defer allocator.free(lock_path);
    const file = std.Io.Dir.cwd().openFile(io, lock_path, .{
        .mode = .read_only,
        .lock = .shared,
        .lock_nonblocking = true,
    }) catch |err| switch (err) {
        error.FileNotFound => blk: {
            // A distributed snapshot may have no sidecar and live on read-only
            // media. Every sidecar creator must exclude fallback inode readers,
            // including a reader installing the sidecar after media changes.
            var exclusive_inode = true;
            const data = std.Io.Dir.cwd().openFile(io, path, .{
                .mode = .read_only,
                .lock = .exclusive,
                .lock_nonblocking = true,
            }) catch |inode_err| switch (inode_err) {
                error.WouldBlock, error.FileBusy => blk_data: {
                    exclusive_inode = false;
                    break :blk_data try std.Io.Dir.cwd().openFile(io, path, .{
                        .mode = .read_only,
                        .lock = .shared,
                        .lock_nonblocking = true,
                    });
                },
                else => return inode_err,
            };
            var keep_data = false;
            defer if (!keep_data) data.close(io);
            // A writer may have installed the sidecar before our inode fence.
            if (std.Io.Dir.cwd().openFile(io, lock_path, .{
                .mode = .read_only,
                .lock = .shared,
                .lock_nonblocking = true,
            })) |existing| break :blk existing else |open_err| {
                if (open_err != error.FileNotFound) return open_err;
            }
            if (!exclusive_inode) {
                keep_data = true;
                break :blk data;
            }
            break :blk std.Io.Dir.cwd().createFile(io, lock_path, .{
                .read = true,
                .truncate = false,
                .lock = .shared,
                .lock_nonblocking = true,
            }) catch |create_err| switch (create_err) {
                error.AccessDenied, error.ReadOnlyFileSystem => {
                    try data.lock(io, .shared);
                    keep_data = true;
                    break :blk data;
                },
                else => return create_err,
            };
        },
        else => return err,
    };
    return .{ .io_impl = undefined, .borrowed_io = io, .file = file };
}

fn openDataFile(io: std.Io, path: []const u8, lock_mode: LockMode, wait_for_reader_lock: bool) !LockFile {
    const file = std.Io.Dir.cwd().openFile(io, path, .{
        .mode = if (lock_mode != .writer) .read_only else .read_write,
        .lock = if (lock_mode == .reader) .shared else .none,
        // Readers wait for a bounded in-place allocator write to finish.
        // Writers and maintenance retain nonblocking admission.
        .lock_nonblocking = lock_mode != .reader or !wait_for_reader_lock,
    }) catch |err| switch (err) {
        // Snapshot safety and maintenance fencing depend on the kernel lock.
        // Silently reopening without it turns an unsupported filesystem into
        // a data-corruption hazard, so every normal Lite open fails closed.
        error.FileLocksUnsupported => return error.FileLocksUnsupported,
        else => return err,
    };
    return .{ .file = file };
}

const CreateDataFileOptions = struct {
    truncate: bool = true,
    exclusive: bool = false,
};

fn createDataFile(io: std.Io, path: []const u8, opts: CreateDataFileOptions) !std.Io.File {
    return std.Io.Dir.cwd().createFile(io, path, .{
        .read = true,
        .truncate = opts.truncate,
        .exclusive = opts.exclusive,
    });
}

fn acquireWriterLock(allocator: Allocator, io: std.Io, path: []const u8) !LockFile {
    const lock_path = try writerLockPathAlloc(allocator, io, path);
    defer allocator.free(lock_path);
    if (std.Io.Dir.cwd().openFile(io, lock_path, .{
        .mode = .read_write,
        .lock = .exclusive,
        .lock_nonblocking = true,
    })) |existing| return .{ .file = existing } else |err| {
        if (err != error.FileNotFound) return err;
    }
    // Readers of sidecar-free snapshots hold this inode shared. Exclude them
    // before installing the sidecar so switching lock authorities cannot race
    // a reader's header/catalog capture, even if directory permissions change.
    const data = std.Io.Dir.cwd().openFile(io, path, .{
        .mode = .read_only,
        .lock = .exclusive,
        .lock_nonblocking = true,
    }) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    defer if (data) |held| held.close(io);
    const file = std.Io.Dir.cwd().createFile(io, lock_path, .{
        .read = true,
        .truncate = false,
        .lock = .exclusive,
        .lock_nonblocking = true,
    }) catch |err| switch (err) {
        error.FileLocksUnsupported => return error.FileLocksUnsupported,
        else => return err,
    };
    return .{ .file = file };
}

fn writerLockPathAlloc(allocator: Allocator, io: std.Io, path: []const u8) ![]u8 {
    const canonical_data_path = realPathAlloc(allocator, io, path) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    if (canonical_data_path) |canonical| {
        defer allocator.free(canonical);
        return appendLockSuffix(allocator, canonical);
    }

    const dirname = std.fs.path.dirname(path) orelse ".";
    const basename = std.fs.path.basename(path);
    const canonical_parent = try realPathAlloc(allocator, io, dirname);
    defer allocator.free(canonical_parent);
    const canonical_missing_path = try std.fs.path.join(allocator, &.{ canonical_parent, basename });
    defer allocator.free(canonical_missing_path);
    return appendLockSuffix(allocator, canonical_missing_path);
}

fn pathsReferToSameExistingFile(allocator: Allocator, io: std.Io, a: []const u8, b: []const u8) !bool {
    const a_real = realPathAlloc(allocator, io, a) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return false,
        else => return err,
    };
    defer allocator.free(a_real);

    const b_real = realPathAlloc(allocator, io, b) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return false,
        else => return err,
    };
    defer allocator.free(b_real);

    return std.mem.eql(u8, a_real, b_real);
}

pub fn realPathAlloc(allocator: Allocator, io: std.Io, path: []const u8) ![:0]u8 {
    if (std.fs.path.isAbsolute(path)) {
        return try std.Io.Dir.realPathFileAbsoluteAlloc(io, path, allocator);
    }
    return try std.Io.Dir.cwd().realPathFileAlloc(io, path, allocator);
}

fn appendLockSuffix(allocator: Allocator, path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, "{s}.lock", .{path});
}

fn acquireDataRewriteLock(io: std.Io, path: []const u8) !LockFile {
    const file = std.Io.Dir.cwd().openFile(io, path, .{
        .mode = .read_write,
        .lock = .exclusive,
        .lock_nonblocking = true,
    }) catch |err| switch (err) {
        error.FileLocksUnsupported => return error.FileLocksUnsupported,
        else => return err,
    };
    return .{ .file = file };
}

fn pathExists(io: std.Io, path: []const u8) bool {
    if (std.fs.path.isAbsolute(path)) {
        std.Io.Dir.accessAbsolute(io, path, .{}) catch return false;
    } else {
        std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    }
    return true;
}

fn createSnapshotFile(io: std.Io, path: []const u8) !std.Io.File {
    if (std.fs.path.isAbsolute(path)) {
        return try std.Io.Dir.createFileAbsolute(io, path, .{ .read = true, .truncate = true });
    }
    return try std.Io.Dir.cwd().createFile(io, path, .{ .read = true, .truncate = true });
}

fn renameFilePath(io: std.Io, old_path: []const u8, new_path: []const u8) !void {
    if (std.fs.path.isAbsolute(old_path) and std.fs.path.isAbsolute(new_path)) {
        try std.Io.Dir.renameAbsolute(old_path, new_path, io);
    } else {
        try std.Io.Dir.rename(std.Io.Dir.cwd(), old_path, std.Io.Dir.cwd(), new_path, io);
    }
}

fn deleteFilePath(io: std.Io, path: []const u8) !void {
    if (std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.deleteFileAbsolute(io, path);
    } else {
        try std.Io.Dir.cwd().deleteFile(io, path);
    }
}

pub fn inspect(_: Allocator, io: std.Io, path: []const u8) !InspectReport {
    var file = (try openDataFile(io, path, .reader, false)).file;
    defer file.close(io);

    var header_bytes: [header_size]u8 = undefined;
    try readHeaderExactAt(file, io, &header_bytes);
    return inspectBytes(&header_bytes);
}

pub fn checkFile(allocator: Allocator, path: []const u8) !CheckReport {
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var file = (try openDataFile(io, path, .reader, false)).file;
    defer file.close(io);

    const file_size = (try file.stat(io)).size;
    var header_bytes: [header_size]u8 = undefined;
    const read = try file.readPositionalAll(io, &header_bytes, 0);
    if (read != header_size) {
        return invalidCheck(.{
            .valid = true,
            .file_size = file_size,
            .valid_prefix_size = 0,
            .tail_bytes = file_size,
            .record_count = 0,
            .live_file_count = 0,
            .live_bytes = 0,
            .compact_size = 0,
            .reclaimable_bytes = 0,
        }, "truncated_header");
    }
    const header = decodeHeader(&header_bytes) catch |err| {
        return invalidCheck(.{
            .valid = true,
            .file_size = file_size,
            .valid_prefix_size = 0,
            .tail_bytes = file_size,
            .record_count = 0,
            .live_file_count = 0,
            .live_bytes = 0,
            .compact_size = 0,
            .reclaimable_bytes = 0,
        }, issueForDecodeError(err));
    };
    _ = selectCompleteCheckpointForFile(header, file_size) catch |err| {
        const checkpoint = header.checkpoints[header.active_checkpoint];
        const expected_size = checkpointPrefixSize(checkpoint, header.page_size) catch 0;
        return invalidCheck(.{
            .valid = true,
            .file_size = file_size,
            .valid_prefix_size = file_size,
            .tail_bytes = 0,
            .record_count = if (expected_size > 0 and checkpoint.page_count > 0) checkpoint.page_count - 1 else 0,
            .live_file_count = 0,
            .live_bytes = 0,
            .compact_size = expected_size,
            .reclaimable_bytes = 0,
        }, switch (err) {
            error.InvalidNativeCheckpoint => "invalid_checkpoint",
            error.TruncatedNativeFile => "truncated_file",
        });
    };
    var native_file = try NativeFile.open(allocator, path, true);
    defer native_file.close();
    return try native_file.check();
}

pub fn copyStableSnapshot(allocator: Allocator, source_path: []const u8, dest_path: []const u8, replace: bool) !StableSnapshotReport {
    if (std.mem.eql(u8, source_path, dest_path)) return error.InvalidNativeSnapshotPath;

    var source = try NativeFile.open(allocator, source_path, true);
    defer source.close();
    return try source.copyStableSnapshotToPath(dest_path, replace);
}

pub fn inspectBytes(raw: []const u8) InspectReport {
    const header = decodeHeader(raw) catch |err| {
        return .{
            .valid = false,
            .format_version = 0,
            .page_size = 0,
            .active_checkpoint = 0,
            .commit_sequence = 0,
            .page_count = 0,
            .issue = issueForDecodeError(err),
        };
    };
    const active = header.checkpoints[header.active_checkpoint];
    return .{
        .valid = true,
        .format_version = if (header.indexed_reclamation) indexed_format_version else format_version,
        .page_size = header.page_size,
        .active_checkpoint = header.active_checkpoint,
        .commit_sequence = active.commit_sequence,
        .page_count = active.page_count,
    };
}

pub fn encodeHeader(out: *[header_size]u8, header: Header) void {
    @memset(out, 0);
    @memcpy(out[magic_offset..][0..magic.len], if (header.indexed_reclamation) indexed_v4_magic else if (header.packed_records) magic else unpacked_v3_magic);
    std.mem.writeInt(u32, out[version_offset..][0..4], if (header.indexed_reclamation) @as(u32, 4) else format_version, .little);
    std.mem.writeInt(u32, out[page_size_offset..][0..4], header.page_size, .little);
    std.mem.writeInt(u32, out[header_size_offset..][0..4], header_size, .little);
    out[active_checkpoint_offset] = header.active_checkpoint;

    for (header.checkpoints, 0..) |slot, index| {
        encodeCheckpointSlot(out[checkpointOffset(index)..][0..checkpoint_slot_size], slot);
    }

    std.mem.writeInt(u32, out[header_checksum_offset..][0..4], headerChecksum(out), .little);
}

pub fn decodeHeader(raw: []const u8) !Header {
    if (raw.len < header_size) return error.TruncatedNativeHeader;
    const header_raw = raw[0..header_size];
    const indexed_reclamation = std.mem.eql(u8, header_raw[magic_offset..][0..magic.len], indexed_v4_magic);
    const has_packed_records = indexed_reclamation or std.mem.eql(u8, header_raw[magic_offset..][0..magic.len], magic);
    if (!has_packed_records and !std.mem.eql(u8, header_raw[magic_offset..][0..magic.len], unpacked_v3_magic)) return error.InvalidNativeMagic;

    const version = std.mem.readInt(u32, header_raw[version_offset..][0..4], .little);
    if (version != (if (indexed_reclamation) @as(u32, 4) else format_version)) return error.UnsupportedNativeFormatVersion;

    const encoded_header_size = std.mem.readInt(u32, header_raw[header_size_offset..][0..4], .little);
    if (encoded_header_size != header_size) return error.InvalidNativeHeaderSize;

    const expected_checksum = std.mem.readInt(u32, header_raw[header_checksum_offset..][0..4], .little);
    if (expected_checksum != headerChecksum(header_raw)) return error.NativeHeaderChecksumMismatch;

    const page_size = std.mem.readInt(u32, header_raw[page_size_offset..][0..4], .little);
    if (!validPageSize(page_size)) return error.InvalidNativePageSize;

    const active_hint = header_raw[active_checkpoint_offset];

    var checkpoints: [checkpoint_slot_count]CheckpointSlot = undefined;
    var valid_slots: [checkpoint_slot_count]bool = @splat(false);
    for (&checkpoints, 0..) |*slot, index| {
        const raw_slot = header_raw[checkpointOffset(index)..][0..checkpoint_slot_size];
        // Zero checksums are a legacy v3 compatibility encoding. Revision 4
        // requires each recoverable slot to authenticate its ownership root.
        if (indexed_reclamation and std.mem.readInt(u32, raw_slot[checkpoint_slot_checksum_offset..][0..4], .little) != checkpointSlotChecksum(raw_slot)) {
            slot.* = .{ .page_count = 0 };
            continue;
        }
        slot.* = decodeCheckpointSlot(raw_slot) catch {
            slot.* = .{ .page_count = 0 };
            continue;
        };
        valid_slots[index] = validCheckpointSlot(slot.*);
    }
    const active_checkpoint = try selectActiveCheckpoint(checkpoints, valid_slots, active_hint);

    return .{
        .packed_records = has_packed_records,
        .indexed_reclamation = indexed_reclamation,
        .page_size = page_size,
        .active_checkpoint = active_checkpoint,
        .checkpoints = checkpoints,
    };
}

fn checkpointOffset(index: usize) usize {
    return checkpoint_slots_offset + index * checkpoint_slot_size;
}

fn encodeCheckpointSlot(out: []u8, slot: CheckpointSlot) void {
    std.debug.assert(out.len == checkpoint_slot_size);
    @memset(out, 0);
    std.mem.writeInt(u64, out[0..8], slot.commit_sequence, .little);
    std.mem.writeInt(u64, out[8..16], slot.catalog_root_page, .little);
    std.mem.writeInt(u64, out[16..24], slot.document_root_page, .little);
    std.mem.writeInt(u64, out[24..32], slot.index_catalog_root_page, .little);
    std.mem.writeInt(u64, out[32..40], slot.free_map_root_page, .little);
    std.mem.writeInt(u64, out[40..48], slot.page_count, .little);
    std.mem.writeInt(u64, out[48..56], slot.namespace_directory_root_page, .little);
    std.mem.writeInt(u64, out[56..64], slot.document_index_root_page, .little);
    std.mem.writeInt(u32, out[checkpoint_slot_checksum_offset..][0..4], checkpointSlotChecksum(out), .little);
}

fn decodeCheckpointSlot(raw: []const u8) !CheckpointSlot {
    std.debug.assert(raw.len == checkpoint_slot_size);
    const expected_checksum = std.mem.readInt(u32, raw[checkpoint_slot_checksum_offset..][0..4], .little);
    if (expected_checksum != 0 and expected_checksum != checkpointSlotChecksum(raw)) return error.NativeCheckpointChecksumMismatch;
    return .{
        .commit_sequence = std.mem.readInt(u64, raw[0..8], .little),
        .catalog_root_page = std.mem.readInt(u64, raw[8..16], .little),
        .document_root_page = std.mem.readInt(u64, raw[16..24], .little),
        .index_catalog_root_page = std.mem.readInt(u64, raw[24..32], .little),
        .free_map_root_page = std.mem.readInt(u64, raw[32..40], .little),
        .page_count = std.mem.readInt(u64, raw[40..48], .little),
        .namespace_directory_root_page = std.mem.readInt(u64, raw[48..56], .little),
        .document_index_root_page = std.mem.readInt(u64, raw[56..64], .little),
    };
}

fn validCheckpointSlot(slot: CheckpointSlot) bool {
    if (slot.page_count == 0) return false;
    if (!validCheckpointRoot(slot.catalog_root_page, slot.page_count)) return false;
    if (!validCheckpointRoot(slot.document_root_page, slot.page_count)) return false;
    if (!validCheckpointRoot(slot.index_catalog_root_page, slot.page_count)) return false;
    if (!validCheckpointRoot(slot.free_map_root_page, slot.page_count)) return false;
    if (!validCheckpointRoot(slot.namespace_directory_root_page, slot.page_count)) return false;
    if (!validCheckpointRoot(slot.document_index_root_page, slot.page_count)) return false;
    return true;
}

fn checkpointPrefixSize(slot: CheckpointSlot, page_size: u32) !u64 {
    return std.math.mul(u64, slot.page_count, @as(u64, page_size)) catch error.InvalidNativeCheckpoint;
}

fn validCheckpointRoot(root_page: u64, page_count: u64) bool {
    return root_page == 0 or (physicalPage(root_page) != 0 and physicalPage(root_page) < page_count);
}

fn selectActiveCheckpoint(
    checkpoints: [checkpoint_slot_count]CheckpointSlot,
    valid_slots: [checkpoint_slot_count]bool,
    active_hint: u8,
) !u8 {
    var best: ?u8 = null;
    for (checkpoints, 0..) |slot, index| {
        if (!valid_slots[index]) continue;
        const slot_index: u8 = @intCast(index);
        if (best) |best_index| {
            const best_slot = checkpoints[best_index];
            if (slot.commit_sequence > best_slot.commit_sequence or
                (slot.commit_sequence == best_slot.commit_sequence and slot_index == active_hint))
            {
                best = slot_index;
            }
        } else {
            best = slot_index;
        }
    }
    return best orelse error.InvalidNativeCheckpoint;
}

fn selectCompleteCheckpointForFile(header: Header, file_size: u64) !u8 {
    var best: ?u8 = null;
    var saw_valid_slot = false;
    var saw_invalid_size = false;
    for (header.checkpoints, 0..) |slot, index| {
        if (!validCheckpointSlot(slot)) continue;
        saw_valid_slot = true;
        const expected_size = checkpointPrefixSize(slot, header.page_size) catch {
            saw_invalid_size = true;
            continue;
        };
        if (expected_size > file_size) continue;
        const slot_index: u8 = @intCast(index);
        if (best) |best_index| {
            const best_slot = header.checkpoints[best_index];
            if (slot.commit_sequence > best_slot.commit_sequence or
                (slot.commit_sequence == best_slot.commit_sequence and slot_index == header.active_checkpoint))
            {
                best = slot_index;
            }
        } else {
            best = slot_index;
        }
    }
    if (best) |index| return index;
    if (saw_invalid_size) return error.InvalidNativeCheckpoint;
    return if (saw_valid_slot) error.TruncatedNativeFile else error.InvalidNativeCheckpoint;
}

fn encodePage(out: []u8, kind: PageKind, payload: []const u8) void {
    encodePageParts(out, kind, &.{}, payload);
}

fn encodePageParts(out: []u8, kind: PageKind, prefix: []const u8, payload: []const u8) void {
    const len = prefix.len + payload.len;
    std.debug.assert(out.len >= page_header_size);
    std.debug.assert(len <= out.len - page_header_size);
    @memset(out, 0);
    @memcpy(out[0..page_magic.len], page_magic);
    out[4] = @backingInt(kind);
    std.mem.writeInt(u32, out[8..12], @intCast(len), .little);
    @memcpy(out[page_header_size..][0..prefix.len], prefix);
    @memcpy(out[page_header_size + prefix.len ..][0..payload.len], payload);

    var crc = Crc32.init();
    crc.update(out[0..page_crc_offset]);
    crc.update(out[page_header_size..][0..len]);
    std.mem.writeInt(u32, out[page_crc_offset..][0..4], crc.final(), .little);
}

const PackedRecord = struct { kind: PageKind, bytes: []const u8 };

fn packedRecordAtOffset(payload: []const u8, offset: usize) !PackedRecord {
    if (offset > payload.len or payload.len - offset < 4) return error.InvalidNativePageLength;
    const len = std.mem.readInt(u16, payload[offset..][0..2], .little);
    if (len > payload.len - offset - 4 or payload[offset + 3] != 0) return error.InvalidNativePageLength;
    const kind: PageKind = switch (payload[offset + 2]) {
        @backingInt(PageKind.catalog) => .catalog,
        @backingInt(PageKind.document) => .document,
        else => return error.InvalidNativePageKind,
    };
    return .{ .kind = kind, .bytes = payload[offset + 4 ..][0..len] };
}

fn packedRecordPayload(payload: []const u8, reference: u64) !PackedRecord {
    const wanted: usize = @intCast((reference >> 47) & 0xffff);
    var offset: usize = 0;
    while (offset < payload.len) {
        const record = try packedRecordAtOffset(payload, offset);
        if (offset == wanted) return record;
        offset += 4 + record.bytes.len;
    }
    return error.InvalidPageId;
}

fn unpackRecordPage(raw: []u8, reference: u64) !void {
    const record = try packedRecordPayload(try decodePagePayload(raw, .record_bundle), reference);
    var scratch: [65536]u8 = undefined;
    @memcpy(scratch[0..record.bytes.len], record.bytes);
    encodePage(raw, record.kind, scratch[0..record.bytes.len]);
}

fn decodePagePayloadAlloc(allocator: Allocator, raw: []const u8, expected_kind: PageKind) ![]u8 {
    return try allocator.dupe(u8, try decodePagePayload(raw, expected_kind));
}

fn decodePagePayload(raw: []const u8, expected_kind: PageKind) ![]const u8 {
    if (raw.len < page_header_size) return error.TruncatedNativePage;
    if (!std.mem.eql(u8, raw[0..page_magic.len], page_magic)) return error.InvalidNativePageMagic;
    const kind_raw = raw[4];
    const kind: PageKind = switch (kind_raw) {
        @backingInt(PageKind.data) => .data,
        @backingInt(PageKind.catalog) => .catalog,
        @backingInt(PageKind.document) => .document,
        @backingInt(PageKind.value) => .value,
        @backingInt(PageKind.free_map) => .free_map,
        @backingInt(PageKind.document_index) => .document_index,
        @backingInt(PageKind.value_extent) => .value_extent,
        @backingInt(PageKind.catalog_index) => .catalog_index,
        @backingInt(PageKind.record_bundle) => .record_bundle,
        @backingInt(PageKind.allocator) => .allocator,
        @backingInt(PageKind.key) => .key,
        else => return error.InvalidNativePageKind,
    };
    if (kind != expected_kind) return error.UnexpectedNativePageKind;

    const payload_len = std.mem.readInt(u32, raw[8..12], .little);
    if (payload_len > raw.len - page_header_size) return error.InvalidNativePageLength;

    var crc = Crc32.init();
    crc.update(raw[0..page_crc_offset]);
    crc.update(raw[page_header_size..][0..payload_len]);
    const expected_crc = std.mem.readInt(u32, raw[page_crc_offset..][0..4], .little);
    if (crc.final() != expected_crc) return error.NativePageChecksumMismatch;

    return raw[page_header_size..][0..payload_len];
}

fn encodeCatalogEntry(allocator: Allocator, out: *std.ArrayListUnmanaged(u8), entry: CatalogEntry) !void {
    try encodeCatalogEntryRaw(allocator, out, .{
        .previous_page = entry.previous_page,
        .key = entry.key,
        .value = entry.value,
        .is_delete = entry.is_delete,
        .external_value_root_page = entry.external_value_root_page,
        .external_value_len = if (entry.external_value_root_page != 0 and entry.external_value_len == 0) entry.value.len else entry.external_value_len,
    });
}

fn encodeCatalogEntryRaw(allocator: Allocator, out: *std.ArrayListUnmanaged(u8), entry: EncodedCatalogEntry) !void {
    const value_len = if (entry.external_value_root_page != 0) entry.external_value_len else entry.value.len;
    if (entry.key.len > catalog_key_len_mask or value_len > std.math.maxInt(u32)) return error.RecordTooLarge;
    if (entry.is_delete and entry.external_value_root_page != 0) return error.InvalidNativeCatalogEntryFlags;
    const external_value = entry.external_value_root_page != 0;
    if (external_value and entry.external_value_len == 0) return error.InvalidNativeValueChain;

    const start = out.items.len;
    const stored_value_len: usize = if (external_value) 8 else value_len;
    try out.resize(allocator, start + 16 + entry.key.len + stored_value_len);
    const encoded = out.items[start..];
    std.mem.writeInt(u64, encoded[0..8], entry.previous_page, .little);
    const key_len_flags: u32 =
        @as(u32, @intCast(entry.key.len)) |
        (if (entry.is_delete) catalog_delete_flag else 0) |
        (if (external_value) catalog_external_value_flag else 0);
    std.mem.writeInt(u32, encoded[8..12], key_len_flags, .little);
    std.mem.writeInt(u32, encoded[12..16], @intCast(value_len), .little);
    @memcpy(encoded[16..][0..entry.key.len], entry.key);
    if (external_value) {
        std.mem.writeInt(u64, encoded[16 + entry.key.len ..][0..8], entry.external_value_root_page, .little);
    } else {
        @memcpy(encoded[16 + entry.key.len ..][0..entry.value.len], entry.value);
    }
}

fn decodeCatalogEntry(raw: []const u8) !CatalogEntry {
    if (raw.len < 16) return error.TruncatedNativeCatalogEntry;
    const previous_page = std.mem.readInt(u64, raw[0..8], .little);
    const key_len_flags = std.mem.readInt(u32, raw[8..12], .little);
    const flags = key_len_flags & ~catalog_key_len_mask;
    if (flags & ~(catalog_delete_flag | catalog_external_value_flag) != 0) return error.InvalidNativeCatalogEntryFlags;
    const is_delete = flags & catalog_delete_flag != 0;
    const external_value = flags & catalog_external_value_flag != 0;
    if (is_delete and external_value) return error.InvalidNativeCatalogEntryFlags;

    const key_len = key_len_flags & catalog_key_len_mask;
    const value_len = std.mem.readInt(u32, raw[12..16], .little);
    const stored_value_len: u64 = if (external_value) 8 else value_len;
    const payload_len = @as(u64, key_len) + stored_value_len;
    if (payload_len > raw.len - 16) return error.TruncatedNativeCatalogEntry;
    const key_start: usize = 16;
    const key_end = key_start + @as(usize, @intCast(key_len));
    const stored_value_end = key_end + @as(usize, @intCast(stored_value_len));
    const external_value_root_page = if (external_value) blk: {
        if (value_len == 0) return error.InvalidNativeValueChain;
        const root = std.mem.readInt(u64, raw[key_end..][0..8], .little);
        if (root == 0) return error.InvalidNativeValueChain;
        break :blk root;
    } else 0;
    return .{
        .previous_page = previous_page,
        .key = raw[key_start..key_end],
        .value = if (external_value) raw[key_end..key_end] else raw[key_end..stored_value_end],
        .is_delete = is_delete,
        .external_value_root_page = external_value_root_page,
        .external_value_len = if (external_value) @intCast(value_len) else 0,
    };
}

fn encodeDocumentEntry(allocator: Allocator, out: *std.ArrayListUnmanaged(u8), entry: DocumentEntry) !void {
    const value_len = if (entry.external_value_root_page != 0 and entry.external_value_len != 0) entry.external_value_len else entry.value.len;
    if (entry.key.len > std.math.maxInt(u32) or value_len > std.math.maxInt(u32)) return error.RecordTooLarge;
    if (entry.is_delete and entry.external_value_root_page != 0) return error.InvalidNativeDocumentEntryFlags;
    const external_value = entry.external_value_root_page != 0;
    if (external_value and value_len == 0) return error.InvalidNativeValueChain;

    const start = out.items.len;
    const stored_value_len: usize = if (external_value) 8 else value_len;
    const header_len: usize = 28;
    try out.resize(allocator, start + header_len + entry.key.len + stored_value_len);
    const encoded = out.items[start..];
    std.mem.writeInt(u64, encoded[0..8], entry.previous_page, .little);
    encoded[8] =
        (if (entry.is_delete) document_delete_flag else 0) |
        (if (external_value) document_external_value_flag else 0) |
        document_namespace_link_flag;
    @memset(encoded[9..12], 0);
    std.mem.writeInt(u32, encoded[12..16], @intCast(entry.key.len), .little);
    std.mem.writeInt(u32, encoded[16..20], @intCast(value_len), .little);
    std.mem.writeInt(u64, encoded[20..28], entry.previous_namespace_page, .little);
    @memcpy(encoded[header_len..][0..entry.key.len], entry.key);
    if (external_value) {
        std.mem.writeInt(u64, encoded[header_len + entry.key.len ..][0..8], entry.external_value_root_page, .little);
    } else {
        @memcpy(encoded[header_len + entry.key.len ..][0..entry.value.len], entry.value);
    }
}

fn decodeDocumentEntry(raw: []const u8) !DocumentEntry {
    if (raw.len < 20) return error.TruncatedNativeDocumentEntry;
    const previous_page = std.mem.readInt(u64, raw[0..8], .little);
    const flags = raw[8];
    if (flags & ~(document_delete_flag | document_external_value_flag | document_namespace_link_flag) != 0) return error.InvalidNativeDocumentEntryFlags;
    const is_delete = flags & document_delete_flag != 0;
    const external_value = flags & document_external_value_flag != 0;
    const has_namespace_link = flags & document_namespace_link_flag != 0;
    if (!has_namespace_link) return error.InvalidNativeDocumentEntryFlags;
    if (is_delete and external_value) return error.InvalidNativeDocumentEntryFlags;

    const key_len = std.mem.readInt(u32, raw[12..16], .little);
    const value_len = std.mem.readInt(u32, raw[16..20], .little);
    const stored_value_len: u64 = if (external_value) 8 else value_len;
    const header_len: usize = 28;
    if (raw.len < header_len) return error.TruncatedNativeDocumentEntry;
    const payload_len = @as(u64, key_len) + stored_value_len;
    if (payload_len > raw.len - header_len) return error.TruncatedNativeDocumentEntry;
    const key_start: usize = header_len;
    const key_end = key_start + @as(usize, @intCast(key_len));
    const stored_value_end = key_end + @as(usize, @intCast(stored_value_len));
    const external_value_root_page = if (external_value) blk: {
        if (value_len == 0) return error.InvalidNativeValueChain;
        const root = std.mem.readInt(u64, raw[key_end..][0..8], .little);
        if (root == 0) return error.InvalidNativeValueChain;
        break :blk root;
    } else 0;
    return .{
        .previous_page = previous_page,
        .previous_namespace_page = std.mem.readInt(u64, raw[20..28], .little),
        .key = raw[key_start..key_end],
        .value = if (external_value) raw[key_end..key_end] else raw[key_end..stored_value_end],
        .is_delete = is_delete,
        .external_value_root_page = external_value_root_page,
        .external_value_len = if (external_value) @intCast(value_len) else 0,
    };
}

fn encodedDocumentIndexNodeSize(node: DocumentIndexNode) !usize {
    if (node.keys.len > std.math.maxInt(u16)) return error.RecordTooLarge;
    if ((node.kind == .leaf and node.pointers.len != node.keys.len) or
        (node.kind == .internal and node.pointers.len != node.keys.len + 1))
        return error.InvalidDocumentIndex;
    const internal_header_size: usize = if (node.kind == .internal) @sizeOf(u64) else 0;
    var size: usize = document_index_header_size + internal_header_size;
    for (node.keys, 0..) |key, i| {
        if (key.len > std.math.maxInt(u16)) return error.RecordTooLarge;
        size = try std.math.add(usize, size, @sizeOf(u16) + @sizeOf(u64) + (if (key.len > index_inline_key_limit or (node.key_pages != null and node.key_pages.?[i] != 0)) @as(usize, 8) else key.len));
    }
    return size;
}

fn lowerBoundIndexKeys(keys: []const []u8, key: []const u8) usize {
    var low: usize = 0;
    var high = keys.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (std.mem.order(u8, keys[mid], key) == .lt)
            low = mid + 1
        else
            high = mid;
    }
    return low;
}

fn upperBoundIndexKeys(keys: []const []u8, key: []const u8) usize {
    var low: usize = 0;
    var high = keys.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (std.mem.order(u8, keys[mid], key) != .gt)
            low = mid + 1
        else
            high = mid;
    }
    return low;
}

fn encodeDocumentIndexNode(allocator: Allocator, node: DocumentIndexNode) ![]u8 {
    const size = try encodedDocumentIndexNodeSize(node);
    const out = try allocator.alloc(u8, size);
    errdefer allocator.free(out);
    @memcpy(out[0..document_index_magic.len], document_index_magic);
    out[8] = @backingInt(node.kind);
    out[9] = 0;
    std.mem.writeInt(u16, out[10..12], @intCast(node.keys.len), .little);
    var pos: usize = document_index_header_size;
    if (node.kind == .internal) {
        std.mem.writeInt(u64, out[pos..][0..8], node.pointers[0], .little);
        pos += 8;
    }
    for (node.keys, 0..) |key, i| {
        const external = key.len > index_inline_key_limit or (node.key_pages != null and node.key_pages.?[i] != 0);
        std.mem.writeInt(u16, out[pos..][0..2], if (external) index_external_key_marker else @intCast(key.len), .little);
        pos += 2;
        const pointer_index = if (node.kind == .leaf) i else i + 1;
        std.mem.writeInt(u64, out[pos..][0..8], node.pointers[pointer_index], .little);
        pos += 8;
        if (external) {
            const pages = node.key_pages orelse return error.InvalidDocumentIndex;
            if (pages[i] == 0) return error.InvalidDocumentIndex;
            std.mem.writeInt(u64, out[pos..][0..8], pages[i], .little);
            pos += 8;
        } else {
            @memcpy(out[pos..][0..key.len], key);
            pos += key.len;
        }
    }
    std.debug.assert(pos == out.len);
    return out;
}

const IndexProbe = struct { leaf: bool, page: ?u64 };

fn probeDocumentIndexNode(raw: []const u8, key: []const u8) !IndexProbe {
    if (raw.len < document_index_header_size or !std.mem.eql(u8, raw[0..8], document_index_magic) or raw[9] != 0)
        return error.InvalidDocumentIndex;
    const leaf = switch (raw[8]) {
        1 => true,
        2 => false,
        else => return error.InvalidDocumentIndex,
    };
    const count = std.mem.readInt(u16, raw[10..12], .little);
    if (leaf and count == 0) return error.InvalidDocumentIndex;
    var pos: usize = document_index_header_size;
    var selected: ?u64 = null;
    if (!leaf) {
        if (pos + 8 > raw.len) return error.InvalidDocumentIndex;
        selected = std.mem.readInt(u64, raw[pos..][0..8], .little);
        pos += 8;
    }
    var previous: ?[]const u8 = null;
    for (0..count) |_| {
        if (pos + 10 > raw.len) return error.InvalidDocumentIndex;
        const len = std.mem.readInt(u16, raw[pos..][0..2], .little);
        const pointer = std.mem.readInt(u64, raw[pos + 2 ..][0..8], .little);
        pos += 10;
        if (len == index_external_key_marker) return error.ExternalIndexKey;
        if (len > index_inline_key_limit or len > raw.len - pos) return error.InvalidDocumentIndex;
        const candidate = raw[pos..][0..len];
        if (previous) |prev| if (std.mem.order(u8, prev, candidate) != .lt) return error.InvalidDocumentIndex;
        previous = candidate;
        const order = std.mem.order(u8, key, candidate);
        if ((leaf and order == .eq) or (!leaf and order != .lt)) selected = pointer;
        pos += len;
    }
    if (pos != raw.len) return error.InvalidDocumentIndex;
    return .{ .leaf = leaf, .page = selected };
}

fn decodeDocumentIndexNode(allocator: Allocator, raw: []const u8) !DocumentIndexNode {
    if (raw.len < document_index_header_size or !std.mem.eql(u8, raw[0..8], document_index_magic))
        return error.InvalidDocumentIndex;
    const kind: DocumentIndexNodeKind = switch (raw[8]) {
        @backingInt(DocumentIndexNodeKind.leaf) => .leaf,
        @backingInt(DocumentIndexNodeKind.internal) => .internal,
        else => return error.InvalidDocumentIndex,
    };
    if (raw[9] != 0) return error.InvalidDocumentIndex;
    const count: usize = std.mem.readInt(u16, raw[10..12], .little);
    if (kind == .leaf and count == 0) return error.InvalidDocumentIndex;
    const keys = try allocator.alloc([]u8, count);
    var keys_initialized: usize = 0;
    errdefer {
        for (keys[0..keys_initialized]) |key| allocator.free(key);
        allocator.free(keys);
    }
    const key_pages = try allocator.alloc(u64, count);
    errdefer allocator.free(key_pages);
    @memset(key_pages, 0);
    const pointer_extra: usize = if (kind == .internal) 1 else 0;
    const pointers = try allocator.alloc(u64, count + pointer_extra);
    errdefer allocator.free(pointers);
    var pos: usize = document_index_header_size;
    if (kind == .internal) {
        if (pos + 8 > raw.len) return error.InvalidDocumentIndex;
        pointers[0] = std.mem.readInt(u64, raw[pos..][0..8], .little);
        pos += 8;
    }
    for (keys, 0..) |*key, i| {
        if (pos + 10 > raw.len) return error.InvalidDocumentIndex;
        const key_len: usize = std.mem.readInt(u16, raw[pos..][0..2], .little);
        pos += 2;
        const pointer_index = if (kind == .leaf) i else i + 1;
        pointers[pointer_index] = std.mem.readInt(u64, raw[pos..][0..8], .little);
        pos += 8;
        if (key_len == index_external_key_marker) {
            if (pos + 8 > raw.len) return error.InvalidDocumentIndex;
            key_pages[i] = std.mem.readInt(u64, raw[pos..][0..8], .little);
            if (key_pages[i] == 0) return error.InvalidDocumentIndex;
            key.* = try allocator.alloc(u8, 0);
            pos += 8;
        } else {
            if (key_len > index_inline_key_limit or pos + key_len > raw.len) return error.InvalidDocumentIndex;
            key.* = try allocator.dupe(u8, raw[pos .. pos + key_len]);
            pos += key_len;
        }
        keys_initialized += 1;
        if (i > 0 and key_pages[i - 1] == 0 and key_pages[i] == 0 and std.mem.order(u8, keys[i - 1], key.*) != .lt) return error.InvalidDocumentIndex;
    }
    if (pos != raw.len) return error.InvalidDocumentIndex;
    return .{ .kind = kind, .keys = keys, .pointers = pointers, .key_pages = key_pages };
}

fn decodeValuePage(raw: []const u8) !ValuePage {
    if (raw.len < value_page_header_size) return error.TruncatedNativeValuePage;
    return .{
        .next_page = std.mem.readInt(u64, raw[0..8], .little),
        .chunk = raw[value_page_header_size..],
    };
}

fn encodeFreeMapAlloc(allocator: Allocator, page_size: u32, covered_page_count: u64, free_pages: []const u64) ![]u8 {
    if (free_pages.len > maxFreeMapEntries(page_size)) return error.NativeFreeMapTooLarge;
    if (free_pages.len > std.math.maxInt(u32)) return error.NativeFreeMapTooLarge;

    var previous_page_id: u64 = 0;
    for (free_pages) |page_id| {
        if (page_id == 0 or page_id >= covered_page_count) return error.InvalidNativeFreeMap;
        if (page_id <= previous_page_id) return error.InvalidNativeFreeMap;
        previous_page_id = page_id;
    }

    const payload = try allocator.alloc(u8, free_map_header_size + free_pages.len * 8);
    errdefer allocator.free(payload);
    std.mem.writeInt(u32, payload[0..4], free_map_format_version, .little);
    std.mem.writeInt(u32, payload[4..8], @intCast(free_pages.len), .little);
    std.mem.writeInt(u64, payload[8..16], covered_page_count, .little);
    for (free_pages, 0..) |page_id, index| {
        const offset = free_map_header_size + index * 8;
        std.mem.writeInt(u64, payload[offset..][0..8], page_id, .little);
    }
    return payload;
}

fn decodeFreeMapAlloc(allocator: Allocator, raw: []const u8, checkpoint_page_count: u64) !FreeMap {
    if (raw.len < free_map_header_size) return error.TruncatedNativeFreeMap;
    const version = std.mem.readInt(u32, raw[0..4], .little);
    if (version != free_map_format_version) return error.InvalidNativeFreeMap;
    const free_page_count = std.mem.readInt(u32, raw[4..8], .little);
    const covered_page_count = std.mem.readInt(u64, raw[8..16], .little);
    if (covered_page_count != checkpoint_page_count) return error.InvalidNativeFreeMap;
    const expected_len = free_map_header_size + @as(usize, free_page_count) * 8;
    if (raw.len != expected_len) return error.InvalidNativeFreeMap;

    const free_pages = try allocator.alloc(u64, free_page_count);
    errdefer allocator.free(free_pages);
    var previous_page_id: u64 = 0;
    for (free_pages, 0..) |*page_id, index| {
        const offset = free_map_header_size + index * 8;
        page_id.* = std.mem.readInt(u64, raw[offset..][0..8], .little);
        if (page_id.* == 0 or page_id.* >= checkpoint_page_count) return error.InvalidNativeFreeMap;
        if (page_id.* <= previous_page_id) return error.InvalidNativeFreeMap;
        previous_page_id = page_id.*;
    }
    return .{
        .covered_page_count = covered_page_count,
        .free_pages = free_pages,
    };
}

fn maxFreeMapEntries(page_size: u32) usize {
    std.debug.assert(page_size >= page_header_size + free_map_header_size);
    return (@as(usize, @intCast(page_size)) - page_header_size - free_map_header_size) / 8;
}

fn readExactAt(file: std.Io.File, io: std.Io, out: []u8, offset: u64) !void {
    const read = try file.readPositionalAll(io, out, offset);
    if (read != out.len) return error.EndOfStream;
}

fn readHeaderExactAt(file: std.Io.File, io: std.Io, out: *[header_size]u8) !void {
    const read = try file.readPositionalAll(io, out, 0);
    if (read != header_size) return error.TruncatedNativeHeader;
}

fn headerChecksum(raw: []const u8) u32 {
    var crc = Crc32.init();
    crc.update(raw[0..active_checkpoint_offset]);
    crc.update(raw[active_checkpoint_offset + 1 .. checkpoint_slots_offset]);
    crc.update(raw[checkpoint_slots_end..header_checksum_offset]);
    return crc.final();
}

fn checkpointSlotChecksum(raw: []const u8) u32 {
    var crc = Crc32.init();
    crc.update(raw[0..checkpoint_slot_payload_size]);
    return crc.final();
}

fn validPageSize(page_size: u32) bool {
    return page_size >= 4096 and page_size <= 65536 and std.math.isPowerOfTwo(page_size);
}

fn issueForDecodeError(err: anyerror) []const u8 {
    return switch (err) {
        error.TruncatedNativeHeader => "truncated_header",
        error.InvalidNativeMagic => "invalid_magic",
        error.UnsupportedNativeFormatVersion => "unsupported_format_version",
        error.InvalidNativeHeaderSize => "invalid_header_size",
        error.NativeHeaderChecksumMismatch => "header_checksum_mismatch",
        error.InvalidNativePageSize => "invalid_page_size",
        error.InvalidNativeCheckpointSlot => "invalid_checkpoint_slot",
        error.NativeCheckpointChecksumMismatch => "checkpoint_checksum_mismatch",
        error.InvalidNativeCheckpoint => "invalid_checkpoint",
        else => "invalid_header",
    };
}

fn issueForPageCheckError(err: anyerror) []const u8 {
    return switch (err) {
        error.InvalidPageId => "invalid_page_id",
        error.InvalidNativeAllocator => "invalid_allocator",
        error.TruncatedNativePage => "truncated_page",
        error.InvalidNativePageMagic => "invalid_page_magic",
        error.InvalidNativePageKind => "invalid_page_kind",
        error.UnexpectedNativePageKind => "unexpected_page_kind",
        error.InvalidNativePageLength => "invalid_page_length",
        error.NativePageChecksumMismatch => "page_checksum_mismatch",
        error.TruncatedNativeCatalogEntry => "truncated_catalog_entry",
        error.InvalidNativeCatalogEntryFlags => "invalid_catalog_entry_flags",
        error.TruncatedNativeDocumentEntry => "truncated_document_entry",
        error.InvalidNativeDocumentEntryFlags => "invalid_document_entry_flags",
        error.InvalidNamespaceDirectory => "invalid_namespace_directory",
        error.InvalidDocumentIndex,
        error.InvalidDocumentIndexOrder,
        => "invalid_document_index",
        error.InvalidNativePageChain => "invalid_page_chain",
        error.TruncatedNativeValuePage => "truncated_value_page",
        error.InvalidNativeValueChain => "invalid_value_chain",
        error.TruncatedNativeFreeMap,
        error.InvalidNativeFreeMap,
        error.UnsupportedNativeFreeMap,
        => "invalid_free_map",
        else => "invalid_page",
    };
}

fn invalidCheck(report: CheckReport, issue: []const u8) CheckReport {
    var invalid = report;
    invalid.valid = false;
    invalid.issue = issue;
    return invalid;
}

fn testPath(allocator: Allocator, tmp: std.testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

fn readHeaderForTest(path: []const u8) ![header_size]u8 {
    var header_bytes: [header_size]u8 = undefined;
    var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_only });
    defer file.close(std.testing.io);
    try readExactAt(file, std.testing.io, &header_bytes, 0);
    return header_bytes;
}

test "lite native header round trips initial checkpoint" {
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{});

    const header = try decodeHeader(&encoded);
    try std.testing.expectEqual(default_page_size, header.page_size);
    try std.testing.expectEqual(@as(u8, 0), header.active_checkpoint);
    try std.testing.expectEqual(@as(u64, 0), header.checkpoints[0].commit_sequence);
    try std.testing.expectEqual(@as(u64, 1), header.checkpoints[0].page_count);

    const report = inspectBytes(&encoded);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(format_version, report.format_version);
    try std.testing.expectEqual(@as(u64, 1), report.page_count);
}

test "lite native header rejects corrupted checksum" {
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{});
    encoded[page_size_offset] ^= 0xff;

    const report = inspectBytes(&encoded);
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("header_checksum_mismatch", report.issue.?);
}

test "lite native header rejects unsupported format version" {
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{});
    std.mem.writeInt(u32, encoded[version_offset..][0..4], format_version + 1, .little);

    const report = inspectBytes(&encoded);
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("unsupported_format_version", report.issue.?);
    try std.testing.expectError(error.UnsupportedNativeFormatVersion, decodeHeader(&encoded));
}

test "lite native header selects newest valid checkpoint slot" {
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{
        .active_checkpoint = 0,
        .checkpoints = .{
            .{ .commit_sequence = 1, .page_count = 2 },
            .{ .commit_sequence = 2, .page_count = 3 },
        },
    });

    const header = try decodeHeader(&encoded);
    try std.testing.expectEqual(@as(u8, 1), header.active_checkpoint);

    const report = inspectBytes(&encoded);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u8, 1), report.active_checkpoint);
    try std.testing.expectEqual(@as(u64, 2), report.commit_sequence);
    try std.testing.expectEqual(@as(u64, 3), report.page_count);
}

test "lite native header recovers from corrupted active checkpoint hint" {
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{
        .active_checkpoint = 1,
        .checkpoints = .{
            .{ .commit_sequence = 1, .page_count = 2 },
            .{ .commit_sequence = 2, .page_count = 3 },
        },
    });
    encoded[active_checkpoint_offset] = 0xff;

    const header = try decodeHeader(&encoded);
    try std.testing.expectEqual(@as(u8, 1), header.active_checkpoint);
    try std.testing.expectEqual(@as(u64, 2), header.checkpoints[header.active_checkpoint].commit_sequence);
}

test "lite native header recovers previous checkpoint from a checksum-bad slot" {
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{
        .active_checkpoint = 1,
        .checkpoints = .{
            .{ .commit_sequence = 1, .page_count = 2 },
            .{ .commit_sequence = 2, .page_count = 3 },
        },
    });
    encoded[checkpointOffset(1)] ^= 0xff;

    const header = try decodeHeader(&encoded);
    try std.testing.expectEqual(@as(u8, 0), header.active_checkpoint);
    try std.testing.expectEqual(@as(u64, 1), header.checkpoints[header.active_checkpoint].commit_sequence);
}

test "lite native create writes inspectable aflite file" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native.aflite");
    defer allocator.free(path);

    try create(std.testing.io, path);
    const report = try inspect(allocator, std.testing.io, path);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(format_version, report.format_version);
    try std.testing.expectEqual(default_page_size, report.page_size);
    try std.testing.expectEqual(@as(u8, 0), report.active_checkpoint);
    try std.testing.expectEqual(@as(u64, 0), report.commit_sequence);
    try std.testing.expectEqual(@as(u64, 1), report.page_count);
}

test "lite native open options propagate no_sync to file writes" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-no-sync.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.createWithOptions(allocator, path, .{ .no_sync = true });
        defer file.close();
        try std.testing.expect(file.no_sync);
        try file.putDocument("doc:no-sync", "value");
    }

    {
        var reopened = try NativeFile.openWithOptions(allocator, path, .{
            .read_only = true,
            .no_sync = true,
        });
        defer reopened.close();
        try std.testing.expect(reopened.no_sync);

        const value = (try reopened.getDocumentAlloc(allocator, "doc:no-sync")) orelse return error.TestExpectedEqual;
        defer allocator.free(value);
        try std.testing.expectEqualStrings("value", value);
    }
}

test "lite native createNew rejects existing aflite without truncating" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-create-new-existing.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:keep", "survives");
    }

    try std.testing.expectError(error.PathAlreadyExists, NativeFile.createNew(allocator, path));

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    const value = (try reopened.getDocumentAlloc(allocator, "doc:keep")) orelse return error.TestExpectedEqual;
    defer allocator.free(value);
    try std.testing.expectEqualStrings("survives", value);
}

test "lite native recreate atomically replaces the generation pinned by readers" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-recreate-pinned-reader.aflite");
    defer allocator.free(path);

    {
        var original = try NativeFile.create(allocator, path);
        defer original.close();
        try original.putDocument("doc:old", "pinned");
    }

    var pinned = try NativeFile.open(allocator, path, true);
    defer pinned.close();

    {
        var replacement = try NativeFile.create(allocator, path);
        defer replacement.close();
        try std.testing.expectEqual(@as(u64, 0), replacement.activeCheckpoint().commit_sequence);
    }

    const old_value = (try pinned.getDocumentAlloc(allocator, "doc:old")) orelse return error.TestExpectedEqual;
    defer allocator.free(old_value);
    try std.testing.expectEqualStrings("pinned", old_value);

    var current = try NativeFile.open(allocator, path, true);
    defer current.close();
    try std.testing.expect((try current.getDocumentAlloc(allocator, "doc:old")) == null);
}

test "lite native recreate through symlink preserves canonical lock identity" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const target_path = try testPath(allocator, tmp, "native-recreate-symlink-target.aflite");
    defer allocator.free(target_path);
    const alias_path = try testPath(allocator, tmp, "native-recreate-symlink-alias.aflite");
    defer allocator.free(alias_path);

    {
        var original = try NativeFile.create(allocator, target_path);
        defer original.close();
        try original.putDocument("doc:old", "replaced");
    }
    const canonical_target = try realPathAlloc(allocator, std.testing.io, target_path);
    defer allocator.free(canonical_target);
    try std.Io.Dir.cwd().symLink(std.testing.io, canonical_target, alias_path, .{});

    {
        var replacement = try NativeFile.create(allocator, alias_path);
        defer replacement.close();
        try std.testing.expectEqual(@as(u64, 0), replacement.activeCheckpoint().commit_sequence);
    }

    const canonical_alias = try realPathAlloc(allocator, std.testing.io, alias_path);
    defer allocator.free(canonical_alias);
    try std.testing.expectEqualStrings(canonical_target, canonical_alias);

    var current = try NativeFile.open(allocator, target_path, true);
    defer current.close();
    try std.testing.expect((try current.getDocumentAlloc(allocator, "doc:old")) == null);
}

test "lite native open rejects unsupported format version" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-unsupported-version.aflite");
    defer allocator.free(path);

    try create(std.testing.io, path);
    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        var raw_version: [4]u8 = undefined;
        std.mem.writeInt(u32, &raw_version, format_version + 1, .little);
        try file.writePositionalAll(std.testing.io, &raw_version, version_offset);
    }

    try std.testing.expectError(error.UnsupportedNativeFormatVersion, NativeFile.open(allocator, path, true));
}

test "lite native open rejects short files as truncated native headers" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-short-header.aflite");
    defer allocator.free(path);

    {
        var file = try std.Io.Dir.cwd().createFile(std.testing.io, path, .{});
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "not enough header bytes", 0);
    }

    try std.testing.expectError(error.TruncatedNativeHeader, NativeFile.open(allocator, path, true));
    try std.testing.expectError(error.TruncatedNativeHeader, inspect(allocator, std.testing.io, path));
}

test "lite native inspect reads only the header page" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-with-pages.aflite");
    defer allocator.free(path);

    try create(std.testing.io, path);
    {
        var file = try std.Io.Dir.cwd().createFile(std.testing.io, path, .{ .truncate = false });
        defer file.close(std.testing.io);
        const size = (try file.stat(std.testing.io)).size;
        var writer = file.writer(std.testing.io, &.{});
        try writer.seekTo(size);
        try writer.interface.writeAll("future-page-data");
        try writer.end();
    }

    const report = try inspect(allocator, std.testing.io, path);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(format_version, report.format_version);
}

test "lite native file appends page and publishes checkpoint" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-pages.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();

        const page_id = try file.allocatePage("hello native page");
        try std.testing.expectEqual(@as(u64, 1), page_id);
        try std.testing.expectEqual(@as(u64, 1), file.activeCheckpoint().commit_sequence);
        try std.testing.expectEqual(@as(u64, 3), file.activeCheckpoint().page_count);
        try std.testing.expectEqual(@as(u64, 2), file.activeCheckpoint().free_map_root_page);

        const page = try file.readPagePayloadAlloc(allocator, page_id);
        defer allocator.free(page);
        try std.testing.expectEqualStrings("hello native page", page);
    }

    const report = try inspect(allocator, std.testing.io, path);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u64, 1), report.commit_sequence);
    try std.testing.expectEqual(@as(u64, 3), report.page_count);
}

test "lite native file reopens allocated pages" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-reopen.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        _ = try file.allocatePage("persisted");
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    try std.testing.expectEqual(@as(u64, 1), reopened.activeCheckpoint().commit_sequence);
    try std.testing.expectEqual(@as(u64, 3), reopened.activeCheckpoint().page_count);
    const page = try reopened.readPagePayloadAlloc(allocator, 1);
    defer allocator.free(page);
    try std.testing.expectEqualStrings("persisted", page);
    try std.testing.expectError(error.ReadOnly, reopened.allocatePage("nope"));
}

test "lite native file publishes checkpoint without rewriting static header" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-slot-publish.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();

        var before: [header_size]u8 = undefined;
        try readExactAt(file.file, file.io_impl.io(), &before, 0);
        _ = try file.allocatePage("slot-only publish");
        var after: [header_size]u8 = undefined;
        try readExactAt(file.file, file.io_impl.io(), &after, 0);

        try std.testing.expectEqualSlices(u8, before[0..active_checkpoint_offset], after[0..active_checkpoint_offset]);
        try std.testing.expectEqualSlices(u8, before[active_checkpoint_offset + 1 .. checkpoint_slots_offset], after[active_checkpoint_offset + 1 .. checkpoint_slots_offset]);
        try std.testing.expectEqualSlices(u8, before[checkpointOffset(0)..][0..checkpoint_slot_size], after[checkpointOffset(0)..][0..checkpoint_slot_size]);
        try std.testing.expectEqualSlices(u8, before[checkpoint_slots_end..header_size], after[checkpoint_slots_end..header_size]);
        try std.testing.expectEqual(@as(u8, 1), after[active_checkpoint_offset]);
        try std.testing.expect(!std.mem.eql(u8, before[checkpointOffset(1)..][0..checkpoint_slot_size], after[checkpointOffset(1)..][0..checkpoint_slot_size]));
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    try std.testing.expectEqual(@as(u64, 1), reopened.activeCheckpoint().commit_sequence);
    try std.testing.expectEqual(@as(u64, 3), reopened.activeCheckpoint().page_count);
}

test "lite native file recovers older complete checkpoint when newest prefix is truncated" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-truncated-newest-checkpoint.aflite");
    defer allocator.free(path);

    const stable_size = blk: {
        var file = try NativeFile.create(allocator, path);
        defer file.close();

        try file.putDocument("doc:recover", "stable");
        const stable_checkpoint = file.activeCheckpoint();
        const stable_size = try checkpointPrefixSize(stable_checkpoint, file.header.page_size);

        try file.putDocument("doc:recover", "newer");
        try std.testing.expect(file.activeCheckpoint().commit_sequence > stable_checkpoint.commit_sequence);
        try std.testing.expect(try checkpointPrefixSize(file.activeCheckpoint(), file.header.page_size) > stable_size);
        break :blk stable_size;
    };

    {
        var raw = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer raw.close(std.testing.io);
        try raw.setLength(std.testing.io, stable_size);
        try raw.sync(std.testing.io);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    try std.testing.expectEqual(@as(u64, 1), reopened.activeCheckpoint().commit_sequence);
    const value = (try reopened.getDocumentAlloc(allocator, "doc:recover")).?;
    defer allocator.free(value);
    try std.testing.expectEqualStrings("stable", value);

    const report = try checkFile(allocator, path);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u64, 1), report.record_count);
    try std.testing.expectEqual(@as(u64, 0), report.tail_bytes);
}

test "lite native file permits concurrent readers" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-reader-locks.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        _ = try file.allocatePage("persisted");
    }

    var reader_a = try NativeFile.open(allocator, path, true);
    defer reader_a.close();

    var reader_b = try NativeFile.open(allocator, path, true);
    defer reader_b.close();

    try std.testing.expectEqual(@as(u64, 3), reader_a.activeCheckpoint().page_count);
    try std.testing.expectEqual(@as(u64, 3), reader_b.activeCheckpoint().page_count);
}

test "lite native file active writer permits readers but blocks second writer" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-writer-lock.aflite");
    defer allocator.free(path);

    var writer = try NativeFile.create(allocator, path);
    defer writer.close();
    _ = try writer.allocatePage("committed before reader");

    try std.testing.expectError(error.WouldBlock, NativeFile.open(allocator, path, false));

    var reader = try NativeFile.open(allocator, path, true);
    defer reader.close();
    try std.testing.expectEqual(@as(u64, 3), reader.activeCheckpoint().page_count);

    const report = try inspect(allocator, std.testing.io, path);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u64, 1), report.commit_sequence);

    try std.testing.expectError(error.WouldBlock, writer.vacuum());
}

test "lite native file canonicalizes writer lock path spellings" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-writer-lock-canonical.aflite");
    defer allocator.free(path);

    var writer = try NativeFile.create(allocator, path);
    defer writer.close();

    const alternate_path = try std.fmt.allocPrint(allocator, "./{s}", .{path});
    defer allocator.free(alternate_path);
    try std.testing.expectError(error.WouldBlock, NativeFile.open(allocator, alternate_path, false));
}

test "lite native file detects corrupted page payload" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-corrupt-page.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        _ = try file.allocatePage("checksum");
    }

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    try std.testing.expectError(error.NativePageChecksumMismatch, reopened.readPagePayloadAlloc(allocator, 1));
}

test "lite native catalog stores and reopens records" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-catalog.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putCatalogRecord("schema", "{\"version\":1}");
        try file.putCatalogRecord("index:text", "ready");
        try file.putCatalogRecord("schema", "{\"version\":2}");
        try std.testing.expectEqual(@as(u64, 3), file.activeCheckpoint().commit_sequence);
        try std.testing.expect(file.activeCheckpoint().catalog_root_page != 0);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();

    const schema = (try reopened.getCatalogRecordAlloc(allocator, "schema")).?;
    defer allocator.free(schema);
    try std.testing.expectEqualStrings("{\"version\":2}", schema);

    const index = (try reopened.getCatalogRecordAlloc(allocator, "index:text")).?;
    defer allocator.free(index);
    try std.testing.expectEqualStrings("ready", index);

    try std.testing.expectEqual(@as(?[]u8, null), try reopened.getCatalogRecordAlloc(allocator, "missing"));
}

test "lite native catalog supports tombstones and spilled values" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-catalog-large.aflite");
    defer allocator.free(path);

    const large = try allocator.alloc(u8, default_page_size * 3);
    defer allocator.free(large);
    for (large, 0..) |*byte, i| byte.* = @intCast(i % 251);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putCatalogRecord("index:large", large);
        try file.putCatalogRecord("index:gone", "delete me");
        try file.deleteCatalogRecord("index:gone");
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();

    const got = (try reopened.getCatalogRecordAlloc(allocator, "index:large")).?;
    defer allocator.free(got);
    try std.testing.expectEqualSlices(u8, large, got);
    try std.testing.expectEqual(@as(?[]u8, null), try reopened.getCatalogRecordAlloc(allocator, "index:gone"));

    const records = try reopened.snapshotCatalogRecordsAlloc(allocator);
    defer NativeFile.freeSnapshotCatalogRecords(allocator, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqualStrings("index:large", records[0].key);
    try std.testing.expectEqualSlices(u8, large, records[0].value);

    const report = try reopened.check();
    try std.testing.expect(report.valid);
}

test "lite native index catalog snapshots live keys without values" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-index-catalog-keys.aflite");
    defer allocator.free(path);

    const large = try allocator.alloc(u8, default_page_size * 2);
    defer allocator.free(large);
    @memset(large, 'x');

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putIndexCatalogRecord("/index/b.tbl", large);
        try file.putIndexCatalogRecord("/index/a.tbl", "small");
        try file.putIndexCatalogRecord("/index/deleted.tbl", "gone");
        try file.deleteIndexCatalogRecord("/index/deleted.tbl");
        try file.putIndexCatalogRecord("/index/a.tbl", "newer");
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();

    const keys = try reopened.snapshotIndexCatalogKeysAlloc(allocator);
    defer NativeFile.freeSnapshotCatalogKeys(allocator, keys);
    try std.testing.expectEqual(@as(usize, 2), keys.len);
    try std.testing.expectEqualStrings("/index/a.tbl", keys[0].key);
    try std.testing.expectEqualStrings("/index/b.tbl", keys[1].key);
}

test "lite native catalog detects corrupted root page" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-catalog-corrupt.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putCatalogRecord("schema", "value");
    }

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    try std.testing.expectError(error.NativePageChecksumMismatch, reopened.getCatalogRecordAlloc(allocator, "schema"));
}

test "lite native document store persists records" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-documents.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "{\"title\":\"one\"}");
        try file.putDocument("doc:2", "{\"title\":\"two\"}");
        try std.testing.expectEqual(@as(u64, 2), file.activeCheckpoint().commit_sequence);
        try std.testing.expect(file.activeCheckpoint().document_root_page != 0);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();

    const doc1 = (try reopened.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(doc1);
    try std.testing.expectEqualStrings("{\"title\":\"one\"}", doc1);

    const doc2 = (try reopened.getDocumentAlloc(allocator, "doc:2")).?;
    defer allocator.free(doc2);
    try std.testing.expectEqualStrings("{\"title\":\"two\"}", doc2);

    try std.testing.expectEqual(@as(?[]u8, null), try reopened.getDocumentAlloc(allocator, "missing"));
}

test "lite native document store returns newest overwrite" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-document-overwrite.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();

    try file.putDocument("doc:1", "old");
    try file.putDocument("doc:1", "new");

    const value = (try file.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(value);
    try std.testing.expectEqualStrings("new", value);
}

test "lite native hot commits remain append only until explicit vacuum" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-free-map-reuse.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();

    try file.putDocument("doc:1", "v1");
    try file.putDocument("doc:1", "v2");
    try file.putDocument("doc:1", "v3");

    const reusable = try file.readFreePagesAlloc(file.activeCheckpoint());
    defer allocator.free(reusable);
    try std.testing.expectEqual(@as(usize, 0), reusable.len);

    const before_size = (try file.file.stat(file.io_impl.io())).size;
    try file.putDocument("doc:1", "v4");
    const after_size = (try file.file.stat(file.io_impl.io())).size;

    // Commits carry forward already-known free pages but never perform a
    // whole-file reachability walk. Explicit vacuum is the bounded place where
    // obsolete history is reclaimed.
    try std.testing.expectEqual(before_size + 4 * default_page_size, after_size);
    const report = try file.check();
    try std.testing.expect(report.valid);

    const value = (try file.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(value);
    try std.testing.expectEqualStrings("v4", value);
}

test "lite native free map does not reuse pages while reader pins older checkpoint" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-free-map-reader-protected.aflite");
    defer allocator.free(path);

    var writer = try NativeFile.create(allocator, path);
    defer writer.close();

    try writer.putDocument("doc:1", "v1");
    try writer.putDocument("doc:1", "v2");
    const before_size = (try writer.file.stat(writer.io_impl.io())).size;

    var reader = try NativeFile.open(allocator, path, true);
    defer reader.close();
    const reader_checkpoint = reader.activeCheckpoint();
    const reader_value_before = (try reader.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(reader_value_before);
    try std.testing.expectEqualStrings("v2", reader_value_before);

    try writer.putDocument("doc:1", "v3");
    try writer.putDocument("doc:1", "v4");
    const after_size = (try writer.file.stat(writer.io_impl.io())).size;
    try std.testing.expect(after_size > before_size + default_page_size);

    try std.testing.expectEqual(reader_checkpoint.commit_sequence, reader.activeCheckpoint().commit_sequence);
    const reader_value_after = (try reader.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(reader_value_after);
    try std.testing.expectEqualStrings("v2", reader_value_after);
    const reader_free_pages = try reader.readFreePagesAlloc(reader.activeCheckpoint());
    defer allocator.free(reader_free_pages);

    const writer_value = (try writer.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(writer_value);
    try std.testing.expectEqualStrings("v4", writer_value);

    const report = try writer.check();
    try std.testing.expect(report.valid);
}

test "lite native stable snapshot preserves pinned reader checkpoint while writer advances" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-pinned-reader-snapshot.aflite");
    defer allocator.free(path);
    const snapshot_path = try testPath(allocator, tmp, "native-pinned-reader-snapshot-copy.aflite");
    defer allocator.free(snapshot_path);

    var writer = try NativeFile.create(allocator, path);
    defer writer.close();

    try writer.putDocument("doc:1", "v1");
    try writer.putDocument("doc:1", "v2");

    var reader = try NativeFile.open(allocator, path, true);
    defer reader.close();
    const reader_checkpoint = reader.activeCheckpoint();
    const reader_value_before = (try reader.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(reader_value_before);
    try std.testing.expectEqualStrings("v2", reader_value_before);

    try writer.putDocument("doc:1", "v3");
    try writer.putDocument("doc:1", "v4");

    const snapshot_report = try reader.copyStableSnapshotToPath(snapshot_path, false);
    try std.testing.expectEqual(reader_checkpoint.commit_sequence, snapshot_report.checkpoint_sequence);
    try std.testing.expect(snapshot_report.tail_bytes > 0);

    const snapshot_check = try checkFile(allocator, snapshot_path);
    try std.testing.expect(snapshot_check.valid);
    try std.testing.expectEqual(@as(u64, 0), snapshot_check.tail_bytes);

    var snapshot = try NativeFile.open(allocator, snapshot_path, true);
    defer snapshot.close();
    try std.testing.expectEqual(reader_checkpoint.commit_sequence, snapshot.activeCheckpoint().commit_sequence);
    const snapshot_value = (try snapshot.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(snapshot_value);
    try std.testing.expectEqualStrings("v2", snapshot_value);

    const writer_value = (try writer.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(writer_value);
    try std.testing.expectEqualStrings("v4", writer_value);
}

test "lite native document store spills large values into value pages" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-document-large.aflite");
    defer allocator.free(path);

    const large = try allocator.alloc(u8, 9000);
    defer allocator.free(large);
    for (large, 0..) |*byte, i| {
        byte.* = @intCast('a' + (i % 26));
    }

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        const value_pages = std.math.divCeil(usize, large.len, file.maxValuePagePayloadBytes()) catch unreachable;
        try file.putDocument("doc:large", large);
        try std.testing.expectEqual(@as(u64, @intCast(5 + value_pages)), file.activeCheckpoint().page_count);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();

    const value = (try reopened.getDocumentAlloc(allocator, "doc:large")).?;
    defer allocator.free(value);
    try std.testing.expectEqualSlices(u8, large, value);

    const docs = try reopened.snapshotDocumentsAlloc(allocator);
    defer NativeFile.freeSnapshotDocuments(allocator, docs);
    try std.testing.expectEqual(@as(usize, 1), docs.len);
    try std.testing.expectEqualStrings("doc:large", docs[0].key);
    try std.testing.expectEqualSlices(u8, large, docs[0].value);

    const report = try reopened.check();
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u64, 1), report.record_count);
}

test "lite native document tombstone hides older value after reopen" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-document-delete.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "old");
        try file.deleteDocument("doc:1");
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    try std.testing.expectEqual(@as(?[]u8, null), try reopened.getDocumentAlloc(allocator, "doc:1"));
}

test "lite native document store detects corrupted root page" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-document-corrupt.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "value");
    }

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    try std.testing.expectError(error.NativePageChecksumMismatch, reopened.getDocumentAlloc(allocator, "doc:1"));
}

test "lite native document store detects corrupted external value page" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-document-large-corrupt.aflite");
    defer allocator.free(path);

    const large = try allocator.alloc(u8, 9000);
    defer allocator.free(large);
    @memset(large, 'x');

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:large", large);
    }

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    try std.testing.expectError(error.NativePageChecksumMismatch, reopened.getDocumentAlloc(allocator, "doc:large"));

    const report = try reopened.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("page_checksum_mismatch", report.issue.?);
}

test "lite native document batch publishes one checkpoint" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-document-batch.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocumentBatch(&.{
            .{ .key = "doc:b", .value = "second" },
            .{ .key = "doc:a", .value = "first" },
            .{ .key = "doc:b", .value = "newer second" },
            .{ .key = "doc:c", .value = "deleted" },
            .{ .key = "doc:c", .is_delete = true },
        });
        try std.testing.expectEqual(@as(u64, 1), file.activeCheckpoint().commit_sequence);
        try std.testing.expectEqual(@as(u64, 5), file.activeCheckpoint().page_count);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();

    const doc_a = (try reopened.getDocumentAlloc(allocator, "doc:a")).?;
    defer allocator.free(doc_a);
    try std.testing.expectEqualStrings("first", doc_a);

    const doc_b = (try reopened.getDocumentAlloc(allocator, "doc:b")).?;
    defer allocator.free(doc_b);
    try std.testing.expectEqualStrings("newer second", doc_b);

    try std.testing.expectEqual(@as(?[]u8, null), try reopened.getDocumentAlloc(allocator, "doc:c"));
}

test "lite native document snapshot returns sorted live records" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-document-snapshot.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();

    try file.putDocumentBatch(&.{
        .{ .key = "doc:b", .value = "second" },
        .{ .key = "doc:a", .value = "first" },
        .{ .key = "doc:b", .value = "newer second" },
        .{ .key = "doc:c", .value = "third" },
        .{ .key = "doc:c", .is_delete = true },
    });

    const docs = try file.snapshotDocumentsAlloc(allocator);
    defer NativeFile.freeSnapshotDocuments(allocator, docs);

    try std.testing.expectEqual(@as(usize, 2), docs.len);
    try std.testing.expectEqualStrings("doc:a", docs[0].key);
    try std.testing.expectEqualStrings("first", docs[0].value);
    try std.testing.expectEqualStrings("doc:b", docs[1].key);
    try std.testing.expectEqualStrings("newer second", docs[1].value);
}

test "lite native namespace snapshot does not read unrelated document chains" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-namespace-index.aflite");
    defer allocator.free(path);
    const prefix_a = [_]u8{ 't', 'a', 0 };
    const prefix_b = [_]u8{ 't', 'b', 0 };
    const key_a = prefix_a ++ "doc:a".*;
    const key_b = prefix_b ++ "doc:b".*;

    var a_head: u64 = 0;
    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument(&key_a, "a");
        try file.putDocument(&key_b, "b");
        var directory = (try file.loadNamespaceDirectoryAlloc(allocator)).?;
        defer NativeFile.deinitNamespaceDirectory(allocator, &directory);
        a_head = directory.get(&prefix_a).?;
        const user_catalog = try file.snapshotCatalogRecordsAlloc(allocator);
        defer NativeFile.freeSnapshotCatalogRecords(allocator, user_catalog);
        try std.testing.expectEqual(@as(usize, 0), user_catalog.len);
    }
    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", a_head * default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    const docs_b = try reopened.snapshotDocumentsWithPrefixAlloc(allocator, &prefix_b);
    defer NativeFile.freeSnapshotDocuments(allocator, docs_b);
    try std.testing.expectEqual(@as(usize, 1), docs_b.len);
    try std.testing.expectEqualStrings("b", docs_b[0].value);
    try std.testing.expectError(error.NativePageChecksumMismatch, reopened.snapshotDocumentsAlloc(allocator));
}

test "lite native small namespace directory stays inline and survives cold reopen" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-namespace-deltas.aflite");
    defer allocator.free(path);
    const prefix_a = [_]u8{ 't', 'a', 0 };
    const prefix_b = [_]u8{ 't', 'b', 0 };

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        var value_buf: [32]u8 = undefined;
        var key_buf: [64]u8 = undefined;
        for (0..300) |i| {
            const prefix = if (i % 2 == 0) &prefix_a else &prefix_b;
            const key_tail = try std.fmt.bufPrint(&key_buf, "doc-{d}", .{i});
            var key = std.ArrayListUnmanaged(u8).empty;
            defer key.deinit(allocator);
            try key.appendSlice(allocator, prefix);
            try key.appendSlice(allocator, key_tail);
            const value = try std.fmt.bufPrint(&value_buf, "value-{d}", .{i});
            try file.putDocument(key.items, value);
        }
        try std.testing.expect(file.namespace_directory_delta_depth < namespace_directory_snapshot_interval);
        var reachable = std.AutoHashMapUnmanaged(u64, void){};
        defer reachable.deinit(allocator);
        const directory_pages = try file.countReachableChainPages(
            .catalog,
            file.activeCheckpoint().namespace_directory_root_page,
            &reachable,
        );
        try std.testing.expectEqual(@as(u64, 1), directory_pages);
        try std.testing.expect((try file.check()).valid);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    const docs_a = try reopened.snapshotDocumentsWithPrefixAlloc(allocator, &prefix_a);
    defer NativeFile.freeSnapshotDocuments(allocator, docs_a);
    const docs_b = try reopened.snapshotDocumentsWithPrefixAlloc(allocator, &prefix_b);
    defer NativeFile.freeSnapshotDocuments(allocator, docs_b);
    try std.testing.expectEqual(@as(usize, 150), docs_a.len);
    try std.testing.expectEqual(@as(usize, 150), docs_b.len);
}

test "lite native check rejects incomplete namespace links with valid page checksums" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-namespace-link-check.aflite");
    defer allocator.free(path);
    const prefix = [_]u8{ 't', 0 };
    const first_key = prefix ++ "first".*;
    const second_key = prefix ++ "second".*;

    var file = try NativeFile.create(allocator, path);
    defer file.close();
    try file.putDocument(&first_key, "one");
    try file.putDocument(&second_key, "two");

    const head = file.activeCheckpoint().document_root_page;
    const payload = try file.readPagePayloadByKindAlloc(allocator, head, .document);
    defer allocator.free(payload);
    const entry = try decodeDocumentEntry(payload);
    var rewritten = std.ArrayListUnmanaged(u8).empty;
    defer rewritten.deinit(allocator);
    try encodeDocumentEntry(allocator, &rewritten, .{
        .previous_page = entry.previous_page,
        .previous_namespace_page = 0,
        .key = entry.key,
        .value = entry.value,
        .is_delete = entry.is_delete,
        .external_value_root_page = entry.external_value_root_page,
    });
    try file.writePage(head, .document, rewritten.items);

    const report = try file.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("invalid_namespace_directory", report.issue.?);
}

test "lite native check validates committed root chains" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();

    try file.putCatalogRecord("schema", "{\"version\":1}");
    try file.putDocument("doc:1", "{\"title\":\"one\"}");

    const report = try file.check();
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(?[]const u8, null), report.issue);
    try std.testing.expectEqual(@as(u64, 2), report.record_count);
    try std.testing.expectEqual(@as(u64, 2), report.live_file_count);
    try std.testing.expect(report.live_bytes > 0);
    try std.testing.expectEqual(@as(u64, default_page_size * 9), report.file_size);
    try std.testing.expectEqual(@as(u64, default_page_size * 8), report.compact_size);
    try std.testing.expectEqual(@as(u64, 0), report.tail_bytes);
    try std.testing.expectEqual(@as(u64, default_page_size), report.reclaimable_bytes);
}

test "lite native check validates committed index catalog root chain" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-index-catalog.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();

    try file.putCatalogRecord("schema", "{\"version\":1}");
    try file.putIndexCatalogRecord("index/files/hbc/postings.bin", "index bytes");
    try file.putDocument("doc:1", "{\"title\":\"one\"}");

    const checkpoint = file.activeCheckpoint();
    try std.testing.expect(checkpoint.catalog_root_page != 0);
    try std.testing.expect(checkpoint.index_catalog_root_page != 0);
    try std.testing.expect(checkpoint.document_root_page != 0);

    const report = try file.check();
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(?[]const u8, null), report.issue);
    try std.testing.expectEqual(@as(u64, 3), report.record_count);
    try std.testing.expectEqual(@as(u64, 3), report.live_file_count);
    try std.testing.expect(report.live_bytes > 0);
    try std.testing.expectEqual(@as(u64, default_page_size * 13), report.file_size);
    try std.testing.expectEqual(@as(u64, default_page_size * 11), report.compact_size);
    try std.testing.expectEqual(@as(u64, default_page_size * 2), report.reclaimable_bytes);

    const index_file = (try file.getIndexCatalogRecordAlloc(allocator, "index/files/hbc/postings.bin")).?;
    defer allocator.free(index_file);
    try std.testing.expectEqualStrings("index bytes", index_file);
}

test "lite native check reports overlapping committed root pages" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-overlap.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();

        try file.putCatalogRecord("schema", "{\"version\":1}");
        try file.putIndexCatalogRecord("index/files/hbc/postings.bin", "index bytes");

        const checkpoint = file.activeCheckpoint();
        try std.testing.expect(checkpoint.catalog_root_page != 0);
        try std.testing.expect(checkpoint.index_catalog_root_page != 0);
        try std.testing.expect(checkpoint.catalog_root_page != checkpoint.index_catalog_root_page);
    }

    var header_bytes = try readHeaderForTest(path);
    var header = try decodeHeader(&header_bytes);
    header.checkpoints[header.active_checkpoint].index_catalog_root_page =
        header.checkpoints[header.active_checkpoint].catalog_root_page;
    encodeHeader(&header_bytes, header);

    {
        var raw = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer raw.close(std.testing.io);
        try raw.writePositionalAll(std.testing.io, &header_bytes, 0);
        try raw.sync(std.testing.io);
    }

    const report = try checkFile(allocator, path);
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("invalid_page_chain", report.issue.?);
}

test "lite native stable snapshot copies committed prefix without tail bytes" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const source_path = try testPath(allocator, tmp, "native-snapshot-source.aflite");
    defer allocator.free(source_path);
    const snapshot_path = try testPath(allocator, tmp, "native-snapshot-copy.aflite");
    defer allocator.free(snapshot_path);

    const snapshot_size = blk: {
        var file = try NativeFile.create(allocator, source_path);
        defer file.close();
        try file.putCatalogRecord("schema", "{\"version\":1}");
        try file.putIndexCatalogRecord("index/files/hbc/postings.bin", "index bytes");
        try file.putDocument("doc:1", "{\"title\":\"one\"}");
        const checkpoint = file.activeCheckpoint();
        try std.testing.expect(checkpoint.free_map_root_page != 0);
        break :blk try checkpointPrefixSize(checkpoint, file.header.page_size);
    };

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, source_path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "uncommitted tail", snapshot_size);
    }

    const source_report = try checkFile(allocator, source_path);
    try std.testing.expect(!source_report.valid);
    try std.testing.expectEqualStrings("tail_bytes", source_report.issue.?);
    try std.testing.expect(source_report.tail_bytes > 0);

    const snapshot_report = try copyStableSnapshot(allocator, source_path, snapshot_path, false);
    try std.testing.expectEqual(snapshot_size, snapshot_report.snapshot_size);
    try std.testing.expectEqual(@as(u64, "uncommitted tail".len), snapshot_report.tail_bytes);

    const clean_report = try checkFile(allocator, snapshot_path);
    try std.testing.expect(clean_report.valid);
    try std.testing.expectEqual(@as(?[]const u8, null), clean_report.issue);
    try std.testing.expectEqual(@as(u64, 0), clean_report.tail_bytes);
    try std.testing.expectEqual(snapshot_report.snapshot_size, clean_report.file_size);

    var reopened = try NativeFile.open(allocator, snapshot_path, true);
    defer reopened.close();
    try std.testing.expect(reopened.activeCheckpoint().free_map_root_page != 0);

    const schema = (try reopened.getCatalogRecordAlloc(allocator, "schema")).?;
    defer allocator.free(schema);
    try std.testing.expectEqualStrings("{\"version\":1}", schema);

    const index_file = (try reopened.getIndexCatalogRecordAlloc(allocator, "index/files/hbc/postings.bin")).?;
    defer allocator.free(index_file);
    try std.testing.expectEqualStrings("index bytes", index_file);

    const doc = (try reopened.getDocumentAlloc(allocator, "doc:1")).?;
    defer allocator.free(doc);
    try std.testing.expectEqualStrings("{\"title\":\"one\"}", doc);
}

test "lite native stable snapshot rejects same target by canonical path" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const path = try testPath(allocator, tmp, "native-snapshot-self.aflite");
    defer allocator.free(path);
    const nested_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/nested", .{tmp.sub_path});
    defer allocator.free(nested_dir);
    const alias_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/nested/../native-snapshot-self.aflite", .{tmp.sub_path});
    defer allocator.free(alias_path);

    try fs_paths.createDirPathPortable(io, nested_dir);
    {
        var writer = try NativeFile.create(allocator, path);
        defer writer.close();
        try writer.putDocument("doc:self", "{\"title\":\"same target\"}");
    }

    var reader = try NativeFile.open(allocator, path, true);
    defer reader.close();
    try std.testing.expectError(error.InvalidNativeSnapshotPath, reader.copyStableSnapshotToPath(alias_path, true));

    const report = try checkFile(allocator, path);
    try std.testing.expect(report.valid);
}

test "lite native stable snapshot holds output writer lock before staging" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const source_path = try testPath(allocator, tmp, "native-snapshot-lock-source.aflite");
    defer allocator.free(source_path);
    const snapshot_path = try testPath(allocator, tmp, "native-snapshot-lock-copy.aflite");
    defer allocator.free(snapshot_path);
    const tmp_path = try std.fmt.allocPrint(allocator, "{s}.tmp-aflite-snapshot", .{snapshot_path});
    defer allocator.free(tmp_path);

    {
        var file = try NativeFile.create(allocator, source_path);
        defer file.close();
        try file.putDocument("doc:visible", "visible");
    }

    var dest_lock = try lockWriterPath(allocator, snapshot_path);
    defer dest_lock.close();

    try std.testing.expectError(error.WouldBlock, copyStableSnapshot(allocator, source_path, snapshot_path, false));
    try std.testing.expect(!pathExists(std.testing.io, snapshot_path));
    try std.testing.expect(!pathExists(std.testing.io, tmp_path));
}

test "lite vacuum image cleanup removes prepared file despite pending cancellation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/vacuum-cleanup.aflite", .{tmp.sub_path});
    defer alloc.free(path);
    var pool = std.Io.Threaded.init(alloc, .{ .concurrent_limit = .limited(4) });
    defer pool.deinit();
    const io = pool.io();
    var file = try NativeFile.createWithIo(alloc, io, path, .{ .no_sync = true });
    defer file.close();
    try file.putDocument("doc", "retained");
    var image = try file.prepareVacuum(null);
    var image_owned = true;
    defer if (image_owned) image.deinit();
    const prepared_path = try alloc.dupe(u8, image.prepared.path);
    defer alloc.free(prepared_path);
    const State = struct {
        started: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        fn run(i: std.Io, self: *@This(), prepared: *VacuumImage) void {
            self.started.set(i);
            // Re-arm the gate's cancellation before exercising cleanup.
            self.release.wait(i) catch i.recancel();
            prepared.deinit();
        }
    };
    var state: State = .{};
    var child = try io.concurrent(State.run, .{ io, &state, &image });
    image_owned = false; // The child owns cleanup after successful dispatch.
    state.started.waitUncancelable(io);
    child.cancel(io);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, prepared_path, .{}));
    const value = (try file.getDocumentAlloc(alloc, "doc")).?;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("retained", value);
}

test "lite native vacuum rewrites live catalog and document records" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-vacuum.aflite");
    defer allocator.free(path);

    const large_value = try allocator.alloc(u8, default_page_size * 2);
    defer allocator.free(large_value);
    @memset(large_value, 'x');

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();

        try file.putCatalogRecord("schema", "{\"version\":1}");
        try file.putCatalogRecord("schema", "{\"version\":2}");
        try file.putDocument("doc:live", large_value);
        try file.putDocument("doc:live", "small");
        try file.putDocument("doc:gone", "deleted");
        try file.deleteDocument("doc:gone");

        const before = try file.check();
        try std.testing.expect(before.record_count > 2);
        try std.testing.expectEqual(@as(u64, 2), before.live_file_count);
        try std.testing.expect(before.live_bytes > 0);
        try std.testing.expect(before.compact_size < before.file_size);
        try std.testing.expect(before.reclaimable_bytes > 0);

        const vacuumed = try file.vacuum();
        try std.testing.expect(vacuumed.before_size > vacuumed.after_size);
        try std.testing.expect(vacuumed.reclaimed_bytes > 0);
        try std.testing.expect(file.activeCheckpoint().free_map_root_page != 0);

        const after = try file.check();
        try std.testing.expect(after.valid);
        try std.testing.expectEqual(vacuumed.after_size, after.file_size);
        try std.testing.expectEqual(@as(u64, 2), after.record_count);
        try std.testing.expectEqual(@as(u64, 2), after.live_file_count);
        try std.testing.expectEqual(after.file_size, after.compact_size);
        try std.testing.expectEqual(@as(u64, 0), after.reclaimable_bytes);

        const schema = (try file.getCatalogRecordAlloc(allocator, "schema")).?;
        defer allocator.free(schema);
        try std.testing.expectEqualStrings("{\"version\":2}", schema);

        const live = (try file.getDocumentAlloc(allocator, "doc:live")).?;
        defer allocator.free(live);
        try std.testing.expectEqualStrings("small", live);
        try std.testing.expectEqual(@as(?[]u8, null), try file.getDocumentAlloc(allocator, "doc:gone"));
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();

    const report = try reopened.check();
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u64, 2), report.record_count);

    const schema = (try reopened.getCatalogRecordAlloc(allocator, "schema")).?;
    defer allocator.free(schema);
    try std.testing.expectEqualStrings("{\"version\":2}", schema);

    const live = (try reopened.getDocumentAlloc(allocator, "doc:live")).?;
    defer allocator.free(live);
    try std.testing.expectEqualStrings("small", live);
}

test "lite native vacuum atomically replaces file and keeps writer handle usable" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-vacuum-replace.aflite");
    defer allocator.free(path);
    const tmp_path = try std.fmt.allocPrint(allocator, "{s}.tmp-aflite-vacuum", .{path});
    defer allocator.free(tmp_path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();

        try file.putDocument("doc:live", "old");
        try file.putDocument("doc:live", "new");

        const vacuumed = try file.vacuum();
        try std.testing.expect(vacuumed.reclaimed_bytes > 0);
        try std.testing.expect(!pathExists(file.io_impl.io(), tmp_path));

        try file.putDocument("doc:after-vacuum", "writer still attached");
        const after = (try file.getDocumentAlloc(allocator, "doc:after-vacuum")).?;
        defer allocator.free(after);
        try std.testing.expectEqualStrings("writer still attached", after);

        const report = try file.check();
        try std.testing.expect(report.valid);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();

    const live = (try reopened.getDocumentAlloc(allocator, "doc:live")).?;
    defer allocator.free(live);
    try std.testing.expectEqualStrings("new", live);
    const after = (try reopened.getDocumentAlloc(allocator, "doc:after-vacuum")).?;
    defer allocator.free(after);
    try std.testing.expectEqualStrings("writer still attached", after);
}

test "lite native vacuum keeps adopted replacement usable after post rename failure" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-vacuum-post-rename-failure.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:live", "old");
        try file.putDocument("doc:live", "new");
        file.test_fail_vacuum_after_adoption = true;
        try std.testing.expectError(error.InjectedVacuumPostRenameFailure, file.vacuum());

        // The failure is reported, but the process must never continue on the
        // unlinked pre-vacuum inode.
        try file.putDocument("doc:after", "durable on adopted file");
        const current = (try file.getDocumentAlloc(allocator, "doc:after")).?;
        defer allocator.free(current);
        try std.testing.expectEqualStrings("durable on adopted file", current);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    const persisted = (try reopened.getDocumentAlloc(allocator, "doc:after")).?;
    defer allocator.free(persisted);
    try std.testing.expectEqualStrings("durable on adopted file", persisted);
}

test "lite native check validates committed free map root" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-free-map-corrupt.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();

    try file.putDocument("doc:1", "value");
    _ = try file.vacuum();

    const checkpoint = file.activeCheckpoint();
    try std.testing.expect(checkpoint.free_map_root_page != 0);

    const payload = try encodeFreeMapAlloc(allocator, default_page_size, checkpoint.page_count + 1, &.{});
    defer allocator.free(payload);
    var page: [default_page_size]u8 = undefined;
    encodePage(&page, .free_map, payload);
    try file.file.writePositionalAll(file.io_impl.io(), &page, checkpoint.free_map_root_page * default_page_size);
    try file.file.sync(file.io_impl.io());

    const report = try file.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("invalid_free_map", report.issue.?);
}

test "lite native free map reads are bounded by supplied checkpoint" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-free-map-checkpoint-bound.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();

    try file.putDocument("doc:1", "value");

    var checkpoint = file.activeCheckpoint();
    try std.testing.expect(checkpoint.free_map_root_page != 0);
    checkpoint.page_count = checkpoint.free_map_root_page;

    try std.testing.expectError(error.InvalidPageId, file.readFreePagesAlloc(checkpoint));

    var reachable_pages = std.AutoHashMapUnmanaged(u64, void){};
    defer reachable_pages.deinit(allocator);
    try std.testing.expectError(error.InvalidPageId, file.validateReachableFreeMap(checkpoint, &reachable_pages));
}

test "lite native unchanged small index records do not publish checkpoints" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "catalog-noop.aflite");
    defer allocator.free(path);
    var file = try NativeFile.create(allocator, path);
    defer file.close();
    try file.putIndexCatalogRecord("control", "state");
    const before = file.activeCheckpoint();
    for (0..8) |_| {
        try file.putIndexCatalogRecord("control", "state");
        try file.deleteIndexCatalogRecord("absent");
    }
    try std.testing.expectEqualDeep(before, file.activeCheckpoint());
    // Equal length is insufficient: changed bytes must still be published.
    try file.putIndexCatalogRecord("control", "other");
    try std.testing.expectEqual(before.commit_sequence + 1, file.activeCheckpoint().commit_sequence);
    try file.deleteIndexCatalogRecord("control");
    const deleted = file.activeCheckpoint();
    try file.deleteIndexCatalogRecord("control");
    try std.testing.expectEqualDeep(deleted, file.activeCheckpoint());
    try file.putIndexCatalogRecord("control", "other");
    const restored = file.activeCheckpoint();
    // An ambiguous publication cannot use the old header as a no-op proof.
    file.checkpoint_publication_uncertain = true;
    try file.putIndexCatalogRecord("control", "other");
    try std.testing.expectEqual(restored.commit_sequence + 1, file.activeCheckpoint().commit_sequence);
    try std.testing.expect(!file.checkpoint_publication_uncertain);
    file.page_cache.clear(allocator);
    const cold = file.activeCheckpoint();
    try file.putIndexCatalogRecord("control", "other");
    try std.testing.expectEqualDeep(cold, file.activeCheckpoint());
    var reader = try NativeFile.open(allocator, path, true);
    defer reader.close();
    const value = (try reader.getIndexCatalogRecordAlloc(allocator, "control")).?;
    defer allocator.free(value);
    try std.testing.expectEqualStrings("other", value);
    try std.testing.expectError(error.ReadOnly, reader.putIndexCatalogRecord("control", "other"));
}

test "lite native warm catalog point lookups skip history without allocating" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "catalog-probes.aflite");
    defer allocator.free(path);
    var file = try NativeFile.create(allocator, path);
    defer file.close();
    try file.putIndexCatalogRecord("target", "original");
    const pinned = file.activeCheckpoint();
    for (0..32) |i| try file.putIndexCatalogRecord("noise", std.mem.asBytes(&i));
    try file.putIndexCatalogRecord("deleted", "value");
    try file.deleteIndexCatalogRecord("deleted");

    // Cold admission builds the retained slot view once. Warm probes must
    // allocate neither traversal scratch nor another decoded key array.
    _ = try file.getCatalogRecordSizeFromRootAtCheckpoint(.index, "target", file.activeCheckpoint());
    _ = try file.getCatalogRecordSizeFromRootAtCheckpoint(.index, "target", pinned);

    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    file.allocator = failing.allocator();
    {
        defer file.allocator = allocator;
        try std.testing.expectEqual(@as(?usize, 8), try file.getCatalogRecordSizeFromRootAtCheckpoint(.index, "target", file.activeCheckpoint()));
        try std.testing.expectEqual(@as(?usize, null), try file.getCatalogRecordSizeFromRootAtCheckpoint(.index, "missing", file.activeCheckpoint()));
        try std.testing.expectEqual(@as(?usize, null), try file.getCatalogRecordSizeFromRootAtCheckpoint(.index, "deleted", file.activeCheckpoint()));
        const value = (try file.getIndexCatalogRecordAtCheckpointAlloc(allocator, "target", pinned)).?;
        defer allocator.free(value);
        try std.testing.expectEqualStrings("original", value);
        try std.testing.expect(!failing.has_induced_failure);
        file.allocator = allocator;
        const range = (try file.getCatalogRecordRangeFromRootAtCheckpointAlloc(allocator, .index, "target", 1, 3, file.activeCheckpoint())).?;
        defer allocator.free(range);
        try std.testing.expectEqualStrings("rig", range);
        try std.testing.expect(!failing.has_induced_failure);
    }
    try file.putIndexCatalogRecord("target", "new");
    try std.testing.expectEqual(@as(?usize, 3), try file.getCatalogRecordSizeFromRootAtCheckpoint(.index, "target", file.activeCheckpoint()));
    try std.testing.expectEqual(@as(?usize, 8), try file.getCatalogRecordSizeFromRootAtCheckpoint(.index, "target", pinned));
    file.page_cache.clear(allocator);
    try std.testing.expectEqual(@as(?usize, 3), try file.getCatalogRecordSizeFromRootAtCheckpoint(.index, "target", file.activeCheckpoint()));
    file.page_cache_enabled.store(false, .monotonic);
    try std.testing.expectEqual(@as(?usize, 8), try file.getCatalogRecordSizeFromRootAtCheckpoint(.index, "target", pinned));
}

test "lite native empty free map validation does not allocate or walk checkpoints" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-empty-free-map.aflite");
    defer allocator.free(path);
    var file = try NativeFile.create(allocator, path);
    defer file.close();
    try file.putDocument("doc:1", "v1");
    try file.putDocument("doc:1", "v2");

    // Any reachability walk requires scratch allocation, even with warm pages.
    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    file.allocator = failing.allocator();
    defer file.allocator = allocator;
    try file.validateFreePagesSafeForCheckpointSlots(&.{});
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    try std.testing.expect(!failing.has_induced_failure);
}

test "lite native free map cannot reclaim previous checkpoint pages" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-free-map-previous-protected.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();

        try file.putDocument("doc:1", "v1");
        try file.putDocument("doc:1", "v2");

        const active = file.activeCheckpoint();
        const previous = file.header.checkpoints[if (file.header.active_checkpoint == 0) 1 else 0];
        try std.testing.expect(active.free_map_root_page != 0);
        try std.testing.expect(previous.free_map_root_page != 0);
        try std.testing.expect(active.free_map_root_page != previous.free_map_root_page);

        const payload = try encodeFreeMapAlloc(allocator, default_page_size, active.page_count, &.{previous.free_map_root_page});
        defer allocator.free(payload);
        var page: [default_page_size]u8 = undefined;
        encodePage(&page, .free_map, payload);
        try file.file.writePositionalAll(file.io_impl.io(), &page, active.free_map_root_page * default_page_size);
        try file.file.sync(file.io_impl.io());

        const report = try file.check();
        try std.testing.expect(!report.valid);
        try std.testing.expectEqualStrings("invalid_free_map", report.issue.?);
    }

    // The free-map-vs-fallback-checkpoint cross-check only needs to run once
    // per open handle: this process's own commits can only ever reuse pages
    // that a previously verified free map already declared free, so
    // re-scanning every checkpoint slot's full reachable set on every single
    // mutation would make ingest quadratic in the number of commits.
    // Corruption written out of band (as above) is instead caught the next
    // time the file is opened and its free map is trusted again.
    var reopened = try NativeFile.open(allocator, path, false);
    defer reopened.close();
    try std.testing.expectError(error.InvalidNativeFreeMap, reopened.putDocument("doc:1", "v3"));
}

test "lite native check reports corrupted committed document page" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-corrupt.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "value");
    }

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    const report = try reopened.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("page_checksum_mismatch", report.issue.?);
}

test "lite native check reports corrupted committed document index page" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-document-index-corrupt.aflite");
    defer allocator.free(path);

    const root_page = blk: {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "value");
        break :blk file.activeCheckpoint().document_index_root_page;
    };

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", root_page * default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    const report = try reopened.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("page_checksum_mismatch", report.issue.?);
}

test "lite native check rejects a structurally valid stale document index pointer" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-document-index-stale.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();
    try file.putDocument("doc:1", "old");
    try file.putDocument("doc:1", "new");

    const checkpoint = file.activeCheckpoint();
    const newest_payload = try file.readPagePayloadByKindAllocForCheckpoint(allocator, checkpoint.document_root_page, .document, checkpoint);
    defer allocator.free(newest_payload);
    const newest = try decodeDocumentEntry(newest_payload);
    try std.testing.expect(newest.previous_page != 0);

    var index_node = try file.readDocumentIndexNode(checkpoint.document_index_root_page, checkpoint);
    defer index_node.deinit(allocator);
    try std.testing.expectEqual(DocumentIndexNodeKind.leaf, index_node.kind);
    try std.testing.expectEqual(@as(usize, 1), index_node.pointers.len);
    index_node.pointers[0] = newest.previous_page;
    const encoded = try encodeDocumentIndexNode(allocator, index_node);
    defer allocator.free(encoded);
    var page: [default_page_size]u8 = undefined;
    encodePage(&page, .document_index, encoded);
    try file.file.writePositionalAll(file.io_impl.io(), &page, checkpoint.document_index_root_page * default_page_size);
    file.page_cache.clear(allocator);

    const report = try file.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("invalid_document_index", report.issue.?);
}

test "lite native check reports corrupted committed index catalog page" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-index-corrupt.aflite");
    defer allocator.free(path);

    const root_page = blk: {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putIndexCatalogRecord("index/files/hbc/postings.bin", "index bytes");
        break :blk file.activeCheckpoint().index_catalog_root_page;
    };

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", root_page * default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    const report = try reopened.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("page_checksum_mismatch", report.issue.?);
}

test "lite native check reports corrupted index catalog external value page" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-index-value-corrupt.aflite");
    defer allocator.free(path);

    const large_value = try allocator.alloc(u8, default_page_size * 2);
    defer allocator.free(large_value);
    @memset(large_value, 'i');

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putIndexCatalogRecord("index/files/hbc/postings.bin", large_value);
    }

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", default_page_size + page_header_size);
    }

    var reopened = try NativeFile.open(allocator, path, true);
    defer reopened.close();
    const report = try reopened.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("page_checksum_mismatch", report.issue.?);
}

test "lite native check reports truncated committed file" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-truncated.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "value");
    }

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.setLength(std.testing.io, default_page_size + 16);
    }

    {
        var reopened = try NativeFile.open(allocator, path, true);
        defer reopened.close();
        try std.testing.expectEqual(@as(u64, 0), reopened.activeCheckpoint().commit_sequence);
        try std.testing.expectEqual(@as(?[]u8, null), try reopened.getDocumentAlloc(allocator, "doc:1"));
    }

    const report = try checkFile(allocator, path);
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("tail_bytes", report.issue.?);
}

test "lite native checkFile reports corrupted header" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-header-corrupt.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "value");
    }

    {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "X", page_size_offset);
    }

    const report = try checkFile(allocator, path);
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("header_checksum_mismatch", report.issue.?);
}

test "lite native checkFile reports invalid checkpoint metadata separately from truncation" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-invalid-checkpoint.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "value");
    }

    var header_bytes = try readHeaderForTest(path);
    var header = try decodeHeader(&header_bytes);
    for (&header.checkpoints) |*slot| {
        slot.catalog_root_page = slot.page_count;
        slot.document_root_page = slot.page_count;
        slot.index_catalog_root_page = slot.page_count;
        slot.free_map_root_page = slot.page_count;
    }
    encodeHeader(&header_bytes, header);

    {
        var raw = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer raw.close(std.testing.io);
        try raw.writePositionalAll(std.testing.io, &header_bytes, 0);
        try raw.sync(std.testing.io);
    }

    const report = try checkFile(allocator, path);
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("invalid_checkpoint", report.issue.?);
}

test "lite native checkFile reports checkpoint prefix overflow as invalid metadata" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-check-checkpoint-overflow.aflite");
    defer allocator.free(path);

    {
        var file = try NativeFile.create(allocator, path);
        defer file.close();
        try file.putDocument("doc:1", "value");
    }

    var header_bytes = try readHeaderForTest(path);
    var header = try decodeHeader(&header_bytes);
    for (&header.checkpoints, 0..) |*slot, index| {
        slot.* = .{
            .commit_sequence = @as(u64, @intCast(index + 1)),
            .catalog_root_page = 0,
            .document_root_page = 0,
            .index_catalog_root_page = 0,
            .free_map_root_page = 0,
            .page_count = std.math.maxInt(u64),
        };
    }
    encodeHeader(&header_bytes, header);

    {
        var raw = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
        defer raw.close(std.testing.io);
        try raw.writePositionalAll(std.testing.io, &header_bytes, 0);
        try raw.sync(std.testing.io);
    }

    const report = try checkFile(allocator, path);
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("invalid_checkpoint", report.issue.?);
    try std.testing.expectEqual(@as(u64, 0), report.record_count);
    try std.testing.expectEqual(@as(u64, 0), report.compact_size);
}

test "lite native page cache tracks put remove and incremental eviction" {
    const allocator = std.testing.allocator;

    var cache = PageCache{ .limit_bytes = 32 };
    defer cache.deinit(allocator);

    cache.put(allocator, 1, "0123456789ab");
    cache.put(allocator, 2, "0123456789ab");
    try std.testing.expectEqual(@as(usize, 24), cache.total_bytes);

    const hit = (try cache.getCopy(allocator, 1)) orelse return error.TestUnexpectedResult;
    defer allocator.free(hit);
    try std.testing.expectEqualStrings("0123456789ab", hit);
    try std.testing.expectEqual(@as(?[]u8, null), try cache.getCopy(allocator, 3));

    cache.put(allocator, 1, "ba9876543210");
    try std.testing.expectEqual(@as(usize, 24), cache.total_bytes);
    const replaced = (try cache.getCopy(allocator, 1)) orelse return error.TestUnexpectedResult;
    defer allocator.free(replaced);
    try std.testing.expectEqualStrings("ba9876543210", replaced);

    cache.remove(allocator, 2);
    try std.testing.expectEqual(@as(usize, 12), cache.total_bytes);
    try std.testing.expectEqual(@as(?[]u8, null), try cache.getCopy(allocator, 2));

    // A full-capacity incoming page necessarily replaces every resident page.
    cache.put(allocator, 4, "0123456789ab");
    cache.put(allocator, 5, "0123456789abcdefghijklmnopqrstuv");
    try std.testing.expectEqual(@as(usize, 32), cache.total_bytes);
    try std.testing.expectEqual(@as(?[]u8, null), try cache.getCopy(allocator, 1));
    try std.testing.expectEqual(@as(?[]u8, null), try cache.getCopy(allocator, 4));
    const survivor = (try cache.getCopy(allocator, 5)) orelse return error.TestUnexpectedResult;
    defer allocator.free(survivor);
    try std.testing.expectEqual(@as(usize, 32), survivor.len);
}

test "lite native page and link caches report usage to resource manager" {
    const allocator = std.testing.allocator;

    var manager = resource_manager_mod.ResourceManager.init(.{});
    var cache = PageCache{ .limit_bytes = 128, .link_limit_bytes = 256 };
    defer cache.deinit(allocator);
    cache.attachResourceManager(&manager);

    cache.put(allocator, 1, "0123456789ab");
    var page_stats = manager.sliceStats(.lite_native_page_cache);
    try std.testing.expectEqual(@as(u64, 12), page_stats.used_bytes);

    cache.putLinks(allocator, 1, .{
        .kind = .document,
        .link_page = 0,
        .external_value_root_page = 7,
        .external_value_len = 128,
    });
    var link_stats = manager.sliceStats(.lite_native_link_cache);
    try std.testing.expect(link_stats.used_bytes > 0);

    cache.remove(allocator, 1);
    page_stats = manager.sliceStats(.lite_native_page_cache);
    link_stats = manager.sliceStats(.lite_native_link_cache);
    try std.testing.expectEqual(@as(u64, 0), page_stats.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), link_stats.used_bytes);
}

test "lite native page and link caches shrink under hard resource pressure" {
    const allocator = std.testing.allocator;

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.lite_native_page_cache)] = .{
        .soft_limit_bytes = 4,
        .hard_limit_bytes = 8,
    };
    budgets[@backingInt(resource_manager_mod.Slice.lite_native_link_cache)] = .{
        .soft_limit_bytes = 4,
        .hard_limit_bytes = 8,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var cache = PageCache{ .limit_bytes = 128, .link_limit_bytes = 256 };
    defer cache.deinit(allocator);
    cache.attachResourceManager(&manager);

    cache.put(allocator, 1, "0123456789ab");
    const page_stats = manager.sliceStats(.lite_native_page_cache);
    try std.testing.expectEqual(@as(usize, 0), cache.total_bytes);
    try std.testing.expectEqual(@as(u64, 0), page_stats.used_bytes);
    try std.testing.expect(page_stats.hard_limit_rejections > 0);

    cache.putLinks(allocator, 2, .{ .kind = .value, .link_page = 0, .chunk_len = 1 });
    const link_stats = manager.sliceStats(.lite_native_link_cache);
    try std.testing.expectEqual(@as(usize, 0), cache.link_bytes);
    try std.testing.expectEqual(@as(u64, 0), link_stats.used_bytes);
    try std.testing.expect(link_stats.hard_limit_rejections > 0);
}

test "lite native page cache serves updated documents after page reuse and vacuum" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-page-cache-reuse.aflite");
    defer allocator.free(path);

    var file = try NativeFile.create(allocator, path);
    defer file.close();

    // Enough update churn to force free-page reuse across many commits while
    // reads run through the cache.
    var round: usize = 0;
    while (round < 20) : (round += 1) {
        var value_buf: [32]u8 = undefined;
        const value = try std.fmt.bufPrint(&value_buf, "round-{d}", .{round});
        try file.putDocument("doc:cache", value);

        const read = (try file.getDocumentAlloc(allocator, "doc:cache")) orelse return error.TestUnexpectedResult;
        defer allocator.free(read);
        try std.testing.expectEqualStrings(value, read);

        const docs = try file.snapshotDocumentsAlloc(allocator);
        defer NativeFile.freeSnapshotDocuments(allocator, docs);
        try std.testing.expectEqual(@as(usize, 1), docs.len);
        try std.testing.expectEqualStrings(value, docs[0].value);
    }

    const report_before = try file.check();
    try std.testing.expect(report_before.valid);

    _ = try file.vacuum();

    const read = (try file.getDocumentAlloc(allocator, "doc:cache")) orelse return error.TestUnexpectedResult;
    defer allocator.free(read);
    try std.testing.expectEqualStrings("round-19", read);

    const report = try file.check();
    try std.testing.expect(report.valid);
}

test "lite native catalog index bounds cold hits and misses across checkpoints" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "catalog-index-scaling.aflite");
    defer alloc.free(path);
    {
        var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
        defer file.close();
        for (0..400) |i| {
            var key: [32]u8 = undefined;
            try file.putIndexCatalogRecord(try std.fmt.bufPrint(&key, "key-{d:0>4}", .{i}), "original");
        }
    }
    var file = try NativeFile.openWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    const pinned = file.activeCheckpoint();
    for ([_][]const u8{ "key-0000", "key-0200", "missing" }) |key| {
        const before = file.test_page_reads.load(.monotonic);
        const found = try file.getIndexCatalogRecordAlloc(alloc, key);
        defer if (found) |value| alloc.free(value);
        if (std.mem.eql(u8, key, "missing")) try std.testing.expect(found == null) else try std.testing.expectEqualStrings("original", found.?);
        try std.testing.expect(file.test_page_reads.load(.monotonic) - before <= 6);
    }
    try file.putIndexCatalogRecord("key-0000", "changed");
    try file.renameIndexCatalogRecord("key-0200", "renamed");
    try file.deleteIndexCatalogRecord("key-0399");
    const old = (try file.getIndexCatalogRecordAtCheckpointAlloc(alloc, "key-0000", pinned)).?;
    defer alloc.free(old);
    try std.testing.expectEqualStrings("original", old);
    try std.testing.expectEqual(@as(?usize, null), try file.getIndexCatalogRecordSize("key-0200"));
    try std.testing.expectEqual(@as(?usize, 8), try file.getIndexCatalogRecordSize("renamed"));
    try std.testing.expectEqual(@as(?usize, 8), try file.getIndexCatalogRecordSizeAtCheckpoint("key-0399", pinned));
    try std.testing.expectEqual(@as(?usize, null), try file.getIndexCatalogRecordSize("key-0399"));
    try std.testing.expect((try file.check()).valid);
}

test "lite native extent appends and tail reads are bounded and preserve snapshots" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "extent-scaling.aflite");
    defer alloc.free(path);
    const snapshot_path = try testPath(alloc, tmp, "extent-snapshot.aflite");
    defer alloc.free(snapshot_path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const chunk = file.maxValuePagePayloadBytes();
    // Cross the 64-by-64 leaf boundary to exercise cascading splits and
    // growth to a third branch level, not just a single root split.
    const initial = try alloc.alloc(u8, chunk * 4095);
    defer alloc.free(initial);
    @memset(initial, 'a');
    try file.putIndexCatalogRecord("wal", initial);
    const pinned = file.activeCheckpoint();
    var expected = std.ArrayListUnmanaged(u8).empty;
    defer expected.deinit(alloc);
    try expected.appendSlice(alloc, initial);
    const suffix = try alloc.alloc(u8, chunk);
    defer alloc.free(suffix);
    file.page_cache_enabled.store(false, .monotonic);
    for (0..256) |i| {
        @memset(suffix, @intCast(i));
        const reads = file.test_page_reads.load(.monotonic);
        const writes = file.test_page_writes.load(.monotonic);
        try file.appendIndexCatalogRecord("wal", suffix);
        // Copying the existing value would write at least 4096 pages on the
        // very first append and grow linearly thereafter.
        try std.testing.expect(file.test_page_writes.load(.monotonic) - writes <= 10);
        try std.testing.expect(file.test_page_reads.load(.monotonic) - reads <= 20);
        try expected.appendSlice(alloc, suffix);
        const before_tail = file.test_page_reads.load(.monotonic);
        const tail = (try file.getIndexCatalogRecordRangeAlloc(alloc, "wal", expected.items.len - 17, 17)).?;
        defer alloc.free(tail);
        try std.testing.expectEqualSlices(u8, suffix[suffix.len - 17 ..], tail);
        try std.testing.expect(file.test_page_reads.load(.monotonic) - before_tail <= 8);
    }
    // Fill a partial leaf and read a range crossing a leaf boundary.
    try file.appendIndexCatalogRecord("wal", "end");
    try file.appendIndexCatalogRecord("wal", "ing");
    try expected.appendSlice(alloc, "ending");
    const cross = (try file.getIndexCatalogRecordRangeAlloc(alloc, "wal", expected.items.len - 12, 12)).?;
    defer alloc.free(cross);
    try std.testing.expectEqualSlices(u8, expected.items[expected.items.len - 12 ..], cross);
    const original = (try file.getIndexCatalogRecordAtCheckpointAlloc(alloc, "wal", pinned)).?;
    defer alloc.free(original);
    try std.testing.expectEqualSlices(u8, initial, original);
    const all = (try file.getIndexCatalogRecordAlloc(alloc, "wal")).?;
    defer alloc.free(all);
    try std.testing.expectEqualSlices(u8, expected.items, all);
    try std.testing.expect((try file.check()).valid);
    _ = try file.copyStableSnapshotToPath(snapshot_path, false);
    var snapshot = try NativeFile.open(alloc, snapshot_path, true);
    defer snapshot.close();
    const copied = (try snapshot.getIndexCatalogRecordAlloc(alloc, "wal")).?;
    defer alloc.free(copied);
    try std.testing.expectEqualSlices(u8, expected.items, copied);
    _ = try file.vacuum();
    const compact = (try file.getIndexCatalogRecordAlloc(alloc, "wal")).?;
    defer alloc.free(compact);
    try std.testing.expectEqualSlices(u8, expected.items, compact);
    const report = try file.check();
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(report.file_size, report.compact_size);
}

test "lite native revision 3 rejects revision 2 without modifying the file" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "rejected-v2.aflite");
    defer alloc.free(path);
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{});
    std.mem.writeInt(u32, encoded[version_offset..][0..4], 2, .little);
    std.mem.writeInt(u32, encoded[header_checksum_offset..][0..4], headerChecksum(&encoded), .little);
    {
        var file = try std.Io.Dir.cwd().createFile(std.testing.io, path, .{});
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, &encoded, 0);
    }
    try std.testing.expectError(error.UnsupportedNativeFormatVersion, NativeFile.open(alloc, path, true));
    try std.testing.expectError(error.UnsupportedNativeFormatVersion, NativeFile.open(alloc, path, false));
    var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{});
    defer file.close(std.testing.io);
    var actual: [header_size]u8 = undefined;
    try readHeaderExactAt(file, std.testing.io, &actual);
    try std.testing.expectEqualSlices(u8, &encoded, &actual);
}

test "lite native extent checks reject corrupted child lengths" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "extent-corrupt.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const value = try alloc.alloc(u8, default_page_size * 3);
    defer alloc.free(value);
    @memset(value, 'v');
    try file.putIndexCatalogRecord("wal", value);
    const record = (try file.lookupCatalogPage(file.activeCheckpoint(), .index, "wal")).?;
    const record_payload = try file.readPagePayloadByKindAlloc(alloc, record, .catalog);
    defer alloc.free(record_payload);
    const entry = try decodeCatalogEntry(record_payload);
    const payload = try file.readPagePayloadByKindAlloc(alloc, entry.external_value_root_page, .value_extent);
    defer alloc.free(payload);
    payload[NativeFile.extent_header_size + 8] ^= 1;
    try file.writePage(entry.external_value_root_page, .value_extent, payload);
    try std.testing.expectError(error.InvalidNativeValueChain, file.getIndexCatalogRecordAlloc(alloc, "wal"));
    try std.testing.expect(!(try file.check()).valid);
}

test "lite native catalog index check rejects stale or cross key pointers" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "catalog-index-corrupt.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    try file.putIndexCatalogBatch(&.{ .{ .key = "a", .value = "one" }, .{ .key = "b", .value = "two" } });
    const roots = try file.readCatalogRoots(file.activeCheckpoint().index_catalog_root_page, file.activeCheckpoint());
    var node = try file.readDocumentIndexNode(roots.index, file.activeCheckpoint());
    defer node.deinit(alloc);
    node.pointers[0] = node.pointers[1];
    const payload = try encodeDocumentIndexNode(alloc, node);
    defer alloc.free(payload);
    try file.writePage(roots.index, .document_index, payload);
    try std.testing.expectError(error.InvalidNativePageChain, file.getIndexCatalogRecordAlloc(alloc, "a"));
    try std.testing.expect(!(try file.check()).valid);
}

test "lite native incomplete extent commit falls back to prior indexed checkpoint" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "extent-fallback.aflite");
    defer alloc.free(path);
    const value = try alloc.alloc(u8, default_page_size * 3);
    defer alloc.free(value);
    @memset(value, 'v');
    {
        var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
        defer file.close();
        try file.putIndexCatalogRecord("wal", value);
        const previous = file.activeCheckpoint();
        try file.appendIndexCatalogRecord("wal", "incomplete");
        try file.file.setLength(file.runtimeIo(), previous.page_count * default_page_size);
    }
    var file = try NativeFile.open(alloc, path, false);
    defer file.close();
    const recovered = (try file.getIndexCatalogRecordAlloc(alloc, "wal")).?;
    defer alloc.free(recovered);
    try std.testing.expectEqualSlices(u8, value, recovered);
    try file.appendIndexCatalogRecord("wal", "recovered");
    const tail = (try file.getIndexCatalogRecordRangeAlloc(alloc, "wal", value.len, 9)).?;
    defer alloc.free(tail);
    try std.testing.expectEqualStrings("recovered", tail);
    try std.testing.expect((try file.check()).valid);
}

test "lite native commits extend positional writes without stat or resize calls" {
    const Counter = struct {
        var base: std.Io = undefined;
        var stats: usize = 0;
        var resizes: usize = 0;
        fn stat(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.StatError!std.Io.File.Stat {
            stats += 1;
            return base.vtable.fileStat(userdata, file);
        }
        fn resize(userdata: ?*anyopaque, file: std.Io.File, len: u64) std.Io.File.SetLengthError!void {
            resizes += 1;
            return base.vtable.fileSetLength(userdata, file, len);
        }
    };
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "positional-growth.aflite");
    defer alloc.free(path);
    Counter.base = std.testing.io;
    var vtable = std.testing.io.vtable.*;
    vtable.fileStat = Counter.stat;
    vtable.fileSetLength = Counter.resize;
    const io = std.Io{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    var file = try NativeFile.createWithIo(alloc, io, path, .{ .no_sync = true });
    defer file.close();
    Counter.stats = 0;
    Counter.resizes = 0;
    const value = try alloc.alloc(u8, default_page_size * 16);
    defer alloc.free(value);
    @memset(value, 'x');
    try file.putIndexCatalogRecord("large", value);
    try file.appendIndexCatalogRecord("large", "tail");
    try file.putDocument("doc", value);
    try std.testing.expectEqual(@as(usize, 0), Counter.stats);
    try std.testing.expectEqual(@as(usize, 0), Counter.resizes);
    try std.testing.expectEqual(file.activeCheckpoint().page_count * file.header.page_size, (try file.file.stat(std.testing.io)).size);
    try std.testing.expect((try file.check()).valid);
}

test "lite native catalog preserves maximum length keys through vacuum" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "catalog-max-key.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const key = try alloc.alloc(u8, file.maxPagePayloadBytes() - 16);
    defer alloc.free(key);
    @memset(key, 'k');
    try file.putIndexCatalogRecord(key, "");
    try file.putIndexCatalogRecord("ordinary", "indexed");
    try std.testing.expectEqual(@as(?usize, 0), try file.getIndexCatalogRecordSize(key));
    try std.testing.expect((try file.check()).valid);
    _ = try file.vacuum();
    try std.testing.expectEqual(@as(?usize, 0), try file.getIndexCatalogRecordSize(key));
    try file.deleteIndexCatalogRecord(key);
    try std.testing.expectEqual(@as(?usize, null), try file.getIndexCatalogRecordSize(key));
    try std.testing.expect((try file.check()).valid);
}

test "lite native catalog indexes mixed large empty and maximum keys across splits and vacuum" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "catalog-overflow-keys.aflite");
    defer alloc.free(path);
    {
        var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
        defer file.close();
        const a = @as([1000]u8, @splat('a'));
        const z = @as([1000]u8, @splat('z'));
        const m = @as([4000]u8, @splat('m'));
        try file.putIndexCatalogRecord(&a, "a");
        try file.putIndexCatalogRecord(&z, "z");
        // Previously passed the catalog's key-size check but could not be
        // partitioned into two inline B-tree leaves.
        try file.putIndexCatalogRecord(&m, "m");
        try file.putIndexCatalogRecord("", "empty");
        const maximum = try alloc.alloc(u8, file.maxPagePayloadBytes() - 16);
        defer alloc.free(maximum);
        @memset(maximum, 'x');
        try file.putIndexCatalogRecord(maximum, "");
        for (0..400) |i| {
            var key: [1000]u8 = @splat('k');
            _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
            try file.putIndexCatalogRecord(&key, "long");
        }
        const before_lookup = file.test_page_reads.load(.monotonic);
        try std.testing.expectEqual(@as(?usize, 1), try file.getIndexCatalogRecordSize(&m));
        try std.testing.expect(file.test_page_reads.load(.monotonic) - before_lookup <= 20);
        const pinned = file.activeCheckpoint();
        var cursor = try file.indexCatalogCursor(pinned, "");
        defer cursor.deinit();
        try file.deleteIndexCatalogRecord(&a);
        try file.putIndexCatalogRecord(&m, "updated");
        try file.renameIndexCatalogRecord(&z, "renamed");
        var count: usize = 0;
        while (try cursor.next()) |record| {
            defer alloc.free(record.key);
            if (count == 0) try std.testing.expectEqualStrings("", record.key);
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 405), count);
        const old = (try file.getIndexCatalogRecordAtCheckpointAlloc(alloc, &m, pinned)).?;
        defer alloc.free(old);
        try std.testing.expectEqualStrings("m", old);
        try std.testing.expect((try file.check()).valid);
        _ = try file.vacuum();
        try std.testing.expect((try file.check()).valid);
    }
    var reopened = try NativeFile.open(alloc, path, true);
    defer reopened.close();
    const keys = try reopened.snapshotIndexCatalogKeysAlloc(alloc);
    defer NativeFile.freeSnapshotCatalogKeys(alloc, keys);
    try std.testing.expectEqual(@as(usize, 404), keys.len);
    try std.testing.expectEqualStrings("", keys[0].key);
    const m = @as([4000]u8, @splat('m'));
    const value = (try reopened.getIndexCatalogRecordAlloc(alloc, &m)).?;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("updated", value);
    try std.testing.expect((try reopened.check()).valid);
}

test "lite native overflow key references reject non-record pages and out of checkpoint references" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "corrupt-key-reference.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const key = @as([1000]u8, @splat('k'));
    try file.putIndexCatalogRecord(&key, "value");
    const checkpoint = file.activeCheckpoint();
    const roots = try file.readCatalogRoots(checkpoint.index_catalog_root_page, checkpoint);
    var node = try file.readDocumentIndexNode(roots.index, checkpoint);
    defer node.deinit(alloc);
    node.key_pages.?[0] = roots.index;
    const encoded = try encodeDocumentIndexNode(alloc, node);
    defer alloc.free(encoded);
    try file.writePage(roots.index, .document_index, encoded);
    try std.testing.expectError(error.InvalidDocumentIndex, file.getIndexCatalogRecordAlloc(alloc, &key));
    try std.testing.expect(!(try file.check()).valid);
    node.key_pages.?[0] = checkpoint.page_count;
    const outside = try encodeDocumentIndexNode(alloc, node);
    defer alloc.free(outside);
    try file.writePage(roots.index, .document_index, outside);
    try std.testing.expectError(error.InvalidPageId, file.getIndexCatalogRecordAlloc(alloc, &key));
    try std.testing.expect(!(try file.check()).valid);
    // A well-formed record within the file is still unsafe if no checkpoint
    // history owns it: free-page reclamation must never lose a separator key.
    const orphan = try file.allocatePage("orphan");
    const record = try file.readPagePayloadByKindAlloc(alloc, node.pointers[0], .catalog);
    defer alloc.free(record);
    try file.writePage(orphan, .catalog, record);
    node.key_pages.?[0] = orphan;
    const unowned = try encodeDocumentIndexNode(alloc, node);
    defer alloc.free(unowned);
    try file.writePage(roots.index, .document_index, unowned);
    const readable = (try file.getIndexCatalogRecordAlloc(alloc, &key)).?;
    defer alloc.free(readable);
    try std.testing.expectEqualStrings("value", readable);
    try std.testing.expect(!(try file.check()).valid);
}

test "lite native large appends seal each suffix subtree once" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "bulk-extent-append.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const chunk = file.maxValuePagePayloadBytes();
    const bytes = try alloc.alloc(u8, chunk * 4096);
    defer alloc.free(bytes);
    for (bytes, 0..) |*byte, i| byte.* = @truncate(i);
    try file.putIndexCatalogRecord("/wal", bytes);
    const pinned = file.activeCheckpoint();
    const before = file.test_page_writes.load(.monotonic);
    const before_reads = file.test_page_reads.load(.monotonic);
    try file.appendIndexCatalogRecord("/wal", bytes);
    const writes = file.test_page_writes.load(.monotonic) - before;
    // 4096 leaves, 64 branch pages, their parent, a new root, and catalog
    // publication. The old per-leaf path copying wrote 16,453 pages.
    try std.testing.expect(writes <= 4170);
    try std.testing.expect(file.test_page_reads.load(.monotonic) - before_reads <= 20);
    const old = (try file.getIndexCatalogRecordAtCheckpointAlloc(alloc, "/wal", pinned)).?;
    defer alloc.free(old);
    try std.testing.expectEqualSlices(u8, bytes, old);
    const all = (try file.getIndexCatalogRecordAlloc(alloc, "/wal")).?;
    defer alloc.free(all);
    try std.testing.expectEqual(@as(usize, bytes.len * 2), all.len);
    try std.testing.expectEqualSlices(u8, bytes, all[0..bytes.len]);
    try std.testing.expectEqualSlices(u8, bytes, all[bytes.len..]);
    // Cross the same boundaries starting from a partially filled tail.
    try file.putIndexCatalogRecord("/partial", bytes[0 .. bytes.len - 17]);
    const partial_before = file.test_page_writes.load(.monotonic);
    try file.appendIndexCatalogRecord("/partial", bytes);
    try std.testing.expect(file.test_page_writes.load(.monotonic) - partial_before <= 4175);
    const partial = (try file.getIndexCatalogRecordAlloc(alloc, "/partial")).?;
    defer alloc.free(partial);
    try std.testing.expectEqualSlices(u8, bytes[0 .. bytes.len - 17], partial[0 .. bytes.len - 17]);
    try std.testing.expectEqualSlices(u8, bytes, partial[bytes.len - 17 ..]);
    try std.testing.expect((try file.check()).valid);
}

test "lite native batched extent append handles all small tree boundary shapes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "extent-frontier-boundaries.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const chunk = file.maxValuePagePayloadBytes();
    var expected = std.ArrayListUnmanaged(u8).empty;
    defer expected.deinit(alloc);
    for ([_]usize{ 1, chunk - 1, 1, chunk * 62 - 1, chunk, 1, chunk * 65 + 17, 0, chunk * 129 }) |len| {
        const suffix = try alloc.alloc(u8, len);
        defer alloc.free(suffix);
        @memset(suffix, @truncate(expected.items.len));
        try file.appendIndexCatalogRecord("wal", suffix);
        try expected.appendSlice(alloc, suffix);
        const actual = (try file.getIndexCatalogRecordAlloc(alloc, "wal")).?;
        defer alloc.free(actual);
        try std.testing.expectEqualSlices(u8, expected.items, actual);
        try std.testing.expect((try file.check()).valid);
    }
}

test "lite native document overflow keys survive bulk build overwrite and vacuum" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "document-overflow-keys.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const key_bytes = try alloc.alloc(u8, 1000 * 300);
    defer alloc.free(key_bytes);
    @memset(key_bytes, 'k');
    const mutations = try alloc.alloc(DocumentMutation, 300);
    defer alloc.free(mutations);
    for (mutations, 0..) |*mutation, i| {
        const key = key_bytes[i * 1000 ..][0..1000];
        _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        mutation.* = .{ .key = key, .value = "original" };
    }
    try file.putDocumentBatch(mutations);
    const pinned = file.activeCheckpoint();
    try file.putDocument(mutations[225].key, "new");
    try file.deleteDocument(mutations[226].key);
    const old = (try file.getDocumentAtCheckpointAlloc(alloc, pinned, mutations[225].key)).?;
    defer alloc.free(old);
    try std.testing.expectEqualStrings("original", old);
    try std.testing.expect((try file.check()).valid);
    _ = try file.vacuum();
    const current = (try file.getDocumentAlloc(alloc, mutations[225].key)).?;
    defer alloc.free(current);
    try std.testing.expectEqualStrings("new", current);
    try std.testing.expect((try file.getDocumentAlloc(alloc, mutations[226].key)) == null);
    try std.testing.expect((try file.check()).valid);
}

test "lite native catalog deletion keeps directory scans independent of retired generations" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "live-catalog-deletion.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    for (0..1000) |i| {
        var key: [64]u8 = undefined;
        try file.putIndexCatalogRecord(try std.fmt.bufPrint(&key, "/generation/block-{d:0>8}", .{i}), "x");
    }
    const pinned = file.activeCheckpoint();
    try file.putIndexCatalogRecord("/generation/CURRENT", "live");
    for (0..1000) |i| {
        var key: [64]u8 = undefined;
        try file.deleteIndexCatalogRecord(try std.fmt.bufPrint(&key, "/generation/block-{d:0>8}", .{i}));
    }
    const before = file.test_page_reads.load(.monotonic);
    var cursor = try file.indexCatalogCursor(file.activeCheckpoint(), "/generation/");
    defer cursor.deinit();
    const only = (try cursor.next()).?;
    defer alloc.free(only.key);
    try std.testing.expectEqualStrings("/generation/CURRENT", only.key);
    try std.testing.expect((try cursor.next()) == null);
    try std.testing.expect(file.test_page_reads.load(.monotonic) - before <= 4);
    var old = try file.indexCatalogCursor(pinned, "/generation/");
    defer old.deinit();
    var count: usize = 0;
    while (try old.next()) |record| {
        alloc.free(record.key);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 1000), count);
    try std.testing.expect((try file.check()).valid);
    try file.deleteIndexCatalogRecord("/generation/CURRENT");
    const roots = try file.readCatalogRoots(file.activeCheckpoint().index_catalog_root_page, file.activeCheckpoint());
    try std.testing.expectEqual(@as(u64, 0), roots.index);
    try file.putIndexCatalogRecord("/generation/new", "new");
    try std.testing.expect((try file.check()).valid);
}

test "lite native catalog batches write only final reachable index nodes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "catalog-batch-editor.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const keys = try alloc.alloc([16]u8, 1024);
    defer alloc.free(keys);
    const mutations = try alloc.alloc(CatalogMutation, keys.len);
    defer alloc.free(mutations);
    for (mutations, 0..) |*mutation, i| mutation.* = .{ .key = try std.fmt.bufPrint(&keys[i], "key-{d:0>8}", .{i}), .value = "old" };
    for (0..2) |pass| {
        const before = file.activeCheckpoint().page_count;
        try file.putIndexCatalogBatch(mutations);
        const checkpoint = file.activeCheckpoint();
        const roots = try file.readCatalogRoots(checkpoint.index_catalog_root_page, checkpoint);
        var indexed = checkpoint;
        indexed.document_index_root_page = roots.index;
        var reachable = NativeFile.ReachablePageSet{};
        defer reachable.deinit(alloc);
        const nodes = try file.collectDocumentIndexPages(indexed, &reachable, false, false, null);
        // Packed records plus the final index, descriptor and free map.
        // Intermediate copy-on-write paths would exceed this bound.
        try std.testing.expect(checkpoint.page_count - before <= nodes + 16);
        try std.testing.expect(checkpoint.page_count - before <= 1050);
        try std.testing.expect((try file.check()).valid);
        if (pass == 0) for (mutations) |*mutation| {
            mutation.value = "new";
        };
    }
    // Repeated keys preserve input-order semantics without emitting transient
    // tree versions, including removal followed by recreation in one commit.
    const pinned = file.activeCheckpoint();
    try file.putIndexCatalogBatch(&.{
        .{ .key = mutations[0].key, .is_delete = true },
        .{ .key = mutations[0].key, .value = "recreated" },
        .{ .key = mutations[1].key, .value = "temporary" },
        .{ .key = mutations[1].key, .is_delete = true },
    });
    const old = (try file.getIndexCatalogRecordAtCheckpointAlloc(alloc, mutations[0].key, pinned)).?;
    defer alloc.free(old);
    try std.testing.expectEqualStrings("new", old);
    const recreated = (try file.getIndexCatalogRecordAlloc(alloc, mutations[0].key)).?;
    defer alloc.free(recreated);
    try std.testing.expectEqualStrings("recreated", recreated);
    try std.testing.expect((try file.getIndexCatalogRecordAlloc(alloc, mutations[1].key)) == null);
    try std.testing.expect((try file.check()).valid);
}

test "lite native long key updates resolve only comparison keys" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "lazy-key-editor.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    for (0..400) |i| {
        var key: [1000]u8 = @splat('k');
        _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        try file.putIndexCatalogRecord(&key, "old");
    }
    var key: [1000]u8 = @splat('k');
    _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{@as(usize, 200)});
    const before = file.test_page_reads.load(.monotonic);
    try file.putIndexCatalogRecord(&key, "replacement");
    try std.testing.expect(file.test_page_reads.load(.monotonic) - before <= 32);
    const before_delete = file.test_page_reads.load(.monotonic);
    try file.deleteIndexCatalogRecord(&key);
    try std.testing.expect(file.test_page_reads.load(.monotonic) - before_delete <= 32);
    try std.testing.expect((try file.check()).valid);
}

test "lite native catalog editor rebalances mixed key widths across deletion and reopen" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "mixed-key-editor.aflite");
    defer alloc.free(path);
    const count = 1536;
    const keys = try alloc.alloc([]u8, count);
    defer alloc.free(keys);
    var initialized: usize = 0;
    defer for (keys[0..initialized]) |key| alloc.free(key);
    const widths = [_]usize{ 8, 16, 511, 512, 513, 1000, 4000 };
    for (keys, 0..) |*key, i| {
        key.* = try alloc.alloc(u8, if (i == 0) 0 else widths[i % widths.len]);
        initialized += 1;
        @memset(key.*, 'k');
        if (i != 0) _ = try std.fmt.bufPrint(key.*[0..8], "{d:0>8}", .{i});
    }
    {
        var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
        defer file.close();
        const mutations = try alloc.alloc(CatalogMutation, count);
        defer alloc.free(mutations);
        for (mutations, 0..) |*mutation, i| mutation.* = .{ .key = keys[(i * 1009) % count], .value = "old" };
        try file.putIndexCatalogBatch(mutations);
        const pinned = file.activeCheckpoint();
        try std.testing.expect((try file.check()).valid);
        // Coprime permutations exercise both sides of internal nodes, merging,
        // redistribution, and separator replacement with different slot sizes.
        for (0..4) |phase| {
            for (mutations[0 .. count / 4], 0..) |*mutation, j| mutation.* = .{ .key = keys[((phase * (count / 4) + j) * 1013) % count], .is_delete = true };
            try file.putIndexCatalogBatch(mutations[0 .. count / 4]);
            var cursor = try file.indexCatalogCursor(file.activeCheckpoint(), "");
            defer cursor.deinit();
            var actual: usize = 0;
            while (try cursor.next()) |record| {
                alloc.free(record.key);
                actual += 1;
            }
            try std.testing.expectEqual(count - (phase + 1) * (count / 4), actual);
            try std.testing.expect((try file.check()).valid);
        }
        var old = try file.indexCatalogCursor(pinned, "");
        defer old.deinit();
        var old_count: usize = 0;
        while (try old.next()) |record| {
            alloc.free(record.key);
            old_count += 1;
        }
        try std.testing.expectEqual(@as(usize, count), old_count);
        // Rebuild from an empty live tree while preserving deletion history.
        for (mutations, keys) |*mutation, key| mutation.* = .{ .key = key, .value = "new" };
        try file.putIndexCatalogBatch(mutations);
        try file.renameIndexCatalogRecord(keys[17], "/renamed");
        try std.testing.expect((try file.check()).valid);
        _ = try file.vacuum();
    }
    var reopened = try NativeFile.open(alloc, path, false);
    defer reopened.close();
    try std.testing.expect((try reopened.getIndexCatalogRecordAlloc(alloc, keys[17])) == null);
    const value = (try reopened.getIndexCatalogRecordAlloc(alloc, "/renamed")).?;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("new", value);
    try std.testing.expect((try reopened.check()).valid);
}

test "lite native existing document batches write each changed tree node once" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "document-batch-editor.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const keys = try alloc.alloc([16]u8, 1024);
    defer alloc.free(keys);
    const mutations = try alloc.alloc(DocumentMutation, keys.len);
    defer alloc.free(mutations);
    for (mutations, 0..) |*mutation, i| mutation.* = .{ .key = try std.fmt.bufPrint(&keys[i], "doc-{d:0>8}", .{i}), .value = "old" };
    try file.putDocumentBatch(mutations);
    const pinned = file.activeCheckpoint();
    for (mutations) |*mutation| mutation.value = "new";
    const before = file.activeCheckpoint().page_count;
    try file.putDocumentBatch(mutations);
    const checkpoint = file.activeCheckpoint();
    var reachable = NativeFile.ReachablePageSet{};
    defer reachable.deinit(alloc);
    const nodes = try file.collectDocumentIndexPages(checkpoint, &reachable, false, false, null);
    try std.testing.expect(checkpoint.page_count - before <= nodes + 16);
    try std.testing.expect(checkpoint.page_count - before <= 1050);
    const old = (try file.getDocumentAtCheckpointAlloc(alloc, pinned, mutations[0].key)).?;
    defer alloc.free(old);
    try std.testing.expectEqualStrings("old", old);
    const current = (try file.getDocumentAlloc(alloc, mutations[0].key)).?;
    defer alloc.free(current);
    try std.testing.expectEqualStrings("new", current);
    try std.testing.expect((try file.check()).valid);
}

test "lite native catalog editor mixed batches match a reference map" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "model-catalog-editor.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const count = 192;
    var expected: [count]?u64 = @splat(null);
    const keys = try alloc.alloc([]u8, count);
    defer alloc.free(keys);
    var initialized: usize = 0;
    defer for (keys[0..initialized]) |key| alloc.free(key);
    const widths = [_]usize{ 8, 511, 512, 513, 1000 };
    for (keys, 0..) |*key, i| {
        key.* = try alloc.alloc(u8, if (i == 0) 0 else widths[i % widths.len]);
        initialized += 1;
        @memset(key.*, 'm');
        if (i != 0) _ = try std.fmt.bufPrint(key.*[0..8], "{d:0>8}", .{i});
    }
    var rng = std.Random.DefaultPrng.init(803);
    const random = rng.random();
    for (0..120) |round| {
        var values: [24][8]u8 = undefined;
        var mutations: [24]CatalogMutation = undefined;
        for (&mutations, 0..) |*mutation, i| {
            const selected = random.uintLessThan(usize, count);
            const deleted = random.uintLessThan(u8, 3) == 0;
            const version: u64 = round * mutations.len + i;
            std.mem.writeInt(u64, &values[i], version, .little);
            mutation.* = .{ .key = keys[selected], .value = &values[i], .is_delete = deleted };
            expected[selected] = if (deleted) null else version;
        }
        try file.putIndexCatalogBatch(&mutations);
        if (round % 12 == 0 or round == 119) {
            for (keys, expected) |key, want| {
                const actual = try file.getIndexCatalogRecordAlloc(alloc, key);
                defer if (actual) |value| alloc.free(value);
                if (want) |version| {
                    try std.testing.expect(actual != null);
                    try std.testing.expectEqual(version, std.mem.readInt(u64, actual.?[0..8], .little));
                } else try std.testing.expect(actual == null);
            }
            try std.testing.expect((try file.check()).valid);
        }
    }
}

const MaintenanceTestAllocator = @import("test_allocator.zig").BudgetAllocator;

test "lite native maintenance streams large values under a bounded heap budget" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "bounded-maintenance.aflite");
    defer alloc.free(path);
    const value = try alloc.alloc(u8, 2 * 1024 * 1024 + 137);
    defer alloc.free(value);
    for (value, 0..) |*byte, i| byte.* = @intCast(i % 251);
    var budget = MaintenanceTestAllocator{ .backing = alloc };
    {
        var file = try NativeFile.createWithIo(budget.allocator(), std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        try file.putCatalogRecord("schema", value);
        try file.putIndexCatalogRecord("vectors", value[0..4301]);
        try file.appendIndexCatalogRecord("vectors", value[4301..5298]);
        try file.appendIndexCatalogRecord("vectors", value[5298..]);
        try file.putDocument("docs\x00large", value);
        budget.peak = budget.live;
        budget.limit = budget.live + 512 * 1024;
        const reads = file.test_page_reads.load(.monotonic);
        const stats = try file.liveStats(null);
        try std.testing.expectEqual(@as(u64, 3), stats.record_count);
        try std.testing.expectEqual(@as(u64, value.len * 3 + "schema".len + "vectors".len + "docs\x00large".len), stats.bytes);
        // Statistics must not read any of the thousands of value pages.
        try std.testing.expect(file.test_page_reads.load(.monotonic) - reads <= 12);
        const checked = try file.check();
        try std.testing.expect(checked.valid);
        try std.testing.expectEqual(stats.compact_size, checked.compact_size);

        // Cancel during value copying, after the temporary output exists.
        const checkpoint = file.activeCheckpoint();
        var cancel = maintenance.CancelToken{};
        budget.cancel = &cancel;
        budget.cancel_after = 40;
        try std.testing.expectError(error.MaintenanceCanceled, file.vacuumWithCancel(&cancel));
        try std.testing.expectError(error.MaintenanceCanceled, file.liveStats(&cancel));
        budget.cancel = null;
        budget.cancel_after = std.math.maxInt(usize);
        try std.testing.expect(std.meta.eql(checkpoint, file.activeCheckpoint()));
        const temp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-aflite-vacuum", .{path});
        defer alloc.free(temp_path);
        try std.testing.expect(!pathExists(std.testing.io, temp_path));

        const vacuumed = try file.vacuum();
        try std.testing.expectEqual(stats.compact_size, vacuumed.after_size);
        try std.testing.expectEqual(stats.bytes, vacuumed.live_bytes);
        try std.testing.expect((try file.check()).valid);
        try std.testing.expect(budget.peak <= budget.limit);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    var reopened = try NativeFile.open(alloc, path, true);
    defer reopened.close();
    const catalog = (try reopened.getCatalogRecordAlloc(alloc, "schema")).?;
    defer alloc.free(catalog);
    try std.testing.expectEqualSlices(u8, value, catalog);
    const index = (try reopened.getIndexCatalogRecordAlloc(alloc, "vectors")).?;
    defer alloc.free(index);
    try std.testing.expectEqualSlices(u8, value, index);
    const document = (try reopened.getDocumentAlloc(alloc, "docs\x00large")).?;
    defer alloc.free(document);
    try std.testing.expectEqualSlices(u8, value, document);
}

test "lite native vacuum reads live indexes instead of superseded history" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "indexed-vacuum.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    for (0..1000) |i| {
        var value: [8]u8 = undefined;
        std.mem.writeInt(u64, &value, i, .little);
        try file.putCatalogRecord("schema", &value);
        try file.putIndexCatalogRecord("index", &value);
        try file.putDocument("doc", &value);
    }
    const stats = try file.liveStats(null);
    const reads = file.test_page_reads.load(.monotonic);
    const vacuumed = try file.vacuum();
    try std.testing.expect(file.test_page_reads.load(.monotonic) - reads <= 16);
    try std.testing.expectEqual(@as(u64, 3), vacuumed.live_file_count);
    try std.testing.expectEqual(stats.compact_size, vacuumed.after_size);
    const document = (try file.getDocumentAlloc(alloc, "doc")).?;
    defer alloc.free(document);
    try std.testing.expectEqual(@as(u64, 999), std.mem.readInt(u64, document[0..8], .little));
    try std.testing.expect((try file.check()).valid);
}

test "lite native compact statistics match packed live document and namespace indexes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "packed-stats.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    for (0..512) |i| {
        var key: [32]u8 = undefined;
        const formatted = try std.fmt.bufPrint(&key, "namespace-{d:0>4}\x00doc", .{i});
        try file.putDocument(formatted, "value");
        if (i != 511) try file.deleteDocument(formatted);
    }
    const before = try file.check();
    try std.testing.expect(before.valid);
    const vacuumed = try file.vacuum();
    try std.testing.expectEqual(before.compact_size, vacuumed.after_size);
    try std.testing.expectEqual(before.live_bytes, vacuumed.live_bytes);
    try std.testing.expectEqual(@as(u64, 1), vacuumed.live_file_count);
    const after = try file.check();
    try std.testing.expect(after.valid);
    try std.testing.expectEqual(after.file_size, after.compact_size);
    try std.testing.expectEqual(@as(u64, 0), after.reclaimable_bytes);
}

test "lite native streaming vacuum rejects corrupt values before publication" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "stream-corruption.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    const value = try alloc.alloc(u8, default_page_size * 3);
    defer alloc.free(value);
    @memset(value, 'v');
    try file.putIndexCatalogRecord("index", value);
    try file.putDocument("doc", value);
    const checkpoint = file.activeCheckpoint();
    const temp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-aflite-vacuum", .{path});
    defer alloc.free(temp_path);

    const catalog_page = (try file.lookupCatalogPage(checkpoint, .index, "index")).?;
    const catalog_payload = try file.readPagePayloadByKindAlloc(alloc, catalog_page, .catalog);
    defer alloc.free(catalog_payload);
    const catalog = try decodeCatalogEntry(catalog_payload);
    const extent = try file.readPagePayloadByKindAlloc(alloc, catalog.external_value_root_page, .value_extent);
    defer alloc.free(extent);
    extent[NativeFile.extent_header_size + 8] ^= 1;
    try file.writePage(catalog.external_value_root_page, .value_extent, extent);
    try std.testing.expectError(error.InvalidNativeValueChain, file.vacuum());
    try std.testing.expect(std.meta.eql(checkpoint, file.activeCheckpoint()));
    try std.testing.expect(!pathExists(std.testing.io, temp_path));
    try std.testing.expect(!(try file.check()).valid);
    extent[NativeFile.extent_header_size + 8] ^= 1;
    try file.writePage(catalog.external_value_root_page, .value_extent, extent);

    const document_payload = try file.readPagePayloadByKindAlloc(alloc, checkpoint.document_root_page, .document);
    defer alloc.free(document_payload);
    const document = try decodeDocumentEntry(document_payload);
    const first = try file.readPagePayloadByKindAlloc(alloc, document.external_value_root_page, .value);
    defer alloc.free(first);
    const next = std.mem.readInt(u64, first[0..8], .little);
    for ([_]u64{ 0, document.external_value_root_page }) |bad_next| {
        std.mem.writeInt(u64, first[0..8], bad_next, .little);
        try file.writePage(document.external_value_root_page, .value, first);
        try std.testing.expectError(error.InvalidNativeValueChain, file.vacuum());
        try std.testing.expect(std.meta.eql(checkpoint, file.activeCheckpoint()));
        try std.testing.expect(!pathExists(std.testing.io, temp_path));
    }
    std.mem.writeInt(u64, first[0..8], next, .little);
    try file.writePage(document.external_value_root_page, .value, first);
    // A late checksum failure must also discard pages already copied.
    const raw = try file.readPageAlloc(alloc, next);
    defer alloc.free(raw);
    raw[page_header_size + value_page_header_size] ^= 1;
    try file.file.writePositionalAll(file.runtimeIo(), raw, next * default_page_size);
    try std.testing.expectError(error.NativePageChecksumMismatch, file.vacuum());
    try std.testing.expect(std.meta.eql(checkpoint, file.activeCheckpoint()));
    try std.testing.expect(!pathExists(std.testing.io, temp_path));
    raw[page_header_size + value_page_header_size] ^= 1;
    try file.file.writePositionalAll(file.runtimeIo(), raw, next * default_page_size);
    _ = try file.vacuum();
    try std.testing.expect((try file.check()).valid);
}

test "lite native bulk index frontier counts a million long keys with bounded heap" {
    var budget = MaintenanceTestAllocator{ .backing = std.testing.allocator, .limit = 256 * 1024 };
    {
        var file = NativeFile{ .allocator = budget.allocator(), .io_impl = undefined, .borrowed_io = std.testing.io, .path = @constCast(""), .file = undefined, .header = .{} };
        var pages: u64 = 1;
        var builder = DocumentIndexBulkBuilder{ .owner = &file, .file = undefined, .next_page_id = &pages, .count_only = true };
        defer builder.deinit();
        for (0..1_000_000) |i| {
            var key: [4000]u8 = @splat('k');
            _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
            try builder.add(&key, 1);
        }
        try std.testing.expect(try builder.finish() != 0);
        try std.testing.expect(pages > 4000);
        try std.testing.expect(builder.levels.items.len >= 3);
        try std.testing.expect(budget.peak <= budget.limit);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

test "lite native bulk index frontier preserves mixed keys and exact page counts" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "bulk-frontier.aflite");
    defer alloc.free(path);
    var budget = MaintenanceTestAllocator{ .backing = alloc, .limit = 256 * 1024 };
    {
        var file = try NativeFile.createWithIo(budget.allocator(), std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        const count = 16384;
        var next: u64 = 1;
        var counted: u64 = 1;
        var builder = DocumentIndexBulkBuilder{ .owner = &file, .file = file.file, .next_page_id = &next };
        defer builder.deinit();
        var counter = DocumentIndexBulkBuilder{ .owner = &file, .file = undefined, .next_page_id = &counted, .count_only = true };
        defer counter.deinit();
        var history: u64 = 0;
        for (0..count) |i| {
            var key: [4000]u8 = @splat('k');
            _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
            const widths = [_]usize{ 16, 512, 4000 };
            const bytes = key[0..if (i == 0) 0 else widths[i % widths.len]];
            var payload = std.ArrayListUnmanaged(u8).empty;
            defer payload.deinit(budget.allocator());
            try encodeCatalogEntry(budget.allocator(), &payload, .{ .previous_page = history, .key = bytes, .value = "v" });
            history = try appendPageToFile(budget.allocator(), file.file, std.testing.io, default_page_size, &next, .catalog, payload.items);
            try builder.add(bytes, history);
            try counter.add(bytes, history);
        }
        const root = try builder.finish();
        _ = try counter.finish();
        try std.testing.expectEqual(next - count, counted);
        try std.testing.expect(builder.levels.items.len >= 2);
        try std.testing.expect(budget.peak <= budget.limit);
        // Decoding a cursor's long keys has its own path-sized memory bound;
        // the cap above isolates the builder and its counting mode.
        budget.limit = std.math.maxInt(usize);
        var cursor = DocumentIndexCursor.init(&file, .{ .page_count = next, .document_index_root_page = root });
        defer cursor.deinit();
        var entry = try cursor.first();
        var seen: usize = 0;
        while (entry) |value| : (entry = try cursor.next()) {
            var owned = value;
            defer owned.deinit(budget.allocator());
            var key: [4000]u8 = @splat('k');
            _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{seen});
            const widths = [_]usize{ 16, 512, 4000 };
            try std.testing.expectEqualSlices(u8, key[0..if (seen == 0) 0 else widths[seen % widths.len]], value.key);
            seen += 1;
        }
        try std.testing.expectEqual(@as(usize, count), seen);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

test "lite native bulk index frontier releases ownership on allocation failures" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            var file = NativeFile{ .allocator = alloc, .io_impl = undefined, .borrowed_io = std.testing.io, .path = @constCast(""), .file = undefined, .header = .{} };
            var pages: u64 = 1;
            var builder = DocumentIndexBulkBuilder{ .owner = &file, .file = undefined, .next_page_id = &pages, .count_only = true };
            defer builder.deinit();
            for (0..80) |i| {
                var key: [512]u8 = @splat('k');
                _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
                try builder.add(&key, 1);
            }
            try std.testing.expect(try builder.finish() != 0);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "lite native cold payload writes invalidate reused page bytes and links" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "cold-reuse.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithOptions(alloc, path, .{ .no_sync = true });
    defer file.close();
    const id = try file.allocatePage("old");
    const ids = [_]u64{id};
    // Exercise the overwrite boundary directly, including a cached link from
    // the page's previous lifetime. A cold replacement must invalidate both.
    try file.writeValuePageChunk(&ids, 0, "old payload", .{});
    try std.testing.expect(file.page_cache.pages.contains(id));
    try std.testing.expect(file.page_cache.links.contains(id));
    try file.writeValuePageChunk(&ids, 0, "cold replacement", .{ .payload_cache = .cold_sequential });
    try std.testing.expect(!file.page_cache.pages.contains(id));
    try std.testing.expect(!file.page_cache.links.contains(id));
    const page = try file.readPageAlloc(alloc, id);
    defer alloc.free(page);
    const payload = try decodePagePayloadAlloc(alloc, page, .value);
    defer alloc.free(payload);
    try std.testing.expectEqualStrings("cold replacement", payload[value_page_header_size..]);
    // A subsequent read can admit cold-written data normally.
    try std.testing.expect(file.page_cache.pages.contains(id));
}

test "lite native page batches coalesce runs without heap allocation and preserve gaps" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "page-batch.aflite");
    defer alloc.free(path);
    var budget = MaintenanceTestAllocator{ .backing = alloc };
    var file = try NativeFile.createWithIo(budget.allocator(), std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    budget.limit = budget.live; // Neither encoding nor batching may allocate.
    for ([_]u32{ 4096, 65536 }) |size| {
        file.header.page_size = size;
        const run: u64 = PageWriteBatch.capacity / size;
        var batch = PageWriteBatch{ .file = &file };
        const before = file.test_page_write_calls.load(.monotonic);
        // Keep a sentinel between disjoint runs and visit the second run in
        // descending page order, as can happen with a fragmented free map.
        try file.writePage(100, .data, "untouched gap");
        for (1..@intCast(run + 2)) |id| try batch.appendValue(id, 0, "batched value");
        try batch.appendPage(102, .data, "high page");
        try batch.appendPage(101, .data, "low page");
        try batch.flush();
        try std.testing.expectEqual(@as(u64, if (size == 4096) 4 else 5), file.test_page_write_calls.load(.monotonic) - before);
        var raw: [65536]u8 = undefined;
        try readExactAt(file.file, std.testing.io, raw[0..size], @as(u64, size) * 100);
        try std.testing.expectEqualStrings("untouched gap", try decodePagePayload(raw[0..size], .data));
        for (1..@intCast(run + 2)) |id| {
            try readExactAt(file.file, std.testing.io, raw[0..size], @as(u64, size) * id);
            const value = try decodeValuePage(try decodePagePayload(raw[0..size], .value));
            try std.testing.expectEqualStrings("batched value", value.chunk);
            try std.testing.expectEqual(@as(u64, 0), value.next_page);
        }
        try readExactAt(file.file, std.testing.io, raw[0..size], @as(u64, size) * 101);
        try std.testing.expectEqualStrings("low page", try decodePagePayload(raw[0..size], .data));
        try readExactAt(file.file, std.testing.io, raw[0..size], @as(u64, size) * 102);
        try std.testing.expectEqualStrings("high page", try decodePagePayload(raw[0..size], .data));
    }
}

test "lite native failed page batches do not admit cache entries or retry" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "failed-page-batch.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    const first = file.activeCheckpoint().page_count;
    var batch = PageWriteBatch{ .file = &file };
    try batch.appendValue(first, 0, "private");
    try std.testing.expect(!file.page_cache.pages.contains(first));
    file.test_page_write_fail_after = 0;
    try std.testing.expectError(error.TestPageWriteFailure, batch.flush());
    try std.testing.expect(!file.page_cache.pages.contains(first));
    try std.testing.expect(!file.page_cache.links.contains(first));
    file.test_page_write_fail_after = null;
    try std.testing.expectError(error.TestPageWriteFailure, batch.appendValue(first + 1, 0, "retry"));
    try std.testing.expectError(error.TestPageWriteFailure, batch.flush());
    var fresh = PageWriteBatch{ .file = &file };
    try fresh.appendValue(first, 0, "fresh");
    try fresh.flush();
    try std.testing.expect(file.page_cache.pages.contains(first));
    try std.testing.expect(file.page_cache.links.contains(first));
}

test "lite native CLOCK retains hot pages across cold scans and skips oversized admission" {
    const alloc = std.testing.allocator;
    var cache = PageCache{ .limit_bytes = 128 };
    defer cache.deinit(alloc);
    cache.put(alloc, 1, "hot-page");
    for (2..4096) |id| {
        var hot: [8]u8 = undefined;
        try std.testing.expect(cache.copyInto(1, &hot));
        cache.put(alloc, id, "coldpage");
        try std.testing.expect(cache.total_bytes <= 128);
    }
    try std.testing.expect(cache.pages.count() > 1);
    cache.put(alloc, 9999, &(@as([129]u8, @splat(0))));
    try std.testing.expect(!cache.pages.contains(9999));
    var hot: [8]u8 = undefined;
    try std.testing.expect(cache.copyInto(1, &hot));
}

test "lite native transaction publishes catalog and document roots atomically" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "native-root-transaction.aflite");
    defer alloc.free(path);
    {
        var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{});
        defer file.close();
        const before = file.activeCheckpoint().commit_sequence;
        try file.beginTransaction();
        try file.putDocument("document", "value");
        try file.putIndexCatalogRecord("index", "segment");
        try file.putCatalogRecord("metadata", "settings");
        // Independent opens observe only the last durable checkpoint.
        {
            var old = try NativeFile.openWithIo(alloc, std.testing.io, path, .{ .read_only = true });
            defer old.close();
            try std.testing.expect((try old.getDocumentAlloc(alloc, "document")) == null);
        }
        try file.commitTransaction();
        try std.testing.expectEqual(before + 1, file.activeCheckpoint().commit_sequence);
        try file.beginTransaction();
        try file.putDocument("document", "aborted");
        try file.deleteIndexCatalogRecord("index");
        file.abortTransaction();
        const document = (try file.getDocumentAlloc(alloc, "document")).?;
        defer alloc.free(document);
        try std.testing.expectEqualStrings("value", document);
        const index = (try file.getIndexCatalogRecordAlloc(alloc, "index")).?;
        defer alloc.free(index);
        try std.testing.expectEqualStrings("segment", index);
        try std.testing.expect((try file.check()).valid);
    }
    var reopened = try NativeFile.openWithIo(alloc, std.testing.io, path, .{ .read_only = true });
    defer reopened.close();
    const metadata = (try reopened.getCatalogRecordAlloc(alloc, "metadata")).?;
    defer alloc.free(metadata);
    try std.testing.expectEqualStrings("settings", metadata);
    try std.testing.expect((try reopened.check()).valid);
}

test "lite native small document batches coalesce record and index writes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "small-record-batch.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    var keys: [1024][16]u8 = undefined;
    var mutations: [1024]DocumentMutation = undefined;
    for (&keys, &mutations, 0..) |*key, *mutation, i| mutation.* = .{ .key = try std.fmt.bufPrint(key, "key-{d:0>8}", .{i}), .value = "small" };
    const before = file.test_page_write_calls.load(.monotonic);
    try file.putDocumentBatch(&mutations);
    try std.testing.expect(file.test_page_write_calls.load(.monotonic) - before <= 80);
    const reads = file.test_page_reads.load(.monotonic);
    var requested: [1024][]const u8 = undefined;
    for (mutations, &requested) |mutation, *key| key.* = mutation.key;
    var values: [1024]?[]const u8 = undefined;
    try file.getDocumentsAtCheckpointAlloc(alloc, file.activeCheckpoint(), &requested, &values);
    defer for (values) |value| if (value) |bytes| alloc.free(bytes);
    for (values) |value| try std.testing.expectEqualStrings("small", value.?);
    // Each physical bundle and shared index node is read once.
    try std.testing.expect(file.test_page_reads.load(.monotonic) - reads <= 40);
}

test "lite deferred durability preserves the durable roots until a shared barrier" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "deferred-durability.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{});
    defer file.close();
    try file.putIndexCatalogRecord("wal", "first");
    for (0..4) |_| {
        try file.beginTransaction();
        try file.appendIndexCatalogRecord("wal", "+");
        try file.commitTransactionWithDurability(false);
    }
    const live = (try file.getIndexCatalogRecordAlloc(alloc, "wal")).?;
    defer alloc.free(live);
    try std.testing.expectEqualStrings("first++++", live);
    {
        var disk = try NativeFile.openWithIo(alloc, std.testing.io, path, .{ .read_only = true });
        defer disk.close();
        const value = (try disk.getIndexCatalogRecordAlloc(alloc, "wal")).?;
        defer alloc.free(value);
        try std.testing.expectEqualStrings("first", value);
    }
    try file.sync();
    var reopened = try NativeFile.openWithIo(alloc, std.testing.io, path, .{ .read_only = true });
    defer reopened.close();
    const durable = (try reopened.getIndexCatalogRecordAlloc(alloc, "wal")).?;
    defer alloc.free(durable);
    try std.testing.expectEqualStrings("first++++", durable);
    try std.testing.expect((try reopened.check()).valid);
}

test "lite compaction catches final mutations across all roots and streams large values" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "compaction-catch-up.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    try file.putDocument("deleted", "old");
    try file.putIndexCatalogRecord("old-name", "index");
    var source = try NativeFile.openWithIo(alloc, std.testing.io, path, .{ .read_only = true, .no_sync = true });
    defer source.close();
    var capture = ChangeCapture{};
    defer capture.deinit(alloc);
    file.change_capture = &capture;
    defer file.change_capture = null;
    var image = try source.prepareVacuum(null);
    defer image.deinit();
    const large = try alloc.alloc(u8, 2 * 1024 * 1024);
    defer alloc.free(large);
    @memset(large, 123);
    try file.deleteDocument("deleted");
    try file.putDocument("large", large);
    try file.renameIndexCatalogRecord("old-name", "new-name");
    try file.putCatalogRecord("secret-head", "revision-1");
    try file.putCatalogRecord("secret-head", "revision-2");
    try file.putIndexCatalogRecord("large-index", large);
    try file.applyCapturedChanges(&image.prepared, &capture, &image.report, null);
    try file.publishVacuum(&image);
    try std.testing.expect((try file.getDocumentAlloc(alloc, "deleted")) == null);
    try std.testing.expect((try file.getIndexCatalogRecordAlloc(alloc, "old-name")) == null);
    const doc = (try file.getDocumentAlloc(alloc, "large")).?;
    defer alloc.free(doc);
    try std.testing.expectEqualSlices(u8, large, doc);
    const index = (try file.getIndexCatalogRecordAlloc(alloc, "large-index")).?;
    defer alloc.free(index);
    try std.testing.expectEqualSlices(u8, large, index);
    const secret = (try file.getCatalogRecordAlloc(alloc, "secret-head")).?;
    defer alloc.free(secret);
    try std.testing.expectEqualStrings("revision-2", secret);
    const report = try file.check();
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(report.live_file_count, image.report.live_file_count);
    try std.testing.expectEqual(report.live_bytes, image.report.live_bytes);
}

test "lite unpacked v3 remains readable and vacuum explicitly adopts packed signature" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "unpacked-v3.aflite");
    defer alloc.free(path);
    {
        var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.header.packed_records = false;
        var header: [header_size]u8 = undefined;
        encodeHeader(&header, file.header);
        try file.file.writePositionalAll(std.testing.io, &header, 0);
        try file.putDocumentBatch(&.{ .{ .key = "a", .value = "one" }, .{ .key = "b", .value = "two" } });
        try std.testing.expect(file.activeCheckpoint().document_root_page & packed_record_flag == 0);
    }
    var file = try NativeFile.openWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    try std.testing.expect(!file.header.packed_records);
    _ = try file.vacuum();
    try std.testing.expect(file.header.packed_records);
    var header: [header_size]u8 = undefined;
    try readHeaderExactAt(file.file, std.testing.io, &header);
    try std.testing.expect(!std.mem.eql(u8, header[0..magic.len], unpacked_v3_magic));
    try std.testing.expect((try decodeHeader(&header)).packed_records);
    const value = (try file.getDocumentAlloc(alloc, "b")).?;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("two", value);
    try std.testing.expect((try file.check()).valid);
}

test "lite packed record references validate boundaries and physical checksums" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "packed-record-validation.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    try file.putDocumentBatch(&.{ .{ .key = "a", .value = "one" }, .{ .key = "b", .value = "two" } });
    const checkpoint = file.activeCheckpoint();
    const reference = (try file.lookupDocumentIndexPage(checkpoint, "a")).?;
    try std.testing.expect(reference & packed_record_flag != 0);
    try std.testing.expectError(error.InvalidPageId, file.readPageAlloc(alloc, reference + (@as(u64, 1) << 47)));
    file.page_cache_enabled.store(false, .monotonic);
    try file.file.writePositionalAll(std.testing.io, "X", physicalPage(reference) * file.header.page_size + page_header_size);
    try std.testing.expectError(error.NativePageChecksumMismatch, file.getDocumentAlloc(alloc, "a"));
    try std.testing.expect(!(try file.check()).valid);
}

test "lite page replacement invalidates old bytes even when shared pressure bypasses admission" {
    const alloc = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.lite_native_page_cache)] = .{ .soft_limit_bytes = 16, .hard_limit_bytes = 24 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var cache = PageCache{ .limit_bytes = 128 };
    defer cache.deinit(alloc);
    cache.attachResourceManager(&manager);
    cache.put(alloc, 1, "old-page");
    cache.put(alloc, 2, "other");
    var external: u64 = 0;
    manager.observeUsage(.lite_native_page_cache, &external, 32);
    defer manager.observeUsage(.lite_native_page_cache, &external, 0);
    try std.testing.expectEqual(.hard, manager.sliceStats(.lite_native_page_cache).pressure);
    cache.put(alloc, 1, "new-page");
    try std.testing.expect((try cache.getCopy(alloc, 1)) == null);
    try std.testing.expectEqual(@as(usize, 0), cache.total_bytes);
}

test "lite vacuum catchup batches every root and accounts private caches through failure and publication" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "bounded-catchup.aflite");
    defer alloc.free(path);
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.lite_native_page_cache)] = .{ .soft_limit_bytes = 32768, .hard_limit_bytes = 65536 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    {
        var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true, .resource_manager = &manager });
        defer file.close();
        var image = try file.prepareVacuum(null);
        defer image.deinit();
        try std.testing.expect(image.prepared.page_cache.resource_manager == &manager);
        var capture = ChangeCapture{};
        defer capture.deinit(alloc);
        file.change_capture = &capture;
        defer file.change_capture = null;
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const keys = try arena.allocator().alloc([]const u8, 4096);
        for (keys, 0..) |*key, i| key.* = try std.fmt.allocPrint(arena.allocator(), "key-{d:0>8}", .{i});
        const docs = try arena.allocator().alloc(DocumentMutation, keys.len);
        const catalog = try arena.allocator().alloc(CatalogMutation, keys.len);
        for (keys, docs, catalog) |key, *doc, *entry| {
            doc.* = .{ .key = key, .value = "small payload" };
            entry.* = .{ .key = key, .value = "small payload" };
        }
        try file.putDocumentBatch(docs);
        try file.putCatalogBatch(catalog);
        try file.putIndexCatalogBatch(catalog);
        const before = image.prepared.activeCheckpoint();
        const report_before = image.report;
        // Fail after earlier chunks have written and changed private roots.
        image.prepared.test_page_write_fail_after = 5;
        try std.testing.expectError(error.TestPageWriteFailure, file.applyCapturedChanges(&image.prepared, &capture, &image.report, null));
        image.prepared.test_page_write_fail_after = null;
        try std.testing.expectEqualDeep(before, image.prepared.activeCheckpoint());
        try std.testing.expectEqualDeep(report_before, image.report);
        try std.testing.expectEqual(before.page_count * file.header.page_size, (try image.prepared.file.stat(std.testing.io)).size);
        try std.testing.expect((try image.prepared.check()).valid);
        var cancel = maintenance.CancelToken{};
        cancel.request();
        try std.testing.expectError(error.MaintenanceCanceled, file.applyCapturedChanges(&image.prepared, &capture, &image.report, &cancel));
        try std.testing.expectEqualDeep(report_before, image.report);
        // Retry with a streamed value that reuses IDs previously occupied by
        // cached index pages in the aborted tail.
        const large = try alloc.alloc(u8, 1024 * 1024);
        defer alloc.free(large);
        @memset(large, 42);
        try file.putCatalogRecord(keys[0], large);
        try file.applyCapturedChanges(&image.prepared, &capture, &image.report, null);
        const streamed = (try image.prepared.getCatalogRecordAlloc(alloc, keys[0])).?;
        defer alloc.free(streamed);
        try std.testing.expectEqualSlices(u8, large, streamed);
        const stats = manager.sliceStats(.lite_native_page_cache);
        try std.testing.expect(stats.used_bytes <= stats.hard_limit_bytes);
        try std.testing.expectEqual(file.page_cache.total_bytes + image.prepared.page_cache.total_bytes, stats.used_bytes);
        try std.testing.expect(image.report.after_size < file.activeCheckpoint().page_count * file.header.page_size * 2);
        try std.testing.expect(image.prepared.test_page_writes.load(.monotonic) < keys.len);
        const checked = try image.prepared.check();
        try std.testing.expect(checked.valid);
        try std.testing.expectEqual(@as(usize, 3 * keys.len), image.report.live_file_count);
        try std.testing.expectEqual(checked.live_file_count, image.report.live_file_count);
        try std.testing.expectEqual(checked.live_bytes, image.report.live_bytes);
        try file.publishVacuum(&image);
        const value = (try file.getDocumentAlloc(alloc, keys[keys.len - 1])).?;
        defer alloc.free(value);
        try std.testing.expectEqualStrings("small payload", value);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lite_native_page_cache).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lite_native_link_cache).used_bytes);
}

test "lite packed record reader reuses validated pages in both directions and rejects invalid views" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "record-page-reader.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    try file.putDocumentBatch(&.{ .{ .key = "a", .value = "one" }, .{ .key = "b", .value = "two" }, .{ .key = "c", .value = "three" } });
    const checkpoint = file.activeCheckpoint();
    const a = (try file.lookupDocumentIndexPage(checkpoint, "a")).?;
    const b = (try file.lookupDocumentIndexPage(checkpoint, "b")).?;
    const c = (try file.lookupDocumentIndexPage(checkpoint, "c")).?;
    try std.testing.expectEqual(physicalPage(a), physicalPage(c));
    var reader = RecordPageReader{};
    defer reader.deinit(alloc);
    const before = file.test_page_reads.load(.monotonic);
    for ([_]u64{ a, b, c, b, a }) |ref| _ = try reader.read(&file, alloc, checkpoint, ref, .document);
    try std.testing.expectEqual(@as(u64, 1), file.test_page_reads.load(.monotonic) - before);
    try std.testing.expectError(error.InvalidPageId, reader.read(&file, alloc, checkpoint, a + (@as(u64, 1) << 47), .document));
    try std.testing.expectError(error.InvalidNativePageKind, reader.read(&file, alloc, checkpoint, a, .catalog));
    try std.testing.expectError(error.InvalidNativePageKind, reader.read(&file, alloc, checkpoint, physicalPage(a), .document));
    try std.testing.expectEqualStrings("one", (try decodeDocumentEntry(try reader.read(&file, alloc, checkpoint, a, .document))).value);
    reader.deinit(alloc);
    file.page_cache_enabled.store(false, .monotonic);
    const raw = try file.readPhysicalPageAlloc(alloc, physicalPage(a), checkpoint);
    defer alloc.free(raw);
    try file.file.writePositionalAll(std.testing.io, "X", physicalPage(a) * file.header.page_size + page_header_size);
    try std.testing.expectError(error.NativePageChecksumMismatch, reader.read(&file, alloc, checkpoint, a, .document));
    // An unsuccessful load must not cache a validated view; retry disk truth.
    try file.file.writePositionalAll(std.testing.io, raw, physicalPage(a) * file.header.page_size);
    try std.testing.expectEqualStrings("two", (try decodeDocumentEntry(try reader.read(&file, alloc, checkpoint, b, .document))).value);
}

test "lite vacuum catchup bounds retained inline bytes and rolls back mid-copy cancellation" {
    const alloc = std.testing.allocator;
    const BudgetAllocator = @import("test_allocator.zig").BudgetAllocator;
    var budget = BudgetAllocator{ .backing = alloc };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "catchup-inline-budget.aflite");
    defer alloc.free(path);
    {
        var file = try NativeFile.createWithIo(budget.allocator(), std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        var image = try file.prepareVacuum(null);
        defer image.deinit();
        image.prepared.page_cache_enabled.store(false, .monotonic);
        var capture = ChangeCapture{};
        defer capture.deinit(budget.allocator());
        file.change_capture = &capture;
        defer file.change_capture = null;
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const docs = try arena.allocator().alloc(DocumentMutation, 4096);
        const value = @as([3000]u8, @splat(42));
        for (docs, 0..) |*doc, i| doc.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "key-{d:0>8}", .{i}), .value = &value };
        try file.putDocumentBatch(docs);
        const before = image.prepared.activeCheckpoint();
        const report_before = image.report;
        budget.peak = budget.live;
        budget.limit = budget.live + 5 * 1024 * 1024;
        var cancel = maintenance.CancelToken{};
        budget.cancel = &cancel;
        budget.cancel_after = 4000;
        try std.testing.expectError(error.MaintenanceCanceled, file.applyCapturedChanges(&image.prepared, &capture, &image.report, &cancel));
        try std.testing.expect(image.prepared.test_page_writes.load(.monotonic) > 0);
        try std.testing.expectEqualDeep(before, image.prepared.activeCheckpoint());
        try std.testing.expectEqualDeep(report_before, image.report);
        budget.cancel = null;
        try file.applyCapturedChanges(&image.prepared, &capture, &image.report, null);
        try std.testing.expect(budget.peak <= budget.limit);
        try std.testing.expectEqual(@as(usize, docs.len), image.report.live_file_count);
        const copied = (try image.prepared.getDocumentAlloc(alloc, docs[docs.len - 1].key)).?;
        defer alloc.free(copied);
        try std.testing.expectEqualSlices(u8, &value, copied);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

test "lite rollback drops only tail cache pages and independently cached links" {
    const alloc = std.testing.allocator;
    var manager = resource_manager_mod.ResourceManager.init(.{});
    var cache = PageCache{};
    defer cache.deinit(alloc);
    cache.attachResourceManager(&manager);
    cache.put(alloc, 1, "kept");
    cache.put(alloc, 2, "dropped");
    cache.put(alloc, 3, "also dropped");
    cache.putLinks(alloc, 1, .{ .kind = .value, .link_page = 0 });
    cache.putLinks(alloc, 2, .{ .kind = .value, .link_page = 0 });
    cache.putLinks(alloc, 4, .{ .kind = .value, .link_page = 0 });
    cache.discardFrom(alloc, 2);
    try std.testing.expectEqual(@as(usize, 1), cache.pages.count());
    try std.testing.expect(cache.pages.contains(1));
    try std.testing.expectEqual(@as(usize, 1), cache.links.count());
    try std.testing.expect(cache.links.contains(1));
    try std.testing.expectEqual(@as(u64, 4), manager.sliceStats(.lite_native_page_cache).used_bytes);
    try std.testing.expectEqual(cache.link_bytes, manager.sliceStats(.lite_native_link_cache).used_bytes);
}

test "lite grouped callbacks share bounded metadata finalization across roots" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "grouped-metadata.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    const before = file.activeCheckpoint();
    const writes = file.test_page_writes.load(.monotonic);
    try file.beginTransaction();
    errdefer file.abortTransaction();
    for (0..64) |i| {
        var buffer: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&buffer, "key-{d:0>4}", .{i});
        try file.putDocument(key, "document");
        try file.putCatalogRecord(key, "metadata");
        try file.putIndexCatalogRecord(key, "index");
        const staged = (try file.getIndexCatalogRecordAlloc(alloc, key)).?;
        defer alloc.free(staged);
        try std.testing.expectEqualStrings("index", staged);
    }
    // Callbacks perform no metadata I/O before finalization, despite point reads.
    try std.testing.expectEqual(writes, file.test_page_writes.load(.monotonic));
    try std.testing.expect((try file.getDocumentAtCheckpointAlloc(alloc, before, "key-0000")) == null);
    try file.commitTransaction();
    try std.testing.expectEqual(before.commit_sequence + 1, file.activeCheckpoint().commit_sequence);
    try std.testing.expect(file.test_page_writes.load(.monotonic) - writes <= 16);
    try std.testing.expect((try file.check()).valid);
    for (0..64) |i| {
        var buffer: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&buffer, "key-{d:0>4}", .{i});
        const value = (try file.getDocumentAlloc(alloc, key)).?;
        defer alloc.free(value);
        try std.testing.expectEqualStrings("document", value);
    }
}

test "lite grouped operations preserve append rename range scan and streamed import ordering" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "grouped-operations.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    try file.beginTransaction();
    errdefer file.abortTransaction();
    try file.putIndexCatalogRecord("old", "one");
    try file.appendIndexCatalogRecord("old", "+two");
    try file.renameIndexCatalogRecord("old", "new");
    try file.appendIndexCatalogRecord("new", "+three");
    try std.testing.expectEqual(@as(?usize, null), try file.getIndexCatalogRecordSize("old"));
    try std.testing.expectEqual(@as(?usize, 13), try file.getIndexCatalogRecordSize("new"));
    const range = (try file.getIndexCatalogRecordRangeAlloc(alloc, "new", 4, 3)).?;
    defer alloc.free(range);
    try std.testing.expectEqualStrings("two", range);
    const big = try alloc.alloc(u8, 128 * 1024);
    defer alloc.free(big);
    @memset(big, 'x');
    var source = try tmp.dir.createFile(std.testing.io, "source", .{ .read = true });
    defer source.close(std.testing.io);
    try source.writePositionalAll(std.testing.io, big, 0);
    try file.putIndexCatalogRecordFromFile("imported", source, big.len, .{});
    try file.appendIndexCatalogRecord("imported", big);
    try file.renameIndexCatalogRecord("imported", "renamed");
    try file.appendIndexCatalogRecord("renamed", "tail");
    const tail = (try file.getIndexCatalogRecordRangeAlloc(alloc, "renamed", big.len * 2, 4)).?;
    defer alloc.free(tail);
    try std.testing.expectEqualStrings("tail", tail);
    try std.testing.expect(file.transaction_writes.?.bytes < 1024);
    var cursor = try file.indexCatalogCursor(try file.materializeTransactionCheckpoint(), "");
    defer cursor.deinit();
    var count: usize = 0;
    while (try cursor.next()) |entry| {
        alloc.free(entry.key);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), count);
    try file.putDocument("doc", big);
    const staged = (try file.getDocumentAlloc(alloc, "doc")).?;
    defer alloc.free(staged);
    try std.testing.expectEqualSlices(u8, big, staged);
    try file.commitTransaction();
    try std.testing.expect((try file.check()).valid);
}

test "lite grouped spills bound retained memory and abort every private root" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "grouped-spills.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    try file.putDocument("survivor", "original");
    const checkpoint = file.activeCheckpoint();
    const value = @as([2048]u8, @splat('v'));
    try file.beginTransaction();
    errdefer file.abortTransaction();
    for (0..2200) |i| {
        var buffer: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&buffer, "key-{d:0>8}", .{i});
        try file.putDocument(key, &value);
        try file.putIndexCatalogRecord(key, "small");
        try std.testing.expect(file.transaction_writes.?.bytes <= TransactionWrites.max_bytes);
        try std.testing.expect(file.transaction_writes.?.count <= TransactionWrites.max_keys);
    }
    try file.deleteDocument("survivor");
    const old = (try file.getDocumentAtCheckpointAlloc(alloc, checkpoint, "survivor")).?;
    defer alloc.free(old);
    try std.testing.expectEqualStrings("original", old);
    try std.testing.expect((try file.getDocumentAlloc(alloc, "survivor")) == null);
    file.abortTransaction();
    try std.testing.expectEqual(checkpoint.page_count * file.header.page_size, (try file.file.stat(std.testing.io)).size);
    try std.testing.expect((try file.getDocumentAlloc(alloc, "key-00000000")) == null);
    // Reuse the truncated frontier with entirely different pages.
    try file.beginTransaction();
    try file.putIndexCatalogRecord("fresh", "replacement");
    try file.commitTransaction();
    try std.testing.expect((try file.check()).valid);
}

test "lite adoption existence skips system ranges and never reads value pages" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "adoption-metadata.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    try std.testing.expect(!try file.hasLiveDocumentOutsidePrefix(file.activeCheckpoint(), "\x02db/"));
    var keys: [1024][32]u8 = undefined;
    var batch: [1024]DocumentMutation = undefined;
    for (&keys, &batch, 0..) |*key, *m, i| m.* = .{ .key = try std.fmt.bufPrint(key, "\x02db/table/{d:0>8}", .{i}), .value = "system" };
    try file.putDocumentBatch(&batch);
    try file.putDocument("\x01gone", "old");
    try file.deleteDocument("\x01gone");
    const reads = file.test_page_reads.load(.monotonic);
    try std.testing.expect(!try file.hasLiveDocumentOutsidePrefix(file.activeCheckpoint(), "\x02db/"));
    try std.testing.expect(file.test_page_reads.load(.monotonic) - reads < 16);
    const value = try alloc.alloc(u8, 16 * 1024 * 1024);
    defer alloc.free(value);
    @memset(value, 123);
    try file.putDocument("user", value);
    const live = file.activeCheckpoint();
    const live_reads = file.test_page_reads.load(.monotonic);
    try std.testing.expect(try file.hasLiveDocumentOutsidePrefix(live, "\x02db/"));
    try std.testing.expect(file.test_page_reads.load(.monotonic) - live_reads < 16);
    try file.deleteDocument("user");
    try std.testing.expect(!try file.hasLiveDocumentOutsidePrefix(file.activeCheckpoint(), "\x02db/"));
    try std.testing.expect(try file.hasLiveDocumentOutsidePrefix(live, "\x02db/"));
    try file.putDocument("", "empty key");
    try std.testing.expect(try file.hasLiveDocumentOutsidePrefix(file.activeCheckpoint(), "\x02db/"));
    try std.testing.expect(!try file.hasLiveDocumentOutsidePrefix(file.activeCheckpoint(), ""));
    try file.deleteDocument("");
    try file.putDocument("\xff\xff", "excluded");
    try std.testing.expect(try file.hasLiveDocumentOutsidePrefix(file.activeCheckpoint(), "\xff")); // system docs remain live
}

test "lite grouped allocation failures roll back staging and partial finalization" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "grouped-oom.aflite");
    defer alloc.free(path);
    const Runner = struct {
        fn mutate(file: *NativeFile) !void {
            try file.beginTransaction();
            errdefer file.abortTransaction();
            try file.putDocument("doc", "first");
            try file.putDocument("doc", "last");
            try file.putCatalogRecord("meta", "settings");
            try file.putIndexCatalogRecord("index", "prefix");
            try file.appendIndexCatalogRecord("index", "suffix");
            try file.renameIndexCatalogRecord("index", "renamed");
            try file.putIndexCatalogRecord("large", &(@as([16384]u8, @splat('v'))));
            try file.appendIndexCatalogRecord("large", "tail");
            try file.commitTransaction();
        }
    };
    var fail_index: usize = 0;
    while (fail_index < 512) : (fail_index += 1) {
        var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = fail_index });
        file.allocator = failing.allocator();
        const result = Runner.mutate(&file);
        file.allocator = alloc;
        if (result) |_| {
            try std.testing.expect((try file.check()).valid);
            if (!failing.has_induced_failure) break;
        } else |err| {
            // std.Io.Writer maps allocation failures to WriteFailed.
            try std.testing.expect(failing.has_induced_failure);
            try std.testing.expect(err == error.OutOfMemory or err == error.WriteFailed);
            try std.testing.expect(file.transaction_writes == null);
            try std.testing.expectEqual(@as(u64, 0), file.activeCheckpoint().commit_sequence);
            try std.testing.expect((try file.check()).valid);
            try Runner.mutate(&file);
            try std.testing.expect((try file.check()).valid);
        }
    }
    try std.testing.expect(fail_index > 20 and fail_index < 512);
}

test "lite grouped pinned readers never inspect private staging during mutation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "grouped-pinned-readers.aflite");
    defer alloc.free(path);
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    try file.putIndexCatalogRecord("key", "original");
    try file.putDocument("key", "original");
    const Reader = struct {
        file: *NativeFile,
        checkpoint: CheckpointSlot,
        ready: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        result: anyerror!void = {},
        fn run(self: *@This()) void {
            self.result = self.read();
        }
        pub fn read(self: *@This()) !void {
            self.ready.store(true, .release);
            var iterations: usize = 0;
            while (iterations < 64 or !self.done.load(.acquire)) : (iterations += 1) {
                const a = std.testing.allocator;
                const full = (try self.file.getIndexCatalogRecordAtCheckpointAlloc(a, "key", self.checkpoint)).?;
                defer a.free(full);
                try std.testing.expectEqualStrings("original", full);
                try std.testing.expectEqual(@as(?usize, 8), try self.file.getIndexCatalogRecordSizeAtCheckpoint("key", self.checkpoint));
                const part = (try self.file.getIndexCatalogRecordRangeAtCheckpointAlloc(a, "key", 2, 3, self.checkpoint)).?;
                defer a.free(part);
                try std.testing.expectEqualStrings("igi", part);
                var values: [1]?[]const u8 = undefined;
                try self.file.getDocumentsAtCheckpointAlloc(a, self.checkpoint, &.{"key"}, &values);
                defer a.free(values[0].?);
                try std.testing.expectEqualStrings("original", values[0].?);
                var cursor = try self.file.indexCatalogCursor(self.checkpoint, "");
                defer cursor.deinit();
                const entry = (try cursor.next()).?;
                defer a.free(entry.key);
                try std.testing.expectEqualStrings("key", entry.key);
                try std.testing.expect((try cursor.next()) == null);
                try std.testing.expect(try self.file.hasLiveDocumentOutsidePrefix(self.checkpoint, "\x02db/"));
            }
        }
    };
    var reader = Reader{ .file = &file, .checkpoint = file.activeCheckpoint() };
    const thread = try std.Thread.spawn(.{}, Reader.run, .{&reader});
    var joined = false;
    defer if (!joined) {
        reader.done.store(true, .release);
        thread.join();
    };
    while (!reader.ready.load(.acquire)) @import("antfly_platform").time.yieldNow();
    for (0..8) |group| {
        try file.beginTransaction();
        errdefer file.abortTransaction();
        for (0..128) |i| {
            var buffer: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&buffer, "new-{d}-{d}", .{ group, i });
            try file.putIndexCatalogRecord(key, "new");
            try file.putIndexCatalogRecord("key", "replacement");
            try file.deleteDocument("key");
        }
        if (group % 2 == 0) try file.commitTransaction() else file.abortTransaction();
    }
    reader.done.store(true, .release);
    thread.join();
    joined = true;
    try reader.result;
    try std.testing.expect((try file.check()).valid);
}

test "lite grouped large borrowed batches retain one index edit per supplied batch" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var keys: [4096][24]u8 = undefined;
    var docs: [4096]DocumentMutation = undefined;
    var catalog: [4096]CatalogMutation = undefined;
    for (&keys, &docs, &catalog, 0..) |*key, *doc, *entry, i| {
        const name = try std.fmt.bufPrint(key, "key-{d:0>8}", .{i});
        doc.* = .{ .key = name, .value = "small" };
        entry.* = .{ .key = name, .value = "small" };
    }
    var baseline_pages: u64 = 0;
    for ([_]bool{ false, true }) |grouped| {
        const path = try testPath(alloc, tmp, if (grouped) "borrowed-group.aflite" else "borrowed-baseline.aflite");
        defer alloc.free(path);
        var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        if (grouped) try file.beginTransaction();
        errdefer if (grouped) file.abortTransaction();
        try file.putCatalogRecord("metadata", "before");
        try file.putDocumentBatch(&docs);
        try file.putIndexCatalogBatch(&catalog);
        if (grouped) {
            try std.testing.expectEqual(@as(usize, 0), file.transaction_writes.?.count);
            try file.putCatalogRecord("metadata", "after");
            try file.putDocument(docs[0].key, "last");
            try file.commitTransaction();
            const value = (try file.getDocumentAlloc(alloc, docs[0].key)).?;
            defer alloc.free(value);
            try std.testing.expectEqualStrings("last", value);
            // One trailing small mutation may copy its index path, but a
            // complete borrowed batch must not rebuild paths per 1,024 rows.
            try std.testing.expect(file.test_page_writes.load(.monotonic) <= baseline_pages + 16);
        } else baseline_pages = file.test_page_writes.load(.monotonic);
        try std.testing.expect((try file.check()).valid);
    }
}

test "lite document deletes prune the active index and preserve pinned roots" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "pruned-documents.aflite");
    defer a.free(path);
    {
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const batch = try arena.allocator().alloc(DocumentMutation, 2048);
        for (batch, 0..) |*m, i| m.* = .{
            .key = try std.fmt.allocPrint(arena.allocator(), "doc:{d:0>6}", .{i}),
            .value = "original",
        };
        try file.putDocumentBatch(batch);
        const pinned = file.activeCheckpoint();
        for (batch) |*m| m.* = .{ .key = m.key, .is_delete = true };
        // Exercise partial-leaf rebalancing and removal of whole subtrees.
        try file.putDocumentBatch(batch[0..1000]);
        var current = DocumentIndexCursor.init(&file, file.activeCheckpoint());
        defer current.deinit();
        var first = (try current.first()).?;
        defer first.deinit(a);
        try std.testing.expectEqualStrings("doc:001000", first.key);
        try std.testing.expect((try file.check()).valid);
        try file.putDocumentBatch(batch[1000..]);
        try std.testing.expectEqual(@as(u64, 0), file.activeCheckpoint().document_index_root_page);
        const reads = file.test_page_reads.load(.monotonic);
        try std.testing.expect(!try file.hasLiveDocumentOutsidePrefix(file.activeCheckpoint(), "\x02db/"));
        try std.testing.expectEqual(reads, file.test_page_reads.load(.monotonic));
        var old = DocumentIndexCursor.init(&file, pinned);
        defer old.deinit();
        var entry = try old.first();
        var count: usize = 0;
        while (entry) |e| {
            var owned = e;
            owned.deinit(a);
            count += 1;
            entry = try old.next();
        }
        try std.testing.expectEqual(batch.len, count);
        const value = (try file.getDocumentAtCheckpointAlloc(a, pinned, batch[0].key)).?;
        defer a.free(value);
        try std.testing.expectEqualStrings("original", value);
        try std.testing.expect((try file.check()).valid);
    }
    var reopened = try NativeFile.open(a, path, false);
    defer reopened.close();
    try std.testing.expect((try reopened.check()).valid);
    try reopened.putDocument("doc:reborn", "new");
    try std.testing.expect((try reopened.check()).valid);
    try reopened.deleteDocument("doc:reborn");
    _ = try reopened.vacuum();
    try std.testing.expect((try reopened.check()).valid);
    try std.testing.expectEqual(@as(u64, 0), reopened.activeCheckpoint().document_index_root_page);
}

test "lite document bulk and edited indexes retain only latest live mutations" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "delete-duplicates.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    const mutations = [_]DocumentMutation{
        .{ .key = "gone", .value = "old" },
        .{ .key = "gone", .is_delete = true },
        .{ .key = "live", .value = "old" },
        .{ .key = "live", .is_delete = true },
        .{ .key = "live", .value = "new" },
        .{ .key = "absent", .is_delete = true },
    };
    for (0..2) |_| {
        try file.putDocumentBatch(&mutations);
        try std.testing.expect((try file.lookupDocumentIndexPage(file.activeCheckpoint(), "gone")) == null);
        try std.testing.expect((try file.lookupDocumentIndexPage(file.activeCheckpoint(), "absent")) == null);
        const value = (try file.getDocumentAlloc(a, "live")).?;
        defer a.free(value);
        try std.testing.expectEqualStrings("new", value);
        try std.testing.expect((try file.check()).valid);
    }
    const before = file.activeCheckpoint();
    try file.beginTransaction();
    errdefer file.abortTransaction();
    try file.deleteDocument("live");
    const private = try file.materializeTransactionCheckpoint();
    try std.testing.expectEqual(@as(u64, 0), private.document_index_root_page);
    file.abortTransaction();
    try std.testing.expectEqual(before.document_index_root_page, file.activeCheckpoint().document_index_root_page);
    try std.testing.expect((try file.check()).valid);
    try file.deleteDocument("live");
    try file.putDocumentBatch(&.{.{ .key = "absent", .is_delete = true }});
    try std.testing.expectEqual(@as(u64, 0), file.activeCheckpoint().document_index_root_page);
    try std.testing.expect((try file.check()).valid);
}

test "lite document index coverage accepts legacy tombstones and rejects missing live keys" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "legacy-tombstone-index.aflite");
    defer a.free(path);
    {
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        try file.putDocument("doc", "old");
        const live = file.activeCheckpoint();
        var missing = live;
        missing.document_index_root_page = 0;
        try std.testing.expectError(error.InvalidDocumentIndex, file.validateDocumentIndexCoverage(missing, null));
        try file.deleteDocument("doc");
        var deleted = file.activeCheckpoint();
        // A valid old index root must still be rejected if it resurrects a
        // value superseded by a tombstone in the current history.
        deleted.document_index_root_page = live.document_index_root_page;
        try std.testing.expectError(error.InvalidDocumentIndex, file.validateDocumentIndexCoverage(deleted, null));
        // Construct the legacy v3 layout: its index retained the newest
        // tombstone. Reuse the old leaf solely to build this test fixture.
        var node = try file.readDocumentIndexNode(live.document_index_root_page, live);
        defer node.deinit(a);
        node.pointers[0] = deleted.document_root_page;
        const encoded = try encodeDocumentIndexNode(a, node);
        defer a.free(encoded);
        var page: [default_page_size]u8 = undefined;
        encodePage(&page, .document_index, encoded);
        try file.file.writePositionalAll(std.testing.io, &page, live.document_index_root_page * default_page_size);
        file.page_cache.clear(a);
        deleted.commit_sequence += 1;
        try file.publishCheckpoint(deleted);
        try std.testing.expect((try file.check()).valid);
    }
    var file = try NativeFile.open(a, path, false);
    defer file.close();
    try std.testing.expect((try file.check()).valid);
    var cursor = DocumentIndexCursor.init(&file, file.activeCheckpoint());
    defer cursor.deinit();
    var indexed = (try cursor.first()).?;
    defer indexed.deinit(a);
    var records = RecordPageReader{};
    defer records.deinit(a);
    try std.testing.expect(!try records.documentIsLive(&file, a, file.activeCheckpoint(), indexed));
    try std.testing.expect((try file.getDocumentAlloc(a, "doc")) == null);
    const snapshot = try file.snapshotDocumentsAlloc(a);
    defer NativeFile.freeSnapshotDocuments(a, snapshot);
    try std.testing.expectEqual(@as(usize, 0), snapshot.len);
    try file.deleteDocument("doc");
    try std.testing.expectEqual(@as(u64, 0), file.activeCheckpoint().document_index_root_page);
    try file.putDocument("doc", "reborn");
    try std.testing.expect((try file.check()).valid);
}

test "lite lazy overflow cursor bounds seeks scans and allocation failures" {
    const a = std.testing.allocator;
    var budget = MaintenanceTestAllocator{ .backing = a };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "lazy-overflow.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(budget.allocator(), std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const batch = try arena.allocator().alloc(DocumentMutation, 4096);
    for (batch, 0..) |*m, i| {
        const key = try arena.allocator().alloc(u8, 1000);
        @memset(key, 'x');
        _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        m.* = .{ .key = key, .value = "value" };
    }
    try file.putDocumentBatch(batch);
    const pinned = file.activeCheckpoint();
    var cursor = DocumentIndexCursor.init(&file, pinned);
    defer cursor.deinit();
    const baseline = budget.live;
    budget.limit = baseline;
    try std.testing.expectError(error.OutOfMemory, cursor.seekAtOrAfter(batch[2000].key, false));
    budget.limit = baseline + 128 * 1024;
    const reads = file.test_page_reads.load(.monotonic);
    var entry = (try cursor.seekAtOrAfter(batch[2000].key, false)).?;
    try std.testing.expectEqualSlices(u8, batch[2000].key, entry.key);
    entry.deinit(budget.allocator());
    try std.testing.expect(file.test_page_reads.load(.monotonic) - reads <= 24);
    entry = (try cursor.seekAtOrBefore(batch[2000].key, true)).?;
    try std.testing.expectEqualSlices(u8, batch[1999].key, entry.key);
    entry.deinit(budget.allocator());
    entry = (try cursor.seekAtOrAfter(batch[2000].key, true)).?;
    try std.testing.expectEqualSlices(u8, batch[2001].key, entry.key);
    entry.deinit(budget.allocator());
    var next = try cursor.first();
    var count: usize = 0;
    while (next) |e| {
        var owned = e;
        try std.testing.expectEqualSlices(u8, batch[count].key, owned.key);
        owned.deinit(budget.allocator());
        count += 1;
        next = try cursor.next();
    }
    try std.testing.expectEqual(batch.len, count);
    next = try cursor.last();
    while (next) |e| {
        var owned = e;
        count -= 1;
        try std.testing.expectEqualSlices(u8, batch[count].key, owned.key);
        owned.deinit(budget.allocator());
        next = try cursor.prev();
    }
    try std.testing.expectEqual(@as(usize, 0), count);
    budget.limit = std.math.maxInt(usize);
    try file.deleteDocument(batch[2000].key);
    entry = (try cursor.seekAtOrAfter(batch[2000].key, false)).?;
    defer entry.deinit(budget.allocator());
    try std.testing.expectEqualSlices(u8, batch[2000].key, entry.key);
    try std.testing.expect((try file.check()).valid);
    // Full audits must still validate overflow references that a narrow seek
    // need not visit.
    const checkpoint = file.activeCheckpoint();
    var root = try file.readDocumentIndexNode(checkpoint.document_index_root_page, checkpoint);
    defer root.deinit(budget.allocator());
    root.key_pages.?[0] = checkpoint.page_count;
    const encoded = try encodeDocumentIndexNode(budget.allocator(), root);
    defer budget.allocator().free(encoded);
    try file.writePage(checkpoint.document_index_root_page, .document_index, encoded);
    try std.testing.expect(!(try file.check()).valid);
}

test "lite append frontiers write the same pages as one combined append" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "append-frontier.aflite");
    defer a.free(path);
    const initial = try a.alloc(u8, 1024 * 1024);
    defer a.free(initial);
    @memset(initial, 'i');
    const suffix = @as([(1024 * 64)]u8, @splat('s'));
    var combined_pages: u64 = 0;
    for ([_]bool{ true, false }) |combined| {
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        try file.putIndexCatalogRecord("/wal", initial);
        const pinned = file.activeCheckpoint();
        const before = file.test_page_writes.load(.monotonic);
        try file.beginTransaction();
        errdefer file.abortTransaction();
        if (combined) try file.appendIndexCatalogRecord("/wal", &suffix) else {
            for (0..1024) |_| try file.appendIndexCatalogRecord("/wal", suffix[0..64]);
        }
        // Size checks consult buffered length without sealing the frontier.
        try std.testing.expectEqual(@as(?usize, initial.len + suffix.len), try file.getIndexCatalogRecordSize("/wal"));
        try std.testing.expectEqual(@as(usize, 1), file.transaction_writes.?.append_count);
        try std.testing.expectEqual(@as(?usize, initial.len), try file.getIndexCatalogRecordSizeAtCheckpoint("/wal", pinned));
        try file.commitTransaction();
        const pages = file.test_page_writes.load(.monotonic) - before;
        if (combined) combined_pages = pages else try std.testing.expectEqual(combined_pages, pages);
        try std.testing.expect(pages <= 24);
        const tail = (try file.getIndexCatalogRecordRangeAlloc(a, "/wal", initial.len, suffix.len)).?;
        defer a.free(tail);
        try std.testing.expectEqualSlices(u8, &suffix, tail);
        try std.testing.expect((try file.check()).valid);
    }
}

test "lite append frontiers preserve barriers replacements and bounded spills" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "append-barriers.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    const base = @as([8192]u8, @splat('b'));
    const keys = [_][]const u8{ "/a", "/b", "/c", "/d", "/e", "/f" };
    for (keys) |key| try file.putIndexCatalogRecord(key, &base);
    const pinned = file.activeCheckpoint();
    try file.beginTransaction();
    errdefer file.abortTransaction();
    for (keys) |key| {
        try file.appendIndexCatalogRecord(key, "suffix");
        try std.testing.expect(file.transaction_writes.?.append_count <= TransactionWrites.max_append_files);
    }
    const range = (try file.getIndexCatalogRecordRangeAlloc(a, "/e", base.len, 6)).?;
    defer a.free(range);
    try std.testing.expectEqualStrings("suffix", range);
    // Reading one file must not finalize the other retained frontier.
    try std.testing.expectEqual(@as(usize, 1), file.transaction_writes.?.append_count);
    try file.appendIndexCatalogRecord("/e", "again");
    try file.renameIndexCatalogRecord("/e", "/f");
    try file.appendIndexCatalogRecord("/f", "last");
    try file.putIndexCatalogRecord("/a", "replacement");
    try file.deleteIndexCatalogRecord("/b");
    const snapshot = try file.materializeTransactionCheckpoint();
    try std.testing.expectEqual(@as(?usize, base.len + 15), try file.getIndexCatalogRecordSizeAtCheckpoint("/f", snapshot));
    try file.commitTransaction();
    const value = (try file.getIndexCatalogRecordRangeAlloc(a, "/f", base.len, 15)).?;
    defer a.free(value);
    try std.testing.expectEqualStrings("suffixagainlast", value);
    try std.testing.expect((try file.getIndexCatalogRecordSize("/e")) == null);
    try std.testing.expect((try file.getIndexCatalogRecordSize("/b")) == null);
    try std.testing.expectEqual(@as(?usize, "replacement".len), try file.getIndexCatalogRecordSize("/a"));
    try std.testing.expectEqual(@as(?usize, base.len), try file.getIndexCatalogRecordSizeAtCheckpoint("/a", pinned));
    try std.testing.expect((try file.check()).valid);
    const before = file.activeCheckpoint();
    try file.beginTransaction();
    try file.appendIndexCatalogRecord("/f", "discard");
    _ = try file.materializeTransactionCheckpoint();
    file.abortTransaction();
    try std.testing.expectEqual(before.index_catalog_root_page, file.activeCheckpoint().index_catalog_root_page);
    try std.testing.expect((try file.check()).valid);
}

test "lite append frontier allocation and streaming failures roll back and retry" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "append-failures.aflite");
    defer a.free(path);
    const Runner = struct {
        fn apply(file: *NativeFile) !void {
            try file.beginTransaction();
            errdefer file.abortTransaction();
            for (0..64) |_| {
                try file.appendIndexCatalogRecord("/a", "12345678");
                try file.appendIndexCatalogRecord("/b", "abcdefgh");
            }
            const range = (try file.getIndexCatalogRecordRangeAlloc(file.allocator, "/a", 8192, 512)).?;
            defer file.allocator.free(range);
            try file.appendIndexCatalogRecord("/a", "more");
            try file.renameIndexCatalogRecord("/b", "/renamed");
            _ = try file.materializeTransactionCheckpoint();
            try file.appendIndexCatalogRecord("/a", "tail");
            try file.commitTransaction();
        }
    };
    var exhausted = false;
    for (0..512) |fail_index| {
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        const base = @as([8192]u8, @splat('v'));
        try file.putIndexCatalogRecord("/a", &base);
        try file.putIndexCatalogRecord("/b", &base);
        const before = file.activeCheckpoint();
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = fail_index });
        file.allocator = failing.allocator();
        const result = Runner.apply(&file);
        file.allocator = a;
        if (result) |_| {
            exhausted = !failing.has_induced_failure;
        } else |err| {
            try std.testing.expect(failing.has_induced_failure);
            try std.testing.expect(err == error.OutOfMemory or err == error.WriteFailed);
            try std.testing.expectEqual(before.index_catalog_root_page, file.activeCheckpoint().index_catalog_root_page);
            try std.testing.expect((try file.check()).valid);
            try Runner.apply(&file);
        }
        try std.testing.expectEqual(@as(?usize, base.len + 520), try file.getIndexCatalogRecordSize("/a"));
        try std.testing.expectEqual(@as(?usize, base.len + 512), try file.getIndexCatalogRecordSize("/renamed"));
        try std.testing.expect((try file.check()).valid);
        if (exhausted) break;
    }
    try std.testing.expect(exhausted);
    var file = try NativeFile.openWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    const before = file.activeCheckpoint();
    try file.beginTransaction();
    file.test_page_write_fail_after = 0;
    try std.testing.expectError(error.TestPageWriteFailure, file.appendIndexCatalogRecord("/a", &(@as([(128 * 1024)]u8, @splat('s')))));
    file.test_page_write_fail_after = null;
    file.abortTransaction();
    try std.testing.expectEqual(before.index_catalog_root_page, file.activeCheckpoint().index_catalog_root_page);
    try std.testing.expect((try file.check()).valid);
    try file.beginTransaction();
    try file.appendIndexCatalogRecord("/a", "retry");
    try file.commitTransaction();
    try std.testing.expect((try file.check()).valid);
}

test "lite append frontiers span inline leaf and full subtree boundaries" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "append-boundaries.aflite");
    defer a.free(path);
    const leaf_size = default_page_size - page_header_size - value_page_header_size;
    for ([_]usize{ 0, 1, leaf_size, leaf_size * 64 - 1, leaf_size * 64, leaf_size * 65 }) |initial_len| {
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        const initial = try a.alloc(u8, initial_len);
        defer a.free(initial);
        @memset(initial, 'i');
        try file.putIndexCatalogRecord("file", initial);
        const pinned = file.activeCheckpoint();
        try file.beginTransaction();
        errdefer file.abortTransaction();
        for (0..127) |_| try file.appendIndexCatalogRecord("file", &(@as([127]u8, @splat('s'))));
        try file.appendIndexCatalogRecord("file", "");
        try file.commitTransaction();
        const value = (try file.getIndexCatalogRecordAlloc(a, "file")).?;
        defer a.free(value);
        try std.testing.expectEqual(@as(usize, initial_len + 127 * 127), value.len);
        try std.testing.expectEqualSlices(u8, initial, value[0..initial_len]);
        for (value[initial_len..]) |byte| try std.testing.expectEqual(@as(u8, 's'), byte);
        try std.testing.expectEqual(@as(?usize, initial_len), try file.getIndexCatalogRecordSizeAtCheckpoint("file", pinned));
        try std.testing.expect((try file.check()).valid);
    }
}

test "lite value reads bound allocations and coalesce physical IO across pinned roots" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "value-read-window.aflite");
    defer a.free(path);
    var counter = std.testing.FailingAllocator.init(a, .{});
    var file = try NativeFile.createWithIo(counter.allocator(), std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    const value = try a.alloc(u8, 8 * 1024 * 1024);
    defer a.free(value);
    for (value, 0..) |*byte, i| byte.* = @truncate(i);
    try file.putDocument("key", value);
    try file.putIndexCatalogRecord("/key", value);
    const pinned = file.activeCheckpoint();
    try file.putDocument("key", "replacement");
    try file.putIndexCatalogRecord("/key", "replacement");
    for ([_]bool{ false, true }) |catalog| {
        const allocations = counter.alloc_index;
        const calls = file.test_value_read_calls.load(.monotonic);
        const got = (if (catalog)
            try file.getIndexCatalogRecordAtCheckpointAlloc(counter.allocator(), "/key", pinned)
        else
            try file.getDocumentAtCheckpointAlloc(counter.allocator(), pinned, "key")).?;
        defer counter.allocator().free(got);
        try std.testing.expectEqualSlices(u8, value, got);
        try std.testing.expect(counter.alloc_index - allocations < 16);
        try std.testing.expect(file.test_value_read_calls.load(.monotonic) - calls < 200);
    }
    const range = (try file.getIndexCatalogRecordRangeAtCheckpointAlloc(a, "/key", 4031, 65555, pinned)).?;
    defer a.free(range);
    try std.testing.expectEqualSlices(u8, value[4031..][0..65555], range);
    // OOM must release the output and any extent traversal state.
    var exhausted = false;
    for (0..32) |fail_index| {
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = fail_index });
        const original = file.allocator;
        file.allocator = failing.allocator();
        const result = file.getIndexCatalogRecordAtCheckpointAlloc(failing.allocator(), "/key", pinned);
        file.allocator = original;
        if (result) |bytes| {
            failing.allocator().free(bytes.?);
            exhausted = !failing.has_induced_failure;
        } else |err| try std.testing.expectEqual(error.OutOfMemory, err);
        if (exhausted) break;
    }
    try std.testing.expect(exhausted);
}

test "lite namespace index bounds hot and cold single namespace mutations" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "namespace-index-bounds.aflite");
    defer a.free(path);
    var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    var file = try NativeFile.createWithIo(budget.allocator(), std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const batch = try arena.allocator().alloc(DocumentMutation, 16384);
    for (batch, 0..) |*mutation, i| mutation.* = .{
        .key = try std.fmt.allocPrint(arena.allocator(), "ns-{d:0>8}\x00key", .{i}),
        .value = "value",
    };
    try file.putDocumentBatch(batch);
    const pinned = file.activeCheckpoint();
    for (0..2) |phase| {
        if (phase != 0) {
            file.close();
            file = try NativeFile.openWithIo(budget.allocator(), std.testing.io, path, .{ .no_sync = true });
            file.page_cache_enabled.store(false, .monotonic);
        }
        const baseline = budget.live;
        budget.limit = baseline + 256 * 1024;
        budget.peak = baseline;
        for (0..260) |_| {
            const writes = file.test_page_writes.load(.monotonic);
            try file.beginTransaction();
            try file.putDocument(batch[0].key, "changed");
            try file.commitTransaction();
            try std.testing.expect(file.test_page_writes.load(.monotonic) - writes <= 16);
        }
        budget.limit = std.math.maxInt(usize);
        try std.testing.expectEqual(@as(u32, 0), file.namespace_directory_cache.count());
    }
    const old = (try file.getDocumentAtCheckpointAlloc(a, pinned, batch[0].key)).?;
    defer a.free(old);
    try std.testing.expectEqualStrings("value", old);
    try std.testing.expect((try file.check()).valid);
    const before = file.activeCheckpoint();
    const size = (try file.file.stat(std.testing.io)).size;
    try file.beginTransaction();
    try file.putDocument(batch[0].key, "aborted");
    _ = try file.materializeTransactionCheckpoint();
    file.abortTransaction();
    try std.testing.expectEqualDeep(before, file.activeCheckpoint());
    try std.testing.expectEqual(size, (try file.file.stat(std.testing.io)).size);
    _ = try file.vacuum();
    try std.testing.expect((try file.readCatalogRoots(file.activeCheckpoint().namespace_directory_root_page, file.activeCheckpoint())).indexed);
    try std.testing.expect((try file.check()).valid);
}

// Construct the legacy v3 snapshot/delta representation independently of the
// current writer, including its external-value encoding when needed.
fn legacyNamespaceDirectoryForTest(file: *NativeFile) !CheckpointSlot {
    std.debug.assert(builtin.is_test);
    var directory = (try file.loadNamespaceDirectoryAlloc(file.allocator)).?;
    defer NativeFile.deinitNamespaceDirectory(file.allocator, &directory);
    var next = file.activeCheckpoint();
    var pages = try file.pageAllocatorFromFreeMap(next);
    defer pages.deinit();
    var root: u64 = 0;
    for ([_]NativeFile.NamespaceDirectoryRecordKind{ .snapshot, .delta }) |kind| {
        const encoded = try NativeFile.encodeNamespaceDirectoryAlloc(file.allocator, kind, &directory);
        defer file.allocator.free(encoded);
        const external = if (file.catalogEntryFitsInline(namespace_directory_key, encoded)) 0 else try file.writeValuePagesAllocated(&pages, encoded);
        var payload = std.ArrayListUnmanaged(u8).empty;
        defer payload.deinit(file.allocator);
        try encodeCatalogEntry(file.allocator, &payload, .{ .previous_page = root, .key = namespace_directory_key, .value = encoded, .external_value_root_page = external });
        root = try pages.allocate();
        try pages.writePage(root, .catalog, payload.items);
    }
    next.namespace_directory_root_page = root;
    next.free_map_root_page = try pages.allocate();
    next.page_count = pages.next_page_id;
    next.commit_sequence += 1;
    try pages.flush();
    try file.writeFreeMapPage(next.free_map_root_page, next.page_count, pages.remainingFreePages());
    try file.publishCheckpoint(next);
    return next;
}

test "lite namespace index migrates legacy v3 directories atomically and preserves old checkpoints" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |use_packing| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "namespace-migration.aflite");
        defer a.free(path);
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.header.packed_records = use_packing;
        var header: [header_size]u8 = undefined;
        encodeHeader(&header, file.header);
        try file.file.writePositionalAll(std.testing.io, &header, 0);
        file.page_cache_enabled.store(false, .monotonic);
        var long_key: [2048]u8 = @splat('n');
        long_key[1800] = 0;
        try file.putDocumentBatch(&.{
            .{ .key = "plain", .value = "root" },
            .{ .key = "ns\x00key", .value = "old" },
            .{ .key = &long_key, .value = "long" },
        });
        for (0..40) |i| {
            var key: [40]u8 = undefined;
            try file.putDocument(try std.fmt.bufPrint(&key, "extra-{d}\x00key", .{i}), "value");
        }
        const legacy = try legacyNamespaceDirectoryForTest(&file);
        try std.testing.expect(!(try file.readCatalogRoots(legacy.namespace_directory_root_page, legacy)).indexed);
        try std.testing.expect((try file.check()).valid);
        const length = (try file.file.stat(std.testing.io)).size;
        // A failed migration must restore the legacy root and every tail page.
        try file.beginTransaction();
        try file.putDocument("ns\x00key", "aborted");
        _ = try file.materializeTransactionCheckpoint();
        file.abortTransaction();
        try std.testing.expectEqualDeep(legacy, file.activeCheckpoint());
        try std.testing.expectEqual(length, (try file.file.stat(std.testing.io)).size);
        try std.testing.expect((try file.check()).valid);
        var exhausted = false;
        for (0..2048) |fail_index| {
            var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = fail_index });
            file.allocator = failing.allocator();
            const Run = struct {
                fn apply(f: *NativeFile) !void {
                    try f.beginTransaction();
                    errdefer f.abortTransaction();
                    try f.putDocument("ns\x00key", "new");
                    try f.commitTransaction();
                }
            };
            const result = Run.apply(&file);
            file.allocator = a;
            if (result) |_| {
                exhausted = !failing.has_induced_failure;
                try std.testing.expect(exhausted);
            } else |err| {
                try std.testing.expectEqual(error.OutOfMemory, err);
                try std.testing.expectEqualDeep(legacy, file.activeCheckpoint());
                try std.testing.expectEqual(length, (try file.file.stat(std.testing.io)).size);
            }
            try std.testing.expect((try file.check()).valid);
            if (exhausted) break;
        }
        try std.testing.expect(exhausted);
        try std.testing.expect((try file.readCatalogRoots(file.activeCheckpoint().namespace_directory_root_page, file.activeCheckpoint())).indexed);
        var old = (try file.loadNamespaceDirectoryAtCheckpointAlloc(a, legacy)).?;
        defer NativeFile.deinitNamespaceDirectory(a, &old);
        try std.testing.expectEqual(@as(u32, 43), old.count());
        const old_value = (try file.getDocumentAtCheckpointAlloc(a, legacy, "ns\x00key")).?;
        defer a.free(old_value);
        try std.testing.expectEqualStrings("old", old_value);
        file.close();
        file = try NativeFile.openWithIo(a, std.testing.io, path, .{ .no_sync = true });
        try file.putDocument(&long_key, "updated");
        try std.testing.expect((try file.check()).valid);
        _ = try file.vacuum();
        try std.testing.expect((try file.check()).valid);
    }
}

test "lite indexed namespace directory allocation failures release keys exactly once" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "namespace-load-oom.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const batch = try arena.allocator().alloc(DocumentMutation, 40);
    for (batch, 0..) |*m, i| m.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "ns-{d:0>8}\x00key", .{i}), .value = "v" };
    try file.putDocumentBatch(batch);
    var exhausted = false;
    for (0..512) |index| {
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = index });
        file.allocator = failing.allocator();
        defer file.allocator = a;
        if (file.loadNamespaceDirectoryAlloc(failing.allocator())) |loaded| {
            var directory = loaded.?;
            defer NativeFile.deinitNamespaceDirectory(failing.allocator(), &directory);
            try std.testing.expectEqual(@as(u32, 40), directory.count());
            exhausted = !failing.has_induced_failure;
        } else |err| try std.testing.expectEqual(error.OutOfMemory, err);
        if (exhausted) break;
    }
    try std.testing.expect(exhausted);
    try std.testing.expect((try file.check()).valid);
}

test "lite value windows cache requested pages and retain warm extent metadata" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |metadata_only| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "value-window-cache.aflite");
        defer a.free(path);
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        file.page_cache_policy = if (metadata_only) .metadata_only else .normal;
        const value = try a.alloc(u8, 1024 * 1024);
        defer a.free(value);
        @memset(value, 'v');
        try file.putIndexCatalogRecord("wal", value);
        try file.putDocument("chain", value);
        const checkpoint = file.activeCheckpoint();
        const record = (try file.lookupDocumentIndexPage(checkpoint, "chain")).?;
        const raw = try file.readPagePayloadByKindAlloc(a, record, .document);
        defer a.free(raw);
        const root = (try decodeDocumentEntry(raw)).external_value_root_page;
        file.page_cache_enabled.store(true, .monotonic);
        var reader = NativeFile.ValuePageReader{};
        defer reader.deinit(&file);
        _ = try reader.read(&file, checkpoint, root, 16);
        try std.testing.expectEqual(!metadata_only, file.page_cache.pages.contains(root));
        try std.testing.expect(!file.page_cache.pages.contains(root + 1));
        const calls = file.test_value_read_calls.load(.monotonic);
        _ = try reader.read(&file, checkpoint, root + 1, 16);
        try std.testing.expectEqual(calls, file.test_value_read_calls.load(.monotonic));
        try std.testing.expectEqual(!metadata_only, file.page_cache.pages.contains(root + 1));
        try std.testing.expect(!file.page_cache.pages.contains(root + 2));
        for (0..2) |pass| {
            const before = file.test_value_read_calls.load(.monotonic);
            for (0..100) |_| {
                const got = (try file.getIndexCatalogRecordRangeAlloc(a, "wal", 8192, 16)).?;
                defer a.free(got);
                try std.testing.expectEqualSlices(u8, value[8192..][0..16], got);
            }
            if (pass == 1) try std.testing.expectEqual(@as(u64, if (metadata_only) 100 else 0), file.test_value_read_calls.load(.monotonic) - before);
        }
    }
}

test "lite extent read ahead bounds bytes across fragmented appends and ranges" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |fragmented| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "fragmented-read.aflite");
        defer a.free(path);
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        const value = try a.alloc(u8, 1024 * 1024);
        defer a.free(value);
        for (value, 0..) |*byte, i| byte.* = @truncate(i);
        if (fragmented) {
            try file.putIndexCatalogRecord("wal", value[0..4096]);
            for (1..256) |i| try file.appendIndexCatalogRecord("wal", value[i * 4096 ..][0..4096]);
        } else try file.putIndexCatalogRecord("wal", value);
        const pinned = file.activeCheckpoint();
        try file.putIndexCatalogRecord("wal", "replacement");
        const before = file.test_value_read_bytes.load(.monotonic);
        const calls = file.test_value_read_calls.load(.monotonic);
        const got = (try file.getIndexCatalogRecordAtCheckpointAlloc(a, "wal", pinned)).?;
        defer a.free(got);
        try std.testing.expectEqualSlices(u8, value, got);
        try std.testing.expect(file.test_value_read_bytes.load(.monotonic) - before < value.len + 128 * 1024);
        if (!fragmented) try std.testing.expect(file.test_value_read_calls.load(.monotonic) - calls < 40);
        const range_before = file.test_value_read_bytes.load(.monotonic);
        const range = (try file.getIndexCatalogRecordRangeAtCheckpointAlloc(a, "wal", 4031, 65555, pinned)).?;
        defer a.free(range);
        try std.testing.expectEqualSlices(u8, value[4031..][0..65555], range);
        try std.testing.expect(file.test_value_read_bytes.load(.monotonic) - range_before < range.len + 32 * 1024);
    }
}

test "lite chain read ahead learns adjacency and resets at gaps" {
    const a = std.testing.allocator;
    for ([_]u64{ 1, 3 }) |stride| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "chain-read-locality.aflite");
        defer a.free(path);
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        var ids: [128]u64 = undefined;
        for (&ids, 0..) |*id, i| id.* = 1 + i * stride;
        const chunk = try a.alloc(u8, file.maxValuePagePayloadBytes());
        defer a.free(chunk);
        @memset(chunk, 'v');
        for (ids, 0..) |_, i| try file.writeValuePageChunk(&ids, i, chunk, .{});
        var checkpoint = file.activeCheckpoint();
        checkpoint.page_count = ids[ids.len - 1] + 1;
        const before = file.test_value_read_bytes.load(.monotonic);
        const calls = file.test_value_read_calls.load(.monotonic);
        const got = try file.readValuePagesAtCheckpointAlloc(a, ids[0], chunk.len * ids.len, checkpoint);
        defer a.free(got);
        for (got) |byte| try std.testing.expectEqual(@as(u8, 'v'), byte);
        try std.testing.expectEqual(@as(u64, ids.len * default_page_size), file.test_value_read_bytes.load(.monotonic) - before);
        if (stride == 1) try std.testing.expect(file.test_value_read_calls.load(.monotonic) - calls < 20);
    }
}

test "lite inline namespace cache survives hot commits and rolls back promotion" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "inline-cache.aflite");
    defer a.free(path);
    var counter = std.testing.FailingAllocator.init(a, .{});
    var file = try NativeFile.createWithIo(counter.allocator(), std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const batch = try arena.allocator().alloc(DocumentMutation, 32);
    for (batch, 0..) |*m, i| m.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "ns-{d:0>8}\x00key", .{i}), .value = "v" };
    try file.putDocumentBatch(batch);
    const namespace = NativeFile.documentNamespace(batch[0].key);
    const owned_key = file.namespace_directory_cache.getKey(namespace).?;
    const before = counter.alloc_index;
    for (0..200) |_| {
        try file.putDocument(batch[0].key, "new");
        try std.testing.expect(owned_key.ptr == file.namespace_directory_cache.getKey(namespace).?.ptr);
        try std.testing.expectEqual(file.activeCheckpoint().namespace_directory_root_page, file.namespace_directory_cache_root);
    }
    try std.testing.expect(counter.alloc_index - before < 6000);
    const pinned = file.activeCheckpoint();
    try file.beginTransaction();
    try file.putDocument("extra\x00key", "abort");
    _ = try file.materializeTransactionCheckpoint();
    try std.testing.expectEqual(@as(u32, 0), file.namespace_directory_cache.count());
    file.abortTransaction();
    try std.testing.expectEqualDeep(pinned, file.activeCheckpoint());
    try file.putDocument(batch[0].key, "after abort");
    try std.testing.expectEqual(@as(u32, 32), file.namespace_directory_cache.count());
    try std.testing.expect((try file.check()).valid);
    file.close();
    file = try NativeFile.openWithIo(counter.allocator(), std.testing.io, path, .{ .no_sync = true });
    try file.putDocument("extra\x00key", "committed");
    try std.testing.expectEqual(@as(u32, 0), file.namespace_directory_cache.count());
    try std.testing.expect((try file.check()).valid);
}

test "lite inline namespace cache prepares ownership before publication under OOM" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "inline-cache-oom.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    try file.putDocument("old\x00key", "before");
    const pinned = file.activeCheckpoint();
    var exhausted = false;
    for (0..512) |index| {
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = index });
        file.allocator = failing.allocator();
        defer file.allocator = a;
        try file.beginTransaction();
        defer if (file.transaction_header != null) file.abortTransaction();
        const Attempt = struct {
            fn run(f: *NativeFile) !void {
                try f.putDocumentBatch(&.{ .{ .key = "old\x00key", .value = "after" }, .{ .key = "new\x00key", .value = "new" } });
                _ = try f.materializeTransactionCheckpoint();
            }
        };
        if (Attempt.run(&file)) |_| {
            exhausted = !failing.has_induced_failure;
            try std.testing.expectEqual(@as(u32, 2), file.namespace_directory_cache.count());
            try std.testing.expectEqual(file.activeCheckpoint().namespace_directory_root_page, file.namespace_directory_cache_root);
        } else |err| {
            try std.testing.expect(failing.has_induced_failure);
            // The allocating inline encoder reports OOM through Writer.
            try std.testing.expect(err == error.OutOfMemory or err == error.WriteFailed);
        }
        file.abortTransaction();
        try std.testing.expectEqualDeep(pinned, file.activeCheckpoint());
        try std.testing.expectEqual(@as(u32, 0), file.namespace_directory_cache.count());
        if (exhausted) break;
    }
    try std.testing.expect(exhausted);
    try file.putDocument("new\x00key", "committed");
    const old = (try file.getDocumentAlloc(a, "old\x00key")).?;
    defer a.free(old);
    try std.testing.expectEqualStrings("before", old);
    try std.testing.expectEqual(@as(u32, 2), file.namespace_directory_cache.count());
    try std.testing.expect((try file.check()).valid);
}

test "lite namespace batch reads share tree paths and physically regroup packed records" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "namespace-batch-reads.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const count = 16384;
    const batch = try arena.allocator().alloc(DocumentMutation, count);
    for (batch, 0..) |*m, i| m.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "ns-{d:0>8}\x00key", .{i}), .value = "old" };
    try file.putDocumentBatch(batch);
    const pinned = file.activeCheckpoint();
    for (batch) |*m| m.value = "new";
    const before = file.test_page_reads.load(.monotonic);
    try file.putDocumentBatch(batch);
    try std.testing.expect(file.test_page_reads.load(.monotonic) - before < 700);
    // Interleaved namespace updates make key order alternate between physical
    // pages. A reader that only follows sorted keys would reread each bundle.
    const group = try arena.allocator().alloc(DocumentMutation, count / 8);
    for (0..8) |phase| {
        for (group, 0..) |*m, i| m.* = .{ .key = batch[i * 8 + phase].key, .value = "interleaved" };
        try file.putDocumentBatch(group);
    }
    file.close();
    file = try NativeFile.openWithIo(a, std.testing.io, path, .{ .no_sync = true });
    file.page_cache_enabled.store(false, .monotonic);
    const cold_before = file.test_page_reads.load(.monotonic);
    try file.putDocumentBatch(batch);
    try std.testing.expect(file.test_page_reads.load(.monotonic) - cold_before < 700);
    const old = (try file.getDocumentAtCheckpointAlloc(a, pinned, batch[0].key)).?;
    defer a.free(old);
    try std.testing.expectEqualStrings("old", old);
    const current = (try file.getDocumentAlloc(a, batch[count - 1].key)).?;
    defer a.free(current);
    try std.testing.expectEqualStrings("new", current);
    try std.testing.expect((try file.check()).valid);
}

test "lite namespace batch tracking memory depends on namespaces not documents" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "namespace-batch-memory.aflite");
    defer a.free(path);
    var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    var file = try NativeFile.createWithIo(budget.allocator(), std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const batch = try arena.allocator().alloc(DocumentMutation, 65536);
    for (batch, 0..) |*m, i| m.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "single\x00key-{d:0>8}", .{i}), .value = "v" };
    const baseline = budget.live;
    budget.peak = baseline;
    budget.limit = baseline + 4 * 1024 * 1024;
    defer budget.limit = std.math.maxInt(usize);
    // Includes the document index builder, not just the namespace map.
    try file.beginTransaction();
    errdefer file.abortTransaction();
    try file.putDocumentBatch(batch);
    try file.commitTransaction();
    try std.testing.expect(budget.peak - baseline < 4 * 1024 * 1024);
    budget.limit = std.math.maxInt(usize);
    try std.testing.expect((try file.check()).valid);
}

test "lite namespace batch resolution preserves mutation order and aborts allocation failures" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |packed_records| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "namespace-batch-ownership.aflite");
        defer a.free(path);
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.header.packed_records = packed_records;
        var header: [header_size]u8 = undefined;
        encodeHeader(&header, file.header);
        try file.file.writePositionalAll(std.testing.io, &header, 0);
        file.page_cache_enabled.store(false, .monotonic);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const seed = try arena.allocator().alloc(DocumentMutation, 48);
        for (seed, 0..) |*m, i| m.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "ns-{d:0>8}\x00key", .{i}), .value = "old" };
        var long_key: [1804]u8 = @splat('z');
        long_key[1800] = 0;
        try file.putDocumentBatch(seed);
        try file.putDocumentBatch(&.{ .{ .key = "plain", .value = "old" }, .{ .key = &long_key, .value = "old" } });
        const pinned = file.activeCheckpoint();
        const size = (try file.file.stat(std.testing.io)).size;
        const mutations = [_]DocumentMutation{
            .{ .key = seed[5].key, .value = "intermediate" },
            .{ .key = "added\x00key", .value = "first" },
            .{ .key = seed[7].key, .is_delete = true },
            .{ .key = &long_key, .value = "long final" },
            .{ .key = seed[5].key, .is_delete = true },
            .{ .key = "plain", .value = "root final" },
            .{ .key = "added\x00key", .value = "new final" },
            .{ .key = seed[5].key, .value = "final" },
        };
        var exhausted = false;
        for (0..2048) |index| {
            var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = index });
            file.allocator = failing.allocator();
            defer file.allocator = a;
            try file.beginTransaction();
            defer if (file.transaction_header != null) file.abortTransaction();
            const Attempt = struct {
                fn run(f: *NativeFile, batch: []const DocumentMutation) !void {
                    try f.putDocumentBatch(batch);
                    _ = try f.materializeTransactionCheckpoint();
                }
            };
            if (Attempt.run(&file, &mutations)) |_| {
                exhausted = !failing.has_induced_failure;
            } else |err| {
                try std.testing.expect(failing.has_induced_failure);
                try std.testing.expect(err == error.OutOfMemory or err == error.WriteFailed);
            }
            file.abortTransaction();
            try std.testing.expectEqualDeep(pinned, file.activeCheckpoint());
            try std.testing.expectEqual(size, (try file.file.stat(std.testing.io)).size);
            if (exhausted) break;
        }
        try std.testing.expect(exhausted);
        try std.testing.expect((try file.check()).valid);
        try file.putDocumentBatch(&mutations);
        const keys = [_][]const u8{ seed[5].key, "added\x00key", &long_key, "plain" };
        const expected = [_][]const u8{ "final", "new final", "long final", "root final" };
        for (keys, expected) |key, value| {
            const got = (try file.getDocumentAlloc(a, key)).?;
            defer a.free(got);
            try std.testing.expectEqualStrings(value, got);
        }
        try std.testing.expectEqual(@as(?[]u8, null), try file.getDocumentAlloc(a, seed[7].key));
        const old = (try file.getDocumentAtCheckpointAlloc(a, pinned, seed[5].key)).?;
        defer a.free(old);
        try std.testing.expectEqualStrings("old", old);
        try std.testing.expect((try file.check()).valid);
        _ = try file.vacuum();
        try std.testing.expect((try file.check()).valid);
    }
}

test "lite index views bound batch and cursor allocations and reuse seek buffers" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/reads.aflite", .{tmp.sub_path});
    defer a.free(path);
    var counter = std.testing.FailingAllocator.init(a, .{});
    const alloc = counter.allocator();
    var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const count = 16384;
    const batch = try arena.allocator().alloc(DocumentMutation, count);
    const keys = try arena.allocator().alloc([]const u8, count);
    const values = try arena.allocator().alloc(?[]const u8, count);
    for (batch, keys, 0..) |*m, *key, i| {
        key.* = try std.fmt.allocPrint(arena.allocator(), "ns\x00key-{d:0>8}", .{i});
        m.* = .{ .key = key.*, .value = "value" };
    }
    try file.putDocumentBatch(batch);
    const cp = file.activeCheckpoint();
    const point_before = counter.alloc_index;
    const point = (try file.getDocumentAtCheckpointAlloc(alloc, cp, keys[0])).?;
    try std.testing.expectEqualStrings("value", point);
    alloc.free(point);
    try std.testing.expect(counter.alloc_index - point_before <= 4);
    for ([_]usize{ 1, 64, count }) |num| {
        const before = counter.alloc_index;
        const reads = file.test_page_reads.load(.monotonic);
        try file.getDocumentsAtCheckpointAlloc(alloc, cp, keys[0..num], values[0..num]);
        try std.testing.expect(counter.alloc_index - before <= num + 32);
        try std.testing.expect(file.test_page_reads.load(.monotonic) - reads <= 350);
        for (values[0..num]) |v| {
            try std.testing.expectEqualStrings("value", v.?);
            alloc.free(v.?);
        }
    }
    const cursor_before = counter.alloc_index;
    var cursor = DocumentIndexCursor.init(&file, cp);
    defer cursor.deinit();
    var current = try cursor.first();
    var seen: usize = 0;
    while (current) |entry| {
        var owned = entry;
        owned.deinit(alloc);
        seen += 1;
        current = try cursor.next();
    }
    try std.testing.expectEqual(count, seen);
    try std.testing.expect(counter.alloc_index - cursor_before <= seen + 32);
    const seek_before = counter.alloc_index;
    for (0..64) |i| {
        var entry = (try cursor.seekAtOrAfter(keys[i * 251], false)).?;
        defer entry.deinit(alloc);
        try std.testing.expectEqualStrings(keys[i * 251], entry.key);
    }
    try std.testing.expectEqual(@as(usize, 64), counter.alloc_index - seek_before);
}

test "lite index views resolve namespace snapshots without loading unrelated heads" {
    const a = std.testing.allocator;
    for ([_]usize{ 64, 4096, 16384 }) |count| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/snapshot.aflite", .{tmp.sub_path});
        defer a.free(path);
        var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
        var counter = std.testing.FailingAllocator.init(budget.allocator(), .{});
        const alloc = counter.allocator();
        var file = try NativeFile.createWithIo(alloc, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const batch = try arena.allocator().alloc(DocumentMutation, count);
        for (batch, 0..) |*m, i| m.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "ns-{d:0>8}\x00key", .{i}), .value = "v" };
        try file.putDocumentBatch(batch);
        const before = counter.alloc_index;
        const reads = file.test_page_reads.load(.monotonic);
        const baseline = budget.live;
        budget.peak = baseline;
        const docs = try file.snapshotDocumentsWithPrefixAlloc(alloc, batch[0].key[0..12]);
        defer NativeFile.freeSnapshotDocuments(alloc, docs);
        try std.testing.expectEqual(@as(usize, 1), docs.len);
        try std.testing.expectEqualStrings("v", docs[0].value);
        try std.testing.expect(counter.alloc_index - before <= 16);
        try std.testing.expect(file.test_page_reads.load(.monotonic) - reads <= 8);
        try std.testing.expect(budget.peak - baseline < 16 * 1024);
        const pinned = file.activeCheckpoint();
        try file.putDocument(batch[0].key, "new");
        const old = try file.snapshotDocumentsWithPrefixAtCheckpointAlloc(alloc, batch[0].key[0..12], pinned);
        defer NativeFile.freeSnapshotDocuments(alloc, old);
        try std.testing.expectEqual(@as(usize, 1), old.len);
        try std.testing.expectEqualStrings("v", old[0].value);
        const missing = try file.snapshotDocumentsWithPrefixAlloc(alloc, "missing\x00");
        defer NativeFile.freeSnapshotDocuments(alloc, missing);
        try std.testing.expectEqual(@as(usize, 0), missing.len);
    }
}

test "lite index views preserve packed and unpacked overflow reads through OOM and retry" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |packed_records| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "view-overflow-oom.aflite");
        defer a.free(path);
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.header.packed_records = packed_records;
        var header: [header_size]u8 = undefined;
        encodeHeader(&header, file.header);
        try file.file.writePositionalAll(std.testing.io, &header, 0);
        file.page_cache_enabled.store(false, .monotonic);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const count = 512;
        const batch = try arena.allocator().alloc(DocumentMutation, count);
        const keys = try arena.allocator().alloc([]const u8, count);
        const values = try arena.allocator().alloc(?[]const u8, count);
        for (batch, keys, 0..) |*m, *key, i| {
            const bytes = try arena.allocator().alloc(u8, if (i % 5 == 0) 16 else 1000);
            @memset(bytes, 'x');
            _ = try std.fmt.bufPrint(bytes[0..8], "{d:0>8}", .{i});
            key.* = bytes;
            m.* = .{ .key = bytes, .value = "original" };
        }
        try file.putDocumentBatch(batch);
        const pinned = file.activeCheckpoint();
        try file.putDocument(keys[1], "changed");
        // Dense overflow batches must not binary-search the same separators
        // afresh for every key. Results remain in caller order and pinned.
        const reads = file.test_page_reads.load(.monotonic);
        try file.getDocumentsAtCheckpointAlloc(a, pinned, keys, values);
        try std.testing.expect(file.test_page_reads.load(.monotonic) - reads < count * 3);
        for (values) |value| {
            try std.testing.expectEqualStrings("original", value.?);
            a.free(value.?);
        }
        var exhausted = false;
        for (0..256) |fail_index| {
            var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = fail_index });
            file.allocator = failing.allocator();
            defer file.allocator = a;
            var cursor = DocumentIndexCursor.init(&file, pinned);
            defer cursor.deinit();
            const Attempt = struct {
                fn run(c: *DocumentIndexCursor, query: []const u8) !void {
                    var first = (try c.first()).?;
                    defer first.deinit(c.file.allocator);
                    var last = (try c.last()).?;
                    defer last.deinit(c.file.allocator);
                    var found = (try c.seekAtOrAfter(query, false)).?;
                    defer found.deinit(c.file.allocator);
                    var previous = (try c.prev()).?;
                    defer previous.deinit(c.file.allocator);
                    var next = (try c.next()).?;
                    defer next.deinit(c.file.allocator);
                    try std.testing.expectEqualStrings(query, found.key);
                    try std.testing.expectEqualStrings(query, next.key);
                    // Earlier returned entries survive every buffer refill.
                    try std.testing.expect(std.mem.lessThan(u8, first.key, last.key));
                    const queries = [_][]const u8{ first.key, query, query, last.key, "zzzz" };
                    var outputs: [queries.len]?[]const u8 = undefined;
                    c.file.getDocumentsAtCheckpointAlloc(c.file.allocator, c.checkpoint, &queries, &outputs) catch |err| {
                        for (outputs) |value| try std.testing.expect(value == null);
                        return err;
                    };
                    defer for (outputs) |value| if (value) |bytes| c.file.allocator.free(bytes);
                    for (outputs[0..4]) |value| try std.testing.expectEqualStrings("original", value.?);
                    try std.testing.expect(outputs[4] == null);
                }
            };
            if (Attempt.run(&cursor, keys[251])) |_| {
                exhausted = !failing.has_induced_failure;
            } else |err| {
                try std.testing.expect(failing.has_induced_failure);
                try std.testing.expectEqual(error.OutOfMemory, err);
                failing.fail_index = std.math.maxInt(usize);
                failing.resize_fail_index = std.math.maxInt(usize);
                // The same cursor must recover after any failed refill.
                try Attempt.run(&cursor, keys[251]);
            }
            if (exhausted) break;
        }
        try std.testing.expect(exhausted);
        try std.testing.expect((try file.check()).valid);
    }
}

test "lite index views reject malformed slot layouts before exposing borrowed keys" {
    const a = std.testing.allocator;
    var frame = IndexReadFrame{};
    defer frame.deinit(a);
    var key_bytes = [_]u8{'a'};
    var keys = [_][]u8{&key_bytes};
    var pointers = [_]u64{1};
    const encoded = try encodeDocumentIndexNode(a, .{ .kind = .leaf, .keys = &keys, .pointers = &pointers });
    defer a.free(encoded);
    try frame.decode(a, encoded);
    try std.testing.expectEqual(@as(usize, 1), frame.offsets.items.len);
    try std.testing.expectEqual(@as(u64, 1), frame.pointer(0));
    for (0..encoded.len) |end| {
        try std.testing.expectError(error.InvalidDocumentIndex, frame.decode(a, encoded[0..end]));
        try std.testing.expectEqual(@as(usize, 0), frame.payload.len);
    }
    const bad = try a.dupe(u8, encoded);
    defer a.free(bad);
    bad[8] = 0xff;
    try std.testing.expectError(error.InvalidDocumentIndex, frame.decode(a, bad));
    @memcpy(bad, encoded);
    bad[9] = 1;
    try std.testing.expectError(error.InvalidDocumentIndex, frame.decode(a, bad));
    @memcpy(bad, encoded);
    std.mem.writeInt(u16, bad[10..12], 65535, .little);
    try std.testing.expectError(error.InvalidDocumentIndex, frame.decode(a, bad));
    try frame.decode(a, encoded);
    try std.testing.expectEqual(@as(u64, 1), frame.pointer(0));
}

test "lite scan integrity scales with packed pages for documents and namespaces" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |many_namespaces| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "scan-integrity.aflite");
        defer a.free(path);
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const batch = try arena.allocator().alloc(DocumentMutation, 16384);
        for (batch, 0..) |*m, i| m.* = .{
            .key = if (many_namespaces) try std.fmt.allocPrint(arena.allocator(), "ns-{d:0>8}\x00key", .{i}) else try std.fmt.allocPrint(arena.allocator(), "ns\x00key-{d:0>8}", .{i}),
            .value = "v",
        };
        try file.putDocumentBatch(batch);
        const before = file.test_page_reads.load(.monotonic);
        try std.testing.expect((try file.check()).valid);
        // Includes namespace validation, coverage, reachability and liveStats.
        // Per-record unpacking or index probes exceed this physical-page bound.
        try std.testing.expect(file.test_page_reads.load(.monotonic) - before < file.activeCheckpoint().page_count * 6);
    }
}

test "lite scan integrity resolves collisions and rejects stale missing and extra references" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "scan-collisions.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    const Runner = struct {
        fn verify(file_: *NativeFile, checkpoint: CheckpointSlot, kind: PageKind, root: u64, history: u64) !void {
            var coverage = HistoryIndexCoverage{ .hash_mask = 0 };
            defer coverage.deinit(file_.allocator);
            try coverage.initIndex(file_, checkpoint, root, null);
            var records = RecordPageReader{};
            defer records.deinit(file_.allocator);
            var page = history;
            while (page != 0) {
                const payload = try records.read(file_, file_.allocator, checkpoint, page, kind);
                if (kind == .catalog) {
                    const entry = try decodeCatalogEntry(payload);
                    _ = try coverage.observe(file_, checkpoint, kind, page, entry.key, entry.is_delete);
                    page = entry.previous_page;
                } else {
                    const entry = try decodeDocumentEntry(payload);
                    _ = try coverage.observe(file_, checkpoint, kind, page, entry.key, entry.is_delete);
                    page = entry.previous_page;
                }
            }
            if (coverage.unresolved != 0) return error.InvalidDocumentIndex;
        }
    };
    for ([_]bool{ false, true }) |catalog| {
        if (catalog) try file.putIndexCatalogBatch(&.{ .{ .key = "a", .value = "old" }, .{ .key = "b", .value = "old" }, .{ .key = "c", .value = "old" } }) else try file.putDocumentBatch(&.{ .{ .key = "a", .value = "old" }, .{ .key = "b", .value = "old" }, .{ .key = "c", .value = "old" } });
        const old = file.activeCheckpoint();
        const old_roots = if (catalog) try file.readCatalogRoots(old.index_catalog_root_page, old) else NativeFile.CatalogRoots{ .history = old.document_root_page, .index = old.document_index_root_page, .indexed = true };
        if (catalog) try file.putIndexCatalogBatch(&.{ .{ .key = "a", .value = "new" }, .{ .key = "b", .is_delete = true }, .{ .key = "d", .value = "added" } }) else try file.putDocumentBatch(&.{ .{ .key = "a", .value = "new" }, .{ .key = "b", .is_delete = true }, .{ .key = "d", .value = "added" } });
        const current = file.activeCheckpoint();
        const roots = if (catalog) try file.readCatalogRoots(current.index_catalog_root_page, current) else NativeFile.CatalogRoots{ .history = current.document_root_page, .index = current.document_index_root_page, .indexed = true };
        const kind: PageKind = if (catalog) .catalog else .document;
        try Runner.verify(&file, current, kind, roots.index, roots.history);
        try std.testing.expectError(error.InvalidDocumentIndex, Runner.verify(&file, current, kind, old_roots.index, roots.history));
        try std.testing.expectError(error.InvalidDocumentIndex, Runner.verify(&file, current, kind, 0, roots.history));
        try std.testing.expectError(error.InvalidDocumentIndex, Runner.verify(&file, current, kind, roots.index, old_roots.history));
        try std.testing.expect((try file.check()).valid);
    }
}

test "lite sorted editor bounds update and deletion heap across tree heights" {
    const a = std.testing.allocator;
    for ([_]usize{ 16384, 65536 }) |count| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "sorted-budget.aflite");
        defer a.free(path);
        var budget = MaintenanceTestAllocator{ .backing = a };
        var file = try NativeFile.createWithIo(budget.allocator(), std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const batch = try arena.allocator().alloc(DocumentMutation, count);
        for (batch, 0..) |*m, i| m.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "ns\x00key-{d:0>8}", .{i}), .value = "old" };
        try file.putDocumentBatch(batch);
        const pinned = file.activeCheckpoint();
        for (0..2) |pass| {
            for (batch) |*m| {
                m.value = "new";
                m.is_delete = pass == 1;
            }
            const baseline = budget.live;
            budget.peak = baseline;
            budget.limit = baseline + 1024 * 1024;
            try file.putDocumentBatch(batch);
            try std.testing.expect(budget.peak - baseline < 1024 * 1024);
            budget.limit = std.math.maxInt(usize);
            try std.testing.expect((try file.check()).valid);
            const old = (try file.getDocumentAtCheckpointAlloc(a, pinned, batch[count / 2].key)).?;
            defer a.free(old);
            try std.testing.expectEqualStrings("old", old);
        }
        try std.testing.expectEqual(@as(u64, 0), file.activeCheckpoint().document_index_root_page);
    }
}

test "lite sorted editor mixed widths duplicates sparse writes and rollback preserve roots" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |packed_records| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "sorted-mixed.aflite");
        defer a.free(path);
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.header.packed_records = packed_records;
        file.page_cache_enabled.store(false, .monotonic);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const count = 2048;
        const batch = try arena.allocator().alloc(CatalogMutation, count);
        for (batch, 0..) |*m, i| {
            const key = try arena.allocator().alloc(u8, ([_]usize{ 16, 511, 513, 1000 })[i % 4]);
            @memset(key, 'k');
            _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
            m.* = .{ .key = key, .value = "old" };
        }
        try file.putIndexCatalogBatch(batch);
        const pinned = file.activeCheckpoint();
        const before_sparse = file.test_page_writes.load(.monotonic);
        try file.putIndexCatalogBatch(&.{ .{ .key = batch[1].key, .value = "sparse" }, .{ .key = batch[count - 1].key, .value = "sparse" } });
        try std.testing.expect(file.test_page_writes.load(.monotonic) - before_sparse < 24);
        const changes = try arena.allocator().alloc(CatalogMutation, count * 2);
        for (batch, 0..) |m, i| {
            changes[i * 2] = .{ .key = m.key, .is_delete = true };
            changes[i * 2 + 1] = .{ .key = m.key, .value = "new", .is_delete = i % 3 != 0 };
        }
        const before = file.activeCheckpoint();
        try file.beginTransaction();
        file.test_page_write_fail_after = 24;
        try std.testing.expectError(error.TestPageWriteFailure, file.putIndexCatalogBatch(changes));
        file.test_page_write_fail_after = null;
        file.abortTransaction();
        try std.testing.expectEqual(before.index_catalog_root_page, file.activeCheckpoint().index_catalog_root_page);
        try std.testing.expect((try file.check()).valid);
        try file.putIndexCatalogBatch(changes);
        for (batch, 0..) |m, i| {
            const actual = try file.getIndexCatalogRecordAlloc(a, m.key);
            defer if (actual) |v| a.free(v);
            if (i % 3 == 0) try std.testing.expectEqualStrings("new", actual.?) else try std.testing.expect(actual == null);
        }
        const old = (try file.getIndexCatalogRecordAtCheckpointAlloc(a, batch[count / 2].key, pinned)).?;
        defer a.free(old);
        try std.testing.expectEqualStrings("old", old);
        try std.testing.expect((try file.check()).valid);
        for (batch) |*m| m.is_delete = true;
        try file.putIndexCatalogBatch(batch);
        try std.testing.expect((try file.check()).valid);
        const roots = try file.readCatalogRoots(file.activeCheckpoint().index_catalog_root_page, file.activeCheckpoint());
        try std.testing.expectEqual(@as(u64, 0), roots.index);
    }
}

test "lite sorted editor releases ownership on every allocation failure" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "sorted-oom.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const batch = try arena.allocator().alloc(DocumentMutation, 64);
    const references = try arena.allocator().alloc(u64, batch.len);
    for (batch, 0..) |*m, i| {
        const key = try arena.allocator().alloc(u8, if (i % 3 == 0) 1000 else 511);
        @memset(key, 'k');
        _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        m.* = .{ .key = key, .value = "old" };
    }
    try file.putDocumentBatch(batch);
    const pinned = file.activeCheckpoint();
    for (batch, references) |m, *reference| reference.* = (try file.lookupDocumentIndexPage(pinned, m.key)).?;
    const Runner = struct {
        fn run(allocator: Allocator, f: *NativeFile, mutations: []DocumentMutation, refs: []u64) !void {
            const saved = f.allocator;
            f.allocator = allocator;
            defer f.allocator = saved;
            var pages = try f.pageAllocatorFromFreeMap(f.activeCheckpoint());
            defer pages.deinit();
            var editor = IndexEditor.init(f, f.activeCheckpoint(), f.activeCheckpoint().document_index_root_page);
            defer editor.deinit();
            editor.useSortedBatch(&pages, mutations);
            // Force reclamation of a live frontier arena, including its OOM
            // handoff, without making the exhaustive sweep a huge workload.
            const root = try editor.load(&editor.root);
            _ = try editor.nodeAllocator(root).alloc(u8, 256 * 1024);
            root.compact_at = 0;
            for (mutations, refs, 0..) |m, reference, i| {
                try editor.remove(m.key);
                if (i % 3 == 0) try editor.put(m.key, reference);
            }
            const index = editor.finish(&pages) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
            try pages.flush();
            var checkpoint = f.activeCheckpoint();
            checkpoint.document_index_root_page = index;
            checkpoint.page_count = pages.next_page_id;
            var cursor = DocumentIndexCursor.init(f, checkpoint);
            defer cursor.deinit();
            var current = try cursor.first();
            var ordinal: usize = 0;
            while (current) |entry| {
                var owned = entry;
                defer owned.deinit(allocator);
                try std.testing.expectEqualStrings(mutations[ordinal].key, entry.key);
                try std.testing.expectEqual(refs[ordinal], entry.document_page_id);
                ordinal += 3;
                current = try cursor.next();
            }
            try std.testing.expectEqual((mutations.len + 2) / 3 * 3, ordinal);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Runner.run, .{ &file, batch, references });
    // The direct editor never publishes; discard its private fixture pages.
    file.discardTransactionTail();
    try std.testing.expectEqual(pinned.document_index_root_page, file.activeCheckpoint().document_index_root_page);
    try std.testing.expect((try file.check()).valid);
}

test "lite scan integrity allocation failures preserve reusable file state" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "scan-oom.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    try file.putDocumentBatch(&.{ .{ .key = "a", .value = "old" }, .{ .key = "b", .value = "old" }, .{ .key = "c", .value = "old" } });
    try file.putDocumentBatch(&.{ .{ .key = "a", .value = "new" }, .{ .key = "b", .is_delete = true } });
    const Runner = struct {
        fn run(allocator: Allocator, f: *NativeFile) !void {
            const saved = f.allocator;
            f.allocator = allocator;
            defer f.allocator = saved;
            try f.validateDocumentIndexCoverage(f.activeCheckpoint(), null);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Runner.run, .{&file});
    try std.testing.expect((try file.check()).valid);
}

test "lite single key edits retain arena allocation bounds" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "single-key-allocations.aflite");
    defer a.free(path);
    var counter = std.testing.FailingAllocator.init(a, .{});
    var file = try NativeFile.createWithIo(counter.allocator(), std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    const before = counter.alloc_index;
    for (0..200) |i| {
        var key: [32]u8 = undefined;
        try file.putDocument(try std.fmt.bufPrint(&key, "ns\x00key-{d:0>8}", .{i}), "v");
    }
    // The unsorted/single-key editor still shares an arena for flush scratch;
    // three independent scratch allocations per leaf violate this bound.
    try std.testing.expect(counter.alloc_index - before < 4200);
    try std.testing.expect((try file.check()).valid);
}

test "lite addressed writes retain late-page runs and poison partial flush failures" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "addressed-writes.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    const first = file.activeCheckpoint().page_count;
    var batch = PageWriteBatch{ .file = &file };
    const calls = file.test_page_write_calls.load(.monotonic);
    for (0..8) |i| try batch.appendPage(first + i, .data, "ready");
    for (9..18) |i| try batch.appendPage(first + i, .data, "ready");
    // The first full window drains only its ready prefix, retaining the run
    // above this missing packed page until the page becomes ready.
    try batch.appendPage(first + 8, .data, "late");
    try batch.appendPage(first + 8, .data, "latest");
    for (18..24) |i| try batch.appendPage(first + i, .data, "ready");
    try batch.flush();
    try std.testing.expectEqual(@as(u64, 2), file.test_page_write_calls.load(.monotonic) - calls);
    var raw: [default_page_size]u8 = undefined;
    for (0..24) |i| {
        try readExactAt(file.file, std.testing.io, &raw, (first + i) * default_page_size);
        try std.testing.expectEqualStrings(if (i == 8) "latest" else "ready", try decodePagePayload(&raw, .data));
    }
    var failing = PageWriteBatch{ .file = &file };
    try failing.appendPage(first + 34, .data, "second run");
    try failing.appendPage(first + 32, .data, "first run");
    file.test_page_write_fail_after = 1;
    try std.testing.expectError(error.TestPageWriteFailure, failing.flush());
    file.test_page_write_fail_after = null;
    // The first run reached disk, but none of this failed flush is admitted.
    try readExactAt(file.file, std.testing.io, &raw, (first + 32) * default_page_size);
    try std.testing.expectEqualStrings("first run", try decodePagePayload(&raw, .data));
    try std.testing.expect(!file.page_cache.pages.contains(first + 32));
    try std.testing.expect(!file.page_cache.pages.contains(first + 34));
    try std.testing.expectError(error.TestPageWriteFailure, failing.appendPage(first + 35, .data, "retry"));
    try std.testing.expectError(error.TestPageWriteFailure, failing.flush());
    file.discardTransactionTail();
    try std.testing.expect((try file.check()).valid);
}

test "lite addressed writes coalesce sorted document and catalog updates" {
    const a = std.testing.allocator;
    inline for ([_]bool{ false, true }) |catalog| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "sorted-write-calls.aflite");
        defer a.free(path);
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const batch = try arena.allocator().alloc(if (catalog) CatalogMutation else DocumentMutation, 16384);
        for (batch, 0..) |*m, i| m.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "ns\x00key-{d:0>8}", .{i}), .value = "v" };
        if (catalog) try file.putIndexCatalogBatch(batch) else try file.putDocumentBatch(batch);
        for (batch) |*m| m.value = "new";
        const pages = file.test_page_writes.load(.monotonic);
        const calls = file.test_page_write_calls.load(.monotonic);
        if (catalog) try file.putIndexCatalogBatch(batch) else try file.putDocumentBatch(batch);
        const written = file.test_page_writes.load(.monotonic) - pages;
        // Allow partial boundary runs, but retain nearly full 64 KiB writes.
        try std.testing.expect(file.test_page_write_calls.load(.monotonic) - calls <= (written + 15) / 16 + 8);
        try std.testing.expect((try file.check()).valid);
    }
}

test "lite sorted initial ingest bounds heap independently of input length" {
    const a = std.testing.allocator;
    for ([_]usize{ 16384, 131072 }) |count| {
        for ([_]bool{ false, true }) |grouped| {
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            const path = try testPath(a, tmp, "stream-initial-budget.aflite");
            defer a.free(path);
            var budget = MaintenanceTestAllocator{ .backing = a };
            var file = try NativeFile.createWithIo(budget.allocator(), std.testing.io, path, .{ .no_sync = true });
            defer file.close();
            file.page_cache_enabled.store(false, .monotonic);
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const batch = try arena.allocator().alloc(DocumentMutation, count);
            for (batch, 0..) |*m, i| m.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "ns\x00key-{d:0>8}", .{i}), .value = "v" };
            const baseline = budget.live;
            budget.peak = baseline;
            budget.limit = baseline + 512 * 1024;
            if (grouped) try file.beginTransaction();
            errdefer if (grouped) file.abortTransaction();
            try file.putDocumentBatch(batch);
            if (grouped) try file.commitTransaction();
            try std.testing.expect(budget.peak - baseline < 512 * 1024);
            budget.limit = std.math.maxInt(usize);
            const last = (try file.getDocumentAlloc(a, batch[count - 1].key)).?;
            defer a.free(last);
            try std.testing.expectEqualStrings("v", last);
            try std.testing.expect((try file.check()).valid);
        }
    }
}

test "lite sorted initial ingest preserves duplicate tombstones and unsorted fallback" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |packed_records| {
        for ([_]bool{ false, true }) |sorted| {
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            const path = try testPath(a, tmp, "stream-initial-groups.aflite");
            defer a.free(path);
            var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
            defer file.close();
            file.header.packed_records = packed_records;
            file.page_cache_enabled.store(false, .monotonic);
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const keys = try arena.allocator().alloc([]const u8, 256);
            const batch = try arena.allocator().alloc(DocumentMutation, keys.len * 3);
            for (keys, 0..) |*key, i| {
                const bytes = try arena.allocator().alloc(u8, ([_]usize{ 16, 511, 513, 1000 })[i % 4]);
                @memset(bytes, 'k');
                _ = try std.fmt.bufPrint(bytes[0..8], "{d:0>8}", .{i});
                key.* = bytes;
                const ordinal = if (sorted) i else keys.len - 1 - i;
                batch[ordinal * 3] = .{ .key = bytes, .value = "old" };
                batch[ordinal * 3 + 1] = .{ .key = bytes, .is_delete = true };
                batch[ordinal * 3 + 2] = .{ .key = bytes, .value = "latest", .is_delete = i % 3 == 0 };
            }
            try file.putDocumentBatch(batch);
            for (keys, 0..) |key, i| {
                const actual = try file.getDocumentAlloc(a, key);
                defer if (actual) |value| a.free(value);
                if (i % 3 == 0) try std.testing.expect(actual == null) else try std.testing.expectEqualStrings("latest", actual.?);
            }
            try std.testing.expect((try file.check()).valid);
            // An all-tombstone initial index is empty, and later creation must
            // take the streaming initial path again while retaining history.
            for (keys, 0..) |key, i| batch[i] = .{ .key = key, .is_delete = true };
            try file.putDocumentBatch(batch[0..keys.len]);
            try std.testing.expectEqual(@as(u64, 0), file.activeCheckpoint().document_index_root_page);
            try file.putDocumentBatch(&.{ .{ .key = keys[0], .is_delete = true }, .{ .key = keys[0], .value = "reborn" } });
            try std.testing.expect((try file.check()).valid);
        }
    }
}

test "lite sorted initial ingest allocation and partial write failures roll back" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "stream-initial-failures.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const batch = try arena.allocator().alloc(DocumentMutation, 48);
    for (batch, 0..) |*m, i| {
        const key = try arena.allocator().alloc(u8, if (i % 3 == 0) 1000 else 511);
        @memset(key, 'k');
        _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        m.* = .{ .key = key, .value = "v" };
    }
    const before = file.activeCheckpoint();
    const Runner = struct {
        fn run(allocator: Allocator, f: *NativeFile, mutations: []DocumentMutation) !void {
            const saved = f.allocator;
            f.allocator = allocator;
            defer f.allocator = saved;
            try f.beginTransaction();
            defer f.abortTransaction();
            // Exercise the supplied sorted batch directly, including its
            // duplicate/history path, inside an abortable private checkpoint.
            f.flushing_transaction = true;
            defer f.flushing_transaction = false;
            f.putDocumentBatch(mutations) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
            const snapshot = try f.snapshotDocumentsAlloc(allocator);
            defer NativeFile.freeSnapshotDocuments(allocator, snapshot);
            try std.testing.expectEqual(mutations.len, snapshot.len);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Runner.run, .{ &file, batch });
    try std.testing.expectEqual(before.document_root_page, file.activeCheckpoint().document_root_page);
    const large = try arena.allocator().alloc(DocumentMutation, 2048);
    for (large, 0..) |*m, i| m.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "ns\x00key-{d:0>8}", .{i}), .value = &(@as([100]u8, @splat('v'))) };
    for ([_]usize{ 0, 1, 3 }) |after| {
        try file.beginTransaction();
        file.test_page_write_fail_after = after;
        try std.testing.expectError(error.TestPageWriteFailure, file.putDocumentBatch(large));
        file.test_page_write_fail_after = null;
        file.abortTransaction();
        try std.testing.expectEqual(before.document_root_page, file.activeCheckpoint().document_root_page);
        try std.testing.expect((try file.check()).valid);
    }
    try file.putDocumentBatch(large);
    try std.testing.expect((try file.check()).valid);
}

test "lite index snapshots ignore overwritten history with bounded reads and allocations" {
    const a = std.testing.allocator;
    for ([_]usize{ 1, 4096, 16384 }) |count| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "live-snapshot-bound.aflite");
        defer a.free(path);
        var counter = std.testing.FailingAllocator.init(a, .{});
        var file = try NativeFile.createWithIo(counter.allocator(), std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        const batch = try a.alloc(DocumentMutation, count);
        defer a.free(batch);
        @memset(batch, .{ .key = "ns\x00key", .value = "v" });
        try file.putDocumentBatch(batch);
        for ([_][]const u8{ "", "ns\x00", "ns\x00k" }) |prefix| {
            const reads = file.test_page_reads.load(.monotonic);
            const allocations = counter.alloc_index;
            const docs = try file.snapshotDocumentsWithPrefixAlloc(counter.allocator(), prefix);
            defer NativeFile.freeSnapshotDocuments(counter.allocator(), docs);
            try std.testing.expectEqual(@as(usize, 1), docs.len);
            try std.testing.expectEqualStrings("v", docs[0].value);
            try std.testing.expect(file.test_page_reads.load(.monotonic) - reads <= 2);
            try std.testing.expect(counter.alloc_index - allocations <= 12);
        }
    }
}

test "lite index snapshots preserve pinned external values through allocation failures" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "live-snapshot-ownership.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    file.page_cache_enabled.store(false, .monotonic);
    const large = @as([16384]u8, @splat('v'));
    try file.putDocumentBatch(&.{ .{ .key = "ns\x00a", .value = &large }, .{ .key = "ns\x00b", .value = "old" }, .{ .key = "other\x00x", .value = "outside" } });
    const pinned = file.activeCheckpoint();
    try file.putDocumentBatch(&.{ .{ .key = "ns\x00a", .value = "new" }, .{ .key = "ns\x00b", .is_delete = true } });
    const Runner = struct {
        fn run(allocator: Allocator, f: *NativeFile, checkpoint: CheckpointSlot) !void {
            const saved = f.allocator;
            f.allocator = allocator;
            defer f.allocator = saved;
            const docs = try f.snapshotDocumentsWithPrefixAtCheckpointAlloc(allocator, "ns\x00", checkpoint);
            defer NativeFile.freeSnapshotDocuments(allocator, docs);
            try std.testing.expectEqual(@as(usize, 2), docs.len);
            try std.testing.expectEqualStrings("ns\x00a", docs[0].key);
            try std.testing.expectEqual(@as(usize, 16384), docs[0].value.len);
            try std.testing.expectEqualStrings("old", docs[1].value);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Runner.run, .{ &file, pinned });
    const current = try file.snapshotDocumentsWithPrefixAlloc(a, "ns\x00");
    defer NativeFile.freeSnapshotDocuments(a, current);
    try std.testing.expectEqual(@as(usize, 1), current.len);
    try std.testing.expectEqualStrings("new", current[0].value);
    const missing = try file.snapshotDocumentsWithPrefixAlloc(a, "missing");
    defer NativeFile.freeSnapshotDocuments(a, missing);
    try std.testing.expectEqual(@as(usize, 0), missing.len);
    // A checksum-valid index pointing at another key must still be rejected.
    const checkpoint = file.activeCheckpoint();
    var node = try file.readDocumentIndexNode(checkpoint.document_index_root_page, checkpoint);
    defer node.deinit(a);
    try std.testing.expectEqual(DocumentIndexNodeKind.leaf, node.kind);
    node.pointers[0] = node.pointers[1];
    const encoded = try encodeDocumentIndexNode(a, node);
    defer a.free(encoded);
    try file.writePage(checkpoint.document_index_root_page, .document_index, encoded);
    try std.testing.expectError(error.InvalidDocumentIndex, file.snapshotDocumentsWithPrefixAlloc(a, "ns\x00"));
}

test "lite ingest scratch and bulk key storage bound allocation churn" {
    const a = std.testing.allocator;
    for ([_]usize{ 16384, 65536 }) |count| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "ingest-allocation-churn.aflite");
        defer a.free(path);
        var counter = std.testing.FailingAllocator.init(a, .{});
        var file = try NativeFile.createWithIo(counter.allocator(), std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const batch = try arena.allocator().alloc(DocumentMutation, count);
        for (batch, 0..) |*m, i| m.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "ns\x00key-{d:0>8}", .{i}), .value = "v" };
        const before = counter.alloc_index;
        try file.putDocumentBatch(batch);
        // Scratch grows by page/tree frontier, not once or twice per key.
        try std.testing.expect(counter.alloc_index - before < count / 32);
        try std.testing.expect((try file.check()).valid);
    }
}

test "lite record encoding scratch is shared across catalog and document writes" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |packed_records| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "record-encoding-scratch.aflite");
        defer a.free(path);
        var counter = std.testing.FailingAllocator.init(a, .{});
        var file = try NativeFile.createWithIo(counter.allocator(), std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        var pages = try file.pageAllocatorFromFreeMap(file.activeCheckpoint());
        defer pages.deinit();
        pages.pack_records = packed_records;
        var references: [2048]u64 = undefined;
        var key: [1500]u8 = undefined;
        const before = counter.alloc_index;
        for (&references, 0..) |*ref, i| {
            const len = ([_]usize{ 16, 511, 513, 1500 })[i % 4];
            @memset(&key, 'k');
            _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
            ref.* = if (i % 2 == 0)
                try pages.writeCatalog(.{ .previous_page = 0, .key = key[0..len], .value = "catalog" })
            else
                try pages.writeDocument(.{ .previous_page = 0, .key = key[0..len], .value = "document" });
        }
        try pages.flush();
        try std.testing.expect(counter.alloc_index - before < 16);
        var checkpoint = file.activeCheckpoint();
        checkpoint.page_count = pages.next_page_id;
        var reader = RecordPageReader{};
        defer reader.deinit(a);
        for (references, 0..) |ref, i| {
            const payload = try reader.read(&file, a, checkpoint, ref, if (i % 2 == 0) .catalog else .document);
            const entry: CatalogEntry = if (i % 2 == 0) try decodeCatalogEntry(payload) else blk: {
                const doc = try decodeDocumentEntry(payload);
                break :blk .{ .previous_page = doc.previous_page, .key = doc.key, .value = doc.value };
            };
            try std.testing.expectEqual(i, try std.fmt.parseInt(usize, entry.key[0..8], 10));
            try std.testing.expectEqual(([_]usize{ 16, 511, 513, 1500 })[i % 4], entry.key.len);
            try std.testing.expectEqualStrings(if (i % 2 == 0) "catalog" else "document", entry.value);
        }
        file.discardTransactionTail();
    }
}

test "lite materialized snapshots group scattered pages and own each key once" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |scattered| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "snapshot-locality.aflite");
        defer a.free(path);
        var counter = std.testing.FailingAllocator.init(a, .{});
        var file = try NativeFile.createWithIo(counter.allocator(), std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.page_cache_enabled.store(false, .monotonic);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const count = 16384;
        const keys = try arena.allocator().alloc([]const u8, count);
        const batch = try arena.allocator().alloc(DocumentMutation, count);
        for (keys, 0..) |*key, i| key.* = try std.fmt.allocPrint(arena.allocator(), "ns\x00key-{d:0>8}", .{i});
        for (batch, 0..) |*m, i| {
            const key = keys[if (scattered) (i * 4051) % count else i];
            m.* = .{ .key = key, .value = key };
        }
        try file.putDocumentBatch(batch);
        const reads = file.test_page_reads.load(.monotonic);
        const allocations = counter.alloc_index;
        const docs = try file.snapshotDocumentsWithPrefixAlloc(counter.allocator(), "ns\x00");
        defer NativeFile.freeSnapshotDocuments(counter.allocator(), docs);
        try std.testing.expect(file.test_page_reads.load(.monotonic) - reads < count / 16);
        try std.testing.expect(counter.alloc_index - allocations < count * 2 + 100);
        try std.testing.expectEqual(count, docs.len);
        for (docs, keys) |doc, key| {
            try std.testing.expectEqualStrings(key, doc.key);
            try std.testing.expectEqualStrings(key, doc.value);
        }
    }
}

test "lite grouped snapshots preserve allocator ownership pinned values and mixed legacy tombstones" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |packed_records| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "snapshot-mixed-ownership.aflite");
        defer a.free(path);
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.header.packed_records = packed_records;
        file.page_cache_enabled.store(false, .monotonic);
        const external = @as([16384]u8, @splat('v'));
        try file.putDocumentBatch(&.{ .{ .key = "", .value = "delete" }, .{ .key = "a", .value = "old" }, .{ .key = "b", .value = &external }, .{ .key = "c", .value = "delete" }, .{ .key = "d", .value = "stay" }, .{ .key = "e", .value = "delete" } });
        const original = file.activeCheckpoint();
        var leaf = try file.readDocumentIndexNode(original.document_index_root_page, original);
        defer leaf.deinit(a);
        for ([_]usize{ 0, 3, 5 }) |i| {
            try file.deleteDocument(leaf.keys[i]);
            leaf.pointers[i] = file.activeCheckpoint().document_root_page;
        }
        // Construct a legacy index with tombstones interspersed with live keys.
        const encoded = try encodeDocumentIndexNode(a, leaf);
        defer a.free(encoded);
        try file.writePage(original.document_index_root_page, .document_index, encoded);
        var pinned = file.activeCheckpoint();
        pinned.document_index_root_page = original.document_index_root_page;
        pinned.commit_sequence += 1;
        try file.publishCheckpoint(pinned);
        try std.testing.expect((try file.check()).valid);
        try file.putDocument("a", "new");
        try file.deleteDocument("b");
        const Runner = struct {
            fn run(output_allocator: Allocator, f: *NativeFile, checkpoint: CheckpointSlot) !void {
                // File/cursor scratch uses std.testing.allocator independently
                // of the failing output allocator. Ownership must never cross.
                const docs = try f.snapshotDocumentsWithPrefixAtCheckpointAlloc(output_allocator, "", checkpoint);
                defer NativeFile.freeSnapshotDocuments(output_allocator, docs);
                try std.testing.expectEqual(@as(usize, 3), docs.len);
                try std.testing.expectEqualStrings("a", docs[0].key);
                try std.testing.expectEqualStrings("old", docs[0].value);
                try std.testing.expectEqualStrings("b", docs[1].key);
                try std.testing.expectEqual(@as(usize, 16384), docs[1].value.len);
                try std.testing.expectEqualStrings("d", docs[2].key);
            }
        };
        try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Runner.run, .{ &file, pinned });
        var output_budget = MaintenanceTestAllocator{ .backing = a };
        const docs = try file.snapshotDocumentsWithPrefixAtCheckpointAlloc(output_budget.allocator(), "", pinned);
        NativeFile.freeSnapshotDocuments(output_budget.allocator(), docs);
        try std.testing.expectEqual(@as(usize, 0), output_budget.live);
        const current = try file.snapshotDocumentsWithPrefixAlloc(a, "a");
        defer NativeFile.freeSnapshotDocuments(a, current);
        try std.testing.expectEqual(@as(usize, 1), current.len);
        try std.testing.expectEqualStrings("new", current[0].value);
    }
}

test "lite validated index views bound warm point work and preserve checkpoint results" {
    const a = std.testing.allocator;
    for ([_]bool{ true, false }) |packed_records| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "validated-views.aflite");
        defer a.free(path);
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        file.header.packed_records = packed_records;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const count = 4096;
        const batch = try arena.allocator().alloc(DocumentMutation, count);
        for (batch, 0..) |*m, i| m.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "key-{d:0>8}", .{i}), .value = "old" };
        try file.putDocumentBatch(batch);
        const checkpoint = file.activeCheckpoint();
        const references = try a.alloc(?u64, count);
        defer a.free(references);
        file.page_cache_enabled.store(false, .monotonic);
        for (batch, references) |m, *reference| reference.* = try file.lookupDocumentIndexPage(checkpoint, m.key);
        file.page_cache_enabled.store(true, .monotonic);
        for (batch, references) |m, reference| try std.testing.expectEqual(reference, try file.lookupDocumentIndexPage(checkpoint, m.key));
        const hits = file.test_index_view_hits.load(.monotonic);
        const comparisons = file.test_index_comparisons.load(.monotonic);
        for (0..count) |i| {
            const index = (i * 4051) % count;
            try std.testing.expectEqual(references[index], try file.lookupDocumentIndexPage(checkpoint, batch[index].key));
        }
        try std.testing.expect(file.test_index_view_hits.load(.monotonic) - hits >= count * 2);
        try std.testing.expect(file.test_index_comparisons.load(.monotonic) - comparisons < count * 24);
        try std.testing.expectEqual(@as(?u64, null), try file.lookupDocumentIndexPage(checkpoint, "absent"));
        var invalid_checkpoint = checkpoint;
        invalid_checkpoint.page_count = checkpoint.document_index_root_page;
        try std.testing.expectError(error.InvalidPageId, file.lookupDocumentIndexPage(invalid_checkpoint, batch[0].key));
        try file.putDocument(batch[0].key, "new");
        const old = (try file.getDocumentAtCheckpointAlloc(a, checkpoint, batch[0].key)).?;
        defer a.free(old);
        const current = (try file.getDocumentAlloc(a, batch[0].key)).?;
        defer a.free(current);
        try std.testing.expectEqualStrings("old", old);
        try std.testing.expectEqualStrings("new", current);
        try std.testing.expect((try file.check()).valid);
    }
}

test "lite validated index views retain evicted pins and account replacement storage" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "pinned-view.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    try file.putDocument("key", "value");
    const checkpoint = file.activeCheckpoint();
    _ = try file.lookupDocumentIndexPage(checkpoint, "key");
    const page = checkpoint.document_index_root_page;
    const view = (try file.page_cache.acquireIndex(a, page)).?;
    const expected_pointer = view.frame.pointer(0);
    var manager = resource_manager_mod.ResourceManager.init(.{});
    file.page_cache.attachResourceManager(&manager);
    // Replacement can happen while an old reader is comparing an overflow
    // key. The old bytes stay alive and charged after their cache entry leaves.
    file.page_cache.clear(a);
    try std.testing.expectEqual(view.size(), file.page_cache.total_bytes);
    try std.testing.expectEqual(view.size(), manager.sliceStats(.lite_native_page_cache).used_bytes);
    file.page_cache.put(a, page, view.frame.raw.?);
    const newer = (try file.page_cache.acquireIndex(a, page)).?;
    try std.testing.expect(newer != view);
    try std.testing.expectEqual(expected_pointer, newer.frame.pointer(0));
    file.page_cache.discardFrom(a, page);
    try std.testing.expectEqual(view.size() + newer.size(), file.page_cache.total_bytes);
    file.page_cache.releaseIndex(a, newer);
    try std.testing.expectEqual(expected_pointer, view.frame.pointer(0));
    file.page_cache.limit_bytes = view.size();
    file.page_cache.put(a, page, view.frame.raw.?);
    try std.testing.expect(!file.page_cache.pages.contains(page));
    file.page_cache.releaseIndex(a, view);
    try std.testing.expectEqual(@as(usize, 0), file.page_cache.total_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lite_native_page_cache).used_bytes);
}

test "lite validated index views bypass integrity checks and support overflow keys" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "overflow-view.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    var key: [600]u8 = @splat('x');
    var other: [600]u8 = @splat('y');
    try file.putDocumentBatch(&.{ .{ .key = &key, .value = "one" }, .{ .key = &other, .value = "two" } });
    for (0..3) |_| {
        const value = (try file.getDocumentAlloc(a, &other)).?;
        defer a.free(value);
        try std.testing.expectEqualStrings("two", value);
    }
    const before = file.test_index_view_hits.load(.monotonic);
    try std.testing.expect(before > 0);
    const page = file.activeCheckpoint().document_index_root_page;
    try file.file.writePositionalAll(file.runtimeIo(), "X", page * default_page_size + page_header_size);
    const report = try file.check();
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("page_checksum_mismatch", report.issue.?);
    try std.testing.expectEqual(before, file.test_index_view_hits.load(.monotonic));
    _ = file.page_cache_bypass.fetchAdd(1, .monotonic);
    defer _ = file.page_cache_bypass.fetchSub(1, .monotonic);
    try std.testing.expectError(error.NativePageChecksumMismatch, file.lookupDocumentIndexPage(file.activeCheckpoint(), &other));
}

test "lite validated index views survive concurrent overflow reads and eviction" {
    if (builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "concurrent-views.aflite");
    defer a.free(path);
    var file = try NativeFile.create(a, path);
    defer file.close();
    var key: [600]u8 = @splat('x');
    try file.putDocument(&key, "value");
    const Runner = struct {
        fn run(f: *NativeFile, k: []const u8, failed: *std.atomic.Value(bool)) void {
            for (0..200) |_| {
                const value = f.getDocumentAlloc(f.allocator, k) catch {
                    failed.store(true, .monotonic);
                    return;
                };
                if (value) |bytes| {
                    defer f.allocator.free(bytes);
                    if (!std.mem.eql(u8, bytes, "value")) failed.store(true, .monotonic);
                } else failed.store(true, .monotonic);
            }
        }
    };
    var failed = std.atomic.Value(bool).init(false);
    var threads: [4]std.Thread = undefined;
    var started: usize = 0;
    defer for (threads[0..started]) |thread| thread.join();
    for (&threads) |*thread| {
        thread.* = try std.Thread.spawn(.{}, Runner.run, .{ &file, &key, &failed });
        started += 1;
    }
    for (0..200) |_| file.page_cache.clear(a);
    for (threads[0..started]) |thread| thread.join();
    started = 0;
    try std.testing.expect(!failed.load(.monotonic));
    file.page_cache.clear(a);
    try std.testing.expectEqual(@as(usize, 0), file.page_cache.total_bytes);
}

test "lite validated index view admission handles every allocation failure" {
    const a = std.testing.allocator;
    var keys = [_][]u8{@constCast("key")};
    var pointers = [_]u64{7};
    const payload = try encodeDocumentIndexNode(a, .{ .kind = .leaf, .keys = &keys, .pointers = &pointers });
    defer a.free(payload);
    var raw: [4096]u8 = undefined;
    encodePage(&raw, .document_index, payload);
    const Runner = struct {
        fn run(allocator: Allocator, page: []const u8) !void {
            var cache = PageCache{};
            defer cache.deinit(allocator);
            cache.put(allocator, 1, page);
            // Admission is best effort in production. Report a skipped
            // admission to the failure-sweep harness after checking cleanup.
            const view = (try cache.acquireIndex(allocator, 1)) orelse return error.OutOfMemory;
            defer cache.releaseIndex(allocator, view);
            cache.remove(allocator, 1);
            try std.testing.expectEqual(@as(u64, 7), view.frame.pointer(0));
        }
    };
    try std.testing.checkAllAllocationFailures(a, Runner.run, .{&raw});
}

test "lite validated index views release residency under shared hard pressure" {
    const a = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.lite_native_page_cache)] = .{ .soft_limit_bytes = 8192, .hard_limit_bytes = 16384 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "view-pressure.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true, .resource_manager = &manager });
    defer file.close();
    try file.putDocument("key", "value");
    const checkpoint = file.activeCheckpoint();
    const expected = try file.lookupDocumentIndexPage(checkpoint, "key");
    const view = (try file.page_cache.acquireIndex(a, checkpoint.document_index_root_page)).?;
    var external: u64 = 0;
    manager.observeUsage(.lite_native_page_cache, &external, 32768);
    defer manager.observeUsage(.lite_native_page_cache, &external, 0);
    try std.testing.expectEqual(@as(?*PageCache.IndexView, null), try file.page_cache.acquireIndex(a, checkpoint.document_index_root_page));
    try std.testing.expectEqual(view.size(), file.page_cache.total_bytes);
    try std.testing.expectEqual(expected.?, view.frame.pointer(0));
    try std.testing.expectEqual(expected, try file.lookupDocumentIndexPage(checkpoint, "key"));
    try std.testing.expectEqual(view.size() + external, manager.sliceStats(.lite_native_page_cache).used_bytes);
    file.page_cache.releaseIndex(a, view);
    try std.testing.expectEqual(external, manager.sliceStats(.lite_native_page_cache).used_bytes);
}

test "lite allocator v4 compact estimates match long keys values and migration" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "long-key-estimate.aflite");
    defer a.free(path);
    for ([_]bool{ false, true }) |source_indexed| {
        var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .indexed_reclamation = source_indexed, .no_sync = true });
        defer file.close();
        file.vacuum_target_indexed = true;
        var key: [1024]u8 = @splat('k');
        for (0..16) |i| {
            std.mem.writeInt(u64, key[0..8], i, .big);
            try file.putDocument(&key, "value");
            try file.putCatalogRecord(&key, "catalog value");
        }
        const value: [32768]u8 = @splat('v');
        try file.putDocument("chain", &value);
        try file.putCatalogRecord("extent", &value);
        const stats = try file.liveStats(null);
        var image = try file.prepareVacuum(null);
        defer image.deinit();
        try std.testing.expectEqual(image.report.after_size, stats.compact_size);
    }
}

test "lite allocator v4 failed replacement create preserves original" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "review-create-replacement.aflite");
    defer a.free(path);
    for (0..96) |fail_index| {
        {
            var original = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
            defer original.close();
            try original.putDocument("original", "must survive failed create");
        }
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = fail_index });
        if (NativeFile.createWithIo(failing.allocator(), std.testing.io, path, .{ .indexed_reclamation = true, .no_sync = true })) |result| {
            var created = result;
            created.close();
        } else |err| {
            if (err != error.OutOfMemory) return err;
            var reopened = try NativeFile.openWithIo(a, std.testing.io, path, .{ .read_only = true });
            defer reopened.close();
            const value = try reopened.getDocumentAlloc(a, "original");
            defer if (value) |bytes| a.free(bytes);
            try std.testing.expect(value != null);
        }
    }
}

test "lite allocator v4 ownership graph cancels within an external value" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "cancel-value-graph.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .indexed_reclamation = true, .no_sync = true });
    defer file.close();
    const payload = try a.alloc(u8, 256 * 1024);
    defer a.free(payload);
    @memset(payload, 'v');
    try file.putDocument("key", payload);
    const checkpoint = file.activeCheckpoint();
    var node = try file.readDocumentIndexNode(checkpoint.document_index_root_page, checkpoint);
    defer node.deinit(a);
    const record_payload = try file.readPagePayloadByKindAllocForCheckpoint(a, node.pointers[0], .document, checkpoint);
    defer a.free(record_payload);
    const record = try decodeDocumentEntry(record_payload);
    var cancel = maintenance.CancelToken{};
    var graph: NativeFile.IndexedGraph = .{};
    defer graph.deinit(a);
    file.test_page_reads.store(0, .monotonic);
    file.test_cancel_on_read = &cancel;
    defer file.test_cancel_on_read = null;
    try std.testing.expectError(error.MaintenanceCanceled, file.indexedValue(&graph, checkpoint, record.external_value_root_page, record.external_value_len, &cancel));
    try std.testing.expectEqual(@as(u64, 1), file.test_page_reads.load(.monotonic));
}

test "lite allocator v4 metadata chain retirement progresses behind data reader" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "metadata-chain-reader.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .indexed_reclamation = true, .no_sync = true });
    defer file.close();
    file.retirement_work_pages = 0;
    try file.putDocument("key", "pinned");
    const pinned = file.activeCheckpoint();
    file.minimum_reader_sequence = pinned.commit_sequence;
    for (0..24) |_| try file.putDocument("key", "replacement");
    (try file.loadLedger(file.activeCheckpoint())).force_checkpoint = true;
    try file.putDocument("key", "checkpoint");
    try file.putDocument("advance", "frontier");
    const before = (try file.allocatorStats()).?;
    try std.testing.expect(try file.retirementNeedsService());
    _ = try file.reclaimPages(1024);
    const after = (try file.allocatorStats()).?;
    try std.testing.expect(after.reusable_pages > before.reusable_pages);
    try std.testing.expectEqual(before.pending_data_objects, after.pending_data_objects);
    const value = (try file.getDocumentAtCheckpointAlloc(a, pinned, "key")).?;
    defer a.free(value);
    try std.testing.expectEqualStrings("pinned", value);
    try std.testing.expect((try file.check()).valid);
    file.minimum_reader_sequence = null;
    for (0..8) |_| _ = try file.reclaimPages(1024);
    try std.testing.expectEqual(@as(u64, 0), (try file.allocatorStats()).?.pending_data_objects);
    try std.testing.expect((try file.check()).valid);
    // Force a replay from disk, rather than only validating cached state.
    file.invalidateLedger();
    try std.testing.expect((try file.check()).valid);
}

test "lite allocator v4 saturated queue drains with a single object budget" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "saturated-single-budget.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .indexed_reclamation = true, .no_sync = true });
    defer file.close();
    file.retirement_work_pages = 0;
    const value = try a.alloc(u8, 512 * 1024);
    defer a.free(value);
    @memset(value, 'v');
    try file.putCatalogRecord("key", value);
    try file.putCatalogRecord("key", "replacement");
    try file.putDocument("advance", "frontier");
    const state = try file.loadLedger(file.activeCheckpoint());
    state.pending_limit = state.pending.count();
    try std.testing.expectError(error.LiteRetirementBacklogExceeded, state.retire(.{ .epoch = file.activeCheckpoint().commit_sequence, .page = 1, .kind = .page }));
    for (0..512) |_| {
        if (!try file.retirementNeedsService()) break;
        try std.testing.expect(try file.reclaimPages(1) <= 1);
    }
    try std.testing.expectEqual(@as(u64, 0), (try file.allocatorStats()).?.pending_data_objects);
    try std.testing.expect(!try file.retirementNeedsService());
    file.invalidateLedger();
    try std.testing.expect((try file.check()).valid);
    const surviving = (try file.getCatalogRecordAlloc(a, "key")).?;
    defer a.free(surviving);
    try std.testing.expectEqualStrings("replacement", surviving);
}

test "lite allocator v4 idle metadata advances recovery behind a pinned reader" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "idle-metadata-reader.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .indexed_reclamation = true, .no_sync = true });
    defer file.close();
    file.retirement_work_pages = 0;
    const pinned = file.activeCheckpoint();
    file.minimum_reader_sequence = pinned.commit_sequence;
    (try file.loadLedger(pinned)).force_checkpoint = true;
    try file.putDocument("live", "value");
    try std.testing.expectEqual(@as(u64, 0), (try file.allocatorStats()).?.pending_data_objects);
    try std.testing.expect(try file.retirementNeedsService());
    _ = try file.reclaimPages(1);
    _ = try file.reclaimPages(1);
    // Chain completion itself must cross the allocator recovery frontier
    // before its physical pages can enter the free bitmap.
    var rounds: usize = 0;
    while (try file.retirementNeedsService()) : (rounds += 1) {
        try std.testing.expect(rounds < 16);
        _ = try file.reclaimPages(1);
    }
    try std.testing.expect(!try file.retirementNeedsService());
    try std.testing.expectEqual(@as(?[]u8, null), try file.getDocumentAtCheckpointAlloc(a, pinned, "live"));
    try std.testing.expect((try file.check()).valid);
}

test "lite allocator v4 completed retirement preserves rollback and fallback bytes" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "retirement-completion-recovery.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .indexed_reclamation = true, .no_sync = true });
    defer file.close();
    file.retirement_work_pages = 0;
    const value: [8192]u8 = @splat('v');
    try file.putCatalogRecord("key", &value);
    const checkpoint = file.activeCheckpoint();
    const reference = (try file.lookupCatalogPage(checkpoint, .metadata, "key")).?;
    const payload = try file.readPagePayloadByKindAllocForCheckpoint(a, reference, .catalog, checkpoint);
    defer a.free(payload);
    const root = (try decodeCatalogEntry(payload)).external_value_root_page;
    try file.putCatalogRecord("key", "replacement");
    try file.putDocument("advance", "frontier");
    var rounds: usize = 0;
    while ((try file.loadLedger(file.activeCheckpoint())).count(root) != 0) : (rounds += 1) {
        try std.testing.expect(rounds < 16);
        _ = try file.reclaimPages(1);
    }
    // The completion is committed, but the previous allocator still owns a
    // queue entry that must read this value node. It cannot be reused yet.
    const state = try file.loadLedger(file.activeCheckpoint());
    var found = false;
    var pending = state.pending.valueIterator();
    while (pending.next()) |item| if (item.kind == .metadata and item.page == root) {
        found = true;
    };
    try std.testing.expect(found);
    try file.beginTransaction();
    try file.putDocument("private", &value);
    _ = try file.materializeTransactionCheckpoint();
    file.abortTransaction();
    try std.testing.expect((try file.check()).valid);
    var fallback = try NativeFile.openWithIo(a, std.testing.io, path, .{ .read_only = true });
    defer fallback.close();
    fallback.header.active_checkpoint = 1 - fallback.header.active_checkpoint;
    try std.testing.expect((try fallback.check()).valid);
}

test "lite allocator v4 external reader waits for in place reclamation lock" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "reader-reclamation-lock.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, io, path, .{ .indexed_reclamation = true, .no_sync = true });
    defer file.close();
    try file.putDocument("key", "value");
    const held = try acquireDataRewriteLock(io, path);
    var lock_open = true;
    defer if (lock_open) held.file.close(io);
    const Reader = struct {
        started: std.atomic.Value(bool) = .init(false),
        opened: std.atomic.Value(bool) = .init(false),
        result: anyerror!void = {},
        fn run(self: *@This(), allocator: Allocator, runtime_io: std.Io, source: []const u8) void {
            self.started.store(true, .release);
            self.read(allocator, runtime_io, source) catch |err| {
                self.result = err;
            };
        }
        pub fn read(self: *@This(), allocator: Allocator, runtime_io: std.Io, source: []const u8) !void {
            var reader = try NativeFile.openWithIo(allocator, runtime_io, source, .{ .read_only = true, .wait_for_reader_lock = true });
            defer reader.close();
            const value = (try reader.getDocumentAlloc(allocator, "key")).?;
            defer allocator.free(value);
            try std.testing.expectEqualStrings("value", value);
            self.opened.store(true, .release);
        }
    };
    var context = Reader{};
    var future = try io.concurrent(Reader.run, .{ &context, a, io, path });
    var joined = false;
    defer if (!joined) {
        if (lock_open) {
            held.file.close(io);
            lock_open = false;
        }
        future.await(io);
    };
    const deadline = std.Io.Clock.awake.now(io).addDuration(std.Io.Duration.fromSeconds(3));
    while (!context.started.load(.acquire)) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= deadline.nanoseconds) return error.TestUnexpectedResult;
        try std.Io.sleep(io, .fromMilliseconds(1), .awake);
    }
    try std.Io.sleep(io, .fromMilliseconds(10), .awake);
    try std.testing.expect(!context.opened.load(.acquire));
    held.file.close(io);
    lock_open = false;
    future.await(io);
    joined = true;
    try context.result;
    try std.testing.expect(context.opened.load(.acquire));
}

test "lite immutable checksums traverse linked values once and seek extent ranges" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "stream-checksum.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    const bytes = try a.alloc(u8, 2 * 1024 * 1024);
    defer a.free(bytes);
    for (bytes, 0..) |*byte, i| byte.* = @truncate(i *% 17);
    try file.putDocument("chain", bytes);
    const checkpoint = file.activeCheckpoint();
    const reference = (try file.lookupDocumentIndexPage(checkpoint, "chain")).?;
    var page: [65536]u8 = undefined;
    const entry = try decodeDocumentEntry(try decodePagePayload(try file.readPageInto(reference, checkpoint, &page), .document));
    const chain = NativeFile.IndexValue{ .root = entry.external_value_root_page, .length = entry.external_value_len };
    const start = bytes.len - 128 * 1024;
    file.test_page_reads.store(0, .monotonic);
    var crc = Crc32.init();
    var scratch: [8192]u8 = undefined;
    var offset: usize = start;
    while (offset < bytes.len) : (offset += scratch.len) {
        try file.readIndexValueInto(chain, offset, &scratch, checkpoint);
        crc.update(&scratch);
    }
    const baseline_reads = file.test_page_reads.load(.monotonic);
    file.test_page_reads.store(0, .monotonic);
    try std.testing.expectEqual(crc.final(), try file.checksumIndexValue(chain, start, bytes.len - start, checkpoint));
    const sequential_reads = file.test_page_reads.load(.monotonic);
    try std.testing.expect(sequential_reads < baseline_reads / 8);
    for ([_]usize{ 0, 1, 8191, 8192, start, bytes.len }) |position| {
        const length = @min(@as(usize, 10001), bytes.len - position);
        try std.testing.expectEqual(Crc32.hash(bytes[position..][0..length]), try file.checksumIndexValue(chain, position, length, checkpoint));
    }
    try file.putIndexCatalogRecord("extent", bytes);
    const extent_checkpoint = file.activeCheckpoint();
    var extent = try file.openIndexValue(a, "extent", extent_checkpoint);
    defer extent.deinit(a);
    file.test_page_reads.store(0, .monotonic);
    try std.testing.expectEqual(Crc32.hash(bytes[start..]), try file.checksumIndexValue(extent, start, bytes.len - start, extent_checkpoint));
    const extent_reads = file.test_page_reads.load(.monotonic);
    try std.testing.expect(extent_reads < sequential_reads / 2);
    try std.testing.expectError(error.EndOfStream, file.checksumIndexValue(extent, bytes.len, 1, extent_checkpoint));
    std.debug.print("LITE_CHECKSUM chain_bytes={d} range_bytes={d} repeated_range_page_reads={d} sequential_page_reads={d} extent_range_page_reads={d}\n", .{ bytes.len, bytes.len - start, baseline_reads, sequential_reads, extent_reads });
}

test "lite cold artifact export keeps payload pages out of native cache" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "cold-export.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    const bytes = try a.alloc(u8, 4 * 1024 * 1024);
    defer a.free(bytes);
    for (bytes, 0..) |*byte, i| byte.* = @truncate(i *% 17);
    try file.putIndexCatalogRecord("artifact", bytes);
    const checkpoint = file.activeCheckpoint();
    var value = try file.openIndexValue(a, "artifact", checkpoint);
    defer value.deinit(a);
    file.page_cache.clear(a);
    const destination = try tmp.dir.createFile(std.testing.io, "export", .{ .read = true });
    defer destination.close(std.testing.io);
    try file.copyIndexValueTo(value, destination, checkpoint);
    var payload_count: usize = 0;
    for (file.page_cache.pages.values()) |cached| {
        if (cached.bytes[4] == @backingInt(PageKind.value)) payload_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), payload_count);
    var sample: [8192]u8 = undefined;
    try std.testing.expectEqual(sample.len, try destination.readPositionalAll(std.testing.io, &sample, bytes.len - sample.len));
    try std.testing.expectEqualSlices(u8, bytes[bytes.len - sample.len ..], &sample);
    // The scoped policy must not disable normal foreground payload caching.
    try file.readIndexValueInto(value, 0, &sample, checkpoint);
    for (file.page_cache.pages.values()) |cached| {
        if (cached.bytes[4] == @backingInt(PageKind.value)) payload_count += 1;
    }
    try std.testing.expect(payload_count > 0);
    std.debug.print("LITE_COLD_EXPORT bytes={d} exported_payload_cache_pages=0\n", .{bytes.len});
}

test "lite native value cursor uses inline frames and bounded deep overflow" {
    const a = std.testing.allocator;
    var file: NativeFile = undefined;
    var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 0 };
    file.allocator = budget.allocator();
    var cursor = NativeFile.ValueChunkCursor{ .file = &file, .checkpoint = undefined, .pending = null, .chain_page = 0, .remaining = 0, .max_depth = 63 };
    defer cursor.deinit();
    const node = NativeFile.ExtentNode{ .height = 1, .count = 0 };
    for (0..4) |_| try cursor.pushFrame(node);
    try std.testing.expectEqual(@as(usize, 0), budget.alloc_calls);
    try std.testing.expectError(error.OutOfMemory, cursor.pushFrame(node));
    try std.testing.expectEqual(@as(usize, 4), cursor.frame_count);
    budget.limit = 128 * 1024;
    for (4..63) |_| try cursor.pushFrame(node);
    try std.testing.expectEqual(@as(usize, 1), budget.alloc_calls);
    try std.testing.expectEqual(@as(usize, 63), cursor.frames.capacity);
    try std.testing.expectError(error.InvalidNativeValueChain, cursor.pushFrame(node));
    for (0..63) |_| cursor.popFrame();
    for (0..63) |_| try cursor.pushFrame(node);
    try std.testing.expectEqual(@as(usize, 1), budget.alloc_calls);
    std.debug.print("LITE_CURSOR_FRAMES inline_levels=4 inline_heap=0 max_levels=63 overflow_bytes={d} allocations={d}\n", .{ budget.peak, budget.alloc_calls });
}

test "lite native immutable value traversal needs no heap scratch for ordinary extents" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/cursor-scratch.aflite", .{tmp.sub_path});
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    const payload = try a.alloc(u8, 1024 * 1024);
    defer a.free(payload);
    @memset(payload, 19);
    try file.putIndexCatalogRecord("/artifact", payload);
    file.page_cache_enabled.store(false, .monotonic);
    const checkpoint = file.activeCheckpoint();
    var value = try file.openIndexValue(a, "/artifact", checkpoint);
    defer value.deinit(a);
    var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 0 };
    file.allocator = budget.allocator();
    defer file.allocator = a;
    const Visitor = struct {
        length: usize = 0,
        crc: Crc32 = Crc32.init(),
        fn consume(raw: *anyopaque, offset: u64, bytes: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.length, offset);
            self.length += bytes.len;
            self.crc.update(bytes);
        }
    };
    for (0..8) |_| {
        var visitor = Visitor{};
        try file.visitIndexValue(value, 0, payload.len, checkpoint, &visitor, Visitor.consume);
        try std.testing.expectEqual(payload.len, visitor.length);
        try std.testing.expectEqual(Crc32.hash(payload), visitor.crc.final());
        var out: [8]u8 = undefined;
        try file.readAuthenticatedIndexValue(value, 0, payload.len, 123, &out, Crc32.hash(payload), checkpoint);
        try std.testing.expectEqualSlices(u8, payload[123..131], &out);
        const before = file.test_backing_read_bytes.load(.monotonic);
        var limited = Visitor{};
        try file.visitIndexValue(value, 123, 65536, checkpoint, &limited, Visitor.consume);
        try std.testing.expectEqual(@as(usize, 65536), limited.length);
        try std.testing.expectEqual(Crc32.hash(payload[123..][0..65536]), limited.crc.final());
        const bytes_read = file.test_backing_read_bytes.load(.monotonic) - before;
        // Account for overlapping leaves and two navigation levels, without
        // allowing speculative batches to run beyond this logical range.
        try std.testing.expect(bytes_read <= 65536 + 4 * file.header.page_size);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.alloc_calls);
    std.debug.print("LITE_CURSOR_TRAVERSAL bytes=1048576 traversals=24 heap_allocations={d}\n", .{budget.alloc_calls});
}

test "lite raw value leases retain replacement bytes and account retired pins" {
    const a = std.testing.allocator;
    var cache = PageCache{};
    defer cache.deinit(a);
    var manager = resource_manager_mod.ResourceManager.init(.{});
    cache.attachResourceManager(&manager);
    cache.put(a, 1, "original");
    const old = cache.acquireRaw(a, 1, 8).?;
    defer cache.releaseRaw(a, old);
    const same = cache.acquireRaw(a, 1, 8).?;
    try std.testing.expectEqual(old, same);
    cache.releaseRaw(a, same);
    cache.put(a, 1, "replaced");
    const newer = cache.acquireRaw(a, 1, 8).?;
    try std.testing.expect(newer != old);
    try std.testing.expectEqualStrings("original", old.bytes);
    try std.testing.expectEqualStrings("replaced", newer.bytes);
    cache.clear(a);
    try std.testing.expectEqual(old.size() + newer.size(), cache.total_bytes);
    try std.testing.expectEqual(cache.total_bytes, manager.sliceStats(.lite_native_page_cache).used_bytes);
    cache.releaseRaw(a, newer);
    cache.limit_bytes = old.size();
    cache.put(a, 2, "pressure");
    try std.testing.expect(!cache.pages.contains(2));
    try std.testing.expectEqualStrings("original", old.bytes);
    try std.testing.expectEqual(old.size(), cache.total_bytes);
}

test "lite raw value leases fall back without promotion allocation or headroom" {
    const a = std.testing.allocator;
    var cache = PageCache{};
    defer cache.deinit(a);
    cache.put(a, 1, "original");
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectEqual(@as(?*PageCache.RawView, null), cache.acquireRaw(failing.allocator(), 1, 8));
    try std.testing.expect(failing.has_induced_failure);
    var out: [8]u8 = undefined;
    try std.testing.expect(cache.copyInto(1, &out));
    try std.testing.expectEqualStrings("original", &out);
    cache.limit_bytes = 8;
    try std.testing.expectEqual(@as(?*PageCache.RawView, null), cache.acquireRaw(a, 1, 8));
    try std.testing.expectEqual(@as(usize, 8), cache.total_bytes);
}

test "lite warm value ranges borrow pages without copies or allocations" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "warm-value-lease.aflite");
    defer a.free(path);
    var counter = std.testing.FailingAllocator.init(a, .{});
    var file = try NativeFile.createWithIo(counter.allocator(), std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    const value = try a.alloc(u8, 1024 * 1024);
    defer a.free(value);
    @memset(value, 'v');
    try file.putIndexCatalogRecord("wal", value);
    for (0..2) |_| {
        const got = (try file.getIndexCatalogRecordRangeAlloc(a, "wal", 8192, 16)).?;
        defer a.free(got);
        try std.testing.expectEqualStrings(value[8192..][0..16], got);
    }
    const borrowed = file.test_value_cache_borrowed_bytes.load(.monotonic);
    const copied = file.test_value_cache_copy_bytes.load(.monotonic);
    const reads = file.test_value_read_calls.load(.monotonic);
    const allocations = counter.alloc_index;
    counter.fail_index = counter.alloc_index;
    for (0..100) |_| {
        const got = (try file.getIndexCatalogRecordRangeAlloc(a, "wal", 8192, 16)).?;
        defer a.free(got);
        try std.testing.expectEqualStrings(value[8192..][0..16], got);
    }
    try std.testing.expect(!counter.has_induced_failure);
    try std.testing.expectEqual(allocations, counter.alloc_index);
    try std.testing.expectEqual(reads, file.test_value_read_calls.load(.monotonic));
    try std.testing.expectEqual(copied, file.test_value_cache_copy_bytes.load(.monotonic));
    const bytes = file.test_value_cache_borrowed_bytes.load(.monotonic) - borrowed;
    try std.testing.expect(bytes > 100 * 16);
    std.debug.print("LITE_VALUE_LEASE ranges=100 returned_bytes=1600 borrowed_page_bytes={d} copied_page_bytes=0 backing_reads=0 allocations=0\n", .{bytes});
}

test "lite value cursor initialization errors release borrowed pages" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "value-lease-error.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    try file.putDocument("key", "value");
    const checkpoint = file.activeCheckpoint();
    const root = checkpoint.document_index_root_page;
    file.page_cache.clear(a);
    var scratch: [65536]u8 = undefined;
    _ = try file.readPageInto(root, checkpoint, &scratch);
    try std.testing.expectError(error.UnexpectedNativePageKind, NativeFile.ValueChunkCursor.initAtCheckpoint(&file, root, 5, checkpoint));
    try std.testing.expectEqual(@as(usize, 1), file.page_cache.pages.get(root).?.raw_view.?.references);
    file.page_cache.clear(a);
    try std.testing.expectEqual(@as(usize, 0), file.page_cache.total_bytes);
}

test "lite raw value leases retire under shared hard pressure" {
    const a = std.testing.allocator;
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.lite_native_page_cache)] = .{ .soft_limit_bytes = 8192, .hard_limit_bytes = 16384 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var cache = PageCache{};
    defer cache.deinit(a);
    cache.attachResourceManager(&manager);
    cache.put(a, 1, "original");
    const view = cache.acquireRaw(a, 1, 8).?;
    var external: u64 = 0;
    manager.observeUsage(.lite_native_page_cache, &external, 32768);
    defer manager.observeUsage(.lite_native_page_cache, &external, 0);
    try std.testing.expectEqual(@as(?*PageCache.RawView, null), cache.acquireRaw(a, 1, 8));
    try std.testing.expectEqual(@as(u32, 0), cache.pages.count());
    try std.testing.expectEqual(view.size(), cache.total_bytes);
    try std.testing.expectEqualStrings("original", view.bytes);
    try std.testing.expectEqual(view.size() + external, manager.sliceStats(.lite_native_page_cache).used_bytes);
    cache.releaseRaw(a, view);
    try std.testing.expectEqual(external, manager.sliceStats(.lite_native_page_cache).used_bytes);
}

test "lite borrowed value callbacks allow reentrant eviction and release on failure" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "value-lease-callback.aflite");
    defer a.free(path);
    var file = try NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
    defer file.close();
    var bytes: [8192]u8 = @splat('v');
    try file.putIndexCatalogRecord("value", &bytes);
    const checkpoint = file.activeCheckpoint();
    var value = try file.openIndexValue(a, "value", checkpoint);
    defer value.deinit(a);
    var warm: [16]u8 = undefined;
    try file.readIndexValueInto(value, 0, &warm, checkpoint);
    const Probe = struct {
        file: *NativeFile,
        value: NativeFile.IndexValue,
        checkpoint: CheckpointSlot,
        fn visit(raw: *anyopaque, _: u64, chunk: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // The outer callback must own a lease while residency disappears.
            self.file.page_cache.clear(self.file.allocator);
            try std.testing.expect(self.file.page_cache.total_bytes > 0);
            var nested: [16]u8 = undefined;
            try self.file.readIndexValueInto(self.value, 0, &nested, self.checkpoint);
            for (chunk) |byte| try std.testing.expectEqual(@as(u8, 'v'), byte);
            self.file.page_cache.clear(self.file.allocator);
            return error.CallbackStopped;
        }
    };
    var probe = Probe{ .file = &file, .value = value, .checkpoint = checkpoint };
    try std.testing.expectError(error.CallbackStopped, file.visitIndexValue(value, 0, 16, checkpoint, &probe, Probe.visit));
    try std.testing.expectEqual(@as(usize, 0), file.page_cache.total_bytes);
}

test "lite raw value leases survive concurrent replacement and eviction" {
    if (builtin.single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    var cache = PageCache{};
    defer cache.deinit(a);
    cache.put(a, 1, "original");
    const pinned = cache.acquireRaw(a, 1, 8).?;
    defer cache.releaseRaw(a, pinned);
    const Runner = struct {
        fn run(c: *PageCache, failed: *std.atomic.Value(bool)) void {
            for (0..200) |_| {
                c.put(std.testing.allocator, 1, "replaced");
                if (c.acquireRaw(std.testing.allocator, 1, 8)) |view| {
                    if (!std.mem.eql(u8, view.bytes, "replaced")) failed.store(true, .monotonic);
                    c.releaseRaw(std.testing.allocator, view);
                }
                c.clear(std.testing.allocator);
            }
        }
    };
    var failed = std.atomic.Value(bool).init(false);
    const thread = try std.Thread.spawn(.{}, Runner.run, .{ &cache, &failed });
    for (0..200) |_| {
        if (!std.mem.eql(u8, "original", pinned.bytes)) failed.store(true, .monotonic);
        cache.clear(a);
    }
    thread.join();
    try std.testing.expect(!failed.load(.monotonic));
    try std.testing.expectEqualStrings("original", pinned.bytes);
}
