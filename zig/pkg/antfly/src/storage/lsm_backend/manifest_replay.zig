// Copyright 2026 Antfly, Inc.
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

//! Streaming replay owns only live metadata, one record being decoded and a
//! fixed I/O buffer. The catalog is unpublished until every complete frame is
//! checked; a corrupt frame discards the entire candidate, never a partial edit.
const std = @import("std");
const manifest = @import("../lsm/manifest.zig");
const storage_io = @import("storage_io.zig");
const set = @import("manifest_set.zig");
const Crc32 = @import("antfly_hash").Crc32;
const Allocator = std.mem.Allocator;

pub const RecoveryState = struct {
    descriptor: set.Descriptor,
    segment_ids: [set.max_segments]u64 = @splat(0),
    segment_count: usize = 0,
    last_sequence: u64 = 0,
    checkpoint_bytes: u64 = 0,
    valid_bytes: u64 = 0,
    valid_active_size: u64 = 0,
    torn_tail_bytes: u64 = 0,
};

pub const Catalog = struct {
    runs: std.AutoHashMapUnmanaged(u64, manifest.OwnedRunMeta) = .empty,
    paths: std.StringHashMapUnmanaged(manifest.OwnedObsoletePathMeta) = .empty,
    next_run_id: u64 = 0,
    next_sequence: u64 = 0,
    active_segment: u64 = 0,
    segments: usize = 0,
    bytes_read: u64 = 0,
    recovery: ?RecoveryState = null,

    pub fn deinit(self: *Catalog, allocator: Allocator) void {
        var runs = self.runs.valueIterator();
        while (runs.next()) |run| run.deinit(allocator);
        self.runs.deinit(allocator);
        var paths = self.paths.valueIterator();
        while (paths.next()) |path| path.deinit(allocator);
        self.paths.deinit(allocator);
        self.* = .{};
    }
};

const Reader = struct {
    storage: storage_io.Storage,
    allocator: Allocator,
    path: []const u8,
    size: u64,
    offset: u64 = 0,
    buffer: []u8,
    used: usize = 0,
    filled: usize = 0,
    remaining: ?u64 = null,
    outer: Crc32 = Crc32.init(),
    inner: ?Crc32 = null,

    fn read(self: *Reader, out: []u8) !void {
        if (out.len > self.size - self.offset) return error.InvalidManifest;
        if (self.remaining) |remaining| {
            if (out.len > remaining) return error.InvalidManifest;
            self.remaining = remaining - out.len;
        }
        var dest = out;
        while (dest.len != 0) {
            if (self.used == self.filled) {
                self.filled = @intCast(@min(self.buffer.len, self.size - self.offset));
                try self.storage.readFileRangeInto(self.allocator, self.path, self.offset, self.buffer[0..self.filled]);
                self.used = 0;
            }
            const len = @min(dest.len, self.filled - self.used);
            @memcpy(dest[0..len], self.buffer[self.used..][0..len]);
            self.used += len;
            self.offset += len;
            dest = dest[len..];
        }
        if (self.remaining != null) self.outer.update(out);
        if (self.inner) |*crc| crc.update(out);
    }

    fn int(self: *Reader, comptime T: type) !T {
        var raw: [@sizeOf(T)]u8 = undefined;
        try self.read(&raw);
        return std.mem.readInt(T, &raw, .little);
    }

    fn bytes(self: *Reader, len: usize) ![]u8 {
        if (len > (self.remaining orelse self.size - self.offset)) return error.InvalidManifest;
        const result = try self.allocator.alloc(u8, len);
        errdefer self.allocator.free(result);
        try self.read(result);
        return result;
    }

    fn optionalBytes(self: *Reader, len: usize) !?[]u8 {
        return if (len == 0) null else try self.bytes(len);
    }

    fn run(self: *Reader, version: u32) !manifest.OwnedRunMeta {
        const id = try self.int(u64);
        const legacy_sequence = if (version == 10) try self.int(u64) else id;
        var out: manifest.OwnedRunMeta = .{
            .id = id,
            .visibility_id = legacy_sequence,
            .level = try self.int(u32),
            .size_bytes = try self.int(u64),
            .path = &.{},
            .smallest_namespace_name = null,
            .smallest_key = &.{},
            .largest_namespace_name = null,
            .largest_key = &.{},
            .entry_count = 0,
        };
        errdefer out.deinit(self.allocator);
        inline for (.{ "logical_entry_bytes", "physical_entry_bytes", "raw_blocks", "compressed_blocks", "compression_codec_mask" }) |field|
            @field(out.compression_stats, field) = try self.int(u64);
        const path_len = try self.int(u32);
        const lower_ns_len = try self.int(u32);
        const lower_len = try self.int(u32);
        const upper_ns_len = try self.int(u32);
        const upper_len = try self.int(u32);
        out.entry_count = try self.int(u32);
        if (version >= 11) {
            const tombstones = try self.int(u64);
            if (tombstones != std.math.maxInt(u64)) {
                if (tombstones > out.entry_count) return error.InvalidManifest;
                out.tombstone_count = @intCast(tombstones);
            }
            out.oldest_tombstone_unix_ns = try self.int(u64);
            out.visibility_id = try self.int(u64);
            const gc = try self.int(u32);
            if (gc > 1) return error.InvalidManifest;
            out.gc_requested = gc != 0;
        }
        if (out.id == 0 or path_len == 0) return error.InvalidManifest;
        out.path = try self.bytes(path_len);
        out.smallest_namespace_name = try self.optionalBytes(lower_ns_len);
        out.smallest_key = try self.bytes(lower_len);
        out.largest_namespace_name = try self.optionalBytes(upper_ns_len);
        out.largest_key = try self.bytes(upper_len);
        return out;
    }

    /// Returns false only for an incomplete final frame. Header corruption is
    /// never treated as a torn tail. Check complete length before any mutation.
    fn frame(self: *Reader, catalog: *Catalog, checkpoint: bool, sealed: bool) !bool {
        if (self.size - self.offset < 24) {
            if (sealed and self.offset != self.size) return error.InvalidManifest;
            return false;
        }
        var header: [24]u8 = undefined;
        try self.read(&header);
        if (Crc32.hash(header[0..20]) != std.mem.readInt(u32, header[20..24], .little) or
            std.mem.readInt(u64, header[8..16], .little) != catalog.next_sequence or
            std.mem.readInt(u32, header[16..20], .little) != @intFromBool(checkpoint)) return error.InvalidManifest;
        const len = std.mem.readInt(u64, header[0..8], .little);
        if (len > std.math.maxInt(u32)) return error.InvalidManifest;
        if (len + 4 > self.size - self.offset) {
            if (sealed) return error.InvalidManifest;
            return false;
        }
        self.remaining = len;
        self.outer = Crc32.init();
        const removed_runs = try self.int(u32);
        if (removed_runs > self.remaining.? / 8 or (checkpoint and removed_runs != 0)) return error.InvalidManifest;
        for (0..removed_runs) |_| {
            var old = catalog.runs.fetchRemove(try self.int(u64)) orelse return error.InvalidManifest;
            old.value.deinit(self.allocator);
        }
        const removed_paths = try self.int(u32);
        if (removed_paths > self.remaining.? / 4 or (checkpoint and removed_paths != 0)) return error.InvalidManifest;
        for (0..removed_paths) |_| {
            const path = try self.bytes(try self.int(u32));
            defer self.allocator.free(path);
            var old = catalog.paths.fetchRemove(path) orelse return error.InvalidManifest;
            old.value.deinit(self.allocator);
        }
        self.inner = Crc32.init();
        var magic: [8]u8 = undefined;
        try self.read(&magic);
        if (!std.mem.eql(u8, &magic, manifest.magic)) return error.InvalidManifest;
        const version = try self.int(u32);
        if (version != manifest.version and version != 10 and version != 9) return error.UnsupportedVersion;
        const next_id = try self.int(u64);
        if (!checkpoint and next_id < catalog.next_run_id) return error.InvalidManifest;
        const runs = try self.int(u32);
        const paths = try self.int(u32);
        if (runs > self.remaining.? / @as(u64, if (version >= 11) 112 else if (version >= 10) 92 else 84) or paths > self.remaining.? / 12) return error.InvalidManifest;
        for (0..runs) |_| {
            var entry = try self.run(version);
            errdefer entry.deinit(self.allocator);
            if (entry.visibility_id >= next_id) return error.InvalidManifest;
            const slot = try catalog.runs.getOrPut(self.allocator, entry.id);
            if (slot.found_existing) slot.value_ptr.deinit(self.allocator);
            slot.value_ptr.* = entry;
        }
        for (0..paths) |_| {
            const deadline = try self.int(u64);
            const path_len = try self.int(u32);
            if (path_len == 0) return error.InvalidManifest;
            const path = try self.bytes(path_len);
            errdefer self.allocator.free(path);
            const slot = try catalog.paths.getOrPut(self.allocator, path);
            if (slot.found_existing) slot.value_ptr.deinit(self.allocator);
            slot.key_ptr.* = path;
            slot.value_ptr.* = .{ .path = path, .delete_after_ns = deadline };
        }
        const inner = self.inner.?.final();
        self.inner = null;
        if (try self.int(u32) != inner or self.remaining.? != 0) return error.InvalidManifest;
        self.remaining = null;
        if (try self.int(u32) != self.outer.final()) return error.InvalidManifest;
        catalog.next_sequence = std.math.add(u64, catalog.next_sequence, 1) catch return error.InvalidManifest;
        catalog.next_run_id = next_id;
        return true;
    }
};

pub fn load(allocator: Allocator, storage: storage_io.Storage, root: []const u8, descriptor: set.Descriptor, limit: usize) !Catalog {
    var catalog: Catalog = .{ .next_sequence = descriptor.sequence, .recovery = .{ .descriptor = descriptor } };
    errdefer catalog.deinit(allocator);
    const buffer = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(buffer);
    const checkpoint = try set.pathAlloc(allocator, root, descriptor.checkpoint, .checkpoint);
    defer allocator.free(checkpoint);
    const size = try storage.fileSize(checkpoint);
    if (size > limit) return error.FileTooBig;
    var reader = Reader{ .storage = storage, .allocator = allocator, .path = checkpoint, .size = size, .buffer = buffer };
    var magic: [8]u8 = undefined;
    try reader.read(&magic);
    if (!std.mem.eql(u8, &magic, manifest.journal_magic) or !try reader.frame(&catalog, true, true) or reader.offset != size) return error.InvalidManifest;
    catalog.bytes_read = size;
    catalog.recovery.?.checkpoint_bytes = size;
    catalog.recovery.?.valid_bytes = size;
    var id = descriptor.first_segment;
    while (catalog.segments < set.max_segments) {
        // NEXT is observed before the segment length: a sealed segment cannot
        // change underneath this read. An active append is bounded by its size
        // snapshot, and only its last incomplete frame may be ignored.
        const next = try set.nextSegment(allocator, storage, root, id);
        const path = try set.pathAlloc(allocator, root, id, .journal);
        defer allocator.free(path);
        const segment_size = try storage.fileSize(path);
        if (segment_size > limit -| catalog.bytes_read) return error.FileTooBig;
        reader = .{ .storage = storage, .allocator = allocator, .path = path, .size = segment_size, .buffer = buffer };
        var header: [set.header_len]u8 = undefined;
        try reader.read(&header);
        if (!std.mem.eql(u8, header[0..8], "ALSMSEG1") or std.mem.readInt(u64, header[8..16], .little) != id or std.mem.readInt(u64, header[16..24], .little) != catalog.next_sequence or Crc32.hash(header[0..24]) != std.mem.readInt(u32, header[24..28], .little)) return error.InvalidManifest;
        var valid_end = reader.offset;
        while (reader.offset < reader.size) {
            if (!try reader.frame(&catalog, false, next != null)) break;
            valid_end = reader.offset;
        }
        catalog.recovery.?.segment_ids[catalog.segments] = id;
        catalog.recovery.?.segment_count += 1;
        catalog.recovery.?.last_sequence = catalog.next_sequence - 1;
        catalog.recovery.?.valid_bytes += valid_end - set.header_len;
        catalog.recovery.?.valid_active_size = valid_end;
        catalog.recovery.?.torn_tail_bytes = segment_size - valid_end;
        catalog.bytes_read += segment_size;
        catalog.active_segment = id;
        catalog.segments += 1;
        id = next orelse return catalog;
    }
    return error.ManifestJournalBacklogExceeded;
}

test "streaming replay validates large records with bounded reads and allocation failure cleanup" {
    const allocator = std.testing.allocator;
    var memory = storage_io.MemoryStorage.init(allocator);
    defer memory.deinit();
    const storage = memory.storage();
    const root = "/streaming-replay";
    const key = try allocator.alloc(u8, 128 * 1024 + 7);
    defer allocator.free(key);
    @memset(key, 'k');
    const Source = struct {
        key: []const u8,
        index: usize = 0,
        path_done: bool = false,
        pub fn runCount(_: *@This()) usize {
            return 3;
        }
        pub fn obsoleteCount(_: *@This()) usize {
            return 1;
        }
        pub fn nextRun(self: *@This()) ?manifest.RunMeta {
            if (self.index == 3) return null;
            self.index += 1;
            return .{ .id = self.index, .level = 1, .size_bytes = 1024, .path = "runs/example.tbl", .smallest_namespace_name = "docs", .smallest_key = self.key, .largest_namespace_name = "docs", .largest_key = self.key, .entry_count = 1 };
        }
        pub fn nextObsolete(self: *@This()) ?manifest.ObsoletePathMeta {
            if (self.path_done) return null;
            self.path_done = true;
            return .{ .path = "runs/old.tbl", .delete_after_ns = 42 };
        }
    };
    var source: Source = .{ .key = key };
    const descriptor: set.Descriptor = .{ .checkpoint = 4, .first_segment = 4, .sequence = 0 };
    _ = try set.writeCheckpoint(allocator, storage, root, 4, 0, 5, &source, 2 * 1024 * 1024);
    try set.createSegment(allocator, storage, root, 4, 1);
    const edit = try manifest.encodeJournalFrameAlloc(allocator, 1, false, &.{2}, &.{"runs/old.tbl"}, .{
        .next_run_id = 6,
        .runs = &.{.{ .id = 1, .level = 2, .size_bytes = 2048, .path = "runs/example.tbl", .smallest_namespace_name = "docs", .smallest_key = "new", .largest_namespace_name = "docs", .largest_key = "new", .entry_count = 2 }},
        .obsolete_paths = &.{.{ .path = "runs/new.tbl", .delete_after_ns = 84 }},
    });
    defer allocator.free(edit);
    const segment = try set.pathAlloc(allocator, root, 4, .journal);
    defer allocator.free(segment);
    try storage.appendFileAbsolute(allocator, segment, edit, true);
    const Checked = struct {
        fn file(ptr: *anyopaque, alloc: Allocator, path: []const u8, limit: usize) ![]u8 {
            if (limit > 64 * 1024) return error.UnboundedManifestRead;
            const backing: *storage_io.MemoryStorage = @ptrCast(@alignCast(ptr));
            return backing.storage().readFileAlloc(alloc, path, limit);
        }
        fn range(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8) !void {
            if (out.len > 64 * 1024) return error.UnboundedManifestRead;
            const backing: *storage_io.MemoryStorage = @ptrCast(@alignCast(ptr));
            return backing.storage().readFileRangeInto(std.testing.allocator, path, offset, out);
        }
        fn check(alloc: Allocator, checked: storage_io.Storage, desc: set.Descriptor) !void {
            var catalog = try load(alloc, checked, "/streaming-replay", desc, 2 * 1024 * 1024);
            defer catalog.deinit(alloc);
            try std.testing.expectEqual(@as(u32, 2), catalog.runs.count());
            try std.testing.expectEqualStrings("new", catalog.runs.get(1).?.smallest_key);
            try std.testing.expect(!catalog.runs.contains(2));
            try std.testing.expect(!catalog.paths.contains("runs/old.tbl"));
            try std.testing.expectEqual(@as(u64, 84), catalog.paths.get("runs/new.tbl").?.delete_after_ns);
        }
    };
    var checked_vtable = storage.vtable.*;
    checked_vtable.read_file_alloc = Checked.file;
    checked_vtable.read_file_range_into = Checked.range;
    const checked: storage_io.Storage = .{ .ptr = storage.ptr, .vtable = &checked_vtable };
    try std.testing.checkAllAllocationFailures(allocator, Checked.check, .{ checked, descriptor });
    try std.testing.expectError(error.UnboundedManifestRead, set.load(allocator, checked, root, descriptor, 2 * 1024 * 1024));
    // Corruption after earlier valid records still discards the entire catalog.
    const checkpoint = try set.pathAlloc(allocator, root, 4, .checkpoint);
    defer allocator.free(checkpoint);
    const raw = try storage.readFileAlloc(allocator, checkpoint, 2 * 1024 * 1024);
    defer allocator.free(raw);
    for ([_]usize{ 40, 64 * 1024, raw.len - 1 }) |offset| {
        raw[offset] ^= 1;
        try set.replace(allocator, storage, checkpoint, raw);
        try std.testing.expectError(error.InvalidManifest, load(allocator, checked, root, descriptor, 2 * 1024 * 1024));
        raw[offset] ^= 1;
    }
}

test "streaming manifest replay churn benchmark" {
    if (@import("builtin").mode != .ReleaseFast) return error.SkipZigTest;
    const allocator = std.heap.smp_allocator;
    const time = @import("antfly_platform").time;
    for ([_]usize{ 64, 256, 1024 }) |edits| {
        var memory = storage_io.MemoryStorage.init(allocator);
        defer memory.deinit();
        const storage = memory.storage();
        const root = "/replay-benchmark";
        const descriptor: set.Descriptor = .{ .checkpoint = 1001, .first_segment = 1001, .sequence = 0 };
        const Source = struct {
            index: usize = 0,
            pub fn runCount(_: *@This()) usize {
                return 1000;
            }
            pub fn obsoleteCount(_: *@This()) usize {
                return 0;
            }
            pub fn nextRun(self: *@This()) ?manifest.RunMeta {
                if (self.index == 1000) return null;
                self.index += 1;
                return .{ .id = self.index, .level = 1, .size_bytes = 1024, .path = "runs/example.tbl", .smallest_namespace_name = "docs", .smallest_key = "key", .largest_namespace_name = "docs", .largest_key = "key", .entry_count = 1 };
            }
            pub fn nextObsolete(_: *@This()) ?manifest.ObsoletePathMeta {
                return null;
            }
        };
        var source: Source = .{};
        const limit = 128 * 1024 * 1024;
        _ = try set.writeCheckpoint(allocator, storage, root, 1001, 0, 1002, &source, limit);
        try set.createSegment(allocator, storage, root, 1001, 1);
        const path = try set.pathAlloc(allocator, root, 1001, .journal);
        defer allocator.free(path);
        {
            const key = try allocator.alloc(u8, 16 * 1024);
            defer allocator.free(key);
            @memset(key, 'k');
            var suffix: std.ArrayListUnmanaged(u8) = .empty;
            defer suffix.deinit(allocator);
            for (0..edits) |i| {
                const frame = try manifest.encodeJournalFrameAlloc(allocator, i + 1, false, &.{}, &.{}, .{ .next_run_id = 1002 + i, .runs = &.{.{ .id = 123, .level = 1, .size_bytes = 1024, .path = "runs/example.tbl", .smallest_namespace_name = "docs", .smallest_key = key, .largest_namespace_name = "docs", .largest_key = key, .entry_count = 1 }} });
                defer allocator.free(frame);
                try suffix.appendSlice(allocator, frame);
            }
            try storage.appendFileAbsolute(allocator, path, suffix.items, true);
        }
        var elapsed: [2][3]u64 = undefined;
        var retained: [2]usize = undefined;
        var allocated: [2]usize = undefined;
        for (0..3) |sample| for (0..2) |turn| {
            const mode = (sample + turn) % 2;
            var tracker = std.testing.FailingAllocator.init(allocator, .{});
            const tracked = tracker.allocator();
            const started = time.monotonicNs();
            if (mode == 0) {
                const loaded = try set.load(tracked, storage, root, descriptor, limit);
                var decoded = manifest.decodeBorrowedOwnedAlloc(tracked, loaded.bytes) catch |err| {
                    tracked.free(loaded.bytes);
                    return err;
                };
                elapsed[mode][sample] = time.monotonicNs() - started;
                retained[mode] = tracker.allocated_bytes - tracker.freed_bytes;
                allocated[mode] = tracker.allocated_bytes;
                try std.testing.expectEqual(@as(usize, 1000), decoded.runs.len);
                decoded.deinit(tracked);
            } else {
                var decoded = try load(tracked, storage, root, descriptor, limit);
                elapsed[mode][sample] = time.monotonicNs() - started;
                retained[mode] = tracker.allocated_bytes - tracker.freed_bytes;
                allocated[mode] = tracker.allocated_bytes;
                try std.testing.expectEqual(@as(u32, 1000), decoded.runs.count());
                decoded.deinit(tracked);
            }
            try std.testing.expectEqual(tracker.allocated_bytes, tracker.freed_bytes);
        };
        std.debug.print("\nLSM replay edits={d} buffered_ns={any} streaming_ns={any} buffered_retained_bytes={d} streaming_retained_bytes={d} buffered_allocated_bytes={d} streaming_allocated_bytes={d}\n", .{ edits, elapsed[0], elapsed[1], retained[0], retained[1], allocated[0], allocated[1] });
    }
}
