// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Streaming maintenance for an idle prepaid completion pool. Callers pin all
//! input runs and serialize publication; this module grants no Raft admission.
const std = @import("std");
const io_mod = @import("storage_io.zig");
const table = @import("../lsm/table_file.zig");
const repository = @import("repository.zig");
const state = @import("state.zig");
const domains = @import("completion_allocator.zig");
pub const capacity = @import("completion_capacity.zig");
const Allocator = std.mem.Allocator;

comptime {
    // The certificate documents v11 packed offsets, 14-bit block filters,
    // fixed run filter and prefix-none output. Format changes need a new proof.
    if (table.version != 11 or table.default_block_size != capacity.block_bytes or
        @sizeOf(table.SequentialTableIndex.Block) != 32)
        @compileError("update completion format-cost certificate for the SST format");
}

pub const Limits = struct {
    max_inputs: usize = 68,
    max_outputs: usize = 64,
    max_metadata_bytes: usize = 1024 * 1024,
    max_output_metadata_bytes: usize = 1024 * 1024,
    max_block_bytes: usize = 1024 * 1024,
    max_record_bytes: usize = 256 * 1024,
    max_frontier_bytes: usize = 16 * 1024 * 1024,
};

const Cursor = struct {
    allocator: Allocator,
    io: io_mod.Storage,
    path: []const u8,
    index: table.SequentialTableIndex,
    limits: Limits,
    block: usize = 0,
    entry_in_block: usize = 0,
    advanced_entries: usize = 0,
    offset: usize = 0,
    loaded: ?[]u8 = null,
    entry: ?table.Entry = null,
    frontier_bytes: usize,

    fn init(allocator: Allocator, io: io_mod.Storage, path: []const u8, limits: Limits) !Cursor {
        const size = try io.fileSize(path);
        if (size < table.header_len + table.footer_len) return error.InvalidTableFile;
        var footer_bytes: [table.footer_len]u8 = undefined;
        try io.readFileRangeInto(allocator, path, size - table.footer_len, &footer_bytes);
        const footer = try table.decodeFooterBytes(&footer_bytes);
        if (footer.metadata_len > limits.max_metadata_bytes or footer.metadata_offset > size - table.footer_len or
            footer.metadata_len != size - table.footer_len - footer.metadata_offset) return error.UnsupportedCompletionProfile;
        const metadata = try allocator.alloc(u8, footer.metadata_len);
        defer allocator.free(metadata);
        try io.readFileRangeInto(allocator, path, footer.metadata_offset, metadata);
        var index = try table.decodeSequentialIndexFromFooterAlloc(allocator, footer, metadata);
        errdefer index.deinit(allocator);
        var largest: usize = 0;
        for (index.blocks) |block| {
            if (block.window.len > limits.max_block_bytes or block.window.physicalLen() > limits.max_block_bytes)
                return error.UnsupportedCompletionProfile;
            largest = @max(largest, block.window.len +| block.window.physicalLen());
        }
        return .{ .allocator = allocator, .io = io, .path = path, .index = index, .limits = limits, .frontier_bytes = index.blocks.len *| @sizeOf(table.SequentialTableIndex.Block) +| largest };
    }
    fn deinit(self: *Cursor) void {
        if (self.loaded) |bytes| self.allocator.free(bytes);
        self.index.deinit(self.allocator);
    }
    fn current(self: *Cursor) !?table.Entry {
        if (self.block >= self.index.blocks.len) {
            if (self.advanced_entries != self.index.entry_count) return error.InvalidTableFile;
            return null;
        }
        if (self.entry) |entry| return entry;
        if (self.loaded == null) {
            const window = self.index.blocks[self.block].window;
            const physical = try self.allocator.alloc(u8, window.physicalLen());
            defer self.allocator.free(physical);
            try self.io.readFileRangeInto(self.allocator, self.path, self.index.entry_data_start + window.physicalRelativeOffset(), physical);
            self.loaded = try table.decodeBlockPayloadAlloc(self.allocator, window.compression, physical, window.len, window.checksum);
        }
        const entry = try table.parseEntryAt(self.loaded.?, self.offset);
        if (encodedEntryLen(entry) > self.limits.max_record_bytes) return error.UnsupportedCompletionProfile;
        self.entry = entry;
        return entry;
    }
    fn advance(self: *Cursor) !void {
        const entry = (try self.current()) orelse return;
        self.offset += encodedEntryLen(entry);
        if (self.offset > self.loaded.?.len) return error.InvalidTableFile;
        self.entry = null;
        self.entry_in_block += 1;
        self.advanced_entries += 1;
        if (self.advanced_entries > self.index.entry_count) return error.InvalidTableFile;
        if (self.entry_in_block > self.index.blocks[self.block].entry_count) return error.InvalidTableFile;
        if (self.entry_in_block == self.index.blocks[self.block].entry_count) {
            if (self.offset != self.loaded.?.len) return error.InvalidTableFile;
            self.allocator.free(self.loaded.?);
            self.loaded = null;
            self.offset = 0;
            self.entry_in_block = 0;
            self.block += 1;
        }
    }
};

fn encodedEntryLen(entry: table.Entry) usize {
    return 13 +| (if (entry.namespace_name) |name| name.len else 0) +| entry.key.len +| entry.value.len;
}
fn order(a: table.Entry, b: table.Entry) std.math.Order {
    const ns = state.compareNamespace(.{ .name = a.namespace_name }, .{ .name = b.namespace_name });
    return if (ns == .eq) std.mem.order(u8, a.key, b.key) else ns;
}
fn stateEntry(entry: anytype) table.Entry {
    return .{ .namespace_name = entry.namespace_name, .key = entry.key, .value = entry.value, .tombstone = entry.tombstone };
}

/// Qualify immutable input format and aggregate live cursor memory. This is a
/// frontier limit, not a limit on the number of records streamed through it.
pub fn qualify(allocator: Allocator, io: io_mod.Storage, paths: []const []const u8, limits: Limits) !void {
    _ = try measure(allocator, io, paths, limits);
}

pub const Measurement = struct { cost: capacity.Cost, frontier_bytes: usize };

/// Counting all input versions (including overwritten records) is conservative:
/// a merge emits a subset. No data-sized vector or set is needed for the proof.
pub fn measure(allocator: Allocator, io: io_mod.Storage, paths: []const []const u8, limits: Limits) !Measurement {
    if (paths.len > limits.max_inputs) return error.UnsupportedCompletionProfile;
    var frontier: usize = 0;
    var cost: capacity.Cost = .{};
    for (paths) |path| {
        var cursor = try Cursor.init(allocator, io, path, limits);
        defer cursor.deinit();
        frontier +|= cursor.frontier_bytes;
        if (frontier > limits.max_frontier_bytes) return error.UnsupportedCompletionProfile;
        // Block bounds alone do not constrain one unusually large record.
        // Activation performs this read-only scan before issuing readiness.
        while (try cursor.current()) |entry| {
            cost = try cost.plus(try capacity.Cost.record(if (entry.namespace_name) |ns| ns.len else 0, entry.key.len, entry.value.len));
            try cursor.advance();
        }
    }
    return .{ .cost = cost, .frontier_bytes = frontier };
}

const Output = struct {
    allocator: Allocator,
    writer: repository.StreamingRunFileWriter = undefined,
    run: repository.Run,
    fn init(self: *Output, allocator: Allocator, io: io_mod.Storage, root: []const u8, id: u64) !void {
        self.* = .{ .allocator = allocator, .run = .{ .id = id, .level = 1, .size_bytes = 0, .path = null, .smallest_namespace_name = null, .smallest_key = &.{}, .largest_namespace_name = null, .largest_key = &.{}, .entry_count = 0, .bloom_filter = null, .state = null, .tombstone_count = 0 } };
        // A fixed small run filter may saturate (extra false positives), but
        // never rejects an inserted key. Block filters remain bounded by the
        // encoder block size; no allocation is sized from the whole store.
        try self.writer.initInPlace(io, allocator, root, id, 1, std.math.maxInt(u32), .{ .bits_per_key = 1, .min_bits = 64, .max_hash_count = 1 }, .snappy_adaptive, .none, null, .cold_sequential);
    }
    fn deinit(self: *Output) void {
        self.writer.deinit();
        self.run.deinit(self.allocator);
    }
    fn fits(self: *Output, entry: table.Entry, limits: Limits) bool {
        const total = self.writer.encoder.encodedSizeUpperBoundAfterEntry(entry) catch return false;
        const data = self.writer.sink.len() +| self.writer.encoder.block_bytes.items.len +| encodedEntryLen(entry) +| table.footer_len;
        return total >= data and total - data <= @min(limits.max_metadata_bytes, limits.max_output_metadata_bytes) and self.writer.canAppendEntry(entry);
    }
    fn append(self: *Output, entry: table.Entry) !void {
        const allocator = self.allocator;
        if (self.run.entry_count == 0) {
            self.run.smallest_namespace_name = if (entry.namespace_name) |name| try allocator.dupe(u8, name) else null;
            self.run.smallest_key = try allocator.dupe(u8, entry.key);
        }
        const last_ns = if (entry.namespace_name) |name| try allocator.dupe(u8, name) else null;
        errdefer if (last_ns) |name| allocator.free(name);
        const last_key = try allocator.dupe(u8, entry.key);
        errdefer allocator.free(last_key);
        try self.writer.appendEntry(entry);
        if (self.run.largest_namespace_name) |name| allocator.free(name);
        allocator.free(self.run.largest_key);
        self.run.largest_namespace_name = last_ns;
        self.run.largest_key = last_key;
        self.run.entry_count += 1;
        if (entry.tombstone) self.run.tombstone_count.? += 1;
    }
    fn finish(self: *Output) !repository.Run {
        var file = try self.writer.finish();
        // On-disk lookup uses the encoded fixed filter; do not retain a second
        // in-memory copy in the newly published run metadata.
        file.filter.deinit(self.allocator);
        self.run.path = file.path;
        self.run.owns_path = true;
        self.run.size_bytes = file.size_bytes;
        self.run.compression_stats = file.compression_stats;
        const result = self.run;
        self.run = .{ .id = 0, .level = 0, .size_bytes = 0, .path = null, .smallest_namespace_name = null, .smallest_key = &.{}, .largest_namespace_name = null, .largest_key = &.{}, .entry_count = 0, .bloom_filter = null, .state = null };
        return result;
    }
};

/// Inputs are newest first. The mutable snapshot wins over every run. Outputs
/// and all temporary blocks use the caller's preowned recycling domain; no
/// ordinary allocator, FD admission, or unbounded whole-store materialization.
/// The caller owns cleanup of written paths after any failure.
pub fn build(allocator: Allocator, io: io_mod.Storage, root: []const u8, paths: []const []const u8, mutable: *const state.State, output_base: u64, limits: Limits) !std.ArrayListUnmanaged(repository.Run) {
    if (!domains.isPrepaid(allocator) or paths.len > 68 or paths.len > limits.max_inputs or limits.max_outputs > 64)
        return error.UnsupportedCompletionProfile;
    var cursors: [68]Cursor = undefined;
    var cursor_count: usize = 0;
    defer for (cursors[0..cursor_count]) |*cursor| cursor.deinit();
    var frontier: usize = 0;
    for (paths) |path| {
        cursors[cursor_count] = try Cursor.init(allocator, io, path, limits);
        frontier +|= cursors[cursor_count].frontier_bytes;
        cursor_count += 1;
        if (frontier > limits.max_frontier_bytes) return error.UnsupportedCompletionProfile;
    }
    var results: std.ArrayListUnmanaged(repository.Run) = .empty;
    errdefer {
        for (results.items) |*run| run.deinit(allocator);
        results.deinit(allocator);
    }
    try results.ensureTotalCapacity(allocator, limits.max_outputs);
    var output: Output = undefined;
    var active = false;
    defer if (active) output.deinit();
    var mutable_cursor: state.State.EntryCursor = .{};
    var mutable_position: usize = 0;
    while (true) {
        var entries: [69]?table.Entry = @splat(null);
        if (mutable_position < mutable.entryCount()) entries[0] = stateEntry(mutable_cursor.at(mutable, mutable_position));
        for (cursors[0..cursor_count], 0..) |*cursor, i| entries[i + 1] = try cursor.current();
        var selected: ?table.Entry = null;
        for (entries[0 .. cursor_count + 1]) |maybe| if (maybe) |entry| {
            if (selected == null or order(entry, selected.?) == .lt) selected = entry;
        };
        const winner = selected orelse break;
        if (encodedEntryLen(winner) > limits.max_record_bytes) return error.UnsupportedCompletionProfile;
        if (active and !output.fits(winner, limits)) {
            results.appendAssumeCapacity(try output.finish());
            output.deinit();
            active = false;
        }
        if (!active) {
            if (results.items.len == limits.max_outputs) return error.UnsupportedCompletionProfile;
            try output.init(allocator, io, root, try std.math.add(u64, output_base, results.items.len));
            active = true;
        }
        if (!output.fits(winner, limits)) return error.UnsupportedCompletionProfile;
        try output.append(winner);
        var advance: [69]bool = @splat(false);
        for (entries[0 .. cursor_count + 1], 0..) |maybe, i| if (maybe) |entry| {
            advance[i] = order(entry, winner) == .eq;
        };
        if (advance[0]) mutable_position += 1;
        for (cursors[0..cursor_count], 0..) |*cursor, i| if (advance[i + 1]) try cursor.advance();
    }
    if (active) {
        results.appendAssumeCapacity(try output.finish());
        output.deinit();
        active = false;
    }
    return results;
}

test "workload admission completion capacity certificate bounds actual SST metadata and greedy splits" {
    const alloc = std.testing.allocator;
    // Different binary-key shapes exercise both packed multi-entry blocks and
    // oversized individual records. The proof must cover the actual encoder,
    // including power-of-two block Bloom rounding and namespace bounds.
    for ([_]usize{ 8, 1400, 40 * 1024 }) |key_len| {
        var key: [40 * 1024]u8 = @splat(0xa5);
        const count: usize = if (key_len > 1400) 12 else 200;
        const metadata_limit: u64 = if (key_len > 1400) 256 * 1024 else 64 * 1024;
        var total: capacity.Cost = .{};
        var current: capacity.Cost = .{};
        var sink_impl = table.MemoryTableSink.init(alloc);
        defer sink_impl.deinit();
        var sink = sink_impl.sink();
        const options: table.StreamingEncoderOptions = .{ .bloom_config = .{ .bits_per_key = 1, .min_bits = 64, .max_hash_count = 1 }, .block_compression = .snappy_adaptive, .prefix_extractor = .none };
        var encoder = try table.StreamingEncoder.init(alloc, &sink, 1, options);
        defer encoder.deinit();
        var outputs: u64 = 1;
        var blocks: u64 = 0;
        for (0..count) |i| {
            std.mem.writeInt(u64, key[0..8], i, .big);
            const entry: table.Entry = .{ .namespace_name = "binary\x00namespace", .key = key[0..key_len], .value = "payload", .tombstone = i % 7 == 0 };
            const cost = try capacity.Cost.record(entry.namespace_name.?.len, entry.key.len, entry.value.len);
            total = try total.plus(cost);
            var projected = try encoder.encodedSizeUpperBoundAfterEntry(entry);
            var data = sink.len() + encoder.block_bytes.items.len + encodedEntryLen(entry) + table.footer_len;
            if (projected - data > metadata_limit and current.records != 0) {
                var result = try encoder.finish();
                result.filter.deinit(alloc);
                blocks += encoder.blocks.items.len;
                const footer = try table.decodeFooterBytes(sink_impl.out.items[sink_impl.out.items.len - table.footer_len ..]);
                try std.testing.expect(footer.metadata_len <= current.metadata_bytes + capacity.fixed_file_bytes);
                encoder.deinit();
                sink_impl.out.clearRetainingCapacity();
                encoder = try table.StreamingEncoder.init(alloc, &sink, 1, options);
                current = .{};
                outputs += 1;
                projected = try encoder.encodedSizeUpperBoundAfterEntry(entry);
                data = sink.len() + encoder.block_bytes.items.len + encodedEntryLen(entry) + table.footer_len;
            }
            current = try current.plus(cost);
            try std.testing.expect(projected - data <= current.metadata_bytes + capacity.fixed_file_bytes);
            try std.testing.expect(projected - data <= metadata_limit);
            try encoder.appendEntry(entry);
        }
        var result = try encoder.finish();
        result.filter.deinit(alloc);
        blocks += encoder.blocks.items.len;
        const proof = try capacity.certify(total, .{ .metadata_bytes = metadata_limit });
        try std.testing.expect(outputs <= proof.outputs);
        try std.testing.expect(blocks <= proof.blocks);
        const footer = try table.decodeFooterBytes(sink_impl.out.items[sink_impl.out.items.len - table.footer_len ..]);
        try std.testing.expect(footer.metadata_len <= current.metadata_bytes + capacity.fixed_file_bytes);
    }
}

test "workload admission completion maintenance streams bounded blocks with exhausted heap and descriptors" {
    const alloc = std.testing.allocator;
    const resources = @import("../resource_manager.zig");
    const pool_mod = @import("completion_pool.zig");
    var backing = std.testing.FailingAllocator.init(alloc, .{});
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    var fd_pool = io_mod.NativeStoragePool.initWithCapacityForTest(backing.allocator(), 8);
    defer fd_pool.deinit();
    var native = try io_mod.NativeStorage.initWithPool(backing.allocator(), .threaded, &fd_pool);
    defer native.deinit();
    var path_buffer: [256]u8 = undefined;
    const root_z = repository.tmpPath(&path_buffer, "completion-maintenance-stream");
    const root = std.mem.span(root_z);
    defer repository.cleanupTmp(root_z);
    var paths: [18][]u8 = undefined;
    var count: usize = 0;
    defer for (paths[0..count]) |path| alloc.free(path);
    for (&paths, 0..) |*path, i| {
        path.* = try repository.runPath(alloc, root, 1 + i);
        count += 1;
    }
    for (0..2) |source| {
        var writer: repository.StreamingRunFileWriter = undefined;
        try writer.initInPlace(native.storage(), alloc, root, source + 1, 2000, 8 * 1024 * 1024, .{}, .snappy_adaptive, .none, null, .normal);
        defer writer.deinit();
        for (0..2000) |i| {
            var key: [8]u8 = undefined;
            std.mem.writeInt(u64, &key, i, .big);
            try writer.appendEntry(.{ .namespace_name = if (i < 1000) "docs" else "other\x00namespace", .key = &key, .value = if (source == 0) "newer" else "old", .tombstone = source == 0 and i == 1 });
        }
        var result = try writer.finish();
        result.filter.deinit(alloc);
        alloc.free(result.path);
    }
    var mutable: state.State = .{};
    defer mutable.deinit(alloc);
    const first = [_]u8{0} ** 8;
    try mutable.upsert(alloc, .{ .name = "docs" }, &first, "new", false);
    const scratch = try domains.RecyclingScratch.create(backing.allocator(), &manager, 32 * 1024 * 1024);
    defer scratch.destroy() catch unreachable;
    var specs: [18]io_mod.NativeCompletionIo.FileSpec = undefined;
    for (paths, &specs) |path, *spec| spec.* = .{ .path = path, .max_bytes = 8 * 1024 * 1024, .allow_delete = true };
    const scope = try io_mod.NativeCompletionIo.createWithFiles(backing.allocator(), &native, root, &specs);
    defer scope.deinit() catch unreachable;
    scope.allow_sequential_input = true;
    const limits: Limits = .{ .max_outputs = 16, .max_output_metadata_bytes = 8192 };
    try qualify(scratch.allocator(), scope.storage(), paths[0..2], limits);
    backing.fail_index = backing.alloc_index;
    backing.resize_fail_index = backing.resize_index;
    fd_pool.fd_cache.capacity = 1;
    manager.memory.budget.hard_limit_bytes = 1;
    var output = try build(scratch.allocator(), scope.storage(), root, paths[0..2], &mutable, 3, limits);
    defer {
        for (output.items) |*run| run.deinit(scratch.allocator());
        output.deinit(scratch.allocator());
    }
    try std.testing.expect(output.items.len > 1 and output.items.len <= 16);
    var total: usize = 0;
    var found = false;
    for (output.items) |run| {
        total += run.entry_count;
        var cursor = try Cursor.init(scratch.allocator(), scope.storage(), run.path.?, limits);
        defer cursor.deinit();
        var seen: usize = 0;
        while (try cursor.current()) |entry| {
            const number = std.mem.readInt(u64, entry.key[0..8], .big);
            try std.testing.expectEqual(number == 1, entry.tombstone);
            if (!entry.tombstone) try std.testing.expectEqualStrings(if (number == 0) "new" else "newer", entry.value);
            try std.testing.expectEqualStrings(if (number < 1000) "docs" else "other\x00namespace", entry.namespace_name.?);
            seen += 1;
            try cursor.advance();
        }
        try std.testing.expectEqual(run.entry_count, seen);
        const point = try pool_mod.readRunPoint(scope.storage(), scratch.allocator(), run.path.?, .{ .max_metadata_bytes = limits.max_output_metadata_bytes }, "docs", &first);
        defer point.deinit(scratch.allocator());
        if (point.value) |value| {
            try std.testing.expectEqualStrings("new", value);
            found = true;
        }
    }
    try std.testing.expectEqual(@as(usize, 2000), total);
    try std.testing.expect(found);
    try std.testing.expect(!backing.has_induced_failure);
    try std.testing.expectError(error.UnsupportedCompletionProfile, qualify(scratch.allocator(), scope.storage(), paths[0..2], .{ .max_frontier_bytes = 1 }));
    try std.testing.expectError(error.UnsupportedCompletionProfile, qualify(scratch.allocator(), scope.storage(), paths[0..2], .{ .max_record_bytes = 1 }));
    var insufficient_outputs = limits;
    insufficient_outputs.max_outputs = 1;
    try std.testing.expectError(error.UnsupportedCompletionProfile, build(scratch.allocator(), scope.storage(), root, paths[0..2], &mutable, 18, insufficient_outputs));
    try scope.storage().deleteFileAbsolute(paths[17]);
    // A failed bounded build leaves immutable inputs valid and usable.
    try qualify(scratch.allocator(), scope.storage(), paths[0..2], limits);
}
