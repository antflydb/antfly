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
        @sizeOf(table.SequentialTableIndex.Block) != 32 or repository.maxRunFileReadBytes() != (capacity.Limits{}).file_bytes)
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
    max_output_file_bytes: usize = repository.maxRunFileReadBytes(),
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
    first: []u8,
    second: []u8,
    loaded: ?[]u8 = null,
    entry: ?table.Entry = null,
    frontier_bytes: usize,

    fn init(allocator: Allocator, io: io_mod.Storage, path: []const u8, limits: Limits, metadata_workspace: []u8) !Cursor {
        const size = try io.fileSize(path);
        if (size < table.header_len + table.footer_len) return error.InvalidTableFile;
        var footer_bytes: [table.footer_len]u8 = undefined;
        try io.readFileRangeInto(allocator, path, size - table.footer_len, &footer_bytes);
        const footer = try table.decodeFooterBytes(&footer_bytes);
        if (footer.metadata_len > limits.max_metadata_bytes or footer.metadata_offset > size - table.footer_len or
            footer.metadata_len != size - table.footer_len - footer.metadata_offset) return error.UnsupportedCompletionProfile;
        if (footer.metadata_len > metadata_workspace.len) return error.UnsupportedCompletionProfile;
        const metadata = metadata_workspace[0..footer.metadata_len];
        try io.readFileRangeInto(allocator, path, footer.metadata_offset, metadata);
        var index = try table.decodeSequentialIndexFromFooterAlloc(allocator, footer, metadata);
        errdefer index.deinit(allocator);
        var largest: usize = 0;
        for (index.blocks) |block| {
            if (block.window.len > limits.max_block_bytes or block.window.physicalLen() > limits.max_block_bytes)
                return error.UnsupportedCompletionProfile;
            largest = @max(largest, @max(block.window.len, block.window.physicalLen()));
        }
        const first = try allocator.alloc(u8, largest);
        errdefer allocator.free(first);
        const second = try allocator.alloc(u8, largest);
        return .{ .allocator = allocator, .io = io, .path = path, .index = index, .limits = limits, .first = first, .second = second, .frontier_bytes = index.blocks.len *| @sizeOf(table.SequentialTableIndex.Block) +| (2 *| largest) };
    }
    fn deinit(self: *Cursor) void {
        self.allocator.free(self.first);
        self.allocator.free(self.second);
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
            const physical = self.first[0..window.physicalLen()];
            try self.io.readFileRangeInto(self.allocator, self.path, self.index.entry_data_start + window.physicalRelativeOffset(), physical);
            self.loaded = try table.decodeBlockPayloadInto(window.compression, self.first, physical.len, self.second, window.len, window.checksum);
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
    const metadata = try allocator.alloc(u8, limits.max_metadata_bytes);
    defer allocator.free(metadata);
    var frontier: usize = 0;
    var cost: capacity.Cost = .{};
    for (paths) |path| {
        var cursor = try Cursor.init(allocator, io, path, limits, metadata);
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

pub const compiler_workspace_bytes = 32 * 1024 * 1024;
pub const Workspace = struct {
    writer: usize,
    compression: usize,
    key: usize,
    record: usize,
    outputs: usize,
    total: usize,
};

/// Monotonic outer allocations plus one resettable writer arena. Input buffers
/// remain live throughout; output metadata only accumulates. No allocation size
/// depends on prior streaming history or on total logical database bytes.
pub fn workspaceRequirement(cost: capacity.Cost, frontier: u64, output_count: u64, limits: Limits) !Workspace {
    const record = std.math.cast(usize, @max(cost.max_record_bytes, 1)) orelse return error.UnsupportedCompletionProfile;
    const key = std.math.cast(usize, cost.max_key_bytes) orelse return error.UnsupportedCompletionProfile;
    const outputs = std.math.cast(usize, output_count) orelse return error.UnsupportedCompletionProfile;
    if (outputs > limits.max_outputs or record > limits.max_record_bytes or key > record) return error.UnsupportedCompletionProfile;
    const encoder = try table.boundedEncoderWorkspace(.{ .metadata_bytes = @min(limits.max_metadata_bytes, limits.max_output_metadata_bytes), .record_bytes = record, .key_bytes = key });
    var writer = try std.math.add(usize, encoder.persistent_bytes, try repository.streamingWriterWorkspaceBytes(512));
    writer = try std.math.add(usize, writer, try std.math.mul(usize, key, 2));
    const footprint = domains.RecyclingScratch.allocationFootprint;
    var total = std.math.cast(usize, frontier) orelse return error.UnsupportedCompletionProfile;
    // Each cursor owns one compact index and two fixed block allocations.
    total = try std.math.add(usize, total, try std.math.mul(usize, 3 * 68, try footprint(0, 8)));
    total = try std.math.add(usize, total, try footprint(limits.max_metadata_bytes, 1));
    total = try std.math.add(usize, total, try footprint(writer, 1));
    total = try std.math.add(usize, total, try footprint(encoder.compression_bytes, 1));
    total = try std.math.add(usize, total, try footprint(try std.math.mul(usize, outputs, @sizeOf(repository.Run)), @alignOf(repository.Run)));
    const bounds = try std.math.mul(usize, 2, try std.math.add(usize, try footprint(key, 1), try footprint(0, 1)));
    const per_output = try std.math.add(usize, bounds, try footprint(512 + 32, 1));
    total = try std.math.add(usize, total, try std.math.mul(usize, outputs, per_output));
    if (total > compiler_workspace_bytes) return error.UnsupportedCompletionProfile;
    return .{ .writer = writer, .compression = encoder.compression_bytes, .key = key, .record = record, .outputs = outputs, .total = total };
}

const Output = struct {
    allocator: Allocator,
    fixed: std.heap.FixedBufferAllocator,
    first_bound: []u8,
    last_bound: []u8,
    writer: repository.StreamingRunFileWriter = undefined,
    run: repository.Run,
    fn init(self: *Output, allocator: Allocator, io: io_mod.Storage, root: []const u8, id: u64, limits: Limits, shape: Workspace, writer_buffer: []u8, compression: []u8) !void {
        self.* = .{ .allocator = allocator, .fixed = std.heap.FixedBufferAllocator.init(writer_buffer), .first_bound = undefined, .last_bound = undefined, .run = .{ .id = id, .level = 1, .size_bytes = 0, .path = null, .smallest_namespace_name = null, .smallest_key = &.{}, .largest_namespace_name = null, .largest_key = &.{}, .entry_count = 0, .bloom_filter = null, .state = null, .tombstone_count = 0 } };
        const fixed = self.fixed.allocator();
        self.first_bound = try fixed.alloc(u8, shape.key);
        self.last_bound = try fixed.alloc(u8, shape.key);
        try self.writer.initInPlace(io, fixed, root, id, 1, @min(limits.max_output_file_bytes, repository.maxRunFileReadBytes()), .{ .bits_per_key = 1, .min_bits = 64, .max_hash_count = 1 }, .snappy_adaptive, .none, null, .cold_sequential);
        errdefer self.writer.deinit();
        try self.writer.encoder.configureBoundedWorkspace(.{ .metadata_bytes = @min(limits.max_metadata_bytes, limits.max_output_metadata_bytes), .record_bytes = shape.record, .key_bytes = shape.key }, compression);
    }
    fn deinit(self: *Output) void {
        self.writer.deinit();
        // run's temporary bounds borrow the two fixed arrays. Only finish's
        // separately owned clone may escape this writer arena.
        self.* = undefined;
    }
    fn fits(self: *Output, entry: table.Entry, limits: Limits) bool {
        const total = self.writer.encoder.encodedSizeUpperBoundAfterEntry(entry) catch return false;
        const data = self.writer.sink.len() +| self.writer.encoder.block_bytes.items.len +| encodedEntryLen(entry) +| table.footer_len;
        return total >= data and total - data <= @min(limits.max_metadata_bytes, limits.max_output_metadata_bytes) and self.writer.canAppendEntry(entry);
    }
    fn append(self: *Output, entry: table.Entry) !void {
        const ns = entry.namespace_name orelse "";
        if (ns.len > self.last_bound.len or entry.key.len > self.last_bound.len - ns.len) return error.UnsupportedCompletionProfile;
        if (self.run.entry_count == 0) {
            @memcpy(self.first_bound[0..ns.len], ns);
            @memcpy(self.first_bound[ns.len..][0..entry.key.len], entry.key);
            self.run.smallest_namespace_name = if (entry.namespace_name != null) self.first_bound[0..ns.len] else null;
            self.run.smallest_key = self.first_bound[ns.len..][0..entry.key.len];
        }
        try self.writer.appendEntry(entry);
        @memcpy(self.last_bound[0..ns.len], ns);
        @memcpy(self.last_bound[ns.len..][0..entry.key.len], entry.key);
        self.run.largest_namespace_name = if (entry.namespace_name != null) self.last_bound[0..ns.len] else null;
        self.run.largest_key = self.last_bound[ns.len..][0..entry.key.len];
        self.run.entry_count += 1;
        if (entry.tombstone) self.run.tombstone_count.? += 1;
    }
    fn finish(self: *Output) !repository.Run {
        var file = try self.writer.finish();
        defer file.filter.deinit(self.fixed.allocator());
        defer self.fixed.allocator().free(file.path);
        self.run.path = file.path;
        self.run.size_bytes = file.size_bytes;
        self.run.compression_stats = file.compression_stats;
        return repository.cloneRunCompactionSnapshot(self.allocator, self.run);
    }
};

/// Inputs are newest first. The mutable snapshot wins over every run. Outputs
/// and all temporary blocks use the caller's preowned recycling domain; no
/// ordinary allocator, FD admission, or unbounded whole-store materialization.
/// The caller owns cleanup of written paths after any failure.
pub fn build(allocator: Allocator, io: io_mod.Storage, root: []const u8, paths: []const []const u8, mutable: *const state.State, output_base: u64, limits: Limits) !std.ArrayListUnmanaged(repository.Run) {
    return buildInternal(allocator, io, root, paths, mutable, output_base, limits, false);
}

/// Installed pools require the same format-cost output proof checked before
/// acceptance. Generic build callers instead preown their full output limit;
/// those callers receive the ordinary bounded-output error if it is exhausted.
pub fn buildCertified(allocator: Allocator, io: io_mod.Storage, root: []const u8, paths: []const []const u8, mutable: *const state.State, output_base: u64, limits: Limits) !std.ArrayListUnmanaged(repository.Run) {
    return buildInternal(allocator, io, root, paths, mutable, output_base, limits, true);
}

fn buildInternal(allocator: Allocator, io: io_mod.Storage, root: []const u8, paths: []const []const u8, mutable: *const state.State, output_base: u64, limits: Limits, certified: bool) !std.ArrayListUnmanaged(repository.Run) {
    if (!domains.isPrepaid(allocator) or root.len > 512 or paths.len > 68 or paths.len > limits.max_inputs or limits.max_outputs > 64)
        return error.UnsupportedCompletionProfile;
    const metadata = try allocator.alloc(u8, limits.max_metadata_bytes);
    defer allocator.free(metadata);
    var cursors: [68]Cursor = undefined;
    var cursor_count: usize = 0;
    defer for (cursors[0..cursor_count]) |*cursor| cursor.deinit();
    var frontier: usize = 0;
    for (paths) |path| {
        cursors[cursor_count] = try Cursor.init(allocator, io, path, limits, metadata);
        frontier +|= cursors[cursor_count].frontier_bytes;
        cursor_count += 1;
        if (frontier > limits.max_frontier_bytes) return error.UnsupportedCompletionProfile;
    }
    // Scan once before any output I/O, retaining the same fixed cursor buffers.
    // The second pass merges; neither pass allocates per block or record.
    var cost: capacity.Cost = .{};
    for (cursors[0..cursor_count]) |*cursor| {
        while (try cursor.current()) |entry| {
            cost = try cost.plus(try capacity.Cost.record(if (entry.namespace_name) |ns| ns.len else 0, entry.key.len, entry.value.len));
            try cursor.advance();
        }
        cursor.block = 0;
        cursor.entry_in_block = 0;
        cursor.advanced_entries = 0;
        cursor.offset = 0;
        cursor.loaded = null;
        cursor.entry = null;
    }
    for (0..mutable.entryCount()) |i| {
        const entry = mutable.entryAt(i);
        if (encodedEntryLen(stateEntry(entry)) > limits.max_record_bytes) return error.UnsupportedCompletionProfile;
        cost = try cost.plus(try capacity.Cost.record(if (entry.namespace_name) |ns| ns.len else 0, entry.key.len, entry.value.len));
    }
    const output_count = if (certified) (try capacity.certify(cost, .{
        .metadata_bytes = @min(limits.max_metadata_bytes, limits.max_output_metadata_bytes),
        .file_bytes = @min(limits.max_output_file_bytes, repository.maxRunFileReadBytes()),
        .outputs = limits.max_outputs,
    })).outputs else limits.max_outputs;
    const shape = try workspaceRequirement(cost, frontier, output_count, limits);
    const writer_buffer = try allocator.alloc(u8, shape.writer);
    defer allocator.free(writer_buffer);
    const compression = try allocator.alloc(u8, shape.compression);
    defer allocator.free(compression);
    var results: std.ArrayListUnmanaged(repository.Run) = .empty;
    errdefer {
        for (results.items) |*run| run.deinit(allocator);
        results.deinit(allocator);
    }
    try results.ensureTotalCapacityPrecise(allocator, shape.outputs);
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
            if (results.items.len == shape.outputs) return error.UnsupportedCompletionProfile;
            try output.init(allocator, io, root, try std.math.add(u64, output_base, results.items.len), limits, shape, writer_buffer, compression);
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
    const metadata_workspace = try scratch.allocator().alloc(u8, limits.max_metadata_bytes);
    defer scratch.allocator().free(metadata_workspace);
    for (output.items) |run| {
        total += run.entry_count;
        var cursor = try Cursor.init(scratch.allocator(), scope.storage(), run.path.?, limits, metadata_workspace);
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
    // A smaller allowance exercises the same physical split used for the
    // native 512MiB cap without constructing a half-gigabyte fixture.
    var physical = try build(scratch.allocator(), scope.storage(), root, paths[0..2], &mutable, 3, .{
        .max_outputs = 16,
        .max_output_file_bytes = 8192,
    });
    defer {
        for (physical.items) |*run| run.deinit(scratch.allocator());
        physical.deinit(scratch.allocator());
    }
    try std.testing.expect(physical.items.len > 1);
    for (physical.items) |run| try std.testing.expect(run.size_bytes <= 8192);
}

test "workload admission completion single drain certificate matches actual oversized-key SST metadata" {
    const alloc = std.testing.allocator;
    const metadata_limit = 1024 * 1024;
    var sink_impl = table.MemoryTableSink.init(alloc);
    defer sink_impl.deinit();
    var sink = sink_impl.sink();
    var encoder = try table.StreamingEncoder.init(alloc, &sink, 1, .{ .bloom_config = .{ .bits_per_key = 1, .min_bits = 64, .max_hash_count = 1 }, .block_compression = .snappy_adaptive, .prefix_extractor = .none });
    defer encoder.deinit();
    var key: [40000]u8 = @splat(0x91);
    var cost: capacity.Cost = .{};
    for (0..12) |i| {
        std.mem.writeInt(u64, key[0..8], i, .big);
        const entry: table.Entry = .{ .namespace_name = null, .key = &key, .value = "v" };
        cost = try cost.plus(try capacity.Cost.record(0, key.len, 1));
        try capacity.certifySingleDrain(cost, metadata_limit);
        try encoder.appendEntry(entry);
    }
    var result = try encoder.finish();
    defer result.filter.deinit(alloc);
    const footer = try table.decodeFooterBytes(sink_impl.out.items[sink_impl.out.items.len - table.footer_len ..]);
    try std.testing.expect(footer.metadata_len <= cost.metadata_bytes + capacity.fixed_file_bytes);
    try std.testing.expect(footer.metadata_len <= metadata_limit);
    const grown = try cost.plus(try (try capacity.Cost.record(0, key.len, 1)).repeated(6));
    try std.testing.expectError(error.UnsupportedCompletionProfile, capacity.certifySingleDrain(grown, metadata_limit));
}

test "workload admission completion aggregate workspace rejects a sum exceeding individually bounded frontiers" {
    const cost = try (try capacity.Cost.record(4, 64, 1024)).repeated(1000);
    const proof = try capacity.certify(cost, .{});
    const normal = try workspaceRequirement(cost, 16 * 1024 * 1024, proof.outputs, .{});
    try std.testing.expect(normal.total <= compiler_workspace_bytes);
    // Each larger metadata buffer is individually bounded and the cursor
    // frontier still fits; their aggregate writer arrays do not fit 32 MiB.
    try std.testing.expectError(error.UnsupportedCompletionProfile, workspaceRequirement(cost, 16 * 1024 * 1024, proof.outputs, .{
        .max_metadata_bytes = 4 * 1024 * 1024,
        .max_output_metadata_bytes = 4 * 1024 * 1024,
    }));
}

test "workload admission completion aggregate workspace builds split binary runs with exactly certified backing" {
    const alloc = std.testing.allocator;
    const resources = @import("../resource_manager.zig");
    var failing = std.testing.FailingAllocator.init(alloc, .{});
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    var fd_pool = io_mod.NativeStoragePool.initWithCapacityForTest(alloc, 16);
    defer fd_pool.deinit();
    var native = try io_mod.NativeStorage.initWithPool(alloc, .threaded, &fd_pool);
    defer native.deinit();
    var root_buffer: [256]u8 = undefined;
    const root_z = repository.tmpPath(&root_buffer, "completion-exact-workspace");
    defer repository.cleanupTmp(root_z);
    const root = std.mem.span(root_z);
    var paths: [3][]u8 = undefined;
    var path_count: usize = 0;
    defer for (paths[0..path_count]) |path| alloc.free(path);
    for (&paths, 0..) |*path, i| {
        path.* = try repository.runPath(alloc, root, i + 1);
        path_count += 1;
    }
    {
        var writer: repository.StreamingRunFileWriter = undefined;
        try writer.initInPlace(native.storage(), alloc, root, 1, 8, 8 * 1024 * 1024, .{}, .snappy_adaptive, .none, null, .normal);
        defer writer.deinit();
        var key: [40000]u8 = @splat(0x91);
        for (0..8) |i| {
            std.mem.writeInt(u64, key[0..8], i, .big);
            try writer.appendEntry(.{ .namespace_name = "ns\x00\xff", .key = &key, .value = "value" });
        }
        var result = try writer.finish();
        result.filter.deinit(alloc);
        alloc.free(result.path);
    }
    var specs: [3]io_mod.NativeCompletionIo.FileSpec = undefined;
    for (paths, &specs) |path, *spec| spec.* = .{ .path = path, .max_bytes = 8 * 1024 * 1024, .allow_delete = true };
    const scope = try io_mod.NativeCompletionIo.createWithFiles(alloc, &native, root, &specs);
    defer scope.deinit() catch unreachable;
    scope.allow_sequential_input = true;
    const limits: Limits = .{ .max_outputs = 2, .max_output_metadata_bytes = 512 * 1024 };
    const measured = try measure(alloc, scope.storage(), paths[0..1], limits);
    const proof = try capacity.certify(measured.cost, .{ .metadata_bytes = limits.max_output_metadata_bytes, .outputs = limits.max_outputs });
    const shape = try workspaceRequirement(measured.cost, measured.frontier_bytes, proof.outputs, limits);
    const scratch = try domains.RecyclingScratch.create(failing.allocator(), &manager, shape.total);
    defer scratch.destroy() catch unreachable;
    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    manager.memory.budget.hard_limit_bytes = 1;
    fd_pool.fd_cache.capacity = 1;
    const mutable: state.State = .{};
    var output = try buildCertified(scratch.allocator(), scope.storage(), root, paths[0..1], &mutable, 2, limits);
    defer {
        for (output.items) |*run| run.deinit(scratch.allocator());
        output.deinit(scratch.allocator());
    }
    try std.testing.expectEqual(@as(usize, 2), output.items.len);
    try std.testing.expectEqual(@as(usize, 8), output.items[0].entry_count + output.items[1].entry_count);
    try std.testing.expect(std.mem.order(u8, output.items[0].largest_key, output.items[1].smallest_key) == .lt);
    try std.testing.expectEqualStrings("ns\x00\xff", output.items[0].smallest_namespace_name.?);
    try std.testing.expectEqual(@as(usize, 40000), output.items[1].largest_key.len);
    try std.testing.expect(!failing.has_induced_failure);
}
