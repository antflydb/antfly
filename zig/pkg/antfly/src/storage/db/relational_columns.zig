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

//! Table-owned, disposable column segments. A ready manifest is a snapshot
//! fence: every primary/schema mutation deletes it in that same transaction.
//! Builders stream into unpublished generations; readers never mix generations
//! or combine an uncovered segment with a newer primary snapshot.
const std = @import("std");
const store_mod = @import("../docstore.zig");
const keys = @import("../internal_keys.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const schema = @import("../schema.zig");
const registry = @import("schema_registry.zig");
const dv = @import("../../section/typed_doc_values.zig");
const graph = @import("query/graph_exec.zig");
const types = @import("types.zig");
const platform_time = @import("antfly_platform").time;
const alloc_type = std.mem.Allocator;
const prefix = "\x00\x00__columnar__:blocks:";
const counter_key = "\x00\x00__columnar__:next";
const manifest_key = keys.relational_columnar_manifest_key;
pub var test_before_publish: ?struct { context: *anyopaque, run: *const fn (*anyopaque) anyerror!void } = null;
const max_rows = 256;
const null_bytes = max_rows / 8;
const Manifest = struct {
    ready: bool,
    generation: u64,
    sequence: u64,
    blocks: u64,

    fn encode(self: @This()) [33]u8 {
        var out: [33]u8 = undefined;
        @memcpy(out[0..4], "ACL1");
        out[4] = @intFromBool(self.ready);
        std.mem.writeInt(u64, out[5..13], self.generation, .little);
        std.mem.writeInt(u64, out[13..21], self.sequence, .little);
        std.mem.writeInt(u64, out[21..29], self.blocks, .little);
        std.mem.writeInt(u32, out[29..33], std.hash.Crc32.hash(out[0..29]), .little);
        return out;
    }

    fn decode(encoded: []const u8) !@This() {
        const bytes = try verified(encoded);
        if (bytes.len != 29 or !std.mem.eql(u8, bytes[0..4], "ACL1") or bytes[4] > 1) return error.InvalidColumnSegment;
        return .{ .ready = bytes[4] == 1, .generation = std.mem.readInt(u64, bytes[5..13], .little), .sequence = std.mem.readInt(u64, bytes[13..21], .little), .blocks = std.mem.readInt(u64, bytes[21..29], .little) };
    }
};

fn checked(alloc: alloc_type, bytes: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, bytes.len + 4);
    @memcpy(out[0..bytes.len], bytes);
    std.mem.writeInt(u32, out[bytes.len..][0..4], std.hash.Crc32.hash(bytes), .little);
    return out;
}

fn verified(bytes: []const u8) ![]const u8 {
    if (bytes.len < 4) return error.InvalidColumnSegment;
    const body = bytes[0 .. bytes.len - 4];
    if (std.hash.Crc32.hash(body) != std.mem.readInt(u32, bytes[bytes.len - 4 ..][0..4], .little)) return error.InvalidColumnSegment;
    return body;
}

fn blockKey(alloc: alloc_type, generation: u64, block: u64, ordinal: ?u32) ![]u8 {
    return if (ordinal) |column|
        try std.fmt.allocPrint(alloc, "{s}{x:0>16}:{x:0>16}:c{x:0>8}", .{ prefix, generation, block, column })
    else
        try std.fmt.allocPrint(alloc, "{s}{x:0>16}:{x:0>16}:m", .{ prefix, generation, block });
}

fn appendInt(list: *std.ArrayListUnmanaged(u8), alloc: alloc_type, comptime T: type, value: T) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try list.appendSlice(alloc, &bytes);
}

const Row = struct { key: []const u8, hash: [32]u8, timestamp: u64 };
const Bounds = struct { present: bool = false, minimum: f64 = 0, maximum: f64 = 0 };
const Column = struct { writer: dv.TypedDocValuesWriter, nulls: [null_bytes]u8 = @splat(0), bounds: Bounds = .{} };

/// Uses one pinned primary snapshot and at most one block of preparation
/// memory. A racing mutation invalidates the pending fence, so publication is
/// rejected instead of exposing a partially covered generation.
pub fn rebuild(db: anytype, alloc: alloc_type, force: bool) !bool {
    db.core.lockApplyShared();
    var initial_locked = true;
    defer if (initial_locked) db.core.unlockApplyShared();
    const namespace = db.core.schemaNamespaceGeneration();
    var start = try db.core.store.beginWriteTxn();
    var start_live = true;
    defer if (start_live) start.abort();
    const current = start.get(manifest_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (current) |bytes| {
        const manifest = Manifest.decode(bytes) catch Manifest{ .ready = false, .generation = 0, .sequence = 0, .blocks = 0 };
        if (manifest.ready and !force) {
            start.abort();
            start_live = false;
            db.core.unlockApplyShared();
            initial_locked = false;
            try prune(db, alloc, manifest.generation, namespace);
            return false;
        }
    }
    const old = start.get(counter_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    const previous = if (old) |bytes| blk: {
        if (bytes.len != 8) return error.InvalidColumnSegment;
        break :blk std.mem.readInt(u64, bytes[0..8], .little);
    } else 0;
    const generation = std.math.add(u64, previous, 1) catch return error.InvalidColumnSegment;
    var counter: [8]u8 = undefined;
    std.mem.writeInt(u64, &counter, generation, .little);
    const pending = (Manifest{ .ready = false, .generation = generation, .sequence = 0, .blocks = 0 }).encode();
    try start.put(counter_key, &counter);
    try start.put(manifest_key, &pending);
    try start.commit();
    start_live = false;

    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    db.core.unlockApplyShared();
    initial_locked = false;
    try prune(db, alloc, generation, namespace);
    const sequence = try db.core.store.lastReplaySequenceFromTxn(&read, 0);
    const Builder = struct {
        db: @TypeOf(db),
        alloc: alloc_type,
        arena: std.heap.ArenaAllocator,
        generation: u64,
        namespace: u64,
        blocks: u64 = 0,
        bytes: usize = 0,
        view: ?registry.SchemaView = null,
        rows: std.ArrayListUnmanaged(Row) = .empty,
        columns: std.AutoHashMapUnmanaged(u32, Column) = .empty,

        fn flush(self: *@This()) !void {
            if (self.rows.items.len == 0) return;
            const scratch = self.arena.allocator();
            var meta = std.ArrayListUnmanaged(u8).empty;
            try meta.appendSlice(scratch, "ACB1");
            try appendInt(&meta, scratch, u32, self.view.?.version());
            try appendInt(&meta, scratch, u32, @intCast(self.rows.items.len));
            try appendInt(&meta, scratch, u32, self.columns.count());
            var writes = std.ArrayListUnmanaged(store_mod.KVPair).empty;
            var columns = self.columns.iterator();
            while (columns.next()) |entry| {
                const ordinal = entry.key_ptr.*;
                try appendInt(&meta, scratch, u32, ordinal);
                const bounds = entry.value_ptr.bounds;
                try meta.append(scratch, @intFromBool(bounds.present));
                try appendInt(&meta, scratch, u64, @bitCast(bounds.minimum));
                try appendInt(&meta, scratch, u64, @bitCast(bounds.maximum));
                const values = try entry.value_ptr.writer.build();
                const value = try std.mem.concat(scratch, u8, &.{ &entry.value_ptr.nulls, values });
                try writes.append(scratch, .{ .key = try blockKey(scratch, self.generation, self.blocks, ordinal), .value = try checked(scratch, value) });
            }
            for (self.rows.items) |row| {
                try appendInt(&meta, scratch, u32, std.math.cast(u32, row.key.len) orelse return error.InvalidColumnSegment);
                try meta.appendSlice(scratch, row.key);
                try meta.appendSlice(scratch, &row.hash);
                try appendInt(&meta, scratch, u64, row.timestamp);
            }
            try writes.append(scratch, .{ .key = try blockKey(scratch, self.generation, self.blocks, null), .value = try checked(scratch, meta.items) });
            // Sparse row-key directory keeps resumed/bounded scans logarithmic
            // instead of walking every earlier block's metadata.
            const directory_key = try std.fmt.allocPrint(scratch, "{s}{x:0>16}:r:{s}", .{ prefix, self.generation, self.rows.items[0].key });
            var block_index: [8]u8 = undefined;
            std.mem.writeInt(u64, &block_index, self.blocks, .little);
            try writes.append(scratch, .{ .key = directory_key, .value = try checked(scratch, &block_index) });
            {
                self.db.core.lockApplyShared();
                defer self.db.core.unlockApplyShared();
                if (self.namespace != self.db.core.schemaNamespaceGeneration()) return error.PreparedGenerationChanged;
                const fence = self.db.core.store.get(scratch, manifest_key) catch |err| switch (err) {
                    error.NotFound => return error.PreparedGenerationChanged,
                    else => return err,
                };
                const expected = (Manifest{ .ready = false, .generation = self.generation, .sequence = 0, .blocks = 0 }).encode();
                if (!std.mem.eql(u8, fence, &expected)) return error.PreparedGenerationChanged;
                try self.db.core.store.putBatch(writes.items, &.{});
            }
            self.blocks += 1;
            self.rows = .empty;
            self.columns = .empty;
            self.bytes = 0;
            _ = self.arena.reset(.free_all);
        }

        fn visit(ptr: ?*anyopaque, key: []const u8, value: []const u8) !store_mod.DocStore.ScanAction {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            if (self.db.artifact_repair_metadata_stop.load(.acquire)) return error.Canceled;
            if (!keys.isRelationalRowKey(key)) return .@"continue";
            const version = try codec.rowSchemaVersion(value);
            if (self.view == null or self.view.?.version() != version) {
                try self.flush();
                if (self.view) |*view| view.release();
                self.view = null;
                self.view = (try self.db.core.acquireSchemaVersionView(version)) orelse return error.UnknownSchemaVersion;
            }
            const view = self.view.?;
            const row = try codec.ordinalRowView(value, view.tableSchema().*, view.physicalLayout());
            const scratch = self.arena.allocator();
            const id: u32 = @intCast(self.rows.items.len);
            try self.rows.append(scratch, .{ .key = (try keys.decodeStoredDocumentRowKeyAlloc(scratch, key)) orelse return error.InvalidColumnSegment, .hash = row.semanticHash(), .timestamp = row.writeTimestampNs() });
            var cells = codec.OrdinalCellIterator{ .parsed = row.parsed, .table_schema = row.table_schema };
            while (try cells.next()) |cell| {
                const entry = try self.columns.getOrPut(scratch, cell.ordinal);
                if (!entry.found_existing) entry.value_ptr.* = .{ .writer = dv.TypedDocValuesWriter.init(scratch, cell.value_type, max_rows) };
                if (cell.is_null) entry.value_ptr.nulls[id / 8] |= @as(u8, 1) << @intCast(id % 8) else try entry.value_ptr.writer.add(id, cell.value);
                if (!cell.is_null) {
                    const number: ?f64 = switch (cell.value) {
                        .u64_val => |n| @floatFromInt(n),
                        .i64_val => |n| @floatFromInt(n),
                        .f64_val => |n| n,
                        else => null,
                    };
                    if (number) |n| {
                        const bounds = &entry.value_ptr.bounds;
                        bounds.minimum = if (bounds.present) @min(bounds.minimum, n) else n;
                        bounds.maximum = if (bounds.present) @max(bounds.maximum, n) else n;
                        bounds.present = true;
                    }
                }
            }
            self.bytes +|= value.len;
            if (self.rows.items.len == max_rows or self.bytes >= 1024 * 1024) try self.flush();
            return .@"continue";
        }
    };
    var builder = Builder{ .db = db, .alloc = alloc, .arena = std.heap.ArenaAllocator.init(alloc), .generation = generation, .namespace = namespace };
    defer builder.arena.deinit();
    defer if (builder.view) |*view| view.release();
    const lower = try db.core.documentRangeLowerAlloc("");
    defer db.core.alloc.free(lower);
    try db.core.store.scanReadTxnWithContext(&read, lower, "", .{}, &builder, Builder.visit);
    try builder.flush();
    if (comptime @import("builtin").is_test) if (test_before_publish) |hook| try hook.run(hook.context);
    if (db.artifact_repair_metadata_stop.load(.acquire)) return error.Canceled;
    db.core.lockApplyShared();
    defer db.core.unlockApplyShared();
    if (namespace != db.core.schemaNamespaceGeneration()) return false;
    var publish = try db.core.store.beginWriteTxn();
    var publish_live = true;
    defer if (publish_live) publish.abort();
    const fence = publish.get(manifest_key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    if (!std.mem.eql(u8, fence, &pending)) return false;
    const ready = (Manifest{ .ready = true, .generation = generation, .sequence = sequence, .blocks = builder.blocks }).encode();
    try publish.put(manifest_key, &ready);
    try publish.commit();
    publish_live = false;
    return true;
}

fn prune(db: anytype, alloc: alloc_type, generation: u64, namespace: u64) !void {
    const upper = try std.fmt.allocPrint(alloc, "{s}{x:0>16}:", .{ prefix, generation });
    defer alloc.free(upper);
    const Pruner = struct {
        db: @TypeOf(db),
        namespace: u64,
        arena: std.heap.ArenaAllocator,
        deletes: std.ArrayListUnmanaged([]const u8) = .empty,
        fn flush(self: *@This()) !void {
            if (self.deletes.items.len == 0) return;
            self.db.core.lockApplyShared();
            defer self.db.core.unlockApplyShared();
            if (self.namespace != self.db.core.schemaNamespaceGeneration()) return error.PreparedGenerationChanged;
            try self.db.core.store.putBatch(&.{}, self.deletes.items);
            self.deletes = .empty;
            _ = self.arena.reset(.free_all);
        }
        fn visit(ptr: ?*anyopaque, key: []const u8, _: []const u8) !store_mod.DocStore.ScanAction {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            if (self.db.artifact_repair_metadata_stop.load(.acquire)) return error.Canceled;
            const scratch = self.arena.allocator();
            try self.deletes.append(scratch, try scratch.dupe(u8, key));
            if (self.deletes.items.len == 256) try self.flush();
            return .@"continue";
        }
    };
    var pruner = Pruner{ .db = db, .arena = std.heap.ArenaAllocator.init(alloc), .namespace = namespace };
    defer pruner.arena.deinit();
    try db.core.store.scanWithContext(prefix, upper, .{}, &pruner, Pruner.visit);
    try pruner.flush();
}

const Decoder = struct {
    bytes: []const u8,
    fn take(self: *@This(), n: usize) ![]const u8 {
        if (n > self.bytes.len) return error.InvalidColumnSegment;
        const result = self.bytes[0..n];
        self.bytes = self.bytes[n..];
        return result;
    }
    fn int(self: *@This(), comptime T: type) !T {
        return std.mem.readInt(T, (try self.take(@sizeOf(T)))[0..@sizeOf(T)], .little);
    }
};

fn flatPath(path: graph.CompiledPatternFilter.FieldPath) ?[]const u8 {
    return switch (path) {
        .single => |name| name,
        .dotted, .json_pointer => |parts| if (parts.len == 1) parts[0] else null,
    };
}

fn supports(filter: graph.CompiledPatternFilter) bool {
    return switch (filter) {
        .match_all, .match_none, .doc_id => true,
        .field_matcher => |matcher| flatPath(matcher.path) != null,
        .conjuncts, .disjuncts => |items| blk: {
            for (items) |item| if (!supports(item)) break :blk false;
            break :blk true;
        },
        .bool_query => |query| blk: {
            for (query.must) |item| if (!supports(item)) break :blk false;
            for (query.should) |item| if (!supports(item)) break :blk false;
            for (query.must_not) |item| if (!supports(item)) break :blk false;
            break :blk true;
        },
    };
}

const Block = struct {
    alloc: alloc_type,
    txn: *store_mod.DocStore.Txn,
    generation: u64,
    index: u64,
    table: schema.TableSchema,
    layout: *const codec.PhysicalLayout,
    rows: []Row,
    ordinals: []u32,
    bounds: []Bounds,
    values: std.AutoHashMapUnmanaged(u32, []?std.json.Value) = .empty,
    stats: ?*types.ColumnarScanStats = null,

    fn column(self: *@This(), name: []const u8) ![]?std.json.Value {
        const ordinal: u32 = @intCast(self.layout.ordinalForName(self.table.relational_columns, name) orelse return &.{});
        if (self.values.get(ordinal)) |values| return values;
        const values = try self.alloc.alloc(?std.json.Value, self.rows.len);
        @memset(values, null);
        if (std.mem.indexOfScalar(u32, self.ordinals, ordinal) != null) {
            const key = try blockKey(self.alloc, self.generation, self.index, ordinal);
            const bytes = try verified(try self.txn.get(key));
            if (self.stats) |stats| {
                stats.columns_read += 1;
                stats.encoded_bytes_read += bytes.len + 4;
            }
            if (bytes.len < null_bytes) return error.InvalidColumnSegment;
            for (values, 0..) |*value, i| if (bytes[i / 8] & (@as(u8, 1) << @intCast(i % 8)) != 0) {
                value.* = .null;
            };
            var reader = try dv.TypedDocValuesReader.init(self.alloc, bytes[null_bytes..]);
            const expected: dv.ValueType = switch (self.table.relational_columns[ordinal].column_type) {
                .datetime => .u64_val,
                .integer => .i64_val,
                .number => .f64_val,
                .boolean => .bool_val,
                .geopoint => .geo_point,
                .string, .blob, .geoshape, .json, .dense_vector => .bytes_val,
            };
            if (reader.value_type != expected) return error.InvalidColumnSegment;
            for (0..reader.num_chunks) |chunk_index| {
                var chunk = try reader.decodeChunk(@intCast(chunk_index));
                // Block arena owns decoded payloads, including borrowed strings.
                var it = chunk.iterator();
                while (try it.next()) |entry| {
                    if (entry.doc_id >= values.len or values[entry.doc_id] != null) return error.InvalidColumnSegment;
                    const col = self.table.relational_columns[ordinal];
                    values[entry.doc_id] = try codec.ownedJsonValueFromCellAlloc(self.alloc, col, .{ .ordinal = ordinal, .path = col.path, .value_type = reader.value_type, .is_json = col.is_json, .is_dense_vector = col.column_type == .dense_vector, .value = entry.value });
                }
            }
        }
        try self.values.put(self.alloc, ordinal, values);
        return values;
    }

    fn evaluate(self: *@This(), filter: graph.CompiledPatternFilter, out: []bool) !void {
        switch (filter) {
            .match_all => @memset(out, true),
            .match_none => @memset(out, false),
            .doc_id => |ids| for (self.rows, out) |row, *matched| {
                matched.* = false;
                for (ids) |id| if (std.mem.eql(u8, id, row.key)) {
                    matched.* = true;
                    break;
                };
            },
            .field_matcher => |matcher| {
                const name = flatPath(matcher.path).?;
                if (self.layout.ordinalForName(self.table.relational_columns, name)) |ordinal|
                    if (std.mem.indexOfScalar(u32, self.ordinals, @intCast(ordinal))) |index| {
                        const bounds = self.bounds[index];
                        if (bounds.present and !try matcher.predicate.mayMatchNumericBounds(bounds.minimum, bounds.maximum)) {
                            @memset(out, false);
                            return;
                        }
                    };
                const values = try self.column(name);
                for (out, 0..) |*matched, i| {
                    const value = if (values.len == 0) null else values[i];
                    matched.* = try matcher.predicate.matches(self.alloc, if (value) |v| &.{v} else &.{});
                }
            },
            .conjuncts, .disjuncts => |items| {
                const conjunction = filter == .conjuncts;
                @memset(out, conjunction);
                var buffer: [max_rows]bool = undefined;
                for (items) |item| {
                    try self.evaluate(item, buffer[0..out.len]);
                    for (out, buffer[0..out.len]) |*value, matched| value.* = if (conjunction) value.* and matched else value.* or matched;
                }
            },
            .bool_query => |query| {
                @memset(out, true);
                var buffer: [max_rows]bool = undefined;
                for (query.must) |item| {
                    try self.evaluate(item, buffer[0..out.len]);
                    for (out, buffer[0..out.len]) |*value, matched| value.* = value.* and matched;
                }
                if (query.min_should > 0) {
                    var counts: [max_rows]usize = @splat(0);
                    for (query.should) |item| {
                        try self.evaluate(item, buffer[0..out.len]);
                        for (buffer[0..out.len], 0..) |matched, i| counts[i] += @intFromBool(matched);
                    }
                    for (out, 0..) |*value, i| value.* = value.* and counts[i] >= query.min_should;
                }
                for (query.must_not) |item| {
                    try self.evaluate(item, buffer[0..out.len]);
                    for (out, buffer[0..out.len]) |*value, matched| value.* = value.* and !matched;
                }
            },
        }
    }
};

pub const Progress = struct {
    delivered: u32 = 0,
    last_key: std.ArrayListUnmanaged(u8) = .empty,
    callback_failed: bool = false,
};

pub fn scan(db: anytype, alloc: alloc_type, txn: *store_mod.DocStore.Txn, from: []const u8, to: []const u8, byte_range: types.ByteRange, opts: types.ScanOptions, visitor: types.ScanVisitor, ttl_ns: u64, now_ns: u64, progress: *Progress, filter: ?*const graph.PreparedPatternFilter) !bool {
    if (opts.include_documents and (opts.include_all_fields or opts.fields.len == 0)) return false;
    for (opts.fields) |field| if (field.len == 0 or field[0] == '_' or std.mem.indexOfAny(u8, field, ".*-") != null) return false;
    if (filter) |value| if (!supports(value.compiled)) return false;
    const raw = txn.get(manifest_key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    const manifest = try Manifest.decode(raw);
    if (!manifest.ready) return false;
    if (opts.columnar_stats) |stats| stats.used = true;
    const directory_prefix = try std.fmt.allocPrint(alloc, "{s}{x:0>16}:r:", .{ prefix, manifest.generation });
    defer alloc.free(directory_prefix);
    const lower_key = if (std.mem.order(u8, from, byte_range.start) == .gt) from else byte_range.start;
    const seek_key = try std.mem.concat(alloc, u8, &.{ directory_prefix, lower_key });
    defer alloc.free(seek_key);
    var cursor = try txn.openCursor();
    defer cursor.close();
    var first_block: usize = 0;
    if (try cursor.seekAtOrBefore(seek_key)) |entry| if (std.mem.startsWith(u8, entry.key, directory_prefix)) {
        const index_bytes = try verified(entry.value);
        if (index_bytes.len != 8) return error.InvalidColumnSegment;
        const index = std.mem.readInt(u64, index_bytes[0..8], .little);
        if (index >= manifest.blocks) return error.InvalidColumnSegment;
        first_block = std.math.cast(usize, index) orelse return error.InvalidColumnSegment;
    };
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var count: usize = 0;
    for (first_block..std.math.cast(usize, manifest.blocks) orelse return error.InvalidColumnSegment) |index| {
        if (opts.cancellation) |token| if (token.isCancelled()) return error.Canceled;
        if (opts.execution_deadline_ns) |deadline| if (platform_time.monotonicNs() >= deadline) return error.DeadlineExceeded;
        const scratch = arena.allocator();
        const key = try blockKey(scratch, manifest.generation, index, null);
        const meta = try verified(try txn.get(key));
        if (opts.columnar_stats) |stats| {
            stats.blocks_read += 1;
            stats.encoded_bytes_read += meta.len + 4;
        }
        var decoder = Decoder{ .bytes = meta };
        if (!std.mem.eql(u8, try decoder.take(4), "ACB1")) return error.InvalidColumnSegment;
        const version = try decoder.int(u32);
        const rows_len = try decoder.int(u32);
        if (rows_len == 0 or rows_len > max_rows) return error.InvalidColumnSegment;
        const columns_len = try decoder.int(u32);
        if (columns_len > decoder.bytes.len / 21) return error.InvalidColumnSegment;
        const ordinals = try scratch.alloc(u32, columns_len);
        const bounds = try scratch.alloc(Bounds, columns_len);
        for (ordinals, bounds) |*ordinal, *bound| {
            ordinal.* = try decoder.int(u32);
            const present = try decoder.int(u8);
            if (present > 1) return error.InvalidColumnSegment;
            bound.* = .{ .present = present == 1, .minimum = @bitCast(try decoder.int(u64)), .maximum = @bitCast(try decoder.int(u64)) };
            if (!std.math.isFinite(bound.minimum) or !std.math.isFinite(bound.maximum) or bound.minimum > bound.maximum) return error.InvalidColumnSegment;
        }
        const rows = try scratch.alloc(Row, rows_len);
        for (rows) |*row| {
            row.key = try decoder.take(try decoder.int(u32));
            @memcpy(&row.hash, try decoder.take(32));
            row.timestamp = try decoder.int(u64);
        }
        if (decoder.bytes.len != 0) return error.InvalidColumnSegment;
        if (to.len != 0 and std.mem.order(u8, rows[0].key, to) == .gt) return true;
        if (byte_range.end.len != 0 and std.mem.order(u8, rows[0].key, byte_range.end) != .lt) return true;
        var view = (try db.core.acquireSchemaVersionView(version)) orelse return error.UnknownSchemaVersion;
        defer view.release();
        for (ordinals) |ordinal| if (ordinal >= view.tableSchema().relational_columns.len) return error.InvalidColumnSegment;
        var block = Block{ .alloc = scratch, .txn = txn, .generation = manifest.generation, .index = index, .table = view.tableSchema().*, .layout = view.physicalLayout(), .rows = rows, .ordinals = ordinals, .bounds = bounds, .stats = opts.columnar_stats };
        var matched: [max_rows]bool = @splat(true);
        if (filter) |value| try block.evaluate(value.compiled, matched[0..rows.len]);
        if (opts.columnar_stats) |stats| if (std.mem.indexOfScalar(bool, matched[0..rows.len], true) == null) {
            stats.blocks_pruned += 1;
        };
        for (rows, 0..) |row, i| {
            if (opts.cancellation) |token| if (token.isCancelled()) return error.Canceled;
            if (opts.execution_deadline_ns) |deadline| if (platform_time.monotonicNs() >= deadline) return error.DeadlineExceeded;
            if (!matched[i] or !byte_range.contains(row.key) or std.mem.order(u8, row.key, from) == .lt or
                (from.len != 0 and !opts.inclusive_from and std.mem.eql(u8, row.key, from))) continue;
            if (to.len != 0 and (if (opts.exclusive_to) std.mem.order(u8, row.key, to) != .lt else std.mem.order(u8, row.key, to) == .gt)) continue;
            if (ttl_ns != 0 and row.timestamp != 0 and @import("../ttl.zig").isExpired(row.timestamp, ttl_ns, now_ns)) continue;
            var projected: ?[]u8 = null;
            if (opts.include_documents) {
                var object = std.json.ObjectMap.empty;
                for (opts.fields) |field| {
                    const values = try block.column(field);
                    if (values.len != 0) if (values[i]) |value| try object.put(scratch, field, value);
                }
                projected = try std.json.Stringify.valueAlloc(scratch, std.json.Value{ .object = object }, .{});
            }
            try progress.last_key.ensureTotalCapacity(alloc, row.key.len);
            visitor.visit(visitor.context, .{ .id = row.key, .hash = std.mem.readInt(u64, row.hash[0..8], .little), .content_hash = if (opts.include_content_hashes) row.hash else null, .document_json = projected }) catch |err| {
                progress.callback_failed = true;
                return err;
            };
            progress.last_key.clearRetainingCapacity();
            progress.last_key.appendSliceAssumeCapacity(row.key);
            progress.delivered += 1;
            count += 1;
            if (opts.columnar_stats) |stats| stats.rows_selected += 1;
            if (opts.limit > 0 and count >= opts.limit) return true;
        }
        _ = arena.reset(.free_all);
    }
    return true;
}
