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

//! Table-owned immutable column blocks and a transactionally maintained dirty
//! key directory. Scans merge typed blocks with authoritative row deltas in
//! one read snapshot. Bounded range compaction publishes replacements
//! atomically and compare-clears only the dirty images covered by its snapshot.
const std = @import("std");
const store_mod = @import("../docstore.zig");
const backend_erased = @import("../backend_erased.zig");
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
const directory_prefix_len = prefix.len + 16 + 3;
const counter_key = "\x00\x00__columnar__:next";
const manifest_key = keys.relational_columnar_manifest_key;
const building_key = "\x00\x00__columnar__:building";
const maintenance_cursor_key = "\x00\x00__columnar__:maintenance_cursor";
const maintenance_turn_key = "\x00\x00__columnar__:maintenance_turn";
const merge_cursor_key = "\x00\x00__columnar__:merge_cursor";
const cleanup_key = "\x00\x00__columnar__:cleanup";
const garbage_key = "\x00\x00__columnar__:garbage";
const dirty_prefix = keys.relational_columnar_dirty_prefix;
pub var test_before_publish: ?struct { context: *anyopaque, run: *const fn (*anyopaque) anyerror!void } = null;
pub var test_compaction_block_limit: ?u64 = null;
pub var test_cleanup_page_limit: ?usize = null;
const maintenance_records = 256;
const maintenance_bytes = 256 * 1024;
const max_rows = 256;
const null_bytes = max_rows / 8;

/// Process-local observations, sampled without taking the maintenance lock.
/// The fairness cursor itself is durable and advances even when a build fails.
pub const Maintenance = struct {
    pending: std.atomic.Value(bool) = .init(false),
    backing_off: std.atomic.Value(bool) = .init(false),
    retry_after_ns: std.atomic.Value(u64) = .init(0),
    observed_since_ns: std.atomic.Value(u64) = .init(0),
    passes: std.atomic.Value(u64) = .init(0),
    ranges_compacted: std.atomic.Value(u64) = .init(0),
    blocks_written: std.atomic.Value(u64) = .init(0),
    rows_written: std.atomic.Value(u64) = .init(0),
    ranges_merged: std.atomic.Value(u64) = .init(0),
    dirty_markers_cleared: std.atomic.Value(u64) = .init(0),
    gc_records_deleted: std.atomic.Value(u64) = .init(0),
    failures: std.atomic.Value(u64) = .init(0),
    last_pass_ns: std.atomic.Value(u64) = .init(0),

    pub fn notePending(self: *@This(), pending: bool) void {
        self.pending.store(pending, .release);
        if (pending) {
            _ = self.observed_since_ns.cmpxchgStrong(0, platform_time.monotonicNs(), .monotonic, .monotonic);
        } else self.observed_since_ns.store(0, .monotonic);
    }

    pub fn snapshot(self: *const @This()) types.ColumnarMaintenanceStats {
        const since = self.observed_since_ns.load(.monotonic);
        return .{
            .pending = self.pending.load(.acquire),
            .backing_off = self.backing_off.load(.acquire),
            .pending_age_ns = if (since == 0) 0 else platform_time.monotonicNs() -| since,
            .passes = self.passes.load(.monotonic),
            .ranges_compacted = self.ranges_compacted.load(.monotonic),
            .blocks_written = self.blocks_written.load(.monotonic),
            .rows_written = self.rows_written.load(.monotonic),
            .ranges_merged = self.ranges_merged.load(.monotonic),
            .dirty_markers_cleared = self.dirty_markers_cleared.load(.monotonic),
            .gc_records_deleted = self.gc_records_deleted.load(.monotonic),
            .failures = self.failures.load(.monotonic),
            .last_pass_ns = self.last_pass_ns.load(.monotonic),
        };
    }
};
const Manifest = struct {
    ready: bool,
    generation: u64,
    sequence: u64,
    blocks: u64,
    ranges: u64 = 0,

    fn encode(self: @This()) [41]u8 {
        var out: [41]u8 = undefined;
        @memcpy(out[0..4], "ACL3");
        out[4] = @intFromBool(self.ready);
        std.mem.writeInt(u64, out[5..13], self.generation, .little);
        std.mem.writeInt(u64, out[13..21], self.sequence, .little);
        std.mem.writeInt(u64, out[21..29], self.blocks, .little);
        std.mem.writeInt(u64, out[29..37], self.ranges, .little);
        std.mem.writeInt(u32, out[37..41], std.hash.Crc32.hash(out[0..37]), .little);
        return out;
    }

    fn decode(encoded: []const u8) !@This() {
        const bytes = try verified(encoded);
        if (bytes.len != 37 or !std.mem.eql(u8, bytes[0..4], "ACL3") or bytes[4] > 1) return error.InvalidColumnSegment;
        return .{ .ready = bytes[4] == 1, .generation = std.mem.readInt(u64, bytes[5..13], .little), .sequence = std.mem.readInt(u64, bytes[13..21], .little), .blocks = std.mem.readInt(u64, bytes[21..29], .little), .ranges = std.mem.readInt(u64, bytes[29..37], .little) };
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

fn columnMetaKey(alloc: alloc_type, generation: u64, block: u64, ordinal: u32) ![]u8 {
    return std.fmt.allocPrint(alloc, "{s}{x:0>16}:{x:0>16}:p{x:0>8}", .{ prefix, generation, block, ordinal });
}

fn appendInt(list: *std.ArrayListUnmanaged(u8), alloc: alloc_type, comptime T: type, value: T) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try list.appendSlice(alloc, &bytes);
}

const Row = struct { key: []const u8, hash: [32]u8, timestamp: u64 };
const Bounds = struct { present: bool = false, minimum: f64 = 0, maximum: f64 = 0 };
const Column = struct { writer: dv.TypedDocValuesWriter, presence: [null_bytes]u8 = @splat(0), nulls: [null_bytes]u8 = @splat(0), bounds: Bounds = .{} };

fn ColumnBuilder(comptime DBType: type) type {
    return struct {
        db: DBType,
        alloc: alloc_type,
        arena: std.heap.ArenaAllocator,
        generation: u64,
        namespace: u64,
        blocks: u64 = 0,
        expected_manifest: [41]u8,
        build_token: [16]u8,
        deferred_directory: bool = false,
        directory: std.ArrayListUnmanaged(store_mod.KVPair) = .empty,
        candidates: std.ArrayListUnmanaged(store_mod.KVPair) = .empty,
        boundary: ?[]const u8 = "",
        stop_after_block: ?u64 = null,
        continuation: ?[]u8 = null,
        deadline_ns: u64 = std.math.maxInt(u64),
        last_directory_key: ?[]u8 = null,
        last_directory_block: u64 = 0,
        bytes: usize = 0,
        view: ?registry.SchemaView = null,
        rows: std.ArrayListUnmanaged(Row) = .empty,
        columns: std.AutoHashMapUnmanaged(u32, Column) = .empty,

        fn flush(self: *@This()) !void {
            if (self.rows.items.len == 0) return;
            const scratch = self.arena.allocator();
            var meta = std.ArrayListUnmanaged(u8).empty;
            try meta.appendSlice(scratch, "ACB4");
            try appendInt(&meta, scratch, u32, self.view.?.version());
            try appendInt(&meta, scratch, u32, @intCast(self.rows.items.len));
            const ordinals = try scratch.alloc(u32, self.columns.count());
            var ordinal_it = self.columns.keyIterator();
            for (ordinals) |*ordinal| ordinal.* = (ordinal_it.next()).?.*;
            std.mem.sort(u32, ordinals, {}, std.sort.asc(u32));
            var pages: u32 = 0;
            for (ordinals, 0..) |ordinal, i| if (i == 0 or ordinal / 64 != ordinals[i - 1] / 64) {
                pages += 1;
            };
            try appendInt(&meta, scratch, u32, pages);
            var pos: usize = 0;
            while (pos < ordinals.len) {
                const page = ordinals[pos] / 64;
                var mask: u64 = 0;
                while (pos < ordinals.len and ordinals[pos] / 64 == page) : (pos += 1) mask |= @as(u64, 1) << @intCast(ordinals[pos] % 64);
                try appendInt(&meta, scratch, u32, page);
                try appendInt(&meta, scratch, u64, mask);
            }
            var writes = std.ArrayListUnmanaged(store_mod.KVPair).empty;
            var columns = self.columns.iterator();
            while (columns.next()) |entry| {
                const ordinal = entry.key_ptr.*;
                var column_meta = std.ArrayListUnmanaged(u8).empty;
                const bounds = entry.value_ptr.bounds;
                try column_meta.append(scratch, @intFromBool(bounds.present));
                try appendInt(&column_meta, scratch, u64, @bitCast(bounds.minimum));
                try appendInt(&column_meta, scratch, u64, @bitCast(bounds.maximum));
                try column_meta.appendSlice(scratch, &entry.value_ptr.presence);
                try column_meta.appendSlice(scratch, &entry.value_ptr.nulls);
                try writes.append(scratch, .{ .key = try columnMetaKey(scratch, self.generation, self.blocks, ordinal), .value = try checked(scratch, column_meta.items) });
                const values = try entry.value_ptr.writer.build();
                try writes.append(scratch, .{ .key = try blockKey(scratch, self.generation, self.blocks, ordinal), .value = try checked(scratch, values) });
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
            const directory_key = try std.fmt.allocPrint(scratch, "{s}{x:0>16}:r:{s}", .{ prefix, self.generation, self.boundary orelse self.rows.items[0].key });
            self.boundary = null;
            const directory_value = try directoryValue(scratch, self.blocks, "");
            if (self.deferred_directory) {
                if (self.directory.items.len != 0) try self.finishDirectory(directory_key[directory_prefix_len..]);
                const owned_key = try self.alloc.dupe(u8, directory_key);
                errdefer self.alloc.free(owned_key);
                const owned_value = try self.alloc.dupe(u8, directory_value);
                errdefer self.alloc.free(owned_value);
                try self.directory.append(self.alloc, .{ .key = owned_key, .value = owned_value });
                if (self.rows.items.len < max_rows / 2 and self.bytes < 512 * 1024) {
                    const candidate_key = try candidateKey(self.alloc, self.generation, directory_key[directory_prefix_len..]);
                    errdefer self.alloc.free(candidate_key);
                    const candidate_value = try self.alloc.dupe(u8, directory_value);
                    errdefer self.alloc.free(candidate_value);
                    try self.candidates.append(self.alloc, .{ .key = candidate_key, .value = candidate_value });
                }
            } else {
                if (self.last_directory_key) |previous_key| try writes.append(scratch, .{ .key = previous_key, .value = try directoryValue(scratch, self.last_directory_block, directory_key[directory_prefix_len..]) });
                try writes.append(scratch, .{ .key = directory_key, .value = directory_value });
                if (self.rows.items.len < max_rows / 2 and self.bytes < 512 * 1024) {
                    try writes.append(scratch, .{ .key = try candidateKey(scratch, self.generation, directory_key[directory_prefix_len..]), .value = directory_value });
                    try writes.append(scratch, .{ .key = try mergeQueueKey(scratch, self.generation, directory_key[directory_prefix_len..]), .value = "" });
                }
            }
            const next_directory_key = if (!self.deferred_directory) try self.alloc.dupe(u8, directory_key) else null;
            errdefer if (next_directory_key) |key| self.alloc.free(key);
            {
                self.db.core.lockApplyShared();
                defer self.db.core.unlockApplyShared();
                if (self.namespace != self.db.core.schemaNamespaceGeneration()) return error.PreparedGenerationChanged;
                var txn = try self.db.core.store.beginWriteTxn();
                var live = true;
                defer if (live) txn.abort();
                if (!try sameValue(&txn, manifest_key, &self.expected_manifest) or !try sameValue(&txn, building_key, &self.build_token)) return error.PreparedGenerationChanged;
                for (writes.items) |write| try txn.put(write.key, write.value);
                try txn.commit();
                live = false;
            }
            if (self.last_directory_key) |key| self.alloc.free(key);
            self.last_directory_key = next_directory_key;
            self.last_directory_block = self.blocks;
            self.blocks += 1;
            _ = self.db.relational_column_maintenance.blocks_written.fetchAdd(1, .monotonic);
            _ = self.db.relational_column_maintenance.rows_written.fetchAdd(self.rows.items.len, .monotonic);
            self.rows = .empty;
            self.columns = .empty;
            self.bytes = 0;
            _ = self.arena.reset(.free_all);
        }

        fn yieldBeforeRow(self: *@This(), key: []const u8) !bool {
            const full = if (self.stop_after_block) |limit| self.blocks >= limit else false;
            if (full or (self.directory.items.len != 0 and self.deferred_directory and platform_time.monotonicNs() >= self.deadline_ns)) {
                self.continuation = (try keys.decodeStoredDocumentRowKeyAlloc(self.alloc, key)).?;
                return true;
            }
            return false;
        }

        fn visit(ptr: ?*anyopaque, key: []const u8, value: []const u8) !store_mod.DocStore.ScanAction {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            if (self.db.artifact_repair_metadata_stop.load(.acquire)) return error.Canceled;
            if (!keys.isRelationalRowKey(key)) return .@"continue";
            if (try self.yieldBeforeRow(key)) return .stop;
            const version = try codec.rowSchemaVersion(value);
            if (self.view == null or self.view.?.version() != version) {
                try self.flush();
                // Epoch changes can flush a partial block and exhaust the
                // quantum before the incoming row is added.
                if (try self.yieldBeforeRow(key)) return .stop;
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
                entry.value_ptr.presence[id / 8] |= @as(u8, 1) << @intCast(id % 8);
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

        fn deinit(self: *@This()) void {
            if (self.last_directory_key) |key| self.alloc.free(key);
            if (self.view) |*view| view.release();
            self.arena.deinit();
            for (self.directory.items) |entry| {
                self.alloc.free(entry.key);
                self.alloc.free(entry.value);
            }
            self.directory.deinit(self.alloc);
            for (self.candidates.items) |entry| {
                self.alloc.free(entry.key);
                self.alloc.free(entry.value);
            }
            self.candidates.deinit(self.alloc);
            if (self.continuation) |value| self.alloc.free(value);
        }

        fn finishDirectory(self: *@This(), end: []const u8) !void {
            if (self.directory.items.len == 0) return;
            const last = &self.directory.items[self.directory.items.len - 1];
            const body = try verified(last.value);
            const replacement = try directoryValue(self.alloc, std.mem.readInt(u64, body[0..8], .little), end);
            self.alloc.free(last.value);
            last.value = replacement;
        }
    };
}

fn sameValue(txn: *store_mod.DocStore.Txn, key: []const u8, expected: []const u8) !bool {
    const value = txn.get(key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    return std.mem.eql(u8, value, expected);
}

fn deleteIfPresent(txn: *store_mod.DocStore.Txn, key: []const u8) !void {
    txn.delete(key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
}

fn buildToken(generation: u64, first_block: u64) [16]u8 {
    var value: [16]u8 = undefined;
    std.mem.writeInt(u64, value[0..8], generation, .little);
    std.mem.writeInt(u64, value[8..16], first_block, .little);
    return value;
}

/// Uses one pinned primary snapshot and at most one block of preparation
/// memory. Racing writes remain in the dirty directory; schema/namespace
/// replacement invalidates the pending generation's publication fence.
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
            const abandoned = start.get(building_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (abandoned) |token| {
                // Revoke the abandoned builder before incrementally reclaiming
                // its prefix. The durable job survives another cancellation.
                try start.put(garbage_key, token);
                try start.delete(building_key);
                try start.commit();
                start_live = false;
                return true;
            }
            start.abort();
            start_live = false;
            db.core.unlockApplyShared();
            initial_locked = false;
            if (try drainCleanup(db, alloc, manifest.generation, namespace)) return true;
            if (try drainGarbage(db, alloc, namespace)) return true;
            if (try prune(db, alloc, manifest.generation, namespace)) return true;
            return try compact(db, alloc, namespace);
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
    const token = buildToken(generation, 0);
    try start.put(counter_key, &counter);
    try start.put(manifest_key, &pending);
    try start.put(building_key, &token);
    try deleteIfPresent(&start, cleanup_key);
    try deleteIfPresent(&start, garbage_key);
    try start.commit();
    start_live = false;

    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    db.core.unlockApplyShared();
    initial_locked = false;
    const sequence = try db.core.store.lastReplaySequenceFromTxn(&read, 0);
    var builder = ColumnBuilder(@TypeOf(db)){ .db = db, .alloc = alloc, .arena = std.heap.ArenaAllocator.init(alloc), .generation = generation, .namespace = namespace, .expected_manifest = pending, .build_token = token };
    defer builder.deinit();
    const lower = try db.core.documentRangeLowerAlloc("");
    defer db.core.alloc.free(lower);
    try db.core.store.scanReadTxnWithContext(&read, lower, "", .{}, &builder, ColumnBuilder(@TypeOf(db)).visit);
    try builder.flush();
    const cleanup = try stageCleanup(db, alloc, &read, "", "", &pending, token, namespace);
    defer cleanup.deinit(alloc);
    if (comptime @import("builtin").is_test) if (test_before_publish) |hook| try hook.run(hook.context);
    if (db.artifact_repair_metadata_stop.load(.acquire)) return error.Canceled;
    db.core.lockApplyShared();
    var publish_locked = true;
    defer if (publish_locked) db.core.unlockApplyShared();
    if (namespace != db.core.schemaNamespaceGeneration()) return false;
    var publish = try db.core.store.beginWriteTxn();
    var publish_live = true;
    defer if (publish_live) publish.abort();
    const fence = publish.get(manifest_key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    if (!std.mem.eql(u8, fence, &pending) or !try sameValue(&publish, building_key, &token)) return false;
    const ready = (Manifest{ .ready = true, .generation = generation, .sequence = sequence, .blocks = builder.blocks, .ranges = builder.blocks }).encode();
    try publish.put(manifest_key, &ready);
    try publish.delete(building_key);
    const inline_cleared = try cleanup.publish(&publish, &token);
    try publish.commit();
    publish_live = false;
    _ = db.relational_column_maintenance.dirty_markers_cleared.fetchAdd(inline_cleared, .monotonic);
    db.core.unlockApplyShared();
    publish_locked = false;
    try finishCleanupQuantum(db, alloc, generation, namespace);
    return true;
}

fn cleanupPrefix(alloc: alloc_type, token: []const u8) ![]u8 {
    if (token.len != 16) return error.InvalidColumnSegment;
    return std.fmt.allocPrint(alloc, "{s}{x:0>16}:{x:0>16}:q", .{ prefix, std.mem.readInt(u64, token[0..8], .little), std.mem.readInt(u64, token[8..16], .little) });
}

/// Journal exact dirty images before publication. Pages are unpublished until
/// cleanup_key is committed with the directory. They require no live snapshot
/// during cleanup/restart and cannot clear a newer primary image.
const Cleanup = struct {
    pages: usize = 0,
    inline_page: ?[]u8 = null,

    fn deinit(self: @This(), alloc: alloc_type) void {
        if (self.inline_page) |page| alloc.free(page);
    }

    fn publish(self: @This(), txn: *store_mod.DocStore.Txn, token: []const u8) !u64 {
        if (self.pages != 0) try txn.put(cleanup_key, token);
        // The common one-page case compare-clears in the publication itself:
        // no journal write, extra commit, or post-publication cleanup work.
        return if (self.inline_page) |page| clearPage(txn, page) else 0;
    }
};

fn clearPage(txn: *store_mod.DocStore.Txn, page: []const u8) !u64 {
    var decoder = Decoder{ .bytes = page };
    var cleared: u64 = 0;
    while (decoder.bytes.len != 0) {
        const key = try decoder.take(try decoder.int(u32));
        const expected = try decoder.take(@sizeOf(keys.ColumnarMutationToken));
        if (!std.mem.startsWith(u8, key, dirty_prefix)) return error.InvalidColumnSegment;
        if (try sameValue(txn, key, expected)) {
            try txn.delete(key);
            cleared += 1;
        }
    }
    return cleared;
}

fn stageCleanup(db: anytype, alloc: alloc_type, read: *store_mod.DocStore.Txn, from: []const u8, to: []const u8, manifest: []const u8, token: [16]u8, namespace: u64) !Cleanup {
    const lower = try std.mem.concat(alloc, u8, &.{ dirty_prefix, from });
    defer alloc.free(lower);
    const page_prefix = try cleanupPrefix(alloc, &token);
    defer alloc.free(page_prefix);
    var cursor = try read.openCursor();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(lower);
    var pages: usize = 0;
    while (entry) |first| {
        if (!std.mem.startsWith(u8, first.key, dirty_prefix)) break;
        if (to.len != 0 and std.mem.order(u8, first.key[dirty_prefix.len..], to) != .lt) break;
        if (db.artifact_repair_metadata_stop.load(.acquire)) return error.Canceled;
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        var page = std.ArrayListUnmanaged(u8).empty;
        var count: usize = 0;
        const limit = if (@import("builtin").is_test) test_cleanup_page_limit orelse maintenance_records else maintenance_records;
        while (entry) |item| {
            if (!std.mem.startsWith(u8, item.key, dirty_prefix) or (to.len != 0 and std.mem.order(u8, item.key[dirty_prefix.len..], to) != .lt)) break;
            if (item.value.len != @sizeOf(keys.ColumnarMutationToken)) return error.InvalidColumnSegment;
            try appendInt(&page, scratch, u32, std.math.cast(u32, item.key.len) orelse return error.InvalidColumnSegment);
            try page.appendSlice(scratch, item.key);
            try page.appendSlice(scratch, item.value);
            count += 1;
            entry = try cursor.next();
            if (count >= limit or page.items.len >= maintenance_bytes) break;
        }
        const more = if (entry) |item| std.mem.startsWith(u8, item.key, dirty_prefix) and
            (to.len == 0 or std.mem.order(u8, item.key[dirty_prefix.len..], to) == .lt) else false;
        if (pages == 0 and !more) return .{ .inline_page = try alloc.dupe(u8, page.items) };
        db.core.lockApplyShared();
        defer db.core.unlockApplyShared();
        if (namespace != db.core.schemaNamespaceGeneration()) return error.PreparedGenerationChanged;
        var txn = try db.core.store.beginWriteTxn();
        var live = true;
        defer if (live) txn.abort();
        if (!try sameValue(&txn, manifest_key, manifest) or !try sameValue(&txn, building_key, &token)) return error.PreparedGenerationChanged;
        try txn.put(try std.fmt.allocPrint(scratch, "{s}{x:0>16}", .{ page_prefix, pages }), try checked(scratch, page.items));
        try txn.commit();
        live = false;
        pages += 1;
    }
    return .{ .pages = pages };
}

/// One bounded, atomic cleanup page per quantum. Deleting the page is the
/// durable continuation; cleanup never scans/rebuilds already published rows.
fn drainCleanup(db: anytype, alloc: alloc_type, generation: u64, namespace: u64) !bool {
    db.core.lockApplyShared();
    defer db.core.unlockApplyShared();
    if (namespace != db.core.schemaNamespaceGeneration()) return error.PreparedGenerationChanged;
    if (db.artifact_repair_metadata_stop.load(.acquire)) return error.Canceled;
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    const token = read.get(cleanup_key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    const page_prefix = try cleanupPrefix(alloc, token);
    defer alloc.free(page_prefix);
    if (std.mem.readInt(u64, token[0..8], .little) != generation) return error.InvalidColumnSegment;
    var cursor = try read.openCursor();
    defer cursor.close();
    const entry = try cursor.seekAtOrAfter(page_prefix);
    var txn = try db.core.store.beginWriteTxn();
    var live = true;
    defer if (live) txn.abort();
    if (!try sameValue(&txn, cleanup_key, token)) return error.PreparedGenerationChanged;
    const current = txn.get(manifest_key) catch |err| switch (err) {
        error.NotFound => return error.PreparedGenerationChanged,
        else => return err,
    };
    const manifest = try Manifest.decode(current);
    if (!manifest.ready or manifest.generation != generation) return error.PreparedGenerationChanged;
    var cleared: u64 = 0;
    if (entry) |page| {
        if (std.mem.startsWith(u8, page.key, page_prefix)) {
            cleared = try clearPage(&txn, try verified(page.value));
            try txn.delete(page.key);
            const following = try cursor.next();
            if (following == null or !std.mem.startsWith(u8, following.?.key, page_prefix)) try txn.delete(cleanup_key);
        } else try txn.delete(cleanup_key);
    } else try txn.delete(cleanup_key);
    try txn.commit();
    live = false;
    _ = db.relational_column_maintenance.dirty_markers_cleared.fetchAdd(cleared, .monotonic);
    return true;
}

fn finishCleanupQuantum(db: anytype, alloc: alloc_type, generation: u64, namespace: u64) !void {
    const started = platform_time.monotonicNs();
    const pages: usize = if (@import("builtin").is_test and test_cleanup_page_limit != null) 1 else 8;
    for (0..pages) |_| {
        if (!try drainCleanup(db, alloc, generation, namespace)) break;
        if (platform_time.monotonicNs() -| started >= 50 * std.time.ns_per_ms) break;
    }
}

const Range = struct { key: []const u8, value: []const u8, start: []const u8, end: []const u8, block: u64 };
fn candidateKey(alloc: alloc_type, generation: u64, start: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc, "{s}{x:0>16}:s:{s}", .{ prefix, generation, start });
}

fn mergeQueueKey(alloc: alloc_type, generation: u64, start: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc, "{s}{x:0>16}:t:{s}", .{ prefix, generation, start });
}

fn mergeCandidate(read: *store_mod.DocStore.Txn, alloc: alloc_type, generation: u64, range: Range) !bool {
    const key = try candidateKey(alloc, generation, range.start);
    const value = read.get(key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    const body = try verified(value);
    return body.len >= 12 and std.mem.readInt(u64, body[0..8], .little) == range.block;
}

fn blockVersion(read: *store_mod.DocStore.Txn, alloc: alloc_type, generation: u64, block: u64) !u32 {
    const meta = try verified(try read.get(try blockKey(alloc, generation, block, null)));
    if (meta.len < 16 or !std.mem.eql(u8, meta[0..4], "ACB4")) return error.InvalidColumnSegment;
    return std.mem.readInt(u32, meta[4..8], .little);
}

fn retireBlock(txn: *store_mod.DocStore.Txn, read: *store_mod.DocStore.Txn, alloc: alloc_type, generation: u64, block: u64) !void {
    const key = try blockKey(alloc, generation, block, null);
    var decoder = Decoder{ .bytes = try verified(try read.get(key)) };
    _ = try decoder.take(12);
    const count = try decoder.int(u32);
    for (0..count) |_| {
        const page = try decoder.int(u32);
        var mask = try decoder.int(u64);
        if (page > std.math.maxInt(u32) / 64) return error.InvalidColumnSegment;
        while (mask != 0) {
            const ordinal = page * 64 + @as(u32, @intCast(@ctz(mask)));
            mask &= mask - 1;
            try txn.delete(try blockKey(alloc, generation, block, ordinal));
            try txn.delete(try columnMetaKey(alloc, generation, block, ordinal));
        }
    }
    try txn.delete(key);
}
fn directoryValue(alloc: alloc_type, block: u64, end: []const u8) ![]u8 {
    var bytes = std.ArrayListUnmanaged(u8).empty;
    defer bytes.deinit(alloc);
    try appendInt(&bytes, alloc, u64, block);
    try appendInt(&bytes, alloc, u32, std.math.cast(u32, end.len) orelse return error.InvalidColumnSegment);
    try bytes.appendSlice(alloc, end);
    return checked(alloc, bytes.items);
}
const Directory = struct {
    alloc: alloc_type,
    cursor: store_mod.DocStore.Txn.CursorAdapter,
    prefix: []u8,
    pending: ?store_mod.KVPair,

    fn init(alloc: alloc_type, txn: *store_mod.DocStore.Txn, generation: u64, from: []const u8) !Directory {
        const dir = try std.fmt.allocPrint(alloc, "{s}{x:0>16}:r:", .{ prefix, generation });
        errdefer alloc.free(dir);
        const lower = try std.mem.concat(alloc, u8, &.{ dir, from });
        defer alloc.free(lower);
        var cursor = try txn.openCursor();
        errdefer cursor.close();
        var entry = try cursor.seekAtOrBefore(lower);
        if (entry == null or !std.mem.startsWith(u8, entry.?.key, dir)) {
            entry = try cursor.seekAtOrAfter(dir);
            if (entry) |kv| if (std.mem.startsWith(u8, kv.key, dir) and kv.key.len != dir.len) return error.InvalidColumnSegment;
        }
        return .{ .alloc = alloc, .cursor = cursor, .prefix = dir, .pending = if (entry) |kv| .{ .key = kv.key, .value = kv.value } else null };
    }
    fn deinit(self: *@This()) void {
        self.cursor.close();
        self.alloc.free(self.prefix);
    }
    fn next(self: *@This(), alloc: alloc_type) !?Range {
        const entry = self.pending orelse return null;
        if (!std.mem.startsWith(u8, entry.key, self.prefix)) return null;
        const key = try alloc.dupe(u8, entry.key);
        const value = try alloc.dupe(u8, entry.value);
        const index = try verified(value);
        if (index.len < 12 or std.mem.readInt(u32, index[8..12], .little) != index.len - 12) return error.InvalidColumnSegment;
        const following = try self.cursor.next();
        self.pending = if (following) |kv| .{ .key = kv.key, .value = kv.value } else null;
        const end = if (self.pending) |kv| if (std.mem.startsWith(u8, kv.key, self.prefix)) try alloc.dupe(u8, kv.key[self.prefix.len..]) else "" else "";
        if (!std.mem.eql(u8, index[12..], end)) return error.InvalidColumnSegment;
        return .{ .key = key, .value = value, .start = key[self.prefix.len..], .end = end, .block = std.mem.readInt(u64, index[0..8], .little) };
    }
};

const DirtyRanges = struct {
    cursor: store_mod.DocStore.Txn.CursorAdapter,
    pending: ?[]const u8,
    fn init(txn: *store_mod.DocStore.Txn, alloc: alloc_type, from: []const u8) !DirtyRanges {
        const lower = try std.mem.concat(alloc, u8, &.{ dirty_prefix, from });
        defer alloc.free(lower);
        var cursor = try txn.openCursor();
        errdefer cursor.close();
        const entry = try cursor.seekAtOrAfter(lower);
        return .{ .cursor = cursor, .pending = if (entry) |kv| kv.key else null };
    }
    fn overlaps(self: *@This(), alloc: alloc_type, from: []const u8, to: []const u8) !bool {
        var key = self.pending orelse return false;
        if (!std.mem.startsWith(u8, key, dirty_prefix)) return false;
        if (std.mem.order(u8, key[dirty_prefix.len..], from) == .lt) {
            const lower = try std.mem.concat(alloc, u8, &.{ dirty_prefix, from });
            defer alloc.free(lower);
            const entry = try self.cursor.seekAtOrAfter(lower);
            self.pending = if (entry) |kv| kv.key else null;
            key = self.pending orelse return false;
            if (!std.mem.startsWith(u8, key, dirty_prefix)) return false;
        }
        return to.len == 0 or std.mem.order(u8, key[dirty_prefix.len..], to) == .lt;
    }

    /// Merge authoritative mutations up to a base-row key (inclusive), or to a
    /// range end (exclusive). The cursor streams arbitrarily large deltas with
    /// one row arena/read scope; no dirty-range materialization is needed.
    fn emitThrough(self: *@This(), db: anytype, alloc: alloc_type, txn: *store_mod.DocStore.Txn, end: []const u8, inclusive: bool, from: []const u8, to: []const u8, byte_range: types.ByteRange, opts: types.ScanOptions, visitor: types.ScanVisitor, ttl_ns: u64, now_ns: u64, progress: *Progress, filter: ?*const graph.PreparedPatternFilter) !bool {
        var replaced = false;
        while (self.pending) |key| {
            if (!std.mem.startsWith(u8, key, dirty_prefix)) break;
            const raw = key[dirty_prefix.len..];
            if ((to.len != 0 and (if (opts.exclusive_to) std.mem.order(u8, raw, to) != .lt else std.mem.order(u8, raw, to) == .gt)) or
                (byte_range.end.len != 0 and std.mem.order(u8, raw, byte_range.end) != .lt))
            {
                self.pending = null;
                break;
            }
            if (end.len != 0 or inclusive) {
                const order = std.mem.order(u8, raw, end);
                if (order == .gt or (order == .eq and !inclusive)) break;
                replaced = order == .eq;
            }
            if (opts.cancellation) |token| if (token.isCancelled()) return error.Canceled;
            if (opts.execution_deadline_ns) |deadline| if (platform_time.monotonicNs() >= deadline) return error.DeadlineExceeded;
            if (opts.limit > 0 and progress.delivered >= opts.limit) return replaced;
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const scratch = arena.allocator();
            const id = try scratch.dupe(u8, raw);
            const next = try self.cursor.next();
            self.pending = if (next) |entry| entry.key else null;
            if (!eligibleRow(.{ .key = id, .hash = undefined, .timestamp = 0 }, from, to, byte_range, opts, 0, now_ns)) continue;
            var scope = try txn.openReadScope(scratch);
            defer scope.close();
            if (opts.columnar_stats) |stats| stats.overlay_rows_read += 1;
            const packed_key = try keys.relationalRowKeyAlloc(scratch, id);
            const bytes = scope.get(packed_key) catch |err| switch (err) {
                error.NotFound => continue, // Tombstone still masks the base row.
                else => return err,
            };
            if (opts.columnar_stats) |stats| stats.primary_rows_read += 1;
            const version = try codec.rowSchemaVersion(bytes);
            var view = (try db.core.acquireSchemaVersionView(version)) orelse return error.UnknownSchemaVersion;
            defer view.release();
            const typed = if (db.core.store.valuesAreAuthenticated()) try codec.ordinalRowViewTrusted(bytes, view.tableSchema().*, view.physicalLayout()) else try codec.ordinalRowView(bytes, view.tableSchema().*, view.physicalLayout());
            const row = Row{ .key = id, .hash = typed.semanticHash(), .timestamp = typed.writeTimestampNs() };
            if (!eligibleRow(row, from, to, byte_range, opts, ttl_ns, now_ns)) continue;
            if (filter) |compiled| {
                const matches = (try compiled.compiled.matchesOrdinal(scratch, id, typed)) orelse blk: {
                    var logical = try typed.materializeRootAlloc(scratch);
                    defer logical.deinit(scratch);
                    break :blk try compiled.compiled.matches(scratch, id, logical.root);
                };
                if (!matches) continue;
            }
            const projected = if (opts.include_documents) try codec.projectOrdinalFieldsTrustedWithLayoutAlloc(scratch, bytes, view.tableSchema().*, view.physicalLayout(), opts.fields) else null;
            try deliver(alloc, row, projected, opts, visitor, progress);
        }
        return replaced;
    }
};

fn rangeClean(read: *store_mod.DocStore.Txn, alloc: alloc_type, range: Range) !bool {
    var dirty = try DirtyRanges.init(read, alloc, range.start);
    defer dirty.cursor.close();
    return !try dirty.overlaps(alloc, range.start, range.end);
}

fn scheduleMaintenance(txn: *store_mod.DocStore.Txn, alloc: alloc_type, turn: u64, selected: ?[]const u8) !void {
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, turn +% 1, .little);
    try txn.put(maintenance_turn_key, &encoded);
    if (selected) |key| try txn.put(merge_cursor_key, try checked(alloc, key));
}

/// Dense insert/update bursts favor a sequential primary scan over hundreds of
/// individual point reads. Probe only a bounded number of dirty keys; small
/// LIMIT queries retain the streaming overlay's early exit.
fn preferPrimaryScan(txn: *store_mod.DocStore.Txn, alloc: alloc_type, range: Range, from: []const u8, to: []const u8, byte_range: types.ByteRange, opts: types.ScanOptions, base_rows: usize) !bool {
    if (opts.limit > 0 and opts.limit <= 16) return false;
    const threshold = @max(@as(usize, 64), base_rows);
    var lower = range.start;
    if (std.mem.order(u8, lower, from) == .lt) lower = from;
    if (std.mem.order(u8, lower, byte_range.start) == .lt) lower = byte_range.start;
    var cursor = try txn.openCursor();
    defer cursor.close();
    const key = try std.mem.concat(alloc, u8, &.{ dirty_prefix, lower });
    defer alloc.free(key);
    var entry = try cursor.seekAtOrAfter(key);
    var count: usize = 0;
    while (entry) |item| {
        if (!std.mem.startsWith(u8, item.key, dirty_prefix)) break;
        const id = item.key[dirty_prefix.len..];
        if ((range.end.len != 0 and std.mem.order(u8, id, range.end) != .lt) or
            (byte_range.end.len != 0 and std.mem.order(u8, id, byte_range.end) != .lt) or
            (to.len != 0 and (if (opts.exclusive_to) std.mem.order(u8, id, to) != .lt else std.mem.order(u8, id, to) == .gt))) break;
        if (opts.cancellation) |token| if (token.isCancelled()) return error.Canceled;
        if (opts.execution_deadline_ns) |deadline| if (platform_time.monotonicNs() >= deadline) return error.DeadlineExceeded;
        count += 1;
        if (count >= threshold) return true;
        entry = try cursor.next();
    }
    return false;
}

/// Compact one dirty key range per maintenance pass. Large insertion bursts
/// split at 64 blocks; the old base retains the uncovered suffix and its dirty
/// markers until another pass. Neither preparation memory nor writer work
/// scales with the total table size.
fn compact(db: anytype, alloc: alloc_type, namespace: u64) !bool {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    db.core.lockApplyShared();
    var locked = true;
    defer if (locked) db.core.unlockApplyShared();
    if (namespace != db.core.schemaNamespaceGeneration()) return error.PreparedGenerationChanged;
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    const manifest_bytes = try read.get(manifest_key);
    const manifest = try Manifest.decode(manifest_bytes);
    if (!manifest.ready) return false;
    var dirty = try read.openCursor();
    defer dirty.close();
    const saved_cursor = read.get(maintenance_cursor_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    const resume_id = if (saved_cursor) |encoded| blk: {
        const body = verified(encoded) catch break :blk "";
        if (body.len < 12 or std.mem.readInt(u64, body[0..8], .little) != manifest.generation or
            std.mem.readInt(u32, body[8..12], .little) != body.len - 12) break :blk "";
        break :blk body[12..];
    } else "";
    const resume_key = try std.mem.concat(scratch, u8, &.{ dirty_prefix, resume_id });
    var pending_entry = try dirty.seekAtOrAfter(resume_key);
    if (pending_entry == null or !std.mem.startsWith(u8, pending_entry.?.key, dirty_prefix)) pending_entry = try dirty.seekAtOrAfter(dirty_prefix);
    const any_dirty = if (pending_entry) |entry| std.mem.startsWith(u8, entry.key, dirty_prefix) else false;
    const candidate_prefix = try mergeQueueKey(scratch, manifest.generation, "");
    const turn_bytes = read.get(maintenance_turn_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    const turn: u64 = if (turn_bytes) |bytes| if (bytes.len == 8) std.mem.readInt(u64, bytes[0..8], .little) else 0 else 0;
    var candidate_cursor = try read.openCursor();
    defer candidate_cursor.close();
    const saved_merge = read.get(merge_cursor_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    const merge_resume = if (saved_merge) |bytes| verified(bytes) catch "" else "";
    const candidate_lower = if (std.mem.startsWith(u8, merge_resume, candidate_prefix)) try std.mem.concat(scratch, u8, &.{ merge_resume, "\x00" }) else candidate_prefix;
    var queued_candidate = try candidate_cursor.seekAtOrAfter(candidate_lower);
    if (queued_candidate == null or !std.mem.startsWith(u8, queued_candidate.?.key, candidate_prefix)) queued_candidate = try candidate_cursor.seekAtOrAfter(candidate_prefix);
    const has_candidate = if (queued_candidate) |entry| std.mem.startsWith(u8, entry.key, candidate_prefix) else false;
    const merge_turn = has_candidate and (!any_dirty or turn % 4 == 3);
    const has_dirty = any_dirty and !merge_turn;
    if (merge_turn) pending_entry = queued_candidate;
    if (!any_dirty and !has_candidate) return false;
    const first = pending_entry orelse return false;
    if (!has_dirty and !std.mem.startsWith(u8, first.key, candidate_prefix)) return false;
    const selected_candidate = if (!has_dirty) try scratch.dupe(u8, first.key) else null;
    const dirty_id = try scratch.dupe(u8, first.key[if (has_dirty) dirty_prefix.len else candidate_prefix.len..]);
    var directory = try Directory.init(alloc, &read, manifest.generation, dirty_id);
    defer directory.deinit();
    const selected = (try directory.next(scratch)) orelse {
        db.core.unlockApplyShared();
        locked = false;
        return rebuild(db, alloc, true);
    };
    var ranges = std.ArrayListUnmanaged(Range).empty;
    const version = try blockVersion(&read, scratch, manifest.generation, selected.block);
    // Dirty ranges must reach their own rows before spending a quantum on a
    // predecessor: otherwise a partial build at an epoch boundary can keep
    // rewriting the same clean prefix. The clean merge queue handles neighbors
    // after dirty compaction has made their occupancy/schema metadata current.
    if (!has_dirty and selected.start.len != 0) {
        var previous_cursor = try read.openCursor();
        defer previous_cursor.close();
        _ = try previous_cursor.seekAtOrBefore(selected.key);
        if (try previous_cursor.prev()) |previous| if (std.mem.startsWith(u8, previous.key, directory.prefix)) {
            const previous_id = try scratch.dupe(u8, previous.key[directory.prefix.len..]);
            var previous_directory = try Directory.init(alloc, &read, manifest.generation, previous_id);
            defer previous_directory.deinit();
            const previous_range = (try previous_directory.next(scratch)).?;
            if (try mergeCandidate(&read, scratch, manifest.generation, previous_range) and
                try rangeClean(&read, scratch, previous_range) and
                try blockVersion(&read, scratch, manifest.generation, previous_range.block) == version)
                try ranges.append(scratch, previous_range);
        };
    }
    try ranges.append(scratch, selected);
    while (ranges.items.len < 8) {
        const next = (try directory.next(scratch)) orelse break;
        if (!try mergeCandidate(&read, scratch, manifest.generation, next) or
            (!has_dirty and !try rangeClean(&read, scratch, next)) or
            try blockVersion(&read, scratch, manifest.generation, next.block) != version) break;
        try ranges.append(scratch, next);
    }
    if (selected_candidate) |candidate| {
        if (!std.mem.eql(u8, selected.start, dirty_id) or !try mergeCandidate(&read, scratch, manifest.generation, selected) or ranges.items.len == 1 or !try rangeClean(&read, scratch, selected)) {
            var discard = try db.core.store.beginWriteTxn();
            var discard_live = true;
            defer if (discard_live) discard.abort();
            try scheduleMaintenance(&discard, scratch, turn, selected_candidate);
            try discard.delete(candidate);
            try discard.commit();
            discard_live = false;
            return true;
        }
    }
    const range = Range{ .key = ranges.items[0].key, .value = ranges.items[0].value, .start = ranges.items[0].start, .end = ranges.items[ranges.items.len - 1].end, .block = ranges.items[0].block };
    // Bound dirty journal work independently of live/output rows. A large
    // deleted insertion burst must not produce an unbounded cleanup journal.
    var dirty_limit: ?[]const u8 = null;
    var captured_dirty = try read.openCursor();
    defer captured_dirty.close();
    var captured = try captured_dirty.seekAtOrAfter(try std.mem.concat(scratch, u8, &.{ dirty_prefix, range.start }));
    var dirty_count: usize = 0;
    var dirty_bytes: usize = 0;
    while (captured) |entry| {
        if (!std.mem.startsWith(u8, entry.key, dirty_prefix)) break;
        const id = entry.key[dirty_prefix.len..];
        if (range.end.len != 0 and std.mem.order(u8, id, range.end) != .lt) break;
        if (dirty_count >= 4 * maintenance_records or dirty_bytes >= maintenance_bytes) {
            dirty_limit = try scratch.dupe(u8, id);
            break;
        }
        dirty_count += 1;
        dirty_bytes +|= entry.key.len + entry.value.len;
        captured = try captured_dirty.next();
    }
    var start = try db.core.store.beginWriteTxn();
    var start_live = true;
    defer if (start_live) start.abort();
    if (!try sameValue(&start, manifest_key, manifest_bytes)) return false;
    try scheduleMaintenance(&start, scratch, turn, selected_candidate);
    const old_counter = try start.get(counter_key);
    if (old_counter.len != 8) return error.InvalidColumnSegment;
    const build_id = std.math.add(u64, std.mem.readInt(u64, old_counter[0..8], .little), 1) catch return error.InvalidColumnSegment;
    if (build_id > std.math.maxInt(u32)) return error.InvalidColumnSegment;
    const first_block = build_id << 32;
    const token = buildToken(manifest.generation, first_block);
    var counter: [8]u8 = undefined;
    std.mem.writeInt(u64, &counter, build_id, .little);
    try start.put(counter_key, &counter);
    try start.put(building_key, &token);
    // Publish the next range before staging. Crashes, cancellation and a hot
    // first key must not force every subsequent pass back onto the same range.
    try start.put(maintenance_cursor_key, try directoryValue(scratch, manifest.generation, range.end));
    try start.commit();
    start_live = false;
    db.core.unlockApplyShared();
    locked = false;
    var builder = ColumnBuilder(@TypeOf(db)){
        .db = db,
        .alloc = alloc,
        .arena = std.heap.ArenaAllocator.init(alloc),
        .generation = manifest.generation,
        .namespace = namespace,
        .blocks = first_block,
        .expected_manifest = manifest.encode(),
        .build_token = token,
        .deferred_directory = true,
        .boundary = range.start,
        .stop_after_block = first_block + (if (@import("builtin").is_test) test_compaction_block_limit orelse 64 else 64),
        .deadline_ns = platform_time.monotonicNs() +| 50 * std.time.ns_per_ms,
    };
    defer builder.deinit();
    const lower = try keys.documentRangeLowerAlloc(scratch, range.start);
    const scan_end = dirty_limit orelse range.end;
    const upper = if (scan_end.len == 0) "" else try keys.documentRangeLowerAlloc(scratch, scan_end);
    try db.core.store.scanReadTxnWithContext(&read, lower, upper, .{}, &builder, ColumnBuilder(@TypeOf(db)).visit);
    try builder.flush();
    if (builder.continuation == null) if (dirty_limit) |limit| {
        builder.continuation = try alloc.dupe(u8, limit);
    };
    try builder.finishDirectory(builder.continuation orelse range.end);
    const cleanup = try stageCleanup(db, alloc, &read, range.start, builder.continuation orelse range.end, manifest_bytes, token, namespace);
    defer cleanup.deinit(alloc);
    if (comptime @import("builtin").is_test) if (test_before_publish) |hook| try hook.run(hook.context);
    if (db.artifact_repair_metadata_stop.load(.acquire)) return error.Canceled;
    db.core.lockApplyShared();
    locked = true;
    if (namespace != db.core.schemaNamespaceGeneration()) return error.PreparedGenerationChanged;
    var publish = try db.core.store.beginWriteTxn();
    var live = true;
    defer if (live) publish.abort();
    if (!try sameValue(&publish, manifest_key, manifest_bytes) or !try sameValue(&publish, building_key, &token)) return false;
    var removed_ranges: u64 = 0;
    var retained_suffix: u64 = 0;
    for (ranges.items) |old_range| {
        if (builder.continuation) |continuation| if (std.mem.order(u8, old_range.start, continuation) != .lt) break;
        try publish.delete(old_range.key);
        try deleteIfPresent(&publish, try candidateKey(scratch, manifest.generation, old_range.start));
        try deleteIfPresent(&publish, try mergeQueueKey(scratch, manifest.generation, old_range.start));
        removed_ranges += 1;
        if (builder.continuation) |continuation| {
            if (old_range.end.len == 0 or std.mem.order(u8, continuation, old_range.end) == .lt) {
                const continuation_key = try std.mem.concat(scratch, u8, &.{ directory.prefix, continuation });
                try publish.put(continuation_key, old_range.value);
                retained_suffix = 1;
                break;
            }
        }
        try retireBlock(&publish, &read, scratch, manifest.generation, old_range.block);
    }
    if (builder.directory.items.len == 0 and range.start.len != 0) {
        var previous_cursor = try read.openCursor();
        defer previous_cursor.close();
        _ = try previous_cursor.seekAtOrBefore(range.key);
        const previous = (try previous_cursor.prev()) orelse return error.InvalidColumnSegment;
        if (!std.mem.startsWith(u8, previous.key, directory.prefix)) return error.InvalidColumnSegment;
        const previous_value = try verified(previous.value);
        if (previous_value.len < 12 or !std.mem.eql(u8, previous_value[12..], range.start)) return error.InvalidColumnSegment;
        try publish.put(previous.key, try directoryValue(scratch, std.mem.readInt(u64, previous_value[0..8], .little), builder.continuation orelse range.end));
    }
    for (builder.directory.items) |entry| try publish.put(entry.key, entry.value);
    for (builder.candidates.items) |entry| {
        try publish.put(entry.key, entry.value);
        try publish.put(try mergeQueueKey(scratch, manifest.generation, entry.key[directory_prefix_len..]), "");
    }
    var empty_prefix: u64 = 0;
    if (builder.directory.items.len == 0 and range.start.len == 0 and builder.continuation != null) {
        // A bounded all-deleted prefix still needs its own empty block. Using
        // the old suffix's block here would resurrect its retired prefix rows.
        var meta: [16]u8 = @splat(0);
        @memcpy(meta[0..4], "ACB4");
        std.mem.writeInt(u32, meta[4..8], version, .little);
        try publish.put(try blockKey(scratch, manifest.generation, builder.blocks, null), try checked(scratch, &meta));
        try publish.put(range.key, try directoryValue(scratch, builder.blocks, builder.continuation.?));
        try publish.put(try candidateKey(scratch, manifest.generation, ""), try directoryValue(scratch, builder.blocks, ""));
        try publish.put(try mergeQueueKey(scratch, manifest.generation, ""), "");
        builder.blocks += 1;
        empty_prefix = 1;
    }
    // Preserve a zero boundary even if the first range became empty, so a
    // later insertion preceding all remaining rows cannot escape the directory.
    if (builder.directory.items.len == 0 and range.start.len == 0 and range.end.len != 0 and builder.continuation == null) {
        var next_directory = try Directory.init(alloc, &read, manifest.generation, range.end);
        defer next_directory.deinit();
        const next_range = (try next_directory.next(scratch)).?;
        try publish.delete(next_range.key);
        try deleteIfPresent(&publish, try candidateKey(scratch, manifest.generation, next_range.start));
        try deleteIfPresent(&publish, try mergeQueueKey(scratch, manifest.generation, next_range.start));
        if (try mergeCandidate(&read, scratch, manifest.generation, next_range)) {
            try publish.put(try candidateKey(scratch, manifest.generation, ""), next_range.value);
            try publish.put(try mergeQueueKey(scratch, manifest.generation, ""), "");
        }
        try publish.put(range.key, next_range.value);
    }
    var next_manifest = manifest;
    next_manifest.blocks = builder.blocks;
    next_manifest.ranges = manifest.ranges - removed_ranges + builder.directory.items.len + retained_suffix + empty_prefix;
    const ready = next_manifest.encode();
    try publish.put(manifest_key, &ready);
    try publish.delete(building_key);
    const inline_cleared = try cleanup.publish(&publish, &token);
    try publish.commit();
    live = false;
    _ = db.relational_column_maintenance.dirty_markers_cleared.fetchAdd(inline_cleared, .monotonic);
    db.core.unlockApplyShared();
    locked = false;
    try finishCleanupQuantum(db, alloc, manifest.generation, namespace);
    if (empty_prefix != 0) _ = db.relational_column_maintenance.blocks_written.fetchAdd(empty_prefix, .monotonic);
    _ = db.relational_column_maintenance.ranges_compacted.fetchAdd(1, .monotonic);
    if (removed_ranges > builder.directory.items.len + retained_suffix + empty_prefix)
        _ = db.relational_column_maintenance.ranges_merged.fetchAdd(removed_ranges - builder.directory.items.len - retained_suffix - empty_prefix, .monotonic);
    return true;
}

fn prunePrefix(db: anytype, alloc: alloc_type, lower: []const u8, namespace: u64) !bool {
    const upper = (try keys.nextPrefixAlloc(alloc, lower)) orelse return error.InvalidColumnSegment;
    defer alloc.free(upper);
    return pruneRange(db, alloc, lower, upper, namespace);
}

fn prune(db: anytype, alloc: alloc_type, generation: u64, namespace: u64) !bool {
    const upper = try std.fmt.allocPrint(alloc, "{s}{x:0>16}:", .{ prefix, generation });
    defer alloc.free(upper);
    return pruneRange(db, alloc, prefix, upper, namespace);
}

fn drainGarbage(db: anytype, alloc: alloc_type, namespace: u64) !bool {
    const token = db.core.store.get(alloc, garbage_key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    defer alloc.free(token);
    if (token.len != 16) return error.InvalidColumnSegment;
    const orphan_prefix = try std.fmt.allocPrint(alloc, "{s}{x:0>16}:{x:0>8}", .{ prefix, std.mem.readInt(u64, token[0..8], .little), std.mem.readInt(u64, token[8..16], .little) >> 32 });
    defer alloc.free(orphan_prefix);
    if (try prunePrefix(db, alloc, orphan_prefix, namespace)) return true;
    db.core.lockApplyShared();
    defer db.core.unlockApplyShared();
    if (namespace != db.core.schemaNamespaceGeneration()) return error.PreparedGenerationChanged;
    var txn = try db.core.store.beginWriteTxn();
    var live = true;
    defer if (live) txn.abort();
    if (try sameValue(&txn, garbage_key, token)) try txn.delete(garbage_key);
    try txn.commit();
    live = false;
    return true;
}

/// Deletion itself is the durable GC cursor. At most one page is collected and
/// committed; the next turn seeks directly to the first remaining record.
fn pruneRange(db: anytype, alloc: alloc_type, lower: []const u8, upper: []const u8, namespace: u64) !bool {
    const Pruner = struct {
        db: @TypeOf(db),
        namespace: u64,
        arena: std.heap.ArenaAllocator,
        deletes: std.ArrayListUnmanaged([]const u8) = .empty,
        bytes: usize = 0,
        deleted: usize = 0,
        fn flush(self: *@This()) !void {
            if (self.deletes.items.len == 0) return;
            self.db.core.lockApplyShared();
            defer self.db.core.unlockApplyShared();
            if (self.namespace != self.db.core.schemaNamespaceGeneration()) return error.PreparedGenerationChanged;
            try self.db.core.store.putBatch(&.{}, self.deletes.items);
            self.deleted += self.deletes.items.len;
            self.deletes = .empty;
            _ = self.arena.reset(.free_all);
        }
        fn visit(ptr: ?*anyopaque, key: []const u8, _: []const u8) !store_mod.DocStore.ScanAction {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            if (self.db.artifact_repair_metadata_stop.load(.acquire)) return error.Canceled;
            const scratch = self.arena.allocator();
            try self.deletes.append(scratch, try scratch.dupe(u8, key));
            self.bytes +|= key.len;
            if (self.deletes.items.len >= maintenance_records or self.bytes >= maintenance_bytes) return .stop;
            return .@"continue";
        }
    };
    var pruner = Pruner{ .db = db, .arena = std.heap.ArenaAllocator.init(alloc), .namespace = namespace };
    defer pruner.arena.deinit();
    try db.core.store.scanWithContext(lower, upper, .{}, &pruner, Pruner.visit);
    try pruner.flush();
    _ = db.relational_column_maintenance.gc_records_deleted.fetchAdd(pruner.deleted, .monotonic);
    return pruner.deleted != 0;
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
    scope: *backend_erased.ReadScope,
    generation: u64,
    index: u64,
    table: schema.TableSchema,
    layout: *const codec.PhysicalLayout,
    rows: []Row,
    ordinal_pages: []const u8,
    values: std.AutoHashMapUnmanaged(u32, *ColumnView) = .empty,
    stats: ?*types.ColumnarScanStats = null,

    const ColumnView = struct {
        bounds: Bounds = .{},
        bitmaps: []const u8 = &.{},
        cells: ?[]?codec.Cell = null,
        logical: [max_rows]?std.json.Value = @splat(null),
        fn present(self: @This(), row: usize) bool {
            return self.bitmaps.len != 0 and self.bitmaps[row / 8] & (@as(u8, 1) << @intCast(row % 8)) != 0;
        }
    };

    fn column(self: *@This(), ordinal: u32) !*ColumnView {
        if (self.values.get(ordinal)) |value| return value;
        const value = try self.alloc.create(ColumnView);
        value.* = .{};
        var low: usize = 0;
        var high = self.ordinal_pages.len / 12;
        while (low < high) {
            const mid = low + (high - low) / 2;
            const page = self.ordinal_pages[mid * 12 ..][0..12];
            const id = std.mem.readInt(u32, page[0..4], .little);
            if (id < ordinal / 64) low = mid + 1 else high = mid;
        }
        if (low < self.ordinal_pages.len / 12) {
            const page = self.ordinal_pages[low * 12 ..][0..12];
            if (std.mem.readInt(u32, page[0..4], .little) == ordinal / 64 and
                std.mem.readInt(u64, page[4..12], .little) & (@as(u64, 1) << @intCast(ordinal % 64)) != 0)
            {
                const meta = try verified(try self.scope.get(try columnMetaKey(self.alloc, self.generation, self.index, ordinal)));
                if (meta.len != 17 + 2 * null_bytes or meta[0] > 1) return error.InvalidColumnSegment;
                value.bounds = .{ .present = meta[0] == 1, .minimum = @bitCast(std.mem.readInt(u64, meta[1..9], .little)), .maximum = @bitCast(std.mem.readInt(u64, meta[9..17], .little)) };
                if (!std.math.isFinite(value.bounds.minimum) or !std.math.isFinite(value.bounds.maximum) or value.bounds.minimum > value.bounds.maximum) return error.InvalidColumnSegment;
                value.bitmaps = meta[17..];
                for (value.bitmaps[0..null_bytes], value.bitmaps[null_bytes..]) |presence, nulls| if (nulls & ~presence != 0) return error.InvalidColumnSegment;
                for (self.rows.len..max_rows) |row| if (value.bitmaps[row / 8] & (@as(u8, 1) << @intCast(row % 8)) != 0) return error.InvalidColumnSegment;
                if (self.stats) |stats| {
                    stats.metadata_bytes_read += meta.len + 4;
                    stats.encoded_bytes_read += meta.len + 4;
                    stats.column_metadata_reads += 1;
                }
            }
        }
        try self.values.put(self.alloc, ordinal, value);
        return value;
    }

    fn cells(self: *@This(), ordinal: u32) ![]?codec.Cell {
        const column_view = try self.column(ordinal);
        if (column_view.cells) |values| return values;
        const values = try self.alloc.alloc(?codec.Cell, self.rows.len);
        @memset(values, null);
        const col = self.table.relational_columns[ordinal];
        if (column_view.bitmaps.len != 0) {
            const bits = column_view.bitmaps;
            const expected: dv.ValueType = switch (self.table.relational_columns[ordinal].column_type) {
                .datetime => .u64_val,
                .integer => .i64_val,
                .number => .f64_val,
                .boolean => .bool_val,
                .geopoint => .geo_point,
                .string, .blob, .geoshape, .json, .dense_vector => .bytes_val,
            };
            var has_values = false;
            for (bits[0..null_bytes], bits[null_bytes..]) |presence, nulls| has_values = has_values or (presence & ~nulls != 0);
            for (values, 0..) |*value, i| if (bits[null_bytes + i / 8] & (@as(u8, 1) << @intCast(i % 8)) != 0) {
                value.* = .{ .ordinal = ordinal, .path = col.path, .value_type = expected, .is_null = true, .value = undefined };
            };
            if (has_values) {
                const key = try blockKey(self.alloc, self.generation, self.index, ordinal);
                const bytes = try verified(try self.scope.get(key));
                if (self.stats) |stats| {
                    stats.columns_read += 1;
                    stats.encoded_bytes_read += bytes.len + 4;
                    stats.payload_bytes_read += bytes.len + 4;
                }
                var reader = try dv.TypedDocValuesReader.init(self.alloc, bytes);
                if (reader.value_type != expected) return error.InvalidColumnSegment;
                for (0..reader.num_chunks) |chunk_index| {
                    var chunk = try reader.decodeChunk(@intCast(chunk_index));
                    // Block arena owns decoded payloads, including borrowed strings.
                    var it = chunk.iterator();
                    while (try it.next()) |entry| {
                        if (entry.doc_id >= values.len or values[entry.doc_id] != null or !column_view.present(entry.doc_id)) return error.InvalidColumnSegment;
                        values[entry.doc_id] = .{ .ordinal = ordinal, .path = col.path, .value_type = reader.value_type, .is_json = col.is_json, .is_dense_vector = col.column_type == .dense_vector, .value = entry.value };
                    }
                }
            }
            for (values, 0..) |value, i| if ((value != null) != column_view.present(i)) return error.InvalidColumnSegment;
        }
        column_view.cells = values;
        return values;
    }

    fn logicalValue(self: *@This(), ordinal: u32, row: usize) !?std.json.Value {
        const value = try self.column(ordinal);
        if (!value.present(row)) return null;
        if (value.logical[row]) |logical| return logical;
        if (value.bitmaps[null_bytes + row / 8] & (@as(u8, 1) << @intCast(row % 8)) != 0) {
            value.logical[row] = .null;
            return .null;
        }
        const cell = (try self.cells(ordinal))[row].?;
        const logical = try codec.ownedJsonValueFromCellAlloc(self.alloc, self.table.relational_columns[ordinal], cell);
        value.logical[row] = logical;
        if (self.stats) |stats| stats.values_materialized += 1;
        return logical;
    }

    fn evaluate(self: *@This(), filter: graph.CompiledPatternFilter, candidates: []const bool, out: []bool) !void {
        @memset(out, false);
        if (std.mem.indexOfScalar(bool, candidates, true) == null) return;
        switch (filter) {
            .match_all => @memcpy(out, candidates),
            .match_none => @memset(out, false),
            .doc_id => |ids| for (self.rows, candidates, out) |row, candidate, *matched| {
                matched.* = false;
                if (!candidate) continue;
                for (ids) |id| if (std.mem.eql(u8, id, row.key)) {
                    matched.* = true;
                    break;
                };
            },
            .field_matcher => |matcher| {
                const name = flatPath(matcher.path).?;
                const ordinal: u32 = @intCast(self.layout.ordinalForName(self.table.relational_columns, name) orelse {
                    const missing = try matcher.predicate.matches(self.alloc, &.{});
                    for (out, candidates) |*matched, candidate| matched.* = candidate and missing;
                    return;
                });
                const column_view = try self.column(ordinal);
                const bounds = column_view.bounds;
                if (bounds.present and !try matcher.predicate.mayMatchNumericBounds(bounds.minimum, bounds.maximum)) return;
                const values = if (matcher.predicate == .exists) null else try self.cells(ordinal);
                for (out, 0..) |*matched, i| {
                    if (!candidates[i]) continue;
                    matched.* = if (values) |typed| blk: {
                        if (typed[i]) |cell| if (!cell.is_null and (cell.is_json or cell.value_type == .geo_point or (cell.is_dense_vector and matcher.predicate != .term and matcher.predicate != .terms))) {
                            const logical = (try self.logicalValue(ordinal, i)).?;
                            break :blk try matcher.predicate.matches(self.alloc, &.{logical});
                        };
                        break :blk try matcher.predicate.matchesCell(self.alloc, self.table.relational_columns[ordinal], typed[i]);
                    } else column_view.present(i);
                }
            },
            .conjuncts, .disjuncts => |items| {
                const conjunction = filter == .conjuncts;
                if (conjunction) @memcpy(out, candidates);
                var buffer: [max_rows]bool = undefined;
                var eligible: [max_rows]bool = undefined;
                // Cheap metadata/scalar leaves precede composite payloads.
                for (0..2) |pass| for (items) |item| {
                    if (self.expensive(item) != (pass == 1)) continue;
                    for (out, candidates, eligible[0..out.len]) |matched, candidate, *value| value.* = candidate and (if (conjunction) matched else !matched);
                    if (std.mem.indexOfScalar(bool, eligible[0..out.len], true) == null) return;
                    try self.evaluate(item, eligible[0..out.len], buffer[0..out.len]);
                    for (out, buffer[0..out.len]) |*value, matched| value.* = if (conjunction) matched else value.* or matched;
                };
            },
            .bool_query => |query| {
                @memcpy(out, candidates);
                var buffer: [max_rows]bool = undefined;
                for (0..2) |pass| for (query.must) |item| {
                    if (self.expensive(item) != (pass == 1)) continue;
                    try self.evaluate(item, out, buffer[0..out.len]);
                    @memcpy(out, buffer[0..out.len]);
                    if (std.mem.indexOfScalar(bool, out, true) == null) return;
                };
                if (query.min_should > 0) {
                    var counts: [max_rows]usize = @splat(0);
                    var unresolved: [max_rows]bool = undefined;
                    for (query.should, 0..) |item, item_index| {
                        for (out, unresolved[0..out.len], 0..) |candidate, *eligible, i| eligible.* = candidate and counts[i] < query.min_should and counts[i] + query.should.len - item_index >= query.min_should;
                        try self.evaluate(item, unresolved[0..out.len], buffer[0..out.len]);
                        for (buffer[0..out.len], 0..) |matched, i| counts[i] += @intFromBool(matched);
                    }
                    for (out, 0..) |*value, i| value.* = value.* and counts[i] >= query.min_should;
                }
                for (query.must_not) |item| {
                    try self.evaluate(item, out, buffer[0..out.len]);
                    for (out, buffer[0..out.len]) |*value, matched| value.* = value.* and !matched;
                }
            },
        }
    }

    fn expensive(self: *@This(), filter: graph.CompiledPatternFilter) bool {
        return switch (filter) {
            .match_all, .match_none, .doc_id => false,
            .field_matcher => |matcher| blk: {
                if (matcher.predicate == .exists or matcher.predicate == .numeric_range) break :blk false;
                const ordinal = self.layout.ordinalForName(self.table.relational_columns, flatPath(matcher.path).?) orelse break :blk false;
                break :blk switch (self.table.relational_columns[ordinal].column_type) {
                    .json, .dense_vector, .geoshape => true,
                    else => false,
                };
            },
            else => true,
        };
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
    const lower_key = if (std.mem.order(u8, from, byte_range.start) == .gt) from else byte_range.start;
    var directory = try Directory.init(alloc, txn, manifest.generation, lower_key);
    defer directory.deinit();
    var dirty_ranges = try DirtyRanges.init(txn, alloc, lower_key);
    defer dirty_ranges.cursor.close();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var had_range = false;
    while (try directory.next(arena.allocator())) |range| {
        defer _ = arena.reset(.free_all);
        had_range = true;
        const index = range.block;
        if (opts.cancellation) |token| if (token.isCancelled()) return error.Canceled;
        if (opts.execution_deadline_ns) |deadline| if (platform_time.monotonicNs() >= deadline) return error.DeadlineExceeded;
        if (to.len != 0 and (if (opts.exclusive_to) std.mem.order(u8, range.start, to) != .lt else std.mem.order(u8, range.start, to) == .gt)) return true;
        if (byte_range.end.len != 0 and std.mem.order(u8, range.start, byte_range.end) != .lt) return true;
        const dirty_range = try dirty_ranges.overlaps(alloc, range.start, range.end);
        if (dirty_range) {
            if (opts.columnar_stats) |stats| stats.dirty_ranges_read += 1;
        }
        const scratch = arena.allocator();
        var scope = try txn.openReadScope(scratch);
        defer scope.close();
        const key = try blockKey(scratch, manifest.generation, index, null);
        const meta = try verified(try scope.get(key));
        if (opts.columnar_stats) |stats| {
            stats.blocks_read += 1;
            stats.encoded_bytes_read += meta.len + 4;
            stats.metadata_bytes_read += meta.len + 4;
        }
        var decoder = Decoder{ .bytes = meta };
        if (!std.mem.eql(u8, try decoder.take(4), "ACB4")) return error.InvalidColumnSegment;
        const version = try decoder.int(u32);
        const rows_len = try decoder.int(u32);
        if (rows_len > max_rows) return error.InvalidColumnSegment;
        if (dirty_range and try preferPrimaryScan(txn, scratch, range, from, to, byte_range, opts, rows_len)) {
            if (opts.columnar_stats) |stats| stats.dense_delta_scans += 1;
            try scanPrimaryRange(db, alloc, txn, range.start, range.end, from, to, byte_range, opts, visitor, ttl_ns, now_ns, progress, filter);
            if (opts.limit > 0 and progress.delivered >= opts.limit) return true;
            continue;
        }
        const pages_len = try decoder.int(u32);
        if (pages_len > decoder.bytes.len / 12 or (rows_len == 0 and pages_len != 0)) return error.InvalidColumnSegment;
        const ordinal_pages = try decoder.take(@as(usize, pages_len) * 12);
        const rows = try scratch.alloc(Row, rows_len);
        for (rows) |*row| {
            row.key = try decoder.take(try decoder.int(u32));
            @memcpy(&row.hash, try decoder.take(32));
            row.timestamp = try decoder.int(u64);
        }
        if (decoder.bytes.len != 0) return error.InvalidColumnSegment;
        var view = (try db.core.acquireSchemaVersionView(version)) orelse return error.UnknownSchemaVersion;
        defer view.release();
        for (0..pages_len) |i| {
            const page = ordinal_pages[i * 12 ..][0..12];
            const id = std.mem.readInt(u32, page[0..4], .little);
            const mask = std.mem.readInt(u64, page[4..12], .little);
            if (mask == 0 or @as(u64, id) * 64 + 63 - @clz(mask) >= view.tableSchema().relational_columns.len) return error.InvalidColumnSegment;
            if (i > 0 and id <= std.mem.readInt(u32, ordinal_pages[(i - 1) * 12 ..][0..4], .little)) return error.InvalidColumnSegment;
        }
        var block = Block{ .alloc = scratch, .scope = &scope, .generation = manifest.generation, .index = index, .table = view.tableSchema().*, .layout = view.physicalLayout(), .rows = rows, .ordinal_pages = ordinal_pages, .stats = opts.columnar_stats };
        var matched: [max_rows]bool = @splat(true);
        var candidates: [max_rows]bool = @splat(false);
        for (rows, 0..) |row, i| candidates[i] = std.mem.order(u8, row.key, range.start) != .lt and (range.end.len == 0 or std.mem.order(u8, row.key, range.end) == .lt) and eligibleRow(row, from, to, byte_range, opts, ttl_ns, now_ns);
        @memcpy(matched[0..rows.len], candidates[0..rows.len]);
        if (filter) |value| try block.evaluate(value.compiled, candidates[0..rows.len], matched[0..rows.len]);
        if (opts.columnar_stats) |stats| if (std.mem.indexOfScalar(bool, matched[0..rows.len], true) == null) {
            stats.blocks_pruned += 1;
        };
        for (rows, 0..) |row, i| {
            if (opts.cancellation) |token| if (token.isCancelled()) return error.Canceled;
            if (opts.execution_deadline_ns) |deadline| if (platform_time.monotonicNs() >= deadline) return error.DeadlineExceeded;
            // A retained suffix block may contain retired prefix rows. Never
            // merge across the current directory range's boundary.
            if (std.mem.order(u8, row.key, range.start) == .lt or (range.end.len != 0 and std.mem.order(u8, row.key, range.end) != .lt)) continue;
            const replaced = try dirty_ranges.emitThrough(db, alloc, txn, row.key, true, from, to, byte_range, opts, visitor, ttl_ns, now_ns, progress, filter);
            if (opts.limit > 0 and progress.delivered >= opts.limit) return true;
            if (replaced) continue;
            if (!matched[i] or !byte_range.contains(row.key) or std.mem.order(u8, row.key, from) == .lt or
                (from.len != 0 and !opts.inclusive_from and std.mem.eql(u8, row.key, from))) continue;
            if (to.len != 0 and (if (opts.exclusive_to) std.mem.order(u8, row.key, to) != .lt else std.mem.order(u8, row.key, to) == .gt)) continue;
            if (ttl_ns != 0 and row.timestamp != 0 and @import("../ttl.zig").isExpired(row.timestamp, ttl_ns, now_ns)) continue;
            var projected: ?[]u8 = null;
            if (opts.include_documents) {
                var object = std.json.ObjectMap.empty;
                for (opts.fields) |field| {
                    const ordinal: u32 = @intCast(block.layout.ordinalForName(block.table.relational_columns, field) orelse continue);
                    if (try block.logicalValue(ordinal, i)) |value| try object.put(scratch, field, value);
                }
                projected = try std.json.Stringify.valueAlloc(scratch, std.json.Value{ .object = object }, .{});
            }
            try deliver(alloc, row, projected, opts, visitor, progress);
            if (opts.limit > 0 and progress.delivered >= opts.limit) return true;
        }
        _ = try dirty_ranges.emitThrough(db, alloc, txn, range.end, false, from, to, byte_range, opts, visitor, ttl_ns, now_ns, progress, filter);
        if (opts.limit > 0 and progress.delivered >= opts.limit) return true;
    }
    if (!had_range) {
        if (manifest.ranges != 0) return error.InvalidColumnSegment;
        try scanPrimaryRange(db, alloc, txn, "", "", from, to, byte_range, opts, visitor, ttl_ns, now_ns, progress, filter);
    }
    return true;
}

fn eligibleRow(row: Row, from: []const u8, to: []const u8, byte_range: types.ByteRange, opts: types.ScanOptions, ttl_ns: u64, now_ns: u64) bool {
    if (!byte_range.contains(row.key) or std.mem.order(u8, row.key, from) == .lt or (from.len != 0 and !opts.inclusive_from and std.mem.eql(u8, row.key, from))) return false;
    if (to.len != 0 and (if (opts.exclusive_to) std.mem.order(u8, row.key, to) != .lt else std.mem.order(u8, row.key, to) == .gt)) return false;
    return ttl_ns == 0 or row.timestamp == 0 or !@import("../ttl.zig").isExpired(row.timestamp, ttl_ns, now_ns);
}

fn deliver(alloc: alloc_type, row: Row, projected: ?[]const u8, opts: types.ScanOptions, visitor: types.ScanVisitor, progress: *Progress) !void {
    try progress.last_key.ensureTotalCapacity(alloc, row.key.len);
    visitor.visit(visitor.context, .{ .id = row.key, .hash = std.mem.readInt(u64, row.hash[0..8], .little), .content_hash = if (opts.include_content_hashes) row.hash else null, .document_json = projected }) catch |err| {
        progress.callback_failed = true;
        return err;
    };
    progress.last_key.clearRetainingCapacity();
    progress.last_key.appendSliceAssumeCapacity(row.key);
    progress.delivered += 1;
    if (opts.columnar_stats) |stats| stats.rows_selected += 1;
}

fn scanPrimaryRange(db: anytype, alloc: alloc_type, txn: *store_mod.DocStore.Txn, range_start: []const u8, range_end: []const u8, from: []const u8, to: []const u8, byte_range: types.ByteRange, opts: types.ScanOptions, visitor: types.ScanVisitor, ttl_ns: u64, now_ns: u64, progress: *Progress, filter: ?*const graph.PreparedPatternFilter) !void {
    const Context = struct {
        db: @TypeOf(db),
        alloc: alloc_type,
        arena: std.heap.ArenaAllocator,
        from: []const u8,
        to: []const u8,
        byte_range: types.ByteRange,
        opts: types.ScanOptions,
        visitor: types.ScanVisitor,
        ttl_ns: u64,
        now_ns: u64,
        progress: *Progress,
        filter: ?*const graph.PreparedPatternFilter,
        view: ?registry.SchemaView = null,
        fn visit(ptr: ?*anyopaque, key: []const u8, value: []const u8) !store_mod.DocStore.ScanAction {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            if (self.opts.cancellation) |token| if (token.isCancelled()) return error.Canceled;
            if (self.opts.execution_deadline_ns) |deadline| if (platform_time.monotonicNs() >= deadline) return error.DeadlineExceeded;
            if (!keys.isRelationalRowKey(key)) return .@"continue";
            const scratch = self.arena.allocator();
            defer _ = self.arena.reset(.retain_capacity);
            const id = (try keys.decodeStoredDocumentRowKeyAlloc(scratch, key)).?;
            // Key bounds precede schema lookup and checksum/decode: damage in
            // a neighboring row must not fail this bounded query.
            if (!self.byte_range.contains(id) or std.mem.order(u8, id, self.from) == .lt or
                (self.from.len != 0 and !self.opts.inclusive_from and std.mem.eql(u8, id, self.from))) return .@"continue";
            if (self.to.len != 0 and (if (self.opts.exclusive_to) std.mem.order(u8, id, self.to) != .lt else std.mem.order(u8, id, self.to) == .gt)) return .stop;
            if (self.opts.columnar_stats) |stats| stats.primary_rows_read += 1;
            const version = try codec.rowSchemaVersion(value);
            if (self.view == null or self.view.?.version() != version) {
                if (self.view) |*view| view.release();
                self.view = null;
                self.view = (try self.db.core.acquireSchemaVersionView(version)) orelse return error.UnknownSchemaVersion;
            }
            const view = self.view.?;
            const typed = if (self.db.core.store.valuesAreAuthenticated())
                try codec.ordinalRowViewTrusted(value, view.tableSchema().*, view.physicalLayout())
            else
                try codec.ordinalRowView(value, view.tableSchema().*, view.physicalLayout());
            const row = Row{ .key = id, .hash = typed.semanticHash(), .timestamp = typed.writeTimestampNs() };
            if (!eligibleRow(row, self.from, self.to, self.byte_range, self.opts, self.ttl_ns, self.now_ns)) return .@"continue";
            if (self.filter) |compiled| {
                const matches = (try compiled.compiled.matchesOrdinal(scratch, row.key, typed)) orelse blk: {
                    var logical = try typed.materializeRootAlloc(scratch);
                    defer logical.deinit(scratch);
                    break :blk try compiled.compiled.matches(scratch, row.key, logical.root);
                };
                if (!matches) return .@"continue";
            }
            const projected = if (self.opts.include_documents) try codec.projectOrdinalFieldsTrustedWithLayoutAlloc(scratch, value, view.tableSchema().*, view.physicalLayout(), self.opts.fields) else null;
            try deliver(self.alloc, row, projected, self.opts, self.visitor, self.progress);
            return if (self.opts.limit > 0 and self.progress.delivered >= self.opts.limit) .stop else .@"continue";
        }
    };
    var context = Context{ .db = db, .alloc = alloc, .arena = std.heap.ArenaAllocator.init(alloc), .from = from, .to = to, .byte_range = byte_range, .opts = opts, .visitor = visitor, .ttl_ns = ttl_ns, .now_ns = now_ns, .progress = progress, .filter = filter };
    defer context.arena.deinit();
    defer if (context.view) |*view| view.release();
    var lower_raw = if (std.mem.order(u8, range_start, from) == .lt) from else range_start;
    if (std.mem.order(u8, lower_raw, byte_range.start) == .lt) lower_raw = byte_range.start;
    var upper_raw: ?[]const u8 = if (range_end.len != 0) range_end else null;
    var inclusive_upper = false;
    if (byte_range.end.len != 0 and (upper_raw == null or std.mem.order(u8, byte_range.end, upper_raw.?) == .lt)) upper_raw = byte_range.end;
    if (to.len != 0 and (upper_raw == null or std.mem.order(u8, to, upper_raw.?) == .lt)) {
        upper_raw = to;
        inclusive_upper = !opts.exclusive_to;
    }
    if (upper_raw) |end| {
        const order = std.mem.order(u8, lower_raw, end);
        if (order == .gt or (order == .eq and !inclusive_upper)) return;
    }
    const lower = try keys.documentRangeLowerAlloc(alloc, lower_raw);
    defer alloc.free(lower);
    const upper = if (upper_raw) |end| if (inclusive_upper) blk: {
        const exact = try keys.documentExactPrefixAlloc(alloc, end);
        defer alloc.free(exact);
        break :blk (try keys.nextPrefixAlloc(alloc, exact)) orelse return error.InvalidColumnSegment;
    } else try keys.documentRangeLowerAlloc(alloc, end) else null;
    defer if (upper) |bytes| alloc.free(bytes);
    try db.core.store.scanReadTxnWithContext(txn, lower, upper orelse "", .{}, &context, Context.visit);
}
