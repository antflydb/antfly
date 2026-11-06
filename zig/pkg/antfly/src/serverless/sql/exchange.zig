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

//! One immutable object per map attempt, with independently authenticated
//! partitions. Reducers fetch a small footer and only their partition range.
const std = @import("std");
const local = @import("antfly_local_sources");
const storage = @import("objectstore");
const spill = local.sql_spill;
const operators = local.sql_operators;
const catalog = local.serverless_external_source_mod.lake_catalog;
const state = @import("state.zig");
const A = std.mem.Allocator;
const trailer_bytes = 48;
const entry_bytes = 72;
const block_entry_bytes = 48;
const max_index_bytes = 4 * 1024 * 1024;
pub const max_partitions = 1024;
pub const max_object_bytes = 64 * 1024 * 1024;
const magic = "AFSQLX02";
pub const Block = struct { offset: u64, bytes: u32, rows: u32, hash: [32]u8 };
pub const Partition = struct { offset: u64, bytes: u64, rows: u64, hash: [32]u8, first_block: u32, blocks: u32, retained_bytes: u64 };
pub const Footer = struct { partitions: []const Partition, blocks: []const Block, data_bytes: u64 };
pub const Data = struct {
    store: Store,
    ref: state.Output,
    blocks: []const Block,
    rows: u64,
    offset: usize = 0,
    consumed: u64 = 0,
    // Borrowed from the directory arena, independent of the resettable block
    // arena. Never prefetch outside the requested partition range.
    window: []u8 = &.{},
    window_offset: u64 = 0,
    window_len: usize = 0,
    /// The block arena is separate from the authenticated index's lifetime.
    /// Caller must retire the previous block before advancing.
    pub fn nextBlock(self: *Data, arena: *std.heap.ArenaAllocator, limit: usize) !?spill.ColumnarBlock {
        _ = arena.reset(.free_all);
        return self.next(arena.allocator(), limit);
    }
    pub fn next(self: *Data, a: A, limit: usize) !?spill.ColumnarBlock {
        if (self.offset == self.blocks.len) {
            if (self.consumed != self.rows) return error.InvalidSqlExchange;
            return null;
        }
        const entry = self.blocks[self.offset];
        if (entry.bytes > limit) return error.SqlJoinBudgetExceeded;
        const bytes = try a.alloc(u8, entry.bytes);
        if (entry.bytes <= self.window.len) {
            if (entry.offset < self.window_offset or entry.offset + entry.bytes > self.window_offset + self.window_len) {
                var length: usize = entry.bytes;
                for (self.blocks[self.offset + 1 ..]) |next_block| {
                    if (next_block.bytes > self.window.len - length) break;
                    length += next_block.bytes;
                }
                var object = try self.store.range(a, self.ref, entry.offset, length);
                defer object.deinit(a);
                @memcpy(self.window[0..length], object.body);
                self.window_offset = entry.offset;
                self.window_len = length;
            }
            const begin: usize = @intCast(entry.offset - self.window_offset);
            @memcpy(bytes, self.window[begin..][0..entry.bytes]);
        } else {
            var object = try self.store.range(a, self.ref, entry.offset, entry.bytes);
            defer object.deinit(a);
            @memcpy(bytes, object.body);
        }
        if (!std.mem.eql(u8, &hash(bytes), &entry.hash)) return error.InvalidSqlExchange;
        const block = try spill.decodeColumnarBlockInArena(a, bytes, limit);
        errdefer block.deinit();
        if (block.count() != entry.rows) return error.InvalidSqlExchange;
        self.offset += 1;
        self.consumed = try std.math.add(u64, self.consumed, block.count());
        if (self.consumed > self.rows) return error.InvalidSqlExchange;
        return block;
    }
};
fn hash(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
/// One expiry lane's bounded cleanup result. A truncated, fully expired page
/// needs another pass even when every other lane is empty.
pub const Collection = struct { deleted: usize = 0, more: bool = false };
/// Only a child work quantum may yield. Caller cancellation/deadline and
/// provider failures remain errors; a yielded lane is retried without backoff.
pub fn collectionYield(parent: catalog.types.Context, child: catalog.types.Context, err: anyerror) !bool {
    try parent.ensureActive();
    if (err != error.DeadlineExceeded and err != error.Canceled and err != error.Cancelled) return false;
    return if (child.deadline_ns) |deadline| @import("antfly_platform").time.monotonicNs() >= deadline else false;
}
pub const Store = struct {
    read_cache: ?*@import("read_cache.zig").Cache = null,
    client: storage.Client,
    bucket: []const u8,
    prefix: []const u8,
    context: catalog.types.Context = .{},
    pub const Input = struct { values: local.sql_execution_batch.Batch, ordinals: []const u64 };
    pub fn put(self: Store, a: A, query: state.Digest, expires_ms: u64, partitions: []const []const operators.Row, limit: usize) !state.Output {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const inputs = try arena.allocator().alloc(Input, partitions.len);
        for (partitions, inputs) |rows, *input| {
            const values = try arena.allocator().alloc([]const local.sql_scalar.Datum, rows.len);
            const ordinals = try arena.allocator().alloc(u64, rows.len);
            for (rows, values, ordinals) |row, *cells, *ordinal| {
                cells.* = row.values;
                ordinal.* = row.ordinal;
            }
            input.* = .{ .values = .{ .rows = values }, .ordinals = ordinals };
        }
        return self.putBatches(a, query, expires_ms, inputs, limit);
    }
    pub fn putBatches(self: Store, a: A, query: state.Digest, expires_ms: u64, partitions: []const Input, limit: usize) !state.Output {
        try state.validateDigest(query);
        if (partitions.len == 0 or partitions.len > max_partitions or !std.math.isPowerOfTwo(partitions.len) or limit > max_object_bytes) return error.InvalidSqlExchange;
        var bytes: std.ArrayList(u8) = .empty;
        defer bytes.deinit(a);
        const directory = try a.alloc(u8, partitions.len * entry_bytes);
        defer a.free(directory);
        var total_rows: u64 = 0;
        var block_directory: std.ArrayList(u8) = .empty;
        defer block_directory.deinit(a);
        for (partitions, 0..) |input, index| {
            const rows = input.values;
            if (rows.len() != input.ordinals.len) return error.InvalidSqlExchange;
            try self.context.ensureActive();
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const partition_begin = bytes.items.len;
            const first_block = block_directory.items.len / block_entry_bytes;
            var begin: usize = 0;
            while (begin < rows.len()) {
                _ = scratch.reset(.free_all);
                const end = @min(rows.len(), begin + 256);
                const selection = try scratch.allocator().alloc(usize, end - begin);
                for (selection, begin..) |*position, row| position.* = row;
                const selected = try rows.select(scratch.allocator(), selection);
                const block = try spill.encodeColumnarBatchVersionAlloc(scratch.allocator(), selected, input.ordinals[begin..end], limit, .v2);
                if (block.len > limit -| bytes.items.len) return error.SqlJoinBudgetExceeded;
                var block_entry: [block_entry_bytes]u8 = undefined;
                std.mem.writeInt(u64, block_entry[0..8], bytes.items.len, .little);
                std.mem.writeInt(u32, block_entry[8..12], @intCast(block.len), .little);
                std.mem.writeInt(u32, block_entry[12..16], @intCast(end - begin), .little);
                @memcpy(block_entry[16..48], &hash(block));
                try block_directory.appendSlice(a, &block_entry);
                if (block_directory.items.len + directory.len > max_index_bytes) return error.SqlJoinBudgetExceeded;
                try bytes.appendSlice(a, block);
                begin = end;
            }
            const encoded = bytes.items[partition_begin..];
            if (directory.len + block_directory.items.len + trailer_bytes > limit -| bytes.items.len) return error.SqlJoinBudgetExceeded;
            const entry = directory[index * entry_bytes ..][0..entry_bytes];
            std.mem.writeInt(u64, entry[0..8], partition_begin, .little);
            std.mem.writeInt(u64, entry[8..16], encoded.len, .little);
            std.mem.writeInt(u64, entry[16..24], rows.len(), .little);
            @memcpy(entry[24..56], &hash(encoded));
            std.mem.writeInt(u32, entry[56..60], @intCast(first_block), .little);
            std.mem.writeInt(u32, entry[60..64], @intCast(block_directory.items.len / block_entry_bytes - first_block), .little);
            var retained_bytes: u64 = 0;
            for (0..rows.len()) |row| {
                retained_bytes +|= @sizeOf(operators.Row) + rows.width() * @sizeOf(local.sql_scalar.Datum);
                for (0..rows.width()) |column| retained_bytes +|= try rows.cellBytes(scratch.allocator(), row, column);
            }
            std.mem.writeInt(u64, entry[64..72], retained_bytes, .little);
            total_rows = try std.math.add(u64, total_rows, rows.len());
        }
        const index_begin = bytes.items.len;
        try bytes.appendSlice(a, directory);
        try bytes.appendSlice(a, block_directory.items);
        const index_bytes = bytes.items.len - index_begin;
        const directory_hash = hash(bytes.items[index_begin..]);
        try bytes.appendSlice(a, magic);
        var numbers: [8]u8 = undefined;
        std.mem.writeInt(u32, numbers[0..4], @intCast(partitions.len), .little);
        std.mem.writeInt(u32, numbers[4..8], @intCast(index_bytes), .little);
        try bytes.appendSlice(a, &numbers);
        try bytes.appendSlice(a, &directory_hash);
        const digest = std.fmt.bytesToHex(hash(bytes.items), .lower);
        const key = try std.fmt.allocPrint(a, "{s}/sql-exchange/{d:0>20}/{s}/{s}.bin", .{ self.prefix, expires_ms, query, digest });
        errdefer a.free(key);
        var client = self.client;
        client.allocator = a;
        var result = client.putObject(self.bucket, key, bytes.items, .{ .if_none_match = true, .cancellation = catalog.types.contextCancellation(&self.context) }) catch |err| {
            // Content identity resolves both duplicate uploads and lost ACKs.
            var existing = client.getObject(self.bucket, key, .{ .skip_metadata_probe = true, .max_response_bytes = limit, .cancellation = catalog.types.contextCancellation(&self.context) }) catch return err;
            defer existing.deinit(a);
            if (!std.mem.eql(u8, existing.body, bytes.items)) return error.InvalidSqlExchange;
            return .{ .key = key, .hash = digest, .bytes = bytes.items.len, .rows = total_rows, .partitions = @intCast(partitions.len), .index_bytes = @intCast(index_bytes), .index_hash = std.fmt.bytesToHex(directory_hash, .lower) };
        };
        result.deinit(a);
        return .{ .key = key, .hash = digest, .bytes = bytes.items.len, .rows = total_rows, .partitions = @intCast(partitions.len), .index_bytes = @intCast(index_bytes), .index_hash = std.fmt.bytesToHex(directory_hash, .lower) };
    }
    fn range(self: Store, a: A, ref: state.Output, offset: u64, length: usize) !storage.GetResult {
        try self.context.ensureActive();
        const allowed = try std.fmt.allocPrint(a, "{s}/sql-exchange/", .{self.prefix});
        defer a.free(allowed);
        if (!std.mem.startsWith(u8, ref.key, allowed) or std.mem.indexOf(u8, ref.key[allowed.len..], "..") != null or offset > ref.bytes or length > ref.bytes - offset or ref.bytes > max_object_bytes) return error.InvalidSqlExchange;
        if (self.read_cache) |cache| {
            const whole = ref.bytes <= 16 * 1024;
            // Large windows/indexes bypass residency without extra copies or
            // cache-key allocation. Their provider response is already bounded.
            if (!whole and length > @import("read_cache.zig").Cache.max_entry_bytes) return self.fetchRange(a, ref, offset, length);
            const requested_offset = if (whole) 0 else offset;
            const requested_length = if (whole) @as(usize, @intCast(ref.bytes)) else length;
            const identity = try std.json.Stringify.valueAlloc(a, .{ .bucket = self.bucket, .ref = ref, .offset = requested_offset, .length = requested_length }, .{});
            defer a.free(identity);
            const cache_key = hash(identity);
            const body = (try cache.get(a, cache_key)) orelse blk: {
                var fetched = try self.fetchRange(a, ref, requested_offset, requested_length);
                defer fetched.deinit(a);
                if (whole and !std.mem.eql(u8, &std.fmt.bytesToHex(hash(fetched.body), .lower), &ref.hash)) return error.InvalidSqlExchange;
                cache.put(cache_key, fetched.body);
                break :blk try a.dupe(u8, fetched.body);
            };
            defer a.free(body);
            const relative: usize = @intCast(offset - requested_offset);
            const result = try a.dupe(u8, body[relative..][0..length]);
            errdefer a.free(result);
            const bucket = try a.dupe(u8, self.bucket);
            errdefer a.free(bucket);
            return .{ .body = result, .metadata = .{ .bucket = bucket, .key = try a.dupe(u8, ref.key), .content_length = length } };
        }
        return self.fetchRange(a, ref, offset, length);
    }
    fn fetchRange(self: Store, a: A, ref: state.Output, offset: u64, length: usize) !storage.GetResult {
        var client = self.client;
        client.allocator = a;
        var object = try client.getObject(self.bucket, ref.key, .{ .skip_metadata_probe = true, .range = .{ .offset = offset, .length = length }, .max_response_bytes = length, .cancellation = catalog.types.contextCancellation(&self.context) });
        errdefer object.deinit(a);
        if (object.body.len != length) return error.InvalidSqlExchange;
        return object;
    }
    pub fn footer(self: Store, a: A, ref: state.Output) !Footer {
        if (ref.partitions == 0 or ref.partitions > max_partitions or !std.math.isPowerOfTwo(ref.partitions)) return error.InvalidSqlExchange;
        if (ref.index_bytes < @as(usize, ref.partitions) * entry_bytes or ref.index_bytes > max_index_bytes) return error.InvalidSqlExchange;
        const tail_bytes: usize = @as(usize, ref.index_bytes) + trailer_bytes;
        if (ref.bytes < tail_bytes) return error.InvalidSqlExchange;
        var tail = try self.range(a, ref, ref.bytes - tail_bytes, tail_bytes);
        defer tail.deinit(a);
        const bytes = tail.body[tail.body.len - trailer_bytes ..];
        if (!std.mem.eql(u8, bytes[0..8], magic)) return error.InvalidSqlExchange;
        const count = std.mem.readInt(u32, bytes[8..12], .little);
        const size = std.mem.readInt(u32, bytes[12..16], .little);
        if (count != ref.partitions or size != ref.index_bytes or (size - count * entry_bytes) % block_entry_bytes != 0) return error.InvalidSqlExchange;
        const directory = tail.body[0..size];
        if (ref.index_hash == null or !std.mem.eql(u8, &std.fmt.bytesToHex(hash(directory), .lower), &ref.index_hash.?) or !std.mem.eql(u8, &hash(directory), bytes[16..48])) return error.InvalidSqlExchange;
        const entries = try a.alloc(Partition, count);
        errdefer a.free(entries);
        const blocks = try a.alloc(Block, (size - count * entry_bytes) / block_entry_bytes);
        errdefer a.free(blocks);
        var offset: u64 = 0;
        var rows: u64 = 0;
        var next_block: u32 = 0;
        for (entries, 0..) |*entry, index| {
            const encoded = directory[index * entry_bytes ..][0..entry_bytes];
            entry.* = .{ .offset = std.mem.readInt(u64, encoded[0..8], .little), .bytes = std.mem.readInt(u64, encoded[8..16], .little), .rows = std.mem.readInt(u64, encoded[16..24], .little), .hash = encoded[24..56].*, .first_block = std.mem.readInt(u32, encoded[56..60], .little), .blocks = std.mem.readInt(u32, encoded[60..64], .little), .retained_bytes = std.mem.readInt(u64, encoded[64..72], .little) };
            if (entry.offset != offset or entry.bytes > ref.bytes - trailer_bytes - size -| offset) return error.InvalidSqlExchange;
            if (entry.first_block != next_block or entry.blocks > blocks.len -| next_block) return error.InvalidSqlExchange;
            var block_offset = offset;
            var block_rows: u64 = 0;
            for (blocks[next_block..][0..entry.blocks], next_block..) |*block, block_index| {
                const raw = directory[count * entry_bytes + block_index * block_entry_bytes ..][0..block_entry_bytes];
                block.* = .{ .offset = std.mem.readInt(u64, raw[0..8], .little), .bytes = std.mem.readInt(u32, raw[8..12], .little), .rows = std.mem.readInt(u32, raw[12..16], .little), .hash = raw[16..48].* };
                if (block.offset != block_offset or block.bytes == 0 or block.rows == 0 or block.rows > 256 or block.bytes > entry.bytes -| (block_offset - offset)) return error.InvalidSqlExchange;
                block_offset = try std.math.add(u64, block_offset, block.bytes);
                block_rows = try std.math.add(u64, block_rows, block.rows);
            }
            if (block_offset != offset + entry.bytes or block_rows != entry.rows) return error.InvalidSqlExchange;
            next_block += entry.blocks;
            offset = try std.math.add(u64, offset, entry.bytes);
            rows = try std.math.add(u64, rows, entry.rows);
        }
        if (offset != ref.bytes - trailer_bytes - size or rows != ref.rows or next_block != blocks.len) return error.InvalidSqlExchange;
        return .{ .partitions = entries, .blocks = blocks, .data_bytes = offset };
    }
    /// The returned block owns its bytes through the caller's arena. No JSON
    /// conversion occurs, preserving SQL NULL, exact NUMERIC, arrays and types.
    pub fn read(self: Store, a: A, ref: state.Output, index: usize, limit: usize) !Data {
        return self.readRange(a, ref, index, index + 1, limit);
    }
    pub fn readRange(self: Store, a: A, ref: state.Output, begin: usize, end: usize, limit: usize) !Data {
        const directory = try self.footer(a, ref);
        defer a.free(directory.partitions);
        errdefer a.free(directory.blocks);
        if (begin >= end or end > directory.partitions.len) return error.InvalidSqlExchange;
        const first = directory.partitions[begin];
        const last = directory.partitions[end - 1];
        const blocks = directory.blocks[first.first_block .. last.first_block + last.blocks];
        var rows: u64 = 0;
        for (directory.partitions[begin..end]) |partition| rows = try std.math.add(u64, rows, partition.rows);
        for (blocks) |block| if (block.bytes > limit) return error.SqlJoinBudgetExceeded;
        // Bound read-ahead independently of output size and leave most of the
        // caller's quantum available for decoding. Oversized blocks bypass it.
        const window_bytes = @min(@min(limit / 4, 256 * 1024), last.offset + last.bytes - first.offset);
        const window = try a.alloc(u8, @intCast(window_bytes));
        return .{ .store = self, .ref = ref, .blocks = blocks, .rows = rows, .window = window };
    }
    pub fn collect(self: Store, a: A, now_ms: u64, max_keys: u32) !usize {
        return (try self.collectPage(a, now_ms, max_keys)).deleted;
    }
    pub fn collectPage(self: Store, a: A, now_ms: u64, max_keys: u32) !Collection {
        return self.collectPageWithin(a, now_ms, max_keys, self.context);
    }
    pub fn collectPageWithin(self: Store, a: A, now_ms: u64, max_keys: u32, parent: catalog.types.Context) !Collection {
        if (max_keys == 0 or max_keys > 256) return error.InvalidSqlExchange;
        const prefix = try std.fmt.allocPrint(a, "{s}/sql-exchange/", .{self.prefix});
        defer a.free(prefix);
        var client = self.client;
        client.allocator = a;
        var listed = client.listObjects(self.bucket, .{ .prefix = prefix, .max_keys = max_keys, .cancellation = catalog.types.contextCancellation(&self.context) }) catch |err| {
            if (try collectionYield(parent, self.context, err)) return .{ .more = true };
            return err;
        };
        defer listed.deinit(a);
        var deleted: usize = 0;
        for (listed.entries) |entry| {
            self.context.ensureActive() catch |err| {
                if (try collectionYield(parent, self.context, err)) return .{ .deleted = deleted, .more = true };
                return err;
            };
            if (!std.mem.startsWith(u8, entry.key, prefix) or entry.key.len < prefix.len + 21) return error.InvalidSqlExchange;
            const expiry = std.fmt.parseInt(u64, entry.key[prefix.len..][0..20], 10) catch return error.InvalidSqlExchange;
            if (expiry +| 30_000 >= now_ms) break;
            // Expired content-addressed exchange names can never be a live
            // generation again. A late upload is found by the next expiry walk.
            client.deleteObject(self.bucket, entry.key, .{ .cancellation = catalog.types.contextCancellation(&self.context) }) catch |err| switch (err) {
                // Concurrent collectors and accepted DELETE response loss are
                // replayable: an already absent expired object is reclaimed.
                error.FileNotFound, error.NotFound, error.ObjectNotFound => {},
                else => {
                    if (try collectionYield(parent, self.context, err)) return .{ .deleted = deleted, .more = true };
                    return err;
                },
            };
            deleted += 1;
        }
        return .{ .deleted = deleted, .more = deleted != 0 and deleted == listed.entries.len and listed.next_continuation_token != null };
    }
};

test "distributed SQL exchange authenticates partition ranges preserves typed nulls and expires bounded outputs" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    const store: Store = .{ .client = memory.client(), .bucket = "queries", .prefix = "tenant" };
    const Datum = local.sql_scalar.Datum;
    const cells = [_]Datum{ .{}, Datum.json(.null), Datum.json(.{ .integer = 42 }) };
    const partitions = [_][]const operators.Row{ &.{.{ .values = &cells, .keys = &.{}, .ordinal = 0 }}, &.{} };
    const ref = try store.put(sa, @splat('a'), 1000, &partitions, 1 << 20);
    var data = try store.read(sa, ref, 0, 1 << 20);
    const block = (try data.next(sa, 1 << 20)).?;
    defer block.deinit();
    try std.testing.expect((try block.cell(0, 0)).sql_null);
    try std.testing.expect(!(try block.cell(0, 1)).sql_null);
    try std.testing.expectEqual(@as(i64, 42), (try block.cell(0, 2)).value.integer);
    try std.testing.expectEqual(@as(usize, 0), (try store.read(sa, ref, 1, 1 << 20)).rows);
    try std.testing.expectEqual(@as(usize, 0), try store.collect(sa, 31000, 1));
    try std.testing.expectEqual(@as(usize, 1), try store.collect(sa, 31001, 1));
}

test "distributed SQL exchange streams authenticated bounded blocks for large partitions and rejects corruption" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var faults = storage.ScriptedFaultClient.init(a, memory.client());
    defer faults.deinit();
    const store: Store = .{ .client = faults.client(), .bucket = "queries", .prefix = "tenant" };
    const rows = try sa.alloc(operators.Row, 5000);
    for (rows, 0..) |*row, index| {
        const cells = try sa.alloc(local.sql_scalar.Datum, 2);
        cells[0] = local.sql_scalar.Datum.json(.{ .integer = @intCast(index) });
        cells[1] = local.sql_scalar.Datum.json(.{ .string = "long repeated payload retained in typed columnar blocks" });
        row.* = .{ .values = cells, .keys = &.{}, .ordinal = index };
    }
    const ref = try store.put(sa, @splat('a'), 1000000, &.{ rows, &.{} }, 16 * 1024 * 1024);
    const before = faults.get_attempts;
    try std.testing.expect(ref.bytes > 16 * 1024);
    var data = try store.read(sa, ref, 0, 1024 * 1024);
    try std.testing.expectEqual(@as(u64, 1), faults.get_attempts - before);
    var count: usize = 0;
    while (try data.next(sa, 16 * 1024)) |block| {
        defer block.deinit();
        for (0..block.count()) |index| try std.testing.expectEqual(@as(i64, @intCast(count + index)), (try block.cell(index, 0)).value.integer);
        count += block.count();
    }
    try std.testing.expectEqual(@as(usize, 5000), count);
    try std.testing.expectEqual(@as(u64, 2), faults.get_attempts - before);
    const empty_before = faults.get_attempts;
    try std.testing.expectEqual(@as(u64, 0), (try store.read(sa, ref, 1, 1024)).rows);
    try std.testing.expectEqual(@as(u64, 1), faults.get_attempts - empty_before);
    var client = memory.client();
    var object = try client.getObject("queries", ref.key, .{});
    defer object.deinit(a);
    object.body[0] ^= 1;
    var corrupted = try client.putObject("queries", ref.key, object.body, .{});
    corrupted.deinit(a);
    var corrupted_data = try store.read(sa, ref, 0, 16 * 1024 * 1024);
    try std.testing.expectError(error.InvalidSqlExchange, corrupted_data.next(sa, 16 * 1024 * 1024));
    object.body[object.body.len - 1] ^= 1;
    var bad_footer = try client.putObject("queries", ref.key, object.body, .{});
    bad_footer.deinit(a);
    try std.testing.expectError(error.InvalidSqlExchange, store.footer(sa, ref));
}

test "distributed SQL immutable exchange cache combines small object reads and isolates authenticated identities" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var faults = storage.ScriptedFaultClient.init(a, memory.client());
    defer faults.deinit();
    var cache: @import("read_cache.zig").Cache = .{ .a = a };
    defer cache.deinit();
    const store: Store = .{ .client = faults.client(), .bucket = "queries", .prefix = "tenant", .read_cache = &cache };
    const cells = [_]local.sql_scalar.Datum{local.sql_scalar.Datum.json(.{ .integer = 42 })};
    const ref = try store.put(sa, @splat('a'), 1000000, &.{&.{.{ .values = &cells, .keys = &.{}, .ordinal = 0 }}}, 1024 * 1024);
    const before = faults.get_attempts;
    var data = try store.read(sa, ref, 0, 1024 * 1024);
    const block = (try data.next(sa, 1024 * 1024)).?;
    defer block.deinit();
    try std.testing.expectEqual(@as(i64, 42), (try block.cell(0, 0)).value.integer);
    try std.testing.expectEqual(@as(u64, 1), faults.get_attempts - before);
    _ = try store.footer(sa, ref);
    try std.testing.expectEqual(@as(u64, 1), faults.get_attempts - before);
    var invalid = ref;
    invalid.hash = @splat('b');
    try std.testing.expectError(error.InvalidSqlExchange, store.footer(sa, invalid));
}
