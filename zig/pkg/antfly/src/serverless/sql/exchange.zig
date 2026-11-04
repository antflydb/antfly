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
const entry_bytes = 56;
pub const max_partitions = 1024;
pub const max_object_bytes = 64 * 1024 * 1024;
const magic = "AFSQLX01";
pub const Partition = struct { offset: u64, bytes: u64, rows: u64, hash: [32]u8 };
pub const Footer = struct { partitions: []const Partition, data_bytes: u64 };
pub const Data = struct {
    bytes: []const u8,
    rows: u64,
    offset: usize = 0,
    consumed: u64 = 0,
    pub fn next(self: *Data, a: A, limit: usize) !?spill.ColumnarBlock {
        if (self.offset == self.bytes.len) {
            if (self.consumed != self.rows) return error.InvalidSqlExchange;
            return null;
        }
        if (self.offset > self.bytes.len or self.bytes.len - self.offset < 4) return error.InvalidSqlExchange;
        const count = std.mem.readInt(u32, self.bytes[self.offset..][0..4], .little);
        self.offset += 4;
        if (count > self.bytes.len - self.offset or count > limit) return error.InvalidSqlExchange;
        const block = try spill.decodeColumnarBlockInArena(a, self.bytes[self.offset..][0..count], limit);
        self.offset += count;
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
pub const Store = struct {
    client: storage.Client,
    bucket: []const u8,
    prefix: []const u8,
    context: catalog.types.Context = .{},
    pub fn put(self: Store, a: A, query: state.Digest, expires_ms: u64, partitions: []const []const operators.Row, limit: usize) !state.Output {
        try state.validateDigest(query);
        if (partitions.len == 0 or partitions.len > max_partitions or !std.math.isPowerOfTwo(partitions.len) or limit > max_object_bytes) return error.InvalidSqlExchange;
        var bytes: std.ArrayList(u8) = .empty;
        defer bytes.deinit(a);
        const directory = try a.alloc(u8, partitions.len * entry_bytes);
        defer a.free(directory);
        var total_rows: u64 = 0;
        for (partitions, 0..) |rows, index| {
            try self.context.ensureActive();
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            var blocks: std.ArrayList(u8) = .empty;
            var begin: usize = 0;
            while (begin < rows.len) {
                const end = @min(rows.len, begin + 256);
                const block = try spill.encodeColumnarBlockVersionAlloc(scratch.allocator(), rows[begin..end], limit, .v2);
                if (block.len + 4 > limit -| blocks.items.len) return error.SqlJoinBudgetExceeded;
                var length: [4]u8 = undefined;
                std.mem.writeInt(u32, &length, @intCast(block.len), .little);
                try blocks.appendSlice(scratch.allocator(), &length);
                try blocks.appendSlice(scratch.allocator(), block);
                begin = end;
            }
            const encoded = blocks.items;
            if (encoded.len > limit -| bytes.items.len or directory.len + trailer_bytes > limit -| (bytes.items.len + encoded.len)) return error.SqlJoinBudgetExceeded;
            const entry = directory[index * entry_bytes ..][0..entry_bytes];
            std.mem.writeInt(u64, entry[0..8], bytes.items.len, .little);
            std.mem.writeInt(u64, entry[8..16], encoded.len, .little);
            std.mem.writeInt(u64, entry[16..24], rows.len, .little);
            @memcpy(entry[24..56], &hash(encoded));
            try bytes.appendSlice(a, encoded);
            total_rows = try std.math.add(u64, total_rows, rows.len);
        }
        const directory_hash = hash(directory);
        try bytes.appendSlice(a, directory);
        try bytes.appendSlice(a, magic);
        var numbers: [8]u8 = undefined;
        std.mem.writeInt(u32, numbers[0..4], @intCast(partitions.len), .little);
        std.mem.writeInt(u32, numbers[4..8], @intCast(directory.len), .little);
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
            return .{ .key = key, .hash = digest, .bytes = bytes.items.len, .rows = total_rows, .partitions = @intCast(partitions.len), .index_hash = std.fmt.bytesToHex(directory_hash, .lower) };
        };
        result.deinit(a);
        return .{ .key = key, .hash = digest, .bytes = bytes.items.len, .rows = total_rows, .partitions = @intCast(partitions.len), .index_hash = std.fmt.bytesToHex(directory_hash, .lower) };
    }
    fn range(self: Store, a: A, ref: state.Output, offset: u64, length: usize) !storage.GetResult {
        try self.context.ensureActive();
        const allowed = try std.fmt.allocPrint(a, "{s}/sql-exchange/", .{self.prefix});
        defer a.free(allowed);
        if (!std.mem.startsWith(u8, ref.key, allowed) or std.mem.indexOf(u8, ref.key[allowed.len..], "..") != null or offset > ref.bytes or length > ref.bytes - offset or ref.bytes > max_object_bytes) return error.InvalidSqlExchange;
        var client = self.client;
        client.allocator = a;
        var object = try client.getObject(self.bucket, ref.key, .{ .skip_metadata_probe = true, .range = .{ .offset = offset, .length = length }, .max_response_bytes = length, .cancellation = catalog.types.contextCancellation(&self.context) });
        errdefer object.deinit(a);
        if (object.body.len != length) return error.InvalidSqlExchange;
        return object;
    }
    pub fn footer(self: Store, a: A, ref: state.Output) !Footer {
        if (ref.partitions == 0 or ref.partitions > max_partitions or !std.math.isPowerOfTwo(ref.partitions)) return error.InvalidSqlExchange;
        const tail_bytes: usize = @as(usize, ref.partitions) * entry_bytes + trailer_bytes;
        if (ref.bytes < tail_bytes) return error.InvalidSqlExchange;
        var tail = try self.range(a, ref, ref.bytes - tail_bytes, tail_bytes);
        defer tail.deinit(a);
        const bytes = tail.body[tail.body.len - trailer_bytes ..];
        if (!std.mem.eql(u8, bytes[0..8], magic)) return error.InvalidSqlExchange;
        const count = std.mem.readInt(u32, bytes[8..12], .little);
        const size = std.mem.readInt(u32, bytes[12..16], .little);
        if (count != ref.partitions or size != count * entry_bytes) return error.InvalidSqlExchange;
        const directory = tail.body[0..size];
        if (ref.index_hash == null or !std.mem.eql(u8, &std.fmt.bytesToHex(hash(directory), .lower), &ref.index_hash.?) or !std.mem.eql(u8, &hash(directory), bytes[16..48])) return error.InvalidSqlExchange;
        const entries = try a.alloc(Partition, count);
        errdefer a.free(entries);
        var offset: u64 = 0;
        var rows: u64 = 0;
        for (entries, 0..) |*entry, index| {
            const encoded = directory[index * entry_bytes ..][0..entry_bytes];
            entry.* = .{ .offset = std.mem.readInt(u64, encoded[0..8], .little), .bytes = std.mem.readInt(u64, encoded[8..16], .little), .rows = std.mem.readInt(u64, encoded[16..24], .little), .hash = encoded[24..56].* };
            if (entry.offset != offset or entry.bytes > ref.bytes - trailer_bytes - size -| offset) return error.InvalidSqlExchange;
            offset = try std.math.add(u64, offset, entry.bytes);
            rows = try std.math.add(u64, rows, entry.rows);
        }
        if (offset != ref.bytes - trailer_bytes - size or rows != ref.rows) return error.InvalidSqlExchange;
        return .{ .partitions = entries, .data_bytes = offset };
    }
    /// The returned block owns its bytes through the caller's arena. No JSON
    /// conversion occurs, preserving SQL NULL, exact NUMERIC, arrays and types.
    pub fn read(self: Store, a: A, ref: state.Output, index: usize, limit: usize) !Data {
        const directory = try self.footer(a, ref);
        defer a.free(directory.partitions);
        if (index >= directory.partitions.len) return error.InvalidSqlExchange;
        const entry = directory.partitions[index];
        if (entry.bytes > limit) return error.SqlJoinBudgetExceeded;
        if (entry.bytes == 0) {
            if (entry.rows != 0 or !std.mem.eql(u8, &entry.hash, &hash(""))) return error.InvalidSqlExchange;
            return .{ .bytes = &.{}, .rows = 0 };
        }
        var result = try self.range(a, ref, entry.offset, @intCast(entry.bytes));
        defer result.deinit(a);
        if (!std.mem.eql(u8, &hash(result.body), &entry.hash)) return error.InvalidSqlExchange;
        const owned = try a.dupe(u8, result.body);
        errdefer a.free(owned);
        return .{ .bytes = owned, .rows = entry.rows };
    }
    pub fn collect(self: Store, a: A, now_ms: u64, max_keys: u32) !usize {
        if (max_keys == 0 or max_keys > 256) return error.InvalidSqlExchange;
        const prefix = try std.fmt.allocPrint(a, "{s}/sql-exchange/", .{self.prefix});
        defer a.free(prefix);
        var client = self.client;
        client.allocator = a;
        var listed = try client.listObjects(self.bucket, .{ .prefix = prefix, .max_keys = max_keys, .cancellation = catalog.types.contextCancellation(&self.context) });
        defer listed.deinit(a);
        var deleted: usize = 0;
        for (listed.entries) |entry| {
            if (!std.mem.startsWith(u8, entry.key, prefix) or entry.key.len < prefix.len + 21) return error.InvalidSqlExchange;
            const expiry = std.fmt.parseInt(u64, entry.key[prefix.len..][0..20], 10) catch return error.InvalidSqlExchange;
            if (expiry +| 30_000 >= now_ms) break;
            var metadata = client.statObjectWithOptions(self.bucket, entry.key, .{ .cancellation = catalog.types.contextCancellation(&self.context) }) catch |err| switch (err) {
                error.NotFound, error.ObjectNotFound, error.FileNotFound => continue,
                else => return err,
            };
            defer metadata.deinit(a);
            try client.compareAndDelete(self.bucket, entry.key, metadata.writeRevision() orelse return error.MissingObjectRevision, .{ .cancellation = catalog.types.contextCancellation(&self.context) });
            deleted += 1;
        }
        return deleted;
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

test "distributed SQL exchange uses two bounded range reads for large partitions and rejects corruption" {
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
    var data = try store.read(sa, ref, 0, 16 * 1024 * 1024);
    try std.testing.expectEqual(@as(u64, 2), faults.get_attempts - before);
    var count: usize = 0;
    while (try data.next(sa, 16 * 1024 * 1024)) |block| {
        defer block.deinit();
        for (0..block.count()) |index| try std.testing.expectEqual(@as(i64, @intCast(count + index)), (try block.cell(index, 0)).value.integer);
        count += block.count();
    }
    try std.testing.expectEqual(@as(usize, 5000), count);
    const empty_before = faults.get_attempts;
    try std.testing.expectEqual(@as(u64, 0), (try store.read(sa, ref, 1, 1024)).rows);
    try std.testing.expectEqual(@as(u64, 1), faults.get_attempts - empty_before);
    var client = memory.client();
    var object = try client.getObject("queries", ref.key, .{});
    defer object.deinit(a);
    object.body[0] ^= 1;
    var corrupted = try client.putObject("queries", ref.key, object.body, .{});
    corrupted.deinit(a);
    try std.testing.expectError(error.InvalidSqlExchange, store.read(sa, ref, 0, 16 * 1024 * 1024));
    object.body[object.body.len - 1] ^= 1;
    var bad_footer = try client.putObject("queries", ref.key, object.body, .{});
    bad_footer.deinit(a);
    try std.testing.expectError(error.InvalidSqlExchange, store.footer(sa, ref));
}
