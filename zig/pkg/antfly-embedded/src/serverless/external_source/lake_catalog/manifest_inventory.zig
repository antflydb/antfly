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

//! Immutable Avro inventories packed into authenticated, range-readable chunks.
//! Cache storage may be a private maintenance lane distinct from the source.
const std = @import("std");
const storage = @import("objectstore");
const types = @import("types.zig");
const rows = @import("row_commit.zig");
const avro = @import("../iceberg_avro.zig");
const A = std.mem.Allocator;
const Digest = [32]u8;
const ChunkRef = struct { hash: Digest, start: usize, count: usize, offset: usize, bytes: usize };
pub const Inventory = struct { version: u8 = 1, uri: []const u8, manifest_list: bool, pack: Digest, chunks: []const ChunkRef, count: usize };
const Chunk = struct { paths: []const []const u8 };
pub const max_source_bytes = 16 * 1024 * 1024;
pub const BootstrapBudget = struct {
    pub const credit_bytes = 4 * max_source_bytes;
    pub const max_sources = 64;
    pub const plain_min_credit = 1024 * 1024;
    remaining_sources: usize = max_sources,
    remaining_credit: usize = credit_bytes,

    pub fn exhausted(self: BootstrapBudget) bool {
        return self.remaining_sources == 0 or self.remaining_credit == 0;
    }
    fn validate(self: BootstrapBudget) !void {
        if (self.remaining_sources > max_sources or self.remaining_credit > credit_bytes) return error.InvalidLakeReachability;
    }
    fn charge(self: *BootstrapBudget, bytes: []const u8) !bool {
        // Compressed containers keep the former four-object allowance: small
        // encoded bodies do not imply small decoded blocks. Plain containers
        // share the same 64 MiB credit, with a minimum charge for per-file work.
        const cost = if (try avro.isUncompressedContainer(bytes)) @max(bytes.len, plain_min_credit) else max_source_bytes;
        if (cost > self.remaining_credit) {
            self.remaining_credit = 0;
            return false;
        }
        self.remaining_credit -= cost;
        return true;
    }
};

pub const Store = struct {
    files: rows.Files,
    client: storage.Client,
    bucket: []const u8,
    prefix: []const u8,
    source_bytes_read: usize = 0,
    bytes_read: usize = 0,
    fn key(self: Store, a: A, name: []const u8) ![]u8 {
        return std.fmt.allocPrint(a, "{s}/{s}", .{ self.prefix, name });
    }
    fn get(self: *Store, a: A, key_: []const u8, limit: usize) !?storage.GetResult {
        try self.files.context.ensureActive();
        var client = self.client;
        client.allocator = a;
        const result = client.getObject(self.bucket, key_, .{ .skip_metadata_probe = true, .max_response_bytes = limit, .cancellation = types.contextCancellation(&self.files.context) }) catch |err| switch (err) {
            error.ObjectNotFound, error.FileNotFound => return null,
            else => return err,
        };
        self.bytes_read += result.body.len;
        return result;
    }
    fn immutable(self: *Store, a: A, key_: []const u8, bytes: []const u8) !void {
        try self.files.context.ensureActive();
        var client = self.client;
        client.allocator = a;
        var stored = client.putObject(self.bucket, key_, bytes, .{ .if_none_match = true, .cancellation = types.contextCancellation(&self.files.context) }) catch |err| {
            var existing = (try self.get(a, key_, @max(bytes.len, 64 * 1024))) orelse return err;
            defer existing.deinit(a);
            if (!std.mem.eql(u8, existing.body, bytes)) return error.InvalidLakeReachability;
            return;
        };
        stored.deinit(a);
    }
    pub fn load(self: *Store, a: A, uri: []const u8, manifest_list: bool, sources: *BootstrapBudget) !?Inventory {
        try sources.validate();
        const path = try self.key(a, try std.fmt.allocPrint(a, "inventories/{s}-{s}.json", .{ types.digestHex(uri), if (manifest_list) "list" else "manifest" }));
        if (try self.get(a, path, types.max_metadata_bytes)) |value| {
            var object = value;
            defer object.deinit(a);
            const result = try std.json.parseFromSliceLeaky(Inventory, a, object.body, .{ .allocate = .alloc_always });
            if (result.version != 1 or result.manifest_list != manifest_list or !std.mem.eql(u8, result.uri, uri) or result.chunks.len > 65536) return error.InvalidLakeReachability;
            var count: usize = 0;
            var offset: usize = 0;
            for (result.chunks) |chunk| {
                if (chunk.start != count or chunk.count == 0 or chunk.count > 256 or chunk.offset != offset or chunk.bytes == 0 or chunk.bytes > 2 * 1024 * 1024) return error.InvalidLakeReachability;
                count = std.math.add(usize, count, chunk.count) catch return error.InvalidLakeReachability;
                offset = std.math.add(usize, offset, chunk.bytes) catch return error.InvalidLakeReachability;
                if (offset > 64 * 1024 * 1024) return error.InvalidLakeReachability;
            }
            if (count != result.count) return error.InvalidLakeReachability;
            return result;
        }
        // One bounded Avro object is the indivisible bootstrap unit. The caller
        // bounds source reads; a durable receipt avoids reparsing that source
        // on cold resumes.
        if (sources.exhausted()) return null;
        sources.remaining_sources -= 1;
        const limit = @min(max_source_bytes, sources.remaining_credit);
        const bytes = rows.readLimited(a, self.files, uri, limit) catch |err| {
            if (err == error.ResponseTooLarge and limit < max_source_bytes) {
                // This valid object may only be too large for the remainder
                // of this turn. Leave its cursor untouched for a fresh budget.
                sources.remaining_credit = 0;
                return null;
            }
            return err;
        };
        self.source_bytes_read += bytes.len;
        self.bytes_read += bytes.len;
        const admitted = try sources.charge(bytes);
        try self.files.context.ensureActive();
        if (!admitted) return null;
        var paths: std.ArrayList([]const u8) = .empty;
        if (manifest_list) {
            const list = try avro.parseManifestListAlloc(a, bytes);
            for (list.entries) |entry| try paths.append(a, entry.manifest_path);
        } else {
            try paths.append(a, uri);
            const manifest = try avro.parseDataManifestAlloc(a, bytes);
            for (manifest.entries) |entry| if (entry.status != .deleted) try paths.append(a, entry.file_path);
        }
        var chunks: std.ArrayList(ChunkRef) = .empty;
        var bundle: std.ArrayList(u8) = .empty;
        var start: usize = 0;
        while (start < paths.items.len) {
            var end = start;
            var size: usize = 0;
            while (end < paths.items.len and end - start < 256) : (end += 1) {
                const item = paths.items[end];
                if (item.len == 0 or item.len > 4096) return error.InvalidLakeReachability;
                if (size + item.len > 256 * 1024 and end != start) break;
                size += item.len;
            }
            const encoded = try std.json.Stringify.valueAlloc(a, Chunk{ .paths = paths.items[start..end] }, .{});
            var hash: Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(encoded, &hash, .{});
            if (encoded.len + bundle.items.len > 64 * 1024 * 1024) return error.LakeReachabilityBudgetExceeded;
            try chunks.append(a, .{ .hash = hash, .start = start, .count = end - start, .offset = bundle.items.len, .bytes = encoded.len });
            try bundle.appendSlice(a, encoded);
            start = end;
        }
        // Pack the authenticated chunks in one immutable object. Even an
        // inventory with thousands of chunks takes one PUT; cold workers read
        // just the chunk containing their persisted cursor using a range GET.
        var pack: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(bundle.items, &pack, .{});
        try self.immutable(a, try self.key(a, try std.fmt.allocPrint(a, "chunks/{s}.bin", .{std.fmt.bytesToHex(pack, .lower)})), bundle.items);
        const result: Inventory = .{ .uri = uri, .manifest_list = manifest_list, .pack = pack, .chunks = chunks.items, .count = paths.items.len };
        try self.immutable(a, path, try std.json.Stringify.valueAlloc(a, result, .{}));
        return result;
    }
    pub const Reader = struct {
        store: *Store,
        a: A,
        inventory_: Inventory,
        chunk: usize = 0,
        offset: usize = 0,
        paths: []const []const u8 = &.{},
        /// Chunk inventories use at most 256 entries, but long paths can split
        /// earlier. Seek in the small header, not preceding data pages.
        pub fn next(self: *Reader, position: usize) !?[]const u8 {
            if (position >= self.inventory_.count) return null;
            while (self.offset + self.paths.len <= position) {
                var low = self.chunk;
                var high = self.inventory_.chunks.len;
                while (low < high) {
                    const middle = low + (high - low) / 2;
                    const candidate = self.inventory_.chunks[middle];
                    if (candidate.start + candidate.count <= position) low = middle + 1 else high = middle;
                }
                self.chunk = low;
                if (self.chunk >= self.inventory_.chunks.len) return error.InvalidLakeReachability;
                const ref = self.inventory_.chunks[self.chunk];
                if (ref.start > position) return error.InvalidLakeReachability;
                self.offset = ref.start;
                const hash = ref.hash;
                const key_ = try self.store.key(self.a, try std.fmt.allocPrint(self.a, "chunks/{s}.bin", .{std.fmt.bytesToHex(self.inventory_.pack, .lower)}));
                try self.store.files.context.ensureActive();
                var client = self.store.client;
                client.allocator = self.a;
                var object = try client.getObject(self.store.bucket, key_, .{ .skip_metadata_probe = true, .range = .{ .offset = ref.offset, .length = ref.bytes }, .max_response_bytes = ref.bytes, .cancellation = types.contextCancellation(&self.store.files.context) });
                defer object.deinit(self.a);
                self.store.bytes_read += object.body.len;
                if (!std.mem.eql(u8, &types.digestHex(object.body), &std.fmt.bytesToHex(hash, .lower))) return error.InvalidLakeReachability;
                const parsed = try std.json.parseFromSliceLeaky(Chunk, self.a, object.body, .{ .allocate = .alloc_always });
                if (parsed.paths.len == 0 or parsed.paths.len > 256 or parsed.paths.len != ref.count) return error.InvalidLakeReachability;
                self.paths = parsed.paths;
                self.chunk += 1;
            }
            return self.paths[position - self.offset];
        }
    };
};

fn emptyListFixture(a: A, codec: []const u8) ![]u8 {
    const writer = @import("avro_writer.zig");
    const plain = try writer.ocf(a, writer.list_schema, &.{}, &.{});
    defer a.free(plain);
    const tag = "\x14avro.codec";
    const old = tag ++ "\x08null";
    const offset = std.mem.indexOf(u8, plain, old) orelse return error.TestUnexpectedResult;
    var bytes: std.ArrayList(u8) = .empty;
    try bytes.appendSlice(a, plain[0 .. offset + tag.len]);
    try bytes.append(a, @intCast(codec.len * 2));
    try bytes.appendSlice(a, codec);
    try bytes.appendSlice(a, plain[offset + old.len ..]);
    return bytes.toOwnedSlice(a);
}

test "external lake bootstrap budget admits small plain cohorts and retains compressed allowance" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    for ([_][]const u8{ "null", "deflate", "snappy", "zstandard" }) |codec| {
        const body = try emptyListFixture(scratch, codec);
        var parsed = try avro.parseManifestListAlloc(a, body);
        parsed.deinit(a);
        var store: Store = .{ .files = .{ .client = client, .bucket = "archive", .prefix = "events", .uri = "gs://archive/events" }, .client = client, .bucket = "archive", .prefix = try std.fmt.allocPrint(scratch, "proof-{s}", .{codec}) };
        var budget: BootstrapBudget = .{};
        var loaded: usize = 0;
        for (0..10) |i| {
            const key_ = try std.fmt.allocPrint(scratch, "events/metadata/{s}-{d}.avro", .{ codec, i });
            var result = try client.putObject("archive", key_, body, .{});
            result.deinit(a);
            if (try store.load(scratch, try std.fmt.allocPrint(scratch, "gs://archive/{s}", .{key_}), true, &budget)) |_| loaded += 1;
        }
        try std.testing.expectEqual(@as(usize, if (std.mem.eql(u8, codec, "null")) 10 else 4), loaded);
        try std.testing.expect(store.source_bytes_read <= BootstrapBudget.credit_bytes);
        if (!std.mem.eql(u8, codec, "null")) try std.testing.expect(budget.exhausted());
    }
}

test "external lake bootstrap budget defers without publishing a partial inventory" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    const body = try emptyListFixture(scratch, "null");
    var put = try client.putObject("archive", "events/metadata/list.avro", body, .{});
    put.deinit(a);
    const uri = "gs://archive/events/metadata/list.avro";
    for ([_]usize{ 1, BootstrapBudget.plain_min_credit / 2 }) |remaining| {
        const prefix = try std.fmt.allocPrint(scratch, "proof-{d}", .{remaining});
        var store: Store = .{ .files = .{ .client = client, .bucket = "archive", .prefix = "events", .uri = "gs://archive/events" }, .client = client, .bucket = "archive", .prefix = prefix };
        var budget: BootstrapBudget = .{ .remaining_credit = remaining };
        try std.testing.expectEqual(@as(?Inventory, null), try store.load(scratch, uri, true, &budget));
        try std.testing.expect(budget.exhausted());
        var objects = try client.listObjects("archive", .{ .prefix = prefix, .max_keys = 100 });
        defer objects.deinit(a);
        try std.testing.expectEqual(@as(usize, 0), objects.entries.len);
        var fresh: BootstrapBudget = .{};
        const recovered = (try store.load(scratch, uri, true, &fresh)).?;
        try std.testing.expectEqual(@as(usize, 0), recovered.count);
    }
}

test "external lake bootstrap budget rejects expanded envelopes before IO" {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var store: Store = .{ .files = .{ .client = memory.client(), .bucket = "archive", .prefix = "events", .uri = "gs://archive/events" }, .client = memory.client(), .bucket = "archive", .prefix = "proof" };
    for ([_]BootstrapBudget{
        .{ .remaining_sources = BootstrapBudget.max_sources + 1 },
        .{ .remaining_credit = BootstrapBudget.credit_bytes + 1 },
    }) |invalid| {
        var budget = invalid;
        try std.testing.expectError(error.InvalidLakeReachability, store.load(a, "gs://archive/events/list.avro", true, &budget));
    }
    try std.testing.expectEqual(@as(u64, 0), memory.operationCount());
}
