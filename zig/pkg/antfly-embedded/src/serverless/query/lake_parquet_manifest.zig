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

//! Explicit Parquet snapshot commits. The mutable small head is read fresh;
//! immutable checksummed chunks own file/version membership. A prefix without
//! this commit protocol continues to require a complete object listing.
const std = @import("std");
const external = @import("../external_source/types.zig");
const storage = @import("../../storage/object_storage.zig");
const decoded = @import("lake_decoded_cache.zig");
const catalogs = @import("lake_inventory_catalog.zig");
const iceberg = @import("lake_iceberg_snapshot.zig");
const Context = @import("lake_read_context.zig").Context;
const A = std.mem.Allocator;
pub const Chunk = struct { uri: []const u8, sha256: []const u8, byte_len: u64, file_count: u32 };
pub const Head = struct { version: u8, source_id: []const u8, chunks: []const Chunk, file_count: u32 };
pub const File = struct { file_id: []const u8, object_uri: []const u8, byte_len: u64, row_count: u64, etag: []const u8 = "", version_id: []const u8 = "" };
pub const Request = struct {
    client: storage.ObjectStorage,
    source: []const u8,
    uri: []const u8,
    schema: []const u8,
    chunks: ?*decoded.Cache = null,
    catalogs: ?*catalogs.Cache = null,
    scope: [32]u8 = @splat(0),
    context: Context = .{},
};
fn checksum(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
pub fn isManifest(uri: []const u8) bool {
    return std.mem.endsWith(u8, uri, ".parquet-manifest.json");
}
fn parse(a: A, source: []const u8, bytes: []const u8) !std.json.Parsed(Head) {
    if (bytes.len == 0 or bytes.len > 1024 * 1024) return error.ExternalSourceSnapshotTooLarge;
    var result = std.json.parseFromSlice(Head, a, bytes, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidExternalSourceSnapshot,
    };
    errdefer result.deinit();
    const head = result.value;
    if (head.version != 1 or !std.mem.eql(u8, head.source_id, source) or head.chunks.len > 4096 or head.file_count > 1_000_000) return error.InvalidExternalSourceSnapshot;
    var total: u64 = 0;
    var metadata_bytes: u64 = 0;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    defer seen.deinit(a);
    for (head.chunks) |chunk| {
        if (chunk.uri.len == 0 or chunk.byte_len == 0 or chunk.byte_len > 8 * 1024 * 1024 or chunk.sha256.len != 64 or chunk.file_count == 0 or chunk.file_count > 4096) return error.InvalidExternalSourceSnapshot;
        for (chunk.sha256) |c| if (!std.ascii.isDigit(c) and !(c >= 'a' and c <= 'f')) return error.InvalidExternalSourceSnapshot;
        if ((try seen.getOrPut(a, chunk.uri)).found_existing) return error.InvalidExternalSourceSnapshot;
        total += chunk.file_count;
        metadata_bytes += chunk.byte_len;
        // Private inference/planning must have the same bounded shape even
        // when there is no shared decoded-cache allocator to enforce it.
        if (metadata_bytes > 64 * 1024 * 1024) return error.ExternalSourceSnapshotTooLarge;
    }
    if (total != head.file_count) return error.InvalidExternalSourceSnapshot;
    return result;
}
pub fn header(a: A, source: []const u8, uri: []const u8, schema: []const u8, bytes: []const u8) !external.Inventory {
    var parsed = try parse(a, source, bytes);
    defer parsed.deinit();
    var result: external.Inventory = .{ .format = .parquet, .source_id = &.{}, .source_uri = &.{}, .snapshot_id = &.{}, .schema_fingerprint = &.{}, .files = &.{} };
    errdefer result.deinit(a);
    result.source_id = try a.dupe(u8, source);
    result.source_uri = try a.dupe(u8, uri);
    const digest = checksum(bytes);
    result.snapshot_id = try std.fmt.allocPrint(a, "parquet-manifest:{s}", .{std.fmt.bytesToHex(digest, .lower)});
    result.schema_fingerprint = try a.dupe(u8, schema);
    return result;
}
fn chunkKey(request: Request, chunk: Chunk) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("parquet-manifest-chunk-v1");
    hash.update(&request.scope);
    for ([_][]const u8{ request.source, request.uri, request.schema, chunk.uri, chunk.sha256 }) |part| {
        var n: [8]u8 = undefined;
        std.mem.writeInt(u64, &n, part.len, .little);
        hash.update(&n);
        hash.update(part);
    }
    var sizes: [12]u8 = undefined;
    std.mem.writeInt(u64, sizes[0..8], chunk.byte_len, .little);
    std.mem.writeInt(u32, sizes[8..12], chunk.file_count, .little);
    hash.update(&sizes);
    return hash.finalResult();
}
const Loader = struct {
    request: Request,
    chunk: Chunk,
    fn load(raw: *anyopaque, item: *decoded.Item) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        item.payload = .{ .iceberg_chunk = try readChunk(item.budget.allocator(), self.request, self.chunk) };
    }
};
const scheduler = @import("../../sql/parallel_scheduler.zig");
const LoadedChunk = union(enum) {
    serial,
    lease: decoded.Lease,
    private: external.Inventory,
    fn release(self: *LoadedChunk, a: A) void {
        switch (self.*) {
            .serial => {},
            .lease => |lease| lease.release(),
            .private => |*inventory| inventory.deinit(a),
        }
    }
};
fn loadPlanChunk(a: A, request: Request, chunk: Chunk, prior: ?*catalogs.Index) anyerror!LoadedChunk {
    try request.context.ensureActive();
    if (request.chunks) |cache| {
        const key = chunkKey(request, chunk);
        if (prior) |index| if (index.acquireChunk(key)) |lease| return .{ .lease = lease };
        var loader: Loader = .{ .request = request, .chunk = chunk };
        return .{ .lease = try cache.acquire(key, 64 * 1024 * 1024, request.context, .{ .ptr = &loader, .load = Loader.load }) };
    }
    return .{ .private = try readChunk(a, request, chunk) };
}
fn loadPlanChunkBounded(a: A, request: Request, chunk: Chunk, prior: ?*catalogs.Index) anyerror!LoadedChunk {
    // Shared decodes have their own node-wide loading/residency admission.
    if (request.chunks != null) return loadPlanChunk(a, request, chunk, prior);
    var budget: @import("../../sql/memory_budget.zig") = .{ .backing = a, .limit = 8 * 1024 * 1024 };
    return loadPlanChunk(budget.allocator(), request, chunk, prior) catch |err| {
        if (err == error.OutOfMemory and budget.isLocallyExhausted()) return .serial;
        return err;
    };
}
fn readChunk(a: A, request: Request, chunk: Chunk) !external.Inventory {
    try request.context.ensureActive();
    var client = request.client;
    client.allocator = a;
    const bytes = try iceberg.readFullObjectAlloc(a, &client, null, chunk.uri, .iceberg_metadata, chunk.byte_len, 8 * 1024 * 1024);
    defer a.free(bytes);
    const actual = std.fmt.bytesToHex(checksum(bytes), .lower);
    if (!std.mem.eql(u8, &actual, chunk.sha256)) return error.InvalidExternalSourceSnapshot;
    var parsed = std.json.parseFromSlice([]const File, a, bytes, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidExternalSourceSnapshot,
    };
    defer parsed.deinit();
    if (parsed.value.len != chunk.file_count) return error.InvalidExternalSourceSnapshot;
    var inventory: external.Inventory = .{ .format = .parquet, .source_id = &.{}, .source_uri = &.{}, .snapshot_id = &.{}, .schema_fingerprint = &.{}, .files = &.{} };
    errdefer inventory.deinit(a);
    inventory.source_id = try a.dupe(u8, request.source);
    inventory.source_uri = try a.dupe(u8, request.uri);
    inventory.snapshot_id = try a.dupe(u8, "manifest-chunk");
    inventory.schema_fingerprint = try a.dupe(u8, request.schema);
    const files = try a.alloc(external.FileEntry, parsed.value.len);
    var count: usize = 0;
    errdefer {
        for (files[0..count]) |*file| file.deinit(a);
        a.free(files);
    }
    for (parsed.value, files) |file, *out| {
        out.* = try cloneFile(a, .{ .file_id = @constCast(file.file_id), .object_uri = @constCast(file.object_uri), .etag = @constCast(file.etag), .version_id = @constCast(file.version_id), .byte_len = file.byte_len, .row_count = file.row_count, .row_groups = &.{} });
        count += 1;
        try out.validate();
    }
    std.mem.sort(external.FileEntry, files, {}, struct {
        fn less(_: void, l: external.FileEntry, r: external.FileEntry) bool {
            return std.mem.lessThan(u8, l.file_id, r.file_id);
        }
    }.less);
    var view = inventory;
    view.files = files;
    try view.validateAlloc(a);
    inventory.files = files;
    count = 0;
    return inventory;
}
pub fn plan(a: A, request: Request, bytes: []const u8) !iceberg.SnapshotWithDeletePlan {
    try request.context.ensureActive();
    if ((request.chunks != null or request.catalogs != null) and std.mem.allEqual(u8, &request.scope, 0)) return error.InvalidExternalSourceSnapshot;
    if (request.catalogs != null and request.chunks == null) return error.InvalidExternalSourceSnapshot;
    var parsed = try parse(a, request.source, bytes);
    defer parsed.deinit();
    var inventory = try header(a, request.source, request.uri, request.schema, bytes);
    errdefer inventory.deinit(a);
    var leases: std.ArrayList(decoded.Lease) = .empty;
    defer leases.deinit(a);
    defer for (leases.items) |lease| lease.release();
    var chunks: std.ArrayList(catalogs.Chunk) = .empty;
    defer chunks.deinit(a);
    var owned: std.ArrayList(external.Inventory) = .empty;
    defer {
        for (owned.items) |*part| part.deinit(a);
        owned.deinit(a);
    }
    var family_hash = std.crypto.hash.sha2.Sha256.init(.{});
    family_hash.update("parquet-manifest-family-v1");
    family_hash.update(&request.scope);
    for ([_][]const u8{ request.source, request.uri, request.schema }) |part| {
        var n: [8]u8 = undefined;
        std.mem.writeInt(u64, &n, part.len, .little);
        family_hash.update(&n);
        family_hash.update(part);
    }
    const family = family_hash.finalResult();
    const prior = if (request.catalogs) |cache| cache.get(family) else null;
    defer if (prior) |index| index.release();
    var residency: usize = 0;
    // At most three decoded metadata chunks are in flight. The shared I/O
    // scheduler admits 8 MiB per job; shared decodes have independent
    // loading admission and larger private chunks fall back to serial reads.
    // A request allocator may be an arena; synchronize every allocation made
    // while workers are active, including result-list growth on this thread.
    var locked: scheduler.LockedAllocator = .{ .backing = a };
    const work = locked.allocator();
    var offset: usize = 0;
    while (offset < parsed.value.chunks.len) {
        const count = @min(3, parsed.value.chunks.len - offset);
        var tasks: [3]?scheduler.Task(anyerror!LoadedChunk) = @splat(null);
        var ready: [3]?LoadedChunk = @splat(null);
        defer {
            for (&tasks) |*task| if (task.*) |pending| {
                var running = pending;
                if (running.cancel(request.context.io.?)) |result| {
                    var loaded = result;
                    loaded.release(work);
                } else |_| {}
            };
            for (&ready) |*result| if (result.*) |*loaded| loaded.release(work);
        }
        for (parsed.value.chunks[offset..][0..count], 0..) |chunk, slot| {
            try request.context.ensureActive();
            if (request.context.io) |io| tasks[slot] = scheduler.global().submitIoTransient(io, 8 * 1024 * 1024, loadPlanChunkBounded, .{ work, request, chunk, prior });
            if (tasks[slot] == null) ready[slot] = try loadPlanChunkBounded(work, request, chunk, prior);
        }
        // Deterministic head order also defines which provider failure wins.
        for (parsed.value.chunks[offset..][0..count], 0..) |chunk, slot| {
            if (tasks[slot]) |pending| {
                var running = pending;
                tasks[slot] = null;
                ready[slot] = try running.await(request.context.io.?);
            }
            try request.context.ensureActive();
            if (ready[slot].? == .serial) ready[slot] = try loadPlanChunk(work, request, chunk, prior);
            switch (ready[slot].?) {
                .serial => unreachable,
                .lease => |lease| {
                    residency +|= lease.item.budget.live;
                    if (residency > 256 * 1024 * 1024) return error.ExternalSourceSnapshotTooLarge;
                    try leases.append(work, lease);
                    ready[slot] = null;
                    try chunks.append(work, .{ .key = chunkKey(request, chunk), .lease = lease });
                },
                .private => |part| {
                    try owned.append(work, part);
                    ready[slot] = null;
                },
            }
        }
        offset += count;
    }
    if (request.catalogs) |cache| {
        const catalog = cache.build(family, prior, chunks.items, request.context) catch |err| switch (err) {
            error.OutOfMemory => null,
            else => return err,
        };
        if (catalog) |index| {
            errdefer index.release();
            if (index.count() != parsed.value.file_count) return error.InvalidExternalSourceSnapshot;
            return .{ .inventory = inventory, .catalog = index };
        }
    }
    const files = try a.alloc(external.FileEntry, parsed.value.file_count);
    var count: usize = 0;
    errdefer {
        for (files[0..count]) |*file| file.deinit(a);
        a.free(files);
    }
    for (leases.items) |lease| for (lease.item.payload.iceberg_chunk.files) |file| {
        files[count] = try cloneFile(a, file);
        count += 1;
    };
    for (owned.items) |part| for (part.files) |file| {
        files[count] = try cloneFile(a, file);
        count += 1;
    };
    std.mem.sort(external.FileEntry, files, {}, struct {
        fn less(_: void, l: external.FileEntry, r: external.FileEntry) bool {
            return std.mem.lessThan(u8, l.file_id, r.file_id);
        }
    }.less);
    var view = inventory;
    view.files = files;
    try view.validateAlloc(a);
    inventory.files = files;
    count = 0;
    return .{ .inventory = inventory };
}
fn cloneFile(a: A, file: external.FileEntry) !external.FileEntry {
    const id = try a.dupe(u8, file.file_id);
    errdefer a.free(id);
    const uri = try a.dupe(u8, file.object_uri);
    errdefer a.free(uri);
    const etag = try a.dupe(u8, file.etag);
    errdefer a.free(etag);
    return .{ .file_id = id, .object_uri = uri, .etag = etag, .version_id = try a.dupe(u8, file.version_id), .byte_len = file.byte_len, .row_count = file.row_count, .row_groups = &.{} };
}

test "external lake metadata navigation Parquet commits check chunks reuse catalogs and fence membership" {
    const a = std.testing.allocator;
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("parquet-manifest");
    defer directory.cleanup();
    var fs = try storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const data = "[{\"file_id\":\"part\",\"object_uri\":\"object://antfly/part.parquet\",\"byte_len\":100,\"row_count\":3,\"etag\":\"e\"}]";
    var put = try client.putObject("antfly", "chunk.json", data, .{});
    put.deinit(a);
    const chunk: Chunk = .{ .uri = "object://antfly/chunk.json", .sha256 = &std.fmt.bytesToHex(checksum(data), .lower), .byte_len = data.len, .file_count = 1 };
    const bytes = try std.json.Stringify.valueAlloc(a, Head{ .version = 1, .source_id = "s", .chunks = &.{chunk}, .file_count = 1 }, .{});
    defer a.free(bytes);
    var chunks: decoded.Cache = .{ .a = a };
    defer chunks.deinit();
    var cache: catalogs.Cache = .{ .budget = .{ .backing = a, .limit = 8 * 1024 * 1024 } };
    defer cache.deinit();
    var request: Request = .{ .client = client, .source = "s", .uri = "object://antfly/head.parquet-manifest.json", .schema = "schema", .chunks = &chunks, .catalogs = &cache, .scope = @splat(1), .context = .{ .io = std.testing.io } };
    var first = try plan(a, request, bytes);
    defer first.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), first.inventory.files.len);
    try std.testing.expectEqual(@as(usize, 1), first.catalog.?.count());
    try std.testing.expect(first.catalog.?.allStrong());
    var proof: catalogs.CoverageBuilder = .{};
    var proof_files = first.catalog.?.iterator();
    while (try proof_files.next()) |file| try proof.add(file.*);
    try std.testing.expectEqualDeep(proof.finish(), first.catalog.?.coverageDigest());

    const CensusHash = struct {
        calls: usize = 0,
        fn get(raw: *anyopaque, id: []const u8) ![32]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return @import("lake_file_census.zig").key(id);
        }
    };
    var hasher: CensusHash = .{};
    const fingerprint: @import("lake_file_census.zig").Fingerprint = .{ .ptr = &hasher, .get = CensusHash.get };
    const initial = try first.catalog.?.prepareCensus(@splat(1), &.{first.catalog.?.find("part").?.*}, fingerprint, .{});
    defer initial.release();
    try std.testing.expectEqual(@as(usize, 1), hasher.calls);
    hasher.calls = 0;
    var second = try plan(a, request, bytes);
    defer second.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), second.catalog.?.new_files);
    const unchanged = try second.catalog.?.prepareCensus(@splat(1), &.{second.catalog.?.find("part").?.*}, fingerprint, .{});
    defer unchanged.release();
    try std.testing.expectEqual(@as(usize, 0), hasher.calls);
    const next_data = "[{\"file_id\":\"part2\",\"object_uri\":\"object://antfly/part2.parquet\",\"byte_len\":200,\"row_count\":4,\"etag\":\"e2\"}]";
    put = try client.putObject("antfly", "chunk2.json", next_data, .{});
    put.deinit(a);
    const next_chunk: Chunk = .{ .uri = "object://antfly/chunk2.json", .sha256 = &std.fmt.bytesToHex(checksum(next_data), .lower), .byte_len = next_data.len, .file_count = 1 };
    const next_bytes = try std.json.Stringify.valueAlloc(a, Head{ .version = 1, .source_id = "s", .chunks = &.{ chunk, next_chunk }, .file_count = 2 }, .{});
    defer a.free(next_bytes);
    var refreshed = try plan(a, request, next_bytes);
    defer refreshed.deinit(a);
    const changed = try refreshed.catalog.?.prepareCensus(@splat(1), &.{ refreshed.catalog.?.find("part").?.*, refreshed.catalog.?.find("part2").?.* }, fingerprint, .{});
    defer changed.release();
    try std.testing.expectEqual(@as(usize, 1), hasher.calls);
    try std.testing.expectEqual(@as(u64, 2), changed.summary().?.count);
    proof = .{};
    proof_files = refreshed.catalog.?.iterator();
    while (try proof_files.next()) |file| try proof.add(file.*);
    try std.testing.expectEqualDeep(proof.finish(), refreshed.catalog.?.coverageDigest());

    try std.testing.expectEqual(@as(u64, 1), initial.summary().?.count);
    // Eviction cannot reclaim catalogs held by older cuts. Admission remains
    // optional and falls back to a private plan from the already pinned chunks.
    cache.budget.limit = cache.budget.live;
    var fallback = try plan(a, request, next_bytes);
    defer fallback.deinit(a);
    try std.testing.expect(fallback.catalog == null);
    try std.testing.expectEqual(@as(usize, 2), fallback.inventory.files.len);
    try std.testing.expectEqualStrings("part", fallback.inventory.files[0].file_id);
    try std.testing.expectEqual(@as(usize, 1), first.catalog.?.count());
    request.catalogs = null;
    var full = try plan(a, request, bytes);
    defer full.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), full.inventory.files.len);
    try std.testing.expectEqualStrings("part", full.inventory.files[0].file_id);
    request.chunks = null;
    request.context.io = null;
    const Failure = struct {
        fn run(alloc: A, req: Request, head_bytes: []const u8) !void {
            var snapshot = try plan(alloc, req, head_bytes);
            defer snapshot.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 1), snapshot.inventory.files.len);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Failure.run, .{ request, bytes });
    put = try client.putObject("antfly", "chunk.json", "corrupt", .{});
    put.deinit(a);
    try std.testing.expectError(error.IcebergManifestLengthMismatch, plan(a, request, bytes));
}

test "external lake Parquet commit admission bounds aggregate private metadata before I/O" {
    const a = std.testing.allocator;
    var chunks: [9]Chunk = undefined;
    const checksum_text: [64]u8 = @splat('0');
    for ([_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h", "i" }, &chunks) |uri, *chunk| chunk.* = .{ .uri = uri, .sha256 = &checksum_text, .byte_len = 8 * 1024 * 1024, .file_count = 1 };
    const bytes = try std.json.Stringify.valueAlloc(a, Head{ .version = 1, .source_id = "s", .chunks = &chunks, .file_count = chunks.len }, .{});
    defer a.free(bytes);
    try std.testing.expectError(error.ExternalSourceSnapshotTooLarge, header(a, "s", "head.parquet-manifest.json", "schema", bytes));
}
