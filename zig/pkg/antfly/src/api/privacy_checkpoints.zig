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

//! Large private range keys live outside the configuration CAS. Immutable
//! generations are attached atomically by the catalog owner and reclaimed in
//! bounded transactions after their last catalog reference disappears.
const std = @import("std");
const catalog = @import("antfly_local_sources").privacy_catalog;
const keys = catalog.checkpoints;
const jobs = @import("../metadata/job_state.zig");
const operation = @import("antfly_local_sources").api_operation;
const time = @import("antfly_platform").time;
const A = std.mem.Allocator;

pub const GC = struct { after: [128]u8 = undefined, len: usize = 0, next_ns: u64 = 0, quantum_ns: u64 = 25 * std.time.ns_per_ms };
pub const RetryCache = struct {
    a: A,
    table: u64 = 0,
    refs: std.ArrayList(keys.Ref) = .empty,
    pub fn deinit(self: *@This()) void {
        self.refs.deinit(self.a);
    }
};
pub const Context = struct {
    a: A,
    persistence: ?jobs.Persistence,
    table: u64,
    term: u64,
    request: operation.RequestContext,
    retained: std.ArrayList([]const u8) = .empty,
    refs: std.ArrayList(keys.Ref) = .empty,
    retry: ?*RetryCache = null,
    capacity_limited: bool = false,

    pub fn init(server: anytype, a: A, table: u64, term: u64, resource: catalog.Resource, request: operation.RequestContext) !Context {
        var self: Context = .{ .a = a, .persistence = null, .table = table, .term = term, .request = request };
        if (comptime @hasField(@TypeOf(server.*), "document_job_store")) self.persistence = server.document_job_store.replicated;
        if (resource.checkpoint_encoding > 1) return error.InvalidPrivacyCatalog;
        if (resource.checkpoint_encoding == 1) {
            if (resource.coverage.cursor) |value| try self.retain(value);
            for (resource.scans) |scan| {
                for ([_][]const u8{ scan.from_key, scan.to_key, scan.next_key orelse "", scan.verification_cursor orelse "" }) |value| try self.retain(value);
                if (scan.shard_cursors == .array) for (scan.shard_cursors.array.items) |row| {
                    if (row == .object) if (row.object.get("next_key")) |value| if (value == .string) try self.retain(value.string);
                };
            }
        }
        return self;
    }
    pub fn reuseUploads(self: *Context, cache: *RetryCache) void {
        if (cache.table != self.table) {
            cache.refs.clearRetainingCapacity();
            cache.table = self.table;
        }
        self.retry = cache;
    }
    fn retain(self: *Context, value: []const u8) !void {
        if (keys.reference(value)) |ref| {
            try self.retained.append(self.a, value);
            try self.refs.append(self.a, ref);
        }
    }
    fn cas(self: *Context, key: []const u8, expected: ?[]const u8, value: []const u8) !void {
        try self.request.ensureActive();
        if (!try (self.persistence orelse return error.RestoreJobPersistenceUnavailable).compareExchange(key, expected, value, self.term)) return error.TableGenerationChanged;
    }
    pub fn encode(self: *Context, value: []const u8) ![]const u8 {
        // Identity distinguishes retained references from user keys that happen
        // to spell a reference. New input is always interpreted as a raw key.
        for (self.retained.items) |token| if (token.ptr == value.ptr and token.len == value.len) return value;
        if (value.len <= 4096 and !std.mem.startsWith(u8, value, keys.prefix)) return value;
        if (value.len > keys.max_bytes) return error.InvalidDocumentCheckpoint;
        const digest = std.fmt.bytesToHex(jobs.digest(value), .lower);
        for (self.refs.items) |ref| if (std.mem.eql(u8, &ref.digest, &digest)) {
            const token = try self.a.dupe(u8, &ref.token());
            try self.retained.append(self.a, token);
            return token;
        };
        if (self.retry) |cache| for (cache.refs.items) |ref| if (std.mem.eql(u8, &ref.digest, &digest)) {
            const token = try self.a.dupe(u8, &ref.token());
            try self.retain(token);
            return token;
        };
        const persistence = self.persistence orelse return error.RestoreJobPersistenceUnavailable;
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        var entropy: [16]u8 = undefined;
        try io_impl.io().randomSecure(&entropy);
        const ref: keys.Ref = .{ .digest = digest, .generation = std.fmt.bytesToHex(entropy, .lower) };
        var buffer: [128]u8 = undefined;
        const manifest = try ref.storageKey(&buffer, self.table, null);
        var header: keys.Header = .{ .accounted = true, .length = @intCast(value.len), .created_at_millis = @intCast(@divFloor(time.realtimeNs(), std.time.ns_per_ms)) };
        const initial = try std.json.Stringify.valueAlloc(self.a, header, .{});
        defer self.a.free(initial);
        self.cas(manifest, null, initial) catch |err| {
            if (err != error.TableGenerationChanged) return err;
            // Diagnose capacity only on a miss; successful uploads need no
            // extra metadata reads. Owner quota checks remain authoritative.
            for ([_]bool{ false, true }) |retained| {
                var quota_buffer: [64]u8 = undefined;
                const quota = try persistence.get(self.a, try keys.quotaKey(&quota_buffer, self.table, retained));
                defer if (quota) |bytes| self.a.free(bytes);
                if (quota) |bytes| {
                    if (bytes.len != 8) return error.InvalidDocumentCheckpoint;
                    const used = std.mem.readInt(u64, bytes[0..8], .little);
                    const limit = if (retained) keys.max_retained_bytes else keys.max_pending_bytes;
                    if (@max(value.len, 4096) > limit -| used) {
                        self.capacity_limited = true;
                        return error.PrivacyCheckpointCapacityExceeded;
                    }
                }
            }
            return err;
        };
        // A single reusable chunk avoids retaining an additional copy of every
        // large key in the request arena while staging it.
        const backing = @import("antfly_platform").allocator.reclaimingScratch(self.a);
        const chunk = try backing.alloc(u8, keys.chunk_bytes + 1);
        defer backing.free(chunk);
        chunk[0] = 'V';
        for (0..header.chunks()) |index| {
            const start = index * keys.chunk_bytes;
            const part = value[start..@min(value.len, start + keys.chunk_bytes)];
            @memcpy(chunk[1..][0..part.len], part);
            var chunk_buffer: [128]u8 = undefined;
            try self.cas(try ref.storageKey(&chunk_buffer, self.table, index), null, chunk[0 .. part.len + 1]);
        }
        header.state = .ready;
        const ready = try std.json.Stringify.valueAlloc(self.a, header, .{});
        defer self.a.free(ready);
        const expected = jobs.digest(initial);
        try self.cas(manifest, &expected, ready);
        if (self.retry) |cache| try cache.refs.append(cache.a, ref);
        const token = try self.a.dupe(u8, &ref.token());
        try self.retain(token);
        return token;
    }
    pub fn decode(self: *Context, value: []const u8) ![]const u8 {
        var retained = false;
        for (self.retained.items) |token| if (token.ptr == value.ptr and token.len == value.len) {
            retained = true;
            break;
        };
        if (!retained) return value;
        const ref = keys.reference(value).?;
        const persistence = self.persistence orelse return error.RestoreJobPersistenceUnavailable;
        const backing = @import("antfly_platform").allocator.reclaimingScratch(self.a);
        var buffer: [128]u8 = undefined;
        const raw = (try persistence.get(backing, try ref.storageKey(&buffer, self.table, null))) orelse return error.InvalidDocumentCheckpoint;
        defer backing.free(raw);
        var parsed = try std.json.parseFromSlice(keys.Header, backing, raw, .{});
        defer parsed.deinit();
        const header = parsed.value;
        if (header.state != .ready or header.length == 0 or header.length > keys.max_bytes) return error.InvalidDocumentCheckpoint;
        const result = try self.a.alloc(u8, header.length);
        errdefer self.a.free(result);
        for (0..header.chunks()) |index| {
            try self.request.ensureActive();
            const part = (try persistence.get(backing, try ref.storageKey(&buffer, self.table, index))) orelse return error.InvalidDocumentCheckpoint;
            defer backing.free(part);
            const start = index * keys.chunk_bytes;
            const length = @min(keys.chunk_bytes, result.len - start);
            if (part.len != length + 1 or part[0] != 'V') return error.InvalidDocumentCheckpoint;
            @memcpy(result[start..][0..length], part[1..]);
        }
        if (!std.mem.eql(u8, &std.fmt.bytesToHex(jobs.digest(result), .lower), &ref.digest)) return error.InvalidDocumentCheckpoint;
        return result;
    }
    fn shards(self: *Context, value: std.json.Value, comptime decoding: bool) !std.json.Value {
        if (value != .array) return value;
        var result: std.json.Value = .{ .array = .init(self.a) };
        for (value.array.items) |row| {
            var copy = row;
            if (row == .object) {
                copy.object = try row.object.clone(self.a);
                if (copy.object.getPtr("next_key")) |next| if (next.* == .string) {
                    next.string = if (decoding) try self.decode(next.string) else try self.encode(next.string);
                };
            }
            try result.array.append(copy);
        }
        return result;
    }
    pub fn decodeScan(self: *Context, scan: catalog.Scan) !catalog.Scan {
        var result = scan;
        result.from_key = try self.decode(scan.from_key);
        result.to_key = try self.decode(scan.to_key);
        if (scan.next_key) |value| result.next_key = try self.decode(value);
        if (scan.verification_cursor) |value| result.verification_cursor = try self.decode(value);
        result.shard_cursors = try self.shards(scan.shard_cursors, true);
        return result;
    }
    pub fn encodeResource(self: *Context, resource: *catalog.Resource) !void {
        const scans = try self.a.dupe(catalog.Scan, resource.scans);
        for (scans) |*scan| {
            scan.from_key = try self.encode(scan.from_key);
            scan.to_key = try self.encode(scan.to_key);
            if (scan.next_key) |value| scan.next_key = try self.encode(value);
            if (scan.verification_cursor) |value| scan.verification_cursor = try self.encode(value);
            scan.shard_cursors = try self.shards(scan.shard_cursors, false);
        }
        resource.scans = scans;
        if (resource.coverage.cursor) |value| resource.coverage.cursor = try self.encode(value);
        resource.checkpoint_encoding = 1;
    }
};

/// Spend shared chunk, operation and elapsed-time budgets across manifests.
/// Busy collections run promptly; idle catalogs need only a slow poll.
pub fn collect(server: anytype, a: A) !void {
    if (comptime !@hasField(@TypeOf(server.*), "privacy_checkpoint_gc")) return;
    const persistence = server.document_job_store.replicated orelse return;
    const term = (try server.source.privacyAuthorityTerm()) orelse return;
    const state = &server.privacy_checkpoint_gc;
    const now = time.monotonicNs();
    if (now < state.next_ns) return;
    state.next_ns = now +| std.time.ns_per_s;
    const raw = try persistence.scan(a, if (state.len == 0) "privacy/m/" else state.after[0..state.len]);
    defer a.free(raw);
    var page = try std.json.parseFromSlice(jobs.Page, a, raw, .{});
    defer page.deinit();
    var chunks: usize = keys.gc_chunks;
    var operations: usize = 32;
    const deadline = now +| state.quantum_ns;
    for (page.value.rows, 0..) |row, index| {
        if (operations == 0 or (index != 0 and time.monotonicNs() >= deadline)) {
            state.next_ns = now +| 100 * std.time.ns_per_ms;
            return;
        }
        var parsed = try std.json.parseFromSlice(keys.Header, a, row.value, .{});
        defer parsed.deinit();
        var header = parsed.value;
        var expected = jobs.digest(row.value);
        if (header.deleted_chunks != header.chunks()) {
            if (chunks == 0) {
                state.next_ns = now +| 100 * std.time.ns_per_ms;
                return;
            }
            const count = @min(header.chunks() - header.deleted_chunks, chunks);
            header.state = .deleting;
            header.deleted_chunks += @intCast(count);
            const next = try std.json.Stringify.valueAlloc(a, header, .{});
            defer a.free(next);
            const changed = try persistence.compareExchange(row.key, &expected, next, term);
            operations -= 1;
            chunks -= count;
            if (changed) expected = jobs.digest(next);
            if (!changed) {
                try gcAfter(state, row.key);
                continue;
            }
        }
        // Owner-side reference and progress checks fence both CAS operations.
        // Finishing a small blob need not cost a second scheduler tick.
        if (header.deleted_chunks == header.chunks() and operations != 0) {
            _ = try persistence.compareExchange(row.key, &expected, keys.erase, term);
            operations -= 1;
        }
        // Rotate after partial blobs too; unfinished rows return on wrap.
        try gcAfter(state, row.key);
    }
    if (page.value.next) |next| try gcAfter(state, next) else state.len = 0;
    if (page.value.rows.len != 0 or page.value.next != null) state.next_ns = now +| 100 * std.time.ns_per_ms;
}
fn gcAfter(state: *GC, key: []const u8) !void {
    if (key.len > state.after.len) return error.InvalidDocumentCheckpoint;
    @memcpy(state.after[0..key.len], key);
    state.len = key.len;
}

test "privacy key generations round trip deduplicate and escape reference-shaped user keys" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Fake = struct {
        a: A,
        rows: std.StringHashMapUnmanaged([]const u8) = .empty,
        writes: usize = 0,
        fn get(raw: *anyopaque, alloc: A, key: []const u8) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return if (self.rows.get(key)) |value| try alloc.dupe(u8, value) else null;
        }
        fn scan(_: *anyopaque, _: A, _: ?[]const u8) ![]u8 {
            return error.UnexpectedCall;
        }
        fn cas(raw: *anyopaque, key: []const u8, expected: ?[]const u8, value: []const u8, term: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(u64, 9), term);
            if (self.rows.get(key)) |old| {
                if (expected == null or !std.mem.eql(u8, &jobs.digest(old), expected.?)) return false;
            } else if (expected != null) return false;
            try self.rows.put(self.a, try self.a.dupe(u8, key), try self.a.dupe(u8, value));
            self.writes += 1;
            return true;
        }
    };
    var fake: Fake = .{ .a = a };
    const persistence = jobs.Persistence.fromLocal(&fake, .{ .get = Fake.get, .scan = Fake.scan, .cas = Fake.cas });
    var context: Context = .{ .a = a, .persistence = persistence, .table = 3, .term = 9, .request = .{} };
    const large = try a.alloc(u8, keys.chunk_bytes + 21);
    @memset(large, 'x');
    var retries: RetryCache = .{ .a = a };
    defer retries.deinit();
    context.reuseUploads(&retries);
    const token = try context.encode(large);
    try std.testing.expectEqual(@as(usize, 101), token.len);
    try std.testing.expectEqualStrings(large, try context.decode(token));
    const writes = fake.writes;
    var contender: Context = .{ .a = a, .persistence = persistence, .table = 3, .term = 9, .request = .{} };
    contender.reuseUploads(&retries);
    try std.testing.expectEqualStrings(token, try contender.encode(large));
    try std.testing.expectEqual(writes, fake.writes);
    try std.testing.expectEqualStrings(token, try context.encode(large));
    try std.testing.expectEqual(writes, fake.writes);
    // A newly authored literal with the same spelling has a different identity.
    const literal = try a.dupe(u8, token);
    const escaped = try context.encode(literal);
    try std.testing.expect(!std.mem.eql(u8, token, escaped));
    try std.testing.expectEqualStrings(literal, try context.decode(escaped));
    var buffer: [128]u8 = undefined;
    const ref = keys.reference(token).?;
    const selected = try ref.storageKey(&buffer, 3, 1);
    try std.testing.expectEqual(@as(usize, 127), selected.len);
    try std.testing.expectEqual(ref, (try keys.parseKey(selected)).ref.?);
    // Corrupted durable data cannot be used as a scan boundary.
    try fake.rows.put(a, try a.dupe(u8, selected), "Vcorrupt");
    try std.testing.expectError(error.InvalidDocumentCheckpoint, context.decode(token));
}
