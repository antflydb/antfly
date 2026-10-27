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

//! Bounded authenticated range waves shared by navigation pages and column stripes.
//! Immutable cache units stay independent of physical request coalescing.
const std = @import("std");
const local = @import("antfly_local_sources");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;
pub const max_batch = 16;
pub const Unit = struct { object: artifacts.ChunkRef, offset: u64, bytes: usize, digest: [32]u8 };
pub const Options = struct { max_range: usize, max_gap: usize = 0 };
pub const Budget = struct { ptr: *anyopaque, charge: *const fn (*anyopaque, u64) anyerror!void };
pub const Batch = struct {
    a: A,
    values: [max_batch]?[]u8 = @splat(null),
    pub fn deinit(self: *Batch) void {
        for (&self.values) |*value| if (value.*) |bytes| {
            self.a.free(bytes);
            value.* = null;
        };
    }
};
const Decoded = local.serverless_query_lake_decoded_cache;
const Job = struct {
    const Physical = struct { start: u64, bytes: []const u8 };
    store: stores.ArtifactStore,
    a: A,
    id: []const u8,
    units: []const Unit,
    begin: u64,
    length: usize,
    token: Token,
    cached: ?artifacts.CachedRead,
    lease: ?Decoded.Lease = null,
    result: ?[]const u8 = null,
    fn deinit(self: *Job) void {
        if (self.lease) |lease| lease.release() else if (self.result) |bytes| self.a.free(bytes);
    }
    fn canceled(raw: *const anyopaque) bool {
        const self: *const Job = @ptrCast(@alignCast(raw));
        self.token.check() catch return true;
        if (self.cached) |cache| cache.context.ensureActive() catch return true;
        return false;
    }
    fn read(self: *Job, alloc: A) ![]u8 {
        const token: Token = .{ .ptr = self, .is_cancelled_fn = canceled };
        const bytes = try self.store.getRangeAllocWithCancellationUsingAllocator(alloc, self.id, self.begin, self.length, token);
        errdefer alloc.free(bytes);
        if (bytes.len != self.length) return error.ArtifactIntegrityMismatch;
        if (self.cached) |cache| cache.cache.recordPhysicalRead(bytes.len);
        // Shared physical flights publish only ranges whose requested pages
        // have been authenticated. Each waiter also validates its own units.
        for (self.units) |ref| {
            if (!std.mem.eql(u8, self.id, ref.object.artifact_id) or ref.offset < self.begin or ref.offset + ref.bytes > self.begin + self.length) continue;
            const offset: usize = @intCast(ref.offset - self.begin);
            try stores.validatePayloadSha256WithCancellation(bytes[offset..][0..ref.bytes], &std.fmt.bytesToHex(&ref.digest, .lower), token);
        }
        return bytes;
    }
    fn fill(raw: *anyopaque, item: *Decoded.Item) !void {
        const self: *Job = @ptrCast(@alignCast(raw));
        const a = item.arena.allocator();
        const value = a.create(Physical) catch |err| {
            if (err == error.OutOfMemory and item.budget.isLocallyExhausted()) return error.SqlMemoryLimitExceeded;
            return err;
        };
        const bytes = self.read(a) catch |err| {
            if (err == error.OutOfMemory and item.budget.isLocallyExhausted()) return error.SqlMemoryLimitExceeded;
            return err;
        };
        value.* = .{ .start = self.begin, .bytes = bytes };
        item.payload = .{ .extension = value };
    }
    fn run(self: *Job) anyerror!void {
        if (self.cached) |cache| {
            var hash = std.crypto.hash.sha2.Sha256.init(.{});
            hash.update("native-lake-authenticated-range-wave-v1");
            hash.update(&cache.scope);
            hash.update(self.id);
            const domain = hash.finalResult();
            var interval: [16]u8 = undefined;
            std.mem.writeInt(u64, interval[0..8], self.begin, .big);
            std.mem.writeInt(u64, interval[8..16], self.begin + self.length, .big);
            hash.update(&interval);
            var context = cache.context;
            context.checkpoint = null;
            context.cancellation = .fromCallback(self, canceled);
            const lease = cache.cache.physical.acquireCovered(hash.finalResult(), self.length * 2 + 64 * 1024, context, .{ .ptr = self, .load = fill }, .{ .domain = domain, .start = self.begin, .end = self.begin + self.length }) catch |err| fallback: {
                try self.token.check();
                try cache.context.ensureActive();
                if (err != error.SqlMemoryLimitExceeded) return err;
                break :fallback null;
            };
            if (lease) |held| {
                errdefer held.release();
                const value: *const Physical = @ptrCast(@alignCast(held.item.payload.extension));
                if (value.start > self.begin or self.begin - value.start > value.bytes.len or self.length > value.bytes.len - (self.begin - value.start)) return error.ArtifactIntegrityMismatch;
                self.result = value.bytes[@intCast(self.begin - value.start)..][0..self.length];
                self.lease = held;
                return;
            }
        }
        self.result = try self.read(self.a);
    }
};

pub fn load(a: A, store: stores.ArtifactStore, refs: []const Unit, token: Token, cached: ?artifacts.CachedRead, budget: Budget, options: Options) !Batch {
    if (refs.len > max_batch or options.max_range == 0) return error.InvalidGraphPage;
    try token.check();
    if (cached) |cache| try cache.context.ensureActive();
    var result: Batch = .{ .a = a };
    errdefer result.deinit();
    var ids: [max_batch][]const u8 = undefined;
    var key_storage: [max_batch][256]u8 = undefined;
    var keys: [max_batch][]const u8 = undefined;
    var missing: [max_batch]usize = undefined;
    var count: usize = 0;
    var logical: u64 = 0;
    for (refs, 0..) |ref, i| {
        try stores.validateSha256ArtifactIdentity(ref.object.artifact_id, ref.object.checksum);
        if (ref.bytes == 0 or ref.bytes > options.max_range or ref.offset > ref.object.byte_len or ref.bytes > ref.object.byte_len - ref.offset) return error.InvalidGraphPage;
        ids[i] = ref.object.artifact_id;
        keys[i] = if (ref.offset == 0 and ref.bytes == ref.object.byte_len) ids[i] else try std.fmt.bufPrint(&key_storage[i], "{s}:page:{x}:{x}", .{ ids[i], ref.offset, ref.bytes });
        logical += ref.bytes;
        if (cached) |cache| if (try cache.cache.lookupImmutableBlockLease(a, cache.scope, keys[i], ref.bytes, ref.digest, cache.context)) |found| {
            var lease = found;
            defer lease.deinit();
            result.values[i] = try a.dupe(u8, lease.bytes());
            continue;
        };
        missing[count] = i;
        count += 1;
    }
    const Order = struct {
        units: []const Unit,
        ids: []const []const u8,
        fn less(self: @This(), l: usize, r: usize) bool {
            const order = std.mem.order(u8, self.ids[l], self.ids[r]);
            return order == .lt or (order == .eq and self.units[l].offset < self.units[r].offset);
        }
    };
    std.mem.sort(usize, missing[0..count], Order{ .units = refs, .ids = ids[0..refs.len] }, Order.less);
    var allocations: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = a };
    var jobs: [max_batch]Job = undefined;
    var group_for: [max_batch]usize = undefined;
    var groups: usize = 0;
    var members: [max_batch]usize = @splat(0);
    var gap_bytes: u64 = 0;
    for (missing[0..count]) |i| {
        const ref = refs[i];
        const end = ref.offset + ref.bytes;
        if (groups != 0) {
            const previous = &jobs[groups - 1];
            if (std.mem.eql(u8, previous.id, ids[i]) and ref.offset <= previous.begin + previous.length + options.max_gap and
                @max(end, previous.begin + previous.length) - previous.begin <= options.max_range)
            {
                const length: usize = @intCast(@max(end, previous.begin + previous.length) - previous.begin);
                gap_bytes += ref.offset -| (previous.begin + previous.length);
                previous.length = length;
                group_for[i] = groups - 1;
                members[groups - 1] += 1;
                continue;
            }
        }
        jobs[groups] = .{ .store = store, .a = allocations.allocator(), .id = ids[i], .begin = ref.offset, .length = ref.bytes, .token = token, .units = refs, .cached = cached };
        group_for[i] = groups;
        members[groups] = 1;
        groups += 1;
    }
    // Cache hits retain the ordinary logical read charge. Coalesced gaps are
    // additional physical traffic and cannot bypass the request's allowance.
    try budget.charge(budget.ptr, logical + gap_bytes);
    defer for (jobs[0..groups]) |*job| job.deinit();
    var tasks: [max_batch]?local.sql_parallel_scheduler.Task(anyerror!void) = @splat(null);
    const io = if (cached) |cache| cache.context.io else null;
    defer if (io) |active| for (&tasks) |*task| if (task.*) |*held| {
        held.cancel(active) catch {};
    };
    var failure: ?anyerror = null;
    for (jobs[0..groups], 0..) |*job, i| {
        if (failure != null) break;
        if (io) |active| if (groups > 1) {
            tasks[i] = local.sql_parallel_scheduler.global().submitIo(active, job.length * 2 + 64 * 1024, Job.run, .{job});
        };
        if (tasks[i] == null) job.run() catch |err| {
            failure = err;
        };
    }
    if (io) |active| for (&tasks) |*task| if (task.*) |*held| {
        held.await(active) catch |err| {
            if (failure == null) failure = err;
        };
        task.* = null;
    };
    if (failure) |err| return err;
    const Loader = struct {
        data: []const u8,
        fn read(raw: *anyopaque, alloc: A) ![]u8 {
            return alloc.dupe(u8, @as(*@This(), @ptrCast(@alignCast(raw))).data);
        }
        fn traffic(_: *anyopaque) local.serverless_query_lake_serving_cache.Cache.ProviderRead {
            return .{ .requests = 0, .bytes = 0 };
        }
    };
    for (missing[0..count]) |i| {
        try token.check();
        const job = &jobs[group_for[i]];
        const offset: usize = @intCast(refs[i].offset - job.begin);
        const bytes = job.result.?[offset..][0..refs[i].bytes];
        try stores.validatePayloadSha256WithCancellation(bytes, &std.fmt.bytesToHex(&refs[i].digest, .lower), token);
        var loader: Loader = .{ .data = bytes };
        result.values[i] = if (cached) |cache| try cache.cache.readImmutableAlloc(a, cache.scope, keys[i], bytes.len, refs[i].digest, cache.context, .{ .ptr = &loader, .load = Loader.read, .provider_read = Loader.traffic }) else if (members[group_for[i]] == 1 and job.lease == null and refs[i].offset == job.begin and refs[i].bytes == job.length) transfer: {
            job.result = null;
            break :transfer @constCast(bytes);
        } else try a.dupe(u8, bytes);
    }
    return result;
}
