// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Bounded query-owned navigation range waves. Cache units remain individual
//! authenticated pages; physical requests coalesce nearby misses in one pack.
const std = @import("std");
const local = @import("antfly_local_sources");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const pages = @import("../serverless/graph_segment/page_store.zig");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;
pub const Budget = struct { ptr: *anyopaque, charge: *const fn (*anyopaque, u64) anyerror!void };
pub const Batch = struct {
    a: A,
    refs: []const tree.Ref,
    values: [tree.max_read_batch]?[]u8 = @splat(null),
    pub fn deinit(self: *Batch) void {
        for (&self.values) |*value| if (value.*) |bytes| {
            self.a.free(bytes);
            value.* = null;
        };
    }
    pub fn lookup(self: *const Batch, ref: tree.Ref) ?[]const u8 {
        for (self.refs, self.values[0..self.refs.len]) |candidate, value| if (candidate.eql(ref)) return value;
        return null;
    }
};
const Decoded = local.serverless_query_lake_decoded_cache;
const Job = struct {
    const Physical = struct { start: u64, bytes: []const u8 };
    store: stores.ArtifactStore,
    a: A,
    id: []const u8,
    domain: [32]u8,
    refs: []const tree.Ref,
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
        for (self.refs) |ref| {
            const id = try pages.PageStore.identity(self.domain, ref);
            if (!std.mem.eql(u8, self.id, &id) or ref.offset() < self.begin or @as(u64, ref.offset()) + ref.bytes > self.begin + self.length) continue;
            const offset: usize = @intCast(ref.offset() - self.begin);
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
            hash.update("native-lake-navigation-range-wave-v1");
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

pub fn load(a: A, store: stores.ArtifactStore, domain: [32]u8, refs: []const tree.Ref, token: Token, cached: ?artifacts.CachedRead, budget: Budget) !Batch {
    if (refs.len > tree.max_read_batch) return error.InvalidGraphPage;
    try token.check();
    if (cached) |cache| try cache.context.ensureActive();
    var result: Batch = .{ .a = a, .refs = refs };
    errdefer result.deinit();
    var ids: [tree.max_read_batch][175]u8 = undefined;
    var key_storage: [tree.max_read_batch][256]u8 = undefined;
    var keys: [tree.max_read_batch][]const u8 = undefined;
    var missing: [tree.max_read_batch]usize = undefined;
    var count: usize = 0;
    var logical: u64 = 0;
    var miss_bytes: u64 = 0;
    for (refs, 0..) |ref, i| {
        try ref.validate();
        ids[i] = try pages.PageStore.identity(domain, ref);
        keys[i] = if (ref.offset() == 0 and ref.bytes == ref.artifactBytes()) &ids[i] else try std.fmt.bufPrint(&key_storage[i], "{s}:page:{x}:{x}", .{ ids[i], ref.offset(), ref.bytes });
        logical += ref.bytes;
        if (cached) |cache| if (try cache.cache.lookupImmutableBlockLease(a, cache.scope, keys[i], ref.bytes, ref.digest, cache.context)) |found| {
            var lease = found;
            defer lease.deinit();
            result.values[i] = try a.dupe(u8, lease.bytes());
            continue;
        };
        missing[count] = i;
        count += 1;
        miss_bytes += ref.bytes;
    }
    const Order = struct {
        refs: []const tree.Ref,
        ids: []const [175]u8,
        fn less(self: @This(), l: usize, r: usize) bool {
            const order = std.mem.order(u8, &self.ids[l], &self.ids[r]);
            return order == .lt or (order == .eq and self.refs[l].offset() < self.refs[r].offset());
        }
    };
    std.mem.sort(usize, missing[0..count], Order{ .refs = refs, .ids = ids[0..refs.len] }, Order.less);
    var allocations: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = a };
    var jobs: [tree.max_read_batch]Job = undefined;
    var group_for: [tree.max_read_batch]usize = undefined;
    var groups: usize = 0;
    var physical: u64 = 0;
    for (missing[0..count]) |i| {
        const ref = refs[i];
        const end = @as(u64, ref.offset()) + ref.bytes;
        if (groups != 0) {
            const previous = &jobs[groups - 1];
            if (std.mem.eql(u8, previous.id, &ids[i]) and ref.offset() <= previous.begin + previous.length + 16 * 1024 and
                @max(end, previous.begin + previous.length) - previous.begin <= tree.max_page_bytes)
            {
                const length: usize = @intCast(@max(end, previous.begin + previous.length) - previous.begin);
                physical += length - previous.length;
                previous.length = length;
                group_for[i] = groups - 1;
                continue;
            }
        }
        jobs[groups] = .{ .store = store, .a = allocations.allocator(), .id = &ids[i], .begin = ref.offset(), .length = ref.bytes, .token = token, .domain = domain, .refs = refs, .cached = cached };
        group_for[i] = groups;
        groups += 1;
        physical += ref.bytes;
    }
    // Cache hits retain the ordinary logical read charge. Coalesced gaps are
    // additional physical traffic and cannot bypass the request's allowance.
    try budget.charge(budget.ptr, logical + (physical -| miss_bytes));
    defer for (jobs[0..groups]) |*job| job.deinit();
    var tasks: [tree.max_read_batch]?local.sql_parallel_scheduler.Task(anyerror!void) = @splat(null);
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
        const job = jobs[group_for[i]];
        const offset: usize = @intCast(refs[i].offset() - job.begin);
        const bytes = job.result.?[offset..][0..refs[i].bytes];
        try stores.validatePayloadSha256WithCancellation(bytes, &std.fmt.bytesToHex(&refs[i].digest, .lower), token);
        var loader: Loader = .{ .data = bytes };
        result.values[i] = if (cached) |cache| try cache.cache.readImmutableAlloc(a, cache.scope, keys[i], bytes.len, refs[i].digest, cache.context, .{ .ptr = &loader, .load = Loader.read, .provider_read = Loader.traffic }) else try a.dupe(u8, bytes);
    }
    return result;
}

test "external lake navigation range waves coalesce authenticate cache and bound budgets" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-range-waves");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var backend = fs.artifactStore();
    const scope: stores.UploadScope = .{ .domain = @splat(7), .attempt = @splat(1) };
    var payload: [384]u8 = undefined;
    for (&payload, 0..) |*byte, i| byte.* = @intCast(i % 251);
    var uploaded = try backend.putScoped(scope, &payload, .none);
    defer uploaded.deinit(a);
    var pack_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&payload, &pack_digest, .{});
    var refs_: [3]tree.Ref = undefined;
    for (&refs_, 0..) |*ref, i| {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(payload[i * 128 ..][0..128], &digest, .{});
        ref.* = .{ .digest = digest, .attempt = scope.attempt, .bytes = 128, .height = 0, .records = 1, .pack = .{ .digest = pack_digest, .bytes = payload.len, .offset = @intCast(i * 128) } };
    }
    const Probe = struct {
        var active: *@This() = undefined;
        vtable: *const stores.ArtifactStore.VTable,
        reads: std.atomic.Value(usize) = .init(0),
        max_length: std.atomic.Value(usize) = .init(0),
        corrupt: bool = false,
        fn get(raw: *anyopaque, alloc: A, id: []const u8, offset: u64, length: usize, token: Token) ![]u8 {
            const self = active;
            _ = self.reads.fetchAdd(1, .monotonic);
            _ = self.max_length.fetchMax(length, .monotonic);
            const bytes = if (self.vtable.get_range_alloc_with_cancellation) |read| try read(raw, alloc, id, offset, length, token) else try self.vtable.get_range_alloc(raw, alloc, id, offset, length);
            if (self.corrupt and bytes.len > 128) bytes[128] ^= 1;
            return bytes;
        }
    };
    var probe: Probe = .{ .vtable = backend.vtable };
    Probe.active = &probe;
    var vtable = backend.vtable.*;
    vtable.get_range_alloc_with_cancellation = Probe.get;
    backend.vtable = &vtable;
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(8), .context = .{ .io = std.testing.io } };
    const Charge = struct {
        fn charge(raw: *anyopaque, bytes: u64) !void {
            try stores.chargeReadBudget(@ptrCast(@alignCast(raw)), bytes);
        }
    };
    const shuffled = [_]tree.Ref{ refs_[2], refs_[0], refs_[1], refs_[0] };
    var remaining: u64 = 4096;
    var cold = try load(a, backend, scope.domain, &shuffled, .none, cached, .{ .ptr = &remaining, .charge = Charge.charge });
    defer cold.deinit();
    try std.testing.expectEqual(@as(usize, 1), probe.reads.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().provider_reads);
    for (shuffled, cold.values[0..shuffled.len]) |ref, bytes| try std.testing.expectEqualSlices(u8, payload[ref.offset()..][0..ref.bytes], bytes.?);
    var warm = try load(a, backend, scope.domain, &shuffled, .none, cached, .{ .ptr = &remaining, .charge = Charge.charge });
    defer warm.deinit();
    try std.testing.expectEqual(@as(usize, 1), probe.reads.load(.monotonic));
    remaining = 1;
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, load(a, backend, scope.domain, &refs_, .none, cached, .{ .ptr = &remaining, .charge = Charge.charge }));
    try std.testing.expectEqual(@as(usize, 1), probe.reads.load(.monotonic));
    remaining = 4096;
    probe.corrupt = true;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, load(a, backend, scope.domain, &refs_, .none, null, .{ .ptr = &remaining, .charge = Charge.charge }));
    probe.corrupt = false;
    // Bounded coalescing never downloads a whole large pack. Independent
    // requests still use the shared scheduler and preserve per-page results.
    {
        const large = try a.alloc(u8, 1200 * 1024);
        defer a.free(large);
        @memset(large, 37);
        var object = try backend.putScoped(scope, large, .none);
        defer object.deinit(a);
        std.crypto.hash.sha2.Sha256.hash(large, &pack_digest, .{});
        var wide: [2]tree.Ref = undefined;
        for (&wide, 0..) |*ref, i| {
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(large[i * 600 * 1024 ..][0 .. 600 * 1024], &digest, .{});
            ref.* = .{ .digest = digest, .attempt = scope.attempt, .bytes = 600 * 1024, .height = 0, .records = 1, .pack = .{ .digest = pack_digest, .bytes = @intCast(large.len), .offset = @intCast(i * 600 * 1024) } };
        }
        remaining = 8 * 1024 * 1024;
        const before = probe.reads.load(.monotonic);
        var wave = try load(a, backend, scope.domain, &wide, .none, cached, .{ .ptr = &remaining, .charge = Charge.charge });
        defer wave.deinit();
        try std.testing.expectEqual(before + 2, probe.reads.load(.monotonic));
        try std.testing.expect(probe.max_length.load(.monotonic) <= tree.max_page_bytes);
    }
    // Coalesced gaps consume real allowance and are checked before provider IO.
    {
        var spaced: [272]u8 = @splat(59);
        var object = try backend.putScoped(scope, &spaced, .none);
        defer object.deinit(a);
        std.crypto.hash.sha2.Sha256.hash(&spaced, &pack_digest, .{});
        var gap: [2]tree.Ref = undefined;
        for (&gap, 0..) |*ref, i| {
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(spaced[i * 144 ..][0..128], &digest, .{});
            ref.* = .{ .digest = digest, .attempt = scope.attempt, .bytes = 128, .height = 0, .records = 1, .pack = .{ .digest = pack_digest, .bytes = spaced.len, .offset = @intCast(i * 144) } };
        }
        remaining = 256;
        const before = probe.reads.load(.monotonic);
        try std.testing.expectError(error.ArtifactReadBudgetExceeded, load(a, backend, scope.domain, &gap, .none, null, .{ .ptr = &remaining, .charge = Charge.charge }));
        try std.testing.expectEqual(before, probe.reads.load(.monotonic));
        remaining = 272;
        var wave = try load(a, backend, scope.domain, &gap, .none, null, .{ .ptr = &remaining, .charge = Charge.charge });
        defer wave.deinit();
        try std.testing.expectEqual(@as(u64, 0), remaining);
        try std.testing.expectEqual(before + 1, probe.reads.load(.monotonic));
    }
    const Sweep = struct {
        fn run(alloc: A, store: stores.ArtifactStore, domain: [32]u8, references: []const tree.Ref) !void {
            var allowance: u64 = 4096;
            var batch = try load(alloc, store, domain, references, .none, null, .{ .ptr = &allowance, .charge = Charge.charge });
            defer batch.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Sweep.run, .{ backend, scope.domain, &shuffled });
}
