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
const ImmutableLease = local.serverless_query_lake_serving_cache.Cache.ImmutableLease;
const RangeLease = local.serverless_query_lake_range_io.RangeLease;
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
    owned: ?*SharedBytes = null,
    const SharedBytes = struct {
        a: A,
        bytes: []const u8,
        refs: std.atomic.Value(usize) = .init(1),
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.refs.fetchSub(1, .acq_rel) != 1) return;
            const a = self.a;
            a.free(self.bytes);
            a.destroy(self);
        }
    };
    /// Each slice owns a share of the physical response, independently of the
    /// stack-owned wave and its temporary synchronized allocator descriptor.
    fn rangeLease(self: *Job, a: A, offset: usize, len: usize) !RangeLease {
        const bytes = self.result.?[offset..][0..len];
        if (self.lease) |held| {
            const Pin = struct {
                a: A,
                held: Decoded.Lease,
                fn release(raw: *anyopaque) void {
                    const pin: *@This() = @ptrCast(@alignCast(raw));
                    const alloc = pin.a;
                    pin.held.release();
                    alloc.destroy(pin);
                }
            };
            const pin = try a.create(Pin);
            pin.* = .{ .a = a, .held = held.retain() };
            return .{ .bytes = bytes, .owner = .{ .shared = .{ .ptr = pin, .release_fn = Pin.release } } };
        }
        if (self.owned == null) {
            const owner = try a.create(SharedBytes);
            owner.* = .{ .a = a, .bytes = self.result.? };
            self.owned = owner;
        }
        _ = self.owned.?.refs.fetchAdd(1, .monotonic);
        return .{ .bytes = bytes, .owner = .{ .shared = .{ .ptr = self.owned.?, .release_fn = SharedBytes.release } } };
    }
    fn deinit(self: *Job) void {
        if (self.owned) |owned| SharedBytes.release(owned) else if (self.lease) |lease| lease.release() else if (self.result) |bytes| self.a.free(bytes);
    }
    fn check(raw: *const anyopaque) !void {
        const self: *const Job = @ptrCast(@alignCast(raw));
        // Context is authoritative even when a transport-only adapter flattened
        // the same deadline to Canceled.
        if (self.cached) |cache| try cache.context.ensureActive();
        try self.token.check();
    }
    fn canceled(raw: *const anyopaque) bool {
        const self: *const Job = @ptrCast(@alignCast(raw));
        self.token.check() catch return true;
        if (self.cached) |cache| cache.context.ensureActive() catch return true;
        return false;
    }
    fn read(self: *Job, alloc: A) ![]u8 {
        const token: Token = .{ .ptr = self, .is_cancelled_fn = canceled, .check_fn = check };
        const bytes = self.store.getRangeAllocWithCancellationUsingAllocator(alloc, self.id, self.begin, self.length, token) catch |err| {
            try check(self);
            return err;
        };
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
                try check(self);
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

/// Logical units and physical jobs have independent bounds. Wide covering
/// packs may contain 1025 stripes, while at most 16 bounded reads run together.
pub const ManyBatch = struct {
    a: A,
    values: []?[]u8,
    pub fn deinit(self: *ManyBatch) void {
        for (self.values) |value| if (value) |bytes| self.a.free(bytes);
        self.a.free(self.values);
    }
};
pub const LeasedBatch = struct {
    a: A,
    values: []?ImmutableLease,
    pub fn deinit(self: *LeasedBatch) void {
        for (self.values) |*value| if (value.*) |*lease| lease.deinit();
        self.a.free(self.values);
    }
};
const Output = union(enum) {
    owned: []?[]u8,
    leased: []?ImmutableLease,
    fn len(self: Output) usize {
        return switch (self) {
            .owned => |v| v.len,
            .leased => |v| v.len,
        };
    }
    fn warm(self: Output, a: A, index: usize, lease: ImmutableLease) !void {
        switch (self) {
            .leased => |v| v[index] = lease,
            .owned => |v| {
                var held = lease;
                defer held.deinit();
                v[index] = try a.dupe(u8, held.bytes());
            },
        }
    }
};
/// Required immutable units retain their physical/cache owner instead of an
/// intermediate payload copy. Unit authentication, persistence and budgets are
/// identical to the owning adapter. The allocator outlives the returned leases.
pub fn loadManyLeased(a: A, store: stores.ArtifactStore, refs: []const Unit, token: Token, cached: ?artifacts.CachedRead, budget: Budget, options: Options) !LeasedBatch {
    if (refs.len > 1025) return error.InvalidGraphPage;
    const values = try a.alloc(?ImmutableLease, refs.len);
    @memset(values, null);
    var result: LeasedBatch = .{ .a = a, .values = values };
    errdefer result.deinit();
    try loadInto(a, store, refs, .{ .leased = values }, token, cached, budget, options);
    return result;
}
pub fn loadMany(a: A, store: stores.ArtifactStore, refs: []const Unit, token: Token, cached: ?artifacts.CachedRead, budget: Budget, options: Options) !ManyBatch {
    if (refs.len > 1025) return error.InvalidGraphPage;
    const values = try a.alloc(?[]u8, refs.len);
    @memset(values, null);
    var result: ManyBatch = .{ .a = a, .values = values };
    errdefer result.deinit();
    try loadInto(a, store, refs, .{ .owned = values }, token, cached, budget, options);
    return result;
}
pub fn load(a: A, store: stores.ArtifactStore, refs: []const Unit, token: Token, cached: ?artifacts.CachedRead, budget: Budget, options: Options) !Batch {
    if (refs.len > max_batch) return error.InvalidGraphPage;
    var result: Batch = .{ .a = a };
    errdefer result.deinit();
    try loadInto(a, store, refs, .{ .owned = result.values[0..refs.len] }, token, cached, budget, options);
    return result;
}
fn loadInto(a: A, store: stores.ArtifactStore, refs: []const Unit, values: Output, token: Token, cached: ?artifacts.CachedRead, budget: Budget, options: Options) !void {
    if (refs.len != values.len() or options.max_range == 0) return error.InvalidGraphPage;
    try token.check();
    if (cached) |cache| try cache.context.ensureActive();
    if (refs.len == 0) return;
    var scratch = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(a));
    defer scratch.deinit();
    const sa = scratch.allocator();
    const key_storage = try sa.alloc([256]u8, refs.len);
    const keys = try sa.alloc([]const u8, refs.len);
    const missing = try sa.alloc(usize, refs.len);
    var count: usize = 0;
    var logical: u64 = 0;
    for (refs, 0..) |ref, i| {
        try stores.validateSha256ArtifactIdentity(ref.object.artifact_id, ref.object.checksum);
        if (ref.bytes == 0 or ref.bytes > options.max_range or ref.offset > ref.object.byte_len or ref.bytes > ref.object.byte_len - ref.offset) return error.InvalidGraphPage;
        keys[i] = if (ref.offset == 0 and ref.bytes == ref.object.byte_len) ref.object.artifact_id else try std.fmt.bufPrint(&key_storage[i], "{s}:page:{x}:{x}", .{ ref.object.artifact_id, ref.offset, ref.bytes });
        logical = std.math.add(u64, logical, ref.bytes) catch return error.InvalidGraphPage;
        if (cached) |cache| if (try cache.cache.lookupImmutableBlockLease(a, cache.scope, keys[i], ref.bytes, ref.digest, cache.context)) |found| {
            try values.warm(a, i, found);
            continue;
        };
        missing[count] = i;
        count += 1;
    }
    const Order = struct {
        units: []const Unit,
        fn less(self: @This(), l: usize, r: usize) bool {
            const order = std.mem.order(u8, self.units[l].object.artifact_id, self.units[r].object.artifact_id);
            return order == .lt or (order == .eq and self.units[l].offset < self.units[r].offset);
        }
    };
    std.mem.sort(usize, missing[0..count], Order{ .units = refs }, Order.less);
    const Run = struct { first: usize, end: usize, offset: u64, length: usize };
    const runs = try sa.alloc(Run, count);
    const ordered = try sa.alloc(Unit, count);
    var run_count: usize = 0;
    var gaps: u64 = 0;
    for (missing[0..count], ordered, 0..) |index, *unit, position| {
        const ref = refs[index];
        unit.* = ref;
        const end = ref.offset + ref.bytes;
        if (run_count != 0) {
            const previous = &runs[run_count - 1];
            const previous_end = previous.offset + previous.length;
            if (std.mem.eql(u8, ordered[previous.first].object.artifact_id, ref.object.artifact_id) and ref.offset <= previous_end +| options.max_gap and @max(end, previous_end) - previous.offset <= options.max_range) {
                gaps = std.math.add(u64, gaps, ref.offset -| previous_end) catch return error.InvalidGraphPage;
                previous.length = @intCast(@max(end, previous_end) - previous.offset);
                previous.end = position + 1;
                continue;
            }
        }
        runs[run_count] = .{ .first = position, .end = position + 1, .offset = ref.offset, .length = ref.bytes };
        run_count += 1;
    }
    // Charge every logical byte and physical gap before provider work starts.
    // Decode batch boundaries never split an otherwise admissible range.
    try budget.charge(budget.ptr, std.math.add(u64, logical, gaps) catch return error.InvalidGraphPage);
    const io = if (cached) |cache| cache.context.io else null;
    var allocations: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = a };
    var first: usize = 0;
    while (first < run_count) {
        const end = @min(first + max_batch, run_count);
        const wave = runs[first..end];
        var jobs: [max_batch]Job = undefined;
        for (wave, jobs[0..wave.len]) |run, *job| job.* = .{ .store = store, .a = allocations.allocator(), .id = ordered[run.first].object.artifact_id, .units = ordered[run.first..run.end], .begin = run.offset, .length = run.length, .token = token, .cached = cached };
        defer for (jobs[0..wave.len]) |*job| job.deinit();
        var ready: std.Io.Event = .unset;
        var tasks: [max_batch]?local.sql_parallel_scheduler.Task(anyerror!void) = @splat(null);
        defer if (io) |active| for (&tasks) |*task| if (task.*) |*held| {
            held.cancel(active) catch {};
        };
        var failure: ?anyerror = null;
        for (jobs[0..wave.len], 0..) |*job, i| {
            if (failure != null) break;
            if (io) |active| if (wave.len > 1) {
                tasks[i] = local.sql_parallel_scheduler.global().submitIoNotified(active, job.length * 2 + 64 * 1024, &ready, Job.run, .{job});
            };
            if (tasks[i] == null) job.run() catch |err| {
                failure = err;
            };
        }
        if (failure) |err| return err;
        if (io) |active| local.sql_parallel_scheduler.joinNotified(active, &ready, &tasks) catch |err| {
            if (cached) |cache| try cache.context.ensureActive();
            try token.check();
            return err;
        };
        const Loader = struct {
            data: []const u8,
            fn read(raw: *anyopaque, alloc: A) ![]u8 {
                return alloc.dupe(u8, @as(*@This(), @ptrCast(@alignCast(raw))).data);
            }
            fn traffic(_: *anyopaque) local.serverless_query_lake_serving_cache.Cache.ProviderRead {
                return .{ .requests = 0, .bytes = 0 };
            }
        };
        for (wave, jobs[0..wave.len]) |run, *job| for (missing[run.first..run.end]) |index| {
            try token.check();
            const ref = refs[index];
            const offset: usize = @intCast(ref.offset - job.begin);
            const bytes = job.result.?[offset..][0..ref.bytes];
            try stores.validatePayloadSha256WithCancellation(bytes, &std.fmt.bytesToHex(&ref.digest, .lower), token);
            var loader: Loader = .{ .data = bytes };
            if (values == .leased) {
                const held = try job.rangeLease(a, offset, ref.bytes);
                values.leased[index] = if (cached) |cache| try cache.cache.admitVerifiedBlock(a, cache.scope, keys[index], ref.bytes, ref.digest, cache.context, .{ .value = .{ .bytes = bytes, .digest = ref.digest }, .owner = held }) else .{ .shared = held };
                continue;
            }
            values.owned[index] = if (cached) |cache| try cache.cache.readImmutableAlloc(a, cache.scope, keys[index], bytes.len, ref.digest, cache.context, .{ .ptr = &loader, .load = Loader.read, .provider_read = Loader.traffic }) else if (run.end - run.first == 1 and job.lease == null and ref.offset == job.begin and ref.bytes == job.length) transfer: {
                job.result = null;
                break :transfer @constCast(bytes);
            } else try a.dupe(u8, bytes);
        };
        first = end;
    }
}

test "external lake metadata navigation wide range plans bound physical jobs charge gaps and unwind partial waves" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("wide-range-plans");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const scope: stores.UploadScope = .{ .domain = @splat(7), .attempt = @splat(3) };
    const payload: [1280]u8 = @splat(37);
    var object = try store.putScoped(scope, &payload, .none);
    defer object.deinit(a);
    var units: [80]Unit = undefined;
    for (&units, 0..) |*unit, i| {
        const start = i * 16;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(payload[start..][0..8], &digest, .{});
        unit.* = .{ .object = .{ .artifact_id = object.artifact_id, .checksum = object.checksum, .byte_len = object.byte_len }, .offset = start, .bytes = 8, .digest = digest };
    }
    const Probe = struct {
        var active: *@This() = undefined;
        original: *const stores.ArtifactStore.VTable,
        calls: usize = 0,
        fail_offset: ?u64 = null,
        fn get(raw: *anyopaque, alloc: A, id: []const u8, offset: u64, length: usize, token: Token) ![]u8 {
            const self = active;
            self.calls += 1;
            if (self.fail_offset == offset) return error.InjectedRangeFailure;
            return if (self.original.get_range_alloc_with_cancellation) |read| read(raw, alloc, id, offset, length, token) else self.original.get_range_alloc(raw, alloc, id, offset, length);
        }
    };
    var probe: Probe = .{ .original = store.vtable };
    Probe.active = &probe;
    var traced = store.vtable.*;
    traced.get_range_alloc_with_cancellation = Probe.get;
    store.vtable = &traced;
    const Charge = struct {
        fn charge(raw: *anyopaque, bytes: u64) !void {
            try stores.chargeReadBudget(@ptrCast(@alignCast(raw)), bytes);
        }
    };
    for ([_]usize{ 0, 8 }) |gap| {
        var remaining: u64 = 16 * 1024;
        const before = probe.calls;
        var loaded = try loadMany(a, store, &units, .none, null, .{ .ptr = &remaining, .charge = Charge.charge }, .{ .max_range = 1024, .max_gap = gap });
        defer loaded.deinit();
        for (units, loaded.values) |unit, value| try std.testing.expectEqualSlices(u8, payload[@intCast(unit.offset)..][0..unit.bytes], value.?);
        try std.testing.expectEqual(@as(usize, if (gap == 0) 80 else 2), probe.calls - before);
        try std.testing.expectEqual(@as(u64, if (gap == 0) 640 else 1264), 16 * 1024 - remaining);
    }
    var remaining: u64 = 0;
    const before = probe.calls;
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, loadMany(a, store, &units, .none, null, .{ .ptr = &remaining, .charge = Charge.charge }, .{ .max_range = 1024 }));
    try std.testing.expectEqual(before, probe.calls);
    probe.fail_offset = units[17].offset;
    remaining = 16 * 1024;
    try std.testing.expectError(error.InjectedRangeFailure, loadMany(a, store, &units, .none, null, .{ .ptr = &remaining, .charge = Charge.charge }, .{ .max_range = 1024 }));
    probe.fail_offset = null;
    const Faults = struct {
        fn run(alloc: A, backend: stores.ArtifactStore, refs: []const Unit) !void {
            var allowance: u64 = 16 * 1024;
            var batch = try loadMany(alloc, backend, refs, .none, null, .{ .ptr = &allowance, .charge = Charge.charge }, .{ .max_range = 1024 });
            defer batch.deinit();
            for (batch.values) |value| try std.testing.expectEqual(@as(usize, 8), value.?.len);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Faults.run, .{ store, @as([]const Unit, units[0..18]) });
}

test "external lake metadata navigation range failures cancel parked siblings before joining" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("range-failure-progress");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const scope: stores.UploadScope = .{ .domain = @splat(7), .attempt = @splat(3) };
    const bytes: [24]u8 = @splat(37);
    var object = try store.putScoped(scope, &bytes, .none);
    defer object.deinit(a);
    var units: [2]Unit = undefined;
    for (&units, 0..) |*unit, i| {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes[i * 16 ..][0..8], &digest, .{});
        unit.* = .{ .object = .{ .artifact_id = object.artifact_id, .checksum = object.checksum, .byte_len = object.byte_len }, .offset = i * 16, .bytes = 8, .digest = digest };
    }
    const Probe = struct {
        var active: *@This() = undefined;
        entered: std.Io.Event = .unset,
        gate: std.Io.Event = .unset,
        live: std.atomic.Value(usize) = .init(0),
        fn get(_: *anyopaque, _: A, _: []const u8, offset: u64, _: usize, _: Token) ![]u8 {
            const self = active;
            _ = self.live.fetchAdd(1, .acq_rel);
            defer _ = self.live.fetchSub(1, .acq_rel);
            if (offset == 0) {
                self.entered.set(std.testing.io);
                // A finite safeguard makes the old ordered-join path fail the
                // oracle instead of hanging the test. Healthy work cancels it.
                try self.gate.waitTimeout(std.testing.io, .{ .duration = .{ .clock = .awake, .raw = .fromSeconds(2) } });
                return error.UnexpectedGateRelease;
            }
            try self.entered.wait(std.testing.io);
            return error.InjectedRangeFailure;
        }
    };
    var probe: Probe = .{};
    Probe.active = &probe;
    var traced = store.vtable.*;
    traced.get_range_alloc_with_cancellation = Probe.get;
    store.vtable = &traced;
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(8), .context = .{ .io = std.testing.io } };
    const Charge = struct {
        fn charge(raw: *anyopaque, length: u64) !void {
            try stores.chargeReadBudget(@ptrCast(@alignCast(raw)), length);
        }
    };
    var remaining: u64 = 1024;
    try std.testing.expectError(error.InjectedRangeFailure, loadMany(a, store, &units, .none, cached, .{ .ptr = &remaining, .charge = Charge.charge }, .{ .max_range = 1024 }));
    try std.testing.expectEqual(@as(usize, 0), probe.live.load(.acquire));
}

test "external lake metadata navigation leased range units retain owners and reduce cold and warm copies" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("leased-range-units");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const payload = try a.alloc(u8, 2 << 20);
    defer a.free(payload);
    for (payload, 0..) |*byte, i| byte.* = @truncate(i *% 17 +% i / 251);
    var object = try store.putScoped(.{ .domain = @splat(7), .attempt = @splat(3) }, payload, .none);
    defer object.deinit(a);
    var units: [32]Unit = undefined;
    for (&units, 0..) |*unit, i| {
        const begin = i * (64 << 10);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(payload[begin..][0 .. 64 << 10], &digest, .{});
        unit.* = .{ .object = .{ .artifact_id = object.artifact_id, .checksum = object.checksum, .byte_len = object.byte_len }, .offset = begin, .bytes = 64 << 10, .digest = digest };
    }
    const Provider = struct {
        var active: *@This() = undefined;
        original: *const stores.ArtifactStore.VTable,
        calls: usize = 0,
        bytes: usize = 0,
        fn read(raw: *anyopaque, alloc: A, id: []const u8, offset: u64, len: usize, token: Token) ![]u8 {
            const self = active;
            self.calls += 1;
            self.bytes += len;
            return if (self.original.get_range_alloc_with_cancellation) |get| get(raw, alloc, id, offset, len, token) else self.original.get_range_alloc(raw, alloc, id, offset, len);
        }
    };
    var provider: Provider = .{ .original = store.vtable };
    Provider.active = &provider;
    var traced = store.vtable.*;
    traced.get_range_alloc_with_cancellation = Provider.read;
    store.vtable = &traced;
    const Charge = struct {
        fn charge(raw: *anyopaque, bytes: u64) !void {
            try stores.chargeReadBudget(@ptrCast(@alignCast(raw)), bytes);
        }
    };
    const Run = struct {
        fn run(backing: A, storage: stores.ArtifactStore, refs: []const Unit, expected: []const u8, cached: ?artifacts.CachedRead, leased: bool) !usize {
            var tracking = std.testing.FailingAllocator.init(backing, .{});
            const alloc = tracking.allocator();
            var output: std.ArrayList([]u8) = .empty;
            defer {
                for (output.items) |bytes| alloc.free(bytes);
                output.deinit(alloc);
            }
            var reads: u64 = 4 << 20;
            if (leased) {
                var batch = try loadManyLeased(alloc, storage, refs, .none, cached, .{ .ptr = &reads, .charge = Charge.charge }, .{ .max_range = 4 << 20 });
                defer batch.deinit();
                for (refs, batch.values) |ref, value| {
                    const bytes = value.?.bytes();
                    try std.testing.expectEqualSlices(u8, expected[@intCast(ref.offset)..][0..ref.bytes], bytes);
                    // Both variants include the decoded owner's required copy.
                    const owned = try alloc.dupe(u8, bytes);
                    errdefer alloc.free(owned);
                    try output.append(alloc, owned);
                }
            } else {
                var batch = try loadMany(alloc, storage, refs, .none, cached, .{ .ptr = &reads, .charge = Charge.charge }, .{ .max_range = 4 << 20 });
                defer batch.deinit();
                for (refs, batch.values) |ref, value| {
                    try std.testing.expectEqualSlices(u8, expected[@intCast(ref.offset)..][0..ref.bytes], value.?);
                    const owned = try alloc.dupe(u8, value.?);
                    errdefer alloc.free(owned);
                    try output.append(alloc, owned);
                }
            }
            try std.testing.expectEqual(@as(u64, expected.len), (4 << 20) - reads);
            return tracking.allocated_bytes;
        }
    };
    for ([_]bool{ false, true }) |cached| {
        var observed: [2][2]usize = undefined;
        for ([_]bool{ false, true }, 0..) |leased, variant| {
            var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
            defer cache.deinit();
            const read: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(8), .context = .{} };
            for (0..2) |pass| {
                const before = provider.calls;
                const bytes = provider.bytes;
                observed[variant][pass] = try Run.run(a, store, &units, payload, if (cached) read else null, leased);
                try std.testing.expectEqual(@as(usize, if (cached and pass == 1) 0 else 1), provider.calls - before);
                try std.testing.expectEqual(@as(usize, if (cached and pass == 1) 0 else payload.len), provider.bytes - bytes);
            }
        }
        for (0..2) |pass| {
            std.debug.print("LEASED_RANGE_UNITS cached={} pass={} owning_allocated={} leased_allocated={} payload_bytes={}\n", .{ cached, pass, observed[0][pass], observed[1][pass], payload.len });
            try std.testing.expect(observed[1][pass] + payload.len / 2 < observed[0][pass]);
        }
    }
    const Faults = struct {
        fn run(alloc: A, storage: stores.ArtifactStore, refs: []const Unit, use_cache: bool) !void {
            var cache = local.serverless_query_lake_serving_cache.Cache.init(std.testing.allocator);
            defer cache.deinit();
            const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(9), .context = .{} };
            var reads: u64 = 4 << 20;
            var batch = try loadManyLeased(alloc, storage, refs, .none, if (use_cache) cached else null, .{ .ptr = &reads, .charge = Charge.charge }, .{ .max_range = 4 << 20 });
            defer batch.deinit();
            for (refs, batch.values) |ref, lease| try std.testing.expectEqual(ref.bytes, lease.?.bytes().len);
        }
    };
    for ([_]bool{ false, true }) |cached| try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Faults.run, .{ store, @as([]const Unit, units[0..3]), cached });
}
