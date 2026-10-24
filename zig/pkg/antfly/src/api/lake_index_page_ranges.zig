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
const Context = local.serverless_query_lake_read_context.Context;
const Execution = local.common_request_admission.runnable_scheduler.Execution;
const scheduling = local.sql_parallel_scheduler;

/// Temporary wave payloads outlive task joins, so their reservation is separate
/// from worker metadata. A reserved child consumes no CPU and retires only after
/// its buffers and physical-cache leases are gone.
pub const Workspace = struct {
    execution: Execution = .{},
    pub fn reserve(self: *Workspace, context: Context, bytes: usize) !bool {
        const parent = context.execution orelse return false;
        const io = context.io orelse return false;
        try context.ensureActive();
        // Verified bounded reads promise no fan-out, even on configurations
        // where the bounded lane shares ordinary CPU capacity.
        if (parent.entry) |entry| if (entry.read_lane == .bounded) return false;
        const retained = std.math.add(u64, bytes, @sizeOf(Workspace)) catch return error.InvalidRunnableDemand;
        self.execution.reserveChild(parent, io, retained) catch |err| {
            if (err == error.SchedulerFull or err == error.ExecutionWaitUnsupported) return false;
            return err;
        };
        return true;
    }
    pub fn deinit(self: *Workspace) void {
        self.execution.finish();
    }
};

fn copy(a: A, bytes: []const u8, context: Context) ![]u8 {
    try context.ensureActive();
    const output = try a.alloc(u8, bytes.len);
    errdefer a.free(output);
    var offset: usize = 0;
    while (offset != bytes.len) {
        const end = offset + @min(16 * 1024, bytes.len - offset);
        @memcpy(output[offset..end], bytes[offset..end]);
        offset = end;
        try context.ensureActive();
    }
    return output;
}
fn discardVoid(_: A, _: void) void {}
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
    const Read = struct {
        job: *Job,
        context: Context,
        fn canceled(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            self.job.token.check() catch return true;
            // Provider polling runs while parked; it must never reacquire CPU.
            self.context.checkLifetime() catch return true;
            return false;
        }
        fn read(self: *@This(), alloc: A) ![]u8 {
            const job = self.job;
            const token: Token = .{ .ptr = self, .is_cancelled_fn = canceled };
            const bytes = try job.store.getRangeAllocFor(alloc, job.id, job.begin, job.length, token, self.context);
            errdefer alloc.free(bytes);
            if (bytes.len != job.length) return error.ArtifactIntegrityMismatch;
            if (job.cached) |cache| cache.cache.recordPhysicalRead(bytes.len);
            // Authenticate on this worker's CPU, after physical IO has resumed.
            for (job.refs) |ref| {
                try self.context.ensureActive();
                const id = try pages.PageStore.identity(job.domain, ref);
                if (!std.mem.eql(u8, job.id, &id) or ref.offset() < job.begin or @as(u64, ref.offset()) + ref.bytes > job.begin + job.length) continue;
                const offset: usize = @intCast(ref.offset() - job.begin);
                try stores.validatePayloadSha256For(bytes[offset..][0..ref.bytes], &std.fmt.bytesToHex(&ref.digest, .lower), token, self.context);
            }
            return bytes;
        }
        fn fill(raw: *anyopaque, item: *Decoded.Item) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const a = item.arena.allocator();
            const value = a.create(Physical) catch |err| {
                if (err == error.OutOfMemory and item.budget.isLocallyExhausted()) return error.SqlMemoryLimitExceeded;
                return err;
            };
            const bytes = self.read(a) catch |err| {
                if (err == error.OutOfMemory and item.budget.isLocallyExhausted()) return error.SqlMemoryLimitExceeded;
                return err;
            };
            value.* = .{ .start = self.job.begin, .bytes = bytes };
            item.payload = .{ .extension = value };
        }
    };
    fn run(self: *Job) anyerror!void {
        var context: Context = if (self.cached) |cache| cache.context else .{};
        // Legacy workers have no borrowed CPU. Their caller validates its own
        // checkpoint before dispatch and after joining; lifetime still travels.
        if (context.execution == null and context.wait == null) context.checkpoint = null;
        return self.runFor(context);
    }
    fn runChild(self: *Job, execution: *Execution) anyerror!void {
        return self.runFor(self.cached.?.context.withExecution(execution));
    }
    fn runFor(self: *Job, context: Context) anyerror!void {
        try self.token.check();
        try context.ensureActive();
        var reader: Read = .{ .job = self, .context = context };
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
            var current = context;
            current.cancellation = .fromCallback(&reader, Read.canceled);
            const lease = cache.cache.physical.acquireCovered(hash.finalResult(), self.length * 2 + 64 * 1024, current, .{ .ptr = &reader, .load = Read.fill }, .{ .domain = domain, .start = self.begin, .end = self.begin + self.length }) catch |err| fallback: {
                try self.token.check();
                try context.ensureActive();
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
        self.result = try reader.read(self.a);
    }
};

pub fn load(a: A, store: stores.ArtifactStore, domain: [32]u8, refs: []const tree.Ref, token: Token, cached: ?artifacts.CachedRead, budget: Budget) !Batch {
    if (refs.len > tree.max_read_batch) return error.InvalidGraphPage;
    try token.check();
    const context: Context = if (cached) |cache| cache.context else .{};
    try context.ensureActive();
    var result: Batch = .{ .a = a, .refs = refs };
    errdefer result.deinit();
    var ids: [tree.max_read_batch][175]u8 = undefined;
    var key_storage: [tree.max_read_batch][256]u8 = undefined;
    var keys: [tree.max_read_batch][]const u8 = undefined;
    var missing: [tree.max_read_batch]usize = undefined;
    var count: usize = 0;
    var logical: u64 = 0;
    for (refs, 0..) |ref, i| {
        try ref.validate();
        ids[i] = try pages.PageStore.identity(domain, ref);
        keys[i] = if (ref.offset() == 0 and ref.bytes == ref.artifactBytes()) &ids[i] else try std.fmt.bufPrint(&key_storage[i], "{s}:page:{x}:{x}", .{ ids[i], ref.offset(), ref.bytes });
        logical += ref.bytes;
        if (cached) |cache| if (try cache.cache.lookupImmutableBlockLease(a, cache.scope, keys[i], ref.bytes, ref.digest, cache.context)) |found| {
            var lease = found;
            defer lease.deinit();
            result.values[i] = try copy(a, lease.bytes(), context);
            continue;
        };
        missing[count] = i;
        count += 1;
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
    var temporary: local.sql_memory_budget = .{ .backing = local.sql_memory_budget.reclaimingAllocator(a), .limit = std.math.maxInt(usize) };
    var allocations: scheduling.LockedAllocator = .{ .backing = temporary.allocator() };
    var jobs: [tree.max_read_batch]Job = undefined;
    var group_for: [tree.max_read_batch]usize = undefined;
    var groups: usize = 0;
    var gap_bytes: u64 = 0;
    for (missing[0..count]) |i| {
        const ref = refs[i];
        const end = @as(u64, ref.offset()) + ref.bytes;
        if (groups != 0) {
            const previous = &jobs[groups - 1];
            if (std.mem.eql(u8, previous.id, &ids[i]) and ref.offset() <= previous.begin + previous.length + 16 * 1024 and
                @max(end, previous.begin + previous.length) - previous.begin <= tree.max_page_bytes)
            {
                const length: usize = @intCast(@max(end, previous.begin + previous.length) - previous.begin);
                gap_bytes += ref.offset() -| (previous.begin + previous.length);
                previous.length = length;
                group_for[i] = groups - 1;
                continue;
            }
        }
        jobs[groups] = .{ .store = store, .a = allocations.allocator(), .id = &ids[i], .begin = ref.offset(), .length = ref.bytes, .token = token, .domain = domain, .refs = refs, .cached = cached };
        group_for[i] = groups;
        groups += 1;
    }
    // Worker metadata retires at join; payloads remain live through result
    // copying. Fund that distinct lifetime before allocating or dispatching IO.
    var workspace: Workspace = .{};
    defer workspace.deinit();
    if (context.execution != null and groups != 0) {
        var bytes: usize = 0;
        for (jobs[0..groups]) |job| bytes += job.length * 2 + 64 * 1024;
        if (!try workspace.reserve(context, bytes)) {
            // Capacity refusal keeps the required, caller-funded page path.
            // No speculative worker or temporary wave allocation was published.
            try budget.charge(budget.ptr, logical);
            for (missing[0..count]) |i| {
                result.values[i] = try artifacts.readPageRangeFor(a, store, .{ .artifact_id = &ids[i], .checksum = ids[i][7..71], .byte_len = refs[i].artifactBytes() }, refs[i].offset(), refs[i].bytes, refs[i].digest, token, cached, context);
            }
            return result;
        }
        temporary.limit = bytes;
    }
    // Cache hits retain the ordinary logical read charge. Coalesced gaps are
    // additional physical traffic and cannot bypass the request's allowance.
    try budget.charge(budget.ptr, logical + gap_bytes);
    defer for (jobs[0..groups]) |*job| job.deinit();
    var tasks: [tree.max_read_batch]?local.sql_parallel_scheduler.Task(anyerror!void) = @splat(null);
    const io = if (cached) |cache| cache.context.io else null;
    defer if (io) |active| for (&tasks) |*task| if (task.*) |*held| {
        held.cancel(active) catch {};
    };
    var failure: ?anyerror = null;
    for (jobs[0..groups], 0..) |*job, i| {
        if (failure != null) break;
        try context.ensureActive();
        if (io) |active| if (groups > 1) {
            if (context.execution) |execution| {
                const scope: scheduling.ExecutionScope = .{ .execution = execution, .allocator = allocations.allocator(), .io = active, .wait = context.wait orelse local.sql_spill.childWait(execution) };
                tasks[i] = scope.submit(0, allocations.allocator(), Job.runChild, .{job}, discardVoid) catch |err| fallback: {
                    if (err != error.ConcurrencyUnavailable) return err;
                    break :fallback null;
                };
            } else if (context.wait == null) tasks[i] = scheduling.global().submitIo(active, job.length * 2 + 64 * 1024, Job.run, .{job});
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
    try context.ensureActive();
    const Loader = struct {
        data: []const u8,
        context: Context,
        fn read(raw: *anyopaque, alloc: A) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return copy(alloc, self.data, self.context);
        }
        fn traffic(_: *anyopaque) local.serverless_query_lake_serving_cache.Cache.ProviderRead {
            return .{ .requests = 0, .bytes = 0 };
        }
    };
    for (missing[0..count]) |i| {
        try token.check();
        try context.ensureActive();
        const job = jobs[group_for[i]];
        const offset: usize = @intCast(refs[i].offset() - job.begin);
        const bytes = job.result.?[offset..][0..refs[i].bytes];
        try stores.validatePayloadSha256For(bytes, &std.fmt.bytesToHex(&refs[i].digest, .lower), token, context);
        var loader: Loader = .{ .data = bytes, .context = context };
        result.values[i] = if (cached) |cache| try cache.cache.readImmutableAlloc(a, cache.scope, keys[i], bytes.len, refs[i].digest, cache.context, .{ .ptr = &loader, .load = Loader.read, .provider_read = Loader.traffic }) else try copy(a, bytes, context);
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
        // Duplicate logical seeks do not provide credit for physical gap IO.
        const repeated = [_]tree.Ref{ gap[0], gap[0], gap[1] };
        remaining = 384;
        try std.testing.expectError(error.ArtifactReadBudgetExceeded, load(a, backend, scope.domain, &repeated, .none, null, .{ .ptr = &remaining, .charge = Charge.charge }));
        remaining = 400;
        var duplicates = try load(a, backend, scope.domain, &repeated, .none, null, .{ .ptr = &remaining, .charge = Charge.charge });
        defer duplicates.deinit();
        try std.testing.expectEqual(@as(u64, 0), remaining);
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

test "external lake navigation scoped waves retain payload credit through joins and preserve original rejection" {
    const Runnable = local.common_request_admission.runnable_scheduler;
    const Case = enum { success, child_refused, wave_refused, bounded, wait_only, dispatch_refused, allocation_failure, budget_refused, canceled, deadline, revoked, io_failure, resume_denied };
    const Clock = struct {
        var value: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return value.load(.acquire);
        }
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    const Probe = struct {
        base: stores.ArtifactStore,
        parent: *Execution,
        owner: *Runnable.Scheduler,
        case: Case,
        signal: std.atomic.Value(bool) = .init(false),
        revoked: std.atomic.Value(bool) = .init(false),
        reads: std.atomic.Value(usize) = .init(0),
        controls: std.atomic.Value(usize) = .init(0),
        children: std.atomic.Value(usize) = .init(0),
        retained_joins: usize = 0,
        fn authority(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.revoked.load(.acquire)) return error.FileNotFound;
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.parent.checkpoint();
            try self.parent.park(.resource_blocked);
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // Task.join has released its child metadata, but its returned bytes
            // still belong to the wave until copying and physical retirement.
            if (self.case == .wave_refused or self.case == .bounded or self.case == .wait_only)
                try std.testing.expectEqual(@as(u64, 17), self.owner.stats().retained_bytes)
            else
                try std.testing.expect(self.owner.stats().retained_bytes > 17);
            self.retained_joins += 1;
            if (self.case == .resume_denied) return error.FileNotFound;
            try self.parent.enter(.query);
            try authority(raw);
        }
        fn get(raw: *anyopaque, a: A, id: []const u8, offset: u64, length: usize, token: Token) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.owner.state(&self.parent.entry.?) != .running);
            if (self.owner.stats().child_operations >= 2) _ = self.children.fetchAdd(1, .monotonic);
            var control: Execution = .{};
            try control.start(self.owner, .{ .io = std.testing.io, .allow_wait = false }, .control);
            try control.checkpoint();
            control.finish();
            _ = self.controls.fetchAdd(1, .monotonic);
            _ = self.reads.fetchAdd(1, .monotonic);
            if (self.case == .io_failure) return error.FileNotFound;
            const bytes = try self.base.getRangeAllocWithCancellationUsingAllocator(a, id, offset, length, token);
            switch (self.case) {
                .canceled => self.signal.store(true, .release),
                .deadline => Clock.value.store(10, .release),
                .revoked => self.revoked.store(true, .release),
                else => {},
            }
            return bytes;
        }
    };
    const Charge = struct {
        fn charge(raw: *anyopaque, bytes: u64) !void {
            try stores.chargeReadBudget(@ptrCast(@alignCast(raw)), bytes);
        }
    };
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("scoped-navigation-waves");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const domain: [32]u8 = @splat(81);
    const upload: stores.UploadScope = .{ .domain = domain, .attempt = @splat(3) };
    var references: [3]tree.Ref = undefined;
    for (0..2) |pack| {
        var payload: [512]u8 = @splat(@intCast(71 + pack));
        var artifact = try store.putScoped(upload, &payload, .none);
        defer artifact.deinit(a);
        var pack_digest: [32]u8 = undefined;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(&payload, &pack_digest, .{});
        std.crypto.hash.sha2.Sha256.hash(payload[0..128], &digest, .{});
        if (pack == 0) {
            for (references[0..2], 0..) |*ref, i| ref.* = .{ .digest = digest, .attempt = upload.attempt, .bytes = 128, .height = 0, .records = 1, .pack = .{ .digest = pack_digest, .bytes = payload.len, .offset = @intCast(i * 144) } };
        } else references[2] = .{ .digest = digest, .attempt = upload.attempt, .bytes = 128, .height = 0, .records = 1, .pack = .{ .digest = pack_digest, .bytes = payload.len, .offset = 0 } };
    }
    const refs = [_]tree.Ref{ references[0], references[1], references[2], references[0] };
    for (std.enums.values(Case)) |case| {
        var owner = try Runnable.Scheduler.init(.{ .slots = 2, .control_slots = 1, .max_operations = if (case == .wave_refused) 2 else if (case == .child_refused) 3 else 16, .max_bytes = 4 * 1024 * 1024, .control_bytes = 64 * 1024 });
        defer owner.close();
        var threads = std.Io.Threaded.init(a, .{ .concurrent_limit = .limited(0) });
        defer threads.deinit();
        var parent: Execution = .{};
        defer parent.finish();
        var probe: Probe = .{ .base = store, .owner = &owner, .parent = &parent, .case = case };
        Clock.value.store(0, .release);
        try parent.startRead(&owner, .{ .io = std.testing.io, .retained_bytes = 17, .deadline_ns = 10, .native_now_ns = Clock.now, .cancellation = .{ .ptr = &probe.signal, .requested = Clock.requested } }, if (case == .bounded) .bounded else .general);
        var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
        defer cache.deinit();
        var vtable = store.vtable.*;
        vtable.get_range_alloc_with_cancellation = Probe.get;
        var observed = store;
        observed.ptr = &probe;
        observed.vtable = &vtable;
        var context = (Context{ .io = if (case == .dispatch_refused) threads.io() else std.testing.io, .authority = .{ .ptr = &probe, .check = Probe.authority } }).withExecution(&parent);
        if (case == .wait_only) context.execution = null;
        context.wait = .{ .ptr = &probe, .begin = Probe.begin, .end = Probe.end };
        const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(38), .context = context };
        var remaining: u64 = if (case == .budget_refused) 1 else 4096;
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        const result = load(if (case == .allocation_failure) failing.allocator() else a, observed, domain, &refs, .none, cached, .{ .ptr = &remaining, .charge = Charge.charge });
        switch (case) {
            .success, .child_refused, .wave_refused, .bounded, .wait_only, .dispatch_refused => {
                var batch = try result;
                defer batch.deinit();
                for (batch.values[0..4], [_]u8{ 71, 71, 72, 71 }) |bytes, value| for (bytes.?) |byte| try std.testing.expectEqual(value, byte);
                try std.testing.expectEqual(@as(usize, if (case == .wave_refused or case == .bounded) 3 else 2), probe.reads.load(.acquire));
                try std.testing.expectEqual(@as(u64, 4096 - 512 - @as(u64, if (case == .wave_refused or case == .bounded) 0 else 16)), remaining);
                if (case == .success) {
                    try std.testing.expect(probe.children.load(.acquire) > 0);
                    try std.testing.expect(probe.retained_joins > 0);
                } else try std.testing.expectEqual(@as(usize, 0), probe.children.load(.acquire));
                const before = probe.reads.load(.acquire);
                var warm = try load(a, observed, domain, &refs, .none, cached, .{ .ptr = &remaining, .charge = Charge.charge });
                defer warm.deinit();
                try std.testing.expectEqual(before, probe.reads.load(.acquire));
            },
            .allocation_failure => {
                try std.testing.expectError(error.OutOfMemory, result);
                try std.testing.expectEqual(@as(usize, 0), probe.reads.load(.acquire));
            },
            .budget_refused => {
                try std.testing.expectError(error.ArtifactReadBudgetExceeded, result);
                try std.testing.expectEqual(@as(usize, 0), probe.reads.load(.acquire));
            },
            .canceled => try std.testing.expectError(error.Canceled, result),
            .deadline => try std.testing.expectError(error.DeadlineExceeded, result),
            .revoked, .io_failure, .resume_denied => try std.testing.expectError(error.FileNotFound, result),
        }
        try std.testing.expectEqual(probe.reads.load(.acquire), probe.controls.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), owner.stats().child_operations);
        try std.testing.expectEqual(@as(u64, 17), owner.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 0), cache.physical.flights.count());
        parent.finish();
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
        if (case == .canceled or case == .deadline or case == .revoked or case == .resume_denied) {
            // A different connection may reuse immutable physical bytes, never
            // the failed leader's source authority or execution capsule.
            var healthy: Execution = .{};
            defer healthy.finish();
            var next: Probe = .{ .base = store, .owner = &owner, .parent = &healthy, .case = .success };
            Clock.value.store(0, .release);
            try healthy.start(&owner, .{ .io = std.testing.io, .retained_bytes = 17, .native_now_ns = Clock.now }, .query);
            var backend = observed;
            backend.ptr = &next;
            const original = (Context{ .io = std.testing.io, .authority = .{ .ptr = &next, .check = Probe.authority } }).withExecution(&healthy);
            remaining = 4096;
            var again = try load(a, backend, domain, &refs, .none, .{ .cache = &cache, .scope = cached.scope, .context = original }, .{ .ptr = &remaining, .charge = Charge.charge });
            defer again.deinit();
            for (again.values[0..4], [_]u8{ 71, 71, 72, 71 }) |bytes, value| for (bytes.?) |byte| try std.testing.expectEqual(value, byte);
            try std.testing.expectEqual(@as(usize, 0), owner.stats().child_operations);
            healthy.finish();
            try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
        }
    }
}
