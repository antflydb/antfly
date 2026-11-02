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

//! Navigation adapter for the shared authenticated range planner.
const std = @import("std");
const local = @import("antfly_local_sources");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const pages = @import("../serverless/graph_segment/page_store.zig");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const ranges = @import("lake_artifact_ranges.zig");
const A = std.mem.Allocator;
const Context = local.serverless_query_lake_read_context.Context;
const Execution = local.common_request_admission.runnable_scheduler.Execution;
pub const Workspace = ranges.Workspace;
const Token = @import("antfly_cancellation").CancellationToken;
pub const Budget = ranges.Budget;
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
pub fn load(a: A, store: stores.ArtifactStore, domain: [32]u8, refs: []const tree.Ref, token: Token, cached: ?artifacts.CachedRead, budget: Budget) !Batch {
    if (refs.len > tree.max_read_batch) return error.InvalidGraphPage;
    var ids: [tree.max_read_batch][175]u8 = undefined;
    var units: [tree.max_read_batch]ranges.Unit = undefined;
    for (refs, 0..) |ref, i| {
        try ref.validate();
        ids[i] = try pages.PageStore.identity(domain, ref);
        units[i] = .{ .object = .{ .artifact_id = &ids[i], .checksum = ids[i][7..71], .byte_len = ref.artifactBytes() }, .offset = ref.offset(), .bytes = ref.bytes, .digest = ref.digest };
    }
    const batch = try ranges.load(a, store, units[0..refs.len], token, cached, budget, .{ .max_range = tree.max_page_bytes, .max_gap = 16 * 1024 });
    return .{ .a = a, .refs = refs, .values = batch.values[0..tree.max_read_batch].* };
}

test "external lake metadata navigation range waves coalesce authenticate cache and bound budgets" {
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
        fn getFor(raw: *anyopaque, alloc: A, id: []const u8, offset: u64, length: usize, token: Token, context: Context) ![]u8 {
            return local.sql_execution_wait.runOwned(context.wait, get, .{ raw, alloc, id, offset, length, token }, discardRange, .{alloc});
        }
        fn discardRange(range_payload: []u8, alloc: A) void {
            alloc.free(range_payload);
        }
    };
    var probe: Probe = .{ .vtable = backend.vtable };
    Probe.active = &probe;
    var vtable = backend.vtable.*;
    vtable.get_range_alloc_with_cancellation = Probe.get;
    vtable.get_range_alloc_for = Probe.getFor;
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
        fn getFor(raw: *anyopaque, alloc: A, id: []const u8, offset: u64, length: usize, token: Token, context: Context) ![]u8 {
            return local.sql_execution_wait.runOwned(context.wait, get, .{ raw, alloc, id, offset, length, token }, discardRange, .{alloc});
        }
        fn discardRange(range_payload: []u8, alloc: A) void {
            alloc.free(range_payload);
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
        errdefer std.debug.print("navigation original-scope case={s}\n", .{@tagName(case)});
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
        vtable.get_range_alloc_for = Probe.getFor;
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
                // Main's authenticated coalescing also applies to serial,
                // caller-funded fallback. It dispatches no child and charges
                // the same actual gap bytes as the parallel path.
                try std.testing.expectEqual(@as(usize, 2), probe.reads.load(.acquire));
                try std.testing.expectEqual(@as(u64, 4096 - 512 - 16), remaining);
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
