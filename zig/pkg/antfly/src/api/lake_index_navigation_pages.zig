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

//! Shared immutable decoded navigation, independent of request authority.
//! Cold waves coalesce authenticated bytes; warm pages retain cache-owned leases.
const std = @import("std");
const local = @import("antfly_local_sources");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const pages = @import("../serverless/graph_segment/page_store.zig");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const ranges = @import("lake_index_page_ranges.zig");
const Decoded = local.serverless_query_lake_decoded_cache;
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;
const Value = struct { page: tree.Page };
fn release(owner: *anyopaque, item: *anyopaque) void {
    const lease: Decoded.Lease = .{ .cache = @ptrCast(@alignCast(owner)), .item = @ptrCast(@alignCast(item)) };
    lease.release();
}
fn page(lease: Decoded.Lease) tree.Page {
    const value: *const Value = @ptrCast(@alignCast(lease.item.payload.extension));
    var result = value.page;
    result.lease = .{ .owner = lease.cache, .item = lease.item, .release = release };
    return result;
}
pub const Reader = struct {
    store: stores.ArtifactStore,
    domain: [32]u8,
    cached: artifacts.CachedRead,
    token: Token,
    fn check(self: *const Reader) !void {
        try self.token.check();
        try self.cached.context.ensureActive();
    }
    fn key(self: *const Reader, ref: tree.Ref) [32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("native-lake-shared-navigation-page-v1");
        hash.update(&self.cached.scope);
        hash.update(&self.domain);
        hash.update(&ref.digest);
        hash.update(&ref.attempt);
        if (ref.pack) |pack_| {
            hash.update(&pack_.digest);
            var locator: [8]u8 = undefined;
            std.mem.writeInt(u32, locator[0..4], pack_.bytes, .big);
            std.mem.writeInt(u32, locator[4..8], pack_.offset, .big);
            hash.update(&locator);
        }
        var shape: [13]u8 = undefined;
        std.mem.writeInt(u32, shape[0..4], ref.bytes, .big);
        shape[4] = ref.height;
        std.mem.writeInt(u64, shape[5..13], ref.records, .big);
        hash.update(&shape);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
    /// Authority is checked even when a caller reuses a warm-page allowance.
    pub fn lookup(self: *const Reader, ref: tree.Ref) !?tree.Page {
        try ref.validate();
        try self.check();
        const held = self.cached.cache.navigation.lookup(self.key(ref)) orelse return null;
        errdefer held.release();
        try self.check();
        return page(held);
    }
    pub fn read(self: *const Reader, a: A, ref: tree.Ref, encoded: ?[]const u8) !tree.Page {
        try ref.validate();
        try self.check();
        const Loader = struct {
            reader: *const Reader,
            ref: tree.Ref,
            encoded: ?[]const u8,
            fn bytes(self_: *@This(), out: A) ![]u8 {
                if (self_.encoded) |data| return out.dupe(u8, data);
                const id = try pages.PageStore.identity(self_.reader.domain, self_.ref);
                return artifacts.readPageRange(out, self_.reader.store, .{ .artifact_id = &id, .checksum = id[7..71], .byte_len = self_.ref.artifactBytes() }, self_.ref.offset(), self_.ref.bytes, self_.ref.digest, self_.reader.token, self_.reader.cached);
            }
            fn load(raw: *anyopaque, item: *Decoded.Item) !void {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                const out = item.arena.allocator();
                fill(self_, item, out) catch |err| {
                    if (err == error.OutOfMemory and item.budget.isLocallyExhausted()) return error.SqlMemoryLimitExceeded;
                    return err;
                };
            }
            fn fill(self_: *@This(), item: *Decoded.Item, out: A) !void {
                const value = try out.create(Value);
                value.* = .{ .page = try tree.decodePage(out, self_.reader.domain, self_.ref, try self_.bytes(out)) };
                item.payload = .{ .extension = value };
            }
        };
        var loader: Loader = .{ .reader = self, .ref = ref, .encoded = encoded };
        const Check = struct {
            reader: *const Reader,
            fn active(raw: *anyopaque) !void {
                try @as(*@This(), @ptrCast(@alignCast(raw))).reader.check();
            }
        };
        var check_: Check = .{ .reader = self };
        var context = self.cached.context;
        context.checkpoint = .{ .ptr = &check_, .check = Check.active };
        const held = self.cached.cache.navigation.acquire(self.key(ref), tree.readWorkspace(ref) * 2, context, .{ .ptr = &loader, .load = Loader.load }) catch |err| {
            if (err != error.SqlMemoryLimitExceeded) return err;
            try self.check();
            return tree.decodePage(a, self.domain, ref, try loader.bytes(a));
        };
        errdefer held.release();
        try self.check();
        return page(held);
    }
    pub fn many(self: *const Reader, a: A, refs: []const tree.Ref, output: []tree.Page, budget: ranges.Budget) !void {
        if (refs.len != output.len or refs.len > tree.max_read_batch) return error.InvalidGraphPage;
        var ready: [tree.max_read_batch]?tree.Page = @splat(null);
        defer for (&ready) |*value| if (value.*) |*held| held.deinit(a);
        var missing: [tree.max_read_batch]tree.Ref = undefined;
        var positions: [tree.max_read_batch]usize = undefined;
        var count: usize = 0;
        var warm_bytes: u64 = 0;
        var cold_bytes: usize = 0;
        for (refs, 0..) |ref, i| {
            try ref.validate();
            try self.check();
            if (self.cached.cache.navigation.lookup(self.key(ref))) |lease| {
                ready[i] = page(lease);
                warm_bytes += ref.bytes;
            } else {
                missing[count] = ref;
                positions[count] = i;
                cold_bytes += ref.bytes;
                count += 1;
            }
        }
        try budget.charge(budget.ptr, warm_bytes);
        if (count != 0) {
            const reservation = @min(24 * 1024 * 1024, cold_bytes * 3 + count * 32 * 1024 + 256 * 1024);
            const scheduler = local.sql_parallel_scheduler.global();
            var workspace_owner: ranges.Workspace = .{};
            defer workspace_owner.deinit();
            // Encoded output copies survive the inner range-worker joins. Their
            // owner is separate from that loader's temporary physical buffers.
            const scoped = self.cached.context.execution != null;
            const admitted = if (scoped)
                try workspace_owner.reserve(self.cached.context, cold_bytes + count * 32 * 1024 + 256 * 1024)
            else
                scheduler.reservePull(0, reservation);
            if (admitted) {
                defer if (!scoped) scheduler.releasePull(0, reservation);
                var workspace: local.sql_memory_budget = .{ .backing = local.sql_memory_budget.reclaimingAllocator(a), .limit = reservation };
                // Charge the wave once, including coalesced gaps. A declined
                // optional copy must leave the required page path available.
                const Charge = struct {
                    target: ranges.Budget,
                    charged: bool = false,
                    fn apply(raw: *anyopaque, bytes: u64) !void {
                        const self_: *@This() = @ptrCast(@alignCast(raw));
                        try self_.target.charge(self_.target.ptr, bytes);
                        self_.charged = true;
                    }
                };
                var charge: Charge = .{ .target = budget };
                var encoded: ?ranges.Batch = ranges.load(workspace.allocator(), self.store, self.domain, missing[0..count], self.token, self.cached, .{ .ptr = &charge, .charge = Charge.apply }) catch |err| fallback: {
                    if (err != error.OutOfMemory or !workspace.isExhausted()) return err;
                    try self.check();
                    if (!charge.charged) for (missing[0..count]) |ref| try budget.charge(budget.ptr, ref.bytes);
                    break :fallback null;
                };
                defer if (encoded) |*value| value.deinit();
                for (missing[0..count], positions[0..count], 0..) |ref, i, j| ready[i] = try self.read(a, ref, if (encoded) |value| value.values[j] else null);
            } else for (missing[0..count], positions[0..count]) |ref, i| {
                try budget.charge(budget.ptr, ref.bytes);
                ready[i] = try self.read(a, ref, null);
            }
        }
        try self.check();
        for (output, ready[0..refs.len]) |*out, *value| {
            out.* = value.*.?;
            value.* = null;
        }
    }
};

test "external lake shared navigation leases reuse decoded pages and preserve authority budgets and shapes" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("shared-navigation-pages");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const scope = try stores.UploadScope.forPublication(@splat(5), 1, std.testing.io);
    store.upload_scope = scope;
    var reads: u64 = 16 * 1024 * 1024;
    var writes = reads;
    var storage: pages.PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var roots: [2]tree.Ref = undefined;
    for (&roots, [_][]const u8{ "one", "two" }) |*root, value| {
        var builder: tree.SortedBuilder = .{ .alloc = a, .store = storage.store() };
        defer builder.deinit();
        try builder.add(value, value);
        root.* = (try builder.finish()).?;
    }
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    var reader: Reader = .{ .store = store, .domain = scope.domain, .token = .none, .cached = .{ .cache = &cache, .scope = @splat(9), .context = .{ .io = std.testing.io } } };
    const Charge = struct {
        fn charge(raw: *anyopaque, bytes: u64) !void {
            try stores.chargeReadBudget(@ptrCast(@alignCast(raw)), bytes);
        }
    };
    const refs = [_]tree.Ref{ roots[1], roots[0], roots[1] };
    // Optional encoded copies cannot fail required cache-owned page reads.
    var narrow: local.sql_memory_budget = .{ .backing = a, .limit = 1 };
    const before_narrow = reads;
    var recovered: [3]tree.Page = undefined;
    try reader.many(narrow.allocator(), &refs, &recovered, .{ .ptr = &reads, .charge = Charge.charge });
    for (&recovered) |*value| value.deinit(narrow.allocator());
    try std.testing.expectEqual(before_narrow - refs[0].bytes - refs[1].bytes - refs[2].bytes, reads);
    reader.cached.scope = @splat(20);
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, reader.many(failing.allocator(), &refs, &recovered, .{ .ptr = &reads, .charge = Charge.charge }));
    reader.cached.scope = @splat(9);
    var first: [3]tree.Page = undefined;
    try reader.many(a, &refs, &first, .{ .ptr = &reads, .charge = Charge.charge });
    defer for (&first) |*value| value.deinit(a);
    try std.testing.expect(first[0].entries.ptr == first[2].entries.ptr);
    const traffic = cache.snapshot().provider_reads;
    const before = reads;
    var second: [3]tree.Page = undefined;
    try reader.many(a, &refs, &second, .{ .ptr = &reads, .charge = Charge.charge });
    defer for (&second) |*value| value.deinit(a);
    for (first, second) |left, right| try std.testing.expect(left.entries.ptr == right.entries.ptr);
    try std.testing.expectEqual(traffic, cache.snapshot().provider_reads);
    try std.testing.expectEqual(before - refs[0].bytes - refs[1].bytes - refs[2].bytes, reads);
    var denied: u64 = 1;
    var rejected: [3]tree.Page = undefined;
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, reader.many(a, &refs, &rejected, .{ .ptr = &denied, .charge = Charge.charge }));
    var wrong = roots[0];
    wrong.records += 1;
    try std.testing.expectError(error.InvalidGraphPage, reader.read(a, wrong, null));
    reader.cached.context.deadline_ns = @import("antfly_platform").time.monotonicNs() -| 1;
    try std.testing.expectError(error.DeadlineExceeded, reader.read(a, roots[0], null));
    reader.cached.context.deadline_ns = null;
    reader.cached.scope = @splat(10);
    cache.navigation.max_loading_bytes = 1;
    var fallback = try reader.read(a, roots[0], null);
    defer fallback.deinit(a);
    try std.testing.expect(fallback.lease == null);
    try std.testing.expectEqualStrings("one", fallback.entries[0].value);
}

test "external lake shared navigation scoped copies use node capacity and retire before output escapes" {
    const Runnable = local.common_request_admission.runnable_scheduler;
    const Execution = Runnable.Execution;
    const Probe = struct {
        store: stores.ArtifactStore,
        owner: *Runnable.Scheduler,
        parent: *Execution,
        calls: std.atomic.Value(usize) = .init(0),
        children: std.atomic.Value(usize) = .init(0),
        fn before(self: *@This()) !void {
            try std.testing.expect(self.owner.state(&self.parent.entry.?) != .running);
            if (self.owner.stats().child_operations != 0) _ = self.children.fetchAdd(1, .monotonic);
            var control: Execution = .{};
            try control.start(self.owner, .{ .io = std.testing.io, .allow_wait = false }, .control);
            try control.checkpoint();
            control.finish();
            _ = self.calls.fetchAdd(1, .monotonic);
        }
        fn get(raw: *anyopaque, alloc: A, id: []const u8, offset: u64, length: usize, token: Token) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.before();
            return self.store.getRangeAllocWithCancellationUsingAllocator(alloc, id, offset, length, token);
        }
        fn bounded(raw: *anyopaque, alloc: A, id: []const u8, max: usize, token: Token) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.before();
            const bytes = try self.store.getAllocWithCancellationUsingAllocator(alloc, id, token);
            if (bytes.len > max) {
                alloc.free(bytes);
                return error.ArtifactTooLarge;
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
    var directory = try local.common_test_directory.TestDirectory.init("scoped-decoded-navigation");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var backend = fs.artifactStore();
    const upload: stores.UploadScope = .{ .domain = @splat(19), .attempt = @splat(14) };
    backend.upload_scope = upload;
    var reads: u64 = 8 * 1024 * 1024;
    var writes = reads;
    var page_store: pages.PageStore = .{ .domain = upload.domain, .attempt = upload.attempt, .artifacts = &backend, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var roots: [2]tree.Ref = undefined;
    for (&roots, [_][]const u8{ "first", "second" }) |*ref, text| {
        var builder: tree.SortedBuilder = .{ .alloc = a, .store = page_store.store() };
        defer builder.deinit();
        try builder.add(text, text);
        ref.* = (try builder.finish()).?;
    }
    // A scoped caller must not need the legacy process-global pull allowance.
    const legacy = local.sql_parallel_scheduler.global();
    const occupied = legacy.max_pull_bytes;
    try std.testing.expect(legacy.reservePull(0, occupied));
    defer legacy.releasePull(0, occupied);
    for ([_]bool{ false, true }) |refused| {
        var owner = try Runnable.Scheduler.init(.{ .slots = 2, .control_slots = 1, .max_operations = 16, .max_bytes = if (refused) 128 * 1024 else 4 * 1024 * 1024, .control_bytes = 64 * 1024 });
        defer owner.close();
        var execution: Execution = .{};
        defer execution.finish();
        try execution.start(&owner, .{ .io = std.testing.io, .retained_bytes = 17 }, .query);
        var probe: Probe = .{ .store = backend, .owner = &owner, .parent = &execution };
        var vtable = backend.vtable.*;
        vtable.get_range_alloc_with_cancellation = Probe.get;
        vtable.get_bounded_alloc_with_cancellation = Probe.bounded;
        var observed = backend;
        observed.ptr = &probe;
        observed.vtable = &vtable;
        var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
        defer cache.deinit();
        var reader: Reader = .{ .store = observed, .domain = upload.domain, .token = .none, .cached = .{ .cache = &cache, .scope = @splat(22), .context = (local.serverless_query_lake_read_context.Context{ .io = std.testing.io }).withExecution(&execution) } };
        const wanted = [_]tree.Ref{ roots[1], roots[0], roots[1] };
        var output: [3]tree.Page = undefined;
        const before = reads;
        try reader.many(a, &wanted, &output, .{ .ptr = &reads, .charge = Charge.charge });
        defer for (&output) |*owned| owned.deinit(a);
        try std.testing.expectEqualSlices(u8, "second", output[0].entries[0].value);
        try std.testing.expectEqualSlices(u8, "first", output[1].entries[0].value);
        try std.testing.expect(output[0].entries.ptr == output[2].entries.ptr);
        try std.testing.expectEqual(@as(usize, 2), probe.calls.load(.acquire));
        try std.testing.expectEqual(!refused, probe.children.load(.acquire) != 0);
        try std.testing.expectEqual(before - roots[0].bytes - 2 * roots[1].bytes, reads);
        try std.testing.expectEqual(@as(usize, 0), owner.stats().child_operations);
        try std.testing.expectEqual(@as(u64, 17), owner.stats().retained_bytes);
        try std.testing.expectEqual(occupied, legacy.pull_bytes);
        execution.finish();
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    }
}
