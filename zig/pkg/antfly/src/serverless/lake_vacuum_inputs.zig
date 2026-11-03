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

//! Operation-owned temporary inventories. Closing admission through the same CAS
//! as writer registration fences new planners. Collection waits for registered
//! writers to release or expire, including the snapshot-pin clock-skew grace.
const std = @import("std");
const storage = @import("objectstore");
const catalog = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog;
const platform = @import("antfly_platform");
const A = std.mem.Allocator;
const Context = catalog.types.Context;
pub const lease_ms: u64 = 60_000;
pub const grace_ms: u64 = 30_000;
pub const max_deletes: u32 = 16;
const Registration = struct { id: [32]u8, expires_ms: u64 };
const State = struct {
    version: u8 = 1,
    closed: bool = false,
    collected: bool = false,
    leases: []const Registration = &.{},
    after: ?[]const u8 = null,
};
pub const Clock = struct {
    ptr: ?*const anyopaque = null,
    read: *const fn (?*const anyopaque) u64 = now,
    fn now(_: ?*const anyopaque) u64 {
        return @intCast(platform.time.realtimeNs() / std.time.ns_per_ms);
    }
};
pub const Store = struct {
    client: storage.Client,
    bucket: []const u8,
    prefix: []const u8,
    state_key: []const u8,
    planning_key: []const u8,
    legacy_prefix: []const u8,
    context: Context,
    clock: Clock = .{},

    pub fn init(a: A, client: storage.Client, bucket: []const u8, base: []const u8, operation: []const u8, context: Context) !Store {
        const hash = catalog.types.digestHex(operation);
        return .{
            .client = client,
            .bucket = bucket,
            .prefix = try std.fmt.allocPrint(a, "{s}/maintenance/vacuum-inputs/v2/{s}/", .{ base, hash }),
            .state_key = try std.fmt.allocPrint(a, "{s}/maintenance/vacuum-input-state/{s}.json", .{ base, hash }),
            .planning_key = try std.fmt.allocPrint(a, "{s}/maintenance/vacuum-planning/{s}.json", .{ base, hash }),
            .legacy_prefix = try std.fmt.allocPrint(a, "{s}/maintenance/vacuum-inputs/", .{base}),
            .context = context,
        };
    }
    fn get(self: Store, a: A) !?storage.GetResult {
        try self.context.ensureActive();
        var client = self.client;
        client.allocator = a;
        return client.getObject(self.bucket, self.state_key, .{ .skip_metadata_probe = true, .max_response_bytes = 64 * 1024, .cancellation = catalog.types.contextCancellation(&self.context) }) catch |err| switch (err) {
            error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
            else => return err,
        };
    }
    fn parse(a: A, saved: ?storage.GetResult) !State {
        const state = if (saved) |value| try std.json.parseFromSliceLeaky(State, a, value.body, .{ .allocate = .alloc_always }) else State{};
        if (state.version != 1 or state.leases.len > 64 or (state.collected and !state.closed)) return error.InvalidLakeVacuumPlan;
        return state;
    }
    fn save(self: Store, a: A, state: State, saved: ?storage.GetResult) !void {
        try self.context.ensureActive();
        const bytes = try std.json.Stringify.valueAlloc(a, state, .{});
        if (bytes.len > 64 * 1024) return error.InvalidLakeVacuumPlan;
        var client = self.client;
        client.allocator = a;
        var stored = try client.compareAndPut(self.bucket, self.state_key, bytes, if (saved) |value| value.metadata.writeRevision() orelse return error.MissingObjectRevision else null, .{ .cancellation = catalog.types.contextCancellation(&self.context) });
        stored.deinit(a);
    }
    pub const Lease = struct {
        store: Store,
        id: [32]u8,
        expires_ms: u64,
        deadline_ns: u64,
        fn canceled(raw: *const anyopaque) bool {
            const self: *const Lease = @ptrCast(@alignCast(raw));
            // Deadline and checkpoint hooks remain on the copied Context, so
            // their original errors are preserved by ensureActive().
            if (self.store.context.cancellation) |token| if (token.isCancelled()) return true;
            return self.store.clock.read(self.store.clock.ptr) >= self.expires_ms;
        }
        pub fn context(self: *const Lease) Context {
            var result = self.store.context;
            result.deadline_ns = if (result.deadline_ns) |prior| @min(prior, self.deadline_ns) else self.deadline_ns;
            result.cancellation = .{ .ptr = self, .is_cancelled_fn = canceled };
            return result;
        }
        pub fn release(self: *const Lease, a: A) !void {
            for (0..4) |_| {
                var arena = std.heap.ArenaAllocator.init(a);
                defer arena.deinit();
                const scratch = arena.allocator();
                var saved = (try self.store.get(scratch)) orelse return error.InvalidLakeVacuumPlan;
                defer saved.deinit(scratch);
                var state = try parse(scratch, saved);
                var leases: std.ArrayList(Registration) = .empty;
                for (state.leases) |entry| if (!std.mem.eql(u8, &entry.id, &self.id)) try leases.append(scratch, entry);
                if (leases.items.len == state.leases.len) return;
                state.leases = leases.items;
                self.store.save(scratch, state, saved) catch |err| switch (err) {
                    error.PreconditionFailed => continue,
                    else => return err,
                };
                return;
            }
            return error.PreconditionFailed;
        }
    };
    pub fn acquire(self: Store, a: A) !Lease {
        var saved = try self.get(a);
        defer if (saved) |*value| value.deinit(a);
        var state = try parse(a, saved);
        if (state.closed) return error.LakeVacuumPlanClosed;
        const now_ms = self.clock.read(self.clock.ptr);
        var leases: std.ArrayList(Registration) = .empty;
        for (state.leases) |entry| if (entry.expires_ms +| grace_ms >= now_ms) try leases.append(a, entry);
        if (leases.items.len >= 64) return error.LakeVacuumBusy;
        var threaded: ?std.Io.Threaded = if (self.context.io == null) std.Io.Threaded.init(a, .{}) else null;
        defer if (threaded) |*io| io.deinit();
        var random: [16]u8 = undefined;
        const io = self.context.io orelse threaded.?.io();
        io.random(&random);
        const entry: Registration = .{ .id = std.fmt.bytesToHex(random, .lower), .expires_ms = now_ms +| lease_ms };
        try leases.append(a, entry);
        state.leases = leases.items;
        const lease: Lease = .{ .store = self, .id = entry.id, .expires_ms = entry.expires_ms, .deadline_ns = platform.time.monotonicNs() +| lease_ms * std.time.ns_per_ms };
        self.save(a, state, saved) catch |err| {
            // Resolve a successful registration whose response was lost.
            var latest = (self.get(a) catch return err) orelse return err;
            defer latest.deinit(a);
            const actual = parse(a, latest) catch return err;
            for (actual.leases) |candidate| if (std.mem.eql(u8, &candidate.id, &entry.id) and candidate.expires_ms == entry.expires_ms) return lease;
            return err;
        };
        return lease;
    }
    /// Call only after the immutable job (including a no-op job) is durable.
    pub fn close(self: Store, a: A) !void {
        var saved = try self.get(a);
        defer if (saved) |*value| value.deinit(a);
        var state = try parse(a, saved);
        if (state.closed) return;
        state.closed = true;
        try self.save(a, state, saved);
    }
    fn remove(self: Store, a: A, key: []const u8) !void {
        var client = self.client;
        client.allocator = a;
        var metadata = client.statObjectWithOptions(self.bucket, key, .{ .cancellation = catalog.types.contextCancellation(&self.context) }) catch |err| switch (err) {
            error.NotFound, error.ObjectNotFound, error.FileNotFound => return,
            else => return err,
        };
        defer metadata.deinit(a);
        client.compareAndDelete(self.bucket, key, metadata.writeRevision() orelse return error.MissingObjectRevision, .{ .cancellation = catalog.types.contextCancellation(&self.context) }) catch |err| switch (err) {
            error.NotFound, error.ObjectNotFound, error.FileNotFound => {},
            else => return err,
        };
    }
    /// Compact closure state is retained to reject late writer registration;
    /// large inputs and the mutable planner are no longer needed for replay.
    pub fn finish(self: Store, a: A) !bool {
        var saved = try self.get(a);
        defer if (saved) |*value| value.deinit(a);
        var state = try parse(a, saved);
        if (!state.closed) return error.InvalidLakeVacuumPlan;
        if (!state.collected) {
            const now_ms = self.clock.read(self.clock.ptr);
            for (state.leases) |entry| if (entry.expires_ms +| grace_ms >= now_ms) return false;
            // A planner that started before closure may have published its
            // checkpoint late. Delete it only after writer quiescence.
            try self.remove(a, self.planning_key);
            var client = self.client;
            client.allocator = a;
            var listed = try client.listObjects(self.bucket, .{ .prefix = self.prefix, .start_after = state.after, .max_keys = max_deletes, .cancellation = catalog.types.contextCancellation(&self.context) });
            defer listed.deinit(a);
            for (listed.entries) |entry| {
                if (!std.mem.startsWith(u8, entry.key, self.prefix)) return error.InvalidLakeVacuumPlan;
                try self.remove(a, entry.key);
                state.after = try a.dupe(u8, entry.key);
            }
            state.leases = &.{};
            state.collected = listed.entries.len == 0;
            if (state.collected) state.after = null;
            self.save(a, state, saved) catch |err| switch (err) {
                error.PreconditionFailed => return false,
                else => return err,
            };
            if (!state.collected) return false;
        }
        return self.finishLegacy(a);
    }
    fn finishLegacy(self: Store, a: A) !bool {
        // Pre-scoped caches are disposable optimizations. New planners never
        // read these two directories; never sweep v2 scopes or catalog proofs.
        var client = self.client;
        client.allocator = a;
        var empty = true;
        inline for (.{ "inventories/", "chunks/" }) |kind| {
            const prefix = try std.fmt.allocPrint(a, "{s}{s}", .{ self.legacy_prefix, kind });
            var listed = try client.listObjects(self.bucket, .{ .prefix = prefix, .max_keys = max_deletes / 2, .cancellation = catalog.types.contextCancellation(&self.context) });
            defer listed.deinit(a);
            if (listed.entries.len != 0) empty = false;
            for (listed.entries) |entry| {
                if (!std.mem.startsWith(u8, entry.key, prefix)) return error.InvalidLakeVacuumPlan;
                try self.remove(a, entry.key);
            }
        }
        return empty;
    }
};

const TestClock = struct {
    ms: u64 = 1000,
    fn read(raw: ?*const anyopaque) u64 {
        const self: *const @This() = @ptrCast(@alignCast(raw.?));
        return self.ms;
    }
    fn clock(self: *const @This()) Clock {
        return .{ .ptr = self, .read = read };
    }
};
fn put(a: A, client_value: storage.Client, bucket: []const u8, key: []const u8, body: []const u8) !void {
    var client = client_value;
    var value = try client.putObject(bucket, key, body, .{});
    value.deinit(a);
}

test "external lake vacuum inputs close admission wait for writers and isolate concurrent operations" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = storage.MemoryClient.init(alloc);
    defer memory.deinit();
    var clock: TestClock = .{};
    var first = try Store.init(a, memory.client(), "journal", "table", "first", .{});
    first.clock = clock.clock();
    const second = try Store.init(a, memory.client(), "journal", "table", "second", .{});
    var writer = try first.acquire(a);
    var other = try second.acquire(a);
    defer other.release(a) catch {};
    const first_key = try std.fmt.allocPrint(a, "{s}chunks/input.bin", .{first.prefix});
    const other_key = try std.fmt.allocPrint(a, "{s}chunks/input.bin", .{second.prefix});
    try put(alloc, memory.client(), "journal", first_key, "first inventory");
    try put(alloc, memory.client(), "journal", other_key, "other active inventory");
    try first.close(a);
    try std.testing.expectError(error.LakeVacuumPlanClosed, first.acquire(a));
    try std.testing.expect(!try first.finish(a));
    var client = memory.client();
    var held = try client.getObject("journal", first_key, .{});
    held.deinit(alloc);
    try writer.release(a);
    try put(alloc, memory.client(), "journal", first.planning_key, "late checkpoint");
    try std.testing.expect(!try first.finish(a));
    try std.testing.expect(try first.finish(a));
    try std.testing.expectError(error.FileNotFound, client.getObject("journal", first_key, .{}));
    try std.testing.expectError(error.FileNotFound, client.getObject("journal", first.planning_key, .{}));
    var untouched = try client.getObject("journal", other_key, .{});
    defer untouched.deinit(alloc);
    try std.testing.expectEqualStrings("other active inventory", untouched.body);
}

test "external lake vacuum inputs recover abandoned writer leases after deadline and grace" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = storage.MemoryClient.init(alloc);
    defer memory.deinit();
    var clock: TestClock = .{};
    var store = try Store.init(a, memory.client(), "journal", "table", "crash", .{});
    store.clock = clock.clock();
    var writer = try store.acquire(a);
    const key = try std.fmt.allocPrint(a, "{s}chunks/input.bin", .{store.prefix});
    try put(alloc, memory.client(), "journal", key, "inventory");
    try store.close(a);
    clock.ms += lease_ms;
    try std.testing.expectError(error.Canceled, writer.context().ensureActive());
    try std.testing.expect(!try store.finish(a));
    clock.ms += grace_ms + 1;
    try std.testing.expect(!try store.finish(a));
    try std.testing.expect(try store.finish(a));
    try std.testing.expectError(error.LakeVacuumPlanClosed, store.acquire(a));
}

test "external lake vacuum inputs cleanup is bounded resumes lost outcomes and reaps legacy caches" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = storage.MemoryClient.init(alloc);
    defer memory.deinit();
    var faults = storage.ScriptedFaultClient.init(alloc, memory.client());
    defer faults.deinit();
    const store = try Store.init(a, faults.client(), "journal", "table", "bounded", .{});
    faults.next_put = .{ .commit_then_fail = error.ConnectionResetByPeer };
    var writer = try store.acquire(a);
    try writer.release(a);
    for (0..40) |n| try put(alloc, memory.client(), "journal", try std.fmt.allocPrint(a, "{s}chunks/{d:0>3}.bin", .{ store.prefix, n }), "inventory");
    try put(alloc, memory.client(), "journal", "table/maintenance/vacuum-inputs/chunks/legacy.bin", "legacy pack");
    try put(alloc, memory.client(), "journal", "table/maintenance/vacuum-inputs/inventories/legacy.json", "legacy receipt");
    try put(alloc, memory.client(), "journal", "table/metadata/antfly-catalog/reachability/chunks/proof.bin", "authoritative proof");
    try store.close(a);
    faults.next_delete = .{ .commit_then_fail = error.ConnectionResetByPeer };
    try std.testing.expectError(error.ConnectionResetByPeer, store.finish(a));
    faults.next_put = .{ .commit_then_fail = error.ConnectionResetByPeer };
    try std.testing.expectError(error.ConnectionResetByPeer, store.finish(a));
    var complete = false;
    for (0..16) |_| {
        const before = faults.delete_attempts;
        complete = try store.finish(a);
        try std.testing.expect(faults.delete_attempts - before <= max_deletes + 1);
        if (complete) break;
    }
    try std.testing.expect(complete);
    var client = memory.client();
    var listed = try client.listObjects("journal", .{ .prefix = "table/maintenance/vacuum-inputs/" });
    defer listed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), listed.entries.len);
    var proof = try client.getObject("journal", "table/metadata/antfly-catalog/reachability/chunks/proof.bin", .{});
    defer proof.deinit(alloc);
    try std.testing.expectEqualStrings("authoritative proof", proof.body);
    try std.testing.expect(try store.finish(a));
}

test "external lake vacuum inputs preserve caller checkpoint errors" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = storage.MemoryClient.init(alloc);
    defer memory.deinit();
    const Check = struct {
        fail: bool = false,
        fn run(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail) return error.InjectedCallerCheckpoint;
        }
    };
    var check: Check = .{};
    const store = try Store.init(a, memory.client(), "journal", "table", "caller-context", .{ .checkpoint = .{ .ptr = &check, .check = Check.run } });
    var writer = try store.acquire(a);
    check.fail = true;
    try std.testing.expectError(error.InjectedCallerCheckpoint, writer.context().ensureActive());
    check.fail = false;
    try writer.release(a);
}
