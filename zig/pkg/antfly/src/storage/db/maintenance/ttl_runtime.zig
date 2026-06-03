// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const backend_erased = @import("../../backend_erased.zig");
const backend_scan = @import("../../backend_scan.zig");
const internal_keys = @import("../../internal_keys.zig");
const lsm_backend = @import("../../lsm_backend.zig");
const mem_backend = @import("../../mem_backend.zig");
const schema_mod = @import("../../schema.zig");
const ttl_mod = @import("../../ttl.zig");
const types = @import("../types.zig");
const ownership_mod = @import("../ownership.zig");
const platform_clock = @import("../../../platform/clock.zig");
const background_runtime_mod = @import("../../background_runtime.zig");

pub const Config = struct {
    enabled: bool = builtin.os.tag != .freestanding and !builtin.is_test,
    lease_owned: bool = false,
    owner_id: []const u8 = "local",
    lease_ttl_ms: u64 = 30_000,
    interval_ms: u64 = 30_000,
    batch_size: u32 = 256,
    grace_period_ns: u64 = 5_000_000_000,
    clock: platform_clock.Clock = platform_clock.Clock.real(),
};

pub const DeleteCandidate = struct {
    key: []u8,
    timestamp_ns: u64,

    pub fn deinit(self: *DeleteCandidate, alloc: Allocator) void {
        alloc.free(self.key);
        self.* = undefined;
    }
};

pub const DeleteFn = *const fn (ctx_ptr: *anyopaque, candidates: []const DeleteCandidate) anyerror!u32;

pub const default_lease_key = "\x00\x00__metadata__:ttl_cleanup_lease";

pub const TtlRuntime = if (builtin.os.tag == .freestanding) struct {
    config: Config,
    defer_flag: ?*const std.atomic.Value(bool),
    stats_value: types.TTLCleanupStats = .{},

    pub fn init(
        _: Allocator,
        store: anytype,
        delete_ctx: *anyopaque,
        delete_fn: DeleteFn,
        defer_flag: ?*const std.atomic.Value(bool),
        _: *background_runtime_mod.BackendRuntime,
        config: Config,
    ) !@This() {
        _ = store;
        _ = delete_ctx;
        _ = delete_fn;
        return .{
            .config = config,
            .defer_flag = defer_flag,
            .stats_value = .{
                .enabled = config.enabled,
            },
        };
    }

    pub fn deinit(self: *@This()) void {
        self.* = undefined;
    }

    pub fn start(self: *@This()) !void {
        if (self.config.enabled) return error.UnsupportedPlatform;
    }

    pub fn runOnce(self: *@This()) !void {
        if (self.config.enabled) return error.UnsupportedPlatform;
    }

    pub fn stats(self: *@This()) types.TTLCleanupStats {
        return self.stats_value;
    }
} else struct {
    alloc: Allocator,
    io_impl: ?*Io.Threaded,
    store: backend_erased.Store,
    owns_store: bool,
    delete_ctx: *anyopaque,
    delete_fn: DeleteFn,
    config: Config,
    defer_flag: ?*const std.atomic.Value(bool),
    ownership: ownership_mod.State,
    mutex: Io.Mutex = .init,
    shutdown: bool = false,
    stats_value: types.TTLCleanupStats = .{},
    future: ?Io.Future(void) = null,

    pub fn init(
        alloc: Allocator,
        store: anytype,
        delete_ctx: *anyopaque,
        delete_fn: DeleteFn,
        defer_flag: ?*const std.atomic.Value(bool),
        backend_runtime: *background_runtime_mod.BackendRuntime,
        config: Config,
    ) !TtlRuntime {
        const io_impl = backend_runtime.io_impl;
        if (config.enabled and io_impl == null) return error.MissingBackendRuntimeIo;
        var runtime_store = try initRuntimeStore(alloc, store);
        errdefer runtime_store.deinit();
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .store = runtime_store.store,
            .owns_store = runtime_store.owned,
            .delete_ctx = delete_ctx,
            .delete_fn = delete_fn,
            .config = config,
            .defer_flag = defer_flag,
            .ownership = try ownership_mod.State.init(alloc, store, default_lease_key, .{
                .lease_owned = config.lease_owned,
                .owner_id = config.owner_id,
                .lease_ttl_ms = config.lease_ttl_ms,
            }),
            .stats_value = .{
                .enabled = config.enabled,
            },
        };
    }

    pub fn deinit(self: *TtlRuntime) void {
        if (self.io_impl) |io_impl| {
            const io = io_impl.io();
            self.mutex.lockUncancelable(io);
            self.shutdown = true;
            self.mutex.unlock(io);

            if (self.future) |*future| _ = future.await(io);
        }
        self.future = null;
        self.ownership.deinit(self.alloc);
        if (self.owns_store) self.store.deinit();
        self.* = undefined;
    }

    pub fn start(self: *TtlRuntime) !void {
        if (!self.config.enabled) return;
        const io_impl = self.io_impl orelse return error.MissingBackendRuntimeIo;
        const io = io_impl.io();
        self.future = try io.concurrent(workerMain, .{self});
    }

    pub fn runOnce(self: *TtlRuntime) !void {
        if (!self.config.enabled) return;
        if (workDeferred(self)) return;
        const now_ns = self.config.clock.nowRealtimeNs();
        if (!ensureLease(self, now_ns)) return;
        const summary = try collectAndDelete(self, now_ns);
        recordRun(self, now_ns, summary, false);
    }

    pub fn stats(self: *TtlRuntime) types.TTLCleanupStats {
        const maybe_io = if (self.io_impl) |io_impl| io_impl.io() else null;
        if (maybe_io) |io| self.mutex.lockUncancelable(io);
        defer if (maybe_io) |io| self.mutex.unlock(io);
        var snapshot = self.stats_value;
        const ownership_stats = self.ownership.stats();
        snapshot.lease_owned = ownership_stats.lease_owned;
        snapshot.has_lease = ownership_stats.has_lease;
        snapshot.acquisition_count = ownership_stats.acquisition_count;
        snapshot.lease_acquire_failures = ownership_stats.lease_acquire_failures;
        snapshot.lost_leases = ownership_stats.lost_leases;
        snapshot.last_acquired_ms = ownership_stats.last_acquired_ms;
        return snapshot;
    }
};

const ScanSummary = struct {
    scanned_timestamps: u64 = 0,
    deleted_docs: u32 = 0,
};

fn workerMain(runtime: *TtlRuntime) void {
    while (true) {
        if (isShutdown(runtime)) return;
        if (workDeferred(runtime)) {
            sleepInterval(runtime);
            continue;
        }
        const now_ns = runtime.config.clock.nowRealtimeNs();
        if (!ensureLease(runtime, now_ns)) {
            sleepInterval(runtime);
            continue;
        }
        const summary = collectAndDelete(runtime, now_ns) catch {
            recordRun(runtime, now_ns, .{
                .scanned_timestamps = 0,
                .deleted_docs = 0,
            }, true);
            sleepInterval(runtime);
            continue;
        };
        recordRun(runtime, now_ns, summary, false);
        sleepInterval(runtime);
    }
}

fn workDeferred(runtime: *const TtlRuntime) bool {
    const flag = runtime.defer_flag orelse return false;
    return flag.load(.acquire);
}

fn ensureLease(runtime: *TtlRuntime, now_ns: u64) bool {
    const now_ms: u64 = @intCast(now_ns / std.time.ns_per_ms);
    const io_impl = runtime.io_impl orelse return false;
    const io = io_impl.io();
    runtime.mutex.lockUncancelable(io);
    defer runtime.mutex.unlock(io);
    const acquired = runtime.ownership.ensureLease(now_ms) catch {
        runtime.ownership.noteAcquireFailure();
        return false;
    };
    return acquired;
}

fn collectAndDelete(runtime: *TtlRuntime, now_ns: u64) !ScanSummary {
    const loaded_schema = try schema_mod.loadSchema(runtime.store, runtime.alloc);
    defer if (loaded_schema) |schema| schema_mod.freeSchema(runtime.alloc, schema);

    const duration_ns = if (loaded_schema) |schema|
        schema.ttl_duration_ns
    else
        0;
    if (duration_ns == 0) return .{};

    var candidates = std.ArrayListUnmanaged(DeleteCandidate).empty;
    defer {
        for (candidates.items) |*candidate| candidate.deinit(runtime.alloc);
        candidates.deinit(runtime.alloc);
    }

    var summary = ScanSummary{};

    const ScanState = struct {
        runtime: *TtlRuntime,
        now_ns: u64,
        duration_ns: u64,
        summary: *ScanSummary,
        candidates: *std.ArrayListUnmanaged(DeleteCandidate),

        threadlocal var active: ?*@This() = null;

        fn cb(key: []const u8, value: []const u8) anyerror!backend_scan.ScanAction {
            const self = active.?;
            if (!internal_keys.isTtlKey(key)) return .@"continue";
            self.summary.scanned_timestamps += 1;
            if (value.len < 8) return .@"continue";

            const timestamp_ns = std.mem.readInt(u64, value[0..8], .little);
            if (!ttl_mod.isExpiredWithGrace(timestamp_ns, self.duration_ns, self.runtime.config.grace_period_ns, self.now_ns)) {
                return .@"continue";
            }

            const base_key = (try internal_keys.decodeDocumentComponentAlloc(self.runtime.alloc, key)) orelse return .@"continue";
            try self.candidates.append(self.runtime.alloc, .{
                .key = base_key,
                .timestamp_ns = timestamp_ns,
            });
            if (self.candidates.items.len >= self.runtime.config.batch_size) return .stop;
            return .@"continue";
        }
    };

    var state = ScanState{
        .runtime = runtime,
        .now_ns = now_ns,
        .duration_ns = duration_ns,
        .summary = &summary,
        .candidates = &candidates,
    };
    ScanState.active = &state;
    defer ScanState.active = null;
    try backend_scan.scanCurrent(&runtime.store, "", "", .{}, &ScanState.cb);

    if (candidates.items.len == 0) return summary;
    summary.deleted_docs = try runtime.delete_fn(runtime.delete_ctx, candidates.items);
    return summary;
}

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    fn deinit(self: *@This()) void {
        if (self.owned) self.store.deinit();
    }
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = false };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };

    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
    }

    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

fn sleepInterval(runtime: *TtlRuntime) void {
    var remaining_ms = runtime.config.interval_ms;
    if (remaining_ms == 0) remaining_ms = 1;

    while (remaining_ms > 0) {
        if (isShutdown(runtime)) return;
        const slice_ms: u64 = @min(remaining_ms, 100);
        runtime.config.clock.sleepMs(slice_ms);
        remaining_ms -= slice_ms;
    }
}

fn isShutdown(runtime: *TtlRuntime) bool {
    const io_impl = runtime.io_impl orelse return runtime.shutdown;
    const io = io_impl.io();
    runtime.mutex.lockUncancelable(io);
    defer runtime.mutex.unlock(io);
    return runtime.shutdown;
}

fn recordRun(runtime: *TtlRuntime, now_ns: u64, summary: ScanSummary, failed: bool) void {
    const maybe_io = if (runtime.io_impl) |io_impl| io_impl.io() else null;
    if (maybe_io) |io| runtime.mutex.lockUncancelable(io);
    defer if (maybe_io) |io| runtime.mutex.unlock(io);
    runtime.stats_value.runs += 1;
    runtime.stats_value.scanned_timestamps += summary.scanned_timestamps;
    runtime.stats_value.deleted_docs += summary.deleted_docs;
    runtime.stats_value.last_run_ns = now_ns;
    if (failed) runtime.stats_value.error_count += 1;
}

const TestDeleteContext = struct {
    alloc: Allocator,
    store: *backend_erased.Store,

    fn deleteCandidates(ctx_ptr: *anyopaque, candidates: []const DeleteCandidate) !u32 {
        const ctx: *TestDeleteContext = @ptrCast(@alignCast(ctx_ptr));
        var txn = try ctx.store.beginWrite();
        errdefer txn.abort();

        for (candidates) |candidate| {
            const doc_key = try internal_keys.documentKeyAlloc(ctx.alloc, candidate.key);
            defer ctx.alloc.free(doc_key);
            const ts_key = try internal_keys.ttlKeyAlloc(ctx.alloc, candidate.key);
            defer ctx.alloc.free(ts_key);

            txn.delete(doc_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(ts_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }

        try txn.commit();
        return @intCast(candidates.len);
    }
};

fn putTestDoc(store: *backend_erased.Store, alloc: Allocator, key: []const u8, value: []const u8, timestamp_ns: u64) !void {
    const doc_key = try internal_keys.documentKeyAlloc(alloc, key);
    defer alloc.free(doc_key);
    const ts_key = try internal_keys.ttlKeyAlloc(alloc, key);
    defer alloc.free(ts_key);

    var ts_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &ts_buf, timestamp_ns, .little);

    var txn = try store.beginWrite();
    errdefer txn.abort();
    try txn.put(doc_key, value);
    try txn.put(ts_key, &ts_buf);
    try txn.commit();
}

fn expectMissingDoc(store: *backend_erased.Store, alloc: Allocator, key: []const u8) !void {
    const doc_key = try internal_keys.documentKeyAlloc(alloc, key);
    defer alloc.free(doc_key);
    var txn = try store.beginRead();
    defer txn.abort();
    _ = txn.get(doc_key) catch |err| {
        try std.testing.expect(err == error.NotFound);
        return;
    };
    return error.TestExpectedError;
}

test "ttl runtime runOnce works with memory backend store" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    var runtime_store = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime_store.deinit();

    try schema_mod.saveSchema(runtime_store, alloc, .{ .version = 1, .default_type = "doc", .ttl_duration_ns = 1_000 });
    try putTestDoc(&runtime_store, alloc, "doc1", "value", 1_000);

    var delete_ctx = TestDeleteContext{ .alloc = alloc, .store = &runtime_store };
    var backend_runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{});
    defer backend_runtime.deinit();
    var clock = platform_clock.ManualClock{};
    clock.setRealtimeNs(10_000);
    var runtime = try TtlRuntime.init(alloc, runtime_store, &delete_ctx, TestDeleteContext.deleteCandidates, null, backend_runtime.ptr(), .{
        .enabled = true,
        .clock = clock.clock(),
        .grace_period_ns = 0,
        .batch_size = 8,
    });
    defer runtime.deinit();

    try runtime.runOnce();

    const stats = runtime.stats();
    try std.testing.expectEqual(@as(u32, 1), stats.deleted_docs);
    try std.testing.expectEqual(@as(u64, 1), stats.scanned_timestamps);
    try expectMissingDoc(&runtime_store, alloc, "doc1");
}

test "ttl runtime runOnce works with lsm backend store" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime_store = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime_store.deinit();

    try schema_mod.saveSchema(runtime_store, alloc, .{ .version = 1, .default_type = "doc", .ttl_duration_ns = 1_000 });
    try putTestDoc(&runtime_store, alloc, "doc1", "value", 1_000);

    var delete_ctx = TestDeleteContext{ .alloc = alloc, .store = &runtime_store };
    var backend_runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{});
    defer backend_runtime.deinit();
    var clock = platform_clock.ManualClock{};
    clock.setRealtimeNs(10_000);
    var runtime = try TtlRuntime.init(alloc, runtime_store, &delete_ctx, TestDeleteContext.deleteCandidates, null, backend_runtime.ptr(), .{
        .enabled = true,
        .clock = clock.clock(),
        .grace_period_ns = 0,
        .batch_size = 8,
    });
    defer runtime.deinit();

    try runtime.runOnce();

    const stats = runtime.stats();
    try std.testing.expectEqual(@as(u32, 1), stats.deleted_docs);
    try std.testing.expectEqual(@as(u64, 1), stats.scanned_timestamps);
    try expectMissingDoc(&runtime_store, alloc, "doc1");
}
