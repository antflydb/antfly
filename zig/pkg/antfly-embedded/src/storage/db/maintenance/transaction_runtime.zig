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

const native_platform = @import("antfly_platform");
const std = @import("std");

const driver = @import("transaction_recovery_driver.zig");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const backend_erased = @import("../../backend_erased.zig");
const lsm_backend = @import("../../lsm_backend.zig");
const mem_backend = @import("../../mem_backend.zig");
const transactions_mod = @import("../../transactions.zig");
const types = @import("../types.zig");
const platform_clock = native_platform.clock;
const background_runtime_mod = @import("../../background_runtime.zig");

pub const Config = @import("../transaction_recovery_contract.zig").Config;

pub const default_lease_key = driver.default_lease_key;

const LocalRuntime = driver.Driver(Policy);

const Policy = struct {
    pub const Config = @import("../transaction_recovery_contract.zig").Config;
    pub const runPage = runRecoveryPageWithConfig;
    pub fn validate(config: @This().Config) !void {
        _ = config;
    }
};

const RunSummary = driver.RunSummary;

fn runRecoveryPageWithConfig(
    alloc: Allocator,
    store: anytype,
    config: Config,
    now_ns: u64,
    after: ?transactions_mod.TxnId,
    limit: usize,
) !RunSummary {
    var summary: RunSummary = .{};
    var manager = try transactions_mod.TxnManager.init(alloc, try backend_erased.storeFrom(alloc, store));
    defer manager.deinit();
    const page = try manager.listTransactionsPage(alloc, after, limit);
    defer alloc.free(page.items);
    summary.next_scan_after = page.next_after;

    var admitted: usize = 0;
    for (page.items) |txn| {
        if (txn.status == .pending) {
            page.items[admitted] = txn;
            admitted += 1;
            continue;
        }
        if (try manager.hasIntents(txn.txn_id) or try manager.hasReplicationOutbox(txn.txn_id)) {
            const resolve = config.resolve_local_fn orelse return error.MissingLocalTransactionResolver;
            resolve(config.local_resolution_ctx orelse return error.MissingLocalTransactionResolver, txn.txn_id, txn.status, txn.commit_version) catch {
                summary.record_failures += 1;
                continue;
            };
        }
        page.items[admitted] = txn;
        admitted += 1;
    }

    const cutoff = now_ns -| config.cutoff_ns;
    summary.recovery = try manager.recoverTransactionSummariesWithExtraBatchHooksAndOptions(
        page.items[0..admitted],
        cutoff,
        now_ns,
        config.resolution_extra_hooks,
        .{
            .presume_abort_distributed = false,
            .retained_cutoff_timestamp = now_ns -| config.retained_terminal_ns,
        },
    );
    summary.recovery.scanned_records += summary.record_failures;
    summary.recovery.deferred_unresolved += summary.record_failures;
    return summary;
}

const RuntimeStoreHandle = driver.RuntimeStoreHandle;
const initRuntimeStore = driver.initRuntimeStore;

const contract = @import("../transaction_recovery_contract.zig");
pub const Runtime = struct {
    local: ?LocalRuntime = null,
    external: ?contract.OwnedRuntime = null,
    external_store: ?RuntimeStoreHandle = null,

    pub fn init(alloc: Allocator, store: anytype, background: *background_runtime_mod.BackendRuntime, config: Config) !Runtime {
        if (config.enabled) if (config.factory) |factory| {
            var runtime_store = try initRuntimeStore(alloc, store);
            errdefer runtime_store.deinit();
            const external = try factory.create(factory.ptr, alloc, runtime_store.store, background, .{
                .resolution_extra_hooks = config.resolution_extra_hooks,
                .local_resolution_ctx = config.local_resolution_ctx,
                .resolve_local_fn = config.resolve_local_fn,
            });
            return .{ .external = external, .external_store = runtime_store };
        };
        return .{ .local = try LocalRuntime.init(alloc, store, background, config) };
    }
    pub fn deinit(self: *Runtime) void {
        if (self.external) |runtime| {
            runtime.vtable.deinit(runtime.ptr);
            if (self.external_store) |*store| store.deinit();
            self.* = undefined;
            return;
        }
        return self.local.?.deinit();
    }
    pub fn start(self: *Runtime) anyerror!void {
        if (self.external) |runtime| return try runtime.vtable.start(runtime.ptr);
        return try self.local.?.start();
    }
    pub fn stop(self: *Runtime) bool {
        if (self.external) |runtime| return runtime.vtable.stop(runtime.ptr);
        return self.local.?.stop();
    }
    pub fn pause(self: *Runtime) bool {
        if (self.external) |runtime| return runtime.vtable.pause(runtime.ptr);
        return self.local.?.pause();
    }
    pub fn resumeAfterPause(self: *Runtime) anyerror!void {
        if (self.external) |runtime| return try runtime.vtable.resume_after_pause(runtime.ptr);
        return try self.local.?.resumeAfterPause();
    }
    pub fn ensureRunning(self: *Runtime) anyerror!bool {
        if (self.external) |runtime| return try runtime.vtable.ensure_running(runtime.ptr);
        return try self.local.?.ensureRunning();
    }
    pub fn isStarted(self: *Runtime) bool {
        if (self.external) |runtime| return runtime.vtable.is_started(runtime.ptr);
        return self.local.?.isStarted();
    }
    pub fn beginTeardown(self: *Runtime) void {
        if (self.external) |runtime| return runtime.vtable.teardown(runtime.ptr);
        if (comptime builtin.os.tag != .freestanding) if (self.local) |*runtime| runtime.beginTeardown();
    }
    pub fn stats(self: *Runtime) types.TransactionRecoveryStats {
        if (self.external) |runtime| return runtime.vtable.stats(runtime.ptr);
        return self.local.?.stats();
    }
    pub fn runOnce(self: *Runtime) anyerror!void {
        if (self.external) |runtime| return try runtime.vtable.run_once(runtime.ptr);
        return try self.local.?.runOnce();
    }
};

test "local transaction recovery preserves failed resolution and makes bounded progress" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "local-recovery" });
    defer store.deinit();
    var manager = try transactions_mod.TxnManager.init(alloc, &store);
    defer manager.deinit();
    const poison: transactions_mod.TxnId = @splat(1);
    const healthy: transactions_mod.TxnId = @splat(2);
    const pending: transactions_mod.TxnId = @splat(3);
    for ([_]transactions_mod.TxnId{ poison, healthy }) |id| {
        try manager.initTransaction(id, 1_000);
        try manager.writeIntents(id, &.{.{ .key = &id, .value = "{}" }}, &.{});
        const outbox = transactions_mod.makeTransactionReplicationBatchOutboxKey(id);
        _ = try manager.resolveIntentsWithExtraBatch(id, .committed, 2_000, .{ .writes = &.{.{ .key = &outbox, .value = "batch" }} });
    }
    try manager.initTransactionWithParticipants(pending, 1_000, &.{"remote"});
    const Resolver = struct {
        store: *backend_erased.Store,
        calls: usize = 0,
        fn resolve(raw: *anyopaque, id: transactions_mod.TxnId, _: transactions_mod.TxnStatus, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (std.mem.eql(u8, &id, &poison)) return error.PoisonTransaction;
            var txn_manager = try transactions_mod.TxnManager.init(std.testing.allocator, self.store);
            defer txn_manager.deinit();
            try txn_manager.clearReplicationOutbox(id, .batch);
        }
    };
    var resolver = Resolver{ .store = &store };
    const config: Config = .{ .enabled = true, .cutoff_ns = 500, .local_resolution_ctx = &resolver, .resolve_local_fn = Resolver.resolve };
    const first = try runRecoveryPageWithConfig(alloc, store, config, 3_000, null, 2);
    try std.testing.expectEqual(@as(usize, 2), resolver.calls);
    try std.testing.expectEqual(@as(u64, 1), first.record_failures);
    try std.testing.expect(try manager.hasReplicationOutbox(poison));
    try std.testing.expect(!try manager.hasReplicationOutbox(healthy));
    try std.testing.expect(first.next_scan_after != null);
    const second = try runRecoveryPageWithConfig(alloc, store, config, 3_000, first.next_scan_after, 2);
    try std.testing.expectEqual(@as(u64, 0), second.recovery.auto_aborted);
    try std.testing.expectEqual(transactions_mod.TxnStatus.pending, try manager.getTransactionStatus(pending));
}

test "local transaction recovery factory releases owned adapters on initialization failure" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "factory-failure" });
    defer store.deinit();
    var background = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{ .backend = .manual, .filesystem_io = native_platform.testing.io });
    defer background.deinit();
    const Failure = struct {
        fn create(_: *anyopaque, _: Allocator, _: backend_erased.Store, _: *background_runtime_mod.BackendRuntime, _: contract.CreateContext) !contract.OwnedRuntime {
            return error.FactoryInitializationFailed;
        }
    };
    // Force creation of an owned erased adapter around a borrowed backend.
    // The caller's store remains live after factory failure and runtime close.
    const Handle = struct {
        store: backend_erased.Store,
        pub fn backendStore(self: *@This()) backend_erased.Store {
            return self.store;
        }
    };
    var handle = Handle{ .store = store };
    var ctx: u8 = 0;
    try std.testing.expectError(error.FactoryInitializationFailed, Runtime.init(alloc, &handle, background.ptr(), .{
        .enabled = true,
        .factory = .{ .ptr = &ctx, .create = Failure.create },
    }));
    var disabled = try Runtime.init(alloc, &handle, background.ptr(), .{
        .enabled = false,
        .factory = .{ .ptr = &ctx, .create = Failure.create },
    });
    defer disabled.deinit();
    try std.testing.expect(disabled.external == null);
    try disabled.start();
    try std.testing.expect(!disabled.isStarted());
}
