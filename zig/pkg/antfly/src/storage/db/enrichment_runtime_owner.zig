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
const enrichment_runtime_mod = @import("enrichment/enrichment_runtime.zig");
const Runtime = enrichment_runtime_mod.EnrichmentRuntime;
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const background = @import("../background_runtime.zig");

pub fn deinitConfig(alloc: Allocator, cfg: *enrichment_runtime_mod.Config) void {
    if (cfg.dense_embedder) |dense_embedder| {
        dense_embedder.deinit(alloc);
        cfg.dense_embedder = null;
    }
    if (cfg.sparse_embedder) |sparse_embedder| {
        sparse_embedder.deinit(alloc);
        cfg.sparse_embedder = null;
    }
    if (cfg.asset_producer) |producer| {
        producer.deinit(alloc);
        cfg.asset_producer = null;
    }
    if (cfg.chunk_provider) |*provider| {
        provider.deinit();
        cfg.chunk_provider = null;
    }
}

/// Owns runtime/context allocation and provider adoption as one bundle. The
/// resident DB supplies durable recovery operations and borrowed execution
/// capabilities; this owner never imports DB or server coordination.
pub fn Owner(comptime Context: type) type {
    return ManagedOwner(Context, Runtime);
}
fn ManagedOwner(comptime Context: type, comptime RuntimeType: type) type {
    return struct {
        const Self = @This();
        bundle: Bundle = .{},
        replacement_mutex: std.atomic.Mutex = .unlocked,
        pub const Bundle = struct {
            append_ctx: ?*Context = null,
            runtime: ?*RuntimeType = null,
            pub const Construction = struct {
                backend_runtime: *background.BackendRuntime,
                neighbor_acquire: @TypeOf(@as(enrichment_runtime_mod.NeighborContextGraphSource, undefined).acquire_fn),
                write: enrichment_runtime_mod.GeneratedRecordWriter,
                failure: enrichment_runtime_mod.FailureRecorder,
                failure_pending: enrichment_runtime_mod.FailurePendingCheck,
                failure_range_pending: enrichment_runtime_mod.FailureRangePendingCheck,
                failure_lock: @TypeOf(@as(enrichment_runtime_mod.FailurePendingFence, undefined).lock_fn),
                failure_unlock: @TypeOf(@as(enrichment_runtime_mod.FailurePendingFence, undefined).unlock_fn),
                notify_ctx: *anyopaque,
                notify: enrichment_runtime_mod.NotifyFn,
                commit: @TypeOf(@as(enrichment_runtime_mod.ArtifactUnitTurnCommit, undefined).commit),
            };
            pub fn create(alloc: Allocator, source: *enrichment_runtime_mod.Config, config: enrichment_runtime_mod.Config, context: Context, ops: Construction) !Bundle {
                const ctx = try alloc.create(Context);
                errdefer alloc.destroy(ctx);
                ctx.* = context;
                var cfg = config;
                cfg.neighbor_context_graph_source = .{ .ptr = ctx, .acquire_fn = ops.neighbor_acquire };
                const runtime = try alloc.create(RuntimeType);
                errdefer alloc.destroy(runtime);
                runtime.* = try RuntimeType.init(alloc, ctx.store, ctx.change_journal, ctx.replay_source, ctx.index_manager, ctx.apply_mutex, ctx, ops.write, ctx, ops.failure, ops.failure_pending, ops.failure_range_pending, .{ .ptr = ctx, .lock_fn = ops.failure_lock, .unlock_fn = ops.failure_unlock }, ops.notify_ctx, ops.notify, ops.backend_runtime, cfg);
                // Successful init adopts providers. Clear the source before
                // any subsequent fallible operation; Bundle is now sole owner.
                source.dense_embedder = null;
                source.sparse_embedder = null;
                source.asset_producer = null;
                source.chunk_provider = null;
                runtime.artifact_publication_dispatcher = ctx.artifact_publication_dispatcher;
                runtime.artifact_unit_turn_commit = .{ .ptr = ctx, .commit = ops.commit };
                return .{ .append_ctx = ctx, .runtime = runtime };
            }
            pub fn deinit(self: *Bundle, alloc: Allocator) void {
                if (self.runtime) |runtime| {
                    runtime.deinit();
                    alloc.destroy(runtime);
                }
                if (self.append_ctx) |ctx| alloc.destroy(ctx);
                self.* = .{};
            }
            pub fn take(self: *Bundle) Bundle {
                const owned = self.*;
                self.* = .{};
                return owned;
            }
        };
        pub const Port = struct {
            ptr: *anyopaque,
            alloc: Allocator,
            mutex: *std.atomic.Mutex,
            desired_running: *std.atomic.Value(bool),
            allows_running: bool,
            status_hook: ?enrichment_runtime_mod.StatusHook,
            create: *const fn (*anyopaque, *enrichment_runtime_mod.Config) anyerror!?Bundle,
            after_create: *const fn (*anyopaque) anyerror!void,
            prepare: *const fn (*anyopaque, *RuntimeType, ?types.EnrichmentStats) anyerror!void,
            restore: *const fn (*anyopaque) anyerror!void,
            publish: *const fn (*anyopaque) void,
        };
        fn lock(mutex: *std.atomic.Mutex) void {
            while (!mutex.tryLock()) @import("antfly_platform").time.yieldNow();
        }
        pub fn deinit(self: *Self, alloc: Allocator) void {
            self.bundle.deinit(alloc);
        }
        /// A private fence serializes replacement even without transaction
        /// recovery. The caller additionally fences recovery provider borrows.
        /// A replacement
        /// is fully constructed before the previous worker is stopped; failure
        /// retains the previous bundle and restores its desired running state.
        pub fn replace(self: *Self, port: Port, cfg: enrichment_runtime_mod.Config, start_replacement: bool) !void {
            lock(&self.replacement_mutex);
            defer self.replacement_mutex.unlock();
            var owned_cfg = cfg;
            defer deinitConfig(port.alloc, &owned_cfg);
            var detached = try port.create(port.ptr, &owned_cfg);
            errdefer if (detached) |*bundle| bundle.deinit(port.alloc);
            try port.after_create(port.ptr);
            if (detached) |*bundle| if (port.status_hook) |hook| bundle.runtime.?.setStatusHook(hook);
            const can_run = detached != null and port.allows_running;
            const should_start = can_run and start_replacement;
            const previous_desired = port.desired_running.swap(false, .acq_rel);
            var stopped_existing = false;
            var telemetry: ?types.EnrichmentStats = null;
            lock(port.mutex);
            if (self.bundle.runtime) |runtime| {
                stopped_existing = runtime.isStarted();
                runtime.stop();
                telemetry = runtime.stats();
            }
            port.mutex.unlock();
            errdefer {
                const wanted = previous_desired or stopped_existing;
                port.desired_running.store(wanted, .release);
                if (wanted) port.restore(port.ptr) catch |err| std.log.err("failed to restart previous enrichment runtime after reconfigure failure: {}", .{err});
            }
            if (can_run) {
                try port.prepare(port.ptr, detached.?.runtime.?, telemetry);
                if (should_start) try detached.?.runtime.?.start();
            }
            lock(port.mutex);
            defer port.mutex.unlock();
            self.bundle.deinit(port.alloc);
            if (detached) |*bundle| self.bundle = bundle.take();
            port.publish(port.ptr);
            port.desired_running.store(should_start, .release);
        }
    };
}

test "uninstalled enrichment config releases owned chunk provider routing" {
    var cfg = enrichment_runtime_mod.Config{
        .chunk_provider = try (enrichment_runtime_mod.ChunkProvider{
            .execution = .{ .routing = .{ .source_table = "docs" } },
        }).ownExecutionStrings(std.testing.allocator),
    };
    deinitConfig(std.testing.allocator, &cfg);
    try std.testing.expect(cfg.chunk_provider == null);
}

test "enrichment owner restores running and pending demand and transfers paused replacement once" {
    const F = struct {
        const Self = @This();
        const Fake = struct {
            fixture: *Self,
            started: bool = false,
            fn deinit(self: *@This()) void {
                self.fixture.destroyed += 1;
            }
            fn isStarted(self: *@This()) bool {
                return self.started;
            }
            fn stop(self: *@This()) void {
                self.started = false;
            }
            fn start(self: *@This()) !void {
                self.started = true;
            }
            fn stats(_: *@This()) types.EnrichmentStats {
                return .{};
            }
            fn setStatusHook(_: *@This(), _: enrichment_runtime_mod.StatusHook) void {}
        };
        const Managed = ManagedOwner(u8, Fake);
        owner: Managed = .{},
        mutex: std.atomic.Mutex = .unlocked,
        desired: std.atomic.Value(bool) = .init(true),
        fail_prepare: bool = true,
        destroyed: usize = 0,
        restored: usize = 0,
        published: usize = 0,
        fn f(ptr: *anyopaque) *Self {
            return @ptrCast(@alignCast(ptr));
        }
        fn create(ptr: *anyopaque, _: *enrichment_runtime_mod.Config) !?Managed.Bundle {
            const runtime = try std.testing.allocator.create(Fake);
            runtime.* = .{ .fixture = f(ptr) };
            return .{ .runtime = runtime };
        }
        fn afterCreate(ptr: *anyopaque) !void {
            // Construction is inside the replacement fence.
            try std.testing.expect(!f(ptr).owner.replacement_mutex.tryLock());
        }
        fn prepare(ptr: *anyopaque, _: *Fake, _: ?types.EnrichmentStats) !void {
            if (f(ptr).fail_prepare) return error.PreparationFailed;
        }
        fn restore(ptr: *anyopaque) !void {
            const self = f(ptr);
            try std.testing.expect(self.desired.load(.acquire));
            self.restored += 1;
            self.owner.bundle.runtime.?.started = true;
        }
        fn publish(ptr: *anyopaque) void {
            f(ptr).published += 1;
        }
        fn port(self: *Self) Managed.Port {
            return .{ .ptr = self, .alloc = std.testing.allocator, .mutex = &self.mutex, .desired_running = &self.desired, .allows_running = true, .status_hook = null, .create = create, .after_create = afterCreate, .prepare = prepare, .restore = restore, .publish = publish };
        }
    };
    var f: F = .{};
    defer f.owner.deinit(std.testing.allocator);
    f.owner.bundle = (try F.create(&f, undefined)).?;
    const original = f.owner.bundle.runtime.?;
    original.started = true;
    try std.testing.expectError(error.PreparationFailed, f.owner.replace(f.port(), .{}, true));
    try std.testing.expectEqual(original, f.owner.bundle.runtime.?);
    try std.testing.expect(original.started);
    try std.testing.expectEqual(@as(usize, 1), f.destroyed);
    try std.testing.expectEqual(@as(usize, 1), f.restored);
    // A queued restart is demand even when the runtime is not started yet.
    original.started = false;
    try std.testing.expectError(error.PreparationFailed, f.owner.replace(f.port(), .{}, true));
    try std.testing.expect(f.desired.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), f.restored);
    f.fail_prepare = false;
    try f.owner.replace(f.port(), .{}, false);
    try std.testing.expect(f.owner.bundle.runtime.? != original);
    try std.testing.expect(!f.owner.bundle.runtime.?.started);
    try std.testing.expect(!f.desired.load(.acquire));
    try std.testing.expectEqual(@as(usize, 3), f.destroyed);
    try std.testing.expectEqual(@as(usize, 1), f.published);
}

test "enrichment runtime construction unwinds allocation and durable state failures" {
    const mem_backend = @import("../mem_backend.zig");
    const docstore_mod = @import("../docstore.zig");
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    const store_runtime = try backend.runtimeStore(alloc, .{ .name = "enrichment-construction" });
    var store = try docstore_mod.DocStore.openRuntime(alloc, store_runtime);
    defer store.close();
    var host = try background.BackendRuntimeHandle.init(alloc, .{ .backend = .manual });
    defer host.deinit();
    const Check = struct {
        fn run(failing: Allocator, target: *docstore_mod.DocStore, execution: *background.BackendRuntime, corrupt: bool) !void {
            // Initialization only borrows these execution capabilities. With no
            // producers or started worker it never calls them, including on close.
            var runtime = Runtime.init(failing, target, undefined, undefined, undefined, undefined, undefined, undefined, null, null, null, null, null, undefined, undefined, execution, .{}) catch |err| {
                if (err == error.OutOfMemory) return err;
                if (corrupt) {
                    try std.testing.expectEqual(error.InvalidEnrichmentState, err);
                    return;
                }
                return err;
            };
            defer runtime.deinit();
            try std.testing.expect(!corrupt);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{ &store, host.ptr(), false });
    try store.put("\x00\x00__metadata__:enrichment_status:generated", "corrupt");
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{ &store, host.ptr(), true });
}
