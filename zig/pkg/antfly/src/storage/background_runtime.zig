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

const builtin = @import("builtin");
const std = @import("std");
const platform = @import("antfly_platform");
const runtime_backend = @import("runtime_backend.zig");
const storage_io = @import("lsm_backend/storage_io.zig");
const threaded_io_limits = @import("../common/threaded_io_limits.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const AtomicU64 = platform.atomic.Value(u64);

pub const Backend = runtime_backend.Backend;
pub const IoImpl = if (builtin.os.tag == .freestanding) void else Io.Threaded;
pub const default_io_concurrent_limit: u32 = threaded_io_limits.service;

pub const Config = struct {
    backend: Backend = runtime_backend.defaultExecutorBackend(),
};

/// Atomic admission gate for a lane whose backing executor is destroyed only
/// after every committed borrower has released it. The high bit permanently
/// closes admission; the remaining bits are the active lease count. Keeping
/// both in one word eliminates the check/increment teardown race.
const LaneLeaseGate = struct {
    const closed_bit: usize = @as(usize, 1) << (@bitSizeOf(usize) - 1);
    const count_mask: usize = closed_bit - 1;

    state: std.atomic.Value(usize) = .init(0),
    drain_mutex: Io.Mutex = .init,
    drained: Io.Condition = .init,

    fn tryAcquire(self: *LaneLeaseGate) ?usize {
        var observed = self.state.load(.acquire);
        while (true) {
            if (observed & closed_bit != 0) return null;
            const count = observed & count_mask;
            std.debug.assert(count < count_mask);
            if (self.state.cmpxchgWeak(observed, observed + 1, .acq_rel, .acquire)) |actual| {
                observed = actual;
                continue;
            }
            return count + 1;
        }
    }

    fn release(self: *LaneLeaseGate, coordinator_io: ?Io) void {
        const previous = self.state.fetchSub(1, .acq_rel);
        std.debug.assert(previous & count_mask > 0);
        if (previous & closed_bit != 0 and previous & count_mask == 1) {
            if (coordinator_io) |io| {
                // Synchronize with waitDrained's final state check so a last
                // release cannot race between that check and parking.
                self.drain_mutex.lockUncancelable(io);
                self.drained.broadcast(io);
                self.drain_mutex.unlock(io);
            }
        }
    }

    fn close(self: *LaneLeaseGate) void {
        _ = self.state.fetchOr(closed_bit, .acq_rel);
    }

    fn active(self: *const LaneLeaseGate) usize {
        return self.state.load(.acquire) & count_mask;
    }

    fn isClosed(self: *const LaneLeaseGate) bool {
        return self.state.load(.acquire) & closed_bit != 0;
    }

    fn waitDrained(self: *LaneLeaseGate, coordinator_io: ?Io) void {
        if (coordinator_io) |io| {
            self.drain_mutex.lockUncancelable(io);
            defer self.drain_mutex.unlock(io);
            while (self.active() != 0) self.drained.waitUncancelable(io, &self.drain_mutex);
            return;
        }

        if (comptime builtin.os.tag == .freestanding or builtin.single_threaded) {
            if (self.active() != 0) @panic("cannot drain a lane lease without an I/O coordinator");
            return;
        }
        // Manual runtimes have no executor to park on. They ordinarily have
        // no successful lane leases; retain an executor-independent fallback
        // for a close racing an unavailable acquisition.
        while (self.active() != 0) std.Thread.yield() catch {};
    }
};

/// Process-local hook used by composed runtimes to replace a filesystem DB
/// open with another storage implementation. The options pointer is opaque here
/// to keep the executor layer independent of the DB module; DB.open is the sole
/// caller and passes a `*db.OpenOptions`.
pub const DbOpenConfigurator = struct {
    ptr: *anyopaque,
    configure_fn: *const fn (ptr: *anyopaque, path: []const u8, options: *anyopaque) anyerror!void,

    pub fn configure(self: @This(), path: []const u8, options: anytype) !void {
        try self.configure_fn(self.ptr, path, @ptrCast(options));
    }
};

pub const DurableJobLane = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        submit: *const fn (ptr: *anyopaque, job: Job) anyerror!void,
        drain_owner: *const fn (ptr: *anyopaque, owner_id: u64) void,
        close_owner: *const fn (ptr: *anyopaque, owner_id: u64) void,
        poll: *const fn (ptr: *anyopaque, max_jobs: usize) anyerror!usize,
        executes_inline: bool = false,
    };

    /// On success, the lane owns `job` and will call `job.deinit`.
    /// On error, ownership remains with the caller.
    pub fn submit(self: DurableJobLane, job: Job) !void {
        return try self.vtable.submit(self.ptr, job);
    }

    pub fn drainOwner(self: DurableJobLane, owner_id: u64) void {
        self.vtable.drain_owner(self.ptr, owner_id);
    }

    pub fn closeOwner(self: DurableJobLane, owner_id: u64) void {
        self.vtable.close_owner(self.ptr, owner_id);
    }

    pub fn poll(self: DurableJobLane, max_jobs: usize) !usize {
        return try self.vtable.poll(self.ptr, max_jobs);
    }

    /// Manual runtimes execute submissions on the caller's stack. Workers
    /// that page durable work must leave the marker pending for a later
    /// explicit poll/reopen instead of recursively submitting their successor.
    pub fn executesInline(self: DurableJobLane) bool {
        return self.vtable.executes_inline;
    }
};

pub const Job = struct {
    owner_id: u64,
    class: Class,
    ptr: *anyopaque,
    run: *const fn (ptr: *anyopaque) anyerror!void,
    deinit: *const fn (ptr: *anyopaque) void,

    pub const Class = enum {
        commit_durable,
        maintenance,
        cleanup,
    };
};

fn initIoLane(alloc: Allocator, concurrent_limit: u32) !*IoImpl {
    if (comptime builtin.os.tag == .freestanding) {
        return error.UnsupportedPlatform;
    } else {
        const io_impl = try alloc.create(IoImpl);
        errdefer alloc.destroy(io_impl);
        // Backend runtimes are process-long and own several independent I/O
        // lanes. Threaded retains concurrent workers until deinit, so a finite
        // ceiling prevents any lane from converting a transient fan-out spike
        // into an unbounded kernel-thread/stack reservation ratchet.
        io_impl.* = Io.Threaded.init(alloc, .{
            .concurrent_limit = .limited(concurrent_limit),
        });
        return io_impl;
    }
}

fn deinitIoLane(alloc: Allocator, io_impl: *IoImpl) void {
    if (comptime builtin.os.tag != .freestanding) {
        io_impl.deinit();
    }
    alloc.destroy(io_impl);
}

const OwnerRegistry = struct {
    const State = struct {
        closing: bool = false,
        in_flight: usize = 0,
    };

    alloc: Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    states: std.AutoHashMapUnmanaged(u64, State) = .empty,

    fn init(alloc: Allocator) OwnerRegistry {
        return .{ .alloc = alloc };
    }

    fn deinit(self: *OwnerRegistry) void {
        var iterator = self.states.valueIterator();
        while (iterator.next()) |state| std.debug.assert(state.in_flight == 0);
        self.states.deinit(self.alloc);
        self.* = undefined;
    }

    fn register(self: *OwnerRegistry, owner_id: u64) !void {
        if (owner_id == 0) return error.InvalidBackgroundOwner;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.states.contains(owner_id)) return error.BackgroundOwnerIdExhausted;
        try self.states.putNoClobber(self.alloc, owner_id, .{});
    }

    fn beginJob(self: *OwnerRegistry, owner_id: u64) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.states.getPtr(owner_id) orelse return error.BackgroundOwnerClosed;
        if (state.closing) return error.BackgroundOwnerClosing;
        if (state.in_flight == std.math.maxInt(usize)) return error.BackgroundOwnerCapacityExceeded;
        state.in_flight += 1;
    }

    fn finishJob(self: *OwnerRegistry, owner_id: u64) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.states.getPtr(owner_id) orelse {
            std.debug.panic("background owner {} retired with a job in flight", .{owner_id});
        };
        std.debug.assert(state.in_flight > 0);
        state.in_flight -= 1;
    }

    fn beginClose(self: *OwnerRegistry, owner_id: u64) bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.states.getPtr(owner_id) orelse return false;
        state.closing = true;
        return true;
    }

    fn waitIdle(self: *OwnerRegistry, owner_id: u64) void {
        while (true) {
            lockAtomic(&self.mutex);
            const idle = if (self.states.getPtr(owner_id)) |state|
                state.in_flight == 0
            else
                true;
            self.mutex.unlock();
            if (idle) return;
            if (builtin.os.tag == .freestanding or builtin.single_threaded) {
                std.atomic.spinLoopHint();
            } else {
                std.Thread.yield() catch {};
            }
        }
    }

    fn retireClosed(self: *OwnerRegistry, owner_id: u64) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.states.getPtr(owner_id) orelse return;
        std.debug.assert(state.closing);
        std.debug.assert(state.in_flight == 0);
        _ = self.states.remove(owner_id);
    }

    fn close(self: *OwnerRegistry, owner_id: u64) void {
        if (!self.beginClose(owner_id)) return;
        self.waitIdle(owner_id);
        self.retireClosed(owner_id);
    }
};

pub const BackendRuntime = struct {
    alloc: Allocator,
    backend: Backend,
    next_owner_id: AtomicU64,
    retired_generation_cleanup_owner_id: u64,
    owner_registry: *OwnerRegistry,
    native_storage_pool: *storage_io.NativeStoragePool,
    io_impl: ?*IoImpl = null,
    raft_inbound_io_impl: ?*IoImpl = null,
    raft_outbound_io_impl: ?*IoImpl = null,
    api_io_impl: ?*IoImpl = null,
    inference_io_impl: ?*IoImpl = null,
    control_io_impl: ?*IoImpl = null,
    api_lane_gate: LaneLeaseGate = .{},
    api_lane_peak_leases: std.atomic.Value(usize) = .init(0),
    api_lane_acquisitions_total: std.atomic.Value(u64) = .init(0),
    api_lane_rejections_total: std.atomic.Value(u64) = .init(0),
    inference_lane_gate: LaneLeaseGate = .{},
    inference_lane_peak_leases: std.atomic.Value(usize) = .init(0),
    inference_lane_acquisitions_total: std.atomic.Value(u64) = .init(0),
    inference_lane_rejections_total: std.atomic.Value(u64) = .init(0),
    control_lane_gate: LaneLeaseGate = .{},
    control_lane_peak_leases: std.atomic.Value(usize) = .init(0),
    control_lane_acquisitions_total: std.atomic.Value(u64) = .init(0),
    control_lane_rejections_total: std.atomic.Value(u64) = .init(0),
    threaded_jobs: ?*ThreadedDurableJobLane = null,
    durable_jobs: DurableJobLane,
    db_open_configurator: ?DbOpenConfigurator = null,

    pub fn init(alloc: Allocator, config: Config) !BackendRuntime {
        try runtime_backend.ensureExecutorBackendAvailable(config.backend);

        const owner_registry = try alloc.create(OwnerRegistry);
        errdefer alloc.destroy(owner_registry);
        owner_registry.* = OwnerRegistry.init(alloc);
        errdefer owner_registry.deinit();
        const retired_generation_cleanup_owner_id: u64 = 1;
        try owner_registry.register(retired_generation_cleanup_owner_id);

        const native_storage_pool = try alloc.create(storage_io.NativeStoragePool);
        errdefer alloc.destroy(native_storage_pool);
        native_storage_pool.* = storage_io.NativeStoragePool.init(alloc);
        errdefer native_storage_pool.deinit();

        var runtime = BackendRuntime{
            .alloc = alloc,
            .backend = config.backend,
            .next_owner_id = .init(retired_generation_cleanup_owner_id + 1),
            .retired_generation_cleanup_owner_id = retired_generation_cleanup_owner_id,
            .owner_registry = owner_registry,
            .native_storage_pool = native_storage_pool,
            .durable_jobs = undefined,
        };
        runtime.durable_jobs = InlineDurableJobLane.lane(owner_registry);

        if (config.backend != .manual) {
            if (comptime builtin.os.tag == .freestanding) {
                return error.UnsupportedPlatform;
            } else {
                const io_impl = try initIoLane(alloc, threaded_io_limits.service);
                errdefer deinitIoLane(alloc, io_impl);
                const raft_inbound_io_impl = try initIoLane(alloc, threaded_io_limits.service);
                errdefer deinitIoLane(alloc, raft_inbound_io_impl);
                const raft_outbound_io_impl = try initIoLane(alloc, threaded_io_limits.service);
                errdefer deinitIoLane(alloc, raft_outbound_io_impl);
                const api_io_impl = try initIoLane(alloc, threaded_io_limits.service);
                errdefer deinitIoLane(alloc, api_io_impl);
                const inference_io_impl = try initIoLane(alloc, threaded_io_limits.inference);
                errdefer deinitIoLane(alloc, inference_io_impl);
                const control_io_impl = try initIoLane(alloc, threaded_io_limits.service);
                errdefer deinitIoLane(alloc, control_io_impl);

                const threaded_jobs = try alloc.create(ThreadedDurableJobLane);
                errdefer alloc.destroy(threaded_jobs);
                threaded_jobs.* = ThreadedDurableJobLane.init(alloc, io_impl, owner_registry);
                try threaded_jobs.start();
                errdefer threaded_jobs.deinit();

                runtime.io_impl = io_impl;
                runtime.raft_inbound_io_impl = raft_inbound_io_impl;
                runtime.raft_outbound_io_impl = raft_outbound_io_impl;
                runtime.api_io_impl = api_io_impl;
                runtime.inference_io_impl = inference_io_impl;
                runtime.control_io_impl = control_io_impl;
                runtime.threaded_jobs = threaded_jobs;
                runtime.durable_jobs = threaded_jobs.lane();
            }
        }

        return runtime;
    }

    pub fn deinit(self: *BackendRuntime) void {
        // Close every lane before waiting for any one of them. Otherwise a
        // borrower could continue entering a later lane while teardown drains
        // an earlier one. These waits are production lifetime enforcement,
        // not debug-only diagnostics: no executor is destroyed while a lease
        // can still expose its std.Io interface.
        const coordinator_io = self.io();
        self.api_lane_gate.close();
        self.inference_lane_gate.close();
        self.control_lane_gate.close();
        self.api_lane_gate.waitDrained(coordinator_io);
        self.inference_lane_gate.waitDrained(coordinator_io);
        self.control_lane_gate.waitDrained(coordinator_io);
        if (self.threaded_jobs) |jobs| {
            jobs.deinit();
            self.alloc.destroy(jobs);
            self.threaded_jobs = null;
        }
        if (self.api_io_impl) |io_impl| {
            deinitIoLane(self.alloc, io_impl);
            self.api_io_impl = null;
        }
        if (self.inference_io_impl) |io_impl| {
            deinitIoLane(self.alloc, io_impl);
            self.inference_io_impl = null;
        }
        if (self.control_io_impl) |io_impl| {
            deinitIoLane(self.alloc, io_impl);
            self.control_io_impl = null;
        }
        if (self.raft_outbound_io_impl) |io_impl| {
            deinitIoLane(self.alloc, io_impl);
            self.raft_outbound_io_impl = null;
        }
        if (self.raft_inbound_io_impl) |io_impl| {
            deinitIoLane(self.alloc, io_impl);
            self.raft_inbound_io_impl = null;
        }
        if (self.io_impl) |io_impl| {
            deinitIoLane(self.alloc, io_impl);
            self.io_impl = null;
        }
        self.native_storage_pool.deinit();
        self.alloc.destroy(self.native_storage_pool);
        self.owner_registry.deinit();
        self.alloc.destroy(self.owner_registry);
        self.* = undefined;
    }

    pub fn io(self: *BackendRuntime) ?Io {
        if (comptime builtin.os.tag == .freestanding) return null;
        return if (self.io_impl) |io_impl| io_impl.io() else null;
    }

    pub fn nativeStoragePool(self: *BackendRuntime) *storage_io.NativeStoragePool {
        return self.native_storage_pool;
    }

    pub fn snapshotNativeStorageStats(self: *const BackendRuntime) storage_io.NativeStorageStats {
        return self.native_storage_pool.snapshotStats();
    }

    /// Installs a process-local DB-open policy for composed simulations and
    /// embedded runtimes. The configurator is borrowed and must outlive this
    /// runtime and every DB opened through it.
    pub fn setDbOpenConfigurator(self: *BackendRuntime, configurator: ?DbOpenConfigurator) void {
        self.db_open_configurator = configurator;
    }

    pub fn hasDbOpenConfigurator(self: *const BackendRuntime) bool {
        return self.db_open_configurator != null;
    }

    pub fn raftInboundIo(self: *BackendRuntime) ?Io {
        if (comptime builtin.os.tag == .freestanding) return null;
        return if (self.raft_inbound_io_impl) |io_impl| io_impl.io() else self.io();
    }

    pub fn raftInboundIoImpl(self: *BackendRuntime) ?*IoImpl {
        if (comptime builtin.os.tag == .freestanding) return null;
        return self.raft_inbound_io_impl orelse self.io_impl;
    }

    pub fn raftOutboundIo(self: *BackendRuntime) ?Io {
        if (comptime builtin.os.tag == .freestanding) return null;
        return if (self.raft_outbound_io_impl) |io_impl| io_impl.io() else self.io();
    }

    pub fn raftOutboundIoImpl(self: *BackendRuntime) ?*IoImpl {
        if (comptime builtin.os.tag == .freestanding) return null;
        return self.raft_outbound_io_impl orelse self.io_impl;
    }

    pub fn apiIoImpl(self: *BackendRuntime) ?*IoImpl {
        if (comptime builtin.os.tag == .freestanding) return null;
        return self.api_io_impl orelse self.io_impl;
    }

    /// Returns the API executor interface without exposing its implementation.
    /// Components own and await the tasks they submit; BackendRuntime only owns
    /// the executor lane and must outlive every borrower.
    pub fn apiIo(self: *BackendRuntime) ?Io {
        const io_impl = self.apiIoImpl() orelse return null;
        return io_impl.io();
    }

    pub const ApiLaneLease = struct {
        runtime: *BackendRuntime,
        borrowed_io: Io,
        concurrent_capacity: u32,
        released: bool = false,

        pub fn io(self: *const ApiLaneLease) Io {
            std.debug.assert(!self.released);
            return self.borrowed_io;
        }

        pub fn concurrentCapacity(self: *const ApiLaneLease) u32 {
            std.debug.assert(!self.released);
            return self.concurrent_capacity;
        }

        pub fn release(self: *ApiLaneLease) void {
            if (self.released) return;
            self.released = true;
            self.runtime.api_lane_gate.release(self.runtime.io());
        }
    };

    /// Acquires an explicit lifetime lease for the API executor lane. The
    /// caller must stop and await every submitted task before releasing it.
    pub fn acquireApiLane(self: *BackendRuntime) !ApiLaneLease {
        const leases = self.api_lane_gate.tryAcquire() orelse {
            _ = self.api_lane_rejections_total.fetchAdd(1, .monotonic);
            return error.BackendRuntimeShuttingDown;
        };
        errdefer self.api_lane_gate.release(self.io());
        const borrowed_io = self.apiIo() orelse return error.BackendRuntimeUnavailable;
        updateAtomicMax(&self.api_lane_peak_leases, leases);
        _ = self.api_lane_acquisitions_total.fetchAdd(1, .monotonic);
        return .{
            .runtime = self,
            .borrowed_io = borrowed_io,
            .concurrent_capacity = threaded_io_limits.service,
        };
    }

    pub fn outstandingApiLeases(self: *const BackendRuntime) usize {
        return self.api_lane_gate.active();
    }

    /// Executor isolated for inference graph I/O, model loading, and nested
    /// fan-out. A lifetime lease is required because the linked inference
    /// archive retains a copy of the interface until its node is destroyed.
    pub fn inferenceIo(self: *BackendRuntime) ?Io {
        if (comptime builtin.os.tag == .freestanding) return null;
        const io_impl = self.inference_io_impl orelse self.io_impl orelse return null;
        return io_impl.io();
    }

    pub const InferenceLaneLease = struct {
        runtime: *BackendRuntime,
        borrowed_io: Io,
        released: bool = false,

        pub fn io(self: *const InferenceLaneLease) Io {
            std.debug.assert(!self.released);
            return self.borrowed_io;
        }

        pub fn release(self: *InferenceLaneLease) void {
            if (self.released) return;
            self.released = true;
            self.runtime.inference_lane_gate.release(self.runtime.io());
        }
    };

    pub fn acquireInferenceLane(self: *BackendRuntime) !InferenceLaneLease {
        const leases = self.inference_lane_gate.tryAcquire() orelse {
            _ = self.inference_lane_rejections_total.fetchAdd(1, .monotonic);
            return error.BackendRuntimeShuttingDown;
        };
        errdefer self.inference_lane_gate.release(self.io());
        const borrowed_io = self.inferenceIo() orelse return error.BackendRuntimeUnavailable;
        updateAtomicMax(&self.inference_lane_peak_leases, leases);
        _ = self.inference_lane_acquisitions_total.fetchAdd(1, .monotonic);
        return .{ .runtime = self, .borrowed_io = borrowed_io };
    }

    pub fn outstandingInferenceLeases(self: *const BackendRuntime) usize {
        return self.inference_lane_gate.active();
    }

    /// Reserved control-plane executor for health, metrics, and shutdown
    /// coordination. It is intentionally isolated from public API work so
    /// overload cannot consume the runtime's last observable control path.
    pub fn controlIo(self: *BackendRuntime) ?Io {
        if (comptime builtin.os.tag == .freestanding) return null;
        const io_impl = self.control_io_impl orelse self.io_impl orelse return null;
        return io_impl.io();
    }

    pub const ControlLaneLease = struct {
        runtime: *BackendRuntime,
        borrowed_io: Io,
        released: bool = false,

        pub fn io(self: *const ControlLaneLease) Io {
            std.debug.assert(!self.released);
            return self.borrowed_io;
        }

        pub fn release(self: *ControlLaneLease) void {
            if (self.released) return;
            self.released = true;
            self.runtime.control_lane_gate.release(self.runtime.io());
        }
    };

    pub fn acquireControlLane(self: *BackendRuntime) !ControlLaneLease {
        const leases = self.control_lane_gate.tryAcquire() orelse {
            _ = self.control_lane_rejections_total.fetchAdd(1, .monotonic);
            return error.BackendRuntimeShuttingDown;
        };
        errdefer self.control_lane_gate.release(self.io());
        const borrowed_io = self.controlIo() orelse return error.BackendRuntimeUnavailable;
        updateAtomicMax(&self.control_lane_peak_leases, leases);
        _ = self.control_lane_acquisitions_total.fetchAdd(1, .monotonic);
        return .{ .runtime = self, .borrowed_io = borrowed_io };
    }

    pub fn outstandingControlLeases(self: *const BackendRuntime) usize {
        return self.control_lane_gate.active();
    }

    pub const LaneStats = struct {
        api_active_leases: usize,
        api_peak_leases: usize,
        api_acquisitions_total: u64,
        api_rejections_total: u64,
        inference_active_leases: usize,
        inference_peak_leases: usize,
        inference_acquisitions_total: u64,
        inference_rejections_total: u64,
        control_active_leases: usize,
        control_peak_leases: usize,
        control_acquisitions_total: u64,
        control_rejections_total: u64,
    };

    pub fn laneStats(self: *const BackendRuntime) LaneStats {
        return .{
            .api_active_leases = self.api_lane_gate.active(),
            .api_peak_leases = self.api_lane_peak_leases.load(.acquire),
            .api_acquisitions_total = self.api_lane_acquisitions_total.load(.acquire),
            .api_rejections_total = self.api_lane_rejections_total.load(.acquire),
            .inference_active_leases = self.inference_lane_gate.active(),
            .inference_peak_leases = self.inference_lane_peak_leases.load(.acquire),
            .inference_acquisitions_total = self.inference_lane_acquisitions_total.load(.acquire),
            .inference_rejections_total = self.inference_lane_rejections_total.load(.acquire),
            .control_active_leases = self.control_lane_gate.active(),
            .control_peak_leases = self.control_lane_peak_leases.load(.acquire),
            .control_acquisitions_total = self.control_lane_acquisitions_total.load(.acquire),
            .control_rejections_total = self.control_lane_rejections_total.load(.acquire),
        };
    }

    fn updateAtomicMax(counter: *std.atomic.Value(usize), value: usize) void {
        var observed = counter.load(.acquire);
        while (observed < value) {
            if (counter.cmpxchgWeak(observed, value, .acq_rel, .acquire) == null) return;
            observed = counter.load(.acquire);
        }
    }

    pub fn allocOwnerId(self: *BackendRuntime) !u64 {
        while (true) {
            const owner_id = self.next_owner_id.fetchAdd(1, .monotonic);
            if (owner_id == 0) continue;
            try self.owner_registry.register(owner_id);
            return owner_id;
        }
    }
};

pub const BackendRuntimeHandle = struct {
    alloc: Allocator,
    runtime: *BackendRuntime,

    pub fn init(alloc: Allocator, config: Config) !BackendRuntimeHandle {
        const runtime = try alloc.create(BackendRuntime);
        errdefer alloc.destroy(runtime);
        runtime.* = try BackendRuntime.init(alloc, config);
        return .{
            .alloc = alloc,
            .runtime = runtime,
        };
    }

    pub fn deinit(self: *BackendRuntimeHandle) void {
        self.runtime.deinit();
        self.alloc.destroy(self.runtime);
        self.* = undefined;
    }

    pub fn ptr(self: *BackendRuntimeHandle) *BackendRuntime {
        return self.runtime;
    }
};

const InlineDurableJobLane = struct {
    fn lane(owners: *OwnerRegistry) DurableJobLane {
        return .{
            .ptr = owners,
            .vtable = &inline_vtable,
        };
    }

    fn submit(ptr: *anyopaque, job: Job) !void {
        const owners: *OwnerRegistry = @ptrCast(@alignCast(ptr));
        try owners.beginJob(job.owner_id);
        defer owners.finishJob(job.owner_id);
        try job.run(job.ptr);
        job.deinit(job.ptr);
    }

    fn drainOwner(ptr: *anyopaque, owner_id: u64) void {
        const owners: *OwnerRegistry = @ptrCast(@alignCast(ptr));
        owners.waitIdle(owner_id);
    }

    fn closeOwner(ptr: *anyopaque, owner_id: u64) void {
        const owners: *OwnerRegistry = @ptrCast(@alignCast(ptr));
        owners.close(owner_id);
    }

    fn poll(_: *anyopaque, _: usize) !usize {
        return 0;
    }
};

const inline_vtable = DurableJobLane.VTable{
    .submit = InlineDurableJobLane.submit,
    .drain_owner = InlineDurableJobLane.drainOwner,
    .close_owner = InlineDurableJobLane.closeOwner,
    .poll = InlineDurableJobLane.poll,
    .executes_inline = true,
};

const ThreadedDurableJobLane = if (builtin.os.tag == .freestanding) struct {
    fn init(_: Allocator, _: *IoImpl, _: *OwnerRegistry) ThreadedDurableJobLane {
        return .{};
    }

    fn start(_: *ThreadedDurableJobLane) !void {}

    fn lane(self: *ThreadedDurableJobLane) DurableJobLane {
        return .{
            .ptr = self,
            .vtable = &threaded_vtable,
        };
    }

    fn deinit(_: *ThreadedDurableJobLane) void {}

    fn submit(_: *anyopaque, _: Job) !void {
        return error.UnsupportedPlatform;
    }

    fn drainOwner(_: *anyopaque, _: u64) void {}

    fn closeOwner(_: *anyopaque, _: u64) void {}

    fn poll(_: *anyopaque, _: usize) !usize {
        return 0;
    }
} else struct {
    const Entry = struct {
        lane: *ThreadedDurableJobLane,
        job: Job,
        future: Io.Future(void),
        completed: std.atomic.Value(bool) = .init(false),
        job_deinited: std.atomic.Value(bool) = .init(false),

        fn deinitJobOnce(self: *Entry) void {
            if (self.job_deinited.swap(true, .acq_rel)) return;
            self.job.deinit(self.job.ptr);
        }
    };

    const reap_batch_limit: usize = 4096;
    const idle_reap_interval_ms: u64 = 10;

    alloc: Allocator,
    io_impl: *IoImpl,
    owners: *OwnerRegistry,
    mutex: std.atomic.Mutex = .unlocked,
    reap_mutex: std.atomic.Mutex = .unlocked,
    shutdown_reaper: std.atomic.Value(bool) = .init(false),
    completed_count: std.atomic.Value(usize) = .init(0),
    reaper_future: ?Io.Future(void) = null,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,

    fn init(alloc: Allocator, io_impl: *IoImpl, owners: *OwnerRegistry) ThreadedDurableJobLane {
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .owners = owners,
        };
    }

    fn start(self: *ThreadedDurableJobLane) !void {
        self.reaper_future = try self.io_impl.io().concurrent(reaperLoop, .{self});
    }

    fn lane(self: *ThreadedDurableJobLane) DurableJobLane {
        return .{
            .ptr = self,
            .vtable = &threaded_vtable,
        };
    }

    fn deinit(self: *ThreadedDurableJobLane) void {
        self.shutdown_reaper.store(true, .release);
        if (self.reaper_future) |*future| {
            _ = future.await(self.io_impl.io());
            self.reaper_future = null;
        }
        self.drainAll();
        self.entries.deinit(self.alloc);
        self.* = undefined;
    }

    fn submit(ptr: *anyopaque, job: Job) !void {
        const self: *ThreadedDurableJobLane = @ptrCast(@alignCast(ptr));
        try self.owners.beginJob(job.owner_id);
        errdefer self.owners.finishJob(job.owner_id);
        const entry = try self.alloc.create(Entry);
        entry.* = .{
            .lane = self,
            .job = job,
            .future = undefined,
        };
        errdefer self.alloc.destroy(entry);

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        try self.entries.ensureUnusedCapacity(self.alloc, 1);
        entry.future = try self.io_impl.io().concurrent(runEntry, .{entry});
        self.entries.appendAssumeCapacity(entry);
    }

    fn drainOwner(ptr: *anyopaque, owner_id: u64) void {
        const self: *ThreadedDurableJobLane = @ptrCast(@alignCast(ptr));
        self.drainMatching(owner_id);
    }

    fn closeOwner(ptr: *anyopaque, owner_id: u64) void {
        const self: *ThreadedDurableJobLane = @ptrCast(@alignCast(ptr));
        if (!self.owners.beginClose(owner_id)) return;
        self.drainMatching(owner_id);
        self.owners.waitIdle(owner_id);
        self.owners.retireClosed(owner_id);
    }

    fn poll(ptr: *anyopaque, max_jobs: usize) !usize {
        const self: *ThreadedDurableJobLane = @ptrCast(@alignCast(ptr));
        return self.reapCompleted(max_jobs);
    }

    fn runEntry(entry: *Entry) void {
        entry.job.run(entry.job.ptr) catch |err| {
            std.log.warn("background durable job failed owner={} class={s} err={s}", .{
                entry.job.owner_id,
                @tagName(entry.job.class),
                @errorName(err),
            });
        };
        // The payload often owns the transaction state and buffers that make
        // a durable job large. Release it on the worker at the actual lifetime
        // boundary instead of retaining it until the bookkeeping reaper joins
        // the already-completed future. `deinitJobOnce` also makes concurrent
        // owner drains safe.
        entry.deinitJobOnce();
        // Publish the count first. It is only a wake/drain hint; the release
        // store below remains authoritative. Publishing in this order also
        // prevents a reaper from freeing `entry` before this worker's last
        // access to it.
        entry.lane.owners.finishJob(entry.job.owner_id);
        _ = entry.lane.completed_count.fetchAdd(1, .monotonic);
        entry.completed.store(true, .release);
    }

    fn reaperLoop(self: *ThreadedDurableJobLane) void {
        while (!self.shutdown_reaper.load(.acquire)) {
            const reaped = self.reapCompleted(reap_batch_limit);
            // Drain a backlog without an artificial rate cap. At idle, a
            // short sleep avoids scanning the active set continuously.
            if (reaped == reap_batch_limit or self.completed_count.load(.monotonic) > 0) continue;
            self.io_impl.io().sleep(Io.Duration.fromMilliseconds(idle_reap_interval_ms), .awake) catch {};
        }
        while (self.reapCompleted(reap_batch_limit) > 0) {}
    }

    fn drainAll(self: *ThreadedDurableJobLane) void {
        lockAtomic(&self.reap_mutex);
        defer self.reap_mutex.unlock();
        while (true) {
            const entry = self.popAny() orelse return;
            self.awaitAndDestroy(entry);
        }
    }

    fn drainMatching(self: *ThreadedDurableJobLane, owner_id: u64) void {
        lockAtomic(&self.reap_mutex);
        defer self.reap_mutex.unlock();
        while (true) {
            const entry = self.popOwner(owner_id) orelse return;
            self.awaitAndDestroy(entry);
        }
    }

    fn reapCompleted(self: *ThreadedDurableJobLane, max_jobs: usize) usize {
        if (max_jobs == 0 or self.completed_count.load(.monotonic) == 0) return 0;
        lockAtomic(&self.reap_mutex);
        defer self.reap_mutex.unlock();

        // Detach completed entries in one pass. The previous implementation
        // repeatedly called orderedRemove, shifting the entire tail for every
        // completed job. A large ingest could therefore retain millions of
        // finished payloads while spending most of a core in memmove.
        var detached: [reap_batch_limit]*Entry = undefined;
        const target = @min(max_jobs, detached.len);
        var detached_count: usize = 0;
        lockAtomic(&self.mutex);
        var idx: usize = 0;
        while (idx < self.entries.items.len and detached_count < target) {
            const entry = self.entries.items[idx];
            if (!entry.completed.load(.acquire)) {
                idx += 1;
                continue;
            }
            detached[detached_count] = entry;
            detached_count += 1;
            _ = self.entries.swapRemove(idx);
        }
        self.mutex.unlock();

        for (detached[0..detached_count]) |entry| self.awaitAndDestroy(entry);
        return detached_count;
    }

    fn popAny(self: *ThreadedDurableJobLane) ?*Entry {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.entries.items.len == 0) return null;
        return self.entries.swapRemove(0);
    }

    fn popOwner(self: *ThreadedDurableJobLane, owner_id: u64) ?*Entry {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items, 0..) |entry, idx| {
            if (entry.job.owner_id == owner_id) return self.entries.swapRemove(idx);
        }
        return null;
    }

    fn awaitAndDestroy(self: *ThreadedDurableJobLane, entry: *Entry) void {
        _ = entry.future.await(self.io_impl.io());
        if (entry.completed.swap(false, .acq_rel)) {
            _ = self.completed_count.fetchSub(1, .monotonic);
        }
        entry.deinitJobOnce();
        self.alloc.destroy(entry);
    }
};

const threaded_vtable = DurableJobLane.VTable{
    .submit = ThreadedDurableJobLane.submit,
    .drain_owner = ThreadedDurableJobLane.drainOwner,
    .close_owner = ThreadedDurableJobLane.closeOwner,
    .poll = ThreadedDurableJobLane.poll,
};

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) {
        if (builtin.os.tag == .freestanding or builtin.single_threaded) {
            std.atomic.spinLoopHint();
            continue;
        }
        std.Thread.yield() catch {};
    }
}

test "lane lease gate closes admission and drains a committed borrower" {
    if (builtin.os.tag == .freestanding) return;

    var gate = LaneLeaseGate{};
    try std.testing.expectEqual(@as(?usize, 1), gate.tryAcquire());

    var drained = std.atomic.Value(bool).init(false);
    const closer = try std.Thread.spawn(.{}, struct {
        fn run(g: *LaneLeaseGate, done: *std.atomic.Value(bool)) void {
            g.close();
            g.waitDrained(null);
            done.store(true, .release);
        }
    }.run, .{ &gate, &drained });

    while (!gate.isClosed()) std.Thread.yield() catch {};
    try std.testing.expectEqual(@as(?usize, null), gate.tryAcquire());
    try std.testing.expect(!drained.load(.acquire));
    gate.release(null);
    closer.join();
    try std.testing.expect(drained.load(.acquire));
}

test "backend runtime handle owns a stable runtime pointer" {
    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();

    const first = handle.ptr();
    const second = handle.ptr();
    try std.testing.expect(first == second);
    try std.testing.expect(first.io_impl == null);
}

test "backend runtime durable lane runs inline jobs" {
    const Ctx = struct {
        ran: bool = false,
        deinit_called: bool = false,
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.ran = true;
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.deinit_called = true;
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();

    try std.testing.expect(handle.ptr().durable_jobs.executesInline());
    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });

    try std.testing.expect(ctx.ran);
    try std.testing.expect(ctx.deinit_called);
}

test "backend runtime durable lane leaves inline failed jobs owned by caller" {
    const Ctx = struct {
        ran: bool = false,
        deinit_called: bool = false,
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.ran = true;
            return error.ExpectedFailure;
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.deinit_called = true;
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();

    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{};
    try std.testing.expectError(error.ExpectedFailure, handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    }));

    try std.testing.expect(ctx.ran);
    try std.testing.expect(!ctx.deinit_called);
    Fns.deinit(&ctx);
    try std.testing.expect(ctx.deinit_called);
}

test "backend runtime threaded durable lane sees initialized jobs" {
    if (builtin.os.tag == .freestanding) return error.SkipZigTest;

    const Ctx = struct {
        ran: std.atomic.Value(bool) = .init(false),
        deinit_called: std.atomic.Value(bool) = .init(false),
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.ran.store(true, .release);
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.deinit_called.store(true, .release);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    const owner_id = try handle.ptr().allocOwnerId();
    var ctxs: [64]Ctx = [_]Ctx{.{}} ** 64;
    for (&ctxs) |*ctx| {
        try handle.ptr().durable_jobs.submit(.{
            .owner_id = owner_id,
            .class = .commit_durable,
            .ptr = ctx,
            .run = Fns.run,
            .deinit = Fns.deinit,
        });
    }
    handle.ptr().durable_jobs.drainOwner(owner_id);

    for (&ctxs) |*ctx| {
        try std.testing.expect(ctx.ran.load(.acquire));
        try std.testing.expect(ctx.deinit_called.load(.acquire));
    }
}

test "backend runtime allocates stable nonzero owner ids" {
    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();

    const first = try handle.ptr().allocOwnerId();
    const second = try handle.ptr().allocOwnerId();

    try std.testing.expect(first != 0);
    try std.testing.expectEqual(first + 1, second);
}

test "backend runtime API lane leases expose and release the interface" {
    if (builtin.os.tag == .freestanding) return;

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    try std.testing.expectEqual(@as(usize, 0), handle.ptr().outstandingApiLeases());
    var first = try handle.ptr().acquireApiLane();
    var second = try handle.ptr().acquireApiLane();
    try std.testing.expectEqual(@as(usize, 2), handle.ptr().outstandingApiLeases());
    const active_stats = handle.ptr().laneStats();
    try std.testing.expectEqual(@as(usize, 2), active_stats.api_active_leases);
    try std.testing.expectEqual(@as(usize, 2), active_stats.api_peak_leases);
    try std.testing.expectEqual(@as(u64, 2), active_stats.api_acquisitions_total);
    _ = first.io();
    _ = second.io();

    first.release();
    try std.testing.expectEqual(@as(usize, 1), handle.ptr().outstandingApiLeases());
    // Release is idempotent so cleanup paths may call it defensively.
    first.release();
    try std.testing.expectEqual(@as(usize, 1), handle.ptr().outstandingApiLeases());
    second.release();
    try std.testing.expectEqual(@as(usize, 0), handle.ptr().outstandingApiLeases());
}

test "backend runtime deinit closes admission and waits for active lane leases" {
    if (builtin.os.tag == .freestanding) return;

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    const runtime = handle.ptr();
    var lease = try runtime.acquireApiLane();
    var deinitialized = std.atomic.Value(bool).init(false);
    const deinit_thread = try std.Thread.spawn(.{}, struct {
        fn run(h: *BackendRuntimeHandle, done: *std.atomic.Value(bool)) void {
            h.deinit();
            done.store(true, .release);
        }
    }.run, .{ &handle, &deinitialized });

    while (!runtime.api_lane_gate.isClosed()) std.Thread.yield() catch {};
    try std.testing.expectError(error.BackendRuntimeShuttingDown, runtime.acquireApiLane());
    try std.testing.expect(!deinitialized.load(.acquire));
    lease.release();
    deinit_thread.join();
    try std.testing.expect(deinitialized.load(.acquire));
}

test "backend runtime rejects API lane leases after shutdown begins" {
    if (builtin.os.tag == .freestanding) return;

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();
    handle.ptr().api_lane_gate.close();

    try std.testing.expectError(error.BackendRuntimeShuttingDown, handle.ptr().acquireApiLane());
    try std.testing.expectEqual(@as(usize, 0), handle.ptr().outstandingApiLeases());
    try std.testing.expectEqual(@as(u64, 1), handle.ptr().laneStats().api_rejections_total);
}

test "backend runtime control lane leases are isolated from API leases" {
    if (builtin.os.tag == .freestanding) return;

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    var api = try handle.ptr().acquireApiLane();
    defer api.release();
    var control = try handle.ptr().acquireControlLane();
    defer control.release();

    try std.testing.expectEqual(@as(usize, 1), handle.ptr().outstandingApiLeases());
    try std.testing.expectEqual(@as(usize, 1), handle.ptr().outstandingControlLeases());
    const stats = handle.ptr().laneStats();
    try std.testing.expectEqual(@as(usize, 1), stats.control_peak_leases);
    try std.testing.expectEqual(@as(u64, 1), stats.control_acquisitions_total);
    _ = api.io();
    _ = control.io();
    try std.testing.expect(handle.ptr().api_io_impl.? != handle.ptr().control_io_impl.?);
}

test "backend runtime inference lane has an isolated bounded executor" {
    if (builtin.os.tag == .freestanding) return;

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    var inference = try handle.ptr().acquireInferenceLane();
    defer inference.release();
    _ = inference.io();
    try std.testing.expectEqual(@as(usize, 1), handle.ptr().outstandingInferenceLeases());
    const stats = handle.ptr().laneStats();
    try std.testing.expectEqual(@as(usize, 1), stats.inference_peak_leases);
    try std.testing.expectEqual(@as(u64, 1), stats.inference_acquisitions_total);
    try std.testing.expect(handle.ptr().inference_io_impl.? != handle.ptr().api_io_impl.?);
    try std.testing.expectEqual(
        std.Io.Limit.limited(threaded_io_limits.inference),
        handle.ptr().inference_io_impl.?.concurrent_limit,
    );
}

test "backend runtime rejects control lane leases after shutdown begins" {
    if (builtin.os.tag == .freestanding) return;

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();
    handle.ptr().control_lane_gate.close();

    try std.testing.expectError(error.BackendRuntimeShuttingDown, handle.ptr().acquireControlLane());
    try std.testing.expectEqual(@as(usize, 0), handle.ptr().outstandingControlLeases());
    try std.testing.expectEqual(@as(u64, 1), handle.ptr().laneStats().control_rejections_total);
}

test "backend runtime retires closed owner registry state" {
    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();

    const shared_owner_count = handle.ptr().owner_registry.states.count();
    for (0..1024) |_| {
        const owner_id = try handle.ptr().allocOwnerId();
        handle.ptr().durable_jobs.closeOwner(owner_id);
    }
    try std.testing.expectEqual(shared_owner_count, handle.ptr().owner_registry.states.count());
}

test "backend runtime durable lane drains threaded jobs by owner" {
    if (builtin.os.tag == .freestanding) return;

    const Ctx = struct {
        value: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
        deinits: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.value.fetchAdd(1, .monotonic);
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.deinits.fetchAdd(1, .monotonic);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    const first_owner_id = try handle.ptr().allocOwnerId();
    const second_owner_id = try handle.ptr().allocOwnerId();
    var first = Ctx{};
    var second = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = first_owner_id,
        .class = .cleanup,
        .ptr = &first,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = second_owner_id,
        .class = .cleanup,
        .ptr = &second,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });

    handle.ptr().durable_jobs.drainOwner(first_owner_id);
    try std.testing.expectEqual(@as(u32, 1), first.value.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), first.deinits.load(.monotonic));

    handle.ptr().durable_jobs.drainOwner(second_owner_id);
    try std.testing.expectEqual(@as(u32, 1), second.value.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), second.deinits.load(.monotonic));
}

test "backend runtime threaded durable lane rejects jobs after owner close" {
    if (builtin.os.tag == .freestanding) return;

    const Ctx = struct {
        ran: std.atomic.Value(u32) = .init(0),
        deinits: std.atomic.Value(u32) = .init(0),
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.ran.fetchAdd(1, .release);
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.deinits.fetchAdd(1, .release);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });
    handle.ptr().durable_jobs.closeOwner(owner_id);

    try std.testing.expectEqual(@as(u32, 1), ctx.ran.load(.acquire));
    try std.testing.expectEqual(@as(u32, 1), ctx.deinits.load(.acquire));
    try std.testing.expect(!handle.ptr().owner_registry.states.contains(owner_id));
    try std.testing.expectError(error.BackgroundOwnerClosed, handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    }));
    try std.testing.expectEqual(@as(u32, 1), ctx.deinits.load(.acquire));
}

test "backend runtime owner close rejects recursive submit from draining job" {
    if (builtin.os.tag == .freestanding) return;

    const Ctx = struct {
        lane: DurableJobLane,
        owner_id: u64,
        started: std.atomic.Value(bool) = .init(false),
        allow_submit: std.atomic.Value(bool) = .init(false),
        submit_rejected: std.atomic.Value(bool) = .init(false),
        run_count: std.atomic.Value(u32) = .init(0),
        deinits: std.atomic.Value(u32) = .init(0),
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.run_count.fetchAdd(1, .release);
            ctx.started.store(true, .release);
            while (!ctx.allow_submit.load(.acquire)) {
                std.atomic.spinLoopHint();
            }
            ctx.lane.submit(.{
                .owner_id = ctx.owner_id,
                .class = .maintenance,
                .ptr = ctx,
                .run = run,
                .deinit = deinit,
            }) catch |err| switch (err) {
                error.BackgroundOwnerClosing => {
                    ctx.submit_rejected.store(true, .release);
                    return;
                },
                else => return err,
            };
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.deinits.fetchAdd(1, .release);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{ .lane = handle.ptr().durable_jobs, .owner_id = owner_id };
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });
    while (!ctx.started.load(.acquire)) {
        std.atomic.spinLoopHint();
    }
    try std.testing.expect(handle.ptr().owner_registry.beginClose(owner_id));
    ctx.allow_submit.store(true, .release);
    handle.ptr().durable_jobs.drainOwner(owner_id);
    handle.ptr().owner_registry.waitIdle(owner_id);
    handle.ptr().owner_registry.retireClosed(owner_id);

    try std.testing.expect(ctx.submit_rejected.load(.acquire));
    const run_count = ctx.run_count.load(.acquire);
    try std.testing.expect(run_count >= 1);
    try std.testing.expectEqual(run_count, ctx.deinits.load(.acquire));
}

test "backend runtime durable lane deinits threaded job payload after completion" {
    if (builtin.os.tag == .freestanding) return;

    const Ctx = struct {
        ran: std.atomic.Value(u32) = .init(0),
        deinits: std.atomic.Value(u32) = .init(0),
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.ran.fetchAdd(1, .release);
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.deinits.fetchAdd(1, .release);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });

    var attempts: usize = 0;
    while (ctx.deinits.load(.acquire) == 0 and attempts < 200) : (attempts += 1) {
        _ = try handle.ptr().durable_jobs.poll(8);
        if (handle.ptr().io()) |io| io.sleep(Io.Duration.fromMilliseconds(2), .awake) catch {};
    }
    handle.ptr().durable_jobs.drainOwner(owner_id);

    try std.testing.expectEqual(@as(u32, 1), ctx.ran.load(.acquire));
    try std.testing.expectEqual(@as(u32, 1), ctx.deinits.load(.acquire));
}

test "backend runtime threaded worker releases payload before reaper joins" {
    if (builtin.os.tag == .freestanding) return;

    const Ctx = struct {
        ran: std.atomic.Value(bool) = .init(false),
        deinit_called: std.atomic.Value(bool) = .init(false),
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.ran.store(true, .release);
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.deinit_called.store(true, .release);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    // Prevent both the background reaper and explicit poll from joining the
    // future. The worker must still release the owned payload promptly.
    const jobs = handle.ptr().threaded_jobs.?;
    lockAtomic(&jobs.reap_mutex);
    defer jobs.reap_mutex.unlock();

    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .commit_durable,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });

    var attempts: usize = 0;
    while (!ctx.deinit_called.load(.acquire) and attempts < 200) : (attempts += 1) {
        if (handle.ptr().io()) |io| io.sleep(Io.Duration.fromMilliseconds(2), .awake) catch {};
    }
    try std.testing.expect(ctx.ran.load(.acquire));
    try std.testing.expect(ctx.deinit_called.load(.acquire));
}
