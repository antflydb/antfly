// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! One synchronous invocation peak for temporary fixed-geometry device state.
//! The parent charge remains alive through the actual invocation boundary;
//! returning a loan never refunds external quota between decoder layers.
const std = @import("std");
const memory = @import("../runtime/tier/memory.zig");
const admission = @import("../runtime/tier/admission_domain.zig");
const retirement = @import("resource_retirement.zig");
const Cleanup = @import("cleanup_context.zig").Context;
const Epoch = @import("retirement_sink.zig").Epoch;
const allocator = @import("antfly_platform").allocator.processAllocator(@import("antfly_platform").allocator.concurrentFallback());

pub const Layout = struct {
    key_heads: usize,
    value_heads: usize,
    pub fn bytes(self: Layout) !usize {
        if (self.key_heads == 0 or self.value_heads == 0 or self.key_heads > 256 or self.value_heads > 256 or self.value_heads % self.key_heads != 0) return error.InvalidTensorShape;
        const channels = try std.math.mul(usize, try std.math.add(usize, try std.math.mul(usize, self.key_heads, 2), self.value_heads), 128);
        const histories_and_convolution = try std.math.mul(usize, channels, 4 * 4 * 3);
        const recurrent = try std.math.mul(usize, self.value_heads, 128 * 128 * 4);
        const side = try std.math.mul(usize, try std.math.add(usize, try std.math.mul(usize, self.value_heads, 2), 128), 4);
        return std.math.add(usize, try std.math.add(usize, histories_and_convolution, recurrent), side);
    }
};

pub const Budget = struct {
    metadata: std.mem.Allocator,
    layout: Layout,
    expected_compute: usize,
    shell: retirement.PreparedCharge,
    charge: ?*retirement.Charge = null,
    // Borrowed ONLY while the actual synchronous invocation is executing.
    current: ?*const Epoch = null,
    boundary_held: bool = false,
    in_use: bool = false,

    pub fn prepare(layout: Layout, expected_compute: usize) !*Budget {
        return prepareWithAllocator(allocator, layout, expected_compute);
    }
    fn prepareWithAllocator(metadata: std.mem.Allocator, layout: Layout, expected_compute: usize) !*Budget {
        _ = try layout.bytes();
        if (expected_compute == 0) return error.MissingRecurrentStateOwner;
        const self = try metadata.create(Budget);
        errdefer metadata.destroy(self);
        self.* = .{ .metadata = metadata, .layout = layout, .expected_compute = expected_compute, .shell = try retirement.PreparedCharge.prepare(0) };
        return self;
    }
    /// Moves an independently funded exact peak; no reservation or callback.
    pub fn arm(self: *Budget, lease: *?memory.AdmissionLease, pin: *?admission.Pin) !void {
        if (self.charge != null) return error.RecurrentStateBudgetAlreadyArmed;
        const value = lease.* orelse return error.MissingResourceRetirementLease;
        if (!std.meta.eql(value.amounts, memory.AdmissionAmounts{ .backend_scratch_bytes = try self.layout.bytes() })) return error.RecurrentStateBudgetChanged;
        const domain = pin.* orelse return error.MissingRecurrentStateDomain;
        if (domain.controller() != value.controller) return error.ForeignAdmissionDomain;
        self.charge = try self.shell.arm(lease, pin);
    }
    pub fn bind(self: *Budget, epoch: *const Epoch) !void {
        if (self.current != null or self.in_use or self.boundary_held) return error.RecurrentStateBudgetAlreadyBound;
        const charge = self.charge orelse return error.UnpreparedRecurrentStateBudget;
        if (try epoch.isSealed()) return error.RetirementEpochSealed;
        try charge.holdUntilBoundary(try epoch.boundary());
        self.boundary_held = true;
        self.current = epoch;
    }
    pub fn acquire(self: *Budget, compute: usize, layout: Layout) !void {
        if (compute != self.expected_compute or !std.meta.eql(layout, self.layout)) return error.ForeignRecurrentStateBudget;
        const epoch = self.current orelse return error.UnboundRecurrentStateBudget;
        if (try epoch.isSealed()) return error.RetirementEpochSealed;
        if (self.in_use) return error.RecurrentStateLoanStillLive;
        self.in_use = true;
    }
    pub fn validateCleanup(self: *const Budget, current: Cleanup) !void {
        const bound = self.current orelse return error.UnboundRecurrentStateBudget;
        const actual = current.boundary orelse return error.MissingCurrentCleanupEpoch;
        if (current.outside_execution or actual.record != bound.record or actual.nonce != bound.nonce or try actual.isSealed()) return error.ForeignRecurrentStateCleanup;
        if (!self.in_use) return error.RecurrentStateLoanAlreadyReturned;
    }
    /// Called only after every allocating descriptor has checked SDK death or
    /// moved into the existing independently funded retained-buffer cache.
    pub fn returnAfterBackingRetired(self: *Budget, current: Cleanup) !void {
        try self.validateCleanup(current);
        self.in_use = false;
    }
    pub fn cleanup(self: *const Budget) !Cleanup {
        const epoch = self.current orelse return error.UnboundRecurrentStateBudget;
        if (try epoch.isSealed()) return error.RetirementEpochSealed;
        return .{ .boundary = epoch };
    }
    pub fn retireWithCleanup(self: *Budget, current: Cleanup) !void {
        if (current.outside_execution) return error.ForeignRecurrentStateCleanup;
        if (self.in_use) return error.RecurrentStateLoanStillLive;
        if (self.current) |bound| {
            const actual = current.boundary orelse return error.MissingCurrentCleanupEpoch;
            if (actual.record != bound.record or actual.nonce != bound.nonce) return error.ForeignRecurrentStateCleanup;
            if (try actual.isSealed()) return error.RetirementEpochSealed;
        } else if (!self.boundary_held) {
            const epoch = current.boundary orelse return error.MissingCurrentCleanupEpoch;
            try self.bind(epoch);
        }
        try self.unbind();
        try self.destroyBoundRoot();
    }
    fn unbind(self: *Budget) !void {
        if (self.in_use) return error.RecurrentStateLoanStillLive;
        self.current = null;
    }
    /// The actual synchronous producer has ended and returned every loan.
    /// Its prepared boundary ticket prevents supplier refunds while the caller
    /// still holds its Model/lane. No Epoch survives in the late charge.
    fn destroyBoundRoot(self: *Budget) !void {
        if (self.in_use or self.current != null) return error.RecurrentStateLoanStillLive;
        const charge = self.charge;
        if (charge != null and !self.boundary_held) return error.RecurrentStateBudgetNeverBound;
        self.charge = null;
        self.shell.destroyUnused();
        const metadata = self.metadata;
        metadata.destroy(self);
        if (charge) |owner| owner.release();
    }
    /// Cold rollback only, BEFORE dispatch binding and all numerical lanes.
    pub fn destroyUnusedOutsideExecution(self: *Budget) !void {
        if (self.current != null or self.in_use or self.boundary_held) return error.RecurrentStateBudgetAlreadyBound;
        const charge = self.charge;
        self.charge = null;
        self.shell.destroyUnused();
        const metadata = self.metadata;
        metadata.destroy(self);
        if (charge) |owner| owner.release();
    }
};

test "temporary recurrent peak loans preserve exact quota across sequential returns until actual invocation B seals" {
    const Owner = @import("cleanup_owner.zig").Owner;
    const domain = try admission.Domain.create();
    defer domain.release();
    const Probe = struct {
        gate: *std.atomic.Mutex,
        held: bool = false,
        releases: usize = 0,
        fn reserve(_: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            return 1;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.releases += 1;
            if (!self.gate.tryLock()) self.held = true else self.gate.unlock();
        }
    };
    var gate: std.atomic.Mutex = .unlocked;
    var probe = Probe{ .gate = &gate };
    try domain.controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    const layout = Layout{ .key_heads = 1, .value_heads = 1 };
    const bytes = try layout.bytes();
    var lease: ?memory.AdmissionLease = try domain.controller.tryAcquire(.gpu, .{ .backend_limit_bytes = bytes }, .{ .backend_scratch_bytes = bytes }, false);
    defer if (lease) |*value| value.release();
    var pin: ?admission.Pin = domain.pin();
    defer if (pin) |value| value.release();
    var owner = try Owner.beginOutsideCleanup();
    defer owner.finishAfterUnlock();
    const epoch = owner.epoch().?;
    const budget = try Budget.prepare(layout, 17);
    var budget_live = true;
    defer if (budget_live) {
        if (budget.current != null or budget.boundary_held) budget.retireWithCleanup(.{ .boundary = epoch }) catch unreachable else budget.destroyUnusedOutsideExecution() catch unreachable;
    };
    try budget.arm(&lease, &pin);
    try budget.bind(epoch);
    while (!gate.tryLock()) std.atomic.spinLoopHint();
    var held = true;
    defer if (held) gate.unlock();
    for (0..3) |_| {
        try std.testing.expectError(error.ForeignRecurrentStateBudget, budget.acquire(18, layout));
        try budget.acquire(17, layout);
        try std.testing.expectError(error.RecurrentStateLoanStillLive, budget.acquire(17, layout));
        try std.testing.expectError(error.ForeignRecurrentStateCleanup, budget.returnAfterBackingRetired(.{ .boundary = epoch, .outside_execution = true }));
        try budget.returnAfterBackingRetired(.{ .boundary = epoch });
        try std.testing.expectEqual(bytes, domain.controller.snapshot().backend_scratch_bytes);
        try std.testing.expectEqual(@as(usize, 0), probe.releases);
    }
    gate.unlock();
    held = false;
    try budget.unbind();
    try budget.destroyBoundRoot();
    budget_live = false;
    try std.testing.expectEqual(bytes, domain.controller.snapshot().backend_scratch_bytes);
    owner.finishAfterUnlock();
    try std.testing.expectEqual(@as(usize, 0), domain.controller.snapshot().backend_scratch_bytes);
    try std.testing.expect(!probe.held and probe.releases == 1);
}

test "temporary recurrent layout shares exact original fixed geometry bound and rejects malformed heads" {
    try std.testing.expectEqual(@as(usize, 3 * 128 * 4 * 4 * 3 + 128 * 128 * 4 + 130 * 4), try (Layout{ .key_heads = 1, .value_heads = 1 }).bytes());
    try std.testing.expectError(error.InvalidTensorShape, (Layout{ .key_heads = 0, .value_heads = 1 }).bytes());
    try std.testing.expectError(error.InvalidTensorShape, (Layout{ .key_heads = 2, .value_heads = 3 }).bytes());
    try std.testing.expectError(error.InvalidTensorShape, (Layout{ .key_heads = 1, .value_heads = 257 }).bytes());
}

test "temporary recurrent cold header OOM precedes any quota handoff" {
    var failed = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, Budget.prepareWithAllocator(failed.allocator(), .{ .key_heads = 1, .value_heads = 1 }, 1));
    try std.testing.expectEqual(@as(usize, 0), failed.allocated_bytes);
}

/// Native generation state is request-owned. Its additive bytes remain in the
/// SAME primary throughout the request; this owner never reserves, partitions,
/// retains, or refunds quota. The actual caller closes it AFTER all Model/step
/// gates and BEFORE releasing that primary. Raw createRuntime APIs are unchanged.
pub const NativeHostState = struct {
    const Gpt = @import("../architectures/gpt.zig");
    const Session = @import("../backends/session.zig").Session;
    const Config = @import("../models/gpt.zig").Config;
    const Geometry = @import("../models/qwen35_state_layout.zig").Layout;
    const State = @import("../graph/decode_state_runtime.zig").NativeDecodeState;
    const Owner = @import("cleanup_owner.zig").Owner;
    const Control = @import("../execution_control.zig").InferenceExecutionControl;
    const Alignment = std.mem.Alignment.of(Gpt.Qwen35LinearLayerState);

    metadata: std.mem.Allocator,
    backing: []align(@alignOf(Gpt.Qwen35LinearLayerState)) u8,
    fixed: std.heap.FixedBufferAllocator,
    geometry: Geometry,
    cleanup: Owner,
    cache: ?Gpt.Qwen35LinearCache = null,
    live_allocations: usize = 0,
    borrowed: bool = false,

    pub fn requiredBytes(config: Config) !usize {
        if (!config.isQwen35() or !config.qwen35_has_linear_attention or config.qwen35_device_only_state) return 0;
        const layout = try Geometry.init(config, @sizeOf(Gpt.Qwen35LinearLayerState), @alignOf(Gpt.Qwen35LinearLayerState));
        if (layout.layers == 0) return 0;
        return std.math.add(usize, layout.backing_bytes, @sizeOf(NativeHostState) + @import("retirement_sink.zig").Ref.requestedMetadataBytes());
    }

    /// All constructors and cancellation unwinds run BEFORE actual lanes. The
    /// caller supplies the sum of target+draft additive components from the
    /// SAME primary, including this exact component once per independent state.
    pub fn prepare(session: Session, config: Config, primary: ?*memory.AdmissionLease, total_components: usize, control: Control) !?*NativeHostState {
        if (session.backend() != .native or session.run_admission == null or try requiredBytes(config) == 0) return null;
        const policy = session.run_admission.?;
        const held = primary orelse return error.MissingNativeRecurrentPrimary;
        const model = session.model_lifetime orelse return error.MissingSourceModelLifetime;
        const domain = model.admission_domain orelse return error.ForeignAdmissionDomain;
        if (held.controller != policy.controller or domain.controller() != policy.controller) return error.ForeignAdmissionDomain;
        const required = try requiredBytes(config);
        if (total_components < required or held.amounts.host_scratch_bytes < total_components or held.amounts_by_backend[@backingInt(memory.BackendClass.cpu)].host_scratch_bytes < total_components) return error.InsufficientNativeRecurrentPrimary;
        try control.check();
        const result = try prepareWithAllocator(allocator, config);
        errdefer result.destroyAfterUnlock() catch unreachable;
        try control.check();
        return result;
    }

    fn prepareWithAllocator(a: std.mem.Allocator, config: Config) !*NativeHostState {
        const layout = try Geometry.init(config, @sizeOf(Gpt.Qwen35LinearLayerState), @alignOf(Gpt.Qwen35LinearLayerState));
        const self = try a.create(NativeHostState);
        errdefer a.destroy(self);
        const bytes = try a.alignedAlloc(u8, Alignment, layout.backing_bytes);
        errdefer a.free(bytes);
        var cleanup = try Owner.beginOutsideCleanup();
        // This private Sink now belongs to an actual numerical invocation,
        // not the outside-only raw cleanup compatibility entry. Restrict that
        // capability before publishing the owner; its live B/finish are intact.
        cleanup.outside_cleanup = false;
        errdefer cleanup.finishAfterUnlock();
        self.* = .{ .metadata = a, .backing = bytes, .fixed = .init(bytes), .geometry = layout, .cleanup = cleanup };
        self.cache = try Gpt.Qwen35LinearCache.init(self.childAllocator(), config);
        self.cache.?.host_cleanup = .{ .ptr = self, .validate = validateHostCleanup };
        std.debug.assert(self.fixed.end_index == layout.backing_bytes);
        return self;
    }

    pub fn cleanupOwner(self: *NativeHostState) *Owner {
        return &self.cleanup;
    }

    pub fn adopt(self: *NativeHostState, state: *State, config: Config, current: Cleanup) !void {
        const actual = current.boundary orelse return error.MissingCurrentCleanupEpoch;
        const expected_epoch = self.cleanup.epoch() orelse return error.MissingCurrentCleanupEpoch;
        if (current.outside_execution or actual.record != expected_epoch.record or actual.nonce != expected_epoch.nonce or try actual.isSealed()) return error.ForeignRecurrentStateCleanup;
        const expected = try Geometry.init(config, @sizeOf(Gpt.Qwen35LinearLayerState), @alignOf(Gpt.Qwen35LinearLayerState));
        if (!std.meta.eql(expected, self.geometry)) return error.NativeRecurrentGeometryChanged;
        if (self.borrowed or state.qwen35_linear_cache != null) return error.NativeRecurrentStateAlreadyPublished;
        const value = self.cache orelse return error.NativeRecurrentStateAlreadyPublished;
        state.qwen35_linear_cache = value;
        self.cache = null;
        self.borrowed = true;
    }

    fn validateHostCleanup(raw: *anyopaque, current: Cleanup) !void {
        const self: *NativeHostState = @ptrCast(@alignCast(raw));
        if (!self.borrowed and current.outside_execution and current.boundary == null and current.operation == null) return;
        const actual = current.boundary orelse return error.MissingCurrentCleanupEpoch;
        const expected = self.cleanup.epoch() orelse return error.MissingCurrentCleanupEpoch;
        if (current.outside_execution or actual.record != expected.record or actual.nonce != expected.nonce or try actual.isSealed()) return error.ForeignRecurrentStateCleanup;
    }

    fn childAllocator(self: *NativeHostState) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
        const self: *NativeHostState = @ptrCast(@alignCast(raw));
        const ptr = self.fixed.allocator().rawAlloc(len, alignment, ret) orelse return null;
        self.live_allocations += 1;
        return ptr;
    }
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
    fn free(raw: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize) void {
        const self: *NativeHostState = @ptrCast(@alignCast(raw));
        std.debug.assert(self.live_allocations != 0);
        self.live_allocations -= 1;
        if (self.live_allocations == 0) self.borrowed = false;
        // Child frees retire logical views; the process slab remains live until
        // actual unlock. This allocator has no fallback or external callback.
    }

    /// Explicit caller custody, after real unlock on every exit. No creation B
    /// chooses cleanup: children were retired through the caller's NOW context;
    /// the outer guard is finished only after the actual process backing dies.
    pub fn destroyAfterUnlock(self: *NativeHostState) !void {
        if (self.borrowed) return error.NativeRecurrentStateStillBorrowed;
        if (self.cache) |value| {
            self.cache = null;
            var cache = value;
            try cache.deinitWithCleanup(.{ .outside_execution = true });
        }
        if (self.live_allocations != 0) return error.NativeRecurrentStateStillBorrowed;
        const a = self.metadata;
        var cleanup = self.cleanup;
        a.free(self.backing);
        a.destroy(self);
        cleanup.finishAfterUnlock();
    }
};

const native_host_test_config = @import("../models/gpt.zig").Config{ .family = .qwen3_5, .num_hidden_layers = 5, .qwen35_has_linear_attention = true, .qwen35_full_attention_interval = 4, .qwen35_linear_num_key_heads = 1, .qwen35_linear_num_value_heads = 1, .qwen35_linear_key_head_dim = 2, .qwen35_linear_value_head_dim = 2, .qwen35_linear_conv_kernel_dim = 4 };

test "Native recurrent cold backing keeps exact child pointers across reset and refuses foreign B before retirement" {
    const a = std.testing.allocator;
    var counted = std.testing.FailingAllocator.init(a, .{});
    const owner = try NativeHostState.prepareWithAllocator(counted.allocator(), native_host_test_config);
    var live = true;
    defer if (live) owner.destroyAfterUnlock() catch unreachable;
    var other = try @import("cleanup_owner.zig").Owner.beginOutsideCleanup();
    defer other.finishAfterUnlock();
    var state = @import("../graph/decode_state_runtime.zig").NativeDecodeState.initContiguous(a);
    var state_live = true;
    defer if (state_live) state.deinitWithCleanup(.{ .boundary = owner.cleanupOwner().epoch() }) catch unreachable;
    try std.testing.expectError(error.ForeignRecurrentStateCleanup, owner.adopt(&state, native_host_test_config, .{ .boundary = other.epoch() }));
    try std.testing.expect(state.qwen35_linear_cache == null and !owner.borrowed);
    const current = Cleanup{ .boundary = owner.cleanupOwner().epoch() };
    try owner.adopt(&state, native_host_test_config, current);
    const layers = state.qwen35_linear_cache.?.layers.ptr;
    const conv = state.qwen35_linear_cache.?.layers[0].conv.ptr;
    const recurrent = state.qwen35_linear_cache.?.layers[0].recurrent.ptr;
    state.qwen35_linear_cache.?.layers[0].conv[0] = 17;
    state.qwen35_linear_cache.?.layers[0].recurrent[0] = 19;
    const allocations = counted.alloc_index;
    for (0..3) |_| state.resetQwen35LinearCache();
    try std.testing.expectEqual(allocations, counted.alloc_index);
    try std.testing.expectEqual(layers, state.qwen35_linear_cache.?.layers.ptr);
    try std.testing.expectEqual(conv, state.qwen35_linear_cache.?.layers[0].conv.ptr);
    try std.testing.expectEqual(recurrent, state.qwen35_linear_cache.?.layers[0].recurrent.ptr);
    try std.testing.expectEqual(@as(f32, 0), state.qwen35_linear_cache.?.layers[0].recurrent[0]);
    const children = owner.live_allocations;
    try std.testing.expectError(error.ForeignRecurrentStateCleanup, state.deinitWithCleanup(.{ .boundary = other.epoch() }));
    try std.testing.expectEqual(children, owner.live_allocations);
    try std.testing.expectError(error.NativeRecurrentStateStillBorrowed, owner.destroyAfterUnlock());
    try state.deinitWithCleanup(current);
    state_live = false;
    try std.testing.expect(!owner.borrowed and owner.live_allocations == 0);
    // Logical child frees cannot refund or reclaim the parent early.
    try std.testing.expect(counted.allocated_bytes > counted.freed_bytes);
    try owner.destroyAfterUnlock();
    live = false;
    try std.testing.expectEqual(counted.allocated_bytes, counted.freed_bytes);
}

test "Native recurrent cold owner and slab construction unwind every owned allocation failure" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            const owner = try NativeHostState.prepareWithAllocator(a, native_host_test_config);
            defer owner.destroyAfterUnlock() catch unreachable;
        }
    };
    // Existing cleanup Owner/Sink constructor OOM tests cover its independent
    // process metadata; this enumerates every new owner/slab allocation here.
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "Native recurrent header-only cache owns exact cold storage and current B through reset and actual final header free" {
    const a = std.testing.allocator;
    var cfg = native_host_test_config;
    cfg.qwen35_full_attention_interval = 1;
    const layout = try NativeHostState.Geometry.init(cfg, @sizeOf(NativeHostState.Gpt.Qwen35LinearLayerState), @alignOf(NativeHostState.Gpt.Qwen35LinearLayerState));
    try std.testing.expectEqual(@as(usize, 0), layout.linear_layers);
    try std.testing.expectEqual(@as(usize, cfg.num_hidden_layers) * @sizeOf(NativeHostState.Gpt.Qwen35LinearLayerState), layout.backing_bytes);
    try std.testing.expectEqual(layout.backing_bytes + @sizeOf(NativeHostState) + @import("retirement_sink.zig").Ref.requestedMetadataBytes(), try NativeHostState.requiredBytes(cfg));
    const owner = try NativeHostState.prepareWithAllocator(a, cfg);
    var live = true;
    defer if (live) owner.destroyAfterUnlock() catch unreachable;
    var state = NativeHostState.State.initContiguous(a);
    var state_live = true;
    const current = Cleanup{ .boundary = owner.cleanupOwner().epoch() };
    defer if (state_live) state.deinitWithCleanup(current) catch unreachable;
    try owner.adopt(&state, cfg, current);
    try std.testing.expect(owner.borrowed and owner.live_allocations == 1);
    const headers = state.qwen35_linear_cache.?.layers.ptr;
    for (state.qwen35_linear_cache.?.layers) |layer| try std.testing.expect(layer.conv.len == 0 and layer.recurrent.len == 0);
    for (0..3) |_| state.resetQwen35LinearCache();
    try std.testing.expectEqual(headers, state.qwen35_linear_cache.?.layers.ptr);
    try std.testing.expectEqual(@as(usize, 1), owner.live_allocations);
    var other = try NativeHostState.Owner.beginOutsideCleanup();
    defer other.finishAfterUnlock();
    try std.testing.expectError(error.ForeignRecurrentStateCleanup, state.deinitWithCleanup(.{ .boundary = other.epoch() }));
    try std.testing.expect(owner.borrowed and owner.live_allocations == 1);
    try state.deinitWithCleanup(current);
    state_live = false;
    try std.testing.expect(!owner.borrowed and owner.live_allocations == 0);
    try owner.destroyAfterUnlock();
    live = false;
    // True no-producer/device-only cases retain the original exclusion.
    cfg.qwen35_device_only_state = true;
    try std.testing.expectEqual(@as(usize, 0), try NativeHostState.requiredBytes(cfg));
    cfg.qwen35_device_only_state = false;
    cfg.num_hidden_layers = 0;
    try std.testing.expectEqual(@as(usize, 0), try NativeHostState.requiredBytes(cfg));
    cfg = native_host_test_config;
    cfg.qwen35_has_linear_attention = false;
    try std.testing.expectEqual(@as(usize, 0), try NativeHostState.requiredBytes(cfg));
}

test "Native numerical recurrent owner never grants outside raw cleanup authority" {
    const owner = try NativeHostState.prepareWithAllocator(std.testing.allocator, native_host_test_config);
    defer owner.destroyAfterUnlock() catch unreachable;
    var pending: @import("resource_retirement.zig").SynchronousOutsideRawLeases = .{};
    try std.testing.expectError(error.MissingOutsideCleanupOwner, owner.cleanupOwner().synchronousOutsideContext(&pending));
    try std.testing.expect(pending.boundary_context == null);
    try std.testing.expect(!try owner.cleanupOwner().epoch().?.isSealed());
}
