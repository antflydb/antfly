// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Input-aware physical variants. A cached owner is never a hash proof: every
//! hit performs the backend's full metadata equivalence test, then dispatch
//! validates again before claiming/allocating a frame. Variants borrow semantic
//! graphs/backend contexts from the pinned program/invocation owner.
const std = @import("std");
const ml = @import("ml");
const ops = @import("../ops/ops.zig");
const route = @import("kernel_route.zig");
const physical = @import("physical_buffers.zig");
const platform = @import("antfly_platform");

/// Independently retained declared facts, shared directly by base and variants.
/// Only explicit metadata retention is allowed; missing provider support yields
/// unknown facts and preserves compatibility admission instead of cloning data.
pub const ImmutableBindings = struct {
    const OriginalTensor = struct { value: ops.CT, implementation: route.Implementation };
    allocator: std.mem.Allocator,
    references: std.atomic.Value(usize) = .init(1),
    entries: []route.InputBinding,
    original_tensor_values: []?OriginalTensor,
    // Entry plane slices borrow this single owner allocation.
    planes: []@import("storage_ownership.zig").Plane,
    indices: std.AutoHashMapUnmanaged(ml.graph.NodeId, usize) = .empty,

    pub fn create(allocator: std.mem.Allocator, inputs: []const route.InputBinding) !*ImmutableBindings {
        var plane_count: usize = 0;
        for (inputs) |input| plane_count = try std.math.add(usize, plane_count, switch (input.value) {
            .planned => |value| value.planes.len,
            .resident => |value| value.planes.len,
            else => 0,
        });
        const planes = try allocator.alloc(@import("storage_ownership.zig").Plane, plane_count);
        errdefer allocator.free(planes);
        const entries = try allocator.alloc(route.InputBinding, inputs.len);
        errdefer allocator.free(entries);
        const original_values = try allocator.alloc(?OriginalTensor, inputs.len);
        errdefer allocator.free(original_values);
        @memset(original_values, null);
        var initialized: usize = 0;
        errdefer for (entries[0..initialized]) |*entry| releaseBindingWithCleanup(&entry.value, .{});
        var indices = std.AutoHashMapUnmanaged(ml.graph.NodeId, usize).empty;
        errdefer indices.deinit(allocator);
        try indices.ensureTotalCapacity(allocator, std.math.cast(u32, inputs.len) orelse return error.TooManyKernelInputBindings);
        var plane_offset: usize = 0;
        for (inputs, entries, 0..) |input, *entry, index| {
            const slot = indices.getOrPutAssumeCapacity(input.node_id);
            if (slot.found_existing) return error.DuplicateKernelInputBinding;
            slot.value_ptr.* = index;
            const count = switch (input.value) {
                .planned => |value| value.planes.len,
                .resident => |value| value.planes.len,
                else => 0,
            };
            entry.* = .{ .node_id = input.node_id, .value = try retainBinding(input.value, planes[plane_offset..][0..count]) };
            plane_offset += count;
            original_values[index] = switch (input.value) {
                .tensor => |tensor| .{ .value = tensor.value, .implementation = tensor.implementation },
                else => null,
            };
            initialized += 1;
        }
        const self = try allocator.create(ImmutableBindings);
        self.* = .{ .allocator = allocator, .entries = entries, .original_tensor_values = original_values, .planes = planes, .indices = indices };
        return self;
    }
    fn backend(implementation: route.Implementation) ops.ComputeBackend {
        return .{ .ptr = implementation.context, .vtable = @ptrCast(@alignCast(implementation.table)) };
    }
    fn retainBinding(value: route.Binding, planes: []@import("storage_ownership.zig").Plane) !route.Binding {
        return switch (value) {
            .tensor => |input| blk: {
                const target = backend(input.implementation);
                const functions = target.vtable.kernels orelse break :blk .unknown;
                if (!functions.accepts(input.implementation.table)) break :blk .unknown;
                const retain_tensor = functions.retainImmutableTensor orelse break :blk .unknown;
                const retained = (try retain_tensor(target.ptr, input.value)) orelse break :blk .unknown;
                break :blk .{ .tensor = .{ .value = retained, .implementation = input.implementation } };
            },
            .planned => |input| copied: {
                @memcpy(planes, input.planes);
                break :copied .{ .planned = .{ .logical = input.logical, .planes = planes } };
            },
            .resident => |input| copied: {
                @memcpy(planes, input.planes);
                break :copied .{ .resident = .{ .logical = input.logical, .planes = planes, .implementation = input.implementation } };
            },
            .imported_host, .unknown => value,
        };
    }
    fn releaseBindingWithCleanup(value: *route.Binding, cleanup: @import("cleanup_context.zig").Context) void {
        switch (value.*) {
            .tensor => |input| backend(input.implementation).freeWithCleanup(input.value, cleanup),
            .planned, .resident, .imported_host, .unknown => {},
        }
        value.* = .unknown;
    }
    pub fn originalTensor(self: *const ImmutableBindings, node: ml.graph.NodeId) ?OriginalTensor {
        const index = self.indices.get(node) orelse return null;
        return self.original_tensor_values[index];
    }
    pub fn retain(self: *ImmutableBindings) *ImmutableBindings {
        std.debug.assert(self.references.fetchAdd(1, .monotonic) > 0);
        return self;
    }
    pub fn release(self: *ImmutableBindings) void {
        self.releaseWithCleanup(.{});
    }
    pub fn releaseWithCleanup(self: *ImmutableBindings, cleanup: @import("cleanup_context.zig").Context) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        for (self.entries) |*entry| releaseBindingWithCleanup(&entry.value, cleanup);
        self.allocator.free(self.planes);
        self.allocator.free(self.entries);
        self.allocator.free(self.original_tensor_values);
        self.indices.deinit(self.allocator);
        self.allocator.destroy(self);
    }
    pub fn find(self: *const ImmutableBindings, node: ml.graph.NodeId) ?route.Binding {
        const index = self.indices.get(node) orelse return null;
        return self.entries[index].value;
    }
};

/// Concrete immutable variant owner. Pool has only a borrowed route_owner back
/// pointer. Cache, a prepared invocation, claims and active frames own refs;
/// completed published tensors retain their independent Lease slot authority.
pub const PreparedRoutes = struct {
    allocator: std.mem.Allocator,
    references: std.atomic.Value(usize) = .init(1),
    pool: *physical.Pool,
    immutable: *ImmutableBindings,
    key: u64,

    pub fn create(allocator: std.mem.Allocator, pool: *physical.Pool, immutable: *ImmutableBindings, key: u64) !*PreparedRoutes {
        const self = try allocator.create(PreparedRoutes);
        self.* = .{ .allocator = allocator, .pool = pool, .immutable = immutable.retain(), .key = key };
        pool.route_owner = self;
        return self;
    }
    pub fn retain(self: *PreparedRoutes) *PreparedRoutes {
        std.debug.assert(self.references.fetchAdd(1, .monotonic) > 0);
        return self;
    }
    pub fn release(self: *PreparedRoutes) void {
        self.releaseWithCleanup(.{});
    }
    pub fn releaseWithCleanup(self: *PreparedRoutes, cleanup: @import("cleanup_context.zig").Context) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        self.pool.route_owner = null;
        self.pool.deinitWithCleanup(cleanup);
        self.immutable.releaseWithCleanup(cleanup);
        self.allocator.destroy(self);
    }
    pub inline fn matchesChecked(self: *const PreparedRoutes, inputs: []const route.InputBinding) anyerror!bool {
        return self.pool.matchesRoutesChecked(inputs);
    }
    pub fn matches(self: *const PreparedRoutes, inputs: []const route.InputBinding) bool {
        return self.pool.matchesRoutes(inputs);
    }
    pub fn validate(self: *const PreparedRoutes, inputs: []const route.InputBinding) !void {
        try self.pool.validateRoutes(inputs);
    }
    /// RuntimeInput includes immutable initializers on the compatibility API.
    /// An exact physical variant may only reuse their original bound handles;
    /// overriding model/literal nodes requires a fresh semantic preparation.
    pub fn validateRuntimeInputs(self: *const PreparedRoutes, inputs: []const @import("../graph/interpreter.zig").RuntimeInput, default_backend: *const ops.ComputeBackend) !void {
        for (inputs) |input| {
            const slot = self.pool.plan.slotForNode(input.node_id) orelse return error.InvalidRuntimeInputNode;
            // Weight initializers can also be parameter-shaped graph nodes.
            // The cold immutable contract takes precedence over that slot role.
            if (self.immutable.originalTensor(input.node_id)) |original| {
                const backend = input.source_backend orelse default_backend;
                if (input.value != original.value or backend.ptr != original.implementation.context or @as(*const anyopaque, backend.vtable) != original.implementation.table) return error.PreparedImmutableBindingChanged;
            } else if (self.immutable.find(input.node_id) != null or (!slot.roles.runtime_input and slot.kind != .runtime_input)) return error.PreparedImmutableBindingChanged;
        }
    }
    pub fn requiresCleanupContext(self: *PreparedRoutes) bool {
        return self.pool.requiresCleanupContext();
    }
    pub fn storageRequirement(self: *PreparedRoutes) !@import("../runtime/tier/memory.zig").AdmissionAmounts {
        return self.pool.storageRequirement();
    }
    pub fn claimImmutable(self: *PreparedRoutes, identity: @import("storage_admission.zig").Identity) !?physical.SharedLiteralStore.ImmutableClaim {
        const store = self.pool.literal_store orelse return null;
        return try store.claimImmutableFor(identity, self.pool.selection.?.literal_claim_selection);
    }
    /// Hold the warm token until this acquisition is consumed or released.
    /// Reprepare selects the explicit compatibility policy before admission.
    pub fn claimRegions(self: *PreparedRoutes, allocator: std.mem.Allocator, warm: ?*const physical.FrameClaim, inputs: []const route.InputBinding) !@import("region_claims.zig").Acquisition {
        return self.pool.claimRegions(allocator, warm, inputs);
    }
    pub fn validateWithClaims(self: *const PreparedRoutes, inputs: []const route.InputBinding, pending: ?*const @import("region_claims.zig").Set) !void {
        try self.pool.validateRoutesWithClaims(inputs, pending);
    }
    pub fn claimWarm(self: *PreparedRoutes, identity: @import("storage_admission.zig").Identity) !?physical.FrameClaim {
        return self.pool.claimWarm(identity);
    }
};

pub const CacheStats = struct { hits: u64 = 0, misses: u64 = 0, builds: u64 = 0, evictions: u64 = 0 };

/// Bounded LRU, with cold preparation and owner retirement outside the mutex.
/// Owner must expose retain/release/matches. This generic ownership primitive
/// lets GPU-free tests exercise eviction and exact matching without a device.
pub fn InternedVariants(comptime Owner: type) type {
    return struct {
        const Self = @This();
        allocator: std.mem.Allocator,
        mutex: std.atomic.Mutex = .unlocked,
        entries: std.ArrayList(*Owner) = .empty,
        limit: usize = 8,
        closed: bool = false,
        stats: CacheStats = .{},

        fn lookupLocked(self: *Self, facts: []const route.InputBinding, filter: anytype) anyerror!?*Owner {
            for (self.entries.items, 0..) |owner, index| {
                if (!filter.accept(owner)) continue;
                const matches = if (comptime @hasDecl(Owner, "matchesChecked")) try owner.matchesChecked(facts) else owner.matches(facts);
                if (!matches) continue;
                _ = self.entries.orderedRemove(index);
                self.entries.appendAssumeCapacity(owner);
                return owner.retain();
            }
            return null;
        }
        pub fn prepare(self: *Self, context: anytype, facts: []const route.InputBinding, comptime create: anytype) !?*Owner {
            const All = struct {
                fn accept(_: @This(), _: *Owner) bool {
                    return true;
                }
            };
            return self.prepareFiltered(context, facts, create, All{});
        }
        pub fn prepareFiltered(self: *Self, context: anytype, facts: []const route.InputBinding, comptime create: anytype, filter: anytype) !?*Owner {
            return self.prepareFilteredWithCleanup(context, facts, create, filter, .{});
        }
        pub fn prepareFilteredWithCleanup(self: *Self, context: anytype, facts: []const route.InputBinding, comptime create: anytype, filter: anytype, cleanup: @import("cleanup_context.zig").Context) !?*Owner {
            platform.sync.lockYielding(&self.mutex);
            if (self.closed) {
                self.mutex.unlock();
                return error.PreparedRouteCacheClosed;
            }
            const hit = self.lookupLocked(facts, filter) catch |err| {
                self.mutex.unlock();
                return err;
            };
            if (hit) |owner| {
                self.stats.hits +|= 1;
                self.mutex.unlock();
                return owner;
            }
            self.stats.misses +|= 1;
            self.mutex.unlock();
            const candidate = (try create(context, facts)) orelse return null;
            errdefer releaseOwnerWithCleanup(candidate, cleanup);
            var victim: ?*Owner = null;
            const result = blk: {
                platform.sync.lockYielding(&self.mutex);
                defer self.mutex.unlock();
                self.stats.builds +|= 1;
                if (self.closed) return error.PreparedRouteCacheClosed;
                if (self.limit == 0) break :blk candidate;
                // Another preparation may have interned the same full metadata
                // while this thread was doing cold work. Keep the single owner.
                if (try self.lookupLocked(facts, filter)) |existing| break :blk existing;
                if (self.entries.items.len < self.limit) try self.entries.ensureUnusedCapacity(self.allocator, 1);
                if (self.entries.items.len == self.limit) {
                    if (comptime Owner == PreparedRoutes) {
                        if (cleanup.boundary == null and self.entries.items[0].requiresCleanupContext()) return error.PreparedEvictionRequiresCleanupContext;
                    }
                    victim = self.entries.orderedRemove(0);
                    self.stats.evictions +|= 1;
                }
                self.entries.appendAssumeCapacity(candidate);
                break :blk candidate.retain();
            };
            if (result != candidate) releaseOwnerWithCleanup(candidate, cleanup);
            if (victim) |old| releaseOwnerWithCleanup(old, cleanup);
            return result;
        }
        fn releaseOwnerWithCleanup(owner: *Owner, cleanup: @import("cleanup_context.zig").Context) void {
            if (comptime @hasDecl(Owner, "releaseWithCleanup")) owner.releaseWithCleanup(cleanup) else owner.release();
        }
        pub fn snapshot(self: *Self) CacheStats {
            platform.sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            return self.stats;
        }
        pub fn deinit(self: *Self) void {
            self.deinitWithCleanup(.{});
        }
        pub fn deinitWithCleanup(self: *Self, cleanup: @import("cleanup_context.zig").Context) void {
            platform.sync.lockYielding(&self.mutex);
            self.closed = true;
            var entries = self.entries;
            self.entries = .empty;
            self.mutex.unlock();
            for (entries.items) |owner| {
                if (comptime @hasDecl(Owner, "releaseWithCleanup")) owner.releaseWithCleanup(cleanup) else owner.release();
            }
            entries.deinit(self.allocator);
        }
    };
}
pub const Cache = InternedVariants(PreparedRoutes);

const TestOwner = struct {
    const Factory = struct {
        allocator: std.mem.Allocator,
        destroyed: std.atomic.Value(usize) = .init(0),
    };
    factory: *Factory,
    references: std.atomic.Value(usize) = .init(1),
    // Deliberate collision: keys never prove metadata equivalence.
    key: u64 = 0,
    dimension: i64,
    fn make(factory: *Factory, facts: []const route.InputBinding) !?*TestOwner {
        const dimension = facts[0].value.imported_host.logical.dim(0);
        const self = try factory.allocator.create(TestOwner);
        self.* = .{ .factory = factory, .dimension = dimension };
        return self;
    }
    fn retain(self: *TestOwner) *TestOwner {
        _ = self.references.fetchAdd(1, .monotonic);
        return self;
    }
    fn release(self: *TestOwner) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        _ = self.factory.destroyed.fetchAdd(1, .monotonic);
        self.factory.allocator.destroy(self);
    }
    fn matches(self: *const TestOwner, facts: []const route.InputBinding) bool {
        return facts.len == 1 and facts[0].value == .imported_host and facts[0].value.imported_host.logical.dim(0) == self.dimension;
    }
};
fn testFact(dimension: i64) route.InputBinding {
    return .{ .node_id = 0, .value = .{ .imported_host = .{ .logical = ml.graph.Shape.init(.f32, &.{dimension}), .target = .native } } };
}

test "prepared route cache compares full metadata and retained owners survive bounded eviction" {
    var factory = TestOwner.Factory{ .allocator = std.testing.allocator };
    var cache = InternedVariants(TestOwner){ .allocator = std.testing.allocator, .limit = 2 };
    const first = (try cache.prepare(&factory, &.{testFact(1)}, TestOwner.make)).?;
    const second = (try cache.prepare(&factory, &.{testFact(2)}, TestOwner.make)).?;
    try std.testing.expect(first != second and first.key == second.key);
    const repeated = (try cache.prepare(&factory, &.{testFact(1)}, TestOwner.make)).?;
    try std.testing.expect(repeated == first);
    const third = (try cache.prepare(&factory, &.{testFact(3)}, TestOwner.make)).?;
    try std.testing.expectEqual(@as(usize, 0), factory.destroyed.load(.monotonic));
    try std.testing.expect(second.matches(&.{testFact(2)}));
    const stats = cache.snapshot();
    try std.testing.expectEqual(@as(u64, 1), stats.hits);
    try std.testing.expectEqual(@as(u64, 3), stats.misses);
    try std.testing.expectEqual(@as(u64, 3), stats.builds);
    try std.testing.expectEqual(@as(u64, 1), stats.evictions);
    cache.deinit();
    try std.testing.expectError(error.PreparedRouteCacheClosed, cache.prepare(&factory, &.{testFact(1)}, TestOwner.make));
    first.release();
    second.release();
    repeated.release();
    third.release();
    try std.testing.expectEqual(@as(usize, 3), factory.destroyed.load(.monotonic));
}

test "prepared route cache construction and eviction unwind every allocation failure" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var factory = TestOwner.Factory{ .allocator = a };
            var cache = InternedVariants(TestOwner){ .allocator = a, .limit = 1 };
            defer cache.deinit();
            const first = (try cache.prepare(&factory, &.{testFact(1)}, TestOwner.make)).?;
            defer first.release();
            const next = (try cache.prepare(&factory, &.{testFact(2)}, TestOwner.make)).?;
            defer next.release();
            try std.testing.expect(first != next);
        }
    };
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "declared route bindings deep copy plane facts and reject duplicate node ownership" {
    const ownership = @import("storage_ownership.zig");
    var planes = [_]ownership.Plane{.{ .descriptor = @import("../graph/backend_contracts.zig").TensorDesc.init(ml.graph.Shape.init(.f32, &.{1}), .host_f32), .role = .primary, .layout_key = 1, .storage_epoch = 1, .byte_offset = 0, .logical_bytes = 4, .readable_bytes = 4 }};
    const input = route.InputBinding{ .node_id = 2, .value = .{ .planned = .{ .logical = ml.graph.Shape.init(.f32, &.{1}), .planes = &planes } } };
    const owner = try ImmutableBindings.create(std.testing.allocator, &.{input});
    defer owner.release();
    planes[0].logical_bytes = 100;
    try std.testing.expectEqual(@as(usize, 4), owner.find(2).?.planned.planes[0].logical_bytes);
    try std.testing.expectError(error.DuplicateKernelInputBinding, ImmutableBindings.create(std.testing.allocator, &.{ input, input }));
}

test "prepared region cache keeps compatibility policy separate for identical input facts" {
    const Case = struct {
        const Context = struct { factory: *TestOwner.Factory, policy: u64 };
        const Filter = struct {
            policy: u64,
            pub fn accept(self: @This(), owner: *TestOwner) bool {
                return owner.key == self.policy;
            }
        };
        fn make(context: Context, facts: []const route.InputBinding) !?*TestOwner {
            const owner = (try TestOwner.make(context.factory, facts)).?;
            owner.key = context.policy;
            return owner;
        }
        fn run(a: std.mem.Allocator) !void {
            var factory = TestOwner.Factory{ .allocator = a };
            var cache = InternedVariants(TestOwner){ .allocator = a, .limit = 2 };
            defer cache.deinit();
            const selected = (try cache.prepareFiltered(Context{ .factory = &factory, .policy = 1 }, &.{testFact(4)}, make, Filter{ .policy = 1 })).?;
            defer selected.release();
            const compatible = (try cache.prepareFiltered(Context{ .factory = &factory, .policy = 2 }, &.{testFact(4)}, make, Filter{ .policy = 2 })).?;
            defer compatible.release();
            try std.testing.expect(selected != compatible);
            const repeated = (try cache.prepareFiltered(Context{ .factory = &factory, .policy = 1 }, &.{testFact(4)}, make, Filter{ .policy = 1 })).?;
            defer repeated.release();
            try std.testing.expect(repeated == selected);
            try std.testing.expectEqual(@as(u64, 1), cache.snapshot().hits);
            try std.testing.expectEqual(@as(u64, 2), cache.snapshot().builds);
        }
    };
    try Case.run(std.testing.allocator);
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

fn packedInputSnapshotsCase(a: std.mem.Allocator) !void {
    const ownership = @import("storage_ownership.zig");
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const tensor = try cb.fromFloat32Shape(&.{ 1, -2, 3, -4 }, &.{4});
    var tensor_live = true;
    defer if (tensor_live) cb.free(tensor);
    const implementation = route.Implementation{ .context = cb.ptr, .table = cb.vtable };
    const shape = ml.graph.Shape.init(.f32, &.{4});
    var planes = [_]ownership.Plane{
        .{ .descriptor = @import("../graph/backend_contracts.zig").TensorDesc.init(shape, .host_f32), .role = .primary, .layout_key = 7, .storage_epoch = 9, .byte_offset = 0, .logical_bytes = 16, .readable_bytes = 16 },
        .{ .descriptor = @import("../graph/backend_contracts.zig").TensorDesc.init(shape, .host_f32), .role = .host_mirror, .layout_key = 11, .storage_epoch = 13, .byte_offset = 4, .logical_bytes = 16, .readable_bytes = 20 },
        .{ .descriptor = @import("../graph/backend_contracts.zig").TensorDesc.init(shape, .host_f32), .role = .primary, .layout_key = 17, .storage_epoch = 19, .byte_offset = 8, .logical_bytes = 16, .readable_bytes = 24 },
    };
    const original = planes;
    var inputs = [_]route.InputBinding{
        .{ .node_id = 0, .value = .{ .resident = .{ .logical = shape, .planes = planes[0..2], .implementation = implementation } } },
        .{ .node_id = 1, .value = .{ .planned = .{ .logical = shape, .planes = planes[2..] } } },
        .{ .node_id = 2, .value = .{ .tensor = .{ .value = tensor, .implementation = implementation } } },
        .{ .node_id = 3, .value = .{ .resident = .{ .logical = shape, .planes = &.{}, .implementation = implementation } } },
        .{ .node_id = 4, .value = .{ .planned = .{ .logical = shape, .planes = &.{} } } },
        testFact(4),
        .{ .node_id = 6, .value = .unknown },
    };
    inputs[5].node_id = 5;
    // A duplicate reached after the real tensor retain must unwind that retain
    // as well as the flat snapshot, without consuming the original caller CT.
    if (ImmutableBindings.create(a, &.{ inputs[2], inputs[0], inputs[1], inputs[2] })) |unexpected| {
        unexpected.release();
        return error.ExpectedDuplicateBinding;
    } else |err| switch (err) {
        error.DuplicateKernelInputBinding => {},
        else => return err,
    }
    try std.testing.expectEqualSlices(f32, &.{ 1, -2, 3, -4 }, (try cb.hostFloat32Read(tensor)).?);
    const first = try ImmutableBindings.create(a, &inputs);
    var first_live = true;
    defer if (first_live) first.release();
    const second = try ImmutableBindings.create(a, &inputs);
    defer second.release();
    try std.testing.expect(first.planes.ptr != second.planes.ptr);
    try std.testing.expectEqual(@as(usize, 3), first.planes.len);
    try std.testing.expect(first.find(0).?.resident.planes.ptr == first.planes.ptr);
    try std.testing.expect(first.find(1).?.planned.planes.ptr == first.planes[2..].ptr);
    try std.testing.expectEqual(@as(usize, 0), first.find(3).?.resident.planes.len);
    try std.testing.expectEqual(@as(usize, 0), first.find(4).?.planned.planes.len);
    try std.testing.expect(first.originalTensor(2).?.value == tensor);
    try std.testing.expect(first.originalTensor(0) == null);
    // Both real receiving snapshots survive mutation of every source plane
    // and the source binding list. No request aliases the authoring storage.
    for (&planes) |*plane| {
        plane.storage_epoch = 100;
        plane.logical_bytes = 0;
        plane.readable_bytes = 0;
    }
    for (&inputs) |*input| input.value = .unknown;
    try std.testing.expectEqualDeep(original[0..2], first.find(0).?.resident.planes);
    try std.testing.expectEqualDeep(original[2..], first.find(1).?.planned.planes);
    try std.testing.expectEqualDeep(original[0..2], second.find(0).?.resident.planes);
    cb.free(tensor);
    tensor_live = false;
    const retained_first = first.retain();
    var retained_first_live = true;
    defer if (retained_first_live) retained_first.release();
    first.release();
    first_live = false;
    try std.testing.expectEqualSlices(f32, &.{ 1, -2, 3, -4 }, (try cb.hostFloat32Read(retained_first.find(2).?.tensor.value)).?);
    retained_first.release();
    retained_first_live = false;
    // The last reference of the first complete owner is gone; the second
    // still owns both its copied planes and the actual retained tensor.
    try std.testing.expectEqualDeep(original[2..], second.find(1).?.planned.planes);
    try std.testing.expectEqualSlices(f32, &.{ 1, -2, 3, -4 }, (try cb.hostFloat32Read(second.find(2).?.tensor.value)).?);
}

test "packed immutable input snapshots preserve mutable source copies and retained tensor ownership across last owner death" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try packedInputSnapshotsCase(std.testing.allocator);
}

test "packed immutable input snapshot construction unwinds every allocation failure including initialized tensor retains" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, packedInputSnapshotsCase, .{});
}
