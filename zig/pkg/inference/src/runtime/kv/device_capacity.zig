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

//! Metadata-only encoded KV growth. Canonical providers compute targets using
//! their existing growth policy; callers admit the complete new plane envelope
//! while every old backing stays owned. Preparing a claim precedes numerical
//! gates/SDK allocation. Physical retirement receives the actual cleanup epoch.
const std = @import("std");
const memory = @import("../tier/memory.zig");
const storage = @import("../../execution/storage_admission.zig");
const retirement = @import("../../execution/resource_retirement.zig");
const sink = @import("../../execution/retirement_sink.zig");
const Backend = @import("../../graph/backend_contracts.zig").BackendKind;
const platform = @import("antfly_platform");
const metadata_allocator = platform.allocator.processAllocator(platform.allocator.concurrentFallback());

/// Logical head-row geometry is interpreted from immutable decoder metadata.
/// Encoding/scale/residual row bytes remain owned by the canonical provider.
pub const LayerGeometry = struct {
    index: u16,
    num_kv_heads: u32,
    head_dim: u32,
    pub fn width(self: LayerGeometry) !usize {
        if (self.num_kv_heads == 0 or self.head_dim == 0) return error.InvalidDeviceCapacityLayout;
        return std.math.mul(usize, self.num_kv_heads, self.head_dim);
    }
};
pub const Geometry = struct {
    layers: [256]LayerGeometry = undefined,
    count: u16 = 0,
    pub fn append(self: *Geometry, layer: LayerGeometry) !void {
        if (self.count >= self.layers.len) return error.InvalidDeviceCapacityLayout;
        _ = try layer.width();
        // Immutable adapters publish a canonical sparse ordering. This also
        // excludes donor duplicates without another cold map/allocation.
        if (self.count != 0 and self.layers[self.count - 1].index >= layer.index) return error.InvalidDeviceCapacityLayout;
        self.layers[self.count] = layer;
        self.count += 1;
    }
    pub fn dense(count: u16, heads: u32, dimension: u32) !Geometry {
        if (count == 0 or count > 256) return error.InvalidDeviceCapacityLayout;
        var geometry = Geometry{};
        for (0..count) |index| try geometry.append(.{ .index = @intCast(index), .num_kv_heads = heads, .head_dim = dimension });
        return geometry;
    }
    pub fn items(self: *const Geometry) []const LayerGeometry {
        return self.layers[0..self.count];
    }
};

pub const Plane = struct {
    handle: usize,
    length: usize,
    allocated: usize,
    row_bytes: usize,
    new_length: usize = 0,
    new_bound: usize = 0,
};
pub const Layer = struct {
    index: u16,
    geometry: ?LayerGeometry = null,
    slot: u32,
    current_tokens: usize,
    target_tokens: usize,
    keys: Plane,
    values: Plane,
    table: ?Plane = null,
};
pub const Plan = struct {
    backend: Backend,
    owner: usize,
    implementation: usize,
    epoch: u64,
    minimum_tokens: usize,
    page_size: u16,
    layers: [256]Layer = undefined,
    count: u16 = 0,
    old_bytes: usize = 0,
    new_bytes: usize = 0,

    pub fn append(self: *Plan, layer: Layer) !void {
        if (self.owner == 0 or self.implementation == 0 or self.epoch == 0 or self.minimum_tokens == 0 or self.page_size == 0 or self.count >= self.layers.len or layer.target_tokens < @max(layer.current_tokens, self.minimum_tokens)) return error.InvalidDeviceCapacityPlan;
        for (self.layers[0..self.count]) |prior| if (prior.index == layer.index) return error.InvalidDeviceCapacityPlan;
        if (layer.geometry) |layout| {
            if (layout.index != layer.index) return error.InvalidDeviceCapacityLayout;
            _ = try layout.width();
        }
        var old: usize = 0;
        var fresh: usize = 0;
        for ([_]Plane{ layer.keys, layer.values }) |plane| {
            const logical = try std.math.mul(usize, layer.current_tokens, plane.row_bytes);
            if (plane.handle == 0 or plane.row_bytes == 0 or logical > plane.length or plane.length > plane.allocated) return error.InvalidDeviceCapacityPlan;
            old = try std.math.add(usize, old, plane.allocated);
            const required = try std.math.mul(usize, layer.target_tokens, plane.row_bytes);
            if (required > plane.length) {
                if (plane.new_length != required or plane.new_bound < plane.new_length) return error.InvalidDeviceCapacityPlan;
                fresh = try std.math.add(usize, fresh, plane.new_bound);
            } else if (plane.new_length != 0 or plane.new_bound != 0) return error.InvalidDeviceCapacityPlan;
        }
        if (layer.table) |table| {
            if (table.handle == 0 or table.row_bytes != @sizeOf(u32) or table.length > table.allocated or table.length % table.row_bytes != 0) return error.InvalidDeviceCapacityPlan;
            old = try std.math.add(usize, old, table.allocated);
            if (table.new_length != 0) {
                if (table.new_length < table.length or table.new_bound < table.new_length or table.new_length % table.row_bytes != 0) return error.InvalidDeviceCapacityPlan;
                fresh = try std.math.add(usize, fresh, table.new_bound);
            } else if (table.new_bound != 0) return error.InvalidDeviceCapacityPlan;
        }
        const old_total = try std.math.add(usize, self.old_bytes, old);
        const new_total = try std.math.add(usize, self.new_bytes, fresh);
        _ = try std.math.add(usize, old_total, new_total);
        self.layers[self.count] = layer;
        self.count += 1;
        self.old_bytes = old_total;
        self.new_bytes = new_total;
    }
    /// Full NEW ownership for a cold bank. An empty provider has no handles;
    /// a real cached-unowned handle identifies staged reuse but grants no old
    /// credit. Resident growth retains its separate append validation.
    pub fn appendCold(self: *Plan, layer: Layer) !void {
        if (self.owner == 0 or self.implementation == 0 or self.epoch == 0 or self.minimum_tokens == 0 or self.page_size == 0 or self.count >= self.layers.len or self.old_bytes != 0 or layer.current_tokens != 0 or layer.target_tokens < self.minimum_tokens) return error.InvalidDeviceCapacityPlan;
        for (self.layers[0..self.count]) |prior| if (prior.index == layer.index) return error.InvalidDeviceCapacityPlan;
        if (layer.geometry) |layout| {
            if (layout.index != layer.index) return error.InvalidDeviceCapacityLayout;
            _ = try layout.width();
        }
        var fresh: usize = 0;
        for ([_]Plane{ layer.keys, layer.values }) |plane| {
            if (plane.length != 0 or plane.allocated != 0 or plane.row_bytes == 0) return error.InvalidDeviceCapacityPlan;
            const logical = try std.math.mul(usize, layer.target_tokens, plane.row_bytes);
            if (plane.new_length < logical or plane.new_bound < plane.new_length) return error.InvalidDeviceCapacityPlan;
            fresh = try std.math.add(usize, fresh, plane.new_bound);
        }
        if (layer.table) |table| {
            if (table.length != 0 or table.allocated != 0 or table.row_bytes != @sizeOf(u32) or table.new_length == 0 or table.new_length % table.row_bytes != 0 or table.new_bound < table.new_length) return error.InvalidDeviceCapacityPlan;
            fresh = try std.math.add(usize, fresh, table.new_bound);
        }
        const total = try std.math.add(usize, self.new_bytes, fresh);
        self.layers[self.count] = layer;
        self.count += 1;
        self.new_bytes = total;
    }

    pub fn requiredGeometry(self: *const Plan) !Geometry {
        var result = Geometry{};
        if (self.count > self.layers.len) return error.InvalidDeviceCapacityPlan;
        for (self.layers[0..self.count]) |layer| try result.append(layer.geometry orelse return error.InvalidDeviceCapacityLayout);
        return result;
    }
    pub fn peakBytes(self: *const Plan) !usize {
        return std.math.add(usize, self.old_bytes, self.new_bytes);
    }
    pub fn eql(self: *const Plan, other: *const Plan) bool {
        if (self.count > self.layers.len or other.count > other.layers.len) return false;
        if (self.backend != other.backend or self.owner != other.owner or self.implementation != other.implementation or self.epoch != other.epoch or self.minimum_tokens != other.minimum_tokens or self.page_size != other.page_size or self.count != other.count or self.old_bytes != other.old_bytes or self.new_bytes != other.new_bytes) return false;
        for (self.layers[0..self.count], other.layers[0..other.count]) |a, b| if (!std.meta.eql(a, b)) return false;
        return true;
    }
};

/// Borrows an actual pre-admission owner for this synchronous reserve only.
/// No legacy allocation estimate can supply old-backing credit. A pure domain
/// pin enables late SDK buffer-witness collection after model/provider teardown.
pub const Admission = struct {
    identity: storage.Identity,
    context: *anyopaque,
    reserve: *const fn (*anyopaque, std.mem.Allocator, usize) anyerror!storage.Reservation,
    domain: ?@import("../tier/admission_domain.zig").Pin = null,
};

pub const BackingOwner = @import("../../execution/backing_retirement.zig").BackingOwner;

const OwnerBatch = struct {
    references: std.atomic.Value(usize) = .init(1), // Claim plus initialized cells
    plan: Plan,
    owners: [256][3]?*BackingOwner = @splat(@splat(null)),
    cells: [768]BackingOwner = undefined,
    initialized: usize = 0,

    fn releaseCell(raw: *anyopaque) void {
        const self: *OwnerBatch = @ptrCast(@alignCast(raw));
        self.release();
    }
    fn release(self: *OwnerBatch) void {
        const count = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(count != 0);
        if (count == 1) metadata_allocator.destroy(self);
    }
};

pub const Claim = struct {
    batch: ?*OwnerBatch,
    attempted: bool = false,

    /// One complete external reserve, then prepare independent plane charges
    /// before any SDK work or numerical gate. Metadata cells share one slab;
    /// exact quota release remains independent of that slab's lifetime.
    pub fn prepare(plan: *const Plan, admission: Admission) !Claim {
        if ((plan.backend != .cuda and plan.backend != .metal) or plan.count == 0 or plan.count > 256 or plan.new_bytes == 0) return error.InvalidDeviceCapacityPlan;
        if (admission.identity.backend_class != .gpu) return error.InvalidDeviceCapacityAdmission;
        if (plan.backend == .metal and admission.domain == null) return error.MissingDeviceCapacityDomain;
        if (admission.domain) |domain| if (domain.controller() != admission.identity.controller) return error.InvalidDeviceCapacityAdmission;
        const batch = try metadata_allocator.create(OwnerBatch);
        batch.* = .{ .plan = plan.* };
        errdefer {
            // Construction is outside the provider/model gate. No SDK payload
            // exists, so no cleanup Epoch or physical witness is needed yet.
            for (batch.cells[0..batch.initialized]) |*cell| cell.charge.release();
            metadata_allocator.destroy(batch);
        }
        var reservation: ?storage.Reservation = try admission.reserve(admission.context, metadata_allocator, plan.new_bytes);
        defer if (reservation) |*value| value.release();
        if (!reservation.?.identity.eql(admission.identity) or !std.meta.eql(reservation.?.amounts(), memory.AdmissionAmounts{ .backend_kv_bytes = plan.new_bytes })) return error.InvalidDeviceCapacityAdmission;
        for (plan.layers[0..plan.count], 0..) |layer, index| {
            const planes = [_]?Plane{ layer.keys, layer.values, layer.table };
            for (planes, 0..) |maybe_plane, which| {
                const plane = maybe_plane orelse continue;
                if (plane.new_bound == 0) continue;
                var piece: ?storage.Reservation = try reservation.?.partition(metadata_allocator, plane.new_bound);
                errdefer if (piece) |*value| value.release();
                var domain = if (admission.domain) |pin| @as(?@import("../tier/admission_domain.zig").Pin, pin.retain()) else null;
                errdefer if (domain) |pin| pin.release();
                const charge = try retirement.Charge.createReservation(&piece, &domain);
                if (comptime @import("build_options").enable_metal) if (plan.backend == .metal) {
                    charge.prepareBackendMetadata(.metal, 1, 1) catch |err| {
                        charge.release();
                        return err;
                    };
                };

                const owner = &batch.cells[batch.initialized];
                owner.init(charge, .{ .context = batch, .release = OwnerBatch.releaseCell });
                batch.owners[index][which] = owner;
                batch.initialized += 1;
                _ = batch.references.fetchAdd(1, .monotonic);
            }
        }
        if (reservation.?.reservedBytes() != 0) return error.InvalidDeviceCapacityPlan;
        return .{ .batch = batch };
    }
    pub fn require(self: *const Claim, plan: *const Plan) !void {
        const batch = self.batch orelse return error.DeviceCapacityClaimConsumed;
        if (self.attempted) return error.DeviceCapacityClaimConsumed;
        if (!batch.plan.eql(plan)) return error.DeviceCapacityClaimChanged;
    }
    pub fn ownerAt(self: *const Claim, layer: usize, which: usize) *BackingOwner {
        const batch = self.batch orelse @panic("consumed KV claim");
        std.debug.assert(layer < batch.plan.count and which < 3);
        return batch.owners[layer][which] orelse @panic("undeclared KV plane");
    }
    pub fn takeOwner(self: *Claim, layer: usize, which: usize) *BackingOwner {
        const owner = self.ownerAt(layer, which);
        self.batch.?.owners[layer][which] = null;
        return owner;
    }
    pub fn deinit(self: *Claim, cleanup: *const sink.Epoch) void {
        const batch = self.batch orelse return;
        self.batch = null;
        for (batch.owners[0..batch.plan.count]) |*row| for (row) |*owner| {
            if (owner.*) |value| value.release(cleanup);
            owner.* = null;
        };
        // Callback releases can destroy every cell, but this Claim's own
        // metadata reference keeps the slab alive until traversal finishes.
        batch.release();
    }
};

fn testPlan() !Plan {
    var plan = Plan{ .backend = .cuda, .owner = 1, .implementation = 2, .epoch = 1, .minimum_tokens = 8, .page_size = 4 };
    for (0..2) |index| try plan.append(.{ .index = @intCast(index), .slot = @intCast(index), .current_tokens = 4, .target_tokens = 8, .keys = .{ .handle = 0x1000 + index * 0x100, .length = 16, .allocated = 16, .row_bytes = 4, .new_length = 32, .new_bound = 32 }, .values = .{ .handle = 0x2000 + index * 0x100, .length = 16, .allocated = 16, .row_bytes = 4, .new_length = 32, .new_bound = 32 } });
    return plan;
}

test "device capacity plan counts full old and new envelopes and checks active metadata exactly" {
    var plan = try testPlan();
    try std.testing.expectEqual(@as(usize, 64), plan.old_bytes);
    try std.testing.expectEqual(@as(usize, 128), plan.new_bytes);
    try std.testing.expectEqual(@as(usize, 192), try plan.peakBytes());
    var changed = plan;
    changed.layers[1].values.handle += 16;
    try std.testing.expect(!plan.eql(&changed));
    changed = plan;
    changed.layers[255] = undefined; // Inactive metadata never participates.
    try std.testing.expect(plan.eql(&changed));
    var invalid = plan.layers[0];
    invalid.index = plan.count;
    invalid.keys.new_bound = 1;
    try std.testing.expectError(error.InvalidDeviceCapacityPlan, plan.append(invalid));
    try std.testing.expectEqual(@as(u16, 2), plan.count);
    try std.testing.expectEqual(@as(usize, 192), try plan.peakBytes());
}

test "device capacity full overlap quota survives current cleanup epoch until its exact outer unlock" {
    const Domain = @import("../tier/admission_domain.zig").Domain;
    const domain = try Domain.create();
    defer domain.release();
    var old = try domain.controller.tryAcquire(.gpu, .{}, .{ .backend_kv_bytes = 64 }, false);
    defer old.release();
    const Reserving = struct {
        domain: *Domain,
        source: memory.AdmissionLease,
        limit: usize,
        fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, bytes: usize) !storage.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return storage.fromRunKvLease(allocator, &self.source, .{ .controller = &self.domain.controller, .backend_class = .gpu, .limits = .{ .backend_limit_bytes = self.limit }, .check_live_memory = false }, bytes);
        }
    };
    var current = Reserving{ .domain = domain, .source = try domain.controller.tryAcquire(.gpu, .{}, .{}, false), .limit = 191 };
    defer current.source.release();
    const pin = domain.pin();
    defer pin.release();
    const admission = Admission{ .identity = .{ .controller = &domain.controller, .backend_class = .gpu }, .context = &current, .reserve = Reserving.reserve, .domain = pin };
    const plan = try testPlan();
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, Claim.prepare(&plan, admission));
    try std.testing.expectEqual(@as(usize, 64), domain.controller.snapshot().backend_kv_bytes);
    current.limit = 192;
    var receiver = try sink.Ref.create();
    defer receiver.release();
    var guard = try receiver.enter();
    defer guard.finish();
    var cleanup = try guard.retainEpoch();
    defer cleanup.release();
    var claim = try Claim.prepare(&plan, admission);
    defer claim.deinit(&cleanup);
    var next = try receiver.enter();
    defer next.finish();
    claim.deinit(&cleanup);
    try std.testing.expectEqual(@as(usize, 192), domain.controller.snapshot().backend_kv_bytes);
    guard.finish();
    try std.testing.expectEqual(@as(usize, 64), domain.controller.snapshot().backend_kv_bytes);
    try std.testing.expect(!try next.epoch.isSealed());
}

/// Shared actual-controller test fixture. The budget context is heap owned so
/// delayed Metal collector callbacks never borrow a returned test stack.
pub const TestAdmission = if (@import("builtin").is_test) struct {
    allocator: std.mem.Allocator,
    domain: *@import("../tier/admission_domain.zig").Domain,
    source: ?memory.AdmissionLease = null,
    old: ?memory.AdmissionLease = null,
    limit: usize,
    provider_gate: std.atomic.Mutex = .unlocked,
    actual_provider_gate: ?*std.atomic.Mutex = null,
    actual_model_gate: ?*std.atomic.Mutex = null,
    actual_sdk_active: ?*std.atomic.Value(bool) = null,
    references: std.atomic.Value(usize) = .init(1),
    released: std.atomic.Value(usize) = .init(0),
    next_token: std.atomic.Value(usize) = .init(1),

    pub fn create(allocator: std.mem.Allocator, old_bytes: usize, limit: usize) !*@This() {
        const domain = try @import("../tier/admission_domain.zig").Domain.create();
        const self = allocator.create(@This()) catch |err| {
            domain.release();
            return err;
        };
        self.* = .{ .allocator = allocator, .domain = domain, .limit = limit };
        errdefer self.deinit();
        try domain.controller.configureResourceBudget(.{ .context = self, .retain_context = retainContext, .release_context = dropContext, .try_reserve = reserveExternal, .retain = retainExternal, .release = releaseExternal });
        self.source = try domain.controller.tryAcquire(.gpu, .{}, .{}, false);
        self.old = try domain.controller.tryAcquire(.gpu, .{}, .{ .backend_kv_bytes = old_bytes }, false);
        return self;
    }
    pub fn deinit(self: *@This()) void {
        if (self.old) |*value| value.release();
        if (self.source) |*value| value.release();
        self.domain.release();
        dropContext(self);
    }
    pub fn admission(self: *@This()) Admission {
        return .{ .identity = .{ .controller = &self.domain.controller, .backend_class = .gpu }, .context = self, .reserve = reserveOwned, .domain = .{ .owner = self.domain } };
    }
    fn reserveOwned(raw: *anyopaque, allocator: std.mem.Allocator, bytes: usize) !storage.Reservation {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return storage.fromRunKvLease(allocator, &self.source.?, .{ .controller = &self.domain.controller, .backend_class = .gpu, .limits = .{ .backend_limit_bytes = self.limit }, .check_live_memory = false }, bytes);
    }
    fn retainContext(raw: *anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        _ = self.references.fetchAdd(1, .monotonic);
        return true;
    }
    fn dropContext(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.references.fetchSub(1, .acq_rel) == 1) self.allocator.destroy(self);
    }
    fn assertOutsideGate(self: *@This()) void {
        if (self.actual_sdk_active) |active| if (active.load(.acquire)) @panic("quota callback inside SDK scope");
        if (!self.provider_gate.tryLock()) @panic("capacity quota callback reentered held outer gate");
        self.provider_gate.unlock();
        for ([_]?*std.atomic.Mutex{ self.actual_provider_gate, self.actual_model_gate }) |optional| if (optional) |gate| {
            if (!gate.tryLock()) @panic("capacity callback ran under actual provider/model gate");
            gate.unlock();
        };
    }
    fn reserveExternal(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.assertOutsideGate();
        return self.next_token.fetchAdd(1, .monotonic);
    }
    fn retainExternal(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.assertOutsideGate();
    }
    fn releaseExternal(raw: *anyopaque, _: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.assertOutsideGate();
        _ = self.released.fetchAdd(1, .monotonic);
    }
} else void;

test "device capacity plane claims refund dead layers independently of a held old key or table-only successor" {
    const plan = try testPlan();
    const budget = try TestAdmission.create(std.testing.allocator, plan.old_bytes, try plan.peakBytes());
    defer budget.deinit();
    var receiver = try sink.Ref.create();
    defer receiver.release();
    var guard = try receiver.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    var claim = try Claim.prepare(&plan, budget.admission());
    defer claim.deinit(&epoch);
    try claim.require(&plan);
    claim.attempted = true;
    const held_key = claim.takeOwner(0, 0);
    const unrelated_value = claim.takeOwner(1, 1);
    // Other planes have died; one key and an unrelated value remain alive.
    claim.deinit(&epoch);
    guard.finish();
    try std.testing.expectEqual(plan.old_bytes + 64, budget.domain.controller.snapshot().backend_kv_bytes);
    var other_guard = try receiver.enter();
    defer other_guard.finish();
    var other_epoch = try other_guard.retainEpoch();
    defer other_epoch.release();
    unrelated_value.release(&other_epoch);
    try std.testing.expectEqual(plan.old_bytes + 64, budget.domain.controller.snapshot().backend_kv_bytes);
    other_guard.finish();
    try std.testing.expectEqual(plan.old_bytes + 32, budget.domain.controller.snapshot().backend_kv_bytes);
    held_key.release(&other_epoch); // sealed late release, independent metadata slab
    try std.testing.expectEqual(plan.old_bytes, budget.domain.controller.snapshot().backend_kv_bytes);

    // A table-only replacement cannot create ownership of unchanged K/V.
    var table_plan = Plan{ .backend = .cuda, .owner = 9, .implementation = 8, .epoch = 7, .minimum_tokens = 8, .page_size = 4 };
    try table_plan.append(.{ .index = 0, .slot = 0, .current_tokens = 8, .target_tokens = 8, .keys = .{ .handle = 1, .length = 32, .allocated = 32, .row_bytes = 4 }, .values = .{ .handle = 2, .length = 32, .allocated = 32, .row_bytes = 4 }, .table = .{ .handle = 3, .length = 4, .allocated = 4, .row_bytes = 4, .new_length = 8, .new_bound = 8 } });
    var table_claim = try Claim.prepare(&table_plan, budget.admission());
    defer table_claim.deinit(&other_epoch);
    try std.testing.expectEqual(@as(usize, 1), table_claim.batch.?.initialized);
    try std.testing.expect(table_claim.batch.?.owners[0][0] == null and table_claim.batch.?.owners[0][1] == null);
    const table = table_claim.takeOwner(0, 2);
    table_claim.deinit(&other_epoch);
    try std.testing.expectEqual(plan.old_bytes + 8, budget.domain.controller.snapshot().backend_kv_bytes);
    table.release(&other_epoch);
    try std.testing.expectEqual(plan.old_bytes, budget.domain.controller.snapshot().backend_kv_bytes);
}

test "device capacity heterogeneous layers never charge unchanged key value or replay table planes" {
    var plan = Plan{ .backend = .cuda, .owner = 1, .implementation = 2, .epoch = 3, .minimum_tokens = 8, .page_size = 4 };
    try plan.append(.{ .index = 0, .slot = 0, .current_tokens = 4, .target_tokens = 8, .keys = .{ .handle = 1, .length = 16, .allocated = 16, .row_bytes = 4, .new_length = 32, .new_bound = 32 }, .values = .{ .handle = 2, .length = 16, .allocated = 16, .row_bytes = 4, .new_length = 32, .new_bound = 32 } });
    try plan.append(.{ .index = 1, .slot = 1, .current_tokens = 16, .target_tokens = 16, .keys = .{ .handle = 3, .length = 64, .allocated = 64, .row_bytes = 4 }, .values = .{ .handle = 4, .length = 64, .allocated = 64, .row_bytes = 4 }, .table = .{ .handle = 5, .length = 16, .allocated = 16, .row_bytes = 4 } });
    const budget = try TestAdmission.create(std.testing.allocator, plan.old_bytes, try plan.peakBytes());
    defer budget.deinit();
    var receiver = try sink.Ref.create();
    defer receiver.release();
    var guard = try receiver.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    var claim = try Claim.prepare(&plan, budget.admission());
    defer claim.deinit(&epoch);
    try std.testing.expectEqual(@as(usize, 2), claim.batch.?.initialized);
    for (claim.batch.?.owners[1]) |owner| try std.testing.expect(owner == null);
    var changed = plan;
    changed.layers[1].keys.handle += 1;
    try std.testing.expectError(error.DeviceCapacityClaimChanged, claim.require(&changed));
    try claim.require(&plan);
    const key = claim.takeOwner(0, 0);
    claim.deinit(&epoch);
    guard.finish();
    try std.testing.expectEqual(plan.old_bytes + 32, budget.domain.controller.snapshot().backend_kv_bytes);
    key.release(&epoch);
    try std.testing.expectEqual(plan.old_bytes, budget.domain.controller.snapshot().backend_kv_bytes);
}

test "device capacity earlier shrink epoch never authorizes a newer physical cleanup gate" {
    var plan = Plan{ .backend = .cuda, .owner = 1, .implementation = 2, .epoch = 3, .minimum_tokens = 4, .page_size = 4 };
    try plan.append(.{ .index = 0, .slot = 0, .current_tokens = 4, .target_tokens = 4, .keys = .{ .handle = 1, .length = 16, .allocated = 16, .row_bytes = 4 }, .values = .{ .handle = 2, .length = 16, .allocated = 16, .row_bytes = 4 }, .table = .{ .handle = 3, .length = 4, .allocated = 4, .row_bytes = 4, .new_length = 32, .new_bound = 32 } });
    const budget = try TestAdmission.create(std.testing.allocator, plan.old_bytes, try plan.peakBytes());
    defer budget.deinit();
    var receiver = try sink.Ref.create();
    defer receiver.release();
    var growth = try receiver.enter();
    defer growth.finish();
    var a = try growth.retainEpoch();
    defer a.release();
    var claim = try Claim.prepare(&plan, budget.admission());
    defer claim.deinit(&a);
    const key = claim.takeOwner(0, 2);
    key.measure(&a, 16); // 32-byte conservative plane, 16-byte measured SDK span
    // This claim owns exactly one new physical plane.
    claim.deinit(&a);
    var physical = try receiver.enter();
    defer physical.finish();
    var b = try physical.retainEpoch();
    defer b.release();
    try std.testing.expect(budget.provider_gate.tryLock());
    var gate_held = true;
    defer if (gate_held) budget.provider_gate.unlock();
    key.release(&b); // actual final cleanup under B, while shrink(A) is pending
    growth.finish(); // Earlier A becomes ready while B's real gate remains held.
    // No padding/last-payload release callback may reenter B. Its entire
    // conservative plane remains charged until the actual B cleanup seal.
    try std.testing.expectEqual(plan.old_bytes + 32, budget.domain.controller.snapshot().backend_kv_bytes);
    budget.provider_gate.unlock();
    gate_held = false;
    physical.finish();
    try std.testing.expectEqual(plan.old_bytes, budget.domain.controller.snapshot().backend_kv_bytes);
}

test "device capacity replaces only uncovered physical planes and keeps reused capacity" {
    const example = try testPlan();
    var plan = Plan{ .backend = example.backend, .owner = example.owner, .implementation = example.implementation, .epoch = example.epoch, .minimum_tokens = example.minimum_tokens, .page_size = example.page_size };
    var layer = example.layers[0];
    layer.index = 3;
    layer.keys.length = 64;
    layer.keys.allocated = 64;
    layer.keys.new_length = 0;
    layer.keys.new_bound = 0;
    try plan.append(layer);
    try std.testing.expectEqual(@as(usize, 80), plan.old_bytes);
    try std.testing.expectEqual(@as(usize, 32), plan.new_bytes);
    var redundant = layer;
    redundant.index = 7;
    redundant.keys.new_length = 32;
    redundant.keys.new_bound = 32;
    try std.testing.expectError(error.InvalidDeviceCapacityPlan, plan.append(redundant));
    var missing = layer;
    missing.index = 7;
    missing.values.new_length = 0;
    missing.values.new_bound = 0;
    try std.testing.expectError(error.InvalidDeviceCapacityPlan, plan.append(missing));
    try std.testing.expectEqual(@as(u16, 1), plan.count);
    var warm = Plan{ .backend = example.backend, .owner = example.owner, .implementation = example.implementation, .epoch = example.epoch, .minimum_tokens = example.minimum_tokens, .page_size = example.page_size };
    layer.values = layer.keys;
    layer.values.handle += 16;
    try warm.append(layer);
    try std.testing.expectEqual(@as(usize, 128), warm.old_bytes);
    try std.testing.expectEqual(@as(usize, 0), warm.new_bytes);
    try std.testing.expectEqual(@as(usize, 128), try warm.peakBytes());
}

/// Frozen request geometry, separate from an epoch's refill/fairness limit.
/// Backends use their actual selected codecs/layouts to derive physical spans.
/// Shared physical block IDs reuse the bounded live window across refills.
pub const Window = struct {
    sequence_tokens: usize,
    live_sequences: usize,
    page_size: u16,
    /// Explicitly proven extra physical blocks (for example retained COW
    /// sources). Private per-sequence layouts must handle or reject these.
    spare_blocks: usize,

    pub fn fromRequest(prefix_tokens: usize, max_new_tokens: usize, live_sequences: usize, page_size: u16, spare_blocks: usize) !Window {
        const tokens = try std.math.add(usize, prefix_tokens, max_new_tokens);
        if (tokens == 0 or live_sequences == 0 or page_size == 0) return error.InvalidDeviceCapacityLayout;
        const result = Window{ .sequence_tokens = tokens, .live_sequences = live_sequences, .page_size = page_size, .spare_blocks = spare_blocks };
        _ = try result.sharedTokens();
        return result;
    }
    pub fn blocksPerSequence(self: Window) !usize {
        if (self.sequence_tokens == 0 or self.live_sequences == 0 or self.page_size == 0) return error.InvalidDeviceCapacityLayout;
        return std.math.divCeil(usize, self.sequence_tokens, self.page_size);
    }
    pub fn privateTokens(self: Window) !usize {
        return std.math.mul(usize, try self.blocksPerSequence(), self.page_size);
    }
    pub fn sharedBlocks(self: Window) !usize {
        return std.math.add(usize, try std.math.mul(usize, try self.blocksPerSequence(), self.live_sequences), self.spare_blocks);
    }
    pub fn sharedTokens(self: Window) !usize {
        return std.math.mul(usize, try self.sharedBlocks(), self.page_size);
    }
    /// Physical eligibility only. Workspace/input plans must separately prove
    /// their own immutable bounds before a request joins the existing epoch.
    pub fn covers(self: Window, request: Window) bool {
        if (self.page_size != request.page_size or self.live_sequences < request.live_sequences or self.spare_blocks < request.spare_blocks) return false;
        const available = self.privateTokens() catch return false;
        const needed = request.privateTokens() catch return false;
        return available >= needed;
    }
};

test "device capacity live window uses measured request bounds independent of refill count" {
    const window = try Window.fromRequest(17, 15, 3, 16, 1);
    try std.testing.expectEqual(@as(usize, 32), window.sequence_tokens);
    try std.testing.expectEqual(@as(usize, 32), try window.privateTokens());
    try std.testing.expectEqual(@as(usize, 7), try window.sharedBlocks());
    try std.testing.expectEqual(@as(usize, 112), try window.sharedTokens());
    try std.testing.expect(window.covers(try Window.fromRequest(16, 16, 3, 16, 1)));
    try std.testing.expect(!window.covers(try Window.fromRequest(33, 0, 3, 16, 1)));
    try std.testing.expect(!window.covers(try Window.fromRequest(17, 15, 4, 16, 1)));
    try std.testing.expect(!window.covers(try Window.fromRequest(17, 15, 3, 8, 1)));
    try std.testing.expectError(error.Overflow, Window.fromRequest(std.math.maxInt(usize), 1, 1, 16, 0));
    try std.testing.expectError(error.Overflow, Window.fromRequest(std.math.maxInt(usize), 0, 2, 1, 0));
    try std.testing.expectError(error.InvalidDeviceCapacityLayout, Window.fromRequest(0, 0, 1, 16, 0));
}

test "cold KV plan admits genuine empty planes and charges cached unowned spans without old credit" {
    var plan = Plan{ .backend = .cuda, .owner = 1, .implementation = 2, .epoch = 1, .minimum_tokens = 8, .page_size = 4 };
    const layer = Layer{ .index = 1, .geometry = .{ .index = 1, .num_kv_heads = 1, .head_dim = 2 }, .slot = 1, .current_tokens = 0, .target_tokens = 8, .keys = .{ .handle = 0, .length = 0, .allocated = 0, .row_bytes = 4, .new_length = 32, .new_bound = 32 }, .values = .{ .handle = 0, .length = 0, .allocated = 0, .row_bytes = 4, .new_length = 32, .new_bound = 32 }, .table = .{ .handle = 0, .length = 0, .allocated = 0, .row_bytes = 4, .new_length = 8, .new_bound = 8 } };
    try std.testing.expectError(error.InvalidDeviceCapacityPlan, plan.append(layer));
    try plan.appendCold(layer);
    try std.testing.expectEqual(@as(usize, 0), plan.old_bytes);
    try std.testing.expectEqual(@as(usize, 72), plan.new_bytes);
    var reused = layer;
    reused.index = 3;
    reused.geometry.?.index = 3;
    reused.slot = 3;
    reused.keys.handle = 0x1000;
    reused.keys.new_length = 48;
    reused.keys.new_bound = 64;
    reused.values.handle = 0x2000;
    reused.values.new_length = 48;
    reused.values.new_bound = 64;
    reused.table.?.handle = 0x3000;
    reused.table.?.new_bound = 16;
    try plan.appendCold(reused);
    try std.testing.expectEqual(@as(usize, 0), plan.old_bytes);
    try std.testing.expectEqual(@as(usize, 216), plan.new_bytes);
    var wrong = reused;
    wrong.index = 5;
    wrong.geometry.?.index = 5;
    wrong.keys.allocated = 64;
    try std.testing.expectError(error.InvalidDeviceCapacityPlan, plan.appendCold(wrong));
    wrong = reused;
    wrong.index = 5;
    wrong.geometry.?.index = 5;
    wrong.values.new_bound = 31;
    try std.testing.expectError(error.InvalidDeviceCapacityPlan, plan.appendCold(wrong));
    try std.testing.expectEqual(@as(u16, 2), plan.count);
    try std.testing.expectEqual(@as(usize, 216), plan.new_bytes);
}
