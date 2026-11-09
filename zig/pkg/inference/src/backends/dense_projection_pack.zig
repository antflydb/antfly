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

//! Checked contiguous pair/QKV weight and F32 bias geometry. Byte lengths
//! describe payload only; the accepted allocator supplies physical charges.
const std = @import("std");

pub const Layout = struct {
    width: usize,
    count: u8,
    widths: [3]usize = @splat(0),
    weight_offsets: [3]usize = @splat(0),
    bias_offsets: [3]usize = @splat(0),
    element_bytes: usize,
    total_width: usize = 0,
    weight_bytes: usize = 0,
    bias_bytes: usize = 0,

    pub fn init(width: usize, widths: []const usize, element_bytes: usize) !Layout {
        if (width == 0 or (widths.len != 2 and widths.len != 3) or (element_bytes != 2 and element_bytes != 4)) return error.InvalidDenseProjectionPack;
        var result = Layout{ .width = width, .count = @intCast(widths.len), .element_bytes = element_bytes };
        for (widths, 0..) |output, index| {
            if (output == 0) return error.InvalidDenseProjectionPack;
            result.widths[index] = output;
            result.weight_offsets[index] = result.weight_bytes;
            result.bias_offsets[index] = result.bias_bytes;
            result.total_width = try std.math.add(usize, result.total_width, output);
            const weight = try std.math.mul(usize, try std.math.mul(usize, width, output), element_bytes);
            const bias = try std.math.mul(usize, output, @sizeOf(f32));
            result.weight_bytes = try std.math.add(usize, result.weight_bytes, weight);
            result.bias_bytes = try std.math.add(usize, result.bias_bytes, bias);
        }
        return result;
    }

    pub fn outputBytes(self: Layout, rows: usize) !usize {
        if (rows == 0) return error.InvalidDenseProjectionPack;
        return std.math.mul(usize, try std.math.mul(usize, rows, self.total_width), @sizeOf(f32));
    }

    /// Two independent roots round independently. This is a requested lower
    /// bound only; it never replaces actual accepted-provider allocation size.
    pub fn roundedPayloadBytes(self: Layout, alignment: usize) !usize {
        if (alignment == 0 or !std.math.isPowerOfTwo(alignment)) return error.InvalidDenseProjectionPack;
        const weight = (try std.math.add(usize, self.weight_bytes, alignment - 1)) & ~(alignment - 1);
        const bias = (try std.math.add(usize, self.bias_bytes, alignment - 1)) & ~(alignment - 1);
        return std.math.add(usize, weight, bias);
    }
};

/// Copied source-version facts from the actual accepted native receiver.
/// No SDK pointer, Model, Domain or creation boundary enters this request.
pub const Request = struct {
    count: u8,
    slots: [3]usize,
    generations: [3]u64,
    declarations: [3]u64,
    width: usize,
    widths: [3]usize,

    pub fn layout(self: Request) !Layout {
        if ((self.count != 2 and self.count != 3) or self.width > std.math.maxInt(u32)) return error.InvalidDenseProjectionPack;
        if (self.count == 2 and self.widths[0] != self.widths[1]) return error.InvalidDenseProjectionPack;
        if (self.count == 3 and self.widths[1] != self.widths[2]) return error.InvalidDenseProjectionPack;
        for (0..self.count) |index| {
            if (self.generations[index] == 0 or self.declarations[index] == 0 or (self.declarations[index] & 1) != 0 or self.widths[index] > std.math.maxInt(u32)) return error.InvalidDenseProjectionPack;
            for (self.slots[0..index]) |slot| if (slot == self.slots[index]) return error.InvalidDenseProjectionPack;
        }
        const result = try Layout.init(self.width, self.widths[0..self.count], @sizeOf(f32));
        if (result.total_width > std.math.maxInt(u32)) return error.InvalidDenseProjectionPack;
        return result;
    }
};

/// One independently charged immutable native packed entry. The actual C
/// entry owns both SDK roots. The shared Charge's witnesses own the SAME cache
/// token until BOTH SDK roots die, independently of provider/model lifetime.
pub const Owner = struct {
    const retirement = @import("../execution/resource_retirement.zig");
    const Cleanup = @import("../execution/cleanup_context.zig").Context;
    const operation_owner = @import("../execution/operation_owner.zig");
    const Ticket = @import("../execution/operation_gate.zig").Gate.Ticket;
    const Epoch = @import("../execution/retirement_sink.zig").Epoch;
    const Use = struct {
        owner: *Owner = undefined,
        node: retirement.Node = undefined,
        ticket: ?Ticket = null,
        storage: ?operation_owner.Owner.StoragePin = null,
        epoch: ?Epoch = null,
        fn ended(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const owner = self.owner;
            owner.lock();
            var ticket = self.ticket.?;
            var storage = self.storage.?;
            var epoch = self.epoch.?;
            // Slot reusable only after its actual old ticket is released.
            // Capture ALL locals: a subsequent operation may rewrite this cell.
            self.ticket = null;
            self.storage = null;
            self.epoch = null;
            owner.mutex.unlock();
            ticket.release() catch @panic("packed operation ticket release failed");
            storage.release();
            epoch.release();
            owner.releaseWithCleanup(.{ .outside_execution = true }) catch @panic("packed use retirement failed");
        }
    };
    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    references: usize = 1,
    final_boundary_prepared: bool = false,
    entry: *anyopaque,
    charge: *retirement.Charge,
    release_entry: *const fn (*anyopaque) callconv(.c) void,
    uses: [@import("../execution/operation_gate.zig").Gate.capacity]Use = @splat(.{}),

    /// Requested project metadata only. The two SDK buffer contents are
    /// separately charged as backend payload; allocator/vendor overhead is not
    /// represented as an invented exact count here.
    pub fn requestedMetadataBytes() !usize {
        if (comptime !@import("build_options").enable_metal) return error.UnsupportedBackend;
        const Native = struct {
            extern fn antfly_metal_retirement_arena_requested_bytes(usize, usize) usize;
        };
        // BackendPreparation uses three potential backing views per output.
        const arena = Native.antfly_metal_retirement_arena_requested_bytes(6, 1);
        if (arena == 0) return error.InvalidDenseProjectionPack;
        return std.math.add(usize, @sizeOf(Owner), try std.math.add(usize, try retirement.PreparedCharge.requestedMetadataBytes(1), arena));
    }

    /// All Charge/Group/native witness metadata is prepared before SDK roots.
    pub fn prepareCharge() !retirement.PreparedCharge {
        return retirement.PreparedCharge.prepareForBackend(.metal, 2, 1);
    }
    /// The accepted concrete receiver supplies the exact independently owned
    /// SDK roots; this service never derives ownership from a pointer value.
    /// Attachment failure may retain partial death witnesses conservatively.
    pub fn attachSDKRoots(charge: *retirement.Charge, roots: [2]*anyopaque) !void {
        if (roots[0] == roots[1]) return error.InvalidDenseProjectionPack;
        const Attach = struct {
            roots: [2]*anyopaque,
            fn begin(_: *anyopaque, _: []const @import("../ops/ops.zig").CT) !retirement.Scope.Begun {
                return error.UnpreparedPackedProjectionScope;
            }
            fn unsupported(_: *anyopaque, _: []const @import("../ops/ops.zig").CT, _: []retirement.Node, _: retirement.Payload) !void {
                return error.UnpreparedPackedProjectionWitnesses;
            }
            fn attach(raw: *anyopaque, _: []const @import("../ops/ops.zig").CT, nodes: []retirement.Node, payload: retirement.Payload, prepared: *retirement.BackendPreparation) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (nodes.len != 0) return error.InvalidDenseProjectionPack;
                const witnesses = try prepared.armMetal(null, &self.roots, payload);
                if (@import("metal_runtime.zig").antfly_metal_buffer_retirement_attach(witnesses) != 0) return error.PackedProjectionWitnessAttachmentFailed;
            }
            fn end(_: *anyopaque) void {}
            const table = retirement.Scope.VTable{ .begin = begin, .attach = unsupported, .attach_prepared = attach, .end = end, .notify = retirement.pollRetirementChargesCPU };
        };
        var receiver = Attach{ .roots = roots };
        var scope = retirement.Scope{ .context = &receiver, .origin = retirement.Queue.init(), .outputs = &.{}, .vtable = &Attach.table, .active = true };
        defer scope.end();
        try charge.attach(&scope);
    }

    /// Caller prepares this final metadata address BEFORE SDK work. A fully
    /// attached Charge and exact native entry transfer only on success.
    pub fn init(self: *Owner, allocator: std.mem.Allocator, entry: *anyopaque, charge: *retirement.Charge, release_entry: *const fn (*anyopaque) callconv(.c) void) void {
        self.* = .{ .allocator = allocator, .entry = entry, .charge = charge, .release_entry = release_entry };
        for (&self.uses) |*use| {
            use.owner = self;
            use.node = .{ .context = use, .release_fn = Use.ended };
        }
    }
    fn lock(self: *Owner) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    pub fn retain(self: *Owner) *Owner {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.references != 0 and self.references != std.math.maxInt(usize));
        self.references += 1;
        return self;
    }
    /// The native compatibility cache supplies the exact CURRENT entered
    /// operation, never an ambient epoch or this entry's creation boundary.
    /// One fixed cell per retained real Gate ticket; no warm allocations.
    pub fn noteUse(self: *Owner, record: *operation_owner.Owner.Record, nonce: u64) !void {
        var epoch = (try record.retainCleanupEpoch(nonce)) orelse return error.BackendOperationCleanupUnavailable;
        var epoch_moved = false;
        defer if (!epoch_moved) epoch.release();
        var storage = try record.retainStorage(nonce);
        var storage_moved = false;
        defer if (!storage_moved) storage.release();
        if (!storage.owner.?.hasStorageBacking()) return error.UnfundedBackendOperationStorage;
        const snapshot = try record.snapshot(nonce);
        if (!snapshot.entered or !try snapshot.ticket.ownsGate()) return error.BackendOperationInactive;
        var ticket = try snapshot.ticket.retain();
        var ticket_moved = false;
        defer if (!ticket_moved) ticket.release() catch unreachable;
        self.lock();
        defer self.mutex.unlock();
        const use = &self.uses[ticket.index];
        if (use.ticket) |existing| {
            const existing_epoch = use.epoch.?;
            if (existing.owner != ticket.owner or existing.nonce != nonce or existing_epoch.record != epoch.record or existing_epoch.nonce != epoch.nonce) return error.PackedProjectionUseChanged;
            return;
        }
        try self.charge.validateCacheCleanup(.{ .boundary = &epoch });
        const boundary = try epoch.boundaryBeforeSeal();
        use.ticket = ticket;
        use.storage = storage;
        use.epoch = epoch;
        errdefer {
            use.ticket = null;
            use.storage = null;
            use.epoch = null;
        }
        std.debug.assert(self.references != 0 and self.references != std.math.maxInt(usize));
        self.references += 1;
        boundary.register(boundary.context, boundary.nonce, &use.node) catch |err| {
            self.references -= 1;
            return err;
        };
        epoch_moved = true;
        storage_moved = true;
        ticket_moved = true;
    }
    /// Terminal cache/provider retirement preflights its actual B BEFORE
    /// clearing either charged SDK alias, even when old per-use references make
    /// creator release nonfinal. Caller excludes all future cache use/creation.
    /// Repeated preflight of this same terminal retirement consumes no ticket.
    pub fn prepareFinalRetirement(self: *Owner, current: Cleanup) !void {
        self.lock();
        defer self.mutex.unlock();
        try self.prepareFinalRetirementLocked(current);
    }
    fn prepareFinalRetirementLocked(self: *Owner, current: Cleanup) !void {
        try self.charge.validateCacheCleanup(current);
        if (self.final_boundary_prepared) return;
        if (current.boundary) |epoch| {
            const boundary = try epoch.boundaryBeforeSeal();
            try self.charge.holdUntilBoundary(boundary);
        }
        self.final_boundary_prepared = true;
    }
    /// Preflight the actual last owner before any native/SDK release. Every
    /// recoverable error leaves the caller's reference and both roots intact.
    pub fn releaseWithCleanup(self: *Owner, current: Cleanup) !void {
        self.lock();
        std.debug.assert(self.references != 0);
        if (self.references != 1) {
            self.references -= 1;
            self.mutex.unlock();
            return;
        }
        // Pure current-B validation/registration only. Its final callbacks
        // run after the operation record unlocks; no SDK/supplier under mutex.
        self.prepareFinalRetirementLocked(current) catch |err| {
            self.mutex.unlock();
            return err;
        };
        self.references = 0;
        self.mutex.unlock();
        const a = self.allocator;
        const entry = self.entry;
        const charge = self.charge;
        const release_entry = self.release_entry;
        a.destroy(self);
        // SDK deallocation witnesses only signal existing Group atomics.
        // Even immediate death cannot refund under the still-live current B.
        release_entry(entry);
        charge.release();
    }
};

test "dense projection pack preserves asymmetric QKV and pair weight and bias order" {
    const qkv = try Layout.init(64, &.{ 128, 32, 32 }, 2);
    try std.testing.expectEqualSlices(usize, &.{ 0, 16384, 20480 }, &qkv.weight_offsets);
    try std.testing.expectEqualSlices(usize, &.{ 0, 512, 640 }, &qkv.bias_offsets);
    try std.testing.expectEqual(@as(usize, 24576), qkv.weight_bytes);
    try std.testing.expectEqual(@as(usize, 768), qkv.bias_bytes);
    try std.testing.expectEqual(@as(usize, 98304), try qkv.outputBytes(128));
    try std.testing.expectEqual(@as(usize, 49152), try qkv.roundedPayloadBytes(16384));
    const pair = try Layout.init(32, &.{ 64, 64 }, 4);
    try std.testing.expectEqual(@as(usize, 16384), pair.weight_bytes);
    try std.testing.expectEqual(@as(usize, 512), pair.bias_bytes);
    try std.testing.expectEqual(@as(usize, 1536), try pair.outputBytes(3));
    try std.testing.expectEqual(@as(usize, 32768), try pair.roundedPayloadBytes(16384));
}

test "dense projection pack rejects malformed geometry and every checked byte overflow" {
    try std.testing.expectError(error.InvalidDenseProjectionPack, Layout.init(0, &.{ 1, 1 }, 4));
    try std.testing.expectError(error.InvalidDenseProjectionPack, Layout.init(1, &.{1}, 4));
    try std.testing.expectError(error.InvalidDenseProjectionPack, Layout.init(1, &.{ 1, 0 }, 4));
    try std.testing.expectError(error.InvalidDenseProjectionPack, Layout.init(1, &.{ 1, 1 }, 1));
    try std.testing.expectError(error.Overflow, Layout.init(std.math.maxInt(usize), &.{ 1, 1 }, 4));
    try std.testing.expectError(error.Overflow, Layout.init(1, &.{ std.math.maxInt(usize), 1 }, 2));
    const pair = try Layout.init(1, &.{ 1, 1 }, 4);
    try std.testing.expectError(error.Overflow, pair.outputBytes(std.math.maxInt(usize)));
    try std.testing.expectError(error.InvalidDenseProjectionPack, pair.roundedPayloadBytes(3));
    try std.testing.expectError(error.Overflow, pair.roundedPayloadBytes(@as(usize, 1) << (@bitSizeOf(usize) - 1)));
}

test "packed immutable owner waits both payload witnesses and current B without source quota credit" {
    const cache_mod = @import("../runtime/tier/cache.zig");
    const admission = @import("../runtime/tier/admission_domain.zig");
    const retirement = @import("../execution/resource_retirement.zig");
    const Sink = @import("../execution/retirement_sink.zig");
    const a = std.testing.allocator;
    const domain = try admission.Domain.create();
    defer domain.release();
    const pin = domain.pin();
    defer pin.release();
    var cache = cache_mod.SharedCache.init(.{ .backend_limit_bytes = 32 });
    defer cache.deinitAdmission();
    try cache.configureManagedAdmissionOutsideExecution(pin.controller(), .gpu, .{ .backend_limit_bytes = 32 }, .{ .backend_limit_bytes = 64 });
    try cache.installRetirementDomainOutsideExecution(&pin);
    var growth = try cache.snapshotPhysicalGrowthLocked(16, 16, 32);
    defer growth.finishOutsideExecution();
    try growth.fundOutsideExecution(&pin);
    try growth.validatePhysicalBacking();
    try growth.commit();
    var backing: ?[2]cache_mod.PhysicalBacking = growth.retainCommittedPhysicalBackings();
    var shell = try retirement.PreparedCharge.prepare(1);
    defer shell.destroyUnused();
    const charge = try shell.armCacheBackings(&backing);
    var charge_live = true;
    errdefer if (charge_live) charge.release();
    const Probe = struct {
        payload: ?retirement.Payload = null,
        native_releases: usize = 0,
        fn begin(_: *anyopaque, _: []const @import("../ops/ops.zig").CT) !retirement.Scope.Begun {
            return .{ .origin = .{ .identity = 123 }, .pending_batches = 0 };
        }
        fn attach(raw: *anyopaque, _: []const @import("../ops/ops.zig").CT, nodes: []retirement.Node, payload: retirement.Payload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 0), nodes.len);
            payload.retain_fn(payload.context); // actual weight witness analogue
            payload.retain_fn(payload.context); // actual bias witness analogue
            self.payload = payload;
        }
        fn end(_: *anyopaque) void {}
        fn releaseEntry(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.native_releases += 1;
        }
        const table = retirement.Scope.VTable{ .begin = begin, .attach = attach, .end = end, .notify = retirement.pollRetirementChargesCPU };
    };
    var probe: Probe = .{};
    var scope = retirement.Scope{ .context = &probe, .origin = .{ .identity = 123 }, .outputs = &.{}, .vtable = &Probe.table, .active = true };
    defer scope.end();
    try charge.attach(&scope);
    var witness_count: usize = 2;
    defer while (witness_count != 0) {
        const payload = probe.payload.?;
        payload.release_fn(payload.context);
        witness_count -= 1;
        retirement.pollRetirementChargesCPU();
    };
    const owner = try a.create(Owner);
    owner.init(a, &probe, charge, Probe.releaseEntry);
    charge_live = false;
    var owner_live = true;
    defer if (owner_live) owner.releaseWithCleanup(.{ .outside_execution = true }) catch unreachable;
    try std.testing.expectError(error.CacheRetirementCleanupAuthorityRequired, owner.releaseWithCleanup(.{}));
    try std.testing.expectEqual(@as(usize, 0), probe.native_releases);
    cache.noteLogicalPhysicalOwnerRemovalForTier(.backend, 16);
    var sink = try Sink.Ref.create();
    defer sink.release();
    var guard = try sink.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    try cache.deinitAdmissionWithCleanup(.{ .boundary = &epoch });
    try owner.releaseWithCleanup(.{ .boundary = &epoch });
    owner_live = false;
    try std.testing.expectEqual(@as(usize, 1), probe.native_releases);
    const payload = probe.payload.?;
    payload.release_fn(payload.context);
    witness_count -= 1;
    retirement.pollRetirementChargesCPU();
    try std.testing.expectEqual(@as(usize, 32), pin.controller().snapshot().backend_weight_bytes);
    payload.release_fn(payload.context);
    witness_count -= 1;
    retirement.pollRetirementChargesCPU();
    try std.testing.expectEqual(@as(usize, 32), pin.controller().snapshot().backend_weight_bytes);
    guard.finish();
    retirement.pollRetirementChargesCPU();
    try std.testing.expectEqualDeep(@import("../runtime/tier/memory.zig").AdmissionAmounts{}, pin.controller().snapshot());
}

test "native packed request rejects mismatched slot groups and u32 narrowing before any provider callback" {
    const good = Request{ .count = 3, .slots = .{ 1, 2, 3 }, .generations = .{ 1, 2, 3 }, .declarations = .{ 2, 4, 6 }, .width = 64, .widths = .{ 128, 32, 32 } };
    try std.testing.expectEqual(@as(usize, 192), (try good.layout()).total_width);
    for (0..5) |changed| {
        var value = good;
        switch (changed) {
            0 => value.slots[1] = 1,
            1 => value.declarations[1] = 3,
            2 => value.widths[2] = 64,
            3 => value.generations[2] = 0,
            4 => value.width = std.math.add(usize, std.math.maxInt(u32), 1) catch continue,
            else => unreachable,
        }
        try std.testing.expectError(error.InvalidDenseProjectionPack, value.layout());
    }
}

test "packed reuse retains actual B1 and B2 tickets and separately charged operation storage through close" {
    const cache_mod = @import("../runtime/tier/cache.zig");
    const admission = @import("../runtime/tier/admission_domain.zig");
    const retirement = @import("../execution/resource_retirement.zig");
    const Sink = @import("../execution/retirement_sink.zig");
    const a = std.testing.allocator;
    const domain = try admission.Domain.create();
    defer domain.release();
    const pin = domain.pin();
    defer pin.release();
    var cache = cache_mod.SharedCache.init(.{ .backend_limit_bytes = 32 });
    defer cache.deinitAdmission();
    try cache.configureManagedAdmissionOutsideExecution(pin.controller(), .gpu, .{ .backend_limit_bytes = 32 }, .{ .backend_limit_bytes = 64 });
    try cache.installRetirementDomainOutsideExecution(&pin);
    var growth = try cache.snapshotPhysicalGrowthLocked(16 + @import("../execution/operation_owner.zig").Owner.retainedStorageBytes(), 16, 32);
    defer growth.finishOutsideExecution();
    try growth.fundOutsideExecution(&pin);
    try growth.validatePhysicalBacking();
    try growth.commit();
    var backing: ?[2]cache_mod.PhysicalBacking = growth.retainCommittedPhysicalBackings();
    defer if (backing) |parts| for (parts) |part| part.releaseAfterMetadataDeathOutsideExecution();
    const operation_owner = @import("../execution/operation_owner.zig");
    const operations = try operation_owner.Owner.createRetained(a);
    var operations_live = true;
    defer if (operations_live) {
        operations.closeRetained() catch unreachable;
        operations.releaseClosedStorage();
    };
    var storage_part: ?cache_mod.PhysicalBacking = try backing.?[0].takePart(operation_owner.Owner.retainedStorageBytes());
    defer if (storage_part) |part| part.releaseAfterMetadataDeathOutsideExecution();
    try operations.attachStorageBacking(&storage_part);
    try std.testing.expect(operations.hasStorageBacking());
    var shell = try retirement.PreparedCharge.prepare(1);
    defer shell.destroyUnused();
    const charge = try shell.armCacheBackings(&backing);
    var charge_live = true;
    errdefer if (charge_live) charge.release();
    const Probe = struct {
        payload: ?retirement.Payload = null,
        native_releases: usize = 0,
        fn begin(_: *anyopaque, _: []const @import("../ops/ops.zig").CT) !retirement.Scope.Begun {
            return .{ .origin = .{ .identity = 123 }, .pending_batches = 0 };
        }
        fn attach(raw: *anyopaque, _: []const @import("../ops/ops.zig").CT, nodes: []retirement.Node, payload: retirement.Payload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 0), nodes.len);
            payload.retain_fn(payload.context); // actual weight witness analogue
            payload.retain_fn(payload.context); // actual bias witness analogue
            self.payload = payload;
        }
        fn end(_: *anyopaque) void {}
        fn releaseEntry(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.native_releases += 1;
        }
        const table = retirement.Scope.VTable{ .begin = begin, .attach = attach, .end = end, .notify = retirement.pollRetirementChargesCPU };
    };
    var probe: Probe = .{};
    var scope = retirement.Scope{ .context = &probe, .origin = .{ .identity = 123 }, .outputs = &.{}, .vtable = &Probe.table, .active = true };
    defer scope.end();
    try charge.attach(&scope);
    var witness_count: usize = 2;
    defer while (witness_count != 0) {
        const payload = probe.payload.?;
        payload.release_fn(payload.context);
        witness_count -= 1;
        retirement.pollRetirementChargesCPU();
    };
    const owner = try a.create(Owner);
    owner.init(a, &probe, charge, Probe.releaseEntry);
    charge_live = false;
    var owner_live = true;
    defer if (owner_live) owner.releaseWithCleanup(.{ .outside_execution = true }) catch unreachable;
    try std.testing.expectError(error.CacheRetirementCleanupAuthorityRequired, owner.releaseWithCleanup(.{}));
    try std.testing.expectEqual(@as(usize, 0), probe.native_releases);
    var sink = try Sink.Ref.create();
    defer sink.release();
    var b1 = try sink.enter();
    defer b1.finish();
    var b2 = try sink.enter();
    defer b2.finish();
    var close_b = try sink.enter();
    defer close_b.finish();
    var close_epoch = try close_b.retainEpoch();
    defer close_epoch.release();
    var e1 = try b1.retainEpoch();
    defer e1.release();
    var e2 = try b2.retainEpoch();
    defer e2.release();
    var marker: u8 = 0;
    const request = @import("../execution/backend_operation.zig").Request{ .implementation = .{ .context = &marker, .table = &marker }, .program = null, .mode = .managed };
    const first = try operations.prepare(&marker, request);
    var first_live = true;
    defer if (first_live) first.record.finishAndRelease(first.nonce) catch unreachable;
    try (try first.record.snapshot(first.nonce)).ticket.begin();
    try first.record.setEntered(first.nonce, true);
    try first.record.bindCleanupEpoch(first.nonce, &e1);
    try owner.noteUse(first.record, first.nonce);
    const after_first = owner.references;
    try owner.noteUse(first.record, first.nonce);
    try std.testing.expectEqual(after_first, owner.references);
    try std.testing.expectError(error.BackendOperationConsumed, owner.noteUse(first.record, first.nonce + 1));
    try first.record.finishAndRelease(first.nonce);
    first_live = false;
    const second = try operations.prepare(&marker, request);
    var second_live = true;
    defer if (second_live) second.record.finishAndRelease(second.nonce) catch unreachable;
    try std.testing.expect(first.record != second.record);
    try (try second.record.snapshot(second.nonce)).ticket.begin();
    try second.record.setEntered(second.nonce, true);
    try second.record.bindCleanupEpoch(second.nonce, &e2);
    try owner.noteUse(second.record, second.nonce);
    try second.record.finishAndRelease(second.nonce);
    second_live = false;
    // Provider may close before either outer B ends. SDK completion alone is
    // insufficient: live per-use custody and the real record storage remain.
    try operations.holdStorageUntilCleanup(.{ .boundary = &close_epoch });
    try operations.closeRetained();
    operations.releaseClosedStorage();
    operations_live = false;
    cache.noteLogicalPhysicalOwnerRemovalForTier(.backend, 16);
    try cache.deinitAdmissionWithCleanup(.{ .boundary = &close_epoch });
    try owner.prepareFinalRetirement(.{ .boundary = &close_epoch });
    try owner.prepareFinalRetirement(.{ .boundary = &close_epoch });
    try std.testing.expect(owner.final_boundary_prepared);
    try owner.releaseWithCleanup(.{ .boundary = &close_epoch });
    owner_live = false;
    const payload = probe.payload.?;
    while (witness_count != 0) {
        payload.release_fn(payload.context);
        witness_count -= 1;
    }
    retirement.pollRetirementChargesCPU();
    try std.testing.expectEqual(@as(usize, 0), probe.native_releases);
    try std.testing.expectEqual(@as(usize, 32), pin.controller().snapshot().backend_weight_bytes);
    try std.testing.expect(pin.controller().snapshot().host_scratch_bytes != 0 or pin.controller().snapshot().host_weight_bytes != 0);
    b1.finish();
    retirement.pollRetirementChargesCPU();
    try std.testing.expectEqual(@as(usize, 0), probe.native_releases);
    try std.testing.expectEqual(@as(usize, 32), pin.controller().snapshot().backend_weight_bytes);
    b2.finish();
    retirement.pollRetirementChargesCPU();
    try std.testing.expectEqual(@as(usize, 1), probe.native_releases);
    // Both reusable use cells and their Owner are now gone. The independently
    // registered FINAL close B still forbids quota refund, even though the two
    // SDK death witnesses completed earlier and no per-use node names close B.
    try std.testing.expectEqual(@as(usize, 32), pin.controller().snapshot().backend_weight_bytes);
    close_b.finish();
    retirement.pollRetirementChargesCPU();
    try std.testing.expectEqualDeep(@import("../runtime/tier/memory.zig").AdmissionAmounts{}, pin.controller().snapshot());
}
