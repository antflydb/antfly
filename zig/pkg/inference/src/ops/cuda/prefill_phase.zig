// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Synchronous pre-forward preparation of real optional DSO storage. Only
//! value metadata lives on the worker stack. No phase pointer/creation B is
//! published into a Module, graph, state, cache or another queue.
const std = @import("std");
const platform = @import("antfly_platform");
const metadata = @import("prefill_geometry.zig");
const funding_mod = @import("prepared_cache.zig");
const quota_mod = @import("cache_quota.zig");
const retirement = @import("../../execution/resource_retirement.zig");
const Cleanup = @import("../../execution/cleanup_context.zig").Context;
const Model = @import("../../backends/session.zig").Session.ModelLifetime;
pub const Query = *const fn (*const metadata.Geometry) callconv(.c) usize;
pub const Descriptor = struct {
    packets: [2]metadata.Geometry = undefined,
    packet_count: usize = 0,
    attention: bool = false,
    pub fn init(key_heads: usize, value_heads: usize, lengths: []const usize, recurrent: bool, attention: bool) !Descriptor {
        var value: Descriptor = .{ .attention = attention };
        var prefixes = false;
        for (lengths) |length| prefixes = prefixes or length > 1;
        if (recurrent and prefixes and lengths.len >= 2) {
            if (lengths.len > 128) return error.InvalidTensorShape;
            var rows: usize = 0;
            for (lengths) |length| {
                if (length == 0 or length > 4096 - rows) return error.InvalidTensorShape;
                rows += length;
            }
            var first: usize = 0;
            while (first < lengths.len) : (first += 64) {
                const packet = try metadata.Geometry.fromLengths(key_heads, value_heads, lengths[first..@min(first + 64, lengths.len)]);
                value.packets[value.packet_count] = packet;
                value.packet_count += 1;
            }
        }
        return value;
    }
    pub fn hash(self: *const Descriptor) u64 {
        var hash_value = std.hash.Wyhash.init(0);
        for (self.packets[0..self.packet_count]) |*packet| hash_value.update(std.mem.asBytes(packet));
        hash_value.update(&.{@intFromBool(self.attention)});
        return hash_value.final();
    }
};
pub const Snapshot = struct {
    recurrent_revision: u64,
    recurrent_ptr: u64,
    recurrent_bytes: usize,
    recurrent_quota: quota_mod.Quota,
    attention_revision: u64 = 0,
    attention_device: u64 = 0,
    attention_host: usize = 0,
    attention_quota: quota_mod.Quota = .none,
    library_identity: usize,
    query: ?Query,
};
const Cold = struct {
    allocator: std.mem.Allocator,
    node: retirement.Node = undefined,
    funding: ?*funding_mod.Funding = null,
    upgrades: [2]?quota_mod.RawUpgrade = @splat(null),
    fn destroyOutside(self: *Cold) void {
        if (self.funding) |value| value.finishOutsideExecution();
        for (&self.upgrades) |*value| if (value.*) |*upgrade| upgrade.deinitOutsideExecution();
        self.allocator.destroy(self);
    }
    fn retire(raw: *anyopaque) callconv(.c) void {
        const self: *Cold = @ptrCast(@alignCast(raw));
        self.destroyOutside();
    }
};
pub const Phase = struct {
    descriptor: Descriptor,
    expected_compute: usize = 0,
    old: Snapshot = undefined,
    admission: ?@import("video_cache_budget.zig").Admission = null,
    mode: enum { none, query, upgrade, fund } = .none,
    queried: bool = false,
    recurrent_bytes: usize = 0,
    packet_bytes: [2]usize = @splat(0),
    declined: bool = false,
    requests: [2]funding_mod.Request = undefined,
    request_count: usize = 0,
    cold: ?*Cold = null,
    bound: bool = false,
    pub fn initInto(self: *Phase, descriptor: Descriptor) !void {
        if (descriptor.packet_count > 2) return error.InvalidTensorShape;
        for (descriptor.packets[0..descriptor.packet_count]) |*packet| try packet.validate();
        self.* = .{ .descriptor = descriptor };
    }
    pub fn prepareOutside(self: *Phase, model: *const Model) !void {
        return self.prepareOutsideUsing(model, platform.allocator.processAllocator(platform.allocator.concurrentFallback()));
    }
    fn prepareOutsideUsing(self: *Phase, model: *const Model, a: std.mem.Allocator) !void {
        if (self.cold != null or self.bound or self.mode == .none) return error.InvalidPreparedCacheState;
        if (self.mode == .query) {
            const query = self.old.query orelse return error.RecurrentPrefillColdGeometryUnavailable;
            var bytes: usize = 0;
            for (self.descriptor.packets[0..self.descriptor.packet_count], 0..) |*packet, index| {
                const result = query(packet);
                if (result == std.math.maxInt(usize)) return error.InvalidTensorShape;
                self.packet_bytes[index] = result;
                bytes = @max(bytes, result);
            }
            self.recurrent_bytes = bytes;
            self.queried = true;
            return;
        }
        const cold = try a.create(Cold);
        cold.* = .{ .allocator = a };
        errdefer cold.destroyOutside();
        if (self.mode == .upgrade) {
            const values = [_]quota_mod.Quota{ self.old.recurrent_quota, self.old.attention_quota };
            for (values, 0..) |quota, index| if (quota == .raw) {
                const controller = quota.raw.controller orelse return error.MissingCacheRetirementDomain;
                const pin = model.retainAdmissionDomain(controller) orelse return error.MissingCacheRetirementDomain;
                defer pin.release();
                cold.upgrades[index] = try quota_mod.RawUpgrade.prepare(quota.raw, &pin);
            };
        } else {
            if (self.mode != .fund or self.request_count == 0) return error.InvalidPreparedCacheState;
            const funding = try funding_mod.Funding.createWithAllocator(a, .{ .requests = self.requests[0..self.request_count] });
            cold.funding = funding;
            for (funding.entries, 0..) |entry, index| try funding.noteMiss(index, entry.request.external_prefill.amounts(), 0);
            funding.admission = self.admission;
            funding.compute_identity = self.expected_compute;
            funding.state = .snapped;
            var pin: ?@import("../../runtime/tier/admission_domain.zig").Pin = null;
            if (self.admission) |policy| pin = model.retainAdmissionDomain(policy.controller) orelse return error.MissingCacheRetirementDomain;
            defer if (pin) |value| value.release();
            try funding.fundOutsideExecution(if (pin) |*value| value else null);
        }
        self.cold = cold;
    }
    pub fn preparedFunding(self: *Phase) !*funding_mod.Funding {
        return (self.cold orelse return error.InvalidPreparedCacheState).funding orelse return error.InvalidPreparedCacheState;
    }
    pub fn installUpgrades(self: *Phase, recurrent: *quota_mod.Quota, attention: ?*quota_mod.Quota) !void {
        if (self.mode != .upgrade) return error.InvalidPreparedCacheState;
        const cold = self.cold orelse return error.InvalidPreparedCacheState;
        if (cold.upgrades[0]) |*value| _ = try value.install(recurrent);
        if (cold.upgrades[1]) |*value| _ = try value.install(attention orelse return error.PreparedCachePlanChanged);
    }
    pub fn bindFunding(self: *Phase, current: Cleanup) !void {
        const epoch = current.boundary orelse return error.MissingCurrentCleanupEpoch;
        if (current.outside_execution or self.bound or try epoch.isSealed()) return error.InvalidPreparedCacheState;
        try (try self.preparedFunding()).bind(epoch);
        self.bound = true;
    }
    /// Use after all first-birth tokens are published, BEFORE any model math.
    /// The preallocated heap node may outlive this stack and owns no SDK/Model.
    pub fn close(self: *Phase, current: Cleanup) !void {
        const cold = self.cold orelse return;
        const epoch = current.boundary orelse return error.MissingCurrentCleanupEpoch;
        if (current.outside_execution or try epoch.isSealed()) return error.InvalidPreparedCacheState;
        if (self.bound) {
            (try self.preparedFunding()).unbind();
            self.bound = false;
        }
        cold.node = .{ .context = cold, .release_fn = Cold.retire };
        const boundary = try epoch.boundary();
        self.cold = null;
        errdefer self.cold = cold;
        try boundary.register(boundary.context, boundary.nonce, &cold.node);
    }
    pub fn destroyOutside(self: *Phase) void {
        std.debug.assert(!self.bound);
        if (self.cold) |value| {
            self.cold = null;
            value.destroyOutside();
        }
    }
};

fn prefillColdFundingOom136(a: std.mem.Allocator) !void {
    const domain_mod = @import("../../runtime/tier/admission_domain.zig");
    const memory = @import("../../runtime/tier/memory.zig");
    const Sink = @import("../../execution/retirement_sink.zig");
    const owned = try domain_mod.Domain.create();
    defer owned.release();
    const External = struct {
        active: bool = false,
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(!self.active);
            self.active = true;
            return 1;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.active);
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.active);
            self.active = false;
        }
    };
    var external: External = .{};
    try owned.controller.configureResourceBudget(.{ .context = &external, .try_reserve = External.reserve, .retain = External.retain, .release = External.release });
    defer owned.controller.configureResourceBudget(null) catch @panic("prefill external token leak");
    const Noop = struct {
        fn close(_: *anyopaque) void {}
    };
    var marker: u8 = 0;
    var model: Model = .{ .owner = &marker, .destroy_fn = Noop.close, .admission_domain = owned.pin() };
    defer model.release();
    defer std.testing.expectEqualDeep(memory.AdmissionAmounts{}, owned.controller.snapshot()) catch @panic("prefill cold OOM quota leak");
    var phase: Phase = undefined;
    try phase.initInto(.{});
    defer phase.destroyOutside();
    phase.mode = .fund;
    phase.expected_compute = 0x1000;
    phase.admission = .{ .controller = &owned.controller, .limits = .{ .host_limit_bytes = 32, .backend_limit_bytes = 192, .combined_limit_bytes = 224 }, .check_live_memory = false };
    phase.request_count = 2;
    phase.requests[0] = .{ .external_prefill = .{ .kind = .recurrent, .library_identity = 1, .geometry_hash = 3, .device_bytes = 64 } };
    phase.requests[1] = .{ .external_prefill = .{ .kind = .attention, .library_identity = 1, .geometry_hash = 3, .device_bytes = 128, .host_bytes = 32 } };
    // Exact own two-class peak. Intrinsic one-byte denial leaves no partial
    // published SDK/token/Cold owner and cannot borrow the run's floors.
    phase.admission.?.limits.combined_limit_bytes = 223;
    phase.prepareOutsideUsing(&model, a) catch |err| {
        if (err == error.OutOfMemory) return err;
        try std.testing.expectEqual(error.ResourceLimitExceeded, err);
    };
    try std.testing.expect(phase.cold == null);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, owned.controller.snapshot());
    phase.admission.?.limits.combined_limit_bytes = 224;
    try phase.prepareOutsideUsing(&model, a);
    var reference = try Sink.Ref.create();
    defer reference.release();
    var publication = try reference.enter();
    defer publication.finish();
    var publication_b = try publication.retainEpoch();
    defer publication_b.release();
    try phase.bindFunding(.{ .boundary = &publication_b });
    var quotas: [2]quota_mod.Quota = @splat(.none);
    var quota_live: [2]bool = @splat(false);
    defer for (&quotas, quota_live) |*quota, live| if (live) quota.releaseOutsideExecution();
    for (phase.requests[0..2], &quotas, &quota_live) |request, *quota, *live| {
        var token = try (try phase.preparedFunding()).take(request, request.external_prefill.amounts());
        quota.* = token.publishQuota(); // pure quota test, no physical SDK claim
        live.* = true;
    }
    try phase.close(.{ .boundary = &publication_b });
    publication.finish();
    try std.testing.expectEqual(@as(usize, 192), owned.controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 32), owned.controller.snapshot().host_scratch_bytes);
    var consumer = try reference.enter();
    defer consumer.finish();
    var current_b = try consumer.retainEpoch();
    defer current_b.release();
    try std.testing.expect(publication_b.record != current_b.record or publication_b.nonce != current_b.nonce);
    for (&quotas, &quota_live) |*quota, *live| {
        try quota.releaseAfterSdkDeath(.{ .boundary = &current_b });
        live.* = false;
    }
    try std.testing.expectEqual(@as(usize, 192), owned.controller.snapshot().backend_scratch_bytes);
    consumer.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, owned.controller.snapshot());
    try std.testing.expect(!external.active);
}

test "optional prefill exact cold full peak and actual publication A versus final B preserve independent quota classes" {
    try prefillColdFundingOom136(std.testing.allocator);
}

test "optional prefill cold metadata and external token partition failure unwind owns every allocation" {
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, prefillColdFundingOom136, .{});
}
