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

//! Initial native history policy provisioning before hosted transaction admission.
//! Existing administrative policies are preserved. Initial configuration is an
//! exact CAS with no consumer, ACK, expiry refresh or source checkpoint effect.
const std = @import("std");
const local = @import("antfly_local_sources");
const metadata = @import("../metadata/api.zig");
const controls = @import("history_control_operations.zig");
const owners = @import("history_owner_operations.zig");
const Context = local.api_operation.RequestContext;
const A = std.mem.Allocator;
pub const Guard = struct {
    ptr: *anyopaque,
    check_fn: *const fn (*anyopaque, Context) anyerror!void,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    const VTable = struct { check: @FieldType(Guard, "check_fn") };
    const Boundary = local.runtime_callback_abi.Boundary(VTable);
    fn check(self: Guard, context: Context) !void {
        try context.ensureActive();
        try Boundary.call("check", self.boundary_dispatch, self.check_fn, .{ self.ptr, context });
        try context.ensureActive();
    }
};
/// Shared system authority for retention and future feed factories. Excludes
/// names, schema/index generations, leader terms and per-feed authorization.
pub fn authority(fence: metadata.CatalogRouteFence) ![32]u8 {
    try fence.validate();
    const incarnation = fence.metadata_incarnation orelse return error.ChangefeedCapabilityUnsupported;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:root-history-authority:v1");
    var group: [8]u8 = undefined;
    std.mem.writeInt(u64, &group, fence.metadata_group_id, .little);
    hash.update(&group);
    hash.update(&incarnation);
    return hash.finalResult();
}
/// Supported policy controls cannot remove activation on the same physical
/// owner. Each cache hit still requires fresh qualified ownership/cut/ceiling.
/// A future disable operation must invalidate this retained activation proof.
pub const Cache = struct {
    const Proof = struct { owner: [32]u8, limit: u64, minimum_index: u64 };
    mutex: std.atomic.Mutex = .unlocked,
    entries: [64]?Proof = @splat(null),
    next: usize = 0,
    fn contains(self: *Cache, qualified: owners.Reply) bool {
        if (!self.mutex.tryLock()) return false;
        defer self.mutex.unlock();
        const digest = qualified.owner.digest();
        for (self.entries) |entry| if (entry) |proof| {
            if (std.mem.eql(u8, &proof.owner, &digest) and proof.limit == qualified.observation.retention_limit_bytes and qualified.observation.cut.index >= proof.minimum_index) return true;
        };
        return false;
    }
    fn remember(self: *Cache, qualified: owners.Reply, minimum_index: u64) void {
        if (!self.mutex.tryLock()) return;
        defer self.mutex.unlock();
        const proof: Proof = .{ .owner = qualified.owner.digest(), .limit = qualified.observation.retention_limit_bytes, .minimum_index = minimum_index };
        for (&self.entries) |*entry| if (entry.*) |existing| {
            if (std.mem.eql(u8, &existing.owner, &proof.owner)) {
                entry.* = proof;
                return;
            }
        };
        self.entries[self.next] = proof;
        self.next = (self.next + 1) % self.entries.len;
    }
};
pub const Provisioner = struct {
    controls: controls.Port,
    owners: owners.Port,
    guard: ?Guard = null,
    cache: ?*Cache = null,
    fn checkpoint(self: Provisioner, context: Context) !void {
        try context.ensureActive();
        if (self.guard) |guard| try guard.check(context);
    }
    pub fn ensure(self: Provisioner, a: A, fence: metadata.CatalogRouteFence, table: []const u8, input: owners.Request, original: Context) !owners.Reply {
        var scope = try owners.Scope.init(fence, original);
        const context = scope.context();
        try self.checkpoint(context);
        if (self.cache) |cache| {
            const current = self.owners.read(a, fence, table, input, context) catch |err| switch (err) {
                error.ChangefeedCapabilityUnsupported => null,
                else => return err,
            };
            try self.checkpoint(context);
            if (current) |qualified| if (cache.contains(qualified)) return qualified;
        }
        const binding = try authority(fence);
        const ns = fence.route.identity_namespace;
        var namespace: [24]u8 = undefined;
        std.mem.writeInt(u64, namespace[0..8], ns.table_id, .big);
        std.mem.writeInt(u64, namespace[8..16], ns.shard_id, .big);
        std.mem.writeInt(u64, namespace[16..24], ns.range_id, .big);
        const inspect: controls.wire.Request = .{ .version = 1, .operation = .submit, .command = .{ .inspect_policy = .{ .namespace = namespace, .authority = binding } } };
        var result = try self.controls.execute(a, fence, table, inspect, context);
        try result.validate(inspect);
        try self.checkpoint(context);
        if (!result.confirmed()) return error.ReadUnavailable;
        if (result.outcome.? == .rejected) {
            if (result.outcome.?.rejected != .resynchronization) return result.outcome.?.rejected.asError();
            // This is not an absence proof or authority takeover. Native CAS
            // independently rejects a conflicting root authority/configuration.
            // Only the exact initial policy is authorized, never a resize.
            const configure: controls.wire.Request = .{ .version = 1, .operation = .submit, .command = .{ .configure_policy = .{ .namespace = namespace, .authority = binding, .previous_revision = 0, .revision = 1, .limit = local.storage_committed_history.default_limit } } };
            try self.checkpoint(context);
            result = try self.controls.execute(a, fence, table, configure, context);
            try result.validate(configure);
            try self.checkpoint(context);
            if (!result.confirmed()) return error.ReadUnavailable;
            if (result.outcome.? == .rejected) return result.outcome.?.rejected.asError();
        }
        if (result.outcome.? != .applied or result.outcome.?.applied.policy == null) return error.HistoryCorrupt;
        const policy = result.outcome.?.applied.policy.?;
        try self.checkpoint(context);
        const qualified = try self.owners.read(a, fence, table, input, context);
        if (qualified.observation.cut.index < result.receipt.?.index) return error.ReadUnavailable;
        if (qualified.observation.retention_limit_bytes != policy.limit) return error.ReadUnavailable;
        try self.checkpoint(context);
        if (self.cache) |cache| cache.remember(qualified, result.receipt.?.index);
        return qualified;
    }
};

test "data raft ordered history policy provisioning confirms configuration before admission and preserves uncertainty" {
    const Fake = struct {
        configured: bool = false,
        revision: u64 = 1,
        limit: u64 = local.storage_committed_history.default_limit,
        index: u64 = 10,
        inspections: usize = 0,
        configurations: usize = 0,
        owner_reads: usize = 0,
        generation: u64 = 0,
        mode: enum { normal, unknown_inspect, lost_configuration, conflicting_authority, lagged_owner } = .normal,
        fn control(ptr: *anyopaque, _: A, _: metadata.CatalogRouteFence, _: []const u8, input: controls.wire.Request, _: Context) !controls.wire.Response {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.index += 1;
            const receipt: local.api_local_table_writes.table_write_source.history_ordered.Entry = .{ .term = 1, .index = self.index };
            if (input.command == .inspect_policy) {
                self.inspections += 1;
                if (self.mode == .unknown_inspect) return .{ .version = 1, .receipt = receipt };
                if (!self.configured or self.mode == .conflicting_authority) return .{ .version = 1, .receipt = receipt, .outcome = .{ .rejected = .resynchronization } };
            } else {
                try std.testing.expect(input.command == .configure_policy);
                try std.testing.expectEqual(@as(u64, 0), input.command.configure_policy.previous_revision);
                try std.testing.expectEqual(@as(u64, 1), input.command.configure_policy.revision);
                self.configurations += 1;
                if (self.mode == .conflicting_authority) return .{ .version = 1, .receipt = receipt, .outcome = .{ .rejected = .resynchronization } };
                self.configured = true;
                self.revision = 1;
                self.limit = input.command.configure_policy.limit;
                if (self.mode == .lost_configuration) return .{ .version = 1, .receipt = receipt };
            }
            return .{ .version = 1, .receipt = receipt, .outcome = .{ .applied = .{ .policy = .{ .revision = self.revision, .limit = self.limit } } } };
        }
        fn owner(ptr: *anyopaque, _: A, fence: metadata.CatalogRouteFence, _: []const u8, input: owners.Request, _: Context) !owners.Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.owner_reads += 1;
            if (!self.configured) return error.ChangefeedCapabilityUnsupported;
            var ns: [24]u8 = undefined;
            std.mem.writeInt(u64, ns[0..8], 7, .big);
            std.mem.writeInt(u64, ns[8..16], 9, .big);
            std.mem.writeInt(u64, ns[16..24], 9, .big);
            const binding: @FieldType(owners.Reply, "owner") = .{ .metadata_group_id = 1, .metadata_incarnation = fence.metadata_incarnation.?, .namespace = ns, .table_binding = input.expected_table_binding, .root_generation = self.generation, .group_id = 9, .topology_epoch = 1 };
            return .{ .owner = binding, .observation = try owners.wire.Reply.init(.{ .owner = binding }, .{ .term = 1, .index = if (self.mode == .lagged_owner) 1 else self.index }, self.limit) };
        }
        fn deny(_: *anyopaque, _: Context) !void {
            return error.NotLeader;
        }
    };
    const a = std.testing.allocator;
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 9, .range_id = 9, .identity_namespace = .{ .table_id = 7, .shard_id = 9, .range_id = 9 } } };
    var fake: Fake = .{};
    var provisioner: Provisioner = .{ .controls = .{ .ptr = &fake, .execute_fn = Fake.control }, .owners = .{ .ptr = &fake, .read_fn = Fake.owner } };
    const input: owners.Request = .{ .version = 1, .expected_table_binding = @splat(2) };
    _ = try provisioner.ensure(a, fence, "docs", input, .{});
    try std.testing.expectEqual(@as(usize, 1), fake.configurations);
    fake.revision = 3;
    fake.limit /= 2;
    const administrative = try provisioner.ensure(a, fence, "docs", input, .{});
    try std.testing.expectEqual(fake.limit, administrative.observation.retention_limit_bytes);
    try std.testing.expectEqual(@as(usize, 1), fake.configurations);
    const owner_reads = fake.owner_reads;
    fake.mode = .unknown_inspect;
    try std.testing.expectError(error.ReadUnavailable, provisioner.ensure(a, fence, "docs", input, .{}));
    try std.testing.expectEqual(@as(usize, 1), fake.configurations);
    try std.testing.expectEqual(owner_reads, fake.owner_reads);
    fake.configured = false;
    fake.mode = .lost_configuration;
    try std.testing.expectError(error.ReadUnavailable, provisioner.ensure(a, fence, "docs", input, .{}));
    try std.testing.expectEqual(@as(usize, 2), fake.configurations);
    try std.testing.expectEqual(owner_reads, fake.owner_reads);
    fake.mode = .normal;
    _ = try provisioner.ensure(a, fence, "docs", input, .{});
    try std.testing.expectEqual(@as(usize, 2), fake.configurations);
    fake.mode = .lagged_owner;
    try std.testing.expectError(error.ReadUnavailable, provisioner.ensure(a, fence, "docs", input, .{}));
    fake.mode = .conflicting_authority;
    try std.testing.expectError(error.HistoryResynchronizationRequired, provisioner.ensure(a, fence, "docs", input, .{}));
    const inspections = fake.inspections;
    provisioner.guard = .{ .ptr = &fake, .check_fn = Fake.deny };
    try std.testing.expectError(error.NotLeader, provisioner.ensure(a, fence, "docs", input, .{}));
    try std.testing.expectEqual(inspections, fake.inspections);
    provisioner.guard = null;
    fake.mode = .normal;
    var cache: Cache = .{};
    provisioner.cache = &cache;
    _ = try provisioner.ensure(a, fence, "docs", input, .{});
    const cached_inspections = fake.inspections;
    _ = try provisioner.ensure(a, fence, "docs", input, .{});
    try std.testing.expectEqual(cached_inspections, fake.inspections);
    fake.generation += 1;
    _ = try provisioner.ensure(a, fence, "docs", input, .{});
    try std.testing.expectEqual(cached_inspections + 1, fake.inspections);
    fake.limit /= 2;
    _ = try provisioner.ensure(a, fence, "docs", input, .{});
    try std.testing.expectEqual(cached_inspections + 2, fake.inspections);
    try std.testing.expect(cache.mutex.tryLock());
    {
        defer cache.mutex.unlock();
        // Cache contention must not block admission or hold a lock over IO.
        _ = try provisioner.ensure(a, fence, "docs", input, .{});
        try std.testing.expectEqual(cached_inspections + 3, fake.inspections);
    }
}
