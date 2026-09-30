// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! One bounded, restartable metadata/owner publication step. The metadata
//! Raft state is the job journal: a lost response is reconciled from a fresh
//! leader read-index snapshot, never from a coordinator-local cursor.
const std = @import("std");
const policies = @import("../system_catalog/policies.zig");
const Receipt = @import("../storage/db/row_policy_bundle.zig").Receipt;

pub const Port = struct {
    ptr: *anyopaque,
    /// Returns exact immutable phase bytes from metadata read-index. Caller
    /// owns the result. The owner obtains the same bytes independently.
    bundle: *const fn (*anyopaque, std.mem.Allocator, policies.InstallRequest) anyerror![]u8,
    install: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, policies.InstallRequest) anyerror!Receipt,
    mutate: *const fn (*anyopaque, std.mem.Allocator, policies.PublicationCommand) anyerror!void,
};

pub const Step = enum { installed_owner, advanced_phase, complete };

pub fn beginCommand(revision: u64, table_id: u64, schema_version: u32, schema_digest: [32]u8, prior: ?policies.Publication, has_policies: bool, owners: []const policies.Publication.OwnerIdentity) !?policies.PublicationCommand {
    if (table_id == 0 or schema_version == 0 or owners.len == 0) return error.InvalidRowPolicyPublication;
    if (prior) |previous| {
        try previous.validateShape();
        if (previous.table_id != table_id or (previous.phase != .active and previous.phase != .disabled)) return error.RowPolicyCatalogChanged;
        if (previous.phase == .active and (previous.schema_version != schema_version or !std.mem.eql(u8, &previous.schema_digest, &schema_digest))) return error.RowPolicyCatalogChanged;
        if (previous.phase == .disabled and previous.disabled_acknowledged_owners.len != previous.required_owners.len) return error.RowPolicyCatalogChanged;
        if (!has_policies and previous.phase == .disabled) return null;
    } else if (!has_policies) return null;
    const generation = if (prior) |previous| try std.math.add(u64, previous.generation, 1) else @as(u64, 1);
    const publication: policies.Publication = .{
        .table_id = table_id,
        .schema_version = schema_version,
        .schema_digest = schema_digest,
        .generation = generation,
        .catalog_epoch = try std.math.add(u64, revision, 1),
        .serving_generation = if (prior) |previous| if (previous.phase == .active) previous.generation else null else null,
        .serving_catalog_epoch = if (prior) |previous| if (previous.phase == .active) previous.catalog_epoch else null else null,
        .phase = if (has_policies) .pending_install else .pending_disable,
        .required_owners = owners,
        .acknowledged_owners = &.{},
    };
    try publication.validateShape();
    return .{ .expected_revision = revision, .change = .{ .begin = publication } };
}

fn ownerWithoutAck(required: []const policies.Publication.OwnerIdentity, acknowledgements: []const policies.Publication.OwnerAck) ?policies.Publication.OwnerIdentity {
    var acknowledged_index: usize = 0;
    for (required) |owner| {
        if (acknowledged_index == acknowledgements.len or acknowledgements[acknowledged_index].owner.group_id != owner.group_id)
            return owner;
        acknowledged_index += 1;
    }
    return null;
}

pub fn advance(alloc: std.mem.Allocator, table_name: []const u8, revision: u64, publication: policies.Publication, port: Port) !Step {
    try publication.validateShape();
    const acknowledgements = switch (publication.phase) {
        .pending_install, .pending_disable => publication.acknowledged_owners,
        .serving_install, .serving_disable => publication.serving_acknowledged_owners,
        .disabled => publication.disabled_acknowledged_owners,
        .active => return .complete,
    };
    if (ownerWithoutAck(publication.required_owners, acknowledgements)) |owner| {
        const request: policies.InstallRequest = .{
            .table_id = publication.table_id,
            .expected_generation = publication.generation,
            .expected_catalog_epoch = publication.catalog_epoch,
            .expected_phase = publication.phase,
            .owner_group_id = owner.group_id,
            .expected_descriptor_digest = owner.descriptor_digest,
        };
        const bytes = try port.bundle(port.ptr, alloc, request);
        defer alloc.free(bytes);
        if (bytes.len == 0 or bytes.len > policies.max_install_snapshot_bytes) return error.InvalidRowPolicyBundle;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        const receipt = try port.install(port.ptr, alloc, table_name, owner.group_id, request);
        if (receipt.table_id != publication.table_id or receipt.generation != publication.generation or
            receipt.catalog_epoch != publication.catalog_epoch or receipt.phase != publication.phase or
            receipt.applied_term == 0 or receipt.applied_index == 0 or
            !std.mem.eql(u8, &receipt.bundle_digest, &digest) or
            !std.mem.eql(u8, &receipt.descriptor_digest, &owner.descriptor_digest)) return error.RowPolicyCatalogChanged;
        const ack: policies.Publication.OwnerAck = .{
            .owner = owner,
            .catalog_epoch = receipt.catalog_epoch,
            .phase = receipt.phase,
            .applied_term = receipt.applied_term,
            .applied_index = receipt.applied_index,
            .bundle_digest = receipt.bundle_digest,
        };
        try port.mutate(port.ptr, alloc, .{ .expected_revision = revision, .change = .{ .acknowledge = .{ .table_id = publication.table_id, .generation = publication.generation, .receipt = ack } } });
        return .installed_owner;
    }
    const next: policies.Publication.Phase = switch (publication.phase) {
        .pending_install => .serving_install,
        .pending_disable => .serving_disable,
        .serving_install => .active,
        .serving_disable => .disabled,
        .active, .disabled => return .complete,
    };
    try port.mutate(port.ptr, alloc, .{ .expected_revision = revision, .change = .{ .promote = .{ .table_id = publication.table_id, .generation = publication.generation, .phase = next } } });
    return .advanced_phase;
}

test "row policy coordinator selects exact unacknowledged owner and holds final publication" {
    const owners = [_]policies.Publication.OwnerIdentity{
        .{ .group_id = 7, .descriptor_digest = @splat(1) },
        .{ .group_id = 8, .descriptor_digest = @splat(2) },
    };
    var bundle_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("exact immutable bundle", &bundle_digest, .{});
    const first: policies.Publication.OwnerAck = .{ .owner = owners[0], .catalog_epoch = 10, .phase = .pending_install, .applied_term = 2, .applied_index = 3, .bundle_digest = bundle_digest };
    const publication: policies.Publication = .{ .table_id = 9, .schema_version = 1, .schema_digest = @splat(5), .generation = 1, .catalog_epoch = 10, .phase = .pending_install, .required_owners = &owners, .acknowledged_owners = &.{first} };
    const Fake = struct {
        selected: u64 = 0,
        command: ?policies.PublicationCommand = null,
        fn bundle(_: *anyopaque, alloc: std.mem.Allocator, _: policies.InstallRequest) ![]u8 {
            return alloc.dupe(u8, "exact immutable bundle");
        }
        fn install(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, group: u64, request: policies.InstallRequest) !Receipt {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.selected = group;
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash("exact immutable bundle", &digest, .{});
            return .{ .table_id = request.table_id, .generation = request.expected_generation, .catalog_epoch = request.expected_catalog_epoch, .phase = request.expected_phase, .applied_term = 3, .applied_index = 4, .bundle_digest = digest, .descriptor_digest = request.expected_descriptor_digest };
        }
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, command: policies.PublicationCommand) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.command = command;
        }
    };
    var fake: Fake = .{};
    try std.testing.expectEqual(Step.installed_owner, try advance(std.testing.allocator, "table:9", 10, publication, .{ .ptr = &fake, .bundle = Fake.bundle, .install = Fake.install, .mutate = Fake.mutate }));
    try std.testing.expectEqual(@as(u64, 8), fake.selected);
    try std.testing.expectEqual(@as(u64, 8), fake.command.?.change.acknowledge.receipt.owner.group_id);
    var full = publication;
    const second = fake.command.?.change.acknowledge.receipt;
    full.acknowledged_owners = &.{ first, second };
    try std.testing.expectEqual(Step.advanced_phase, try advance(std.testing.allocator, "table:9", 11, full, .{ .ptr = &fake, .bundle = Fake.bundle, .install = Fake.install, .mutate = Fake.mutate }));
    try std.testing.expectEqual(policies.Publication.Phase.serving_install, fake.command.?.change.promote.phase);
}

test "row policy coordinator resumes final disabled owner installation after metadata commit" {
    const owner: policies.Publication.OwnerIdentity = .{ .group_id = 17, .descriptor_digest = @splat(3) };
    const candidate: policies.Publication.OwnerAck = .{ .owner = owner, .catalog_epoch = 22, .phase = .pending_disable, .applied_term = 2, .applied_index = 3, .bundle_digest = @splat(4) };
    const serving: policies.Publication.OwnerAck = .{ .owner = owner, .catalog_epoch = 22, .phase = .serving_disable, .applied_term = 2, .applied_index = 4, .bundle_digest = @splat(5) };
    const publication: policies.Publication = .{
        .table_id = 9,
        .schema_version = 1,
        .schema_digest = @splat(6),
        .generation = 2,
        .catalog_epoch = 22,
        .phase = .disabled,
        .required_owners = &.{owner},
        .acknowledged_owners = &.{candidate},
        .serving_acknowledged_owners = &.{serving},
    };
    const Fake = struct {
        installed: bool = false,
        command: ?policies.PublicationCommand = null,
        fn bundle(_: *anyopaque, alloc: std.mem.Allocator, request: policies.InstallRequest) ![]u8 {
            try std.testing.expectEqual(policies.Publication.Phase.disabled, request.expected_phase);
            return alloc.dupe(u8, "final disabled snapshot");
        }
        fn install(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: u64, request: policies.InstallRequest) !Receipt {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.installed = true;
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash("final disabled snapshot", &digest, .{});
            return .{ .table_id = request.table_id, .generation = request.expected_generation, .catalog_epoch = request.expected_catalog_epoch, .phase = request.expected_phase, .applied_term = 3, .applied_index = 8, .bundle_digest = digest, .descriptor_digest = request.expected_descriptor_digest };
        }
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, command: policies.PublicationCommand) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.command = command;
        }
    };
    var fake: Fake = .{};
    try std.testing.expectEqual(Step.installed_owner, try advance(std.testing.allocator, "table:9", 22, publication, .{ .ptr = &fake, .bundle = Fake.bundle, .install = Fake.install, .mutate = Fake.mutate }));
    try std.testing.expect(fake.installed);
    try std.testing.expectEqual(policies.Publication.Phase.disabled, fake.command.?.change.acknowledge.receipt.phase);
    var completed = publication;
    completed.disabled_acknowledged_owners = &.{fake.command.?.change.acknowledge.receipt};
    try std.testing.expectEqual(Step.complete, try advance(std.testing.allocator, "table:9", 23, completed, .{ .ptr = &fake, .bundle = Fake.bundle, .install = Fake.install, .mutate = Fake.mutate }));
}
