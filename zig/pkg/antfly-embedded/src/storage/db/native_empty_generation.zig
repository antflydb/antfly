// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Hidden native replacement of an irreversibly lost image. There is no
//! source summary/seal and no claim that an absent owner processed a command.
//! Metadata admits loss provenance; the new native owner attests only its own
//! installation of freshly derived target-generation acceptance scopes.
const std = @import("std");
const contract = @import("restore_staging_contract.zig");
const admission = @import("relational_integrity_generation_admission.zig");
const positions = @import("receipt_position.zig");
pub const origin_contract = @import("native_empty_origin.zig");
pub const Origin = origin_contract.Origin;
const Digest = [32]u8;
const A = std.mem.Allocator;
pub const receipt_key = "\x00\x00__metadata__:restore_native_empty_installed";
pub const max_scopes = 1024;

pub const Install = struct {
    scope: Digest,
    origin: Origin,
    transitions: []const admission.Transition,
    pub fn validate(self: Install) !void {
        try self.origin.validate();
        if (std.mem.allEqual(u8, &self.scope, 0) or self.transitions.len > max_scopes) return error.InvalidRestoreStagingCommand;
        for (self.transitions, 0..) |transition, i| {
            try transition.validate();
            if (transition.expected_generation != null or transition.next_generation == null or !std.mem.eql(u8, &transition.decision_digest, &self.scope)) return error.InvalidRestoreStagingCommand;
            if (i != 0) {
                const previous = self.transitions[i - 1];
                const order = std.mem.order(u8, previous.child_table_name, transition.child_table_name);
                if (order == .gt or (order == .eq and std.mem.order(u8, previous.constraint_name, transition.constraint_name) != .lt)) return error.InvalidRestoreStagingCommand;
            }
        }
    }
    pub fn digest(self: Install) !Digest {
        try self.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/native-empty-target-install/v1\x00");
        hash.update(&self.scope);
        hash.update(&try self.origin.digest());
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, @intCast(self.transitions.len), .little);
        hash.update(&bytes);
        for (self.transitions) |transition| {
            std.mem.writeInt(u64, &bytes, transition.child_table_id, .little);
            hash.update(&bytes);
            for ([_][]const u8{ transition.child_table_name, transition.constraint_name }) |name| {
                std.mem.writeInt(u64, &bytes, @intCast(name.len), .little);
                hash.update(&bytes);
                hash.update(name);
            }
            hash.update(&transition.next_generation.?);
            hash.update(&transition.plan_id);
        }
        var result: Digest = undefined;
        hash.final(&result);
        return result;
    }
};

pub const Receipt = struct {
    scope: Digest,
    origin_kind: origin_contract.Kind,
    origin_digest: Digest,
    install_digest: Digest,
    position: positions.Native,
    pub const encoded_len = 4 + 1 + 96 + positions.Position.encoded_len + 32;
    pub fn encode(self: Receipt) ![encoded_len]u8 {
        try self.position.validate();
        if (std.mem.allEqual(u8, &self.scope, 0) or std.mem.allEqual(u8, &self.origin_digest, 0) or std.mem.allEqual(u8, &self.install_digest, 0)) return error.InvalidRestoreStagingCommand;
        var bytes: [encoded_len]u8 = undefined;
        @memcpy(bytes[0..4], "NEI1");
        bytes[4] = @backingInt(self.origin_kind);
        @memcpy(bytes[5..37], &self.scope);
        @memcpy(bytes[37..69], &self.origin_digest);
        @memcpy(bytes[69..101], &self.install_digest);
        @memcpy(bytes[101..134], &try (positions.Position{ .native = self.position }).encode());
        std.crypto.hash.Blake3.hash(bytes[0..134], bytes[134..166], .{});
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Receipt {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..4], "NEI1")) return error.InvalidRestoreStagingCommand;
        const kind: origin_contract.Kind = switch (bytes[4]) {
            @backingInt(origin_contract.Kind.lost_source) => .lost_source,
            @backingInt(origin_contract.Kind.schema_only_snapshot) => .schema_only_snapshot,
            else => return error.InvalidRestoreStagingCommand,
        };
        var checksum: Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..134], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[134..166])) return error.InvalidRestoreStagingCommand;
        const position = try positions.Position.decode(bytes[101..134]);
        if (position != .native) return error.InvalidRestoreStagingCommand;
        const result: Receipt = .{ .scope = bytes[5..37].*, .origin_kind = kind, .origin_digest = bytes[37..69].*, .install_digest = bytes[69..101].*, .position = position.native };
        _ = try result.encode();
        return result;
    }
};

pub fn load(txn: anytype) !?Receipt {
    const raw = txn.get(receipt_key) catch |err| return if (err == error.NotFound) null else err;
    return try Receipt.decode(raw);
}

/// Called only through the authenticated native hidden-owner control. The
/// bootstrap's exact digest and fresh validated namespace were independently
/// rejoined to the immutable metadata plan before this transaction starts.
pub fn requireInstalled(txn: anytype, command: Install) !?Receipt {
    const receipt = (try load(txn)) orelse return null;
    if (!std.mem.eql(u8, &receipt.scope, &command.scope) or receipt.origin_kind != command.origin.kind() or !std.mem.eql(u8, &receipt.origin_digest, &try command.origin.digest()) or
        !std.mem.eql(u8, &receipt.install_digest, &try command.digest())) return error.RestoreStagingScopeChanged;
    const ns = @import("online_source_contract.zig").namespaceBytes(receipt.position.namespace);
    const clock = try @import("../source_authority.zig").require(txn, .native, ns);
    if (clock.sequence < receipt.position.sequence) return error.OnlineSourceCorrupt;
    for (command.transitions) |transition| {
        const current = (try admission.load(txn, transition.child_table_name, transition.constraint_name)) orelse return error.GenerationAdmissionChanged;
        if (current.phase != .active or current.child_table_id != transition.child_table_id or
            !std.meta.eql(current.active_generation, transition.next_generation) or !std.mem.eql(u8, &current.plan_id, &transition.plan_id) or
            !std.mem.eql(u8, &current.decision_digest, &command.scope)) return error.GenerationAdmissionChanged;
    }
    return receipt;
}

/// Called only through the authenticated native hidden-owner control. The
/// bootstrap's exact digest and fresh validated namespace were independently
/// rejoined to the immutable metadata plan before this transaction starts.
pub fn stageInstall(a: A, txn: anytype, bootstrap: contract.OwnerBootstrap, command: Install, stamp: positions.Native) !Receipt {
    try bootstrap.validate();
    try stamp.validate();
    const expected = bootstrap.native_empty_source orelse return error.InvalidRestoreStagingCommand;
    try expected.validate(bootstrap.scope);
    const logical = try command.digest();
    if (!bootstrap.scope.target_namespace.eql(stamp.namespace) or !std.mem.eql(u8, &bootstrap.scope.digest(), &command.scope) or
        !std.meta.eql(expected.origin, command.origin) or
        !std.mem.eql(u8, &expected.expected_install_digest, &logical)) return error.RestoreStagingScopeChanged;
    const catalog_raw = txn.get(@import("table_catalog.zig").key) catch return error.RestoreStagingScopeChanged;
    const catalog = try @import("table_catalog.zig").Catalog.decode(catalog_raw);
    if (catalog.row_count != 0 or catalog.sql_persistence == null or catalog.sql_persistence.?.kind != .unlogged) return error.RestoreStagingScopeChanged;
    const raw = txn.get(contract.key) catch return error.RestoreStagingScopeChanged;
    var progress = try contract.Progress.decode(a, raw);
    defer progress.deinit();
    if (!std.mem.eql(u8, &progress.value.scope.digest(), &command.scope) or progress.value.phase == .canceled) return error.RestoreStagingScopeChanged;
    if (try load(txn)) |receipt| {
        if (!std.mem.eql(u8, &receipt.scope, &command.scope) or receipt.origin_kind != command.origin.kind() or !std.mem.eql(u8, &receipt.origin_digest, &try command.origin.digest()) or !std.mem.eql(u8, &receipt.install_digest, &logical)) return error.RestoreStagingScopeChanged;
        for (command.transitions) |transition| {
            const current = (try admission.load(txn, transition.child_table_name, transition.constraint_name)) orelse return error.GenerationAdmissionChanged;
            if (current.phase != .active or current.child_table_id != transition.child_table_id or
                !std.meta.eql(current.active_generation, transition.next_generation) or !std.mem.eql(u8, &current.plan_id, &bootstrap.scope.plan_id) or
                !std.mem.eql(u8, &current.decision_digest, &command.scope)) return error.GenerationAdmissionChanged;
        }
        return receipt;
    }
    if (progress.value.phase != .validated) return error.RestoreStagingInProgress;
    for (command.transitions) |transition| {
        if (!std.mem.eql(u8, &transition.plan_id, &bootstrap.scope.plan_id) or (try admission.load(txn, transition.child_table_name, transition.constraint_name)) != null) return error.GenerationAdmissionChanged;
    }
    for (command.transitions) |transition| {
        try admission.stageTransition(a, txn, transition);
        try admission.activateTransition(a, txn, transition);
    }
    const receipt: Receipt = .{ .scope = command.scope, .origin_kind = command.origin.kind(), .origin_digest = try command.origin.digest(), .install_digest = logical, .position = stamp };
    const bytes = try receipt.encode();
    try txn.put(receipt_key, &bytes);
    return receipt;
}

test "native lost-image target receipt never certifies a source or raft position" {
    const receipt: Receipt = .{ .scope = @splat(1), .origin_kind = .schema_only_snapshot, .origin_digest = @splat(2), .install_digest = @splat(3), .position = .{ .namespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 }, .sequence = 11 } };
    var bytes = try receipt.encode();
    try std.testing.expectEqualDeep(receipt, try Receipt.decode(&bytes));
    bytes[37] ^= 1;
    try std.testing.expectError(error.InvalidRestoreStagingCommand, Receipt.decode(&bytes));
    bytes = try receipt.encode();
    // Even a checksum-correct Raft stamp cannot substitute for the actual
    // local native transaction position of this target-only receipt.
    bytes[101] = 1;
    @memset(bytes[118..134], 0);
    std.crypto.hash.Blake3.hash(bytes[0..134], bytes[134..166], .{});
    try std.testing.expectError(error.InvalidRestoreStagingCommand, Receipt.decode(&bytes));
}

test "native lost-image acceptance installs contain only fresh target generations" {
    const transition: admission.Transition = .{ .child_table_id = 7, .child_table_name = "children", .constraint_name = "fk", .expected_generation = null, .next_generation = @splat(4), .plan_id = @splat(5), .decision_digest = @splat(1) };
    var transitions = [_]admission.Transition{transition};
    const command: Install = .{ .scope = @splat(1), .origin = .{ .schema_only_snapshot = .{ .manifest_digest = @splat(2), .schema_digest = @splat(3), .source_table_id = 11 } }, .transitions = &transitions };
    try command.validate();
    const digest = try command.digest();
    var other_loss = command;
    other_loss.origin.schema_only_snapshot.manifest_digest[0] ^= 1;
    try std.testing.expect(!std.mem.eql(u8, &digest, &try other_loss.digest()));
    transitions[0].expected_generation = @splat(6);
    try std.testing.expectError(error.InvalidRestoreStagingCommand, command.validate());
    transitions[0] = transition;
    const duplicated = [_]admission.Transition{ transition, transition };
    try std.testing.expectError(error.InvalidRestoreStagingCommand, (Install{ .scope = @splat(1), .origin = .{ .schema_only_snapshot = .{ .manifest_digest = @splat(2), .schema_digest = @splat(3), .source_table_id = 11 } }, .transitions = &duplicated }).validate());
}
