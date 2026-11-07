// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Root-authenticated native process lifecycle, not table/Raft ownership.
//! The receiver independently rejoins enrollment and the current reporter.
const std = @import("std");
const boots = @import("sql_relation_boots.zig");
const enrollment = @import("store_root_enrollment.zig");
const signing = @import("antfly_local_sources").storage_db_root_signing_identity;

pub const max_command_bytes = 1024 * 1024;
pub const Command = union(enum) {
    claim: boots.Claim,
    advance: struct { expected: boots.Record, after: ?u64 },
    begin_drain: struct { node_id: u64, instance: [16]u8 },
    finish_drain: boots.Record,
    initialized: @import("sql_relation_attestation.zig").SignedBinding,
    adopted: @import("sql_relation_attestation.zig").SignedBinding,
    reserve_empty: struct { expected: boots.Record, scope: @import("sql_relation_attestation.zig").CreationScope, table_id: u64, previous: @import("sql_relation_empty_target.zig").Attempt },
    retire_unpublished: struct { expected: boots.Record, scope: @import("sql_relation_attestation.zig").CreationScope, table_id: u64, proof_digest: [32]u8 },
    initialized_empty: struct { proof: @import("sql_relation_attestation.zig").SignedBinding, attempt: @import("sql_relation_empty_target.zig").Attempt },
    group_report: struct { boot_generation: u64, instance: [16]u8, report: @import("table_manager.zig").RuntimeGroupStatusReport },

    pub fn node(self: Command) u64 {
        return switch (self) {
            .claim => |value| value.node_id,
            .advance => |value| value.expected.node_id,
            .begin_drain => |value| value.node_id,
            .finish_drain => |value| value.node_id,
            .initialized, .adopted => |value| value.identity.node_id,
            .group_report => |value| value.report.node_id,
            .reserve_empty => |value| value.expected.node_id,
            .retire_unpublished => |value| value.expected.node_id,
            .initialized_empty => |value| value.proof.identity.node_id,
        };
    }
    pub fn instance(self: Command) [16]u8 {
        return switch (self) {
            .claim => |value| value.instance,
            .advance => |value| value.expected.instance,
            .begin_drain => |value| value.instance,
            .finish_drain => |value| value.instance,
            .initialized, .adopted => |value| value.binding.native_instance,
            .group_report => |value| value.instance,
            .reserve_empty => |value| value.expected.instance,
            .retire_unpublished => |value| value.expected.instance,
            .initialized_empty => |value| value.proof.binding.native_instance,
        };
    }
    pub fn validate(self: Command) !void {
        if (std.mem.allEqual(u8, &self.instance(), 0)) return error.InvalidSqlRelationPersistence;
        switch (self) {
            .claim => |value| _ = try value.digest(),
            .advance => |value| {
                try value.expected.validate();
                if (value.expected.phase != .recovering or
                    (value.after != null and value.expected.recovery_after != null and value.after.? <= value.expected.recovery_after.?))
                    return error.InvalidSqlRelationPersistence;
            },
            .finish_drain => |value| {
                try value.validate();
                if (value.phase != .draining) return error.InvalidSqlRelationPersistence;
            },
            .initialized => |value| {
                try value.verify();
                if (value.kind != .initialized) return error.InvalidSqlRelationPersistence;
            },
            .adopted => |value| {
                try value.verify();
                if (value.kind != .adopted) return error.InvalidSqlRelationPersistence;
            },
            .retire_unpublished => |value| {
                try value.expected.validate();
                try value.scope.validate();
                if (value.table_id == 0 or std.mem.allEqual(u8, &value.proof_digest, 0)) return error.InvalidSqlRelationPersistence;
            },
            .reserve_empty => |value| {
                try value.expected.validate();
                try value.scope.validate();
                try value.previous.validate();
                if (value.expected.phase != .recovering or !value.expected.unclean_restart or value.table_id == 0) return error.InvalidSqlRelationPersistence;
            },
            .initialized_empty => |value| {
                try value.proof.verify();
                try value.attempt.validate();
                if (value.proof.kind != .initialized or value.proof.creation_scope == null or value.attempt.number < 2) return error.InvalidSqlRelationPersistence;
            },
            .group_report => |value| {
                const report = value.report;
                if (value.boot_generation == 0 or report.table_id == 0 or report.group_id == 0 or report.node_id == 0 or report.store_id == 0 or report.table_name.len == 0 or report.table_name.len > 512 or report.source_authority == null or report.source_authority.?.kind != .native)
                    return error.InvalidSqlRelationPersistence;
            },
            .begin_drain => {},
        }
    }
};

pub const Request = struct {
    version: u8 = 1,
    metadata_group_id: u64,
    identity: enrollment.Identity,
    reporter_incarnation: u64,
    /// Native-generated context epoch. Signing is permitted only after the
    /// compiled context proves that the requested action matches this epoch.
    native_instance: [16]u8,
    /// A clean completion additionally binds the native closed-admission /
    /// successful all-owner flush proof. Other commands must leave it zero.
    clean_digest: [32]u8 = @splat(0),
    command: Command,
    signature: [64]u8 = @splat(0),

    pub fn digest(self: Request) ![32]u8 {
        if (self.version != 1 or self.metadata_group_id == 0 or self.reporter_incarnation == 0 or
            self.identity.node_id != self.command.node() or
            !std.mem.eql(u8, &self.native_instance, &self.command.instance()) or
            ((self.command == .finish_drain) == std.mem.allEqual(u8, &self.clean_digest, 0)))
            return error.InvalidSqlRelationPersistence;
        try self.command.validate();
        if (self.command == .initialized or self.command == .adopted) {
            const proof = if (self.command == .initialized) self.command.initialized else self.command.adopted;
            if (proof.metadata_group_id != self.metadata_group_id or proof.reporter_incarnation != self.reporter_incarnation or
                !std.mem.eql(u8, &try proof.identity.digest(), &try self.identity.digest())) return error.InvalidSqlRelationPersistence;
        }
        if (self.command == .initialized_empty) {
            const proof = self.command.initialized_empty.proof;
            if (proof.metadata_group_id != self.metadata_group_id or proof.reporter_incarnation != self.reporter_incarnation or !std.mem.eql(u8, &try proof.identity.digest(), &try self.identity.digest())) return error.InvalidSqlRelationPersistence;
        }
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/sql-relation-boot/native-context/v1\x00");
        hash.update(&try self.identity.digest());
        var number: [8]u8 = undefined;
        for ([_]u64{ self.metadata_group_id, self.reporter_incarnation }) |value| {
            std.mem.writeInt(u64, &number, value, .little);
            hash.update(&number);
        }
        hash.update(&self.native_instance);
        hash.update(&self.clean_digest);
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(hash, &.{});
        try std.json.Stringify.value(self.command, .{}, &writer.writer);
        var result: [32]u8 = undefined;
        writer.hasher.final(&result);
        return result;
    }
    pub fn verify(self: Request) !void {
        signing.verify(self.identity.public_key, &try self.digest(), self.signature) catch return error.InvalidSqlRelationPersistence;
    }
    pub fn requireApplied(self: Request, observation: boots.Observation) !boots.Record {
        const value = observation.record orelse return error.SqlRelationOwnerChanged;
        if (!observation.registration_current) return error.SqlRelationOwnerChanged;
        try value.requireRegistration(.{ .identity = self.identity, .reporter_incarnation = self.reporter_incarnation, .instance = self.native_instance });
        const applied = switch (self.command) {
            .claim => |claim| value.generation == (std.math.add(u64, claim.expected_generation, 1) catch return error.CatalogGenerationExhausted),
            .advance => |advance| value.generation == advance.expected.generation and
                (if (advance.after) |after| value.phase == .ready or (value.phase == .recovering and value.recovery_after != null and value.recovery_after.? >= after) else value.phase == .ready),
            .begin_drain => value.phase == .draining or value.phase == .clean,
            .finish_drain => |expected| value.generation == expected.generation and value.phase == .clean,
            .initialized => |proof| value.generation == proof.boot_generation and (value.phase == .recovering or value.phase == .ready) and observation.initialized != null and
                std.mem.eql(u8, &try observation.initialized.?.digest(), &try proof.digest()),
            .adopted => |proof| value.generation == proof.boot_generation and (value.phase == .recovering or value.phase == .ready) and observation.initialized != null and
                std.mem.eql(u8, &try observation.initialized.?.digest(), &try proof.digest()),
            .retire_unpublished => |retirement| observation.unpublished_retirement_committed and observation.unpublished_retirement != null and std.mem.eql(u8, &try observation.unpublished_retirement.?.digest(), &retirement.proof_digest),
            .reserve_empty => |reserve| value.generation == reserve.expected.generation and value.phase == .recovering and observation.attempt != null and observation.attempt.?.number == (std.math.add(u64, reserve.previous.number, 1) catch return error.CatalogGenerationExhausted),
            .initialized_empty => |initialized| value.generation == initialized.proof.boot_generation and value.phase == .recovering and observation.initialized != null and observation.attempt != null and observation.attempt.?.number == initialized.attempt.number and std.mem.eql(u8, &observation.attempt.?.digest, &initialized.attempt.digest) and std.mem.eql(u8, &try observation.initialized.?.digest(), &try initialized.proof.digest()),
            .group_report => |report| value.generation == report.boot_generation and value.phase == .ready and observation.report_digest != null and
                std.mem.eql(u8, &observation.report_digest.?, &try @import("sql_relation_attestation.zig").nativeReportDigest(report.report)),
        };
        if (!applied) return error.SqlRelationOwnerChanged;
        return value;
    }
    pub fn observationQuery(self: Request) boots.Query {
        return .{
            .node = self.command.node(),
            .unpublished_retirement = if (self.command == .retire_unpublished) .{ .scope = self.command.retire_unpublished.scope, .table_id = self.command.retire_unpublished.table_id } else null,
            .binding_table = if (self.command == .initialized) self.command.initialized.binding.table_id else if (self.command == .adopted) self.command.adopted.binding.table_id else if (self.command == .initialized_empty) self.command.initialized_empty.proof.binding.table_id else if (self.command == .reserve_empty) self.command.reserve_empty.table_id else null,
            .report_group = if (self.command == .group_report) self.command.group_report.report.group_id else null,
            .creation_scope = if (self.command == .initialized) self.command.initialized.creation_scope else if (self.command == .adopted) self.command.adopted.creation_scope else if (self.command == .initialized_empty) self.command.initialized_empty.proof.creation_scope else if (self.command == .reserve_empty) self.command.reserve_empty.scope else null,
        };
    }
    pub fn sign(self: Request, root: signing.State) !Request {
        if (root.root_incarnation != self.identity.root_incarnation or
            !std.mem.eql(u8, &root.public_key, &self.identity.public_key)) return error.InvalidSqlRelationPersistence;
        var result = self;
        result.signature = try root.sign(&try self.digest());
        return result;
    }
    pub fn parse(a: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(Request) {
        if (bytes.len == 0 or bytes.len > max_command_bytes) return error.InvalidSqlRelationPersistence;
        var parsed = try std.json.parseFromSlice(Request, a, bytes, .{});
        errdefer parsed.deinit();
        try parsed.value.verify();
        return parsed;
    }
};

test "SQL relation boot proof binds actual process action reporter root and clean proof" {
    const seed: [32]u8 = @splat(7);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: signing.State = .{ .root_incarnation = 19, .seed = seed, .public_key = pair.public_key.toBytes() };
    const request = try (Request{
        .metadata_group_id = 3,
        .identity = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 11, .root_incarnation = root.root_incarnation, .public_key = root.public_key },
        .reporter_incarnation = 17,
        .native_instance = @splat(1),
        .command = .{ .claim = .{ .node_id = 9, .instance = @splat(1), .expected_generation = 0, .expected_instance = null } },
    }).sign(root);
    try request.verify();
    var changed = request;
    changed.native_instance = @splat(2);
    try std.testing.expectError(error.InvalidSqlRelationPersistence, changed.verify());
    changed = request;
    changed.reporter_incarnation += 1;
    try std.testing.expectError(error.InvalidSqlRelationPersistence, changed.verify());
    changed = request;
    changed.metadata_group_id += 1;
    try std.testing.expectError(error.InvalidSqlRelationPersistence, changed.verify());
    changed = request;
    changed.clean_digest = @splat(1);
    try std.testing.expectError(error.InvalidSqlRelationPersistence, changed.verify());
}
