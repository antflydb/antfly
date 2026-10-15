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

//! Terminal release identity. Checksums/receipts do not authorize execution:
//! receivers must obtain metadata permission and stage the record, exact fence
//! release, native clock, transport marker and outbox in one transaction.
const std = @import("std");
const validation = @import("relational_index_validation.zig");
const owner = @import("relational_index_owner_job.zig");
const positions = @import("receipt_position.zig");
const publication = @import("relational_index_publication_progress.zig");
const authority = @import("../source_authority.zig");
const Digest = [32]u8;
const magic = "AIIR\x01\x00\x00\x00";

fn hash(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

/// Root-independent metadata intent. The installed command/position must come
/// from the retained accepted installation proof, not from an RPC caller.
pub const Intent = struct {
    installed: validation.Installed,
    metadata_decision: Digest,

    pub fn validate(self: @This()) !void {
        if (std.mem.allEqual(u8, &self.metadata_decision, 0)) return error.InvalidPrivateIndexNativeReceipt;
        _ = try self.installed.command.receipt(self.installed.position);
    }

    pub fn requireInstalled(self: @This(), alloc: std.mem.Allocator, txn: anytype) !void {
        try self.validate();
        const actual = (try validation.Installed.load(alloc, txn, self.installed.command.validation.command.binding)) orelse return error.PrivateIndexTargetValidationRequired;
        if (!std.meta.eql(actual, self.installed)) return error.InvalidPrivateIndexNativeReceipt;
        try actual.requireCurrent(alloc, txn);
    }

    pub fn evidenceDigest(self: @This()) !Digest {
        try self.validate();
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-index-release-intent/v1");
        state.update(&try self.installed.command.evidenceDigest());
        state.update(&try (positions.Position{ .native = self.installed.position }).encode());
        state.update(&self.metadata_decision);
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }

    pub fn receipt(self: @This(), position: positions.Native) !publication.Receipt {
        try self.validate();
        try position.validate();
        if (!position.namespace.eql(self.installed.position.namespace) or position.sequence <= self.installed.position.sequence) return error.InvalidPrivateIndexNativeReceipt;
        var result = try self.installed.command.receipt(self.installed.position);
        result.action = .released;
        result.position = .{ .native = position };
        result.evidence_digest = try self.evidenceDigest();
        _ = try result.digest();
        return result;
    }
};

/// Owned, bounded evidence survives retirement of the large build inputs.
/// It records a historical committed cut; it is not a claim that mutable index
/// data still matches an old build audit, nor a replacement-root capability.
pub const Record = struct {
    pub const key = owner.key ++ ":released";
    pub const max_wire_bytes = 32 * 1024;
    intent: Intent,
    position: positions.Native,

    pub const Encoded = struct {
        bytes: [max_wire_bytes]u8,
        len: usize,
        pub fn slice(self: *const @This()) []const u8 {
            return self.bytes[0..self.len];
        }
    };

    pub fn validate(self: @This()) !void {
        _ = try self.intent.receipt(self.position);
    }

    pub fn encode(self: @This()) !Encoded {
        try self.validate();
        var result: Encoded = .{ .bytes = undefined, .len = 0 };
        @memcpy(result.bytes[0..magic.len], magic);
        var writer = std.Io.Writer.fixed(result.bytes[magic.len .. max_wire_bytes - 32]);
        std.json.Stringify.value(self, .{}, &writer) catch return error.InvalidPrivateIndexNativeReceipt;
        result.len = magic.len + writer.end;
        @memcpy(result.bytes[result.len..][0..32], &hash(result.bytes[0..result.len]));
        result.len += 32;
        return result;
    }

    pub fn decode(alloc: std.mem.Allocator, bytes: []const u8) !@This() {
        if (bytes.len <= magic.len + 32 or bytes.len > max_wire_bytes or !std.mem.eql(u8, bytes[0..magic.len], magic) or
            !std.mem.eql(u8, bytes[bytes.len - 32 ..], &hash(bytes[0 .. bytes.len - 32]))) return error.InvalidPrivateIndexNativeReceipt;
        var parsed = std.json.parseFromSlice(@This(), alloc, bytes[magic.len .. bytes.len - 32], .{ .ignore_unknown_fields = false, .max_value_len = 128 }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidPrivateIndexNativeReceipt,
        };
        defer parsed.deinit();
        const result = parsed.value;
        try result.validate();
        const canonical = try result.encode();
        if (!std.mem.eql(u8, bytes, canonical.slice())) return error.InvalidPrivateIndexNativeReceipt;
        return result;
    }

    /// Decode an actual durable slot and its native clock. A different job's
    /// retained terminal receipt cannot stand in for this requested intent.
    pub fn load(alloc: std.mem.Allocator, txn: anytype, intent: Intent) !?@This() {
        try intent.validate();
        const bytes = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        const record = try decode(alloc, bytes);
        const source = try authority.require(txn, .native, @import("online_source_contract.zig").namespaceBytes(record.position.namespace));
        if (source.sequence < record.position.sequence) return error.InvalidPrivateIndexNativeReceipt;
        if (!std.meta.eql(record.intent, intent)) return null;
        return record;
    }
};

fn testIntent() !Intent {
    const namespace: @import("doc_identity_namespace.zig").Namespace = .{ .table_id = 101, .shard_id = 11, .range_id = 12 };
    const binding: owner.Binding = .{ .id = @splat(1), .plan_digest = @splat(2), .owner_digest = @splat(3), .ordinal = 0, .authority = .native, .phase = .reserved, .fence = .{ .role = .index_build, .transition_id = 7, .attempt = 1, .admission_epoch = 7, .owner_group_id = 11, .peer_group_id = 11, .namespace = namespace, .catalog_digest = @splat(4) }, .integrity_before = @splat(4), .integrity_reserved = @splat(5), .integrity_candidate = @splat(6), .schema_before = @splat(7), .physical_before = null, .physical_reserved = .{ .revision = 2, .schema_version = 1, .blob_digest = @splat(8) }, .physical_candidate = .{ .revision = 2, .schema_version = 2, .blob_digest = @splat(9) }, .index = .{ .generation = 2, .slot = 0 }, .inputs_digest = @splat(10) };
    const expected: @import("relational_index_private_claims.zig").TargetExpectation = .{ .metadata_incarnation = @splat('1'), .binding_digest = hash(&try binding.encode()), .generation = @splat(2), .completion_revision = 1, .source_consumption_digest = @splat(3), .source_progress_digest = @splat(4), .table_name_digest = hash("items"), .rows = 0, .inventory_xor = @splat(0), .provenance = @splat(0) };
    const seal: validation.Seal = .{ .command = .{ .binding = binding, .expected = expected, .generation_digest = @splat(5), .inventory_digest = @splat(6), .source_members = 0, .generation_position = .{ .namespace = namespace, .sequence = 1 } }, .position = .{ .namespace = namespace, .sequence = 2 } };
    return .{ .installed = .{ .command = .{ .validation = seal, .metadata_decision = @splat(7), .schema_digest = @splat(8), .activation_digest = @splat(9) }, .position = .{ .namespace = namespace, .sequence = 3 } }, .metadata_decision = @splat(10) };
}

fn testDecodeAllocations(alloc: std.mem.Allocator, bytes: []const u8) !void {
    _ = try Record.decode(alloc, bytes);
}

test "relational index release receipt binds installed cut decision and later native position" {
    const intent = try testIntent();
    var position = intent.installed.position;
    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, intent.receipt(position));
    position.sequence += 1;
    const receipt = try intent.receipt(position);
    try std.testing.expectEqual(publication.Action.released, receipt.action);
    try std.testing.expectEqual(position, receipt.position.native);
    var changed = intent;
    changed.metadata_decision[0] ^= 1;
    try std.testing.expect(!std.mem.eql(u8, &try receipt.digest(), &try (try changed.receipt(position)).digest()));
    changed = intent;
    changed.metadata_decision = @splat(0);
    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, changed.validate());
    position.namespace.table_id += 1;
    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, intent.receipt(position));
}

test "relational index release terminal codec is bounded canonical and allocation failure clean" {
    const intent = try testIntent();
    const record: Record = .{ .intent = intent, .position = .{ .namespace = intent.installed.position.namespace, .sequence = 4 } };
    const encoded = try record.encode();
    try std.testing.expectEqualDeep(record, try Record.decode(std.testing.allocator, encoded.slice()));
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, testDecodeAllocations, .{encoded.slice()});
    var corrupt = encoded;
    corrupt.bytes[corrupt.len - 1] ^= 1;
    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, Record.decode(std.testing.allocator, corrupt.slice()));
    var spaced: Record.Encoded = .{ .bytes = undefined, .len = encoded.len + 1 };
    @memcpy(spaced.bytes[0..8], magic);
    spaced.bytes[8] = ' ';
    @memcpy(spaced.bytes[9 .. spaced.len - 32], encoded.bytes[8 .. encoded.len - 32]);
    @memcpy(spaced.bytes[spaced.len - 32 ..][0..32], &hash(spaced.bytes[0 .. spaced.len - 32]));
    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, Record.decode(std.testing.allocator, spaced.slice()));
}
