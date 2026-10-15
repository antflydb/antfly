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
    /// A bounded cleanup objective, not permission to delete live claims.
    /// A later full companion pass may clear it only at this exact native cut.
    pub const retirement_pending_key = owner.key ++ ":released-companions-dirty";
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
        const record = (try loadRaw(alloc, txn)) orelse return null;
        if (!std.meta.eql(record.intent, intent)) return null;
        return record;
    }

    fn loadRaw(alloc: std.mem.Allocator, txn: anytype) !?@This() {
        const bytes = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        const record = try decode(alloc, bytes);
        const source = try authority.require(txn, .native, @import("online_source_contract.zig").namespaceBytes(record.position.namespace));
        if (source.sequence < record.position.sequence) return error.InvalidPrivateIndexNativeReceipt;
        return record;
    }

    pub fn verifyRecovery(alloc: std.mem.Allocator, txn: anytype) !void {
        const maybe_record = try loadRaw(alloc, txn);
        const topology = @import("relational_integrity_topology.zig");
        const bytes = txn.get(topology.receipt_key) catch |err| switch (err) {
            error.NotFound => {
                if (maybe_record != null) return error.IntegrityTopologyFenceMissing;
                return;
            },
            else => return err,
        };
        const receipt = try topology.Fence.decode(bytes);
        const record = maybe_record orelse {
            if (receipt.role == .index_build and (try owner.load(txn)) == null) return error.InvalidPrivateIndexNativeReceipt;
            return;
        };
        const binding = record.intent.installed.command.validation.command.binding;
        if (receipt.admission_epoch < binding.fence.admission_epoch or
            ((receipt.role == .index_build or receipt.admission_epoch == binding.fence.admission_epoch) and !receipt.eql(binding.fence))) return error.IntegrityTopologyChanged;
        if (try owner.load(txn)) |current| {
            var original = current;
            original.phase = .reserved;
            if (std.meta.eql(original, binding)) return error.InvalidPrivateIndexOwnerJob;
        }
    }

    /// Caller owns commit/abort and stages transport markers/outboxes in THIS
    /// transaction. No row scan, artifact GC or metadata ACK occurs here.
    pub fn stage(alloc: std.mem.Allocator, txn: anytype, intent: Intent, replay: ?positions.Native) !struct { record: Record, duplicate: bool } {
        try intent.validate();
        if (replay) |position| _ = try intent.receipt(position);
        if (try load(alloc, txn, intent)) |previous| {
            if (replay) |position| if (!std.meta.eql(position, previous.position)) return error.InvalidPrivateIndexNativeReceipt;
            try verifyRecovery(alloc, txn);
            return .{ .record = previous, .duplicate = true };
        }
        try intent.requireInstalled(alloc, txn);
        const binding = intent.installed.command.validation.command.binding;
        const expected = intent.installed.command.validation.command.expected;
        const claims = @import("relational_index_private_claims.zig");
        const stream = claims.SourceStream;
        const head = (try stream.load(txn, binding)) orelse return error.PrivateIndexRootRecoveryRequired;
        const consumed = (try stream.loadConsumption(txn, binding, head)) orelse return error.PrivateIndexRootRecoveryRequired;
        if (head.state != .ready or head.pending() != 0 or
            !std.mem.eql(u8, &head.progress_digest, &expected.source_progress_digest) or
            !std.mem.eql(u8, &try consumed.prefix().digest(), &expected.source_consumption_digest)) return error.PrivateIndexTargetValidationRequired;
        const namespace = @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace);
        const source = try authority.require(txn, .native, namespace);
        const position: positions.Native = .{ .namespace = binding.fence.namespace, .sequence = std.math.add(u64, source.sequence, 1) catch return error.InvalidControlReceiptPosition };
        if (replay) |supplied| if (!std.meta.eql(supplied, position)) return error.InvalidPrivateIndexNativeReceipt;
        const record: Record = .{ .intent = intent, .position = position };
        const encoded = try record.encode();
        try @import("relational_integrity_topology.zig").stageInstalledIndexRelease(alloc, txn, intent.installed);
        const sequence = try authority.advance(txn, namespace, if (replay) |supplied| supplied.sequence else null);
        std.debug.assert(sequence == position.sequence);
        try txn.put(key, encoded.slice());
        try txn.put(retirement_pending_key, &try (positions.Position{ .native = position }).encode());
        // These are private retry/proof slots, never primary/index rows or live
        // UNIQUE claims. Preserve schema-version history and active progress.
        for ([_][]const u8{ owner.key, @import("relational_index_inputs.zig").key, validation.Installed.key, validation.Seal.key, stream.head_key, stream.consumption_key, @import("relational_index_owner_control.zig").PreparedBuild.checkpoint_key }) |retired| try txn.delete(retired);
        for ([_]owner.NativeEffect{ .admitted, .fenced, .reserved }) |effect| try txn.delete(&owner.nativeEffectKey(effect));
        try txn.delete(&claims.InventoryValidation.key(binding));
        try txn.delete(&validation.key(binding));
        try txn.delete(&claims.registrationKey(expected.generation));
        const physical = @import("relational_index_catalog.zig");
        if (!std.mem.eql(u8, &binding.physical_reserved.blob_digest, &binding.physical_candidate.blob_digest)) try txn.delete(&physical.blobKey(binding.physical_reserved.blob_digest));
        return .{ .record = record, .duplicate = false };
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
