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

//! Private parent-owner FK generation publication boundary. The request is
//! only an identity/probe; the receiving owner must fetch its own exact,
//! linearizable metadata decision before proposing a replicated transition.
const std = @import("std");
const operation = @import("antfly_local_sources").api_operation;
const callback_abi = @import("antfly_local_sources").runtime_callback_abi;
const integrity = @import("antfly_local_sources").storage_db_relational_integrity_contract;
const identity = @import("antfly_local_sources").storage_db_doc_identity_namespace;

const receipt_position = @import("antfly_local_sources").storage_db_receipt_position;
const persistence = @import("antfly_local_sources").common_topology_records.RelationPersistence;
pub const NativeBoot = struct {
    generation: u64,
    instance: [16]u8,
    pub fn requireCurrent(self: NativeBoot, current: @import("../metadata/sql_relation_boots.zig").Record) !void {
        try current.validate();
        if (current.phase != .ready or self.generation != current.generation or !std.mem.eql(u8, &self.instance, &current.instance)) return error.InvalidGenerationPublication;
    }
};
pub const NativeProof = @import("../metadata/store_root_attestation.zig").ProofFor("antfly/fk-generation-native-receipt/signature/v1", "antfly/fk-generation-native-receipt/accepted/v1");

fn receiptPosition(receipt: anytype) !receipt_position.Position {
    const stamp = try receipt_position.fromFields(receipt.applied_term, receipt.applied_index, receipt.native_position);
    if ((receipt.native_position == null) != (receipt.native_proof == null) or (receipt.native_position == null) != (receipt.native_descriptor_fingerprint == null) or (receipt.native_position == null) != (receipt.native_boot == null)) return error.InvalidGenerationPublication;
    if (receipt.native_boot) |boot| if (boot.generation == 0 or std.mem.allEqual(u8, &boot.instance, 0)) return error.InvalidGenerationPublication;
    if (receipt.native_proof) |proof| {
        if (proof.authority != .native or proof.native_pin == null) return error.InvalidGenerationPublication;
        try stamp.requireNamespace(.{ .table_id = proof.native_pin.?.table_id, .shard_id = proof.native_pin.?.shard_id, .range_id = proof.native_pin.?.range_id });
        try proof.verify(receipt.digest());
    }
    return stamp;
}

fn hashNativePosition(hash: *std.crypto.hash.Blake3, native: receipt_position.Native) void {
    var bytes: [33]u8 = undefined;
    bytes[0] = 2;
    std.mem.writeInt(u64, bytes[1..9], native.namespace.table_id, .little);
    std.mem.writeInt(u64, bytes[9..17], native.namespace.shard_id, .little);
    std.mem.writeInt(u64, bytes[17..25], native.namespace.range_id, .little);
    std.mem.writeInt(u64, bytes[25..33], native.sequence, .little);
    hash.update(&bytes);
}

pub fn requireCapturedPosition(receipt: anytype, descriptor: ?persistence.Descriptor, namespace: identity.Namespace) !void {
    const stamp = try receiptPosition(receipt);
    try stamp.requireNamespace(namespace);
    if (descriptor) |native| {
        const proof = receipt.native_proof orelse return error.InvalidGenerationPublication;
        const pin = proof.native_pin orelse return error.InvalidGenerationPublication;
        const fingerprint = receipt.native_descriptor_fingerprint orelse return error.InvalidGenerationPublication;
        if (!std.mem.eql(u8, &fingerprint, &try native.fingerprint())) return error.InvalidGenerationPublication;
        if (stamp != .native or pin.data_generation != native.data_generation or pin.table_id != namespace.table_id or pin.shard_id != namespace.shard_id or pin.range_id != namespace.range_id or
            proof.identity.node_id != native.owner_node_id or proof.identity.store_id != native.owner_store_id) return error.InvalidGenerationPublication;
    } else if (stamp != .raft or receipt.native_proof != null) return error.InvalidGenerationPublication;
}

pub const Action = enum { stage, activate, acknowledge, cancel };

/// A child owner first fences old-generation writes, then installs the exact
/// metadata-published successor schema before reopening admission. These are
/// identity-only requests; the owner obtains the current plan at read-index.
pub const SourceAction = enum { fence, install, cancel };

/// Initial FK-bearing CREATE has no prior child catalog. Metadata keeps the
/// candidate and owner descriptors hidden until every parent has accepted the
/// new generation and each child owner has a durable provision/release proof.
pub const InitialChildAction = enum { provision, release, cancel };

pub const InitialChildRequest = struct {
    plan_id: [16]u8,
    child_table_id: u64,
    child_table_name: []const u8,
    child_group_id: u64,
    action: InitialChildAction,

    pub fn validate(self: InitialChildRequest, group_id: u64) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or self.child_table_id == 0 or
            self.child_group_id == 0 or self.child_group_id != group_id or
            self.child_table_name.len == 0 or self.child_table_name.len > 256 or
            !std.unicode.utf8ValidateSlice(self.child_table_name)) return error.InvalidGenerationPublication;
    }
};

pub const InitialChildReceipt = struct {
    root_attestation: ?@import("../metadata/initial_child_root_attestation.zig").Attestation = null,
    plan_id: [16]u8,
    child_table_id: u64,
    child_group_id: u64,
    action: InitialChildAction,
    namespace: identity.Namespace,
    plan_digest: integrity.Digest,
    schema_version: u32,
    schema_digest: integrity.Digest,
    public_schema_json_digest: integrity.Digest,
    catalog_digest: integrity.Digest,
    row_count: u64,
    applied_term: u64,
    applied_index: u64,

    pub fn validate(self: InitialChildReceipt, request: InitialChildRequest) !void {
        if (self.root_attestation) |proof| try proof.verify(self.digest());
        if (!std.mem.eql(u8, &self.plan_id, &request.plan_id) or
            self.child_table_id != request.child_table_id or
            self.child_group_id != request.child_group_id or
            self.action != request.action or
            self.namespace.table_id != request.child_table_id or
            self.namespace.shard_id == 0 or self.namespace.range_id == 0 or
            self.row_count != 0 or
            self.applied_term == 0 or self.applied_index == 0 or
            std.mem.allEqual(u8, &self.plan_digest, 0) or
            std.mem.allEqual(u8, &self.schema_digest, 0) or
            std.mem.allEqual(u8, &self.public_schema_json_digest, 0) or
            std.mem.allEqual(u8, &self.catalog_digest, 0)) return error.InvalidGenerationPublication;
    }

    pub fn digest(self: InitialChildReceipt) integrity.Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly initial FK child owner receipt v1");
        hash.update(&self.plan_id);
        hash.update(@tagName(self.action));
        var buf: [8]u8 = undefined;
        inline for (.{ self.child_table_id, self.child_group_id, self.namespace.table_id, self.namespace.shard_id, self.namespace.range_id, self.row_count, self.applied_term, self.applied_index }) |value| {
            std.mem.writeInt(u64, &buf, value, .little);
            hash.update(&buf);
        }
        std.mem.writeInt(u32, buf[0..4], self.schema_version, .little);
        hash.update(buf[0..4]);
        hash.update(&self.plan_digest);
        hash.update(&self.schema_digest);
        hash.update(&self.public_schema_json_digest);
        hash.update(&self.catalog_digest);
        var result: integrity.Digest = undefined;
        hash.final(&result);
        return result;
    }
};

pub const SourceRequest = struct {
    plan_id: [16]u8,
    child_table_id: u64,
    child_table_name: []const u8,
    child_group_id: u64,
    action: SourceAction,

    pub fn validate(self: SourceRequest, group_id: u64) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or self.child_table_id == 0 or
            self.child_group_id == 0 or self.child_group_id != group_id or
            self.child_table_name.len == 0 or self.child_table_name.len > 256 or
            !std.unicode.utf8ValidateSlice(self.child_table_name))
            return error.InvalidGenerationPublication;
    }
};

pub const SourceReceipt = struct {
    document_source: bool = false,
    plan_id: [16]u8,
    child_table_id: u64,
    child_group_id: u64,
    action: SourceAction,
    plan_digest: integrity.Digest,
    fence_digest: integrity.Digest,
    before_schema_version: u32,
    before_schema_digest: integrity.Digest,
    before_catalog_digest: integrity.Digest,
    after_schema_version: u32,
    after_schema_digest: integrity.Digest,
    after_catalog_digest: integrity.Digest,
    applied_term: u64,
    applied_index: u64,
    native_position: ?receipt_position.Native = null,
    native_proof: ?NativeProof = null,
    native_descriptor_fingerprint: ?[32]u8 = null,
    native_boot: ?NativeBoot = null,

    reference_coverage_ready: bool = false,
    pub fn nativeJsonSkipField(self: @This(), comptime field: []const u8) bool {
        return (std.mem.eql(u8, field, "native_position") and self.native_position == null) or
            (std.mem.eql(u8, field, "native_proof") and self.native_proof == null) or
            (std.mem.eql(u8, field, "native_descriptor_fingerprint") and self.native_descriptor_fingerprint == null) or
            (std.mem.eql(u8, field, "native_boot") and self.native_boot == null) or
            (std.mem.eql(u8, field, "reference_coverage_ready") and !self.reference_coverage_ready) or
            (std.mem.eql(u8, field, "document_source") and !self.document_source);
    }
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |field| {
            if (!self.nativeJsonSkipField(field)) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }

    pub fn validate(self: SourceReceipt, request: SourceRequest) !void {
        _ = try self.position();
        if (self.reference_coverage_ready and self.action != .install) return error.InvalidGenerationPublication;
        if (!std.mem.eql(u8, &self.plan_id, &request.plan_id) or
            self.child_table_id != request.child_table_id or
            self.child_group_id != request.child_group_id or
            self.action != request.action or
            (if (self.document_source) self.action != .fence or self.after_schema_version != self.before_schema_version or
                !std.mem.eql(u8, &self.before_schema_digest, &self.after_schema_digest) or
                !std.mem.eql(u8, &self.before_catalog_digest, &self.after_catalog_digest) else self.after_schema_version <= self.before_schema_version) or
            std.mem.allEqual(u8, &self.plan_digest, 0) or
            std.mem.allEqual(u8, &self.fence_digest, 0) or
            std.mem.allEqual(u8, &self.before_schema_digest, 0) or
            std.mem.allEqual(u8, &self.after_schema_digest, 0) or
            std.mem.allEqual(u8, &self.before_catalog_digest, 0) or
            std.mem.allEqual(u8, &self.after_catalog_digest, 0))
            return error.InvalidGenerationPublication;
    }

    pub fn position(self: @This()) !receipt_position.Position {
        return receiptPosition(self);
    }

    pub fn digest(self: SourceReceipt) integrity.Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly child FK generation publication receipt v1");
        hash.update(&self.plan_id);
        hash.update(@tagName(self.action));
        var buf: [8]u8 = undefined;
        inline for (.{ self.child_table_id, self.child_group_id, self.applied_term, self.applied_index }) |value| {
            std.mem.writeInt(u64, &buf, value, .little);
            hash.update(&buf);
        }
        inline for (.{ self.before_schema_version, self.after_schema_version }) |value| {
            std.mem.writeInt(u32, buf[0..4], value, .little);
            hash.update(buf[0..4]);
        }
        hash.update(&self.plan_digest);
        hash.update(&self.fence_digest);
        hash.update(&self.before_schema_digest);
        hash.update(&self.before_catalog_digest);
        hash.update(&self.after_schema_digest);
        hash.update(&self.after_catalog_digest);
        if (self.document_source) hash.update("document-retirement-source/v1");
        if (self.reference_coverage_ready) hash.update("reference-coverage-ready/v1");
        if (self.native_position) |native| {
            hash.update("native-fk-position/v1\x00");
            hashNativePosition(&hash, native);
            if (self.native_descriptor_fingerprint) |fingerprint| hash.update(&fingerprint);
            if (self.native_boot) |boot| {
                var generation: [8]u8 = undefined;
                std.mem.writeInt(u64, &generation, boot.generation, .little);
                hash.update(&generation);
                hash.update(&boot.instance);
            }
        }
        var result: integrity.Digest = undefined;
        hash.final(&result);
        return result;
    }
};

pub const Request = struct {
    plan_id: [16]u8,
    parent_table_id: u64,
    parent_group_id: u64,
    child_table_id: u64,
    child_table_name: []const u8,
    action: Action,

    pub fn validate(self: Request, group_id: u64) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or self.parent_table_id == 0 or
            self.parent_group_id == 0 or self.parent_group_id != group_id or
            self.child_table_id == 0 or self.child_table_name.len == 0 or
            self.child_table_name.len > 256 or !std.unicode.utf8ValidateSlice(self.child_table_name)) return error.InvalidGenerationPublication;
    }
};

pub const Receipt = struct {
    plan_id: [16]u8,
    parent_table_id: u64,
    parent_group_id: u64,
    child_table_id: u64,
    action: Action,
    transitions_digest: integrity.Digest,
    decision_digest: integrity.Digest,
    applied_term: u64,
    applied_index: u64,
    native_position: ?receipt_position.Native = null,
    native_proof: ?NativeProof = null,
    native_descriptor_fingerprint: ?[32]u8 = null,
    native_boot: ?NativeBoot = null,

    reference_coverage_ready: bool = false,
    pub fn nativeJsonSkipField(self: @This(), comptime field: []const u8) bool {
        return (std.mem.eql(u8, field, "native_position") and self.native_position == null) or
            (std.mem.eql(u8, field, "native_proof") and self.native_proof == null) or
            (std.mem.eql(u8, field, "native_descriptor_fingerprint") and self.native_descriptor_fingerprint == null) or
            (std.mem.eql(u8, field, "native_boot") and self.native_boot == null) or
            (std.mem.eql(u8, field, "reference_coverage_ready") and !self.reference_coverage_ready);
    }
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |field| {
            if (!self.nativeJsonSkipField(field)) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }

    pub fn validate(self: Receipt, request: Request) !void {
        _ = try self.position();
        if (self.reference_coverage_ready and self.action != .acknowledge) return error.InvalidGenerationPublication;
        if (!std.mem.eql(u8, &self.plan_id, &request.plan_id) or
            self.parent_table_id != request.parent_table_id or
            self.parent_group_id != request.parent_group_id or
            self.child_table_id != request.child_table_id or
            self.action != request.action or
            std.mem.allEqual(u8, &self.transitions_digest, 0) or
            std.mem.allEqual(u8, &self.decision_digest, 0))
            return error.InvalidGenerationPublication;
    }

    pub fn position(self: @This()) !receipt_position.Position {
        return receiptPosition(self);
    }

    pub fn digest(self: Receipt) integrity.Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly parent FK generation publication receipt v1");
        hash.update(&self.plan_id);
        var buf: [8]u8 = undefined;
        inline for (.{ self.parent_table_id, self.parent_group_id, self.child_table_id, self.applied_term, self.applied_index }) |value| {
            std.mem.writeInt(u64, &buf, value, .little);
            hash.update(&buf);
        }
        hash.update(@tagName(self.action));
        hash.update(&self.transitions_digest);
        hash.update(&self.decision_digest);
        if (self.reference_coverage_ready) hash.update("reference-coverage-ready/v1");
        if (self.native_position) |native| {
            hash.update("native-fk-position/v1\x00");
            hashNativePosition(&hash, native);
            if (self.native_descriptor_fingerprint) |fingerprint| hash.update(&fingerprint);
            if (self.native_boot) |boot| {
                var generation: [8]u8 = undefined;
                std.mem.writeInt(u64, &generation, boot.generation, .little);
                hash.update(&generation);
                hash.update(&boot.instance);
            }
        }
        var result: integrity.Digest = undefined;
        hash.final(&result);
        return result;
    }
};

pub const Port = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, Request, operation.RequestContext) anyerror!Receipt,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    const VTable = struct { execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, Request, operation.RequestContext) anyerror!Receipt };
    const BoundaryAbi = callback_abi.Boundary(VTable);

    pub fn execute(self: Port, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, request: Request, context: operation.RequestContext) !Receipt {
        try context.ensureActive();
        try request.validate(group_id);
        const receipt = try BoundaryAbi.call("execute_fn", self.boundary_dispatch, self.execute_fn, .{ self.ptr, alloc, table_name, group_id, request, context });
        try receipt.validate(request);
        return receipt;
    }
};

pub const SourcePort = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, SourceRequest, operation.RequestContext) anyerror!SourceReceipt,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    const VTable = struct { execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, SourceRequest, operation.RequestContext) anyerror!SourceReceipt };
    const BoundaryAbi = callback_abi.Boundary(VTable);

    pub fn execute(self: SourcePort, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, request: SourceRequest, context: operation.RequestContext) !SourceReceipt {
        try context.ensureActive();
        try request.validate(group_id);
        const receipt = try BoundaryAbi.call("execute_fn", self.boundary_dispatch, self.execute_fn, .{ self.ptr, alloc, table_name, group_id, request, context });
        try receipt.validate(request);
        return receipt;
    }
};

pub const InitialChildPort = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, InitialChildRequest, operation.RequestContext) anyerror!InitialChildReceipt,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    const VTable = struct { execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, InitialChildRequest, operation.RequestContext) anyerror!InitialChildReceipt };
    const BoundaryAbi = callback_abi.Boundary(VTable);

    pub fn execute(self: InitialChildPort, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, request: InitialChildRequest, context: operation.RequestContext) !InitialChildReceipt {
        try context.ensureActive();
        try request.validate(group_id);
        const receipt = try BoundaryAbi.call("execute_fn", self.boundary_dispatch, self.execute_fn, .{ self.ptr, alloc, table_name, group_id, request, context });
        try receipt.validate(request);
        return receipt;
    }
};

test "FK generation private owner request binds physical owner and receipt transition" {
    const req: Request = .{ .plan_id = @splat(1), .parent_table_id = 7, .parent_group_id = 9, .child_table_id = 11, .child_table_name = "children", .action = .stage };
    try req.validate(9);
    try std.testing.expectError(error.InvalidGenerationPublication, req.validate(10));
    const receipt: Receipt = .{ .plan_id = req.plan_id, .parent_table_id = req.parent_table_id, .parent_group_id = req.parent_group_id, .child_table_id = req.child_table_id, .action = .stage, .transitions_digest = @splat(2), .decision_digest = @splat(4), .applied_term = 5, .applied_index = 6 };
    try receipt.validate(req);
    var changed = receipt;
    changed.transitions_digest = @splat(9);
    const first = receipt.digest();
    const second = changed.digest();
    try std.testing.expect(!std.mem.eql(u8, &first, &second));
}

test "FK generation child source request and receipt bind schema cut" {
    const req: SourceRequest = .{ .plan_id = @splat(1), .child_table_id = 7, .child_table_name = "children", .child_group_id = 9, .action = .fence };
    try req.validate(9);
    try std.testing.expectError(error.InvalidGenerationPublication, req.validate(10));
    const receipt: SourceReceipt = .{
        .plan_id = req.plan_id,
        .child_table_id = req.child_table_id,
        .child_group_id = req.child_group_id,
        .action = .fence,
        .plan_digest = @splat(2),
        .fence_digest = @splat(3),
        .before_schema_version = 1,
        .before_schema_digest = @splat(4),
        .before_catalog_digest = @splat(6),
        .after_schema_version = 2,
        .after_schema_digest = @splat(5),
        .after_catalog_digest = @splat(7),
        .applied_term = 6,
        .applied_index = 7,
    };
    try receipt.validate(req);
    // A newly created SQL table has native schema version zero. Its first
    // FK generation is the valid 0 -> 1 schema cut.
    var initial = receipt;
    initial.before_schema_version = 0;
    initial.after_schema_version = 1;
    try initial.validate(req);
    const wire = try std.json.Stringify.valueAlloc(std.testing.allocator, initial, .{});
    defer std.testing.allocator.free(wire);
    var decoded = try std.json.parseFromSlice(SourceReceipt, std.testing.allocator, wire, .{});
    defer decoded.deinit();
    try decoded.value.validate(req);
    try std.testing.expectEqual(initial.digest(), decoded.value.digest());
    initial.after_schema_version = 0;
    try std.testing.expectError(error.InvalidGenerationPublication, initial.validate(req));
    var changed = receipt;
    changed.after_schema_digest = @splat(9);
    const first = receipt.digest();
    const second = changed.digest();
    try std.testing.expect(!std.mem.eql(u8, &first, &second));
}

test "initial FK child receipt binds hidden owner and empty state" {
    const request: InitialChildRequest = .{ .plan_id = @splat(1), .child_table_id = 7, .child_table_name = "children", .child_group_id = 9, .action = .provision };
    try request.validate(9);
    try std.testing.expectError(error.InvalidGenerationPublication, request.validate(10));
    const receipt: InitialChildReceipt = .{
        .plan_id = request.plan_id,
        .child_table_id = request.child_table_id,
        .child_group_id = request.child_group_id,
        .action = .provision,
        .namespace = .{ .table_id = 7, .shard_id = 11, .range_id = 13 },
        .plan_digest = @splat(2),
        .schema_version = 1,
        .schema_digest = @splat(3),
        .public_schema_json_digest = @splat(4),
        .catalog_digest = @splat(5),
        .row_count = 0,
        .applied_term = 1,
        .applied_index = 2,
    };
    try receipt.validate(request);
    var first_epoch = receipt;
    first_epoch.schema_version = 0;
    try first_epoch.validate(request);
    var changed = receipt;
    changed.row_count = 1;
    try std.testing.expectError(error.InvalidGenerationPublication, changed.validate(request));
    changed = receipt;
    changed.catalog_digest = @splat(6);
    const first = receipt.digest();
    const second = changed.digest();
    try std.testing.expect(!std.mem.eql(u8, &first, &second));
}

test "FK generation schema Raft command keeps canonical JSON bytes on wire" {
    const topology = @import("antfly_local_sources").storage_db_relational_integrity_topology_contract;
    const schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}";
    const command: topology.Command = .{
        .action = .install_child_schema,
        .fence = .{
            .role = .child_generation_source,
            .transition_id = 7,
            .attempt = 8,
            .admission_epoch = 9,
            .peer_group_id = 10,
            .owner_group_id = 10,
            .namespace = .{ .table_id = 11, .shard_id = 12, .range_id = 13 },
            .catalog_digest = @splat(14),
        },
        .child_schema_install = .{
            .schema_json = schema_json,
            .before_schema_json_digest = @splat(18),
            .schema_json_digest = @splat(15),
            .before_catalog_digest = @splat(16),
            .after_catalog_digest = @splat(17),
        },
    };
    const alloc = std.testing.allocator;
    const encoded = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\\\"version\\\"") != null);
    var parsed = try std.json.parseFromSlice(topology.Command, alloc, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings(schema_json, parsed.value.child_schema_install.?.schema_json);
}

test "SQL relation lifetime FK receipts bind actual native position descriptor and enrolled signer" {
    const a = std.testing.allocator;
    const seed: [32]u8 = @splat(7);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: @import("antfly_local_sources").storage_db_root_signing_identity.State = .{ .root_incarnation = 11, .seed = seed, .public_key = pair.public_key.toBytes() };
    const native: persistence.Descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(1), .data_generation = 3 };
    const pin: persistence.PhysicalPin = .{ .physical_root_incarnation = 99, .table_id = 7, .shard_id = 17, .range_id = 27, .data_generation = 3 };
    const enrolled: @import("../metadata/store_root_enrollment.zig").Identity = .{ .metadata_incarnation = @splat('a'), .node_id = 9, .store_id = 11, .root_incarnation = root.root_incarnation, .public_key = root.public_key };
    const template: NativeProof = .{ .identity = enrolled, .reporter_incarnation = 3, .authority = .{ .native = try @import("../metadata/sql_relation_attestation.zig").nativeAuthorityDigest(enrolled, pin) }, .native_pin = pin, .root_generation = 1, .signature = @splat(0) };
    const request: SourceRequest = .{ .plan_id = @splat(2), .child_table_id = 7, .child_table_name = "native", .child_group_id = 17, .action = .fence };
    var receipt: SourceReceipt = .{ .plan_id = request.plan_id, .child_table_id = 7, .child_group_id = 17, .action = .fence, .plan_digest = @splat(3), .fence_digest = @splat(4), .before_schema_version = 1, .before_schema_digest = @splat(5), .before_catalog_digest = @splat(6), .after_schema_version = 2, .after_schema_digest = @splat(7), .after_catalog_digest = @splat(8), .applied_term = 0, .applied_index = 0, .native_position = .{ .namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 27 }, .sequence = 9 }, .native_descriptor_fingerprint = try native.fingerprint(), .native_boot = .{ .generation = 2, .instance = @splat(1) } };
    receipt.native_proof = try template.sign(root, receipt.digest());
    try receipt.validate(request);
    try requireCapturedPosition(receipt, native, receipt.native_position.?.namespace);
    var current_boot: @import("../metadata/sql_relation_boots.zig").Record = .{ .node_id = enrolled.node_id, .identity = enrolled, .reporter_incarnation = template.reporter_incarnation, .generation = 2, .instance = @splat(1), .claim_digest = @splat(2), .unclean_restart = false, .phase = .ready };
    try receipt.native_boot.?.requireCurrent(current_boot);
    current_boot.generation += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, receipt.native_boot.?.requireCurrent(current_boot));
    current_boot.generation -= 1;
    current_boot.instance = @splat(9);
    try std.testing.expectError(error.InvalidGenerationPublication, receipt.native_boot.?.requireCurrent(current_boot));
    current_boot.instance = @splat(1);
    current_boot.phase = .draining;
    try std.testing.expectError(error.InvalidGenerationPublication, receipt.native_boot.?.requireCurrent(current_boot));
    const bytes = try std.json.Stringify.valueAlloc(a, receipt, .{});
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(SourceReceipt, a, bytes, .{});
    defer parsed.deinit();
    try parsed.value.validate(request);
    var altered = receipt;
    altered.native_position.?.sequence += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, altered.validate(request));
    altered = receipt;
    altered.applied_term = 1;
    try std.testing.expectError(error.InvalidControlReceiptPosition, altered.validate(request));
    altered = receipt;
    altered.native_descriptor_fingerprint.?[0] ^= 1;
    try std.testing.expectError(error.InvalidGenerationPublication, altered.validate(request));
    var successor = native;
    successor.generation_instance = @splat(9);
    try std.testing.expectError(error.InvalidGenerationPublication, requireCapturedPosition(receipt, successor, receipt.native_position.?.namespace));
    try std.testing.expectError(error.InvalidGenerationPublication, requireCapturedPosition(receipt, null, receipt.native_position.?.namespace));
    var ordinary = receipt;
    ordinary.native_position = null;
    ordinary.native_proof = null;
    ordinary.native_descriptor_fingerprint = null;
    ordinary.native_boot = null;
    ordinary.applied_term = 1;
    ordinary.applied_index = 2;
    try ordinary.validate(request);
    const previous = try std.json.Stringify.valueAlloc(a, .{ .plan_id = ordinary.plan_id, .child_table_id = ordinary.child_table_id, .child_group_id = ordinary.child_group_id, .action = ordinary.action, .plan_digest = ordinary.plan_digest, .fence_digest = ordinary.fence_digest, .before_schema_version = ordinary.before_schema_version, .before_schema_digest = ordinary.before_schema_digest, .before_catalog_digest = ordinary.before_catalog_digest, .after_schema_version = ordinary.after_schema_version, .after_schema_digest = ordinary.after_schema_digest, .after_catalog_digest = ordinary.after_catalog_digest, .applied_term = ordinary.applied_term, .applied_index = ordinary.applied_index }, .{});
    defer a.free(previous);
    const current = try std.json.Stringify.valueAlloc(a, ordinary, .{});
    defer a.free(current);
    try std.testing.expectEqualStrings(previous, current);
}

/// Private source-control result: the actual fence was installed, but its
/// native transaction/lock custody is still draining. This grants no receipt.
pub const source_draining_error_json = "{\"error_code\":\"transaction_topology_busy\"}";
pub fn isSourceDrainingFailure(status: u16, body: []const u8) bool {
    return status == 503 and std.mem.eql(u8, body, source_draining_error_json);
}
test "SQL relation lifetime source draining transport preserves only exact actual busy diagnosis" {
    try std.testing.expect(isSourceDrainingFailure(503, source_draining_error_json));
    try std.testing.expect(!isSourceDrainingFailure(200, source_draining_error_json));
    try std.testing.expect(!isSourceDrainingFailure(503, "FK generation source unavailable"));
    try std.testing.expect(!isSourceDrainingFailure(503, "{\"error_code\":\"another_failure\"}"));
}

pub fn requireDrainedSource(status: @import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Status, expected: @import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Fence) !void {
    if (status.fence == null or !status.fence.?.eql(expected)) return error.GenerationAdmissionPending;
    if (!status.drained) return error.TransactionTopologyBusy;
}
test "SQL relation lifetime source draining requires the exact installed fence before exposing cleanup" {
    const topology = @import("antfly_local_sources").storage_db_relational_integrity_topology_contract;
    const fence: topology.Fence = .{ .transition_id = 1, .attempt = 1, .peer_group_id = 2, .owner_group_id = 2, .role = .document_retirement_source, .namespace = .{ .table_id = 3, .shard_id = 2, .range_id = 2 }, .catalog_digest = @splat(4) };
    try std.testing.expectError(error.GenerationAdmissionPending, requireDrainedSource(.{ .fence = null, .drained = false }, fence));
    var wrong = fence;
    wrong.attempt += 1;
    try std.testing.expectError(error.GenerationAdmissionPending, requireDrainedSource(.{ .fence = wrong, .drained = false }, fence));
    try std.testing.expectError(error.TransactionTopologyBusy, requireDrainedSource(.{ .fence = fence, .drained = false }, fence));
    try requireDrainedSource(.{ .fence = fence, .drained = true }, fence);
}
