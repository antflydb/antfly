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

//! Private native index effects and their owned preparation. Receiving server
//! workers must independently obtain live metadata permission; these local
//! objects neither authenticate a coordinator nor enable a public producer.
const std = @import("std");
const jobs = @import("relational_index_owner_job.zig");
const inputs = @import("relational_index_inputs.zig");
const candidate_mod = @import("relational_index_candidate.zig");
const physical = @import("relational_index_catalog.zig");
const registry = @import("schema_registry.zig");
const schema_api = @import("../../schema/mod.zig");
const source_authority = @import("../source_authority.zig");
const Native = @import("receipt_position.zig").Native;
const Allocator = std.mem.Allocator;
pub const envelope_version = 22;
pub const max_encoded_inputs = std.base64.standard.Encoder.calcSize(inputs.max_bytes);
pub const max_wire_bytes = max_encoded_inputs + 32 * 1024;
pub const Manifest = inputs.Prepared;

pub const Admission = struct {
    id: [16]u8,
    plan_digest: [32]u8,
    owner_digest: [32]u8,
    ordinal: u16,
    fence: @import("relational_integrity_topology.zig").Fence,
    candidate_digest: [32]u8,
    reservation_digest: [32]u8,
};

/// Metadata's immutable owner identity, without physical allocation choices or
/// schema-sized inputs. Status is information, never publication permission.
pub const Identity = struct {
    id: [16]u8,
    plan_digest: [32]u8,
    owner_digest: [32]u8,
    ordinal: u16,
    namespace: @import("doc_identity.zig").Namespace,
    owner_group_id: u64,

    pub fn validate(self: Identity) !void {
        if (std.mem.allEqual(u8, &self.id, 0) or std.mem.allEqual(u8, &self.plan_digest, 0) or
            std.mem.allEqual(u8, &self.owner_digest, 0) or self.owner_group_id == 0 or
            self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0 or
            self.ordinal >= @import("relational_index_publication_progress.zig").max_owners) return error.InvalidPrivateIndexOwnerJob;
    }
};

pub const Command = struct {
    effect: jobs.NativeEffect,
    binding: jobs.Binding,
    inputs_b64: ?[]const u8 = null,

    pub fn validate(self: Command) !void {
        _ = try self.binding.encode();
        if (self.binding.authority != .native or self.binding.phase != .fencing or
            ((self.effect == .admitted) != (self.inputs_b64 != null))) return error.InvalidPrivateIndexOwnerJob;
        if (self.inputs_b64) |bytes| if (bytes.len == 0 or bytes.len > max_encoded_inputs) return error.InvalidPrivateIndexInputs;
    }
};

/// Recover only a previously committed publication effect. No physical binding,
/// candidate digest or replay position is accepted from a receipt caller.
pub const ReceiptRequest = struct {
    identity: Identity,
    effect: enum { fenced, reserved },
};

pub const Frame = struct {
    command: Command,
    position: Native,

    pub fn validate(self: Frame) !void {
        try self.command.validate();
        try self.position.validate();
        if (!self.position.namespace.eql(self.command.binding.fence.namespace)) return error.IdentityNamespaceMismatch;
    }
};

pub const Outcome = struct {
    binding: jobs.Binding,
    position: Native,
    duplicate: bool,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,
    receipt: ?@import("relational_index_publication_progress.zig").Receipt = null,
};

const Artifacts = struct {
    candidate: candidate_mod.Candidate,
    addition: physical.PrivateAddition,

    fn deinit(self: *Artifacts) void {
        self.addition.deinit();
        self.candidate.deinit();
    }

    fn init(alloc: Allocator, before: registry.SchemaView, txn: anytype, bytes: []const u8) !Artifacts {
        const view = try inputs.decode(bytes);
        const namespace = (try @import("doc_identity.zig").loadNamespaceTxn(txn)) orelse return error.IdentityNamespaceMismatch;
        _ = try source_authority.require(txn, .native, @import("online_source_contract.zig").namespaceBytes(namespace));
        var candidate = try candidate_mod.Candidate.prepare(alloc, namespace.table_id, view.before_json, view.after_json, view.predecessor, view.name);
        errdefer candidate.deinit();
        var after_schema = try schema_api.parseValidatedTableSchema(alloc, view.after_json);
        defer after_schema.deinit(alloc);
        const runtime = try schema_api.deriveRuntimeTableSchema(alloc, after_schema);
        const epoch = registry.Epoch.createOwned(alloc, runtime) catch |err| {
            @import("../schema.zig").freeSchema(alloc, runtime);
            return err;
        };
        var after: registry.SchemaView = .{ .epoch = epoch };
        defer after.release();
        var declaration_arena = std.heap.ArenaAllocator.init(alloc);
        defer declaration_arena.deinit();
        const definitions = (try after_schema.relationalIndexDefinitions(declaration_arena.allocator())) orelse return error.InvalidIndexPublicationCandidate;
        var existing = try physical.load(alloc, txn);
        defer if (existing) |*value| value.deinit();
        const job = try jobs.load(txn);
        const addition = if (job != null and job.?.phase == .reserved)
            try jobs.reopenPhysical(alloc, txn, before, after, view.name)
        else
            try physical.PrivateAddition.init(alloc, before, after, if (existing) |*value| value else null, definitions, view.name);
        return .{ .candidate = candidate, .addition = addition };
    }
};

pub const Prepared = struct {
    pub const max_manifest_bytes = inputs.max_bytes;

    alloc: Allocator,
    command: Command,
    artifacts: ?Artifacts = null,
    payload: ?[]u8 = null,
    prefix_len: usize = 0,

    pub fn deinit(self: *Prepared) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        if (self.command.inputs_b64) |bytes| self.alloc.free(bytes);
        if (self.artifacts) |*value| value.deinit();
        self.* = undefined;
    }

    /// Physical allocation comes from the actual local immutable catalog, not
    /// from a coordinator-selected counter. Compile before taking the apply lock.
    pub fn admission(alloc: Allocator, before: registry.SchemaView, txn: anytype, header: Admission, manifest: []const u8) !Prepared {
        var artifacts = try Artifacts.init(alloc, before, txn, manifest);
        errdefer artifacts.deinit();
        if (!std.mem.eql(u8, &inputs.digest(artifacts.candidate.next.value), &header.candidate_digest) or
            !std.mem.eql(u8, &inputs.digest(artifacts.candidate.reservation.value), &header.reservation_digest)) return error.InvalidIndexPublicationCandidate;
        const binding = try jobs.prepare(header.id, header.plan_digest, header.owner_digest, header.ordinal, header.fence, .native, &artifacts.candidate, &artifacts.addition);
        const b64 = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(manifest.len));
        _ = std.base64.standard.Encoder.encode(b64, manifest);
        return .{ .alloc = alloc, .command = .{ .effect = .admitted, .binding = binding, .inputs_b64 = b64 }, .artifacts = artifacts };
    }

    pub fn control(alloc: Allocator, before: registry.SchemaView, txn: anytype, command: Command) !Prepared {
        try command.validate();
        var prepared: Prepared = .{ .alloc = alloc, .command = command };
        prepared.command.inputs_b64 = null;
        errdefer prepared.deinit();
        if (command.inputs_b64) |b64| {
            const length = std.base64.standard.Decoder.calcSizeForSlice(b64) catch return error.InvalidPrivateIndexInputs;
            if (length > inputs.max_bytes) return error.InvalidPrivateIndexInputs;
            const bytes = try alloc.alloc(u8, length);
            defer alloc.free(bytes);
            std.base64.standard.Decoder.decode(bytes, b64) catch return error.InvalidPrivateIndexInputs;
            if (!std.mem.eql(u8, &inputs.digest(bytes), &command.binding.inputs_digest)) return error.InvalidPrivateIndexInputs;
            prepared.artifacts = try Artifacts.init(alloc, before, txn, bytes);
            prepared.command.inputs_b64 = try alloc.dupe(u8, b64);
        } else if (command.effect == .reserved) {
            var manifest = try inputs.Prepared.load(alloc, txn, command.binding.inputs_digest);
            defer manifest.deinit();
            prepared.artifacts = try Artifacts.init(alloc, before, txn, manifest.bytes);
        }
        if (prepared.artifacts) |*artifacts| {
            const b = command.binding;
            const actual = try jobs.prepare(b.id, b.plan_digest, b.owner_digest, b.ordinal, b.fence, .native, &artifacts.candidate, &artifacts.addition);
            if (!std.mem.eql(u8, &try actual.encode(), &try b.encode())) return error.PrivateIndexOwnerJobChanged;
        }
        return prepared;
    }

    /// Serialize the large immutable prefix off-lock. Only the locally assigned
    /// sequence (at most 20 digits) is filled after transactional staging.
    pub fn preparePayload(self: *Prepared) !void {
        try self.command.validate();
        if (self.payload != null) return;
        var writer: std.Io.Writer.Allocating = .init(self.alloc);
        defer writer.deinit();
        try writer.writer.writeAll("{\"schema_version\":22,\"request\":{},\"index_owner_control\":{\"command\":");
        try std.json.Stringify.value(self.command, .{}, &writer.writer);
        try writer.writer.writeAll(",\"position\":{\"namespace\":");
        try std.json.Stringify.value(self.command.binding.fence.namespace, .{}, &writer.writer);
        try writer.writer.writeAll(",\"sequence\":");
        self.prefix_len = writer.written().len;
        if (self.prefix_len + 23 > max_wire_bytes) return error.InvalidPrivateIndexInputs;
        try writer.writer.splatByteAll(' ', 23);
        self.payload = try writer.toOwnedSlice();
    }

    pub fn finalizePayload(self: *Prepared, position: Native) ![]const u8 {
        try position.validate();
        if (!position.namespace.eql(self.command.binding.fence.namespace)) return error.IdentityNamespaceMismatch;
        const bytes = self.payload orelse return error.InvalidPrivateIndexOwnerJob;
        const digits = try std.fmt.bufPrint(bytes[self.prefix_len .. self.prefix_len + 20], "{d}", .{position.sequence});
        const end = self.prefix_len + digits.len;
        @memcpy(bytes[end .. end + 3], "}}}");
        return bytes[0 .. end + 3];
    }
};
