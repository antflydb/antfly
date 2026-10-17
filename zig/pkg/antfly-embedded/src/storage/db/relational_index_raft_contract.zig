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

//! Data-only private Raft controls. Decoding is not metadata authorization.
const std = @import("std");
const jobs = @import("relational_index_owner_job.zig");
const inputs = @import("relational_index_inputs.zig");

pub const Effect = enum(u8) { admitted, fenced, reserved };
pub const Identity = @import("relational_index_owner_identity.zig").Identity;
pub const Admission = @import("relational_index_cancellation_intent.zig").Intent;
const publication = @import("relational_index_publication_progress.zig");
const positions = @import("receipt_position.zig");

/// Advisory compilation result, not a reservation or metadata permission.
/// Only the actual storage owner chooses the physical binding. Committed apply
/// must still revalidate its complete catalog/schema preimage.
pub const AdmissionPreparation = struct {
    pub const max_header_json_bytes = 8 * 1024;
    pub const max_json_bytes = 16 * 1024;
    pub const max_manifest_bytes = inputs.max_bytes;
    format_version: u16,
    binding: jobs.Binding,

    pub fn validate(self: AdmissionPreparation, expected: Admission, manifest_digest: [32]u8) !void {
        try expected.validate();
        _ = try self.binding.encode();
        if (self.format_version != 1 or self.binding.authority != .raft or self.binding.phase != .fencing or
            !expected.eql(Admission.fromBinding(self.binding))) return error.PrivateIndexOwnerJobChanged;
        if (!std.mem.eql(u8, &self.binding.inputs_digest, &manifest_digest)) return error.InvalidPrivateIndexInputs;
    }
};

/// Deterministic integrity binding, not authentication or proof of commit.
/// Storage separately verifies the retained cut and durable apply watermark.
pub fn effectEvidence(binding: jobs.Binding, effect: Effect, position: positions.Position) ![32]u8 {
    try position.validate();
    if (binding.authority != .raft or position != .raft) return error.InvalidControlReceiptPosition;
    var original = binding;
    original.phase = .fencing;
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly:index-owner-raft-effect:v1");
    state.update(&inputs.digest(&try original.encode()));
    state.update(&.{@backingInt(effect)});
    state.update(&try position.encode());
    var result: [32]u8 = undefined;
    state.final(&result);
    return result;
}

/// One coherent read, not a signature, metadata permission or cancellation
/// fence. The physical owner supplies these facts from the same store snapshot.
pub const Observation = struct {
    pub const max_json_bytes = 32 * 1024;
    format_version: u16,
    binding: ?jobs.Binding = null,
    /// Last mutation committed by this store, not the host's completed Raft
    /// prefix/read index. Rejected commands and protocol barriers may leave a
    /// gap; they must never be mistaken for a committed phase receipt.
    applied: ?positions.Position = null,
    fenced: ?publication.Receipt = null,
    reserved: ?publication.Receipt = null,

    pub fn validate(self: Observation, expected: Identity) !void {
        try expected.validate();
        if (self.format_version != 1) return error.InvalidPrivateIndexOwnerJob;
        if (self.applied) |applied| {
            try applied.validate();
            if (applied != .raft) return error.InvalidControlReceiptPosition;
        }
        const binding = self.binding orelse {
            if (self.fenced != null or self.reserved != null) return error.InvalidControlReceiptPosition;
            return;
        };
        _ = try binding.encode();
        if (binding.authority != .raft or !std.mem.eql(u8, &binding.id, &expected.id) or
            !std.mem.eql(u8, &binding.plan_digest, &expected.plan_digest) or
            !std.mem.eql(u8, &binding.owner_digest, &expected.owner_digest) or binding.ordinal != expected.ordinal or
            !binding.fence.namespace.eql(expected.namespace) or binding.fence.owner_group_id != expected.owner_group_id)
            return error.PrivateIndexOwnerJobChanged;
        if (binding.phase != .fencing and binding.phase != .reserved) return error.InvalidPrivateIndexOwnerJob;
        const applied = self.applied orelse return error.InvalidControlReceiptPosition;
        if (binding.phase == .reserved) {
            if (self.fenced == null or self.reserved == null) return error.InvalidControlReceiptPosition;
        } else if (self.reserved != null) return error.InvalidControlReceiptPosition;
        const owner: publication.Owner = .{ .plan_id = binding.id, .plan_digest = binding.plan_digest, .ordinal = binding.ordinal, .group_id = binding.fence.owner_group_id, .namespace = binding.fence.namespace, .authority = .raft, .fence_digest = binding.owner_digest };
        inline for (.{ "fenced", "reserved" }) |field| {
            if (@field(self, field)) |receipt| {
                try receipt.owner.validate();
                try receipt.position.validate();
                if (!std.meta.eql(owner, receipt.owner) or receipt.action != @field(publication.Action, field) or
                    !std.mem.eql(u8, &receipt.candidate_digest, &binding.integrity_candidate) or
                    !std.mem.eql(u8, &receipt.reservation_digest, &binding.integrity_reserved) or
                    std.mem.allEqual(u8, &receipt.evidence_digest, 0)) return error.InvalidControlReceiptPosition;
                if (receipt.position != .raft or receipt.position.raft.index > applied.raft.index or
                    receipt.position.raft.term > applied.raft.term or
                    (receipt.position.raft.index == applied.raft.index and receipt.position.raft.term != applied.raft.term)) return error.InvalidControlReceiptPosition;
                if (!std.mem.eql(u8, &receipt.evidence_digest, &try effectEvidence(binding, @field(Effect, field), receipt.position))) return error.InvalidControlReceiptPosition;
            }
        }
        if (self.reserved) |reserved| {
            const fenced = self.fenced.?;
            if (fenced.position.raft.index >= reserved.position.raft.index or fenced.position.raft.term > reserved.position.raft.term) return error.InvalidControlReceiptPosition;
        }
    }
};
/// A control is one isolated store transaction. Reflect the request's default
/// shape so future effects cannot accidentally be silently ignored here.
pub fn validateBatchRequest(request: anytype) !void {
    const command = request.relational_index_owner_control orelse return;
    try command.validate();
    const defaults: @TypeOf(request) = .{};
    inline for (comptime std.meta.fieldNames(@TypeOf(request))) |name| {
        if (comptime std.mem.eql(u8, name, "relational_index_owner_control") or std.mem.eql(u8, name, "timestamp_ns")) continue;
        const value = @field(request, name);
        const info = @typeInfo(@TypeOf(value));
        if (comptime info == .pointer and info.pointer.size == .slice) {
            // Parsed empty arrays own a different address than &.{}. Pointer
            // identity is neither a mutation nor a semantic nondefault value.
            if (comptime @field(defaults, name).len != 0) @compileError("review nonempty default in isolated index control: " ++ name);
            if (value.len != 0) return error.InvalidBatchRequest;
        } else if (!std.meta.eql(value, @field(defaults, name))) return error.InvalidBatchRequest;
    }
}

pub const Command = struct {
    effect: Effect,
    binding: jobs.Binding,
    inputs_b64: ?[]const u8 = null,

    pub fn validate(self: Command) !void {
        _ = try self.binding.encode();
        if (self.binding.authority != .raft or self.binding.phase != .fencing or
            ((self.effect == .admitted) != (self.inputs_b64 != null))) return error.InvalidPrivateIndexOwnerJob;
        if (self.inputs_b64) |bytes| if (bytes.len == 0 or bytes.len > std.base64.standard.Encoder.calcSize(inputs.max_bytes)) return error.InvalidPrivateIndexInputs;
    }
};
