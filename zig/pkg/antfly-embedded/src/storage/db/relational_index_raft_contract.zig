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

pub const Control = struct {
    effect: Effect,
    binding: jobs.Binding,
    inputs_b64: ?[]const u8 = null,

    pub fn validate(self: Control) !void {
        _ = try self.binding.encode();
        if (self.binding.authority != .raft or self.binding.phase != .fencing or
            ((self.effect == .admitted) != (self.inputs_b64 != null))) return error.InvalidPrivateIndexOwnerJob;
        if (self.inputs_b64) |bytes| if (bytes.len == 0 or bytes.len > std.base64.standard.Encoder.calcSize(inputs.max_bytes)) return error.InvalidPrivateIndexInputs;
    }
};

pub const BuildRequest = @import("relational_index_owner_control.zig").BuildRequest;
pub const BuildProgress = struct {
    binding: jobs.Binding,
    state: @import("relational_index_jobs.zig").State = .building,
    failure: @import("relational_index_jobs.zig").Failure = .none,
    rows_scanned: u64 = 0,
    progress_digest: [32]u8 = @splat(0),
    position: ?positions.Position = null,
    claim_batches_pending: u64 = 0,
    claim_bytes_pending: u64 = 0,

    pub fn validateFor(self: @This(), request: BuildRequest) !void {
        try request.requireRaftBinding(self.binding);
        if (request.target_expectation != null or (self.state == .failed) != (self.failure != .none) or
            self.claim_batches_pending > jobs.private_claims.SourceStream.max_pending or
            self.claim_bytes_pending > jobs.private_claims.SourceStream.max_pending_bytes or
            (self.claim_batches_pending == 0) != (self.claim_bytes_pending == 0)) return error.InvalidRelationalIndexProgress;
        if (self.position) |position| {
            try position.validate();
            if (position != .raft or std.mem.allEqual(u8, &self.progress_digest, 0)) return error.InvalidControlReceiptPosition;
        } else if (self.state != .building or self.rows_scanned != 0 or self.claim_batches_pending != 0 or
            !std.mem.allEqual(u8, &self.progress_digest, 0)) return error.InvalidRelationalIndexProgress;
    }
};
pub const BuildPreparation = union(enum) {
    pub const max_request_bytes = 8 * 1024;
    pub const max_response_bytes = @import("relational_index_jobs.zig").Page.PrivateCommand.max_wire_bytes + 1024;
    page: @import("relational_index_jobs.zig").Page.PrivateCommand,
    progress: BuildProgress,

    pub fn validateFor(self: @This(), request: BuildRequest) !void {
        switch (self) {
            .page => |page| {
                try page.validate();
                try request.requireRaftBinding(page.binding);
                if (request.target_expectation != null) return error.InvalidGenerationPublication;
            },
            .progress => |progress| try progress.validateFor(request),
        }
    }
};

/// Exactly one owned effect per committed entry. The execution position is
/// absent: only the committed-entry callback can supply that authority cut.
/// This replaces the earlier branch-local flat control framing.
pub const Command = union(enum) {
    control: Control,
    cancel_control: Control,
    cancel_intent: Admission,
    cancel_cleanup: jobs.Binding,
    cancel_artifacts: @import("relational_index_gc.zig").Page.Replay,
    cancel_claims: @import("relational_index_private_claims.zig").GcPage.Replay,
    cancel_complete: jobs.Binding,
    cancel_release: @import("relational_index_release.zig").CancellationIntent,
    build: @import("relational_index_jobs.zig").Page.PrivateCommand,
    claims: @import("relational_index_owner_control.zig").ClaimCommand,
    consume: @import("relational_index_owner_control.zig").ConsumeCommand,
    inventory_validation: @import("relational_index_private_claims.zig").InventoryValidation.Command,
    generation_validation: @import("relational_index_validation.zig").Command,
    validation_seal: @import("relational_index_validation.zig").SealCommand,
    install: @import("relational_index_validation.zig").InstallCommand,
    release: @import("relational_index_release.zig").Intent,
    release_gc: @import("relational_index_release.zig").Gc.Command,

    pub fn fence(self: Command) @import("relational_integrity_topology.zig").Fence {
        return switch (self) {
            .control, .cancel_control => |value| value.binding.fence,
            .cancel_intent => |value| value.fence,
            .cancel_cleanup, .cancel_complete => |value| value.fence,
            .cancel_release => |value| value.canceled.binding.fence,
            .install => |value| value.validation.command.binding.fence,
            .release => |value| value.installed.command.validation.command.binding.fence,
            .release_gc => |value| value.record.intent.installed.command.validation.command.binding.fence,
            inline else => |value| value.binding.fence,
        };
    }

    pub fn validate(self: Command) !void {
        switch (self) {
            .cancel_intent => |value| try value.validate(),
            .cancel_cleanup, .cancel_complete => |value| {
                _ = try value.encode();
                if (value.authority != .raft or value.phase != .reserved) return error.InvalidPrivateIndexOwnerJob;
            },
            .cancel_release => |value| {
                try value.validate();
                if (value.canceled.position != .raft) return error.InvalidControlReceiptPosition;
            },
            .install => |value| {
                try value.validate();
                if (value.validation.command.binding.authority != .raft) return error.InvalidControlReceiptPosition;
            },
            .release => |value| {
                try value.validate();
                if (value.installed.position != .raft) return error.InvalidControlReceiptPosition;
            },
            .release_gc => |value| {
                try value.validate();
                if (value.record.position != .raft) return error.InvalidControlReceiptPosition;
            },
            inline else => |value| {
                try value.validate();
                if (value.binding.authority != .raft) return error.InvalidControlReceiptPosition;
            },
        }
    }

    pub fn wireLimit(self: Command) usize {
        const native = @import("relational_index_owner_control.zig");
        return switch (self) {
            .control, .cancel_control => native.max_control_wire_bytes,
            .claims => native.max_claim_wire_bytes,
            .build => @import("relational_index_jobs.zig").Page.PrivateCommand.max_wire_bytes,
            .generation_validation => @import("relational_index_validation.zig").max_wire_bytes,
            .inventory_validation => @import("relational_index_private_claims.zig").InventoryValidation.max_wire_bytes,
            .release_gc => @import("relational_index_release.zig").Gc.max_wire_bytes,
            .cancel_artifacts => @import("relational_index_gc.zig").Page.Replay.max_wire_bytes,
            else => native.max_cancellation_wire_bytes,
        };
    }

    /// Bound typed duplication while still borrowing the general JSON tree.
    /// Claim/build parsers additionally enforce their record-count budgets.
    pub fn validateBorrowed(value: std.json.Value) !void {
        if (value != .object or value.object.count() != 1) return error.InvalidBatchRequest;
        const tag = std.meta.stringToEnum(std.meta.Tag(Command), value.object.keys()[0]) orelse return error.InvalidBatchRequest;
        const body = value.object.values()[0];
        if (body != .object) return error.InvalidBatchRequest;
        const native = @import("relational_index_owner_control.zig");
        const limit: usize = switch (tag) {
            .control, .cancel_control => blk: {
                if (body.object.get("inputs_b64")) |raw| if (raw != .null) {
                    if (raw != .string or raw.string.len == 0 or raw.string.len > std.base64.standard.Encoder.calcSize(inputs.max_bytes)) return error.InvalidPrivateIndexInputs;
                };
                break :blk native.max_control_wire_bytes;
            },
            .claims => native.max_claim_wire_bytes,
            .build => @import("relational_index_jobs.zig").Page.PrivateCommand.max_wire_bytes,
            .generation_validation => @import("relational_index_validation.zig").max_wire_bytes,
            .inventory_validation => @import("relational_index_private_claims.zig").InventoryValidation.max_wire_bytes,
            .release_gc => @import("relational_index_release.zig").Gc.max_wire_bytes,
            .cancel_artifacts => blk: {
                const gc = @import("relational_index_gc.zig").Page.Replay;
                inline for (.{ "expected", "next" }) |name| if (body.object.get(name)) |raw| {
                    if (raw != .null) {
                        if (raw != .array or raw.array.items.len > gc.max_progress_bytes) return error.InvalidRelationalIndexGcProgress;
                    }
                };
                break :blk gc.max_wire_bytes;
            },
            else => native.max_cancellation_wire_bytes,
        };
        var buffer: [256]u8 = undefined;
        var count = std.Io.Writer.Discarding.init(&buffer);
        std.json.Stringify.value(body, .{}, &count.writer) catch return error.InvalidBatchRequest;
        if (count.fullCount() > limit) return error.TransactionTooLarge;
    }
};
