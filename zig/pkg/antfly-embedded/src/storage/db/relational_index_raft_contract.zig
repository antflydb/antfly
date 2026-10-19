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
        if (binding.phase != .fencing and binding.phase != .reserved and binding.phase != .installed) return error.InvalidPrivateIndexOwnerJob;
        const applied = self.applied orelse return error.InvalidControlReceiptPosition;
        if (binding.phase == .reserved or binding.phase == .installed) {
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
pub const CancellationRequest = union(enum) {
    cancel: Admission,
    release: @import("relational_index_release.zig").CancellationIntent,
    pub fn identity(self: @This()) Identity {
        const intent = switch (self) {
            .cancel => |value| value,
            .release => |value| Admission.fromBinding(value.canceled.binding),
        };
        return .{ .id = intent.id, .plan_digest = intent.plan_digest, .owner_digest = intent.owner_digest, .ordinal = intent.ordinal, .namespace = intent.fence.namespace, .owner_group_id = intent.fence.owner_group_id };
    }
    pub fn validate(self: @This()) !void {
        try self.identity().validate();
        switch (self) {
            .cancel => |value| try value.validate(),
            .release => |value| {
                try value.validate();
                if (value.canceled.position != .raft) return error.InvalidControlReceiptPosition;
            },
        }
    }
};
pub const CancellationProgress = union(enum) {
    cancel: jobs.RaftCancellationState,
    release: ?@import("relational_index_release.zig").CancellationRecord,
    pub fn validateFor(self: @This(), request: CancellationRequest) !void {
        try request.validate();
        if ((self == .cancel) != (request == .cancel)) return error.InvalidArgument;
        switch (self) {
            .cancel => |state| {
                try state.validate(request.identity());
                if (state.intent) |record| if (!record.intent.eql(request.cancel)) return error.PrivateIndexOwnerJobChanged;
                if (state.binding) |binding| if (!Admission.fromBinding(binding).eql(request.cancel)) return error.PrivateIndexOwnerJobChanged;
            },
            .release => |record| if (record) |value| {
                _ = try value.intent.receiptAt(value.position);
                if (value.position != .raft or !std.meta.eql(value.intent, request.release)) return error.PrivateIndexOwnerJobChanged;
            },
        }
    }
};
pub const CancellationPreparation = struct {
    pub const max_request_bytes = 32 * 1024;
    pub const max_response_bytes = @import("relational_index_gc.zig").Page.Replay.max_wire_bytes + 64 * 1024;
    progress: CancellationProgress,
    command: ?Command = null,
    pub fn validateFor(self: @This(), request: CancellationRequest) !void {
        try self.progress.validateFor(request);
        if (self.command) |command| {
            try command.validate();
            if (request == .release) {
                if (command != .cancel_release or self.progress.release != null or !std.meta.eql(command.cancel_release, request.release)) return error.InvalidPrivateIndexCancellation;
            } else {
                if (self.progress.cancel.completed != null) return error.InvalidPrivateIndexCancellation;
                const intent = switch (command) {
                    .cancel_intent => |value| value,
                    .cancel_control => |value| blk: {
                        if (value.effect == .admitted) return error.InvalidPrivateIndexCancellation;
                        break :blk Admission.fromBinding(value.binding);
                    },
                    .cancel_cleanup, .cancel_complete => |value| Admission.fromBinding(value),
                    inline .cancel_artifacts, .cancel_claims => |value| Admission.fromBinding(value.binding),
                    else => return error.InvalidPrivateIndexCancellation,
                };
                if (!intent.eql(request.cancel)) return error.PrivateIndexOwnerJobChanged;
            }
        }
    }
};
const installation = @import("relational_index_validation.zig");
const release = @import("relational_index_release.zig");
pub const Release = release;

/// Read-only preparation/observation through the leased physical owner. No
/// execution cut or metadata permission bit is accepted at this boundary.
pub const LifecycleRequest = union(enum) {
    install: struct { intent: installation.InstallIntent, budget: @import("relational_index_jobs.zig").Budget = .{} },
    release: release.Intent,

    pub fn binding(self: @This()) jobs.Binding {
        return switch (self) {
            .install => |value| value.intent.validation.command.binding,
            .release => |value| value.installed.command.validation.command.binding,
        };
    }
    pub fn validate(self: @This()) !void {
        if (self.binding().authority != .raft) return error.InvalidControlReceiptPosition;
        switch (self) {
            .install => |value| {
                try value.intent.validate();
                try value.budget.validate();
            },
            .release => |value| try value.validate(),
        }
    }
};

pub const InstallationProgress = struct {
    physical: ?@import("relational_index_owner_control.zig").AuditOutcome = null,
    command: ?installation.InstallCommand = null,
    installed: ?installation.Installed = null,

    pub fn validateFor(self: @This(), intent: installation.InstallIntent) !void {
        try intent.validate();
        if (intent.validation.position != .raft) return error.InvalidControlReceiptPosition;
        if (self.physical) |physical| {
            const b = intent.validation.command.binding;
            var request = try BuildRequest.initRaft(.{ .id = b.id, .plan_digest = b.plan_digest, .owner_digest = b.owner_digest, .ordinal = b.ordinal, .namespace = b.fence.namespace, .owner_group_id = b.fence.owner_group_id }, b, .{});
            request.target_expectation = intent.validation.command.expected;
            try physical.validateForRaft(request);
        }
        if (self.command) |command| {
            try command.validate();
            const physical = self.physical orelse return error.PrivateIndexTargetValidationRequired;
            if (!std.meta.eql(command.intent(), intent) or !std.meta.eql(try physical.logicalCommand(), intent.validation.command)) return error.PrivateIndexAuditScopeChanged;
        }
        if (self.installed) |installed| {
            _ = try installed.command.receiptAt(installed.position);
            if (installed.position != .raft or !std.meta.eql(installed.command.intent(), intent)) return error.InvalidControlReceiptPosition;
            if (self.command) |command| if (!std.meta.eql(command, installed.command)) return error.PrivateIndexAuditScopeChanged;
        }
    }
};
pub const ReleaseProgress = struct {
    record: ?release.Record = null,
    retired: bool = false,

    pub fn validateFor(self: @This(), intent: release.Intent) !void {
        try intent.validate();
        if (intent.installed.position != .raft) return error.InvalidControlReceiptPosition;
        if (self.record) |record| {
            try record.validate();
            if (record.position != .raft or !std.meta.eql(record.intent, intent)) return error.InvalidControlReceiptPosition;
        } else if (self.retired) return error.InvalidPrivateIndexNativeReceipt;
    }
};
pub const LifecycleProgress = union(enum) {
    install: InstallationProgress,
    release: ReleaseProgress,
    pub fn validateFor(self: @This(), request: LifecycleRequest) !void {
        try request.validate();
        if ((self == .install) != (request == .install)) return error.InvalidArgument;
        switch (self) {
            .install => |value| try value.validateFor(request.install.intent),
            .release => |value| try value.validateFor(request.release),
        }
    }
};
pub const LifecyclePreparation = struct {
    pub const max_request_bytes = 32 * 1024;
    pub const max_response_bytes = release.Gc.max_wire_bytes + 64 * 1024;
    progress: LifecycleProgress,
    gc: ?release.Gc.Command = null,
    pub fn validateFor(self: @This(), request: LifecycleRequest) !void {
        try self.progress.validateFor(request);
        if (self.gc) |gc| {
            try gc.validate();
            if (self.progress != .release or self.progress.release.retired or !std.meta.eql(gc.record, self.progress.release.record orelse return error.InvalidPrivateIndexNativeReceipt)) return error.InvalidPrivateIndexNativeReceipt;
        }
    }
};
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
pub const ValidationProgress = struct {
    binding: jobs.Binding,
    inventory: ?jobs.private_claims.InventoryValidation.Record = null,
    generation: ?@import("relational_index_validation.zig").Summary = null,
    seal: ?@import("relational_index_validation.zig").Seal = null,

    pub fn validateFor(self: @This(), request: BuildRequest) !void {
        try request.requireRaftBinding(self.binding);
        const expected = request.target_expectation orelse return error.InvalidGenerationPublication;
        if (self.inventory) |record| {
            try record.validate();
            if (record.position != .raft or !std.mem.eql(u8, &try record.expected.digest(), &try expected.digest())) return error.InvalidPrivateIndexClaimInventory;
        }
        if (self.generation) |record| {
            try record.validate();
            const inventory = self.inventory orelse return error.PrivateIndexTargetValidationRequired;
            if (inventory.fields.phase != .ready or record.position != .raft or !record.position.strictlyAfter(inventory.position) or
                !std.mem.eql(u8, &try record.expected.digest(), &try expected.digest())) return error.InvalidRelationalIndexValidation;
        }
        if (self.seal) |seal| {
            try seal.command.validate();
            const generation = self.generation orelse return error.PrivateIndexTargetValidationRequired;
            if (generation.fields.phase != .ready or seal.position != .raft or !seal.position.strictlyAfter(generation.position) or
                !std.meta.eql(seal.command.generation_position, generation.position) or
                !std.mem.eql(u8, &try seal.command.binding.encode(), &try self.binding.encode()) or
                !std.mem.eql(u8, &try seal.command.expected.digest(), &try expected.digest())) return error.InvalidRelationalIndexValidation;
        }
    }
    pub fn requireReady(self: @This()) !void {
        if (self.inventory == null or self.inventory.?.fields.phase != .ready or
            self.generation == null or self.generation.?.fields.phase != .ready) return error.PrivateIndexTargetValidationRequired;
    }
};

pub const AuditProgress = struct {
    outcome: @import("relational_index_owner_control.zig").AuditOutcome,
    seal: ?@import("relational_index_validation.zig").Seal = null,
    pub fn validateFor(self: @This(), request: BuildRequest) !void {
        try self.outcome.validateForRaft(request);
        if (self.seal) |seal| {
            if (seal.position != .raft or !std.meta.eql(seal.command, try self.outcome.logicalCommand()) or
                !seal.position.strictlyAfter(seal.command.generation_position)) return error.InvalidRelationalIndexValidation;
        }
    }
};

pub const BuildPreparation = union(enum) {
    pub const max_request_bytes = 8 * 1024;
    pub const max_response_bytes = @max(@import("relational_index_jobs.zig").Page.PrivateCommand.max_wire_bytes, @import("relational_index_validation.zig").max_wire_bytes) + 32 * 1024;
    page: @import("relational_index_jobs.zig").Page.PrivateCommand,
    progress: BuildProgress,
    inventory_page: jobs.private_claims.InventoryValidation.Command,
    generation_page: @import("relational_index_validation.zig").Command,
    validation: ValidationProgress,
    audit: AuditProgress,

    pub fn validateFor(self: @This(), request: BuildRequest) !void {
        switch (self) {
            .page => |page| {
                try page.validate();
                try request.requireRaftBinding(page.binding);
                if (request.target_expectation != null) return error.InvalidGenerationPublication;
            },
            .progress => |progress| try progress.validateFor(request),
            inline .inventory_page, .generation_page => |page| {
                try page.validate();
                try request.requireRaftBinding(page.binding);
                const expected = request.target_expectation orelse return error.InvalidGenerationPublication;
                if (!std.mem.eql(u8, &try expected.digest(), &try page.expected.digest())) return error.InvalidGenerationPublication;
            },
            .validation => |value| try value.validateFor(request),
            .audit => |value| try value.validateFor(request),
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
