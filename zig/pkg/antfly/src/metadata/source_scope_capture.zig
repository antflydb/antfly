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

//! Source lifecycle capture at one applied metadata snapshot. The service must
//! first establish qualified ReadIndex authority. This value carries observed
//! facts, not authority manufactured from caller-supplied cuts or state JSON.
const std = @import("std");
const control = @import("source_control.zig");
pub const native = @import("antfly_local_sources").storage_source_scope;
const A = std.mem.Allocator;
const native_control = @import("antfly_local_sources").storage_source_scope_control;
pub const Query = struct {
    metadata_group_id: u64,
    metadata_incarnation: [32]u8,
    actor_table_id: u64,
    source_id: control.Digest,
    minimum_applied_index: u64,
    pub fn validate(self: Query) !void {
        if (self.metadata_group_id == 0 or self.actor_table_id == 0 or self.minimum_applied_index == 0 or std.mem.allEqual(u8, &self.source_id, 0) or std.mem.allEqual(u8, &self.metadata_incarnation, '0')) return error.InvalidSourceControl;
        for (self.metadata_incarnation) |c| if (!std.ascii.isDigit(c) and !(c >= 'a' and c <= 'f')) return error.InvalidSourceControl;
    }
};
pub const Phase = enum { active, closing, retired };
pub const Pending = struct {
    reference: native.Reference,
    transaction_id: [16]u8,
    purpose: control.Digest,
    reservation: control.Digest,
    plan_digest: control.Digest,
    artifact_plan_digest: control.Digest,
};
const PreparedInstall = struct {
    owner: @FieldType(native_control.Intent, "owner"),
    transfer: native_control.InstallCheckpoint,
};
pub const Snapshot = struct {
    cut: native.MetadataCut,
    spec: native.Spec,
    phase: Phase,
    actor_revision: u64,
    live_definition: bool,
    checkpoint_table: []const u8,
    pending: ?Pending = null,
    native_bootstrap_certified: bool = false,
    pending_fenceable: bool = false,
    checkpoint_transfer: bool = false,
    settled_attempt_fence: bool = false,
    checkpoint_adopted: bool = false,
    checkpoint_donor_close_ready: bool = false,
    prepared_install: ?PreparedInstall = null,
    pub fn jsonStringify(self: Snapshot, stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        inline for (@typeInfo(Snapshot).@"struct".field_names) |name| {
            const boolean_flag = comptime std.mem.eql(u8, name, "native_bootstrap_certified") or std.mem.eql(u8, name, "pending_fenceable") or std.mem.eql(u8, name, "settled_attempt_fence") or std.mem.eql(u8, name, "checkpoint_transfer") or std.mem.eql(u8, name, "checkpoint_adopted") or std.mem.eql(u8, name, "checkpoint_donor_close_ready");
            const include = if (comptime boolean_flag)
                @field(self, name)
            else if (comptime std.mem.eql(u8, name, "prepared_install"))
                self.prepared_install != null
            else
                true;
            if (include) {
                try stream.objectField(name);
                try stream.write(@field(self, name));
            }
        }
        try stream.endObject();
    }
    pub fn authorize(self: Snapshot, intent: native_control.Intent) !native_control.Command {
        try intent.validate();
        if (intent.action != .install_checkpoint) try self.requireBinding(intent.spec);
        switch (intent.action) {
            .bind => try self.requireBind(),
            .close => try self.requireClose(),
            .close_checkpoint => {
                if (self.phase != .active or !self.checkpoint_donor_close_ready or !self.live_definition or
                    !std.meta.eql(try native.Basis.fromOwner(intent.owner), self.spec.checkpoint_owner))
                    return error.SourceBindingConflict;
            },
            .drop => try self.requireDrop(),
            .install_checkpoint => |install| {
                const prepared = self.prepared_install orelse return error.SourceBindingConflict;
                var qualified = prepared.owner;
                qualified.topology_epoch = intent.owner.topology_epoch;
                if (self.phase != .active or !self.live_definition or
                    !std.meta.eql(self.spec, install.donor.spec) or
                    !std.meta.eql(qualified, intent.owner) or !std.meta.eql(prepared.transfer, install))
                    return error.SourceBindingConflict;
            },
            .fence_attempt => |attempt| {
                const expected = self.pending orelse return error.SourcePendingWork;
                if (!self.pending_fenceable or !std.meta.eql(attempt.reference, expected.reference) or
                    !std.meta.eql(attempt.transaction_id, expected.transaction_id) or !std.meta.eql(attempt.reservation, expected.reservation))
                    return error.SourceBindingConflict;
            },
        }
        const result: native_control.Command = .{
            .version = switch (intent.action) {
                .install_checkpoint, .close_checkpoint => 4,
                .fence_attempt => if (self.checkpoint_transfer or self.settled_attempt_fence) 4 else 3,
                .bind => if (self.checkpoint_adopted) 4 else if (self.native_bootstrap_certified) 2 else 1,
                else => 1,
            },
            .intent = intent,
            .metadata_cut = self.cut,
        };
        try result.validate();
        return result;
    }
    pub fn deinit(self: *Snapshot, a: A) void {
        a.free(self.checkpoint_table);
        self.* = undefined;
    }
    pub fn requireBinding(self: Snapshot, expected: native.Spec) !void {
        if (!std.meta.eql(self.spec, expected)) return error.SourceBindingConflict;
    }
    pub fn requireBind(self: Snapshot) !void {
        if (self.phase != .active or !self.live_definition or self.checkpoint_transfer) return error.SourceBindingConflict;
    }
    pub fn requireClose(self: Snapshot) !void {
        if (self.phase == .active) return error.SourceBindingConflict;
    }
    pub fn requireDrop(self: Snapshot) !void {
        if (self.phase != .retired) return error.SourcePendingWork;
    }
};
/// All arguments originate in the same native metadata read transaction.
/// checkpoint_table is copied out; no parsed-state storage escapes this call.
pub fn capture(a: A, query: Query, applied_index: u64, incarnation: [32]u8, decoder_version: u16, state: control.State, sources_json: []const u8) !Snapshot {
    try query.validate();
    if (applied_index < query.minimum_applied_index) return error.ReadUnavailable;
    if (!std.meta.eql(query.metadata_incarnation, incarnation)) return error.InvalidMetadataIncarnation;
    if (decoder_version < control.closing_protocol_version) return error.TableTopologyProtocolUpgradeRequired;
    try state.validate();
    if (state.native_bootstrap_checkpoint != null and decoder_version < control.native_bootstrap_protocol_version) return error.TableTopologyProtocolUpgradeRequired;
    if (state.native_bootstrap_first_page != null and decoder_version < control.scoped_snapshot_protocol_version) return error.TableTopologyProtocolUpgradeRequired;
    const source = state.scope orelse return error.SourceBindingConflict;
    const authority = state.checkpoint_authority orelse return error.SourceBindingConflict;
    if (!state.scope_fencing or source.destination != .native or source.table_id != query.actor_table_id or !std.meta.eql(source.source_id, query.source_id) or authority.owner.metadata_group_id != query.metadata_group_id or !std.meta.eql(authority.owner.metadata_incarnation, incarnation)) return error.SourceBindingConflict;
    const spec = (try @import("source_scope_binding.zig").spec(state)) orelse return error.SourceBindingConflict;
    const definition = control.definitionDigest(a, sources_json, source.source_ordinal) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => null,
    };
    var pending: ?Pending = null;
    if (state.pending) |reservation| {
        const artifact = reservation.artifact orelse return error.SourceControlCorrupt;
        pending = .{ .reference = .{ .domain = spec.domain(), .binding = spec.binding, .generation = spec.generation, .token = artifact.token(), .sequence = reservation.sequence }, .transaction_id = reservation.transaction_id, .purpose = try reservation.inputFingerprint(source), .reservation = reservation.identity(source), .plan_digest = reservation.plan_digest, .artifact_plan_digest = artifact.plan_digest };
        try pending.?.reference.validate();
    }
    const pending_fenceable = decoder_version >= control.retained_payload_protocol_version and
        state.pending != null and state.pending.?.retain_payload and
        state.artifact_control.?.current != null and state.artifact_control.?.current.?.phase == .sealed;
    const checkpoint_transfer = state.checkpoint_transfer_target != null;
    if ((checkpoint_transfer or state.checkpoint_install_receipt != null) and decoder_version < control.checkpoint_authority.successor_protocol_version) return error.TableTopologyProtocolUpgradeRequired;
    const prepared_install: ?PreparedInstall = if (state.checkpoint_preparation) |prepared| blk: {
        if (decoder_version < control.checkpoint_authority.successor_protocol_version) return error.TableTopologyProtocolUpgradeRequired;
        break :blk .{ .owner = prepared.successor.owner, .transfer = prepared.transfer };
    } else null;
    return .{ .cut = .{ .group_id = query.metadata_group_id, .incarnation = incarnation, .index = applied_index }, .spec = spec, .phase = if (state.retired) .retired else if (state.closing) .closing else .active, .actor_revision = state.revision, .live_definition = if (definition) |value| std.meta.eql(value, source.definition) else false, .checkpoint_table = try a.dupe(u8, authority.table_name), .pending = pending, .native_bootstrap_certified = state.native_bootstrap_checkpoint != null, .pending_fenceable = pending_fenceable, .checkpoint_transfer = checkpoint_transfer, .settled_attempt_fence = pending_fenceable and decoder_version >= control.epoch_attempt_retirement_protocol_version, .checkpoint_adopted = state.checkpoint_install_receipt != null, .checkpoint_donor_close_ready = checkpoint_transfer and state.checkpointTransferDrained(), .prepared_install = prepared_install };
}

pub fn readOnService(svc: anytype, a: A, query: Query, request: anytype) !Snapshot {
    try query.validate();
    try request.ensureActive();
    if (svc.metadata_group_id != query.metadata_group_id) return error.SourceBindingConflict;
    try svc.ensureLinearizableReadWithContext(request);
    try request.ensureActive();
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    var snapshot = try store.captureSourceScope(a, query);
    errdefer snapshot.deinit(a);
    try request.ensureActive();
    return snapshot;
}

/// The intent carries only a minimum metadata cut. Re-read independently on the
/// metadata service, require the exact persisted binding and lifecycle phase,
/// then construct the private command using the actual observed cut. This does
/// not qualify the destination root or establish first-bind legacy drainage.
pub fn authorizeOnService(svc: anytype, a: A, intent: @import("antfly_local_sources").storage_source_scope_control.Intent, request: anytype) !@import("antfly_local_sources").storage_source_scope_control.Command {
    try intent.validate();
    var snapshot = try readOnService(svc, a, .{ .metadata_group_id = intent.owner.metadata_group_id, .metadata_incarnation = intent.owner.metadata_incarnation, .actor_table_id = intent.spec.actor_table_id, .source_id = intent.spec.source_id, .minimum_applied_index = intent.minimum_metadata_index }, request);
    defer snapshot.deinit(a);
    return snapshot.authorize(intent);
}
