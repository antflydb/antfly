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
pub const Snapshot = struct {
    cut: native.MetadataCut,
    spec: native.Spec,
    phase: Phase,
    actor_revision: u64,
    live_definition: bool,
    checkpoint_table: []const u8,
    pending: ?Pending = null,
    pub fn deinit(self: *Snapshot, a: A) void {
        a.free(self.checkpoint_table);
        self.* = undefined;
    }
    pub fn requireBinding(self: Snapshot, expected: native.Spec) !void {
        if (!std.meta.eql(self.spec, expected)) return error.SourceBindingConflict;
    }
    pub fn requireBind(self: Snapshot) !void {
        if (self.phase != .active or !self.live_definition) return error.SourceBindingConflict;
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
    const source = state.scope orelse return error.SourceBindingConflict;
    const authority = state.checkpoint_authority orelse return error.SourceBindingConflict;
    if (!state.scope_fencing or source.destination != .native or source.table_id != query.actor_table_id or !std.meta.eql(source.source_id, query.source_id) or authority.owner.metadata_group_id != query.metadata_group_id or !std.meta.eql(authority.owner.metadata_incarnation, incarnation)) return error.SourceBindingConflict;
    const spec: native.Spec = .{ .actor_table_id = source.table_id, .source_id = source.source_id, .generation = source.generation, .binding = source.identity(), .checkpoint_owner = try native.Basis.fromOwner(authority.owner) };
    try spec.validate();
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
    return .{ .cut = .{ .group_id = query.metadata_group_id, .incarnation = incarnation, .index = applied_index }, .spec = spec, .phase = if (state.retired) .retired else if (state.closing) .closing else .active, .actor_revision = state.revision, .live_definition = if (definition) |value| std.meta.eql(value, source.definition) else false, .checkpoint_table = try a.dupe(u8, authority.table_name), .pending = pending };
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
