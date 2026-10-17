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

//! Raft-authoritative private index admission, drain and allocation controls.
//! Positions come only from committed-entry apply, never a request body.
//! Transaction helpers do not commit or authenticate metadata; every error
//! requires abort. Receipt recovery verifies the same durable apply watermark.
const std = @import("std");
const A = std.mem.Allocator;
const jobs = @import("relational_index_owner_job.zig");
const inputs = @import("relational_index_inputs.zig");
const preparation = @import("relational_index_preparation.zig");
const registry = @import("schema_registry.zig");
const topology = @import("relational_integrity_topology.zig");
const catalog = @import("relational_integrity_catalog.zig");
const physical = @import("relational_index_catalog.zig");
const authority = @import("../source_authority.zig");
const receipts = @import("apply_receipts.zig");
const publication = @import("relational_index_publication_progress.zig");
const Entry = receipts.OrderedApplyReceipt;

const contract = @import("relational_index_raft_contract.zig");
pub const Effect = contract.Effect;
pub const Command = contract.Command;
pub const Identity = contract.Identity;
pub const Observation = contract.Observation;
pub const validateBatchRequest = contract.validateBatchRequest;
pub const Admission = @import("relational_index_cancellation_intent.zig").Intent;
pub const prefix = jobs.key ++ ":raft:";

pub const Cut = struct {
    entry: Entry,
    evidence: [32]u8,
};
pub const Outcome = struct { binding: jobs.Binding, cut: Cut, duplicate: bool };

fn key(effect: Effect) [prefix.len + 1]u8 {
    var result: [prefix.len + 1]u8 = undefined;
    @memcpy(result[0..prefix.len], prefix);
    result[prefix.len] = @backingInt(effect);
    return result;
}
fn hash(bytes: []const u8) [32]u8 {
    return inputs.digest(bytes);
}
fn context(binding: jobs.Binding) ![32]u8 {
    var original = binding;
    original.phase = .fencing;
    return hash(&try original.encode());
}
fn evidence(binding: jobs.Binding, effect: Effect, entry: Entry) ![32]u8 {
    return contract.effectEvidence(binding, effect, .{ .raft = .{ .term = entry.term, .index = entry.index } });
}
fn optional(txn: anytype, name: []const u8) !?[]const u8 {
    return txn.get(name) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}
fn requireAuthority(txn: anytype, binding: jobs.Binding) !void {
    _ = try binding.encode();
    if (binding.authority != .raft) return error.InvalidControlReceiptPosition;
    const actual = (try @import("doc_identity.zig").loadNamespaceTxn(txn)) orelse return error.IdentityNamespaceMismatch;
    if (!actual.eql(binding.fence.namespace)) return error.IdentityNamespaceMismatch;
    _ = try authority.require(txn, .raft, @import("online_source_contract.zig").namespaceBytes(actual));
}
fn marker(txn: anytype) !?Entry {
    return receipts.readOrderedApplyReceiptTxn(txn);
}

/// Fixed slots retain the original committed cut even when a retry is applied
/// at a later log position. Old-job slots are inert, not an unbounded history.
fn loadCut(txn: anytype, binding: jobs.Binding, effect: Effect) !?Cut {
    const bytes = (try optional(txn, &key(effect))) orelse return null;
    if (bytes.len != 120 or !std.mem.eql(u8, bytes[0..4], "AIR1") or
        bytes[4] != @backingInt(effect) or !std.mem.allEqual(u8, bytes[5..8], 0) or
        !std.mem.eql(u8, bytes[88..120], &hash(bytes[0..88]))) return error.InvalidControlReceiptPosition;
    // Decode all borrowed fields before another transaction read.
    const entry: Entry = .{ .term = std.mem.readInt(u64, bytes[40..48], .little), .index = std.mem.readInt(u64, bytes[48..56], .little) };
    const stored_context = bytes[8..40].*;
    const stored_evidence = bytes[56..88].*;
    _ = try receipts.orderedApplyDisposition(null, entry);
    if (!std.mem.eql(u8, &stored_context, &try context(binding))) return null;
    if (!std.mem.eql(u8, &stored_evidence, &try evidence(binding, effect, entry))) return error.InvalidControlReceiptPosition;
    return .{ .entry = entry, .evidence = stored_evidence };
}
fn requirePrecedes(first: Entry, second: Entry) !void {
    if (first.index >= second.index or first.term > second.term) return error.InvalidControlReceiptPosition;
}
fn requireCommitted(cut: Cut, applied: Entry) !void {
    if (cut.entry.index > applied.index or cut.entry.term > applied.term or
        (cut.entry.index == applied.index and cut.entry.term != applied.term)) return error.InvalidControlReceiptPosition;
}

pub fn requireCuts(txn: anytype, binding: jobs.Binding) !void {
    try requireAuthority(txn, binding);
    const applied = (try marker(txn)) orelse return error.InvalidControlReceiptPosition;
    const admitted = (try loadCut(txn, binding, .admitted)) orelse return error.InvalidControlReceiptPosition;
    try requireCommitted(admitted, applied);
    const fenced = try loadCut(txn, binding, .fenced);
    const reserved = try loadCut(txn, binding, .reserved);
    if (fenced) |cut| {
        try requireCommitted(cut, applied);
        try requirePrecedes(admitted.entry, cut.entry);
    }
    if (binding.phase == .reserved or binding.phase == .installed) {
        const cut = reserved orelse return error.InvalidControlReceiptPosition;
        try requireCommitted(cut, applied);
        try requirePrecedes((fenced orelse return error.InvalidControlReceiptPosition).entry, cut.entry);
    } else if (reserved != null) return error.InvalidControlReceiptPosition;
}

fn currentState(txn: anytype, binding: jobs.Binding) !jobs.Binding {
    try requireAuthority(txn, binding);
    const current = (try jobs.load(txn)) orelse return error.PrivateIndexJobRequired;
    if (!std.mem.eql(u8, &try context(binding), &try context(current))) return error.PrivateIndexOwnerJobChanged;
    try @import("relational_index_cancellation_intent.zig").requireOpen(txn, binding);
    try jobs.requireActive(txn, binding.fence);
    const fence = (try topology.current(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!fence.eql(binding.fence)) return error.IntegrityTopologyChanged;
    try inputs.require(txn, current.inputs_digest);
    if (current.phase == .installed) return error.PrivateIndexPublicationRequired;
    const schema = (try optional(txn, @import("../schema.zig").schema_key)) orelse return error.RelationalIndexCatalogSchemaMismatch;
    if (schema.len > inputs.max_schema_bytes or !std.mem.eql(u8, &hash(schema), &binding.schema_before)) return error.RelationalIndexCatalogSchemaMismatch;
    const integrity = (try optional(txn, catalog.key)) orelse return error.IntegrityCatalogChanged;
    const expected = if (current.phase == .reserved) binding.integrity_reserved else binding.integrity_before;
    if (integrity.len > catalog.max_catalog_bytes or !std.mem.eql(u8, &hash(integrity), &expected)) return error.IntegrityCatalogChanged;
    const head = if (try optional(txn, physical.head_key)) |bytes| try physical.Head.decode(bytes) else null;
    const expected_head = if (current.phase == .reserved) binding.physical_reserved else binding.physical_before;
    if (expected_head) |value| {
        if (head == null or !head.?.eql(value)) return error.PreparedGenerationChanged;
    } else if (head != null) return error.PreparedGenerationChanged;
    try requireCuts(txn, current);
    if (current.phase == .reserved) try jobs.requireReserved(txn, current);
    return current;
}

/// Coherent bounded observation for an independently authorized receiver.
/// Absence grants nothing. Existing jobs must verify their durable phase cuts
/// and catalog preimage in this same snapshot, not just deserialize a binding.
pub fn inspect(txn: anytype, expected: Identity) !?jobs.Binding {
    try expected.validate();
    const actual = (try @import("doc_identity.zig").loadNamespaceTxn(txn)) orelse return error.IdentityNamespaceMismatch;
    if (!actual.eql(expected.namespace)) return error.IdentityNamespaceMismatch;
    _ = try authority.require(txn, .raft, @import("online_source_contract.zig").namespaceBytes(actual));
    const binding = (try jobs.load(txn)) orelse return null;
    if (binding.authority != .raft or !std.mem.eql(u8, &binding.id, &expected.id) or
        !std.mem.eql(u8, &binding.plan_digest, &expected.plan_digest) or
        !std.mem.eql(u8, &binding.owner_digest, &expected.owner_digest) or binding.ordinal != expected.ordinal or
        !binding.fence.namespace.eql(expected.namespace) or binding.fence.owner_group_id != expected.owner_group_id) return error.PrivateIndexOwnerJobChanged;
    return try currentState(txn, binding);
}

/// Read-only recovery never mints a cut from the current term/index.
pub fn receipt(txn: anytype, binding: jobs.Binding, effect: Effect) !?publication.Receipt {
    if (effect == .admitted) return error.InvalidControlReceiptPosition;
    const current = try currentState(txn, binding);
    if (effect == .reserved and current.phase != .reserved) return null;
    const cut = (try loadCut(txn, binding, effect)) orelse return null;
    return logicalReceipt(binding, effect, cut);
}

fn logicalReceipt(binding: jobs.Binding, effect: Effect, cut: Cut) publication.Receipt {
    return .{
        .owner = .{ .plan_id = binding.id, .plan_digest = binding.plan_digest, .ordinal = binding.ordinal, .group_id = binding.fence.owner_group_id, .namespace = binding.fence.namespace, .authority = .raft, .fence_digest = binding.owner_digest },
        .action = if (effect == .fenced) .fenced else .reserved,
        .position = .{ .raft = .{ .term = cut.entry.term, .index = cut.entry.index } },
        .candidate_digest = binding.integrity_candidate,
        .reservation_digest = binding.integrity_reserved,
        .evidence_digest = cut.evidence,
    };
}

pub fn observe(txn: anytype, expected: Identity) !Observation {
    const binding = try inspect(txn, expected);
    const applied = try marker(txn);
    var result: Observation = .{ .format_version = 1, .binding = binding, .applied = if (applied) |entry| .{ .raft = .{ .term = entry.term, .index = entry.index } } else null };
    if (binding) |value| {
        if (try loadCut(txn, value, .fenced)) |cut| result.fenced = logicalReceipt(value, .fenced, cut);
        if (try loadCut(txn, value, .reserved)) |cut| result.reserved = logicalReceipt(value, .reserved, cut);
    }
    try result.validate(expected);
    return result;
}
fn stageCut(txn: anytype, binding: jobs.Binding, effect: Effect, entry: Entry) !Cut {
    var bytes: [120]u8 = @splat(0);
    @memcpy(bytes[0..4], "AIR1");
    bytes[4] = @backingInt(effect);
    @memcpy(bytes[8..40], &try context(binding));
    std.mem.writeInt(u64, bytes[40..48], entry.term, .little);
    std.mem.writeInt(u64, bytes[48..56], entry.index, .little);
    const proof = try evidence(binding, effect, entry);
    @memcpy(bytes[56..88], &proof);
    @memcpy(bytes[88..120], &hash(bytes[0..88]));
    try txn.put(&key(effect), &bytes);
    return .{ .entry = entry, .evidence = proof };
}

pub const Prepared = struct {
    alloc: A,
    command: Command,
    artifacts: ?preparation.PreparedCandidate = null,

    pub fn deinit(self: *Prepared) void {
        if (self.command.inputs_b64) |bytes| self.alloc.free(bytes);
        if (self.artifacts) |*value| value.deinit();
        self.* = undefined;
    }
    pub fn admission(a: A, before: registry.SchemaView, txn: anytype, header: Admission, manifest: []const u8) !Prepared {
        try header.validate();
        var artifacts = try preparation.PreparedCandidate.init(a, before, txn, manifest, .raft);
        errdefer artifacts.deinit();
        if (!std.mem.eql(u8, &hash(artifacts.candidate.next.value), &header.candidate_digest) or
            !std.mem.eql(u8, &hash(artifacts.candidate.reservation.value), &header.reservation_digest)) return error.InvalidIndexPublicationCandidate;
        const binding = try jobs.prepare(header.id, header.plan_digest, header.owner_digest, header.ordinal, header.fence, .raft, &artifacts.candidate, &artifacts.addition);
        const b64 = try a.alloc(u8, std.base64.standard.Encoder.calcSize(manifest.len));
        _ = std.base64.standard.Encoder.encode(b64, manifest);
        return .{ .alloc = a, .command = .{ .effect = .admitted, .binding = binding, .inputs_b64 = b64 }, .artifacts = artifacts };
    }
    pub fn control(a: A, before: registry.SchemaView, txn: anytype, command: Command) !Prepared {
        try command.validate();
        var result: Prepared = .{ .alloc = a, .command = command };
        result.command.inputs_b64 = null;
        errdefer result.deinit();
        if (command.inputs_b64) |b64| {
            const length = std.base64.standard.Decoder.calcSizeForSlice(b64) catch return error.InvalidPrivateIndexInputs;
            if (length > inputs.max_bytes) return error.InvalidPrivateIndexInputs;
            const bytes = try a.alloc(u8, length);
            defer a.free(bytes);
            std.base64.standard.Decoder.decode(bytes, b64) catch return error.InvalidPrivateIndexInputs;
            if (!std.mem.eql(u8, &hash(bytes), &command.binding.inputs_digest)) return error.InvalidPrivateIndexInputs;
            result.artifacts = try preparation.PreparedCandidate.init(a, before, txn, bytes, .raft);
            result.command.inputs_b64 = try a.dupe(u8, b64);
        } else if (command.effect == .reserved) {
            var manifest = try inputs.Prepared.load(a, txn, command.binding.inputs_digest);
            defer manifest.deinit();
            result.artifacts = try preparation.PreparedCandidate.init(a, before, txn, manifest.bytes, .raft);
        }
        if (result.artifacts) |*artifacts| {
            const b = command.binding;
            const actual = try jobs.prepare(b.id, b.plan_digest, b.owner_digest, b.ordinal, b.fence, .raft, &artifacts.candidate, &artifacts.addition);
            if (!std.mem.eql(u8, &try actual.encode(), &try b.encode())) return error.PrivateIndexOwnerJobChanged;
        }
        return result;
    }
    /// Caller has authenticated the owner group and holds apply exclusion.
    /// This stages no apply watermark: commit it with the returned effect.
    pub fn stage(self: *Prepared, core: anytype, txn: anytype, owner_group: u64, entry: Entry) !Outcome {
        try self.command.validate();
        const binding = self.command.binding;
        if (owner_group == 0 or owner_group != binding.fence.owner_group_id) return error.PrivateIndexOwnerJobChanged;
        if (!binding.fence.namespace.eql(core.identity_namespace)) return error.IdentityNamespaceMismatch;
        try requireAuthority(txn, binding);
        if (try receipts.orderedApplyDisposition(try marker(txn), entry) != .apply) return error.InvalidControlReceiptPosition;
        if (try marker(txn)) |prior| try requirePrecedes(prior, entry);
        if (try jobs.load(txn) != null) {
            _ = try currentState(txn, binding);
            if (try loadCut(txn, binding, self.command.effect)) |cut| return .{ .binding = binding, .cut = cut, .duplicate = true };
        } else if (self.command.effect != .admitted) return error.PrivateIndexJobRequired;
        switch (self.command.effect) {
            .admitted => {
                const artifacts = if (self.artifacts) |*value| value else return error.InvalidPrivateIndexOwnerJob;
                if (!try jobs.stageBegin(txn, binding, &artifacts.candidate.inputs)) return error.InvalidControlReceiptPosition;
            },
            .fenced => {
                const current = try currentState(txn, binding);
                if (current.phase != .fencing) return error.InvalidPrivateIndexOwnerJob;
                const admitted = (try loadCut(txn, binding, .admitted)) orelse return error.InvalidControlReceiptPosition;
                try requirePrecedes(admitted.entry, entry);
                var manager = try core.initTxnManager();
                defer manager.deinit();
                try topology.requireDrained(txn, &manager, binding.fence);
            },
            .reserved => {
                const fenced = (try loadCut(txn, binding, .fenced)) orelse return error.InvalidControlReceiptPosition;
                try requirePrecedes(fenced.entry, entry);
                const artifacts = if (self.artifacts) |*value| value else return error.InvalidPrivateIndexOwnerJob;
                var manager = try core.initTxnManager();
                defer manager.deinit();
                if (!try jobs.stageReservation(txn, &manager, binding, &artifacts.candidate, &artifacts.addition)) return error.InvalidControlReceiptPosition;
            },
        }
        return .{ .binding = binding, .cut = try stageCut(txn, binding, self.command.effect, entry), .duplicate = false };
    }
};
