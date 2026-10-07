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

//! Receiver-local aggregate projection evidence: one current record per index,
//! not per document or historical epoch. Issuance requires a retained physical
//! generation guard, and consumers must fence the receiver's lifecycle epoch.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const epoch = @import("artifact_projection_epoch.zig");
const prefix = "\x00\x00__artifact_publication__:projection:";
pub const Key = [prefix.len + 32]u8;
pub const Encoded = [318]u8;
const SourceGuard = @import("artifact_source_gap.zig").Guard;

pub fn key(index_name: []const u8) Key {
    var result: Key = undefined;
    @memcpy(result[0..prefix.len], prefix);
    std.crypto.hash.sha2.Sha256.hash(index_name, result[prefix.len..], .{});
    return result;
}

pub const Certificate = struct {
    authority: publication.Authority,
    root: u128,
    lifecycle: u64,
    requirement: publication.Digest,
    applied_sequence: u64,
    baseline: ?SourceGuard = null,

    pub fn sameIdentity(self: Certificate, other: Certificate) bool {
        return std.meta.eql(self.authority, other.authority) and self.root == other.root and
            self.lifecycle == other.lifecycle and std.mem.eql(u8, &self.requirement, &other.requirement) and std.meta.eql(self.baseline, other.baseline);
    }

    pub fn encode(self: Certificate, selected: *const Key) !Encoded {
        if (self.root == 0 or self.authority.epoch == 0 or std.mem.allEqual(u8, &self.authority.namespace, 0) or
            std.mem.allEqual(u8, &self.authority.catalog_digest, 0) or std.mem.allEqual(u8, &self.requirement, 0) or
            self.applied_sequence == std.math.maxInt(u64)) return error.ArtifactCatalogCorrupt;
        if (self.baseline) |guard| {
            const boundary = guard.boundary orelse return error.ArtifactCatalogCorrupt;
            if (!std.meta.eql(boundary.authority, self.authority) or self.applied_sequence < boundary.replay_sequence) return error.ArtifactCatalogCorrupt;
        }
        var raw: Encoded = @splat(0);
        @memcpy(raw[0..4], "APC2");
        @memcpy(raw[4..28], &self.authority.namespace);
        std.mem.writeInt(u64, raw[28..36], self.authority.epoch, .little);
        @memcpy(raw[36..68], &self.authority.catalog_digest);
        std.mem.writeInt(u128, raw[68..84], self.root, .little);
        std.mem.writeInt(u64, raw[84..92], self.lifecycle, .little);
        @memcpy(raw[92..124], &self.requirement);
        std.mem.writeInt(u64, raw[124..132], self.applied_sequence, .little);
        raw[132] = @intFromBool(self.baseline != null);
        if (self.baseline) |guard| @memcpy(raw[133..286], &try guard.encode());
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(selected);
        hash.update(raw[0..286]);
        hash.final(raw[286..318]);
        return raw;
    }

    pub fn decode(raw: []const u8, selected: *const Key) !Certificate {
        if (raw.len != @sizeOf(Encoded) or !std.mem.eql(u8, raw[0..4], "APC2") or raw[132] > 1 or (raw[132] == 0 and !std.mem.allEqual(u8, raw[133..286], 0))) return error.ArtifactCatalogCorrupt;
        const result: Certificate = .{ .authority = .{ .namespace = raw[4..28].*, .epoch = std.mem.readInt(u64, raw[28..36], .little), .catalog_digest = raw[36..68].* }, .root = std.mem.readInt(u128, raw[68..84], .little), .lifecycle = std.mem.readInt(u64, raw[84..92], .little), .requirement = raw[92..124].*, .applied_sequence = std.mem.readInt(u64, raw[124..132], .little), .baseline = if (raw[132] == 1) try SourceGuard.decode(raw[133..286]) else null };
        if (!std.mem.eql(u8, raw, &try result.encode(selected))) return error.ArtifactCatalogCorrupt;
        return result;
    }

    pub fn requireCurrent(self: Certificate, txn: anytype, root: u128, requirement: publication.Digest, through: u64) !void {
        if (root != self.root or !std.mem.eql(u8, &requirement, &self.requirement)) return error.EnrichmentSourceChanged;
        const active = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
        if (!std.meta.eql(active, self.authority)) return error.ArtifactCatalogDrift;
        try epoch.requireCurrent(txn, self.lifecycle);
        if (self.applied_sequence < through) return error.ArtifactPublicationPending;
    }

    pub fn requireBaseline(self: Certificate, txn: anytype) !SourceGuard {
        const guard = self.baseline orelse return error.ArtifactPublicationPending;
        try guard.requireCurrent(txn);
        return guard;
    }
};

pub fn load(txn: anytype, selected: *const Key) !?Certificate {
    const raw = txn.get(selected) catch |err| if (err == error.NotFound) return null else return err;
    return try Certificate.decode(raw, selected);
}

/// Both leases outlive this call and the caller's commit. The node must come
/// from the caller's canonical pinned completion plan; no command payload can
/// supply or install a certificate. The physical guard repeats all final
/// receiver-local metadata fences in this same transaction.
pub fn stageFullText(
    txn: anytype,
    guard: *const @import("catalog/index_manager.zig").IndexManager.FullTextProjectionGuard,
    node: *const @import("artifact_completion_plan.zig").Node,
) !bool {
    try node.requireValidIdentity();
    if (node.kind != .index_projection or node.index_kind != .full_text or
        node.generation != guard.seal.generation or !std.mem.eql(u8, node.name, guard.index_name)) return error.ArtifactCatalogDrift;
    try guard.requireCurrent(txn);
    const selected = key(node.name);
    const baseline: ?SourceGuard = if (guard.seal.baseline) |source| blk: {
        source.requireCurrent(txn) catch |err| switch (err) {
            error.EnrichmentSourceChanged, error.ArtifactCatalogDrift => break :blk null,
            else => return err,
        };
        if (guard.checkpoint.applied_sequence < source.boundary.?.replay_sequence) break :blk null;
        break :blk source;
    } else null;
    const next: Certificate = .{ .authority = guard.snapshot.authority, .root = guard.seal.root, .lifecycle = guard.snapshot.epoch, .requirement = node.id, .applied_sequence = guard.checkpoint.applied_sequence, .baseline = baseline };
    // This is reconstructible evidence, not source authority. A fully checked
    // physical guard may replace a corrupt old cache record; readers still
    // fail closed until that replacement commits.
    const previous = load(txn, &selected) catch |err| switch (err) {
        error.ArtifactCatalogCorrupt => null,
        else => return err,
    };
    if (previous) |old| {
        if (old.sameIdentity(next) and old.applied_sequence >= next.applied_sequence) return false;
    }
    try txn.put(&selected, &try next.encode(&selected));
    return true;
}

pub fn remove(txn: anytype, index_name: []const u8) !void {
    txn.delete(&key(index_name)) catch |err| if (err != error.NotFound) return err;
}

pub const Closure = struct {
    alloc: std.mem.Allocator,
    document: []u8,
    root: u128,
    requirement: publication.Digest,
    observation: @import("artifact_stream_observation.zig").Observation,
    selected: Key,
    lifecycle: u64,
    source_sequence: u64,
    baseline: ?SourceGuard = null,

    pub fn deinit(self: *@This()) void {
        self.alloc.free(self.document);
        self.* = undefined;
    }

    /// Fixed metadata probes only; never reopen a physical index in the
    /// serialized primary writer. A newer same-epoch certificate may satisfy
    /// this exact source cut without invalidating already prepared work.
    pub fn requireCurrent(self: Closure, txn: anytype, root: u128) !void {
        if (root != self.root) return error.EnrichmentSourceChanged;
        try self.observation.requireCurrent(txn, self.document);
        const source = try publication.materializationState(txn, self.observation.authority.namespace, self.document);
        if (self.baseline) |guard| {
            if (source != null and source.?.replay_sequence != null) return error.EnrichmentSourceChanged;
            try guard.requireCurrent(txn);
        } else if (source == null or source.?.replay_sequence != self.source_sequence) return error.EnrichmentSourceChanged;
        try epoch.requireCurrent(txn, self.lifecycle);
        const certificate = (try load(txn, &self.selected)) orelse return error.EnrichmentSourceChanged;
        try certificate.requireCurrent(txn, root, self.requirement, self.source_sequence);
        if (self.baseline) |guard| if (!std.meta.eql(guard, try certificate.requireBaseline(txn))) return error.EnrichmentSourceChanged;
    }
};

pub fn prepareClosure(alloc: std.mem.Allocator, txn: anytype, root: u128, document: []const u8, node: *const @import("artifact_completion_plan.zig").Node) !Closure {
    if (node.kind != .index_projection or node.index_kind != .full_text) return error.ArtifactPublicationPending;
    try node.requireValidIdentity();
    const observation = try @import("artifact_stream_observation.zig").Observation.capture(txn, document);
    const selected = key(node.name);
    const certificate = (try load(txn, &selected)) orelse return error.ArtifactPublicationPending;
    const source = try publication.materializationState(txn, observation.authority.namespace, document);
    const replay_sequence = if (source) |state| state.replay_sequence else null;
    const baseline = if (replay_sequence == null) try certificate.requireBaseline(txn) else null;
    // A full snapshot includes historical and already-unjournaled rows. Its
    // unchanged source-gap guard proves no later unjournaled mutation escaped
    // replay; the observation additionally fences this exact row revision.
    const sequence = replay_sequence orelse baseline.?.boundary.?.replay_sequence;
    certificate.requireCurrent(txn, root, node.id, sequence) catch |err| switch (err) {
        error.ArtifactCatalogDrift, error.EnrichmentSourceChanged => return error.ArtifactPublicationPending,
        else => return err,
    };
    return .{ .alloc = alloc, .document = try alloc.dupe(u8, document), .root = root, .requirement = node.id, .observation = observation, .selected = selected, .lifecycle = certificate.lifecycle, .source_sequence = sequence, .baseline = baseline };
}

test "ordered artifact inventory projection certificate binds every byte to its index slot" {
    const selected = key("text");
    const certificate: Certificate = .{ .authority = .{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) }, .root = 3, .lifecycle = 4, .requirement = @splat(5), .applied_sequence = 6 };
    const raw = try certificate.encode(&selected);
    try std.testing.expectEqualDeep(certificate, try Certificate.decode(&raw, &selected));
    try std.testing.expectError(error.ArtifactCatalogCorrupt, Certificate.decode(&raw, &key("other")));
    for (0..raw.len) |i| {
        var corrupt = raw;
        corrupt[i] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, Certificate.decode(&corrupt, &selected));
        try std.testing.expectError(error.ArtifactCatalogCorrupt, Certificate.decode(raw[0..i], &selected));
    }
    var adopted = certificate;
    adopted.baseline = .{ .boundary = .{ .authority = certificate.authority, .replay_sequence = 2 }, .gap_epoch = 1 };
    try std.testing.expectEqualDeep(adopted, try Certificate.decode(&try adopted.encode(&selected), &selected));
    adopted.baseline.?.boundary.?.authority.epoch += 1;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, adopted.encode(&selected));
}
