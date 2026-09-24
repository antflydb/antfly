// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Parent-replicated accepted child-FK generations. A scope is permanent even
//! after its last FK is dropped: absence means legacy/unregistered, whereas a
//! present scope with no active generation denies every delayed attach.
const std = @import("std");
const integrity = @import("relational_integrity_contract.zig");

pub const prefix = "\x00\x00__metadata__:relational_integrity_child_generation:";
pub const staged_receipt_key = "\x00\x00__metadata__:relational_integrity_child_generation_staged";
pub const activation_receipt_key = "\x00\x00__metadata__:relational_integrity_child_generation_activation";
pub const acknowledged_receipt_key = "\x00\x00__metadata__:relational_integrity_child_generation_acknowledged";
pub const cancel_receipt_key = "\x00\x00__metadata__:relational_integrity_child_generation_canceled";
pub const source_cancel_receipt_key = "\x00\x00__metadata__:relational_integrity_child_source_canceled";
pub const source_fence_receipt_key = "\x00\x00__metadata__:relational_integrity_child_source_fenced";
pub const source_install_receipt_key = "\x00\x00__metadata__:relational_integrity_child_source_installed";
const magic = "AIGA";
const header_len = 116;
const checksum_len = 32;
const max_name_len = 256;

pub const Phase = enum(u8) { staged = 1, active = 2 };

pub const Scope = struct {
    child_table_id: u64,
    child_table_name: []const u8,
    constraint_name: []const u8,
    revision: u64,
    phase: Phase,
    active_generation: ?integrity.Generation,
    staged_generation: ?integrity.Generation = null,
    staged_child_table_id: u64 = 0,
    plan_id: [16]u8,
    decision_digest: integrity.Digest,

    pub fn validate(self: Scope) !void {
        if (self.child_table_id == 0 or self.revision == 0 or
            self.child_table_name.len == 0 or self.child_table_name.len > max_name_len or
            self.constraint_name.len == 0 or self.constraint_name.len > max_name_len or
            !std.unicode.utf8ValidateSlice(self.child_table_name) or
            !std.unicode.utf8ValidateSlice(self.constraint_name) or
            std.mem.allEqual(u8, &self.plan_id, 0) or
            std.mem.allEqual(u8, &self.decision_digest, 0)) return error.InvalidGenerationAdmission;
        if (self.active_generation) |generation| if (std.mem.allEqual(u8, &generation, 0)) return error.InvalidGenerationAdmission;
        if (self.staged_generation) |generation| if (std.mem.allEqual(u8, &generation, 0)) return error.InvalidGenerationAdmission;
        if (self.phase == .active and (self.staged_generation != null or self.staged_child_table_id != 0)) return error.InvalidGenerationAdmission;
        if (self.phase == .staged and (self.staged_generation != null) != (self.staged_child_table_id != 0)) return error.InvalidGenerationAdmission;
        if (self.staged_generation != null and self.active_generation != null and
            std.mem.eql(u8, &self.staged_generation.?, &self.active_generation.?)) return error.InvalidGenerationAdmission;
    }

    pub fn encode(self: Scope, alloc: std.mem.Allocator) ![]u8 {
        try self.validate();
        const bytes = try alloc.alloc(u8, header_len + self.child_table_name.len + self.constraint_name.len + checksum_len);
        @memcpy(bytes[0..4], magic);
        bytes[4] = 1;
        bytes[5] = @intFromEnum(self.phase);
        @memset(bytes[6..8], 0);
        std.mem.writeInt(u64, bytes[8..16], self.revision, .little);
        std.mem.writeInt(u64, bytes[16..24], self.child_table_id, .little);
        @memcpy(bytes[24..40], if (self.active_generation) |generation| &generation else &([_]u8{0} ** 16));
        @memcpy(bytes[40..56], if (self.staged_generation) |generation| &generation else &([_]u8{0} ** 16));
        @memcpy(bytes[56..72], &self.plan_id);
        @memcpy(bytes[72..104], &self.decision_digest);
        std.mem.writeInt(u64, bytes[104..112], self.staged_child_table_id, .little);
        std.mem.writeInt(u16, bytes[112..114], @intCast(self.child_table_name.len), .little);
        std.mem.writeInt(u16, bytes[114..116], @intCast(self.constraint_name.len), .little);
        @memcpy(bytes[116..][0..self.child_table_name.len], self.child_table_name);
        @memcpy(bytes[116 + self.child_table_name.len ..][0..self.constraint_name.len], self.constraint_name);
        std.crypto.hash.sha2.Sha256.hash(bytes[0 .. bytes.len - checksum_len], bytes[bytes.len - checksum_len ..][0..checksum_len], .{});
        return bytes;
    }

    pub fn decode(bytes: []const u8) !Scope {
        if (bytes.len < header_len + 2 + checksum_len or !std.mem.eql(u8, bytes[0..4], magic) or
            bytes[4] != 1 or !std.mem.allEqual(u8, bytes[6..8], 0)) return error.InvalidGenerationAdmission;
        const phase: Phase = switch (bytes[5]) {
            1 => .staged,
            2 => .active,
            else => return error.InvalidGenerationAdmission,
        };
        const child_len = std.mem.readInt(u16, bytes[112..114], .little);
        const constraint_len = std.mem.readInt(u16, bytes[114..116], .little);
        if (child_len == 0 or child_len > max_name_len or constraint_len == 0 or constraint_len > max_name_len or
            bytes.len != header_len + @as(usize, child_len) + @as(usize, constraint_len) + checksum_len) return error.InvalidGenerationAdmission;
        var digest: integrity.Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes[0 .. bytes.len - checksum_len], &digest, .{});
        if (!std.mem.eql(u8, &digest, bytes[bytes.len - checksum_len ..])) return error.InvalidGenerationAdmission;
        const active = bytes[24..40].*;
        const staged = bytes[40..56].*;
        const scope: Scope = .{
            .child_table_id = std.mem.readInt(u64, bytes[16..24], .little),
            .child_table_name = bytes[116..][0..child_len],
            .constraint_name = bytes[116 + child_len ..][0..constraint_len],
            .revision = std.mem.readInt(u64, bytes[8..16], .little),
            .phase = phase,
            .active_generation = if (std.mem.allEqual(u8, &active, 0)) null else active,
            .staged_generation = if (std.mem.allEqual(u8, &staged, 0)) null else staged,
            .staged_child_table_id = std.mem.readInt(u64, bytes[104..112], .little),
            .plan_id = bytes[56..72].*,
            .decision_digest = bytes[72..104].*,
        };
        try scope.validate();
        return scope;
    }

    pub fn accepts(self: Scope, reference: integrity.Reference) bool {
        return std.mem.eql(u8, self.child_table_name, reference.child_table) and
            std.mem.eql(u8, self.constraint_name, reference.constraint_name) and
            (if (self.active_generation) |generation| std.mem.eql(u8, &generation, &reference.constraint_generation) else false);
    }
};

pub const Transition = struct {
    child_table_id: u64,
    child_table_name: []const u8,
    constraint_name: []const u8,
    expected_generation: ?integrity.Generation,
    next_generation: ?integrity.Generation,
    plan_id: [16]u8,
    decision_digest: integrity.Digest,

    pub fn validate(self: Transition) !void {
        if (self.child_table_id == 0 or self.child_table_name.len == 0 or self.constraint_name.len == 0 or
            self.child_table_name.len > max_name_len or self.constraint_name.len > max_name_len or
            std.mem.allEqual(u8, &self.plan_id, 0) or std.mem.allEqual(u8, &self.decision_digest, 0) or
            (self.expected_generation == null and self.next_generation == null)) return error.InvalidGenerationAdmission;
        if (self.expected_generation) |generation| if (std.mem.allEqual(u8, &generation, 0)) return error.InvalidGenerationAdmission;
        if (self.next_generation) |generation| if (std.mem.allEqual(u8, &generation, 0)) return error.InvalidGenerationAdmission;
        if (self.expected_generation != null and self.next_generation != null and
            std.mem.eql(u8, &self.expected_generation.?, &self.next_generation.?)) return error.InvalidGenerationAdmission;
    }
};

/// One parent group can own several FK constraints for the same child. Their
/// accepted generations and the topology release are a single Raft decision:
/// publishing one at a time would reopen admission between constraints.
pub fn validateTransitions(transitions: []const Transition) !void {
    if (transitions.len == 0 or transitions.len > 128) return error.InvalidGenerationAdmission;
    const first = transitions[0];
    try first.validate();
    for (transitions[1..], 1..) |transition, index| {
        try transition.validate();
        if (transition.child_table_id != first.child_table_id or
            !std.mem.eql(u8, transition.child_table_name, first.child_table_name) or
            !std.mem.eql(u8, &transition.plan_id, &first.plan_id) or
            !std.mem.eql(u8, &transition.decision_digest, &first.decision_digest) or
            std.mem.order(u8, transitions[index - 1].constraint_name, transition.constraint_name) != .lt)
            return error.InvalidGenerationAdmission;
    }
}

/// Canonical, fence-independent batch identity used by metadata and owner
/// receipts. Names are prefix-free through scopeKey; order is validated.
pub fn transitionsDigest(transitions: []const Transition) !integrity.Digest {
    try validateTransitions(transitions);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly accepted child FK generation transitions v1");
    var id: [8]u8 = undefined;
    std.mem.writeInt(u64, &id, @intCast(transitions.len), .little);
    hash.update(&id);
    for (transitions) |transition| {
        const key = try scopeKey(transition.child_table_name, transition.constraint_name);
        hash.update(&key);
        std.mem.writeInt(u64, &id, transition.child_table_id, .little);
        hash.update(&id);
        hash.update(if (transition.expected_generation) |generation| &generation else &([_]u8{0} ** 16));
        hash.update(if (transition.next_generation) |generation| &generation else &([_]u8{0} ** 16));
    }
    var digest: integrity.Digest = undefined;
    hash.final(&digest);
    return digest;
}

/// Name lengths make the hash encoding prefix-free. The encoded value repeats
/// both names and is checked on read, so a hash collision fails closed.
pub fn scopeKey(child_table: []const u8, constraint_name: []const u8) ![prefix.len + 32]u8 {
    if (child_table.len == 0 or child_table.len > max_name_len or constraint_name.len == 0 or constraint_name.len > max_name_len)
        return error.InvalidGenerationAdmission;
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly parent accepted child FK generation v1");
    var len: [2]u8 = undefined;
    std.mem.writeInt(u16, &len, @intCast(child_table.len), .little);
    state.update(&len);
    state.update(child_table);
    std.mem.writeInt(u16, &len, @intCast(constraint_name.len), .little);
    state.update(&len);
    state.update(constraint_name);
    var key: [prefix.len + 32]u8 = undefined;
    @memcpy(key[0..prefix.len], prefix);
    state.final(key[prefix.len..][0..32]);
    return key;
}

pub fn load(txn: anytype, child_table: []const u8, constraint_name: []const u8) !?Scope {
    const key = try scopeKey(child_table, constraint_name);
    const bytes = txn.get(&key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
    const scope = try Scope.decode(bytes);
    if (!std.mem.eql(u8, scope.child_table_name, child_table) or
        !std.mem.eql(u8, scope.constraint_name, constraint_name)) return error.InvalidGenerationAdmission;
    return scope;
}

pub fn excludes(txn: anytype, reference: integrity.Reference) !bool {
    const scope = (try load(txn, reference.child_table, reference.constraint_name)) orelse return false;
    return !scope.accepts(reference);
}

/// Switch the accepted generation in the same replicated owner transaction
/// that installs the old-generation tombstone and releases its topology fence.
/// An absent scope is the first publication for this child/constraint pair;
/// subsequent publications must retire exactly its currently accepted value.
pub fn activateTruncate(alloc: std.mem.Allocator, txn: anytype, entry: @import("relational_integrity_topology_contract.zig").ParentRetirementEntry, plan_id: [16]u8, decision_digest: integrity.Digest) !void {
    const key = try scopeKey(entry.child_table_name, entry.constraint_name);
    const previous = try load(txn, entry.child_table_name, entry.constraint_name);
    if (previous) |prior| {
        if (prior.phase != .active or prior.active_generation == null or
            !std.mem.eql(u8, &prior.active_generation.?, &entry.generation)) return error.GenerationAdmissionChanged;
    }
    const scope: Scope = .{
        .child_table_id = entry.child_table_id,
        .child_table_name = entry.child_table_name,
        .constraint_name = entry.constraint_name,
        .revision = if (previous) |prior| std.math.add(u64, prior.revision, 1) catch return error.GenerationAdmissionRevisionExhausted else 1,
        .phase = .active,
        .active_generation = entry.next_generation,
        .plan_id = plan_id,
        .decision_digest = decision_digest,
    };
    const encoded = try scope.encode(alloc);
    defer alloc.free(encoded);
    try txn.put(&key, encoded);
}

/// Normal child-FK schema publication stages the parent decision while the
/// child is fenced. An absent scope becomes default-deny, so a newly added FK
/// cannot attach until every parent owner activates the exact generation.
pub fn stageTransition(alloc: std.mem.Allocator, txn: anytype, transition: Transition) !void {
    try transition.validate();
    const key = try scopeKey(transition.child_table_name, transition.constraint_name);
    const before = try load(txn, transition.child_table_name, transition.constraint_name);
    if (before) |scope| {
        if (scope.phase == .staged) {
            if ((if (transition.next_generation != null) scope.staged_child_table_id else scope.child_table_id) == transition.child_table_id and
                std.mem.eql(u8, &scope.plan_id, &transition.plan_id) and
                std.mem.eql(u8, &scope.decision_digest, &transition.decision_digest) and
                std.meta.eql(scope.active_generation, transition.expected_generation) and
                std.meta.eql(scope.staged_generation, transition.next_generation)) return;
            return error.GenerationAdmissionChanged;
        }
        if (!std.meta.eql(scope.active_generation, transition.expected_generation)) return error.GenerationAdmissionChanged;
    }
    const scope: Scope = .{
        .child_table_id = if (before) |prior| prior.child_table_id else transition.child_table_id,
        .child_table_name = transition.child_table_name,
        .constraint_name = transition.constraint_name,
        .revision = if (before) |prior| std.math.add(u64, prior.revision, 1) catch return error.GenerationAdmissionRevisionExhausted else 1,
        .phase = .staged,
        .active_generation = transition.expected_generation,
        .staged_generation = transition.next_generation,
        .staged_child_table_id = if (transition.next_generation != null) transition.child_table_id else 0,
        .plan_id = transition.plan_id,
        .decision_digest = transition.decision_digest,
    };
    const encoded = try scope.encode(alloc);
    defer alloc.free(encoded);
    try txn.put(&key, encoded);
}

/// The caller must have fetched a fresh metadata leader decision and proposed
/// it through the owner's Raft log. Neither a coordinator token nor the
/// caller-supplied digest alone authorizes this irreversible flip.
pub fn activateTransition(alloc: std.mem.Allocator, txn: anytype, transition: Transition) !void {
    try transition.validate();
    const key = try scopeKey(transition.child_table_name, transition.constraint_name);
    const before = (try load(txn, transition.child_table_name, transition.constraint_name)) orelse return error.GenerationAdmissionChanged;
    if (before.phase == .active) {
        if (std.mem.eql(u8, &before.plan_id, &transition.plan_id) and
            std.mem.eql(u8, &before.decision_digest, &transition.decision_digest) and
            std.meta.eql(before.active_generation, transition.next_generation)) return;
        return error.GenerationAdmissionChanged;
    }
    if (!std.mem.eql(u8, &before.plan_id, &transition.plan_id) or
        !std.mem.eql(u8, &before.decision_digest, &transition.decision_digest) or
        !std.meta.eql(before.active_generation, transition.expected_generation) or
        !std.meta.eql(before.staged_generation, transition.next_generation)) return error.GenerationAdmissionChanged;
    var after = before;
    after.revision = std.math.add(u64, before.revision, 1) catch return error.GenerationAdmissionRevisionExhausted;
    after.phase = .active;
    after.active_generation = transition.next_generation;
    after.staged_generation = null;
    after.child_table_id = if (transition.next_generation != null) transition.child_table_id else before.child_table_id;
    after.staged_child_table_id = 0;
    const encoded = try after.encode(alloc);
    defer alloc.free(encoded);
    try txn.put(&key, encoded);
}

pub fn cancelTransition(alloc: std.mem.Allocator, txn: anytype, transition: Transition) !void {
    try transition.validate();
    const key = try scopeKey(transition.child_table_name, transition.constraint_name);
    const before = (try load(txn, transition.child_table_name, transition.constraint_name)) orelse return;
    if (before.phase == .active) {
        if (std.mem.eql(u8, &before.plan_id, &transition.plan_id) and
            !std.meta.eql(before.active_generation, transition.expected_generation)) return error.GenerationAdmissionChanged;
        return;
    }
    if (!std.mem.eql(u8, &before.plan_id, &transition.plan_id) or
        !std.mem.eql(u8, &before.decision_digest, &transition.decision_digest) or
        !std.meta.eql(before.active_generation, transition.expected_generation) or
        !std.meta.eql(before.staged_generation, transition.next_generation)) return error.GenerationAdmissionChanged;
    var after = before;
    after.revision = std.math.add(u64, before.revision, 1) catch return error.GenerationAdmissionRevisionExhausted;
    after.phase = .active;
    after.staged_generation = null;
    after.staged_child_table_id = 0;
    const encoded = try after.encode(alloc);
    defer alloc.free(encoded);
    try txn.put(&key, encoded);
}

pub fn completionReceipt(fence: @import("relational_integrity_topology_contract.zig").Fence, transitions: []const Transition) !integrity.Digest {
    try validateTransitions(transitions);
    const encoded = try fence.encode();
    if (fence.role != .child_generation_parent) return error.InvalidGenerationAdmission;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly accepted child FK generation owner activation v2");
    hash.update(&encoded);
    hash.update(&transitions[0].plan_id);
    hash.update(&transitions[0].decision_digest);
    const transitions_digest = try transitionsDigest(transitions);
    hash.update(&transitions_digest);
    var digest: integrity.Digest = undefined;
    hash.final(&digest);
    return digest;
}

pub fn stageReceipt(fence: @import("relational_integrity_topology_contract.zig").Fence, transitions: []const Transition) !integrity.Digest {
    const encoded = try fence.encode();
    if (fence.role != .child_generation_parent) return error.InvalidGenerationAdmission;
    const batch_digest = try transitionsDigest(transitions);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly accepted child FK generation owner stage v1");
    hash.update(&encoded);
    hash.update(&transitions[0].plan_id);
    hash.update(&transitions[0].decision_digest);
    hash.update(&batch_digest);
    var digest: integrity.Digest = undefined;
    hash.final(&digest);
    return digest;
}

pub fn cancelReceipt(fence: @import("relational_integrity_topology_contract.zig").Fence, transitions: ?[]const Transition) !integrity.Digest {
    const encoded = try fence.encode();
    if ((transitions == null and fence.role != .child_generation_source) or
        (transitions != null and fence.role != .child_generation_parent)) return error.InvalidGenerationAdmission;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly accepted child FK generation owner cancel v1");
    hash.update(&encoded);
    if (transitions) |items| {
        const batch_digest = try transitionsDigest(items);
        hash.update(&items[0].plan_id);
        hash.update(&items[0].decision_digest);
        hash.update(&batch_digest);
    }
    var digest: integrity.Digest = undefined;
    hash.final(&digest);
    return digest;
}

pub fn stageCanceledReceipt(txn: anytype, fence: @import("relational_integrity_topology_contract.zig").Fence, transitions: ?[]const Transition, term: u64, index: u64) !void {
    const digest = try cancelReceipt(fence, transitions);
    const encoded = (AppliedReceipt{ .digest = digest, .term = term, .index = index }).encode();
    try txn.put(if (transitions == null) source_cancel_receipt_key else cancel_receipt_key, &encoded);
}

pub fn sourceFenceDigest(fence: @import("relational_integrity_topology_contract.zig").Fence) !integrity.Digest {
    if (fence.role != .child_generation_source) return error.InvalidGenerationAdmission;
    const encoded = try fence.encode();
    var digest: integrity.Digest = undefined;
    std.crypto.hash.Blake3.hash(&encoded, &digest, .{});
    return digest;
}

pub fn stageSourceFencedReceipt(txn: anytype, fence: @import("relational_integrity_topology_contract.zig").Fence, term: u64, index: u64) !void {
    const digest = try sourceFenceDigest(fence);
    const encoded = (AppliedReceipt{ .digest = digest, .term = term, .index = index }).encode();
    try txn.put(source_fence_receipt_key, &encoded);
}

pub fn sourceInstallDigest(fence: @import("relational_integrity_topology_contract.zig").Fence, before_schema_json_digest: integrity.Digest, schema_json_digest: integrity.Digest, before_catalog_digest: integrity.Digest, after_catalog_digest: integrity.Digest) !integrity.Digest {
    const encoded_fence = try fence.encode();
    if (fence.role != .child_generation_source) return error.InvalidGenerationAdmission;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly child FK generation owner schema install v1");
    hash.update(&encoded_fence);
    hash.update(&before_schema_json_digest);
    hash.update(&schema_json_digest);
    hash.update(&before_catalog_digest);
    hash.update(&after_catalog_digest);
    var digest: integrity.Digest = undefined;
    hash.final(&digest);
    return digest;
}

pub const AppliedReceipt = struct {
    digest: integrity.Digest,
    term: u64,
    index: u64,

    pub fn encode(self: AppliedReceipt) [48]u8 {
        var encoded: [48]u8 = undefined;
        @memcpy(encoded[0..32], &self.digest);
        std.mem.writeInt(u64, encoded[32..40], self.term, .little);
        std.mem.writeInt(u64, encoded[40..48], self.index, .little);
        return encoded;
    }

    pub fn decode(bytes: []const u8) !AppliedReceipt {
        if (bytes.len != 48) return error.InvalidGenerationAdmission;
        return .{ .digest = bytes[0..32].*, .term = std.mem.readInt(u64, bytes[32..40], .little), .index = std.mem.readInt(u64, bytes[40..48], .little) };
    }
};

pub fn stageStagedReceipt(txn: anytype, fence: @import("relational_integrity_topology_contract.zig").Fence, transitions: []const Transition, term: u64, index: u64) !void {
    const receipt = try stageReceipt(fence, transitions);
    const encoded = (AppliedReceipt{ .digest = receipt, .term = term, .index = index }).encode();
    try txn.put(staged_receipt_key, &encoded);
}

pub const OwnerStatus = struct {
    fence: ?@import("relational_integrity_topology_contract.zig").Fence,
    completed: ?@import("relational_integrity_topology_contract.zig").Fence,
    staged_receipt: ?AppliedReceipt,
    activation_receipt: ?AppliedReceipt,
    cancel_receipt: ?AppliedReceipt,
    source_cancel_receipt: ?AppliedReceipt,
    source_fence_receipt: ?AppliedReceipt,
    source_install_receipt: ?AppliedReceipt,
    acknowledged_receipt: ?AppliedReceipt,
};

pub fn ownerStatus(txn: anytype) !OwnerStatus {
    const topology = @import("relational_integrity_topology.zig");
    return .{
        .fence = try topology.current(txn),
        .completed = try topology.completed(txn),
        .staged_receipt = try readAppliedReceipt(txn, staged_receipt_key),
        .activation_receipt = try readAppliedReceipt(txn, activation_receipt_key),
        .cancel_receipt = try readAppliedReceipt(txn, cancel_receipt_key),
        .source_cancel_receipt = try readAppliedReceipt(txn, source_cancel_receipt_key),
        .source_fence_receipt = try readAppliedReceipt(txn, source_fence_receipt_key),
        .source_install_receipt = try readAppliedReceipt(txn, source_install_receipt_key),
        .acknowledged_receipt = try readAppliedReceipt(txn, acknowledged_receipt_key),
    };
}

fn readAppliedReceipt(txn: anytype, key: []const u8) !?AppliedReceipt {
    const value = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try AppliedReceipt.decode(value);
}

pub fn stageCompletion(txn: anytype, fence: @import("relational_integrity_topology_contract.zig").Fence, transitions: []const Transition, term: u64, index: u64) !void {
    const receipt = try completionReceipt(fence, transitions);
    const encoded = (AppliedReceipt{ .digest = receipt, .term = term, .index = index }).encode();
    try txn.put(activation_receipt_key, &encoded);
    try txn.delete(acknowledged_receipt_key);
}

pub fn requireAcknowledged(txn: anytype) !void {
    const receipt = txn.get(activation_receipt_key) catch |err| {
        if (err == error.NotFound) return;
        return err;
    };
    const acknowledged = txn.get(acknowledged_receipt_key) catch |err| {
        if (err == error.NotFound) return error.GenerationAdmissionAcknowledgementPending;
        return err;
    };
    const applied = try AppliedReceipt.decode(receipt);
    const ack = try AppliedReceipt.decode(acknowledged);
    if (!std.mem.eql(u8, &applied.digest, &ack.digest)) return error.GenerationAdmissionAcknowledgementPending;
}

pub fn stageAcknowledgement(txn: anytype, fence: @import("relational_integrity_topology_contract.zig").Fence, transitions: []const Transition, term: u64, index: u64) !void {
    const receipt = try completionReceipt(fence, transitions);
    const stored = txn.get(activation_receipt_key) catch return error.GenerationAdmissionChanged;
    const applied = try AppliedReceipt.decode(stored);
    if (!std.mem.eql(u8, &applied.digest, &receipt)) return error.GenerationAdmissionChanged;
    for (transitions) |transition| {
        const scope = (try load(txn, transition.child_table_name, transition.constraint_name)) orelse return error.GenerationAdmissionChanged;
        if (scope.phase != .active or !std.mem.eql(u8, &scope.plan_id, &transition.plan_id) or
            !std.meta.eql(scope.active_generation, transition.next_generation)) return error.GenerationAdmissionChanged;
    }
    const topology = @import("relational_integrity_topology.zig");
    if (try topology.current(txn) != null) return error.IntegrityTopologyChanged;
    const completed = (try topology.completed(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!completed.eql(fence)) return error.IntegrityTopologyChanged;
    const encoded = (AppliedReceipt{ .digest = receipt, .term = term, .index = index }).encode();
    try txn.put(acknowledged_receipt_key, &encoded);
}

pub fn stageTransferred(txn: anytype, physical_key: []const u8, value: []const u8) !void {
    if (physical_key.len != prefix.len + 32 or !std.mem.startsWith(u8, physical_key, prefix)) return error.InvalidGenerationAdmission;
    const scope = try Scope.decode(value);
    const expected = try scopeKey(scope.child_table_name, scope.constraint_name);
    if (!std.mem.eql(u8, &expected, physical_key)) return error.InvalidGenerationAdmission;
    if (txn.get(physical_key)) |existing| {
        const prior = try Scope.decode(existing);
        if (!std.mem.eql(u8, prior.child_table_name, scope.child_table_name) or
            !std.mem.eql(u8, prior.constraint_name, scope.constraint_name) or
            !std.mem.eql(u8, existing, value)) return error.IntegrityHandoffCollision;
        return;
    } else |err| if (err != error.NotFound) return err;
    try txn.put(physical_key, value);
}

test "parent generation scope denies retired and unknown generations after activation" {
    const active: integrity.Generation = @splat(2);
    const old: integrity.Generation = @splat(1);
    const scope: Scope = .{ .child_table_id = 7, .child_table_name = "children", .constraint_name = "fk", .revision = 3, .phase = .active, .active_generation = active, .plan_id = @splat(4), .decision_digest = @splat(5) };
    const encoded = try scope.encode(std.testing.allocator);
    defer std.testing.allocator.free(encoded);
    const decoded = try Scope.decode(encoded);
    const reference: integrity.Reference = .{ .child_table = "children", .child_key = "c", .constraint_name = "fk", .constraint_generation = old };
    try std.testing.expect(!decoded.accepts(reference));
    var next = reference;
    next.constraint_generation = active;
    try std.testing.expect(decoded.accepts(next));
    const corrupted = try std.testing.allocator.dupe(u8, encoded);
    defer std.testing.allocator.free(corrupted);
    corrupted[24] ^= 1;
    try std.testing.expectError(error.InvalidGenerationAdmission, Scope.decode(corrupted));
    const first_key = try scopeKey("children", "fk");
    const distinct_key = try scopeKey("children", "other");
    try std.testing.expect(!std.mem.eql(u8, &first_key, &distinct_key));
}

test "parent generation publication stages then activates exact successor and remains default-deny after drop" {
    const Mock = struct {
        value: ?[]u8 = null,
        fn get(self: *@This(), _: []const u8) error{NotFound}![]const u8 {
            return self.value orelse error.NotFound;
        }
        fn put(self: *@This(), _: []const u8, value: []const u8) !void {
            if (self.value) |prior| std.testing.allocator.free(prior);
            self.value = try std.testing.allocator.dupe(u8, value);
        }
    };
    var txn: Mock = .{};
    defer if (txn.value) |value| std.testing.allocator.free(value);
    const alloc = std.testing.allocator;
    const create: Transition = .{ .child_table_id = 7, .child_table_name = "children", .constraint_name = "fk", .expected_generation = null, .next_generation = @splat(2), .plan_id = @splat(3), .decision_digest = @splat(4) };
    try stageTransition(alloc, &txn, create);
    try stageTransition(alloc, &txn, create);
    var scope = (try load(&txn, create.child_table_name, create.constraint_name)).?;
    try std.testing.expectEqual(Phase.staged, scope.phase);
    try std.testing.expect(scope.active_generation == null);
    try activateTransition(alloc, &txn, create);
    try activateTransition(alloc, &txn, create);
    scope = (try load(&txn, create.child_table_name, create.constraint_name)).?;
    try std.testing.expectEqual(Phase.active, scope.phase);
    try std.testing.expectEqual(@as(integrity.Generation, @splat(2)), scope.active_generation.?);
    var dropped = create;
    dropped.expected_generation = @splat(2);
    dropped.next_generation = null;
    dropped.plan_id = @splat(5);
    dropped.decision_digest = @splat(6);
    try stageTransition(alloc, &txn, dropped);
    try activateTransition(alloc, &txn, dropped);
    scope = (try load(&txn, create.child_table_name, create.constraint_name)).?;
    try std.testing.expect(scope.active_generation == null);
    try std.testing.expect(!scope.accepts(.{ .child_table = "children", .child_key = "c", .constraint_name = "fk", .constraint_generation = @splat(2) }));
}
