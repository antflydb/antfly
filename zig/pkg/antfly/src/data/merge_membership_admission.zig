// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Append-time membership barrier derived from the same replicated lifecycle
//! records used by apply. An unapplied acquisition fences immediately; an
//! unapplied release never unfences, because apply may reject a stale identity.
//! Consequently retries, log truncation, snapshots and restart need no separate
//! volatile barrier state and no second persistence protocol.
const std = @import("std");
const raft = @import("raft_engine").core;
const batch = @import("raft_batch.zig");
const source = @import("antfly_local_sources").storage_db_online_source_contract;
const types = @import("antfly_local_sources").storage_db_types;

pub const Receiver = struct {
    transition_id: u64,
    donor_group_id: u64,
    receiver_group_id: u64,
    copy_attempt: types.MergeCopyAttempt,
};

/// All fields must be read under the same apply-store lock. The raw projection
/// can precede native delegate apply; check also requires Raft's acknowledged
/// applied watermark before trusting its terminal release.
pub const Observation = struct {
    applied_index: u64 = 0,
    source_scope: ?source.Scope = null,
    receiver: ?Receiver = null,
};

/// Ordinary writes do not change membership or acquire a topology fence.
/// Avoid a storage read and retained-log walk on that hot path.
pub fn needsObservation(alloc: std.mem.Allocator, entries: []const raft.Entry) !bool {
    for (entries) |entry| {
        if (entry.entry_type != .normal or try acquires(alloc, entry.data)) return true;
    }
    return false;
}

pub fn check(alloc: std.mem.Allocator, observed: Observation, context: raft.ProposalAdmission.Context) !void {
    var fenced = observed.source_scope != null or observed.receiver != null;
    var pending_configuration = context.pending_conf_index > context.applied_index;
    // Missing compacted history or a raw projection ahead of delegate apply
    // cannot prove membership is safe, even if its current fence is absent.
    const uncertain = observed.applied_index < context.first_index -| 1 or
        observed.applied_index > context.applied_index;
    // Applied history can be large between snapshots. Locate the suffix by
    // index without decoding or linearly walking that already-projected prefix.
    var lower: usize = 0;
    var upper = context.retained_entries.len;
    while (lower < upper) {
        const middle = lower + (upper - lower) / 2;
        if (context.retained_entries[middle].index <= observed.applied_index)
            lower = middle + 1
        else
            upper = middle;
    }
    for (context.retained_entries[lower..]) |entry| {
        if (entry.entry_type != .normal) {
            pending_configuration = true;
        } else if (try acquires(alloc, entry.data)) {
            fenced = true;
        }
    }
    for (context.proposed_entries) |entry| {
        if (entry.entry_type != .normal) {
            if (fenced or uncertain) return error.MembershipChangeFenced;
            pending_configuration = true;
        } else if (try acquires(alloc, entry.data)) {
            if (uncertain) return error.MembershipChangeFenced;
            if (pending_configuration) return error.PendingConfChange;
            if (context.conf_state.voters_outgoing.len != 0) return error.MustLeaveJointFirst;
            fenced = true;
        }
    }
}

fn acquires(alloc: std.mem.Allocator, payload: []const u8) !bool {
    if (payload.len == 0 or !batch.looksLikeEnvelope(payload)) return false;
    // Canonical encoders use literal field names. Escaped JSON keys must fall
    // back to the parser; a Unicode escape is the only way to disguise one
    // of these ASCII identifier characters in a valid JSON object key.
    if (std.mem.indexOf(u8, payload, "\"_merge_checkpoint\"") == null and
        std.mem.indexOf(u8, payload, "\\u") == null) return false;
    var decoded = try batch.decode(alloc, payload);
    defer decoded.deinit(alloc);
    if (decoded.protocol_barrier_version != null) return false;
    if (decoded.batch.req.online_source) |command| {
        if (command == .admit and command.scope().fence.role == .merge_source) return true;
    }
    if (decoded.batch.req.merge_checkpoint) |checkpoint| {
        // Fence accept too: it precedes begin_copy, and membership must remain
        // stable while the donor's immutable source certificate is installed.
        if (checkpoint.kind == .accept or checkpoint.kind == .begin_copy) return true;
    }
    return false;
}

test "membership reducer rejects acquire plus configuration in either batch order" {
    const alloc = std.testing.allocator;
    const payload = try batch.encode(alloc, "docs", .{ .merge_checkpoint = .{
        .kind = .accept,
        .transition_id = 7,
        .donor_group_id = 1,
        .receiver_group_id = 2,
        .receiver_base_start = "",
        .receiver_base_end = "",
        .merged_start = "",
        .merged_end = "",
    } });
    defer alloc.free(payload);
    try std.testing.expect(try needsObservation(alloc, &.{.{ .index = 1, .data = payload }}));
    const escaped = try std.mem.replaceOwned(u8, alloc, payload, "\"_merge_checkpoint\"", "\"_merge_check\\u0070oint\"");
    defer alloc.free(escaped);
    try std.testing.expect(try needsObservation(alloc, &.{.{ .index = 1, .data = escaped }}));
    const ordinary = try batch.encode(alloc, "docs", .{});
    defer alloc.free(ordinary);
    try std.testing.expect(!try needsObservation(std.testing.failing_allocator, &.{.{ .index = 1, .data = ordinary }}));
    var entries = [_]raft.Entry{ .{ .data = payload }, .{ .entry_type = .conf_change } };
    var context: raft.ProposalAdmission.Context = .{
        .group_id = 2,
        .conf_state = .{},
        .applied_index = 3,
        .pending_conf_index = 0,
        .first_index = 4,
        .retained_entries = &.{},
        .proposed_entries = &entries,
    };
    try std.testing.expectError(error.MembershipChangeFenced, check(alloc, .{ .applied_index = 3 }, context));
    std.mem.swap(raft.Entry, &entries[0], &entries[1]);
    try std.testing.expectError(error.PendingConfChange, check(alloc, .{ .applied_index = 3 }, context));
    context.proposed_entries = entries[0..1];
    try std.testing.expectError(error.MembershipChangeFenced, check(alloc, .{ .applied_index = 2 }, context));
    try std.testing.expectError(error.MembershipChangeFenced, check(alloc, .{ .applied_index = 4 }, context));
    try check(alloc, .{ .applied_index = 3 }, context);
}

test "membership reducer never trusts an unapplied terminal release" {
    const alloc = std.testing.allocator;
    const acquire = try batch.encode(alloc, "docs", .{ .merge_checkpoint = .{
        .kind = .begin_copy,
        .transition_id = 7,
        .donor_group_id = 1,
        .receiver_group_id = 2,
        .receiver_base_start = "",
        .receiver_base_end = "",
        .merged_start = "",
        .merged_end = "",
        .copy_attempt = .{ .donor_term = 8, .sequence = 1 },
    } });
    defer alloc.free(acquire);
    const release = try batch.encode(alloc, "docs", .{ .merge_checkpoint = .{
        .kind = .rollback,
        .transition_id = 7,
        .donor_group_id = 1,
        .receiver_group_id = 2,
        .receiver_base_start = "",
        .receiver_base_end = "",
        .merged_start = "",
        .merged_end = "",
        .copy_attempt = .{ .donor_term = 7, .sequence = 1 },
    } });
    defer alloc.free(release);
    const retained = [_]raft.Entry{ .{ .index = 4, .data = acquire }, .{ .index = 5, .data = release } };
    const context: raft.ProposalAdmission.Context = .{
        .group_id = 2,
        .conf_state = .{},
        .applied_index = 3,
        .pending_conf_index = 0,
        .first_index = 4,
        .retained_entries = &retained,
        .proposed_entries = &.{.{ .entry_type = .conf_change_v2 }},
    };
    try std.testing.expectError(error.MembershipChangeFenced, check(alloc, .{ .applied_index = 3 }, context));
}

test "membership reducer gates donor admission on stable applied configuration" {
    const alloc = std.testing.allocator;
    const scope: source.Scope = .{
        .fence = .{ .transition_id = 91, .attempt = 1, .admission_epoch = 1, .owner_group_id = 1, .peer_group_id = 2, .role = .merge_source, .namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 700 }, .catalog_digest = @splat(8) },
        .receiver_namespace = .{ .table_id = 7, .shard_id = 2, .range_id = 701 },
        .consumer_epoch = 4,
        .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
    };
    const acquire = try batch.encode(alloc, "docs", .{ .online_source = .{ .admit = .{ .scope = scope } } });
    defer alloc.free(acquire);
    var context: raft.ProposalAdmission.Context = .{
        .group_id = 1,
        .conf_state = .{},
        .applied_index = 3,
        .pending_conf_index = 4,
        .first_index = 4,
        .retained_entries = &.{},
        .proposed_entries = &.{.{ .data = acquire }},
    };
    try std.testing.expectError(error.PendingConfChange, check(alloc, .{ .applied_index = 3 }, context));
    context.pending_conf_index = 3;
    context.conf_state.voters_outgoing = @constCast(&[_]u64{1});
    try std.testing.expectError(error.MustLeaveJointFirst, check(alloc, .{ .applied_index = 3 }, context));
    context.conf_state.voters_outgoing = &.{};
    try check(alloc, .{ .applied_index = 3 }, context);
    context.proposed_entries = &.{.{ .entry_type = .conf_change }};
    try std.testing.expectError(error.MembershipChangeFenced, check(alloc, .{ .applied_index = 3, .source_scope = scope }, context));
    // Applied exact terminal state, with no unapplied admission, releases it.
    try check(alloc, .{ .applied_index = 3 }, context);
}
