// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Derive future control obligations from the actual fresh BEGIN mutation.
//! This validates physical shape and sizing, not a consensus promise. Native
//! admission must still prove the baseline, installation and physical backing.
const std = @import("std");
const shape = @import("../completion_control_budget.zig");
const record = @import("completion_control_record.zig");
const slot = @import("completion_slot.zig");

pub const Declaration = struct {
    txn_id: [16]u8,
    participants: record.Participants,
    budget: shape.Budget,
    coordinator: bool,
    retain_terminal: bool,
    ledger_bytes: u64,
};

/// Fresh, completion-accounted BEGIN has exactly one record, participant
/// sidecar (or tombstone), resolved-set tombstone, credit and shared summary.
/// Idempotent BEGIN and legacy unaccounted metadata are separate lifecycle
/// paths; they must not be mistaken for a new resource-owning transaction.
pub fn inspect(operations: []const slot.Operation) !Declaration {
    if (operations.len != 5) return error.UnsupportedCompletionProfile;
    var txn_id: ?[16]u8 = null;
    var coordinator = false;
    var retain_terminal = false;
    for (operations) |op| {
        if (op.bindings.len != 0) return error.InvalidCompletionSlot;
        if (!std.mem.startsWith(u8, op.key, shape.records_prefix)) continue;
        if (txn_id != null or op.key.len != shape.records_prefix.len + 16 or op.kind != .put or
            op.value.len != shape.txn_record_v6_size) return error.InvalidCompletionSlot;
        // V6 pending record: no decision, finalization, intents, replay, prepare
        // or local-resolution effects have happened yet. Both role bits are
        // canonical booleans, including follower and retained-history begins.
        if (op.value[0] != 0 or !std.mem.allEqual(u8, op.value[9..17], 0) or
            !std.mem.allEqual(u8, op.value[25..50], 0) or op.value[50] > 1 or
            op.value[51] > 1 or op.value[52] != 0) return error.InvalidCompletionSlot;
        txn_id = op.key[shape.records_prefix.len..][0..16].*;
        coordinator = op.value[50] != 0;
        retain_terminal = op.value[51] != 0;
    }
    const id = txn_id orelse return error.InvalidCompletionSlot;
    var participants: ?record.Participants = null;
    var budget: ?shape.Budget = null;
    var resolved_seen = false;
    var ledger_bytes: ?u64 = null;
    var summary_bytes: ?u64 = null;
    for (operations) |op| {
        if (matches(op.key, shape.records_prefix, id)) continue;
        if (matches(op.key, shape.participants_prefix, id)) {
            if (participants != null) return error.InvalidCompletionSlot;
            if (op.kind == .delete) {
                if (op.value.len != 0) return error.InvalidCompletionSlot;
                participants = try record.Participants.measure(&.{});
                budget = try shape.Budget.measure(&.{});
            } else {
                const sized = try shape.Budget.measureEncodedList(op.value);
                const decoded = try record.Participants.fromEncodedList(op.value);
                if (decoded.count == 0) return error.InvalidCompletionSlot;
                participants = decoded;
                budget = sized;
            }
        } else if (matches(op.key, shape.resolved_participants_prefix, id)) {
            if (resolved_seen or op.kind != .delete or op.value.len != 0) return error.InvalidCompletionSlot;
            resolved_seen = true;
        } else if (matches(op.key, shape.completion_prefix, id)) {
            if (ledger_bytes != null or op.kind != .put or op.value.len != 16 or
                !std.mem.allEqual(u8, op.value[8..16], 0)) return error.InvalidCompletionSlot;
            ledger_bytes = std.mem.readInt(u64, op.value[0..8], .little);
        } else if (std.mem.eql(u8, op.key, shape.completion_summary_key)) {
            if (summary_bytes != null or op.kind != .put or op.value.len != 16 or
                std.mem.readInt(u64, op.value[0..8], .little) == 0) return error.InvalidCompletionSlot;
            summary_bytes = std.mem.readInt(u64, op.value[8..16], .little);
        } else return error.InvalidCompletionSlot;
    }
    if (participants == null or budget == null or !resolved_seen or ledger_bytes == null or summary_bytes == null)
        return error.InvalidCompletionSlot;
    if (ledger_bytes.? < budget.?.wal_bytes or summary_bytes.? < ledger_bytes.?)
        return error.CompletionPlanCapacityExceeded;
    return .{
        .txn_id = id,
        .participants = participants.?,
        .budget = budget.?,
        .coordinator = coordinator,
        .retain_terminal = retain_terminal,
        .ledger_bytes = ledger_bytes.?,
    };
}

fn matches(key: []const u8, comptime prefix: []const u8, txn_id: [16]u8) bool {
    return key.len == prefix.len + txn_id.len and std.mem.startsWith(u8, key, prefix) and
        std.mem.eql(u8, key[prefix.len..], &txn_id);
}
