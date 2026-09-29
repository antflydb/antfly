// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Receiver-owned, ordered adoption of one imported source proof. The command
//! carries only immutable evidence identity; followers recapture their local
//! read sets and never accept donor positions or copied receipt bytes.
const std = @import("std");
const Digest = @import("artifact_publication.zig").Digest;
const Attempt = @import("relational_integrity_handoff_contract.zig").MergeCopyAttempt;
const pages = @import("merge_page_contract.zig");
const merge = @import("merge_state.zig");
const Namespace = @import("doc_identity_namespace.zig").Namespace;
const ByteRange = @import("../byte_range.zig").ByteRange;

pub const Command = struct {
    transition_id: u64,
    attempt: Attempt,
    source_pin: Digest,
    proof_digest: Digest,
    record_digest: Digest,

    pub fn validate(self: Command) !void {
        if (self.transition_id == 0 or self.attempt.donor_term == 0 or self.attempt.sequence == 0 or
            std.mem.allEqual(u8, &self.source_pin, 0) or
            std.mem.allEqual(u8, &self.proof_digest, 0) or
            std.mem.allEqual(u8, &self.record_digest, 0)) return error.InvalidBatchRequest;
    }
};

/// The copied proof is not authority on its own. Both preflight and ordered
/// apply read this receiver-owned fence; a replaced attempt becomes a durable
/// no-op rather than adopting evidence from an obsolete source cut.
pub const Fence = struct {
    progress: std.json.Parsed(pages.Progress),
    state: merge.State,
    progress_raw: []u8,
    state_raw: []u8,

    pub fn deinit(self: *Fence, alloc: std.mem.Allocator) void {
        self.progress.deinit();
        self.state.deinit(alloc);
        alloc.free(self.progress_raw);
        alloc.free(self.state_raw);
        self.* = undefined;
    }

    /// Exact serialized compare is cheaper than rebuilding both large parsed
    /// controls under the global writer lock and catches even a same-attempt
    /// cursor, catalog or source-cut replacement.
    pub fn matchesStored(self: *const Fence, txn: anytype) !bool {
        const current_state = txn.get(merge.key) catch |err| {
            if (err == error.NotFound) return false;
            return err;
        };
        if (!std.mem.eql(u8, current_state, self.state_raw)) return false;
        const current_progress = txn.get(pages.key) catch |err| {
            if (err == error.NotFound) return false;
            return err;
        };
        return std.mem.eql(u8, current_progress, self.progress_raw);
    }

    pub fn donorRange(self: *const Fence) !ByteRange {
        const merged = self.state.merged_range orelse return error.InvalidMergeState;
        const base = self.state.receiver_base_range;
        if (base.start.len != 0 and base.end.len != 0 and std.mem.order(u8, base.start, base.end) != .lt)
            return error.InvalidMergeState;
        const extends_left = !std.mem.eql(u8, merged.start, base.start);
        const extends_right = !std.mem.eql(u8, merged.end, base.end);
        if (extends_left == extends_right) return error.InvalidMergeState;
        if (extends_left) {
            if (base.start.len == 0 or
                (merged.start.len != 0 and std.mem.order(u8, merged.start, base.start) != .lt))
                return error.InvalidMergeState;
            return .{ .start = merged.start, .end = base.start };
        }
        if (base.end.len == 0 or
            (merged.end.len != 0 and std.mem.order(u8, base.end, merged.end) != .lt))
            return error.InvalidMergeState;
        return .{ .start = base.end, .end = merged.end };
    }
};

pub fn loadFence(alloc: std.mem.Allocator, txn: anytype, receiver: Namespace, command: Command) !?Fence {
    const state_raw = txn.get(merge.key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
    var state = try merge.decodeAlloc(alloc, state_raw);
    var state_owned = true;
    defer if (state_owned) state.deinit(alloc);
    const progress_raw = txn.get(pages.key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
    var progress = try pages.decode(alloc, progress_raw);
    var progress_owned = true;
    defer if (progress_owned) progress.deinit();
    const source = progress.value.source;
    if (!progress.value.receiver_namespace.eql(receiver) or !source.provenance_required or
        !std.mem.eql(u8, &source.pin_digest, &command.source_pin) or
        progress.value.transition_id != command.transition_id or
        progress.value.attempt.order(command.attempt) != .eq or
        !merge.copyAllowed(state, .{
            .transition_id = progress.value.transition_id,
            .donor_group_id = progress.value.donor_group_id,
            .receiver_group_id = progress.value.receiver_group_id,
            .identity_namespace = receiver,
            .copy_attempt = command.attempt,
        })) return null;
    const state_copy = try alloc.dupe(u8, state_raw);
    errdefer alloc.free(state_copy);
    const progress_copy = try alloc.dupe(u8, progress_raw);
    errdefer alloc.free(progress_copy);
    const fence: Fence = .{ .progress = progress, .state = state, .progress_raw = progress_copy, .state_raw = state_copy };
    _ = try fence.donorRange();
    state_owned = false;
    progress_owned = false;
    return fence;
}

/// A proof-adoption log entry is its own bounded control. Mixing it with row,
/// page, publication, or catalog mutations would make stale-evidence rejection
/// ambiguous and could give a response credit to unrelated effects.
pub fn validateRequest(req: anytype) !void {
    const command = req.merge_proof_adoption orelse return;
    try command.validate();
    const empty: @TypeOf(req) = .{};
    inline for (std.meta.fields(@TypeOf(req))) |field| {
        if (comptime !std.mem.eql(u8, field.name, "merge_proof_adoption") and !std.mem.eql(u8, field.name, "sync_level")) {
            if (comptime @typeInfo(field.type) == .pointer and @typeInfo(field.type).pointer.size == .slice) {
                if (@field(req, field.name).len != 0) return error.InvalidBatchRequest;
            } else if (!std.meta.eql(@field(req, field.name), @field(empty, field.name))) return error.InvalidBatchRequest;
        }
    }
}

test "ordered artifact inventory merge proof adoption has bounded isolated identity" {
    const Request = struct {
        merge_proof_adoption: ?Command = null,
        sync_level: enum { write, full_index } = .write,
        writes: []const []const u8 = &.{},
        merge_page: ?u64 = null,
        artifact_publication: ?u64 = null,
    };
    const command: Command = .{ .transition_id = 1, .attempt = .{ .donor_term = 2, .sequence = 3 }, .source_pin = @splat(4), .proof_digest = @splat(5), .record_digest = @splat(6) };
    try validateRequest(Request{ .merge_proof_adoption = command });
    try std.testing.expectError(error.InvalidBatchRequest, validateRequest(Request{ .merge_proof_adoption = command, .writes = &.{"doc"} }));
    try std.testing.expectError(error.InvalidBatchRequest, validateRequest(Request{ .merge_proof_adoption = command, .merge_page = 1 }));
    var invalid = command;
    invalid.record_digest = @splat(0);
    try std.testing.expectError(error.InvalidBatchRequest, invalid.validate());
}

test "ordered artifact inventory adoption fence compares both exact receiver controls" {
    const Txn = struct {
        state: []const u8 = "state",
        progress: []const u8 = "page",
        fn get(self: *@This(), key: []const u8) ![]const u8 {
            if (std.mem.eql(u8, key, merge.key)) return self.state;
            if (std.mem.eql(u8, key, pages.key)) return self.progress;
            return error.NotFound;
        }
    };
    var fence: Fence = .{ .state = .{ .donor_group_id = 2, .receiver_group_id = 3, .phase = .accepting, .receiver_base_range = .{ .start = "m", .end = "z" }, .merged_range = .{ .start = "a", .end = "z" } }, .progress = undefined, .state_raw = @constCast("state"), .progress_raw = @constCast("page") };
    var txn: Txn = .{};
    try std.testing.expect(try fence.matchesStored(&txn));
    try std.testing.expectEqualStrings("m", (try fence.donorRange()).end);
    fence.state.merged_range = .{ .start = "a", .end = "zz" };
    try std.testing.expectError(error.InvalidMergeState, fence.donorRange());
    fence.state.merged_range = .{ .start = "m", .end = "z" };
    try std.testing.expectError(error.InvalidMergeState, fence.donorRange());
    fence.state.merged_range = .{ .start = "n", .end = "z" };
    try std.testing.expectError(error.InvalidMergeState, fence.donorRange());
    fence.state.merged_range = .{ .start = "m", .end = "y" };
    try std.testing.expectError(error.InvalidMergeState, fence.donorRange());
    fence.state.merged_range = .{ .start = "m", .end = "" };
    try std.testing.expectEqualStrings("z", (try fence.donorRange()).start);
    fence.state.receiver_base_range = .{ .start = "", .end = "z" };
    fence.state.merged_range = .{ .start = "a", .end = "z" };
    try std.testing.expectError(error.InvalidMergeState, fence.donorRange());
    txn.progress = "later-page";
    try std.testing.expect(!try fence.matchesStored(&txn));
    txn.progress = "page";
    txn.state = "later-state";
    try std.testing.expect(!try fence.matchesStored(&txn));
}
