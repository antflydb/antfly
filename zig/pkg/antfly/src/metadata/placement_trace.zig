// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const build_options = @import("build_options");
const tracing = @import("../tracing/mod.zig");

pub fn observeReport(event_name: []const u8, store_id: u64, report: anytype) void {
    if (comptime !build_options.with_tla) return;
    var fingerprint_buf: [64]u8 = undefined;
    const fingerprint = if (report.voter_set_known)
        std.fmt.bufPrint(&fingerprint_buf, "{x}", .{report.voter_set_fingerprint}) catch ""
    else
        null;
    tracing.stderrProtocolTraceWriter().traceEvent(&.{
        .family = "placement-readiness",
        .trace_id = report.group_id,
        .name = event_name,
        .facts = .{
            .group_id = report.group_id,
            .store_id = store_id,
            .raft_term = report.raft_term,
            .membership_index = report.raft_membership_index,
            .voter_count = report.voter_count,
            .local_leader = report.local_leader,
            .voter_set_known = report.voter_set_known,
            .fingerprint = fingerprint,
            .joint_consensus = report.joint_consensus,
        },
    });
}

pub fn recomputeEvidence(event_name: []const u8, status: anytype, ambiguous: bool) void {
    if (comptime !build_options.with_tla) return;
    var fingerprint_buf: [64]u8 = undefined;
    const fingerprint = if (status.voter_set_known)
        std.fmt.bufPrint(&fingerprint_buf, "{x}", .{status.voter_set_fingerprint}) catch ""
    else
        null;
    tracing.stderrProtocolTraceWriter().traceEvent(&.{
        .family = "placement-readiness",
        .trace_id = status.group_id,
        .name = event_name,
        .facts = .{
            .group_id = status.group_id,
            .store_id = if (status.leader_known) status.leader_store_id else null,
            .voter_count = status.voter_count,
            .healthy_voter_reports = status.healthy_voter_reports,
            .leader_known = status.leader_known,
            .voter_count_known = status.voter_count_known,
            .voter_set_known = status.voter_set_known,
            .fingerprint = fingerprint,
            .ambiguous = ambiguous,
            .joint_consensus = status.joint_consensus,
        },
    });
}

pub fn transitionAdmission(
    status: anytype,
    expected_voters: u64,
    leader_placed: bool,
    stable: bool,
    reason: ?[]const u8,
) void {
    if (comptime !build_options.with_tla) return;
    var fingerprint_buf: [64]u8 = undefined;
    const fingerprint = if (status.voter_set_known)
        std.fmt.bufPrint(&fingerprint_buf, "{x}", .{status.voter_set_fingerprint}) catch ""
    else
        null;
    tracing.stderrProtocolTraceWriter().traceEvent(&.{
        .family = "placement-readiness",
        .trace_id = status.group_id,
        .name = if (stable) "StartTransition" else "RejectTransition",
        .facts = .{
            .group_id = status.group_id,
            .store_id = if (status.leader_known) status.leader_store_id else null,
            .voter_count = status.voter_count,
            .expected_voters = expected_voters,
            .healthy_voter_reports = status.healthy_voter_reports,
            .leader_known = status.leader_known,
            .leader_placed = leader_placed,
            .voter_count_known = status.voter_count_known,
            .voter_set_known = status.voter_set_known,
            .fingerprint = fingerprint,
            .ambiguous = status.voter_count_known and !status.voter_set_known,
            .joint_consensus = status.joint_consensus,
            .stable_placement = stable,
            .reason = reason,
        },
    });
}
