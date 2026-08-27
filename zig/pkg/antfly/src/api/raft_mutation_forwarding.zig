// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at
//
// https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations.

const std = @import("std");

pub const max_forwards: u8 = 2;
pub const max_remaining_ms: u32 = 5_000;

/// Group-independent routing state carried between authenticated Raft
/// mutation endpoints. Budgets are relative because monotonic clocks are
/// process-local. A sender must subtract its elapsed work and response reserve
/// before constructing the next context.
pub const Context = struct {
    remaining_ms: u32,
    forwards_remaining: u8,
    campaign_allowed: bool,

    pub fn child(
        self: Context,
        elapsed_ms: u32,
        response_reserve_ms: u32,
    ) !Context {
        if (self.forwards_remaining == 0) return error.RaftMutationForwardLimitReached;
        const consumed = @min(self.remaining_ms, elapsed_ms +| response_reserve_ms);
        const remaining_ms = self.remaining_ms - consumed;
        if (remaining_ms == 0) return error.RaftMutationDeadlineExceeded;
        return .{
            .remaining_ms = remaining_ms,
            .forwards_remaining = self.forwards_remaining - 1,
            // Campaign ownership belongs to the first admitted node. A
            // forwarded request may discover or contact a leader, not start a
            // second election storm.
            .campaign_allowed = false,
        };
    }
};

pub const Limits = struct {
    max_remaining_ms: u32,
    max_forwards: u8,
};

/// Builds the next-hop relative budget from a process-local absolute deadline.
/// This is shared by data and metadata Raft groups so response reserve and hop
/// ownership cannot drift into subtly different retry contracts.
pub fn contextForDeadline(
    now_ns: u64,
    deadline_ns: u64,
    response_reserve_ns: u64,
    forwards_remaining: u8,
    campaign_allowed: bool,
) ?Context {
    if (forwards_remaining == 0 or now_ns >= deadline_ns) return null;
    const remaining_ns = deadline_ns - now_ns;
    if (remaining_ns <= response_reserve_ns + std.time.ns_per_ms) return null;
    const target_budget_ms = (remaining_ns - response_reserve_ns) / std.time.ns_per_ms;
    if (target_budget_ms == 0) return null;
    return .{
        .remaining_ms = @intCast(@min(target_budget_ms, max_remaining_ms)),
        .forwards_remaining = forwards_remaining - 1,
        .campaign_allowed = campaign_allowed,
    };
}

/// Selects a configured, routable remote leader without coupling the routing
/// policy to a particular Raft group or peer record type. Peer records are
/// expected to expose `node_id` and optional `orchestration_url` fields.
pub fn selectRemoteLeaderPeerIndex(
    local_node_id: u64,
    leader_id: ?u64,
    peers: anytype,
) !usize {
    const target = leader_id orelse return error.RaftMutationLeaderUnknown;
    if (target == local_node_id) return error.RaftMutationLeaderUnknown;
    for (peers, 0..) |peer, index| {
        if (peer.node_id != target) continue;
        const url = peer.orchestration_url orelse return error.RaftMutationLeaderUnroutable;
        if (url.len == 0) return error.RaftMutationLeaderUnroutable;
        return index;
    }
    return error.RaftMutationLeaderUnroutable;
}

pub fn parseValues(
    remaining_raw: ?[]const u8,
    forwards_raw: ?[]const u8,
    campaign_raw: ?[]const u8,
    limits: Limits,
) !?Context {
    if (remaining_raw == null and forwards_raw == null and campaign_raw == null) return null;
    if (remaining_raw == null or forwards_raw == null or campaign_raw == null)
        return error.InvalidRaftMutationForwardingHeaders;

    const remaining_ms = std.fmt.parseUnsigned(u32, remaining_raw.?, 10) catch
        return error.InvalidRaftMutationForwardingHeaders;
    const forwards_remaining = std.fmt.parseUnsigned(u8, forwards_raw.?, 10) catch
        return error.InvalidRaftMutationForwardingHeaders;
    const campaign_allowed = if (std.mem.eql(u8, campaign_raw.?, "true"))
        true
    else if (std.mem.eql(u8, campaign_raw.?, "false"))
        false
    else
        return error.InvalidRaftMutationForwardingHeaders;
    if (remaining_ms == 0 or
        remaining_ms > limits.max_remaining_ms or
        forwards_remaining > limits.max_forwards)
    {
        return error.InvalidRaftMutationForwardingHeaders;
    }
    return .{
        .remaining_ms = remaining_ms,
        .forwards_remaining = forwards_remaining,
        .campaign_allowed = campaign_allowed,
    };
}

/// Transport-level outcome of an authenticated Raft mutation request. Only
/// `not_proposed` permits blind rediscovery/retry. `unknown` must converge by
/// observing the mutation's exact durable projection.
pub const Outcome = enum {
    not_proposed,
    unknown,
    committed,
    committed_visibility_pending,
    committed_repair_required,
};

pub fn parseOutcome(
    raw: ?[]const u8,
    not_proposed_value: []const u8,
    unknown_value: []const u8,
    committed_value: []const u8,
    committed_visibility_pending_value: []const u8,
    committed_repair_required_value: []const u8,
) ?Outcome {
    const value = raw orelse return null;
    if (std.mem.eql(u8, value, not_proposed_value)) return .not_proposed;
    if (std.mem.eql(u8, value, unknown_value)) return .unknown;
    if (std.mem.eql(u8, value, committed_value)) return .committed;
    if (std.mem.eql(u8, value, committed_visibility_pending_value))
        return .committed_visibility_pending;
    if (std.mem.eql(u8, value, committed_repair_required_value))
        return .committed_repair_required;
    return null;
}

test "raft mutation child context bounds hops, budget, and campaign ownership" {
    const child = try (Context{
        .remaining_ms = 500,
        .forwards_remaining = 2,
        .campaign_allowed = true,
    }).child(75, 25);
    try std.testing.expectEqual(@as(u32, 400), child.remaining_ms);
    try std.testing.expectEqual(@as(u8, 1), child.forwards_remaining);
    try std.testing.expect(!child.campaign_allowed);

    try std.testing.expectError(
        error.RaftMutationForwardLimitReached,
        (Context{ .remaining_ms = 500, .forwards_remaining = 0, .campaign_allowed = false }).child(0, 1),
    );
    try std.testing.expectError(
        error.RaftMutationDeadlineExceeded,
        (Context{ .remaining_ms = 25, .forwards_remaining = 1, .campaign_allowed = false }).child(10, 15),
    );
}

test "raft mutation forwarding headers are all-or-none" {
    const limits = Limits{ .max_remaining_ms = 5_000, .max_forwards = 2 };
    const parsed = (try parseValues("425", "1", "false", limits)).?;
    try std.testing.expectEqual(@as(u32, 425), parsed.remaining_ms);
    try std.testing.expectEqual(@as(u8, 1), parsed.forwards_remaining);
    try std.testing.expect(!parsed.campaign_allowed);
    try std.testing.expect((try parseValues(null, null, null, limits)) == null);
    try std.testing.expectError(
        error.InvalidRaftMutationForwardingHeaders,
        parseValues("425", "1", null, limits),
    );
}

test "raft mutation deadline context reserves response time and one hop" {
    const context = contextForDeadline(
        1_000 * std.time.ns_per_ms,
        1_500 * std.time.ns_per_ms,
        75 * std.time.ns_per_ms,
        2,
        false,
    ).?;
    try std.testing.expectEqual(@as(u32, 425), context.remaining_ms);
    try std.testing.expectEqual(@as(u8, 1), context.forwards_remaining);
    try std.testing.expect(!context.campaign_allowed);
    try std.testing.expect(contextForDeadline(1_500, 1_500, 0, 2, true) == null);
    try std.testing.expect(contextForDeadline(0, 10 * std.time.ns_per_ms, 9 * std.time.ns_per_ms, 2, true) == null);
}
