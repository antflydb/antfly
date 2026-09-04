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
const platform_time = @import("antfly_platform").time;

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

pub const AbsoluteDriver = struct {
    deadline_ns: u64,
    forwards_remaining: u8 = max_forwards,
    mutation_attempted: bool = false,

    pub fn init(deadline_ns: u64) AbsoluteDriver {
        return .{ .deadline_ns = deadline_ns };
    }

    pub fn nextContext(self: AbsoluteDriver, now_ns: u64, response_reserve_ns: u64) ?Context {
        return contextForDeadline(
            now_ns,
            self.deadline_ns,
            response_reserve_ns,
            self.forwards_remaining,
            !self.mutation_attempted,
        );
    }

    /// Consume a hop and the unique campaign privilege only after delivery may
    /// have crossed the process boundary. Proven `not_sent` failures do not
    /// call this method and retain both.
    pub fn recordDelivered(self: *AbsoluteDriver, child: Context) void {
        self.forwards_remaining = child.forwards_remaining;
        self.mutation_attempted = true;
    }
};

pub const RoutedMutationOptions = struct {
    max_attempts: usize = 3,
    initial_remaining_ms: u32 = max_remaining_ms,
    initial_forwards: u8 = max_forwards,
    response_reserve_ms: u32 = 50,
};

/// Shared local-or-forwarded mutation state machine. `router` resolves either
/// `.local` or `.forward`; `ops` supplies `local()` and `forward(peer, Context)`.
/// The driver is intentionally strict: only NotLeader and a delivery-proven
/// RaftMutationRequestNotSent are replayable.
pub fn runRoutedMutation(
    router: anytype,
    ops: anytype,
    options: RoutedMutationOptions,
) !void {
    return runRoutedMutationWithClock(router, ops, options, SystemClock{});
}

const SystemClock = struct {
    fn nowNs(_: SystemClock) u64 {
        return platform_time.monotonicNs();
    }
};

fn runRoutedMutationWithClock(
    router: anytype,
    ops: anytype,
    options: RoutedMutationOptions,
    clock: anytype,
) !void {
    const started_ns = clock.nowNs();
    const deadline_ns = started_ns +|
        @as(u64, options.initial_remaining_ms) * std.time.ns_per_ms;
    var attempts: usize = 0;
    var forwarding = Context{
        .remaining_ms = options.initial_remaining_ms,
        .forwards_remaining = options.initial_forwards,
        .campaign_allowed = true,
    };
    var campaign_allowed = true;
    var attempt_started_ns = started_ns;
    while (true) {
        const now_ns = clock.nowNs();
        const absolute_remaining_ms = remainingMsUntil(deadline_ns, now_ns) orelse
            return error.RaftMutationDeadlineExceeded;
        // A child context may already have reserved response time. Preserve
        // that tighter bound while also debiting all time spent in a delivered
        // remote request before route discovery or a local retry.
        forwarding.remaining_ms = @min(forwarding.remaining_ms, absolute_remaining_ms);
        attempt_started_ns = now_ns;
        attempts += 1;
        const route = if (comptime @hasDecl(@TypeOf(router.*), "resolveTableMutationRouteWithCampaign"))
            try router.resolveTableMutationRouteWithCampaign(&campaign_allowed, forwarding.remaining_ms)
        else
            try router.resolveTableMutationRoute();
        switch (route) {
            .local => {
                ops.local() catch |err| switch (err) {
                    error.NotLeader => {
                        if (attempts >= options.max_attempts) return error.NotLeader;
                        const elapsed_ms = elapsedMsSince(attempt_started_ns, clock.nowNs());
                        if (elapsed_ms >= forwarding.remaining_ms)
                            return error.RaftMutationDeadlineExceeded;
                        forwarding.remaining_ms -= elapsed_ms;
                        attempt_started_ns = clock.nowNs();
                        continue;
                    },
                    else => return err,
                };
                return;
            },
            .forward => |peer| {
                const parent_attempt_started_ns = attempt_started_ns;
                const child_forwarding = forwarding.child(
                    elapsedMsSince(attempt_started_ns, clock.nowNs()),
                    options.response_reserve_ms,
                ) catch |err| return switch (err) {
                    error.RaftMutationDeadlineExceeded => error.RaftMutationDeadlineExceeded,
                    error.RaftMutationForwardLimitReached => error.NotLeader,
                };
                attempt_started_ns = clock.nowNs();
                ops.forward(peer, child_forwarding) catch |err| switch (err) {
                    error.RaftMutationRequestNotSent => {
                        if (attempts >= options.max_attempts) return error.NotLeader;
                        const total_elapsed_ms = elapsedMsSince(parent_attempt_started_ns, clock.nowNs());
                        if (total_elapsed_ms >= forwarding.remaining_ms)
                            return error.RaftMutationDeadlineExceeded;
                        forwarding.remaining_ms -= total_elapsed_ms;
                        attempt_started_ns = clock.nowNs();
                        continue;
                    },
                    error.NotLeader => {
                        forwarding = child_forwarding;
                        if (attempts >= options.max_attempts) return error.NotLeader;
                        continue;
                    },
                    else => return err,
                };
                return;
            },
        }
    }
}

fn remainingMsUntil(deadline_ns: u64, now_ns: u64) ?u32 {
    if (now_ns >= deadline_ns) return null;
    const remaining_ns = deadline_ns - now_ns;
    const remaining_ms = remaining_ns / std.time.ns_per_ms;
    if (remaining_ms == 0) return null;
    return @intCast(@min(remaining_ms, std.math.maxInt(u32)));
}

fn elapsedMsSince(started_ns: u64, now_ns: u64) u32 {
    const elapsed_ns = now_ns -| started_ns;
    return @intCast(@min(elapsed_ns / std.time.ns_per_ms, std.math.maxInt(u32)));
}

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

test "absolute mutation driver consumes only delivered hops" {
    var driver = AbsoluteDriver.init(2_000 * std.time.ns_per_ms);
    const first = driver.nextContext(1_000 * std.time.ns_per_ms, 50 * std.time.ns_per_ms).?;
    try std.testing.expectEqual(@as(u8, 1), first.forwards_remaining);
    try std.testing.expect(!driver.mutation_attempted);
    // A not-sent result deliberately leaves the driver unchanged.
    const retry = driver.nextContext(1_100 * std.time.ns_per_ms, 50 * std.time.ns_per_ms).?;
    try std.testing.expectEqual(@as(u8, 1), retry.forwards_remaining);
    driver.recordDelivered(retry);
    try std.testing.expect(driver.mutation_attempted);
    const final = driver.nextContext(1_200 * std.time.ns_per_ms, 50 * std.time.ns_per_ms).?;
    try std.testing.expectEqual(@as(u8, 0), final.forwards_remaining);
    try std.testing.expect(!final.campaign_allowed);
}

test "raft mutation routed driver rejects an exhausted absolute budget before routing" {
    const Script = struct {
        route_calls: usize = 0,

        pub fn resolveTableMutationRoute(self: *@This()) !union(enum) { local, forward: void } {
            self.route_calls += 1;
            return .local;
        }

        const Ops = struct {
            pub fn local(_: @This()) !void {}
            pub fn forward(_: @This(), _: void, _: Context) !void {}
        };
    };
    var script = Script{};
    try std.testing.expectError(
        error.RaftMutationDeadlineExceeded,
        runRoutedMutation(&script, Script.Ops{}, .{ .initial_remaining_ms = 0 }),
    );
    try std.testing.expectEqual(@as(usize, 0), script.route_calls);
}

test "raft mutation routed driver debits delivered remote time before rediscovery" {
    const Test = struct {
        const Clock = struct {
            now_ns: u64 = std.time.ns_per_ms,

            fn nowNs(self: *@This()) u64 {
                return self.now_ns;
            }
        };

        const Router = struct {
            route_calls: usize = 0,

            pub fn resolveTableMutationRoute(self: *@This()) !union(enum) { local, forward: void } {
                self.route_calls += 1;
                return if (self.route_calls == 1) .{ .forward = {} } else .local;
            }
        };

        const Ops = struct {
            clock: *Clock,
            local_calls: *usize,

            pub fn local(self: @This()) anyerror!void {
                self.local_calls.* += 1;
            }

            pub fn forward(self: @This(), _: void, _: Context) anyerror!void {
                self.clock.now_ns += 100 * std.time.ns_per_ms;
                return error.NotLeader;
            }
        };
    };
    var clock = Test.Clock{};
    var router = Test.Router{};
    var local_calls: usize = 0;
    try std.testing.expectError(
        error.RaftMutationDeadlineExceeded,
        runRoutedMutationWithClock(
            &router,
            Test.Ops{ .clock = &clock, .local_calls = &local_calls },
            .{ .initial_remaining_ms = 100 },
            &clock,
        ),
    );
    try std.testing.expectEqual(@as(usize, 1), router.route_calls);
    try std.testing.expectEqual(@as(usize, 0), local_calls);
}
