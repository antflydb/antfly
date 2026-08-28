// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Saved, validated event-set programs over one or more canonical histories.
//!
//! A program is a forward-only DAG: every step may reference only prior
//! steps. This makes saved JSON plans portable, rejects cycles without a
//! recursive parser, and gives evaluation deterministic order and bounds.

const std = @import("std");
const event_query = @import("event_query.zig");
const trace = @import("trace.zig");

pub const format = "vopr-event-set-v1";

pub const History = struct {
    id: []const u8,
    artifact: *const trace.Trace,
};

pub const Match = struct {
    history: usize,
    position: usize,
    index: u64,
    ordinal: u64,
    event_id: u64,

    fn lessThan(_: void, left: Match, right: Match) bool {
        if (left.history != right.history) return left.history < right.history;
        if (left.position != right.position) return left.position < right.position;
        return left.event_id < right.event_id;
    }

    fn equal(left: Match, right: Match) bool {
        return left.history == right.history and left.position == right.position;
    }
};

pub const Operation = enum {
    select,
    @"union",
    intersection,
    difference,
    complement,
    distinct_moment,
    first_per_moment,
    last_per_moment,
    previous_before,
    next_after,
    sequence,
};

pub const Step = struct {
    name: []const u8,
    operation: Operation,
    query: ?event_query.Query = null,
    inputs: []const usize = &.{},
    /// Timeline distance for previous/next/sequence. Null is unbounded.
    max_transition_distance: ?u64 = null,
};

pub const Plan = struct {
    format: []const u8,
    name: []const u8,
    steps: []const Step,
    result: usize,
    max_matches: usize = 1_000_000,

    pub fn validate(self: Plan) !void {
        if (!std.mem.eql(u8, self.format, format)) return error.UnsupportedEventSetFormat;
        if (self.name.len == 0) return error.EmptyEventSetName;
        if (self.steps.len == 0) return error.EmptyEventSetPlan;
        if (self.result >= self.steps.len) return error.InvalidEventSetResult;
        if (self.max_matches == 0) return error.InvalidEventSetMatchLimit;
        for (self.steps, 0..) |step, step_index| {
            if (step.name.len == 0) return error.EmptyEventSetStepName;
            for (self.steps[0..step_index]) |prior|
                if (std.mem.eql(u8, prior.name, step.name)) return error.DuplicateEventSetStepName;
            for (step.inputs) |input|
                if (input >= step_index) return error.EventSetInputMustReferencePriorStep;
            switch (step.operation) {
                .select => {
                    if (step.query == null or step.inputs.len != 0 or step.max_transition_distance != null) return error.InvalidEventSetSelect;
                    try step.query.?.validate();
                },
                .@"union", .intersection, .difference => if (step.query != null or step.inputs.len != 2 or step.max_transition_distance != null) return error.InvalidBinaryEventSetOperation,
                .complement, .distinct_moment, .first_per_moment, .last_per_moment => if (step.query != null or step.inputs.len != 1 or step.max_transition_distance != null) return error.InvalidUnaryEventSetOperation,
                .previous_before, .next_after, .sequence => if (step.query != null or step.inputs.len != 2) return error.InvalidTimelineEventSetOperation,
            }
        }
    }
};

pub const Result = struct {
    allocator: std.mem.Allocator,
    matches: []Match,

    pub fn deinit(self: *Result) void {
        self.allocator.free(self.matches);
        self.* = undefined;
    }
};

pub fn evaluateAlloc(allocator: std.mem.Allocator, histories: []const History, plan: Plan) !Result {
    try plan.validate();
    try validateHistories(histories);
    const values = try allocator.alloc([]Match, plan.steps.len);
    defer allocator.free(values);
    var initialized: usize = 0;
    errdefer for (values[0..initialized]) |value| allocator.free(value);

    for (plan.steps, 0..) |step, step_index| {
        values[step_index] = try evaluateStepAlloc(allocator, histories, step, values[0..step_index], plan.max_matches);
        initialized += 1;
    }
    const result_matches = values[plan.result];
    for (values, 0..) |value, index| if (index != plan.result) allocator.free(value);
    return .{ .allocator = allocator, .matches = result_matches };
}

pub fn count(allocator: std.mem.Allocator, histories: []const History, plan: Plan) !usize {
    var result = try evaluateAlloc(allocator, histories, plan);
    defer result.deinit();
    return result.matches.len;
}

pub fn parseAlloc(allocator: std.mem.Allocator, encoded: []const u8) !std.json.Parsed(Plan) {
    var parsed = try std.json.parseFromSlice(Plan, allocator, encoded, .{});
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}

pub fn renderAlloc(allocator: std.mem.Allocator, plan: Plan) ![]u8 {
    try plan.validate();
    return std.json.Stringify.valueAlloc(allocator, plan, .{ .whitespace = .indent_2 });
}

fn validateHistories(histories: []const History) !void {
    if (histories.len == 0) return error.EventSetHasNoHistories;
    for (histories, 0..) |history, index| {
        if (history.id.len == 0) return error.EmptyEventSetHistoryId;
        try history.artifact.validate();
        for (histories[0..index]) |prior|
            if (std.mem.eql(u8, prior.id, history.id)) return error.DuplicateEventSetHistoryId;
    }
}

fn evaluateStepAlloc(
    allocator: std.mem.Allocator,
    histories: []const History,
    step: Step,
    prior: []const []Match,
    limit: usize,
) ![]Match {
    const result = switch (step.operation) {
        .select => try selectAlloc(allocator, histories, step.query.?, limit),
        .@"union" => try setBinaryAlloc(allocator, prior[step.inputs[0]], prior[step.inputs[1]], .set_union, limit),
        .intersection => try setBinaryAlloc(allocator, prior[step.inputs[0]], prior[step.inputs[1]], .intersection, limit),
        .difference => try setBinaryAlloc(allocator, prior[step.inputs[0]], prior[step.inputs[1]], .difference, limit),
        .complement => blk: {
            const universe = try selectAlloc(allocator, histories, .{}, limit);
            defer allocator.free(universe);
            break :blk try setBinaryAlloc(allocator, universe, prior[step.inputs[0]], .difference, limit);
        },
        .distinct_moment, .first_per_moment => try momentsAlloc(allocator, prior[step.inputs[0]], false, limit),
        .last_per_moment => try momentsAlloc(allocator, prior[step.inputs[0]], true, limit),
        .previous_before => try timelineAlloc(allocator, prior[step.inputs[0]], prior[step.inputs[1]], step.max_transition_distance, .previous, limit),
        .next_after => try timelineAlloc(allocator, prior[step.inputs[0]], prior[step.inputs[1]], step.max_transition_distance, .next, limit),
        .sequence => try timelineAlloc(allocator, prior[step.inputs[0]], prior[step.inputs[1]], step.max_transition_distance, .sequence, limit),
    };
    if (result.len > limit) return error.EventSetMatchLimitExceeded;
    return result;
}

fn selectAlloc(allocator: std.mem.Allocator, histories: []const History, query: event_query.Query, limit: usize) ![]Match {
    var out: std.ArrayListUnmanaged(Match) = .empty;
    errdefer out.deinit(allocator);
    for (histories, 0..) |history, history_index| {
        const matches = try event_query.searchAlloc(allocator, history.artifact, query);
        defer allocator.free(matches);
        for (matches) |value| {
            if (out.items.len == limit) return error.EventSetMatchLimitExceeded;
            try out.append(allocator, .{
                .history = history_index,
                .position = value.position,
                .index = value.index,
                .ordinal = value.ordinal,
                .event_id = value.event_id,
            });
        }
    }
    return out.toOwnedSlice(allocator);
}

const Binary = enum { set_union, intersection, difference };

fn setBinaryAlloc(allocator: std.mem.Allocator, left: []const Match, right: []const Match, operation: Binary, limit: usize) ![]Match {
    var out: std.ArrayListUnmanaged(Match) = .empty;
    errdefer out.deinit(allocator);
    var left_index: usize = 0;
    var right_index: usize = 0;
    while (left_index < left.len or right_index < right.len) {
        const ordering: std.math.Order = if (left_index == left.len)
            .gt
        else if (right_index == right.len)
            .lt
        else
            compare(left[left_index], right[right_index]);
        switch (operation) {
            .set_union => switch (ordering) {
                .lt => {
                    try appendBounded(allocator, &out, left[left_index], limit);
                    left_index += 1;
                },
                .gt => {
                    try appendBounded(allocator, &out, right[right_index], limit);
                    right_index += 1;
                },
                .eq => {
                    try appendBounded(allocator, &out, left[left_index], limit);
                    left_index += 1;
                    right_index += 1;
                },
            },
            .intersection => switch (ordering) {
                .lt => left_index += 1,
                .gt => right_index += 1,
                .eq => {
                    try appendBounded(allocator, &out, left[left_index], limit);
                    left_index += 1;
                    right_index += 1;
                },
            },
            .difference => switch (ordering) {
                .lt => {
                    try appendBounded(allocator, &out, left[left_index], limit);
                    left_index += 1;
                },
                .gt => right_index += 1,
                .eq => {
                    left_index += 1;
                    right_index += 1;
                },
            },
        }
    }
    return out.toOwnedSlice(allocator);
}

fn compare(left: Match, right: Match) std.math.Order {
    if (left.history < right.history) return .lt;
    if (left.history > right.history) return .gt;
    if (left.position < right.position) return .lt;
    if (left.position > right.position) return .gt;
    return .eq;
}

fn appendBounded(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(Match), value: Match, limit: usize) !void {
    if (out.items.len == limit) return error.EventSetMatchLimitExceeded;
    try out.append(allocator, value);
}

fn momentsAlloc(allocator: std.mem.Allocator, input: []const Match, last: bool, limit: usize) ![]Match {
    var out: std.ArrayListUnmanaged(Match) = .empty;
    errdefer out.deinit(allocator);
    for (input) |value| {
        if (out.items.len > 0) {
            const prior = &out.items[out.items.len - 1];
            if (prior.history == value.history and prior.index == value.index) {
                if (last) prior.* = value;
                continue;
            }
        }
        if (out.items.len == limit) return error.EventSetMatchLimitExceeded;
        try out.append(allocator, value);
    }
    return out.toOwnedSlice(allocator);
}

const Timeline = enum { previous, next, sequence };

fn timelineAlloc(allocator: std.mem.Allocator, anchors: []const Match, related: []const Match, maximum: ?u64, operation: Timeline, limit: usize) ![]Match {
    var out: std.ArrayListUnmanaged(Match) = .empty;
    errdefer out.deinit(allocator);
    var related_cursor: usize = 0;
    for (anchors) |anchor| {
        while (related_cursor < related.len and
            (related[related_cursor].history < anchor.history or
                (related[related_cursor].history == anchor.history and related[related_cursor].position < anchor.position)))
            related_cursor += 1;
        const chosen: ?Match = switch (operation) {
            .previous => if (related_cursor > 0 and
                related[related_cursor - 1].history == anchor.history and
                related[related_cursor - 1].position < anchor.position)
                related[related_cursor - 1]
            else
                null,
            .next, .sequence => blk: {
                var next_cursor = related_cursor;
                while (next_cursor < related.len and related[next_cursor].history == anchor.history and
                    related[next_cursor].position <= anchor.position) next_cursor += 1;
                break :blk if (next_cursor < related.len and related[next_cursor].history == anchor.history)
                    related[next_cursor]
                else
                    null;
            },
        };
        if (chosen) |value| {
            const distance = if (value.index > anchor.index) value.index - anchor.index else anchor.index - value.index;
            if (maximum) |max| if (distance > max) continue;
            const emitted = if (operation == .sequence) anchor else value;
            if (out.items.len == 0 or !Match.equal(out.items[out.items.len - 1], emitted))
                try appendBounded(allocator, &out, emitted, limit);
        }
    }
    return out.toOwnedSlice(allocator);
}

test "saved event-set plans compose and count across histories" {
    const observation = @import("observation.zig");
    var history = try trace.Trace.init(std.testing.allocator, .{ .scenario = "event-set", .scenario_version = 1 }, .{ .transition_budget = 2 });
    defer history.deinit();
    const digest = observation.digestFeatures(&.{});
    try history.addObservation(.{ .index = 0, .digest = digest, .features = &.{} });
    for (1..3) |index| {
        try history.addChoice(.{ .site_id = 1, .site_name = "event-set", .occurrence = index - 1, .enabled_ids = &.{2}, .selected_id = 2 });
        try history.addTransition(.{ .index = index, .id = 2, .name = "step", .kind = .workload });
        try history.addObservation(.{ .index = index, .digest = digest, .features = &.{} });
    }
    try history.addEvent(.{ .index = 1, .ordinal = 0, .id = 10, .name = "request", .kind = .domain, .actor_id = 7, .resource_id = 8, .payload_digest = 1 });
    try history.addEvent(.{ .index = 2, .ordinal = 0, .id = 11, .name = "response", .kind = .client_response, .actor_id = 7, .resource_id = 8, .payload_digest = 2 });
    history.summary = .{ .transitions = 2, .final_observation_digest = digest, .property_failures = 0 };

    const steps = [_]Step{
        .{ .name = "requests", .operation = .select, .query = .{ .selector = .{ .name = "request" } } },
        .{ .name = "responses", .operation = .select, .query = .{ .selector = .{ .kind = .client_response } } },
        .{ .name = "all", .operation = .@"union", .inputs = &.{ 0, 1 } },
        .{ .name = "request-intersection", .operation = .intersection, .inputs = &.{ 0, 2 } },
        .{ .name = "request-difference", .operation = .difference, .inputs = &.{ 2, 1 } },
        .{ .name = "not-requests", .operation = .complement, .inputs = &.{0} },
        .{ .name = "next-response", .operation = .next_after, .inputs = &.{ 0, 1 }, .max_transition_distance = 1 },
        .{ .name = "previous-request", .operation = .previous_before, .inputs = &.{ 1, 0 }, .max_transition_distance = 1 },
        .{ .name = "sequence", .operation = .sequence, .inputs = &.{ 0, 1 }, .max_transition_distance = 1 },
    };
    var plan: Plan = .{
        .format = format,
        .name = "request-followed-by-response",
        .steps = &steps,
        .result = 8,
    };
    const histories = [_]History{ .{ .id = "run-a", .artifact = &history }, .{ .id = "run-b", .artifact = &history } };
    try std.testing.expectEqual(@as(usize, 2), try count(std.testing.allocator, &histories, plan));
    for ([_]usize{ 3, 4, 5, 6, 7 }) |result_index| {
        plan.result = result_index;
        try std.testing.expectEqual(@as(usize, 2), try count(std.testing.allocator, &histories, plan));
    }
    plan.result = 2;
    try std.testing.expectEqual(@as(usize, 4), try count(std.testing.allocator, &histories, plan));
    plan.result = 8;
    const encoded = try renderAlloc(std.testing.allocator, plan);
    defer std.testing.allocator.free(encoded);
    var parsed = try parseAlloc(std.testing.allocator, encoded);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), try count(std.testing.allocator, &histories, parsed.value));
}

test "event-set validation rejects forward references and invalid operations" {
    try std.testing.expectError(error.EventSetInputMustReferencePriorStep, (Plan{
        .format = format,
        .name = "bad",
        .steps = &.{.{ .name = "cycle", .operation = .@"union", .inputs = &.{ 0, 0 } }},
        .result = 0,
    }).validate());
}
