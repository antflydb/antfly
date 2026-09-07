// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Deterministic fielded queries over canonical VOPR event histories.
//!
//! Queries never affect replay. They operate on the already validated trace
//! and return stable `(index, ordinal)` coordinates suitable for reports,
//! debugger recipes, and CI APIs.

const std = @import("std");
const event = @import("event.zig");
const ids = @import("id.zig");
const trace = @import("trace.zig");

pub const Selector = struct {
    id: ?ids.StableId = null,
    name: ?[]const u8 = null,
    kind: ?event.Kind = null,
    actor_id: ?ids.StableId = null,
    resource_id: ?ids.StableId = null,
    payload_digest: ?u64 = null,
    fault_phase: ?trace.FaultPhase = null,

    pub fn matches(self: Selector, record: trace.EventRecord) bool {
        if (self.id) |value| if (record.id != value) return false;
        if (self.name) |value| if (!std.mem.eql(u8, record.name, value)) return false;
        if (self.kind) |value| if (record.kind != value) return false;
        if (self.actor_id) |value| if (record.actor_id != value) return false;
        if (self.resource_id) |value| if (record.resource_id != value) return false;
        if (self.payload_digest) |value| if (record.payload_digest != value) return false;
        if (self.fault_phase) |value| {
            const actual: ?trace.FaultPhase = switch (record.kind) {
                .fault_started => .start,
                .fault_stopped => .end,
                else => null,
            };
            if (actual != value) return false;
        }
        return true;
    }
};

pub const Relation = struct {
    selector: Selector,
    /// Maximum transition distance. `null` searches the complete history.
    max_transition_distance: ?u64 = null,
    same_actor: bool = false,
    same_resource: bool = false,
};

pub const Query = struct {
    selector: Selector = .{},
    first_index: u64 = 0,
    last_index: u64 = std.math.maxInt(u64),
    preceded_by: ?Relation = null,
    followed_by: ?Relation = null,
    limit: usize = std.math.maxInt(usize),

    pub fn validate(self: Query) !void {
        if (self.first_index > self.last_index) return error.InvalidEventQueryWindow;
        if (self.limit == 0) return error.InvalidEventQueryLimit;
    }
};

pub const Match = struct {
    position: usize,
    index: u64,
    ordinal: u64,
    event_id: ids.StableId,
};

pub fn searchAlloc(allocator: std.mem.Allocator, history: *const trace.Trace, query: Query) ![]Match {
    try history.validate();
    try query.validate();
    var matches: std.ArrayListUnmanaged(Match) = .empty;
    errdefer matches.deinit(allocator);
    for (history.events.items, 0..) |record, position| {
        if (record.index < query.first_index or record.index > query.last_index) continue;
        if (!query.selector.matches(record)) continue;
        if (query.preceded_by) |relation| {
            if (!hasRelated(history.events.items, position, record, relation, .before)) continue;
        }
        if (query.followed_by) |relation| {
            if (!hasRelated(history.events.items, position, record, relation, .after)) continue;
        }
        try matches.append(allocator, .{
            .position = position,
            .index = record.index,
            .ordinal = record.ordinal,
            .event_id = record.id,
        });
        if (matches.items.len == query.limit) break;
    }
    return matches.toOwnedSlice(allocator);
}

const Direction = enum { before, after };

fn hasRelated(
    records: []const trace.EventRecord,
    position: usize,
    anchor: trace.EventRecord,
    relation: Relation,
    direction: Direction,
) bool {
    switch (direction) {
        .before => {
            var cursor = position;
            while (cursor > 0) {
                cursor -= 1;
                const candidate = records[cursor];
                if (!withinDistance(anchor.index, candidate.index, relation.max_transition_distance)) break;
                if (relatedMatches(anchor, candidate, relation)) return true;
            }
        },
        .after => {
            var cursor = position + 1;
            while (cursor < records.len) : (cursor += 1) {
                const candidate = records[cursor];
                if (!withinDistance(candidate.index, anchor.index, relation.max_transition_distance)) break;
                if (relatedMatches(anchor, candidate, relation)) return true;
            }
        },
    }
    return false;
}

fn withinDistance(later: u64, earlier: u64, maximum: ?u64) bool {
    const limit = maximum orelse return true;
    return later -| earlier <= limit;
}

fn relatedMatches(anchor: trace.EventRecord, candidate: trace.EventRecord, relation: Relation) bool {
    if (!relation.selector.matches(candidate)) return false;
    if (relation.same_actor and (anchor.actor_id == null or anchor.actor_id != candidate.actor_id)) return false;
    if (relation.same_resource and (anchor.resource_id == null or anchor.resource_id != candidate.resource_id)) return false;
    return true;
}

test "event queries combine fields windows and temporal relations" {
    const observation = @import("observation.zig");
    var history = try trace.Trace.init(std.testing.allocator, .{
        .scenario = "event-query",
        .scenario_version = 1,
    }, .{ .transition_budget = 3 });
    defer history.deinit();
    const digest = observation.digestFeatures(&.{});
    try history.addObservation(.{ .index = 0, .digest = digest, .features = &.{} });
    for (1..4) |index| {
        try history.addChoice(.{ .site_id = 1, .site_name = "event-query", .occurrence = index - 1, .enabled_ids = &.{2}, .selected_id = 2 });
        try history.addTransition(.{ .index = index, .id = 2, .name = "step", .kind = .workload });
        try history.addObservation(.{ .index = index, .digest = digest, .features = &.{} });
    }
    try history.addEvent(.{ .index = 1, .ordinal = 0, .id = 10, .name = "request", .kind = .domain, .actor_id = 7, .resource_id = 9, .payload_digest = 1 });
    try history.addEvent(.{ .index = 2, .ordinal = 0, .id = 11, .name = "fault", .kind = .fault_started, .actor_id = 7, .resource_id = 9, .payload_digest = 2 });
    try history.addEvent(.{ .index = 3, .ordinal = 0, .id = 12, .name = "response", .kind = .client_response, .actor_id = 7, .resource_id = 9, .payload_digest = 3 });
    history.summary = .{ .transitions = 3, .final_observation_digest = digest, .property_failures = 0 };

    const matches = try searchAlloc(std.testing.allocator, &history, .{
        .selector = .{ .name = "fault", .fault_phase = .start, .actor_id = 7 },
        .preceded_by = .{ .selector = .{ .name = "request" }, .max_transition_distance = 1, .same_resource = true },
        .followed_by = .{ .selector = .{ .kind = .client_response }, .max_transition_distance = 1, .same_actor = true },
    });
    defer std.testing.allocator.free(matches);
    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqual(@as(u64, 2), matches[0].index);
}
