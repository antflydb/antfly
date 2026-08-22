// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");
const ids = @import("id.zig");

pub const Kind = enum { always, always_or_unreachable, reachable, @"unreachable", sometimes, eventually_after_quiescence };

pub const Declaration = struct {
    id: ids.StableId,
    name: []const u8,
    kind: Kind,
    /// Required only for eventually_after_quiescence.
    quiescence_budget: u64 = 0,

    pub fn named(kind: Kind, name: []const u8) Declaration {
        return .{ .id = ids.stable("property", name), .name = name, .kind = kind };
    }
};

pub const Evaluation = struct {
    property_id: ids.StableId,
    condition: bool,
    details: []const u8 = "",
};

pub const Sink = struct {
    evaluations: std.ArrayListUnmanaged(Evaluation) = .empty,

    pub fn deinit(self: *Sink, allocator: std.mem.Allocator) void {
        self.evaluations.deinit(allocator);
        self.* = .{};
    }

    pub fn check(self: *Sink, allocator: std.mem.Allocator, property_id: ids.StableId, condition: bool) !void {
        try self.evaluations.append(allocator, .{ .property_id = property_id, .condition = condition });
    }

    pub fn canonicalize(self: *Sink) !void {
        std.mem.sort(Evaluation, self.evaluations.items, {}, struct {
            fn lessThan(_: void, lhs: Evaluation, rhs: Evaluation) bool {
                return lhs.property_id < rhs.property_id;
            }
        }.lessThan);
        for (self.evaluations.items, 0..) |evaluation, index| {
            if (index > 0 and self.evaluations.items[index - 1].property_id == evaluation.property_id) return error.DuplicatePropertyEvaluation;
        }
    }
};

pub const Status = struct {
    evaluations: u64 = 0,
    ever_true: bool = false,
    ever_false: bool = false,
    failed: bool = false,
    first_failure_transition: ?u64 = null,
    quiescence_started_at: ?u64 = null,
    satisfied_after_quiescence: bool = false,
};

pub const Tracker = struct {
    allocator: std.mem.Allocator,
    declarations: []const Declaration,
    statuses: []Status,

    pub fn init(allocator: std.mem.Allocator, declarations: []const Declaration) !Tracker {
        for (declarations, 0..) |decl, index| {
            if (decl.id == 0) return error.InvalidPropertyId;
            if (decl.name.len == 0) return error.EmptyPropertyName;
            if (decl.kind == .eventually_after_quiescence and decl.quiescence_budget == 0) return error.InvalidQuiescenceBudget;
            for (declarations[0..index]) |prior| if (prior.id == decl.id) return error.DuplicatePropertyId;
        }
        const statuses = try allocator.alloc(Status, declarations.len);
        @memset(statuses, .{});
        return .{ .allocator = allocator, .declarations = declarations, .statuses = statuses };
    }

    pub fn deinit(self: *Tracker) void {
        self.allocator.free(self.statuses);
        self.* = undefined;
    }

    pub fn beginQuiescence(self: *Tracker, transition_index: u64) void {
        for (self.declarations, self.statuses) |decl, *status| {
            if (decl.kind == .eventually_after_quiescence and status.quiescence_started_at == null) {
                status.quiescence_started_at = transition_index;
                status.satisfied_after_quiescence = false;
            }
        }
    }

    pub fn record(self: *Tracker, transition_index: u64, evaluation: Evaluation) !void {
        const index = self.indexOf(evaluation.property_id) orelse return error.UnknownPropertyId;
        var status = &self.statuses[index];
        const decl = self.declarations[index];
        status.evaluations += 1;
        status.ever_true = status.ever_true or evaluation.condition;
        status.ever_false = status.ever_false or !evaluation.condition;
        if (decl.kind == .eventually_after_quiescence and status.quiescence_started_at != null and evaluation.condition) {
            status.satisfied_after_quiescence = true;
        }

        const violated = switch (decl.kind) {
            .always => !evaluation.condition,
            .always_or_unreachable => !evaluation.condition,
            .@"unreachable" => evaluation.condition,
            .reachable, .sometimes => false,
            .eventually_after_quiescence => blk: {
                const start = status.quiescence_started_at orelse break :blk false;
                break :blk !status.satisfied_after_quiescence and transition_index >= start +| decl.quiescence_budget;
            },
        };
        if (violated) markFailed(status, transition_index);
    }

    pub fn finish(self: *Tracker, final_transition: u64) void {
        for (self.declarations, self.statuses) |decl, *status| {
            const violated = switch (decl.kind) {
                .always => status.evaluations == 0,
                .always_or_unreachable => false,
                .@"unreachable" => false,
                .reachable, .sometimes => !status.ever_true,
                .eventually_after_quiescence => status.quiescence_started_at != null and !status.satisfied_after_quiescence,
            };
            if (violated) markFailed(status, final_transition);
        }
    }

    pub fn failureCount(self: *const Tracker) usize {
        var count: usize = 0;
        for (self.statuses) |status| count += @intFromBool(status.failed);
        return count;
    }

    fn indexOf(self: *const Tracker, property_id: ids.StableId) ?usize {
        for (self.declarations, 0..) |decl, index| if (decl.id == property_id) return index;
        return null;
    }

    fn markFailed(status: *Status, transition_index: u64) void {
        if (!status.failed) status.first_failure_transition = transition_index;
        status.failed = true;
    }
};

test "property kinds aggregate without short-circuiting" {
    const declarations = [_]Declaration{
        .{ .id = 1, .name = "always", .kind = .always },
        .{ .id = 2, .name = "reachable", .kind = .reachable },
        .{ .id = 3, .name = "forbidden", .kind = .@"unreachable" },
        .{ .id = 4, .name = "eventual", .kind = .eventually_after_quiescence, .quiescence_budget = 2 },
    };
    var tracker = try Tracker.init(std.testing.allocator, &declarations);
    defer tracker.deinit();
    try tracker.record(1, .{ .property_id = 1, .condition = false });
    try tracker.record(1, .{ .property_id = 2, .condition = true });
    try tracker.record(1, .{ .property_id = 3, .condition = false });
    tracker.beginQuiescence(2);
    try tracker.record(4, .{ .property_id = 4, .condition = false });
    tracker.finish(4);
    try std.testing.expectEqual(@as(usize, 2), tracker.failureCount());
}

test "property evaluations canonicalize and reject duplicates" {
    var sink = Sink{};
    defer sink.deinit(std.testing.allocator);
    try sink.check(std.testing.allocator, 9, true);
    try sink.check(std.testing.allocator, 2, false);
    try sink.canonicalize();
    try std.testing.expectEqual(@as(u64, 2), sink.evaluations.items[0].property_id);
    try sink.check(std.testing.allocator, 2, true);
    try std.testing.expectError(error.DuplicatePropertyEvaluation, sink.canonicalize());
}
