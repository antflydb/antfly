// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded, model-independent classification constraint compiler and decoder.
//! The AST and centered-logit objective follow Fastino GLiNER2 commit
//! 3c913c7369301133d3b7699252074c4303ada50e. Search exhaustion is not
//! infeasibility, and a beam solution is never presented as an exact optimum.
const std = @import("std");

pub const max_tasks = 32;
pub const max_labels = 128;
pub const Selection = u128;
pub const Truth = enum { yes, no, unknown };
pub const Task = struct {
    name: []const u8,
    labels: []const []const u8,
    min_labels: usize = 0,
    max_labels: ?usize = null,
    ordered: bool = false,
    threshold: f64 = 0.5,
    candidate_threshold: ?f64 = null,
    temperature: f64 = 1,
    default_label: ?usize = null,

    pub fn maximum(self: Task) usize {
        return self.max_labels orelse self.labels.len;
    }
};
pub const Ref = struct { task: usize, label: usize };
pub const Count = struct { task: usize, minimum: usize, maximum: usize };
pub const Pair = struct { left: usize, right: usize };
pub const Node = union(enum) {
    label: Ref,
    any_selected: usize,
    any_other_selected: usize,
    is_default: usize,
    cardinality: Count,
    min_level: Ref,
    max_level: Ref,
    at_level: Ref,
    negate: usize,
    all: []const usize,
    any: []const usize,
    exactly_one: []const usize,
    implies: Pair,
    iff: Pair,
    excludes: Pair,

    pub fn jsonStringify(self: Node, writer: anytype) !void {
        try writer.beginObject();
        try writer.objectField("operator");
        try writer.write(@tagName(self));
        try writer.objectField("arguments");
        switch (self) {
            inline else => |arguments| try writer.write(arguments),
        }
        try writer.endObject();
    }
};
pub const CompileLimits = struct { max_nodes: usize = 512, max_depth: usize = 32 };

pub const Assignment = struct {
    /// Labels already fixed to selected. A complete assignment has no
    /// additional possible labels (or may repeat the selected set there).
    selected: []const Selection,
    /// Labels that may still be selected; callers need not repeat selected.
    possible: []const Selection,
};

/// All memory belongs to the allocator supplied to compileValue. Use an arena,
/// or the OwnedProgram returned by compile, to retain and release it together.
pub const Program = struct {
    tasks: []const Task,
    nodes: []const Node,
    roots: []const usize,

    pub fn evaluate(self: Program, assignment: Assignment) !Truth {
        if (assignment.selected.len != self.tasks.len or assignment.possible.len != self.tasks.len)
            return error.InvalidConstraintAssignment;
        for (self.tasks, 0..) |task, i| {
            const valid = labelMask(task.labels.len);
            if ((assignment.selected[i] | assignment.possible[i]) & ~valid != 0)
                return error.InvalidConstraintAssignment;
        }
        var answer: Truth = .yes;
        for (self.roots) |root| {
            const value = self.evalNode(root, assignment);
            if (value == .no) return .no;
            if (value == .unknown) answer = .unknown;
        }
        return answer;
    }

    fn evalNode(self: Program, index: usize, a: Assignment) Truth {
        return switch (self.nodes[index]) {
            .label => |ref| holds(a, ref),
            .is_default => |task| holds(a, .{ .task = task, .label = self.tasks[task].default_label.? }),
            .any_selected, .any_other_selected => |task| blk: {
                const omit = if (self.nodes[index] == .any_other_selected) bit(self.tasks[task].default_label.?) else 0;
                if (a.selected[task] & ~omit != 0) break :blk .yes;
                break :blk if (a.possible[task] & ~omit != 0) .unknown else .no;
            },
            .cardinality => |count| blk: {
                const lo = @popCount(a.selected[count.task]);
                const hi = @popCount(a.selected[count.task] | a.possible[count.task]);
                if (lo > count.maximum or hi < count.minimum) break :blk .no;
                break :blk if (lo >= count.minimum and hi <= count.maximum) .yes else .unknown;
            },
            .min_level, .max_level, .at_level => |ref| blk: {
                const levels = a.selected[ref.task] | a.possible[ref.task];
                if (levels == 0) break :blk .no;
                const low: usize = @intCast(@ctz(levels));
                const high: usize = 127 - @as(usize, @intCast(@clz(levels)));
                break :blk switch (self.nodes[index]) {
                    .min_level => if (low >= ref.label) .yes else if (high < ref.label) .no else .unknown,
                    .max_level => if (high <= ref.label) .yes else if (low > ref.label) .no else .unknown,
                    .at_level => if (levels == bit(ref.label)) .yes else if (levels & bit(ref.label) == 0) .no else .unknown,
                    else => unreachable,
                };
            },
            .negate => |child| negate(self.evalNode(child, a)),
            .all, .any, .exactly_one => |children| blk: {
                var yes: usize = 0;
                var unknown: usize = 0;
                for (children) |child| switch (self.evalNode(child, a)) {
                    .yes => yes += 1,
                    .unknown => unknown += 1,
                    .no => {},
                };
                break :blk switch (self.nodes[index]) {
                    .all => if (yes + unknown < children.len) .no else if (unknown > 0) .unknown else .yes,
                    .any => if (yes > 0) .yes else if (unknown > 0) .unknown else .no,
                    .exactly_one => if (yes > 1 or yes + unknown == 0) .no else if (unknown > 0) .unknown else .yes,
                    else => unreachable,
                };
            },
            .implies, .iff, .excludes => |pair| blk: {
                const left = self.evalNode(pair.left, a);
                const right = self.evalNode(pair.right, a);
                break :blk switch (self.nodes[index]) {
                    .implies => if (left == .no or right == .yes) .yes else if (left == .yes and right == .no) .no else .unknown,
                    .iff => if (left == .unknown or right == .unknown) .unknown else if (left == right) .yes else .no,
                    .excludes => if (left == .no or right == .no) .yes else if (left == .yes and right == .yes) .no else .unknown,
                    else => unreachable,
                };
            },
        };
    }

    fn taskReferences(self: Program, index: usize) u32 {
        return switch (self.nodes[index]) {
            .label, .min_level, .max_level, .at_level => |ref| taskBit(ref.task),
            .any_selected, .any_other_selected, .is_default => |task| taskBit(task),
            .cardinality => |count| taskBit(count.task),
            .negate => |child| self.taskReferences(child),
            .all, .any, .exactly_one => |children| blk: {
                var refs: u32 = 0;
                for (children) |child| refs |= self.taskReferences(child);
                break :blk refs;
            },
            .implies, .iff, .excludes => |pair| self.taskReferences(pair.left) | self.taskReferences(pair.right),
        };
    }
};

pub const OwnedProgram = struct {
    arena: std.heap.ArenaAllocator,
    program: Program,
    pub fn deinit(self: *OwnedProgram) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn compile(allocator: std.mem.Allocator, tasks: []const Task, json: []const u8, limits: CompileLimits) !OwnedProgram {
    if (json.len > 65536) return error.ConstraintLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const parsed = try std.json.parseFromSlice(std.json.Value, arena.allocator(), json, .{
        .allocate = .alloc_always,
        .duplicate_field_behavior = .@"error",
    });
    const program = try compileValue(arena.allocator(), tasks, parsed.value, limits);
    return .{ .arena = arena, .program = program };
}

pub fn compileValue(allocator: std.mem.Allocator, tasks: []const Task, value: std.json.Value, limits: CompileLimits) !Program {
    if (tasks.len > max_tasks or limits.max_depth == 0 or limits.max_depth > 64 or limits.max_nodes == 0 or limits.max_nodes > 4096)
        return error.ConstraintLimitExceeded;
    if (value != .array) return error.InvalidConstraint;
    const owned_tasks = try allocator.dupe(Task, tasks);
    for (owned_tasks, 0..) |*task, i| {
        try validateTask(task.*);
        for (owned_tasks[0..i]) |other| if (std.mem.eql(u8, task.name, other.name)) return error.DuplicateClassificationTask;
        task.name = try allocator.dupe(u8, task.name);
        const names = try allocator.alloc([]const u8, task.labels.len);
        for (task.labels, names) |name, *out| out.* = try allocator.dupe(u8, name);
        task.labels = names;
    }
    var parser = Parser{ .allocator = allocator, .tasks = owned_tasks, .limits = limits };
    var roots = std.ArrayListUnmanaged(usize).empty;
    for (value.array.items) |raw| try roots.append(allocator, try parser.parse(raw, 1));
    // The default means precisely "selected iff no other label is selected".
    for (owned_tasks, 0..) |task, task_index| if (task.default_label != null) {
        const default = try parser.append(.{ .is_default = task_index });
        const other = try parser.append(.{ .any_other_selected = task_index });
        const no_other = try parser.append(.{ .negate = other });
        try roots.append(allocator, try parser.append(.{ .iff = .{ .left = default, .right = no_other } }));
    };
    return .{ .tasks = owned_tasks, .nodes = try parser.nodes.toOwnedSlice(allocator), .roots = try roots.toOwnedSlice(allocator) };
}

pub fn validateTask(task: Task) !void {
    if (task.name.len == 0 or task.labels.len == 0 or task.labels.len > max_labels) return error.InvalidClassificationTask;
    if (task.min_labels > task.maximum() or task.maximum() > task.labels.len) return error.InvalidClassificationCardinality;
    if (task.ordered and task.labels.len < 2) return error.InvalidOrdinalTask;
    if (!std.math.isFinite(task.threshold) or task.threshold <= 0 or task.threshold >= 1 or
        !std.math.isFinite(task.temperature) or task.temperature <= 0) return error.InvalidClassificationCalibration;
    if (task.candidate_threshold) |threshold| if (!std.math.isFinite(threshold) or threshold < 0 or threshold > 1)
        return error.InvalidClassificationCalibration;
    if (task.default_label) |index| if (index >= task.labels.len or task.min_labels == 0) return error.InvalidClassificationDefault;
    for (task.labels, 0..) |label, i| {
        if (label.len == 0) return error.InvalidClassificationLabel;
        for (task.labels[0..i]) |other| if (std.mem.eql(u8, label, other)) return error.DuplicateClassificationLabel;
    }
}

const Parser = struct {
    allocator: std.mem.Allocator,
    tasks: []const Task,
    limits: CompileLimits,
    nodes: std.ArrayListUnmanaged(Node) = .empty,

    fn append(self: *Parser, node: Node) !usize {
        if (self.nodes.items.len >= self.limits.max_nodes) return error.ConstraintLimitExceeded;
        const index = self.nodes.items.len;
        try self.nodes.append(self.allocator, node);
        return index;
    }
    fn taskIndex(self: *Parser, value: std.json.Value) !usize {
        const name = try string(value);
        for (self.tasks, 0..) |task, i| if (std.mem.eql(u8, task.name, name)) return i;
        return error.UnknownConstraintTask;
    }
    fn ref(self: *Parser, object: std.json.ObjectMap, label_key: []const u8, ordered: bool) !Ref {
        const task = try self.taskIndex(try required(object, "task"));
        if (ordered and !self.tasks[task].ordered) return error.InvalidOrdinalConstraint;
        const label = try string(try required(object, label_key));
        for (self.tasks[task].labels, 0..) |name, i| if (std.mem.eql(u8, label, name)) return .{ .task = task, .label = i };
        return error.UnknownConstraintLabel;
    }
    fn parse(self: *Parser, value: std.json.Value, depth: usize) anyerror!usize {
        if (depth > self.limits.max_depth) return error.ConstraintLimitExceeded;
        if (value != .object) return error.InvalidConstraint;
        const obj = value.object;
        const kind = try string(try required(obj, "type"));
        if (std.mem.eql(u8, kind, "LabelRef")) {
            try keys(obj, &.{ "type", "task", "label" });
            return self.append(.{ .label = try self.ref(obj, "label", false) });
        }
        inline for (.{ .{ "AnySelected", "any_selected" }, .{ "AnyOtherSelected", "any_other_selected" }, .{ "IsDefault", "is_default" } }) |entry| {
            if (std.mem.eql(u8, kind, entry[0])) {
                try keys(obj, &.{ "type", "task" });
                const task = try self.taskIndex(try required(obj, "task"));
                if (!std.mem.eql(u8, entry[0], "AnySelected") and self.tasks[task].default_label == null) return error.MissingClassificationDefault;
                return self.append(@unionInit(Node, entry[1], task));
            }
        }
        if (std.mem.eql(u8, kind, "Cardinality")) {
            try keys(obj, &.{ "type", "task", "minimum", "maximum" });
            const task = try self.taskIndex(try required(obj, "task"));
            const minimum = if (obj.get("minimum")) |v| try integer(v) else 0;
            const maximum = if (obj.get("maximum")) |v| if (v == .null) self.tasks[task].labels.len else try integer(v) else self.tasks[task].labels.len;
            if (minimum > maximum or maximum > self.tasks[task].labels.len) return error.InvalidConstraintCardinality;
            return self.append(.{ .cardinality = .{ .task = task, .minimum = minimum, .maximum = maximum } });
        }
        inline for (.{ .{ "MinLevel", "min_level" }, .{ "MaxLevel", "max_level" }, .{ "AtLevel", "at_level" } }) |entry| {
            if (std.mem.eql(u8, kind, entry[0])) {
                try keys(obj, &.{ "type", "task", "level" });
                return self.append(@unionInit(Node, entry[1], try self.ref(obj, "level", true)));
            }
        }
        if (std.mem.eql(u8, kind, "Not")) {
            try keys(obj, &.{ "type", "child" });
            return self.append(.{ .negate = try self.parse(try required(obj, "child"), depth + 1) });
        }
        inline for (.{ .{ "And", "all" }, .{ "Or", "any" }, .{ "ExactlyOneOf", "exactly_one" } }) |entry| {
            if (std.mem.eql(u8, kind, entry[0])) {
                try keys(obj, &.{ "type", "children" });
                const children = try required(obj, "children");
                if (children != .array or children.array.items.len > self.limits.max_nodes) return error.InvalidConstraint;
                const out = try self.allocator.alloc(usize, children.array.items.len);
                for (children.array.items, out) |child, *index| index.* = try self.parse(child, depth + 1);
                return self.append(@unionInit(Node, entry[1], out));
            }
        }
        inline for (.{ .{ "Implies", "implies", "cond", "then" }, .{ "Iff", "iff", "left", "right" }, .{ "Excludes", "excludes", "left", "right" } }) |entry| {
            if (std.mem.eql(u8, kind, entry[0])) {
                try keys(obj, &.{ "type", entry[2], entry[3] });
                return self.append(@unionInit(Node, entry[1], Pair{
                    .left = try self.parse(try required(obj, entry[2]), depth + 1),
                    .right = try self.parse(try required(obj, entry[3]), depth + 1),
                }));
            }
        }
        return error.UnknownConstraintType;
    }
};

fn required(obj: std.json.ObjectMap, name: []const u8) !std.json.Value {
    return obj.get(name) orelse error.InvalidConstraint;
}
fn string(value: std.json.Value) ![]const u8 {
    return if (value == .string) value.string else error.InvalidConstraint;
}
fn integer(value: std.json.Value) !usize {
    if (value != .integer or value.integer < 0) return error.InvalidConstraint;
    return std.math.cast(usize, value.integer) orelse error.InvalidConstraint;
}
fn keys(obj: std.json.ObjectMap, allowed: []const []const u8) !void {
    for (obj.keys()) |key| {
        var found = false;
        for (allowed) |name| if (std.mem.eql(u8, key, name)) {
            found = true;
            break;
        };
        if (!found) return error.UnknownConstraintField;
    }
}
fn bit(index: usize) Selection {
    return @as(Selection, 1) << @as(u7, @intCast(index));
}
fn taskBit(index: usize) u32 {
    return @as(u32, 1) << @as(u5, @intCast(index));
}
fn labelMask(count: usize) Selection {
    return if (count == 128) std.math.maxInt(Selection) else if (count == 0) 0 else bit(count) - 1;
}
fn negate(value: Truth) Truth {
    return switch (value) {
        .yes => .no,
        .no => .yes,
        .unknown => .unknown,
    };
}
fn holds(a: Assignment, ref: Ref) Truth {
    return if (a.selected[ref.task] & bit(ref.label) != 0) .yes else if (a.possible[ref.task] & bit(ref.label) != 0) .unknown else .no;
}

pub const Algorithm = enum { exact, beam, auto };
pub const Status = enum { optimal, feasible, infeasible, search_exhausted };
pub const SolveOptions = struct {
    algorithm: Algorithm = .auto,
    max_local_assignments: usize = 4096,
    max_subset_visits: usize = 65536,
    exact_node_budget: usize = 200000,
    beam_node_budget: usize = 200000,
    beam_width: usize = 64,
    /// Cooperative cancellation/deadline hook. Called during enumeration and search.
    check_context: ?*anyopaque = null,
    check_fn: ?*const fn (?*anyopaque) anyerror!void = null,
    fn check(self: SolveOptions) !void {
        if (self.check_fn) |f| try f(self.check_context);
    }
};
pub const Result = struct {
    allocator: std.mem.Allocator,
    status: Status,
    selections: []Selection,
    utility: f64,
    visited_nodes: usize,
    /// True only when the final search method hit its node budget. A bounded
    /// beam that completed all layers can be approximate without exhaustion.
    exhausted: bool = false,
    pub fn deinit(self: *Result) void {
        self.allocator.free(self.selections);
        self.* = undefined;
    }
    pub fn valid(self: Result) bool {
        return self.status == .optimal or self.status == .feasible;
    }
};
const Candidate = struct { selected: Selection, utility: f64, lexical: Selection = 0 };
const BeamOutcome = enum { complete, pruned, exhausted };

/// Uses every supplied label. Candidate retention is intentionally an upstream
/// caller concern: silently dropping low-score constraint labels can manufacture
/// infeasibility. Resource limits reject enumeration instead of dropping choices.
pub fn solve(allocator: std.mem.Allocator, program: Program, logits: []const []const f64, options: SolveOptions) !Result {
    if (program.tasks.len > max_tasks or logits.len != program.tasks.len or options.max_local_assignments == 0 or options.max_subset_visits == 0 or
        options.beam_width == 0 or options.beam_width > 4096) return error.InvalidConstraintSolveOptions;
    try options.check();
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const locals = try alloc.alloc([]const Candidate, program.tasks.len);
    var possible = [_]Selection{0} ** max_tasks;
    var always = [_]Selection{0} ** max_tasks;
    for (program.tasks, logits, 0..) |task, scores, i| {
        if (scores.len != task.labels.len) return error.InvalidClassificationLogits;
        locals[i] = try enumerate(alloc, program, i, scores, options);
        always[i] = labelMask(task.labels.len);
        for (locals[i]) |candidate| {
            possible[i] |= candidate.selected;
            always[i] &= candidate.selected;
        }
        if (locals[i].len == 0) return .{ .allocator = allocator, .status = .infeasible, .selections = try allocator.alloc(Selection, 0), .utility = -std.math.inf(f64), .visited_nodes = 0 };
    }
    var search = Search{ .program = program, .locals = locals, .possible = possible, .always = always, .options = options };
    search.initializeOrder();
    var state = State{};
    var exhausted = false;
    var approximate = false;
    if (options.algorithm != .beam) search.dfs(0, &state) catch |err| switch (err) {
        error.ConstraintSearchExhausted => {
            exhausted = true;
            approximate = true;
        },
        else => return err,
    };
    if (options.algorithm == .beam or (options.algorithm == .auto and exhausted)) {
        const outcome = try search.beam(alloc);
        approximate = outcome != .complete;
        exhausted = outcome == .exhausted;
    }
    const status: Status = if (search.best) |_| if (approximate) .feasible else .optimal else if (approximate) .search_exhausted else .infeasible;
    try options.check();
    return .{
        .allocator = allocator,
        .status = status,
        .selections = if (search.best) |best| try allocator.dupe(Selection, best.selected[0..program.tasks.len]) else try allocator.alloc(Selection, 0),
        .utility = if (search.best) |best| best.score else -std.math.inf(f64),
        .visited_nodes = search.visits,
        .exhausted = exhausted,
    };
}

pub fn utility(logit: f64, task: Task) !f64 {
    if (!std.math.isFinite(logit)) return error.InvalidClassificationLogits;
    const result = logit / task.temperature - @log(task.threshold / (1 - task.threshold));
    if (!std.math.isFinite(result)) return error.InvalidClassificationLogits;
    return result;
}

fn enumerate(alloc: std.mem.Allocator, program: Program, task_index: usize, logits: []const f64, options: SolveOptions) ![]const Candidate {
    const task = program.tasks[task_index];
    var utils: [max_labels]f64 = undefined;
    for (logits, 0..) |logit, i| utils[i] = try utility(logit, task);
    var bound: Selection = 0;
    var full_set = false;
    var count_coupled = false;
    for (program.nodes) |node| switch (node) {
        .label => |ref| if (ref.task == task_index) {
            bound |= bit(ref.label);
        },
        .any_selected, .any_other_selected, .is_default => |index| if (index == task_index) {
            full_set = true;
        },
        .min_level, .max_level, .at_level => |ref| if (ref.task == task_index) {
            full_set = true;
        },
        .cardinality => |count| if (count.task == task_index) {
            count_coupled = true;
        },
        else => {},
    };
    if (full_set) bound = labelMask(task.labels.len);
    var list = std.ArrayListUnmanaged(Candidate).empty;
    if (task.maximum() <= 1) {
        if (task.min_labels == 0) try list.append(alloc, .{ .selected = 0, .utility = 0 });
        if (task.maximum() == 1) for (task.labels, 0..) |_, i| try list.append(alloc, .{ .selected = bit(i), .utility = utils[i] });
    } else {
        const count = @popCount(bound);
        if (count >= @bitSizeOf(usize)) return error.ConstraintCandidateLimitExceeded;
        const subsets = @as(usize, 1) << @as(std.math.Log2Int(usize), @intCast(count));
        if (subsets > options.max_subset_visits) return error.ConstraintCandidateLimitExceeded;
        var free: [max_labels]usize = undefined;
        var free_len: usize = 0;
        var bound_indices: [max_labels]usize = undefined;
        var bound_len: usize = 0;
        for (task.labels, 0..) |_, i| {
            if (bound & bit(i) != 0) {
                bound_indices[bound_len] = i;
                bound_len += 1;
            } else {
                free[free_len] = i;
                free_len += 1;
            }
        }
        const FreeOrder = struct { scores: *const [max_labels]f64, names: []const []const u8 };
        std.mem.sort(usize, free[0..free_len], FreeOrder{ .scores = &utils, .names = task.labels }, struct {
            fn less(context: FreeOrder, a: usize, b: usize) bool {
                return context.scores[a] > context.scores[b] or
                    (context.scores[a] == context.scores[b] and std.mem.lessThan(u8, context.names[a], context.names[b]));
            }
        }.less);
        for (0..subsets) |subset| {
            if (subset % 128 == 0) try options.check();
            var selected: Selection = 0;
            for (bound_indices[0..bound_len], 0..) |index, j| if (subset & (@as(usize, 1) << @as(std.math.Log2Int(usize), @intCast(j))) != 0) {
                selected |= bit(index);
            };
            const chosen: usize = @intCast(@popCount(selected));
            if (chosen > task.maximum()) continue;
            if (count_coupled and !full_set) {
                const lo = @max(task.min_labels, chosen);
                const hi = @min(task.maximum(), chosen + free_len);
                if (lo > hi) continue;
                for (lo..hi + 1) |total| {
                    var mask = selected;
                    for (free[0 .. total - chosen]) |index| mask |= bit(index);
                    try appendCandidate(alloc, &list, mask, &utils, options.max_local_assignments);
                }
            } else {
                var total = chosen;
                for (free[0..free_len]) |index| {
                    if (total >= task.maximum()) break;
                    if (utils[index] > 0 or total < task.min_labels) {
                        selected |= bit(index);
                        total += 1;
                    }
                }
                if (total >= task.min_labels) try appendCandidate(alloc, &list, selected, &utils, options.max_local_assignments);
            }
        }
    }
    if (list.items.len > options.max_local_assignments) return error.ConstraintCandidateLimitExceeded;
    var alphabetic: [max_labels]usize = undefined;
    for (task.labels, 0..) |_, i| alphabetic[i] = i;
    std.mem.sort(usize, alphabetic[0..task.labels.len], task.labels, struct {
        fn less(names: []const []const u8, a: usize, b: usize) bool {
            return std.mem.lessThan(u8, names[a], names[b]);
        }
    }.less);
    for (list.items) |*candidate| for (alphabetic[0..task.labels.len], 0..) |index, rank| {
        if (candidate.selected & bit(index) != 0) candidate.lexical |= bit(rank);
    };
    std.mem.sort(Candidate, list.items, {}, struct {
        fn less(_: void, a: Candidate, b: Candidate) bool {
            return a.utility > b.utility or (a.utility == b.utility and lexicalLess(a.lexical, b.lexical));
        }
    }.less);
    return try list.toOwnedSlice(alloc);
}
fn appendCandidate(alloc: std.mem.Allocator, list: *std.ArrayListUnmanaged(Candidate), selected: Selection, utils: *const [max_labels]f64, limit: usize) !void {
    if (list.items.len >= limit) return error.ConstraintCandidateLimitExceeded;
    var score: f64 = 0;
    var mask = selected;
    while (mask != 0) {
        const index: usize = @intCast(@ctz(mask));
        score += utils[index];
        mask &= mask - 1;
    }
    if (!std.math.isFinite(score)) return error.InvalidClassificationLogits;
    try list.append(alloc, .{ .selected = selected, .utility = score });
}
const State = struct { selected: [max_tasks]Selection = [_]Selection{0} ** max_tasks, lexical: [max_tasks]Selection = [_]Selection{0} ** max_tasks, score: f64 = 0 };
const Search = struct {
    program: Program,
    locals: []const []const Candidate,
    possible: [max_tasks]Selection,
    always: [max_tasks]Selection,
    options: SolveOptions,
    order: [max_tasks]usize = undefined,
    visits: usize = 0,
    best: ?State = null,

    fn initializeOrder(self: *Search) void {
        var touching = [_]usize{0} ** max_tasks;
        for (self.program.roots) |root| {
            const refs = self.program.taskReferences(root);
            for (self.program.tasks, 0..) |_, i| if (refs & taskBit(i) != 0) {
                touching[i] += 1;
            };
        }
        for (self.program.tasks, 0..) |_, i| self.order[i] = i;
        const Context = struct { search: *Search, counts: *const [max_tasks]usize };
        std.mem.sort(usize, self.order[0..self.locals.len], Context{ .search = self, .counts = &touching }, struct {
            fn less(context: Context, a: usize, b: usize) bool {
                if (context.counts[a] != context.counts[b]) return context.counts[a] > context.counts[b];
                const locals = context.search.locals;
                if (locals[a].len != locals[b].len) return locals[a].len < locals[b].len;
                return std.mem.lessThan(u8, context.search.program.tasks[a].name, context.search.program.tasks[b].name);
            }
        }.less);
    }

    fn better(self: *Search, a: State, b: State) bool {
        if (a.score != b.score) return a.score > b.score;
        for (self.order[0..self.locals.len]) |task| if (a.lexical[task] != b.lexical[task]) return lexicalLess(a.lexical[task], b.lexical[task]);
        return false;
    }

    fn admissible(self: *Search, state: State, decided: usize) !bool {
        var possible = self.possible;
        var selected = self.always;
        for (self.order[0..decided]) |i| {
            possible[i] = state.selected[i];
            selected[i] = state.selected[i];
        }
        return (try self.program.evaluate(.{ .selected = selected[0..self.locals.len], .possible = possible[0..self.locals.len] })) != .no;
    }
    fn consider(self: *Search, state: State, prefer_lexical_tie: bool) !void {
        if ((try self.program.evaluate(.{ .selected = state.selected[0..self.locals.len], .possible = state.selected[0..self.locals.len] })) != .yes) return;
        if (self.best == null or state.score > self.best.?.score or (prefer_lexical_tie and self.better(state, self.best.?))) self.best = state;
    }
    fn dfs(self: *Search, depth: usize, state: *State) anyerror!void {
        // Preserve the first utility-descending DFS solution at equal score,
        // matching upstream exact decoding; beam uses lexical signature ties.
        if (depth == self.locals.len) return self.consider(state.*, false);
        const task_index = self.order[depth];
        for (self.locals[task_index]) |candidate| {
            // Count attempted branches, including those rejected by constraints.
            if (self.visits >= self.options.exact_node_budget) return error.ConstraintSearchExhausted;
            self.visits += 1;
            if (self.visits % 128 == 1) try self.options.check();
            const old_score = state.score;
            state.selected[task_index] = candidate.selected;
            state.lexical[task_index] = candidate.lexical;
            state.score = old_score + candidate.utility;
            if (!std.math.isFinite(state.score)) return error.InvalidClassificationLogits;
            var upper = state.score;
            for (self.order[depth + 1 .. self.locals.len]) |remaining| upper += self.locals[remaining][0].utility;
            if ((self.best == null or upper > self.best.?.score) and try self.admissible(state.*, depth + 1)) try self.dfs(depth + 1, state);
            state.score = old_score;
        }
    }
    fn beam(self: *Search, alloc: std.mem.Allocator) !BeamOutcome {
        var current = try alloc.alloc(State, self.options.beam_width);
        var next = try alloc.alloc(State, self.options.beam_width);
        current[0] = .{};
        var current_len: usize = 1;
        var visits: usize = 0;
        var complete = true;
        for (0..self.locals.len) |depth| {
            const task_index = self.order[depth];
            var next_len: usize = 0;
            for (current[0..current_len]) |previous| for (self.locals[task_index]) |candidate| {
                if (visits >= self.options.beam_node_budget) return .exhausted;
                visits += 1;
                self.visits += 1;
                if (visits % 128 == 1) try self.options.check();
                var state = previous;
                state.selected[task_index] = candidate.selected;
                state.lexical[task_index] = candidate.lexical;
                state.score += candidate.utility;
                if (!std.math.isFinite(state.score)) return error.InvalidClassificationLogits;
                if (!try self.admissible(state, depth + 1)) continue;
                if (depth + 1 == self.locals.len) try self.consider(state, true);
                var insert: usize = 0;
                while (insert < next_len and !self.better(state, next[insert])) : (insert += 1) {}
                if (next_len == next.len) complete = false;
                if (insert >= next.len) continue;
                const new_len = @min(next.len, next_len + 1);
                var i = new_len - 1;
                while (i > insert) : (i -= 1) next[i] = next[i - 1];
                next[insert] = state;
                next_len = new_len;
            };
            std.mem.swap([]State, &current, &next);
            current_len = next_len;
            if (current_len == 0) return if (complete) .complete else .pruned;
        }
        if (self.locals.len == 0) try self.consider(.{}, true);
        return if (complete) .complete else .pruned;
    }
};
fn lexicalLess(a: Selection, b: Selection) bool {
    var left = a;
    var right = b;
    while (left != 0 and right != 0) {
        const l = @ctz(left);
        const r = @ctz(right);
        if (l != r) return l < r;
        left &= left - 1;
        right &= right - 1;
    }
    return left == 0 and right != 0;
}

test "constraint AST preserves Kleene truth and refuses unknown references" {
    const tasks = [_]Task{.{ .name = "topic", .labels = &.{ "a", "b" } }};
    var compiled = try compile(std.testing.allocator, &tasks, "[{\"type\":\"Implies\",\"cond\":{\"type\":\"LabelRef\",\"task\":\"topic\",\"label\":\"a\"},\"then\":{\"type\":\"Not\",\"child\":{\"type\":\"LabelRef\",\"task\":\"topic\",\"label\":\"b\"}}}]", .{});
    defer compiled.deinit();
    try std.testing.expectEqual(Truth.unknown, try compiled.program.evaluate(.{ .selected = &.{0}, .possible = &.{3} }));
    try std.testing.expectEqual(Truth.no, try compiled.program.evaluate(.{ .selected = &.{3}, .possible = &.{3} }));
    try std.testing.expectEqual(Truth.yes, try compiled.program.evaluate(.{ .selected = &.{1}, .possible = &.{1} }));
    try std.testing.expectError(error.UnknownConstraintLabel, compile(std.testing.allocator, &tasks, "[{\"type\":\"LabelRef\",\"task\":\"topic\",\"label\":\"missing\"}]", .{}));
}

test "ordinal predicates honor committed selections and remaining alternatives" {
    const tasks = [_]Task{.{ .name = "level", .labels = &.{ "low", "medium", "high" }, .min_labels = 1, .max_labels = 1, .ordered = true }};
    const cases = .{
        .{ "MinLevel", [_]Truth{ .no, .yes, .yes }, Truth.unknown },
        .{ "MaxLevel", [_]Truth{ .yes, .yes, .no }, Truth.unknown },
        .{ "AtLevel", [_]Truth{ .no, .yes, .no }, Truth.unknown },
    };
    inline for (cases) |case| {
        const json = "[{\"type\":\"" ++ case[0] ++ "\",\"task\":\"level\",\"level\":\"medium\"}]";
        var compiled = try compile(std.testing.allocator, &tasks, json, .{});
        defer compiled.deinit();
        for (0..3) |level| {
            const committed = [_]Selection{bit(level)};
            try std.testing.expectEqual(case[1][level], try compiled.program.evaluate(.{ .selected = &committed, .possible = &.{0} }));
            try std.testing.expectEqual(case[1][level], try compiled.program.evaluate(.{ .selected = &committed, .possible = &committed }));
        }
        try std.testing.expectEqual(case[2], try compiled.program.evaluate(.{ .selected = &.{0}, .possible = &.{7} }));
        try std.testing.expectEqual(Truth.no, try compiled.program.evaluate(.{ .selected = &.{0}, .possible = &.{0} }));
    }
    var invalid = tasks[0];
    invalid.threshold = 0;
    try std.testing.expectError(error.InvalidClassificationCalibration, validateTask(invalid));
    invalid.threshold = 1;
    try std.testing.expectError(error.InvalidClassificationCalibration, validateTask(invalid));
}

test "constraint ties match alphabetical label order and cancellation propagates" {
    const tasks = [_]Task{.{ .name = "topic", .labels = &.{ "z", "a" }, .min_labels = 1, .max_labels = 1 }};
    var compiled = try compile(std.testing.allocator, &tasks, "[]", .{});
    defer compiled.deinit();
    var result = try solve(std.testing.allocator, compiled.program, &.{&.{ 0, 0 }}, .{});
    defer result.deinit();
    try std.testing.expectEqual(Status.optimal, result.status);
    try std.testing.expectEqual(@as(Selection, 2), result.selections[0]);
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, solve(std.testing.allocator, compiled.program, &.{&.{ 0, 0 }}, .{ .check_fn = Cancel.check }));
}

test "constraint exact search matches exhaustive cross task assignments" {
    const tasks = [_]Task{
        .{ .name = "a", .labels = &.{ "x", "y", "z" }, .max_labels = 2 },
        .{ .name = "b", .labels = &.{ "n", "p" }, .min_labels = 1, .max_labels = 1 },
    };
    var compiled = try compile(std.testing.allocator, &tasks, "[{\"type\":\"Iff\",\"left\":{\"type\":\"Cardinality\",\"task\":\"a\",\"minimum\":2,\"maximum\":2},\"right\":{\"type\":\"LabelRef\",\"task\":\"b\",\"label\":\"p\"}}]", .{});
    defer compiled.deinit();
    const scores = [_][]const f64{ &.{ 0.4, -0.2, 1.1 }, &.{ 0.8, 0.1 } };
    var best = -std.math.inf(f64);
    for (0..8) |a| for (0..4) |b| {
        if (@popCount(a) > 2 or @popCount(b) != 1) continue;
        const masks = [_]Selection{ a, b };
        if ((try compiled.program.evaluate(.{ .selected = &masks, .possible = &masks })) != .yes) continue;
        var score: f64 = 0;
        for (scores, 0..) |values, t| for (values, 0..) |value, l| {
            if (masks[t] & bit(l) != 0) score += value;
        };
        best = @max(best, score);
    };
    var result = try solve(std.testing.allocator, compiled.program, &scores, .{ .algorithm = .exact });
    defer result.deinit();
    try std.testing.expectEqual(Status.optimal, result.status);
    try std.testing.expectApproxEqAbs(best, result.utility, 1e-12);
}

test "constraint beam exhaustion never certifies infeasibility" {
    const tasks = [_]Task{.{ .name = "a", .labels = &.{ "x", "y" }, .min_labels = 1, .max_labels = 1 }};
    var compiled = try compile(std.testing.allocator, &tasks, "[]", .{});
    defer compiled.deinit();
    var limited = try solve(std.testing.allocator, compiled.program, &.{&.{ 1, 2 }}, .{ .algorithm = .exact, .exact_node_budget = 0 });
    defer limited.deinit();
    try std.testing.expectEqual(Status.search_exhausted, limited.status);
    try std.testing.expect(limited.exhausted);
    var beam = try solve(std.testing.allocator, compiled.program, &.{&.{ 1, 2 }}, .{ .algorithm = .beam, .beam_width = 1 });
    defer beam.deinit();
    try std.testing.expect(beam.valid());
    try std.testing.expectEqual(@as(Selection, 2), beam.selections[0]);
    try std.testing.expectEqual(Status.feasible, beam.status);
    try std.testing.expect(!beam.exhausted);
    var witness = try solve(std.testing.allocator, compiled.program, &.{&.{ 1, 2 }}, .{ .algorithm = .exact, .exact_node_budget = 1 });
    defer witness.deinit();
    try std.testing.expect(witness.valid());
    try std.testing.expect(witness.exhausted);
}

test "constraint defaults cannot coexist with other labels and infeasibility is explicit" {
    const tasks = [_]Task{.{ .name = "a", .labels = &.{ "none", "x" }, .min_labels = 1, .default_label = 0 }};
    var compiled = try compile(std.testing.allocator, &tasks, "[]", .{});
    defer compiled.deinit();
    var result = try solve(std.testing.allocator, compiled.program, &.{&.{ 1, 2 }}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(Selection, 2), result.selections[0]);
    var impossible = try compile(std.testing.allocator, &tasks, "[{\"type\":\"And\",\"children\":[{\"type\":\"LabelRef\",\"task\":\"a\",\"label\":\"none\"},{\"type\":\"LabelRef\",\"task\":\"a\",\"label\":\"x\"}]}]", .{});
    defer impossible.deinit();
    var failed = try solve(std.testing.allocator, impossible.program, &.{&.{ 1, 2 }}, .{});
    defer failed.deinit();
    try std.testing.expectEqual(Status.infeasible, failed.status);
}
