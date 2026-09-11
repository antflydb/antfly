// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded selection of complete record alternatives. Identity is a mutex;
//! resources are exclusive across selected records. This is the versioned
//! native long-document extension, not an upstream record-assignment oracle.
const std = @import("std");
const Allocator = std.mem.Allocator;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Algorithm = @import("extraction_constraints.zig").Algorithm;

pub const Candidate = struct {
    identity: usize,
    resources: []const usize,
    score: f64,
};
pub const Options = struct {
    algorithm: Algorithm = .auto,
    max_candidates: usize = 4096,
    max_resource_references: usize = 131072,
    beam_width: usize = 64,
    exact_node_budget: usize = 200000,
    beam_node_budget: usize = 200000,
    max_work: usize = 10000000,
    control: ?Control = null,
};
pub const Status = enum { optimal, feasible };
pub const Result = struct {
    allocator: Allocator,
    selected: []usize,
    status: Status,
    utility: f64,
    visited_nodes: usize,
    exhausted: bool,
    work_steps: usize,
    pub fn deinit(self: *Result) void {
        self.allocator.free(self.selected);
        self.* = undefined;
    }
};
const Word = u64;
const bits = @bitSizeOf(Word);
fn has(mask: []const Word, index: usize) bool {
    return mask[index / bits] & (@as(Word, 1) << @intCast(index % bits)) != 0;
}
fn set(mask: []Word, index: usize) void {
    mask[index / bits] |= @as(Word, 1) << @intCast(index % bits);
}
fn clear(mask: []Word, index: usize) void {
    mask[index / bits] &= ~(@as(Word, 1) << @intCast(index % bits));
}
const Work = struct {
    options: Options,
    steps: usize = 0,
    fn tick(self: *Work) !void {
        if (self.steps >= self.options.max_work) return error.LongDocumentWorkLimitExceeded;
        self.steps += 1;
        if (self.steps % 64 == 1) if (self.options.control) |control| try control.check();
    }
};
const Problem = struct {
    candidates: []const Candidate,
    words: usize,
    conflicts: []Word,
    groups: []usize,
    group_count: usize,
    fn row(self: Problem, index: usize) []const Word {
        return self.conflicts[index * self.words ..][0..self.words];
    }
};
const State = struct {
    available: []Word,
    selected: []Word,
    score: f64 = 0,
    count: usize = 0,
    upper: f64 = 0,
};
const Search = struct {
    problem: Problem,
    work: *Work,
    best: []Word,
    best_score: f64 = 0,
    best_count: usize = 0,
    visits: usize = 0,
    group_scores: []f64,
    group_seen: []bool,

    fn better(self: *const Search, state: State) bool {
        if (state.score != self.best_score) return state.score > self.best_score;
        if (state.count != self.best_count) return state.count > self.best_count;
        return lexical(state.selected, self.best, self.problem.candidates.len);
    }
    fn consider(self: *Search, state: State) void {
        if (self.better(state)) {
            @memcpy(self.best, state.selected);
            self.best_score = state.score;
            self.best_count = state.count;
        }
    }
    fn bound(self: *Search, state: State) !struct { score: f64, count: usize } {
        @memset(self.group_seen, false);
        for (self.problem.candidates, 0..) |candidate, index| {
            try self.work.tick();
            if (!has(state.available, index)) continue;
            const group = self.problem.groups[index];
            if (!self.group_seen[group] or candidate.score > self.group_scores[group]) self.group_scores[group] = candidate.score;
            self.group_seen[group] = true;
        }
        var score = state.score;
        var count = state.count;
        for (self.group_seen, self.group_scores) |seen, value| if (seen) {
            score += value;
            count += 1;
        };
        return .{ .score = score, .count = count };
    }
    fn branch(self: *Search, parent: State, child: *State, candidate: usize, include: bool) !void {
        @memcpy(child.available, parent.available);
        @memcpy(child.selected, parent.selected);
        clear(child.available, candidate);
        child.score = parent.score;
        child.count = parent.count;
        if (include) {
            set(child.selected, candidate);
            child.score += self.problem.candidates[candidate].score;
            child.count += 1;
            for (child.available, self.problem.row(candidate)) |*available, conflicts| {
                try self.work.tick();
                available.* &= ~conflicts;
            }
        }
    }
    fn greedy(self: *Search, state: *State) !void {
        for (self.problem.candidates, 0..) |candidate, index| {
            try self.work.tick();
            if (!has(state.available, index)) continue;
            set(state.selected, index);
            state.score += candidate.score;
            state.count += 1;
            clear(state.available, index);
            for (state.available, self.problem.row(index)) |*available, conflicts| available.* &= ~conflicts;
        }
        self.consider(state.*);
    }
};
fn lexical(left: []const Word, right: []const Word, count: usize) bool {
    // Candidate order is an explicit caller contract: earlier retained
    // source/ownership alternatives win otherwise identical objectives.
    for (0..count) |index| if (has(left, index) != has(right, index)) return has(left, index);
    return false;
}
fn first(mask: []const Word) ?usize {
    for (mask, 0..) |word, index| if (word != 0) return index * bits + @ctz(word);
    return null;
}
fn makeState(a: Allocator, words: usize) !State {
    const masks = try a.alloc(Word, 2 * words);
    @memset(masks, 0);
    return .{ .available = masks[0..words], .selected = masks[words..] };
}
fn full(mask: []Word, count: usize) void {
    @memset(mask, std.math.maxInt(Word));
    if (count % bits != 0) mask[mask.len - 1] = (@as(Word, 1) << @intCast(count % bits)) - 1;
}
fn initialize(a: Allocator, candidates: []const Candidate, work: *Work) !Problem {
    const words = (candidates.len + bits - 1) / bits;
    const conflicts = try a.alloc(Word, try std.math.mul(usize, candidates.len, words));
    @memset(conflicts, 0);
    const groups = try a.alloc(usize, candidates.len);
    var identities = std.AutoHashMapUnmanaged(usize, std.ArrayListUnmanaged(usize)).empty;
    var resources = std.AutoHashMapUnmanaged(usize, std.ArrayListUnmanaged(usize)).empty;
    var group_ids = std.AutoHashMapUnmanaged(usize, usize).empty;
    var references: usize = 0;
    for (candidates, 0..) |candidate, index| {
        try work.tick();
        if (!std.math.isFinite(candidate.score) or candidate.score < 0 or candidate.score > 1) return error.InvalidLongDocumentRecordScore;
        if (candidate.resources.len > work.options.max_resource_references - references) return error.LongDocumentCandidateLimitExceeded;
        references += candidate.resources.len;
        const group = try group_ids.getOrPut(a, candidate.identity);
        if (!group.found_existing) group.value_ptr.* = group_ids.count() - 1;
        groups[index] = group.value_ptr.*;
        const previous = try identities.getOrPut(a, candidate.identity);
        if (!previous.found_existing) previous.value_ptr.* = .empty;
        for (previous.value_ptr.items) |other| {
            try work.tick();
            set(conflicts[index * words ..][0..words], other);
            set(conflicts[other * words ..][0..words], index);
        }
        try previous.value_ptr.append(a, index);
        for (candidate.resources) |resource| {
            try work.tick();
            const owners = try resources.getOrPut(a, resource);
            if (!owners.found_existing) owners.value_ptr.* = .empty;
            // Duplicate values inside one complete record consume one global
            // resource; they do not create an artificial self-conflict.
            if (owners.value_ptr.items.len > 0 and owners.value_ptr.items[owners.value_ptr.items.len - 1] == index) continue;
            for (owners.value_ptr.items) |other| {
                try work.tick();
                set(conflicts[index * words ..][0..words], other);
                set(conflicts[other * words ..][0..words], index);
            }
            try owners.value_ptr.append(a, index);
        }
    }
    return .{ .candidates = candidates, .words = words, .conflicts = conflicts, .groups = groups, .group_count = group_ids.count() };
}

fn exact(a: Allocator, search: *Search, budget: usize) !bool {
    const Frame = struct { state: State, phase: enum { enter, exclude, done } = .enter, candidate: usize = 0 };
    const n = search.problem.candidates.len;
    const frames = try a.alloc(Frame, n + 1);
    const masks = try a.alloc(Word, try std.math.mul(usize, (n + 1) * 2, search.problem.words));
    for (frames, 0..) |*frame, index| {
        const pair = masks[index * 2 * search.problem.words ..][0 .. 2 * search.problem.words];
        frame.* = .{ .state = .{ .available = pair[0..search.problem.words], .selected = pair[search.problem.words..] } };
    }
    full(frames[0].state.available, n);
    @memset(frames[0].state.selected, 0);
    var depth: usize = 1;
    var visits: usize = 0;
    while (depth > 0) {
        try search.work.tick();
        const frame = &frames[depth - 1];
        switch (frame.phase) {
            .enter => {
                if (visits >= budget) return false;
                visits += 1;
                search.visits += 1;
                search.consider(frame.state);
                const bound = try search.bound(frame.state);
                const candidate = first(frame.state.available);
                if (candidate == null or bound.score < search.best_score or (bound.score == search.best_score and bound.count < search.best_count)) {
                    depth -= 1;
                    continue;
                }
                frame.candidate = candidate.?;
                frame.phase = .exclude;
                const child = &frames[depth];
                child.phase = .enter;
                try search.branch(frame.state, &child.state, candidate.?, true);
                depth += 1;
            },
            .exclude => {
                frame.phase = .done;
                const child = &frames[depth];
                child.phase = .enter;
                try search.branch(frame.state, &child.state, frame.candidate, false);
                depth += 1;
            },
            .done => depth -= 1,
        }
    }
    return true;
}

const BeamOutcome = enum { optimal, pruned, exhausted };
fn beam(a: Allocator, search: *Search, budget: usize, width: usize) !BeamOutcome {
    const current = try a.alloc(State, width);
    for (current) |*state| state.* = try makeState(a, search.problem.words);
    const children = try a.alloc(State, 2 * width);
    for (children) |*state| state.* = try makeState(a, search.problem.words);
    const order = try a.alloc(usize, children.len);
    full(current[0].available, search.problem.candidates.len);
    var current_count: usize = 1;
    var pruned = false;
    var visits: usize = 0;
    for (0..search.problem.candidates.len) |candidate| {
        var child_count: usize = 0;
        for (current[0..current_count]) |parent| {
            try search.work.tick();
            if (visits >= budget) return .exhausted;
            visits += 1;
            search.visits += 1;
            const include = has(parent.available, candidate);
            for (0..@as(usize, if (include) 2 else 1)) |choice| {
                const child = &children[child_count];
                try search.branch(parent, child, candidate, include and choice == 0);
                search.consider(child.*);
                const bound = try search.bound(child.*);
                if (bound.score < search.best_score or (bound.score == search.best_score and bound.count < search.best_count)) continue;
                child.upper = bound.score;
                order[child_count] = child_count;
                child_count += 1;
            }
        }
        if (child_count == 0) return if (pruned) .pruned else .optimal;
        const Context = struct { states: []const State, n: usize };
        const Compare = struct {
            fn less(context: Context, left: usize, right: usize) bool {
                const l = context.states[left];
                const r = context.states[right];
                if (l.upper != r.upper) return l.upper > r.upper;
                if (l.score != r.score) return l.score > r.score;
                if (l.count != r.count) return l.count > r.count;
                return lexical(l.selected, r.selected, context.n);
            }
        };
        std.mem.sort(usize, order[0..child_count], Context{ .states = children, .n = search.problem.candidates.len }, Compare.less);
        if (child_count > width) pruned = true;
        current_count = @min(child_count, width);
        for (current[0..current_count], order[0..current_count]) |*to, from| {
            @memcpy(to.available, children[from].available);
            @memcpy(to.selected, children[from].selected);
            to.score = children[from].score;
            to.count = children[from].count;
        }
    }
    return if (pruned) .pruned else .optimal;
}

/// Candidate order resolves exact objective/count ties. Selection never
/// modifies a candidate or combines fields. A valid empty set always exists;
/// final node-budget exhaustion is reported separately from feasibility.
pub fn solve(allocator: Allocator, candidates: []const Candidate, options: Options) !Result {
    if (options.control) |control| try control.check();
    if (options.max_candidates == 0 or options.max_candidates > 4096 or candidates.len > options.max_candidates or options.max_resource_references == 0 or options.max_resource_references > 1048576 or
        options.beam_width == 0 or options.beam_width > 1024 or options.max_work == 0 or options.max_work > 1000000000) return error.InvalidLongDocumentRecordSelectionLimits;
    var work = Work{ .options = options };
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const problem = try initialize(a, candidates, &work);
    const best = try a.alloc(Word, problem.words);
    @memset(best, 0);
    var search = Search{ .problem = problem, .work = &work, .best = best, .group_scores = try a.alloc(f64, problem.group_count), .group_seen = try a.alloc(bool, problem.group_count) };
    @memset(search.group_scores, 0);
    var initial = try makeState(a, problem.words);
    full(initial.available, candidates.len);
    const root_bound = try search.bound(initial);
    try search.greedy(&initial);
    var status: Status = .optimal;
    var exhausted = false;
    if (search.best_score != root_bound.score or search.best_count != root_bound.count) {
        var complete = false;
        if (options.algorithm != .beam) {
            // Each search phase releases its scratch before an automatic
            // fallback. Resident model/request allocations remain external.
            var exact_arena = std.heap.ArenaAllocator.init(allocator);
            defer exact_arena.deinit();
            complete = try exact(exact_arena.allocator(), &search, options.exact_node_budget);
        }
        if (options.algorithm == .beam or (options.algorithm == .auto and !complete)) {
            var beam_arena = std.heap.ArenaAllocator.init(allocator);
            defer beam_arena.deinit();
            switch (try beam(beam_arena.allocator(), &search, options.beam_node_budget, options.beam_width)) {
                .optimal => {},
                .pruned => status = .feasible,
                .exhausted => {
                    status = .feasible;
                    exhausted = true;
                },
            }
        } else if (!complete) {
            status = .feasible;
            exhausted = true;
        }
    }
    // Independent final validation checks every retained identity/resource
    // conflict. Search state is never trusted as the final contract.
    for (candidates, 0..) |_, index| {
        try work.tick();
        if (!has(search.best, index)) continue;
        for (search.best, problem.row(index)) |selected, conflicts| if (selected & conflicts != 0) return error.InvalidLongDocumentRecordSelection;
    }
    if (options.control) |control| try control.check();
    const selected = try allocator.alloc(usize, search.best_count);
    errdefer allocator.free(selected);
    var destination: usize = 0;
    for (candidates, 0..) |_, index| if (has(search.best, index)) {
        selected[destination] = index;
        destination += 1;
    };
    return .{ .allocator = allocator, .selected = selected, .status = status, .utility = search.best_score, .visited_nodes = search.visits, .exhausted = exhausted, .work_steps = work.steps };
}

fn validMask(candidates: []const Candidate, mask: u64) bool {
    for (candidates, 0..) |left, i| {
        if (mask & (@as(u64, 1) << @intCast(i)) == 0) continue;
        for (candidates[0..i], 0..) |right, j| {
            if (mask & (@as(u64, 1) << @intCast(j)) == 0) continue;
            if (left.identity == right.identity) return false;
            for (left.resources) |resource| if (std.mem.indexOfScalar(usize, right.resources, resource) != null) return false;
        }
    }
    return true;
}
fn maskScore(candidates: []const Candidate, mask: u64) f64 {
    var score: f64 = 0;
    for (candidates, 0..) |candidate, i| if (mask & (@as(u64, 1) << @intCast(i)) != 0) {
        score += candidate.score;
    };
    return score;
}
fn brute(candidates: []const Candidate) u64 {
    var best: u64 = 0;
    var score: f64 = 0;
    for (0..@as(usize, 1) << @intCast(candidates.len)) |raw| {
        const mask: u64 = @intCast(raw);
        if (!validMask(candidates, mask)) continue;
        const value = maskScore(candidates, mask);
        const count = @popCount(mask);
        const best_count = @popCount(best);
        const diff = mask ^ best;
        if (value > score or (value == score and (count > best_count or (count == best_count and diff != 0 and (mask & (@as(u64, 1) << @intCast(@ctz(diff)))) != 0)))) {
            best = mask;
            score = value;
        }
    }
    return best;
}
fn resultMask(result: Result) u64 {
    var mask: u64 = 0;
    for (result.selected) |index| mask |= @as(u64, 1) << @intCast(index);
    return mask;
}

test "gliner boundary record selection exact and complete beam match exhaustive tied graphs" {
    const a = std.testing.allocator;
    var seed: u64 = 0x23b6eadf81c029;
    for (0..128) |case| {
        const n = 2 + case % 7;
        var candidates: [8]Candidate = undefined;
        var resources: [8][5]usize = undefined;
        for (candidates[0..n], 0..) |*candidate, index| {
            seed = seed *% 6364136223846793005 +% 1442695040888963407;
            const count = (seed >> 40) % 4;
            for (resources[index][0..count], 0..) |*resource, r| resource.* = (seed >> @intCast(8 * r)) % 6;
            candidate.* = .{ .identity = (seed >> 32) % (n - 1), .score = @as(f64, @floatFromInt((seed >> 48) % 5)) / 4, .resources = resources[index][0..count] };
        }
        const expected = brute(candidates[0..n]);
        var exact_result = try solve(a, candidates[0..n], .{ .algorithm = .exact });
        defer exact_result.deinit();
        try std.testing.expectEqual(Status.optimal, exact_result.status);
        try std.testing.expect(!exact_result.exhausted);
        try std.testing.expectEqual(expected, resultMask(exact_result));
        try std.testing.expectEqual(maskScore(candidates[0..n], expected), exact_result.utility);
        var complete_beam = try solve(a, candidates[0..n], .{ .algorithm = .beam, .beam_width = 256 });
        defer complete_beam.deinit();
        try std.testing.expectEqual(Status.optimal, complete_beam.status);
        try std.testing.expectEqual(expected, resultMask(complete_beam));
        var narrow = try solve(a, candidates[0..n], .{ .algorithm = .beam, .beam_width = 1 });
        defer narrow.deinit();
        try std.testing.expect(!narrow.exhausted);
        try std.testing.expect(validMask(candidates[0..n], resultMask(narrow)));
        try std.testing.expect(narrow.utility <= exact_result.utility);
    }
}

test "gliner boundary record selection preserves alternatives and distinguishes exhaustion" {
    const a = std.testing.allocator;
    const candidates = [_]Candidate{
        .{ .identity = 0, .resources = &.{0}, .score = 0.9 },
        .{ .identity = 0, .resources = &.{1}, .score = 0.8 },
        .{ .identity = 1, .resources = &.{ 0, 0 }, .score = 0.85 },
    };
    var exact_result = try solve(a, &candidates, .{ .algorithm = .exact });
    defer exact_result.deinit();
    try std.testing.expectEqualSlices(usize, &.{ 1, 2 }, exact_result.selected);
    var exhausted = try solve(a, &candidates, .{ .algorithm = .exact, .exact_node_budget = 0 });
    defer exhausted.deinit();
    try std.testing.expectEqual(Status.feasible, exhausted.status);
    try std.testing.expect(exhausted.exhausted);
    try std.testing.expectEqualSlices(usize, &.{0}, exhausted.selected);
    var automatic = try solve(a, &candidates, .{ .algorithm = .auto, .exact_node_budget = 0 });
    defer automatic.deinit();
    try std.testing.expectEqual(Status.optimal, automatic.status);
    try std.testing.expect(!automatic.exhausted);
    try std.testing.expectEqualSlices(usize, &.{ 1, 2 }, automatic.selected);
    var bounded = try solve(a, &candidates, .{ .algorithm = .beam, .beam_width = 1 });
    defer bounded.deinit();
    try std.testing.expectEqual(Status.feasible, bounded.status);
    try std.testing.expect(!bounded.exhausted);
    try std.testing.expectEqualSlices(usize, &.{ 1, 2 }, bounded.selected);
    try std.testing.expectError(error.LongDocumentWorkLimitExceeded, solve(a, &candidates, .{ .max_work = 1 }));
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, solve(a, &candidates, .{ .control = .{ .ptr = null, .check_fn = Cancel.check } }));
    const Check = struct {
        fn run(allocator: Allocator, items: []const Candidate) !void {
            var result = try solve(allocator, items, .{ .exact_node_budget = 0 });
            defer result.deinit();
            try std.testing.expectEqualSlices(usize, &.{ 1, 2 }, result.selected);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Check.run, .{&candidates});
}
