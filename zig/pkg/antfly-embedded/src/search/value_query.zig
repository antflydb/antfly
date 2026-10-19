// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Logical string-value semantics, independent of emitted index names.
//! Plans use ordinary doc-number producers so verification participates in
//! boolean membership, counts, ordering and aggregation before pagination.
const std = @import("std");
const regex = @import("antfly_regex").search;
const A = std.mem.Allocator;
pub const Kind = enum { equals, contains, matches };
pub const Spec = struct {
    kind: Kind,
    field: []const u8,
    text: []const u8,
    case_sensitive: bool = true,
    boost: f32 = 1,
};
pub const Policy = struct {
    allow_scan: bool = false,
    max_documents: u64 = 100_000,
    max_bytes: u64 = 64 * 1024 * 1024,
    max_regex_work: u64 = 100_000_000,
};

/// One shared verification budget across scoring/filter/exclusion components.
/// Atomic reservations also cover providers that evaluate components concurrently.
pub const Plan = struct {
    operation: Kind,
    field: []const u8,
    candidate_field: ?[]const u8,
    index: ?[]const u8,
    verification: []const u8 = "original_value",
    fallback_reason: ?[]const u8,
};
pub const Work = struct {
    documents: std.atomic.Value(u64) = .init(0),
    bytes: std.atomic.Value(u64) = .init(0),
    regex_work: std.atomic.Value(u64) = .init(0),

    mutex: std.atomic.Mutex = .unlocked,
    plans: std.ArrayListUnmanaged(Plan) = .empty,
    pub fn record(self: *Work, a: A, spec: Spec, candidate: ?[]const u8, index: ?[]const u8, reason: ?[]const u8) !void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const field = try a.dupe(u8, spec.field);
        errdefer a.free(field);
        const physical = if (candidate) |name| try a.dupe(u8, name) else null;
        errdefer if (physical) |name| a.free(name);
        const index_name = if (index) |name| try a.dupe(u8, name) else null;
        errdefer if (index_name) |name| a.free(name);
        try self.plans.append(a, .{ .operation = spec.kind, .field = field, .candidate_field = physical, .index = index_name, .fallback_reason = reason });
    }
    pub fn deinit(self: *Work, a: A) void {
        for (self.plans.items) |plan| {
            a.free(plan.field);
            if (plan.candidate_field) |name| a.free(name);
            if (plan.index) |name| a.free(name);
        }
        self.plans.deinit(a);
    }
    pub fn profileAlloc(self: *Work, a: A) !?[]u8 {
        if (self.plans.items.len == 0) return null;
        return try std.json.Stringify.valueAlloc(a, .{ .documents = self.documents.load(.monotonic), .bytes = self.bytes.load(.monotonic), .plans = self.plans.items }, .{});
    }

    pub fn charge(counter: *std.atomic.Value(u64), amount: u64, maximum: u64) !void {
        var current = counter.load(.monotonic);
        while (true) {
            if (amount > maximum -| current) return error.ValueSearchLimitExceeded;
            if (counter.cmpxchgWeak(current, current + amount, .monotonic, .monotonic)) |changed| current = changed else return;
        }
    }
};

/// Prepared once per leaf, with span-only scratch reused across documents.
/// Sensitive literal operations never compile a regex or allocate VM scratch.
pub const Matcher = struct {
    spec: Spec,
    budget: @import("antfly_regex").Budget = undefined,
    work: ?*Work = null,
    policy: Policy = .{},
    pattern: ?regex.Pattern = null,
    runner: ?regex.Runner = null,

    pub fn init(self: *Matcher, a: A, spec: Spec) !void {
        self.* = .{ .spec = spec };
        if (spec.text.len > 64 * 1024) return error.ValueSearchLimitExceeded;
        if (spec.kind != .matches and spec.case_sensitive) return;
        var escaped = std.ArrayListUnmanaged(u8).empty;
        defer escaped.deinit(a);
        const source = if (spec.kind == .matches) spec.text else blk: {
            if (spec.kind == .equals) try escaped.appendSlice(a, "\\A(?:");
            for (spec.text) |byte| {
                if (std.mem.indexOfScalar(u8, "\\.^$|?*+()[]{}", byte) != null) try escaped.append(a, '\\');
                try escaped.append(a, byte);
            }
            if (spec.kind == .equals) try escaped.appendSlice(a, ")\\z");
            break :blk escaped.items;
        };
        self.budget = .{ .backing = a, .limit = 8 * 1024 * 1024 };
        const bounded = self.budget.allocator();
        self.pattern = regex.Pattern.init(bounded, source, .{ .insensitive = !spec.case_sensitive }) catch |err| switch (err) {
            error.InvalidRegex => return error.InvalidQueryRequest,
            error.UnsupportedRegex => return error.UnsupportedQueryRequest,
            error.RegexTooLarge => return error.ValueSearchLimitExceeded,
            error.OutOfMemory => return if (self.budget.denied) error.ValueSearchLimitExceeded else err,
        };
        errdefer self.pattern.?.deinit();
        if (self.pattern.?.states.len > 4096) return error.ValueSearchLimitExceeded;
        self.runner = regex.Runner.initMode(bounded, &self.pattern.?, .span) catch |err| {
            if (err == error.OutOfMemory and self.budget.denied) return error.ValueSearchLimitExceeded;
            return err;
        };
    }
    pub fn deinit(self: *Matcher) void {
        if (self.runner) |*r| r.deinit();
        if (self.pattern) |*p| p.deinit();
    }
    pub fn string(self: *Matcher, text: []const u8) !bool {
        if (self.runner) |*r| {
            const estimate = std.math.mul(u64, @intCast(text.len), @intCast(self.pattern.?.states.len)) catch return error.ValueSearchLimitExceeded;
            if (self.work) |work| try Work.charge(&work.regex_work, estimate, self.policy.max_regex_work) else if (estimate > self.policy.max_regex_work) return error.ValueSearchLimitExceeded;
            const found = r.find(text, 0) != null;
            if (r.failure) |err| {
                if (err == error.OutOfMemory and self.budget.denied) return error.ValueSearchLimitExceeded;
                return err;
            }
            return found;
        }
        return switch (self.spec.kind) {
            .equals => std.mem.eql(u8, text, self.spec.text),
            .contains => std.mem.indexOf(u8, text, self.spec.text) != null,
            .matches => unreachable,
        };
    }
    pub fn document(self: *Matcher, root: std.json.Value) !bool {
        var remaining: usize = 16_384;
        return self.at(root, self.spec.field, 0, &remaining);
    }
    fn at(self: *Matcher, value: std.json.Value, path: []const u8, depth: usize, remaining: *usize) anyerror!bool {
        if (depth >= 128 or remaining.* == 0) return error.ValueSearchLimitExceeded;
        remaining.* -= 1;
        if (value == .array) {
            for (value.array.items) |item| if (try self.at(item, path, depth + 1, remaining)) return true;
            return false;
        }
        if (path.len == 0) return value == .string and try self.string(value.string);
        if (value != .object) return false;
        if (std.mem.indexOfScalar(u8, path, '.')) |dot| {
            const child = value.object.get(path[0..dot]) orelse return false;
            return self.at(child, path[dot + 1 ..], depth + 1, remaining);
        }
        const child = value.object.get(path) orelse return false;
        return self.at(child, "", depth + 1, remaining);
    }
};

test "logical value matching preserves source punctuation arrays and Unicode folding" {
    const a = std.testing.allocator;
    var doc = try std.json.parseFromSlice(std.json.Value, a,
        \\{"content":["Connection-Refused",null,17,"Kelvin"],"nested":[{"name":"hello"},{"name":"world"}]}
    , .{});
    defer doc.deinit();
    const cases = [_]struct { spec: Spec, expected: bool }{
        .{ .spec = .{ .kind = .contains, .field = "content", .text = "tion-Ref" }, .expected = true },
        .{ .spec = .{ .kind = .contains, .field = "content", .text = "tion Ref" }, .expected = false },
        .{ .spec = .{ .kind = .equals, .field = "content", .text = "kelvin", .case_sensitive = false }, .expected = true },
        .{ .spec = .{ .kind = .equals, .field = "nested.name", .text = "world" }, .expected = true },
        .{ .spec = .{ .kind = .contains, .field = "content", .text = "RefusedK" }, .expected = false },
        .{ .spec = .{ .kind = .matches, .field = "content", .text = "^Connection-.*$" }, .expected = true },
        .{ .spec = .{ .kind = .equals, .field = "missing", .text = "" }, .expected = false },
    };
    for (cases) |case| {
        var matcher: Matcher = undefined;
        try matcher.init(a, case.spec);
        defer matcher.deinit();
        try std.testing.expectEqual(case.expected, try matcher.document(doc.value));
    }
}

/// Arena-owned source-only tree for primary scans when no text index exists.
/// Analyzed/indexed-term operations are deliberately not reinterpreted here.
pub const Tree = union(enum) {
    leaf: *Matcher,
    all: void,
    none: void,
    boolean: struct { must: []Tree, should: []Tree, must_not: []Tree, minimum: u32 },
    pub fn prepare(a: A, query: anytype) anyerror!Tree {
        return switch (query) {
            .value => |spec| blk: {
                const matcher = try a.create(Matcher);
                try matcher.init(a, spec);
                break :blk .{ .leaf = matcher };
            },
            .match_all => .all,
            .match_none => .none,
            .bool_query => |b| .{ .boolean = .{
                .must = try children(a, b.must),
                .should = try children(a, b.should),
                .must_not = try children(a, b.must_not),
                .minimum = if (b.should.len != 0 and b.must.len == 0 and !b.pure_should_optional) @max(1, b.min_should) else b.min_should,
            } },
            else => error.UnsupportedQueryRequest,
        };
    }
    fn children(a: A, queries: anytype) anyerror![]Tree {
        const out = try a.alloc(Tree, queries.len);
        for (queries, out) |query, *child| child.* = try prepare(a, query);
        return out;
    }
    pub fn bindWork(self: *Tree, work: *Work, policy: Policy) void {
        switch (self.*) {
            .leaf => |matcher| {
                matcher.work = work;
                matcher.policy = policy;
            },
            .boolean => |b| {
                for (b.must) |*child| child.bindWork(work, policy);
                for (b.should) |*child| child.bindWork(work, policy);
                for (b.must_not) |*child| child.bindWork(work, policy);
            },
            else => {},
        }
    }
    pub fn matches(self: *Tree, doc: std.json.Value) anyerror!bool {
        return switch (self.*) {
            .leaf => |matcher| matcher.document(doc),
            .all => true,
            .none => false,
            .boolean => |b| blk: {
                for (b.must) |*child| if (!try child.matches(doc)) break :blk false;
                for (b.must_not) |*child| if (try child.matches(doc)) break :blk false;
                var matching: u32 = 0;
                for (b.should) |*child| if (try child.matches(doc)) {
                    matching += 1;
                };
                break :blk matching >= b.minimum;
            },
        };
    }
};

/// Preserve per-shard executable plans and aggregate actual verification work.
pub fn mergeProfilesAlloc(a: A, results: anytype) !?[]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var plans = std.ArrayListUnmanaged(std.json.Value).empty;
    var documents: u64 = 0;
    var bytes: u64 = 0;
    var present = false;
    for (results) |result| if (result.value_search_profile_json) |json| {
        const parsed = try std.json.parseFromSlice(std.json.Value, arena.allocator(), json, .{});
        const object = parsed.value.object;
        const doc_count = object.get("documents") orelse return error.InvalidRemoteResponse;
        const byte_count = object.get("bytes") orelse return error.InvalidRemoteResponse;
        const physical_plans = object.get("plans") orelse return error.InvalidRemoteResponse;
        if (doc_count != .integer or doc_count.integer < 0 or byte_count != .integer or byte_count.integer < 0 or physical_plans != .array) return error.InvalidRemoteResponse;
        documents = try std.math.add(u64, documents, @intCast(doc_count.integer));
        bytes = try std.math.add(u64, bytes, @intCast(byte_count.integer));
        try plans.appendSlice(arena.allocator(), physical_plans.array.items);
        present = true;
    };
    if (!present) return null;
    return try std.json.Stringify.valueAlloc(a, .{ .documents = documents, .bytes = bytes, .plans = plans.items }, .{});
}
