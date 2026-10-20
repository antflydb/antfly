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
pub const LiteralSet = @import("value_literals.zig").Set;
pub const membership = @import("value_membership.zig");
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

    source_read_ns: std.atomic.Value(u64) = .init(0),
    source_parse_ns: std.atomic.Value(u64) = .init(0),
    source_batches: std.atomic.Value(u64) = .init(0),
    source_scalar_reads: std.atomic.Value(u64) = .init(0),
    projected_sources: std.atomic.Value(u64) = .init(0),
    stored_field_sources: std.atomic.Value(u64) = .init(0),
    batch_refusals: std.atomic.Value(u64) = .init(0),
    batch_peak_bytes: std.atomic.Value(usize) = .init(0),
    sources: Sources = .{},
    prepared_leaves: std.atomic.Value(u32) = .init(0),

    mutex: std.atomic.Mutex = .unlocked,
    plans: std.ArrayListUnmanaged(Plan) = .empty,
    pub fn registerLeaf(self: *Work) void {
        if (self.prepared_leaves.fetchAdd(1, .monotonic) != 0) self.sources.enabled.store(true, .release);
    }
    pub fn record(self: *Work, a: A, spec: Spec, candidate: ?[]const u8, index: ?[]const u8, reason: ?[]const u8, verification: []const u8) !void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const field = try a.dupe(u8, spec.field);
        errdefer a.free(field);
        const physical = if (candidate) |name| try a.dupe(u8, name) else null;
        errdefer if (physical) |name| a.free(name);
        const index_name = if (index) |name| try a.dupe(u8, name) else null;
        errdefer if (index_name) |name| a.free(name);
        try self.plans.append(a, .{ .operation = spec.kind, .field = field, .candidate_field = physical, .index = index_name, .fallback_reason = reason, .verification = verification });
    }
    pub fn deinit(self: *Work, a: A) void {
        self.sources.deinit();
        for (self.plans.items) |plan| {
            a.free(plan.field);
            if (plan.candidate_field) |name| a.free(name);
            if (plan.index) |name| a.free(name);
        }
        self.plans.deinit(a);
    }
    pub fn profileAlloc(self: *Work, a: A) !?[]u8 {
        if (self.plans.items.len == 0) return null;
        return try std.json.Stringify.valueAlloc(a, .{ .documents = self.documents.load(.monotonic), .bytes = self.bytes.load(.monotonic), .plans = self.plans.items, .verification_metrics_version = @as(u32, 1), .source_read_us = self.source_read_ns.load(.monotonic) / 1000, .source_parse_us = self.source_parse_ns.load(.monotonic) / 1000, .source_batches = self.source_batches.load(.monotonic), .source_scalar_reads = self.source_scalar_reads.load(.monotonic), .projected_sources = self.projected_sources.load(.monotonic), .stored_field_sources = self.stored_field_sources.load(.monotonic), .batch_refusals = self.batch_refusals.load(.monotonic), .batch_peak_bytes = self.batch_peak_bytes.load(.monotonic) }, .{});
    }

    pub fn charge(counter: *std.atomic.Value(u64), amount: u64, maximum: u64) !void {
        var current = counter.load(.monotonic);
        while (true) {
            if (amount > maximum -| current) return error.ValueSearchLimitExceeded;
            if (counter.cmpxchgWeak(current, current + amount, .monotonic, .monotonic)) |changed| current = changed else return;
        }
    }
};

/// Request-owned decoded sources shared by value clauses. No eviction means a
/// borrowed Value remains stable until Work.deinit; refusal simply uses scratch.
/// Actual allocator capacity, including parser arenas and map keys, is bounded.
pub const Sources = struct {
    const Entry = std.json.Parsed(std.json.Value);
    enabled: std.atomic.Value(bool) = .init(false),
    populated: std.atomic.Value(bool) = .init(false),
    budget: @import("../sql/memory_budget.zig") = .{ .backing = undefined, .limit = 8 * 1024 * 1024 },
    initialized: bool = false,
    context: ?*anyopaque = null,
    entries: std.StringHashMapUnmanaged(Entry) = .empty,
    mutex: std.atomic.Mutex = .unlocked,
    fn lock(self: *Sources) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
    }
    pub fn get(self: *Sources, context: ?*anyopaque, id: []const u8) ?std.json.Value {
        if (!self.enabled.load(.acquire) or !self.populated.load(.acquire)) return null;
        self.lock();
        defer self.mutex.unlock();
        if (!self.initialized or self.context != context) return null;
        return if (self.entries.get(id)) |entry| entry.value else null;
    }
    pub fn remember(self: *Sources, a: A, context: ?*anyopaque, id: []const u8, bytes: []const u8) !?std.json.Value {
        if (!self.enabled.load(.acquire)) return null;
        self.lock();
        defer self.mutex.unlock();
        if (!self.initialized) {
            self.budget.backing = @import("../sql/memory_budget.zig").reclaimingAllocator(a);
            self.context = context;
            self.initialized = true;
        }
        if (self.context != context) return null;
        if (self.entries.get(id)) |entry| return entry.value;
        if (self.entries.count() >= 4096 or bytes.len > (self.budget.limit - self.budget.live) / 4) return null;
        const ca = self.budget.allocator();
        var parsed = std.json.parseFromSlice(std.json.Value, ca, bytes, .{ .allocate = .alloc_always }) catch |err| switch (err) {
            error.OutOfMemory => return null,
            else => return err,
        };
        const owned = ca.dupe(u8, id) catch {
            parsed.deinit();
            return null;
        };
        self.entries.put(ca, owned, parsed) catch {
            ca.free(owned);
            parsed.deinit();
            return null;
        };
        self.populated.store(true, .release);
        return parsed.value;
    }
    fn deinit(self: *Sources) void {
        if (!self.initialized) return;
        const ca = self.budget.allocator();
        var it = self.entries.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit();
            ca.free(entry.key_ptr.*);
        }
        self.entries.deinit(ca);
        std.debug.assert(self.budget.live == 0);
    }
};

test "logical value decoded sources are owned bounded and loader scoped" {
    const a = std.testing.allocator;
    var sources: Sources = .{ .enabled = .init(true) };
    defer sources.deinit();
    const bytes = try a.dupe(u8, "{\"body\":\"needle\"}");
    _ = (try sources.remember(a, null, "doc", bytes)).?;
    a.free(bytes);
    try std.testing.expectEqualStrings("needle", sources.get(null, "doc").?.object.get("body").?.string);
    var other: u8 = 0;
    try std.testing.expect(sources.get(&other, "doc") == null);
    try std.testing.expect(try sources.remember(a, &other, "doc", "{}") == null);
    sources.budget.limit = sources.budget.live;
    try std.testing.expect(try sources.remember(a, null, "new", "{}") == null);
    try std.testing.expect(sources.get(null, "doc") != null);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var scoped: Sources = .{ .enabled = .init(true) };
    defer scoped.deinit();
    _ = try scoped.remember(arena.allocator(), null, "owned", "{\"body\":\"needle\"}");
    try std.testing.expectEqual(@as(usize, 0), arena.queryCapacity());
}

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
    pub fn sourcePlan(self: *Tree, a: A) !@import("value_source.zig").Plan {
        var fields: [64][]const u8 = undefined;
        var count: usize = 0;
        try self.collectFields(&fields, &count);
        return @import("value_source.zig").Plan.init(a, fields[0..count]);
    }
    fn collectFields(self: *Tree, fields: *[64][]const u8, count: *usize) anyerror!void {
        switch (self.*) {
            .leaf => |matcher| {
                if (count.* == fields.len) return error.ProjectionTooLarge;
                fields[count.*] = matcher.spec.field;
                count.* += 1;
            },
            .boolean => |b| {
                for (b.must) |*child| try child.collectFields(fields, count);
                for (b.should) |*child| try child.collectFields(fields, count);
                for (b.must_not) |*child| try child.collectFields(fields, count);
            },
            else => {},
        }
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
    const Counters = struct {
        source_read_us: u64 = 0,
        source_parse_us: u64 = 0,
        source_batches: u64 = 0,
        source_scalar_reads: u64 = 0,
        projected_sources: u64 = 0,
        stored_field_sources: u64 = 0,
        batch_refusals: u64 = 0,
        batch_peak_bytes: u64 = 0,
    };
    var counters: Counters = .{};
    var metrics_complete = true;
    var present = false;
    for (results) |result| if (result.value_search_profile_json) |json| {
        const parsed = try std.json.parseFromSlice(std.json.Value, arena.allocator(), json, .{});
        const object = parsed.value.object;
        const version = object.get("verification_metrics_version");
        metrics_complete = metrics_complete and version != null and version.? == .integer and version.?.integer == 1;
        inline for (comptime std.meta.fieldNames(Counters)) |name| if (object.get(name)) |value| {
            if (value != .integer or value.integer < 0) return error.InvalidRemoteResponse;
            const amount: u64 = @intCast(value.integer);
            @field(counters, name) = if (comptime std.mem.eql(u8, name, "batch_peak_bytes")) @max(@field(counters, name), amount) else try std.math.add(u64, @field(counters, name), amount);
        } else {
            // Supplementary counter added after metrics v1; old producers can
            // still attest complete v1 read/parse/window measurements.
            if (comptime !std.mem.eql(u8, name, "stored_field_sources")) metrics_complete = false;
        };
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
    return try std.json.Stringify.valueAlloc(a, .{ .documents = documents, .bytes = bytes, .plans = plans.items, .verification_metrics_version = @as(u32, if (metrics_complete) 1 else 0), .source_read_us = counters.source_read_us, .source_parse_us = counters.source_parse_us, .source_batches = counters.source_batches, .source_scalar_reads = counters.source_scalar_reads, .projected_sources = counters.projected_sources, .stored_field_sources = counters.stored_field_sources, .batch_refusals = counters.batch_refusals, .batch_peak_bytes = counters.batch_peak_bytes }, .{});
}

test "logical value verification telemetry survives shard merging and marks legacy coverage" {
    const a = std.testing.allocator;
    const Row = struct { value_search_profile_json: ?[]const u8 };
    const current = "{\"documents\":2,\"bytes\":10,\"plans\":[],\"verification_metrics_version\":1,\"source_read_us\":7,\"source_parse_us\":3,\"source_batches\":1,\"source_scalar_reads\":0,\"projected_sources\":2,\"batch_refusals\":0,\"batch_peak_bytes\":4096}";
    const legacy = "{\"documents\":2,\"bytes\":10,\"plans\":[]}";
    for ([_][]const u8{ current, legacy }, 0..) |second, i| {
        const merged = (try mergeProfilesAlloc(a, &[_]Row{ .{ .value_search_profile_json = current }, .{ .value_search_profile_json = second } })).?;
        defer a.free(merged);
        var parsed = try std.json.parseFromSlice(std.json.Value, a, merged, .{});
        defer parsed.deinit();
        const object = parsed.value.object;
        try std.testing.expectEqual(@as(i64, 4), object.get("documents").?.integer);
        try std.testing.expectEqual(@as(i64, if (i == 0) 1 else 0), object.get("verification_metrics_version").?.integer);
        try std.testing.expectEqual(@as(i64, if (i == 0) 14 else 7), object.get("source_read_us").?.integer);
        try std.testing.expectEqual(@as(i64, 4096), object.get("batch_peak_bytes").?.integer);
    }
}
