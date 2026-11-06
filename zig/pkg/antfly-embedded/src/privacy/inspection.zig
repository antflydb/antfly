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

//! One inspection path for preview, backfill and admission. Providers supply
//! exact token/source geometry; neither a byte heuristic nor a truncated model
//! response can certify coverage. All output is owned by Result's arena.
const std = @import("std");
pub const contract = @import("contract.zig");
const A = std.mem.Allocator;
pub const Window = struct { start: usize, end: usize };
pub const Token = struct { start: usize, end: usize };
pub const Geometry = struct {
    tokens: []const Token,
    /// Exact composed schema/instruction/special-token overhead for this policy.
    overhead: usize,
    limit: usize,
};
pub const Input = struct { field: []const u8, text: []const u8, window: Window };
pub const Windowing = struct { max_windows: u32 = 64, overlap_tokens: u16 = 32 };
pub const Inspector = struct {
    windowing: Windowing = .{},
    ptr: *anyopaque,
    /// The adapter owns/releases tokenizer resources during this call. Returned
    /// geometry is allocated in the supplied request arena.
    tokenize: *const fn (*anyopaque, A, contract.Policy, []const u8) anyerror!Geometry,
    /// Exactly one JSON result per input in order. Both extractor entity output
    /// and constrained generator findings are normalized below. The adapter
    /// retains existing artifact memory, cancellation, timeout and retry controls.
    produce: *const fn (*anyopaque, A, contract.Policy, []const Input) anyerror![]const []const u8,
    check: *const fn (*anyopaque) anyerror!void,
};
pub const FieldResult = struct {
    field: []const u8,
    source_fingerprint: [64]u8,
    text: []const u8,
    findings: []const contract.Finding,
    receipt: contract.Receipt,
};
pub const Result = struct {
    arena: std.heap.ArenaAllocator,
    fields: []const FieldResult,
    policy_version: [64]u8,
    pub fn deinit(self: *Result) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn windows(a: A, text: []const u8, geometry: Geometry, execution: Windowing) ![]Window {
    if (!std.unicode.utf8ValidateSlice(text)) return error.InvalidPrivacySource;
    if (geometry.limit <= geometry.overhead or geometry.tokens.len == 0 and text.len != 0) return error.InvalidPrivacyTokenGeometry;
    const capacity = geometry.limit - geometry.overhead;
    if (capacity <= execution.overlap_tokens) return error.PrivacySchemaExceedsWindow;
    var last_end: usize = 0;
    var last_start: usize = 0;
    for (geometry.tokens) |token| {
        // Subword tokens may repeat/overlap byte ranges, but their end positions
        // must be monotonic. Boundaries must be usable directly on the source.
        if (token.start >= token.end or token.end > text.len or token.end < last_end or token.start < last_start or
            !contract.boundary(text, token.start) or !contract.boundary(text, token.end)) return error.InvalidPrivacyTokenGeometry;
        last_end = token.end;
        last_start = token.start;
    }
    var result: std.ArrayList(Window) = .empty;
    errdefer result.deinit(a);
    var start: usize = 0;
    while (true) {
        if (result.items.len >= execution.max_windows) return error.PrivacyWindowLimitExceeded;
        const end = @min(geometry.tokens.len, start + capacity);
        const first_byte = if (start == 0) 0 else geometry.tokens[start].start;
        const last_byte = if (end == geometry.tokens.len) text.len else geometry.tokens[end].start;
        if (last_byte <= first_byte and text.len != 0) return error.InvalidPrivacyTokenGeometry;
        try result.append(a, .{ .start = first_byte, .end = last_byte });
        if (end == geometry.tokens.len) break;
        const next = end - execution.overlap_tokens;
        if (next <= start) return error.InvalidPrivacyTokenGeometry;
        start = next;
    }
    return result.toOwnedSlice(a);
}

pub fn inspect(a: A, policy: contract.Policy, document: std.json.Value, inspector: Inspector) !Result {
    try policy.validate();
    if (document != .object) return error.InvalidPrivacyDocument;
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const scratch = arena.allocator();
    const version = try policy.identity(scratch);
    const categories = try policy.labels(scratch);
    const fields = try scratch.alloc(FieldResult, policy.fields.len);
    const revision = try policy.revision();
    for (policy.fields, fields) |field, *out| {
        try inspector.check(inspector.ptr);
        const value = try fieldValue(document, field);
        // Missing/null fields are inspected successfully as empty strings. A
        // non-string selected value is a failure, never silently unprotected.
        const source = switch (value) {
            .null => "",
            .string => |text| text,
            else => return error.InvalidPrivacyFieldType,
        };
        const plan = if (source.len == 0) try scratch.dupe(Window, &.{.{ .start = 0, .end = 0 }}) else try windows(scratch, source, try inspector.tokenize(inspector.ptr, scratch, policy, source), inspector.windowing);
        var findings: FindingSet = .{};
        defer findings.positions.deinit(scratch);
        var offset: usize = 0;
        while (offset < plan.len) {
            try inspector.check(inspector.ptr);
            const batch = plan[offset..@min(plan.len, offset + policy.execution.batch_items)];
            const inputs = try scratch.alloc(Input, batch.len);
            for (inputs, batch) |*input, window| {
                input.* = .{ .field = field, .text = source[window.start..window.end], .window = window };
                // Slicing can change token merges. Admit the actual composed
                // window as well as using source-token geometry for planning.
                if (input.text.len != 0) {
                    const geometry = try inspector.tokenize(inspector.ptr, scratch, policy, input.text);
                    if (geometry.overhead >= geometry.limit or geometry.tokens.len > geometry.limit - geometry.overhead) return error.PrivacyWindowTokenLimitExceeded;
                }
            }
            // Output validation is part of a bounded attempt. No successful
            // sibling leaks into a later retry; commit findings only as a batch.
            var attempt: u8 = 0;
            while (true) {
                try inspector.check(inspector.ptr);
                attempt += 1;
                var attempt_arena = std.heap.ArenaAllocator.init(scratch);
                defer attempt_arena.deinit();
                const attempt_alloc = attempt_arena.allocator();
                const accepted = inspectBatch(attempt_alloc, policy, categories, inputs, inspector) catch |err| {
                    if (err == error.Canceled or err == error.DeadlineExceeded or err == error.Timeout or attempt >= policy.execution.max_attempts) return err;
                    continue;
                };
                for (accepted) |finding| {
                    var owned = finding;
                    owned.field = try scratch.dupe(u8, finding.field);
                    owned.category = try scratch.dupe(u8, finding.category);
                    try findings.append(scratch, owned);
                }
                break;
            }
            offset += batch.len;
        }
        out.* = .{
            .field = try scratch.dupe(u8, field),
            .source_fingerprint = contract.fingerprint(source),
            .text = try contract.redact(scratch, source, findings.items.items, policy.replacement),
            .findings = findings.items.items,
            .receipt = .{ .policy_version = version, .source_fingerprint = contract.fingerprint(source), .producer_revision = try scratch.dupe(u8, revision), .inspected_windows = @intCast(plan.len), .required_windows = @intCast(plan.len), .state = .inspected },
        };
    }
    return .{ .arena = arena, .fields = fields, .policy_version = version };
}

fn inspectBatch(a: A, policy: contract.Policy, categories: []const contract.Category, inputs: []const Input, inspector: Inspector) ![]contract.Finding {
    const outputs = try inspector.produce(inspector.ptr, a, policy, inputs);
    if (outputs.len != inputs.len) return error.InvalidPrivacyOutputCardinality;
    var accepted: FindingSet = .{};
    defer accepted.positions.deinit(a);
    errdefer accepted.items.deinit(a);
    for (outputs, inputs) |raw, input| {
        var tree = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
        defer tree.deinit();
        const local = try normalize(a, input.field, input.text, tree.value, categories);
        for (local) |finding| {
            var rebased = finding;
            rebased.start = try std.math.add(usize, finding.start, input.window.start);
            rebased.end = try std.math.add(usize, finding.end, input.window.start);
            try accepted.append(a, rebased);
        }
    }
    return accepted.items.toOwnedSlice(a);
}

pub fn fieldValue(document: std.json.Value, field: []const u8) !std.json.Value {
    if (document != .object) return error.InvalidPrivacyDocument;
    // Artifact source fields are literal object keys.
    return document.object.get(field) orelse .null;
}

/// Accept the artifact API's extraction item (entities) or a constrained
/// generator object (findings). A missing collection is malformed, never clean.
pub fn normalize(a: A, field: []const u8, source: []const u8, value: std.json.Value, categories: []const contract.Category) ![]contract.Finding {
    if (value != .object) return error.InvalidPrivacyOutput;
    if (value.object.get("offset_unit")) |unit| {
        if (unit != .string or !std.mem.eql(u8, unit.string, "utf8_bytes")) return error.InvalidPrivacyOffsetUnit;
    }
    const entries = value.object.get("findings") orelse value.object.get("entities") orelse return error.InvalidPrivacyOutput;
    var result: FindingSet = .{};
    defer result.positions.deinit(a);
    errdefer result.items.deinit(a);
    switch (entries) {
        .array => |array| for (array.items) |entry| try appendEntry(a, &result, field, source, entry, categories, null),
        .object => |object| {
            var groups = object.iterator();
            while (groups.next()) |group| {
                const values = group.value_ptr.*;
                if (values != .array) return error.InvalidPrivacyOutput;
                for (values.array.items) |entry| try appendEntry(a, &result, field, source, entry, categories, group.key_ptr.*);
            }
        },
        else => return error.InvalidPrivacyOutput,
    }
    return result.items.toOwnedSlice(a);
}

fn appendEntry(a: A, result: *FindingSet, field: []const u8, source: []const u8, entry: std.json.Value, categories: []const contract.Category, group: ?[]const u8) !void {
    if (entry != .object) return error.InvalidPrivacyOutput;
    const name: []const u8 = if (group) |name| name else name: {
        const value = entry.object.get("category") orelse entry.object.get("label") orelse return error.InvalidPrivacyOutput;
        if (value != .string) return error.InvalidPrivacyOutput;
        break :name value.string;
    };
    const category = for (categories) |category| {
        if (std.mem.eql(u8, category.name, name)) break category;
    } else return error.UnknownPrivacyCategory;
    const start = entry.object.get("start") orelse return error.InvalidPrivacyOutput;
    const end = entry.object.get("end") orelse return error.InvalidPrivacyOutput;
    if (start != .integer or end != .integer or start.integer < 0 or end.integer < 0) return error.InvalidPrivacySpan;
    const confidence = entry.object.get("confidence") orelse entry.object.get("score") orelse .null;
    const score: ?f32 = switch (confidence) {
        .null => null,
        .float => |v| @floatCast(v),
        .integer => |v| @floatFromInt(v),
        else => return error.InvalidPrivacyConfidence,
    };
    const evidence = entry.object.get("text") orelse .null;
    if (evidence != .null and evidence != .string) return error.InvalidPrivacyOutput;
    const finding = contract.Finding{ .field = field, .category = category.name, .start = std.math.cast(usize, start.integer) orelse return error.InvalidPrivacySpan, .end = std.math.cast(usize, end.integer) orelse return error.InvalidPrivacySpan, .confidence = score };
    try finding.validate(source, if (evidence == .string) evidence.string else null);
    const threshold = category.threshold orelse return error.InvalidPrivacyThreshold;
    if (!std.math.isFinite(threshold) or threshold < 0 or threshold > 1) return error.InvalidPrivacyThreshold;
    if (score) |p| if (p < threshold) return;
    try result.append(a, finding);
}

/// Preserve detector order and maximum confidence in expected linear time.
/// Keys borrow the field/category strings owned by the inspection arena.
const FindingSet = struct {
    const Key = struct { field: []const u8, category: []const u8, start: usize, end: usize };
    const Context = struct {
        pub fn hash(_: @This(), key: Key) u64 {
            var hasher = std.hash.Wyhash.init(0);
            hasher.update(key.field);
            hasher.update(key.category);
            hasher.update(std.mem.asBytes(&key.start));
            hasher.update(std.mem.asBytes(&key.end));
            return hasher.final();
        }
        pub fn eql(_: @This(), left: Key, right: Key) bool {
            return left.start == right.start and left.end == right.end and
                std.mem.eql(u8, left.field, right.field) and std.mem.eql(u8, left.category, right.category);
        }
    };
    items: std.ArrayList(contract.Finding) = .empty,
    positions: std.HashMapUnmanaged(Key, usize, Context, std.hash_map.default_max_load_percentage) = .empty,

    fn append(self: *FindingSet, a: A, finding: contract.Finding) !void {
        // Reserve before inserting so allocation failure cannot leave a key
        // pointing outside the result array.
        try self.items.ensureUnusedCapacity(a, 1);
        const entry = try self.positions.getOrPut(a, .{ .field = finding.field, .category = finding.category, .start = finding.start, .end = finding.end });
        if (entry.found_existing) {
            const prior = &self.items.items[entry.value_ptr.*];
            if (finding.confidence) |score| if (prior.confidence == null or score > prior.confidence.?) {
                prior.confidence = score;
            };
        } else {
            entry.value_ptr.* = self.items.items.len;
            self.items.appendAssumeCapacity(finding);
        }
    }
};

test "privacy deduplication preserves first order strongest confidence and distinct identities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var set: FindingSet = .{};
    defer set.positions.deinit(a);
    for (0..32768) |i| try set.append(a, .{ .field = "body", .category = "person", .start = i, .end = i + 1 });
    // Reversed detector/window duplicates must not reorder retained evidence.
    var i: usize = 32768;
    while (i != 0) {
        i -= 1;
        try set.append(a, .{ .field = "body", .category = "person", .start = i, .end = i + 1, .confidence = 0.9 });
        try set.append(a, .{ .field = "body", .category = "person", .start = i, .end = i + 1, .confidence = 0.5 });
    }
    try std.testing.expectEqual(@as(usize, 32768), set.items.items.len);
    for (set.items.items, 0..) |finding, index| {
        try std.testing.expectEqual(index, finding.start);
        try std.testing.expectEqual(@as(?f32, 0.9), finding.confidence);
    }
    try set.append(a, .{ .field = "other", .category = "person", .start = 0, .end = 1 });
    try set.append(a, .{ .field = "body", .category = "email", .start = 0, .end = 1 });
    try set.append(a, .{ .field = "body", .category = "person", .start = 0, .end = 2 });
    try std.testing.expectEqual(@as(usize, 32771), set.items.items.len);
}

test "privacy deduplication retains coherent ownership on allocation failure" {
    const Attempt = struct {
        fn run(a: A) !void {
            var set: FindingSet = .{};
            defer set.items.deinit(a);
            defer set.positions.deinit(a);
            for (0..32) |i| try set.append(a, .{ .field = "body", .category = "person", .start = i, .end = i + 1 });
            try set.append(a, .{ .field = "body", .category = "person", .start = 0, .end = 1, .confidence = 0.9 });
        }
    };
    // ArrayList can grow in place depending on allocator addresses. Exercise
    // both allocation and resize failures without assuming a fixed count.
    for (0..48) |fail_index| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index, .resize_fail_index = fail_index });
        Attempt.run(failing.allocator()) catch |err| if (err != error.OutOfMemory) return err;
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    }
}

test "privacy windowing accounts for schema tokens and covers original bytes" {
    const a = std.testing.allocator;
    const source = "a b c d e f g h";
    const tokens = [_]Token{ .{ .start = 0, .end = 1 }, .{ .start = 2, .end = 3 }, .{ .start = 4, .end = 5 }, .{ .start = 6, .end = 7 }, .{ .start = 8, .end = 9 }, .{ .start = 10, .end = 11 }, .{ .start = 12, .end = 13 }, .{ .start = 14, .end = 15 } };
    const plan = try windows(a, source, .{ .tokens = &tokens, .overhead = 2, .limit = 6 }, .{ .overlap_tokens = 1 });
    defer a.free(plan);
    try std.testing.expectEqual(@as(usize, 3), plan.len);
    try std.testing.expectEqual(@as(usize, 0), plan[0].start);
    try std.testing.expectEqual(source.len, plan[2].end);
    try std.testing.expect(plan[1].start < plan[0].end);
    try std.testing.expectError(error.PrivacyWindowLimitExceeded, windows(a, source, .{ .tokens = &tokens, .overhead = 2, .limit = 6 }, .{ .overlap_tokens = 1, .max_windows = 1 }));
}

test "privacy generated evidence is validated before threshold filtering and absent findings fail" {
    const a = std.testing.allocator;
    var malformed = try std.json.parseFromSlice(std.json.Value, a, "{\"findings\":[{\"category\":\"email\",\"start\":0,\"end\":99,\"confidence\":0.1}]}", .{});
    defer malformed.deinit();
    const categories = [_]contract.Category{.{ .name = "email", .threshold = 0.5 }};
    try std.testing.expectError(error.InvalidPrivacySpan, normalize(a, "body", "hello", malformed.value, &categories));
    var absent = try std.json.parseFromSlice(std.json.Value, a, "{}", .{});
    defer absent.deinit();
    try std.testing.expectError(error.InvalidPrivacyOutput, normalize(a, "body", "hello", absent.value, &categories));
}

test "privacy inspection batches all windows, preserves optional confidence, and fails without partial receipts" {
    const a = std.testing.allocator;
    const Fake = struct {
        calls: usize = 0,
        invalid_after: ?usize = null,
        canceled: bool = false,
        fn check(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.canceled) return error.Canceled;
        }
        fn tokenize(_: *anyopaque, out: A, _: contract.Policy, text: []const u8) !Geometry {
            const tokens = try out.alloc(Token, text.len);
            for (tokens, 0..) |*token, i| token.* = .{ .start = i, .end = i + 1 };
            return .{ .tokens = tokens, .overhead = 2, .limit = 6 };
        }
        fn produce(ptr: *anyopaque, out: A, _: contract.Policy, inputs: []const Input) ![]const []const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expect(inputs.len <= 8);
            const outputs = try out.alloc([]const u8, inputs.len);
            for (outputs) |*output| output.* = if (self.invalid_after != null and self.calls > self.invalid_after.?) "{}" else "{\"findings\":[{\"category\":\"person\",\"start\":0,\"end\":1}]}";
            return outputs;
        }
        fn inspector(self: *@This()) Inspector {
            return .{ .ptr = self, .tokenize = tokenize, .produce = produce, .check = check, .windowing = .{ .overlap_tokens = 1 } };
        }
    };
    var document = try std.json.parseFromSlice(std.json.Value, a, "{\"body\":\"abcdefghijklmnopqrstuvwxyz1234\"}", .{});
    defer document.deinit();
    const policy = contract.Policy{ .mode = .monitor, .fields = &.{"body"}, .category_groups = &.{}, .categories = &.{.{ .name = "person" }}, .execution = .{ .max_attempts = 2 } };
    var fake = Fake{};
    var inspected = try inspect(a, policy, document.value, fake.inspector());
    defer inspected.deinit();
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expect(inspected.fields[0].receipt.current(inspected.policy_version, "abcdefghijklmnopqrstuvwxyz1234", contract.default_revision));
    try std.testing.expectEqual(@as(?f32, null), inspected.fields[0].findings[0].confidence);
    try std.testing.expect(inspected.fields[0].findings[8].start > 0);
    fake = .{ .invalid_after = 1 };
    try std.testing.expectError(error.InvalidPrivacyOutput, inspect(a, policy, document.value, fake.inspector()));
    try std.testing.expectEqual(@as(usize, 3), fake.calls);
    fake = .{ .canceled = true };
    try std.testing.expectError(error.Canceled, inspect(a, policy, document.value, fake.inspector()));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
}
