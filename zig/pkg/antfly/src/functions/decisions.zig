// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Provider-neutral typed decisions. All values returned by this module belong
//! to the supplied allocator (normally a bounded request arena).
const std = @import("std");
pub const Json = std.json.Value;
pub const Provider = enum { antfly, jev };
pub const Kind = enum { choice, score, noul };
pub const Function = enum { ai_decide, ai_choice, ai_score, ai_probability };
pub const Capabilities = struct { max_questions: usize = 64, max_choices: usize = 64, max_levels: usize, max_input_bytes: usize = 1024 * 1024, full_distribution: bool = true };
pub fn capabilities(provider: Provider) Capabilities {
    return .{ .max_levels = if (provider == .jev) 10 else 64 };
}
pub const RateLimit = struct {
    pacing: ?enum { token_bucket, completion } = null,
    requests_per_minute: ?u32 = null,
    burst: ?u32 = null,
    tokens_per_minute: ?u64 = null,
    max_concurrency: ?u32 = null,
};
pub const DeciderConfig = struct {
    provider: Provider,
    model: []const u8 = "",
    url: []const u8 = "",
    api_key: ?[]const u8 = null,
    max_rows: u32 = 10000,
    max_input_tokens: u64 = 1000000,
    batch_size: u16 = 32,
    rate_limit: ?RateLimit = null,
    pub fn validate(self: @This()) !void {
        if (self.max_rows == 0 or self.max_input_tokens == 0 or self.batch_size == 0 or self.batch_size > 256) return error.InvalidDeciderConfig;
        if (self.provider == .antfly and std.mem.trim(u8, self.model, " \r\n\t").len == 0) return error.InvalidDeciderConfig;
        if (std.mem.indexOfAny(u8, self.url, "\r\n") != null) return error.InvalidDeciderConfig;
        if (self.rate_limit) |policy| {
            inline for (.{ policy.requests_per_minute, policy.burst, policy.tokens_per_minute, policy.max_concurrency }) |value| if (value) |v| if (v == 0) return error.InvalidDeciderConfig;
            if (policy.pacing == .completion and (policy.requests_per_minute == null or (policy.burst orelse 1) != 1)) return error.InvalidDeciderConfig;
        }
    }
    pub fn modelName(self: @This()) []const u8 {
        return if (self.model.len != 0) self.model else "jev-latest";
    }
    pub fn baseUrl(self: @This()) []const u8 {
        return if (self.url.len != 0) self.url else if (self.provider == .jev) "https://api.typesafe.ai" else "http://127.0.0.1:8082";
    }
    pub fn clone(self: @This(), a: std.mem.Allocator) !@This() {
        var result = self;
        result.model = try a.dupe(u8, self.model);
        errdefer a.free(result.model);
        result.url = try a.dupe(u8, self.url);
        errdefer a.free(result.url);
        result.api_key = if (self.api_key) |key| try a.dupe(u8, key) else null;
        return result;
    }
    pub fn deinit(self: *@This(), a: std.mem.Allocator) void {
        a.free(self.model);
        a.free(self.url);
        if (self.api_key) |key| a.free(key);
        self.* = undefined;
    }
};
pub fn parseConfig(a: std.mem.Allocator, v: Json) !DeciderConfig {
    const bytes = try std.json.Stringify.valueAlloc(a, v, .{});
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(DeciderConfig, a, bytes, .{});
    defer parsed.deinit();
    try parsed.value.validate();
    return parsed.value.clone(a);
}
pub const Descriptor = struct { function: Function, result: enum { json, string, number }, argument_count: usize, external_io: bool = true, nullable: bool = true, execution_stable: bool = true, batchable: bool = true };
pub fn descriptor(name: []const u8) ?Descriptor {
    const f = std.meta.stringToEnum(Function, name) orelse return null;
    return .{ .function = f, .result = switch (f) {
        .ai_decide => .json,
        .ai_choice => .string,
        else => .number,
    }, .argument_count = switch (f) {
        .ai_decide, .ai_probability => 3,
        else => 4,
    } };
}
pub fn text(v: Json) ![]const u8 {
    if (v != .string or !std.unicode.utf8ValidateSlice(v.string) or std.mem.trim(u8, v.string, " \r\n\t").len == 0) return error.InvalidDecisionSpecification;
    return v.string;
}
pub fn object(v: Json) !std.json.ObjectMap {
    return if (v == .object) v.object else error.InvalidDecisionSpecification;
}
pub fn put(a: std.mem.Allocator, v: *Json, name: []const u8, value: Json) !void {
    if (v.object.getPtr(name)) |existing| {
        existing.* = value;
    } else {
        try v.object.put(a, name, value);
    }
}
pub fn jsonObject() Json {
    return .{ .object = .empty };
}
pub fn validateQuestions(questions: Json, caps: Capabilities) !void {
    const map = try object(questions);
    if (map.count() == 0 or map.count() > caps.max_questions) return error.DecisionLimitExceeded;
    var schema_bytes: usize = 0;
    var it = map.iterator();
    while (it.next()) |entry| {
        schema_bytes +|= (try text(.{ .string = entry.key_ptr.* })).len;
        const q = try object(entry.value_ptr.*);
        var keys = q.iterator();
        while (keys.next()) |key| if (!std.mem.eql(u8, key.key_ptr.*, "type") and !std.mem.eql(u8, key.key_ptr.*, "instructions") and !std.mem.eql(u8, key.key_ptr.*, "criteria")) return error.InvalidDecisionSpecification;
        const kind = std.meta.stringToEnum(Kind, try text(q.get("type") orelse return error.InvalidDecisionSpecification)) orelse return error.InvalidDecisionSpecification;
        schema_bytes +|= (try text(q.get("instructions") orelse return error.InvalidDecisionSpecification)).len;
        const criteria = q.get("criteria");
        switch (kind) {
            .noul => if (criteria != null) return error.InvalidDecisionSpecification,
            .choice => {
                const options = try object(criteria orelse return error.InvalidDecisionSpecification);
                if (options.count() < 2 or options.count() > caps.max_choices) return error.DecisionLimitExceeded;
                var options_it = options.iterator();
                while (options_it.next()) |option| {
                    schema_bytes +|= (try text(.{ .string = option.key_ptr.* })).len;
                    if (option.value_ptr.* != .string or !std.unicode.utf8ValidateSlice(option.value_ptr.string)) return error.InvalidDecisionSpecification;
                    schema_bytes +|= option.value_ptr.string.len;
                }
            },
            .score => {
                const levels = criteria orelse return error.InvalidDecisionSpecification;
                if (levels != .array) return error.InvalidDecisionSpecification;
                if (levels.array.items.len < 2 or levels.array.items.len > caps.max_levels) return error.DecisionLimitExceeded;
                for (levels.array.items) |level| schema_bytes +|= (try text(level)).len;
            },
        }
        if (schema_bytes > caps.max_input_bytes) return error.DecisionLimitExceeded;
    }
}
fn parseSpecificationJson(a: std.mem.Allocator, bytes: []const u8) !Json {
    return std.json.parseFromSliceLeaky(Json, a, bytes, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidDecisionSpecification,
    };
}
pub fn questionsFor(a: std.mem.Allocator, function: Function, args: []const Json) !Json {
    if (args.len != descriptor(@tagName(function)).?.argument_count) return error.InvalidDecisionSpecification;
    if (function == .ai_decide) {
        if (args[1] == .string) return parseSpecificationJson(a, args[1].string);
        return args[1];
    }
    var question = jsonObject();
    const kind: Kind = switch (function) {
        .ai_choice => .choice,
        .ai_score => .score,
        .ai_probability => .noul,
        else => unreachable,
    };
    try put(a, &question, "type", .{ .string = @tagName(kind) });
    try put(a, &question, "instructions", args[1]);
    if (kind != .noul) try put(a, &question, "criteria", if (args[2] == .string) try parseSpecificationJson(a, args[2].string) else args[2]);
    var questions = jsonObject();
    try put(a, &questions, "answer", question);
    return questions;
}
pub fn selectResult(function: Function, response: Json) !Json {
    if (function == .ai_decide) return response;
    const answers = try object((try object(response)).get("answers") orelse return error.InvalidDecisionOutput);
    const answer = try object(answers.get("answer") orelse return error.InvalidDecisionOutput);
    return answer.get(switch (function) {
        .ai_choice => "choice",
        .ai_score => "score",
        .ai_probability => "noul",
        else => unreachable,
    }) orelse error.InvalidDecisionOutput;
}
fn number(v: Json) !f64 {
    const n: f64 = switch (v) {
        .integer => @floatFromInt(v.integer),
        .float => v.float,
        else => return error.InvalidDecisionOutput,
    };
    if (!std.math.isFinite(n)) return error.InvalidDecisionOutput;
    return n;
}
fn probability(v: Json) !f64 {
    const n = try number(v);
    if (n < 0 or n > 1) return error.InvalidDecisionOutput;
    return n;
}
/// Validate complete distributions and derive selected values ourselves so all
/// adapters use the same ordinal and Boolean meaning. Preserve provider metadata.
pub fn normalizeResponse(a: std.mem.Allocator, questions: Json, source: Json) !Json {
    const bytes = try std.json.Stringify.valueAlloc(a, source, .{});
    defer a.free(bytes);
    const response = try std.json.parseFromSliceLeaky(Json, a, bytes, .{ .allocate = .alloc_always });
    const root = object(response) catch return error.InvalidDecisionOutput;
    _ = text(root.get("model") orelse return error.InvalidDecisionOutput) catch return error.InvalidDecisionOutput;
    const answers = object(root.get("answers") orelse return error.InvalidDecisionOutput) catch return error.InvalidDecisionOutput;
    if (answers.count() != questions.object.count()) return error.InvalidDecisionOutput;
    const usage = object(root.get("usage") orelse return error.InvalidDecisionOutput) catch return error.InvalidDecisionOutput;
    for ([_][]const u8{ "input_tokens", "output_tokens" }) |key| {
        const value = usage.get(key) orelse return error.InvalidDecisionOutput;
        if (value != .integer or value.integer < 0) return error.InvalidDecisionOutput;
    }
    var normalized = jsonObject();
    var it = questions.object.iterator();
    while (it.next()) |q| {
        const spec = q.value_ptr.object;
        const kind = std.meta.stringToEnum(Kind, spec.get("type").?.string).?;
        var answer = answers.get(q.key_ptr.*) orelse return error.InvalidDecisionOutput;
        if (answer != .object) return error.InvalidDecisionOutput;
        const actual_kind = answer.object.get("type") orelse return error.InvalidDecisionOutput;
        if (actual_kind != .string or !std.mem.eql(u8, actual_kind.string, @tagName(kind))) return error.InvalidDecisionOutput;
        if (answer.object.get("confidence")) |confidence| _ = try probability(confidence);
        if (kind == .noul) {
            _ = try probability(answer.object.get("noul") orelse return error.InvalidDecisionOutput);
        } else {
            if (kind == .choice) {
                const choice = answer.object.get("choice") orelse return error.InvalidDecisionOutput;
                if (choice != .string or !spec.get("criteria").?.object.contains(choice.string)) return error.InvalidDecisionOutput;
            } else _ = try number(answer.object.get("score") orelse return error.InvalidDecisionOutput);
            const dist = object(answer.object.get("probabilities") orelse return error.InvalidDecisionOutput) catch return error.InvalidDecisionOutput;
            const criteria = spec.get("criteria").?;
            const count = if (kind == .choice) criteria.object.count() else criteria.array.items.len;
            if (dist.count() != count) return error.InvalidDecisionOutput;
            var total: f64 = 0;
            var expected: f64 = 0;
            var best: f64 = -1;
            var best_label: []const u8 = "";
            var legend = jsonObject();
            for (0..count) |i| {
                const key = if (kind == .choice) criteria.object.keys()[i] else try std.fmt.allocPrint(a, "{d}", .{i});
                const p = try probability(dist.get(key) orelse return error.InvalidDecisionOutput);
                total += p;
                expected += @as(f64, @floatFromInt(i)) * p;
                if (p > best) {
                    best = p;
                    best_label = key;
                }
                if (kind == .score) try put(a, &legend, key, criteria.array.items[i]);
            }
            if (@abs(total - 1) > 0.01 or total <= 0) return error.InvalidDecisionOutput;
            if (kind == .choice) try put(a, &answer, "choice", .{ .string = best_label }) else {
                try put(a, &answer, "score", .{ .float = expected / total });
                try put(a, &answer, "legend", legend);
            }
        }
        try put(a, &normalized, q.key_ptr.*, answer);
    }
    var result = response;
    try put(a, &result, "answers", normalized);
    return result;
}
pub const Request = struct { decider: []const u8, questions: Json, input: []const u8, source_table: []const u8 = "" };
pub const DecisionProvider = struct {
    /// Trusted routing scope borrowed from the bound statement, never public JSON.
    source_table: []const u8 = "",
    ptr: *anyopaque,
    validate_fn: *const fn (*anyopaque, []const u8, Json) anyerror!void,
    evaluate_batch_fn: *const fn (*anyopaque, std.mem.Allocator, []const Request) anyerror![]const Json,
    checkpoint_fn: ?*const fn (*anyopaque) anyerror!void = null,
    pub fn withSourceTable(self: @This(), table: []const u8) @This() {
        var scoped = self;
        scoped.source_table = table;
        return scoped;
    }
    pub fn checkpoint(self: @This()) !void {
        if (self.checkpoint_fn) |f| try f(self.ptr);
    }
    pub fn validate(self: @This(), decider: []const u8, questions: Json) !void {
        try self.validate_fn(self.ptr, decider, questions);
    }
    pub fn evaluateBatch(self: @This(), a: std.mem.Allocator, requests: []const Request) ![]const Json {
        try self.checkpoint();
        for (requests) |request| {
            try self.validate(request.decider, request.questions);
            _ = try text(.{ .string = request.input });
        }
        const scoped = if (self.source_table.len > 0) try a.dupe(Request, requests) else null;
        defer if (scoped) |owned| a.free(owned);
        if (scoped) |owned| for (owned) |*request| {
            request.source_table = self.source_table;
        };
        const results = try self.evaluate_batch_fn(self.ptr, a, scoped orelse requests);
        if (results.len != requests.len) return error.InvalidDecisionOutput;
        const out = try a.alloc(Json, results.len);
        for (requests, results, out) |request, result, *value| value.* = try normalizeResponse(a, request.questions, result);
        try self.checkpoint();
        return out;
    }
};

test "decision provider limits and score normalization preserve ordinal semantics" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const questions = try std.json.parseFromSliceLeaky(Json, a, "{\"priority\":{\"type\":\"score\",\"instructions\":\"Urgency\",\"criteria\":[\"Low\",\"High\"]}}", .{});
    try validateQuestions(questions, capabilities(.jev));
    const response = try std.json.parseFromSliceLeaky(Json, a, "{\"model\":\"jev-test\",\"answers\":{\"priority\":{\"type\":\"score\",\"score\":99,\"probabilities\":{\"0\":0.2,\"1\":0.8}}},\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}", .{});
    const normalized = try normalizeResponse(a, questions, response);
    try std.testing.expectApproxEqAbs(@as(f64, 0.8), normalized.object.get("answers").?.object.get("priority").?.object.get("score").?.float, 0.0001);
    var malformed = response;
    var answers = malformed.object.getPtr("answers").?;
    var priority = answers.object.getPtr("priority").?;
    try priority.object.put(a, "probabilities", jsonObject());
    try std.testing.expectError(error.InvalidDecisionOutput, normalizeResponse(a, questions, malformed));
}
