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

//! Provider-neutral typed decisions. All values returned by this module belong
//! to the supplied allocator (normally a bounded request arena).
const std = @import("std");
pub const Json = std.json.Value;
pub const Provider = enum { antfly, jev };
pub const Kind = enum { choice, score, noul };
pub const Function = enum { ai_decide, ai_choice, ai_score, ai_probability };
pub const Capabilities = struct { max_questions: usize = 64, max_choices: usize = 64, max_levels: usize, max_input_bytes: usize = 1024 * 1024, full_distribution: bool = true, embedding_similarity: bool = false };
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
pub const EmbeddingOptions = struct {
    task_type: enum { CLUSTERING, CLASSIFICATION } = .CLUSTERING,
    dimensions: u16 = 768,
    min_similarity: ?f64 = null,
    min_margin: ?f64 = null,
    calibration_id: ?[]const u8 = null,
    pub fn validate(self: @This()) !void {
        switch (self.dimensions) {
            128, 256, 512, 768 => {},
            else => return error.InvalidDeciderConfig,
        }
        if (self.min_similarity) |v| if (!std.math.isFinite(v) or v < -1 or v > 1) return error.InvalidDeciderConfig;
        if (self.min_margin) |v| if (!std.math.isFinite(v) or v < 0 or v > 2) return error.InvalidDeciderConfig;
        if (self.calibration_id) |id| {
            if (id.len == 0 or id.len > 64 or self.min_similarity != null or self.min_margin != null) return error.InvalidDeciderConfig;
            for (id) |c| if (!std.ascii.isAlphanumeric(c) and c != '_' and c != '-') return error.InvalidDeciderConfig;
        }
    }
};
pub const DeciderConfig = struct {
    provider: Provider,
    decision_method: enum { typed, embedding_similarity } = .typed,
    model: []const u8 = "",
    model_identity: ?[]const u8 = null,
    embedding_options: ?EmbeddingOptions = null,
    url: []const u8 = "",
    api_key: ?[]const u8 = null,
    max_rows: u32 = 10000,
    max_input_tokens: u64 = 1000000,
    batch_size: u16 = 32,
    rate_limit: ?RateLimit = null,
    pub fn validate(self: @This()) !void {
        if (self.decision_method == .embedding_similarity and self.provider != .antfly) return error.InvalidDeciderConfig;
        if (self.model_identity != null or self.embedding_options != null) {
            if (self.decision_method != .embedding_similarity) return error.InvalidDeciderConfig;
            if (self.embedding_options) |options| try options.validate();
            if (self.model_identity) |identity| {
                if (identity.len != 64) return error.InvalidDeciderConfig;
                for (identity) |c| if (!std.ascii.isDigit(c) and (c < 'a' or c > 'f')) return error.InvalidDeciderConfig;
            }
        }
        if (self.max_rows == 0 or self.max_input_tokens == 0 or self.batch_size == 0 or self.batch_size > 256) return error.InvalidDeciderConfig;
        if (self.provider == .antfly and std.mem.trim(u8, self.model, " \r\n\t").len == 0) return error.InvalidDeciderConfig;
        if (std.mem.indexOfAny(u8, self.url, "\r\n") != null) return error.InvalidDeciderConfig;
        if (self.rate_limit) |policy| {
            inline for (.{ policy.requests_per_minute, policy.burst, policy.tokens_per_minute, policy.max_concurrency }) |value| if (value) |v| if (v == 0) return error.InvalidDeciderConfig;
            if (policy.pacing == .completion and (policy.requests_per_minute == null or (policy.burst orelse 1) != 1)) return error.InvalidDeciderConfig;
        }
    }
    pub fn resolvedCapabilities(self: @This()) Capabilities {
        var caps = capabilities(self.provider);
        if (self.decision_method == .embedding_similarity) {
            caps.embedding_similarity = true;
            caps.full_distribution = false;
            caps.max_levels = 0;
        }
        return caps;
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
        errdefer if (result.api_key) |key| a.free(key);
        result.model_identity = if (self.model_identity) |identity| try a.dupe(u8, identity) else null;
        errdefer if (result.model_identity) |identity| a.free(identity);
        if (result.embedding_options) |*options| if (options.calibration_id) |id| {
            options.calibration_id = try a.dupe(u8, id);
        };
        return result;
    }
    pub fn deinit(self: *@This(), a: std.mem.Allocator) void {
        a.free(self.model);
        a.free(self.url);
        if (self.api_key) |key| a.free(key);
        if (self.model_identity) |identity| a.free(identity);
        if (self.embedding_options) |options| if (options.calibration_id) |id| a.free(id);
        self.* = undefined;
    }
};
pub fn wireRequest(a: std.mem.Allocator, cfg: DeciderConfig, input: []const u8, questions: Json) ![]u8 {
    return std.json.Stringify.valueAlloc(a, .{ .model = cfg.modelName(), .model_identity = cfg.model_identity, .embedding_options = cfg.embedding_options, .state = input, .questions = questions }, .{ .emit_null_optional_fields = false });
}
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
        if (caps.embedding_similarity and kind != .choice) return error.UnsupportedDecisionKind;
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
                    if (option.value_ptr.* == .string) {
                        if (!std.unicode.utf8ValidateSlice(option.value_ptr.string)) return error.InvalidDecisionSpecification;
                        schema_bytes +|= option.value_ptr.string.len;
                    } else {
                        if (!caps.embedding_similarity) return error.UnsupportedDecisionKind;
                        const definition = try object(option.value_ptr.*);
                        for (definition.keys()) |key| if (!std.mem.eql(u8, key, "description") and !std.mem.eql(u8, key, "examples")) return error.InvalidDecisionSpecification;
                        if (definition.get("description")) |description| schema_bytes +|= (try text(description)).len;
                        const examples = definition.get("examples") orelse return error.InvalidDecisionSpecification;
                        if (examples != .array or examples.array.items.len == 0 or examples.array.items.len > 32) return error.DecisionLimitExceeded;
                        for (examples.array.items) |example| schema_bytes +|= (try text(example)).len;
                    }
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
    return normalizeResponseWithCapabilities(a, questions, source, .{ .max_levels = 64 });
}

// The allowed result contract is supplied by trusted decider configuration;
// a provider's response cannot opt itself into accepting uncalibrated scores.
pub fn normalizeResponseWithCapabilities(a: std.mem.Allocator, questions: Json, source: Json, caps: Capabilities) !Json {
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
        if (answer.object.get("decision_method")) |method| {
            if (method != .string or !std.mem.eql(u8, method.string, "embedding_similarity") or !caps.embedding_similarity or kind != .choice) return error.InvalidDecisionOutput;
            if (answer.object.contains("confidence") or answer.object.contains("probabilities")) return error.InvalidDecisionOutput;
            const similarities = try object(answer.object.get("similarities") orelse return error.InvalidDecisionOutput);
            const criteria = spec.get("criteria").?.object;
            if (similarities.count() != criteria.count()) return error.InvalidDecisionOutput;
            var best: f64 = -std.math.inf(f64);
            var second: f64 = -std.math.inf(f64);
            var best_label: []const u8 = "";
            for (criteria.keys()) |label| {
                const score = try number(similarities.get(label) orelse return error.InvalidDecisionOutput);
                if (score < -1 or score > 1) return error.InvalidDecisionOutput;
                if (score > best) {
                    second = best;
                    best = score;
                    best_label = label;
                } else second = @max(second, score);
            }
            const margin = try number(answer.object.get("margin") orelse return error.InvalidDecisionOutput);
            if (@abs(margin - (best - second)) > 1e-6) return error.InvalidDecisionOutput;
            const status = try text(answer.object.get("status") orelse return error.InvalidDecisionOutput);
            const choice = answer.object.get("choice") orelse return error.InvalidDecisionOutput;
            if (std.mem.eql(u8, status, "selected")) {
                if (best - second <= 1e-6 or choice != .string or !std.mem.eql(u8, choice.string, best_label)) return error.InvalidDecisionOutput;
            } else if (std.mem.eql(u8, status, "abstained")) {
                if (choice != .null) return error.InvalidDecisionOutput;
                const reason = try text(answer.object.get("abstention_reason") orelse return error.InvalidDecisionOutput);
                if (!std.mem.eql(u8, reason, "tie") and !std.mem.eql(u8, reason, "min_similarity") and !std.mem.eql(u8, reason, "min_margin")) return error.InvalidDecisionOutput;
                if (std.mem.eql(u8, reason, "tie") and best - second > 1e-6) return error.InvalidDecisionOutput;
            } else return error.InvalidDecisionOutput;
            try put(a, &normalized, q.key_ptr.*, answer);
            continue;
        }
        if (caps.embedding_similarity) return error.InvalidDecisionOutput;
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
    result.object.getPtr("answers").?.* = normalized;
    return result;
}

test "embeddinggemma2 scored decisions require configured capability and preserve SQL null" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const questions = try std.json.parseFromSliceLeaky(Json, a, "{\"answer\":{\"type\":\"choice\",\"instructions\":\"Route\",\"criteria\":{\"a\":\"Account\",\"b\":\"Billing\"}}}", .{});
    const response = try std.json.parseFromSliceLeaky(Json, a, "{\"model\":\"embeddinggemma2\",\"answers\":{\"answer\":{\"type\":\"choice\",\"choice\":null,\"decision_method\":\"embedding_similarity\",\"similarities\":{\"a\":0.3,\"b\":0.3},\"margin\":0,\"status\":\"abstained\",\"abstention_reason\":\"tie\"}},\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}", .{});
    try std.testing.expectError(error.InvalidDecisionOutput, normalizeResponse(a, questions, response));
    const caps = (DeciderConfig{ .provider = .antfly, .model = "embeddinggemma2", .decision_method = .embedding_similarity }).resolvedCapabilities();
    try validateQuestions(questions, caps);
    const normalized = try normalizeResponseWithCapabilities(a, questions, response, caps);
    try std.testing.expect((try selectResult(.ai_choice, normalized)) == .null);
    const answer = response.object.getPtr("answers").?.object.getPtr("answer").?;
    try put(a, answer, "confidence", .{ .float = 0.9 });
    try std.testing.expectError(error.InvalidDecisionOutput, normalizeResponseWithCapabilities(a, questions, response, caps));
    try std.testing.expectError(error.InvalidDeciderConfig, (DeciderConfig{ .provider = .jev, .decision_method = .embedding_similarity }).validate());
}

test "embeddinggemma2 SQL configuration owns calibration and renders the pinned request" {
    const a = std.testing.allocator;
    const identity: [64]u8 = @splat('a');
    const cfg = DeciderConfig{ .provider = .antfly, .decision_method = .embedding_similarity, .model = "embeddinggemma2", .model_identity = &identity, .embedding_options = .{ .dimensions = 128, .calibration_id = "routing_v1" } };
    try cfg.validate();
    var copy = try cfg.clone(a);
    defer copy.deinit(a);
    const body = try wireRequest(a, copy, "Reset my password", jsonObject());
    defer a.free(body);
    const parsed = try std.json.parseFromSlice(Json, a, body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings(&identity, parsed.value.object.get("model_identity").?.string);
    try std.testing.expectEqualStrings("routing_v1", parsed.value.object.get("embedding_options").?.object.get("calibration_id").?.string);
    var invalid = cfg;
    invalid.embedding_options.?.min_margin = 0.1;
    try std.testing.expectError(error.InvalidDeciderConfig, invalid.validate());
}

test "embeddinggemma2 provider wrapper retains the trusted similarity contract" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Mock = struct {
        response: Json,
        fn validate(_: *anyopaque, _: []const u8, questions: Json) !void {
            try validateQuestions(questions, (DeciderConfig{ .provider = .antfly, .decision_method = .embedding_similarity }).resolvedCapabilities());
        }
        fn caps(_: *anyopaque, _: []const u8) !Capabilities {
            return (DeciderConfig{ .provider = .antfly, .decision_method = .embedding_similarity }).resolvedCapabilities();
        }
        fn evaluate(raw: *anyopaque, alloc: std.mem.Allocator, _: []const Request) ![]const Json {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return alloc.dupe(Json, &.{self.response});
        }
    };
    const questions = try std.json.parseFromSliceLeaky(Json, a, "{\"answer\":{\"type\":\"choice\",\"instructions\":\"Route\",\"criteria\":{\"a\":\"Account\",\"b\":\"Billing\"}}}", .{});
    var mock = Mock{ .response = try std.json.parseFromSliceLeaky(Json, a, "{\"model\":\"embeddinggemma2\",\"answers\":{\"answer\":{\"type\":\"choice\",\"decision_method\":\"embedding_similarity\",\"choice\":null,\"status\":\"abstained\",\"abstention_reason\":\"tie\",\"similarities\":{\"a\":0.3,\"b\":0.3},\"margin\":0}},\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}", .{}) };
    const provider = DecisionProvider{ .ptr = &mock, .validate_fn = Mock.validate, .evaluate_batch_fn = Mock.evaluate, .capabilities_fn = Mock.caps };
    const rows = try provider.evaluateBatch(a, &.{.{ .decider = "routing", .questions = questions, .input = "Reset password" }});
    try std.testing.expect((try selectResult(.ai_choice, rows[0])) == .null);
}
pub const Request = struct { decider: []const u8, questions: Json, input: []const u8, source_table: []const u8 = "" };
pub const DecisionProvider = struct {
    /// Trusted routing scope borrowed from the bound statement, never public JSON.
    source_table: []const u8 = "",
    ptr: *anyopaque,
    validate_fn: *const fn (*anyopaque, []const u8, Json) anyerror!void,
    evaluate_batch_fn: *const fn (*anyopaque, std.mem.Allocator, []const Request) anyerror![]const Json,
    capabilities_fn: ?*const fn (*anyopaque, []const u8) anyerror!Capabilities = null,
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
        for (requests, results, out) |request, result, *value| {
            const caps = if (self.capabilities_fn) |f| try f(self.ptr, request.decider) else Capabilities{ .max_levels = 64 };
            value.* = try normalizeResponseWithCapabilities(a, request.questions, result, caps);
        }
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
