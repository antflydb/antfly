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
const contract = @import("antfly_decisions");
const generating = @import("antfly_generating");
const constrained = @import("antfly_structured");
pub const Json = std.json.Value;
pub const Input = @import("decision_input.zig").Input;
/// Request-arena-owned wire payload; adapters supply transport metadata along
/// with their body so the shared executor is independent of vendor formats.
pub const WirePayload = struct {
    body: []const u8,
    /// Encoded/binary image bytes in the body; visual tokens are reserved
    /// separately instead of charging compressed images as UTF-8 text.
    image_bytes: usize = 0,
    content_type: []const u8 = "application/json",
    headers: []const [2][]const u8 = &.{},
};
pub const Provider = enum { antfly, jev, openai, cloudflare };
pub const Kind = enum { choice, multi_choice, score, noul };
pub const Function = enum { ai_decide, ai_choice, ai_score, ai_probability };
pub const Capabilities = struct { max_questions: usize = 64, max_choices: usize = 64, max_levels: usize, max_input_bytes: usize = 1024 * 1024, images: bool = false, messages: bool = false, full_distribution: bool = true, embedding_similarity: bool = false, generated: bool = false };
pub fn capabilities(provider: Provider) Capabilities {
    return .{ .max_levels = if (provider == .antfly) 64 else 10, .images = provider != .antfly, .messages = provider != .antfly };
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
        if (self.dimensions == 0) return error.InvalidDeciderConfig;
        if (self.min_similarity) |v| if (!std.math.isFinite(v) or v < -1 or v > 1) return error.InvalidDeciderConfig;
        if (self.min_margin) |v| if (!std.math.isFinite(v) or v < 0 or v > 2) return error.InvalidDeciderConfig;
        if (self.calibration_id) |id| {
            if (id.len == 0 or id.len > 64 or self.min_similarity != null or self.min_margin != null) return error.InvalidDeciderConfig;
            for (id) |c| if (!std.ascii.isAlphanumeric(c) and c != '_' and c != '-') return error.InvalidDeciderConfig;
        }
    }
};
pub const DeciderConfig = struct {
    provider: Provider = .antfly,
    decision_method: enum { typed, embedding_similarity, generated } = .typed,
    generator: ?generating.GeneratorConfig = null,
    embedder: ?[]const u8 = null,
    output: constrained.Output = .auto,
    probabilities: constrained.Probabilities = .token_logprobs,
    instructions: []const u8 = "",
    batch: constrained.Batch = .{},
    model: []const u8 = "",
    model_identity: ?[]const u8 = null,
    embedding_options: ?EmbeddingOptions = null,
    url: []const u8 = "",
    api_key: ?[]const u8 = null,
    account_id: ?[]const u8 = null,
    gateway_id: ?[]const u8 = null,
    max_rows: u32 = 10000,
    max_input_tokens: u64 = 1000000,
    /// Operator-supplied upper bound for one image's token contribution for
    /// this provider/model. Required for image calls, including remote URLs.
    max_image_input_tokens: ?u64 = null,
    batch_size: u16 = 32,
    rate_limit: ?RateLimit = null,
    pub fn validate(self: @This()) !void {
        if (self.embedder == null) if (self.embedding_options) |options| switch (options.dimensions) {
            128, 256, 512, 768 => {},
            else => return error.InvalidDeciderConfig,
        };
        if (self.embedder) |name| {
            if (name.len == 0 or self.decision_method != .embedding_similarity or self.model.len != 0 or self.model_identity != null or self.url.len != 0 or self.api_key != null or self.rate_limit != null or (self.embedding_options != null and self.embedding_options.?.calibration_id != null)) return error.InvalidDeciderConfig;
        }
        if (self.provider == .cloudflare) {
            if (self.account_id == null or self.decision_method != .typed) return error.InvalidDeciderConfig;
            if (!std.mem.eql(u8, self.model, "@cf/cloudflare/clef") and !std.mem.eql(u8, self.model, "@cf/cloudflare/clef-flash")) return error.UnsupportedCloudflareModel;
            var buf: [1024]u8 = undefined;
            var fixed = std.heap.FixedBufferAllocator.init(&buf);
            _ = try contract.cloudflare.endpoint(fixed.allocator(), self.account_id.?, self.model);
        } else if (self.account_id != null or self.gateway_id != null) return error.InvalidDeciderConfig;
        if (self.decision_method == .generated) {
            const generator = self.generator orelse return error.InvalidDeciderConfig;
            try generator.validate();
            if (generator.provider != .antfly) _ = try constrained.hostedCapabilities(generator).select(self.output, self.probabilities);
            try self.batch.validate();
            if (self.model.len != 0 or self.url.len != 0 or self.api_key != null or self.rate_limit != null or self.instructions.len > 65536) return error.InvalidDeciderConfig;
        } else if (self.generator != null or self.instructions.len != 0 or self.output != .auto or self.probabilities != .token_logprobs or !std.meta.eql(self.batch, constrained.Batch{})) return error.InvalidDeciderConfig;
        if (self.decision_method == .embedding_similarity and self.provider != .antfly) return error.InvalidDeciderConfig;
        if (self.model_identity != null or self.embedding_options != null) {
            if (self.decision_method != .embedding_similarity) return error.InvalidDeciderConfig;
            if (self.embedding_options) |options| try options.validate();
            if (self.model_identity) |identity| {
                if (identity.len != 64) return error.InvalidDeciderConfig;
                for (identity) |c| if (!std.ascii.isDigit(c) and (c < 'a' or c > 'f')) return error.InvalidDeciderConfig;
            }
        }
        if (self.max_image_input_tokens) |bound| {
            if (bound == 0 or (self.decision_method != .generated and (self.provider == .antfly or self.decision_method != .typed))) return error.InvalidDeciderConfig;
        }
        if (self.max_rows == 0 or self.max_input_tokens == 0 or self.batch_size == 0 or self.batch_size > 256) return error.InvalidDeciderConfig;
        if (self.decision_method != .generated and self.provider != .jev and self.embedder == null and std.mem.trim(u8, self.model, " \r\n\t").len == 0) return error.InvalidDeciderConfig;
        if (std.mem.indexOfAny(u8, self.url, "\r\n") != null) return error.InvalidDeciderConfig;
        if (self.rate_limit) |policy| {
            inline for (.{ policy.requests_per_minute, policy.burst, policy.tokens_per_minute, policy.max_concurrency }) |value| if (value) |v| if (v == 0) return error.InvalidDeciderConfig;
            if (policy.pacing == .completion and (policy.requests_per_minute == null or (policy.burst orelse 1) != 1)) return error.InvalidDeciderConfig;
        }
    }
    pub fn resolvedCapabilities(self: @This()) Capabilities {
        var caps = capabilities(self.provider);
        caps.generated = self.decision_method == .generated;
        if (caps.generated) {
            caps.max_levels = 64;
            const generator = self.generator orelse return caps;
            caps.images = generator.provider == .antfly or constrained.hostedCapabilities(generator).images;
            caps.messages = true;
        }
        if (self.decision_method == .embedding_similarity) {
            caps.embedding_similarity = true;
            caps.full_distribution = false;
            caps.max_levels = 0;
        }
        return caps;
    }
    pub fn modelName(self: @This()) []const u8 {
        return if (self.embedder) |name| name else if (self.generator) |generator| generator.model else if (self.model.len != 0) self.model else "jev-latest";
    }
    pub fn baseUrl(self: @This()) []const u8 {
        return if (self.url.len != 0) self.url else switch (self.provider) {
            .antfly => "http://127.0.0.1:8082",
            .jev => "https://api.typesafe.ai",
            .openai => "https://api.openai.com/v1",
            .cloudflare => "https://api.cloudflare.com",
        };
    }
    pub fn clone(self: @This(), a: std.mem.Allocator) !@This() {
        var result = self;
        result.embedder = if (self.embedder) |name| try a.dupe(u8, name) else null;
        errdefer if (result.embedder) |name| a.free(name);
        result.account_id = if (self.account_id) |v| try a.dupe(u8, v) else null;
        errdefer if (result.account_id) |v| a.free(v);
        result.gateway_id = if (self.gateway_id) |v| try a.dupe(u8, v) else null;
        errdefer if (result.gateway_id) |v| a.free(v);
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
        errdefer if (result.embedding_options) |options| if (options.calibration_id) |id| a.free(id);
        result.generator = if (self.generator) |generator| try generator.clone(a) else null;
        errdefer if (result.generator) |*generator| generator.deinit(a);
        result.instructions = try a.dupe(u8, self.instructions);
        return result;
    }
    pub fn deinit(self: *@This(), a: std.mem.Allocator) void {
        if (self.embedder) |name| a.free(name);
        if (self.generator) |*generator| generator.deinit(a);
        a.free(self.instructions);
        if (self.account_id) |v| a.free(v);
        if (self.gateway_id) |v| a.free(v);
        a.free(self.model);
        a.free(self.url);
        if (self.api_key) |key| a.free(key);
        if (self.model_identity) |identity| a.free(identity);
        if (self.embedding_options) |options| if (options.calibration_id) |id| a.free(id);
        self.* = undefined;
    }
};
pub fn wireRequest(a: std.mem.Allocator, cfg: DeciderConfig, input: []const u8, questions: Json) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temporary = arena.allocator();
    if (cfg.provider == .jev or cfg.provider == .cloudflare) {
        // Public score labels are response metadata, not Jev input fields.
        var upstream = jsonObject();
        const original = try object(questions);
        for (original.keys(), original.values()) |name, raw| {
            var question = jsonObject();
            const fields = try object(raw);
            for (fields.keys(), fields.values()) |key, value| {
                if (!std.mem.eql(u8, key, "level_labels")) try put(temporary, &question, key, value);
            }
            try put(temporary, &upstream, name, question);
        }
        const model = cfg.modelName();
        const selector = if (cfg.provider == .cloudflare) model[(std.mem.lastIndexOfScalar(u8, model, '/') orelse return error.InvalidDeciderConfig) + 1 ..] else model;
        return std.json.Stringify.valueAlloc(a, .{ .model = selector, .state = input, .questions = upstream }, .{});
    }
    const options: ?Json = if (cfg.embedding_options) |configured| try std.json.parseFromSliceLeaky(Json, temporary, try std.json.Stringify.valueAlloc(temporary, configured, .{ .emit_null_optional_fields = false }), .{}) else null;
    return contract.requestJson(a, cfg.modelName(), input, questions, options, cfg.model_identity);
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
        while (keys.next()) |key| if (!std.mem.eql(u8, key.key_ptr.*, "type") and !std.mem.eql(u8, key.key_ptr.*, "instructions") and !std.mem.eql(u8, key.key_ptr.*, "criteria") and !std.mem.eql(u8, key.key_ptr.*, "level_labels") and !(caps.embedding_similarity and (std.mem.eql(u8, key.key_ptr.*, "embedding_options") or std.mem.eql(u8, key.key_ptr.*, "similarity_thresholds")))) return error.InvalidDecisionSpecification;
        const kind = std.meta.stringToEnum(Kind, try text(q.get("type") orelse return error.InvalidDecisionSpecification)) orelse return error.InvalidDecisionSpecification;
        if ((caps.embedding_similarity and kind != .choice and kind != .multi_choice) or (!caps.embedding_similarity and !caps.generated and kind == .multi_choice)) return error.UnsupportedDecisionKind;
        schema_bytes +|= (try text(q.get("instructions") orelse return error.InvalidDecisionSpecification)).len;
        const criteria = q.get("criteria");
        switch (kind) {
            .noul => if (criteria != null) return error.InvalidDecisionSpecification,
            .choice, .multi_choice => {
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
                if (caps.generated) {
                    if (criteria != null) return error.InvalidDecisionSpecification;
                } else {
                    const levels = criteria orelse return error.InvalidDecisionSpecification;
                    if (levels != .array) return error.InvalidDecisionSpecification;
                    if (levels.array.items.len < 2 or levels.array.items.len > caps.max_levels) return error.DecisionLimitExceeded;
                    for (levels.array.items) |level| schema_bytes +|= (try text(level)).len;
                }
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
pub fn generatedQuestions(a: std.mem.Allocator, questions: Json, instructions: []const u8) !Json {
    var envelope = jsonObject();
    var generator = jsonObject();
    try put(a, &generator, "model", .{ .string = "contract-validation" });
    try put(a, &envelope, "generator", generator);
    try put(a, &envelope, "probabilities", .{ .string = "verdict_only" });
    try put(a, &envelope, "input", .{ .string = "validation" });
    try put(a, &envelope, "questions", questions);
    if (instructions.len > 0) try put(a, &envelope, "instructions", .{ .string = instructions });
    return (try contract.generated.parseRequest(a, envelope)).questions;
}
pub fn publicQuestions(a: std.mem.Allocator, questions: Json) !Json {
    if (questions == .array) for (questions.array.items) |question| {
        if (question == .object) if (question.object.get("type")) |kind| if (kind == .string and std.mem.eql(u8, kind.string, "score") and !question.object.contains("levels")) return generatedQuestions(a, questions, "");
    };
    return contract.internalQuestions(a, questions) catch |err| return if (err == error.OutOfMemory) err else error.InvalidDecisionSpecification;
}
pub fn questionsFor(a: std.mem.Allocator, function: Function, args: []const Json) !Json {
    if (args.len != descriptor(@tagName(function)).?.argument_count) return error.InvalidDecisionSpecification;
    if (function == .ai_decide) {
        const public = if (args[1] == .string) try parseSpecificationJson(a, args[1].string) else args[1];
        return publicQuestions(a, public);
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
    const answers = (try object(response)).get("answers") orelse return error.InvalidDecisionOutput;
    if (answers != .array) return error.InvalidDecisionOutput;
    for (answers.array.items) |entry| {
        const answer = try object(entry);
        const name = answer.get("name") orelse return error.InvalidDecisionOutput;
        if (name == .string and std.mem.eql(u8, name.string, "answer")) return answer.get(switch (function) {
            .ai_choice => "choice",
            .ai_score => "score",
            .ai_probability => "probability",
            else => unreachable,
        }) orelse error.InvalidDecisionOutput;
    }
    return error.InvalidDecisionOutput;
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
/// Resolve defaults through the provider serializer so response validation and
/// upstream execution use exactly the same per-question acceptance policy.
pub fn normalizeResponseWithConfig(a: std.mem.Allocator, questions: Json, source: Json, cfg: DeciderConfig) !Json {
    if (cfg.decision_method != .embedding_similarity) return normalizeResponseWithCapabilities(a, questions, source, cfg.resolvedCapabilities());
    const bytes = try wireRequest(a, cfg, "validation", questions);
    defer a.free(bytes);
    const wire = try std.json.parseFromSlice(Json, a, bytes, .{});
    defer wire.deinit();
    var effective = try publicQuestions(a, wire.value.object.get("questions").?);
    for (effective.object.values()) |*question| {
        if (!question.object.contains("embedding_options")) try put(a, question, "embedding_options", jsonObject());
    }
    return normalizeResponseWithCapabilities(a, effective, source, cfg.resolvedCapabilities());
}

pub fn normalizeResponseWithCapabilities(a: std.mem.Allocator, questions: Json, source: Json, caps: Capabilities) !Json {
    const private = try contract.internalResponse(a, source);
    return contract.publicResponse(a, try normalizeLegacy(a, questions, private, caps));
}
fn normalizeLegacy(a: std.mem.Allocator, questions: Json, source: Json, caps: Capabilities) !Json {
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
        if (caps.generated) {
            try validateGeneratedAnswer(a, spec, kind, answer);
            try put(a, &normalized, q.key_ptr.*, answer);
            continue;
        }
        if (answer.object.get("decision_method")) |method| {
            if (method != .string or !std.mem.eql(u8, method.string, "embedding_similarity") or !caps.embedding_similarity or (kind != .choice and kind != .multi_choice)) return error.InvalidDecisionOutput;
            inline for (.{ "confidence", "confidence_method", "act_probability", "probabilities", "noul", "probability" }) |field|
                if (answer.object.contains(field)) return error.InvalidDecisionOutput;
            const configured = spec.get("embedding_options");
            const acceptance = if (configured) |options| contract.scoring.Options.parse(options) catch return error.InvalidDecisionOutput else contract.scoring.Options{};
            if (acceptance.calibration_id) |expected| {
                const actual = text(answer.object.get("calibration_id") orelse return error.InvalidDecisionOutput) catch return error.InvalidDecisionOutput;
                if (!std.mem.eql(u8, actual, expected)) return error.InvalidDecisionOutput;
            }
            const manual_policy = configured != null and acceptance.calibration_id == null;
            const prototype_hash = text(answer.object.get("prototype_set_hash") orelse return error.InvalidDecisionOutput) catch return error.InvalidDecisionOutput;
            if (prototype_hash.len != 64) return error.InvalidDecisionOutput;
            for (prototype_hash) |c| if (!std.ascii.isDigit(c) and (c < 'a' or c > 'f')) return error.InvalidDecisionOutput;
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
            const metric = try text(answer.object.get("similarity_metric") orelse return error.InvalidDecisionOutput);
            if (!std.mem.eql(u8, metric, "cosine")) return error.InvalidDecisionOutput;
            if (kind == .multi_choice) {
                const thresholds = try object(answer.object.get("similarity_thresholds") orelse return error.InvalidDecisionOutput);
                if (thresholds.count() != criteria.count()) return error.InvalidDecisionOutput;
                const selected = answer.object.get("choices") orelse return error.InvalidDecisionOutput;
                if (selected != .array) return error.InvalidDecisionOutput;
                var expected_count: usize = 0;
                var expected_margin: f64 = std.math.inf(f64);
                for (criteria.keys()) |label| {
                    const score = try number(similarities.get(label).?);
                    const threshold = try number(thresholds.get(label) orelse return error.InvalidDecisionOutput);
                    if (threshold < -1 or threshold > 1) return error.InvalidDecisionOutput;
                    if (spec.get("similarity_thresholds")) |requested| {
                        const expected_threshold = try number(if (requested == .object) requested.object.get(label) orelse return error.InvalidDecisionOutput else requested);
                        if (@abs(threshold - expected_threshold) > 1e-6) return error.InvalidDecisionOutput;
                    }
                    expected_margin = @min(expected_margin, @abs(score - threshold));
                    if (score >= threshold) expected_count += 1;
                }
                for (selected.array.items, 0..) |value, index| {
                    const label = try text(value);
                    const score = try number(similarities.get(label) orelse return error.InvalidDecisionOutput);
                    if (score < try number(thresholds.get(label).?)) return error.InvalidDecisionOutput;
                    for (selected.array.items[0..index]) |other| if (std.mem.eql(u8, label, try text(other))) return error.InvalidDecisionOutput;
                }
                const margin = try number(answer.object.get("margin") orelse return error.InvalidDecisionOutput);
                if (@abs(margin - expected_margin) > 1e-6) return error.InvalidDecisionOutput;
                const status = try text(answer.object.get("status") orelse return error.InvalidDecisionOutput);
                if (manual_policy) {
                    const ambiguous = acceptance.min_margin != null and expected_margin < acceptance.min_margin.?;
                    const expected_status = if (ambiguous) "abstained" else if (expected_count == 0) "empty" else "selected";
                    if (!std.mem.eql(u8, status, expected_status)) return error.InvalidDecisionOutput;
                }
                if (std.mem.eql(u8, status, "abstained")) {
                    if (selected.array.items.len != 0 or !std.mem.eql(u8, try text(answer.object.get("abstention_reason") orelse return error.InvalidDecisionOutput), "min_margin")) return error.InvalidDecisionOutput;
                } else if (!std.mem.eql(u8, status, if (expected_count == 0) "empty" else "selected") or selected.array.items.len != expected_count) return error.InvalidDecisionOutput;
                try put(a, &normalized, q.key_ptr.*, answer);
                continue;
            }
            const margin = try number(answer.object.get("margin") orelse return error.InvalidDecisionOutput);
            if (@abs(margin - (best - second)) > 1e-6) return error.InvalidDecisionOutput;
            const status = try text(answer.object.get("status") orelse return error.InvalidDecisionOutput);
            const choice = answer.object.get("choice") orelse return error.InvalidDecisionOutput;
            if (manual_policy) {
                const reason: ?[]const u8 = if (best - second <= 1e-6) "tie" else if (acceptance.min_similarity != null and best < acceptance.min_similarity.?) "min_similarity" else if (acceptance.min_margin != null and best - second < acceptance.min_margin.?) "min_margin" else null;
                if (reason) |expected| {
                    if (!std.mem.eql(u8, status, "abstained")) return error.InvalidDecisionOutput;
                    const actual = text(answer.object.get("abstention_reason") orelse return error.InvalidDecisionOutput) catch return error.InvalidDecisionOutput;
                    if (!std.mem.eql(u8, actual, expected)) return error.InvalidDecisionOutput;
                } else if (!std.mem.eql(u8, status, "selected")) return error.InvalidDecisionOutput;
            }
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
        if (answer.object.get("confidence_method")) |method| {
            const name = try text(method);
            if (!std.mem.eql(u8, name, "normalized_inverse_entropy") and !std.mem.eql(u8, name, "max_probability")) return error.InvalidDecisionOutput;
        }
        if (answer.object.get("act_probability")) |act| _ = try probability(act);
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
            var entropy: f64 = 0;
            for (0..count) |i| {
                const key = if (kind == .choice) criteria.object.keys()[i] else try std.fmt.allocPrint(a, "{d}", .{i});
                const p = try probability(dist.get(key) orelse return error.InvalidDecisionOutput);
                total += p;
                if (p > 0) entropy -= p * @log(p);
                expected += @as(f64, @floatFromInt(i)) * p;
                if (p > best) {
                    best = p;
                    best_label = key;
                }
                if (kind == .score) try put(a, &legend, key, if (spec.get("level_labels")) |labels| labels.array.items[i] else criteria.array.items[i]);
            }
            if (@abs(total - 1) > 0.01 or total <= 0) return error.InvalidDecisionOutput;
            if (!answer.object.contains("confidence")) try put(a, &answer, "confidence", .{ .float = std.math.clamp(1 - entropy / @log(@as(f64, @floatFromInt(count))), 0, 1) });
            if (!answer.object.contains("confidence_method")) try put(a, &answer, "confidence_method", .{ .string = "normalized_inverse_entropy" });
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

fn validateGeneratedAnswer(a: std.mem.Allocator, spec: std.json.ObjectMap, kind: Kind, answer: Json) !void {
    const method = try text(answer.object.get("decision_method") orelse return error.InvalidDecisionOutput);
    if (!std.mem.eql(u8, method, "generated")) return error.InvalidDecisionOutput;
    const source = try text(answer.object.get("probability_source") orelse return error.InvalidDecisionOutput);
    const verdict = std.mem.eql(u8, source, "verdict");
    if (!verdict and !std.mem.eql(u8, source, "token_logprobs")) return error.InvalidDecisionOutput;
    const raw = answer.object.get("raw_structured_answer") orelse return error.InvalidDecisionOutput;
    if (kind == .score) {
        const score = try probability(answer.object.get("score") orelse return error.InvalidDecisionOutput);
        if (!verdict or @abs(score - try probability(raw)) > 1e-9) return error.InvalidDecisionOutput;
        return;
    }
    if (kind == .noul) {
        if (raw != .bool) return error.InvalidDecisionOutput;
        const p = try probability(answer.object.get("noul") orelse return error.InvalidDecisionOutput);
        if (verdict and p != @as(f64, if (raw.bool) 1 else 0)) return error.InvalidDecisionOutput;
        return;
    }
    const criteria = spec.get("criteria").?.object;
    const dist = try object(answer.object.get("probabilities") orelse return error.InvalidDecisionOutput);
    if (dist.count() != criteria.count()) return error.InvalidDecisionOutput;
    var total: f64 = 0;
    if (kind == .choice) {
        if (raw != .string or !criteria.contains(raw.string)) return error.InvalidDecisionOutput;
        if (!std.mem.eql(u8, try text(answer.object.get("choice") orelse return error.InvalidDecisionOutput), raw.string)) return error.InvalidDecisionOutput;
    } else {
        if (!verdict or raw != .array) return error.InvalidDecisionOutput;
        const choices = answer.object.get("choices") orelse return error.InvalidDecisionOutput;
        if (choices != .array or choices.array.items.len != raw.array.items.len) return error.InvalidDecisionOutput;
        for (raw.array.items, 0..) |choice, i| {
            if (choice != .string or !criteria.contains(choice.string) or !std.mem.eql(u8, choice.string, try text(choices.array.items[i]))) return error.InvalidDecisionOutput;
            for (raw.array.items[0..i]) |previous| if (std.mem.eql(u8, previous.string, choice.string)) return error.InvalidDecisionOutput;
        }
    }
    for (criteria.keys()) |label| {
        const p = try probability(dist.get(label) orelse return error.InvalidDecisionOutput);
        var selected = kind == .choice and std.mem.eql(u8, raw.string, label);
        if (kind == .multi_choice) for (raw.array.items) |choice| {
            if (std.mem.eql(u8, choice.string, label)) selected = true;
        };
        if (verdict and p != @as(f64, if (selected) 1 else 0)) return error.InvalidDecisionOutput;
        total += p;
    }
    if (kind == .choice and (@abs(total - 1) > 1e-6)) return error.InvalidDecisionOutput;
    _ = a;
}

test "decision provider embeddinggemma2 scored decisions require configured capability and preserve SQL null" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const questions = try std.json.parseFromSliceLeaky(Json, a, "{\"answer\":{\"type\":\"choice\",\"instructions\":\"Route\",\"criteria\":{\"a\":\"Account\",\"b\":\"Billing\"}}}", .{});
    const response = try std.json.parseFromSliceLeaky(Json, a, "{\"model\":\"embeddinggemma2\",\"answers\":{\"answer\":{\"type\":\"choice\",\"choice\":null,\"decision_method\":\"embedding_similarity\",\"similarity_metric\":\"cosine\",\"similarities\":{\"a\":0.3,\"b\":0.3},\"margin\":0,\"status\":\"abstained\",\"abstention_reason\":\"tie\"}},\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}", .{});
    try std.testing.expectError(error.InvalidDecisionOutput, normalizeResponse(a, questions, response));
    const caps = (DeciderConfig{ .provider = .antfly, .model = "embeddinggemma2", .decision_method = .embedding_similarity }).resolvedCapabilities();
    try validateQuestions(questions, caps);
    try std.testing.expectError(error.InvalidDecisionOutput, normalizeResponseWithCapabilities(a, questions, response, caps));
    const answer = response.object.getPtr("answers").?.object.getPtr("answer").?;
    const prototype_hash: [64]u8 = @splat('a');
    try put(a, answer, "prototype_set_hash", .{ .string = &prototype_hash });
    const normalized = try normalizeResponseWithCapabilities(a, questions, response, caps);
    try std.testing.expect((try selectResult(.ai_choice, normalized)) == .null);
    try std.testing.expectEqualStrings(&prototype_hash, normalized.object.get("answers").?.array.items[0].object.get("prototype_set_hash").?.string);
    try put(a, answer, "prototype_set_hash", .{ .string = "invalid" });
    try std.testing.expectError(error.InvalidDecisionOutput, normalizeResponseWithCapabilities(a, questions, response, caps));
    try put(a, answer, "prototype_set_hash", .{ .string = &prototype_hash });
    try put(a, answer, "confidence", .{ .float = 0.9 });
    try std.testing.expectError(error.InvalidDecisionOutput, normalizeResponseWithCapabilities(a, questions, response, caps));
    try std.testing.expectError(error.InvalidDeciderConfig, (DeciderConfig{ .provider = .jev, .decision_method = .embedding_similarity }).validate());
}

test "decision provider dimensions match named embedders while direct routes retain native geometry" {
    for ([_]u16{ 1, 384, 1024, 65535 }) |dimensions| {
        const named: DeciderConfig = .{ .decision_method = .embedding_similarity, .embedder = "dense", .embedding_options = .{ .dimensions = dimensions } };
        try named.validate();
        const native: DeciderConfig = .{ .decision_method = .embedding_similarity, .model = "embeddinggemma2", .embedding_options = .{ .dimensions = dimensions } };
        try std.testing.expectError(error.InvalidDeciderConfig, native.validate());
    }
    for ([_]u16{ 128, 256, 512, 768 }) |dimensions| {
        try (DeciderConfig{ .decision_method = .embedding_similarity, .model = "embeddinggemma2", .embedding_options = .{ .dimensions = dimensions } }).validate();
    }
    try std.testing.expectError(error.InvalidDeciderConfig, (DeciderConfig{ .decision_method = .embedding_similarity, .embedder = "dense", .embedding_options = .{ .dimensions = 0 } }).validate());
    try std.testing.expectError(error.InvalidDeciderConfig, (DeciderConfig{ .decision_method = .embedding_similarity, .embedder = "dense", .embedding_options = .{ .dimensions = 1024, .calibration_id = "native-only" } }).validate());
}

test "decision provider embeddinggemma2 SQL configuration owns calibration and renders the pinned request" {
    const a = std.testing.allocator;
    const identity: [64]u8 = @splat('a');
    const cfg = DeciderConfig{ .provider = .antfly, .decision_method = .embedding_similarity, .model = "embeddinggemma2", .model_identity = &identity, .embedding_options = .{ .dimensions = 128, .calibration_id = "routing_v1" } };
    try cfg.validate();
    var copy = try cfg.clone(a);
    defer copy.deinit(a);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const questions = try std.json.parseFromSliceLeaky(Json, arena.allocator(), "{\"route\":{\"type\":\"choice\",\"instructions\":\"Route\",\"criteria\":{\"a\":\"Account\",\"b\":\"Billing\"}}}", .{});
    const body = try wireRequest(a, copy, "Reset my password", questions);
    defer a.free(body);
    const parsed = try std.json.parseFromSlice(Json, a, body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings(&identity, parsed.value.object.get("model_identity").?.string);
    try std.testing.expectEqualStrings("routing_v1", parsed.value.object.get("questions").?.array.items[0].object.get("embedding_options").?.object.get("calibration_id").?.string);
    var invalid = cfg;
    invalid.embedding_options.?.min_margin = 0.1;
    try std.testing.expectError(error.InvalidDeciderConfig, invalid.validate());
}

test "decision provider embeddinggemma2 provider wrapper retains the trusted similarity contract" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Mock = struct {
        response: Json,
        fn validate(_: *anyopaque, _: []const u8, questions: Json, _: []const u8) !void {
            try validateQuestions(questions, (DeciderConfig{ .provider = .antfly, .decision_method = .embedding_similarity }).resolvedCapabilities());
        }
        fn caps(_: *anyopaque, _: []const u8, _: []const u8) !Capabilities {
            return (DeciderConfig{ .provider = .antfly, .decision_method = .embedding_similarity }).resolvedCapabilities();
        }
        fn evaluate(raw: *anyopaque, alloc: std.mem.Allocator, _: []const Request) ![]const Json {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return alloc.dupe(Json, &.{self.response});
        }
    };
    const questions = try std.json.parseFromSliceLeaky(Json, a, "{\"answer\":{\"type\":\"choice\",\"instructions\":\"Route\",\"criteria\":{\"a\":\"Account\",\"b\":\"Billing\"}}}", .{});
    var mock = Mock{ .response = try std.json.parseFromSliceLeaky(Json, a, "{\"model\":\"embeddinggemma2\",\"answers\":{\"answer\":{\"type\":\"choice\",\"decision_method\":\"embedding_similarity\",\"similarity_metric\":\"cosine\",\"prototype_set_hash\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"choice\":null,\"status\":\"abstained\",\"abstention_reason\":\"tie\",\"similarities\":{\"a\":0.3,\"b\":0.3},\"margin\":0}},\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}", .{}) };
    const provider = DecisionProvider{ .ptr = &mock, .validate_fn = Mock.validate, .evaluate_batch_fn = Mock.evaluate, .capabilities_fn = Mock.caps };
    const rows = try provider.evaluateBatch(a, &.{.{ .decider = "routing", .questions = questions, .input = .{ .text = "Reset password" } }});
    try std.testing.expect((try selectResult(.ai_choice, rows[0])) == .null);
}
pub const Request = struct { decider: []const u8, questions: Json, input: Input, source_table: []const u8 = "" };
pub const DecisionProvider = struct {
    /// Trusted routing scope borrowed from the bound statement, never public JSON.
    source_table: []const u8 = "",
    ptr: *anyopaque,
    /// Both callbacks use the same trusted table scope as execution.
    validate_fn: *const fn (*anyopaque, []const u8, Json, []const u8) anyerror!void,
    evaluate_batch_fn: *const fn (*anyopaque, std.mem.Allocator, []const Request) anyerror![]const Json,
    capabilities_fn: ?*const fn (*anyopaque, []const u8, []const u8) anyerror!Capabilities = null,
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
        try self.validate_fn(self.ptr, decider, questions, self.source_table);
    }
    fn requestSourceTable(self: @This(), request: Request) []const u8 {
        return if (self.source_table.len > 0) self.source_table else request.source_table;
    }
    pub fn validateRequests(self: @This(), a: std.mem.Allocator, requests: []const Request) !void {
        try self.checkpoint();
        for (requests) |request| {
            const table = self.requestSourceTable(request);
            try self.validate_fn(self.ptr, request.decider, request.questions, table);
            try request.input.validate();
            const caps = if (self.capabilities_fn) |f| try f(self.ptr, request.decider, self.requestSourceTable(request)) else Capabilities{ .max_levels = 64 };
            if (!caps.messages) _ = try request.input.requireText();
            if (!caps.images and request.input.imageCount() != 0) return error.UnsupportedInferenceFeature;
            try request.input.checkLimit(a, caps.max_input_bytes);
        }
    }
    pub fn evaluateBatch(self: @This(), a: std.mem.Allocator, requests: []const Request) ![]const Json {
        try self.validateRequests(a, requests);
        const scoped = if (self.source_table.len > 0) try a.dupe(Request, requests) else null;
        defer if (scoped) |owned| a.free(owned);
        if (scoped) |owned| for (owned) |*request| {
            request.source_table = self.source_table;
        };
        const results = try self.evaluate_batch_fn(self.ptr, a, scoped orelse requests);
        if (results.len != requests.len) return error.InvalidDecisionOutput;
        const out = try a.alloc(Json, results.len);
        for (requests, results, out) |request, result, *value| {
            const caps = if (self.capabilities_fn) |f| try f(self.ptr, request.decider, self.requestSourceTable(request)) else Capabilities{ .max_levels = 64 };
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
    const public = try std.json.parseFromSliceLeaky(Json, a,
        \\[{"name":"priority","type":"score","instructions":"Urgency","levels":[{"label":"low"},{"label":"high"}]}]
    , .{});
    const labeled = try publicQuestions(a, public);
    const upstream = try std.json.parseFromSliceLeaky(Json, a, try wireRequest(a, .{ .provider = .jev }, "text", labeled), .{});
    try std.testing.expect(!upstream.object.get("questions").?.object.get("priority").?.object.contains("level_labels"));
    const questions = try std.json.parseFromSliceLeaky(Json, a, "{\"priority\":{\"type\":\"score\",\"instructions\":\"Urgency\",\"criteria\":[\"Low\",\"High\"]}}", .{});
    try validateQuestions(questions, capabilities(.jev));
    const response = try std.json.parseFromSliceLeaky(Json, a, "{\"model\":\"jev-test\",\"answers\":{\"priority\":{\"type\":\"score\",\"score\":99,\"probabilities\":{\"0\":0.2,\"1\":0.8}}},\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}", .{});
    const normalized = try normalizeResponse(a, questions, response);
    try std.testing.expectApproxEqAbs(@as(f64, 0.8), normalized.object.get("answers").?.array.items[0].object.get("score").?.float, 0.0001);
    var malformed = response;
    var answers = malformed.object.getPtr("answers").?;
    var priority = answers.object.getPtr("priority").?;
    try priority.object.put(a, "probabilities", jsonObject());
    try std.testing.expectError(error.InvalidDecisionOutput, normalizeResponse(a, questions, malformed));
}

test "decision provider OpenAI configuration and score limits" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const json = try std.json.parseFromSliceLeaky(Json, a, "{\"provider\":\"openai\",\"model\":\"gpt-6-luna\",\"rate_limit\":{\"max_concurrency\":2}}", .{});
    const cfg = try parseConfig(a, json);
    try std.testing.expectEqual(Provider.openai, cfg.provider);
    try std.testing.expectEqualStrings("gpt-6-luna", cfg.modelName());
    try std.testing.expectEqualStrings("https://api.openai.com/v1", cfg.baseUrl());
    try std.testing.expectEqual(@as(?u32, 2), cfg.rate_limit.?.max_concurrency);
    try std.testing.expectError(error.InvalidDeciderConfig, (DeciderConfig{ .provider = .openai }).validate());
    try std.testing.expectError(error.InvalidDeciderConfig, (DeciderConfig{ .provider = .openai, .model = " \t " }).validate());
    const levels = try std.json.parseFromSliceLeaky(Json, a, "[\"0\",\"1\",\"2\",\"3\",\"4\",\"5\",\"6\",\"7\",\"8\",\"9\",\"10\"]", .{});
    const questions = try questionsFor(a, .ai_score, &.{ .{ .string = "context" }, .{ .string = "Risk?" }, levels, .{ .string = "openai" } });
    try std.testing.expectError(error.DecisionLimitExceeded, validateQuestions(questions, capabilities(.openai)));
    try validateQuestions(questions, capabilities(.antfly));
}

test "decision provider multi choice cutoffs retain the configured ambiguity margin" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const public = try std.json.parseFromSliceLeaky(Json, a,
        \\[{"name":"tags","type":"multi_choice","instructions":"Tags","choices":[{"value":"a"},{"value":"b"}],"similarity_thresholds":0.5}]
    , .{});
    const questions = try publicQuestions(a, public);
    const cfg: DeciderConfig = .{ .provider = .antfly, .model = "embeddinggemma2", .decision_method = .embedding_similarity, .embedding_options = .{ .min_similarity = 0.4, .min_margin = 0.1 } };
    const request = try contract.parse(a, try wireRequest(a, cfg, "input", questions));
    const policy = request.policies[0];
    try std.testing.expectEqual(@as(?f64, null), policy.options.min_similarity);
    try std.testing.expectEqual(@as(?f64, 0.1), policy.options.min_margin);
    const selection = try contract.scoring.selectMulti(a, &.{ 0.51, 0.2 }, policy.thresholds.?, policy.options.min_margin);
    try std.testing.expectEqualStrings("abstained", selection.status);
}

test "decision provider named arrays isolate calibration and validate empty multi choice" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const public = try std.json.parseFromSliceLeaky(Json, a,
        \\[{"name":"route","type":"choice","instructions":"Route","choices":[{"value":"a"},{"value":"b"}],"embedding_options":{"calibration_id":"routing_v1"}},{"name":"tags","type":"multi_choice","instructions":"Tags","choices":[{"value":"a"},{"value":"b"}],"similarity_thresholds":0.5}]
    , .{});
    const questions = try publicQuestions(a, public);
    const cfg: DeciderConfig = .{ .provider = .antfly, .model = "embeddinggemma2", .decision_method = .embedding_similarity, .embedding_options = .{ .dimensions = 128, .calibration_id = "default_policy" } };
    try validateQuestions(questions, cfg.resolvedCapabilities());
    const request = try std.json.parseFromSliceLeaky(Json, a, try wireRequest(a, cfg, "input", questions), .{});
    const wire = request.object.get("questions").?.array.items;
    try std.testing.expectEqualStrings("routing_v1", wire[0].object.get("embedding_options").?.object.get("calibration_id").?.string);
    try std.testing.expect(!wire[1].object.contains("embedding_options"));
    try std.testing.expect(!request.object.get("embedding_options").?.object.contains("calibration_id"));
    try std.testing.expect(!request.object.contains("state"));
    try std.testing.expectError(error.InvalidDecisionSpecification, validateQuestions(questions, capabilities(.openai)));
    var multi = jsonObject();
    try put(a, &multi, "tags", questions.object.get("tags").?);
    const response = try std.json.parseFromSliceLeaky(Json, a,
        \\{"model":"embeddinggemma2","answers":[{"name":"tags","type":"multi_choice","decision_method":"embedding_similarity","similarity_metric":"cosine","prototype_set_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","choices":[],"similarities":[{"value":"a","similarity":0.2},{"value":"b","similarity":0.1}],"similarity_thresholds":{"a":0.5,"b":0.5},"margin":0.3,"status":"empty"}],"usage":{"input_tokens":2,"output_tokens":0}}
    , .{});
    const normalized = try normalizeResponseWithCapabilities(a, multi, response, cfg.resolvedCapabilities());
    try std.testing.expectEqualStrings("empty", normalized.object.get("answers").?.array.items[0].object.get("status").?.string);
    try std.testing.expectEqual(@as(usize, 0), normalized.object.get("answers").?.array.items[0].object.get("choices").?.array.items.len);
    var changed = response;
    const answer = &changed.object.getPtr("answers").?.array.items[0];
    try put(a, answer, "choices", try std.json.parseFromSliceLeaky(Json, a, "[\"a\"]", .{}));
    try std.testing.expectError(error.InvalidDecisionOutput, normalizeResponseWithCapabilities(a, multi, changed, cfg.resolvedCapabilities()));
}

test "decision provider embeddinggemma2 validates effective manual acceptance and calibration identity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var questions = try std.json.parseFromSliceLeaky(Json, a,
        \\{"answer":{"type":"choice","instructions":"Route","criteria":{"a":"Account","b":"Billing"}}}
    , .{});
    var response = try std.json.parseFromSliceLeaky(Json, a,
        \\{"model":"embeddinggemma2","answers":[{"name":"answer","type":"choice","choice":"a","decision_method":"embedding_similarity","similarity_metric":"cosine","prototype_set_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","similarities":[{"value":"a","similarity":0.51},{"value":"b","similarity":0.5}],"margin":0.01,"status":"selected"}],"usage":{"input_tokens":2,"output_tokens":0}}
    , .{});
    var cfg = DeciderConfig{ .provider = .antfly, .model = "embeddinggemma2", .decision_method = .embedding_similarity, .embedding_options = .{ .min_similarity = 0.8, .min_margin = 0.2 } };
    try std.testing.expectError(error.InvalidDecisionOutput, normalizeResponseWithConfig(a, questions, response, cfg));
    const spec = questions.object.getPtr("answer").?;
    try put(a, spec, "embedding_options", try std.json.parseFromSliceLeaky(Json, a, "{\"min_similarity\":0.4,\"min_margin\":0.001}", .{}));
    _ = try normalizeResponseWithConfig(a, questions, response, cfg);
    const answer = &response.object.getPtr("answers").?.array.items[0];
    try put(a, answer, "choice", .null);
    try put(a, answer, "status", .{ .string = "abstained" });
    try put(a, answer, "abstention_reason", .{ .string = "min_margin" });
    try std.testing.expectError(error.InvalidDecisionOutput, normalizeResponseWithConfig(a, questions, response, cfg));
    try put(a, spec, "embedding_options", try std.json.parseFromSliceLeaky(Json, a, "{\"min_similarity\":0.8,\"min_margin\":0.2}", .{}));
    try std.testing.expectError(error.InvalidDecisionOutput, normalizeResponseWithConfig(a, questions, response, cfg));
    try put(a, answer, "abstention_reason", .{ .string = "min_similarity" });
    _ = try normalizeResponseWithConfig(a, questions, response, cfg);
    try put(a, spec, "embedding_options", try std.json.parseFromSliceLeaky(Json, a, "{\"min_margin\":0.2}", .{}));
    try put(a, answer, "abstention_reason", .{ .string = "min_margin" });
    _ = try normalizeResponseWithConfig(a, questions, response, cfg);
    _ = spec.object.swapRemove("embedding_options");
    cfg.embedding_options = .{ .calibration_id = "routing_v1" };
    try std.testing.expectError(error.InvalidDecisionOutput, normalizeResponseWithConfig(a, questions, response, cfg));
    try put(a, answer, "calibration_id", .{ .string = "wrong_policy" });
    try std.testing.expectError(error.InvalidDecisionOutput, normalizeResponseWithConfig(a, questions, response, cfg));
    try put(a, answer, "calibration_id", .{ .string = "routing_v1" });
    _ = try normalizeResponseWithConfig(a, questions, response, cfg);
}

test "decision provider embeddinggemma2 validates multi choice configured margins and explicit overrides" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var questions = try std.json.parseFromSliceLeaky(Json, a,
        \\{"tags":{"type":"multi_choice","instructions":"Tags","criteria":{"a":"Account","b":"Billing"},"similarity_thresholds":0.5}}
    , .{});
    var response = try std.json.parseFromSliceLeaky(Json, a,
        \\{"model":"embeddinggemma2","answers":[{"name":"tags","type":"multi_choice","choices":["a"],"decision_method":"embedding_similarity","similarity_metric":"cosine","prototype_set_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","similarities":[{"value":"a","similarity":0.51},{"value":"b","similarity":0.2}],"similarity_thresholds":{"a":0.5,"b":0.5},"margin":0.01,"status":"selected"}],"usage":{"input_tokens":2,"output_tokens":0}}
    , .{});
    const cfg = DeciderConfig{ .provider = .antfly, .model = "embeddinggemma2", .decision_method = .embedding_similarity, .embedding_options = .{ .min_similarity = 0.4, .min_margin = 0.1 } };
    try std.testing.expectError(error.InvalidDecisionOutput, normalizeResponseWithConfig(a, questions, response, cfg));
    const answer = &response.object.getPtr("answers").?.array.items[0];
    const selected = answer.object.get("choices").?;
    try put(a, answer, "choices", .{ .array = .init(a) });
    try put(a, answer, "status", .{ .string = "abstained" });
    try put(a, answer, "abstention_reason", .{ .string = "min_margin" });
    _ = try normalizeResponseWithConfig(a, questions, response, cfg);
    try put(a, questions.object.getPtr("tags").?, "embedding_options", jsonObject());
    try std.testing.expectError(error.InvalidDecisionOutput, normalizeResponseWithConfig(a, questions, response, cfg));
    try put(a, answer, "choices", selected);
    try put(a, answer, "status", .{ .string = "selected" });
    _ = answer.object.swapRemove("abstention_reason");
    _ = try normalizeResponseWithConfig(a, questions, response, cfg);
}
