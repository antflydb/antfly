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

//! Provider-independent REVIEW policy and durable evidence. Execution is injected
//! by the host; no inference or storage dependency belongs in the matcher.
const std = @import("std");

pub const Acceptance = struct {
    choice: []const u8,
    min_probability: f64,
};
pub const Choice = struct { value: []const u8, description: ?[]const u8 = null };
pub const Question = struct {
    type: enum { choice },
    name: []const u8,
    instructions: []const u8 = "",
    choices: []const Choice,
};
pub const Config = struct {
    decider: std.json.Value,
    template: []const u8,
    question: Question,
    accept: Acceptance,
    /// An uncertain negative answer is an abstention, not evidence of difference.
    reject: ?Acceptance = null,
    /// Retain REVIEW by default. Minting on operational failure is opt-in.
    on_failure: enum { review, mint } = .review,
    description_inputs: struct { mention_artifact: ?[]const u8 = null, candidate_artifact: ?[]const u8 = null } = .{},
    max_pairs: u32 = 32,
    max_input_bytes: u32 = 16384,
    timeout_ms: u32 = 10000,
    budget_ms: u32 = 10000,

    pub fn parse(a: std.mem.Allocator, raw: []const u8) !std.json.Parsed(Config) {
        var parsed = try std.json.parseFromSlice(Config, a, raw, .{ .allocate = .alloc_always });
        errdefer parsed.deinit();
        try parsed.value.validate();
        return parsed;
    }
    pub fn validate(self: Config) !void {
        if (self.decider != .object or self.template.len == 0 or self.template.len > 65536 or
            self.question.name.len == 0 or
            self.question.choices.len < 2 or self.question.choices.len > 64 or
            self.max_pairs == 0 or self.max_pairs > 256 or self.max_input_bytes == 0 or
            self.max_input_bytes > 1024 * 1024 or self.timeout_ms == 0 or self.timeout_ms > 300000 or self.budget_ms == 0 or self.budget_ms > 300000)
            return error.InvalidAdjudicationConfig;
        if (self.question.instructions.len == 0) {
            const method = self.decider.object.get("decision_method") orelse return error.InvalidAdjudicationConfig;
            const instructions = self.decider.object.get("instructions") orelse return error.InvalidAdjudicationConfig;
            if (method != .string or !std.mem.eql(u8, method.string, "generated") or instructions != .string or instructions.string.len == 0) return error.InvalidAdjudicationConfig;
        }
        if (self.decider.object.get("decision_method")) |method| {
            if (method != .string or (!std.mem.eql(u8, method.string, "typed") and !std.mem.eql(u8, method.string, "generated") and !std.mem.eql(u8, method.string, "cascade"))) return error.InvalidAdjudicationConfig;
            if (std.mem.eql(u8, method.string, "cascade")) try self.validateCascade();
        }
        if (self.decider.object.get("probabilities")) |probabilities| {
            if (probabilities == .string and std.mem.eql(u8, probabilities.string, "verdict_only")) {
                if (self.accept.min_probability != 0 and self.accept.min_probability != 1) return error.InvalidAdjudicationConfig;
                if (self.reject) |reject| if (reject.min_probability != 0 and reject.min_probability != 1) return error.InvalidAdjudicationConfig;
            }
        }
        inline for (.{ "mention_artifact", "candidate_artifact" }) |field| if (@field(self.description_inputs, field)) |name| {
            if (name.len == 0 or name.len > 256) return error.InvalidAdjudicationConfig;
        };
        for (self.question.choices, 0..) |choice, i| {
            if (choice.value.len == 0) return error.InvalidAdjudicationConfig;
            for (self.question.choices[0..i]) |prior| if (std.mem.eql(u8, prior.value, choice.value)) return error.InvalidAdjudicationConfig;
        }
        try self.validateAcceptance(self.accept);
        if (self.reject) |reject| {
            try self.validateAcceptance(reject);
            if (std.mem.eql(u8, reject.choice, self.accept.choice)) return error.InvalidAdjudicationConfig;
        }
        // The template is a bounded interpolation language, not executable code.
        var pos: usize = 0;
        while (std.mem.indexOfPos(u8, self.template, pos, "{{")) |start| {
            const end = std.mem.indexOfPos(u8, self.template, start + 2, "}}") orelse return error.InvalidAdjudicationConfig;
            const field = std.mem.trim(u8, self.template[start + 2 .. end], " \t\r\n");
            if (!validField(field)) return error.InvalidAdjudicationConfig;
            pos = end + 2;
        }
    }
    /// Every stage can be the answering stage. Shared probability thresholds
    /// remain meaningful for the probabilistic stages; a verdict-only stage's
    /// distribution is separately required to be categorical by classify.
    fn validateCascade(self: Config) !void {
        const body = self.decider.object.get("cascade") orelse return error.InvalidAdjudicationConfig;
        if (body != .object) return error.InvalidAdjudicationConfig;
        const stages = body.object.get("stages") orelse return error.InvalidAdjudicationConfig;
        if (stages != .array or stages.array.items.len == 0) return error.InvalidAdjudicationConfig;
        for (stages.array.items) |stage| {
            if (stage != .object) return error.InvalidAdjudicationConfig;
            const decider = stage.object.get("decider") orelse return error.InvalidAdjudicationConfig;
            if (decider != .object) return error.InvalidAdjudicationConfig;
        }
    }
    fn validateAcceptance(self: Config, acceptance: Acceptance) !void {
        if (!std.math.isFinite(acceptance.min_probability) or acceptance.min_probability < 0 or acceptance.min_probability > 1) return error.InvalidAdjudicationConfig;
        if (acceptance.min_probability < 0.5) {
            const configured = self.decider.object.get("probabilities") orelse return error.InvalidAdjudicationConfig;
            if (configured != .string or !std.mem.eql(u8, configured.string, "verdict_only") or acceptance.min_probability != 0) return error.InvalidAdjudicationConfig;
        }
        for (self.question.choices) |choice| if (std.mem.eql(u8, choice.value, acceptance.choice)) return;
        return error.InvalidAdjudicationConfig;
    }
    fn boundGeneratedBatch(self: Config, a: std.mem.Allocator, decider: *std.json.Value) !void {
        var batch = decider.object.get("batch") orelse std.json.Value{ .object = .empty };
        if (batch != .object) return error.InvalidAdjudicationConfig;
        const items = batch.object.get("max_items_per_call");
        const bytes = batch.object.get("max_input_bytes");
        if (items != null and (items.? != .integer or items.?.integer <= 0)) return error.InvalidAdjudicationConfig;
        if (bytes != null and (bytes.? != .integer or bytes.?.integer <= 0)) return error.InvalidAdjudicationConfig;
        try batch.object.put(a, "max_items_per_call", .{ .integer = @min(self.max_pairs, if (items) |v| v.integer else 8) });
        try batch.object.put(a, "max_input_bytes", .{ .integer = @min(self.max_input_bytes, if (bytes) |v| v.integer else 65536) });
        try decider.object.put(a, "batch", batch);
    }
    /// The producer specification for one cascade stage alone, used to pin
    /// that stage's generated model revision.
    pub fn stageProducerSpecification(self: Config, a: std.mem.Allocator, stage: std.json.Value) ![]u8 {
        var copy = self;
        copy.decider = stage.object.get("decider") orelse return error.InvalidAdjudicationConfig;
        return copy.producerSpecification(a);
    }
    /// Stage decider configurations, in order. Empty unless this is a cascade.
    pub fn cascadeStages(self: Config) []const std.json.Value {
        const method = self.decider.object.get("decision_method") orelse return &.{};
        if (method != .string or !std.mem.eql(u8, method.string, "cascade")) return &.{};
        return self.decider.object.get("cascade").?.object.get("stages").?.array.items;
    }

    pub const RevisionProbe = struct {
        resolved: bool = false,
        revision: ?[32]u8 = null,
        failure: ?anyerror = null,
    };
    pub const RevisionCache = [@import("antfly_decisions").cascade.max_stages]RevisionProbe;

    /// Receipt dependencies are the generated stages that actually answered,
    /// not unreachable stages. With an executor, refresh those dependencies
    /// for reuse; without one, hash the revisions recorded by execution.
    pub fn cascadeRevision(self: Config, a: std.mem.Allocator, response: std.json.Value, executor: ?Executor, cache: *RevisionCache) !?[32]u8 {
        const stages = self.cascadeStages();
        if (stages.len == 0 or response != .object) return error.InvalidAdjudicationOutput;
        const trace = response.object.get("cascade") orelse return error.InvalidAdjudicationOutput;
        if (trace != .object) return error.InvalidAdjudicationOutput;
        const executed = trace.object.get("stages") orelse return error.InvalidAdjudicationOutput;
        if (executed != .array or executed.array.items.len > stages.len or stages.len > cache.len) return error.InvalidAdjudicationOutput;
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        var pinned = false;
        for (executed.array.items, 0..) |entry, i| {
            if (entry != .object) return error.InvalidAdjudicationOutput;
            const name = entry.object.get("name") orelse return error.InvalidAdjudicationOutput;
            const configured_name = stages[i].object.get("name").?.string;
            if (name != .string or !std.mem.eql(u8, name.string, configured_name)) return error.InvalidAdjudicationOutput;
            if (!entry.object.contains("answers")) continue;
            const decider = stages[i].object.get("decider").?;
            const method = decider.object.get("decision_method") orelse continue;
            if (method != .string or !std.mem.eql(u8, method.string, "generated")) continue;
            const revision: ?[32]u8 = if (executor) |current| blk: {
                const resolve = current.model_revision orelse break :blk null;
                const probe = &cache[i];
                if (!probe.resolved) {
                    var timeout = @min(self.timeout_ms, self.budget_ms);
                    const body = self.decider.object.get("cascade").?;
                    if (body.object.get("budget")) |budget| if (budget == .object) if (budget.object.get("deadline_ms")) |deadline| {
                        if (deadline != .integer or deadline.integer <= 0) return error.InvalidAdjudicationConfig;
                        timeout = @intCast(@min(timeout, deadline.integer));
                    };
                    probe.revision = resolve(current.ptr, a, try self.stageProducerSpecification(a, stages[i]), timeout) catch |err| {
                        probe.resolved = true;
                        probe.failure = err;
                        return err;
                    };
                    probe.resolved = true;
                }
                if (probe.failure) |err| return err;
                break :blk probe.revision;
            } else blk: {
                const raw = entry.object.get("model_revision") orelse break :blk null;
                if (raw != .string or raw.string.len != 64) return error.InvalidAdjudicationOutput;
                var digest: [32]u8 = undefined;
                _ = std.fmt.hexToBytes(&digest, raw.string) catch return error.InvalidAdjudicationOutput;
                break :blk digest;
            };
            if (revision) |digest| {
                hasher.update(configured_name);
                hasher.update(&digest);
                pinned = true;
            }
        }
        if (!pinned) return null;
        var digest: [32]u8 = undefined;
        hasher.final(&digest);
        return digest;
    }
    /// Credentials and routing leases do not change the question's meaning.
    /// Clone only the decider JSON; all other policy fields remain immutable.
    pub fn semantic(self: Config, a: std.mem.Allocator) !Config {
        var copy = self;
        const raw = try std.json.Stringify.valueAlloc(a, self.decider, .{});
        defer a.free(raw);
        copy.decider = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{ .allocate = .alloc_always });
        redactCredentials(&copy.decider);
        return copy;
    }
    pub fn producerSpecification(self: Config, a: std.mem.Allocator) ![]u8 {
        var decider = self.decider;
        if (decider.object.get("decision_method")) |method| if (method == .string and (std.mem.eql(u8, method.string, "generated") or std.mem.eql(u8, method.string, "cascade"))) {
            // Clone before adding resolver-specific bounds: registered config is immutable.
            const raw = try std.json.Stringify.valueAlloc(a, decider, .{});
            defer a.free(raw);
            decider = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{ .allocate = .alloc_always });
            if (std.mem.eql(u8, method.string, "generated")) {
                try self.boundGeneratedBatch(a, &decider);
            } else for (decider.object.get("cascade").?.object.get("stages").?.array.items) |*stage| {
                // Generated stages honor the same per-call pair and byte bounds.
                const stage_decider = stage.object.getPtr("decider").?;
                const stage_method = stage_decider.object.get("decision_method") orelse continue;
                if (stage_method == .string and std.mem.eql(u8, stage_method.string, "generated")) try self.boundGeneratedBatch(a, stage_decider);
            }
        };
        var question = self.question;
        if (question.instructions.len == 0) question.instructions = "Use the decider instructions and the named question.";
        return std.json.Stringify.valueAlloc(a, .{ .version = "resolver-adjudication-v1", .decider = decider, .questions = [_]Question{question} }, .{ .emit_null_optional_fields = false });
    }
};

fn redactCredentials(value: *std.json.Value) void {
    switch (value.*) {
        .object => |*fields| {
            for ([_][]const u8{ "api_key", "bearer_token", "credentials_path", "capability_token", "capability_revision" }) |key| _ = fields.swapRemove(key);
            for (fields.values()) |*entry| redactCredentials(entry);
        },
        .array => |*entries| for (entries.items) |*entry| redactCredentials(entry),
        else => {},
    }
}

fn validField(field: []const u8) bool {
    for ([_][]const u8{ "m.text", "m.surface_form", "m.canonical_text", "m.label", "m.description", "c.canonical_name", "c.label", "c.description" }) |allowed| {
        if (std.mem.eql(u8, field, allowed)) return true;
    }
    return false;
}

pub const Inputs = struct {
    mention: []const u8,
    label: []const u8,
    mention_description: ?[]const u8,
    candidate: []const u8,
    candidate_label: []const u8,
    candidate_description: ?[]const u8,

    pub fn render(self: Inputs, a: std.mem.Allocator, config: Config) ![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(a);
        var pos: usize = 0;
        while (std.mem.indexOfPos(u8, config.template, pos, "{{")) |start| {
            const end = std.mem.indexOfPos(u8, config.template, start + 2, "}}") orelse return error.InvalidAdjudicationConfig;
            try append(a, &out, config.template[pos..start], config.max_input_bytes);
            const field = std.mem.trim(u8, config.template[start + 2 .. end], " \t\r\n");
            const value = if (std.mem.eql(u8, field, "m.text") or std.mem.eql(u8, field, "m.canonical_text") or std.mem.eql(u8, field, "m.surface_form")) self.mention else if (std.mem.eql(u8, field, "m.label")) self.label else if (std.mem.eql(u8, field, "m.description")) self.mention_description orelse "" else if (std.mem.eql(u8, field, "c.canonical_name")) self.candidate else if (std.mem.eql(u8, field, "c.label")) self.candidate_label else if (std.mem.eql(u8, field, "c.description")) self.candidate_description orelse "" else return error.InvalidAdjudicationConfig;
            try append(a, &out, value, config.max_input_bytes);
            pos = end + 2;
        }
        try append(a, &out, config.template[pos..], config.max_input_bytes);
        return out.toOwnedSlice(a);
    }
};
fn append(a: std.mem.Allocator, out: *std.ArrayList(u8), value: []const u8, limit: usize) !void {
    if (value.len > limit - out.items.len) return error.AdjudicationInputTooLarge;
    try out.appendSlice(a, value);
}

/// This is authoritative evidence, never part of the expendable work cache.
pub const Status = enum { accepted, rejected, abstained, failed, budget_exhausted };
pub const Evidence = struct {
    version: u32 = 1,
    execution_hash: []const u8,
    specification_hash: []const u8,
    input_hash: []const u8,
    candidate_table: []const u8,
    candidate_key: []const u8,
    candidate_hash: []const u8,
    deterministic_probability: f64,
    status: Status,
    /// The normalized typed decision includes resolved model, distribution, usage
    /// and the decision producer's specification/source provenance.
    response: ?std.json.Value = null,
    failure: ?[]const u8 = null,
};

pub const Request = struct {
    specification_json: []const u8,
    input: []const u8,
    source_fingerprint: []const u8,
    timeout_ms: u32,
};
pub const Result = union(enum) { value: []u8, failure: []const u8 };
pub const Executor = struct {
    ptr: *anyopaque,
    execute: *const fn (*anyopaque, std.mem.Allocator, Request) anyerror![]u8,
    model_revision: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, u32) anyerror!?[32]u8 = null,
    execute_batch: ?*const fn (*anyopaque, std.mem.Allocator, []const Request) anyerror![]Result = null,
    get_description: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8, []const u8) anyerror!?[]u8 = null,
};

/// Prepared policy for a resolution batch. Owns its lowered specification
/// and compiled definition; the source policy must outlive this classifier.
pub const Classifier = struct {
    arena: std.heap.ArenaAllocator,
    config: Config,
    specification_json: []const u8,
    definition: ?@import("antfly_decisions").cascade.Definition,

    pub fn init(backing: std.mem.Allocator, config: Config) !Classifier {
        var arena = std.heap.ArenaAllocator.init(backing);
        errdefer arena.deinit();
        const a = arena.allocator();
        const specification = try config.producerSpecification(a);
        const definition = if (config.cascadeStages().len > 0) blk: {
            const lowered = try std.json.parseFromSliceLeaky(std.json.Value, a, specification, .{});
            break :blk try @import("antfly_decisions").cascade.compile(a, lowered.object.get("decider").?.object.get("cascade").?);
        } else null;
        return .{ .arena = arena, .config = config, .specification_json = specification, .definition = definition };
    }

    pub fn deinit(self: *Classifier) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn classify(self: *const Classifier, response: std.json.Value) !Status {
        return classifyResponse(self.config, response, if (self.definition) |*definition| definition else null);
    }
};

fn answeringDecider(config: Config, response: std.json.Value, definition: ?*const @import("antfly_decisions").cascade.Definition) anyerror!std.json.Value {
    const stages = config.cascadeStages();
    if (stages.len == 0) return config.decider;
    const compiled = definition orelse return error.InvalidAdjudicationOutput;
    const index = @import("antfly_decisions").cascade.validateTrace(compiled, response) catch return error.InvalidAdjudicationOutput;
    // Independently validate every successful stage's distribution.
    const executed = response.object.get("cascade").?.object.get("stages").?.array.items;
    for (executed, stages[0..executed.len]) |entry, stage| {
        if (!entry.object.contains("answers")) continue;
        var stage_config = config;
        stage_config.decider = stage.object.get("decider").?;
        _ = try classifyResponse(stage_config, entry, null);
    }
    return stages[index].object.get("decider").?;
}

/// Only a well-formed, typed, normalized choice distribution can change identity.
pub fn classify(a: std.mem.Allocator, config: Config, response: std.json.Value) !Status {
    if (config.cascadeStages().len == 0) return classifyResponse(config, response, null);
    var classifier = try Classifier.init(a, config);
    defer classifier.deinit();
    return classifier.classify(response);
}

fn classifyResponse(config: Config, response: std.json.Value, definition: ?*const @import("antfly_decisions").cascade.Definition) !Status {
    if (response != .object) return error.InvalidAdjudicationOutput;
    const model = response.object.get("model") orelse return error.InvalidAdjudicationOutput;
    if (model != .string or model.string.len == 0) return error.InvalidAdjudicationOutput;
    const answers = response.object.get("answers") orelse return error.InvalidAdjudicationOutput;
    if (answers != .array or answers.array.items.len != 1) return error.InvalidAdjudicationOutput;
    const answer = answers.array.items[0];
    if (answer != .object) return error.InvalidAdjudicationOutput;
    const name = answer.object.get("name") orelse return error.InvalidAdjudicationOutput;
    const kind = answer.object.get("type") orelse return error.InvalidAdjudicationOutput;
    const method = answer.object.get("decision_method") orelse return error.InvalidAdjudicationOutput;
    if (name != .string or !std.mem.eql(u8, name.string, config.question.name) or kind != .string or !std.mem.eql(u8, kind.string, "choice") or method != .string or (!std.mem.eql(u8, method.string, "typed") and !std.mem.eql(u8, method.string, "generated"))) return error.InvalidAdjudicationOutput;
    // A cascade answer is checked against the stage that produced it.
    const answering = try answeringDecider(config, response, definition);
    const expected_method = answering.object.get("decision_method") orelse std.json.Value{ .string = "typed" };
    if (expected_method != .string or !std.mem.eql(u8, method.string, expected_method.string)) return error.InvalidAdjudicationOutput;
    var verdict_only = false;
    if (std.mem.eql(u8, method.string, "generated")) {
        const source = answer.object.get("probability_source") orelse return error.InvalidAdjudicationOutput;
        const policy = answering.object.get("probabilities") orelse std.json.Value{ .string = "token_logprobs" };
        verdict_only = policy == .string and std.mem.eql(u8, policy.string, "verdict_only");
        if (source != .string or !std.mem.eql(u8, source.string, if (verdict_only) "verdict" else "token_logprobs")) return error.InvalidAdjudicationOutput;
        if (!answer.object.contains("raw_structured_answer")) return error.InvalidAdjudicationOutput;
    }
    const probabilities = answer.object.get("probabilities") orelse return error.InvalidAdjudicationOutput;
    if (probabilities != .array or probabilities.array.items.len != config.question.choices.len) return error.InvalidAdjudicationOutput;
    const selected = answer.object.get("choice") orelse return error.InvalidAdjudicationOutput;
    if (selected != .string) return error.InvalidAdjudicationOutput;
    var selected_probability: ?f64 = null;
    var maximum: f64 = 0;
    var accepted: f64 = 0;
    var rejected: f64 = 0;
    var total: f64 = 0;
    for (config.question.choices) |choice| {
        var found = false;
        for (probabilities.array.items) |entry| {
            if (entry != .object) return error.InvalidAdjudicationOutput;
            const value = entry.object.get("value") orelse return error.InvalidAdjudicationOutput;
            if (value != .string) return error.InvalidAdjudicationOutput;
            if (!std.mem.eql(u8, value.string, choice.value)) continue;
            if (found) return error.InvalidAdjudicationOutput;
            found = true;
            const raw = entry.object.get("probability") orelse return error.InvalidAdjudicationOutput;
            const probability: f64 = switch (raw) {
                .float => raw.float,
                .integer => @floatFromInt(raw.integer),
                else => return error.InvalidAdjudicationOutput,
            };
            if (!std.math.isFinite(probability) or probability < 0 or probability > 1) return error.InvalidAdjudicationOutput;
            if (verdict_only and probability != 0 and probability != 1) return error.InvalidAdjudicationOutput;
            total += probability;
            maximum = @max(maximum, probability);
            if (std.mem.eql(u8, selected.string, choice.value)) selected_probability = probability;
            if (std.mem.eql(u8, choice.value, config.accept.choice)) accepted = probability;
            if (config.reject) |reject| if (std.mem.eql(u8, choice.value, reject.choice)) {
                rejected = probability;
            };
        }
        if (!found) return error.InvalidAdjudicationOutput;
    }
    if (@abs(total - 1) > 0.001 or (selected_probability orelse return error.InvalidAdjudicationOutput) != maximum) return error.InvalidAdjudicationOutput;
    if (definition != null and try @import("antfly_decisions").cascade.agreementBlocked(response)) return .abstained;
    if (accepted >= config.accept.min_probability and accepted > 0.5) return .accepted;
    if (config.reject) |reject| if (rejected >= reject.min_probability and rejected > 0.5) return .rejected;
    return .abstained;
}
