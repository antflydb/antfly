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

//! Decider cascades: an ordered list of deciders where a later stage runs only
//! for items whose previous answer has a probability inside a configured band.
//!
//! This module owns the definition contract, compilation, band evaluation and
//! per-item trace assembly. Stage execution is injected, so every host that
//! executes named deciders shares one implementation. Escalation is a policy
//! on a successful answer; it is never a failover path.
const std = @import("std");
const A = std.mem.Allocator;
const V = std.json.Value;

pub const max_stages = 4;
/// Matches the gateway's chain ceiling of eight total attempts.
pub const max_model_calls_limit = 8;
pub const max_deadline_ms: u64 = 600_000;
pub const max_stage_name_bytes = 64;

/// Credential-bearing fields. They never contribute to cascade identity, so
/// rotating a key does not look like a behavioral change.
pub const credential_fields = [_][]const u8{ "api_key", "bearer_token", "credentials_path", "capability_token", "capability_revision" };

pub const Method = enum { typed, generated };
pub const OnFailure = enum { fail, keep_previous };

pub const Band = struct {
    /// Restrict the rule to one question. Omitted: any question in the band.
    question: ?[]const u8 = null,
    /// For choice questions, measure this value's probability. Omitted: the
    /// probability of the selected value. Ignored for predicates.
    choice: ?[]const u8 = null,
    low: f64,
    high: f64,

    pub fn contains(self: Band, p: f64) bool {
        return p >= self.low and p <= self.high;
    }
};

pub const Stage = struct {
    name: []const u8,
    /// Inline decider configuration, validated by the executing host.
    decider: V,
    method: Method,
    when: ?Band = null,
    on_failure: OnFailure = .fail,
};

pub const Budget = struct {
    max_model_calls: u32,
    deadline_ms: ?u64 = null,
};

pub const Definition = struct {
    stages: []const Stage,
    budget: Budget,
    /// Hex SHA-256 over the canonical compiled definition, excluding
    /// credentials. Any behavior-affecting change changes this identity.
    sha256: [64]u8,
};

fn object(v: V) !std.json.ObjectMap {
    return if (v == .object) v.object else error.InvalidDecisionCascade;
}

fn text(v: V) ![]const u8 {
    if (v != .string or v.string.len == 0 or v.string.len > 2048 or !std.unicode.utf8ValidateSlice(v.string)) return error.InvalidDecisionCascade;
    return v.string;
}

fn keys(o: std.json.ObjectMap, allowed: []const []const u8) !void {
    for (o.keys()) |key| {
        for (allowed) |candidate| {
            if (std.mem.eql(u8, key, candidate)) break;
        } else return error.InvalidDecisionCascade;
    }
}

fn probabilityValue(v: V) !f64 {
    const n: f64 = switch (v) {
        .float => v.float,
        .integer => @floatFromInt(v.integer),
        else => return error.InvalidDecisionCascade,
    };
    if (!std.math.isFinite(n) or n < 0 or n > 1) return error.InvalidDecisionCascade;
    return n;
}

fn positive(v: V, max: u64) !u64 {
    if (v != .integer or v.integer < 1 or v.integer > max) return error.InvalidDecisionCascade;
    return @intCast(v.integer);
}

/// Stage names become part of derived registry keys and traces.
pub fn validStageName(name: []const u8) bool {
    if (name.len == 0 or name.len > max_stage_name_bytes) return false;
    for (name) |c| if (!std.ascii.isAlphanumeric(c) and c != '_' and c != '-') return false;
    return true;
}

pub fn methodOf(decider: V) !Method {
    const fields = try object(decider);
    const raw = fields.get("decision_method") orelse return .typed;
    // Similarity scores are not probabilities, and nesting would hide stages.
    return std.meta.stringToEnum(Method, try text(raw)) orelse error.InvalidDecisionCascade;
}

/// Compile and validate `{"stages":[...], "budget":{...}}`. Unknown fields,
/// repeated names, misplaced bands and unmeasurable bands are rejected.
pub fn compile(a: A, value: V) !Definition {
    const root = try object(value);
    try keys(root, &.{ "stages", "budget" });
    const raw_stages = root.get("stages") orelse return error.InvalidDecisionCascade;
    if (raw_stages != .array or raw_stages.array.items.len < 2 or raw_stages.array.items.len > max_stages) return error.InvalidDecisionCascade;
    const stages = try a.alloc(Stage, raw_stages.array.items.len);
    for (raw_stages.array.items, stages, 0..) |raw, *stage, i| {
        const fields = try object(raw);
        try keys(fields, &.{ "name", "decider", "when", "on_failure" });
        const name = try text(fields.get("name") orelse return error.InvalidDecisionCascade);
        if (!validStageName(name)) return error.InvalidDecisionCascade;
        for (stages[0..i]) |other| if (std.mem.eql(u8, other.name, name)) return error.InvalidDecisionCascade;
        const decider = fields.get("decider") orelse return error.InvalidDecisionCascade;
        stage.* = .{ .name = name, .decider = decider, .method = try methodOf(decider) };
        if (fields.get("on_failure")) |policy| stage.on_failure = std.meta.stringToEnum(OnFailure, try text(policy)) orelse return error.InvalidDecisionCascade;
        if (i == 0) {
            // The first stage answers every item and has nothing to fall back to.
            if (fields.contains("when") or fields.contains("on_failure")) return error.InvalidDecisionCascade;
            continue;
        }
        const when = try object(fields.get("when") orelse return error.InvalidDecisionCascade);
        try keys(when, &.{ "question", "choice", "probability_between" });
        const bounds = when.get("probability_between") orelse return error.InvalidDecisionCascade;
        if (bounds != .array or bounds.array.items.len != 2) return error.InvalidDecisionCascade;
        const low = try probabilityValue(bounds.array.items[0]);
        const high = try probabilityValue(bounds.array.items[1]);
        if (!(low < high)) return error.InvalidDecisionCascade;
        stage.when = .{
            .question = if (when.get("question")) |q| try text(q) else null,
            .choice = if (when.get("choice")) |c| try text(c) else null,
            .low = low,
            .high = high,
        };
        // A band needs a measured probability from the stage it inspects.
        const previous = stages[i - 1];
        if (previous.method == .generated) if (previous.decider.object.get("probabilities")) |p| {
            if (p == .string and std.mem.eql(u8, p.string, "verdict_only")) return error.InvalidDecisionCascade;
        };
    }

    var budget = Budget{ .max_model_calls = @intCast(stages.len) };
    if (root.get("budget")) |raw_budget| {
        const fields = try object(raw_budget);
        try keys(fields, &.{ "max_model_calls", "deadline_ms" });
        if (fields.get("max_model_calls")) |calls| budget.max_model_calls = @intCast(try positive(calls, max_model_calls_limit));
        if (fields.get("deadline_ms")) |deadline| budget.deadline_ms = try positive(deadline, max_deadline_ms);
    }
    if (budget.max_model_calls < stages.len) return error.InvalidDecisionCascade;

    var definition = Definition{ .stages = stages, .budget = budget, .sha256 = undefined };
    definition.sha256 = try identity(a, &definition);
    return definition;
}

/// Canonical identity. Field order is fixed and objects are key-sorted, so
/// whitespace and source order do not change it, while every behavioral field
/// does. Credentials are excluded.
fn identity(a: A, definition: *const Definition) ![64]u8 {
    var out: std.Io.Writer.Allocating = .init(a);
    defer out.deinit();
    var s: std.json.Stringify = .{ .writer = &out.writer };
    try s.beginObject();
    try s.objectField("version");
    try s.write(@as(u32, 1));
    try s.objectField("stages");
    try s.beginArray();
    for (definition.stages) |stage| {
        try s.beginObject();
        try s.objectField("name");
        try s.write(stage.name);
        try s.objectField("decider");
        try writeCanonical(&s, stage.decider);
        try s.objectField("on_failure");
        try s.write(@tagName(stage.on_failure));
        if (stage.when) |when| {
            try s.objectField("when");
            try s.beginObject();
            if (when.question) |q| {
                try s.objectField("question");
                try s.write(q);
            }
            if (when.choice) |c| {
                try s.objectField("choice");
                try s.write(c);
            }
            try s.objectField("probability_between");
            try s.write([2]f64{ when.low, when.high });
            try s.endObject();
        }
        try s.endObject();
    }
    try s.endArray();
    try s.objectField("budget");
    try s.beginObject();
    try s.objectField("max_model_calls");
    try s.write(definition.budget.max_model_calls);
    if (definition.budget.deadline_ms) |deadline| {
        try s.objectField("deadline_ms");
        try s.write(deadline);
    }
    try s.endObject();
    try s.endObject();
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(out.written(), &digest, .{});
    return std.fmt.bytesToHex(digest, .lower);
}

fn isCredential(name: []const u8) bool {
    for (credential_fields) |field| if (std.mem.eql(u8, field, name)) return true;
    return false;
}

fn writeCanonical(s: *std.json.Stringify, value: V) !void {
    switch (value) {
        .object => |o| {
            var sorted: [128][]const u8 = undefined;
            if (o.count() > sorted.len) return error.InvalidDecisionCascade;
            var count: usize = 0;
            for (o.keys()) |key| if (!isCredential(key)) {
                sorted[count] = key;
                count += 1;
            };
            const names = sorted[0..count];
            std.mem.sort([]const u8, names, {}, struct {
                fn lessThan(_: void, l: []const u8, r: []const u8) bool {
                    return std.mem.lessThan(u8, l, r);
                }
            }.lessThan);
            try s.beginObject();
            for (names) |name| {
                try s.objectField(name);
                try writeCanonical(s, o.get(name).?);
            }
            try s.endObject();
        },
        .array => |items| {
            try s.beginArray();
            for (items.items) |item| try writeCanonical(s, item);
            try s.endArray();
        },
        else => try s.write(value),
    }
}

// ---------------------------------------------------------------------------
// Band evaluation over public answer arrays
// ---------------------------------------------------------------------------

fn findAnswer(answers: V, name: []const u8) ?V {
    if (answers != .array) return null;
    for (answers.array.items) |answer| {
        if (answer != .object) continue;
        const n = answer.object.get("name") orelse continue;
        if (n == .string and std.mem.eql(u8, n.string, name)) return answer;
    }
    return null;
}

fn number(v: V) !f64 {
    return switch (v) {
        .float => v.float,
        .integer => @floatFromInt(v.integer),
        else => error.InvalidDecisionOutput,
    };
}

/// Probability the band measures for one public answer. Similarity-only
/// answers have no probability and cannot drive escalation.
pub fn answerProbability(answer: V, choice: ?[]const u8) !f64 {
    if (answer != .object) return error.InvalidDecisionOutput;
    const kind = answer.object.get("type") orelse return error.InvalidDecisionOutput;
    if (kind != .string) return error.InvalidDecisionOutput;
    if (std.mem.eql(u8, kind.string, "predicate")) {
        return number(answer.object.get("probability") orelse return error.UnsupportedDecisionCascadeAnswer);
    }
    if (!std.mem.eql(u8, kind.string, "choice")) return error.UnsupportedDecisionCascadeAnswer;
    const probabilities = answer.object.get("probabilities") orelse return error.UnsupportedDecisionCascadeAnswer;
    const target = choice orelse blk: {
        const selected = answer.object.get("choice") orelse return error.InvalidDecisionOutput;
        if (selected != .string) return error.UnsupportedDecisionCascadeAnswer;
        break :blk selected.string;
    };
    if (probabilities != .array) return error.InvalidDecisionOutput;
    for (probabilities.array.items) |entry| {
        if (entry != .object) return error.InvalidDecisionOutput;
        const value = entry.object.get("value") orelse continue;
        if (value == .string and std.mem.eql(u8, value.string, target)) return number(entry.object.get("probability") orelse return error.InvalidDecisionOutput);
    }
    return error.InvalidDecisionOutput;
}

pub fn inBand(band: Band, answers: V) !bool {
    if (band.question) |name| {
        const answer = findAnswer(answers, name) orelse return error.InvalidDecisionOutput;
        return band.contains(try answerProbability(answer, band.choice));
    }
    if (answers != .array) return error.InvalidDecisionOutput;
    for (answers.array.items) |answer| {
        const kind = answer.object.get("type") orelse return error.InvalidDecisionOutput;
        // A named choice value only applies to choice answers.
        const choice = if (kind == .string and std.mem.eql(u8, kind.string, "choice")) band.choice else null;
        if (band.contains(try answerProbability(answer, choice))) return true;
    }
    return false;
}

/// Bind selectors to the validated internal question schema before any model
/// runs. Every answer inspected by an unqualified band must be measurable;
/// otherwise validity would depend on the order and values of model answers.
pub fn validateBands(definition: *const Definition, questions: V) !void {
    if (questions != .object) return error.InvalidDecisionCascade;
    for (definition.stages[1..]) |stage| {
        const band = stage.when.?;
        if (band.question) |name| {
            try validateBandQuestion(band, questions.object.get(name) orelse return error.InvalidDecisionCascade);
        } else for (questions.object.values()) |question| try validateBandQuestion(band, question);
    }
}

fn validateBandQuestion(band: Band, question: V) !void {
    if (question != .object) return error.InvalidDecisionCascade;
    const kind = question.object.get("type") orelse return error.InvalidDecisionCascade;
    if (kind != .string) return error.InvalidDecisionCascade;
    if (std.mem.eql(u8, kind.string, "noul") or std.mem.eql(u8, kind.string, "predicate")) return;
    if (!std.mem.eql(u8, kind.string, "choice")) return error.InvalidDecisionCascade;
    if (band.choice) |choice| {
        const criteria = question.object.get("criteria") orelse return error.InvalidDecisionCascade;
        if (criteria != .object or !criteria.object.contains(choice)) return error.InvalidDecisionCascade;
    }
}

/// Validate a successful composite receipt against the definition that was
/// actually executed, including escalation and explicit keep_previous stops.
/// Returns the index of the stage responsible for the public answer.
pub fn validateTrace(definition: *const Definition, response: V) !usize {
    if (response != .object) return error.InvalidDecisionOutput;
    const cascade = try object(response.object.get("cascade") orelse return error.InvalidDecisionOutput);
    const digest = cascade.get("definition_sha256") orelse return error.InvalidDecisionOutput;
    if (digest != .string or !std.mem.eql(u8, digest.string, &definition.sha256)) return error.InvalidDecisionOutput;
    const executed = cascade.get("stages") orelse return error.InvalidDecisionOutput;
    const answered = cascade.get("answered_by") orelse return error.InvalidDecisionOutput;
    if (executed != .array or executed.array.items.len == 0 or executed.array.items.len > definition.stages.len or answered != .string) return error.InvalidDecisionOutput;
    var last: ?usize = null;
    for (executed.array.items, 0..) |entry, index| {
        const fields = try object(entry);
        const stage = definition.stages[index];
        const name = fields.get("name") orelse return error.InvalidDecisionOutput;
        const method = fields.get("decision_method") orelse return error.InvalidDecisionOutput;
        const escalated = fields.get("escalated") orelse return error.InvalidDecisionOutput;
        if (name != .string or !std.mem.eql(u8, name.string, stage.name) or method != .string or !std.mem.eql(u8, method.string, @tagName(stage.method)) or escalated != .bool) return error.InvalidDecisionOutput;
        const has_next = index + 1 < executed.array.items.len;
        if (fields.get("error")) |err| {
            if (err != .string or err.string.len == 0 or fields.contains("answers") or index == 0 or has_next or escalated.bool or stage.on_failure != .keep_previous) return error.InvalidDecisionOutput;
            continue;
        }
        const answers = fields.get("answers") orelse return error.InvalidDecisionOutput;
        if (answers != .array or answers.array.items.len == 0) return error.InvalidDecisionOutput;
        const should_escalate = if (index + 1 < definition.stages.len) try inBand(definition.stages[index + 1].when.?, answers) else false;
        if (escalated.bool != should_escalate or has_next != should_escalate) return error.InvalidDecisionOutput;
        last = index;
    }
    const index = last orelse return error.InvalidDecisionOutput;
    const final = executed.array.items[index].object;
    if (!std.mem.eql(u8, answered.string, definition.stages[index].name) or
        !jsonEqual(final.get("answers").?, response.object.get("answers") orelse return error.InvalidDecisionOutput) or
        !jsonEqual(final.get("model") orelse return error.InvalidDecisionOutput, response.object.get("model") orelse return error.InvalidDecisionOutput)) return error.InvalidDecisionOutput;
    const revision = final.get("model_revision");
    const public_revision = response.object.get("model_revision");
    if ((revision == null) != (public_revision == null)) return error.InvalidDecisionOutput;
    if (revision) |v| if (!jsonEqual(v, public_revision.?)) return error.InvalidDecisionOutput;
    return index;
}

fn jsonEqual(lhs: V, rhs: V) bool {
    if ((lhs == .integer or lhs == .float) and (rhs == .integer or rhs == .float)) return (number(lhs) catch return false) == (number(rhs) catch return false);
    if (std.meta.activeTag(lhs) != std.meta.activeTag(rhs)) return false;
    return switch (lhs) {
        .null => true,
        .bool => lhs.bool == rhs.bool,
        .string => std.mem.eql(u8, lhs.string, rhs.string),
        .number_string => std.mem.eql(u8, lhs.number_string, rhs.number_string),
        .integer => lhs.integer == rhs.integer,
        .float => lhs.float == rhs.float,
        .array => blk: {
            if (lhs.array.items.len != rhs.array.items.len) break :blk false;
            for (lhs.array.items, rhs.array.items) |l, r| if (!jsonEqual(l, r)) break :blk false;
            break :blk true;
        },
        .object => blk: {
            if (lhs.object.count() != rhs.object.count()) break :blk false;
            for (lhs.object.keys(), lhs.object.values()) |key, l| if (!jsonEqual(l, rhs.object.get(key) orelse break :blk false)) break :blk false;
            break :blk true;
        },
    };
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// One stage's normalized public responses (`{model, answers, usage, ...}`)
/// for the selected items, positionally aligned with the requested indices.
pub const StageResult = struct {
    values: []const ?V,
    failures: []const ?anyerror,
};

pub const Runner = struct {
    ptr: *anyopaque,
    run: *const fn (ptr: *anyopaque, a: A, stage_index: usize, stage: Stage, indices: []const usize) anyerror!StageResult,
    /// Called before each stage; return an error to stop (deadline/cancel).
    checkpoint: ?*const fn (ptr: *anyopaque) anyerror!void = null,
};

/// Per-item final response, or the error that ended the item.
pub const Outcome = struct {
    values: []?V,
    failures: []?anyerror,
};

const Row = struct {
    response: ?V = null,
    failure: ?anyerror = null,
    input_tokens: u64 = 0,
    output_tokens: u64 = 0,
    stages: std.array_list.Managed(V),
};

/// Never converted into a keep_previous answer.
pub fn isFatal(err: anyerror) bool {
    return switch (err) {
        error.OutOfMemory, error.Cancelled, error.Canceled, error.Timeout, error.DeadlineExceeded, error.DecisionCascadeBudgetExceeded => true,
        else => false,
    };
}

fn usageField(usage: ?V, field: []const u8) u64 {
    const u = usage orelse return 0;
    if (u != .object) return 0;
    const v = u.object.get(field) orelse return 0;
    return if (v == .integer and v.integer > 0) @intCast(v.integer) else 0;
}

fn put(a: A, o: *std.json.ObjectMap, key: []const u8, value: V) !void {
    try o.put(a, key, value);
}

fn markEscalated(a: A, row: *Row, escalated: bool) !void {
    if (row.stages.items.len == 0) return;
    const last = &row.stages.items[row.stages.items.len - 1];
    if (!last.object.contains("escalated")) try last.object.put(a, "escalated", .{ .bool = escalated });
}

/// Run the cascade over `item_count` items. Every stage call carries the
/// escalating items together. Each item's response is the last successful
/// stage's answer with a complete trace of the stages that ran for it.
pub fn execute(a: A, definition: *const Definition, name: []const u8, item_count: usize, runner: Runner) !Outcome {
    const rows = try a.alloc(Row, item_count);
    for (rows) |*row| row.* = .{ .stages = .init(a) };
    var active: std.array_list.Managed(usize) = .init(a);
    for (0..item_count) |i| try active.append(i);
    var calls: u32 = 0;

    for (definition.stages, 0..) |stage, stage_index| {
        if (stage_index > 0) {
            var next: std.array_list.Managed(usize) = .init(a);
            for (active.items) |index| {
                const row = &rows[index];
                const response = row.response orelse continue;
                const escalate = try inBand(stage.when.?, response.object.get("answers") orelse return error.InvalidDecisionOutput);
                try markEscalated(a, row, escalate);
                if (escalate) try next.append(index);
            }
            active.deinit();
            active = next;
        }
        if (active.items.len == 0) break;
        if (calls >= definition.budget.max_model_calls) return error.DecisionCascadeBudgetExceeded;
        if (runner.checkpoint) |check| try check(runner.ptr);
        calls += 1;
        const result = runner.run(runner.ptr, a, stage_index, stage, active.items) catch |err| {
            if (isFatal(err)) return err;
            for (active.items) |index| {
                const row = &rows[index];
                try row.stages.append(try trace(a, stage, null, null, err));
                if (stage_index == 0 or stage.on_failure == .fail) {
                    row.response = null;
                    row.failure = err;
                }
            }
            break;
        };
        if (result.values.len != active.items.len or result.failures.len != active.items.len) return error.InvalidDecisionOutput;
        var still: std.array_list.Managed(usize) = .init(a);
        for (active.items, result.values, result.failures) |index, value, failure| {
            const row = &rows[index];
            if (value) |response| {
                if (response != .object or response.object.get("answers") == null) return error.InvalidDecisionOutput;
                const usage = response.object.get("usage");
                row.input_tokens +|= usageField(usage, "input_tokens");
                row.output_tokens +|= usageField(usage, "output_tokens");
                try row.stages.append(try trace(a, stage, response, usage, null));
                row.response = response;
                row.failure = null;
                try still.append(index);
                continue;
            }
            const err = failure orelse error.InvalidDecisionOutput;
            if (isFatal(err)) return err;
            try row.stages.append(try trace(a, stage, null, null, err));
            // keep_previous retains the previous answer; the item stops here.
            if (stage_index > 0 and stage.on_failure == .keep_previous) continue;
            row.response = null;
            row.failure = err;
        }
        active.deinit();
        active = still;
    }

    const out = Outcome{ .values = try a.alloc(?V, item_count), .failures = try a.alloc(?anyerror, item_count) };
    for (rows, out.values, out.failures) |*row, *value, *failure| {
        try markEscalated(a, row, false);
        value.* = null;
        failure.* = row.failure;
        const final = row.response orelse {
            if (failure.* == null) failure.* = error.InvalidDecisionOutput;
            continue;
        };
        value.* = try assemble(a, definition, name, row, final);
    }
    return out;
}

fn trace(a: A, stage: Stage, response: ?V, usage: ?V, err: ?anyerror) !V {
    var entry = std.json.ObjectMap{};
    try put(a, &entry, "name", .{ .string = stage.name });
    try put(a, &entry, "decision_method", .{ .string = @tagName(stage.method) });
    if (response) |value| {
        if (value.object.get("model")) |model| try put(a, &entry, "model", model);
        if (value.object.get("model_revision")) |revision| try put(a, &entry, "model_revision", revision);
        try put(a, &entry, "answers", value.object.get("answers").?);
        var u = std.json.ObjectMap{};
        try put(a, &u, "input_tokens", .{ .integer = @intCast(usageField(usage, "input_tokens")) });
        try put(a, &u, "output_tokens", .{ .integer = @intCast(usageField(usage, "output_tokens")) });
        try put(a, &entry, "usage", .{ .object = u });
        if (value.object.get("provenance")) |provenance| try put(a, &entry, "provenance", provenance);
    }
    if (err) |e| try put(a, &entry, "error", .{ .string = @errorName(e) });
    return .{ .object = entry };
}

fn assemble(a: A, definition: *const Definition, name: []const u8, row: *Row, final: V) !V {
    var root = std.json.ObjectMap{};
    // Retain the answering stage's metadata (model, identity, provenance).
    for (final.object.keys(), final.object.values()) |key, value| {
        if (std.mem.eql(u8, key, "usage") or std.mem.eql(u8, key, "cascade")) continue;
        try put(a, &root, key, value);
    }
    var usage = std.json.ObjectMap{};
    try put(a, &usage, "input_tokens", .{ .integer = @intCast(row.input_tokens) });
    try put(a, &usage, "output_tokens", .{ .integer = @intCast(row.output_tokens) });
    try put(a, &root, "usage", .{ .object = usage });
    var cascade = std.json.ObjectMap{};
    try put(a, &cascade, "decider", .{ .string = name });
    try put(a, &cascade, "definition_sha256", .{ .string = try a.dupe(u8, &definition.sha256) });
    var answered: []const u8 = "";
    for (row.stages.items) |stage| if (stage.object.contains("answers")) {
        answered = stage.object.get("name").?.string;
    };
    try put(a, &cascade, "answered_by", .{ .string = answered });
    try put(a, &cascade, "stages", .{ .array = row.stages });
    try put(a, &root, "cascade", .{ .object = cascade });
    return .{ .object = root };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const example =
    \\{"stages":[
    \\   {"name":"fast","decider":{"provider":"jev","model":"jev-latest"}},
    \\   {"name":"strong","decider":{"decision_method":"generated","generator":{"provider":"anthropic","model":"strong-gen","api_key":"k"}},
    \\    "when":{"question":"same","choice":"same","probability_between":[0.3,0.9]}}],
    \\ "budget":{"max_model_calls":2,"deadline_ms":10000}}
;

fn compileJson(a: A, bytes: []const u8) !Definition {
    return compile(a, try std.json.parseFromSliceLeaky(V, a, bytes, .{}));
}

test "decision cascade compiles and rejects malformed definitions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const d = try compileJson(a, example);
    try std.testing.expectEqual(@as(usize, 2), d.stages.len);
    try std.testing.expectEqual(Method.typed, d.stages[0].method);
    try std.testing.expectEqual(Method.generated, d.stages[1].method);
    try std.testing.expectEqual(@as(f64, 0.3), d.stages[1].when.?.low);
    try std.testing.expectEqual(@as(?u64, 10000), d.budget.deadline_ms);

    const bad = [_][]const u8{
        // unknown field
        \\{"stages":[{"name":"a","decider":{"model":"x"}},{"name":"b","decider":{"model":"y"},"when":{"probability_between":[0.1,0.9]}}],"extra":1}
        ,
        // a single stage is not a cascade
        \\{"stages":[{"name":"a","decider":{"model":"x"}}]}
        ,
        // repeated name
        \\{"stages":[{"name":"a","decider":{"model":"x"}},{"name":"a","decider":{"model":"y"},"when":{"probability_between":[0.1,0.9]}}]}
        ,
        // unsafe stage name
        \\{"stages":[{"name":"a#b","decider":{"model":"x"}},{"name":"b","decider":{"model":"y"},"when":{"probability_between":[0.1,0.9]}}]}
        ,
        // first stage with a band
        \\{"stages":[{"name":"a","decider":{"model":"x"},"when":{"probability_between":[0.1,0.9]}},{"name":"b","decider":{"model":"y"},"when":{"probability_between":[0.1,0.9]}}]}
        ,
        // later stage without a band
        \\{"stages":[{"name":"a","decider":{"model":"x"}},{"name":"b","decider":{"model":"y"}}]}
        ,
        // inverted band
        \\{"stages":[{"name":"a","decider":{"model":"x"}},{"name":"b","decider":{"model":"y"},"when":{"probability_between":[0.9,0.1]}}]}
        ,
        // band outside [0,1]
        \\{"stages":[{"name":"a","decider":{"model":"x"}},{"name":"b","decider":{"model":"y"},"when":{"probability_between":[0.1,1.5]}}]}
        ,
        // budget below the stage count
        \\{"stages":[{"name":"a","decider":{"model":"x"}},{"name":"b","decider":{"model":"y"},"when":{"probability_between":[0.1,0.9]}}],"budget":{"max_model_calls":1}}
        ,
        // budget above the chain ceiling
        \\{"stages":[{"name":"a","decider":{"model":"x"}},{"name":"b","decider":{"model":"y"},"when":{"probability_between":[0.1,0.9]}}],"budget":{"max_model_calls":9}}
        ,
        // band over an unmeasured verdict-only generated stage
        \\{"stages":[{"name":"a","decider":{"decision_method":"generated","generator":{"model":"g"},"probabilities":"verdict_only"}},{"name":"b","decider":{"model":"y"},"when":{"probability_between":[0.1,0.9]}}]}
        ,
        // similarity stages have no probabilities
        \\{"stages":[{"name":"a","decider":{"decision_method":"embedding_similarity","model":"e"}},{"name":"b","decider":{"model":"y"},"when":{"probability_between":[0.1,0.9]}}]}
        ,
        // nested cascades
        \\{"stages":[{"name":"a","decider":{"model":"x"}},{"name":"b","decider":{"decision_method":"cascade"},"when":{"probability_between":[0.1,0.9]}}]}
        ,
        // first stage cannot fall back
        \\{"stages":[{"name":"a","decider":{"model":"x"},"on_failure":"keep_previous"},{"name":"b","decider":{"model":"y"},"when":{"probability_between":[0.1,0.9]}}]}
        ,
    };
    for (bad) |bytes| try std.testing.expectError(error.InvalidDecisionCascade, compileJson(a, bytes));
}

test "decision cascade identity ignores formatting and credentials and tracks behavior" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const base = try compileJson(a, example);
    const reordered = try compileJson(a,
        \\{"budget":{"deadline_ms":10000,"max_model_calls":2},
        \\ "stages":[{"decider":{"model":"jev-latest","provider":"jev"},"name":"fast"},
        \\   {"when":{"probability_between":[0.3,0.9],"choice":"same","question":"same"},"name":"strong",
        \\    "decider":{"generator":{"api_key":"rotated","model":"strong-gen","provider":"anthropic"},"decision_method":"generated"}}]}
    );
    try std.testing.expectEqualStrings(&base.sha256, &reordered.sha256);
    const changed = try compileJson(a,
        \\{"stages":[
        \\   {"name":"fast","decider":{"provider":"jev","model":"jev-latest"}},
        \\   {"name":"strong","decider":{"decision_method":"generated","generator":{"provider":"anthropic","model":"strong-gen"}},
        \\    "when":{"question":"same","choice":"same","probability_between":[0.2,0.9]}}],
        \\ "budget":{"max_model_calls":2,"deadline_ms":10000}}
    );
    try std.testing.expect(!std.mem.eql(u8, &base.sha256, &changed.sha256));
}

const Fake = struct {
    /// Probability of "same" the fast stage returns, per item index.
    fast: []const f64,
    strong_same: f64 = 0.04,
    strong_fails: bool = false,
    strong_item_failure: ?usize = null,
    calls: usize = 0,
    strong_items: usize = 0,

    fn response(a: A, model: []const u8, method: []const u8, p_same: f64) !V {
        const body = try std.fmt.allocPrint(a,
            \\{{"model":"{s}","answers":[{{"name":"same","type":"choice","decision_method":"{s}","choice":"{s}","probabilities":[{{"value":"same","probability":{d}}},{{"value":"different","probability":{d}}}]}}],"usage":{{"input_tokens":10,"output_tokens":2}}}}
        , .{ model, method, if (p_same >= 0.5) "same" else "different", p_same, 1 - p_same });
        return std.json.parseFromSliceLeaky(V, a, body, .{});
    }

    fn run(ptr: *anyopaque, a: A, stage_index: usize, stage: Stage, indices: []const usize) anyerror!StageResult {
        const self: *Fake = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        const values = try a.alloc(?V, indices.len);
        const failures = try a.alloc(?anyerror, indices.len);
        @memset(failures, null);
        if (stage_index == 1) {
            try std.testing.expectEqualStrings("strong", stage.name);
            if (self.strong_fails) return error.DecisionUpstreamFailure;
            self.strong_items = indices.len;
        }
        for (indices, values, failures) |index, *value, *failure| {
            if (stage_index == 1 and self.strong_item_failure == index) {
                value.* = null;
                failure.* = error.InvalidDecisionOutput;
                continue;
            }
            value.* = if (stage_index == 0) try response(a, "fast-model", "typed", self.fast[index]) else try response(a, "strong-gen", "generated", self.strong_same);
        }
        return .{ .values = values, .failures = failures };
    }
};

test "decision cascade escalates only in-band items and records every stage" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const d = try compileJson(a, example);
    var fake = Fake{ .fast = &.{ 0.97, 0.6, 0.05 } };
    const out = try execute(a, &d, "same-entity", 3, .{ .ptr = &fake, .run = Fake.run });
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expectEqual(@as(usize, 1), fake.strong_items);
    // Confident items keep the fast answer and record one stage.
    const sure = out.values[0].?.object;
    try std.testing.expectEqualStrings("same", sure.get("answers").?.array.items[0].object.get("choice").?.string);
    try std.testing.expectEqualStrings("fast-model", sure.get("model").?.string);
    const sure_trace = sure.get("cascade").?.object;
    try std.testing.expectEqualStrings("fast", sure_trace.get("answered_by").?.string);
    try std.testing.expectEqual(@as(usize, 1), sure_trace.get("stages").?.array.items.len);
    try std.testing.expect(!sure_trace.get("stages").?.array.items[0].object.get("escalated").?.bool);
    // The in-band item takes the strong stage's answer, with both stages traced.
    const unsure = out.values[1].?.object;
    try std.testing.expectEqualStrings("different", unsure.get("answers").?.array.items[0].object.get("choice").?.string);
    try std.testing.expectEqualStrings("strong-gen", unsure.get("model").?.string);
    const trace_value = unsure.get("cascade").?.object;
    try std.testing.expectEqualStrings("strong", trace_value.get("answered_by").?.string);
    try std.testing.expectEqualStrings(&d.sha256, trace_value.get("definition_sha256").?.string);
    const stages = trace_value.get("stages").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), stages.len);
    try std.testing.expect(stages[0].object.get("escalated").?.bool);
    try std.testing.expect(!stages[1].object.get("escalated").?.bool);
    // Usage accumulates over the stages each item used.
    try std.testing.expectEqual(@as(i64, 20), unsure.get("usage").?.object.get("input_tokens").?.integer);
    try std.testing.expectEqual(@as(i64, 10), sure.get("usage").?.object.get("input_tokens").?.integer);
    try std.testing.expectEqual(@as(usize, 1), out.values[2].?.object.get("cascade").?.object.get("stages").?.array.items.len);
}

test "decision cascade skips later stages when nothing is in band" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const d = try compileJson(a, example);
    var fake = Fake{ .fast = &.{ 0.97, 0.95, 0.05 } };
    _ = try execute(a, &d, "same-entity", 3, .{ .ptr = &fake, .run = Fake.run });
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
}

test "decision cascade stage failure fails unless keep_previous is configured" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fake = Fake{ .fast = &.{0.5}, .strong_fails = true };
    const strict = try compileJson(a, example);
    const failed = try execute(a, &strict, "c", 1, .{ .ptr = &fake, .run = Fake.run });
    try std.testing.expect(failed.values[0] == null);
    try std.testing.expectEqual(@as(?anyerror, error.DecisionUpstreamFailure), failed.failures[0]);

    const lenient = try compileJson(a,
        \\{"stages":[{"name":"fast","decider":{"model":"x"}},
        \\  {"name":"strong","decider":{"decision_method":"generated","generator":{"model":"g"}},"on_failure":"keep_previous","when":{"probability_between":[0.3,0.9]}}]}
    );
    try std.testing.expect(!std.mem.eql(u8, &strict.sha256, &lenient.sha256));
    const out = try execute(a, &lenient, "c", 1, .{ .ptr = &fake, .run = Fake.run });
    const kept = out.values[0].?.object;
    try std.testing.expectEqualStrings("same", kept.get("answers").?.array.items[0].object.get("choice").?.string);
    const stages = kept.get("cascade").?.object.get("stages").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), stages.len);
    try std.testing.expectEqualStrings("DecisionUpstreamFailure", stages[1].object.get("error").?.string);
    try std.testing.expectEqualStrings("fast", kept.get("cascade").?.object.get("answered_by").?.string);
    try std.testing.expectEqual(@as(usize, 0), try validateTrace(&lenient, .{ .object = kept }));
}

test "decision cascade per-item failures stay isolated" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const d = try compileJson(a, example);
    var fake = Fake{ .fast = &.{ 0.5, 0.6, 0.99 }, .strong_item_failure = 0 };
    const out = try execute(a, &d, "c", 3, .{ .ptr = &fake, .run = Fake.run });
    try std.testing.expect(out.values[0] == null);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidDecisionOutput), out.failures[0]);
    try std.testing.expect(out.values[1] != null);
    try std.testing.expect(out.values[2] != null);
}

test "decision cascade stage-wide failures preserve completed siblings" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const definition = try compileJson(a, example);
    var fake = Fake{ .fast = &.{ 0.99, 0.6, 0.02 }, .strong_fails = true };
    const result = try execute(a, &definition, "c", 3, .{ .ptr = &fake, .run = Fake.run });
    try std.testing.expect(result.values[0] != null);
    try std.testing.expect(result.values[1] == null);
    try std.testing.expectEqual(@as(?anyerror, error.DecisionUpstreamFailure), result.failures[1]);
    try std.testing.expect(result.values[2] != null);
    _ = try validateTrace(&definition, result.values[0].?);
    _ = try validateTrace(&definition, result.values[2].?);
}

test "decision cascade binds question and choice selectors before inference" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const definition = try compileJson(a, example);
    const valid = try std.json.parseFromSliceLeaky(V, a,
        \\{"same":{"type":"choice","criteria":{"same":"Same","different":"Different"}}}
    , .{});
    try validateBands(&definition, valid);
    const invalid = [_][]const u8{
        \\{"typo":{"type":"choice","criteria":{"same":"Same","different":"Different"}}}
        ,
        \\{"same":{"type":"choice","criteria":{"yes":"Yes","no":"No"}}}
        ,
    };
    for (invalid) |bytes| try std.testing.expectError(error.InvalidDecisionCascade, validateBands(&definition, try std.json.parseFromSliceLeaky(V, a, bytes, .{})));
    const predicate = try std.json.parseFromSliceLeaky(V, a,
        \\{"same":{"type":"noul"}}
    , .{});
    try validateBands(&definition, predicate);
}

test "decision cascade receipts retain every revision and reject contradictory traces" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const definition = try compileJson(a, example);
    const RunnerWithRevisions = struct {
        fn run(ptr: *anyopaque, alloc: A, stage_index: usize, stage: Stage, indices: []const usize) !StageResult {
            const result = try Fake.run(ptr, alloc, stage_index, stage, indices);
            const values = try alloc.dupe(?V, result.values);
            for (values) |*value| if (value.*) |*response| {
                try response.object.put(alloc, "model_revision", .{ .string = if (stage_index == 0) "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" else "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" });
            };
            return .{ .values = values, .failures = result.failures };
        }
    };
    var fake = Fake{ .fast = &.{0.6} };
    const result = try execute(a, &definition, "c", 1, .{ .ptr = &fake, .run = RunnerWithRevisions.run });
    var response = result.values[0].?;
    try std.testing.expectEqual(@as(usize, 1), try validateTrace(&definition, response));
    const stages = response.object.getPtr("cascade").?.object.getPtr("stages").?.array.items;
    try std.testing.expectEqualStrings("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", stages[0].object.get("model_revision").?.string);
    try std.testing.expectEqualStrings("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", stages[1].object.get("model_revision").?.string);
    try stages[0].object.put(a, "escalated", .{ .bool = false });
    try std.testing.expectError(error.InvalidDecisionOutput, validateTrace(&definition, response));
    try stages[0].object.put(a, "escalated", .{ .bool = true });
    try response.object.put(a, "model", .{ .string = "invented" });
    try std.testing.expectError(error.InvalidDecisionOutput, validateTrace(&definition, response));
    try response.object.put(a, "model", stages[1].object.get("model").?);
    try response.object.getPtr("cascade").?.object.put(a, "definition_sha256", .{ .string = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" });
    try std.testing.expectError(error.InvalidDecisionOutput, validateTrace(&definition, response));
}

test "decision cascade rejects similarity-only answers as band input" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const answers = try std.json.parseFromSliceLeaky(V, a,
        \\[{"name":"q","type":"choice","decision_method":"embedding_similarity","choice":"a","similarities":[{"value":"a","similarity":0.8},{"value":"b","similarity":0.1}]}]
    , .{});
    try std.testing.expectError(error.UnsupportedDecisionCascadeAnswer, inBand(.{ .low = 0.3, .high = 0.9 }, answers));
    const predicate = try std.json.parseFromSliceLeaky(V, a,
        \\[{"name":"q","type":"predicate","decision_method":"typed","probability":0.9}]
    , .{});
    try std.testing.expect(try inBand(.{ .low = 0.3, .high = 0.9 }, predicate));
    try std.testing.expect(!try inBand(.{ .low = 0.3, .high = 0.89 }, predicate));
    try std.testing.expect(try inBand(.{ .question = "q", .low = 0.9, .high = 1 }, predicate));
}
