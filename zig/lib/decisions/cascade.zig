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
    require_agreement: bool = false,
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
        try keys(fields, &.{ "name", "decider", "when", "on_failure", "require_agreement" });
        const name = try text(fields.get("name") orelse return error.InvalidDecisionCascade);
        if (!validStageName(name)) return error.InvalidDecisionCascade;
        for (stages[0..i]) |other| if (std.mem.eql(u8, other.name, name)) return error.InvalidDecisionCascade;
        const decider = fields.get("decider") orelse return error.InvalidDecisionCascade;
        stage.* = .{ .name = name, .decider = decider, .method = try methodOf(decider) };
        if (fields.get("on_failure")) |policy| stage.on_failure = std.meta.stringToEnum(OnFailure, try text(policy)) orelse return error.InvalidDecisionCascade;
        if (fields.get("require_agreement")) |required| {
            if (required != .bool) return error.InvalidDecisionCascade;
            stage.require_agreement = required.bool;
        }
        if (i == 0) {
            // The first stage answers every item and has nothing to fall back to.
            // Explicit fail is equivalent to omission, including defaults
            // emitted by generated SDKs. Only keep_previous needs a prior answer.
            if (fields.contains("when") or stage.on_failure != .fail or stage.require_agreement) return error.InvalidDecisionCascade;
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
        // Preserve identities of definitions that do not enable the gate.
        if (stage.require_agreement) {
            try s.objectField("require_agreement");
            try s.write(true);
        }
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

pub const AgreementStatus = enum { agreed, disagreed, unverified };
const agreement_fields = [_][]const u8{ "agreement_status", "agreed", "disagreed_questions" };
pub const Agreement = struct {
    status: AgreementStatus = .agreed,
    questions: [64][]const u8 = undefined,
    count: usize = 0,
};

/// Reader fencing inspects an already validated, frozen definition.
pub fn requiresAgreement(value: V) bool {
    if (value != .object) return false;
    const stages = value.object.get("stages") orelse return false;
    if (stages != .array) return false;
    for (stages.array.items) |stage| {
        if (stage != .object) continue;
        const required = stage.object.get("require_agreement") orelse continue;
        if (required == .bool and required.bool) return true;
    }
    return false;
}

/// Normalize a caller-owned decider tree, preserving the relative order of all
/// retained fields. A disabled guard is the epoch-30 spelling (omission).
/// Enabled and malformed values are retained for ordinary validation.
pub fn omitDisabledAgreement(decider: *V) bool {
    if (decider.* != .object) return false;
    const method = decider.object.get("decision_method") orelse return false;
    if (method != .string or !std.mem.eql(u8, method.string, "cascade")) return false;
    const body = decider.object.getPtr("cascade") orelse return false;
    return omitDisabledAgreementDefinition(body);
}

pub fn omitDisabledAgreementDefinition(body: *V) bool {
    if (body.* != .object) return false;
    const stages = body.object.getPtr("stages") orelse return false;
    if (stages.* != .array) return false;
    var changed = false;
    for (stages.array.items) |*stage| {
        if (stage.* != .object) continue;
        const guard = stage.object.get("require_agreement") orelse continue;
        if (guard == .bool and !guard.bool) {
            _ = stage.object.orderedRemove("require_agreement");
            changed = true;
        }
    }
    return changed;
}

/// Return an owned replacement only when an envelope's decider needs default
/// normalization. The caller retains its original JSON otherwise. This runs at
/// configuration/persistence boundaries, never during per-result comparison.
pub fn normalizeDisabledAgreementJson(a: A, raw: []const u8, decider_path: []const []const u8) !?[]u8 {
    // Escaped keys must take the parsed path too. Most pre-feature policies
    // need neither parsing nor allocation here.
    if (std.mem.indexOf(u8, raw, "require_agreement") == null and std.mem.indexOfScalar(u8, raw, '\\') == null) return null;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var value = try std.json.parseFromSliceLeaky(V, arena.allocator(), raw, .{ .parse_numbers = false });
    var decider = &value;
    for (decider_path) |key| {
        if (decider.* != .object) return null;
        decider = decider.object.getPtr(key) orelse return null;
    }
    if (!omitDisabledAgreement(decider)) return null;
    return try std.json.Stringify.valueAlloc(a, value, .{});
}

const NamedAnswer = struct { name: []const u8, value: V };
fn orderedAnswers(answers: V, storage: *[64]NamedAnswer) ![]NamedAnswer {
    if (answers != .array or answers.array.items.len == 0 or answers.array.items.len > storage.len) return error.InvalidDecisionOutput;
    const entries = storage[0..answers.array.items.len];
    for (answers.array.items, entries) |answer, *entry| {
        if (answer != .object) return error.InvalidDecisionOutput;
        const name = answer.object.get("name") orelse return error.InvalidDecisionOutput;
        if (name != .string or name.string.len == 0) return error.InvalidDecisionOutput;
        entry.* = .{ .name = name.string, .value = answer };
    }
    std.mem.sort(NamedAnswer, entries, {}, struct {
        fn less(_: void, lhs: NamedAnswer, rhs: NamedAnswer) bool {
            return std.mem.lessThan(u8, lhs.name, rhs.name);
        }
    }.less);
    for (entries[1..], entries[0 .. entries.len - 1]) |entry, previous| if (std.mem.eql(u8, entry.name, previous.name)) return error.InvalidDecisionOutput;
    return entries;
}
fn sameVote(lhs: V, rhs: V) !bool {
    const ltype = lhs.object.get("type") orelse return error.InvalidDecisionOutput;
    const rtype = rhs.object.get("type") orelse return error.InvalidDecisionOutput;
    if (ltype != .string or rtype != .string or !std.mem.eql(u8, ltype.string, rtype.string)) return error.InvalidDecisionOutput;
    if (std.mem.eql(u8, ltype.string, "choice")) {
        const l = lhs.object.get("choice") orelse return error.InvalidDecisionOutput;
        const r = rhs.object.get("choice") orelse return error.InvalidDecisionOutput;
        if (l != .string or r != .string) return error.InvalidDecisionOutput;
        return std.mem.eql(u8, l.string, r.string);
    }
    if (!std.mem.eql(u8, ltype.string, "predicate")) return error.InvalidDecisionOutput;
    const l = try number(lhs.object.get("probability") orelse return error.InvalidDecisionOutput);
    const r = try number(rhs.object.get("probability") orelse return error.InvalidDecisionOutput);
    if (!std.math.isFinite(l) or !std.math.isFinite(r) or l < 0 or l > 1 or r < 0 or r > 1) return error.InvalidDecisionOutput;
    return l != 0.5 and r != 0.5 and (l > 0.5) == (r > 0.5);
}

/// Bounded stack scratch; no per-result plan, prompt parsing or allocation.
pub fn compareAgreement(previous: V, current: V, scope: ?[]const u8) !Agreement {
    var left: [64]NamedAnswer = undefined;
    var right: [64]NamedAnswer = undefined;
    const lhs = try orderedAnswers(previous, &left);
    const rhs = try orderedAnswers(current, &right);
    if (lhs.len != rhs.len) return error.InvalidDecisionOutput;
    var result = Agreement{};
    var selected = scope == null;
    for (lhs, rhs) |l, r| {
        if (!std.mem.eql(u8, l.name, r.name)) return error.InvalidDecisionOutput;
        if (scope) |name| if (!std.mem.eql(u8, name, l.name)) continue;
        selected = true;
        if (!try sameVote(l.value, r.value)) {
            result.status = .disagreed;
            result.questions[result.count] = r.name;
            result.count += 1;
        }
    }
    if (!selected) return error.InvalidDecisionOutput;
    return result;
}
fn writeAgreement(a: A, fields: *std.json.ObjectMap, agreement: Agreement) !void {
    try put(a, fields, "agreement_status", .{ .string = @tagName(agreement.status) });
    if (agreement.status != .unverified) try put(a, fields, "agreed", .{ .bool = agreement.status == .agreed });
    if (agreement.status == .disagreed) {
        var names = std.json.Array.init(a);
        for (agreement.questions[0..agreement.count]) |name| try names.append(.{ .string = name });
        try put(a, fields, "disagreed_questions", .{ .array = names });
    }
}
fn validateAgreement(fields: std.json.ObjectMap, expected: ?Agreement) !void {
    if (expected) |agreement| {
        const status = fields.get("agreement_status") orelse return error.InvalidDecisionOutput;
        if (status != .string or !std.mem.eql(u8, status.string, @tagName(agreement.status))) return error.InvalidDecisionOutput;
        if (agreement.status == .unverified) {
            if (fields.contains("agreed")) return error.InvalidDecisionOutput;
        } else {
            const agreed = fields.get("agreed") orelse return error.InvalidDecisionOutput;
            if (agreed != .bool or agreed.bool != (agreement.status == .agreed)) return error.InvalidDecisionOutput;
        }
        if (agreement.status == .disagreed) {
            const names = fields.get("disagreed_questions") orelse return error.InvalidDecisionOutput;
            if (names != .array or names.array.items.len != agreement.count) return error.InvalidDecisionOutput;
            for (names.array.items, agreement.questions[0..agreement.count]) |name, actual| {
                if (name != .string or !std.mem.eql(u8, name.string, actual)) return error.InvalidDecisionOutput;
            }
        } else if (fields.contains("disagreed_questions")) return error.InvalidDecisionOutput;
    } else for (agreement_fields) |field| if (fields.contains(field)) return error.InvalidDecisionOutput;
}

const upstream_identity_fields = [_][]const u8{ "decider", "definition_sha256", "answered_by" };

fn readAgreement(fields: std.json.ObjectMap) !?Agreement {
    const status = fields.get("agreement_status") orelse {
        try validateAgreement(fields, null);
        return null;
    };
    if (status != .string) return error.InvalidDecisionOutput;
    const parsed = std.meta.stringToEnum(AgreementStatus, status.string) orelse return error.InvalidDecisionOutput;
    var expected = Agreement{ .status = parsed };
    if (parsed == .disagreed) {
        const names = fields.get("disagreed_questions") orelse return error.InvalidDecisionOutput;
        if (names != .array or names.array.items.len == 0 or names.array.items.len > expected.questions.len) return error.InvalidDecisionOutput;
        for (names.array.items, 0..) |name, i| {
            if (name != .string or name.string.len == 0) return error.InvalidDecisionOutput;
            if (i > 0 and !std.mem.lessThan(u8, expected.questions[i - 1], name.string)) return error.InvalidDecisionOutput;
            expected.questions[i] = name.string;
        }
        expected.count = names.array.items.len;
    }
    try validateAgreement(fields, expected);
    return expected;
}

fn validateReportedIdentity(fields: std.json.ObjectMap) !void {
    for (upstream_identity_fields) |key| if (fields.get(key)) |value| {
        if (value != .string or value.string.len == 0 or !std.unicode.utf8ValidateSlice(value.string)) return error.InvalidDecisionOutput;
        if (std.mem.eql(u8, key, "definition_sha256")) {
            if (value.string.len != 64) return error.InvalidDecisionOutput;
            for (value.string) |byte| if (!std.ascii.isDigit(byte) and (byte < 'a' or byte > 'f')) return error.InvalidDecisionOutput;
        }
    };
}

fn validateDisagreedQuestions(agreement: Agreement, answers: V) !void {
    if (agreement.status != .disagreed) return;
    var storage: [64]NamedAnswer = undefined;
    const ordered = try orderedAnswers(answers, &storage);
    var cursor: usize = 0;
    for (agreement.questions[0..agreement.count]) |name| {
        while (cursor < ordered.len and std.mem.lessThan(u8, ordered[cursor].name, name)) cursor += 1;
        if (cursor == ordered.len or !std.mem.eql(u8, ordered[cursor].name, name)) return error.InvalidDecisionOutput;
    }
}

/// Reported upstream state is an opaque provider receipt, not a locally frozen
/// cascade definition. Validate its bounded wire contract before consuming it.
pub fn responseAgreement(response: V) !?Agreement {
    if (response != .object) return error.InvalidDecisionOutput;
    const raw = response.object.get("cascade") orelse return null;
    if (raw != .object) return error.InvalidDecisionOutput;
    const agreement = try readAgreement(raw.object) orelse return null;
    try validateReportedIdentity(raw.object);
    try validateDisagreedQuestions(agreement, response.object.get("answers") orelse return error.InvalidDecisionOutput);
    return agreement;
}

/// Shared gate for every response consumer, independent of the local decider
/// type. A typed provider can proxy an upstream cascade.
pub fn agreementBlocked(response: V) !bool {
    const agreement = try responseAgreement(response) orelse return false;
    return agreement.status != .agreed;
}

fn readUpstreamAgreement(fields: std.json.ObjectMap, answers: V) !?Agreement {
    const value = fields.get("upstream_agreement") orelse return null;
    if (value != .object) return error.InvalidDecisionOutput;
    for (value.object.keys()) |key| {
        for (agreement_fields ++ upstream_identity_fields) |allowed| {
            if (std.mem.eql(u8, key, allowed)) break;
        } else return error.InvalidDecisionOutput;
    }
    const agreement = try readAgreement(value.object) orelse return error.InvalidDecisionOutput;
    try validateReportedIdentity(value.object);
    try validateDisagreedQuestions(agreement, answers);
    return agreement;
}

fn blockedAgreement(agreement: ?Agreement) bool {
    return if (agreement) |value| value.status != .agreed else false;
}

/// A veto is sticky. A later successful check cannot turn an upstream
/// abstention into an accepted vote.
fn combineAgreement(previous: ?Agreement, current: ?Agreement) ?Agreement {
    if (blockedAgreement(previous)) return previous;
    return current orelse previous;
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
    // A categorical verdict is a vote, not measured confidence. Proxies can
    // resolve their method dynamically, beyond compile-time policy checks.
    if (answer.object.get("probability_source")) |source| {
        if (source == .string and std.mem.eql(u8, source.string, "verdict")) return error.UnsupportedDecisionCascadeAnswer;
    }
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
    var matched = false;
    for (answers.array.items) |answer| {
        const kind = answer.object.get("type") orelse return error.InvalidDecisionOutput;
        // A named choice value only applies to choice answers.
        const choice = if (kind == .string and std.mem.eql(u8, kind.string, "choice")) band.choice else null;
        // Validate all inspected answers even if an earlier one was in band.
        matched = band.contains(try answerProbability(answer, choice)) or matched;
    }
    return matched;
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
    var previous_answers: ?V = null;
    var aggregate: ?Agreement = null;
    for (executed.array.items, 0..) |entry, index| {
        const fields = try object(entry);
        const stage = definition.stages[index];
        const name = fields.get("name") orelse return error.InvalidDecisionOutput;
        const method = fields.get("decision_method") orelse return error.InvalidDecisionOutput;
        const escalated = fields.get("escalated") orelse return error.InvalidDecisionOutput;
        if (name != .string or !std.mem.eql(u8, name.string, stage.name) or method != .string or !std.mem.eql(u8, method.string, @tagName(stage.method)) or escalated != .bool) return error.InvalidDecisionOutput;
        const has_next = index + 1 < executed.array.items.len;
        if (fields.get("error")) |err| {
            if (err != .string or err.string.len == 0 or fields.contains("answers") or index == 0 or has_next or escalated.bool or stage.on_failure != .keep_previous or fields.contains("upstream_agreement") or fields.contains("upstream_revisions")) return error.InvalidDecisionOutput;
            const expected: ?Agreement = if (stage.require_agreement) .{ .status = .unverified } else null;
            try validateAgreement(fields, expected);
            if (expected) |agreement| aggregate = agreement;
            continue;
        }
        const answers = fields.get("answers") orelse return error.InvalidDecisionOutput;
        if (answers != .array or answers.array.items.len == 0) return error.InvalidDecisionOutput;
        if (fields.contains("upstream_revisions")) {
            var dependencies: @import("revisions.zig").Manifest = .{};
            try dependencies.collect(entry);
        }
        const upstream = try readUpstreamAgreement(fields, answers);
        const expected: ?Agreement = if (stage.require_agreement)
            (if (blockedAgreement(upstream)) .{ .status = .unverified } else try compareAgreement(previous_answers orelse return error.InvalidDecisionOutput, answers, stage.when.?.question))
        else
            null;
        try validateAgreement(fields, expected);
        aggregate = combineAgreement(combineAgreement(aggregate, upstream), expected);
        const blocked = blockedAgreement(aggregate);
        const should_escalate = !blocked and if (index + 1 < definition.stages.len) try inBand(definition.stages[index + 1].when.?, answers) else false;
        if (escalated.bool != should_escalate or has_next != should_escalate) return error.InvalidDecisionOutput;
        last = index;
        previous_answers = answers;
    }
    try validateAgreement(cascade, aggregate);
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
    /// Per-input accounting supplied by the host when provider usage belongs
    /// to a shared batch. Estimates must be explicitly labelled as such.
    item_usage: ?[]const V = null,
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

const AgreementRef = struct { stage_index: usize, upstream: bool };

const Row = struct {
    response: ?V = null,
    failure: ?anyerror = null,
    input_tokens: u64 = 0,
    output_tokens: u64 = 0,
    estimated_usage: bool = false,
    agreement: ?AgreementRef = null,
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
        if (stage_index > 0) for (active.items) |index| try markEscalated(a, &rows[index], true);
        if (active.items.len == 0) break;
        if (calls >= definition.budget.max_model_calls) return error.DecisionCascadeBudgetExceeded;
        if (runner.checkpoint) |check| try check(runner.ptr);
        calls += 1;
        const result = runner.run(runner.ptr, a, stage_index, stage, active.items) catch |err| {
            if (isFatal(err)) return err;
            for (active.items) |index| {
                const row = &rows[index];
                _ = try appendTrace(a, row, stage, null, null, err, null);
                if (stage_index == 0 or stage.on_failure == .fail) {
                    row.response = null;
                    row.failure = err;
                }
            }
            break;
        };
        if (result.values.len != active.items.len or result.failures.len != active.items.len) return error.InvalidDecisionOutput;
        if (result.item_usage) |usage| if (usage.len != active.items.len) return error.InvalidDecisionOutput;
        var still: std.array_list.Managed(usize) = .init(a);
        for (active.items, result.values, result.failures, 0..) |index, value, failure, position| {
            const row = &rows[index];
            if (value) |response| if (response != .object) return error.InvalidDecisionOutput;
            const usage: ?V = if (result.item_usage) |items| items[position] else if (value) |response| response.object.get("usage") else null;
            row.input_tokens +|= usageField(usage, "input_tokens");
            row.output_tokens +|= usageField(usage, "output_tokens");
            if (usage) |u| if (u == .object) if (u.object.get("estimated")) |estimated| {
                if (estimated != .bool) return error.InvalidDecisionOutput;
                row.estimated_usage = row.estimated_usage or estimated.bool;
            };
            if (value) |response| {
                if (response != .object or response.object.get("answers") == null) return error.InvalidDecisionOutput;
                const next_band = if (stage_index + 1 < definition.stages.len) definition.stages[stage_index + 1].when else null;
                const escalate = appendTrace(a, row, stage, response, usage, null, next_band) catch |err| {
                    if (err != error.InvalidDecisionOutput and err != error.UnsupportedDecisionCascadeAnswer) return err;
                    // Invalid metadata or unmeasurable band evidence is an
                    // item failure; completed and healthy siblings survive.
                    _ = try appendTrace(a, row, stage, null, usage, err, null);
                    if (stage_index == 0 or stage.on_failure == .fail) {
                        row.response = null;
                        row.failure = err;
                    }
                    continue;
                };
                row.response = response;
                row.failure = null;
                if (escalate) try still.append(index);
                continue;
            }
            const err = failure orelse error.InvalidDecisionOutput;
            if (isFatal(err)) return err;
            _ = try appendTrace(a, row, stage, null, usage, err, null);
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

fn appendTrace(a: A, row: *Row, stage: Stage, response: ?V, usage: ?V, err: ?anyerror, next_band: ?Band) !bool {
    const upstream = if (response) |value| try responseAgreement(value) else null;
    const local: ?Agreement = if (stage.require_agreement)
        (if (response != null and !blockedAgreement(upstream))
            try compareAgreement(row.response.?.object.get("answers").?, response.?.object.get("answers").?, stage.when.?.question)
        else
            .{ .status = .unverified })
    else
        null;
    // Validate the next band's evidence before committing this successful
    // stage. A bad dynamic result belongs to this stage's failure policy;
    // valid vetoes stop before any later band's evidence is inspected.
    const escalate = if (response != null and !blockedAgreement(combineAgreement(upstream, local)) and next_band != null)
        try inBand(next_band.?, response.?.object.get("answers").?)
    else
        false;
    var dependencies: @import("revisions.zig").Manifest = .{};
    if (response) |value| try dependencies.collect(value);
    var entry = try trace(a, stage, response, usage, err);
    if (response != null) try put(a, &entry.object, "upstream_revisions", try dependencies.value(a));
    var reference: ?AgreementRef = null;
    if (upstream) |agreement| {
        var receipt: std.json.ObjectMap = .empty;
        try writeAgreement(a, &receipt, agreement);
        const source = response.?.object.get("cascade").?.object;
        for (upstream_identity_fields) |field| if (source.get(field)) |value| try put(a, &receipt, field, value);
        try put(a, &entry.object, "upstream_agreement", .{ .object = receipt });
        reference = .{ .stage_index = row.stages.items.len, .upstream = true };
    }
    if (local) |agreement| {
        try writeAgreement(a, &entry.object, agreement);
        if (!blockedAgreement(upstream)) reference = .{ .stage_index = row.stages.items.len, .upstream = false };
    }
    try row.stages.append(entry);
    if (reference) |value| row.agreement = value;
    return escalate;
}

fn trace(a: A, stage: Stage, response: ?V, usage: ?V, err: ?anyerror) !V {
    var entry = std.json.ObjectMap{};
    try put(a, &entry, "name", .{ .string = stage.name });
    try put(a, &entry, "decision_method", .{ .string = @tagName(stage.method) });
    if (response) |value| {
        if (value.object.get("model")) |model| try put(a, &entry, "model", model);
        if (value.object.get("model_revision")) |revision| try put(a, &entry, "model_revision", revision);
        try put(a, &entry, "answers", value.object.get("answers").?);
        if (value.object.get("provenance")) |provenance| try put(a, &entry, "provenance", provenance);
    }
    if (usage) |reported| try put(a, &entry, "usage", reported);
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
    try put(a, &usage, "input_tokens", .{ .integer = std.math.cast(i64, row.input_tokens) orelse return error.InvalidDecisionOutput });
    try put(a, &usage, "output_tokens", .{ .integer = std.math.cast(i64, row.output_tokens) orelse return error.InvalidDecisionOutput });
    if (row.estimated_usage) try put(a, &usage, "estimated", .{ .bool = true });
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
    if (row.agreement) |reference| {
        const stage = row.stages.items[reference.stage_index].object;
        const agreement = if (reference.upstream) stage.get("upstream_agreement").?.object else stage;
        for (agreement_fields) |field| if (agreement.get(field)) |value| try put(a, &cascade, field, value);
    }
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

test "decision cascade apportioned usage includes failed attempts in keep_previous receipts" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const definition = try compileJson(a,
        \\{"stages":[{"name":"fast","decider":{"model":"fast"}},{"name":"strong","decider":{"decision_method":"generated","generator":{"model":"strong"}},"when":{"probability_between":[0.3,0.9]},"on_failure":"keep_previous"}]}
    );
    const Metered = struct {
        fn run(ptr: *anyopaque, alloc: A, i: usize, stage: Stage, indices: []const usize) !StageResult {
            var result = try Fake.run(ptr, alloc, i, stage, indices);
            if (i == 1) result.item_usage = (try std.json.parseFromSliceLeaky(V, alloc,
                \\[{"input_tokens":5,"output_tokens":3,"estimated":true},{"input_tokens":5,"output_tokens":2,"estimated":true}]
            , .{})).array.items;
            return result;
        }
    };
    var fake = Fake{ .fast = &.{ 0.5, 0.5 }, .strong_item_failure = 1 };
    const outcome = try execute(a, &definition, "c", 2, .{ .ptr = &fake, .run = Metered.run });
    for (outcome.values, 0..) |value, i| {
        const response = value.?;
        const usage = response.object.get("usage").?.object;
        try std.testing.expectEqual(@as(i64, 15), usage.get("input_tokens").?.integer);
        try std.testing.expectEqual(@as(i64, if (i == 0) 5 else 4), usage.get("output_tokens").?.integer);
        try std.testing.expect(usage.get("estimated").?.bool);
        _ = try validateTrace(&definition, response);
    }
    const failed = outcome.values[1].?.object.get("cascade").?.object.get("stages").?.array.items[1].object;
    try std.testing.expect(failed.contains("error"));
    try std.testing.expectEqual(@as(i64, 5), failed.get("usage").?.object.get("input_tokens").?.integer);
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

test "decision cascade accepts SDK defaults and canonicalizes explicit first-stage fail" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // Also asserted by the Python SDK constructor/round-trip regression.
    const fixture = try std.json.parseFromSliceLeaky(V, a, @embedFile("testdata/sdk_cascade_defaults.json"), .{});
    const body = fixture.object.get("cascade").?;
    const explicit = try compile(a, body);
    const first = &body.object.getPtr("stages").?.array.items[0];
    try std.testing.expect(first.object.swapRemove("on_failure"));
    const omitted = try compile(a, body);
    try std.testing.expectEqualSlices(u8, &explicit.sha256, &omitted.sha256);
    try first.object.put(a, "on_failure", .{ .string = "keep_previous" });
    try std.testing.expectError(error.InvalidDecisionCascade, compile(a, body));
}

test "decision cascade agreement configuration preserves disabled identities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const base = try compileJson(a, example);
    const raw = try std.json.parseFromSliceLeaky(V, a, example, .{});
    const stages = raw.object.getPtr("stages").?.array.items;
    for (stages) |*stage| try stage.object.put(a, "require_agreement", .{ .bool = false });
    const disabled = try compile(a, raw);
    try std.testing.expectEqualSlices(u8, &base.sha256, &disabled.sha256);
    try stages[0].object.put(a, "require_agreement", .{ .bool = true });
    try std.testing.expectError(error.InvalidDecisionCascade, compile(a, raw));
    try stages[0].object.put(a, "require_agreement", .{ .bool = false });
    try stages[1].object.put(a, "require_agreement", .{ .string = "true" });
    try std.testing.expectError(error.InvalidDecisionCascade, compile(a, raw));
    try stages[1].object.put(a, "require_agreement", .{ .bool = true });
    const enabled = try compile(a, raw);
    try std.testing.expect(!std.mem.eql(u8, &base.sha256, &enabled.sha256));
    try std.testing.expect(requiresAgreement(raw));
}

test "decision cascade agreement compares scoped choices and predicate ties without allocation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const lhs = try std.json.parseFromSliceLeaky(V, a,
        \\[{"name":"a","type":"choice","choice":"same"},{"name":"b","type":"predicate","probability":0.9}]
    , .{});
    const rhs = try std.json.parseFromSliceLeaky(V, a,
        \\[{"name":"b","type":"predicate","probability":0.1},{"name":"a","type":"choice","choice":"same"}]
    , .{});
    try std.testing.expectEqual(AgreementStatus.agreed, (try compareAgreement(lhs, rhs, "a")).status);
    const disagreement = try compareAgreement(lhs, rhs, null);
    try std.testing.expectEqual(AgreementStatus.disagreed, disagreement.status);
    try std.testing.expectEqual(@as(usize, 1), disagreement.count);
    try std.testing.expectEqualStrings("b", disagreement.questions[0]);
    try rhs.array.items[0].object.put(a, "probability", .{ .float = 0.5 });
    try std.testing.expectEqual(AgreementStatus.disagreed, (try compareAgreement(rhs, rhs, "b")).status);
    try rhs.array.items[0].object.put(a, "probability", .{ .float = 0.99 });
    try std.testing.expectEqual(AgreementStatus.agreed, (try compareAgreement(lhs, rhs, null)).status);
    try std.testing.expectError(error.InvalidDecisionOutput, compareAgreement(lhs, rhs, "missing"));
    rhs.array.items[0] = rhs.array.items[1];
    try std.testing.expectError(error.InvalidDecisionOutput, compareAgreement(lhs, rhs, null));
}

test "decision cascade agreement gates batches stops later stages and verifies the trace" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const definition = try compileJson(a,
        \\{"stages":[{"name":"fast","decider":{"model":"fast"}},{"name":"strong","decider":{"decision_method":"generated","generator":{"model":"strong"}},"require_agreement":true,"when":{"choice":"same","probability_between":[0.3,1.0]}},{"name":"never","decider":{"model":"never"},"when":{"probability_between":[0,1]}}]}
    );
    var fake = Fake{ .fast = &.{ 0.99, 0.6, 0.1 }, .strong_same = 0.2 };
    const result = try execute(a, &definition, "c", 3, .{ .ptr = &fake, .run = Fake.run });
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    for (result.values, 0..) |value, index| {
        const response = value.?;
        _ = try validateTrace(&definition, response);
        const trace_value = response.object.get("cascade").?.object;
        if (index == 2) {
            try std.testing.expect(!trace_value.contains("agreement_status"));
            try std.testing.expect(!try agreementBlocked(response));
        } else {
            try std.testing.expect(try agreementBlocked(response));
            try std.testing.expectEqualStrings("disagreed", trace_value.get("agreement_status").?.string);
            try std.testing.expectEqual(@as(usize, 2), trace_value.get("stages").?.array.items.len);
            try std.testing.expectEqual(@as(i64, 20), response.object.get("usage").?.object.get("input_tokens").?.integer);
        }
    }
    var forged = result.values[0].?;
    try forged.object.getPtr("cascade").?.object.put(a, "agreed", .{ .bool = true });
    try std.testing.expectError(error.InvalidDecisionOutput, validateTrace(&definition, forged));
    _ = forged.object.getPtr("cascade").?.object.swapRemove("agreement_status");
    try std.testing.expectError(error.InvalidDecisionOutput, validateTrace(&definition, forged));
}

test "decision cascade guarded keep_previous is unverified not an agreeing vote" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const raw = try std.json.parseFromSliceLeaky(V, a, example, .{});
    const strong = &raw.object.getPtr("stages").?.array.items[1];
    try strong.object.put(a, "require_agreement", .{ .bool = true });
    try strong.object.put(a, "on_failure", .{ .string = "keep_previous" });
    const definition = try compile(a, raw);
    for ([_]bool{ false, true }) |whole_stage| {
        var fake = Fake{ .fast = &.{0.6}, .strong_fails = whole_stage, .strong_item_failure = if (whole_stage) null else 0 };
        const result = try execute(a, &definition, "c", 1, .{ .ptr = &fake, .run = Fake.run });
        const response = result.values[0].?;
        _ = try validateTrace(&definition, response);
        try std.testing.expect(try agreementBlocked(response));
        const fields = response.object.get("cascade").?.object;
        try std.testing.expectEqualStrings("unverified", fields.get("agreement_status").?.string);
        try std.testing.expect(!fields.contains("agreed"));
    }
    var agreeing = Fake{ .fast = &.{0.6}, .strong_same = 0.9 };
    const result = try execute(a, &definition, "c", 1, .{ .ptr = &agreeing, .run = Fake.run });
    _ = try validateTrace(&definition, result.values[0].?);
    try std.testing.expect(!try agreementBlocked(result.values[0].?));
    try std.testing.expect(result.values[0].?.object.get("cascade").?.object.get("agreed").?.bool);
}

test "decision cascade disabled defaults normalize to the epoch-30 SDK fixture" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // This fixture is the constructed SDK wire form accepted by epoch 30.
    var fixture = try std.json.parseFromSliceLeaky(V, a, @embedFile("testdata/sdk_cascade_defaults.json"), .{});
    const baseline = try std.json.Stringify.valueAlloc(a, fixture, .{});
    const stages = fixture.object.getPtr("cascade").?.object.getPtr("stages").?.array.items;
    for (stages) |*stage| try stage.object.put(a, "require_agreement", .{ .bool = false });
    const disabled = try std.json.Stringify.valueAlloc(a, fixture, .{});
    const normalized = (try normalizeDisabledAgreementJson(a, disabled, &.{})).?;
    try std.testing.expectEqualStrings(baseline, normalized);
    try std.testing.expect((try normalizeDisabledAgreementJson(a, normalized, &.{})) == null);
    // The fast path cannot allocate, and escaped property names cannot bypass normalization.
    try std.testing.expect((try normalizeDisabledAgreementJson(std.testing.failing_allocator, baseline, &.{})) == null);
    const escaped = try std.mem.replaceOwned(u8, a, disabled, "require_agreement", "require_\\u0061greement");
    try std.testing.expectEqualStrings(baseline, (try normalizeDisabledAgreementJson(a, escaped, &.{})).?);
    try stages[1].object.put(a, "require_agreement", .{ .bool = true });
    try std.testing.expect(omitDisabledAgreement(&fixture));
    try std.testing.expect(!stages[0].object.contains("require_agreement"));
    try std.testing.expect(stages[1].object.get("require_agreement").?.bool);
    try std.testing.expect(!omitDisabledAgreement(&fixture));
    // Preserve malformed fields for validation instead of silently disabling them.
    try stages[1].object.put(a, "require_agreement", .{ .string = "false" });
    try std.testing.expect(!omitDisabledAgreement(&fixture));
    try std.testing.expectError(error.InvalidDecisionCascade, compile(a, fixture.object.get("cascade").?));
}

test "decision cascade default normalization preserves opaque numeric literals and nulls" {
    const a = std.testing.allocator;
    const raw =
        \\{"decider":{"decision_method":"cascade","cascade":{"stages":[{"name":"first","decider":{"model":"m"}},{"name":"next","decider":{"model":"m"},"require_agreement":false,"when":{"probability_between":[0.3,0.9]}}]}},"opaque":{"tiny":1e-400,"bound":1.0000000000000001,"missing":null}}
    ;
    const expected = try std.mem.replaceOwned(u8, a, raw, "\"require_agreement\":false,", "");
    defer a.free(expected);
    const normalized = (try normalizeDisabledAgreementJson(a, raw, &.{"decider"})).?;
    defer a.free(normalized);
    try std.testing.expectEqualStrings(expected, normalized);
}

test "decision cascade preserves upstream vetoes at every boundary with bounded receipts" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const ProxyFixture = struct {
        upstream: V,
        at: usize,
        calls: usize = 0,
        fn run(ptr: *anyopaque, alloc: A, index: usize, _: Stage, indices: []const usize) anyerror!StageResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const values = try alloc.alloc(?V, indices.len);
            for (values, indices) |*v, row| v.* = if (index == self.at and row == 0) self.upstream else try Fake.response(alloc, "proxy", "typed", 0.97);
            const failures = try alloc.alloc(?anyerror, indices.len);
            @memset(failures, null);
            return .{ .values = values, .failures = failures };
        }
    };
    for ([_]bool{ false, true }) |guard| {
        const definition = try compileJson(a, try std.fmt.allocPrint(a,
            \\{{"stages":[{{"name":"first","decider":{{"model":"proxy"}}}},{{"name":"second","decider":{{"model":"proxy"}},"require_agreement":{},"when":{{"question":"same","choice":"same","probability_between":[0.3,1]}}}},{{"name":"third","decider":{{"model":"proxy"}},"when":{{"question":"same","choice":"same","probability_between":[0.3,1]}}}}]}}
        , .{guard}));
        for ([_][]const u8{ "disagreed", "unverified", "agreed" }) |status| {
            const source_definition = try compileJson(a,
                \\{"stages":[{"name":"fast","decider":{"model":"fast"}},{"name":"strong","decider":{"decision_method":"generated","generator":{"model":"strong"}},"require_agreement":true,"on_failure":"keep_previous","when":{"question":"same","choice":"same","probability_between":[0.3,1]}}]}
            );
            var source = Fake{ .fast = &.{if (std.mem.eql(u8, status, "disagreed")) 0.4 else 0.97}, .strong_same = 0.97, .strong_fails = std.mem.eql(u8, status, "unverified") };
            const source_result = try execute(a, &source_definition, "remote", 1, .{ .ptr = &source, .run = Fake.run });
            const upstream = source_result.values[0].?;
            _ = try validateTrace(&source_definition, upstream);
            for (0..2) |at| {
                var runner = ProxyFixture{ .upstream = upstream, .at = at };
                const result = try execute(a, &definition, "outer", 2, .{ .ptr = &runner, .run = ProxyFixture.run });
                const response = result.values[0].?;
                const blocked = !std.mem.eql(u8, status, "agreed");
                try std.testing.expectEqual(blocked, try agreementBlocked(response));
                try std.testing.expect(!(try agreementBlocked(result.values[1].?)));
                _ = try validateTrace(&definition, response);
                _ = try validateTrace(&definition, result.values[1].?);
                const stages = response.object.get("cascade").?.object.get("stages").?.array.items;
                try std.testing.expectEqual(if (blocked) at + 1 else 3, stages.len);
                const receipt = stages[at].object.get("upstream_agreement").?.object;
                try std.testing.expect(!receipt.contains("stages"));
                try std.testing.expectEqualStrings(status, receipt.get("agreement_status").?.string);
                const usage = response.object.get("usage").?.object;
                try std.testing.expectEqual(@as(i64, @intCast((stages.len - 1) * 10)) + upstream.object.get("usage").?.object.get("input_tokens").?.integer, usage.get("input_tokens").?.integer);
                if (blocked) {
                    var altered = response;
                    try altered.object.getPtr("cascade").?.object.put(a, "agreement_status", .{ .string = "agreed" });
                    try std.testing.expectError(error.InvalidDecisionOutput, validateTrace(&definition, altered));
                }
            }
        }
    }
}

test "decision cascade malformed upstream state is isolated and uses operational fallback" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const ProxyFixture = struct {
        bad: V,
        fn run(ptr: *anyopaque, alloc: A, index: usize, _: Stage, indices: []const usize) anyerror!StageResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const values = try alloc.alloc(?V, indices.len);
            for (values, indices) |*v, row| v.* = if (index == 1 and row == 0) self.bad else try Fake.response(alloc, "proxy", "typed", 0.6);
            const failures = try alloc.alloc(?anyerror, indices.len);
            @memset(failures, null);
            return .{ .values = values, .failures = failures };
        }
    };
    for ([_][]const u8{
        "{\"agreed\":false}",
        "{\"agreement_status\":\"disagreed\",\"agreed\":true,\"disagreed_questions\":[\"same\"]}",
        "{\"agreement_status\":\"disagreed\",\"agreed\":false,\"disagreed_questions\":[\"unknown\"]}",
        "{\"agreement_status\":\"agreed\",\"agreed\":true,\"definition_sha256\":\"invalid\"}",
    }) |metadata| {
        var bad = try Fake.response(a, "proxy", "typed", 0.97);
        try bad.object.put(a, "cascade", try std.json.parseFromSliceLeaky(V, a, metadata, .{}));
        var runner = ProxyFixture{ .bad = bad };
        const definition = try compileJson(a,
            \\{"stages":[{"name":"first","decider":{"model":"proxy"}},{"name":"second","decider":{"model":"proxy"},"require_agreement":true,"on_failure":"keep_previous","when":{"probability_between":[0.3,0.9]}}]}
        );
        const result = try execute(a, &definition, "outer", 2, .{ .ptr = &runner, .run = ProxyFixture.run });
        try std.testing.expect(result.failures[0] == null);
        try std.testing.expect(try agreementBlocked(result.values[0].?));
        try std.testing.expect(!(try agreementBlocked(result.values[1].?)));
        _ = try validateTrace(&definition, result.values[0].?);
        _ = try validateTrace(&definition, result.values[1].?);
    }
}

test "decision cascade verdict confidence is rejected per stage without losing vetoes or siblings" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Proxy = struct {
        fn run(_: *anyopaque, alloc: A, index: usize, _: Stage, indices: []const usize) anyerror!StageResult {
            try std.testing.expect(index < 2);
            const values = try alloc.alloc(?V, indices.len);
            const failures = try alloc.alloc(?anyerror, indices.len);
            @memset(failures, null);
            for (values, indices) |*value, row| {
                value.* = if (index == 0)
                    try Fake.response(alloc, "fast", "typed", if (row == 0) 0.6 else if (row == 1) 0.4 else 0.1)
                else
                    try std.json.parseFromSliceLeaky(V, alloc,
                        \\{"model":"proxy","answers":[{"name":"same","type":"choice","decision_method":"generated","probability_source":"verdict","raw_structured_answer":"same","choice":"same","probabilities":[{"value":"same","probability":1},{"value":"different","probability":0}]}],"usage":{"input_tokens":20,"output_tokens":1}}
                    , .{});
            }
            return .{ .values = values, .failures = failures };
        }
    };
    for ([_][]const u8{ "fail", "keep_previous" }) |policy| {
        const definition = try compileJson(a, try std.fmt.allocPrint(a,
            \\{{"stages":[{{"name":"fast","decider":{{"model":"fast"}}}},{{"name":"proxy","decider":{{"model":"remote"}},"require_agreement":true,"on_failure":"{s}","when":{{"question":"same","choice":"same","probability_between":[0.3,1]}}}},{{"name":"after","decider":{{"model":"after"}},"when":{{"question":"same","choice":"same","probability_between":[0.3,0.9]}}}}]}}
        , .{policy}));
        var token: u8 = 0;
        const out = try execute(a, &definition, "outer", 3, .{ .ptr = &token, .run = Proxy.run });
        if (std.mem.eql(u8, policy, "fail")) {
            try std.testing.expect(out.values[0] == null);
            try std.testing.expectEqual(error.UnsupportedDecisionCascadeAnswer, out.failures[0].?);
        } else {
            const fallback = out.values[0].?;
            try std.testing.expect(try agreementBlocked(fallback));
            try std.testing.expectEqualStrings("unverified", fallback.object.get("cascade").?.object.get("agreement_status").?.string);
            try std.testing.expectEqual(@as(i64, 30), fallback.object.get("usage").?.object.get("input_tokens").?.integer);
            _ = try validateTrace(&definition, fallback);
        }
        const veto = out.values[1].?;
        try std.testing.expect(try agreementBlocked(veto));
        try std.testing.expectEqualStrings("disagreed", veto.object.get("cascade").?.object.get("agreement_status").?.string);
        _ = try validateTrace(&definition, veto);
        try std.testing.expect(!(try agreementBlocked(out.values[2].?)));
        _ = try validateTrace(&definition, out.values[2].?);
    }
    const mixed = try std.json.parseFromSliceLeaky(V, a,
        \\[{"name":"measured","type":"predicate","probability":0.6},{"name":"verdict","type":"predicate","decision_method":"generated","probability_source":"verdict","raw_structured_answer":true,"probability":1}]
    , .{});
    const band = Band{ .low = 0.3, .high = 0.9 };
    try std.testing.expectError(error.UnsupportedDecisionCascadeAnswer, inBand(band, mixed));
    var scoped = band;
    scoped.question = "measured";
    try std.testing.expect(try inBand(scoped, mixed));
}
