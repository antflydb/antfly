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

//! Reproducible judge contracts and sufficient statistics. Gold labels never
//! enter a judge invocation. Abstention and unavailable input are distinct.
const std = @import("std");
const t = @import("types.zig");
pub const prompt_revision = "memory-evaluation/v1";
pub const Task = enum { identity, relationship, supersession, retrieval };
pub const Label = enum { same, different, related, unrelated, supersedes, contradicts, restates, independent };
pub const Case = struct {
    id: []const u8,
    task: Task,
    input: std.json.Value,
    policy: ?PolicyInput = null,
    expected: ?Label = null,
    /// Binary legacy corpora say only whether the later claim replaces the earlier one.
    expected_supersedes: ?bool = null,
    relevant_ids: []const []const u8 = &.{},
    /// Production-derived inputs carry live evidence fences. Fixtures may be
    /// independently uploaded by the dataset administrator without evidence.
    evidence: []const t.EvidenceRef = &.{},
};
pub const StoredCase = struct { forgotten: bool = false, value: Case, dataset: []const u8, revision: u64, owner: []const u8, branch: []const u8, recorded_at: i64, fingerprint: []const u8 };
pub const Dataset = struct { id: []const u8, root: ?[]const u8 = null, revision: u64, owner: []const u8, branch: []const u8, visibility: t.Visibility = .private, cases: u64 = 0, recorded_at: i64 };
pub const DatasetRequest = struct { id: []const u8, expected_revision: u64, cases: []const Case = &.{}, remove: []const []const u8 = &.{}, visibility: t.Visibility = .private };
pub const Start = struct { id: []const u8, dataset: []const u8, dataset_revision: ?u64 = null, expected_revision: u64, mode: enum { external, managed, policy } = .external, judge: ?t.Model = null, thresholds: ?t.SemanticDiscovery = null };
pub const JudgeInput = struct { task: Task, input: std.json.Value, prompt_revision: []const u8 = prompt_revision };
pub const JudgeOutput = struct {
    label: ?Label = null,
    ranking: ?[]const []const u8 = null,
    reason: []const u8 = "",
    pub fn prediction(self: JudgeOutput) Prediction {
        return .{ .label = self.label, .ranking = self.ranking, .reason = self.reason };
    }
};
pub const Prediction = struct { admitted: ?bool = null, label: ?Label = null, ranking: ?[]const []const u8 = null, reason: []const u8 = "" };
pub const Verdict = struct { case_id: []const u8, case_revision: u64, input_fingerprint: []const u8, prediction: JudgeOutput, invalid_output: bool = false };
pub const Submit = struct { expected_revision: u64, verdicts: []const Verdict, actor: []const u8 = "", model: ?t.Model = null };
pub const Result = struct { invalid_output: bool = false, scrubbed: bool = false, run: []const u8, case_id: []const u8, case_revision: u64, input_fingerprint: []const u8, prediction: ?Prediction = null, unavailable: bool = false, principal: []const u8, actor: []const u8, model: ?t.Model, revision: u64, recorded_at: i64 };
pub const Counts = struct {
    cases: u64 = 0,
    binary_supersession_cases: u64 = 0,
    graded: u64 = 0,
    abstained: u64 = 0,
    invalid_outputs: u64 = 0,
    unavailable: u64 = 0,
    correct: u64 = 0,
    true_positive: u64 = 0,
    false_positive: u64 = 0,
    false_negative: u64 = 0,
    true_negative: u64 = 0,
    pub fn report(self: Counts) Report {
        const precision = ratio(self.true_positive, self.true_positive + self.false_positive);
        const recall = ratio(self.true_positive, self.true_positive + self.false_negative);
        return .{ .counts = self, .precision = precision, .recall = recall, .f1 = if (precision != null and recall != null) if (precision.? + recall.? > 0) 2 * precision.? * recall.? / (precision.? + recall.?) else 0 else null, .coverage = ratio(self.graded, self.cases - self.unavailable), .accuracy = ratio(self.correct, self.graded) };
    }
};
pub const Report = struct { counts: Counts, precision: ?f64, recall: ?f64, f1: ?f64, coverage: ?f64, accuracy: ?f64 };
pub const Run = struct {
    version: u8 = 1,
    id: []const u8,
    dataset: []const u8,
    dataset_revision: u64,
    root: ?[]const u8,
    case_count: u64,
    owner: []const u8,
    branch: []const u8,
    visibility: t.Visibility,
    mode: @FieldType(Start, "mode"),
    knowledge_revision: u64,
    config_generation: u64,
    config_fingerprint: []const u8,
    source_config_fingerprint: []const u8,
    thresholds: ?t.SemanticDiscovery = null,
    judge: ?t.Model,
    prompt_revision: []const u8 = prompt_revision,
    fingerprint: []const u8,
    phase: enum { pending, completed, cancelled } = .pending,
    error_code: ?[]const u8 = null,
    completed: u64 = 0,
    cursor: ?[]const u8 = null,
    counts: [4]Counts = @splat(.{}),
    admission: [3]Counts = @splat(.{}),
    revision: u64,
    recorded_at: i64,
};
pub const Status = struct { run: Run, identity: Report, relationship: Report, supersession: Report, retrieval: Report, identity_admission: Report, relationship_admission: Report, supersession_admission: Report };
pub const JudgeTask = struct { case_id: []const u8, case_revision: u64, input_fingerprint: []const u8, input: JudgeInput };
pub const TaskPage = struct { items: []const JudgeTask, next_cursor: ?[]const u8, complete: bool, run_fingerprint: []const u8 };
pub const CasePage = struct { items: []const StoredCase, next_cursor: ?[]const u8, complete: bool, revision: u64 };
pub const PolicyInput = struct { seed: []const u8 = "", target: []const u8 = "", entity_candidates: t.EntityCandidateRequest = .{}, supersession_candidates: t.SupersessionCandidateRequest = .{}, recall: t.Recall = .{} };

fn ratio(n: u64, d: u64) ?f64 {
    return if (d == 0) null else @as(f64, @floatFromInt(n)) / @as(f64, @floatFromInt(d));
}
fn positive(label: Label) bool {
    return switch (label) {
        .different, .unrelated, .independent => false,
        else => true,
    };
}
pub fn labelValid(task: Task, label: Label) bool {
    return switch (task) {
        .identity => label == .same or label == .different,
        .relationship => label == .related or label == .unrelated,
        .supersession => label == .supersedes or label == .contradicts or label == .restates or label == .independent,
        .retrieval => false,
    };
}
pub fn validateCase(value: Case) !void {
    try t.validateId(value.id);
    if (value.evidence.len > 256 or value.relevant_ids.len > 256) return error.InvalidMemoryRequest;
    if (value.expected_supersedes != null) {
        if (value.task != .supersession or value.expected != null) return error.InvalidMemoryRequest;
    } else if (value.task == .retrieval) {
        if (value.expected != null) return error.InvalidMemoryRequest;
    } else if (value.expected == null or !labelValid(value.task, value.expected.?)) return error.InvalidMemoryRequest;
    for (value.relevant_ids, 0..) |id, i| {
        try t.validateId(id);
        for (value.relevant_ids[0..i]) |prior| if (std.mem.eql(u8, prior, id)) return error.InvalidMemoryRequest;
    }
}
pub fn score(counts: *Counts, value: Case, prediction: ?Prediction) !void {
    counts.cases += 1;
    const result = prediction orelse {
        counts.unavailable += 1;
        return;
    };
    if (value.task == .retrieval) {
        if (result.label != null) return error.InvalidMemoryRequest;
        const ranking = result.ranking orelse {
            counts.abstained += 1;
            counts.false_negative += value.relevant_ids.len;
            return;
        };
        if (ranking.len > 256) return error.InvalidMemoryRequest;
        var correct: u64 = 0;
        for (ranking, 0..) |id, i| {
            try t.validateId(id);
            for (ranking[0..i]) |prior| if (std.mem.eql(u8, prior, id)) return error.InvalidMemoryRequest;
            for (value.relevant_ids) |relevant| if (std.mem.eql(u8, id, relevant)) {
                correct += 1;
                break;
            };
        }
        counts.graded += 1;
        counts.true_positive += correct;
        counts.false_positive += ranking.len - correct;
        counts.false_negative += value.relevant_ids.len - correct;
        if (correct == ranking.len and correct == value.relevant_ids.len) counts.correct += 1;
        return;
    }
    if (result.ranking != null) return error.InvalidMemoryRequest;
    if (value.expected_supersedes) |expected| {
        counts.binary_supersession_cases += 1;
        const label = result.label orelse {
            counts.abstained += 1;
            if (expected) counts.false_negative += 1;
            return;
        };
        if (!labelValid(.supersession, label)) return error.InvalidMemoryRequest;
        counts.graded += 1;
        const replaces = label == .supersedes;
        if (expected == replaces) counts.correct += 1;
        if (replaces) {
            if (expected) counts.true_positive += 1 else counts.false_positive += 1;
        } else if (expected) counts.false_negative += 1 else counts.true_negative += 1;
        return;
    }
    const expected = value.expected orelse return error.InvalidMemoryRequest;
    const label = result.label orelse {
        counts.abstained += 1;
        if (positive(expected)) counts.false_negative += 1;
        return;
    };
    if (!labelValid(value.task, label)) return error.InvalidMemoryRequest;
    counts.graded += 1;
    if (expected == label) counts.correct += 1;
    if (positive(label)) {
        if (label == expected) counts.true_positive += 1 else counts.false_positive += 1;
    } else if (!positive(expected)) counts.true_negative += 1;
    if (positive(expected) and label != expected) counts.false_negative += 1;
}

test "evaluation metrics distinguish abstention availability wrong positive classes and retrieval misses" {
    var counts: Counts = .{};
    const value: Case = .{ .id = "pair", .task = .supersession, .input = .null, .expected = .supersedes };
    try score(&counts, value, .{ .label = .supersedes });
    try score(&counts, value, .{ .label = .contradicts });
    try score(&counts, value, .{});
    try score(&counts, value, null);
    const report = counts.report();
    try std.testing.expectEqual(@as(f64, 0.5), report.precision.?);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0 / 3.0), report.recall.?, 0.000001);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0 / 3.0), report.coverage.?, 0.000001);
    var retrieval: Counts = .{};
    try score(&retrieval, .{ .id = "query", .task = .retrieval, .input = .null, .relevant_ids = &.{ "a", "b" } }, .{ .ranking = &.{ "b", "c" } });
    try std.testing.expectEqual(@as(f64, 0.5), retrieval.report().precision.?);
    try std.testing.expectEqual(@as(f64, 0.5), retrieval.report().recall.?);
}

pub fn scoreAdmission(counts: *Counts, value: Case, admitted: bool) !void {
    if (value.expected_supersedes != null) return;
    if (value.task == .retrieval or value.expected == null) return error.InvalidMemoryRequest;
    counts.cases += 1;
    counts.graded += 1;
    const expected = positive(value.expected.?);
    if (expected == admitted) counts.correct += 1;
    if (admitted) {
        if (expected) counts.true_positive += 1 else counts.false_positive += 1;
    } else if (expected) counts.false_negative += 1 else counts.true_negative += 1;
}

pub const ResultEntry = struct { case: StoredCase, result: ?Result };
pub const ResultPage = struct { items: []const ResultEntry, next_cursor: ?[]const u8, complete: bool, run_fingerprint: []const u8 };
