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

const std = @import("std");
const pipeline = @import("../../pipelines/laya.zig");
const model = @import("../../models/laya.zig");
const objective = @import("objective.zig");
const training = @import("training.zig");
const tree = @import("../../pipelines/laya_tree.zig");
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const files = @import("../../util/c_file.zig");

pub const Record = struct {
    id: []const u8,
    group_id: []const u8,
    text: []const u8,
    kind: model.QuestionType,
    instruction: []const u8,
    labels: []const []const u8,
    descriptions: ?[]const []const u8 = null,
    target: []const f32,
    /// Set only by `addStageTwo`, never by an input file: a synthetic
    /// joint-branch record for two-stage choice (LAYA.md, roadmap 2b). It is
    /// packed with `BranchStyle.question` instead of the config's own mode.
    is_stage_two: bool = false,
};
/// Where a record's question lives: `examples[example].question(question)`.
pub const Placement = struct { example: usize, question: usize };
pub const Dataset = struct {
    records: []const Record,
    examples: []const training.Example,
    /// One per record, in record order.
    placements: []const Placement,
    sha256: [32]u8,
};

pub fn digest(bytes: []const u8) [32]u8 {
    var out: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &out, .{});
    return out;
}

pub fn validate(r: Record) !void {
    if (r.id.len == 0 or r.id.len > 1024 or r.group_id.len == 0 or r.group_id.len > 1024 or r.text.len == 0 or r.text.len > 1024 * 1024 or r.instruction.len == 0 or r.instruction.len > 65536 or r.labels.len != r.target.len)
        return error.InvalidLayaTrainingRecord;
    try objective.validateTarget(r.kind, r.target);
    if (r.descriptions) |descs| if (descs.len != r.labels.len) return error.InvalidLayaTrainingRecord;
    for (r.labels, 0..) |label, i| {
        if (label.len == 0 or label.len > 65536) return error.InvalidLayaTrainingRecord;
        for (r.labels[0..i]) |prior| if (std.mem.eql(u8, label, prior)) return error.InvalidLayaTrainingRecord;
    }
    if (r.kind == .noul and (!std.mem.eql(u8, r.labels[0], "false") or !std.mem.eql(u8, r.labels[1], "true"))) return error.InvalidLayaTrainingRecord;
}

/// Sanity caps on one split. Memory is bounded separately by the caller's
/// arena (`max_host_bytes`); these only reject an obviously wrong input.
/// Sized for public typed-decision corpora (Open-Jev, 10^5 records).
pub const max_file_bytes = 1024 * 1024 * 1024;
pub const max_records = 1_000_000;

/// The dataset and tokenized sequences belong to the caller's bounded arena.
/// With tree packing, every record that shares a group and state text becomes
/// one question of the same packed example (pipelines/laya_tree.zig). `seed`
/// only matters when `cfg.packing.two_stage` is enabled: it drives the
/// deterministic negative sampling in `addStageTwo`.
pub fn load(a: std.mem.Allocator, path: []const u8, tok: Tokenizer, cfg: model.Config, seed: u64) !Dataset {
    const bytes = try files.readFileMax(a, path, max_file_bytes);
    var records: std.ArrayListUnmanaged(Record) = .empty;
    var examples: std.ArrayListUnmanaged(training.Example) = .empty;
    var placements: std.ArrayListUnmanaged(Placement) = .empty;
    var ids: std.StringHashMapUnmanaged(void) = .empty;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, " \t\r");
        if (line.len == 0) continue;
        if (records.items.len >= max_records) return error.LayaDatasetLimitExceeded;
        const parsed = try std.json.parseFromSlice(Record, a, line, .{ .allocate = .alloc_always });
        const record = parsed.value;
        try validate(record);
        const entry = try ids.getOrPut(a, record.id);
        if (entry.found_existing) return error.DuplicateLayaTrainingId;
        try records.append(a, record);
        if (cfg.packing.enabled()) continue;
        const sequence = try pipeline.prepare(a, tok, cfg, .{ .text = record.text, .question = try question(a, record) });
        try placements.append(a, .{ .example = examples.items.len, .question = 0 });
        try examples.append(a, .{ .ids = sequence.ids, .markers = sequence.markers, .kind = record.kind, .target = record.target });
    }
    if (records.items.len == 0) return error.EmptyLayaDataset;
    if (cfg.packing.enabled()) {
        if (cfg.packing.mode == .candidate and cfg.packing.two_stage.enabled())
            try addStageTwo(a, cfg.packing.two_stage.top_k, seed, &records, &ids);
        try pack(a, tok, cfg, records.items, &examples, &placements);
    }
    return .{ .records = try records.toOwnedSlice(a), .examples = try examples.toOwnedSlice(a), .placements = try placements.toOwnedSlice(a), .sha256 = digest(bytes) };
}

/// Add one synthetic joint-branch record per already-loaded `choice` record
/// whose option count exceeds `top_k` (LAYA.md, roadmap 2b): the gold label
/// plus `top_k - 1` other options sampled uniformly at random, seeded so a
/// run is reproducible. `pack` packs these with `BranchStyle.question`
/// instead of the config's own (candidate) style, so training sees both
/// branch shapes the served checkpoint must handle.
fn addStageTwo(a: std.mem.Allocator, top_k: usize, seed: u64, records: *std.ArrayListUnmanaged(Record), ids: *std.StringHashMapUnmanaged(void)) !void {
    var prng = std.Random.DefaultPrng.init(seed +% 0x9e3779b97f4a7c15);
    const random = prng.random();
    const original_count = records.items.len;
    for (0..original_count) |i| {
        const record = records.items[i];
        if (record.kind != .choice or record.labels.len <= top_k) continue;
        var gold: usize = 0;
        for (record.target, 0..) |t, k| {
            if (t > record.target[gold]) gold = k;
        }
        const order = try a.alloc(usize, record.labels.len);
        defer a.free(order);
        for (order, 0..) |*o, k| o.* = k;
        std.mem.swap(usize, &order[0], &order[gold]);
        random.shuffle(usize, order[1..]);
        const count = @min(top_k, order.len);
        const chosen = try a.dupe(usize, order[0..count]);
        defer a.free(chosen);
        std.mem.sort(usize, chosen, {}, std.sort.asc(usize));
        const descriptions = record.descriptions orelse blk: {
            const empty = try a.alloc([]const u8, record.labels.len);
            @memset(empty, "");
            break :blk empty;
        };
        const labels = try a.alloc([]const u8, count);
        const shortlist_descriptions = try a.alloc([]const u8, count);
        const target = try a.alloc(f32, count);
        for (chosen, labels, shortlist_descriptions, target) |idx, *l, *d, *t| {
            l.* = record.labels[idx];
            d.* = descriptions[idx];
            t.* = record.target[idx];
        }
        const synthetic_id = try std.fmt.allocPrint(a, "{s}#stage2", .{record.id});
        const entry = try ids.getOrPut(a, synthetic_id);
        if (entry.found_existing) return error.DuplicateLayaTrainingId;
        try records.append(a, .{
            .id = synthetic_id,
            .group_id = record.group_id,
            .text = record.text,
            .kind = record.kind,
            .instruction = record.instruction,
            .labels = labels,
            .descriptions = shortlist_descriptions,
            .target = target,
            .is_stage_two = true,
        });
    }
}

fn question(a: std.mem.Allocator, record: Record) !pipeline.Question {
    const descriptions = record.descriptions orelse blk: {
        const defaults = try a.alloc([]const u8, record.labels.len);
        @memset(defaults, "");
        break :blk defaults;
    };
    return .{ .name = record.id, .kind = record.kind, .instruction = record.instruction, .labels = record.labels, .descriptions = descriptions };
}

/// Group records by (group_id, text) in first-appearance order and pack each
/// group's stage-1 and stage-2 (`Record.is_stage_two`) questions separately
/// into as few tree rows as the physical budget allows. They are split so a
/// stage-2 record's joint branch (`BranchStyle.question`) never shares a row
/// with its group's ordinary candidate branches.
fn pack(a: std.mem.Allocator, tok: Tokenizer, cfg: model.Config, records: []const Record, examples: *std.ArrayListUnmanaged(training.Example), placements: *std.ArrayListUnmanaged(Placement)) !void {
    try placements.resize(a, records.len);
    const Group = struct { stage1: std.ArrayListUnmanaged(usize) = .empty, stage2: std.ArrayListUnmanaged(usize) = .empty };
    var groups: std.StringArrayHashMapUnmanaged(Group) = .empty;
    for (records, 0..) |record, i| {
        const key = try std.fmt.allocPrint(a, "{s}\x00{s}", .{ record.group_id, record.text });
        const entry = try groups.getOrPut(a, key);
        if (!entry.found_existing) entry.value_ptr.* = .{};
        if (record.is_stage_two) try entry.value_ptr.stage2.append(a, i) else try entry.value_ptr.stage1.append(a, i);
    }
    for (groups.values()) |group| {
        if (group.stage1.items.len > 0) try packGroup(a, tok, cfg, records, group.stage1.items, null, examples, placements);
        if (group.stage2.items.len > 0) try packGroup(a, tok, cfg, records, group.stage2.items, .question, examples, placements);
    }
}

/// Pack one group's members into as few tree rows as the physical budget
/// allows, with `style` (null keeps `cfg.packing.mode`'s own branch style).
fn packGroup(a: std.mem.Allocator, tok: Tokenizer, cfg: model.Config, records: []const Record, members: []const usize, style: ?tree.BranchStyle, examples: *std.ArrayListUnmanaged(training.Example), placements: *std.ArrayListUnmanaged(Placement)) !void {
    const questions = try a.alloc(pipeline.Question, members.len);
    for (questions, members) |*q, index| q.* = try question(a, records[index]);
    const text = records[members[0]].text;
    for (try tree.build(a, tok, cfg, text, questions, style)) |row| {
        const kinds = try a.alloc(model.QuestionType, row.questions());
        const targets = try a.alloc([]const f32, row.questions());
        for (row.question_index, kinds, targets, 0..) |local, *kind, *target, qi| {
            const index = members[local];
            kind.* = records[index].kind;
            target.* = records[index].target;
            placements.items[index] = .{ .example = examples.items.len, .question = qi };
        }
        const packed_row = try a.create(training.Packed);
        packed_row.* = .{ .row = row, .kinds = kinds, .targets = targets };
        try examples.append(a, .{ .ids = row.ids, .packed_row = packed_row });
    }
}

/// Cases stay together across all questions. Also catch renamed copies of
/// source text and token-identical examples, even with different IDs/groups.
pub fn disjoint(a: std.mem.Allocator, left: Dataset, right: Dataset) !void {
    var groups: std.StringHashMapUnmanaged(void) = .empty;
    defer groups.deinit(a);
    var ids: std.StringHashMapUnmanaged(void) = .empty;
    defer ids.deinit(a);
    var texts: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer texts.deinit(a);
    var tokens: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer tokens.deinit(a);
    for (left.records, left.placements) |record, place| {
        try groups.put(a, record.group_id, {});
        try ids.put(a, record.id, {});
        try texts.put(a, digest(record.text), {});
        try tokens.put(a, digest(std.mem.sliceAsBytes(left.examples[place.example].ids)), {});
    }
    for (right.records, right.placements) |record, place| {
        if (groups.contains(record.group_id) or ids.contains(record.id) or texts.contains(digest(record.text)) or tokens.contains(digest(std.mem.sliceAsBytes(right.examples[place.example].ids))))
            return error.LayaDatasetOverlap;
    }
}

test "laya training records reject mismatched targets and boolean order" {
    var record = Record{ .id = "case1/decision1", .group_id = "case1", .text = "hello", .kind = .noul, .instruction = "is this a greeting?", .labels = &.{ "false", "true" }, .target = &.{ 0.1, 0.9 } };
    try validate(record);
    record.labels = &.{ "true", "false" };
    try std.testing.expectError(error.InvalidLayaTrainingRecord, validate(record));
    record.kind = .choice;
    record.labels = &.{ "a", "a" };
    try std.testing.expectError(error.InvalidLayaTrainingRecord, validate(record));
}

test "laya stage-two synthesis keeps the gold label, renormalizes the target, and is deterministic" {
    const a = std.testing.allocator;
    const target = [_]f32{ 0, 0, 0, 1, 0 };
    const source = Record{
        .id = "case1",
        .group_id = "case1",
        .text = "hello",
        .kind = .choice,
        .instruction = "which?",
        .labels = &.{ "a", "b", "c", "d", "e" },
        .descriptions = &.{ "", "", "", "", "" },
        .target = &target,
    };
    var records: std.ArrayListUnmanaged(Record) = .empty;
    defer records.deinit(a);
    try records.append(a, source);
    var ids: std.StringHashMapUnmanaged(void) = .empty;
    defer ids.deinit(a);
    try addStageTwo(a, 3, 7, &records, &ids);
    try std.testing.expectEqual(@as(usize, 2), records.items.len);
    const synthetic = records.items[1];
    defer {
        a.free(synthetic.id);
        a.free(synthetic.labels);
        a.free(synthetic.descriptions.?);
        a.free(synthetic.target);
    }
    try std.testing.expect(synthetic.is_stage_two);
    try std.testing.expectEqualStrings("case1#stage2", synthetic.id);
    try std.testing.expectEqualStrings("case1", synthetic.group_id);
    try std.testing.expectEqualStrings("hello", synthetic.text);
    try std.testing.expectEqual(@as(usize, 3), synthetic.labels.len);
    try std.testing.expectEqual(synthetic.labels.len, synthetic.target.len);
    var gold_seen = false;
    var sum: f32 = 0;
    for (synthetic.labels, synthetic.target) |label, t| {
        sum += t;
        if (std.mem.eql(u8, label, "d")) {
            try std.testing.expectEqual(@as(f32, 1), t);
            gold_seen = true;
        } else try std.testing.expectEqual(@as(f32, 0), t);
    }
    try std.testing.expect(gold_seen);
    try std.testing.expectApproxEqAbs(@as(f32, 1), sum, 1e-6);

    // A source with at most `top_k` labels needs no shortlisting.
    var small: std.ArrayListUnmanaged(Record) = .empty;
    defer small.deinit(a);
    var narrow = source;
    narrow.labels = source.labels[0..3];
    narrow.target = target[0..3];
    try small.append(a, narrow);
    var small_ids: std.StringHashMapUnmanaged(void) = .empty;
    defer small_ids.deinit(a);
    try addStageTwo(a, 3, 7, &small, &small_ids);
    try std.testing.expectEqual(@as(usize, 1), small.items.len);

    // The same seed picks the same shortlist.
    var again: std.ArrayListUnmanaged(Record) = .empty;
    defer again.deinit(a);
    try again.append(a, source);
    var again_ids: std.StringHashMapUnmanaged(void) = .empty;
    defer again_ids.deinit(a);
    try addStageTwo(a, 3, 7, &again, &again_ids);
    defer {
        a.free(again.items[1].id);
        a.free(again.items[1].labels);
        a.free(again.items[1].descriptions.?);
        a.free(again.items[1].target);
    }
    try std.testing.expectEqual(synthetic.labels.len, again.items[1].labels.len);
    for (synthetic.labels, again.items[1].labels) |want, got| try std.testing.expectEqualStrings(want, got);
}

test "laya training split separation catches renamed text and token duplicates" {
    const r = Record{ .id = "a", .group_id = "g", .text = "text", .kind = .choice, .instruction = "choose", .labels = &.{ "a", "b" }, .target = &.{ 1, 0 } };
    const e = training.Example{ .ids = &.{ 1, 2 }, .markers = &.{ 0, 1 }, .kind = .choice, .target = &.{ 1, 0 } };
    const left = Dataset{ .records = &.{r}, .examples = &.{e}, .placements = &.{.{ .example = 0, .question = 0 }}, .sha256 = @as([32]u8, @splat(0)) };
    var changed = r;
    changed.id = "b";
    changed.group_id = "h";
    var other = e;
    other.ids = &.{ 3, 4 };
    var right = Dataset{ .records = &.{changed}, .examples = &.{other}, .placements = &.{.{ .example = 0, .question = 0 }}, .sha256 = @as([32]u8, @splat(0)) };
    try std.testing.expectError(error.LayaDatasetOverlap, disjoint(std.testing.allocator, left, right));
    changed.text = "different text";
    right.records = &.{changed};
    try disjoint(std.testing.allocator, left, right);
    right.examples = &.{e};
    try std.testing.expectError(error.LayaDatasetOverlap, disjoint(std.testing.allocator, left, right));
}
