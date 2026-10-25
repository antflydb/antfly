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

//! Media outputs have independent recipes. Only the selected boundary recipe
//! participates in planning; adding consumers cannot change parent identities.
const std = @import("std");

pub const Kind = enum { embedding, transcribe, classify };
pub const Target = struct {
    name: []const u8,
    kind: Kind,
    model: []const u8,
    required: bool = true,
    execute: bool = true,
    /// Execution-only receipts which the caller already owns. Parent identity
    /// and recipe hashes never depend on this selective-repair mask.
    skip_windows: []const Window = &.{},
    model_identity: ?[]const u8 = null,
    task_type: []const u8 = "RETRIEVAL_DOCUMENT",
    dimensions: ?usize = null,
    language: ?[]const u8 = null,
    diarize: bool = false,
    top_k: usize = 5,

    pub const Window = struct { ordinal: u64, sub_index: u32 };
    pub fn wants(self: Target, ordinal: u64, sub_index: u32) bool {
        if (!self.execute) return false;
        for (self.skip_windows) |window| if (window.ordinal == ordinal and window.sub_index == sub_index) return false;
        return true;
    }

    pub fn recipeIdentity(self: Target, a: std.mem.Allocator, identity: []const u8, revision: []const u8) ![64]u8 {
        var recipe = self;
        recipe.model_identity = null;
        recipe.execute = true;
        recipe.skip_windows = &.{};
        recipe.required = true; // Coverage policy does not change inference work.
        // Keep the original v1 canonical field set: adding an execution mask
        // must not invalidate every previously accepted model computation.
        const stable = .{ .name = recipe.name, .kind = recipe.kind, .model = recipe.model, .required = recipe.required, .execute = recipe.execute, .model_identity = recipe.model_identity, .task_type = recipe.task_type, .dimensions = recipe.dimensions, .language = recipe.language, .diarize = recipe.diarize, .top_k = recipe.top_k };
        const encoded = try std.json.Stringify.valueAlloc(a, .{ .format = "antfly-media-output-v1", .target = stable, .model_identity = identity, .model_revision = revision }, .{ .emit_null_optional_fields = false });
        defer a.free(encoded);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(encoded, &digest, .{});
        return std.fmt.bytesToHex(digest, .lower);
    }
};
pub const Config = struct {
    boundary_model: []const u8,
    targets: []const Target,

    /// The pre-existing embedding member is a named compatibility output.
    pub fn includingEmbedding(self: Config, a: std.mem.Allocator, legacy: Target) !Config {
        for (self.targets) |target| if (std.mem.eql(u8, target.name, "embedding")) {
            if (target.kind != .embedding or !std.mem.eql(u8, target.model, legacy.model) or target.dimensions != legacy.dimensions or !std.mem.eql(u8, target.task_type, legacy.task_type)) return error.InvalidChunkOutputs;
            if (legacy.model_identity) |pin| {
                if (target.model_identity) |named| {
                    if (!std.mem.eql(u8, pin, named)) return error.InvalidChunkOutputs;
                } else {
                    const targets = try a.dupe(Target, self.targets);
                    for (targets) |*value| if (std.mem.eql(u8, value.name, "embedding")) {
                        value.model_identity = pin;
                    };
                    return .{ .boundary_model = self.boundary_model, .targets = targets };
                }
            }
            return self;
        };
        if (self.targets.len >= 16) return error.InvalidChunkOutputs;
        const targets = try a.alloc(Target, self.targets.len + 1);
        @memcpy(targets[0..self.targets.len], self.targets);
        targets[self.targets.len] = legacy;
        return .{ .boundary_model = self.boundary_model, .targets = targets };
    }

    pub fn validate(self: Config) !void {
        if (self.targets.len == 0 or self.targets.len > 16) return error.InvalidChunkOutputs;
        var found = false;
        for (self.targets, 0..) |target, index| {
            if (target.name.len == 0 or target.name.len > 128 or target.model.len == 0 or target.model.len > 4096 or !std.unicode.utf8ValidateSlice(target.name)) return error.InvalidChunkOutputs;
            if (target.skip_windows.len > 4096) return error.InvalidChunkOutputs;
            if (target.dimensions == 0 or target.top_k == 0 or target.top_k > 128) return error.InvalidChunkOutputs;
            if (target.model_identity) |identity| {
                if (identity.len != 64) return error.InvalidChunkOutputs;
                for (identity) |c| if (!std.ascii.isDigit(c) and (c < 'a' or c > 'f')) return error.InvalidChunkOutputs;
            }
            for (self.targets[0..index]) |earlier| if (std.mem.eql(u8, target.name, earlier.name)) return error.InvalidChunkOutputs;
            if (std.mem.eql(u8, target.name, self.boundary_model)) {
                if (target.kind == .classify) return error.InvalidChunkBoundaryModel;
                found = true;
            }
        }
        if (!found) return error.InvalidChunkBoundaryModel;
    }
    pub fn boundary(self: Config) !Target {
        try self.validate();
        for (self.targets) |target| if (std.mem.eql(u8, target.name, self.boundary_model)) return target;
        unreachable;
    }
};
pub const Span = struct {
    text: []const u8,
    start_ns: i64,
    end_ns: i64,
    /// Local to this output sub-window. Speaker numbering across independent
    /// windows requires an explicit diarization stitching stage.
    speaker_index: ?u32 = null,
};
pub const Label = struct { label: []const u8, score: f32 };
pub const Result = struct {
    name: []const u8,
    kind: Kind,
    required: bool,
    model: []const u8,
    model_identity: []const u8,
    model_revision: []const u8,
    recipe_id: []const u8,
    sub_index: u32 = 0,
    start_ns: ?i64 = null,
    end_ns: ?i64 = null,
    status: enum { complete, failed, skipped },
    code: ?[]const u8 = null,
    attempts: u32 = 0,
    terminal: bool = false,
    embedding: ?[]const f32 = null,
    spans: ?[]const Span = null,
    labels: ?[]const Label = null,

    pub fn reusable(self: Result) bool {
        return (self.status == .failed and self.terminal) or (self.status == .complete and switch (self.kind) {
            .embedding => self.embedding != null,
            .transcribe => self.spans != null,
            .classify => self.labels != null,
        });
    }

    pub fn validate(self: Result, target: Target) !void {
        if (!std.mem.eql(u8, self.name, target.name) or self.kind != target.kind or self.required != target.required or !std.mem.eql(u8, self.model, target.model)) return error.InvalidChunkOutput;
        if ((self.start_ns == null) != (self.end_ns == null)) return error.InvalidChunkOutput;
        if (self.start_ns) |start| if (start < 0 or self.end_ns.? < start) return error.InvalidChunkOutput;
        if (self.status == .skipped) {
            if ((target.execute and target.skip_windows.len == 0) or self.code != null or self.embedding != null or self.spans != null or self.labels != null) return error.InvalidChunkOutput;
            return;
        }
        if (self.status == .failed) {
            if (self.code == null or self.embedding != null or self.spans != null or self.labels != null) return error.InvalidChunkOutput;
            return;
        }
        if (self.terminal or self.code != null or self.model_identity.len != 64 or self.model_revision.len != 64 or self.recipe_id.len != 64) return error.InvalidChunkOutput;
        if (target.model_identity) |expected| if (!std.mem.eql(u8, expected, self.model_identity)) return error.EmbeddingIdentityMismatch;
        var digest: [32]u8 = undefined;
        for ([_][]const u8{ self.model_identity, self.model_revision, self.recipe_id }) |value| _ = std.fmt.hexToBytes(&digest, value) catch return error.InvalidChunkOutput;

        switch (self.kind) {
            .embedding => {
                if (self.spans != null or self.labels != null) return error.InvalidChunkOutput;
                const vector = self.embedding orelse return error.InvalidChunkOutput;
                if (vector.len == 0 or (target.dimensions != null and target.dimensions.? != vector.len)) return error.InvalidChunkOutput;
                for (vector) |value| if (!std.math.isFinite(value)) return error.InvalidChunkOutput;
            },
            .transcribe => {
                if (self.embedding != null or self.labels != null) return error.InvalidChunkOutput;
                const spans = self.spans orelse return error.InvalidChunkOutput;
                const start = self.start_ns orelse return error.InvalidChunkOutput;
                const end = self.end_ns orelse return error.InvalidChunkOutput;
                var previous = start;
                for (spans) |span| {
                    if (!std.unicode.utf8ValidateSlice(span.text) or span.start_ns < previous or span.end_ns < span.start_ns or span.end_ns > end) return error.InvalidChunkOutput;
                    previous = span.start_ns;
                }
            },
            .classify => {
                if (self.embedding != null or self.spans != null) return error.InvalidChunkOutput;
                const labels = self.labels orelse return error.InvalidChunkOutput;
                if (labels.len > target.top_k) return error.InvalidChunkOutput;
                var previous: f32 = std.math.inf(f32);
                for (labels, 0..) |label, index| {
                    if (label.label.len == 0 or !std.math.isFinite(label.score) or label.score < 0 or label.score > 1 or label.score > previous) return error.InvalidChunkOutput;
                    for (labels[0..index]) |earlier| if (std.mem.eql(u8, earlier.label, label.label)) return error.InvalidChunkOutput;
                    previous = label.score;
                }
            },
        }
    }
};

/// Sample-aligned sub-windows never participate in parent plan identity.
/// Integer clocks avoid accumulating rounding drift at non-kHz sample rates.
pub const AudioWindows = struct {
    samples: usize,
    sample_rate: u32,
    max_ns: i64,
    parent_start_ns: i64,
    position: usize = 0,
    sub_index: u32 = 0,
    pub const Window = struct { first: usize, last: usize, sub_index: u32, start_ns: i64, end_ns: i64 };
    pub fn next(self: *AudioWindows) !?Window {
        if (self.sample_rate == 0 or self.max_ns <= 0 or self.parent_start_ns < 0) return error.InvalidChunkCoordinates;
        if (self.position == self.samples) return null;
        const count = std.math.cast(usize, @divFloor(@as(u128, @intCast(self.max_ns)) * self.sample_rate, std.time.ns_per_s)) orelse return error.InvalidChunkCoordinates;
        if (count == 0) return error.InvalidChunkCoordinates;
        const first = self.position;
        const last = first + @min(count, self.samples - first);
        const start_ns = try std.math.add(i64, self.parent_start_ns, std.math.cast(i64, @divFloor(@as(u128, first) * std.time.ns_per_s, self.sample_rate)) orelse return error.InvalidChunkCoordinates);
        const end_ns = try std.math.add(i64, self.parent_start_ns, std.math.cast(i64, @divFloor(@as(u128, last) * std.time.ns_per_s, self.sample_rate)) orelse return error.InvalidChunkCoordinates);
        const result = Window{ .first = first, .last = last, .sub_index = self.sub_index, .start_ns = start_ns, .end_ns = end_ns };
        self.position = last;
        self.sub_index = try std.math.add(u32, self.sub_index, 1);
        return result;
    }
};

test "model aware media output boundary remains explicit when independent consumers change" {
    const targets = [_]Target{ .{ .name = "clip", .kind = .embedding, .model = "clip-v1" }, .{ .name = "speech", .kind = .transcribe, .model = "whisper-v1" } };
    const config = Config{ .boundary_model = "clip", .targets = &targets };
    try std.testing.expectEqualStrings("clip-v1", (try config.boundary()).model);
    try std.testing.expectError(error.InvalidChunkBoundaryModel, (Config{ .boundary_model = "missing", .targets = &targets }).validate());
    try std.testing.expectError(error.InvalidChunkOutputs, (Config{ .boundary_model = "clip", .targets = &.{ targets[0], targets[0] } }).validate());
}

test "model aware media output audio subwindows retain parent clocks and complete tails" {
    var windows = AudioWindows{ .samples = 32 * 44100 + 7, .sample_rate = 44100, .max_ns = 30 * std.time.ns_per_s, .parent_start_ns = 64 * std.time.ns_per_s };
    const first = (try windows.next()).?;
    const last = (try windows.next()).?;
    try std.testing.expectEqual(@as(u32, 0), first.sub_index);
    try std.testing.expectEqual(@as(u32, 1), last.sub_index);
    try std.testing.expectEqual(first.last, last.first);
    try std.testing.expectEqual(first.end_ns, last.start_ns);
    try std.testing.expectEqual(windows.samples, last.last);
    try std.testing.expect(try windows.next() == null);
}

pub const Row = struct { chunk_id: u32, outputs: []const Result };
pub const TranscriptSpan = struct {
    char_start: u32,
    char_end: u32,
    start_ns: i64,
    end_ns: i64,
    parent_ordinal: u32,
    sub_index: u32,
    speaker_index: ?u32,
};
pub const Projection = struct {
    text: []const u8,
    transcript_spans: []const TranscriptSpan,
    complete_chunks: u64 = 0,
    failed_chunks: u64 = 0,
    terminal_chunks: u64 = 0,
    status: enum { complete, degraded, failed } = .complete,
};

/// The caller owns an arena. Timing remains attached to original utterances;
/// overlapping windows and local speaker scopes are explicit, never silently
/// merged into invented global speaker identities or interpolated timestamps.
pub fn project(a: std.mem.Allocator, rows: []const Row) !Projection {
    var text = std.ArrayList(u8).empty;
    var spans = std.ArrayList(TranscriptSpan).empty;
    var projection = Projection{ .text = "", .transcript_spans = &.{} };
    for (rows) |row| {
        var failed = false;
        var terminal = false;
        for (row.outputs) |output| {
            if (output.status == .failed) {
                failed = true;
                terminal = terminal or output.terminal;
                if (output.required) projection.status = .failed else if (projection.status != .failed) {
                    projection.status = .degraded;
                }
                continue;
            }
            if (output.spans) |phrases| for (phrases) |phrase| {
                if (phrase.text.len == 0) continue;
                if (text.items.len + phrase.text.len + 1 > 16 * 1024 * 1024) return error.ResourceLimitExceeded;
                if (text.items.len != 0) try text.append(a, ' ');
                const first: u32 = @intCast(text.items.len);
                try text.appendSlice(a, phrase.text);
                try spans.append(a, .{ .char_start = first, .char_end = @intCast(text.items.len), .start_ns = phrase.start_ns, .end_ns = phrase.end_ns, .parent_ordinal = row.chunk_id, .sub_index = output.sub_index, .speaker_index = phrase.speaker_index });
            };
        }
        if (failed) projection.failed_chunks += 1 else projection.complete_chunks += 1;
        if (terminal) projection.terminal_chunks += 1;
    }
    projection.text = try text.toOwnedSlice(a);
    projection.transcript_spans = try spans.toOwnedSlice(a);
    return projection;
}

pub const CoverageValues = struct { complete: u64 = 0, failed: u64 = 0, terminal: u64 = 0, degraded: u64 = 0 };
pub const CoverageCounts = struct {
    embedding: CoverageValues = .{},
    transcribe: CoverageValues = .{},
    classify: CoverageValues = .{},
    pub fn add(self: CoverageCounts, delta: CoverageCounts) CoverageCounts {
        var result = self;
        inline for (.{ "embedding", "transcribe", "classify" }) |kind| inline for (.{ "complete", "failed", "terminal", "degraded" }) |name| {
            @field(@field(result, kind), name) +|= @field(@field(delta, kind), name);
        };
        return result;
    }
    pub fn mergeMaximum(self: CoverageCounts, other: CoverageCounts) CoverageCounts {
        var result = self;
        inline for (.{ "embedding", "transcribe", "classify" }) |kind| inline for (.{ "complete", "failed", "terminal", "degraded" }) |name| {
            @field(@field(result, kind), name) = @max(@field(@field(result, kind), name), @field(@field(other, kind), name));
        };
        return result;
    }
};

test "model aware media legacy embedding pins merge without weakening either declaration" {
    const a = std.testing.allocator;
    const pin: [64]u8 = @splat('a');
    const other: [64]u8 = @splat('b');
    const legacy = Target{ .name = "embedding", .kind = .embedding, .model = "model", .model_identity = &pin, .dimensions = 2 };
    var named = legacy;
    named.model_identity = &other;
    try std.testing.expectError(error.InvalidChunkOutputs, (Config{ .boundary_model = "embedding", .targets = &.{named} }).includingEmbedding(a, legacy));
    named.model_identity = null;
    const merged = try (Config{ .boundary_model = "embedding", .targets = &.{named} }).includingEmbedding(a, legacy);
    defer a.free(merged.targets);
    try std.testing.expectEqualStrings(&pin, merged.targets[0].model_identity.?);
}

/// Merge independent receipts without regressing accepted or terminal work.
/// Allocations belong to the caller's arena.
pub fn mergeResults(a: std.mem.Allocator, fresh: []const Result, cached: []const Result, max_attempts: u32) ![]const Result {
    var results = std.ArrayList(Result).empty;
    errdefer results.deinit(a);
    var counts = std.StringHashMap(usize).init(a);
    defer counts.deinit();
    for (fresh) |value| {
        const entry = try counts.getOrPut(value.name);
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* += 1;
    }
    const Key = struct { name: []const u8, recipe: []const u8, sub_index: u32 };
    const Context = struct {
        pub fn hash(_: @This(), value: Key) u64 {
            var digest = std.hash.Wyhash.init(0);
            digest.update(value.name);
            digest.update(&.{0});
            digest.update(value.recipe);
            digest.update(std.mem.asBytes(&value.sub_index));
            return digest.final();
        }
        pub fn eql(_: @This(), left: Key, right: Key) bool {
            return left.sub_index == right.sub_index and std.mem.eql(u8, left.name, right.name) and std.mem.eql(u8, left.recipe, right.recipe);
        }
    };
    var previous_by_window = std.HashMap(Key, Result, Context, 80).init(a);
    defer previous_by_window.deinit();
    for (cached) |value| {
        const entry = try previous_by_window.getOrPut(.{ .name = value.name, .recipe = value.recipe_id, .sub_index = value.sub_index });
        if (entry.found_existing) return error.InvalidChunkOutput;
        entry.value_ptr.* = value;
    }
    for (fresh) |incoming| {
        // A skipped target is a single recipe-level placeholder, regardless of
        // how many windows that target produced. Restore the entire result set.
        if (incoming.status == .skipped) {
            // execute:false emits one target-level placeholder. Selective
            // execution emits an ordered receipt for each actual sub-window.
            const whole_target = counts.get(incoming.name).? == 1;
            if (!whole_target) {
                var old = previous_by_window.get(.{ .name = incoming.name, .recipe = incoming.recipe_id, .sub_index = incoming.sub_index }) orelse return error.IncompleteChunkCoverage;
                if (!old.reusable()) return error.IncompleteChunkCoverage;
                old.required = incoming.required;
                try results.append(a, old);
                continue;
            }
            var found = false;
            for (cached) |old| {
                if (!std.mem.eql(u8, old.name, incoming.name) or !std.mem.eql(u8, old.recipe_id, incoming.recipe_id)) continue;
                if (!old.reusable()) return error.IncompleteChunkCoverage;
                var restored = old;
                restored.required = incoming.required;
                try results.append(a, restored);
                found = true;
            }
            if (!found) return error.IncompleteChunkCoverage;
            continue;
        }
        var value = incoming;
        const previous = previous_by_window.get(.{ .name = value.name, .recipe = value.recipe_id, .sub_index = value.sub_index });
        if (previous) |old| if (old.reusable()) {
            const required = value.required;
            value = old;
            value.required = required;
            try results.append(a, value);
            continue;
        };
        if (value.status == .skipped) return error.IncompleteChunkCoverage;
        value.attempts = (if (previous) |old| old.attempts else @as(u32, 0)) +| 1;
        value.terminal = value.status == .failed and value.attempts >= @max(max_attempts, 1);
        try results.append(a, value);
    }
    return results.toOwnedSlice(a);
}

test "model aware media selective receipts preserve healthy windows and independent attempt budgets" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const target = Target{ .name = "frames", .kind = .classify, .model = "model" };
    const recipe = try target.recipeIdentity(a, "identity", "revision");
    const base = Result{ .name = "frames", .kind = .classify, .required = true, .model = "model", .model_identity = "identity", .model_revision = "revision", .recipe_id = &recipe, .status = .complete, .labels = &.{.{ .label = "speech", .score = 1 }}, .attempts = 1 };
    var cached: [32]Result = undefined;
    var fresh: [32]Result = undefined;
    var skips: [31]Target.Window = undefined;
    for (&cached, &fresh, 0..) |*old, *next, i| {
        old.* = base;
        old.sub_index = @intCast(i);
        next.* = old.*;
        next.status = .skipped;
        next.labels = null;
        if (i < skips.len) skips[i] = .{ .ordinal = 7, .sub_index = @intCast(i) };
    }
    cached[31].status = .failed;
    cached[31].labels = null;
    cached[31].code = "Unavailable";
    fresh[31] = base;
    fresh[31].sub_index = 31;
    var selective = target;
    selective.skip_windows = &skips;
    try std.testing.expectEqualSlices(u8, &recipe, &try selective.recipeIdentity(a, "identity", "revision"));
    var executed: usize = 0;
    for (0..32) |i| if (selective.wants(7, @intCast(i))) {
        executed += 1;
    };
    try std.testing.expectEqual(@as(usize, 1), executed);
    try std.testing.expect(selective.wants(8, 0));
    const merged = try mergeResults(arena.allocator(), &fresh, &cached, 3);
    try std.testing.expectEqual(@as(usize, 32), merged.len);
    for (merged[0..31]) |value| {
        try std.testing.expect(value.status == .complete);
        try std.testing.expectEqual(@as(u32, 1), value.attempts);
    }
    try std.testing.expectEqual(@as(u32, 2), merged[31].attempts);
    cached[31].attempts = 3;
    cached[31].terminal = true;
    fresh[31].status = .failed;
    fresh[31].labels = null;
    fresh[31].code = "Unavailable";
    const terminal = try mergeResults(arena.allocator(), &fresh, &cached, 3);
    try std.testing.expect(terminal[31].terminal);
    try std.testing.expectEqual(@as(u32, 3), terminal[31].attempts);
    // Compacted recipes without a hydrated payload are debt, not reusable
    // completed work. A fresh successful result must fill that missing data.
    cached[0].labels = null;
    fresh[0] = base;
    const hydrated = try mergeResults(arena.allocator(), &fresh, &cached, 3);
    try std.testing.expect(hydrated[0].labels != null);
    try std.testing.expectEqual(@as(u32, 2), hydrated[0].attempts);
}
