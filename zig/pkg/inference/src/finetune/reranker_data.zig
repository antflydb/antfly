// Copyright 2026 Antfly, Inc.
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
const compat = @import("../io/compat.zig");

pub const Example = struct {
    query: []const u8,
    document: []const u8,
    score: f32,
};

pub const DatasetStats = struct {
    num_examples: usize = 0,
    num_query_groups: usize = 0,
    avg_query_chars: f64 = 0,
    avg_document_chars: f64 = 0,
    avg_score: f64 = 0,
    min_score: f32 = 0,
    max_score: f32 = 0,
    avg_examples_per_query: f64 = 0,
    max_examples_per_query: usize = 0,
};

pub const LoadedExamples = struct {
    arena: std.heap.ArenaAllocator,
    dataset_root: []const u8,
    examples: []Example,

    pub fn deinit(self: *LoadedExamples) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

const ResolvedFiles = struct {
    arena: std.heap.ArenaAllocator,
    base_dir: []const u8,
    paths: [][]const u8,

    fn deinit(self: *ResolvedFiles) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn loadExamples(allocator: std.mem.Allocator, path: []const u8, split: ?[]const u8) !LoadedExamples {
    var resolved = try resolveJsonlFiles(allocator, path, split);
    defer resolved.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const arena_alloc = arena.allocator();

    var examples = std.ArrayListUnmanaged(Example).empty;
    defer examples.deinit(arena_alloc);

    for (resolved.paths) |resolved_path| {
        try loadExamplesFromFile(arena_alloc, resolved_path, &examples);
    }

    const dataset_root = try arena_alloc.dupe(u8, resolved.base_dir);
    const owned_examples = try examples.toOwnedSlice(arena_alloc);
    return .{
        .arena = arena,
        .dataset_root = dataset_root,
        .examples = owned_examples,
    };
}

pub fn computeStats(examples: []const Example) DatasetStats {
    var stats = DatasetStats{
        .num_examples = examples.len,
    };
    if (examples.len == 0) return stats;

    var query_chars: usize = 0;
    var document_chars: usize = 0;
    var total_score: f64 = 0;
    stats.min_score = examples[0].score;
    stats.max_score = examples[0].score;

    var group_start: usize = 0;
    while (group_start < examples.len) {
        var group_end = group_start + 1;
        while (group_end < examples.len and std.mem.eql(u8, examples[group_end].query, examples[group_start].query)) : (group_end += 1) {}
        const group_size = group_end - group_start;
        stats.num_query_groups += 1;
        stats.max_examples_per_query = @max(stats.max_examples_per_query, group_size);
        group_start = group_end;
    }

    for (examples) |example| {
        query_chars += example.query.len;
        document_chars += example.document.len;
        total_score += example.score;
        stats.min_score = @min(stats.min_score, example.score);
        stats.max_score = @max(stats.max_score, example.score);
    }

    const n = @as(f64, @floatFromInt(examples.len));
    stats.avg_query_chars = @as(f64, @floatFromInt(query_chars)) / n;
    stats.avg_document_chars = @as(f64, @floatFromInt(document_chars)) / n;
    stats.avg_score = total_score / n;
    if (stats.num_query_groups > 0) {
        stats.avg_examples_per_query = n / @as(f64, @floatFromInt(stats.num_query_groups));
    }
    return stats;
}

pub fn limitExamplesByCount(allocator: std.mem.Allocator, examples: []const Example, max_examples: usize) ![]Example {
    const limit = @min(examples.len, max_examples);
    return try allocator.dupe(Example, examples[0..limit]);
}

pub fn countPairwiseTrainingPairs(examples: []const Example) usize {
    var pairs: usize = 0;
    var group_start: usize = 0;
    while (group_start < examples.len) {
        var group_end = group_start + 1;
        while (group_end < examples.len and std.mem.eql(u8, examples[group_end].query, examples[group_start].query)) : (group_end += 1) {}
        const group_size = group_end - group_start;
        if (group_size >= 2) {
            pairs += (group_size * (group_size - 1)) / 2;
        }
        group_start = group_end;
    }
    return pairs;
}

fn resolveJsonlFiles(allocator: std.mem.Allocator, path: []const u8, split: ?[]const u8) !ResolvedFiles {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const arena_alloc = arena.allocator();

    if (std.mem.trim(u8, path, " \t\r\n").len == 0) return error.EmptyPath;
    const stat = try compat.cwd().statFile(compat.io(), path, .{});
    if (stat.kind == .file) {
        const one = try arena_alloc.alloc([]const u8, 1);
        one[0] = try arena_alloc.dupe(u8, path);
        const base_dir = try arena_alloc.dupe(u8, std.fs.path.dirname(path) orelse ".");
        return .{
            .arena = arena,
            .base_dir = base_dir,
            .paths = one,
        };
    }
    if (stat.kind != .directory) return error.UnsupportedPathType;

    var dir = try compat.cwd().openDir(compat.io(), path, .{ .iterate = true });
    defer dir.close(compat.io());
    var iter = dir.iterate();
    var paths = std.ArrayListUnmanaged([]const u8).empty;
    defer paths.deinit(arena_alloc);
    while (try iter.next(compat.io())) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".jsonl")) continue;
        if (split) |want_split| {
            const prefix = try std.fmt.allocPrint(arena_alloc, "{s}-", .{want_split});
            if (!std.mem.startsWith(u8, entry.name, prefix)) continue;
        }
        try paths.append(arena_alloc, try std.fs.path.join(arena_alloc, &.{ path, entry.name }));
    }
    if (paths.items.len == 0) return error.NoJsonlFilesForSplit;
    std.mem.sort([]const u8, paths.items, {}, lessThanString);
    const base_dir = try arena_alloc.dupe(u8, path);
    const owned_paths = try paths.toOwnedSlice(arena_alloc);
    return .{
        .arena = arena,
        .base_dir = base_dir,
        .paths = owned_paths,
    };
}

fn loadExamplesFromFile(allocator: std.mem.Allocator, path: []const u8, out: *std.ArrayListUnmanaged(Example)) !void {
    const data = try compat.cwd().readFileAlloc(compat.io(), path, allocator, .limited(64 * 1024 * 1024));
    var lines = std.mem.tokenizeScalar(u8, data, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0) continue;
        const parsed = try std.json.parseFromSliceLeaky(Example, allocator, line, .{
            .ignore_unknown_fields = true,
        });
        if (std.mem.trim(u8, parsed.query, " \t\r\n").len == 0) return error.MissingQuery;
        if (std.mem.trim(u8, parsed.document, " \t\r\n").len == 0) return error.MissingDocument;
        try out.append(allocator, parsed);
    }
}

fn lessThanString(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.lessThan(u8, lhs, rhs);
}

test "load reranker examples and compute grouped stats" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_reranker_data_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    try compat.cwd().createDirPath(compat.io(), root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};

    const manifest_json =
        \\{
        \\  "schema_version": 1,
        \\  "family": "reranker",
        \\  "files": [
        \\    { "relative_path": "train-00000.jsonl", "kind": "jsonl", "split": "train", "records": 3, "size": 10, "digest": "sha256:a" }
        \\  ]
        \\}
    ;
    const train_jsonl =
        \\{"query":"q1","document":"d1","score":1.0}
        \\{"query":"q1","document":"d2","score":0.0}
        \\{"query":"q2","document":"d3","score":0.5}
        \\
    ;
    const manifest_path = try std.fs.path.join(allocator, &.{ root, "manifest.json" });
    defer allocator.free(manifest_path);
    const train_path = try std.fs.path.join(allocator, &.{ root, "train-00000.jsonl" });
    defer allocator.free(train_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = manifest_path, .data = manifest_json });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = train_path, .data = train_jsonl });

    var loaded = try loadExamples(allocator, root, "train");
    defer loaded.deinit();
    try std.testing.expectEqualStrings(root, loaded.dataset_root);
    try std.testing.expectEqual(@as(usize, 3), loaded.examples.len);

    const stats = computeStats(loaded.examples);
    try std.testing.expectEqual(@as(usize, 2), stats.num_query_groups);
    try std.testing.expectEqual(@as(usize, 2), stats.max_examples_per_query);
    try std.testing.expectApproxEqAbs(@as(f64, 1.5), stats.avg_examples_per_query, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f64, 0.5), stats.avg_score, 1e-6);
    try std.testing.expectEqual(@as(usize, 1), countPairwiseTrainingPairs(loaded.examples));
}
