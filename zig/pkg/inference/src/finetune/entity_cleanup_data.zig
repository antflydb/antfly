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
const resolve_mod = @import("jsonl_resolve.zig");

pub const schema_v1 = "entity_cleanup/v1";

pub const Mention = struct {
    start: usize,
    end: usize,
    label: []const u8,
    keep: bool,
    group_id: ?[]const u8 = null,
    preferred_surface: bool = false,
};

pub const Example = struct {
    id: ?[]const u8 = null,
    text: []const u8,
    mentions: []Mention,
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

pub const Stats = struct {
    num_examples: usize = 0,
    num_mentions: usize = 0,
    kept_mentions: usize = 0,
    dropped_mentions: usize = 0,
    grouped_mentions: usize = 0,
    preferred_mentions: usize = 0,
};

pub fn loadExamples(allocator: std.mem.Allocator, path: []const u8, split: ?[]const u8) !LoadedExamples {
    var resolved = try resolve_mod.resolveJsonlFiles(allocator, path, split);
    defer resolved.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const arena_alloc = arena.allocator();
    const dataset_root = try deriveDatasetRoot(arena_alloc, path);

    var examples = std.ArrayListUnmanaged(Example).empty;
    defer examples.deinit(arena_alloc);

    for (resolved.paths) |resolved_path| {
        try loadExamplesFromFile(arena_alloc, resolved_path, split, &examples);
    }
    if (examples.items.len == 0) return error.NoExamples;
    try validateDataset(arena_alloc, examples.items);

    return .{
        .arena = arena,
        .dataset_root = dataset_root,
        .examples = try examples.toOwnedSlice(arena_alloc),
    };
}

pub fn computeStats(examples: []const Example) Stats {
    var stats = Stats{
        .num_examples = examples.len,
    };

    for (examples) |example| {
        for (example.mentions) |mention| {
            stats.num_mentions += 1;
            if (mention.keep) {
                stats.kept_mentions += 1;
            } else {
                stats.dropped_mentions += 1;
            }
            if (mention.group_id != null) stats.grouped_mentions += 1;
            if (mention.preferred_surface) stats.preferred_mentions += 1;
        }
    }

    return stats;
}

fn loadExamplesFromFile(
    allocator: std.mem.Allocator,
    path: []const u8,
    split_filter: ?[]const u8,
    out: *std.ArrayListUnmanaged(Example),
) !void {
    const file_data = try compat.cwd().readFileAlloc(compat.io(), path, allocator, .limited(64 * 1024 * 1024));
    var lines = std.mem.tokenizeScalar(u8, file_data, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0) continue;
        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (!(try rowMatchesSplit(parsed.value, split_filter))) continue;
        try out.append(allocator, try coerceExample(allocator, parsed.value));
    }
}

fn rowMatchesSplit(value: std.json.Value, split_filter: ?[]const u8) !bool {
    const split = split_filter orelse return true;
    if (value != .object) return error.InvalidCleanupSplit;
    const row_split = value.object.get("split") orelse return error.InvalidCleanupSplit;
    if (row_split != .string) return error.InvalidCleanupSplit;
    return std.mem.eql(u8, row_split.string, split);
}

fn coerceExample(allocator: std.mem.Allocator, value: std.json.Value) !Example {
    const obj = if (value == .object) value.object else return error.InvalidCleanupExample;

    const schema = obj.get("schema") orelse return error.UnsupportedCleanupSchema;
    if (schema != .string or !std.mem.eql(u8, schema.string, schema_v1)) {
        return error.UnsupportedCleanupSchema;
    }

    const text_value = obj.get("text") orelse return error.InvalidCleanupExample;
    if (text_value != .string) return error.InvalidCleanupExample;

    const mentions_value = obj.get("mentions") orelse return error.InvalidCleanupExample;
    if (mentions_value != .array) return error.InvalidCleanupExample;

    const mentions = try allocator.alloc(Mention, mentions_value.array.items.len);
    errdefer allocator.free(mentions);
    for (mentions_value.array.items, 0..) |mention_value, idx| {
        mentions[idx] = try coerceMention(allocator, mention_value);
    }
    try validateExample(allocator, text_value.string, mentions);

    return .{
        .id = optionalString(obj.get("id")),
        .text = text_value.string,
        .mentions = mentions,
    };
}

fn coerceMention(allocator: std.mem.Allocator, value: std.json.Value) !Mention {
    const obj = if (value == .object) value.object else return error.InvalidCleanupMention;

    const start = parseUnsignedField(obj, "start") orelse return error.InvalidCleanupMention;
    const end = parseUnsignedField(obj, "end") orelse return error.InvalidCleanupMention;
    const label = optionalString(obj.get("label")) orelse return error.InvalidCleanupMention;
    const keep = parseBoolField(obj, "keep") orelse return error.InvalidCleanupMention;
    const group_id = optionalString(obj.get("group_id"));
    const preferred_surface = blk: {
        if (keep) break :blk parseBoolField(obj, "preferred_surface") orelse return error.InvalidCleanupMentionPreferredSurface;
        break :blk parseBoolField(obj, "preferred_surface") orelse false;
    };

    _ = allocator;
    return .{
        .start = start,
        .end = end,
        .label = label,
        .keep = keep,
        .group_id = group_id,
        .preferred_surface = preferred_surface,
    };
}

fn parseUnsignedField(obj: std.json.ObjectMap, key: []const u8) ?usize {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .integer => if (value.integer >= 0) @intCast(value.integer) else null,
        else => null,
    };
}

fn parseBoolField(obj: std.json.ObjectMap, key: []const u8) ?bool {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .bool => value.bool,
        else => null,
    };
}

fn optionalString(value: ?std.json.Value) ?[]const u8 {
    const resolved = value orelse return null;
    return if (resolved == .string) resolved.string else null;
}

const GroupStats = struct {
    mentions: usize = 0,
    preferred_mentions: usize = 0,
};

fn validateExample(allocator: std.mem.Allocator, text: []const u8, mentions: []const Mention) !void {
    var grouped = std.StringHashMapUnmanaged(GroupStats).empty;
    defer grouped.deinit(allocator);

    for (mentions) |mention| {
        if (mention.label.len == 0) return error.InvalidCleanupMentionLabel;
        if (mention.start >= mention.end or mention.end > text.len) return error.InvalidCleanupMentionSpan;

        if (mention.group_id) |group_id| {
            if (group_id.len == 0 or !mention.keep) return error.InvalidCleanupMentionGroup;
            const entry = try grouped.getOrPut(allocator, group_id);
            if (!entry.found_existing) entry.value_ptr.* = .{};
            entry.value_ptr.mentions += 1;
            if (mention.preferred_surface) entry.value_ptr.preferred_mentions += 1;
        } else if (mention.keep) {
            return error.InvalidCleanupMentionGroup;
        } else if (mention.preferred_surface) {
            return error.InvalidCleanupMentionPreferredSurface;
        }

        if (mention.preferred_surface and !mention.keep) {
            return error.InvalidCleanupMentionPreferredSurface;
        }
    }

    var it = grouped.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.preferred_mentions != 1) return error.InvalidCleanupMentionGroup;
    }
}

fn validateDataset(allocator: std.mem.Allocator, examples: []const Example) !void {
    var groups = std.StringHashMapUnmanaged([]const u8).empty;
    defer groups.deinit(allocator);

    for (examples) |example| {
        for (example.mentions) |mention| {
            const group_id = mention.group_id orelse continue;
            const entry = try groups.getOrPut(allocator, group_id);
            if (!entry.found_existing) {
                entry.value_ptr.* = mention.label;
                continue;
            }
            if (!std.mem.eql(u8, entry.value_ptr.*, mention.label)) {
                return error.InvalidCleanupMentionGroup;
            }
        }
    }
}

fn deriveDatasetRoot(allocator: std.mem.Allocator, path: []const u8) ![]const u8 {
    const dirname = std.fs.path.dirname(path) orelse ".";
    return allocator.dupe(u8, dirname);
}

test "load entity cleanup row" {
    const allocator = std.testing.allocator;
    const line =
        \\{
        \\  "schema":"entity_cleanup/v1",
        \\  "text":"T1mKaye met Tim Kaye.",
        \\  "mentions":[
        \\    {"start":0,"end":8,"label":"person","keep":false},
        \\    {"start":13,"end":21,"label":"person","keep":true,"group_id":"person:tim_kaye","preferred_surface":true}
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{});
    defer parsed.deinit();

    const example = try coerceExample(allocator, parsed.value);
    try std.testing.expectEqualStrings("T1mKaye met Tim Kaye.", example.text);
    try std.testing.expectEqual(@as(usize, 2), example.mentions.len);
    try std.testing.expect(!example.mentions[0].keep);
    try std.testing.expect(example.mentions[1].preferred_surface);
    try std.testing.expectEqualStrings("person:tim_kaye", example.mentions[1].group_id.?);
    allocator.free(example.mentions);
}

test "entity cleanup row requires schema" {
    const allocator = std.testing.allocator;
    const line =
        \\{
        \\  "text":"Tim Kaye",
        \\  "mentions":[{"start":0,"end":3,"label":"person","keep":true}]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{});
    defer parsed.deinit();

    try std.testing.expectError(error.UnsupportedCleanupSchema, coerceExample(allocator, parsed.value));
}

test "entity cleanup row rejects invalid mention span" {
    const allocator = std.testing.allocator;
    const line =
        \\{
        \\  "schema":"entity_cleanup/v1",
        \\  "text":"Tim Kaye",
        \\  "mentions":[{"start":0,"end":20,"label":"person","keep":true,"group_id":"g1","preferred_surface":true}]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{});
    defer parsed.deinit();

    try std.testing.expectError(error.InvalidCleanupMentionSpan, coerceExample(allocator, parsed.value));
}

test "entity cleanup row rejects invalid group semantics" {
    const allocator = std.testing.allocator;
    const line =
        \\{
        \\  "schema":"entity_cleanup/v1",
        \\  "text":"Tim Kaye met TIM KAYE",
        \\  "mentions":[
        \\    {"start":0,"end":8,"label":"person","keep":true,"group_id":"g1","preferred_surface":true},
        \\    {"start":13,"end":21,"label":"person","keep":true,"group_id":"g1","preferred_surface":true}
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{});
    defer parsed.deinit();

    try std.testing.expectError(error.InvalidCleanupMentionGroup, coerceExample(allocator, parsed.value));
}

test "entity cleanup row requires grouped kept mentions" {
    const allocator = std.testing.allocator;
    const line =
        \\{
        \\  "schema":"entity_cleanup/v1",
        \\  "text":"Tim Kaye",
        \\  "mentions":[{"start":0,"end":8,"label":"person","keep":true,"preferred_surface":true}]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{});
    defer parsed.deinit();

    try std.testing.expectError(error.InvalidCleanupMentionGroup, coerceExample(allocator, parsed.value));
}

test "entity cleanup dataset rejects group label mismatches" {
    var mentions_a = [_]Mention{
        .{ .start = 0, .end = 8, .label = "person", .keep = true, .group_id = "g1", .preferred_surface = true },
    };
    var mentions_b = [_]Mention{
        .{ .start = 0, .end = 5, .label = "organization", .keep = true, .group_id = "g1", .preferred_surface = true },
    };
    const examples = [_]Example{
        .{ .text = "Tim Kaye", .mentions = mentions_a[0..] },
        .{ .text = "Apple", .mentions = mentions_b[0..] },
    };

    try std.testing.expectError(error.InvalidCleanupMentionGroup, validateDataset(std.testing.allocator, &examples));
}

test "entity cleanup split filter requires explicit split field" {
    const allocator = std.testing.allocator;
    const line =
        \\{
        \\  "schema":"entity_cleanup/v1",
        \\  "text":"Tim Kaye",
        \\  "mentions":[{"start":0,"end":8,"label":"person","keep":true,"group_id":"g1","preferred_surface":true}]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{});
    defer parsed.deinit();

    try std.testing.expectError(error.InvalidCleanupSplit, rowMatchesSplit(parsed.value, "train"));
}

test "compute cleanup stats" {
    var mentions = [_]Mention{
        .{ .start = 0, .end = 4, .label = "person", .keep = true, .group_id = "g1", .preferred_surface = true },
        .{ .start = 5, .end = 9, .label = "person", .keep = false },
    };
    const examples = [_]Example{
        .{ .text = "test text", .mentions = mentions[0..] },
    };

    const stats = computeStats(&examples);
    try std.testing.expectEqual(@as(usize, 1), stats.num_examples);
    try std.testing.expectEqual(@as(usize, 2), stats.num_mentions);
    try std.testing.expectEqual(@as(usize, 1), stats.kept_mentions);
    try std.testing.expectEqual(@as(usize, 1), stats.dropped_mentions);
    try std.testing.expectEqual(@as(usize, 1), stats.grouped_mentions);
    try std.testing.expectEqual(@as(usize, 1), stats.preferred_mentions);
}
