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
const resolve_mod = @import("jsonl_resolve.zig");
const compat = @import("../io/compat.zig");

pub const schema_v1 = "gemma_chat/v1";

pub const Role = enum { system, user, assistant, tool };

pub const ToolCall = struct {
    id: []const u8,
    name: []const u8,
    arguments_json: []const u8,
};

pub const ToolSpec = struct {
    name: []const u8,
    description: ?[]const u8 = null,
    input_schema_json: ?[]const u8 = null,
};

pub const Metadata = struct {
    policy_version: ?[]const u8 = null,
    source: ?[]const u8 = null,
};

pub const Message = struct {
    role: Role,
    content: []const u8,
    tool_call_id: ?[]const u8 = null,
    name: ?[]const u8 = null,
    tool_calls: []ToolCall = &.{},
};

pub const Example = struct {
    id: ?[]const u8 = null,
    messages: []Message,
    tools: []ToolSpec = &.{},
    image_paths: []const []const u8 = &.{},
    audio_paths: []const []const u8 = &.{},
    metadata: Metadata = .{},
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

pub fn loadExamples(allocator: std.mem.Allocator, path: []const u8, split: ?[]const u8) !LoadedExamples {
    var resolved = try resolve_mod.resolveJsonlFiles(allocator, path, split);
    defer resolved.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const arena_alloc = arena.allocator();
    const dataset_root = try deriveDatasetRoot(arena_alloc, path);

    var examples: std.ArrayListUnmanaged(Example) = .empty;
    defer examples.deinit(arena_alloc);

    for (resolved.paths) |resolved_path| {
        try loadExamplesFromFile(arena_alloc, resolved_path, split, &examples);
    }
    if (examples.items.len == 0) return error.NoExamples;

    return .{
        .arena = arena,
        .dataset_root = dataset_root,
        .examples = try examples.toOwnedSlice(arena_alloc),
    };
}

fn loadExamplesFromFile(
    allocator: std.mem.Allocator,
    path: []const u8,
    split_filter: ?[]const u8,
    out: *std.ArrayListUnmanaged(Example),
) !void {
    const source_root = try deriveDatasetRoot(allocator, path);
    const file_data = try compat.cwd().readFileAlloc(compat.io(), path, allocator, .limited(64 * 1024 * 1024));
    var lines = std.mem.tokenizeScalar(u8, file_data, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0) continue;
        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (!rowMatchesSplit(parsed.value, split_filter)) continue;
        var example = try coerceExample(allocator, parsed.value);
        example.image_paths = try resolveMediaPaths(allocator, source_root, example.image_paths);
        example.audio_paths = try resolveMediaPaths(allocator, source_root, example.audio_paths);
        try out.append(allocator, example);
    }
}

fn rowMatchesSplit(value: std.json.Value, split_filter: ?[]const u8) bool {
    const split = split_filter orelse return true;
    if (value != .object) return true;
    const row_split = value.object.get("split") orelse return true;
    return row_split == .string and std.mem.eql(u8, row_split.string, split);
}

fn coerceExample(allocator: std.mem.Allocator, value: std.json.Value) !Example {
    const obj = if (value == .object) value.object else return error.UnsupportedGemmaChatExampleShape;
    if (obj.get("messages") != null or obj.get("schema") != null) {
        return try coerceChatExample(allocator, obj);
    }
    return try coerceLegacyExample(allocator, obj);
}

fn coerceChatExample(allocator: std.mem.Allocator, obj: std.json.ObjectMap) !Example {
    if (obj.get("schema")) |schema| {
        if (schema != .string or !std.mem.eql(u8, schema.string, schema_v1)) {
            return error.UnsupportedGemmaChatSchema;
        }
    }

    const messages_value = obj.get("messages") orelse return error.MissingMessages;
    if (messages_value != .array) return error.MissingMessages;
    const messages = try allocator.alloc(Message, messages_value.array.items.len);
    errdefer allocator.free(messages);
    var image_paths_buf: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer image_paths_buf.deinit(allocator);
    var audio_paths_buf: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer audio_paths_buf.deinit(allocator);

    var msg_count: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < msg_count) : (i += 1) allocator.free(messages[i].tool_calls);
    }
    for (messages_value.array.items, 0..) |msg_value, idx| {
        messages[idx] = try coerceMessage(allocator, msg_value, &image_paths_buf, &audio_paths_buf);
        msg_count += 1;
    }

    var tools: []ToolSpec = &.{};
    if (obj.get("tools")) |tools_value| {
        if (tools_value != .array) return error.InvalidTools;
        tools = try allocator.alloc(ToolSpec, tools_value.array.items.len);
        for (tools_value.array.items, 0..) |tool_value, idx| {
            tools[idx] = try coerceToolSpec(allocator, tool_value);
        }
    }

    const metadata = if (obj.get("metadata")) |meta|
        try coerceMetadata(meta)
    else
        Metadata{};

    return .{
        .id = optionalString(obj.get("id")),
        .messages = messages,
        .tools = tools,
        .image_paths = try image_paths_buf.toOwnedSlice(allocator),
        .audio_paths = try audio_paths_buf.toOwnedSlice(allocator),
        .metadata = metadata,
    };
}

fn coerceLegacyExample(allocator: std.mem.Allocator, obj: std.json.ObjectMap) !Example {
    if (firstNonEmptyString(obj, &.{ "prompt", "instruction" })) |prompt| {
        const input = firstNonEmptyString(obj, &.{"input"});
        const response = firstNonEmptyString(obj, &.{ "response", "completion", "output" }) orelse
            return error.MissingResponse;
        const user_content = if (input) |extra|
            try std.fmt.allocPrint(allocator, "{s}\n\n{s}", .{ prompt, extra })
        else
            prompt;
        const messages = try allocator.alloc(Message, 2);
        messages[0] = .{ .role = .user, .content = user_content };
        messages[1] = .{ .role = .assistant, .content = response };
        return .{ .messages = messages };
    }

    if (firstNonEmptyString(obj, &.{"text"})) |text| {
        const messages = try allocator.alloc(Message, 1);
        messages[0] = .{ .role = .assistant, .content = text };
        return .{ .messages = messages };
    }

    return error.UnsupportedGemmaChatExampleShape;
}

fn coerceMessage(
    allocator: std.mem.Allocator,
    value: std.json.Value,
    image_paths: *std.ArrayListUnmanaged([]const u8),
    audio_paths: *std.ArrayListUnmanaged([]const u8),
) !Message {
    const obj = if (value == .object) value.object else return error.InvalidMessage;
    const role_value = obj.get("role") orelse return error.InvalidMessage;
    if (role_value != .string) return error.InvalidMessage;
    const role = parseRole(role_value.string) orelse return error.InvalidMessageRole;

    const content_value = obj.get("content");
    const content = if (content_value) |v|
        try coerceContentString(allocator, v, image_paths, audio_paths)
    else
        "";

    var tool_calls: []ToolCall = &.{};
    if (obj.get("tool_calls")) |tc_value| {
        if (tc_value != .array) return error.InvalidToolCalls;
        tool_calls = try allocator.alloc(ToolCall, tc_value.array.items.len);
        for (tc_value.array.items, 0..) |call_value, idx| {
            tool_calls[idx] = try coerceToolCall(call_value);
        }
    }

    return .{
        .role = role,
        .content = content,
        .tool_call_id = optionalString(obj.get("tool_call_id")),
        .name = optionalString(obj.get("name")),
        .tool_calls = tool_calls,
    };
}

fn coerceToolCall(value: std.json.Value) !ToolCall {
    const obj = if (value == .object) value.object else return error.InvalidToolCall;
    const id = firstNonEmptyString(obj, &.{"id"}) orelse return error.InvalidToolCall;
    if (firstNonEmptyString(obj, &.{"type"})) |tool_type| {
        if (!std.mem.eql(u8, tool_type, "function")) return error.InvalidToolCall;
    } else return error.InvalidToolCall;
    const function = if (obj.get("function")) |function_value| blk: {
        if (function_value != .object) return error.InvalidToolCall;
        break :blk function_value.object;
    } else return error.InvalidToolCall;
    const name = firstNonEmptyString(function, &.{"name"}) orelse return error.InvalidToolCall;
    const arguments_json = if (function.get("arguments")) |arg| blk: {
        if (arg != .string) return error.InvalidToolCall;
        break :blk arg.string;
    } else return error.InvalidToolCall;
    return .{ .id = id, .name = name, .arguments_json = arguments_json };
}

fn coerceToolSpec(allocator: std.mem.Allocator, value: std.json.Value) !ToolSpec {
    const obj = if (value == .object) value.object else return error.InvalidTools;
    const name = firstNonEmptyString(obj, &.{"name"}) orelse return error.InvalidTools;
    const input_schema_json = blk: {
        if (obj.get("input_schema_json")) |schema| {
            if (schema == .string) break :blk schema.string;
        }
        if (obj.get("input_schema")) |schema| {
            var buf: std.Io.Writer.Allocating = .init(allocator);
            defer buf.deinit();
            try std.json.Stringify.value(schema, .{}, &buf.writer);
            break :blk try allocator.dupe(u8, buf.written());
        }
        break :blk null;
    };
    return .{
        .name = name,
        .description = optionalString(obj.get("description")),
        .input_schema_json = input_schema_json,
    };
}

fn coerceMetadata(value: std.json.Value) !Metadata {
    const obj = if (value == .object) value.object else return error.InvalidMetadata;
    return .{
        .policy_version = optionalString(obj.get("policy_version")),
        .source = optionalString(obj.get("source")),
    };
}

fn coerceContentString(
    allocator: std.mem.Allocator,
    value: std.json.Value,
    image_paths: *std.ArrayListUnmanaged([]const u8),
    audio_paths: *std.ArrayListUnmanaged([]const u8),
) ![]const u8 {
    return switch (value) {
        .string => std.mem.trim(u8, value.string, " \t\r\n"),
        .array => try concatContentParts(allocator, value.array.items, image_paths, audio_paths),
        else => return error.InvalidMessageContent,
    };
}

fn concatContentParts(
    allocator: std.mem.Allocator,
    parts: []const std.json.Value,
    image_paths: *std.ArrayListUnmanaged([]const u8),
    audio_paths: *std.ArrayListUnmanaged([]const u8),
) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    for (parts) |part| {
        if (part != .object) return error.InvalidMessageContentPart;
        const part_type = part.object.get("type") orelse return error.InvalidMessageContentPart;
        if (part_type != .string) return error.InvalidMessageContentPart;
        if (std.mem.eql(u8, part_type.string, "text")) {
            const text = part.object.get("text") orelse return error.InvalidMessageContentPart;
            if (text != .string) return error.InvalidMessageContentPart;
            if (out.items.len > 0) try out.append(allocator, '\n');
            try out.appendSlice(allocator, std.mem.trim(u8, text.string, " \t\r\n"));
            continue;
        }
        if (std.mem.eql(u8, part_type.string, "image")) {
            const path = findMediaPath(part.object, &.{ "image_path", "image", "path", "file_name" }) orelse
                return error.InvalidMessageContentPart;
            if (out.items.len > 0) try out.append(allocator, '\n');
            try out.appendSlice(allocator, "<|image|>");
            try image_paths.append(allocator, path);
            continue;
        }
        if (std.mem.eql(u8, part_type.string, "audio")) {
            const path = findMediaPath(part.object, &.{ "audio_path", "audio", "path", "file_name" }) orelse
                return error.InvalidMessageContentPart;
            if (out.items.len > 0) try out.append(allocator, '\n');
            try out.appendSlice(allocator, "<|audio|>");
            try audio_paths.append(allocator, path);
            continue;
        }
        return error.UnsupportedMessageContentPartType;
    }
    return try out.toOwnedSlice(allocator);
}

fn findMediaPath(obj: std.json.ObjectMap, keys: []const []const u8) ?[]const u8 {
    for (keys) |key| {
        if (obj.get(key)) |value| {
            if (value == .string) {
                const trimmed = std.mem.trim(u8, value.string, " \t\r\n");
                if (trimmed.len > 0) return trimmed;
            }
        }
    }
    return null;
}

fn parseRole(text: []const u8) ?Role {
    if (std.mem.eql(u8, text, "system")) return .system;
    if (std.mem.eql(u8, text, "user")) return .user;
    if (std.mem.eql(u8, text, "assistant")) return .assistant;
    if (std.mem.eql(u8, text, "tool")) return .tool;
    return null;
}

fn firstNonEmptyString(obj: std.json.ObjectMap, names: []const []const u8) ?[]const u8 {
    for (names) |name| {
        if (obj.get(name)) |value| {
            if (value == .string and std.mem.trim(u8, value.string, " \t\r\n").len > 0) return std.mem.trim(u8, value.string, " \t\r\n");
        }
    }
    return null;
}

fn optionalString(value: ?std.json.Value) ?[]const u8 {
    const v = value orelse return null;
    if (v != .string) return null;
    const trimmed = std.mem.trim(u8, v.string, " \t\r\n");
    if (trimmed.len == 0) return null;
    return trimmed;
}

fn deriveDatasetRoot(allocator: std.mem.Allocator, path: []const u8) ![]const u8 {
    const stat = try compat.cwd().statFile(compat.io(), path, .{});
    if (stat.kind == .directory) return allocator.dupe(u8, path);
    const dir = std.fs.path.dirname(path) orelse ".";
    return allocator.dupe(u8, dir);
}

fn resolveMediaPaths(
    allocator: std.mem.Allocator,
    dataset_root: []const u8,
    paths: []const []const u8,
) ![]const []const u8 {
    if (paths.len == 0) return &.{};
    const out = try allocator.alloc([]const u8, paths.len);
    for (paths, 0..) |path, idx| {
        out[idx] = if (std.fs.path.isAbsolute(path))
            path
        else
            try std.fs.path.join(allocator, &.{ dataset_root, path });
    }
    return out;
}

test "load legacy prompt response into chat messages" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const jsonl =
        \\{"prompt":"Say hi","response":"Hello"}
        \\
    ;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "train.jsonl", .data = jsonl });
    const path = try tmpPathAlloc(allocator, &tmp, "train.jsonl");
    defer allocator.free(path);

    var loaded = try loadExamples(allocator, path, null);
    defer loaded.deinit();

    try std.testing.expectEqual(@as(usize, 1), loaded.examples.len);
    try std.testing.expectEqual(Role.user, loaded.examples[0].messages[0].role);
    try std.testing.expectEqual(Role.assistant, loaded.examples[0].messages[1].role);
}

test "load tool chat example" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const jsonl =
        \\{"schema":"gemma_chat/v1","messages":[{"role":"user","content":"list"},{"role":"assistant","content":"Checking","tool_calls":[{"id":"call_1","type":"function","function":{"name":"shell","arguments":"{\"cmd\":\"ls\"}"}}]},{"role":"tool","tool_call_id":"call_1","name":"shell","content":"file.txt"},{"role":"assistant","content":"Found file.txt"}],"metadata":{"policy_version":"v1"}}
        \\
    ;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "train.jsonl", .data = jsonl });
    const path = try tmpPathAlloc(allocator, &tmp, "train.jsonl");
    defer allocator.free(path);

    var loaded = try loadExamples(allocator, path, null);
    defer loaded.deinit();

    try std.testing.expectEqual(@as(usize, 4), loaded.examples[0].messages.len);
    try std.testing.expectEqualStrings("v1", loaded.examples[0].metadata.policy_version.?);
    try std.testing.expectEqual(@as(usize, 1), loaded.examples[0].messages[1].tool_calls.len);
    try std.testing.expectEqualStrings("shell", loaded.examples[0].messages[2].name.?);
}

test "load multimodal chat example records media placeholders and paths" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const jsonl =
        \\{"schema":"gemma_chat/v1","messages":[{"role":"user","content":[{"type":"text","text":"Describe"},{"type":"image","image_path":"images/cat.png"},{"type":"audio","audio_path":"audio/bark.wav"}]},{"role":"assistant","content":"A cat"}]}
        \\
    ;
    try tmp.dir.createDirPath(std.testing.io, "images");
    try tmp.dir.createDirPath(std.testing.io, "audio");
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "train.jsonl", .data = jsonl });
    const path = try tmpPathAlloc(allocator, &tmp, "train.jsonl");
    defer allocator.free(path);

    var loaded = try loadExamples(allocator, path, null);
    defer loaded.deinit();

    try std.testing.expectEqualStrings("Describe\n<|image|>\n<|audio|>", loaded.examples[0].messages[0].content);
    try std.testing.expectEqual(@as(usize, 1), loaded.examples[0].image_paths.len);
    try std.testing.expectEqual(@as(usize, 1), loaded.examples[0].audio_paths.len);
    try std.testing.expect(std.fs.path.isAbsolute(loaded.examples[0].image_paths[0]));
    try std.testing.expect(std.fs.path.isAbsolute(loaded.examples[0].audio_paths[0]));
}

test "load chat example rejects malformed content parts" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const jsonl =
        \\{"schema":"gemma_chat/v1","messages":[{"role":"user","content":[{"type":"image"}]},{"role":"assistant","content":"A cat"}]}
        \\
    ;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "train.jsonl", .data = jsonl });
    const path = try tmpPathAlloc(allocator, &tmp, "train.jsonl");
    defer allocator.free(path);

    try std.testing.expectError(error.InvalidMessageContentPart, loadExamples(allocator, path, null));
}

fn tmpPathAlloc(allocator: std.mem.Allocator, tmp: *std.testing.TmpDir, sub_path: []const u8) ![]u8 {
    const relative = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], sub_path });
    defer allocator.free(relative);
    var buffer: [std.fs.max_path_bytes]u8 = undefined;
    const len = try compat.cwd().realPathFile(compat.io(), relative, &buffer);
    return allocator.dupe(u8, buffer[0..len]);
}
