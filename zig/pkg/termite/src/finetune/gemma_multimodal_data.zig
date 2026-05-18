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
const gemma_chat_data = @import("gemma_chat_data.zig");

pub const Example = struct {
    image_path: []const u8,
    prompt: []const u8,
    response: []const u8,
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

pub const CsvSummary = struct {
    examples_written: usize,
    id_column: []const u8,
    image_column: []const u8,
    prompt_column: []const u8,
    text_column: []const u8,
    max_prompt_chars: usize,
    max_response_chars: usize,
    out_csv_path: []const u8,
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
        try loadExamplesFromFile(arena_alloc, resolved_path, &examples);
    }
    if (examples.items.len == 0) return error.NoExamples;

    return .{
        .arena = arena,
        .dataset_root = dataset_root,
        .examples = try examples.toOwnedSlice(arena_alloc),
    };
}

pub fn writeCsv(allocator: std.mem.Allocator, path: []const u8, examples: []const Example, max_examples: usize) !CsvSummary {
    if (examples.len == 0) return error.NoExamples;

    var max_prompt_chars: usize = 0;
    var max_response_chars: usize = 0;
    const limit = if (max_examples == 0) examples.len else @min(examples.len, max_examples);

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try out.writer.writeAll("id,image,prompt,response\n");

    for (examples[0..limit], 0..) |example, idx| {
        max_prompt_chars = @max(max_prompt_chars, example.prompt.len);
        max_response_chars = @max(max_response_chars, example.response.len);
        var id_buf: [32]u8 = undefined;
        const row_id = try std.fmt.bufPrint(&id_buf, "row-{d}", .{idx});
        try writeCsvCell(&out.writer, row_id);
        try out.writer.writeByte(',');
        try writeCsvCell(&out.writer, example.image_path);
        try out.writer.writeByte(',');
        try writeCsvCell(&out.writer, example.prompt);
        try out.writer.writeByte(',');
        try writeCsvCell(&out.writer, example.response);
        try out.writer.writeByte('\n');
    }

    try writeFilePath(path, out.written());
    return .{
        .examples_written = limit,
        .id_column = "id",
        .image_column = "image",
        .prompt_column = "prompt",
        .text_column = "response",
        .max_prompt_chars = max_prompt_chars,
        .max_response_chars = max_response_chars,
        .out_csv_path = path,
    };
}

pub fn resolveImagePath(allocator: std.mem.Allocator, dataset_root: []const u8, image_path: []const u8) ![]const u8 {
    if (std.fs.path.isAbsolute(image_path)) return allocator.dupe(u8, image_path);
    return std.fs.path.join(allocator, &.{ dataset_root, image_path });
}

fn loadExamplesFromFile(allocator: std.mem.Allocator, path: []const u8, out: *std.ArrayListUnmanaged(Example)) !void {
    const source_root = try deriveDatasetRoot(allocator, path);
    const file_data = try compat.cwd().readFileAlloc(compat.io(), path, allocator, .limited(64 * 1024 * 1024));
    var lines = std.mem.tokenizeScalar(u8, file_data, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0) continue;
        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        var example = try coerceExample(allocator, parsed.value);
        example.image_path = try resolveImagePath(allocator, source_root, example.image_path);
        try out.append(allocator, example);
    }
}

fn coerceExample(allocator: std.mem.Allocator, value: std.json.Value) !Example {
    const obj = if (value == .object) value.object else return error.UnsupportedGemmaMultimodalExampleShape;

    const image_path = findFirstString(obj, &.{ "image_path", "image", "image_file", "file_name" }) orelse blk: {
        if (obj.get("images")) |images| {
            if (firstImagePath(images)) |path| break :blk path;
        }
        if (obj.get("messages")) |messages| {
            if (firstImagePathFromMessages(messages)) |path| break :blk path;
        }
        break :blk null;
    } orelse return error.MissingImagePath;

    const prompt = findFirstString(obj, &.{ "prompt", "question", "query", "user_text", "instruction" }) orelse blk: {
        if (obj.get("messages")) |messages| {
            if (extractFirstMessageContent(messages, "user")) |content| break :blk content;
        }
        break :blk null;
    } orelse return error.MissingPrompt;

    const response = findFirstString(obj, &.{ "response", "answer", "assistant_text", "output", "completion" }) orelse blk: {
        if (obj.get("messages")) |messages| {
            if (extractLastAssistantResponse(allocator, messages)) |content| break :blk content;
        }
        break :blk null;
    } orelse return error.MissingResponse;

    return .{
        .image_path = std.mem.trim(u8, image_path, " \t\r\n"),
        .prompt = std.mem.trim(u8, prompt, " \t\r\n"),
        .response = std.mem.trim(u8, response, " \t\r\n"),
    };
}

fn firstImagePath(value: std.json.Value) ?[]const u8 {
    if (value != .array) return null;
    for (value.array.items) |item| {
        switch (item) {
            .string => return item.string,
            .object => {
                if (findFirstString(item.object, &.{ "image_path", "image", "path", "file_name" })) |path| return path;
            },
            else => {},
        }
    }
    return null;
}

fn extractFirstMessageContent(value: std.json.Value, role_name: []const u8) ?[]const u8 {
    if (value != .array) return null;
    for (value.array.items) |item| {
        if (item != .object) continue;
        const role = item.object.get("role") orelse continue;
        if (role != .string or !std.mem.eql(u8, role.string, role_name)) continue;
        const content = item.object.get("content") orelse continue;
        return switch (content) {
            .string => content.string,
            .array => extractTextFromContentParts(content),
            else => null,
        };
    }
    return null;
}

fn extractLastAssistantResponse(allocator: std.mem.Allocator, value: std.json.Value) ?[]const u8 {
    if (value != .array) return null;
    var last_text: ?[]const u8 = null;
    for (value.array.items) |item| {
        if (item != .object) continue;
        const role = item.object.get("role") orelse continue;
        if (role != .string or !std.mem.eql(u8, role.string, "assistant")) continue;
        const content = item.object.get("content") orelse continue;
        const text = switch (content) {
            .string => content.string,
            .array => extractTextFromContentParts(content),
            else => null,
        } orelse continue;

        const tool_calls = item.object.get("tool_calls");
        const has_tool_calls = tool_calls != null and tool_calls.? == .array and tool_calls.?.array.items.len > 0;
        if (has_tool_calls) {
            last_text = formatAssistantToolCallText(allocator, text, tool_calls.?) catch text;
        } else {
            last_text = text;
        }
    }
    return last_text;
}

fn formatAssistantToolCallText(
    allocator: std.mem.Allocator,
    content_text: []const u8,
    tool_calls_value: std.json.Value,
) ![]const u8 {
    var rendered_calls: std.ArrayList(gemma_chat_data.ToolCall) = .empty;
    defer rendered_calls.deinit(allocator);
    if (tool_calls_value != .array) return allocator.dupe(u8, content_text);
    for (tool_calls_value.array.items) |call_value| {
        if (call_value != .object) continue;
        const id = findFirstString(call_value.object, &.{"id"}) orelse continue;
        const name = findFirstString(call_value.object, &.{ "name", "tool_name" }) orelse continue;
        const args_json = blk: {
            if (call_value.object.get("arguments_json")) |arg| {
                if (arg == .string) break :blk arg.string;
            }
            if (call_value.object.get("arguments")) |arg| {
                var buf: std.ArrayList(u8) = .empty;
                errdefer buf.deinit(allocator);
                var writer = std.Io.Writer.Allocating.fromArrayList(allocator, &buf);
                defer buf = writer.toArrayList();
                try std.json.Stringify.value(arg, .{}, &writer.writer);
                break :blk try buf.toOwnedSlice(allocator);
            }
            break :blk "{}";
        };
        try rendered_calls.append(allocator, .{ .id = id, .name = name, .arguments_json = args_json });
    }

    if (rendered_calls.items.len == 0) return allocator.dupe(u8, content_text);

    var tool_call_buf: std.ArrayList(u8) = .empty;
    defer tool_call_buf.deinit(allocator);
    var writer = std.Io.Writer.Allocating.fromArrayList(allocator, &tool_call_buf);
    defer tool_call_buf = writer.toArrayList();
    try std.json.Stringify.value(rendered_calls.items, .{}, &writer.writer);
    if (content_text.len == 0) {
        return std.fmt.allocPrint(allocator, "<tool_call>\n{s}\n</tool_call>", .{tool_call_buf.items});
    }
    return std.fmt.allocPrint(allocator, "{s}\n<tool_call>\n{s}\n</tool_call>", .{ content_text, tool_call_buf.items });
}

fn firstImagePathFromMessages(value: std.json.Value) ?[]const u8 {
    if (value != .array) return null;
    for (value.array.items) |item| {
        if (item != .object) continue;
        const content = item.object.get("content") orelse continue;
        if (content != .array) continue;
        for (content.array.items) |part| {
            if (part != .object) continue;
            const ty = part.object.get("type") orelse continue;
            if (ty != .string) continue;
            if (!std.mem.eql(u8, ty.string, "image")) continue;
            if (findFirstString(part.object, &.{ "image_path", "image", "path", "file_name" })) |path| return path;
        }
    }
    return null;
}

fn extractTextFromContentParts(value: std.json.Value) ?[]const u8 {
    if (value != .array) return null;
    for (value.array.items) |part| {
        if (part != .object) continue;
        const ty = part.object.get("type") orelse continue;
        if (ty != .string) continue;
        if (!std.mem.eql(u8, ty.string, "text")) continue;
        const text = part.object.get("text") orelse continue;
        if (text == .string) return text.string;
    }
    return null;
}

fn findFirstString(obj: std.json.ObjectMap, names: []const []const u8) ?[]const u8 {
    for (names) |name| {
        if (obj.get(name)) |value| {
            if (value == .string and std.mem.trim(u8, value.string, " \t\r\n").len > 0) return value.string;
        }
    }
    return null;
}

fn writeCsvCell(writer: *std.Io.Writer, value: []const u8) !void {
    try writer.writeByte('"');
    for (value) |ch| {
        if (ch == '"') try writer.writeByte('"');
        try writer.writeByte(ch);
    }
    try writer.writeByte('"');
}

fn deriveDatasetRoot(allocator: std.mem.Allocator, path: []const u8) ![]const u8 {
    const stat = try compat.cwd().statFile(compat.io(), path, .{});
    if (stat.kind == .directory) return allocator.dupe(u8, path);
    const dir = std.fs.path.dirname(path) orelse ".";
    return allocator.dupe(u8, dir);
}

fn writeFilePath(path: []const u8, data: []const u8) !void {
    const io_inst = compat.io();
    if (std.fs.path.dirname(path)) |dir| {
        if (dir.len > 0) {
            try compat.cwd().createDirPath(io_inst, dir);
        }
    }
    try compat.cwd().writeFile(io_inst, .{ .sub_path = path, .data = data });
}

test "load multimodal prompt response example" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const jsonl =
        \\{"image_path":"img.png","prompt":"Describe the image","response":"A cat on a sofa."}
        \\
    ;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "train.jsonl", .data = jsonl });
    const path = try tmpPathAlloc(allocator, &tmp, "train.jsonl");
    defer allocator.free(path);

    var loaded = try loadExamples(allocator, path, null);
    defer loaded.deinit();
    try std.testing.expectEqual(@as(usize, 1), loaded.examples.len);
    try std.testing.expect(std.mem.endsWith(u8, loaded.examples[0].image_path, "img.png"));
}

test "load multimodal messages example" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const jsonl =
        \\{"images":["img.png"],"messages":[{"role":"user","content":"What is shown?"},{"role":"assistant","content":"A chart."}]}
        \\
    ;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "train.jsonl", .data = jsonl });
    const path = try tmpPathAlloc(allocator, &tmp, "train.jsonl");
    defer allocator.free(path);

    var loaded = try loadExamples(allocator, path, null);
    defer loaded.deinit();
    try std.testing.expectEqualStrings("What is shown?", loaded.examples[0].prompt);
    try std.testing.expectEqualStrings("A chart.", loaded.examples[0].response);
}

test "load multimodal gemma chat v1 with image parts and tool call" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const jsonl =
        \\{"schema":"gemma_chat/v1","messages":[{"role":"user","content":[{"type":"text","text":"What is shown?"},{"type":"image","image_path":"img.png"}]},{"role":"assistant","content":"Checking","tool_calls":[{"id":"call_1","name":"ocr","arguments":{"region":"full"}}]},{"role":"tool","tool_call_id":"call_1","name":"ocr","content":"Quarterly revenue chart"},{"role":"assistant","content":"A revenue chart."}]}
        \\
    ;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "train.jsonl", .data = jsonl });
    const path = try tmpPathAlloc(allocator, &tmp, "train.jsonl");
    defer allocator.free(path);

    var loaded = try loadExamples(allocator, path, null);
    defer loaded.deinit();
    try std.testing.expect(std.mem.endsWith(u8, loaded.examples[0].image_path, "img.png"));
    try std.testing.expectEqualStrings("What is shown?", loaded.examples[0].prompt);
    try std.testing.expectEqualStrings("A revenue chart.", loaded.examples[0].response);
}

test "write multimodal csv includes id and absolute image path" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_path = try tmpPathAlloc(allocator, &tmp, "train.csv");
    defer allocator.free(csv_path);
    const image_path = try tmpPathAlloc(allocator, &tmp, "img.png");
    defer allocator.free(image_path);
    const examples = [_]Example{
        .{ .image_path = image_path, .prompt = "Describe the image", .response = "A cat on a sofa." },
    };
    const summary = try writeCsv(allocator, csv_path, examples[0..], 0);
    try std.testing.expectEqualStrings("id", summary.id_column);
    const raw = try compat.cwd().readFileAlloc(compat.io(), csv_path, allocator, .limited(1024));
    defer allocator.free(raw);
    try std.testing.expect(std.mem.indexOf(u8, raw, "id,image,prompt,response") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"row-0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, image_path) != null);
}

fn tmpPathAlloc(allocator: std.mem.Allocator, tmp: *const std.testing.TmpDir, sub_path: []const u8) ![]u8 {
    return std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], sub_path });
}
