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

pub const Mode = enum {
    instruction,
    completion,
};

pub const Example = struct {
    mode: Mode,
    prompt: []const u8 = "",
    response: []const u8,

    pub fn completionText(self: Example) []const u8 {
        return if (self.mode == .completion) self.response else "";
    }
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
    mode: Mode,
    examples_written: usize,
    id_column: []const u8,
    prompt_column: ?[]const u8,
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

    var detected_mode: ?Mode = null;
    for (resolved.paths) |resolved_path| {
        try loadExamplesFromFile(arena_alloc, resolved_path, &examples, &detected_mode);
    }
    if (detected_mode == null) return error.NoExamples;

    return .{
        .arena = arena,
        .dataset_root = dataset_root,
        .examples = try examples.toOwnedSlice(arena_alloc),
    };
}

pub fn writeCsv(
    allocator: std.mem.Allocator,
    path: []const u8,
    examples: []const Example,
    max_examples: usize,
) !CsvSummary {
    if (examples.len == 0) return error.NoExamples;

    const mode = examples[0].mode;
    var max_prompt_chars: usize = 0;
    var max_response_chars: usize = 0;
    const limit = if (max_examples == 0) examples.len else @min(examples.len, max_examples);

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    switch (mode) {
        .instruction => try out.writer.writeAll("id,prompt,response\n"),
        .completion => try out.writer.writeAll("id,text\n"),
    }

    for (examples[0..limit], 0..) |example, idx| {
        if (example.mode != mode) return error.MixedGemmaDatasetModes;
        max_prompt_chars = @max(max_prompt_chars, example.prompt.len);
        max_response_chars = @max(max_response_chars, example.response.len);
        var id_buf: [32]u8 = undefined;
        const row_id = try std.fmt.bufPrint(&id_buf, "row-{d}", .{idx});
        try writeCsvCell(&out.writer, row_id);
        try out.writer.writeByte(',');
        switch (mode) {
            .instruction => {
                try writeCsvCell(&out.writer, example.prompt);
                try out.writer.writeByte(',');
                try writeCsvCell(&out.writer, example.response);
                try out.writer.writeByte('\n');
            },
            .completion => {
                try writeCsvCell(&out.writer, example.response);
                try out.writer.writeByte('\n');
            },
        }
    }

    try writeFilePath(path, out.written());

    return .{
        .mode = mode,
        .examples_written = limit,
        .id_column = "id",
        .prompt_column = if (mode == .instruction) "prompt" else null,
        .text_column = if (mode == .instruction) "response" else "text",
        .max_prompt_chars = max_prompt_chars,
        .max_response_chars = max_response_chars,
        .out_csv_path = path,
    };
}

fn loadExamplesFromFile(
    allocator: std.mem.Allocator,
    path: []const u8,
    out: *std.ArrayListUnmanaged(Example),
    detected_mode: *?Mode,
) !void {
    const RawExample = struct {
        prompt: ?[]const u8 = null,
        completion: ?[]const u8 = null,
        response: ?[]const u8 = null,
        text: ?[]const u8 = null,
        instruction: ?[]const u8 = null,
        input: ?[]const u8 = null,
        output: ?[]const u8 = null,
    };

    const file_data = try compat.cwd().readFileAlloc(compat.io(), path, allocator, .limited(64 * 1024 * 1024));
    var lines = std.mem.tokenizeScalar(u8, file_data, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0) continue;

        const parsed = try std.json.parseFromSliceLeaky(RawExample, allocator, line, .{
            .ignore_unknown_fields = true,
        });
        const example = try coerceExample(allocator, parsed);
        if (detected_mode.*) |mode| {
            if (mode != example.mode) return error.MixedGemmaDatasetModes;
        } else {
            detected_mode.* = example.mode;
        }
        try out.append(allocator, example);
    }
}

fn coerceExample(allocator: std.mem.Allocator, raw: anytype) !Example {
    if (trimOrNull(raw.prompt)) |prompt| {
        const response = trimOrNull(raw.response) orelse trimOrNull(raw.completion) orelse return error.MissingResponse;
        return .{
            .mode = .instruction,
            .prompt = prompt,
            .response = response,
        };
    }
    if (trimOrNull(raw.instruction)) |instruction| {
        const output = trimOrNull(raw.output) orelse trimOrNull(raw.response) orelse trimOrNull(raw.completion) orelse return error.MissingResponse;
        const prompt = if (trimOrNull(raw.input)) |input|
            try std.fmt.allocPrint(allocator, "{s}\n\n{s}", .{ instruction, input })
        else
            instruction;
        return .{
            .mode = .instruction,
            .prompt = prompt,
            .response = output,
        };
    }
    if (trimOrNull(raw.text)) |text| {
        return .{
            .mode = .completion,
            .response = text,
        };
    }
    return error.UnsupportedGemmaExampleShape;
}

fn trimOrNull(value: ?[]const u8) ?[]const u8 {
    const slice = value orelse return null;
    const trimmed = std.mem.trim(u8, slice, " \t\r\n");
    if (trimmed.len == 0) return null;
    return trimmed;
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

test "load instruction examples from prompt response jsonl" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const jsonl =
        \\{"prompt":"Summarize the note","response":"A short summary."}
        \\{"prompt":"Answer the question","completion":"The answer."}
        \\
    ;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "train.jsonl", .data = jsonl });
    const path = try tmpPathAlloc(allocator, &tmp, "train.jsonl");
    defer allocator.free(path);

    var loaded = try loadExamples(allocator, path, null);
    defer loaded.deinit();

    try std.testing.expectEqual(@as(usize, 2), loaded.examples.len);
    try std.testing.expectEqual(Mode.instruction, loaded.examples[0].mode);
    try std.testing.expectEqualStrings("Summarize the note", loaded.examples[0].prompt);
    try std.testing.expectEqualStrings("The answer.", loaded.examples[1].response);
}

test "load completion examples from text jsonl" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const jsonl =
        \\{"text":"First sample"}
        \\{"text":"Second sample"}
        \\
    ;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "train.jsonl", .data = jsonl });
    const path = try tmpPathAlloc(allocator, &tmp, "train.jsonl");
    defer allocator.free(path);

    var loaded = try loadExamples(allocator, path, null);
    defer loaded.deinit();

    try std.testing.expectEqual(@as(usize, 2), loaded.examples.len);
    try std.testing.expectEqual(Mode.completion, loaded.examples[0].mode);
    try std.testing.expectEqualStrings("First sample", loaded.examples[0].completionText());
}

test "write instruction csv" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_path = try tmpPathAlloc(allocator, &tmp, "train.csv");
    defer allocator.free(csv_path);
    const examples = [_]Example{
        .{ .mode = .instruction, .prompt = "Prompt 1", .response = "Response 1" },
        .{ .mode = .instruction, .prompt = "Prompt 2", .response = "Response 2" },
    };
    const summary = try writeCsv(allocator, csv_path, examples[0..], 0);
    try std.testing.expectEqual(Mode.instruction, summary.mode);
    const raw = try compat.cwd().readFileAlloc(compat.io(), csv_path, allocator, .limited(1024));
    defer allocator.free(raw);
    try std.testing.expect(std.mem.indexOf(u8, raw, "id,prompt,response") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"row-0\",\"Prompt 1\",\"Response 1\"") != null);
}

fn tmpPathAlloc(allocator: std.mem.Allocator, tmp: *const std.testing.TmpDir, sub_path: []const u8) ![]u8 {
    return std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], sub_path });
}
