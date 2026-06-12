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
const build_options = @import("build_options");
const backends = @import("../backends/backends.zig");
const donut_mod = @import("donut.zig");
const moondream_mod = @import("moondream.zig");
const ortgenai = if (build_options.enable_onnx) @import("../backends/ortgenai.zig") else struct {};
const model_manager_mod = @import("../server/model_manager.zig");
const generation = @import("../pipelines/generation.zig");
const onnx_decoder_only_vlm = @import("../pipelines/onnx_decoder_only_vlm.zig");
const multistage_metadata = @import("multistage_metadata.zig");
const multistage_reader_mod = @import("multistage_reader.zig");
const pix2struct_mod = @import("pix2struct.zig");
const vision_reader_mod = @import("vision_reader.zig");
const reader_types = @import("types.zig");

pub const Field = reader_types.Field;
pub const Region = reader_types.Region;
pub const Result = reader_types.Result;
pub const ReadOptions = reader_types.ReadOptions;
pub const StructuredValue = reader_types.StructuredValue;

const ParserKind = enum {
    default,
    donut,
    florence,
    moondream,
    pix2struct,
};

const VisionLoadedReader = struct {
    allocator: std.mem.Allocator,
    parser_kind: ParserKind,
    core: vision_reader_mod.LoadedVisionReader,

    pub fn loadFromDir(
        allocator: std.mem.Allocator,
        model_path: []const u8,
        session_manager: *backends.SessionManager,
        model_manager: *model_manager_mod.ModelManager,
    ) !VisionLoadedReader {
        return .{
            .allocator = allocator,
            .parser_kind = try detectParserKind(allocator, model_path),
            .core = try vision_reader_mod.LoadedVisionReader.loadFromDir(allocator, model_path, session_manager, model_manager),
        };
    }

    pub fn deinit(self: *VisionLoadedReader) void {
        self.core.deinit();
    }

    pub fn read(self: *VisionLoadedReader, image_data: []const u8, options: ReadOptions) !Result {
        const normalized_prompt = normalizePromptForFamily(self.parser_kind, options.prompt);
        var raw = try self.core.readRaw(image_data, .{
            .prompt = normalized_prompt,
            .max_tokens = options.max_tokens,
        });
        defer raw.deinit();

        return parseOutput(self.allocator, self.parser_kind, raw.text, normalized_prompt);
    }
};

const VlmLoadedReader = struct {
    allocator: std.mem.Allocator,
    parser_kind: ParserKind,
    pipeline: onnx_decoder_only_vlm.Pipeline,

    pub fn loadFromDir(
        allocator: std.mem.Allocator,
        model_path: []const u8,
    ) !VlmLoadedReader {
        const parser_kind = try detectParserKind(allocator, model_path);
        if (parser_kind != .moondream) return error.InvalidModelForReading;

        return .{
            .allocator = allocator,
            .parser_kind = parser_kind,
            .pipeline = try onnx_decoder_only_vlm.Pipeline.load(allocator, model_path),
        };
    }

    pub fn deinit(self: *VlmLoadedReader) void {
        self.pipeline.deinit();
    }

    pub fn read(self: *VlmLoadedReader, image_data: []const u8, options: ReadOptions) !Result {
        const prompt = switch (self.parser_kind) {
            .moondream => try moondream_mod.buildSingleImagePrompt(self.allocator, options.prompt),
            else => return error.InvalidModelForReading,
        };
        defer self.allocator.free(prompt);

        const images = [_][]const u8{image_data};
        var raw = try self.pipeline.generatePrompt(prompt, images[0..], .{
            .max_tokens = @intCast(options.max_tokens orelse 256),
            .cache_dtype = options.cache_dtype,
        });
        defer raw.deinit();

        return parseOutput(self.allocator, self.parser_kind, raw.text, options.prompt);
    }
};

const GenAiLoadedReader = struct {
    allocator: std.mem.Allocator,
    parser_kind: ParserKind,
    prepared_model_dir: if (build_options.enable_onnx) []u8 else void,
    model: if (build_options.enable_onnx) ortgenai.GenAiModel else void,

    pub fn loadFromDir(
        allocator: std.mem.Allocator,
        model_path: []const u8,
    ) !GenAiLoadedReader {
        if (!build_options.enable_onnx) return error.OnnxNotEnabled;

        const parser_kind = try detectParserKind(allocator, model_path);
        if (parser_kind != .moondream) return error.InvalidModelForReading;

        const prepared_model_dir = (try ortgenai.prepareGenerativeModelPackage(allocator, model_path)) orelse
            return error.InvalidModelForReading;
        errdefer allocator.free(prepared_model_dir);

        const model = try ortgenai.GenAiModel.load(allocator, prepared_model_dir);
        errdefer {
            var model_mut = model;
            model_mut.deinit();
        }

        return .{
            .allocator = allocator,
            .parser_kind = parser_kind,
            .prepared_model_dir = prepared_model_dir,
            .model = model,
        };
    }

    pub fn deinit(self: *GenAiLoadedReader) void {
        if (!build_options.enable_onnx) return;
        self.model.deinit();
        self.allocator.free(self.prepared_model_dir);
    }

    pub fn read(self: *GenAiLoadedReader, image_data: []const u8, options: ReadOptions) !Result {
        if (!build_options.enable_onnx) return error.OnnxNotEnabled;

        const prompt = switch (self.parser_kind) {
            .moondream => try moondream_mod.buildSingleImagePrompt(self.allocator, options.prompt),
            else => return error.InvalidModelForReading,
        };
        defer self.allocator.free(prompt);

        const images = [_][]const u8{image_data};
        var raw = try ortgenai.generateWithImages(
            self.allocator,
            &self.model,
            prompt,
            images[0..],
            .{ .max_tokens = @intCast(options.max_tokens orelse 256) },
        );
        defer raw.deinit();

        return parseOutput(self.allocator, self.parser_kind, raw.text, options.prompt);
    }
};

pub const LoadedReader = union(enum) {
    vision: VisionLoadedReader,
    genai: GenAiLoadedReader,
    vlm: VlmLoadedReader,
    multistage: multistage_reader_mod.LoadedMultiStageReader,

    pub fn loadFromDir(
        allocator: std.mem.Allocator,
        model_path: []const u8,
        session_manager: *backends.SessionManager,
        model_manager: *model_manager_mod.ModelManager,
    ) !LoadedReader {
        if (multistage_metadata.isMultiStageModelDir(allocator, model_path)) {
            return .{ .multistage = try multistage_reader_mod.LoadedMultiStageReader.loadFromDir(allocator, model_path, session_manager) };
        }

        const parser_kind = try detectParserKind(allocator, model_path);
        if (parser_kind == .moondream) {
            if (onnx_decoder_only_vlm.isSupportedModelDir(allocator, model_path)) {
                return .{ .vlm = try VlmLoadedReader.loadFromDir(allocator, model_path) };
            }
            if (build_options.enable_onnx) {
                if (GenAiLoadedReader.loadFromDir(allocator, model_path)) |reader| {
                    return .{ .genai = reader };
                } else |err| {
                    std.log.warn("ortgenai moondream reader load failed for {s}: {s}", .{ model_path, @errorName(err) });
                }
            }
        }
        if (parser_kind == .pix2struct and !vision_reader_mod.isSupportedModelDir(allocator, model_path)) {
            return error.NativePix2StructNotYetSupported;
        }

        return .{ .vision = try VisionLoadedReader.loadFromDir(allocator, model_path, session_manager, model_manager) };
    }

    pub fn deinit(self: *LoadedReader) void {
        switch (self.*) {
            .vision => |*reader| reader.deinit(),
            .genai => |*reader| reader.deinit(),
            .vlm => |*reader| reader.deinit(),
            .multistage => |*reader| reader.deinit(),
        }
    }

    pub fn read(self: *LoadedReader, image_data: []const u8, options: ReadOptions) !Result {
        return switch (self.*) {
            .vision => |*reader| reader.read(image_data, options),
            .genai => |*reader| reader.read(image_data, options),
            .vlm => |*reader| reader.read(image_data, options),
            .multistage => |*reader| reader.read(image_data, options),
        };
    }
};

pub fn isSupportedModelDir(allocator: std.mem.Allocator, model_path: []const u8) bool {
    if (multistage_metadata.isMultiStageModelDir(allocator, model_path)) return true;

    const parser_kind = detectParserKind(allocator, model_path) catch return false;
    if (parser_kind == .moondream) {
        return onnx_decoder_only_vlm.isSupportedModelDir(allocator, model_path);
    }

    return vision_reader_mod.isSupportedModelDir(allocator, model_path);
}

fn detectParserKind(allocator: std.mem.Allocator, model_path: []const u8) !ParserKind {
    const lower = try std.ascii.allocLowerString(allocator, model_path);
    defer allocator.free(lower);

    if (std.mem.indexOf(u8, lower, "donut") != null) return .donut;
    if (std.mem.indexOf(u8, lower, "florence") != null) return .florence;
    if (std.mem.indexOf(u8, lower, "moondream") != null) return .moondream;
    if (std.mem.indexOf(u8, lower, "pix2struct") != null) return .pix2struct;
    return .default;
}

fn parseOutput(allocator: std.mem.Allocator, parser_kind: ParserKind, text: []const u8, prompt: ?[]const u8) !Result {
    return switch (parser_kind) {
        .default => .{
            .text = try allocator.dupe(u8, std.mem.trim(u8, text, " \t\r\n")),
            .allocator = allocator,
        },
        .florence => .{
            .text = try parseFlorenceText(allocator, text),
            .allocator = allocator,
        },
        .donut => try parseDonutResult(allocator, text, prompt),
        .moondream => try parseMoondreamResult(allocator, text),
        .pix2struct => .{
            .text = try allocator.dupe(u8, std.mem.trim(u8, text, " \t\r\n")),
            .allocator = allocator,
        },
    };
}

pub fn normalizePromptForFamily(parser_kind: ParserKind, prompt: ?[]const u8) ?[]const u8 {
    return switch (parser_kind) {
        .pix2struct => if (prompt) |p| pix2struct_mod.docVqaPrompt(p) else null,
        else => prompt,
    };
}

fn parseMoondreamResult(allocator: std.mem.Allocator, text: []const u8) !Result {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    const json_text = extractMoondreamJson(trimmed) orelse {
        return .{
            .text = try allocator.dupe(u8, trimmed),
            .allocator = allocator,
        };
    };

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const parsed = std.json.parseFromSlice(std.json.Value, arena.allocator(), json_text, .{}) catch {
        return .{
            .text = try allocator.dupe(u8, trimmed),
            .allocator = allocator,
        };
    };
    defer parsed.deinit();

    if (parsed.value != .object) {
        return .{
            .text = try allocator.dupe(u8, trimmed),
            .allocator = allocator,
        };
    }

    const description = jsonObjectGetString(parsed.value.object, "description");
    const structured = try StructuredValue.cloneFromJsonValue(allocator, parsed.value);
    var fields = std.ArrayListUnmanaged(Field).empty;
    errdefer {
        var structured_copy = structured;
        structured_copy.deinit(allocator);
        for (fields.items) |*field| field.deinit(allocator);
        fields.deinit(allocator);
    }

    try appendMoondreamField(allocator, &fields, parsed.value.object, "mood");
    try appendMoondreamField(allocator, &fields, parsed.value.object, "possible_source");
    try appendMoondreamField(allocator, &fields, parsed.value.object, "temporal_flow");
    if (jsonObjectGetStringArrayJoined(allocator, parsed.value.object, "tags")) |tags| {
        errdefer allocator.free(tags);
        try fields.append(allocator, .{
            .name = try allocator.dupe(u8, "tags"),
            .value = tags,
        });
    }

    return .{
        .text = if (description) |value|
            try allocator.dupe(u8, value)
        else
            try allocator.dupe(u8, trimmed),
        .fields = try fields.toOwnedSlice(allocator),
        .structured = structured,
        .allocator = allocator,
    };
}

fn appendMoondreamField(
    allocator: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(Field),
    obj: anytype,
    key: []const u8,
) !void {
    const value = jsonObjectGetString(obj, key) orelse return;
    try fields.append(allocator, .{
        .name = try allocator.dupe(u8, key),
        .value = try allocator.dupe(u8, value),
    });
}

fn jsonObjectGetString(obj: anytype, key: []const u8) ?[]const u8 {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .string => |s| if (std.mem.trim(u8, s, " \t\r\n").len > 0) s else null,
        else => null,
    };
}

fn jsonObjectGetStringArrayJoined(
    allocator: std.mem.Allocator,
    obj: anytype,
    key: []const u8,
) ?[]u8 {
    const value = obj.get(key) orelse return null;
    if (value != .array) return null;

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    var count: usize = 0;
    for (value.array.items) |item| {
        if (item != .string) continue;
        const s = std.mem.trim(u8, item.string, " \t\r\n");
        if (s.len == 0) continue;
        if (count > 0) out.appendSlice(allocator, ",") catch return null;
        out.appendSlice(allocator, s) catch return null;
        count += 1;
    }
    if (count == 0) {
        out.deinit(allocator);
        return null;
    }
    return out.toOwnedSlice(allocator) catch null;
}

fn extractMoondreamJson(text: []const u8) ?[]const u8 {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (trimmed.len == 0) return null;

    if (extractJsonFromCodeBlock(trimmed)) |json_block| return json_block;
    if (trimmed[0] == '{') return extractBalancedJson(trimmed);

    var start: usize = 0;
    while (start < trimmed.len) {
        const open_rel = std.mem.indexOfScalarPos(u8, trimmed, start, '{') orelse break;
        if (extractBalancedJson(trimmed[open_rel..])) |json_text| return json_text;
        start = open_rel + 1;
    }

    return null;
}

fn extractJsonFromCodeBlock(text: []const u8) ?[]const u8 {
    const open = std.mem.indexOf(u8, text, "```") orelse return null;
    const rest = text[open + 3 ..];
    const close_rel = std.mem.indexOf(u8, rest, "```") orelse return null;
    var block = std.mem.trim(u8, rest[0..close_rel], " \t\r\n");
    if (std.mem.startsWith(u8, block, "json")) {
        block = std.mem.trim(u8, block[4..], " \t\r\n");
    }
    if (block.len == 0 or block[0] != '{') return null;
    return extractBalancedJson(block);
}

fn extractBalancedJson(text: []const u8) ?[]const u8 {
    if (text.len == 0 or text[0] != '{') return null;

    var depth: usize = 0;
    var in_string = false;
    var escaped = false;

    for (text, 0..) |ch, i| {
        if (escaped) {
            escaped = false;
            continue;
        }

        if (ch == '\\' and in_string) {
            escaped = true;
            continue;
        }

        if (ch == '"') {
            in_string = !in_string;
            continue;
        }

        if (in_string) continue;

        switch (ch) {
            '{' => depth += 1,
            '}' => {
                if (depth == 0) return null;
                depth -= 1;
                if (depth == 0) return text[0 .. i + 1];
            },
            else => {},
        }
    }

    return null;
}

fn parseFlorenceText(allocator: std.mem.Allocator, text: []const u8) ![]const u8 {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (trimmed.len == 0) return allocator.dupe(u8, trimmed);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);

    for (trimmed, 0..) |ch, i| {
        try out.append(allocator, ch);
        if (i + 1 >= trimmed.len) continue;
        const next = trimmed[i + 1];
        if (std.ascii.isLower(ch) and std.ascii.isUpper(next) and lowerRunLenEndingAt(trimmed, i) >= 5) {
            try out.append(allocator, '\n');
        } else if ((ch == '.' or ch == '!' or ch == '?') and std.ascii.isUpper(next)) {
            try out.append(allocator, '\n');
        }
    }

    return allocator.dupe(u8, out.items);
}

fn lowerRunLenEndingAt(text: []const u8, end: usize) usize {
    var len: usize = 0;
    var idx = end + 1;
    while (idx > 0) {
        idx -= 1;
        if (!std.ascii.isLower(text[idx])) break;
        len += 1;
    }
    return len;
}

fn parseDonutResult(allocator: std.mem.Allocator, text: []const u8, prompt: ?[]const u8) !Result {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    const prompt_text = prompt orelse "";

    const result_text = if (std.mem.indexOf(u8, prompt_text, "<s_docvqa>") != null)
        try donut_mod.parseDocVqaAnswer(allocator, trimmed)
    else
        try allocator.dupe(u8, trimmed);

    const fields = try donutParseFields(allocator, trimmed);
    const structured = try reader_types.structuredFromFields(allocator, fields);

    return .{
        .text = result_text,
        .fields = fields,
        .structured = structured,
        .allocator = allocator,
    };
}

fn donutParseFields(allocator: std.mem.Allocator, text: []const u8) ![]Field {
    var fields = std.ArrayListUnmanaged(Field).empty;
    errdefer {
        for (fields.items) |*field| field.deinit(allocator);
        fields.deinit(allocator);
    }

    try donutParseFieldsWithPrefix(allocator, &fields, text, "");
    return try fields.toOwnedSlice(allocator);
}

fn donutParseFieldsWithPrefix(
    allocator: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(Field),
    text: []const u8,
    prefix: []const u8,
) !void {
    var pos: usize = 0;
    while (pos < text.len) {
        const open_rel = std.mem.indexOf(u8, text[pos..], "<s_") orelse break;
        const open_idx = pos + open_rel;
        const name_start = open_idx + 3;
        const name_end_rel = std.mem.indexOfScalar(u8, text[name_start..], '>') orelse break;
        const name_end = name_start + name_end_rel;
        const name = text[name_start..name_end];
        pos = name_end + 1;

        if (!isDonutFieldName(name)) continue;

        const close_tag = try std.fmt.allocPrint(allocator, "</s_{s}>", .{name});
        defer allocator.free(close_tag);
        const close_rel = std.mem.indexOf(u8, text[pos..], close_tag) orelse continue;
        const value = std.mem.trim(u8, text[pos .. pos + close_rel], " \t\r\n");
        const full_name = if (prefix.len == 0)
            try allocator.dupe(u8, name)
        else
            try std.fmt.allocPrint(allocator, "{s}.{s}", .{ prefix, name });
        errdefer allocator.free(full_name);

        if (std.mem.indexOf(u8, value, "<s_") != null) {
            try donutParseFieldsWithPrefix(allocator, fields, value, full_name);
            allocator.free(full_name);
        } else {
            try fields.append(allocator, .{
                .name = full_name,
                .value = try allocator.dupe(u8, value),
            });
        }

        pos += close_rel + close_tag.len;
    }
}

fn isDonutFieldName(name: []const u8) bool {
    if (name.len == 0) return false;
    for (name) |ch| {
        if ((ch >= 'a' and ch <= 'z') or
            (ch >= 'A' and ch <= 'Z') or
            (ch >= '0' and ch <= '9') or
            ch == '_')
        {
            continue;
        }
        return false;
    }
    return true;
}

test "donut parser flattens nested fields" {
    const allocator = std.testing.allocator;
    const input = "<s_menu><s_nm>Coffee</s_nm><s_price>$3.50</s_price></s_menu>";
    const fields = try donutParseFields(allocator, input);
    defer {
        for (fields) |*field| field.deinit(allocator);
        allocator.free(fields);
    }

    try std.testing.expectEqual(@as(usize, 2), fields.len);
    try std.testing.expectEqualStrings("menu.nm", fields[0].name);
    try std.testing.expectEqualStrings("Coffee", fields[0].value);
    try std.testing.expectEqualStrings("menu.price", fields[1].name);
    try std.testing.expectEqualStrings("$3.50", fields[1].value);
}

test "detectParserKind recognizes pix2struct models" {
    const allocator = std.testing.allocator;
    try std.testing.expectEqual(ParserKind.pix2struct, try detectParserKind(allocator, "/tmp/models/readers/google/pix2struct-docvqa-base"));
}

test "pix2struct prompt normalization preserves natural language" {
    try std.testing.expectEqualStrings(
        "What type of document is this?",
        normalizePromptForFamily(.pix2struct, "What type of document is this?").?,
    );
}

test "pix2struct output trims plain text answers" {
    const allocator = std.testing.allocator;
    var result = try parseOutput(allocator, .pix2struct, "  invoice  \n", "What type of document is this?");
    defer result.deinit();

    try std.testing.expectEqualStrings("invoice", result.text);
}

test "donut result preserves structured object" {
    const allocator = std.testing.allocator;
    var result = try parseDonutResult(allocator, "<s_menu><s_nm>Coffee</s_nm><s_price>$3.50</s_price></s_menu>", null);
    defer result.deinit();

    try std.testing.expect(result.structured != null);
    try std.testing.expect(result.structured.? == .object);
    try std.testing.expectEqual(@as(usize, 1), result.structured.?.object.len);
}

test "florence parser inserts likely line breaks" {
    const allocator = std.testing.allocator;
    const parsed = try parseFlorenceText(allocator, "headingThis is next.LineTwo");
    defer allocator.free(parsed);

    try std.testing.expectEqualStrings("heading\nThis is next.\nLineTwo", parsed);
}

test "moondream prompt uses default instruction" {
    const allocator = std.testing.allocator;
    const prompt = try moondream_mod.buildSingleImagePrompt(allocator, null);
    defer allocator.free(prompt);

    try std.testing.expect(std.mem.indexOf(u8, prompt, "Describe this image.") != null);
    try std.testing.expect(std.mem.indexOf(u8, prompt, "\"description\"") != null);
}

test "moondream parser extracts description and fields from json" {
    const allocator = std.testing.allocator;
    var result = try parseMoondreamResult(allocator,
        \\```json
        \\{"description":"A receipt on a table","mood":"neutral","possible_source":"photo","tags":["receipt","table"]}
        \\```
    );
    defer result.deinit();

    try std.testing.expectEqualStrings("A receipt on a table", result.text);
    try std.testing.expectEqual(@as(usize, 3), result.fields.len);
    try std.testing.expectEqualStrings("mood", result.fields[0].name);
    try std.testing.expectEqualStrings("neutral", result.fields[0].value);
    try std.testing.expectEqualStrings("possible_source", result.fields[1].name);
    try std.testing.expectEqualStrings("photo", result.fields[1].value);
    try std.testing.expectEqualStrings("tags", result.fields[2].name);
    try std.testing.expectEqualStrings("receipt,table", result.fields[2].value);
    try std.testing.expect(result.structured != null);
    try std.testing.expect(result.structured.? == .object);
}
