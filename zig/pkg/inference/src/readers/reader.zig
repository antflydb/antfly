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
const enc_dec_mod = @import("../pipelines/encoder_decoder.zig");
const manifest_mod = @import("../models/manifest.zig");
const reader_types = @import("types.zig");
const metal_generated_quant_stats = @import("../metal_generated_quant_stats.zig");

pub const Field = reader_types.Field;
pub const Region = reader_types.Region;
pub const Result = reader_types.Result;
pub const ReadOptions = reader_types.ReadOptions;
pub const StructuredValue = reader_types.StructuredValue;
pub const nativeFlorenceReadBatchSize = @import("../pipelines/reading.zig").nativeFlorenceReadBatchSize;

pub const BatchExecutionMode = enum { native, serial, fallback };

pub const BatchResult = struct {
    results: []Result,
    mode: BatchExecutionMode,
    native_batches: usize = 0,
    fallback_reason: ?[]const u8 = null,
};

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
        try validateVisionReadOptions(self.parser_kind, options);
        const normalized_prompt = normalizePromptForFamily(self.parser_kind, options.prompt);
        var raw = try self.core.readRaw(image_data, .{
            .prompt = normalized_prompt,
            .max_tokens = options.max_tokens,
            .source_fingerprint = options.source_fingerprint,
        });
        defer raw.deinit();

        return parseOutput(self.allocator, self.parser_kind, raw.text, normalized_prompt);
    }

    pub fn readBatch(self: *VisionLoadedReader, image_datas: []const []const u8, options: ReadOptions) ![]Result {
        return (try self.readBatchReported(image_datas, options)).results;
    }

    pub fn readBatchReported(self: *VisionLoadedReader, image_datas: []const []const u8, options: ReadOptions) !BatchResult {
        try validateVisionReadOptions(self.parser_kind, options);
        const normalized_prompt = normalizePromptForFamily(self.parser_kind, options.prompt);
        const raw_batch = try self.core.readRawBatchReported(image_datas, .{
            .prompt = normalized_prompt,
            .max_tokens = options.max_tokens,
            .source_fingerprint = options.source_fingerprint,
        });
        const raw_results = raw_batch.results;
        defer {
            for (raw_results) |raw| {
                var tmp = raw;
                tmp.deinit();
            }
            self.allocator.free(raw_results);
        }

        const out = try self.allocator.alloc(Result, raw_results.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |*result| result.deinit();
            self.allocator.free(out);
        }

        for (raw_results, 0..) |raw, i| {
            out[i] = try parseOutput(self.allocator, self.parser_kind, raw.text, normalized_prompt);
            filled += 1;
        }
        return .{
            .results = out,
            .mode = switch (raw_batch.mode) {
                .native => .native,
                .serial => .serial,
                .fallback => .fallback,
            },
            .native_batches = raw_batch.native_batches,
            .fallback_reason = raw_batch.fallback_reason,
        };
    }

    pub fn inputTokenCount(self: *VisionLoadedReader, options: ReadOptions) !usize {
        try validateVisionReadOptions(self.parser_kind, options);
        const normalized_prompt = normalizePromptForFamily(self.parser_kind, options.prompt);
        return self.core.inputTokenCount(.{
            .prompt = normalized_prompt,
            .max_tokens = options.max_tokens,
            .cache_dtype = options.cache_dtype,
            .source_fingerprint = options.source_fingerprint,
        });
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

    pub fn readBatch(self: *VlmLoadedReader, image_datas: []const []const u8, options: ReadOptions) ![]Result {
        return readBatchSerial(VlmLoadedReader, self, image_datas, options);
    }

    pub fn inputTokenCount(self: *VlmLoadedReader, options: ReadOptions) !usize {
        const prompt = switch (self.parser_kind) {
            .moondream => try moondream_mod.buildSingleImagePrompt(self.allocator, options.prompt),
            else => return error.InvalidModelForReading,
        };
        defer self.allocator.free(prompt);
        return self.pipeline.inputTokenCount(prompt);
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

    pub fn readBatch(self: *GenAiLoadedReader, image_datas: []const []const u8, options: ReadOptions) ![]Result {
        return readBatchSerial(GenAiLoadedReader, self, image_datas, options);
    }

    pub fn inputTokenCount(self: *GenAiLoadedReader, options: ReadOptions) !usize {
        if (!build_options.enable_onnx) return error.OnnxNotEnabled;
        const prompt = switch (self.parser_kind) {
            .moondream => try moondream_mod.buildSingleImagePrompt(self.allocator, options.prompt),
            else => return error.InvalidModelForReading,
        };
        defer self.allocator.free(prompt);
        return ortgenai.inputTokenCount(self.allocator, &self.model, prompt);
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
            return .{ .multistage = try multistage_reader_mod.LoadedMultiStageReader.loadFromDir(
                allocator,
                model_path,
                session_manager,
                model_manager,
            ) };
        }

        const parser_kind = try detectParserKind(allocator, model_path);
        if (parser_kind == .moondream) {
            if (try session_manager.allowsDirectBackend(.onnx)) {
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
        try validateReadOptions(options);
        var result = try switch (self.*) {
            .vision => |*reader| reader.read(image_data, options),
            .genai => |*reader| reader.read(image_data, options),
            .vlm => |*reader| reader.read(image_data, options),
            .multistage => |*reader| reader.read(image_data, options),
        };
        errdefer result.deinit();
        try sanitizeResultUtf8(&result);
        return result;
    }

    pub fn snapshotMetalGeneratedQuantStats(self: *LoadedReader, allocator: std.mem.Allocator) metal_generated_quant_stats.Stats {
        return switch (self.*) {
            .vision => |*reader| reader.core.snapshotMetalGeneratedQuantStats(allocator),
            .genai, .vlm, .multistage => .{},
        };
    }

    pub fn readBatch(self: *LoadedReader, image_datas: []const []const u8, options: ReadOptions) ![]Result {
        return (try self.readBatchReported(image_datas, options)).results;
    }

    pub fn readBatchReported(self: *LoadedReader, image_datas: []const []const u8, options: ReadOptions) !BatchResult {
        try validateReadOptions(options);
        const allocator = self.resultAllocator();
        const batch = try switch (self.*) {
            .vision => |*reader| reader.readBatchReported(image_datas, options),
            .genai => |*reader| BatchResult{ .results = try reader.readBatch(image_datas, options), .mode = .serial },
            .vlm => |*reader| BatchResult{ .results = try reader.readBatch(image_datas, options), .mode = .serial },
            .multistage => |*reader| BatchResult{ .results = try readBatchSerial(@TypeOf(reader.*), reader, image_datas, options), .mode = .serial },
        };
        errdefer {
            for (batch.results) |*result| result.deinit();
            allocator.free(batch.results);
        }
        for (batch.results) |*result| try sanitizeResultUtf8(result);
        return batch;
    }

    /// Return the exact per-item textual input length produced by this loaded
    /// reader. Multi-stage OCR has no text input; every generative reader uses
    /// the same prompt builder and tokenizer as execution.
    pub fn inputTokenCount(self: *LoadedReader, options: ReadOptions) !usize {
        try validateReadOptions(options);
        return switch (self.*) {
            .vision => |*reader| reader.inputTokenCount(options),
            .genai => |*reader| reader.inputTokenCount(options),
            .vlm => |*reader| reader.inputTokenCount(options),
            .multistage => 0,
        };
    }

    fn resultAllocator(self: *LoadedReader) std.mem.Allocator {
        return switch (self.*) {
            inline else => |*reader| reader.allocator,
        };
    }
};

fn readBatchSerial(comptime ReaderType: type, reader: *ReaderType, image_datas: []const []const u8, options: ReadOptions) ![]Result {
    const allocator = reader.allocator;
    const out = try allocator.alloc(Result, image_datas.len);
    var filled: usize = 0;
    errdefer {
        for (out[0..filled]) |*result| result.deinit();
        allocator.free(out);
    }

    for (image_datas, 0..) |image_data, i| {
        out[i] = try reader.read(image_data, options);
        filled += 1;
    }
    return out;
}

fn validateReadOptions(options: ReadOptions) !void {
    if (options.max_tokens) |max_tokens| {
        if (max_tokens == 0) return error.InvalidMaxTokens;
    }
}

fn validateVisionReadOptions(parser_kind: ParserKind, options: ReadOptions) !void {
    if (parser_kind != .florence) return;
    if (options.cache_dtype) |cache_dtype| {
        if (!std.mem.eql(u8, cache_dtype, "f32")) return error.UnsupportedCacheDtype;
    }
}

pub fn isSupportedModelDir(allocator: std.mem.Allocator, model_path: []const u8) bool {
    if (multistage_metadata.isMultiStageModelDir(allocator, model_path)) return true;

    const parser_kind = detectParserKind(allocator, model_path) catch return false;
    if (parser_kind == .moondream) {
        return onnx_decoder_only_vlm.isSupportedModelDir(allocator, model_path);
    }

    return vision_reader_mod.isSupportedModelDir(allocator, model_path);
}

/// Same check, reusing a manifest the caller already loaded.
///
/// The directory-based form ends in a full `manifest.loadFromDir`, which parses GGUF
/// tokenizer metadata. Running that once per model made `/ai/v1/models` take about a
/// second per GGUF model even though the listing never needed those fields.
pub fn isSupportedManifest(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    man: manifest_mod.ModelManifest,
) bool {
    if (multistage_metadata.isMultiStageModelDir(allocator, model_path)) return true;

    const parser_kind = detectParserKind(allocator, model_path) catch return false;
    if (parser_kind == .moondream) {
        return onnx_decoder_only_vlm.isSupportedModelDir(allocator, model_path);
    }

    if (enc_dec_mod.hasEncoderDecoderPaths(allocator, model_path, man)) return true;

    return vision_reader_mod.isSupportedManifest(man);
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
    // Generation cut off at max_tokens can end mid-multibyte character, leaving
    // invalid UTF-8. std.json serializes such slices as arrays of byte integers
    // instead of strings, breaking API clients — drop invalid sequences first.
    const sanitized = try sanitizeUtf8Alloc(allocator, text);
    defer if (sanitized) |s| allocator.free(s);
    const clean_text = sanitized orelse text;

    return switch (parser_kind) {
        .default => .{
            .text = try allocator.dupe(u8, std.mem.trim(u8, clean_text, " \t\r\n")),
            .allocator = allocator,
        },
        .florence => .{
            .text = try parseFlorenceText(allocator, clean_text),
            .allocator = allocator,
        },
        .donut => try parseDonutResult(allocator, clean_text, prompt),
        .moondream => try parseMoondreamResult(allocator, clean_text),
        .pix2struct => .{
            .text = try allocator.dupe(u8, std.mem.trim(u8, clean_text, " \t\r\n")),
            .allocator = allocator,
        },
    };
}

/// Returns a copy of `text` with invalid UTF-8 byte sequences removed, or null
/// when `text` is already valid (no allocation needed).
fn sanitizeUtf8Alloc(allocator: std.mem.Allocator, text: []const u8) !?[]u8 {
    if (std.unicode.utf8ValidateSlice(text)) return null;
    var out = try std.ArrayListUnmanaged(u8).initCapacity(allocator, text.len);
    errdefer out.deinit(allocator);
    var i: usize = 0;
    while (i < text.len) {
        const seq_len = std.unicode.utf8ByteSequenceLength(text[i]) catch {
            i += 1; // invalid lead byte
            continue;
        };
        if (i + seq_len > text.len) break; // sequence truncated at end of text
        if (std.unicode.utf8ValidateSlice(text[i .. i + seq_len])) {
            out.appendSliceAssumeCapacity(text[i .. i + seq_len]);
            i += seq_len;
        } else {
            i += 1; // invalid continuation byte; resync on next byte
        }
    }
    return try out.toOwnedSlice(allocator);
}

fn sanitizeOwnedUtf8(allocator: std.mem.Allocator, value: *[]const u8) !void {
    if (try sanitizeUtf8Alloc(allocator, value.*)) |sanitized| {
        allocator.free(value.*);
        value.* = sanitized;
    }
}

fn sanitizeResultUtf8(result: *Result) !void {
    try sanitizeOwnedUtf8(result.allocator, &result.text);
    for (result.fields) |*field| {
        try sanitizeOwnedUtf8(result.allocator, &field.name);
        try sanitizeOwnedUtf8(result.allocator, &field.value);
    }
    for (result.regions) |*region| {
        try sanitizeOwnedUtf8(result.allocator, &region.text);
        if (region.label) |label| {
            var sanitized_label = label;
            try sanitizeOwnedUtf8(result.allocator, &sanitized_label);
            region.label = sanitized_label;
        }
    }
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

test "sanitizeUtf8Alloc passes valid text through without allocating" {
    const allocator = std.testing.allocator;
    try std.testing.expectEqual(@as(?[]u8, null), try sanitizeUtf8Alloc(allocator, "hello 世界"));
}

test "sanitizeUtf8Alloc drops sequence truncated at end of text" {
    const allocator = std.testing.allocator;
    // "世" is E4 B8 96; cut after two bytes to simulate a max_tokens cutoff.
    const sanitized = (try sanitizeUtf8Alloc(allocator, "abc\xE4\xB8")).?;
    defer allocator.free(sanitized);
    try std.testing.expectEqualStrings("abc", sanitized);
}

test "sanitizeUtf8Alloc drops interior invalid bytes and resyncs" {
    const allocator = std.testing.allocator;
    const sanitized = (try sanitizeUtf8Alloc(allocator, "a\xFFb\xE4\xB8\x96c")).?;
    defer allocator.free(sanitized);
    try std.testing.expectEqualStrings("ab世c", sanitized);
}

test "parseOutput sanitizes truncated utf8 for florence" {
    const allocator = std.testing.allocator;
    var result = try parseOutput(allocator, .florence, "5月普\xE8\xA7", null);
    defer result.deinit();
    try std.testing.expectEqualStrings("5月普", result.text);
    try std.testing.expect(std.unicode.utf8ValidateSlice(result.text));
}

test "LoadedReader options reject zero tokens and Florence cache dtypes" {
    try std.testing.expectError(error.InvalidMaxTokens, validateReadOptions(.{ .max_tokens = 0 }));
    try validateVisionReadOptions(.florence, .{});
    try validateVisionReadOptions(.florence, .{ .cache_dtype = "f32" });
    try std.testing.expectError(error.UnsupportedCacheDtype, validateVisionReadOptions(.florence, .{ .cache_dtype = "f16" }));
    try std.testing.expectError(error.UnsupportedCacheDtype, validateVisionReadOptions(.florence, .{ .cache_dtype = "F32" }));
    try validateVisionReadOptions(.default, .{ .cache_dtype = "f16" });
}

test "vision reader validates max tokens against the model limit" {
    try std.testing.expectEqual(@as(usize, 512), try vision_reader_mod.resolveMaxLength(512, null, 1));
    try std.testing.expectEqual(@as(usize, 3), try vision_reader_mod.resolveMaxLength(512, 2, 1));
    try std.testing.expectEqual(@as(usize, 4), try vision_reader_mod.resolveMaxLength(512, 2, 2));
    try std.testing.expectEqual(@as(usize, 512), try vision_reader_mod.resolveMaxLength(512, 511, 1));
    try std.testing.expectEqual(@as(usize, 512), try vision_reader_mod.resolveMaxLength(512, 510, 2));
    try std.testing.expectError(error.InvalidMaxTokens, vision_reader_mod.resolveMaxLength(512, 0, 1));
    try std.testing.expectError(error.InvalidMaxTokens, vision_reader_mod.resolveMaxLength(512, 512, 1));
    try std.testing.expectError(error.InvalidMaxTokens, vision_reader_mod.resolveMaxLength(512, 511, 2));
    try std.testing.expectError(error.InvalidMaxTokens, vision_reader_mod.resolveMaxLength(512, std.math.maxInt(usize), 1));
}

test "sanitizeResultUtf8 covers text fields and regions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const fields = try allocator.alloc(Field, 1);
    fields[0] = .{
        .name = try allocator.dupe(u8, "na\xFFme"),
        .value = try allocator.dupe(u8, "va\xFFlue"),
    };
    const regions = try allocator.alloc(Region, 1);
    regions[0] = .{
        .text = try allocator.dupe(u8, "re\xFFgion"),
        .bbox = .{ 0, 0, 1, 1 },
        .label = try allocator.dupe(u8, "la\xFFbel"),
    };
    var result = Result{
        .text = try allocator.dupe(u8, "te\xFFxt"),
        .fields = fields,
        .regions = regions,
        .allocator = allocator,
    };

    try sanitizeResultUtf8(&result);
    try std.testing.expectEqualStrings("text", result.text);
    try std.testing.expectEqualStrings("name", result.fields[0].name);
    try std.testing.expectEqualStrings("value", result.fields[0].value);
    try std.testing.expectEqualStrings("region", result.regions[0].text);
    try std.testing.expectEqualStrings("label", result.regions[0].label.?);
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
