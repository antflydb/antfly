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
const compat = @import("../../io/compat.zig");
const manifest_mod = @import("../../models/manifest.zig");
const c_file = @import("../../util/c_file.zig");
const layoutlmv3_document = @import("../../pipelines/layoutlmv3_document.zig");

const ConfigSummary = struct {
    model_type: ?[]const u8 = null,
    hidden_size: ?usize = null,
    intermediate_size: ?usize = null,
    max_position_embeddings: ?usize = null,
    max_2d_position_embeddings: ?usize = null,
    coordinate_size: ?usize = null,
    shape_size: ?usize = null,
    num_hidden_layers: ?usize = null,
    num_attention_heads: ?usize = null,
    vocab_size: ?usize = null,
    classifier_dropout: ?f32 = null,
    has_relative_attention_bias: ?bool = null,
    has_spatial_attention_bias: ?bool = null,
};

const PreprocessorSummary = struct {
    do_resize: ?bool = null,
    do_normalize: ?bool = null,
    apply_ocr: ?bool = null,
    size: ?struct {
        height: ?usize = null,
        width: ?usize = null,
        shortest_edge: ?usize = null,
    } = null,
};

const TokenizerSummary = struct {
    tokenizer_class: ?[]const u8 = null,
    model_max_length: ?usize = null,
    padding_side: ?[]const u8 = null,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_alloc = arena.allocator();

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const model_dir = args.next() orelse {
        printUsage();
        return error.MissingModelDir;
    };
    const report_path = args.next();

    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    const config = try loadOptionalJson(ConfigSummary, arena_alloc, model_dir, "config.json");
    const preprocessor = try loadOptionalJson(PreprocessorSummary, arena_alloc, model_dir, "preprocessor_config.json");
    const tokenizer = try loadOptionalJson(TokenizerSummary, arena_alloc, model_dir, "tokenizer_config.json");
    var runtime_report = try layoutlmv3_document.inspectRuntimeBundle(allocator, model_dir);
    defer runtime_report.deinit(allocator);

    const report = .{
        .task = "inspect_layoutlmv3_bundle",
        .model_dir = model_dir,
        .manifest_model_type = @tagName(manifest.model_type),
        .config_model_arch = manifest.config_model_arch,
        .native_arch_hint = @tagName(manifest.native_arch_hint),
        .safetensors_path = manifest.safetensors_path,
        .safetensors_index_path = manifest.safetensors_index_path,
        .tokenizer_type = if (manifest.tokenizer_type) |value| @tagName(value) else null,
        .model_type = if (config) |value| value.model_type else null,
        .hidden_size = if (config) |value| value.hidden_size else null,
        .intermediate_size = if (config) |value| value.intermediate_size else null,
        .max_position_embeddings = if (config) |value| value.max_position_embeddings else null,
        .max_2d_position_embeddings = if (config) |value| value.max_2d_position_embeddings else null,
        .coordinate_size = if (config) |value| value.coordinate_size else null,
        .shape_size = if (config) |value| value.shape_size else null,
        .num_hidden_layers = if (config) |value| value.num_hidden_layers else null,
        .num_attention_heads = if (config) |value| value.num_attention_heads else null,
        .vocab_size = if (config) |value| value.vocab_size else null,
        .classifier_dropout = if (config) |value| value.classifier_dropout else null,
        .has_relative_attention_bias = if (config) |value| value.has_relative_attention_bias else null,
        .has_spatial_attention_bias = if (config) |value| value.has_spatial_attention_bias else null,
        .do_resize = if (preprocessor) |value| value.do_resize else null,
        .do_normalize = if (preprocessor) |value| value.do_normalize else null,
        .apply_ocr = if (preprocessor) |value| value.apply_ocr else null,
        .input_height = if (preprocessor) |value| if (value.size) |size| size.height else null else null,
        .input_width = if (preprocessor) |value| if (value.size) |size| size.width else null else null,
        .shortest_edge = if (preprocessor) |value| if (value.size) |size| size.shortest_edge else null else null,
        .tokenizer_class = if (tokenizer) |value| value.tokenizer_class else null,
        .tokenizer_model_max_length = if (tokenizer) |value| value.model_max_length else null,
        .padding_side = if (tokenizer) |value| value.padding_side else null,
        .has_tokenizer = manifest.tokenizer_type != null,
        .has_merged_weights = manifest.safetensors_path != null or manifest.safetensors_index_path != null,
        .runtime_ready = runtime_report.looks_like_full_bundle,
        .runtime_has_preprocessor = runtime_report.has_preprocessor,
        .runtime_has_sequence_labels = runtime_report.has_sequence_labels,
        .runtime_has_token_labels = runtime_report.has_token_labels,
        .runtime_label_source_sequence = runtime_report.label_source_sequence,
        .runtime_label_source_token = runtime_report.label_source_token,
        .runtime_missing_required = runtime_report.missing_required,
    };

    if (report_path) |path| {
        const io = compat.io();
        var file = try compat.cwd().createFile(io, path, .{ .truncate = true });
        defer file.close(io);
        const rendered = try std.json.Stringify.valueAlloc(allocator, report, .{ .whitespace = .indent_2 });
        defer allocator.free(rendered);
        try file.writeStreamingAll(io, rendered);
        try file.writeStreamingAll(io, "\n");
        std.debug.print("saved_report: {s}\n", .{path});
    }

    const io = compat.io();
    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(io, &buf);
    try std.json.Stringify.value(report, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn loadOptionalJson(comptime T: type, allocator: std.mem.Allocator, model_dir: []const u8, basename: []const u8) !?T {
    const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ model_dir, basename });
    defer allocator.free(path);
    const bytes = c_file.readFile(allocator, path) catch return null;
    return try std.json.parseFromSliceLeaky(T, allocator, bytes, .{ .ignore_unknown_fields = true });
}

fn printUsage() void {
    std.debug.print(
        \\usage: inspect-layoutlmv3-bundle <model_dir> [report_path]
        \\example: inspect-layoutlmv3-bundle /tmp/layoutlmv3-base /tmp/layoutlmv3_runtime_inspect.json
        \\
    , .{});
}
