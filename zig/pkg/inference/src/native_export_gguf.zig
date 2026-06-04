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
const compat = @import("io/compat.zig");
const c_file = @import("util/c_file.zig");
const manifest_mod = @import("models/manifest.zig");
const tensor_access_mod = @import("models/tensor_access.zig");
const gguf_mod = @import("gguf/root.zig");
const gpt_mod = @import("models/gpt.zig");
const bert_mod = @import("models/bert.zig");
const t5_mod = @import("models/t5.zig");
const whisper_mod = @import("models/whisper.zig");
const deberta_mod = @import("models/deberta.zig");
const layoutlmv3_mod = @import("models/layoutlmv3.zig");
const clip_mod = @import("models/clip.zig");
const clap_mod = @import("models/clap.zig");
const florence_mod = @import("models/florence.zig");
const model_manager_mod = @import("server/model_manager.zig");
const projector_format_mod = @import("architectures/projector_format.zig");
const qwen2vl_types_mod = @import("architectures/qwen2vl_types.zig");
const session_factory_mod = @import("architectures/session_factory.zig");
const backends_mod = @import("backends/backends.zig");
const weight_source_mod = @import("models/weight_source.zig");
const hf_tokenizer_mod = @import("inference_hf_tokenizer");
const tokenizer_mod = @import("inference_tokenizer");
const multimodal_reranker_mod = @import("pipelines/multimodal_reranker.zig");
const qwen2vl_multimodal_mod = @import("pipelines/qwen2vl_multimodal.zig");
const variants_manifest = @import("quantize/variants_manifest.zig");

const print = std.debug.print;
const io_compat = compat.io;

const QuantizationMode = enum {
    none,
    q1_0,
    q2_k,
    q3_k,
    q4_0,
    q4_1,
    q5_0,
    q5_1,
    q4_k,
    q5_k,
    q6_k,
    q8_k,
    q8_0,
    q8_1,
};

const ProjectorFormat = enum {
    auto,
    antfly,
    clip,
};

const ResolvedProjectorFormat = enum {
    antfly,
    clip,
};

const Options = struct {
    model_dir: []const u8,
    output_path: ?[]const u8 = null,
    projector_output_path: ?[]const u8 = null,
    projector_format: ProjectorFormat = .auto,
    quantization: QuantizationMode = .none,
    quantize_include_prefixes: ?[]const u8 = null,
    quantize_exclude_prefixes: ?[]const u8 = null,
    dry_run: bool = false,
};

const QuantizationFilter = struct {
    include_prefixes_csv: ?[]const u8 = null,
    exclude_prefixes_csv: ?[]const u8 = null,
};

const TensorTransform = enum {
    none,
    transpose_2d_dense,
    onnx_gru_zrh_to_rzn,
};

const TensorPlan = struct {
    source_name: []const u8,
    output_name: []const u8,
    dimensions: []u64,
    tensor_type: gguf_mod.tensor_types.TensorType,
    quantization: QuantizationMode = .none,
    transform: TensorTransform = .none,
    source_byte_range: ?SourceByteRange = null,

    fn deinit(self: *TensorPlan, allocator: std.mem.Allocator) void {
        allocator.free(self.source_name);
        allocator.free(self.output_name);
        allocator.free(self.dimensions);
    }
};

const SourceByteRange = struct {
    start: usize,
    len: usize,
};

const ExportPlan = struct {
    metadata_owned: ?[]gguf_mod.format.MetadataEntry = null,
    borrowed_metadata_file: ?*const gguf_mod.format.File = null,
    tensors: []TensorPlan,

    fn metadata(self: *const ExportPlan) []const gguf_mod.format.MetadataEntry {
        if (self.metadata_owned) |items| return items;
        if (self.borrowed_metadata_file) |file| return file.metadata;
        return &.{};
    }

    fn deinit(self: *ExportPlan, allocator: std.mem.Allocator) void {
        if (self.metadata_owned) |items| {
            for (items) |*entry| entry.deinit(allocator);
            allocator.free(items);
        }
        for (self.tensors) |*tensor| tensor.deinit(allocator);
        allocator.free(self.tensors);
    }
};

const ExportSourceKind = enum {
    gguf,
    dense,
};

const DenseDecoderExportSupport = enum {
    supported,
    unsupported_name_mapping,
    unsupported_tensor_transform,
};

const UnsupportedModelReason = enum {
    composite_wrapper,
    unsupported_architecture,
};

const TensorNameFilter = enum {
    all,
    clipclap_clip,
    clipclap_clap,
};

const ExportArchitecture = union(enum) {
    gpt: gpt_mod.Config,
    bert: bert_mod.Config,
    t5: t5_mod.Config,
    whisper: whisper_mod.Config,
    deberta: deberta_mod.Config,
    layoutlmv3: layoutlmv3_mod.Config,
    florence: florence_mod.Config,
    clip: clip_mod.Config,
    clap: clap_mod.Config,
};

const PlannedExport = struct {
    source_kind: ExportSourceKind,
    plan: ExportPlan,
    projector_source_path: ?[]const u8 = null,
    projector_plan: ?ExportPlan = null,
    projector_format: ?ResolvedProjectorFormat = null,

    fn deinit(self: *PlannedExport, allocator: std.mem.Allocator) void {
        self.plan.deinit(allocator);
        if (self.projector_source_path) |path| allocator.free(path);
        if (self.projector_plan) |*plan| plan.deinit(allocator);
    }
};

pub fn main(allocator: std.mem.Allocator, _: std.Io, args: []const []const u8) !void {
    const opts = try parseArgs(args);
    const output_path = if (opts.output_path) |value|
        value
    else
        try std.fs.path.join(allocator, &.{ opts.model_dir, "export.gguf" });
    defer if (opts.output_path == null) allocator.free(output_path);

    const filter: QuantizationFilter = .{
        .include_prefixes_csv = opts.quantize_include_prefixes,
        .exclude_prefixes_csv = opts.quantize_exclude_prefixes,
    };
    {
        var manifest = try manifest_mod.loadFromDir(allocator, opts.model_dir);
        defer manifest.deinit();
        if (isGlinerBundleExportManifest(manifest)) {
            if (opts.dry_run) {
                const report = try formatGlinerBundleDryRunReport(allocator, opts.model_dir, output_path, opts.quantization, filter);
                defer allocator.free(report);
                print("{s}", .{report});
                return;
            }
            try exportGlinerBundleToGguf(allocator, opts.model_dir, output_path, opts.quantization, filter);
            print("exported gguf to {s}\n", .{output_path});
            return;
        }
        if (isClipclapExportManifest(manifest)) {
            if (opts.projector_output_path != null or opts.projector_format != .auto) return error.UnsupportedArgumentsForGgufExport;
            const bundle_dir = try bundleDirFromOutputPath(allocator, output_path);
            defer allocator.free(bundle_dir);
            if (opts.dry_run) {
                const report = try formatClipclapBundleDryRunReport(allocator, opts.model_dir, bundle_dir, opts.quantization, filter);
                defer allocator.free(report);
                print("{s}", .{report});
                return;
            }
            try exportClipclapBundleToGguf(allocator, opts.model_dir, bundle_dir, opts.quantization, filter);
            print("exported clipclap gguf bundle to {s}\n", .{bundle_dir});
            return;
        }
    }
    var planned = buildPlannedExport(allocator, opts.model_dir, opts.quantization, opts.projector_format, filter) catch |err| switch (err) {
        error.UnsupportedDenseArchitectureForGgufExport => {
            if (!opts.dry_run) return err;
            const report = try formatUnsupportedDenseDryRunReport(allocator, opts.model_dir, output_path, opts.quantization, filter);
            defer allocator.free(report);
            print("{s}", .{report});
            return;
        },
        error.UnsupportedCompositeModelForGgufExport => {
            if (!opts.dry_run) return err;
            const report = try formatUnsupportedModelDryRunReport(allocator, opts.model_dir, output_path, opts.quantization, filter, .composite_wrapper);
            defer allocator.free(report);
            print("{s}", .{report});
            return;
        },
        error.UnsupportedModelForGgufExport => {
            if (!opts.dry_run) return err;
            const report = try formatUnsupportedModelDryRunReport(allocator, opts.model_dir, output_path, opts.quantization, filter, .unsupported_architecture);
            defer allocator.free(report);
            print("{s}", .{report});
            return;
        },
        else => return err,
    };
    defer planned.deinit(allocator);

    const projector_output_path = if (planned.projector_source_path != null or planned.projector_plan != null)
        if (opts.projector_output_path) |value|
            value
        else
            try defaultProjectorOutputPath(allocator, output_path)
    else
        null;
    defer if ((planned.projector_source_path != null or planned.projector_plan != null) and opts.projector_output_path == null) allocator.free(projector_output_path.?);

    if (opts.dry_run) {
        const report = try formatDryRunReport(allocator, opts.model_dir, output_path, projector_output_path, opts.quantization, filter, planned.source_kind, &planned);
        defer allocator.free(report);
        print("{s}", .{report});
        return;
    }

    try writePlannedExport(allocator, opts.model_dir, output_path, projector_output_path, &planned);
    print("exported gguf to {s}\n", .{output_path});
    if (projector_output_path) |path| {
        print("exported projector gguf to {s}\n", .{path});
    }
}

pub fn exportModelDirToGguf(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    output_path: []const u8,
    quantization: QuantizationMode,
) !void {
    return exportModelDirToGgufFiltered(allocator, model_dir, output_path, quantization, .{});
}

fn exportModelDirToGgufFiltered(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    output_path: []const u8,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
) !void {
    return exportModelDirToGgufFilteredWithProjector(allocator, model_dir, output_path, null, .auto, quantization, filter);
}

fn exportModelDirToGgufFilteredWithProjector(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    output_path: []const u8,
    projector_output_path: ?[]const u8,
    projector_format: ProjectorFormat,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
) !void {
    {
        var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
        defer manifest.deinit();
        if (isGlinerBundleExportManifest(manifest)) {
            if (projector_output_path != null or projector_format != .auto) return error.UnsupportedArgumentsForGgufExport;
            return exportGlinerBundleToGguf(allocator, model_dir, output_path, quantization, filter);
        }
        if (isClipclapExportManifest(manifest)) {
            if (projector_output_path != null or projector_format != .auto) return error.UnsupportedArgumentsForGgufExport;
            const bundle_dir = try bundleDirFromOutputPath(allocator, output_path);
            defer allocator.free(bundle_dir);
            return exportClipclapBundleToGguf(allocator, model_dir, bundle_dir, quantization, filter);
        }
    }
    var planned = try buildPlannedExport(allocator, model_dir, quantization, projector_format, filter);
    defer planned.deinit(allocator);

    return writePlannedExport(allocator, model_dir, output_path, projector_output_path, &planned);
}

fn writePlannedExport(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    output_path: []const u8,
    projector_output_path: ?[]const u8,
    planned: *const PlannedExport,
) !void {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    try preferClipclapDefaultOnnxInputs(allocator, model_dir, &manifest);

    var access = try tensor_access_mod.openFromManifest(allocator, manifest);
    defer access.deinit();

    try writeSinglePlanExport(allocator, output_path, access, &planned.plan);

    if (planned.projector_source_path) |source_path| {
        const target_path = projector_output_path orelse return error.MissingProjectorOutputPath;
        try copyProjectorGguf(allocator, source_path, target_path);
    } else if (planned.projector_plan) |projector_plan| {
        const target_path = projector_output_path orelse return error.MissingProjectorOutputPath;
        const projector_specs = try allocator.alloc(gguf_mod.writer.TensorSpec, projector_plan.tensors.len);
        defer allocator.free(projector_specs);
        for (projector_plan.tensors, 0..) |tensor, i| {
            projector_specs[i] = .{
                .name = tensor.output_name,
                .dimensions = tensor.dimensions,
                .tensor_type = tensor.tensor_type,
            };
        }
        var projector_layout = try gguf_mod.writer.buildLayout(allocator, projector_plan.metadata(), projector_specs);
        defer projector_layout.deinit(allocator);
        try writeExportFile(allocator, target_path, projector_layout, access, projector_plan.tensors);
    }

    if (isColqwenBundleExportManifest(manifest)) {
        try exportColqwenBundleSidecars(allocator, model_dir, output_path, manifest);
    }
}

fn writeSinglePlanExport(
    allocator: std.mem.Allocator,
    output_path: []const u8,
    access: tensor_access_mod.TensorAccess,
    plan: *const ExportPlan,
) !void {
    const tensor_specs = try allocator.alloc(gguf_mod.writer.TensorSpec, plan.tensors.len);
    defer allocator.free(tensor_specs);
    for (plan.tensors, 0..) |tensor, i| {
        tensor_specs[i] = .{
            .name = tensor.output_name,
            .dimensions = tensor.dimensions,
            .tensor_type = tensor.tensor_type,
        };
    }

    var layout = try gguf_mod.writer.buildLayout(allocator, plan.metadata(), tensor_specs);
    defer layout.deinit(allocator);

    try writeExportFile(allocator, output_path, layout, access, plan.tensors);
}

fn isGlinerBundleExportManifest(manifest: manifest_mod.ModelManifest) bool {
    return manifest.gliner_model_type.len > 0 or std.mem.eql(u8, manifest.config_model_arch, "extractor");
}

fn isColqwenBundleExportManifest(manifest: manifest_mod.ModelManifest) bool {
    return manifest.isColqwenBundle();
}

fn isClipclapExportManifest(manifest: manifest_mod.ModelManifest) bool {
    return std.mem.eql(u8, manifest.config_model_arch, "clipclap") or
        std.mem.eql(u8, manifest.inference_bundle_family, "clipclap_gguf_bundle/v1");
}

const colqwen_required_sidecars = [_][]const u8{
    "config.json",
    "model_manifest.json",
    "tokenizer.json",
    "tokenizer_config.json",
    "preprocessor_config.json",
    "processor_config.json",
};

const colqwen_optional_sidecars = [_][]const u8{
    "special_tokens_map.json",
};

fn exportColqwenBundleSidecars(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    output_path: []const u8,
    manifest: manifest_mod.ModelManifest,
) !void {
    const out_dir = std.fs.path.dirname(output_path) orelse ".";

    for (colqwen_required_sidecars) |file_name| {
        if (std.mem.eql(u8, file_name, "model_manifest.json")) continue;
        try copyRequiredSidecar(allocator, model_dir, out_dir, file_name);
    }
    for (colqwen_optional_sidecars) |file_name| {
        try copySidecarIfExists(allocator, model_dir, out_dir, file_name);
    }

    const model_manifest_out = try std.fs.path.join(allocator, &.{ out_dir, "model_manifest.json" });
    defer allocator.free(model_manifest_out);
    const manifest_json = try synthesizeColqwenModelManifestJson(allocator, manifest);
    defer allocator.free(manifest_json);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = model_manifest_out, .data = manifest_json });

    try writeColqwenBundleMarker(allocator, out_dir, output_path);
}

fn copySidecarIfExists(
    allocator: std.mem.Allocator,
    source_dir: []const u8,
    out_dir: []const u8,
    file_name: []const u8,
) !void {
    if (!c_file.fileExistsInDir(allocator, source_dir, file_name)) return;
    const src_path = try std.fs.path.join(allocator, &.{ source_dir, file_name });
    defer allocator.free(src_path);
    const dst_path = try std.fs.path.join(allocator, &.{ out_dir, file_name });
    defer allocator.free(dst_path);
    try copyFileStreaming(allocator, src_path, dst_path);
}

fn trimRightSlash(value: []const u8) []const u8 {
    var end = value.len;
    while (end > 0 and value[end - 1] == '/') : (end -= 1) {}
    return value[0..end];
}

fn copyFileStreaming(allocator: std.mem.Allocator, src_path: []const u8, dst_path: []const u8) !void {
    const io = compat.io();
    if (try pathsReferToSameFile(allocator, io, src_path, dst_path)) return;

    var src = try compat.cwd().openFile(io, src_path, .{});
    defer src.close(io);
    var dst = try compat.cwd().createFile(io, dst_path, .{ .truncate = true });
    defer dst.close(io);

    var buf: [1024 * 1024]u8 = undefined;
    while (true) {
        const n = src.readStreaming(io, &.{buf[0..]}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (n == 0) break;
        try dst.writeStreamingAll(io, buf[0..n]);
    }
}

fn pathsReferToSameFile(allocator: std.mem.Allocator, io: std.Io, src_path: []const u8, dst_path: []const u8) !bool {
    if (std.mem.eql(u8, trimRightSlash(src_path), trimRightSlash(dst_path))) return true;

    const src_real = try compat.cwd().realPathFileAlloc(io, src_path, allocator);
    defer allocator.free(src_real);
    const dst_real = compat.cwd().realPathFileAlloc(io, dst_path, allocator) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer allocator.free(dst_real);
    return std.mem.eql(u8, src_real, dst_real);
}

test "copyFileStreaming skips same file through canonical alias" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "same-file-streaming-copy");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const source_path = try std.fs.path.join(allocator, &.{ dir_path, "payload.bin" });
    defer allocator.free(source_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = source_path, .data = "payload-data" });

    const alias_path = try std.fmt.allocPrint(allocator, "{s}/./payload.bin", .{dir_path});
    defer allocator.free(alias_path);
    try copyFileStreaming(allocator, source_path, alias_path);

    const after = try c_file.readFile(allocator, source_path);
    defer allocator.free(after);
    try std.testing.expectEqualStrings("payload-data", after);
}

fn copyRequiredSidecar(
    allocator: std.mem.Allocator,
    source_dir: []const u8,
    out_dir: []const u8,
    file_name: []const u8,
) !void {
    if (!c_file.fileExistsInDir(allocator, source_dir, file_name)) return error.MissingRequiredColqwenSidecar;
    try copySidecarIfExists(allocator, source_dir, out_dir, file_name);
}

fn writeColqwenBundleMarker(
    allocator: std.mem.Allocator,
    out_dir: []const u8,
    output_path: []const u8,
) !void {
    const bundle_marker_path = try std.fs.path.join(allocator, &.{ out_dir, "antfly_inference_bundle.json" });
    defer allocator.free(bundle_marker_path);
    const gguf_name = std.fs.path.basename(output_path);
    const marker_json = try std.json.Stringify.valueAlloc(allocator, .{
        .family = "colqwen2_gguf_bundle/v1",
        .wrapper = "colqwen2",
        .model = gguf_name,
        .required_sidecars = &colqwen_required_sidecars,
        .optional_sidecars = &colqwen_optional_sidecars,
        .capabilities = &[_][]const u8{ "colqwen", "multimodal_late_interaction", "late_interaction" },
        .inputs = &[_][]const u8{ "text", "image" },
    }, .{});
    defer allocator.free(marker_json);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = bundle_marker_path, .data = marker_json });
}

fn synthesizeColqwenModelManifestJson(
    allocator: std.mem.Allocator,
    manifest: manifest_mod.ModelManifest,
) ![]u8 {
    var caps = std.ArrayListUnmanaged([]const u8).empty;
    defer caps.deinit(allocator);
    for (manifest.capabilities) |cap| {
        try caps.append(allocator, cap);
    }
    if (!manifest.hasCapability("colqwen")) {
        try caps.append(allocator, "colqwen");
    }
    if (!manifest.hasCapability("multimodal_late_interaction")) {
        try caps.append(allocator, "multimodal_late_interaction");
    }
    if (!manifest.hasCapability("late_interaction")) {
        try caps.append(allocator, "late_interaction");
    }
    return std.json.Stringify.valueAlloc(allocator, .{
        .type = "reranker",
        .capabilities = caps.items,
        .inputs = &[_][]const u8{ "text", "image" },
    }, .{});
}

const clipclap_required_sidecars = [_][]const u8{
    "clip_config.json",
    "model_manifest.json",
    "tokenizer.json",
    "tokenizer_config.json",
    "processor_config.json",
};

const clipclap_optional_sidecars = [_][]const u8{
    "special_tokens_map.json",
    "preprocessor_config.json",
    "text_model.onnx",
    "text_model.onnx.data",
    "visual_model.onnx",
    "visual_model.onnx.data",
    "audio_model.onnx",
    "audio_model.onnx.data",
    "text_projection.onnx",
    "text_projection.onnx.data",
    "visual_projection.onnx",
    "visual_projection.onnx.data",
    "audio_projection.onnx",
    "audio_projection.onnx.data",
};

const ClipclapBundlePlans = struct {
    clip: PlannedExport,
    clap: PlannedExport,

    fn deinit(self: *ClipclapBundlePlans, allocator: std.mem.Allocator) void {
        self.clip.deinit(allocator);
        self.clap.deinit(allocator);
    }
};

fn bundleDirFromOutputPath(allocator: std.mem.Allocator, output_path: []const u8) ![]const u8 {
    if (std.mem.endsWith(u8, output_path, ".gguf")) {
        return allocator.dupe(u8, output_path[0 .. output_path.len - ".gguf".len]);
    }
    return allocator.dupe(u8, output_path);
}

fn buildClipclapBundlePlans(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
    access: tensor_access_mod.TensorAccess,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
) !ClipclapBundlePlans {
    const config_bytes = c_file.readFileFromDir(allocator, model_dir, "clip_config.json") catch |err| switch (err) {
        error.FileNotFound => try c_file.readFileFromDir(allocator, model_dir, "config.json"),
        else => return err,
    };
    defer allocator.free(config_bytes);

    const clip_config = try clip_mod.parseConfig(allocator, config_bytes);
    const clap_config = try clap_mod.parseConfig(allocator, config_bytes);

    var clip_plan = try buildClipPlannedExportFiltered(
        allocator,
        model_dir,
        manifest,
        access,
        clip_config,
        quantization,
        filter,
        null,
        .dense,
        .clipclap_clip,
    );
    errdefer clip_plan.deinit(allocator);

    var clap_plan = try buildClapPlannedExportFiltered(
        allocator,
        model_dir,
        manifest,
        access,
        clap_config,
        quantization,
        filter,
        null,
        .dense,
        .clipclap_clap,
    );
    errdefer clap_plan.deinit(allocator);

    return .{
        .clip = clip_plan,
        .clap = clap_plan,
    };
}

fn formatClipclapBundleDryRunReport(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle_dir: []const u8,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
) ![]u8 {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    try preferClipclapDefaultOnnxInputs(allocator, model_dir, &manifest);

    var access = try tensor_access_mod.openFromManifest(allocator, manifest);
    defer access.deinit();

    var plans = try buildClipclapBundlePlans(allocator, model_dir, manifest, access, quantization, filter);
    defer plans.deinit(allocator);

    const clip_name = try variants_manifest.clipclapGgufName(allocator, "clip", @tagName(quantization));
    defer allocator.free(clip_name);
    const clap_name = try variants_manifest.clipclapGgufName(allocator, "clap", @tagName(quantization));
    defer allocator.free(clap_name);
    const clip_path = try std.fs.path.join(allocator, &.{ bundle_dir, clip_name });
    defer allocator.free(clip_path);
    const clap_path = try std.fs.path.join(allocator, &.{ bundle_dir, clap_name });
    defer allocator.free(clap_path);

    var clip_quantized: usize = 0;
    var clip_transposed: usize = 0;
    for (plans.clip.plan.tensors) |tensor| {
        if (tensor.quantization != .none) clip_quantized += 1;
        if (tensor.transform == .transpose_2d_dense) clip_transposed += 1;
    }
    var clap_quantized: usize = 0;
    var clap_transposed: usize = 0;
    for (plans.clap.plan.tensors) |tensor| {
        if (tensor.quantization != .none) clap_quantized += 1;
        if (tensor.transform == .transpose_2d_dense) clap_transposed += 1;
    }

    var text: std.Io.Writer.Allocating = .init(allocator);
    defer text.deinit();
    const writer = &text.writer;
    try writer.writeAll(
        \\export dry-run target=gguf
        \\
    );
    try writer.print("source: {s}\n", .{model_dir});
    try writer.writeAll("mode: clipclap split bundle\n");
    try writer.print("output: {s} (not written)\n", .{bundle_dir});
    try writer.print("clip gguf: {s} (not written)\n", .{clip_path});
    try writer.print("clap gguf: {s} (not written)\n", .{clap_path});
    try writer.print("quantization: {s}\n", .{@tagName(quantization)});
    try writer.print("filters: include={s}, exclude={s}\n", .{
        filter.include_prefixes_csv orelse "none",
        filter.exclude_prefixes_csv orelse "none",
    });
    try writer.print("clip tensors: {d}, {d} quantized, {d} transposed\n", .{ plans.clip.plan.tensors.len, clip_quantized, clip_transposed });
    try writer.print("clap tensors: {d}, {d} quantized, {d} transposed\n", .{ plans.clap.plan.tensors.len, clap_quantized, clap_transposed });
    try writer.writeAll("notes: text/image projection weights are bundled into the clip GGUF; audio projection weights are bundled into the clap GGUF; default F32 GGUF names are unsuffixed and quantized variants use a format suffix\n");
    return text.toOwnedSlice();
}

fn preferClipclapDefaultOnnxInputs(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: *manifest_mod.ModelManifest,
) !void {
    if (!hasDefaultClipclapOnnxInputs(allocator, model_dir)) return;

    replaceOptionalPath(allocator, &manifest.gguf_path, null);
    replaceOptionalPath(allocator, &manifest.gguf_projector_path, null);
    try replaceOptionalPathJoined(allocator, &manifest.onnx_path, model_dir, "text_model.onnx");
    try replaceOptionalPathJoined(allocator, &manifest.visual_model_path, model_dir, "visual_model.onnx");
    try replaceOptionalPathJoined(allocator, &manifest.audio_model_path, model_dir, "audio_model.onnx");
    try replaceOptionalPathJoined(allocator, &manifest.text_projection_path, model_dir, "text_projection.onnx");
    try replaceOptionalPathJoined(allocator, &manifest.visual_projection_path, model_dir, "visual_projection.onnx");
    try replaceOptionalPathJoined(allocator, &manifest.audio_projection_path, model_dir, "audio_projection.onnx");
}

fn hasDefaultClipclapOnnxInputs(allocator: std.mem.Allocator, model_dir: []const u8) bool {
    const required = [_][]const u8{
        "text_model.onnx",
        "visual_model.onnx",
        "audio_model.onnx",
        "text_projection.onnx",
        "visual_projection.onnx",
        "audio_projection.onnx",
    };
    for (required) |name| {
        if (!c_file.fileExistsInDir(allocator, model_dir, name)) return false;
    }
    return true;
}

fn replaceOptionalPath(allocator: std.mem.Allocator, slot: *?[]const u8, value: ?[]const u8) void {
    if (slot.*) |old| allocator.free(old);
    slot.* = value;
}

fn replaceOptionalPathJoined(
    allocator: std.mem.Allocator,
    slot: *?[]const u8,
    model_dir: []const u8,
    file_name: []const u8,
) !void {
    const path = try std.fs.path.join(allocator, &.{ model_dir, file_name });
    replaceOptionalPath(allocator, slot, path);
}

fn exportClipclapBundleToGguf(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle_dir: []const u8,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
) !void {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    try preferClipclapDefaultOnnxInputs(allocator, model_dir, &manifest);

    var access = try tensor_access_mod.openFromManifest(allocator, manifest);
    defer access.deinit();

    var plans = try buildClipclapBundlePlans(allocator, model_dir, manifest, access, quantization, filter);
    defer plans.deinit(allocator);

    try compat.cwd().createDirPath(io_compat(), bundle_dir);

    const clip_name = try variants_manifest.clipclapGgufName(allocator, "clip", @tagName(quantization));
    defer allocator.free(clip_name);
    const clap_name = try variants_manifest.clipclapGgufName(allocator, "clap", @tagName(quantization));
    defer allocator.free(clap_name);
    const clip_path = try std.fs.path.join(allocator, &.{ bundle_dir, clip_name });
    defer allocator.free(clip_path);
    const clap_path = try std.fs.path.join(allocator, &.{ bundle_dir, clap_name });
    defer allocator.free(clap_path);

    try writeSinglePlanExport(allocator, clip_path, access, &plans.clip.plan);
    try writeSinglePlanExport(allocator, clap_path, access, &plans.clap.plan);
    try copyClipclapBundleAssets(allocator, model_dir, bundle_dir);
    if (quantization == .none) {
        try writeClipclapBundleMarker(allocator, bundle_dir, clip_name, clap_name);
    }
    try variants_manifest.writeClipclapVariantsManifest(allocator, compat.io(), bundle_dir);
}

fn copyClipclapBundleAssets(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    bundle_dir: []const u8,
) !void {
    for (clipclap_required_sidecars) |file_name| {
        try copyRequiredClipclapSidecar(allocator, model_dir, bundle_dir, file_name);
    }
    for (clipclap_optional_sidecars) |file_name| {
        try copySidecarIfExists(allocator, model_dir, bundle_dir, file_name);
    }
}

fn copyRequiredClipclapSidecar(
    allocator: std.mem.Allocator,
    source_dir: []const u8,
    out_dir: []const u8,
    file_name: []const u8,
) !void {
    if (!c_file.fileExistsInDir(allocator, source_dir, file_name)) return error.MissingRequiredClipclapSidecar;
    try copySidecarIfExists(allocator, source_dir, out_dir, file_name);
}

fn writeClipclapBundleMarker(
    allocator: std.mem.Allocator,
    bundle_dir: []const u8,
    clip_name: []const u8,
    clap_name: []const u8,
) !void {
    const bundle_marker_path = try std.fs.path.join(allocator, &.{ bundle_dir, "antfly_inference_bundle.json" });
    defer allocator.free(bundle_marker_path);
    const marker_json = try std.json.Stringify.valueAlloc(allocator, .{
        .family = "clipclap_gguf_bundle/v1",
        .clip = clip_name,
        .clap = clap_name,
        .required_sidecars = &clipclap_required_sidecars,
        .optional_sidecars = &clipclap_optional_sidecars,
        .tasks = &[_][]const u8{"embed"},
        .inputs = &[_][]const u8{ "text", "image", "audio" },
        .projections_embedded = true,
    }, .{});
    defer allocator.free(marker_json);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = bundle_marker_path, .data = marker_json });
}

fn formatGlinerBundleDryRunReport(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    output_path: []const u8,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
) ![]u8 {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    var access = try tensor_access_mod.openFromManifest(allocator, manifest);
    defer access.deinit();

    const names = try access.listNames(allocator);
    defer allocator.free(names);

    var encoder_count: usize = 0;
    var head_count: usize = 0;
    for (names) |name| {
        if (isGlinerEncoderTensorName(name)) encoder_count += 1 else head_count += 1;
    }

    const head_path = try defaultGlinerHeadOutputPath(allocator, output_path);
    defer allocator.free(head_path);

    var text: std.Io.Writer.Allocating = .init(allocator);
    defer text.deinit();
    const writer = &text.writer;
    try writer.writeAll(
        \\export dry-run target=gguf
        \\
    );
    try writer.print("source: {s}\n", .{model_dir});
    try writer.writeAll("mode: gliner2 split bundle\n");
    try writer.print("output: {s} (not written)\n", .{output_path});
    try writer.print("head sidecar: {s} (not written)\n", .{head_path});
    try writer.print("quantization: {s}\n", .{@tagName(quantization)});
    try writer.print("filters: include={s}, exclude={s}\n", .{
        filter.include_prefixes_csv orelse "none",
        filter.exclude_prefixes_csv orelse "none",
    });
    try writer.print("wrapper: {s}\n", .{if (manifest.gliner_model_type.len > 0) manifest.gliner_model_type else "extractor"});
    try writer.print("encoder tensors: {d}\n", .{encoder_count});
    try writer.print("head tensors: {d}\n", .{head_count});
    try writer.writeAll("notes: writes a DeBERTa encoder GGUF plus gliner_head.gguf and copies GLiNER runtime sidecars into the output directory\n");
    return text.toOwnedSlice();
}

fn exportGlinerBundleToGguf(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    output_path: []const u8,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
) !void {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    var access = try tensor_access_mod.openFromManifest(allocator, manifest);
    defer access.deinit();

    try writeGlinerEncoderGguf(allocator, model_dir, output_path, manifest, access, quantization, filter);
    const head_output_path = try defaultGlinerHeadOutputPath(allocator, output_path);
    defer allocator.free(head_output_path);
    try writeGlinerHeadGguf(allocator, head_output_path, access, quantization, filter);
    try copyGlinerBundleAssets(allocator, model_dir, output_path);
}

fn defaultGlinerHeadOutputPath(allocator: std.mem.Allocator, output_path: []const u8) ![]u8 {
    const parent = std.fs.path.dirname(output_path) orelse ".";
    return std.fs.path.join(allocator, &.{ parent, "gliner_head.gguf" });
}

fn isGlinerEncoderTensorName(name: []const u8) bool {
    var trimmed = name;
    if (std.mem.startsWith(u8, trimmed, "deberta.")) trimmed = trimmed["deberta.".len..];
    if (std.mem.startsWith(u8, trimmed, "encoder.")) trimmed = trimmed["encoder.".len..];
    return std.mem.startsWith(u8, trimmed, "embeddings.") or std.mem.startsWith(u8, trimmed, "encoder.");
}

fn mapGlinerEncoderTensorNameToDebertaGguf(allocator: std.mem.Allocator, source_name: []const u8) !OutputName {
    var name = source_name;
    if (std.mem.startsWith(u8, name, "deberta.")) name = name["deberta.".len..];
    if (std.mem.startsWith(u8, name, "encoder.")) name = name["encoder.".len..];
    return mapDenseTensorNameToDebertaGguf(allocator, name);
}

fn writeGlinerEncoderGguf(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    output_path: []const u8,
    manifest: manifest_mod.ModelManifest,
    access: tensor_access_mod.TensorAccess,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
) !void {
    const config_bytes = try c_file.readFileFromDir(allocator, model_dir, "config.json");
    defer allocator.free(config_bytes);
    const config = try deberta_mod.parseConfig(allocator, config_bytes);

    const names = try access.listNames(allocator);
    defer allocator.free(names);
    const source_is_onnx = tensor_access_mod.isOnnxInitializerAccess(access);

    var tensors = std.ArrayListUnmanaged(TensorPlan).empty;
    defer {
        for (tensors.items) |*tensor| tensor.deinit(allocator);
        tensors.deinit(allocator);
    }

    for (names) |name| {
        if (!isGlinerEncoderTensorName(name)) continue;
        var record = try access.getRecord(allocator, name);
        defer record.deinit();

        const output_name_result = try mapGlinerEncoderTensorNameToDebertaGguf(allocator, record.descriptor.name);
        defer if (output_name_result.owned) allocator.free(output_name_result.name);

        const transform = if (source_is_onnx)
            glinerOnnxLinearWeightTransform(record.descriptor.name, record.descriptor.shape)
        else
            .none;
        const dimensions = try dimsForTransform(allocator, record.descriptor.shape, transform);
        errdefer allocator.free(dimensions);
        const tensor_quantization = supportedQuantizationForDescriptor(false, quantization, record.descriptor, transform);
        const filtered_quantization = if (quantizationFilterMatches(filter, record.descriptor.name, output_name_result.name))
            tensor_quantization
        else
            .none;
        const tensor_type = if (filtered_quantization != .none)
            tensorTypeForQuantizationMode(filtered_quantization)
        else
            try denseTensorType(record.descriptor.encoding.dense);

        try tensors.append(allocator, .{
            .source_name = try allocator.dupe(u8, record.descriptor.name),
            .output_name = try allocator.dupe(u8, output_name_result.name),
            .dimensions = dimensions,
            .tensor_type = tensor_type,
            .quantization = filtered_quantization,
            .transform = transform,
        });
    }

    if (tensors.items.len == 0) return error.NoExportableTensors;

    const metadata = try buildDebertaMetadataEntries(allocator, manifest, config, model_dir);
    defer {
        for (metadata) |*entry| entry.deinit(allocator);
        allocator.free(metadata);
    }

    const tensor_specs = try allocator.alloc(gguf_mod.writer.TensorSpec, tensors.items.len);
    defer allocator.free(tensor_specs);
    for (tensors.items, 0..) |tensor, i| {
        tensor_specs[i] = .{
            .name = tensor.output_name,
            .dimensions = tensor.dimensions,
            .tensor_type = tensor.tensor_type,
        };
    }
    var layout = try gguf_mod.writer.buildLayout(allocator, metadata, tensor_specs);
    defer layout.deinit(allocator);
    try writeExportFile(allocator, output_path, layout, access, tensors.items);
}

fn buildBertPlannedExport(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
    access: tensor_access_mod.TensorAccess,
    config: bert_mod.Config,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
    source_metadata_file: ?*const gguf_mod.format.File,
    source_kind: ExportSourceKind,
) !PlannedExport {
    const source_is_gguf = source_kind == .gguf;
    const names = try access.listNames(allocator);
    defer allocator.free(names);
    std.mem.sort([]const u8, names, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    var tensors = std.ArrayListUnmanaged(TensorPlan).empty;
    errdefer {
        for (tensors.items) |*tensor| tensor.deinit(allocator);
        tensors.deinit(allocator);
    }

    for (names) |name| {
        var record = try access.getRecord(allocator, name);
        defer record.deinit();

        const output_name_result = if (source_is_gguf)
            OutputName{ .name = try allocator.dupe(u8, record.descriptor.name), .owned = true }
        else
            try mapDenseTensorNameToBertGguf(allocator, config, record.descriptor.name);
        defer if (output_name_result.owned) allocator.free(output_name_result.name);

        const dimensions = if (source_is_gguf)
            try reversedDimsFromShape(allocator, record.descriptor.shape)
        else
            try reversedDimsFromShape(allocator, record.descriptor.shape);
        errdefer allocator.free(dimensions);

        const tensor_quantization = if (source_is_gguf)
            .none
        else
            supportedQuantizationForDescriptor(source_is_gguf, quantization, record.descriptor, .none);
        const filtered_quantization = if (!source_is_gguf and quantizationFilterMatches(filter, record.descriptor.name, output_name_result.name))
            tensor_quantization
        else
            QuantizationMode.none;
        const tensor_type = if (source_is_gguf)
            switch (record.descriptor.encoding) {
                .gguf => |value| value,
                else => unreachable,
            }
        else if (filtered_quantization != .none)
            tensorTypeForQuantizationMode(filtered_quantization)
        else
            try denseTensorType(record.descriptor.encoding.dense);

        try tensors.append(allocator, .{
            .source_name = try allocator.dupe(u8, record.descriptor.name),
            .output_name = try allocator.dupe(u8, output_name_result.name),
            .dimensions = dimensions,
            .tensor_type = tensor_type,
            .quantization = filtered_quantization,
            .transform = .none,
        });
    }

    if (tensors.items.len == 0) return error.NoExportableTensors;

    var plan = ExportPlan{
        .metadata_owned = if (source_is_gguf)
            try cloneMetadataEntries(allocator, source_metadata_file.?.metadata)
        else
            try buildBertMetadataEntries(allocator, manifest, config, model_dir),
        .borrowed_metadata_file = null,
        .tensors = try tensors.toOwnedSlice(allocator),
    };
    errdefer plan.deinit(allocator);

    return .{
        .source_kind = source_kind,
        .plan = plan,
    };
}

fn writeGlinerHeadGguf(
    allocator: std.mem.Allocator,
    output_path: []const u8,
    access: tensor_access_mod.TensorAccess,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
) !void {
    const names = try access.listNames(allocator);
    defer allocator.free(names);
    const source_is_onnx = tensor_access_mod.isOnnxInitializerAccess(access);

    var head_tensors = std.ArrayListUnmanaged(TensorPlan).empty;
    defer {
        for (head_tensors.items) |*tensor| tensor.deinit(allocator);
        head_tensors.deinit(allocator);
    }

    for (names) |name| {
        if (isGlinerEncoderTensorName(name)) continue;
        var record = try access.getRecord(allocator, name);
        defer record.deinit();

        if (try appendGlinerSpecialHeadTensorPlans(allocator, &head_tensors, record, source_is_onnx)) continue;

        const transform = glinerHeadTransform(record.descriptor.name, record.descriptor.shape, source_is_onnx);
        const dimensions = try glinerHeadDimsForRecord(allocator, record.descriptor.name, record.descriptor.shape, transform);
        errdefer allocator.free(dimensions);
        const tensor_quantization = supportedQuantizationForDescriptor(false, quantization, record.descriptor, transform);
        const filtered_quantization = if (quantizationFilterMatches(filter, record.descriptor.name, record.descriptor.name))
            tensor_quantization
        else
            .none;
        const tensor_type = if (filtered_quantization != .none)
            tensorTypeForQuantizationMode(filtered_quantization)
        else
            try denseTensorType(record.descriptor.encoding.dense);
        try head_tensors.append(allocator, .{
            .source_name = try allocator.dupe(u8, record.descriptor.name),
            .output_name = try allocator.dupe(u8, record.descriptor.name),
            .dimensions = dimensions,
            .tensor_type = tensor_type,
            .quantization = filtered_quantization,
            .transform = transform,
        });
    }

    if (head_tensors.items.len == 0) return error.NoExportableTensors;

    const metadata = try buildGlinerHeadMetadataEntries(allocator);
    defer {
        for (metadata) |*entry| entry.deinit(allocator);
        allocator.free(metadata);
    }

    const tensor_specs = try allocator.alloc(gguf_mod.writer.TensorSpec, head_tensors.items.len);
    defer allocator.free(tensor_specs);
    for (head_tensors.items, 0..) |tensor, i| {
        tensor_specs[i] = .{
            .name = tensor.output_name,
            .dimensions = tensor.dimensions,
            .tensor_type = tensor.tensor_type,
        };
    }
    var layout = try gguf_mod.writer.buildLayout(allocator, metadata, tensor_specs);
    defer layout.deinit(allocator);
    try writeExportFile(allocator, output_path, layout, access, head_tensors.items);
}

fn buildGlinerHeadMetadataEntries(allocator: std.mem.Allocator) ![]gguf_mod.format.MetadataEntry {
    var entries = std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry).empty;
    errdefer {
        for (entries.items) |*entry| entry.deinit(allocator);
        entries.deinit(allocator);
    }
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "general.architecture"),
        .value = .{ .string = try allocator.dupe(u8, "antfly-gliner-head") },
    });
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "general.alignment"),
        .value = .{ .u32 = 32 },
    });
    return try entries.toOwnedSlice(allocator);
}

fn appendGlinerSpecialHeadTensorPlans(
    allocator: std.mem.Allocator,
    tensors: *std.ArrayListUnmanaged(TensorPlan),
    record: tensor_access_mod.Record,
    source_is_onnx: bool,
) !bool {
    if (!std.mem.eql(u8, record.descriptor.name, "count_embed.gru.bias")) return false;
    const dtype = switch (record.descriptor.encoding) {
        .dense => |value| value,
        else => return error.UnsupportedDenseTensorTypeForGgufExport,
    };
    const elem_size = denseElementSize(dtype);
    if (elem_size == 0) return error.UnsupportedDenseTensorTypeForGgufExport;
    if (record.descriptor.shape.len == 0) return error.InvalidTensorShape;
    const last_dim = record.descriptor.shape[record.descriptor.shape.len - 1];
    if (last_dim <= 0 or @mod(last_dim, 2) != 0) return error.InvalidTensorShape;

    const split_elements: usize = @intCast(@divExact(last_dim, 2));
    const split_bytes = split_elements * elem_size;
    if (record.descriptor.byte_len < split_bytes * 2) return error.InvalidTensorShape;

    const transform: TensorTransform = if (source_is_onnx) .onnx_gru_zrh_to_rzn else .none;
    try appendGlinerGruBiasTensorPlan(allocator, tensors, record.descriptor.name, "count_embed.gru.bias_ih_l0", split_elements, split_bytes, 0, dtype, transform);
    try appendGlinerGruBiasTensorPlan(allocator, tensors, record.descriptor.name, "count_embed.gru.bias_hh_l0", split_elements, split_bytes, split_bytes, dtype, transform);
    return true;
}

fn appendGlinerGruBiasTensorPlan(
    allocator: std.mem.Allocator,
    tensors: *std.ArrayListUnmanaged(TensorPlan),
    source_name: []const u8,
    output_name: []const u8,
    split_elements: usize,
    split_bytes: usize,
    start: usize,
    dtype: @import("backends/tensor.zig").DType,
    transform: TensorTransform,
) !void {
    const dimensions = try allocator.alloc(u64, 1);
    errdefer allocator.free(dimensions);
    dimensions[0] = @intCast(split_elements);

    const source_name_owned = try allocator.dupe(u8, source_name);
    errdefer allocator.free(source_name_owned);
    const output_name_owned = try allocator.dupe(u8, output_name);
    errdefer allocator.free(output_name_owned);

    try tensors.append(allocator, .{
        .source_name = source_name_owned,
        .output_name = output_name_owned,
        .dimensions = dimensions,
        .tensor_type = try denseTensorType(dtype),
        .quantization = .none,
        .transform = transform,
        .source_byte_range = .{ .start = start, .len = split_bytes },
    });
}

fn glinerHeadDimsForRecord(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    transform: TensorTransform,
) ![]u64 {
    if (glinerHeadDropsLeadingSingletonDim(name, shape)) {
        return reversedDimsFromShape(allocator, shape[1..]);
    }
    return dimsForTransform(allocator, shape, transform);
}

fn glinerHeadDropsLeadingSingletonDim(name: []const u8, shape: []const i64) bool {
    if (shape.len != 3 or shape[0] != 1) return false;
    return std.mem.eql(u8, name, "count_embed.gru.weight_ih_l0") or
        std.mem.eql(u8, name, "count_embed.gru.weight_hh_l0");
}

fn dimsForTransform(allocator: std.mem.Allocator, shape: []const i64, transform: TensorTransform) ![]u64 {
    return switch (transform) {
        .none => reversedDimsFromShape(allocator, shape),
        .transpose_2d_dense => dimsFromShape(allocator, shape),
        .onnx_gru_zrh_to_rzn => reversedDimsFromShape(allocator, shape),
    };
}

fn glinerHeadTransform(name: []const u8, shape: []const i64, source_is_onnx: bool) TensorTransform {
    if (!source_is_onnx) return .none;
    if (isGlinerOnnxGruGateTensorName(name, shape)) return .onnx_gru_zrh_to_rzn;
    return glinerOnnxLinearWeightTransform(name, shape);
}

fn glinerOnnxLinearWeightTransform(name: []const u8, shape: []const i64) TensorTransform {
    if (shape.len != 2) return .none;
    if (isGlinerEncoderOnnxLinearWeightName(name) or isGlinerHeadOnnxLinearWeightName(name)) return .transpose_2d_dense;
    return .none;
}

fn isGlinerOnnxGruGateTensorName(name: []const u8, shape: []const i64) bool {
    if (shape.len != 3 or shape[0] != 1) return false;
    return std.mem.eql(u8, name, "count_embed.gru.weight_ih_l0") or
        std.mem.eql(u8, name, "count_embed.gru.weight_hh_l0");
}

fn isGlinerEncoderOnnxLinearWeightName(name: []const u8) bool {
    var trimmed = name;
    if (std.mem.startsWith(u8, trimmed, "deberta.")) trimmed = trimmed["deberta.".len..];
    if (std.mem.startsWith(u8, trimmed, "encoder.")) trimmed = trimmed["encoder.".len..];
    return std.mem.startsWith(u8, trimmed, "encoder.layer.") and std.mem.endsWith(u8, trimmed, ".weight");
}

fn isGlinerHeadOnnxLinearWeightName(name: []const u8) bool {
    if (std.mem.startsWith(u8, name, "span_rep.") and std.mem.endsWith(u8, name, ".weight")) return true;
    if (!std.mem.startsWith(u8, name, "count_embed.transformer.")) return false;
    if (std.mem.indexOf(u8, name, ".norm") != null) return false;
    return std.mem.endsWith(u8, name, ".weight") or std.mem.endsWith(u8, name, "_weight");
}

fn copyGlinerBundleAssets(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    output_path: []const u8,
) !void {
    const io = io_compat();
    const out_dir = std.fs.path.dirname(output_path) orelse ".";
    if (out_dir.len > 0) try compat.cwd().createDirPath(io, out_dir);
    const asset_names = [_][]const u8{
        "config.json",
        "gliner_config.json",
        "added_tokens.json",
        "model_manifest.json",
        "tokenizer.json",
        "tokenizer.model",
        "tokenizer_config.json",
        "special_tokens_map.json",
        "chat_template.jinja",
        "vocab.txt",
        "vocab.json",
        "merges.txt",
    };
    for (asset_names) |asset_name| {
        const bytes = c_file.readFileFromDir(allocator, model_dir, asset_name) catch continue;
        defer allocator.free(bytes);
        const target = try std.fs.path.join(allocator, &.{ out_dir, asset_name });
        defer allocator.free(target);
        try compat.cwd().writeFile(io, .{ .sub_path = target, .data = bytes });
    }
    try writeGlinerBundleMarker(allocator, out_dir, output_path);
}

fn writeGlinerBundleMarker(allocator: std.mem.Allocator, out_dir: []const u8, output_path: []const u8) !void {
    const marker_path = try std.fs.path.join(allocator, &.{ out_dir, "antfly_inference_bundle.json" });
    defer allocator.free(marker_path);
    const encoder_name = std.fs.path.basename(output_path);
    const marker_bytes = try std.json.Stringify.valueAlloc(allocator, .{
        .family = "gliner2_split_bundle/v1",
        .wrapper = "gliner2",
        .encoder = encoder_name,
        .head = "gliner_head.gguf",
    }, .{});
    defer allocator.free(marker_bytes);
    try compat.cwd().writeFile(io_compat(), .{
        .sub_path = marker_path,
        .data = marker_bytes,
    });
}

fn buildPlannedExport(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    quantization: QuantizationMode,
    requested_projector_format: ProjectorFormat,
    filter: QuantizationFilter,
) !PlannedExport {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    var parsed_source_gguf: ?SourceGguf = null;
    defer if (parsed_source_gguf) |*source| source.deinit();

    const export_arch = try resolveExportArchitecture(allocator, model_dir, manifest, &parsed_source_gguf);
    var access = try tensor_access_mod.openFromManifest(allocator, manifest);
    defer access.deinit();
    const source_metadata_file = if (parsed_source_gguf) |*source| &source.parsed else null;
    const source_kind: ExportSourceKind = if (manifest.gguf_path != null and manifest.safetensors_path == null and manifest.safetensors_index_path == null) .gguf else .dense;

    if (export_arch == .bert) {
        var plan = try buildBertPlannedExport(
            allocator,
            model_dir,
            manifest,
            access,
            export_arch.bert,
            quantization,
            filter,
            source_metadata_file,
            source_kind,
        );
        errdefer plan.deinit(allocator);
        return plan;
    }
    if (export_arch == .t5) {
        var plan = try buildT5PlannedExport(
            allocator,
            model_dir,
            manifest,
            access,
            export_arch.t5,
            quantization,
            filter,
            source_metadata_file,
            source_kind,
        );
        errdefer plan.deinit(allocator);
        return plan;
    }
    if (export_arch == .whisper) {
        var plan = try buildWhisperPlannedExport(
            allocator,
            model_dir,
            manifest,
            access,
            export_arch.whisper,
            quantization,
            filter,
            source_metadata_file,
            source_kind,
        );
        errdefer plan.deinit(allocator);
        return plan;
    }
    if (export_arch == .deberta) {
        var plan = try buildDebertaPlannedExport(
            allocator,
            model_dir,
            manifest,
            access,
            export_arch.deberta,
            quantization,
            filter,
            source_metadata_file,
            source_kind,
        );
        errdefer plan.deinit(allocator);
        return plan;
    }
    if (export_arch == .layoutlmv3) {
        var plan = try buildLayoutlmv3PlannedExport(
            allocator,
            model_dir,
            manifest,
            access,
            export_arch.layoutlmv3,
            quantization,
            filter,
            source_metadata_file,
            source_kind,
        );
        errdefer plan.deinit(allocator);
        return plan;
    }
    if (export_arch == .florence) {
        var plan = try buildFlorencePlannedExport(
            allocator,
            model_dir,
            manifest,
            access,
            export_arch.florence,
            quantization,
            filter,
            source_metadata_file,
            source_kind,
        );
        errdefer plan.deinit(allocator);
        return plan;
    }
    if (export_arch == .clip) {
        var plan = try buildClipPlannedExport(
            allocator,
            model_dir,
            manifest,
            access,
            export_arch.clip,
            quantization,
            filter,
            source_metadata_file,
            source_kind,
        );
        errdefer plan.deinit(allocator);
        return plan;
    }
    if (export_arch == .clap) {
        var plan = try buildClapPlannedExport(
            allocator,
            model_dir,
            manifest,
            access,
            export_arch.clap,
            quantization,
            filter,
            source_metadata_file,
            source_kind,
        );
        errdefer plan.deinit(allocator);
        return plan;
    }

    const gpt_config = export_arch.gpt;

    if (source_kind == .dense) {
        switch (denseDecoderExportSupport(gpt_config)) {
            .supported => {},
            .unsupported_tensor_transform => return error.UnsupportedDenseArchitectureForGgufExport,
            .unsupported_name_mapping => return error.UnsupportedDenseArchitectureForGgufExport,
        }
    }

    const projector_source_path = if (gpt_config.isMultimodal() and manifest.gguf_projector_path != null)
        try allocator.dupe(u8, manifest.gguf_projector_path.?)
    else
        null;
    errdefer if (projector_source_path) |path| allocator.free(path);

    const resolved_projector_format = try resolveRequestedProjectorFormat(allocator, projector_source_path, requested_projector_format);
    if (requested_projector_format == .clip and projector_source_path == null and gpt_config.isMultimodal()) {
        return error.UnsupportedCanonicalProjectorExport;
    }

    var split_plans = try buildExportPlans(
        allocator,
        model_dir,
        manifest,
        access,
        gpt_config,
        resolved_projector_format,
        quantization,
        filter,
        source_metadata_file,
    );
    errdefer {
        split_plans.decoder.deinit(allocator);
        if (split_plans.projector) |*plan| plan.deinit(allocator);
    }

    if (gpt_config.isMultimodal() and
        !shouldKeepMultimodalAuxInDecoder(manifest, gpt_config) and
        projector_source_path == null and
        split_plans.projector == null)
    {
        return error.UnsupportedMultimodalExport;
    }

    return .{
        .source_kind = source_kind,
        .plan = split_plans.decoder,
        .projector_source_path = projector_source_path,
        .projector_plan = split_plans.projector,
        .projector_format = resolved_projector_format,
    };
}

const SourceGguf = struct {
    allocator: std.mem.Allocator,
    region: c_file.MmapRegion,
    parsed: gguf_mod.format.File,

    fn init(allocator: std.mem.Allocator, path: []const u8) !SourceGguf {
        var region = try c_file.MmapRegion.init(allocator, path);
        errdefer region.deinit();
        var parsed = try gguf_mod.format.parse(allocator, region.data);
        errdefer parsed.deinit(allocator);
        return .{
            .allocator = allocator,
            .region = region,
            .parsed = parsed,
        };
    }

    fn deinit(self: *SourceGguf) void {
        self.parsed.deinit(self.allocator);
        self.region.deinit();
    }
};

const SplitExportPlans = struct {
    decoder: ExportPlan,
    projector: ?ExportPlan = null,
};

fn buildExportPlans(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
    access: tensor_access_mod.TensorAccess,
    gpt_config: gpt_mod.Config,
    resolved_projector_format: ?ResolvedProjectorFormat,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
    source_metadata_file: ?*const gguf_mod.format.File,
) !SplitExportPlans {
    const source_is_gguf = manifest.safetensors_path == null and manifest.safetensors_index_path == null and manifest.gguf_path != null;
    const keep_multimodal_aux_in_decoder = shouldKeepMultimodalAuxInDecoder(manifest, gpt_config);
    const names = try access.listNames(allocator);
    defer allocator.free(names);
    std.mem.sort([]const u8, names, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    var tensors = std.ArrayListUnmanaged(TensorPlan).empty;
    var projector_tensors = std.ArrayListUnmanaged(TensorPlan).empty;
    errdefer {
        for (tensors.items) |*tensor| tensor.deinit(allocator);
        tensors.deinit(allocator);
        for (projector_tensors.items) |*tensor| tensor.deinit(allocator);
        projector_tensors.deinit(allocator);
    }

    for (names) |name| {
        var record = try access.getRecord(allocator, name);
        defer record.deinit();
        const is_projector_tensor = gpt_config.isMultimodal() and !keep_multimodal_aux_in_decoder and isMultimodalAuxTensor(record.descriptor.name);

        if (is_projector_tensor) {
            const dimensions = if (source_is_gguf)
                try reversedDimsFromShape(allocator, record.descriptor.shape)
            else
                try denseExportDimsForTensor(allocator, gpt_config, record.descriptor.name, record.descriptor.shape);
            const owned_source_name = try allocator.dupe(u8, record.descriptor.name);
            errdefer allocator.free(owned_source_name);
            const owned_output_name = try allocator.dupe(u8, record.descriptor.name);
            errdefer allocator.free(owned_output_name);
            const tensor_type = switch (record.descriptor.encoding) {
                .gguf => |value| value,
                .dense => |dtype| try denseTensorType(dtype),
            };
            try projector_tensors.append(allocator, .{
                .source_name = owned_source_name,
                .output_name = owned_output_name,
                .dimensions = dimensions,
                .tensor_type = tensor_type,
                .quantization = .none,
                .transform = denseExportTransformForTensor(gpt_config, record.descriptor.name, record.descriptor.shape),
            });
            continue;
        }

        if (!source_is_gguf and gpt_config.family == .gpt_neox and try appendGptNeoxSplitTensorPlans(allocator, &tensors, record, quantization, filter)) {
            continue;
        }

        const output_name_result = if (source_is_gguf)
            OutputName{ .name = record.descriptor.name, .owned = false }
        else if (keep_multimodal_aux_in_decoder and isMultimodalAuxTensor(record.descriptor.name))
            OutputName{ .name = try allocator.dupe(u8, record.descriptor.name), .owned = true }
        else
            try mapDenseTensorNameToGguf(allocator, gpt_config, record.descriptor.name);

        if (gpt_config.weight_tying and std.mem.eql(u8, output_name_result.name, "output.weight")) {
            if (output_name_result.owned) allocator.free(output_name_result.name);
            continue;
        }

        const dimensions = if (source_is_gguf)
            try reversedDimsFromShape(allocator, record.descriptor.shape)
        else
            try denseExportDimsForTensor(allocator, gpt_config, record.descriptor.name, record.descriptor.shape);

        const tensor_quantization = if (quantizationFilterMatches(filter, record.descriptor.name, output_name_result.name))
            supportedQuantizationForTensor(gpt_config, source_is_gguf, quantization, record.descriptor)
        else
            .none;
        const tensor_type = if (tensor_quantization != .none)
            tensorTypeForQuantizationMode(tensor_quantization)
        else switch (record.descriptor.encoding) {
            .gguf => |value| value,
            .dense => |dtype| try denseTensorType(dtype),
        };

        const owned_source_name = try allocator.dupe(u8, record.descriptor.name);
        errdefer allocator.free(owned_source_name);
        const owned_output_name = if (output_name_result.owned)
            output_name_result.name
        else
            try allocator.dupe(u8, output_name_result.name);
        errdefer allocator.free(owned_output_name);

        try tensors.append(allocator, .{
            .source_name = owned_source_name,
            .output_name = owned_output_name,
            .dimensions = dimensions,
            .tensor_type = tensor_type,
            .quantization = tensor_quantization,
            .transform = denseExportTransformForTensor(gpt_config, record.descriptor.name, record.descriptor.shape),
        });
    }

    if (tensors.items.len == 0) return error.NoExportableTensors;

    var decoder_plan: ExportPlan = .{
        .metadata_owned = if (source_is_gguf)
            try cloneMetadataEntries(allocator, source_metadata_file.?.metadata)
        else
            try buildMetadataEntries(allocator, manifest, gpt_config, model_dir),
        .borrowed_metadata_file = null,
        .tensors = try tensors.toOwnedSlice(allocator),
    };
    errdefer decoder_plan.deinit(allocator);

    const projector_plan = if (projector_tensors.items.len > 0 and manifest.gguf_projector_path == null)
        ExportPlan{
            .metadata_owned = try buildProjectorMetadataEntries(allocator, manifest, gpt_config, resolved_projector_format orelse .antfly),
            .borrowed_metadata_file = null,
            .tensors = try projector_tensors.toOwnedSlice(allocator),
        }
    else blk: {
        for (projector_tensors.items) |*tensor| tensor.deinit(allocator);
        projector_tensors.deinit(allocator);
        break :blk null;
    };

    return .{
        .decoder = decoder_plan,
        .projector = projector_plan,
    };
}

fn shouldKeepMultimodalAuxInDecoder(manifest: manifest_mod.ModelManifest, gpt_config: gpt_mod.Config) bool {
    return gpt_config.family == .qwen2 and gpt_config.isMultimodal() and isColqwenBundleExportManifest(manifest);
}

fn buildDebertaPlannedExport(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
    access: tensor_access_mod.TensorAccess,
    config: deberta_mod.Config,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
    source_metadata_file: ?*const gguf_mod.format.File,
    source_kind: ExportSourceKind,
) !PlannedExport {
    const source_is_gguf = source_kind == .gguf;
    const names = try access.listNames(allocator);
    defer allocator.free(names);
    std.mem.sort([]const u8, names, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    var tensors = std.ArrayListUnmanaged(TensorPlan).empty;
    errdefer {
        for (tensors.items) |*tensor| tensor.deinit(allocator);
        tensors.deinit(allocator);
    }

    for (names) |name| {
        if (isMultimodalAuxTensor(name)) continue;
        var record = try access.getRecord(allocator, name);
        defer record.deinit();

        const output_name_result = if (source_is_gguf)
            OutputName{ .name = try allocator.dupe(u8, record.descriptor.name), .owned = true }
        else
            try mapDenseTensorNameToDebertaGguf(allocator, record.descriptor.name);
        defer if (output_name_result.owned) allocator.free(output_name_result.name);

        const dimensions = if (source_is_gguf)
            try reversedDimsFromShape(allocator, record.descriptor.shape)
        else
            try reversedDimsFromShape(allocator, record.descriptor.shape);
        errdefer allocator.free(dimensions);

        const tensor_quantization = if (source_is_gguf)
            .none
        else
            supportedQuantizationForDescriptor(source_is_gguf, quantization, record.descriptor, .none);
        const filtered_quantization = if (!source_is_gguf and quantizationFilterMatches(filter, record.descriptor.name, output_name_result.name))
            tensor_quantization
        else
            QuantizationMode.none;
        const tensor_type = if (source_is_gguf)
            switch (record.descriptor.encoding) {
                .gguf => |value| value,
                else => unreachable,
            }
        else if (filtered_quantization != .none)
            tensorTypeForQuantizationMode(filtered_quantization)
        else
            try denseTensorType(record.descriptor.encoding.dense);

        try tensors.append(allocator, .{
            .source_name = try allocator.dupe(u8, record.descriptor.name),
            .output_name = try allocator.dupe(u8, output_name_result.name),
            .dimensions = dimensions,
            .tensor_type = tensor_type,
            .quantization = filtered_quantization,
            .transform = .none,
        });
    }

    if (tensors.items.len == 0) return error.NoExportableTensors;

    var plan = ExportPlan{
        .metadata_owned = if (source_is_gguf)
            try cloneMetadataEntries(allocator, source_metadata_file.?.metadata)
        else
            try buildDebertaMetadataEntries(allocator, manifest, config, model_dir),
        .borrowed_metadata_file = null,
        .tensors = try tensors.toOwnedSlice(allocator),
    };
    errdefer plan.deinit(allocator);

    return .{
        .source_kind = source_kind,
        .plan = plan,
    };
}

fn buildT5PlannedExport(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
    access: tensor_access_mod.TensorAccess,
    config: t5_mod.Config,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
    source_metadata_file: ?*const gguf_mod.format.File,
    source_kind: ExportSourceKind,
) !PlannedExport {
    const source_is_gguf = source_kind == .gguf;
    const names = try access.listNames(allocator);
    defer allocator.free(names);
    std.mem.sort([]const u8, names, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    var tensors = std.ArrayListUnmanaged(TensorPlan).empty;
    errdefer {
        for (tensors.items) |*tensor| tensor.deinit(allocator);
        tensors.deinit(allocator);
    }

    for (names) |name| {
        var record = try access.getRecord(allocator, name);
        defer record.deinit();

        const output_name_result = if (source_is_gguf)
            OutputName{ .name = try allocator.dupe(u8, record.descriptor.name), .owned = true }
        else
            try mapDenseTensorNameToT5Gguf(allocator, record.descriptor.name);
        defer if (output_name_result.owned) allocator.free(output_name_result.name);

        const dimensions = if (source_is_gguf)
            try reversedDimsFromShape(allocator, record.descriptor.shape)
        else
            try reversedDimsFromShape(allocator, record.descriptor.shape);
        errdefer allocator.free(dimensions);

        const tensor_quantization = if (source_is_gguf)
            .none
        else
            supportedQuantizationForDescriptor(source_is_gguf, quantization, record.descriptor, .none);
        const filtered_quantization = if (!source_is_gguf and quantizationFilterMatches(filter, record.descriptor.name, output_name_result.name))
            tensor_quantization
        else
            QuantizationMode.none;
        const tensor_type = if (source_is_gguf)
            switch (record.descriptor.encoding) {
                .gguf => |value| value,
                else => unreachable,
            }
        else if (filtered_quantization != .none)
            tensorTypeForQuantizationMode(filtered_quantization)
        else
            try denseTensorType(record.descriptor.encoding.dense);

        try tensors.append(allocator, .{
            .source_name = try allocator.dupe(u8, record.descriptor.name),
            .output_name = try allocator.dupe(u8, output_name_result.name),
            .dimensions = dimensions,
            .tensor_type = tensor_type,
            .quantization = filtered_quantization,
            .transform = .none,
        });
    }

    if (tensors.items.len == 0) return error.NoExportableTensors;

    var plan = ExportPlan{
        .metadata_owned = if (source_is_gguf)
            try cloneMetadataEntries(allocator, source_metadata_file.?.metadata)
        else
            try buildT5MetadataEntries(allocator, manifest, config, model_dir),
        .borrowed_metadata_file = null,
        .tensors = try tensors.toOwnedSlice(allocator),
    };
    errdefer plan.deinit(allocator);

    return .{
        .source_kind = source_kind,
        .plan = plan,
    };
}

fn buildWhisperPlannedExport(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
    access: tensor_access_mod.TensorAccess,
    config: whisper_mod.Config,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
    source_metadata_file: ?*const gguf_mod.format.File,
    source_kind: ExportSourceKind,
) !PlannedExport {
    const source_is_gguf = source_kind == .gguf;
    const names = try access.listNames(allocator);
    defer allocator.free(names);
    std.mem.sort([]const u8, names, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    var tensors = std.ArrayListUnmanaged(TensorPlan).empty;
    errdefer {
        for (tensors.items) |*tensor| tensor.deinit(allocator);
        tensors.deinit(allocator);
    }

    for (names) |name| {
        var record = try access.getRecord(allocator, name);
        defer record.deinit();

        const output_name_result = if (source_is_gguf)
            OutputName{ .name = try allocator.dupe(u8, record.descriptor.name), .owned = true }
        else
            try mapDenseTensorNameToWhisperGguf(allocator, record.descriptor.name);
        defer if (output_name_result.owned) allocator.free(output_name_result.name);

        const dimensions = if (source_is_gguf)
            try reversedDimsFromShape(allocator, record.descriptor.shape)
        else
            try reversedDimsFromShape(allocator, record.descriptor.shape);
        errdefer allocator.free(dimensions);

        const tensor_quantization = if (source_is_gguf)
            .none
        else
            supportedQuantizationForDescriptor(source_is_gguf, quantization, record.descriptor, .none);
        const filtered_quantization = if (!source_is_gguf and quantizationFilterMatches(filter, record.descriptor.name, output_name_result.name))
            tensor_quantization
        else
            QuantizationMode.none;
        const tensor_type = if (source_is_gguf)
            switch (record.descriptor.encoding) {
                .gguf => |value| value,
                else => unreachable,
            }
        else if (filtered_quantization != .none)
            tensorTypeForQuantizationMode(filtered_quantization)
        else
            try denseTensorType(record.descriptor.encoding.dense);

        try tensors.append(allocator, .{
            .source_name = try allocator.dupe(u8, record.descriptor.name),
            .output_name = try allocator.dupe(u8, output_name_result.name),
            .dimensions = dimensions,
            .tensor_type = tensor_type,
            .quantization = filtered_quantization,
            .transform = .none,
        });
    }

    if (tensors.items.len == 0) return error.NoExportableTensors;

    var plan = ExportPlan{
        .metadata_owned = if (source_is_gguf)
            try cloneMetadataEntries(allocator, source_metadata_file.?.metadata)
        else
            try buildWhisperMetadataEntries(allocator, manifest, config, model_dir),
        .borrowed_metadata_file = null,
        .tensors = try tensors.toOwnedSlice(allocator),
    };
    errdefer plan.deinit(allocator);

    return .{
        .source_kind = source_kind,
        .plan = plan,
    };
}

fn buildLayoutlmv3PlannedExport(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
    access: tensor_access_mod.TensorAccess,
    config: layoutlmv3_mod.Config,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
    source_metadata_file: ?*const gguf_mod.format.File,
    source_kind: ExportSourceKind,
) !PlannedExport {
    const source_is_gguf = source_kind == .gguf;
    const names = try access.listNames(allocator);
    defer allocator.free(names);
    std.mem.sort([]const u8, names, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    var tensors = std.ArrayListUnmanaged(TensorPlan).empty;
    errdefer {
        for (tensors.items) |*tensor| tensor.deinit(allocator);
        tensors.deinit(allocator);
    }

    for (names) |name| {
        var record = try access.getRecord(allocator, name);
        defer record.deinit();

        const output_name_result = if (source_is_gguf)
            OutputName{ .name = try allocator.dupe(u8, record.descriptor.name), .owned = true }
        else
            try mapDenseTensorNameToLayoutlmv3Gguf(allocator, config, record.descriptor.name);
        defer if (output_name_result.owned) allocator.free(output_name_result.name);

        const dimensions = if (source_is_gguf)
            try reversedDimsFromShape(allocator, record.descriptor.shape)
        else
            try reversedDimsFromShape(allocator, record.descriptor.shape);
        errdefer allocator.free(dimensions);

        const tensor_quantization = if (source_is_gguf)
            .none
        else
            supportedQuantizationForDescriptor(source_is_gguf, quantization, record.descriptor, .none);
        const filtered_quantization = if (!source_is_gguf and quantizationFilterMatches(filter, record.descriptor.name, output_name_result.name))
            tensor_quantization
        else
            QuantizationMode.none;
        const tensor_type = if (source_is_gguf)
            switch (record.descriptor.encoding) {
                .gguf => |value| value,
                else => unreachable,
            }
        else if (filtered_quantization != .none)
            tensorTypeForQuantizationMode(filtered_quantization)
        else
            try denseTensorType(record.descriptor.encoding.dense);

        try tensors.append(allocator, .{
            .source_name = try allocator.dupe(u8, record.descriptor.name),
            .output_name = try allocator.dupe(u8, output_name_result.name),
            .dimensions = dimensions,
            .tensor_type = tensor_type,
            .quantization = filtered_quantization,
            .transform = .none,
        });
    }

    if (tensors.items.len == 0) return error.NoExportableTensors;

    var plan = ExportPlan{
        .metadata_owned = if (source_is_gguf)
            try cloneMetadataEntries(allocator, source_metadata_file.?.metadata)
        else
            try buildLayoutlmv3MetadataEntries(allocator, manifest, config, model_dir),
        .borrowed_metadata_file = null,
        .tensors = try tensors.toOwnedSlice(allocator),
    };
    errdefer plan.deinit(allocator);

    return .{
        .source_kind = source_kind,
        .plan = plan,
    };
}

fn buildFlorencePlannedExport(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
    access: tensor_access_mod.TensorAccess,
    config: florence_mod.Config,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
    source_metadata_file: ?*const gguf_mod.format.File,
    source_kind: ExportSourceKind,
) !PlannedExport {
    const source_is_gguf = source_kind == .gguf;
    const names = try access.listNames(allocator);
    defer allocator.free(names);
    std.mem.sort([]const u8, names, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    var tensors = std.ArrayListUnmanaged(TensorPlan).empty;
    errdefer {
        for (tensors.items) |*tensor| tensor.deinit(allocator);
        tensors.deinit(allocator);
    }

    for (names) |name| {
        var record = try access.getRecord(allocator, name);
        defer record.deinit();

        const output_name_result = if (source_is_gguf)
            OutputName{ .name = try allocator.dupe(u8, record.descriptor.name), .owned = true }
        else
            try mapDenseTensorNameToFlorenceGguf(allocator, record.descriptor.name);
        defer if (output_name_result.owned) allocator.free(output_name_result.name);

        const dimensions = if (source_is_gguf)
            try reversedDimsFromShape(allocator, record.descriptor.shape)
        else
            try reversedDimsFromShape(allocator, record.descriptor.shape);
        errdefer allocator.free(dimensions);

        const tensor_quantization = if (source_is_gguf)
            .none
        else
            supportedQuantizationForDescriptor(source_is_gguf, quantization, record.descriptor, .none);
        const filtered_quantization = if (!source_is_gguf and quantizationFilterMatches(filter, record.descriptor.name, output_name_result.name))
            tensor_quantization
        else
            QuantizationMode.none;
        const tensor_type = if (source_is_gguf)
            switch (record.descriptor.encoding) {
                .gguf => |value| value,
                else => unreachable,
            }
        else if (filtered_quantization != .none)
            tensorTypeForQuantizationMode(filtered_quantization)
        else
            try denseTensorType(record.descriptor.encoding.dense);

        try tensors.append(allocator, .{
            .source_name = try allocator.dupe(u8, record.descriptor.name),
            .output_name = try allocator.dupe(u8, output_name_result.name),
            .dimensions = dimensions,
            .tensor_type = tensor_type,
            .quantization = filtered_quantization,
            .transform = .none,
        });
    }

    if (tensors.items.len == 0) return error.NoExportableTensors;

    var plan = ExportPlan{
        .metadata_owned = if (source_is_gguf)
            try cloneMetadataEntries(allocator, source_metadata_file.?.metadata)
        else
            try buildFlorenceMetadataEntries(allocator, manifest, config, model_dir),
        .borrowed_metadata_file = null,
        .tensors = try tensors.toOwnedSlice(allocator),
    };
    errdefer plan.deinit(allocator);

    return .{
        .source_kind = source_kind,
        .plan = plan,
    };
}

fn buildClipPlannedExport(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
    access: tensor_access_mod.TensorAccess,
    config: clip_mod.Config,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
    source_metadata_file: ?*const gguf_mod.format.File,
    source_kind: ExportSourceKind,
) !PlannedExport {
    return buildClipPlannedExportFiltered(
        allocator,
        model_dir,
        manifest,
        access,
        config,
        quantization,
        filter,
        source_metadata_file,
        source_kind,
        .all,
    );
}

fn buildClipPlannedExportFiltered(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
    access: tensor_access_mod.TensorAccess,
    config: clip_mod.Config,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
    source_metadata_file: ?*const gguf_mod.format.File,
    source_kind: ExportSourceKind,
    name_filter: TensorNameFilter,
) !PlannedExport {
    const source_is_gguf = source_kind == .gguf;
    const names = try access.listNames(allocator);
    defer allocator.free(names);
    std.mem.sort([]const u8, names, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    var tensors = std.ArrayListUnmanaged(TensorPlan).empty;
    errdefer {
        for (tensors.items) |*tensor| tensor.deinit(allocator);
        tensors.deinit(allocator);
    }

    for (names) |name| {
        if (!tensorNameMatchesExportFilter(name_filter, name)) continue;
        var record = try access.getRecord(allocator, name);
        defer record.deinit();
        if (!source_is_gguf and !isExportableDenseRecord(record)) continue;

        const output_name_result = if (source_is_gguf)
            OutputName{ .name = try allocator.dupe(u8, record.descriptor.name), .owned = true }
        else
            try mapDenseTensorNameToClipGguf(allocator, record.descriptor.name);
        defer if (output_name_result.owned) allocator.free(output_name_result.name);

        const transform = if (source_is_gguf)
            TensorTransform.none
        else
            clipClapDenseExportTransformForTensor(name_filter, record.descriptor.name, record.descriptor.shape);
        const dimensions = switch (transform) {
            .none => try reversedDimsFromShape(allocator, record.descriptor.shape),
            .transpose_2d_dense => try dimsFromShape(allocator, record.descriptor.shape),
            .onnx_gru_zrh_to_rzn => return error.UnsupportedDenseArchitectureForGgufExport,
        };
        errdefer allocator.free(dimensions);

        const tensor_quantization = if (source_is_gguf)
            .none
        else
            supportedQuantizationForDescriptor(source_is_gguf, quantization, record.descriptor, transform);
        const filtered_quantization = if (!source_is_gguf and quantizationFilterMatches(filter, record.descriptor.name, output_name_result.name))
            tensor_quantization
        else
            QuantizationMode.none;
        const tensor_type = if (source_is_gguf)
            switch (record.descriptor.encoding) {
                .gguf => |value| value,
                else => unreachable,
            }
        else if (filtered_quantization != .none)
            tensorTypeForQuantizationMode(filtered_quantization)
        else
            try denseTensorType(record.descriptor.encoding.dense);

        try tensors.append(allocator, .{
            .source_name = try allocator.dupe(u8, record.descriptor.name),
            .output_name = try allocator.dupe(u8, output_name_result.name),
            .dimensions = dimensions,
            .tensor_type = tensor_type,
            .quantization = filtered_quantization,
            .transform = transform,
        });
    }

    if (tensors.items.len == 0) return error.NoExportableTensors;

    var plan = ExportPlan{
        .metadata_owned = if (source_is_gguf)
            try cloneMetadataEntries(allocator, source_metadata_file.?.metadata)
        else
            try buildClipMetadataEntries(allocator, manifest, config, model_dir),
        .borrowed_metadata_file = null,
        .tensors = try tensors.toOwnedSlice(allocator),
    };
    errdefer plan.deinit(allocator);

    return .{
        .source_kind = source_kind,
        .plan = plan,
    };
}

fn buildClapPlannedExport(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
    access: tensor_access_mod.TensorAccess,
    config: clap_mod.Config,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
    source_metadata_file: ?*const gguf_mod.format.File,
    source_kind: ExportSourceKind,
) !PlannedExport {
    return buildClapPlannedExportFiltered(
        allocator,
        model_dir,
        manifest,
        access,
        config,
        quantization,
        filter,
        source_metadata_file,
        source_kind,
        .all,
    );
}

fn buildClapPlannedExportFiltered(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
    access: tensor_access_mod.TensorAccess,
    config: clap_mod.Config,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
    source_metadata_file: ?*const gguf_mod.format.File,
    source_kind: ExportSourceKind,
    name_filter: TensorNameFilter,
) !PlannedExport {
    const source_is_gguf = source_kind == .gguf;
    const names = try access.listNames(allocator);
    defer allocator.free(names);
    std.mem.sort([]const u8, names, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    var tensors = std.ArrayListUnmanaged(TensorPlan).empty;
    errdefer {
        for (tensors.items) |*tensor| tensor.deinit(allocator);
        tensors.deinit(allocator);
    }

    for (names) |name| {
        if (!tensorNameMatchesExportFilter(name_filter, name)) continue;
        var record = try access.getRecord(allocator, name);
        defer record.deinit();
        if (!source_is_gguf and !isExportableDenseRecord(record)) continue;

        const output_name_result = if (source_is_gguf)
            OutputName{ .name = try allocator.dupe(u8, record.descriptor.name), .owned = true }
        else
            try mapDenseTensorNameToClapGguf(allocator, record.descriptor.name);
        defer if (output_name_result.owned) allocator.free(output_name_result.name);

        const transform = if (source_is_gguf)
            TensorTransform.none
        else
            clipClapDenseExportTransformForTensor(name_filter, record.descriptor.name, record.descriptor.shape);
        const dimensions = switch (transform) {
            .none => try reversedDimsFromShape(allocator, record.descriptor.shape),
            .transpose_2d_dense => try dimsFromShape(allocator, record.descriptor.shape),
            .onnx_gru_zrh_to_rzn => return error.UnsupportedDenseArchitectureForGgufExport,
        };
        errdefer allocator.free(dimensions);

        const tensor_quantization = if (source_is_gguf)
            .none
        else
            supportedQuantizationForDescriptor(source_is_gguf, quantization, record.descriptor, transform);
        const filtered_quantization = if (!source_is_gguf and quantizationFilterMatches(filter, record.descriptor.name, output_name_result.name))
            tensor_quantization
        else
            QuantizationMode.none;
        const tensor_type = if (source_is_gguf)
            switch (record.descriptor.encoding) {
                .gguf => |value| value,
                else => unreachable,
            }
        else if (filtered_quantization != .none)
            tensorTypeForQuantizationMode(filtered_quantization)
        else
            try denseTensorType(record.descriptor.encoding.dense);

        try tensors.append(allocator, .{
            .source_name = try allocator.dupe(u8, record.descriptor.name),
            .output_name = try allocator.dupe(u8, output_name_result.name),
            .dimensions = dimensions,
            .tensor_type = tensor_type,
            .quantization = filtered_quantization,
            .transform = transform,
        });
    }

    if (tensors.items.len == 0) return error.NoExportableTensors;

    var plan = ExportPlan{
        .metadata_owned = if (source_is_gguf)
            try cloneMetadataEntries(allocator, source_metadata_file.?.metadata)
        else
            try buildClapMetadataEntries(allocator, manifest, config, model_dir),
        .borrowed_metadata_file = null,
        .tensors = try tensors.toOwnedSlice(allocator),
    };
    errdefer plan.deinit(allocator);

    return .{
        .source_kind = source_kind,
        .plan = plan,
    };
}

fn tensorNameMatchesExportFilter(filter: TensorNameFilter, source_name: []const u8) bool {
    if (filter == .all) return true;
    var name = source_name;
    if (std.mem.startsWith(u8, name, "clip.")) name = name["clip.".len..];
    if (std.mem.startsWith(u8, name, "clap.")) name = name["clap.".len..];
    if (isGeneratedOnnxExportConstant(name)) return false;
    return switch (filter) {
        .all => true,
        .clipclap_clip => std.mem.startsWith(u8, name, "text_model.") or
            std.mem.startsWith(u8, name, "vision_model.") or
            std.mem.eql(u8, name, "text_projection.weight") or
            std.mem.eql(u8, name, "visual_projection.weight") or
            std.mem.eql(u8, name, "logit_scale"),
        .clipclap_clap => std.mem.startsWith(u8, name, "audio_model.") or
            std.mem.startsWith(u8, name, "audio_projection.") or
            std.mem.eql(u8, name, "logit_scale"),
    };
}

fn isGeneratedOnnxExportConstant(name: []const u8) bool {
    if (std.mem.endsWith(u8, name, ".position_ids")) return true;
    const leaf = if (std.mem.lastIndexOfScalar(u8, name, '.')) |idx| name[idx + 1 ..] else name;
    return std.mem.startsWith(u8, leaf, "val_") or
        std.mem.startsWith(u8, leaf, "arange_") or
        std.mem.startsWith(u8, leaf, "index") or
        std.mem.startsWith(u8, leaf, "sub_") or
        std.mem.eql(u8, leaf, "scalar_tensor_default") or
        std.mem.eql(u8, leaf, "new_ones");
}

fn isExportableDenseRecord(record: tensor_access_mod.Record) bool {
    return switch (record.descriptor.encoding) {
        .dense => |dtype| switch (dtype) {
            .f32, .f16, .bf16 => true,
            else => false,
        },
        .gguf => true,
    };
}

fn clipClapDenseExportTransformForTensor(
    name_filter: TensorNameFilter,
    source_name: []const u8,
    shape: []const i64,
) TensorTransform {
    if (name_filter == .all) return .none;
    if (shape.len != 2) return .none;
    if (!std.mem.endsWith(u8, source_name, ".weight")) return .none;
    if (isClipOnnxMatMulWeight(source_name) or isClapOnnxMatMulWeight(source_name)) return .transpose_2d_dense;
    return .none;
}

fn isClipOnnxMatMulWeight(source_name: []const u8) bool {
    if (!std.mem.startsWith(u8, source_name, "text_model.encoder.layers.") and
        !std.mem.startsWith(u8, source_name, "vision_model.encoder.layers."))
    {
        return false;
    }
    return std.mem.indexOf(u8, source_name, ".self_attn.") != null or
        std.mem.indexOf(u8, source_name, ".mlp.fc") != null;
}

fn isClapOnnxMatMulWeight(source_name: []const u8) bool {
    if (!std.mem.startsWith(u8, source_name, "audio_model.audio_encoder.layers.")) return false;
    return std.mem.indexOf(u8, source_name, ".attention.self.") != null or
        std.mem.indexOf(u8, source_name, ".attention.output.dense.") != null or
        std.mem.indexOf(u8, source_name, ".downsample.reduction.") != null or
        std.mem.indexOf(u8, source_name, ".intermediate.dense.") != null or
        std.mem.indexOf(u8, source_name, ".output.dense.") != null;
}

fn appendGptNeoxSplitTensorPlans(
    allocator: std.mem.Allocator,
    tensors: *std.ArrayListUnmanaged(TensorPlan),
    record: tensor_access_mod.Record,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
) !bool {
    _ = quantization;
    _ = filter;
    if (record.descriptor.encoding != .dense) return false;
    var name = record.descriptor.name;
    if (std.mem.startsWith(u8, name, "gpt_neox.")) name = name["gpt_neox.".len..];
    if (!std.mem.startsWith(u8, name, "layers.")) return false;

    var parts = std.mem.splitScalar(u8, name, '.');
    _ = parts.next() orelse return false;
    const layer_str = parts.next() orelse return false;
    const layer = std.fmt.parseInt(usize, layer_str, 10) catch return false;
    const suffix_start = "layers.".len + layer_str.len + 1;
    if (suffix_start >= name.len) return false;
    const suffix = name[suffix_start..];

    if (std.mem.eql(u8, suffix, "attention.query_key_value.weight")) {
        if (record.descriptor.shape.len != 2) return error.UnsupportedTensorNameForGgufExport;
        const rows: usize = @intCast(record.descriptor.shape[0]);
        const cols: usize = @intCast(record.descriptor.shape[1]);
        if (rows % 3 != 0) return error.UnsupportedTensorNameForGgufExport;
        const chunk_rows = rows / 3;
        const elem_size = denseElementSize(record.descriptor.encoding.dense);
        const chunk_len = chunk_rows * cols * elem_size;
        const dims = try reversedDimsFromShape(allocator, &.{ @as(i64, @intCast(chunk_rows)), @as(i64, @intCast(cols)) });
        errdefer allocator.free(dims);
        const dtype = record.descriptor.encoding.dense;
        const tensor_type = try denseTensorType(dtype);
        const projs = [_][]const u8{ "q", "k", "v" };
        for (projs, 0..) |proj, idx| {
            try appendSlicedTensorPlan(
                allocator,
                tensors,
                record.descriptor.name,
                try std.fmt.allocPrint(allocator, "model.layers.{d}.self_attn.{s}_proj.weight", .{ layer, proj }),
                dims,
                tensor_type,
                idx * chunk_len,
                chunk_len,
            );
        }
        allocator.free(dims);
        return true;
    }

    if (std.mem.eql(u8, suffix, "attention.query_key_value.bias")) {
        if (record.descriptor.shape.len != 1) return error.UnsupportedTensorNameForGgufExport;
        const len: usize = @intCast(record.descriptor.shape[0]);
        if (len % 3 != 0) return error.UnsupportedTensorNameForGgufExport;
        const chunk_len_elems = len / 3;
        const elem_size = denseElementSize(record.descriptor.encoding.dense);
        const chunk_len = chunk_len_elems * elem_size;
        const dims = try reversedDimsFromShape(allocator, &.{@as(i64, @intCast(chunk_len_elems))});
        errdefer allocator.free(dims);
        const tensor_type = try denseTensorType(record.descriptor.encoding.dense);
        const projs = [_][]const u8{ "q", "k", "v" };
        for (projs, 0..) |proj, idx| {
            try appendSlicedTensorPlan(
                allocator,
                tensors,
                record.descriptor.name,
                try std.fmt.allocPrint(allocator, "model.layers.{d}.self_attn.{s}_proj.bias", .{ layer, proj }),
                dims,
                tensor_type,
                idx * chunk_len,
                chunk_len,
            );
        }
        allocator.free(dims);
        return true;
    }

    return false;
}

fn appendSlicedTensorPlan(
    allocator: std.mem.Allocator,
    tensors: *std.ArrayListUnmanaged(TensorPlan),
    source_name: []const u8,
    output_name_owned: []u8,
    dims: []const u64,
    tensor_type: gguf_mod.tensor_types.TensorType,
    start: usize,
    len: usize,
) !void {
    errdefer allocator.free(output_name_owned);
    const owned_source_name = try allocator.dupe(u8, source_name);
    errdefer allocator.free(owned_source_name);
    const owned_dims = try allocator.dupe(u64, dims);
    errdefer allocator.free(owned_dims);
    try tensors.append(allocator, .{
        .source_name = owned_source_name,
        .output_name = output_name_owned,
        .dimensions = owned_dims,
        .tensor_type = tensor_type,
        .quantization = .none,
        .transform = .none,
        .source_byte_range = .{ .start = start, .len = len },
    });
}

const OutputName = struct {
    name: []const u8,
    owned: bool,
};

fn mapDenseTensorNameToDebertaGguf(allocator: std.mem.Allocator, source_name: []const u8) !OutputName {
    var name = source_name;
    if (std.mem.startsWith(u8, name, "deberta.")) {
        name = name["deberta.".len..];
    }
    if (std.mem.startsWith(u8, name, "encoder.")) {
        return .{ .name = try allocator.dupe(u8, name), .owned = true };
    }
    if (std.mem.startsWith(u8, name, "embeddings.")) {
        return .{ .name = try allocator.dupe(u8, name), .owned = true };
    }
    if (std.mem.startsWith(u8, name, "pooler.") or
        std.mem.startsWith(u8, name, "classifier.") or
        std.mem.startsWith(u8, name, "pre_classifier."))
    {
        return .{ .name = try allocator.dupe(u8, name), .owned = true };
    }
    return error.UnsupportedTensorNameForGgufExport;
}

fn mapDenseTensorNameToBertGguf(allocator: std.mem.Allocator, config: bert_mod.Config, source_name: []const u8) !OutputName {
    var name = source_name;
    const prefix = config.effectivePrefix();
    if (prefix.len > 0) {
        const prefix_dot = try std.fmt.allocPrint(allocator, "{s}.", .{prefix});
        defer allocator.free(prefix_dot);
        if (std.mem.startsWith(u8, name, prefix_dot)) {
            name = name[prefix_dot.len..];
        }
    }

    if (config.model_type == .distilbert) {
        if (std.mem.startsWith(u8, name, "transformer.layer.")) {
            var parts = std.mem.splitScalar(u8, name, '.');
            _ = parts.next();
            _ = parts.next();
            const layer_str = parts.next() orelse return error.UnsupportedTensorNameForGgufExport;
            const layer = std.fmt.parseInt(usize, layer_str, 10) catch return error.UnsupportedTensorNameForGgufExport;
            const suffix_start = "transformer.layer.".len + layer_str.len + 1;
            if (suffix_start >= name.len) return error.UnsupportedTensorNameForGgufExport;
            const suffix = name[suffix_start..];

            if (std.mem.eql(u8, suffix, "attention.q_lin.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.attention.self.query.weight", .{layer}), .owned = true };
            if (std.mem.eql(u8, suffix, "attention.q_lin.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.attention.self.query.bias", .{layer}), .owned = true };
            if (std.mem.eql(u8, suffix, "attention.k_lin.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.attention.self.key.weight", .{layer}), .owned = true };
            if (std.mem.eql(u8, suffix, "attention.k_lin.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.attention.self.key.bias", .{layer}), .owned = true };
            if (std.mem.eql(u8, suffix, "attention.v_lin.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.attention.self.value.weight", .{layer}), .owned = true };
            if (std.mem.eql(u8, suffix, "attention.v_lin.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.attention.self.value.bias", .{layer}), .owned = true };
            if (std.mem.eql(u8, suffix, "attention.out_lin.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.attention.output.dense.weight", .{layer}), .owned = true };
            if (std.mem.eql(u8, suffix, "attention.out_lin.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.attention.output.dense.bias", .{layer}), .owned = true };
            if (std.mem.eql(u8, suffix, "sa_layer_norm.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.attention.output.LayerNorm.weight", .{layer}), .owned = true };
            if (std.mem.eql(u8, suffix, "sa_layer_norm.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.attention.output.LayerNorm.bias", .{layer}), .owned = true };
            if (std.mem.eql(u8, suffix, "ffn.lin1.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.intermediate.dense.weight", .{layer}), .owned = true };
            if (std.mem.eql(u8, suffix, "ffn.lin1.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.intermediate.dense.bias", .{layer}), .owned = true };
            if (std.mem.eql(u8, suffix, "ffn.lin2.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.output.dense.weight", .{layer}), .owned = true };
            if (std.mem.eql(u8, suffix, "ffn.lin2.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.output.dense.bias", .{layer}), .owned = true };
            if (std.mem.eql(u8, suffix, "output_layer_norm.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.output.LayerNorm.weight", .{layer}), .owned = true };
            if (std.mem.eql(u8, suffix, "output_layer_norm.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "encoder.layer.{d}.output.LayerNorm.bias", .{layer}), .owned = true };
        }
    }

    if (std.mem.startsWith(u8, name, "embeddings.") or
        std.mem.startsWith(u8, name, "encoder.") or
        std.mem.startsWith(u8, name, "pooler.") or
        std.mem.startsWith(u8, name, "classifier.") or
        std.mem.startsWith(u8, name, "pre_classifier."))
    {
        return .{ .name = try allocator.dupe(u8, name), .owned = true };
    }
    return error.UnsupportedTensorNameForGgufExport;
}

fn mapDenseTensorNameToLayoutlmv3Gguf(allocator: std.mem.Allocator, config: layoutlmv3_mod.Config, source_name: []const u8) !OutputName {
    var name = source_name;
    if (config.weight_prefix.len > 0) {
        const prefix = try std.fmt.allocPrint(allocator, "{s}.", .{config.weight_prefix});
        defer allocator.free(prefix);
        if (std.mem.startsWith(u8, name, prefix)) {
            name = name[prefix.len..];
        }
    }

    if (std.mem.startsWith(u8, name, "embeddings.") or
        std.mem.startsWith(u8, name, "encoder.") or
        std.mem.startsWith(u8, name, "patch_embed.") or
        std.mem.startsWith(u8, name, "classifier.") or
        std.mem.startsWith(u8, name, "pre_classifier.") or
        std.mem.eql(u8, name, "LayerNorm.weight") or
        std.mem.eql(u8, name, "LayerNorm.bias") or
        std.mem.eql(u8, name, "pos_embed") or
        std.mem.eql(u8, name, "cls_token") or
        std.mem.eql(u8, name, "norm.weight") or
        std.mem.eql(u8, name, "norm.bias"))
    {
        return .{ .name = try allocator.dupe(u8, name), .owned = true };
    }
    return error.UnsupportedTensorNameForGgufExport;
}

fn mapDenseTensorNameToFlorenceGguf(allocator: std.mem.Allocator, source_name: []const u8) !OutputName {
    var name = source_name;
    if (std.mem.startsWith(u8, name, "florence.")) {
        name = name["florence.".len..];
    }
    if (std.mem.startsWith(u8, name, "vision_tower.") or
        std.mem.startsWith(u8, name, "language_model.") or
        std.mem.startsWith(u8, name, "model.decoder.") or
        std.mem.startsWith(u8, name, "image_proj_norm.") or
        std.mem.startsWith(u8, name, "image_pos_embed.") or
        std.mem.startsWith(u8, name, "visual_temporal_embed.") or
        std.mem.eql(u8, name, "image_projection") or
        std.mem.eql(u8, name, "language_model.final_logits_bias") or
        std.mem.eql(u8, name, "lm_head.weight"))
    {
        return .{ .name = try allocator.dupe(u8, name), .owned = true };
    }
    return error.UnsupportedTensorNameForGgufExport;
}

fn mapDenseTensorNameToT5Gguf(allocator: std.mem.Allocator, source_name: []const u8) !OutputName {
    var name = source_name;
    if (std.mem.startsWith(u8, name, "t5.")) {
        name = name["t5.".len..];
    }
    if (std.mem.eql(u8, name, "shared.weight") or
        std.mem.eql(u8, name, "lm_head.weight") or
        std.mem.startsWith(u8, name, "encoder.") or
        std.mem.startsWith(u8, name, "decoder."))
    {
        return .{ .name = try allocator.dupe(u8, name), .owned = true };
    }
    return error.UnsupportedTensorNameForGgufExport;
}

fn mapDenseTensorNameToWhisperGguf(allocator: std.mem.Allocator, source_name: []const u8) !OutputName {
    var name = source_name;
    if (std.mem.startsWith(u8, name, "whisper.")) {
        name = name["whisper.".len..];
    }
    if (std.mem.eql(u8, name, "proj_out.weight") or
        std.mem.startsWith(u8, name, "model.encoder.") or
        std.mem.startsWith(u8, name, "model.decoder."))
    {
        return .{ .name = try allocator.dupe(u8, name), .owned = true };
    }
    return error.UnsupportedTensorNameForGgufExport;
}

fn mapDenseTensorNameToClipGguf(allocator: std.mem.Allocator, source_name: []const u8) !OutputName {
    var name = source_name;
    if (std.mem.startsWith(u8, name, "clip.")) {
        name = name["clip.".len..];
    }
    if (std.mem.startsWith(u8, name, "text_model.") or
        std.mem.startsWith(u8, name, "vision_model.") or
        std.mem.eql(u8, name, "text_projection.weight") or
        std.mem.eql(u8, name, "visual_projection.weight") or
        std.mem.eql(u8, name, "logit_scale"))
    {
        return .{ .name = try allocator.dupe(u8, name), .owned = true };
    }
    return error.UnsupportedTensorNameForGgufExport;
}

fn mapDenseTensorNameToClapGguf(allocator: std.mem.Allocator, source_name: []const u8) !OutputName {
    var name = source_name;
    if (std.mem.startsWith(u8, name, "clap.")) {
        name = name["clap.".len..];
    }
    if (std.mem.startsWith(u8, name, "audio_projection.clap_audio_proj.")) {
        return .{
            .name = try std.fmt.allocPrint(allocator, "audio_projection.{s}", .{name["audio_projection.clap_audio_proj.".len..]}),
            .owned = true,
        };
    }
    if (std.mem.startsWith(u8, name, "text_model.embeddings.") or
        std.mem.startsWith(u8, name, "text_model.encoder.") or
        std.mem.startsWith(u8, name, "text_model.pooler.") or
        std.mem.startsWith(u8, name, "audio_model.audio_encoder.") or
        std.mem.startsWith(u8, name, "text_projection.") or
        std.mem.startsWith(u8, name, "audio_projection.") or
        std.mem.eql(u8, name, "logit_scale"))
    {
        return .{ .name = try allocator.dupe(u8, name), .owned = true };
    }
    return error.UnsupportedTensorNameForGgufExport;
}

fn mapDenseTensorNameToGguf(allocator: std.mem.Allocator, config: gpt_mod.Config, source_name: []const u8) !OutputName {
    var name = source_name;
    if (std.mem.startsWith(u8, name, "language_model.")) {
        name = name["language_model.".len..];
    }
    if (std.mem.startsWith(u8, name, "transformer.")) {
        name = name["transformer.".len..];
    }
    if (std.mem.startsWith(u8, name, "gpt_neox.")) {
        name = name["gpt_neox.".len..];
    }

    if (config.family == .gpt2 or config.family == .gpt_neo) {
        if (std.mem.eql(u8, name, "wte.weight")) return .{ .name = "wte.weight", .owned = false };
        if (std.mem.eql(u8, name, "wpe.weight")) return .{ .name = "wpe.weight", .owned = false };
        if (std.mem.eql(u8, name, "ln_f.weight")) return .{ .name = "ln_f.weight", .owned = false };
        if (std.mem.eql(u8, name, "ln_f.bias")) return .{ .name = "ln_f.bias", .owned = false };
        if (std.mem.eql(u8, name, "lm_head.weight")) return .{ .name = "lm_head.weight", .owned = false };
        if (std.mem.startsWith(u8, name, "h.")) return .{ .name = name, .owned = false };
        return error.UnsupportedTensorNameForGgufExport;
    }

    if (config.family == .gptj) {
        if (std.mem.eql(u8, name, "wte.weight")) return .{ .name = "model.embed_tokens.weight", .owned = false };
        if (std.mem.eql(u8, name, "wpe.weight")) return .{ .name = "wpe.weight", .owned = false };
        if (std.mem.eql(u8, name, "ln_f.weight")) return .{ .name = "model.norm.weight", .owned = false };
        if (std.mem.eql(u8, name, "ln_f.bias")) return .{ .name = "model.norm.bias", .owned = false };
        if (std.mem.eql(u8, name, "lm_head.weight")) return .{ .name = "lm_head.weight", .owned = false };

        if (!std.mem.startsWith(u8, name, "h.")) return error.UnsupportedTensorNameForGgufExport;
        var parts = std.mem.splitScalar(u8, name, '.');
        _ = parts.next() orelse return error.UnsupportedTensorNameForGgufExport;
        const layer_str = parts.next() orelse return error.UnsupportedTensorNameForGgufExport;
        const layer = std.fmt.parseInt(usize, layer_str, 10) catch return error.UnsupportedTensorNameForGgufExport;
        const suffix_start = 2 + layer_str.len + 1;
        if (suffix_start >= name.len) return error.UnsupportedTensorNameForGgufExport;
        const suffix = name[suffix_start..];

        if (std.mem.eql(u8, suffix, "ln_1.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.input_layernorm.weight", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "ln_1.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.input_layernorm.bias", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "attn.q_proj.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.self_attn.q_proj.weight", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "attn.q_proj.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.self_attn.q_proj.bias", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "attn.k_proj.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.self_attn.k_proj.weight", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "attn.k_proj.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.self_attn.k_proj.bias", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "attn.v_proj.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.self_attn.v_proj.weight", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "attn.v_proj.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.self_attn.v_proj.bias", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "attn.out_proj.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.self_attn.o_proj.weight", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "attn.out_proj.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.self_attn.o_proj.bias", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "ln_2.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.post_attention_layernorm.weight", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "ln_2.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.post_attention_layernorm.bias", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "mlp.fc_in.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.mlp.fc1_proj.weight", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "mlp.fc_in.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.mlp.fc1_proj.bias", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "mlp.fc_out.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.mlp.fc2_proj.weight", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "mlp.fc_out.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.mlp.fc2_proj.bias", .{layer}), .owned = true };
        return error.UnsupportedTensorNameForGgufExport;
    }

    if (config.family == .gpt_neox) {
        if (std.mem.eql(u8, name, "embed_in.weight")) return .{ .name = "model.embed_tokens.weight", .owned = false };
        if (std.mem.eql(u8, name, "embed_out.weight")) return .{ .name = "lm_head.weight", .owned = false };
        if (std.mem.eql(u8, name, "final_layer_norm.weight")) return .{ .name = "model.norm.weight", .owned = false };
        if (std.mem.eql(u8, name, "final_layer_norm.bias")) return .{ .name = "model.norm.bias", .owned = false };
        if (std.mem.eql(u8, name, "wpe.weight")) return .{ .name = "wpe.weight", .owned = false };

        if (!std.mem.startsWith(u8, name, "layers.")) return error.UnsupportedTensorNameForGgufExport;
        var parts = std.mem.splitScalar(u8, name, '.');
        _ = parts.next() orelse return error.UnsupportedTensorNameForGgufExport;
        const layer_str = parts.next() orelse return error.UnsupportedTensorNameForGgufExport;
        const layer = std.fmt.parseInt(usize, layer_str, 10) catch return error.UnsupportedTensorNameForGgufExport;
        const suffix_start = "layers.".len + layer_str.len + 1;
        if (suffix_start >= name.len) return error.UnsupportedTensorNameForGgufExport;
        const suffix = name[suffix_start..];

        if (std.mem.eql(u8, suffix, "input_layernorm.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.input_layernorm.weight", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "input_layernorm.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.input_layernorm.bias", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "attention.dense.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.self_attn.o_proj.weight", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "attention.dense.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.self_attn.o_proj.bias", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "post_attention_layernorm.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.post_attention_layernorm.weight", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "post_attention_layernorm.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.post_attention_layernorm.bias", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "mlp.dense_h_to_4h.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.mlp.fc1_proj.weight", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "mlp.dense_h_to_4h.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.mlp.fc1_proj.bias", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "mlp.dense_4h_to_h.weight")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.mlp.fc2_proj.weight", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "mlp.dense_4h_to_h.bias")) return .{ .name = try std.fmt.allocPrint(allocator, "model.layers.{d}.mlp.fc2_proj.bias", .{layer}), .owned = true };
        if (std.mem.eql(u8, suffix, "attention.query_key_value.weight") or std.mem.eql(u8, suffix, "attention.query_key_value.bias")) {
            return error.UnsupportedTensorNameForGgufExport;
        }
        return error.UnsupportedTensorNameForGgufExport;
    }

    if (std.mem.eql(u8, name, "model.embed_tokens.weight")) return .{ .name = "token_embd.weight", .owned = false };
    if (std.mem.eql(u8, name, "model.norm.weight")) return .{ .name = "output_norm.weight", .owned = false };
    if (std.mem.eql(u8, name, "model.norm.bias")) {
        return switch (config.family) {
            .phi => .{ .name = "output_norm.bias", .owned = false },
            else => error.UnsupportedTensorNameForGgufExport,
        };
    }
    if (std.mem.eql(u8, name, "lm_head.weight")) return .{ .name = "output.weight", .owned = false };
    if (std.mem.eql(u8, name, "model.per_layer_input.per_layer_token_embd.weight")) return .{ .name = "per_layer_token_embd.weight", .owned = false };
    if (std.mem.eql(u8, name, "model.per_layer_input.per_layer_model_proj.weight")) return .{ .name = "per_layer_model_proj.weight", .owned = false };
    if (std.mem.eql(u8, name, "model.per_layer_input.per_layer_proj_norm.weight")) return .{ .name = "per_layer_proj_norm.weight", .owned = false };

    if (!std.mem.startsWith(u8, name, "model.layers.")) return error.UnsupportedTensorNameForGgufExport;

    var parts = std.mem.splitScalar(u8, name, '.');
    _ = parts.next() orelse return error.UnsupportedTensorNameForGgufExport;
    _ = parts.next() orelse return error.UnsupportedTensorNameForGgufExport;
    const layer_str = parts.next() orelse return error.UnsupportedTensorNameForGgufExport;
    const layer = std.fmt.parseInt(usize, layer_str, 10) catch return error.UnsupportedTensorNameForGgufExport;

    const suffix_start = "model.layers.".len + layer_str.len + 1;
    const suffix = name[suffix_start..];

    if (std.mem.eql(u8, suffix, "input_layernorm.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.attn_norm.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "input_layernorm.bias")) {
        return switch (config.family) {
            .phi => .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.attn_norm.bias", .{layer}), .owned = true },
            else => error.UnsupportedTensorNameForGgufExport,
        };
    }
    if (std.mem.eql(u8, suffix, "self_attn.q_norm.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.attn_q_norm.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "self_attn.k_norm.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.attn_k_norm.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "self_attn.attn_sub_norm.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.attn_sub_norm.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "mlp.ffn_sub_norm.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.ffn_sub_norm.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "self_attn.q_proj.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.attn_q.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "self_attn.q_proj.bias")) {
        return switch (config.family) {
            .qwen2, .phi => .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.attn_q.bias", .{layer}), .owned = true },
            else => error.UnsupportedTensorNameForGgufExport,
        };
    }
    if (std.mem.eql(u8, suffix, "self_attn.k_proj.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.attn_k.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "self_attn.k_proj.bias")) {
        return switch (config.family) {
            .qwen2, .phi => .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.attn_k.bias", .{layer}), .owned = true },
            else => error.UnsupportedTensorNameForGgufExport,
        };
    }
    if (std.mem.eql(u8, suffix, "self_attn.v_proj.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.attn_v.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "self_attn.v_proj.bias")) {
        return switch (config.family) {
            .qwen2, .phi => .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.attn_v.bias", .{layer}), .owned = true },
            else => error.UnsupportedTensorNameForGgufExport,
        };
    }
    if (std.mem.eql(u8, suffix, "self_attn.o_proj.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.attn_output.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "mlp.gate_proj.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.ffn_gate.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "mlp.up_proj.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.ffn_up.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "mlp.down_proj.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.ffn_down.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "mlp.fc1_proj.weight")) {
        return switch (config.family) {
            .phi => .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.ffn_up.weight", .{layer}), .owned = true },
            else => error.UnsupportedTensorNameForGgufExport,
        };
    }
    if (std.mem.eql(u8, suffix, "mlp.fc2_proj.weight")) {
        return switch (config.family) {
            .phi => .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.ffn_down.weight", .{layer}), .owned = true },
            else => error.UnsupportedTensorNameForGgufExport,
        };
    }
    if (std.mem.eql(u8, suffix, "block_sparse_moe.gate.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.ffn_gate_inp.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "block_sparse_moe.gate.input_scale")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.ffn_gate_inp.scale", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "block_sparse_moe.expert_output_scale")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.ffn_down_exps.scale", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "block_sparse_moe.shared_expert.gate_proj.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.ffn_gate_shexp.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "block_sparse_moe.shared_expert.up_proj.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.ffn_up_shexp.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "block_sparse_moe.shared_expert.down_proj.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.ffn_down_shexp.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "per_layer_input.inp_gate.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.inp_gate.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "per_layer_input.proj.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.proj.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "per_layer_input.layer_output_scale.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.layer_output_scale.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "per_layer_input.post_norm.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.post_norm.weight", .{layer}), .owned = true };
    }

    if (std.mem.eql(u8, suffix, "pre_feedforward_layernorm.weight")) {
        return switch (config.family) {
            .gemma => .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.ffn_norm.weight", .{layer}), .owned = true },
            else => error.UnsupportedTensorNameForGgufExport,
        };
    }
    if (std.mem.eql(u8, suffix, "post_attention_layernorm.weight")) {
        return switch (config.family) {
            .gemma => .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.post_attention_norm.weight", .{layer}), .owned = true },
            else => .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.ffn_norm.weight", .{layer}), .owned = true },
        };
    }
    if (std.mem.eql(u8, suffix, "post_attention_layernorm.bias")) {
        return switch (config.family) {
            .phi => .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.ffn_norm.bias", .{layer}), .owned = true },
            else => error.UnsupportedTensorNameForGgufExport,
        };
    }
    if (std.mem.eql(u8, suffix, "post_feedforward_layernorm.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.post_ffw_norm.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "post_feedforward_layernorm_1.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.post_ffw_norm_1.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "pre_feedforward_layernorm_2.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.pre_ffw_norm_2.weight", .{layer}), .owned = true };
    }
    if (std.mem.eql(u8, suffix, "post_feedforward_layernorm_2.weight")) {
        return .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.post_ffw_norm_2.weight", .{layer}), .owned = true };
    }

    if (std.mem.startsWith(u8, suffix, "block_sparse_moe.experts.")) {
        return mapMoeExpertTensorName(allocator, layer, suffix);
    }

    return error.UnsupportedTensorNameForGgufExport;
}

fn denseDecoderExportSupport(config: gpt_mod.Config) DenseDecoderExportSupport {
    return switch (config.family) {
        .llama, .mistral, .qwen2, .qwen3, .qwen3_5, .gemma, .bitnet, .phi, .gpt2, .gpt_neo, .gpt_neox, .gptj => .supported,
        .deepseek_v4, .falcon, .opt, .bloom, .other => .unsupported_name_mapping,
    };
}

fn denseDecoderExportSupportLabel(config: gpt_mod.Config) []const u8 {
    return switch (denseDecoderExportSupport(config)) {
        .supported => "supported",
        .unsupported_name_mapping => "unsupported: dense tensor name mapping not implemented",
        .unsupported_tensor_transform => "unsupported: dense export requires tensor reshaping or splitting before GGUF serialization",
    };
}

fn modelFamilyLabel(family: gpt_mod.ModelFamily) []const u8 {
    return switch (family) {
        .gpt2 => "gpt2",
        .gpt_neo => "gpt_neo",
        .gpt_neox => "gpt_neox",
        .gptj => "gptj",
        .llama => "llama",
        .mistral => "mistral",
        .phi => "phi",
        .qwen2 => "qwen2",
        .qwen3 => "qwen3",
        .qwen3_5 => "qwen3_5",
        .deepseek_v4 => "deepseek_v4",
        .gemma => "gemma",
        .bitnet => "bitnet",
        .falcon => "falcon",
        .opt => "opt",
        .bloom => "bloom",
        .other => "other",
    };
}

fn mapMoeExpertTensorName(allocator: std.mem.Allocator, layer: usize, suffix: []const u8) !OutputName {
    const prefix = "block_sparse_moe.experts.";
    var parts = std.mem.splitScalar(u8, suffix[prefix.len..], '.');
    const expert_str = parts.next() orelse return error.UnsupportedTensorNameForGgufExport;
    const proj = parts.next() orelse return error.UnsupportedTensorNameForGgufExport;
    const weight = parts.next() orelse return error.UnsupportedTensorNameForGgufExport;
    if (!std.mem.eql(u8, weight, "weight")) return error.UnsupportedTensorNameForGgufExport;

    const expert = std.fmt.parseInt(usize, expert_str, 10) catch return error.UnsupportedTensorNameForGgufExport;
    const gguf_prefix = if (std.mem.eql(u8, proj, "w1"))
        "ffn_gate"
    else if (std.mem.eql(u8, proj, "w2"))
        "ffn_down"
    else if (std.mem.eql(u8, proj, "w3"))
        "ffn_up"
    else
        return error.UnsupportedTensorNameForGgufExport;

    return .{
        .name = try std.fmt.allocPrint(allocator, "blk.{d}.{s}.{d}.weight", .{ layer, gguf_prefix, expert }),
        .owned = true,
    };
}

fn denseTensorType(dtype: @import("backends/tensor.zig").DType) !gguf_mod.tensor_types.TensorType {
    return switch (dtype) {
        .f32 => .{ .known = .F32 },
        .f16 => .{ .known = .F16 },
        .bf16 => .{ .known = .BF16 },
        else => error.UnsupportedDenseTensorTypeForGgufExport,
    };
}

fn denseElementSize(dtype: @import("backends/tensor.zig").DType) usize {
    return switch (dtype) {
        .f32 => 4,
        .f16, .bf16 => 2,
        else => 0,
    };
}

fn denseExportTransformForTensor(config: gpt_mod.Config, source_name: []const u8, shape: []const i64) TensorTransform {
    if (config.family == .gpt2 and isGpt2Conv1dWeight(source_name, shape)) return .transpose_2d_dense;
    return .none;
}

fn denseExportDimsForTensor(
    allocator: std.mem.Allocator,
    config: gpt_mod.Config,
    source_name: []const u8,
    shape: []const i64,
) ![]u64 {
    return switch (denseExportTransformForTensor(config, source_name, shape)) {
        .none => reversedDimsFromShape(allocator, shape),
        .transpose_2d_dense => dimsFromShape(allocator, shape),
        .onnx_gru_zrh_to_rzn => error.UnsupportedDenseArchitectureForGgufExport,
    };
}

fn supportedQuantizationForTensor(
    config: gpt_mod.Config,
    source_is_gguf: bool,
    quantization: QuantizationMode,
    descriptor: tensor_access_mod.Descriptor,
) QuantizationMode {
    const transform = denseExportTransformForTensor(config, descriptor.name, descriptor.shape);
    return supportedQuantizationForDescriptor(source_is_gguf, quantization, descriptor, transform);
}

fn supportedQuantizationForDescriptor(
    source_is_gguf: bool,
    quantization: QuantizationMode,
    descriptor: tensor_access_mod.Descriptor,
    transform: TensorTransform,
) QuantizationMode {
    if (source_is_gguf or quantization == .none) return .none;
    if (descriptor.encoding != .dense) return .none;
    if (descriptor.shape.len < 2) return .none;
    const row_width = quantizedRowWidth(descriptor.shape, transform) orelse return .none;
    const block_width: i64 = switch (quantization) {
        .q1_0 => 128,
        .q2_k, .q3_k, .q4_k, .q5_k, .q6_k, .q8_k => 256,
        .q4_0, .q4_1, .q5_0, .q5_1, .q8_0, .q8_1 => 32,
        .none => return .none,
    };
    if (@mod(@as(i64, @intCast(row_width)), block_width) != 0) return .none;
    return switch (descriptor.encoding) {
        .dense => |dtype| if (dtype == .f32 or dtype == .f16 or dtype == .bf16) quantization else .none,
        else => .none,
    };
}

fn quantizationFilterMatches(filter: QuantizationFilter, source_name: []const u8, output_name: []const u8) bool {
    if (filter.exclude_prefixes_csv) |csv| {
        if (matchesAnyPrefix(csv, source_name) or matchesAnyPrefix(csv, output_name)) return false;
    }
    if (filter.include_prefixes_csv) |csv| {
        return matchesAnyPrefix(csv, source_name) or matchesAnyPrefix(csv, output_name);
    }
    return true;
}

fn isGpt2Conv1dWeight(name: []const u8, shape: []const i64) bool {
    if (shape.len != 2) return false;
    if (!std.mem.startsWith(u8, name, "h.")) return false;
    if (!std.mem.endsWith(u8, name, ".weight")) return false;
    if (std.mem.indexOf(u8, name, ".attn.") != null) return true;
    if (std.mem.indexOf(u8, name, ".mlp.") != null) return true;
    return false;
}

fn isMultimodalAuxTensor(name: []const u8) bool {
    return std.mem.startsWith(u8, name, "vision_tower.") or
        std.mem.startsWith(u8, name, "multi_modal_projector.") or
        std.mem.startsWith(u8, name, "visual.") or
        std.mem.startsWith(u8, name, "model.visual.") or
        std.mem.startsWith(u8, name, "vlm.model.visual.") or
        std.mem.startsWith(u8, name, "vision_model.") or
        std.mem.startsWith(u8, name, "audio_tower.") or
        std.mem.startsWith(u8, name, "audio_model.");
}

fn defaultProjectorOutputPath(allocator: std.mem.Allocator, output_path: []const u8) ![]u8 {
    const dir = std.fs.path.dirname(output_path) orelse ".";
    return std.fs.path.join(allocator, &.{ dir, "mmproj.gguf" });
}

fn copyProjectorGguf(allocator: std.mem.Allocator, source_path: []const u8, target_path: []const u8) !void {
    if (std.mem.eql(u8, source_path, target_path)) return;
    const raw = try c_file.readFile(allocator, source_path);
    defer allocator.free(raw);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = target_path, .data = raw });
}

fn resolveRequestedProjectorFormat(
    allocator: std.mem.Allocator,
    projector_source_path: ?[]const u8,
    requested: ProjectorFormat,
) !?ResolvedProjectorFormat {
    if (projector_source_path) |path| {
        const detected_kind = try projector_format_mod.detectPath(allocator, path);
        const detected: ResolvedProjectorFormat = if (projector_format_mod.isAntfly(detected_kind))
            .antfly
        else if (projector_format_mod.isClip(detected_kind))
            .clip
        else
            return error.UnsupportedProjectorFormat;

        return switch (requested) {
            .auto => detected,
            .antfly => if (detected == .antfly) .antfly else error.UnsupportedRequestedProjectorFormat,
            .clip => if (detected == .clip) .clip else error.UnsupportedRequestedProjectorFormat,
        };
    }

    return switch (requested) {
        .auto, .antfly => .antfly,
        .clip => null,
    };
}

fn buildProjectorMetadataEntries(
    allocator: std.mem.Allocator,
    manifest: manifest_mod.ModelManifest,
    config: gpt_mod.Config,
    format: ResolvedProjectorFormat,
) ![]gguf_mod.format.MetadataEntry {
    return switch (format) {
        .antfly => buildAntflyProjectorMetadataEntries(allocator, manifest, config),
        .clip => error.UnsupportedCanonicalProjectorExport,
    };
}

fn buildAntflyProjectorMetadataEntries(
    allocator: std.mem.Allocator,
    manifest: manifest_mod.ModelManifest,
    config: gpt_mod.Config,
) ![]gguf_mod.format.MetadataEntry {
    var entries = std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry).empty;
    errdefer {
        for (entries.items) |*entry| entry.deinit(allocator);
        entries.deinit(allocator);
    }

    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "general.architecture"),
        .value = .{ .string = try allocator.dupe(u8, "antfly-projector") },
    });
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "general.alignment"),
        .value = .{ .u32 = 32 },
    });
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "inference.projector.kind"),
        .value = .{ .string = try allocator.dupe(u8, "integrated-multimodal") },
    });
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "inference.projector.source_architecture"),
        .value = .{ .string = try allocator.dupe(u8, archStringForConfig(manifest, config)) },
    });

    if (config.mm_tokens_per_image > 0) {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "inference.projector.mm_tokens_per_image"),
            .value = .{ .u32 = config.mm_tokens_per_image },
        });
    }
    if (config.hidden_size > 0) {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "inference.projector.text_hidden_size"),
            .value = .{ .u32 = config.hidden_size },
        });
    }
    if (config.vision_hidden_size > 0) {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "inference.projector.vision_hidden_size"),
            .value = .{ .u32 = config.vision_hidden_size },
        });
    }
    if (config.vision_num_hidden_layers > 0) {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "inference.projector.vision_block_count"),
            .value = .{ .u32 = config.vision_num_hidden_layers },
        });
    }
    if (config.vision_num_attention_heads > 0) {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "inference.projector.vision_attention_head_count"),
            .value = .{ .u32 = config.vision_num_attention_heads },
        });
    }
    if (config.vision_intermediate_size > 0) {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "inference.projector.vision_feed_forward_length"),
            .value = .{ .u32 = config.vision_intermediate_size },
        });
    }
    if (config.vision_image_size > 0) {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "inference.projector.vision_image_size"),
            .value = .{ .u32 = config.vision_image_size },
        });
    }
    if (config.vision_patch_size > 0) {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "inference.projector.vision_patch_size"),
            .value = .{ .u32 = config.vision_patch_size },
        });
    }

    return try entries.toOwnedSlice(allocator);
}

fn formatDryRunReport(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    output_path: []const u8,
    projector_output_path: ?[]const u8,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
    source_kind: ExportSourceKind,
    planned: *const PlannedExport,
) ![]u8 {
    var text: std.Io.Writer.Allocating = .init(allocator);
    defer text.deinit();
    const writer = &text.writer;
    const plan = &planned.plan;

    var quantized_count: usize = 0;
    for (plan.tensors) |tensor| {
        if (tensor.quantization != .none) quantized_count += 1;
    }

    try writer.writeAll(
        \\export dry-run target=gguf
        \\
    );
    try writer.print("source: {s}\n", .{model_dir});
    try writer.print("output: {s} (not written)\n", .{output_path});
    if (planned.projector_source_path) |source| {
        try writer.print("projector: {s} -> {s} (not written, format={s})\n", .{ source, projector_output_path orelse "missing", @tagName(planned.projector_format.?) });
    } else if (planned.projector_plan) |projector_plan| {
        try writer.print("projector: synthesized -> {s} (not written, format={s})\n", .{ projector_output_path orelse "missing", @tagName(planned.projector_format.?) });
        try writer.print("projector metadata: {d} entries\n", .{projector_plan.metadata().len});
        try writer.print("projector plan: {d} tensors\n", .{projector_plan.tensors.len});
    }
    try writer.print("mode: {s} -> gguf\n", .{switch (source_kind) {
        .gguf => "gguf",
        .dense => "safetensors",
    }});
    try writer.print("quantization: {s}\n", .{@tagName(quantization)});
    try writer.print("filters: include={s}, exclude={s}\n", .{
        filter.include_prefixes_csv orelse "none",
        filter.exclude_prefixes_csv orelse "none",
    });
    try writer.print("metadata: {d} entries\n", .{plan.metadata().len});
    try writer.print("plan: {d} tensors, {d} quantized\n", .{ plan.tensors.len, quantized_count });
    try writer.writeAll("\nTensors\n");

    for (plan.tensors) |tensor| {
        const type_name = tensorTypeName(tensor.tensor_type);
        const output_shape = formatShape(allocator, tensor.dimensions, false) catch "<?>"; // fallback impossible in tests
        defer if (!std.mem.eql(u8, output_shape, "<?>")) allocator.free(output_shape);
        if (gguf_mod.tensor_types.byteLen(tensor.tensor_type, tensor.dimensions)) |byte_len| {
            try writer.print("  [{s:9}] {s} src={s} shape={s} type={s} bytes={d}\n", .{
                if (tensor.quantization == .none)
                    (switch (tensor.tensor_type) {
                        .known => |known| switch (known) {
                            .Q1_0, .Q2_K, .Q3_K, .Q4_0, .Q4_1, .Q5_0, .Q5_1, .Q4_K, .Q5_K, .Q6_K, .Q8_0, .Q8_1, .Q8_K => "passthru",
                            else => "dense",
                        },
                        else => "dense",
                    })
                else
                    "quantize",
                tensor.output_name,
                tensor.source_name,
                output_shape,
                type_name,
                byte_len,
            });
        } else {
            try writer.print("  [{s:9}] {s} src={s} shape={s} type={s} bytes=unsupported\n", .{
                if (tensor.quantization == .none)
                    (switch (tensor.tensor_type) {
                        .known => |known| switch (known) {
                            .Q1_0, .Q2_K, .Q3_K, .Q4_0, .Q4_1, .Q5_0, .Q5_1, .Q4_K, .Q5_K, .Q6_K, .Q8_0, .Q8_1, .Q8_K => "passthru",
                            else => "dense",
                        },
                        else => "dense",
                    })
                else
                    "quantize",
                tensor.output_name,
                tensor.source_name,
                output_shape,
                type_name,
            });
        }
    }

    return text.toOwnedSlice();
}

fn formatUnsupportedDenseDryRunReport(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    output_path: []const u8,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
) ![]u8 {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    var parsed_source_gguf: ?SourceGguf = null;
    defer if (parsed_source_gguf) |*source| source.deinit();

    const export_arch = try resolveExportArchitecture(allocator, model_dir, manifest, &parsed_source_gguf);
    const config = switch (export_arch) {
        .gpt => |value| value,
        else => return error.UnsupportedModelForGgufExport,
    };

    var text: std.Io.Writer.Allocating = .init(allocator);
    defer text.deinit();
    const writer = &text.writer;

    try writer.writeAll(
        \\export dry-run target=gguf
        \\
    );
    try writer.print("source: {s}\n", .{model_dir});
    try writer.print("output: {s} (not written)\n", .{output_path});
    try writer.writeAll("mode: safetensors -> gguf\n");
    try writer.print("family: {s}\n", .{modelFamilyLabel(config.family)});
    try writer.print("quantization: {s}\n", .{@tagName(quantization)});
    try writer.print("filters: include={s}, exclude={s}\n", .{
        filter.include_prefixes_csv orelse "none",
        filter.exclude_prefixes_csv orelse "none",
    });
    try writer.print("support: {s}\n", .{denseDecoderExportSupportLabel(config)});
    try writer.writeAll("plan: unavailable\n");
    try writer.writeAll("reason: dense decoder export is not implemented for this architecture family yet\n");

    return text.toOwnedSlice();
}

fn formatUnsupportedModelDryRunReport(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    output_path: []const u8,
    quantization: QuantizationMode,
    filter: QuantizationFilter,
    reason: UnsupportedModelReason,
) ![]u8 {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    var text: std.Io.Writer.Allocating = .init(allocator);
    defer text.deinit();
    const writer = &text.writer;

    try writer.writeAll(
        \\export dry-run target=gguf
        \\
    );
    try writer.print("source: {s}\n", .{model_dir});
    try writer.print("output: {s} (not written)\n", .{output_path});
    try writer.writeAll("plan: unavailable\n");
    try writer.print("quantization: {s}\n", .{@tagName(quantization)});
    try writer.print("filters: include={s}, exclude={s}\n", .{
        filter.include_prefixes_csv orelse "none",
        filter.exclude_prefixes_csv orelse "none",
    });
    try writer.print("model_type: {s}\n", .{if (manifest.config_model_arch.len > 0) manifest.config_model_arch else "unknown"});
    if (manifest.gliner_model_type.len > 0) {
        try writer.print("wrapper: {s}\n", .{manifest.gliner_model_type});
    }
    switch (reason) {
        .composite_wrapper => try writer.writeAll("reason: this model family is a composite wrapper over another backbone and does not have a standalone GGUF export contract yet\n"),
        .unsupported_architecture => try writer.writeAll("reason: this model family does not currently have a GGUF export path in Antfly inference\n"),
    }
    return text.toOwnedSlice();
}

fn cloneMetadataEntries(
    allocator: std.mem.Allocator,
    entries: []const gguf_mod.format.MetadataEntry,
) ![]gguf_mod.format.MetadataEntry {
    const cloned = try allocator.alloc(gguf_mod.format.MetadataEntry, entries.len);
    errdefer allocator.free(cloned);
    var len: usize = 0;
    errdefer {
        for (cloned[0..len]) |*entry| entry.deinit(allocator);
    }
    for (entries) |entry| {
        cloned[len] = .{
            .key = try allocator.dupe(u8, entry.key),
            .value = try cloneMetadataValue(allocator, entry.value),
        };
        len += 1;
    }
    return cloned[0..len];
}

fn cloneMetadataValue(
    allocator: std.mem.Allocator,
    value: gguf_mod.format.MetadataValue,
) !gguf_mod.format.MetadataValue {
    return switch (value) {
        .u8 => |v| .{ .u8 = v },
        .i8 => |v| .{ .i8 = v },
        .u16 => |v| .{ .u16 = v },
        .i16 => |v| .{ .i16 = v },
        .u32 => |v| .{ .u32 = v },
        .i32 => |v| .{ .i32 = v },
        .f32 => |v| .{ .f32 = v },
        .bool_ => |v| .{ .bool_ = v },
        .string => |v| .{ .string = try allocator.dupe(u8, v) },
        .array => |arr| blk: {
            const values = try allocator.alloc(gguf_mod.format.MetadataValue, arr.values.len);
            errdefer allocator.free(values);
            var len: usize = 0;
            errdefer {
                for (values[0..len]) |*item| item.deinit(allocator);
            }
            for (arr.values) |item| {
                values[len] = try cloneMetadataValue(allocator, item);
                len += 1;
            }
            break :blk .{ .array = .{
                .element_type = arr.element_type,
                .values = values[0..len],
            } };
        },
        .u64 => |v| .{ .u64 = v },
        .i64 => |v| .{ .i64 = v },
        .f64 => |v| .{ .f64 = v },
    };
}

fn tensorTypeName(tensor_type: gguf_mod.tensor_types.TensorType) []const u8 {
    return switch (tensor_type) {
        .known => |known| @tagName(known),
        .bitnet_tl2 => "bitnet_tl2",
        .unknown => "unknown",
    };
}

fn formatShape(allocator: std.mem.Allocator, dims: []const u64, reverse: bool) ![]u8 {
    var text: std.Io.Writer.Allocating = .init(allocator);
    defer text.deinit();
    const writer = &text.writer;
    if (reverse) {
        for (dims, 0..) |_, i| {
            if (i > 0) try writer.writeByte('x');
            try writer.print("{d}", .{dims[dims.len - 1 - i]});
        }
    } else {
        for (dims, 0..) |dim, i| {
            if (i > 0) try writer.writeByte('x');
            try writer.print("{d}", .{dim});
        }
    }
    return text.toOwnedSlice();
}

fn matchesAnyPrefix(csv: []const u8, value: []const u8) bool {
    var iter = std.mem.splitScalar(u8, csv, ',');
    while (iter.next()) |raw_prefix| {
        const prefix = std.mem.trim(u8, raw_prefix, " \t\r\n");
        if (prefix.len == 0) continue;
        if (std.mem.startsWith(u8, value, prefix)) return true;
    }
    return false;
}

fn tensorTypeForQuantizationMode(mode: QuantizationMode) gguf_mod.tensor_types.TensorType {
    return switch (mode) {
        .q1_0 => .{ .known = .Q1_0 },
        .q2_k => .{ .known = .Q2_K },
        .q3_k => .{ .known = .Q3_K },
        .q4_0 => .{ .known = .Q4_0 },
        .q4_1 => .{ .known = .Q4_1 },
        .q5_0 => .{ .known = .Q5_0 },
        .q5_1 => .{ .known = .Q5_1 },
        .q4_k => .{ .known = .Q4_K },
        .q5_k => .{ .known = .Q5_K },
        .q6_k => .{ .known = .Q6_K },
        .q8_k => .{ .known = .Q8_K },
        .q8_0 => .{ .known = .Q8_0 },
        .q8_1 => .{ .known = .Q8_1 },
        .none => unreachable,
    };
}

fn dimsFromShape(allocator: std.mem.Allocator, shape: []const i64) ![]u64 {
    const dims = try allocator.alloc(u64, shape.len);
    for (shape, 0..) |dim, i| {
        if (dim < 0) return error.InvalidTensorShape;
        dims[i] = @intCast(dim);
    }
    return dims;
}

fn reversedDimsFromShape(allocator: std.mem.Allocator, shape: []const i64) ![]u64 {
    const dims = try allocator.alloc(u64, shape.len);
    for (shape, 0..) |_, i| {
        const dim = shape[shape.len - 1 - i];
        if (dim < 0) return error.InvalidTensorShape;
        dims[i] = @intCast(dim);
    }
    return dims;
}

fn resolveExportArchitecture(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
    parsed_source_gguf: *?SourceGguf,
) !ExportArchitecture {
    if (c_file.readFileFromDir(allocator, model_dir, "config.json")) |json| {
        defer allocator.free(json);
        if (manifest.gliner_model_type.len > 0 or std.mem.eql(u8, manifest.config_model_arch, "extractor")) {
            return error.UnsupportedCompositeModelForGgufExport;
        }
        if (bert_mod.isBertModel(manifest.config_model_arch)) {
            return .{ .bert = try bert_mod.parseConfig(allocator, json) };
        }
        if (t5_mod.isT5Model(manifest.config_model_arch)) {
            return .{ .t5 = try t5_mod.parseConfig(allocator, json) };
        }
        if (whisper_mod.isWhisperModel(manifest.config_model_arch)) {
            return .{ .whisper = try whisper_mod.parseConfig(allocator, json) };
        }
        if (deberta_mod.isDebertaModel(manifest.config_model_arch)) {
            return .{ .deberta = try deberta_mod.parseConfig(allocator, json) };
        }
        if (std.mem.eql(u8, manifest.config_model_arch, "layoutlmv3")) {
            return .{ .layoutlmv3 = try layoutlmv3_mod.parseConfig(allocator, json) };
        }
        if (florence_mod.isFlorenceModel(manifest.config_model_arch)) {
            return .{ .florence = try florence_mod.parseConfig(allocator, json) };
        }
        if (clip_mod.isClipModel(manifest.config_model_arch)) {
            return .{ .clip = try clip_mod.parseConfig(allocator, json) };
        }
        if (clap_mod.isClapModel(manifest.config_model_arch)) {
            return .{ .clap = try clap_mod.parseConfig(allocator, json) };
        }
        const config = try gpt_mod.parseConfig(allocator, json);
        if (!gpt_mod.isGenerativeModel(archStringForConfig(manifest, config))) return error.UnsupportedModelForGgufExport;
        return .{ .gpt = config };
    } else |_| {}

    if (manifest.gguf_path) |path| {
        parsed_source_gguf.* = try SourceGguf.init(allocator, path);
        const view = gguf_mod.metadata.View.init(&parsed_source_gguf.*.?.parsed);
        if (gpt_mod.parseGgufMetadata(view)) |config| return .{ .gpt = config };
        if (bert_mod.parseGgufMetadata(view)) |config| return .{ .bert = config };
        if (t5_mod.parseGgufMetadata(view)) |config| return .{ .t5 = config };
        if (whisper_mod.parseGgufMetadata(view)) |config| return .{ .whisper = config };
        if (deberta_mod.parseGgufMetadata(view)) |config| return .{ .deberta = config };
        if (layoutlmv3_mod.parseGgufMetadata(view)) |config| return .{ .layoutlmv3 = config };
        if (florence_mod.parseGgufMetadata(view)) |config| return .{ .florence = config };
        if (clip_mod.parseGgufMetadata(view)) |config| return .{ .clip = config };
        if (clap_mod.parseGgufMetadata(view)) |config| return .{ .clap = config };
        return error.UnsupportedModelForGgufExport;
    }

    return error.UnsupportedModelForGgufExport;
}

fn buildDebertaMetadataEntries(
    allocator: std.mem.Allocator,
    manifest: manifest_mod.ModelManifest,
    config: deberta_mod.Config,
    model_dir: []const u8,
) ![]gguf_mod.format.MetadataEntry {
    var entries = std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry).empty;
    errdefer {
        for (entries.items) |*entry| entry.deinit(allocator);
        entries.deinit(allocator);
    }

    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.architecture"), .value = .{ .string = try allocator.dupe(u8, "deberta") } });
    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.alignment"), .value = .{ .u32 = 32 } });
    try appendMetadataU32Entry(allocator, &entries, "deberta.vocab_size", config.vocab_size);
    try appendMetadataU32Entry(allocator, &entries, "deberta.embedding_length", config.hidden_size);
    try appendMetadataU32Entry(allocator, &entries, "deberta.block_count", config.num_hidden_layers);
    try appendMetadataU32Entry(allocator, &entries, "deberta.attention.head_count", config.num_attention_heads);
    try appendMetadataU32Entry(allocator, &entries, "deberta.feed_forward_length", config.intermediate_size);
    try appendMetadataU32Entry(allocator, &entries, "deberta.context_length", config.max_position_embeddings);
    try appendMetadataU32Entry(allocator, &entries, "deberta.position_buckets", config.position_buckets);
    try appendMetadataU32Entry(allocator, &entries, "deberta.label_count", config.num_labels);
    try appendMetadataF32Entry(allocator, &entries, "deberta.layer_norm_epsilon", config.layer_norm_eps);

    if (manifest.chat_template) |value| {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "tokenizer.chat_template"),
            .value = .{ .string = try allocator.dupe(u8, value) },
        });
    }
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "tokenizer.ggml.add_bos_token"),
        .value = .{ .bool_ = manifest.add_bos_token },
    });
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "tokenizer.ggml.add_eos_token"),
        .value = .{ .bool_ = manifest.add_eos_token },
    });
    if (manifest.tokenizer_type != null) {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "tokenizer.ggml.model"),
            .value = .{ .string = try allocator.dupe(u8, switch (manifest.tokenizer_type.?) {
                .huggingface => "bert",
                .sentencepiece => "bert",
            }) },
        });
    }

    try appendStandaloneTokenizerMetadata(allocator, &entries, model_dir, manifest);
    return try entries.toOwnedSlice(allocator);
}

fn buildBertMetadataEntries(
    allocator: std.mem.Allocator,
    manifest: manifest_mod.ModelManifest,
    config: bert_mod.Config,
    model_dir: []const u8,
) ![]gguf_mod.format.MetadataEntry {
    var entries = std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry).empty;
    errdefer {
        for (entries.items) |*entry| entry.deinit(allocator);
        entries.deinit(allocator);
    }

    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.architecture"), .value = .{ .string = try allocator.dupe(u8, "bert") } });
    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.alignment"), .value = .{ .u32 = 32 } });
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "bert.family"),
        .value = .{ .string = try allocator.dupe(u8, switch (config.model_type) {
            .bert => "bert",
            .roberta => "roberta",
            .distilbert => "distilbert",
        }) },
    });
    try appendMetadataU32Entry(allocator, &entries, "bert.vocab_size", config.vocab_size);
    try appendMetadataU32Entry(allocator, &entries, "bert.embedding_length", config.hidden_size);
    try appendMetadataU32Entry(allocator, &entries, "bert.block_count", config.num_hidden_layers);
    try appendMetadataU32Entry(allocator, &entries, "bert.attention.head_count", config.num_attention_heads);
    try appendMetadataU32Entry(allocator, &entries, "bert.feed_forward_length", config.intermediate_size);
    try appendMetadataU32Entry(allocator, &entries, "bert.context_length", config.max_position_embeddings);
    try appendMetadataU32Entry(allocator, &entries, "bert.token_type_count", config.type_vocab_size);
    try appendMetadataU32Entry(allocator, &entries, "bert.label_count", config.num_labels);
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "bert.hidden_act"),
        .value = .{ .string = try allocator.dupe(u8, config.hidden_act) },
    });

    if (manifest.chat_template) |value| {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "tokenizer.chat_template"),
            .value = .{ .string = try allocator.dupe(u8, value) },
        });
    }
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "tokenizer.ggml.add_bos_token"),
        .value = .{ .bool_ = manifest.add_bos_token },
    });
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "tokenizer.ggml.add_eos_token"),
        .value = .{ .bool_ = manifest.add_eos_token },
    });
    if (manifest.tokenizer_type != null) {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "tokenizer.ggml.model"),
            .value = .{ .string = try allocator.dupe(u8, "bert") },
        });
    }

    try appendStandaloneTokenizerMetadata(allocator, &entries, model_dir, manifest);
    return try entries.toOwnedSlice(allocator);
}

fn buildT5MetadataEntries(
    allocator: std.mem.Allocator,
    manifest: manifest_mod.ModelManifest,
    config: t5_mod.Config,
    model_dir: []const u8,
) ![]gguf_mod.format.MetadataEntry {
    var entries = std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry).empty;
    errdefer {
        for (entries.items) |*entry| entry.deinit(allocator);
        entries.deinit(allocator);
    }

    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.architecture"), .value = .{ .string = try allocator.dupe(u8, "t5") } });
    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.alignment"), .value = .{ .u32 = 32 } });
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "t5.family"),
        .value = .{ .string = try allocator.dupe(u8, switch (config.model_type) {
            .t5 => "t5",
            .mt5 => "mt5",
            .longt5 => "longt5",
        }) },
    });
    try appendMetadataU32Entry(allocator, &entries, "t5.embedding_length", config.d_model);
    try appendMetadataU32Entry(allocator, &entries, "t5.attention.key_value_length", config.d_kv);
    try appendMetadataU32Entry(allocator, &entries, "t5.feed_forward_length", config.d_ff);
    try appendMetadataU32Entry(allocator, &entries, "t5.attention.head_count", config.num_heads);
    try appendMetadataU32Entry(allocator, &entries, "t5.encoder.block_count", config.num_layers);
    try appendMetadataU32Entry(allocator, &entries, "t5.decoder.block_count", config.effectiveDecoderLayers());
    try appendMetadataU32Entry(allocator, &entries, "t5.attention.relative_buckets", config.relative_attention_num_buckets);
    try appendMetadataU32Entry(allocator, &entries, "t5.attention.relative_max_distance", config.relative_attention_max_distance);
    try appendMetadataU32Entry(allocator, &entries, "t5.vocab_size", config.vocab_size);
    try appendMetadataI64Entry(allocator, &entries, "t5.decoder_start_token_id", config.decoder_start_token_id);
    try appendMetadataI64Entry(allocator, &entries, "t5.eos_token_id", config.eos_token_id);
    try appendMetadataI64Entry(allocator, &entries, "t5.pad_token_id", config.pad_token_id);
    try appendMetadataBoolEntry(allocator, &entries, "t5.is_gated_act", config.is_gated_act);

    if (manifest.chat_template) |value| {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "tokenizer.chat_template"),
            .value = .{ .string = try allocator.dupe(u8, value) },
        });
    }
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "tokenizer.ggml.add_bos_token"),
        .value = .{ .bool_ = manifest.add_bos_token },
    });
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "tokenizer.ggml.add_eos_token"),
        .value = .{ .bool_ = manifest.add_eos_token },
    });
    if (manifest.tokenizer_type != null) {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "tokenizer.ggml.model"),
            .value = .{ .string = try allocator.dupe(u8, "t5") },
        });
    }

    try appendStandaloneTokenizerMetadata(allocator, &entries, model_dir, manifest);
    return try entries.toOwnedSlice(allocator);
}

fn buildWhisperMetadataEntries(
    allocator: std.mem.Allocator,
    manifest: manifest_mod.ModelManifest,
    config: whisper_mod.Config,
    model_dir: []const u8,
) ![]gguf_mod.format.MetadataEntry {
    var entries = std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry).empty;
    errdefer {
        for (entries.items) |*entry| entry.deinit(allocator);
        entries.deinit(allocator);
    }

    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.architecture"), .value = .{ .string = try allocator.dupe(u8, "whisper") } });
    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.alignment"), .value = .{ .u32 = 32 } });
    try appendMetadataU32Entry(allocator, &entries, "whisper.embedding_length", config.d_model);
    try appendMetadataU32Entry(allocator, &entries, "whisper.encoder.block_count", config.encoder_layers);
    try appendMetadataU32Entry(allocator, &entries, "whisper.decoder.block_count", config.decoder_layers);
    try appendMetadataU32Entry(allocator, &entries, "whisper.encoder.attention.head_count", config.encoder_attention_heads);
    try appendMetadataU32Entry(allocator, &entries, "whisper.decoder.attention.head_count", config.decoder_attention_heads);
    try appendMetadataU32Entry(allocator, &entries, "whisper.encoder.feed_forward_length", config.encoder_ffn_dim);
    try appendMetadataU32Entry(allocator, &entries, "whisper.decoder.feed_forward_length", config.decoder_ffn_dim);
    try appendMetadataU32Entry(allocator, &entries, "whisper.num_mel_bins", config.num_mel_bins);
    try appendMetadataU32Entry(allocator, &entries, "whisper.vocab_size", config.vocab_size);
    try appendMetadataU32Entry(allocator, &entries, "whisper.encoder.context_length", config.max_source_positions);
    try appendMetadataU32Entry(allocator, &entries, "whisper.decoder.context_length", config.max_target_positions);
    try appendMetadataBoolEntry(allocator, &entries, "whisper.scale_embedding", config.scale_embedding);
    try appendMetadataI64Entry(allocator, &entries, "whisper.bos_token_id", config.bos_token_id);
    try appendMetadataI64Entry(allocator, &entries, "whisper.eos_token_id", config.eos_token_id);
    try appendMetadataI64Entry(allocator, &entries, "whisper.pad_token_id", config.pad_token_id);
    try appendMetadataI64Entry(allocator, &entries, "whisper.decoder_start_token_id", config.decoder_start_token_id);

    if (manifest.chat_template) |value| {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "tokenizer.chat_template"),
            .value = .{ .string = try allocator.dupe(u8, value) },
        });
    }
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "tokenizer.ggml.add_bos_token"),
        .value = .{ .bool_ = manifest.add_bos_token },
    });
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "tokenizer.ggml.add_eos_token"),
        .value = .{ .bool_ = manifest.add_eos_token },
    });
    if (manifest.tokenizer_type != null) {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "tokenizer.ggml.model"),
            .value = .{ .string = try allocator.dupe(u8, "gpt2") },
        });
    }

    try appendStandaloneTokenizerMetadata(allocator, &entries, model_dir, manifest);
    return try entries.toOwnedSlice(allocator);
}

fn buildLayoutlmv3MetadataEntries(
    allocator: std.mem.Allocator,
    manifest: manifest_mod.ModelManifest,
    config: layoutlmv3_mod.Config,
    model_dir: []const u8,
) ![]gguf_mod.format.MetadataEntry {
    var entries = std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry).empty;
    errdefer {
        for (entries.items) |*entry| entry.deinit(allocator);
        entries.deinit(allocator);
    }

    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.architecture"), .value = .{ .string = try allocator.dupe(u8, "layoutlmv3") } });
    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.alignment"), .value = .{ .u32 = 32 } });
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.vocab_size", config.vocab_size);
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.embedding_length", config.hidden_size);
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.block_count", config.num_hidden_layers);
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.attention.head_count", config.num_attention_heads);
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.feed_forward_length", config.intermediate_size);
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.context_length", config.max_position_embeddings);
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.token_type_count", config.type_vocab_size);
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.max_2d_position_embeddings", config.max_2d_position_embeddings);
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.coordinate_size", config.coordinate_size);
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.shape_size", config.shape_size);
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.input_size", config.input_size);
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.patch_size", config.patch_size);
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.num_channels", @intCast(config.num_channels));
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.label_count", config.num_labels);
    try appendMetadataI64Entry(allocator, &entries, "layoutlmv3.pad_token_id", config.pad_token_id);
    try appendMetadataF32Entry(allocator, &entries, "layoutlmv3.layer_norm_epsilon", config.layer_norm_eps);
    try appendMetadataBoolEntry(allocator, &entries, "layoutlmv3.has_relative_attention_bias", config.has_relative_attention_bias);
    try appendMetadataBoolEntry(allocator, &entries, "layoutlmv3.has_spatial_attention_bias", config.has_spatial_attention_bias);
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.rel_pos_bins", config.rel_pos_bins);
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.max_rel_pos", config.max_rel_pos);
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.rel_2d_pos_bins", config.rel_2d_pos_bins);
    try appendMetadataU32Entry(allocator, &entries, "layoutlmv3.max_rel_2d_pos", config.max_rel_2d_pos);
    try appendMetadataI64Entry(allocator, &entries, "layoutlmv3.visual_bbox_max_len", config.visual_bbox_max_len);

    if (manifest.chat_template) |value| {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "tokenizer.chat_template"),
            .value = .{ .string = try allocator.dupe(u8, value) },
        });
    }
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "tokenizer.ggml.add_bos_token"),
        .value = .{ .bool_ = manifest.add_bos_token },
    });
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "tokenizer.ggml.add_eos_token"),
        .value = .{ .bool_ = manifest.add_eos_token },
    });
    if (manifest.tokenizer_type != null) {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "tokenizer.ggml.model"),
            .value = .{ .string = try allocator.dupe(u8, "bert") },
        });
    }

    try appendStandaloneTokenizerMetadata(allocator, &entries, model_dir, manifest);
    return try entries.toOwnedSlice(allocator);
}

fn buildFlorenceMetadataEntries(
    allocator: std.mem.Allocator,
    manifest: manifest_mod.ModelManifest,
    config: florence_mod.Config,
    model_dir: []const u8,
) ![]gguf_mod.format.MetadataEntry {
    var entries = std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry).empty;
    var image_feature_source_strings: [3][]const u8 = undefined;
    errdefer {
        for (entries.items) |*entry| entry.deinit(allocator);
        entries.deinit(allocator);
    }

    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.architecture"), .value = .{ .string = try allocator.dupe(u8, "florence") } });
    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.alignment"), .value = .{ .u32 = 32 } });
    try appendMetadataU32Entry(allocator, &entries, "florence.text.d_model", config.d_model);
    try appendMetadataU32Entry(allocator, &entries, "florence.text.encoder_layers", config.encoder_layers);
    try appendMetadataU32Entry(allocator, &entries, "florence.text.decoder_layers", config.decoder_layers);
    try appendMetadataU32Entry(allocator, &entries, "florence.text.encoder_attention_heads", config.encoder_attention_heads);
    try appendMetadataU32Entry(allocator, &entries, "florence.text.decoder_attention_heads", config.decoder_attention_heads);
    try appendMetadataU32Entry(allocator, &entries, "florence.text.encoder_ffn_dim", config.encoder_ffn_dim);
    try appendMetadataU32Entry(allocator, &entries, "florence.text.decoder_ffn_dim", config.decoder_ffn_dim);
    try appendMetadataU32Entry(allocator, &entries, "florence.text.vocab_size", config.vocab_size);
    try appendMetadataU32Entry(allocator, &entries, "florence.text.max_position_embeddings", config.max_position_embeddings);

    try appendMetadataU32Entry(allocator, &entries, "florence.vision.image_size", config.image_size);
    try appendMetadataU32Entry(allocator, &entries, "florence.vision.hidden_size", config.vision_hidden_size);
    try appendMetadataU32ArrayEntry(allocator, &entries, "florence.vision.patch_size", &config.patch_size);
    try appendMetadataU32ArrayEntry(allocator, &entries, "florence.vision.patch_stride", &config.patch_stride);
    try appendMetadataU32ArrayEntry(allocator, &entries, "florence.vision.patch_padding", &config.patch_padding);
    try appendMetadataBoolArrayEntry(allocator, &entries, "florence.vision.patch_prenorm", &config.patch_prenorm);
    try appendMetadataU32ArrayEntry(allocator, &entries, "florence.vision.dim_embed", &config.dim_embed);
    try appendMetadataU32ArrayEntry(allocator, &entries, "florence.vision.num_heads", &config.num_heads);
    try appendMetadataU32ArrayEntry(allocator, &entries, "florence.vision.num_groups", &config.num_groups);
    try appendMetadataU32ArrayEntry(allocator, &entries, "florence.vision.depths", &config.depths);
    try appendMetadataU32Entry(allocator, &entries, "florence.vision.window_size", config.window_size);
    try appendMetadataU32Entry(allocator, &entries, "florence.vision.image_pos_embed_max_pos", config.image_pos_embed_max_pos);
    try appendMetadataU32Entry(allocator, &entries, "florence.vision.visual_temporal_max_embeddings", config.visual_temporal_max_embeddings);
    const image_feature_source_count: usize = @min(image_feature_source_strings.len, @as(usize, @intCast(config.image_feature_source_count)));
    for (0..image_feature_source_count) |idx| {
        image_feature_source_strings[idx] = imageFeatureSourceString(config.image_feature_sources[idx]);
    }
    try appendMetadataStringArrayEntry(
        allocator,
        &entries,
        "florence.vision.image_feature_source",
        image_feature_source_strings[0..image_feature_source_count],
    );

    try appendMetadataU32Entry(allocator, &entries, "florence.projection_dim", config.projection_dim);
    try appendMetadataI64Entry(allocator, &entries, "florence.image_token_id", config.image_token_id);
    try appendMetadataI64Entry(allocator, &entries, "florence.bos_token_id", config.bos_token_id);
    try appendMetadataI64Entry(allocator, &entries, "florence.eos_token_id", config.eos_token_id);
    try appendMetadataI64Entry(allocator, &entries, "florence.pad_token_id", config.pad_token_id);
    try appendMetadataI64Entry(allocator, &entries, "florence.decoder_start_token_id", config.decoder_start_token_id);

    if (manifest.chat_template) |value| {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "tokenizer.chat_template"),
            .value = .{ .string = try allocator.dupe(u8, value) },
        });
    }
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "tokenizer.ggml.add_bos_token"),
        .value = .{ .bool_ = manifest.add_bos_token },
    });
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "tokenizer.ggml.add_eos_token"),
        .value = .{ .bool_ = manifest.add_eos_token },
    });
    if (manifest.tokenizer_type != null) {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "tokenizer.ggml.model"),
            .value = .{ .string = try allocator.dupe(u8, "bart") },
        });
    }

    try appendStandaloneTokenizerMetadata(allocator, &entries, model_dir, manifest);
    return try entries.toOwnedSlice(allocator);
}

fn buildClipMetadataEntries(
    allocator: std.mem.Allocator,
    manifest: manifest_mod.ModelManifest,
    config: clip_mod.Config,
    model_dir: []const u8,
) ![]gguf_mod.format.MetadataEntry {
    var entries = std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry).empty;
    errdefer {
        for (entries.items) |*entry| entry.deinit(allocator);
        entries.deinit(allocator);
    }

    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.architecture"), .value = .{ .string = try allocator.dupe(u8, "clip") } });
    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.alignment"), .value = .{ .u32 = 32 } });
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "clip.family"),
        .value = .{ .string = try allocator.dupe(u8, switch (config.family) {
            .clip => "clip",
            .siglip => "siglip",
        }) },
    });
    try appendMetadataU32Entry(allocator, &entries, "clip.text.embedding_length", config.text_hidden_size);
    try appendMetadataU32Entry(allocator, &entries, "clip.text.block_count", config.text_num_layers);
    try appendMetadataU32Entry(allocator, &entries, "clip.text.attention.head_count", config.text_num_heads);
    try appendMetadataU32Entry(allocator, &entries, "clip.text.feed_forward_length", config.text_intermediate_size);
    try appendMetadataU32Entry(allocator, &entries, "clip.text.context_length", config.text_max_position_embeddings);
    try appendMetadataU32Entry(allocator, &entries, "clip.text.vocab_size", config.vocab_size);
    try appendMetadataU32Entry(allocator, &entries, "clip.vision.embedding_length", config.vision_hidden_size);
    try appendMetadataU32Entry(allocator, &entries, "clip.vision.block_count", config.vision_num_layers);
    try appendMetadataU32Entry(allocator, &entries, "clip.vision.attention.head_count", config.vision_num_heads);
    try appendMetadataU32Entry(allocator, &entries, "clip.vision.feed_forward_length", config.vision_intermediate_size);
    try appendMetadataU32Entry(allocator, &entries, "clip.vision.image_size", config.image_size);
    try appendMetadataU32Entry(allocator, &entries, "clip.vision.patch_size", config.patch_size);
    try appendMetadataU32Entry(allocator, &entries, "clip.projection_dim", config.projection_dim);

    if (manifest.chat_template) |value| {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "tokenizer.chat_template"),
            .value = .{ .string = try allocator.dupe(u8, value) },
        });
    }
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "tokenizer.ggml.add_bos_token"),
        .value = .{ .bool_ = manifest.add_bos_token },
    });
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "tokenizer.ggml.add_eos_token"),
        .value = .{ .bool_ = manifest.add_eos_token },
    });
    if (manifest.tokenizer_type != null) {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "tokenizer.ggml.model"),
            .value = .{ .string = try allocator.dupe(u8, "bert") },
        });
    }

    try appendStandaloneTokenizerMetadata(allocator, &entries, model_dir, manifest);
    return try entries.toOwnedSlice(allocator);
}

fn buildClapMetadataEntries(
    allocator: std.mem.Allocator,
    manifest: manifest_mod.ModelManifest,
    config: clap_mod.Config,
    model_dir: []const u8,
) ![]gguf_mod.format.MetadataEntry {
    var entries = std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry).empty;
    errdefer {
        for (entries.items) |*entry| entry.deinit(allocator);
        entries.deinit(allocator);
    }

    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.architecture"), .value = .{ .string = try allocator.dupe(u8, "clap") } });
    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.alignment"), .value = .{ .u32 = 32 } });
    try appendMetadataU32Entry(allocator, &entries, "clap.projection_dim", config.projection_dim);
    try appendMetadataF32Entry(allocator, &entries, "clap.logit_scale_init_value", config.logit_scale_init_value);
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "clap.projection_hidden_act"),
        .value = .{ .string = try allocator.dupe(u8, switch (config.projection_hidden_act) {
            .relu => "relu",
            .gelu => "gelu",
        }) },
    });

    try appendMetadataU32Entry(allocator, &entries, "clap.text.vocab_size", config.text_config.vocab_size);
    try appendMetadataU32Entry(allocator, &entries, "clap.text.embedding_length", config.text_config.hidden_size);
    try appendMetadataU32Entry(allocator, &entries, "clap.text.block_count", config.text_config.num_hidden_layers);
    try appendMetadataU32Entry(allocator, &entries, "clap.text.attention.head_count", config.text_config.num_attention_heads);
    try appendMetadataU32Entry(allocator, &entries, "clap.text.feed_forward_length", config.text_config.intermediate_size);
    try appendMetadataU32Entry(allocator, &entries, "clap.text.context_length", config.text_config.max_position_embeddings);
    try appendMetadataU32Entry(allocator, &entries, "clap.text.token_type_count", config.text_config.type_vocab_size);
    try appendMetadataI64Entry(allocator, &entries, "clap.text.pad_token_id", config.text_pad_token_id);

    try appendMetadataU32Entry(allocator, &entries, "clap.audio.embedding_length", config.audio_config.hidden_size);
    try appendMetadataU32Entry(allocator, &entries, "clap.audio.patch_embeds_hidden_size", config.audio_config.patch_embeds_hidden_size);
    try appendMetadataU32Entry(allocator, &entries, "clap.audio.patch_embed_input_channels", config.audio_config.patch_embed_input_channels);
    try appendMetadataU32Entry(allocator, &entries, "clap.audio.patch_size", config.audio_config.patch_size);
    try appendMetadataU32Entry(allocator, &entries, "clap.audio.num_mel_bins", config.audio_config.num_mel_bins);
    try appendMetadataU32Entry(allocator, &entries, "clap.audio.spec_size", config.audio_config.spec_size);
    try appendMetadataU32Entry(allocator, &entries, "clap.audio.window_size", config.audio_config.window_size);
    try appendMetadataF32Entry(allocator, &entries, "clap.audio.mlp_ratio", config.audio_config.mlp_ratio);
    try appendMetadataF32Entry(allocator, &entries, "clap.audio.layer_norm_epsilon", config.audio_config.layer_norm_eps);
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "clap.audio.hidden_act"),
        .value = .{ .string = try allocator.dupe(u8, switch (config.audio_config.hidden_act) {
            .relu => "relu",
            .gelu => "gelu",
        }) },
    });
    try appendMetadataBoolEntry(allocator, &entries, "clap.audio.qkv_bias", config.audio_config.qkv_bias);
    try appendMetadataBoolEntry(allocator, &entries, "clap.audio.enable_fusion", config.audio_config.enable_fusion);
    try appendMetadataBoolEntry(allocator, &entries, "clap.audio.enable_patch_fusion", config.audio_config.enable_patch_fusion);
    try appendMetadataBoolEntry(allocator, &entries, "clap.audio.enable_patch_layer_norm", config.audio_config.enable_patch_layer_norm);
    try appendMetadataBoolEntry(allocator, &entries, "clap.enable_fusion", config.enable_fusion);
    try appendMetadataU32ArrayEntry(allocator, &entries, "clap.audio.patch_stride", &config.audio_config.patch_stride);
    try appendMetadataU32ArrayEntry(allocator, &entries, "clap.audio.depths", &config.audio_config.depths);
    try appendMetadataU32ArrayEntry(allocator, &entries, "clap.audio.attention_head_counts", &config.audio_config.num_attention_heads);

    try appendStandaloneTokenizerMetadata(allocator, &entries, model_dir, manifest);
    return try entries.toOwnedSlice(allocator);
}

fn buildMetadataEntries(
    allocator: std.mem.Allocator,
    manifest: manifest_mod.ModelManifest,
    config: gpt_mod.Config,
    model_dir: []const u8,
) ![]gguf_mod.format.MetadataEntry {
    var entries = std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry).empty;
    errdefer {
        for (entries.items) |*entry| entry.deinit(allocator);
        entries.deinit(allocator);
    }

    const arch = try allocator.dupe(u8, archStringForConfig(manifest, config));
    errdefer allocator.free(arch);
    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.architecture"), .value = .{ .string = arch } });
    try entries.append(allocator, .{ .key = try allocator.dupe(u8, "general.alignment"), .value = .{ .u32 = 32 } });

    try appendArchU32(allocator, &entries, arch, "vocab_size", config.vocab_size);
    try appendArchU32(allocator, &entries, arch, "embedding_length", config.hidden_size);
    try appendArchU32(allocator, &entries, arch, "block_count", config.num_hidden_layers);
    try appendArchU32(allocator, &entries, arch, "attention.head_count", config.num_attention_heads);
    try appendHeadCountKvMetadata(allocator, &entries, arch, config);
    try appendArchU32(allocator, &entries, arch, "attention.key_length", config.headDim());

    if (config.global_head_dim > 0) {
        try appendArchU32(allocator, &entries, arch, "attention.key_length_swa", config.attention_head_dim);
        try appendArchU32(allocator, &entries, arch, "attention.global_head_dim", config.global_head_dim);
    }

    try appendFeedForwardMetadata(allocator, &entries, arch, config);
    try appendArchU32(allocator, &entries, arch, "context_length", config.max_position_embeddings);
    if (config.sliding_window > 0) {
        try appendArchU32(allocator, &entries, arch, "attention.sliding_window", config.sliding_window);
    }
    if (config.num_local_experts > 0) {
        try appendArchU32(allocator, &entries, arch, "expert_count", config.num_local_experts);
    }
    if (config.num_experts_per_tok > 0) {
        try appendArchU32(allocator, &entries, arch, "expert_used_count", config.num_experts_per_tok);
    }
    if (config.num_shared_experts > 0) {
        try appendArchU32(allocator, &entries, arch, "expert_shared_count", config.num_shared_experts);
    }
    if (config.expert_intermediate_size > 0) {
        try appendArchU32(allocator, &entries, arch, "expert_feed_forward_length", config.expertIntermediateSize());
    }
    if (config.rope_theta != 10000.0 or config.position_encoding == .rope) {
        try appendArchF32(allocator, &entries, arch, "rope.freq_base", config.rope_theta);
    }
    if (config.rope_local_theta != 10000.0) {
        try appendArchF32(allocator, &entries, arch, "rope.freq_base_swa", config.rope_local_theta);
    }
    if (config.rope_freq_scale != 1.0 and config.family != .gemma and config.rope_freq_scale > 0.0) {
        try appendArchF32(allocator, &entries, arch, "rope.scaling.factor", 1.0 / config.rope_freq_scale);
    }
    if (config.norm_type == .rms_norm) {
        try appendArchF32(allocator, &entries, arch, "attention.layer_norm_rms_epsilon", config.norm_eps);
    }
    if (config.final_logit_softcapping > 0.0) {
        try appendArchF32(allocator, &entries, arch, "final_logit_softcapping", config.final_logit_softcapping);
    }
    if (config.num_kv_shared_layers > 0) {
        try appendArchU32(allocator, &entries, arch, "attention.kv_shared_layer_count", config.num_kv_shared_layers);
    }
    if (config.num_global_key_value_heads > 0) {
        try appendArchU32(allocator, &entries, arch, "attention.global_head_count_kv", config.num_global_key_value_heads);
    }
    if (config.ple_hidden_size > 0) {
        try appendArchU32(allocator, &entries, arch, "embedding_length_per_layer_input", config.ple_hidden_size);
    }
    if (config.rope_partial_factor < 1.0 and config.num_hidden_layers > 0) {
        const full_attn_layer = fullAttentionLayerIndex(config) orelse 0;
        try appendArchU32(allocator, &entries, arch, "rope.dimension_count", config.layerRopeActiveDim(full_attn_layer));
    }
    if (config.sliding_window_pattern > 0 and config.sliding_window > 0) {
        try appendSlidingPatternMetadata(allocator, &entries, arch, config);
    }

    if (manifest.chat_template) |value| {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "tokenizer.chat_template"),
            .value = .{ .string = try allocator.dupe(u8, value) },
        });
    }
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "tokenizer.ggml.add_bos_token"),
        .value = .{ .bool_ = manifest.add_bos_token },
    });
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, "tokenizer.ggml.add_eos_token"),
        .value = .{ .bool_ = manifest.add_eos_token },
    });
    if (tokenizerModelName(manifest, config)) |value| {
        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, "tokenizer.ggml.model"),
            .value = .{ .string = try allocator.dupe(u8, value) },
        });
    }

    try appendStandaloneTokenizerMetadata(allocator, &entries, model_dir, manifest);

    return try entries.toOwnedSlice(allocator);
}

fn appendStandaloneTokenizerMetadata(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
) !void {
    if (c_file.fileExistsInDir(allocator, model_dir, "tokenizer.model")) {
        try appendSentencePieceTokenizerMetadata(allocator, entries, model_dir, manifest);
        return;
    }
    if (c_file.fileExistsInDir(allocator, model_dir, "tokenizer.json")) {
        try appendHfTokenizerMetadata(allocator, entries, model_dir);
    }
}

fn appendSentencePieceTokenizerMetadata(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
) !void {
    const model_path = try std.fs.path.join(allocator, &.{ model_dir, "tokenizer.model" });
    defer allocator.free(model_path);

    var sp = try tokenizer_mod.sentencepiece.Processor.initFromPath(allocator, model_path);
    defer sp.deinit();
    try model_manager_mod.loadSentencePieceAddedTokens(model_dir, allocator, &sp);

    const token_count = sp.max_vocab_id + 1;
    if (token_count <= 0) return;

    const tokens = try allocator.alloc(gguf_mod.format.MetadataValue, @intCast(token_count));
    errdefer freeMetadataValueArray(allocator, tokens);
    const scores = try allocator.alloc(gguf_mod.format.MetadataValue, @intCast(token_count));
    errdefer freeMetadataValueArray(allocator, scores);
    const token_types = try allocator.alloc(gguf_mod.format.MetadataValue, @intCast(token_count));
    errdefer freeMetadataValueArray(allocator, token_types);

    for (tokens, 0..) |*value, idx| {
        value.* = .{ .string = try allocator.dupe(u8, "") };
        scores[idx] = .{ .f32 = 0.0 };
        token_types[idx] = .{ .i32 = 5 };
    }

    for (sp.pieces, 0..) |piece, idx| {
        const piece_index: usize = idx;
        tokens[piece_index].deinit(allocator);
        tokens[piece_index] = .{ .string = try allocator.dupe(u8, piece.text) };
        scores[piece_index] = .{ .f32 = piece.score };
        token_types[piece_index] = .{ .i32 = @intCast(@intFromEnum(piece.piece_type)) };
    }

    var extra_it = sp.extra_id_to_text.iterator();
    while (extra_it.next()) |entry| {
        const token_id = std.math.cast(usize, entry.key_ptr.*) orelse continue;
        if (token_id >= tokens.len) continue;
        tokens[token_id].deinit(allocator);
        tokens[token_id] = .{ .string = try allocator.dupe(u8, entry.value_ptr.*) };
        scores[token_id] = .{ .f32 = 0.0 };
        token_types[token_id] = .{ .i32 = 4 };
    }

    try appendMetadataArrayEntry(allocator, entries, "tokenizer.ggml.tokens", .string, tokens);
    try appendMetadataArrayEntry(allocator, entries, "tokenizer.ggml.scores", .f32, scores);
    try appendMetadataArrayEntry(allocator, entries, "tokenizer.ggml.token_type", .i32, token_types);
    try appendMetadataBool(allocator, entries, "tokenizer.ggml.add_space_prefix", sp.add_dummy_prefix);
    try appendMetadataBool(allocator, entries, "tokenizer.ggml.remove_extra_whitespaces", sp.remove_extra_whitespaces);

    const info = sp.modelInfo();
    try appendTokenizerIdMetadata(allocator, entries, "tokenizer.ggml.bos_token_id", info.bos_id);
    try appendTokenizerIdMetadata(allocator, entries, "tokenizer.ggml.eos_token_id", info.eos_id);
    try appendTokenizerIdMetadata(allocator, entries, "tokenizer.ggml.unknown_token_id", info.unk_id);
    try appendTokenizerIdMetadata(allocator, entries, "tokenizer.ggml.padding_token_id", info.pad_id);

    if (info.bos_id < 0) try appendTokenizerIdByText(allocator, entries, "tokenizer.ggml.bos_token_id", manifest.bos_token, tokens);
    if (info.eos_id < 0) try appendTokenizerIdByText(allocator, entries, "tokenizer.ggml.eos_token_id", manifest.eos_token, tokens);
    if (info.pad_id < 0) try appendTokenizerIdByText(allocator, entries, "tokenizer.ggml.padding_token_id", manifest.pad_token, tokens);
}

fn appendHfTokenizerMetadata(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    model_dir: []const u8,
) !void {
    const tokenizer_json = try c_file.readFileFromDir(allocator, model_dir, "tokenizer.json");
    defer allocator.free(tokenizer_json);

    var hf = try hf_tokenizer_mod.HfTokenizer.loadFromBytes(allocator, tokenizer_json);
    defer hf.tokenizer().deinitTokenizer();

    var max_id: i32 = -1;
    var id_it = hf.id_to_token.iterator();
    while (id_it.next()) |entry| {
        max_id = @max(max_id, entry.key_ptr.*);
    }
    if (max_id < 0) return;

    const token_count: usize = @intCast(max_id + 1);
    const tokens = try allocator.alloc(gguf_mod.format.MetadataValue, token_count);
    errdefer freeMetadataValueArray(allocator, tokens);
    const scores = try allocator.alloc(gguf_mod.format.MetadataValue, token_count);
    errdefer freeMetadataValueArray(allocator, scores);
    const token_types = try allocator.alloc(gguf_mod.format.MetadataValue, token_count);
    errdefer freeMetadataValueArray(allocator, token_types);

    for (tokens, 0..) |*value, idx| {
        value.* = .{ .string = try allocator.dupe(u8, "") };
        scores[idx] = .{ .f32 = 0.0 };
        token_types[idx] = .{ .i32 = 5 };
    }

    id_it = hf.id_to_token.iterator();
    while (id_it.next()) |entry| {
        const token_id = std.math.cast(usize, entry.key_ptr.*) orelse continue;
        if (token_id >= tokens.len) continue;
        tokens[token_id].deinit(allocator);
        tokens[token_id] = .{ .string = try allocator.dupe(u8, entry.value_ptr.*) };
        token_types[token_id] = .{ .i32 = hfTokenType(hf, entry.key_ptr.*, entry.value_ptr.*) };
    }

    try appendMetadataArrayEntry(allocator, entries, "tokenizer.ggml.tokens", .string, tokens);
    try appendMetadataArrayEntry(allocator, entries, "tokenizer.ggml.scores", .f32, scores);
    try appendMetadataArrayEntry(allocator, entries, "tokenizer.ggml.token_type", .i32, token_types);
    try appendTokenizerIdMetadata(allocator, entries, "tokenizer.ggml.bos_token_id", hf.special.cls_id);
    try appendTokenizerIdMetadata(allocator, entries, "tokenizer.ggml.eos_token_id", hf.special.sep_id);
    try appendTokenizerIdMetadata(allocator, entries, "tokenizer.ggml.unknown_token_id", hf.special.unk_id);
    try appendTokenizerIdMetadata(allocator, entries, "tokenizer.ggml.padding_token_id", hf.special.pad_id);
}

fn hfTokenType(hf: *const hf_tokenizer_mod.HfTokenizer, token_id: i32, token: []const u8) i32 {
    if (token_id == hf.special.unk_id) return 2;
    if (token_id == hf.special.cls_id or
        token_id == hf.special.sep_id or
        token_id == hf.special.pad_id or
        token_id == hf.special.mask_id)
    {
        return 3;
    }
    if (hf.added_tokens.get(token) != null) return 4;
    return 1;
}

fn appendMetadataArrayEntry(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    key: []const u8,
    element_type: gguf_mod.format.MetadataValueType,
    values: []gguf_mod.format.MetadataValue,
) !void {
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, key),
        .value = .{ .array = .{
            .element_type = element_type,
            .values = values,
        } },
    });
}

fn appendMetadataBool(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    key: []const u8,
    value: bool,
) !void {
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, key),
        .value = .{ .bool_ = value },
    });
}

fn appendTokenizerIdMetadata(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    key: []const u8,
    token_id: i32,
) !void {
    if (token_id < 0) return;
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, key),
        .value = .{ .u32 = @intCast(token_id) },
    });
}

fn appendTokenizerIdByText(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    key: []const u8,
    token_text: []const u8,
    tokens: []const gguf_mod.format.MetadataValue,
) !void {
    if (token_text.len == 0) return;
    for (tokens, 0..) |value, idx| {
        if (value != .string) continue;
        if (std.mem.eql(u8, value.string, token_text)) {
            try entries.append(allocator, .{
                .key = try allocator.dupe(u8, key),
                .value = .{ .u32 = @intCast(idx) },
            });
            return;
        }
    }
}

fn freeMetadataValueArray(allocator: std.mem.Allocator, values: []gguf_mod.format.MetadataValue) void {
    for (values) |*value| value.deinit(allocator);
    allocator.free(values);
}

fn appendMetadataU32Entry(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    key: []const u8,
    value: u32,
) !void {
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, key),
        .value = .{ .u32 = value },
    });
}

fn appendMetadataF32Entry(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    key: []const u8,
    value: f32,
) !void {
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, key),
        .value = .{ .f32 = value },
    });
}

fn appendMetadataI64Entry(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    key: []const u8,
    value: i64,
) !void {
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, key),
        .value = .{ .i64 = value },
    });
}

fn appendMetadataBoolEntry(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    key: []const u8,
    value: bool,
) !void {
    try entries.append(allocator, .{
        .key = try allocator.dupe(u8, key),
        .value = .{ .bool_ = value },
    });
}

fn appendMetadataU32ArrayEntry(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    key: []const u8,
    values_src: []const u32,
) !void {
    const values = try allocator.alloc(gguf_mod.format.MetadataValue, values_src.len);
    for (values_src, 0..) |value, idx| {
        values[idx] = .{ .u32 = value };
    }
    try appendMetadataArrayEntry(allocator, entries, key, .u32, values);
}

fn appendMetadataBoolArrayEntry(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    key: []const u8,
    values_src: []const bool,
) !void {
    const values = try allocator.alloc(gguf_mod.format.MetadataValue, values_src.len);
    for (values_src, 0..) |value, idx| {
        values[idx] = .{ .bool_ = value };
    }
    try appendMetadataArrayEntry(allocator, entries, key, .bool_, values);
}

fn appendMetadataStringArrayEntry(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    key: []const u8,
    values_src: []const []const u8,
) !void {
    const values = try allocator.alloc(gguf_mod.format.MetadataValue, values_src.len);
    var initialized: usize = 0;
    errdefer {
        for (values[0..initialized]) |*value| value.deinit(allocator);
        allocator.free(values);
    }
    for (values_src, 0..) |value, idx| {
        values[idx] = .{ .string = try allocator.dupe(u8, value) };
        initialized += 1;
    }
    try appendMetadataArrayEntry(allocator, entries, key, .string, values);
}

fn imageFeatureSourceString(source: florence_mod.ImageFeatureSource) []const u8 {
    return switch (source) {
        .spatial_avg_pool => "spatial_avg_pool",
        .temporal_avg_pool => "temporal_avg_pool",
        .last_frame => "last_frame",
    };
}

fn appendArchU32(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    arch: []const u8,
    suffix: []const u8,
    value: u32,
) !void {
    try entries.append(allocator, .{
        .key = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ arch, suffix }),
        .value = .{ .u32 = value },
    });
}

fn appendArchF32(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    arch: []const u8,
    suffix: []const u8,
    value: f32,
) !void {
    try entries.append(allocator, .{
        .key = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ arch, suffix }),
        .value = .{ .f32 = value },
    });
}

fn appendFeedForwardMetadata(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    arch: []const u8,
    config: gpt_mod.Config,
) !void {
    if (config.shared_layer_intermediate_size == 0 or config.num_hidden_layers == 0) {
        try appendArchU32(allocator, entries, arch, "feed_forward_length", config.intermediate_size);
        return;
    }

    const values = try allocator.alloc(gguf_mod.format.MetadataValue, config.num_hidden_layers);
    errdefer allocator.free(values);
    for (0..config.num_hidden_layers) |layer| {
        values[layer] = .{ .u32 = config.intermediateSize(layer) };
    }
    try entries.append(allocator, .{
        .key = try std.fmt.allocPrint(allocator, "{s}.feed_forward_length", .{arch}),
        .value = .{ .array = .{
            .element_type = .u32,
            .values = values,
        } },
    });
}

fn appendHeadCountKvMetadata(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    arch: []const u8,
    config: gpt_mod.Config,
) !void {
    if (config.num_global_key_value_heads == 0 or config.sliding_window_pattern == 0 or config.num_hidden_layers == 0) {
        try appendArchU32(allocator, entries, arch, "attention.head_count_kv", config.effectiveKVHeads());
        return;
    }

    const values = try allocator.alloc(gguf_mod.format.MetadataValue, config.num_hidden_layers);
    errdefer allocator.free(values);
    for (0..config.num_hidden_layers) |layer| {
        values[layer] = .{ .u32 = if (config.layerUsesSlidingAttention(layer)) config.effectiveKVHeads() else config.num_global_key_value_heads };
    }
    try entries.append(allocator, .{
        .key = try std.fmt.allocPrint(allocator, "{s}.attention.head_count_kv", .{arch}),
        .value = .{ .array = .{
            .element_type = .u32,
            .values = values,
        } },
    });
}

fn appendSlidingPatternMetadata(
    allocator: std.mem.Allocator,
    entries: *std.ArrayListUnmanaged(gguf_mod.format.MetadataEntry),
    arch: []const u8,
    config: gpt_mod.Config,
) !void {
    const values = try allocator.alloc(gguf_mod.format.MetadataValue, config.num_hidden_layers);
    errdefer allocator.free(values);
    for (0..config.num_hidden_layers) |layer| {
        values[layer] = .{ .bool_ = config.layerUsesSlidingAttention(layer) };
    }
    try entries.append(allocator, .{
        .key = try std.fmt.allocPrint(allocator, "{s}.attention.sliding_window_pattern", .{arch}),
        .value = .{ .array = .{
            .element_type = .bool_,
            .values = values,
        } },
    });
}

fn fullAttentionLayerIndex(config: gpt_mod.Config) ?usize {
    for (0..config.num_hidden_layers) |layer| {
        if (!config.layerUsesSlidingAttention(layer)) return layer;
    }
    return null;
}

fn tokenizerModelName(manifest: manifest_mod.ModelManifest, config: gpt_mod.Config) ?[]const u8 {
    if (manifest.tokenizer_type == null) return null;
    return switch (manifest.tokenizer_type.?) {
        .sentencepiece => switch (config.family) {
            .gemma, .llama, .mistral, .bitnet => "llama",
            else => "llama",
        },
        .huggingface => "gpt2",
    };
}

fn archStringForConfig(manifest: manifest_mod.ModelManifest, config: gpt_mod.Config) []const u8 {
    if (manifest.config_model_arch.len > 0 and gpt_mod.isGenerativeModel(manifest.config_model_arch)) {
        return manifest.config_model_arch;
    }
    return switch (config.family) {
        .llama => "llama",
        .mistral => "mistral",
        .qwen2 => "qwen2",
        .gemma => "gemma",
        .bitnet => "bitnet-b1.58",
        .phi => "phi",
        else => "llama",
    };
}

fn writeExportFile(
    allocator: std.mem.Allocator,
    output_path: []const u8,
    layout: gguf_mod.writer.Layout,
    access: tensor_access_mod.TensorAccess,
    tensors: []const TensorPlan,
) !void {
    const io = io_compat();
    if (std.fs.path.dirname(output_path)) |parent| {
        if (parent.len > 0) try compat.cwd().createDirPath(io, parent);
    }

    var file = try compat.cwd().createFile(io, output_path, .{ .truncate = true });
    defer file.close(io);

    try file.writeStreamingAll(io, layout.header_bytes);
    const data_region_offset = std.mem.alignForward(usize, layout.header_bytes.len, @intCast(layout.alignment));
    try writeZeroPadding(io, &file, data_region_offset - layout.header_bytes.len);

    var current_offset: u64 = 0;
    for (tensors, 0..) |tensor, index| {
        const target_offset = layout.offsets[index];
        if (target_offset < current_offset) return error.InvalidTensorOffset;
        try writeZeroPadding(io, &file, @intCast(target_offset - current_offset));
        var record = try access.getRecord(allocator, tensor.source_name);
        defer record.deinit();
        if (tensor.quantization != .none) {
            try writeDenseRecordQuantized(allocator, io, &file, record, tensor.quantization, tensor.transform);
        } else if (tensor.source_byte_range) |byte_range| {
            if (byte_range.start > record.raw_bytes.len or byte_range.len > record.raw_bytes.len - byte_range.start) return error.InvalidTensorShape;
            if (tensor.transform != .none) {
                try writeTransformedDenseRecordRange(allocator, io, &file, record, tensor.transform, byte_range);
            } else {
                try file.writeStreamingAll(io, record.raw_bytes[byte_range.start .. byte_range.start + byte_range.len]);
            }
        } else if (tensor.transform != .none) {
            try writeTransformedDenseRecord(allocator, io, &file, record, tensor.transform);
        } else {
            try file.writeStreamingAll(io, record.raw_bytes);
        }
        current_offset = target_offset + (gguf_mod.tensor_types.byteLen(tensor.tensor_type, tensor.dimensions) orelse return error.UnsupportedTensorType);
    }
}

fn writeZeroPadding(io: std.Io, file: anytype, count: usize) !void {
    if (count == 0) return;
    var buf: [256]u8 = @as([256]u8, @splat(0));
    var remaining = count;
    while (remaining > 0) {
        const chunk = @min(remaining, buf.len);
        try file.writeStreamingAll(io, buf[0..chunk]);
        remaining -= chunk;
    }
}

fn parseArgs(args: []const []const u8) !Options {
    if (args.len == 0) {
        printUsage();
        return error.InvalidArguments;
    }
    var opts = Options{ .model_dir = args[0] };

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--output") and i + 1 < args.len) {
            opts.output_path = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--projector-output") and i + 1 < args.len) {
            opts.projector_output_path = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--projector-format") and i + 1 < args.len) {
            opts.projector_format = parseProjectorFormat(args[i + 1]) orelse {
                printUsage();
                return error.InvalidArguments;
            };
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--quantize") and i + 1 < args.len) {
            opts.quantization = parseQuantizationMode(args[i + 1]) orelse {
                printUsage();
                return error.InvalidArguments;
            };
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--quantize-include") and i + 1 < args.len) {
            opts.quantize_include_prefixes = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--quantize-exclude") and i + 1 < args.len) {
            opts.quantize_exclude_prefixes = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--dry-run")) {
            opts.dry_run = true;
        } else {
            printUsage();
            return error.InvalidArguments;
        }
    }
    return opts;
}

fn printUsage() void {
    print(
        \\usage: antfly inference export <model-dir> --target gguf [--output <path>] [--projector-output <path>] [--projector-format auto|antfly|clip] [--format q1_0|q2_k|q3_k|q4_0|q4_1|q5_0|q5_1|q4_k|q5_k|q6_k|q8_k|q8_0|q8_1] [--quantize-include <csv-prefixes>] [--quantize-exclude <csv-prefixes>] [--dry-run]
        \\
        \\Exports a native model directory to a serialized GGUF file.
        \\Dense export is currently implemented for bert, t5, whisper, clip, clap, florence, layoutlmv3, and deberta plus gpt2, gpt_neo, gpt_neox, gptj, llama, mistral, qwen2, gemma, bitnet, and phi families. GPT-2 export applies the same hybrid quantization policy as other dense families, with Conv1D tensors quantized on their transformed GGUF row layout; GPT-NeoX split query_key_value export currently keeps those split tensors dense while other eligible tensors may quantize.
        \\Current scope: dense safetensors export, optional hybrid q1_0/q2_k/q3_k/q4_0/q4_1/q5_0/q5_1/q4_k/q5_k/q6_k/q8_k/q8_0/q8_1 quantization, prefix-based quantize include/exclude filters, dry-run planning, raw GGUF passthrough, multimodal split export with projector target selection, and GLiNER2 split-bundle export as encoder GGUF plus gliner_head.gguf. ColQwen2 currently rides the Qwen2 decoder path rather than a dedicated multimodal GGUF family.
        \\
    , .{});
}

fn parseProjectorFormat(value: []const u8) ?ProjectorFormat {
    if (std.ascii.eqlIgnoreCase(value, "auto")) return .auto;
    if (std.ascii.eqlIgnoreCase(value, "antfly")) return .antfly;
    if (std.ascii.eqlIgnoreCase(value, "clip")) return .clip;
    return null;
}

fn parseQuantizationMode(value: []const u8) ?QuantizationMode {
    if (std.ascii.eqlIgnoreCase(value, "q1_0")) return .q1_0;
    if (std.ascii.eqlIgnoreCase(value, "q2_k")) return .q2_k;
    if (std.ascii.eqlIgnoreCase(value, "q3_k")) return .q3_k;
    if (std.ascii.eqlIgnoreCase(value, "q4_0")) return .q4_0;
    if (std.ascii.eqlIgnoreCase(value, "q4_1")) return .q4_1;
    if (std.ascii.eqlIgnoreCase(value, "q5_0")) return .q5_0;
    if (std.ascii.eqlIgnoreCase(value, "q5_1")) return .q5_1;
    if (std.ascii.eqlIgnoreCase(value, "q4_k")) return .q4_k;
    if (std.ascii.eqlIgnoreCase(value, "q5_k")) return .q5_k;
    if (std.ascii.eqlIgnoreCase(value, "q6_k")) return .q6_k;
    if (std.ascii.eqlIgnoreCase(value, "q8_k")) return .q8_k;
    if (std.ascii.eqlIgnoreCase(value, "q8_0")) return .q8_0;
    if (std.ascii.eqlIgnoreCase(value, "q8_1")) return .q8_1;
    if (std.ascii.eqlIgnoreCase(value, "none")) return .none;
    return null;
}

fn writeDenseRecordQuantized(
    allocator: std.mem.Allocator,
    io: std.Io,
    file: anytype,
    record: tensor_access_mod.Record,
    quantization: QuantizationMode,
    transform: TensorTransform,
) !void {
    const row_width = quantizedRowWidth(record.descriptor.shape, transform) orelse return error.UnsupportedQuantizedTensorShape;
    const row_count = quantizedRowCount(record.descriptor.shape, transform, row_width) orelse return error.UnsupportedQuantizedTensorShape;
    if (row_width % 32 != 0) return error.UnsupportedQuantizedTensorShape;
    if (record.descriptor.encoding != .dense) return error.UnsupportedTensorType;

    const scratch = try allocator.alloc(f32, row_width);
    defer allocator.free(scratch);

    for (0..row_count) |row_index| {
        try decodeDenseRowToF32(record, transform, row_index, row_width, scratch);
        const encoded = try quantizeDenseRow(allocator, quantization, scratch);
        defer allocator.free(encoded);
        try file.writeStreamingAll(io, encoded);
    }
}

fn writeTransformedDenseRecord(
    allocator: std.mem.Allocator,
    io: std.Io,
    file: anytype,
    record: tensor_access_mod.Record,
    transform: TensorTransform,
) !void {
    switch (transform) {
        .none => try file.writeStreamingAll(io, record.raw_bytes),
        .transpose_2d_dense => try writeDenseRecordTransposed2d(allocator, io, file, record),
        .onnx_gru_zrh_to_rzn => try writeDenseRecordOnnxGruGateOrder(allocator, io, file, record.raw_bytes, try denseElementSizeForRecord(record)),
    }
}

fn writeTransformedDenseRecordRange(
    allocator: std.mem.Allocator,
    io: std.Io,
    file: anytype,
    record: tensor_access_mod.Record,
    transform: TensorTransform,
    byte_range: SourceByteRange,
) !void {
    const bytes = record.raw_bytes[byte_range.start .. byte_range.start + byte_range.len];
    switch (transform) {
        .none => try file.writeStreamingAll(io, bytes),
        .onnx_gru_zrh_to_rzn => try writeDenseRecordOnnxGruGateOrder(allocator, io, file, bytes, try denseElementSizeForRecord(record)),
        .transpose_2d_dense => return error.UnsupportedDenseArchitectureForGgufExport,
    }
}

fn denseElementSizeForRecord(record: tensor_access_mod.Record) !usize {
    const dense_dtype = switch (record.descriptor.encoding) {
        .dense => |dtype| dtype,
        else => return error.UnsupportedTensorType,
    };
    const elem_size = denseElementSize(dense_dtype);
    if (elem_size == 0) return error.UnsupportedDenseTensorTypeForGgufExport;
    return elem_size;
}

fn writeDenseRecordOnnxGruGateOrder(
    allocator: std.mem.Allocator,
    io: std.Io,
    file: anytype,
    bytes: []const u8,
    elem_size: usize,
) !void {
    const reordered = try allocator.alloc(u8, bytes.len);
    defer allocator.free(reordered);
    try reorderOnnxGruZrhToRznBytes(reordered, bytes, elem_size);
    try file.writeStreamingAll(io, reordered);
}

fn reorderOnnxGruZrhToRznBytes(output: []u8, input: []const u8, elem_size: usize) !void {
    if (elem_size == 0 or input.len != output.len or input.len % (3 * elem_size) != 0) return error.InvalidTensorShape;
    const gate_bytes = input.len / 3;
    @memcpy(output[0..gate_bytes], input[gate_bytes .. 2 * gate_bytes]);
    @memcpy(output[gate_bytes .. 2 * gate_bytes], input[0..gate_bytes]);
    @memcpy(output[2 * gate_bytes .. 3 * gate_bytes], input[2 * gate_bytes .. 3 * gate_bytes]);
}

test "gliner ONNX GRU gate transform reorders zrh to rzn" {
    const input = "zzzzrrrrhhhh".*;
    var output: [input.len]u8 = undefined;
    try reorderOnnxGruZrhToRznBytes(&output, &input, 1);
    try std.testing.expectEqualStrings("rrrrzzzzhhhh", &output);
}

fn writeDenseRecordTransposed2d(
    allocator: std.mem.Allocator,
    io: std.Io,
    file: anytype,
    record: tensor_access_mod.Record,
) !void {
    const dense_dtype = switch (record.descriptor.encoding) {
        .dense => |dtype| dtype,
        else => return error.UnsupportedTensorType,
    };
    if (record.descriptor.shape.len != 2) return error.UnsupportedTensorType;
    const rows: usize = @intCast(record.descriptor.shape[0]);
    const cols: usize = @intCast(record.descriptor.shape[1]);
    const elem_size: usize = switch (dense_dtype) {
        .f32 => 4,
        .f16, .bf16 => 2,
        else => return error.UnsupportedDenseTensorTypeForGgufExport,
    };

    const transposed = try allocator.alloc(u8, record.raw_bytes.len);
    defer allocator.free(transposed);
    for (0..rows) |r| {
        for (0..cols) |c| {
            const src_start = (r * cols + c) * elem_size;
            const dst_start = (c * rows + r) * elem_size;
            @memcpy(transposed[dst_start .. dst_start + elem_size], record.raw_bytes[src_start .. src_start + elem_size]);
        }
    }
    try file.writeStreamingAll(io, transposed);
}

fn quantizeDenseRow(allocator: std.mem.Allocator, quantization: QuantizationMode, row: []const f32) ![]u8 {
    return switch (quantization) {
        .q1_0 => gguf_mod.quant_codec.quantizeQ1_0FromF32(allocator, row),
        .q2_k => gguf_mod.quant_codec.quantizeQ2_KFromF32(allocator, row),
        .q3_k => gguf_mod.quant_codec.quantizeQ3_KFromF32(allocator, row),
        .q4_0 => gguf_mod.quant_codec.quantizeQ4_0FromF32(allocator, row),
        .q4_1 => gguf_mod.quant_codec.quantizeQ4_1FromF32(allocator, row),
        .q5_0 => gguf_mod.quant_codec.quantizeQ5_0FromF32(allocator, row),
        .q5_1 => gguf_mod.quant_codec.quantizeQ5_1FromF32(allocator, row),
        .q4_k => gguf_mod.quant_codec.quantizeQ4_KFromF32(allocator, row),
        .q5_k => gguf_mod.quant_codec.quantizeQ5_KFromF32(allocator, row),
        .q6_k => gguf_mod.quant_codec.quantizeQ6_KFromF32(allocator, row),
        .q8_k => gguf_mod.quant_codec.quantizeQ8_KFromF32(allocator, row),
        .q8_0 => gguf_mod.quant_codec.quantizeQ8_0FromF32(allocator, row),
        .q8_1 => gguf_mod.quant_codec.quantizeQ8_1FromF32(allocator, row),
        .none => error.UnsupportedTensorType,
    };
}

fn quantizedRowWidth(shape: []const i64, transform: TensorTransform) ?usize {
    if (shape.len == 0) return null;
    return switch (transform) {
        .none => blk: {
            const last_dim = shape[shape.len - 1];
            if (last_dim <= 0) return null;
            break :blk @intCast(last_dim);
        },
        .transpose_2d_dense => blk: {
            if (shape.len != 2) return null;
            const rows = shape[0];
            if (rows <= 0) return null;
            break :blk @intCast(rows);
        },
        .onnx_gru_zrh_to_rzn => null,
    };
}

fn quantizedRowCount(shape: []const i64, transform: TensorTransform, row_width: usize) ?usize {
    return switch (transform) {
        .none => blk: {
            var total: usize = 1;
            for (shape) |dim| {
                if (dim <= 0) return null;
                total = std.math.mul(usize, total, @intCast(dim)) catch return null;
            }
            if (row_width == 0 or total % row_width != 0) return null;
            break :blk total / row_width;
        },
        .transpose_2d_dense => blk: {
            if (shape.len != 2 or row_width == 0) return null;
            const rows: usize = @intCast(shape[0]);
            const cols: usize = @intCast(shape[1]);
            if (rows == 0 or cols == 0 or row_width != rows) return null;
            break :blk cols;
        },
        .onnx_gru_zrh_to_rzn => null,
    };
}

fn decodeDenseRowToF32(
    record: tensor_access_mod.Record,
    transform: TensorTransform,
    row_index: usize,
    row_width: usize,
    output: []f32,
) !void {
    std.debug.assert(output.len == row_width);
    const dense_dtype = switch (record.descriptor.encoding) {
        .dense => |dtype| dtype,
        else => return error.UnsupportedTensorType,
    };
    switch (dense_dtype) {
        .f32 => {
            try decodeDenseRowTyped(f32, record.raw_bytes, record.descriptor.shape, transform, row_index, row_width, output);
        },
        .f16 => {
            try decodeDenseRowTyped(f16, record.raw_bytes, record.descriptor.shape, transform, row_index, row_width, output);
        },
        .bf16 => {
            try decodeDenseRowTyped(u16, record.raw_bytes, record.descriptor.shape, transform, row_index, row_width, output);
        },
        else => return error.UnsupportedDenseTensorTypeForGgufExport,
    }
}

fn decodeDenseRowTyped(
    comptime T: type,
    raw_bytes: []const u8,
    shape: []const i64,
    transform: TensorTransform,
    row_index: usize,
    row_width: usize,
    output: []f32,
) !void {
    switch (transform) {
        .none => {
            const start_elem = row_index * row_width;
            for (0..row_width) |i| {
                output[i] = decodeDenseElemAsF32(T, raw_bytes, start_elem + i);
            }
        },
        .transpose_2d_dense => {
            if (shape.len != 2) return error.UnsupportedQuantizedTensorShape;
            const source_rows: usize = @intCast(shape[0]);
            const source_cols: usize = @intCast(shape[1]);
            if (source_rows == 0 or source_cols == 0) return error.UnsupportedQuantizedTensorShape;
            if (row_width != source_rows or row_index >= source_cols) return error.UnsupportedQuantizedTensorShape;
            for (0..row_width) |i| {
                output[i] = decodeDenseElemAsF32(T, raw_bytes, i * source_cols + row_index);
            }
        },
        .onnx_gru_zrh_to_rzn => return error.UnsupportedQuantizedTensorShape,
    }
}

fn decodeDenseElemAsF32(comptime T: type, raw_bytes: []const u8, elem_index: usize) f32 {
    if (T == f32) {
        const offset = elem_index * 4;
        const bits = std.mem.readInt(u32, raw_bytes[offset .. offset + 4][0..4], .little);
        return @bitCast(bits);
    }
    const offset = elem_index * 2;
    const bits: u16 = @bitCast([2]u8{ raw_bytes[offset], raw_bytes[offset + 1] });
    if (T == f16) {
        const half: f16 = @bitCast(bits);
        return @floatCast(half);
    }
    return @bitCast(@as(u32, bits) << 16);
}

test "dense export writes parseable gguf with reversed dims and mapped names" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-dense");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"llama","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"num_key_value_heads":2,"intermediate_size":8,"vocab_size":6,"max_position_embeddings":16,"rms_norm_eps":1e-5}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "model.embed_tokens.weight", .shape = &.{ 6, 4 }, .data = &[_]f32{
            0,  1,  2,  3,
            4,  5,  6,  7,
            8,  9,  10, 11,
            12, 13, 14, 15,
            16, 17, 18, 19,
            20, 21, 22, 23,
        } },
        .{ .name = "model.norm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 2, 3, 4 } },
        .{ .name = "model.layers.0.input_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "model.layers.0.self_attn.q_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            1, 0, 0, 0,
            0, 1, 0, 0,
            0, 0, 1, 0,
            0, 0, 0, 1,
        } },
        .{ .name = "model.layers.0.self_attn.k_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            2, 0, 0, 0,
            0, 2, 0, 0,
            0, 0, 2, 0,
            0, 0, 0, 2,
        } },
        .{ .name = "model.layers.0.self_attn.v_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            3, 0, 0, 0,
            0, 3, 0, 0,
            0, 0, 3, 0,
            0, 0, 0, 3,
        } },
        .{ .name = "model.layers.0.self_attn.o_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            4, 0, 0, 0,
            0, 4, 0, 0,
            0, 0, 4, 0,
            0, 0, 0, 4,
        } },
        .{ .name = "model.layers.0.post_attention_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 2, 2, 2, 2 } },
        .{ .name = "model.layers.0.mlp.gate_proj.weight", .shape = &.{ 8, 4 }, .data = &[_]f32{
            0,  1,  2,  3,
            4,  5,  6,  7,
            8,  9,  10, 11,
            12, 13, 14, 15,
            16, 17, 18, 19,
            20, 21, 22, 23,
            24, 25, 26, 27,
            28, 29, 30, 31,
        } },
        .{ .name = "model.layers.0.mlp.up_proj.weight", .shape = &.{ 8, 4 }, .data = &[_]f32{
            31, 30, 29, 28,
            27, 26, 25, 24,
            23, 22, 21, 20,
            19, 18, 17, 16,
            15, 14, 13, 12,
            11, 10, 9,  8,
            7,  6,  5,  4,
            3,  2,  1,  0,
        } },
        .{ .name = "model.layers.0.mlp.down_proj.weight", .shape = &.{ 4, 8 }, .data = &[_]f32{
            1,  2,  3,  4,  5,  6,  7,  8,
            9,  10, 11, 12, 13, 14, 15, 16,
            17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        } },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "out.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("llama", view.getString("general.architecture").?);
    try std.testing.expectEqual(@as(u64, 4), view.getU64("llama.embedding_length").?);
    const embed = gguf_mod.tensor_catalog.Catalog.init(&parsed).find("token_embd.weight").?;
    try std.testing.expectEqual(@as(u64, 4), embed.dimensions[0]);
    try std.testing.expectEqual(@as(u64, 6), embed.dimensions[1]);
}

test "gguf passthrough preserves quantized tensor type and metadata" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-passthrough");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const source_path = try std.fs.path.join(allocator, &.{ dir_path, "model.gguf" });
    defer allocator.free(source_path);
    try writeMinimalQuantizedGgufFixture(allocator, source_path);

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "copy.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    const tensor = gguf_mod.tensor_catalog.Catalog.init(&parsed).find("tok_embeddings.weight").?;
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .Q8_0 }, tensor.tensor_type);
    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("llama", view.getString("general.architecture").?);
}

test "dense export emits standalone hf tokenizer metadata arrays" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-hf-tokenizer");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"llama","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"num_key_value_heads":2,"intermediate_size":8,"vocab_size":6,"max_position_embeddings":16,"rms_norm_eps":1e-5}
        ,
    });

    const tokenizer_path = try std.fs.path.join(allocator, &.{ dir_path, "tokenizer.json" });
    defer allocator.free(tokenizer_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = tokenizer_path,
        .data =
        \\{
        \\  "version":"1.0",
        \\  "normalizer":{"type":"BertNormalizer","lowercase":true},
        \\  "pre_tokenizer":{"type":"BertPreTokenizer"},
        \\  "post_processor":{"type":"BertProcessing","sep":["[SEP]",3],"cls":["[CLS]",2]},
        \\  "added_tokens":[
        \\    {"id":0,"content":"[PAD]"},
        \\    {"id":1,"content":"[UNK]"},
        \\    {"id":2,"content":"[CLS]"},
        \\    {"id":3,"content":"[SEP]"}
        \\  ],
        \\  "model":{
        \\    "type":"WordPiece",
        \\    "unk_token":"[UNK]",
        \\    "continuing_subword_prefix":"##",
        \\    "max_input_chars_per_word":100,
        \\    "vocab":{"[PAD]":0,"[UNK]":1,"[CLS]":2,"[SEP]":3,"hello":4,"world":5}
        \\  }
        \\}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "model.embed_tokens.weight", .shape = &.{ 6, 4 }, .data = &[_]f32{
            0,  1,  2,  3,
            4,  5,  6,  7,
            8,  9,  10, 11,
            12, 13, 14, 15,
            16, 17, 18, 19,
            20, 21, 22, 23,
        } },
        .{ .name = "model.norm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 2, 3, 4 } },
        .{ .name = "model.layers.0.input_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "model.layers.0.self_attn.q_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            1, 0, 0, 0,
            0, 1, 0, 0,
            0, 0, 1, 0,
            0, 0, 0, 1,
        } },
        .{ .name = "model.layers.0.self_attn.k_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            1, 0, 0, 0,
            0, 1, 0, 0,
            0, 0, 1, 0,
            0, 0, 0, 1,
        } },
        .{ .name = "model.layers.0.self_attn.v_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            1, 0, 0, 0,
            0, 1, 0, 0,
            0, 0, 1, 0,
            0, 0, 0, 1,
        } },
        .{ .name = "model.layers.0.self_attn.o_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            1, 0, 0, 0,
            0, 1, 0, 0,
            0, 0, 1, 0,
            0, 0, 0, 1,
        } },
        .{ .name = "model.layers.0.post_attention_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 2, 2, 2, 2 } },
        .{ .name = "model.layers.0.mlp.gate_proj.weight", .shape = &.{ 8, 4 }, .data = &[_]f32{
            0,  1,  2,  3,
            4,  5,  6,  7,
            8,  9,  10, 11,
            12, 13, 14, 15,
            16, 17, 18, 19,
            20, 21, 22, 23,
            24, 25, 26, 27,
            28, 29, 30, 31,
        } },
        .{ .name = "model.layers.0.mlp.up_proj.weight", .shape = &.{ 8, 4 }, .data = &[_]f32{
            31, 30, 29, 28,
            27, 26, 25, 24,
            23, 22, 21, 20,
            19, 18, 17, 16,
            15, 14, 13, 12,
            11, 10, 9,  8,
            7,  6,  5,  4,
            3,  2,  1,  0,
        } },
        .{ .name = "model.layers.0.mlp.down_proj.weight", .shape = &.{ 4, 8 }, .data = &[_]f32{
            1,  2,  3,  4,  5,  6,  7,  8,
            9,  10, 11, 12, 13, 14, 15, 16,
            17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        } },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "tokenizer-out.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    const view = gguf_mod.metadata.View.init(&parsed);

    try std.testing.expectEqualStrings("gpt2", view.getString("tokenizer.ggml.model").?);
    try std.testing.expectEqual(@as(u64, 2), view.getU64("tokenizer.ggml.bos_token_id").?);
    try std.testing.expectEqual(@as(u64, 3), view.getU64("tokenizer.ggml.eos_token_id").?);
    try std.testing.expectEqual(@as(u64, 1), view.getU64("tokenizer.ggml.unknown_token_id").?);
    try std.testing.expectEqual(@as(u64, 0), view.getU64("tokenizer.ggml.padding_token_id").?);

    const tokens_entry = findMetadataEntry(&parsed, "tokenizer.ggml.tokens").?;
    const scores_entry = findMetadataEntry(&parsed, "tokenizer.ggml.scores").?;
    const types_entry = findMetadataEntry(&parsed, "tokenizer.ggml.token_type").?;
    try std.testing.expectEqual(@as(usize, 6), tokens_entry.value.array.values.len);
    try std.testing.expectEqual(@as(usize, 6), scores_entry.value.array.values.len);
    try std.testing.expectEqual(@as(usize, 6), types_entry.value.array.values.len);
    try std.testing.expectEqualStrings("[PAD]", tokens_entry.value.array.values[0].string);
    try std.testing.expectEqualStrings("[UNK]", tokens_entry.value.array.values[1].string);
    try std.testing.expectEqualStrings("hello", tokens_entry.value.array.values[4].string);
    try std.testing.expectEqual(@as(i32, 3), types_entry.value.array.values[0].i32);
    try std.testing.expectEqual(@as(i32, 2), types_entry.value.array.values[1].i32);
    try std.testing.expectEqual(@as(i32, 1), types_entry.value.array.values[4].i32);
}

test "dense export golden fixture round trips through gguf re-export" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-golden-roundtrip");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    try writeGoldenDenseFixture(allocator, dir_path, true);

    const first_path = try std.fs.path.join(allocator, &.{ dir_path, "first.gguf" });
    defer allocator.free(first_path);
    try exportModelDirToGgufFiltered(allocator, dir_path, first_path, .q5_k, .{});

    const first_raw = try c_file.readFile(allocator, first_path);
    defer allocator.free(first_raw);
    var first_parsed = try gguf_mod.format.parse(allocator, first_raw);
    defer first_parsed.deinit(allocator);

    const roundtrip_dir = try std.fs.path.join(allocator, &.{ dir_path, "roundtrip-dir" });
    defer allocator.free(roundtrip_dir);
    try compat.cwd().createDirPath(compat.io(), roundtrip_dir);
    const roundtrip_model_path = try std.fs.path.join(allocator, &.{ roundtrip_dir, "model.gguf" });
    defer allocator.free(roundtrip_model_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = roundtrip_model_path, .data = first_raw });

    const second_path = try std.fs.path.join(allocator, &.{ roundtrip_dir, "second.gguf" });
    defer allocator.free(second_path);
    try exportModelDirToGgufFiltered(allocator, roundtrip_dir, second_path, .none, .{});

    const second_raw = try c_file.readFile(allocator, second_path);
    defer allocator.free(second_raw);
    var second_parsed = try gguf_mod.format.parse(allocator, second_raw);
    defer second_parsed.deinit(allocator);

    const first_view = gguf_mod.metadata.View.init(&first_parsed);
    const second_view = gguf_mod.metadata.View.init(&second_parsed);
    try std.testing.expectEqualStrings("llama", first_view.getString("general.architecture").?);
    try std.testing.expectEqualStrings("llama", second_view.getString("general.architecture").?);
    try std.testing.expectEqual(@as(u64, 32), first_view.getU64("general.alignment").?);
    try std.testing.expectEqual(@as(u64, 32), second_view.getU64("general.alignment").?);
    try std.testing.expectEqual(@as(u64, 4), second_view.getU64("llama.embedding_length").?);
    try std.testing.expectEqual(@as(u64, 1), second_view.getU64("llama.block_count").?);
    try std.testing.expectEqual(@as(u64, 2), second_view.getU64("llama.attention.head_count").?);
    try std.testing.expectEqual(@as(u64, 2), second_view.getU64("llama.attention.head_count_kv").?);
    try std.testing.expectEqual(@as(u64, 8), second_view.getU64("llama.feed_forward_length").?);
    try std.testing.expectEqual(@as(u64, 16), second_view.getU64("llama.context_length").?);
    try std.testing.expectApproxEqAbs(@as(f32, 1e-5), second_view.getF32("llama.attention.layer_norm_rms_epsilon").?, 1e-8);
    try std.testing.expectEqualStrings("gpt2", second_view.getString("tokenizer.ggml.model").?);
    try std.testing.expectEqual(@as(u64, 2), second_view.getU64("tokenizer.ggml.bos_token_id").?);
    try std.testing.expectEqual(@as(u64, 3), second_view.getU64("tokenizer.ggml.eos_token_id").?);
    try std.testing.expectEqual(@as(u64, 1), second_view.getU64("tokenizer.ggml.unknown_token_id").?);
    try std.testing.expectEqual(@as(u64, 0), second_view.getU64("tokenizer.ggml.padding_token_id").?);

    const first_catalog = gguf_mod.tensor_catalog.Catalog.init(&first_parsed);
    const second_catalog = gguf_mod.tensor_catalog.Catalog.init(&second_parsed);
    const expected = [_]struct { name: []const u8, dims: []const u64 }{
        .{ .name = "token_embd.weight", .dims = &.{ 4, 6 } },
        .{ .name = "output_norm.weight", .dims = &.{4} },
        .{ .name = "blk.0.attn_norm.weight", .dims = &.{4} },
        .{ .name = "blk.0.attn_q.weight", .dims = &.{ 4, 4 } },
        .{ .name = "blk.0.attn_k.weight", .dims = &.{ 4, 4 } },
        .{ .name = "blk.0.attn_v.weight", .dims = &.{ 4, 4 } },
        .{ .name = "blk.0.attn_output.weight", .dims = &.{ 4, 4 } },
        .{ .name = "blk.0.ffn_norm.weight", .dims = &.{4} },
        .{ .name = "blk.0.ffn_gate.weight", .dims = &.{ 4, 8 } },
        .{ .name = "blk.0.ffn_up.weight", .dims = &.{ 4, 8 } },
        .{ .name = "blk.0.ffn_down.weight", .dims = &.{ 8, 4 } },
    };
    try std.testing.expectEqual(expected.len, first_parsed.tensors.len);
    try std.testing.expectEqual(expected.len, second_parsed.tensors.len);
    for (expected) |item| {
        const first_tensor = first_catalog.find(item.name).?;
        const second_tensor = second_catalog.find(item.name).?;
        try std.testing.expectEqualSlices(u64, item.dims, first_tensor.dimensions);
        try std.testing.expectEqualSlices(u64, item.dims, second_tensor.dimensions);
        try std.testing.expectEqual(first_tensor.tensor_type, second_tensor.tensor_type);

        const first_len = gguf_mod.tensor_types.byteLen(first_tensor.tensor_type, first_tensor.dimensions).?;
        const second_len = gguf_mod.tensor_types.byteLen(second_tensor.tensor_type, second_tensor.dimensions).?;
        try std.testing.expectEqual(first_len, second_len);
        const first_bytes = try c_file.readRegion(allocator, first_path, first_tensor.data_offset, @intCast(first_len));
        defer allocator.free(first_bytes);
        const second_bytes = try c_file.readRegion(allocator, second_path, second_tensor.data_offset, @intCast(second_len));
        defer allocator.free(second_bytes);
        try std.testing.expectEqualSlices(u8, first_bytes, second_bytes);
    }

    const tokens_entry = findMetadataEntry(&second_parsed, "tokenizer.ggml.tokens").?;
    try std.testing.expectEqual(@as(usize, 6), tokens_entry.value.array.values.len);
    try std.testing.expectEqualStrings("[PAD]", tokens_entry.value.array.values[0].string);
    try std.testing.expectEqualStrings("[UNK]", tokens_entry.value.array.values[1].string);
    try std.testing.expectEqualStrings("[CLS]", tokens_entry.value.array.values[2].string);
    try std.testing.expectEqualStrings("[SEP]", tokens_entry.value.array.values[3].string);
    try std.testing.expectEqualStrings("hello", tokens_entry.value.array.values[4].string);
    try std.testing.expectEqualStrings("world", tokens_entry.value.array.values[5].string);
}

test "multimodal export writes decoder gguf and preserves companion projector gguf" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-multimodal-companion");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"gemma3","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"num_key_value_heads":2,"intermediate_size":8,"vocab_size":6,"max_position_embeddings":16,"rms_norm_eps":1e-5,"image_token_index":1,"mm_tokens_per_image":2,"vision_patch_size":14,"vision_hidden_size":8}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "model.embed_tokens.weight", .shape = &.{ 6, 4 }, .data = &[_]f32{
            0,  1,  2,  3,
            4,  5,  6,  7,
            8,  9,  10, 11,
            12, 13, 14, 15,
            16, 17, 18, 19,
            20, 21, 22, 23,
        } },
        .{ .name = "model.norm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 2, 3, 4 } },
        .{ .name = "model.layers.0.input_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "model.layers.0.self_attn.q_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            1, 0, 0, 0,
            0, 1, 0, 0,
            0, 0, 1, 0,
            0, 0, 0, 1,
        } },
        .{ .name = "model.layers.0.self_attn.k_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            2, 0, 0, 0,
            0, 2, 0, 0,
            0, 0, 2, 0,
            0, 0, 0, 2,
        } },
        .{ .name = "model.layers.0.self_attn.v_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            3, 0, 0, 0,
            0, 3, 0, 0,
            0, 0, 3, 0,
            0, 0, 0, 3,
        } },
        .{ .name = "model.layers.0.self_attn.o_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            4, 0, 0, 0,
            0, 4, 0, 0,
            0, 0, 4, 0,
            0, 0, 0, 4,
        } },
        .{ .name = "model.layers.0.post_attention_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 2, 2, 2, 2 } },
        .{ .name = "model.layers.0.mlp.gate_proj.weight", .shape = &.{ 8, 4 }, .data = &[_]f32{
            0,  1,  2,  3,
            4,  5,  6,  7,
            8,  9,  10, 11,
            12, 13, 14, 15,
            16, 17, 18, 19,
            20, 21, 22, 23,
            24, 25, 26, 27,
            28, 29, 30, 31,
        } },
        .{ .name = "model.layers.0.mlp.up_proj.weight", .shape = &.{ 8, 4 }, .data = &[_]f32{
            31, 30, 29, 28,
            27, 26, 25, 24,
            23, 22, 21, 20,
            19, 18, 17, 16,
            15, 14, 13, 12,
            11, 10, 9,  8,
            7,  6,  5,  4,
            3,  2,  1,  0,
        } },
        .{ .name = "model.layers.0.mlp.down_proj.weight", .shape = &.{ 4, 8 }, .data = &[_]f32{
            1,  2,  3,  4,  5,  6,  7,  8,
            9,  10, 11, 12, 13, 14, 15, 16,
            17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        } },
        .{ .name = "vision_tower.vision_model.post_layernorm.weight", .shape = &.{8}, .data = &(@as([8]f32, @splat(1.0))) },
        .{ .name = "multi_modal_projector.mm_soft_emb_norm.weight", .shape = &.{8}, .data = &(@as([8]f32, @splat(1.0))) },
    });

    const projector_source_path = try std.fs.path.join(allocator, &.{ dir_path, "mmproj.gguf" });
    defer allocator.free(projector_source_path);
    try writeMinimalProjectorFixture(allocator, projector_source_path, .clip);

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "decoder.gguf" });
    defer allocator.free(out_path);
    const projector_out_path = try std.fs.path.join(allocator, &.{ dir_path, "copied.mmproj.gguf" });
    defer allocator.free(projector_out_path);
    try exportModelDirToGgufFilteredWithProjector(allocator, dir_path, out_path, projector_out_path, .clip, .none, .{});

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("gemma3", view.getString("general.architecture").?);
    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    try std.testing.expect(catalog.find("token_embd.weight") != null);
    try std.testing.expect(catalog.find("vision_tower.vision_model.post_layernorm.weight") == null);
    try std.testing.expect(catalog.find("multi_modal_projector.mm_soft_emb_norm.weight") == null);

    const projector_source_raw = try c_file.readFile(allocator, projector_source_path);
    defer allocator.free(projector_source_raw);
    const projector_raw = try c_file.readFile(allocator, projector_out_path);
    defer allocator.free(projector_raw);
    try std.testing.expectEqualSlices(u8, projector_source_raw, projector_raw);
}

test "multimodal export synthesizes projector gguf from integrated tensors" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-multimodal-synth-projector");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"gemma3","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"num_key_value_heads":2,"intermediate_size":8,"vocab_size":6,"max_position_embeddings":16,"rms_norm_eps":1e-5,"image_token_index":1,"mm_tokens_per_image":2,"vision_config":{"hidden_size":8,"num_hidden_layers":3,"num_attention_heads":4,"intermediate_size":16,"patch_size":14,"image_size":224}}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "model.embed_tokens.weight", .shape = &.{ 6, 4 }, .data = &[_]f32{
            0,  1,  2,  3,
            4,  5,  6,  7,
            8,  9,  10, 11,
            12, 13, 14, 15,
            16, 17, 18, 19,
            20, 21, 22, 23,
        } },
        .{ .name = "model.norm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 2, 3, 4 } },
        .{ .name = "model.layers.0.input_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "model.layers.0.self_attn.q_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            1, 0, 0, 0,
            0, 1, 0, 0,
            0, 0, 1, 0,
            0, 0, 0, 1,
        } },
        .{ .name = "model.layers.0.self_attn.k_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            2, 0, 0, 0,
            0, 2, 0, 0,
            0, 0, 2, 0,
            0, 0, 0, 2,
        } },
        .{ .name = "model.layers.0.self_attn.v_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            3, 0, 0, 0,
            0, 3, 0, 0,
            0, 0, 3, 0,
            0, 0, 0, 3,
        } },
        .{ .name = "model.layers.0.self_attn.o_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            4, 0, 0, 0,
            0, 4, 0, 0,
            0, 0, 4, 0,
            0, 0, 0, 4,
        } },
        .{ .name = "model.layers.0.post_attention_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 2, 2, 2, 2 } },
        .{ .name = "model.layers.0.mlp.gate_proj.weight", .shape = &.{ 8, 4 }, .data = &[_]f32{
            0,  1,  2,  3,
            4,  5,  6,  7,
            8,  9,  10, 11,
            12, 13, 14, 15,
            16, 17, 18, 19,
            20, 21, 22, 23,
            24, 25, 26, 27,
            28, 29, 30, 31,
        } },
        .{ .name = "model.layers.0.mlp.up_proj.weight", .shape = &.{ 8, 4 }, .data = &[_]f32{
            31, 30, 29, 28,
            27, 26, 25, 24,
            23, 22, 21, 20,
            19, 18, 17, 16,
            15, 14, 13, 12,
            11, 10, 9,  8,
            7,  6,  5,  4,
            3,  2,  1,  0,
        } },
        .{ .name = "model.layers.0.mlp.down_proj.weight", .shape = &.{ 4, 8 }, .data = &[_]f32{
            1,  2,  3,  4,  5,  6,  7,  8,
            9,  10, 11, 12, 13, 14, 15, 16,
            17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        } },
        .{ .name = "vision_tower.vision_model.post_layernorm.weight", .shape = &.{8}, .data = &(@as([8]f32, @splat(1.0))) },
        .{ .name = "multi_modal_projector.mm_soft_emb_norm.weight", .shape = &.{8}, .data = &(@as([8]f32, @splat(2.0))) },
        .{ .name = "multi_modal_projector.mm_input_projection_weight", .shape = &.{ 8, 4 }, .data = &[_]f32{
            0,  1,  2,  3,
            4,  5,  6,  7,
            8,  9,  10, 11,
            12, 13, 14, 15,
            16, 17, 18, 19,
            20, 21, 22, 23,
            24, 25, 26, 27,
            28, 29, 30, 31,
        } },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "decoder.gguf" });
    defer allocator.free(out_path);
    const projector_out_path = try std.fs.path.join(allocator, &.{ dir_path, "synth.mmproj.gguf" });
    defer allocator.free(projector_out_path);
    try exportModelDirToGgufFilteredWithProjector(allocator, dir_path, out_path, projector_out_path, .auto, .none, .{});

    const decoder_raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(decoder_raw);
    var decoder_parsed = try gguf_mod.format.parse(allocator, decoder_raw);
    defer decoder_parsed.deinit(allocator);
    const decoder_view = gguf_mod.metadata.View.init(&decoder_parsed);
    try std.testing.expectEqualStrings("gemma3", decoder_view.getString("general.architecture").?);
    const decoder_catalog = gguf_mod.tensor_catalog.Catalog.init(&decoder_parsed);
    try std.testing.expect(decoder_catalog.find("token_embd.weight") != null);
    try std.testing.expect(decoder_catalog.find("vision_tower.vision_model.post_layernorm.weight") == null);
    try std.testing.expect(decoder_catalog.find("multi_modal_projector.mm_soft_emb_norm.weight") == null);
    try std.testing.expect(decoder_catalog.find("multi_modal_projector.mm_input_projection_weight") == null);

    const projector_raw = try c_file.readFile(allocator, projector_out_path);
    defer allocator.free(projector_raw);
    var projector_parsed = try gguf_mod.format.parse(allocator, projector_raw);
    defer projector_parsed.deinit(allocator);
    const projector_view = gguf_mod.metadata.View.init(&projector_parsed);
    try std.testing.expectEqualStrings("antfly-projector", projector_view.getString("general.architecture").?);
    try std.testing.expectEqualStrings("integrated-multimodal", projector_view.getString("inference.projector.kind").?);
    try std.testing.expectEqualStrings("gemma3", projector_view.getString("inference.projector.source_architecture").?);
    try std.testing.expectEqual(@as(u64, 2), projector_view.getU64("inference.projector.mm_tokens_per_image").?);
    try std.testing.expectEqual(@as(u64, 8), projector_view.getU64("inference.projector.vision_hidden_size").?);
    try std.testing.expectEqual(@as(u64, 14), projector_view.getU64("inference.projector.vision_patch_size").?);

    const projector_catalog = gguf_mod.tensor_catalog.Catalog.init(&projector_parsed);
    const vision_ln = projector_catalog.find("vision_tower.vision_model.post_layernorm.weight").?;
    try std.testing.expectEqualSlices(u64, &.{8}, vision_ln.dimensions);
    const mm_norm = projector_catalog.find("multi_modal_projector.mm_soft_emb_norm.weight").?;
    try std.testing.expectEqualSlices(u64, &.{8}, mm_norm.dimensions);
    const mm_proj = projector_catalog.find("multi_modal_projector.mm_input_projection_weight").?;
    try std.testing.expectEqualSlices(u64, &.{ 4, 8 }, mm_proj.dimensions);
}

test "requesting clip projector export for integrated gemma3 tensors fails" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-multimodal-clip-unsupported");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"gemma3","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"num_key_value_heads":2,"intermediate_size":8,"vocab_size":6,"max_position_embeddings":16,"rms_norm_eps":1e-5,"image_token_index":1,"mm_tokens_per_image":2,"vision_config":{"hidden_size":8,"num_hidden_layers":3,"num_attention_heads":4,"intermediate_size":16,"patch_size":14,"image_size":224}}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "model.embed_tokens.weight", .shape = &.{ 6, 4 }, .data = &[_]f32{
            0,  1,  2,  3,
            4,  5,  6,  7,
            8,  9,  10, 11,
            12, 13, 14, 15,
            16, 17, 18, 19,
            20, 21, 22, 23,
        } },
        .{ .name = "model.norm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 2, 3, 4 } },
        .{ .name = "model.layers.0.input_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "model.layers.0.self_attn.q_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(1.0))) },
        .{ .name = "model.layers.0.self_attn.k_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(2.0))) },
        .{ .name = "model.layers.0.self_attn.v_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(3.0))) },
        .{ .name = "model.layers.0.self_attn.o_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(4.0))) },
        .{ .name = "model.layers.0.post_attention_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 2, 2, 2, 2 } },
        .{ .name = "model.layers.0.mlp.gate_proj.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(1.0))) },
        .{ .name = "model.layers.0.mlp.up_proj.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(2.0))) },
        .{ .name = "model.layers.0.mlp.down_proj.weight", .shape = &.{ 4, 8 }, .data = &(@as([32]f32, @splat(3.0))) },
        .{ .name = "vision_tower.vision_model.post_layernorm.weight", .shape = &.{8}, .data = &(@as([8]f32, @splat(1.0))) },
        .{ .name = "multi_modal_projector.mm_soft_emb_norm.weight", .shape = &.{8}, .data = &(@as([8]f32, @splat(2.0))) },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "decoder.gguf" });
    defer allocator.free(out_path);
    const projector_out_path = try std.fs.path.join(allocator, &.{ dir_path, "mmproj.gguf" });
    defer allocator.free(projector_out_path);
    try std.testing.expectError(
        error.UnsupportedCanonicalProjectorExport,
        exportModelDirToGgufFilteredWithProjector(allocator, dir_path, out_path, projector_out_path, .clip, .none, .{}),
    );
}

test "dense decoder export support matrix is explicit" {
    try std.testing.expectEqual(DenseDecoderExportSupport.supported, denseDecoderExportSupport(.{ .family = .llama }));
    try std.testing.expectEqual(DenseDecoderExportSupport.supported, denseDecoderExportSupport(.{ .family = .mistral }));
    try std.testing.expectEqual(DenseDecoderExportSupport.supported, denseDecoderExportSupport(.{ .family = .gpt2 }));
    try std.testing.expectEqual(DenseDecoderExportSupport.supported, denseDecoderExportSupport(.{ .family = .gpt_neo }));
    try std.testing.expectEqual(DenseDecoderExportSupport.supported, denseDecoderExportSupport(.{ .family = .gpt_neox }));
    try std.testing.expectEqual(DenseDecoderExportSupport.supported, denseDecoderExportSupport(.{ .family = .gptj }));
    try std.testing.expectEqual(DenseDecoderExportSupport.supported, denseDecoderExportSupport(.{ .family = .qwen2 }));
    try std.testing.expectEqual(DenseDecoderExportSupport.supported, denseDecoderExportSupport(.{ .family = .gemma }));
    try std.testing.expectEqual(DenseDecoderExportSupport.supported, denseDecoderExportSupport(.{ .family = .bitnet }));
    try std.testing.expectEqual(DenseDecoderExportSupport.supported, denseDecoderExportSupport(.{ .family = .phi }));
    try std.testing.expectEqual(DenseDecoderExportSupport.unsupported_name_mapping, denseDecoderExportSupport(.{ .family = .falcon }));
    try std.testing.expectEqual(DenseDecoderExportSupport.unsupported_name_mapping, denseDecoderExportSupport(.{ .family = .opt }));
    try std.testing.expectEqual(DenseDecoderExportSupport.unsupported_name_mapping, denseDecoderExportSupport(.{ .family = .bloom }));
}

test "dense deberta export writes deberta metadata and tensors" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-deberta");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"deberta-v3","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"vocab_size":16,"max_position_embeddings":32,"position_buckets":16,"num_labels":3}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "deberta.embeddings.word_embeddings.weight", .shape = &.{ 16, 4 }, .data = &(@as([(16 * 4)]f32, @splat(0.0))) },
        .{ .name = "deberta.embeddings.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "deberta.embeddings.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "deberta.encoder.rel_embeddings.weight", .shape = &.{ 32, 4 }, .data = &(@as([(32 * 4)]f32, @splat(0.0))) },
        .{ .name = "deberta.encoder.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "deberta.encoder.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "deberta.encoder.layer.0.attention.self.query_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([(4 * 4)]f32, @splat(0.0))) },
        .{ .name = "deberta.encoder.layer.0.attention.self.query_proj.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "deberta.encoder.layer.0.attention.self.key_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([(4 * 4)]f32, @splat(0.0))) },
        .{ .name = "deberta.encoder.layer.0.attention.self.key_proj.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "deberta.encoder.layer.0.attention.self.value_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([(4 * 4)]f32, @splat(0.0))) },
        .{ .name = "deberta.encoder.layer.0.attention.self.value_proj.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "deberta.encoder.layer.0.attention.output.dense.weight", .shape = &.{ 4, 4 }, .data = &(@as([(4 * 4)]f32, @splat(0.0))) },
        .{ .name = "deberta.encoder.layer.0.attention.output.dense.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "deberta.encoder.layer.0.attention.output.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "deberta.encoder.layer.0.attention.output.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "deberta.encoder.layer.0.intermediate.dense.weight", .shape = &.{ 8, 4 }, .data = &(@as([(8 * 4)]f32, @splat(0.0))) },
        .{ .name = "deberta.encoder.layer.0.intermediate.dense.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0.0))) },
        .{ .name = "deberta.encoder.layer.0.output.dense.weight", .shape = &.{ 4, 8 }, .data = &(@as([(4 * 8)]f32, @splat(0.0))) },
        .{ .name = "deberta.encoder.layer.0.output.dense.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "deberta.encoder.layer.0.output.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "deberta.encoder.layer.0.output.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "deberta.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("deberta", view.getString("general.architecture").?);
    try std.testing.expectEqual(@as(u64, 16), view.getU64("deberta.vocab_size").?);
    try std.testing.expectEqual(@as(u64, 16), view.getU64("deberta.position_buckets").?);
    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    try std.testing.expect(catalog.find("embeddings.word_embeddings.weight") != null);
    try std.testing.expect(catalog.find("encoder.layer.0.attention.self.query_proj.weight") != null);
}

test "dense bert export writes bert metadata and tensors" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-bert");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"bert","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"vocab_size":16,"max_position_embeddings":32,"type_vocab_size":2,"num_labels":3}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "bert.embeddings.word_embeddings.weight", .shape = &.{ 16, 4 }, .data = &(@as([(16 * 4)]f32, @splat(0.0))) },
        .{ .name = "bert.embeddings.position_embeddings.weight", .shape = &.{ 32, 4 }, .data = &(@as([(32 * 4)]f32, @splat(0.0))) },
        .{ .name = "bert.embeddings.token_type_embeddings.weight", .shape = &.{ 2, 4 }, .data = &(@as([8]f32, @splat(0.0))) },
        .{ .name = "bert.embeddings.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "bert.embeddings.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "bert.encoder.layer.0.attention.self.query.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "bert.encoder.layer.0.attention.self.query.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "bert.encoder.layer.0.attention.self.key.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "bert.encoder.layer.0.attention.self.key.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "bert.encoder.layer.0.attention.self.value.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "bert.encoder.layer.0.attention.self.value.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "bert.encoder.layer.0.attention.output.dense.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "bert.encoder.layer.0.attention.output.dense.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "bert.encoder.layer.0.attention.output.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "bert.encoder.layer.0.attention.output.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "bert.encoder.layer.0.intermediate.dense.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "bert.encoder.layer.0.intermediate.dense.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0.0))) },
        .{ .name = "bert.encoder.layer.0.output.dense.weight", .shape = &.{ 4, 8 }, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "bert.encoder.layer.0.output.dense.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "bert.encoder.layer.0.output.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "bert.encoder.layer.0.output.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "bert.pooler.dense.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "bert.pooler.dense.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "classifier.weight", .shape = &.{ 3, 4 }, .data = &(@as([12]f32, @splat(0.0))) },
        .{ .name = "classifier.bias", .shape = &.{3}, .data = &[_]f32{ 0, 0, 0 } },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "bert.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("bert", view.getString("general.architecture").?);
    try std.testing.expectEqualStrings("bert", view.getString("bert.family").?);
    try std.testing.expectEqual(@as(u64, 16), view.getU64("bert.vocab_size").?);
    try std.testing.expectEqual(@as(u64, 3), view.getU64("bert.label_count").?);
    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    try std.testing.expect(catalog.find("embeddings.word_embeddings.weight") != null);
    try std.testing.expect(catalog.find("encoder.layer.0.attention.self.query.weight") != null);
    try std.testing.expect(catalog.find("pooler.dense.weight") != null);
    try std.testing.expect(catalog.find("classifier.weight") != null);
}

test "dense t5 export writes t5 metadata and tensors" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-t5");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"t5","d_model":8,"d_kv":4,"d_ff":16,"num_heads":2,"num_layers":1,"num_decoder_layers":1,"relative_attention_num_buckets":32,"relative_attention_max_distance":128,"vocab_size":32,"decoder_start_token_id":0,"eos_token_id":1,"pad_token_id":0,"feed_forward_proj":"gated-gelu"}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "shared.weight", .shape = &.{ 32, 8 }, .data = &(@as([(32 * 8)]f32, @splat(0.0))) },
        .{ .name = "encoder.block.0.layer.0.SelfAttention.q.weight", .shape = &.{ 8, 8 }, .data = &(@as([64]f32, @splat(0.0))) },
        .{ .name = "encoder.block.0.layer.0.SelfAttention.relative_attention_bias.weight", .shape = &.{ 32, 2 }, .data = &(@as([64]f32, @splat(0.0))) },
        .{ .name = "encoder.final_layer_norm.weight", .shape = &.{8}, .data = &(@as([8]f32, @splat(1.0))) },
        .{ .name = "decoder.block.0.layer.0.SelfAttention.q.weight", .shape = &.{ 8, 8 }, .data = &(@as([64]f32, @splat(0.0))) },
        .{ .name = "decoder.final_layer_norm.weight", .shape = &.{8}, .data = &(@as([8]f32, @splat(1.0))) },
        .{ .name = "lm_head.weight", .shape = &.{ 32, 8 }, .data = &(@as([(32 * 8)]f32, @splat(0.0))) },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "t5.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("t5", view.getString("general.architecture").?);
    try std.testing.expectEqual(@as(u64, 8), view.getU64("t5.embedding_length").?);
    try std.testing.expectEqual(@as(u64, 1), view.getU64("t5.encoder.block_count").?);
    try std.testing.expectEqual(@as(u64, 1), view.getU64("t5.decoder.block_count").?);
    try std.testing.expect(view.getBool("t5.is_gated_act").?);
    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    try std.testing.expect(catalog.find("shared.weight") != null);
    try std.testing.expect(catalog.find("encoder.block.0.layer.0.SelfAttention.q.weight") != null);
    try std.testing.expect(catalog.find("encoder.block.0.layer.0.SelfAttention.relative_attention_bias.weight") != null);
    try std.testing.expect(catalog.find("decoder.block.0.layer.0.SelfAttention.q.weight") != null);
    try std.testing.expect(catalog.find("lm_head.weight") != null);
}

test "distilbert tensor names map to bert gguf surface" {
    const allocator = std.testing.allocator;
    const cfg = bert_mod.Config{ .model_type = .distilbert };

    const mapped_q = try mapDenseTensorNameToBertGguf(allocator, cfg, "distilbert.transformer.layer.0.attention.q_lin.weight");
    defer if (mapped_q.owned) allocator.free(mapped_q.name);
    try std.testing.expectEqualStrings("encoder.layer.0.attention.self.query.weight", mapped_q.name);

    const mapped_ln = try mapDenseTensorNameToBertGguf(allocator, cfg, "distilbert.transformer.layer.0.output_layer_norm.bias");
    defer if (mapped_ln.owned) allocator.free(mapped_ln.name);
    try std.testing.expectEqualStrings("encoder.layer.0.output.LayerNorm.bias", mapped_ln.name);
}

test "dense layoutlmv3 export writes layoutlmv3 metadata and tensors" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-layoutlmv3");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"layoutlmv3","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"vocab_size":16,"max_position_embeddings":16,"type_vocab_size":2,"max_2d_position_embeddings":16,"coordinate_size":4,"shape_size":4,"input_size":16,"patch_size":16,"num_channels":3,"num_labels":3}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "layoutlmv3.embeddings.word_embeddings.weight", .shape = &.{ 16, 4 }, .data = &(@as([(16 * 4)]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.embeddings.position_embeddings.weight", .shape = &.{ 16, 4 }, .data = &(@as([(16 * 4)]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.embeddings.token_type_embeddings.weight", .shape = &.{ 2, 4 }, .data = &(@as([8]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.embeddings.x_position_embeddings.weight", .shape = &.{ 16, 4 }, .data = &(@as([(16 * 4)]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.embeddings.y_position_embeddings.weight", .shape = &.{ 16, 4 }, .data = &(@as([(16 * 4)]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.embeddings.h_position_embeddings.weight", .shape = &.{ 16, 4 }, .data = &(@as([(16 * 4)]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.embeddings.w_position_embeddings.weight", .shape = &.{ 16, 4 }, .data = &(@as([(16 * 4)]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.embeddings.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "layoutlmv3.embeddings.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "layoutlmv3.patch_embed.proj.weight", .shape = &.{ 4, 3, 16, 16 }, .data = &(@as([(4 * 3 * 16 * 16)]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.patch_embed.proj.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "layoutlmv3.pos_embed", .shape = &.{ 2, 4 }, .data = &(@as([8]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.cls_token", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "layoutlmv3.norm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "layoutlmv3.norm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "layoutlmv3.encoder.rel_pos_bias.weight", .shape = &.{ 32, 2 }, .data = &(@as([64]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.encoder.rel_pos_x_bias.weight", .shape = &.{ 64, 2 }, .data = &(@as([128]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.encoder.rel_pos_y_bias.weight", .shape = &.{ 64, 2 }, .data = &(@as([128]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.encoder.layer.0.attention.self.query.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.encoder.layer.0.attention.self.query.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "layoutlmv3.encoder.layer.0.attention.self.key.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.encoder.layer.0.attention.self.key.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "layoutlmv3.encoder.layer.0.attention.self.value.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.encoder.layer.0.attention.self.value.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "layoutlmv3.encoder.layer.0.attention.output.dense.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.encoder.layer.0.attention.output.dense.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "layoutlmv3.encoder.layer.0.attention.output.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "layoutlmv3.encoder.layer.0.attention.output.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "layoutlmv3.encoder.layer.0.intermediate.dense.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.encoder.layer.0.intermediate.dense.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.encoder.layer.0.output.dense.weight", .shape = &.{ 4, 8 }, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "layoutlmv3.encoder.layer.0.output.dense.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "layoutlmv3.encoder.layer.0.output.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "layoutlmv3.encoder.layer.0.output.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "classifier.weight", .shape = &.{ 3, 4 }, .data = &(@as([12]f32, @splat(0.0))) },
        .{ .name = "classifier.bias", .shape = &.{3}, .data = &[_]f32{ 0, 0, 0 } },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "layoutlmv3.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("layoutlmv3", view.getString("general.architecture").?);
    try std.testing.expectEqual(@as(u64, 16), view.getU64("layoutlmv3.vocab_size").?);
    try std.testing.expectEqual(@as(u64, 16), view.getU64("layoutlmv3.max_2d_position_embeddings").?);
    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    try std.testing.expect(catalog.find("embeddings.word_embeddings.weight") != null);
    try std.testing.expect(catalog.find("patch_embed.proj.weight") != null);
    try std.testing.expect(catalog.find("encoder.layer.0.attention.self.query.weight") != null);
    try std.testing.expect(catalog.find("classifier.weight") != null);
}

test "dense clip export writes clip metadata and tensors" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-clip");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"clip","projection_dim":4,"text_config":{"hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"max_position_embeddings":16,"vocab_size":32},"vision_config":{"hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"image_size":16,"patch_size":8}}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "clip.text_model.embeddings.token_embedding.weight", .shape = &.{ 32, 4 }, .data = &(@as([(32 * 4)]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.embeddings.position_embedding.weight", .shape = &.{ 16, 4 }, .data = &(@as([(16 * 4)]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.encoder.layers.0.self_attn.q_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.encoder.layers.0.self_attn.k_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.encoder.layers.0.self_attn.v_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.encoder.layers.0.self_attn.out_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.encoder.layers.0.layer_norm1.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "clip.text_model.encoder.layers.0.layer_norm1.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clip.text_model.encoder.layers.0.mlp.fc1.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.encoder.layers.0.mlp.fc1.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.encoder.layers.0.mlp.fc2.weight", .shape = &.{ 4, 8 }, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.encoder.layers.0.mlp.fc2.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clip.text_model.encoder.layers.0.layer_norm2.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "clip.text_model.encoder.layers.0.layer_norm2.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clip.text_model.final_layer_norm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "clip.text_model.final_layer_norm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clip.text_projection.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clip.vision_model.embeddings.class_embedding", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clip.vision_model.embeddings.patch_embedding.weight", .shape = &.{ 4, 3, 8, 8 }, .data = &(@as([(4 * 3 * 8 * 8)]f32, @splat(0.0))) },
        .{ .name = "clip.vision_model.embeddings.position_embedding.weight", .shape = &.{ 5, 4 }, .data = &(@as([20]f32, @splat(0.0))) },
        .{ .name = "clip.vision_model.pre_layrnorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "clip.vision_model.pre_layrnorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clip.vision_model.encoder.layers.0.self_attn.q_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clip.vision_model.encoder.layers.0.self_attn.k_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clip.vision_model.encoder.layers.0.self_attn.v_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clip.vision_model.encoder.layers.0.self_attn.out_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clip.vision_model.encoder.layers.0.layer_norm1.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "clip.vision_model.encoder.layers.0.layer_norm1.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clip.vision_model.encoder.layers.0.mlp.fc1.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "clip.vision_model.encoder.layers.0.mlp.fc1.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0.0))) },
        .{ .name = "clip.vision_model.encoder.layers.0.mlp.fc2.weight", .shape = &.{ 4, 8 }, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "clip.vision_model.encoder.layers.0.mlp.fc2.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clip.vision_model.encoder.layers.0.layer_norm2.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "clip.vision_model.encoder.layers.0.layer_norm2.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clip.vision_model.post_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "clip.vision_model.post_layernorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clip.visual_projection.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clip.logit_scale", .shape = &.{1}, .data = &[_]f32{1.0} },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "clip.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("clip", view.getString("general.architecture").?);
    try std.testing.expectEqual(@as(u64, 32), view.getU64("clip.text.vocab_size").?);
    try std.testing.expectEqual(@as(u64, 16), view.getU64("clip.vision.image_size").?);
    try std.testing.expectEqual(@as(u64, 4), view.getU64("clip.projection_dim").?);
    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    try std.testing.expect(catalog.find("text_model.embeddings.token_embedding.weight") != null);
    try std.testing.expect(catalog.find("vision_model.embeddings.patch_embedding.weight") != null);
    try std.testing.expect(catalog.find("text_projection.weight") != null);
    try std.testing.expect(catalog.find("visual_projection.weight") != null);
    try std.testing.expect(catalog.find("logit_scale") != null);
}

test "dense siglip text export preserves siglip family metadata" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-siglip-text");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"siglip_text_model","projection_dim":4,"text_config":{"hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"max_position_embeddings":16,"vocab_size":32}}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "clip.text_model.embeddings.token_embedding.weight", .shape = &.{ 32, 4 }, .data = &(@as([(32 * 4)]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.embeddings.position_embedding.weight", .shape = &.{ 16, 4 }, .data = &(@as([(16 * 4)]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.encoder.layers.0.self_attn.q_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.encoder.layers.0.self_attn.k_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.encoder.layers.0.self_attn.v_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.encoder.layers.0.self_attn.out_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.encoder.layers.0.layer_norm1.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "clip.text_model.encoder.layers.0.layer_norm1.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clip.text_model.encoder.layers.0.mlp.fc1.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.encoder.layers.0.mlp.fc1.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.encoder.layers.0.mlp.fc2.weight", .shape = &.{ 4, 8 }, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "clip.text_model.encoder.layers.0.mlp.fc2.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clip.text_model.encoder.layers.0.layer_norm2.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "clip.text_model.encoder.layers.0.layer_norm2.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clip.text_model.final_layer_norm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "clip.text_model.final_layer_norm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clip.text_projection.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "siglip.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("clip", view.getString("general.architecture").?);
    try std.testing.expectEqualStrings("siglip", view.getString("clip.family").?);
}

test "dense whisper export writes whisper metadata and tensors" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-whisper");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"whisper","d_model":8,"encoder_layers":1,"decoder_layers":1,"encoder_attention_heads":2,"decoder_attention_heads":2,"encoder_ffn_dim":16,"decoder_ffn_dim":16,"num_mel_bins":80,"vocab_size":64,"max_source_positions":1500,"max_target_positions":448,"decoder_start_token_id":2}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "model.encoder.conv1.weight", .shape = &.{ 8, 80, 3 }, .data = &(@as([(8 * 80 * 3)]f32, @splat(0.0))) },
        .{ .name = "model.encoder.conv1.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0.0))) },
        .{ .name = "model.encoder.embed_positions.weight", .shape = &.{ 1500, 8 }, .data = &(@as([(1500 * 8)]f32, @splat(0.0))) },
        .{ .name = "model.encoder.layers.0.self_attn.q_proj.weight", .shape = &.{ 8, 8 }, .data = &(@as([64]f32, @splat(0.0))) },
        .{ .name = "model.encoder.layer_norm.weight", .shape = &.{8}, .data = &(@as([8]f32, @splat(1.0))) },
        .{ .name = "model.decoder.embed_tokens.weight", .shape = &.{ 64, 8 }, .data = &(@as([(64 * 8)]f32, @splat(0.0))) },
        .{ .name = "model.decoder.embed_positions.weight", .shape = &.{ 448, 8 }, .data = &(@as([(448 * 8)]f32, @splat(0.0))) },
        .{ .name = "model.decoder.layers.0.self_attn.q_proj.weight", .shape = &.{ 8, 8 }, .data = &(@as([64]f32, @splat(0.0))) },
        .{ .name = "model.decoder.layer_norm.weight", .shape = &.{8}, .data = &(@as([8]f32, @splat(1.0))) },
        .{ .name = "proj_out.weight", .shape = &.{ 64, 8 }, .data = &(@as([(64 * 8)]f32, @splat(0.0))) },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "whisper.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("whisper", view.getString("general.architecture").?);
    try std.testing.expectEqual(@as(u64, 8), view.getU64("whisper.embedding_length").?);
    try std.testing.expectEqual(@as(u64, 80), view.getU64("whisper.num_mel_bins").?);
    try std.testing.expectEqual(@as(i64, 2), view.getI64("whisper.decoder_start_token_id").?);
    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    try std.testing.expect(catalog.find("model.encoder.conv1.weight") != null);
    try std.testing.expect(catalog.find("model.encoder.embed_positions.weight") != null);
    try std.testing.expect(catalog.find("model.decoder.embed_tokens.weight") != null);
    try std.testing.expect(catalog.find("proj_out.weight") != null);
}

test "dense clap export writes clap metadata and tensors" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-clap");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"clap","projection_dim":4,"projection_hidden_act":"relu","logit_scale_init_value":14.285714,"text_config":{"vocab_size":32,"hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"max_position_embeddings":16,"type_vocab_size":1,"pad_token_id":1},"audio_config":{"hidden_size":8,"patch_embeds_hidden_size":4,"patch_embed_input_channels":1,"patch_size":4,"patch_stride":[4,4],"num_mel_bins":8,"spec_size":16,"window_size":4,"depths":[1,1,1,1],"num_attention_heads":[1,1,1,2],"mlp_ratio":2.0,"layer_norm_eps":1e-5,"enable_fusion":false}}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "clap.text_model.embeddings.word_embeddings.weight", .shape = &.{ 32, 4 }, .data = &(@as([(32 * 4)]f32, @splat(0.0))) },
        .{ .name = "clap.text_model.pooler.dense.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clap.text_model.pooler.dense.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clap.text_projection.linear1.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clap.text_projection.linear1.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clap.text_projection.linear2.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clap.text_projection.linear2.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clap.audio_model.audio_encoder.patch_embed.proj.weight", .shape = &.{ 4, 1, 4, 4 }, .data = &(@as([(4 * 1 * 4 * 4)]f32, @splat(0.0))) },
        .{ .name = "clap.audio_model.audio_encoder.patch_embed.proj.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clap.audio_model.audio_encoder.batch_norm.weight", .shape = &.{8}, .data = &(@as([8]f32, @splat(1.0))) },
        .{ .name = "clap.audio_model.audio_encoder.batch_norm.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0.0))) },
        .{ .name = "clap.audio_model.audio_encoder.batch_norm.running_mean", .shape = &.{8}, .data = &(@as([8]f32, @splat(0.0))) },
        .{ .name = "clap.audio_model.audio_encoder.batch_norm.running_var", .shape = &.{8}, .data = &(@as([8]f32, @splat(1.0))) },
        .{ .name = "clap.audio_model.audio_encoder.norm.weight", .shape = &.{8}, .data = &(@as([8]f32, @splat(1.0))) },
        .{ .name = "clap.audio_model.audio_encoder.norm.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0.0))) },
        .{ .name = "clap.audio_projection.linear1.weight", .shape = &.{ 4, 8 }, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "clap.audio_projection.linear1.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clap.audio_projection.linear2.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clap.audio_projection.linear2.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clap.logit_scale", .shape = &.{1}, .data = &[_]f32{1.0} },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "clap.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("clap", view.getString("general.architecture").?);
    try std.testing.expectEqual(@as(u64, 4), view.getU64("clap.projection_dim").?);
    try std.testing.expectEqual(@as(u64, 32), view.getU64("clap.text.vocab_size").?);
    try std.testing.expectEqual(@as(u64, 8), view.getU64("clap.audio.num_mel_bins").?);
    const depth_entry = findMetadataEntry(&parsed, "clap.audio.depths").?;
    try std.testing.expectEqual(@as(usize, 4), depth_entry.value.array.values.len);
    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    try std.testing.expect(catalog.find("text_model.pooler.dense.weight") != null);
    try std.testing.expect(catalog.find("audio_model.audio_encoder.patch_embed.proj.weight") != null);
    try std.testing.expect(catalog.find("audio_projection.linear1.weight") != null);
    try std.testing.expect(catalog.find("logit_scale") != null);
}

test "exported clap gguf loads and runs through native text path" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-clap-runtime-src");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const export_dir = try testScratchDir(allocator, "native-export-gguf-clap-runtime-out");
    defer {
        compat.cwd().deleteTree(compat.io(), export_dir) catch {};
        allocator.free(export_dir);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"clap","projection_dim":4,"projection_hidden_act":"relu","logit_scale_init_value":14.285714,"text_config":{"vocab_size":32,"hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"max_position_embeddings":16,"type_vocab_size":1,"pad_token_id":1},"audio_config":{"hidden_size":8,"patch_embeds_hidden_size":4,"patch_embed_input_channels":1,"patch_size":4,"patch_stride":[4,4],"num_mel_bins":8,"spec_size":16,"window_size":4,"depths":[1,1,1,1],"num_attention_heads":[1,1,1,2],"mlp_ratio":2.0,"layer_norm_eps":1e-5,"enable_fusion":false}}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "clap.text_model.embeddings.word_embeddings.weight", .shape = &.{ 32, 4 }, .data = &(@as([(32 * 4)]f32, @splat(0.0))) },
        .{ .name = "clap.text_model.embeddings.position_embeddings.weight", .shape = &.{ 16, 4 }, .data = &(@as([(16 * 4)]f32, @splat(0.0))) },
        .{ .name = "clap.text_model.embeddings.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "clap.text_model.embeddings.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clap.text_model.encoder.layer.0.attention.self.query.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clap.text_model.encoder.layer.0.attention.self.query.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clap.text_model.encoder.layer.0.attention.self.key.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clap.text_model.encoder.layer.0.attention.self.key.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clap.text_model.encoder.layer.0.attention.self.value.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clap.text_model.encoder.layer.0.attention.self.value.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clap.text_model.encoder.layer.0.attention.output.dense.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clap.text_model.encoder.layer.0.attention.output.dense.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clap.text_model.encoder.layer.0.attention.output.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "clap.text_model.encoder.layer.0.attention.output.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clap.text_model.encoder.layer.0.intermediate.dense.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "clap.text_model.encoder.layer.0.intermediate.dense.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0.0))) },
        .{ .name = "clap.text_model.encoder.layer.0.output.dense.weight", .shape = &.{ 4, 8 }, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "clap.text_model.encoder.layer.0.output.dense.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clap.text_model.encoder.layer.0.output.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "clap.text_model.encoder.layer.0.output.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clap.text_model.pooler.dense.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clap.text_model.pooler.dense.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clap.text_projection.linear1.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clap.text_projection.linear1.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "clap.text_projection.linear2.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0.0))) },
        .{ .name = "clap.text_projection.linear2.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
    });

    const out_path = try std.fs.path.join(allocator, &.{ export_dir, "model.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    var session = try session_factory_mod.createNativeSession(allocator, export_dir);
    defer session.close();
    try std.testing.expect(session_factory_mod.getClapConfig(session) != null);

    var input_ids = try backends_mod.Tensor.initInt64(allocator, "input_ids", &.{ 1, 2 }, &[_]i64{ 0, 2 });
    defer input_ids.deinit();
    var attention_mask = try backends_mod.Tensor.initInt64(allocator, "attention_mask", &.{ 1, 2 }, &[_]i64{ 1, 1 });
    defer attention_mask.deinit();

    const outputs = try session.run(&.{ input_ids, attention_mask }, allocator);
    defer {
        for (outputs) |*tensor| tensor.deinit();
        allocator.free(outputs);
    }

    try std.testing.expectEqual(@as(usize, 1), outputs.len);
    try std.testing.expectEqualStrings("text_embeds", outputs[0].name);
    try std.testing.expectEqualSlices(i64, &.{ 1, 4 }, outputs[0].shape);
}

test "clap export rejects unsupported prefixed tensor names" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.UnsupportedTensorNameForGgufExport, mapDenseTensorNameToClapGguf(allocator, "clap.text_model.not_a_real_branch.weight"));
    try std.testing.expectError(error.UnsupportedTensorNameForGgufExport, mapDenseTensorNameToClapGguf(allocator, "clap.audio_model.not_a_real_branch.weight"));
}

test "dense florence export writes florence metadata and tensors" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-florence");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"florence2","text_config":{"d_model":8,"encoder_layers":1,"decoder_layers":1,"encoder_attention_heads":2,"decoder_attention_heads":2,"encoder_ffn_dim":16,"decoder_ffn_dim":16,"vocab_size":32,"max_position_embeddings":16},"vision_config":{"image_size":32,"hidden_size":8,"patch_size":[7,3,3,3],"patch_stride":[4,2,2,2],"patch_padding":[3,1,1,1],"patch_prenorm":[false,true,true,true],"dim_embed":[8,16,24,32],"num_heads":[1,2,3,4],"num_groups":[1,2,3,4],"depths":[1,1,2,1],"window_size":12,"image_pos_embed":{"max_pos_embeddings":50},"visual_temporal_embedding":{"max_temporal_embeddings":100},"image_feature_source":["spatial_avg_pool","last_frame"]},"projection_dim":8,"image_token_id":31,"bos_token_id":2,"eos_token_id":3,"pad_token_id":1,"decoder_start_token_id":2}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "vision_tower.convs.0.proj.weight", .shape = &.{ 8, 3, 7, 7 }, .data = &(@as([(8 * 3 * 7 * 7)]f32, @splat(0.0))) },
        .{ .name = "image_projection", .shape = &.{ 8, 8 }, .data = &(@as([64]f32, @splat(0.0))) },
        .{ .name = "image_proj_norm.weight", .shape = &.{8}, .data = &(@as([8]f32, @splat(1.0))) },
        .{ .name = "image_pos_embed.row_embeddings.weight", .shape = &.{ 50, 4 }, .data = &(@as([200]f32, @splat(0.0))) },
        .{ .name = "visual_temporal_embed.weight", .shape = &.{ 100, 8 }, .data = &(@as([800]f32, @splat(0.0))) },
        .{ .name = "language_model.model.encoder.embed_positions.weight", .shape = &.{ 16, 8 }, .data = &(@as([128]f32, @splat(0.0))) },
        .{ .name = "language_model.model.decoder.embed_positions.weight", .shape = &.{ 16, 8 }, .data = &(@as([128]f32, @splat(0.0))) },
        .{ .name = "language_model.model.shared.weight", .shape = &.{ 32, 8 }, .data = &(@as([256]f32, @splat(0.0))) },
        .{ .name = "language_model.model.decoder.layers.0.self_attn.k_proj.weight", .shape = &.{ 8, 8 }, .data = &(@as([64]f32, @splat(0.0))) },
        .{ .name = "language_model.final_logits_bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "lm_head.weight", .shape = &.{ 32, 8 }, .data = &(@as([256]f32, @splat(0.0))) },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "florence.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("florence", view.getString("general.architecture").?);
    try std.testing.expectEqual(@as(u64, 8), view.getU64("florence.text.d_model").?);
    try std.testing.expectEqual(@as(u64, 32), view.getU64("florence.vision.image_size").?);
    try std.testing.expectEqual(@as(u64, 8), view.getU64("florence.projection_dim").?);
    const image_feature_source = findMetadataEntry(&parsed, "florence.vision.image_feature_source").?;
    try std.testing.expectEqual(@as(usize, 2), image_feature_source.value.array.values.len);
    try std.testing.expectEqualStrings("spatial_avg_pool", image_feature_source.value.array.values[0].string);
    try std.testing.expectEqualStrings("last_frame", image_feature_source.value.array.values[1].string);
    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    try std.testing.expect(catalog.find("vision_tower.convs.0.proj.weight") != null);
    try std.testing.expect(catalog.find("image_projection") != null);
    try std.testing.expect(catalog.find("image_pos_embed.row_embeddings.weight") != null);
    try std.testing.expect(catalog.find("language_model.model.encoder.embed_positions.weight") != null);
    try std.testing.expect(catalog.find("language_model.model.decoder.layers.0.self_attn.k_proj.weight") != null);
    try std.testing.expect(catalog.find("language_model.final_logits_bias") != null);
    try std.testing.expect(catalog.find("lm_head.weight") != null);
}

test "gliner2 export writes split encoder gguf bundle" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-gliner2-wrapper");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const export_dir = try testScratchDir(allocator, "native-export-gguf-gliner2-wrapper-out");
    defer {
        compat.cwd().deleteTree(compat.io(), export_dir) catch {};
        allocator.free(export_dir);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"extractor","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"vocab_size":16,"max_position_embeddings":16,"position_buckets":16}
        ,
    });

    const gliner_config_path = try std.fs.path.join(allocator, &.{ dir_path, "gliner_config.json" });
    defer allocator.free(gliner_config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = gliner_config_path,
        .data = "{\"model_type\":\"gliner2\",\"max_width\":4}",
    });

    const added_tokens_path = try std.fs.path.join(allocator, &.{ dir_path, "added_tokens.json" });
    defer allocator.free(added_tokens_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = added_tokens_path,
        .data = "{\"[E]\":42}",
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "encoder.embeddings.word_embeddings.weight", .shape = &.{ 16, 4 }, .data = &(@as([(16 * 4)]f32, @splat(0))) },
        .{ .name = "encoder.embeddings.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "encoder.embeddings.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "encoder.encoder.rel_embeddings.weight", .shape = &.{ 16, 4 }, .data = &(@as([(16 * 4)]f32, @splat(0))) },
        .{ .name = "encoder.encoder.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "encoder.encoder.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "encoder.encoder.layer.0.attention.self.query_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.self.query_proj.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.self.key_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.self.key_proj.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.self.value_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.self.value_proj.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.output.dense.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.output.dense.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.output.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "encoder.encoder.layer.0.attention.output.LayerNorm.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.intermediate.dense.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.intermediate.dense.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.output.dense.weight", .shape = &.{ 4, 8 }, .data = &(@as([32]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.output.dense.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.output.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "encoder.encoder.layer.0.output.LayerNorm.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "span_rep.span_rep_layer.project_start.0.weight", .shape = &.{ 16, 4 }, .data = &(@as([64]f32, @splat(0))) },
        .{ .name = "span_rep.span_rep_layer.project_start.0.bias", .shape = &.{16}, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "count_embed.pos_embedding.weight", .shape = &.{ 1, 4 }, .data = &[_]f32{ 0, 0, 0, 0 } },
    });

    const out_path = try std.fs.path.join(allocator, &.{ export_dir, "encoder.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    const encoder_bytes = try c_file.readFile(allocator, out_path);
    defer allocator.free(encoder_bytes);
    var parsed = try gguf_mod.format.parse(allocator, encoder_bytes);
    defer parsed.deinit(allocator);
    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    try std.testing.expect(catalog.find("embeddings.word_embeddings.weight") != null);
    try std.testing.expect(catalog.find("encoder.layer.0.attention.self.query_proj.weight") != null);

    const head_path = try std.fs.path.join(allocator, &.{ export_dir, "gliner_head.gguf" });
    defer allocator.free(head_path);
    try std.testing.expect(c_file.fileExists(allocator, head_path));
    const bundle_marker_path = try std.fs.path.join(allocator, &.{ export_dir, "antfly_inference_bundle.json" });
    defer allocator.free(bundle_marker_path);
    try std.testing.expect(c_file.fileExists(allocator, bundle_marker_path));

    var exported_manifest = try manifest_mod.loadFromDir(allocator, export_dir);
    defer exported_manifest.deinit();
    try std.testing.expect(exported_manifest.gguf_path != null);
    try std.testing.expect(exported_manifest.gliner_head_gguf_path != null);
    try std.testing.expectEqualStrings("gliner2", exported_manifest.gliner_model_type);
    try std.testing.expectEqualStrings("gliner2_split_bundle/v1", exported_manifest.inference_bundle_family);
}

test "gliner2 export preserves requested encoder basename in bundle marker" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-gliner2-wrapper-custom-name");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const export_dir = try testScratchDir(allocator, "native-export-gguf-gliner2-wrapper-custom-name-out");
    defer {
        compat.cwd().deleteTree(compat.io(), export_dir) catch {};
        allocator.free(export_dir);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"extractor","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"vocab_size":16,"max_position_embeddings":16,"position_buckets":16}
        ,
    });

    const gliner_config_path = try std.fs.path.join(allocator, &.{ dir_path, "gliner_config.json" });
    defer allocator.free(gliner_config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = gliner_config_path,
        .data = "{\"model_type\":\"gliner2\",\"max_width\":4}",
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "encoder.embeddings.word_embeddings.weight", .shape = &.{ 16, 4 }, .data = &(@as([(16 * 4)]f32, @splat(0))) },
        .{ .name = "encoder.embeddings.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "encoder.embeddings.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "encoder.encoder.rel_embeddings.weight", .shape = &.{ 16, 4 }, .data = &(@as([(16 * 4)]f32, @splat(0))) },
        .{ .name = "encoder.encoder.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "encoder.encoder.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "encoder.encoder.layer.0.attention.self.query_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.self.query_proj.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.self.key_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.self.key_proj.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.self.value_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.self.value_proj.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.output.dense.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.output.dense.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.output.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "encoder.encoder.layer.0.attention.output.LayerNorm.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.intermediate.dense.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.intermediate.dense.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.output.dense.weight", .shape = &.{ 4, 8 }, .data = &(@as([32]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.output.dense.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.output.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "encoder.encoder.layer.0.output.LayerNorm.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "span_rep.span_rep_layer.project_start.0.weight", .shape = &.{ 16, 4 }, .data = &(@as([64]f32, @splat(0))) },
        .{ .name = "span_rep.span_rep_layer.project_start.0.bias", .shape = &.{16}, .data = &(@as([16]f32, @splat(0))) },
    });

    const out_path = try std.fs.path.join(allocator, &.{ export_dir, "model.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    const bundle_marker_path = try std.fs.path.join(allocator, &.{ export_dir, "antfly_inference_bundle.json" });
    defer allocator.free(bundle_marker_path);
    const marker_bytes = try c_file.readFile(allocator, bundle_marker_path);
    defer allocator.free(marker_bytes);
    try std.testing.expect(std.mem.indexOf(u8, marker_bytes, "\"encoder\":\"model.gguf\"") != null);

    var exported_manifest = try manifest_mod.loadFromDir(allocator, export_dir);
    defer exported_manifest.deinit();
    try std.testing.expect(exported_manifest.gguf_path != null);
    try std.testing.expect(std.mem.endsWith(u8, exported_manifest.gguf_path.?, "model.gguf"));
    try std.testing.expect(exported_manifest.gliner_head_gguf_path != null);
}

test "gliner2 export can quantize gguf head sidecar tensors" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-gliner2-quantized-head");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const export_dir = try testScratchDir(allocator, "native-export-gguf-gliner2-quantized-head-out");
    defer {
        compat.cwd().deleteTree(compat.io(), export_dir) catch {};
        allocator.free(export_dir);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"recognizer","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"vocab_size":16,"max_position_embeddings":16,"position_buckets":16}
        ,
    });

    const gliner_config_path = try std.fs.path.join(allocator, &.{ dir_path, "gliner_config.json" });
    defer allocator.free(gliner_config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = gliner_config_path,
        .data = "{\"model_type\":\"gliner2\",\"max_width\":4,\"capabilities\":[\"extraction\"]}",
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    const head_row = [_]f32{
        0,  1,  2,  3,  4,  5,  6,  7,
        8,  9,  10, 11, 12, 13, 14, 15,
        16, 17, 18, 19, 20, 21, 22, 23,
        24, 25, 26, 27, 28, 29, 30, 31,
    };
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "encoder.embeddings.word_embeddings.weight", .shape = &.{ 16, 4 }, .data = &(@as([(16 * 4)]f32, @splat(0))) },
        .{ .name = "encoder.embeddings.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "encoder.embeddings.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "encoder.encoder.rel_embeddings.weight", .shape = &.{ 16, 4 }, .data = &(@as([(16 * 4)]f32, @splat(0))) },
        .{ .name = "encoder.encoder.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "encoder.encoder.LayerNorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 0, 0, 0 } },
        .{ .name = "encoder.encoder.layer.0.attention.self.query_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.self.query_proj.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.self.key_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.self.key_proj.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.self.value_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.self.value_proj.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.output.dense.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.output.dense.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.attention.output.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "encoder.encoder.layer.0.attention.output.LayerNorm.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.intermediate.dense.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.intermediate.dense.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.output.dense.weight", .shape = &.{ 4, 8 }, .data = &(@as([32]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.output.dense.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "encoder.encoder.layer.0.output.LayerNorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "encoder.encoder.layer.0.output.LayerNorm.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "span_rep.span_rep_layer.project_start.0.weight", .shape = &.{ 1, 32 }, .data = &head_row },
    });

    const out_path = try std.fs.path.join(allocator, &.{ export_dir, "encoder.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .q8_0);

    const head_path = try std.fs.path.join(allocator, &.{ export_dir, "gliner_head.gguf" });
    defer allocator.free(head_path);
    const head_bytes = try c_file.readFile(allocator, head_path);
    defer allocator.free(head_bytes);
    var parsed = try gguf_mod.format.parse(allocator, head_bytes);
    defer parsed.deinit(allocator);

    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    const head_tensor = catalog.find("span_rep.span_rep_layer.project_start.0.weight").?;
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .Q8_0 }, head_tensor.tensor_type);
    try std.testing.expectEqual(@as(u64, 32), head_tensor.dimensions[0]);
    try std.testing.expectEqual(@as(u64, 1), head_tensor.dimensions[1]);
}

test "colqwen bundle export keeps visual weights integrated and preserves sidecars" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-colqwen2-wrapper");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const export_dir = try testScratchDir(allocator, "native-export-gguf-colqwen2-wrapper-out");
    defer {
        compat.cwd().deleteTree(compat.io(), export_dir) catch {};
        allocator.free(export_dir);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"qwen2","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"num_key_value_heads":2,"intermediate_size":8,"vocab_size":32,"max_position_embeddings":16,"rms_norm_eps":1e-5,"image_token_index":31,"vision_start_token_id":29,"vision_end_token_id":30,"vision_config":{"hidden_size":8,"embed_dim":8,"num_hidden_layers":1,"num_attention_heads":2,"mlp_ratio":2,"patch_size":14,"spatial_merge_size":2,"temporal_patch_size":2,"image_size":224,"hidden_act":"quick_gelu"}}
        ,
    });

    const model_manifest_path = try std.fs.path.join(allocator, &.{ dir_path, "model_manifest.json" });
    defer allocator.free(model_manifest_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = model_manifest_path,
        .data = "{\"type\":\"reranker\",\"capabilities\":[\"colqwen\",\"multimodal_late_interaction\",\"late_interaction\"],\"inputs\":[\"text\",\"image\"],\"extra\":\"discard-me\"}",
    });

    const preprocessor_path = try std.fs.path.join(allocator, &.{ dir_path, "preprocessor_config.json" });
    defer allocator.free(preprocessor_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = preprocessor_path,
        .data = "{\"do_resize\":true,\"do_rescale\":true,\"do_normalize\":true,\"do_convert_rgb\":true,\"patch_size\":14,\"temporal_patch_size\":2,\"merge_size\":2,\"min_pixels\":3136,\"max_pixels\":1003520}",
    });

    const processor_path = try std.fs.path.join(allocator, &.{ dir_path, "processor_config.json" });
    defer allocator.free(processor_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = processor_path,
        .data = "{\"image_processor_type\":\"Qwen2VLImageProcessor\"}",
    });

    const chat_template_path = try std.fs.path.join(allocator, &.{ dir_path, "chat_template.jinja" });
    defer allocator.free(chat_template_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = chat_template_path,
        .data = "{{ messages }}",
    });

    const tokenizer_config_path = try std.fs.path.join(allocator, &.{ dir_path, "tokenizer_config.json" });
    defer allocator.free(tokenizer_config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = tokenizer_config_path,
        .data = "{\"model_max_length\":16,\"bos_token\":\"<s>\",\"eos_token\":\"</s>\",\"pad_token\":\"<pad>\",\"unk_token\":\"<unk>\"}",
    });

    const tokenizer_path = try std.fs.path.join(allocator, &.{ dir_path, "tokenizer.json" });
    defer allocator.free(tokenizer_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = tokenizer_path,
        .data =
        \\{"version":"1.0","added_tokens":[{"id":0,"content":"<pad>"},{"id":1,"content":"<unk>"},{"id":2,"content":"<s>"},{"id":3,"content":"</s>"}],"model":{"type":"BPE","vocab":{"<pad>":0,"<unk>":1,"<s>":2,"</s>":3,"Query":4,"image":5},"merges":[]}}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "model.embed_tokens.weight", .shape = &.{ 32, 4 }, .data = &(@as([(32 * 4)]f32, @splat(0))) },
        .{ .name = "model.norm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "model.layers.0.input_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "model.layers.0.self_attn.q_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "model.layers.0.self_attn.q_proj.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "model.layers.0.self_attn.k_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "model.layers.0.self_attn.k_proj.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "model.layers.0.self_attn.v_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "model.layers.0.self_attn.v_proj.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "model.layers.0.self_attn.o_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "model.layers.0.post_attention_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "model.layers.0.mlp.gate_proj.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(0))) },
        .{ .name = "model.layers.0.mlp.up_proj.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(0))) },
        .{ .name = "model.layers.0.mlp.down_proj.weight", .shape = &.{ 4, 8 }, .data = &(@as([32]f32, @splat(0))) },
        .{ .name = "visual.patch_embed.proj.weight", .shape = &.{ 8, 1176 }, .data = &(@as([(8 * 1176)]f32, @splat(0))) },
        .{ .name = "visual.patch_embed.proj.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.norm1.weight", .shape = &.{8}, .data = &(@as([8]f32, @splat(1))) },
        .{ .name = "visual.blocks.0.norm1.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.attn.qkv.weight", .shape = &.{ 24, 8 }, .data = &(@as([(24 * 8)]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.attn.qkv.bias", .shape = &.{24}, .data = &(@as([24]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.attn.proj.weight", .shape = &.{ 8, 8 }, .data = &(@as([64]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.attn.proj.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.norm2.weight", .shape = &.{8}, .data = &(@as([8]f32, @splat(1))) },
        .{ .name = "visual.blocks.0.norm2.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.mlp.fc1.weight", .shape = &.{ 16, 8 }, .data = &(@as([128]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.mlp.fc1.bias", .shape = &.{16}, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.mlp.fc2.weight", .shape = &.{ 8, 16 }, .data = &(@as([128]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.mlp.fc2.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0))) },
        .{ .name = "visual.merger.ln_q.weight", .shape = &.{32}, .data = &(@as([32]f32, @splat(1))) },
        .{ .name = "visual.merger.ln_q.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(0))) },
        .{ .name = "visual.merger.mlp.0.weight", .shape = &.{ 32, 32 }, .data = &(@as([(32 * 32)]f32, @splat(0))) },
        .{ .name = "visual.merger.mlp.0.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(0))) },
        .{ .name = "visual.merger.mlp.2.weight", .shape = &.{ 4, 32 }, .data = &(@as([(4 * 32)]f32, @splat(0))) },
        .{ .name = "visual.merger.mlp.2.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
    });

    const out_path = try std.fs.path.join(allocator, &.{ export_dir, "model.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("qwen2", view.getString("general.architecture").?);
    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    try std.testing.expect(catalog.find("token_embd.weight") != null);
    try std.testing.expect(catalog.find("visual.patch_embed.proj.weight") != null);
    try std.testing.expect(catalog.find("visual.merger.mlp.2.weight") != null);

    const bundle_marker_path = try std.fs.path.join(allocator, &.{ export_dir, "antfly_inference_bundle.json" });
    defer allocator.free(bundle_marker_path);
    try std.testing.expect(c_file.fileExists(allocator, bundle_marker_path));
    const exported_model_manifest_path = try std.fs.path.join(allocator, &.{ export_dir, "model_manifest.json" });
    defer allocator.free(exported_model_manifest_path);
    const exported_model_manifest_bytes = try c_file.readFile(allocator, exported_model_manifest_path);
    defer allocator.free(exported_model_manifest_bytes);
    try std.testing.expect(std.mem.indexOf(u8, exported_model_manifest_bytes, "\"extra\"") == null);

    const exported_preprocessor_path = try std.fs.path.join(allocator, &.{ export_dir, "preprocessor_config.json" });
    defer allocator.free(exported_preprocessor_path);
    try std.testing.expect(c_file.fileExists(allocator, exported_preprocessor_path));
    const exported_processor_path = try std.fs.path.join(allocator, &.{ export_dir, "processor_config.json" });
    defer allocator.free(exported_processor_path);
    try std.testing.expect(c_file.fileExists(allocator, exported_processor_path));
    const exported_chat_template_path = try std.fs.path.join(allocator, &.{ export_dir, "chat_template.jinja" });
    defer allocator.free(exported_chat_template_path);
    try std.testing.expect(!c_file.fileExists(allocator, exported_chat_template_path));

    var exported_manifest = try manifest_mod.loadFromDir(allocator, export_dir);
    defer exported_manifest.deinit();
    try std.testing.expect(exported_manifest.gguf_path != null);
    try std.testing.expect(exported_manifest.gguf_projector_path == null);
    try std.testing.expect(exported_manifest.hasCapability("colqwen"));
    try std.testing.expect(exported_manifest.hasCapability("multimodal_late_interaction"));
    try std.testing.expectEqualStrings("colqwen2_gguf_bundle/v1", exported_manifest.inference_bundle_family);
}

test "exported colqwen bundle loads and prepares multimodal reranker prompts" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-colqwen2-runtime");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const export_dir = try testScratchDir(allocator, "native-export-gguf-colqwen2-runtime-out");
    defer {
        compat.cwd().deleteTree(compat.io(), export_dir) catch {};
        allocator.free(export_dir);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"qwen2","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"num_key_value_heads":2,"intermediate_size":8,"vocab_size":32,"max_position_embeddings":32,"rms_norm_eps":1e-5,"image_token_index":31,"vision_start_token_id":29,"vision_end_token_id":30,"vision_config":{"hidden_size":8,"embed_dim":8,"num_hidden_layers":1,"num_attention_heads":2,"mlp_ratio":2,"patch_size":14,"spatial_merge_size":2,"temporal_patch_size":2,"image_size":224,"hidden_act":"quick_gelu"}}
        ,
    });

    const model_manifest_path = try std.fs.path.join(allocator, &.{ dir_path, "model_manifest.json" });
    defer allocator.free(model_manifest_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = model_manifest_path,
        .data = "{\"type\":\"reranker\",\"capabilities\":[\"colqwen\",\"multimodal_late_interaction\",\"late_interaction\"],\"inputs\":[\"text\",\"image\"]}",
    });

    const preprocessor_path = try std.fs.path.join(allocator, &.{ dir_path, "preprocessor_config.json" });
    defer allocator.free(preprocessor_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = preprocessor_path,
        .data = "{\"do_resize\":true,\"do_rescale\":true,\"do_normalize\":true,\"do_convert_rgb\":true,\"patch_size\":14,\"temporal_patch_size\":2,\"merge_size\":2,\"min_pixels\":3136,\"max_pixels\":1003520}",
    });

    const processor_path = try std.fs.path.join(allocator, &.{ dir_path, "processor_config.json" });
    defer allocator.free(processor_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = processor_path,
        .data = "{\"image_processor_type\":\"Qwen2VLImageProcessor\"}",
    });

    const tokenizer_config_path = try std.fs.path.join(allocator, &.{ dir_path, "tokenizer_config.json" });
    defer allocator.free(tokenizer_config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = tokenizer_config_path,
        .data = "{\"model_max_length\":32,\"bos_token\":\"<s>\",\"eos_token\":\"</s>\",\"pad_token\":\"<pad>\",\"unk_token\":\"<unk>\"}",
    });

    const tokenizer_path = try std.fs.path.join(allocator, &.{ dir_path, "tokenizer.json" });
    defer allocator.free(tokenizer_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = tokenizer_path,
        .data =
        \\{"version":"1.0","added_tokens":[{"id":0,"content":"<pad>"},{"id":1,"content":"<unk>"},{"id":2,"content":"<s>"},{"id":3,"content":"</s>"},{"id":29,"content":"<|vision_start|>"},{"id":30,"content":"<|vision_end|>"},{"id":31,"content":"<|image_pad|>"},{"id":32,"content":"<|im_start|>"},{"id":33,"content":"<|im_end|>"},{"id":34,"content":"<|endoftext|>"}],"model":{"type":"BPE","vocab":{"<pad>":0,"<unk>":1,"<s>":2,"</s>":3,"Query":4,"hello":5,"image":6,"Describe":7,"the":8,"document":9,"<|vision_start|>":29,"<|vision_end|>":30,"<|image_pad|>":31,"<|im_start|>":32,"<|im_end|>":33,"<|endoftext|>":34},"merges":[]}}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "model.embed_tokens.weight", .shape = &.{ 32, 4 }, .data = &(@as([(32 * 4)]f32, @splat(0))) },
        .{ .name = "model.norm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "model.layers.0.input_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "model.layers.0.self_attn.q_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "model.layers.0.self_attn.q_proj.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "model.layers.0.self_attn.k_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "model.layers.0.self_attn.k_proj.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "model.layers.0.self_attn.v_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "model.layers.0.self_attn.v_proj.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
        .{ .name = "model.layers.0.self_attn.o_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "model.layers.0.post_attention_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "model.layers.0.mlp.gate_proj.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(0))) },
        .{ .name = "model.layers.0.mlp.up_proj.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(0))) },
        .{ .name = "model.layers.0.mlp.down_proj.weight", .shape = &.{ 4, 8 }, .data = &(@as([32]f32, @splat(0))) },
        .{ .name = "visual.patch_embed.proj.weight", .shape = &.{ 8, 1176 }, .data = &(@as([(8 * 1176)]f32, @splat(0))) },
        .{ .name = "visual.patch_embed.proj.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.norm1.weight", .shape = &.{8}, .data = &(@as([8]f32, @splat(1))) },
        .{ .name = "visual.blocks.0.norm1.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.attn.qkv.weight", .shape = &.{ 24, 8 }, .data = &(@as([(24 * 8)]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.attn.qkv.bias", .shape = &.{24}, .data = &(@as([24]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.attn.proj.weight", .shape = &.{ 8, 8 }, .data = &(@as([64]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.attn.proj.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.norm2.weight", .shape = &.{8}, .data = &(@as([8]f32, @splat(1))) },
        .{ .name = "visual.blocks.0.norm2.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.mlp.fc1.weight", .shape = &.{ 16, 8 }, .data = &(@as([128]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.mlp.fc1.bias", .shape = &.{16}, .data = &(@as([16]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.mlp.fc2.weight", .shape = &.{ 8, 16 }, .data = &(@as([128]f32, @splat(0))) },
        .{ .name = "visual.blocks.0.mlp.fc2.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(0))) },
        .{ .name = "visual.merger.ln_q.weight", .shape = &.{32}, .data = &(@as([32]f32, @splat(1))) },
        .{ .name = "visual.merger.ln_q.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(0))) },
        .{ .name = "visual.merger.mlp.0.weight", .shape = &.{ 32, 32 }, .data = &(@as([(32 * 32)]f32, @splat(0))) },
        .{ .name = "visual.merger.mlp.0.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(0))) },
        .{ .name = "visual.merger.mlp.2.weight", .shape = &.{ 4, 32 }, .data = &(@as([(4 * 32)]f32, @splat(0))) },
        .{ .name = "visual.merger.mlp.2.bias", .shape = &.{4}, .data = &(@as([4]f32, @splat(0))) },
    });

    const out_path = try std.fs.path.join(allocator, &.{ export_dir, "model.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    var manager = model_manager_mod.ModelManager.init(allocator, .{
        .allocator = allocator,
        .preferred_backends = &.{.native},
    });
    defer manager.deinit();

    const model = try manager.loadFromDir(export_dir);
    try std.testing.expect(model.manifest.hasCapability("colqwen"));
    try std.testing.expect(model.manifest.hasCapability("multimodal_late_interaction"));
    try std.testing.expect(model.chat_tmpl == null);

    const gpt_cfg = session_factory_mod.getGptConfig(model.session) orelse return error.TestUnexpectedResult;
    try std.testing.expect(gpt_cfg.isMultimodal());
    const prep_cfg = try qwen2vl_multimodal_mod.loadPreprocessorConfig(allocator, export_dir);
    try std.testing.expectEqual(@as(u32, 14), prep_cfg.patch_size);

    var cb = try session_factory_mod.getComputeBackend(model.session, allocator);
    defer cb.deinit();
    var mm_pipeline = multimodal_reranker_mod.Pipeline.init(
        allocator,
        &cb,
        model.vision_session,
        model.getTokenizer(),
        gpt_cfg,
        prep_cfg,
        model.manifest.max_position_embeddings,
        model.manifest.add_bos_token,
        .{},
    );

    var query = try mm_pipeline.encodeQueryText("hello");
    defer query.deinit();
    try std.testing.expectEqual(@as(usize, model.manifest.max_position_embeddings), query.input_ids.len);
    try std.testing.expectEqual(@as(usize, model.manifest.max_position_embeddings), query.attention_mask.len);
    try std.testing.expectEqual(@as(usize, @intCast(gpt_cfg.hidden_size)), query.hidden_size);

    const fake_pixels = try allocator.alloc(f32, 0);
    defer allocator.free(fake_pixels);
    const fake_image = qwen2vl_types_mod.PreparedImage{
        .allocator = allocator,
        .pixel_values = fake_pixels,
        .resized_width = 28,
        .resized_height = 28,
        .image_grid_thw = .{ 1, 2, 2 },
        .image_token_count = 4,
    };
    var prepared = try multimodal_reranker_mod.prepareDocumentPrompt(
        allocator,
        model.getTokenizer(),
        .{},
        gpt_cfg,
        &.{fake_image},
        "document",
        model.manifest.max_position_embeddings,
        model.manifest.add_bos_token,
    );
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, model.manifest.max_position_embeddings), prepared.input_ids.len);
    try std.testing.expectEqual(@as(usize, model.manifest.max_position_embeddings), prepared.attention_mask.len);
    try std.testing.expect(std.mem.indexOfScalar(i32, prepared.input_ids, gpt_cfg.image_token_index) != null);
}

test "unsupported dense decoder family fails before tensor mapping" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-unsupported-dense-family");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"falcon","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"vocab_size":6}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "model.embed_tokens.weight", .shape = &.{ 6, 4 }, .data = &[_]f32{
            0,  1,  2,  3,
            4,  5,  6,  7,
            8,  9,  10, 11,
            12, 13, 14, 15,
            16, 17, 18, 19,
            20, 21, 22, 23,
        } },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "falcon.gguf" });
    defer allocator.free(out_path);
    try std.testing.expectError(
        error.UnsupportedDenseArchitectureForGgufExport,
        exportModelDirToGguf(allocator, dir_path, out_path, .none),
    );
}

test "unsupported dense decoder dry-run reports support status" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-unsupported-dense-family-dry-run");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"falcon","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"vocab_size":6}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "model.embed_tokens.weight", .shape = &.{ 6, 4 }, .data = &[_]f32{
            0,  1,  2,  3,
            4,  5,  6,  7,
            8,  9,  10, 11,
            12, 13, 14, 15,
            16, 17, 18, 19,
            20, 21, 22, 23,
        } },
    });

    const report = try formatUnsupportedDenseDryRunReport(allocator, dir_path, "/tmp/out.gguf", .q8_0, .{});
    defer allocator.free(report);

    try std.testing.expect(std.mem.indexOf(u8, report, "family: falcon") != null);
    try std.testing.expect(std.mem.indexOf(u8, report, "support: unsupported: dense tensor name mapping not implemented") != null);
    try std.testing.expect(std.mem.indexOf(u8, report, "plan: unavailable") != null);
}

test "dense gpt neo export maps transformer names to native gguf names" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-gpt-neo");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"gpt_neo","n_embd":32,"n_layer":1,"n_head":4,"n_positions":16,"vocab_size":8,"intermediate_size":64}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "transformer.wte.weight", .shape = &.{ 8, 32 }, .data = &(@as([256]f32, @splat(1.0))) },
        .{ .name = "transformer.wpe.weight", .shape = &.{ 16, 32 }, .data = &(@as([512]f32, @splat(2.0))) },
        .{ .name = "transformer.h.0.ln_1.weight", .shape = &.{32}, .data = &(@as([32]f32, @splat(1.0))) },
        .{ .name = "transformer.h.0.ln_1.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "transformer.h.0.attn.attention.q_proj.weight", .shape = &.{ 32, 32 }, .data = &(@as([1024]f32, @splat(3.0))) },
        .{ .name = "transformer.h.0.attn.attention.k_proj.weight", .shape = &.{ 32, 32 }, .data = &(@as([1024]f32, @splat(4.0))) },
        .{ .name = "transformer.h.0.attn.attention.v_proj.weight", .shape = &.{ 32, 32 }, .data = &(@as([1024]f32, @splat(5.0))) },
        .{ .name = "transformer.h.0.attn.attention.out_proj.weight", .shape = &.{ 32, 32 }, .data = &(@as([1024]f32, @splat(6.0))) },
        .{ .name = "transformer.h.0.ln_2.weight", .shape = &.{32}, .data = &(@as([32]f32, @splat(7.0))) },
        .{ .name = "transformer.h.0.ln_2.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "transformer.h.0.mlp.c_fc.weight", .shape = &.{ 64, 32 }, .data = &(@as([2048]f32, @splat(8.0))) },
        .{ .name = "transformer.h.0.mlp.c_fc.bias", .shape = &.{64}, .data = &(@as([64]f32, @splat(9.0))) },
        .{ .name = "transformer.h.0.mlp.c_proj.weight", .shape = &.{ 32, 64 }, .data = &(@as([2048]f32, @splat(10.0))) },
        .{ .name = "transformer.h.0.mlp.c_proj.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(11.0))) },
        .{ .name = "transformer.ln_f.weight", .shape = &.{32}, .data = &(@as([32]f32, @splat(12.0))) },
        .{ .name = "transformer.ln_f.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "lm_head.weight", .shape = &.{ 8, 32 }, .data = &(@as([256]f32, @splat(13.0))) },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "gpt-neo.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .q8_0);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);

    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("gpt_neo", view.getString("general.architecture").?);

    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    try std.testing.expect(catalog.find("transformer.h.0.attn.attention.q_proj.weight") == null);
    const q_proj = catalog.find("h.0.attn.attention.q_proj.weight").?;
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .Q8_0 }, q_proj.tensor_type);
    try std.testing.expectEqualSlices(u64, &.{ 32, 32 }, q_proj.dimensions);
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .F32 }, catalog.find("ln_f.weight").?.tensor_type);
}

test "dense gptj export maps transformer names to generic model-layer names" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-gptj");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"gptj","n_embd":32,"n_layer":1,"n_head":4,"n_positions":16,"vocab_size":8,"n_inner":64}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "transformer.wte.weight", .shape = &.{ 8, 32 }, .data = &(@as([256]f32, @splat(1.0))) },
        .{ .name = "transformer.wpe.weight", .shape = &.{ 16, 32 }, .data = &(@as([512]f32, @splat(2.0))) },
        .{ .name = "transformer.h.0.ln_1.weight", .shape = &.{32}, .data = &(@as([32]f32, @splat(1.0))) },
        .{ .name = "transformer.h.0.ln_1.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "transformer.h.0.attn.q_proj.weight", .shape = &.{ 32, 32 }, .data = &(@as([1024]f32, @splat(3.0))) },
        .{ .name = "transformer.h.0.attn.k_proj.weight", .shape = &.{ 32, 32 }, .data = &(@as([1024]f32, @splat(4.0))) },
        .{ .name = "transformer.h.0.attn.v_proj.weight", .shape = &.{ 32, 32 }, .data = &(@as([1024]f32, @splat(5.0))) },
        .{ .name = "transformer.h.0.attn.out_proj.weight", .shape = &.{ 32, 32 }, .data = &(@as([1024]f32, @splat(6.0))) },
        .{ .name = "transformer.h.0.ln_2.weight", .shape = &.{32}, .data = &(@as([32]f32, @splat(7.0))) },
        .{ .name = "transformer.h.0.ln_2.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "transformer.h.0.mlp.fc_in.weight", .shape = &.{ 64, 32 }, .data = &(@as([2048]f32, @splat(8.0))) },
        .{ .name = "transformer.h.0.mlp.fc_in.bias", .shape = &.{64}, .data = &(@as([64]f32, @splat(9.0))) },
        .{ .name = "transformer.h.0.mlp.fc_out.weight", .shape = &.{ 32, 64 }, .data = &(@as([2048]f32, @splat(10.0))) },
        .{ .name = "transformer.h.0.mlp.fc_out.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(11.0))) },
        .{ .name = "transformer.ln_f.weight", .shape = &.{32}, .data = &(@as([32]f32, @splat(12.0))) },
        .{ .name = "transformer.ln_f.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "lm_head.weight", .shape = &.{ 8, 32 }, .data = &(@as([256]f32, @splat(13.0))) },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "gptj.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .q8_0);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);

    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("gptj", view.getString("general.architecture").?);

    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    try std.testing.expect(catalog.find("transformer.h.0.attn.q_proj.weight") == null);
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .Q8_0 }, catalog.find("model.layers.0.self_attn.q_proj.weight").?.tensor_type);
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .Q8_0 }, catalog.find("model.layers.0.mlp.fc1_proj.weight").?.tensor_type);
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .F32 }, catalog.find("model.norm.weight").?.tensor_type);
}

test "dense gpt neox export splits fused query key value tensors" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-gpt-neox");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"gpt_neox","hidden_size":32,"num_hidden_layers":1,"num_attention_heads":4,"intermediate_size":64,"vocab_size":8}
        ,
    });

    const qkv = makeRangeF32(0, 3072);
    const qkv_bias = makeRangeF32(0, 96);

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "gpt_neox.embed_in.weight", .shape = &.{ 8, 32 }, .data = &(@as([256]f32, @splat(1.0))) },
        .{ .name = "gpt_neox.final_layer_norm.weight", .shape = &.{32}, .data = &(@as([32]f32, @splat(2.0))) },
        .{ .name = "gpt_neox.final_layer_norm.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "gpt_neox.layers.0.input_layernorm.weight", .shape = &.{32}, .data = &(@as([32]f32, @splat(3.0))) },
        .{ .name = "gpt_neox.layers.0.input_layernorm.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "gpt_neox.layers.0.attention.query_key_value.weight", .shape = &.{ 96, 32 }, .data = &qkv },
        .{ .name = "gpt_neox.layers.0.attention.query_key_value.bias", .shape = &.{96}, .data = &qkv_bias },
        .{ .name = "gpt_neox.layers.0.attention.dense.weight", .shape = &.{ 32, 32 }, .data = &(@as([1024]f32, @splat(4.0))) },
        .{ .name = "gpt_neox.layers.0.attention.dense.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(5.0))) },
        .{ .name = "gpt_neox.layers.0.post_attention_layernorm.weight", .shape = &.{32}, .data = &(@as([32]f32, @splat(6.0))) },
        .{ .name = "gpt_neox.layers.0.post_attention_layernorm.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "gpt_neox.layers.0.mlp.dense_h_to_4h.weight", .shape = &.{ 64, 32 }, .data = &(@as([2048]f32, @splat(7.0))) },
        .{ .name = "gpt_neox.layers.0.mlp.dense_h_to_4h.bias", .shape = &.{64}, .data = &(@as([64]f32, @splat(8.0))) },
        .{ .name = "gpt_neox.layers.0.mlp.dense_4h_to_h.weight", .shape = &.{ 32, 64 }, .data = &(@as([2048]f32, @splat(9.0))) },
        .{ .name = "gpt_neox.layers.0.mlp.dense_4h_to_h.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(10.0))) },
        .{ .name = "embed_out.weight", .shape = &.{ 8, 32 }, .data = &(@as([256]f32, @splat(11.0))) },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "gpt-neox.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .q8_0);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);

    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("gpt_neox", view.getString("general.architecture").?);

    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    try std.testing.expect(catalog.find("gpt_neox.layers.0.attention.query_key_value.weight") == null);
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .F32 }, catalog.find("model.layers.0.self_attn.q_proj.weight").?.tensor_type);
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .F32 }, catalog.find("model.layers.0.self_attn.k_proj.weight").?.tensor_type);
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .F32 }, catalog.find("model.layers.0.self_attn.v_proj.weight").?.tensor_type);
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .Q8_0 }, catalog.find("model.layers.0.self_attn.o_proj.weight").?.tensor_type);
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .F32 }, catalog.find("model.layers.0.self_attn.q_proj.bias").?.tensor_type);
}

test "dense phi export includes norm and attention bias tensors" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-phi-bias");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"phi","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"num_key_value_heads":2,"intermediate_size":8,"vocab_size":6,"max_position_embeddings":16,"layer_norm_epsilon":1e-5}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "model.embed_tokens.weight", .shape = &.{ 6, 4 }, .data = &[_]f32{
            0,  1,  2,  3,
            4,  5,  6,  7,
            8,  9,  10, 11,
            12, 13, 14, 15,
            16, 17, 18, 19,
            20, 21, 22, 23,
        } },
        .{ .name = "model.norm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 2, 3, 4 } },
        .{ .name = "model.norm.bias", .shape = &.{4}, .data = &[_]f32{ 4, 3, 2, 1 } },
        .{ .name = "model.layers.0.input_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "model.layers.0.input_layernorm.bias", .shape = &.{4}, .data = &[_]f32{ 0, 1, 0, 1 } },
        .{ .name = "model.layers.0.self_attn.q_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(1.0))) },
        .{ .name = "model.layers.0.self_attn.q_proj.bias", .shape = &.{4}, .data = &[_]f32{ 1, 2, 3, 4 } },
        .{ .name = "model.layers.0.self_attn.k_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(2.0))) },
        .{ .name = "model.layers.0.self_attn.k_proj.bias", .shape = &.{4}, .data = &[_]f32{ 4, 3, 2, 1 } },
        .{ .name = "model.layers.0.self_attn.v_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(3.0))) },
        .{ .name = "model.layers.0.self_attn.v_proj.bias", .shape = &.{4}, .data = &[_]f32{ 5, 6, 7, 8 } },
        .{ .name = "model.layers.0.self_attn.o_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(4.0))) },
        .{ .name = "model.layers.0.post_attention_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 2, 2, 2, 2 } },
        .{ .name = "model.layers.0.post_attention_layernorm.bias", .shape = &.{4}, .data = &[_]f32{ 1, 0, 1, 0 } },
        .{ .name = "model.layers.0.mlp.fc1_proj.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(1.0))) },
        .{ .name = "model.layers.0.mlp.fc2_proj.weight", .shape = &.{ 4, 8 }, .data = &(@as([32]f32, @splat(3.0))) },
        .{ .name = "lm_head.weight", .shape = &.{ 6, 4 }, .data = &(@as([24]f32, @splat(5.0))) },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "phi.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);

    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("phi", view.getString("general.architecture").?);

    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    try std.testing.expectEqualSlices(u64, &.{4}, catalog.find("output_norm.bias").?.dimensions);
    try std.testing.expectEqualSlices(u64, &.{4}, catalog.find("blk.0.attn_norm.bias").?.dimensions);
    try std.testing.expectEqualSlices(u64, &.{4}, catalog.find("blk.0.ffn_norm.bias").?.dimensions);
    try std.testing.expectEqualSlices(u64, &.{4}, catalog.find("blk.0.attn_q.bias").?.dimensions);
    try std.testing.expectEqualSlices(u64, &.{ 4, 8 }, catalog.find("blk.0.ffn_up.weight").?.dimensions);
    try std.testing.expectEqualSlices(u64, &.{ 8, 4 }, catalog.find("blk.0.ffn_down.weight").?.dimensions);
}

test "dense gpt2 export transposes conv1d weights and stays unquantized when transformed row width is ineligible" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-gpt2");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"gpt2","n_embd":4,"n_layer":1,"n_head":2,"n_positions":16,"vocab_size":6,"n_inner":8}
        ,
    });

    const c_attn = makeRangeF32(0, 48);
    const c_proj = makeRangeF32(100, 116);
    const c_fc = makeRangeF32(200, 232);
    const mlp_proj = makeRangeF32(300, 332);

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "wte.weight", .shape = &.{ 6, 4 }, .data = &(@as([24]f32, @splat(1.0))) },
        .{ .name = "wpe.weight", .shape = &.{ 16, 4 }, .data = &(@as([64]f32, @splat(2.0))) },
        .{ .name = "h.0.ln_1.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "h.0.ln_1.bias", .shape = &.{4}, .data = &[_]f32{ 0, 1, 0, 1 } },
        .{ .name = "h.0.attn.c_attn.weight", .shape = &.{ 4, 12 }, .data = &c_attn },
        .{ .name = "h.0.attn.c_attn.bias", .shape = &.{12}, .data = &(@as([12]f32, @splat(3.0))) },
        .{ .name = "h.0.attn.c_proj.weight", .shape = &.{ 4, 4 }, .data = &c_proj },
        .{ .name = "h.0.attn.c_proj.bias", .shape = &.{4}, .data = &[_]f32{ 1, 2, 3, 4 } },
        .{ .name = "h.0.ln_2.weight", .shape = &.{4}, .data = &[_]f32{ 2, 2, 2, 2 } },
        .{ .name = "h.0.ln_2.bias", .shape = &.{4}, .data = &[_]f32{ 1, 0, 1, 0 } },
        .{ .name = "h.0.mlp.c_fc.weight", .shape = &.{ 4, 8 }, .data = &c_fc },
        .{ .name = "h.0.mlp.c_fc.bias", .shape = &.{8}, .data = &(@as([8]f32, @splat(4.0))) },
        .{ .name = "h.0.mlp.c_proj.weight", .shape = &.{ 8, 4 }, .data = &mlp_proj },
        .{ .name = "h.0.mlp.c_proj.bias", .shape = &.{4}, .data = &[_]f32{ 4, 3, 2, 1 } },
        .{ .name = "ln_f.weight", .shape = &.{4}, .data = &[_]f32{ 1, 2, 3, 4 } },
        .{ .name = "ln_f.bias", .shape = &.{4}, .data = &[_]f32{ 4, 3, 2, 1 } },
        .{ .name = "lm_head.weight", .shape = &.{ 6, 4 }, .data = &(@as([24]f32, @splat(5.0))) },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "gpt2.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .q8_0);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);

    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("gpt2", view.getString("general.architecture").?);

    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    const attn_tensor = catalog.find("h.0.attn.c_attn.weight").?;
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .F32 }, attn_tensor.tensor_type);
    try std.testing.expectEqualSlices(u64, &.{ 4, 12 }, attn_tensor.dimensions);

    const byte_len: usize = @intCast(gguf_mod.tensor_types.byteLen(attn_tensor.tensor_type, attn_tensor.dimensions).?);
    const data_offset: usize = @intCast(attn_tensor.data_offset);
    const stored_bytes = raw[data_offset .. data_offset + byte_len];
    var expected: [48]f32 = undefined;
    var actual: [48]f32 = undefined;
    for (0..4) |r| {
        for (0..12) |c| {
            expected[c * 4 + r] = c_attn[r * 12 + c];
        }
    }
    for (0..actual.len) |i| {
        const bits = std.mem.readInt(u32, stored_bytes[i * 4 ..][0..4], .little);
        actual[i] = @bitCast(bits);
    }
    try std.testing.expectEqualSlices(f32, &expected, &actual);
}

test "dense gpt2 export quantizes conv1d weights after transpose when transformed rows are eligible" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-gpt2-q8");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"gpt2","n_embd":32,"n_layer":1,"n_head":4,"n_positions":16,"vocab_size":8,"n_inner":64}
        ,
    });

    const c_attn = makeRangeF32(0, 3072);

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "wte.weight", .shape = &.{ 8, 32 }, .data = &(@as([256]f32, @splat(1.0))) },
        .{ .name = "wpe.weight", .shape = &.{ 16, 32 }, .data = &(@as([512]f32, @splat(2.0))) },
        .{ .name = "h.0.ln_1.weight", .shape = &.{32}, .data = &(@as([32]f32, @splat(1.0))) },
        .{ .name = "h.0.ln_1.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "h.0.attn.c_attn.weight", .shape = &.{ 32, 96 }, .data = &c_attn },
        .{ .name = "h.0.attn.c_attn.bias", .shape = &.{96}, .data = &(@as([96]f32, @splat(3.0))) },
        .{ .name = "h.0.attn.c_proj.weight", .shape = &.{ 32, 32 }, .data = &(@as([1024]f32, @splat(4.0))) },
        .{ .name = "h.0.attn.c_proj.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(5.0))) },
        .{ .name = "h.0.ln_2.weight", .shape = &.{32}, .data = &(@as([32]f32, @splat(6.0))) },
        .{ .name = "h.0.ln_2.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "h.0.mlp.c_fc.weight", .shape = &.{ 32, 64 }, .data = &(@as([2048]f32, @splat(7.0))) },
        .{ .name = "h.0.mlp.c_fc.bias", .shape = &.{64}, .data = &(@as([64]f32, @splat(8.0))) },
        .{ .name = "h.0.mlp.c_proj.weight", .shape = &.{ 64, 32 }, .data = &(@as([2048]f32, @splat(9.0))) },
        .{ .name = "h.0.mlp.c_proj.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(10.0))) },
        .{ .name = "ln_f.weight", .shape = &.{32}, .data = &(@as([32]f32, @splat(11.0))) },
        .{ .name = "ln_f.bias", .shape = &.{32}, .data = &(@as([32]f32, @splat(0.0))) },
        .{ .name = "lm_head.weight", .shape = &.{ 8, 32 }, .data = &(@as([256]f32, @splat(12.0))) },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "gpt2-q8.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .q8_0);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);

    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    const attn_tensor = catalog.find("h.0.attn.c_attn.weight").?;
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .Q8_0 }, attn_tensor.tensor_type);
    try std.testing.expectEqualSlices(u64, &.{ 32, 96 }, attn_tensor.dimensions);
}

test "dense qwen2 export includes attention bias tensors" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-qwen2-bias");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"qwen2","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"num_key_value_heads":2,"intermediate_size":8,"vocab_size":6,"max_position_embeddings":16,"rms_norm_eps":1e-5}
        ,
    });

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "model.embed_tokens.weight", .shape = &.{ 6, 4 }, .data = &[_]f32{
            0,  1,  2,  3,
            4,  5,  6,  7,
            8,  9,  10, 11,
            12, 13, 14, 15,
            16, 17, 18, 19,
            20, 21, 22, 23,
        } },
        .{ .name = "model.norm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 2, 3, 4 } },
        .{ .name = "model.layers.0.input_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "model.layers.0.self_attn.q_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(1.0))) },
        .{ .name = "model.layers.0.self_attn.q_proj.bias", .shape = &.{4}, .data = &[_]f32{ 1, 2, 3, 4 } },
        .{ .name = "model.layers.0.self_attn.k_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(2.0))) },
        .{ .name = "model.layers.0.self_attn.k_proj.bias", .shape = &.{4}, .data = &[_]f32{ 4, 3, 2, 1 } },
        .{ .name = "model.layers.0.self_attn.v_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(3.0))) },
        .{ .name = "model.layers.0.self_attn.v_proj.bias", .shape = &.{4}, .data = &[_]f32{ 5, 6, 7, 8 } },
        .{ .name = "model.layers.0.self_attn.o_proj.weight", .shape = &.{ 4, 4 }, .data = &(@as([16]f32, @splat(4.0))) },
        .{ .name = "model.layers.0.post_attention_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 2, 2, 2, 2 } },
        .{ .name = "model.layers.0.mlp.gate_proj.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(1.0))) },
        .{ .name = "model.layers.0.mlp.up_proj.weight", .shape = &.{ 8, 4 }, .data = &(@as([32]f32, @splat(2.0))) },
        .{ .name = "model.layers.0.mlp.down_proj.weight", .shape = &.{ 4, 8 }, .data = &(@as([32]f32, @splat(3.0))) },
        .{ .name = "lm_head.weight", .shape = &.{ 6, 4 }, .data = &(@as([24]f32, @splat(5.0))) },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "qwen2.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .none);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);

    const view = gguf_mod.metadata.View.init(&parsed);
    try std.testing.expectEqualStrings("qwen2", view.getString("general.architecture").?);

    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    const q_bias = catalog.find("blk.0.attn_q.bias").?;
    try std.testing.expectEqualSlices(u64, &.{4}, q_bias.dimensions);
    const k_bias = catalog.find("blk.0.attn_k.bias").?;
    try std.testing.expectEqualSlices(u64, &.{4}, k_bias.dimensions);
    const v_bias = catalog.find("blk.0.attn_v.bias").?;
    try std.testing.expectEqualSlices(u64, &.{4}, v_bias.dimensions);
}

test "dense export can quantize compatible tensors to q8_0" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-q8_0");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"llama","hidden_size":32,"num_hidden_layers":1,"num_attention_heads":4,"num_key_value_heads":4,"intermediate_size":32,"vocab_size":32,"max_position_embeddings":16,"rms_norm_eps":1e-5}
        ,
    });

    const embed = makeRangeF32(0, 1024);
    const ones = makeFilledF32(32, 1);
    const twos = makeFilledF32(32, 2);

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "model.embed_tokens.weight", .shape = &.{ 32, 32 }, .data = &embed },
        .{ .name = "model.norm.weight", .shape = &.{32}, .data = &twos },
        .{ .name = "model.layers.0.input_layernorm.weight", .shape = &.{32}, .data = &ones },
        .{ .name = "model.layers.0.self_attn.q_proj.weight", .shape = &.{ 32, 32 }, .data = &embed },
        .{ .name = "model.layers.0.self_attn.k_proj.weight", .shape = &.{ 32, 32 }, .data = &embed },
        .{ .name = "model.layers.0.self_attn.v_proj.weight", .shape = &.{ 32, 32 }, .data = &embed },
        .{ .name = "model.layers.0.self_attn.o_proj.weight", .shape = &.{ 32, 32 }, .data = &embed },
        .{ .name = "model.layers.0.post_attention_layernorm.weight", .shape = &.{32}, .data = &twos },
        .{ .name = "model.layers.0.mlp.gate_proj.weight", .shape = &.{ 32, 32 }, .data = &embed },
        .{ .name = "model.layers.0.mlp.up_proj.weight", .shape = &.{ 32, 32 }, .data = &embed },
        .{ .name = "model.layers.0.mlp.down_proj.weight", .shape = &.{ 32, 32 }, .data = &embed },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "q8_0.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, .q8_0);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);

    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    const embed_tensor = catalog.find("token_embd.weight").?;
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .Q8_0 }, embed_tensor.tensor_type);
    try std.testing.expectEqual(@as(u64, 32), embed_tensor.dimensions[0]);
    try std.testing.expectEqual(@as(u64, 32), embed_tensor.dimensions[1]);

    const embed_len = gguf_mod.tensor_types.byteLen(embed_tensor.tensor_type, embed_tensor.dimensions).?;
    const embed_bytes = try c_file.readRegion(allocator, out_path, embed_tensor.data_offset, @intCast(embed_len));
    defer allocator.free(embed_bytes);
    var dense: [32 * 32]f32 = undefined;
    try gguf_mod.quant_codec.dequantizeToFloat32(.{ .known = .Q8_0 }, embed_bytes, &dense);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), dense[0], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), dense[1], 1.0);

    const norm_tensor = catalog.find("output_norm.weight").?;
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .F32 }, norm_tensor.tensor_type);
}

test "dense export can quantize compatible tensors to q4_0" {
    try expectDenseExportQuantization(std.testing.allocator, "native-export-gguf-q4_0", .q4_0, .{ .known = .Q4_0 });
}

test "dense export can quantize compatible tensors to q5_0" {
    try expectDenseExportQuantization(std.testing.allocator, "native-export-gguf-q5_0", .q5_0, .{ .known = .Q5_0 });
}

test "dense export can quantize compatible tensors to q2_k" {
    try expectDenseExportQuantization(std.testing.allocator, "native-export-gguf-q2_k", .q2_k, .{ .known = .Q2_K });
}

test "dense export can quantize compatible tensors to q3_k" {
    try expectDenseExportQuantization(std.testing.allocator, "native-export-gguf-q3_k", .q3_k, .{ .known = .Q3_K });
}

test "dense export can quantize compatible tensors to q1_0" {
    try expectDenseExportQuantization(std.testing.allocator, "native-export-gguf-q1_0", .q1_0, .{ .known = .Q1_0 });
}

test "dense export can quantize compatible tensors to q4_k" {
    try expectDenseExportQuantization(std.testing.allocator, "native-export-gguf-q4_k", .q4_k, .{ .known = .Q4_K });
}

test "dense export can quantize compatible tensors to q4_1" {
    try expectDenseExportQuantization(std.testing.allocator, "native-export-gguf-q4_1", .q4_1, .{ .known = .Q4_1 });
}

test "dense export can quantize compatible tensors to q5_k" {
    try expectDenseExportQuantization(std.testing.allocator, "native-export-gguf-q5_k", .q5_k, .{ .known = .Q5_K });
}

test "dense export can quantize compatible tensors to q5_1" {
    try expectDenseExportQuantization(std.testing.allocator, "native-export-gguf-q5_1", .q5_1, .{ .known = .Q5_1 });
}

test "dense export can quantize compatible tensors to q6_k" {
    try expectDenseExportQuantization(std.testing.allocator, "native-export-gguf-q6_k", .q6_k, .{ .known = .Q6_K });
}

test "dense export can quantize compatible tensors to q8_k" {
    try expectDenseExportQuantization(std.testing.allocator, "native-export-gguf-q8_k", .q8_k, .{ .known = .Q8_K });
}

test "dense export can quantize compatible tensors to q8_1" {
    try expectDenseExportQuantization(std.testing.allocator, "native-export-gguf-q8_1", .q8_1, .{ .known = .Q8_1 });
}

test "256-wide quantization export quantizes wide tensors and leaves narrow tensors dense" {
    try expectMixedKFamilyExport(std.testing.allocator, "native-export-gguf-q2_k-mixed", .q2_k, .{ .known = .Q2_K });
    try expectMixedKFamilyExport(std.testing.allocator, "native-export-gguf-q3_k-mixed", .q3_k, .{ .known = .Q3_K });
    try expectMixedKFamilyExport(std.testing.allocator, "native-export-gguf-q4_k-mixed", .q4_k, .{ .known = .Q4_K });
    try expectMixedKFamilyExport(std.testing.allocator, "native-export-gguf-q5_k-mixed", .q5_k, .{ .known = .Q5_K });
    try expectMixedKFamilyExport(std.testing.allocator, "native-export-gguf-q6_k-mixed", .q6_k, .{ .known = .Q6_K });
    try expectMixedKFamilyExport(std.testing.allocator, "native-export-gguf-q8_k-mixed", .q8_k, .{ .known = .Q8_K });
}

test "quantization include filter limits quantization to matching prefixes" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-q5k-include");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    try writeWideQuantFixture(allocator, dir_path);
    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "filtered.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGgufFiltered(allocator, dir_path, out_path, .q5_k, .{ .include_prefixes_csv = "model.embed_tokens" });

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .Q5_K }, catalog.find("token_embd.weight").?.tensor_type);
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .F32 }, catalog.find("blk.0.attn_q.weight").?.tensor_type);
}

test "quantization exclude filter keeps matching prefixes dense" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "native-export-gguf-q5k-exclude");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    try writeWideQuantFixture(allocator, dir_path);
    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "filtered.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGgufFiltered(allocator, dir_path, out_path, .q5_k, .{ .exclude_prefixes_csv = "model.embed_tokens" });

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);
    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .F32 }, catalog.find("token_embd.weight").?.tensor_type);
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .Q5_K }, catalog.find("blk.0.attn_q.weight").?.tensor_type);
}

test "k-family quantization falls back to dense when width is not divisible by 256" {
    const shape = [_]i64{ 64, 96 };
    const descriptor: tensor_access_mod.Descriptor = .{
        .name = "model.embed_tokens.weight",
        .shape = &shape,
        .encoding = .{ .dense = .f32 },
        .byte_len = shape[0] * shape[1] * @sizeOf(f32),
        .quantized = false,
    };

    try std.testing.expectEqual(
        QuantizationMode.none,
        supportedQuantizationForDescriptor(false, .q1_0, descriptor, .none),
    );
    try std.testing.expectEqual(
        QuantizationMode.none,
        supportedQuantizationForDescriptor(false, .q2_k, descriptor, .none),
    );
    try std.testing.expectEqual(
        QuantizationMode.none,
        supportedQuantizationForDescriptor(false, .q3_k, descriptor, .none),
    );
    try std.testing.expectEqual(
        QuantizationMode.none,
        supportedQuantizationForDescriptor(false, .q4_k, descriptor, .none),
    );
    try std.testing.expectEqual(
        QuantizationMode.none,
        supportedQuantizationForDescriptor(false, .q5_k, descriptor, .none),
    );
    try std.testing.expectEqual(
        QuantizationMode.none,
        supportedQuantizationForDescriptor(false, .q6_k, descriptor, .none),
    );
    try std.testing.expectEqual(
        QuantizationMode.none,
        supportedQuantizationForDescriptor(false, .q8_k, descriptor, .none),
    );
    try std.testing.expectEqual(
        QuantizationMode.q4_1,
        supportedQuantizationForDescriptor(false, .q4_1, descriptor, .none),
    );
    try std.testing.expectEqual(
        QuantizationMode.q5_1,
        supportedQuantizationForDescriptor(false, .q5_1, descriptor, .none),
    );
    try std.testing.expectEqual(
        QuantizationMode.q8_1,
        supportedQuantizationForDescriptor(false, .q8_1, descriptor, .none),
    );
}

test "parseQuantizationMode accepts q5_k" {
    try std.testing.expectEqual(QuantizationMode.q5_k, parseQuantizationMode("q5_k").?);
}

test "parseQuantizationMode accepts q2_k and q8_k" {
    try std.testing.expectEqual(QuantizationMode.q2_k, parseQuantizationMode("q2_k").?);
    try std.testing.expectEqual(QuantizationMode.q8_k, parseQuantizationMode("q8_k").?);
}

test "parseQuantizationMode accepts q1_0 q4_1 q5_1 q8_1" {
    try std.testing.expectEqual(QuantizationMode.q1_0, parseQuantizationMode("q1_0").?);
    try std.testing.expectEqual(QuantizationMode.q4_1, parseQuantizationMode("q4_1").?);
    try std.testing.expectEqual(QuantizationMode.q5_1, parseQuantizationMode("q5_1").?);
    try std.testing.expectEqual(QuantizationMode.q8_1, parseQuantizationMode("q8_1").?);
}

test "parseArgs accepts quantization include and exclude filters" {
    const opts = try parseArgs(&.{
        "/tmp/model",
        "--quantize",
        "q8_k",
        "--quantize-include",
        "token_embd,blk.0",
        "--quantize-exclude",
        "blk.0.attn_q",
    });
    try std.testing.expectEqualStrings("/tmp/model", opts.model_dir);
    try std.testing.expectEqual(QuantizationMode.q8_k, opts.quantization);
    try std.testing.expectEqualStrings("token_embd,blk.0", opts.quantize_include_prefixes.?);
    try std.testing.expectEqualStrings("blk.0.attn_q", opts.quantize_exclude_prefixes.?);
}

test "parseArgs accepts projector output" {
    const opts = try parseArgs(&.{
        "/tmp/model",
        "--output",
        "/tmp/out.gguf",
        "--projector-output",
        "/tmp/mmproj.gguf",
    });
    try std.testing.expectEqualStrings("/tmp/model", opts.model_dir);
    try std.testing.expectEqualStrings("/tmp/out.gguf", opts.output_path.?);
    try std.testing.expectEqualStrings("/tmp/mmproj.gguf", opts.projector_output_path.?);
}

test "parseArgs accepts projector format" {
    const opts = try parseArgs(&.{
        "/tmp/model",
        "--projector-format",
        "clip",
    });
    try std.testing.expectEqualStrings("/tmp/model", opts.model_dir);
    try std.testing.expectEqual(ProjectorFormat.clip, opts.projector_format);
}

const SafetensorsFixture = struct {
    name: []const u8,
    shape: []const usize,
    data: []const f32,
};

fn writeSafetensorsFixture(allocator: std.mem.Allocator, path: []const u8, tensors: []const SafetensorsFixture) !void {
    var offsets = try allocator.alloc(u64, tensors.len + 1);
    defer allocator.free(offsets);
    offsets[0] = 0;
    for (tensors, 0..) |tensor, i| {
        offsets[i + 1] = offsets[i] + @as(u64, tensor.data.len) * 4;
    }

    var json: std.Io.Writer.Allocating = .init(allocator);
    defer json.deinit();
    const writer = &json.writer;
    try writer.writeAll("{\"__metadata__\":{\"format\":\"pt\"}");
    for (tensors, 0..) |tensor, i| {
        try writer.print(",\"{s}\":{{\"dtype\":\"F32\",\"shape\":[", .{tensor.name});
        for (tensor.shape, 0..) |dim, dim_idx| {
            if (dim_idx > 0) try writer.writeByte(',');
            try writer.print("{d}", .{dim});
        }
        try writer.print("],\"data_offsets\":[{d},{d}]}}", .{ offsets[i], offsets[i + 1] });
    }
    try writer.writeByte('}');

    const io = io_compat();
    var file = try compat.cwd().createFile(io, path, .{ .truncate = true });
    defer file.close(io);
    var header_size: [8]u8 = undefined;
    std.mem.writeInt(u64, &header_size, json.written().len, .little);
    try file.writeStreamingAll(io, &header_size);
    try file.writeStreamingAll(io, json.written());
    for (tensors) |tensor| {
        try file.writeStreamingAll(io, std.mem.sliceAsBytes(tensor.data));
    }
}

fn writeGoldenDenseFixture(allocator: std.mem.Allocator, dir_path: []const u8, with_hf_tokenizer: bool) !void {
    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"llama","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"num_key_value_heads":2,"intermediate_size":8,"vocab_size":6,"max_position_embeddings":16,"rms_norm_eps":1e-5}
        ,
    });

    if (with_hf_tokenizer) {
        const tokenizer_path = try std.fs.path.join(allocator, &.{ dir_path, "tokenizer.json" });
        defer allocator.free(tokenizer_path);
        try compat.cwd().writeFile(compat.io(), .{
            .sub_path = tokenizer_path,
            .data =
            \\{
            \\  "version":"1.0",
            \\  "normalizer":{"type":"BertNormalizer","lowercase":true},
            \\  "pre_tokenizer":{"type":"BertPreTokenizer"},
            \\  "post_processor":{"type":"BertProcessing","sep":["[SEP]",3],"cls":["[CLS]",2]},
            \\  "added_tokens":[
            \\    {"id":0,"content":"[PAD]"},
            \\    {"id":1,"content":"[UNK]"},
            \\    {"id":2,"content":"[CLS]"},
            \\    {"id":3,"content":"[SEP]"}
            \\  ],
            \\  "model":{
            \\    "type":"WordPiece",
            \\    "unk_token":"[UNK]",
            \\    "continuing_subword_prefix":"##",
            \\    "max_input_chars_per_word":100,
            \\    "vocab":{"[PAD]":0,"[UNK]":1,"[CLS]":2,"[SEP]":3,"hello":4,"world":5}
            \\  }
            \\}
            ,
        });
    }

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "model.embed_tokens.weight", .shape = &.{ 6, 4 }, .data = &[_]f32{
            0,  1,  2,  3,
            4,  5,  6,  7,
            8,  9,  10, 11,
            12, 13, 14, 15,
            16, 17, 18, 19,
            20, 21, 22, 23,
        } },
        .{ .name = "model.norm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 2, 3, 4 } },
        .{ .name = "model.layers.0.input_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 1, 1, 1, 1 } },
        .{ .name = "model.layers.0.self_attn.q_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            1, 0, 0, 0,
            0, 1, 0, 0,
            0, 0, 1, 0,
            0, 0, 0, 1,
        } },
        .{ .name = "model.layers.0.self_attn.k_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            2, 0, 0, 0,
            0, 2, 0, 0,
            0, 0, 2, 0,
            0, 0, 0, 2,
        } },
        .{ .name = "model.layers.0.self_attn.v_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            3, 0, 0, 0,
            0, 3, 0, 0,
            0, 0, 3, 0,
            0, 0, 0, 3,
        } },
        .{ .name = "model.layers.0.self_attn.o_proj.weight", .shape = &.{ 4, 4 }, .data = &[_]f32{
            4, 0, 0, 0,
            0, 4, 0, 0,
            0, 0, 4, 0,
            0, 0, 0, 4,
        } },
        .{ .name = "model.layers.0.post_attention_layernorm.weight", .shape = &.{4}, .data = &[_]f32{ 2, 2, 2, 2 } },
        .{ .name = "model.layers.0.mlp.gate_proj.weight", .shape = &.{ 8, 4 }, .data = &[_]f32{
            0,  1,  2,  3,
            4,  5,  6,  7,
            8,  9,  10, 11,
            12, 13, 14, 15,
            16, 17, 18, 19,
            20, 21, 22, 23,
            24, 25, 26, 27,
            28, 29, 30, 31,
        } },
        .{ .name = "model.layers.0.mlp.up_proj.weight", .shape = &.{ 8, 4 }, .data = &[_]f32{
            31, 30, 29, 28,
            27, 26, 25, 24,
            23, 22, 21, 20,
            19, 18, 17, 16,
            15, 14, 13, 12,
            11, 10, 9,  8,
            7,  6,  5,  4,
            3,  2,  1,  0,
        } },
        .{ .name = "model.layers.0.mlp.down_proj.weight", .shape = &.{ 4, 8 }, .data = &[_]f32{
            1,  2,  3,  4,  5,  6,  7,  8,
            9,  10, 11, 12, 13, 14, 15, 16,
            17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        } },
    });
}

fn writeWideQuantFixture(allocator: std.mem.Allocator, dir_path: []const u8) !void {
    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"llama","hidden_size":256,"num_hidden_layers":1,"num_attention_heads":8,"num_key_value_heads":8,"intermediate_size":256,"vocab_size":256,"max_position_embeddings":16,"rms_norm_eps":1e-5}
        ,
    });

    const matrix_elem_count = 256 * 256;
    const wide = try allocator.alloc(f32, matrix_elem_count);
    defer allocator.free(wide);
    for (wide, 0..) |*value, i| {
        const idx: f32 = @floatFromInt(i % 256);
        value.* = (idx - 128.0) * 0.5;
    }

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "model.embed_tokens.weight", .shape = &.{ 256, 256 }, .data = wide },
        .{ .name = "model.norm.weight", .shape = &.{256}, .data = &(@as([256]f32, @splat(2.0))) },
        .{ .name = "model.layers.0.input_layernorm.weight", .shape = &.{256}, .data = &(@as([256]f32, @splat(1.0))) },
        .{ .name = "model.layers.0.self_attn.q_proj.weight", .shape = &.{ 256, 256 }, .data = wide },
        .{ .name = "model.layers.0.self_attn.k_proj.weight", .shape = &.{ 256, 256 }, .data = wide },
        .{ .name = "model.layers.0.self_attn.v_proj.weight", .shape = &.{ 256, 256 }, .data = wide },
        .{ .name = "model.layers.0.self_attn.o_proj.weight", .shape = &.{ 256, 256 }, .data = wide },
        .{ .name = "model.layers.0.post_attention_layernorm.weight", .shape = &.{256}, .data = &(@as([256]f32, @splat(2.0))) },
        .{ .name = "model.layers.0.mlp.gate_proj.weight", .shape = &.{ 256, 256 }, .data = wide },
        .{ .name = "model.layers.0.mlp.up_proj.weight", .shape = &.{ 256, 256 }, .data = wide },
        .{ .name = "model.layers.0.mlp.down_proj.weight", .shape = &.{ 256, 256 }, .data = wide },
    });
}

fn writeMinimalQuantizedGgufFixture(allocator: std.mem.Allocator, path: []const u8) !void {
    var metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "llama" } },
        .{ .key = "general.alignment", .value = .{ .u32 = 32 } },
    };
    const dims = [_]u64{32};
    const tensors = [_]gguf_mod.writer.TensorSpec{.{
        .name = "tok_embeddings.weight",
        .dimensions = &dims,
        .tensor_type = .{ .known = .Q8_0 },
    }};
    const raw = [_]u8{ 0, 60 } ++ (@as([32]u8, @splat(128)));
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &tensors);
    defer layout.deinit(allocator);
    const io = io_compat();
    var file = try compat.cwd().createFile(io, path, .{ .truncate = true });
    defer file.close(io);
    try file.writeStreamingAll(io, layout.header_bytes);
    const data_region_offset = std.mem.alignForward(usize, layout.header_bytes.len, @intCast(layout.alignment));
    try writeZeroPadding(io, &file, data_region_offset - layout.header_bytes.len);
    try file.writeStreamingAll(io, &raw);
}

fn testScratchDir(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    const root = try std.fmt.allocPrint(allocator, "antfly-inference-export-gguf-tests-{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    const dir_path = try std.fs.path.join(allocator, &.{ "/tmp", root, name });
    errdefer allocator.free(dir_path);
    compat.cwd().deleteTree(compat.io(), dir_path) catch {};
    try compat.cwd().createDirPath(compat.io(), dir_path);
    return dir_path;
}

fn findMetadataEntry(parsed: *const gguf_mod.format.File, key: []const u8) ?*const gguf_mod.format.MetadataEntry {
    for (parsed.metadata) |*entry| {
        if (std.mem.eql(u8, entry.key, key)) return entry;
    }
    return null;
}

fn writeMinimalProjectorFixture(
    allocator: std.mem.Allocator,
    path: []const u8,
    format: ResolvedProjectorFormat,
) !void {
    const clip_metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.vision.projector_type", .value = .{ .string = "gemma4v" } },
        .{ .key = "clip.vision.projection_dim", .value = .{ .u32 = 4 } },
        .{ .key = "clip.vision.embedding_length", .value = .{ .u32 = 8 } },
        .{ .key = "clip.vision.feed_forward_length", .value = .{ .u32 = 16 } },
        .{ .key = "clip.vision.block_count", .value = .{ .u32 = 2 } },
        .{ .key = "clip.vision.attention.head_count", .value = .{ .u32 = 4 } },
        .{ .key = "clip.vision.image_size", .value = .{ .u32 = 224 } },
        .{ .key = "clip.vision.patch_size", .value = .{ .u32 = 14 } },
    };
    const antfly_metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "antfly-projector" } },
        .{ .key = "inference.projector.source_architecture", .value = .{ .string = "gemma3" } },
        .{ .key = "inference.projector.text_hidden_size", .value = .{ .u32 = 4 } },
        .{ .key = "inference.projector.vision_hidden_size", .value = .{ .u32 = 8 } },
        .{ .key = "inference.projector.vision_feed_forward_length", .value = .{ .u32 = 16 } },
        .{ .key = "inference.projector.vision_block_count", .value = .{ .u32 = 3 } },
        .{ .key = "inference.projector.vision_attention_head_count", .value = .{ .u32 = 4 } },
        .{ .key = "inference.projector.vision_image_size", .value = .{ .u32 = 224 } },
        .{ .key = "inference.projector.vision_patch_size", .value = .{ .u32 = 14 } },
        .{ .key = "inference.projector.mm_tokens_per_image", .value = .{ .u32 = 256 } },
    };
    const metadata: []const gguf_mod.format.MetadataEntry = switch (format) {
        .clip => &clip_metadata,
        .antfly => &antfly_metadata,
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, metadata, &.{});
    defer layout.deinit(allocator);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = layout.header_bytes });
}

fn makeRangeF32(comptime start: usize, comptime end: usize) [end - start]f32 {
    var out: [end - start]f32 = undefined;
    for (start..end, 0..) |value, i| out[i] = @floatFromInt(value);
    return out;
}

fn makeFilledF32(comptime len: usize, comptime value: i32) [len]f32 {
    var out: [len]f32 = undefined;
    for (&out) |*item| item.* = @floatFromInt(value);
    return out;
}

fn expectDenseExportQuantization(
    allocator: std.mem.Allocator,
    test_name: []const u8,
    quantization: QuantizationMode,
    expected_tensor_type: gguf_mod.tensor_types.TensorType,
) !void {
    const width: usize = switch (quantization) {
        .q1_0 => 128,
        .q2_k, .q3_k, .q4_k, .q5_k, .q6_k, .q8_k => 256,
        .q4_0, .q4_1, .q5_0, .q5_1, .q8_0, .q8_1 => 32,
        .none => unreachable,
    };

    const dir_path = try testScratchDir(allocator, test_name);
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    const config_json = try std.fmt.allocPrint(allocator,
        \\{{"model_type":"llama","hidden_size":{d},"num_hidden_layers":1,"num_attention_heads":4,"num_key_value_heads":4,"intermediate_size":{d},"vocab_size":{d},"max_position_embeddings":16,"rms_norm_eps":1e-5}}
    , .{ width, width, width });
    defer allocator.free(config_json);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = config_path, .data = config_json });

    const matrix_elem_count = width * width;
    const embed = try allocator.alloc(f32, matrix_elem_count);
    defer allocator.free(embed);
    for (embed, 0..) |*value, i| value.* = @floatFromInt(i);

    const ones = try allocator.alloc(f32, width);
    defer allocator.free(ones);
    for (ones) |*value| value.* = 1.0;

    const twos = try allocator.alloc(f32, width);
    defer allocator.free(twos);
    for (twos) |*value| value.* = 2.0;

    const matrix_shape = [_]usize{ width, width };
    const vector_shape = [_]usize{width};

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "model.embed_tokens.weight", .shape = &matrix_shape, .data = embed },
        .{ .name = "model.norm.weight", .shape = &vector_shape, .data = twos },
        .{ .name = "model.layers.0.input_layernorm.weight", .shape = &vector_shape, .data = ones },
        .{ .name = "model.layers.0.self_attn.q_proj.weight", .shape = &matrix_shape, .data = embed },
        .{ .name = "model.layers.0.self_attn.k_proj.weight", .shape = &matrix_shape, .data = embed },
        .{ .name = "model.layers.0.self_attn.v_proj.weight", .shape = &matrix_shape, .data = embed },
        .{ .name = "model.layers.0.self_attn.o_proj.weight", .shape = &matrix_shape, .data = embed },
        .{ .name = "model.layers.0.post_attention_layernorm.weight", .shape = &vector_shape, .data = twos },
        .{ .name = "model.layers.0.mlp.gate_proj.weight", .shape = &matrix_shape, .data = embed },
        .{ .name = "model.layers.0.mlp.up_proj.weight", .shape = &matrix_shape, .data = embed },
        .{ .name = "model.layers.0.mlp.down_proj.weight", .shape = &matrix_shape, .data = embed },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "quantized.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, quantization);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);

    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    const embed_tensor = catalog.find("token_embd.weight").?;
    try std.testing.expectEqual(expected_tensor_type, embed_tensor.tensor_type);
    try std.testing.expectEqual(@as(u64, @intCast(width)), embed_tensor.dimensions[0]);
    try std.testing.expectEqual(@as(u64, @intCast(width)), embed_tensor.dimensions[1]);

    const embed_len = gguf_mod.tensor_types.byteLen(embed_tensor.tensor_type, embed_tensor.dimensions).?;
    const embed_bytes = try c_file.readRegion(allocator, out_path, embed_tensor.data_offset, @intCast(embed_len));
    defer allocator.free(embed_bytes);
    const dense = try allocator.alloc(f32, matrix_elem_count);
    defer allocator.free(dense);
    try gguf_mod.quant_codec.dequantizeToFloat32(expected_tensor_type, embed_bytes, dense);
    switch (quantization) {
        .q1_0 => {
            try std.testing.expectApproxEqAbs(@as(f32, 63.5), dense[0], 1e-3);
            try std.testing.expectApproxEqAbs(@as(f32, 63.5), dense[1], 1e-3);
        },
        else => {
            try std.testing.expectApproxEqAbs(@as(f32, 0.0), dense[0], 1e-3);
            try std.testing.expectApproxEqAbs(@as(f32, 1.0), dense[1], 2.0);
        },
    }
}

fn expectMixedKFamilyExport(
    allocator: std.mem.Allocator,
    test_name: []const u8,
    quantization: QuantizationMode,
    expected_wide_tensor_type: gguf_mod.tensor_types.TensorType,
) !void {
    const dir_path = try testScratchDir(allocator, test_name);
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"llama","hidden_size":256,"num_hidden_layers":1,"num_attention_heads":8,"num_key_value_heads":8,"intermediate_size":256,"vocab_size":256,"max_position_embeddings":16,"rms_norm_eps":1e-5}
        ,
    });

    const wide = try allocator.alloc(f32, 256 * 256);
    defer allocator.free(wide);
    for (wide, 0..) |*value, i| {
        const idx: f32 = @floatFromInt(i % 256);
        value.* = (idx - 128.0) * 0.5;
    }

    const narrow = try allocator.alloc(f32, 32);
    defer allocator.free(narrow);
    for (narrow, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i)) - 16));

    const st_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(st_path);
    try writeSafetensorsFixture(allocator, st_path, &.{
        .{ .name = "model.embed_tokens.weight", .shape = &.{ 256, 256 }, .data = wide },
        .{ .name = "model.layers.0.input_layernorm.weight", .shape = &.{32}, .data = narrow },
    });

    const out_path = try std.fs.path.join(allocator, &.{ dir_path, "mixed.gguf" });
    defer allocator.free(out_path);
    try exportModelDirToGguf(allocator, dir_path, out_path, quantization);

    const raw = try c_file.readFile(allocator, out_path);
    defer allocator.free(raw);
    var parsed = try gguf_mod.format.parse(allocator, raw);
    defer parsed.deinit(allocator);

    const catalog = gguf_mod.tensor_catalog.Catalog.init(&parsed);
    const wide_tensor = catalog.find("token_embd.weight").?;
    try std.testing.expectEqual(expected_wide_tensor_type, wide_tensor.tensor_type);
    const narrow_tensor = catalog.find("blk.0.attn_norm.weight").?;
    try std.testing.expectEqual(gguf_mod.tensor_types.TensorType{ .known = .F32 }, narrow_tensor.tensor_type);
}
