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

//! Atomic EmbeddingGemma 2 LoRA export.
//!
//! The exporter deliberately accepts plain host FP32 adapter tensors instead
//! of depending on a trainer implementation. This keeps checkpoint restore and
//! export separate and gives the CLI one narrow, testable integration point.

const std = @import("std");
const builtin = @import("builtin");
const compat = @import("../io/compat.zig");
const safetensors = @import("../models/safetensors.zig");
const checkpoint = @import("safetensors_checkpoint.zig");
const catalog = @import("../registry/embedding_gemma2_catalog.zig");
const fs_support = @import("embedding_gemma2_fs.zig");
const job = @import("embedding_gemma2_job.zig");

pub const default_rank: usize = 16;
pub const default_alpha: f32 = 32;
pub const adapter_file_name = "adapter_model.safetensors";
pub const adapter_config_file_name = "adapter_config.json";
pub const merged_file_name = "model.safetensors";
pub const provenance_file_name = "antfly_finetune_provenance.json";

pub const AdapterTensor = struct {
    /// Exact SafeTensors name, including the authoritative language_model prefix.
    base_weight_name: []const u8,
    /// PEFT A matrix in row-major [rank, in_features].
    A: []const f32,
    /// PEFT B matrix in row-major [out_features, rank].
    B: []const f32,
    out_features: usize,
    in_features: usize,
    rank: usize = default_rank,
};

pub const Options = struct {
    base_model_dir: []const u8,
    output_dir: []const u8,
    base_model_id: []const u8 = catalog.repo,
    base_revision: []const u8 = catalog.revision,
    base_weight_sha256: []const u8 = catalog.bundles[0].artifact_list[0].sha256,
    rank: usize = default_rank,
    alpha: f32 = default_alpha,
    target_modules: []const []const u8,
    training_provenance_json: []const u8,
};

pub const ExportReport = struct {
    merged_tensor_count: usize,
    copied_tensor_count: usize,
    adapter_tensor_count: usize,
    output_weight_sha256: [64]u8,
};

const sidecars = [_][]const u8{
    "config.json",
    "tokenizer.json",
    "tokenizer.model",
    "tokenizer_config.json",
    "chat_template.jinja",
    "preprocessor_config.json",
    "processor_config.json",
    "modules.json",
    "sentence_bert_config.json",
    "config_sentence_transformers.json",
    "1_Pooling/config.json",
    "2_Normalize/config.json",
};

pub fn exportBundle(allocator: std.mem.Allocator, options: Options, adapters: []const AdapterTensor) !ExportReport {
    try validateOptions(options, adapters);
    const base_weight_path = try std.fs.path.join(allocator, &.{ options.base_model_dir, merged_file_name });
    defer allocator.free(base_weight_path);
    const actual_base_sha = try sha256FileHex(allocator, base_weight_path);
    defer allocator.free(actual_base_sha);
    if (!std.mem.eql(u8, actual_base_sha, options.base_weight_sha256)) return error.BaseWeightIdentityMismatch;
    // Processor, tokenizer, prompt, pooling, and normalization sidecars are
    // executable model state. Refuse to publish a bundle under the pinned base
    // identity if any of those bytes differ from the reviewed revision.
    // Synthetic unit fixtures cannot reproduce the upstream artifacts; this
    // bypass is compile-time limited to Zig test binaries with a synthetic
    // base digest and cannot exist in a production exporter.
    if (!builtin.is_test or std.mem.eql(u8, options.base_weight_sha256, catalog.bundles[0].artifact_list[0].sha256))
        try verifyPinnedSidecars(allocator, options.base_model_dir);
    const io = compat.io();
    try job.rejectPathOverlap(allocator, io, options.output_dir, &.{options.base_model_dir});
    std.Io.Dir.cwd().access(io, options.output_dir, .{}) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    if (std.Io.Dir.cwd().access(io, options.output_dir, .{})) |_| return error.OutputAlreadyExists else |_| {}

    var staging = try fs_support.OwnedStagingDir.create(allocator, io, options.output_dir, "tmp");
    defer staging.deinit();

    for (sidecars) |name| try copyRequired(allocator, options.base_model_dir, staging.path, name);
    try writeAdapter(allocator, staging.path, options, adapters);
    const merged_path = try std.fs.path.join(allocator, &.{ staging.path, merged_file_name });
    defer allocator.free(merged_path);
    const counts = try writeMergedCheckpoint(allocator, base_weight_path, merged_path, options.alpha / @as(f32, @floatFromInt(options.rank)), adapters);
    const output_sha = try sha256FileHex(allocator, merged_path);
    defer allocator.free(output_sha);
    try writeIdentityFiles(allocator, staging.path, options, adapters, actual_base_sha, output_sha);

    try syncDirectory(staging.path);
    try staging.publish(options.output_dir);
    try syncDirectory(std.fs.path.dirname(options.output_dir) orelse ".");

    var digest: [64]u8 = undefined;
    @memcpy(&digest, output_sha);
    return .{
        .merged_tensor_count = counts.merged,
        .copied_tensor_count = counts.copied,
        .adapter_tensor_count = adapters.len * 2,
        .output_weight_sha256 = digest,
    };
}

fn validateOptions(options: Options, adapters: []const AdapterTensor) !void {
    if ((options.rank != 8 and options.rank != 16 and options.rank != 32) or !std.math.isFinite(options.alpha) or options.alpha <= 0 or options.alpha > 1024 or adapters.len == 0) return error.InvalidEmbeddingGemma2LoRAContract;
    if (!std.mem.eql(u8, options.base_model_id, catalog.repo) or !std.mem.eql(u8, options.base_revision, catalog.revision)) return error.BaseModelIdentityMismatch;
    if (options.base_weight_sha256.len != 64 or options.training_provenance_json.len == 0 or options.target_modules.len == 0) return error.InvalidEmbeddingGemma2LoRAContract;
    for (options.target_modules, 0..) |module, i| {
        if (module.len == 0) return error.InvalidEmbeddingGemma2TargetName;
        for (module) |c| if (!(std.ascii.isAlphanumeric(c) or c == '_' or c == '.')) return error.InvalidEmbeddingGemma2TargetName;
        for (options.target_modules[0..i]) |prior| if (std.mem.eql(u8, prior, module)) return error.DuplicateAdapterTarget;
    }
    var parsed = try std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, options.training_provenance_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidTrainingProvenance;
    for (adapters, 0..) |adapter, i| {
        if (!validBaseWeightName(adapter.base_weight_name)) return error.InvalidEmbeddingGemma2TargetName;
        if (adapter.rank != options.rank or adapter.in_features == 0 or adapter.out_features == 0) return error.InvalidAdapterShape;
        if (adapter.A.len != adapter.rank * adapter.in_features or adapter.B.len != adapter.out_features * adapter.rank) return error.InvalidAdapterShape;
        for (adapter.A) |v| if (!std.math.isFinite(v)) return error.NonFiniteAdapter;
        for (adapter.B) |v| if (!std.math.isFinite(v)) return error.NonFiniteAdapter;
        const base = peftBaseName(adapter.base_weight_name);
        var target_found = false;
        for (options.target_modules) |module| if (std.mem.eql(u8, base, module) or (base.len > module.len and std.mem.endsWith(u8, base, module) and base[base.len - module.len - 1] == '.')) {
            target_found = true;
            break;
        };
        if (!target_found) return error.AdapterTargetNotDeclared;
        for (adapters[0..i]) |previous| if (std.mem.eql(u8, previous.base_weight_name, adapter.base_weight_name)) return error.DuplicateAdapterTarget;
    }
}

pub fn validBaseWeightName(name: []const u8) bool {
    return std.mem.startsWith(u8, name, "language_model.") and
        std.mem.endsWith(u8, name, ".weight") and
        std.mem.indexOf(u8, name, "..") == null and
        std.mem.indexOfScalar(u8, name, '/') == null;
}

fn peftBaseName(name: []const u8) []const u8 {
    return name[0 .. name.len - ".weight".len];
}

fn writeAdapter(allocator: std.mem.Allocator, staging: []const u8, options: Options, adapters: []const AdapterTensor) !void {
    const named = try allocator.alloc(checkpoint.NamedTensor, adapters.len * 2);
    defer allocator.free(named);
    const shapes = try allocator.alloc([2]usize, adapters.len * 2);
    defer allocator.free(shapes);
    var names = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (names.items) |name| allocator.free(name);
        names.deinit(allocator);
    }
    for (adapters, 0..) |adapter, i| {
        const base = peftBaseName(adapter.base_weight_name);
        const a_name = try std.fmt.allocPrint(allocator, "base_model.model.{s}.lora_A.weight", .{base});
        try names.append(allocator, a_name);
        const b_name = try std.fmt.allocPrint(allocator, "base_model.model.{s}.lora_B.weight", .{base});
        try names.append(allocator, b_name);
        shapes[i * 2] = .{ adapter.rank, adapter.in_features };
        shapes[i * 2 + 1] = .{ adapter.out_features, adapter.rank };
        named[i * 2] = .{ .name = a_name, .data = adapter.A, .shape = &shapes[i * 2] };
        named[i * 2 + 1] = .{ .name = b_name, .data = adapter.B, .shape = &shapes[i * 2 + 1] };
    }
    const path = try std.fs.path.join(allocator, &.{ staging, adapter_file_name });
    defer allocator.free(path);
    try checkpoint.saveControlled(allocator, path, named, null, true);

    var json: std.Io.Writer.Allocating = .init(allocator);
    defer json.deinit();
    const w = &json.writer;
    try w.print("{{\"base_model_name_or_path\":\"{s}\",\"bias\":\"none\",\"inference_mode\":true,\"lora_alpha\":{d},\"lora_dropout\":0.0,\"peft_type\":\"LORA\",\"r\":{d},\"task_type\":\"FEATURE_EXTRACTION\",\"target_modules\":[", .{ options.base_model_id, options.alpha, options.rank });
    for (options.target_modules, 0..) |module, i| {
        if (i != 0) try w.writeByte(',');
        try w.writeByte('"');
        try jsonEscape(w, module);
        try w.writeByte('"');
    }
    try w.writeAll("]}");
    try writeFileSynced(allocator, staging, adapter_config_file_name, json.written());
}

fn writeMergedCheckpoint(allocator: std.mem.Allocator, base_path: []const u8, output_path: []const u8, scale: f32, adapters: []const AdapterTensor) !struct { merged: usize, copied: usize } {
    var reader = try safetensors.MMapReader.openFileAbsolute(allocator, base_path);
    defer reader.deinit();
    const names = try reader.header.tensorNames(allocator);
    defer allocator.free(names);
    std.mem.sort([]const u8, names, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lessThan);

    var header: std.Io.Writer.Allocating = .init(allocator);
    defer header.deinit();
    const hw = &header.writer;
    try hw.writeAll("{\"__metadata__\":{\"format\":\"pt\",\"antfly.export\":\"embedding_gemma2_lora_merged_v1\"}");
    var offset: u64 = 0;
    for (names) |name| {
        const meta = reader.header.tensors.get(name).?;
        try hw.writeAll(",\"");
        try jsonEscape(hw, name);
        try hw.writeAll("\":{\"dtype\":\"");
        try hw.writeAll(dtypeName(meta.dtype));
        try hw.writeAll("\",\"shape\":[");
        for (meta.shape, 0..) |dim, i| {
            if (i != 0) try hw.writeByte(',');
            try hw.print("{d}", .{dim});
        }
        const len = meta.data_end - meta.data_start;
        try hw.print("],\"data_offsets\":[{d},{d}]}}", .{ offset, offset + len });
        offset += len;
    }
    try hw.writeByte('}');
    const padding = (8 - header.written().len % 8) % 8;
    const io = compat.io();
    var file = try compat.cwd().createFile(io, output_path, .{ .truncate = true, .exclusive = true });
    defer file.close(io);
    var buf: [256 * 1024]u8 = undefined;
    var fw = file.writer(io, &buf);
    defer fw.end() catch {};
    const w = &fw.interface;
    var size_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &size_buf, header.written().len + padding, .little);
    try w.writeAll(&size_buf);
    try w.writeAll(header.written());
    for (0..padding) |_| try w.writeByte(' ');

    var merged: usize = 0;
    for (names) |name| {
        const meta = reader.header.tensors.get(name).?;
        if (findAdapter(adapters, name)) |adapter| {
            if (meta.dtype != .bf16 or meta.shape.len != 2 or meta.shape[0] != adapter.out_features or meta.shape[1] != adapter.in_features) return error.BaseTensorShapeMismatch;
            const raw = tensorRaw(&reader, meta);
            const values = try allocator.alloc(u16, adapter.out_features * adapter.in_features);
            defer allocator.free(values);
            for (values, 0..) |*slot, idx| {
                const base_bits = std.mem.readInt(u16, raw[idx * 2 ..][0..2], .little);
                const base_value: f32 = @bitCast(@as(u32, base_bits) << 16);
                const out = idx / adapter.in_features;
                const in = idx % adapter.in_features;
                var delta: f32 = 0;
                for (0..adapter.rank) |r|
                    delta = @mulAdd(f32, adapter.B[out * adapter.rank + r], adapter.A[r * adapter.in_features + in], delta);
                const scaled_delta = scale * delta;
                const sum = base_value + scaled_delta;
                if (!std.math.isFinite(sum)) return error.NonFiniteMergedWeight;
                slot.* = f32ToBf16Rne(sum);
            }
            try w.writeAll(std.mem.sliceAsBytes(values));
            merged += 1;
        } else try w.writeAll(tensorRaw(&reader, meta));
    }
    try w.flush();
    try file.sync(io);
    return .{ .merged = merged, .copied = names.len - merged };
}

fn tensorRaw(reader: *const safetensors.MMapReader, meta: safetensors.TensorMeta) []const u8 {
    const start: usize = @intCast(reader.data_offset + meta.data_start);
    const end: usize = @intCast(reader.data_offset + meta.data_end);
    return reader.file_bytes[start..end];
}

fn findAdapter(adapters: []const AdapterTensor, name: []const u8) ?AdapterTensor {
    for (adapters) |adapter| if (std.mem.eql(u8, adapter.base_weight_name, name)) return adapter;
    return null;
}

fn f32ToBf16Rne(value: f32) u16 {
    const bits: u32 = @bitCast(value);
    const rounded = bits +% (0x7fff + ((bits >> 16) & 1));
    return @truncate(rounded >> 16);
}

fn dtypeName(dtype: @import("../backends/tensor.zig").DType) []const u8 {
    return switch (dtype) {
        .f32 => "F32",
        .f16 => "F16",
        .bf16 => "BF16",
        .f64 => "F64",
        .i8 => "I8",
        .i16 => "I16",
        .i32 => "I32",
        .i64 => "I64",
        .u8 => "U8",
        else => unreachable,
    };
}

fn writeIdentityFiles(allocator: std.mem.Allocator, staging: []const u8, options: Options, adapters: []const AdapterTensor, base_sha: []const u8, merged_sha: []const u8) !void {
    var provenance: std.Io.Writer.Allocating = .init(allocator);
    defer provenance.deinit();
    try provenance.writer.print("{{\"schema\":\"antfly.embedding_gemma2.finetune_export.v1\",\"base_model\":\"{s}\",\"base_revision\":\"{s}\",\"base_weight_sha256\":\"{s}\",\"merged_weight_sha256\":\"{s}\",\"rank\":{d},\"alpha\":{d},\"merged_tensors\":{d},\"inputs\":[\"text\"],\"training\":{s}}}", .{ options.base_model_id, options.base_revision, base_sha, merged_sha, options.rank, options.alpha, adapters.len, options.training_provenance_json });
    try writeFileSynced(allocator, staging, provenance_file_name, provenance.written());
    const manifest = "{\"type\":\"embedder\",\"model_family\":\"embedding_gemma2\",\"inputs\":[\"text\"],\"capabilities\":[\"embedding.text\",\"embedding.code\"],\"adapter\":\"merged_lora\",\"media_enabled\":false}";
    try writeFileSynced(allocator, staging, "model_manifest.json", manifest);
}

fn copyRequired(allocator: std.mem.Allocator, base_dir: []const u8, staging: []const u8, name: []const u8) !void {
    const src = try std.fs.path.join(allocator, &.{ base_dir, name });
    defer allocator.free(src);
    const dst = try std.fs.path.join(allocator, &.{ staging, name });
    defer allocator.free(dst);
    if (std.fs.path.dirname(dst)) |dir| try compat.cwd().createDirPath(compat.io(), dir);
    var input = try compat.cwd().openFile(compat.io(), src, .{});
    defer input.close(compat.io());
    var output = try compat.cwd().createFile(compat.io(), dst, .{ .truncate = true, .exclusive = true });
    defer output.close(compat.io());
    var buffer: [256 * 1024]u8 = undefined;
    while (true) {
        const n = input.readStreaming(compat.io(), &.{buffer[0..]}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (n == 0) break;
        try output.writeStreamingAll(compat.io(), buffer[0..n]);
    }
    try output.sync(compat.io());
    if (std.fs.path.dirname(dst)) |dir| try syncDirectory(dir);
}

fn verifyPinnedSidecars(allocator: std.mem.Allocator, base_dir: []const u8) !void {
    for (sidecars) |name| {
        const artifact = artifactForPath(name) orelse return error.IncompleteEmbeddingGemma2Catalog;
        const path = try std.fs.path.join(allocator, &.{ base_dir, name });
        defer allocator.free(path);
        const actual = try sha256FileHex(allocator, path);
        defer allocator.free(actual);
        if (!std.mem.eql(u8, actual, artifact.sha256)) return error.BaseSidecarIdentityMismatch;
    }
}

fn artifactForPath(name: []const u8) ?catalog.Artifact {
    for (catalog.bundles[0].artifact_list) |artifact|
        if (std.mem.eql(u8, artifact.path, name)) return artifact;
    return null;
}

fn writeFileSynced(allocator: std.mem.Allocator, dir: []const u8, name: []const u8, data: []const u8) !void {
    const path = try std.fs.path.join(allocator, &.{ dir, name });
    defer allocator.free(path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = data });
    var file = try compat.cwd().openFile(compat.io(), path, .{});
    defer file.close(compat.io());
    try file.sync(compat.io());
    try syncDirectory(dir);
}

fn syncDirectory(path: []const u8) !void {
    var dir = try compat.cwd().openDir(compat.io(), path, .{ .iterate = true });
    defer dir.close(compat.io());
    const file = std.Io.File{ .handle = dir.handle, .flags = .{ .nonblocking = false } };
    try file.sync(compat.io());
}

fn sha256FileHex(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    var file = try compat.cwd().openFile(compat.io(), path, .{});
    defer file.close(compat.io());
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var buffer: [256 * 1024]u8 = undefined;
    while (true) {
        const n = file.readStreaming(compat.io(), &.{buffer[0..]}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (n == 0) break;
        hash.update(buffer[0..n]);
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return std.fmt.allocPrint(allocator, "{s}", .{std.fmt.bytesToHex(digest, .lower)});
}

fn jsonEscape(writer: *std.Io.Writer, text: []const u8) !void {
    for (text) |c| switch (c) {
        '"' => try writer.writeAll("\\\""),
        '\\' => try writer.writeAll("\\\\"),
        else => try writer.writeByte(c),
    };
}

test "EmbeddingGemma 2 export validates strict PEFT contract" {
    var a: [default_rank * 2]f32 = @splat(0);
    var b: [3 * default_rank]f32 = @splat(0);
    const adapters = [_]AdapterTensor{.{ .base_weight_name = "language_model.layers.0.self_attn.q_proj.weight", .A = &a, .B = &b, .out_features = 3, .in_features = 2 }};
    try validateOptions(.{ .base_model_dir = "base", .output_dir = "out", .target_modules = &.{"q_proj"}, .training_provenance_json = "{}" }, &adapters);
    try std.testing.expect(!validBaseWeightName("model.language_model.layers.0.q_proj.weight"));
    try std.testing.expect(!validBaseWeightName("language_model.layers/0/q_proj.weight"));
}

test "BF16 merge rounds ties to even" {
    try std.testing.expectEqual(@as(u16, 0x3f80), f32ToBf16Rne(1.0));
    try std.testing.expectEqual(@as(u16, 0xbf80), f32ToBf16Rne(-1.0));
}

test "EmbeddingGemma 2 export reduces FP32 adapter delta before adding BF16 base" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", allocator);
    defer allocator.free(root);
    const base = try std.fs.path.join(allocator, &.{ root, "base" });
    defer allocator.free(base);
    const output = try std.fs.path.join(allocator, &.{ root, "merged" });
    defer allocator.free(output);
    try compat.cwd().createDirPath(compat.io(), base);

    const target_name = "language_model.layers.0.self_attn.q_proj.weight";
    const header = "{\"language_model.layers.0.self_attn.q_proj.weight\":{\"dtype\":\"BF16\",\"shape\":[1,1],\"data_offsets\":[0,2]}}";
    var bytes = std.ArrayListUnmanaged(u8).empty;
    defer bytes.deinit(allocator);
    var size: [8]u8 = undefined;
    std.mem.writeInt(u64, &size, header.len, .little);
    try bytes.appendSlice(allocator, &size);
    try bytes.appendSlice(allocator, header);
    var base_bits: [2]u8 = undefined;
    std.mem.writeInt(u16, &base_bits, 0x3f80, .little);
    try bytes.appendSlice(allocator, &base_bits);
    const base_weight = try std.fs.path.join(allocator, &.{ base, merged_file_name });
    defer allocator.free(base_weight);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = base_weight, .data = bytes.items });
    for (sidecars) |name| {
        const path = try std.fs.path.join(allocator, &.{ base, name });
        defer allocator.free(path);
        if (std.fs.path.dirname(path)) |dir| try compat.cwd().createDirPath(compat.io(), dir);
        try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = "sidecar" });
    }
    const digest = try sha256FileHex(allocator, base_weight);
    defer allocator.free(digest);
    var a: [default_rank]f32 = @splat(1);
    var b: [default_rank]f32 = @splat(@as(f32, 0x1.0004p-12));
    const adapters = [_]AdapterTensor{.{ .base_weight_name = target_name, .A = &a, .B = &b, .out_features = 1, .in_features = 1 }};
    _ = try exportBundle(allocator, .{ .base_model_dir = base, .output_dir = output, .base_weight_sha256 = digest, .rank = default_rank, .alpha = default_rank, .target_modules = &.{"q_proj"}, .training_provenance_json = "{\"fixture\":true}" }, &adapters);

    const merged_path = try std.fs.path.join(allocator, &.{ output, merged_file_name });
    defer allocator.free(merged_path);
    var reader = try safetensors.MMapReader.openFileAbsolute(allocator, merged_path);
    defer reader.deinit();
    const merged = tensorRaw(&reader, reader.header.tensors.get(target_name).?);
    try std.testing.expectEqual(@as(u16, 0x3f81), std.mem.readInt(u16, merged[0..2], .little));
}

test "EG2 exporter sidecar inventory is exactly covered by pinned catalog" {
    try std.testing.expectEqual(catalog.bundles[0].artifact_list.len - 1, sidecars.len);
    for (sidecars) |name| try std.testing.expect(artifactForPath(name) != null);
}

test "EmbeddingGemma 2 export preserves untouched bytes and publishes PEFT text-only bundle" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", allocator);
    defer allocator.free(root);
    const base = try std.fs.path.join(allocator, &.{ root, "base" });
    defer allocator.free(base);
    const output = try std.fs.path.join(allocator, &.{ root, "merged" });
    defer allocator.free(output);
    try compat.cwd().createDirPath(compat.io(), base);
    const target_name = "language_model.layers.0.self_attn.q_proj.weight";
    const header = "{\"language_model.layers.0.self_attn.q_proj.weight\":{\"dtype\":\"BF16\",\"shape\":[2,2],\"data_offsets\":[0,8]},\"language_model.untouched\":{\"dtype\":\"U8\",\"shape\":[4],\"data_offsets\":[8,12]}}";
    var bytes = std.ArrayListUnmanaged(u8).empty;
    defer bytes.deinit(allocator);
    var size: [8]u8 = undefined;
    std.mem.writeInt(u64, &size, header.len, .little);
    try bytes.appendSlice(allocator, &size);
    try bytes.appendSlice(allocator, header);
    try bytes.appendNTimes(allocator, 0, 8);
    try bytes.appendSlice(allocator, &.{ 9, 8, 7, 6 });
    const base_weight = try std.fs.path.join(allocator, &.{ base, merged_file_name });
    defer allocator.free(base_weight);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = base_weight, .data = bytes.items });
    for (sidecars) |name| {
        const path = try std.fs.path.join(allocator, &.{ base, name });
        defer allocator.free(path);
        if (std.fs.path.dirname(path)) |dir| try compat.cwd().createDirPath(compat.io(), dir);
        try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = "sidecar" });
    }
    const digest = try sha256FileHex(allocator, base_weight);
    defer allocator.free(digest);
    var a: [default_rank * 2]f32 = @splat(0);
    var b: [2 * default_rank]f32 = @splat(0);
    a[0] = 1;
    b[0] = 1;
    const adapters = [_]AdapterTensor{.{ .base_weight_name = target_name, .A = &a, .B = &b, .out_features = 2, .in_features = 2 }};
    _ = try exportBundle(allocator, .{ .base_model_dir = base, .output_dir = output, .base_weight_sha256 = digest, .rank = default_rank, .alpha = 31.5, .target_modules = &.{"q_proj"}, .training_provenance_json = "{\"fixture\":true}" }, &adapters);
    const merged_path = try std.fs.path.join(allocator, &.{ output, merged_file_name });
    defer allocator.free(merged_path);
    var reader = try safetensors.MMapReader.openFileAbsolute(allocator, merged_path);
    defer reader.deinit();
    const target_meta = reader.header.tensors.get(target_name).?;
    const target = tensorRaw(&reader, target_meta);
    try std.testing.expectEqual(@as(u16, 0x3ffc), std.mem.readInt(u16, target[0..2], .little)); // alpha/rank = 1.96875
    const untouched_meta = reader.header.tensors.get("language_model.untouched").?;
    try std.testing.expectEqualSlices(u8, &.{ 9, 8, 7, 6 }, tensorRaw(&reader, untouched_meta));
    const adapter_path = try std.fs.path.join(allocator, &.{ output, adapter_file_name });
    defer allocator.free(adapter_path);
    var adapter_reader = try safetensors.MMapReader.openFileAbsolute(allocator, adapter_path);
    defer adapter_reader.deinit();
    try std.testing.expectEqual(@as(u32, 2), adapter_reader.header.tensors.count());
    const manifest_path = try std.fs.path.join(allocator, &.{ output, "model_manifest.json" });
    defer allocator.free(manifest_path);
    const manifest = try compat.cwd().readFileAlloc(compat.io(), manifest_path, allocator, .limited(4096));
    defer allocator.free(manifest);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"inputs\":[\"text\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest, "\"media_enabled\":false") != null);
    const adapter_config_path = try std.fs.path.join(allocator, &.{ output, adapter_config_file_name });
    defer allocator.free(adapter_config_path);
    const adapter_config = try compat.cwd().readFileAlloc(compat.io(), adapter_config_path, allocator, .limited(16 * 1024));
    defer allocator.free(adapter_config);
    try std.testing.expect(std.mem.indexOf(u8, adapter_config, "\"lora_alpha\":31.5") != null);
    const provenance_path = try std.fs.path.join(allocator, &.{ output, provenance_file_name });
    defer allocator.free(provenance_path);
    const provenance = try compat.cwd().readFileAlloc(compat.io(), provenance_path, allocator, .limited(4096));
    defer allocator.free(provenance);
    try std.testing.expect(std.mem.indexOf(u8, provenance, "\"alpha\":31.5") != null);
    var parsed_provenance = try std.json.parseFromSlice(std.json.Value, allocator, provenance, .{});
    defer parsed_provenance.deinit();
    const merged_identity = parsed_provenance.value.object.get("merged_weight_sha256") orelse return error.MissingMergedWeightIdentity;
    const actual_merged_identity = try sha256FileHex(allocator, merged_path);
    defer allocator.free(actual_merged_identity);
    try std.testing.expectEqualStrings(actual_merged_identity, merged_identity.string);
}
