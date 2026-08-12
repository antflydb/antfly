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
const quant_kernel_compiler = @import("graph/quant_kernel_compiler.zig");
const quant_kernel_compiler_renderer = @import("graph/quant_kernel_metal_renderer.zig");
const quant_kernel_model_catalog = @import("graph/quant_kernel_model_catalog.zig");

const print = std.debug.print;

const Mode = enum {
    check,
    write,
    check_metal,
    inspect_model,
    check_model,
};

const Options = struct {
    mode: Mode,
    model_path: ?[]const u8 = null,
};

const GeneratedFile = struct {
    path: []const u8,
    data: []const u8,
    owned: bool = false,
};

const CompiledSourceData = struct {
    data: []const u8,
    owned: bool = false,
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    const options = try parseOptions(args);
    const mode = options.mode;
    switch (mode) {
        .inspect_model, .check_model => {
            var inventory = try quant_kernel_model_catalog.inspectModel(allocator, options.model_path.?);
            defer inventory.deinit();
            try quant_kernel_model_catalog.writeJson(io, inventory);
            if (mode == .check_model and !inventory.coverage.complete) {
                print(
                    "quant kernel model catalog incomplete: unsupported_quantized_operations={d} unsupported_attention_topologies={d} unsupported_cuda_quantized_operations={d} unsupported_metal_quantized_operations={d} unsupported_cuda_attention_topologies={d} unsupported_metal_attention_topologies={d}\n",
                    .{
                        inventory.coverage.unsupported_quantized_operation_count,
                        inventory.coverage.unsupported_attention_topology_count,
                        inventory.coverage.unsupported_cuda_quantized_operation_count,
                        inventory.coverage.unsupported_metal_quantized_operation_count,
                        inventory.coverage.unsupported_cuda_attention_topology_count,
                        inventory.coverage.unsupported_metal_attention_topology_count,
                    },
                );
                return error.ModelCatalogCoverageIncomplete;
            }
            return;
        },
        .check, .write, .check_metal => {},
    }
    const files = try generatedFiles(allocator);
    defer freeGeneratedFiles(allocator, files);
    for (files) |source| {
        switch (mode) {
            .check, .check_metal => try checkSource(allocator, io, source),
            .write => try writeSource(allocator, io, source),
            .inspect_model, .check_model => unreachable,
        }
    }
    switch (mode) {
        .check, .check_metal => {
            try checkMetalRuntimeRegion(allocator, io);
            try checkMetalLaunchShapeRegion(allocator, io);
            try checkCudaRuntimeAttentionRegion(allocator, io);
            try checkCudaRuntimeDevMatmulRegion(allocator, io);
        },
        .write => {
            try writeMetalRuntimeRegion(allocator, io);
            try writeMetalLaunchShapeRegion(allocator, io);
            try writeCudaRuntimeAttentionRegion(allocator, io);
            try writeCudaRuntimeDevMatmulRegion(allocator, io);
        },
        .inspect_model, .check_model => unreachable,
    }
    if (mode == .check_metal) try checkMetalArtifacts(allocator, io);
    print("quant kernel generated sources {s}: {d} files + Metal/CUDA runtime regions\n", .{ @tagName(mode), files.len });
}

// The runtime-embedded copy of the generated Metal quant kernels lives in
// metal_kernels.m between these marker lines. The codegen tool owns the
// markers and everything between them; the compiler's section table renders
// the region content.
const metal_runtime_source_path = "src/backends/metal_kernels.m";
const metal_runtime_region_begin_marker = "           // quant-kernel-codegen:begin generated quant kernels (do not edit; run: zig build quant-kernel-codegen -- --write)";
const metal_runtime_region_end_marker = "           // quant-kernel-codegen:end generated quant kernels";

const MetalRuntimeRegionSplit = struct {
    prefix: []const u8,
    region: []const u8,
    suffix: []const u8,
};

fn splitMetalRuntimeRegion(contents: []const u8) !MetalRuntimeRegionSplit {
    const begin_line = metal_runtime_region_begin_marker ++ "\n";
    const end_line = metal_runtime_region_end_marker ++ "\n";
    const begin = std.mem.indexOf(u8, contents, begin_line) orelse return error.MetalRuntimeRegionMarkerMissing;
    const region_start = begin + begin_line.len;
    const end = std.mem.indexOfPos(u8, contents, region_start, end_line) orelse return error.MetalRuntimeRegionMarkerMissing;
    return .{
        .prefix = contents[0..region_start],
        .region = contents[region_start..end],
        .suffix = contents[end..],
    };
}

fn readMetalRuntimeSource(allocator: std.mem.Allocator, io: std.Io) ![]u8 {
    const path = try existingSourcePath(allocator, io, metal_runtime_source_path);
    defer if (path.owned) allocator.free(path.value);
    return std.Io.Dir.cwd().readFileAlloc(io, path.value, allocator, .limited(8 * 1024 * 1024));
}

fn checkMetalRuntimeRegion(allocator: std.mem.Allocator, io: std.Io) !void {
    const expected = try quant_kernel_compiler.renderMetalRuntimeQuantRegion(allocator);
    defer allocator.free(expected);
    const contents = try readMetalRuntimeSource(allocator, io);
    defer allocator.free(contents);
    const split = splitMetalRuntimeRegion(contents) catch |err| {
        print("quant kernel Metal runtime region markers missing in {s}; run `zig build quant-kernel-codegen -- --write`\n", .{metal_runtime_source_path});
        return err;
    };
    if (!std.mem.eql(u8, split.region, expected)) {
        print(
            "quant kernel Metal runtime region stale: path={s} expected_bytes={d} actual_bytes={d}; run `zig build quant-kernel-codegen -- --write`\n",
            .{ metal_runtime_source_path, expected.len, split.region.len },
        );
        return error.GeneratedQuantKernelSourceStale;
    }
    try checkMetalRuntimeExternalHelpers(allocator, split);
}

// Helpers that live in metal_kernels.m outside the codegen-owned region but
// are duplicated into standalone generated sources must keep matching the
// compiler's copy byte-for-byte.
fn checkMetalRuntimeExternalHelpers(allocator: std.mem.Allocator, split: MetalRuntimeRegionSplit) !void {
    for (quant_kernel_compiler.metal_runtime_external_helpers) |helper| {
        const fragment = try std.fmt.allocPrint(allocator, "           \"{s}\\n\"\n", .{helper});
        defer allocator.free(fragment);
        if (std.mem.indexOf(u8, split.prefix, fragment) == null and
            std.mem.indexOf(u8, split.suffix, fragment) == null)
        {
            print(
                "quant kernel Metal external helper drifted from compiler copy in {s}: {s}\n",
                .{ metal_runtime_source_path, helper },
            );
            return error.MetalRuntimeExternalHelperDrift;
        }
    }
}

fn writeMetalRuntimeRegion(allocator: std.mem.Allocator, io: std.Io) !void {
    const expected = try quant_kernel_compiler.renderMetalRuntimeQuantRegion(allocator);
    defer allocator.free(expected);
    const contents = try readMetalRuntimeSource(allocator, io);
    defer allocator.free(contents);
    const split = try splitMetalRuntimeRegion(contents);
    try checkMetalRuntimeExternalHelpers(allocator, split);
    if (std.mem.eql(u8, split.region, expected)) return;
    const updated = try std.mem.concat(allocator, u8, &.{ split.prefix, expected, split.suffix });
    defer allocator.free(updated);
    const path = try existingSourcePath(allocator, io, metal_runtime_source_path);
    defer if (path.owned) allocator.free(path.value);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path.value, .data = updated });
}

// Generated CUDA attention kernels are linked into the runtime artifact from
// this marker-delimited region. The standalone candidate sources and embedded
// bundle are both rendered from the compiler registry.
const cuda_runtime_source_path = "src/ops/cuda/artifacts/inference_cuda_kernels.cu";
const cuda_runtime_attention_region_begin_marker = "// quant-kernel-codegen:begin generated CUDA attention kernels (do not edit; run: zig build quant-kernel-codegen -- --write)";
const cuda_runtime_attention_region_end_marker = "// quant-kernel-codegen:end generated CUDA attention kernels";

const CudaRuntimeAttentionRegionSplit = struct {
    prefix: []const u8,
    region: []const u8,
    suffix: []const u8,
};

fn splitCudaRuntimeAttentionRegion(contents: []const u8) !CudaRuntimeAttentionRegionSplit {
    const begin_line = cuda_runtime_attention_region_begin_marker ++ "\n";
    const end_line = cuda_runtime_attention_region_end_marker ++ "\n";
    const begin_count = std.mem.count(u8, contents, begin_line);
    const end_count = std.mem.count(u8, contents, end_line);
    if (begin_count == 0 or end_count == 0) return error.CudaRuntimeAttentionRegionMarkerMissing;
    if (begin_count != 1 or end_count != 1) return error.CudaRuntimeAttentionRegionMarkerDuplicate;
    const begin = std.mem.indexOf(u8, contents, begin_line) orelse return error.CudaRuntimeAttentionRegionMarkerMissing;
    const region_start = begin + begin_line.len;
    const end = std.mem.indexOfPos(u8, contents, region_start, end_line) orelse return error.CudaRuntimeAttentionRegionMarkerMissing;
    return .{
        .prefix = contents[0..region_start],
        .region = contents[region_start..end],
        .suffix = contents[end..],
    };
}

fn cudaRuntimeAttentionRegionIsCurrent(contents: []const u8, expected: []const u8) !bool {
    const split = try splitCudaRuntimeAttentionRegion(contents);
    return std.mem.eql(u8, split.region, expected);
}

fn readCudaRuntimeSource(allocator: std.mem.Allocator, io: std.Io) ![]u8 {
    const path = try existingSourcePath(allocator, io, cuda_runtime_source_path);
    defer if (path.owned) allocator.free(path.value);
    return std.Io.Dir.cwd().readFileAlloc(io, path.value, allocator, .limited(8 * 1024 * 1024));
}

fn checkCudaRuntimeAttentionRegion(allocator: std.mem.Allocator, io: std.Io) !void {
    const expected = try quant_kernel_compiler.renderCudaRuntimeAttentionRegion(allocator);
    defer allocator.free(expected);
    const contents = try readCudaRuntimeSource(allocator, io);
    defer allocator.free(contents);
    const split = splitCudaRuntimeAttentionRegion(contents) catch |err| {
        print("quant kernel CUDA attention runtime region markers missing or duplicated in {s}; run `zig build quant-kernel-codegen -- --write`\n", .{cuda_runtime_source_path});
        return err;
    };
    quant_kernel_compiler.validateCudaRuntimeAttentionExternalHelpers(split.prefix) catch |err| {
        print("quant kernel CUDA attention runtime helper drift in {s}; keep the shared fast-attention reduction helpers before the generated region\n", .{cuda_runtime_source_path});
        return err;
    };
    if (!std.mem.eql(u8, split.region, expected)) {
        print(
            "quant kernel CUDA attention runtime region stale: path={s} expected_bytes={d} actual_bytes={d}; run `zig build quant-kernel-codegen -- --write`\n",
            .{ cuda_runtime_source_path, expected.len, split.region.len },
        );
        return error.GeneratedQuantKernelSourceStale;
    }
}

fn writeCudaRuntimeAttentionRegion(allocator: std.mem.Allocator, io: std.Io) !void {
    const expected = try quant_kernel_compiler.renderCudaRuntimeAttentionRegion(allocator);
    defer allocator.free(expected);
    const contents = try readCudaRuntimeSource(allocator, io);
    defer allocator.free(contents);
    const split = try splitCudaRuntimeAttentionRegion(contents);
    try quant_kernel_compiler.validateCudaRuntimeAttentionExternalHelpers(split.prefix);
    if (std.mem.eql(u8, split.region, expected)) return;
    const updated = try std.mem.concat(allocator, u8, &.{ split.prefix, expected, split.suffix });
    defer allocator.free(updated);
    const path = try existingSourcePath(allocator, io, cuda_runtime_source_path);
    defer if (path.owned) allocator.free(path.value);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path.value, .data = updated });
}

const cuda_runtime_dev_matmul_region_begin_marker = "// quant-kernel-codegen:begin generated CUDA runtime-wired dev matmul candidates (do not edit; run: zig build quant-kernel-codegen -- --write)";
const cuda_runtime_dev_matmul_region_end_marker = "// quant-kernel-codegen:end generated CUDA runtime-wired dev matmul candidates";

const CudaRuntimeDevMatmulRegionSplit = struct {
    prefix: []const u8,
    region: []const u8,
    suffix: []const u8,
};

fn splitCudaRuntimeDevMatmulRegion(contents: []const u8) !CudaRuntimeDevMatmulRegionSplit {
    const begin_line = cuda_runtime_dev_matmul_region_begin_marker ++ "\n";
    const end_line = cuda_runtime_dev_matmul_region_end_marker ++ "\n";
    const begin_count = std.mem.count(u8, contents, begin_line);
    const end_count = std.mem.count(u8, contents, end_line);
    if (begin_count == 0 or end_count == 0) return error.CudaRuntimeDevMatmulRegionMarkerMissing;
    if (begin_count != 1 or end_count != 1) return error.CudaRuntimeDevMatmulRegionMarkerDuplicate;
    const begin = std.mem.indexOf(u8, contents, begin_line) orelse return error.CudaRuntimeDevMatmulRegionMarkerMissing;
    const region_start = begin + begin_line.len;
    const end = std.mem.indexOfPos(u8, contents, region_start, end_line) orelse return error.CudaRuntimeDevMatmulRegionMarkerMissing;
    return .{
        .prefix = contents[0..region_start],
        .region = contents[region_start..end],
        .suffix = contents[end..],
    };
}

fn cudaRuntimeDevMatmulRegionIsCurrent(contents: []const u8, expected: []const u8) !bool {
    const split = try splitCudaRuntimeDevMatmulRegion(contents);
    return std.mem.eql(u8, split.region, expected);
}

fn checkCudaRuntimeDevMatmulRegion(allocator: std.mem.Allocator, io: std.Io) !void {
    const expected = try quant_kernel_compiler.renderCudaRuntimeDevMatmulRegion(allocator);
    defer allocator.free(expected);
    const contents = try readCudaRuntimeSource(allocator, io);
    defer allocator.free(contents);
    const split = splitCudaRuntimeDevMatmulRegion(contents) catch |err| {
        print("quant kernel CUDA dev matmul runtime region markers missing or duplicated in {s}; run `zig build quant-kernel-codegen -- --write`\n", .{cuda_runtime_source_path});
        return err;
    };
    quant_kernel_compiler.validateCudaRuntimeDevMatmulExternalHelpers(split.prefix) catch |err| {
        print("quant kernel CUDA dev matmul external helpers drifted or moved after the owned region in {s}\n", .{cuda_runtime_source_path});
        return err;
    };
    if (!std.mem.eql(u8, split.region, expected)) {
        print(
            "quant kernel CUDA dev matmul runtime region stale: path={s} expected_bytes={d} actual_bytes={d}; run `zig build quant-kernel-codegen -- --write`\n",
            .{ cuda_runtime_source_path, expected.len, split.region.len },
        );
        return error.GeneratedQuantKernelSourceStale;
    }
}

fn writeCudaRuntimeDevMatmulRegion(allocator: std.mem.Allocator, io: std.Io) !void {
    const expected = try quant_kernel_compiler.renderCudaRuntimeDevMatmulRegion(allocator);
    defer allocator.free(expected);
    const contents = try readCudaRuntimeSource(allocator, io);
    defer allocator.free(contents);
    const split = try splitCudaRuntimeDevMatmulRegion(contents);
    try quant_kernel_compiler.validateCudaRuntimeDevMatmulExternalHelpers(split.prefix);
    if (std.mem.eql(u8, split.region, expected)) return;
    const updated = try std.mem.concat(allocator, u8, &.{ split.prefix, expected, split.suffix });
    defer allocator.free(updated);
    const path = try existingSourcePath(allocator, io, cuda_runtime_source_path);
    defer if (path.owned) allocator.free(path.value);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path.value, .data = updated });
}

// The generated launch-shape table + lookup in metal_kernels.m lives between
// these markers (file scope, no indentation). Rendered from the compiler's
// metal_production_schedules table.
const metal_launch_region_begin_marker = "// quant-kernel-codegen:begin generated launch-shape table (do not edit; run: zig build quant-kernel-codegen -- --write)";
const metal_launch_region_end_marker = "// quant-kernel-codegen:end generated launch-shape table";

fn splitMetalLaunchRegion(contents: []const u8) !MetalRuntimeRegionSplit {
    const begin_line = metal_launch_region_begin_marker ++ "\n";
    const end_line = metal_launch_region_end_marker ++ "\n";
    const begin = std.mem.indexOf(u8, contents, begin_line) orelse return error.MetalLaunchRegionMarkerMissing;
    const region_start = begin + begin_line.len;
    const end = std.mem.indexOfPos(u8, contents, region_start, end_line) orelse return error.MetalLaunchRegionMarkerMissing;
    return .{
        .prefix = contents[0..region_start],
        .region = contents[region_start..end],
        .suffix = contents[end..],
    };
}

fn checkMetalLaunchShapeRegion(allocator: std.mem.Allocator, io: std.Io) !void {
    const expected = try quant_kernel_compiler.renderMetalLaunchShapeRegion(allocator);
    defer allocator.free(expected);
    const contents = try readMetalRuntimeSource(allocator, io);
    defer allocator.free(contents);
    const split = splitMetalLaunchRegion(contents) catch |err| {
        print("quant kernel Metal launch-shape region markers missing in {s}; run `zig build quant-kernel-codegen -- --write`\n", .{metal_runtime_source_path});
        return err;
    };
    if (!std.mem.eql(u8, split.region, expected)) {
        print(
            "quant kernel Metal launch-shape region stale: path={s} expected_bytes={d} actual_bytes={d}; run `zig build quant-kernel-codegen -- --write`\n",
            .{ metal_runtime_source_path, expected.len, split.region.len },
        );
        return error.GeneratedQuantKernelSourceStale;
    }
}

fn writeMetalLaunchShapeRegion(allocator: std.mem.Allocator, io: std.Io) !void {
    const expected = try quant_kernel_compiler.renderMetalLaunchShapeRegion(allocator);
    defer allocator.free(expected);
    const contents = try readMetalRuntimeSource(allocator, io);
    defer allocator.free(contents);
    const split = try splitMetalLaunchRegion(contents);
    if (std.mem.eql(u8, split.region, expected)) return;
    const updated = try std.mem.concat(allocator, u8, &.{ split.prefix, expected, split.suffix });
    defer allocator.free(updated);
    const path = try existingSourcePath(allocator, io, metal_runtime_source_path);
    defer if (path.owned) allocator.free(path.value);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path.value, .data = updated });
}

fn parseMode(args: []const []const u8) !Mode {
    return (try parseOptions(args)).mode;
}

fn parseOptions(args: []const []const u8) !Options {
    if (args.len == 0) return .{ .mode = .check };
    if (args.len == 1 and std.mem.eql(u8, args[0], "--check")) return .{ .mode = .check };
    if (args.len == 1 and std.mem.eql(u8, args[0], "--write")) return .{ .mode = .write };
    if (args.len == 1 and std.mem.eql(u8, args[0], "--check-metal")) return .{ .mode = .check_metal };
    if (args.len == 2 and std.mem.eql(u8, args[0], "--inspect-model")) {
        return .{ .mode = .inspect_model, .model_path = args[1] };
    }
    if (args.len == 2 and std.mem.eql(u8, args[0], "--check-model")) {
        return .{ .mode = .check_model, .model_path = args[1] };
    }
    printUsage();
    return error.InvalidArguments;
}

fn printUsage() void {
    print(
        \\usage: antfly inference quant-kernel-codegen [--check|--write|--check-metal]
        \\       antfly inference quant-kernel-codegen --inspect-model <model.gguf>
        \\       antfly inference quant-kernel-codegen --check-model <model.gguf>
        \\  Rewrites or verifies dev-generated quant kernel sources, manifests, and Metal artifacts.
        \\  Model inspection emits normalized decode signatures and compiler catalog coverage as JSON.
        \\
    , .{});
}

fn generatedFiles(allocator: std.mem.Allocator) ![]GeneratedFile {
    try quant_kernel_compiler.validateGeneratedArtifactRegistry();
    const source_count = generatedSourceFileCount();
    const files = try allocator.alloc(GeneratedFile, source_count + 4);
    @memset(files, .{ .path = "", .data = "" });
    errdefer freeGeneratedFiles(allocator, files);
    var index: usize = 0;
    for (quant_kernel_compiler.first_generated_artifacts) |artifact| {
        const source = try compiledSourceForArtifact(allocator, artifact);
        files[index] = .{
            .path = artifact.source_path,
            .data = source.data,
            .owned = source.owned,
        };
        index += 1;
    }
    if (index != source_count) return error.GeneratedQuantKernelSourceCountMismatch;
    var manifest_index = index;
    files[manifest_index] = .{
        .path = quant_kernel_compiler.first_spec_manifest_path,
        .data = try quant_kernel_compiler.specManifestJson(allocator),
        .owned = true,
    };
    manifest_index += 1;
    files[manifest_index] = .{
        .path = quant_kernel_compiler.first_artifact_manifest_path,
        .data = try quant_kernel_compiler.artifactManifestJson(allocator),
        .owned = true,
    };
    manifest_index += 1;
    files[manifest_index] = .{
        .path = quant_kernel_compiler.first_benchmark_manifest_path,
        .data = try quant_kernel_compiler.benchmarkManifestJson(allocator),
        .owned = true,
    };
    manifest_index += 1;
    files[manifest_index] = .{
        .path = quant_kernel_compiler.first_conformance_manifest_path,
        .data = try quant_kernel_compiler.conformanceManifestJson(allocator),
        .owned = true,
    };
    return files;
}

fn compiledSourceForArtifact(
    allocator: std.mem.Allocator,
    artifact: quant_kernel_compiler.GeneratedArtifact,
) !CompiledSourceData {
    // Routing seam for the op-kind framework. Small-batch matmul flows through
    // the quant-kernel compile path; microkernels (RMSNorm) and attention
    // (decode-1x paged) render their self-contained body via the renderer's
    // op-kind paths — no dequant decoder, no matmul lowering. Both return the
    // comptime-rendered source constant directly.
    switch (artifact.opKind()) {
        .small_batch_matmul => {},
        .microkernel, .attention => {
            const source = quant_kernel_compiler.generatedSourceForArtifact(artifact) orelse
                return error.MissingGeneratedQuantKernelSource;
            return .{ .data = source, .owned = false };
        },
    }
    const op = artifact.matmulOp() orelse return error.InvalidGeneratedArtifactOp;
    _ = op;
    const compiled = quant_kernel_compiler.compileQuantKernelArtifactSource(
        quant_kernel_compiler.matmulArtifactView(artifact),
    ) orelse return error.MissingGeneratedQuantKernelSource;
    if (!std.mem.eql(u8, compiled.artifact.kernel_id, artifact.kernel_id)) {
        return error.MissingGeneratedQuantKernelSource;
    }
    const emitted = try quant_kernel_compiler.emitCompiledSource(allocator, compiled);
    errdefer emitted.deinit(allocator);
    if (!try quant_kernel_compiler.compiledSourceHeaderMatchesSource(allocator, compiled, emitted.data)) {
        return error.GeneratedQuantKernelSourceHeaderMismatch;
    }
    return .{ .data = emitted.data, .owned = emitted.owned };
}

fn generatedSourceFileCount() usize {
    return quant_kernel_compiler.first_generated_artifacts.len;
}

fn freeGeneratedFiles(allocator: std.mem.Allocator, files: []GeneratedFile) void {
    for (files) |file| {
        if (file.owned) allocator.free(file.data);
    }
    allocator.free(files);
}

fn checkSource(allocator: std.mem.Allocator, io: std.Io, source: GeneratedFile) !void {
    const path = existingSourcePath(allocator, io, source.path) catch |err| {
        if (err == error.GeneratedQuantKernelSourceMissing) {
            print("quant kernel generated source missing: path={s}; run `zig build quant-kernel-codegen -- --write`\n", .{source.path});
        }
        return err;
    };
    defer if (path.owned) allocator.free(path.value);
    const max_bytes = @max(source.data.len + 1024 * 1024, 4 * 1024 * 1024);
    const actual = try std.Io.Dir.cwd().readFileAlloc(io, path.value, allocator, .limited(max_bytes));
    defer allocator.free(actual);
    if (!std.mem.eql(u8, source.data, actual)) {
        print(
            "quant kernel generated source stale: path={s} expected_bytes={d} actual_bytes={d}; run `zig build quant-kernel-codegen -- --write`\n",
            .{ path.value, source.data.len, actual.len },
        );
        return error.GeneratedQuantKernelSourceStale;
    }
}

fn writeSource(allocator: std.mem.Allocator, io: std.Io, source: GeneratedFile) !void {
    const path = try writableSourcePath(allocator, io, source.path);
    defer if (path.owned) allocator.free(path.value);
    try ensureParentDir(io, path.value);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path.value, .data = source.data });
}

const ResolvedPath = struct {
    value: []const u8,
    owned: bool = false,
};

fn existingSourcePath(allocator: std.mem.Allocator, io: std.Io, package_path: []const u8) !ResolvedPath {
    if (pathExists(io, package_path)) return .{ .value = package_path };
    const repo_path = try std.fmt.allocPrint(allocator, "zig/pkg/inference/{s}", .{package_path});
    if (pathExists(io, repo_path)) return .{ .value = repo_path, .owned = true };
    allocator.free(repo_path);
    return error.GeneratedQuantKernelSourceMissing;
}

fn writableSourcePath(allocator: std.mem.Allocator, io: std.Io, package_path: []const u8) !ResolvedPath {
    if (existingSourcePath(allocator, io, package_path)) |path| {
        return path;
    } else |err| switch (err) {
        error.GeneratedQuantKernelSourceMissing => {},
        else => return err,
    }
    if (pathExists(io, "zig/pkg/inference/build.zig")) {
        return .{
            .value = try std.fmt.allocPrint(allocator, "zig/pkg/inference/{s}", .{package_path}),
            .owned = true,
        };
    }
    return .{ .value = package_path };
}

fn pathExists(io: std.Io, path: []const u8) bool {
    std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    return true;
}

fn ensureParentDir(io: std.Io, path: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse return;
    if (parent.len == 0) return;
    try std.Io.Dir.cwd().createDirPath(io, parent);
}

fn checkMetalArtifacts(allocator: std.mem.Allocator, io: std.Io) !void {
    const temp_dir = try std.fmt.allocPrint(allocator, "/tmp/antfly-quant-kernel-codegen-metal-{d}", .{std.posix.system.getpid()});
    defer allocator.free(temp_dir);
    const module_cache_arg = try std.fmt.allocPrint(allocator, "-fmodules-cache-path={s}", .{temp_dir});
    defer allocator.free(module_cache_arg);
    std.Io.Dir.cwd().deleteTree(io, temp_dir) catch {};
    try std.Io.Dir.cwd().createDirPath(io, temp_dir);
    defer std.Io.Dir.cwd().deleteTree(io, temp_dir) catch {};

    var count: usize = 0;
    for (quant_kernel_compiler.first_generated_artifacts) |artifact| {
        if (artifact.backend != .metal) continue;
        try checkMetalArtifact(allocator, io, temp_dir, module_cache_arg, artifact);
        count += 1;
    }
    const v2_count = try checkMetalRenderedV2(allocator, io, temp_dir, module_cache_arg);
    print("quant kernel generated Metal check: {d} artifacts + {d} rendered v2\n", .{ count, v2_count });
}

// Renders every metal_production_schedules route through the descriptor-driven
// renderer and compiles it with `xcrun metal`. Shadow-only: these v2 sources are
// not checked in; this proves the renderer emits compilable MSL for every route.
fn checkMetalRenderedV2(allocator: std.mem.Allocator, io: std.Io, temp_dir: []const u8, module_cache_arg: []const u8) !usize {
    const renderer = quant_kernel_compiler_renderer;
    var count: usize = 0;
    for (quant_kernel_compiler.metal_production_schedules) |entry| {
        const decoder = renderer.decoderFor(entry.format) orelse return error.MissingV2Decoder;
        const suffix = switch (entry.epilogue) {
            .none => "",
            .bias => "_bias",
            .bias_gelu => "_bias_gelu",
            .relu => "_relu",
            else => return error.UnsupportedV2Epilogue,
        };
        const kernel_id = try std.fmt.allocPrint(allocator, "antfly_{s}_small_batch{s}_msl_v2", .{ @tagName(entry.format), suffix });
        defer allocator.free(kernel_id);
        const body = try renderer.renderKernel(allocator, kernel_id, decoder, entry.schedule, entry.epilogue);
        defer allocator.free(body);
        const full = try std.fmt.allocPrint(allocator, "#include <metal_stdlib>\nusing namespace metal;\n{s}", .{body});
        defer allocator.free(full);
        const src_path = try std.fmt.allocPrint(allocator, "{s}/{s}.metal", .{ temp_dir, kernel_id });
        defer allocator.free(src_path);
        const air_path = try std.fmt.allocPrint(allocator, "{s}/{s}.air", .{ temp_dir, kernel_id });
        defer allocator.free(air_path);
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = src_path, .data = full });
        var child = try std.process.spawn(io, .{
            .argv = &.{ "xcrun", "--toolchain", "Metal", "metal", module_cache_arg, "-c", src_path, "-o", air_path },
            .stdin = .ignore,
            .stdout = .inherit,
            .stderr = .inherit,
        });
        const term = try child.wait(io);
        switch (term) {
            .exited => |code| if (code != 0) {
                print("rendered v2 kernel failed to compile: {s}\n", .{kernel_id});
                return error.MetalV2CheckFailed;
            },
            else => return error.MetalV2CheckFailed,
        }
        count += 1;
    }
    return count;
}

fn checkMetalArtifact(allocator: std.mem.Allocator, io: std.Io, temp_dir: []const u8, module_cache_arg: []const u8, artifact: quant_kernel_compiler.GeneratedArtifact) !void {
    const source_path = try existingSourcePath(allocator, io, artifact.source_path);
    defer if (source_path.owned) allocator.free(source_path.value);
    _ = metalCheckOutputPath(artifact.check_command) orelse return error.InvalidMetalCheckCommand;
    const output_path = try std.fmt.allocPrint(allocator, "{s}/{s}.air", .{ temp_dir, artifact.kernel_id });
    defer allocator.free(output_path);
    var child = try std.process.spawn(io, .{
        .argv = &.{ "xcrun", "--toolchain", "Metal", "metal", module_cache_arg, "-c", source_path.value, "-o", output_path },
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .inherit,
    });
    const term = try child.wait(io);
    switch (term) {
        .exited => |code| if (code != 0) return error.MetalCheckFailed,
        else => return error.MetalCheckFailed,
    }
}

fn metalCheckOutputPath(command: []const u8) ?[]const u8 {
    var tokens = std.mem.tokenizeScalar(u8, command, ' ');
    while (tokens.next()) |token| {
        if (!std.mem.eql(u8, token, "-o")) continue;
        return tokens.next();
    }
    return null;
}

test "quant kernel codegen splits the Metal runtime region on marker lines" {
    const contents = "prefix\n" ++
        metal_runtime_region_begin_marker ++ "\n" ++
        "           \"kernel line\\n\"\n" ++
        metal_runtime_region_end_marker ++ "\n" ++
        "suffix\n";
    const split = try splitMetalRuntimeRegion(contents);
    try std.testing.expectEqualStrings("           \"kernel line\\n\"\n", split.region);
    try std.testing.expect(std.mem.startsWith(u8, split.prefix, "prefix\n"));
    try std.testing.expect(std.mem.startsWith(u8, split.suffix, metal_runtime_region_end_marker));
    try std.testing.expectError(error.MetalRuntimeRegionMarkerMissing, splitMetalRuntimeRegion("no markers"));
}

test "quant kernel codegen Metal runtime region matches the checked-in runtime source" {
    const expected = try quant_kernel_compiler.renderMetalRuntimeQuantRegion(std.testing.allocator);
    defer std.testing.allocator.free(expected);
    const contents = try readMetalRuntimeSource(std.testing.allocator, std.testing.io);
    defer std.testing.allocator.free(contents);
    const split = try splitMetalRuntimeRegion(contents);
    try std.testing.expectEqualStrings(expected, split.region);
    try checkMetalRuntimeExternalHelpers(std.testing.allocator, split);
}

test "quant kernel codegen splits the CUDA attention runtime region on marker lines" {
    const contents = "prefix\n" ++
        cuda_runtime_attention_region_begin_marker ++ "\n" ++
        "generated kernel\n" ++
        cuda_runtime_attention_region_end_marker ++ "\n" ++
        "suffix\n";
    const split = try splitCudaRuntimeAttentionRegion(contents);
    try std.testing.expectEqualStrings("generated kernel\n", split.region);
    try std.testing.expect(std.mem.startsWith(u8, split.prefix, "prefix\n"));
    try std.testing.expect(std.mem.startsWith(u8, split.suffix, cuda_runtime_attention_region_end_marker));
    try std.testing.expectError(error.CudaRuntimeAttentionRegionMarkerMissing, splitCudaRuntimeAttentionRegion("no markers"));
    try std.testing.expectError(
        error.CudaRuntimeAttentionRegionMarkerMissing,
        splitCudaRuntimeAttentionRegion(cuda_runtime_attention_region_begin_marker ++ "\nbody\n"),
    );
    try std.testing.expectError(
        error.CudaRuntimeAttentionRegionMarkerMissing,
        splitCudaRuntimeAttentionRegion("body\n" ++ cuda_runtime_attention_region_end_marker ++ "\n"),
    );
    const reversed = cuda_runtime_attention_region_end_marker ++ "\nbody\n" ++ cuda_runtime_attention_region_begin_marker ++ "\n";
    try std.testing.expectError(error.CudaRuntimeAttentionRegionMarkerMissing, splitCudaRuntimeAttentionRegion(reversed));

    const duplicate_begin = contents ++ cuda_runtime_attention_region_begin_marker ++ "\n";
    try std.testing.expectError(error.CudaRuntimeAttentionRegionMarkerDuplicate, splitCudaRuntimeAttentionRegion(duplicate_begin));
    const duplicate_end = contents ++ cuda_runtime_attention_region_end_marker ++ "\n";
    try std.testing.expectError(error.CudaRuntimeAttentionRegionMarkerDuplicate, splitCudaRuntimeAttentionRegion(duplicate_end));
    const orphan_end_before = cuda_runtime_attention_region_end_marker ++ "\n" ++ contents;
    try std.testing.expectError(error.CudaRuntimeAttentionRegionMarkerDuplicate, splitCudaRuntimeAttentionRegion(orphan_end_before));
}

test "quant kernel codegen checks CUDA attention runtime region contents" {
    const expected = "generated kernel\n";
    const current = "prefix\n" ++
        cuda_runtime_attention_region_begin_marker ++ "\n" ++
        expected ++
        cuda_runtime_attention_region_end_marker ++ "\n" ++
        "suffix\n";
    try std.testing.expect(try cudaRuntimeAttentionRegionIsCurrent(current, expected));
    try std.testing.expect(!try cudaRuntimeAttentionRegionIsCurrent(current, "stale kernel\n"));
}

test "quant kernel codegen CUDA attention runtime region matches the checked-in bundle" {
    const expected = try quant_kernel_compiler.renderCudaRuntimeAttentionRegion(std.testing.allocator);
    defer std.testing.allocator.free(expected);
    const contents = try readCudaRuntimeSource(std.testing.allocator, std.testing.io);
    defer std.testing.allocator.free(contents);
    const split = try splitCudaRuntimeAttentionRegion(contents);
    try quant_kernel_compiler.validateCudaRuntimeAttentionExternalHelpers(split.prefix);
    try std.testing.expectEqualStrings(expected, split.region);
}

test "quant kernel codegen splits the CUDA dev matmul runtime region on exactly one marker pair" {
    const contents = "prefix\n" ++
        cuda_runtime_dev_matmul_region_begin_marker ++ "\n" ++
        "generated kernel\n" ++
        cuda_runtime_dev_matmul_region_end_marker ++ "\n" ++
        "suffix\n";
    const split = try splitCudaRuntimeDevMatmulRegion(contents);
    try std.testing.expectEqualStrings("generated kernel\n", split.region);
    try std.testing.expect(std.mem.startsWith(u8, split.prefix, "prefix\n"));
    try std.testing.expect(std.mem.startsWith(u8, split.suffix, cuda_runtime_dev_matmul_region_end_marker));
    try std.testing.expectError(error.CudaRuntimeDevMatmulRegionMarkerMissing, splitCudaRuntimeDevMatmulRegion("no markers"));
    try std.testing.expectError(
        error.CudaRuntimeDevMatmulRegionMarkerMissing,
        splitCudaRuntimeDevMatmulRegion(cuda_runtime_dev_matmul_region_begin_marker ++ "\nbody\n"),
    );
    try std.testing.expectError(
        error.CudaRuntimeDevMatmulRegionMarkerMissing,
        splitCudaRuntimeDevMatmulRegion("body\n" ++ cuda_runtime_dev_matmul_region_end_marker ++ "\n"),
    );
    const reversed = cuda_runtime_dev_matmul_region_end_marker ++ "\nbody\n" ++ cuda_runtime_dev_matmul_region_begin_marker ++ "\n";
    try std.testing.expectError(error.CudaRuntimeDevMatmulRegionMarkerMissing, splitCudaRuntimeDevMatmulRegion(reversed));

    const duplicate_begin = contents ++ cuda_runtime_dev_matmul_region_begin_marker ++ "\n";
    try std.testing.expectError(error.CudaRuntimeDevMatmulRegionMarkerDuplicate, splitCudaRuntimeDevMatmulRegion(duplicate_begin));
    const duplicate_end = contents ++ cuda_runtime_dev_matmul_region_end_marker ++ "\n";
    try std.testing.expectError(error.CudaRuntimeDevMatmulRegionMarkerDuplicate, splitCudaRuntimeDevMatmulRegion(duplicate_end));
    const orphan_end_before = cuda_runtime_dev_matmul_region_end_marker ++ "\n" ++ contents;
    try std.testing.expectError(error.CudaRuntimeDevMatmulRegionMarkerDuplicate, splitCudaRuntimeDevMatmulRegion(orphan_end_before));
}

test "quant kernel codegen checks CUDA dev matmul runtime region contents" {
    const expected = "generated kernel\n";
    const current = "prefix\n" ++
        cuda_runtime_dev_matmul_region_begin_marker ++ "\n" ++
        expected ++
        cuda_runtime_dev_matmul_region_end_marker ++ "\n" ++
        "suffix\n";
    try std.testing.expect(try cudaRuntimeDevMatmulRegionIsCurrent(current, expected));
    try std.testing.expect(!try cudaRuntimeDevMatmulRegionIsCurrent(current, "stale kernel\n"));
}

test "quant kernel codegen CUDA dev matmul runtime region matches the checked-in bundle" {
    const expected = try quant_kernel_compiler.renderCudaRuntimeDevMatmulRegion(std.testing.allocator);
    defer std.testing.allocator.free(expected);
    const contents = try readCudaRuntimeSource(std.testing.allocator, std.testing.io);
    defer std.testing.allocator.free(contents);
    const split = try splitCudaRuntimeDevMatmulRegion(contents);
    try std.testing.expectEqualStrings(expected, split.region);
    try quant_kernel_compiler.validateCudaRuntimeDevMatmulExternalHelpers(split.prefix);
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, expected, "extern \"C\" __global__ void antfly_q4_0_q8_1_argmax_rows_stage1_tile8_v1("));
}

test "quant kernel codegen splits the Metal launch-shape region on marker lines" {
    const contents = "prefix\n" ++
        metal_launch_region_begin_marker ++ "\n" ++
        "static const int x = 1;\n" ++
        metal_launch_region_end_marker ++ "\n" ++
        "suffix\n";
    const split = try splitMetalLaunchRegion(contents);
    try std.testing.expectEqualStrings("static const int x = 1;\n", split.region);
    try std.testing.expect(std.mem.startsWith(u8, split.suffix, metal_launch_region_end_marker));
    try std.testing.expectError(error.MetalLaunchRegionMarkerMissing, splitMetalLaunchRegion("no markers"));
}

test "quant kernel codegen Metal launch-shape region matches the checked-in runtime source" {
    const expected = try quant_kernel_compiler.renderMetalLaunchShapeRegion(std.testing.allocator);
    defer std.testing.allocator.free(expected);
    const contents = try readMetalRuntimeSource(std.testing.allocator, std.testing.io);
    defer std.testing.allocator.free(contents);
    const split = try splitMetalLaunchRegion(contents);
    try std.testing.expectEqualStrings(expected, split.region);
}

test "quant kernel codegen defaults to check mode" {
    try std.testing.expectEqual(Mode.check, try parseMode(&.{}));
    try std.testing.expectEqual(Mode.check, try parseMode(&.{"--check"}));
    try std.testing.expectEqual(Mode.write, try parseMode(&.{"--write"}));
    try std.testing.expectEqual(Mode.check_metal, try parseMode(&.{"--check-metal"}));
}

test "quant kernel codegen parses read-only model catalog modes" {
    const inspect = try parseOptions(&.{ "--inspect-model", "model.gguf" });
    try std.testing.expectEqual(Mode.inspect_model, inspect.mode);
    try std.testing.expectEqualStrings("model.gguf", inspect.model_path.?);

    const check = try parseOptions(&.{ "--check-model", "model.gguf" });
    try std.testing.expectEqual(Mode.check_model, check.mode);
    try std.testing.expectEqualStrings("model.gguf", check.model_path.?);
    try std.testing.expectError(error.InvalidArguments, parseOptions(&.{"--inspect-model"}));
    try std.testing.expectError(error.InvalidArguments, parseOptions(&.{ "--check", "model.gguf" }));
}

test "quant kernel codegen sources map to compiler artifacts" {
    const sources = try generatedFiles(std.testing.allocator);
    defer freeGeneratedFiles(std.testing.allocator, sources);

    try std.testing.expectEqual(generatedSourceFileCount() + 4, sources.len);
    for (quant_kernel_compiler.first_generated_artifacts, sources[0..quant_kernel_compiler.first_generated_artifacts.len]) |artifact, source| {
        try std.testing.expectEqualStrings(artifact.source_path, source.path);
        try std.testing.expect(source.data.len != 0);
        if (artifact.backend == .cuda and artifact.opKind() == .small_batch_matmul) {
            try std.testing.expect(source.owned);
        } else {
            try std.testing.expect(!source.owned);
        }
    }

    const spec_manifest = sources[sources.len - 4];
    try std.testing.expectEqualStrings(quant_kernel_compiler.first_spec_manifest_path, spec_manifest.path);
    try std.testing.expect(std.mem.containsAtLeast(u8, spec_manifest.data, 1, "antfly.quant_kernel_specs.v1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, spec_manifest.data, 1, "\"format_count\": 28"));
    try std.testing.expect(std.mem.containsAtLeast(u8, spec_manifest.data, 1, "\"format\": \"q4_k\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, spec_manifest.data, 1, "\"block_bytes\": 144"));
    try std.testing.expect(std.mem.containsAtLeast(u8, spec_manifest.data, 1, "\"name\": \"qs\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, spec_manifest.data, 1, "load block_q4_K"));

    const artifact_manifest = sources[sources.len - 3];
    try std.testing.expectEqualStrings(quant_kernel_compiler.first_artifact_manifest_path, artifact_manifest.path);
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, quant_kernel_compiler.first_artifact_manifest_schema));
    const artifact_count = try std.fmt.allocPrint(std.testing.allocator, "\"artifact_count\": {d}", .{quant_kernel_compiler.first_generated_matmul_artifacts.len});
    defer std.testing.allocator.free(artifact_count);
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, artifact_count));
    const registry_artifact_count = try std.fmt.allocPrint(std.testing.allocator, "\"registry_artifact_count\": {d}", .{quant_kernel_compiler.first_generated_artifacts.len});
    defer std.testing.allocator.free(registry_artifact_count);
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, registry_artifact_count));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, "\"op_kind\": \"microkernel\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, "\"op_kind\": \"attention\""));
    var runtime_evidence_count: usize = 0;
    var runtime_route_evidence_count: usize = 0;
    var promotion_evidence_count: usize = 0;
    var promotion_check_count: usize = 0;
    var metal_evidence_count: usize = 0;
    for (quant_kernel_compiler.first_generated_matmul_artifacts) |artifact| {
        try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, artifact.kernel_id));
        try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, artifact.source_path));
        try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, artifact.check_command));
        if (artifact.runtime_evidence_command.len != 0) runtime_evidence_count += 1;
        if (quant_kernel_compiler.artifactNeedsRuntimeRouteEvidence(artifact)) runtime_route_evidence_count += 1;
        if (artifact.promotion_evidence_command.len != 0) promotion_evidence_count += 1;
        if (artifact.promotion_check_command.len != 0) promotion_check_count += 1;
        if (artifact.backend == .metal and artifact.runtime_evidence_command.len != 0) metal_evidence_count += 1;
    }
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, runtime_evidence_count, "\"runtime_evidence_command\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, runtime_route_evidence_count, "\"runtime_route_evidence_command\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, "--runtime-route-kernel"));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, promotion_evidence_count, "\"promotion_evidence_command\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, promotion_check_count, "\"promotion_check_command\":"));
    const checked_in_metal_evidence_count = try std.fmt.allocPrint(std.testing.allocator, "\"checked_in_metal_evidence_count\": {d}", .{quant_kernel_compiler.first_metal_runtime_evidence_count});
    defer std.testing.allocator.free(checked_in_metal_evidence_count);
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, checked_in_metal_evidence_count));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, quant_kernel_compiler.first_general_metal_q4_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, quant_kernel_compiler.first_general_metal_q8_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, quant_kernel_compiler.first_general_metal_q8_bias_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, quant_kernel_compiler.first_general_metal_q5_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, quant_kernel_compiler.first_general_metal_q6_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, quant_kernel_compiler.first_lazy_benchmark.benchmark_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, quant_kernel_compiler.first_lazy_benchmark_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, metal_evidence_count, quant_kernel_compiler.first_metal_runtime_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, quant_kernel_compiler.first_metal_local_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, quant_kernel_compiler.first_metal_runtime_route_all_build_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, quant_kernel_compiler.first_metal_runtime_route_all_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, quant_kernel_compiler.first_metal_runtime_route_all_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, metal_evidence_count, quant_kernel_compiler.first_metal_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, metal_evidence_count, quant_kernel_compiler.first_metal_promotion_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, metal_evidence_count, "--require-kernel"));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, metal_evidence_count, "\"metal_promotion_min_speedup\": 1.02"));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, metal_evidence_count, "\"metal_promotion_repeat_runs\": 5"));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, metal_evidence_count, "\"metal_promotion_warmup_repeat_runs\": 2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, quant_kernel_compiler.first_metal_runtime_evidence_count, "\"candidate_status\": \"promoted\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, "\"candidate_status\": \"dev_only_candidate\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, artifact_manifest.data, 1, "\"candidate_status\": \"blocked_by_evidence\""));

    const benchmark_manifest = sources[sources.len - 2];
    try std.testing.expectEqualStrings(quant_kernel_compiler.first_benchmark_manifest_path, benchmark_manifest.path);
    try std.testing.expect(std.mem.containsAtLeast(u8, benchmark_manifest.data, 1, quant_kernel_compiler.first_benchmark_manifest_schema));
    const benchmark_count = try std.fmt.allocPrint(std.testing.allocator, "\"benchmark_count\": {d}", .{quant_kernel_compiler.first_benchmarks.len});
    defer std.testing.allocator.free(benchmark_count);
    try std.testing.expect(std.mem.containsAtLeast(u8, benchmark_manifest.data, 1, benchmark_count));
    try std.testing.expect(std.mem.containsAtLeast(u8, benchmark_manifest.data, 1, "\"metal_promotion_warmup_repeat_runs\": 2"));
    const metal_production_regression_case_count = try std.fmt.allocPrint(std.testing.allocator, "\"metal_production_regression_expected_case_count\": {d}", .{quant_kernel_compiler.first_metal_production_regression_expected_case_count});
    defer std.testing.allocator.free(metal_production_regression_case_count);
    try std.testing.expect(std.mem.containsAtLeast(u8, benchmark_manifest.data, 1, metal_production_regression_case_count));
    const metal_production_regression_case_fingerprint = try std.fmt.allocPrint(std.testing.allocator, "\"metal_production_regression_case_fingerprint\": {d}", .{quant_kernel_compiler.metalProductionBenchmarkCaseManifestFingerprint()});
    defer std.testing.allocator.free(metal_production_regression_case_fingerprint);
    try std.testing.expect(std.mem.containsAtLeast(u8, benchmark_manifest.data, 1, metal_production_regression_case_fingerprint));
    try std.testing.expect(std.mem.containsAtLeast(u8, benchmark_manifest.data, 1, "\"metal_production_regression_cases\": ["));
    try std.testing.expect(std.mem.containsAtLeast(u8, benchmark_manifest.data, 1, "\"name\": \"q6_k_rows_8_cols_7_none\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, benchmark_manifest.data, 1, "\"production_kernel_id\": \"antfly_q6_k_small_batch_msl_v1\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, benchmark_manifest.data, 1, quant_kernel_compiler.first_metal_production_regression_build_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, benchmark_manifest.data, 1, quant_kernel_compiler.first_metal_production_regression_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, benchmark_manifest.data, 1, quant_kernel_compiler.first_lazy_benchmark.generated_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, benchmark_manifest.data, 1, quant_kernel_compiler.first_lazy_benchmark.benchmark_command));

    const conformance_manifest = sources[sources.len - 1];
    try std.testing.expectEqualStrings(quant_kernel_compiler.first_conformance_manifest_path, conformance_manifest.path);
    try std.testing.expect(std.mem.containsAtLeast(u8, conformance_manifest.data, 1, "antfly.quant_kernel_conformance.v1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, conformance_manifest.data, 1, "\"format\": \"q4_k\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, conformance_manifest.data, 1, "\"cuda_fallback_reason\": \"generated_artifact_missing\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, conformance_manifest.data, 1, "\"metal_fallback_reason\": \"generated_artifact_missing\""));
}

test "quant kernel codegen emits a self-contained microkernel source per microkernel artifact" {
    const sources = try generatedFiles(std.testing.allocator);
    defer freeGeneratedFiles(std.testing.allocator, sources);

    const matmul_count = quant_kernel_compiler.first_generated_matmul_artifacts.len;
    const micro_count = quant_kernel_compiler.first_generated_microkernel_artifacts.len;
    const attn_count = quant_kernel_compiler.first_generated_attention_artifacts.len;
    try std.testing.expect(micro_count > 0);
    try std.testing.expect(attn_count > 0);
    // Source layout: [matmul..][microkernel..][attention..][4 manifests].
    for (quant_kernel_compiler.first_generated_microkernel_artifacts, sources[matmul_count .. matmul_count + micro_count]) |artifact, source| {
        try std.testing.expectEqualStrings(artifact.source_path, source.path);
        try std.testing.expect(source.data.len != 0);
        const kernel_decl = try std.fmt.allocPrint(std.testing.allocator, "kernel void {s}(", .{artifact.kernel_id});
        defer std.testing.allocator.free(kernel_decl);
        try std.testing.expect(std.mem.containsAtLeast(u8, source.data, 1, kernel_decl));
        try std.testing.expect(std.mem.containsAtLeast(u8, source.data, 1, "#include <metal_stdlib>"));
    }
    // Attention sources follow the microkernels; each is self-contained for
    // its backend and is emitted from the typed attention renderer plan.
    for (quant_kernel_compiler.first_generated_attention_artifacts, sources[matmul_count + micro_count .. matmul_count + micro_count + attn_count]) |artifact, source| {
        try std.testing.expectEqualStrings(artifact.source_path, source.path);
        try std.testing.expect(source.data.len != 0);
        // Flash and split-K online sources export their entry point through a
        // typed `#define` consumed by a macro-declared kernel and take the
        // device decode scalars as `const unsigned*`; the decode composites
        // declare the kernel identifier directly with `const unsigned int*`.
        const kernel_decl = switch (artifact.backend) {
            .metal => try std.fmt.allocPrint(std.testing.allocator, "kernel void {s}(", .{artifact.kernel_id}),
            .cuda => if (artifact.cuda_flash_prefill_kernel != null)
                try std.fmt.allocPrint(std.testing.allocator, "#define ANTFLY_FLASH_KERNEL {s}\n", .{artifact.kernel_id})
            else if (artifact.cuda_splitk_online_decode_kernel != null)
                try std.fmt.allocPrint(std.testing.allocator, "#define ANTFLY_SPLITK_ONLINE_KERNEL {s}\n", .{artifact.kernel_id})
            else
                try std.fmt.allocPrint(std.testing.allocator, "extern \"C\" __global__ void {s}(", .{artifact.kernel_id}),
        };
        defer std.testing.allocator.free(kernel_decl);
        try std.testing.expect(std.mem.containsAtLeast(u8, source.data, 1, kernel_decl));
        switch (artifact.backend) {
            .metal => {
                try std.testing.expect(std.mem.containsAtLeast(u8, source.data, 1, "#include <metal_stdlib>"));
                // Self-contained: its own params struct + paging helper travel with it.
                try std.testing.expect(std.mem.containsAtLeast(u8, source.data, 1, "struct antfly_paged_attention_1x_params {"));
            },
            .cuda => if (artifact.cuda_flash_prefill_kernel != null or
                artifact.cuda_splitk_online_decode_kernel != null)
            {
                try std.testing.expect(std.mem.containsAtLeast(u8, source.data, 1, "#include <cuda_fp16.h>"));
                try std.testing.expect(std.mem.containsAtLeast(u8, source.data, 1, "const unsigned* decode_scalars"));
            } else {
                try std.testing.expect(std.mem.containsAtLeast(u8, source.data, 1, "#include <math.h>"));
                try std.testing.expect(std.mem.containsAtLeast(u8, source.data, 1, "const unsigned int* decode_scalars"));
            },
        }
    }
    // Manifests stay the trailing 4 files.
    try std.testing.expectEqualStrings(quant_kernel_compiler.first_spec_manifest_path, sources[sources.len - 4].path);
}

test "quant kernel codegen resolves package-root source paths" {
    const path = try existingSourcePath(
        std.testing.allocator,
        std.testing.io,
        quant_kernel_compiler.first_lazy_benchmark.generated_source_path,
    );
    defer if (path.owned) std.testing.allocator.free(path.value);
    try std.testing.expect(!path.owned);
    try std.testing.expectEqualStrings(quant_kernel_compiler.first_lazy_benchmark.generated_source_path, path.value);
}

test "quant kernel codegen can choose a writable path for missing package-root files" {
    const path = try writableSourcePath(std.testing.allocator, std.testing.io, "src/ops/cuda/generated/does_not_exist.cu");
    defer if (path.owned) std.testing.allocator.free(path.value);
    try std.testing.expect(!path.owned);
    try std.testing.expectEqualStrings("src/ops/cuda/generated/does_not_exist.cu", path.value);
}

test "quant kernel codegen reports missing check targets" {
    try std.testing.expectError(
        error.GeneratedQuantKernelSourceMissing,
        existingSourcePath(std.testing.allocator, std.testing.io, "src/ops/cuda/generated/does_not_exist.cu"),
    );
}

test "quant kernel codegen parses Metal check output path" {
    try std.testing.expectEqualStrings(
        "/tmp/antfly_q4_k_small_batch_bias_gelu_msl_v1.air",
        metalCheckOutputPath(quant_kernel_compiler.first_lazy_metal_check_command) orelse return error.MissingMetalCheckOutputPath,
    );
    try std.testing.expect(metalCheckOutputPath("xcrun --toolchain Metal metal -c source.metal") == null);
}

test "quant kernel codegen write creates parent directories" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "nested", "generated.cu" });
    defer std.testing.allocator.free(path);

    try writeSource(std.testing.allocator, std.testing.io, .{ .path = path, .data = "generated", .owned = false });
    const actual = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, std.testing.allocator, .limited(64));
    defer std.testing.allocator.free(actual);
    try std.testing.expectEqualStrings("generated", actual);
}
