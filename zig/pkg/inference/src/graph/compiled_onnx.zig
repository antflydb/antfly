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
const ml = @import("ml");
const compiled_artifact = @import("../compiled_artifact.zig");
const ops = @import("../ops/ops.zig");
const cache_mod = @import("cache.zig");
const compiled_backend = @import("compiled_backend.zig");
const partition_mod = @import("partition.zig");
const multi_executor_mod = @import("multi_executor.zig");
const model_runtime = @import("model_runtime.zig");
const onnx_executor_mod = if (build_options.enable_onnx) @import("onnx_executor.zig") else struct {};
const onnx_artifact_executor_mod = if (build_options.enable_onnx) @import("onnx_artifact_executor.zig") else struct {};

fn supportsOnnxGraphPartition(op: ml.graph.OpCode) bool {
    return switch (op) {
        .fused_from_float32, .fused_to_float32, .fused_linear_no_bias_pair => false,
        else => partition_mod.supportsLinearNormActivation(op),
    };
}

fn supportsForMode(
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) *const fn (op: @import("ml").graph.OpCode) bool {
    if (context.attachment_target == .whole_model) return &partition_mod.supportsAll;
    return &supportsOnnxGraphPartition;
}

fn shapeIsConcretePositive(shape: @import("ml").graph.Shape) bool {
    for (0..shape.rank()) |axis| {
        if (shape.dim(@intCast(axis)) <= 0) return false;
    }
    return true;
}

fn reshapeHasCompileSafeShape(op: ml.graph.OpCode) bool {
    return switch (op) {
        .reshape => |attrs| shapeIsConcretePositive(attrs.new_shape),
        else => true,
    };
}

fn graphHasUnsupportedDynamicReshape(graph: *const ml.graph.Graph) bool {
    for (0..graph.nodeCount()) |idx| {
        const node = graph.node(@intCast(idx));
        if (!reshapeHasCompileSafeShape(node.op)) return true;
    }
    return false;
}

fn nodeHasSafeOnnxShapes(
    graph: *const ml.graph.Graph,
    nid: ml.graph.NodeId,
) bool {
    const node = graph.node(nid);
    if (!shapeIsConcretePositive(node.output_shape)) return false;
    if (!reshapeHasCompileSafeShape(node.op)) return false;

    return switch (node.op) {
        .fused_linear => blk: {
            const input_shape = graph.node(node.inputs[0]).output_shape;
            const weight_shape = graph.node(node.inputs[1]).output_shape;
            const bias_shape = graph.node(node.inputs[2]).output_shape;
            break :blk input_shape.rank() == 2 and
                weight_shape.rank() == 2 and
                bias_shape.rank() == 1 and
                shapeIsConcretePositive(input_shape) and
                shapeIsConcretePositive(weight_shape) and
                shapeIsConcretePositive(bias_shape);
        },
        .fused_linear_no_bias => blk: {
            const input_shape = graph.node(node.inputs[0]).output_shape;
            const weight_shape = graph.node(node.inputs[1]).output_shape;
            break :blk input_shape.rank() == 2 and
                weight_shape.rank() == 2 and
                shapeIsConcretePositive(input_shape) and
                shapeIsConcretePositive(weight_shape);
        },
        .fused_layer_norm => blk: {
            const input_shape = graph.node(node.inputs[0]).output_shape;
            const gamma_shape = graph.node(node.inputs[1]).output_shape;
            const beta_shape = graph.node(node.inputs[2]).output_shape;
            break :blk input_shape.rank() == 2 and
                gamma_shape.rank() == 1 and
                beta_shape.rank() == 1 and
                shapeIsConcretePositive(input_shape) and
                shapeIsConcretePositive(gamma_shape) and
                shapeIsConcretePositive(beta_shape);
        },
        .fused_rms_norm => blk: {
            const input_shape = graph.node(node.inputs[0]).output_shape;
            const weight_shape = graph.node(node.inputs[1]).output_shape;
            break :blk input_shape.rank() == 2 and
                weight_shape.rank() == 1 and
                shapeIsConcretePositive(input_shape) and
                shapeIsConcretePositive(weight_shape);
        },
        .fused_gelu,
        .fused_relu,
        .fused_silu,
        .fused_quick_gelu,
        .fused_sigmoid,
        .fused_tanh_act,
        .fused_elem_add,
        .fused_elem_multiply,
        .fused_from_float32,
        .fused_to_float32,
        .reshape,
        => node.output_shape.rank() <= 2,
        else => true,
    };
}

pub fn attachOnnxExecutors(
    allocator: std.mem.Allocator,
    entry: *cache_mod.CacheEntry,
    graph: *const @import("ml").graph.Graph,
    dpp: *multi_executor_mod.DevicePartitionPlan,
    cb: *const ops.ComputeBackend,
    cache_dir: ?[]const u8,
    attachment_target: compiled_backend.AttachmentTarget,
) !void {
    if (!build_options.enable_onnx) return;

    entry.selectCompiledPartitionsBackend(.onnx, attachment_target);

    if (entry.compiled_partitions_status == .uninitialized) {
        if (attachment_target == .whole_model and graphHasUnsupportedDynamicReshape(graph)) {
            std.log.info("ONNX whole-model attachment unavailable: graph contains dynamic reshape targets that require runtime shape subgraphs", .{});
            entry.compiled_partitions = null;
            entry.compiled_partitions_status = .unavailable;
            return;
        }
        if (!compiled_backend.attachmentTargetAllowsPlan(graph, dpp, .onnx, attachment_target)) {
            std.log.info("ONNX whole-model attachment unavailable: partition plan requires host fallback", .{});
            entry.compiled_partitions = null;
            entry.compiled_partitions_status = .unavailable;
            return;
        }

        var compiled = std.ArrayListUnmanaged(cache_mod.CompiledPartition).empty;
        errdefer {
            for (compiled.items) |*cp| cp.executor.deinitExecutor();
            compiled.deinit(allocator);
        }

        for (dpp.base.partitions, 0..) |part, part_idx| {
            if (part.backend != .onnx) continue;
            if (!isPartitionOnnxEligible(graph, part)) continue;

            const onnx_exec = onnx_executor_mod.createExecutor(
                allocator,
                graph,
                &dpp.base.partitions[part_idx],
                cb,
                cb,
                cache_dir,
            ) catch |err| {
                std.log.warn("ONNX compilation failed for partition {d}: {s}", .{ part_idx, @errorName(err) });
                continue;
            };

            try compiled.append(allocator, .{
                .partition_idx = @intCast(part_idx),
                .executor = onnx_exec.partitionExecutor().*,
            });
        }

        if (compiled.items.len > 0) {
            entry.compiled_partitions = try compiled.toOwnedSlice(allocator);
            entry.compiled_partitions_status = .ready;
        } else {
            entry.compiled_partitions = null;
            entry.compiled_partitions_status = .unavailable;
        }
    }

    if (entry.compiled_partitions_status == .ready) {
        if (entry.compiled_partitions) |cps| {
            for (cps) |*cp| {
                dpp.base.partitions[cp.partition_idx].executor = &cp.executor;
            }
            dpp.base.owns_executors = false;
        }
    }
}

pub fn isPartitionOnnxEligible(
    graph: *const @import("ml").graph.Graph,
    part: partition_mod.Partition,
) bool {
    var has_compute = false;
    for (part.external_inputs) |ext_in| {
        switch (graph.node(ext_in.node_id).op) {
            .fused_to_float32,
            .fused_linear_no_bias_pair,
            => return false,
            else => {},
        }
    }
    for (part.node_ids) |nid| {
        const op = graph.node(nid).op;
        if (op == .parameter or op == .constant) continue;
        if (!supportsOnnxGraphPartition(op)) return false;
        if (!nodeHasSafeOnnxShapes(graph, nid)) return false;
        has_compute = true;
    }
    return has_compute;
}

fn hasCompilablePartition(
    graph: *const ml.graph.Graph,
    dpp: *const multi_executor_mod.DevicePartitionPlan,
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) bool {
    if (!compiled_backend.attachmentTargetAllowsPlan(graph, dpp, .onnx, context.attachment_target)) return false;
    if (context.attachment_target == .whole_model) {
        if (graphHasUnsupportedDynamicReshape(graph)) return false;
        return context.artifact_dir != null and context.model_dir != null;
    }
    for (dpp.base.partitions) |part| {
        if (part.backend == .onnx and isPartitionOnnxEligible(graph, part)) return true;
    }
    return false;
}

test "ONNX whole-model availability rejects runtime-shaped reshape graphs" {
    const allocator = std.testing.allocator;

    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const x = try builder.parameter("input", ml.graph.Shape.init(.f32, &.{ -1, -1 }));
    const y = try builder.reshape(x, ml.graph.Shape.init(.f32, &.{ -1, 1, 1, -1 }));
    try graph.markOutput(y);

    const capabilities = [_]partition_mod.Capability{
        .{ .backend = .onnx, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    const plan = try partition_mod.partition(allocator, &graph, &capabilities);
    const assignments = try allocator.alloc(@import("device_mesh.zig").DeviceId, plan.partitions.len);
    @memset(assignments, 0);
    var dpp = multi_executor_mod.DevicePartitionPlan{
        .base = plan,
        .device_assignment = assignments,
        .allocator = allocator,
    };
    defer dpp.deinit();

    var cb: ops.ComputeBackend = undefined;
    const context = compiled_backend.AttachContext{
        .cb = &cb,
        .model_dir = "model",
        .artifact_dir = "artifacts",
        .attachment_target = .whole_model,
    };

    try std.testing.expect(graphHasUnsupportedDynamicReshape(&graph));
    try std.testing.expect(!hasCompilablePartition(&graph, &dpp, context, .single_device));
}

fn attachExecutorsViaDefinition(
    allocator: std.mem.Allocator,
    entry: *cache_mod.CacheEntry,
    graph: *const ml.graph.Graph,
    dpp: *multi_executor_mod.DevicePartitionPlan,
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) !void {
    return attachOnnxExecutors(allocator, entry, graph, dpp, context.cb, context.model_dir, context.attachment_target);
}

fn executeModelForwardViaDefinition(
    allocator: std.mem.Allocator,
    cache: *cache_mod.GraphCache,
    entry: *cache_mod.CacheEntry,
    graph: *const ml.graph.Graph,
    dpp: *multi_executor_mod.DevicePartitionPlan,
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
    request: model_runtime.ForwardRequest,
) !?[]f32 {
    if (!build_options.enable_onnx) return null;
    if (context.attachment_target != .whole_model) return null;
    if (!compiled_backend.attachmentTargetAllowsPlan(graph, dpp, .onnx, .whole_model)) return null;

    entry.selectCompiledModelExecutorBackend(.onnx, context.attachment_target);

    switch (request) {
        .decode => |decode_request| {
            if (cache.getSessionCompiledModelRuntime(.onnx, context.attachment_target)) |runtime| {
                var output = runtime.decode(allocator, decode_request) catch |err| switch (err) {
                    error.UnsupportedDecode, error.MissingPastKeyValue => return null,
                    else => return err,
                };
                return try output.takeHostLogits(allocator);
            }
        },
        else => {},
    }

    if (entry.compiled_model_status == .uninitialized) {
        const artifact_dir = context.artifact_dir orelse {
            std.log.warn("ONNX whole-model artifact unavailable: no artifact_dir configured", .{});
            entry.compiled_model_status = .unavailable;
            return null;
        };
        const model_dir = context.model_dir orelse {
            std.log.warn("ONNX whole-model artifact unavailable: no model_dir configured", .{});
            entry.compiled_model_status = .unavailable;
            return null;
        };
        const shape = wholeModelArtifactShape(request) orelse {
            entry.compiled_model_status = .unavailable;
            return null;
        };
        const package_path = try compiled_artifact.packageManifestPath(
            allocator,
            artifact_dir,
            "onnx",
            model_dir,
            "onnx_graph",
            null,
        );
        defer allocator.free(package_path);

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        const executor_ctx = onnx_artifact_executor_mod.createModelExecutorFromPackageManifestPath(
            allocator,
            io_impl.io(),
            package_path,
            shape.seq_len,
            shape.query_seq_len,
            shape.attention_mode,
            inferFallbackVocabSize(graph),
        ) catch |err| switch (err) {
            error.FileNotFound, error.InvalidCharacter, error.SyntaxError, error.UnexpectedToken, error.UnknownField, error.ArtifactShapeMismatch => blk: {
                const found = try findMatchingOnnxWholeModelArtifact(
                    allocator,
                    artifact_dir,
                    model_dir,
                    request,
                ) orelse {
                    logMissingOnnxWholeModelArtifact(artifact_dir, model_dir, request);
                    entry.compiled_model_status = .unavailable;
                    return null;
                };
                defer allocator.free(found.manifest_path);
                defer allocator.free(found.artifact_path);
                const decode_found = if (std.meta.activeTag(request) == .prefill)
                    try findMatchingOnnxWholeModelDecodeArtifact(allocator, artifact_dir, model_dir)
                else
                    null;
                defer if (decode_found) |located| {
                    allocator.free(located.manifest_path);
                    allocator.free(located.artifact_path);
                };
                if (decode_found) |located| {
                    std.log.info("ONNX whole-model artifact package attached via manifest fallback: prefill_manifest={s} decode_manifest={s}", .{ found.manifest_path, located.manifest_path });
                } else {
                    std.log.info("ONNX whole-model artifact attached via manifest fallback: manifest={s}", .{found.manifest_path});
                }
                break :blk try onnx_artifact_executor_mod.createModelExecutorFromManifestPaths(
                    allocator,
                    io_impl.io(),
                    found.manifest_path,
                    if (decode_found) |located| located.manifest_path else null,
                    inferFallbackVocabSize(graph),
                );
            },
            else => return err,
        };
        std.log.info("ONNX whole-model artifact package attached: package={s}", .{package_path});
        entry.compiled_model_executor = .{
            .ptr = executor_ctx,
            .deinit = &onnx_artifact_executor_mod.deinitModelExecutorPtr,
        };
        entry.compiled_model_status = .ready;
    }

    if (entry.compiled_model_status != .ready) return null;
    const cached = entry.compiled_model_executor orelse return null;
    const model_executor_ctx: *onnx_artifact_executor_mod.Executor = @ptrCast(@alignCast(cached.ptr));
    const model_executor = model_executor_ctx.modelExecutor();
    const runtime = try compiled_backend.modelRuntimeForExecutor(
        allocator,
        cache,
        entry,
        .onnx,
        context.attachment_target,
        &model_executor,
    );

    var output = switch (request) {
        .decode => |decode_request| runtime.decode(allocator, decode_request) catch |err| switch (err) {
            error.UnsupportedDecode, error.MissingPastKeyValue => {
                std.log.warn(
                    "ONNX whole-model decode unavailable: runtime has no usable past/present state at position={d}",
                    .{decode_request.position},
                );
                return null;
            },
            else => return err,
        },
        .prefill => |prefill_request| blk: {
            try runtime.reset();
            break :blk runtime.prefill(allocator, prefill_request) catch |err| switch (err) {
                error.ArtifactShapeMismatch,
                error.UnsupportedArtifactInputs,
                error.UnsupportedShape,
                error.UnsupportedTensorType,
                => {
                    std.log.warn(
                        "ONNX whole-model prefill artifact rejected request: err={s} seq_len={d} query_seq_len={d} attention_mode={s}",
                        .{ @errorName(err), prefill_request.seq_len, prefill_request.query_seq_len, @tagName(prefill_request.attention_mode) },
                    );
                    return null;
                },
                else => return err,
            };
        },
    };
    return try output.takeHostLogits(allocator);
}

const OnnxArtifactMatch = struct {
    manifest_path: []u8,
    artifact_path: []u8,
};

fn findMatchingOnnxWholeModelArtifact(
    allocator: std.mem.Allocator,
    artifact_dir: []const u8,
    model_dir: []const u8,
    request: model_runtime.ForwardRequest,
) !?OnnxArtifactMatch {
    const shape = wholeModelArtifactShape(request) orelse return null;

    if (try findMatchingOnnxArtifactFromPackage(
        allocator,
        artifact_dir,
        model_dir,
        if (std.meta.activeTag(request) == .decode) compiled_artifact.artifact_role_decode else compiled_artifact.artifact_role_prefill,
        shape,
    )) |found| return found;

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var located = try compiled_artifact.findUniqueMatchingArtifactPath(allocator, io, artifact_dir, .{
        .backend = "onnx",
        .kind = "onnx_graph",
        .artifact_role = if (std.meta.activeTag(request) == .decode) compiled_artifact.artifact_role_decode else compiled_artifact.artifact_role_prefill,
        .model_dir = model_dir,
        .seq_len = shape.seq_len,
        .query_seq_len = shape.query_seq_len,
        .attention_mode = shape.attention_mode,
    }) orelse return null;
    errdefer located.deinit(allocator);

    return .{
        .manifest_path = located.manifest_path,
        .artifact_path = located.artifact_path,
    };
}

fn logMissingOnnxWholeModelArtifact(
    artifact_dir: []const u8,
    model_dir: []const u8,
    request: model_runtime.ForwardRequest,
) void {
    const shape = wholeModelArtifactShape(request) orelse {
        std.log.warn(
            "ONNX whole-model artifact unavailable: unsupported request shape in artifact_dir={s} model_dir={s}",
            .{ artifact_dir, model_dir },
        );
        return;
    };
    std.log.warn(
        "ONNX whole-model artifact unavailable: no {s} artifact in {s} for model_dir={s} seq_len={d} query_seq_len={d} attention_mode={s}",
        .{
            if (std.meta.activeTag(request) == .decode) compiled_artifact.artifact_role_decode else compiled_artifact.artifact_role_prefill,
            artifact_dir,
            model_dir,
            shape.seq_len,
            shape.query_seq_len,
            shape.attention_mode,
        },
    );
}

fn findMatchingOnnxWholeModelDecodeArtifact(
    allocator: std.mem.Allocator,
    artifact_dir: []const u8,
    model_dir: []const u8,
) !?OnnxArtifactMatch {
    if (try findMatchingOnnxDecodeArtifactFromPackage(
        allocator,
        artifact_dir,
        model_dir,
    )) |found| return found;

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var dir = if (std.fs.path.isAbsolute(artifact_dir))
        try std.Io.Dir.openDirAbsolute(io, artifact_dir, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, artifact_dir, .{ .iterate = true });
    defer dir.close(io);

    var iter = dir.iterate();
    var found: ?OnnxArtifactMatch = null;
    errdefer if (found) |*located| {
        allocator.free(located.manifest_path);
        allocator.free(located.artifact_path);
    };
    while (try iter.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".inference.json")) continue;

        const manifest_path = try std.fs.path.join(allocator, &.{ artifact_dir, entry.name });
        errdefer allocator.free(manifest_path);
        var parsed = compiled_artifact.readManifest(allocator, io, manifest_path) catch |err| switch (err) {
            error.FileNotFound, error.InvalidCharacter, error.SyntaxError, error.UnexpectedToken, error.UnknownField => {
                allocator.free(manifest_path);
                continue;
            },
            else => return err,
        };
        defer parsed.deinit();

        const manifest = parsed.value;
        if (!std.mem.eql(u8, manifest.backend, "onnx") or
            !std.mem.eql(u8, manifest.kind, "onnx_graph") or
            !std.mem.eql(u8, manifest.artifact_role, compiled_artifact.artifact_role_decode) or
            !std.mem.eql(u8, manifest.model_dir, model_dir) or
            manifest.query_seq_len != 1 or
            !std.mem.eql(u8, manifest.attention_mode, "paged_decode"))
        {
            allocator.free(manifest_path);
            continue;
        }

        if (found != null) {
            return error.AmbiguousCompiledArtifact;
        }

        found = .{
            .manifest_path = manifest_path,
            .artifact_path = try allocator.dupe(u8, manifest.artifact_path),
        };
    }

    return found;
}

fn findMatchingOnnxArtifactFromPackage(
    allocator: std.mem.Allocator,
    artifact_dir: []const u8,
    model_dir: []const u8,
    artifact_role: []const u8,
    shape: ArtifactShape,
) !?OnnxArtifactMatch {
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const package_path = try compiled_artifact.packageManifestPath(
        allocator,
        artifact_dir,
        "onnx",
        model_dir,
        "onnx_graph",
        null,
    );
    defer allocator.free(package_path);

    var parsed = compiled_artifact.readPackageManifest(allocator, io, package_path) catch |err| switch (err) {
        error.FileNotFound, error.InvalidCharacter, error.SyntaxError, error.UnexpectedToken, error.UnknownField => return null,
        else => return err,
    };
    defer parsed.deinit();

    const package = parsed.value;
    try compiled_artifact.validatePackageManifest(package, "onnx", model_dir, "onnx_graph", null);

    const entry = try compiled_artifact.findUniqueMatchingPackageEntry(package, .{
        .artifact_role = artifact_role,
        .seq_len = shape.seq_len,
        .query_seq_len = shape.query_seq_len,
        .attention_mode = shape.attention_mode,
    });
    if (entry) |found| {
        return .{
            .manifest_path = try allocator.dupe(u8, found.manifest_path),
            .artifact_path = try allocator.dupe(u8, found.artifact_path),
        };
    }
    return null;
}

fn findMatchingOnnxDecodeArtifactFromPackage(
    allocator: std.mem.Allocator,
    artifact_dir: []const u8,
    model_dir: []const u8,
) !?OnnxArtifactMatch {
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const package_path = try compiled_artifact.packageManifestPath(
        allocator,
        artifact_dir,
        "onnx",
        model_dir,
        "onnx_graph",
        null,
    );
    defer allocator.free(package_path);

    var parsed = compiled_artifact.readPackageManifest(allocator, io, package_path) catch |err| switch (err) {
        error.FileNotFound, error.InvalidCharacter, error.SyntaxError, error.UnexpectedToken, error.UnknownField => return null,
        else => return err,
    };
    defer parsed.deinit();

    const package = parsed.value;
    try compiled_artifact.validatePackageManifest(package, "onnx", model_dir, "onnx_graph", null);

    const entry = try compiled_artifact.findUniqueMatchingPackageEntry(package, .{
        .artifact_role = compiled_artifact.artifact_role_decode,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
    });
    if (entry) |found| {
        return .{
            .manifest_path = try allocator.dupe(u8, found.manifest_path),
            .artifact_path = try allocator.dupe(u8, found.artifact_path),
        };
    }
    return null;
}

test "ONNX whole-model decode artifact lookup rejects ambiguous candidates" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(base_dir);

    inline for (.{ "old", "new" }, 0..) |name, idx| {
        const manifest_path = try std.fs.path.join(allocator, &.{ base_dir, name ++ ".onnx.inference.json" });
        defer allocator.free(manifest_path);
        try compiled_artifact.writeManifest(allocator, io, manifest_path, .{
            .kind = "onnx_graph",
            .artifact_role = compiled_artifact.artifact_role_decode,
            .backend = "onnx",
            .model_dir = "/tmp/model",
            .artifact_path = "/tmp/model." ++ name ++ ".onnx",
            .prompt_tokens = 2 + idx,
            .seq_len = 2 + idx,
            .query_seq_len = 1,
            .attention_mode = "paged_decode",
            .raw_prompt = true,
            .chat_template_applied = false,
        });
    }

    try std.testing.expectError(
        error.AmbiguousCompiledArtifact,
        findMatchingOnnxWholeModelDecodeArtifact(allocator, base_dir, "/tmp/model"),
    );
}

test "ONNX whole-model artifact lookup resolves package manifest entries" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(base_dir);
    const package_path = try compiled_artifact.packageManifestPath(
        allocator,
        base_dir,
        "onnx",
        "/tmp/model",
        "onnx_graph",
        null,
    );
    defer allocator.free(package_path);

    try compiled_artifact.writePackageManifest(allocator, io, package_path, .{
        .backend = "onnx",
        .model_dir = "/tmp/model",
        .kind = "onnx_graph",
        .artifacts = &.{
            .{
                .manifest_path = "/tmp/model.prefill.onnx.inference.json",
                .artifact_path = "/tmp/model.prefill.onnx",
                .artifact_role = compiled_artifact.artifact_role_prefill,
                .seq_len = 2,
                .query_seq_len = 2,
                .attention_mode = "paged_prefill",
            },
            .{
                .manifest_path = "/tmp/model.decode.onnx.inference.json",
                .artifact_path = "/tmp/model.decode.onnx",
                .artifact_role = compiled_artifact.artifact_role_decode,
                .seq_len = 2,
                .query_seq_len = 1,
                .attention_mode = "paged_decode",
            },
        },
    });

    const prefill = try findMatchingOnnxWholeModelArtifact(
        allocator,
        base_dir,
        "/tmp/model",
        .{ .prefill = .{
            .input_ids = &.{ 10, 11 },
            .seq_len = 2,
            .query_seq_len = 2,
            .attention_mode = .paged_prefill,
        } },
    );
    try std.testing.expect(prefill != null);
    defer {
        allocator.free(prefill.?.manifest_path);
        allocator.free(prefill.?.artifact_path);
    }
    try std.testing.expectEqualStrings("/tmp/model.prefill.onnx.inference.json", prefill.?.manifest_path);

    const decode = try findMatchingOnnxWholeModelDecodeArtifact(allocator, base_dir, "/tmp/model");
    try std.testing.expect(decode != null);
    defer {
        allocator.free(decode.?.manifest_path);
        allocator.free(decode.?.artifact_path);
    }
    try std.testing.expectEqualStrings("/tmp/model.decode.onnx.inference.json", decode.?.manifest_path);
}

const ArtifactShape = struct {
    seq_len: usize,
    query_seq_len: usize,
    attention_mode: []const u8,
};

fn wholeModelArtifactShape(request: model_runtime.ForwardRequest) ?ArtifactShape {
    return switch (request) {
        .prefill => |prefill| .{
            .seq_len = prefill.seq_len,
            .query_seq_len = prefill.query_seq_len,
            .attention_mode = @tagName(prefill.attention_mode),
        },
        .decode => .{
            .seq_len = 1,
            .query_seq_len = 1,
            .attention_mode = "paged_decode",
        },
    };
}

fn inferFallbackVocabSize(graph: *const ml.graph.Graph) usize {
    if (graph.outputs.items.len == 0) return 0;
    const out_node = graph.node(graph.outputs.items[graph.outputs.items.len - 1]);
    const shape = out_node.output_shape;
    if (shape.rank() == 0) return 0;
    const dim = shape.dim(@intCast(shape.rank() - 1));
    return if (dim > 0) @intCast(dim) else 0;
}

fn shouldAttach(
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) bool {
    return context.requested_backend == .onnx;
}

pub const backend = compiled_backend.Definition{
    .kind = .onnx,
    .model_runtime_strategy = .offline_artifact,
    .supports_for_mode = &supportsForMode,
    .should_attach = &shouldAttach,
    .has_compilable_partition = &hasCompilablePartition,
    .attach_executors = &attachExecutorsViaDefinition,
    .execute_model_forward = &executeModelForwardViaDefinition,
};
