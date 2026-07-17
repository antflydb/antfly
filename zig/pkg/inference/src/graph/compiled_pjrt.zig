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
const platform = @import("antfly_platform");
const compiled_artifact = @import("../compiled_artifact.zig");
const ops = @import("../ops/ops.zig");
const cache_mod = @import("cache.zig");
const compiled_backend = @import("compiled_backend.zig");
const partition_mod = @import("partition.zig");
const multi_executor_mod = @import("multi_executor.zig");
const model_runtime = @import("model_runtime.zig");
const pjrt_artifact_executor_mod = if (build_options.enable_pjrt) @import("pjrt_artifact_executor.zig") else struct {};
const pjrt_executor_mod = if (build_options.enable_pjrt) @import("pjrt_executor.zig") else struct {};

const Graph = @import("ml").graph.Graph;
const NodeId = @import("ml").graph.NodeId;
const OpCode = @import("ml").graph.OpCode;

fn supportsPjrtGraphPartition(op: @import("ml").graph.OpCode) bool {
    return partition_mod.supportsPjrt(op);
}

fn pjrtExecDebugEnabled() bool {
    return platform.env.getenv("TERMITE_PJRT_EXEC_DEBUG") != null;
}

fn logPjrtRuntimeDecline(
    phase: []const u8,
    err: anyerror,
    request: model_runtime.ForwardRequest,
) void {
    if (!pjrtExecDebugEnabled()) return;
    switch (request) {
        .prefill => |prefill| std.log.warn(
            "PJRT whole-model {s} declined prefill attention_mode={s} seq_len={d} query_seq_len={d} input_ids={d} err={s}",
            .{
                phase,
                @tagName(prefill.attention_mode),
                prefill.seq_len,
                prefill.query_seq_len,
                prefill.input_ids.len,
                @errorName(err),
            },
        ),
        .decode => |decode| std.log.warn(
            "PJRT whole-model {s} declined decode attention_mode={s} position={d} token_id={d} err={s}",
            .{
                phase,
                @tagName(decode.attention_mode),
                decode.position,
                decode.token_id,
                @errorName(err),
            },
        ),
    }
}

const PjrtWholeModelCoverage = struct {
    compute_nodes: usize = 0,
    unsupported_nodes: usize = 0,
    first_unsupported_node: ?NodeId = null,
    first_unsupported_op: ?OpCode = null,
    attention_blockers: usize = 0,
    rope_blockers: usize = 0,

    fn canOwnWholeModel(self: PjrtWholeModelCoverage) bool {
        return self.compute_nodes > 0 and self.unsupported_nodes == 0;
    }
};

fn isComputeOp(op: OpCode) bool {
    return op != .parameter and op != .constant;
}

fn opIsAttentionBlocker(op: OpCode) bool {
    return switch (op) {
        .fused_causal_self_attention,
        .fused_gqa_causal_attention,
        .fused_sdpa,
        .fused_cross_attention,
        .fused_windowed_self_attention,
        .fused_channel_self_attention,
        => true,
        else => false,
    };
}

fn opIsRopeBlocker(op: OpCode) bool {
    return switch (op) {
        .fused_rope => true,
        else => false,
    };
}

fn analyzePjrtWholeModelCoverage(graph: *const Graph) PjrtWholeModelCoverage {
    var coverage = PjrtWholeModelCoverage{};
    for (0..graph.nodeCount()) |idx| {
        const node_id: NodeId = @intCast(idx);
        const op = graph.node(node_id).op;
        if (!isComputeOp(op)) continue;
        coverage.compute_nodes += 1;
        if (partition_mod.supportsPjrt(op)) continue;
        coverage.unsupported_nodes += 1;
        if (coverage.first_unsupported_node == null) {
            coverage.first_unsupported_node = node_id;
            coverage.first_unsupported_op = op;
        }
        if (opIsAttentionBlocker(op)) coverage.attention_blockers += 1;
        if (opIsRopeBlocker(op)) coverage.rope_blockers += 1;
    }
    return coverage;
}

fn shapeIsConcretePositive(shape: @import("ml").graph.Shape) bool {
    for (0..shape.rank()) |axis| {
        if (shape.dim(@intCast(axis)) <= 0) return false;
    }
    return true;
}

fn reshapeHasCompileSafeShape(op: OpCode) bool {
    return switch (op) {
        .reshape => |attrs| shapeIsConcretePositive(attrs.new_shape),
        else => true,
    };
}

fn graphHasUnsupportedDynamicReshape(graph: *const Graph) bool {
    for (0..graph.nodeCount()) |idx| {
        const node = graph.node(@intCast(idx));
        if (!reshapeHasCompileSafeShape(node.op)) return true;
    }
    return false;
}

fn logPjrtWholeModelBlockers(
    graph: *const Graph,
    dpp: *const multi_executor_mod.DevicePartitionPlan,
) void {
    const coverage = analyzePjrtWholeModelCoverage(graph);
    if (!coverage.canOwnWholeModel()) {
        std.log.info(
            "PJRT whole-model blockers: compute_nodes={d} unsupported_nodes={d} first_unsupported_node={any} first_unsupported_op={s} attention_blockers={d} rope_blockers={d}",
            .{
                coverage.compute_nodes,
                coverage.unsupported_nodes,
                coverage.first_unsupported_node,
                if (coverage.first_unsupported_op) |op| @tagName(op) else "none",
                coverage.attention_blockers,
                coverage.rope_blockers,
            },
        );
        return;
    }
    if (!compiled_backend.planHasSingleBackendComputePartition(graph, dpp, .pjrt)) {
        std.log.info("PJRT whole-model blockers: all compute ops are supported, but the plan is not one PJRT-owned compute partition", .{});
    }
}

fn attachmentTargetAllowsPjrtPlan(
    graph: *const Graph,
    dpp: *const multi_executor_mod.DevicePartitionPlan,
    target: compiled_backend.AttachmentTarget,
) bool {
    return switch (target) {
        .partitioned => true,
        .whole_model => compiled_backend.planHasSingleBackendComputePartition(graph, dpp, .pjrt),
    };
}

fn supportsForMode(
    _: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) *const fn (op: @import("ml").graph.OpCode) bool {
    return &supportsPjrtGraphPartition;
}

pub fn attachPjrtExecutors(
    allocator: std.mem.Allocator,
    entry: *cache_mod.CacheEntry,
    graph: *const @import("ml").graph.Graph,
    dpp: *multi_executor_mod.DevicePartitionPlan,
    cb: *const ops.ComputeBackend,
    pjrt_client: *anyopaque,
    attachment_target: compiled_backend.AttachmentTarget,
) !void {
    if (!build_options.enable_pjrt) return;

    const pjrt_lib = @import("pjrt");
    const client: *pjrt_lib.pjrt.Client = @ptrCast(@alignCast(pjrt_client));

    entry.selectCompiledPartitionsBackend(.pjrt, attachment_target);

    if (entry.compiled_partitions_status == .uninitialized) {
        if (attachment_target == .whole_model and graphHasUnsupportedDynamicReshape(graph)) {
            std.log.info("PJRT whole-model attachment unavailable: graph contains dynamic reshape targets that require runtime shape lowering", .{});
            entry.compiled_partitions = null;
            entry.compiled_partitions_status = .unavailable;
            return;
        }
        if (!attachmentTargetAllowsPjrtPlan(graph, dpp, attachment_target)) {
            if (attachment_target == .whole_model) {
                std.log.info("PJRT whole-model attachment unavailable: current plan is not one PJRT-owned compute partition", .{});
                logPjrtWholeModelBlockers(graph, dpp);
            }
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
            if (!isPartitionPjrtEligible(graph, part)) continue;

            const pjrt_exec = pjrt_executor_mod.createExecutor(
                allocator,
                graph,
                &dpp.base.partitions[part_idx],
                cb,
                cb,
                client,
            ) catch |err| {
                std.log.warn("PJRT compilation failed for partition {d}: {s}", .{ part_idx, @errorName(err) });
                continue;
            };

            try compiled.append(allocator, .{
                .partition_idx = @intCast(part_idx),
                .executor = pjrt_exec.partitionExecutor().*,
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

pub fn isPartitionPjrtEligible(
    graph: *const Graph,
    part: partition_mod.Partition,
) bool {
    for (part.external_inputs) |ext_in| {
        const node = graph.node(ext_in.node_id);
        if (node.op == .constant and !pjrtSupportsGraphConstantDType(node.output_shape.dtype)) return false;
    }
    for (part.node_ids) |nid| {
        const node = graph.node(nid);
        const op = node.op;
        if (op == .constant) {
            if (!pjrtSupportsGraphConstantDType(node.output_shape.dtype)) return false;
            continue;
        }
        if (op == .parameter) continue;
        if (!partition_mod.supportsPjrt(op)) return false;
        if (!reshapeHasCompileSafeShape(op)) return false;
        if (!isNodeShapePjrtEligible(graph, node)) return false;
    }
    return part.node_ids.len > 0;
}

fn pjrtSupportsGraphConstantDType(dtype: @import("ml").graph.DType) bool {
    return switch (dtype) {
        .f32, .i32 => true,
        else => false,
    };
}

fn isNodeShapePjrtEligible(graph: *const Graph, node: *const @import("ml").graph.Node) bool {
    const ins = node.getInputs();
    return switch (node.op) {
        .fused_linear => ins.len >= 3 and pjrtLinearMatmulShapesEligible(graph, ins[0], ins[1]),
        .fused_linear_no_bias => ins.len >= 2 and pjrtLinearMatmulShapesEligible(graph, ins[0], ins[1]),
        .fused_linear_no_bias_pair => ins.len >= 3 and
            pjrtLinearMatmulShapesEligible(graph, ins[0], ins[1]) and
            pjrtLinearMatmulShapesEligible(graph, ins[0], ins[2]),
        else => true,
    };
}

fn pjrtLinearMatmulShapesEligible(graph: *const Graph, input_id: NodeId, weight_id: NodeId) bool {
    const input_shape = graph.node(input_id).output_shape;
    const weight_shape = graph.node(weight_id).output_shape;
    const input_rank = input_shape.rank();
    const weight_rank = weight_shape.rank();
    if (input_rank < 2 or weight_rank < 2) return false;
    if (input_rank > 8 or weight_rank > 8) return false;

    const input_k = input_shape.dims[input_rank - 1];
    const weight_k = weight_shape.dims[weight_rank - 1];
    return input_k == weight_k;
}

fn hasCompilablePartition(
    graph: *const Graph,
    dpp: *const multi_executor_mod.DevicePartitionPlan,
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) bool {
    if (context.attachment_target == .whole_model and graphHasUnsupportedDynamicReshape(graph)) return false;
    if (!attachmentTargetAllowsPjrtPlan(graph, dpp, context.attachment_target)) {
        if (context.attachment_target == .whole_model) logPjrtWholeModelBlockers(graph, dpp);
        return false;
    }
    for (dpp.base.partitions) |part| {
        if (part.backend == .pjrt and isPartitionPjrtEligible(graph, part)) return true;
    }
    return false;
}

fn attachExecutorsViaDefinition(
    allocator: std.mem.Allocator,
    entry: *cache_mod.CacheEntry,
    graph: *const @import("ml").graph.Graph,
    dpp: *multi_executor_mod.DevicePartitionPlan,
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) !void {
    const client = context.pjrt_client orelse return error.MissingPjrtClient;
    return attachPjrtExecutors(allocator, entry, graph, dpp, context.cb, client, context.attachment_target);
}

fn executeModelForwardViaDefinition(
    allocator: std.mem.Allocator,
    cache: *cache_mod.GraphCache,
    entry: *cache_mod.CacheEntry,
    graph: *const @import("ml").graph.Graph,
    dpp: *multi_executor_mod.DevicePartitionPlan,
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
    request: model_runtime.ForwardRequest,
) !?[]f32 {
    if (!build_options.enable_pjrt) return null;
    if (context.attachment_target != .whole_model) return null;

    entry.selectCompiledModelExecutorBackend(.pjrt, context.attachment_target);

    if (cache.getSessionCompiledModelRuntime(.pjrt, context.attachment_target)) |runtime| {
        switch (request) {
            .decode => |decode_request| {
                var output = runtime.decode(allocator, decode_request) catch |err| switch (err) {
                    error.UnsupportedDecode, error.UnsupportedShape, error.MissingPastKeyValue, error.MissingValue => {
                        logPjrtRuntimeDecline("cached runtime", err, request);
                        return null;
                    },
                    else => return err,
                };
                return try output.takeHostLogits(allocator);
            },
            else => {},
        }
    }

    if (entry.compiled_model_status == .uninitialized) {
        const client_raw = context.pjrt_client orelse return error.MissingPjrtClient;
        const pjrt_lib = @import("pjrt");
        const client: *pjrt_lib.pjrt.Client = @ptrCast(@alignCast(client_raw));

        if (context.artifact_dir != null and context.model_dir != null) artifact_blk: {
            if (std.meta.activeTag(request) != .prefill) break :artifact_blk;
            var io_check = std.Io.Threaded.init(allocator, .{});
            defer io_check.deinit();
            const shape = wholeModelArtifactShape(request) orelse break :artifact_blk;
            if (try findMatchingPjrtWholeModelPackageManifest(
                allocator,
                io_check.io(),
                context.artifact_dir.?,
                context.model_dir.?,
                shape,
            )) |package_manifest_path| {
                defer allocator.free(package_manifest_path);
                var io_impl = std.Io.Threaded.init(allocator, .{});
                defer io_impl.deinit();
                const model_executor_ctx = try pjrt_artifact_executor_mod.createModelExecutorFromPackageManifestPath(
                    allocator,
                    io_impl.io(),
                    package_manifest_path,
                    shape.seq_len,
                    shape.query_seq_len,
                    shape.attention_mode,
                    graph,
                    context.cb,
                    client,
                );
                std.log.info("PJRT whole-model artifact package attached: package={s}", .{package_manifest_path});
                entry.compiled_model_executor = .{
                    .ptr = model_executor_ctx,
                    .deinit = &pjrt_executor_mod.deinitModelExecutorPtr,
                };
                entry.compiled_model_status = .ready;
                break :artifact_blk;
            }

            const found = try findMatchingPjrtWholeModelArtifact(
                allocator,
                context.artifact_dir.?,
                context.model_dir.?,
                request,
            ) orelse break :artifact_blk;
            defer allocator.free(found.manifest_path);
            defer allocator.free(found.artifact_path);
            const decode_found = try findMatchingPjrtWholeModelDecodeArtifacts(
                allocator,
                io_check.io(),
                context.artifact_dir.?,
                found.manifest_path,
            );
            defer freePjrtArtifactMatches(allocator, decode_found);

            const decode_manifest_paths = try allocator.alloc([]const u8, decode_found.len);
            defer allocator.free(decode_manifest_paths);
            for (decode_found, 0..) |match, i| {
                decode_manifest_paths[i] = match.manifest_path;
            }

            var io_impl = std.Io.Threaded.init(allocator, .{});
            defer io_impl.deinit();
            const model_executor_ctx = try pjrt_artifact_executor_mod.createModelExecutorFromManifestPaths(
                allocator,
                io_impl.io(),
                found.manifest_path,
                decode_manifest_paths,
                graph,
                context.cb,
                client,
            );
            if (decode_found.len > 0) {
                std.log.info("PJRT whole-model artifact package attached via manifest fallback: prefill_manifest={s} decode_buckets={d} first_decode_seq_len={d} last_decode_seq_len={d}", .{
                    found.manifest_path,
                    decode_found.len,
                    decode_found[0].seq_len,
                    decode_found[decode_found.len - 1].seq_len,
                });
            } else {
                std.log.info("PJRT whole-model artifact attached via manifest fallback: manifest={s}", .{found.manifest_path});
            }
            entry.compiled_model_executor = .{
                .ptr = model_executor_ctx,
                .deinit = &pjrt_executor_mod.deinitModelExecutorPtr,
            };
            entry.compiled_model_status = .ready;
        }
    }

    if (entry.compiled_model_status == .uninitialized) {
        if (!attachmentTargetAllowsPjrtPlan(graph, dpp, .whole_model)) return null;
        const client_raw = context.pjrt_client orelse return error.MissingPjrtClient;
        const pjrt_lib = @import("pjrt");
        const client: *pjrt_lib.pjrt.Client = @ptrCast(@alignCast(client_raw));

        for (dpp.base.partitions) |*part| {
            if (part.backend != .pjrt) continue;
            if (!isPartitionPjrtEligible(graph, part.*)) continue;

            const model_executor_ctx = try pjrt_executor_mod.createModelExecutor(
                allocator,
                graph,
                part,
                context.cb,
                context.cb,
                client,
            );
            entry.compiled_model_executor = .{
                .ptr = model_executor_ctx,
                .deinit = &pjrt_executor_mod.deinitModelExecutorPtr,
            };
            entry.compiled_model_status = .ready;
            break;
        }

        if (entry.compiled_model_status == .uninitialized) {
            entry.compiled_model_status = .unavailable;
        }
    }

    if (entry.compiled_model_status != .ready) return null;
    const cached = entry.compiled_model_executor orelse return null;
    const model_executor_ctx: *pjrt_executor_mod.PjrtModelExecutor = @ptrCast(@alignCast(cached.ptr));
    const model_executor = model_executor_ctx.modelExecutor();
    const runtime = try compiled_backend.modelRuntimeForExecutor(
        allocator,
        cache,
        entry,
        .pjrt,
        context.attachment_target,
        &model_executor,
    );

    var output = switch (request) {
        .decode => |decode_request| runtime.decode(allocator, decode_request) catch |err| switch (err) {
            error.UnsupportedDecode, error.UnsupportedShape, error.MissingPastKeyValue, error.MissingValue => {
                logPjrtRuntimeDecline("runtime", err, request);
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
                error.MissingValue,
                => {
                    logPjrtRuntimeDecline("runtime", err, request);
                    return null;
                },
                else => return err,
            };
        },
    };
    return try output.takeHostLogits(allocator);
}

const PjrtArtifactMatch = struct {
    manifest_path: []u8,
    artifact_path: []u8,
    seq_len: usize,
};

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

fn findMatchingPjrtWholeModelArtifact(
    allocator: std.mem.Allocator,
    artifact_dir: []const u8,
    model_dir: []const u8,
    request: model_runtime.ForwardRequest,
) !?PjrtArtifactMatch {
    const shape = wholeModelArtifactShape(request) orelse return null;

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const role = if (std.meta.activeTag(request) == .decode) compiled_artifact.artifact_role_decode else compiled_artifact.artifact_role_prefill;
    if (try findMatchingPjrtArtifactFromPackage(allocator, io, artifact_dir, model_dir, role, shape)) |found| {
        return found;
    }
    const located = try findMatchingPjrtArtifactWithKinds(
        allocator,
        io,
        artifact_dir,
        model_dir,
        role,
        shape,
    ) orelse return null;

    return .{
        .manifest_path = located.manifest_path,
        .artifact_path = located.artifact_path,
        .seq_len = shape.seq_len,
    };
}

fn findMatchingPjrtArtifactFromPackage(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact_dir: []const u8,
    model_dir: []const u8,
    artifact_role: []const u8,
    shape: ArtifactShape,
) !?PjrtArtifactMatch {
    const preferences = pjrtArtifactPreferences();
    for (preferences) |preference| {
        const package_path = try compiled_artifact.packageManifestPath(
            allocator,
            artifact_dir,
            "xla",
            model_dir,
            preference.kind,
            preference.parameter_mode,
        );
        defer allocator.free(package_path);

        var parsed = compiled_artifact.readPackageManifest(allocator, io, package_path) catch |err| switch (err) {
            error.FileNotFound, error.InvalidCharacter, error.SyntaxError, error.UnexpectedToken, error.UnknownField => continue,
            else => return err,
        };
        defer parsed.deinit();

        const package = parsed.value;
        try compiled_artifact.validatePackageManifest(package, "xla", model_dir, preference.kind, preference.parameter_mode);

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
                .seq_len = found.seq_len,
            };
        }
    }
    return null;
}

fn findMatchingPjrtWholeModelPackageManifest(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact_dir: []const u8,
    model_dir: []const u8,
    shape: ArtifactShape,
) !?[]u8 {
    const preferences = pjrtArtifactPreferences();
    for (preferences) |preference| {
        const package_path = try compiled_artifact.packageManifestPath(
            allocator,
            artifact_dir,
            "xla",
            model_dir,
            preference.kind,
            preference.parameter_mode,
        );
        defer allocator.free(package_path);

        var parsed = compiled_artifact.readPackageManifest(allocator, io, package_path) catch |err| switch (err) {
            error.FileNotFound, error.InvalidCharacter, error.SyntaxError, error.UnexpectedToken, error.UnknownField => continue,
            else => return err,
        };
        defer parsed.deinit();

        const package = parsed.value;
        try compiled_artifact.validatePackageManifest(package, "xla", model_dir, preference.kind, preference.parameter_mode);

        const entry = try compiled_artifact.findUniqueMatchingPackageEntry(package, .{
            .artifact_role = compiled_artifact.artifact_role_prefill,
            .seq_len = shape.seq_len,
            .query_seq_len = shape.query_seq_len,
            .attention_mode = shape.attention_mode,
        });
        if (entry != null) {
            const owned_package_path = try allocator.dupe(u8, package_path);
            return owned_package_path;
        }
    }
    return null;
}

fn findMatchingPjrtWholeModelDecodeArtifacts(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact_dir: []const u8,
    prefill_manifest_path: []const u8,
) ![]PjrtArtifactMatch {
    var prefill_parsed = try compiled_artifact.readManifest(allocator, io, prefill_manifest_path);
    defer prefill_parsed.deinit();

    var matches = (try findMatchingPjrtDecodeArtifactsFromPackage(allocator, io, artifact_dir, prefill_parsed.value)) orelse
        try findMatchingPjrtDecodeArtifactsForPrefill(allocator, io, artifact_dir, prefill_parsed.value);
    errdefer freePjrtArtifactMatches(allocator, matches);

    if (matches.len == 0) return matches;
    if (matches[0].seq_len != prefill_parsed.value.seq_len + 1) {
        freePjrtArtifactMatches(allocator, matches);
        return &.{};
    }

    if (!try pjrtPackageKvShapesCompatible(allocator, io, prefill_manifest_path, matches[0].manifest_path)) {
        std.log.warn(
            "PJRT whole-model decode artifact skipped: retained KV shape ABI is incompatible with prefill_manifest={s} decode_manifest={s}",
            .{ prefill_manifest_path, matches[0].manifest_path },
        );
        freePjrtArtifactMatches(allocator, matches);
        return &.{};
    }

    var contiguous_len: usize = 1;
    while (contiguous_len < matches.len) : (contiguous_len += 1) {
        const prev = matches[contiguous_len - 1];
        const curr = matches[contiguous_len];
        if (curr.seq_len != prev.seq_len + 1) break;
        if (!try pjrtPackageKvShapesCompatible(allocator, io, prev.manifest_path, curr.manifest_path)) {
            std.log.warn(
                "PJRT whole-model decode artifact chain truncated: retained KV shape ABI is incompatible with previous_decode_manifest={s} decode_manifest={s}",
                .{ prev.manifest_path, curr.manifest_path },
            );
            break;
        }
    }

    if (contiguous_len == matches.len) return matches;

    for (matches[contiguous_len..]) |match| {
        allocator.free(match.manifest_path);
        allocator.free(match.artifact_path);
    }
    return allocator.realloc(matches, contiguous_len);
}

fn findMatchingPjrtDecodeArtifactsFromPackage(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact_dir: []const u8,
    prefill_manifest: compiled_artifact.Manifest,
) !?[]PjrtArtifactMatch {
    const package_path = compiled_artifact.packageManifestPath(
        allocator,
        artifact_dir,
        prefill_manifest.backend,
        prefill_manifest.model_dir,
        prefill_manifest.kind,
        prefill_manifest.pjrt_parameter_mode,
    ) catch |err| return err;
    defer allocator.free(package_path);

    var parsed = compiled_artifact.readPackageManifest(allocator, io, package_path) catch |err| switch (err) {
        error.FileNotFound, error.InvalidCharacter, error.SyntaxError, error.UnexpectedToken, error.UnknownField => return null,
        else => return err,
    };
    defer parsed.deinit();

    const package = parsed.value;
    try compiled_artifact.validatePackageManifest(
        package,
        prefill_manifest.backend,
        prefill_manifest.model_dir,
        prefill_manifest.kind,
        prefill_manifest.pjrt_parameter_mode,
    );

    var out = std.ArrayListUnmanaged(PjrtArtifactMatch).empty;
    errdefer freePjrtArtifactMatches(allocator, out.items);
    for (package.artifacts) |entry| {
        if (!compiled_artifact.packageEntryMatches(entry, .{
            .artifact_role = compiled_artifact.artifact_role_decode,
            .query_seq_len = 1,
            .attention_mode = "paged_decode",
        })) continue;
        if (entry.seq_len <= prefill_manifest.seq_len) continue;
        try out.append(allocator, .{
            .manifest_path = try allocator.dupe(u8, entry.manifest_path),
            .artifact_path = try allocator.dupe(u8, entry.artifact_path),
            .seq_len = entry.seq_len,
        });
    }

    std.mem.sort(PjrtArtifactMatch, out.items, {}, struct {
        fn lessThan(_: void, a: PjrtArtifactMatch, b: PjrtArtifactMatch) bool {
            return a.seq_len < b.seq_len;
        }
    }.lessThan);
    if (out.items.len > 1) {
        for (out.items[1..], out.items[0 .. out.items.len - 1]) |curr, prev| {
            if (curr.seq_len == prev.seq_len) return error.AmbiguousCompiledArtifact;
        }
    }
    const owned = try out.toOwnedSlice(allocator);
    return owned;
}

fn pjrtPackageKvShapesCompatible(
    allocator: std.mem.Allocator,
    io: std.Io,
    source_manifest_path: []const u8,
    target_manifest_path: []const u8,
) !bool {
    var source_parsed = try compiled_artifact.readManifest(allocator, io, source_manifest_path);
    defer source_parsed.deinit();
    var target_parsed = try compiled_artifact.readManifest(allocator, io, target_manifest_path);
    defer target_parsed.deinit();

    const source = source_parsed.value;
    const target = target_parsed.value;
    if (source.pjrt_output_shapes.len != source.pjrt_output_node_ids.len or
        target.pjrt_input_shapes.len != target.pjrt_input_bindings.len)
    {
        return false;
    }

    for (target.pjrt_input_bindings, 0..) |input_binding, input_index| {
        if (!pjrtBindingIsPastKv(input_binding.kind)) continue;
        const output_index = findMatchingPresentOutputIndex(source, input_binding) orelse return false;
        if (!std.mem.eql(i64, target.pjrt_input_shapes[input_index], source.pjrt_output_shapes[output_index])) {
            return false;
        }
    }
    return true;
}

fn pjrtBindingIsPastKv(kind: []const u8) bool {
    return std.mem.eql(u8, kind, compiled_artifact.pjrt_binding_past_key) or
        std.mem.eql(u8, kind, compiled_artifact.pjrt_binding_past_value);
}

fn pjrtPresentKindForPast(kind: []const u8) ?[]const u8 {
    if (std.mem.eql(u8, kind, compiled_artifact.pjrt_binding_past_key)) {
        return compiled_artifact.pjrt_binding_present_key;
    }
    if (std.mem.eql(u8, kind, compiled_artifact.pjrt_binding_past_value)) {
        return compiled_artifact.pjrt_binding_present_value;
    }
    return null;
}

fn findMatchingPresentOutputIndex(
    prefill: compiled_artifact.Manifest,
    input_binding: compiled_artifact.PjrtInputBindingMeta,
) ?usize {
    const expected_kind = pjrtPresentKindForPast(input_binding.kind) orelse return null;
    for (prefill.pjrt_output_bindings) |output_binding| {
        if (!std.mem.eql(u8, output_binding.kind, expected_kind)) continue;
        if (output_binding.layer_index != input_binding.layer_index) continue;
        return findOutputNodeIndex(prefill.pjrt_output_node_ids, output_binding.node_id);
    }
    return null;
}

fn findOutputNodeIndex(node_ids: []const u32, node_id: u32) ?usize {
    for (node_ids, 0..) |candidate, index| {
        if (candidate == node_id) return index;
    }
    return null;
}

fn findMatchingPjrtArtifactWithKinds(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact_dir: []const u8,
    model_dir: []const u8,
    artifact_role: []const u8,
    shape: ArtifactShape,
) !?PjrtArtifactMatch {
    const preferences = pjrtArtifactPreferences();
    for (preferences) |preference| {
        var located = try compiled_artifact.findUniqueMatchingArtifactPath(allocator, io, artifact_dir, .{
            .backend = "xla",
            .kind = preference.kind,
            .artifact_role = artifact_role,
            .model_dir = model_dir,
            .seq_len = shape.seq_len,
            .query_seq_len = shape.query_seq_len,
            .attention_mode = shape.attention_mode,
            .pjrt_parameter_mode = preference.parameter_mode,
        }) orelse continue;
        errdefer located.deinit(allocator);
        return .{
            .manifest_path = located.manifest_path,
            .artifact_path = located.artifact_path,
            .seq_len = shape.seq_len,
        };
    }
    return null;
}

const PjrtArtifactPreference = struct {
    kind: []const u8,
    parameter_mode: []const u8,
};

fn pjrtArtifactPreferences() []const PjrtArtifactPreference {
    return &.{
        .{ .kind = "pjrt_executable", .parameter_mode = compiled_artifact.pjrt_parameter_mode_embedded },
        .{ .kind = "pjrt_executable", .parameter_mode = compiled_artifact.pjrt_parameter_mode_inputs },
        .{ .kind = "pjrt_hlo", .parameter_mode = compiled_artifact.pjrt_parameter_mode_embedded },
        .{ .kind = "pjrt_hlo", .parameter_mode = compiled_artifact.pjrt_parameter_mode_inputs },
    };
}

fn findMatchingPjrtDecodeArtifactsForPrefill(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact_dir: []const u8,
    prefill_manifest: compiled_artifact.Manifest,
) ![]PjrtArtifactMatch {
    var dir = if (std.fs.path.isAbsolute(artifact_dir))
        try std.Io.Dir.openDirAbsolute(io, artifact_dir, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, artifact_dir, .{ .iterate = true });
    defer dir.close(io);

    var found = std.ArrayListUnmanaged(PjrtArtifactMatch).empty;
    errdefer freePjrtArtifactMatches(allocator, found.items);

    var iter = dir.iterate();
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
        if (!std.mem.eql(u8, manifest.backend, "xla") or
            !std.mem.eql(u8, manifest.kind, prefill_manifest.kind) or
            !std.mem.eql(u8, manifest.artifact_role, compiled_artifact.artifact_role_decode) or
            !std.mem.eql(u8, manifest.model_dir, prefill_manifest.model_dir) or
            !std.mem.eql(u8, manifest.pjrt_parameter_mode, prefill_manifest.pjrt_parameter_mode) or
            manifest.query_seq_len != 1 or
            !std.mem.eql(u8, manifest.attention_mode, "paged_decode") or
            manifest.seq_len <= prefill_manifest.seq_len)
        {
            allocator.free(manifest_path);
            continue;
        }

        try found.append(allocator, .{
            .manifest_path = manifest_path,
            .artifact_path = try allocator.dupe(u8, manifest.artifact_path),
            .seq_len = manifest.seq_len,
        });
    }

    std.mem.sort(PjrtArtifactMatch, found.items, {}, struct {
        fn lessThan(_: void, a: PjrtArtifactMatch, b: PjrtArtifactMatch) bool {
            return a.seq_len < b.seq_len;
        }
    }.lessThan);
    if (found.items.len > 1) {
        for (found.items[1..], found.items[0 .. found.items.len - 1]) |curr, prev| {
            if (curr.seq_len == prev.seq_len) return error.AmbiguousCompiledArtifact;
        }
    }

    return found.toOwnedSlice(allocator);
}

fn freePjrtArtifactMatches(allocator: std.mem.Allocator, matches: []const PjrtArtifactMatch) void {
    for (matches) |match| {
        allocator.free(match.manifest_path);
        allocator.free(match.artifact_path);
    }
    if (matches.len > 0) allocator.free(matches);
}

fn shouldAttach(
    context: compiled_backend.AttachContext,
    _: compiled_backend.CompileMode,
) bool {
    return context.requested_backend == .pjrt or
        (context.requested_backend == null and context.pjrt_client != null);
}

pub const backend = compiled_backend.Definition{
    .kind = .pjrt,
    .model_runtime_strategy = .inline_compiled_graph,
    .supports_for_mode = &supportsForMode,
    .should_attach = &shouldAttach,
    .has_compilable_partition = &hasCompilablePartition,
    .attach_executors = &attachExecutorsViaDefinition,
    .execute_model_forward = &executeModelForwardViaDefinition,
};

test "PJRT whole-model decode artifact lookup collects sorted decode buckets" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(base_dir);

    const prefill_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "prefill.exec.inference.json" });
    defer allocator.free(prefill_manifest_path);
    try compiled_artifact.writeManifest(allocator, io, prefill_manifest_path, .{
        .kind = "pjrt_executable",
        .artifact_role = compiled_artifact.artifact_role_prefill,
        .backend = "xla",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.prefill.exec",
        .prompt_tokens = 2,
        .seq_len = 2,
        .query_seq_len = 2,
        .attention_mode = "paged_prefill",
        .raw_prompt = true,
        .chat_template_applied = false,
        .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_inputs,
    });

    inline for (.{ 4, 3 }) |seq_len| {
        const manifest_name = try std.fmt.allocPrint(allocator, "decode-{d}.exec.inference.json", .{seq_len});
        defer allocator.free(manifest_name);
        const manifest_path = try std.fs.path.join(allocator, &.{ base_dir, manifest_name });
        defer allocator.free(manifest_path);
        const artifact_path = try std.fmt.allocPrint(allocator, "/tmp/model.decode-{d}.exec", .{seq_len});
        defer allocator.free(artifact_path);
        try compiled_artifact.writeManifest(allocator, io, manifest_path, .{
            .kind = "pjrt_executable",
            .artifact_role = compiled_artifact.artifact_role_decode,
            .backend = "xla",
            .model_dir = "/tmp/model",
            .artifact_path = artifact_path,
            .prompt_tokens = seq_len,
            .seq_len = seq_len,
            .query_seq_len = 1,
            .attention_mode = "paged_decode",
            .raw_prompt = true,
            .chat_template_applied = false,
            .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_inputs,
        });
    }

    const package_path = try compiled_artifact.packageManifestPath(
        allocator,
        base_dir,
        "xla",
        "/tmp/model",
        "pjrt_executable",
        compiled_artifact.pjrt_parameter_mode_inputs,
    );
    defer allocator.free(package_path);
    const decode4_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "decode-4.exec.inference.json" });
    defer allocator.free(decode4_manifest_path);
    const decode3_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "decode-3.exec.inference.json" });
    defer allocator.free(decode3_manifest_path);
    try compiled_artifact.writePackageManifest(allocator, io, package_path, .{
        .backend = "xla",
        .model_dir = "/tmp/model",
        .kind = "pjrt_executable",
        .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_inputs,
        .artifacts = &.{
            .{
                .manifest_path = "/tmp/unused.prefill.exec.inference.json",
                .artifact_path = "/tmp/model.prefill.exec",
                .artifact_role = compiled_artifact.artifact_role_prefill,
                .seq_len = 2,
                .query_seq_len = 2,
                .attention_mode = "paged_prefill",
            },
            .{
                .manifest_path = decode4_manifest_path,
                .artifact_path = "/tmp/model.decode-4.exec",
                .artifact_role = compiled_artifact.artifact_role_decode,
                .seq_len = 4,
                .query_seq_len = 1,
                .attention_mode = "paged_decode",
            },
            .{
                .manifest_path = decode3_manifest_path,
                .artifact_path = "/tmp/model.decode-3.exec",
                .artifact_role = compiled_artifact.artifact_role_decode,
                .seq_len = 3,
                .query_seq_len = 1,
                .attention_mode = "paged_decode",
            },
        },
    });

    const found = try findMatchingPjrtWholeModelDecodeArtifacts(allocator, io, base_dir, prefill_manifest_path);
    defer freePjrtArtifactMatches(allocator, found);
    try std.testing.expectEqual(@as(usize, 2), found.len);
    try std.testing.expectEqual(@as(usize, 3), found[0].seq_len);
    try std.testing.expectEqual(@as(usize, 4), found[1].seq_len);
    try std.testing.expectEqualStrings("/tmp/model.decode-3.exec", found[0].artifact_path);
}

test "PJRT whole-model decode artifact lookup rejects first missing decode bucket" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(base_dir);

    const prefill_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "prefill.hlo.inference.json" });
    defer allocator.free(prefill_manifest_path);
    try compiled_artifact.writeManifest(allocator, io, prefill_manifest_path, .{
        .kind = "pjrt_hlo",
        .artifact_role = compiled_artifact.artifact_role_prefill,
        .backend = "xla",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.prefill.hlo",
        .prompt_tokens = 2,
        .seq_len = 2,
        .query_seq_len = 2,
        .attention_mode = "paged_prefill",
        .raw_prompt = true,
        .chat_template_applied = false,
        .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_inputs,
    });

    const manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "decode-4.hlo.inference.json" });
    defer allocator.free(manifest_path);
    try compiled_artifact.writeManifest(allocator, io, manifest_path, .{
        .kind = "pjrt_hlo",
        .artifact_role = compiled_artifact.artifact_role_decode,
        .backend = "xla",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.decode-4.hlo",
        .prompt_tokens = 4,
        .seq_len = 4,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
        .raw_prompt = true,
        .chat_template_applied = false,
        .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_inputs,
    });

    const found = try findMatchingPjrtWholeModelDecodeArtifacts(allocator, io, base_dir, prefill_manifest_path);
    defer freePjrtArtifactMatches(allocator, found);
    try std.testing.expectEqual(@as(usize, 0), found.len);
}

test "PJRT whole-model artifact lookup prefers embedded HLO over parameter-input HLO" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(base_dir);

    const embedded_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "embedded.hlo.inference.json" });
    defer allocator.free(embedded_manifest_path);
    try compiled_artifact.writeManifest(allocator, io, embedded_manifest_path, .{
        .kind = "pjrt_hlo",
        .artifact_role = compiled_artifact.artifact_role_prefill,
        .backend = "xla",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.embedded.hlo",
        .prompt_tokens = 1,
        .seq_len = 1,
        .query_seq_len = 1,
        .attention_mode = "paged_prefill",
        .raw_prompt = true,
        .chat_template_applied = false,
        .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_embedded,
    });

    const inputs_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "inputs.hlo.inference.json" });
    defer allocator.free(inputs_manifest_path);
    try compiled_artifact.writeManifest(allocator, io, inputs_manifest_path, .{
        .kind = "pjrt_hlo",
        .artifact_role = compiled_artifact.artifact_role_prefill,
        .backend = "xla",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.inputs.hlo",
        .prompt_tokens = 1,
        .seq_len = 1,
        .query_seq_len = 1,
        .attention_mode = "paged_prefill",
        .raw_prompt = true,
        .chat_template_applied = false,
        .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_inputs,
    });

    const maybe_found = try findMatchingPjrtWholeModelArtifact(
        allocator,
        base_dir,
        "/tmp/model",
        .{ .prefill = .{
            .input_ids = &.{1},
            .seq_len = 1,
            .query_seq_len = 1,
            .attention_mode = .paged_prefill,
        } },
    );
    try std.testing.expect(maybe_found != null);
    const found = maybe_found.?;
    defer allocator.free(found.manifest_path);
    defer allocator.free(found.artifact_path);
    try std.testing.expectEqualStrings("/tmp/model.embedded.hlo", found.artifact_path);
}

test "PJRT whole-model artifact lookup can resolve prefill from package index" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(base_dir);

    const package_path = try compiled_artifact.packageManifestPath(
        allocator,
        base_dir,
        "xla",
        "/tmp/model",
        "pjrt_executable",
        compiled_artifact.pjrt_parameter_mode_inputs,
    );
    defer allocator.free(package_path);
    try compiled_artifact.writePackageManifest(allocator, io, package_path, .{
        .backend = "xla",
        .model_dir = "/tmp/model",
        .kind = "pjrt_executable",
        .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_inputs,
        .artifacts = &.{
            .{
                .manifest_path = "/tmp/model.prefill.exec.inference.json",
                .artifact_path = "/tmp/model.prefill.exec",
                .artifact_role = compiled_artifact.artifact_role_prefill,
                .seq_len = 2,
                .query_seq_len = 2,
                .attention_mode = "paged_prefill",
            },
        },
    });

    const maybe_found = try findMatchingPjrtWholeModelArtifact(
        allocator,
        base_dir,
        "/tmp/model",
        .{ .prefill = .{
            .input_ids = &.{ 1, 2 },
            .seq_len = 2,
            .query_seq_len = 2,
            .attention_mode = .paged_prefill,
        } },
    );
    try std.testing.expect(maybe_found != null);
    const found = maybe_found.?;
    defer allocator.free(found.manifest_path);
    defer allocator.free(found.artifact_path);
    try std.testing.expectEqualStrings("/tmp/model.prefill.exec", found.artifact_path);
}

test "PJRT whole-model coverage reports unsupported stateful attention blockers" {
    const allocator = std.testing.allocator;
    var graph = @import("ml").graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = @import("ml").graph.Builder.init(&graph);
    const shape = @import("ml").graph.Shape.init(.f32, &.{ 1, 8 });

    const input = try builder.parameter("x", shape);
    const rope = try graph.addNode(.{
        .op = .{ .fused_rope = .{
            .seq_len = 1,
            .head_dim = 8,
            .rope_dim = 8,
            .theta = 10000,
            .freq_scale = 1,
            .position_offset = 0,
            .consecutive_pairs = false,
        } },
        .output_shape = shape,
        .inputs = .{ input, @import("ml").graph.null_node, @import("ml").graph.null_node, @import("ml").graph.null_node },
        .num_inputs = 1,
    });
    const attn = try graph.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 1,
            .num_heads = 1,
            .num_kv_heads = 1,
            .head_dim = 8,
            .skip_kv_write = true,
        } },
        .output_shape = shape,
        .inputs = .{ rope, rope, rope, @import("ml").graph.null_node },
        .num_inputs = 3,
    });
    try graph.markOutput(attn);

    const coverage = analyzePjrtWholeModelCoverage(&graph);
    try std.testing.expect(!coverage.canOwnWholeModel());
    try std.testing.expectEqual(@as(usize, 1), coverage.unsupported_nodes);
    try std.testing.expectEqual(@as(?NodeId, attn), coverage.first_unsupported_node);
    try std.testing.expectEqual(@as(usize, 0), coverage.rope_blockers);
    try std.testing.expectEqual(@as(usize, 1), coverage.attention_blockers);
}

test "PJRT partition eligibility rejects scalar linear weights" {
    const allocator = std.testing.allocator;
    var graph = @import("ml").graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = @import("ml").graph.Builder.init(&graph);
    const ml_graph = @import("ml").graph;

    const input = try builder.parameter("x", ml_graph.Shape.init(.f32, &.{ 1, 4 }));
    const scalar_weight = try builder.parameter("opaque_weight", ml_graph.Shape.scalar(.f32));
    const bad_linear = try graph.addNode(.{
        .op = .{ .fused_linear_no_bias = .{ .rows = 1, .in_dim = 4, .out_dim = 3 } },
        .output_shape = ml_graph.Shape.init(.f32, &.{ 1, 3 }),
        .inputs = .{ input, scalar_weight, ml_graph.null_node, ml_graph.null_node },
        .num_inputs = 2,
    });
    var bad_nodes = [_]NodeId{bad_linear};
    const bad_part = partition_mod.Partition{
        .backend = .pjrt,
        .node_ids = bad_nodes[0..],
        .external_inputs = &.{},
    };
    try std.testing.expect(!isPartitionPjrtEligible(&graph, bad_part));

    const matrix_weight = try builder.parameter("weight", ml_graph.Shape.init(.f32, &.{ 3, 4 }));
    const good_linear = try graph.addNode(.{
        .op = .{ .fused_linear_no_bias = .{ .rows = 1, .in_dim = 4, .out_dim = 3 } },
        .output_shape = ml_graph.Shape.init(.f32, &.{ 1, 3 }),
        .inputs = .{ input, matrix_weight, ml_graph.null_node, ml_graph.null_node },
        .num_inputs = 2,
    });
    var good_nodes = [_]NodeId{good_linear};
    const good_part = partition_mod.Partition{
        .backend = .pjrt,
        .node_ids = good_nodes[0..],
        .external_inputs = &.{},
    };
    try std.testing.expect(isPartitionPjrtEligible(&graph, good_part));
}

test "PJRT whole-model availability rejects host-fallback plans even with artifact dirs" {
    const allocator = std.testing.allocator;
    const ml_graph = @import("ml").graph;

    var graph = ml_graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml_graph.Builder.init(&graph);

    const shape = ml_graph.Shape.init(.f32, &.{ 2, 3 });
    const cond = try builder.parameter("cond", ml_graph.Shape.init(.bool_, &.{ 2, 3 }));
    const on_true = try builder.parameter("true", shape);
    const on_false = try builder.parameter("false", shape);
    const selected = try graph.addNode(.{
        .op = .{ .where_select = {} },
        .output_shape = shape,
        .inputs = .{ cond, on_true, on_false, ml_graph.null_node },
        .num_inputs = 3,
    });
    try graph.markOutput(selected);

    const capabilities = [_]partition_mod.Capability{
        .{ .backend = .pjrt, .priority = 10, .supports = &partition_mod.supportsPjrt },
        .{ .backend = .native, .priority = 1, .supports = &partition_mod.supportsAll },
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

    try std.testing.expect(!attachmentTargetAllowsPjrtPlan(&graph, &dpp, .whole_model));
    try std.testing.expect(!hasCompilablePartition(&graph, &dpp, context, .single_device));
}

test "PJRT whole-model availability accepts single PJRT-owned plans without artifact dirs" {
    const allocator = std.testing.allocator;
    const ml_graph = @import("ml").graph;

    var graph = ml_graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml_graph.Builder.init(&graph);

    const x = try builder.parameter("x", ml_graph.Shape.init(.f32, &.{ 2, 4 }));
    const w = try builder.parameter("w", ml_graph.Shape.init(.f32, &.{ 3, 4 }));
    const y = try graph.addNode(.{
        .op = .{ .fused_linear_no_bias = .{ .rows = 2, .in_dim = 4, .out_dim = 3 } },
        .output_shape = ml_graph.Shape.init(.f32, &.{ 2, 3 }),
        .inputs = .{ x, w, ml_graph.null_node, ml_graph.null_node },
        .num_inputs = 2,
    });
    try graph.markOutput(y);

    const capabilities = [_]partition_mod.Capability{
        .{ .backend = .pjrt, .priority = 10, .supports = &partition_mod.supportsPjrt },
        .{ .backend = .native, .priority = 1, .supports = &partition_mod.supportsAll },
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
        .attachment_target = .whole_model,
    };

    try std.testing.expect(attachmentTargetAllowsPjrtPlan(&graph, &dpp, .whole_model));
    try std.testing.expect(hasCompilablePartition(&graph, &dpp, context, .single_device));
}

test "PJRT partition eligibility rejects unsupported typed constants" {
    const allocator = std.testing.allocator;
    var graph = @import("ml").graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = @import("ml").graph.Builder.init(&graph);
    const ml_graph = @import("ml").graph;

    const values = [_]i64{ 1, 2 };
    const c = try builder.tensorConstBytes(std.mem.sliceAsBytes(&values), ml_graph.Shape.init(.i64, &.{2}));
    const cast = try graph.addNode(.{
        .op = .{ .convert_dtype = .{ .target = .f32 } },
        .output_shape = ml_graph.Shape.init(.f32, &.{2}),
        .inputs = .{ c, ml_graph.null_node, ml_graph.null_node, ml_graph.null_node },
        .num_inputs = 1,
    });

    var internal_nodes = [_]NodeId{ c, cast };
    const internal_part = partition_mod.Partition{
        .backend = .pjrt,
        .node_ids = internal_nodes[0..],
        .external_inputs = &.{},
    };
    try std.testing.expect(!isPartitionPjrtEligible(&graph, internal_part));

    var external_nodes = [_]NodeId{cast};
    const external_inputs = [_]partition_mod.ExternalInput{
        .{ .node_id = c, .source_partition = 0 },
    };
    const external_part = partition_mod.Partition{
        .backend = .pjrt,
        .node_ids = external_nodes[0..],
        .external_inputs = &external_inputs,
    };
    try std.testing.expect(!isPartitionPjrtEligible(&graph, external_part));
}
