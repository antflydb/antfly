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

//! PJRT partition executor.
//!
//! Implements PartitionExecutor for compiled PJRT/HLO programs. Holds a
//! PJRT LoadedExecutable, converts between host ComputeBackend CTs and
//! PJRT buffers, and runs the compiled HLO on the target device.
//!
//! The executor runs on a single PJRT device (CPU or TPU). Input CTs are
//! converted to f32, uploaded to device memory as PJRT buffers, executed,
//! and output f32 data is wrapped back into host CTs.

const std = @import("std");
const ml = @import("ml");
const pjrt_lib = @import("pjrt");
const Allocator = std.mem.Allocator;

const Graph = ml.graph.Graph;
const Node = ml.graph.Node;
const NodeId = ml.graph.NodeId;

const ops_mod = @import("../ops/ops.zig");
const contracts = @import("backend_contracts.zig");
const CT = contracts.CT;
const ComputeBackend = ops_mod.ComputeBackend;

const partition_mod = @import("partition.zig");
const PartitionExecutor = partition_mod.PartitionExecutor;
const Partition = partition_mod.Partition;
const DeviceId = @import("device_mesh.zig").DeviceId;

const compiler = @import("pjrt_compiler.zig");
const model_runtime = @import("model_runtime.zig");

fn pjrtExecDebugEnabled() bool {
    return std.c.getenv("TERMITE_PJRT_EXEC_DEBUG") != null;
}

fn shouldLogPjrtInput(index: usize, total: usize) bool {
    return index < 8 or index + 8 >= total or index % 32 == 0;
}

// ── PjrtExecutor ───────────────────────────────────────────────────

pub const PjrtExecutor = struct {
    allocator: Allocator,
    /// Compiled PJRT executable. Owned.
    executable: pjrt_lib.pjrt.LoadedExecutable,
    /// PJRT client for buffer uploads.
    client: *pjrt_lib.pjrt.Client,
    /// Host backend for CT ↔ f32 conversion.
    host_backend: *const ComputeBackend,
    /// Graph node IDs for inputs, ordered to match HLO parameters.
    input_node_ids: []const NodeId,
    /// Full input bindings, ordered to match HLO parameters.
    input_bindings: []const compiler.InputBinding,
    /// Graph node IDs for outputs, ordered to match HLO outputs.
    output_node_ids: []const NodeId,
    /// Output shapes for element count calculation.
    output_shapes: [][]i64,
    /// Input shapes for buffer uploads.
    input_shapes_: [][]i64 = &.{},
    /// Inline PartitionExecutor for stable pointer access.
    pe: PartitionExecutor = undefined,

    const vtable = PartitionExecutor.VTable{
        .execute = &executeFn,
        .deinit = &deinitFn,
    };

    pub fn partitionExecutor(self: *PjrtExecutor) *const PartitionExecutor {
        return &self.pe;
    }

    pub fn executeToBuffers(
        self: *PjrtExecutor,
        values: []?CT,
        exec_ctx: PartitionExecutor.ExecutionContext,
        allocator: Allocator,
    ) ![]pjrt_lib.pjrt.Buffer {
        return self.executeToBuffersWithRetained(values, exec_ctx, allocator, null, &.{});
    }

    pub fn executeToBuffersWithRetained(
        self: *PjrtExecutor,
        values: []?CT,
        exec_ctx: PartitionExecutor.ExecutionContext,
        allocator: Allocator,
        retained_cache: ?*const PjrtRetainedBufferCache,
        semantic_input_bindings: []const PjrtSemanticInputBinding,
    ) ![]pjrt_lib.pjrt.Buffer {
        return self.executeToBuffersWithAllocators(values, exec_ctx, allocator, allocator, retained_cache, semantic_input_bindings);
    }

    fn executeToBuffersWithAllocators(
        self: *PjrtExecutor,
        values: []?CT,
        exec_ctx: PartitionExecutor.ExecutionContext,
        allocator: Allocator,
        transfer_allocator: Allocator,
        retained_cache: ?*const PjrtRetainedBufferCache,
        semantic_input_bindings: []const PjrtSemanticInputBinding,
    ) ![]pjrt_lib.pjrt.Buffer {
        const staging_allocator = @import("antfly_platform").allocator.reclaimingScratch(transfer_allocator);
        const num_inputs = self.input_bindings.len;
        const debug = pjrtExecDebugEnabled();
        if (debug) {
            std.log.info("PJRT execute begin inputs={d} outputs={d}", .{ num_inputs, self.output_node_ids.len });
        }

        // ── 1. Convert host CTs → PJRT buffers ─────────────────

        var input_bufs = try allocator.alloc(pjrt_lib.pjrt.Buffer, num_inputs);
        defer allocator.free(input_bufs);
        var owned_input_bufs = try allocator.alloc(bool, num_inputs);
        defer allocator.free(owned_input_bufs);
        @memset(owned_input_bufs, false);
        var input_bufs_initialized: usize = 0;
        defer for (input_bufs[0..input_bufs_initialized], 0..) |*buf, i| {
            if (owned_input_bufs[i]) buf.deinit();
        };

        var semantic_inputs = try SemanticInputCursor.init(semantic_input_bindings, num_inputs);
        for (self.input_bindings, 0..) |binding, i| {
            if (semantic_inputs.at(i)) |semantic_binding| {
                const cache = retained_cache orelse {
                    if (debug) {
                        std.log.warn(
                            "PJRT execute input[{d}/{d}] missing retained cache for semantic input name={s}",
                            .{ i + 1, num_inputs, semantic_binding.name },
                        );
                    }
                    return error.MissingPastKeyValue;
                };
                const retained = cache.getForSemanticInput(semantic_binding) orelse {
                    if (debug) {
                        std.log.warn(
                            "PJRT execute input[{d}/{d}] missing retained buffer name={s} layer={?d} kind={s} retained_entries={d}",
                            .{
                                i + 1,
                                num_inputs,
                                semantic_binding.name,
                                semantic_binding.layer_index,
                                @tagName(semantic_binding.kind),
                                cache.len(),
                            },
                        );
                    }
                    return error.MissingPastKeyValue;
                };
                if (!try retained.matchesFloat32Shape(self.input_shapes_[i])) {
                    if (debug) std.log.warn("PJRT retained input shape/type mismatch input={d} expected={any}", .{ i, self.input_shapes_[i] });
                    return error.UnsupportedShape;
                }
                if (debug and shouldLogPjrtInput(i, num_inputs)) {
                    std.log.info(
                        "PJRT execute input[{d}/{d}] retained name={s}",
                        .{ i + 1, num_inputs, semantic_binding.name },
                    );
                }
                input_bufs[i] = retained.*;
            } else {
                input_bufs[i] = switch (binding) {
                    .graph_node => |nid| blk: {
                        const ct = values[@intCast(nid)] orelse {
                            if (debug) {
                                std.log.warn(
                                    "PJRT execute input[{d}/{d}] missing graph node value node={d}",
                                    .{ i + 1, num_inputs, nid },
                                );
                            }
                            return error.MissingValue;
                        };
                        const f32_data = try self.host_backend.toFloat32(ct, staging_allocator);
                        defer staging_allocator.free(f32_data);
                        const upload_shape = resolveUploadShapeFromTensor(
                            allocator,
                            self.host_backend,
                            ct,
                            self.input_shapes_[i],
                            f32_data.len,
                        ) catch |err| {
                            if (debug) {
                                std.log.warn(
                                    "PJRT execute input[{d}/{d}] cannot resolve upload shape node={d} expected_shape={any} elems={d} err={s}",
                                    .{ i + 1, num_inputs, nid, self.input_shapes_[i], f32_data.len, @errorName(err) },
                                );
                            }
                            return err;
                        };
                        defer allocator.free(upload_shape);
                        if (debug and shouldLogPjrtInput(i, num_inputs)) {
                            std.log.info(
                                "PJRT execute input[{d}/{d}] upload graph_node={d} dtype=f32 elems={d} shape={any}",
                                .{ i + 1, num_inputs, nid, f32_data.len, upload_shape },
                            );
                        }
                        break :blk self.client.bufferFromHostFloat32(f32_data, upload_shape) catch |err| {
                            if (debug) {
                                std.log.warn(
                                    "PJRT execute input[{d}/{d}] upload graph_node={d} failed shape={any} elems={d} err={s}",
                                    .{ i + 1, num_inputs, nid, upload_shape, f32_data.len, @errorName(err) },
                                );
                            }
                            return err;
                        };
                    },
                    .semantic_past_graph_node => |nid| blk: {
                        const ct = values[@intCast(nid)] orelse {
                            if (debug) {
                                std.log.warn(
                                    "PJRT execute input[{d}/{d}] missing semantic graph node value node={d}",
                                    .{ i + 1, num_inputs, nid },
                                );
                            }
                            return error.MissingValue;
                        };
                        const f32_data = try self.host_backend.toFloat32(ct, staging_allocator);
                        defer staging_allocator.free(f32_data);
                        const upload_shape = resolveUploadShapeFromTensor(
                            allocator,
                            self.host_backend,
                            ct,
                            self.input_shapes_[i],
                            f32_data.len,
                        ) catch |err| {
                            if (debug) {
                                std.log.warn(
                                    "PJRT execute input[{d}/{d}] cannot resolve semantic upload shape node={d} expected_shape={any} elems={d} err={s}",
                                    .{ i + 1, num_inputs, nid, self.input_shapes_[i], f32_data.len, @errorName(err) },
                                );
                            }
                            return err;
                        };
                        defer allocator.free(upload_shape);
                        break :blk self.client.bufferFromHostFloat32(f32_data, upload_shape) catch |err| {
                            if (debug) {
                                std.log.warn(
                                    "PJRT execute input[{d}/{d}] upload semantic graph_node={d} failed shape={any} elems={d} err={s}",
                                    .{ i + 1, num_inputs, nid, upload_shape, f32_data.len, @errorName(err) },
                                );
                            }
                            return err;
                        };
                    },
                    .embedding_ids => blk: {
                        const ids = exec_ctx.embedding_ids orelse {
                            if (debug) {
                                std.log.warn(
                                    "PJRT execute input[{d}/{d}] missing embedding ids",
                                    .{ i + 1, num_inputs },
                                );
                            }
                            return error.MissingValue;
                        };
                        const upload_shape = resolveUploadShapeFromElementCount(allocator, self.input_shapes_[i], ids.len) catch |err| {
                            if (debug) {
                                std.log.warn(
                                    "PJRT execute input[{d}/{d}] cannot resolve embedding ids upload shape expected_shape={any} elems={d} err={s}",
                                    .{ i + 1, num_inputs, self.input_shapes_[i], ids.len, @errorName(err) },
                                );
                            }
                            return err;
                        };
                        defer allocator.free(upload_shape);
                        if (ids.len != numElements(upload_shape)) {
                            if (debug) {
                                std.log.warn(
                                    "PJRT execute input[{d}/{d}] embedding ids shape mismatch elems={d} shape={any}",
                                    .{ i + 1, num_inputs, ids.len, upload_shape },
                                );
                            }
                            return error.UnsupportedShape;
                        }
                        if (debug and shouldLogPjrtInput(i, num_inputs)) {
                            std.log.info(
                                "PJRT execute input[{d}/{d}] upload embedding_ids elems={d} shape={any}",
                                .{ i + 1, num_inputs, ids.len, upload_shape },
                            );
                        }
                        break :blk self.client.bufferFromHostInt64(ids, upload_shape) catch |err| {
                            if (debug) {
                                std.log.warn(
                                    "PJRT execute input[{d}/{d}] upload embedding_ids failed shape={any} elems={d} err={s}",
                                    .{ i + 1, num_inputs, upload_shape, ids.len, @errorName(err) },
                                );
                            }
                            return err;
                        };
                    },
                };
                owned_input_bufs[i] = true;
            }
            input_bufs_initialized += 1;
        }

        // ── 2. Execute ──────────────────────────────────────────

        if (debug) std.log.info("PJRT execute calling LoadedExecutable.Execute", .{});
        const outputs = try self.executable.execute(input_bufs[0..num_inputs], allocator);
        if (debug) std.log.info("PJRT execute completed outputs={d}", .{outputs.len});
        return outputs;
    }

    fn executeFn(
        ctx: *anyopaque,
        values: []?CT,
        value_device: []DeviceId,
        _: []const NodeId,
        device_id: DeviceId,
        exec_ctx: PartitionExecutor.ExecutionContext,
    ) anyerror!void {
        const self: *PjrtExecutor = @ptrCast(@alignCast(ctx));
        var outputs = try self.executeToBuffers(values, exec_ctx, self.allocator);
        defer {
            for (outputs) |*buf| buf.deinit();
            self.allocator.free(outputs);
        }

        // ── 2. Convert outputs to host CTs ──────────────────────

        for (self.output_node_ids, 0..) |nid, i| {
            const f32_data = try outputs[i].toFloat32(self.allocator);
            defer self.allocator.free(f32_data);
            const ct = try self.host_backend.fromFloat32(f32_data);
            values[@intCast(nid)] = ct;
            value_device[@intCast(nid)] = device_id;
        }
    }

    fn deinitFn(ctx: *anyopaque) void {
        const self: *PjrtExecutor = @ptrCast(@alignCast(ctx));
        self.executable.deinit();
        for (self.output_shapes) |s| self.allocator.free(s);
        self.allocator.free(self.output_shapes);
        for (self.input_shapes_) |s| self.allocator.free(s);
        self.allocator.free(self.input_shapes_);
        self.allocator.free(self.input_bindings);
        self.allocator.free(self.input_node_ids);
        self.allocator.free(self.output_node_ids);
        self.allocator.destroy(self);
    }
};

// ── Factory ─────────────────────────────────────────────────────────

/// Compile a graph partition to HLO via PJRT and create a PartitionExecutor.
///
/// Full pipeline: compile graph → serialize HLO → PJRT compile →
/// create executor.
pub fn createExecutor(
    allocator: Allocator,
    graph: *const Graph,
    part: *const Partition,
    cb: *const ComputeBackend,
    host_backend: *const ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) !*PjrtExecutor {
    // 1. Compile partition to HLO bytes
    var result = try compiler.compilePartition(allocator, graph, part, cb);
    defer result.deinit();

    // 2. Compile HLO via PJRT
    var executable = try client.compile(result.hlo_bytes, result.output_node_ids.len);
    errdefer executable.deinit();

    return createExecutorFromLoadedExecutable(
        allocator,
        graph,
        executable,
        result.input_bindings,
        result.output_node_ids,
        result.input_shapes,
        result.output_shapes,
        host_backend,
        client,
    );
}

pub fn createExecutorFromHlo(
    allocator: Allocator,
    graph: *const Graph,
    hlo_bytes: []const u8,
    input_bindings_source: []const compiler.InputBinding,
    output_node_ids_source: []const NodeId,
    host_backend: *const ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) !*PjrtExecutor {
    if (pjrtExecDebugEnabled()) {
        std.log.info(
            "PJRT compile HLO begin bytes={d} inputs={d} outputs={d}",
            .{ hlo_bytes.len, input_bindings_source.len, output_node_ids_source.len },
        );
    }
    var executable = try client.compile(hlo_bytes, output_node_ids_source.len);
    errdefer executable.deinit();
    if (pjrtExecDebugEnabled()) std.log.info("PJRT compile HLO completed", .{});

    return createExecutorFromLoadedExecutable(
        allocator,
        graph,
        executable,
        input_bindings_source,
        output_node_ids_source,
        null,
        null,
        host_backend,
        client,
    );
}

pub fn createExecutorFromHloWithShapes(
    allocator: Allocator,
    graph: *const Graph,
    hlo_bytes: []const u8,
    input_bindings_source: []const compiler.InputBinding,
    output_node_ids_source: []const NodeId,
    input_shapes_source: []const []const i64,
    output_shapes_source: []const []const i64,
    host_backend: *const ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) !*PjrtExecutor {
    if (pjrtExecDebugEnabled()) {
        std.log.info(
            "PJRT compile HLO begin bytes={d} inputs={d} outputs={d}",
            .{ hlo_bytes.len, input_bindings_source.len, output_node_ids_source.len },
        );
    }
    var executable = try client.compile(hlo_bytes, output_node_ids_source.len);
    errdefer executable.deinit();
    if (pjrtExecDebugEnabled()) std.log.info("PJRT compile HLO completed", .{});

    return createExecutorFromLoadedExecutable(
        allocator,
        graph,
        executable,
        input_bindings_source,
        output_node_ids_source,
        input_shapes_source,
        output_shapes_source,
        host_backend,
        client,
    );
}

pub fn createExecutorFromSerializedExecutable(
    allocator: Allocator,
    graph: *const Graph,
    serialized_executable: []const u8,
    input_bindings_source: []const compiler.InputBinding,
    output_node_ids_source: []const NodeId,
    host_backend: *const ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) !*PjrtExecutor {
    var executable = try client.deserializeExecutable(serialized_executable, output_node_ids_source.len);
    errdefer executable.deinit();

    return createExecutorFromLoadedExecutable(
        allocator,
        graph,
        executable,
        input_bindings_source,
        output_node_ids_source,
        null,
        null,
        host_backend,
        client,
    );
}

pub fn createExecutorFromSerializedExecutableWithShapes(
    allocator: Allocator,
    graph: *const Graph,
    serialized_executable: []const u8,
    input_bindings_source: []const compiler.InputBinding,
    output_node_ids_source: []const NodeId,
    input_shapes_source: []const []const i64,
    output_shapes_source: []const []const i64,
    host_backend: *const ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) !*PjrtExecutor {
    var executable = try client.deserializeExecutable(serialized_executable, output_node_ids_source.len);
    errdefer executable.deinit();

    return createExecutorFromLoadedExecutable(
        allocator,
        graph,
        executable,
        input_bindings_source,
        output_node_ids_source,
        input_shapes_source,
        output_shapes_source,
        host_backend,
        client,
    );
}

fn validateInputNodes(graph: *const Graph, bindings: []const compiler.InputBinding) !void {
    for (bindings) |binding| switch (binding) {
        .graph_node => |node_id| {
            if (node_id >= graph.nodeCount()) return error.MissingArtifactMetadata;
        },
        .embedding_ids, .semantic_past_graph_node => {},
    };
}

fn createExecutorFromLoadedExecutable(
    allocator: Allocator,
    graph: *const Graph,
    executable: pjrt_lib.pjrt.LoadedExecutable,
    input_bindings_source: []const compiler.InputBinding,
    output_node_ids_source: []const NodeId,
    input_shapes_source: ?[]const []const i64,
    output_shapes_source: ?[]const []const i64,
    host_backend: *const ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) !*PjrtExecutor {
    try validateInputNodes(graph, input_bindings_source);
    const num_inputs = input_bindings_source.len;
    const num_outputs = output_node_ids_source.len;
    if (input_shapes_source) |shapes| {
        if (shapes.len != num_inputs) return error.MissingArtifactMetadata;
    }
    if (output_shapes_source) |shapes| {
        if (shapes.len != num_outputs) return error.MissingArtifactMetadata;
    }

    const input_shapes = try allocator.alloc([]i64, num_inputs);
    var input_shapes_initialized: usize = 0;
    errdefer {
        for (input_shapes[0..input_shapes_initialized]) |s| allocator.free(s);
        allocator.free(input_shapes);
    }
    for (input_bindings_source, 0..) |binding, i| {
        input_shapes[i] = if (input_shapes_source) |shapes|
            try allocator.dupe(i64, shapes[i])
        else blk: {
            const node_id = switch (binding) {
                .graph_node => |nid| nid,
                .embedding_ids => |nid| nid,
                .semantic_past_graph_node => |nid| nid,
            };
            const n = graph.node(node_id);
            break :blk try allocator.dupe(i64, n.output_shape.dims[0..n.output_shape.rank()]);
        };
        input_shapes_initialized += 1;
    }

    const output_shapes = try allocator.alloc([]i64, num_outputs);
    var output_shapes_initialized: usize = 0;
    errdefer {
        for (output_shapes[0..output_shapes_initialized]) |s| allocator.free(s);
        allocator.free(output_shapes);
    }
    for (output_node_ids_source, 0..) |nid, i| {
        output_shapes[i] = if (output_shapes_source) |shapes|
            try allocator.dupe(i64, shapes[i])
        else blk: {
            const n = graph.node(nid);
            break :blk try allocator.dupe(i64, n.output_shape.dims[0..n.output_shape.rank()]);
        };
        output_shapes_initialized += 1;
    }

    try executable.validateOutputShapes(output_shapes);

    var input_node_ids_list = std.ArrayListUnmanaged(NodeId).empty;
    errdefer input_node_ids_list.deinit(allocator);
    for (input_bindings_source) |binding| {
        switch (binding) {
            .graph_node => |nid| try input_node_ids_list.append(allocator, nid),
            .embedding_ids => {},
            .semantic_past_graph_node => |nid| try input_node_ids_list.append(allocator, nid),
        }
    }

    const input_bindings = try allocator.dupe(compiler.InputBinding, input_bindings_source);
    errdefer allocator.free(input_bindings);
    const output_node_ids = try allocator.dupe(NodeId, output_node_ids_source);
    errdefer allocator.free(output_node_ids);
    const input_node_ids = try input_node_ids_list.toOwnedSlice(allocator);
    errdefer allocator.free(input_node_ids);

    const exec = try allocator.create(PjrtExecutor);
    exec.* = .{
        .allocator = allocator,
        .executable = executable,
        .client = client,
        .host_backend = host_backend,
        .input_node_ids = input_node_ids,
        .input_bindings = input_bindings,
        .output_node_ids = output_node_ids,
        .output_shapes = output_shapes,
        .input_shapes_ = input_shapes,
    };
    exec.pe = .{ .ptr = exec, .vtable = &PjrtExecutor.vtable };
    return exec;
}

pub const PjrtInputMaterialization = enum {
    /// Current PJRT whole-model wrapper still materializes graph inputs from
    /// host state: external parameters come from the host backend and token
    /// IDs come from the ModelRuntime prefill request.
    host_assisted_graph_inputs,
    /// Mode where PJRT owns per-session cache state directly through semantic
    /// past/present buffer bindings.
    backend_owned_state,
};

pub fn runtimeCapabilitiesForInputMaterialization(input_materialization: PjrtInputMaterialization) model_runtime.RuntimeCapabilities {
    return switch (input_materialization) {
        .host_assisted_graph_inputs => .{
            .supports_decode = true,
            .state_ownership = .host_assisted_inputs,
            .interruption = .uninterruptible,
        },
        .backend_owned_state => .{
            .supports_decode = true,
            .state_ownership = .backend_owned,
            .interruption = .uninterruptible,
        },
    };
}

fn inputBindingsRequireHostMaterialization(input_bindings: []const compiler.InputBinding) bool {
    for (input_bindings) |binding| {
        switch (binding) {
            .graph_node => return true,
            .embedding_ids, .semantic_past_graph_node => {},
        }
    }
    return false;
}

pub const PjrtRetainedBufferKind = @import("pjrt_kv_identity.zig").Kind;
const KvIdentity = @import("pjrt_kv_identity.zig").Identity;

pub const PjrtSemanticOutputBinding = struct {
    output_index: usize,
    name: []const u8,
    layer_index: ?u32,
    kind: PjrtRetainedBufferKind,
    pub fn identity(self: @This()) KvIdentity {
        return .{ .layer_index = self.layer_index, .kind = self.kind, .name = self.name };
    }
};

pub const PjrtSemanticInputBinding = struct {
    input_index: usize,
    name: []const u8,
    layer_index: ?u32,
    kind: PjrtRetainedBufferKind,
    pub fn identity(self: @This()) KvIdentity {
        return .{ .layer_index = self.layer_index, .kind = self.kind, .name = self.name };
    }
};

pub const PjrtRetainedBufferCache = struct {
    allocator: Allocator,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    owns_entries: bool = true,
    fixed_capacity: bool = false,
    // Open-addressed table of entry index + 1, at less than 50% occupancy.
    // Admitted runtime slots own this storage; reset clears it without allocating.
    // Layerless bindings use their name as part of the semantic identity.
    semantic_index: []usize = &.{},

    pub const Entry = struct {
        name: []const u8,
        owns_name: bool = true,
        layer_index: ?u32,
        kind: PjrtRetainedBufferKind,
        buffer: pjrt_lib.pjrt.Buffer,
        fn identity(self: @This()) KvIdentity {
            return .{ .layer_index = self.layer_index, .kind = self.kind, .name = self.name };
        }
    };

    pub fn init(allocator: Allocator) PjrtRetainedBufferCache {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *PjrtRetainedBufferCache) void {
        self.clear();
        if (self.owns_entries) self.entries.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn clear(self: *PjrtRetainedBufferCache) void {
        const debug = pjrtExecDebugEnabled();
        if (debug) std.log.info("PJRT retained buffer clear entries={d}", .{self.entries.items.len});
        for (self.entries.items, 0..) |*entry, i| {
            if (debug) std.log.info("PJRT retained buffer deinit[{d}] name={s}", .{ i, entry.name });
            entry.buffer.deinit();
            if (debug) std.log.info("PJRT retained buffer deinit[{d}] complete", .{i});
            if (entry.owns_name) self.allocator.free(entry.name);
        }
        self.entries.clearRetainingCapacity();
        @memset(self.semantic_index, 0);
    }

    pub fn len(self: *const PjrtRetainedBufferCache) usize {
        return self.entries.items.len;
    }

    pub fn get(self: *const PjrtRetainedBufferCache, name: []const u8) ?*const pjrt_lib.pjrt.Buffer {
        for (self.entries.items) |*entry| {
            if (std.mem.eql(u8, entry.name, name)) return &entry.buffer;
        }
        return null;
    }

    pub fn getForSemanticInput(
        self: *const PjrtRetainedBufferCache,
        binding: PjrtSemanticInputBinding,
    ) ?*const pjrt_lib.pjrt.Buffer {
        const index = self.semanticEntryIndex(binding) orelse return null;
        return &self.entries.items[index].buffer;
    }

    fn semanticEntryIndex(self: *const @This(), binding: PjrtSemanticInputBinding) ?usize {
        if (self.semantic_index.len != 0) {
            const slot = self.semanticSlot(binding.name, binding.layer_index, binding.kind);
            const index = self.semantic_index[slot];
            return if (index == 0) null else index - 1;
        }
        for (self.entries.items, 0..) |entry, i| {
            if (retainedEntryMatchesSemanticInput(entry, binding)) return i;
        }
        return null;
    }

    /// Keep only inputs consumed by this phase. Pruning before native Execute
    /// bounds old residency by the phase's admitted inputs; retries retain
    /// those same inputs until successful output publication.
    pub fn retainSemanticInputs(self: *@This(), allocator: Allocator, bindings: []const PjrtSemanticInputBinding) !void {
        const live = try allocator.alloc(bool, self.entries.items.len);
        defer allocator.free(live);
        @memset(live, false);
        for (bindings) |binding| {
            const index = self.semanticEntryIndex(binding) orelse return error.MissingPastKeyValue;
            live[index] = true;
        }
        var count: usize = 0;
        for (self.entries.items, live) |entry, keep| {
            if (keep) {
                self.entries.items[count] = entry;
                count += 1;
            } else {
                var buffer = entry.buffer;
                buffer.deinit();
                if (entry.owns_name) self.allocator.free(entry.name);
            }
        }
        self.entries.items.len = count;
        @memset(self.semantic_index, 0);
        if (self.semantic_index.len != 0) {
            for (self.entries.items, 0..) |entry, i| {
                const slot = self.semanticSlot(entry.name, entry.layer_index, entry.kind);
                self.semantic_index[slot] = i + 1;
            }
        }
    }

    pub fn put(
        self: *PjrtRetainedBufferCache,
        name: []const u8,
        layer_index: ?u32,
        kind: PjrtRetainedBufferKind,
        buffer: pjrt_lib.pjrt.Buffer,
    ) !void {
        const owned_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned_name);
        try self.putName(owned_name, true, layer_index, kind, buffer);
    }

    /// The runtime retains the executor that owns these immutable names.
    pub fn putBorrowed(self: *@This(), name: []const u8, layer_index: ?u32, kind: PjrtRetainedBufferKind, buffer: pjrt_lib.pjrt.Buffer) !void {
        try self.putName(name, false, layer_index, kind, buffer);
    }

    fn semanticSlot(self: *const @This(), name: []const u8, layer: ?u32, kind: PjrtRetainedBufferKind) usize {
        const identity = KvIdentity{ .name = name, .layer_index = layer, .kind = kind };
        var slot = identity.hash() % self.semantic_index.len;
        while (self.semantic_index[slot] != 0) : (slot = (slot + 1) % self.semantic_index.len) {
            const entry = self.entries.items[self.semantic_index[slot] - 1];
            if (entry.identity().eql(identity)) break;
        }
        return slot;
    }

    fn putName(self: *@This(), name: []const u8, owns_name: bool, layer_index: ?u32, kind: PjrtRetainedBufferKind, buffer: pjrt_lib.pjrt.Buffer) !void {
        const indexed_slot: ?usize = if (!owns_name and self.semantic_index.len != 0) self.semanticSlot(name, layer_index, kind) else null;
        const candidates = if (indexed_slot) |slot| if (self.semantic_index[slot] == 0) self.entries.items[0..0] else self.entries.items[self.semantic_index[slot] - 1 .. self.semantic_index[slot]] else self.entries.items;
        for (candidates) |*entry| {
            const matches = entry.identity().eql(.{ .layer_index = layer_index, .kind = kind, .name = name });
            if (!matches) continue;
            entry.buffer.deinit();
            if (entry.owns_name) self.allocator.free(entry.name);
            entry.* = .{
                .name = name,
                .owns_name = owns_name,
                .layer_index = layer_index,
                .kind = kind,
                .buffer = buffer,
            };
            return;
        }
        if (self.fixed_capacity) {
            if (self.entries.items.len == self.entries.capacity) return error.MissingArtifactMetadata;
            if (indexed_slot) |slot| self.semantic_index[slot] = self.entries.items.len + 1;
            self.entries.appendAssumeCapacity(.{ .name = name, .owns_name = owns_name, .layer_index = layer_index, .kind = kind, .buffer = buffer });
            return;
        }
        try self.entries.append(self.allocator, .{
            .name = name,
            .owns_name = owns_name,
            .layer_index = layer_index,
            .kind = kind,
            .buffer = buffer,
        });
    }
};

fn retainedEntryMatchesSemanticInput(
    entry: PjrtRetainedBufferCache.Entry,
    binding: PjrtSemanticInputBinding,
) bool {
    return entry.identity().eql(binding.identity());
}

pub const PjrtModelExecutor = struct {
    references: std.atomic.Value(usize) = .init(1),
    owned_graph: ?*Graph = null,
    metadata_budget: ?*@import("pjrt_metadata.zig").Budget = null,
    retained_capacity: usize = 0,
    runtime_slots: []RuntimeSlot = &.{},

    artifact_owner: ?*@import("pjrt_admission.zig").Package = null,
    allocator: Allocator,
    graph: *const Graph,
    executor: *PjrtExecutor,
    host_backend: *const ComputeBackend,
    input_materialization: PjrtInputMaterialization = .host_assisted_graph_inputs,
    semantic_input_bindings: []PjrtSemanticInputBinding = &.{},
    semantic_output_bindings: []PjrtSemanticOutputBinding = &.{},
    decode_variants: []DecodeVariant = &.{},
    logits_index: ?usize = null,

    pub const DecodeVariant = struct {
        logits_index: ?usize = null,
        seq_len: usize,
        executor: *PjrtExecutor,
        input_materialization: PjrtInputMaterialization,
        semantic_input_bindings: []PjrtSemanticInputBinding,
        semantic_output_bindings: []PjrtSemanticOutputBinding,
    };

    /// Resolve immutable output mappings once, before publishing the executor.
    pub fn prepareOutputMappings(self: *@This()) void {
        self.logits_index = primaryGraphOutputIndex(self.graph, self.executor.output_node_ids, self.semantic_output_bindings);
        for (self.decode_variants) |*variant| {
            variant.logits_index = primaryGraphOutputIndex(self.graph, variant.executor.output_node_ids, variant.semantic_output_bindings);
        }
    }

    const RuntimeSlot = struct {
        busy: std.atomic.Value(bool) = .init(false),
        runtime: PjrtModelRuntime = undefined,
        entries: []PjrtRetainedBufferCache.Entry,
        semantic_index: []usize,
        values: []?CT,
        scratch: []align(8) u8,
    };

    /// Conservative arena bound for the concrete execution allocation sites.
    /// Shapes may coexist with an inferred shape; native argument/output lists
    /// and wrappers coexist until the phase returns. Every term is checked.
    fn phaseWorkspaceBytes(exec: *const PjrtExecutor) !usize {
        var bytes: usize = 0;
        const c = pjrt_lib.c_types;
        try workspaceArray(&bytes, bool, exec.output_node_ids.len); // retained flags
        try workspaceArray(&bytes, pjrt_lib.pjrt.Buffer, exec.input_bindings.len);
        try workspaceArray(&bytes, bool, exec.input_bindings.len); // owned inputs
        try workspaceArray(&bytes, ?*c.PjrtBuffer, exec.input_bindings.len);
        try workspaceArray(&bytes, i64, exec.input_bindings.len); // non-donatable indices
        try workspaceArray(&bytes, ?*c.PjrtBuffer, exec.output_node_ids.len);
        try workspaceArray(&bytes, pjrt_lib.pjrt.Buffer, exec.output_node_ids.len);
        for (exec.input_shapes_) |shape| {
            try workspaceArray(&bytes, i64, shape.len);
            try workspaceArray(&bytes, i64, shape.len);
        }
        for (exec.output_shapes) |shape| try workspaceArray(&bytes, i64, shape.len);
        return bytes;
    }

    fn workspaceArray(bytes: *usize, comptime T: type, count: usize) !void {
        bytes.* = try std.math.add(usize, bytes.*, try std.math.add(usize, try std.math.mul(usize, count, @sizeOf(T)), @alignOf(T) - 1));
    }

    /// Host metadata for every execution slot is admitted and allocated during
    /// loading. Runtime creation and KV replacement need no allocator or live
    /// manager, and cannot acquire unplanned metadata after fixing the pool.
    pub fn prepareRuntimeSlots(self: *@This(), capacity: usize) !void {
        var scratch_bytes = try phaseWorkspaceBytes(self.executor);
        for (self.decode_variants) |variant| scratch_bytes = @max(scratch_bytes, try phaseWorkspaceBytes(variant.executor));
        try workspaceArray(&scratch_bytes, bool, self.retained_capacity); // phase liveness marks
        const slots = try self.allocator.alloc(RuntimeSlot, capacity);
        errdefer self.allocator.free(slots);
        var initialized: usize = 0;
        errdefer for (slots[0..initialized]) |slot| {
            self.allocator.free(slot.entries);
            self.allocator.free(slot.semantic_index);
            self.allocator.free(slot.values);
            self.allocator.free(slot.scratch);
        };
        for (slots) |*slot| {
            const entries = try self.allocator.alloc(PjrtRetainedBufferCache.Entry, self.retained_capacity);
            errdefer self.allocator.free(entries);
            const index = try self.allocator.alloc(usize, try std.math.add(usize, try std.math.mul(usize, self.retained_capacity, 2), 1));
            @memset(index, 0);
            errdefer self.allocator.free(index);
            const values = try self.allocator.alloc(?CT, self.graph.nodeCount());
            errdefer self.allocator.free(values);
            @memset(values, null);
            const scratch = try self.allocator.alignedAlloc(u8, .@"8", scratch_bytes);
            slot.* = .{ .entries = entries, .semantic_index = index, .values = values, .scratch = scratch };
            initialized += 1;
        }
        self.runtime_slots = slots;
    }

    const executor_vtable = model_runtime.ModelExecutor.VTable{
        .create_runtime = createRuntime,
        .deinit = deinit,
    };

    pub fn modelExecutor(self: *@This()) model_runtime.ModelExecutor {
        return .{ .ptr = self, .vtable = &executor_vtable };
    }

    fn createRuntime(ctx: *anyopaque, allocator: Allocator) !model_runtime.ModelRuntime {
        const self: *PjrtModelExecutor = @ptrCast(@alignCast(ctx));
        var execution_lease = if (self.artifact_owner) |owner| try owner.acquireRuntime() else null;
        errdefer if (execution_lease) |*lease| lease.release();
        var selected: ?*RuntimeSlot = null;
        if (self.artifact_owner != null or self.metadata_budget != null) {
            for (self.runtime_slots) |*slot| {
                if (slot.busy.cmpxchgStrong(false, true, .acq_rel, .acquire) == null) {
                    selected = slot;
                    break;
                }
            }
            if (selected == null) return error.ResourceTemporarilyUnavailable;
        }
        const runtime_ctx = if (selected) |slot| &slot.runtime else try allocator.create(PjrtModelRuntime);
        runtime_ctx.* = .{
            .allocator = allocator,
            .runtime_slot = selected,
            .logits_index = self.logits_index,
            .graph = self.graph,
            .executor = self.executor,
            .host_backend = self.host_backend,
            .input_materialization = self.input_materialization,
            .retained_buffers = if (selected) |slot| .{ .allocator = self.allocator, .entries = .initBuffer(slot.entries), .owns_entries = false, .fixed_capacity = true, .semantic_index = slot.semantic_index } else PjrtRetainedBufferCache.init(allocator),
            .execution_lease = execution_lease,
            .semantic_input_bindings = self.semantic_input_bindings,
            .semantic_output_bindings = self.semantic_output_bindings,
            .decode_variants = self.decode_variants,
        };
        if (self.artifact_owner != null or self.metadata_budget != null) {
            _ = self.references.fetchAdd(1, .monotonic);
            runtime_ctx.artifact_executor_owner = self;
        }
        return .{ .ptr = runtime_ctx, .vtable = &PjrtModelRuntime.runtime_vtable };
    }

    pub fn deinit(ctx: *anyopaque) void {
        const self: *PjrtModelExecutor = @ptrCast(@alignCast(ctx));
        if ((self.artifact_owner != null or self.metadata_budget != null) and self.references.fetchSub(1, .acq_rel) != 1) return;
        const allocator = self.allocator;
        if (pjrtExecDebugEnabled()) std.log.info("PJRT ModelExecutor deinit begin", .{});
        self.executor.partitionExecutor().deinitExecutor();
        if (pjrtExecDebugEnabled()) std.log.info("PJRT ModelExecutor prefill executor deinit complete", .{});
        freeSemanticInputBindings(allocator, self.semantic_input_bindings);
        freeSemanticOutputBindings(allocator, self.semantic_output_bindings);
        freeDecodeVariants(allocator, self.decode_variants);
        if (pjrtExecDebugEnabled()) std.log.info("PJRT ModelExecutor decode variants deinit complete", .{});
        if (self.owned_graph) |graph| {
            graph.deinit();
            allocator.destroy(graph);
        }
        for (self.runtime_slots) |slot| {
            std.debug.assert(!slot.busy.load(.acquire));
            allocator.free(slot.entries);
            allocator.free(slot.semantic_index);
            allocator.free(slot.values);
            allocator.free(slot.scratch);
        }
        allocator.free(self.runtime_slots);
        if (self.artifact_owner) |owner| owner.deinit();
        const metadata = self.metadata_budget;
        allocator.destroy(self);
        if (metadata) |budget| budget.destroy();
        if (pjrtExecDebugEnabled()) std.log.info("PJRT ModelExecutor deinit complete", .{});
    }
};

pub fn deinitModelExecutorPtr(ctx: *anyopaque) void {
    PjrtModelExecutor.deinit(ctx);
}

const PjrtModelRuntime = struct {
    logits_index: ?usize = null,
    execution_lease: ?@import("pjrt_admission.zig").Package.RuntimeLease = null,
    artifact_executor_owner: ?*PjrtModelExecutor = null,
    allocator: Allocator,
    runtime_slot: ?*PjrtModelExecutor.RuntimeSlot = null,
    graph: *const Graph,
    executor: *PjrtExecutor,
    host_backend: *const ComputeBackend,
    input_materialization: PjrtInputMaterialization,
    retained_buffers: PjrtRetainedBufferCache,
    semantic_input_bindings: []const PjrtSemanticInputBinding,
    semantic_output_bindings: []const PjrtSemanticOutputBinding,
    decode_variants: []const PjrtModelExecutor.DecodeVariant,

    const runtime_vtable = model_runtime.ModelRuntime.VTable{
        .capabilities = capabilities,
        .prefill = prefill,
        .decode = decode,
        .deinit = deinit,
        .reset = reset,
    };

    fn capabilities(ctx: *anyopaque) model_runtime.RuntimeCapabilities {
        const self: *PjrtModelRuntime = @ptrCast(@alignCast(ctx));
        if (self.decode_variants.len > 0) {
            var backend_owned_decode = self.input_materialization == .backend_owned_state and
                self.semantic_output_bindings.len > 0;
            for (self.decode_variants) |variant| {
                backend_owned_decode = backend_owned_decode and
                    variant.input_materialization == .backend_owned_state and
                    variant.semantic_input_bindings.len > 0;
            }
            return .{
                .supports_decode = true,
                .state_ownership = if (backend_owned_decode) .backend_owned else .host_assisted_inputs,
                .interruption = .uninterruptible,
            };
        }
        var result = runtimeCapabilitiesForInputMaterialization(self.input_materialization);
        if (self.artifact_executor_owner != null) result.supports_decode = false;
        return result;
    }

    fn prefill(
        ctx: *anyopaque,
        allocator: Allocator,
        request: model_runtime.PrefillRequest,
    ) !model_runtime.ModelOutput {
        const self: *PjrtModelRuntime = @ptrCast(@alignCast(ctx));
        try request.check();
        // PJRT prefill starts a fresh sequence. Release prior KV before the
        // provider allocates replacement outputs, including direct API reuse.
        self.retained_buffers.clear();
        return self.executeEmbeddingIds(allocator, request.input_ids, request.query_seq_len, self.prefillPhase());
    }

    fn decode(
        ctx: *anyopaque,
        allocator: Allocator,
        request: model_runtime.DecodeRequest,
    ) !model_runtime.ModelOutput {
        const self: *PjrtModelRuntime = @ptrCast(@alignCast(ctx));
        // A managed prefill executable is not a decode ABI, even when its
        // input happens to contain one token. Never discard history silently.
        if (self.artifact_executor_owner != null and self.decode_variants.len == 0) return error.UnsupportedDecode;
        const ids = [_]i64{request.token_id};
        const seq_len = try std.math.add(usize, request.position, 1);
        const phase = if (self.decode_variants.len > 0)
            self.decodePhaseForSeqLen(seq_len) orelse return error.UnsupportedShape
        else
            self.prefillPhase();
        return self.executeEmbeddingIds(allocator, &ids, 1, phase);
    }

    const Phase = struct {
        logits_index: ?usize,
        executor: *PjrtExecutor,
        semantic_input_bindings: []const PjrtSemanticInputBinding,
        semantic_output_bindings: []const PjrtSemanticOutputBinding,
    };

    fn prefillPhase(self: *const PjrtModelRuntime) Phase {
        return .{
            .executor = self.executor,
            .logits_index = self.logits_index,
            .semantic_input_bindings = self.semantic_input_bindings,
            .semantic_output_bindings = self.semantic_output_bindings,
        };
    }

    fn decodePhaseForSeqLen(self: *const PjrtModelRuntime, seq_len: usize) ?Phase {
        var low: usize = 0;
        var high = self.decode_variants.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            const variant = self.decode_variants[middle];
            if (variant.seq_len < seq_len) {
                low = middle + 1;
            } else if (variant.seq_len > seq_len) {
                high = middle;
            } else return .{
                .executor = variant.executor,
                .logits_index = variant.logits_index,
                .semantic_input_bindings = variant.semantic_input_bindings,
                .semantic_output_bindings = variant.semantic_output_bindings,
            };
        }
        return null;
    }

    fn executeEmbeddingIds(
        self: *PjrtModelRuntime,
        allocator: Allocator,
        input_ids: []const i64,
        query_seq_len: usize,
        phase: Phase,
    ) !model_runtime.ModelOutput {
        var workspace = std.heap.FixedBufferAllocator.init(if (self.runtime_slot) |slot| slot.scratch else &.{});
        const bookkeeping = if (self.runtime_slot != null) workspace.allocator() else allocator;
        const values = if (self.runtime_slot) |slot| slot.values else try allocator.alloc(?CT, self.graph.nodeCount());
        if (self.runtime_slot == null) @memset(values, null);
        defer {
            // Only bound inputs can be populated. Clear those positions so a
            // large traced graph causes no graph-wide work on every token.
            for (phase.executor.input_bindings) |binding| {
                const node_id = switch (binding) {
                    .embedding_ids => continue,
                    .graph_node, .semantic_past_graph_node => |id| id,
                };
                if (node_id >= values.len) continue;
                if (values[node_id]) |ct| self.host_backend.free(ct);
                values[node_id] = null;
            }
            if (self.runtime_slot == null) allocator.free(values);
        }
        if (self.artifact_executor_owner != null) try self.retained_buffers.retainSemanticInputs(bookkeeping, phase.semantic_input_bindings);
        try self.materializeGraphInputs(values, phase);
        const retained = try bookkeeping.alloc(bool, phase.executor.output_node_ids.len);
        defer bookkeeping.free(retained);
        @memset(retained, false);
        const exec_ctx: PartitionExecutor.ExecutionContext = .{ .embedding_ids = input_ids };
        var outputs = try phase.executor.executeToBuffersWithAllocators(
            values,
            exec_ctx,
            bookkeeping,
            allocator,
            &self.retained_buffers,
            phase.semantic_input_bindings,
        );
        defer bookkeeping.free(outputs);
        const debug = pjrtExecDebugEnabled();
        if (debug) {
            std.log.info(
                "PJRT ModelRuntime post-execute outputs={d} semantic_outputs={d}",
                .{ outputs.len, phase.semantic_output_bindings.len },
            );
        }

        defer {
            for (outputs, 0..) |*buffer, i| {
                if (!retained[i]) {
                    if (pjrtExecDebugEnabled()) std.log.info("PJRT ModelRuntime deinit transient output[{d}]", .{i});
                    buffer.deinit();
                    if (pjrtExecDebugEnabled()) std.log.info("PJRT ModelRuntime deinit transient output[{d}] complete", .{i});
                }
            }
        }

        const logits_index = phase.logits_index orelse return error.MissingOutput;
        if (debug) {
            const size_bytes = try outputs[logits_index].onDeviceSizeInBytes();
            std.log.info(
                "PJRT ModelRuntime read logits output[{d}] shape={any} bytes={d}",
                .{ logits_index, phase.executor.output_shapes[logits_index], size_bytes },
            );
        }
        const staging_allocator = @import("antfly_platform").allocator.reclaimingScratch(allocator);
        const output_f32 = try outputs[logits_index].toFloat32WithShapeAndScratch(staging_allocator, bookkeeping, phase.executor.output_shapes[logits_index]);
        defer staging_allocator.free(output_f32);
        if (debug) std.log.info("PJRT ModelRuntime logits read floats={d}", .{output_f32.len});
        const logits = try sliceLastLogits(allocator, output_f32, phase.executor.output_shapes[logits_index], query_seq_len);
        errdefer allocator.free(logits);
        // Publish KV only after readback and host-logits allocation succeed.
        // A failed decode can retry against the previous cache generation.
        {
            errdefer self.retained_buffers.clear();
            // Managed chains consume only their immediately preceding phase.
            // Native inputs stay alive until readback succeeds, then give way
            // to precisely the new phase outputs rather than a historical union.
            if (self.artifact_executor_owner != null) self.retained_buffers.clear();
            for (phase.semantic_output_bindings) |binding| {
                if (binding.output_index >= outputs.len) return error.MissingOutput;
                if (debug) {
                    std.log.info(
                        "PJRT ModelRuntime retain output[{d}] name={s}",
                        .{ binding.output_index, binding.name },
                    );
                }
                if (self.artifact_executor_owner != null) {
                    try self.retained_buffers.putBorrowed(binding.name, binding.layer_index, binding.kind, outputs[binding.output_index]);
                } else try self.retained_buffers.put(binding.name, binding.layer_index, binding.kind, outputs[binding.output_index]);
                retained[binding.output_index] = true;
            }
        }
        if (debug) std.log.info("PJRT ModelRuntime returning logits floats={d}", .{logits.len});
        return .{ .logits = logits };
    }

    fn materializeGraphInputs(self: *PjrtModelRuntime, values: []?CT, phase: Phase) !void {
        var semantic_inputs = try SemanticInputCursor.init(phase.semantic_input_bindings, phase.executor.input_bindings.len);
        for (phase.executor.input_bindings, 0..) |binding, input_index| {
            if (semantic_inputs.at(input_index) != null) continue;
            switch (binding) {
                .graph_node => |node_id| {
                    if (node_id >= values.len) return error.MissingArtifactMetadata;
                    if (values[@intCast(node_id)] != null) continue;
                    const node = self.graph.node(node_id);
                    if (node.op != .parameter) return error.MissingValue;
                    const name = self.graph.parameterName(node);
                    const ct = try self.materializeGraphWeight(name);
                    errdefer self.host_backend.free(ct);
                    values[@intCast(node_id)] = ct;
                },
                .embedding_ids => {},
                .semantic_past_graph_node => |node_id| {
                    if (node_id >= values.len) return error.MissingArtifactMetadata;
                    if (values[@intCast(node_id)] != null) continue;
                },
            }
        }
    }

    fn materializeGraphWeight(self: *PjrtModelRuntime, name: []const u8) !CT {
        return self.host_backend.getWeight(name) catch |err| {
            if (std.mem.eql(u8, name, "lm_head.weight")) {
                return self.host_backend.getWeight("wte.weight") catch err;
            }
            return err;
        };
    }

    fn reset(ctx: *anyopaque) !void {
        const self: *PjrtModelRuntime = @ptrCast(@alignCast(ctx));
        self.retained_buffers.clear();
    }

    pub fn deinit(ctx: *anyopaque) void {
        const self: *PjrtModelRuntime = @ptrCast(@alignCast(ctx));
        const allocator = self.allocator;
        self.retained_buffers.deinit();
        var lease = self.execution_lease;
        const owner = self.artifact_executor_owner;
        if (self.runtime_slot) |slot| {
            // All old state is copied before publishing this slot for reuse.
            slot.busy.store(false, .release);
        } else allocator.destroy(self);
        if (lease) |*held| held.release();
        if (owner) |executor| PjrtModelExecutor.deinit(executor);
    }
};

pub fn createModelExecutor(
    allocator: Allocator,
    graph: *const Graph,
    part: *const Partition,
    cb: *const ComputeBackend,
    host_backend: *const ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) !*PjrtModelExecutor {
    const partition_exec = try createExecutor(allocator, graph, part, cb, host_backend, client);
    errdefer partition_exec.partitionExecutor().deinitExecutor();

    const exec = try allocator.create(PjrtModelExecutor);
    exec.* = .{
        .allocator = allocator,
        .graph = graph,
        .executor = partition_exec,
        .host_backend = host_backend,
        .semantic_input_bindings = &.{},
        .semantic_output_bindings = &.{},
    };
    exec.prepareOutputMappings();
    return exec;
}

pub fn createModelExecutorFromHlo(
    allocator: Allocator,
    graph: *const Graph,
    hlo_bytes: []const u8,
    input_bindings: []const compiler.InputBinding,
    output_node_ids: []const NodeId,
    semantic_input_bindings: []const PjrtSemanticInputBinding,
    semantic_output_bindings: []const PjrtSemanticOutputBinding,
    host_backend: *const ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) !*PjrtModelExecutor {
    const partition_exec = try createExecutorFromHlo(
        allocator,
        graph,
        hlo_bytes,
        input_bindings,
        output_node_ids,
        host_backend,
        client,
    );
    errdefer partition_exec.partitionExecutor().deinitExecutor();

    const owned_semantic_input_bindings = try cloneSemanticInputBindings(allocator, semantic_input_bindings);
    errdefer freeSemanticInputBindings(allocator, owned_semantic_input_bindings);
    const owned_semantic_output_bindings = try cloneSemanticOutputBindings(allocator, semantic_output_bindings);
    errdefer freeSemanticOutputBindings(allocator, owned_semantic_output_bindings);

    const exec = try allocator.create(PjrtModelExecutor);
    exec.* = .{
        .allocator = allocator,
        .graph = graph,
        .executor = partition_exec,
        .host_backend = host_backend,
        .input_materialization = detectInputMaterialization(input_bindings, semantic_input_bindings, semantic_output_bindings),
        .semantic_input_bindings = owned_semantic_input_bindings,
        .semantic_output_bindings = owned_semantic_output_bindings,
    };
    exec.prepareOutputMappings();
    return exec;
}

pub const DecodePackageSpec = struct {
    seq_len: usize,
    artifact_bytes: []const u8,
    input_bindings: []const compiler.InputBinding,
    output_node_ids: []const NodeId,
    input_shapes: []const []const i64,
    output_shapes: []const []const i64,
    semantic_input_bindings: []const PjrtSemanticInputBinding,
    semantic_output_bindings: []const PjrtSemanticOutputBinding,
};

pub fn createModelExecutorFromHloPackage(
    allocator: Allocator,
    graph: *const Graph,
    prefill_hlo_bytes: []const u8,
    prefill_input_bindings: []const compiler.InputBinding,
    prefill_output_node_ids: []const NodeId,
    prefill_input_shapes: []const []const i64,
    prefill_output_shapes: []const []const i64,
    prefill_semantic_input_bindings: []const PjrtSemanticInputBinding,
    prefill_semantic_output_bindings: []const PjrtSemanticOutputBinding,
    decode_hlo_bytes: ?[]const u8,
    decode_input_bindings: []const compiler.InputBinding,
    decode_output_node_ids: []const NodeId,
    decode_input_shapes: []const []const i64,
    decode_output_shapes: []const []const i64,
    decode_semantic_input_bindings: []const PjrtSemanticInputBinding,
    decode_semantic_output_bindings: []const PjrtSemanticOutputBinding,
    host_backend: *const ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) !*PjrtModelExecutor {
    const decode_packages: [1]DecodePackageSpec = if (decode_hlo_bytes) |bytes|
        .{.{
            .seq_len = decodeSeqLenFromShapes(decode_input_shapes, decode_semantic_input_bindings) orelse 0,
            .artifact_bytes = bytes,
            .input_bindings = decode_input_bindings,
            .output_node_ids = decode_output_node_ids,
            .input_shapes = decode_input_shapes,
            .output_shapes = decode_output_shapes,
            .semantic_input_bindings = decode_semantic_input_bindings,
            .semantic_output_bindings = decode_semantic_output_bindings,
        }}
    else
        undefined;
    return createModelExecutorFromHloPackages(
        allocator,
        graph,
        prefill_hlo_bytes,
        prefill_input_bindings,
        prefill_output_node_ids,
        prefill_input_shapes,
        prefill_output_shapes,
        prefill_semantic_input_bindings,
        prefill_semantic_output_bindings,
        if (decode_hlo_bytes != null) decode_packages[0..1] else &.{},
        host_backend,
        client,
    );
}

pub fn createModelExecutorFromHloPackages(
    allocator: Allocator,
    graph: *const Graph,
    prefill_hlo_bytes: []const u8,
    prefill_input_bindings: []const compiler.InputBinding,
    prefill_output_node_ids: []const NodeId,
    prefill_input_shapes: []const []const i64,
    prefill_output_shapes: []const []const i64,
    prefill_semantic_input_bindings: []const PjrtSemanticInputBinding,
    prefill_semantic_output_bindings: []const PjrtSemanticOutputBinding,
    decode_packages: []const DecodePackageSpec,
    host_backend: *const ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) !*PjrtModelExecutor {
    const prefill_exec = try createExecutorFromHloWithShapes(
        allocator,
        graph,
        prefill_hlo_bytes,
        prefill_input_bindings,
        prefill_output_node_ids,
        prefill_input_shapes,
        prefill_output_shapes,
        host_backend,
        client,
    );
    errdefer prefill_exec.partitionExecutor().deinitExecutor();

    const owned_prefill_semantic_input_bindings = try cloneSemanticInputBindings(allocator, prefill_semantic_input_bindings);
    errdefer freeSemanticInputBindings(allocator, owned_prefill_semantic_input_bindings);
    const owned_prefill_semantic_output_bindings = try cloneSemanticOutputBindings(allocator, prefill_semantic_output_bindings);
    errdefer freeSemanticOutputBindings(allocator, owned_prefill_semantic_output_bindings);
    const decode_variants = try createDecodeVariantsFromHloPackages(
        allocator,
        graph,
        decode_packages,
        host_backend,
        client,
    );
    errdefer freeDecodeVariants(allocator, decode_variants);

    const exec = try allocator.create(PjrtModelExecutor);
    exec.* = .{
        .allocator = allocator,
        .graph = graph,
        .executor = prefill_exec,
        .host_backend = host_backend,
        .input_materialization = detectInputMaterialization(
            prefill_input_bindings,
            prefill_semantic_input_bindings,
            prefill_semantic_output_bindings,
        ),
        .semantic_input_bindings = owned_prefill_semantic_input_bindings,
        .semantic_output_bindings = owned_prefill_semantic_output_bindings,
        .decode_variants = decode_variants,
    };
    exec.prepareOutputMappings();
    return exec;
}

pub fn createModelExecutorFromExecutablePackage(
    allocator: Allocator,
    graph: *const Graph,
    prefill_executable_bytes: []const u8,
    prefill_input_bindings: []const compiler.InputBinding,
    prefill_output_node_ids: []const NodeId,
    prefill_input_shapes: []const []const i64,
    prefill_output_shapes: []const []const i64,
    prefill_semantic_input_bindings: []const PjrtSemanticInputBinding,
    prefill_semantic_output_bindings: []const PjrtSemanticOutputBinding,
    decode_executable_bytes: ?[]const u8,
    decode_input_bindings: []const compiler.InputBinding,
    decode_output_node_ids: []const NodeId,
    decode_input_shapes: []const []const i64,
    decode_output_shapes: []const []const i64,
    decode_semantic_input_bindings: []const PjrtSemanticInputBinding,
    decode_semantic_output_bindings: []const PjrtSemanticOutputBinding,
    host_backend: *const ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) !*PjrtModelExecutor {
    const decode_packages: [1]DecodePackageSpec = if (decode_executable_bytes) |bytes|
        .{.{
            .seq_len = decodeSeqLenFromShapes(decode_input_shapes, decode_semantic_input_bindings) orelse 0,
            .artifact_bytes = bytes,
            .input_bindings = decode_input_bindings,
            .output_node_ids = decode_output_node_ids,
            .input_shapes = decode_input_shapes,
            .output_shapes = decode_output_shapes,
            .semantic_input_bindings = decode_semantic_input_bindings,
            .semantic_output_bindings = decode_semantic_output_bindings,
        }}
    else
        undefined;
    return createModelExecutorFromExecutablePackages(
        allocator,
        graph,
        prefill_executable_bytes,
        prefill_input_bindings,
        prefill_output_node_ids,
        prefill_input_shapes,
        prefill_output_shapes,
        prefill_semantic_input_bindings,
        prefill_semantic_output_bindings,
        if (decode_executable_bytes != null) decode_packages[0..1] else &.{},
        host_backend,
        client,
    );
}

pub fn createModelExecutorFromExecutablePackages(
    allocator: Allocator,
    graph: *const Graph,
    prefill_executable_bytes: []const u8,
    prefill_input_bindings: []const compiler.InputBinding,
    prefill_output_node_ids: []const NodeId,
    prefill_input_shapes: []const []const i64,
    prefill_output_shapes: []const []const i64,
    prefill_semantic_input_bindings: []const PjrtSemanticInputBinding,
    prefill_semantic_output_bindings: []const PjrtSemanticOutputBinding,
    decode_packages: []const DecodePackageSpec,
    host_backend: *const ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) !*PjrtModelExecutor {
    const prefill_exec = try createExecutorFromSerializedExecutableWithShapes(
        allocator,
        graph,
        prefill_executable_bytes,
        prefill_input_bindings,
        prefill_output_node_ids,
        prefill_input_shapes,
        prefill_output_shapes,
        host_backend,
        client,
    );
    errdefer prefill_exec.partitionExecutor().deinitExecutor();

    const owned_prefill_semantic_input_bindings = try cloneSemanticInputBindings(allocator, prefill_semantic_input_bindings);
    errdefer freeSemanticInputBindings(allocator, owned_prefill_semantic_input_bindings);
    const owned_prefill_semantic_output_bindings = try cloneSemanticOutputBindings(allocator, prefill_semantic_output_bindings);
    errdefer freeSemanticOutputBindings(allocator, owned_prefill_semantic_output_bindings);
    const decode_variants = try createDecodeVariantsFromExecutablePackages(
        allocator,
        graph,
        decode_packages,
        host_backend,
        client,
    );
    errdefer freeDecodeVariants(allocator, decode_variants);

    const exec = try allocator.create(PjrtModelExecutor);
    exec.* = .{
        .allocator = allocator,
        .graph = graph,
        .executor = prefill_exec,
        .host_backend = host_backend,
        .input_materialization = detectInputMaterialization(
            prefill_input_bindings,
            prefill_semantic_input_bindings,
            prefill_semantic_output_bindings,
        ),
        .semantic_input_bindings = owned_prefill_semantic_input_bindings,
        .semantic_output_bindings = owned_prefill_semantic_output_bindings,
        .decode_variants = decode_variants,
    };
    exec.prepareOutputMappings();
    return exec;
}

fn createDecodeVariantsFromHloPackages(
    allocator: Allocator,
    graph: *const Graph,
    decode_packages: []const DecodePackageSpec,
    host_backend: *const ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) ![]PjrtModelExecutor.DecodeVariant {
    if (decode_packages.len == 0) return &.{};

    const variants = try allocator.alloc(PjrtModelExecutor.DecodeVariant, decode_packages.len);
    var initialized: usize = 0;
    errdefer {
        for (variants[0..initialized]) |variant| {
            variant.executor.partitionExecutor().deinitExecutor();
            freeSemanticInputBindings(allocator, variant.semantic_input_bindings);
            freeSemanticOutputBindings(allocator, variant.semantic_output_bindings);
        }
        allocator.free(variants);
    }

    for (decode_packages, 0..) |pkg, i| {
        const seq_len = pkg.seq_len;
        if (seq_len == 0) return error.MissingArtifactMetadata;
        for (variants[0..i]) |existing| {
            if (existing.seq_len == seq_len) return error.AmbiguousCompiledArtifact;
        }

        const executor = try createExecutorFromHloWithShapes(
            allocator,
            graph,
            pkg.artifact_bytes,
            pkg.input_bindings,
            pkg.output_node_ids,
            pkg.input_shapes,
            pkg.output_shapes,
            host_backend,
            client,
        );
        errdefer executor.partitionExecutor().deinitExecutor();

        const semantic_input_bindings = try cloneSemanticInputBindings(allocator, pkg.semantic_input_bindings);
        errdefer freeSemanticInputBindings(allocator, semantic_input_bindings);
        const semantic_output_bindings = try cloneSemanticOutputBindings(allocator, pkg.semantic_output_bindings);
        errdefer freeSemanticOutputBindings(allocator, semantic_output_bindings);

        variants[i] = .{
            .seq_len = seq_len,
            .executor = executor,
            .input_materialization = detectInputMaterialization(
                pkg.input_bindings,
                pkg.semantic_input_bindings,
                pkg.semantic_output_bindings,
            ),
            .semantic_input_bindings = semantic_input_bindings,
            .semantic_output_bindings = semantic_output_bindings,
        };
        initialized += 1;
    }

    std.mem.sort(PjrtModelExecutor.DecodeVariant, variants, {}, struct {
        fn lessThan(_: void, a: PjrtModelExecutor.DecodeVariant, b: PjrtModelExecutor.DecodeVariant) bool {
            return a.seq_len < b.seq_len;
        }
    }.lessThan);
    return variants;
}

fn createDecodeVariantsFromExecutablePackages(
    allocator: Allocator,
    graph: *const Graph,
    decode_packages: []const DecodePackageSpec,
    host_backend: *const ComputeBackend,
    client: *pjrt_lib.pjrt.Client,
) ![]PjrtModelExecutor.DecodeVariant {
    if (decode_packages.len == 0) return &.{};

    const variants = try allocator.alloc(PjrtModelExecutor.DecodeVariant, decode_packages.len);
    var initialized: usize = 0;
    errdefer {
        for (variants[0..initialized]) |variant| {
            variant.executor.partitionExecutor().deinitExecutor();
            freeSemanticInputBindings(allocator, variant.semantic_input_bindings);
            freeSemanticOutputBindings(allocator, variant.semantic_output_bindings);
        }
        allocator.free(variants);
    }

    for (decode_packages, 0..) |pkg, i| {
        const seq_len = pkg.seq_len;
        if (seq_len == 0) return error.MissingArtifactMetadata;
        for (variants[0..i]) |existing| {
            if (existing.seq_len == seq_len) return error.AmbiguousCompiledArtifact;
        }

        const executor = try createExecutorFromSerializedExecutableWithShapes(
            allocator,
            graph,
            pkg.artifact_bytes,
            pkg.input_bindings,
            pkg.output_node_ids,
            pkg.input_shapes,
            pkg.output_shapes,
            host_backend,
            client,
        );
        errdefer executor.partitionExecutor().deinitExecutor();

        const semantic_input_bindings = try cloneSemanticInputBindings(allocator, pkg.semantic_input_bindings);
        errdefer freeSemanticInputBindings(allocator, semantic_input_bindings);
        const semantic_output_bindings = try cloneSemanticOutputBindings(allocator, pkg.semantic_output_bindings);
        errdefer freeSemanticOutputBindings(allocator, semantic_output_bindings);

        variants[i] = .{
            .seq_len = seq_len,
            .executor = executor,
            .input_materialization = detectInputMaterialization(
                pkg.input_bindings,
                pkg.semantic_input_bindings,
                pkg.semantic_output_bindings,
            ),
            .semantic_input_bindings = semantic_input_bindings,
            .semantic_output_bindings = semantic_output_bindings,
        };
        initialized += 1;
    }

    std.mem.sort(PjrtModelExecutor.DecodeVariant, variants, {}, struct {
        fn lessThan(_: void, a: PjrtModelExecutor.DecodeVariant, b: PjrtModelExecutor.DecodeVariant) bool {
            return a.seq_len < b.seq_len;
        }
    }.lessThan);
    return variants;
}

fn freeDecodeVariants(allocator: Allocator, variants: []PjrtModelExecutor.DecodeVariant) void {
    if (variants.len == 0) return;
    for (variants) |variant| {
        variant.executor.partitionExecutor().deinitExecutor();
        freeSemanticInputBindings(allocator, variant.semantic_input_bindings);
        freeSemanticOutputBindings(allocator, variant.semantic_output_bindings);
    }
    allocator.free(variants);
}

fn decodeSeqLenFromShapes(
    input_shapes: []const []const i64,
    semantic_input_bindings: []const PjrtSemanticInputBinding,
) ?usize {
    for (semantic_input_bindings) |binding| {
        if (binding.input_index >= input_shapes.len) return null;
        const shape = input_shapes[binding.input_index];
        if (shape.len == 0) return null;
        const dim = shape[0];
        if (dim <= 0) return null;
        return @as(usize, @intCast(dim)) + 1;
    }
    return null;
}

// ── Helpers ─────────────────────────────────────────────────────────

fn numElements(shape: []const i64) usize {
    var n: usize = 1;
    for (shape) |d| n *= @intCast(d);
    return n;
}

fn resolveUploadShapeFromTensor(
    allocator: Allocator,
    backend: *const ComputeBackend,
    tensor: CT,
    expected_shape: []const i64,
    element_count: usize,
) ![]i64 {
    const out = try allocator.dupe(i64, expected_shape);
    errdefer allocator.free(out);

    if (shapeHasDynamicDim(out)) {
        const actual_shape = try backend.tensorShape(tensor, allocator);
        defer allocator.free(actual_shape);
        if (actual_shape.len != out.len) return error.UnsupportedShape;
        for (out, actual_shape) |*dim, actual_dim| {
            if (dim.* < 0) dim.* = actual_dim;
        }
    }
    if (shapeHasDynamicDim(out)) {
        const inferred = try resolveUploadShapeFromElementCount(allocator, out, element_count);
        allocator.free(out);
        return inferred;
    }

    try validateConcreteElementCount(out, element_count);
    return out;
}

fn resolveUploadShapeFromElementCount(
    allocator: Allocator,
    expected_shape: []const i64,
    element_count: usize,
) ![]i64 {
    const out = try allocator.dupe(i64, expected_shape);
    errdefer allocator.free(out);

    var known_product: usize = 1;
    var dynamic_index: ?usize = null;
    for (out, 0..) |dim, idx| {
        if (dim < 0) {
            if (dynamic_index != null) return error.UnsupportedShape;
            dynamic_index = idx;
            continue;
        }
        if (dim == 0) return error.UnsupportedShape;
        known_product *= @intCast(dim);
    }
    if (dynamic_index) |idx| {
        if (known_product == 0 or element_count % known_product != 0) return error.UnsupportedShape;
        out[idx] = @intCast(element_count / known_product);
    }

    try validateConcreteElementCount(out, element_count);
    return out;
}

fn shapeHasDynamicDim(shape: []const i64) bool {
    for (shape) |dim| {
        if (dim < 0) return true;
    }
    return false;
}

fn validateConcreteElementCount(shape: []const i64, element_count: usize) !void {
    for (shape) |dim| {
        if (dim <= 0) return error.UnsupportedShape;
    }
    if (numElements(shape) != element_count) return error.UnsupportedShape;
}

fn sliceLastLogits(
    allocator: Allocator,
    output: []f32,
    shape: []const i64,
    query_seq_len: usize,
) ![]f32 {
    if (shape.len >= 2 and query_seq_len > 0) {
        const vocab_size: usize = @intCast(shape[shape.len - 1]);
        if (output.len >= query_seq_len * vocab_size) {
            const offset = (query_seq_len - 1) * vocab_size;
            return allocator.dupe(f32, output[offset..][0..vocab_size]);
        }
    }
    return allocator.dupe(f32, output);
}

fn firstNonSemanticOutputIndex(output_count: usize, bindings: []const PjrtSemanticOutputBinding) ?usize {
    for (0..output_count) |idx| {
        var semantic = false;
        for (bindings) |binding| {
            if (binding.output_index == idx) {
                semantic = true;
                break;
            }
        }
        if (!semantic) return idx;
    }
    return null;
}

fn primaryGraphOutputIndex(
    graph: *const Graph,
    output_node_ids: []const NodeId,
    bindings: []const PjrtSemanticOutputBinding,
) ?usize {
    var idx = graph.outputs.items.len;
    while (idx > 0) {
        idx -= 1;
        const graph_output_id = graph.outputs.items[idx];
        for (output_node_ids, 0..) |output_node_id, output_index| {
            if (output_node_id == graph_output_id and findSemanticOutputBinding(bindings, output_index) == null) {
                return output_index;
            }
        }
    }
    return firstNonSemanticOutputIndex(output_node_ids.len, bindings);
}

fn findSemanticOutputBinding(bindings: []const PjrtSemanticOutputBinding, output_index: usize) ?PjrtSemanticOutputBinding {
    for (bindings) |binding| {
        if (binding.output_index == output_index) return binding;
    }
    return null;
}

/// Bindings are sorted once by the owner. Each forward pass traverses them
/// alongside input positions, including sparse semantic inputs, in O(inputs +
/// bindings) time. Validation keeps direct low-level callers from silently
/// executing with duplicate, unordered, or out-of-range binding metadata.
const SemanticInputCursor = struct {
    bindings: []const PjrtSemanticInputBinding,
    position: usize = 0,

    fn init(bindings: []const PjrtSemanticInputBinding, num_inputs: usize) !@This() {
        for (bindings, 0..) |binding, i| {
            if (binding.input_index >= num_inputs or (i > 0 and bindings[i - 1].input_index >= binding.input_index)) return error.MissingArtifactMetadata;
        }
        return .{ .bindings = bindings };
    }

    // Called with increasing input positions by both execution passes.
    fn at(self: *@This(), input_index: usize) ?PjrtSemanticInputBinding {
        if (self.position == self.bindings.len) return null;
        const binding = self.bindings[self.position];
        if (binding.input_index != input_index) return null;
        self.position += 1;
        return binding;
    }
};

pub fn detectInputMaterialization(
    input_bindings: []const compiler.InputBinding,
    semantic_input_bindings: []const PjrtSemanticInputBinding,
    semantic_output_bindings: []const PjrtSemanticOutputBinding,
) PjrtInputMaterialization {
    if ((semantic_input_bindings.len > 0 or semantic_output_bindings.len > 0) and
        !inputBindingsRequireHostMaterialization(input_bindings))
    {
        return .backend_owned_state;
    }
    return .host_assisted_graph_inputs;
}

fn cloneSemanticInputBindings(
    allocator: Allocator,
    bindings: []const PjrtSemanticInputBinding,
) ![]PjrtSemanticInputBinding {
    if (bindings.len == 0) return &.{};
    const out = try allocator.alloc(PjrtSemanticInputBinding, bindings.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*binding| allocator.free(binding.name);
        allocator.free(out);
    }
    for (bindings, 0..) |binding, i| {
        out[i] = .{
            .input_index = binding.input_index,
            .name = try allocator.dupe(u8, binding.name),
            .layer_index = binding.layer_index,
            .kind = binding.kind,
        };
        initialized += 1;
    }
    std.mem.sort(PjrtSemanticInputBinding, out, {}, struct {
        fn lessThan(_: void, a: PjrtSemanticInputBinding, b: PjrtSemanticInputBinding) bool {
            return a.input_index < b.input_index;
        }
    }.lessThan);
    for (out, 0..) |binding, i| {
        if (i > 0 and out[i - 1].input_index == binding.input_index) return error.MissingArtifactMetadata;
    }
    return out;
}

fn freeSemanticInputBindings(allocator: Allocator, bindings: []PjrtSemanticInputBinding) void {
    if (bindings.len == 0) return;
    for (bindings) |*binding| allocator.free(binding.name);
    allocator.free(bindings);
}

fn cloneSemanticOutputBindings(
    allocator: Allocator,
    bindings: []const PjrtSemanticOutputBinding,
) ![]PjrtSemanticOutputBinding {
    if (bindings.len == 0) return &.{};
    const out = try allocator.alloc(PjrtSemanticOutputBinding, bindings.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*binding| allocator.free(binding.name);
        allocator.free(out);
    }
    for (bindings, 0..) |binding, i| {
        out[i] = .{
            .output_index = binding.output_index,
            .name = try allocator.dupe(u8, binding.name),
            .layer_index = binding.layer_index,
            .kind = binding.kind,
        };
        initialized += 1;
    }
    return out;
}

fn freeSemanticOutputBindings(allocator: Allocator, bindings: []PjrtSemanticOutputBinding) void {
    if (bindings.len == 0) return;
    for (bindings) |*binding| allocator.free(binding.name);
    allocator.free(bindings);
}

test "PJRT runtime capabilities distinguish host-assisted replay from backend-owned state" {
    const host_assisted = runtimeCapabilitiesForInputMaterialization(.host_assisted_graph_inputs);
    try std.testing.expect(host_assisted.supports_decode);
    try std.testing.expectEqual(model_runtime.RuntimeStateOwnership.host_assisted_inputs, host_assisted.state_ownership);

    const backend_owned = runtimeCapabilitiesForInputMaterialization(.backend_owned_state);
    try std.testing.expect(backend_owned.supports_decode);
    try std.testing.expectEqual(model_runtime.RuntimeStateOwnership.backend_owned, backend_owned.state_ownership);
}

test "PJRT retained buffer cache starts empty and reset is safe" {
    var cache = PjrtRetainedBufferCache.init(std.testing.allocator);
    defer cache.deinit();

    try std.testing.expectEqual(@as(usize, 0), cache.len());
    try std.testing.expect(cache.get("present.0.key") == null);
    cache.clear();
    try std.testing.expectEqual(@as(usize, 0), cache.len());
}

test "PJRT retained present buffers satisfy matching past semantic inputs" {
    const present_entry = PjrtRetainedBufferCache.Entry{
        .name = @constCast("present.0.key"),
        .layer_index = 0,
        .kind = .key,
        .buffer = undefined,
    };
    const matching_past = PjrtSemanticInputBinding{
        .input_index = 1,
        .name = "past_key_values.0.key",
        .layer_index = 0,
        .kind = .key,
    };
    const wrong_kind = PjrtSemanticInputBinding{
        .input_index = 2,
        .name = "past_key_values.0.value",
        .layer_index = 0,
        .kind = .value,
    };

    try std.testing.expect(retainedEntryMatchesSemanticInput(present_entry, matching_past));
    try std.testing.expect(!retainedEntryMatchesSemanticInput(present_entry, wrong_kind));
}

test "PJRT semantic output helper chooses first non-retained output" {
    const bindings = [_]PjrtSemanticOutputBinding{
        .{ .output_index = 1, .name = "present.0.key", .layer_index = 0, .kind = .key },
        .{ .output_index = 2, .name = "present.0.value", .layer_index = 0, .kind = .value },
    };
    try std.testing.expectEqual(@as(?usize, 0), firstNonSemanticOutputIndex(3, &bindings));
    const all_semantic = [_]PjrtSemanticOutputBinding{
        .{ .output_index = 0, .name = "present.0.key", .layer_index = 0, .kind = .key },
        .{ .output_index = 1, .name = "present.0.value", .layer_index = 0, .kind = .value },
    };
    try std.testing.expectEqual(@as(?usize, null), firstNonSemanticOutputIndex(2, &all_semantic));
}

test "PJRT primary graph output helper prefers final graph output" {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const x = try builder.parameter("x", ml.graph.Shape.init(.f32, &.{ 1, 4 }));
    const aux = try builder.gelu(x);
    const logits = try builder.gelu(aux);
    try graph.markOutput(aux);
    try graph.markOutput(logits);

    const outputs = [_]NodeId{ x, aux, logits };
    try std.testing.expectEqual(@as(?usize, 2), primaryGraphOutputIndex(&graph, &outputs, &.{}));

    const semantic_bindings = [_]PjrtSemanticOutputBinding{
        .{ .output_index = 1, .name = "present.0.key", .layer_index = 0, .kind = .key },
    };
    try std.testing.expectEqual(@as(?usize, 2), primaryGraphOutputIndex(&graph, &outputs, &semantic_bindings));

    const fallback_outputs = [_]NodeId{x};
    try std.testing.expectEqual(@as(?usize, 0), primaryGraphOutputIndex(&graph, &fallback_outputs, &.{}));
}

test "PJRT semantic binding helper detects backend owned state with semantic cache bindings" {
    const no_graph_inputs = [_]compiler.InputBinding{
        .{ .embedding_ids = 0 },
    };
    const semantic_input_bindings = [_]PjrtSemanticInputBinding{
        .{ .input_index = 1, .name = "past_key_values.0.key", .layer_index = 0, .kind = .key },
    };
    const semantic_output_bindings = [_]PjrtSemanticOutputBinding{
        .{ .output_index = 1, .name = "present.0.key", .layer_index = 0, .kind = .key },
    };
    try std.testing.expectEqual(
        PjrtInputMaterialization.backend_owned_state,
        detectInputMaterialization(&no_graph_inputs, &.{}, &semantic_output_bindings),
    );
    try std.testing.expectEqual(
        PjrtInputMaterialization.backend_owned_state,
        detectInputMaterialization(&no_graph_inputs, &semantic_input_bindings, &semantic_output_bindings),
    );
    var cursor = try SemanticInputCursor.init(&semantic_input_bindings, 2);
    try std.testing.expect(cursor.at(0) == null);
    try std.testing.expect(cursor.at(1) != null);
}

test "PJRT semantic cache bindings still report host-assisted when graph inputs remain" {
    const input_bindings = [_]compiler.InputBinding{
        .{ .embedding_ids = 0 },
        .{ .graph_node = 7 },
    };
    const semantic_output_bindings = [_]PjrtSemanticOutputBinding{
        .{ .output_index = 1, .name = "present.0.key", .layer_index = 0, .kind = .key },
    };

    try std.testing.expectEqual(
        PjrtInputMaterialization.host_assisted_graph_inputs,
        detectInputMaterialization(&input_bindings, &.{}, &semantic_output_bindings),
    );
}

test "PJRT runtime capabilities keep decode enabled for host-assisted package phases" {
    const decode_variants = [_]PjrtModelExecutor.DecodeVariant{
        .{
            .seq_len = 3,
            .executor = undefined,
            .input_materialization = .host_assisted_graph_inputs,
            .semantic_input_bindings = &.{},
            .semantic_output_bindings = &.{},
        },
    };

    var runtime: PjrtModelRuntime = .{
        .allocator = std.testing.allocator,
        .graph = undefined,
        .executor = undefined,
        .host_backend = undefined,
        .input_materialization = .host_assisted_graph_inputs,
        .retained_buffers = PjrtRetainedBufferCache.init(std.testing.allocator),
        .semantic_input_bindings = &.{},
        .semantic_output_bindings = &.{},
        .decode_variants = &decode_variants,
    };
    defer runtime.retained_buffers.deinit();

    const caps = PjrtModelRuntime.capabilities(&runtime);
    try std.testing.expect(caps.supports_decode);
    try std.testing.expectEqual(model_runtime.RuntimeStateOwnership.host_assisted_inputs, caps.state_ownership);
}

test "PJRT upload shape resolver fills one dynamic dimension from element count" {
    const shape = try resolveUploadShapeFromElementCount(std.testing.allocator, &.{ -1, 1536 }, 3072);
    defer std.testing.allocator.free(shape);

    try std.testing.expectEqual(@as(i64, 2), shape[0]);
    try std.testing.expectEqual(@as(i64, 1536), shape[1]);
}

test "PJRT upload shape resolver rejects ambiguous dynamic dimensions" {
    try std.testing.expectError(
        error.UnsupportedShape,
        resolveUploadShapeFromElementCount(std.testing.allocator, &.{ -1, -1 }, 16),
    );
}

test "managed PJRT borrowed KV names replace semantic identities without allocation" {
    const c = pjrt_lib.c_types;
    const Fake = struct {
        var destroyed: usize = 0;
        fn destroy(_: *c.BufferDestroyArgs) callconv(.c) ?*c.PjrtError {
            destroyed += 1;
            return null;
        }
    };
    var api = std.mem.zeroes(c.Api);
    api.struct_size = @sizeOf(c.Api);
    api._fns[58] = @ptrCast(&Fake.destroy);
    Fake.destroyed = 0;
    var failure = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var cache = PjrtRetainedBufferCache.init(failure.allocator());
    var live = true;
    defer if (live) cache.deinit();
    try cache.entries.ensureTotalCapacityPrecise(failure.allocator(), 1);
    failure.fail_index = failure.alloc_index;
    const buffer = pjrt_lib.pjrt.Buffer{ .api = &api, .handle = @ptrFromInt(1) };
    try cache.putBorrowed("prefill.key", 0, .key, buffer);
    try cache.putBorrowed("decode.key", 0, .key, buffer);
    try std.testing.expect(!failure.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 1), cache.len());
    try std.testing.expectEqual(@as(usize, 1), Fake.destroyed);
    try std.testing.expectEqualStrings("decode.key", cache.entries.items[0].name);
    try std.testing.expect(cache.getForSemanticInput(.{ .input_index = 0, .name = "past.key", .layer_index = 0, .kind = .key }) != null);
    cache.deinit();
    live = false;
    try std.testing.expectEqual(@as(usize, 2), Fake.destroyed);
}

test "managed PJRT indexed layerless states remain distinct across replacement and reset" {
    const c = pjrt_lib.c_types;
    const Fake = struct {
        fn destroy(_: *c.BufferDestroyArgs) callconv(.c) ?*c.PjrtError {
            return null;
        }
    };
    var api = std.mem.zeroes(c.Api);
    api.struct_size = @sizeOf(c.Api);
    api._fns[58] = @ptrCast(&Fake.destroy);
    var entries: [2]PjrtRetainedBufferCache.Entry = undefined;
    var index: [5]usize = @splat(0);
    var cache = PjrtRetainedBufferCache{ .allocator = std.testing.failing_allocator, .entries = .initBuffer(&entries), .owns_entries = false, .fixed_capacity = true, .semantic_index = &index };
    defer cache.deinit();
    for (0..3) |_| {
        try cache.putBorrowed("a", null, .key, .{ .api = &api, .handle = @ptrFromInt(1) });
        try cache.putBorrowed("b", null, .key, .{ .api = &api, .handle = @ptrFromInt(2) });
        try cache.putBorrowed("a", null, .key, .{ .api = &api, .handle = @ptrFromInt(3) });
        try std.testing.expectEqual(@as(usize, 2), cache.len());
        const a = cache.getForSemanticInput(.{ .input_index = 0, .name = "a", .layer_index = null, .kind = .key }).?;
        const b = cache.getForSemanticInput(.{ .input_index = 1, .name = "b", .layer_index = null, .kind = .key }).?;
        try std.testing.expectEqual(@as(usize, 3), @intFromPtr(a.handle));
        try std.testing.expectEqual(@as(usize, 2), @intFromPtr(b.handle));
        cache.clear();
    }
}

test "managed PJRT rejects out of range graph references before materialization" {
    var graph = Graph.init(std.testing.allocator);
    defer graph.deinit();
    try std.testing.expectError(error.MissingArtifactMetadata, validateInputNodes(&graph, &.{.{ .graph_node = 2147483647 }}));
    // Semantic KV IDs refer to the compiled phase, not the host graph.
    try validateInputNodes(&graph, &.{.{ .semantic_past_graph_node = 2147483647 }});
}

test "managed PJRT semantic index resolves collisions without allocation" {
    const c = pjrt_lib.c_types;
    const Fake = struct {
        fn destroy(_: *c.BufferDestroyArgs) callconv(.c) ?*c.PjrtError {
            return null;
        }
    };
    var api = std.mem.zeroes(c.Api);
    api.struct_size = @sizeOf(c.Api);
    api._fns[58] = @ptrCast(&Fake.destroy);
    var entries: [256]PjrtRetainedBufferCache.Entry = undefined;
    var index: [513]usize = @splat(0);
    var cache = PjrtRetainedBufferCache{ .allocator = std.testing.failing_allocator, .entries = .initBuffer(&entries), .owns_entries = false, .fixed_capacity = true, .semantic_index = &index };
    defer cache.deinit();
    for (0..2) |_| {
        for (0..256) |i| try cache.putBorrowed("present", @intCast(i), .key, .{ .api = &api, .handle = @ptrFromInt(i + 1) });
        for (0..256) |i| {
            const buffer = cache.getForSemanticInput(.{ .input_index = i, .name = "past", .layer_index = @intCast(i), .kind = .key }).?;
            try std.testing.expectEqual(i + 1, @intFromPtr(buffer.handle));
        }
        try std.testing.expect(cache.getForSemanticInput(.{ .input_index = 0, .name = "absent", .layer_index = 999, .kind = .key }) == null);
        var required: [128]PjrtSemanticInputBinding = undefined;
        for (&required, 0..) |*binding, i| binding.* = .{ .input_index = i, .name = "past", .layer_index = @intCast(i * 2 + 1), .kind = .key };
        var marks: [256]u8 = undefined;
        var arena = std.heap.FixedBufferAllocator.init(&marks);
        try cache.retainSemanticInputs(arena.allocator(), &required);
        try std.testing.expectEqual(@as(usize, 128), cache.len());
        for (0..256) |i| {
            const buffer = cache.getForSemanticInput(.{ .input_index = i, .name = "past", .layer_index = @intCast(i), .kind = .key });
            if (i % 2 == 1) try std.testing.expectEqual(i + 1, @intFromPtr(buffer.?.handle)) else try std.testing.expect(buffer == null);
        }
        // Missing inputs fail before pruning any live state, and the same
        // admitted mark arena can be reused after index compaction.
        try std.testing.expectError(error.MissingPastKeyValue, cache.retainSemanticInputs(arena.allocator(), &.{.{ .input_index = 0, .name = "missing", .layer_index = 999, .kind = .key }}));
        try std.testing.expectEqual(@as(usize, 128), cache.len());
        try cache.retainSemanticInputs(arena.allocator(), &required);
        cache.clear();
    }
}

test "managed PJRT semantic cursor traverses sparse inputs once" {
    var bindings: [1024]PjrtSemanticInputBinding = undefined;
    for (&bindings, 0..) |*binding, i| binding.* = .{ .input_index = i * 3 + 1, .name = "past", .layer_index = @intCast(i), .kind = .key };
    var cursor = try SemanticInputCursor.init(&bindings, 3073);
    for (0..3073) |i| {
        const binding = cursor.at(i);
        if (i % 3 == 1) try std.testing.expectEqual(i, binding.?.input_index) else try std.testing.expect(binding == null);
    }
    try std.testing.expectEqual(bindings.len, cursor.position);
    try std.testing.expectError(error.MissingArtifactMetadata, SemanticInputCursor.init(&bindings, 100));
    bindings[1].input_index = bindings[0].input_index;
    try std.testing.expectError(error.MissingArtifactMetadata, SemanticInputCursor.init(&bindings, 3073));
}

test "managed PJRT binding clones canonicalize order and reject duplicates" {
    const Scenario = struct {
        fn run(a: Allocator) !void {
            var bindings = [_]PjrtSemanticInputBinding{
                .{ .input_index = 4, .name = "last", .layer_index = 1, .kind = .key },
                .{ .input_index = 1, .name = "first", .layer_index = 0, .kind = .key },
            };
            const result = try cloneSemanticInputBindings(a, &bindings);
            defer freeSemanticInputBindings(a, result);
            var cursor = try SemanticInputCursor.init(result, 5);
            try std.testing.expect(cursor.at(0) == null);
            try std.testing.expectEqualStrings("first", cursor.at(1).?.name);
            bindings[1].input_index = 4;
            const duplicate = cloneSemanticInputBindings(a, &bindings) catch |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.MissingArtifactMetadata, err);
                return;
            };
            freeSemanticInputBindings(a, duplicate);
            return error.TestUnexpectedResult;
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Scenario.run, .{});
}

test "managed PJRT KV identity hashing agrees with equality" {
    const a = KvIdentity{ .name = "prefill", .layer_index = 0, .kind = .key };
    const b = KvIdentity{ .name = "past", .layer_index = 0, .kind = .key };
    try std.testing.expect(a.eql(b));
    try std.testing.expectEqual(a.hash(), b.hash());
    try std.testing.expect(!(KvIdentity{ .name = "a", .layer_index = null, .kind = .key }).eql(.{ .name = "b", .layer_index = null, .kind = .key }));
    try std.testing.expect(!a.eql(.{ .name = "prefill", .layer_index = 0, .kind = .value }));
}

test "PJRT whole-model caches permuted logits mappings and finds sparse decode phases" {
    const a = std.testing.allocator;
    var graph = Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input = try builder.parameter("input", ml.graph.Shape.init(.f32, &.{2}));
    const logits = try builder.parameter("logits", ml.graph.Shape.init(.f32, &.{2}));
    try graph.markOutput(logits);
    var prefill_ids = [_]NodeId{ input, logits, logits };
    var decode_ids = [_]NodeId{ logits, input };
    var prefill: PjrtExecutor = undefined;
    prefill.output_node_ids = &prefill_ids;
    var decode: PjrtExecutor = undefined;
    decode.output_node_ids = &decode_ids;
    var bindings = [_]PjrtSemanticOutputBinding{.{ .output_index = 2, .name = "present", .layer_index = 0, .kind = .key }};
    var variants = [_]PjrtModelExecutor.DecodeVariant{
        .{ .seq_len = 3, .executor = &decode, .input_materialization = .host_assisted_graph_inputs, .semantic_input_bindings = &.{}, .semantic_output_bindings = &.{} },
        .{ .seq_len = 16, .executor = &decode, .input_materialization = .host_assisted_graph_inputs, .semantic_input_bindings = &.{}, .semantic_output_bindings = &.{} },
        .{ .seq_len = 128, .executor = &decode, .input_materialization = .host_assisted_graph_inputs, .semantic_input_bindings = &.{}, .semantic_output_bindings = &.{} },
    };
    var model = PjrtModelExecutor{ .allocator = a, .graph = &graph, .executor = &prefill, .host_backend = undefined, .semantic_output_bindings = &bindings, .decode_variants = &variants };
    model.prepareOutputMappings();
    try std.testing.expectEqual(@as(?usize, 1), model.logits_index);
    var runtime: PjrtModelRuntime = undefined;
    runtime.decode_variants = &variants;
    for ([_]usize{ 3, 16, 128 }) |seq_len| {
        const phase = runtime.decodePhaseForSeqLen(seq_len).?;
        try std.testing.expectEqual(&decode, phase.executor);
        try std.testing.expectEqual(@as(?usize, 0), phase.logits_index);
    }
    for ([_]usize{ 0, 2, 4, 127, 129 }) |seq_len| try std.testing.expect(runtime.decodePhaseForSeqLen(seq_len) == null);
}
