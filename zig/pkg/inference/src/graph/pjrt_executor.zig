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

        for (self.input_bindings, 0..) |binding, i| {
            if (findSemanticInputBinding(semantic_input_bindings, i)) |semantic_binding| {
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
                if (try retainedBufferShapeMismatch(retained, self.input_shapes_[i])) |sizes| {
                    if (debug) {
                        std.log.warn(
                            "PJRT execute input[{d}/{d}] retained buffer shape mismatch name={s} expected_shape={any} expected_bytes={d} actual_bytes={d}",
                            .{
                                i + 1,
                                num_inputs,
                                semantic_binding.name,
                                self.input_shapes_[i],
                                sizes.expected_bytes,
                                sizes.actual_bytes,
                            },
                        );
                    }
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
                        const f32_data = try self.host_backend.toFloat32(ct, allocator);
                        defer allocator.free(f32_data);
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
                        const f32_data = try self.host_backend.toFloat32(ct, allocator);
                        defer allocator.free(f32_data);
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

    // 3. Collect shapes from graph nodes
    const num_inputs = result.input_bindings.len;
    const num_outputs = result.output_node_ids.len;

    const input_shapes = try allocator.alloc([]i64, num_inputs);
    errdefer {
        for (input_shapes[0..num_inputs]) |s| allocator.free(s);
        allocator.free(input_shapes);
    }
    for (result.input_shapes, 0..) |shape, i| input_shapes[i] = try allocator.dupe(i64, shape);

    const output_shapes = try allocator.alloc([]i64, num_outputs);
    errdefer {
        for (output_shapes[0..num_outputs]) |s| allocator.free(s);
        allocator.free(output_shapes);
    }
    for (result.output_shapes, 0..) |shape, i| output_shapes[i] = try allocator.dupe(i64, shape);

    // 4. Take ownership of node IDs
    const input_node_ids = try allocator.dupe(NodeId, result.input_node_ids);
    errdefer allocator.free(input_node_ids);
    const input_bindings = try allocator.dupe(compiler.InputBinding, result.input_bindings);
    errdefer allocator.free(input_bindings);
    const output_node_ids = try allocator.dupe(NodeId, result.output_node_ids);
    errdefer allocator.free(output_node_ids);

    // 5. Create executor
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
        },
        .backend_owned_state => .{
            .supports_decode = true,
            .state_ownership = .backend_owned,
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

pub const PjrtRetainedBufferKind = enum {
    key,
    value,
};

pub const PjrtSemanticOutputBinding = struct {
    output_index: usize,
    name: []const u8,
    layer_index: ?u32,
    kind: PjrtRetainedBufferKind,
};

pub const PjrtSemanticInputBinding = struct {
    input_index: usize,
    name: []const u8,
    layer_index: ?u32,
    kind: PjrtRetainedBufferKind,
};

pub const PjrtRetainedBufferCache = struct {
    allocator: Allocator,
    entries: std.ArrayListUnmanaged(Entry) = .empty,

    pub const Entry = struct {
        name: []u8,
        layer_index: ?u32,
        kind: PjrtRetainedBufferKind,
        buffer: pjrt_lib.pjrt.Buffer,
    };

    pub fn init(allocator: Allocator) PjrtRetainedBufferCache {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *PjrtRetainedBufferCache) void {
        self.clear();
        self.entries.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn clear(self: *PjrtRetainedBufferCache) void {
        const debug = pjrtExecDebugEnabled();
        if (debug) std.log.info("PJRT retained buffer clear entries={d}", .{self.entries.items.len});
        for (self.entries.items, 0..) |*entry, i| {
            if (debug) std.log.info("PJRT retained buffer deinit[{d}] name={s}", .{ i, entry.name });
            entry.buffer.deinit();
            if (debug) std.log.info("PJRT retained buffer deinit[{d}] complete", .{i});
            self.allocator.free(entry.name);
        }
        self.entries.clearRetainingCapacity();
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
        if (self.get(binding.name)) |buffer| return buffer;
        for (self.entries.items) |*entry| {
            if (retainedEntryMatchesSemanticInput(entry.*, binding)) return &entry.buffer;
        }
        return null;
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
        for (self.entries.items) |*entry| {
            if (!std.mem.eql(u8, entry.name, name)) continue;
            entry.buffer.deinit();
            self.allocator.free(entry.name);
            entry.* = .{
                .name = owned_name,
                .layer_index = layer_index,
                .kind = kind,
                .buffer = buffer,
            };
            return;
        }
        try self.entries.append(self.allocator, .{
            .name = owned_name,
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
    return entry.kind == binding.kind and entry.layer_index == binding.layer_index;
}

pub const PjrtModelExecutor = struct {
    allocator: Allocator,
    graph: *const Graph,
    executor: *PjrtExecutor,
    host_backend: *const ComputeBackend,
    input_materialization: PjrtInputMaterialization = .host_assisted_graph_inputs,
    semantic_input_bindings: []PjrtSemanticInputBinding = &.{},
    semantic_output_bindings: []PjrtSemanticOutputBinding = &.{},
    decode_variants: []DecodeVariant = &.{},

    pub const DecodeVariant = struct {
        seq_len: usize,
        executor: *PjrtExecutor,
        input_materialization: PjrtInputMaterialization,
        semantic_input_bindings: []PjrtSemanticInputBinding,
        semantic_output_bindings: []PjrtSemanticOutputBinding,
    };

    const executor_vtable = model_runtime.ModelExecutor.VTable{
        .create_runtime = createRuntime,
        .deinit = deinit,
    };

    pub fn modelExecutor(self: *@This()) model_runtime.ModelExecutor {
        return .{ .ptr = self, .vtable = &executor_vtable };
    }

    fn createRuntime(ctx: *anyopaque, allocator: Allocator) !model_runtime.ModelRuntime {
        const self: *PjrtModelExecutor = @ptrCast(@alignCast(ctx));
        const runtime_ctx = try allocator.create(PjrtModelRuntime);
        runtime_ctx.* = .{
            .allocator = allocator,
            .graph = self.graph,
            .executor = self.executor,
            .host_backend = self.host_backend,
            .input_materialization = self.input_materialization,
            .retained_buffers = PjrtRetainedBufferCache.init(allocator),
            .semantic_input_bindings = self.semantic_input_bindings,
            .semantic_output_bindings = self.semantic_output_bindings,
            .decode_variants = self.decode_variants,
        };
        return .{ .ptr = runtime_ctx, .vtable = &PjrtModelRuntime.runtime_vtable };
    }

    fn deinit(ctx: *anyopaque) void {
        const self: *PjrtModelExecutor = @ptrCast(@alignCast(ctx));
        const allocator = self.allocator;
        if (pjrtExecDebugEnabled()) std.log.info("PJRT ModelExecutor deinit begin", .{});
        self.executor.partitionExecutor().deinitExecutor();
        if (pjrtExecDebugEnabled()) std.log.info("PJRT ModelExecutor prefill executor deinit complete", .{});
        freeSemanticInputBindings(allocator, self.semantic_input_bindings);
        freeSemanticOutputBindings(allocator, self.semantic_output_bindings);
        freeDecodeVariants(allocator, self.decode_variants);
        if (pjrtExecDebugEnabled()) std.log.info("PJRT ModelExecutor decode variants deinit complete", .{});
        allocator.destroy(self);
        if (pjrtExecDebugEnabled()) std.log.info("PJRT ModelExecutor deinit complete", .{});
    }
};

pub fn deinitModelExecutorPtr(ctx: *anyopaque) void {
    PjrtModelExecutor.deinit(ctx);
}

const PjrtModelRuntime = struct {
    allocator: Allocator,
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
            };
        }
        return runtimeCapabilitiesForInputMaterialization(self.input_materialization);
    }

    fn prefill(
        ctx: *anyopaque,
        allocator: Allocator,
        request: model_runtime.PrefillRequest,
    ) !model_runtime.ModelOutput {
        const self: *PjrtModelRuntime = @ptrCast(@alignCast(ctx));
        return self.executeEmbeddingIds(allocator, request.input_ids, request.query_seq_len, self.prefillPhase());
    }

    fn decode(
        ctx: *anyopaque,
        allocator: Allocator,
        request: model_runtime.DecodeRequest,
    ) !model_runtime.ModelOutput {
        const self: *PjrtModelRuntime = @ptrCast(@alignCast(ctx));
        const ids = [_]i64{request.token_id};
        const seq_len = request.position + 1;
        const phase = if (self.decode_variants.len > 0)
            self.decodePhaseForSeqLen(seq_len) orelse return error.UnsupportedShape
        else
            self.prefillPhase();
        return self.executeEmbeddingIds(allocator, &ids, 1, phase);
    }

    const Phase = struct {
        executor: *PjrtExecutor,
        semantic_input_bindings: []const PjrtSemanticInputBinding,
        semantic_output_bindings: []const PjrtSemanticOutputBinding,
    };

    fn prefillPhase(self: *const PjrtModelRuntime) Phase {
        return .{
            .executor = self.executor,
            .semantic_input_bindings = self.semantic_input_bindings,
            .semantic_output_bindings = self.semantic_output_bindings,
        };
    }

    fn decodePhaseForSeqLen(self: *const PjrtModelRuntime, seq_len: usize) ?Phase {
        for (self.decode_variants) |variant| {
            if (variant.seq_len != seq_len) continue;
            return .{
                .executor = variant.executor,
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
        const count = self.graph.nodeCount();
        const values = try allocator.alloc(?CT, count);
        defer {
            for (values) |maybe_ct| {
                if (maybe_ct) |ct| self.host_backend.free(ct);
            }
            allocator.free(values);
        }
        @memset(values, null);

        try self.materializeGraphInputs(values, phase);

        const exec_ctx: PartitionExecutor.ExecutionContext = .{ .embedding_ids = input_ids };
        var outputs = try phase.executor.executeToBuffersWithRetained(
            values,
            exec_ctx,
            allocator,
            &self.retained_buffers,
            phase.semantic_input_bindings,
        );
        defer allocator.free(outputs);
        const debug = pjrtExecDebugEnabled();
        if (debug) {
            std.log.info(
                "PJRT ModelRuntime post-execute outputs={d} semantic_outputs={d}",
                .{ outputs.len, phase.semantic_output_bindings.len },
            );
        }

        var retained = try allocator.alloc(bool, outputs.len);
        defer allocator.free(retained);
        @memset(retained, false);
        defer {
            for (outputs, 0..) |*buffer, i| {
                if (!retained[i]) {
                    if (pjrtExecDebugEnabled()) std.log.info("PJRT ModelRuntime deinit transient output[{d}]", .{i});
                    buffer.deinit();
                    if (pjrtExecDebugEnabled()) std.log.info("PJRT ModelRuntime deinit transient output[{d}] complete", .{i});
                }
            }
        }

        for (phase.semantic_output_bindings) |binding| {
            if (binding.output_index >= outputs.len) return error.MissingOutput;
            if (debug) {
                std.log.info(
                    "PJRT ModelRuntime retain output[{d}] name={s}",
                    .{ binding.output_index, binding.name },
                );
            }
            try self.retained_buffers.put(binding.name, binding.layer_index, binding.kind, outputs[binding.output_index]);
            retained[binding.output_index] = true;
        }

        const logits_index = primaryGraphOutputIndex(self.graph, phase.executor.output_node_ids, phase.semantic_output_bindings) orelse return error.MissingOutput;
        if (debug) {
            const size_bytes = try outputs[logits_index].onDeviceSizeInBytes();
            std.log.info(
                "PJRT ModelRuntime read logits output[{d}] shape={any} bytes={d}",
                .{ logits_index, phase.executor.output_shapes[logits_index], size_bytes },
            );
        }
        const output_f32 = try outputs[logits_index].toFloat32(allocator);
        defer allocator.free(output_f32);
        if (debug) std.log.info("PJRT ModelRuntime logits read floats={d}", .{output_f32.len});
        const logits = try sliceLastLogits(allocator, output_f32, phase.executor.output_shapes[logits_index], query_seq_len);
        if (debug) std.log.info("PJRT ModelRuntime returning logits floats={d}", .{logits.len});
        return .{ .logits = logits };
    }

    fn materializeGraphInputs(self: *PjrtModelRuntime, values: []?CT, phase: Phase) !void {
        for (phase.executor.input_bindings, 0..) |binding, input_index| {
            if (findSemanticInputBinding(phase.semantic_input_bindings, input_index) != null) continue;
            switch (binding) {
                .graph_node => |node_id| {
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

    fn deinit(ctx: *anyopaque) void {
        const self: *PjrtModelRuntime = @ptrCast(@alignCast(ctx));
        const allocator = self.allocator;
        if (pjrtExecDebugEnabled()) std.log.info("PJRT ModelRuntime deinit begin", .{});
        self.retained_buffers.deinit();
        if (pjrtExecDebugEnabled()) std.log.info("PJRT ModelRuntime retained buffers deinit complete", .{});
        allocator.destroy(self);
        if (pjrtExecDebugEnabled()) std.log.info("PJRT ModelRuntime deinit complete", .{});
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

const RetainedBufferSizeMismatch = struct {
    expected_bytes: usize,
    actual_bytes: usize,
};

fn staticNumElements(shape: []const i64) ?usize {
    var n: usize = 1;
    for (shape) |d| {
        if (d < 0) return null;
        n *= @intCast(d);
    }
    return n;
}

fn retainedBufferShapeMismatch(
    buffer: *const pjrt_lib.pjrt.Buffer,
    expected_shape: []const i64,
) !?RetainedBufferSizeMismatch {
    const expected_elements = staticNumElements(expected_shape) orelse return null;
    const expected_bytes = expected_elements * @sizeOf(f32);
    const actual_bytes = try buffer.onDeviceSizeInBytes();
    if (actual_bytes == expected_bytes) return null;
    return .{
        .expected_bytes = expected_bytes,
        .actual_bytes = actual_bytes,
    };
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

fn findSemanticInputBinding(bindings: []const PjrtSemanticInputBinding, input_index: usize) ?PjrtSemanticInputBinding {
    for (bindings) |binding| {
        if (binding.input_index == input_index) return binding;
    }
    return null;
}

fn detectInputMaterialization(
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
    try std.testing.expect(findSemanticInputBinding(&semantic_input_bindings, 1) != null);
    try std.testing.expect(findSemanticInputBinding(&semantic_input_bindings, 0) == null);
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
