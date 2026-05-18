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

//! ONNX Runtime partition executor.

const std = @import("std");
const build_options = @import("build_options");
const backends = @import("../backends/backends.zig");
const c_file = @import("../util/c_file.zig");

const ops_mod = @import("../ops/ops.zig");
const contracts = @import("backend_contracts.zig");
const CT = contracts.CT;
const ComputeBackend = ops_mod.ComputeBackend;

const partition_mod = @import("partition.zig");
const PartitionExecutor = partition_mod.PartitionExecutor;
const Partition = partition_mod.Partition;
const DeviceId = @import("device_mesh.zig").DeviceId;
const Graph = @import("ml").graph.Graph;
const NodeId = @import("ml").graph.NodeId;
const Shape = @import("ml").graph.Shape;

const compiler = @import("onnx_compiler.zig");

const Tensor = backends.Tensor;
const Session = backends.Session;

pub const OnnxExecutor = struct {
    allocator: std.mem.Allocator,
    session: Session,
    host_backend: *const ComputeBackend,
    input_node_ids: []const NodeId,
    input_shapes: []const Shape,
    output_node_ids: []const NodeId,
    pe: PartitionExecutor = undefined,

    const vtable = PartitionExecutor.VTable{
        .execute = &executeFn,
        .deinit = &deinitFn,
    };

    pub fn partitionExecutor(self: *OnnxExecutor) *const PartitionExecutor {
        return &self.pe;
    }

    fn executeFn(
        ctx: *anyopaque,
        values: []?CT,
        value_device: []DeviceId,
        _: []const NodeId,
        device_id: DeviceId,
        _: PartitionExecutor.ExecutionContext,
    ) anyerror!void {
        const self: *OnnxExecutor = @ptrCast(@alignCast(ctx));

        const input_info = self.session.inputInfo();
        if (input_info.len != self.input_node_ids.len) return error.OnnxInputArityMismatch;

        const inputs = try self.allocator.alloc(Tensor, input_info.len);
        defer {
            for (inputs) |*tensor| tensor.deinit();
            self.allocator.free(inputs);
        }

        for (input_info, self.input_node_ids, self.input_shapes, 0..) |info, nid, shape, i| {
            const ct = values[@intCast(nid)] orelse return error.MissingValue;
            const data = try self.host_backend.toFloat32(ct, self.allocator);
            defer self.allocator.free(data);
            const shape_i64 = try shapeToI64(self.allocator, shape);
            defer self.allocator.free(shape_i64);
            inputs[i] = try Tensor.initFloat32(self.allocator, info.name, shape_i64, data);
        }

        const outputs = try self.session.run(inputs, self.allocator);
        defer {
            for (outputs) |*tensor| tensor.deinit();
            self.allocator.free(outputs);
        }

        if (outputs.len != self.output_node_ids.len) return error.OnnxOutputArityMismatch;

        for (outputs, self.output_node_ids) |*tensor, nid| {
            if (tensor.dtype != .f32) return error.UnsupportedOnnxOutputDType;
            const data = try tensorDataAsF32(self.allocator, tensor);
            defer self.allocator.free(data);
            const shape_i32 = try castShapeToI32(self.allocator, tensor.shape);
            defer self.allocator.free(shape_i32);
            const ct = try self.host_backend.fromFloat32Shape(data, shape_i32);
            values[@intCast(nid)] = ct;
            value_device[@intCast(nid)] = device_id;
        }
    }

    fn deinitFn(ctx: *anyopaque) void {
        const self: *OnnxExecutor = @ptrCast(@alignCast(ctx));
        self.session.close();
        self.allocator.free(self.input_node_ids);
        self.allocator.free(self.input_shapes);
        self.allocator.free(self.output_node_ids);
        self.allocator.destroy(self);
    }
};

pub fn createExecutor(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    part: *const Partition,
    cb: *const ComputeBackend,
    host_backend: *const ComputeBackend,
    cache_dir: ?[]const u8,
) !*OnnxExecutor {
    if (!build_options.enable_onnx) return error.OnnxUnavailable;

    var result = try compiler.compilePartition(allocator, graph, part, cb, null, .{}, null, null, null, &.{}, .{});
    defer result.deinit();

    const base_dir = cache_dir orelse "/tmp";
    const model_path = try std.fmt.allocPrint(allocator, "{s}/termite_onnx_{x}.onnx", .{
        base_dir,
        std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(part.node_ids)),
    });
    defer allocator.free(model_path);

    try writeAbsoluteFile(allocator, model_path, result.onnx_bytes);

    const session = try backends.onnx.createSession(allocator, model_path);
    errdefer session.close();

    if (session.inputInfo().len != result.input_node_ids.len) return error.OnnxInputArityMismatch;
    if (session.outputInfo().len != result.output_node_ids.len) return error.OnnxOutputArityMismatch;

    const input_shapes = try allocator.alloc(Shape, result.input_node_ids.len);
    errdefer allocator.free(input_shapes);
    for (result.input_node_ids, 0..) |nid, i| {
        input_shapes[i] = graph.node(nid).output_shape;
    }

    const exec = try allocator.create(OnnxExecutor);
    exec.* = .{
        .allocator = allocator,
        .session = session,
        .host_backend = host_backend,
        .input_node_ids = try allocator.dupe(NodeId, result.input_node_ids),
        .input_shapes = input_shapes,
        .output_node_ids = try allocator.dupe(NodeId, result.output_node_ids),
    };
    exec.pe = .{ .ptr = exec, .vtable = &OnnxExecutor.vtable };
    return exec;
}

fn tensorDataAsF32(allocator: std.mem.Allocator, tensor: *const Tensor) ![]f32 {
    if (tensor.asFloat32IfAligned()) |aligned| {
        return allocator.dupe(f32, aligned);
    }
    const elem_count = tensor.elementCount();
    const copied = try allocator.alloc(f32, elem_count);
    @memcpy(std.mem.sliceAsBytes(copied), tensor.data[0 .. elem_count * @sizeOf(f32)]);
    return copied;
}

fn castShapeToI32(allocator: std.mem.Allocator, shape: []const i64) ![]i32 {
    const result = try allocator.alloc(i32, shape.len);
    errdefer allocator.free(result);
    for (shape, 0..) |dim, i| {
        result[i] = std.math.cast(i32, dim) orelse return error.UnsupportedShape;
    }
    return result;
}

fn shapeToI64(allocator: std.mem.Allocator, shape: Shape) ![]i64 {
    const result = try allocator.alloc(i64, shape.rank());
    errdefer allocator.free(result);
    for (0..shape.rank()) |axis| {
        result[axis] = shape.dim(@intCast(axis));
    }
    return result;
}

fn writeAbsoluteFile(
    allocator: std.mem.Allocator,
    path: []const u8,
    data: []const u8,
) !void {
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    const fd = c_file.c.open(path_z.ptr, c_file.c.O_WRONLY | c_file.c.O_CREAT | c_file.c.O_TRUNC, @as(c_file.c.mode_t, 0o644));
    if (fd < 0) return error.CreateFailed;
    defer _ = c_file.c.close(fd);

    var written: usize = 0;
    while (written < data.len) {
        const n = c_file.c.write(fd, data[written..].ptr, data.len - written);
        if (n <= 0) return error.WriteFailed;
        written += @intCast(n);
    }
}
