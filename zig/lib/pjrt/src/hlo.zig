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

//! HLO (High-Level Operations) program builder and serializer.
//!
//! Builds XLA HLO computation graphs and serializes them to the
//! HloModuleProto protobuf format accepted by PJRT's compile API
//! (format = "hlo").
//!
//! The builder provides a small, opinionated XLA-style API (shape
//! inference, implicit broadcasts, composite ops). Serialization goes
//! through the generated xla_proto types (from xla/service/hlo.proto)
//! so the wire format stays in lockstep with upstream XLA without any
//! hand-maintained field numbers.

const std = @import("std");
const xla = @import("xla_proto").xla;
const Allocator = std.mem.Allocator;

// ── Public types ────────────────────────────────────────────────────

/// XLA element types (matches xla/xla_data.proto PrimitiveType enum).
pub const ElementType = enum(i32) {
    pred = 1,
    s8 = 2,
    s16 = 3,
    s32 = 4,
    s64 = 5,
    u8 = 6,
    u16 = 7,
    u32 = 8,
    u64 = 9,
    f16 = 10,
    f32 = 11,
    f64 = 12,
    bf16 = 16,
    c64 = 15,
    c128 = 18,

    pub fn byteSize(self: ElementType) usize {
        return switch (self) {
            .pred, .s8, .u8 => 1,
            .s16, .u16, .f16, .bf16 => 2,
            .s32, .u32, .f32 => 4,
            .s64, .u64, .f64, .c64 => 8,
            .c128 => 16,
        };
    }
};

/// Shape of an HLO value.
pub const Shape = struct {
    element_type: ElementType,
    dimensions: []const i64,

    pub fn init(element_type: ElementType, dimensions: []const i64) Shape {
        return .{ .element_type = element_type, .dimensions = dimensions };
    }

    pub fn scalar(element_type: ElementType) Shape {
        return .{ .element_type = element_type, .dimensions = &.{} };
    }

    pub fn elementCount(self: Shape) usize {
        var count: usize = 1;
        for (self.dimensions) |d| count *= @intCast(d);
        return count;
    }
};

pub const DotDimensionNumbers = struct {
    lhs_contracting_dimensions: []const i64 = &.{},
    rhs_contracting_dimensions: []const i64 = &.{},
    lhs_batch_dimensions: []const i64 = &.{},
    rhs_batch_dimensions: []const i64 = &.{},
};

pub const GatherDimensionNumbers = struct {
    offset_dims: []const i64 = &.{},
    collapsed_slice_dims: []const i64 = &.{},
    start_index_map: []const i64 = &.{},
    index_vector_dim: i64 = 0,
};

/// Instruction ID — references an instruction in the computation.
pub const Id = u64;

// ── Builder ─────────────────────────────────────────────────────────

/// Builds an HLO computation graph. Instructions are appended in order;
/// the last instruction added is the root unless overridden.
pub const Builder = struct {
    allocator: Allocator,
    name: []const u8,
    instructions: std.ArrayListUnmanaged(Instruction),
    next_id: Id,
    /// First ID this builder will assign. Used by getInst to compute array index.
    base_id: Id = 1,

    const Instruction = struct {
        id: Id,
        opcode: []const u8,
        name: []const u8,
        shape: Shape,
        operand_ids: []const Id,
        // Op-specific payloads (null if unused):
        parameter_number: ?u32 = null,
        dot_dimension_numbers: ?DotDimensionNumbers = null,
        gather_dimension_numbers: ?GatherDimensionNumbers = null,
        gather_slice_sizes: ?[]const i64 = null,
        slice_dimensions: ?[]const xla.HloInstructionProto.SliceDimensions = null,
        dimensions: ?[]const i64 = null,
        literal_f32s: ?[]const f32 = null,
        literal_f32s_owned: bool = false,
        literal_s32s: ?[]const i32 = null,
        called_computation_ids: ?[]const Id = null,
        comparison_direction: ?[]const u8 = null,
        // Tuple root support:
        is_tuple_root: bool = false,
        tuple_child_shapes: ?[]Shape = null,
    };

    pub fn init(allocator: Allocator, name: []const u8) Builder {
        return .{
            .allocator = allocator,
            .name = name,
            .instructions = .empty,
            .next_id = 1, // IDs start at 1 (XLA treats 0 as "not set")
            .base_id = 1,
        };
    }

    pub fn deinit(self: *Builder) void {
        for (self.instructions.items) |inst| {
            self.allocator.free(inst.name);
            if (inst.shape.dimensions.len > 0)
                self.allocator.free(inst.shape.dimensions);
            if (inst.operand_ids.len > 0)
                self.allocator.free(inst.operand_ids);
            if (inst.dimensions) |d|
                self.allocator.free(d);
            if (inst.called_computation_ids) |c|
                self.allocator.free(c);
            if (inst.gather_slice_sizes) |s|
                self.allocator.free(s);
            if (inst.slice_dimensions) |s|
                self.allocator.free(s);
            if (inst.literal_f32s_owned)
                if (inst.literal_f32s) |f| self.allocator.free(f);
            if (inst.tuple_child_shapes) |tcs|
                self.allocator.free(tcs);
            if (inst.dot_dimension_numbers) |dot_dims| {
                if (dot_dims.lhs_contracting_dimensions.len > 0)
                    self.allocator.free(dot_dims.lhs_contracting_dimensions);
                if (dot_dims.rhs_contracting_dimensions.len > 0)
                    self.allocator.free(dot_dims.rhs_contracting_dimensions);
                if (dot_dims.lhs_batch_dimensions.len > 0)
                    self.allocator.free(dot_dims.lhs_batch_dimensions);
                if (dot_dims.rhs_batch_dimensions.len > 0)
                    self.allocator.free(dot_dims.rhs_batch_dimensions);
            }
            if (inst.gather_dimension_numbers) |gather_dims| {
                if (gather_dims.offset_dims.len > 0)
                    self.allocator.free(gather_dims.offset_dims);
                if (gather_dims.collapsed_slice_dims.len > 0)
                    self.allocator.free(gather_dims.collapsed_slice_dims);
                if (gather_dims.start_index_map.len > 0)
                    self.allocator.free(gather_dims.start_index_map);
            }
        }
        self.instructions.deinit(self.allocator);
    }

    fn nextId(self: *Builder) Id {
        const id = self.next_id;
        self.next_id += 1;
        return id;
    }

    fn addInst(self: *Builder, inst: Instruction) !Id {
        // Duplicate borrowed slices so the instruction owns its memory
        // (callers may pass slices pointing to stack or temp-allocated buffers).
        var owned_inst = inst;
        // XLA requires unique instruction names within a computation.
        owned_inst.name = try std.fmt.allocPrint(self.allocator, "{s}.{d}", .{ inst.name, inst.id });
        if (inst.shape.dimensions.len > 0) {
            owned_inst.shape.dimensions = try self.allocator.dupe(i64, inst.shape.dimensions);
        }
        // Duplicate DotDimensionNumbers slices (often stack-allocated by callers like matmul).
        if (inst.dot_dimension_numbers) |dot_dims| {
            owned_inst.dot_dimension_numbers = .{
                .lhs_contracting_dimensions = if (dot_dims.lhs_contracting_dimensions.len > 0) try self.allocator.dupe(i64, dot_dims.lhs_contracting_dimensions) else dot_dims.lhs_contracting_dimensions,
                .rhs_contracting_dimensions = if (dot_dims.rhs_contracting_dimensions.len > 0) try self.allocator.dupe(i64, dot_dims.rhs_contracting_dimensions) else dot_dims.rhs_contracting_dimensions,
                .lhs_batch_dimensions = if (dot_dims.lhs_batch_dimensions.len > 0) try self.allocator.dupe(i64, dot_dims.lhs_batch_dimensions) else dot_dims.lhs_batch_dimensions,
                .rhs_batch_dimensions = if (dot_dims.rhs_batch_dimensions.len > 0) try self.allocator.dupe(i64, dot_dims.rhs_batch_dimensions) else dot_dims.rhs_batch_dimensions,
            };
        }
        if (inst.gather_dimension_numbers) |gather_dims| {
            owned_inst.gather_dimension_numbers = .{
                .offset_dims = if (gather_dims.offset_dims.len > 0) try self.allocator.dupe(i64, gather_dims.offset_dims) else gather_dims.offset_dims,
                .collapsed_slice_dims = if (gather_dims.collapsed_slice_dims.len > 0) try self.allocator.dupe(i64, gather_dims.collapsed_slice_dims) else gather_dims.collapsed_slice_dims,
                .start_index_map = if (gather_dims.start_index_map.len > 0) try self.allocator.dupe(i64, gather_dims.start_index_map) else gather_dims.start_index_map,
                .index_vector_dim = gather_dims.index_vector_dim,
            };
        }
        if (inst.gather_slice_sizes) |slice_sizes| {
            owned_inst.gather_slice_sizes = try self.allocator.dupe(i64, slice_sizes);
        }
        if (inst.slice_dimensions) |slice_dims| {
            owned_inst.slice_dimensions = try self.allocator.dupe(xla.HloInstructionProto.SliceDimensions, slice_dims);
        }
        try self.instructions.append(self.allocator, owned_inst);
        return inst.id;
    }

    pub fn getInst(self: *const Builder, id: Id) *const Instruction {
        return &self.instructions.items[@intCast(id - self.base_id)];
    }

    fn dupeIds(self: *Builder, ids: []const Id) ![]const Id {
        return self.allocator.dupe(Id, ids);
    }

    fn dupeI64s(self: *Builder, vals: []const i64) ![]const i64 {
        return self.allocator.dupe(i64, vals);
    }

    // ── Parameters & constants ──────────────────────────────────────

    pub fn parameter(self: *Builder, param_number: u32, shape: Shape, name: []const u8) !Id {
        return self.addInst(.{
            .id = self.nextId(),
            .opcode = "parameter",
            .name = name,
            .shape = shape,
            .operand_ids = &.{},
            .parameter_number = param_number,
        });
    }

    pub fn constantF32(self: *Builder, shape: Shape, data: []const f32) !Id {
        return self.addInst(.{
            .id = self.nextId(),
            .opcode = "constant",
            .name = "constant",
            .shape = shape,
            .operand_ids = &.{},
            .literal_f32s = data,
        });
    }

    pub fn constantS32(self: *Builder, shape: Shape, data: []const i32) !Id {
        return self.addInst(.{
            .id = self.nextId(),
            .opcode = "constant",
            .name = "constant",
            .shape = shape,
            .operand_ids = &.{},
            .literal_s32s = data,
        });
    }

    pub fn constantScalarF32(self: *Builder, value: f32) !Id {
        const data = try self.allocator.dupe(f32, &.{value});
        return self.addInst(.{
            .id = self.nextId(),
            .opcode = "constant",
            .name = "constant",
            .shape = Shape.scalar(.f32),
            .operand_ids = &.{},
            .literal_f32s = data,
            .literal_f32s_owned = true,
        });
    }

    // ── Elementwise unary ───────────────────────────────────────────

    fn unaryOp(self: *Builder, opcode: []const u8, operand: Id) !Id {
        const op_shape = self.getInst(operand).shape;
        const ids = try self.dupeIds(&.{operand});
        return self.addInst(.{
            .id = self.nextId(),
            .opcode = opcode,
            .name = opcode,
            .shape = op_shape,
            .operand_ids = ids,
        });
    }

    pub fn negate(self: *Builder, x: Id) !Id {
        return self.unaryOp("negate", x);
    }
    pub fn abs(self: *Builder, x: Id) !Id {
        return self.unaryOp("abs", x);
    }
    pub fn exponential(self: *Builder, x: Id) !Id {
        return self.unaryOp("exponential", x);
    }
    pub fn log(self: *Builder, x: Id) !Id {
        return self.unaryOp("log", x);
    }
    pub fn sqrt(self: *Builder, x: Id) !Id {
        return self.unaryOp("sqrt", x);
    }
    pub fn rsqrt(self: *Builder, x: Id) !Id {
        return self.unaryOp("rsqrt", x);
    }
    pub fn tanh(self: *Builder, x: Id) !Id {
        return self.unaryOp("tanh", x);
    }
    pub fn logistic(self: *Builder, x: Id) !Id {
        return self.unaryOp("logistic", x);
    }
    pub fn sine(self: *Builder, x: Id) !Id {
        return self.unaryOp("sine", x);
    }
    pub fn cosine(self: *Builder, x: Id) !Id {
        return self.unaryOp("cosine", x);
    }
    pub fn erf(self: *Builder, x: Id) !Id {
        return self.unaryOp("erf", x);
    }

    // ── Elementwise binary ──────────────────────────────────────────

    fn binaryOp(self: *Builder, opcode: []const u8, lhs: Id, rhs: Id) !Id {
        const lhs_shape = self.getInst(lhs).shape;
        const rhs_shape = self.getInst(rhs).shape;

        var actual_lhs = lhs;
        var actual_rhs = rhs;
        var result_shape = lhs_shape;

        // HLO requires explicit broadcasts — no implicit broadcasting.
        // Auto-broadcast the lower-rank operand to match the higher-rank one
        // (NumPy-style trailing-dimension alignment).
        if (lhs_shape.dimensions.len < rhs_shape.dimensions.len) {
            actual_lhs = try self.broadcastToMatch(lhs, lhs_shape, rhs_shape);
            result_shape = rhs_shape;
        } else if (rhs_shape.dimensions.len < lhs_shape.dimensions.len) {
            actual_rhs = try self.broadcastToMatch(rhs, rhs_shape, lhs_shape);
        }

        const ids = try self.dupeIds(&.{ actual_lhs, actual_rhs });
        return self.addInst(.{
            .id = self.nextId(),
            .opcode = opcode,
            .name = opcode,
            .shape = result_shape,
            .operand_ids = ids,
        });
    }

    /// Broadcast a lower-rank tensor to a higher-rank target shape.
    /// Aligns trailing dimensions (NumPy convention):
    ///   [N] → [B, N]:    broadcast_dimensions = {1}
    ///   [H, W] → [B, H, W]: broadcast_dimensions = {1, 2}
    ///   scalar → [B, N]:  broadcast_dimensions = {}
    fn broadcastToMatch(self: *Builder, id: Id, smaller: Shape, larger: Shape) !Id {
        if (smaller.dimensions.len == 0) {
            return self.broadcast(id, &.{}, larger);
        }
        if (shapeIsAllUnitDims(smaller)) {
            const scalar = try self.reshape(id, Shape.scalar(smaller.element_type));
            return self.broadcast(scalar, &.{}, larger);
        }
        var dims: [8]i64 = undefined;
        var used: [8]bool = @splat(false);
        const offset = larger.dimensions.len - smaller.dimensions.len;
        for (0..smaller.dimensions.len) |i| {
            const smaller_dim = smaller.dimensions[i];
            const trailing_dim = offset + i;
            if (trailing_dim < larger.dimensions.len and
                !used[trailing_dim] and
                larger.dimensions[trailing_dim] == smaller_dim)
            {
                dims[i] = @intCast(trailing_dim);
                used[trailing_dim] = true;
                continue;
            }

            var matched: ?usize = null;
            for (0..larger.dimensions.len) |larger_idx| {
                if (used[larger_idx]) continue;
                if (larger.dimensions[larger_idx] == smaller_dim) {
                    matched = larger_idx;
                    break;
                }
            }
            if (matched == null and smaller_dim == 1) {
                for (0..larger.dimensions.len) |larger_idx| {
                    if (used[larger_idx]) continue;
                    if (larger.dimensions[larger_idx] == 1) {
                        matched = larger_idx;
                        break;
                    }
                }
            }
            const larger_idx = matched orelse return error.UnsupportedShape;
            dims[i] = @intCast(larger_idx);
            used[larger_idx] = true;
        }
        return self.broadcast(id, dims[0..smaller.dimensions.len], larger);
    }

    fn shapeIsAllUnitDims(shape: Shape) bool {
        if (shape.dimensions.len == 0) return false;
        for (shape.dimensions) |dim| {
            if (dim != 1) return false;
        }
        return true;
    }

    pub fn add(self: *Builder, lhs: Id, rhs: Id) !Id {
        return self.binaryOp("add", lhs, rhs);
    }
    pub fn subtract(self: *Builder, lhs: Id, rhs: Id) !Id {
        return self.binaryOp("subtract", lhs, rhs);
    }
    pub fn multiply(self: *Builder, lhs: Id, rhs: Id) !Id {
        return self.binaryOp("multiply", lhs, rhs);
    }
    pub fn divide(self: *Builder, lhs: Id, rhs: Id) !Id {
        return self.binaryOp("divide", lhs, rhs);
    }
    pub fn maximum(self: *Builder, lhs: Id, rhs: Id) !Id {
        return self.binaryOp("maximum", lhs, rhs);
    }
    pub fn minimum(self: *Builder, lhs: Id, rhs: Id) !Id {
        return self.binaryOp("minimum", lhs, rhs);
    }
    pub fn power(self: *Builder, lhs: Id, rhs: Id) !Id {
        return self.binaryOp("power", lhs, rhs);
    }

    // ── Comparison ──────────────────────────────────────────────────

    pub fn compare(self: *Builder, lhs: Id, rhs: Id, direction: []const u8) !Id {
        const lhs_shape = self.getInst(lhs).shape;
        const ids = try self.dupeIds(&.{ lhs, rhs });
        return self.addInst(.{
            .id = self.nextId(),
            .opcode = "compare",
            .name = "compare",
            .shape = Shape.init(.pred, lhs_shape.dimensions),
            .operand_ids = ids,
            .comparison_direction = direction,
        });
    }

    pub fn lessThan(self: *Builder, lhs: Id, rhs: Id) !Id {
        return self.compare(lhs, rhs, "LT");
    }

    // ── Contraction ─────────────────────────────────────────────────

    pub fn dot(self: *Builder, lhs: Id, rhs: Id, result_shape: Shape, dot_dims: DotDimensionNumbers) !Id {
        const ids = try self.dupeIds(&.{ lhs, rhs });
        return self.addInst(.{
            .id = self.nextId(),
            .opcode = "dot",
            .name = "dot",
            .shape = result_shape,
            .operand_ids = ids,
            .dot_dimension_numbers = dot_dims,
        });
    }

    /// Standard matmul: [M, K] x [K, N] -> [M, N]. Contracts last dim of lhs
    /// with first dim (second-to-last) of rhs.
    pub fn matmul(self: *Builder, lhs: Id, rhs: Id) !Id {
        const lhs_shape = self.getInst(lhs).shape;
        const rhs_shape = self.getInst(rhs).shape;
        const lhs_rank = lhs_shape.dimensions.len;
        const rhs_rank = rhs_shape.dimensions.len;
        if (lhs_rank < 2 or rhs_rank < 2) return error.UnsupportedShape;
        if (lhs_rank > 8 or rhs_rank > 8) return error.UnsupportedShape;
        if (lhs_shape.dimensions[lhs_rank - 1] != rhs_shape.dimensions[rhs_rank - 2]) return error.UnsupportedShape;

        // Result shape: lhs dims except last, rhs dims except second-to-last
        var result_dims: [8]i64 = undefined;
        var ri: usize = 0;
        for (0..lhs_rank - 1) |i| {
            result_dims[ri] = lhs_shape.dimensions[i];
            ri += 1;
        }
        result_dims[ri] = rhs_shape.dimensions[rhs_rank - 1];
        ri += 1;

        return self.dot(lhs, rhs, Shape.init(lhs_shape.element_type, result_dims[0..ri]), .{
            .lhs_contracting_dimensions = &.{@as(i64, @intCast(lhs_rank - 1))},
            .rhs_contracting_dimensions = &.{@as(i64, @intCast(rhs_rank - 2))},
        });
    }

    // ── Reduction ───────────────────────────────────────────────────

    /// Reduce along `dimensions` using a sub-computation identified by
    /// `reducer_id`. The reducer must be a separate computation built and
    /// serialized into the module.
    pub fn reduce(self: *Builder, input: Id, init_value: Id, dimensions: []const i64, result_shape: Shape, reducer_computation_id: Id) !Id {
        const ids = try self.dupeIds(&.{ input, init_value });
        const dims = try self.dupeI64s(dimensions);
        const comp_ids = try self.dupeIds(&.{reducer_computation_id});
        return self.addInst(.{
            .id = self.nextId(),
            .opcode = "reduce",
            .name = "reduce",
            .shape = result_shape,
            .operand_ids = ids,
            .dimensions = dims,
            .called_computation_ids = comp_ids,
        });
    }

    // ── Shape manipulation ──────────────────────────────────────────

    pub fn reshape(self: *Builder, operand: Id, new_shape: Shape) !Id {
        const ids = try self.dupeIds(&.{operand});
        return self.addInst(.{
            .id = self.nextId(),
            .opcode = "reshape",
            .name = "reshape",
            .shape = new_shape,
            .operand_ids = ids,
        });
    }

    pub fn transpose(self: *Builder, operand: Id, permutation: []const i64, result_shape: Shape) !Id {
        const ids = try self.dupeIds(&.{operand});
        const dims = try self.dupeI64s(permutation);
        return self.addInst(.{
            .id = self.nextId(),
            .opcode = "transpose",
            .name = "transpose",
            .shape = result_shape,
            .operand_ids = ids,
            .dimensions = dims,
        });
    }

    pub fn broadcast(self: *Builder, operand: Id, broadcast_dimensions: []const i64, result_shape: Shape) !Id {
        const ids = try self.dupeIds(&.{operand});
        const dims = try self.dupeI64s(broadcast_dimensions);
        return self.addInst(.{
            .id = self.nextId(),
            .opcode = "broadcast",
            .name = "broadcast",
            .shape = result_shape,
            .operand_ids = ids,
            .dimensions = dims,
        });
    }

    pub fn slice(self: *Builder, operand: Id, starts: []const i64, limits: []const i64, strides: []const i64, result_shape: Shape) !Id {
        const rank = self.getInst(operand).shape.dimensions.len;
        if (starts.len != rank or limits.len != rank or strides.len != rank) return error.UnsupportedShape;
        if (rank > 8) return error.UnsupportedShape;

        var slice_dims: [8]xla.HloInstructionProto.SliceDimensions = undefined;
        for (0..rank) |i| {
            slice_dims[i] = .{
                .start = starts[i],
                .limit = limits[i],
                .stride = strides[i],
            };
        }

        const ids = try self.dupeIds(&.{operand});
        return self.addInst(.{
            .id = self.nextId(),
            .opcode = "slice",
            .name = "slice",
            .shape = result_shape,
            .operand_ids = ids,
            .slice_dimensions = slice_dims[0..rank],
        });
    }

    pub fn concatenate(self: *Builder, operands: []const Id, dimension: i64, result_shape: Shape) !Id {
        const ids = try self.dupeIds(operands);
        const dims = try self.dupeI64s(&.{dimension});
        return self.addInst(.{
            .id = self.nextId(),
            .opcode = "concatenate",
            .name = "concatenate",
            .shape = result_shape,
            .operand_ids = ids,
            .dimensions = dims,
        });
    }

    /// Create an HLO tuple packing multiple values into a single output.
    /// Returns the ID of the tuple instruction. When this is the last instruction
    /// added it becomes the computation root, and PJRT will return one buffer
    /// per element.
    pub fn tuple(self: *Builder, operand_ids: []const Id) !Id {
        // Collect the child shapes from the operand instructions.
        const child_shapes = try self.allocator.alloc(Shape, operand_ids.len);
        errdefer self.allocator.free(child_shapes);
        for (operand_ids, 0..) |id, i| {
            child_shapes[i] = self.getInst(id).shape;
        }
        const ids = try self.dupeIds(operand_ids);
        errdefer self.allocator.free(ids);
        const inst_id = self.nextId();
        // shape field is unused for the tuple instruction itself; the real shape
        // is serialized from tuple_child_shapes.  Use a scalar f32 placeholder.
        // Use addInst so the name is heap-allocated (deinit frees all names).
        return self.addInst(.{
            .id = inst_id,
            .opcode = "tuple",
            .name = "tuple",
            .shape = Shape.scalar(.f32),
            .operand_ids = ids,
            .is_tuple_root = true,
            .tuple_child_shapes = child_shapes,
        });
    }

    pub fn convertType(self: *Builder, operand: Id, target_type: ElementType) !Id {
        const op_shape = self.getInst(operand).shape;
        const ids = try self.dupeIds(&.{operand});
        return self.addInst(.{
            .id = self.nextId(),
            .opcode = "convert",
            .name = "convert",
            .shape = Shape.init(target_type, op_shape.dimensions),
            .operand_ids = ids,
        });
    }

    // ── Select ──────────────────────────────────────────────────────

    pub fn select(self: *Builder, pred: Id, on_true: Id, on_false: Id) !Id {
        const true_shape = self.getInst(on_true).shape;
        const ids = try self.dupeIds(&.{ pred, on_true, on_false });
        return self.addInst(.{
            .id = self.nextId(),
            .opcode = "select",
            .name = "select",
            .shape = true_shape,
            .operand_ids = ids,
        });
    }

    // ── Gather ──────────────────────────────────────────────────────

    pub fn gather(self: *Builder, operand: Id, indices: Id, result_shape: Shape) !Id {
        const ids = try self.dupeIds(&.{ operand, indices });
        const operand_shape = self.getInst(operand).shape;
        const indices_shape = self.getInst(indices).shape;
        const operand_rank = operand_shape.dimensions.len;
        const indices_rank = indices_shape.dimensions.len;
        if (operand_rank == 0) return error.UnsupportedShape;

        var offset_dims_buf: [8]i64 = undefined;
        if (operand_rank - 1 > offset_dims_buf.len) return error.UnsupportedShape;
        for (0..operand_rank - 1) |i| {
            offset_dims_buf[i] = @intCast(indices_rank + i);
        }

        var slice_sizes_buf: [8]i64 = undefined;
        if (operand_rank > slice_sizes_buf.len) return error.UnsupportedShape;
        slice_sizes_buf[0] = 1;
        for (1..operand_rank) |i| {
            slice_sizes_buf[i] = operand_shape.dimensions[i];
        }

        return self.addInst(.{
            .id = self.nextId(),
            .opcode = "gather",
            .name = "gather",
            .shape = result_shape,
            .operand_ids = ids,
            .gather_dimension_numbers = .{
                .offset_dims = offset_dims_buf[0 .. operand_rank - 1],
                .collapsed_slice_dims = &.{0},
                .start_index_map = &.{0},
                .index_vector_dim = @intCast(indices_rank),
            },
            .gather_slice_sizes = slice_sizes_buf[0..operand_rank],
        });
    }

    // ── Composite ops (decomposed into primitives) ──────────────────

    /// GELU approximation: 0.5 * x * (1 + tanh(sqrt(2/pi) * (x + 0.044715 * x^3)))
    pub fn gelu(self: *Builder, x: Id) !Id {
        const c_044715 = try self.constantScalarF32(0.044715);
        const c_sqrt2pi = try self.constantScalarF32(0.7978845608028654); // sqrt(2/pi)
        const c_half = try self.constantScalarF32(0.5);
        const c_one = try self.constantScalarF32(1.0);
        const c_three = try self.constantScalarF32(3.0);

        const x3 = try self.power(x, c_three);
        const inner = try self.add(x, try self.multiply(c_044715, x3));
        const scaled = try self.multiply(c_sqrt2pi, inner);
        const tanh_val = try self.tanh(scaled);
        const one_plus_tanh = try self.add(c_one, tanh_val);
        return self.multiply(c_half, try self.multiply(x, one_plus_tanh));
    }

    /// SiLU: x * sigmoid(x)
    pub fn silu(self: *Builder, x: Id) !Id {
        const sig = try self.logistic(x);
        return self.multiply(x, sig);
    }

    /// ReLU: max(x, 0)
    pub fn relu(self: *Builder, x: Id) !Id {
        const zero = try self.constantScalarF32(0.0);
        return self.maximum(x, zero);
    }

    /// Linear layer: matmul(x, w) + bias
    pub fn linear(self: *Builder, x: Id, weight: Id, bias: Id) !Id {
        const mm = try self.matmul(x, weight);
        return self.add(mm, bias);
    }

    // ── Build & serialize ───────────────────────────────────────────

    /// Build the computation as a Module ready for serialization.
    /// The last instruction is the root of the computation.
    pub fn build(self: *Builder) Computation {
        const root_id = if (self.instructions.items.len > 0)
            self.instructions.items[self.instructions.items.len - 1].id
        else
            1;
        // Computation ID = next_id (one past the last instruction ID)
        return .{
            .name = self.name,
            .instructions = self.instructions.items,
            .root_id = root_id,
            .id = self.next_id,
        };
    }
};

pub const Computation = struct {
    name: []const u8,
    instructions: []const Builder.Instruction,
    root_id: Id,
    id: Id,
};

pub const Module = struct {
    name: []const u8,
    entry: Computation,
    /// Additional computations (e.g., reduce bodies).
    auxiliary: []const Computation,

    pub fn init(name: []const u8, entry: Computation) Module {
        return .{ .name = name, .entry = entry, .auxiliary = &.{} };
    }

    pub fn initWithAux(name: []const u8, entry: Computation, aux: []const Computation) Module {
        return .{ .name = name, .entry = entry, .auxiliary = aux };
    }

    /// Serialize to HloModuleProto protobuf bytes. Caller owns the result.
    pub fn serialize(self: Module, alloc: Allocator) ![]u8 {
        var module_proto = try buildModuleProto(alloc, self);
        defer module_proto.deinit(alloc);
        return module_proto.encode(alloc);
    }
};

// ── Proto construction ─────────────────────────────────────────────────
//
// The helpers below translate the in-memory builder representation into
// owned `xla_proto` structs. Every slice/box they attach to the returned
// struct is freshly allocated via `alloc`, so `message.deinit` can clean
// the whole tree up without double-freeing anything that the Builder
// already owns.

fn toPrimitiveType(et: ElementType) xla.PrimitiveType {
    // ElementType wire values match PrimitiveType exactly.
    return @enumFromInt(@intFromEnum(et));
}

fn buildShapeProto(alloc: Allocator, shape: Shape) !xla.ShapeProto {
    var proto: xla.ShapeProto = .{};
    errdefer proto.deinit(alloc);

    proto.element_type = toPrimitiveType(shape.element_type);
    if (shape.dimensions.len > 0) {
        proto.dimensions = try alloc.dupe(i64, shape.dimensions);
    }

    // XLA requires a layout on every ShapeProto. Use row-major:
    //   minor_to_major = [rank-1, rank-2, ..., 0]
    const layout = try alloc.create(xla.LayoutProto);
    errdefer alloc.destroy(layout);
    layout.* = .{};
    errdefer layout.deinit(alloc);

    if (shape.dimensions.len > 0) {
        const rank = shape.dimensions.len;
        const minor_to_major = try alloc.alloc(i64, rank);
        for (0..rank) |i| {
            minor_to_major[i] = @intCast(rank - 1 - i);
        }
        layout.minor_to_major = minor_to_major;
    }
    layout.tail_padding_alignment_in_elements = 1;

    proto.layout = layout;
    return proto;
}

fn buildTupleShapeProto(alloc: Allocator, shapes: []const Shape) !xla.ShapeProto {
    var proto: xla.ShapeProto = .{};
    errdefer proto.deinit(alloc);

    proto.element_type = .tuple;
    const children = try alloc.alloc(xla.ShapeProto, shapes.len);
    var written: usize = 0;
    errdefer {
        for (children[0..written]) |*child| child.deinit(alloc);
        alloc.free(children);
    }
    for (shapes) |shape| {
        children[written] = try buildShapeProto(alloc, shape);
        written += 1;
    }
    proto.tuple_shapes = children;
    return proto;
}

fn buildInstructionShapeProto(alloc: Allocator, inst: Builder.Instruction) !xla.ShapeProto {
    if (inst.is_tuple_root) {
        return buildTupleShapeProto(alloc, inst.tuple_child_shapes orelse &.{});
    }
    return buildShapeProto(alloc, inst.shape);
}

fn buildLiteralProto(
    alloc: Allocator,
    shape: Shape,
    f32s: ?[]const f32,
    s32s: ?[]const i32,
) !xla.LiteralProto {
    var proto: xla.LiteralProto = .{};
    errdefer proto.deinit(alloc);

    proto.shape = try buildShapeProto(alloc, shape);
    if (f32s) |data| proto.f32s = try alloc.dupe(f32, data);
    if (s32s) |data| proto.s32s = try alloc.dupe(i32, data);
    return proto;
}

fn buildDotDimensionNumbers(
    alloc: Allocator,
    dot: DotDimensionNumbers,
) !xla.DotDimensionNumbers {
    var proto: xla.DotDimensionNumbers = .{};
    errdefer proto.deinit(alloc);

    if (dot.lhs_contracting_dimensions.len > 0)
        proto.lhs_contracting_dimensions = try alloc.dupe(i64, dot.lhs_contracting_dimensions);
    if (dot.rhs_contracting_dimensions.len > 0)
        proto.rhs_contracting_dimensions = try alloc.dupe(i64, dot.rhs_contracting_dimensions);
    if (dot.lhs_batch_dimensions.len > 0)
        proto.lhs_batch_dimensions = try alloc.dupe(i64, dot.lhs_batch_dimensions);
    if (dot.rhs_batch_dimensions.len > 0)
        proto.rhs_batch_dimensions = try alloc.dupe(i64, dot.rhs_batch_dimensions);
    return proto;
}

fn buildGatherDimensionNumbers(
    alloc: Allocator,
    gather: GatherDimensionNumbers,
) !xla.GatherDimensionNumbers {
    var proto: xla.GatherDimensionNumbers = .{};
    errdefer proto.deinit(alloc);

    if (gather.offset_dims.len > 0)
        proto.offset_dims = try alloc.dupe(i64, gather.offset_dims);
    if (gather.collapsed_slice_dims.len > 0)
        proto.collapsed_slice_dims = try alloc.dupe(i64, gather.collapsed_slice_dims);
    if (gather.start_index_map.len > 0)
        proto.start_index_map = try alloc.dupe(i64, gather.start_index_map);
    proto.index_vector_dim = gather.index_vector_dim;
    return proto;
}

fn idsToI64s(alloc: Allocator, ids: []const Id) ![]i64 {
    const out = try alloc.alloc(i64, ids.len);
    for (ids, 0..) |id, i| out[i] = @intCast(id);
    return out;
}

fn buildInstructionProto(alloc: Allocator, inst: Builder.Instruction) !xla.HloInstructionProto {
    var proto: xla.HloInstructionProto = .{};
    errdefer proto.deinit(alloc);

    // Strings are borrowed from the Builder, which owns them for the
    // lifetime of serialization. `message.deinit` treats `.string` fields
    // as borrowed and will not free them.
    proto.name = inst.name;
    proto.opcode = inst.opcode;
    proto.shape = try buildInstructionShapeProto(alloc, inst);
    proto.id = @intCast(inst.id);

    if (inst.operand_ids.len > 0) {
        proto.operand_ids = try idsToI64s(alloc, inst.operand_ids);
    }
    if (inst.dimensions) |dims| {
        proto.dimensions = try alloc.dupe(i64, dims);
    }
    if (inst.dot_dimension_numbers) |dot_dims| {
        proto.dot_dimension_numbers = try buildDotDimensionNumbers(alloc, dot_dims);
    }
    if (inst.gather_dimension_numbers) |gather_dims| {
        proto.gather_dimension_numbers = try buildGatherDimensionNumbers(alloc, gather_dims);
    }
    if (inst.gather_slice_sizes) |slice_sizes| {
        proto.gather_slice_sizes = try alloc.dupe(i64, slice_sizes);
    }
    if (inst.slice_dimensions) |slice_dims| {
        proto.slice_dimensions = try alloc.dupe(xla.HloInstructionProto.SliceDimensions, slice_dims);
    }
    if (inst.literal_f32s != null or inst.literal_s32s != null) {
        proto.literal = try buildLiteralProto(alloc, inst.shape, inst.literal_f32s, inst.literal_s32s);
    }
    if (inst.parameter_number) |pn| {
        proto.parameter_number = @intCast(pn);
    }
    if (inst.called_computation_ids) |comp_ids| {
        proto.called_computation_ids = try idsToI64s(alloc, comp_ids);
    }
    if (inst.comparison_direction) |dir| {
        proto.comparison_direction = dir;
    }

    return proto;
}

fn buildProgramShapeProto(alloc: Allocator, comp: Computation) !xla.ProgramShapeProto {
    var proto: xla.ProgramShapeProto = .{};
    errdefer proto.deinit(alloc);

    // Count parameters first so we can allocate exactly.
    var param_count: usize = 0;
    for (comp.instructions) |inst| {
        if (inst.parameter_number != null) param_count += 1;
    }

    if (param_count > 0) {
        const params = try alloc.alloc(xla.ShapeProto, param_count);
        var written: usize = 0;
        errdefer {
            for (params[0..written]) |*p| p.deinit(alloc);
            alloc.free(params);
        }
        const names = try alloc.alloc([]const u8, param_count);
        errdefer alloc.free(names);

        var names_written: usize = 0;
        for (comp.instructions) |inst| {
            if (inst.parameter_number == null) continue;
            params[written] = try buildShapeProto(alloc, inst.shape);
            written += 1;
            // Borrow: Builder owns inst.name for the lifetime of serialization.
            names[names_written] = inst.name;
            names_written += 1;
        }
        proto.parameters = params;
        proto.parameter_names = names;
    }

    if (comp.instructions.len > 0) {
        const root = comp.instructions[comp.instructions.len - 1];
        proto.result = try buildInstructionShapeProto(alloc, root);
    }

    return proto;
}

fn buildComputationProto(alloc: Allocator, comp: Computation) !xla.HloComputationProto {
    var proto: xla.HloComputationProto = .{};
    errdefer proto.deinit(alloc);

    // Borrow name; Builder owns it for the lifetime of serialization.
    proto.name = comp.name;
    proto.id = @intCast(comp.id);
    proto.root_id = @intCast(comp.root_id);

    if (comp.instructions.len > 0) {
        const insts = try alloc.alloc(xla.HloInstructionProto, comp.instructions.len);
        var written: usize = 0;
        errdefer {
            for (insts[0..written]) |*ins| ins.deinit(alloc);
            alloc.free(insts);
        }
        for (comp.instructions) |inst| {
            insts[written] = try buildInstructionProto(alloc, inst);
            written += 1;
        }
        proto.instructions = insts;
    }

    proto.program_shape = try buildProgramShapeProto(alloc, comp);
    return proto;
}

fn buildModuleProto(alloc: Allocator, module: Module) !xla.HloModuleProto {
    var proto: xla.HloModuleProto = .{};
    errdefer proto.deinit(alloc);

    // Borrow strings; Builder/Module own them for the lifetime of serialization.
    proto.name = module.name;
    proto.entry_computation_name = module.entry.name;
    proto.id = @intCast(module.entry.id);
    proto.entry_computation_id = @intCast(module.entry.id);

    // Auxiliary computations (reduce bodies etc.) come before the entry
    // so that PJRT sees the callee definitions before the caller.
    const total_comps = module.auxiliary.len + 1;
    const comps = try alloc.alloc(xla.HloComputationProto, total_comps);
    var written: usize = 0;
    errdefer {
        for (comps[0..written]) |*c| c.deinit(alloc);
        alloc.free(comps);
    }
    for (module.auxiliary) |aux| {
        comps[written] = try buildComputationProto(alloc, aux);
        written += 1;
    }
    comps[written] = try buildComputationProto(alloc, module.entry);
    written += 1;
    proto.computations = comps;

    proto.host_program_shape = try buildProgramShapeProto(alloc, module.entry);
    return proto;
}

// ── Tests ───────────────────────────────────────────────────────────

test "build and serialize simple add" {
    const alloc = std.testing.allocator;
    var b = Builder.init(alloc, "add_computation");
    defer b.deinit();

    const shape = Shape.init(.f32, &.{ 2, 3 });
    const p0 = try b.parameter(0, shape, "x");
    const p1 = try b.parameter(1, shape, "y");
    const result = try b.add(p0, p1);

    try std.testing.expectEqual(@as(Id, 1), p0);
    try std.testing.expectEqual(@as(Id, 2), p1);
    try std.testing.expectEqual(@as(Id, 3), result);

    const comp = b.build();
    try std.testing.expectEqual(@as(Id, 3), comp.root_id);
    try std.testing.expectEqual(@as(usize, 3), comp.instructions.len);

    const module = Module.init("test_module", comp);
    const bytes = try module.serialize(alloc);
    defer alloc.free(bytes);

    // Verify non-empty protobuf output
    try std.testing.expect(bytes.len > 0);

    // Round-trip through the generated HloModuleProto to verify top-level fields.
    var decoded = try xla.HloModuleProto.decode(alloc, bytes);
    defer decoded.deinit(alloc);

    try std.testing.expectEqualStrings("test_module", decoded.name);
    try std.testing.expectEqualStrings("add_computation", decoded.entry_computation_name);
    try std.testing.expectEqual(@as(usize, 1), decoded.computations.len);
    try std.testing.expectEqualStrings("add_computation", decoded.computations[0].name);
    try std.testing.expectEqual(@as(usize, 3), decoded.computations[0].instructions.len);
}

test "rank-one unit tensor broadcasts as scalar" {
    const alloc = std.testing.allocator;
    var b = Builder.init(alloc, "unit_broadcast");
    defer b.deinit();

    const x = try b.parameter(0, Shape.init(.f32, &.{ 2, 3 }), "x");
    const scale = try b.parameter(1, Shape.init(.f32, &.{1}), "scale");
    const y = try b.multiply(x, scale);

    try std.testing.expectEqual(@as(Id, 5), y);
    try std.testing.expectEqualStrings("reshape", b.getInst(3).opcode);
    try std.testing.expectEqualStrings("broadcast", b.getInst(4).opcode);
    try std.testing.expectEqual(@as(usize, 0), b.getInst(3).shape.dimensions.len);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, b.getInst(4).shape.dimensions);
}

test "matmul shape inference" {
    const alloc = std.testing.allocator;
    var b = Builder.init(alloc, "matmul_test");
    defer b.deinit();

    const lhs = try b.parameter(0, Shape.init(.f32, &.{ 4, 8 }), "lhs");
    const rhs = try b.parameter(1, Shape.init(.f32, &.{ 8, 16 }), "rhs");
    const result = try b.matmul(lhs, rhs);

    const result_shape = b.getInst(result).shape;
    try std.testing.expectEqual(@as(usize, 2), result_shape.dimensions.len);
    try std.testing.expectEqual(@as(i64, 4), result_shape.dimensions[0]);
    try std.testing.expectEqual(@as(i64, 16), result_shape.dimensions[1]);
}

test "matmul rejects low-rank operands" {
    const alloc = std.testing.allocator;
    var b = Builder.init(alloc, "matmul_rank_guard_test");
    defer b.deinit();

    const lhs = try b.parameter(0, Shape.init(.f32, &.{8}), "lhs");
    const rhs = try b.parameter(1, Shape.init(.f32, &.{ 8, 16 }), "rhs");
    try std.testing.expectError(error.UnsupportedShape, b.matmul(lhs, rhs));
}

test "gelu decomposition" {
    const alloc = std.testing.allocator;
    var b = Builder.init(alloc, "gelu_test");
    defer b.deinit();

    const x = try b.parameter(0, Shape.init(.f32, &.{4}), "x");
    const result = try b.gelu(x);

    // gelu decomposes into: 5 constants + power + multiply + add + multiply +
    // tanh + add + multiply + multiply = ~13 instructions total
    try std.testing.expect(b.instructions.items.len > 5);

    // Root should be the gelu result
    const comp = b.build();
    try std.testing.expectEqual(result, comp.root_id);

    // Should serialize without error
    const module = Module.init("gelu_module", comp);
    const bytes = try module.serialize(alloc);
    defer alloc.free(bytes);
    try std.testing.expect(bytes.len > 0);
}

test "linear decomposition" {
    const alloc = std.testing.allocator;
    var b = Builder.init(alloc, "linear_test");
    defer b.deinit();

    const x = try b.parameter(0, Shape.init(.f32, &.{ 2, 8 }), "x");
    const w = try b.parameter(1, Shape.init(.f32, &.{ 8, 16 }), "weight");
    const bias = try b.parameter(2, Shape.init(.f32, &.{ 2, 16 }), "bias");
    _ = try b.linear(x, w, bias);

    // linear = matmul + add = dot + add = 5 total (3 params + dot + add)
    try std.testing.expectEqual(@as(usize, 5), b.instructions.items.len);
}

test "constant scalar f32" {
    const alloc = std.testing.allocator;
    var b = Builder.init(alloc, "const_test");
    defer b.deinit();

    const c = try b.constantScalarF32(3.14);
    const inst = b.getInst(c).*;
    try std.testing.expectEqualStrings("constant", inst.opcode);
    try std.testing.expectEqual(@as(usize, 0), inst.shape.dimensions.len);
    try std.testing.expect(inst.literal_f32s != null);
    try std.testing.expectEqual(@as(f32, 3.14), inst.literal_f32s.?[0]);
}

test "silu decomposition" {
    const alloc = std.testing.allocator;
    var b = Builder.init(alloc, "silu_test");
    defer b.deinit();

    const x = try b.parameter(0, Shape.init(.f32, &.{ 2, 4 }), "x");
    _ = try b.silu(x);

    // silu = logistic + multiply = 3 total (param + logistic + multiply)
    try std.testing.expectEqual(@as(usize, 3), b.instructions.items.len);
}

test "relu decomposition" {
    const alloc = std.testing.allocator;
    var b = Builder.init(alloc, "relu_test");
    defer b.deinit();

    const x = try b.parameter(0, Shape.init(.f32, &.{8}), "x");
    const result = try b.relu(x);

    // relu = constant(0) + broadcast + maximum = 4 total.
    // HLO serialization requires the scalar zero to be explicitly broadcast.
    try std.testing.expectEqual(@as(usize, 4), b.instructions.items.len);
    try std.testing.expectEqualStrings("constant", b.instructions.items[1].opcode);
    try std.testing.expectEqualStrings("broadcast", b.instructions.items[2].opcode);
    try std.testing.expectEqualStrings("maximum", b.instructions.items[3].opcode);
    try std.testing.expectEqual(result, b.build().root_id);
}

test "rank-one size-one operands broadcast along matching unit dimension" {
    const alloc = std.testing.allocator;
    var b = Builder.init(alloc, "unit_broadcast_test");
    defer b.deinit();

    const x = try b.parameter(0, Shape.init(.f32, &.{ 1, 1536 }), "x");
    const y = try b.parameter(1, Shape.init(.f32, &.{1}), "row_scale");
    _ = try b.multiply(x, y);

    const broadcast = b.instructions.items[2];
    try std.testing.expectEqualStrings("broadcast", broadcast.opcode);
    try std.testing.expectEqual(@as(usize, 1), broadcast.dimensions.?.len);
    try std.testing.expectEqual(@as(i64, 0), broadcast.dimensions.?[0]);
}

test "dot with explicit dimension numbers" {
    const alloc = std.testing.allocator;
    var b = Builder.init(alloc, "dot_test");
    defer b.deinit();

    // Batched matmul: [B, M, K] x [B, K, N] -> [B, M, N]
    const lhs = try b.parameter(0, Shape.init(.f32, &.{ 2, 4, 8 }), "lhs");
    const rhs = try b.parameter(1, Shape.init(.f32, &.{ 2, 8, 16 }), "rhs");
    _ = try b.dot(lhs, rhs, Shape.init(.f32, &.{ 2, 4, 16 }), .{
        .lhs_contracting_dimensions = &.{2},
        .rhs_contracting_dimensions = &.{1},
        .lhs_batch_dimensions = &.{0},
        .rhs_batch_dimensions = &.{0},
    });

    const module = Module.init("batched_dot", b.build());
    const bytes = try module.serialize(alloc);
    defer alloc.free(bytes);
    try std.testing.expect(bytes.len > 0);
}
