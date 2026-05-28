//! Stub `xla_proto` module.
//!
//! The real `xla_proto` module used to be generated at build time from
//! `proto/hlo.desc` via `@import("protobuf").addProtoModule(...)`. That
//! helper was removed when the pinned `protobuf` library dropped its
//! build-time codegen support, and no checked-in generated file exists
//! yet. Rather than port the generator or stop compiling pjrt entirely
//! (pjrt is imported unconditionally by `lib/pjrt/src/root.zig` which is
//! in turn imported by `inference_mod`), we stub out just enough of the
//! `xla.*` surface that `src/hlo.zig` actually touches so the module
//! semantically analyses. Methods that serialize/deserialize return
//! empty buffers / errors at runtime — pjrt is disabled by default
//! (`-Dpjrt=false`), and when someone turns it on they'll need to
//! restore real generated bindings.
//!
//! Keep this file in lockstep with the field set used by `src/hlo.zig`.

const std = @import("std");
const wire = @import("protobuf").wire;
const Allocator = std.mem.Allocator;

pub const xla = struct {
    pub const PrimitiveType = enum(i32) {
        invalid = 0,
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
        tuple = 13,
        opaque_type = 14,
        c64 = 15,
        bf16 = 16,
        token = 17,
        c128 = 18,
        _,
    };

    pub const LayoutProto = struct {
        minor_to_major: []const i64 = &.{},
        tail_padding_alignment_in_elements: i64 = 0,

        pub fn deinit(self: *LayoutProto, alloc: Allocator) void {
            if (self.minor_to_major.len > 0) alloc.free(self.minor_to_major);
            self.* = .{};
        }
    };

    pub const ShapeProto = struct {
        element_type: PrimitiveType = .invalid,
        dimensions: []const i64 = &.{},
        tuple_shapes: []ShapeProto = &.{},
        layout: ?*LayoutProto = null,

        pub fn deinit(self: *ShapeProto, alloc: Allocator) void {
            if (self.dimensions.len > 0) alloc.free(self.dimensions);
            for (self.tuple_shapes) |*shape| shape.deinit(alloc);
            if (self.tuple_shapes.len > 0) alloc.free(self.tuple_shapes);
            if (self.layout) |l| {
                l.deinit(alloc);
                alloc.destroy(l);
            }
            self.* = .{};
        }
    };

    pub const LiteralProto = struct {
        shape: ShapeProto = .{},
        f32s: []const f32 = &.{},
        s32s: []const i32 = &.{},

        pub fn deinit(self: *LiteralProto, alloc: Allocator) void {
            self.shape.deinit(alloc);
            if (self.f32s.len > 0) alloc.free(self.f32s);
            if (self.s32s.len > 0) alloc.free(self.s32s);
            self.* = .{};
        }
    };

    pub const DotDimensionNumbers = struct {
        lhs_contracting_dimensions: []const i64 = &.{},
        rhs_contracting_dimensions: []const i64 = &.{},
        lhs_batch_dimensions: []const i64 = &.{},
        rhs_batch_dimensions: []const i64 = &.{},

        pub fn deinit(self: *DotDimensionNumbers, alloc: Allocator) void {
            if (self.lhs_contracting_dimensions.len > 0) alloc.free(self.lhs_contracting_dimensions);
            if (self.rhs_contracting_dimensions.len > 0) alloc.free(self.rhs_contracting_dimensions);
            if (self.lhs_batch_dimensions.len > 0) alloc.free(self.lhs_batch_dimensions);
            if (self.rhs_batch_dimensions.len > 0) alloc.free(self.rhs_batch_dimensions);
            self.* = .{};
        }
    };

    pub const GatherDimensionNumbers = struct {
        offset_dims: []const i64 = &.{},
        collapsed_slice_dims: []const i64 = &.{},
        start_index_map: []const i64 = &.{},
        index_vector_dim: i64 = 0,

        pub fn deinit(self: *GatherDimensionNumbers, alloc: Allocator) void {
            if (self.offset_dims.len > 0) alloc.free(self.offset_dims);
            if (self.collapsed_slice_dims.len > 0) alloc.free(self.collapsed_slice_dims);
            if (self.start_index_map.len > 0) alloc.free(self.start_index_map);
            self.* = .{};
        }
    };

    pub const HloInstructionProto = struct {
        pub const SliceDimensions = struct {
            start: i64 = 0,
            limit: i64 = 0,
            stride: i64 = 1,
        };

        name: []const u8 = "",
        opcode: []const u8 = "",
        shape: ShapeProto = .{},
        id: i64 = 0,
        operand_ids: []const i64 = &.{},
        dimensions: []const i64 = &.{},
        slice_dimensions: []const SliceDimensions = &.{},
        dot_dimension_numbers: ?DotDimensionNumbers = null,
        gather_dimension_numbers: ?GatherDimensionNumbers = null,
        gather_slice_sizes: []const i64 = &.{},
        literal: ?LiteralProto = null,
        parameter_number: i64 = 0,
        called_computation_ids: []const i64 = &.{},
        comparison_direction: []const u8 = "",

        pub fn deinit(self: *HloInstructionProto, alloc: Allocator) void {
            self.shape.deinit(alloc);
            if (self.operand_ids.len > 0) alloc.free(self.operand_ids);
            if (self.dimensions.len > 0) alloc.free(self.dimensions);
            if (self.slice_dimensions.len > 0) alloc.free(self.slice_dimensions);
            if (self.dot_dimension_numbers) |*d| d.deinit(alloc);
            if (self.gather_dimension_numbers) |*g| g.deinit(alloc);
            if (self.gather_slice_sizes.len > 0) alloc.free(self.gather_slice_sizes);
            if (self.literal) |*l| l.deinit(alloc);
            if (self.called_computation_ids.len > 0) alloc.free(self.called_computation_ids);
            self.* = .{};
        }
    };

    pub const ProgramShapeProto = struct {
        parameters: []ShapeProto = &.{},
        parameter_names: []const []const u8 = &.{},
        result: ShapeProto = .{},

        pub fn deinit(self: *ProgramShapeProto, alloc: Allocator) void {
            for (self.parameters) |*p| p.deinit(alloc);
            if (self.parameters.len > 0) alloc.free(self.parameters);
            if (self.parameter_names.len > 0) alloc.free(self.parameter_names);
            self.result.deinit(alloc);
            self.* = .{};
        }
    };

    pub const HloComputationProto = struct {
        name: []const u8 = "",
        id: i64 = 0,
        root_id: i64 = 0,
        instructions: []HloInstructionProto = &.{},
        program_shape: ProgramShapeProto = .{},

        pub fn deinit(self: *HloComputationProto, alloc: Allocator) void {
            for (self.instructions) |*i| i.deinit(alloc);
            if (self.instructions.len > 0) alloc.free(self.instructions);
            self.program_shape.deinit(alloc);
            self.* = .{};
        }
    };

    pub const HloModuleProto = struct {
        name: []const u8 = "",
        entry_computation_name: []const u8 = "",
        id: i64 = 0,
        entry_computation_id: i64 = 0,
        computations: []HloComputationProto = &.{},
        host_program_shape: ProgramShapeProto = .{},

        pub fn deinit(self: *HloModuleProto, alloc: Allocator) void {
            for (self.computations) |*c| c.deinit(alloc);
            if (self.computations.len > 0) alloc.free(self.computations);
            self.host_program_shape.deinit(alloc);
            self.* = .{};
        }

        pub fn encode(self: HloModuleProto, alloc: Allocator) ![]u8 {
            var buf: wire.Buf = .empty;
            errdefer buf.deinit(alloc);
            try encodeHloModuleProto(alloc, &buf, self);
            return try buf.toOwnedSlice(alloc);
        }

        pub fn decode(alloc: Allocator, bytes: []const u8) !HloModuleProto {
            return decodeHloModuleProto(alloc, bytes);
        }
    };
};

fn writeInt64(alloc: Allocator, buf: *wire.Buf, field: u32, value: i64) !void {
    try wire.writeTag(alloc, buf, field, .varint);
    try wire.writeVarint(alloc, buf, @bitCast(value));
}

fn writeInt32(alloc: Allocator, buf: *wire.Buf, field: u32, value: i32) !void {
    try wire.writeTag(alloc, buf, field, .varint);
    try wire.writeVarint(alloc, buf, @as(u64, @bitCast(@as(i64, value))));
}

fn writeEnum(alloc: Allocator, buf: *wire.Buf, field: u32, value: anytype) !void {
    try writeInt32(alloc, buf, field, @intFromEnum(value));
}

fn writePackedInt32s(alloc: Allocator, buf: *wire.Buf, field: u32, values: []const i32) !void {
    if (values.len == 0) return;
    var payload_len: usize = 0;
    for (values) |v| payload_len += wire.varintSize(@as(u64, @bitCast(@as(i64, v))));
    try wire.writeTag(alloc, buf, field, .length_delimited);
    try wire.writeVarint(alloc, buf, payload_len);
    for (values) |v| try wire.writeVarint(alloc, buf, @as(u64, @bitCast(@as(i64, v))));
}

fn encodeMessage(alloc: Allocator, parent: *wire.Buf, field: u32, value: anytype, encodeFn: anytype) anyerror!void {
    var child: wire.Buf = .empty;
    defer child.deinit(alloc);
    try encodeFn(alloc, &child, value);
    try wire.writeMessage(alloc, parent, field, child.items);
}

fn encodeLayoutProto(alloc: Allocator, buf: *wire.Buf, value: xla.LayoutProto) !void {
    if (value.minor_to_major.len > 0) try wire.writePackedInt64s(alloc, buf, 1, value.minor_to_major);
    if (value.tail_padding_alignment_in_elements != 0) {
        try writeInt64(alloc, buf, 16, value.tail_padding_alignment_in_elements);
    }
}

fn encodeShapeProto(alloc: Allocator, buf: *wire.Buf, value: xla.ShapeProto) !void {
    if (value.element_type != .invalid) try writeEnum(alloc, buf, 2, value.element_type);
    if (value.dimensions.len > 0) try wire.writePackedInt64s(alloc, buf, 3, value.dimensions);
    for (value.tuple_shapes) |shape| try encodeMessage(alloc, buf, 4, shape, encodeShapeProto);
    if (value.layout) |layout| try encodeMessage(alloc, buf, 5, layout.*, encodeLayoutProto);
}

fn encodeLiteralProto(alloc: Allocator, buf: *wire.Buf, value: xla.LiteralProto) !void {
    try encodeMessage(alloc, buf, 1, value.shape, encodeShapeProto);
    if (value.s32s.len > 0) try writePackedInt32s(alloc, buf, 4, value.s32s);
    if (value.f32s.len > 0) try wire.writePackedFloats(alloc, buf, 8, value.f32s);
}

fn encodeDotDimensionNumbers(alloc: Allocator, buf: *wire.Buf, value: xla.DotDimensionNumbers) !void {
    if (value.lhs_contracting_dimensions.len > 0) try wire.writePackedInt64s(alloc, buf, 1, value.lhs_contracting_dimensions);
    if (value.rhs_contracting_dimensions.len > 0) try wire.writePackedInt64s(alloc, buf, 2, value.rhs_contracting_dimensions);
    if (value.lhs_batch_dimensions.len > 0) try wire.writePackedInt64s(alloc, buf, 3, value.lhs_batch_dimensions);
    if (value.rhs_batch_dimensions.len > 0) try wire.writePackedInt64s(alloc, buf, 4, value.rhs_batch_dimensions);
}

fn encodeGatherDimensionNumbers(alloc: Allocator, buf: *wire.Buf, value: xla.GatherDimensionNumbers) !void {
    if (value.offset_dims.len > 0) try wire.writePackedInt64s(alloc, buf, 1, value.offset_dims);
    if (value.collapsed_slice_dims.len > 0) try wire.writePackedInt64s(alloc, buf, 2, value.collapsed_slice_dims);
    if (value.start_index_map.len > 0) try wire.writePackedInt64s(alloc, buf, 3, value.start_index_map);
    try writeInt64(alloc, buf, 4, value.index_vector_dim);
}

fn encodeSliceDimensions(alloc: Allocator, buf: *wire.Buf, value: xla.HloInstructionProto.SliceDimensions) !void {
    try writeInt64(alloc, buf, 1, value.start);
    try writeInt64(alloc, buf, 2, value.limit);
    try writeInt64(alloc, buf, 3, value.stride);
}

fn encodeHloInstructionProto(alloc: Allocator, buf: *wire.Buf, value: xla.HloInstructionProto) !void {
    if (value.name.len > 0) try wire.writeString(alloc, buf, 1, value.name);
    if (value.opcode.len > 0) try wire.writeString(alloc, buf, 2, value.opcode);
    try encodeMessage(alloc, buf, 3, value.shape, encodeShapeProto);
    if (value.literal) |literal| try encodeMessage(alloc, buf, 8, literal, encodeLiteralProto);
    if (value.parameter_number != 0) try writeInt64(alloc, buf, 9, value.parameter_number);
    if (value.dimensions.len > 0) try wire.writePackedInt64s(alloc, buf, 14, value.dimensions);
    for (value.slice_dimensions) |dim| try encodeMessage(alloc, buf, 17, dim, encodeSliceDimensions);
    if (value.dot_dimension_numbers) |dot| try encodeMessage(alloc, buf, 30, dot, encodeDotDimensionNumbers);
    if (value.gather_dimension_numbers) |gather| try encodeMessage(alloc, buf, 33, gather, encodeGatherDimensionNumbers);
    if (value.gather_slice_sizes.len > 0) try wire.writePackedInt64s(alloc, buf, 34, value.gather_slice_sizes);
    if (value.id != 0) try writeInt64(alloc, buf, 35, value.id);
    if (value.operand_ids.len > 0) try wire.writePackedInt64s(alloc, buf, 36, value.operand_ids);
    if (value.called_computation_ids.len > 0) try wire.writePackedInt64s(alloc, buf, 38, value.called_computation_ids);
    if (value.comparison_direction.len > 0) try wire.writeString(alloc, buf, 63, value.comparison_direction);
}

fn encodeProgramShapeProto(alloc: Allocator, buf: *wire.Buf, value: xla.ProgramShapeProto) !void {
    for (value.parameters) |param| try encodeMessage(alloc, buf, 1, param, encodeShapeProto);
    try encodeMessage(alloc, buf, 2, value.result, encodeShapeProto);
    for (value.parameter_names) |name| try wire.writeString(alloc, buf, 3, name);
}

fn encodeHloComputationProto(alloc: Allocator, buf: *wire.Buf, value: xla.HloComputationProto) !void {
    if (value.name.len > 0) try wire.writeString(alloc, buf, 1, value.name);
    for (value.instructions) |inst| try encodeMessage(alloc, buf, 2, inst, encodeHloInstructionProto);
    try encodeMessage(alloc, buf, 4, value.program_shape, encodeProgramShapeProto);
    if (value.id != 0) try writeInt64(alloc, buf, 5, value.id);
    if (value.root_id != 0) try writeInt64(alloc, buf, 6, value.root_id);
}

fn encodeHloModuleProto(alloc: Allocator, buf: *wire.Buf, value: xla.HloModuleProto) !void {
    if (value.name.len > 0) try wire.writeString(alloc, buf, 1, value.name);
    if (value.entry_computation_name.len > 0) try wire.writeString(alloc, buf, 2, value.entry_computation_name);
    for (value.computations) |comp| try encodeMessage(alloc, buf, 3, comp, encodeHloComputationProto);
    try encodeMessage(alloc, buf, 4, value.host_program_shape, encodeProgramShapeProto);
    if (value.id != 0) try writeInt64(alloc, buf, 5, value.id);
    if (value.entry_computation_id != 0) try writeInt64(alloc, buf, 6, value.entry_computation_id);
}

fn decodeHloModuleProto(alloc: Allocator, bytes: []const u8) !xla.HloModuleProto {
    var out: xla.HloModuleProto = .{};
    errdefer out.deinit(alloc);
    var computations = std.ArrayListUnmanaged(xla.HloComputationProto).empty;
    defer computations.deinit(alloc);

    var pos: usize = 0;
    while (pos < bytes.len) {
        const tag = try wire.readTag(bytes, &pos);
        switch (tag.field) {
            1 => out.name = try wire.readLengthDelimited(bytes, &pos),
            2 => out.entry_computation_name = try wire.readLengthDelimited(bytes, &pos),
            3 => {
                const payload = try wire.readLengthDelimited(bytes, &pos);
                try computations.append(alloc, try decodeHloComputationProto(alloc, payload));
            },
            4 => try wire.skipField(bytes, &pos, tag.wire_type),
            5 => out.id = @bitCast(try wire.readVarint(bytes, &pos)),
            6 => out.entry_computation_id = @bitCast(try wire.readVarint(bytes, &pos)),
            else => try wire.skipField(bytes, &pos, tag.wire_type),
        }
    }
    out.computations = try computations.toOwnedSlice(alloc);
    return out;
}

fn decodeHloComputationProto(alloc: Allocator, bytes: []const u8) !xla.HloComputationProto {
    var out: xla.HloComputationProto = .{};
    errdefer out.deinit(alloc);
    var instructions = std.ArrayListUnmanaged(xla.HloInstructionProto).empty;
    defer instructions.deinit(alloc);

    var pos: usize = 0;
    while (pos < bytes.len) {
        const tag = try wire.readTag(bytes, &pos);
        switch (tag.field) {
            1 => out.name = try wire.readLengthDelimited(bytes, &pos),
            2 => {
                const payload = try wire.readLengthDelimited(bytes, &pos);
                _ = payload;
                try instructions.append(alloc, .{});
            },
            4 => try wire.skipField(bytes, &pos, tag.wire_type),
            5 => out.id = @bitCast(try wire.readVarint(bytes, &pos)),
            6 => out.root_id = @bitCast(try wire.readVarint(bytes, &pos)),
            else => try wire.skipField(bytes, &pos, tag.wire_type),
        }
    }
    out.instructions = try instructions.toOwnedSlice(alloc);
    return out;
}
