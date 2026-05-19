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
const node_mod = @import("node.zig");
const shape_mod = @import("shape.zig");

const Node = node_mod.Node;
const NodeId = node_mod.NodeId;
const OpCode = node_mod.OpCode;
const null_node = node_mod.null_node;
const Shape = shape_mod.Shape;
const DType = shape_mod.DType;
const ConstantCache = shape_mod.ConstantCache;

/// Maximum element-size alignment we need to satisfy. f64 / i64 are the
/// widest currently-supported dtypes (8 bytes); the backing allocation and
/// every interned constant offset are both aligned to this value.
const constant_pool_alignment = std.mem.Alignment.@"8";
const constant_pool_alignment_bytes = constant_pool_alignment.toByteUnits();

/// Append-only computation DAG. Nodes are stored in topological order
/// (every node's inputs have a lower index than the node itself).
pub const Graph = struct {
    allocator: std.mem.Allocator,
    nodes: std.ArrayListUnmanaged(Node),

    /// Constant data pool, stored as raw bytes so it can hold any dtype
    /// (f32, f16, i32, i64, bool, …). `ConstantAttrs.data_offset` is a
    /// byte offset into this buffer; `ConstantAttrs.data_len` is an
    /// element count — the dtype lives on the consuming node's
    /// `output_shape.dtype`. The backing allocation and each interned
    /// constant offset are aligned to `constant_pool_alignment`, so reading
    /// back as the appropriate typed slice is sound.
    constant_pool: std.ArrayListAlignedUnmanaged(u8, constant_pool_alignment),

    /// String table for parameter names, referenced by ParameterAttrs.
    string_table: std.ArrayListUnmanaged(u8),

    /// Graph output node IDs.
    outputs: std.ArrayListUnmanaged(NodeId),

    /// Parameter node IDs in definition order (for binding at execution time).
    parameters: std.ArrayListUnmanaged(NodeId),

    /// Deduplicates scalar constants.
    constant_cache: ConstantCache,

    pub fn init(allocator: std.mem.Allocator) Graph {
        return .{
            .allocator = allocator,
            .nodes = .empty,
            .constant_pool = .empty,
            .string_table = .empty,
            .outputs = .empty,
            .parameters = .empty,
            .constant_cache = ConstantCache.init(),
        };
    }

    pub fn deinit(self: *Graph) void {
        self.nodes.deinit(self.allocator);
        self.constant_pool.deinit(self.allocator);
        self.string_table.deinit(self.allocator);
        self.outputs.deinit(self.allocator);
        self.parameters.deinit(self.allocator);
        self.constant_cache.deinit(self.allocator);
    }

    /// Append a node and return its ID (index).
    pub fn addNode(self: *Graph, n: Node) !NodeId {
        const id: NodeId = @intCast(self.nodes.items.len);
        try self.nodes.append(self.allocator, n);
        return id;
    }

    pub fn node(self: *const Graph, id: NodeId) *const Node {
        return &self.nodes.items[id];
    }

    pub fn nodeMut(self: *Graph, id: NodeId) *Node {
        return &self.nodes.items[id];
    }

    pub fn nodeCount(self: *const Graph) u32 {
        return @intCast(self.nodes.items.len);
    }

    /// Mark a node as a graph output.
    pub fn markOutput(self: *Graph, id: NodeId) !void {
        try self.outputs.append(self.allocator, id);
    }

    /// Retrieve the name of a parameter node from the string table.
    pub fn parameterName(self: *const Graph, n: *const Node) []const u8 {
        const attrs = n.op.parameter;
        return self.string_table.items[attrs.name_offset..][0..attrs.name_len];
    }

    /// Store a string in the string table, returning offset and length.
    pub fn internString(self: *Graph, s: []const u8) !struct { offset: u32, len: u16 } {
        const offset: u32 = @intCast(self.string_table.items.len);
        try self.string_table.appendSlice(self.allocator, s);
        return .{ .offset = offset, .len = @intCast(s.len) };
    }

    /// Pad the pool up to `constant_pool_alignment` so the next interned
    /// constant starts at an address compatible with any supported dtype.
    fn alignConstantPool(self: *Graph) !void {
        const cur = self.constant_pool.items.len;
        const padded = std.mem.alignForward(usize, cur, constant_pool_alignment_bytes);
        if (padded > cur) {
            try self.constant_pool.appendNTimes(self.allocator, 0, padded - cur);
        }
    }

    /// Result of an `internConstant*` call: byte offset into
    /// `constant_pool` and number of elements stored.
    pub const ConstantLoc = struct { offset: u32, len: u32 };

    /// Intern a single scalar value into the pool, encoded in the
    /// requested dtype's native byte layout. The caller passes the
    /// value as `f64` (it can losslessly hold any of the dtypes the
    /// IR currently supports), and we narrow it on the way in.
    pub fn internScalarConst(self: *Graph, value: f64, dtype: DType) !ConstantLoc {
        var buf: [8]u8 = undefined;
        const bytes = encodeScalar(value, dtype, &buf);
        return self.internConstantBytes(bytes, dtype);
    }

    /// Store constant f32 data. Convenience shortcut for the very
    /// common f32 case; equivalent to
    /// `internConstantBytes(sliceAsBytes(data), .f32)`.
    pub fn internConstant(self: *Graph, data: []const f32) !ConstantLoc {
        return self.internConstantBytes(std.mem.sliceAsBytes(data), .f32);
    }

    /// Generic intern. Stores raw bytes for any supported dtype after
    /// aligning the pool. The dtype is taken at runtime so a single
    /// function covers every element type without a comptime
    /// dispatch. The byte length must be an exact multiple of the
    /// dtype's element size; the returned `len` is the element count.
    pub fn internConstantBytes(self: *Graph, bytes: []const u8, dtype: DType) !ConstantLoc {
        const elem_size = dtype.byteSize();
        std.debug.assert(elem_size > 0);
        std.debug.assert(bytes.len % elem_size == 0);
        try self.alignConstantPool();
        const offset: u32 = @intCast(self.constant_pool.items.len);
        try self.constant_pool.appendSlice(self.allocator, bytes);
        return .{ .offset = offset, .len = @intCast(@divExact(bytes.len, elem_size)) };
    }

    /// Read constant data back as `[]const f32`. Equivalent to the
    /// dtype-aware `constantDataAs(f32, …)` — kept as the canonical
    /// f32 entry point.
    pub fn constantData(self: *const Graph, offset: u32, len: u32) []const f32 {
        return self.constantDataAs(f32, offset, len);
    }

    pub const ConstantF32View = struct {
        data: []const f32,
        owned: ?[]f32 = null,

        pub fn deinit(self: ConstantF32View, allocator: std.mem.Allocator) void {
            if (self.owned) |owned| allocator.free(owned);
        }
    };

    /// Decode a constant of any graph dtype into f32 values. This is for
    /// backend boundaries that still model eager tensors as f32 buffers; it
    /// preserves numeric values instead of reinterpreting raw bytes.
    pub fn constantDataAsF32(
        self: *const Graph,
        allocator: std.mem.Allocator,
        dtype: DType,
        offset: u32,
        len: u32,
    ) !ConstantF32View {
        if (dtype == .f32) return .{ .data = self.constantData(offset, len) };

        const n: usize = @intCast(len);
        const out = try allocator.alloc(f32, n);
        errdefer allocator.free(out);
        switch (dtype) {
            .f32 => unreachable,
            .f16 => {
                const src = self.constantDataAs(f16, offset, len);
                for (src, out) |v, *dst| dst.* = @floatCast(v);
            },
            .bf16 => {
                const src = self.constantDataAs(u16, offset, len);
                for (src, out) |v, *dst| {
                    const bits: u32 = @as(u32, v) << 16;
                    dst.* = @bitCast(bits);
                }
            },
            .f64 => {
                const src = self.constantDataAs(f64, offset, len);
                for (src, out) |v, *dst| dst.* = @floatCast(v);
            },
            .i8 => {
                const src = self.constantDataAs(i8, offset, len);
                for (src, out) |v, *dst| dst.* = @floatFromInt(v);
            },
            .i16 => {
                const src = self.constantDataAs(i16, offset, len);
                for (src, out) |v, *dst| dst.* = @floatFromInt(v);
            },
            .i32 => {
                const src = self.constantDataAs(i32, offset, len);
                for (src, out) |v, *dst| dst.* = @floatFromInt(v);
            },
            .i64 => {
                const src = self.constantDataAs(i64, offset, len);
                for (src, out) |v, *dst| dst.* = @floatFromInt(v);
            },
            .u8 => {
                const src = self.constantDataAs(u8, offset, len);
                for (src, out) |v, *dst| dst.* = @floatFromInt(v);
            },
            .bool_ => {
                const src = self.constantDataAs(u8, offset, len);
                for (src, out) |v, *dst| dst.* = if (v == 0) 0.0 else 1.0;
            },
        }
        return .{ .data = out, .owned = out };
    }

    /// Typed read of an interned constant. The caller must pass the
    /// correct element type — the dtype isn't recorded in the pool
    /// itself; it lives on the consuming node's `output_shape.dtype`.
    pub fn constantDataAs(self: *const Graph, comptime T: type, offset: u32, len: u32) []const T {
        const bytes = self.constant_pool.items[offset..][0 .. len * @sizeOf(T)];
        const aligned: [*]align(constant_pool_alignment_bytes) const u8 = @alignCast(bytes.ptr);
        return @as([*]const T, @ptrCast(aligned))[0..len];
    }

    /// Raw byte view of an interned constant. Useful for op-codes that
    /// want to inspect bits regardless of dtype (e.g. for hashing /
    /// equality).
    pub fn constantBytes(self: *const Graph, offset: u32, len_bytes: u32) []const u8 {
        return self.constant_pool.items[offset..][0..len_bytes];
    }
};

/// Write a single `value` into `buf` using the byte layout of `dtype`,
/// returning the slice of `buf` actually used. Handles every dtype the
/// IR knows about, including bf16 (encoded as the top 16 bits of the
/// f32 representation).
pub fn encodeScalar(value: f64, dtype: DType, buf: *[8]u8) []const u8 {
    switch (dtype) {
        .f32 => {
            const v: f32 = @floatCast(value);
            const bytes = std.mem.toBytes(v);
            @memcpy(buf[0..4], &bytes);
            return buf[0..4];
        },
        .f64 => {
            const bytes = std.mem.toBytes(value);
            @memcpy(buf[0..8], &bytes);
            return buf[0..8];
        },
        .f16 => {
            const v: f16 = @floatCast(value);
            const bytes = std.mem.toBytes(v);
            @memcpy(buf[0..2], &bytes);
            return buf[0..2];
        },
        .bf16 => {
            // bf16 = top 16 bits of f32. Round to nearest even by
            // adding 0x8000 + last-bit-of-result before truncation
            // (the IEEE round-to-nearest-even bias).
            const f32_val: f32 = @floatCast(value);
            const bits: u32 = @bitCast(f32_val);
            const lsb: u32 = (bits >> 16) & 1;
            const rounded = bits +% 0x7FFF +% lsb;
            const top: u16 = @truncate(rounded >> 16);
            const bytes = std.mem.toBytes(top);
            @memcpy(buf[0..2], &bytes);
            return buf[0..2];
        },
        .i8 => {
            buf[0] = @bitCast(@as(i8, @intFromFloat(value)));
            return buf[0..1];
        },
        .i16 => {
            const v: i16 = @intFromFloat(value);
            const bytes = std.mem.toBytes(v);
            @memcpy(buf[0..2], &bytes);
            return buf[0..2];
        },
        .i32 => {
            const v: i32 = @intFromFloat(value);
            const bytes = std.mem.toBytes(v);
            @memcpy(buf[0..4], &bytes);
            return buf[0..4];
        },
        .i64 => {
            const v: i64 = @intFromFloat(value);
            const bytes = std.mem.toBytes(v);
            @memcpy(buf[0..8], &bytes);
            return buf[0..8];
        },
        .u8 => {
            buf[0] = @intFromFloat(value);
            return buf[0..1];
        },
        .bool_ => {
            buf[0] = if (value != 0.0) 1 else 0;
            return buf[0..1];
        },
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

test "Graph basic operations" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();

    // Add a parameter
    const str = try g.internString("weight");
    const p = try g.addNode(.{
        .op = .{ .parameter = .{ .name_offset = str.offset, .name_len = str.len } },
        .output_shape = Shape.init(.f32, &.{ 4, 3 }),
    });
    try g.parameters.append(allocator, p);

    // Add a unary op
    const n = try g.addNode(.{
        .op = .{ .neg = {} },
        .output_shape = Shape.init(.f32, &.{ 4, 3 }),
        .inputs = .{ p, null_node, null_node, null_node },
        .num_inputs = 1,
    });

    try g.markOutput(n);

    try std.testing.expectEqual(@as(u32, 2), g.nodeCount());
    try std.testing.expectEqual(@as(usize, 1), g.outputs.items.len);
    try std.testing.expectEqual(n, g.outputs.items[0]);
    try std.testing.expectEqualStrings("weight", g.parameterName(g.node(p)));
}

test "Graph constant pool" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();

    const data = [_]f32{ 1.0, 2.0, 3.0 };
    const loc = try g.internConstant(&data);
    const read_back = g.constantData(loc.offset, loc.len);
    try std.testing.expectEqualSlices(f32, &data, read_back);
}

test "Graph constant pool preserves typed alignment" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();

    _ = try g.internConstantBytes(&.{1}, .u8);
    const values = [_]i64{ 11, 22 };
    const loc = try g.internConstantBytes(std.mem.sliceAsBytes(&values), .i64);

    try std.testing.expectEqual(@as(u32, 8), loc.offset);
    const read_back = g.constantDataAs(i64, loc.offset, loc.len);
    try std.testing.expectEqual(@as(usize, 0), @intFromPtr(read_back.ptr) % @alignOf(i64));
    try std.testing.expectEqualSlices(i64, &values, read_back);
}

test "Graph constant cache deduplication" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();

    // First scalar constant
    const id1 = try g.addNode(.{
        .op = .{ .constant = .{ .data_offset = 0, .data_len = 1 } },
        .output_shape = Shape.scalar(.f32),
    });
    try g.constant_cache.putScalar(allocator, .f32, 1.0, id1);

    // Second request for same scalar should hit cache
    const cached = g.constant_cache.getScalar(.f32, 1.0);
    try std.testing.expectEqual(@as(?u32, id1), cached);

    // Different value should miss
    try std.testing.expectEqual(@as(?u32, null), g.constant_cache.getScalar(.f32, 2.0));
}
