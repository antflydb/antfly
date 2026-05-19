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

// ONNX protobuf message types driven by the comptime message runtime.
//
// Struct shapes match the fields we actually use from `onnx-ml.proto` (we
// don't try to be exhaustive). Each message declares a `_pb_field_map` that
// tells `lib/protobuf/src/message.zig` how to encode/decode each field; the
// runtime handles all the wire-format boilerplate.
//
// Zero-copy / lazy behaviour is preserved via encoding overrides:
//   - `TensorProto.{float,int32,int64,double}_data` use `packed_raw_*` so the
//     decoded slice borrows directly from the input buffer (no parallel
//     [][]f32 tree for multi-GB weight tensors).
//   - `LazyGraphProto.initializer_bytes` uses `lazy_repeated_submessage` so
//     each initializer is stored as its raw encoded payload until the caller
//     explicitly decodes it via `parseInitializer(i)`.

const std = @import("std");
const message = @import("protobuf").message;
const FieldDesc = message.FieldDesc;

// ── ONNX Data Types ──────────────────────────────────────────────────

pub const DataType = enum(u32) {
    undefined = 0,
    float32 = 1,
    uint8 = 2,
    int8 = 3,
    uint16 = 4,
    int16 = 5,
    int32 = 6,
    int64 = 7,
    string = 8,
    bool_ = 9,
    float16 = 10,
    float64 = 11,
    uint32 = 12,
    uint64 = 13,
    complex64 = 14,
    complex128 = 15,
    bfloat16 = 16,
    _,
};

pub const AttributeType = enum(u32) {
    undefined = 0,
    float = 1,
    int = 2,
    string = 3,
    tensor = 4,
    graph = 5,
    floats = 6,
    ints = 7,
    strings = 8,
    tensors = 9,
    graphs = 10,
    _,
};

pub const DataLocation = enum(u8) {
    default = 0,
    external = 1,
};

// ── Proto Message Types ──────────────────────────────────────────────

pub const OpsetImport = struct {
    domain: []const u8 = "",
    version: u64 = 0,

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "domain", .encoding = .string },
        .{ .field_num = 2, .name = "version", .encoding = .varint },
    };
};

pub const ExternalDataEntry = struct {
    key: []const u8 = "",
    value: []const u8 = "",

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "key", .encoding = .string },
        .{ .field_num = 2, .name = "value", .encoding = .string },
    };
};

/// Parsed external data info for loading weights from separate files.
pub const ExternalDataInfo = struct {
    location: []const u8 = "", // path relative to model directory
    offset: i64 = 0,
    length: i64 = -1, // -1 means read all remaining
};

pub const TensorProto = struct {
    dims: []i64 = &.{},
    data_type: DataType = .undefined,
    // Packed scalar payloads kept as raw wire bytes (zero-copy into the input
    // buffer). Callers cast via std.mem.bytesAsSlice when they need typed
    // access; otherwise the model copy remains cheap even for GB-sized
    // initializers.
    float_data: []const u8 = "", // packed float
    int32_data: []const u8 = "", // packed int32 varint
    int64_data: []const u8 = "", // packed int64 varint
    name: []const u8 = "",
    raw_data: []const u8 = "",
    double_data: []const u8 = "", // packed double
    external_data: []ExternalDataEntry = &.{},
    data_location: DataLocation = .default,

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "dims", .encoding = .repeated_varint },
        .{ .field_num = 2, .name = "data_type", .encoding = .varint },
        .{ .field_num = 4, .name = "float_data", .encoding = .packed_raw_fixed32 },
        .{ .field_num = 5, .name = "int32_data", .encoding = .packed_raw_varint },
        .{ .field_num = 7, .name = "int64_data", .encoding = .packed_raw_varint },
        .{ .field_num = 8, .name = "name", .encoding = .string },
        .{ .field_num = 9, .name = "raw_data", .encoding = .string },
        .{ .field_num = 10, .name = "double_data", .encoding = .packed_raw_fixed64 },
        .{ .field_num = 13, .name = "external_data", .encoding = .repeated_submessage },
        .{ .field_num = 14, .name = "data_location", .encoding = .varint },
    };

    pub fn deinit(self: *TensorProto, allocator: std.mem.Allocator) void {
        message.deinit(TensorProto, allocator, self);
    }

    /// Parse external_data key-value pairs into structured info.
    pub fn externalDataInfo(self: *const TensorProto) ExternalDataInfo {
        var info = ExternalDataInfo{};
        for (self.external_data) |entry| {
            if (std.mem.eql(u8, entry.key, "location")) {
                info.location = entry.value;
            } else if (std.mem.eql(u8, entry.key, "offset")) {
                info.offset = std.fmt.parseInt(i64, entry.value, 10) catch 0;
            } else if (std.mem.eql(u8, entry.key, "length")) {
                info.length = std.fmt.parseInt(i64, entry.value, 10) catch -1;
            }
        }
        return info;
    }

    /// Check if this tensor uses external data storage.
    pub fn isExternal(self: *const TensorProto) bool {
        return self.data_location == .external;
    }
};

pub const TensorShapeProto = struct {
    dims: []Dimension = &.{},

    pub const Dimension = struct {
        // `oneof value { int64 dim_value = 1; string dim_param = 2; }` — we
        // split the oneof into two fields. `dim_value = null` means dynamic
        // (the dim_param branch was taken, or neither was set).
        dim_value: ?i64 = null,
        dim_param: []const u8 = "",

        pub const _pb_field_map = [_]FieldDesc{
            .{ .field_num = 1, .name = "dim_value", .encoding = .varint },
            .{ .field_num = 2, .name = "dim_param", .encoding = .string },
        };
    };

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "dims", .encoding = .repeated_submessage },
    };
};

pub const TensorTypeProto = struct {
    elem_type: DataType = .undefined,
    shape: ?TensorShapeProto = null,

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "elem_type", .encoding = .varint },
        .{ .field_num = 2, .name = "shape", .encoding = .submessage },
    };
};

pub const TypeProto = struct {
    tensor_type: ?TensorTypeProto = null,

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "tensor_type", .encoding = .submessage },
    };
};

pub const ValueInfoProto = struct {
    name: []const u8 = "",
    type_proto: ?TypeProto = null,

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "name", .encoding = .string },
        // ONNX wire field: `optional TypeProto type = 2;`. We name the Zig
        // field `type_proto` to avoid clashing with Zig's `type` keyword, and
        // map it to wire field 2.
        .{ .field_num = 2, .name = "type_proto", .encoding = .submessage },
    };
};

pub const AttributeProto = struct {
    name: []const u8 = "",
    f: f32 = 0,
    i: i64 = 0,
    s: []const u8 = "",
    t: ?TensorProto = null,
    g: ?GraphProto = null,
    floats: []f32 = &.{},
    ints: []i64 = &.{},
    strings: [][]const u8 = &.{},
    graphs: []GraphProto = &.{},
    attr_type: AttributeType = .undefined,

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "name", .encoding = .string },
        .{ .field_num = 2, .name = "f", .encoding = .fixed32 },
        .{ .field_num = 3, .name = "i", .encoding = .varint },
        .{ .field_num = 4, .name = "s", .encoding = .string },
        .{ .field_num = 5, .name = "t", .encoding = .submessage },
        .{ .field_num = 6, .name = "g", .encoding = .submessage },
        .{ .field_num = 7, .name = "floats", .encoding = .repeated_fixed32 },
        .{ .field_num = 8, .name = "ints", .encoding = .repeated_varint },
        .{ .field_num = 9, .name = "strings", .encoding = .repeated_string },
        .{ .field_num = 11, .name = "graphs", .encoding = .repeated_submessage },
        .{ .field_num = 20, .name = "attr_type", .encoding = .varint },
    };
};

pub const NodeProto = struct {
    inputs: [][]const u8 = &.{},
    outputs: [][]const u8 = &.{},
    name: []const u8 = "",
    op_type: []const u8 = "",
    attributes: []AttributeProto = &.{},
    domain: []const u8 = "",

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "inputs", .encoding = .repeated_string },
        .{ .field_num = 2, .name = "outputs", .encoding = .repeated_string },
        .{ .field_num = 3, .name = "name", .encoding = .string },
        .{ .field_num = 4, .name = "op_type", .encoding = .string },
        .{ .field_num = 5, .name = "attributes", .encoding = .repeated_submessage },
        .{ .field_num = 7, .name = "domain", .encoding = .string },
    };
};

pub const GraphProto = struct {
    nodes: []NodeProto = &.{},
    name: []const u8 = "",
    initializers: []TensorProto = &.{},
    inputs: []ValueInfoProto = &.{},
    outputs: []ValueInfoProto = &.{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "nodes", .encoding = .repeated_submessage },
        .{ .field_num = 2, .name = "name", .encoding = .string },
        .{ .field_num = 5, .name = "initializers", .encoding = .repeated_submessage },
        .{ .field_num = 11, .name = "inputs", .encoding = .repeated_submessage },
        .{ .field_num = 12, .name = "outputs", .encoding = .repeated_submessage },
    };
};

pub const ModelProto = struct {
    ir_version: u64 = 0,
    opset_import: []OpsetImport = &.{},
    graph: ?GraphProto = null,

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "ir_version", .encoding = .varint },
        .{ .field_num = 7, .name = "graph", .encoding = .submessage },
        .{ .field_num = 8, .name = "opset_import", .encoding = .repeated_submessage },
    };

    pub fn deinit(self: *ModelProto, allocator: std.mem.Allocator) void {
        message.deinit(ModelProto, allocator, self);
    }
};

// ── Eager parsing ────────────────────────────────────────────────────

/// Parse a complete ONNX ModelProto from binary protobuf data.
pub fn parseModelProto(allocator: std.mem.Allocator, data: []const u8) !ModelProto {
    return message.decode(ModelProto, allocator, data);
}

// ── Lazy parsing ─────────────────────────────────────────────────────
//
// For models with hundreds of initializers totalling multiple GB, we don't
// want to eagerly allocate a parallel tree of TensorProto structs: each
// TensorProto owns an `[]i64` dims slice and an `[]ExternalDataEntry`
// external_data slice, so eager parsing of a 2B-parameter model burns
// thousands of allocations for metadata the caller may never touch.
//
// `LazyGraphProto` stores initializers as their raw encoded payload bytes
// (one `[]const u8` per initializer, zero-copy into the input buffer) via
// the runtime's `lazy_repeated_submessage` encoding. Callers extract a
// single initializer on demand via `parseInitializer(i)`, or just read its
// name cheaply via `initializerName(i)`.

pub const LazyGraphProto = struct {
    nodes: []NodeProto = &.{},
    name: []const u8 = "",
    initializer_bytes: [][]const u8 = &.{},
    inputs: []ValueInfoProto = &.{},
    outputs: []ValueInfoProto = &.{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "nodes", .encoding = .repeated_submessage },
        .{ .field_num = 2, .name = "name", .encoding = .string },
        .{ .field_num = 5, .name = "initializer_bytes", .encoding = .lazy_repeated_submessage },
        .{ .field_num = 11, .name = "inputs", .encoding = .repeated_submessage },
        .{ .field_num = 12, .name = "outputs", .encoding = .repeated_submessage },
    };

    /// Number of initializers.
    pub fn initializerCount(self: *const LazyGraphProto) usize {
        return self.initializer_bytes.len;
    }

    /// Parse a single initializer by index (on demand).
    /// Caller must free via `tensor.deinit(allocator)`.
    pub fn parseInitializer(self: *const LazyGraphProto, allocator: std.mem.Allocator, index: usize) !TensorProto {
        if (index >= self.initializer_bytes.len) return error.EndOfStream;
        return message.decode(TensorProto, allocator, self.initializer_bytes[index]);
    }

    /// Extract just the name of an initializer without full parsing.
    /// Zero-copy — returns a slice into the original buffer.
    pub fn initializerName(self: *const LazyGraphProto, index: usize) ![]const u8 {
        if (index >= self.initializer_bytes.len) return error.EndOfStream;
        return parseTensorName(self.initializer_bytes[index]);
    }
};

pub const LazyModelProto = struct {
    ir_version: u64 = 0,
    opset_import: []OpsetImport = &.{},
    graph: ?LazyGraphProto = null,

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "ir_version", .encoding = .varint },
        .{ .field_num = 7, .name = "graph", .encoding = .submessage },
        .{ .field_num = 8, .name = "opset_import", .encoding = .repeated_submessage },
    };

    pub fn deinit(self: *LazyModelProto, allocator: std.mem.Allocator) void {
        message.deinit(LazyModelProto, allocator, self);
    }
};

/// Parse an ONNX model with lazy initializer loading.
/// Initializers are not parsed up front — use graph.parseInitializer(i)
/// to parse individual weights on demand.
pub fn parseLazyModelProto(allocator: std.mem.Allocator, data: []const u8) !LazyModelProto {
    return message.decode(LazyModelProto, allocator, data);
}

/// Extract only the name field (field 8) from a TensorProto without allocating.
/// Uses the raw wire reader directly since we only care about one string.
fn parseTensorName(data: []const u8) ![]const u8 {
    const pb = @import("protobuf").wire;
    var pos: usize = 0;
    while (pos < data.len) {
        const tag = try pb.readTag(data, &pos);
        switch (tag.field) {
            8 => return try pb.readLengthDelimited(data, &pos),
            else => try pb.skipField(data, &pos, tag.wire_type),
        }
    }
    return "";
}

// ── Tests ────────────────────────────────────────────────────────────

test "parse empty model" {
    const allocator = std.testing.allocator;
    var result = try parseModelProto(allocator, "");
    defer message.deinit(ModelProto, allocator, &result);
    try std.testing.expectEqual(@as(?GraphProto, null), result.graph);
}

test "TensorShapeProto.Dimension: dim_value only" {
    const allocator = std.testing.allocator;
    // Build dim via roundtrip through the runtime.
    const original = TensorShapeProto.Dimension{ .dim_value = 42 };
    const bytes = try message.encode(TensorShapeProto.Dimension, allocator, &original);
    defer allocator.free(bytes);

    var decoded = try message.decode(TensorShapeProto.Dimension, allocator, bytes);
    defer message.deinit(TensorShapeProto.Dimension, allocator, &decoded);
    try std.testing.expectEqual(@as(?i64, 42), decoded.dim_value);
    try std.testing.expectEqualStrings("", decoded.dim_param);
}

test "TensorShapeProto.Dimension: dim_param only, dim_value unset" {
    const allocator = std.testing.allocator;
    const original = TensorShapeProto.Dimension{ .dim_param = "batch" };
    const bytes = try message.encode(TensorShapeProto.Dimension, allocator, &original);
    defer allocator.free(bytes);

    var decoded = try message.decode(TensorShapeProto.Dimension, allocator, bytes);
    defer message.deinit(TensorShapeProto.Dimension, allocator, &decoded);
    try std.testing.expectEqual(@as(?i64, null), decoded.dim_value);
    try std.testing.expectEqualStrings("batch", decoded.dim_param);
}

test "parseLazyModelProto skips initializer parsing" {
    const allocator = std.testing.allocator;

    // Build a model with an initializer via the runtime encoder.
    var dims = [_]i64{ 2, 3 };
    const float_vals = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };
    const raw = std.mem.sliceAsBytes(&float_vals);
    var initializers = [_]TensorProto{
        .{ .name = "weight", .dims = &dims, .data_type = .float32, .raw_data = raw },
    };
    var inp_names = [_][]const u8{"weight"};
    var out_names = [_][]const u8{"y"};
    var nodes = [_]NodeProto{
        .{ .op_type = "Identity", .inputs = &inp_names, .outputs = &out_names },
    };
    var input_infos = [_]ValueInfoProto{.{ .name = "weight" }};
    var output_infos = [_]ValueInfoProto{.{ .name = "y" }};
    const graph = GraphProto{
        .name = "test",
        .nodes = &nodes,
        .initializers = &initializers,
        .inputs = &input_infos,
        .outputs = &output_infos,
    };
    const model = ModelProto{ .ir_version = 8, .graph = graph };
    const bytes = try message.encode(ModelProto, allocator, &model);
    defer allocator.free(bytes);

    // Parse lazily — initializers are not parsed up front.
    var lazy = try parseLazyModelProto(allocator, bytes);
    defer message.deinit(LazyModelProto, allocator, &lazy);

    try std.testing.expectEqual(@as(u64, 8), lazy.ir_version);
    try std.testing.expect(lazy.graph != null);
    const g = lazy.graph.?;
    try std.testing.expectEqualStrings("test", g.name);
    try std.testing.expectEqual(@as(usize, 1), g.nodes.len);
    try std.testing.expectEqual(@as(usize, 1), g.initializerCount());

    // Initializer name can be extracted without full parsing.
    const name = try g.initializerName(0);
    try std.testing.expectEqualStrings("weight", name);

    // Parse the initializer on demand.
    var tensor = try g.parseInitializer(allocator, 0);
    defer message.deinit(TensorProto, allocator, &tensor);
    try std.testing.expectEqualStrings("weight", tensor.name);
    try std.testing.expectEqual(@as(usize, 2), tensor.dims.len);
    try std.testing.expectEqual(@as(i64, 2), tensor.dims[0]);
    try std.testing.expectEqual(@as(i64, 3), tensor.dims[1]);
    try std.testing.expectEqual(DataType.float32, tensor.data_type);
    try std.testing.expectEqual(@as(usize, 24), tensor.raw_data.len);
}

test "parseLazyModelProto empty model" {
    const allocator = std.testing.allocator;
    var lazy = try parseLazyModelProto(allocator, "");
    defer message.deinit(LazyModelProto, allocator, &lazy);
    try std.testing.expect(lazy.graph == null);
}
