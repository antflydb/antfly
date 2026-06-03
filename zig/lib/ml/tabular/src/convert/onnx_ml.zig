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

//! ONNX-ML opset parser. Reads a serialised `onnx.ModelProto` and emits a
//! `TabularModel` IR for the first `TreeEnsembleClassifier`,
//! `TreeEnsembleRegressor`, `LinearClassifier`, or `LinearRegressor` node in
//! its graph.
//!
//! We inline a minimal protobuf reader (varint + length-delimited + packed
//! repeated scalars) rather than pulling `lib/onnx` into this module. This
//! keeps `lib/ml/tabular` WASM-clean and dependency-free. The wire format is
//! tiny and stable for the fields we care about.
//!
//! Currently supported nodes:
//!   - TreeEnsembleRegressor (single output)
//!   - TreeEnsembleClassifier (single tree per class or shared trees)
//!   - LinearRegressor
//!   - LinearClassifier
//!
//! Not yet supported: SVMRegressor / SVMClassifier. The same protobuf
//! shape works, but the attribute set differs; unsupported nodes are
//! rejected by the converter.

const std = @import("std");
const ir = @import("../ir.zig");

pub const Error = error{
    OutOfMemory,
    NotOnnxMl,
    UnsupportedNode,
    UnsupportedAttribute,
    MalformedModel,
    InvalidJson,
    Truncated,
};

pub const Parsed = struct {
    arena: *std.heap.ArenaAllocator,
    model: ir.TabularModel,

    pub fn deinit(self: *Parsed) void {
        const child = self.arena.child_allocator;
        self.arena.deinit();
        child.destroy(self.arena);
    }
};

pub fn parse(parent: std.mem.Allocator, bytes: []const u8) Error!Parsed {
    var arena = try parent.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(parent);
    errdefer {
        arena.deinit();
        parent.destroy(arena);
    }
    const alloc = arena.allocator();

    // Top-level ModelProto: field 7 = GraphProto. Find it and dive in.
    const graph_bytes = try findSubmessage(bytes, 7);
    if (graph_bytes.len == 0) return Error.NotOnnxMl;

    // GraphProto: field 1 = repeated NodeProto.
    var pr = ProtoReader{ .buf = graph_bytes };
    var ml_node_bytes: ?[]const u8 = null;
    var ml_op_type: []const u8 = "";
    while (pr.hasMore()) {
        const tag = pr.readTag() catch return Error.Truncated;
        if (tag.field_num == 1 and tag.wire_type == 2) {
            const node_bytes = pr.readBytes() catch return Error.Truncated;
            // NodeProto field 4 = op_type (string).
            const op_type = try findString(node_bytes, 4);
            if (isTabularOp(op_type)) {
                ml_node_bytes = node_bytes;
                ml_op_type = op_type;
                break;
            }
        } else {
            pr.skipField(tag.wire_type) catch return Error.Truncated;
        }
    }
    const node_bytes = ml_node_bytes orelse return Error.UnsupportedNode;

    // NodeProto field 5 = repeated AttributeProto (one entry per attribute).
    var attrs = std.array_list.Managed(Attribute).init(alloc);
    var npr = ProtoReader{ .buf = node_bytes };
    while (npr.hasMore()) {
        const tag = npr.readTag() catch return Error.Truncated;
        if (tag.field_num == 5 and tag.wire_type == 2) {
            const attr_bytes = npr.readBytes() catch return Error.Truncated;
            try attrs.append(try parseAttribute(alloc, attr_bytes));
        } else {
            npr.skipField(tag.wire_type) catch return Error.Truncated;
        }
    }
    const bag: AttributeBag = .{ .attrs = attrs.items };

    if (std.mem.eql(u8, ml_op_type, "TreeEnsembleRegressor"))
        return finishTree(alloc, arena, bag, .regression);
    if (std.mem.eql(u8, ml_op_type, "TreeEnsembleClassifier"))
        return finishTree(alloc, arena, bag, .multiclass);
    if (std.mem.eql(u8, ml_op_type, "LinearRegressor"))
        return finishLinear(alloc, arena, bag, .regression);
    if (std.mem.eql(u8, ml_op_type, "LinearClassifier"))
        return finishLinear(alloc, arena, bag, .multiclass);
    return Error.UnsupportedNode;
}

fn isTabularOp(op: []const u8) bool {
    return std.mem.eql(u8, op, "TreeEnsembleRegressor") or
        std.mem.eql(u8, op, "TreeEnsembleClassifier") or
        std.mem.eql(u8, op, "LinearRegressor") or
        std.mem.eql(u8, op, "LinearClassifier");
}

// ---------------------------------------------------------------------------
// Tree-ensemble assembly
// ---------------------------------------------------------------------------

fn finishTree(
    alloc: std.mem.Allocator,
    arena: *std.heap.ArenaAllocator,
    bag: AttributeBag,
    task_hint: ir.TaskType,
) Error!Parsed {
    const treeids = bag.ints("nodes_treeids") orelse return Error.MalformedModel;
    const nodeids = bag.ints("nodes_nodeids") orelse return Error.MalformedModel;
    const featureids = bag.ints("nodes_featureids") orelse return Error.MalformedModel;
    const modes = bag.strings("nodes_modes") orelse return Error.MalformedModel;
    const values = bag.floats("nodes_values") orelse return Error.MalformedModel;
    const truenodes = bag.ints("nodes_truenodeids") orelse return Error.MalformedModel;
    const falsenodes = bag.ints("nodes_falsenodeids") orelse return Error.MalformedModel;
    const missing_true = bag.ints("nodes_missing_value_tracks_true");

    const n = treeids.len;
    if (n == 0 or nodeids.len != n or featureids.len != n or modes.len != n or
        values.len != n or truenodes.len != n or falsenodes.len != n) return Error.MalformedModel;

    var max_tree: i64 = 0;
    for (treeids) |t| if (t > max_tree) {
        max_tree = t;
    };
    if (max_tree < 0) return Error.MalformedModel;
    const num_trees = try i64PlusOneToU32(max_tree);

    var tree_starts = try alloc.alloc(i32, num_trees);
    @memset(tree_starts, -1);

    // Build the (tree_id, intra-tree node id) → global slot map first.
    var per_tree_map = try alloc.alloc(std.AutoHashMap(i64, i32), num_trees);
    var t: usize = 0;
    while (t < num_trees) : (t += 1) per_tree_map[t] = std.AutoHashMap(i64, i32).init(alloc);
    defer for (per_tree_map) |*m| m.deinit();

    {
        var i: usize = 0;
        while (i < n) : (i += 1) {
            if (treeids[i] < 0) return Error.MalformedModel;
            const tt = try i64ToUsize(treeids[i]);
            if (tt >= num_trees) return Error.MalformedModel;
            per_tree_map[tt].put(nodeids[i], try usizeToI32(i)) catch return Error.OutOfMemory;
        }
    }

    // tree_starts[t] is the global slot of the ROOT (nodes_nodeids == 0).
    // ONNX-ML does not guarantee root-first ordering, so we look it up
    // explicitly instead of using the first matching tree-id slot.
    {
        var tt: usize = 0;
        while (tt < num_trees) : (tt += 1) {
            const root_slot = per_tree_map[tt].get(0) orelse return Error.MalformedModel;
            tree_starts[tt] = root_slot;
        }
    }

    var feature_index = try alloc.alloc(i32, n);
    var threshold = try alloc.alloc(f64, n);
    var left_child = try alloc.alloc(i32, n);
    var right_child = try alloc.alloc(i32, n);
    var leaf_value = try alloc.alloc(f64, n);
    var default_left = try alloc.alloc(bool, n);
    @memset(leaf_value, 0);

    {
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const tt = try i64ToUsize(treeids[i]);
            if (tt >= num_trees) return Error.MalformedModel;
            const mode = modes[i];
            if (std.mem.eql(u8, mode, "LEAF")) {
                feature_index[i] = -1;
                threshold[i] = 0;
                left_child[i] = -1;
                right_child[i] = -1;
            } else {
                feature_index[i] = try i64ToI32(featureids[i]);
                if (feature_index[i] < 0) return Error.MalformedModel;
                // BRANCH_LEQ ("<=") nudged to "<" for engine compatibility.
                threshold[i] = if (std.mem.eql(u8, mode, "BRANCH_LEQ"))
                    std.math.nextAfter(f64, values[i], std.math.inf(f64))
                else if (std.mem.eql(u8, mode, "BRANCH_LT"))
                    values[i]
                else
                    return Error.UnsupportedAttribute;

                const lt = per_tree_map[tt].get(truenodes[i]) orelse return Error.MalformedModel;
                const rt = per_tree_map[tt].get(falsenodes[i]) orelse return Error.MalformedModel;
                left_child[i] = lt;
                right_child[i] = rt;
            }
            default_left[i] = if (missing_true) |mt|
                (if (i < mt.len) mt[i] != 0 else false)
            else
                false;
        }
    }

    // Leaf weights — same shape for regressor and classifier.
    var num_outputs: u32 = 1;
    {
        const target_treeids = bag.ints("target_treeids") orelse bag.ints("class_treeids");
        const target_nodeids = bag.ints("target_nodeids") orelse bag.ints("class_nodeids");
        const target_ids = bag.ints("target_ids") orelse bag.ints("class_ids");
        const target_weights = bag.floats("target_weights") orelse bag.floats("class_weights");
        if (target_treeids == null or target_nodeids == null or target_ids == null or target_weights == null)
            return Error.MalformedModel;
        if (target_treeids.?.len != target_weights.?.len or target_nodeids.?.len != target_weights.?.len or
            target_ids.?.len != target_weights.?.len)
            return Error.MalformedModel;
        var max_out: i64 = 0;
        for (target_ids.?) |o| {
            if (o < 0) return Error.MalformedModel;
            if (o > max_out) {
                max_out = o;
            }
        }
        num_outputs = try i64PlusOneToU32(max_out);

        var i: usize = 0;
        while (i < target_weights.?.len) : (i += 1) {
            if (target_treeids.?[i] < 0) return Error.MalformedModel;
            const tt = try i64ToUsize(target_treeids.?[i]);
            if (tt >= num_trees) return Error.MalformedModel;
            const local = target_nodeids.?[i];
            const slot = per_tree_map[tt].get(local) orelse return Error.MalformedModel;
            leaf_value[try i32ToUsize(slot)] = target_weights.?[i];
        }
    }

    const base_values = bag.floats("base_values");
    const base_score: f64 = if (base_values) |bv| (if (bv.len > 0) bv[0] else 0) else 0;
    const num_features = inferNumFeatures(feature_index);

    const te_ptr = try alloc.create(ir.TreeEnsemble);
    te_ptr.* = .{
        .objective = "",
        .base_score = base_score,
        .num_trees = num_trees,
        .num_features = num_features,
        .max_depth = 0,
        .nodes = .{
            .feature_index = feature_index,
            .threshold = threshold,
            .left_child = left_child,
            .right_child = right_child,
            .leaf_value = leaf_value,
            .default_left = default_left,
            .tree_starts = tree_starts,
        },
    };
    const stages = try alloc.alloc(ir.Stage, 1);
    stages[0] = .{ .type = .tree_ensemble, .tree_ensemble = te_ptr };

    const activation = postTransformActivation(bag.string("post_transform") orelse "NONE");
    const task: ir.TaskType = if (task_hint == .multiclass and num_outputs > 1) .multiclass else if (task_hint == .multiclass) .binary_classification else .regression;

    return .{
        .arena = arena,
        .model = .{
            .schema_version = ir.schema_version,
            .metadata = .{
                .source_framework = try alloc.dupe(u8, "onnx_ml"),
                .task = task,
                .num_features = num_features,
                .num_classes = if (task == .multiclass) num_outputs else 0,
            },
            .pipeline = stages,
            .output = .{ .activation = activation, .num_outputs = num_outputs },
        },
    };
}

fn finishLinear(
    alloc: std.mem.Allocator,
    arena: *std.heap.ArenaAllocator,
    bag: AttributeBag,
    task_hint: ir.TaskType,
) Error!Parsed {
    const coefficients = bag.floats("coefficients") orelse return Error.MalformedModel;
    const intercepts = bag.floats("intercepts") orelse return Error.MalformedModel;
    const num_outputs = try usizeToU32(intercepts.len);
    if (num_outputs == 0 or coefficients.len % num_outputs != 0) return Error.MalformedModel;
    const num_features = try usizeToU32(coefficients.len / num_outputs);

    var weights = try alloc.alloc([]const f64, num_outputs);
    var biases = try alloc.alloc(f64, num_outputs);
    var oi: usize = 0;
    while (oi < num_outputs) : (oi += 1) {
        const row = try alloc.alloc(f64, num_features);
        for (row, 0..) |*v, fi| v.* = @floatCast(coefficients[oi * num_features + fi]);
        weights[oi] = row;
        biases[oi] = @floatCast(intercepts[oi]);
    }

    const lm_ptr = try alloc.create(ir.LinearModel);
    lm_ptr.* = .{ .weights = weights, .biases = biases };
    const stages = try alloc.alloc(ir.Stage, 1);
    stages[0] = .{ .type = .linear, .linear = lm_ptr };

    const activation = postTransformActivation(bag.string("post_transform") orelse "NONE");
    const task: ir.TaskType = if (task_hint == .multiclass and num_outputs > 1) .multiclass else if (task_hint == .multiclass) .binary_classification else .regression;

    return .{
        .arena = arena,
        .model = .{
            .schema_version = ir.schema_version,
            .metadata = .{
                .source_framework = try alloc.dupe(u8, "onnx_ml"),
                .task = task,
                .num_features = num_features,
                .num_classes = if (task == .multiclass) num_outputs else 0,
            },
            .pipeline = stages,
            .output = .{ .activation = activation, .num_outputs = num_outputs },
        },
    };
}

fn postTransformActivation(s: []const u8) ir.Activation {
    if (std.mem.eql(u8, s, "SOFTMAX") or std.mem.eql(u8, s, "SOFTMAX_ZERO")) return .softmax;
    if (std.mem.eql(u8, s, "LOGISTIC")) return .sigmoid;
    return .identity;
}

fn inferNumFeatures(feature_index: []const i32) u32 {
    var max_f: i32 = -1;
    for (feature_index) |f| if (f > max_f) {
        max_f = f;
    };
    if (max_f < 0) return 1;
    const plus_one = std.math.add(i64, @as(i64, max_f), 1) catch return 1;
    return std.math.cast(u32, plus_one) orelse 1;
}

fn i64ToU32(v: i64) Error!u32 {
    return std.math.cast(u32, v) orelse Error.MalformedModel;
}

fn i64PlusOneToU32(v: i64) Error!u32 {
    const plus_one = std.math.add(i64, v, 1) catch return Error.MalformedModel;
    return i64ToU32(plus_one);
}

fn i64ToUsize(v: i64) Error!usize {
    return std.math.cast(usize, v) orelse Error.MalformedModel;
}

fn i64ToI32(v: i64) Error!i32 {
    return std.math.cast(i32, v) orelse Error.MalformedModel;
}

fn i32ToUsize(v: i32) Error!usize {
    return std.math.cast(usize, v) orelse Error.MalformedModel;
}

fn usizeToU32(v: usize) Error!u32 {
    return std.math.cast(u32, v) orelse Error.MalformedModel;
}

fn usizeToI32(v: usize) Error!i32 {
    return std.math.cast(i32, v) orelse Error.MalformedModel;
}

// ---------------------------------------------------------------------------
// Minimal protobuf reader. We only need:
//   - varint
//   - length-delimited (string / submessage / packed scalars)
//   - fixed32 (used by `float` and packed_floats)
//   - fixed64 (used by `double`; not needed for ONNX-ML attributes but
//     skip-able)
// ---------------------------------------------------------------------------

const ProtoReader = struct {
    buf: []const u8,
    pos: usize = 0,

    fn hasMore(self: ProtoReader) bool {
        return self.pos < self.buf.len;
    }

    fn readVarint(self: *ProtoReader) error{Truncated}!u64 {
        var x: u64 = 0;
        var s: u6 = 0;
        while (true) {
            if (self.pos >= self.buf.len) return error.Truncated;
            const b = self.buf[self.pos];
            self.pos += 1;
            x |= @as(u64, b & 0x7f) << s;
            if ((b & 0x80) == 0) return x;
            if (s >= 63) return error.Truncated;
            s += 7;
        }
    }

    const Tag = struct { field_num: u32, wire_type: u3 };

    fn readTag(self: *ProtoReader) error{Truncated}!Tag {
        const v = try self.readVarint();
        const field_num = std.math.cast(u32, v >> 3) orelse return error.Truncated;
        return .{ .field_num = field_num, .wire_type = @intCast(v & 0x7) };
    }

    fn readFixed32(self: *ProtoReader) error{Truncated}!u32 {
        if (self.pos + 4 > self.buf.len) return error.Truncated;
        const b = self.buf[self.pos .. self.pos + 4];
        self.pos += 4;
        return std.mem.readInt(u32, b[0..4], .little);
    }

    fn readFixed64(self: *ProtoReader) error{Truncated}!u64 {
        if (self.pos + 8 > self.buf.len) return error.Truncated;
        const b = self.buf[self.pos .. self.pos + 8];
        self.pos += 8;
        return std.mem.readInt(u64, b[0..8], .little);
    }

    fn readBytes(self: *ProtoReader) error{Truncated}![]const u8 {
        const len = try self.readVarint();
        const n = std.math.cast(usize, len) orelse return error.Truncated;
        if (n > self.buf.len - self.pos) return error.Truncated;
        const out = self.buf[self.pos .. self.pos + n];
        self.pos += n;
        return out;
    }

    fn skipField(self: *ProtoReader, wire_type: u3) error{Truncated}!void {
        switch (wire_type) {
            0 => _ = try self.readVarint(),
            1 => _ = try self.readFixed64(),
            2 => _ = try self.readBytes(),
            5 => _ = try self.readFixed32(),
            else => return error.Truncated,
        }
    }
};

fn findSubmessage(buf: []const u8, field_num: u32) Error![]const u8 {
    var pr = ProtoReader{ .buf = buf };
    while (pr.hasMore()) {
        const tag = pr.readTag() catch return Error.Truncated;
        if (tag.field_num == field_num and tag.wire_type == 2) {
            return pr.readBytes() catch return Error.Truncated;
        }
        pr.skipField(tag.wire_type) catch return Error.Truncated;
    }
    return &.{};
}

fn findString(buf: []const u8, field_num: u32) Error![]const u8 {
    return findSubmessage(buf, field_num); // same wire encoding
}

// AttributeProto: name=1 (string), f=2 (fixed32), i=3 (varint), s=4 (string),
//   floats=7 (repeated fixed32), ints=8 (repeated varint), strings=9 (repeated string),
//   type=20 (varint enum). Repeated fields may be encoded packed or unpacked.
const Attribute = struct {
    name: []const u8,
    s: []const u8 = "",
    floats: []const f32 = &.{},
    ints: []const i64 = &.{},
    strings: []const []const u8 = &.{},
};

fn parseAttribute(alloc: std.mem.Allocator, buf: []const u8) Error!Attribute {
    var pr = ProtoReader{ .buf = buf };
    var name: []const u8 = "";
    var s: []const u8 = "";
    var floats = std.array_list.Managed(f32).init(alloc);
    var ints = std.array_list.Managed(i64).init(alloc);
    var strings = std.array_list.Managed([]const u8).init(alloc);

    while (pr.hasMore()) {
        const tag = pr.readTag() catch return Error.Truncated;
        switch (tag.field_num) {
            1 => name = pr.readBytes() catch return Error.Truncated,
            4 => s = pr.readBytes() catch return Error.Truncated,
            7 => switch (tag.wire_type) {
                5 => {
                    const w = pr.readFixed32() catch return Error.Truncated;
                    try floats.append(@bitCast(w));
                },
                2 => {
                    const packed_bytes = pr.readBytes() catch return Error.Truncated;
                    var i: usize = 0;
                    while (i + 4 <= packed_bytes.len) : (i += 4) {
                        const w = std.mem.readInt(u32, packed_bytes[i..][0..4], .little);
                        try floats.append(@bitCast(w));
                    }
                },
                else => return Error.MalformedModel,
            },
            8 => switch (tag.wire_type) {
                0 => {
                    const v = pr.readVarint() catch return Error.Truncated;
                    try ints.append(@bitCast(v));
                },
                2 => {
                    const packed_bytes = pr.readBytes() catch return Error.Truncated;
                    var sub = ProtoReader{ .buf = packed_bytes };
                    while (sub.hasMore()) {
                        const v = sub.readVarint() catch return Error.Truncated;
                        try ints.append(@bitCast(v));
                    }
                },
                else => return Error.MalformedModel,
            },
            9 => {
                const v = pr.readBytes() catch return Error.Truncated;
                try strings.append(v);
            },
            else => pr.skipField(tag.wire_type) catch return Error.Truncated,
        }
    }

    return .{
        .name = name,
        .s = s,
        .floats = floats.items,
        .ints = ints.items,
        .strings = strings.items,
    };
}

const AttributeBag = struct {
    attrs: []const Attribute,

    fn find(self: AttributeBag, name: []const u8) ?*const Attribute {
        for (self.attrs) |*a| if (std.mem.eql(u8, a.name, name)) return a;
        return null;
    }
    fn ints(self: AttributeBag, name: []const u8) ?[]const i64 {
        return if (self.find(name)) |a| a.ints else null;
    }
    fn floats(self: AttributeBag, name: []const u8) ?[]const f32 {
        return if (self.find(name)) |a| a.floats else null;
    }
    fn strings(self: AttributeBag, name: []const u8) ?[]const []const u8 {
        return if (self.find(name)) |a| a.strings else null;
    }
    fn string(self: AttributeBag, name: []const u8) ?[]const u8 {
        return if (self.find(name)) |a| a.s else null;
    }
};

test "parse rejects malformed/empty input" {
    // A single zero byte parses as field_num=0, wire_type=0, then varint
    // wants more bytes — proto reader returns Truncated. Either rejection
    // mode is fine; we just want a non-success.
    try std.testing.expect(std.meta.isError(parse(std.testing.allocator, &[_]u8{0x00})));
}

test "ProtoReader varint round-trip" {
    // Manually-encoded varint 300 = 0xAC 0x02
    var pr = ProtoReader{ .buf = &[_]u8{ 0xac, 0x02 } };
    const v = try pr.readVarint();
    try std.testing.expectEqual(@as(u64, 300), v);
}
