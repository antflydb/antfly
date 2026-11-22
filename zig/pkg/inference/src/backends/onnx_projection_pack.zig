// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const ml = @import("ml");
const onnx = @import("onnx_graph");
const ops = @import("../ops/ops.zig");
const context = @import("compute_context.zig");
const Operation = @import("../execution/backend_operation.zig").Borrowed;
const Cleanup = @import("../execution/cleanup_context.zig").Context;

const NodeId = ml.graph.NodeId;
const Graph = ml.graph.Graph;
const Shape = ml.graph.Shape;
const prefix = "__antfly_packed_onnx_projection_";
const Name = [prefix.len + 64]u8;
const byte_cap = 32 * 1024 * 1024;

const Member = struct {
    name: []const u8,
    output: NodeId,
    columns: usize,

    fn lessThan(_: void, a: Member, b: Member) bool {
        return std.mem.order(u8, a.name, b.name) == .lt;
    }
};

const Group = struct {
    lhs: NodeId,
    width: usize,
    rows: i64,
    entries: [16]Member = undefined,
    count: usize = 0,

    fn columns(self: *const Group) usize {
        var total: usize = 0;
        for (self.entries[0..self.count]) |entry| total += entry.columns;
        return total;
    }

    fn name(self: *const Group) Name {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(std.mem.asBytes(&self.width));
        for (self.entries[0..self.count]) |entry| {
            hash.update(std.mem.asBytes(&entry.name.len));
            hash.update(entry.name);
            hash.update(std.mem.asBytes(&entry.columns));
        }
        return prefix.* ++ std.fmt.bytesToHex(hash.finalResult(), .lower);
    }
};

const GroupKey = struct { source: NodeId, rows: i64, width: usize };

fn groups(a: std.mem.Allocator, model: *const onnx.Model, graph: *const Graph) ![]Group {
    var index: std.AutoHashMapUnmanaged(GroupKey, usize) = .empty;
    defer index.deinit(a);
    var result: std.ArrayList(Group) = .empty;
    defer result.deinit(a);
    const reachable = try a.alloc(bool, graph.nodeCount());
    defer a.free(reachable);
    @memset(reachable, false);
    for (graph.outputs.items) |output| reachable[output] = true;
    var reverse = graph.nodeCount();
    while (reverse > 0) {
        reverse -= 1;
        if (reachable[reverse]) {
            for (graph.node(reverse).getInputs()) |input| {
                if (input != ml.graph.null_node) reachable[input] = true;
            }
        }
    }
    for (graph.nodes.items, 0..) |node, raw| {
        if (!reachable[raw]) continue;
        if (node.op != .dot_general or node.num_inputs != 2) continue;
        const dot = node.op.dot_general;
        if (dot.num_batch != 0 or dot.num_contracting != 1 or dot.lhs_contracting[0] != 1 or dot.rhs_contracting[0] != 0) continue;
        const lhs = graph.node(node.inputs[0]);
        const rhs = graph.node(node.inputs[1]);
        if (rhs.op != .parameter or lhs.output_shape.rank() != 2 or rhs.output_shape.rank() != 2 or
            lhs.output_shape.dtype != .f32 or rhs.output_shape.dtype != .f32 or node.output_shape.dtype != .f32) continue;
        const name = graph.parameterName(rhs);
        if (!model.initializer_map.contains(name)) continue;
        const width = rhs.output_shape.dim(0);
        const cols = rhs.output_shape.dim(1);
        const rows = lhs.output_shape.dim(0);
        if (width < 128 or cols < 128 or width > byte_cap / 4 or cols > byte_cap / 4 or
            lhs.output_shape.dim(1) != width or rows == 0) continue;
        const source = if (lhs.op == .reshape and lhs.num_inputs == 1) lhs.inputs[0] else node.inputs[0];
        const key = GroupKey{ .source = source, .rows = rows, .width = @intCast(width) };
        const slot = try index.getOrPut(a, key);
        if (!slot.found_existing) {
            slot.value_ptr.* = result.items.len;
            try result.append(a, .{ .lhs = node.inputs[0], .width = key.width, .rows = rows });
        }
        const group = &result.items[slot.value_ptr.*];
        if (group.count < group.entries.len) group.entries[group.count] = .{ .name = name, .output = @intCast(raw), .columns = @intCast(cols) };
        group.count += 1;
    }
    var live: usize = 0;
    for (result.items) |group| {
        if (group.count < 2 or group.count > group.entries.len) continue;
        var candidate = group;
        const members = candidate.entries[0..candidate.count];
        std.mem.sort(Member, members, {}, Member.lessThan);
        var distinct = true;
        for (members[0 .. members.len - 1], members[1..]) |left, right| {
            if (std.mem.eql(u8, left.name, right.name)) distinct = false;
        }
        const cols = candidate.columns();
        if (!distinct or cols > byte_cap / 4 / candidate.width) continue;
        result.items[live] = candidate;
        live += 1;
    }
    result.shrinkRetainingCapacity(live);
    return result.toOwnedSlice(a);
}

pub const Plan = struct {
    candidates: []Group = &.{},
    names: ?std.heap.ArenaAllocator = null,

    pub fn init(a: std.mem.Allocator, model: *const onnx.Model, graph: *const Graph) !Plan {
        const candidates = try groups(a, model, graph);
        errdefer a.free(candidates);
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        for (candidates) |*candidate| {
            for (candidate.entries[0..candidate.count]) |*member| {
                member.name = try arena.allocator().dupe(u8, member.name);
            }
        }
        return .{ .candidates = candidates, .names = arena };
    }

    pub fn deinit(self: *Plan, a: std.mem.Allocator) void {
        if (self.names) |*arena| arena.deinit();
        a.free(self.candidates);
    }
};

pub const Registry = struct {
    const Entry = struct { name: Name, tensor: ops.CT };
    entries: std.ArrayList(Entry) = .empty,

    pub fn get(self: *const Registry, parameter_name: []const u8) ?ops.CT {
        for (self.entries.items) |entry| {
            if (std.mem.eql(u8, &entry.name, parameter_name)) return entry.tensor;
        }
        return null;
    }

    pub fn deinit(self: *Registry, a: std.mem.Allocator, cb: *const ops.ComputeBackend, cleanup: Cleanup) void {
        for (self.entries.items) |entry| cb.freeWithCleanup(entry.tensor, cleanup);
        self.entries.deinit(a);
    }

    pub fn import(self: *Registry, a: std.mem.Allocator, model: *onnx.Model, plan: *const Plan, backend: *context.SharedBackendContext, operation: ?Operation) !void {
        var remaining: usize = byte_cap;
        for (plan.candidates) |candidate| {
            const name = candidate.name();
            if (self.get(&name) != null or model.initializer_map.contains(&name) or model.input_set.contains(&name)) continue;
            const cols = candidate.columns();
            const count = candidate.width * cols;
            if (count > remaining / 4) continue;
            try self.entries.ensureUnusedCapacity(a, 1);
            const packed_values = try pack(a, model, candidate, cols);
            const ct = backend.importDenseStaticF32WithOperation(a, .f32, &.{ @intCast(candidate.width), @intCast(cols) }, packed_values, operation) catch |err| switch (err) {
                error.CudaMemoryLimitExceeded => continue,
                else => return err,
            };
            self.entries.appendAssumeCapacity(.{ .name = name, .tensor = ct });
            remaining -= count * 4;
        }
    }

    /// Rebuild only concrete graphs and only when the model root owns the
    /// exact packed initializer. Specialization never reads weight payloads.
    pub fn rewrite(self: *const Registry, a: std.mem.Allocator, names: std.mem.Allocator, model: *const onnx.Model, converted: *onnx.ConvertResult) !void {
        if (self.entries.items.len == 0) return;
        const graph = &converted.graph;
        var plan = try Plan.init(a, model, graph);
        defer plan.deinit(a);
        var changes: std.ArrayList(struct { old: NodeId, new: NodeId }) = .empty;
        defer changes.deinit(a);
        var builder = ml.graph.Builder.init(graph);
        var parameters: std.StringHashMapUnmanaged(NodeId) = .empty;
        defer parameters.deinit(a);
        for (plan.candidates) |candidate| {
            if (candidate.rows <= 0) continue;
            const name = candidate.name();
            if (self.get(&name) == null) continue;
            const cols = candidate.columns();
            const parameter = if (parameters.get(&name)) |id| id else add: {
                const owned = try names.dupe(u8, &name);
                const id = try builder.parameter(owned, Shape.init(.f32, &.{ @intCast(candidate.width), @intCast(cols) }));
                try parameters.put(a, owned, id);
                break :add id;
            };
            const output = try builder.matmul(candidate.lhs, parameter);
            var start: usize = 0;
            for (candidate.entries[0..candidate.count]) |member| {
                const end = start + member.columns;
                const slice = try builder.sliceLastDim(output, @intCast(start), @intCast(end));
                try changes.append(a, .{ .old = member.output, .new = slice });
                start = end;
            }
        }
        if (changes.items.len == 0) return;
        const redirects = try a.alloc(NodeId, graph.nodeCount());
        defer a.free(redirects);
        for (redirects, 0..) |*target, raw| target.* = @intCast(raw);
        for (changes.items) |change| redirects[change.old] = change.new;
        var rebuilt = try ml.graph.passes.fuse.rebuildWithRedirects(a, graph, redirects);
        errdefer rebuilt.graph.deinit();
        defer a.free(rebuilt.id_map);
        const parameters_out = try a.alloc([]const u8, rebuilt.graph.parameters.items.len);
        errdefer a.free(parameters_out);
        for (rebuilt.graph.parameters.items, parameters_out) |node, *name| {
            name.* = try names.dupe(u8, rebuilt.graph.parameterName(rebuilt.graph.node(node)));
        }
        for (converted.output_ids) |*id| id.* = rebuilt.id_map[id.*];
        a.free(converted.parameter_names);
        converted.parameter_names = parameters_out;
        graph.deinit();
        graph.* = rebuilt.graph;
    }
};

fn pack(a: std.mem.Allocator, model: *onnx.Model, group: Group, cols: usize) ![]f32 {
    const packed_values = try a.alloc(f32, group.width * cols);
    errdefer a.free(packed_values);
    var start: usize = 0;
    for (group.entries[0..group.count]) |member| {
        const data = try model.loadInitializerData(a, member.name);
        defer a.free(data);
        if (data.len != group.width * member.columns) return error.InvalidOnnxGraph;
        for (0..group.width) |row| {
            @memcpy(packed_values[row * cols + start ..][0..member.columns], data[row * member.columns ..][0..member.columns]);
        }
        start += member.columns;
    }
    return packed_values;
}

test "immutable projection grouping reuses one registered weight across row specializations" {
    const a = std.testing.allocator;
    var model = try onnx.Model.init(a, .{});
    defer model.deinit();
    try model.initializer_map.put(a, "w0", 0);
    try model.initializer_map.put(a, "w1", 1);
    var names = std.heap.ArenaAllocator.init(a);
    defer names.deinit();

    var root_name: ?Name = null;
    for ([_]i64{ -1, 1, 8 }) |rows| {
        var converted = onnx.ConvertResult{ .graph = Graph.init(a), .output_ids = &.{}, .parameter_names = &.{} };
        defer converted.deinit(a);
        var builder = ml.graph.Builder.init(&converted.graph);
        const lhs = try builder.parameter("x", Shape.init(.f32, &.{ rows, 128 }));
        const w0 = try builder.parameter("w0", Shape.init(.f32, &.{ 128, 192 }));
        const w1 = try builder.parameter("w1", Shape.init(.f32, &.{ 128, 128 }));
        const y1 = try builder.matmul(lhs, w1);
        const y0 = try builder.matmul(lhs, w0);
        try converted.graph.markOutput(y1);
        try converted.graph.markOutput(y0);
        converted.output_ids = try a.dupe(NodeId, &.{ y1, y0 });
        converted.parameter_names = try a.dupe([]const u8, &.{ "x", "w0", "w1" });

        var plan = try Plan.init(a, &model, &converted.graph);
        defer plan.deinit(a);
        try std.testing.expectEqual(@as(usize, 1), plan.candidates.len);
        const name = plan.candidates[0].name();
        if (root_name) |registered| {
            try std.testing.expectEqualSlices(u8, &registered, &name);
        } else {
            root_name = name;
            continue;
        }

        var placeholder: u8 = 0;
        var registry = Registry{};
        defer registry.entries.deinit(a);
        try registry.entries.append(a, .{ .name = root_name.?, .tensor = &placeholder });
        try registry.rewrite(a, names.allocator(), &model, &converted);
        try std.testing.expectEqual(@as(usize, 2), converted.graph.parameters.items.len);
        const first = converted.graph.node(converted.output_ids[0]);
        const second = converted.graph.node(converted.output_ids[1]);
        try std.testing.expectEqual(first.inputs[0], second.inputs[0]);
        try std.testing.expectEqual(@as(i64, 128), first.output_shape.dim(1));
        try std.testing.expectEqual(@as(i64, 192), second.output_shape.dim(1));
        try std.testing.expectEqual(@as(i64, 192), first.op.slice.starts[1]);
        try std.testing.expectEqual(@as(i64, 0), second.op.slice.starts[1]);
    }
}
