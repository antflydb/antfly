// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Opt-in diagnostics for deciding the exact resident executor surface from
//! the actual reachable differentiated forward and cut backward programs.
//! No tensor data or parameter values are inspected or printed.
const std = @import("std");
const builtin = @import("builtin");
const ml = @import("ml").graph;
const OpTag = std.meta.Tag(ml.OpCode);
const op_tags = std.meta.tags(OpTag);
const dtype_tags = std.meta.tags(ml.DType);
var sequence = std.atomic.Value(u64).init(0);

fn enabled() bool {
    if (comptime builtin.os.tag == .freestanding) return false;
    const c = @cImport(@cInclude("stdlib.h"));
    const value = c.getenv("ANTFLY_RESIDENT_TRAINING_CENSUS") orelse return false;
    return std.mem.eql(u8, std.mem.span(value), "1");
}

pub fn emitIfEnabled(a: std.mem.Allocator, phase: []const u8, graph: *const ml.Graph, reachable: []const bool) void {
    if (!enabled()) return;
    emit(a, phase, graph, reachable) catch |err| {
        std.debug.print("resident-census phase={s} unavailable={s}\n", .{ phase, @errorName(err) });
    };
}

fn formatVariant(graph: *const ml.Graph, node: *const ml.Node, buffer: []u8) ![]const u8 {
    var used: usize = 0;
    const first = try std.fmt.bufPrint(buffer, "{s} out={s}{any}", .{ @tagName(node.op), @tagName(node.output_shape.dtype), node.output_shape.dims[0..node.output_shape.rank_] });
    used += first.len;
    for (node.getInputs(), 0..) |id, i| {
        if (id == ml.null_node) continue;
        const shape = graph.node(id).output_shape;
        const part = try std.fmt.bufPrint(buffer[used..], " in{d}={s}{any}", .{ i, @tagName(shape.dtype), shape.dims[0..shape.rank_] });
        used += part.len;
    }
    switch (node.op) {
        .parameter, .constant => {}, // exclude names, string offsets and data
        else => {
            const part = try std.fmt.bufPrint(buffer[used..], " attrs={any}", .{node.op});
            used += part.len;
        },
    }
    return buffer[0..used];
}

fn emit(a: std.mem.Allocator, phase: []const u8, graph: *const ml.Graph, reachable: []const bool) !void {
    if (reachable.len != graph.nodeCount()) return error.InvalidCensusReachability;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var variants = std.StringHashMapUnmanaged(usize).empty;
    var ordered = std.ArrayListUnmanaged([]const u8).empty;
    var op_counts: [op_tags.len]usize = @splat(0);
    var dtype_counts: [dtype_tags.len]usize = @splat(0);
    var nodes: usize = 0;
    var unknown_shapes: usize = 0;
    var max_rank: usize = 0;
    var variant_bytes: usize = 0;
    var omitted_variants: usize = 0;
    for (graph.nodes.items, reachable) |*node, live| {
        if (!live) continue;
        nodes += 1;
        op_counts[@intFromEnum(std.meta.activeTag(node.op))] += 1;
        dtype_counts[@intFromEnum(node.output_shape.dtype)] += 1;
        max_rank = @max(max_rank, node.output_shape.rank_);
        for (node.output_shape.dims[0..node.output_shape.rank_]) |dim| if (dim < 0) {
            unknown_shapes += 1;
            break;
        };
        var buffer: [4096]u8 = undefined;
        const key = formatVariant(graph, node, &buffer) catch {
            omitted_variants += 1;
            continue;
        };
        if (variants.getPtr(key)) |count| {
            count.* += 1;
        } else if (variants.count() < 512 and key.len <= 128 * 1024 - variant_bytes) {
            const owned = try scratch.dupe(u8, key);
            try variants.put(scratch, owned, 1);
            try ordered.append(scratch, owned);
            variant_bytes += owned.len;
        } else omitted_variants += 1;
    }
    const id = sequence.fetchAdd(1, .monotonic);
    std.debug.print("resident-census id={d} phase={s} reachable={d} graph_nodes={d} max_rank={d} dynamic_shapes={d} variants={d} omitted_nodes={d}\n", .{ id, phase, nodes, graph.nodeCount(), max_rank, unknown_shapes, variants.count(), omitted_variants });
    for (op_tags, op_counts) |tag, count| if (count != 0)
        std.debug.print("resident-census id={d} opcode={s} count={d}\n", .{ id, @tagName(tag), count });
    for (dtype_tags, dtype_counts) |tag, count| if (count != 0)
        std.debug.print("resident-census id={d} dtype={s} count={d}\n", .{ id, @tagName(tag), count });
    for (ordered.items) |key| std.debug.print("resident-census id={d} count={d} geometry={s}\n", .{ id, variants.get(key).?, key });
}
