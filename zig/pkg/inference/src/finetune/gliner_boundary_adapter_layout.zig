// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Schema-independent PEFT parameter layout for the exact published models.
//! Resolve and enroll this layout once per run. Individual task graphs use its
//! exact target intersection and keep every optimizer/checkpoint slot across
//! changing schemas. The trainer resolves absent gradients, optional-head zero
//! touches, and the upstream all-selected zero-loss fallback separately.
const std = @import("std");
const ml = @import("ml").graph;
const model = @import("../models/gliner_boundary.zig");
const inventory = @import("../models/gliner_boundary_artifact.zig");
const artifact = @import("gliner_boundary_adapter.zig");
const peft = @import("gliner_boundary_peft_graph.zig");
const Allocator = std.mem.Allocator;

pub const Descriptor = struct {
    target: artifact.Target,
    /// Native graph/store name, distinct from canonical checkpoint name.
    base_name: []const u8,
    a_name: []const u8,
    b_name: []const u8,
    magnitude_name: ?[]const u8,
    rank: u32,
    scale: f32,

    pub fn initialize(self: Descriptor, a: Allocator, base: []const f32, seed: u64) !peft.InitialWeights {
        return peft.initializeModule(a, self.target.module, self.target.in_dim, self.target.out_dim, self.rank, self.magnitude_name != null, base, seed);
    }

    pub fn aShape(self: Descriptor) [2]i64 {
        return .{ self.rank, self.target.in_dim };
    }
    pub fn bShape(self: Descriptor) [2]i64 {
        return .{ self.target.out_dim, self.rank };
    }
};

pub const Layout = struct {
    arena: std.heap.ArenaAllocator,
    backbone: model.Backbone,
    config: peft.Config,
    modules: []const Descriptor,
    /// Canonical sorted key/shape/PEFT configuration digest. Bind source
    /// artifact, schema/data and initialization seed in the run fingerprint.
    fingerprint: [32]u8,
    parameter_bytes: usize,
    slot_count: usize,

    pub fn deinit(self: *Layout) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Owned slice of borrowed canonical names, sorted as the model layout.
    /// An empty result explicitly means skip graph injection for this stage.
    pub fn targetsForGraph(self: *const Layout, a: Allocator, graph: *const ml.Graph) ![][]const u8 {
        if (graph.nodes.items.len > (peft.Limits{}).max_source_nodes) return error.BoundaryPeftLimitExceeded;
        var result = std.ArrayListUnmanaged([]const u8).empty;
        errdefer result.deinit(a);
        for (self.modules) |module| {
            var found = false;
            for (graph.nodes.items) |node| {
                const attrs = switch (node.op) {
                    .fused_linear, .fused_linear_no_bias => |attrs| attrs,
                    else => continue,
                };
                if (node.num_inputs < 2 or node.num_inputs > node.inputs.len or node.inputs[1] >= graph.nodes.items.len)
                    return error.InvalidBoundaryPeftGraph;
                const weight = graph.node(node.inputs[1]);
                if (weight.op != .parameter) continue;
                const name_attrs = weight.op.parameter;
                if (@as(usize, name_attrs.name_offset) + name_attrs.name_len > graph.string_table.items.len)
                    return error.InvalidBoundaryPeftGraph;
                const name = graph.parameterName(weight);
                if (!std.mem.eql(u8, name, module.base_name) and !std.mem.eql(u8, name, module.target.base_weight)) continue;
                if (attrs.in_dim != module.target.in_dim or attrs.out_dim != module.target.out_dim or
                    !weight.output_shape.eq(ml.Shape.init(.f32, &.{ module.target.out_dim, module.target.in_dim })))
                    return error.InvalidBoundaryPeftShape;
                found = true;
            }
            if (found) try result.append(a, module.target.module);
        }
        return result.toOwnedSlice(a);
    }
};

fn nativeWeight(canonical: []const u8) []const u8 {
    return if (std.mem.startsWith(u8, canonical, "encoder.encoder.layer.")) canonical["encoder.".len..] else canonical;
}

fn scalarHash(hash: *std.crypto.hash.sha2.Sha256, value: u32) void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, value, .little);
    hash.update(&bytes);
}
fn stringHash(hash: *std.crypto.hash.sha2.Sha256, value: []const u8) void {
    scalarHash(hash, @intCast(value.len));
    hash.update(value);
}

pub fn init(a: Allocator, backbone: model.Backbone, config: peft.Config, limits: peft.Limits) !Layout {
    try peft.validateConfig(config, limits);
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const scratch = arena.allocator();
    const matched = try scratch.alloc(bool, config.targets.len);
    @memset(matched, false);
    var modules = std.ArrayListUnmanaged(Descriptor).empty;
    var parameter_bytes: usize = 0;
    for (inventory.specs(backbone)) |spec| {
        if (spec.shape.len != 2 or !std.mem.endsWith(u8, spec.name, ".weight")) continue;
        const canonical = spec.name[0 .. spec.name.len - ".weight".len];
        var selected = false;
        for (config.targets) |target| if (peft.matchesModule(nativeWeight(canonical), canonical, target)) {
            // Delay marking aliases until the inventory verifies nn.Linear.
            selected = true;
        };
        if (!selected) continue;
        const target = artifact.expectedTarget(scratch, backbone, canonical, config.kind == .dora) catch |err| switch (err) {
            error.InvalidBoundaryAdapterTarget => continue,
            else => return err,
        };
        for (config.targets, 0..) |name, i| if (peft.matchesModule(nativeWeight(canonical), canonical, name)) {
            matched[i] = true;
        };
        if (modules.items.len >= limits.max_adapters) return error.BoundaryPeftLimitExceeded;
        const a_count = std.math.mul(usize, config.rank, target.in_dim) catch return error.BoundaryPeftLimitExceeded;
        const b_count = std.math.mul(usize, config.rank, target.out_dim) catch return error.BoundaryPeftLimitExceeded;
        const count = std.math.add(usize, a_count, b_count) catch return error.BoundaryPeftLimitExceeded;
        const total = std.math.add(usize, count, if (config.kind == .dora) target.out_dim else 0) catch return error.BoundaryPeftLimitExceeded;
        parameter_bytes = std.math.add(usize, parameter_bytes, std.math.mul(usize, total, 4) catch return error.BoundaryPeftLimitExceeded) catch return error.BoundaryPeftLimitExceeded;
        if (parameter_bytes > limits.max_adapter_bytes) return error.BoundaryPeftLimitExceeded;
        try modules.append(scratch, .{
            .target = target,
            .base_name = nativeWeight(target.base_weight),
            .a_name = try std.fmt.allocPrint(scratch, "base_model.model.{s}.lora_A.default.weight", .{canonical}),
            .b_name = try std.fmt.allocPrint(scratch, "base_model.model.{s}.lora_B.default.weight", .{canonical}),
            .magnitude_name = if (config.kind == .dora) try std.fmt.allocPrint(scratch, "base_model.model.{s}.lora_magnitude_vector.default.weight", .{canonical}) else null,
            .rank = config.rank,
            .scale = config.alpha / @as(f32, @floatFromInt(config.rank)),
        });
    }
    for (matched) |hit| if (!hit) return error.BoundaryPeftTargetNotResolved;
    std.mem.sort(Descriptor, modules.items, {}, struct {
        fn lessThan(_: void, lhs: Descriptor, rhs: Descriptor) bool {
            return std.mem.order(u8, lhs.target.module, rhs.target.module) == .lt;
        }
    }.lessThan);
    const aliases = try scratch.alloc([]const u8, config.targets.len);
    for (config.targets, aliases) |source, *target| target.* = try scratch.dupe(u8, source);
    var owned_config = config;
    owned_config.targets = aliases;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-gliner25-model-peft-layout/v1\x00");
    stringHash(&hash, @tagName(backbone));
    stringHash(&hash, @tagName(config.kind));
    scalarHash(&hash, config.rank);
    scalarHash(&hash, @bitCast(config.alpha));
    scalarHash(&hash, @bitCast(config.dropout));
    scalarHash(&hash, @intCast(modules.items.len));
    for (modules.items) |module| {
        for ([_][]const u8{ module.target.module, module.base_name, module.a_name, module.b_name, module.target.a_key, module.target.b_key }) |name| stringHash(&hash, name);
        if (module.magnitude_name) |name| stringHash(&hash, name);
        if (module.target.magnitude_key) |name| stringHash(&hash, name);
        scalarHash(&hash, module.target.in_dim);
        scalarHash(&hash, module.target.out_dim);
    }
    return .{ .arena = arena, .backbone = backbone, .config = owned_config, .modules = modules.items, .fingerprint = hash.finalResult(), .parameter_bytes = parameter_bytes, .slot_count = modules.items.len * @as(usize, if (config.kind == .dora) 3 else 2) };
}

test "GLiNER2.5 model adapter layout is stable across aliases variants and absent task graphs" {
    const a = std.testing.allocator;
    for ([_]model.Backbone{ .base, .multi, .small }) |backbone| {
        var layout = try init(a, backbone, .{ .kind = .dora, .rank = 2, .targets = &.{ "encoder", "all_task_heads" } }, .{});
        defer layout.deinit();
        var equivalent = try init(a, backbone, .{ .kind = .dora, .rank = 2, .targets = &.{ "relation_head", "record_head", "extractive_head", "classification_head", "encoder" } }, .{});
        defer equivalent.deinit();
        try std.testing.expectEqualSlices(u8, &layout.fingerprint, &equivalent.fingerprint);
        try std.testing.expectEqual(@as(usize, 3 * layout.modules.len), layout.slot_count);
        try std.testing.expectEqual(@as(usize, 131), layout.modules.len);
        var graph = ml.Graph.init(a);
        defer graph.deinit();
        const empty = try layout.targetsForGraph(a, &graph);
        defer a.free(empty);
        try std.testing.expectEqual(@as(usize, 0), empty.len);
        var b = ml.Builder.init(&graph);
        var selected: ?Descriptor = null;
        for (layout.modules) |module| if (std.mem.eql(u8, module.target.module, "boundary_head.count_head")) {
            selected = module;
        };
        const module = selected.?;
        const input = try b.parameter("request", ml.Shape.init(.f32, &.{ 1, module.target.in_dim }));
        const weight = try b.parameter(module.base_name, ml.Shape.init(.f32, &.{ module.target.out_dim, module.target.in_dim }));
        const output = try b.linearNoBias(input, weight, 1, module.target.in_dim, module.target.out_dim);
        try graph.markOutput(output);
        const targets = try layout.targetsForGraph(a, &graph);
        defer a.free(targets);
        try std.testing.expectEqual(@as(usize, 1), targets.len);
        var active = try peft.inject(a, &graph, .{ .kind = .dora, .mode = .eval, .rank = 2, .targets = targets }, .{});
        defer active.deinit();
        try std.testing.expectEqualStrings(module.a_name, active.adapters[0].a_name);
        try std.testing.expectEqualStrings(module.target.a_key, active.adapters[0].a_saved_key);
        try std.testing.expectEqualStrings(module.magnitude_name.?, active.adapters[0].magnitude_name.?);
        const base = try a.alloc(f32, module.target.in_dim);
        defer a.free(base);
        @memset(base, 0.25);
        var model_init = try module.initialize(a, base, 719);
        defer model_init.deinit();
        var graph_init = try peft.initialize(a, active.adapters[0], base, 719);
        defer graph_init.deinit();
        try std.testing.expectEqualSlices(f32, graph_init.a, model_init.a);
        try std.testing.expectEqualSlices(f32, graph_init.magnitude.?, model_init.magnitude.?);
    }
}

fn allocationCheck(a: Allocator) !void {
    var layout = try init(a, .small, .{ .rank = 2, .targets = &.{ "classification_head", "record_head" } }, .{});
    defer layout.deinit();
}

test "GLiNER2.5 model adapter layout rejects invalid nonlinears resources and cleans allocation failures" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.BoundaryPeftTargetNotResolved, init(a, .small, .{ .rank = 2, .targets = &.{"encoder.embeddings.word_embeddings"} }, .{}));
    try std.testing.expectError(error.BoundaryPeftTargetNotResolved, init(a, .small, .{ .rank = 2, .targets = &.{"missing"} }, .{}));
    try std.testing.expectError(error.BoundaryPeftLimitExceeded, init(a, .small, .{ .rank = 2 }, .{ .max_adapters = 1 }));
    try std.testing.expectError(error.BoundaryPeftLimitExceeded, init(a, .small, .{ .rank = 2 }, .{ .max_adapter_bytes = 1 }));
    try std.testing.checkAllAllocationFailures(a, allocationCheck, .{});
}
