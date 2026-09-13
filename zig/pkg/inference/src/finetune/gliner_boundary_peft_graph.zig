// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Exact linear-module PEFT for the boundary training graph. Adapter weights
//! are shared by physical base parameter; dropout belongs to each module call.
//! DoRA's norm is recomputed from live weights on every forward and detached
//! intrinsically in the graph. The compatibility LoRA injector is unchanged.
const std = @import("std");
const ml = @import("ml").graph;
const Allocator = std.mem.Allocator;
const Id = ml.NodeId;
const Shape = ml.Shape;
const nil = ml.null_node;

pub const Kind = enum { lora, dora };
pub const Mode = enum { eval, train };
pub const Config = struct {
    kind: Kind = .lora,
    mode: Mode = .train,
    rank: u32,
    alpha: f32 = 16,
    dropout: f32 = 0,
    /// Public GLiNER aliases, or exact native/HF module paths without .weight.
    /// Every requested target must resolve to a linear module in this graph.
    targets: []const []const u8 = &.{"encoder"},
};
pub const Limits = struct {
    max_source_nodes: usize = 1_000_000,
    max_result_nodes: usize = 2_000_000,
    max_adapters: usize = 4096,
    max_uses: usize = 16384,
    max_rank: u32 = 1024,
    max_adapter_bytes: usize = 512 * 1024 * 1024,
    max_dropout_bytes: usize = 512 * 1024 * 1024,
    /// Conservative sum of new forward tensors; not a peak or a backward bound.
    max_added_forward_bytes: usize = 4 * 1024 * 1024 * 1024,
};
pub const Plan = struct {
    adapters: usize = 0,
    uses: usize = 0,
    adapter_bytes: usize = 0,
    dropout_bytes: usize = 0,
    added_forward_upper_bound_bytes: usize = 0,
    result_node_upper_bound: usize = 0,
};
pub const Adapter = struct {
    /// Canonical upstream module name, including the outer encoder prefix.
    module_name: []const u8,
    /// Existing native parameter name and NodeId, never renamed or copied.
    base_name: []const u8,
    weight: Id,
    a_name: []const u8,
    b_name: []const u8,
    magnitude_name: ?[]const u8,
    /// PEFT adapter_model.safetensors names (adapter namespace removed).
    a_saved_key: []const u8,
    b_saved_key: []const u8,
    magnitude_saved_key: ?[]const u8,
    a: Id,
    b: Id,
    magnitude: Id = nil,
    /// Detached live row norm. Also usable to initialize the magnitude vector
    /// before the first forward; its ancestors never depend on magnitude.
    norm: Id = nil,
    in_dim: u32,
    out_dim: u32,
    rank: u32,
    scale: f32,
};
pub const Use = struct {
    adapter: u32,
    source: Id,
    output: Id,
    input: Id,
    rows: u32,
    /// Zero-based occurrence within this shared module, not adapter index.
    occurrence: u32,
    mask: Id = nil,
    mask_name: ?[]const u8 = null,
    mask_shape: Shape,
    probability: f32,
    /// Stable hash of canonical module name + occurrence, independent of
    /// graph sorting and allocations. The replay tuple has separate domains.
    stream: u64,
};
pub const Result = struct {
    graph: ml.Graph,
    arena: std.heap.ArenaAllocator,
    adapters: []const Adapter,
    uses: []const Use,
    trainable_parameters: []const Id,
    /// Original NodeIds to semantic outputs; adapters may replace a linear
    /// that is referenced by an external seed/routing descriptor, not outputs.
    rewrites: []const Id,
    admission: Plan,

    pub fn deinit(self: *Result) void {
        self.graph.deinit();
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn remap(self: *const Result, original: Id) !Id {
        if (original == nil) return nil;
        if (original >= self.rewrites.len) return error.InvalidBoundaryPeftNode;
        return self.rewrites[original];
    }
};

fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.BoundaryPeftLimitExceeded;
}
fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.BoundaryPeftLimitExceeded;
}
fn bytes(rows: usize, dim: usize) !usize {
    return mul(try mul(rows, dim), @sizeOf(f32));
}
pub fn validateConfig(config: Config, limits: Limits) !void {
    if (config.rank == 0 or config.rank > limits.max_rank or
        !std.math.isFinite(config.alpha) or config.alpha <= 0 or
        !std.math.isFinite(config.dropout) or config.dropout < 0 or config.dropout >= 1 or
        config.targets.len == 0 or config.targets.len > 4096)
        return error.InvalidBoundaryPeftConfig;
    for (config.targets, 0..) |target, i| {
        if (target.len == 0 or target.len > 1024 or std.mem.indexOfScalar(u8, target, 0) != null)
            return error.InvalidBoundaryPeftTarget;
        for (config.targets[0..i]) |previous| if (std.mem.eql(u8, previous, target)) return error.DuplicateBoundaryPeftTarget;
    }
    if (limits.max_source_nodes == 0 or limits.max_result_nodes > std.math.maxInt(Id) or
        limits.max_adapters == 0 or limits.max_adapters > std.math.maxInt(u32) or
        limits.max_uses == 0 or limits.max_uses > std.math.maxInt(u32)) return error.InvalidBoundaryPeftConfig;
}

const Site = struct { id: Id, weight: Id, module: []const u8, canonical: []const u8, base_name: []const u8, rows: u32, in_dim: u32, out_dim: u32 };
fn linearSite(a: Allocator, graph: *const ml.Graph, id: Id) !?Site {
    const node = graph.node(id);
    const attrs = switch (node.op) {
        .fused_linear, .fused_linear_no_bias => |value| value,
        else => return null,
    };
    if (attrs.rows == 0 or attrs.in_dim == 0 or attrs.out_dim == 0 or
        attrs.rows > std.math.maxInt(i32) or attrs.in_dim > std.math.maxInt(i32) or attrs.out_dim > std.math.maxInt(i32))
        return error.InvalidBoundaryPeftShape;
    if (node.num_inputs != (if (node.op == .fused_linear) @as(u8, 3) else 2)) return error.InvalidBoundaryPeftGraph;
    const weight = graph.node(node.inputs[1]);
    if (weight.op != .parameter) return null;
    if (!weight.output_shape.eq(Shape.init(.f32, &.{ attrs.out_dim, attrs.in_dim })) or
        !graph.node(node.inputs[0]).output_shape.eq(Shape.init(.f32, &.{ attrs.rows, attrs.in_dim })) or
        !node.output_shape.eq(Shape.init(.f32, &.{ attrs.rows, attrs.out_dim }))) return error.InvalidBoundaryPeftShape;
    if (node.op == .fused_linear and !graph.node(node.inputs[2]).output_shape.eq(Shape.init(.f32, &.{attrs.out_dim}))) return error.InvalidBoundaryPeftShape;
    const name = graph.parameterName(weight);
    if (!std.mem.endsWith(u8, name, ".weight")) return null;
    const module = name[0 .. name.len - ".weight".len];
    // Native DeBERTa graph removes the GLiNER wrapper's outer `encoder.`.
    const canonical = if (std.mem.startsWith(u8, module, "encoder.layer."))
        try std.fmt.allocPrint(a, "encoder.{s}", .{module})
    else
        module;
    return .{ .id = id, .weight = node.inputs[1], .module = module, .canonical = canonical, .base_name = name, .rows = attrs.rows, .in_dim = attrs.in_dim, .out_dim = attrs.out_dim };
}
fn hasPrefix(name: []const u8, prefix: []const u8) bool {
    return std.mem.eql(u8, name, prefix) or (std.mem.startsWith(u8, name, prefix) and name.len > prefix.len and name[prefix.len] == '.');
}
fn matches(site: Site, target: []const u8) bool {
    return matchesModule(site.module, site.canonical, target);
}

/// The same target semantics are used by graph injection and by the complete
/// model adapter layout, whose inactive modules have no graph NodeId.
pub fn matchesModule(native: []const u8, canonical: []const u8, target: []const u8) bool {
    if (std.mem.eql(u8, target, native) or std.mem.eql(u8, target, canonical)) return true;
    const encoder = std.mem.startsWith(u8, canonical, "encoder.");
    const local = canonical[(std.mem.lastIndexOfScalar(u8, canonical, '.') orelse 0) + 1 ..];
    if (std.mem.eql(u8, target, "encoder")) {
        if (!encoder) return false;
        for ([_][]const u8{ "query", "key", "value", "dense" }) |pattern| if (std.mem.indexOf(u8, local, pattern) != null) return true;
        return false;
    }
    // The public encoder.<local-name-pattern> aliases are explicit and small.
    for ([_][]const u8{ "query", "key", "value", "dense" }) |pattern| {
        if (std.mem.startsWith(u8, target, "encoder.") and std.mem.eql(u8, target[8..], pattern))
            return encoder and std.mem.indexOf(u8, local, pattern) != null;
    }
    if (std.mem.eql(u8, target, "all_task_heads")) {
        for ([_][]const u8{ "boundary_head", "classifier", "record_decoder", "relation_scorer" }) |prefix| if (hasPrefix(canonical, prefix)) return true;
    }
    for ([_]struct { alias: []const u8, prefix: []const u8 }{
        .{ .alias = "extractive_head", .prefix = "boundary_head" },
        .{ .alias = "classification_head", .prefix = "classifier" },
        .{ .alias = "record_head", .prefix = "record_decoder" },
        .{ .alias = "relation_head", .prefix = "relation_scorer" },
    }) |entry| if (std.mem.eql(u8, target, entry.alias) or std.mem.eql(u8, target, entry.prefix)) return hasPrefix(canonical, entry.prefix);
    return false;
}

const Scan = struct { sites: []const Site, admission: Plan };
fn scan(a: Allocator, graph: *const ml.Graph, config: Config, limits: Limits) !Scan {
    try validateConfig(config, limits);
    if (graph.nodes.items.len == 0 or graph.nodes.items.len > limits.max_source_nodes or graph.nodes.items.len > std.math.maxInt(Id))
        return error.BoundaryPeftLimitExceeded;
    // This injector accepts the append-order source builders. It deliberately
    // rejects an already rewritten/non-topological graph instead of adapting
    // adapter branches a second time or guessing dependencies.
    for (graph.nodes.items) |node| {
        if (node.num_inputs > node.inputs.len or node.output_shape.rank_ > ml.shape.max_rank) return error.InvalidBoundaryPeftGraph;
        if (node.op == .parameter) {
            const attrs = node.op.parameter;
            const end = try add(attrs.name_offset, attrs.name_len);
            if (end > graph.string_table.items.len) return error.InvalidBoundaryPeftGraph;
            const name = graph.parameterName(&node);
            if (std.mem.indexOf(u8, name, ".lora_A.") != null or std.mem.indexOf(u8, name, ".lora_B.") != null or
                std.mem.indexOf(u8, name, ".lora_magnitude_vector.") != null) return error.BoundaryPeftAlreadyInjected;
        }
    }
    for (graph.nodes.items, 0..) |node, id| {
        for (node.getInputs()) |input| if (input == nil or input >= id) return error.InvalidBoundaryPeftGraph;
        if (node.vjp_alternate != nil and node.vjp_alternate >= id) return error.InvalidBoundaryPeftGraph;
    }
    for (graph.outputs.items) |id| if (id >= graph.nodes.items.len) return error.InvalidBoundaryPeftGraph;
    const matched = try a.alloc(bool, config.targets.len);
    @memset(matched, false);
    var sites = std.ArrayListUnmanaged(Site).empty;
    var seen = std.StringHashMapUnmanaged(Id).empty;
    var result = Plan{ .result_node_upper_bound = graph.nodes.items.len };
    for (graph.nodes.items, 0..) |_, index| {
        const site = (try linearSite(a, graph, @intCast(index))) orelse continue;
        var selected = false;
        for (config.targets, matched) |target, *hit| if (matches(site, target)) {
            hit.* = true;
            selected = true;
        };
        if (!selected) continue;
        if (seen.get(site.base_name)) |weight| {
            if (weight != site.weight) return error.AmbiguousBoundaryPeftWeight;
        } else {
            try seen.put(a, site.base_name, site.weight);
            result.adapters = try add(result.adapters, 1);
            result.adapter_bytes = try add(result.adapter_bytes, try add(try bytes(config.rank, site.in_dim), try bytes(site.out_dim, config.rank)));
            if (config.kind == .dora) {
                result.adapter_bytes = try add(result.adapter_bytes, try bytes(1, site.out_dim));
                result.added_forward_upper_bound_bytes = try add(result.added_forward_upper_bound_bytes, try add(try mul(try bytes(site.out_dim, site.in_dim), 8), try bytes(8, site.out_dim)));
            }
            result.result_node_upper_bound = try add(result.result_node_upper_bound, 32);
        }
        result.uses = try add(result.uses, 1);
        if (config.mode == .train and config.dropout > 0) result.dropout_bytes = try add(result.dropout_bytes, try bytes(site.rows, site.in_dim));
        result.added_forward_upper_bound_bytes = try add(result.added_forward_upper_bound_bytes, try add(try bytes(site.rows, site.in_dim), try add(try bytes(site.rows, config.rank), try mul(try bytes(site.rows, site.out_dim), 24))));
        result.result_node_upper_bound = try add(result.result_node_upper_bound, 64);
        if (result.adapters > limits.max_adapters or result.uses > limits.max_uses or
            result.adapter_bytes > limits.max_adapter_bytes or result.dropout_bytes > limits.max_dropout_bytes or
            result.added_forward_upper_bound_bytes > limits.max_added_forward_bytes or result.result_node_upper_bound > limits.max_result_nodes)
            return error.BoundaryPeftLimitExceeded;
        try sites.append(a, site);
    }
    for (matched) |hit| if (!hit) return error.BoundaryPeftTargetNotResolved;
    return .{ .sites = sites.items, .admission = result };
}

pub fn plan(a: Allocator, graph: *const ml.Graph, config: Config, limits: Limits) !Plan {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    return (try scan(arena.allocator(), graph, config, limits)).admission;
}

fn expand(b: *ml.Builder, vector: Id, row_count: u32, dim: u32) !Id {
    return b.graph.addNode(.{
        .op = .{ .broadcast_in_dim = .{ .target_shape = Shape.init(.f32, &.{ row_count, dim }), .broadcast_axes = .{ 1, 0, 0, 0, 0, 0, 0, 0 }, .num_axes = 1 } },
        .output_shape = Shape.init(.f32, &.{ row_count, dim }),
        .inputs = .{ vector, nil, nil, nil },
        .num_inputs = 1,
    });
}
fn ownedName(a: Allocator, module: []const u8, suffix: []const u8) ![]const u8 {
    const name = try std.fmt.allocPrint(a, "base_model.model.{s}.{s}", .{ module, suffix });
    if (name.len > std.math.maxInt(u16)) return error.BoundaryPeftLimitExceeded;
    return name;
}
fn makeAdapter(a: Allocator, b: *ml.Builder, site: Site, config: Config) !Adapter {
    const module = try a.dupe(u8, site.canonical);
    const a_name = try ownedName(a, module, "lora_A.default.weight");
    const b_name = try ownedName(a, module, "lora_B.default.weight");
    const magnitude_name = if (config.kind == .dora) try ownedName(a, module, "lora_magnitude_vector.default.weight") else null;
    var adapter = Adapter{
        .module_name = module,
        .base_name = try a.dupe(u8, site.base_name),
        .weight = site.weight,
        .a_name = a_name,
        .b_name = b_name,
        .magnitude_name = magnitude_name,
        .a_saved_key = try ownedName(a, module, "lora_A.weight"),
        .b_saved_key = try ownedName(a, module, "lora_B.weight"),
        .magnitude_saved_key = if (config.kind == .dora) try ownedName(a, module, "lora_magnitude_vector") else null,
        .a = try b.parameter(a_name, Shape.init(.f32, &.{ config.rank, site.in_dim })),
        .b = try b.parameter(b_name, Shape.init(.f32, &.{ site.out_dim, config.rank })),
        .in_dim = site.in_dim,
        .out_dim = site.out_dim,
        .rank = config.rank,
        .scale = config.alpha / @as(f32, @floatFromInt(config.rank)),
    };
    if (magnitude_name) |name| {
        adapter.magnitude = try b.parameter(name, Shape.init(.f32, &.{site.out_dim}));
        const delta = try b.matmul(adapter.b, adapter.a);
        const direction = try b.add(site.weight, try b.mul(delta, try b.scalarConst(.f32, adapter.scale)));
        const squared = try b.mul(direction, direction);
        const sum = try b.reduceSum(squared, &.{1});
        adapter.norm = try b.stopGradient(try b.reshape(try b.sqrt(sum), Shape.init(.f32, &.{site.out_dim})));
    }
    return adapter;
}

/// Clone and adapt an append-order source graph. Original parameters and node
/// IDs are retained; consumers and outputs use the rewritten semantic result.
/// Run normal lowering before execution (appended adapters add forward edges).
pub fn inject(a: Allocator, source: *const ml.Graph, config: Config, limits: Limits) !Result {
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const scratch = arena.allocator();
    const scanned = try scan(scratch, source, config, limits);
    var graph = ml.Graph.init(a);
    errdefer graph.deinit();
    try graph.nodes.appendSlice(a, source.nodes.items);
    try graph.constant_pool.appendSlice(a, source.constant_pool.items);
    try graph.string_table.appendSlice(a, source.string_table.items);
    try graph.outputs.appendSlice(a, source.outputs.items);
    try graph.parameters.appendSlice(a, source.parameters.items);
    var b = ml.Builder.init(&graph);
    const rewrites = try scratch.alloc(Id, source.nodes.items.len);
    for (rewrites, 0..) |*id, index| id.* = @intCast(index);
    var adapters = std.ArrayListUnmanaged(Adapter).empty;
    var uses = std.ArrayListUnmanaged(Use).empty;
    var trainable = std.ArrayListUnmanaged(Id).empty;
    var by_weight = std.AutoHashMapUnmanaged(Id, u32).empty;
    const occurrences = try scratch.alloc(u32, scanned.admission.adapters);
    @memset(occurrences, 0);
    var site_index: usize = 0;
    for (source.nodes.items, 0..) |original, index| {
        var node = original;
        for (node.inputs[0..node.num_inputs]) |*input| input.* = rewrites[input.*];
        if (node.vjp_alternate != nil) node.vjp_alternate = rewrites[node.vjp_alternate];
        graph.nodes.items[index] = node;
        if (site_index == scanned.sites.len or scanned.sites[site_index].id != index) continue;
        const site = scanned.sites[site_index];
        site_index += 1;
        const adapter_index = by_weight.get(site.weight) orelse blk: {
            const adapter = try makeAdapter(scratch, &b, site, config);
            const adapter_index: u32 = @intCast(adapters.items.len);
            try adapters.append(scratch, adapter);
            try by_weight.put(scratch, site.weight, adapter_index);
            try trainable.appendSlice(scratch, &.{ adapter.a, adapter.b });
            if (adapter.magnitude != nil) try trainable.append(scratch, adapter.magnitude);
            break :blk adapter_index;
        };
        const adapter = adapters.items[adapter_index];
        const occurrence = occurrences[adapter_index];
        occurrences[adapter_index] += 1;
        const input = node.inputs[0];
        const mask_shape = Shape.init(.f32, &.{ site.rows, site.in_dim });
        var use = Use{
            .adapter = adapter_index,
            .source = @intCast(index),
            .output = nil,
            .input = input,
            .rows = site.rows,
            .occurrence = occurrence,
            .mask_shape = mask_shape,
            .probability = config.dropout,
            .stream = useStream(adapter.module_name, occurrence),
        };
        var adapted_input = input;
        if (config.mode == .train and config.dropout > 0) {
            const name = try std.fmt.allocPrint(scratch, "__boundary_peft_mask.{s}.{d}", .{ adapter.module_name, occurrence });
            if (name.len > std.math.maxInt(u16)) return error.BoundaryPeftLimitExceeded;
            use.mask_name = name;
            use.mask = try b.parameter(name, mask_shape);
            adapted_input = try b.mul(input, use.mask);
        }
        const projected = try b.linearNoBias(adapted_input, adapter.a, site.rows, site.in_dim, config.rank);
        const delta = try b.linearNoBias(projected, adapter.b, site.rows, config.rank, site.out_dim);
        const scaled_delta = try b.mul(delta, try b.scalarConst(.f32, adapter.scale));
        if (config.kind == .lora) {
            use.output = try b.add(@intCast(index), scaled_delta);
        } else {
            const scale = try b.div(adapter.magnitude, adapter.norm);
            const correction = try b.sub(scale, try b.scalarConst(.f32, 1));
            const base_unbiased = if (use.mask != nil)
                try b.linearNoBias(adapted_input, site.weight, site.rows, site.in_dim, site.out_dim)
            else if (node.op == .fused_linear)
                try b.sub(@intCast(index), try expand(&b, node.inputs[2], site.rows, site.out_dim))
            else
                @as(Id, @intCast(index));
            const adjusted = try b.add(try b.mul(base_unbiased, try expand(&b, correction, site.rows, site.out_dim)), try b.mul(scaled_delta, try expand(&b, scale, site.rows, site.out_dim)));
            use.output = try b.add(@intCast(index), adjusted);
        }
        rewrites[index] = use.output;
        try uses.append(scratch, use);
        if (graph.nodes.items.len > scanned.admission.result_node_upper_bound) return error.BoundaryPeftLimitExceeded;
    }
    for (graph.outputs.items) |*id| id.* = rewrites[id.*];
    return .{ .graph = graph, .arena = arena, .adapters = adapters.items, .uses = uses.items, .trainable_parameters = trainable.items, .rewrites = rewrites, .admission = scanned.admission };
}

fn mix(value: u64) u64 {
    var x = value +% 0x9e3779b97f4a7c15;
    x = (x ^ (x >> 30)) *% 0xbf58476d1ce4e5b9;
    x = (x ^ (x >> 27)) *% 0x94d049bb133111eb;
    return x ^ (x >> 31);
}
fn useStream(module: []const u8, occurrence: u32) u64 {
    var hash: u64 = 0xcbf29ce484222325;
    for (module) |byte| hash = (hash ^ byte) *% 0x100000001b3;
    return mix(hash ^ mix(@as(u64, occurrence) +% 0x757365));
}
pub const Replay = struct { seed: u64, optimizer_step: u64, micro_batch: u64, replica: u64 = 0 };
/// Counter replay ABI `boundary_peft_site_element_v1`. Explicit masks are
/// portable and replayable; this does not claim equivalence to Torch's RNG.
pub fn fillDropout(use: Use, replay: Replay, output: []f32) !void {
    const count = use.mask_shape.numElements() orelse return error.InvalidBoundaryPeftShape;
    if (count < 0 or count != output.len or !std.math.isFinite(use.probability) or use.probability < 0 or use.probability >= 1)
        return error.InvalidBoundaryPeftShape;
    if (use.mask == nil or use.probability == 0) {
        @memset(output, 1);
        return;
    }
    const stream = mix(replay.seed ^ 0x73656564) ^ mix(replay.optimizer_step ^ 0x73746570) ^
        mix(replay.micro_batch ^ 0x6d6963726f) ^ mix(replay.replica ^ 0x7265706c696361) ^ use.stream;
    const threshold: u64 = @intFromFloat(@as(f64, use.probability) * 4294967296.0);
    const scale: f32 = 1 / (1 - use.probability);
    for (output, 0..) |*value, index| value.* = if ((mix(stream ^ mix(index)) >> 32) < threshold) 0 else scale;
}

pub const InitialWeights = struct {
    allocator: Allocator,
    a: []f32,
    b: []f32,
    magnitude: ?[]f32,
    pub fn deinit(self: *InitialWeights) void {
        self.allocator.free(self.a);
        self.allocator.free(self.b);
        if (self.magnitude) |values| self.allocator.free(values);
        self.* = undefined;
    }
};
/// Kaiming-uniform A, zero B, and base row norms for DoRA. Random bits use a
/// native deterministic seed, not PEFT's RNG. Imported adapters bind their
/// serialized A/B/magnitude directly instead. Nonfinite/zero base norms fail.
pub fn initialize(a: Allocator, adapter: Adapter, base: []const f32, seed: u64) !InitialWeights {
    return initializeModule(a, adapter.module_name, adapter.in_dim, adapter.out_dim, adapter.rank, adapter.magnitude != nil, base, seed);
}

/// Initialize a configured module before any schema-specific graph exists.
/// This uses exactly the graph adapter's name-derived random stream.
pub fn initializeModule(a: Allocator, module_name: []const u8, in_dim: u32, out_dim: u32, rank: u32, dora: bool, base: []const f32, seed: u64) !InitialWeights {
    if (base.len != try mul(in_dim, out_dim) or in_dim == 0 or out_dim == 0 or rank == 0)
        return error.InvalidBoundaryPeftShape;
    for (base) |value| if (!std.math.isFinite(value)) return error.NonFiniteBoundaryPeftWeight;
    const left = try a.alloc(f32, try mul(rank, in_dim));
    errdefer a.free(left);
    const right = try a.alloc(f32, try mul(out_dim, rank));
    errdefer a.free(right);
    @memset(right, 0);
    const bound = 1 / @sqrt(@as(f32, @floatFromInt(in_dim)));
    for (left, 0..) |*value, index| {
        const bits: u32 = @truncate(mix(seed ^ useStream(module_name, 0) ^ mix(index)) >> 40);
        value.* = (2 * (@as(f32, @floatFromInt(bits)) / 16777216.0) - 1) * bound;
    }
    const magnitude = if (dora) try a.alloc(f32, out_dim) else null;
    errdefer if (magnitude) |values| a.free(values);
    if (magnitude) |values| for (values, 0..) |*value, row| {
        var sum: f64 = 0;
        for (base[row * in_dim ..][0..in_dim]) |weight| sum += @as(f64, weight) * weight;
        value.* = @floatCast(@sqrt(sum));
        if (!std.math.isFinite(value.*) or value.* <= 0) return error.InvalidBoundaryPeftNorm;
    };
    return .{ .allocator = a, .a = left, .b = right, .magnitude = magnitude };
}
