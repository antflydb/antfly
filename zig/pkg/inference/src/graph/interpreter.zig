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

// Eager graph interpreter: executes a traced Graph node-by-node through
// a real ComputeBackend. Walks the append-only DAG in topological order
// (= array order), dispatches each fused op to the corresponding VTable
// method, and frees intermediate tensors once their last consumer has
// executed.
//
// Achieves bit-exact parity with eager execution: same backend + same
// weights + same op sequence = identical results.
//
// Stateful ops (paged attention, embedding lookup, MoE routing) receive
// runtime data via ExecuteOptions side channels.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const ml = @import("ml");
const ops_mod = @import("../ops/ops.zig");
const contracts = @import("backend_contracts.zig");
const transpose_utils = @import("transpose_utils.zig");
const buffer_plan_mod = @import("buffer_plan.zig");
const runtime_slice = @import("runtime_slice.zig");

const Graph = ml.graph.Graph;
const Node = ml.graph.Node;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;
const OpCode = ml.graph.OpCode;
const Shape = ml.graph.Shape;

const CT = contracts.CT;
const ComputeBackend = ops_mod.ComputeBackend;

pub const InterpreterError = error{
    /// The op requires runtime data (masks, indices, etc.) that was not
    /// provided. Use runtime_inputs to supply them.
    MissingRuntimeInput,
    /// Encountered a primitive op that has no direct backend mapping.
    /// The graph should be lowered or only fused ops should be used.
    UnsupportedPrimitiveOp,
};

/// Optional runtime data injected at execution time for nodes that
/// represent dynamic inputs (embedding indices, attention masks, etc.).
pub const RuntimeInput = struct {
    node_id: NodeId,
    value: CT,
};

/// Pre-computed graph analysis that can be cached across executions.
/// For a given graph these are invariant — recomputing them every
/// decode step is pure overhead.
pub const CachedAnalysis = struct {
    reachable: []const bool,
    last_use: []const u32,
    /// Last use of the complete backing-storage alias group for each node.
    /// This is stricter than `last_use` and is used only to decide whether an
    /// in-place consume/donation is safe.
    donation_last_use: []const u32,
    runtime_shape_capture: []const bool,
    gather_add_bias_preserve: []const bool,

    /// Compute and allocate a CachedAnalysis for the given graph.
    pub fn compute(allocator: std.mem.Allocator, graph: *const Graph) !CachedAnalysis {
        const reachable = try computeReachable(allocator, graph);
        errdefer allocator.free(reachable);
        const last_use = try computeLastUse(allocator, graph, reachable);
        errdefer allocator.free(last_use);
        const donation_last_use = try computeDonationLastUse(allocator, graph, reachable, last_use);
        errdefer allocator.free(donation_last_use);
        const runtime_shape_capture = try computeRuntimeShapeCaptureSet(allocator, graph);
        errdefer allocator.free(runtime_shape_capture);
        const gather_add_bias_preserve = try computeGatherAddBiasPreserveSet(allocator, graph, reachable);
        return .{
            .reachable = reachable,
            .last_use = last_use,
            .donation_last_use = donation_last_use,
            .runtime_shape_capture = runtime_shape_capture,
            .gather_add_bias_preserve = gather_add_bias_preserve,
        };
    }

    /// Compute analysis for a bounded capture. This executes only the
    /// dependency closure needed to materialize target nodes instead of the
    /// full graph-output closure.
    pub fn computeForTargets(
        allocator: std.mem.Allocator,
        graph: *const Graph,
        target_node_ids: []const NodeId,
    ) !CachedAnalysis {
        const reachable = try computeReachableFromNodes(allocator, graph, target_node_ids);
        errdefer allocator.free(reachable);
        const last_use = try computeLastUse(allocator, graph, reachable);
        errdefer allocator.free(last_use);
        const donation_last_use = try computeDonationLastUse(allocator, graph, reachable, last_use);
        errdefer allocator.free(donation_last_use);
        const runtime_shape_capture = try computeRuntimeShapeCaptureSet(allocator, graph);
        errdefer allocator.free(runtime_shape_capture);
        const gather_add_bias_preserve = try computeGatherAddBiasPreserveSet(allocator, graph, reachable);
        return .{
            .reachable = reachable,
            .last_use = last_use,
            .donation_last_use = donation_last_use,
            .runtime_shape_capture = runtime_shape_capture,
            .gather_add_bias_preserve = gather_add_bias_preserve,
        };
    }

    /// Free the backing arrays.
    pub fn deinit(self: *CachedAnalysis, allocator: std.mem.Allocator) void {
        allocator.free(self.reachable);
        allocator.free(self.last_use);
        allocator.free(self.donation_last_use);
        allocator.free(self.runtime_shape_capture);
        allocator.free(self.gather_add_bias_preserve);
        self.reachable = &.{};
        self.last_use = &.{};
        self.donation_last_use = &.{};
        self.runtime_shape_capture = &.{};
        self.gather_add_bias_preserve = &.{};
    }
};

/// Side channels and runtime data for graph execution. Stateful ops
/// (paged attention, embedding lookup, MoE routing) pull from these
/// rather than from the graph, since their data varies per invocation.
pub const ExecuteOptions = struct {
    /// Per-node CT overrides (e.g. pre-computed tensors).
    runtime_inputs: ?[]const RuntimeInput = null,

    /// Buffer donation flags, parallel to runtime_inputs. When
    /// donate[i] is true the interpreter transfers ownership of
    /// runtime_inputs[i].value into the graph — the buffer may be
    /// overwritten by a downstream op and must NOT be freed by the
    /// caller afterward.  Donated buffers that are not consumed as
    /// outputs are freed by the interpreter at cleanup.
    ///
    /// This follows the GoMLX pattern: in the decode loop the same-
    /// shaped KV tensors are passed every step, and donation lets
    /// backends reuse them without allocating.
    donate: ?[]const bool = null,

    /// Attention context for GQA paged/causal attention nodes.
    /// The interpreter auto-increments layer_index for each
    /// successive attention node encountered during execution.
    attention: ?contracts.AttentionContext = null,

    /// Token IDs for embedding lookup nodes (consumed in encounter
    /// order). Shared across all embedding ops in the graph.
    embedding_ids: ?[]const i64 = null,

    /// Attention mask for scaled dot-product attention ops.
    /// Shape: [batch, seq_len] where 0 = masked, 1 = attend.
    sdpa_mask: ?[]const i64 = null,

    /// Encoder mask for cross-attention ops.
    cross_attention_mask: ?[]const i64 = null,

    /// Pre-computed graph analysis (reachable set + last-use).  When
    /// provided, execute() skips recomputing these per-call — a win
    /// for the decode loop where the graph never changes.
    cached_analysis: ?CachedAnalysis = null,

    /// Pre-computed graph buffer/lifetime plan. This is invariant for a fixed
    /// graph and partition plan, so compiled training sessions can borrow it
    /// across repeated steps.
    cached_buffer_plan: ?*const buffer_plan_mod.BufferPlan = null,

    /// Skip Metal graph-fusion probes for graphs whose hot path is already
    /// covered by backend primitive/runtime commands.
    skip_metal_fused_patterns: bool = false,

    /// Collect detailed partition-executor counters. Training uses the graph
    /// executor as a single-device fast path and can skip this unless stats
    /// tracing is explicitly enabled.
    collect_partition_stats: bool = true,

    /// Preserve runtime input residency instead of eagerly materializing them
    /// on the partition backend. This keeps graph-exec training semantically
    /// aligned with the direct interpreter, where labels, masks, and borrowed
    /// weights stay in the representation supplied by the caller.
    preserve_runtime_input_residency: bool = false,
};

/// Result of graph execution. Caller owns the output tensors and must
/// free them via the backend.
pub const ExecutionResult = struct {
    /// Output tensors in the same order as graph.outputs.
    outputs: []CT,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ExecutionResult, cb: *const ComputeBackend) void {
        for (self.outputs, 0..) |ct, idx| {
            if (containsCt(self.outputs[0..idx], ct)) continue;
            cb.free(ct);
        }
        self.allocator.free(self.outputs);
    }
};

const OpProfileEntry = struct {
    name: []const u8,
    count: usize = 0,
    total_ns: u64 = 0,
};

const OpProfiler = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayListUnmanaged(OpProfileEntry) = .empty,

    fn deinit(self: *OpProfiler) void {
        self.entries.deinit(self.allocator);
    }

    fn add(self: *OpProfiler, name: []const u8, ns: u64) !void {
        for (self.entries.items) |*entry| {
            if (std.mem.eql(u8, entry.name, name)) {
                entry.count += 1;
                entry.total_ns += ns;
                return;
            }
        }
        try self.entries.append(self.allocator, .{
            .name = name,
            .count = 1,
            .total_ns = ns,
        });
    }

    fn print(self: *OpProfiler) void {
        std.debug.print("[graph-op-profile] top ops by wall time\n", .{});
        var printed: usize = 0;
        var last_ns: ?u64 = null;
        var last_name: []const u8 = "";
        while (printed < 24) : (printed += 1) {
            var best: ?OpProfileEntry = null;
            for (self.entries.items) |entry| {
                if (last_ns) |limit| {
                    if (entry.total_ns > limit) continue;
                    if (entry.total_ns == limit and std.mem.order(u8, entry.name, last_name) != .gt) continue;
                }
                if (best == null or entry.total_ns > best.?.total_ns or
                    (entry.total_ns == best.?.total_ns and std.mem.order(u8, entry.name, best.?.name) == .lt))
                {
                    best = entry;
                }
            }
            const entry = best orelse break;
            last_ns = entry.total_ns;
            last_name = entry.name;
            const avg_ms = if (entry.count == 0) 0.0 else nsToMs(entry.total_ns) / @as(f64, @floatFromInt(entry.count));
            std.debug.print(
                "[graph-op-profile] {s}: count={} total_ms={d:.3} avg_ms={d:.3}\n",
                .{ entry.name, entry.count, nsToMs(entry.total_ns), avg_ms },
            );
        }
        if (printed == 0) std.debug.print("[graph-op-profile] no profiled ops\n", .{});
    }
};

pub const CapturedValuesResult = struct {
    values: []CT,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *CapturedValuesResult, cb: *const ComputeBackend) void {
        for (self.values) |ct| cb.free(ct);
        self.allocator.free(self.values);
    }
};

fn containsCt(values: []const CT, needle: CT) bool {
    for (values) |value| {
        if (value == needle) return true;
    }
    return false;
}

fn nullCtAliases(values: []?CT, needle: CT) void {
    for (values) |*value| {
        if (value.* == needle) value.* = null;
    }
}

fn graphExecTraceEnabled() bool {
    const value = platform.env.getenv("TERMITE_GRAPH_EXEC_TRACE") orelse return false;
    return value.len > 0 and !std.mem.eql(u8, value, "0") and !std.ascii.eqlIgnoreCase(value, "false");
}

fn graphOpProfileEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_GRAPH_OP_PROFILE", false);
}

fn graphFiniteTraceEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_GRAPH_FINITE_TRACE", false);
}

fn graphFiniteTraceMaxElems() usize {
    const value = platform.env.getenv("TERMITE_GRAPH_FINITE_TRACE_MAX_ELEMS") orelse return 200_000;
    return std.fmt.parseUnsigned(usize, value, 10) catch 200_000;
}

fn graphOpSlowThresholdNs() u64 {
    const value = platform.env.getenv("TERMITE_GRAPH_OP_SLOW_MS") orelse return 0;
    const ms = std.fmt.parseFloat(f64, value) catch return 0;
    if (ms <= 0) return 0;
    return @intFromFloat(ms * 1_000_000.0);
}

fn checkNodeFinite(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    node_id: NodeId,
    ct: CT,
    max_elems: usize,
) !void {
    const shape = cb.tensorShape(ct, allocator) catch return;
    defer allocator.free(shape);
    var elem_count: usize = 1;
    for (shape) |dim| {
        if (dim <= 0) return;
        const dim_usize: usize = @intCast(dim);
        elem_count = std.math.mul(usize, elem_count, dim_usize) catch return;
    }
    if (max_elems > 0 and elem_count > max_elems) return;
    const data = cb.toFloat32(ct, allocator) catch return;
    defer allocator.free(data);
    for (data, 0..) |value, idx| {
        if (!std.math.isFinite(value)) {
            const node = graph.node(node_id);
            std.debug.print(
                "[graph-finite] first_nonfinite node={} op={s} idx={} value={d} shape={any} declared_shape={any}\n",
                .{
                    node_id,
                    @tagName(std.meta.activeTag(node.op)),
                    idx,
                    value,
                    shape,
                    node.output_shape,
                },
            );
            for (node.getInputs(), 0..) |input_id, input_idx| {
                if (input_id == null_node or input_id >= graph.nodeCount()) continue;
                const input_node = graph.node(input_id);
                std.debug.print(
                    "[graph-finite] input{} id={} op={s} shape={any}\n",
                    .{ input_idx, input_id, @tagName(std.meta.activeTag(input_node.op)), input_node.output_shape },
                );
            }
            return error.NonFiniteGraphNode;
        }
    }
    if (platform.env.getenvBoolDefault("TERMITE_GRAPH_ABS_TRACE", false)) {
        var abs_sum: f64 = 0;
        for (data) |value| abs_sum += @abs(value);
        std.debug.print("[abs] {d} {d:.6}\n", .{ node_id, abs_sum });
    }
    if (graphZeroTraceEnabled()) {
        var abs_sum: f64 = 0;
        for (data) |value| abs_sum += @abs(value);
        if (abs_sum == 0 and data.len > 0) {
            const node = graph.node(node_id);
            std.debug.print(
                "[graph-zero] node={} op={s} numel={} declared_shape={any}\n",
                .{ node_id, @tagName(std.meta.activeTag(node.op)), data.len, node.output_shape },
            );
        }
    }
    if (platform.env.getenv("TERMITE_GRAPH_NODE_VALUES")) |spec| {
        var it = std.mem.splitScalar(u8, spec, ',');
        while (it.next()) |tok| {
            const want = std.fmt.parseUnsigned(u32, std.mem.trim(u8, tok, " "), 10) catch continue;
            if (want != node_id) continue;
            var abs_sum: f64 = 0;
            for (data) |value| abs_sum += @abs(value);
            const node = graph.node(node_id);
            std.debug.print(
                "[node-values] node={} op={s} ct=0x{x} len={} first4={any} abs_sum={d:.6}\n",
                .{ node_id, @tagName(std.meta.activeTag(node.op)), @intFromPtr(ct), data.len, data[0..@min(4, data.len)], abs_sum },
            );
        }
    }
}

fn graphZeroTraceEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_GRAPH_ZERO_TRACE", false);
}

fn graphExecDiag(comptime fmt: []const u8, args: anytype) void {
    std.debug.print("[graph-exec] " ++ fmt ++ "\n", args);
}

fn graphNowNs() u64 {
    return platform.time.monotonicNs();
}

fn graphElapsedNs(start_ns: u64, end_ns: u64) u64 {
    if (end_ns <= start_ns) return 0;
    return end_ns - start_ns;
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000_000.0;
}

fn currentResidentBytes() usize {
    return platform.time.residentBytes();
}

/// Compute which nodes are reachable from graph outputs (walking
/// backward through inputs). Unreachable nodes (e.g. decomposed
/// primitive subgraphs stored in vjp_alternate) are skipped during
/// execution.
pub fn computeReachable(allocator: std.mem.Allocator, graph: *const Graph) ![]bool {
    const count = graph.nodeCount();
    const reachable = try allocator.alloc(bool, count);
    @memset(reachable, false);

    // Seed with output nodes
    for (graph.outputs.items) |out_id| {
        markReachable(graph, reachable, out_id);
    }

    return reachable;
}

/// Compute which nodes are reachable from an explicit target set. This is used
/// by debug/artifact capture paths that should not execute the full model tail.
pub fn computeReachableFromNodes(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    target_node_ids: []const NodeId,
) ![]bool {
    const count = graph.nodeCount();
    const reachable = try allocator.alloc(bool, count);
    @memset(reachable, false);

    for (target_node_ids) |node_id| {
        markReachable(graph, reachable, node_id);
    }

    return reachable;
}

fn markReachable(graph: *const Graph, reachable: []bool, id: NodeId) void {
    if (id == null_node or id >= reachable.len) return;
    if (reachable[id]) return; // already visited

    reachable[id] = true;

    const n = graph.node(id);
    for (n.getInputs()) |input_id| {
        markReachable(graph, reachable, input_id);
    }
    // Note: we do NOT follow vjp_alternate — those are for autograd only.
}

/// Compute the last node index that uses each node as an input.
/// Used for liveness-based tensor freeing.
pub fn computeLastUse(allocator: std.mem.Allocator, graph: *const Graph, reachable: []const bool) ![]u32 {
    const count = graph.nodeCount();
    const last_use = try allocator.alloc(u32, count);
    @memset(last_use, std.math.maxInt(u32)); // "no use" sentinel

    for (0..count) |i| {
        if (!reachable[i]) continue;
        const n = graph.node(@intCast(i));
        for (n.getInputs()) |input_id| {
            if (input_id != null_node and input_id < count) {
                last_use[input_id] = @intCast(i);
            }
        }
    }

    // Output nodes are live beyond the graph — mark them as never freed
    for (graph.outputs.items) |out_id| {
        last_use[out_id] = std.math.maxInt(u32);
    }

    return last_use;
}

/// Compute the last use of every backing-storage alias group.
///
/// A native reshape is a zero-copy view. Looking only at the reshape node's
/// own last consumer can therefore authorize an in-place op while the source
/// tensor still has a future consumer. That future consumer then observes the
/// mutated bytes. This occurs naturally in multi-head loss graphs: one branch
/// consumes a flattened encoder view before the next branch creates its own
/// view of the same encoder output.
///
/// Keep ordinary `last_use` for freeing individual handles. For donation,
/// group zero-copy pass-through/view nodes with their input and use the latest
/// consumer anywhere in the group.
fn computeDonationLastUse(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    reachable: []const bool,
    last_use: []const u32,
) ![]u32 {
    const count = graph.nodeCount();
    const roots = try allocator.alloc(NodeId, count);
    defer allocator.free(roots);
    for (roots, 0..) |*root, i| root.* = @intCast(i);

    for (0..count) |i| {
        if (!reachable[i]) continue;
        const node_id: NodeId = @intCast(i);
        const node = graph.node(node_id);
        const aliases_input = switch (node.op) {
            .reshape,
            .transpose,
            .broadcast_in_dim,
            .slice,
            .convert_dtype,
            .fused_eval_tensor,
            .fused_to_float32,
            => true,
            else => false,
        };
        if (!aliases_input or node.num_inputs == 0) continue;
        const input_id = node.inputs[0];
        if (input_id == null_node or input_id >= count) continue;
        roots[i] = roots[@intCast(input_id)];
    }

    const donation_last_use = try allocator.dupe(u32, last_use);
    for (0..count) |i| {
        if (!reachable[i]) continue;
        const root_idx: usize = @intCast(roots[i]);
        donation_last_use[root_idx] = @max(donation_last_use[root_idx], last_use[i]);
    }
    for (0..count) |i| {
        if (!reachable[i]) continue;
        donation_last_use[i] = donation_last_use[@intCast(roots[i])];
    }
    return donation_last_use;
}

fn computeGatherAddBiasPreserveSet(allocator: std.mem.Allocator, graph: *const Graph, reachable: []const bool) ![]bool {
    const count = graph.nodeCount();
    const preserve = try allocator.alloc(bool, count);
    @memset(preserve, false);

    for (0..count) |i| {
        if (!reachable[i]) continue;
        const gather_node = graph.node(@intCast(i));
        if (gather_node.op != .gather) continue;
        const gather_attrs = switch (gather_node.op) {
            .gather => |attrs| attrs,
            else => unreachable,
        };
        if (gather_attrs.axis != 0) continue;

        const gather_inputs = gather_node.getInputs();
        if (gather_inputs.len < 2 or gather_inputs[0] == null_node or gather_inputs[0] >= count) continue;
        const add_id = gather_inputs[0];
        const add_node = graph.node(add_id);
        if (add_node.op != .add) continue;
        const add_inputs = add_node.getInputs();
        if (add_inputs.len != 2) continue;

        const lhs = add_inputs[0];
        const rhs = add_inputs[1];
        if (lhs == null_node or rhs == null_node or lhs >= count or rhs >= count) continue;
        var lhs_buf: [8]i64 = undefined;
        var rhs_buf: [8]i64 = undefined;
        const lhs_shape = fillShapeDims(graph, lhs, &lhs_buf);
        const rhs_shape = fillShapeDims(graph, rhs, &rhs_buf);
        const can_fuse =
            (lhs_shape.len == 2 and rhs_shape.len == 1 and lhs_shape[1] == rhs_shape[0]) or
            (rhs_shape.len == 2 and lhs_shape.len == 1 and rhs_shape[1] == lhs_shape[0]);
        if (!can_fuse) continue;
        preserve[lhs] = true;
        preserve[rhs] = true;
    }

    return preserve;
}

/// Execute a graph through a real backend.
pub fn execute(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    options: ExecuteOptions,
) !ExecutionResult {
    const count = graph.nodeCount();
    const trace_nodes = graphExecTraceEnabled();
    const profile_ops = graphOpProfileEnabled();
    const finite_trace = graphFiniteTraceEnabled();
    const finite_trace_max_elems = graphFiniteTraceMaxElems();
    const slow_op_threshold_ns = graphOpSlowThresholdNs();
    var op_profiler = OpProfiler{ .allocator = allocator };
    defer op_profiler.deinit();
    defer if (profile_ops) op_profiler.print();
    if (trace_nodes) {
        graphExecDiag("begin nodes={} outputs={} runtime_inputs={} rss={}", .{
            count,
            graph.outputs.items.len,
            if (options.runtime_inputs) |inputs| inputs.len else @as(usize, 0),
            currentResidentBytes(),
        });
    }

    // 1-2. Use cached analysis if available, otherwise compute on the fly.
    const have_cache = options.cached_analysis != null;
    const reachable = if (options.cached_analysis) |ca| ca.reachable else try computeReachable(allocator, graph);
    defer if (!have_cache) allocator.free(reachable);
    const last_use = if (options.cached_analysis) |ca| ca.last_use else try computeLastUse(allocator, graph, reachable);
    defer if (!have_cache) allocator.free(last_use);
    const donation_last_use = if (options.cached_analysis) |ca| ca.donation_last_use else try computeDonationLastUse(allocator, graph, reachable, last_use);
    defer if (!have_cache) allocator.free(donation_last_use);
    const gather_add_bias_preserve = if (options.cached_analysis) |ca| ca.gather_add_bias_preserve else try computeGatherAddBiasPreserveSet(allocator, graph, reachable);
    defer if (!have_cache) allocator.free(gather_add_bias_preserve);

    // 3. Build runtime input lookup + donation set. Dense indexed arrays
    // avoid a hash lookup for every graph node during compiled training.
    const rt_values = try allocator.alloc(?CT, count);
    defer allocator.free(rt_values);
    @memset(rt_values, null);
    const donated_values = try allocator.alloc(bool, count);
    defer allocator.free(donated_values);
    @memset(donated_values, false);
    if (options.runtime_inputs) |inputs| {
        for (inputs, 0..) |ri, idx| {
            if (ri.node_id >= count) continue;
            rt_values[@intCast(ri.node_id)] = ri.value;
            if (options.donate) |d| {
                if (idx < d.len and d[idx]) {
                    donated_values[@intCast(ri.node_id)] = true;
                }
            }
        }
    }

    // 4. Allocate execution slots and retain lightweight shape provenance.
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);

    const shape_capture = if (options.cached_analysis) |ca| ca.runtime_shape_capture else try computeRuntimeShapeCaptureSet(allocator, graph);
    defer if (!have_cache) allocator.free(shape_capture);

    var runtime_shapes: ?[]?[]i64 = null;
    if (shapeCaptureSetHasAny(shape_capture)) {
        const shapes = try allocator.alloc(?[]i64, count);
        @memset(shapes, null);
        runtime_shapes = shapes;
    }
    defer if (runtime_shapes) |shapes| {
        for (shapes) |maybe_shape| {
            if (maybe_shape) |shape| allocator.free(shape);
        }
        allocator.free(shapes);
    };

    // 5. Mutable execution state
    var exec_state = ExecState{
        .attention_layer = 0,
        .options = options,
        .last_use = donation_last_use,
        .runtime_shapes = runtime_shapes,
    };

    for (0..count) |i| {
        if (!reachable[i]) continue;

        const node_id: NodeId = @intCast(i);

        // Check for runtime input override
        if (rt_values[i]) |rt_val| {
            if (trace_nodes) {
                const node = graph.node(node_id);
                graphExecDiag("node runtime id={} op={s} shape={any} rss={}", .{
                    node_id,
                    @tagName(std.meta.activeTag(node.op)),
                    node.output_shape,
                    currentResidentBytes(),
                });
            }
            values[i] = rt_val;
            logNodeRuntimeShape(graph, cb, node_id, rt_val);
            try recordRuntimeShape(allocator, cb, runtime_shapes, shape_capture, node_id, rt_val);
            continue;
        }

        // fused_from_float32 nodes are runtime data placeholders (e.g.
        // embedding indices). Consumers that need the data pull it from
        // side channels, so leave the value as null when no runtime_input
        // override was provided.
        if (graph.node(node_id).op == .fused_from_float32) continue;

        if (trace_nodes) {
            const node = graph.node(node_id);
            graphExecDiag("node begin id={} op={s} shape={any} rss={}", .{
                node_id,
                @tagName(std.meta.activeTag(node.op)),
                node.output_shape,
                currentResidentBytes(),
            });
            if (std.meta.activeTag(node.op) == .add) {
                for (node.getInputs(), 0..) |input_id, input_idx| {
                    if (input_id == null_node or input_id >= count) continue;
                    const input_node = graph.node(input_id);
                    graphExecDiag("node input id={} input{}={} op={s} shape={any}", .{
                        node_id,
                        input_idx,
                        input_id,
                        @tagName(std.meta.activeTag(input_node.op)),
                        input_node.output_shape,
                    });
                }
            }
        }
        const op_start_ns = if (profile_ops) graphNowNs() else 0;
        values[i] = executeNode(graph, cb, values, node_id, &exec_state) catch |err| {
            std.log.warn("executeNode failed node_id={d} op={s} shape={any} err={}", .{
                node_id,
                @tagName(std.meta.activeTag(graph.node(node_id).op)),
                graph.node(node_id).output_shape,
                err,
            });
            return err;
        };
        if (profile_ops) {
            const elapsed_ns = graphElapsedNs(op_start_ns, graphNowNs());
            try op_profiler.add(@tagName(std.meta.activeTag(graph.node(node_id).op)), elapsed_ns);
            if (slow_op_threshold_ns != 0 and elapsed_ns >= slow_op_threshold_ns) {
                const node = graph.node(node_id);
                std.debug.print(
                    "[graph-op-profile] slow node id={} op={s} ms={d:.3} shape={any}",
                    .{ node_id, @tagName(std.meta.activeTag(node.op)), nsToMs(elapsed_ns), node.output_shape },
                );
                for (node.getInputs(), 0..) |input_id, input_idx| {
                    if (input_id == null_node or input_id >= count) continue;
                    const input_node = graph.node(input_id);
                    std.debug.print(
                        " input{}={}:{s}:{any}",
                        .{ input_idx, input_id, @tagName(std.meta.activeTag(input_node.op)), input_node.output_shape },
                    );
                }
                std.debug.print("\n", .{});
            }
        }
        if (trace_nodes) {
            const node = graph.node(node_id);
            graphExecDiag("node done id={} op={s} rss={}", .{
                node_id,
                @tagName(std.meta.activeTag(node.op)),
                currentResidentBytes(),
            });
        }
        logNodeRuntimeShape(graph, cb, node_id, values[i].?);
        if (finite_trace) {
            try checkNodeFinite(
                allocator,
                graph,
                cb,
                node_id,
                values[i].?,
                finite_trace_max_elems,
            );
        }
        try recordRuntimeShape(allocator, cb, runtime_shapes, shape_capture, node_id, values[i].?);
        try cloneOutputIfAliasedInputWouldBeFreedFast(
            allocator,
            graph,
            cb,
            values,
            node_id,
            last_use,
            rt_values,
            donated_values,
        );

        // Free inputs whose last consumer is this node
        const n = graph.node(node_id);
        for (n.getInputs()) |input_id| {
            if (input_id == null_node or input_id >= count) continue;
            if (last_use[input_id] == i) {
                if (gather_add_bias_preserve[input_id]) continue;
                // Don't free non-donated runtime inputs (caller owns them).
                // Donated inputs are owned by the interpreter now.
                const input_idx: usize = @intCast(input_id);
                if (rt_values[input_idx] != null and !donated_values[input_idx]) continue;
                if (values[input_id]) |ct| {
                    if (values[i]) |out_ct| {
                        if (ct == out_ct and canKeepAliasedOutput(n.op)) {
                            values[input_id] = null;
                            continue;
                        }
                    }
                    // Clear every alias before releasing the handle. Keeping
                    // raw addresses of already-freed handles is ABA-unsafe:
                    // the allocator may recycle an address for a later live
                    // tensor during this same execution.
                    nullCtAliases(values, ct);
                    cb.free(ct);
                }
            }
        }
    }

    // 6. Collect outputs. Guard against aliased runtime inputs: a
    //    fused_to_float32 pass-through can produce an output CT that is
    //    the same pointer as a non-donated runtime input (weight handle).
    //    ExecutionResult.deinit frees all outputs, which would destroy
    //    the cached weight handle for future executions. Detect this by
    //    comparing output CT pointers against runtime input CTs.
    const outputs = try allocator.alloc(CT, graph.outputs.items.len);
    for (graph.outputs.items, 0..) |out_id, idx| {
        const ct = values[out_id] orelse return error.MissingRuntimeInput;
        // Check if this output CT pointer aliases any non-donated runtime input.
        var aliases_rt = false;
        if (options.runtime_inputs) |inputs| {
            for (inputs) |ri| {
                if (ri.node_id < donated_values.len and !donated_values[@intCast(ri.node_id)] and ri.value == ct) {
                    aliases_rt = true;
                    break;
                }
            }
        }
        if (aliases_rt) {
            // Create a fresh copy so deinit doesn't destroy the weight.
            const one_data: [1]f32 = .{1.0};
            const one = try cb.fromFloat32(&one_data);
            defer cb.free(one);
            outputs[idx] = try cb.multiply(ct, one);
        } else {
            outputs[idx] = ct;
        }
    }

    // 7. Free remaining parameter handles. getWeight() allocates a new
    //    handle each call (e.g. native buffer); the underlying weight data is
    //    borrowed, but the handle itself must be freed. Skip outputs
    //    (caller owns them) and runtime inputs (caller owns them).
    //
    //    Skip by returned CT handle, not only by output node id: view,
    //    passthrough, and fused ops may legally share an exact handle across
    //    graph nodes, and ExecutionResult owns that handle until deinit.
    for (0..count) |i| {
        if (values[i] == null) continue;
        if (containsCt(outputs, values[i].?)) continue;
        // Skip output nodes — caller frees via ExecutionResult.deinit
        var is_output = false;
        for (graph.outputs.items) |out_id| {
            if (out_id == @as(NodeId, @intCast(i))) {
                is_output = true;
                break;
            }
        }
        if (is_output) continue;
        // Skip non-donated runtime inputs — caller owns them.
        // Donated inputs are interpreter-owned; free if still live.
        if (rt_values[i] != null and !donated_values[i]) continue;
        // Free any remaining handles (parameters, donated inputs, or
        // intermediates that weren't caught by liveness-based freeing)
        const ct = values[i].?;
        nullCtAliases(values, ct);
        cb.free(ct);
    }

    // 8. Free MoE routing state from the last layer.
    exec_state.freeMoeState();

    if (trace_nodes) {
        graphExecDiag("done outputs={} rss={}", .{ outputs.len, currentResidentBytes() });
    }
    return .{ .outputs = outputs, .allocator = allocator };
}

pub fn captureNodeValues(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    options: ExecuteOptions,
    capture_node_ids: []const NodeId,
) !CapturedValuesResult {
    const count = graph.nodeCount();

    const have_cache = options.cached_analysis != null;
    const reachable = if (options.cached_analysis) |ca| ca.reachable else try computeReachable(allocator, graph);
    defer if (!have_cache) allocator.free(reachable);
    const last_use = if (options.cached_analysis) |ca| ca.last_use else try computeLastUse(allocator, graph, reachable);
    defer if (!have_cache) allocator.free(last_use);
    const donation_last_use = if (options.cached_analysis) |ca| ca.donation_last_use else try computeDonationLastUse(allocator, graph, reachable, last_use);
    defer if (!have_cache) allocator.free(donation_last_use);

    const rt_values = try allocator.alloc(?CT, count);
    defer allocator.free(rt_values);
    @memset(rt_values, null);
    const donated_values = try allocator.alloc(bool, count);
    defer allocator.free(donated_values);
    @memset(donated_values, false);
    if (options.runtime_inputs) |inputs| {
        for (inputs, 0..) |ri, idx| {
            if (ri.node_id >= count) continue;
            rt_values[@intCast(ri.node_id)] = ri.value;
            if (options.donate) |d| {
                if (idx < d.len and d[idx]) {
                    donated_values[@intCast(ri.node_id)] = true;
                }
            }
        }
    }

    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);

    const shape_capture = if (options.cached_analysis) |ca| ca.runtime_shape_capture else try computeRuntimeShapeCaptureSet(allocator, graph);
    defer if (!have_cache) allocator.free(shape_capture);

    var runtime_shapes: ?[]?[]i64 = null;
    if (shapeCaptureSetHasAny(shape_capture)) {
        const shapes = try allocator.alloc(?[]i64, count);
        @memset(shapes, null);
        runtime_shapes = shapes;
    }
    defer if (runtime_shapes) |shapes| {
        for (shapes) |maybe_shape| {
            if (maybe_shape) |shape| allocator.free(shape);
        }
        allocator.free(shapes);
    };

    const captured = try allocator.alloc(?CT, capture_node_ids.len);
    defer allocator.free(captured);
    @memset(captured, null);
    errdefer {
        for (captured) |maybe_ct| {
            if (maybe_ct) |ct| cb.free(ct);
        }
    }

    var exec_state = ExecState{
        .attention_layer = 0,
        .options = options,
        .last_use = donation_last_use,
        .runtime_shapes = runtime_shapes,
    };
    defer exec_state.freeMoeState();

    for (0..count) |i| {
        if (!reachable[i]) continue;

        const node_id: NodeId = @intCast(i);

        if (rt_values[i]) |rt_val| {
            values[i] = rt_val;
            logNodeRuntimeShape(graph, cb, node_id, rt_val);
            try recordRuntimeShape(allocator, cb, runtime_shapes, shape_capture, node_id, rt_val);
            try maybeCaptureNodeValue(allocator, graph, cb, capture_node_ids, captured, node_id, rt_val);
            continue;
        }

        if (graph.node(node_id).op == .fused_from_float32) continue;

        values[i] = executeNode(graph, cb, values, node_id, &exec_state) catch |err| {
            std.log.warn("capture executeNode failed node_id={d} op={s} shape={any} err={}", .{
                node_id,
                @tagName(std.meta.activeTag(graph.node(node_id).op)),
                graph.node(node_id).output_shape,
                err,
            });
            return err;
        };
        logNodeRuntimeShape(graph, cb, node_id, values[i].?);
        try recordRuntimeShape(allocator, cb, runtime_shapes, shape_capture, node_id, values[i].?);
        try maybeCaptureNodeValue(allocator, graph, cb, capture_node_ids, captured, node_id, values[i].?);
        try cloneOutputIfAliasedInputWouldBeFreedFast(
            allocator,
            graph,
            cb,
            values,
            node_id,
            last_use,
            rt_values,
            donated_values,
        );

        const n = graph.node(node_id);
        for (n.getInputs()) |input_id| {
            if (input_id == null_node or input_id >= count) continue;
            if (last_use[input_id] == i) {
                const input_idx: usize = @intCast(input_id);
                if (rt_values[input_idx] != null and !donated_values[input_idx]) continue;
                if (values[input_id]) |ct| {
                    if (values[i]) |out_ct| {
                        if (ct == out_ct and canKeepAliasedOutput(n.op)) {
                            values[input_id] = null;
                            continue;
                        }
                    }
                    cb.free(ct);
                    values[input_id] = null;
                }
            }
        }
    }

    const out = try allocator.alloc(CT, capture_node_ids.len);
    errdefer allocator.free(out);
    for (capture_node_ids, 0..) |node_id, idx| {
        out[idx] = captured[idx] orelse {
            std.log.err("captureNodeValues missing node id={d} op={s}", .{ node_id, @tagName(graph.node(node_id).op) });
            return error.MissingRuntimeInput;
        };
    }
    return .{ .values = out, .allocator = allocator };
}

fn maybeCaptureNodeValue(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    capture_node_ids: []const NodeId,
    captured: []?CT,
    node_id: NodeId,
    value: CT,
) !void {
    for (capture_node_ids, 0..) |capture_id, idx| {
        if (capture_id != node_id or captured[idx] != null) continue;
        captured[idx] = try cloneTensorForShape(allocator, cb, value, graph.node(node_id).output_shape);
    }
}

pub fn cloneOutputIfAliasedInputWouldBeFreed(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    node_id: NodeId,
    last_use: []const u32,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    donated: std.AutoHashMapUnmanaged(NodeId, void),
) !void {
    const out_idx: usize = @intCast(node_id);
    const output_ct = values[out_idx] orelse return;
    const n = graph.node(node_id);

    for (n.getInputs()) |input_id| {
        if (input_id == null_node or input_id >= values.len) continue;
        const input_ct = values[@intCast(input_id)] orelse continue;
        if (input_ct != output_ct) continue;

        const input_is_non_donated_runtime = rt_map.contains(input_id) and !donated.contains(input_id);
        const input_dies_now = last_use[@intCast(input_id)] == out_idx;
        if (canKeepAliasedOutput(n.op) and input_dies_now and !input_is_non_donated_runtime) {
            // Last-use consume paths transfer ownership from the input slot to
            // the output slot later in execute() by nulling the input instead
            // of freeing it. Keep the aliased output in that specific case.
            return;
        }

        values[out_idx] = try cloneTensorForShape(allocator, cb, output_ct, n.output_shape);
        return;
    }
}

fn cloneOutputIfAliasedInputWouldBeFreedFast(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    node_id: NodeId,
    last_use: []const u32,
    rt_values: []const ?CT,
    donated_values: []const bool,
) !void {
    const out_idx: usize = @intCast(node_id);
    const output_ct = values[out_idx] orelse return;
    const n = graph.node(node_id);

    for (n.getInputs()) |input_id| {
        if (input_id == null_node or input_id >= values.len) continue;
        const input_ct = values[@intCast(input_id)] orelse continue;
        if (input_ct != output_ct) continue;

        const input_idx: usize = @intCast(input_id);
        const input_is_non_donated_runtime = input_idx < rt_values.len and rt_values[input_idx] != null and !donated_values[input_idx];
        const input_dies_now = last_use[input_idx] == out_idx;
        if (canKeepAliasedOutput(n.op) and input_dies_now and !input_is_non_donated_runtime) {
            // Last-use consume paths transfer ownership from the input slot to
            // the output slot later in execute() by nulling the input instead
            // of freeing it. Keep the aliased output in that specific case.
            return;
        }

        values[out_idx] = try cloneTensorForShape(allocator, cb, output_ct, n.output_shape);
        return;
    }
}

pub fn canKeepAliasedOutput(op: anytype) bool {
    return switch (op) {
        .fused_gelu,
        .fused_gelu_exact,
        .fused_relu,
        .fused_silu,
        .fused_quick_gelu,
        .fused_sigmoid,
        .fused_tanh_act,
        .fused_layer_norm,
        .fused_rms_norm,
        .fused_softmax,
        .fused_log_softmax,
        .fused_elem_add,
        .fused_elem_multiply,
        .fused_add_mul_scalar,
        .neg,
        .sqrt,
        .rsqrt,
        .exp,
        .log,
        .sin,
        .cos,
        .tanh,
        .erf,
        .abs,
        .add,
        .mul,
        .sub,
        .div,
        .less_than,
        .where_select,
        .reshape,
        .transpose,
        .broadcast_in_dim,
        .convert_dtype,
        .fused_eval_tensor,
        => true,
        else => false,
    };
}

fn cloneTensorForShape(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    tensor: CT,
    shape: Shape,
) !CT {
    var dims: [8]i32 = undefined;
    const runtime_shape: ?[]i64 = cb.tensorShape(tensor, allocator) catch null;
    defer if (runtime_shape) |actual| allocator.free(actual);

    // Dynamic imported graphs retain their exported/static dimensions in the
    // graph Shape while the backend tensor carries the dimensions for this
    // request. An alias-safety clone must preserve that runtime shape; using
    // the declaration can either reject a symbolic dimension or manufacture a
    // larger element count than the tensor actually contains.
    const rank = if (runtime_shape) |actual| actual.len else shape.rank();
    if (rank > dims.len) return error.UnsupportedShape;
    for (0..rank) |axis| {
        const dim = if (runtime_shape) |actual| actual[axis] else shape.dim(@intCast(axis));
        dims[axis] = std.math.cast(i32, dim) orelse return error.UnsupportedShape;
    }

    if (try cb.cloneTensorShape(tensor, dims[0..rank])) |cloned| return cloned;

    const data = try cb.toFloat32(tensor, allocator);
    defer allocator.free(data);
    return cb.fromFloat32Shape(data, dims[0..rank]);
}

/// Grouped MoE routing data computed from a flat MoeRouteSelection.
/// Sorted by expert so moeLinearNoBias gets contiguous expert batches.
const MoeGroupedState = struct {
    rows: []u32,
    expert_ids: []u32,
    route_weights: []f32,
    expert_tile_ids: []u32,
    tile_row_starts: []u32,
    tile_row_counts: []u32,
    allocator: std.mem.Allocator,

    fn deinit(self: *MoeGroupedState) void {
        self.allocator.free(self.rows);
        self.allocator.free(self.expert_ids);
        self.allocator.free(self.route_weights);
        self.allocator.free(self.expert_tile_ids);
        self.allocator.free(self.tile_row_starts);
        self.allocator.free(self.tile_row_counts);
    }
};

/// Mutable state carried across node executions within a single
/// execute() call. Tracks counters for side-channel consumption.
pub const ExecState = struct {
    /// Auto-incremented for each attention node, so each layer gets
    /// the correct layer_index in its AttentionContext.
    attention_layer: usize,
    options: ExecuteOptions,
    last_use: []const u32 = &.{},
    runtime_shapes: ?[]const ?[]const i64 = null,

    /// Second output from the most recent fused_linear_no_bias_pair.
    /// Picked up by the downstream fused_to_float32 pass-through node.
    pair_second: ?CT = null,

    /// MoE routing state from the most recent fused_moe_select_routes
    /// node. Replaced per-layer; consumed by fused_moe_linear_no_bias,
    /// fused_moe_scatter_add, and fused_take_rows within that layer.
    moe_routes: ?ops_mod.MoeRouteSelection = null,
    moe_routes_allocator: std.mem.Allocator = std.heap.page_allocator,
    moe_grouped: ?MoeGroupedState = null,

    pub fn isLastUseBy(self: *const ExecState, input_id: NodeId, node_id: NodeId) bool {
        const idx: usize = @intCast(input_id);
        return idx < self.last_use.len and self.last_use[idx] == node_id;
    }

    pub fn freeMoeState(self: *ExecState) void {
        if (self.moe_routes) |routes| {
            self.moe_routes_allocator.free(routes.expert_ids);
            self.moe_routes_allocator.free(routes.route_weights);
            self.moe_routes = null;
        }
        if (self.moe_grouped) |*g| {
            g.deinit();
            self.moe_grouped = null;
        }
    }
};

fn shapeCaptureSetHasAny(capture: []const bool) bool {
    for (capture) |enabled| {
        if (enabled) return true;
    }
    return false;
}

fn computeRuntimeShapeCaptureSet(allocator: std.mem.Allocator, graph: *const Graph) ![]bool {
    const capture = try allocator.alloc(bool, graph.nodeCount());
    // Runtime shape restoration should be a graph-wide execution contract, not
    // a pattern-specific escape hatch for one exported model layout.
    @memset(capture, true);

    return capture;
}

/// Request-scoped concrete shapes shared by partition executors. Imported
/// graphs can retain symbolic dimensions in their declarations after the
/// backend has resolved them for a particular request; interpreter fallbacks
/// inside a partition need the same provenance as whole-graph interpretation.
pub const RuntimeShapeTracker = struct {
    allocator: std.mem.Allocator,
    capture: []const bool = &.{},
    owned_capture: ?[]bool = null,
    shapes: ?[]?[]i64 = null,

    pub fn init(
        allocator: std.mem.Allocator,
        graph: *const Graph,
        cached_analysis: ?CachedAnalysis,
    ) !RuntimeShapeTracker {
        var tracker = RuntimeShapeTracker{ .allocator = allocator };
        errdefer tracker.deinit();

        if (cached_analysis) |analysis| {
            tracker.capture = analysis.runtime_shape_capture;
        } else {
            const capture = try computeRuntimeShapeCaptureSet(allocator, graph);
            tracker.capture = capture;
            tracker.owned_capture = capture;
        }

        if (shapeCaptureSetHasAny(tracker.capture) and graphNeedsRuntimeShapeTracking(graph)) {
            const shapes = try allocator.alloc(?[]i64, graph.nodeCount());
            @memset(shapes, null);
            tracker.shapes = shapes;
        }
        return tracker;
    }

    pub fn deinit(self: *RuntimeShapeTracker) void {
        if (self.shapes) |shapes| {
            for (shapes) |maybe_shape| {
                if (maybe_shape) |shape| self.allocator.free(shape);
            }
            self.allocator.free(shapes);
            self.shapes = null;
        }
        if (self.owned_capture) |capture| {
            self.allocator.free(capture);
            self.owned_capture = null;
        }
        self.capture = &.{};
    }

    pub fn runtimeShapes(self: *const RuntimeShapeTracker) ?[]const ?[]const i64 {
        return self.shapes;
    }

    pub fn record(
        self: *RuntimeShapeTracker,
        cb: *const ComputeBackend,
        node_id: NodeId,
        value: CT,
    ) !void {
        try recordRuntimeShape(self.allocator, cb, self.shapes, self.capture, node_id, value);
    }
};

fn graphNeedsRuntimeShapeTracking(graph: *const Graph) bool {
    for (0..graph.nodeCount()) |index| {
        const node = graph.node(@intCast(index));
        for (0..node.output_shape.rank()) |axis| {
            if (node.output_shape.dim(@intCast(axis)) < 0) return true;
        }
        switch (node.op) {
            .reshape => |attrs| if (attrs.runtime_shape) return true,
            .broadcast_in_dim => if (node.num_inputs > 1) return true,
            else => {},
        }
    }
    return false;
}

/// Convert a flat MoeRouteSelection (rows * top_k entries) into a grouped
/// format sorted by expert. Mirrors the grouping in gpt.zig's
/// runGroupedExpertBatchTensor.
fn buildGroupedFromRouting(
    allocator: std.mem.Allocator,
    sel: ops_mod.MoeRouteSelection,
) !MoeGroupedState {
    const total = sel.rows * sel.top_k;
    const num_experts: usize = blk: {
        var max_eid: u32 = 0;
        for (sel.expert_ids[0..total]) |eid| max_eid = @max(max_eid, eid);
        break :blk @as(usize, max_eid) + 1;
    };

    // Count entries per expert.
    var expert_counts = try allocator.alloc(usize, num_experts);
    defer allocator.free(expert_counts);
    @memset(expert_counts, 0);
    for (sel.expert_ids[0..total]) |eid| expert_counts[eid] += 1;

    // Compute start offset per expert.
    var offsets = try allocator.alloc(usize, num_experts);
    defer allocator.free(offsets);
    var off: usize = 0;
    for (0..num_experts) |e| {
        offsets[e] = off;
        off += expert_counts[e];
    }

    // Allocate grouped arrays.
    const grouped_rows = try allocator.alloc(u32, total);
    errdefer allocator.free(grouped_rows);
    const grouped_expert_ids = try allocator.alloc(u32, total);
    errdefer allocator.free(grouped_expert_ids);
    const grouped_route_weights = try allocator.alloc(f32, total);
    errdefer allocator.free(grouped_route_weights);

    // Fill sorted by expert.
    var cursors = try allocator.alloc(usize, num_experts);
    defer allocator.free(cursors);
    @memcpy(cursors, offsets);
    for (0..total) |i| {
        const eid = sel.expert_ids[i];
        const row: u32 = @intCast(i / sel.top_k);
        const c = cursors[eid];
        grouped_rows[c] = row;
        grouped_expert_ids[c] = eid;
        grouped_route_weights[c] = sel.route_weights[i];
        cursors[eid] = c + 1;
    }

    const row_tile_size: usize = 4;
    var tile_count: usize = 0;
    var segment_start: usize = 0;
    while (segment_start < grouped_expert_ids.len) {
        const expert_id = grouped_expert_ids[segment_start];
        var segment_end = segment_start + 1;
        while (segment_end < grouped_expert_ids.len and grouped_expert_ids[segment_end] == expert_id) : (segment_end += 1) {}
        const segment_len = segment_end - segment_start;
        tile_count += (segment_len + row_tile_size - 1) / row_tile_size;
        segment_start = segment_end;
    }

    const expert_tile_ids = try allocator.alloc(u32, tile_count);
    errdefer allocator.free(expert_tile_ids);
    const tile_row_starts = try allocator.alloc(u32, tile_count);
    errdefer allocator.free(tile_row_starts);
    const tile_row_counts = try allocator.alloc(u32, tile_count);
    errdefer allocator.free(tile_row_counts);

    var tile_index: usize = 0;
    segment_start = 0;
    while (segment_start < grouped_expert_ids.len) {
        const expert_id = grouped_expert_ids[segment_start];
        var segment_end = segment_start + 1;
        while (segment_end < grouped_expert_ids.len and grouped_expert_ids[segment_end] == expert_id) : (segment_end += 1) {}
        var row_cursor = segment_start;
        while (row_cursor < segment_end) : (row_cursor += row_tile_size) {
            const remaining = segment_end - row_cursor;
            expert_tile_ids[tile_index] = expert_id;
            tile_row_starts[tile_index] = @intCast(row_cursor);
            tile_row_counts[tile_index] = @intCast(@min(remaining, row_tile_size));
            tile_index += 1;
        }
        segment_start = segment_end;
    }

    return .{
        .rows = grouped_rows,
        .expert_ids = grouped_expert_ids,
        .route_weights = grouped_route_weights,
        .expert_tile_ids = expert_tile_ids,
        .tile_row_starts = tile_row_starts,
        .tile_row_counts = tile_row_counts,
        .allocator = allocator,
    };
}

/// Fill a caller-owned buffer with shape dimensions from a graph node.
fn fillShapeDims(graph: *const Graph, node_id: NodeId, buf: *[8]i64) []const i64 {
    const shape = graph.node(node_id).output_shape;
    const rank = shape.rank();
    for (0..rank) |d| {
        buf[d] = shape.dim(@intCast(d));
    }
    return buf[0..rank];
}

fn runtimeOrDeclaredShape(state: *const ExecState, graph: *const Graph, node_id: NodeId, buf: *[8]i64) []const i64 {
    if (state.runtime_shapes) |runtime_shapes| {
        if (node_id < runtime_shapes.len) {
            if (runtime_shapes[@intCast(node_id)]) |runtime_shape| return runtime_shape;
        }
    }
    return fillShapeDims(graph, node_id, buf);
}

fn executeScatterAdd(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    dest: CT,
    values: CT,
    indices: CT,
    dest_shape: []const i64,
    values_shape: []const i64,
    indices_shape: []const i64,
    axis: u8,
) !CT {
    if (axis != 0) return error.UnsupportedPrimitiveOp;
    if (dest_shape.len != 2 or values_shape.len != 2) return error.UnsupportedPrimitiveOp;
    if (dest_shape[0] < 0 or dest_shape[1] <= 0 or values_shape[0] < 0 or values_shape[1] != dest_shape[1]) return error.UnsupportedShape;
    if (indices_shape.len == 0) return error.UnsupportedShape;

    const out_rows: usize = @intCast(dest_shape[0]);
    const value_rows: usize = @intCast(values_shape[0]);
    const dim: usize = @intCast(dest_shape[1]);

    const dest_data = try cb.toFloat32(dest, allocator);
    defer allocator.free(dest_data);
    const values_data = try cb.toFloat32(values, allocator);
    defer allocator.free(values_data);
    const index_data = try cb.toFloat32(indices, allocator);
    defer allocator.free(index_data);

    if (dest_data.len != out_rows * dim or values_data.len != value_rows * dim or index_data.len < value_rows) return error.ShapeMismatch;

    const output = try allocator.dupe(f32, dest_data);
    defer allocator.free(output);
    for (0..value_rows) |row_idx| {
        const out_row_f = @round(index_data[row_idx]);
        if (out_row_f < 0) return error.IndexOutOfBounds;
        const out_row: usize = @intFromFloat(out_row_f);
        if (out_row >= out_rows) return error.IndexOutOfBounds;
        const src = values_data[row_idx * dim ..][0..dim];
        const dst = output[out_row * dim ..][0..dim];
        for (src, dst) |v, *d| d.* += v;
    }

    const dims = [_]i32{ @intCast(out_rows), @intCast(dim) };
    return cb.fromFloat32Shape(output, &dims);
}

fn safeElementCountFromDims(dims: []const i64) ?usize {
    var count: usize = 1;
    for (dims) |d| {
        if (d <= 0) return null;
        count = std.math.mul(usize, count, @intCast(d)) catch return null;
    }
    return count;
}

fn safeElementCountFromShape(shape: Shape) ?usize {
    var dims: [8]i64 = undefined;
    const rank = shape.rank();
    for (0..rank) |d| {
        dims[d] = shape.dim(@intCast(d));
    }
    return safeElementCountFromDims(dims[0..rank]);
}

fn positiveShapeDim(shape: Shape, axis: usize) !usize {
    if (axis >= shape.rank()) return error.UnsupportedShape;
    const dim = shape.dim(@intCast(axis));
    if (dim <= 0) return error.UnsupportedShape;
    return std.math.cast(usize, dim) orelse return error.UnsupportedShape;
}

fn shouldReshapeToDeclaredShape(actual: []const i64, declared: Shape) bool {
    const rank = declared.rank();
    if (rank < 1 or actual.len == 0) return false;
    if (actual.len > rank) return false;

    // A concrete runtime batch must not be folded into a later axis just
    // because an exported graph carried a stale singleton batch dimension.
    if (actual.len == rank and actual[0] > 1) {
        const declared_batch = declared.dim(0);
        if (declared_batch > 0 and declared_batch != actual[0]) return false;
    }

    const actual_size = safeElementCountFromDims(actual);
    const declared_size = safeElementCountFromShape(declared) orelse return false;
    if (actual_size) |size| {
        if (declared_size != size) return false;
    }

    if (actual.len < rank) return true;

    for (actual, 0..) |ad, d| {
        if (ad != declared.dim(@intCast(d))) return true;
    }
    return actual_size == null;
}

fn positiveResolvedDim(actual: ?[]const i64, shape: Shape, axis: usize) !usize {
    if (actual) |dims| {
        if (axis < dims.len and dims[axis] > 0) {
            return std.math.cast(usize, dims[axis]) orelse return error.UnsupportedShape;
        }
    }
    return positiveShapeDim(shape, axis);
}

/// For shape-tracking backends (MLX), reshape a tensor to its declared
/// shape when the declared shape is fully concrete, the actual rank differs,
/// and element counts match. Returns
/// the reshaped tensor (owned, caller must free) or null (no reshape
/// needed — use original value).
fn ensureDeclaredShape(cb: *const ComputeBackend, val: CT, declared: Shape) ?CT {
    const rank = declared.rank();
    if (rank < 1) return null;
    var dims: [8]i64 = undefined;
    for (0..rank) |d| {
        dims[d] = declared.dim(@intCast(d));
        if (dims[d] <= 0) return null;
    }
    if (cb.tensorShapeMatches(val, dims[0..rank]) catch null) |matches| {
        return if (matches) null else cb.primReshape(val, dims[0..rank]) catch null;
    }
    const actual = cb.tensorShape(val, std.heap.page_allocator) catch {
        return cb.primReshape(val, dims[0..rank]) catch null;
    };
    defer std.heap.page_allocator.free(actual);
    if (!shouldReshapeToDeclaredShape(actual, declared)) {
        return null;
    }
    return cb.primReshape(val, dims[0..rank]) catch null;
}

fn resolveRuntimeBroadcastShape(lhs: []const i64, rhs: []const i64, out: *[8]i64) ?[]const i64 {
    const rank = @max(lhs.len, rhs.len);
    if (rank > out.len) return null;

    for (0..rank) |axis| {
        const lhs_axis = axis + lhs.len;
        const rhs_axis = axis + rhs.len;
        const lhs_dim: i64 = if (lhs_axis >= rank) lhs[lhs_axis - rank] else 1;
        const rhs_dim: i64 = if (rhs_axis >= rank) rhs[rhs_axis - rank] else 1;
        if (lhs_dim <= 0 or rhs_dim <= 0) return null;
        if (lhs_dim == rhs_dim) {
            out[axis] = lhs_dim;
        } else if (lhs_dim == 1) {
            out[axis] = rhs_dim;
        } else if (rhs_dim == 1) {
            out[axis] = lhs_dim;
        } else {
            return null;
        }
    }
    return out[0..rank];
}

fn runtimeBroadcastOperand(
    cb: *const ComputeBackend,
    value: CT,
    actual_shape: []const i64,
    target_shape: []const i64,
) !?CT {
    if (std.mem.eql(i64, actual_shape, target_shape)) return null;
    if (actual_shape.len > target_shape.len or target_shape.len > ml.graph.shape.max_rank)
        return error.ShapeMismatch;

    var axes: [ml.graph.shape.max_rank]u8 = undefined;
    const offset = target_shape.len - actual_shape.len;
    for (0..actual_shape.len) |axis| axes[axis] = @intCast(offset + axis);
    return try cb.primBroadcastInDim(value, target_shape, axes[0..actual_shape.len], actual_shape);
}

fn executeGeluBackwardFallback(
    allocator: std.mem.Allocator,
    output_shape: Shape,
    cb: *const ComputeBackend,
    input: CT,
    upstream_grad: CT,
    exact: bool,
) !CT {
    const input_data = try cb.toFloat32(input, allocator);
    defer allocator.free(input_data);
    const upstream_data = try cb.toFloat32(upstream_grad, allocator);
    defer allocator.free(upstream_data);
    if (input_data.len != upstream_data.len) return error.ShapeMismatch;

    const output = try allocator.alloc(f32, input_data.len);
    defer allocator.free(output);
    for (input_data, upstream_data, output) |x, upstream, *dst| {
        if (!std.math.isFinite(x)) {
            dst.* = 0.0;
            continue;
        }
        if (exact) {
            const cdf = 0.5 * (1.0 + erfApproxF32(x * 0.7071067811865476));
            const pdf = std.math.exp(-0.5 * x * x) * 0.3989422804014327;
            const derivative = cdf + x * pdf;
            dst.* = if (std.math.isFinite(derivative)) upstream * derivative else 0.0;
            continue;
        }
        const x2 = x * x;
        const inner = 0.7978845608028654 * (x + 0.044715 * x * x2);
        if (inner > 10.0) {
            dst.* = upstream;
            continue;
        }
        if (inner < -10.0) {
            dst.* = 0.0;
            continue;
        }
        const t = std.math.tanh(inner);
        const sech2 = 1.0 - t * t;
        const derivative = 0.5 * (1.0 + t) + 0.5 * x * sech2 * 0.7978845608028654 * (1.0 + 0.134145 * x2);
        dst.* = if (std.math.isFinite(derivative)) upstream * derivative else 0.0;
    }

    var shape_buf: [8]i32 = undefined;
    const rank = output_shape.rank();
    if (rank > shape_buf.len) return error.UnsupportedShape;
    for (0..rank) |axis| {
        const dim = output_shape.dim(@intCast(axis));
        if (dim <= 0) return error.UnsupportedShape;
        shape_buf[axis] = @intCast(dim);
    }
    return cb.fromFloat32Shape(output, shape_buf[0..rank]);
}

fn executeExactGeluFallback(
    allocator: std.mem.Allocator,
    output_shape: Shape,
    cb: *const ComputeBackend,
    input: CT,
) !CT {
    const input_data = try cb.toFloat32(input, allocator);
    defer allocator.free(input_data);

    const output = try allocator.alloc(f32, input_data.len);
    defer allocator.free(output);
    for (input_data, output) |x, *dst| {
        if (!std.math.isFinite(x)) {
            dst.* = 0.0;
            continue;
        }
        dst.* = 0.5 * x * (1.0 + erfApproxF32(x * 0.7071067811865476));
    }

    var shape_buf: [8]i32 = undefined;
    const rank = output_shape.rank();
    if (rank > shape_buf.len) return error.UnsupportedShape;
    for (0..rank) |axis| {
        const dim = output_shape.dim(@intCast(axis));
        if (dim <= 0) return error.UnsupportedShape;
        shape_buf[axis] = @intCast(dim);
    }
    return cb.fromFloat32Shape(output, shape_buf[0..rank]);
}

fn erfApproxF32(x: f32) f32 {
    const sign: f32 = if (x < 0) -1.0 else 1.0;
    const ax = @abs(x);
    const t = 1.0 / (1.0 + 0.3275911 * ax);
    const poly = (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t;
    return sign * (1.0 - poly * @exp(-(ax * ax)));
}

fn graphTraceShapesEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_GRAPH_TRACE_SHAPES", false);
}

fn logNodeRuntimeShape(graph: *const Graph, cb: *const ComputeBackend, node_id: NodeId, value: CT) void {
    if (!graphTraceShapesEnabled()) return;
    const actual = cb.tensorShape(value, std.heap.page_allocator) catch return;
    defer std.heap.page_allocator.free(actual);
    const inputs = graph.node(node_id).getInputs();
    std.log.info("termite graph shape node_id={d} op={s} inputs={any} declared={any} actual={any}", .{
        node_id,
        @tagName(std.meta.activeTag(graph.node(node_id).op)),
        inputs,
        graph.node(node_id).output_shape,
        actual,
    });
}

fn recordRuntimeShape(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    maybe_runtime_shapes: ?[]?[]i64,
    shape_capture: []const bool,
    node_id: NodeId,
    value: CT,
) !void {
    const idx: usize = @intCast(node_id);
    if (idx >= shape_capture.len or !shape_capture[idx]) return;
    const runtime_shapes = maybe_runtime_shapes orelse return;
    if (idx >= runtime_shapes.len) return;
    if (runtime_shapes[idx] != null) return;
    const actual = cb.tensorShape(value, allocator) catch return;
    runtime_shapes[idx] = actual;
}

fn hasNegativeDim(dims: []const i64) bool {
    for (dims) |dim| {
        if (dim < 0) return true;
    }
    return false;
}

fn countNegativeDims(dims: []const i64) usize {
    var count: usize = 0;
    for (dims) |dim| {
        if (dim < 0) count += 1;
    }
    return count;
}

fn resolveSingleInferredDim(dims: []i64, input_numel: usize) bool {
    var infer_index: ?usize = null;
    var known_product: usize = 1;

    for (dims, 0..) |dim, i| {
        if (dim == -1) {
            if (infer_index != null) return false;
            infer_index = i;
            continue;
        }
        if (dim <= 0) return false;
        known_product = std.math.mul(usize, known_product, @intCast(dim)) catch return false;
    }

    if (infer_index) |idx| {
        if (known_product == 0 or input_numel % known_product != 0) return false;
        dims[idx] = @intCast(input_numel / known_product);
        return true;
    }
    return known_product == input_numel;
}

fn resolveOnnxRuntimeReshapeDims(
    shape_values: []const f32,
    actual_input_shape: []const i64,
    allow_zero: bool,
    out: *[8]i64,
) ![]const i64 {
    if (shape_values.len == 0 or shape_values.len > out.len) return error.InvalidTensorShape;
    const input_numel = safeElementCountFromDims(actual_input_shape) orelse return error.InvalidTensorShape;

    var inferred_axis: ?usize = null;
    var known_product: usize = 1;
    for (shape_values, 0..) |value, axis| {
        const rounded = @round(value);
        if (!std.math.isFinite(rounded) or @abs(value - rounded) > 1e-3 or
            rounded < @as(f32, @floatFromInt(std.math.minInt(i32))) or
            rounded > @as(f32, @floatFromInt(std.math.maxInt(i32))))
        {
            return error.InvalidTensorShape;
        }
        var dim: i64 = @intFromFloat(rounded);
        if (dim == 0 and !allow_zero) {
            if (axis >= actual_input_shape.len or actual_input_shape[axis] <= 0) return error.InvalidTensorShape;
            dim = actual_input_shape[axis];
        }
        if (dim < -1) return error.InvalidTensorShape;
        if (dim == -1) {
            if (inferred_axis != null) return error.InvalidTensorShape;
            inferred_axis = axis;
        } else if (dim == 0) {
            known_product = 0;
        } else {
            known_product = std.math.mul(usize, known_product, @intCast(dim)) catch return error.InvalidTensorShape;
        }
        out[axis] = dim;
    }

    if (inferred_axis) |axis| {
        if (known_product == 0 or input_numel % known_product != 0) return error.ShapeMismatch;
        out[axis] = @intCast(input_numel / known_product);
    } else if (known_product != input_numel) {
        return error.ShapeMismatch;
    }
    return out[0..shape_values.len];
}

fn resolveRuntimeReshapeDims(actual: []const i64, declared: Shape, target: Shape, out: *[8]i64) ?[]const i64 {
    const rank = target.rank();
    if (rank < 1 or rank > out.len or actual.len == 0) return null;
    const input_numel = safeElementCountFromDims(actual) orelse return null;

    for (0..rank) |i| {
        const dim = target.dim(@intCast(i));
        if (dim == 0) {
            if (i >= actual.len or actual[i] <= 0) return null;
            out[i] = actual[i];
        } else {
            out[i] = dim;
        }
    }

    // Unsqueeze is represented by the graph IR as a reshape. When its input
    // contains dynamic dimensions, the converted target can contain multiple
    // -1 placeholders (for example [-1, -1] -> [1, 1, -1, -1]). Recover the
    // request dimensions by matching the declared input axes from the right;
    // any unmatched target axes must be inserted singleton dimensions.
    if (resolveRuntimeSingletonInsertion(actual, declared, target, input_numel, out)) |resolved| {
        return resolved;
    }
    if (resolveRuntimeSingletonRemoval(actual, declared, target, input_numel, out)) |resolved| {
        return resolved;
    }

    const declared_batch = if (declared.rank() > 0) declared.dim(0) else -1;
    if (actual[0] > 1 and out[0] > 1 and rank != actual.len and !hasNegativeDim(out[0..rank]) and
        (declared_batch <= 0 or declared_batch != actual[0]))
    {
        const target_numel = safeElementCountFromDims(out[0..rank]) orelse return null;
        if (target_numel > 0 and input_numel > target_numel and input_numel % target_numel == 0 and out[0] > 0) {
            const batch_factor = input_numel / target_numel;
            const scaled = @as(usize, @intCast(out[0])) * batch_factor;
            out[0] = std.math.cast(i64, scaled) orelse return null;
            if (safeElementCountFromDims(out[0..rank]) == input_numel) return out[0..rank];
        }
    }

    if (actual[0] > 1 and out[0] == 1 and !hasNegativeDim(out[0..rank])) {
        const target_numel = safeElementCountFromDims(out[0..rank]) orelse return null;
        if (target_numel > 0 and input_numel % target_numel == 0) {
            const batch_factor = input_numel / target_numel;
            if (batch_factor == @as(usize, @intCast(actual[0]))) {
                out[0] = actual[0];
                if (safeElementCountFromDims(out[0..rank]) == input_numel) return out[0..rank];
            }
        }
    }

    if (actual[0] > 1 and out[0] == 1 and hasNegativeDim(out[0..rank]) and
        (declared_batch <= 0 or declared_batch == 1 or declared_batch != actual[0]))
    {
        out[0] = actual[0];

        if (countNegativeDims(out[0..rank]) > 1 and rank == actual.len + 1 and rank >= 3) {
            for (1..actual.len - 1) |i| {
                if (out[i] < 0 and actual[i] > 0) out[i] = actual[i];
            }
            const input_last = actual[actual.len - 1];
            const target_last = out[rank - 1];
            if (input_last > 0 and target_last > 0 and @rem(input_last, target_last) == 0 and out[rank - 2] < 0) {
                out[rank - 2] = @divTrunc(input_last, target_last);
            }
        }

        if (resolveSingleInferredDim(out[0..rank], input_numel)) return out[0..rank];
        return null;
    }

    if (countNegativeDims(out[0..rank]) > 1) {
        if (resolveRuntimeCollapsedResizeDims(actual, target, input_numel, out)) |resolved| return resolved;
        if (resolveRuntimeInterleavedResizeDims(actual, target, input_numel, out)) |resolved| return resolved;
        if (resolveRuntimeSplitLastDim(actual, target, input_numel, out)) |resolved| return resolved;
        if (resolveRuntimeAlignedDynamicDims(actual, target, input_numel, out)) |resolved| return resolved;
    }

    if (countNegativeDims(out[0..rank]) <= 1 and resolveSingleInferredDim(out[0..rank], input_numel)) {
        return out[0..rank];
    }
    return null;
}

fn resolveRuntimeSingletonInsertion(
    actual: []const i64,
    declared: Shape,
    target: Shape,
    input_numel: usize,
    out: *[8]i64,
) ?[]const i64 {
    const target_rank = target.rank();
    const declared_rank = declared.rank();
    if (actual.len != declared_rank or target_rank <= declared_rank or target_rank > out.len) return null;

    for (0..target_rank) |axis| out[axis] = target.dim(@intCast(axis));

    var target_pos = target_rank;
    var declared_pos = declared_rank;
    while (declared_pos > 0) {
        declared_pos -= 1;
        const declared_dim = declared.dim(@intCast(declared_pos));
        var matched = false;
        while (target_pos > 0) {
            target_pos -= 1;
            const target_dim = target.dim(@intCast(target_pos));
            if (target_dim == declared_dim) {
                if (actual[declared_pos] <= 0) return null;
                out[target_pos] = actual[declared_pos];
                matched = true;
                break;
            }
            if (target_dim != 1) return null;
            out[target_pos] = 1;
        }
        if (!matched) return null;
    }
    while (target_pos > 0) {
        target_pos -= 1;
        if (target.dim(@intCast(target_pos)) != 1) return null;
        out[target_pos] = 1;
    }

    if (safeElementCountFromDims(out[0..target_rank]) != input_numel) return null;
    return out[0..target_rank];
}

fn resolveRuntimeSingletonRemoval(
    actual: []const i64,
    declared: Shape,
    target: Shape,
    input_numel: usize,
    out: *[8]i64,
) ?[]const i64 {
    const target_rank = target.rank();
    const declared_rank = declared.rank();
    if (actual.len != declared_rank or target_rank >= declared_rank or target_rank > out.len) return null;

    var target_pos = target_rank;
    var declared_pos = declared_rank;
    while (target_pos > 0) {
        target_pos -= 1;
        const target_dim = target.dim(@intCast(target_pos));
        var matched = false;
        while (declared_pos > 0) {
            declared_pos -= 1;
            const declared_dim = declared.dim(@intCast(declared_pos));
            if (target_dim == declared_dim) {
                if (actual[declared_pos] <= 0) return null;
                out[target_pos] = actual[declared_pos];
                matched = true;
                break;
            }
            if (declared_dim != 1 or actual[declared_pos] != 1) return null;
        }
        if (!matched) return null;
    }
    while (declared_pos > 0) {
        declared_pos -= 1;
        if (declared.dim(@intCast(declared_pos)) != 1 or actual[declared_pos] != 1) return null;
    }
    if (safeElementCountFromDims(out[0..target_rank]) != input_numel) return null;
    return out[0..target_rank];
}

fn resolveRuntimeCollapsedResizeDims(actual: []const i64, target: Shape, input_numel: usize, out: *[8]i64) ?[]const i64 {
    const rank = target.rank();
    if (actual.len != rank * 2 or rank == 0 or rank > out.len) return null;

    for (0..rank) |i| {
        const copied_axis = i * 2;
        const scale_axis = copied_axis + 1;
        const copied_dim = actual[copied_axis];
        const scale_dim = actual[scale_axis];
        if (copied_dim <= 0 or scale_dim <= 0) return null;

        const target_dim = target.dim(@intCast(i));
        const resolved_dim = std.math.mul(i64, copied_dim, scale_dim) catch return null;
        if (target_dim > 0 and target_dim != resolved_dim) return null;
        out[i] = resolved_dim;
    }

    if (safeElementCountFromDims(out[0..rank]) == input_numel) return out[0..rank];
    return null;
}

fn resolveRuntimeInterleavedResizeDims(actual: []const i64, target: Shape, input_numel: usize, out: *[8]i64) ?[]const i64 {
    const rank = target.rank();
    if (rank != actual.len * 2 or rank > out.len or actual.len == 0) return null;

    for (0..actual.len) |i| {
        const source_dim = actual[i];
        if (source_dim <= 0) return null;

        const copied_axis = i * 2;
        const inserted_axis = copied_axis + 1;
        const copied_dim = target.dim(@intCast(copied_axis));
        const inserted_dim = target.dim(@intCast(inserted_axis));

        if (copied_dim > 0 and copied_dim != source_dim) return null;
        if (inserted_dim != 1) return null;

        out[copied_axis] = source_dim;
        out[inserted_axis] = 1;
    }

    if (safeElementCountFromDims(out[0..rank]) == input_numel) return out[0..rank];
    return null;
}

fn resolveRuntimeAlignedDynamicDims(actual: []const i64, target: Shape, input_numel: usize, out: *[8]i64) ?[]const i64 {
    const rank = target.rank();
    if (actual.len != rank or rank > out.len) return null;
    for (0..rank) |i| {
        const dim = target.dim(@intCast(i));
        if (dim == 0) {
            if (actual[i] <= 0) return null;
            out[i] = actual[i];
        } else if (dim < 0) {
            if (actual[i] <= 0) return null;
            out[i] = actual[i];
        } else {
            out[i] = dim;
        }
    }
    if (safeElementCountFromDims(out[0..rank]) == input_numel) return out[0..rank];
    return null;
}

fn resolveRuntimeSplitLastDim(actual: []const i64, target: Shape, input_numel: usize, out: *[8]i64) ?[]const i64 {
    const rank = target.rank();
    if (rank != actual.len + 1 or rank < 3 or rank > out.len) return null;
    const actual_last = actual[actual.len - 1];
    if (actual_last <= 0) return null;

    for (0..rank) |i| out[i] = target.dim(@intCast(i));

    for (0..actual.len - 1) |i| {
        if (out[i] == 0 or out[i] < 0) {
            if (actual[i] <= 0) return null;
            out[i] = actual[i];
        } else if (actual[i] > 0 and out[i] != actual[i]) {
            return null;
        }
    }

    const split_axis = rank - 2;
    const last_axis = rank - 1;
    const split_dim = out[split_axis];
    const last_dim = out[last_axis];
    if (split_dim > 0 and last_dim > 0) {
        if (split_dim * last_dim != actual_last) return null;
    } else if (split_dim < 0 and last_dim > 0) {
        if (@rem(actual_last, last_dim) != 0) return null;
        out[split_axis] = @divTrunc(actual_last, last_dim);
    } else if (split_dim > 0 and last_dim < 0) {
        if (@rem(actual_last, split_dim) != 0) return null;
        out[last_axis] = @divTrunc(actual_last, split_dim);
    } else {
        return null;
    }

    if (safeElementCountFromDims(out[0..rank]) == input_numel) return out[0..rank];
    return null;
}

fn resolveFlattenedProjectionSuffixDims(src_actual: []const i64, target: Shape, input_numel: usize, out: *[8]i64) ?[]const i64 {
    const rank = target.rank();
    if (src_actual.len < 2 or rank <= src_actual.len - 1 or rank > out.len) return null;

    const prefix_rank = src_actual.len - 1;
    var prefix_numel: usize = 1;
    for (0..prefix_rank) |i| {
        const src_dim = src_actual[i];
        if (src_dim <= 0) return null;
        const target_dim = target.dim(@intCast(i));
        if (target_dim > 0 and target_dim != src_dim) return null;
        out[i] = src_dim;
        prefix_numel = std.math.mul(usize, prefix_numel, @intCast(src_dim)) catch return null;
    }
    if (prefix_numel == 0 or input_numel % prefix_numel != 0) return null;
    const projected_width = input_numel / prefix_numel;
    if (projected_width == 0) return null;

    var suffix_product: usize = 1;
    var unknown_suffix: ?usize = null;
    for (prefix_rank..rank) |i| {
        const dim = target.dim(@intCast(i));
        if (dim > 0) {
            out[i] = dim;
            suffix_product = std.math.mul(usize, suffix_product, @intCast(dim)) catch return null;
        } else if (dim < 0) {
            if (unknown_suffix != null) return null;
            unknown_suffix = i;
            out[i] = dim;
        } else {
            return null;
        }
    }

    if (unknown_suffix) |idx| {
        if (suffix_product == 0 or projected_width % suffix_product != 0) return null;
        out[idx] = @intCast(projected_width / suffix_product);
    } else if (suffix_product != projected_width) {
        return null;
    }

    if (safeElementCountFromDims(out[0..rank]) == input_numel) return out[0..rank];
    return null;
}

fn resolveProjectionRestoreFromSourceActual(src_actual: []const i64, target: Shape, input_numel: usize, out: *[8]i64) ?[]const i64 {
    if (resolveRuntimeAlignedDynamicDims(src_actual, target, input_numel, out)) |resolved| return resolved;
    if (resolveRuntimeSplitLastDim(src_actual, target, input_numel, out)) |resolved| return resolved;
    if (resolveFlattenedProjectionSuffixDims(src_actual, target, input_numel, out)) |resolved| return resolved;

    const rank = target.rank();
    if (src_actual.len == rank + 1 and rank >= 2 and rank <= out.len) {
        const target_last = target.dim(@intCast(rank - 1));
        if (target_last <= 0) return null;
        const src_second_last = src_actual[src_actual.len - 2];
        const src_last = src_actual[src_actual.len - 1];
        if (src_second_last <= 0 or src_last <= 0) return null;
        if (src_second_last * src_last != target_last) return null;

        for (0..rank - 1) |i| {
            const dim = target.dim(@intCast(i));
            if (dim > 0) {
                if (src_actual[i] > 0 and dim != src_actual[i]) return null;
                out[i] = dim;
            } else if (dim == 0 or dim < 0) {
                if (src_actual[i] <= 0) return null;
                out[i] = src_actual[i];
            }
        }
        out[rank - 1] = target_last;
        if (safeElementCountFromDims(out[0..rank]) == input_numel) return out[0..rank];
    }

    return null;
}

fn resolveFlattenedProjectionRestoreDims(
    graph: *const Graph,
    runtime_shapes: ?[]const ?[]const i64,
    input_node_id: NodeId,
    target: Shape,
    input_numel: usize,
    out: *[8]i64,
) ?[]const i64 {
    const producer = graph.node(input_node_id);
    if (std.meta.activeTag(producer.op) != .dot_general) return null;
    const producer_inputs = producer.getInputs();
    if (producer_inputs.len == 0 or producer_inputs[0] == null_node) return null;

    const lhs_id = producer_inputs[0];
    const lhs = graph.node(lhs_id);
    if (std.meta.activeTag(lhs.op) != .reshape or lhs.num_inputs == 0 or lhs.inputs[0] == null_node) return null;

    const src_id = lhs.inputs[0];
    const shapes = runtime_shapes orelse return null;
    if (src_id >= shapes.len or lhs_id >= shapes.len) return null;
    const src_actual = shapes[src_id] orelse return null;
    const lhs_actual = shapes[lhs_id] orelse return null;
    if (!isLeadingAxisFlatten(src_actual, lhs_actual)) return null;

    return resolveProjectionRestoreFromSourceActual(src_actual, target, input_numel, out);
}

fn isLeadingAxisFlatten(src_actual: []const i64, lhs_actual: []const i64) bool {
    if (src_actual.len < 2 or lhs_actual.len != 2) return false;
    const hidden = src_actual[src_actual.len - 1];
    if (hidden <= 0 or lhs_actual[1] != hidden) return false;
    var leading_product: i64 = 1;
    for (src_actual[0 .. src_actual.len - 1]) |dim| {
        if (dim <= 0) return false;
        leading_product = std.math.mul(i64, leading_product, dim) catch return false;
    }
    return lhs_actual[0] == leading_product;
}

fn isNonDonatedRuntimeInput(options: ExecuteOptions, node_id: NodeId) bool {
    if (options.runtime_inputs) |inputs| {
        for (inputs, 0..) |ri, idx| {
            if (ri.node_id != node_id) continue;
            if (options.donate) |donate| {
                if (idx < donate.len and donate[idx]) return false;
            }
            return true;
        }
    }
    return false;
}

/// Dispatch a single node to the backend, using side channels from
/// ExecState for stateful ops.
/// Derive the [batch*seq] 0/1 key mask from the additive attn_bias [bh,S,S]
/// (bias < -1e8 at padded keys). Mirrors metal_partition_executor's
/// attentionMaskFromBias so the fused-attention path matches the decomposed one.
fn disentangledMaskFromBias(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    attn_bias: CT,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
) ![]i64 {
    const bias = try cb.toFloat32(attn_bias, allocator);
    defer allocator.free(bias);
    const mask = try allocator.alloc(i64, batch * seq_len);
    errdefer allocator.free(mask);
    if (bias.len < batch * num_heads * seq_len * seq_len) {
        @memset(mask, 1);
        return mask;
    }
    for (0..batch) |b| {
        for (0..seq_len) |k| {
            const idx = ((b * num_heads) * seq_len + 0) * seq_len + k;
            mask[b * seq_len + k] = if (bias[idx] < -1.0e8) 0 else 1;
        }
    }
    return mask;
}

pub fn executeNode(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []const ?CT,
    node_id: NodeId,
    state: *ExecState,
) !CT {
    const n = graph.node(node_id);
    const ins = n.getInputs();

    // Helper to get a computed input CT
    const V = struct {
        vals: []const ?CT,

        fn get(self: @This(), id: NodeId) CT {
            return self.vals[id].?;
        }

        fn getOpt(self: @This(), id: NodeId) ?CT {
            if (id == null_node) return null;
            return self.vals[id];
        }
    }{ .vals = values };

    return switch (n.op) {
        // ── Constants & Parameters ────────────────────────────────────
        .parameter => |attrs| {
            const name = graph.parameterName(n);
            _ = attrs;
            return cb.getWeight(name);
        },

        .constant => |attrs| {
            const constant = try graph.constantDataAsF32(
                graph.allocator,
                n.output_shape.dtype,
                attrs.data_offset,
                attrs.data_len,
            );
            defer constant.deinit(graph.allocator);
            if (n.output_shape.rank() > 1) {
                var shape_buf: [8]i32 = undefined;
                const rank = n.output_shape.rank();
                for (0..rank) |ax| {
                    shape_buf[ax] = @intCast(n.output_shape.dim(@intCast(ax)));
                }
                return cb.fromFloat32Shape(constant.data, shape_buf[0..rank]);
            }
            return cb.fromFloat32(constant.data);
        },

        // ── Fused ops → backend dispatch ──────────────────────────────

        .fused_linear => |attrs| {
            return cb.linear(V.get(ins[0]), V.get(ins[1]), V.get(ins[2]), attrs.rows, attrs.in_dim, attrs.out_dim);
        },

        .fused_linear_no_bias => |attrs| {
            if (attrs.num_projections > 0) {
                return cb.linearNoBiasGrouped(
                    V.get(ins[0]),
                    V.get(ins[1]),
                    attrs.rows,
                    attrs.in_dim,
                    attrs.out_dim,
                    attrs.projection_out_dims[0..attrs.num_projections],
                    attrs.num_projections,
                );
            }
            return cb.linearNoBias(V.get(ins[0]), V.get(ins[1]), attrs.rows, attrs.in_dim, attrs.out_dim);
        },

        .fused_embedding_lookup => |attrs| {
            if (try cb.embeddingLookupTensor(V.get(ins[0]), V.get(ins[1]), attrs.total, attrs.dim)) |device_result| {
                return device_result;
            }
            var owned_ids: ?[]i64 = null;
            defer if (owned_ids) |buf| std.heap.page_allocator.free(buf);
            const ids = blk: {
                if (graph.node(ins[1]).op == .fused_from_float32) {
                    break :blk state.options.embedding_ids orelse return error.MissingRuntimeInput;
                }
                const raw = try cb.toFloat32(V.get(ins[1]), std.heap.page_allocator);
                defer std.heap.page_allocator.free(raw);
                const converted = try std.heap.page_allocator.alloc(i64, raw.len);
                for (converted, raw) |*dst, value| dst.* = @intFromFloat(@round(value));
                owned_ids = converted;
                break :blk converted;
            };
            return cb.embeddingLookup(V.get(ins[0]), ids, attrs.total, attrs.dim);
        },

        .fused_layer_norm => |attrs| {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.layerNormConsumeInput(V.get(ins[0]), V.get(ins[1]), V.get(ins[2]), attrs.dim, attrs.eps)) |consumed| return consumed;
            }
            return cb.layerNorm(V.get(ins[0]), V.get(ins[1]), V.get(ins[2]), attrs.dim, attrs.eps);
        },

        .fused_layer_norm_backward => |attrs| {
            return (try cb.layerNormBackward(V.get(ins[0]), V.get(ins[1]), V.get(ins[2]), V.get(ins[3]), attrs.dim, attrs.eps)) orelse error.UnsupportedPrimitiveOp;
        },

        .fused_rms_norm => |attrs| {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.rmsNormConsumeInput(V.get(ins[0]), V.get(ins[1]), attrs.dim, attrs.eps)) |consumed| return consumed;
            }
            return cb.rmsNorm(V.get(ins[0]), V.get(ins[1]), attrs.dim, attrs.eps);
        },

        .fused_gelu => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.unaryConsume(.gelu, V.get(ins[0]))) |consumed| return consumed;
            }
            return cb.gelu(V.get(ins[0]));
        },

        .fused_gelu_exact => {
            if (try cb.geluExact(V.get(ins[0]))) |device_result| return device_result;
            return executeExactGeluFallback(graph.allocator, n.output_shape, cb, V.get(ins[0]));
        },

        .fused_gelu_backward => {
            const elem_count_i64 = n.output_shape.numElements() orelse return error.UnsupportedShape;
            if (elem_count_i64 <= 0) return error.UnsupportedShape;
            if (try cb.decoderRuntimeApplyGeluBackward(&.{
                .input = V.get(ins[0]),
                .upstream_grad = V.get(ins[1]),
                .dim = @intCast(elem_count_i64),
                .exact = false,
            })) |fused| return fused;
            return executeGeluBackwardFallback(graph.allocator, graph.node(ins[0]).output_shape, cb, V.get(ins[0]), V.get(ins[1]), false);
        },

        .fused_gelu_exact_backward => {
            const elem_count_i64 = n.output_shape.numElements() orelse return error.UnsupportedShape;
            if (elem_count_i64 <= 0) return error.UnsupportedShape;
            if (try cb.decoderRuntimeApplyGeluBackward(&.{
                .input = V.get(ins[0]),
                .upstream_grad = V.get(ins[1]),
                .dim = @intCast(elem_count_i64),
                .exact = true,
            })) |fused| return fused;
            return executeGeluBackwardFallback(graph.allocator, graph.node(ins[0]).output_shape, cb, V.get(ins[0]), V.get(ins[1]), true);
        },

        .fused_relu => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.unaryConsume(.relu, V.get(ins[0]))) |consumed| return consumed;
            }
            return cb.relu(V.get(ins[0]));
        },

        .fused_silu => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.unaryConsume(.silu, V.get(ins[0]))) |consumed| return consumed;
            }
            return cb.silu(V.get(ins[0]));
        },

        .fused_quick_gelu => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.unaryConsume(.quick_gelu, V.get(ins[0]))) |consumed| return consumed;
            }
            return cb.quickGelu(V.get(ins[0]));
        },

        .fused_sigmoid => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.unaryConsume(.sigmoid, V.get(ins[0]))) |consumed| return consumed;
            }
            return cb.sigmoid(V.get(ins[0]));
        },

        .fused_tanh_act => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.unaryConsume(.tanh_act, V.get(ins[0]))) |consumed| return consumed;
            }
            return cb.tanh_act(V.get(ins[0]));
        },

        .fused_elem_add => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.addConsumeLeft(V.get(ins[0]), V.get(ins[1]))) |consumed| return consumed;
            }
            return cb.add(V.get(ins[0]), V.get(ins[1]));
        },

        .fused_elem_multiply => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.multiplyConsumeLeft(V.get(ins[0]), V.get(ins[1]))) |consumed| return consumed;
            }
            return cb.multiply(V.get(ins[0]), V.get(ins[1]));
        },

        .fused_masked_bce_with_logits_loss => |attrs| {
            var out_shape_buf: [8]i64 = undefined;
            const out_shape = fillShapeDims(graph, node_id, &out_shape_buf);
            return cb.maskedBceWithLogitsLoss(&.{
                .logits = V.get(ins[0]),
                .labels = V.get(ins[1]),
                .mask = V.get(ins[2]),
                .positive_weight = attrs.positive_weight,
                .negative_weight = attrs.negative_weight,
                .eps = attrs.eps,
                .mean_reduction = attrs.reduction == .mean,
                .output_shape = out_shape,
            });
        },

        .fused_masked_bce_with_logits_backward => |attrs| {
            var logits_shape_buf: [8]i64 = undefined;
            const logits_shape = fillShapeDims(graph, ins[0], &logits_shape_buf);
            return cb.maskedBceWithLogitsBackward(&.{
                .logits = V.get(ins[0]),
                .labels = V.get(ins[1]),
                .mask = V.get(ins[2]),
                .upstream = V.get(ins[3]),
                .positive_weight = attrs.positive_weight,
                .negative_weight = attrs.negative_weight,
                .eps = attrs.eps,
                .mean_reduction = attrs.reduction == .mean,
                .logits_shape = logits_shape,
            });
        },

        .fused_add_mul_scalar => {
            if (try cb.addMultiplyScalarTensor(V.get(ins[0]), V.get(ins[1]), V.get(ins[2]))) |fused| return fused;
            const sum = try cb.add(V.get(ins[0]), V.get(ins[1]));
            errdefer cb.free(sum);
            const scaled = try cb.multiply(sum, V.get(ins[2]));
            cb.free(sum);
            return scaled;
        },

        .fused_concat => |attrs| {
            return cb.concat(V.get(ins[0]), V.get(ins[1]), attrs.total, attrs.dim_a, attrs.dim_b);
        },

        .fused_sdpa => |attrs| {
            var batch = attrs.batch;
            var seq_len = attrs.seq_len;
            var num_heads = attrs.num_heads;
            var head_dim = attrs.head_dim;
            if (batch == 0 or seq_len == 0 or num_heads == 0 or head_dim == 0) {
                const tmp_alloc = std.heap.page_allocator;
                const actual = try cb.tensorShape(V.get(ins[0]), tmp_alloc);
                defer tmp_alloc.free(actual);
                if (actual.len == 4) {
                    if (batch == 0 and actual[0] > 0) batch = @intCast(actual[0]);
                    if (num_heads == 0 and actual[1] > 0) num_heads = @intCast(actual[1]);
                    if (seq_len == 0 and actual[2] > 0) seq_len = @intCast(actual[2]);
                    if (head_dim == 0 and actual[3] > 0) head_dim = @intCast(actual[3]);
                } else if (actual.len == 3) {
                    if (seq_len == 0 and actual[1] > 0) seq_len = @intCast(actual[1]);
                    if (head_dim == 0 and actual[2] > 0) head_dim = @intCast(actual[2]);
                    if (batch == 0 and num_heads > 0 and actual[0] > 0) batch = @intCast(@divFloor(actual[0], @as(i64, @intCast(num_heads))));
                }
            }
            var synthesized_mask: ?[]i64 = null;
            defer if (synthesized_mask) |buf| std.heap.page_allocator.free(buf);
            const mask = blk: {
                if (state.options.sdpa_mask) |runtime_mask| {
                    if (attrs.seq_len == 0 and batch > 0 and runtime_mask.len % batch == 0) {
                        seq_len = @intCast(runtime_mask.len / batch);
                    }
                    break :blk runtime_mask;
                }
                if (batch == 0) batch = 1;
                if (seq_len == 0) return error.MissingRuntimeInput;
                const full_mask = try std.heap.page_allocator.alloc(i64, batch * seq_len);
                @memset(full_mask, 1);
                synthesized_mask = full_mask;
                break :blk full_mask;
            };
            return cb.scaledDotProductAttention(
                V.get(ins[0]),
                V.get(ins[1]),
                V.get(ins[2]),
                mask,
                V.getOpt(if (n.num_inputs > 3) ins[3] else null_node),
                batch,
                seq_len,
                num_heads,
                head_dim,
            );
        },

        .fused_causal_self_attention => |attrs| {
            // If an attention context is provided, use paged attention
            // path (auto-incrementing layer_index).
            if (state.options.attention) |base_attn| {
                var attn = base_attn;
                attn.layer_index = if (attrs.layer_index == std.math.maxInt(u32))
                    state.attention_layer
                else
                    attrs.layer_index;
                attn.skip_kv_write = attrs.skip_kv_write;
                state.attention_layer += 1;
                return cb.gqaPagedAttention(
                    V.get(ins[0]),
                    V.get(ins[1]),
                    V.get(ins[2]),
                    V.getOpt(if (n.num_inputs > 3) ins[3] else null_node),
                    attn,
                    attrs.batch,
                    attrs.num_heads,
                    attrs.num_heads, // kv_heads = num_heads for non-GQA
                    attrs.head_dim,
                );
            }
            return cb.causalSelfAttention(
                V.get(ins[0]),
                V.get(ins[1]),
                V.get(ins[2]),
                V.getOpt(if (n.num_inputs > 3) ins[3] else null_node),
                attrs.batch,
                attrs.seq_len,
                attrs.num_heads,
                attrs.head_dim,
            );
        },

        .fused_cross_attention => |attrs| {
            const mask = state.options.cross_attention_mask orelse
                return error.MissingRuntimeInput;
            return cb.crossAttention(
                V.get(ins[0]),
                V.get(ins[1]),
                V.get(ins[2]),
                mask,
                attrs.batch,
                attrs.dec_seq,
                attrs.enc_seq,
                attrs.num_heads,
                attrs.head_dim,
            );
        },

        .fused_gqa_causal_attention => |attrs| {
            // If an attention context is provided, route through
            // gqaPagedAttention with auto-incremented layer_index.
            if (state.options.attention) |base_attn| {
                var attn = base_attn;
                attn.layer_index = if (attrs.layer_index == std.math.maxInt(u32))
                    state.attention_layer
                else
                    attrs.layer_index;
                attn.skip_kv_write = attrs.skip_kv_write;
                state.attention_layer += 1;
                return cb.gqaPagedAttention(
                    V.get(ins[0]),
                    V.get(ins[1]),
                    V.get(ins[2]),
                    V.getOpt(if (n.num_inputs > 3) ins[3] else null_node),
                    attn,
                    attrs.batch,
                    attrs.num_heads,
                    attrs.num_kv_heads,
                    attrs.head_dim,
                );
            }
            return cb.gqaCausalAttention(
                V.get(ins[0]),
                V.get(ins[1]),
                V.get(ins[2]),
                V.getOpt(if (n.num_inputs > 3) ins[3] else null_node),
                attrs.batch,
                attrs.seq_len,
                attrs.num_heads,
                attrs.num_kv_heads,
                attrs.head_dim,
            );
        },

        .fused_disentangled_attention => |attrs| {
            // input0 = qkv_packed [3*B*S, H]; input1 = qr_kr_packed [2*num_rel, H];
            // input2 = attn_bias [bh, S, S]. Slice the packed inputs back into
            // q/k/v and q_r/k_r, derive the padding mask from attn_bias, then
            // call the fused attention op (device kernel / host fallback).
            if (try cb.disentangledRelativeAttentionPacked(
                V.get(ins[0]),
                V.get(ins[1]),
                V.get(ins[2]),
                attrs.batch,
                attrs.seq_len,
                attrs.num_heads,
                attrs.head_dim,
            )) |packed_result| return packed_result;
            const bs: i64 = @intCast(attrs.batch * attrs.seq_len);
            const h: i64 = @intCast(attrs.num_heads * attrs.head_dim);
            const num_rel: i64 = @intCast(2 * attrs.seq_len - 1);
            const qkv = V.get(ins[0]);
            const qkv_shape = [_]i64{ 3 * bs, h };
            const q = try cb.primSlice(qkv, &.{ 0, 0 }, &.{ bs, h }, &.{ 1, 1 }, &qkv_shape);
            defer cb.free(q);
            const k = try cb.primSlice(qkv, &.{ bs, 0 }, &.{ 2 * bs, h }, &.{ 1, 1 }, &qkv_shape);
            defer cb.free(k);
            const v = try cb.primSlice(qkv, &.{ 2 * bs, 0 }, &.{ 3 * bs, h }, &.{ 1, 1 }, &qkv_shape);
            defer cb.free(v);
            const qr_kr = V.get(ins[1]);
            const qr_shape = [_]i64{ 2 * num_rel, h };
            const q_r = try cb.primSlice(qr_kr, &.{ 0, 0 }, &.{ num_rel, h }, &.{ 1, 1 }, &qr_shape);
            defer cb.free(q_r);
            const k_r = try cb.primSlice(qr_kr, &.{ num_rel, 0 }, &.{ 2 * num_rel, h }, &.{ 1, 1 }, &qr_shape);
            defer cb.free(k_r);
            const mask = try disentangledMaskFromBias(graph.allocator, cb, V.get(ins[2]), attrs.batch, attrs.seq_len, attrs.num_heads);
            defer graph.allocator.free(mask);
            return cb.disentangledRelativeAttention(q, k, v, q_r, k_r, mask, attrs.batch, attrs.seq_len, attrs.num_heads, attrs.head_dim);
        },

        .fused_disentangled_attention_backward => |attrs| {
            // input0/1/2 as forward; input3 = dOut [B*S, H]. Returns packed
            // grads [dQ;dK;dV;dQ_r;dK_r] = [3*B*S + 2*num_rel, H].
            if (try cb.disentangledRelativeAttentionBackwardPacked(
                V.get(ins[0]),
                V.get(ins[1]),
                V.get(ins[2]),
                V.get(ins[3]),
                attrs.batch,
                attrs.seq_len,
                attrs.num_heads,
                attrs.head_dim,
            )) |packed_result| return packed_result;
            const bs: i64 = @intCast(attrs.batch * attrs.seq_len);
            const h: i64 = @intCast(attrs.num_heads * attrs.head_dim);
            const num_rel: i64 = @intCast(2 * attrs.seq_len - 1);
            const qkv = V.get(ins[0]);
            const qkv_shape = [_]i64{ 3 * bs, h };
            const q = try cb.primSlice(qkv, &.{ 0, 0 }, &.{ bs, h }, &.{ 1, 1 }, &qkv_shape);
            defer cb.free(q);
            const k = try cb.primSlice(qkv, &.{ bs, 0 }, &.{ 2 * bs, h }, &.{ 1, 1 }, &qkv_shape);
            defer cb.free(k);
            const v = try cb.primSlice(qkv, &.{ 2 * bs, 0 }, &.{ 3 * bs, h }, &.{ 1, 1 }, &qkv_shape);
            defer cb.free(v);
            const qr_kr = V.get(ins[1]);
            const qr_shape = [_]i64{ 2 * num_rel, h };
            const q_r = try cb.primSlice(qr_kr, &.{ 0, 0 }, &.{ num_rel, h }, &.{ 1, 1 }, &qr_shape);
            defer cb.free(q_r);
            const k_r = try cb.primSlice(qr_kr, &.{ num_rel, 0 }, &.{ 2 * num_rel, h }, &.{ 1, 1 }, &qr_shape);
            defer cb.free(k_r);
            const mask = try disentangledMaskFromBias(graph.allocator, cb, V.get(ins[2]), attrs.batch, attrs.seq_len, attrs.num_heads);
            defer graph.allocator.free(mask);
            return cb.disentangledRelativeAttentionBackward(q, k, v, q_r, k_r, mask, V.get(ins[3]), attrs.batch, attrs.seq_len, attrs.num_heads, attrs.head_dim);
        },

        .fused_relative_position_bias => |attrs| {
            return cb.relativePositionBias(
                V.get(ins[0]),
                attrs.q_len,
                attrs.k_len,
                attrs.num_heads,
                attrs.num_buckets,
                attrs.max_distance,
                attrs.bidirectional,
            );
        },

        .fused_rope => |attrs| {
            const rope_dim: usize = if (attrs.rope_dim > 0) attrs.rope_dim else attrs.head_dim;
            const position_offset = if (state.options.attention) |attn|
                attn.total_sequence_len - attn.query_sequence_len
            else
                attrs.position_offset;
            return cb.rope(
                V.get(ins[0]),
                attrs.seq_len,
                attrs.head_dim,
                rope_dim,
                attrs.theta,
                attrs.freq_scale,
                position_offset,
                attrs.consecutive_pairs,
            );
        },

        .fused_conv1d => |attrs| {
            const input_actual = cb.tensorShape(V.get(ins[0]), std.heap.page_allocator) catch null;
            defer if (input_actual) |dims| std.heap.page_allocator.free(dims);
            const input_declared = graph.node(ins[0]).output_shape;
            return cb.conv1d(
                V.get(ins[0]),
                V.get(ins[1]),
                V.get(ins[2]),
                try positiveResolvedDim(input_actual, input_declared, 0),
                try positiveResolvedDim(input_actual, input_declared, 1),
                attrs.out_channels,
                try positiveResolvedDim(input_actual, input_declared, 2),
                attrs.kernel_size,
                attrs.stride,
                attrs.padding,
            );
        },

        .fused_conv2d => |attrs| {
            const input_actual = cb.tensorShape(V.get(ins[0]), std.heap.page_allocator) catch null;
            defer if (input_actual) |dims| std.heap.page_allocator.free(dims);
            const input_declared = graph.node(ins[0]).output_shape;
            return cb.conv2d(
                V.get(ins[0]),
                V.get(ins[1]),
                V.get(ins[2]),
                try positiveResolvedDim(input_actual, input_declared, 0),
                try positiveResolvedDim(input_actual, input_declared, 1),
                attrs.out_channels,
                try positiveResolvedDim(input_actual, input_declared, 2),
                try positiveResolvedDim(input_actual, input_declared, 3),
                attrs.kernel_h,
                attrs.kernel_w,
                attrs.stride_h,
                attrs.stride_w,
                attrs.padding_h,
                attrs.padding_w,
                attrs.groups,
            );
        },

        .fused_windowed_self_attention => {
            // Complex op with >4 inputs — needs extended input support.
            return error.MissingRuntimeInput;
        },

        .fused_channel_self_attention => {
            // Complex op with >4 inputs — needs extended input support.
            return error.MissingRuntimeInput;
        },

        .fused_linear_no_bias_pair => |attrs| {
            const result = try cb.linearNoBiasPair(
                V.get(ins[0]),
                V.get(ins[1]),
                V.get(ins[2]),
                attrs.rows,
                attrs.in_dim,
                attrs.out_dim,
            );
            // Returns the first output; the second is stashed in ExecState
            // and picked up by the downstream fused_to_float32 node.
            state.pair_second = result.second;
            return result.first;
        },

        .fused_moe_linear_no_bias => |attrs| {
            const grouped = state.moe_grouped orelse return error.MissingRuntimeInput;
            return (try cb.moeLinearNoBias(
                V.get(ins[0]),
                grouped.expert_ids,
                grouped.expert_tile_ids,
                grouped.tile_row_starts,
                grouped.tile_row_counts,
                V.get(ins[1]),
                grouped.rows.len,
                attrs.in_dim,
                attrs.out_dim,
            )) orelse return error.MissingRuntimeInput;
        },

        .fused_moe_linear_no_bias_pair => |attrs| {
            const grouped = state.moe_grouped orelse return error.MissingRuntimeInput;
            const result = (try cb.moeLinearNoBiasPair(
                V.get(ins[0]),
                grouped.expert_ids,
                V.get(ins[1]),
                V.get(ins[2]),
                grouped.rows.len,
                attrs.in_dim,
                attrs.out_dim,
            )) orelse return error.MissingRuntimeInput;
            _ = result.second;
            return result.first;
        },

        .fused_moe_scatter_add => |attrs| {
            const grouped = state.moe_grouped orelse return error.MissingRuntimeInput;
            // Apply per-expert output scale (input 3) to route weights if present.
            const n_node = graph.node(node_id);
            if (n_node.num_inputs >= 4 and n_node.inputs[3] != null_node) {
                const alloc = std.heap.page_allocator;
                const scale = try cb.toFloat32(V.get(n_node.inputs[3]), alloc);
                defer alloc.free(scale);
                for (grouped.route_weights, grouped.expert_ids) |*w, eid| {
                    if (eid < scale.len) w.* *= scale[eid];
                }
            }
            return (try cb.moeScatterAdd(
                V.get(ins[0]),
                grouped.rows,
                grouped.route_weights,
                V.get(ins[1]),
                grouped.rows.len,
                attrs.dim,
            )) orelse return error.MissingRuntimeInput;
        },

        .fused_moe_select_routes => |attrs| {
            const alloc = std.heap.page_allocator;
            const sel = (try cb.moeSelectRoutes(
                attrs.layer_index,
                V.get(ins[0]),
                attrs.rows,
                attrs.num_experts,
                attrs.top_k,
                1.0,
                alloc,
            )) orelse return error.MissingRuntimeInput;
            // Free previous layer's routing state if any.
            state.freeMoeState();
            // Build grouped batch from flat routing and save for
            // subsequent fused_moe_linear_no_bias / fused_moe_scatter_add.
            state.moe_grouped = try buildGroupedFromRouting(alloc, sel);
            state.moe_routes = sel;
            state.moe_routes_allocator = alloc;
            // Return a fresh dummy tensor (not the input passthrough) so
            // liveness-based freeing doesn't double-free the input CT.
            // MoE ops that reference this node use state.moe_grouped, not
            // this value.
            const dummy = [_]f32{0.0};
            return cb.fromFloat32(&dummy);
        },

        .fused_take_rows => |attrs| {
            const grouped = state.moe_grouped orelse return error.MissingRuntimeInput;
            return (try cb.takeRows(
                V.get(ins[0]),
                grouped.rows,
                grouped.rows.len,
                attrs.dim,
            )) orelse return error.MissingRuntimeInput;
        },

        .fused_from_float32 => {
            // Runtime data placeholder — must be supplied via runtime_inputs.
            return error.MissingRuntimeInput;
        },

        .fused_to_float32 => {
            // When produced by fused_linear_no_bias_pair, this carries the
            // pair's second output (stashed in ExecState). Otherwise it's
            // a graph output marker and we pass the input through.
            if (state.pair_second) |second| {
                state.pair_second = null;
                return second;
            }
            return V.get(ins[0]);
        },

        .fused_zero_tensor => |attrs| {
            if (try cb.zeroTensor(attrs.rows, attrs.out_dim)) |ct| {
                return ct;
            }
            // Fallback: create a zero-filled f32 tensor
            const size = @as(usize, attrs.rows) * @as(usize, attrs.out_dim);
            const zeros = try std.heap.page_allocator.alloc(f32, size);
            defer std.heap.page_allocator.free(zeros);
            @memset(zeros, 0.0);
            return cb.fromFloat32(zeros);
        },

        .fused_eval_tensor => {
            // Scheduling barrier — evaluate the input tensor.
            try cb.evalTensor(V.get(ins[0]));
            return V.get(ins[0]);
        },

        .fused_softmax => |attrs| {
            const input_ct = V.get(ins[0]);
            var input_r = ensureDeclaredShape(cb, input_ct, graph.node(ins[0]).output_shape);
            const result = blk: {
                if (input_r) |reshaped| {
                    defer if (input_r != null) cb.free(reshaped);
                    if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                        if (try cb.softmaxConsume(reshaped, attrs.dim)) |consumed| {
                            if (consumed == reshaped) input_r = null;
                            break :blk consumed;
                        }
                    }
                    break :blk try cb.primSoftmax(reshaped, attrs.dim);
                }
                if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                    if (try cb.softmaxConsume(input_ct, attrs.dim)) |consumed| break :blk consumed;
                }
                break :blk try cb.primSoftmax(input_ct, attrs.dim);
            };
            const out_rank = n.output_shape.rank();
            if (out_rank < 1) return result;
            var out_dims: [8]i64 = undefined;
            const actual = cb.tensorShape(result, std.heap.page_allocator) catch return result;
            defer std.heap.page_allocator.free(actual);
            const runtime_dims = resolveRuntimeReshapeDims(actual, graph.node(ins[0]).output_shape, n.output_shape, &out_dims) orelse return result;
            if (safeElementCountFromDims(runtime_dims) != safeElementCountFromDims(actual)) return result;
            const reshaped = cb.primReshape(result, runtime_dims) catch return result;
            // A consume path may return the graph input handle itself. That
            // handle is still present in `values[ins[0]]` and the normal
            // last-use cleanup owns it; freeing it here would double-free it.
            // Fresh results and temporary declared-shape aliases are local to
            // this branch and must release their pre-reshape handle.
            if (reshaped != result and result != input_ct) cb.free(result);
            return reshaped;
        },

        .fused_log_softmax => |attrs| {
            const input_ct = V.get(ins[0]);
            var input_r = ensureDeclaredShape(cb, input_ct, graph.node(ins[0]).output_shape);
            const result = blk: {
                if (input_r) |reshaped| {
                    defer if (input_r != null) cb.free(reshaped);
                    if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                        if (try cb.logSoftmaxConsume(reshaped, attrs.dim)) |consumed| {
                            if (consumed == reshaped) input_r = null;
                            break :blk consumed;
                        }
                    }
                    break :blk try cb.primLogSoftmax(reshaped, attrs.dim);
                }
                if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                    if (try cb.logSoftmaxConsume(input_ct, attrs.dim)) |consumed| break :blk consumed;
                }
                break :blk try cb.primLogSoftmax(input_ct, attrs.dim);
            };
            const out_rank = n.output_shape.rank();
            if (out_rank < 1) return result;
            var out_dims: [8]i64 = undefined;
            const actual = cb.tensorShape(result, std.heap.page_allocator) catch return result;
            defer std.heap.page_allocator.free(actual);
            const runtime_dims = resolveRuntimeReshapeDims(actual, graph.node(ins[0]).output_shape, n.output_shape, &out_dims) orelse return result;
            if (safeElementCountFromDims(runtime_dims) != safeElementCountFromDims(actual)) return result;
            const reshaped = cb.primReshape(result, runtime_dims) catch return result;
            if (reshaped != result and result != input_ct) cb.free(result);
            return reshaped;
        },

        .fused_argmax_last_row => |attrs| {
            // argmax returns a scalar u32, not a tensor CT. Run the
            // op for its side effect but return the input unchanged
            // (the graph path is for tracing structure, the actual
            // sampling happens outside).
            _ = try cb.argmaxLastRow(V.get(ins[0]), attrs.rows, attrs.dim);
            return V.get(ins[0]);
        },

        // ── Primitive ops → backend dispatch ─────────────────────────
        // These appear in lowered/gradient graphs produced by autodiff.
        // Each dispatches to an optional VTable method on the backend.

        .neg => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.unaryConsume(.negate, V.get(ins[0]))) |consumed| return consumed;
            }
            return cb.primNegate(V.get(ins[0]));
        },
        .sqrt => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.unaryConsume(.sqrt, V.get(ins[0]))) |consumed| return consumed;
            }
            return cb.primSqrt(V.get(ins[0]));
        },
        .rsqrt => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.unaryConsume(.rsqrt, V.get(ins[0]))) |consumed| return consumed;
            }
            return cb.primRsqrt(V.get(ins[0]));
        },
        .exp => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.unaryConsume(.exp, V.get(ins[0]))) |consumed| return consumed;
            }
            return cb.primExp(V.get(ins[0]));
        },
        .log => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.unaryConsume(.log, V.get(ins[0]))) |consumed| return consumed;
            }
            return cb.primLog(V.get(ins[0]));
        },
        .sin => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.unaryConsume(.sin, V.get(ins[0]))) |consumed| return consumed;
            }
            return cb.primSin(V.get(ins[0]));
        },
        .cos => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.unaryConsume(.cos, V.get(ins[0]))) |consumed| return consumed;
            }
            return cb.primCos(V.get(ins[0]));
        },
        .tanh => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.unaryConsume(.tanh_prim, V.get(ins[0]))) |consumed| return consumed;
            }
            return cb.primTanh(V.get(ins[0]));
        },
        .erf => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.unaryConsume(.erf, V.get(ins[0]))) |consumed| return consumed;
            }
            return cb.primErf(V.get(ins[0]));
        },
        .abs => {
            if (state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (try cb.unaryConsume(.abs, V.get(ins[0]))) |consumed| return consumed;
            }
            return cb.primAbs(V.get(ins[0]));
        },
        .add, .mul, .sub, .div, .less_than => {
            // For backends that track tensor shapes (MLX), ensure inputs
            // match their declared shapes before the binary op. The native
            // backend uses flat arrays and ignores shapes, but MLX uses
            // numpy-style broadcasting which requires correct shapes.
            const a_val = V.get(ins[0]);
            const b_val = V.get(ins[1]);
            var a_reshaped = ensureDeclaredShape(cb, a_val, graph.node(ins[0]).output_shape);
            var b_reshaped = ensureDeclaredShape(cb, b_val, graph.node(ins[1]).output_shape);
            defer {
                if (a_reshaped) |r| cb.free(r);
                if (b_reshaped) |r| cb.free(r);
            }
            const a_declared_ct = a_reshaped orelse a_val;
            const b_declared_ct = b_reshaped orelse b_val;
            const a_actual = cb.tensorShape(a_declared_ct, std.heap.page_allocator) catch null;
            defer if (a_actual) |shape| std.heap.page_allocator.free(shape);
            const b_actual = cb.tensorShape(b_declared_ct, std.heap.page_allocator) catch null;
            defer if (b_actual) |shape| std.heap.page_allocator.free(shape);
            var a_broadcast: ?CT = null;
            defer if (a_broadcast) |value| cb.free(value);
            var b_broadcast: ?CT = null;
            defer if (b_broadcast) |value| cb.free(value);
            if (a_actual != null and b_actual != null and !std.mem.eql(i64, a_actual.?, b_actual.?)) {
                var target_buf: [ml.graph.shape.max_rank]i64 = undefined;
                const target = resolveRuntimeBroadcastShape(a_actual.?, b_actual.?, &target_buf) orelse
                    return error.ShapeMismatch;
                a_broadcast = try runtimeBroadcastOperand(cb, a_declared_ct, a_actual.?, target);
                b_broadcast = try runtimeBroadcastOperand(cb, b_declared_ct, b_actual.?, target);
            }
            const a_ct = a_broadcast orelse a_declared_ct;
            const b_ct = b_broadcast orelse b_declared_ct;
            const can_donate = a_broadcast == null and b_broadcast == null;
            if (can_donate and n.op == .add and state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (cb.addConsumeLeft(a_ct, b_ct) catch |err| switch (err) {
                    error.ShapeMismatch, error.UnsupportedShape, error.UnsupportedPrimitiveOp => null,
                    else => return err,
                }) |consumed| {
                    if (a_reshaped != null and consumed == a_ct) a_reshaped = null;
                    if (b_reshaped != null and consumed == b_ct) b_reshaped = null;
                    return consumed;
                }
            }
            if (can_donate and n.op == .add and state.isLastUseBy(ins[1], node_id) and !isNonDonatedRuntimeInput(state.options, ins[1])) {
                if (cb.addConsumeRight(a_ct, b_ct) catch |err| switch (err) {
                    error.ShapeMismatch, error.UnsupportedShape, error.UnsupportedPrimitiveOp => null,
                    else => return err,
                }) |consumed| {
                    if (a_reshaped != null and consumed == a_ct) a_reshaped = null;
                    if (b_reshaped != null and consumed == b_ct) b_reshaped = null;
                    return consumed;
                }
            }
            if (can_donate and n.op == .mul and state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (cb.multiplyConsumeLeft(a_ct, b_ct) catch |err| switch (err) {
                    error.ShapeMismatch, error.UnsupportedShape, error.UnsupportedPrimitiveOp => null,
                    else => return err,
                }) |consumed| {
                    if (a_reshaped != null and consumed == a_ct) a_reshaped = null;
                    if (b_reshaped != null and consumed == b_ct) b_reshaped = null;
                    return consumed;
                }
            }
            if (can_donate and n.op == .mul and state.isLastUseBy(ins[1], node_id) and !isNonDonatedRuntimeInput(state.options, ins[1])) {
                if (cb.multiplyConsumeRight(a_ct, b_ct) catch |err| switch (err) {
                    error.ShapeMismatch, error.UnsupportedShape, error.UnsupportedPrimitiveOp => null,
                    else => return err,
                }) |consumed| {
                    if (a_reshaped != null and consumed == a_ct) a_reshaped = null;
                    if (b_reshaped != null and consumed == b_ct) b_reshaped = null;
                    return consumed;
                }
            }
            if (can_donate and n.op == .sub and state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (cb.subtractConsumeLeft(a_ct, b_ct) catch |err| switch (err) {
                    error.ShapeMismatch, error.UnsupportedShape, error.UnsupportedPrimitiveOp => null,
                    else => return err,
                }) |consumed| {
                    if (a_reshaped != null and consumed == a_ct) a_reshaped = null;
                    if (b_reshaped != null and consumed == b_ct) b_reshaped = null;
                    return consumed;
                }
            }
            if (can_donate and n.op == .div and state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (cb.divideConsumeLeft(a_ct, b_ct) catch |err| switch (err) {
                    error.ShapeMismatch, error.UnsupportedShape, error.UnsupportedPrimitiveOp => null,
                    else => return err,
                }) |consumed| {
                    if (a_reshaped != null and consumed == a_ct) a_reshaped = null;
                    if (b_reshaped != null and consumed == b_ct) b_reshaped = null;
                    return consumed;
                }
            }
            if (can_donate and n.op == .less_than and state.isLastUseBy(ins[0], node_id) and !isNonDonatedRuntimeInput(state.options, ins[0])) {
                if (cb.lessThanConsumeLeft(a_ct, b_ct) catch |err| switch (err) {
                    error.ShapeMismatch, error.UnsupportedShape, error.UnsupportedPrimitiveOp => null,
                    else => return err,
                }) |consumed| {
                    if (a_reshaped != null and consumed == a_ct) a_reshaped = null;
                    if (b_reshaped != null and consumed == b_ct) b_reshaped = null;
                    return consumed;
                }
            }
            const result = switch (n.op) {
                .add => cb.add(a_ct, b_ct),
                .mul => cb.multiply(a_ct, b_ct),
                .sub => cb.primSubtract(a_ct, b_ct),
                .div => cb.primDivide(a_ct, b_ct),
                .less_than => cb.primLessThan(a_ct, b_ct) catch |err| {
                    if (err == error.ShapeMismatch) {
                        std.log.warn("less_than execution failed node_id={d} lhs_id={d} rhs_id={d} lhs_shape={any} rhs_shape={any}", .{
                            node_id,
                            ins[0],
                            ins[1],
                            graph.node(ins[0]).output_shape,
                            graph.node(ins[1]).output_shape,
                        });
                        std.log.warn("less_than operand ops lhs_op={s} rhs_op={s}", .{
                            @tagName(std.meta.activeTag(graph.node(ins[0]).op)),
                            @tagName(std.meta.activeTag(graph.node(ins[1]).op)),
                        });
                        if (std.meta.activeTag(graph.node(ins[0]).op) == .less_than) {
                            const prev = graph.node(ins[0]);
                            std.log.warn("less_than lhs upstream ids={d},{d} shapes={any},{any}", .{
                                prev.inputs[0],
                                prev.inputs[1],
                                graph.node(prev.inputs[0]).output_shape,
                                graph.node(prev.inputs[1]).output_shape,
                            });
                        }
                    }
                    return err;
                },
                else => unreachable,
            } catch |err| {
                const lhs_actual = cb.tensorShape(a_ct, std.heap.page_allocator) catch null;
                defer if (lhs_actual) |shape| std.heap.page_allocator.free(shape);
                const rhs_actual = cb.tensorShape(b_ct, std.heap.page_allocator) catch null;
                defer if (rhs_actual) |shape| std.heap.page_allocator.free(shape);
                std.log.warn("binary op failed node_id={d} op={s} lhs_id={d} rhs_id={d} lhs_op={s} rhs_op={s} lhs_declared={any} rhs_declared={any} lhs_actual={?any} rhs_actual={?any} err={s}", .{
                    node_id,
                    @tagName(n.op),
                    ins[0],
                    ins[1],
                    @tagName(std.meta.activeTag(graph.node(ins[0]).op)),
                    @tagName(std.meta.activeTag(graph.node(ins[1]).op)),
                    graph.node(ins[0]).output_shape,
                    graph.node(ins[1]).output_shape,
                    lhs_actual,
                    rhs_actual,
                    @errorName(err),
                });
                const lhs_inputs = graph.node(ins[0]).getInputs();
                const rhs_inputs = graph.node(ins[1]).getInputs();
                if (lhs_inputs.len > 0) {
                    std.log.warn("binary lhs input0 id={d} op={s} shape={any}", .{
                        lhs_inputs[0],
                        @tagName(std.meta.activeTag(graph.node(lhs_inputs[0]).op)),
                        graph.node(lhs_inputs[0]).output_shape,
                    });
                }
                if (lhs_inputs.len > 1) {
                    std.log.warn("binary lhs input1 id={d} op={s} shape={any}", .{
                        lhs_inputs[1],
                        @tagName(std.meta.activeTag(graph.node(lhs_inputs[1]).op)),
                        graph.node(lhs_inputs[1]).output_shape,
                    });
                }
                if (rhs_inputs.len > 0) {
                    std.log.warn("binary rhs input0 id={d} op={s} shape={any}", .{
                        rhs_inputs[0],
                        @tagName(std.meta.activeTag(graph.node(rhs_inputs[0]).op)),
                        graph.node(rhs_inputs[0]).output_shape,
                    });
                    const rhs0_inputs = graph.node(rhs_inputs[0]).getInputs();
                    if (rhs0_inputs.len > 0) {
                        std.log.warn("binary rhs input0 source0 id={d} op={s} shape={any}", .{
                            rhs0_inputs[0],
                            @tagName(std.meta.activeTag(graph.node(rhs0_inputs[0]).op)),
                            graph.node(rhs0_inputs[0]).output_shape,
                        });
                    }
                    if (rhs0_inputs.len > 1) {
                        std.log.warn("binary rhs input0 source1 id={d} op={s} shape={any}", .{
                            rhs0_inputs[1],
                            @tagName(std.meta.activeTag(graph.node(rhs0_inputs[1]).op)),
                            graph.node(rhs0_inputs[1]).output_shape,
                        });
                    }
                }
                if (rhs_inputs.len > 1) {
                    std.log.warn("binary rhs input1 id={d} op={s} shape={any}", .{
                        rhs_inputs[1],
                        @tagName(std.meta.activeTag(graph.node(rhs_inputs[1]).op)),
                        graph.node(rhs_inputs[1]).output_shape,
                    });
                }
                return err;
            };
            return result;
        },
        .where_select => {
            var cond_r = ensureDeclaredShape(cb, V.get(ins[0]), graph.node(ins[0]).output_shape);
            defer if (cond_r) |r| cb.free(r);
            var true_r = ensureDeclaredShape(cb, V.get(ins[1]), graph.node(ins[1]).output_shape);
            defer if (true_r) |r| cb.free(r);
            var false_r = ensureDeclaredShape(cb, V.get(ins[2]), graph.node(ins[2]).output_shape);
            defer if (false_r) |r| cb.free(r);
            const cond_ct = cond_r orelse V.get(ins[0]);
            const true_ct = true_r orelse V.get(ins[1]);
            const false_ct = false_r orelse V.get(ins[2]);
            if (state.isLastUseBy(ins[1], node_id) and !isNonDonatedRuntimeInput(state.options, ins[1])) {
                if (cb.whereSelectConsumeTrue(cond_ct, true_ct, false_ct) catch |err| switch (err) {
                    error.ShapeMismatch, error.UnsupportedShape, error.UnsupportedPrimitiveOp => null,
                    else => return err,
                }) |consumed| {
                    if (true_r != null and consumed == true_ct) true_r = null;
                    if (false_r != null and consumed == false_ct) false_r = null;
                    if (cond_r != null and consumed == cond_ct) cond_r = null;
                    return consumed;
                }
            }
            if (state.isLastUseBy(ins[2], node_id) and !isNonDonatedRuntimeInput(state.options, ins[2])) {
                if (cb.whereSelectConsumeFalse(cond_ct, true_ct, false_ct) catch |err| switch (err) {
                    error.ShapeMismatch, error.UnsupportedShape, error.UnsupportedPrimitiveOp => null,
                    else => return err,
                }) |consumed| {
                    if (true_r != null and consumed == true_ct) true_r = null;
                    if (false_r != null and consumed == false_ct) false_r = null;
                    if (cond_r != null and consumed == cond_ct) cond_r = null;
                    return consumed;
                }
            }
            const result = try cb.primWhereSelect(cond_ct, true_ct, false_ct);
            return result;
        },

        .reduce_sum => |attrs| {
            var sbuf: [8]i64 = undefined;
            const in_shape = fillShapeDims(graph, ins[0], &sbuf);
            return cb.primReduceSum(V.get(ins[0]), attrs.axes[0..attrs.num_axes], in_shape);
        },
        .reduce_max => |attrs| {
            var sbuf: [8]i64 = undefined;
            const in_shape = fillShapeDims(graph, ins[0], &sbuf);
            return cb.primReduceMax(V.get(ins[0]), attrs.axes[0..attrs.num_axes], in_shape);
        },
        .reduce_mean => |attrs| {
            var sbuf: [8]i64 = undefined;
            const in_shape = fillShapeDims(graph, ins[0], &sbuf);
            return cb.primReduceMean(V.get(ins[0]), attrs.axes[0..attrs.num_axes], in_shape);
        },
        .argmax => |attrs| {
            var sbuf: [8]i64 = undefined;
            const in_shape = fillShapeDims(graph, ins[0], &sbuf);
            return cb.primArgMax(V.get(ins[0]), attrs.axis, attrs.keepdims, in_shape);
        },
        .reshape => |attrs| {
            const rank = attrs.new_shape.rank();
            var dims: [8]i64 = undefined;
            for (0..rank) |d| dims[d] = attrs.new_shape.dim(@intCast(d));
            var reshaped_input = if (attrs.runtime_shape)
                null
            else
                ensureDeclaredShape(cb, V.get(ins[0]), graph.node(ins[0]).output_shape);
            defer if (reshaped_input) |v| cb.free(v);
            const input_value = reshaped_input orelse V.get(ins[0]);
            var resolved_dims: [8]i64 = undefined;
            const runtime_dims = blk: {
                if (attrs.runtime_shape) {
                    if (ins.len < 2 or ins[1] == null_node) return error.MissingRuntimeInput;
                    const actual = try cb.tensorShape(input_value, std.heap.page_allocator);
                    defer std.heap.page_allocator.free(actual);
                    const shape_values = try cb.toFloat32(V.get(ins[1]), std.heap.page_allocator);
                    defer std.heap.page_allocator.free(shape_values);
                    break :blk try resolveOnnxRuntimeReshapeDims(shape_values, actual, attrs.allow_zero, &resolved_dims);
                }
                const actual = cb.tensorShape(input_value, std.heap.page_allocator) catch break :blk dims[0..rank];
                defer std.heap.page_allocator.free(actual);
                const input_numel = safeElementCountFromDims(actual) orelse break :blk dims[0..rank];
                if (resolveFlattenedProjectionRestoreDims(graph, state.runtime_shapes, ins[0], attrs.new_shape, input_numel, &resolved_dims)) |resolved| {
                    break :blk resolved;
                }
                break :blk resolveRuntimeReshapeDims(actual, graph.node(ins[0]).output_shape, attrs.new_shape, &resolved_dims) orelse dims[0..rank];
            };
            if (cb.tensorShapeMatches(input_value, runtime_dims) catch null) |matches| {
                if (matches) {
                    if (reshaped_input != null) reshaped_input = null;
                    return input_value;
                }
            }
            const result = cb.primReshape(input_value, runtime_dims) catch |err| {
                std.log.warn("reshape execution failed node_id={d} input_id={d} target_shape={any} declared_shape={any} err={s}", .{
                    node_id,
                    ins[0],
                    runtime_dims,
                    graph.node(ins[0]).output_shape,
                    @errorName(err),
                });
                if (graph.node(ins[0]).num_inputs > 0 and graph.node(ins[0]).inputs[0] != null_node) {
                    std.log.warn("reshape input source_id={d} source_op={s} source_shape={any}", .{
                        graph.node(ins[0]).inputs[0],
                        @tagName(std.meta.activeTag(graph.node(graph.node(ins[0]).inputs[0]).op)),
                        graph.node(graph.node(ins[0]).inputs[0]).output_shape,
                    });
                }
                return err;
            };
            return result;
        },
        .transpose => |attrs| {
            var sbuf: [8]i64 = undefined;
            const in_shape = fillShapeDims(graph, ins[0], &sbuf);
            const r = ensureDeclaredShape(cb, V.get(ins[0]), graph.node(ins[0]).output_shape);
            defer if (r) |v| cb.free(v);
            var perm_buf: [ml.graph.shape.max_rank]u8 = undefined;
            const perm = transpose_utils.effectivePerm(attrs, graph.node(ins[0]).output_shape.rank(), &perm_buf);
            const result = cb.primTranspose(r orelse V.get(ins[0]), perm, in_shape) catch |err| {
                if (err == error.UnsupportedShape) {
                    const input_node = graph.node(ins[0]);
                    std.log.warn("transpose execution failed node_id={d} input_id={d} shape={any} perm={any}", .{
                        node_id,
                        ins[0],
                        in_shape,
                        perm,
                    });
                    std.log.warn("transpose input node op={s} declared_shape={any}", .{
                        @tagName(std.meta.activeTag(input_node.op)),
                        input_node.output_shape,
                    });
                    if (input_node.num_inputs > 0 and input_node.inputs[0] != null_node) {
                        std.log.warn("transpose input source_id={d} source_shape={any}", .{
                            input_node.inputs[0],
                            graph.node(input_node.inputs[0]).output_shape,
                        });
                        const source0 = graph.node(input_node.inputs[0]);
                        std.log.warn("transpose input source op={s} declared_shape={any}", .{
                            @tagName(std.meta.activeTag(source0.op)),
                            source0.output_shape,
                        });
                        if (source0.num_inputs > 0 and source0.inputs[0] != null_node) {
                            std.log.warn("transpose input source0 id={d} op={s} shape={any}", .{
                                source0.inputs[0],
                                @tagName(std.meta.activeTag(graph.node(source0.inputs[0]).op)),
                                graph.node(source0.inputs[0]).output_shape,
                            });
                        }
                    }
                }
                return err;
            };
            return result;
        },
        .broadcast_in_dim => |attrs| {
            var sbuf: [8]i64 = undefined;
            const in_shape = fillShapeDims(graph, ins[0], &sbuf);
            var rank = @as(usize, attrs.target_shape.rank());
            var target_dims: [8]i64 = undefined;
            for (0..rank) |d| target_dims[d] = attrs.target_shape.dim(@intCast(d));

            // ONNX Expand accepts its target as a tensor. The importer embeds
            // any statically materializable values in target_shape and keeps
            // the original shape input as input 1 for dynamic shape graphs.
            if (ins.len > 1 and ins[1] != null_node) {
                const shape_values = try cb.toFloat32(V.get(ins[1]), std.heap.page_allocator);
                defer std.heap.page_allocator.free(shape_values);
                if (shape_values.len > 0 and shape_values.len <= target_dims.len) {
                    rank = shape_values.len;
                    const actual_input_shape = cb.tensorShape(V.get(ins[0]), std.heap.page_allocator) catch null;
                    defer if (actual_input_shape) |actual| std.heap.page_allocator.free(actual);
                    const effective_input_shape = actual_input_shape orelse in_shape;
                    for (0..rank) |d| {
                        const rounded = @round(shape_values[d]);
                        if (!std.math.isFinite(rounded) or @abs(shape_values[d] - rounded) > 1e-3)
                            return error.InvalidTensorShape;
                        var target_dim: i64 = @intFromFloat(rounded);
                        const aligned_input_axis = if (d + effective_input_shape.len >= rank)
                            d + effective_input_shape.len - rank
                        else
                            effective_input_shape.len;
                        const input_dim: i64 = if (aligned_input_axis < effective_input_shape.len)
                            effective_input_shape[aligned_input_axis]
                        else
                            1;
                        if (target_dim <= 0) {
                            const declared = if (d < attrs.target_shape.rank()) attrs.target_shape.dim(@intCast(d)) else -1;
                            target_dim = if (declared > 0) declared else if (input_dim > 0) input_dim else 1;
                        } else if (target_dim == 1 and input_dim > 1) {
                            target_dim = input_dim;
                        } else if (input_dim > 1 and target_dim != input_dim) {
                            return error.ShapeMismatch;
                        }
                        target_dims[d] = target_dim;
                    }
                }
            }
            const reshaped = ensureDeclaredShape(cb, V.get(ins[0]), graph.node(ins[0]).output_shape);
            defer if (reshaped) |r| cb.free(r);
            const result = try cb.primBroadcastInDim(
                reshaped orelse V.get(ins[0]),
                target_dims[0..rank],
                attrs.broadcast_axes[0..attrs.num_axes],
                in_shape,
            );
            return result;
        },
        .dot_general => |attrs| {
            var lbuf: [8]i64 = undefined;
            var rbuf: [8]i64 = undefined;
            const lhs_shape = fillShapeDims(graph, ins[0], &lbuf);
            const rhs_shape = fillShapeDims(graph, ins[1], &rbuf);
            // Reshape inputs to declared shapes for shape-tracking backends.
            const lhs_r = ensureDeclaredShape(cb, V.get(ins[0]), graph.node(ins[0]).output_shape);
            defer if (lhs_r) |r| cb.free(r);
            const rhs_r = ensureDeclaredShape(cb, V.get(ins[1]), graph.node(ins[1]).output_shape);
            defer if (rhs_r) |r| cb.free(r);
            const result = cb.primDotGeneral(
                lhs_r orelse V.get(ins[0]),
                rhs_r orelse V.get(ins[1]),
                lhs_shape,
                rhs_shape,
                attrs.lhs_contracting[0..attrs.num_contracting],
                attrs.rhs_contracting[0..attrs.num_contracting],
                attrs.lhs_batch[0..attrs.num_batch],
                attrs.rhs_batch[0..attrs.num_batch],
            ) catch |err| {
                if (err == error.UnsupportedShape) {
                    const lhs_actual = cb.tensorShape(lhs_r orelse V.get(ins[0]), std.heap.page_allocator) catch null;
                    defer if (lhs_actual) |shape| std.heap.page_allocator.free(shape);
                    const rhs_actual = cb.tensorShape(rhs_r orelse V.get(ins[1]), std.heap.page_allocator) catch null;
                    defer if (rhs_actual) |shape| std.heap.page_allocator.free(shape);
                    std.log.warn("dot_general execution failed node_id={d} lhs_id={d} rhs_id={d} lhs_shape={any} rhs_shape={any} lhs_contracting={any} rhs_contracting={any} lhs_batch={any} rhs_batch={any}", .{
                        node_id,
                        ins[0],
                        ins[1],
                        lhs_shape,
                        rhs_shape,
                        attrs.lhs_contracting[0..attrs.num_contracting],
                        attrs.rhs_contracting[0..attrs.num_contracting],
                        attrs.lhs_batch[0..attrs.num_batch],
                        attrs.rhs_batch[0..attrs.num_batch],
                    });
                    std.log.warn("dot_general lhs_actual={?any} rhs_actual={?any}", .{ lhs_actual, rhs_actual });
                    std.log.warn("dot_general lhs node op={s} declared_shape={any} rhs node op={s} declared_shape={any}", .{
                        @tagName(std.meta.activeTag(graph.node(ins[0]).op)),
                        graph.node(ins[0]).output_shape,
                        @tagName(std.meta.activeTag(graph.node(ins[1]).op)),
                        graph.node(ins[1]).output_shape,
                    });
                    const lhs_inputs = graph.node(ins[0]).getInputs();
                    const rhs_inputs = graph.node(ins[1]).getInputs();
                    if (lhs_inputs.len > 0) {
                        std.log.warn("dot_general lhs input0 id={d} op={s} shape={any}", .{
                            lhs_inputs[0],
                            @tagName(std.meta.activeTag(graph.node(lhs_inputs[0]).op)),
                            graph.node(lhs_inputs[0]).output_shape,
                        });
                        if (values[lhs_inputs[0]]) |lhs0_val| {
                            const lhs0_actual = cb.tensorShape(lhs0_val, std.heap.page_allocator) catch null;
                            defer if (lhs0_actual) |shape| std.heap.page_allocator.free(shape);
                            std.log.warn("dot_general lhs input0 actual={?any}", .{lhs0_actual});
                        }
                        const lhs0_inputs = graph.node(lhs_inputs[0]).getInputs();
                        if (lhs0_inputs.len > 0) {
                            std.log.warn("dot_general lhs input0 source0 id={d} op={s} shape={any}", .{
                                lhs0_inputs[0],
                                @tagName(std.meta.activeTag(graph.node(lhs0_inputs[0]).op)),
                                graph.node(lhs0_inputs[0]).output_shape,
                            });
                        }
                    }
                    if (lhs_inputs.len > 1) {
                        std.log.warn("dot_general lhs input1 id={d} op={s} shape={any}", .{
                            lhs_inputs[1],
                            @tagName(std.meta.activeTag(graph.node(lhs_inputs[1]).op)),
                            graph.node(lhs_inputs[1]).output_shape,
                        });
                        if (values[lhs_inputs[1]]) |lhs1_val| {
                            const lhs1_actual = cb.tensorShape(lhs1_val, std.heap.page_allocator) catch null;
                            defer if (lhs1_actual) |shape| std.heap.page_allocator.free(shape);
                            std.log.warn("dot_general lhs input1 actual={?any}", .{lhs1_actual});
                        }
                    }
                    if (rhs_inputs.len > 0) {
                        std.log.warn("dot_general rhs input0 id={d} op={s} shape={any}", .{
                            rhs_inputs[0],
                            @tagName(std.meta.activeTag(graph.node(rhs_inputs[0]).op)),
                            graph.node(rhs_inputs[0]).output_shape,
                        });
                        if (values[rhs_inputs[0]]) |rhs0_val| {
                            const rhs0_actual = cb.tensorShape(rhs0_val, std.heap.page_allocator) catch null;
                            defer if (rhs0_actual) |shape| std.heap.page_allocator.free(shape);
                            std.log.warn("dot_general rhs input0 actual={?any}", .{rhs0_actual});
                        }
                        const rhs0_inputs = graph.node(rhs_inputs[0]).getInputs();
                        if (rhs0_inputs.len > 0) {
                            std.log.warn("dot_general rhs input0 source0 id={d} op={s} shape={any}", .{
                                rhs0_inputs[0],
                                @tagName(std.meta.activeTag(graph.node(rhs0_inputs[0]).op)),
                                graph.node(rhs0_inputs[0]).output_shape,
                            });
                        }
                    }
                    if (rhs_inputs.len > 1) {
                        std.log.warn("dot_general rhs input1 id={d} op={s} shape={any}", .{
                            rhs_inputs[1],
                            @tagName(std.meta.activeTag(graph.node(rhs_inputs[1]).op)),
                            graph.node(rhs_inputs[1]).output_shape,
                        });
                    }
                }
                return err;
            };
            return result;
        },
        .scatter_add => |attrs| {
            var dest_buf: [8]i64 = undefined;
            var values_buf: [8]i64 = undefined;
            var indices_buf: [8]i64 = undefined;
            var generated_dest: ?CT = null;
            defer if (generated_dest) |ct| cb.free(ct);

            var dest: CT = undefined;
            var scatter_values: CT = undefined;
            var indices: CT = undefined;
            var dest_shape: []const i64 = undefined;
            var values_shape: []const i64 = undefined;
            var indices_shape: []const i64 = undefined;

            if (graph.node(node_id).num_inputs == 2) {
                const out_shape = graph.node(node_id).output_shape;
                const rank = out_shape.rank();
                if (rank > dest_buf.len) return error.UnsupportedShape;
                var dims_i32: [8]i32 = undefined;
                for (0..rank) |axis| {
                    const dim = out_shape.dim(@intCast(axis));
                    if (dim <= 0) return error.UnsupportedShape;
                    dest_buf[axis] = dim;
                    dims_i32[axis] = @intCast(dim);
                }
                scatter_values = V.get(ins[0]);
                indices = V.get(ins[1]);
                dest_shape = dest_buf[0..rank];
                values_shape = runtimeOrDeclaredShape(state, graph, ins[0], &values_buf);
                indices_shape = runtimeOrDeclaredShape(state, graph, ins[1], &indices_buf);

                if (cb.primScatterAdd(scatter_values, indices, values_shape, dest_shape, attrs.axis)) |result| {
                    return result;
                } else |err| switch (err) {
                    error.UnsupportedPrimitiveOp, error.UnsupportedTensorType, error.UnsupportedShape => {},
                    else => return err,
                }

                const elem_count_i64 = out_shape.numElements() orelse return error.UnsupportedShape;
                if (elem_count_i64 < 0) return error.UnsupportedShape;
                const elem_count: usize = @intCast(elem_count_i64);
                const zeros = try std.heap.page_allocator.alloc(f32, elem_count);
                defer std.heap.page_allocator.free(zeros);
                @memset(zeros, 0.0);
                generated_dest = try cb.fromFloat32Shape(zeros, dims_i32[0..rank]);
                dest = generated_dest.?;
            } else {
                dest = V.get(ins[0]);
                scatter_values = V.get(ins[1]);
                indices = V.get(ins[2]);
                dest_shape = runtimeOrDeclaredShape(state, graph, ins[0], &dest_buf);
                values_shape = runtimeOrDeclaredShape(state, graph, ins[1], &values_buf);
                indices_shape = runtimeOrDeclaredShape(state, graph, ins[2], &indices_buf);
            }
            return executeScatterAdd(
                std.heap.page_allocator,
                cb,
                dest,
                scatter_values,
                indices,
                dest_shape,
                values_shape,
                indices_shape,
                attrs.axis,
            );
        },
        .gather => |attrs| {
            var sbuf: [8]i64 = undefined;
            const in_shape = runtimeOrDeclaredShape(state, graph, ins[0], &sbuf);
            var indices_buf: [8]i64 = undefined;
            const indices_shape = runtimeOrDeclaredShape(state, graph, ins[1], &indices_buf);
            if (attrs.elements) {
                const actual_in_shape = cb.tensorShape(V.get(ins[0]), std.heap.page_allocator) catch null;
                defer if (actual_in_shape) |shape| std.heap.page_allocator.free(shape);
                const actual_indices_shape = cb.tensorShape(V.get(ins[1]), std.heap.page_allocator) catch null;
                defer if (actual_indices_shape) |shape| std.heap.page_allocator.free(shape);
                return executeGatherElements(
                    std.heap.page_allocator,
                    cb,
                    V.get(ins[0]),
                    V.get(ins[1]),
                    actual_in_shape orelse in_shape,
                    actual_indices_shape orelse indices_shape,
                    attrs.axis,
                );
            }
            if (attrs.axis == 0) gather_add_bias: {
                const input_node = graph.node(ins[0]);
                if (input_node.op != .add) break :gather_add_bias;
                const add_inputs = input_node.getInputs();
                if (add_inputs.len != 2) break :gather_add_bias;
                const lhs = add_inputs[0];
                const rhs = add_inputs[1];
                var lhs_buf: [8]i64 = undefined;
                var rhs_buf: [8]i64 = undefined;
                const lhs_shape = fillShapeDims(graph, lhs, &lhs_buf);
                const rhs_shape = fillShapeDims(graph, rhs, &rhs_buf);

                var matrix_id: NodeId = null_node;
                var bias_id: NodeId = null_node;
                var matrix_shape_buf: [8]i64 = undefined;
                var matrix_shape: []const i64 = &.{};
                if (lhs_shape.len == 2 and rhs_shape.len == 1 and lhs_shape[1] == rhs_shape[0]) {
                    matrix_id = lhs;
                    bias_id = rhs;
                    @memcpy(matrix_shape_buf[0..lhs_shape.len], lhs_shape);
                    matrix_shape = matrix_shape_buf[0..lhs_shape.len];
                } else if (rhs_shape.len == 2 and lhs_shape.len == 1 and rhs_shape[1] == lhs_shape[0]) {
                    matrix_id = rhs;
                    bias_id = lhs;
                    @memcpy(matrix_shape_buf[0..rhs_shape.len], rhs_shape);
                    matrix_shape = matrix_shape_buf[0..rhs_shape.len];
                } else {
                    break :gather_add_bias;
                }

                const matrix = V.getOpt(matrix_id) orelse break :gather_add_bias;
                const bias = V.getOpt(bias_id) orelse break :gather_add_bias;
                if (try cb.primGatherAddBiasAxis0(matrix, bias, V.get(ins[1]), matrix_shape)) |fused| {
                    return fused;
                }
            }
            const result = try cb.primGather(V.get(ins[0]), V.get(ins[1]), attrs.axis, in_shape);
            return result;
        },
        .slice => |attrs| {
            var sbuf: [8]i64 = undefined;
            const in_shape = fillShapeDims(graph, ins[0], &sbuf);
            const rank = @as(usize, attrs.num_axes);
            var starts: [8]i64 = undefined;
            var limits: [8]i64 = undefined;
            var strides: [8]i64 = undefined;
            try runtime_slice.resolve(graph.allocator, cb, values, ins, attrs, &starts, &limits, &strides);
            const result = cb.primSlice(V.get(ins[0]), starts[0..rank], limits[0..rank], strides[0..rank], in_shape) catch |err| {
                const actual = cb.tensorShape(V.get(ins[0]), std.heap.page_allocator) catch null;
                defer if (actual) |shape| std.heap.page_allocator.free(shape);
                std.log.warn("slice execution failed node_id={d} input_id={d} input_op={s} starts={any} limits={any} strides={any} declared={any} actual={?any} out_shape={any} err={s}", .{
                    node_id,
                    ins[0],
                    @tagName(std.meta.activeTag(graph.node(ins[0]).op)),
                    starts[0..rank],
                    limits[0..rank],
                    strides[0..rank],
                    graph.node(ins[0]).output_shape,
                    actual,
                    n.output_shape,
                    @errorName(err),
                });
                return err;
            };
            return result;
        },
        .shape_of => |attrs| {
            const tmp_alloc = std.heap.page_allocator;
            const actual = try cb.tensorShape(V.get(ins[0]), tmp_alloc);
            defer tmp_alloc.free(actual);

            const start: usize = attrs.start;
            const end: usize = attrs.end;
            if (end < start or end > actual.len) return error.InvalidTensorShape;

            const count = end - start;
            const data = try tmp_alloc.alloc(f32, count);
            defer tmp_alloc.free(data);
            for (0..count) |i| {
                data[i] = @floatFromInt(actual[start + i]);
            }

            const shape = [_]i32{@intCast(count)};
            return cb.fromFloat32Shape(data, &shape);
        },
        .range => {
            const tmp_alloc = std.heap.page_allocator;

            const start_data = try cb.toFloat32(V.get(ins[0]), tmp_alloc);
            defer tmp_alloc.free(start_data);
            const limit_data = try cb.toFloat32(V.get(ins[1]), tmp_alloc);
            defer tmp_alloc.free(limit_data);
            const delta_data = try cb.toFloat32(V.get(ins[2]), tmp_alloc);
            defer tmp_alloc.free(delta_data);

            const start = if (start_data.len > 0) start_data[0] else 0.0;
            const limit = if (limit_data.len > 0) limit_data[0] else 0.0;
            const delta = if (delta_data.len > 0) delta_data[0] else 1.0;
            if (delta == 0.0) return error.InvalidAttribute;

            const raw_count = @ceil((limit - start) / delta);
            const count = if (raw_count <= 0.0) @as(usize, 0) else @as(usize, @intFromFloat(raw_count));
            const data = try tmp_alloc.alloc(f32, count);
            defer tmp_alloc.free(data);
            for (0..count) |i| {
                data[i] = start + @as(f32, @floatFromInt(i)) * delta;
            }

            const shape = [_]i32{@intCast(count)};
            return cb.fromFloat32Shape(data, &shape);
        },
        .concat_prim => |attrs| {
            var abuf: [8]i64 = undefined;
            var bbuf: [8]i64 = undefined;
            const a_shape = fillShapeDims(graph, ins[0], &abuf);
            const b_shape = fillShapeDims(graph, ins[1], &bbuf);
            return cb.primConcatPrim(V.get(ins[0]), V.get(ins[1]), attrs.axis, a_shape, b_shape) catch |err| {
                const a_inputs = graph.node(ins[0]).getInputs();
                const b_inputs = graph.node(ins[1]).getInputs();
                std.log.warn("concat execution failed node={d} axis={d} a_id={d} b_id={d} a_shape={any} b_shape={any} a_op={s} b_op={s}", .{
                    node_id,
                    attrs.axis,
                    ins[0],
                    ins[1],
                    a_shape,
                    b_shape,
                    @tagName(std.meta.activeTag(graph.node(ins[0]).op)),
                    @tagName(std.meta.activeTag(graph.node(ins[1]).op)),
                });
                if (a_inputs.len > 0 or b_inputs.len > 0) {
                    std.log.warn("concat upstream a_input0={?d} a_input0_op={s} b_input0={?d} b_input0_op={s}", .{
                        if (a_inputs.len > 0) a_inputs[0] else null,
                        if (a_inputs.len > 0) @tagName(std.meta.activeTag(graph.node(a_inputs[0]).op)) else "none",
                        if (b_inputs.len > 0) b_inputs[0] else null,
                        if (b_inputs.len > 0) @tagName(std.meta.activeTag(graph.node(b_inputs[0]).op)) else "none",
                    });
                }
                return err;
            };
        },

        .convert_dtype => |attrs| {
            const in_dtype = graph.node(ins[0]).output_shape.dtype;

            if (in_dtype == attrs.target) {
                // Same dtype — no-op, forward the input unchanged.
                return V.get(ins[0]);
            }

            if (try cb.tryConvertDType(V.get(ins[0]), attrs.target)) |converted| {
                return converted;
            }

            // The native interpreter stores all tensors as f32 buffers.
            // When converting to an integer type (i64, i32, u8, bool),
            // round each element so downstream consumers that read the
            // f32 values as integers (via @intFromFloat) get correct
            // results even for non-exact values like 2.9999.
            switch (attrs.target) {
                .i64, .i32, .u8, .bool_ => {
                    const tmp_alloc = std.heap.page_allocator;
                    const data = try cb.toFloat32(V.get(ins[0]), tmp_alloc);
                    defer tmp_alloc.free(data);
                    for (data) |*v| {
                        v.* = @round(v.*);
                    }
                    // Preserve the runtime shape when available. Imported ONNX
                    // often carries symbolic graph dims, while the concrete
                    // tensor has already resolved batch/sequence extents.
                    const actual_shape = cb.tensorShape(V.get(ins[0]), tmp_alloc) catch null;
                    defer if (actual_shape) |shape| tmp_alloc.free(shape);
                    const in_shape = graph.node(ins[0]).output_shape;
                    const rank = if (actual_shape) |shape| shape.len else in_shape.rank();
                    if (rank > 1) {
                        var dims: [8]i32 = undefined;
                        for (0..rank) |d| {
                            const dim = if (actual_shape) |shape| shape[d] else in_shape.dim(@intCast(d));
                            dims[d] = @intCast(dim);
                        }
                        return cb.fromFloat32Shape(data, dims[0..rank]);
                    }
                    return cb.fromFloat32(data);
                },
                // float → float (f32/f16/bf16): the underlying buffer is
                // already f32 in the native backend, so pass through.
                else => return V.get(ins[0]),
            }
        },
        .conv_general => |attrs| {
            const input_shape = graph.node(ins[0]).output_shape;
            const weight_shape = graph.node(ins[1]).output_shape;
            const input_actual = cb.tensorShape(V.get(ins[0]), std.heap.page_allocator) catch null;
            defer if (input_actual) |shape| std.heap.page_allocator.free(shape);

            if (attrs.num_spatial == 1 and attrs.groups == 1 and
                input_shape.rank() == 3 and weight_shape.rank() == 3 and
                attrs.padding[0][0] == attrs.padding[0][1])
            {
                const batch = try positiveResolvedDim(input_actual, input_shape, 0);
                const in_channels = try positiveResolvedDim(input_actual, input_shape, 1);
                const time_steps = try positiveResolvedDim(input_actual, input_shape, 2);
                const out_channels = try positiveShapeDim(weight_shape, 0);
                const kernel_size = try positiveShapeDim(weight_shape, 2);
                const stride = std.math.cast(usize, attrs.strides[0]) orelse return error.UnsupportedShape;
                const padding = std.math.cast(usize, attrs.padding[0][0]) orelse return error.UnsupportedShape;

                const tmp_alloc = std.heap.page_allocator;
                const bias_data = try tmp_alloc.alloc(f32, out_channels);
                defer tmp_alloc.free(bias_data);
                @memset(bias_data, 0.0);
                const bias = try cb.fromFloat32(bias_data);
                defer cb.free(bias);

                return cb.conv1d(
                    V.get(ins[0]),
                    V.get(ins[1]),
                    bias,
                    batch,
                    in_channels,
                    out_channels,
                    time_steps,
                    kernel_size,
                    stride,
                    padding,
                ) catch |err| {
                    if (err == error.UnsupportedShape or err == error.InvalidInputShape) {
                        std.log.warn("conv_general 1d execution failed node_id={d} input_shape={any} weight_shape={any} strides={any} padding={any} groups={d}", .{
                            node_id,
                            input_shape,
                            weight_shape,
                            attrs.strides,
                            attrs.padding,
                            attrs.groups,
                        });
                    }
                    return err;
                };
            }

            if (attrs.num_spatial == 2 and
                input_shape.rank() == 4 and weight_shape.rank() == 4 and
                attrs.padding[0][0] == attrs.padding[0][1] and
                attrs.padding[1][0] == attrs.padding[1][1])
            {
                const batch = try positiveResolvedDim(input_actual, input_shape, 0);
                const in_channels = try positiveResolvedDim(input_actual, input_shape, 1);
                const height = try positiveResolvedDim(input_actual, input_shape, 2);
                const width = try positiveResolvedDim(input_actual, input_shape, 3);
                const out_channels = try positiveShapeDim(weight_shape, 0);
                const kernel_h = try positiveShapeDim(weight_shape, 2);
                const kernel_w = try positiveShapeDim(weight_shape, 3);
                const stride_h = std.math.cast(usize, attrs.strides[0]) orelse return error.UnsupportedShape;
                const stride_w = std.math.cast(usize, attrs.strides[1]) orelse return error.UnsupportedShape;
                const padding_h = std.math.cast(usize, attrs.padding[0][0]) orelse return error.UnsupportedShape;
                const padding_w = std.math.cast(usize, attrs.padding[1][0]) orelse return error.UnsupportedShape;
                const groups = std.math.cast(usize, attrs.groups) orelse return error.UnsupportedShape;

                const tmp_alloc = std.heap.page_allocator;
                const bias_data = try tmp_alloc.alloc(f32, out_channels);
                defer tmp_alloc.free(bias_data);
                @memset(bias_data, 0.0);
                const bias = try cb.fromFloat32(bias_data);
                defer cb.free(bias);

                return cb.conv2d(
                    V.get(ins[0]),
                    V.get(ins[1]),
                    bias,
                    batch,
                    in_channels,
                    out_channels,
                    height,
                    width,
                    kernel_h,
                    kernel_w,
                    stride_h,
                    stride_w,
                    padding_h,
                    padding_w,
                    groups,
                ) catch |err| {
                    if (err == error.UnsupportedShape or err == error.InvalidInputShape) {
                        std.log.warn("conv_general 2d execution failed node_id={d} input_shape={any} weight_shape={any} strides={any} padding={any} groups={d}", .{
                            node_id,
                            input_shape,
                            weight_shape,
                            attrs.strides,
                            attrs.padding,
                            attrs.groups,
                        });
                    }
                    return err;
                };
            }

            return error.UnsupportedPrimitiveOp;
        },
    };
}

fn executeGatherElements(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    input: CT,
    indices: CT,
    input_shape: []const i64,
    indices_shape: []const i64,
    axis: u8,
) !CT {
    const rank = input_shape.len;
    if (rank == 0 or rank > 8 or indices_shape.len != rank or axis >= rank)
        return error.InvalidTensorShape;

    const input_count = safeElementCountFromDims(input_shape) orelse return error.InvalidTensorShape;
    const output_count = safeElementCountFromDims(indices_shape) orelse return error.InvalidTensorShape;
    const input_data = try cb.toFloat32(input, allocator);
    defer allocator.free(input_data);
    const index_data = try cb.toFloat32(indices, allocator);
    defer allocator.free(index_data);
    if (input_data.len != input_count or index_data.len != output_count)
        return error.InvalidTensorShape;

    var input_strides: [8]usize = undefined;
    var output_strides: [8]usize = undefined;
    var stride: usize = 1;
    var rev = rank;
    while (rev > 0) {
        rev -= 1;
        input_strides[rev] = stride;
        stride = std.math.mul(usize, stride, @intCast(input_shape[rev])) catch
            return error.InvalidTensorShape;
    }
    stride = 1;
    rev = rank;
    while (rev > 0) {
        rev -= 1;
        output_strides[rev] = stride;
        stride = std.math.mul(usize, stride, @intCast(indices_shape[rev])) catch
            return error.InvalidTensorShape;
    }

    const output = try allocator.alloc(f32, output_count);
    defer allocator.free(output);
    for (0..output_count) |flat_output| {
        var remaining = flat_output;
        var input_offset: usize = 0;
        for (0..rank) |d| {
            const coord = remaining / output_strides[d];
            remaining %= output_strides[d];
            if (d == axis) {
                const raw_index = index_data[flat_output];
                const rounded = @round(raw_index);
                if (!std.math.isFinite(rounded) or @abs(raw_index - rounded) > 1e-3)
                    return error.InvalidTensorIndex;
                var gather_index: i64 = @intFromFloat(rounded);
                if (gather_index < 0) gather_index += input_shape[d];
                if (gather_index < 0 or gather_index >= input_shape[d])
                    return error.IndexOutOfBounds;
                input_offset += @as(usize, @intCast(gather_index)) * input_strides[d];
            } else {
                if (coord >= input_shape[d]) return error.InvalidTensorShape;
                input_offset += coord * input_strides[d];
            }
        }
        output[flat_output] = input_data[input_offset];
    }

    var dims: [8]i32 = undefined;
    for (indices_shape, 0..) |dim, d| {
        dims[d] = std.math.cast(i32, dim) orelse return error.InvalidTensorShape;
    }
    return cb.fromFloat32Shape(output, dims[0..rank]);
}

// ── Tests ──────────────────────────────────────────────────────────────

test "computeReachable skips vjp_alternate subgraph" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    // Build: parameter -> linear (which emits decomposed + fused)
    const x = try b.parameter("input", Shape.init(.f32, &.{ 2, 4 }));
    const w = try b.parameter("weight", Shape.init(.f32, &.{ 3, 4 }));
    const bias = try b.parameter("bias", Shape.init(.f32, &.{3}));
    const result = try b.linear(x, w, bias, 2, 4, 3);
    try g.markOutput(result);

    const reachable = try computeReachable(allocator, &g);
    defer allocator.free(reachable);

    // The fused node and its direct inputs (params) should be reachable
    try std.testing.expect(reachable[result]);
    try std.testing.expect(reachable[x]);
    try std.testing.expect(reachable[w]);
    try std.testing.expect(reachable[bias]);

    // The decomposed subgraph nodes (transpose, matmul, add) should NOT
    // be reachable since they're only referenced via vjp_alternate
    const fused_node = g.node(result);
    try std.testing.expect(fused_node.vjp_alternate != null_node);

    // Count reachable nodes: should be 4 (3 params + 1 fused), not 7+
    var reachable_count: usize = 0;
    for (reachable) |r| {
        if (r) reachable_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 4), reachable_count);
}

test "nullCtAliases clears every slot for a released handle" {
    const first: CT = @ptrFromInt(0x1000);
    const second: CT = @ptrFromInt(0x2000);
    var values = [_]?CT{ first, second, first, null };

    nullCtAliases(&values, first);

    try std.testing.expect(values[0] == null);
    try std.testing.expect(values[1] == second);
    try std.testing.expect(values[2] == null);
    try std.testing.expect(values[3] == null);
}

test "computeLastUse tracks dependencies" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 4, 4 }));
    const bias = try b.parameter("b", Shape.init(.f32, &.{4}));

    // x used by both linear and elemAdd
    const y = try b.linear(x, w, bias, 2, 4, 4);
    const out = try b.elemAdd(x, y);
    try g.markOutput(out);

    const reachable = try computeReachable(allocator, &g);
    defer allocator.free(reachable);

    const last_use = try computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    // x should have last_use = elemAdd's fused node (not the linear)
    // because elemAdd also uses x as input
    try std.testing.expect(last_use[x] > y);

    // output node should never be freed (sentinel value)
    try std.testing.expectEqual(std.math.maxInt(u32), last_use[out]);
}

test "donation last use includes future sibling reshape aliases" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const early_view = try b.reshape(x, Shape.init(.f32, &.{ 2, 2 }));
    const two = try b.scalarConst(.f32, 2.0);
    const scaled = try b.mul(early_view, two);
    const later_view = try b.reshape(x, Shape.init(.f32, &.{ 2, 2 }));
    const later_sum = try b.reduceSum(later_view, &.{ 0, 1 });
    try g.markOutput(scaled);
    try g.markOutput(later_sum);

    const reachable = try computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);
    const donation_last_use = try computeDonationLastUse(allocator, &g, reachable, last_use);
    defer allocator.free(donation_last_use);

    try std.testing.expectEqual(scaled, last_use[early_view]);
    try std.testing.expectEqual(later_sum, donation_last_use[early_view]);
    try std.testing.expect(donation_last_use[early_view] > scaled);
}

test "computeReachable with chained ops" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 8 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{8}));

    // Chain: x -> rmsNorm -> gelu -> silu
    const normed = try b.rmsNorm(x, w, 8, 1e-5);
    const activated = try b.gelu(normed);
    const out = try b.silu(activated);
    try g.markOutput(out);

    const reachable = try computeReachable(allocator, &g);
    defer allocator.free(reachable);

    // The 3 fused ops + 2 params = 5 reachable
    // All the decomposed primitive nodes should be unreachable
    var reachable_count: usize = 0;
    for (reachable) |r| {
        if (r) reachable_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 5), reachable_count);
    try std.testing.expect(reachable[out]);
    try std.testing.expect(reachable[activated]);
    try std.testing.expect(reachable[normed]);
    try std.testing.expect(reachable[x]);
    try std.testing.expect(reachable[w]);
}

// ── TestCompute: minimal backend for round-trip testing ──────────────

const TestBuf = struct {
    data: []f32,
    allocator: std.mem.Allocator,
    owned: bool,
};

fn testToBuf(ct: CT) *TestBuf {
    return @ptrCast(@alignCast(ct));
}

fn testGetData(ct: CT) []f32 {
    return testToBuf(ct).data;
}

const TestCompute = struct {
    allocator: std.mem.Allocator,
    weights: std.StringHashMapUnmanaged([]f32),

    /// Attention layer indices received via gqaPagedAttention dispatch.
    /// Used to verify the interpreter auto-increments layer_index.
    received_layer_indices: [8]usize = .{0} ** 8,
    num_attn_calls: usize = 0,

    /// Embedding IDs received via embeddingLookup dispatch.
    received_embedding_ids: ?[]const i64 = null,
    received_embedding_ids_owned: ?[]i64 = null,

    fn init(allocator: std.mem.Allocator) TestCompute {
        return .{ .allocator = allocator, .weights = .empty };
    }

    fn deinit(self: *TestCompute) void {
        if (self.received_embedding_ids_owned) |ids| self.allocator.free(ids);
        self.weights.deinit(self.allocator);
    }

    fn addWeight(self: *TestCompute, name: []const u8, data: []const f32) !void {
        const owned = try self.allocator.dupe(f32, data);
        try self.weights.put(self.allocator, name, owned);
    }

    fn freeWeights(self: *TestCompute) void {
        var it = self.weights.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.value_ptr.*);
        }
    }

    fn makeBuf(self: *TestCompute, data: []f32, owned: bool) !CT {
        const b = try self.allocator.create(TestBuf);
        b.* = .{ .data = data, .allocator = self.allocator, .owned = owned };
        return @ptrCast(b);
    }

    fn backend(self: *TestCompute) ComputeBackend {
        return .{ .ptr = @ptrCast(self), .vtable = &test_vtable };
    }

    fn fromCtx(ctx: *anyopaque) *TestCompute {
        return @ptrCast(@alignCast(ctx));
    }

    // ── VTable implementations ───────────────────────────────────

    fn backendKind(_: *anyopaque) contracts.BackendKind {
        return .native;
    }
    fn deinitBackend(_: *anyopaque) void {}
    fn prefetchHint(_: *anyopaque, _: []const u8, _: u32) void {}
    fn drainPrefetch(_: *anyopaque, _: usize) void {}

    fn freeTensor(_: *anyopaque, tensor: CT) void {
        const b = testToBuf(tensor);
        if (b.owned) b.allocator.free(b.data);
        b.allocator.destroy(b);
    }

    fn getWeight(ctx: *anyopaque, name: []const u8) anyerror!CT {
        const self = fromCtx(ctx);
        const data = self.weights.get(name) orelse return error.MissingWeight;
        return self.makeBuf(data, false); // borrowed
    }

    fn fromFloat32Op(ctx: *anyopaque, data: []const f32) anyerror!CT {
        const self = fromCtx(ctx);
        const owned = try self.allocator.dupe(f32, data);
        return self.makeBuf(owned, true);
    }

    fn fromFloat32ShapeOp(ctx: *anyopaque, data: []const f32, _: []const i32) anyerror!CT {
        return fromFloat32Op(ctx, data);
    }

    fn toFloat32Op(_: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror![]f32 {
        return allocator.dupe(f32, testGetData(tensor));
    }

    fn linearOp(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT {
        const self = fromCtx(ctx);
        const x = testGetData(input);
        const w = testGetData(weight);
        const b = testGetData(bias);
        const out = try self.allocator.alloc(f32, rows * out_dim);
        // Y = X @ W^T + B  (W is [out_dim, in_dim])
        for (0..rows) |r| {
            for (0..out_dim) |o| {
                var sum: f32 = b[o];
                for (0..in_dim) |i| {
                    sum += x[r * in_dim + i] * w[o * in_dim + i];
                }
                out[r * out_dim + o] = sum;
            }
        }
        return self.makeBuf(out, true);
    }

    fn linearNoBiasOp(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT {
        const self = fromCtx(ctx);
        const x = testGetData(input);
        const w = testGetData(weight);
        const out = try self.allocator.alloc(f32, rows * out_dim);
        for (0..rows) |r| {
            for (0..out_dim) |o| {
                var sum: f32 = 0;
                for (0..in_dim) |i| {
                    sum += x[r * in_dim + i] * w[o * in_dim + i];
                }
                out[r * out_dim + o] = sum;
            }
        }
        return self.makeBuf(out, true);
    }

    fn rmsNormOp(ctx: *anyopaque, input: CT, weight: CT, dim: usize, eps: f32) anyerror!CT {
        const self = fromCtx(ctx);
        const x = testGetData(input);
        const w = testGetData(weight);
        const batch = x.len / dim;
        const out = try self.allocator.dupe(f32, x);
        for (0..batch) |b| {
            const row = out[b * dim .. (b + 1) * dim];
            var sum_sq: f32 = 0;
            for (row) |v| sum_sq += v * v;
            const rms = @sqrt(sum_sq / @as(f32, @floatFromInt(dim)) + eps);
            const inv_rms = 1.0 / rms;
            for (row, 0..) |*v, i| v.* = v.* * inv_rms * w[i];
        }
        return self.makeBuf(out, true);
    }

    fn geluOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const self = fromCtx(ctx);
        const x = testGetData(input);
        const out = try self.allocator.dupe(f32, x);
        const sqrt_2_over_pi: f32 = 0.7978845608028654;
        for (out) |*v| {
            const val = v.*;
            const inner = sqrt_2_over_pi * (val + 0.044715 * val * val * val);
            v.* = 0.5 * val * (1.0 + std.math.tanh(inner));
        }
        return self.makeBuf(out, true);
    }

    fn binaryBroadcastOp(
        allocator: std.mem.Allocator,
        a_data: []const f32,
        b_data: []const f32,
        comptime op: enum { add, mul },
    ) ![]f32 {
        const big = if (a_data.len >= b_data.len) a_data else b_data;
        const small = if (a_data.len >= b_data.len) b_data else a_data;
        const a_is_big = a_data.len >= b_data.len;
        const out = try allocator.alloc(f32, big.len);

        for (0..big.len) |i| {
            const bi = i % small.len;
            const a_val = if (a_is_big) big[i] else small[bi];
            const b_val = if (a_is_big) small[bi] else big[i];
            out[i] = switch (op) {
                .add => a_val + b_val,
                .mul => a_val * b_val,
            };
        }
        return out;
    }

    fn addOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
        const self = fromCtx(ctx);
        const a_data = testGetData(a);
        const b_data = testGetData(b);
        const out = try binaryBroadcastOp(self.allocator, a_data, b_data, .add);
        return self.makeBuf(out, true);
    }

    fn multiplyOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
        const self = fromCtx(ctx);
        const a_data = testGetData(a);
        const b_data = testGetData(b);
        const out = try binaryBroadcastOp(self.allocator, a_data, b_data, .mul);
        return self.makeBuf(out, true);
    }

    fn stubUnary(ctx: *anyopaque, input: CT) anyerror!CT {
        // Pass-through stub for activations we don't test
        const self = fromCtx(ctx);
        return self.makeBuf(try self.allocator.dupe(f32, testGetData(input)), true);
    }

    fn stubBinary(ctx: *anyopaque, _: CT, _: CT, _: usize, _: usize, _: usize) anyerror!CT {
        _ = ctx;
        return error.UnsupportedPrimitiveOp;
    }

    fn stubLayerNorm(ctx: *anyopaque, input: CT, _: CT, _: CT, _: usize, _: f32) anyerror!CT {
        // Pass-through stub
        const self = fromCtx(ctx);
        return self.makeBuf(try self.allocator.dupe(f32, testGetData(input)), true);
    }

    fn stubSdpa(_: *anyopaque, _: CT, _: CT, _: CT, _: []const i64, _: ?CT, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
        return error.UnsupportedPrimitiveOp;
    }
    fn stubCausalAttn(_: *anyopaque, _: CT, _: CT, _: CT, _: ?CT, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
        return error.UnsupportedPrimitiveOp;
    }
    fn stubCrossAttn(_: *anyopaque, _: CT, _: CT, _: CT, _: []const i64, _: usize, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
        return error.UnsupportedPrimitiveOp;
    }
    fn stubRelPosBias(_: *anyopaque, _: CT, _: usize, _: usize, _: usize, _: usize, _: usize, _: bool) anyerror!CT {
        return error.UnsupportedPrimitiveOp;
    }
    fn stubDeberta(_: *anyopaque, _: CT, _: CT, _: CT, _: CT, _: CT, _: []const i64, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
        return error.UnsupportedPrimitiveOp;
    }
    fn stubDebertaBackward(_: *anyopaque, _: CT, _: CT, _: CT, _: CT, _: CT, _: []const i64, _: CT, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
        return error.UnsupportedPrimitiveOp;
    }
    fn stubWindowedAttn(_: *anyopaque, _: CT, _: CT, _: CT, _: CT, _: CT, _: CT, _: CT, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
        return error.UnsupportedPrimitiveOp;
    }
    fn stubChannelAttn(_: *anyopaque, _: CT, _: CT, _: CT, _: CT, _: CT, _: CT, _: CT, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
        return error.UnsupportedPrimitiveOp;
    }
    fn stubTokenConv(_: *anyopaque, _: CT, _: CT, _: CT, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
        return error.UnsupportedPrimitiveOp;
    }
    fn stubConv1d(_: *anyopaque, _: CT, _: CT, _: CT, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
        return error.UnsupportedPrimitiveOp;
    }
    fn stubConv2d(_: *anyopaque, _: CT, _: CT, _: CT, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
        return error.UnsupportedPrimitiveOp;
    }
    fn stubRope(_: *anyopaque, _: CT, _: usize, _: usize, _: usize, _: f32, _: f32, _: usize, _: bool) anyerror!CT {
        return error.UnsupportedPrimitiveOp;
    }
    fn stubRopePerItem(_: *anyopaque, _: CT, _: usize, _: usize, _: usize, _: usize, _: f32, _: f32, _: []const usize, _: []const usize, _: bool) anyerror!CT {
        return error.UnsupportedPrimitiveOp;
    }
    fn embeddingLookupOp(ctx: *anyopaque, weight: CT, ids: []const i64, total: usize, dim: usize) anyerror!CT {
        const self = fromCtx(ctx);
        if (self.received_embedding_ids_owned) |old| self.allocator.free(old);
        const copied_ids = try self.allocator.dupe(i64, ids);
        self.received_embedding_ids_owned = copied_ids;
        self.received_embedding_ids = copied_ids;
        const w = testGetData(weight);
        const out = try self.allocator.alloc(f32, total * dim);
        for (0..total) |i| {
            const row: usize = @intCast(ids[i]);
            @memcpy(out[i * dim .. (i + 1) * dim], w[row * dim .. (row + 1) * dim]);
        }
        return self.makeBuf(out, true);
    }

    fn gqaCausalAttnOp(ctx: *anyopaque, Q: CT, _: CT, _: CT, _: ?CT, _: usize, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
        // Simplified: return copy of Q (tests dispatch, not SDPA math)
        const self = fromCtx(ctx);
        return self.makeBuf(try self.allocator.dupe(f32, testGetData(Q)), true);
    }

    fn moeSelectRoutesOp(_: *anyopaque, _: usize, logits: CT, rows: usize, num_experts: usize, top_k: usize, logit_scale: f32, allocator: std.mem.Allocator) anyerror!?ops_mod.MoeRouteSelection {
        _ = logit_scale;
        // Simple routing: assign rows round-robin across experts
        const total = rows * top_k;
        const expert_ids = try allocator.alloc(u32, total);
        const route_weights = try allocator.alloc(f32, total);
        for (0..rows) |r| {
            for (0..top_k) |k| {
                expert_ids[r * top_k + k] = @intCast((r + k) % num_experts);
                route_weights[r * top_k + k] = 1.0 / @as(f32, @floatFromInt(top_k));
            }
        }
        // Pass through the logits unchanged — routing is metadata only
        _ = logits;
        return ops_mod.MoeRouteSelection{
            .expert_ids = expert_ids,
            .route_weights = route_weights,
            .rows = rows,
            .top_k = top_k,
        };
    }

    fn moeLinearNoBiasOp(ctx: *anyopaque, request: *const ops_mod.MoeLinearNoBiasRequest) anyerror!?CT {
        // Simplified MoE linear: Y[i] = X[i] @ W[expert_ids[i]]^T
        // Weight tensor is [num_experts * out_dim, in_dim]
        const self = fromCtx(ctx);
        const x = testGetData(request.input);
        const w = testGetData(request.weight);
        const rows = request.rows;
        const in_dim = request.in_dim;
        const out_dim = request.out_dim;
        const out = try self.allocator.alloc(f32, rows * out_dim);
        for (0..rows) |r| {
            const eid: usize = request.expert_ids[r];
            for (0..out_dim) |o| {
                var sum: f32 = 0;
                for (0..in_dim) |i| {
                    sum += x[r * in_dim + i] * w[(eid * out_dim + o) * in_dim + i];
                }
                out[r * out_dim + o] = sum;
            }
        }
        return self.makeBuf(out, true);
    }

    fn moeScatterAddOp(ctx: *anyopaque, request: *const ops_mod.MoeScatterAddRequest) anyerror!?CT {
        // Scatter-add: base[row_ids[i]] += updates[i] * row_weights[i]
        const self = fromCtx(ctx);
        const base_data = testGetData(request.base);
        const updates = testGetData(request.updates);
        const out = try self.allocator.dupe(f32, base_data);
        for (0..request.rows) |i| {
            const row: usize = request.row_ids[i];
            const w: f32 = request.row_weights[i];
            for (0..request.dim) |d| {
                out[row * request.dim + d] += updates[i * request.dim + d] * w;
            }
        }
        return self.makeBuf(out, true);
    }

    fn takeRowsOp(ctx: *anyopaque, request: *const ops_mod.TakeRowsRequest) anyerror!?CT {
        // Gather: out[i] = input[row_ids[i]]
        const self = fromCtx(ctx);
        const data = testGetData(request.input);
        const out = try self.allocator.alloc(f32, request.rows * request.dim);
        for (0..request.rows) |i| {
            const row: usize = request.row_ids[i];
            @memcpy(out[i * request.dim .. (i + 1) * request.dim], data[row * request.dim .. (row + 1) * request.dim]);
        }
        return self.makeBuf(out, true);
    }

    fn zeroTensorOp(ctx: *anyopaque, rows: usize, dim: usize) anyerror!?CT {
        const self = fromCtx(ctx);
        const out = try self.allocator.alloc(f32, rows * dim);
        @memset(out, 0.0);
        return self.makeBuf(out, true);
    }

    fn gqaPagedAttnOp(ctx: *anyopaque, Q: CT, _: CT, _: CT, _: ?CT, attention: contracts.AttentionContext, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
        const self = fromCtx(ctx);
        // Record layer_index for test assertions
        if (self.num_attn_calls < self.received_layer_indices.len) {
            self.received_layer_indices[self.num_attn_calls] = attention.layer_index;
        }
        self.num_attn_calls += 1;
        // Simplified: return copy of Q
        return self.makeBuf(try self.allocator.dupe(f32, testGetData(Q)), true);
    }

    fn causalSelfAttnOp(ctx: *anyopaque, Q: CT, _: CT, _: CT, _: ?CT, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
        const self = fromCtx(ctx);
        return self.makeBuf(try self.allocator.dupe(f32, testGetData(Q)), true);
    }

    const test_vtable = ComputeBackend.VTable{
        .backendKind = &backendKind,
        .deinitBackend = &deinitBackend,
        .freeTensor = &freeTensor,
        .getWeight = &getWeight,
        .prefetchWeightHint = &prefetchHint,
        .drainPrefetchBudget = &drainPrefetch,
        .embeddingLookup = &embeddingLookupOp,
        .linear = &linearOp,
        .linearNoBias = &linearNoBiasOp,
        .layerNorm = &stubLayerNorm,
        .rmsNorm = &rmsNormOp,
        .gelu = &geluOp,
        .relu = &stubUnary,
        .silu = &stubUnary,
        .quickGelu = &stubUnary,
        .sigmoid = &stubUnary,
        .tanh_act = &stubUnary,
        .concat = &stubBinary,
        .add = &addOp,
        .scaledDotProductAttention = &stubSdpa,
        .causalSelfAttention = &causalSelfAttnOp,
        .crossAttention = &stubCrossAttn,
        .relativePositionBias = &stubRelPosBias,
        .disentangledRelativeAttention = &stubDeberta,
        .disentangledRelativeAttentionBackward = &stubDebertaBackward,
        .windowedSelfAttention = &stubWindowedAttn,
        .channelSelfAttention = &stubChannelAttn,
        .tokenGridConv2d = &stubTokenConv,
        .multiply = &multiplyOp,
        .conv1d = &stubConv1d,
        .conv2d = &stubConv2d,
        .rope = &stubRope,
        .ropePerItem = &stubRopePerItem,
        .gqaCausalAttention = &gqaCausalAttnOp,
        .gqaPagedAttention = &gqaPagedAttnOp,
        .fromFloat32 = &fromFloat32Op,
        .fromFloat32Shape = &fromFloat32ShapeOp,
        .toFloat32 = &toFloat32Op,
        .moeSelectRoutes = &moeSelectRoutesOp,
        .moeLinearNoBias = &moeLinearNoBiasOp,
        .moeScatterAdd = &moeScatterAddOp,
        .takeRows = &takeRowsOp,
        .zeroTensor = &zeroTensorOp,
    };
};

// ── Round-trip integration tests ─────────────────────────────────────

const tracing_compute = @import("tracing_compute.zig");
const TracingCompute = tracing_compute.TracingCompute;

test "round-trip: trace → interpret produces bit-exact results" {
    const allocator = std.testing.allocator;

    // Deterministic test data
    // x: [2, 4] input
    const x_data = [_]f32{ 0.1, -0.2, 0.3, 0.4, -0.5, 0.6, -0.7, 0.8 };
    // w: [4, 4] weight matrix (out_dim=4, in_dim=4)
    const w_data = [_]f32{
        0.1,  0.2,  -0.1, 0.3,
        -0.2, 0.1,  0.4,  -0.1,
        0.3,  -0.3, 0.2,  0.1,
        0.1,  0.1,  -0.2, 0.2,
    };
    // b: [4] bias
    const b_data = [_]f32{ 0.01, -0.02, 0.03, 0.04 };
    // norm_w: [4] rms norm weight
    const norm_w_data = [_]f32{ 1.0, 1.0, 1.0, 1.0 };

    // ── Eager path: direct backend calls ─────────────────────────
    var tc_backend = TestCompute.init(allocator);
    defer tc_backend.deinit();
    try tc_backend.addWeight("w", &w_data);
    try tc_backend.addWeight("b", &b_data);
    try tc_backend.addWeight("norm_w", &norm_w_data);
    defer tc_backend.freeWeights();

    var cb = tc_backend.backend();

    const eager_w = try cb.getWeight("w");
    const eager_b = try cb.getWeight("b");
    const eager_nw = try cb.getWeight("norm_w");
    const eager_x = try cb.fromFloat32(&x_data);

    const eager_lin = try cb.linear(eager_x, eager_w, eager_b, 2, 4, 4);
    const eager_norm = try cb.rmsNorm(eager_lin, eager_nw, 4, 1e-5);
    const eager_act = try cb.gelu(eager_norm);
    const eager_res = try cb.add(eager_act, eager_x);

    const expected = try cb.toFloat32(eager_res, allocator);
    defer allocator.free(expected);

    // Free eager intermediates
    cb.free(eager_res);
    cb.free(eager_act);
    cb.free(eager_norm);
    cb.free(eager_lin);
    cb.free(eager_x);
    cb.free(eager_nw);
    cb.free(eager_b);
    cb.free(eager_w);

    // ── Traced path: TracingCompute → Graph ──────────────────────
    var tracer = try TracingCompute.initWithWeights(allocator, &.{
        .{ .name = "w", .shape = Shape.init(.f32, &.{ 4, 4 }) },
        .{ .name = "b", .shape = Shape.init(.f32, &.{4}) },
        .{ .name = "norm_w", .shape = Shape.init(.f32, &.{4}) },
    });
    defer tracer.deinit();

    var cb_trace = tracer.backend();
    const tr_w = try cb_trace.getWeight("w");
    const tr_b = try cb_trace.getWeight("b");
    const tr_nw = try cb_trace.getWeight("norm_w");
    const tr_x = try cb_trace.fromFloat32(&x_data);

    const tr_lin = try cb_trace.linear(tr_x, tr_w, tr_b, 2, 4, 4);
    const tr_norm = try cb_trace.rmsNorm(tr_lin, tr_nw, 4, 1e-5);
    const tr_act = try cb_trace.gelu(tr_norm);
    const tr_res = try cb_trace.add(tr_act, tr_x);

    const tr_out = try cb_trace.toFloat32(tr_res, allocator);
    defer allocator.free(tr_out);

    const graph = tracer.getGraph();

    // ── Interpret: execute graph through test backend ─────────────
    var result = try execute(allocator, graph, &cb, .{});
    defer result.deinit(&cb);

    const actual = try cb.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);

    // ── Assert bit-exact match ───────────────────────────────────
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |e, a| {
        try std.testing.expectEqual(e, a);
    }
}

test "primitive elementwise ops broadcast scalar constants in interpreter" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ 2, 2 }));
    const one = try builder.scalarConst(.f32, 1.0);
    const half = try builder.scalarConst(.f32, 0.5);
    const shifted = try builder.add(x, one);
    const out = try builder.mul(shifted, half);
    try g.markOutput(out);

    var tc_backend = TestCompute.init(allocator);
    defer tc_backend.deinit();
    defer tc_backend.freeWeights();
    var cb = tc_backend.backend();

    const x_ct = try cb.fromFloat32(&.{ 1.0, 2.0, 3.0, 4.0 });
    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = x, .value = x_ct },
    };

    var result = try execute(allocator, &g, &cb, .{
        .runtime_inputs = &rt_inputs,
    });
    defer result.deinit(&cb);

    const actual = try cb.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    cb.free(x_ct);

    const expected = [_]f32{ 1.0, 1.5, 2.0, 2.5 };
    try std.testing.expectEqual(@as(usize, expected.len), actual.len);
    for (expected, actual) |e, a| {
        try std.testing.expectApproxEqAbs(e, a, 1e-6);
    }
}

test "interpreter fused gelu backward fallback matches tanh derivative" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const upstream = try builder.parameter("upstream", Shape.init(.f32, &.{ 2, 4 }));
    const out = try g.addNode(.{
        .op = .{ .fused_gelu_backward = {} },
        .output_shape = Shape.init(.f32, &.{ 2, 4 }),
        .inputs = .{ x, upstream, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(out);

    var tc_backend = TestCompute.init(allocator);
    defer tc_backend.deinit();
    defer tc_backend.freeWeights();
    var cb = tc_backend.backend();

    const x_data = [_]f32{ -4.0, -1.0, -0.25, 0.0, 0.25, 1.0, 4.0, 12.0 };
    const upstream_data = [_]f32{ 0.5, 1.25, -2.0, 3.0, -0.75, 0.8, 1.1, -1.2 };
    const x_ct = try cb.fromFloat32(&x_data);
    const upstream_ct = try cb.fromFloat32(&upstream_data);
    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = x, .value = x_ct },
        .{ .node_id = upstream, .value = upstream_ct },
    };

    var result = try execute(allocator, &g, &cb, .{
        .runtime_inputs = &rt_inputs,
    });
    defer result.deinit(&cb);

    const actual = try cb.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    cb.free(x_ct);
    cb.free(upstream_ct);

    const sqrt_2_over_pi: f32 = 0.7978845608028654;
    try std.testing.expectEqual(@as(usize, x_data.len), actual.len);
    for (x_data, upstream_data, actual) |xv, up, got| {
        const x2 = xv * xv;
        const inner = sqrt_2_over_pi * (xv + 0.044715 * xv * x2);
        const expected = if (inner > 10.0)
            up
        else if (inner < -10.0)
            0.0
        else blk: {
            const t = std.math.tanh(inner);
            const sech2 = 1.0 - t * t;
            const derivative = 0.5 * (1.0 + t) + 0.5 * xv * sech2 * sqrt_2_over_pi * (1.0 + 0.134145 * x2);
            break :blk up * derivative;
        };
        try std.testing.expectApproxEqAbs(expected, got, 1e-6);
    }
}

test "scatter_add interpreter uses dest values and explicit indices input" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const dest = try builder.parameter("dest", Shape.init(.f32, &.{ 3, 2 }));
    const values = try builder.parameter("values", Shape.init(.f32, &.{ 3, 2 }));
    const indices = try builder.parameter("indices", Shape.init(.i64, &.{3}));
    const out = try builder.scatterAdd(dest, values, indices, 0);
    try g.markOutput(out);

    var tc_backend = TestCompute.init(allocator);
    defer tc_backend.deinit();
    defer tc_backend.freeWeights();
    var cb = tc_backend.backend();

    const dest_ct = try cb.fromFloat32Shape(&.{ 10, 20, 30, 40, 50, 60 }, &.{ 3, 2 });
    const values_ct = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6 }, &.{ 3, 2 });
    const indices_ct = try cb.fromFloat32Shape(&.{ 0, 1, 0 }, &.{3});
    defer cb.free(dest_ct);
    defer cb.free(values_ct);
    defer cb.free(indices_ct);

    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = dest, .value = dest_ct },
        .{ .node_id = values, .value = values_ct },
        .{ .node_id = indices, .value = indices_ct },
    };

    var result = try execute(allocator, &g, &cb, .{ .runtime_inputs = &rt_inputs });
    defer result.deinit(&cb);

    const actual = try cb.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);

    try std.testing.expectEqualSlices(f32, &.{ 16, 28, 33, 44, 50, 60 }, actual);
}

test "scatter_add interpreter supports autodiff two-input gather gradient form" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const values = try builder.parameter("values", Shape.init(.f32, &.{ 3, 2 }));
    const indices = try builder.parameter("indices", Shape.init(.i64, &.{3}));
    const out = try g.addNode(.{
        .op = .{ .scatter_add = .{ .axis = 0 } },
        .output_shape = Shape.init(.f32, &.{ 3, 2 }),
        .inputs = .{ values, indices, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(out);

    var tc_backend = TestCompute.init(allocator);
    defer tc_backend.deinit();
    defer tc_backend.freeWeights();
    var cb = tc_backend.backend();

    const values_ct = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6 }, &.{ 3, 2 });
    const indices_ct = try cb.fromFloat32Shape(&.{ 0, 1, 0 }, &.{3});
    defer cb.free(values_ct);
    defer cb.free(indices_ct);

    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = values, .value = values_ct },
        .{ .node_id = indices, .value = indices_ct },
    };

    var result = try execute(allocator, &g, &cb, .{ .runtime_inputs = &rt_inputs });
    defer result.deinit(&cb);

    const actual = try cb.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);

    try std.testing.expectEqualSlices(f32, &.{ 6, 8, 3, 4, 0, 0 }, actual);
}

test "shouldReshapeToDeclaredShape does not collapse higher-rank tensors" {
    try std.testing.expect(!shouldReshapeToDeclaredShape(
        &.{ 1, 8, 76, 76, 64 },
        Shape.init(.f32, &.{ 1, 8, 5776, 64 }),
    ));
}

test "shouldReshapeToDeclaredShape still expands lower-rank tensors when counts match" {
    try std.testing.expect(shouldReshapeToDeclaredShape(
        &.{ 608, 76, 64 },
        Shape.init(.f32, &.{ 76, 8, 76, 64 }),
    ));
}

test "shouldReshapeToDeclaredShape preserves concrete runtime batch" {
    try std.testing.expect(!shouldReshapeToDeclaredShape(
        &.{ 2, 5776, 64 },
        Shape.init(.f32, &.{ 1, 11552, 64 }),
    ));
}

test "runtime shape tensors preserve distinct ONNX reshape layouts" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const x = try builder.parameter("x", Shape.init(.f32, &.{16}));
    const row_target = try builder.parameter("row_target", Shape.init(.i64, &.{3}));
    const column_target = try builder.parameter("column_target", Shape.init(.i64, &.{3}));
    const attrs = ml.graph.node.ReshapeAttrs{
        .new_shape = Shape.init(.f32, &.{ 1, -1, -1 }),
        .runtime_shape = true,
    };
    const row = try g.addNode(.{
        .op = .{ .reshape = attrs },
        .output_shape = attrs.new_shape,
        .inputs = .{ x, row_target, null_node, null_node },
        .num_inputs = 2,
    });
    const column = try g.addNode(.{
        .op = .{ .reshape = attrs },
        .output_shape = attrs.new_shape,
        .inputs = .{ x, column_target, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(row);
    try g.markOutput(column);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const x_ct = try cb_val.fromFloat32Shape(&.{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 }, &.{16});
    defer cb_val.free(x_ct);
    const row_target_ct = try cb_val.fromFloat32Shape(&.{ 1, 1, 16 }, &.{3});
    defer cb_val.free(row_target_ct);
    const column_target_ct = try cb_val.fromFloat32Shape(&.{ 1, 16, 1 }, &.{3});
    defer cb_val.free(column_target_ct);
    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = x, .value = x_ct },
        .{ .node_id = row_target, .value = row_target_ct },
        .{ .node_id = column_target, .value = column_target_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{ .runtime_inputs = &rt_inputs });
    defer result.deinit(&cb_val);

    const row_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(row_shape);
    const column_shape = try cb_val.tensorShape(result.outputs[1], allocator);
    defer allocator.free(column_shape);
    try std.testing.expectEqualSlices(i64, &.{ 1, 1, 16 }, row_shape);
    try std.testing.expectEqualSlices(i64, &.{ 1, 16, 1 }, column_shape);
}

test "resolveRuntimeReshapeDims preserves runtime batch for exported singleton reshape" {
    var out: [8]i64 = undefined;
    const resolved = resolveRuntimeReshapeDims(
        &.{ 2, 6, 4 },
        Shape.init(.f32, &.{ -1, 6, 4 }),
        Shape.init(.f32, &.{ 1, -1, -1, 2 }),
        &out,
    ) orelse return error.TestUnexpectedResult;

    try std.testing.expectEqualSlices(i64, &.{ 2, 6, 2, 2 }, resolved);
}

test "resolveRuntimeReshapeDims restores dynamic axes around inserted singleton dimensions" {
    var out: [8]i64 = undefined;
    const resolved = resolveRuntimeReshapeDims(
        &.{ 19, 1 },
        Shape.init(.i64, &.{ -1, -1 }),
        Shape.init(.i64, &.{ 1, 1, -1, -1 }),
        &out,
    ) orelse return error.TestUnexpectedResult;

    try std.testing.expectEqualSlices(i64, &.{ 1, 1, 19, 1 }, resolved);
}

test "resolveRuntimeReshapeDims preserves dynamic axes while removing singleton dimensions" {
    var out: [8]i64 = undefined;
    const resolved = resolveRuntimeReshapeDims(
        &.{ 1, 1, 19, 19 },
        Shape.init(.i64, &.{ 1, 1, -1, -1 }),
        Shape.init(.i64, &.{ 1, -1, -1 }),
        &out,
    ) orelse return error.TestUnexpectedResult;

    try std.testing.expectEqualSlices(i64, &.{ 1, 19, 19 }, resolved);
}

test "resolveRuntimeReshapeDims expands concrete singleton batch reshape" {
    var out: [8]i64 = undefined;
    const resolved = resolveRuntimeReshapeDims(
        &.{ 2, 768, 7, 7 },
        Shape.init(.f32, &.{ -1, 768, 7, 7 }),
        Shape.init(.f32, &.{ 1, 768, 49 }),
        &out,
    ) orelse return error.TestUnexpectedResult;

    try std.testing.expectEqualSlices(i64, &.{ 2, 768, 49 }, resolved);
}

test "resolveRuntimeReshapeDims expands exported concrete leading reshape" {
    var out: [8]i64 = undefined;
    const resolved = resolveRuntimeReshapeDims(
        &.{ 8192, 128 },
        Shape.init(.f32, &.{ -1, 128 }),
        Shape.init(.f32, &.{ 64, 64, 128 }),
        &out,
    ) orelse return error.TestUnexpectedResult;

    try std.testing.expectEqualSlices(i64, &.{ 128, 64, 128 }, resolved);
}

test "resolveRuntimeReshapeDims preserves dynamic leading axes when splitting hidden dim" {
    var out: [8]i64 = undefined;
    const resolved = resolveRuntimeReshapeDims(
        &.{ 3, 512, 128 },
        Shape.init(.f32, &.{ -1, -1, 128 }),
        Shape.init(.f32, &.{ -1, -1, 2, 64 }),
        &out,
    ) orelse return error.TestUnexpectedResult;

    try std.testing.expectEqualSlices(i64, &.{ 3, 512, 2, 64 }, resolved);
}

test "resolveRuntimeReshapeDims preserves dynamic resize interleaved axes" {
    var out: [8]i64 = undefined;
    const resolved = resolveRuntimeReshapeDims(
        &.{ 1, 24, 120, 120 },
        Shape.init(.f32, &.{ -1, 24, -1, -1 }),
        Shape.init(.f32, &.{ -1, 1, 24, 1, -1, 1, -1, 1 }),
        &out,
    ) orelse return error.TestUnexpectedResult;

    try std.testing.expectEqualSlices(i64, &.{ 1, 1, 24, 1, 120, 1, 120, 1 }, resolved);
}

test "resolveRuntimeReshapeDims collapses dynamic resize interleaved axes" {
    var out: [8]i64 = undefined;
    const resolved = resolveRuntimeReshapeDims(
        &.{ 1, 1, 24, 1, 120, 8, 120, 8 },
        Shape.init(.f32, &.{ -1, 1, 24, 1, -1, 8, -1, 8 }),
        Shape.init(.f32, &.{ -1, 24, -1, -1 }),
        &out,
    ) orelse return error.TestUnexpectedResult;

    try std.testing.expectEqualSlices(i64, &.{ 1, 24, 960, 960 }, resolved);
}

test "resolveProjectionRestoreFromSourceActual restores batch sequence after flattened matmul" {
    var out: [8]i64 = undefined;
    const resolved = resolveProjectionRestoreFromSourceActual(
        &.{ 3, 512, 128 },
        Shape.init(.f32, &.{ -1, -1, 128 }),
        3 * 512 * 128,
        &out,
    ) orelse return error.TestUnexpectedResult;

    try std.testing.expectEqualSlices(i64, &.{ 3, 512, 128 }, resolved);
}

test "resolveProjectionRestoreFromSourceActual restores packed attention output projection" {
    var out: [8]i64 = undefined;
    const resolved = resolveProjectionRestoreFromSourceActual(
        &.{ 3, 512, 2, 64 },
        Shape.init(.f32, &.{ -1, -1, 128 }),
        3 * 512 * 128,
        &out,
    ) orelse return error.TestUnexpectedResult;

    try std.testing.expectEqualSlices(i64, &.{ 3, 512, 128 }, resolved);
}

test "isLeadingAxisFlatten validates only true leading-axis flatten" {
    try std.testing.expect(isLeadingAxisFlatten(&.{ 3, 512, 128 }, &.{ 1536, 128 }));
    try std.testing.expect(!isLeadingAxisFlatten(&.{ 3, 512, 128 }, &.{ 384, 512 }));
    try std.testing.expect(!isLeadingAxisFlatten(&.{ 3, 512, 128 }, &.{ 1536, 64 }));
}

test "round-trip: linear → rmsNorm → gelu → elemAdd → multiply chain" {
    const allocator = std.testing.allocator;

    const x_data = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };
    const w_data = [_]f32{ 0.5, -0.5, 0.3, 0.3, -0.3, 0.5, -0.1, 0.1, 0.2 };
    const b_data = [_]f32{ 0.1, 0.2, 0.3 };
    const nw_data = [_]f32{ 1.0, 0.5, 2.0 };
    const scale_data = [_]f32{ 2.0, 2.0, 2.0, 2.0, 2.0, 2.0 };

    // ── Eager path ───────────────────────────────────────────────
    var tc_backend = TestCompute.init(allocator);
    defer tc_backend.deinit();
    try tc_backend.addWeight("w", &w_data);
    try tc_backend.addWeight("b", &b_data);
    try tc_backend.addWeight("nw", &nw_data);
    defer tc_backend.freeWeights();
    var cb = tc_backend.backend();

    const e_w = try cb.getWeight("w");
    const e_b = try cb.getWeight("b");
    const e_nw = try cb.getWeight("nw");
    const e_x = try cb.fromFloat32(&x_data);
    const e_scale = try cb.fromFloat32(&scale_data);

    // x:[2,3], w:[3,3] → linear → rmsNorm → gelu → multiply(scale)
    const e_lin = try cb.linear(e_x, e_w, e_b, 2, 3, 3);
    const e_norm = try cb.rmsNorm(e_lin, e_nw, 3, 1e-5);
    const e_act = try cb.gelu(e_norm);
    const e_out = try cb.multiply(e_act, e_scale);

    const expected = try cb.toFloat32(e_out, allocator);
    defer allocator.free(expected);

    cb.free(e_out);
    cb.free(e_act);
    cb.free(e_norm);
    cb.free(e_lin);
    cb.free(e_scale);
    cb.free(e_x);
    cb.free(e_nw);
    cb.free(e_b);
    cb.free(e_w);

    // ── Traced path ──────────────────────────────────────────────
    var tracer = try TracingCompute.initWithWeights(allocator, &.{
        .{ .name = "w", .shape = Shape.init(.f32, &.{ 3, 3 }) },
        .{ .name = "b", .shape = Shape.init(.f32, &.{3}) },
        .{ .name = "nw", .shape = Shape.init(.f32, &.{3}) },
    });
    defer tracer.deinit();

    var cb_t = tracer.backend();
    const t_w = try cb_t.getWeight("w");
    const t_b = try cb_t.getWeight("b");
    const t_nw = try cb_t.getWeight("nw");
    const t_x = try cb_t.fromFloat32(&x_data);
    const t_scale = try cb_t.fromFloat32(&scale_data);

    const t_lin = try cb_t.linear(t_x, t_w, t_b, 2, 3, 3);
    const t_norm = try cb_t.rmsNorm(t_lin, t_nw, 3, 1e-5);
    const t_act = try cb_t.gelu(t_norm);
    const t_out = try cb_t.multiply(t_act, t_scale);

    const t_result = try cb_t.toFloat32(t_out, allocator);
    defer allocator.free(t_result);

    // ── Interpret ────────────────────────────────────────────────
    var result = try execute(allocator, tracer.getGraph(), &cb, .{});
    defer result.deinit(&cb);

    const actual = try cb.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);

    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |e, a| {
        try std.testing.expectEqual(e, a);
    }
}

test "stateful: paged attention dispatch with layer_index auto-increment" {
    const allocator = std.testing.allocator;

    // Model params: batch=1, seq=1, heads=2, head_dim=4, kv_heads=2, hidden=8
    const heads = 2;
    const head_dim = 4;
    const hidden = heads * head_dim; // 8

    // Weights for 2-layer decoder: each layer has Q, K, V projections + attention
    var embed_w_data = [_]f32{0.1} ** (4 * hidden); // vocab=4, dim=8
    var qw_data = [_]f32{0.5} ** (hidden * hidden);
    var kw_data = [_]f32{0.3} ** (hidden * hidden);
    var vw_data = [_]f32{0.2} ** (hidden * hidden);

    var tc_backend = TestCompute.init(allocator);
    try tc_backend.addWeight("embed", &embed_w_data);
    try tc_backend.addWeight("l0.qw", &qw_data);
    try tc_backend.addWeight("l0.kw", &kw_data);
    try tc_backend.addWeight("l0.vw", &vw_data);
    try tc_backend.addWeight("l1.qw", &qw_data);
    try tc_backend.addWeight("l1.kw", &kw_data);
    try tc_backend.addWeight("l1.vw", &vw_data);
    defer tc_backend.deinit();
    defer tc_backend.freeWeights();
    var cb = tc_backend.backend();

    // ── Trace a 2-layer decoder ──────────────────────────────────
    var tracer = try TracingCompute.initWithWeights(allocator, &.{
        .{ .name = "embed", .shape = Shape.init(.f32, &.{ 4, hidden }) },
        .{ .name = "l0.qw", .shape = Shape.init(.f32, &.{ hidden, hidden }) },
        .{ .name = "l0.kw", .shape = Shape.init(.f32, &.{ hidden, hidden }) },
        .{ .name = "l0.vw", .shape = Shape.init(.f32, &.{ hidden, hidden }) },
        .{ .name = "l1.qw", .shape = Shape.init(.f32, &.{ hidden, hidden }) },
        .{ .name = "l1.kw", .shape = Shape.init(.f32, &.{ hidden, hidden }) },
        .{ .name = "l1.vw", .shape = Shape.init(.f32, &.{ hidden, hidden }) },
    });
    defer tracer.deinit();

    var cb_t = tracer.backend();

    // Embedding lookup
    const ids = [_]i64{2};
    const t_embed_w = try cb_t.getWeight("embed");
    const t_x = try cb_t.embeddingLookup(t_embed_w, &ids, 1, hidden);

    // Layer 0: QKV + attention
    const t_l0_qw = try cb_t.getWeight("l0.qw");
    const t_l0_kw = try cb_t.getWeight("l0.kw");
    const t_l0_vw = try cb_t.getWeight("l0.vw");
    const t_l0_q = try cb_t.linearNoBias(t_x, t_l0_qw, 1, hidden, hidden);
    const t_l0_k = try cb_t.linearNoBias(t_x, t_l0_kw, 1, hidden, hidden);
    const t_l0_v = try cb_t.linearNoBias(t_x, t_l0_vw, 1, hidden, hidden);
    const t_l0_out = try cb_t.gqaCausalAttention(t_l0_q, t_l0_k, t_l0_v, null, 1, 1, heads, heads, head_dim);

    // Layer 1: QKV + attention
    const t_l1_qw = try cb_t.getWeight("l1.qw");
    const t_l1_kw = try cb_t.getWeight("l1.kw");
    const t_l1_vw = try cb_t.getWeight("l1.vw");
    const t_l1_q = try cb_t.linearNoBias(t_l0_out, t_l1_qw, 1, hidden, hidden);
    const t_l1_k = try cb_t.linearNoBias(t_l0_out, t_l1_kw, 1, hidden, hidden);
    const t_l1_v = try cb_t.linearNoBias(t_l0_out, t_l1_vw, 1, hidden, hidden);
    const t_l1_out = try cb_t.gqaCausalAttention(t_l1_q, t_l1_k, t_l1_v, null, 1, 1, heads, heads, head_dim);

    const t_result = try cb_t.toFloat32(t_l1_out, allocator);
    defer allocator.free(t_result);

    // ── Interpret with paged attention context ───────────────────
    var result = try execute(allocator, tracer.getGraph(), &cb, .{
        .attention = contracts.AttentionContext{
            .mode = .paged_decode,
            .total_sequence_len = 1,
            .query_sequence_len = 1,
            .kv_sequence_len = 1,
            .layer_index = 0, // interpreter auto-increments
        },
        .embedding_ids = &ids,
    });
    defer result.deinit(&cb);

    // Verify: 2 attention calls dispatched to gqaPagedAttention
    try std.testing.expectEqual(@as(usize, 2), tc_backend.num_attn_calls);

    // Verify: layer indices auto-incremented [0, 1]
    try std.testing.expectEqual(@as(usize, 0), tc_backend.received_layer_indices[0]);
    try std.testing.expectEqual(@as(usize, 1), tc_backend.received_layer_indices[1]);

    // Verify: embedding ids were passed through
    try std.testing.expect(tc_backend.received_embedding_ids != null);
    try std.testing.expectEqual(@as(usize, 1), tc_backend.received_embedding_ids.?.len);
    try std.testing.expectEqual(@as(i64, 2), tc_backend.received_embedding_ids.?[0]);

    // Verify: output is correct size (1 * hidden = 8 floats)
    const out_data = try cb.toFloat32(result.outputs[0], allocator);
    defer allocator.free(out_data);
    try std.testing.expectEqual(@as(usize, hidden), out_data.len);
}

test "stateful: causal attention without paged context" {
    const allocator = std.testing.allocator;

    const hidden = 8;
    const heads = 2;
    const head_dim = 4;

    // Single-layer: Q projection + attention (no paged context)
    var qw_data = [_]f32{0.5} ** (hidden * hidden);
    var q_input = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0 };
    var k_input = [_]f32{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8 };
    var v_input = [_]f32{ 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5 };

    var tc_backend = TestCompute.init(allocator);
    try tc_backend.addWeight("qw", &qw_data);
    defer tc_backend.deinit();
    defer tc_backend.freeWeights();
    var cb = tc_backend.backend();

    // ── Trace ────────────────────────────────────────────────────
    var tracer = try TracingCompute.initWithWeights(allocator, &.{
        .{ .name = "qw", .shape = Shape.init(.f32, &.{ hidden, hidden }) },
    });
    defer tracer.deinit();
    var cb_t = tracer.backend();

    const t_q = try cb_t.fromFloat32(&q_input);
    const t_k = try cb_t.fromFloat32(&k_input);
    const t_v = try cb_t.fromFloat32(&v_input);
    const t_out = try cb_t.gqaCausalAttention(t_q, t_k, t_v, null, 1, 1, heads, heads, head_dim);

    const t_result = try cb_t.toFloat32(t_out, allocator);
    defer allocator.free(t_result);

    // ── Interpret WITHOUT attention context ───────────────────────
    var result = try execute(allocator, tracer.getGraph(), &cb, .{});
    defer result.deinit(&cb);

    // Verify: gqaPagedAttention was NOT called (no paged context)
    try std.testing.expectEqual(@as(usize, 0), tc_backend.num_attn_calls);

    // The causal attention fallback returns Q data directly in TestCompute
    const out_data = try cb.toFloat32(result.outputs[0], allocator);
    defer allocator.free(out_data);
    try std.testing.expectEqual(@as(usize, hidden), out_data.len);
}

test "buffer donation: donated inputs are freed by interpreter" {
    const allocator = std.testing.allocator;

    // Trace: add(input, weight) → output
    var w_data = [_]f32{ 10.0, 20.0, 30.0 };

    var tc_backend = TestCompute.init(allocator);
    defer tc_backend.deinit();
    try tc_backend.addWeight("w", &w_data);
    defer tc_backend.freeWeights();
    var cb = tc_backend.backend();

    var tracer = try TracingCompute.initWithWeights(allocator, &.{
        .{ .name = "w", .shape = Shape.init(.f32, &.{3}) },
    });
    defer tracer.deinit();
    var cb_t = tracer.backend();

    const t_x = try cb_t.fromFloat32(&[_]f32{ 1.0, 2.0, 3.0 });
    const t_w = try cb_t.getWeight("w");
    const t_out = try cb_t.add(t_x, t_w);
    const t_result = try cb_t.toFloat32(t_out, allocator);
    defer allocator.free(t_result);

    // Identify the fromFloat32 node. It's node 0 in the graph (first
    // op traced). We supply it as a donated runtime input so the
    // interpreter owns and frees it.
    const graph = tracer.getGraph();
    const input_node_id: NodeId = 0;

    // Create a real input buffer via the backend.
    // NOT deferred — ownership transfers to interpreter via donate.
    const input_ct = try cb.fromFloat32(&[_]f32{ 1.0, 2.0, 3.0 });

    var result = try execute(allocator, graph, &cb, .{
        .runtime_inputs = &.{.{ .node_id = input_node_id, .value = input_ct }},
        .donate = &.{true},
    });
    defer result.deinit(&cb);

    // Verify output: [1+10, 2+20, 3+30] = [11, 22, 33]
    const out_data = try cb.toFloat32(result.outputs[0], allocator);
    defer allocator.free(out_data);
    try std.testing.expectEqual(@as(usize, 3), out_data.len);
    try std.testing.expectEqual(@as(f32, 11.0), out_data[0]);
    try std.testing.expectEqual(@as(f32, 22.0), out_data[1]);
    try std.testing.expectEqual(@as(f32, 33.0), out_data[2]);

    // If the donated input leaked, the allocator would detect it.
    // The test passing without leaks proves donation works.
}

test "cache + extract + interpret round-trip (graphForward pattern)" {
    // Simulates the generation pipeline's graphForward() workflow:
    // 1. Trace a forward pass
    // 2. extractGraph() to transfer ownership to cache
    // 3. Deinit tracer
    // 4. Execute cached graph through backend
    // 5. Execute again (cache hit) and verify bit-exact match
    const allocator = std.testing.allocator;
    const cache_mod = @import("cache.zig");

    const x_data = [_]f32{ 0.5, -0.3, 0.7, 0.1, -0.2, 0.4, -0.6, 0.8 };
    const w_data = [_]f32{
        0.2,  -0.1, 0.3,  0.1,
        -0.3, 0.2,  0.1,  -0.2,
        0.1,  0.4,  -0.2, 0.3,
        0.3,  -0.3, 0.1,  0.2,
    };
    const nw_data = [_]f32{ 1.0, 1.0, 1.0, 1.0 };

    // Set up test backend with weights.
    var tc_backend = TestCompute.init(allocator);
    defer tc_backend.deinit();
    try tc_backend.addWeight("w", &w_data);
    try tc_backend.addWeight("nw", &nw_data);
    defer tc_backend.freeWeights();
    var cb = tc_backend.backend();

    // ── Eager reference ─────────────────────────────────────────
    const e_x = try cb.fromFloat32(&x_data);
    const e_w = try cb.getWeight("w");
    const e_nw = try cb.getWeight("nw");
    const e_lin = try cb.linearNoBias(e_x, e_w, 2, 4, 4);
    const e_norm = try cb.rmsNorm(e_lin, e_nw, 4, 1e-5);
    const e_out = try cb.gelu(e_norm);
    const expected = try cb.toFloat32(e_out, allocator);
    defer allocator.free(expected);
    cb.free(e_out);
    cb.free(e_norm);
    cb.free(e_lin);
    cb.free(e_nw);
    cb.free(e_w);
    cb.free(e_x);

    // ── Trace and extract into cache ────────────────────────────
    var cache = cache_mod.GraphCache.init(allocator);
    defer cache.deinit();

    const key = cache_mod.CacheKey{
        .config_hash = 12345,
        .batch = 1,
        .seq_len = 1,
        .attention_mode = .paged_decode,
    };

    {
        var tracer = try TracingCompute.initWithWeights(allocator, &.{
            .{ .name = "w", .shape = Shape.init(.f32, &.{ 4, 4 }) },
            .{ .name = "nw", .shape = Shape.init(.f32, &.{4}) },
        });
        var cb_t = tracer.backend();

        const t_x = try cb_t.fromFloat32(&x_data);
        const t_w = try cb_t.getWeight("w");
        const t_nw = try cb_t.getWeight("nw");
        const t_lin = try cb_t.linearNoBias(t_x, t_w, 2, 4, 4);
        const t_norm = try cb_t.rmsNorm(t_lin, t_nw, 4, 1e-5);
        const t_act = try cb_t.gelu(t_norm);
        const t_result = try cb_t.toFloat32(t_act, allocator);
        allocator.free(t_result);

        // Extract graph into cache, then deinit tracer (no double-free).
        try cache.put(key, tracer.extractGraph());
        tracer.deinit();
    }

    try std.testing.expectEqual(@as(usize, 1), cache.count());

    // ── First interpret from cache ──────────────────────────────
    {
        const graph = cache.get(key).?;
        var result = try execute(allocator, graph, &cb, .{});
        defer result.deinit(&cb);

        const actual = try cb.toFloat32(result.outputs[0], allocator);
        defer allocator.free(actual);

        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |e, a| {
            try std.testing.expectEqual(e, a);
        }
    }

    // ── Second interpret (cache hit, same graph) ────────────────
    {
        const graph = cache.get(key).?;
        var result = try execute(allocator, graph, &cb, .{});
        defer result.deinit(&cb);

        const actual = try cb.toFloat32(result.outputs[0], allocator);
        defer allocator.free(actual);

        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |e, a| {
            try std.testing.expectEqual(e, a);
        }
    }

    // ── Third interpret with cached analysis ───────────────────
    {
        const graph = cache.get(key).?;
        var ca = try CachedAnalysis.compute(allocator, graph);
        defer ca.deinit(allocator);

        var result = try execute(allocator, graph, &cb, .{ .cached_analysis = ca });
        defer result.deinit(&cb);

        const actual = try cb.toFloat32(result.outputs[0], allocator);
        defer allocator.free(actual);

        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |e, a| {
            try std.testing.expectEqual(e, a);
        }
    }
}

test "MoE round-trip: trace grouped path → interpret with live routing" {
    // Simulate a minimal MoE layer:
    //   router_logits = linearNoBias(input, router_w)
    //   routes = moeSelectRoutes(router_logits)
    //   zero = zeroTensor(total, dim)
    //   gathered = takeRows(input, routes.rows)
    //   expert_out = moeLinearNoBias(gathered, routes.expert_ids, expert_w)
    //   output = moeScatterAdd(zero, routes.rows, routes.weights, expert_out)
    //
    // During tracing, moeSelectRoutes returns dummy routing (all -> expert 0).
    // During interpretation, TestCompute.moeSelectRoutes returns round-robin.
    // The test verifies the interpreter threads live routing to MoE ops.

    const allocator = std.testing.allocator;
    const total: usize = 2; // 2 tokens
    const hidden: usize = 4; // hidden dim
    const num_experts: usize = 2;
    const top_k: usize = 1;

    // Input: [2, 4]
    const input_data = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0 };
    // Router weight: [num_experts, hidden] = [2, 4]
    const router_w_data = [_]f32{ 0.1, 0.2, 0.3, 0.4, -0.1, -0.2, -0.3, -0.4 };
    // Expert weight: [num_experts * hidden, hidden] = [8, 4]
    // Expert 0 weights (rows 0-3): identity-ish
    // Expert 1 weights (rows 4-7): 2x scaling
    const expert_w_data = [_]f32{
        1.0, 0.0, 0.0, 0.0, // expert 0, out_dim 0
        0.0, 1.0, 0.0, 0.0, // expert 0, out_dim 1
        0.0, 0.0, 1.0, 0.0, // expert 0, out_dim 2
        0.0, 0.0, 0.0, 1.0, // expert 0, out_dim 3
        2.0, 0.0, 0.0, 0.0, // expert 1, out_dim 0
        0.0, 2.0, 0.0, 0.0, // expert 1, out_dim 1
        0.0, 0.0, 2.0, 0.0, // expert 1, out_dim 2
        0.0, 0.0, 0.0, 2.0, // expert 1, out_dim 3
    };

    // ── Eager: run MoE manually with round-robin routing ────────
    var tc_backend = TestCompute.init(allocator);
    defer tc_backend.deinit();
    try tc_backend.addWeight("router_w", &router_w_data);
    try tc_backend.addWeight("expert_w", &expert_w_data);
    defer tc_backend.freeWeights();
    var cb = tc_backend.backend();

    // Compute router logits (not used for routing in test, but needed for graph structure)
    const e_input = try cb.fromFloat32(&input_data);
    const e_rw = try cb.getWeight("router_w");
    const e_router_logits = try cb.linearNoBias(e_input, e_rw, total, hidden, num_experts);
    cb.free(e_router_logits);
    cb.free(e_rw);

    // TestCompute.moeSelectRoutes does round-robin: row0->expert0, row1->expert1
    // So gathered input is [row0, row1], expert_ids = [0, 1]
    const e_zero = (try cb.zeroTensor(total, hidden)).?;
    // takeRows with round-robin: both rows taken in order
    const e_ew = try cb.getWeight("expert_w");
    // Manually compute: expert0(row0) = row0 * I = row0, expert1(row1) = row1 * 2I = 2*row1
    const e_gathered = (try cb.takeRows(e_input, &.{ 0, 1 }, total * top_k, hidden)).?;
    const e_expert_out = (try cb.moeLinearNoBias(e_gathered, &.{ 0, 1 }, null, null, null, e_ew, total * top_k, hidden, hidden)).?;
    const e_output = (try cb.moeScatterAdd(e_zero, &.{ 0, 1 }, &.{ 1.0, 1.0 }, e_expert_out, total * top_k, hidden)).?;

    const expected = try cb.toFloat32(e_output, allocator);
    defer allocator.free(expected);

    cb.free(e_output);
    cb.free(e_expert_out);
    cb.free(e_gathered);
    cb.free(e_ew);
    cb.free(e_zero);
    cb.free(e_input);

    // ── Trace: moeSelectRoutes returns dummy (all -> expert 0) ──
    var tracer = TracingCompute.init(allocator);
    defer tracer.deinit();
    var cb_t = tracer.backend();

    const t_input = try cb_t.fromFloat32(&input_data);
    const t_rw = try cb_t.getWeight("router_w");
    const t_router_logits = try cb_t.linearNoBias(t_input, t_rw, total, hidden, num_experts);

    // moeSelectRoutes now returns dummy routing during tracing
    const t_routes = (try cb_t.moeSelectRoutes(0, t_router_logits, total, num_experts, top_k, 1.0, allocator)).?;
    defer allocator.free(t_routes.expert_ids);
    defer allocator.free(t_routes.route_weights);

    const t_zero = (try cb_t.zeroTensor(total, hidden)).?;
    const t_gathered = (try cb_t.takeRows(t_input, t_routes.expert_ids, total * top_k, hidden)).?;
    const t_ew = try cb_t.getWeight("expert_w");
    const t_expert_out = (try cb_t.moeLinearNoBias(t_gathered, t_routes.expert_ids, null, null, null, t_ew, total * top_k, hidden, hidden)).?;
    const t_output = (try cb_t.moeScatterAdd(t_zero, t_routes.expert_ids, t_routes.route_weights, t_expert_out, total * top_k, hidden)).?;

    const t_result = try cb_t.toFloat32(t_output, allocator);
    allocator.free(t_result);

    // ── Interpret: moeSelectRoutes uses real TestCompute routing ─
    const graph = tracer.getGraph();
    var result = try execute(allocator, graph, &cb, .{});
    defer result.deinit(&cb);

    const actual = try cb.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);

    // Verify bit-exact match with eager round-robin routing
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |e, a| {
        try std.testing.expectEqual(e, a);
    }
}

// ── Lowered / primitive graph execution tests ────────────────────────

const native_mod = if (build_options.enable_native) @import("../ops/native_compute.zig") else struct {};
const NativeCompute = if (build_options.enable_native) native_mod.NativeCompute else opaque {};
const WeightStore = if (build_options.enable_native) native_mod.WeightStore else opaque {};

test "native interpreter does not donate a reshape view before a future sibling view" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = ml.graph.Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{4}));
    const early_view = try bld.reshape(x, Shape.init(.f32, &.{ 2, 2 }));
    const two = try bld.scalarConst(.f32, 2.0);
    const scaled = try bld.mul(early_view, two);
    // Deliberately create this sibling view after `scaled`. Without
    // alias-group liveness, native's in-place multiply mutates `x` before
    // this node executes and the sum becomes 20 instead of 10.
    const later_view = try bld.reshape(x, Shape.init(.f32, &.{ 2, 2 }));
    const later_sum = try bld.reduceSum(later_view, &.{ 0, 1 });
    try g.markOutput(scaled);
    try g.markOutput(later_sum);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const x_ct = try cb_val.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{4});
    defer cb_val.free(x_ct);
    const rt_inputs = [_]RuntimeInput{.{ .node_id = x, .value = x_ct }};

    var result = try execute(allocator, &g, &cb_val, .{ .runtime_inputs = &rt_inputs });
    defer result.deinit(&cb_val);

    const scaled_data = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(scaled_data);
    try std.testing.expectEqualSlices(f32, &.{ 2, 4, 6, 8 }, scaled_data);
    const sum_data = try cb_val.toFloat32(result.outputs[1], allocator);
    defer allocator.free(sum_data);
    try std.testing.expectEqualSlices(f32, &.{10}, sum_data);
    const original_data = try cb_val.toFloat32(x_ct, allocator);
    defer allocator.free(original_data);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, original_data);
}

test "execute lowered graph through native backend" {
    // Build: y = linear(x, w, b) = x @ w^T + b
    // Lower to primitives and execute. Verify output matches hand-computed values.
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = ml.graph.Builder.init(&g);

    // x:[2,4], w:[3,4], bias:[3] → y:[2,3]
    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w = try bld.parameter("w", Shape.init(.f32, &.{ 3, 4 }));
    const bias = try bld.parameter("bias", Shape.init(.f32, &.{3}));
    const y = try bld.linear(x, w, bias, 2, 4, 3);
    try g.markOutput(y);

    // Lower: fused linear → primitive (transpose + dot_general + broadcast + add).
    var lower_result = try ml.graph.lower.lower(allocator, &g);
    defer lower_result.deinit();

    // Set up native backend (empty WeightStore — we inject params via runtime_inputs).
    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    // Create parameter CTs.
    const x_ct = try cb_val.fromFloat32(&.{ 1, 2, 3, 4, 5, 6, 7, 8 });
    const w_ct = try cb_val.fromFloat32(&.{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2 });
    const bias_ct = try cb_val.fromFloat32(&.{ 0.01, 0.02, 0.03 });

    // Map original param IDs → lowered graph IDs via id_map.
    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = lower_result.id_map[x], .value = x_ct },
        .{ .node_id = lower_result.id_map[w], .value = w_ct },
        .{ .node_id = lower_result.id_map[bias], .value = bias_ct },
    };

    var result = try execute(allocator, &lower_result.graph, &cb_val, .{
        .runtime_inputs = &rt_inputs,
    });

    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);

    // Clean up CTs: free runtime inputs + outputs.
    cb_val.free(x_ct);
    cb_val.free(w_ct);
    cb_val.free(bias_ct);
    result.deinit(&cb_val);

    // Expected: y = x @ w^T + bias
    // row0: [1*0.1+2*0.2+3*0.3+4*0.4, ...] + bias = [3.01, 7.02, 11.03]
    // row1: [5*0.1+6*0.2+7*0.3+8*0.4, 5*0.5+..., 5*0.9+...] + bias = [7.01, 17.42, 27.83]
    const expected = [_]f32{ 3.01, 7.02, 11.03, 7.01, 17.42, 27.83 };
    try std.testing.expectEqual(@as(usize, 6), actual.len);
    for (expected[0..], actual) |e, a| {
        try std.testing.expectApproxEqAbs(e, a, 1e-4);
    }
}

test "primitive elementwise ops execute through native" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    // Build: y = negate(x) + exp(const_1) where const_1 = [1.0, 2.0]
    const x = try builder.parameter("x", Shape.init(.f32, &.{2}));
    const neg_x = try builder.neg(x);
    const c = try builder.tensorConst(&.{ 1.0, 2.0 }, Shape.init(.f32, &.{2}));
    const exp_c = try builder.expOp(c);
    const result_node = try builder.add(neg_x, exp_c);
    try g.markOutput(result_node);

    // All primitive ops — set up native backend, inject x via runtime_inputs.
    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const x_ct = try cb_val.fromFloat32(&.{ 3.0, 4.0 });
    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = x, .value = x_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{
        .runtime_inputs = &rt_inputs,
    });

    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);

    cb_val.free(x_ct);
    result.deinit(&cb_val);

    // Expected: [-3 + e^1, -4 + e^2]
    try std.testing.expectEqual(@as(usize, 2), actual.len);
    try std.testing.expectApproxEqAbs(@as(f32, -3.0 + @exp(@as(f32, 1.0))), actual[0], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, -4.0 + @exp(@as(f32, 2.0))), actual[1], 1e-4);
}

test "execute clones aliased passthrough outputs that outlive their input branch" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const x = try builder.parameter("x", Shape.init(.f32, &.{2}));
    const y = try builder.convertDtype(x, .f32);
    const z = try builder.expOp(x);
    const out = try builder.add(y, z);
    try g.markOutput(out);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const x_ct = try cb_val.fromFloat32(&.{ 1.5, -0.5 });
    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = x, .value = x_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{
        .runtime_inputs = &rt_inputs,
    });
    defer result.deinit(&cb_val);
    defer cb_val.free(x_ct);

    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);

    try std.testing.expectEqual(@as(usize, 2), actual.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.5 + @exp(@as(f32, 1.5))), actual[0], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, -0.5 + @exp(@as(f32, -0.5))), actual[1], 1e-4);
}

test "execute preserves runtime shape when cloning an aliased dynamic tensor" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ -1, 3 }));
    const y = try builder.convertDtype(x, .f32);
    const z = try builder.expOp(x);
    const out = try builder.add(y, z);
    try g.markOutput(out);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const input = [_]f32{ 1.5, -0.5, 2.0, -1.0, 0.0, 0.5 };
    const x_ct = try cb_val.fromFloat32Shape(&input, &.{ 2, 3 });
    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = x, .value = x_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{
        .runtime_inputs = &rt_inputs,
    });
    defer result.deinit(&cb_val);
    defer cb_val.free(x_ct);

    const actual_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(actual_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, actual_shape);

    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    for (input, actual) |value, got| {
        try std.testing.expectApproxEqAbs(value + @exp(value), got, 1e-4);
    }
}

test "execution result deinit frees duplicate output handles once" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const x = try builder.parameter("x", Shape.init(.f32, &.{2}));
    const y = try builder.expOp(x);
    try g.markOutput(y);
    try g.markOutput(y);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const x_ct = try cb_val.fromFloat32(&.{ 1.0, 2.0 });
    defer cb_val.free(x_ct);
    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = x, .value = x_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{
        .runtime_inputs = &rt_inputs,
    });
    defer result.deinit(&cb_val);

    try std.testing.expectEqual(@as(usize, 2), result.outputs.len);
    try std.testing.expect(result.outputs[0] == result.outputs[1]);

    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectApproxEqAbs(@as(f32, @exp(@as(f32, 1.0))), actual[0], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, @exp(@as(f32, 2.0))), actual[1], 1e-4);
}

test "reshape uses declared input shape before symbolic transpose" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ 2, 3, 8 }));
    const reshaped = try builder.reshape(x, Shape.init(.f32, &.{ -1, 3, -1, 2 }));
    const transposed = try builder.transpose(reshaped, &.{ 0, 2, 1, 3 });
    try g.markOutput(transposed);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    var input: [2 * 3 * 8]f32 = undefined;
    for (&input, 0..) |*value, i| value.* = @floatFromInt(i);
    const x_ct = try cb_val.fromFloat32(&input);
    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = x, .value = x_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{
        .runtime_inputs = &rt_inputs,
    });
    defer result.deinit(&cb_val);
    defer cb_val.free(x_ct);

    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectEqual(@as(usize, input.len), actual.len);

    const actual_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(actual_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 4, 3, 2 }, actual_shape);
}

test "reshape preserves runtime batch for exported singleton target" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ -1, 6, 4 }));
    const reshaped = try builder.reshape(x, Shape.init(.f32, &.{ 1, -1, -1, 2 }));
    try g.markOutput(reshaped);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    var input: [2 * 6 * 4]f32 = undefined;
    for (&input, 0..) |*value, i| value.* = @floatFromInt(i);
    const x_ct = try cb_val.fromFloat32Shape(&input, &.{ 2, 6, 4 });
    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = x, .value = x_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{
        .runtime_inputs = &rt_inputs,
    });
    defer result.deinit(&cb_val);
    defer cb_val.free(x_ct);

    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectEqual(@as(usize, input.len), actual.len);

    const actual_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(actual_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 6, 2, 2 }, actual_shape);
}

test "runtime shape drives symbolic reduce" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ -1, -1, 3 }));
    const reduced = try builder.reduceSum(x, &.{1});
    try g.markOutput(reduced);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    var input: [2 * 4 * 3]f32 = undefined;
    for (&input, 0..) |*value, i| value.* = @floatFromInt(i + 1);
    const x_ct = try cb_val.fromFloat32Shape(&input, &.{ 2, 4, 3 });
    defer cb_val.free(x_ct);

    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = x, .value = x_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{
        .runtime_inputs = &rt_inputs,
    });
    defer result.deinit(&cb_val);

    const actual_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(actual_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 1, 3 }, actual_shape);

    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ 22, 26, 30, 70, 74, 78 }, actual);
}

test "runtime shape drives symbolic slice" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ -1, -1, 3 }));
    var attrs = ml.graph.node.SliceAttrs{};
    attrs.num_axes = 3;
    attrs.starts = .{ 0, 1, 0, 0, 0, 0, 0, 0 };
    attrs.limits = .{ -1, 3, 3, 0, 0, 0, 0, 0 };
    attrs.strides = .{ 1, 1, 1, 0, 0, 0, 0, 0 };
    const sliced = try g.addNode(.{
        .op = .{ .slice = attrs },
        .output_shape = Shape.init(.f32, &.{ -1, 2, 3 }),
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(sliced);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    var input: [2 * 4 * 3]f32 = undefined;
    for (&input, 0..) |*value, i| value.* = @floatFromInt(i + 1);
    const x_ct = try cb_val.fromFloat32Shape(&input, &.{ 2, 4, 3 });
    defer cb_val.free(x_ct);

    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = x, .value = x_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{
        .runtime_inputs = &rt_inputs,
    });
    defer result.deinit(&cb_val);

    const actual_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(actual_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 2, 3 }, actual_shape);

    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectEqualSlices(f32, &.{
        4,  5,  6,
        7,  8,  9,
        16, 17, 18,
        19, 20, 21,
    }, actual);
}

test "runtime shape expression bounds a slice of a static tensor" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const input_ids = try builder.parameter("input_ids", Shape.init(.i64, &.{ -1, -1 }));
    const input_shape = try g.addNode(.{
        .op = .{ .shape_of = .{ .start = 0, .end = 2 } },
        .output_shape = Shape.init(.i64, &.{2}),
        .inputs = .{ input_ids, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const sequence_axis = try builder.tensorConst(&.{1.0}, Shape.init(.i64, &.{}));
    const sequence_length = try builder.gather(input_shape, sequence_axis, Shape.init(.i64, &.{}));
    const ends = try builder.reshape(sequence_length, Shape.init(.i64, &.{1}));
    const starts = try builder.tensorConst(&.{0.0}, Shape.init(.i64, &.{1}));

    var positions: [12]f32 = undefined;
    for (&positions, 0..) |*position, i| position.* = @floatFromInt(i);
    const position_ids = try builder.tensorConst(&positions, Shape.init(.i64, &.{ 1, 12 }));
    var attrs = ml.graph.node.SliceAttrs{};
    attrs.num_axes = 2;
    attrs.starts = .{ 0, 0, 0, 0, 0, 0, 0, 0 };
    attrs.limits = .{ 1, -1, 0, 0, 0, 0, 0, 0 };
    attrs.strides = .{ 1, 1, 1, 1, 1, 1, 1, 1 };
    attrs.bound_axes[0] = 1;
    attrs.num_bound_axes = 1;
    attrs.runtime_limits = true;
    const sliced = try g.addNode(.{
        .op = .{ .slice = attrs },
        .output_shape = Shape.init(.i64, &.{ 1, -1 }),
        .inputs = .{ position_ids, starts, ends, null_node },
        .num_inputs = 3,
    });
    try g.markOutput(sliced);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const ids: [3 * 4]f32 = @splat(0);
    const input_ids_ct = try cb_val.fromFloat32Shape(&ids, &.{ 3, 4 });
    defer cb_val.free(input_ids_ct);
    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = input_ids, .value = input_ids_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{ .runtime_inputs = &rt_inputs });
    defer result.deinit(&cb_val);

    const actual_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(actual_shape);
    try std.testing.expectEqualSlices(i64, &.{ 1, 4 }, actual_shape);
    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ 0, 1, 2, 3 }, actual);
}

test "runtime shape drives symbolic concat" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const a = try builder.parameter("a", Shape.init(.f32, &.{ -1, -1, 3 }));
    const b = try builder.parameter("b", Shape.init(.f32, &.{ -1, -1, 3 }));
    const joined = try builder.concat(a, b, 1);
    try g.markOutput(joined);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    var a_input: [2 * 2 * 3]f32 = undefined;
    for (&a_input, 0..) |*value, i| value.* = @floatFromInt(i + 1);
    var b_input: [2 * 3 * 3]f32 = undefined;
    for (&b_input, 0..) |*value, i| value.* = @floatFromInt(i + 101);

    const a_ct = try cb_val.fromFloat32Shape(&a_input, &.{ 2, 2, 3 });
    defer cb_val.free(a_ct);
    const b_ct = try cb_val.fromFloat32Shape(&b_input, &.{ 2, 3, 3 });
    defer cb_val.free(b_ct);

    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = a, .value = a_ct },
        .{ .node_id = b, .value = b_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{
        .runtime_inputs = &rt_inputs,
    });
    defer result.deinit(&cb_val);

    const actual_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(actual_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 5, 3 }, actual_shape);

    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectEqualSlices(f32, &.{
        1,   2,   3,
        4,   5,   6,
        101, 102, 103,
        104, 105, 106,
        107, 108, 109,
        7,   8,   9,
        10,  11,  12,
        110, 111, 112,
        113, 114, 115,
        116, 117, 118,
    }, actual);
}

test "runtime shape drives symbolic batched dot_general" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const a = try builder.parameter("a", Shape.init(.f32, &.{ -1, -1, 3 }));
    const b = try builder.parameter("b", Shape.init(.f32, &.{ -1, 3, 2 }));
    const product = try builder.matmul3D(a, b);
    try g.markOutput(product);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const a_input = [_]f32{
        1,  2,  3,
        4,  5,  6,
        7,  8,  9,
        10, 11, 12,
    };
    const b_input = [_]f32{
        1, 0,
        0, 1,
        1, 1,
        2, 0,
        0, 2,
        1, 1,
    };

    const a_ct = try cb_val.fromFloat32Shape(&a_input, &.{ 2, 2, 3 });
    defer cb_val.free(a_ct);
    const b_ct = try cb_val.fromFloat32Shape(&b_input, &.{ 2, 3, 2 });
    defer cb_val.free(b_ct);

    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = a, .value = a_ct },
        .{ .node_id = b, .value = b_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{
        .runtime_inputs = &rt_inputs,
    });
    defer result.deinit(&cb_val);

    const actual_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(actual_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 2, 2 }, actual_shape);

    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectEqualSlices(f32, &.{
        4,  5,
        10, 11,
        23, 25,
        32, 34,
    }, actual);
}

test "runtime shape drives symbolic argmax" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ -1, -1, 3 }));
    const out = try builder.argMax(x, 1, true);
    try g.markOutput(out);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const input = [_]f32{
        1,  9,  3,
        4,  2,  6,
        7,  5,  8,
        10, 1,  0,
        11, 3,  4,
        5,  12, 6,
        7,  8,  13,
        9,  10, 2,
    };
    const x_ct = try cb_val.fromFloat32Shape(&input, &.{ 2, 4, 3 });
    defer cb_val.free(x_ct);

    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = x, .value = x_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{
        .runtime_inputs = &rt_inputs,
    });
    defer result.deinit(&cb_val);

    const actual_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(actual_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 1, 3 }, actual_shape);

    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ 3, 0, 2, 0, 1, 2 }, actual);
}

test "runtime binary broadcasting expands complementary symbolic axes" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const lhs = try builder.parameter("lhs", Shape.init(.f32, &.{ -1, 1 }));
    const rhs = try builder.parameter("rhs", Shape.init(.f32, &.{ 1, -1 }));
    const out = try builder.sub(lhs, rhs);
    try g.markOutput(out);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const lhs_ct = try cb_val.fromFloat32Shape(&.{ 0, 1, 2 }, &.{ 3, 1 });
    defer cb_val.free(lhs_ct);
    const rhs_ct = try cb_val.fromFloat32Shape(&.{ 0, 10, 20, 30 }, &.{ 1, 4 });
    defer cb_val.free(rhs_ct);
    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = lhs, .value = lhs_ct },
        .{ .node_id = rhs, .value = rhs_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{ .runtime_inputs = &rt_inputs });
    defer result.deinit(&cb_val);

    const actual_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(actual_shape);
    try std.testing.expectEqualSlices(i64, &.{ 3, 4 }, actual_shape);
    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectEqualSlices(f32, &.{
        0, -10, -20, -30,
        1, -9,  -19, -29,
        2, -8,  -18, -28,
    }, actual);
}

test "runtime shape drives symbolic broadcast_in_dim" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ -1, 1, 3 }));
    var attrs = ml.graph.node.BroadcastAttrs{ .target_shape = Shape.init(.f32, &.{ -1, 4, 3 }) };
    attrs.broadcast_axes = .{ 0, 1, 2, 0, 0, 0, 0, 0 };
    attrs.num_axes = 3;
    const out = try g.addNode(.{
        .op = .{ .broadcast_in_dim = attrs },
        .output_shape = attrs.target_shape,
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(out);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const input = [_]f32{ 1, 2, 3, 4, 5, 6 };
    const x_ct = try cb_val.fromFloat32Shape(&input, &.{ 2, 1, 3 });
    defer cb_val.free(x_ct);

    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = x, .value = x_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{
        .runtime_inputs = &rt_inputs,
    });
    defer result.deinit(&cb_val);

    const actual_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(actual_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 4, 3 }, actual_shape);

    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectEqualSlices(f32, &.{
        1, 2, 3,
        1, 2, 3,
        1, 2, 3,
        1, 2, 3,
        4, 5, 6,
        4, 5, 6,
        4, 5, 6,
        4, 5, 6,
    }, actual);
}

test "runtime shape tensor drives dynamic broadcast_in_dim" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ 1, 1, -1 }));
    const target = try builder.parameter("target", Shape.init(.i64, &.{3}));
    var attrs = ml.graph.node.BroadcastAttrs{ .target_shape = Shape.init(.f32, &.{ -1, -1, -1 }) };
    attrs.broadcast_axes = .{ 0, 1, 2, 0, 0, 0, 0, 0 };
    attrs.num_axes = 3;
    const out = try g.addNode(.{
        .op = .{ .broadcast_in_dim = attrs },
        .output_shape = attrs.target_shape,
        .inputs = .{ x, target, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(out);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const x_ct = try cb_val.fromFloat32Shape(&.{ 1, 2, 3 }, &.{ 1, 1, 3 });
    defer cb_val.free(x_ct);
    const target_ct = try cb_val.fromFloat32Shape(&.{ 2, 4, 3 }, &.{3});
    defer cb_val.free(target_ct);
    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = x, .value = x_ct },
        .{ .node_id = target, .value = target_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{ .runtime_inputs = &rt_inputs });
    defer result.deinit(&cb_val);

    const actual_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(actual_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 4, 3 }, actual_shape);
    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectEqual(@as(usize, 24), actual.len);
    for (0..8) |row| {
        try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3 }, actual[row * 3 ..][0..3]);
    }
}

test "native interpreter executes GatherElements along the selected axis" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const data = try builder.parameter("data", Shape.init(.f32, &.{ -1, -1, 5 }));
    const indices = try builder.parameter("indices", Shape.init(.i64, &.{ -1, -1, -1 }));
    const out = try g.addNode(.{
        .op = .{ .gather = .{ .axis = 2, .elements = true } },
        .output_shape = Shape.init(.f32, &.{ -1, -1, -1 }),
        .inputs = .{ data, indices, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(out);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const data_values = [_]f32{
        10, 11, 12, 13, 14,
        20, 21, 22, 23, 24,
        30, 31, 32, 33, 34,
        40, 41, 42, 43, 44,
    };
    const index_values = [_]f32{
        4, 0,  2,
        1, 3,  0,
        2, 2,  4,
        0, -1, 1,
    };
    const data_ct = try cb_val.fromFloat32Shape(&data_values, &.{ 2, 2, 5 });
    defer cb_val.free(data_ct);
    const indices_ct = try cb_val.fromFloat32Shape(&index_values, &.{ 2, 2, 3 });
    defer cb_val.free(indices_ct);
    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = data, .value = data_ct },
        .{ .node_id = indices, .value = indices_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{ .runtime_inputs = &rt_inputs });
    defer result.deinit(&cb_val);
    const actual_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(actual_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 2, 3 }, actual_shape);
    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ 14, 10, 12, 21, 23, 20, 32, 32, 34, 40, 44, 41 }, actual);
}

test "native GatherElements uses concrete shape of a dynamic broadcast result" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const data = try builder.parameter("data", Shape.init(.f32, &.{ -1, -1, 5 }));
    const indices_seed = try builder.parameter("indices_seed", Shape.init(.i64, &.{ 1, -1, -1 }));
    const target = try builder.parameter("target", Shape.init(.i64, &.{3}));
    var broadcast_attrs = ml.graph.node.BroadcastAttrs{ .target_shape = Shape.init(.i64, &.{ -1, -1, -1 }) };
    broadcast_attrs.broadcast_axes = .{ 0, 1, 2, 0, 0, 0, 0, 0 };
    broadcast_attrs.num_axes = 3;
    const indices = try g.addNode(.{
        .op = .{ .broadcast_in_dim = broadcast_attrs },
        .output_shape = broadcast_attrs.target_shape,
        .inputs = .{ indices_seed, target, null_node, null_node },
        .num_inputs = 2,
    });
    const out = try g.addNode(.{
        .op = .{ .gather = .{ .axis = 2, .elements = true } },
        .output_shape = Shape.init(.f32, &.{ -1, -1, -1 }),
        .inputs = .{ data, indices, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(out);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const data_values = [_]f32{
        10, 11, 12, 13, 14,
        20, 21, 22, 23, 24,
        30, 31, 32, 33, 34,
        40, 41, 42, 43, 44,
    };
    const index_values = [_]f32{
        4, 0, 2,
        1, 3, 0,
    };
    const data_ct = try cb_val.fromFloat32Shape(&data_values, &.{ 2, 2, 5 });
    defer cb_val.free(data_ct);
    const indices_ct = try cb_val.fromFloat32Shape(&index_values, &.{ 1, 2, 3 });
    defer cb_val.free(indices_ct);
    const target_ct = try cb_val.fromFloat32Shape(&.{ 2, 2, 3 }, &.{3});
    defer cb_val.free(target_ct);
    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = data, .value = data_ct },
        .{ .node_id = indices_seed, .value = indices_ct },
        .{ .node_id = target, .value = target_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{ .runtime_inputs = &rt_inputs });
    defer result.deinit(&cb_val);
    const actual_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(actual_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 2, 3 }, actual_shape);
    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ 14, 10, 12, 21, 23, 20, 34, 30, 32, 41, 43, 40 }, actual);
}

test "runtime shape drives dynamic integer resize broadcast values" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ -1, 1, -1, -1 }));
    const reshaped = try builder.reshape(x, Shape.init(.f32, &.{ -1, 1, 1, 1, -1, 1, -1, 1 }));

    var attrs = ml.graph.node.BroadcastAttrs{ .target_shape = Shape.init(.f32, &.{ -1, 1, 1, 1, -1, 2, -1, 2 }) };
    attrs.broadcast_axes = .{ 0, 1, 2, 3, 4, 5, 6, 7 };
    attrs.num_axes = 8;
    const broadcasted = try g.addNode(.{
        .op = .{ .broadcast_in_dim = attrs },
        .output_shape = attrs.target_shape,
        .inputs = .{ reshaped, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const out = try builder.reshape(broadcasted, Shape.init(.f32, &.{ -1, 1, -1, -1 }));
    try g.markOutput(out);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const input = [_]f32{
        1, 2,
        3, 4,
    };
    const x_ct = try cb_val.fromFloat32Shape(&input, &.{ 1, 1, 2, 2 });
    defer cb_val.free(x_ct);

    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = x, .value = x_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{
        .runtime_inputs = &rt_inputs,
    });
    defer result.deinit(&cb_val);

    const actual_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(actual_shape);
    try std.testing.expectEqualSlices(i64, &.{ 1, 1, 4, 4 }, actual_shape);

    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectEqualSlices(f32, &.{
        1, 1, 2, 2,
        1, 1, 2, 2,
        3, 3, 4, 4,
        3, 3, 4, 4,
    }, actual);
}

test "runtime shape drives symbolic scatter_add" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const dest = try builder.parameter("dest", Shape.init(.f32, &.{ -1, 2 }));
    const values = try builder.parameter("values", Shape.init(.f32, &.{ -1, 2 }));
    const indices = try builder.parameter("indices", Shape.init(.i64, &.{-1}));
    const out = try builder.scatterAdd(dest, values, indices, 0);
    try g.markOutput(out);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const dest_ct = try cb_val.fromFloat32Shape(&.{ 10, 20, 30, 40, 50, 60 }, &.{ 3, 2 });
    defer cb_val.free(dest_ct);
    const values_ct = try cb_val.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6 }, &.{ 3, 2 });
    defer cb_val.free(values_ct);
    const indices_ct = try cb_val.fromFloat32Shape(&.{ 0, 1, 0 }, &.{3});
    defer cb_val.free(indices_ct);

    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = dest, .value = dest_ct },
        .{ .node_id = values, .value = values_ct },
        .{ .node_id = indices, .value = indices_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{
        .runtime_inputs = &rt_inputs,
    });
    defer result.deinit(&cb_val);

    const actual_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(actual_shape);
    try std.testing.expectEqualSlices(i64, &.{ 3, 2 }, actual_shape);

    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ 16, 28, 33, 44, 50, 60 }, actual);
}

test "reshape restores batched flattened projection shape before gather" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var builder = ml.graph.Builder.init(&g);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ -1, -1, 6 }));
    const flattened = try builder.reshape(x, Shape.init(.f32, &.{ -1, 6 }));
    const weight = try builder.parameter("weight", Shape.init(.f32, &.{ 6, 6 }));
    const projected = try builder.matmul(flattened, weight);
    const attention = try builder.reshape(projected, Shape.init(.f32, &.{ -1, -1, 2, 3 }));
    const indices = try builder.parameter("indices", Shape.init(.i64, &.{2}));
    const gathered = try g.addNode(.{
        .op = .{ .gather = .{ .axis = 1 } },
        .output_shape = Shape.init(.f32, &.{ -1, 2, 2, 3 }),
        .inputs = .{ attention, indices, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(gathered);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    var input: [2 * 4 * 6]f32 = undefined;
    for (&input, 0..) |*value, i| value.* = @floatFromInt(i + 1);
    var identity: [6 * 6]f32 = .{0} ** (6 * 6);
    for (0..6) |i| identity[i * 6 + i] = 1.0;

    const x_ct = try cb_val.fromFloat32Shape(&input, &.{ 2, 4, 6 });
    defer cb_val.free(x_ct);
    const weight_ct = try cb_val.fromFloat32Shape(&identity, &.{ 6, 6 });
    defer cb_val.free(weight_ct);
    const indices_ct = try cb_val.fromFloat32Shape(&.{ 1, 3 }, &.{2});
    defer cb_val.free(indices_ct);

    const rt_inputs = [_]RuntimeInput{
        .{ .node_id = x, .value = x_ct },
        .{ .node_id = weight, .value = weight_ct },
        .{ .node_id = indices, .value = indices_ct },
    };

    var result = try execute(allocator, &g, &cb_val, .{
        .runtime_inputs = &rt_inputs,
    });
    defer result.deinit(&cb_val);

    const actual_shape = try cb_val.tensorShape(result.outputs[0], allocator);
    defer allocator.free(actual_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 2, 2, 3 }, actual_shape);

    const actual = try cb_val.toFloat32(result.outputs[0], allocator);
    defer allocator.free(actual);
    try std.testing.expectEqualSlices(f32, &.{
        7,  8,  9,
        10, 11, 12,
        19, 20, 21,
        22, 23, 24,
        31, 32, 33,
        34, 35, 36,
        43, 44, 45,
        46, 47, 48,
    }, actual);
}
