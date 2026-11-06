// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Complete, selected Native whole-model physical program. The original GPT
//! forward trace supplies semantics; existing Frame/BufferPlan owns every
//! mutable numerical allocation. No compatibility operation executes here.
const std = @import("std");
const ml = @import("ml");
const ops = @import("../ops/ops.zig");
const gpt = @import("../architectures/gpt.zig");
const Context = @import("../graph/execution.zig").Context;
const GraphCache = @import("../graph/cache.zig").GraphCache;
const Native = @import("../ops/native_compute.zig");
const Runtime = @import("../graph/runtime.zig").Runtime;
const Analysis = @import("../graph/interpreter.zig").CachedAnalysis;
const physical = @import("physical_buffers.zig");
const route = @import("kernel_route.zig");
const storage = @import("storage_admission.zig");
const prepared = @import("prepared_storage_admission.zig");
const Cleanup = @import("cleanup_context.zig").Context;
const Tensor = @import("../backends/tensor.zig").Tensor;
const CT = ops.CT;

/// Request-private owning projection. The fact alone is backend-free; the CT
/// must remain private to this actual receiving backend, including names whose
/// parameter nodes the optimizer eliminated.
const StaticStep = union(enum) { single: ml.graph.NodeId, pair: u32 };
const PairGroup = struct { first: ml.graph.NodeId, second: ml.graph.NodeId, activation: ml.graph.NodeId, weights: [2]ml.graph.NodeId };
const SourceQuery = struct { fact: Native.AuthoredSourceQuery, witness: ?CT = null, physical: ?Native.AuthoredQuantizedIdentity = null };

/// Selected original-forward semantics with a fully authored Native lowering.
/// Keep this independent of Metal's Qwen prefill selector. These two semantic
/// families share the plain gated dense decoder; their RoPE layouts, epsilon,
/// optional QK norms and weight names still come from the ORIGINAL trace.
fn eligible(config: gpt.Config) bool {
    switch (config.family) {
        .llama, .qwen3 => {},
        else => return false,
    }
    if (config.usesMoe() or config.num_local_experts != 0 or config.hasPle() or config.isMultimodal() or
        config.gemma4_mtp_assistant or config.qwen35_has_linear_attention) return false;
    if (config.num_hidden_layers == 0 or config.num_hidden_layers > 256 or config.hidden_size == 0 or
        config.num_attention_heads == 0 or config.intermediate_size == 0 or config.vocab_size == 0) return false;
    if (config.num_kv_shared_layers != 0 or config.global_head_dim != 0 or config.num_global_key_value_heads != 0 or
        config.sliding_window != 0 or config.shared_layer_intermediate_size != 0) return false;
    if (config.norm_type != .rms_norm or config.position_encoding != .rope or config.activation != .silu or
        config.norm_weight_offset != 0 or config.final_logit_softcapping != 0 or
        config.attention_k_eq_v or config.mrope_interleaved) return false;
    if (config.attention_head_dim == 0 and config.hidden_size % config.num_attention_heads != 0) return false;
    const head_dim = config.headDim();
    if (head_dim == 0 or head_dim % 2 != 0 or config.num_attention_heads % config.effectiveKVHeads() != 0) return false;
    if (!std.math.isFinite(config.norm_eps) or config.norm_eps <= 0 or
        !std.math.isFinite(config.rope_theta) or config.rope_theta <= 0 or
        !std.math.isFinite(config.rope_freq_scale) or config.rope_freq_scale <= 0 or
        !std.math.isFinite(config.rope_partial_factor) or config.rope_partial_factor <= 0 or config.rope_partial_factor > 1) return false;
    return true;
}

/// Definition-local immutable metadata. No CT, backend, Source, current-B,
/// physical allocation or mutable execution state is retained by this owner.
pub const Template = struct {
    allocator: std.mem.Allocator,
    references: std.atomic.Value(usize) = .init(1),
    artifact: @import("program.zig").ModelProgram,
    queries: []Native.AuthoredSourceQuery,
    descriptors: []?ops.kernel_descriptor.Descriptor,
    authored_descriptors: []?ops.kernel_descriptor.Descriptor,
    steps: []StaticStep,
    pairs: []PairGroup,
    input: ml.graph.NodeId,
    config_hash: u64,
    // Versioned by this executable; never a guessed Model generation.
    reduction_policy: enum { native_original_dense_v1, native_original_kblock_single_v1, native_original_kblock_pair_v1, native_original_resident_quantized_v2 } = .native_original_dense_v1,
    use_blas: bool,

    pub fn retain(self: *Template) *Template {
        std.debug.assert(self.references.fetchAdd(1, .acq_rel) != 0);
        return self;
    }
    pub fn release(self: *Template) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous != 0);
        if (previous != 1) return;
        const allocator = self.allocator;
        for (self.queries) |query| allocator.free(query.name);
        allocator.free(self.queries);
        allocator.free(self.descriptors);
        allocator.free(self.authored_descriptors);
        allocator.free(self.steps);
        allocator.free(self.pairs);
        self.artifact.deinit();
        allocator.destroy(self);
    }
    fn create(allocator: std.mem.Allocator, context: *const Context) !*Template {
        const backend = context.cb.withoutSynchronousBorrows();
        var cache = GraphCache.init(allocator);
        defer cache.deinitWithBackend(&backend);
        // This resolver wraps only the existing cold original-forward trace.
        // It refuses lazy/unprepared storage before any lookup preparation;
        // packed queries require the exact already-prepared resident owner.
        const Resolver = struct {
            original: *const Context,
            allocator: std.mem.Allocator,
            queries: std.ArrayList(SourceQuery) = .empty,
            failure: ?anyerror = null,
            fn note(recorder: *@This(), name: []const u8, answer: Native.AuthoredSourceQuery) !usize {
                const shape = answer.shape;
                for (recorder.queries.items, 0..) |query, index| if (std.mem.eql(u8, query.fact.name, name)) {
                    if ((query.fact.shape == null) != (shape == null) or (shape != null and !query.fact.shape.?.eq(shape.?)) or !std.meta.eql(query.fact.quantized, answer.quantized)) return error.AuthoredResidentTopologyChanged;
                    return index;
                };
                const owned_name = try recorder.allocator.dupe(u8, name);
                errdefer recorder.allocator.free(owned_name);
                try recorder.queries.append(recorder.allocator, .{ .fact = .{ .name = owned_name, .shape = shape, .quantized = answer.quantized } });
                return recorder.queries.items.len - 1;
            }
            fn get(raw: *anyopaque, name: []const u8) !CT {
                const value: *@This() = @ptrCast(@alignCast(raw));
                return value.getImpl(name) catch |err| {
                    // Preserve legitimate missing aliases. The latch also
                    // defends against incomplete query bookkeeping hidden by
                    // any future optional lookup in the original trace.
                    if (err != error.MissingWeight and err != error.WeightNotFound and value.failure == null) value.failure = err;
                    return err;
                };
            }
            fn getImpl(value: *@This(), name: []const u8) !CT {
                const answer = Native.NativeCompute.authoredResidentSourceQuery(value.original.cb, name) catch |err| switch (err) {
                    error.WeightNotFound => {
                        _ = try value.note(name, .{ .name = name, .shape = null });
                        return err; // preserve the original optional lookup behavior
                    },
                    else => return err,
                };
                const index = try value.note(name, answer);
                if (value.queries.items[index].witness == null) {
                    const witness = try value.original.graphWeight(name);
                    errdefer value.original.cb.free(witness);
                    if (answer.quantized == null) _ = try Native.NativeCompute.authoredResidentF32Plane(value.original.cb, name, witness);
                    if (answer.quantized != null) value.queries.items[index].physical = try Native.NativeCompute.authoredQuantizedIdentity(value.original.cb, name, witness);
                    value.queries.items[index].witness = witness;
                }
                return value.original.graphWeight(name);
            }
        };
        var resolver = Resolver{ .original = context, .allocator = allocator };
        defer {
            for (resolver.queries.items) |query| {
                if (query.witness) |witness| backend.free(witness);
                allocator.free(query.fact.name);
            }
            resolver.queries.deinit(allocator);
        }
        var tracing_context = context.*;
        tracing_context.cb = &backend;
        tracing_context.weights = .{ .ptr = &resolver, .get_fn = Resolver.get };
        var id = [_]i64{0};
        const trace_decode = gpt.DecodeContext{ .attention_mode = .paged_decode, .total_sequence_len = 1, .query_sequence_len = 1, .kv_sequence_len = 1 };
        const entry = try @import("../graph/execution.zig").ensureGraphEntry(&tracing_context, &cache, &id, 1, 1, &trace_decode);
        if (resolver.failure) |err| return err;
        const graph = &entry.graph;
        const pairs = try Program.collectQuantizedPairGroups(allocator, graph, resolver.queries.items);
        errdefer allocator.free(pairs);
        try Program.validateQuantizedUses(graph, resolver.queries.items, pairs);
        // Logical two-output IR retains existing Analysis/planning. Packed
        // pairs execute only through the recorded joint schedule below; neither
        // normalized single can be dispatched independently by Call.run.
        try Program.normalizeDensePairTrace(graph);
        try Program.normalizeDenseGroupedTrace(graph);
        var input: ?ml.graph.NodeId = null;
        for (graph.nodes.items, 0..) |*node, index| if (node.op == .fused_from_float32) {
            if (input != null or node.output_shape.dtype != .i64 or node.output_shape.numElements() != 1) return error.IncompleteNativeProgram;
            const name = try graph.internString("native.decoder.token");
            node.op = .{ .parameter = .{ .name_offset = name.offset, .name_len = name.len } };
            try graph.parameters.append(allocator, @intCast(index));
            input = @intCast(index);
        };
        const token_input = input orelse return error.IncompleteNativeProgram;
        if (graph.outputs.items.len != 1 or graph.node(graph.outputs.items[0]).output_shape.dtype != .f32 or graph.node(graph.outputs.items[0]).output_shape.numElements() != context.gpt_config.vocab_size) return error.IncompleteNativeProgram;
        try Native.NativeCompute.validateAuthoredSourceQueries(&backend, resolver.queries.items);
        const queries = try allocator.alloc(Native.AuthoredSourceQuery, resolver.queries.items.len);
        var query_count: usize = 0;
        errdefer {
            for (queries[0..query_count]) |query| allocator.free(query.name);
            allocator.free(queries);
        }
        for (resolver.queries.items, queries) |query, *fact| {
            fact.* = query.fact;
            fact.name = try allocator.dupe(u8, query.fact.name);
            query_count += 1;
        }
        const descriptors = try allocator.alloc(?ops.kernel_descriptor.Descriptor, graph.nodeCount());
        errdefer allocator.free(descriptors);
        const authored = try allocator.alloc(?ops.kernel_descriptor.Descriptor, graph.nodeCount());
        errdefer allocator.free(authored);
        for (descriptors, authored, 0..) |*descriptor, *extra, index| {
            descriptor.* = ops.kernel_descriptor.lower(graph, @intCast(index));
            extra.* = if (descriptor.* == null) try Program.authoredDescriptor(graph, @intCast(index), context.gpt_config.*) else null;
        }
        // ModelProgram takes ONLY normalized IR; no second optimization may
        // combine original independent GEMMs or change their reduction order.
        var artifact = try @import("program.zig").ModelProgram.takeOptimized(graph, token_input);
        errdefer artifact.deinit();
        var steps: std.ArrayList(ml.graph.NodeId) = .empty;
        defer steps.deinit(allocator);
        for (artifact.graph.nodes.items, 0..) |node, index| {
            if (!artifact.analysis.reachable[index] or node.op == .parameter) continue;
            for (node.getInputs()) |source| if (source >= index) return error.IncompleteNativeProgram;
            try steps.append(allocator, @intCast(index));
        }
        for (pairs) |pair| {
            if (!artifact.analysis.reachable[pair.first] or !artifact.analysis.reachable[pair.second]) return error.UnsupportedAuthoredWeight;
            var adjacent = false;
            for (steps.items, 0..) |step_node, i| if (step_node == pair.first) {
                adjacent = i + 1 < steps.items.len and steps.items[i + 1] == pair.second;
                break;
            };
            if (!adjacent) return error.UnsupportedAuthoredWeight;
        }
        // Resolve original group membership once, outside math and every
        // cache mutex. Dispatch performs one constant-time tagged step lookup.
        var schedule: std.ArrayList(StaticStep) = .empty;
        defer schedule.deinit(allocator);
        var position: usize = 0;
        while (position < steps.items.len) {
            const node = steps.items[position];
            var pair_index: ?u32 = null;
            for (pairs, 0..) |pair, i| if (pair.first == node) {
                pair_index = @intCast(i);
                break;
            };
            if (pair_index) |index| {
                try schedule.append(allocator, .{ .pair = index });
                position += 2; // Adjacency and both-output reachability proved above.
            } else {
                try schedule.append(allocator, .{ .single = node });
                position += 1;
            }
        }
        const ordered = try schedule.toOwnedSlice(allocator);
        errdefer allocator.free(ordered);
        var quantized_source = false;
        for (queries) |query| if (query.quantized != null) {
            quantized_source = true;
            break;
        };
        const self = try allocator.create(Template);
        var has_resident_quantized = false;
        for (queries) |query| if (query.quantized) |quantized| {
            if (quantized.kind == .Q8_0 or quantized.kind == .Q6_K) {
                has_resident_quantized = true;
                break;
            }
        };
        self.* = .{ .allocator = allocator, .artifact = artifact, .queries = queries, .descriptors = descriptors, .authored_descriptors = authored, .steps = ordered, .pairs = pairs, .input = token_input, .config_hash = context.gpt_config.fingerprint(), .reduction_policy = if (has_resident_quantized) .native_original_resident_quantized_v2 else if (pairs.len != 0) .native_original_kblock_pair_v1 else if (quantized_source) .native_original_kblock_single_v1 else .native_original_dense_v1, .use_blas = Native.useBlas() };
        return self;
    }
};

/// One immutable specialization held by the existing Definition owner. The
/// short metadata lock never encloses source lookup, allocation or retirement.
/// Runtime-private candidates remain valid when another runtime replaces it.
pub const TemplateSlot = struct {
    mutex: std.atomic.Mutex = .unlocked,
    value: ?*Template = null,
    stats: @import("specialization_cache.zig").Stats = .{},

    fn acquire(self: *TemplateSlot, context: *const Context) !?*Template {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        const candidate = if (self.value) |value| value.retain() else null;
        self.mutex.unlock();
        if (candidate) |value| {
            errdefer value.release();
            if (value.config_hash == context.gpt_config.fingerprint() and (value.reduction_policy == .native_original_dense_v1 or value.reduction_policy == .native_original_kblock_single_v1 or value.reduction_policy == .native_original_kblock_pair_v1 or value.reduction_policy == .native_original_resident_quantized_v2) and value.use_blas == Native.useBlas()) {
                Native.NativeCompute.validateAuthoredSourceQueries(context.cb, value.queries) catch |err| switch (err) {
                    error.AuthoredResidentTopologyChanged => {
                        value.release();
                        return null;
                    },
                    else => return err,
                };
                return value;
            }
            value.release();
        }
        return null;
    }
    fn publish(self: *TemplateSlot, value: *Template, reused: bool) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        const previous = self.value;
        self.value = value.retain();
        if (reused) self.stats.hits +%= 1 else {
            self.stats.misses +%= 1;
            self.stats.builds +%= 1;
            if (previous != null) self.stats.evictions +%= 1;
        }
        self.mutex.unlock();
        if (previous) |old| old.release();
    }
    /// Definition-lifetime successful constructor publications, not hits by a
    /// numerical invocation. Caller retains the actual slot owner; never waits.
    pub fn tryStatistics(self: *TemplateSlot) ?@import("specialization_cache.zig").Stats {
        if (!self.mutex.tryLock()) return null;
        defer self.mutex.unlock();
        return self.stats;
    }
    pub fn deinit(self: *TemplateSlot) void {
        if (self.value) |value| value.release();
        self.value = null;
    }
};

pub const Program = struct {
    allocator: std.mem.Allocator,
    backend: ops.ComputeBackend,
    config: gpt.Config,
    template: *Template,
    graph: *ml.graph.Graph,
    analysis: Analysis,
    runtime: Runtime,
    selected: *@import("prepared_routes.zig").PreparedRoutes,
    bindings: []route.InputBinding,
    weights: []?CT,
    weight_planes: []@import("storage_ownership.zig").Plane,
    source_queries: []SourceQuery,
    values: []?CT,
    steps: []const StaticStep,
    pairs: []const PairGroup,
    input: ml.graph.NodeId,
    mutex: std.atomic.Mutex = .unlocked,
    busy: bool = false,
    failed: bool = false,
    calls: u64 = 0,
    assigned_operations: u64 = 0,
    source_identity: ?struct { ptr: *anyopaque, table: *const @import("../backends/session.zig").Session.VTable },
    domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null,
    source_owner: ?*@import("../backends/session.zig").Session.ModelLifetime = null,

    pub const Statistics = struct { calls: u64, assigned_operations: u64, physical_frames: usize };
    /// Nonblocking values only. The caller retains this program/runtime; a
    /// numerical or cold preparation in progress makes the sample unavailable.
    pub fn statistics(self: *Program) ?Statistics {
        if (!self.mutex.tryLock()) return null;
        defer self.mutex.unlock();
        if (self.busy) return null;
        return .{ .calls = self.calls, .assigned_operations = self.assigned_operations, .physical_frames = self.selected.pool.frames.items.len };
    }

    /// No numerical or SDK callback. The actual backend owner must outlive this
    /// program, just as it outlives the existing GraphCache/executable bindings.
    pub fn tryCreate(allocator: std.mem.Allocator, context: *const Context) !?*Program {
        return tryCreateWithTemplate(allocator, context, null);
    }
    pub const Preparation = union(enum) { prepared: *Program, declined: ?anyerror };
    /// Typed actual constructor result. Optional adapters keep their existing
    /// successful fallback ABI; operational errors still propagate unchanged.
    pub fn tryCreateWithTemplate(allocator: std.mem.Allocator, context: *const Context, slot: ?*TemplateSlot) !?*Program {
        return switch (try prepareWithTemplate(allocator, context, slot)) {
            .prepared => |value| value,
            .declined => null,
        };
    }
    pub fn prepareWithTemplate(allocator: std.mem.Allocator, context: *const Context, slot: ?*TemplateSlot) !Preparation {
        if (comptime !@import("build_options").enable_native) return .{ .declined = null };
        if (context.cb.vtable != &Native.vtable_impl or context.weights != null or !eligible(context.gpt_config.*)) return .{ .declined = null };
        if (context.session) |source| if (!@import("architecture_session.zig").nativeProgramSourceMatches(source, context.cb)) return .{ .declined = null };
        const created = create(allocator, context, slot) catch |err| switch (err) {
            error.UnsupportedAuthoredWeight, error.UnsupportedAuthoredNativeTarget, error.IncompleteNativeProgram, error.UnsupportedNativeProgramWorkspace, error.MissingPreparedKernelRoute, error.UnsupportedShape, error.OwnedNativeIoTaskUnavailable => return .{ .declined = err },
            else => return err,
        };
        return .{ .prepared = created };
    }
    fn create(allocator: std.mem.Allocator, context: *const Context, template_slot: ?*TemplateSlot) !*Program {
        const self = try allocator.create(Program);
        errdefer allocator.destroy(self);
        const backend = context.cb.withoutSynchronousBorrows();
        self.allocator = allocator;
        self.backend = backend;
        self.config = context.gpt_config.*;
        const existing = if (template_slot) |owner| try owner.acquire(context) else null;
        const template = existing orelse try Template.create(allocator, context);
        errdefer template.release();
        self.template = template;
        self.graph = &template.artifact.graph;
        self.analysis = template.artifact.analysis;
        self.input = template.input;
        self.steps = template.steps;
        self.pairs = template.pairs;
        // Every receiving program owns fresh witnesses, including parameters
        // eliminated by graph optimization. Pure cached facts grant no payload
        // authority and no workspace/admission credit.
        self.source_queries = try allocator.alloc(SourceQuery, template.queries.len);
        @memset(self.source_queries, .{ .fact = .{ .name = "", .shape = null } });
        errdefer {
            for (self.source_queries) |query| if (query.witness) |value| self.backend.free(value);
            allocator.free(self.source_queries);
        }
        for (template.queries, self.source_queries) |fact, *query| {
            query.fact = fact;
            if (fact.shape != null) {
                const witness = try context.graphWeight(fact.name);
                query.witness = witness;
                if (fact.quantized == null) _ = try Native.NativeCompute.authoredResidentF32Plane(&self.backend, fact.name, witness) else query.physical = try Native.NativeCompute.authoredQuantizedIdentity(&self.backend, fact.name, witness);
            }
        }
        self.weights = try allocator.alloc(?CT, self.graph.nodeCount());
        @memset(self.weights, null);
        errdefer {
            for (self.weights) |maybe| if (maybe) |value| self.backend.free(value);
            allocator.free(self.weights);
        }
        self.weight_planes = try allocator.alloc(@import("storage_ownership.zig").Plane, self.graph.nodeCount());
        errdefer allocator.free(self.weight_planes);
        var bindings: std.ArrayList(route.InputBinding) = .empty;
        defer bindings.deinit(allocator);
        const descriptors = try allocator.alloc(?@import("../graph/backend_contracts.zig").TensorDesc, self.graph.nodeCount());
        defer allocator.free(descriptors);
        @memset(descriptors, null);
        for (self.graph.nodes.items, 0..) |node, index| {
            if (!self.analysis.reachable[index] or node.op != .parameter) continue;
            if (index == self.input) {
                continue;
            }
            const name = self.graph.parameterName(self.graph.node(@intCast(index)));
            const answer = try Native.NativeCompute.authoredResidentSourceQuery(&self.backend, name);
            const actual = answer.shape orelse return error.IncompleteNativeProgram;
            if (!actual.eq(node.output_shape)) return error.IncompleteNativeProgram;
            const value = try context.graphWeight(name);
            self.weights[index] = value;
            if (answer.quantized) |quantized_source| {
                const contracts = @import("../graph/backend_contracts.zig");
                const format = contracts.quantFormatFromGgufTensorType(.{ .known = quantized_source.kind }) orelse return error.UnsupportedAuthoredWeight;
                var packed_descriptor = contracts.TensorDesc.packedQuant(actual, format);
                packed_descriptor.current_backend = .native;
                packed_descriptor.resident_backend = .native;
                descriptors[index] = packed_descriptor;
                try bindings.append(allocator, .{ .node_id = @intCast(index), .value = .{ .tensor = .{ .value = value, .implementation = .{ .context = self.backend.ptr, .table = self.backend.vtable } } } });
            } else {
                self.weight_planes[index] = try Native.NativeCompute.authoredResidentF32Plane(&self.backend, name, value);
                try bindings.append(allocator, .{ .node_id = @intCast(index), .value = .{ .resident = .{ .logical = actual, .implementation = .{ .context = self.backend.ptr, .table = self.backend.vtable }, .planes = self.weight_planes[index..][0..1] } } });
            }
        }
        self.bindings = try allocator.alloc(route.InputBinding, 1);
        self.bindings[0] = .{ .node_id = self.input, .value = .{ .imported_host = .{ .logical = self.graph.node(self.input).output_shape, .target = .native, .owned_ingress = true } } };
        errdefer allocator.free(self.bindings);
        self.values = try allocator.dupe(?CT, self.weights);
        errdefer allocator.free(self.values);
        // Exact original selected worker policy before any physical route can
        // publish. This preserves null compatibility for generic consumers.
        if (template.reduction_policy == .native_original_resident_quantized_v2) for (self.graph.nodes.items, 0..) |node, index| {
            const descriptor = template.descriptors[index] orelse continue;
            if (descriptor.operation != .linear or node.num_inputs < 2) continue;
            if (self.weights[node.inputs[1]]) |weight| try Native.NativeCompute.validateResidentQuantizedTaskOwnership(&self.backend, weight, descriptor);
        };
        self.runtime = try Runtime.initWithOptions(allocator, self.graph, &self.backend, .partitioned, .{ .analysis = self.analysis, .region_costs = null, .kernel_descriptors = template.descriptors, .tensor_descs = descriptors });
        errdefer self.runtime.deinit();
        const pool = self.runtime.physical_pool orelse return error.IncompleteNativeProgram;
        if (pool.backends.len != 1 or pool.backends[0].ptr != self.backend.ptr or pool.backends[0].vtable != self.backend.vtable or pool.plan.transfers.len != 0) return error.IncompleteNativeProgram;
        pool.coverage_graph = self.graph;
        pool.coverage_reachable = self.analysis.reachable;
        pool.coverage_backend = self.backend;
        pool.native_frame_dispatch = true;
        for (self.graph.nodes.items, 0..) |node, index| {
            if (!self.analysis.reachable[index] or node.op == .parameter) continue;
            for (node.getInputs()) |source| if (source >= index) return error.IncompleteNativeProgram;
            const slot = pool.plan.slotForNode(@intCast(index)) orelse return error.IncompleteNativeProgram;
            if (pool.kernels[index] == null and node.op != .constant and node.op != .reshape) {
                const descriptor = template.authored_descriptors[index] orelse return error.IncompleteNativeProgram;
                var sources: [3]ml.graph.NodeId = @splat(ml.graph.null_node);
                @memcpy(sources[0..descriptor.input_count], node.getInputs());
                pool.kernels[index] = .{ .descriptor = descriptor, .inputs = sources, .backend_index = 0, .output_allocation = slot.allocation, .workspace_bytes = 0 };
            }
            if (node.op == .reshape and (slot.kind != .view or !std.meta.eql(slot.desc.strides, @import("../graph/backend_contracts.zig").TensorStrides.dense(node.output_shape)))) return error.IncompleteNativeProgram;
        }
        for (self.pairs) |pair| {
            const descriptor = template.descriptors[pair.first] orelse return error.IncompleteNativeProgram;
            if (!try Native.NativeCompute.authoredLinearPairSupported(&self.backend, self.weights[pair.weights[0]] orelse return error.MissingWeight, self.weights[pair.weights[1]] orelse return error.MissingWeight, &descriptor)) return error.UnsupportedAuthoredWeight;
            const first = pool.plan.slotForNode(pair.first) orelse return error.IncompleteNativeProgram;
            const second = pool.plan.slotForNode(pair.second) orelse return error.IncompleteNativeProgram;
            const activation = pool.plan.slotForNode(pair.activation) orelse return error.IncompleteNativeProgram;
            if (first.kind != .allocation or second.kind != .allocation or first.allocation == second.allocation or first.allocation == activation.allocation or second.allocation == activation.allocation) return error.UnsupportedAuthoredWeight;
        }
        try pool.configureRoutes(bindings.items);
        self.selected = (try pool.prepareRoutesWithPolicy(self.bindings, .compatibility_regions)) orelse return error.MissingPreparedKernelRoute;
        errdefer self.selected.release();
        if (self.selected.pool.payload_coverage != .complete or self.selected.pool.regions.len != 0) return error.IncompleteNativeProgram;
        _ = try self.selected.storageRequirement();
        try Native.NativeCompute.validateAuthoredSourceQueries(&self.backend, self.source_queries);
        self.mutex = .unlocked;
        self.busy = false;
        self.failed = false;
        self.calls = 0;
        self.assigned_operations = 0;
        self.source_identity = if (context.session) |source| .{ .ptr = source.ptr, .table = source.vtable } else null;
        self.domain = null;
        self.source_owner = if (context.session) |session| if (session.model_lifetime) |owner| owner.retain() else null else null;
        // No incomplete physical candidate or failed query certificate becomes
        // a cache hit. The metadata swap itself cannot allocate or call out.
        if (template_slot) |owner| owner.publish(template, existing != null);
        return self;
    }
    /// The selected resident F32 Native pair executes exactly two ordinary
    /// linearNoBias calls, first A then B. Give those same calls their original
    /// distinct values/assignments. Packed pairs retain their original joint
    /// schedule and may never dispatch as two independent scalar steps.
    /// Inspect the ORIGINAL trace, before paired/grouped F32 normalization.
    /// A packed leaf may be consumed only by a genuine original scalar linear
    /// or the exact original pair captured in the joint schedule.
    /// No pair can be split to manufacture eligibility or a different schedule.
    fn packedQuery(graph: *const ml.graph.Graph, queries: []const SourceQuery, id: ml.graph.NodeId) ?Native.AuthoredQuantizedFacts {
        const node = graph.node(id);
        if (node.op != .parameter) return null;
        const name = graph.parameterName(node);
        for (queries) |query| if (std.mem.eql(u8, query.fact.name, name)) return query.fact.quantized;
        return null;
    }
    fn collectQuantizedPairGroups(allocator: std.mem.Allocator, graph: *const ml.graph.Graph, queries: []const SourceQuery) ![]PairGroup {
        var groups: std.ArrayList(PairGroup) = .empty;
        errdefer groups.deinit(allocator);
        for (graph.nodes.items, 0..) |node, id| {
            if (node.op != .fused_linear_no_bias_pair or node.num_inputs != 3) continue;
            const a = packedQuery(graph, queries, node.inputs[1]);
            const b = packedQuery(graph, queries, node.inputs[2]);
            if (a == null and b == null) continue;
            if (a == null or b == null or a.?.kind != b.?.kind or node.op.fused_linear_no_bias_pair.rows != 1) return error.UnsupportedAuthoredWeight;
            if (a.?.kind == .Q6_K) return error.UnsupportedAuthoredWeight; // preserve original joint producer; no manufactured singles
            var second: ?ml.graph.NodeId = null;
            for (graph.nodes.items, 0..) |marker, marker_id| {
                if (marker.op != .fused_to_float32 or marker.num_inputs != 1 or marker.inputs[0] != id) continue;
                if (second != null or !marker.output_shape.eq(node.output_shape)) return error.UnsupportedAuthoredWeight;
                second = @intCast(marker_id);
            }
            try groups.append(allocator, .{ .first = @intCast(id), .second = second orelse return error.UnsupportedAuthoredWeight, .activation = node.inputs[0], .weights = .{ node.inputs[1], node.inputs[2] } });
        }
        return groups.toOwnedSlice(allocator);
    }
    fn validateQuantizedUses(graph: *const ml.graph.Graph, queries: []const SourceQuery, pairs: []const PairGroup) !void {
        for (graph.nodes.items, 0..) |node, index| {
            if (node.op != .parameter) continue;
            const name = graph.parameterName(&node);
            var quantized_source = false;
            for (queries) |query| if (std.mem.eql(u8, query.fact.name, name)) {
                quantized_source = query.fact.quantized != null;
                break;
            };
            if (!quantized_source) continue;
            var uses: usize = 0;
            for (graph.nodes.items) |consumer| for (consumer.getInputs(), 0..) |source, operand| {
                if (source != index) continue;
                if (consumer.op == .fused_embedding_lookup and operand == 0 and consumer.num_inputs == 2 and consumer.op.fused_embedding_lookup.total == 1) {
                    uses += 1;
                    continue;
                }
                if (consumer.op == .fused_linear_no_bias_pair and (operand == 1 or operand == 2)) {
                    var paired = false;
                    for (pairs) |pair| if (pair.weights[operand - 1] == source and pair.activation == consumer.inputs[0] and pair.weights[0] == consumer.inputs[1] and pair.weights[1] == consumer.inputs[2]) {
                        paired = true;
                        break;
                    };
                    if (!paired) return error.UnsupportedAuthoredWeight;
                    uses += 1;
                    continue;
                }
                if (consumer.op != .fused_linear_no_bias or consumer.num_inputs != 2 or operand != 1) return error.UnsupportedAuthoredWeight;
                const linear = consumer.op.fused_linear_no_bias;
                if (linear.rows != 1 or linear.num_projections != 0) return error.UnsupportedAuthoredWeight;
                uses += 1;
            };
            if (uses == 0) return error.UnsupportedAuthoredWeight;
        }
    }

    fn normalizeDensePairTrace(graph: *ml.graph.Graph) !void {
        for (graph.nodes.items) |*node| {
            if (node.op != .fused_to_float32 or node.num_inputs != 1) continue;
            const first = graph.node(node.inputs[0]);
            if (first.op != .fused_linear_no_bias_pair) continue;
            const pair = first.op.fused_linear_no_bias_pair;
            if (first.num_inputs != 3 or !node.output_shape.eq(first.output_shape)) return error.IncompleteNativeProgram;
            node.inputs = .{ first.inputs[0], first.inputs[2], ml.graph.null_node, ml.graph.null_node };
            node.num_inputs = 2;
            node.op = .{ .fused_linear_no_bias = .{ .rows = pair.rows, .in_dim = pair.in_dim, .out_dim = pair.out_dim } };
        }
        for (graph.nodes.items) |*node| {
            if (node.op != .fused_linear_no_bias_pair) continue;
            const pair = node.op.fused_linear_no_bias_pair;
            if (node.num_inputs != 3) return error.IncompleteNativeProgram;
            node.num_inputs = 2;
            node.inputs[2] = ml.graph.null_node;
            node.op = .{ .fused_linear_no_bias = .{ .rows = pair.rows, .in_dim = pair.in_dim, .out_dim = pair.out_dim } };
        }
    }

    /// Default graph fusion joins distinct named F32 projections into one
    /// wider GEMM. Native's original calls select GEMM geometry independently;
    /// preserve those exact calls before assigning storage or selecting kernels.
    fn projectionLeaf(graph: *const ml.graph.Graph, id: ml.graph.NodeId, start: i64, width: i64, input_width: u32) ?ml.graph.NodeId {
        const node = graph.node(id);
        if (node.output_shape.rank() != 2 or node.output_shape.dtype != .f32 or node.output_shape.dim(1) != input_width or start < 0 or width <= 0) return null;
        if (node.op == .parameter) return if (start == 0 and width == node.output_shape.dim(0)) id else null;
        if (node.op != .concat_prim or node.op.concat_prim.axis != 0 or node.num_inputs != 2) return null;
        const left = graph.node(node.inputs[0]).output_shape;
        if (left.rank() != 2) return null;
        if (start < left.dim(0)) {
            if (width > left.dim(0) - start) return null;
            return projectionLeaf(graph, node.inputs[0], start, width, input_width);
        }
        return projectionLeaf(graph, node.inputs[1], start - left.dim(0), width, input_width);
    }
    fn normalizeDenseGroupedTrace(graph: *ml.graph.Graph) !void {
        for (graph.nodes.items) |*node| {
            if (node.op != .slice or node.num_inputs != 1) continue;
            const slice = node.op.slice;
            const combined = graph.node(node.inputs[0]);
            if (combined.op != .fused_linear_no_bias or combined.num_inputs != 2) continue;
            const linear = combined.op.fused_linear_no_bias;
            if (linear.num_projections == 0) continue;
            if (slice.runtime_starts or slice.runtime_limits or slice.num_axes != 2 or slice.starts[0] != 0 or slice.limits[0] != linear.rows or slice.strides[0] != 1 or slice.strides[1] != 1) return error.IncompleteNativeProgram;
            const width = slice.limits[1] - slice.starts[1];
            const weight = projectionLeaf(graph, combined.inputs[1], slice.starts[1], width, linear.in_dim) orelse return error.IncompleteNativeProgram;
            if (!node.output_shape.eq(ml.graph.Shape.init(.f32, &.{ @intCast(linear.rows), width }))) return error.IncompleteNativeProgram;
            node.inputs = .{ combined.inputs[0], weight, ml.graph.null_node, ml.graph.null_node };
            node.num_inputs = 2;
            node.op = .{ .fused_linear_no_bias = .{ .rows = linear.rows, .in_dim = linear.in_dim, .out_dim = @intCast(width) } };
        }
    }

    fn authoredDescriptor(graph: *const ml.graph.Graph, id: ml.graph.NodeId, config: gpt.Config) !?ops.kernel_descriptor.Descriptor {
        const node = graph.node(id);
        if (node.op == .fused_embedding_lookup) {
            if (node.num_inputs != 2) return null;
            const attrs = node.op.fused_embedding_lookup;
            const weights = graph.node(node.inputs[0]).output_shape;
            const indices = graph.node(node.inputs[1]).output_shape;
            if (attrs.total != 1 or attrs.dim != config.hidden_size or weights.dtype != .f32 or weights.rank() != 2 or weights.dim(0) != config.vocab_size or weights.dim(1) != config.hidden_size or indices.dtype != .i64 or indices.numElements() != 1 or !node.output_shape.eq(ml.graph.Shape.init(.f32, &.{ 1, @intCast(config.hidden_size) }))) return null;
            const geometry = try @import("../ops/kernel_geometry.zig").GatherAxis.init(std.math.maxInt(u32), weights.dims[0..weights.rank()], 0);
            var descriptor = ops.kernel_descriptor.Descriptor{ .operation = .{ .gather = .{ .axis = 0, .geometry = geometry } }, .output_shape = node.output_shape, .input_count = 2, .input_shapes = @splat(ml.graph.Shape.init(.f32, &.{})) };
            descriptor.input_shapes[0] = weights;
            descriptor.input_shapes[1] = indices;
            return descriptor;
        }
        if (node.op != .fused_gqa_causal_attention or node.num_inputs != 3) return null;
        const attention = node.op.fused_gqa_causal_attention;
        if (attention.batch != 1 or attention.seq_len != 1 or attention.skip_kv_write or attention.num_heads != config.num_attention_heads or attention.num_kv_heads != config.effectiveKVHeads() or attention.head_dim != config.headDim() or attention.layer_index >= config.num_hidden_layers) return null;
        var descriptor = ops.kernel_descriptor.Descriptor{ .operation = .{ .paged_attention = .{ .num_heads = attention.num_heads, .num_kv_heads = attention.num_kv_heads, .head_dim = attention.head_dim, .layer_index = attention.layer_index } }, .output_shape = node.output_shape, .input_count = 3, .input_shapes = undefined };
        for (node.getInputs(), &descriptor.input_shapes) |source, *shape| shape.* = graph.node(source).output_shape;
        if (!descriptor.operation.paged_attention.valid(&descriptor)) return null;
        return descriptor;
    }
    pub fn supportsCall(self: *const Program, input_ids: []const i64, batch: usize, decode: *const gpt.DecodeContext) bool {
        // Request-time side effects/alternate position or packed-media state
        // are not represented by this cold scalar decoder trace. Decline
        // before a Frame or any numerical operation so the original caller
        // executes them once, with its original context.
        if (decode.selection_query_capture != null or decode.packed_query_lengths != null or
            decode.mrope_positions != null or decode.qwen3vl_visual_mask != null or
            decode.qwen3vl_deepstack_embeddings != null or decode.qwen3vl_deepstack_layer_count != 0) return false;
        return batch == 1 and input_ids.len == 1 and input_ids[0] >= 0 and input_ids[0] < self.config.vocab_size and decode.query_sequence_len == 1 and (decode.attention_mode == .paged_decode or decode.attention_mode == .paged_prefill);
    }
    pub fn beginCall(self: *Program, input_ids: []const i64, decode: *const gpt.DecodeContext, identity: ?storage.Identity) !Call {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        if (self.busy) {
            self.mutex.unlock();
            return error.QueueFull;
        }
        self.busy = true;
        self.mutex.unlock();
        errdefer self.endCall();
        if (self.failed) return error.NativeProgramFailed;
        if (!self.supportsCall(input_ids, 1, decode)) return error.UnsupportedAuthoredNativeCall;
        const attention = gpt.attentionContextFromDecode(decode);
        for (self.selected.pool.kernels) |maybe| if (maybe) |kernel| if (kernel.descriptor.operation == .paged_attention) try Native.NativeCompute.validateAuthoredAttention(&self.backend, kernel.descriptor.operation.paged_attention, &attention);
        return .{ .program = self, .token = input_ids[0], .total_tokens = decode.total_sequence_len, .sequence = attention.kv_cache.?.sequence_id, .pool = attention.kv_cache.?.pool_id, .manager = attention.kv_manager.?, .claim = if (identity) |value| try self.selected.claimWarm(value) else null };
    }
    fn endCall(self: *Program) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        self.busy = false;
        self.mutex.unlock();
    }
    /// Called only after the owning decoder has successfully reset its KV
    /// state. Failed numerical frames are destroyed, never numerically replayed.
    pub fn afterStateReset(self: *Program, cleanup: Cleanup) !void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        if (self.busy) {
            self.mutex.unlock();
            return error.QueueFull;
        }
        self.busy = true;
        self.mutex.unlock();
        defer self.endCall();
        if (!self.failed) return;
        _ = try self.selected.pool.reclaimRetiredFramesWithCleanup(cleanup);
        self.failed = false;
    }

    pub fn destroyWithCleanup(self: *Program, cleanup: Cleanup) void {
        std.debug.assert(!self.busy);
        // Explicit raw outside close constructs its actual local boundary;
        // managed banks never refund under an absent/currently inferred B.
        if (cleanup.boundary == null and cleanup.outside_execution) {
            var owner = @import("cleanup_owner.zig").Owner.beginOutsideCleanup() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            defer owner.finishAfterUnlock();
            var current = cleanup;
            current.boundary = owner.epoch();
            current.outside_execution = false;
            self.destroyWithCleanup(current);
            return;
        }
        self.selected.releaseWithCleanup(cleanup);
        self.runtime.deinitWithCleanup(cleanup);
        for (self.weights) |maybe| if (maybe) |value| self.backend.freeWithCleanup(value, cleanup);
        for (self.source_queries) |query| {
            if (query.witness) |witness| self.backend.freeWithCleanup(witness, cleanup);
        }
        self.allocator.free(self.source_queries);
        const allocator = self.allocator;
        const source = self.source_owner;
        const domain = self.domain;
        allocator.free(self.bindings);
        allocator.free(self.weights);
        allocator.free(self.weight_planes);
        allocator.free(self.values);
        self.template.release();
        allocator.destroy(self);
        if (source) |owner| owner.releaseWithCleanup(cleanup);
        if (domain) |pin| pin.release();
    }
};

/// Move-only synchronous cold call. No borrowed table slice or creation Epoch
/// survives its preparation window. Each numerical run receives actual NOW B.
pub const Call = struct {
    program: ?*Program,
    token: i64,
    total_tokens: usize,
    sequence: @import("../runtime/root.zig").kv.manager.SequenceId,
    pool: @import("../runtime/root.zig").kv.block.KvPoolId,
    manager: *@import("../runtime/root.zig").kv.manager.KvManager,
    claim: ?physical.FrameClaim = null,
    frame: ?*physical.Frame = null,
    output: ?[]f32 = null,
    output_allocator: ?std.mem.Allocator = null,
    ran: bool = false,
    completed: bool = false,
    construction: ?struct { record: usize, nonce: u64 } = null,
    fn validateBoundary(current: Cleanup) !void {
        if (current.boundary) |epoch| if (try epoch.isSealed()) return error.RetirementEpochSealed;
    }
    /// Caller owns the final-address Source through numerical unlock and
    /// output transfer. All additive declarations and metadata finish here,
    /// before the actual Source FIFO/Model/lane is entered.
    pub fn prepareSourceInto(self: *Call, source: *@import("../backends/session.zig").PreparedSource, session: @import("../backends/session.zig").Session, inputs: []const Tensor, allocator: std.mem.Allocator, control: ?@import("../execution_control.zig").InferenceExecutionControl) !void {
        const program = self.program orelse return error.NativeProgramCallConsumed;
        const identity = program.source_identity orelse return error.MissingNativeProgramSource;
        if (session.ptr != identity.ptr or session.vtable != identity.table or session.model_lifetime != program.source_owner) return error.NativeProgramSourceChanged;
        if (inputs.len != 1 or !std.mem.eql(u8, inputs[0].name, "input_ids") or inputs[0].dtype != .i64 or inputs[0].shape.len != 2 or inputs[0].shape[0] != 1 or inputs[0].shape[1] != 1 or inputs[0].data.len != 8 or std.mem.readInt(i64, inputs[0].data[0..8], @import("builtin").target.cpu.arch.endian()) != self.token) return error.UnsupportedAuthoredNativeCall;
        const policy = session.run_admission orelse return error.MissingRunAdmission;
        if (policy.backend_class != .cpu) return error.UnsupportedAuthoredNativeTarget;
        const model = program.source_owner orelse return error.MissingSourceModelLifetime;
        const pin = model.retainAdmissionDomain(policy.controller) orelse return error.ForeignAdmissionDomain;
        defer pin.release();
        if (program.domain) |old| {
            if (old.controller() != pin.controller()) return error.ForeignAdmissionDomain;
        } else program.domain = pin.retain();
        var builder = prepared.Builder{ .allocator = allocator };
        defer builder.deinit();
        try self.collectRequirements(&builder, .{ .controller = policy.controller, .backend_class = policy.backend_class });
        const request = try session.planRun(inputs, null);
        if (self.claim) |*claim| {
            var combined = (try policy.planRequest(request, session.outputInfo())).required;
            for (builder.items()) |item| {
                const amounts = if (item.backend == .native or item.class == .host_workspace)
                    @import("../runtime/tier/memory.zig").AdmissionAmounts{ .host_scratch_bytes = item.bytes }
                else
                    @import("../runtime/tier/memory.zig").AdmissionAmounts{ .backend_scratch_bytes = item.bytes };
                combined = try combined.merge(amounts);
            }
            try policy.validateOwnedRunAmounts(try combined.merge(try claim.ownedDemand(.{ .controller = policy.controller, .backend_class = policy.backend_class })));
        }
        try source.initDeclaredProgramShapesBeforeExecutionInto(session, session, null, .{ .tensors = inputs }, request, builder.items(), allocator, control);
        try source.bindInputsBeforeExecution(.{ .host = inputs }, .resident_transient);
        try self.prepareInto(allocator, try source.preparedProgramStorageBeforeExecution(), .{ .boundary = source.epoch() });
    }
    pub fn runWithSource(self: *Call, source: *@import("../backends/session.zig").PreparedSource, backend: *const ops.ComputeBackend, decode: *const gpt.DecodeContext, current: Cleanup) !void {
        try validateBoundary(current);
        const program = self.program orelse return error.NativeProgramCallConsumed;
        const identity = program.source_identity orelse return error.MissingNativeProgramSource;
        const b = source.epoch() orelse return error.MissingCurrentCleanupEpoch;
        const now = current.boundary orelse return error.MissingCurrentCleanupEpoch;
        if (b.record != now.record or b.nonce != now.nonce) return error.NativeProgramCleanupChanged;
        const permit = try source.beginExecutionDispatch();
        if (permit.session.ptr != identity.ptr or permit.session.vtable != identity.table or permit.session.model_lifetime != program.source_owner) return error.NativeProgramSourceChanged;
        var numerical = backend.*;
        numerical.execution_control = source.control;
        var binding = permit.session.invocation_diagnostics;
        if (binding) |*selected| {
            selected.* = selected.forBackend(numerical);
            selected.capture_programs = true;
        }
        try @import("invocation_observation.zig").callOperation(binding, .forward, Call.run, .{ self, &numerical, program.config, decode, current });
    }
    pub fn collectRequirements(self: *Call, builder: *prepared.Builder, identity: storage.Identity) !void {
        const program = self.program orelse return error.NativeProgramCallConsumed;
        try program.selected.pool.collectAdmissionRequirements(builder, if (self.claim) |*value| value else null, identity);
    }
    pub fn prepareInto(self: *Call, allocator: std.mem.Allocator, capability: ?storage.Capability, current: Cleanup) !void {
        try validateBoundary(current);
        const program = self.program orelse return error.NativeProgramCallConsumed;
        if (self.frame != null or self.output != null) return error.NativeProgramCallAlreadyPrepared;
        self.output = try allocator.alloc(f32, program.config.vocab_size);
        self.output_allocator = allocator;
        self.frame = (try program.selected.pool.acquirePrepared(capability, if (self.claim) |*value| value else null)) orelse return error.PreparedStorageUnavailable;
        self.claim = null;
        const frame = self.frame.?;
        if (frame.storage_admission) |cap| if (cap.constructionEpoch()) |epoch| {
            self.construction = .{ .record = @intFromPtr(epoch.record.?), .nonce = epoch.nonce };
        };
        try frame.begin();
        @memcpy(program.values, program.weights);
        var tensor = Tensor{ .data = std.mem.asBytes(&self.token), .dtype = .i64, .shape = &.{1}, .name = "native.decoder.token", .allocator = allocator, .owns_data = false, .owns_shape = false };
        const inputs = [_]@import("host_ingress.zig").Input{.{ .node_id = program.input, .value = &tensor }};
        const uploaded = try frame.bindHostInputs(&inputs);
        for (uploaded) |input| program.values[input.node_id] = input.value;
        try frame.prepareNativeProgramViews(&program.backend, program.graph, program.values, current);
        for (program.pairs) |pair| try frame.validateNativeLinearPair(&program.backend, program.values, pair.first, pair.second, current);
    }
    pub fn run(self: *Call, backend: *const ops.ComputeBackend, config: gpt.Config, decode: *const gpt.DecodeContext, current: Cleanup) !void {
        if (!@import("runtime_observation.zig").kernelEnabled()) return self.runCore(backend, config, decode, current);
        return self.runObserved(backend, config, decode, current);
    }
    fn runObserved(self: *Call, backend: *const ops.ComputeBackend, config: gpt.Config, decode: *const gpt.DecodeContext, current: Cleanup) !void {
        // Observation never pre-validates: runCore retains the exact original
        // boundary/program/frame error precedence, including sealed B.
        const maybe_program = self.program;
        const maybe_frame = self.frame;
        const already_ran = self.ran;
        const before = if (maybe_frame) |frame| frame.stats.dispatches else 0;
        const observation = @import("runtime_observation.zig");
        // Read only existing counters, only in the enabled wrapper and after
        // actual entry. No owner pointers escape and pre-entry errors do not
        // overwrite a preceding entered Call's sample.
        defer if (!already_ran and self.ran) {
            const frame = maybe_frame.?;
            observation.nativeProgramBank(.{
                .frame_allocations = frame.stats.allocations,
                .frame_requested_allocation_bytes = frame.stats.allocated_bytes,
                .frame_reuse_accesses = frame.stats.reused_allocations,
                .owned_ledger = if (frame.pool.accounting) |ledger| ledger.snapshot() else null,
            });
        };
        self.runCore(backend, config, decode, current) catch |err| {
            if (maybe_program) |program| observation.nativeProgram(.{ .identity = @intFromPtr(program), .entered = !already_ran and self.ran, .failure = err, .selected_steps = program.steps.len, .selected_pairs = program.pairs.len, .assigned_dispatches = if (maybe_frame) |frame| frame.stats.dispatches - before else 0 });
            return err;
        };
        const program = maybe_program.?;
        observation.nativeProgram(.{ .identity = @intFromPtr(program), .entered = true, .returned = true, .selected_steps = program.steps.len, .selected_pairs = program.pairs.len, .assigned_dispatches = maybe_frame.?.stats.dispatches - before });
    }
    fn runCore(self: *Call, backend: *const ops.ComputeBackend, config: gpt.Config, decode: *const gpt.DecodeContext, current: Cleanup) !void {
        try validateBoundary(current);
        const program = self.program orelse return error.NativeProgramCallConsumed;
        const frame = self.frame orelse return error.UnpreparedNativeProgramCall;
        const output = self.output orelse return error.UnpreparedNativeProgramCall;
        if (self.ran or backend.ptr != program.backend.ptr or backend.vtable != program.backend.vtable or config.fingerprint() != program.template.config_hash or !program.supportsCall(&.{self.token}, 1, decode)) return error.NativeProgramCallChanged;
        const attention = gpt.attentionContextFromDecode(decode);
        if (attention.kv_manager != self.manager or attention.kv_cache == null or attention.kv_cache.?.sequence_id != self.sequence or attention.kv_cache.?.pool_id != self.pool or attention.total_sequence_len != self.total_tokens) return error.NativeProgramCallChanged;
        try Native.NativeCompute.validateAuthoredSourceQueries(backend, program.source_queries);
        try program.selected.validate(program.bindings);
        for (program.selected.pool.kernels) |maybe| if (maybe) |kernel| if (kernel.descriptor.operation == .paged_attention) try Native.NativeCompute.validateAuthoredAttention(backend, kernel.descriptor.operation.paged_attention, &attention);
        if (frame.storage_admission != null and current.boundary == null) return error.MissingCurrentCleanupEpoch;
        if (current.operation != null) _ = try current.operationFor(backend);
        if (self.construction) |construction| {
            const now = current.boundary orelse return error.MissingCurrentCleanupEpoch;
            if (construction.record != @intFromPtr(now.record.?) or construction.nonce != now.nonce) return error.NativeProgramCleanupChanged;
        }
        // Revalidate the complete joint physical/resource selection under the
        // actual now-B before the first numerical operation, not just at its
        // later launch or in the earlier cold preparation window.
        for (program.pairs) |pair| try frame.validateNativeLinearPair(backend, program.values, pair.first, pair.second, current);
        self.ran = true;
        errdefer {
            program.failed = true;
            frame.retired = true;
        }
        const before = frame.stats.dispatches;
        for (program.steps) |step| {
            try backend.checkExecutionControl();
            switch (step) {
                .single => |node| program.values[node] = try frame.executeNativeProgram(backend, program.values, node, decode.total_sequence_len - 1, &attention),
                .pair => |index| {
                    const pair = program.pairs[index];
                    const result = try frame.executeNativeLinearPair(backend, program.values, pair.first, pair.second, current);
                    program.values[pair.first] = result[0];
                    program.values[pair.second] = result[1];
                },
            }
        }
        const result = program.values[program.graph.outputs.items[0]] orelse return error.MissingValue;
        // Existing direct F32 copy, no CT/Frame/backend loan escapes.
        if (!try backend.readFloat32Into(result, output)) return error.UnsupportedNativeProgramOutput;
        self.completed = true;
        program.calls += 1;
        program.assigned_operations += frame.stats.dispatches - before;
    }
    pub fn takeOutput(self: *Call) ![]f32 {
        if (!self.completed) return error.UnexecutedNativeProgramCall;
        const output = self.output orelse return error.NativeProgramOutputConsumed;
        self.output = null;
        return output;
    }
    pub fn close(self: *Call, current: Cleanup) void {
        const program = self.program orelse return;
        self.program = null;
        if (self.output) |output| self.output_allocator.?.free(output);
        self.output = null;
        if (self.claim) |*value| value.releaseWithCleanup(current);
        self.claim = null;
        if (self.frame) |frame| _ = frame.finishWithCleanup(current) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        self.frame = null;
        program.endCall();
    }
};

pub const Testing = if (@import("builtin").is_test) TestModel else struct {};
const TestModel = struct {
    data: Native.WeightStore,
    pub fn init(allocator: std.mem.Allocator) !TestModel {
        var result = TestModel{ .data = .{ .allocator = allocator, .resident_weights = .empty, .lazy_weights = .empty } };
        errdefer result.data.deinitOwned();
        const Spec = struct { name: []const u8, shape: []const i64, norm: bool = false };
        const specs = [_]Spec{
            .{ .name = "model.embed_tokens.weight", .shape = &.{ 16, 8 } },
            .{ .name = "model.norm.weight", .shape = &.{8}, .norm = true },
            .{ .name = "lm_head.weight", .shape = &.{ 16, 8 } },
            .{ .name = "model.layers.0.input_layernorm.weight", .shape = &.{8}, .norm = true },
            .{ .name = "model.layers.0.post_attention_layernorm.weight", .shape = &.{8}, .norm = true },
            .{ .name = "model.layers.0.self_attn.q_norm.weight", .shape = &.{4}, .norm = true },
            .{ .name = "model.layers.0.self_attn.k_norm.weight", .shape = &.{4}, .norm = true },
            .{ .name = "model.layers.0.self_attn.q_proj.weight", .shape = &.{ 8, 8 } },
            .{ .name = "model.layers.0.self_attn.k_proj.weight", .shape = &.{ 4, 8 } },
            .{ .name = "model.layers.0.self_attn.v_proj.weight", .shape = &.{ 4, 8 } },
            .{ .name = "model.layers.0.self_attn.o_proj.weight", .shape = &.{ 8, 8 } },
            .{ .name = "model.layers.0.mlp.gate_proj.weight", .shape = &.{ 16, 8 } },
            .{ .name = "model.layers.0.mlp.up_proj.weight", .shape = &.{ 16, 8 } },
            .{ .name = "model.layers.0.mlp.down_proj.weight", .shape = &.{ 8, 16 } },
        };
        for (specs, 0..) |spec, seed| {
            const shape = ml.graph.Shape.init(.f32, spec.shape);
            const values = try allocator.alloc(f32, @intCast(shape.numElements().?));
            defer allocator.free(values);
            for (values, 0..) |*value, index| value.* = if (spec.norm) 1.0 else @as(f32, @floatFromInt(@as(i32, @intCast((index * 7 + seed * 3) % 29)) - 14)) / 41;
            const name = try allocator.dupe(u8, spec.name);
            errdefer allocator.free(name);
            var tensor = try Tensor.initFloat32(allocator, name, spec.shape, values);
            errdefer tensor.deinit();
            try result.data.resident_weights.put(allocator, name, .{ .tensor = tensor });
        }
        return result;
    }
    pub fn initWithoutQkNorms(allocator: std.mem.Allocator) !TestModel {
        var result = try init(allocator);
        for ([_][]const u8{ "model.layers.0.self_attn.q_norm.weight", "model.layers.0.self_attn.k_norm.weight" }) |name| {
            const removed = result.data.resident_weights.fetchRemove(name).?;
            allocator.free(removed.key);
            var weight = removed.value;
            weight.deinit();
        }
        return result;
    }
    pub fn expanded(allocator: std.mem.Allocator, cfg: gpt.Config) !TestModel {
        var template = try init(allocator);
        defer template.data.deinitOwned();
        var result = TestModel{ .data = .{ .allocator = allocator, .resident_weights = .empty, .lazy_weights = .empty } };
        errdefer result.data.deinitOwned();
        for (0..cfg.num_hidden_layers) |layer| {
            var entries = template.data.resident_weights.iterator();
            while (entries.next()) |entry| {
                const base = entry.key_ptr.*;
                if (cfg.family == .llama and (std.mem.endsWith(u8, base, "q_norm.weight") or std.mem.endsWith(u8, base, "k_norm.weight"))) continue;
                if (cfg.weight_tying and std.mem.eql(u8, base, "lm_head.weight")) continue;
                const per_layer = std.mem.startsWith(u8, base, "model.layers.0.");
                if (!per_layer and layer != 0) continue;
                const key = if (per_layer) try std.fmt.allocPrint(allocator, "model.layers.{d}.{s}", .{ layer, base["model.layers.0.".len..] }) else try allocator.dupe(u8, base);
                errdefer allocator.free(key);
                var dims: [2]i64 = undefined;
                const rank = entry.value_ptr.tensor.shape.len;
                if (rank == 1) {
                    dims[0] = if (std.mem.endsWith(u8, base, "q_norm.weight") or std.mem.endsWith(u8, base, "k_norm.weight")) cfg.headDim() else cfg.hidden_size;
                } else if (std.mem.eql(u8, base, "model.embed_tokens.weight") or std.mem.eql(u8, base, "lm_head.weight")) {
                    dims = .{ cfg.vocab_size, cfg.hidden_size };
                } else if (std.mem.endsWith(u8, base, "q_proj.weight")) {
                    dims = .{ cfg.num_attention_heads * cfg.headDim(), cfg.hidden_size };
                } else if (std.mem.endsWith(u8, base, "k_proj.weight") or std.mem.endsWith(u8, base, "v_proj.weight")) {
                    dims = .{ cfg.effectiveKVHeads() * cfg.headDim(), cfg.hidden_size };
                } else if (std.mem.endsWith(u8, base, "o_proj.weight")) {
                    dims = .{ cfg.hidden_size, cfg.num_attention_heads * cfg.headDim() };
                } else if (std.mem.endsWith(u8, base, "down_proj.weight")) {
                    dims = .{ cfg.hidden_size, cfg.intermediate_size };
                } else {
                    dims = .{ cfg.intermediate_size, cfg.hidden_size };
                }
                const shape = ml.graph.Shape.init(.f32, dims[0..rank]);
                const values = try allocator.alloc(f32, @intCast(shape.numElements().?));
                defer allocator.free(values);
                for (values, 0..) |*value, index| value.* = if (rank == 1) 1 else @as(f32, @floatFromInt(@as(i32, @intCast((index * 7 + key.len * 3 + layer) % 29)) - 14)) / 41;
                var tensor = try Tensor.initFloat32(allocator, key, dims[0..rank], values);
                errdefer tensor.deinit();
                try result.data.resident_weights.put(allocator, key, .{ .tensor = tensor });
            }
        }
        return result;
    }
    pub fn parsedLlamaConfig(allocator: std.mem.Allocator, hidden: u32, layers: u32, tied: bool) !gpt.Config {
        const json = try std.fmt.allocPrint(allocator, "{{\"model_type\":\"llama\",\"hidden_size\":{d},\"intermediate_size\":{d},\"vocab_size\":128,\"num_hidden_layers\":{d},\"num_attention_heads\":8,\"num_key_value_heads\":2,\"head_dim\":32,\"max_position_embeddings\":64,\"tie_word_embeddings\":{s}}}", .{ hidden, hidden * 2, layers, if (tied) "true" else "false" });
        defer allocator.free(json);
        const cfg = try @import("../models/gpt.zig").parseConfig(allocator, json);
        try std.testing.expectEqual(@import("../models/gpt.zig").ModelFamily.llama, cfg.family);
        try std.testing.expectEqual(@import("../models/gpt.zig").RopeLayout.consecutive_pairs, cfg.rope_layout);
        try std.testing.expectEqual(@as(f32, 1e-5), cfg.norm_eps);
        try std.testing.expect(!gpt.denseQwen3PrefillEligible(cfg));
        return cfg;
    }
    /// Require the actual cold preparation result without repeating it.
    pub fn requirePackedProgram(allocator: std.mem.Allocator, context: *const Context) !*Program {
        return switch (try Program.prepareWithTemplate(allocator, context, null)) {
            .prepared => |selected| selected,
            .declined => |reason| return reason orelse error.MissingAuthoredNativeProgram,
        };
    }
    pub fn quantizeSingles(self: *TestModel, a: std.mem.Allocator, cfg: gpt.Config, include_pair: bool) !void {
        return quantizeAuthoredSingleProjections(a, self, cfg, include_pair);
    }
    pub fn quantizeProjections(self: *TestModel, a: std.mem.Allocator) !void {
        return quantizeAuthoredProjections(a, self);
    }
    pub const MatrixFixture = enum { q4q5, q8, mixed, incompatible_pair, q6_singles, q6_rows, q6_mixed, q6_pair_unsupported };
    pub fn quantizeResidentFixture(self: *TestModel, a: std.mem.Allocator, mode: MatrixFixture) !void {
        if (mode == .q4q5) return self.quantizeAllMatrices(a);
        if (mode == .q6_singles or mode == .q6_rows or mode == .q6_mixed or mode == .q6_pair_unsupported) return self.quantizeQ6Fixture(a, mode);
        var it = self.data.resident_weights.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.tensor.shape.len != 2) continue;
            const name = entry.key_ptr.*;
            const kind: @import("../gguf/tensor_types.zig").KnownTensorType = if (mode == .incompatible_pair and std.mem.endsWith(u8, name, ".up_proj.weight")) .Q4_K else if (mode == .mixed and (std.mem.endsWith(u8, name, ".k_proj.weight") or std.mem.endsWith(u8, name, ".v_proj.weight") or std.mem.endsWith(u8, name, ".o_proj.weight"))) .Q4_K else if (mode == .mixed and std.mem.eql(u8, name, "lm_head.weight")) .Q5_K else .Q8_0;
            try quantizeAuthoredProjection(a, entry.value_ptr, name, kind);
        }
    }
    fn quantizeQ6Fixture(self: *TestModel, a: std.mem.Allocator, mode: MatrixFixture) !void {
        var it = self.data.resident_weights.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.tensor.shape.len != 2) continue;
            const name = entry.key_ptr.*;
            const selected = q6SingleFixtureName(name) or ((mode == .q6_rows or mode == .q6_mixed) and std.mem.eql(u8, name, "model.embed_tokens.weight"));
            if (!selected and mode != .q6_mixed and mode != .q6_pair_unsupported) continue;
            const kind: @import("../gguf/tensor_types.zig").KnownTensorType = if (selected or mode == .q6_pair_unsupported) .Q6_K else if (std.mem.indexOf(u8, name, ".mlp.") != null) .Q5_K else .Q4_K;
            try quantizeAuthoredProjection(a, entry.value_ptr, name, kind);
        }
    }
    pub fn parsedQ8Config(a: std.mem.Allocator, family: enum { llama, qwen3 }, tied: bool, layers: u32) !gpt.Config {
        const json = try std.fmt.allocPrint(a, "{{\"model_type\":\"{s}\",\"hidden_size\":256,\"intermediate_size\":512,\"vocab_size\":128,\"num_hidden_layers\":{d},\"num_attention_heads\":8,\"num_key_value_heads\":2,\"head_dim\":64,\"max_position_embeddings\":64,\"tie_word_embeddings\":{s},\"rms_norm_eps\":0.00001,\"rope_theta\":500000}}", .{ @tagName(family), layers, if (tied) "true" else "false" });
        defer a.free(json);
        const cfg = try @import("../models/gpt.zig").parseConfig(a, json);
        try std.testing.expectEqual(@as(u32, 64), cfg.headDim());
        try std.testing.expectEqualStrings(@tagName(family), @tagName(cfg.family));
        return cfg;
    }
    pub fn parsedQ6ActivationConfig(a: std.mem.Allocator) !gpt.Config {
        return @import("../models/gpt.zig").parseConfig(a, "{\"model_type\":\"llama\",\"hidden_size\":768,\"intermediate_size\":1536,\"vocab_size\":512,\"num_hidden_layers\":1,\"num_attention_heads\":12,\"num_key_value_heads\":2,\"head_dim\":64,\"max_position_embeddings\":64,\"tie_word_embeddings\":false}");
    }
    pub fn quantizeAllMatrices(self: *TestModel, a: std.mem.Allocator) !void {
        try quantizeAuthoredProjections(a, self);
        const name = "model.embed_tokens.weight";
        try quantizeAuthoredProjection(a, self.data.resident_weights.getPtr(name).?, self.data.resident_weights.getKey(name).?, .Q4_K);
    }
    pub fn prepareSingles(backend: *const ops.ComputeBackend, data: *Native.WeightStore) !void {
        return prepareAuthoredSingleProjections(backend, data);
    }
    pub fn parsedLlamaPackedConfig(allocator: std.mem.Allocator, layers: u32) !gpt.Config {
        return parsedLlamaPackedConfigWithTying(allocator, layers, false);
    }
    pub fn parsedLlamaPackedConfigWithTying(allocator: std.mem.Allocator, layers: u32, tied: bool) !gpt.Config {
        const json = try std.fmt.allocPrint(allocator, "{{\"model_type\":\"llama\",\"hidden_size\":512,\"intermediate_size\":1536,\"vocab_size\":128,\"num_hidden_layers\":{d},\"num_attention_heads\":8,\"num_key_value_heads\":2,\"head_dim\":64,\"max_position_embeddings\":64,\"tie_word_embeddings\":{s}}}", .{ layers, if (tied) "true" else "false" });
        defer allocator.free(json);
        const cfg = try @import("../models/gpt.zig").parseConfig(allocator, json);
        try std.testing.expectEqual(@import("../models/gpt.zig").ModelFamily.llama, cfg.family);
        try std.testing.expectEqual(@as(u32, 64), cfg.headDim());
        return cfg;
    }
    pub fn config() gpt.Config {
        return .{ .family = .qwen3, .hidden_size = 8, .vocab_size = 16, .num_hidden_layers = 1, .num_attention_heads = 2, .num_key_value_heads = 1, .intermediate_size = 16, .norm_type = .rms_norm, .norm_eps = 1e-6, .position_encoding = .rope, .activation = .silu, .rope_theta = 10000 };
    }
    fn decode(manager: *@import("../runtime/root.zig").kv.manager.KvManager, pool: @import("../runtime/root.zig").kv.block.KvPoolId, sequence: @import("../runtime/root.zig").kv.manager.SequenceId, count: usize) gpt.DecodeContext {
        const table = manager.blockTable(sequence).?;
        return .{ .attention_mode = if (count == 1) .paged_prefill else .paged_decode, .total_sequence_len = count, .query_sequence_len = 1, .kv_sequence_len = count, .kv_manager = manager, .kv_cache = .{ .sequence_id = sequence, .pool_id = pool, .logical_block_count = table.len(), .tail_tokens = table.tail_tokens, .logical_blocks = table.blocks.items } };
    }
};

test "native authored complete F32 plain decoder cold warm reset bitwise and assigned allocation reuse" {
    try nativeAuthoredDecodeCase(TestModel.config());
}

test "native authored complete wider two-layer F32 query-width geometry preserves original bitwise kernels" {
    var cfg = TestModel.config();
    cfg.hidden_size = 64;
    cfg.intermediate_size = 128;
    cfg.vocab_size = 64;
    cfg.num_hidden_layers = 2;
    cfg.num_attention_heads = 4;
    cfg.attention_head_dim = 32;
    try nativeAuthoredDecodeCase(cfg);
}

fn nativeAuthoredDecodeCase(config: gpt.Config) !void {
    return nativeAuthoredDecodeSteps(config, 6, 4);
}

fn nativeAuthoredDecodeSteps(config: gpt.Config, steps: usize, reset_step: usize) !void {
    return nativeAuthoredDecodeStorageSteps(config, steps, reset_step, false);
}
fn nativeAuthoredDecodeStorageSteps(config: gpt.Config, steps: usize, reset_step: usize, packed_singles: bool) !void {
    return nativeAuthoredDecodeProjectionSteps(config, steps, reset_step, packed_singles, false);
}
fn nativeAuthoredDecodeProjectionSteps(config: gpt.Config, steps: usize, reset_step: usize, packed_singles: bool, packed_pairs: bool) !void {
    return nativeAuthoredDecodeMatrixSteps(config, steps, reset_step, packed_singles, packed_pairs, false);
}
fn nativeAuthoredDecodeMatrixSteps(config: gpt.Config, steps: usize, reset_step: usize, packed_singles: bool, packed_pairs: bool, packed_rows: bool) !void {
    return nativeAuthoredDecodeFormatSteps(config, steps, reset_step, packed_singles, packed_pairs, packed_rows, .q4q5);
}
fn nativeAuthoredDecodeFormatSteps(config: gpt.Config, steps: usize, reset_step: usize, packed_singles: bool, packed_pairs: bool, packed_rows: bool, mode: TestModel.MatrixFixture) !void {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    var model = if (config.fingerprint() == TestModel.config().fingerprint()) try TestModel.init(a) else try TestModel.expanded(a, config);
    defer model.data.deinitOwned();
    if (mode != .q4q5) try model.quantizeResidentFixture(a, mode) else if (packed_rows) try model.quantizeAllMatrices(a) else if (packed_pairs) try quantizeAuthoredProjections(a, &model) else if (packed_singles) try quantizeAuthoredSingleProjections(a, &model, config, false);
    var compute = Native.NativeCompute.init(a, &model.data, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    if (packed_singles) try prepareAuthoredSingleProjections(&backend, &model.data);
    const context = Context{ .allocator = a, .cb = &backend, .gpt_config = &config };
    try std.testing.expect(eligible(config));
    const program = if (packed_singles) try TestModel.requirePackedProgram(a, &context) else (try Program.tryCreate(a, &context)) orelse return error.MissingAuthoredNativeProgram;
    defer program.destroyWithCleanup(.{ .outside_execution = true });
    const q6_mode = mode == .q6_singles or mode == .q6_rows or mode == .q6_mixed;
    var expected_assigned_dispatches: u64 = 0;
    var selected_q6_singles: usize = 0;
    for (program.steps) |step| switch (step) {
        .single => |node| if (program.selected.pool.kernels[node]) |kernel| {
            expected_assigned_dispatches += 1;
            if (q6_mode and kernel.descriptor.operation == .linear) {
                const fact = try Native.NativeCompute.authoredResidentSourceQuery(&backend, program.graph.parameterName(program.graph.node(kernel.inputs[1])));
                if (fact.quantized) |quantized| {
                    if (quantized.kind == .Q6_K) selected_q6_singles += 1;
                }
            }
        },
        .pair => |index| {
            expected_assigned_dispatches += 1;
            if (q6_mode) for (program.pairs[index].weights) |weight_node| {
                const fact = try Native.NativeCompute.authoredResidentSourceQuery(&backend, program.graph.parameterName(program.graph.node(weight_node)));
                if (fact.quantized) |quantized| try std.testing.expect(quantized.kind != .Q6_K);
            };
        },
    };
    if (q6_mode) {
        // Original o/down singles plus the untied head (or packed tied row)
        // are Q6; original gate/up pair remains dense or Q4/Q5 as authored.
        const q6_head = !config.weight_tying or mode == .q6_rows or mode == .q6_mixed;
        try std.testing.expectEqual(@as(usize, config.num_hidden_layers) * 2 + @intFromBool(q6_head), selected_q6_singles);
    }

    if (packed_singles and config.hidden_size == 512) {
        var pack_kernels: usize = 0;
        for (program.selected.pool.kernels) |kernel| if (kernel) |selected| {
            if (selected.host_workspace_bytes != 0) pack_kernels += 1;
        };
        try std.testing.expect(pack_kernels != 0);
    }
    if ((mode == .q6_singles or mode == .q6_rows) and config.hidden_size == 768) {
        var activation_routes: usize = 0;
        for (program.selected.pool.kernels) |kernel| if (kernel) |selected| {
            if (selected.host_workspace_bytes != 0) activation_routes += 1;
        };
        try std.testing.expect(activation_routes >= 3); // real o/down/head original Q6 activation selectors
    }
    if (packed_pairs) {
        // Actual plain Llama has no prepared K/V slots: original attention uses
        // separate K and V projections. Its gated FFN makes ONE original
        // gate/up pair per layer. Prove exact source topology, not just >0.
        try std.testing.expectEqual(@as(usize, config.num_hidden_layers), program.pairs.len);
        for (program.pairs, 0..) |pair, layer| {
            var gate_name: [128]u8 = undefined;
            var up_name: [128]u8 = undefined;
            try std.testing.expectEqualStrings(try std.fmt.bufPrint(&gate_name, "model.layers.{d}.mlp.gate_proj.weight", .{layer}), program.graph.parameterName(program.graph.node(pair.weights[0])));
            try std.testing.expectEqualStrings(try std.fmt.bufPrint(&up_name, "model.layers.{d}.mlp.up_proj.weight", .{layer}), program.graph.parameterName(program.graph.node(pair.weights[1])));
            const geometry = program.selected.pool.kernels[pair.first].?.descriptor.operation.linear;
            try std.testing.expect(geometry.rows == 1 and geometry.in_dim == config.hidden_size and geometry.out_dim == config.intermediate_size);
        }
    }
    if (packed_rows) {
        var matrix_queries: usize = 0;
        for (program.source_queries) |query| if (query.fact.shape) |shape| {
            if (shape.rank() == 2) {
                if (mode == .q6_rows and !q6SingleFixtureName(query.fact.name) and !std.mem.eql(u8, query.fact.name, "model.embed_tokens.weight")) {
                    try std.testing.expect(query.fact.quantized == null); // original dense pair remains dense
                } else try std.testing.expect(query.fact.quantized != null);
                if (mode == .q6_rows and query.fact.quantized != null) try std.testing.expectEqual(@import("../gguf/tensor_types.zig").KnownTensorType.Q6_K, query.fact.quantized.?.kind);
                if (mode == .q8) try std.testing.expectEqual(@import("../gguf/tensor_types.zig").KnownTensorType.Q8_0, query.fact.quantized.?.kind);
                matrix_queries += 1;
            }
        };
        try std.testing.expect(matrix_queries != 0);
        var packed_gathers: usize = 0;
        for (program.selected.pool.kernels) |kernel| if (kernel) |selected| {
            if (selected.descriptor.operation == .gather) {
                const weight = program.weights[selected.inputs[0]] orelse return error.MissingWeight;
                try std.testing.expect(Native.nativeTensorHasQuantizedStorage(weight));
                packed_gathers += 1;
            }
        };
        try std.testing.expectEqual(@as(usize, 1), packed_gathers);
    }
    const Kv = @import("../runtime/root.zig").kv;
    var reference = Kv.manager.KvManager.init(a);
    defer reference.deinit();
    var selected = Kv.manager.KvManager.init(a);
    defer selected.deinit();
    const plan = Kv.pool.KvPoolConfig{ .backend = .native, .dtype = .f32, .page_size_tokens = 4, .num_layers_packed = @intCast(config.num_hidden_layers), .num_kv_heads = @intCast(config.effectiveKVHeads()), .head_dim = @intCast(config.headDim()) };
    const reference_pool = try reference.addPool(plan);
    const selected_pool = try selected.addPool(plan);
    var reference_sequence = try reference.attachSequence(reference_pool);
    var selected_sequence = try selected.attachSequence(selected_pool);
    var first_storage: ?*ops.physical_storage.Lease = null;
    // Nested observation only when the caller already enabled diagnostics;
    // the existing disabled fixture keeps its original no-observer path.
    var bank_scope = @import("runtime_observation.zig").KernelScope{};
    if (@import("runtime_observation.zig").kernelEnabled()) bank_scope.begin();
    defer {
        if (bank_scope.attached) _ = bank_scope.finish();
    }
    for (0..steps) |step| {
        if (step == reset_step) {
            try reference.releaseSequence(reference_sequence);
            try selected.releaseSequence(selected_sequence);
            reference_sequence = try reference.attachSequence(reference_pool);
            selected_sequence = try selected.attachSequence(selected_pool);
        }
        const count = if (step < reset_step) step + 1 else step - reset_step + 1;
        try reference.appendTokens(reference_sequence, 1);
        try selected.appendTokens(selected_sequence, 1);
        const ids = [_]i64{@intCast(step + 1)};
        const baseline_context = TestModel.decode(&reference, reference_pool, reference_sequence, count);
        const actual_context = TestModel.decode(&selected, selected_pool, selected_sequence, count);
        const expected = try gpt.forward(&backend, a, config, &ids, 1, count, &baseline_context);
        defer a.free(expected);
        var call = try program.beginCall(&ids, &actual_context, null);
        defer call.close(.{ .outside_execution = true });
        try call.prepareInto(a, null, .{ .outside_execution = true });
        if (step == 0) {
            var owner = try @import("cleanup_owner.zig").Owner.beginOutsideCleanup();
            var sealed = try owner.epoch().?.retain();
            defer sealed.release();
            owner.finishAfterUnlock();
            try std.testing.expectError(error.RetirementEpochSealed, call.run(&backend, config, &actual_context, .{ .boundary = &sealed }));
            try std.testing.expect(!call.ran);
            try std.testing.expectEqual(@as(u64, 0), program.assigned_operations);
            var changed_config = config;
            changed_config.rope_theta *= 2;
            try std.testing.expectError(error.NativeProgramCallChanged, call.run(&backend, changed_config, &actual_context, .{ .outside_execution = true }));
            try std.testing.expect(!call.ran);
            try std.testing.expectEqual(@as(u64, 0), program.assigned_operations);
            // Include trace-resolved names even if optimization consumed their
            // parameter nodes. A changed payload is refused before dispatch.
            const source = &model.data.resident_weights.getPtr(if (packed_pairs) "model.norm.weight" else "model.layers.0.self_attn.q_proj.weight").?.tensor;
            const previous = source.data;
            const replacement = try a.dupe(u8, previous);
            source.data = replacement;
            defer {
                source.data = previous;
                a.free(replacement);
            }
            try std.testing.expectError(error.AuthoredResidentWeightChanged, call.run(&backend, config, &actual_context, .{ .outside_execution = true }));
            try std.testing.expect(!call.ran);
            try std.testing.expectEqual(@as(u64, 0), program.assigned_operations);
        }
        if (packed_singles and step == 0) {
            // The actual Manager/LRU primitive has no resident-weight victim.
            // Both raw bytes and prepared entries belong to the retained model,
            // not an independently evictable lazy/cache entry.
            try std.testing.expectEqual(@as(usize, 0), Native.reclaimOneHostCacheEntry(&model.data));
            const selected_storage = &model.data.resident_weights.getPtr("model.layers.0.self_attn.o_proj.weight").?.quantized_storage.?;
            const index = @backingInt(@import("../models/weight_source.zig").QuantizedStorage.PreparedQuantLayout.row_major_blocks);
            const old = selected_storage.prepared.entries[index].?;
            const replacement = try a.dupe(u8, old.bytes);
            selected_storage.prepared.entries[index].?.bytes = replacement;
            defer {
                selected_storage.prepared.entries[index] = old;
                a.free(replacement);
            }
            // Same topology/lengths and identical numeric bytes cannot hide a
            // changed selected physical representation before any KV writes.
            try std.testing.expectError(error.AuthoredResidentWeightChanged, call.run(&backend, config, &actual_context, .{ .outside_execution = true }));
            try std.testing.expect(!call.ran);
            try std.testing.expectEqual(@as(u64, 0), program.assigned_operations);
        }
        const original_compute_allocator = compute.allocator;
        const original_pool_allocator = program.selected.pool.allocator;
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        compute.allocator = failing.allocator();
        program.selected.pool.allocator = failing.allocator();
        defer {
            compute.allocator = original_compute_allocator;
            program.selected.pool.allocator = original_pool_allocator;
        }
        const assigned_before = call.frame.?.stats.dispatches;
        var q6_activation_before: u64 = 0;
        if (bank_scope.attached and q6_mode and config.hidden_size == 768) {
            for (bank_scope.native_dispatches.records[0..bank_scope.native_dispatches.count]) |record| {
                if (record.phase == .successful_return and std.mem.eql(u8, record.family, "q6_k_q8k_activation")) q6_activation_before += record.events;
            }
        }
        const quant_before = Native.nativeQuantDispatchStats();
        try call.run(&backend, config, &actual_context, .{ .outside_execution = true });
        if (bank_scope.attached) {
            const bank = bank_scope.native_program_bank orelse return error.MissingNativeProgramBank;
            try std.testing.expectEqual(call.frame.?.stats.allocations, bank.frame_allocations);
            try std.testing.expectEqual(call.frame.?.stats.allocated_bytes, bank.frame_requested_allocation_bytes);
            try std.testing.expectEqual(call.frame.?.stats.reused_allocations, bank.frame_reuse_accesses);
            try std.testing.expectEqualDeep(call.frame.?.pool.accounting.?.snapshot(), bank.owned_ledger.?);
            if (step == 0) {
                try std.testing.expect(bank.frame_allocations > 0 and bank.frame_requested_allocation_bytes > 0);
            } else try std.testing.expect(bank.frame_reuse_accesses > 0);
        }
        if (packed_singles and config.hidden_size == 512 and (mode == .q4q5 or mode == .mixed or mode == .q6_mixed)) {
            const quant_after = Native.nativeQuantDispatchStats();
            try std.testing.expect(quant_after.q4_q5_k_q8k_activation > quant_before.q4_q5_k_q8k_activation);
            if (packed_pairs) try std.testing.expectEqual(@as(u64, config.num_hidden_layers), quant_after.q4_q5_k_q8k_activation_pair - quant_before.q4_q5_k_q8k_activation_pair);
        }
        if (q6_mode) {
            // Q6's prepared-row path has no NativeQuantDispatchStats Q8
            // counter. Every selected Q6 single is in this complete schedule;
            // actual Frame successful-return counts prove it executed once.
            try std.testing.expectEqual(expected_assigned_dispatches, call.frame.?.stats.dispatches - assigned_before);
            const quant_after = Native.nativeQuantDispatchStats();
            try std.testing.expectEqual(quant_before.q8_0_direct, quant_after.q8_0_direct);
            try std.testing.expectEqual(quant_before.q8_0_pair, quant_after.q8_0_pair);
            if (bank_scope.attached and config.hidden_size == 768) {
                var q6_activation_after: u64 = 0;
                for (bank_scope.native_dispatches.records[0..bank_scope.native_dispatches.count]) |record| {
                    if (record.phase == .successful_return and std.mem.eql(u8, record.family, "q6_k_q8k_activation")) q6_activation_after += record.events;
                }
                try std.testing.expectEqual(@as(u64, selected_q6_singles), q6_activation_after - q6_activation_before);
            }
        }
        if (mode == .q8 or mode == .mixed) {
            const quant_after = Native.nativeQuantDispatchStats();
            try std.testing.expect(quant_after.q8_0_direct > quant_before.q8_0_direct);
            try std.testing.expectEqual(@as(u64, config.num_hidden_layers), quant_after.q8_0_pair - quant_before.q8_0_pair);
            var actual_pack_routes: usize = 0;
            for (program.selected.pool.kernels) |kernel| if (kernel) |owned_kernel| {
                if (owned_kernel.host_workspace_bytes > 0) actual_pack_routes += 1;
            };
            try std.testing.expect(actual_pack_routes > 0);
        }
        compute.allocator = original_compute_allocator;
        program.selected.pool.allocator = original_pool_allocator;
        const actual = try call.takeOutput();
        defer a.free(actual);
        try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(expected), std.mem.sliceAsBytes(actual));
        try std.testing.expect(call.frame.?.stats.dispatches > 0 and program.assigned_operations > 0);
        const allocation = program.selected.pool.kernels[program.graph.outputs.items[0]].?.output_allocation;
        const owner = call.frame.?.storages[allocation].?;
        if (first_storage) |previous| try std.testing.expect(previous == owner) else first_storage = owner;
    }
    try std.testing.expectEqual(@as(u64, @intCast(steps)), program.calls);
    try std.testing.expectEqual(@as(usize, 1), program.selected.pool.frames.items.len);
}

test "native authored complete program metadata and assigned banks unwind all allocation failures" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            return runCase(a, false, false);
        }
        fn missing(a: std.mem.Allocator) !void {
            return runCase(a, true, false);
        }
        fn missingHead(a: std.mem.Allocator) !void {
            return runCase(a, true, true);
        }
        fn runCase(a: std.mem.Allocator, omit_optional: bool, omit_head: bool) !void {
            var model = if (omit_optional) try TestModel.initWithoutQkNorms(a) else try TestModel.init(a);
            defer model.data.deinitOwned();
            if (omit_head) {
                const removed = model.data.resident_weights.fetchRemove("lm_head.weight").?;
                var weight = removed.value;
                weight.deinit();
                a.free(removed.key);
            }
            var compute = Native.NativeCompute.init(a, &model.data, null);
            defer compute.deinit();
            const cb = compute.computeBackend();
            const cfg = TestModel.config();
            const context = Context{ .allocator = a, .cb = &cb, .gpt_config = &cfg };
            const program = (try Program.tryCreate(a, &context)) orelse return error.MissingAuthoredNativeProgram;
            defer program.destroyWithCleanup(.{ .outside_execution = true });
            if (omit_head) {
                var saw_missing_head = false;
                for (program.source_queries) |query| if (std.mem.eql(u8, query.fact.name, "lm_head.weight")) {
                    try std.testing.expect(query.fact.shape == null and query.witness == null);
                    saw_missing_head = true;
                };
                try std.testing.expect(saw_missing_head);
            }
            var manager = @import("../runtime/root.zig").kv.manager.KvManager.init(a);
            defer manager.deinit();
            const pool = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 4, .num_kv_heads = 1, .head_dim = 4 });
            const sequence = try manager.attachSequence(pool);
            try manager.appendTokens(sequence, 1);
            const decode = TestModel.decode(&manager, pool, sequence, 1);
            var call = try program.beginCall(&.{2}, &decode, null);
            defer call.close(.{ .outside_execution = true });
            try call.prepareInto(a, null, .{ .outside_execution = true });
            try call.run(&cb, cfg, &decode, .{ .outside_execution = true });
            const values = try call.takeOutput();
            defer a.free(values);
            try std.testing.expectEqual(@as(usize, cfg.vocab_size), values.len);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.missing, .{});
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.missingHead, .{});
}

test "native authored post-entry cancellation poisons without replay and real KV reset restores execution" {
    try nativeAuthoredCancellationCase(TestModel.config());
}

fn nativeAuthoredCancellationCase(cfg: gpt.Config) !void {
    return nativeAuthoredCancellationStorageCase(cfg, false);
}
fn nativeAuthoredCancellationStorageCase(cfg: gpt.Config, packed_singles: bool) !void {
    return nativeAuthoredCancellationProjectionCase(cfg, packed_singles, false);
}
fn nativeAuthoredCancellationProjectionCase(cfg: gpt.Config, packed_singles: bool, packed_pairs: bool) !void {
    return nativeAuthoredCancellationMatrixCase(cfg, packed_singles, packed_pairs, false);
}
fn nativeAuthoredCancellationMatrixCase(cfg: gpt.Config, packed_singles: bool, packed_pairs: bool, packed_rows: bool) !void {
    return nativeAuthoredCancellationFormatCase(cfg, packed_singles, packed_pairs, packed_rows, .q4q5);
}
fn nativeAuthoredCancellationFormatCase(cfg: gpt.Config, packed_singles: bool, packed_pairs: bool, packed_rows: bool, mode: TestModel.MatrixFixture) !void {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    var model = if (cfg.fingerprint() == TestModel.config().fingerprint()) try TestModel.init(a) else try TestModel.expanded(a, cfg);
    defer model.data.deinitOwned();
    if (mode != .q4q5) try model.quantizeResidentFixture(a, mode) else if (packed_rows) try model.quantizeAllMatrices(a) else if (packed_pairs) try quantizeAuthoredProjections(a, &model) else if (packed_singles) try quantizeAuthoredSingleProjections(a, &model, cfg, false);
    var compute = Native.NativeCompute.init(a, &model.data, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    if (packed_singles) try prepareAuthoredSingleProjections(&backend, &model.data);
    const context = Context{ .allocator = a, .cb = &backend, .gpt_config = &cfg };
    const program = if (packed_singles) try TestModel.requirePackedProgram(a, &context) else (try Program.tryCreate(a, &context)) orelse return error.MissingAuthoredNativeProgram;
    defer program.destroyWithCleanup(.{ .outside_execution = true });
    const Kv = @import("../runtime/root.zig").kv;
    var manager = Kv.manager.KvManager.init(a);
    defer manager.deinit();
    const pool = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 4, .num_kv_heads = @intCast(cfg.effectiveKVHeads()), .head_dim = @intCast(cfg.headDim()), .num_layers_packed = @intCast(cfg.num_hidden_layers) });
    var sequence = try manager.attachSequence(pool);
    try manager.appendTokens(sequence, 1);
    var decode = TestModel.decode(&manager, pool, sequence, 1);
    var call = try program.beginCall(&.{2}, &decode, null);
    defer call.close(.{ .outside_execution = true });
    try call.prepareInto(a, null, .{ .outside_execution = true });
    const Cancel = struct {
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks == 3) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    var controlled = backend;
    controlled.execution_control = .{ .ptr = &cancel, .check_fn = Cancel.check, .native_call_policy = .in_process };
    try std.testing.expectError(error.Cancelled, call.run(&controlled, cfg, &decode, .{ .outside_execution = true }));
    try std.testing.expect(call.ran and !call.completed and program.failed and call.frame.?.retired);
    try std.testing.expect(call.frame.?.stats.dispatches > 0);
    try std.testing.expectEqual(@as(u64, 0), program.calls);
    try std.testing.expectError(error.UnexecutedNativeProgramCall, call.takeOutput());
    const checks = cancel.checks;
    call.close(.{ .outside_execution = true });
    try std.testing.expectError(error.NativeProgramFailed, program.beginCall(&.{2}, &decode, null));
    try std.testing.expectEqual(checks, cancel.checks);
    // Match the installed caller: actual sequence retirement/reset completes
    // before the program may reclaim its poisoned Frame and accept new math.
    try manager.releaseSequence(sequence);
    sequence = try manager.attachSequence(pool);
    try manager.appendTokens(sequence, 1);
    decode = TestModel.decode(&manager, pool, sequence, 1);
    try program.afterStateReset(.{ .outside_execution = true });
    var fresh = try program.beginCall(&.{2}, &decode, null);
    defer fresh.close(.{ .outside_execution = true });
    try fresh.prepareInto(a, null, .{ .outside_execution = true });
    try fresh.run(&backend, cfg, &decode, .{ .outside_execution = true });
    const actual = try fresh.takeOutput();
    defer a.free(actual);
    const reference_sequence = try manager.attachSequence(pool);
    try manager.appendTokens(reference_sequence, 1);
    const reference_decode = TestModel.decode(&manager, pool, reference_sequence, 1);
    const expected = try gpt.forward(&backend, a, cfg, &.{2}, 1, 1, &reference_decode);
    defer a.free(expected);
    try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(expected), std.mem.sliceAsBytes(actual));
    try std.testing.expectEqual(@as(u64, 1), program.calls);
}

test "native authored larger layered asymmetric F32 shapes cross physical pages bitwise" {
    for ([_]u32{ 128, 256 }) |hidden| {
        var cfg = TestModel.config();
        cfg.hidden_size = hidden;
        cfg.intermediate_size = hidden * 2;
        cfg.vocab_size = 128;
        cfg.num_hidden_layers = if (hidden == 128) 2 else 4;
        cfg.num_attention_heads = 8;
        cfg.num_key_value_heads = 2;
        cfg.attention_head_dim = 32;
        try nativeAuthoredDecodeSteps(cfg, 8, 6);
    }
}

test "native authored BF16 host paged KV and unsupported family or quantized weights decline before dispatch" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    var model = try TestModel.init(a);
    defer model.data.deinitOwned();
    var compute = Native.NativeCompute.init(a, &model.data, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    var cfg = TestModel.config();
    var context = Context{ .allocator = a, .cb = &backend, .gpt_config = &cfg };
    cfg.family = .qwen3_5;
    try std.testing.expectEqual(@as(?*Program, null), try Program.tryCreate(a, &context));
    cfg = TestModel.config();
    const source = model.data.resident_weights.getPtr("model.embed_tokens.weight").?;
    source.quantized = true;
    try std.testing.expectEqual(@as(?*Program, null), try Program.tryCreate(a, &context));
    source.quantized = false;
    const program = (try Program.tryCreate(a, &context)) orelse return error.MissingAuthoredNativeProgram;
    defer program.destroyWithCleanup(.{ .outside_execution = true });
    const Kv = @import("../runtime/root.zig").kv;
    var manager = Kv.manager.KvManager.init(a);
    defer manager.deinit();
    const pool = try manager.addPool(.{ .backend = .native, .dtype = .bf16, .page_size_tokens = 4, .num_kv_heads = 1, .head_dim = 4 });
    const sequence = try manager.attachSequence(pool);
    try manager.appendTokens(sequence, 1);
    const decode = TestModel.decode(&manager, pool, sequence, 1);
    try std.testing.expectError(error.UnsupportedAuthoredPagedAttention, program.beginCall(&.{2}, &decode, null));
    try std.testing.expect(!program.busy and program.calls == 0 and program.assigned_operations == 0);
    // The original host pool also explicitly refuses BF16 writes; admitting
    // it here would advertise a dtype that cannot execute the original path.
    try std.testing.expectError(error.UnsupportedKvDType, manager.writeLayerKvSuffix(sequence, 0, 1, 1, &.{ 1, 2, 3, 4 }, &.{ 5, 6, 7, 8 }));
}

test "native authored parsed Llama original RoPE and no QK norms layered page crossing reset parity" {
    for ([_]u32{ 128, 256 }) |hidden| {
        const cfg = try TestModel.parsedLlamaConfig(std.testing.allocator, hidden, if (hidden == 128) 2 else 4, hidden == 256);
        try nativeAuthoredDecodeSteps(cfg, 8, 6);
    }
}

test "native authored parsed Llama post entry cancellation requires real reset without replay" {
    try nativeAuthoredCancellationCase(try TestModel.parsedLlamaConfig(std.testing.allocator, 128, 2, false));
}

test "native authored structural eligibility refuses incompatible semantics before native context access" {
    const a = std.testing.allocator;
    const cfg = try TestModel.parsedLlamaConfig(a, 128, 2, false);
    try std.testing.expect(eligible(cfg));
    inline for (.{ "num_attention_heads", "hidden_size", "num_hidden_layers", "intermediate_size", "vocab_size" }) |field| {
        var changed = cfg;
        @field(changed, field) = 0;
        const invalid = ops.ComputeBackend{ .ptr = @ptrFromInt(8), .vtable = &Native.vtable_impl };
        const ctx = Context{ .allocator = a, .cb = &invalid, .gpt_config = &changed };
        try std.testing.expectEqual(@as(?*Program, null), try Program.tryCreate(a, &ctx));
    }
    var changed = cfg;
    inline for (.{ "num_local_experts", "ple_hidden_size", "num_kv_shared_layers", "global_head_dim", "num_global_key_value_heads", "sliding_window", "shared_layer_intermediate_size" }) |field| {
        changed = cfg;
        @field(changed, field) = 1;
        try std.testing.expect(!eligible(changed));
    }
    inline for (.{ "gemma4_mtp_assistant", "qwen35_has_linear_attention", "attention_k_eq_v", "mrope_interleaved" }) |field| {
        changed = cfg;
        @field(changed, field) = true;
        try std.testing.expect(!eligible(changed));
    }
    inline for (.{ "norm_eps", "rope_theta", "rope_freq_scale", "rope_partial_factor" }) |field| {
        changed = cfg;
        @field(changed, field) = std.math.nan(f32);
        try std.testing.expect(!eligible(changed));
    }
    changed = cfg;
    changed.family = .qwen3_5;
    try std.testing.expect(!eligible(changed));
    changed = cfg;
    changed.norm_type = .layer_norm;
    try std.testing.expect(!eligible(changed));
    changed = cfg;
    changed.activation = .gelu;
    try std.testing.expect(!eligible(changed));
    changed = cfg;
    changed.position_encoding = .absolute;
    try std.testing.expect(!eligible(changed));
}

test "native authored parsed Llama quantized sources and BF16 KV retain original precommit refusal" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const cfg = try TestModel.parsedLlamaConfig(a, 128, 2, false);
    var model = try TestModel.expanded(a, cfg);
    defer model.data.deinitOwned();
    try std.testing.expect(!model.data.resident_weights.contains("model.layers.0.self_attn.q_norm.weight"));
    try std.testing.expect(!model.data.resident_weights.contains("model.layers.0.self_attn.k_norm.weight"));
    var compute = Native.NativeCompute.init(a, &model.data, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    const context = Context{ .allocator = a, .cb = &backend, .gpt_config = &cfg };
    const weight = model.data.resident_weights.getPtr("model.embed_tokens.weight").?;
    weight.quantized = true;
    try std.testing.expectEqual(@as(?*Program, null), try Program.tryCreate(a, &context));
    weight.quantized = false;
    const program = (try Program.tryCreate(a, &context)) orelse return error.MissingAuthoredNativeProgram;
    defer program.destroyWithCleanup(.{ .outside_execution = true });
    const Kv = @import("../runtime/root.zig").kv;
    var manager = Kv.manager.KvManager.init(a);
    defer manager.deinit();
    const pool = try manager.addPool(.{ .backend = .native, .dtype = .bf16, .page_size_tokens = 4, .num_kv_heads = @intCast(cfg.effectiveKVHeads()), .head_dim = @intCast(cfg.headDim()), .num_layers_packed = @intCast(cfg.num_hidden_layers) });
    const sequence = try manager.attachSequence(pool);
    try manager.appendTokens(sequence, 1);
    const decode = TestModel.decode(&manager, pool, sequence, 1);
    try std.testing.expectError(error.UnsupportedAuthoredPagedAttention, program.beginCall(&.{2}, &decode, null));
    try std.testing.expect(!program.busy and program.calls == 0 and program.assigned_operations == 0);
}

test "native authored parsed Llama effectful decode falls back before math and preserves actual capture once" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const cfg = try TestModel.parsedLlamaConfig(a, 128, 2, false);
    var model = try TestModel.expanded(a, cfg);
    defer model.data.deinitOwned();
    var compute = Native.NativeCompute.init(a, &model.data, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    const context = Context{ .allocator = a, .cb = &backend, .gpt_config = &cfg };
    const program = (try Program.tryCreate(a, &context)) orelse return error.MissingAuthoredNativeProgram;
    defer program.destroyWithCleanup(.{ .outside_execution = true });
    const Kv = @import("../runtime/root.zig").kv;
    var manager = Kv.manager.KvManager.init(a);
    defer manager.deinit();
    const pool = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 4, .num_kv_heads = @intCast(cfg.effectiveKVHeads()), .head_dim = @intCast(cfg.headDim()), .num_layers_packed = @intCast(cfg.num_hidden_layers) });
    const reference_sequence = try manager.attachSequence(pool);
    const selected_sequence = try manager.attachSequence(pool);
    try manager.appendTokens(reference_sequence, 1);
    try manager.appendTokens(selected_sequence, 1);
    const Capture = struct {
        calls: usize = 0,
        fn capture(raw: *anyopaque, _: usize, _: usize, _: usize, _: usize, _: usize, values: []const f32) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            for (values) |value| try std.testing.expect(std.math.isFinite(value));
        }
    };
    var capture = Capture{};
    var reference = TestModel.decode(&manager, pool, reference_sequence, 1);
    reference.selection_query_capture = .{ .ctx = &capture, .allocator = a, .horizon = 1, .captureFn = Capture.capture };
    const expected = try gpt.forward(&backend, a, cfg, &.{2}, 1, 1, &reference);
    defer a.free(expected);
    try std.testing.expectEqual(@as(usize, cfg.num_hidden_layers), capture.calls);
    capture.calls = 0;
    var selected = TestModel.decode(&manager, pool, selected_sequence, 1);
    selected.selection_query_capture = reference.selection_query_capture;
    try std.testing.expect(!program.supportsCall(&.{2}, 1, &selected));
    try std.testing.expectError(error.UnsupportedAuthoredNativeCall, program.beginCall(&.{2}, &selected, null));
    try std.testing.expect(!program.busy);
    var decoder = @import("decoder_forward.zig").Runtime{ .context = context, .native_program = program };
    const actual = try decoder.forwardLastLogits(&.{2}, 1, 1, &selected);
    defer a.free(actual);
    try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(expected), std.mem.sliceAsBytes(actual));
    try std.testing.expectEqual(@as(usize, cfg.num_hidden_layers), capture.calls);
    try std.testing.expectEqual(@as(u64, 0), program.calls);
    try std.testing.expectEqual(@as(u64, 0), program.assigned_operations);
}

test "native authored full query metadata checks missing answers and actual dtype before canonical casts" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    var model = try TestModel.initWithoutQkNorms(a);
    defer model.data.deinitOwned();
    var compute = Native.NativeCompute.init(a, &model.data, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    const name = "model.embed_tokens.weight";
    const shape = try Native.NativeCompute.authoredResidentF32Shape(&backend, name);
    const queries = [_]Native.AuthoredSourceQuery{
        .{ .name = name, .shape = shape },
        .{ .name = "model.layers.0.self_attn.q_norm.weight", .shape = null },
        .{ .name = "model.layers.0.self_attn.k_norm.weight", .shape = null },
    };
    try Native.NativeCompute.validateAuthoredSourceQueries(&backend, &queries);
    const inherited = Native.vtable_impl;
    const fake = ops.ComputeBackend{ .ptr = @ptrFromInt(8), .vtable = &inherited };
    try std.testing.expectError(error.UnsupportedAuthoredNativeTarget, Native.NativeCompute.validateAuthoredSourceQueries(&fake, &queries));
    const tensor = &model.data.resident_weights.getPtr(name).?.tensor;
    tensor.dtype = .bf16;
    try std.testing.expectError(error.AuthoredResidentTopologyChanged, Native.NativeCompute.validateAuthoredSourceQueries(&backend, &queries));
    tensor.dtype = .f32;
    try Native.NativeCompute.validateAuthoredSourceQueries(&backend, &queries);
    const before = tensor.shape;
    defer tensor.shape = before;
    tensor.shape = &.{ 8, 16 }; // same F32 bytes, changed actual semantic shape
    try std.testing.expectError(error.AuthoredResidentTopologyChanged, Native.NativeCompute.validateAuthoredSourceQueries(&backend, &queries));
    tensor.shape = before;
    try Native.NativeCompute.validateAuthoredSourceQueries(&backend, &queries);
}

test "native authored full query cannot hide unsupported actual lm head behind embedding fallback" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    var model = try TestModel.init(a);
    defer model.data.deinitOwned();
    var compute = Native.NativeCompute.init(a, &model.data, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    const cfg = TestModel.config();
    const context = Context{ .allocator = a, .cb = &backend, .gpt_config = &cfg };
    const head = model.data.resident_weights.getPtr("lm_head.weight").?;
    head.quantized = true;
    try std.testing.expectEqual(@as(?*Program, null), try Program.tryCreate(a, &context));
    head.quantized = false;
}

test "native Definition template shares only immutable graph and descriptors across private programs" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    var model = try TestModel.init(a);
    defer model.data.deinitOwned();
    var compute = Native.NativeCompute.init(a, &model.data, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    const cfg = TestModel.config();
    const context = Context{ .allocator = a, .cb = &backend, .gpt_config = &cfg };
    var slot = TemplateSlot{};
    defer slot.deinit();
    const first = (try Program.tryCreateWithTemplate(a, &context, &slot)) orelse return error.MissingAuthoredNativeProgram;
    defer first.destroyWithCleanup(.{ .outside_execution = true });
    const second = (try Program.tryCreateWithTemplate(a, &context, &slot)) orelse return error.MissingAuthoredNativeProgram;
    defer second.destroyWithCleanup(.{ .outside_execution = true });
    try std.testing.expect(first.template == second.template and first.graph == second.graph and first.steps.ptr == second.steps.ptr);
    try std.testing.expect(first.source_queries.ptr != second.source_queries.ptr and first.values.ptr != second.values.ptr and first.weights.ptr != second.weights.ptr);
    try std.testing.expect(first.selected.pool != second.selected.pool);
    try std.testing.expectEqual(@as(u64, 1), slot.stats.builds);
    try std.testing.expectEqual(@as(u64, 1), slot.stats.hits);
    const Kv = @import("../runtime/root.zig").kv;
    var manager = Kv.manager.KvManager.init(a);
    defer manager.deinit();
    const pool = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 4, .num_kv_heads = 1, .head_dim = 4 });
    const cancelled_sequence = try manager.attachSequence(pool);
    try manager.appendTokens(cancelled_sequence, 1);
    const cancelled_decode = TestModel.decode(&manager, pool, cancelled_sequence, 1);
    var cancelled = try first.beginCall(&.{2}, &cancelled_decode, null);
    defer cancelled.close(.{ .outside_execution = true });
    try cancelled.prepareInto(a, null, .{ .outside_execution = true });
    const Cancel = struct {
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks == 3) return error.Cancelled;
        }
    };
    var control = Cancel{};
    var controlled = backend;
    controlled.execution_control = .{ .ptr = &control, .check_fn = Cancel.check, .native_call_policy = .in_process };
    try std.testing.expectError(error.Cancelled, cancelled.run(&controlled, cfg, &cancelled_decode, .{ .outside_execution = true }));
    try std.testing.expect(cancelled.ran and first.failed and !second.failed);
    try std.testing.expect(cancelled.frame.?.stats.dispatches > 0);
    cancelled.close(.{ .outside_execution = true });
    try manager.releaseSequence(cancelled_sequence);
    try first.afterStateReset(.{ .outside_execution = true });
    try std.testing.expect(!first.failed and second.calls == 0);
    const reference_sequence = try manager.attachSequence(pool);
    try manager.appendTokens(reference_sequence, 1);
    const reference_decode = TestModel.decode(&manager, pool, reference_sequence, 1);
    const expected = try gpt.forward(&backend, a, cfg, &.{2}, 1, 1, &reference_decode);
    defer a.free(expected);
    for ([_]*Program{ first, second }) |program| {
        const sequence = try manager.attachSequence(pool);
        try manager.appendTokens(sequence, 1);
        const decode = TestModel.decode(&manager, pool, sequence, 1);
        var owner = try @import("cleanup_owner.zig").Owner.beginOutsideCleanup();
        defer owner.finishAfterUnlock();
        const current = Cleanup{ .boundary = owner.epoch() };
        var call = try program.beginCall(&.{2}, &decode, null);
        defer call.close(current);
        try call.prepareInto(a, null, current);
        try call.run(&backend, cfg, &decode, current);
        const actual = try call.takeOutput();
        defer a.free(actual);
        try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(expected), std.mem.sliceAsBytes(actual));
    }
    try std.testing.expectEqual(@as(u64, 1), first.calls);
    try std.testing.expectEqual(@as(u64, 1), second.calls);
    try std.testing.expect(!first.failed and !second.failed);
}

test "native Definition template cold and cache-hit constructors unwind all allocation failures" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var model = try TestModel.init(a);
            defer model.data.deinitOwned();
            var compute = Native.NativeCompute.init(a, &model.data, null);
            defer compute.deinit();
            const backend = compute.computeBackend();
            const cfg = TestModel.config();
            const context = Context{ .allocator = a, .cb = &backend, .gpt_config = &cfg };
            var slot = TemplateSlot{};
            defer slot.deinit();
            const first = (try Program.tryCreateWithTemplate(a, &context, &slot)) orelse return error.MissingAuthoredNativeProgram;
            defer first.destroyWithCleanup(.{ .outside_execution = true });
            const original = slot.value;
            const second = (try Program.tryCreateWithTemplate(a, &context, &slot)) orelse return error.MissingAuthoredNativeProgram;
            defer second.destroyWithCleanup(.{ .outside_execution = true });
            try std.testing.expect(original == slot.value and first.template == second.template);
            try std.testing.expectEqual(@as(u64, 1), slot.stats.builds);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

fn q6SingleFixtureName(name: []const u8) bool {
    return std.mem.endsWith(u8, name, ".self_attn.o_proj.weight") or std.mem.endsWith(u8, name, ".mlp.down_proj.weight") or std.mem.eql(u8, name, "lm_head.weight");
}

/// Real encoded packed weights, same original storage owner and KBlock cold
/// preparation API. The two F32 pair operands stay F32 deliberately.
fn quantizeAuthoredSingleProjections(a: std.mem.Allocator, model: *TestModel, cfg: gpt.Config, include_pair: bool) !void {
    for (0..cfg.num_hidden_layers) |layer| {
        const suffixes = [_][]const u8{ "self_attn.o_proj.weight", "mlp.down_proj.weight", "mlp.gate_proj.weight" };
        for (suffixes[0..@as(usize, if (include_pair) 3 else 2)], 0..) |suffix, selected| {
            const name = try std.fmt.allocPrint(a, "model.layers.{d}.{s}", .{ layer, suffix });
            defer a.free(name);
            const source = model.data.resident_weights.getPtr(name) orelse return error.MissingWeight;
            try quantizeAuthoredProjection(a, source, model.data.resident_weights.getKey(name).?, if (selected == 0) .Q4_K else .Q5_K);
        }
    }
}
fn quantizeAuthoredProjection(a: std.mem.Allocator, source: *@import("../models/weight_source.zig").LoadedWeight, stable_name: []const u8, kind: @import("../gguf/tensor_types.zig").KnownTensorType) !void {
    const codec = @import("../gguf/quant_codec.zig");
    const values = source.tensor.asFloat32IfAligned() orelse return error.InvalidTensorData;
    const raw = switch (kind) {
        .Q4_K => try codec.quantizeQ4_KFromF32(a, values),
        .Q5_K => try codec.quantizeQ5_KFromF32(a, values),
        .Q6_K => try codec.quantizeQ6_KFromF32(a, values),
        .Q8_0 => try codec.quantizeQ8_0FromF32(a, values),
        else => return error.UnsupportedTensorType,
    };
    errdefer a.free(raw);
    const shape = try a.dupe(i64, source.tensor.shape);
    errdefer a.free(shape);
    source.tensor.deinit();
    source.* = .{ .tensor = .{ .data = &.{}, .dtype = .f32, .shape = &.{}, .name = stable_name, .allocator = a, .owns_data = false, .owns_shape = false }, .quantized = true, .quantized_storage = .{ .tensor_type = .{ .known = kind }, .raw_bytes = raw, .shape = shape, .allocator = a } };
}
fn quantizeAuthoredProjections(a: std.mem.Allocator, model: *TestModel) !void {
    var it = model.data.resident_weights.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.tensor.shape.len != 2 or std.mem.eql(u8, entry.key_ptr.*, "model.embed_tokens.weight")) continue;
        const kind: @import("../gguf/tensor_types.zig").KnownTensorType = if (std.mem.indexOf(u8, entry.key_ptr.*, ".mlp.") != null or std.mem.eql(u8, entry.key_ptr.*, "lm_head.weight")) .Q5_K else .Q4_K;
        try quantizeAuthoredProjection(a, entry.value_ptr, entry.key_ptr.*, kind);
    }
}
fn prepareAuthoredSingleProjections(backend: *const ops.ComputeBackend, data: *Native.WeightStore) !void {
    var it = data.resident_weights.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.quantized_storage == null) continue;
        var pending = (try Native.NativeCompute.snapshotResidentWeightPreparation(backend, data, entry.key_ptr.*)) orelse continue;
        defer pending.finishOutsideExecution();
        try pending.fundOutsideExecution(null);
        try pending.prepareOutsideExecution();
    }
}

test "native authored complete mixed Q4_K Q5_K single projections preserve original kernels pages reset and zero hot allocation" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const cfg = try TestModel.parsedLlamaConfig(std.testing.allocator, 256, 2, false);
    try nativeAuthoredDecodeStorageSteps(cfg, 11, 6, true);
}

test "native authored mixed packed dense pair and unprepared source decline before whole program publication" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const cfg = try TestModel.parsedLlamaConfig(a, 256, 1, false);
    var model = try TestModel.expanded(a, cfg);
    defer model.data.deinitOwned();
    try quantizeAuthoredSingleProjections(a, &model, cfg, true);
    var compute = Native.NativeCompute.init(a, &model.data, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    const context = Context{ .allocator = a, .cb = &backend, .gpt_config = &cfg };
    try std.testing.expect((try Program.tryCreate(a, &context)) == null);
    try prepareAuthoredSingleProjections(&backend, &model.data);
    // The actual original pair remains a pair. One packed operand cannot be
    // normalized into an independent single to manufacture coverage.
    try std.testing.expect((try Program.tryCreate(a, &context)) == null);
}

test "native authored mixed KBlock post-entry cancellation poisons without replay and original reset recovers bitwise" {
    const cfg = try TestModel.parsedLlamaConfig(std.testing.allocator, 256, 1, false);
    try nativeAuthoredCancellationStorageCase(cfg, true);
}

fn quantizedSingleProgramOomCase(a: std.mem.Allocator) !void {
    return quantizedProgramOomCase(a, false);
}
fn quantizedProgramOomCase(a: std.mem.Allocator, paired: bool) !void {
    return quantizedMatrixProgramOomCase(a, paired, false);
}
fn quantizedMatrixProgramOomCase(a: std.mem.Allocator, paired: bool, packed_rows: bool) !void {
    return quantizedFormatProgramOomCase(a, paired, packed_rows, .q4q5);
}
fn quantizedFormatProgramOomCase(a: std.mem.Allocator, paired: bool, packed_rows: bool, mode: TestModel.MatrixFixture) !void {
    const cfg = if (mode == .q4q5) try TestModel.parsedLlamaConfig(a, 256, 1, false) else try TestModel.parsedQ8Config(a, .llama, true, 1);
    var model = try TestModel.expanded(a, cfg);
    defer model.data.deinitOwned();
    if (mode != .q4q5) try model.quantizeResidentFixture(a, mode) else if (packed_rows) try model.quantizeAllMatrices(a) else if (paired) try quantizeAuthoredProjections(a, &model) else try quantizeAuthoredSingleProjections(a, &model, cfg, false);
    var compute = Native.NativeCompute.init(a, &model.data, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    try prepareAuthoredSingleProjections(&backend, &model.data);
    const context = Context{ .allocator = a, .cb = &backend, .gpt_config = &cfg };
    const program = (try Program.tryCreate(a, &context)) orelse return error.MissingAuthoredNativeProgram;
    defer program.destroyWithCleanup(.{ .outside_execution = true });
    const Kv = @import("../runtime/root.zig").kv;
    var manager = Kv.manager.KvManager.init(a);
    defer manager.deinit();
    const pool = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 4, .num_kv_heads = @intCast(cfg.effectiveKVHeads()), .head_dim = @intCast(cfg.headDim()), .num_layers_packed = @intCast(cfg.num_hidden_layers) });
    const sequence = try manager.attachSequence(pool);
    try manager.appendTokens(sequence, 1);
    const decode = TestModel.decode(&manager, pool, sequence, 1);
    var call = try program.beginCall(&.{2}, &decode, null);
    defer call.close(.{ .outside_execution = true });
    try call.prepareInto(a, null, .{ .outside_execution = true });
    try call.run(&backend, cfg, &decode, .{ .outside_execution = true });
    const output = try call.takeOutput();
    a.free(output);
}
test "native authored mixed KBlock retained source views route metadata and assigned Frame unwind every allocation failure" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, quantizedSingleProgramOomCase, .{});
}

test "native authored all Q4_K Q5_K projection matrices retain original joint pair kernels pages reset and zero hot allocation" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const cfg = try TestModel.parsedLlamaConfig(std.testing.allocator, 256, 2, false);
    try nativeAuthoredDecodeProjectionSteps(cfg, 11, 6, true, true);
}
test "native authored paired KBlock post-entry cancellation requires reset and recovers original bitwise output" {
    const cfg = try TestModel.parsedLlamaConfig(std.testing.allocator, 256, 1, false);
    try nativeAuthoredCancellationProjectionCase(cfg, true, true);
}
test "native authored paired KBlock joint metadata both assigned slots and shared pack bank unwind every allocation failure" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            return quantizedProgramOomCase(a, true);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "native authored natural Q4_K Q5_K shared activation pair and single pack banks preserve full original output without hot allocator" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const cfg = try TestModel.parsedLlamaPackedConfig(std.testing.allocator, 2);
    try nativeAuthoredDecodeProjectionSteps(cfg, 7, 5, true, true);
}

test "native authored natural Q4_K Q5_K single activation pack Frame bank preserves full original output without hot allocator" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const cfg = try TestModel.parsedLlamaPackedConfig(std.testing.allocator, 2);
    try nativeAuthoredDecodeStorageSteps(cfg, 7, 5, true);
}

test "native authored complete Q4_K Q5_K matrices including embedding and tied head retain original bytes pages reset and no hot allocation" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    inline for (.{ false, true }) |tied| {
        const cfg = try TestModel.parsedLlamaPackedConfigWithTying(std.testing.allocator, 2, tied);
        try nativeAuthoredDecodeMatrixSteps(cfg, 11, 6, true, true, true);
    }
}
test "native authored complete packed matrices cancellation poisons once and actual KV reset restores original output" {
    const cfg = try TestModel.parsedLlamaPackedConfigWithTying(std.testing.allocator, 1, true);
    try nativeAuthoredCancellationMatrixCase(cfg, true, true, true);
}
test "native authored packed row route retained source output bank and joint projection setup unwind every allocation failure" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            return quantizedMatrixProgramOomCase(a, true, true);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "Native Definition slot statistics copy successful constructor lifetime and decline busy without retaining slot" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    var model = try TestModel.init(a);
    defer model.data.deinitOwned();
    var compute = Native.NativeCompute.init(a, &model.data, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    const cfg = TestModel.config();
    const context = Context{ .allocator = a, .cb = &backend, .gpt_config = &cfg };
    var slot = TemplateSlot{};
    var slot_live = true;
    defer if (slot_live) slot.deinit();
    const first = (try Program.tryCreateWithTemplate(a, &context, &slot)) orelse return error.MissingAuthoredNativeProgram;
    defer first.destroyWithCleanup(.{ .outside_execution = true });
    const second = (try Program.tryCreateWithTemplate(a, &context, &slot)) orelse return error.MissingAuthoredNativeProgram;
    defer second.destroyWithCleanup(.{ .outside_execution = true });
    const copied = slot.tryStatistics().?;
    try std.testing.expectEqual(@as(u64, 1), copied.builds);
    try std.testing.expectEqual(@as(u64, 1), copied.misses);
    try std.testing.expectEqual(@as(u64, 1), copied.hits);
    try std.testing.expect(slot.mutex.tryLock());
    try std.testing.expect(slot.tryStatistics() == null);
    slot.mutex.unlock();
    slot.deinit();
    slot_live = false;
    try std.testing.expect(first.template == second.template);
    try std.testing.expect(first.statistics() != null and second.statistics() != null);
    try std.testing.expectEqual(@as(u64, 1), copied.hits);
}

test "Native actual complete Call success refusal and cancellation publish copied report without constructor hit attribution" {
    const D = @import("diagnostics.zig");
    const O = @import("invocation_observation.zig");
    var diagnostics = D.Diagnostics{};
    const binding = O.Binding{ .diagnostics = &diagnostics, .capture_programs = true };
    try std.testing.expect(binding.configure(true));
    const cfg = TestModel.config();
    try O.call(binding, nativeAuthoredCancellationMatrixCase, .{ cfg, false, false, false });
    const snapshot = diagnostics.snapshot() orelse return error.MissingActualNativeReport;
    const report = snapshot.report orelse return error.MissingActualNativeReport;
    const records = report.authored_native_programs orelse return error.MissingActualNativeReport;
    try std.testing.expect(records.count > 0);
    var entries: u64 = 0;
    var returned: u64 = 0;
    var errors: u64 = 0;
    for (records.records[0..records.count]) |record| {
        entries += record.entries;
        returned += record.returns;
        errors += record.error_returns;
    }
    try std.testing.expect(entries > 0 and returned > 0 and errors > 0);
    try std.testing.expect(report.native_definition_templates == null);
    const bank = report.authored_native_program_bank orelse return error.MissingNativeProgramBank;
    const owned = bank.owned_ledger orelse return error.MissingNativeProgramLedger;
    try std.testing.expect(owned.total.leases_created > 0 and owned.total.requested_peak_bytes > 0);
    try std.testing.expect(owned.total.requested_live_bytes > 0);
    try std.testing.expectEqualDeep(owned.activation, report.program_activations.?);
    try std.testing.expectEqualDeep(owned.workspace, report.program_workspace.?);
    try std.testing.expectEqual(bank.frame_allocations, report.program_storage_allocations.?);
    try std.testing.expectEqual(bank.frame_requested_allocation_bytes, report.program_storage_requested_allocation_bytes.?);
    try std.testing.expectEqual(bank.frame_reuse_accesses, report.program_storage_reuse_hits.?);
    _ = binding.configure(false);
    try O.call(binding, nativeAuthoredDecodeCase, .{cfg});
    try std.testing.expect(diagnostics.snapshot() == null);
}

test "Native actual sealed B and unprepared Call retain original error order and report not entered" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    var model = try TestModel.init(a);
    defer model.data.deinitOwned();
    var compute = Native.NativeCompute.init(a, &model.data, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    const cfg = TestModel.config();
    const context = Context{ .allocator = a, .cb = &backend, .gpt_config = &cfg };
    const program = (try Program.tryCreate(a, &context)) orelse return error.MissingAuthoredNativeProgram;
    defer program.destroyWithCleanup(.{ .outside_execution = true });
    const Kv = @import("../runtime/root.zig").kv;
    var manager = Kv.manager.KvManager.init(a);
    defer manager.deinit();
    const pool = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 4, .num_layers_packed = @intCast(cfg.num_hidden_layers), .num_kv_heads = @intCast(cfg.effectiveKVHeads()), .head_dim = @intCast(cfg.headDim()) });
    const sequence = try manager.attachSequence(pool);
    try manager.appendTokens(sequence, 1);
    const decode = TestModel.decode(&manager, pool, sequence, 1);
    var call = try program.beginCall(&.{2}, &decode, null);
    defer call.close(.{ .outside_execution = true });
    try call.prepareInto(a, null, .{ .outside_execution = true });
    @memset(call.output.?, -91);
    var owner = try @import("cleanup_owner.zig").Owner.beginOutsideCleanup();
    var sealed = try owner.epoch().?.retain();
    defer sealed.release();
    owner.finishAfterUnlock();
    var scope = @import("runtime_observation.zig").KernelScope{};
    scope.begin();
    defer _ = scope.finish();
    try std.testing.expectError(error.RetirementEpochSealed, call.run(&backend, cfg, &decode, .{ .boundary = &sealed }));
    try std.testing.expect(!call.ran and !program.failed);
    for (call.output.?) |value| try std.testing.expectEqual(@as(f32, -91), value);
    try std.testing.expectEqual(@as(u64, 1), scope.native_programs.records[0].not_entered);
    try std.testing.expectEqual(@as(u64, 0), scope.native_programs.records[0].entries);
    try std.testing.expect(scope.native_program_bank == null);
    const actual_frame = call.frame;
    call.frame = null;
    defer call.frame = actual_frame;
    try std.testing.expectError(error.UnpreparedNativeProgramCall, call.run(&backend, cfg, &decode, .{ .outside_execution = true }));
    try std.testing.expectEqual(@as(u64, 2), scope.native_programs.records[0].not_entered);
    try std.testing.expectEqual(@as(u64, 0), scope.native_programs.records[0].assigned_dispatches);
    const actual_program = call.program;
    call.program = null;
    defer call.program = actual_program;
    try std.testing.expectError(error.RetirementEpochSealed, call.run(&backend, cfg, &decode, .{ .boundary = &sealed }));
    try std.testing.expectEqual(@as(u64, 2), scope.native_programs.records[0].not_entered);
}

test "Native complete resident Q8 matrices parsed Llama Qwen tied and mixed preserve original bits reset page crossing and zero numerical allocator" {
    inline for (.{ .llama, .qwen3 }) |family| inline for (.{ false, true }) |tied| {
        const cfg = try TestModel.parsedQ8Config(std.testing.allocator, family, tied, 2);
        try nativeAuthoredDecodeFormatSteps(cfg, 11, 6, true, true, true, .q8);
    };
    const mixed = try TestModel.parsedQ8Config(std.testing.allocator, .llama, false, 2);
    try nativeAuthoredDecodeFormatSteps(mixed, 11, 6, true, true, true, .mixed);
}
test "Native resident Q8 exact shared pair and physical activation bank unwind every allocation failure" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            try quantizedFormatProgramOomCase(a, true, true, .q8);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
test "Native resident Q8 post entry cancellation poisons and actual reset recovers original output" {
    const cfg = try TestModel.parsedQ8Config(std.testing.allocator, .qwen3, true, 2);
    try nativeAuthoredCancellationFormatCase(cfg, true, true, true, .q8);
}
test "Native resident Q8 preserves original group and declines default large geometry threaded tasks before math" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const cfg = try TestModel.parsedQ8Config(a, .llama, false, 1);
    var model = try TestModel.expanded(a, cfg);
    defer model.data.deinitOwned();
    try model.quantizeResidentFixture(a, .incompatible_pair);
    var compute = Native.NativeCompute.init(a, &model.data, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    try prepareAuthoredSingleProjections(&backend, &model.data);
    const context = Context{ .allocator = a, .cb = &backend, .gpt_config = &cfg };
    try std.testing.expect((try Program.tryCreate(a, &context)) == null);
    const large = try TestModel.parsedLlamaPackedConfig(a, 2);
    if (!Native.Testing.q8NeedsTasks(1, large.hidden_size, large.intermediate_size)) return error.SkipZigTest;
    var large_model = try TestModel.expanded(a, large);
    defer large_model.data.deinitOwned();
    try large_model.quantizeResidentFixture(a, .q8);
    var large_compute = Native.NativeCompute.init(a, &large_model.data, null);
    defer large_compute.deinit();
    const large_backend = large_compute.computeBackend();
    try prepareAuthoredSingleProjections(&large_backend, &large_model.data);
    const before = Native.nativeQuantDispatchStats();
    const large_context = Context{ .allocator = a, .cb = &large_backend, .gpt_config = &large };
    const outcome = try Program.prepareWithTemplate(a, &large_context, null);
    switch (outcome) {
        .prepared => |unexpected| {
            unexpected.destroyWithCleanup(.{ .outside_execution = true });
            return error.ExpectedPrecommitTaskDecline;
        },
        .declined => |reason| try std.testing.expectEqual(error.OwnedNativeIoTaskUnavailable, reason.?),
    }
    try std.testing.expect((try Program.tryCreate(a, &large_context)) == null);
    try std.testing.expectEqualDeep(before, Native.nativeQuantDispatchStats());
}

test "Native actual Call bank cold preparation warm reuse and copied ledger survive owner death" {
    const D = @import("diagnostics.zig");
    const O = @import("invocation_observation.zig");
    var diagnostics = D.Diagnostics{};
    const binding = O.Binding{ .diagnostics = &diagnostics, .capture_programs = true };
    try std.testing.expect(binding.configure(true));
    // This actual cold/warm/reset case also installs fail-on-first allocators
    // during run. Existing eager F32 byte oracle remains unchanged.
    try O.call(binding, nativeAuthoredDecodeCase, .{TestModel.config()});
    const copied = diagnostics.snapshot().?.report.?;
    const bank = copied.authored_native_program_bank orelse return error.MissingNativeProgramBank;
    try std.testing.expect(bank.frame_reuse_accesses > 0);
    try std.testing.expect(bank.owned_ledger.?.total.leases_created > 0);
    try std.testing.expect(bank.owned_ledger.?.total.requested_live_bytes > 0);
    try std.testing.expectEqualDeep(bank.owned_ledger.?.constant, copied.program_constants.?);
    try std.testing.expectEqualDeep(bank.owned_ledger.?.host_workspace, copied.program_host_workspace.?);
    try std.testing.expectEqualStrings("prepared_owner_lease_lifetime_sampled_before_call_close", copied.program_storage_scope);
    _ = binding.configure(false);
    try O.call(binding, nativeAuthoredDecodeCase, .{TestModel.config()});
    try std.testing.expect(diagnostics.snapshot() == null);
    // The detached value is neither a live owner pointer nor a refreshed
    // final-live gauge; diagnostics disable and all producer deaths preserve it.
    try std.testing.expectEqualDeep(bank, copied.authored_native_program_bank.?);
}

test "native authored Q6_K single and head original kernels cross pages and reset bitwise" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |tied| {
        const cfg = try TestModel.parsedQ8Config(a, .llama, tied, 2);
        try nativeAuthoredDecodeFormatSteps(cfg, 19, 10, true, false, false, .q6_singles);
    }
}

test "native authored Q6_K packed row tied head and mixed original Q4 Q5 pairs preserve full output" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |tied| {
        const cfg = try TestModel.parsedQ8Config(a, .qwen3, tied, 2);
        try nativeAuthoredDecodeFormatSteps(cfg, 19, 10, true, false, true, .q6_rows);
        try nativeAuthoredDecodeFormatSteps(cfg, 19, 10, true, true, true, .q6_mixed);
    }
}

test "native authored Q6_K cancellation poisons once and actual reset restores original output" {
    const cfg = try TestModel.parsedQ8Config(std.testing.allocator, .llama, true, 2);
    try nativeAuthoredCancellationFormatCase(cfg, true, false, true, .q6_rows);
}

test "native authored actual Q6_K pair stays unsupported before normalization or numerical dispatch" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const cfg = try TestModel.parsedQ8Config(a, .llama, false, 2);
    var model = try TestModel.expanded(a, cfg);
    defer model.data.deinitOwned();
    try model.quantizeResidentFixture(a, .q6_pair_unsupported);
    var compute = Native.NativeCompute.init(a, &model.data, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    try prepareAuthoredSingleProjections(&backend, &model.data);
    const context = Context{ .allocator = a, .cb = &backend, .gpt_config = &cfg };
    switch (try Program.prepareWithTemplate(a, &context, null)) {
        .prepared => |program| {
            program.destroyWithCleanup(.{ .outside_execution = true });
            return error.TestUnexpectedResult;
        },
        .declined => |reason| try std.testing.expectEqual(error.UnsupportedAuthoredWeight, reason.?),
    }
}

test "native authored Q6_K row source route and actual Frame unwind every allocation failure" {
    try quantizedFormatProgramOomCase(std.testing.allocator, false, true, .q6_rows);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, quantizedFormatProgramOomCase, .{ false, true, TestModel.MatrixFixture.q6_rows });
}

test "native authored natural Q6_K activation Frame workspace includes actual wide head and reuses original kernels" {
    const cfg = try TestModel.parsedQ6ActivationConfig(std.testing.allocator);
    try nativeAuthoredDecodeFormatSteps(cfg, 10, 6, true, false, true, .q6_rows);
}

test "native authored Q6_K strict activation policy remains an original precommit decline" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const cfg = try TestModel.parsedQ8Config(a, .llama, false, 1);
    var model = try TestModel.expanded(a, cfg);
    defer model.data.deinitOwned();
    try model.quantizeResidentFixture(a, .q6_rows);
    var compute = Native.NativeCompute.init(a, &model.data, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    try prepareAuthoredSingleProjections(&backend, &model.data);
    compute.quantized_activation_policy = .strict_f32;
    const context = Context{ .allocator = a, .cb = &backend, .gpt_config = &cfg };
    switch (try Program.prepareWithTemplate(a, &context, null)) {
        .prepared => |program| {
            program.destroyWithCleanup(.{ .outside_execution = true });
            return error.TestUnexpectedResult;
        },
        .declined => |reason| try std.testing.expect(reason != null),
    }
}
