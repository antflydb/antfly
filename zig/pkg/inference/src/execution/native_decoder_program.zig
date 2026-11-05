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

const NamedSource = struct { name: []u8, value: CT };

pub const Program = struct {
    allocator: std.mem.Allocator,
    backend: ops.ComputeBackend,
    config: gpt.Config,
    cache: GraphCache,
    graph: *ml.graph.Graph,
    analysis: Analysis,
    runtime: Runtime,
    selected: *@import("prepared_routes.zig").PreparedRoutes,
    bindings: []route.InputBinding,
    weights: []?CT,
    weight_planes: []@import("storage_ownership.zig").Plane,
    named_sources: []NamedSource,
    values: []?CT,
    steps: []ml.graph.NodeId,
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
        if (comptime !@import("build_options").enable_native) return null;
        if (context.cb.vtable != &Native.vtable_impl or context.weights != null or !gpt.denseQwen3PrefillEligible(context.gpt_config.*)) return null;
        if (context.session) |source| if (!@import("architecture_session.zig").nativeProgramSourceMatches(source, context.cb)) return null;
        return create(allocator, context) catch |err| switch (err) {
            error.UnsupportedAuthoredWeight, error.UnsupportedAuthoredNativeTarget, error.IncompleteNativeProgram, error.UnsupportedNativeProgramWorkspace, error.MissingPreparedKernelRoute, error.UnsupportedShape => null,
            else => return err,
        };
    }
    fn create(allocator: std.mem.Allocator, context: *const Context) !*Program {
        const self = try allocator.create(Program);
        errdefer allocator.destroy(self);
        const backend = context.cb.withoutSynchronousBorrows();
        self.allocator = allocator;
        self.backend = backend;
        self.config = context.gpt_config.*;
        self.cache = GraphCache.init(allocator);
        errdefer self.cache.deinitWithBackend(&self.backend);
        // This resolver wraps only the existing cold original-forward trace.
        // It refuses a nonresident/packed source before any lookup preparation.
        const Resolver = struct {
            original: *const Context,
            allocator: std.mem.Allocator,
            sources: std.ArrayList(NamedSource) = .empty,
            fn get(raw: *anyopaque, name: []const u8) !CT {
                const value: *@This() = @ptrCast(@alignCast(raw));
                _ = try Native.NativeCompute.authoredResidentF32Shape(value.original.cb, name);
                var found = false;
                for (value.sources.items) |source| if (std.mem.eql(u8, source.name, name)) {
                    found = true;
                    break;
                };
                if (!found) {
                    const owned_name = try value.allocator.dupe(u8, name);
                    errdefer value.allocator.free(owned_name);
                    const witness = try value.original.graphWeight(name);
                    errdefer value.original.cb.free(witness);
                    _ = try Native.NativeCompute.authoredResidentF32Plane(value.original.cb, name, witness);
                    try value.sources.append(value.allocator, .{ .name = owned_name, .value = witness });
                }
                return value.original.graphWeight(name);
            }
        };
        var resolver = Resolver{ .original = context, .allocator = allocator };
        defer {
            for (resolver.sources.items) |source| {
                self.backend.free(source.value);
                allocator.free(source.name);
            }
            resolver.sources.deinit(allocator);
        }
        var tracing_context = context.*;
        tracing_context.cb = &self.backend;
        tracing_context.weights = .{ .ptr = &resolver, .get_fn = Resolver.get };
        var id = [_]i64{0};
        const trace_decode = gpt.DecodeContext{ .attention_mode = .paged_decode, .total_sequence_len = 1, .query_sequence_len = 1, .kv_sequence_len = 1 };
        const entry = try @import("../graph/execution.zig").ensureGraphEntry(&tracing_context, &self.cache, &id, 1, 1, &trace_decode);
        self.graph = &entry.graph;
        try normalizeDensePairTrace(self.graph);
        try normalizeDenseGroupedTrace(self.graph);
        var input: ?ml.graph.NodeId = null;
        for (self.graph.nodes.items, 0..) |*node, index| if (node.op == .fused_from_float32) {
            if (input != null or node.output_shape.dtype != .i64 or node.output_shape.numElements() != 1) return error.IncompleteNativeProgram;
            const name = try self.graph.internString("native.decoder.token");
            node.op = .{ .parameter = .{ .name_offset = name.offset, .name_len = name.len } };
            try self.graph.parameters.append(allocator, @intCast(index));
            input = @intCast(index);
        };
        self.input = input orelse return error.IncompleteNativeProgram;
        self.analysis = try Analysis.compute(allocator, self.graph);
        errdefer self.analysis.deinit(allocator);
        if (self.graph.outputs.items.len != 1 or self.graph.node(self.graph.outputs.items[0]).output_shape.dtype != .f32 or self.graph.node(self.graph.outputs.items[0]).output_shape.numElements() != self.config.vocab_size) return error.IncompleteNativeProgram;
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
        for (self.graph.nodes.items, 0..) |node, index| {
            if (!self.analysis.reachable[index] or node.op != .parameter) continue;
            if (index == self.input) {
                continue;
            }
            const name = self.graph.parameterName(self.graph.node(@intCast(index)));
            const actual = try Native.NativeCompute.authoredResidentF32Shape(&self.backend, name);
            if (!actual.eq(node.output_shape)) return error.IncompleteNativeProgram;
            const value = try context.graphWeight(name);
            self.weights[index] = value;
            self.weight_planes[index] = try Native.NativeCompute.authoredResidentF32Plane(&self.backend, name, value);
            try bindings.append(allocator, .{ .node_id = @intCast(index), .value = .{ .resident = .{ .logical = actual, .implementation = .{ .context = self.backend.ptr, .table = self.backend.vtable }, .planes = self.weight_planes[index..][0..1] } } });
        }
        self.bindings = try allocator.alloc(route.InputBinding, 1);
        self.bindings[0] = .{ .node_id = self.input, .value = .{ .imported_host = .{ .logical = self.graph.node(self.input).output_shape, .target = .native, .owned_ingress = true } } };
        errdefer allocator.free(self.bindings);
        self.values = try allocator.dupe(?CT, self.weights);
        errdefer allocator.free(self.values);
        self.runtime = try Runtime.initWithOptions(allocator, self.graph, &self.backend, .partitioned, .{ .analysis = self.analysis, .region_costs = null });
        errdefer self.runtime.deinit();
        const pool = self.runtime.physical_pool orelse return error.IncompleteNativeProgram;
        if (pool.backends.len != 1 or pool.backends[0].ptr != self.backend.ptr or pool.backends[0].vtable != self.backend.vtable or pool.plan.transfers.len != 0) return error.IncompleteNativeProgram;
        pool.coverage_graph = self.graph;
        pool.coverage_reachable = self.analysis.reachable;
        pool.coverage_backend = self.backend;
        pool.native_frame_dispatch = true;
        var steps: std.ArrayList(ml.graph.NodeId) = .empty;
        defer steps.deinit(allocator);
        for (self.graph.nodes.items, 0..) |node, index| {
            if (!self.analysis.reachable[index] or node.op == .parameter) continue;
            for (node.getInputs()) |source| if (source >= index) return error.IncompleteNativeProgram;
            const slot = pool.plan.slotForNode(@intCast(index)) orelse return error.IncompleteNativeProgram;
            if (pool.kernels[index] == null and node.op != .constant and node.op != .reshape) {
                const descriptor = (try authoredDescriptor(self.graph, @intCast(index), self.config)) orelse return error.IncompleteNativeProgram;
                var sources: [3]ml.graph.NodeId = @splat(ml.graph.null_node);
                @memcpy(sources[0..descriptor.input_count], node.getInputs());
                pool.kernels[index] = .{ .descriptor = descriptor, .inputs = sources, .backend_index = 0, .output_allocation = slot.allocation, .workspace_bytes = 0 };
            }
            if (node.op == .reshape and (slot.kind != .view or !std.meta.eql(slot.desc.strides, @import("../graph/backend_contracts.zig").TensorStrides.dense(node.output_shape)))) return error.IncompleteNativeProgram;
            try steps.append(allocator, @intCast(index));
        }
        self.steps = try steps.toOwnedSlice(allocator);
        errdefer allocator.free(self.steps);
        try pool.configureRoutes(bindings.items);
        self.selected = (try pool.prepareRoutesWithPolicy(self.bindings, .compatibility_regions)) orelse return error.MissingPreparedKernelRoute;
        errdefer self.selected.release();
        if (self.selected.pool.payload_coverage != .complete or self.selected.pool.regions.len != 0) return error.IncompleteNativeProgram;
        _ = try self.selected.storageRequirement();
        self.named_sources = try resolver.sources.toOwnedSlice(allocator);
        self.mutex = .unlocked;
        self.busy = false;
        self.failed = false;
        self.calls = 0;
        self.assigned_operations = 0;
        self.source_identity = if (context.session) |source| .{ .ptr = source.ptr, .table = source.vtable } else null;
        self.domain = null;
        self.source_owner = if (context.session) |session| if (session.model_lifetime) |owner| owner.retain() else null else null;
        return self;
    }
    /// The selected resident F32 Native pair executes exactly two ordinary
    /// linearNoBias calls, first A then B. Give those same calls their original
    /// distinct values/assignments; packed/quantized pair kernels never enter.
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
        for (self.named_sources) |source| {
            self.backend.freeWithCleanup(source.value, cleanup);
            self.allocator.free(source.name);
        }
        self.allocator.free(self.named_sources);
        self.cache.deinitWithBackend(&self.backend);
        self.analysis.deinit(self.allocator);
        const allocator = self.allocator;
        const source = self.source_owner;
        const domain = self.domain;
        allocator.free(self.bindings);
        allocator.free(self.weights);
        allocator.free(self.weight_planes);
        allocator.free(self.values);
        allocator.free(self.steps);
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
        try self.run(&numerical, program.config, decode, current);
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
    }
    pub fn run(self: *Call, backend: *const ops.ComputeBackend, config: gpt.Config, decode: *const gpt.DecodeContext, current: Cleanup) !void {
        try validateBoundary(current);
        const program = self.program orelse return error.NativeProgramCallConsumed;
        const frame = self.frame orelse return error.UnpreparedNativeProgramCall;
        const output = self.output orelse return error.UnpreparedNativeProgramCall;
        if (self.ran or backend.ptr != program.backend.ptr or backend.vtable != program.backend.vtable or config.fingerprint() != program.config.fingerprint() or !program.supportsCall(&.{self.token}, 1, decode)) return error.NativeProgramCallChanged;
        const attention = gpt.attentionContextFromDecode(decode);
        if (attention.kv_manager != self.manager or attention.kv_cache == null or attention.kv_cache.?.sequence_id != self.sequence or attention.kv_cache.?.pool_id != self.pool or attention.total_sequence_len != self.total_tokens) return error.NativeProgramCallChanged;
        for (program.named_sources) |source| _ = try Native.NativeCompute.authoredResidentF32Plane(backend, source.name, source.value);
        try program.selected.validate(program.bindings);
        for (program.selected.pool.kernels) |maybe| if (maybe) |kernel| if (kernel.descriptor.operation == .paged_attention) try Native.NativeCompute.validateAuthoredAttention(backend, kernel.descriptor.operation.paged_attention, &attention);
        if (frame.storage_admission != null and current.boundary == null) return error.MissingCurrentCleanupEpoch;
        if (current.operation != null) _ = try current.operationFor(backend);
        if (self.construction) |construction| {
            const now = current.boundary orelse return error.MissingCurrentCleanupEpoch;
            if (construction.record != @intFromPtr(now.record.?) or construction.nonce != now.nonce) return error.NativeProgramCleanupChanged;
        }
        self.ran = true;
        errdefer {
            program.failed = true;
            frame.retired = true;
        }
        const before = frame.stats.dispatches;
        for (program.steps) |node| {
            try backend.checkExecutionControl();
            program.values[node] = try frame.executeNativeProgram(backend, program.values, node, decode.total_sequence_len - 1, &attention);
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
    pub fn expanded(allocator: std.mem.Allocator, cfg: gpt.Config) !TestModel {
        var template = try init(allocator);
        defer template.data.deinitOwned();
        var result = TestModel{ .data = .{ .allocator = allocator, .resident_weights = .empty, .lazy_weights = .empty } };
        errdefer result.data.deinitOwned();
        for (0..cfg.num_hidden_layers) |layer| {
            var entries = template.data.resident_weights.iterator();
            while (entries.next()) |entry| {
                const base = entry.key_ptr.*;
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
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    var model = if (config.fingerprint() == TestModel.config().fingerprint()) try TestModel.init(a) else try TestModel.expanded(a, config);
    defer model.data.deinitOwned();
    var compute = Native.NativeCompute.init(a, &model.data, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    const context = Context{ .allocator = a, .cb = &backend, .gpt_config = &config };
    try std.testing.expect(gpt.denseQwen3PrefillEligible(config));
    const program = (try Program.tryCreate(a, &context)) orelse return error.MissingAuthoredNativeProgram;
    defer program.destroyWithCleanup(.{ .outside_execution = true });
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
            // Include trace-resolved names even if optimization consumed their
            // parameter nodes. A changed payload is refused before dispatch.
            const source = &model.data.resident_weights.getPtr("model.layers.0.self_attn.q_proj.weight").?.tensor;
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
        const original_compute_allocator = compute.allocator;
        const original_pool_allocator = program.selected.pool.allocator;
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        compute.allocator = failing.allocator();
        program.selected.pool.allocator = failing.allocator();
        defer {
            compute.allocator = original_compute_allocator;
            program.selected.pool.allocator = original_pool_allocator;
        }
        try call.run(&backend, config, &actual_context, .{ .outside_execution = true });
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
            var model = try TestModel.init(a);
            defer model.data.deinitOwned();
            var compute = Native.NativeCompute.init(a, &model.data, null);
            defer compute.deinit();
            const cb = compute.computeBackend();
            const cfg = TestModel.config();
            const context = Context{ .allocator = a, .cb = &cb, .gpt_config = &cfg };
            const program = (try Program.tryCreate(a, &context)) orelse return error.MissingAuthoredNativeProgram;
            defer program.destroyWithCleanup(.{ .outside_execution = true });
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
}

test "native authored post-entry cancellation poisons without replay and real KV reset restores execution" {
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
    const pool = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 4, .num_kv_heads = 1, .head_dim = 4 });
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
