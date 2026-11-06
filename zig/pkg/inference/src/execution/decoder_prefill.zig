// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Physical prefill requests. Scheduling, prompt preparation and task output
//! policy stay outside this owner; model route/capacity helpers remain canonical.
const std = @import("std");
const platform = @import("antfly_platform");
const build_options = @import("build_options");
const ops = @import("../ops/ops.zig");
const ComputeBackend = ops.ComputeBackend;
const gpt_arch = @import("../architectures/gpt.zig");
const graph_mod = @import("../graph/root.zig");
const decoder_forward = @import("decoder_forward.zig");
const decoder_routes = @import("decoder_routes.zig");
const decoder_storage = @import("decoder_storage.zig");
const decoder_gated_runtime = @import("../backends/decoder_gated_runtime.zig");
const decoder_tail_runtime = @import("../backends/decoder_tail_runtime.zig");
const control = @import("../execution_control.zig");
const runtime = @import("../runtime/root.zig");
const cuda_compute = if (build_options.enable_cuda) @import("../ops/cuda/cuda_compute.zig") else struct {};
const BorrowedDecodeStateRuntime = graph_mod.decode_state_runtime.BorrowedDecodeStateRuntime;

/// A single owned F32 row destination for a prepared embedding layout. Task
/// code supplies semantic text/image ordering; physical copies never rebuild or
/// export the already assembled prefix. Capability decline occurs before writes.
pub const EmbeddingRows = struct {
    backend: *const ComputeBackend,
    allocator: std.mem.Allocator,
    tensor: ?ops.CT,
    rows: usize,
    width: usize,
    namespace: @import("../execution/storage_ownership.zig").Identity,
    written: usize = 0,

    /// The actual accepted storage owner attests the physical readable plane.
    /// A logical F32 dtype alone can also describe packed/opaque model storage,
    /// which copyRows2D cannot necessarily consume. No callback or kind-tag copy
    /// substitutes for this complete plain-row metadata proof.
    fn sourceNamespace(backend: *const ComputeBackend, source: ops.CT, rows: usize, width: usize) !?@import("../execution/storage_ownership.zig").Identity {
        const functions = backend.vtable.kernels orelse return null;
        if (!functions.accepts(backend.vtable)) return null;
        const inspect = functions.storageOwnership orelse return null;
        const bytes = try std.math.mul(usize, try std.math.mul(usize, rows, width), @sizeOf(f32));
        const Proof = struct {
            bytes: usize,
            primary: usize = 0,
            namespace: ?@import("../execution/storage_ownership.zig").Identity = null,
            valid: bool = true,
            fn visit(raw: *anyopaque, report: @import("../execution/storage_ownership.zig").Report) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                const plane = switch (report) {
                    .plane => |plane| plane,
                    .backing => return,
                };
                if (plane.role != .primary) return;
                self.primary += 1;
                self.namespace = plane.backing;
                const descriptor = plane.descriptor;
                if (descriptor.shape.dtype != .f32 or descriptor.quant_format != null or descriptor.attention_kv_format != null or descriptor.attention_storage != null or plane.backing == null or plane.logical_bytes != self.bytes or plane.readable_bytes < self.bytes) {
                    self.valid = false;
                    return;
                }
                switch (descriptor.storage) {
                    .host_f32, .host_dense, .metal_buffer, .cuda_buffer, .metadata_view => {},
                    else => {
                        self.valid = false;
                        return;
                    },
                }
                const elements = descriptor.shape.numElements() orelse {
                    self.valid = false;
                    return;
                };
                if (elements < 0 or std.math.cast(usize, elements) != self.bytes / @sizeOf(f32)) {
                    self.valid = false;
                    return;
                }
                const dense = @import("../graph/backend_contracts.zig").TensorStrides.dense(descriptor.shape);
                if (!std.mem.eql(i64, dense.asSlice(), descriptor.strides.asSlice())) self.valid = false;
            }
        };
        var proof = Proof{ .bytes = bytes };
        const coverage = inspect(backend.ptr, source, .{ .context = &proof, .visit = Proof.visit }) catch |err| switch (err) {
            error.UnsupportedShape, error.UnsupportedTensorType => return null,
            else => return err,
        };
        return if (coverage == .complete and proof.valid and proof.primary == 1) proof.namespace else null;
    }

    pub fn supportsSource(backend: *const ComputeBackend, source: ops.CT, rows: usize, width: usize) !bool {
        return (try sourceNamespace(backend, source, rows, width)) != null;
    }
    /// Device copies stay inside the actual destination provider namespace.
    /// Host-readable F32 sources remain legitimate upload inputs.
    pub fn acceptsSource(self: *const @This(), source: ops.CT, rows: usize) !bool {
        const namespace = (try sourceNamespace(self.backend, source, rows, self.width)) orelse return false;
        if (namespace.kind == .host) return namespace.context == null;
        return namespace.kind == self.namespace.kind and namespace.context == self.namespace.context;
    }

    pub fn init(backend: *const ComputeBackend, allocator: std.mem.Allocator, rows: usize, width: usize) !?EmbeddingRows {
        if (rows == 0 or width == 0) return error.InvalidShape;
        _ = try std.math.mul(usize, rows, width);
        const shape = [_]i32{ std.math.cast(i32, rows) orelse return error.InvalidShape, std.math.cast(i32, width) orelse return error.InvalidShape };
        if (!backend.supportsCopyRows2D() or backend.vtable.allocUninitF32Shape == null) return null;
        const functions = backend.vtable.kernels orelse return null;
        if (!functions.accepts(backend.vtable) or functions.storageOwnership == null) return null;
        const tensor = (backend.allocUninitF32Shape(&shape) catch |err| switch (err) {
            error.UnsupportedShape, error.UnsupportedBackendOp, error.UnsupportedTensorType => return null,
            else => return err,
        }) orelse return null;
        errdefer backend.free(tensor);
        const namespace = (try sourceNamespace(backend, tensor, rows, width)) orelse {
            backend.free(tensor);
            return null;
        };
        return .{ .backend = backend, .allocator = allocator, .tensor = tensor, .rows = rows, .width = width, .namespace = namespace };
    }
    pub fn deinit(self: *@This()) void {
        if (self.tensor) |tensor| self.backend.free(tensor);
        self.tensor = null;
    }
    pub fn append(self: *@This(), source: ops.CT, start: usize, count: usize) !void {
        const destination = self.tensor orelse return error.InvalidEmbeddingAssembly;
        if (count > self.rows - self.written) return error.InvalidEmbeddingAssembly;
        if (!try self.backend.copyRows2D(self.allocator, destination, self.written, source, start, count, self.width)) return error.UnsupportedBackendOp;
        // An error after submission never selects a host replay.
        self.written += count;
    }
    pub fn take(self: *@This()) !ops.CT {
        if (self.written != self.rows) return error.IncompleteEmbeddingAssembly;
        const tensor = self.tensor orelse return error.InvalidEmbeddingAssembly;
        self.tensor = null;
        return tensor;
    }
};

/// Semantic row locations stay with the task. Sources are borrowed only for
/// the synchronous replace call; the completed prompt owns separate storage.
pub const EmbeddingSource = union(enum) { resident: ops.CT, host: []const f32 };
pub const EmbeddingReplacement = struct {
    offset: usize,
    rows: usize,
    source: EmbeddingSource,
};

/// Borrowed rows from an actual projected-input owner. Counts and ordering are
/// semantic producer metadata; the source retains its original allocator or
/// backend owner until synchronous prompt assembly has consumed this view.
pub const ProjectedInputRows = struct {
    rows_per_item: []const usize,
    width: usize,
    source: EmbeddingSource,
};

/// Borrow a contiguous host producer without allocating an image-span array.
pub const EmbeddingReplacements = union(enum) {
    spans: []const EmbeddingReplacement,
    host_chunks: struct { values: []const f32, offsets: []const usize, rows_per_chunk: usize },
    fn count(self: @This(), width: usize) !usize {
        return switch (self) {
            .spans => |spans| spans.len,
            .host_chunks => |chunks| blk: {
                const elements = try std.math.mul(usize, try std.math.mul(usize, chunks.offsets.len, chunks.rows_per_chunk), width);
                if (elements != chunks.values.len) return error.InvalidEmbeddingAssembly;
                break :blk chunks.offsets.len;
            },
        };
    }
    fn at(self: @This(), index: usize, width: usize) EmbeddingReplacement {
        return switch (self) {
            .spans => |spans| spans[index],
            .host_chunks => |chunks| .{ .offset = chunks.offsets[index], .rows = chunks.rows_per_chunk, .source = .{ .host = chunks.values[index * chunks.rows_per_chunk * width ..][0 .. chunks.rows_per_chunk * width] } },
        };
    }
};

/// A single prompt destination for streaming image producers. The base is
/// copied once before image inference; source cleanup retains its existing
/// backend ownership/retirement contract, rather than a completion inference.
/// Unsupported image storage is materialized only for that source, BEFORE its
/// copy. Already submitted producer math and assembled prefixes never replay.
pub const PromptEmbeddingRows = struct {
    pub const ProjectionPlacement = enum { none, host, resident };

    /// Resolve the actual row operations BEFORE producer math. The producer
    /// supplies its bounded resident geometry and an explicit host request;
    /// task code never rediscovers backend/vtable placement after projection.
    pub fn projectionPlacement(backend: *const ComputeBackend, item_count: usize, resident_limit: usize, host_requested: bool) ProjectionPlacement {
        if (item_count == 0) return .none;
        if (!host_requested and item_count <= resident_limit and backend.supportsCopyRows2D() and
            backend.vtable.concatRows2D != null and backend.vtable.multiplyScalar != null) return .resident;
        return .host;
    }

    backend: *const ComputeBackend,
    allocator: std.mem.Allocator,
    rows: usize,
    width: usize,
    storage: union(enum) { resident: EmbeddingRows, host: []f32 },
    last_end: usize = 0,
    finished: bool = false,
    failed: bool = false,

    pub fn init(backend: *const ComputeBackend, allocator: std.mem.Allocator, base: ops.CT, rows: usize, width: usize, scale: f32) !@This() {
        if (rows == 0 or width == 0 or std.math.cast(i32, rows) == null or std.math.cast(i32, width) == null) return error.InvalidEmbeddingAssembly;
        const elements = try std.math.mul(usize, rows, width);
        // Known packed/opaque bases retain their original host route without
        // first allocating an unsupported speculative prompt destination.
        if (std.math.approxEqAbs(f32, scale, 1.0, 1e-6) and try EmbeddingRows.supportsSource(backend, base, rows, width)) {
            if (try EmbeddingRows.init(backend, allocator, rows, width)) |initial| {
                var assembly = initial;
                errdefer assembly.deinit();
                if (try assembly.acceptsSource(base, rows)) {
                    try assembly.append(base, 0, rows);
                    return .{ .backend = backend, .allocator = allocator, .rows = rows, .width = width, .storage = .{ .resident = assembly } };
                }
                assembly.deinit();
            }
        }
        const values = try backend.toFloat32(base, allocator);
        errdefer allocator.free(values);
        if (values.len != elements) return error.InvalidEmbeddingAssembly;
        // Exactly the original host scaling order and approximate-one guard.
        if (!std.math.approxEqAbs(f32, scale, 1.0, 1e-6)) for (values) |*value| {
            value.* *= scale;
        };
        return .{ .backend = backend, .allocator = allocator, .rows = rows, .width = width, .storage = .{ .host = values } };
    }
    pub fn deinit(self: *@This()) void {
        switch (self.storage) {
            .resident => |*assembly| assembly.deinit(),
            .host => |values| self.allocator.free(values),
        }
        self.storage = .{ .host = &.{} };
        self.finished = true;
    }
    pub fn replace(self: *@This(), replacement: EmbeddingReplacement) !void {
        if (self.finished or self.failed) return error.InvalidEmbeddingAssembly;
        errdefer self.failed = true;
        if (replacement.offset < self.last_end or replacement.offset > self.rows or replacement.rows > self.rows - replacement.offset) return error.InvalidEmbeddingAssembly;
        const count = try std.math.mul(usize, replacement.rows, self.width);
        if (replacement.source == .host and replacement.source.host.len != count) return error.InvalidEmbeddingAssembly;
        if (count == 0) {
            self.last_end = replacement.offset;
            return;
        }
        if (self.storage == .resident and replacement.source == .resident and try self.storage.resident.acceptsSource(replacement.source.resident, replacement.rows)) {
            // Dense same-namespace sources require no host touch or temporary.
            const destination = self.storage.resident.tensor orelse return error.InvalidEmbeddingAssembly;
            if (!try self.backend.copyRows2D(self.allocator, destination, replacement.offset, replacement.source.resident, 0, replacement.rows, self.width)) return error.UnsupportedBackendOp;
        } else {
            const values = switch (replacement.source) {
                .host => |value| value,
                .resident => |value| try self.backend.toFloat32(value, self.allocator),
            };
            defer if (replacement.source == .resident) self.allocator.free(values);
            if (values.len != count) return error.InvalidEmbeddingAssembly;
            switch (self.storage) {
                .host => |target| @memcpy(target[replacement.offset * self.width ..][0..count], values),
                .resident => |*assembly| {
                    // One explicit compatibility conversion for this image;
                    // no replay of vision math or earlier destination copies.
                    const shape = [_]i32{ @intCast(replacement.rows), @intCast(self.width) };
                    // A host F32 wrapper need not advertise a physical plane.
                    // Upload through the real current provider before applying
                    // the unchanged destination namespace/readability proof.
                    // Owned upload consumes the host wrapper on every path.
                    const plain = try self.backend.ensureDeviceResidentOwned(try self.backend.fromFloat32Shape(values, &shape));
                    defer self.backend.free(plain);
                    if (!try assembly.acceptsSource(plain, replacement.rows)) return error.UnsupportedBackendOp;
                    if (!try self.backend.copyRows2D(self.allocator, assembly.tensor.?, replacement.offset, plain, 0, replacement.rows, self.width)) return error.UnsupportedBackendOp;
                },
            }
        }
        self.last_end = replacement.offset + replacement.rows;
    }
    pub fn finish(self: *@This()) !ops.CT {
        if (self.finished or self.failed) return error.InvalidEmbeddingAssembly;
        errdefer self.failed = true;
        const result = switch (self.storage) {
            .resident => |*assembly| try assembly.take(),
            .host => |values| try self.backend.fromFloat32Shape(values, &.{ @intCast(self.rows), @intCast(self.width) }),
        };
        self.finished = true;
        return result;
    }
};

/// One-shot compatibility for genuine host producers and already available
/// resident spans. This shares the streaming owner's original host math.
pub fn replaceEmbeddingRows(backend: *const ComputeBackend, allocator: std.mem.Allocator, base: ops.CT, rows: usize, width: usize, scale: f32, replacements: EmbeddingReplacements) !ops.CT {
    const count = try replacements.count(width);
    var previous: usize = 0;
    for (0..count) |index| {
        const replacement = replacements.at(index, width);
        if (replacement.offset < previous or replacement.offset > rows or replacement.rows > rows - replacement.offset) return error.InvalidEmbeddingAssembly;
        if (replacement.source == .host and replacement.source.host.len != try std.math.mul(usize, replacement.rows, width)) return error.InvalidEmbeddingAssembly;
        previous = replacement.offset + replacement.rows;
    }
    var prompt = try PromptEmbeddingRows.init(backend, allocator, base, rows, width, scale);
    defer prompt.deinit();
    for (0..count) |index| try prompt.replace(replacements.at(index, width));
    return prompt.finish();
}

/// Owns decoder hidden rows on their actual backend through optional learned
/// projections. The only host conversion is the caller's requested final rows.
pub const ResidentHiddenRows = struct {
    backend: *const ComputeBackend,
    allocator: std.mem.Allocator,
    tensor: ?ops.CT,
    rows: usize,
    width: usize,

    pub fn fromTokens(backend: *const ComputeBackend, allocator: std.mem.Allocator, config: gpt_arch.Config, ids: []const i64) !ResidentHiddenRows {
        const hidden = try gpt_arch.hiddenForwardResident(backend, allocator, config, ids, 1, ids.len, null);
        return .{ .backend = backend, .allocator = allocator, .tensor = hidden, .rows = ids.len, .width = config.hidden_size };
    }
    /// Consumes owned_input on both success and failure, as the existing
    /// resident decoder does with its default (unbound) cleanup contract.
    pub fn fromEmbeddings(backend: *const ComputeBackend, allocator: std.mem.Allocator, config: gpt_arch.Config, owned_input: ops.CT, rows: usize) !ResidentHiddenRows {
        const hidden = try gpt_arch.hiddenForwardFromEmbeddingsResident(backend, allocator, config, owned_input, 1, rows, null, null);
        return .{ .backend = backend, .allocator = allocator, .tensor = hidden, .rows = rows, .width = config.hidden_size };
    }
    pub fn deinit(self: *@This()) void {
        if (self.tensor) |tensor| self.backend.free(tensor);
        self.tensor = null;
    }
    /// Metadata-only preparation: callers may select a legacy shape-query
    /// fallback here, before the separately submitted linear operation.
    pub fn projectionWidth(self: *const @This(), bias: ops.CT) !usize {
        // The original projection consumes an F32 host round-trip. Preserve
        // that precision contract when another decoder emits a packed dtype.
        if (try self.backend.tensorDType(self.tensor orelse return error.InvalidEmbeddingAssembly) != .f32) return error.UnsupportedTensorType;
        const dimensions = try self.backend.tensorShape(bias, self.allocator);
        defer self.allocator.free(dimensions);
        var width: usize = 1;
        for (dimensions) |dimension| {
            const size = std.math.cast(usize, dimension) orelse return error.InvalidProjectionShape;
            width = try std.math.mul(usize, width, size);
        }
        if (width == 0) return error.InvalidProjectionShape;
        return width;
    }
    pub fn project(self: *const @This(), weight: ops.CT, bias: ops.CT, width: usize) !ResidentHiddenRows {
        const input = self.tensor orelse return error.InvalidEmbeddingAssembly;
        if (width == 0) return error.InvalidProjectionShape;
        const projected = try self.backend.linear(input, weight, bias, self.rows, self.width, width);
        return .{ .backend = self.backend, .allocator = self.allocator, .tensor = projected, .rows = self.rows, .width = width };
    }
    pub fn exportHost(self: *const @This()) ![]f32 {
        const data = try self.backend.toFloat32(self.tensor orelse return error.InvalidEmbeddingAssembly, self.allocator);
        errdefer self.allocator.free(data);
        if (data.len != try std.math.mul(usize, self.rows, self.width)) return error.UnexpectedOutputShape;
        return data;
    }
};

pub const Output = struct {
    last_logits: ?[]f32 = null,
    greedy_token: ?usize = null,
    last_hidden: ?ops.CT = null,
    last_hidden_rows: usize = 0,
};

pub const ChunkRequest = struct {
    token_ids: []const i64,
    total_chunk_end: usize,
    prompt_tokens: usize,
    already_prefilled: usize,
    processed_rows: usize,
    output_intent: decoder_routes.PrefillOutputIntent,
    capture_hidden: bool,
    shared_kv_pruning: bool,
    prepared_metal_whole_prompt: bool,
    metal_target_last_logits: bool,
    cuda_direct_last_logits: bool,
    typed_outputs: bool,
    attempted_chunk_size: usize,
    allow_resize: bool,
    compact_selected: bool = false,
    selection_gpu_oracle: bool = false,
    exact_repeat_probe: bool = false,
    exact_repeat_calls: ?*usize = null,
};
pub const ChunkOutcome = union(enum) { complete: Output, retry_chunk_size: usize };

/// Physical chunk progression. Task adapters supply scheduler fairness limits
/// and publish progress; they cannot mutate admitted shapes or advance a failed
/// numerical attempt. The plan's immutable storage must outlive this cursor.
pub const ChunkCursor = struct {
    const Plan = runtime.scheduler.native_generate.PrefillChunkPlan;
    const Chunk = runtime.scheduler.native_generate.PrefillChunk;
    pub const Options = struct {
        query_tokens: usize,
        prefilled_tokens: usize,
        prompt_tokens: usize,
        initial_chunk_rows: usize = 1,
        admitted_plan: ?Plan = null,
        first_token_coalesced: bool = false,
        greedy_output: bool = false,
        capture_hidden: bool = false,
        execution_control: ?control.InferenceExecutionControl = null,
    };
    pub const Attempt = struct {
        nonce: u64,
        start: usize,
        end: usize,
        total_end: usize,
        chunk: Chunk,
        output_intent: decoder_routes.PrefillOutputIntent,
    };
    options: Options,
    current_chunk_size: usize,
    processed: usize = 0,
    plan_index: usize = 0,
    nonce: u64 = 0,
    pending: ?Attempt = null,
    plan_decision: PlanDecision = .none,
    uncoalesced_chunk_size: usize = 0,

    pub const Selection = struct {
        configured_plan: ?Plan = null,
        leased_plan: ?Plan = null,
        configured_chunk_rows: usize = 0,
        scheduler_chunk_rows: usize = 0,
        selected_kv: bool = false,
        checkpoint_resume: bool = false,
        backend: ops.BackendKind,
        pure_greedy: bool,
        resident_greedy_allowed: bool,
        first_token_enabled: bool,
        coalesce_token_limit: usize,
    };
    pub const PlanDecision = union(enum) {
        none,
        execute: Plan,
        bypassed_prompt_cache: Plan,
        bypassed_geometry: Plan,
    };

    /// Resolve the physical plan once before any chunk/KV mutation. Scheduler
    /// and task configuration become primitive request facts at this boundary.
    pub fn select(request: Options, selection: Selection) !ChunkCursor {
        if (selection.configured_plan != null and selection.leased_plan != null and selection.configured_plan.?.fingerprint != selection.leased_plan.?.fingerprint) return error.InvalidPrefillChunkPlan;
        const borrowed = if (selection.selected_kv) null else selection.configured_plan orelse selection.leased_plan;
        var decision: PlanDecision = .none;
        var admitted: ?Plan = null;
        if (borrowed) |plan| {
            try plan.validate();
            if (request.prefilled_tokens != 0 and !selection.checkpoint_resume) {
                decision = .{ .bypassed_prompt_cache = plan };
            } else if (plan.prompt_tokens != request.query_tokens) {
                if (selection.checkpoint_resume) return error.InvalidPrefillChunkPlan;
                decision = .{ .bypassed_geometry = plan };
            } else {
                decision = .{ .execute = plan };
                admitted = plan;
            }
        }
        var rows = if (admitted) |plan| plan.max_chunk_rows else blk: {
            if (selection.configured_chunk_rows > 0 and selection.scheduler_chunk_rows > 0) break :blk @min(selection.configured_chunk_rows, selection.scheduler_chunk_rows);
            if (selection.configured_chunk_rows > 0) break :blk selection.configured_chunk_rows;
            if (selection.scheduler_chunk_rows > 0) break :blk selection.scheduler_chunk_rows;
            break :blk request.query_tokens;
        };
        rows = @max(@min(rows, request.query_tokens), 1);
        if (selection.selected_kv) rows = @min(rows, runtime.kv.selection.defaults.prefill_chunk_rows);
        const uncoalesced = rows;
        if (borrowed == null and !selection.selected_kv) rows = decoder_routes.coalescedPrefillChunkSizeForFirstToken(selection.backend, selection.pure_greedy, !selection.resident_greedy_allowed, selection.first_token_enabled, request.query_tokens, rows, selection.coalesce_token_limit);
        var options = request;
        options.admitted_plan = admitted;
        options.initial_chunk_rows = rows;
        options.first_token_coalesced = rows > uncoalesced;
        // admitted was fully validated above; do not scan its chunks twice.
        var result = initValidated(options);
        result.plan_decision = decision;
        result.uncoalesced_chunk_size = uncoalesced;
        return result;
    }

    pub fn init(options: Options) !ChunkCursor {
        if (options.admitted_plan) |plan| {
            try plan.validate();
            if (plan.prompt_tokens != options.query_tokens) return error.InvalidPrefillChunkPlan;
        }
        return initValidated(options);
    }
    fn initValidated(options: Options) ChunkCursor {
        return .{ .options = options, .current_chunk_size = @max(options.initial_chunk_rows, 1) };
    }
    pub fn next(self: *ChunkCursor, scheduler_chunk: usize) !?Attempt {
        if (self.pending != null) return error.PrefillChunkInFlight;
        if (self.processed == self.options.query_tokens) return null;
        if (self.options.execution_control) |active| try active.check();
        if (self.processed > self.options.query_tokens) return error.InvalidPrefillChunkState;
        const chunk = if (self.options.admitted_plan) |plan| blk: {
            if (self.plan_index >= plan.chunks.len) return error.InvalidPrefillChunkPlan;
            break :blk plan.chunks[self.plan_index];
        } else Chunk{ .rows = @max(@min(self.current_chunk_size, decoder_routes.schedulerChunkForPrefillIteration(scheduler_chunk, self.current_chunk_size, self.options.first_token_coalesced)), 1) };
        const proposed = std.math.add(usize, self.processed, chunk.rows) catch return error.InvalidPrefillChunkState;
        const end = @min(self.options.query_tokens, proposed);
        if (self.options.admitted_plan != null and end - self.processed != chunk.rows) return error.InvalidPrefillChunkPlan;
        const total_end = std.math.add(usize, self.options.prefilled_tokens, end) catch return error.InvalidPrefillChunkState;
        const nonce = std.math.add(u64, self.nonce, 1) catch return error.InvalidPrefillChunkState;
        const attempt = Attempt{ .nonce = nonce, .start = self.processed, .end = end, .total_end = total_end, .chunk = chunk, .output_intent = decoder_routes.prefillOutputIntent(total_end == self.options.prompt_tokens, self.options.greedy_output, self.options.capture_hidden) };
        self.pending = attempt;
        self.nonce = nonce;
        return attempt;
    }
    fn validateAttempt(self: *const ChunkCursor, attempt: Attempt) !void {
        const pending = self.pending orelse return error.PrefillChunkAlreadyConsumed;
        if (!std.meta.eql(pending, attempt) or attempt.start != self.processed) return error.InvalidPrefillChunkState;
    }
    pub fn commit(self: *ChunkCursor, attempt: Attempt) !void {
        try self.validateAttempt(attempt);
        self.processed = attempt.end;
        if (self.options.admitted_plan != null) self.plan_index += 1;
        self.pending = null;
    }
    pub fn retry(self: *ChunkCursor, attempt: Attempt, smaller: usize) !void {
        try self.validateAttempt(attempt);
        if (self.options.admitted_plan != null) return error.InvalidPrefillChunkPlan;
        if (smaller == 0 or smaller >= attempt.chunk.rows) return error.InvalidPrefillChunkRetry;
        self.current_chunk_size = smaller;
        self.pending = null;
    }
    pub fn retryMemoryBudgetExceeded(self: *ChunkCursor, attempt: Attempt, err: anyerror) !bool {
        try self.validateAttempt(attempt);
        if (self.options.admitted_plan != null or err != error.MemoryBudgetExceeded or attempt.chunk.rows <= 1) return false;
        try self.retry(attempt, @max(attempt.chunk.rows / 2, 1));
        return true;
    }
    /// Cancelling numerical work does not publish progress or resize an
    /// admitted plan. The numerical owner separately owns its KV rollback.
    pub fn cancel(self: *ChunkCursor, attempt: Attempt) !void {
        if (self.pending == null) return;
        try self.validateAttempt(attempt);
        self.pending = null;
    }
    pub fn finish(self: *const ChunkCursor) !void {
        if (self.pending != null or self.processed != self.options.query_tokens) return error.InvalidPrefillChunkState;
        if (self.options.admitted_plan) |plan| if (self.plan_index != plan.chunks.len) return error.InvalidPrefillChunkPlan;
    }
};

fn classifyChunkOutcome(result: ChunkOutcome) @import("invocation_observation.zig").ResultClassification {
    return switch (result) {
        .complete => .{},
        .retry_chunk_size => .{ .error_name = "MemoryBudgetExceeded" },
    };
}

/// Semantic output needs and scheduler fairness are borrowed request facts.
/// The physical decoder owns route, chunk/window and representation choices.
/// No tokenizer/task object, callback or numerical token is retained here.
pub const PromptRequest = struct {
    route: decoder_routes.Request,
    prompt_tokens: usize,
    query_tokens: usize,
    prefilled_tokens: usize,
    allow_resident_greedy: bool,
    capture_hidden: bool,
    selected_kv: bool,
    checkpoint: bool,
    has_draft: bool,
    has_prompt_cache: bool,
    scheduler_peers: bool,
    execution_serialized: bool,
    configured_chunk_rows: usize,
    scheduler_chunk_rows: usize,
    speculative_k: u32,
    sampled_ring_qualified: bool,
};

pub const PromptPlan = struct {
    exact_repeat_probe: bool,
    cuda_greedy: bool,
    metal_greedy: bool,
    metal_last_logits: bool,
    typed_outputs: bool,
    shared_kv: bool,
    cuda_last_logits: bool,
    compiled_whole_model: bool,
    pub fn greedy(self: PromptPlan) bool {
        return self.cuda_greedy or self.metal_greedy;
    }
};

pub const CompiledPromptPlan = struct { chunk_rows: usize, bounded_swa: bool };

pub fn firstTokenEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN", true);
}
pub fn greedyTokenEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_PREFILL_GREEDY_TOKEN", true);
}
pub fn firstTokenCoalesceLimit() usize {
    return platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN_COALESCE_TOKENS") orelse 2048;
}

pub const Runtime = struct {
    inline fn acquireForObservation(self: *Runtime) !?@import("../ops/execution_scope.zig").Scope {
        return self.execution.acquireScope() catch |err| {
            @import("invocation_observation.zig").recordContextAcquisitionFailure(&self.forward.context, .prefill, err);
            return err;
        };
    }

    forward: decoder_forward.Runtime,
    state: *graph_mod.decode_state_runtime.NativeDecodeState,
    execution: control.ExecutionAccess = .{},
    setup_execution: control.ExecutionAccess = .{},
    work_control: runtime.kv.attention_match.Control = .{},

    /// Resolve from the actual loaded backend/model/cache, not task-selected
    /// backend labels. The request retains only semantic and scheduling facts.
    pub fn planPrompt(self: *const Runtime, request: PromptRequest) !PromptPlan {
        const context = self.forward.context;
        const backend = context.cb.kind();
        const model = context.gpt_config;
        const typed = self.forward.graph_cache == null and context.compiled_partition_backend == null;
        const fit_probe = platform.env.getenvBool("ANTFLY_INFERENCE_ATTENTION_MATCH_FIT_PROBE");
        const exact = platform.env.getenvBool("ANTFLY_INFERENCE_ATTENTION_MATCH_EXACT_PREFILL_PROBE");
        if (exact and (!build_options.enable_cuda or backend != .cuda or context.kv_dtype != .f16 or
            model.family != .gemma or !model.usesGemma4Channels() or !fit_probe or !typed or
            request.has_draft or request.has_prompt_cache or request.capture_hidden or request.prefilled_tokens != 0 or
            request.prompt_tokens > 32768 or !self.state.isPaged() or request.selected_kv or decoder_forward.cudaReplayLastLogitsEnabled()))
            return error.UnsupportedCompactionQueryRoute;
        const cuda_greedy = !request.checkpoint and !fit_probe and
            (!request.execution_serialized or !request.scheduler_peers) and
            decoder_routes.shouldUseCudaPrefillGreedyToken(backend, request.route, request.allow_resident_greedy, model.suppressTokenIds().len > 0, greedyTokenEnabled());
        const metal_greedy = backend == .metal and decoder_routes.metalPreparedPrefillHiddenCaptureEnabled() and
            request.capture_hidden and request.allow_resident_greedy and request.route.storage.pure_greedy and !request.route.storage.grammar;
        return .{
            .exact_repeat_probe = exact,
            .cuda_greedy = cuda_greedy,
            .metal_greedy = metal_greedy,
            .metal_last_logits = decoder_routes.shouldUseMetalGemmaTargetLastLogits(backend, model, request.capture_hidden),
            .typed_outputs = typed,
            .shared_kv = typed and !request.checkpoint and !request.capture_hidden and request.route.exact_hybrid,
            .cuda_last_logits = typed and backend == .cuda,
            .compiled_whole_model = !request.selected_kv and context.compiled_partition_backend != null and
                context.compiled_attachment_target == .whole_model and self.forward.graph_cache != null,
        };
    }

    /// Own the complete whole-model physical window before its first chunk.
    /// Scheduler turn/progress publication remains in the caller.
    pub fn prepareCompiledPrompt(self: *Runtime, request: PromptRequest) !CompiledPromptPlan {
        if (request.prefilled_tokens != 0) return error.UnsupportedShape;
        const context = self.forward.context;
        const route: decoder_storage.Route = if (context.compiled_partition_backend == .metal and context.cb.kind() == .metal and self.state.isPaged() and context.kv_dtype != null) .metal_whole_model else .standard;
        const policy = decoder_storage.routeCapacityPolicy(route, context.gpt_config.*, request.route.storage, context.kv_dtype orelse .f16, request.sampled_ring_qualified);
        const bounded = policy == .split_swa_ring;
        const rows = decoder_routes.wholeModelExecutionPrefillChunkSize(context.compiled_partition_backend, request.configured_chunk_rows, request.scheduler_chunk_rows, request.query_tokens, bounded);
        const speculative = @min(@as(usize, @intCast(request.speculative_k)), @as(usize, runtime.tier.memory.generation_max_speculative_k)) + 1;
        self.state.configureKvMaxInflightTokens(@max(rows, speculative), bounded);
        return .{ .chunk_rows = rows, .bounded_swa = bounded };
    }

    pub fn prepareChunkWindow(self: *Runtime, request: PromptRequest, chunk_rows: usize) void {
        const speculative = @min(@as(usize, @intCast(request.speculative_k)), 16) + 1;
        self.state.configureKvMaxInflightTokens(@max(chunk_rows, speculative), self.forward.context.cb.kind() == .metal and
            request.prefilled_tokens == 0 and decoder_storage.ringRequestEligible(request.route.storage, request.sampled_ring_qualified));
    }

    /// Post-forward selected-KV maintenance is shared by eager and scheduled
    /// chunks. The task supplies its requested cache semantics; only this
    /// runtime owns the donor-compaction decision and actual state mutation.
    /// Call after the numerical chunk completed, before publishing its cursor.
    pub fn finishChunkState(self: *Runtime, selected_kv: bool, gpu_oracle: bool) !void {
        if (selected_kv) try self.state.compactSelectedGlobalDonors(self.forward.context.gpt_config.*, self.forward.context.execution_control, gpu_oracle);
    }

    fn checkWork(raw: ?*anyopaque) !void {
        const self: *Runtime = @ptrCast(@alignCast(raw.?));
        try self.work_control.check();
    }

    pub fn runChunk(self: *Runtime, request: ChunkRequest) !ChunkOutcome {
        var execution_scope = try self.acquireForObservation();
        defer if (execution_scope) |*scope| scope.deinit();
        return @import("invocation_observation.zig").callContextOperationClassified(&self.forward.context, .prefill, classifyChunkOutcome, @This().runChunkUnobserved, .{ self, request });
    }

    fn runChunkUnobserved(self: *Runtime, request: ChunkRequest) !ChunkOutcome {
        var repeat: if (build_options.enable_cuda) ?cuda_compute.CudaCompute.AttentionMatchRepeatScope else ?void = null;
        if (comptime build_options.enable_cuda) {
            if (request.exact_repeat_probe and request.token_ids.len > 1) {
                if (request.token_ids.len > 512) return error.UnsupportedCompactionQueryRoute;
                const compute: *cuda_compute.CudaCompute = @ptrCast(@alignCast(self.forward.context.cb.ptr));
                repeat = try compute.beginAttentionMatchRepeat(true, request.token_ids.len);
                compute.kernels.attention_match_repeat_check_ctx = self;
                compute.kernels.attention_match_repeat_check_fn = checkWork;
            }
        }
        defer if (comptime build_options.enable_cuda) {
            if (repeat) |*scope| {
                if (request.exact_repeat_calls) |calls| calls.* += scope.compute.kernels.attention_match_repeat_split_launches;
                scope.deinit();
            }
        };
        const allocator = self.forward.context.allocator;
        const chunk = request.token_ids;
        const total_chunk_end = request.total_chunk_end;
        const seq_len = request.prompt_tokens;
        const prefilled_tokens = request.already_prefilled;
        const processed = request.processed_rows;
        const output_intent = request.output_intent;
        const capture_last_hidden = request.capture_hidden;
        const allow_shared_kv_prefill = request.shared_kv_pruning;
        const use_metal_target_last_logits = request.metal_target_last_logits;
        const prefer_cuda_direct_last_logits = request.cuda_direct_last_logits;
        const can_use_typed_prefill_outputs = request.typed_outputs;
        var decode_runtime = BorrowedDecodeStateRuntime.init(self.state);
        var resized_chunk_size = request.attempted_chunk_size;
        var prepared_runtime = Forward{ .forward = self.forward };
        var prefill_last_logits: ?[]f32 = null;
        var prefill_greedy_token: ?usize = null;
        var prefill_last_hidden: ?ops.CT = null;
        var prefill_last_hidden_rows: usize = 0;
        errdefer if (prefill_last_logits) |values| allocator.free(values);
        errdefer if (prefill_last_hidden) |tensor| self.forward.context.cb.free(tensor);
        try decode_runtime.appendPrefillChunkWithCleanup(chunk.len, .{ .boundary = self.forward.context.cleanupEpoch(), .operation = self.forward.context.cb.operationToken() });
        const decode_context = decode_runtime.makeDecodeContext(total_chunk_end, chunk.len);
        // Shared-KV pruning is only valid for these explicit output
        // intents. Checkpoint fitting collects queries from every
        // consumer; hidden-row/MTP consumers also retain the full path.
        const shared_prefill = if (allow_shared_kv_prefill and
            (output_intent == .kv_only or output_intent == .last_logits or output_intent == .greedy_token))
            try gpt_arch.tryCudaSharedKvPrefill(self.forward.context.cb, allocator, self.forward.context.gpt_config.*, chunk, total_chunk_end, &decode_context, output_intent != .kv_only)
        else
            null;
        const metal_prepared: ?Output = if (shared_prefill == null and request.prepared_metal_whole_prompt)
            try prepared_runtime.tryMetalPreparedTailPrefillGreedy(chunk, total_chunk_end, &decode_context, capture_last_hidden)
        else
            null;
        if (shared_prefill) |prepared| {
            prefill_last_logits = prepared.last_logits;
            debugGenerationStage("executePrefill shared_kv_pruned rows={d} output={s}", .{ chunk.len, @tagName(output_intent) });
        } else if (metal_prepared) |prepared| {
            prefill_last_logits = prepared.last_logits;
            prefill_greedy_token = prepared.greedy_token;
            prefill_last_hidden = prepared.last_hidden;
            prefill_last_hidden_rows = prepared.last_hidden_rows;
        } else if (output_intent == .greedy_token or output_intent == .greedy_token_and_hidden) {
            if (try prepared_runtime.tryCudaPreparedTailPrefillGreedy(chunk, total_chunk_end, &decode_context, capture_last_hidden)) |prepared| {
                prefill_greedy_token = prepared.greedy_token;
                prefill_last_hidden = prepared.last_hidden;
                prefill_last_hidden_rows = prepared.last_hidden_rows;
            } else if (capture_last_hidden) {
                var greedy_hidden = gpt_arch.forwardGreedyLastTokenWithFinalHidden(self.forward.context.cb, allocator, self.forward.context.gpt_config.*, chunk, 1, total_chunk_end, &decode_context) catch |err| {
                    if (try retryAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, request.attempted_chunk_size, &resized_chunk_size, request.allow_resize, err)) return .{ .retry_chunk_size = resized_chunk_size };
                    return err;
                };
                prefill_greedy_token = greedy_hidden.token_id;
                if (greedy_hidden.token_tensor) |token| {
                    self.forward.context.cb.free(token);
                    greedy_hidden.token_tensor = null;
                }
                prefill_last_hidden = greedy_hidden.hidden;
                prefill_last_hidden_rows = greedy_hidden.rows;
            } else {
                prefill_greedy_token = gpt_arch.forwardGreedyLastToken(self.forward.context.cb, allocator, self.forward.context.gpt_config.*, chunk, 1, total_chunk_end, &decode_context) catch |err| {
                    if (try retryAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, request.attempted_chunk_size, &resized_chunk_size, request.allow_resize, err)) return .{ .retry_chunk_size = resized_chunk_size };
                    return err;
                };
            }
            debugGenerationStage(
                "executePrefill captured greedy token={d}",
                .{prefill_greedy_token.?},
            );
        } else if (output_intent == .last_logits_and_hidden or
            (output_intent == .last_logits and use_metal_target_last_logits))
        {
            var target_hidden = self.forward.forwardFinalHiddenDevice(
                chunk,
                1,
                total_chunk_end,
                prefilled_tokens,
                &decode_context,
            ) catch |err| {
                if (try retryAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, request.attempted_chunk_size, &resized_chunk_size, request.allow_resize, err)) return .{ .retry_chunk_size = resized_chunk_size };
                return err;
            };
            var keep_target_hidden = false;
            defer {
                if (!keep_target_hidden) self.forward.context.cb.free(target_hidden.hidden);
                if (target_hidden.pre_norm_hidden) |pre_norm| self.forward.context.cb.free(pre_norm);
                if (target_hidden.prepared_tail_choices) |choices| target_hidden.choices_allocator.?.free(choices);
            }
            const last_hidden = try self.forward.context.cb.sliceRows2D(
                allocator,
                target_hidden.hidden,
                target_hidden.rows - 1,
                1,
                self.forward.context.gpt_config.*.hidden_size,
            );
            defer self.forward.context.cb.free(last_hidden);
            const lm_w = try gpt_arch.getLmHeadWeight(self.forward.context.cb, self.forward.context.gpt_config.*);
            defer self.forward.context.cb.free(lm_w);
            const last_logits_device = try self.forward.context.cb.linearNoBias(
                last_hidden,
                lm_w,
                1,
                self.forward.context.gpt_config.*.hidden_size,
                self.forward.context.gpt_config.*.vocab_size,
            );
            defer self.forward.context.cb.free(last_logits_device);
            const last_logits = try self.forward.context.cb.toFloat32(last_logits_device, allocator);
            gpt_arch.applyFinalLogitSoftcapInPlace(self.forward.context.gpt_config.*, last_logits);
            prefill_last_logits = last_logits;
            if (capture_last_hidden) {
                prefill_last_hidden = target_hidden.hidden;
                prefill_last_hidden_rows = target_hidden.rows;
                keep_target_hidden = true;
            }
            if (capture_last_hidden) {
                debugGenerationStage(
                    "executePrefill captured final logits and MTP hidden vocab_size={d}",
                    .{self.forward.context.gpt_config.*.vocab_size},
                );
            } else {
                debugGenerationStage(
                    "executePrefill captured target final-row logits vocab_size={d}",
                    .{self.forward.context.gpt_config.*.vocab_size},
                );
            }
        } else if (output_intent == .last_logits and prefer_cuda_direct_last_logits) {
            prefill_last_logits = self.forward.forwardLastLogits(chunk, 1, total_chunk_end, &decode_context) catch |err| {
                if (try retryAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, request.attempted_chunk_size, &resized_chunk_size, request.allow_resize, err)) return .{ .retry_chunk_size = resized_chunk_size };
                return err;
            };
            debugGenerationStage(
                "executePrefill captured CUDA final-row logits vocab_size={d}",
                .{self.forward.context.gpt_config.*.vocab_size},
            );
        } else if (output_intent == .kv_only and can_use_typed_prefill_outputs) {
            const hidden_only_result = if (self.forward.context.cb.kind() == .cuda and cudaReplayLastLogitsEnabled())
                gpt_arch.forwardHiddenOnlyWithCudaReplay(
                    self.forward.context.cb,
                    allocator,
                    self.forward.context.gpt_config.*,
                    chunk,
                    1,
                    total_chunk_end,
                    &decode_context,
                    "gpt.prefill_hidden_only",
                )
            else
                gpt_arch.forwardHiddenOnly(
                    self.forward.context.cb,
                    allocator,
                    self.forward.context.gpt_config.*,
                    chunk,
                    1,
                    total_chunk_end,
                    &decode_context,
                );
            hidden_only_result catch |err| {
                if (try retryAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, request.attempted_chunk_size, &resized_chunk_size, request.allow_resize, err)) return .{ .retry_chunk_size = resized_chunk_size };
                return err;
            };
            debugGenerationStage(
                "executePrefill chunk complete processed={d} total_chunk_end={d} output=kv_only",
                .{ processed, total_chunk_end },
            );
        } else {
            const logits = self.forwardAllLogits(chunk, 1, total_chunk_end, &decode_context) catch |err| {
                if (try retryAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, request.attempted_chunk_size, &resized_chunk_size, request.allow_resize, err)) return .{ .retry_chunk_size = resized_chunk_size };
                return err;
            };
            defer allocator.free(logits);
            debugGenerationStage(
                "executePrefill chunk complete processed={d} total_chunk_end={d} logits_len={d}",
                .{ processed, total_chunk_end, logits.len },
            );
            if (total_chunk_end == seq_len) {
                prefill_last_logits = try allocator.dupe(f32, logits[(chunk.len - 1) * self.forward.context.gpt_config.*.vocab_size ..][0..self.forward.context.gpt_config.*.vocab_size]);
                debugGenerationStage(
                    "executePrefill captured last logits vocab_size={d}",
                    .{self.forward.context.gpt_config.*.vocab_size},
                );
            }
        }
        // The chunk's KV writes and query captures are committed at
        // this point. Selection never runs inside an active forward.
        try self.finishChunkState(request.compact_selected, request.selection_gpu_oracle);
        return .{ .complete = .{ .last_logits = prefill_last_logits, .greedy_token = prefill_greedy_token, .last_hidden = prefill_last_hidden, .last_hidden_rows = prefill_last_hidden_rows } };
    }

    pub const CompiledChunk = struct {
        token_ids: []const i64,
        total_sequence_length: usize,
        output: enum { kv_only, last_logits, greedy_token },
    };

    /// The whole-model runtime owns physical KV/cache storage. This owner
    /// advances matching logical coordination state and retires ModelOutput
    /// under the caller's existing execution/provider capability. An error
    /// after the runtime call propagates; no eager/serial replay is attempted.
    pub fn runCompiledChunk(self: *Runtime, request: CompiledChunk) !Output {
        return self.runCompiledChunkWith(request, graph_mod.execution.graphForwardCompiledModelOutput);
    }

    fn runCompiledChunkWith(self: *Runtime, request: CompiledChunk, comptime forward_fn: anytype) !Output {
        const cache = self.forward.graph_cache orelse {
            @import("invocation_observation.zig").recordContextAcquisitionFailure(&self.forward.context, .prefill, error.MissingCompiledModelRuntime);
            return error.MissingCompiledModelRuntime;
        };
        var execution_scope = try self.acquireForObservation();
        defer if (execution_scope) |*scope| scope.deinit();
        return @import("invocation_observation.zig").callContextOperation(&self.forward.context, .prefill, @This().runCompiledChunkWithOwnedLane, .{ self, request, cache, forward_fn });
    }

    fn runCompiledChunkWithOwnedLane(self: *Runtime, request: CompiledChunk, cache: *graph_mod.cache.GraphCache, comptime forward_fn: anytype) !Output {
        const allocator = self.forward.context.allocator;
        var logical = BorrowedDecodeStateRuntime.init(self.state);
        const context = try logical.preparePrefillWithCleanup(request.total_sequence_length, request.token_ids.len, .{ .boundary = self.forward.context.cleanupEpoch(), .operation = self.forward.context.cb.operationToken() });
        const cleanup_operation = self.forward.context.cb.operationToken();
        const supplied_cleanup = self.forward.context.cleanupEpoch();
        var inherited = if (supplied_cleanup == null and cleanup_operation != null) try cleanup_operation.?.retainCleanupEpoch() else null;
        defer if (inherited) |*epoch| epoch.release();
        const cleanup = @import("cleanup_context.zig").Context{ .boundary = supplied_cleanup orelse if (inherited) |*epoch| epoch else null, .operation = cleanup_operation };
        var output = (try forward_fn(&self.forward.context, cache, request.token_ids, 1, request.total_sequence_length, &context)) orelse return error.MissingCompiledModelRuntime;
        defer output.deinitWithCleanup(allocator, cleanup);
        return switch (request.output) {
            .kv_only => .{},
            .last_logits => .{ .last_logits = try output.takeHostLogitsWithCleanup(allocator, cleanup) },
            .greedy_token => blk: {
                const token = try output.greedyTokenWithCleanup(allocator, self.forward.context.gpt_config.vocab_size, cleanup);
                if (token < 0) return error.InvalidModelOutput;
                break :blk .{ .greedy_token = @intCast(token) };
            },
        };
    }

    fn forwardAllLogits(self: *Runtime, ids: []const i64, batch: usize, total: usize, decode: *const gpt_arch.DecodeContext) ![]f32 {
        var fallback_count: u64 = 0;
        return graph_mod.decoder_step_executor.forwardAll(&self.forward.context, self.forward.graph_cache, ids, batch, total, decode, self.forward.replay_fallbacks orelse &fallback_count);
    }
    pub const PromptStateRequest = struct {
        prompt_tokens: usize,
        generated_tokens: i32,
        fresh_cuda_capacity: bool = false,
        prefilled_tokens: usize = 0,
        prompt_cache_enabled: bool = false,
        has_prompt_cache: bool = false,
        has_draft: bool = false,
        speculation_requested: bool = false,
    };

    /// Preserve the existing capacity route and recurrent reset order. Metadata
    /// preparation is separate from model execution lock acquisition because
    /// prepared embedding callers already hold a scheduler turn at this phase.
    pub fn preparePromptState(self: *Runtime, request: PromptStateRequest) !void {
        const config = self.forward.context.gpt_config.*;
        const cb = self.forward.context.cb;
        if (request.fresh_cuda_capacity and cb.kind() == .cuda and self.forward.context.kv_dtype == .f16 and
            !request.has_prompt_cache and !request.prompt_cache_enabled and !request.has_draft and
            !request.speculation_requested and request.prefilled_tokens == 0 and self.state.total_tokens == 0 and
            self.state.sequence_id == null and self.state.isPaged() and self.state.kvSlidingWindowTokens() == null and
            self.state.cudaPagedKvReplayCapacityTokens() == null and
            platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_RESERVE_PREFILL_KV", true))
        {
            if (decoder_routes.cudaE4bPrefillKvCapacity(config, request.prompt_tokens, request.generated_tokens)) |capacity| {
                var execution_scope = try self.setup_execution.acquireScope();
                defer if (execution_scope) |*scope| scope.deinit();
                const limit = std.mem.alignForward(usize, request.prompt_tokens + @as(usize, @intCast(request.generated_tokens)), 16);
                try self.state.reserveFreshCudaKvCapacity(capacity, limit);
                std.log.info("cuda_prefill_kv_reservation prompt_tokens={d} capacity_tokens={d}", .{ request.prompt_tokens, capacity });
            }
        }
        try self.resetRecurrentPrompt();
    }

    pub fn resetRecurrentPrompt(self: *Runtime) !void {
        const config = self.forward.context.gpt_config.*;
        if (config.isQwen35() and self.state.isPaged()) {
            try self.state.ensureQwen35LinearCache(config);
            self.state.resetQwen35LinearCache();
        }
    }

    pub fn prepareWholePrompt(self: *Runtime, sequence_length: usize) !void {
        var state = BorrowedDecodeStateRuntime.init(self.state);
        _ = try state.preparePrefillWithCleanup(sequence_length, sequence_length, .{ .boundary = self.forward.context.cleanupEpoch(), .operation = self.forward.context.cb.operationToken() });
    }
    pub const EmbeddingRequest = struct {
        /// Ownership transfers under the execution lane, after successful
        /// acquisition. Failed admission/locking leaves the original slot live.
        owner: *?ops.CT,
        token_ids: []const i64,
        sequence_length: usize,
        capture_hidden: bool = false,
        ensure_device_resident: bool = true,
        route: enum { prepared_last_row, ordinary_last_row } = .prepared_last_row,
        attention_mask: ?[]const u8 = null,
        positions: ?[]const u32 = null,
        visual_mask: ?[]const bool = null,
        deepstack_embeddings: ?[]const f32 = null,
        deepstack_layer_count: usize = 0,
    };

    /// The caller owns its admitted scheduler turn. Physical state was prepared
    /// before acquiring the model lane, preserving prepared-prefix lock order.
    pub fn runEmbeddingsOwned(self: *Runtime, request: EmbeddingRequest) !Output {
        var execution_scope = try self.acquireForObservation();
        defer if (execution_scope) |*scope| scope.deinit();
        return @import("invocation_observation.zig").callContextOperation(&self.forward.context, .prefill, @This().runEmbeddingsOwnedUnobserved, .{ self, request });
    }

    fn runEmbeddingsOwnedUnobserved(self: *Runtime, request: EmbeddingRequest) !Output {
        const original = request.owner.* orelse return error.InvalidPreparedPrompt;
        request.owner.* = null;
        const cb = self.forward.context.cb;
        const embeddings = if (request.ensure_device_resident) try cb.ensureDeviceResidentOwned(original) else original;
        var owned = true;
        errdefer if (owned) cb.free(embeddings);
        var state = BorrowedDecodeStateRuntime.init(self.state);
        var decode = state.makeDecodeContext(request.sequence_length, request.sequence_length);
        decode.attn_or_mask = request.attention_mask;
        decode.mrope_positions = request.positions;
        decode.qwen3vl_visual_mask = request.visual_mask;
        decode.qwen3vl_deepstack_embeddings = request.deepstack_embeddings;
        decode.qwen3vl_deepstack_layer_count = request.deepstack_layer_count;
        var numerical = Forward{ .forward = self.forward };
        owned = false;
        return switch (request.route) {
            .prepared_last_row => try numerical.forwardPreparedEmbeddingsLastLogits(embeddings, request.token_ids, request.sequence_length, &decode, request.capture_hidden),
            .ordinary_last_row => .{ .last_logits = try gpt_arch.forwardLastLogitsLastRowFromEmbeddingsWithLayer0Overrides(cb, self.forward.context.allocator, self.forward.context.gpt_config.*, embeddings, .{}, 1, request.sequence_length, &decode, null) },
        };
    }
};
pub const PositionedEmbeddingBatchItem = struct {
    diagnostics_binding: ?@import("invocation_observation.zig").Binding = null,
    request_id: ?u64 = null,
    allocator: std.mem.Allocator,
    backend: *const ComputeBackend,
    model: *const @import("../models/gpt.zig").Config,
    state: *graph_mod.decode_state_runtime.NativeDecodeState,
    embeddings_owner: *?ops.CT,
    geometry_tokens: usize,
    position_ids: []const u32,
    deepstack_layer_count: usize,
    sequence_length: usize,
};

pub fn forwardPositionedEmbeddingBatch(items: []const PositionedEmbeddingBatchItem) ![][]f32 {
    if (items.len == 0) return forwardPositionedEmbeddingBatchUnobserved(items);
    const observation = @import("shared_batch_observation.zig");
    return observation.call(observation.positionedBinding(items), items[0].backend.*, .positioned_prefill, items, describePositionedParticipant, forwardPositionedEmbeddingBatchUnobserved, .{items});
}
fn describePositionedParticipant(item: PositionedEmbeddingBatchItem) @import("diagnostics.zig").Batch.Participant {
    return .{ .request_id = item.request_id, .phase = .prefill, .query_rows = item.sequence_length, .total_rows = item.sequence_length };
}
fn forwardPositionedEmbeddingBatchUnobserved(items: []const PositionedEmbeddingBatchItem) ![][]f32 {
    const capacity = if (platform.env.getenvBool("ANTFLY_LIGHTONOCR3_CUDA_WIDE_MICROBATCHES")) decoder_routes.max_positioned_prefill_items else decoder_routes.default_positioned_prefill_items;
    if (items.len == 0 or items.len > capacity) return error.InvalidBatchDecodeState;
    const first = items[0];
    const cb = first.backend;
    const a = first.allocator;
    const seq = first.sequence_length;
    const cfg = first.model.*;
    if (seq == 0 or !cfg.hasQwen35VisionArchitectureContract()) return error.UnsupportedBatchedPrefill;
    const total = try std.math.mul(usize, items.len, seq);
    if (try decoder_routes.positionedPrefillBatchLimitForBackend(cb.kind(), items.len, seq) != items.len) return error.UnsupportedBatchedPrefill;
    const positions = try a.alloc(u32, try std.math.mul(usize, total, 3));
    defer a.free(positions);
    var mixed: [decoder_routes.max_positioned_prefill_items]graph_mod.decode_batch.Item = undefined;
    var reserved: [decoder_routes.max_positioned_prefill_items]usize = @splat(0);
    var prepared_count: usize = 0;
    errdefer for (0..prepared_count) |index| {
        var decode = BorrowedDecodeStateRuntime.init(items[index].state);
        decode.rollbackReservedPrefill(reserved[index]) catch {};
    };
    for (items, 0..) |item, index| {
        if (item.backend.ptr != cb.ptr or item.sequence_length != seq or
            item.geometry_tokens != seq or item.position_ids.len != 3 * seq or
            item.deepstack_layer_count != 0 or item.embeddings_owner.* == null or
            item.state.total_tokens != 0) return error.InvalidPreparedPrompt;
        mixed[index] = .{ .state = item.state, .total_sequence_len = seq, .query_sequence_len = seq, .kv_sequence_len = 0, .kv_position_offset = 0, .attention_mode = .paged_prefill };
        reserved[index] = try graph_mod.decoder_step_executor.Executor.reserveAndRefreshScheduledPrefill(&mixed[index], seq);
        prepared_count += 1;
        for (0..3) |axis| @memcpy(positions[axis * total + index * seq ..][0..seq], item.position_ids[axis * seq ..][0..seq]);
    }
    var owned = try graph_mod.decode_batch.buildWithPositions(a, mixed[0..items.len], positions);
    defer owned.deinit();
    const combined = (try cb.allocUninitF32Shape(&.{ @intCast(total), @intCast(cfg.hidden_size) })) orelse return error.UnsupportedBatchedPrefill;
    var consumed = false;
    defer if (!consumed) cb.free(combined);
    for (items, 0..) |item, index| {
        if (!try cb.copyRows2D(a, combined, index * seq, item.embeddings_owner.*.?, 0, seq, cfg.hidden_size)) return error.UnsupportedBatchedPrefill;
    }
    for (items) |item| {
        cb.free(item.embeddings_owner.*.?);
        item.embeddings_owner.* = null;
    }
    consumed = true;
    const hidden = try gpt_arch.forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(cb, a, cfg, combined, .{}, items.len, seq, &owned.context, null);
    defer cb.free(hidden.hidden);
    if (hidden.total_rows != total) return error.InvalidTensorShape;
    const last = (try cb.allocUninitF32Shape(&.{ @intCast(items.len), @intCast(cfg.hidden_size) })) orelse return error.UnsupportedBatchedPrefill;
    defer cb.free(last);
    for (0..items.len) |index| if (!try cb.copyRows2D(a, last, index, hidden.hidden, (index + 1) * seq - 1, 1, cfg.hidden_size)) return error.UnsupportedBatchedPrefill;
    const weight = try gpt_arch.getLmHeadWeight(cb, cfg);
    defer cb.free(weight);
    const logits = try cb.linearNoBias(last, weight, items.len, cfg.hidden_size, cfg.vocab_size);
    defer cb.free(logits);
    const host = try cb.toFloat32(logits, a);
    defer a.free(host);
    gpt_arch.applyFinalLogitSoftcapInPlace(cfg, host);
    const output = try a.alloc([]f32, items.len);
    var copied: usize = 0;
    errdefer {
        for (output[0..copied]) |row| a.free(row);
        a.free(output);
    }
    for (output, 0..) |*row, index| {
        row.* = try a.dupe(f32, host[index * cfg.vocab_size ..][0..cfg.vocab_size]);
        copied += 1;
    }
    if (platform.env.getenvBool("TERMITE_METAL_STAGE_TIMING_LOG_FRAMES"))
        std.debug.print("LIGHTONOCR3 profile_prefill_items={d} profile_prefill_query_tokens={d}\n", .{ items.len, seq });
    return output;
}

pub const Forward = struct {
    forward: decoder_forward.Runtime,
    pub fn capturePreparedTailPostNormHidden(
        cb: *const ComputeBackend,
        capture: bool,
        pre_norm_hidden: ops.CT,
        final_norm_slot: usize,
        hidden_size: usize,
        eps: f32,
    ) !?ops.CT {
        if (!capture) return null;
        return cb.decoderRuntimeApplyRmsNorm(&.{
            .slot = final_norm_slot,
            .input = pre_norm_hidden,
            .hidden_size = hidden_size,
            .eps = eps,
        });
    }

    pub fn tryCudaPreparedTailPrefillGreedy(
        self: *Forward,
        input_ids: []const i64,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        capture_last_hidden: bool,
    ) !?Output {
        return @import("invocation_observation.zig").callContextOperation(&self.forward.context, .prefill, @This().tryCudaPreparedTailPrefillGreedyUnobserved, .{ self, input_ids, seq_len, decode_context, capture_last_hidden });
    }

    fn tryCudaPreparedTailPrefillGreedyUnobserved(
        self: *Forward,
        input_ids: []const i64,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        capture_last_hidden: bool,
    ) !?Output {
        if (!decoder_routes.cudaPreparedPrefillGreedyEnabled()) return null;
        if (self.forward.context.cb.kind() != .cuda) return null;
        if (decode_context.attention_mode != .paged_prefill or decode_context.query_sequence_len != input_ids.len) return null;

        const tail = (try decoder_gated_runtime.forwardPrefillLastPreparedTail(
            self.forward.context.cb,
            self.forward.context.allocator,
            self.forward.context.gpt_config.*,
            self.forward.context.gpt_config.*.num_hidden_layers,
            input_ids,
            seq_len,
            decode_context,
            false,
        )) orelse return null;
        var owns_hidden = true;
        errdefer if (owns_hidden) self.forward.context.cb.free(tail.final_hidden);

        var greedy = (try decoder_tail_runtime.forwardGreedyTokenTensorFromFinalHidden(
            self.forward.context.cb,
            self.forward.context.allocator,
            self.forward.context.gpt_config.*,
            tail.final_hidden,
            .rms,
            tail.final_norm_slot,
        )) orelse {
            self.forward.context.cb.free(tail.final_hidden);
            return null;
        };
        if (greedy.token_tensor) |token_tensor| {
            self.forward.context.cb.free(token_tensor);
            greedy.token_tensor = null;
        }

        const last_hidden = try capturePreparedTailPostNormHidden(
            self.forward.context.cb,
            capture_last_hidden,
            tail.final_hidden,
            tail.final_norm_slot,
            self.forward.context.gpt_config.*.hidden_size,
            self.forward.context.gpt_config.*.norm_eps,
        );
        if (capture_last_hidden and last_hidden == null) {
            self.forward.context.cb.free(tail.final_hidden);
            owns_hidden = false;
            return null;
        }
        self.forward.context.cb.free(tail.final_hidden);
        owns_hidden = false;
        debugGenerationStage(
            "executePrefill prepared_tail_greedy token={d} capture_hidden={}",
            .{ greedy.token_id, capture_last_hidden },
        );
        return .{
            .greedy_token = greedy.token_id,
            .last_hidden = last_hidden,
            .last_hidden_rows = if (last_hidden != null) 1 else 0,
        };
    }

    pub fn metalPreparedTailOwnsWholePrefill(
        input_len: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) bool {
        return decode_context.attention_mode == .paged_prefill and
            decode_context.total_sequence_len == seq_len and
            decode_context.query_sequence_len == input_len and
            decode_context.kv_sequence_len == input_len and
            decode_context.kv_position_offset == 0 and
            input_len == seq_len;
    }

    /// Metal analog of the CUDA prepared-tail prefill: the planned prefill
    /// frame plus a prepared-slot greedy tail, capturing the last POST-final-
    /// norm hidden row for Gemma4 MTP seeding. Without this the MTP seed path
    /// re-runs the whole prompt through the generic per-op forward (a second
    /// prompt-length forward that scales with prompt size). Returns null on
    /// any miss so the caller falls back to the plain logits prefill.
    pub fn tryMetalPreparedTailPrefillGreedy(
        self: *Forward,
        input_ids: []const i64,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        capture_last_hidden: bool,
    ) !?Output {
        return @import("invocation_observation.zig").callContextOperation(&self.forward.context, .prefill, @This().tryMetalPreparedTailPrefillGreedyUnobserved, .{ self, input_ids, seq_len, decode_context, capture_last_hidden });
    }

    fn tryMetalPreparedTailPrefillGreedyUnobserved(
        self: *Forward,
        input_ids: []const i64,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        capture_last_hidden: bool,
    ) !?Output {
        if (self.forward.context.cb.kind() != .metal) return null;
        if (!decoder_routes.metalPreparedPrefillHiddenCaptureEnabled()) return null;
        if (!metalPreparedTailOwnsWholePrefill(input_ids.len, seq_len, decode_context)) return null;

        const prepared = self.forward.context.cb.decoderRuntimePrepareOrReuseFamily(
            self.forward.context.allocator,
            self.forward.context.gpt_config.*,
            seq_len,
            self.forward.context.gpt_config.*.num_hidden_layers,
        ) catch return null;
        if (!prepared.prepared) return null;

        const tail = (try decoder_gated_runtime.forwardPrefillLastPreparedTail(
            self.forward.context.cb,
            self.forward.context.allocator,
            self.forward.context.gpt_config.*,
            self.forward.context.gpt_config.*.num_hidden_layers,
            input_ids,
            seq_len,
            decode_context,
            true,
        )) orelse return null;
        defer self.forward.context.cb.free(tail.final_hidden);

        const last_hidden = try capturePreparedTailPostNormHidden(
            self.forward.context.cb,
            capture_last_hidden,
            tail.final_hidden,
            tail.final_norm_slot,
            self.forward.context.gpt_config.*.hidden_size,
            self.forward.context.gpt_config.*.norm_eps,
        );
        errdefer if (last_hidden) |hidden| self.forward.context.cb.free(hidden);
        if (capture_last_hidden and last_hidden == null) return null;

        if (tail.greedy_token_id) |greedy_token| {
            debugGenerationStage(
                "executePrefill metal_prepared_tail_greedy token={d} capture_hidden={}",
                .{ greedy_token, capture_last_hidden },
            );
            return .{
                .greedy_token = @intCast(greedy_token),
                .last_hidden = last_hidden,
                .last_hidden_rows = if (last_hidden != null) 1 else 0,
            };
        }

        const logits = if (try decoder_tail_runtime.forwardPreparedLogitsTensorFromFinalHidden(
            self.forward.context.cb,
            self.forward.context.gpt_config.*,
            tail.final_hidden,
            .rms,
            tail.final_norm_slot,
            tail.final_lm_head_slot,
        )) |prepared_logits|
            prepared_logits
        else
            (try decoder_tail_runtime.forwardLogitsTensorFromFinalHidden(
                self.forward.context.cb,
                self.forward.context.gpt_config.*,
                tail.final_hidden,
                .rms,
                tail.final_norm_slot,
            )) orelse {
                if (last_hidden) |hidden| self.forward.context.cb.free(hidden);
                return null;
            };
        defer self.forward.context.cb.free(logits);
        const last_logits = try self.forward.context.cb.toFloat32(logits, self.forward.context.allocator);
        gpt_arch.applyFinalLogitSoftcapInPlace(self.forward.context.gpt_config.*, last_logits);
        debugGenerationStage(
            "executePrefill metal_prepared_tail_logits capture_hidden={}",
            .{capture_last_hidden},
        );
        return .{
            .last_logits = last_logits,
            .last_hidden = last_hidden,
            .last_hidden_rows = if (last_hidden != null) 1 else 0,
        };
    }

    pub fn forwardPreparedEmbeddingsLastLogits(
        self: *Forward,
        input_embeddings: ops.CT,
        ple_token_ids: []const i64,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        capture_last_hidden: bool,
    ) !Output {
        return @import("invocation_observation.zig").callContextOperation(&self.forward.context, .prefill, @This().forwardPreparedEmbeddingsLastLogitsUnobserved, .{ self, input_embeddings, ple_token_ids, seq_len, decode_context, capture_last_hidden });
    }

    fn forwardPreparedEmbeddingsLastLogitsUnobserved(
        self: *Forward,
        input_embeddings: ops.CT,
        ple_token_ids: []const i64,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        capture_last_hidden: bool,
    ) !Output {
        var owns_input = true;
        defer if (owns_input) self.forward.context.cb.free(input_embeddings);
        if (try self.tryMetalPreparedMultimodalPrefill(input_embeddings, ple_token_ids, seq_len, decode_context, capture_last_hidden)) |logits| {
            return logits;
        }
        if (capture_last_hidden) return error.MultimodalMtpPrefillUnavailable;
        const ple_vectors = try gpt_arch.computePleVectors(self.forward.context.cb, self.forward.context.allocator, self.forward.context.gpt_config.*, ple_token_ids, input_embeddings, seq_len);
        defer if (ple_vectors) |vectors| self.forward.context.cb.free(vectors);
        owns_input = false;
        return .{ .last_logits = try gpt_arch.forwardLastLogitsLastRowFromEmbeddingsWithLayer0Overrides(
            self.forward.context.cb,
            self.forward.context.allocator,
            self.forward.context.gpt_config.*,
            input_embeddings,
            .{},
            1,
            seq_len,
            decode_context,
            ple_vectors,
        ) };
    }

    /// The planned Gemma prefill frame for a media prompt: the same route
    /// the token-id prefill takes, fed with the prepared embeddings, so the
    /// whole layer stack runs as one command buffer instead of one
    /// submission per op. Null leaves the embeddings untouched for the
    /// generic forward.
    pub fn tryMetalPreparedMultimodalPrefill(
        self: *Forward,
        input_embeddings: ops.CT,
        ple_token_ids: []const i64,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        capture_last_hidden: bool,
    ) !?Output {
        return @import("invocation_observation.zig").callContextOperation(&self.forward.context, .prefill, @This().tryMetalPreparedMultimodalPrefillUnobserved, .{ self, input_embeddings, ple_token_ids, seq_len, decode_context, capture_last_hidden });
    }

    fn tryMetalPreparedMultimodalPrefillUnobserved(
        self: *Forward,
        input_embeddings: ops.CT,
        ple_token_ids: []const i64,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        capture_last_hidden: bool,
    ) !?Output {
        if (self.forward.context.cb.kind() != .metal) return null;
        if (decode_context.attention_mode != .paged_prefill or decode_context.attn_or_mask != null) return null;
        if (!metalPreparedTailOwnsWholePrefill(ple_token_ids.len, seq_len, decode_context)) return null;
        const prepared = self.forward.context.cb.decoderRuntimePrepareOrReuseFamily(
            self.forward.context.allocator,
            self.forward.context.gpt_config.*,
            seq_len,
            self.forward.context.gpt_config.*.num_hidden_layers,
        ) catch return null;
        if (!prepared.prepared) return null;
        var rows = (try decoder_gated_runtime.forwardFinalHiddenRowsFromEmbeddings(
            self.forward.context.cb,
            self.forward.context.allocator,
            self.forward.context.gpt_config.*,
            self.forward.context.gpt_config.*.num_hidden_layers,
            input_embeddings,
            ple_token_ids,
            seq_len,
            decode_context,
        )) orelse return null;
        defer rows.deinit(self.forward.context.cb);
        const last_hidden = try self.forward.context.cb.sliceRows2D(self.forward.context.allocator, rows.final_hidden, rows.rows - 1, 1, self.forward.context.gpt_config.*.hidden_size);
        var owns_last_hidden = true;
        defer if (owns_last_hidden) self.forward.context.cb.free(last_hidden);
        const lm_w = try gpt_arch.getLmHeadWeight(self.forward.context.cb, self.forward.context.gpt_config.*);
        defer self.forward.context.cb.free(lm_w);
        const last_logits_device = try self.forward.context.cb.linearNoBias(last_hidden, lm_w, 1, self.forward.context.gpt_config.*.hidden_size, self.forward.context.gpt_config.*.vocab_size);
        defer self.forward.context.cb.free(last_logits_device);
        const last_logits = try self.forward.context.cb.toFloat32(last_logits_device, self.forward.context.allocator);
        gpt_arch.applyFinalLogitSoftcapInPlace(self.forward.context.gpt_config.*, last_logits);
        debugGenerationStage("multimodal prefill metal_prepared_frame rows={d}", .{rows.rows});
        if (capture_last_hidden) owns_last_hidden = false;
        return .{ .last_logits = last_logits, .last_hidden = if (capture_last_hidden) last_hidden else null, .last_hidden_rows = if (capture_last_hidden) 1 else 0 };
    }
};
pub fn retryAfterMemoryBudgetExceeded(
    decode_runtime: *BorrowedDecodeStateRuntime,
    reserved_tokens: usize,
    attempted_chunk_size: usize,
    current_chunk_size: *usize,
    allow_resize: bool,
    err: anyerror,
) !bool {
    if (err != error.MemoryBudgetExceeded or attempted_chunk_size <= 1) return false;
    try decode_runtime.rollbackReservedPrefill(reserved_tokens);
    if (!allow_resize) return false;
    current_chunk_size.* = @max(attempted_chunk_size / 2, 1);
    return true;
}

const cudaReplayLastLogitsEnabled = decoder_forward.cudaReplayLastLogitsEnabled;
fn debugGenerationStage(comptime fmt: []const u8, args: anytype) void {
    if (!platform.env.getenvBool("TERMITE_GEN_STAGE_DEBUG")) return;
    std.debug.print("gen_debug: " ++ fmt ++ "\n", args);
}

const CompiledPrefillFixture = @import("testing/decoder.zig");

fn checkNativeCompiledChunkCore(cb: ComputeBackend, _: ComputeBackend) !void {
    const a = std.testing.allocator;
    const model = CompiledPrefillFixture.config();
    const ids = [_]i64{ 0, 1 };
    const expected = try CompiledPrefillFixture.eager(cb, model, &ids);
    defer a.free(expected);
    const Probe = struct {
        backend: ComputeBackend,
        config: *const CompiledPrefillFixture.Config,
        mutex: *std.atomic.Mutex,
        lane: *control.ExecutionLane,
        calls: usize = 0,
        numerical_calls: usize = 0,
        decline: bool = false,
        fail_after: bool = false,
        fn get(raw: *anyopaque, name: []const u8) !ops.CT {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return gpt_arch.getTracedWeight(std.testing.allocator, &self.backend, self.config, name);
        }
        fn forward(ctx: *const graph_mod.execution.Context, _: *graph_mod.cache.GraphCache, tokens: []const i64, batch: usize, sequence: usize, decode: *const gpt_arch.DecodeContext) !?graph_mod.model_runtime.ModelOutput {
            const self: *@This() = @ptrCast(@alignCast(ctx.weights.?.ptr));
            self.calls += 1;
            try std.testing.expect(@import("allocation_registry.zig").enabled());
            if (!self.lane.owned) return error.CompiledPrefillMissingExecutionLane;
            if (self.mutex.tryLock()) {
                self.mutex.unlock();
                return error.CompiledPrefillMissingExecutionMutex;
            }
            if (self.decline) return null;
            self.numerical_calls += 1;
            const values = try gpt_arch.forward(&self.backend, ctx.allocator, self.config.*, tokens, batch, sequence, decode);
            defer ctx.allocator.free(values);
            var output = graph_mod.model_runtime.ModelOutput{ .logits = try ctx.allocator.dupe(f32, values[(tokens.len - 1) * self.config.vocab_size ..][0..self.config.vocab_size]) };
            errdefer output.deinit(ctx.allocator);
            if (self.fail_after) return error.InjectedCommittedCompiledPrefill;
            return output;
        }
    };
    for (0..6) |scenario| {
        var mutex: std.atomic.Mutex = .unlocked;
        var lane = control.ExecutionLane{};
        var probe = Probe{ .backend = cb, .config = &model, .mutex = &mutex, .lane = &lane, .decline = scenario == 3, .fail_after = scenario == 4 };
        var cache = graph_mod.cache.GraphCache.init(a);
        defer cache.deinit();
        var state = graph_mod.decode_state_runtime.NativeDecodeState.initContiguous(a);
        defer state.deinit();
        var diagnostics = @import("diagnostics.zig").Diagnostics{};
        diagnostics.configureBackend(true, &cb);
        const session = @import("../backends/session.zig").Session{ .ptr = &probe, .vtable = undefined, .invocation_diagnostics = .{ .diagnostics = &diagnostics } };
        var physical = Runtime{ .forward = .{ .context = .{ .session = session, .allocator = a, .cb = &cb, .gpt_config = &model, .weights = .{ .ptr = &probe, .get_fn = Probe.get } }, .graph_cache = &cache }, .state = &state, .execution = .{ .mutex = &mutex, .gate = &lane, .control = if (scenario == 5) .{ .deadline_ns = 0 } else null } };
        const request = Runtime.CompiledChunk{ .token_ids = &ids, .total_sequence_length = ids.len, .output = if (scenario == 0) .kv_only else if (scenario == 2) .greedy_token else .last_logits };
        if (scenario == 5) {
            try std.testing.expectError(error.Timeout, physical.runCompiledChunkWith(request, Probe.forward));
        } else if (scenario == 3) {
            try std.testing.expectError(error.MissingCompiledModelRuntime, physical.runCompiledChunkWith(request, Probe.forward));
        } else if (scenario == 4) {
            try std.testing.expectError(error.InjectedCommittedCompiledPrefill, physical.runCompiledChunkWith(request, Probe.forward));
        } else {
            const result = try physical.runCompiledChunkWith(request, Probe.forward);
            defer if (result.last_logits) |values| a.free(values);
            if (scenario == 0) try std.testing.expect(result.last_logits == null and result.greedy_token == null);
            if (scenario == 1) try std.testing.expectEqualSlices(f32, expected, result.last_logits.?);
            if (scenario == 2) try std.testing.expectEqual(@as(usize, if (expected[1] > expected[0]) 1 else 0), result.greedy_token.?);
        }
        try std.testing.expectEqual(@as(usize, if (scenario == 5) 0 else 1), probe.calls);
        try std.testing.expectEqual(@as(usize, if (scenario == 3 or scenario == 5) 0 else 1), probe.numerical_calls);
        try std.testing.expectEqual(@as(usize, if (scenario == 5) 0 else 2), state.total_tokens);
        try CompiledPrefillFixture.checkUnlocked(&mutex, &lane);
        try std.testing.expect(!@import("allocation_registry.zig").enabled());
        const observed = diagnostics.snapshot() orelse return error.MissingCompiledPrefillObservation;
        try std.testing.expect(observed.operation == .prefill);
        if (scenario >= 3) {
            try std.testing.expect(observed.outcome == .failed);
            const name: []const u8 = if (scenario == 3) "MissingCompiledModelRuntime" else if (scenario == 4) "InjectedCommittedCompiledPrefill" else "Timeout";
            try std.testing.expectEqualStrings(name, observed.error_name.?.view());
        } else try std.testing.expect(observed.outcome == .success);
        if (scenario == 5) {
            try std.testing.expect(observed.context_runtime_scope == .unavailable);
            try std.testing.expect(observed.runtime.submission == null);
        } else {
            try std.testing.expect(observed.context_runtime_scope == .borrowed_execution_backend_context);
            try std.testing.expect(observed.runtime.submission != null);
        }
    }
}

test "compiled prefill transaction actual Native parity output intents decline and post-forward faults release lane without replay" {
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onNative(CompiledPrefillFixture.populate, checkNativeCompiledChunkCore);
}

fn checkMetalCompiledChunk(cb: ComputeBackend, native_reference: ComputeBackend) !void {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const a = std.testing.allocator;
    const backend_session = @import("../backends/session.zig");
    const model = CompiledPrefillFixture.config();
    const ids = [_]i64{ 0, 1 };
    const expected = try CompiledPrefillFixture.eager(native_reference, model, &ids);
    defer a.free(expected);
    const Source = struct {
        compute: *@import("../ops/metal_compute.zig").MetalCompute,
        fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .metal;
        }
        fn info(_: *anyopaque) []const @import("../backends/tensor.zig").TensorInfo {
            return &.{};
        }
        fn close(_: *anyopaque) void {}
        const table = backend_session.Session.VTable{ .run = undefined, .inputInfo = info, .outputInfo = info, .backend = backend, .close = close };
        fn acquire(raw: *anyopaque, _: backend_session.Session, allocator: std.mem.Allocator, options: backend_session.ComputeBackendOptions) !ComputeBackend {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!options.borrow_shared_provider or options.run_budget != null) return error.InvalidCompiledPrefillTestProvider;
            return @import("../backends/compute_factory.zig").borrowMetalProvider(allocator, self.compute.data, self.compute.io, .{}, false);
        }
    };
    var source = Source{ .compute = @ptrCast(@alignCast(cb.ptr)) };
    const session = backend_session.Session{ .ptr = &source, .vtable = &Source.table, .model_contract = .{ .decoder_config = &model }, .compute_backend_provider = .{ .ptr = &source, .services = &.{ .acquire = Source.acquire } } };
    const Fault = struct {
        calls: usize = 0,
        fn get(_: *anyopaque, _: []const u8) anyerror!ops.CT {
            return error.UnexpectedCompiledPrefillWeightOverride;
        }
        fn forward(ctx: *const graph_mod.execution.Context, cache_arg: *graph_mod.cache.GraphCache, tokens: []const i64, batch: usize, sequence: usize, decode: *const gpt_arch.DecodeContext) !?graph_mod.model_runtime.ModelOutput {
            const self: *@This() = @ptrCast(@alignCast(ctx.weights.?.ptr));
            self.calls += 1;
            var result = (try graph_mod.execution.graphForwardCompiledModelOutput(ctx, cache_arg, tokens, batch, sequence, decode)) orelse return error.MissingCompiledModelRuntime;
            defer result.deinit(ctx.allocator);
            return error.InjectedCommittedCompiledPrefill;
        }
    };
    const request_control = control.InferenceExecutionControl{ .native_call_policy = .in_process };
    var cache = graph_mod.cache.GraphCache.init(a);
    defer cache.deinit();
    for (0..4) |scenario| {
        var state = graph_mod.decode_state_runtime.NativeDecodeState.initContiguous(a);
        defer state.deinit();
        var physical = Runtime{ .forward = .{ .context = .{ .allocator = a, .cb = &cb, .gpt_config = &model, .weight_semantics = .traced_decoder, .session = session, .compiled_partition_backend = .metal, .compiled_attachment_target = .whole_model, .kv_dtype = .f16, .execution_control = request_control }, .graph_cache = &cache }, .state = &state };
        const request = Runtime.CompiledChunk{ .token_ids = &ids, .total_sequence_length = ids.len, .output = if (scenario == 0) .kv_only else if (scenario == 2) .greedy_token else .last_logits };
        if (scenario == 3) {
            var fault = Fault{};
            physical.forward.context.weights = .{ .ptr = &fault, .get_fn = Fault.get };
            try std.testing.expectError(error.InjectedCommittedCompiledPrefill, physical.runCompiledChunkWith(request, Fault.forward));
            try std.testing.expectEqual(@as(usize, 1), fault.calls);
            try std.testing.expectEqual(@as(usize, 2), state.total_tokens);
            continue;
        }
        const result = try physical.runCompiledChunk(request);
        defer if (result.last_logits) |values| a.free(values);
        if (scenario == 0) try std.testing.expect(result.last_logits == null and result.greedy_token == null);
        if (scenario == 1) for (expected, result.last_logits.?) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.001);
        if (scenario == 2) try std.testing.expectEqual(@as(usize, if (expected[1] > expected[0]) 1 else 0), result.greedy_token.?);
        try std.testing.expectEqual(@as(usize, 2), state.total_tokens);
    }
}

test "compiled prefill transaction actual Metal whole-model runtime matches independent Native final row and greedy token" {
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onMetal(CompiledPrefillFixture.populate, checkMetalCompiledChunk);
}

test "prefill cursor preserves admitted shapes scratch envelope hints and exactly once progression" {
    const a = std.testing.allocator;
    var plan = try runtime.scheduler.native_generate.PrefillChunkPlan.init(a, 19, 8, 4096, .shared, .fixed_ceiling);
    defer plan.deinit(a);
    const fingerprint = plan.fingerprint;
    var cursor = try ChunkCursor.init(.{ .query_tokens = 19, .prefilled_tokens = 3, .prompt_tokens = 22, .initial_chunk_rows = 1, .admitted_plan = plan, .greedy_output = true, .capture_hidden = true });
    const first = (try cursor.next(1)).?;
    try std.testing.expectEqual(@as(usize, 8), first.chunk.rows);
    try std.testing.expectEqual(@as(usize, 11), first.total_end);
    try std.testing.expectEqual(decoder_routes.PrefillOutputIntent.kv_only, first.output_intent);
    try std.testing.expectError(error.PrefillChunkInFlight, cursor.next(1));
    try std.testing.expectError(error.InvalidPrefillChunkPlan, cursor.retry(first, 4));
    try std.testing.expect(!try cursor.retryMemoryBudgetExceeded(first, error.MemoryBudgetExceeded));
    try std.testing.expectEqual(@as(usize, 0), cursor.processed);
    try cursor.commit(first);
    try std.testing.expectError(error.PrefillChunkAlreadyConsumed, cursor.commit(first));
    const second = (try cursor.next(1)).?;
    try std.testing.expectEqual(@as(usize, 8), second.chunk.rows);
    try cursor.commit(second);
    const tail = (try cursor.next(1)).?;
    try std.testing.expectEqual(@as(usize, 3), tail.chunk.rows);
    try std.testing.expectEqual(plan.tail().weight_route_hint, tail.chunk.weight_route_hint);
    try std.testing.expectEqual(decoder_routes.PrefillOutputIntent.greedy_token_and_hidden, tail.output_intent);
    try cursor.commit(tail);
    try std.testing.expect((try cursor.next(1)) == null);
    try cursor.finish();
    try plan.validate();
    try std.testing.expectEqual(fingerprint, plan.fingerprint);
    try std.testing.expectEqual(@as(usize, 4096), plan.max_scratch_bytes);
}

test "prefill cursor retry cancellation and stale attempts preserve actual paged KV and unpublished progress" {
    const Case = struct {
        const Cancel = struct {
            cancelled: bool = false,
            fn check(raw: ?*anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw.?));
                if (self.cancelled) return error.Cancelled;
            }
        };
        fn run(a: std.mem.Allocator) !void {
            var manager = runtime.kv.manager.KvManager.init(a);
            defer manager.deinit();
            // The recycler append on deinit is intentionally best effort and
            // has a scan fallback. Exercise its allocation as fallible setup,
            // so every injected fault here belongs to required forward work.
            try manager.free_sequence_indices.ensureTotalCapacity(a, 1);
            const pool = try manager.addPool(.{ .backend = .native, .dtype = .f16, .page_size_tokens = 4, .num_kv_heads = 1, .head_dim = 8 });
            var state = graph_mod.decode_state_runtime.NativeDecodeState.initPaged(a, &manager, pool, null);
            defer state.deinit();
            var decode = BorrowedDecodeStateRuntime.init(&state);
            try decode.appendPrefillChunk(3);
            var cancel = Cancel{};
            var cursor = try ChunkCursor.init(.{ .query_tokens = 6, .prefilled_tokens = 3, .prompt_tokens = 9, .initial_chunk_rows = 8, .execution_control = .{ .ptr = &cancel, .check_fn = Cancel.check } });
            const first = (try cursor.next(8)).?;
            const reserved = try decode.reservePrefillTo(first.total_end);
            var resized = cursor.current_chunk_size;
            try std.testing.expect(try retryAfterMemoryBudgetExceeded(&decode, reserved, first.chunk.rows, &resized, true, error.MemoryBudgetExceeded));
            try cursor.retry(first, resized);
            try std.testing.expectEqual(@as(usize, 3), state.total_tokens);
            try std.testing.expectEqual(@as(usize, 3), manager.tokenCount(state.sequence_id.?).?);
            try std.testing.expectEqual(@as(usize, 0), cursor.processed);
            const pending = (try cursor.next(8)).?;
            const pending_reserved = try decode.reservePrefillTo(pending.total_end);
            cancel.cancelled = true;
            // Numerical cancellation retires its exact physical reservation;
            // cursor cancellation only discards unpublished scheduling state.
            try decode.rollbackReservedPrefill(pending_reserved);
            try cursor.cancel(pending);
            try std.testing.expectError(error.Cancelled, cursor.next(8));
            try std.testing.expectEqual(@as(usize, 0), cursor.processed);
            try std.testing.expectEqual(@as(usize, 3), state.total_tokens);
            cancel.cancelled = false;
            const resumed = (try cursor.next(8)).?;
            try std.testing.expectError(error.InvalidPrefillChunkState, cursor.commit(pending));
            _ = try decode.reservePrefillTo(resumed.total_end);
            try cursor.commit(resumed);
            try std.testing.expectEqual(@as(usize, 4), cursor.processed);
            const tail = (try cursor.next(8)).?;
            try std.testing.expectEqual(decoder_routes.PrefillOutputIntent.last_logits, tail.output_intent);
            _ = try decode.reservePrefillTo(tail.total_end);
            try cursor.commit(tail);
            try cursor.finish();
            try std.testing.expectEqual(@as(usize, 9), state.total_tokens);
            try std.testing.expectEqual(@as(usize, 9), manager.tokenCount(state.sequence_id.?).?);
        }
    };
    try Case.run(std.testing.allocator);
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "prefill cursor preserves scalar coalescing and rejects invalid retry without consuming attempt" {
    var ordinary = try ChunkCursor.init(.{ .query_tokens = 10, .prefilled_tokens = 0, .prompt_tokens = 10, .initial_chunk_rows = 8 });
    const first = (try ordinary.next(4)).?;
    try std.testing.expectEqual(@as(usize, 4), first.chunk.rows);
    try std.testing.expectError(error.InvalidPrefillChunkRetry, ordinary.retry(first, 0));
    try std.testing.expectError(error.InvalidPrefillChunkRetry, ordinary.retry(first, 4));
    try std.testing.expect(!try ordinary.retryMemoryBudgetExceeded(first, error.Cancelled));
    try std.testing.expectError(error.InvalidPrefillChunkState, ordinary.finish());
    try ordinary.cancel(first);
    var coalesced = try ChunkCursor.init(.{ .query_tokens = 10, .prefilled_tokens = 0, .prompt_tokens = 10, .initial_chunk_rows = 8, .first_token_coalesced = true });
    const large = (try coalesced.next(4)).?;
    try std.testing.expectEqual(@as(usize, 8), large.chunk.rows);
    try coalesced.cancel(large);
}

test "prefill selection reconciles actual admitted plans cache bypass and checkpoint geometry" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var plan = try runtime.scheduler.native_generate.PrefillChunkPlan.init(a, 19, 8, 4096, .shared, .fixed_ceiling);
            defer plan.deinit(a);
            var other = try runtime.scheduler.native_generate.PrefillChunkPlan.init(a, 19, 6, 8192, .shared, .fixed_ceiling);
            defer other.deinit(a);
            var selection = ChunkCursor.Selection{ .configured_plan = plan, .leased_plan = plan, .configured_chunk_rows = 12, .scheduler_chunk_rows = 5, .backend = .cuda, .pure_greedy = true, .resident_greedy_allowed = true, .first_token_enabled = true, .coalesce_token_limit = 512 };
            const request = ChunkCursor.Options{ .query_tokens = 19, .prefilled_tokens = 3, .prompt_tokens = 22 };
            var cursor = try ChunkCursor.select(request, selection);
            try std.testing.expect(cursor.plan_decision == .bypassed_prompt_cache);
            try std.testing.expect(cursor.options.admitted_plan == null);
            try std.testing.expectEqual(@as(usize, 5), cursor.current_chunk_size);
            try std.testing.expect(!cursor.options.first_token_coalesced); // borrowed plans never coalesce
            selection.checkpoint_resume = true;
            cursor = try ChunkCursor.select(request, selection);
            try std.testing.expect(cursor.plan_decision == .execute);
            try std.testing.expect(cursor.options.admitted_plan.?.chunks.ptr == plan.chunks.ptr);
            try std.testing.expectEqual(@as(usize, 8), cursor.current_chunk_size);
            const wrong_geometry = ChunkCursor.Options{ .query_tokens = 7, .prefilled_tokens = 3, .prompt_tokens = 10 };
            try std.testing.expectError(error.InvalidPrefillChunkPlan, ChunkCursor.select(wrong_geometry, selection));
            selection.checkpoint_resume = false;
            cursor = try ChunkCursor.select(.{ .query_tokens = 7, .prefilled_tokens = 0, .prompt_tokens = 7 }, selection);
            try std.testing.expect(cursor.plan_decision == .bypassed_geometry);
            try std.testing.expectEqual(@as(usize, 5), cursor.current_chunk_size);
            selection.leased_plan = other;
            try std.testing.expectError(error.InvalidPrefillChunkPlan, ChunkCursor.select(request, selection));
            selection.selected_kv = true;
            try std.testing.expectError(error.InvalidPrefillChunkPlan, ChunkCursor.select(request, selection));
            selection.leased_plan = plan;
            selection.configured_chunk_rows = 2048;
            selection.scheduler_chunk_rows = 4096;
            cursor = try ChunkCursor.select(.{ .query_tokens = 8192, .prefilled_tokens = 0, .prompt_tokens = 8192 }, selection);
            try std.testing.expect(cursor.plan_decision == .none and cursor.options.admitted_plan == null);
            try std.testing.expectEqual(@min(@as(usize, 2048), runtime.kv.selection.defaults.prefill_chunk_rows), cursor.current_chunk_size);
            try std.testing.expect(!cursor.options.first_token_coalesced);
        }
    };
    try Case.run(std.testing.allocator);
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "prefill selection preserves measured CUDA coalescing eligibility and scheduler ceiling" {
    const request = ChunkCursor.Options{ .query_tokens = 100, .prefilled_tokens = 0, .prompt_tokens = 100 };
    var policy = ChunkCursor.Selection{ .configured_chunk_rows = 64, .scheduler_chunk_rows = 32, .backend = .cuda, .pure_greedy = true, .resident_greedy_allowed = true, .first_token_enabled = true, .coalesce_token_limit = 512 };
    var cursor = try ChunkCursor.select(request, policy);
    try std.testing.expectEqual(@as(usize, 32), cursor.uncoalesced_chunk_size);
    try std.testing.expectEqual(@as(usize, 100), cursor.current_chunk_size);
    try std.testing.expect(cursor.options.first_token_coalesced);
    const attempt = (try cursor.next(16)).?;
    try std.testing.expectEqual(@as(usize, 100), attempt.chunk.rows);
    try cursor.cancel(attempt);
    policy.backend = .metal;
    cursor = try ChunkCursor.select(request, policy);
    try std.testing.expectEqual(@as(usize, 32), cursor.current_chunk_size);
    policy.backend = .cuda;
    policy.pure_greedy = false;
    cursor = try ChunkCursor.select(request, policy);
    try std.testing.expectEqual(@as(usize, 32), cursor.current_chunk_size);
    policy.pure_greedy = true;
    policy.resident_greedy_allowed = false;
    cursor = try ChunkCursor.select(request, policy);
    try std.testing.expectEqual(@as(usize, 32), cursor.current_chunk_size);
    policy.resident_greedy_allowed = true;
    policy.coalesce_token_limit = 99;
    cursor = try ChunkCursor.select(request, policy);
    try std.testing.expectEqual(@as(usize, 32), cursor.current_chunk_size);
}

fn promptOwnerTestRequest(rows: usize) PromptRequest {
    return .{
        .route = .{ .storage = .{ .pure_greedy = true, .grammar = false, .prompt_cache = false, .compaction = false, .speculation = false, .draft = false }, .has_token_observer = false, .exact_hybrid = false },
        .prompt_tokens = rows,
        .query_tokens = rows,
        .prefilled_tokens = 0,
        .allow_resident_greedy = false,
        .capture_hidden = false,
        .selected_kv = false,
        .checkpoint = false,
        .has_draft = false,
        .has_prompt_cache = false,
        .scheduler_peers = false,
        .execution_serialized = false,
        .configured_chunk_rows = 2,
        .scheduler_chunk_rows = 0,
        .speculative_k = 4,
        .sampled_ring_qualified = false,
    };
}

fn checkPromptOwner(cb: ComputeBackend, _: ComputeBackend) !void {
    const a = std.testing.allocator;
    const cfg = CompiledPrefillFixture.config();
    const ids = [_]i64{ 0, 1, 0, 1, 0 };
    const expected = try CompiledPrefillFixture.eager(cb, cfg, &ids);
    defer a.free(expected);
    // The same request facts pass through the physical owner with every
    // numerical chunk executing against real one-layer attention/MLP weights.
    for ([_]usize{ 1, 2, ids.len }) |chunk_rows| {
        const kv_config = runtime.kv.pool.KvPoolConfig{ .backend = .native, .dtype = .f32, .page_size_tokens = 4, .num_layers_packed = @intCast(cfg.num_hidden_layers), .num_kv_heads = cfg.effectiveKVHeads(), .head_dim = cfg.headDim(), .store_cpu_bytes = true };
        var manager = runtime.kv.manager.KvManager.init(a);
        defer manager.deinit();
        const pool = try manager.addPool(kv_config);
        var storage = try runtime.kv.storage_runtime.KvStorageRuntime.init(a, kv_config);
        defer storage.deinit();
        var state = graph_mod.decode_state_runtime.NativeDecodeState.initPaged(a, &manager, pool, null);
        state.kv_storage = &storage;
        defer state.deinit();
        var physical = Runtime{ .forward = .{ .context = .{ .allocator = a, .cb = &cb, .gpt_config = &cfg } }, .state = &state };
        const request = promptOwnerTestRequest(ids.len);
        const plan = try physical.planPrompt(request);
        try std.testing.expect(!plan.greedy() and !plan.compiled_whole_model and plan.typed_outputs);
        physical.prepareChunkWindow(request, chunk_rows);
        var cursor = try ChunkCursor.init(.{ .query_tokens = ids.len, .prefilled_tokens = 0, .prompt_tokens = ids.len, .initial_chunk_rows = chunk_rows });
        while (try cursor.next(chunk_rows)) |attempt| {
            defer cursor.cancel(attempt) catch {};
            const outcome = try physical.runChunk(.{
                .token_ids = ids[attempt.start..attempt.end],
                .total_chunk_end = attempt.total_end,
                .prompt_tokens = ids.len,
                .already_prefilled = 0,
                .processed_rows = attempt.start,
                .output_intent = attempt.output_intent,
                .capture_hidden = false,
                .shared_kv_pruning = plan.shared_kv,
                .prepared_metal_whole_prompt = false,
                .metal_target_last_logits = plan.metal_last_logits,
                .cuda_direct_last_logits = plan.cuda_last_logits,
                .typed_outputs = plan.typed_outputs,
                .attempted_chunk_size = attempt.chunk.rows,
                .allow_resize = false,
            });
            switch (outcome) {
                .retry_chunk_size => return error.UnexpectedPromptOwnerRetry,
                .complete => |output| {
                    defer if (output.last_logits) |values| a.free(values);
                    defer if (output.last_hidden) |hidden| cb.free(hidden);
                    if (attempt.end == ids.len) {
                        const actual = output.last_logits orelse return error.MissingPromptOwnerLogits;
                        try std.testing.expectEqual(expected.len, actual.len);
                        for (expected, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.0001);
                    } else try std.testing.expect(output.last_logits == null and output.greedy_token == null and output.last_hidden == null);
                },
            }
            const view = state.kvView() orelse return error.MissingActualPromptKvView;
            const blocks = view.logical_blocks orelse return error.MissingActualPromptKvBlocks;
            try std.testing.expect(view.kv_storage == &storage and blocks.len > 0);
            try std.testing.expectEqual(attempt.total_end, view.token_count);
            try std.testing.expectEqual(attempt.total_end, state.total_tokens);
            try std.testing.expectEqual(attempt.total_end, manager.tokenCount(state.sequence_id.?).?);
            try std.testing.expectEqual(attempt.total_end, storage.tokenCount(state.sequence_id.?).?);
            var physical_state = BorrowedDecodeStateRuntime.init(&state);
            const decode = physical_state.makeDecodeContext(attempt.total_end, attempt.chunk.rows);
            try std.testing.expect(decode.kv_cache != null and decode.kv_cache.?.kv_storage == &storage);
            try cursor.commit(attempt);
        }
        try cursor.finish();
        try std.testing.expectEqual(ids.len, state.total_tokens);
        // Physical planning borrows the backend and state, retaining neither
        // request memory nor a second state/lease; it allocates nothing.
        var fail = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        physical.forward.context.allocator = fail.allocator();
        _ = try physical.planPrompt(request);
        physical.prepareChunkWindow(request, chunk_rows);
        try std.testing.expectEqual(@as(usize, 0), fail.alloc_index);
        try std.testing.expectEqual(ids.len, state.total_tokens);
        try std.testing.expectEqual(@as(usize, 0), state.kv_max_inflight_tokens);
        var hit = request;
        hit.prefilled_tokens = 1;
        try std.testing.expectError(error.UnsupportedShape, physical.prepareCompiledPrompt(hit));
        try std.testing.expectEqual(ids.len, state.total_tokens);
    }
}

test "physical prompt owner preserves real Native eager chunk numerics and allocation-free planning" {
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onNative(CompiledPrefillFixture.populate, checkPromptOwner);
}

fn checkResidentEmbeddingRows(backend_value: ComputeBackend, _: ComputeBackend) !void {
    const a = std.testing.allocator;
    const cb = &backend_value;
    const source = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }, &.{ 3, 4 });
    defer cb.free(source);
    const tail = try cb.fromFloat32Shape(&.{ 21, 22, 23, 24 }, &.{ 1, 4 });
    defer cb.free(tail);
    const weight = try cb.fromFloat32Shape(&.{ 1, 0, 0, 0, 0, 0, 0, 1 }, &.{ 2, 4 });
    defer cb.free(weight);
    const bias = try cb.fromFloat32Shape(&.{ 0.5, -0.5 }, &.{2});
    defer cb.free(bias);
    const transfers = @import("../ops/media_residency_test.zig").Transfers.snapshot(backend_value);
    var assembly = (try EmbeddingRows.init(cb, a, 3, 4)) orelse return error.ExpectedResidentEmbeddingAssembly;
    defer assembly.deinit();
    try assembly.append(source, 1, 2);
    try std.testing.expectError(error.IncompleteEmbeddingAssembly, assembly.take());
    try assembly.append(tail, 0, 1);
    var hidden = ResidentHiddenRows{ .backend = cb, .allocator = a, .tensor = try assembly.take(), .rows = 3, .width = 4 };
    defer hidden.deinit();
    var projected = try hidden.project(weight, bias, try hidden.projectionWidth(bias));
    defer projected.deinit();
    // A later independent layout cannot overwrite held resident source/head.
    var later = (try EmbeddingRows.init(cb, a, 1, 4)) orelse return error.ExpectedResidentEmbeddingAssembly;
    defer later.deinit();
    try later.append(source, 0, 1);
    try transfers.expectUnchanged(backend_value);
    const values = try projected.exportHost();
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 5.5, 7.5, 9.5, 11.5, 21.5, 23.5 }, values);
    const held = try hidden.exportHost();
    defer a.free(held);
    try std.testing.expectEqualSlices(f32, &.{ 5, 6, 7, 8, 9, 10, 11, 12, 21, 22, 23, 24 }, held);
}

test "Native physical embedding layout and projection retain rows without intermediate exports" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onNative(helper.noWeights, checkResidentEmbeddingRows);
}

test "Metal physical embedding layout and projection retain rows without intermediate exports" {
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onMetal(helper.noWeights, checkResidentEmbeddingRows);
}

test "Metal physical embedding layout rejects an initialized foreign provider source before copy" {
    const Helper = @import("../ops/media_residency_test.zig");
    const Check = struct {
        fn run(backend_value: ComputeBackend, _: ComputeBackend) !void {
            const a = std.testing.allocator;
            const cb = &backend_value;
            const Metal = @import("../ops/metal_compute.zig");
            var store = @import("../ops/gpu_hosted_store.zig").WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .{} };
            defer store.lazy_weights.deinit(a);
            Metal.initPrefetchQueue(&store, a);
            defer Metal.deinitPrefetchQueue(&store);
            defer Metal.deinitSharedNativeProvider(&store);
            var foreign = try Metal.MetalCompute.init(a, &store, null);
            defer foreign.deinit();
            const foreign_cb = foreign.computeBackend();
            const values = [_]f32{ 1, 2, 3, 4 };
            const input = try foreign_cb.fromFloat32Shape(&values, &.{ 1, 4 });
            defer foreign_cb.free(input);
            var source = (try EmbeddingRows.init(&foreign_cb, a, 1, 4)) orelse return error.ExpectedResidentEmbeddingAssembly;
            defer source.deinit();
            try source.append(input, 0, 1);
            const initialized = try source.take();
            defer foreign_cb.free(initialized);
            var destination = (try EmbeddingRows.init(cb, a, 1, 4)) orelse return error.ExpectedResidentEmbeddingAssembly;
            defer destination.deinit();
            try std.testing.expect(source.namespace.kind == destination.namespace.kind and source.namespace.context != destination.namespace.context);
            const transfers = Helper.Transfers.snapshot(backend_value);
            try std.testing.expect(!try destination.acceptsSource(initialized, 1));
            try transfers.expectUnchanged(backend_value);
            try std.testing.expectEqual(@as(usize, 0), destination.written);
        }
    };
    try Helper.onMetal(Helper.noWeights, Check.run);
}

fn checkPromptReplacementRows(backend_value: ComputeBackend, _: ComputeBackend) !void {
    const a = std.testing.allocator;
    const cb = &backend_value;
    const helper = @import("../ops/media_residency_test.zig");
    var base_values: [24]f32 = undefined;
    for (&base_values, 0..) |*value, index| value.* = @floatFromInt(index + 1);
    const base = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&base_values, &.{ 6, 4 }));
    defer cb.free(base);
    const image = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&.{ 41, 42, 43, 44, 51, 52, 53, 54 }, &.{ 2, 4 }));
    defer cb.free(image);
    const before = helper.Transfers.snapshot(backend_value);
    var prompt = try PromptEmbeddingRows.init(cb, a, base, 6, 4, 1);
    defer prompt.deinit();
    try std.testing.expect(prompt.storage == .resident);
    try prompt.replace(.{ .offset = 1, .rows = 2, .source = .{ .resident = image } });
    try prompt.replace(.{ .offset = 4, .rows = 2, .source = .{ .resident = image } });
    const completed = try prompt.finish();
    defer cb.free(completed);
    var later = try PromptEmbeddingRows.init(cb, a, base, 6, 4, 1);
    defer later.deinit();
    try later.replace(.{ .offset = 0, .rows = 2, .source = .{ .resident = image } });
    try before.expectUnchanged(backend_value);
    const actual = try cb.toFloat32(completed, a);
    defer a.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4, 41, 42, 43, 44, 51, 52, 53, 54, 13, 14, 15, 16, 41, 42, 43, 44, 51, 52, 53, 54 }, actual);
    const unchanged = try cb.toFloat32(base, a);
    defer a.free(unchanged);
    try std.testing.expectEqualSlices(f32, &base_values, unchanged);
    // A rejected later write is terminal; an earlier held prompt remains valid.
    try std.testing.expectError(error.InvalidEmbeddingAssembly, later.replace(.{ .offset = 1, .rows = 2, .source = .{ .resident = image } }));
    try std.testing.expectError(error.InvalidEmbeddingAssembly, later.finish());
    const held = try cb.toFloat32(completed, a);
    defer a.free(held);
    try std.testing.expectEqualSlices(f32, actual, held);
    // Unit-scale host producers still need an explicit current-provider
    // upload before joining an already resident prompt. This exercises the
    // genuine host-chunks ABI shared by all three Gemma entrypoints.
    const unit_host = try replaceEmbeddingRows(cb, a, base, 6, 4, 1, .{ .host_chunks = .{ .values = &.{ 101, 102, 103, 104, 201, 202, 203, 204 }, .offsets = &.{ 1, 4 }, .rows_per_chunk = 1 } });
    defer cb.free(unit_host);
    const unit_values = try cb.toFloat32(unit_host, a);
    defer a.free(unit_values);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4, 101, 102, 103, 104, 9, 10, 11, 12, 13, 14, 15, 16, 201, 202, 203, 204, 21, 22, 23, 24 }, unit_values);
    // Real host-producer compatibility preserves scale-before-image-overwrite.
    const host_result = try replaceEmbeddingRows(cb, a, base, 6, 4, 2, .{ .host_chunks = .{ .values = &.{ 101, 102, 103, 104, 201, 202, 203, 204 }, .offsets = &.{ 1, 4 }, .rows_per_chunk = 1 } });
    defer cb.free(host_result);
    const host_values = try cb.toFloat32(host_result, a);
    defer a.free(host_values);
    try std.testing.expectEqualSlices(f32, &.{ 2, 4, 6, 8, 101, 102, 103, 104, 18, 20, 22, 24, 26, 28, 30, 32, 201, 202, 203, 204, 42, 44, 46, 48 }, host_values);
}

test "Native streaming prompt replacement preserves rows scale held outputs and no intermediate export" {
    const helper = @import("../ops/media_residency_test.zig");
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    try helper.onNative(helper.noWeights, checkPromptReplacementRows);
}
test "Metal streaming prompt replacement preserves rows scale held outputs and no intermediate export" {
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onMetal(helper.noWeights, checkPromptReplacementRows);
}

test "Native prompt replacement allocation failures preserve all source and output owners" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            const native = @import("../ops/native_compute.zig");
            var weights = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
            defer weights.deinitOwned();
            var compute = native.NativeCompute.init(a, &weights, null);
            defer compute.deinit();
            const cb = compute.computeBackend();
            const base = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }, &.{ 3, 4 });
            defer cb.free(base);
            const image = try cb.fromFloat32Shape(&.{ 21, 22, 23, 24 }, &.{ 1, 4 });
            defer cb.free(image);
            const spans = [_]EmbeddingReplacement{.{ .offset = 1, .rows = 1, .source = .{ .resident = image } }};
            const output = try replaceEmbeddingRows(&cb, a, base, 3, 4, 1, .{ .spans = &spans });
            defer cb.free(output);
            const values = try cb.toFloat32(output, a);
            defer a.free(values);
            try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4, 21, 22, 23, 24, 9, 10, 11, 12 }, values);
            const unit_host = try replaceEmbeddingRows(&cb, a, base, 3, 4, 1, .{ .host_chunks = .{ .values = &.{ 31, 32, 33, 34 }, .offsets = &.{1}, .rows_per_chunk = 1 } });
            defer cb.free(unit_host);
            const unit_values = try cb.toFloat32(unit_host, a);
            defer a.free(unit_values);
            try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4, 31, 32, 33, 34, 9, 10, 11, 12 }, unit_values);
            const scaled = try replaceEmbeddingRows(&cb, a, base, 3, 4, 2, .{ .host_chunks = .{ .values = &.{ 31, 32, 33, 34 }, .offsets = &.{1}, .rows_per_chunk = 1 } });
            defer cb.free(scaled);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "Native packed prompt image materializes only the unsupported source without replaying earlier rows" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    const Tensor = @import("../backends/tensor.zig").Tensor;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var dense: [64]f32 = undefined;
    for (&dense, 0..) |*value, index| value.* = @as(f32, @floatFromInt(index)) / 32 - 1;
    var source = try Tensor.initFloat32(a, "source", &.{ 2, 32 }, &dense);
    defer source.deinit();
    var quantized = try @import("../models/weight_source.zig").quantizeDenseQ8_0(a, &source);
    {
        errdefer quantized.deinit();
        const key = try a.dupe(u8, "packed.prompt");
        errdefer a.free(key);
        try store.resident_weights.put(a, key, .{ .tensor = .{ .name = key, .data = &.{}, .shape = &.{ 2, 32 }, .dtype = .f32, .allocator = a, .owns_data = false, .owns_shape = false }, .quantized = true, .quantized_storage = quantized });
    }
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const image = try cb.getWeight("packed.prompt");
    defer cb.free(image);
    const decoded = try cb.toFloat32(image, a);
    defer a.free(decoded);
    // The genuine packed source can be read through its old host ABI while
    // every backend destination allocation is refused. The known unsupported
    // base must never try a full speculative destination before that fallback.
    var packed_prompt = blk: {
        var fail = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        const original_allocator = compute.allocator;
        compute.allocator = fail.allocator();
        defer compute.allocator = original_allocator;
        const prepared = try PromptEmbeddingRows.init(&cb, a, image, 2, 32, 1);
        try std.testing.expectEqual(@as(usize, 0), fail.alloc_index);
        break :blk prepared;
    };
    defer packed_prompt.deinit();
    try std.testing.expect(packed_prompt.storage == .host);
    const packed_output = try packed_prompt.finish();
    defer cb.free(packed_output);
    const packed_values = try cb.toFloat32(packed_output, a);
    defer a.free(packed_values);
    try std.testing.expectEqualSlices(f32, decoded, packed_values);
    const base_values: [160]f32 = @splat(3);
    const base = try cb.fromFloat32Shape(&base_values, &.{ 5, 32 });
    defer cb.free(base);
    var prompt = try PromptEmbeddingRows.init(&cb, a, base, 5, 32, 1);
    defer prompt.deinit();
    try std.testing.expect(prompt.storage == .resident);
    try std.testing.expect(!try prompt.storage.resident.acceptsSource(image, 2));
    try prompt.replace(.{ .offset = 1, .rows = 2, .source = .{ .resident = image } });
    const output = try prompt.finish();
    defer cb.free(output);
    const actual = try cb.toFloat32(output, a);
    defer a.free(actual);
    try std.testing.expectEqualSlices(f32, base_values[0..32], actual[0..32]);
    try std.testing.expectEqualSlices(f32, decoded, actual[32..96]);
    try std.testing.expectEqualSlices(f32, base_values[96..], actual[96..]);
}

test "Metal prompt replacement materializes a real foreign source before its destination copy" {
    const helper = @import("../ops/media_residency_test.zig");
    const Fixture = struct {
        fn run(backend_value: ComputeBackend, _: ComputeBackend) !void {
            const a = std.testing.allocator;
            const cb = &backend_value;
            const metal = @import("../ops/metal_compute.zig");
            var store = @import("../ops/gpu_hosted_store.zig").WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .{} };
            defer store.lazy_weights.deinit(a);
            metal.initPrefetchQueue(&store, a);
            defer metal.deinitPrefetchQueue(&store);
            defer metal.deinitSharedNativeProvider(&store);
            var foreign = try metal.MetalCompute.init(a, &store, null);
            defer foreign.deinit();
            const foreign_cb = foreign.computeBackend();
            const image = try foreign_cb.ensureDeviceResidentOwned(try foreign_cb.fromFloat32Shape(&.{ 91, 92, 93, 94 }, &.{ 1, 4 }));
            defer foreign_cb.free(image);
            const base = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }, &.{ 3, 4 }));
            defer cb.free(base);
            var prompt = try PromptEmbeddingRows.init(cb, a, base, 3, 4, 1);
            defer prompt.deinit();
            try std.testing.expect(prompt.storage == .resident);
            try std.testing.expect(!try prompt.storage.resident.acceptsSource(image, 1));
            const before = helper.Transfers.snapshot(foreign_cb);
            try prompt.replace(.{ .offset = 1, .rows = 1, .source = .{ .resident = image } });
            const after = helper.Transfers.snapshot(foreign_cb);
            try std.testing.expect(after.downloads > before.downloads or after.accesses > before.accesses);
            const output = try prompt.finish();
            defer cb.free(output);
            const actual = try cb.toFloat32(output, a);
            defer a.free(actual);
            try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4, 91, 92, 93, 94, 9, 10, 11, 12 }, actual);
        }
    };
    try helper.onMetal(helper.noWeights, Fixture.run);
}

test "CUDA streaming prompt replacement preserves rows scale held outputs and no intermediate export" {
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onCuda(helper.noWeights, checkPromptReplacementRows);
}
